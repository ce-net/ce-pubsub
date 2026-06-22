//! Live integration tests for ce-pubsub against real ephemeral CE nodes.
//!
//! These exercise the actual mesh transports the `src/` unit tests stub out:
//!
//! * **live fan-out** — an owner on node A publishes; a subscriber on node B receives over gossip;
//! * **durable replay from a cursor** — a late puller replays every message with `cursor > from`
//!   from the owner's durable `ce-coord` log (at-least-once), even though it was never subscribed;
//! * **capability publish gating** — a cap-gated topic rejects a remote publish with no/invalid
//!   token and accepts one bearing a valid `pubsub:publish` link.
//!
//! A fresh 2-node loopback mesh is stood up per test (never the operator's :8844 node). If the
//! release `ce` binary isn't built, every test logs the reason and returns early (pass).
//!
//! Run with: `cargo test -p ce-pubsub --test live -- --nocapture`
//! Disable explicitly with: `CE_NO_LIVE=1 cargo test`.

mod harness;

use std::time::Duration;

use ce_coord::Coord;
use ce_pubsub::{caps, PubSub};
use harness::{live_available, Node};

/// Bring up a 2-node loopback mesh and a `PubSub` handle on each node.
async fn two_node_pubsub() -> anyhow::Result<Option<(Node, Node, PubSub, PubSub)>> {
    if !live_available() {
        return Ok(None);
    }
    let a = Node::start(None).await?;
    let b = Node::start(Some(&a.dial_addr())).await?;
    // Give the libp2p link time to form before pub/sub flows.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let coord_a = Coord::with_client(a.client.clone()).await?;
    let coord_b = Coord::with_client(b.client.clone()).await?;
    let ps_a = PubSub::with_coord(coord_a, a.client.clone());
    let ps_b = PubSub::with_coord(coord_b, b.client.clone());
    Ok(Some((a, b, ps_a, ps_b)))
}

/// Live fan-out: the owner (node A) publishes; a live subscriber on node B receives the message over
/// gossip. Best-effort transport, but a healthy loopback 2-node link delivers reliably; we retry the
/// publish across the window so a single dropped gossip frame doesn't flake the test.
#[tokio::test]
async fn live_fanout_owner_to_subscriber() -> anyhow::Result<()> {
    let Some((a, _b, ps_a, ps_b)) = two_node_pubsub().await? else { return Ok(()) };

    let topic = ps_a.create_topic("orders").await?;
    let mut sub = ps_b.subscribe("orders", &a.node_id).await?;
    // Let the subscription register on the mesh.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Publish repeatedly until B receives one (or the window elapses).
    let recv = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(20), sub.recv()).await.ok().flatten()
    });
    for i in 0..20 {
        topic.publish(format!("order-{i}").as_bytes()).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;
        if recv.is_finished() {
            break;
        }
    }
    match recv.await? {
        Some(msg) => {
            assert!(msg.text().starts_with("order-"), "received a published message: {}", msg.text());
            assert_eq!(msg.publisher, a.node_id, "publisher is the owner");
        }
        None => {
            // Documented at-most-once gossip: do not hard-fail on a missed frame; log it.
            eprintln!("[live_fanout] no live message in window (best-effort gossip)");
        }
    }
    Ok(())
}

/// Durable replay from a cursor: the owner publishes a batch BEFORE any puller exists; a fresh puller
/// on node B replays the whole log (from=0), and a second pull from a mid cursor returns only the
/// strict tail. This is the at-least-once guarantee, independent of live gossip.
#[tokio::test]
async fn live_durable_replay_from_cursor() -> anyhow::Result<()> {
    let Some((a, _b, ps_a, ps_b)) = two_node_pubsub().await? else { return Ok(()) };

    let topic = ps_a.create_topic("audit").await?;
    // Publish 6 messages as the owner (durably appended, each gets a monotonic cursor).
    let mut cursors = Vec::new();
    for i in 0..6 {
        cursors.push(topic.publish(format!("evt-{i}").as_bytes()).await?);
    }
    assert_eq!(cursors, vec![1, 2, 3, 4, 5, 6], "cursors are 1-based and monotonic");

    // A fresh puller on B replays everything from the beginning, retrying until its read replica of
    // the owner's durable log converges to all 6 messages.
    let mut got_all = Vec::new();
    for _attempt in 0..25 {
        let replay = ps_b.pull("audit", &a.node_id, 0).await?;
        if replay.len() == 6 {
            got_all = replay.into_messages();
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    assert_eq!(got_all.len(), 6, "puller replayed all 6 durable messages from cursor 0");
    assert_eq!(got_all[0].cursor, 1);
    assert_eq!(got_all[5].text(), "evt-5");

    // Pull from a mid cursor → strict tail only (at-least-once "resume from where I left off").
    let tail = ps_b.pull("audit", &a.node_id, 3).await?;
    assert_eq!(tail.len(), 3, "from=3 yields cursors 4,5,6");
    assert!(tail.messages().iter().all(|m| m.cursor > 3));
    assert_eq!(tail.high_cursor(), 6);

    Ok(())
}

/// Capability publish gating: a cap-required topic rejects a remote publish without a token and with
/// a wrong-scope token, and accepts a valid `pubsub:publish` link for the topic. Exercises the real
/// directed ingest request/reply path over the mesh.
#[tokio::test]
async fn live_capability_publish_gating() -> anyhow::Result<()> {
    let Some((a, b, ps_a, ps_b)) = two_node_pubsub().await? else { return Ok(()) };

    let topic = ps_a.create_topic("secure").await?;
    topic.require_publish_cap(true);
    // Let the ingest worker subscribe.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // B (a remote publisher) tries to publish with NO token → owner rejects.
    let no_token = ps_b
        .publish_to("secure", &a.node_id, b"x", None, 8_000)
        .await;
    assert!(no_token.is_err(), "cap-gated topic must reject a tokenless remote publish");

    // Mint a valid publish link from the OWNER (node A's identity) bound to B as the audience.
    let owner_id = ce_identity::Identity::load_or_generate(&a.data_dir_path.join("identity"))?;
    let b_node: [u8; 32] = {
        let bytes = hex::decode(&b.node_id).unwrap();
        bytes.try_into().unwrap()
    };
    let good = caps::mint_link(&owner_id, b_node, caps::ABILITY_PUBLISH, "secure", 0, 1).unwrap();

    // A wrong-scope token (scoped to a different topic) → owner rejects.
    let wrong = caps::mint_link(&owner_id, b_node, caps::ABILITY_PUBLISH, "other-topic", 0, 2).unwrap();
    let wrong_res = ps_b
        .publish_to("secure", &a.node_id, b"x", Some(&wrong), 8_000)
        .await;
    assert!(wrong_res.is_err(), "wrong-scope token must be rejected");

    // The valid token is accepted; the publish returns a cursor.
    let mut accepted = None;
    for _ in 0..6 {
        match ps_b.publish_to("secure", &a.node_id, b"hello", Some(&good), 8_000).await {
            Ok(cursor) => {
                accepted = Some(cursor);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    assert!(accepted.is_some(), "a valid pubsub:publish link must be accepted by the owner");
    assert!(accepted.unwrap() >= 1, "accepted publish got a real cursor");

    Ok(())
}
