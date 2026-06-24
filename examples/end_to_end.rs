//! A runnable, node-free walkthrough of ce-pubsub's durable core: the topic log and the
//! server-tracked subscription registry. Run with:
//!
//! ```text
//! cargo run -p ce-pubsub --example end_to_end
//! ```
//!
//! This exercises the *pure* layers that back the live mesh paths — the same `TopicLog` the topic
//! owner replicates over `ce-coord`, and the same `SubRegistry` that tracks ack/lease state — so you
//! can see at-least-once replay and lease/ack/dead-letter mechanics without standing up two CE nodes.
//! The mesh-connected API (`PubSub::connect`, `publish_to`, `pull`, `lease`) is demonstrated in the
//! `tests/live.rs` 2-node harness; see the README for the wire walkthrough.

use ce_coord::StateMachine;
use ce_pubsub::log::{LogOp, TopicLog};
use ce_pubsub::message::Message;
use ce_pubsub::subscription::{SubOp, SubRegistry, SubscriptionPolicy};

fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn main() {
    // ---- 1. The durable topic log: append assigns absolute cursors; replay is at-least-once. ----
    println!("== durable topic log ==");
    let mut log = TopicLog::default();
    for body in ["order-1", "order-2", "order-3", "order-4"] {
        let cursor = log.next_cursor();
        log.apply(LogOp::Append(Message::new(
            cursor,
            "owner",
            body.as_bytes(),
            now(),
        )));
        println!("  appended {body:>8} at cursor {cursor}");
    }

    // A late subscriber that processed up to cursor 2 replays only the strict tail (3, 4).
    let tail = log.since(2);
    println!("  replay from cursor 2 -> {} message(s):", tail.len());
    for m in &tail {
        println!("    [{}] {}", m.cursor, m.text());
    }

    // Retention: prune the first two messages. Surviving cursors stay absolute (3, 4).
    log.apply(LogOp::PruneTo(2));
    println!(
        "  after prune_to(2): retained={} floor={} high={} (cursors stay absolute)",
        log.len(),
        log.floor(),
        log.high_cursor()
    );
    assert_eq!(log.all().first().map(|m| m.cursor), Some(3));

    // ---- 2. Server-tracked subscription: lease, ack, redelivery, dead-letter. ----
    println!("\n== server-tracked subscription ==");
    let mut subs = SubRegistry::default();
    let policy = SubscriptionPolicy {
        ack_deadline_secs: 30,
        max_delivery_attempts: 3,
    };
    subs.apply(SubOp::Create {
        name: "workers".into(),
        policy,
    });

    let high = log.high_cursor(); // 4
    let floor = log.floor(); // 2 (1,2 pruned)
    let t0 = 1_000u64;

    // Lease the available backlog (cursors 3, 4 — 1 and 2 are below the retention floor).
    let (op, out) = subs
        .plan_lease("workers", high, floor, t0, 10)
        .expect("a fresh subscription has a backlog to lease");
    subs.apply(op);
    println!("  leased {:?} (cursor, attempt)", out.leased);
    assert_eq!(out.leased, vec![(3, 1), (4, 1)]);

    // Ack cursor 3; its floor advances. Cursor 4 is still outstanding.
    subs.apply(SubOp::Ack {
        name: "workers".into(),
        cursor: 3,
    });
    println!(
        "  acked 3 -> floor now {}",
        subs.get("workers").unwrap().acked
    );

    // Cursor 4 is never acked: after the ack deadline it is redelivered with an incremented attempt.
    let t1 = t0 + 31;
    if let Some((op, out)) = subs.plan_lease("workers", high, floor, t1, 10) {
        subs.apply(op);
        println!("  after deadline, redelivered {:?}", out.leased);
        assert_eq!(out.leased, vec![(4, 2)]);
    }

    // Keep timing out until attempts exceed max_delivery_attempts=3 -> dead-lettered.
    let mut t = t1;
    loop {
        t += 31;
        let Some((op, out)) = subs.plan_lease("workers", high, floor, t, 10) else {
            break;
        };
        subs.apply(op);
        if !out.dead_lettered.is_empty() {
            println!(
                "  cursor(s) {:?} exceeded max attempts -> dead-lettered",
                out.dead_lettered
            );
            break;
        }
    }
    let st = subs.get("workers").unwrap();
    println!(
        "  subscription floor advanced to {} (poison no longer blocks)",
        st.acked
    );
    assert_eq!(
        st.acked, high,
        "dead-lettering advanced the floor past the poison message"
    );

    println!(
        "\nOK — durable replay, leasing, redelivery, and dead-lettering all behaved as documented."
    );
}
