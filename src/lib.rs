//! # ce-pubsub — managed Pub/Sub over CE mesh gossip + a durable ce-coord log
//!
//! Google Pub/Sub is a broker you provision: topics, subscriptions, at-least-once delivery with
//! replay, and IAM bindings on who may publish/subscribe. ce-pubsub is the same surface assembled
//! from CE primitives, with **no broker to run**:
//!
//! * **Topics + live fan-out** ride a `ce-coord` typed [`Stream`](ce_coord::Stream) over the node's
//!   mesh pub/sub — every node subscribed to a topic gets each message as it is broadcast. This is
//!   fast but *at-most-once* (the inbox ring is bounded; an offline subscriber misses messages).
//! * **Durability + replay** ride a `ce-coord` single-writer append log owned by the topic owner.
//!   Every accepted message is appended to that log, so a late subscriber can [`PubSub::pull`] from a
//!   cursor and replay everything it missed — *at-least-once*. The log is snapshot-capable, so a
//!   fresh puller bootstraps from a content-addressed blob instead of replaying from message 1.
//! * **Authorization** is a `ce-cap` chain (`pubsub:publish` / `pubsub:subscribe` scoped to a topic).
//!   The owner verifies a presented token offline before accepting a remote publish — no policy
//!   server. See [`caps`].
//!
//! ## Roles
//!
//! The node that **creates** a topic ([`PubSub::create_topic`]) is its **owner** and the durable-log
//! writer. Other nodes are **publishers** (send messages to the owner over the mesh; the owner
//! appends + fans out) and **subscribers** (live tail, or durable [`pull`](PubSub::pull) replay). A
//! topic is global-by-`(owner_node_id, name)`: a subscriber follows it by both.
//!
//! ## Two transports, one node
//!
//! [`PubSub`] holds a [`Coord`] (which opens its own client to the local node for the durable log and
//! the live stream) **and** a separate [`CeClient`] used only for the remote-publish request/reply
//! path. Both speak to the same local node over HTTP; keeping them separate is what lets ce-pubsub
//! stay entirely on `ce-coord` and `ce-rs` public API.
//!
//! ## Shape
//!
//! ```no_run
//! use ce_pubsub::PubSub;
//! # async fn demo() -> anyhow::Result<()> {
//! let ps = PubSub::connect().await?;
//!
//! // --- owner ---
//! let topic = ps.create_topic("orders").await?;        // become the durable-log writer
//! let cursor = topic.publish(b"order-1").await?;         // append + broadcast; returns the cursor
//!
//! // --- subscriber on another node (knows the owner's NodeId) ---
//! let mut live = ps.subscribe("orders", "owner_node_id_hex").await?;
//! // let msg = live.recv().await;                        // live, at-most-once
//!
//! // --- late subscriber: replay everything from the start ---
//! let replay = ps.pull("orders", "owner_node_id_hex", 0).await?;
//! for m in replay.messages() { println!("{}: {}", m.cursor, m.text()); }
//! # Ok(()) }
//! ```

pub mod caps;
pub mod log;
pub mod message;

pub use message::{Cursor, Message};

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use ce_coord::{Coord, Replicated, Stream};
use ce_rs::CeClient;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use log::{LogOp, TopicLog};

/// The directed-request reply the owner sends after handling a published message: the cursor it was
/// assigned, or an error string the publisher surfaces.
#[derive(Debug, Serialize, Deserialize)]
enum IngestReply {
    /// The message was accepted and appended at this cursor.
    Accepted(Cursor),
    /// The message was rejected (e.g. capability check failed); the string explains why.
    Rejected(String),
}

/// A directed publish request a remote publisher sends to a topic owner: the payload and an optional
/// capability token proving `pubsub:publish` on the topic.
#[derive(Debug, Serialize, Deserialize)]
struct IngestRequest {
    payload_hex: String,
    /// Optional `ce-cap` chain token granting publish on this topic. Required only when the owner runs
    /// cap-gated (see [`Topic::require_publish_cap`]).
    grant: Option<String>,
}

/// Unix seconds now.
fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Handle to the local node's Pub/Sub layer. Cheap to clone. One [`PubSub`] shares a single
/// `ce-coord` [`Coord`] across every topic, subscription, and pull on this node.
#[derive(Clone)]
pub struct PubSub {
    coord: Coord,
    /// A separate client to the same local node, used only for the remote-publish request/reply path.
    ce: CeClient,
    node_id: String,
}

impl PubSub {
    /// Connect to the local CE node on the default port (8844) and start the coordination pump. The
    /// node API token is auto-discovered (`$CE_API_TOKEN`, else `<data dir>/api.token`); set
    /// `$CE_API_TOKEN` to point at a node started with a custom `--data-dir` (e.g. an ephemeral test
    /// node), which is how both transports here reach the same node.
    pub async fn connect() -> Result<PubSub> {
        let coord = Coord::connect().await.context("starting ce-coord")?;
        let node_id = coord.node_id().to_string();
        Ok(PubSub { coord, ce: CeClient::local(), node_id })
    }

    /// This node's NodeId (hex). Subscribers on other nodes need it to follow topics this node owns.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Create (own) a topic: become the durable-log writer for `topic`, open the live broadcast
    /// stream, and start serving remote publishers' ingest requests. Returns a [`Topic`] handle used
    /// to publish. Keep one owner per topic.
    ///
    /// By default the owner accepts any remote publish (open topic). Call
    /// [`Topic::require_publish_cap`] to gate remote publishes behind a `ce-cap` chain.
    pub async fn create_topic(&self, topic: &str) -> Result<Topic> {
        message::validate_topic(topic)?;
        let writer = Replicated::<TopicLog>::writer(self.coord.clone(), &message::log_name(topic))
            .await
            .context("opening durable topic log as writer")?;
        // The live stream name embeds the owner's id so distinct owners' same-named topics never
        // collide, and so a subscriber following `(topic, owner)` opens the exact same name.
        let live_name = format!("{}@{}", message::live_topic(topic), self.node_id);
        let live = self
            .coord
            .stream::<Message>(&live_name)
            .await
            .context("opening live broadcast stream")?;
        let inner = Arc::new(TopicInner {
            ps: self.clone(),
            topic: topic.to_string(),
            writer,
            live: Mutex::new(live),
            accepted_roots: Vec::new(),
            require_cap: std::sync::atomic::AtomicBool::new(false),
        });
        let handle = Topic { inner };
        handle.serve_ingest();
        Ok(handle)
    }

    /// Subscribe to a topic for **live** delivery (at-most-once). Returns a [`Subscription`] whose
    /// [`recv`](Subscription::recv) yields each message as the owner broadcasts it. Live delivery is
    /// best-effort: a message published while this node was offline will not appear here — use
    /// [`pull`](Self::pull) to replay durably.
    ///
    /// `owner` is the topic owner's NodeId hex. (Live messages are authored by the owner; `ce-coord`
    /// authenticates the stream sender, so non-owner traffic on the name is naturally excluded by the
    /// owner being the only publisher.)
    pub async fn subscribe(&self, topic: &str, owner: &str) -> Result<Subscription> {
        message::validate_topic(topic)?;
        // The live stream name embeds the owner so distinct owners' topics never collide.
        let name = format!("{}@{owner}", message::live_topic(topic));
        let stream = self
            .coord
            .stream::<Message>(&name)
            .await
            .context("opening live subscription stream")?;
        Ok(Subscription { stream })
    }

    /// Durably **pull** (replay) a topic from a cursor: open a `ce-coord` read replica of the owner's
    /// log, converge it, and return a [`Replay`] holding every message with `cursor > from`. Pass
    /// `from = 0` to replay from the very first retained message. This is the at-least-once path: it
    /// returns messages even if this node was never subscribed live.
    ///
    /// `owner` is the topic owner's NodeId hex. The replica bootstraps from the owner's latest
    /// snapshot when one exists (cheap), else replays the full log.
    pub async fn pull(&self, topic: &str, owner: &str, from: Cursor) -> Result<Replay> {
        message::validate_topic(topic)?;
        let reader =
            Replicated::<TopicLog>::snapshot_reader(self.coord.clone(), &message::log_name(topic), owner)
                .await
                .context("opening durable topic log as reader")?;
        self.await_convergence(&reader).await;
        let (messages, high) = reader.read(|s| (s.since(from), s.high_cursor()));
        Ok(Replay { messages, high })
    }

    /// Wait until a read replica has stopped advancing for a short quiet window, or a hard deadline
    /// elapses — a pragmatic "converged enough to read" for a one-shot pull. A long-lived subscriber
    /// would instead hold the replica and react to its `version_watch`.
    async fn await_convergence(&self, reader: &Replicated<TopicLog>) {
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        let mut last = reader.version();
        let mut stable_for = 0u32;
        loop {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let v = reader.version();
            if v == last {
                stable_for += 1;
                if stable_for >= 2 && (v > 0 || std::time::Instant::now() >= deadline) {
                    break;
                }
            } else {
                stable_for = 0;
                last = v;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
        }
    }

    /// Send a message to a topic this node does **not** own, over the mesh, as a directed request to
    /// the `owner`, optionally presenting a `grant` capability token. The owner verifies the grant,
    /// appends to the durable log, broadcasts live, and replies with the assigned [`Cursor`].
    ///
    /// Use this from a publisher node. The topic owner uses [`Topic::publish`] directly (no round
    /// trip). `timeout_ms` bounds the wait for the owner's reply.
    pub async fn publish_to(
        &self,
        topic: &str,
        owner: &str,
        payload: &[u8],
        grant: Option<&str>,
        timeout_ms: u64,
    ) -> Result<Cursor> {
        message::validate_topic(topic)?;
        let req = IngestRequest {
            payload_hex: hex::encode(payload),
            grant: grant.map(|g| g.to_string()),
        };
        let body = serde_json::to_vec(&req)?;
        let reply = self
            .ce
            .request(owner, &message::ingest_topic(topic), &body, timeout_ms)
            .await
            .context("publish request to topic owner")?;
        match serde_json::from_slice::<IngestReply>(&reply)? {
            IngestReply::Accepted(cursor) => Ok(cursor),
            IngestReply::Rejected(why) => Err(anyhow!("owner rejected publish: {why}")),
        }
    }
}

/// Shared state behind a [`Topic`] (the owner's writer handle for one topic).
struct TopicInner {
    ps: PubSub,
    topic: String,
    writer: Replicated<TopicLog>,
    /// The live broadcast stream the owner fans messages out on. Behind a mutex because `Stream`'s
    /// `publish` takes `&self` but lives in one place; the owner is the sole publisher.
    live: Mutex<Stream<Message>>,
    /// Root keys (besides this node's own key) whose capability chains the owner accepts for publish.
    accepted_roots: Vec<ce_identity::NodeId>,
    /// When true, remote publishes must present a valid `pubsub:publish` capability for the topic.
    require_cap: std::sync::atomic::AtomicBool,
}

/// An owned topic: the writer side. Created by [`PubSub::create_topic`]. Holds the durable-log writer
/// and the live broadcast stream, and serves remote publishers' directed ingest requests.
#[derive(Clone)]
pub struct Topic {
    inner: Arc<TopicInner>,
}

impl Topic {
    /// The topic name.
    pub fn name(&self) -> &str {
        &self.inner.topic
    }

    /// The highest cursor assigned so far (0 if nothing published).
    pub fn high_cursor(&self) -> Cursor {
        self.inner.writer.read(|s| s.high_cursor())
    }

    /// Require a `ce-cap` chain on every *remote* publish (default: open). The owner always accepts
    /// its own [`publish`](Self::publish) calls.
    pub fn require_publish_cap(&self, require: bool) {
        self.inner.require_cap.store(require, std::sync::atomic::Ordering::Relaxed);
    }

    /// Publish a message as the owner: append it to the durable log (assigning the next cursor) and
    /// broadcast it live to subscribers. Returns the assigned [`Cursor`]. This is the no-round-trip
    /// path the owner uses; remote publishers use [`PubSub::publish_to`].
    pub async fn publish(&self, payload: &[u8]) -> Result<Cursor> {
        self.append_and_fanout(&self.inner.ps.node_id, payload).await
    }

    /// Append a message authored by `publisher` to the durable log, then broadcast it live. Shared by
    /// the owner's direct [`publish`](Self::publish) and the ingest handler for remote publishers.
    async fn append_and_fanout(&self, publisher: &str, payload: &[u8]) -> Result<Cursor> {
        // Stamp the next absolute cursor under the writer's current state, then append durably.
        let cursor = self.inner.writer.read(|s| s.next_cursor());
        let msg = Message::new(cursor, publisher.to_string(), payload, now_secs());
        self.inner.writer.propose(LogOp::Append(msg.clone())).await.context("durable append")?;
        // Live fan-out (best-effort): broadcast the stored message on the live stream.
        if let Err(e) = self.inner.live.lock().await.publish(&msg).await {
            tracing::warn!(topic = %self.inner.topic, "live fan-out failed: {e:#}");
        }
        Ok(cursor)
    }

    /// Drop every retained message with `cursor <= up_to` (retention). Pullers that bootstrap after a
    /// prune get the surviving tail (or, once the owner snapshots, the snapshot then the tail).
    pub async fn prune_to(&self, up_to: Cursor) -> Result<()> {
        self.inner.writer.propose(LogOp::PruneTo(up_to)).await.context("prune")?;
        Ok(())
    }

    /// Take a content-addressed snapshot of the durable log and compact it, so fresh pullers bootstrap
    /// from a blob instead of replaying every message. Safe to call repeatedly.
    pub async fn checkpoint(&self) -> Result<()> {
        self.inner.writer.checkpoint().await.context("checkpoint")?;
        Ok(())
    }

    /// Spawn the ingest loop so remote publishers can send messages to this owner. It polls the node's
    /// inbox for directed requests on the topic's ingest topic, verifies any required capability,
    /// appends + fans out, and replies with the assigned cursor. Best-effort de-dup over the inbox
    /// ring (a request seen twice across polls is appended once per `reply_token`).
    fn serve_ingest(&self) {
        let topic = self.clone();
        let ingest = message::ingest_topic(&self.inner.topic);
        tokio::spawn(async move {
            // Subscribe so the node enqueues directed messages on this topic into the inbox.
            if let Err(e) = topic.inner.ps.ce.subscribe(&ingest).await {
                tracing::warn!(topic = %topic.inner.topic, "ingest subscribe failed: {e:#}");
            }
            // De-dup processed requests by reply_token (each request carries a unique one).
            let mut handled: std::collections::HashSet<u64> = std::collections::HashSet::new();
            loop {
                if let Ok(msgs) = topic.inner.ps.ce.messages().await {
                    for m in msgs {
                        if m.topic != ingest {
                            continue;
                        }
                        let Some(token) = m.reply_token else { continue };
                        if !handled.insert(token) {
                            continue;
                        }
                        let reply = match topic.handle_ingest(&m.payload_hex, &m.from).await {
                            Ok(cursor) => IngestReply::Accepted(cursor),
                            Err(why) => IngestReply::Rejected(why),
                        };
                        if let Ok(bytes) = serde_json::to_vec(&reply) {
                            let _ = topic.inner.ps.ce.reply(token, &bytes).await;
                        }
                    }
                    // Keep the de-dup set bounded.
                    if handled.len() > 16_384 {
                        handled.clear();
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    /// Decode, authorize, and apply one remote ingest request. Returns the assigned cursor or a
    /// rejection reason (string, safe to return to the publisher).
    async fn handle_ingest(&self, payload_hex: &str, publisher: &str) -> Result<Cursor, String> {
        let bytes = hex::decode(payload_hex).map_err(|e| format!("bad request hex: {e}"))?;
        let req: IngestRequest =
            serde_json::from_slice(&bytes).map_err(|e| format!("bad request body: {e}"))?;

        if self.inner.require_cap.load(std::sync::atomic::Ordering::Relaxed) {
            let token = req.grant.as_deref().ok_or_else(|| {
                "topic requires a pubsub:publish capability but none was presented".to_string()
            })?;
            let requester =
                parse_node_id(publisher).map_err(|e| format!("bad publisher node id: {e}"))?;
            let self_id = parse_node_id(&self.inner.ps.node_id)
                .map_err(|e| format!("bad owner node id: {e}"))?;
            let revoked = self.fetch_revoked().await;
            caps::verify_link(
                &self_id,
                &self.inner.accepted_roots,
                &[],
                now_secs(),
                &requester,
                caps::ABILITY_PUBLISH,
                &self.inner.topic,
                token,
                &|issuer, nonce| revoked.iter().any(|(i, n)| i == issuer && *n == nonce),
            )?;
        }

        let payload = hex::decode(&req.payload_hex).map_err(|e| format!("bad payload hex: {e}"))?;
        self.append_and_fanout(publisher, &payload)
            .await
            .map_err(|e| format!("append failed: {e:#}"))
    }

    /// Fetch the on-chain revoked `(issuer, nonce)` set (as raw node ids) so cap checks honor
    /// revocation. Best-effort: on error, treat nothing as revoked (signature/expiry still apply).
    async fn fetch_revoked(&self) -> Vec<(ce_identity::NodeId, u64)> {
        match self.inner.ps.ce.revoked().await {
            Ok(set) => set
                .into_iter()
                .filter_map(|(issuer_hex, nonce)| parse_node_id(&issuer_hex).ok().map(|id| (id, nonce)))
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

/// Parse a 64-hex NodeId into the `[u8; 32]` form `ce-cap` expects.
fn parse_node_id(hex_id: &str) -> Result<ce_identity::NodeId> {
    let bytes = hex::decode(hex_id.trim()).context("node id is not valid hex")?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("node id must be 32 bytes (64 hex chars)"))?;
    Ok(arr)
}

/// A live subscription: the at-most-once tail of a topic. Each [`recv`](Self::recv) yields the next
/// message the owner broadcasts.
pub struct Subscription {
    stream: Stream<Message>,
}

impl Subscription {
    /// Await the next live message, or `None` once the underlying stream closes.
    pub async fn recv(&mut self) -> Option<Message> {
        self.stream.next().await
    }
}

/// The result of a durable [`pull`](PubSub::pull): every message replayed from the requested cursor,
/// plus the topic's high-water cursor at the time of the pull (so the caller can pull again from
/// `high` later to continue the replay).
#[derive(Debug, Clone)]
pub struct Replay {
    messages: Vec<Message>,
    high: Cursor,
}

impl Replay {
    /// The replayed messages, in cursor order.
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// Consume into the owned message vector.
    pub fn into_messages(self) -> Vec<Message> {
        self.messages
    }

    /// The topic's highest cursor at pull time — the cursor to pass as `from` on a later pull to
    /// continue where this replay ended.
    pub fn high_cursor(&self) -> Cursor {
        self.high
    }

    /// Number of replayed messages.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// True if nothing was replayed.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_reply_roundtrips() {
        for reply in [IngestReply::Accepted(7), IngestReply::Rejected("nope".into())] {
            let bytes = serde_json::to_vec(&reply).unwrap();
            let back: IngestReply = serde_json::from_slice(&bytes).unwrap();
            match (reply, back) {
                (IngestReply::Accepted(a), IngestReply::Accepted(b)) => assert_eq!(a, b),
                (IngestReply::Rejected(a), IngestReply::Rejected(b)) => assert_eq!(a, b),
                _ => panic!("variant changed across round-trip"),
            }
        }
    }

    #[test]
    fn ingest_request_roundtrips() {
        let req = IngestRequest { payload_hex: hex::encode(b"hi"), grant: Some("deadbeef".into()) };
        let bytes = serde_json::to_vec(&req).unwrap();
        let back: IngestRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(req.payload_hex, back.payload_hex);
        assert_eq!(req.grant, back.grant);
    }

    #[test]
    fn parse_node_id_validates_length() {
        assert!(parse_node_id(&"ab".repeat(32)).is_ok()); // 64 hex chars
        assert!(parse_node_id("xyz").is_err());
        assert!(parse_node_id(&"ab".repeat(10)).is_err());
    }

    #[test]
    fn replay_exposes_messages_and_high() {
        let r = Replay {
            messages: vec![Message::new(2, "n", b"a", 1), Message::new(3, "n", b"b", 2)],
            high: 3,
        };
        assert_eq!(r.len(), 2);
        assert_eq!(r.high_cursor(), 3);
        assert!(!r.is_empty());
        assert_eq!(r.messages()[0].cursor, 2);
    }
}
