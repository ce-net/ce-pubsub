//! # ce-pubsub — managed Pub/Sub over CE mesh gossip + a durable ce-coord log
//!
//! Google Pub/Sub is a broker you provision: topics, subscriptions, at-least-once delivery with
//! replay, ack/nack with lease-based redelivery, dead-letter routing, ordering keys, attribute
//! filtering, and IAM bindings on who may publish/subscribe. ce-pubsub is the same surface assembled
//! from CE primitives, with **no broker to run**:
//!
//! * **Topics + live fan-out** ride a `ce-coord` typed [`Stream`](ce_coord::Stream) over the node's
//!   mesh pub/sub — every node subscribed to a topic gets each message as it is broadcast. This is
//!   fast but *at-most-once* (the inbox ring is bounded; an offline subscriber misses messages).
//! * **Durability + replay** ride a `ce-coord` single-writer append log owned by the topic owner.
//!   Every accepted message is appended to that log, so a late subscriber can [`PubSub::pull`] from a
//!   cursor and replay everything it missed — *at-least-once*. The log is snapshot-capable, so a
//!   fresh puller bootstraps from a content-addressed blob instead of replaying from message 1.
//! * **Subscriptions** are first-class durable resources (a sibling `ce-coord` log, the
//!   [`SubRegistry`](subscription::SubRegistry)): each named subscription has its own server-tracked
//!   ack cursor, an ack-deadline lease (so a consumer that crashes mid-processing has its messages
//!   redelivered), per-message delivery-attempt counting, and a dead-letter policy. This makes
//!   at-least-once *server-enforced* rather than client cursor bookkeeping. See [`lease`](PubSub::lease),
//!   [`ack`](PubSub::ack), [`nack`](PubSub::nack).
//! * **Authorization** is a `ce-cap` chain (`pubsub:publish` / `pubsub:subscribe` scoped to a topic).
//!   The owner verifies a presented token offline before accepting a remote publish or a subscription
//!   control op — no policy server. See [`caps`].
//!
//! ## Roles
//!
//! The node that **creates** a topic ([`PubSub::create_topic`]) is its **owner** and the durable-log
//! writer. Other nodes are **publishers** (send messages to the owner over the mesh; the owner
//! appends + fans out) and **subscribers** (live tail, durable [`pull`](PubSub::pull) replay, or a
//! server-tracked [`lease`](PubSub::lease)/[`ack`](PubSub::ack) subscription). A topic is global-by-
//! `(owner_node_id, name)`: a subscriber follows it by both.
//!
//! ## Two transports, one node
//!
//! [`PubSub`] holds a [`Coord`] (which opens its own client to the local node for the durable logs and
//! the live stream) **and** a separate [`CeClient`] used only for the remote-publish and control
//! request/reply paths. Both speak to the same local node over HTTP; keeping them separate is what
//! lets ce-pubsub stay entirely on `ce-coord` and `ce-rs` public API.
//!
//! ## What is implemented vs deferred
//!
//! Implemented: durable replay, live fan-out, capability gating with attenuation, idempotent remote
//! publish, payload/attribute size bounds, paginated pull with honest convergence, first-class
//! subscriptions with ack/nack/lease/dead-letter, ordering keys + attributes on messages, and
//! explicit retention via [`Topic::prune_to`]/[`Topic::checkpoint`]. Deferred (documented in the
//! README): push (webhook) subscriptions, automatic time-based retention, seek-by-timestamp, schema
//! validation, and multi-writer HA topic ownership (the owner is currently a single writer).
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
//! let cursor = topic.publish(b"order-1").await?;        // append + broadcast; returns the cursor
//!
//! // --- subscriber on another node (knows the owner's NodeId) ---
//! let mut live = ps.subscribe("orders", "owner_node_id_hex").await?;
//! // let msg = live.recv().await;                       // live, at-most-once
//!
//! // --- late subscriber: replay everything from the start ---
//! let replay = ps.pull("orders", "owner_node_id_hex", 0).await?;
//! for m in replay.messages() { println!("{}: {}", m.cursor, m.text()); }
//! # Ok(()) }
//! ```

pub mod caps;
pub mod dedup;
pub mod log;
pub mod message;
pub mod protocol;
pub mod subscription;

pub use message::{AttributeFilter, Cursor, Message, PublishOptions};
pub use protocol::LeasedMessage;
pub use subscription::{SubscriptionPolicy, SubscriptionState};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use ce_coord::{Coord, Replicated, Stream};
use ce_rs::CeClient;
use tokio::sync::Mutex;

use dedup::{IdempotencyCache, Seen};
use log::{LogOp, TopicLog};
use protocol::{ControlReply, ControlRequest, IngestReply, IngestRequest};
use subscription::{SubOp, SubRegistry};

/// Default maximum number of messages a single [`PubSub::pull`] returns. Bounds the response so a
/// huge backlog is not materialized in one `Vec`; callers paginate via the returned `next_cursor`.
pub const DEFAULT_PULL_LIMIT: usize = 1_000;

/// Hard ceiling on a single pull's message count, regardless of the requested limit. Bounds the
/// owner's read-replica response so no caller can force an unbounded materialization.
pub const MAX_PULL_LIMIT: usize = 100_000;

/// Idempotency-cache capacity (distinct keys retained before FIFO eviction) and TTL (seconds) on the
/// owner's ingest path.
const DEDUP_CAPACITY: usize = 16_384;
const DEDUP_TTL_SECS: u64 = 3_600;

/// How often the background ingest/control workers poll the node inbox.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Unix seconds now.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Handle to the local node's Pub/Sub layer. Cheap to clone. One [`PubSub`] shares a single
/// `ce-coord` [`Coord`] across every topic, subscription, and pull on this node.
#[derive(Clone)]
pub struct PubSub {
    coord: Coord,
    /// A separate client to the same local node, used only for the remote-publish / control
    /// request/reply path.
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
        Ok(PubSub {
            coord,
            ce: CeClient::local(),
            node_id,
        })
    }

    /// Build a [`PubSub`] over an existing [`Coord`] and a matching [`CeClient`] for the same node.
    /// Both must target the *same* CE node (the `Coord`'s pump and the request/reply path share it).
    /// This is the injection point for tests against an ephemeral node on a non-default port, and for
    /// apps that already hold a `Coord`.
    pub fn with_coord(coord: Coord, ce: CeClient) -> PubSub {
        let node_id = coord.node_id().to_string();
        PubSub { coord, ce, node_id }
    }

    /// This node's NodeId (hex). Subscribers on other nodes need it to follow topics this node owns.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Create (own) a topic: become the durable-log writer for `topic`, open the live broadcast
    /// stream and the subscription registry, and start serving remote publishers' ingest requests and
    /// consumers' control requests. Returns a [`Topic`] handle used to publish and administer
    /// subscriptions. Keep **one** owner per topic.
    ///
    /// By default the owner accepts any remote publish (open topic). Call
    /// [`Topic::require_publish_cap`] to gate remote publishes behind a `ce-cap` chain. Configure the
    /// roots whose capability chains the owner accepts (besides its own key) with
    /// [`Topic::accept_root`].
    ///
    /// The background ingest/control workers run until the **last** [`Topic`] handle (and every clone)
    /// is dropped: [`Topic`] holds a shutdown flag the workers poll, so a dropped topic stops serving
    /// rather than leaking a detached task forever. Prefer one long-lived owner per topic per process;
    /// running two writers for the same topic in one process races them on the single-writer log.
    pub async fn create_topic(&self, topic: &str) -> Result<Topic> {
        message::validate_topic(topic)?;
        let writer = Replicated::<TopicLog>::writer(self.coord.clone(), &message::log_name(topic))
            .await
            .context("opening durable topic log as writer")?;
        // The subscription registry is a sibling single-writer log owned by the same node.
        let subs =
            Replicated::<SubRegistry>::writer(self.coord.clone(), &message::subs_log_name(topic))
                .await
                .context("opening subscription registry as writer")?;
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
            subs,
            live: Mutex::new(live),
            accepted_roots: Mutex::new(Vec::new()),
            require_cap: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            dedup: Mutex::new(IdempotencyCache::new(DEDUP_CAPACITY, DEDUP_TTL_SECS)),
        });
        let handle = Topic { inner };
        handle.serve_ingest();
        handle.serve_control();
        Ok(handle)
    }

    /// Subscribe to a topic for **live** delivery (at-most-once). Returns a [`Subscription`] whose
    /// [`recv`](Subscription::recv) yields each message as the owner broadcasts it. Live delivery is
    /// best-effort: a message published while this node was offline will not appear here — use
    /// [`pull`](Self::pull) (or a server-tracked [`lease`](Self::lease) subscription) to replay durably.
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
        Ok(Subscription {
            stream,
            filter: AttributeFilter::any(),
        })
    }

    /// Like [`subscribe`](Self::subscribe) but only yields messages matching `filter` (Google Pub/Sub
    /// attribute filtering). An empty filter matches everything.
    pub async fn subscribe_filtered(
        &self,
        topic: &str,
        owner: &str,
        filter: AttributeFilter,
    ) -> Result<Subscription> {
        let mut sub = self.subscribe(topic, owner).await?;
        sub.filter = filter;
        Ok(sub)
    }

    /// Durably **pull** (replay) a topic from a cursor: open a `ce-coord` read replica of the owner's
    /// log, converge it **honestly** to the owner's current high-water cursor, and return a [`Replay`]
    /// holding up to [`DEFAULT_PULL_LIMIT`] messages with `cursor > from`. Pass `from = 0` to replay
    /// from the very first retained message; paginate large backlogs by passing the previous
    /// [`Replay::next_cursor`].
    ///
    /// Unlike a timing heuristic, this asks the owner (over the control topic) for the topic's current
    /// high-water cursor, then waits for the replica to reach that version. If the replica cannot
    /// reach the target within the deadline, the returned [`Replay`] reports [`Replay::converged`] =
    /// `false` and a non-zero [`Replay::missing`] so the caller can distinguish "no more messages"
    /// from "gave up early" — the at-least-once contract is never silently violated.
    ///
    /// `owner` is the topic owner's NodeId hex. The replica bootstraps from the owner's latest
    /// snapshot when one exists (cheap), else replays the full log.
    pub async fn pull(&self, topic: &str, owner: &str, from: Cursor) -> Result<Replay> {
        self.pull_limited(topic, owner, from, DEFAULT_PULL_LIMIT, 8_000)
            .await
    }

    /// Like [`pull`](Self::pull) but with an explicit per-call message cap and convergence timeout.
    /// Returns at most `limit` messages (hard-capped at [`MAX_PULL_LIMIT`]); if the topic has more,
    /// [`Replay::next_cursor`] points past the last returned message so the caller can continue.
    pub async fn pull_limited(
        &self,
        topic: &str,
        owner: &str,
        from: Cursor,
        limit: usize,
        timeout_ms: u64,
    ) -> Result<Replay> {
        message::validate_topic(topic)?;
        let limit = limit.clamp(1, MAX_PULL_LIMIT);
        let reader = Replicated::<TopicLog>::snapshot_reader(
            self.coord.clone(),
            &message::log_name(topic),
            owner,
        )
        .await
        .context("opening durable topic log as reader")?;

        // Ask the owner for its authoritative high-water cursor (the convergence target). If the owner
        // is unreachable we fall back to best-effort quiet-window convergence and report it.
        let target = self.fetch_high_cursor(topic, owner, timeout_ms).await.ok();
        let converged = self
            .converge_to(&reader, target, Duration::from_millis(timeout_ms))
            .await;

        let (mut messages, high, floor) =
            reader.read(|s| (s.since(from), s.high_cursor(), s.floor()));
        // Page: cap to `limit` and compute the continuation cursor.
        let truncated = messages.len() > limit;
        if truncated {
            messages.truncate(limit);
        }
        let next = messages.last().map(|m| m.cursor).unwrap_or(from);
        // What the owner says exists beyond what we replayed (only meaningful if we know the target).
        let missing = match target {
            Some(t) => t.saturating_sub(high),
            None => 0,
        };
        Ok(Replay {
            messages,
            high,
            next,
            target,
            converged,
            truncated,
            floor,
            missing,
        })
    }

    /// Ask the owner for the topic's current high-water cursor over the control topic.
    async fn fetch_high_cursor(&self, topic: &str, owner: &str, timeout_ms: u64) -> Result<Cursor> {
        // If we ARE the owner, read locally — no round trip, no chance of a self-request hang.
        if owner == self.node_id {
            let reader = Replicated::<TopicLog>::snapshot_reader(
                self.coord.clone(),
                &message::log_name(topic),
                owner,
            )
            .await?;
            return Ok(reader.read(|s| s.high_cursor()));
        }
        let req = ControlRequest::HighCursor { grant: None };
        let reply = self
            .ce
            .request(
                owner,
                &message::control_topic(topic),
                &req.encode()?,
                timeout_ms,
            )
            .await
            .context("high-cursor request to owner")?;
        match ControlReply::decode(&reply)? {
            ControlReply::High(c) => Ok(c),
            ControlReply::Rejected(why) => Err(anyhow!("owner rejected high-cursor: {why}")),
            other => Err(anyhow!("unexpected reply to high-cursor: {other:?}")),
        }
    }

    /// Converge a read replica to `target` version (if known) or, when the target is unknown, to a
    /// quiet window. Returns whether convergence authoritatively reached the target (always `false`
    /// when no target is known, since a quiet window cannot prove completeness).
    async fn converge_to(
        &self,
        reader: &Replicated<TopicLog>,
        target: Option<Cursor>,
        timeout: Duration,
    ) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        if let Some(t) = target {
            // For an append-only/prune log, high_cursor() == applied appends; once the replica's high
            // cursor reaches the owner's, every message up to `t` has been replayed.
            loop {
                if reader.read(|s| s.high_cursor()) >= t {
                    return true;
                }
                if std::time::Instant::now() >= deadline {
                    return reader.read(|s| s.high_cursor()) >= t;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        // No target: best-effort quiet-window convergence, reported as not authoritatively converged.
        let mut last = reader.version();
        let mut stable = 0u32;
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let v = reader.version();
            if v == last {
                stable += 1;
                if stable >= 2 {
                    break;
                }
            } else {
                stable = 0;
                last = v;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
        }
        false
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
        self.publish_to_with(
            topic,
            owner,
            payload,
            grant,
            &PublishOptions::new(),
            None,
            timeout_ms,
        )
        .await
    }

    /// Like [`publish_to`](Self::publish_to) but with ordering key / attributes and an **idempotency
    /// key**. If the owner sees the same idempotency key twice (e.g. a retry after a lost reply) it
    /// appends only once and returns the original cursor — so remote publish is safe to retry without
    /// double-appending.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish_to_with(
        &self,
        topic: &str,
        owner: &str,
        payload: &[u8],
        grant: Option<&str>,
        opts: &PublishOptions,
        idempotency_key: Option<&str>,
        timeout_ms: u64,
    ) -> Result<Cursor> {
        message::validate_topic(topic)?;
        message::validate_payload(payload)?;
        opts.validate()?;
        let req = IngestRequest {
            payload_hex: hex::encode(payload),
            grant: grant.map(|g| g.to_string()),
            idempotency_key: idempotency_key.map(|k| k.to_string()),
            ordering_key: opts.ordering_key.clone(),
            attributes: opts.attributes.clone(),
        };
        let reply = self
            .ce
            .request(
                owner,
                &message::ingest_topic(topic),
                &req.encode()?,
                timeout_ms,
            )
            .await
            .context("publish request to topic owner")?;
        match IngestReply::decode(&reply)? {
            IngestReply::Accepted(cursor) => Ok(cursor),
            IngestReply::Rejected(why) => Err(anyhow!("owner rejected publish: {why}")),
        }
    }

    // ----- Subscription resource API (consumer side, over the control topic) -----

    /// Create (or update the policy of) a durable, server-tracked subscription on a topic owned by
    /// `owner`. The owner persists the subscription's ack cursor and lease state, so at-least-once is
    /// enforced by the server rather than by client cursor bookkeeping.
    pub async fn create_subscription(
        &self,
        topic: &str,
        owner: &str,
        name: &str,
        policy: SubscriptionPolicy,
        grant: Option<&str>,
        timeout_ms: u64,
    ) -> Result<()> {
        message::validate_topic(topic)?;
        message::validate_topic(name)?;
        policy.validate()?;
        let req = ControlRequest::CreateSubscription {
            name: name.to_string(),
            policy,
            grant: grant.map(String::from),
        };
        self.control_ok(topic, owner, req, timeout_ms).await
    }

    /// Delete a subscription.
    pub async fn delete_subscription(
        &self,
        topic: &str,
        owner: &str,
        name: &str,
        grant: Option<&str>,
        timeout_ms: u64,
    ) -> Result<()> {
        message::validate_topic(topic)?;
        let req = ControlRequest::DeleteSubscription {
            name: name.to_string(),
            grant: grant.map(String::from),
        };
        self.control_ok(topic, owner, req, timeout_ms).await
    }

    /// List subscription names on a topic.
    pub async fn list_subscriptions(
        &self,
        topic: &str,
        owner: &str,
        grant: Option<&str>,
        timeout_ms: u64,
    ) -> Result<Vec<String>> {
        message::validate_topic(topic)?;
        let req = ControlRequest::ListSubscriptions {
            grant: grant.map(String::from),
        };
        match self.control(topic, owner, req, timeout_ms).await? {
            ControlReply::Subscriptions(s) => Ok(s),
            ControlReply::Rejected(why) => Err(anyhow!("list rejected: {why}")),
            other => Err(anyhow!("unexpected reply: {other:?}")),
        }
    }

    /// Lease up to `max_messages` messages from a subscription (server-enforced at-least-once). Each
    /// leased message must be [`ack`](Self::ack)'d before its ack deadline or it is redelivered.
    pub async fn lease(
        &self,
        topic: &str,
        owner: &str,
        sub: &str,
        max_messages: usize,
        grant: Option<&str>,
        timeout_ms: u64,
    ) -> Result<Vec<LeasedMessage>> {
        message::validate_topic(topic)?;
        let req = ControlRequest::Pull {
            name: sub.to_string(),
            max_messages,
            grant: grant.map(String::from),
        };
        match self.control(topic, owner, req, timeout_ms).await? {
            ControlReply::Pulled(msgs) => Ok(msgs),
            ControlReply::Rejected(why) => Err(anyhow!("lease rejected: {why}")),
            other => Err(anyhow!("unexpected reply: {other:?}")),
        }
    }

    /// Acknowledge a leased cursor on a subscription (it will not be redelivered).
    pub async fn ack(
        &self,
        topic: &str,
        owner: &str,
        sub: &str,
        cursor: Cursor,
        grant: Option<&str>,
        timeout_ms: u64,
    ) -> Result<()> {
        message::validate_topic(topic)?;
        let req = ControlRequest::Ack {
            name: sub.to_string(),
            cursor,
            grant: grant.map(String::from),
        };
        self.control_ok(topic, owner, req, timeout_ms).await
    }

    /// Negative-ack a leased cursor: release it for immediate redelivery.
    pub async fn nack(
        &self,
        topic: &str,
        owner: &str,
        sub: &str,
        cursor: Cursor,
        grant: Option<&str>,
        timeout_ms: u64,
    ) -> Result<()> {
        message::validate_topic(topic)?;
        let req = ControlRequest::Nack {
            name: sub.to_string(),
            cursor,
            grant: grant.map(String::from),
        };
        self.control_ok(topic, owner, req, timeout_ms).await
    }

    /// Issue a control request and require an `Ok` reply.
    async fn control_ok(
        &self,
        topic: &str,
        owner: &str,
        req: ControlRequest,
        timeout_ms: u64,
    ) -> Result<()> {
        match self.control(topic, owner, req, timeout_ms).await? {
            ControlReply::Ok => Ok(()),
            ControlReply::Rejected(why) => Err(anyhow!("owner rejected: {why}")),
            other => Err(anyhow!("unexpected reply: {other:?}")),
        }
    }

    /// Issue a control request to a topic owner and decode the reply.
    async fn control(
        &self,
        topic: &str,
        owner: &str,
        req: ControlRequest,
        timeout_ms: u64,
    ) -> Result<ControlReply> {
        let reply = self
            .ce
            .request(
                owner,
                &message::control_topic(topic),
                &req.encode()?,
                timeout_ms,
            )
            .await
            .context("control request to topic owner")?;
        ControlReply::decode(&reply)
    }
}

/// Shared state behind a [`Topic`] (the owner's writer handle for one topic).
struct TopicInner {
    ps: PubSub,
    topic: String,
    writer: Replicated<TopicLog>,
    /// The per-subscription durable ack/lease registry (single-writer, owner-held).
    subs: Replicated<SubRegistry>,
    /// The live broadcast stream the owner fans messages out on. Behind a mutex because the owner is
    /// the sole publisher and the workers + the direct publish path share it.
    live: Mutex<Stream<Message>>,
    /// Root keys (besides this node's own key) whose capability chains the owner accepts.
    accepted_roots: Mutex<Vec<ce_identity::NodeId>>,
    /// When true, remote publishes (and subscription control) must present a valid capability.
    require_cap: AtomicBool,
    /// Flipped to `true` when the last [`Topic`] handle is dropped, stopping the background workers.
    shutdown: AtomicBool,
    /// Bounded, time-evicting idempotency cache for the ingest path (de-dups retries by reply_token /
    /// idempotency key without the unbounded-growth or wholesale-clear hazards).
    dedup: Mutex<IdempotencyCache>,
}

/// An owned topic: the writer side. Created by [`PubSub::create_topic`]. Holds the durable-log writer,
/// the subscription registry, and the live broadcast stream, and serves remote publishers' directed
/// ingest requests and consumers' control requests.
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

    /// The number of retained (not-yet-pruned) messages.
    pub fn retained(&self) -> usize {
        self.inner.writer.read(|s| s.len())
    }

    /// Require a `ce-cap` chain on every *remote* publish and subscription-control op (default: open).
    /// The owner always accepts its own [`publish`](Self::publish) calls.
    pub fn require_publish_cap(&self, require: bool) {
        self.inner.require_cap.store(require, Ordering::Relaxed);
    }

    /// Accept capability chains rooted at `root` (a 64-hex NodeId) in addition to this node's own key.
    /// This is how the multi-org authorization story works: point a topic at a partner org's root and
    /// the owner will honor chains that org issued. Returns an error if `root` is not a valid NodeId.
    pub async fn accept_root(&self, root: &str) -> Result<()> {
        let id = parse_node_id(root)?;
        let mut roots = self.inner.accepted_roots.lock().await;
        if !roots.contains(&id) {
            roots.push(id);
        }
        Ok(())
    }

    /// Names of all subscriptions on this topic (owner-local read, no round trip).
    pub fn subscription_names(&self) -> Vec<String> {
        self.inner.subs.read(|s| s.names())
    }

    /// Publish a message as the owner: append it to the durable log (assigning the next cursor) and
    /// broadcast it live to subscribers. Returns the assigned [`Cursor`]. This is the no-round-trip
    /// path the owner uses; remote publishers use [`PubSub::publish_to`].
    pub async fn publish(&self, payload: &[u8]) -> Result<Cursor> {
        message::validate_payload(payload)?;
        let msg =
            Message::new(0, self.inner.ps.node_id.clone(), payload, now_secs()).authenticated(true);
        self.append_and_fanout(msg).await
    }

    /// Publish as the owner with ordering key / attributes.
    pub async fn publish_with(&self, payload: &[u8], opts: &PublishOptions) -> Result<Cursor> {
        message::validate_payload(payload)?;
        opts.validate()?;
        let mut msg =
            Message::new(0, self.inner.ps.node_id.clone(), payload, now_secs()).authenticated(true);
        msg.ordering_key = opts.ordering_key.clone();
        msg.attributes = opts.attributes.clone();
        self.append_and_fanout(msg).await
    }

    /// Append `msg` (with its cursor stamped here under the writer's current state) to the durable
    /// log, then broadcast it live. Shared by the owner's direct publish and the ingest handler.
    async fn append_and_fanout(&self, mut msg: Message) -> Result<Cursor> {
        // Stamp the next absolute cursor under the writer's current state, then append durably.
        let cursor = self.inner.writer.read(|s| s.next_cursor());
        msg.cursor = cursor;
        self.inner
            .writer
            .propose(LogOp::Append(msg.clone()))
            .await
            .context("durable append")?;
        // Live fan-out (best-effort): broadcast the stored message on the live stream. A failure here
        // is logged but does not fail the publish — the message is already durable and pullable.
        if let Err(e) = self.inner.live.lock().await.publish(&msg).await {
            tracing::warn!(topic = %self.inner.topic, "live fan-out failed (message is durable): {e:#}");
        }
        Ok(cursor)
    }

    /// Drop every retained message with `cursor <= up_to` (retention). Pullers that bootstrap after a
    /// prune get the surviving tail (or, once the owner snapshots, the snapshot then the tail).
    pub async fn prune_to(&self, up_to: Cursor) -> Result<()> {
        self.inner
            .writer
            .propose(LogOp::PruneTo(up_to))
            .await
            .context("prune")?;
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
    /// appends + fans out, and replies with the assigned cursor. De-dup is by `reply_token` and the
    /// publisher's idempotency key via a bounded, time-evicting [`IdempotencyCache`]; a retry after a
    /// lost reply returns the original cursor without double-appending.
    fn serve_ingest(&self) {
        let topic = self.clone();
        let ingest = message::ingest_topic(&self.inner.topic);
        tokio::spawn(async move {
            if let Err(e) = topic.inner.ps.ce.subscribe(&ingest).await {
                tracing::warn!(topic = %topic.inner.topic, "ingest subscribe failed: {e:#}");
            }
            loop {
                if topic.inner.shutdown.load(Ordering::Relaxed) {
                    return;
                }
                if let Ok(msgs) = topic.inner.ps.ce.messages().await {
                    for m in msgs {
                        if m.topic != ingest {
                            continue;
                        }
                        let Some(token) = m.reply_token else { continue };
                        let reply = topic.handle_ingest(token, &m.payload_hex, &m.from).await;
                        if let Some(reply) = reply
                            && let Ok(bytes) = reply.encode()
                        {
                            let _ = topic.inner.ps.ce.reply(token, &bytes).await;
                        }
                    }
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });
    }

    /// Decode, de-dup, authorize, and apply one remote ingest request. Returns `Some(reply)` to send,
    /// or `None` when this `reply_token` was already replied to in a previous poll (so we do not
    /// re-reply). De-dup key precedence: the publisher's idempotency key (so a retry on a *new*
    /// reply_token is still deduped), else the reply_token itself (so a re-delivered inbox frame is
    /// deduped within the TTL window).
    async fn handle_ingest(
        &self,
        token: u64,
        payload_hex: &str,
        publisher: &str,
    ) -> Option<IngestReply> {
        let now = now_secs();
        // First, decode just enough to find the idempotency key (the body decode also bounds size).
        let req = match IngestRequest::decode_hex(payload_hex) {
            Ok(r) => r,
            Err(e) => return Some(IngestReply::Rejected(format!("bad request: {e}"))),
        };
        let dedup_key = req
            .idempotency_key
            .clone()
            .unwrap_or_else(|| format!("tok:{token}"));

        // De-dup: if we have already handled this key, return the original outcome and do not append.
        {
            let mut cache = self.inner.dedup.lock().await;
            if let Some(seen) = cache.get(&dedup_key, now) {
                return match seen {
                    Seen::Cursor(c) => Some(IngestReply::Accepted(c)),
                    // A previously-rejected request: re-reply with a stable rejection.
                    Seen::Handled => Some(IngestReply::Rejected("duplicate request".into())),
                };
            }
        }

        let outcome = self.apply_ingest(req, publisher, now).await;
        // Record the outcome so a retry within the TTL is deduped.
        {
            let mut cache = self.inner.dedup.lock().await;
            let seen = match &outcome {
                Ok(c) => Seen::Cursor(*c),
                Err(_) => Seen::Handled,
            };
            cache.insert(dedup_key, seen, now);
        }
        Some(match outcome {
            Ok(c) => IngestReply::Accepted(c),
            Err(why) => IngestReply::Rejected(why),
        })
    }

    /// Authorize and append one decoded ingest request, returning the assigned cursor or a rejection
    /// reason string (safe to return to the publisher).
    async fn apply_ingest(
        &self,
        req: IngestRequest,
        publisher: &str,
        now: u64,
    ) -> Result<Cursor, String> {
        let mut authenticated = false;
        if self.inner.require_cap.load(Ordering::Relaxed) {
            let token = req.grant.as_deref().ok_or_else(|| {
                "topic requires a pubsub:publish capability but none was presented".to_string()
            })?;
            self.authorize(publisher, caps::ABILITY_PUBLISH, token, now)
                .await?;
            authenticated = true;
        }

        // Validate the optional ordering key / attributes before they touch the durable log.
        if let Some(k) = &req.ordering_key
            && k.len() > message::MAX_ATTRIBUTE_LEN
        {
            return Err(format!("ordering key too long ({} bytes)", k.len()));
        }
        message::validate_attributes(&req.attributes).map_err(|e| e.to_string())?;

        let payload = hex::decode(&req.payload_hex).map_err(|e| format!("bad payload hex: {e}"))?;
        message::validate_payload(&payload).map_err(|e| e.to_string())?;

        let mut msg =
            Message::new(0, publisher.to_string(), &payload, now).authenticated(authenticated);
        msg.ordering_key = req.ordering_key;
        msg.attributes = req.attributes;
        self.append_and_fanout(msg)
            .await
            .map_err(|e| format!("append failed: {e:#}"))
    }

    /// Verify a presented `ce-cap` chain for `ability` on this topic, issued by the requester, against
    /// this node's own key and any [`accept_root`](Self::accept_root)ed roots, honoring on-chain
    /// revocation (fail-closed if revocation cannot be consulted).
    async fn authorize(
        &self,
        requester_hex: &str,
        ability: &str,
        token: &str,
        now: u64,
    ) -> Result<(), String> {
        let requester =
            parse_node_id(requester_hex).map_err(|e| format!("bad publisher node id: {e}"))?;
        let self_id =
            parse_node_id(&self.inner.ps.node_id).map_err(|e| format!("bad owner node id: {e}"))?;
        // Fail-closed on revocation: if we cannot consult the on-chain revoked set, reject rather than
        // silently honoring a possibly-revoked capability.
        let revoked = self
            .fetch_revoked()
            .await
            .map_err(|e| format!("cannot verify revocation (fail-closed): {e}"))?;
        let roots = self.inner.accepted_roots.lock().await.clone();
        caps::verify_link(
            &self_id,
            &roots,
            &[],
            now,
            &requester,
            ability,
            &self.inner.topic,
            token,
            &|issuer, nonce| revoked.iter().any(|(i, n)| i == issuer && *n == nonce),
        )
    }

    /// Fetch the on-chain revoked `(issuer, nonce)` set (as raw node ids). Returns an error if the
    /// node API is unreachable so callers can fail closed (an outage must NOT silently honor possibly-
    /// revoked capabilities).
    async fn fetch_revoked(&self) -> Result<Vec<(ce_identity::NodeId, u64)>> {
        let set = self
            .inner
            .ps
            .ce
            .revoked()
            .await
            .context("querying on-chain revocation set")?;
        Ok(set
            .into_iter()
            .filter_map(|(issuer_hex, nonce)| parse_node_id(&issuer_hex).ok().map(|id| (id, nonce)))
            .collect())
    }

    /// Spawn the control loop: serve subscription create/delete/list, high-cursor, and the
    /// lease/ack/nack protocol over the topic's control topic. Single-writer over the [`SubRegistry`],
    /// so all cursor/lease arithmetic is linearized.
    fn serve_control(&self) {
        let topic = self.clone();
        let ctl = message::control_topic(&self.inner.topic);
        tokio::spawn(async move {
            if let Err(e) = topic.inner.ps.ce.subscribe(&ctl).await {
                tracing::warn!(topic = %topic.inner.topic, "control subscribe failed: {e:#}");
            }
            // De-dup control replies by reply_token within a TTL window so a re-delivered inbox frame
            // is not handled twice (ack/nack/create are otherwise idempotent, but lease is not).
            let mut handled: dedup::TokenSet = dedup::TokenSet::new(DEDUP_CAPACITY, DEDUP_TTL_SECS);
            loop {
                if topic.inner.shutdown.load(Ordering::Relaxed) {
                    return;
                }
                if let Ok(msgs) = topic.inner.ps.ce.messages().await {
                    for m in msgs {
                        if m.topic != ctl {
                            continue;
                        }
                        let Some(token) = m.reply_token else { continue };
                        let now = now_secs();
                        if handled.seen(token, now) {
                            continue;
                        }
                        let reply = topic.handle_control(&m.payload_hex, &m.from, now).await;
                        if let Ok(bytes) = reply.encode() {
                            let _ = topic.inner.ps.ce.reply(token, &bytes).await;
                        }
                    }
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });
    }

    /// Decode, authorize, and apply one control request, returning the reply to send.
    async fn handle_control(&self, payload_hex: &str, requester: &str, now: u64) -> ControlReply {
        let bytes = match hex::decode(payload_hex) {
            Ok(b) => b,
            Err(e) => return ControlReply::Rejected(format!("bad request hex: {e}")),
        };
        let req = match ControlRequest::decode(&bytes) {
            Ok(r) => r,
            Err(e) => return ControlReply::Rejected(format!("bad request body: {e}")),
        };

        // HighCursor is always allowed (it is metadata a puller needs to converge honestly, and an
        // open or cap-gated topic both expose it — it leaks only the count, not the contents).
        if let ControlRequest::HighCursor { .. } = req {
            return ControlReply::High(self.inner.writer.read(|s| s.high_cursor()));
        }

        // Everything else is a subscribe-class op: gate it behind pubsub:subscribe when required.
        if self.inner.require_cap.load(Ordering::Relaxed) {
            let token = match req.grant() {
                Some(t) => t,
                None => {
                    return ControlReply::Rejected(
                        "topic requires a pubsub:subscribe capability but none was presented"
                            .into(),
                    );
                }
            };
            if let Err(why) = self
                .authorize(requester, caps::ABILITY_SUBSCRIBE, token, now)
                .await
            {
                return ControlReply::Rejected(why);
            }
        }

        match req {
            ControlRequest::HighCursor { .. } => unreachable!("handled above"),
            ControlRequest::CreateSubscription { name, policy, .. } => {
                if let Err(e) = policy.validate() {
                    return ControlReply::Rejected(format!("invalid policy: {e}"));
                }
                if let Err(e) = message::validate_topic(&name) {
                    return ControlReply::Rejected(format!("invalid subscription name: {e}"));
                }
                if self.inner.subs.read(|s| s.at_capacity(&name)) {
                    return ControlReply::Rejected("subscription limit reached".into());
                }
                match self
                    .inner
                    .subs
                    .propose(SubOp::Create { name, policy })
                    .await
                {
                    Ok(_) => ControlReply::Ok,
                    Err(e) => ControlReply::Rejected(format!("create failed: {e:#}")),
                }
            }
            ControlRequest::DeleteSubscription { name, .. } => {
                match self.inner.subs.propose(SubOp::Delete { name }).await {
                    Ok(_) => ControlReply::Ok,
                    Err(e) => ControlReply::Rejected(format!("delete failed: {e:#}")),
                }
            }
            ControlRequest::ListSubscriptions { .. } => {
                ControlReply::Subscriptions(self.inner.subs.read(|s| s.names()))
            }
            ControlRequest::Pull {
                name, max_messages, ..
            } => self.handle_lease(&name, max_messages, now).await,
            ControlRequest::Ack { name, cursor, .. } => {
                if self.inner.subs.read(|s| s.get(&name).is_none()) {
                    return ControlReply::Rejected(format!("no such subscription '{name}'"));
                }
                match self.inner.subs.propose(SubOp::Ack { name, cursor }).await {
                    Ok(_) => ControlReply::Ok,
                    Err(e) => ControlReply::Rejected(format!("ack failed: {e:#}")),
                }
            }
            ControlRequest::Nack { name, cursor, .. } => {
                if self.inner.subs.read(|s| s.get(&name).is_none()) {
                    return ControlReply::Rejected(format!("no such subscription '{name}'"));
                }
                match self.inner.subs.propose(SubOp::Nack { name, cursor }).await {
                    Ok(_) => ControlReply::Ok,
                    Err(e) => ControlReply::Rejected(format!("nack failed: {e:#}")),
                }
            }
        }
    }

    /// Plan and apply a lease for `name`, returning the leased messages (with their delivery-attempt
    /// numbers) and routing any newly dead-lettered messages to the dead-letter topic.
    async fn handle_lease(&self, name: &str, max_messages: usize, now: u64) -> ControlReply {
        if self.inner.subs.read(|s| s.get(name).is_none()) {
            return ControlReply::Rejected(format!("no such subscription '{name}'"));
        }
        let (high, floor) = self.inner.writer.read(|s| (s.high_cursor(), s.floor()));
        let plan = self
            .inner
            .subs
            .read(|s| s.plan_lease(name, high, floor, now, max_messages));
        let Some((op, outcome)) = plan else {
            return ControlReply::Pulled(Vec::new());
        };
        if let Err(e) = self.inner.subs.propose(op).await {
            return ControlReply::Rejected(format!("lease failed: {e:#}"));
        }

        // Route dead-lettered messages to the DLQ topic (best-effort: a DLQ publish failure is logged
        // but the lease still commits, since the messages are already marked dead and skipped).
        if !outcome.dead_lettered.is_empty() {
            self.route_dead_letters(&outcome.dead_lettered).await;
        }

        // Resolve each leased cursor to its message from the durable log.
        let mut leased = Vec::with_capacity(outcome.leased.len());
        self.inner.writer.read(|s| {
            for (cursor, attempt) in &outcome.leased {
                if let Some(msg) = s.get(*cursor) {
                    leased.push(LeasedMessage {
                        message: msg,
                        delivery_attempt: *attempt,
                    });
                }
            }
        });
        ControlReply::Pulled(leased)
    }

    /// Republish dead-lettered messages onto the dead-letter topic's live stream and (if it exists) a
    /// durable DLQ. Best-effort; a failure is logged, never fatal to the lease.
    async fn route_dead_letters(&self, cursors: &[Cursor]) {
        let dlq = message::dead_letter_topic(&self.inner.topic);
        let dlq_live_name = format!("{}@{}", message::live_topic(&dlq), self.inner.ps.node_id);
        let stream = match self.inner.ps.coord.stream::<Message>(&dlq_live_name).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(topic = %self.inner.topic, "dead-letter stream open failed: {e:#}");
                return;
            }
        };
        let msgs: Vec<Message> = self
            .inner
            .writer
            .read(|s| cursors.iter().filter_map(|c| s.get(*c)).collect());
        for m in msgs {
            if let Err(e) = stream.publish(&m).await {
                tracing::warn!(topic = %self.inner.topic, "dead-letter publish failed: {e:#}");
            }
        }
    }
}

impl Drop for Topic {
    fn drop(&mut self) {
        // When the last handle goes away (strong_count is 1 here because `self` still holds one Arc),
        // signal the background workers to stop on their next poll.
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.shutdown.store(true, Ordering::Relaxed);
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
/// message the owner broadcasts (that matches the optional attribute filter).
pub struct Subscription {
    stream: Stream<Message>,
    filter: AttributeFilter,
}

impl Subscription {
    /// Await the next live message matching the filter, or `None` once the underlying stream closes.
    pub async fn recv(&mut self) -> Option<Message> {
        loop {
            let msg = self.stream.next().await?;
            if self.filter.is_empty() || self.filter.matches(&msg) {
                return Some(msg);
            }
            // Filtered out: keep waiting for the next matching message.
        }
    }
}

/// The result of a durable [`pull`](PubSub::pull): every message replayed from the requested cursor,
/// plus enough metadata to paginate honestly and to detect an incomplete (non-converged) replay.
#[derive(Debug, Clone)]
pub struct Replay {
    messages: Vec<Message>,
    high: Cursor,
    next: Cursor,
    target: Option<Cursor>,
    converged: bool,
    truncated: bool,
    floor: Cursor,
    missing: u64,
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

    /// The replica's highest cursor at pull time.
    pub fn high_cursor(&self) -> Cursor {
        self.high
    }

    /// The cursor to pass as `from` on a later pull to continue exactly where this replay ended. When
    /// the result was truncated to the page limit, this is the last returned message's cursor (more
    /// remains); otherwise it equals the high-water cursor.
    pub fn next_cursor(&self) -> Cursor {
        self.next
    }

    /// The owner's authoritative high-water cursor, if it could be reached (the convergence target).
    /// `None` means the owner was unreachable and convergence fell back to a quiet window — treat the
    /// result as best-effort.
    pub fn target(&self) -> Option<Cursor> {
        self.target
    }

    /// `true` iff the replica provably caught up to the owner's high-water cursor. When `false`, the
    /// replay may be incomplete (the at-least-once contract was not met within the deadline); the
    /// caller should retry or surface [`missing`](Self::missing).
    pub fn converged(&self) -> bool {
        self.converged
    }

    /// `true` iff more messages existed at the requested cursor than the page limit allowed; continue
    /// from [`next_cursor`](Self::next_cursor).
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// The retention floor at pull time: messages with `cursor <= floor` were pruned and can only be
    /// recovered from a snapshot taken before the prune. If `from < floor`, the caller missed messages
    /// to durable retention, not to a transient lag.
    pub fn floor(&self) -> Cursor {
        self.floor
    }

    /// How many messages the owner says exist beyond what this replica replayed (`target - high`). `0`
    /// when fully converged; non-zero signals an incomplete replay (see [`converged`](Self::converged)).
    pub fn missing(&self) -> u64 {
        self.missing
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
    use std::collections::BTreeMap;

    fn replay(messages: Vec<Message>, high: Cursor) -> Replay {
        let next = messages.last().map(|m| m.cursor).unwrap_or(0);
        Replay {
            messages,
            high,
            next,
            target: Some(high),
            converged: true,
            truncated: false,
            floor: 0,
            missing: 0,
        }
    }

    #[test]
    fn ingest_reply_roundtrips() {
        for reply in [
            IngestReply::Accepted(7),
            IngestReply::Rejected("nope".into()),
        ] {
            let bytes = reply.encode().unwrap();
            let back = IngestReply::decode(&bytes).unwrap();
            assert_eq!(reply, back);
        }
    }

    #[test]
    fn ingest_request_roundtrips() {
        let req = IngestRequest {
            payload_hex: hex::encode(b"hi"),
            grant: Some("deadbeef".into()),
            idempotency_key: Some("k".into()),
            ordering_key: None,
            attributes: BTreeMap::new(),
        };
        let bytes = req.encode().unwrap();
        let back = IngestRequest::decode(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn parse_node_id_validates_length() {
        assert!(parse_node_id(&"ab".repeat(32)).is_ok()); // 64 hex chars
        assert!(parse_node_id("xyz").is_err());
        assert!(parse_node_id(&"ab".repeat(10)).is_err());
    }

    #[test]
    fn replay_exposes_messages_and_pagination() {
        let r = replay(
            vec![Message::new(2, "n", b"a", 1), Message::new(3, "n", b"b", 2)],
            3,
        );
        assert_eq!(r.len(), 2);
        assert_eq!(r.high_cursor(), 3);
        assert_eq!(r.next_cursor(), 3);
        assert!(!r.is_empty());
        assert!(r.converged());
        assert!(!r.truncated());
        assert_eq!(r.missing(), 0);
        assert_eq!(r.messages()[0].cursor, 2);
    }

    #[test]
    fn replay_reports_incomplete_when_not_converged() {
        let r = Replay {
            messages: vec![Message::new(1, "n", b"a", 1)],
            high: 1,
            next: 1,
            target: Some(5),
            converged: false,
            truncated: false,
            floor: 0,
            missing: 4,
        };
        assert!(!r.converged());
        assert_eq!(r.missing(), 4);
        assert_eq!(r.target(), Some(5));
    }
}
