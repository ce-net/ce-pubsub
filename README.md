# ce-pubsub

Managed Pub/Sub over the CE mesh — **Gossipsub topics for live fan-out** plus a **durable
`ce-coord` append log for at-least-once replay**. The Google Pub/Sub surface (topics, subscriptions,
replay from a cursor, IAM-style access control) assembled from CE primitives, with **no broker to
provision**.

This is an SDK/app-tier library + CLI built **on** CE primitives (over `ce-rs` + `ce-coord` +
`ce-cap`), not a node change.

## What it composes

| Need | CE primitive |
|---|---|
| Live topic fan-out (events) | mesh pub/sub via a `ce-coord` typed `Stream<Message>` |
| Durability + replay (at-least-once) | a `ce-coord` single-writer append log (`Replicated<TopicLog>` + `Snapshot`) |
| Cheap late-subscriber bootstrap | content-addressed log snapshots (`checkpoint`) over CE blobs |
| Who may publish / subscribe | `ce-cap` capability chains (`pubsub:publish` / `pubsub:subscribe`, topic-scoped) |
| Remote publish transport | `ce-rs` directed request/reply (`/mesh/request` → owner → `/mesh/reply`) |

## Model

A topic is global-by-`(owner_node_id, name)`. The node that **creates** a topic owns it and is the
durable-log **writer**.

- **Owner** — `create_topic(name)` opens the durable log as writer and the live broadcast stream, and
  serves remote publishers. `topic.publish(bytes)` appends to the log (assigning a monotonic
  **cursor**) and broadcasts live.
- **Publisher** (any other node) — `publish_to(name, owner, bytes, grant)` sends a directed request to
  the owner, who authorizes it (capability, if the topic is gated), appends, fans out, and replies
  with the assigned cursor.
- **Subscriber, live** — `subscribe(name, owner)` returns a `Subscription`; `recv().await` yields each
  message as the owner broadcasts it. **At-most-once** (the node inbox ring is bounded; an offline
  subscriber misses messages).
- **Subscriber, durable** — `pull(name, owner, from)` opens a read replica of the owner's log, converges
  it, and returns every message with `cursor > from`. **At-least-once**: it replays what a late or
  reconnecting subscriber missed. Pass `from = 0` to replay from the start; pass the previous
  `high_cursor()` to continue.

### Cursors and durability

Every durable message carries a 1-based `cursor` (its absolute position in the topic log). `pull
--from <cursor>` returns everything strictly after it — the at-least-once contract. The owner can
`prune_to(cursor)` for retention and `checkpoint()` to snapshot+compact the log; a fresh puller then
bootstraps from a content-addressed blob instead of replaying from message 1. Cursors stay absolute
across prunes (a surviving message keeps its original number).

### Authorization

Publish/subscribe access is a signed, attenuating `ce-cap` chain — not a central ACL. The topic owner
mints a token scoped to a topic (or topic prefix) with `pubsub:publish` or `pubsub:subscribe`, an
expiry, and an audience. A holder presents it; the owner verifies it **offline in microseconds**
(signature, attenuation, expiry, on-chain revocation) before accepting a publish. Re-delegation
(narrow the topic prefix, hand it on) is free. By default a topic is **open**; call
`Topic::require_publish_cap(true)` to gate remote publishes.

## CLI

```
# --- on the owner node ---
ce-pubsub create-topic orders                 # own the topic; idles, serving publishers + pullers
ce-pubsub create-topic orders --require-cap    # gate remote publishes behind a capability

# --- publish ---
ce-pubsub publish orders "order-1" --own                       # as the owner (append + fan out)
ce-pubsub publish orders "order-2" --owner <owner-node-id>      # from a publisher node, over the mesh
ce-pubsub publish orders --file ./payload.bin --owner <id> --grant <token>

# --- subscribe ---
ce-pubsub subscribe orders --owner <owner-node-id>             # live tail (at-most-once)
ce-pubsub subscribe orders --owner <id> --count 10             # stop after 10 messages

# --- durable replay ---
ce-pubsub pull orders --owner <owner-node-id> --from 0          # replay from the start (at-least-once)
ce-pubsub pull orders --owner <id> --from 42                    # replay everything after cursor 42

# --- capabilities (offline) ---
ce-pubsub grant orders                                          # mint a pubsub:publish token
ce-pubsub grant orders --subscribe --expires-in 86400           # subscribe token, 24h
ce-pubsub grant orders --audience <holder-node-id>             # bind to a specific holder
ce-pubsub inspect <token>                                       # show abilities + topic scope
```

`grant` and `inspect` work offline (they only touch the local identity). The other verbs need a local
CE node running on `127.0.0.1:8844`; point at a custom-data-dir node by exporting `CE_API_TOKEN`.

## Library

```rust
use ce_pubsub::PubSub;

# async fn demo() -> anyhow::Result<()> {
let ps = PubSub::connect().await?;

// owner
let topic = ps.create_topic("orders").await?;
let cursor = topic.publish(b"order-1").await?;     // -> Cursor

// late subscriber on another node: replay everything
let replay = ps.pull("orders", "owner_node_id_hex", 0).await?;
for m in replay.messages() {
    println!("{}: {}", m.cursor, m.text());
}
# Ok(()) }
```

Key types: `PubSub` (connection), `Topic` (owner/writer handle), `Subscription` (live tail),
`Replay` (durable pull result), `Message` (`cursor` + authenticated `publisher` + payload),
`caps` (mint/inspect/verify capability tokens).

## Delivery guarantees

- **Live** (`subscribe` / `Stream`): at-most-once, best-effort. Mesh gossip does not echo a node's own
  publishes back to itself, so live delivery is inherently cross-node. Use it for events/telemetry.
- **Durable** (`pull` over the append log): at-least-once. The single-writer log carries version
  numbers and repairs gaps on the reader, so a puller converges to the owner's exact tail and replays
  whatever it missed.

The two are complementary: subscribe for low-latency live delivery, pull to backfill on reconnect.

## Scaling notes (v1)

- **Single writer per topic.** One owner node holds each topic's log; if it goes offline, publishing
  stalls until it returns. Multi-writer ownership (a `ce-coord` Merged/Raft writer set) is the next
  layer and slots in without changing the publish/pull call sites.
- **Log growth.** The log is unbounded until the owner `prune_to`/`checkpoint`s. Snapshots make fresh
  pulls cheap; pair them with a retention policy on a long-lived topic.
- **Live ring is bounded.** Slow/offline live subscribers drop messages — that is exactly what `pull`
  exists to recover.

## Tests

`cargo test` runs the unit suite (pure logic — no node required): the topic-log state machine
(monotonic cursors, strict-tail `since`, prune/floor, and **snapshot+tail == full replay**), the
capability mint/verify/scope/expiry/revocation checks, the message + wire round-trips, and the
topic-name validation. Live and durable end-to-end paths require two CE nodes on the mesh (mesh gossip
does not self-echo on one node), matching how the sibling `ce-coord`/`ce-db` crates are tested.

## License

MIT — author Leif Rydenfalk.
