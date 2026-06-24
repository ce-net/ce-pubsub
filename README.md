# ce-pubsub

Managed Pub/Sub over the CE mesh — **Gossipsub topics for live fan-out** plus a **durable
`ce-coord` append log for at-least-once replay** and **first-class server-tracked subscriptions**
(ack/nack/lease, dead-letter, ordering keys, attribute filtering). The Google Pub/Sub surface
assembled from CE primitives, with **no broker to provision**.

This is an SDK/app-tier library + CLI built **on** CE primitives (over `ce-rs` + `ce-coord` +
`ce-cap`), not a node change. CE provides identity, mesh transport, blobs, the ledger, and the
capability verifier; ce-pubsub composes them into Pub/Sub semantics entirely app-side.

## What it composes

| Need | CE primitive |
|---|---|
| Live topic fan-out (events) | mesh pub/sub via a `ce-coord` typed `Stream<Message>` |
| Durability + replay (at-least-once) | a `ce-coord` single-writer append log (`Replicated<TopicLog>` + `Snapshot`) |
| Server-tracked subscriptions (ack state, leases) | a sibling single-writer `ce-coord` log (`SubRegistry`) |
| Cheap late-subscriber bootstrap | content-addressed log snapshots (`checkpoint`) over CE blobs |
| Who may publish / subscribe | `ce-cap` capability chains (`pubsub:publish` / `pubsub:subscribe`, topic-scoped) |
| Remote publish / control transport | `ce-rs` directed request/reply (`/mesh/request` → owner → `/mesh/reply`) |

## Model

A topic is global-by-`(owner_node_id, name)`. The node that **creates** a topic owns it and is the
durable-log **writer**.

- **Owner** — `create_topic(name)` opens the durable log as writer, the subscription registry, and the
  live broadcast stream, and serves remote publishers' ingest requests and consumers' control
  requests. `topic.publish(bytes)` appends to the log (assigning a monotonic **cursor**) and broadcasts
  live. The background workers stop when the last `Topic` handle is dropped (no leaked task).
- **Publisher** (any other node) — `publish_to(name, owner, bytes, grant)` sends a directed request to
  the owner, who authorizes it (capability, if the topic is gated), appends, fans out, and replies with
  the assigned cursor. `publish_to_with(..., idempotency_key)` makes a retry after a lost reply safe:
  the owner appends once and returns the original cursor.
- **Subscriber, live** — `subscribe(name, owner)` returns a `Subscription`; `recv().await` yields each
  message as the owner broadcasts it. **At-most-once** (the node inbox ring is bounded; an offline
  subscriber misses messages). `subscribe_filtered` adds attribute filtering.
- **Subscriber, durable pull** — `pull(name, owner, from)` opens a read replica of the owner's log,
  **honestly** converges it to the owner's high-water cursor, and returns every message with
  `cursor > from`. **At-least-once**: it replays what a late or reconnecting subscriber missed. The
  result reports `converged()` / `missing()` so an incomplete replay is never silently returned as
  complete.
- **Subscriber, server-tracked** — `create_subscription` makes a durable, named subscription with its
  own ack cursor and policy; `lease` hands out redeliverable messages under an ack deadline, `ack`
  advances the floor, `nack` releases for immediate redelivery, and messages exceeding
  `max_delivery_attempts` are routed to a dead-letter topic. This is at-least-once **enforced by the
  server**, not by client cursor bookkeeping.

### Cursors, durability, and retention

Every durable message carries a 1-based `cursor` (its absolute position in the topic log). `pull
--from <cursor>` returns everything strictly after it. The owner can `prune_to(cursor)` for retention
and `checkpoint()` to snapshot+compact the log; a fresh puller then bootstraps from a content-addressed
blob instead of replaying from message 1. Cursors stay absolute across prunes (a surviving message keeps
its original number), and the log re-stamps every append to the canonical next cursor so the
`floor`/`since`/`get` algebra can never desync.

### Honest convergence

`pull` does not guess: it asks the owner (over the control topic) for the topic's current high-water
cursor, then waits for the read replica to provably reach it. If the deadline elapses first,
`Replay::converged()` is `false` and `Replay::missing()` is the count the owner says still exists — the
caller can retry rather than treat a truncated replay as the whole topic. If the owner is unreachable,
convergence falls back to a quiet-window heuristic and `target()` is `None` (best-effort, flagged as
such).

### Subscriptions, ack/nack, leases, dead-letter

A subscription tracks a contiguous **acked floor** plus out-of-order acks held above it (acking 3 then
1 then 2 advances the floor to 3 once contiguous). `lease` returns up to `max_messages` redeliverable
cursors, each stamped with an ack deadline; an unacked lease is redelivered after the deadline with an
incremented delivery-attempt count. A message that exceeds `max_delivery_attempts` is **dead-lettered**
— republished to `<topic>.dlq` and skipped so a poison message never stalls the subscription. All
cursor/lease arithmetic is linearized by the single-writer registry.

### Ordering keys & attributes

A message can carry an `ordering_key` and a string `attributes` map (Google Pub/Sub parity). A live
subscriber can filter on attributes with an exact-match, injection-free filter grammar
(`kind="order" region="eu"`). The global single-writer cursor already gives total order; the ordering
key is preserved for per-key consumers and the attribute map for filtering without decoding payloads.

### Authorization

Publish/subscribe access is a signed, attenuating `ce-cap` chain — not a central ACL. The topic owner
mints a token scoped to a topic (or topic prefix) with `pubsub:publish` or `pubsub:subscribe`, an
expiry, and an audience. A holder presents it; the owner verifies it **offline in microseconds**
(signature, attenuation, expiry, on-chain revocation) before accepting a publish or control op.
Revocation is **fail-closed**: if the on-chain revoked set cannot be consulted, the request is
rejected rather than honored. Re-delegation (narrow the topic prefix, hand it on) is free. Point a
topic at a partner org's root with `Topic::accept_root` for cross-org chains. By default a topic is
**open**; call `Topic::require_publish_cap(true)` to gate remote publishes and control ops.

> On an **open** topic, `Message::publisher` is the transport-claimed sender and is **not**
> cryptographically verified — `Message::publisher_is_authenticated()` is `false`. Only on a cap-gated
> topic is the publisher field authenticated. Do not use the publisher field for authorization on open
> topics.

### Bounds (DoS resistance)

| Bound | Value |
|---|---|
| Max message payload | 10 MiB (`MAX_PAYLOAD_BYTES`) |
| Max attributes per message / max key+value length | 100 / 1024 bytes |
| Max idempotency key length | 256 bytes |
| Max control-request body | 64 KiB |
| Max messages per `pull` page | 1,000 default, 100,000 hard cap (paginate via `next_cursor`) |
| Max cursors per `lease` | 1,000 |
| Max subscriptions per topic | 10,000 |
| Ack deadline | 1..=600 s |

All wire decode paths reject oversized input before allocation. The ingest de-dup cache is bounded
(16,384 keys) with FIFO + 1-hour TTL eviction — never a wholesale clear.

## CLI

```
# --- on the owner node ---
ce-pubsub create-topic orders                  # own the topic; idles, serving publishers + pullers
ce-pubsub create-topic orders --require-cap    # gate remote publishes/control behind a capability

# --- publish ---
ce-pubsub publish orders "order-1" --own                                  # as the owner (append + fan out)
ce-pubsub publish orders "order-2" --owner <owner-node-id>                # from a publisher node, over the mesh
ce-pubsub publish orders --file ./payload.bin --owner <id> --grant <tok>  # binary payload + capability
ce-pubsub publish orders "eu-order" --owner <id> \
  --ordering-key eu --attr kind=order --attr region=eu \
  --idempotency-key req-42                                                # ordering/attrs + safe retry

# --- live subscribe ---
ce-pubsub subscribe orders --owner <owner-node-id>                # live tail (at-most-once)
ce-pubsub subscribe orders --owner <id> --count 10               # stop after 10 messages
ce-pubsub subscribe orders --owner <id> --filter 'kind="order"'  # attribute-filtered tail

# --- durable replay ---
ce-pubsub pull orders --owner <owner-node-id> --from 0           # replay from the start (at-least-once)
ce-pubsub pull orders --owner <id> --from 42                     # replay everything after cursor 42

# --- server-tracked subscriptions ---
ce-pubsub create-subscription orders workers --owner <id> --ack-deadline 30 --max-attempts 5
ce-pubsub list-subscriptions orders --owner <id>
ce-pubsub lease orders workers --owner <id> --max 10 --auto-ack  # lease + ack
ce-pubsub ack orders workers 7 --owner <id>                      # ack a leased cursor
ce-pubsub nack orders workers 7 --owner <id>                     # release for immediate redelivery
ce-pubsub delete-subscription orders workers --owner <id>

# --- capabilities (offline) ---
ce-pubsub grant orders                                           # mint a pubsub:publish token
ce-pubsub grant orders --subscribe --expires-in 86400            # subscribe token, 24h
ce-pubsub grant orders --audience <holder-node-id>               # bind to a specific holder
ce-pubsub inspect <token>                                        # show abilities + topic scope
```

`grant` and `inspect` work offline (they only touch the local identity). The other verbs need a local
CE node running on `127.0.0.1:8844`; point at a custom-data-dir node by exporting `CE_API_TOKEN`.

## Library

```rust
use ce_pubsub::{PubSub, PublishOptions, SubscriptionPolicy};

# async fn demo() -> anyhow::Result<()> {
let ps = PubSub::connect().await?;

// owner
let topic = ps.create_topic("orders").await?;
let cursor = topic.publish(b"order-1").await?;     // -> Cursor
topic.publish_with(b"eu", &PublishOptions::new().ordering_key("eu").attribute("region", "eu")).await?;

// server-tracked subscription on another node
let owner = "owner_node_id_hex";
ps.create_subscription("orders", owner, "workers", SubscriptionPolicy::default(), None, 30_000).await?;
let leased = ps.lease("orders", owner, "workers", 10, None, 30_000).await?;
for lm in &leased {
    // ... process lm.message ...
    ps.ack("orders", owner, "workers", lm.message.cursor, None, 30_000).await?;
}

// late subscriber: durable replay (honest convergence)
let replay = ps.pull("orders", owner, 0).await?;
if !replay.converged() { /* retry — replay.missing() still pending */ }
for m in replay.messages() { println!("{}: {}", m.cursor, m.text()); }
# Ok(()) }
```

Key types: `PubSub` (connection), `Topic` (owner/writer handle), `Subscription` (live tail), `Replay`
(durable pull result with convergence metadata), `Message` (`cursor` + publisher + payload + ordering
key + attributes), `SubscriptionPolicy` / `LeasedMessage` (server-tracked subscriptions), `caps`
(mint/inspect/verify capability tokens).

## Delivery guarantees

- **Live** (`subscribe` / `Stream`): at-most-once, best-effort. Mesh gossip does not echo a node's own
  publishes back to itself, so live delivery is inherently cross-node. Use it for events/telemetry.
- **Durable pull** (over the append log): at-least-once, with honest convergence reporting. A puller
  converges to the owner's exact tail and replays whatever it missed.
- **Server-tracked subscription** (`lease`/`ack`): at-least-once with server-enforced redelivery and
  dead-lettering — a consumer that crashes mid-processing has its leased messages redelivered.

## Implemented vs deferred

**Implemented:** topics + live fan-out; durable replay with snapshot bootstrap; honest, target-based
pull convergence with `converged()`/`missing()`; paginated pull; first-class durable subscriptions with
ack/nack, lease + ack-deadline redelivery, out-of-order acks, delivery-attempt counting, and
dead-letter routing; ordering keys + message attributes + attribute filtering; idempotent remote
publish; capability gating with attenuation, cross-org roots, and fail-closed revocation; explicit
retention (`prune_to`/`checkpoint`) and DoS bounds on every external input.

**Deferred** (documented, not faked):

- **Push (webhook) subscriptions** — only pull + live + lease exist; no worker that POSTs to an
  endpoint. The lease/ack machinery it would reuse is in place.
- **Automatic time-based retention & seek-by-timestamp** — retention is explicit (`prune_to`/
  `checkpoint`); there is no `message_retention_duration` timer or `seek(time)`.
- **Schema registry / payload validation** — payloads are opaque bytes.
- **Exactly-once delivery** — de-dup is best-effort within the idempotency-cache horizon.
- **Multi-writer / HA topic ownership** — a topic has a single owner/writer; if it goes offline,
  publishing stalls until it returns. A `ce-coord` Merged/Raft writer set slots in without changing the
  publish/pull call sites.

## Scaling notes

- **Single writer per topic.** One owner node holds each topic's log and subscription registry.
- **Log growth.** The log is unbounded until the owner `prune_to`/`checkpoint`s. Snapshots make fresh
  pulls cheap; pair them with a retention policy on a long-lived topic.
- **Live ring is bounded.** Slow/offline live subscribers drop messages — that is exactly what `pull`
  and server-tracked subscriptions exist to recover.

## Tests

`cargo test` runs the unit + integration + property + doc suites (pure logic — no node required):

- the topic-log state machine (monotonic/absolute cursors, strict-tail `since`, prune/floor, append
  re-stamping, and **snapshot+tail == full replay** over random op scripts),
- the subscription registry (lease/ack/nack, out-of-order acks, ack-deadline redelivery,
  delivery-attempt counting, dead-letter routing and floor-advance, capacity guard),
- the bounded de-dup / idempotency caches (FIFO + TTL eviction, retry-within-window dedup),
- the wire protocol (round-trips + oversize-rejection for every request/reply),
- the capability mint/verify/scope/expiry/revocation/attenuation checks,
- the message + filter model (binary-safe serde, attribute filter parse/match).

Live end-to-end paths (`tests/live.rs`) require two CE nodes on the mesh; they stand up a fresh 2-node
loopback mesh per test and **skip gracefully (logging why)** when the release `ce` binary is absent, so
the suite stays green where a node genuinely cannot run. Set `CE_NO_LIVE=1` to skip them explicitly.

See [`examples/end_to_end.rs`](examples/end_to_end.rs) for a single-process owner→publish→pull demo.

## License

MIT — author Leif Rydenfalk.
