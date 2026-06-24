//! `ce-pubsub` — CLI for managed Pub/Sub over the CE mesh with durable replay.
//!
//! Verbs:
//! - `create-topic <topic>` — own a topic (become its durable-log writer) and idle, serving
//!   publishers and pullers. Run this on the owner node.
//! - `publish <topic> <msg>` — publish a message. As the owner (`--own`) it appends + fans out
//!   directly; otherwise it sends a directed request to `--owner <node-id>`.
//! - `subscribe <topic> --owner <node-id>` — live tail of a topic (at-most-once), prints each
//!   message as it arrives.
//! - `pull <topic> --owner <node-id> --from <cursor>` — durable replay from a cursor (at-least-once).
//! - `grant <topic>` — mint a `pubsub:publish`/`pubsub:subscribe` capability token (offline).
//! - `inspect <token>` — show a capability token's abilities + topic scope (offline).

use anyhow::{Context, Result};
use ce_pubsub::{AttributeFilter, PubSub, PublishOptions, SubscriptionPolicy, caps};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(
    name = "ce-pubsub",
    about = "Managed Pub/Sub over CE mesh gossip + a durable ce-coord replay log",
    version
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Own a topic: become its durable-log writer and idle, serving publishers and pullers.
    CreateTopic {
        /// Topic name.
        topic: String,
        /// Require a pubsub:publish capability on every remote publish (default: open).
        #[arg(long)]
        require_cap: bool,
    },
    /// Publish a message to a topic.
    Publish {
        /// Topic name.
        topic: String,
        /// Message text (UTF-8). For binary, pipe via --file.
        message: Option<String>,
        /// Read the payload from a file instead of the <message> argument.
        #[arg(long)]
        file: Option<PathBuf>,
        /// Publish as the owner of this topic (append directly, no round trip). Requires this node
        /// to be the topic owner and to keep running long enough to fan out.
        #[arg(long)]
        own: bool,
        /// Topic owner's NodeId hex (required unless --own).
        #[arg(long)]
        owner: Option<String>,
        /// Capability token authorizing publish (for a cap-gated topic).
        #[arg(long)]
        grant: Option<String>,
        /// Per-key FIFO ordering key to attach to the message.
        #[arg(long)]
        ordering_key: Option<String>,
        /// Message attribute as `key=value` (repeatable) for subscription-side filtering.
        #[arg(long = "attr", value_parser = parse_attr)]
        attr: Vec<(String, String)>,
        /// Idempotency key: a retry carrying the same key appends only once (remote publish only).
        #[arg(long)]
        idempotency_key: Option<String>,
        /// Reply timeout in milliseconds for a remote publish.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
    },
    /// Live-tail a topic (at-most-once): print each message as the owner broadcasts it.
    Subscribe {
        /// Topic name.
        topic: String,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Exit after receiving this many messages (default: run until interrupted).
        #[arg(long)]
        count: Option<usize>,
        /// Only show messages matching this attribute filter, e.g. `kind="order" region="eu"`.
        #[arg(long)]
        filter: Option<String>,
    },
    /// Durably replay a topic from a cursor (at-least-once).
    Pull {
        /// Topic name.
        topic: String,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Replay every message with cursor greater than this (0 = from the beginning).
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
    /// Mint a capability token granting publish (or subscribe) on a topic scope. Works offline.
    Grant {
        /// Topic (or topic prefix) to scope the token to.
        topic: String,
        /// Grant subscribe instead of publish.
        #[arg(long)]
        subscribe: bool,
        /// Expiry in seconds from now (0 = never).
        #[arg(long, default_value_t = 3600)]
        expires_in: u64,
        /// Bind the token to a specific holder NodeId (default: an open bearer link to self).
        #[arg(long)]
        audience: Option<String>,
        /// CE data dir holding the owner identity (default: CE default data dir).
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Inspect a capability token: its abilities and topic scope. Works offline.
    Inspect {
        /// The token (hex).
        token: String,
    },
    /// Create (or update) a durable, server-tracked subscription on a topic owned by `--owner`.
    CreateSubscription {
        /// Topic name.
        topic: String,
        /// Subscription name.
        name: String,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Ack deadline in seconds (1..=600): a leased message is redelivered if not acked in time.
        #[arg(long, default_value_t = 30)]
        ack_deadline: u64,
        /// Max delivery attempts before a message is dead-lettered (0 = never dead-letter).
        #[arg(long, default_value_t = 5)]
        max_attempts: u32,
        /// Capability token authorizing subscribe (for a cap-gated topic).
        #[arg(long)]
        grant: Option<String>,
        /// Control round-trip timeout in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
    },
    /// Lease a batch of messages from a subscription, print them, and (optionally) ack them.
    Lease {
        /// Topic name.
        topic: String,
        /// Subscription name.
        name: String,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Max messages to lease.
        #[arg(long, default_value_t = 10)]
        max: usize,
        /// Automatically ack each leased message after printing it.
        #[arg(long)]
        auto_ack: bool,
        /// Capability token authorizing subscribe (for a cap-gated topic).
        #[arg(long)]
        grant: Option<String>,
        /// Control round-trip timeout in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
    },
    /// Acknowledge a leased cursor on a subscription.
    Ack {
        /// Topic name.
        topic: String,
        /// Subscription name.
        name: String,
        /// Cursor to acknowledge.
        cursor: u64,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Capability token (for a cap-gated topic).
        #[arg(long)]
        grant: Option<String>,
        /// Control round-trip timeout in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
    },
    /// Negative-ack a leased cursor: release it for immediate redelivery.
    Nack {
        /// Topic name.
        topic: String,
        /// Subscription name.
        name: String,
        /// Cursor to negative-ack.
        cursor: u64,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Capability token (for a cap-gated topic).
        #[arg(long)]
        grant: Option<String>,
        /// Control round-trip timeout in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
    },
    /// Delete a server-tracked subscription.
    DeleteSubscription {
        /// Topic name.
        topic: String,
        /// Subscription name.
        name: String,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Capability token (for a cap-gated topic).
        #[arg(long)]
        grant: Option<String>,
        /// Control round-trip timeout in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
    },
    /// List subscription names on a topic.
    ListSubscriptions {
        /// Topic name.
        topic: String,
        /// Topic owner's NodeId hex.
        #[arg(long)]
        owner: String,
        /// Capability token (for a cap-gated topic).
        #[arg(long)]
        grant: Option<String>,
        /// Control round-trip timeout in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
    },
}

/// Parse a `key=value` attribute argument.
fn parse_attr(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("attribute must be key=value (got '{s}')")),
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn default_data_dir() -> PathBuf {
    directories::ProjectDirs::from("", "", "ce")
        .map(|p| p.data_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

async fn connect() -> Result<PubSub> {
    PubSub::connect()
        .await
        .context("connecting to the local CE node")
}

fn parse_audience(
    audience: &Option<String>,
    owner: &ce_identity::Identity,
) -> Result<ce_identity::NodeId> {
    match audience {
        None => Ok(owner.node_id()),
        Some(hex_id) => {
            let bytes = hex::decode(hex_id.trim()).context("audience is not valid hex")?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("audience must be 32 bytes (64 hex chars)"))?;
            Ok(arr)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ce_pubsub=info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.cmd {
        Cmd::CreateTopic { topic, require_cap } => {
            let ps = connect().await?;
            let t = ps.create_topic(&topic).await?;
            t.require_publish_cap(require_cap);
            println!("owning topic '{topic}' as {}", ps.node_id());
            println!(
                "publishers: ce-pubsub publish {topic} <msg> --owner {}",
                ps.node_id()
            );
            println!(
                "subscribers: ce-pubsub subscribe {topic} --owner {}",
                ps.node_id()
            );
            if require_cap {
                println!(
                    "publish requires a pubsub:publish capability (mint with: ce-pubsub grant {topic})"
                );
            }
            println!("(serving; press Ctrl-C to stop)");
            // Idle so the durable-log writer and ingest worker keep serving.
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }

        Cmd::Publish {
            topic,
            message,
            file,
            own,
            owner,
            grant,
            ordering_key,
            attr,
            idempotency_key,
            timeout_ms,
        } => {
            let payload = match (file, message) {
                (Some(path), _) => {
                    std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?
                }
                (None, Some(m)) => m.into_bytes(),
                (None, None) => anyhow::bail!("provide a <message> argument or --file"),
            };
            let mut opts = PublishOptions::new();
            if let Some(k) = ordering_key {
                opts = opts.ordering_key(k);
            }
            for (k, v) in attr {
                opts = opts.attribute(k, v);
            }
            let ps = connect().await?;
            if own {
                // --own opens a fresh single-writer for the topic for this one publish. This is
                // correct only when no long-lived `create-topic` owner is already running in another
                // process: two concurrent writers race on the single-writer log. Prefer running
                // `create-topic` once and publishing via `--owner <node-id>` from everywhere else.
                eprintln!(
                    "note: --own opens a one-shot writer; do not run it while a `create-topic {topic}` owner is live"
                );
                let t = ps.create_topic(&topic).await?;
                let cursor = t.publish_with(&payload, &opts).await?;
                // Give live fan-out a brief moment to leave the node before we exit.
                tokio::time::sleep(Duration::from_millis(200)).await;
                println!("published to '{topic}' at cursor {cursor} (as owner)");
            } else {
                let owner =
                    owner.context("remote publish requires --owner <node-id> (or use --own)")?;
                let cursor = ps
                    .publish_to_with(
                        &topic,
                        &owner,
                        &payload,
                        grant.as_deref(),
                        &opts,
                        idempotency_key.as_deref(),
                        timeout_ms,
                    )
                    .await?;
                println!("published to '{topic}' at cursor {cursor} (via owner {owner})");
            }
        }

        Cmd::Subscribe {
            topic,
            owner,
            count,
            filter,
        } => {
            let ps = connect().await?;
            let mut sub = match filter {
                Some(f) => {
                    let parsed = AttributeFilter::parse(&f).context("parsing --filter")?;
                    ps.subscribe_filtered(&topic, &owner, parsed).await?
                }
                None => ps.subscribe(&topic, &owner).await?,
            };
            println!("subscribed to '{topic}' (owner {owner}); live tail, Ctrl-C to stop");
            let mut seen = 0usize;
            while let Some(msg) = sub.recv().await {
                println!(
                    "[{}] {} :: {}",
                    msg.cursor,
                    &msg.publisher[..msg.publisher.len().min(12)],
                    msg.text()
                );
                seen += 1;
                if count.is_some_and(|limit| seen >= limit) {
                    break;
                }
            }
        }

        Cmd::Pull { topic, owner, from } => {
            let ps = connect().await?;
            let replay = ps.pull(&topic, &owner, from).await?;
            for msg in replay.messages() {
                println!(
                    "[{}] {} :: {}",
                    msg.cursor,
                    &msg.publisher[..msg.publisher.len().min(12)],
                    msg.text()
                );
            }
            eprintln!(
                "replayed {} message(s) from cursor {from}; topic high-water = {}",
                replay.len(),
                replay.high_cursor()
            );
            if replay.truncated() {
                eprintln!(
                    "more remain: re-run with --from {} to continue (page limit reached)",
                    replay.next_cursor()
                );
            }
            if !replay.converged() {
                eprintln!(
                    "warning: replica did not provably converge ({} message(s) may still be missing); \
                     retry to get the complete tail",
                    replay.missing()
                );
            }
        }

        Cmd::Grant {
            topic,
            subscribe,
            expires_in,
            audience,
            data_dir,
        } => {
            let dir = data_dir.unwrap_or_else(default_data_dir);
            let owner =
                ce_identity::Identity::load_or_generate(&dir).context("loading owner identity")?;
            let aud = parse_audience(&audience, &owner)?;
            let ability = if subscribe {
                caps::ABILITY_SUBSCRIBE
            } else {
                caps::ABILITY_PUBLISH
            };
            let not_after = if expires_in == 0 {
                0
            } else {
                now() + expires_in
            };
            let nonce = now(); // unique-enough per mint; revoke by (issuer, nonce) on-chain later
            let token = caps::mint_link(&owner, aud, ability, &topic, not_after, nonce)?;
            println!("{token}");
            eprintln!(
                "token: {ability} on topic '{topic}'  expires_at={not_after}  (present this to the topic owner)"
            );
        }

        Cmd::Inspect { token } => {
            let (abilities, scope) = caps::inspect_link(&token)?;
            println!("abilities: {}", abilities.join(", "));
            println!("topic:     {scope}");
        }

        Cmd::CreateSubscription {
            topic,
            name,
            owner,
            ack_deadline,
            max_attempts,
            grant,
            timeout_ms,
        } => {
            let ps = connect().await?;
            let policy = SubscriptionPolicy {
                ack_deadline_secs: ack_deadline,
                max_delivery_attempts: max_attempts,
            };
            ps.create_subscription(&topic, &owner, &name, policy, grant.as_deref(), timeout_ms)
                .await?;
            println!("subscription '{name}' on '{topic}' (owner {owner}) created");
            println!("lease with: ce-pubsub lease {topic} {name} --owner {owner}");
        }

        Cmd::Lease {
            topic,
            name,
            owner,
            max,
            auto_ack,
            grant,
            timeout_ms,
        } => {
            let ps = connect().await?;
            let leased = ps
                .lease(&topic, &owner, &name, max, grant.as_deref(), timeout_ms)
                .await?;
            if leased.is_empty() {
                eprintln!("no messages available on '{name}'");
            }
            for lm in &leased {
                let m = &lm.message;
                println!(
                    "[{}] attempt={} {} :: {}",
                    m.cursor,
                    lm.delivery_attempt,
                    &m.publisher[..m.publisher.len().min(12)],
                    m.text()
                );
                if auto_ack {
                    ps.ack(
                        &topic,
                        &owner,
                        &name,
                        m.cursor,
                        grant.as_deref(),
                        timeout_ms,
                    )
                    .await?;
                }
            }
            if auto_ack && !leased.is_empty() {
                eprintln!("acked {} message(s)", leased.len());
            }
        }

        Cmd::Ack {
            topic,
            name,
            cursor,
            owner,
            grant,
            timeout_ms,
        } => {
            let ps = connect().await?;
            ps.ack(&topic, &owner, &name, cursor, grant.as_deref(), timeout_ms)
                .await?;
            println!("acked cursor {cursor} on '{name}'");
        }

        Cmd::Nack {
            topic,
            name,
            cursor,
            owner,
            grant,
            timeout_ms,
        } => {
            let ps = connect().await?;
            ps.nack(&topic, &owner, &name, cursor, grant.as_deref(), timeout_ms)
                .await?;
            println!("nacked cursor {cursor} on '{name}' (will be redelivered)");
        }

        Cmd::DeleteSubscription {
            topic,
            name,
            owner,
            grant,
            timeout_ms,
        } => {
            let ps = connect().await?;
            ps.delete_subscription(&topic, &owner, &name, grant.as_deref(), timeout_ms)
                .await?;
            println!("deleted subscription '{name}' on '{topic}'");
        }

        Cmd::ListSubscriptions {
            topic,
            owner,
            grant,
            timeout_ms,
        } => {
            let ps = connect().await?;
            let names = ps
                .list_subscriptions(&topic, &owner, grant.as_deref(), timeout_ms)
                .await?;
            if names.is_empty() {
                eprintln!("no subscriptions on '{topic}'");
            }
            for n in names {
                println!("{n}");
            }
        }
    }

    Ok(())
}
