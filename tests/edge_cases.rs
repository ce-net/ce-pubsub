//! Edge-case and failure-mode coverage for ce-pubsub's public surface beyond the in-module unit
//! tests: malformed payloads, topic-name boundaries, message decoding robustness, capability
//! inspection of garbage, and the durable-log cursor boundaries.

use ce_iam_core::{Caveats, Resource, SignedCapability};
use ce_coord::StateMachine;
use ce_identity::{Identity, NodeId};
use ce_pubsub::caps::{ABILITY_PUBLISH, inspect_link, mint_link, verify_link};
use ce_pubsub::dedup::{IdempotencyCache, Seen, TokenSet};
use ce_pubsub::log::{LogOp, TopicLog};
use ce_pubsub::message::{Message, ingest_topic, live_topic, log_name, validate_topic};

fn ident(tag: &str) -> Identity {
    let dir = std::env::temp_dir().join(format!("ce-pubsub-edge-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let id = Identity::load_or_generate(&dir).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    id
}

fn never(_: &NodeId, _: u64) -> bool {
    false
}

// ---------- message decoding robustness ----------

#[test]
fn message_with_corrupt_hex_payload_decodes_to_error_not_panic() {
    // Construct a message then corrupt its payload_hex via JSON round-trip.
    let m = Message::new(1, "n", b"hi", 1);
    let mut v: serde_json::Value = serde_json::to_value(&m).unwrap();
    v["payload_hex"] = serde_json::Value::String("zz not hex".into());
    let bad: Message = serde_json::from_value(v).unwrap();
    assert!(bad.data().is_err(), "corrupt payload hex must error");
    assert_eq!(
        bad.text(),
        "",
        "text() degrades to empty on bad hex, never panics"
    );
}

#[test]
fn message_empty_payload_roundtrips() {
    let m = Message::new(0, "n", b"", 0);
    assert_eq!(m.data().unwrap(), Vec::<u8>::new());
    assert_eq!(m.text(), "");
    let json = serde_json::to_string(&m).unwrap();
    let back: Message = serde_json::from_str(&json).unwrap();
    assert_eq!(back, m);
}

#[test]
fn message_lossy_utf8_text() {
    // Invalid UTF-8 bytes → text() is lossy, never panics.
    let m = Message::new(1, "n", &[0xff, 0xfe, 0x41], 1);
    let t = m.text();
    assert!(t.contains('A'), "valid byte survives lossy decode");
}

// ---------- topic-name boundaries ----------

#[test]
fn topic_length_boundary() {
    assert!(validate_topic(&"a".repeat(200)).is_ok());
    assert!(validate_topic(&"a".repeat(201)).is_err());
    assert!(validate_topic("").is_err());
}

#[test]
fn topic_charset_boundaries() {
    assert!(validate_topic("a.b-c_d:e").is_ok());
    assert!(validate_topic("space here").is_err());
    assert!(validate_topic("slash/here").is_err());
    assert!(validate_topic("emoji\u{1F600}").is_err());
}

#[test]
fn mesh_names_are_distinct_and_namespaced() {
    let (l, v, i) = (log_name("t"), live_topic("t"), ingest_topic("t"));
    assert!(l.starts_with("ce-pubsub."));
    assert!(v.starts_with("ce-pubsub."));
    assert!(i.starts_with("ce-pubsub."));
    assert_ne!(l, v);
    assert_ne!(v, i);
    assert_ne!(l, i);
}

// ---------- durable-log cursor boundaries ----------

#[test]
fn empty_log_cursor_state() {
    let log = TopicLog::default();
    assert_eq!(log.high_cursor(), 0);
    assert_eq!(log.next_cursor(), 1);
    assert_eq!(log.floor(), 0);
    assert!(log.is_empty());
    assert_eq!(log.since(0).len(), 0);
}

#[test]
fn prune_to_zero_is_noop() {
    let mut log = TopicLog::default();
    for c in 1..=3 {
        log.apply(LogOp::Append(Message::new(c, "w", b"x", c)));
    }
    log.apply(LogOp::PruneTo(0));
    assert_eq!(log.len(), 3, "prune to 0 drops nothing");
    assert_eq!(log.floor(), 0);
}

#[test]
fn prune_beyond_high_drops_all() {
    let mut log = TopicLog::default();
    for c in 1..=3 {
        log.apply(LogOp::Append(Message::new(c, "w", b"x", c)));
    }
    log.apply(LogOp::PruneTo(999));
    assert!(log.is_empty(), "pruning past high drops everything");
    assert_eq!(log.floor(), 3, "floor advanced by the 3 dropped");
    // Cursors stay absolute: a new append still gets cursor 4.
    assert_eq!(log.next_cursor(), 4);
}

// ---------- capability inspection robustness ----------

#[test]
fn inspect_garbage_tokens_error_not_panic() {
    assert!(inspect_link("").is_err());
    assert!(inspect_link("not hex @@").is_err());
    assert!(inspect_link("deadbeef").is_err());
}

#[test]
fn verify_garbage_token_errors() {
    let owner = ident("v");
    let r = verify_link(
        &owner.node_id(),
        &[],
        &[],
        1000,
        &owner.node_id(),
        ABILITY_PUBLISH,
        "t",
        "not a token",
        &never,
    );
    assert!(
        r.is_err(),
        "garbage token must be a clean error, not a panic"
    );
}

#[test]
fn empty_scope_link_covers_any_topic() {
    // A link minted with an empty topic scope ("") authorizes any topic (prefix "" matches all).
    let owner = ident("e");
    let token = mint_link(&owner, owner.node_id(), ABILITY_PUBLISH, "", 0, 1).unwrap();
    for topic in ["a", "orders", "anything.at.all"] {
        assert!(
            verify_link(
                &owner.node_id(),
                &[],
                &[],
                1000,
                &owner.node_id(),
                ABILITY_PUBLISH,
                topic,
                &token,
                &never,
            )
            .is_ok()
        );
    }
}

// ---------- multi-hop capability attenuation ----------

/// Build a 2-link delegation chain: owner -> holder (scope `parent_scope`), holder -> delegate
/// (scope `child_scope`), both granting `pubsub:publish`. Returns the encoded chain token.
fn delegated_chain(
    owner: &Identity,
    holder: &Identity,
    delegate: NodeId,
    parent_scope: &str,
    child_scope: &str,
) -> String {
    let root = SignedCapability::issue(
        owner,
        holder.node_id(),
        vec![ABILITY_PUBLISH.to_string()],
        Resource::Node(owner.node_id()),
        Caveats {
            not_after: 0,
            path_prefix: Some(parent_scope.to_string()),
            ..Default::default()
        },
        1,
        None,
    );
    let leaf = SignedCapability::issue(
        holder,
        delegate,
        vec![ABILITY_PUBLISH.to_string()],
        Resource::Node(owner.node_id()),
        Caveats {
            not_after: 0,
            path_prefix: Some(child_scope.to_string()),
            ..Default::default()
        },
        2,
        Some(root.id()),
    );
    ce_iam_core::encode_chain(&[root, leaf])
}

#[test]
fn multi_hop_same_scope_delegation_verifies() {
    // A 2-link chain re-delegating the SAME topic scope must verify for the leaf audience. This is the
    // realistic ce-pubsub case: topics use `.` separators (`orders.eu`), while ce-cap's path_prefix
    // attenuation narrows on `/` segment boundaries — so a `.`-topic prefix is re-delegated whole, and
    // the app-level topic-prefix scope (topic_allows) is what gates the leaf against the request.
    let owner = ident("mh-owner");
    let holder = ident("mh-holder");
    let delegate = ident("mh-delegate");

    let token = delegated_chain(&owner, &holder, delegate.node_id(), "orders", "orders");

    // The delegate may publish within the scope and any deeper `.`-prefix the leaf scope covers.
    assert!(
        verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &delegate.node_id(),
            ABILITY_PUBLISH,
            "orders",
            &token,
            &never,
        )
        .is_ok(),
        "delegate publishes within the re-delegated scope"
    );
    assert!(
        verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &delegate.node_id(),
            ABILITY_PUBLISH,
            "orders.eu",
            &token,
            &never,
        )
        .is_ok(),
        "and under the topic prefix the leaf scope covers"
    );

    // ...but NOT outside the leaf scope.
    assert!(
        verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &delegate.node_id(),
            ABILITY_PUBLISH,
            "payments",
            &token,
            &never,
        )
        .is_err(),
        "a delegated chain cannot reach a topic outside its scope"
    );
}

#[test]
fn multi_hop_path_attenuation_narrows_on_slash_boundary() {
    // ce-cap's path_prefix narrowing IS enforced on `/` segment boundaries between links. Prove it:
    // owner grants `team/`, holder may re-delegate `team/eu` (within), but NOT `team-other` (escape).
    let owner = ident("mhp-owner");
    let holder = ident("mhp-holder");
    let delegate = ident("mhp-delegate");

    let within = delegated_chain(&owner, &holder, delegate.node_id(), "team", "team/eu");
    assert!(
        verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &delegate.node_id(),
            ABILITY_PUBLISH,
            "team/eu",
            &within,
            &never,
        )
        .is_ok(),
        "re-delegating within the parent path segment verifies"
    );

    // A leaf that tries to BROADEN beyond the parent path is rejected by ce-cap attenuation.
    let escape = delegated_chain(&owner, &holder, delegate.node_id(), "team/eu", "team");
    assert!(
        verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &delegate.node_id(),
            ABILITY_PUBLISH,
            "team",
            &escape,
            &never,
        )
        .is_err(),
        "a leaf cannot broaden its path beyond the parent (attenuation is one-way)"
    );
}

#[test]
fn multi_hop_chain_rejects_wrong_audience_at_leaf() {
    let owner = ident("mh2-owner");
    let holder = ident("mh2-holder");
    let delegate = ident("mh2-delegate");
    let stranger = ident("mh2-stranger");
    let token = delegated_chain(&owner, &holder, delegate.node_id(), "t", "t");
    // A node that is not the leaf audience cannot wield the chain.
    assert!(
        verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &stranger.node_id(),
            ABILITY_PUBLISH,
            "t",
            &token,
            &never,
        )
        .is_err(),
        "only the leaf audience may use a delegated chain"
    );
}

// ---------- de-dup cache correctness across the capacity-reset boundary ----------

#[test]
fn idempotency_cache_dedups_recent_after_eviction_pressure() {
    // The exact failure the bounded cache replaces (old code cleared wholesale at capacity): a recent
    // key must stay deduped while the cache is under insertion pressure, and only the OLDEST is ever
    // evicted — never a wholesale forget of every key.
    let mut c = IdempotencyCache::new(4, 3600);
    c.insert("req-recent".into(), Seen::Cursor(42), 100);
    // Push three more distinct keys (cache holds 4). The recent key is still present and deduped.
    for i in 0..3 {
        c.insert(format!("filler-{i}"), Seen::Cursor(i), 100 + i);
    }
    assert_eq!(
        c.get("req-recent", 110),
        Some(Seen::Cursor(42)),
        "recent key survives at capacity"
    );
    // One more insert evicts the OLDEST (req-recent), not the whole set: the just-inserted fillers stay.
    c.insert("filler-3".into(), Seen::Cursor(9), 105);
    assert!(
        c.get("req-recent", 110).is_none(),
        "only the oldest key is evicted"
    );
    assert_eq!(
        c.get("filler-2", 110),
        Some(Seen::Cursor(2)),
        "newer keys are NOT forgotten"
    );
    assert_eq!(c.get("filler-3", 110), Some(Seen::Cursor(9)));
}

#[test]
fn token_set_dedups_across_capacity_and_ttl() {
    let mut s = TokenSet::new(3, 100);
    assert!(!s.seen(1, 0));
    assert!(s.seen(1, 50), "within ttl, a repeat is recognized");
    // Fill to capacity with new tokens; the oldest (1) is evicted, the rest retained.
    assert!(!s.seen(2, 60));
    assert!(!s.seen(3, 61));
    assert!(!s.seen(4, 62)); // evicts token 1
    assert!(
        !s.seen(1, 63),
        "evicted token treated as new (oldest-only eviction)"
    );
    assert!(s.seen(4, 64), "recent token still deduped");
    // TTL expiry: a token older than the ttl is new again.
    assert!(!s.seen(4, 64 + 101), "expired token is new again");
}
