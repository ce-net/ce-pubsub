//! Edge-case and failure-mode coverage for ce-pubsub's public surface beyond the in-module unit
//! tests: malformed payloads, topic-name boundaries, message decoding robustness, capability
//! inspection of garbage, and the durable-log cursor boundaries.

use ce_pubsub::caps::{inspect_link, mint_link, verify_link, ABILITY_PUBLISH};
use ce_pubsub::log::{LogOp, TopicLog};
use ce_pubsub::message::{ingest_topic, live_topic, log_name, validate_topic, Message};
use ce_coord::StateMachine;
use ce_identity::{Identity, NodeId};

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
    assert_eq!(bad.text(), "", "text() degrades to empty on bad hex, never panics");
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
        &owner.node_id(), &[], &[], 1000, &owner.node_id(),
        ABILITY_PUBLISH, "t", "not a token", &never,
    );
    assert!(r.is_err(), "garbage token must be a clean error, not a panic");
}

#[test]
fn empty_scope_link_covers_any_topic() {
    // A link minted with an empty topic scope ("") authorizes any topic (prefix "" matches all).
    let owner = ident("e");
    let token = mint_link(&owner, owner.node_id(), ABILITY_PUBLISH, "", 0, 1).unwrap();
    for topic in ["a", "orders", "anything.at.all"] {
        assert!(verify_link(
            &owner.node_id(), &[], &[], 1000, &owner.node_id(),
            ABILITY_PUBLISH, topic, &token, &never,
        ).is_ok());
    }
}
