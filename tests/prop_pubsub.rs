//! Property/fuzz tests for ce-pubsub's pure layers: the durable [`TopicLog`] state machine
//! (append/prune/since/cursor algebra and the snapshot+tail equivalence), the [`Message`] wire model
//! (binary-safe serde round-trips), topic-name validation, and the `ce-cap` publish/subscribe link
//! (attenuation can never amplify; expiry/revocation/scope honored).
//!
//! These complement the hand-picked unit tests in `src/` by searching the input space: random op
//! sequences, random binary payloads, random topic strings, and random capability scopes.

use ce_coord::{Snapshot, StateMachine};
use ce_identity::{Identity, NodeId};
use ce_pubsub::caps::{ABILITY_PUBLISH, ABILITY_SUBSCRIBE, mint_link, topic_allows, verify_link};
use ce_pubsub::log::{LogOp, TopicLog};
use ce_pubsub::message::{Message, validate_topic};
use proptest::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

fn id() -> Identity {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("ce-pubsub-prop-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let id = Identity::load_or_generate(&dir).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    id
}

fn never(_: &NodeId, _: u64) -> bool {
    false
}

/// A model of the durable log: a writer appends in cursor order; prune drops a prefix. We replay a
/// random op script against the real `TopicLog` and assert the cursor algebra holds.
fn msg(cursor: u64, body: &[u8]) -> Message {
    Message::new(cursor, "w", body, 1000 + cursor)
}

proptest! {
    // ----- Message wire model: binary-safe round-trip. -----

    #[test]
    fn message_serde_roundtrips_binary(
        cursor in any::<u64>(),
        publisher in "[0-9a-f]{0,64}",
        payload in proptest::collection::vec(any::<u8>(), 0..512),
        at in any::<u64>(),
    ) {
        let m = Message::new(cursor, publisher.clone(), &payload, at);
        // data() decodes back to the exact bytes.
        prop_assert_eq!(m.data().unwrap(), payload.clone());
        // JSON round-trip is lossless (incl cursor > 2^53).
        let json = serde_json::to_string(&m).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&back, &m);
        prop_assert_eq!(back.cursor, cursor);
        prop_assert_eq!(back.data().unwrap(), payload);
    }

    // ----- Durable log cursor algebra over a random op script. -----

    #[test]
    fn log_cursor_algebra_holds(
        appends in 0u64..40,
        prune_at in any::<u64>(),
        do_prune in any::<bool>(),
    ) {
        let mut log = TopicLog::default();
        // Append `appends` messages, each stamped with the writer-assigned next_cursor.
        for _ in 0..appends {
            let c = log.next_cursor();
            log.apply(LogOp::Append(msg(c, format!("m{c}").as_bytes())));
        }
        prop_assert_eq!(log.high_cursor(), appends);
        prop_assert_eq!(log.next_cursor(), appends + 1);
        prop_assert_eq!(log.len() as u64, appends);

        if do_prune && appends > 0 {
            let up_to = prune_at % (appends + 1); // 0..=appends
            log.apply(LogOp::PruneTo(up_to));
            // floor advanced to exactly the count of messages with cursor <= up_to.
            prop_assert_eq!(log.floor(), up_to);
            // high/next cursor never move on a prune (cursors stay absolute).
            prop_assert_eq!(log.high_cursor(), appends);
            // surviving messages all have cursor > up_to, in order.
            let all = log.all();
            for w in all.windows(2) {
                prop_assert!(w[0].cursor < w[1].cursor, "cursors must stay strictly increasing");
            }
            for m in &all {
                prop_assert!(m.cursor > up_to, "pruned message survived");
            }
            // since(up_to) == all surviving; since(high) == empty.
            prop_assert_eq!(log.since(up_to).len(), all.len());
            prop_assert_eq!(log.since(appends).len(), 0);
        }
    }

    // ----- Snapshot + tail == full replay, for any cut point in a random op script. -----

    #[test]
    fn snapshot_plus_tail_equals_full(appends in 1u64..30, prune_every in 3u64..9) {
        // Build an op script of appends interleaved with periodic prunes.
        let mut ops: Vec<LogOp> = Vec::new();
        let mut high = 0u64;
        for _ in 0..appends {
            high += 1;
            ops.push(LogOp::Append(msg(high, format!("m{high}").as_bytes())));
            if high.is_multiple_of(prune_every) && high > 2 {
                ops.push(LogOp::PruneTo(high - 2));
            }
        }
        let mut reference = TopicLog::default();
        for op in &ops {
            reference.apply(op.clone());
        }
        // For every cut point, snapshot the prefix, reload, apply the suffix → must equal reference.
        for cut in 0..=ops.len() {
            let mut head = TopicLog::default();
            for op in &ops[..cut] {
                head.apply(op.clone());
            }
            let bytes = head.save().unwrap();
            let mut reader = TopicLog::load(&bytes).unwrap();
            for op in &ops[cut..] {
                reader.apply(op.clone());
            }
            prop_assert_eq!(reader, reference.clone(), "snapshot+tail diverged at cut={}", cut);
        }
    }

    // ----- Topic validation never panics; accepted names are exactly the allowed charset. -----

    #[test]
    fn validate_topic_never_panics_and_is_exact(topic in ".{0,40}") {
        let res = validate_topic(&topic);
        let expect_ok = !topic.is_empty()
            && topic.len() <= 200
            && topic.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':'));
        prop_assert_eq!(res.is_ok(), expect_ok, "validate_topic disagreed for {:?}", topic);
    }

    // ----- Capability: topic-prefix scoping. -----

    #[test]
    fn topic_allows_is_prefix_match(scope in "[a-z.]{0,8}", topic in "[a-z.]{0,12}") {
        prop_assert_eq!(topic_allows(&scope, &topic), topic.starts_with(&scope));
    }

    /// A self-issued publish link verifies iff (ability matches) AND (topic under scope) AND (now in
    /// window) AND (not revoked). Searched over random scope/topic/now/ability/revocation.
    #[test]
    fn link_verify_is_sound(
        scope in "[a-z]{1,6}",
        topic in "[a-z]{1,8}",
        not_after in 0u64..2_000,
        now in 0u64..3_000,
        want_publish in any::<bool>(),
        revoke in any::<bool>(),
    ) {
        let owner = id();
        let nonce = 42u64;
        let token = mint_link(&owner, owner.node_id(), ABILITY_PUBLISH, &scope, not_after, nonce).unwrap();
        let want_ability = if want_publish { ABILITY_PUBLISH } else { ABILITY_SUBSCRIBE };
        let is_revoked = |_i: &NodeId, n: u64| revoke && n == nonce;
        let res = verify_link(
            &owner.node_id(), &[], &[], now, &owner.node_id(),
            want_ability, &topic, &token, &is_revoked,
        );

        let time_ok = not_after == 0 || now <= not_after;
        let ability_ok = want_publish; // link only grants publish
        let scope_ok = topic.starts_with(&scope);
        let expect_ok = time_ok && ability_ok && scope_ok && !revoke;
        prop_assert_eq!(res.is_ok(), expect_ok,
            "verify disagreed: time_ok={} ability_ok={} scope_ok={} revoke={} scope={} topic={}",
            time_ok, ability_ok, scope_ok, revoke, scope, topic);
    }

    /// Only the named audience may wield a bound link, for any random requester.
    #[test]
    fn only_audience_may_use_bound_link(use_correct in any::<bool>()) {
        let owner = id();
        let holder = id();
        let other = id();
        let token = mint_link(&owner, holder.node_id(), ABILITY_PUBLISH, "t", 0, 1).unwrap();
        let requester = if use_correct { holder.node_id() } else { other.node_id() };
        let res = verify_link(
            &owner.node_id(), &[], &[], 1000, &requester, ABILITY_PUBLISH, "t", &token, &never,
        );
        prop_assert_eq!(res.is_ok(), use_correct);
    }
}

/// `decode`/`inspect` on garbage tokens must never panic — only error.
#[test]
fn inspect_garbage_never_panics() {
    use ce_pubsub::caps::inspect_link;
    assert!(inspect_link("").is_err());
    assert!(inspect_link("zzzz not hex").is_err());
    assert!(inspect_link("deadbeef").is_err());
}
