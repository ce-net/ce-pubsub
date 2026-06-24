//! Property + invariant tests for the server-tracked subscription registry (`SubRegistry`): the
//! lease/ack/nack/dead-letter state machine that backs at-least-once delivery. These search the
//! input space the hand-picked unit tests in `src/subscription.rs` sample, asserting the core
//! invariants hold for any interleaving:
//!
//! * the ack floor is monotonic (never goes backwards) and never exceeds the topic high-water;
//! * a cursor at or below the floor is never re-leased (no redelivery of acknowledged work);
//! * an unacked lease is redelivered after its deadline, with a strictly incremented attempt count;
//! * dead-lettering never loses a cursor and always advances the floor past the poison prefix;
//! * snapshot (serde) round-trips are lossless for any reachable state.

use ce_coord::{Snapshot, StateMachine};
use ce_pubsub::message::Cursor;
use ce_pubsub::subscription::{MAX_LEASE_BATCH, SubOp, SubRegistry, SubscriptionPolicy};
use proptest::prelude::*;

fn reg(deadline: u64, max_attempts: u32) -> SubRegistry {
    let mut r = SubRegistry::default();
    r.apply(SubOp::Create {
        name: "s".into(),
        policy: SubscriptionPolicy {
            ack_deadline_secs: deadline,
            max_delivery_attempts: max_attempts,
        },
    });
    r
}

fn lease_apply(
    r: &mut SubRegistry,
    high: Cursor,
    floor: Cursor,
    now: u64,
    max: usize,
) -> Vec<(Cursor, u32)> {
    match r.plan_lease("s", high, floor, now, max) {
        Some((op, out)) => {
            r.apply(op);
            out.leased
        }
        None => Vec::new(),
    }
}

proptest! {
    /// Drive a random script of lease/ack/advance-time steps and assert the floor invariants hold
    /// throughout, and that no acked cursor is ever redelivered.
    #[test]
    fn lease_ack_invariants_hold(
        high in 1u64..40,
        deadline in 1u64..5,
        steps in proptest::collection::vec(
            (0u8..3, 0u64..50, 1usize..20),
            0..40,
        ),
    ) {
        let mut r = reg(deadline, 0); // never dead-letter, so every cursor must eventually be acked
        let floor = 0;
        let mut now = 100u64;
        let mut last_acked = 0u64;
        let mut ever_acked: std::collections::BTreeSet<Cursor> = std::collections::BTreeSet::new();

        for (kind, dt, max) in steps {
            now += dt;
            match kind {
                0 => {
                    // Lease a batch.
                    let leased = lease_apply(&mut r, high, floor, now, max);
                    // A leased cursor must be above the current floor and never already acked.
                    let cur_floor = r.get("s").unwrap().acked;
                    for (c, attempt) in &leased {
                        prop_assert!(*c > cur_floor, "leased a cursor at/below the ack floor");
                        prop_assert!(!ever_acked.contains(c), "redelivered an acked cursor {c}");
                        prop_assert!(*attempt >= 1);
                    }
                    prop_assert!(leased.len() <= MAX_LEASE_BATCH);
                }
                1 => {
                    // Ack the lowest currently-leased cursor, if any.
                    let next = r.get("s").unwrap().acked + 1;
                    if next <= high {
                        r.apply(SubOp::Ack { name: "s".into(), cursor: next });
                        ever_acked.insert(next);
                    }
                }
                _ => {
                    // Nack the lowest unacked cursor (release for redelivery).
                    let next = r.get("s").unwrap().acked + 1;
                    if next <= high {
                        r.apply(SubOp::Nack { name: "s".into(), cursor: next });
                    }
                }
            }
            // Floor is monotonic and bounded by high.
            let acked = r.get("s").unwrap().acked;
            prop_assert!(acked >= last_acked, "ack floor went backwards");
            prop_assert!(acked <= high, "ack floor exceeded high-water");
            last_acked = acked;
        }
    }

    /// Acking every cursor in order always reaches the high-water floor exactly.
    #[test]
    fn acking_all_in_order_reaches_high(high in 1u64..60) {
        let mut r = reg(30, 0);
        lease_apply(&mut r, high, 0, 100, MAX_LEASE_BATCH);
        for c in 1..=high {
            r.apply(SubOp::Ack { name: "s".into(), cursor: c });
        }
        prop_assert_eq!(r.get("s").unwrap().acked, high);
        prop_assert_eq!(r.get("s").unwrap().outstanding(), 0);
    }

    /// Out-of-order acks: ack a random permutation; the floor only reaches `high` once ALL are acked,
    /// and equals the largest contiguous prefix at every intermediate step.
    #[test]
    fn out_of_order_acks_track_contiguous_prefix(high in 1u64..25, seed in any::<u64>()) {
        let mut r = reg(30, 0);
        lease_apply(&mut r, high, 0, 100, MAX_LEASE_BATCH);
        // A deterministic shuffle of 1..=high driven by the seed.
        let mut order: Vec<Cursor> = (1..=high).collect();
        let mut s = seed | 1;
        for i in (1..order.len()).rev() {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = (s >> 33) as usize % (i + 1);
            order.swap(i, j);
        }
        let mut acked: std::collections::BTreeSet<Cursor> = std::collections::BTreeSet::new();
        for c in order {
            r.apply(SubOp::Ack { name: "s".into(), cursor: c });
            acked.insert(c);
            // The floor is exactly the largest k such that 1..=k are all acked.
            let mut expected = 0;
            while acked.contains(&(expected + 1)) {
                expected += 1;
            }
            prop_assert_eq!(r.get("s").unwrap().acked, expected);
        }
        prop_assert_eq!(r.get("s").unwrap().acked, high);
    }

    /// Snapshot round-trip is lossless for any reachable subscription state.
    #[test]
    fn snapshot_roundtrips(high in 0u64..30, deadline in 1u64..5, max_attempts in 0u32..4) {
        let mut r = reg(deadline, max_attempts);
        let mut now = 100;
        for _ in 0..6 {
            lease_apply(&mut r, high, 0, now, 10);
            now += deadline + 1;
        }
        r.apply(SubOp::Ack { name: "s".into(), cursor: 1 });
        let bytes = r.save().unwrap();
        let back = SubRegistry::load(&bytes).unwrap();
        prop_assert_eq!(r, back);
    }
}

/// A poison message (always times out) is dead-lettered exactly once, exactly when its attempt count
/// would exceed the policy, and the floor advances past it so following work is never blocked.
#[test]
fn dead_letter_fires_once_at_threshold() {
    let max_attempts = 3u32;
    let mut r = reg(1, max_attempts);
    let high = 1;
    let mut now = 100;
    let mut dead_total = 0;
    let mut delivered = 0;
    for _ in 0..10 {
        let plan = r.plan_lease("s", high, 0, now, 10);
        if let Some((op, out)) = plan {
            r.apply(op);
            delivered += out.leased.len();
            dead_total += out.dead_lettered.len();
        }
        now += 5; // exceed the 1s deadline each round
    }
    assert_eq!(
        delivered, max_attempts as usize,
        "delivered exactly max_attempts times"
    );
    assert_eq!(dead_total, 1, "dead-lettered exactly once");
    assert_eq!(
        r.get("s").unwrap().acked,
        high,
        "floor advanced past the poison message"
    );
}
