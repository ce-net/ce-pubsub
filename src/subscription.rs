//! Subscriptions as first-class durable resources — the missing half of real Pub/Sub semantics.
//!
//! Google Pub/Sub decouples a *topic* (the durable message log) from a *subscription* (an
//! independently-advanced consumer position with its own backlog, ack state, and delivery policy).
//! ce-pubsub's [`TopicLog`](crate::log::TopicLog) is the topic; this module is the subscription side:
//! a single, owner-written [`SubRegistry`] state machine — replicated by `ce-coord` exactly like the
//! message log — that tracks, per named subscription:
//!
//! * the **acked cursor** (everything `<= acked` is processed and will not be redelivered),
//! * an **ack-deadline lease**: a window of leased-but-unacked cursors with an expiry, so a consumer
//!   that crashes mid-processing has its messages redelivered after the lease lapses (at-least-once
//!   with server-enforced redelivery — not client cursor bookkeeping),
//! * a per-message **delivery-attempt counter** and a **dead-letter policy**: a message that exceeds
//!   `max_delivery_attempts` is routed aside so a poison message never blocks the subscription
//!   forever.
//!
//! The owner is the sole writer (single-writer ce-coord log), so all cursor/lease arithmetic is
//! linearized; consumers talk to the owner over the directed control topic
//! ([`control_topic`](crate::message::control_topic)).

use std::collections::BTreeMap;

use ce_coord::{Snapshot, StateMachine, json_snapshot};
use serde::{Deserialize, Serialize};

use crate::message::Cursor;

/// Upper bound on the number of subscriptions one topic may hold — prevents an unbounded-state DoS
/// where a flood of `create_subscription` calls grows the owner's registry without limit.
pub const MAX_SUBSCRIPTIONS: usize = 10_000;

/// Upper bound on a subscription's ack deadline (seconds). Google caps modifyAckDeadline at 600s.
pub const MAX_ACK_DEADLINE_SECS: u64 = 600;

/// Default ack deadline if a subscription does not specify one.
pub const DEFAULT_ACK_DEADLINE_SECS: u64 = 30;

/// Default maximum delivery attempts before a message is dead-lettered.
pub const DEFAULT_MAX_DELIVERY_ATTEMPTS: u32 = 5;

/// Hard cap on how many cursors a single `lease` call may hand out, bounding the in-memory leased
/// set and the size of any one pull response.
pub const MAX_LEASE_BATCH: usize = 1_000;

/// Per-subscription delivery configuration. Mirrors the knobs Google Pub/Sub exposes on a
/// subscription resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionPolicy {
    /// Seconds a leased (delivered-but-unacked) message is held before it becomes redeliverable.
    pub ack_deadline_secs: u64,
    /// After this many delivery attempts a message is routed to the dead-letter topic. `0` disables
    /// dead-lettering (messages are retried forever, matching the pre-subscription behavior).
    pub max_delivery_attempts: u32,
}

impl Default for SubscriptionPolicy {
    fn default() -> Self {
        SubscriptionPolicy {
            ack_deadline_secs: DEFAULT_ACK_DEADLINE_SECS,
            max_delivery_attempts: DEFAULT_MAX_DELIVERY_ATTEMPTS,
        }
    }
}

impl SubscriptionPolicy {
    /// Validate the policy against the configured bounds, clamping the deadline.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.ack_deadline_secs == 0 || self.ack_deadline_secs > MAX_ACK_DEADLINE_SECS {
            anyhow::bail!(
                "ack_deadline_secs must be 1..={MAX_ACK_DEADLINE_SECS} (got {})",
                self.ack_deadline_secs
            );
        }
        Ok(())
    }
}

/// A leased (delivered, not-yet-acked) cursor and when its lease expires (unix seconds).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Lease {
    cursor: Cursor,
    expires_at: u64,
    attempts: u32,
}

/// One subscription's durable state: where it has acked to, its outstanding leases, its per-cursor
/// attempt history (only for cursors that have been redelivered), and its policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionState {
    /// Everything with `cursor <= acked` is acknowledged and never redelivered.
    pub acked: Cursor,
    /// Currently leased cursors, keyed by cursor for O(log n) lookup.
    leases: BTreeMap<Cursor, Lease>,
    /// Delivery attempts for cursors that have been leased at least once but not yet acked. Survives
    /// lease expiry so the dead-letter threshold accumulates across redeliveries.
    attempts: BTreeMap<Cursor, u32>,
    /// Cursors that exceeded `max_delivery_attempts` and were dead-lettered (so they are skipped on
    /// future leases and counted as "handled" for ack-progress).
    dead: BTreeMap<Cursor, ()>,
    /// Cursors individually acknowledged out of order (above the contiguous `acked` floor). Google
    /// Pub/Sub allows acking messages in any order; the contiguous floor only advances once every
    /// cursor below a point is acked or dead. This holds the not-yet-contiguous acks; once a run from
    /// `acked + 1` becomes contiguous it is folded into `acked` and these entries are removed.
    #[serde(default)]
    acked_set: BTreeMap<Cursor, ()>,
    /// Policy for this subscription.
    pub policy: SubscriptionPolicy,
}

impl SubscriptionState {
    /// Number of currently outstanding (leased, unexpired-or-not) cursors.
    pub fn outstanding(&self) -> usize {
        self.leases.len()
    }

    /// Dead-lettered cursors, in order.
    pub fn dead_lettered(&self) -> Vec<Cursor> {
        self.dead.keys().copied().collect()
    }
}

/// The replicated registry of every subscription on a topic. Single-writer (the owner).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubRegistry {
    subs: BTreeMap<String, SubscriptionState>,
}

/// Result of leasing a batch: the cursors to deliver (with their current attempt counts) and the
/// cursors that just crossed the dead-letter threshold and must be republished to the DLQ.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseOutcome {
    /// `(cursor, attempt_number)` pairs the consumer should now process.
    pub leased: Vec<(Cursor, u32)>,
    /// Cursors that just exceeded `max_delivery_attempts` and were dead-lettered this call.
    pub dead_lettered: Vec<Cursor>,
}

/// Operations on the [`SubRegistry`]. The owner proposes these to the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubOp {
    /// Create (or reset to policy) a subscription. Idempotent: re-creating an existing subscription
    /// updates its policy but preserves its acked cursor and leases.
    Create {
        name: String,
        policy: SubscriptionPolicy,
    },
    /// Delete a subscription entirely.
    Delete { name: String },
    /// Lease up to `max` redeliverable cursors in `(low, high]` whose lease is free at `now`,
    /// stamping each with `expires_at = now + ack_deadline`. `high` is the topic's current
    /// high-water cursor. Records the resulting `LeaseOutcome` is computed deterministically by
    /// [`SubRegistry::plan_lease`] *before* proposing; this op just applies the stamped result.
    ApplyLease {
        name: String,
        /// Cursors newly leased this call with their stamped expiry.
        leased: Vec<(Cursor, u64)>,
        /// Cursors dead-lettered this call.
        dead: Vec<Cursor>,
        /// The contiguous ack-floor advance produced by dead-lettering a prefix (see apply).
        new_acked: Cursor,
    },
    /// Acknowledge processing of `cursor` (and everything below it that is already acked/dead):
    /// removes the lease and advances `acked` over any now-contiguous run.
    Ack { name: String, cursor: Cursor },
    /// Negative-ack `cursor`: release the lease immediately so it is redeliverable now (without
    /// waiting for the deadline). Attempt count is preserved.
    Nack { name: String, cursor: Cursor },
}

impl StateMachine for SubRegistry {
    type Op = SubOp;
    fn apply(&mut self, op: SubOp) {
        match op {
            SubOp::Create { name, policy } => {
                let entry = self.subs.entry(name).or_default();
                entry.policy = policy;
            }
            SubOp::Delete { name } => {
                self.subs.remove(&name);
            }
            SubOp::ApplyLease {
                name,
                leased,
                dead,
                new_acked,
            } => {
                if let Some(s) = self.subs.get_mut(&name) {
                    for c in &dead {
                        s.dead.insert(*c, ());
                        s.leases.remove(c);
                        s.attempts.remove(c);
                    }
                    for (c, expires_at) in leased {
                        let attempts = s.attempts.get(&c).copied().unwrap_or(0) + 1;
                        s.attempts.insert(c, attempts);
                        s.leases.insert(
                            c,
                            Lease {
                                cursor: c,
                                expires_at,
                                attempts,
                            },
                        );
                    }
                    if new_acked > s.acked {
                        s.acked = new_acked;
                        // Drop any acked/dead bookkeeping now subsumed by the advanced floor.
                        s.acked_set.retain(|c, _| *c > new_acked);
                        s.dead.retain(|c, _| *c > new_acked);
                    }
                }
            }
            SubOp::Ack { name, cursor } => {
                if let Some(s) = self.subs.get_mut(&name) {
                    s.leases.remove(&cursor);
                    s.attempts.remove(&cursor);
                    // Record the ack only if it is above the floor (acking an already-acked cursor is
                    // a no-op). Then fold any now-contiguous run from the floor into `acked`.
                    if cursor > s.acked {
                        s.acked_set.insert(cursor, ());
                    }
                    s.advance_acked();
                }
            }
            SubOp::Nack { name, cursor } => {
                if let Some(s) = self.subs.get_mut(&name) {
                    // Expire the lease immediately so the next lease redelivers it.
                    if let Some(l) = s.leases.get_mut(&cursor) {
                        l.expires_at = 0;
                    }
                }
            }
        }
    }
}

impl Snapshot for SubRegistry {
    json_snapshot!();
}

impl SubscriptionState {
    /// Advance the contiguous `acked` floor over a run of cursors that are individually acknowledged
    /// (out-of-order acks held in `acked_set`) or dead-lettered. Starting at `acked + 1`, while the
    /// next cursor is acked or dead, fold it into `acked` and drop it from its set. This is what makes
    /// out-of-order acking (ack 3, then 1, then 2 → floor jumps to 3) and poison-skipping work without
    /// ever stalling the floor on an unacked-but-dead message.
    fn advance_acked(&mut self) {
        loop {
            let next = self.acked + 1;
            if self.acked_set.remove(&next).is_some() || self.dead.remove(&next).is_some() {
                self.acked = next;
            } else {
                break;
            }
        }
    }

    /// Cursors acknowledged out of order that are still above the contiguous floor (not yet folded in).
    pub fn pending_acks(&self) -> Vec<Cursor> {
        self.acked_set.keys().copied().collect()
    }
}

impl SubRegistry {
    /// Read-only view of a subscription's state.
    pub fn get(&self, name: &str) -> Option<&SubscriptionState> {
        self.subs.get(name)
    }

    /// Names of all subscriptions, in order.
    pub fn names(&self) -> Vec<String> {
        self.subs.keys().cloned().collect()
    }

    /// Number of subscriptions.
    pub fn len(&self) -> usize {
        self.subs.len()
    }

    /// True if no subscriptions exist.
    pub fn is_empty(&self) -> bool {
        self.subs.is_empty()
    }

    /// True if creating a new subscription would exceed [`MAX_SUBSCRIPTIONS`] (and `name` is new).
    pub fn at_capacity(&self, name: &str) -> bool {
        !self.subs.contains_key(name) && self.subs.len() >= MAX_SUBSCRIPTIONS
    }

    /// Deterministically plan a lease of up to `max` redeliverable cursors for subscription `name`,
    /// given the topic high-water cursor `high`, retention `floor` (cursors `<= floor` are pruned and
    /// can never be delivered), and the current time `now`. Pure: it does not mutate; the caller
    /// proposes the resulting [`SubOp::ApplyLease`] and replies with [`LeaseOutcome`].
    ///
    /// A cursor in `(acked, high]` is redeliverable iff it is not currently leased with an unexpired
    /// lease, not already dead-lettered, and not `<= floor`. A cursor whose *next* attempt would
    /// exceed `max_delivery_attempts` is dead-lettered instead of leased.
    pub fn plan_lease(
        &self,
        name: &str,
        high: Cursor,
        floor: Cursor,
        now: u64,
        max: usize,
    ) -> Option<(SubOp, LeaseOutcome)> {
        let s = self.subs.get(name)?;
        let deadline = s.policy.ack_deadline_secs.clamp(1, MAX_ACK_DEADLINE_SECS);
        let expires_at = now.saturating_add(deadline);
        let cap = max.min(MAX_LEASE_BATCH);

        let mut leased_stamped: Vec<(Cursor, u64)> = Vec::new();
        let mut leased_out: Vec<(Cursor, u32)> = Vec::new();
        let mut dead: Vec<Cursor> = Vec::new();

        // Scan from just after the ack floor up to high. Skip pruned cursors entirely.
        let start = s.acked.max(floor) + 1;
        let mut c = start;
        while c <= high && leased_out.len() < cap {
            let leased_unexpired = s
                .leases
                .get(&c)
                .map(|l| l.expires_at > now)
                .unwrap_or(false);
            let is_dead = s.dead.contains_key(&c);
            // A cursor acked out of order (above the floor) must never be redelivered.
            let is_acked = s.acked_set.contains_key(&c);
            if leased_unexpired || is_dead || is_acked {
                c += 1;
                continue;
            }
            let next_attempt = s.attempts.get(&c).copied().unwrap_or(0) + 1;
            if s.policy.max_delivery_attempts != 0 && next_attempt > s.policy.max_delivery_attempts
            {
                dead.push(c);
            } else {
                leased_stamped.push((c, expires_at));
                leased_out.push((c, next_attempt));
            }
            c += 1;
        }

        if leased_stamped.is_empty() && dead.is_empty() {
            return None;
        }

        // Dead-lettering a contiguous prefix advances the ack floor so the subscription does not
        // re-scan poison cursors forever. Compute the new contiguous floor over already-acked,
        // out-of-order-acked, and dead cursors (the run the floor can legitimately jump over).
        // Cursors at or below the topic's retention floor were pruned and can never be delivered, so
        // they are implicitly handled: bridge the ack floor across them. Without this a subscription
        // created (or lagging) after a prune would stall forever at `acked + 1` waiting for a message
        // that no longer exists.
        let mut new_acked = s.acked.max(floor);
        let mut handled: std::collections::BTreeSet<Cursor> = s.dead.keys().copied().collect();
        handled.extend(s.acked_set.keys().copied());
        handled.extend(dead.iter().copied());
        loop {
            let next = new_acked + 1;
            if handled.contains(&next) {
                new_acked = next;
            } else {
                break;
            }
        }

        let op = SubOp::ApplyLease {
            name: name.to_string(),
            leased: leased_stamped,
            dead: dead.clone(),
            new_acked,
        };
        Some((
            op,
            LeaseOutcome {
                leased: leased_out,
                dead_lettered: dead,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg_with(name: &str, policy: SubscriptionPolicy) -> SubRegistry {
        let mut r = SubRegistry::default();
        r.apply(SubOp::Create {
            name: name.to_string(),
            policy,
        });
        r
    }

    fn lease(
        reg: &mut SubRegistry,
        name: &str,
        high: Cursor,
        now: u64,
        max: usize,
    ) -> LeaseOutcome {
        match reg.plan_lease(name, high, 0, now, max) {
            Some((op, out)) => {
                reg.apply(op);
                out
            }
            None => LeaseOutcome::default(),
        }
    }

    #[test]
    fn create_is_idempotent_and_preserves_progress() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        lease(&mut r, "s", 3, 100, 10);
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 1,
        });
        assert_eq!(r.get("s").unwrap().acked, 1);
        // Re-create with a new policy: acked preserved.
        r.apply(SubOp::Create {
            name: "s".into(),
            policy: SubscriptionPolicy {
                ack_deadline_secs: 60,
                max_delivery_attempts: 3,
            },
        });
        assert_eq!(
            r.get("s").unwrap().acked,
            1,
            "re-create preserves acked cursor"
        );
        assert_eq!(r.get("s").unwrap().policy.ack_deadline_secs, 60);
    }

    #[test]
    fn lease_then_ack_advances_floor() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        let out = lease(&mut r, "s", 5, 100, 10);
        assert_eq!(
            out.leased.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert!(out.dead_lettered.is_empty());
        // A second lease before any acks/expiry returns nothing (all leased, unexpired).
        let again = lease(&mut r, "s", 5, 100, 10);
        assert!(
            again.leased.is_empty(),
            "leased-unexpired cursors are not re-leased"
        );
        // Ack in order advances floor.
        for c in 1..=5 {
            r.apply(SubOp::Ack {
                name: "s".into(),
                cursor: c,
            });
        }
        assert_eq!(r.get("s").unwrap().acked, 5);
        assert_eq!(r.get("s").unwrap().outstanding(), 0);
    }

    #[test]
    fn expired_lease_is_redelivered_with_incremented_attempt() {
        let mut r = reg_with(
            "s",
            SubscriptionPolicy {
                ack_deadline_secs: 30,
                max_delivery_attempts: 5,
            },
        );
        let first = lease(&mut r, "s", 2, 100, 10);
        assert_eq!(first.leased, vec![(1, 1), (2, 1)]);
        // Before expiry: no redelivery.
        assert!(lease(&mut r, "s", 2, 120, 10).leased.is_empty());
        // After the 30s deadline: both redeliverable, attempt 2.
        let second = lease(&mut r, "s", 2, 200, 10);
        assert_eq!(second.leased, vec![(1, 2), (2, 2)]);
    }

    #[test]
    fn nack_makes_redeliverable_immediately() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        lease(&mut r, "s", 1, 100, 10);
        r.apply(SubOp::Nack {
            name: "s".into(),
            cursor: 1,
        });
        // Same `now`, but the nacked lease is now expired → redelivered, attempt 2.
        let out = lease(&mut r, "s", 1, 100, 10);
        assert_eq!(out.leased, vec![(1, 2)]);
    }

    #[test]
    fn poison_message_is_dead_lettered_and_floor_advances() {
        let mut r = reg_with(
            "s",
            SubscriptionPolicy {
                ack_deadline_secs: 1,
                max_delivery_attempts: 3,
            },
        );
        let mut now = 100;
        // Cursor 1 keeps timing out. Attempts 1,2,3 then dead-letter on the 4th plan.
        for _ in 0..3 {
            let out = lease(&mut r, "s", 1, now, 10);
            assert_eq!(out.leased.len(), 1);
            assert!(out.dead_lettered.is_empty());
            now += 10; // exceed the 1s deadline
        }
        let dl = lease(&mut r, "s", 1, now, 10);
        assert!(dl.leased.is_empty());
        assert_eq!(dl.dead_lettered, vec![1]);
        // The dead-letter advances the ack floor so cursor 1 never blocks again.
        assert_eq!(r.get("s").unwrap().acked, 1);
        assert_eq!(r.get("s").unwrap().dead_lettered(), Vec::<Cursor>::new());
    }

    #[test]
    fn dead_letter_does_not_stall_following_messages() {
        let mut r = reg_with(
            "s",
            SubscriptionPolicy {
                ack_deadline_secs: 1,
                max_delivery_attempts: 1,
            },
        );
        let mut now = 100;
        // First lease delivers 1,2,3 (attempt 1).
        assert_eq!(lease(&mut r, "s", 3, now, 10).leased.len(), 3);
        now += 10;
        // All time out; max=1 so all dead-letter on the next plan.
        let dl = lease(&mut r, "s", 3, now, 10);
        assert_eq!(dl.dead_lettered, vec![1, 2, 3]);
        assert_eq!(
            r.get("s").unwrap().acked,
            3,
            "floor advanced past all poison"
        );
        // A new message 4 is still deliverable.
        let next = lease(&mut r, "s", 4, now, 10);
        assert_eq!(next.leased, vec![(4, 1)]);
    }

    #[test]
    fn max_lease_batch_caps_response() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        let out = lease(&mut r, "s", 5000, 100, 100);
        assert_eq!(out.leased.len(), 100, "respects requested max");
        let out2 = lease(&mut r, "s", 999_999, 100, usize::MAX);
        assert!(out2.leased.len() <= MAX_LEASE_BATCH, "hard cap applies");
    }

    #[test]
    fn floor_skips_pruned_cursors() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        // High=10 but retention floor=4: only 5..=10 are deliverable.
        let (op, out) = r.plan_lease("s", 10, 4, 100, 100).unwrap();
        r.apply(op);
        assert_eq!(out.leased.first().map(|(c, _)| *c), Some(5));
        assert_eq!(out.leased.len(), 6);
    }

    #[test]
    fn floor_bridges_retention_so_subscription_does_not_stall() {
        // A subscription whose backlog starts above a retention floor must advance its ack floor over
        // the pruned (never-deliverable) prefix on the first lease — otherwise it stalls forever
        // waiting to ack messages that no longer exist.
        let mut r = reg_with("s", SubscriptionPolicy::default());
        // high=5, floor=3: only cursors 4,5 are deliverable; 1..=3 were pruned.
        let (op, out) = r.plan_lease("s", 5, 3, 100, 10).unwrap();
        r.apply(op);
        assert_eq!(out.leased, vec![(4, 1), (5, 1)]);
        // The ack floor bridged past the pruned prefix to the retention floor.
        assert_eq!(
            r.get("s").unwrap().acked,
            3,
            "floor bridged across pruned cursors"
        );
        // Acking the leased messages then advances cleanly to high.
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 4,
        });
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 5,
        });
        assert_eq!(r.get("s").unwrap().acked, 5);
    }

    #[test]
    fn delete_removes_subscription() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        assert_eq!(r.len(), 1);
        r.apply(SubOp::Delete { name: "s".into() });
        assert!(r.is_empty());
        assert!(r.get("s").is_none());
    }

    #[test]
    fn capacity_guard() {
        let mut r = SubRegistry::default();
        assert!(!r.at_capacity("new"));
        for i in 0..MAX_SUBSCRIPTIONS {
            r.apply(SubOp::Create {
                name: format!("s{i}"),
                policy: SubscriptionPolicy::default(),
            });
        }
        assert!(r.at_capacity("brand-new"));
        assert!(
            !r.at_capacity("s0"),
            "existing subscription is not at-capacity"
        );
    }

    #[test]
    fn snapshot_roundtrip() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        lease(&mut r, "s", 3, 100, 10);
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 1,
        });
        let bytes = r.save().unwrap();
        let back = SubRegistry::load(&bytes).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn policy_validation() {
        assert!(SubscriptionPolicy::default().validate().is_ok());
        assert!(
            SubscriptionPolicy {
                ack_deadline_secs: 0,
                max_delivery_attempts: 1
            }
            .validate()
            .is_err()
        );
        assert!(
            SubscriptionPolicy {
                ack_deadline_secs: MAX_ACK_DEADLINE_SECS + 1,
                max_delivery_attempts: 1
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn out_of_order_ack_advances_floor_only_when_contiguous() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        lease(&mut r, "s", 5, 100, 10); // lease 1..=5
        // Ack 3 first (out of order): floor must NOT advance (1,2 still unacked).
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 3,
        });
        assert_eq!(
            r.get("s").unwrap().acked,
            0,
            "non-contiguous ack does not advance floor"
        );
        assert_eq!(r.get("s").unwrap().pending_acks(), vec![3]);
        // Ack 1: floor advances to 1 (2 still missing).
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 1,
        });
        assert_eq!(r.get("s").unwrap().acked, 1);
        // Ack 2: now 1,2,3 are contiguous → floor jumps to 3.
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 2,
        });
        assert_eq!(
            r.get("s").unwrap().acked,
            3,
            "contiguous run folds, floor jumps over the held ack"
        );
        assert!(
            r.get("s").unwrap().pending_acks().is_empty(),
            "held ack folded away"
        );
    }

    #[test]
    fn out_of_order_acked_cursor_is_not_redelivered() {
        let mut r = reg_with(
            "s",
            SubscriptionPolicy {
                ack_deadline_secs: 1,
                max_delivery_attempts: 0,
            },
        );
        lease(&mut r, "s", 3, 100, 10); // lease 1..=3
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 2,
        }); // ack 2 out of order
        // After the deadline, 1 and 3 are redeliverable but 2 must not be.
        let out = lease(&mut r, "s", 3, 200, 10);
        let cursors: Vec<Cursor> = out.leased.iter().map(|(c, _)| *c).collect();
        assert_eq!(
            cursors,
            vec![1, 3],
            "out-of-order acked cursor 2 is skipped on redelivery"
        );
    }

    #[test]
    fn ack_is_idempotent() {
        let mut r = reg_with("s", SubscriptionPolicy::default());
        lease(&mut r, "s", 2, 100, 10);
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 1,
        });
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 1,
        }); // re-ack
        assert_eq!(r.get("s").unwrap().acked, 1);
        // Acking a cursor below the floor is a no-op, not an underflow.
        r.apply(SubOp::Ack {
            name: "s".into(),
            cursor: 1,
        });
        assert_eq!(r.get("s").unwrap().acked, 1);
    }

    #[test]
    fn zero_max_attempts_never_dead_letters() {
        let mut r = reg_with(
            "s",
            SubscriptionPolicy {
                ack_deadline_secs: 1,
                max_delivery_attempts: 0,
            },
        );
        let mut now = 100;
        for _ in 0..20 {
            let out = lease(&mut r, "s", 1, now, 10);
            assert!(out.dead_lettered.is_empty(), "0 disables dead-lettering");
            now += 10;
        }
    }
}
