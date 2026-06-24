//! The durable topic log — a single-writer, snapshot-capable [`StateMachine`] replicated by
//! `ce-coord`. This is what makes ce-pubsub delivery *durable* and *replayable* rather than the
//! best-effort, at-most-once fan-out raw mesh gossip gives you.
//!
//! The topic **owner** opens this log as a `ce-coord` writer; it appends every accepted message
//! ([`LogOp::Append`]). A **puller** (a late or catching-up subscriber) opens it as a `ce-coord`
//! reader following the owner, replays the tail from its cursor, and thereby receives every message
//! it missed — at-least-once. Because the log implements [`Snapshot`], the owner can compact it and a
//! fresh puller bootstraps from a content-addressed snapshot instead of replaying from message 1.
//!
//! The state machine is deliberately tiny: it only appends. Truncation/retention is a separate,
//! explicit op so retention policy stays the owner's decision, never an accident of replication.

use ce_coord::{Snapshot, StateMachine, json_snapshot};
use serde::{Deserialize, Serialize};

use crate::message::{Cursor, Message};

/// The replicated state of one topic: the ordered message log plus a retention floor. `floor` is the
/// number of messages dropped from the front by retention; it lets cursors stay absolute (a message
/// keeps its original cursor even after older messages are pruned).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicLog {
    /// Messages still retained, in cursor order. The first entry's cursor is `floor + 1`.
    messages: Vec<Message>,
    /// How many messages have been pruned from the front (retention). Absolute cursors = `floor` +
    /// 1-based index into `messages`.
    floor: u64,
}

/// A mutation on a [`TopicLog`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogOp {
    /// Append a message. The writer stamps its absolute [`Cursor`] before proposing, so every
    /// replica agrees on positions.
    Append(Message),
    /// Drop every retained message with `cursor <= up_to` (retention/compaction of the live log).
    PruneTo(Cursor),
}

impl StateMachine for TopicLog {
    type Op = LogOp;
    fn apply(&mut self, op: LogOp) {
        match op {
            LogOp::Append(mut m) => {
                // Enforce the contiguity invariant the rest of the algebra depends on: every retained
                // message occupies `floor + index + 1`, so a message's absolute cursor MUST be exactly
                // `next_cursor()`. The single-writer stamps this correctly, but a replica applying a
                // malformed op (or a future bug) must not silently desync `floor`/`since`/`get`. We
                // re-stamp the canonical cursor rather than trust the op, keeping cursors absolute and
                // strictly increasing no matter what was proposed.
                m.cursor = self.next_cursor();
                self.messages.push(m);
            }
            LogOp::PruneTo(up_to) => {
                let before = self.messages.len();
                self.messages.retain(|m| m.cursor > up_to);
                self.floor += (before - self.messages.len()) as u64;
            }
        }
    }
}

// Opt into content-addressed snapshots so a fresh puller bootstraps from a blob instead of replaying
// the whole log. JSON encoding matches the op log's, satisfying the faithful-round-trip contract.
impl Snapshot for TopicLog {
    json_snapshot!();
}

impl TopicLog {
    /// The highest cursor ever assigned to this topic (retained or pruned). The next append gets
    /// `next_cursor()`. Equal to `floor + messages.len()`.
    pub fn high_cursor(&self) -> Cursor {
        self.floor + self.messages.len() as u64
    }

    /// The cursor the next appended message will receive.
    pub fn next_cursor(&self) -> Cursor {
        self.high_cursor() + 1
    }

    /// The retention floor: messages with `cursor <= floor` have been pruned and are no longer
    /// replayable from the live log (only from a snapshot taken before the prune).
    pub fn floor(&self) -> Cursor {
        self.floor
    }

    /// Number of retained messages.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// True if no messages are retained.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Every retained message with `cursor > from`, in order — the replay slice a puller wants when
    /// it asks for "everything after `from`". `from = 0` returns all retained messages.
    pub fn since(&self, from: Cursor) -> Vec<Message> {
        self.messages
            .iter()
            .filter(|m| m.cursor > from)
            .cloned()
            .collect()
    }

    /// All retained messages, in cursor order.
    pub fn all(&self) -> Vec<Message> {
        self.messages.clone()
    }

    /// The retained message at absolute `cursor`, or `None` if it was pruned or never existed. Because
    /// retained messages are contiguous with absolute cursors `floor+1 ..= high`, this is an O(1)
    /// indexed lookup rather than a scan.
    pub fn get(&self, cursor: Cursor) -> Option<Message> {
        if cursor <= self.floor || cursor > self.high_cursor() {
            return None;
        }
        let idx = (cursor - self.floor - 1) as usize;
        self.messages.get(idx).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(cursor: Cursor, body: &str) -> Message {
        Message::new(cursor, "writer", body.as_bytes(), 1000 + cursor)
    }

    fn apply_all(log: &mut TopicLog, ops: Vec<LogOp>) {
        for op in ops {
            log.apply(op);
        }
    }

    #[test]
    fn append_assigns_monotonic_cursors() {
        let mut log = TopicLog::default();
        assert_eq!(log.next_cursor(), 1);
        log.apply(LogOp::Append(msg(1, "a")));
        log.apply(LogOp::Append(msg(2, "b")));
        assert_eq!(log.high_cursor(), 2);
        assert_eq!(log.next_cursor(), 3);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn since_returns_strict_tail() {
        let mut log = TopicLog::default();
        apply_all(
            &mut log,
            vec![
                LogOp::Append(msg(1, "a")),
                LogOp::Append(msg(2, "b")),
                LogOp::Append(msg(3, "c")),
            ],
        );
        let tail = log.since(1);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].cursor, 2);
        assert_eq!(tail[1].cursor, 3);
        // from 0 = everything; from high = nothing.
        assert_eq!(log.since(0).len(), 3);
        assert_eq!(log.since(3).len(), 0);
    }

    #[test]
    fn prune_advances_floor_and_keeps_cursors_absolute() {
        let mut log = TopicLog::default();
        for i in 1..=5 {
            log.apply(LogOp::Append(msg(i, &format!("m{i}"))));
        }
        log.apply(LogOp::PruneTo(2));
        assert_eq!(log.floor(), 2);
        assert_eq!(log.len(), 3);
        // Surviving messages keep their original absolute cursors.
        let all = log.all();
        assert_eq!(all[0].cursor, 3);
        assert_eq!(all[2].cursor, 5);
        // high/next cursor are unaffected by pruning.
        assert_eq!(log.high_cursor(), 5);
        assert_eq!(log.next_cursor(), 6);
        // since() respects the floor: asking before the floor returns only surviving messages.
        assert_eq!(log.since(0).len(), 3);
    }

    #[test]
    fn get_indexes_by_absolute_cursor_after_prune() {
        let mut log = TopicLog::default();
        for i in 1..=5 {
            log.apply(LogOp::Append(msg(i, &format!("m{i}"))));
        }
        // Before prune: exact cursor lookup.
        assert_eq!(log.get(1).unwrap().text(), "m1");
        assert_eq!(log.get(5).unwrap().text(), "m5");
        assert!(log.get(0).is_none());
        assert!(log.get(6).is_none());
        // After pruning 1,2: pruned cursors return None, survivors keep absolute lookup.
        log.apply(LogOp::PruneTo(2));
        assert!(log.get(1).is_none(), "pruned cursor is gone");
        assert!(log.get(2).is_none());
        assert_eq!(log.get(3).unwrap().text(), "m3");
        assert_eq!(log.get(5).unwrap().text(), "m5");
        assert!(log.get(6).is_none());
    }

    #[test]
    fn append_restamps_cursor_to_preserve_invariant() {
        // A malformed Append carrying a wrong absolute cursor must not desync the log: the state
        // machine re-stamps the canonical next cursor, keeping `floor`/`since`/`get` consistent.
        let mut log = TopicLog::default();
        log.apply(LogOp::Append(msg(999, "out-of-band cursor")));
        assert_eq!(
            log.high_cursor(),
            1,
            "cursor was re-stamped to 1, not trusted as 999"
        );
        assert_eq!(log.all()[0].cursor, 1);
        log.apply(LogOp::Append(msg(0, "another")));
        assert_eq!(log.all()[1].cursor, 2);
        // get() by the canonical cursor works; the bogus value never indexes anything.
        assert!(log.get(999).is_none());
        assert_eq!(log.get(1).unwrap().text(), "out-of-band cursor");
    }

    #[test]
    fn snapshot_plus_tail_equals_full_replay() {
        // The keystone property the durable-log replay relies on, proven at the state-machine level.
        let mut ops = Vec::new();
        for i in 1..=20u64 {
            ops.push(LogOp::Append(msg(i, &format!("m{i}"))));
            if i % 7 == 0 {
                ops.push(LogOp::PruneTo(i - 3));
            }
        }
        let mut reference = TopicLog::default();
        apply_all(&mut reference, ops.clone());

        for cut in 0..=ops.len() {
            let mut snapshotted = TopicLog::default();
            apply_all(&mut snapshotted, ops[..cut].to_vec());
            let bytes = snapshotted.save().unwrap();
            let mut reader = TopicLog::load(&bytes).unwrap();
            apply_all(&mut reader, ops[cut..].to_vec());
            assert_eq!(reader, reference, "snapshot+tail diverged at cut={cut}");
        }
    }
}
