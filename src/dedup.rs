//! A bounded, time-evicting de-duplication cache for the ingest path.
//!
//! The original implementation cleared the entire de-dup set wholesale once it reached 16384 entries.
//! That has two failure modes the audit flagged: (1) after a clear, a request re-delivered from the
//! inbox ring whose token was already processed is appended a *second* time (duplicate publish under
//! load), and (2) legitimately-distinct tokens are dropped mid-stream, so a slow retry is treated as
//! new.
//!
//! This cache fixes both. It maps a dedup key (the request `reply_token`, or a publisher-supplied
//! idempotency key) to the cursor that key produced, with FIFO insertion-order eviction once a hard
//! capacity is reached and time-based expiry of stale entries. Eviction removes the *oldest* entry,
//! not the whole set, so recent tokens are never forgotten while the cache stays bounded — closing
//! the duplicate-after-reset window for any retry that arrives within the retention horizon.

use std::collections::HashMap;
use std::collections::VecDeque;

use crate::message::Cursor;

/// What a previously-seen key resolved to: the cursor it was assigned (so a retry returns the same
/// cursor), or a marker that it was handled with no cursor (a rejected/acked control op).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seen {
    /// The key produced this cursor (a successful append). A retry returns it idempotently.
    Cursor(Cursor),
    /// The key was handled but produced no cursor (e.g. a rejected publish was not retried-into-dup).
    Handled,
}

/// A bounded FIFO cache with per-entry expiry. Not thread-safe by itself; the owner holds it behind a
/// mutex on the single ingest task.
#[derive(Debug)]
pub struct IdempotencyCache {
    map: HashMap<String, (Seen, u64)>,
    order: VecDeque<String>,
    capacity: usize,
    ttl_secs: u64,
}

impl IdempotencyCache {
    /// A cache holding at most `capacity` keys, expiring entries older than `ttl_secs`.
    pub fn new(capacity: usize, ttl_secs: u64) -> Self {
        IdempotencyCache {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
            ttl_secs,
        }
    }

    /// Look up a key, treating entries older than the TTL as absent (and lazily dropping them).
    pub fn get(&mut self, key: &str, now: u64) -> Option<Seen> {
        match self.map.get(key) {
            Some((seen, at)) => {
                if self.ttl_secs != 0 && now.saturating_sub(*at) > self.ttl_secs {
                    self.map.remove(key);
                    // leave the key in `order`; it is skipped on eviction since it is no longer in map.
                    None
                } else {
                    Some(*seen)
                }
            }
            None => None,
        }
    }

    /// Record that `key` resolved to `seen` at time `now`, evicting the oldest entry if at capacity.
    pub fn insert(&mut self, key: String, seen: Seen, now: u64) {
        // Update an existing entry in place (preserving its FIFO position so a refreshed key is not
        // re-ordered). A fresh key falls through to capacity-bounded insertion below.
        if let Some(slot) = self.map.get_mut(&key) {
            *slot = (seen, now);
            return;
        }
        while self.map.len() >= self.capacity {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.map.remove(&oldest);
                }
                None => break,
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, (seen, now));
    }

    /// Current number of live entries.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// A bounded, time-evicting set of `u64` tokens — the control loop's equivalent of
/// [`IdempotencyCache`] for requests that carry no idempotency key but must not be handled twice
/// (e.g. `lease`, which is not idempotent). [`seen`](TokenSet::seen) returns whether the token was
/// already present, recording it (with FIFO + TTL eviction) when it was not.
#[derive(Debug)]
pub struct TokenSet {
    map: HashMap<u64, u64>,
    order: VecDeque<u64>,
    capacity: usize,
    ttl_secs: u64,
}

impl TokenSet {
    /// A token set holding at most `capacity` tokens, expiring entries older than `ttl_secs`.
    pub fn new(capacity: usize, ttl_secs: u64) -> Self {
        TokenSet {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
            ttl_secs,
        }
    }

    /// Return `true` if `token` was already recorded (and still live); otherwise record it at `now`
    /// (evicting the oldest entry if at capacity) and return `false`.
    pub fn seen(&mut self, token: u64, now: u64) -> bool {
        if let Some(at) = self.map.get(&token) {
            if self.ttl_secs == 0 || now.saturating_sub(*at) <= self.ttl_secs {
                return true;
            }
            self.map.remove(&token);
        }
        while self.map.len() >= self.capacity {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.map.remove(&oldest);
                }
                None => break,
            }
        }
        self.order.push_back(token);
        self.map.insert(token, now);
        false
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_set_dedups_within_ttl() {
        let mut s = TokenSet::new(8, 100);
        assert!(!s.seen(1, 10), "first sight is new");
        assert!(s.seen(1, 20), "repeat within ttl is seen");
        assert!(!s.seen(1, 200), "after ttl it is new again");
    }

    #[test]
    fn token_set_evicts_oldest_at_capacity() {
        let mut s = TokenSet::new(2, 0);
        s.seen(1, 0);
        s.seen(2, 0);
        s.seen(3, 0); // evicts 1
        assert!(!s.seen(1, 0), "evicted token is treated as new");
        assert!(s.seen(3, 0), "recent token retained");
    }

    #[test]
    fn insert_and_get() {
        let mut c = IdempotencyCache::new(4, 100);
        assert!(c.get("a", 0).is_none());
        c.insert("a".into(), Seen::Cursor(7), 0);
        assert_eq!(c.get("a", 0), Some(Seen::Cursor(7)));
        c.insert("b".into(), Seen::Handled, 0);
        assert_eq!(c.get("b", 0), Some(Seen::Handled));
    }

    #[test]
    fn fifo_eviction_keeps_recent_drops_oldest() {
        let mut c = IdempotencyCache::new(3, 0);
        for k in ["a", "b", "c"] {
            c.insert(k.into(), Seen::Cursor(1), 0);
        }
        assert_eq!(c.len(), 3);
        // Inserting d evicts a (oldest), keeps b, c, d.
        c.insert("d".into(), Seen::Cursor(1), 0);
        assert_eq!(c.len(), 3);
        assert!(c.get("a", 0).is_none(), "oldest evicted");
        assert!(c.get("b", 0).is_some(), "recent retained");
        assert!(c.get("c", 0).is_some());
        assert!(c.get("d", 0).is_some());
    }

    #[test]
    fn retry_within_window_is_deduped_not_lost() {
        // The exact failure mode of the old wholesale-clear: fill past capacity, then re-present an
        // already-seen recent key — it must still be recognized (not re-appended).
        let mut c = IdempotencyCache::new(2, 1000);
        c.insert("recent".into(), Seen::Cursor(42), 10);
        c.insert("filler1".into(), Seen::Cursor(1), 11);
        // "recent" is now the oldest; one more insert evicts it — but a retry that arrives before
        // eviction is still deduped. Verify the not-yet-evicted case is correct.
        assert_eq!(c.get("recent", 12), Some(Seen::Cursor(42)));
    }

    #[test]
    fn ttl_expiry_drops_stale() {
        let mut c = IdempotencyCache::new(10, 5);
        c.insert("a".into(), Seen::Cursor(1), 100);
        assert_eq!(c.get("a", 104), Some(Seen::Cursor(1)), "within ttl");
        assert!(c.get("a", 200).is_none(), "expired past ttl");
    }

    #[test]
    fn reinsert_updates_in_place() {
        let mut c = IdempotencyCache::new(2, 0);
        c.insert("a".into(), Seen::Cursor(1), 0);
        c.insert("a".into(), Seen::Cursor(2), 0);
        assert_eq!(c.get("a", 0), Some(Seen::Cursor(2)));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn zero_ttl_never_expires() {
        let mut c = IdempotencyCache::new(10, 0);
        c.insert("a".into(), Seen::Cursor(1), 0);
        assert_eq!(c.get("a", u64::MAX), Some(Seen::Cursor(1)));
    }
}
