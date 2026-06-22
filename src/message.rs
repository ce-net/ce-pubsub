//! The Pub/Sub message model and the topic → mesh-name mapping.
//!
//! A [`Message`] is one durable record in a topic's append log. Every message carries a monotonic
//! [`Cursor`] (its 1-based position in the log — the same number the durable [`RVec`] assigns as a
//! version), the publisher's authenticated NodeId, the opaque payload, and the unix-second publish
//! time. A subscriber that pulls `--from <cursor>` receives every message at a position strictly
//! greater than that cursor, which is exactly at-least-once replay.
//!
//! [`RVec`]: ce_coord::RVec

use serde::{Deserialize, Serialize};

/// A monotonic message position within a topic's log. The first message is at cursor 1; cursor 0
/// means "from the very beginning" when pulling. A cursor equals the [`ce_coord::Version`] the
/// durable log assigned when the message was appended.
pub type Cursor = u64;

/// One durable Pub/Sub message: a payload plus delivery metadata. Stored verbatim in the topic's
/// append log so a late subscriber replays the exact bytes the publisher sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// 1-based position in the topic log (assigned by the durable writer on append).
    pub cursor: Cursor,
    /// Authenticated publisher NodeId (hex). The owner records the verified sender; subscribers can
    /// trust it without re-checking.
    pub publisher: String,
    /// Opaque payload, hex-encoded so it rides JSON cleanly. Use [`Message::data`] to decode.
    pub payload_hex: String,
    /// Unix seconds when the owner accepted the message into the log.
    pub published_at: u64,
}

impl Message {
    /// Construct a message body (cursor is filled in by the durable writer on append; callers that
    /// build a message before it is logged pass `0`).
    pub fn new(cursor: Cursor, publisher: impl Into<String>, payload: &[u8], published_at: u64) -> Self {
        Message {
            cursor,
            publisher: publisher.into(),
            payload_hex: hex::encode(payload),
            published_at,
        }
    }

    /// Decode the payload bytes.
    pub fn data(&self) -> anyhow::Result<Vec<u8>> {
        hex::decode(&self.payload_hex).map_err(|e| anyhow::anyhow!("bad payload hex: {e}"))
    }

    /// The payload as UTF-8, lossily (for human-facing CLI display).
    pub fn text(&self) -> String {
        match hex::decode(&self.payload_hex) {
            Ok(b) => String::from_utf8_lossy(&b).into_owned(),
            Err(_) => String::new(),
        }
    }
}

/// Prefix shared by every ce-pubsub mesh name, so topics never collide with other apps' app-pubsub
/// or ce-coord topics on the same node.
const NS: &str = "ce-pubsub";

/// Validate a topic name. Topics must be non-empty, <= 200 chars, and contain only
/// `a-z` / `A-Z` / `0-9` / `.` / `-` / `_` / `:` — characters safe in a mesh topic name and an
/// `RVec` collection name. Rejecting the rest keeps the name space unambiguous and injection-free.
pub fn validate_topic(topic: &str) -> anyhow::Result<()> {
    if topic.is_empty() {
        anyhow::bail!("topic name must not be empty");
    }
    if topic.len() > 200 {
        anyhow::bail!("topic name too long ({} > 200)", topic.len());
    }
    let ok = topic
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':'));
    if !ok {
        anyhow::bail!(
            "topic '{topic}' has invalid characters (allowed: a-z A-Z 0-9 . - _ :)"
        );
    }
    Ok(())
}

/// The ce-coord collection name backing a topic's durable append log. The owner writes it; a puller
/// reads it as a replica of the owner.
pub fn log_name(topic: &str) -> String {
    format!("{NS}.log.{topic}")
}

/// The live mesh pub/sub topic a publisher fans messages out on and a live subscriber listens on.
/// Distinct from the durable-log topic so live broadcast and durable replay never interfere.
pub fn live_topic(topic: &str) -> String {
    format!("{NS}.live.{topic}")
}

/// The directed request topic a publisher sends a message to the owner on (so the owner can append
/// it to the durable log under its single-writer authority). Routed to the owner by NodeId.
pub fn ingest_topic(topic: &str) -> String {
    format!("{NS}.ingest.{topic}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_roundtrips_payload() {
        let m = Message::new(7, "abc", b"hello world", 1_700_000_000);
        assert_eq!(m.cursor, 7);
        assert_eq!(m.publisher, "abc");
        assert_eq!(m.data().unwrap(), b"hello world");
        assert_eq!(m.text(), "hello world");
    }

    #[test]
    fn message_serde_roundtrips() {
        let m = Message::new(1, "node", b"\x00\x01\xff", 42);
        let json = serde_json::to_string(&m).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn validate_topic_accepts_reasonable_names() {
        for t in ["orders", "orders.eu", "a-b_c:d", "T0pic", "x"] {
            assert!(validate_topic(t).is_ok(), "{t} should be valid");
        }
    }

    #[test]
    fn validate_topic_rejects_bad_names() {
        assert!(validate_topic("").is_err());
        assert!(validate_topic("has space").is_err());
        assert!(validate_topic("slash/here").is_err());
        assert!(validate_topic(&"x".repeat(201)).is_err());
    }

    #[test]
    fn mesh_names_are_namespaced_and_distinct() {
        let log = log_name("t");
        let live = live_topic("t");
        let ingest = ingest_topic("t");
        assert!(log.starts_with("ce-pubsub."));
        assert!(live.starts_with("ce-pubsub."));
        assert!(ingest.starts_with("ce-pubsub."));
        assert_ne!(log, live);
        assert_ne!(live, ingest);
        assert_ne!(log, ingest);
    }
}
