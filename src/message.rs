//! The Pub/Sub message model and the topic → mesh-name mapping.
//!
//! A [`Message`] is one durable record in a topic's append log. Every message carries a monotonic
//! [`Cursor`] (its 1-based position in the log — the same number the durable [`RVec`] assigns as a
//! version), the publisher's authenticated NodeId, the opaque payload, and the unix-second publish
//! time. A subscriber that pulls `--from <cursor>` receives every message at a position strictly
//! greater than that cursor, which is exactly at-least-once replay.
//!
//! [`RVec`]: ce_coord::RVec

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A monotonic message position within a topic's log. The first message is at cursor 1; cursor 0
/// means "from the very beginning" when pulling. A cursor equals the [`ce_coord::Version`] the
/// durable log assigned when the message was appended.
pub type Cursor = u64;

/// Maximum size, in bytes, of a single message payload accepted by [`crate::Topic::publish`] and the
/// remote ingest path. Mirrors Google Pub/Sub's 10 MiB per-message ceiling. Enforced *before* the
/// payload is appended to the in-memory durable log, so an oversized publish is rejected rather than
/// retained — closing the unbounded-memory DoS vector a single huge publish would otherwise open.
pub const MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Maximum number of attribute key/value pairs on one message.
pub const MAX_ATTRIBUTES: usize = 100;

/// Maximum length (bytes) of any single attribute key or value, and of an ordering key.
pub const MAX_ATTRIBUTE_LEN: usize = 1024;

/// One durable Pub/Sub message: a payload plus delivery metadata. Stored verbatim in the topic's
/// append log so a late subscriber replays the exact bytes the publisher sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// 1-based position in the topic log (assigned by the durable writer on append).
    pub cursor: Cursor,
    /// Authenticated publisher NodeId (hex) for cap-gated topics; the transport-claimed sender for
    /// open topics. See [`Message::publisher_is_authenticated`] — on an open topic this value is
    /// *claimed*, not cryptographically verified, so do not trust it for authorization decisions.
    pub publisher: String,
    /// Opaque payload, hex-encoded so it rides JSON cleanly. Use [`Message::data`] to decode.
    pub payload_hex: String,
    /// Unix seconds when the owner accepted the message into the log.
    pub published_at: u64,
    /// Optional per-key FIFO ordering key (Google Pub/Sub `orderingKey`). Messages sharing an
    /// ordering key keep their relative publish order; the global cursor order already subsumes this
    /// for a single-writer topic, but the key is preserved for attribute-filtered consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordering_key: Option<String>,
    /// Optional string key/value attributes (Google Pub/Sub message attributes). Used for
    /// subscription-side filtering without decoding the payload.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
    /// `true` iff the recorded [`publisher`](Self::publisher) was verified via a `ce-cap` chain
    /// (cap-gated topic). On an open topic this is `false` and `publisher` is transport-claimed only.
    #[serde(default)]
    pub publisher_authenticated: bool,
}

impl Message {
    /// Construct a message body (cursor is filled in by the durable writer on append; callers that
    /// build a message before it is logged pass `0`). No attributes, no ordering key, publisher
    /// unauthenticated. Use the builder methods to enrich.
    pub fn new(
        cursor: Cursor,
        publisher: impl Into<String>,
        payload: &[u8],
        published_at: u64,
    ) -> Self {
        Message {
            cursor,
            publisher: publisher.into(),
            payload_hex: hex::encode(payload),
            published_at,
            ordering_key: None,
            attributes: BTreeMap::new(),
            publisher_authenticated: false,
        }
    }

    /// Set the ordering key (builder).
    pub fn with_ordering_key(mut self, key: impl Into<String>) -> Self {
        self.ordering_key = Some(key.into());
        self
    }

    /// Set the attribute map (builder).
    pub fn with_attributes(mut self, attrs: BTreeMap<String, String>) -> Self {
        self.attributes = attrs;
        self
    }

    /// Mark the publisher as cap-verified (builder). Set by the owner only after a successful
    /// capability check.
    pub fn authenticated(mut self, yes: bool) -> Self {
        self.publisher_authenticated = yes;
        self
    }

    /// Whether the recorded publisher was cryptographically authenticated (cap-gated topic).
    pub fn publisher_is_authenticated(&self) -> bool {
        self.publisher_authenticated
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

/// Optional metadata a publisher can attach to a message: an ordering key and string attributes.
/// Default is empty (no key, no attributes), matching the original `publish(bytes)` behavior.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishOptions {
    /// Optional per-key FIFO ordering key.
    pub ordering_key: Option<String>,
    /// String key/value attributes for subscription-side filtering.
    pub attributes: BTreeMap<String, String>,
}

impl PublishOptions {
    /// An empty options set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the ordering key (builder).
    pub fn ordering_key(mut self, key: impl Into<String>) -> Self {
        self.ordering_key = Some(key.into());
        self
    }

    /// Add one attribute (builder).
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Validate the options against the configured bounds.
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(k) = &self.ordering_key
            && k.len() > MAX_ATTRIBUTE_LEN
        {
            anyhow::bail!("ordering key too long ({} > {MAX_ATTRIBUTE_LEN})", k.len());
        }
        validate_attributes(&self.attributes)
    }
}

/// Validate a payload's size against [`MAX_PAYLOAD_BYTES`]. Called on every publish and ingest before
/// the bytes touch the durable log.
pub fn validate_payload(payload: &[u8]) -> anyhow::Result<()> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        anyhow::bail!(
            "payload too large ({} bytes > {MAX_PAYLOAD_BYTES} limit)",
            payload.len()
        );
    }
    Ok(())
}

/// Validate an attribute map against [`MAX_ATTRIBUTES`] and [`MAX_ATTRIBUTE_LEN`].
pub fn validate_attributes(attrs: &BTreeMap<String, String>) -> anyhow::Result<()> {
    if attrs.len() > MAX_ATTRIBUTES {
        anyhow::bail!("too many attributes ({} > {MAX_ATTRIBUTES})", attrs.len());
    }
    for (k, v) in attrs {
        if k.is_empty() {
            anyhow::bail!("attribute key must not be empty");
        }
        if k.len() > MAX_ATTRIBUTE_LEN {
            anyhow::bail!(
                "attribute key '{k}' too long ({} > {MAX_ATTRIBUTE_LEN})",
                k.len()
            );
        }
        if v.len() > MAX_ATTRIBUTE_LEN {
            anyhow::bail!(
                "attribute '{k}' value too long ({} > {MAX_ATTRIBUTE_LEN})",
                v.len()
            );
        }
    }
    Ok(())
}

/// A simple, safe subscription filter over message attributes — the subset of Google Pub/Sub's
/// filter syntax that maps cleanly onto exact-match semantics. A filter is a set of
/// `key == "value"` clauses joined by implicit AND; a message matches iff every clause matches one
/// of its attributes. An empty filter matches everything. This is deliberately a closed grammar (no
/// arbitrary expression evaluation) so it can never be an injection or DoS vector.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttributeFilter {
    clauses: Vec<(String, String)>,
}

impl AttributeFilter {
    /// A filter that matches every message.
    pub fn any() -> Self {
        Self::default()
    }

    /// Require `attributes[key] == value` (builder; clauses AND together).
    pub fn require(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.clauses.push((key.into(), value.into()));
        self
    }

    /// True if this filter has no clauses (matches everything).
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }

    /// Does `msg` satisfy every clause?
    pub fn matches(&self, msg: &Message) -> bool {
        self.clauses
            .iter()
            .all(|(k, v)| msg.attributes.get(k).map(|got| got == v).unwrap_or(false))
    }

    /// Parse a filter from the compact `key="value" key2="value2"` form (whitespace-separated
    /// clauses). Returns an error on malformed input. Supports the `attributes.` prefix Google uses
    /// (stripped). Values must be double-quoted; keys are bare identifiers.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::any());
        }
        let mut clauses = Vec::new();
        // Tokenize on `=`-separated clauses; we scan manually to respect quoted values that may
        // contain spaces.
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= bytes.len() {
                break;
            }
            // key: up to '='
            let key_start = i;
            while i < bytes.len() && bytes[i] != b'=' {
                i += 1;
            }
            if i >= bytes.len() {
                anyhow::bail!("filter clause missing '=' near byte {key_start}");
            }
            let mut key = s[key_start..i].trim().to_string();
            if let Some(rest) = key.strip_prefix("attributes.") {
                key = rest.to_string();
            }
            // skip optional second '=' (support both == and =)
            i += 1; // consume '='
            if i < bytes.len() && bytes[i] == b'=' {
                i += 1;
            }
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= bytes.len() || bytes[i] != b'"' {
                anyhow::bail!("filter value for key '{key}' must be double-quoted");
            }
            i += 1; // opening quote
            let val_start = i;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            if i >= bytes.len() {
                anyhow::bail!("unterminated filter value for key '{key}'");
            }
            let value = s[val_start..i].to_string();
            i += 1; // closing quote
            if key.is_empty() {
                anyhow::bail!("filter clause has an empty key");
            }
            clauses.push((key, value));
        }
        Ok(Self { clauses })
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
        anyhow::bail!("topic '{topic}' has invalid characters (allowed: a-z A-Z 0-9 . - _ :)");
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

/// The directed control topic a subscriber sends ack/pull/create-subscription requests to the owner
/// on. The owner answers them under its single-writer authority over the subscription state log.
pub fn control_topic(topic: &str) -> String {
    format!("{NS}.ctl.{topic}")
}

/// The ce-coord collection name backing a topic's subscription registry (per-subscription durable
/// ack cursors). Owned and written by the topic owner alongside the message log.
pub fn subs_log_name(topic: &str) -> String {
    format!("{NS}.subs.{topic}")
}

/// The configured dead-letter topic name for `topic`: `<topic>.dlq`. Poison messages that exceed a
/// subscription's max delivery attempts are republished here by the owner.
pub fn dead_letter_topic(topic: &str) -> String {
    format!("{topic}.dlq")
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
        let ctl = control_topic("t");
        let subs = subs_log_name("t");
        let names = [&log, &live, &ingest, &ctl, &subs];
        for n in names {
            assert!(n.starts_with("ce-pubsub."), "{n} must be namespaced");
        }
        // All distinct.
        for i in 0..names.len() {
            for j in (i + 1)..names.len() {
                assert_ne!(names[i], names[j], "names {i} and {j} collide");
            }
        }
    }

    #[test]
    fn dead_letter_name() {
        assert_eq!(dead_letter_topic("orders"), "orders.dlq");
    }

    #[test]
    fn payload_size_limit_enforced() {
        assert!(validate_payload(&[0u8; 100]).is_ok());
        let at_limit = vec![0u8; MAX_PAYLOAD_BYTES];
        assert!(validate_payload(&at_limit).is_ok());
        let over_limit = vec![0u8; MAX_PAYLOAD_BYTES + 1];
        assert!(validate_payload(&over_limit).is_err());
    }

    #[test]
    fn attribute_bounds_enforced() {
        let mut ok = BTreeMap::new();
        ok.insert("k".to_string(), "v".to_string());
        assert!(validate_attributes(&ok).is_ok());

        let mut empty_key = BTreeMap::new();
        empty_key.insert(String::new(), "v".to_string());
        assert!(validate_attributes(&empty_key).is_err());

        let mut long_val = BTreeMap::new();
        long_val.insert("k".to_string(), "x".repeat(MAX_ATTRIBUTE_LEN + 1));
        assert!(validate_attributes(&long_val).is_err());

        let too_many: BTreeMap<String, String> = (0..=MAX_ATTRIBUTES)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        assert!(validate_attributes(&too_many).is_err());
    }

    #[test]
    fn publish_options_builder_and_validate() {
        let opts = PublishOptions::new()
            .ordering_key("region-eu")
            .attribute("kind", "order")
            .attribute("priority", "high");
        assert_eq!(opts.ordering_key.as_deref(), Some("region-eu"));
        assert_eq!(opts.attributes.len(), 2);
        assert!(opts.validate().is_ok());

        let bad = PublishOptions::new().ordering_key("x".repeat(MAX_ATTRIBUTE_LEN + 1));
        assert!(bad.validate().is_err());
    }

    #[test]
    fn message_builders_set_fields() {
        let mut attrs = BTreeMap::new();
        attrs.insert("kind".to_string(), "order".to_string());
        let m = Message::new(1, "n", b"hi", 1)
            .with_ordering_key("k1")
            .with_attributes(attrs.clone())
            .authenticated(true);
        assert_eq!(m.ordering_key.as_deref(), Some("k1"));
        assert_eq!(m.attributes, attrs);
        assert!(m.publisher_is_authenticated());
    }

    #[test]
    fn message_with_attributes_serde_roundtrips() {
        let m = Message::new(3, "n", b"\x00\xff", 7)
            .with_ordering_key("order-key")
            .attribute_for_test("a", "1");
        let json = serde_json::to_string(&m).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn attribute_filter_matches_exact() {
        let m = Message::new(1, "n", b"x", 1).attribute_for_test("kind", "order");
        assert!(AttributeFilter::any().matches(&m));
        assert!(AttributeFilter::any().require("kind", "order").matches(&m));
        assert!(
            !AttributeFilter::any()
                .require("kind", "payment")
                .matches(&m)
        );
        assert!(!AttributeFilter::any().require("missing", "x").matches(&m));
        // Multi-clause AND.
        let m2 = m.clone().attribute_for_test("priority", "high");
        assert!(
            AttributeFilter::any()
                .require("kind", "order")
                .require("priority", "high")
                .matches(&m2)
        );
        assert!(
            !AttributeFilter::any()
                .require("kind", "order")
                .require("priority", "low")
                .matches(&m2)
        );
    }

    #[test]
    fn attribute_filter_parse() {
        let f = AttributeFilter::parse(r#"kind="order" priority="high""#).unwrap();
        let m = Message::new(1, "n", b"x", 1)
            .attribute_for_test("kind", "order")
            .attribute_for_test("priority", "high");
        assert!(f.matches(&m));

        // attributes. prefix is stripped, == accepted.
        let f2 = AttributeFilter::parse(r#"attributes.kind == "order""#).unwrap();
        assert!(f2.matches(&m));

        // empty → match all.
        assert!(AttributeFilter::parse("   ").unwrap().is_empty());

        // malformed.
        assert!(AttributeFilter::parse("kind=order").is_err()); // unquoted value
        assert!(AttributeFilter::parse(r#"kind="order"#).is_err()); // unterminated
        assert!(AttributeFilter::parse(r#"="v""#).is_err()); // empty key
    }

    // Test-only helper so message-builder tests read cleanly.
    impl Message {
        fn attribute_for_test(mut self, k: &str, v: &str) -> Self {
            self.attributes.insert(k.to_string(), v.to_string());
            self
        }
    }
}
