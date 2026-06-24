//! Wire protocol for the directed (request/reply) paths between a publisher/consumer node and a
//! topic owner: remote publish (ingest) and subscription control (create/pull/ack/nack/delete).
//!
//! Every request rides a single `ce-rs` directed [`request`](ce_rs::CeClient::request) to the owner's
//! NodeId on the topic's ingest or control mesh-topic, carrying a JSON-encoded [`IngestRequest`] or
//! [`ControlRequest`]; the owner answers with the matching reply enum. JSON (not bincode) is used on
//! this hop because the SDK request/reply path is already string/JSON-shaped and the payloads are
//! small control messages; the durable log itself uses ce-coord's deterministic snapshot encoding.
//!
//! All decode paths are bounded: oversized hex or bodies are rejected before allocation (see
//! [`MAX_CONTROL_BODY_BYTES`]).

use serde::{Deserialize, Serialize};

use crate::message::{Cursor, Message};
use crate::subscription::SubscriptionPolicy;

/// Maximum size (bytes) of a decoded control-request body. Control messages are tiny; anything
/// larger is malicious or buggy and is rejected before deserialization to bound memory.
pub const MAX_CONTROL_BODY_BYTES: usize = 64 * 1024;

/// Maximum size (bytes) of the hex-encoded ingest payload string on the wire. `2 *
/// MAX_PAYLOAD_BYTES` (hex doubles) plus headroom for the JSON envelope. Rejected on decode to bound
/// memory on the owner before it allocates the decoded payload.
pub const MAX_INGEST_HEX_BYTES: usize = 2 * crate::message::MAX_PAYLOAD_BYTES + 64 * 1024;

/// A remote publish request sent to a topic owner over the ingest topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestRequest {
    /// Hex-encoded payload bytes.
    pub payload_hex: String,
    /// Optional `ce-cap` chain token granting `pubsub:publish` on this topic. Required only when the
    /// owner runs cap-gated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<String>,
    /// Optional publisher-chosen idempotency key. If two ingest requests carry the same key, the
    /// owner appends only once and returns the original cursor on the retry — making remote publish
    /// safe to retry after a lost reply. Bounded to 256 bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Optional per-key FIFO ordering key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordering_key: Option<String>,
    /// Optional message attributes.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// Maximum length of an idempotency key.
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 256;

impl IngestRequest {
    /// Encode to JSON bytes for the wire.
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Decode from JSON bytes, rejecting an oversized hex payload before further work.
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() > MAX_INGEST_HEX_BYTES {
            anyhow::bail!("ingest request too large ({} bytes)", bytes.len());
        }
        let req: IngestRequest = serde_json::from_slice(bytes)?;
        if req.payload_hex.len() > MAX_INGEST_HEX_BYTES {
            anyhow::bail!(
                "ingest payload hex too large ({} bytes)",
                req.payload_hex.len()
            );
        }
        if let Some(k) = &req.idempotency_key
            && k.len() > MAX_IDEMPOTENCY_KEY_LEN
        {
            anyhow::bail!("idempotency key too long ({} bytes)", k.len());
        }
        Ok(req)
    }

    /// Decode from a hex-encoded JSON body (as it arrives on an [`ce_rs::AppMessage::payload_hex`]),
    /// bounding the hex string length before allocating the decoded bytes. This is the owner-side
    /// entry point for an inbound ingest request.
    pub fn decode_hex(payload_hex: &str) -> anyhow::Result<Self> {
        if payload_hex.len() > MAX_INGEST_HEX_BYTES {
            anyhow::bail!("ingest request hex too large ({} bytes)", payload_hex.len());
        }
        let bytes = hex::decode(payload_hex)?;
        Self::decode(&bytes)
    }
}

/// The owner's reply to an [`IngestRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestReply {
    /// Accepted and appended at this cursor (or, for an idempotent retry, the original cursor).
    Accepted(Cursor),
    /// Rejected (capability failure, oversize, malformed); the string explains why.
    Rejected(String),
}

impl IngestReply {
    /// Encode to JSON bytes.
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    /// Decode from JSON bytes.
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// A subscription-control request a consumer sends the owner over the control topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlRequest {
    /// Create (or update the policy of) a named subscription.
    CreateSubscription {
        name: String,
        policy: SubscriptionPolicy,
        /// Optional `pubsub:subscribe` capability for a cap-gated topic.
        grant: Option<String>,
    },
    /// Delete a subscription.
    DeleteSubscription { name: String, grant: Option<String> },
    /// List subscription names.
    ListSubscriptions { grant: Option<String> },
    /// Ask the owner for the topic's current high-water cursor (used by honest pull convergence).
    HighCursor { grant: Option<String> },
    /// Lease up to `max_messages` redeliverable messages from a subscription, advancing nothing until
    /// they are acked. Returns the leased [`Message`]s with their delivery-attempt numbers.
    Pull {
        name: String,
        max_messages: usize,
        grant: Option<String>,
    },
    /// Acknowledge a leased cursor (it will not be redelivered).
    Ack {
        name: String,
        cursor: Cursor,
        grant: Option<String>,
    },
    /// Negative-ack a leased cursor (redeliver it immediately).
    Nack {
        name: String,
        cursor: Cursor,
        grant: Option<String>,
    },
}

impl ControlRequest {
    /// Encode to JSON bytes.
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    /// Decode from JSON bytes, bounding the body size first.
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() > MAX_CONTROL_BODY_BYTES {
            anyhow::bail!("control request too large ({} bytes)", bytes.len());
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    /// The capability grant carried by this request, if any (so the owner can authorize uniformly).
    pub fn grant(&self) -> Option<&str> {
        match self {
            ControlRequest::CreateSubscription { grant, .. }
            | ControlRequest::DeleteSubscription { grant, .. }
            | ControlRequest::ListSubscriptions { grant }
            | ControlRequest::HighCursor { grant }
            | ControlRequest::Pull { grant, .. }
            | ControlRequest::Ack { grant, .. }
            | ControlRequest::Nack { grant, .. } => grant.as_deref(),
        }
    }
}

/// One leased message returned by a [`ControlRequest::Pull`]: the message plus its delivery attempt
/// number (1 on first delivery), so a consumer can implement its own give-up logic if it wants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeasedMessage {
    pub message: Message,
    pub delivery_attempt: u32,
}

/// The owner's reply to a [`ControlRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlReply {
    /// CreateSubscription / DeleteSubscription / Ack / Nack succeeded.
    Ok,
    /// HighCursor result.
    High(Cursor),
    /// ListSubscriptions result.
    Subscriptions(Vec<String>),
    /// Pull result: the leased messages, in cursor order.
    Pulled(Vec<LeasedMessage>),
    /// The request was rejected; the string explains why.
    Rejected(String),
}

impl ControlReply {
    /// Encode to JSON bytes.
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    /// Decode from JSON bytes.
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() > MAX_INGEST_HEX_BYTES {
            anyhow::bail!("control reply too large ({} bytes)", bytes.len());
        }
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::SubscriptionPolicy;

    #[test]
    fn ingest_request_roundtrips() {
        let req = IngestRequest {
            payload_hex: hex::encode(b"hi"),
            grant: Some("tok".into()),
            idempotency_key: Some("key-1".into()),
            ordering_key: Some("ord".into()),
            attributes: [("k".to_string(), "v".to_string())].into_iter().collect(),
        };
        let bytes = req.encode().unwrap();
        let back = IngestRequest::decode(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn ingest_request_decode_hex_roundtrips_and_bounds() {
        let req = IngestRequest {
            payload_hex: hex::encode(b"body"),
            grant: None,
            idempotency_key: None,
            ordering_key: None,
            attributes: Default::default(),
        };
        let body = req.encode().unwrap();
        let body_hex = hex::encode(&body);
        assert_eq!(IngestRequest::decode_hex(&body_hex).unwrap(), req);
        // An oversize hex string is rejected before decode.
        assert!(IngestRequest::decode_hex(&"a".repeat(MAX_INGEST_HEX_BYTES + 2)).is_err());
        // Non-hex input is a clean error, not a panic.
        assert!(IngestRequest::decode_hex("zz not hex").is_err());
    }

    #[test]
    fn ingest_request_rejects_oversize_hex() {
        let huge = IngestRequest {
            payload_hex: "a".repeat(MAX_INGEST_HEX_BYTES + 1),
            grant: None,
            idempotency_key: None,
            ordering_key: None,
            attributes: Default::default(),
        };
        // encode produces a big body; decode must reject it.
        let bytes = huge.encode().unwrap();
        assert!(IngestRequest::decode(&bytes).is_err());
    }

    #[test]
    fn ingest_request_rejects_long_idempotency_key() {
        let req = IngestRequest {
            payload_hex: hex::encode(b"x"),
            grant: None,
            idempotency_key: Some("k".repeat(MAX_IDEMPOTENCY_KEY_LEN + 1)),
            ordering_key: None,
            attributes: Default::default(),
        };
        let bytes = req.encode().unwrap();
        assert!(IngestRequest::decode(&bytes).is_err());
    }

    #[test]
    fn ingest_reply_roundtrips() {
        for r in [IngestReply::Accepted(9), IngestReply::Rejected("no".into())] {
            let b = r.encode().unwrap();
            assert_eq!(IngestReply::decode(&b).unwrap(), r);
        }
    }

    #[test]
    fn control_request_roundtrips_all_variants() {
        let reqs = vec![
            ControlRequest::CreateSubscription {
                name: "s".into(),
                policy: SubscriptionPolicy::default(),
                grant: None,
            },
            ControlRequest::DeleteSubscription {
                name: "s".into(),
                grant: Some("g".into()),
            },
            ControlRequest::ListSubscriptions { grant: None },
            ControlRequest::HighCursor { grant: None },
            ControlRequest::Pull {
                name: "s".into(),
                max_messages: 10,
                grant: None,
            },
            ControlRequest::Ack {
                name: "s".into(),
                cursor: 5,
                grant: None,
            },
            ControlRequest::Nack {
                name: "s".into(),
                cursor: 5,
                grant: None,
            },
        ];
        for req in reqs {
            let b = req.encode().unwrap();
            assert_eq!(ControlRequest::decode(&b).unwrap(), req);
        }
    }

    #[test]
    fn control_request_rejects_oversize_body() {
        let mut big = Vec::with_capacity(MAX_CONTROL_BODY_BYTES + 10);
        big.resize(MAX_CONTROL_BODY_BYTES + 1, b'x');
        assert!(ControlRequest::decode(&big).is_err());
    }

    #[test]
    fn control_reply_roundtrips() {
        let replies = vec![
            ControlReply::Ok,
            ControlReply::High(42),
            ControlReply::Subscriptions(vec!["a".into(), "b".into()]),
            ControlReply::Pulled(vec![LeasedMessage {
                message: Message::new(1, "n", b"x", 1),
                delivery_attempt: 2,
            }]),
            ControlReply::Rejected("why".into()),
        ];
        for r in replies {
            let b = r.encode().unwrap();
            assert_eq!(ControlReply::decode(&b).unwrap(), r);
        }
    }

    #[test]
    fn grant_accessor_covers_every_variant() {
        assert_eq!(
            ControlRequest::Ack {
                name: "s".into(),
                cursor: 1,
                grant: Some("g".into())
            }
            .grant(),
            Some("g")
        );
        assert_eq!(ControlRequest::HighCursor { grant: None }.grant(), None);
    }
}
