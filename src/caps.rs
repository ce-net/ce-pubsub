//! Capability-gated publish/subscribe — a scoped, time-bounded `ce-cap` chain granting
//! `pubsub:publish` or `pubsub:subscribe` on a named topic, encoded as a portable token.
//!
//! A managed Pub/Sub needs an authorization story: who may publish to a topic, and who may read it.
//! Google Pub/Sub uses IAM bindings on a central policy server. The CE equivalent is a signed,
//! attenuating `ce-cap` chain: the topic owner mints a capability whose abilities are
//! `["pubsub:publish"]` (or `["pubsub:subscribe"]` — opaque app strings CE assigns no meaning),
//! whose resource is the owning node, and whose caveats carry the expiry (`not_after`) and the topic
//! scope (`path_prefix`). The holder presents the token; the owner (the durable-log writer that
//! enforces access) verifies it offline in microseconds via [`ce_iam_core::authorize`], with no policy
//! server and no shared secret. Re-delegation (attenuation) is free.
//!
//! Abilities used by this app (opaque to `ce-cap`):
//! - `pubsub:publish`   — append messages to the topic.
//! - `pubsub:subscribe` — read / pull / replay messages from the topic.
//!
//! The topic scope lives in the `path_prefix` caveat as the topic name. The *leaf* scope is matched
//! against the requested topic by [`topic_allows`] (a raw string prefix: the requested topic must
//! start with the scoped prefix). So a single self-issued link scoped to `orders` covers `orders`,
//! `orders.eu`, `orders.eu.west`, etc.
//!
//! ## A note on multi-hop attenuation and topic separators
//!
//! When a holder **re-delegates** (a 2+ link chain), `ce-cap` enforces that each child link's
//! `path_prefix` is *narrower-or-equal* to its parent's — but it narrows on **`/` path segments**,
//! not on the `.` separators ce-pubsub topic names use. Concretely: a parent scoped to `orders` may
//! re-delegate `orders` (equal) but `ce-cap` does **not** consider `orders.eu` to be "within"
//! `orders` (no `/` boundary). Re-delegation of a `.`-topic therefore passes the *same* scope down
//! the chain, and the leaf `topic_allows` prefix check is what confines the requested topic. To get
//! true sub-scope attenuation between links, use a `/`-segmented scope (e.g. `team` → `team/eu`),
//! which `ce-cap` narrows correctly. This is by design (it reuses CE's one capability primitive) and
//! is covered by the multi-hop tests in `tests/edge_cases.rs`.

use anyhow::{Context, Result};
use ce_iam_core::{Caveats, Resource, SignedCapability};
use ce_identity::{Identity, NodeId};

/// Ability string: append messages to the scoped topic.
pub const ABILITY_PUBLISH: &str = "pubsub:publish";
/// Ability string: read / pull / replay messages from the scoped topic.
pub const ABILITY_SUBSCRIBE: &str = "pubsub:subscribe";

/// Does a capability scoped to `scope_topic` permit access to `topic`? True iff the requested topic
/// starts with the scoped prefix. An empty scope (`""`) covers every topic the owner holds. This is
/// the app caveat enforcement that `ce-cap` defers to the action.
pub fn topic_allows(scope_topic: &str, topic: &str) -> bool {
    topic.starts_with(scope_topic)
}

/// Mint a publish/subscribe access link: a single self-issued capability granting `ability` on the
/// topic scope `scope_topic`, valid until `not_after` (unix seconds, 0 = never), as a portable hex
/// token.
///
/// `owner` is the topic-owning identity (the root of the chain — a node always implicitly accepts
/// its own key as a root). `audience` is the holder the link is issued to; pass the owner's own node
/// id for an open bearer link, or a specific node id to bind it. `nonce` should be unique per issued
/// link so it can be revoked individually on-chain later.
pub fn mint_link(
    owner: &Identity,
    audience: NodeId,
    ability: &str,
    scope_topic: &str,
    not_after: u64,
    nonce: u64,
) -> Result<String> {
    let caveats = Caveats {
        not_after,
        path_prefix: Some(scope_topic.to_string()),
        ..Default::default()
    };
    let cap = SignedCapability::issue(
        owner,
        audience,
        vec![ability.to_string()],
        Resource::Node(owner.node_id()),
        caveats,
        nonce,
        None,
    );
    Ok(ce_iam_core::encode_chain(&[cap]))
}

/// Decode a link token back into the leaf capability's abilities and topic scope for inspection.
/// Does not verify the signature/expiry — call [`verify_link`] for that.
pub fn inspect_link(token: &str) -> Result<(Vec<String>, String)> {
    let chain = ce_iam_core::decode_chain(token).context("decoding capability link")?;
    let leaf = chain.last().context("empty capability chain")?;
    let scope = leaf.cap.caveats.path_prefix.clone().unwrap_or_default();
    Ok((leaf.cap.abilities.clone(), scope))
}

/// Verify a presented link against the owning node's identity for `ability` on `topic`.
///
/// Runs the full `ce-cap` chain check (signature, attenuation, temporal caveats, revocation) rooted
/// at `self_id` (or `accepted_roots`), then enforces the app-level topic-prefix scope caveat. The
/// requester is the leaf audience. `now` is unix seconds; `is_revoked` consults the on-chain set.
#[allow(clippy::too_many_arguments)]
pub fn verify_link(
    self_id: &NodeId,
    accepted_roots: &[NodeId],
    self_tags: &[String],
    now: u64,
    requester: &NodeId,
    ability: &str,
    topic: &str,
    token: &str,
    is_revoked: &dyn Fn(&NodeId, u64) -> bool,
) -> Result<(), String> {
    let chain = ce_iam_core::decode_chain(token).map_err(|e| e.to_string())?;
    // ce-cap enforces signatures, attenuation, expiry, revocation, and that the leaf grants `ability`.
    ce_iam_core::authorize(
        self_id,
        accepted_roots,
        self_tags,
        now,
        requester,
        ability,
        &chain,
        is_revoked,
    )?;
    // App-level: the topic-prefix caveat must cover the requested topic.
    let leaf = chain.last().ok_or_else(|| "empty chain".to_string())?;
    let scope = leaf
        .cap
        .caveats
        .path_prefix
        .clone()
        .ok_or_else(|| "link has no topic scope".to_string())?;
    if !topic_allows(&scope, topic) {
        return Err(format!(
            "link scope '{scope}' does not cover topic '{topic}'"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_identity::Identity;

    fn ident(seed: &str) -> Identity {
        let dir =
            std::env::temp_dir().join(format!("ce-pubsub-cap-{}-{}", seed, std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let id = Identity::load_or_generate(&dir).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        id
    }

    fn never_revoked(_: &NodeId, _: u64) -> bool {
        false
    }

    #[test]
    fn topic_allows_prefix() {
        assert!(topic_allows("orders", "orders"));
        assert!(topic_allows("orders", "orders.eu"));
        assert!(topic_allows("", "anything"));
        assert!(!topic_allows("orders", "payments"));
    }

    #[test]
    fn mint_and_verify_publish_link() {
        let owner = ident("owner");
        let token = mint_link(&owner, owner.node_id(), ABILITY_PUBLISH, "orders", 0, 1).unwrap();
        let r = verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &owner.node_id(),
            ABILITY_PUBLISH,
            "orders",
            &token,
            &never_revoked,
        );
        assert!(r.is_ok(), "valid link should verify: {r:?}");
    }

    #[test]
    fn link_rejects_out_of_scope_topic() {
        let owner = ident("owner2");
        let token = mint_link(&owner, owner.node_id(), ABILITY_SUBSCRIBE, "orders", 0, 2).unwrap();
        let r = verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &owner.node_id(),
            ABILITY_SUBSCRIBE,
            "payments",
            &token,
            &never_revoked,
        );
        assert!(r.is_err(), "out-of-scope topic must be rejected");
    }

    #[test]
    fn link_rejects_wrong_ability() {
        let owner = ident("owner3");
        // a subscribe-only link must not authorize publish
        let token = mint_link(&owner, owner.node_id(), ABILITY_SUBSCRIBE, "t", 0, 3).unwrap();
        let r = verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &owner.node_id(),
            ABILITY_PUBLISH,
            "t",
            &token,
            &never_revoked,
        );
        assert!(r.is_err(), "wrong ability must be rejected");
    }

    #[test]
    fn link_rejects_expired() {
        let owner = ident("owner4");
        let token = mint_link(&owner, owner.node_id(), ABILITY_PUBLISH, "t", 500, 4).unwrap();
        let r = verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000, // now > not_after
            &owner.node_id(),
            ABILITY_PUBLISH,
            "t",
            &token,
            &never_revoked,
        );
        assert!(r.is_err(), "expired link must be rejected");
    }

    #[test]
    fn inspect_reports_scope_and_abilities() {
        let owner = ident("owner5");
        let token = mint_link(&owner, owner.node_id(), ABILITY_PUBLISH, "logs", 0, 5).unwrap();
        let (abilities, scope) = inspect_link(&token).unwrap();
        assert_eq!(abilities, vec![ABILITY_PUBLISH.to_string()]);
        assert_eq!(scope, "logs");
    }

    #[test]
    fn revocation_kills_link() {
        let owner = ident("owner6");
        let token = mint_link(&owner, owner.node_id(), ABILITY_PUBLISH, "t", 0, 99).unwrap();
        let revoke_99 = |_i: &NodeId, nonce: u64| nonce == 99;
        let r = verify_link(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &owner.node_id(),
            ABILITY_PUBLISH,
            "t",
            &token,
            &revoke_99,
        );
        assert!(r.unwrap_err().contains("revoked"));
    }
}
