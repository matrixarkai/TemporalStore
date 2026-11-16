// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Resolving a chain of dump manifests into the ordered list that makes one image.
//!
//! A manifest is either WHOLE (it carries the complete shard index) or a DELTA (it names the manifest
//! it builds on and carries only what changed). Recovery needs the whole manifest first and every
//! delta after it, in order. This module decides which manifests those are and refuses anything
//! short of the full set: a missing link, a cycle, a chain that crosses shards, or one whose replay
//! watermark runs backwards each end in an error, never in a shorter chain that would install a
//! shard missing buckets.
//!
//! The functions work on [`ChainLink`], a small view of a manifest, so the rules can be tested
//! without building manifests and so the same rules serve every reader of the chain.
use super::BucketDumpManifest;
use std::collections::{BTreeMap, BTreeSet};

/// A delta manifest was asked for the image it does not carry.
///
/// Every reader that wants a manifest's index image asks [`installable_image`], so a manifest that
/// only names what changed is refused in one place with one message, never read as an empty or short
/// image. Resolving a delta onto its base is added with the writer that produces one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DeltaImageUnreadable {
    pub(super) manifest_id: String,
    pub(super) base_manifest_id: String,
}

impl std::fmt::Display for DeltaImageUnreadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "manifest {} builds on {} and carries no whole index image",
            self.manifest_id, self.base_manifest_id
        )
    }
}

/// The whole-shard index image a manifest installs.
///
/// The ONE door to `BucketDumpManifest::index_bytes` for anything that decodes, digests or installs
/// the image; `no_reader_takes_a_manifests_image_around_the_resolver` pins that. A whole manifest
/// returns its own bytes (possibly empty, which each caller already treats as a partial manifest);
/// a delta returns an error.
pub(super) fn installable_image(
    manifest: &BucketDumpManifest,
) -> Result<&[u8], DeltaImageUnreadable> {
    match &manifest.base_manifest_id {
        None => Ok(&manifest.index_bytes),
        Some(base) => Err(DeltaImageUnreadable {
            manifest_id: manifest.manifest_id.clone(),
            base_manifest_id: base.clone(),
        }),
    }
}

impl ChainLink {
    pub(super) fn of(manifest: &BucketDumpManifest) -> Self {
        Self {
            manifest_id: manifest.manifest_id.clone(),
            shard_id: manifest.shard_id,
            base_manifest_id: manifest.base_manifest_id.clone(),
            wal_sequence: manifest.wal_sequence,
        }
    }
}

/// The most links followed before the chain is refused. A real chain is folded into a whole
/// manifest long before this; the bound is what turns a corrupt, very long chain into an error.
pub(super) const MAX_CHAIN_LEN: usize = 4096;

/// What the chain rules need to know about one manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ChainLink {
    pub(super) manifest_id: String,
    pub(super) shard_id: u64,
    /// The manifest this one builds on. `None` for a whole manifest.
    pub(super) base_manifest_id: Option<String>,
    pub(super) wal_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ChainError {
    /// The head is not among the manifests given.
    UnknownHead { manifest_id: String },
    /// A link names a base that is not among the manifests given.
    MissingBase {
        manifest_id: String,
        base_manifest_id: String,
    },
    /// Following bases returned to a manifest already seen.
    Cycle { manifest_id: String },
    /// A link belongs to a different shard than the head.
    ShardMismatch {
        manifest_id: String,
        expected: u64,
        found: u64,
    },
    /// A delta's replay watermark is below the manifest it builds on.
    WatermarkBackwards {
        manifest_id: String,
        base_manifest_id: String,
    },
    /// More than [`MAX_CHAIN_LEN`] links.
    TooLong { manifest_id: String },
}

/// The chain that ends at `head_id`, whole manifest first and `head_id` last.
pub(super) fn resolve_chain<'a>(
    links: &'a [ChainLink],
    head_id: &str,
) -> Result<Vec<&'a ChainLink>, ChainError> {
    let by_id: BTreeMap<&str, &ChainLink> = links
        .iter()
        .map(|link| (link.manifest_id.as_str(), link))
        .collect();
    let head = *by_id.get(head_id).ok_or_else(|| ChainError::UnknownHead {
        manifest_id: head_id.to_string(),
    })?;
    let mut chain = vec![head];
    let mut seen: BTreeSet<&str> = BTreeSet::from([head.manifest_id.as_str()]);
    let mut current = head;
    while let Some(base_id) = current.base_manifest_id.as_deref() {
        let base = *by_id
            .get(base_id)
            .ok_or_else(|| ChainError::MissingBase {
                manifest_id: current.manifest_id.clone(),
                base_manifest_id: base_id.to_string(),
            })?;
        if !seen.insert(base.manifest_id.as_str()) {
            return Err(ChainError::Cycle {
                manifest_id: base.manifest_id.clone(),
            });
        }
        if base.shard_id != head.shard_id {
            return Err(ChainError::ShardMismatch {
                manifest_id: base.manifest_id.clone(),
                expected: head.shard_id,
                found: base.shard_id,
            });
        }
        if current.wal_sequence < base.wal_sequence {
            return Err(ChainError::WatermarkBackwards {
                manifest_id: current.manifest_id.clone(),
                base_manifest_id: base.manifest_id.clone(),
            });
        }
        chain.push(base);
        if chain.len() > MAX_CHAIN_LEN {
            return Err(ChainError::TooLong {
                manifest_id: head.manifest_id.clone(),
            });
        }
        current = base;
    }
    chain.reverse();
    Ok(chain)
}

/// The manifests retention must keep so that `newest_id` still installs: its whole chain.
///
/// An error is returned rather than a smaller set, because a set that omits a link is the
/// retention bug this exists to prevent. The caller treats an error as "keep everything".
pub(super) fn chain_to_retain(
    links: &[ChainLink],
    newest_id: &str,
) -> Result<BTreeSet<String>, ChainError> {
    Ok(resolve_chain(links, newest_id)?
        .into_iter()
        .map(|link| link.manifest_id.clone())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(id: &str, base: Option<&str>, wal: u64) -> ChainLink {
        ChainLink {
            manifest_id: id.to_string(),
            shard_id: 1,
            base_manifest_id: base.map(str::to_string),
            wal_sequence: wal,
        }
    }

    fn ids(chain: &[&ChainLink]) -> Vec<String> {
        chain.iter().map(|link| link.manifest_id.clone()).collect()
    }

    #[test]
    fn a_whole_manifest_is_its_own_chain() {
        let links = vec![link("w", None, 10)];
        assert_eq!(ids(&resolve_chain(&links, "w").unwrap()), ["w"]);
    }

    #[test]
    fn a_chain_resolves_whole_first_and_head_last() {
        let links = vec![
            link("d2", Some("d1"), 30),
            link("w", None, 10),
            link("d1", Some("w"), 20),
            link("unrelated", None, 99),
        ];
        assert_eq!(ids(&resolve_chain(&links, "d2").unwrap()), ["w", "d1", "d2"]);
    }

    #[test]
    fn a_missing_link_is_an_error_and_never_a_shorter_chain() {
        // The planted fault for the reader: drop the middle delta and the chain must refuse, because a
        // chain resolved without it would install a shard missing that delta's buckets.
        let links = vec![link("d2", Some("d1"), 30), link("w", None, 10)];
        assert_eq!(
            resolve_chain(&links, "d2").unwrap_err(),
            ChainError::MissingBase {
                manifest_id: "d2".to_string(),
                base_manifest_id: "d1".to_string()
            }
        );
    }

    #[test]
    fn a_cycle_is_refused() {
        let links = vec![link("a", Some("b"), 20), link("b", Some("a"), 10)];
        assert!(matches!(
            resolve_chain(&links, "a").unwrap_err(),
            ChainError::Cycle { .. }
        ));
    }

    #[test]
    fn a_link_from_another_shard_is_refused() {
        let mut other = link("w", None, 10);
        other.shard_id = 2;
        let links = vec![link("d", Some("w"), 20), other];
        assert!(matches!(
            resolve_chain(&links, "d").unwrap_err(),
            ChainError::ShardMismatch { expected: 1, found: 2, .. }
        ));
    }

    #[test]
    fn a_watermark_that_runs_backwards_is_refused() {
        let links = vec![link("d", Some("w"), 5), link("w", None, 10)];
        assert!(matches!(
            resolve_chain(&links, "d").unwrap_err(),
            ChainError::WatermarkBackwards { .. }
        ));
    }

    #[test]
    fn an_unknown_head_is_refused() {
        assert!(matches!(
            resolve_chain(&[], "x").unwrap_err(),
            ChainError::UnknownHead { .. }
        ));
    }

    #[test]
    fn retention_keeps_the_whole_chain_of_the_newest_and_nothing_else() {
        let links = vec![
            link("old", None, 1),
            link("w", None, 10),
            link("d1", Some("w"), 20),
            link("d2", Some("d1"), 30),
        ];
        let kept = chain_to_retain(&links, "d2").unwrap();
        assert_eq!(
            kept.into_iter().collect::<Vec<_>>(),
            ["d1".to_string(), "d2".to_string(), "w".to_string()]
        );
    }

    #[test]
    fn retention_errors_when_the_chain_is_broken_so_the_caller_keeps_everything() {
        let links = vec![link("d2", Some("gone"), 30)];
        assert!(chain_to_retain(&links, "d2").is_err());
    }
}
