// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `page` and `block` NAME ONE TIER HERE, AND THIS IS THE GUARD THAT SAYS SO.
//!
//! # WHY THIS EXISTS
//!
//! A proposal arrived to rename this crate's whole `page` vocabulary to `block`, on the
//! footing that `block` is canonical here. Before moving anything, the question was asked the
//! other way round: are they even the same thing? Two surfaces say they are NOT.
//!
//!   * `docs/temporalstore_page_block_address_contract.md` defines `PageAddress` as "a logical
//!     page slice inside the TemporalStore serving storage layer" and `BlockAddress` as "the
//!     physical durable bytes that back one page or page slice" -- logical against physical,
//!     with different field sets.
//!   * [`crate::engine::reports::PublicStorageFeatureShapes`] declares those field sets
//!     separately: `page_address_fields` carries `segment_id`, `page_id` and `generation`,
//!     `block_address_fields` carries `block_id` and `checksum`.
//!
//! Both are DECLARATIONS. The types underneath them say one thing, and a type is enforced
//! where a declaration is not:
//!
//!   * NO `page_id` OR `page_ordinal` FIELD EXISTS IN THIS CRATE. Every identity on this tier
//!     is spelled `block_id`, and [`crate::block_store::ElementEntry`]'s `block_id` is
//!     documented as "WHICH PAGE of the object this is".
//!   * [`crate::engine::state::PageIndexNamedBy`] is a `Page`-named type whose only payload is
//!     a `BlockIndexMap`, and `BlockIndex` reaches the wire through `PageEntryAt`.
//!   * `PublicStorageContract` maps BOTH `page_address` and `block_address` to one type string,
//!     `ElementEntry`, and `storage_page_address_sample` and `storage_block_address_sample`
//!     both take `&ElementEntry` and are called on the same value.
//!   * There is no page SIZE tier: no `PAGE_SIZE` constant exists. A page is not a unit between
//!     two others here, the way it is in MatrixObject, where a page is a 1 MiB tier between a
//!     64 MiB chunk and a fragment.
//!
//! So the two words name one tier, `block` is the canonical spelling, and the `page` spellings
//! that remain on published reports are a MIRROR of the block ones, deliberately kept because
//! a published key is the wire. `PublicStorageContract::band` says the rule in prose: folding
//! two published names "would delete a published entry rather than rename a field -- a wire
//! change wearing a rename's clothes."
//!
//! # WHAT GOES WRONG WITHOUT THIS
//!
//! The failure is NOT a rename that breaks the build. It is the opposite: a mirror pair that
//! quietly stops being a mirror, so one published name starts carrying a second quantity and
//! every consumer reading the other name is silently wrong. That has happened twice here
//! already. A committed fixture recorded `page_reads: 4` beside `block_reads: 2` and was
//! believed for long enough to be copied into six places, and `dirty_object_count` was filled
//! with PAGES in one eviction arm and OBJECTS in the other, 3.5x apart, inside a sort key.
//!
//! `page_and_block_counter_ratio` pins the reads and writes pair against nine workloads.
//! NOTHING pinned the other mirrors. `page_index_entries` and `block_index_entries` are
//! assigned from one expression four lines apart in `engine/persistence.rs`, `page_store_reads`
//! and `block_store_reads` are incremented on consecutive lines of one `else if` arm in
//! `engine/storage_reports.rs`, and `page_index_count` is assigned FROM
//! `physical_index.block_index_count` in `engine.rs`. Every one of those is one line away from
//! becoming two quantities, and nothing fails when it does.
//!
//! # WHY IT IS WRITTEN THIS WAY ROUND
//!
//! The assertion is NOT "a `page_` key exists". It resolves both spellings to ONE identity and
//! asserts the members of each identity agree. That holds whichever spelling survives: with two
//! members it compares them, with one it has nothing to disagree with. So a later rename slice
//! does not have to rewrite this file -- only the measured pair COUNT moves, and it is asserted
//! exactly so that moving it has to be deliberate.
//!
//! Canonicalisation is LONGEST MATCH FIRST (`pages` before `page`) and runs on `_`-delimited
//! segments only, so a word that merely contains the letters is untouched. That rule is not
//! hypothetical: this repository's own `LICENSE` contains `stoppage`, and `tools/portal` is full
//! of `pagehide` and `homepage`. A sweep without a boundary rule corrupts the Apache-2.0 text.
#![allow(clippy::all)]
use super::*;

/// Replace a `page`/`pages` SEGMENT with its block spelling, leaving every other segment alone.
///
/// Longest match first, and segment-exact rather than substring: `pages` becomes `blocks`,
/// `page` becomes `block`, and `homepage`, `pagehide` and `stoppage` are returned unchanged.
fn canonical_tier_name(name: &str) -> String {
    name.split('_')
        .map(|segment| match segment {
            "pages" => "blocks",
            "page" => "block",
            "Pages" => "Blocks",
            "Page" => "Block",
            other => other,
        })
        .collect::<Vec<_>>()
        .join("_")
}

/// Every group of published keys that resolve to one canonical name, with at least two members.
fn mirror_groups(
    value: &serde_json::Value,
) -> std::collections::BTreeMap<String, Vec<(String, serde_json::Value)>> {
    let object = value
        .as_object()
        .expect("the published stats must serialize as a JSON object");
    let mut groups: std::collections::BTreeMap<String, Vec<(String, serde_json::Value)>> =
        std::collections::BTreeMap::new();
    for (key, held) in object {
        groups
            .entry(canonical_tier_name(key))
            .or_default()
            .push((key.clone(), held.clone()));
    }
    groups.retain(|_, members| members.len() >= 2);
    groups
}

fn mirror_engine(dir: &std::path::Path, cache_name: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join(cache_name),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn mirror_set(engine: &TemporalEngine, key: &str, value: Vec<u8>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value,
        },
    });
    assert!(
        response.status.ok,
        "write of {key} failed: {:?}",
        response.status
    );
}

fn mirror_get(engine: &TemporalEngine, key: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    assert!(
        response.status.ok,
        "read of {key} failed: {:?}",
        response.status
    );
}

/// The segment rule, against the three words in this repository that would be corrupted by a
/// substring sweep.
///
/// `stoppage` is in `LICENSE` (Apache-2.0, "work stoppage"), `pagehide` and `homepage` are in
/// `tools/portal` and in four `Cargo.toml` files. A regex without a boundary rule rewrites all
/// three, and the LICENSE one is a licence-text change.
#[test]
fn the_segment_rule_leaves_words_that_merely_contain_the_letters_alone() {
    assert_eq!(canonical_tier_name("page_reads"), "block_reads");
    assert_eq!(canonical_tier_name("page_index_entries"), "block_index_entries");
    assert_eq!(canonical_tier_name("staged_pages"), "staged_blocks");
    assert_eq!(canonical_tier_name("page"), "block");
    assert_eq!(canonical_tier_name("pages"), "blocks");

    // Already canonical: the function is idempotent, which is what makes it direction-agnostic.
    assert_eq!(canonical_tier_name("block_reads"), "block_reads");
    assert_eq!(canonical_tier_name("block_index_entries"), "block_index_entries");

    // Words that merely contain the letters.
    for untouched in ["stoppage", "homepage", "pagehide", "pagination", "repaged"] {
        assert_eq!(
            canonical_tier_name(untouched),
            untouched,
            "{untouched} is not the storage tier and must survive the sweep unchanged"
        );
    }
}

/// `page_address` and `block_address` are published as ONE type, and the contract says so.
///
/// This is the pair the contract document describes as logical against physical. In this engine
/// both entries carry the string `ElementEntry`, because one `ElementEntry` is both the address
/// of a page and the address of the block backing it. If a future change gives the physical
/// half an identity of its own, these two stop being equal and this fails -- which is the point
/// at which the contract document becomes true of this engine and the rename becomes wrong.
#[test]
fn the_published_contract_names_one_type_for_both_address_spellings() {
    let contract = crate::engine::reports::PublicStorageContract::default();
    let encoded = serde_json::to_value(&contract).expect("the contract must serialize");

    assert_eq!(
        contract.page_address, contract.block_address,
        "page_address is published as {:?} and block_address as {:?}; in this engine both are \
         one ElementEntry, and a difference here means the two words have stopped naming one \
         tier -- see the module header before renaming either",
        contract.page_address, contract.block_address,
    );
    assert!(
        !contract.page_address.is_empty(),
        "the address entries must name a type, not an empty string"
    );
    assert_eq!(
        contract.page_address, "ElementEntry",
        "the published address type moved; re-read the module header, because the whole \
         one-tier finding rests on both spellings resolving to this type"
    );

    // Denominator, printed. Provenance: `PublicStorageContract` declares 17 fields, of which
    // these are the page-spelled ones -- counted at 2701ed26e with
    // `awk '/pub struct PublicStorageContract/,/^}/' engine/reports.rs | grep -c 'pub page_'`.
    let page_keys: Vec<&String> = encoded
        .as_object()
        .expect("the contract must serialize as an object")
        .keys()
        .filter(|key| key.split('_').any(|segment| segment == "page"))
        .collect();
    println!("contract page-spelled keys: {page_keys:?}");
    assert_eq!(
        page_keys.len(),
        2,
        "expected exactly two page-spelled keys on the published contract \
         (page_address, page_index_entry), found {page_keys:?}"
    );
}

/// Every mirror pair on the canonical shard stats carries one value.
///
/// Driven over a real engine rather than a default, because a `Default` is all zeroes and an
/// equality between two zeroes is not a measurement. Each pair is asserted NON-ZERO first, so
/// this cannot pass by comparing nothing.
#[test]
fn every_page_named_shard_stat_agrees_with_its_block_twin() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    // Write ten keys, then read them back from a restart with a COLD cache directory, so the
    // read counters cannot be served from memory and every mirror below is non-zero.
    {
        let engine = mirror_engine(&root, "cache-write");
        for index in 0..10 {
            mirror_set(&engine, &format!("mirror-{index}"), vec![b'm'; 512]);
        }
    }
    let engine = mirror_engine(&root, "cache-read");
    for index in 0..10 {
        mirror_get(&engine, &format!("mirror-{index}"));
    }
    // Then ten writes ON THE SAME ENGINE, because the stats below are one snapshot and the
    // write mirror is zero on an engine that has only read. The first run of this test asserted
    // the write pair against 0 == 0 and its own non-vacuity clause caught it.
    for index in 10..20 {
        mirror_set(&engine, &format!("mirror-{index}"), vec![b'm'; 512]);
    }

    let stats = engine.loaded_shard_stats();
    assert_eq!(stats.len(), 1, "expected exactly one loaded shard");
    let encoded =
        serde_json::to_value(&stats[0].storage).expect("the canonical storage stats must serialize");

    let groups = mirror_groups(&encoded);
    for (canonical, members) in &groups {
        let rendered: Vec<String> = members
            .iter()
            .map(|(key, held)| format!("{key}={held}"))
            .collect();
        println!("mirror {canonical}: {}", rendered.join(" "));
    }

    // Provenance: sixteen page_*/block_* field twins exist across the crate, enumerated at
    // 2701ed26e by intersecting `^\s*(pub )?page_[a-z0-9_]+:` with the block_* spelling of each.
    // THREE of them live on `ShardCanonicalStorageStats`, which is what this test serializes:
    // page_reads, page_writes and page_index_entries. The other thirteen are on structs this
    // test does not reach; `page_and_block_counter_ratio` covers the reads and writes pair
    // against nine workloads, and the contract pair has its own test above.
    assert_eq!(
        groups.len(),
        3,
        "expected three mirror pairs on the canonical shard stats, found {:?}",
        groups.keys().collect::<Vec<_>>()
    );

    for (canonical, members) in &groups {
        let first = &members[0];
        for other in &members[1..] {
            assert_eq!(
                first.1, other.1,
                "{} and {} both resolve to {canonical} and are published from one source, so \
                 they cannot differ: {} against {}",
                first.0, other.0, first.1, other.1
            );
        }
        let nonzero = members
            .iter()
            .any(|(_, held)| held.as_u64().map(|value| value > 0).unwrap_or(false));
        assert!(
            nonzero,
            "{canonical} is zero on every spelling ({members:?}); the workload above did not \
             reach it, so this pair was compared vacuously"
        );
    }
}
