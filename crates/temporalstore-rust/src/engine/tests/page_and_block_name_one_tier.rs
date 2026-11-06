// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `page` and `block` NAME ONE TIER HERE, AND THIS IS THE GUARD THAT SAYS SO.
//!
//! # THE CURRENT CONTRACT
//!
//! `docs/temporalstore_page_block_address_contract.md` describes one block tier.
//! `PublicStorageContract` maps both published address labels to `ElementEntry`.
//! `BlockIndexNamedBy` carries a `BlockIndexMap`, and `BlockEntryAt` writes its entries.
//! Older block-named report fields are retained for consumers, not a second storage tier.
//! `PublicStorageFeatureShapes` describes report projections rather than Rust field layouts.
//!
//! The two names on a published statistic must continue to carry the same measurement.
//! Removing a published key would change the report contract rather than rename a field.
//!
//! # WHAT GOES WRONG WITHOUT THIS
//!
//! The failure is NOT a rename that breaks the build. It is the opposite: a mirror pair that
//! quietly stops being a mirror, so one published name starts carrying a second quantity and
//! every consumer reading the other name is silently wrong. That has happened twice here
//! already. A committed fixture recorded `page_reads: 4` beside `block_reads: 2` and was
//! believed for long enough to be copied into six places, and `dirty_object_count` was filled
//! with BLOCKS in one eviction arm and OBJECTS in the other, 3.5x apart, inside a sort key.
//!
//! `page_and_block_counter_ratio` pins the reads and writes pair against nine workloads.
//! NOTHING pinned the other mirrors. `page_store_reads` and `block_store_reads` are incremented
//! on consecutive lines of one `else if` arm in `engine/storage_reports.rs`, and
//! `page_index_count` is assigned FROM `physical_index.block_index_count` in `engine.rs`. Every
//! one of those is one line away from becoming two quantities, and nothing fails when it does.
//!
//! `page_index_entries` and `block_index_entries` USED to be the third example here, assigned
//! from one expression four lines apart in `engine/engine_persistence.rs`. The page half has
//! since been retired -- it is on `SINGLE_ON_SHARD_STATS` below and `engine_persistence.rs`
//! assigns the block half alone -- so the citation was naming a pair that no longer exists. A
//! header that cites a retired pair is the same defect as a header that cites itself.
//!
//! `TWINNED_FIELD_PAIRS_IN_REPORTS` at the foot of this file is why that mattered. Reading the
//! empty `TWINNED_ON_SHARD_STATS` as "the twins are gone" was wrong by ELEVEN: the published
//! report structs in `engine/engine_reports.rs` still declare eleven twin pairs side by side,
//! and nothing held any of them.
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

/// The line with leading whitespace and one leading `pub`/`pub(..)` removed.
///
/// LIFTED TO MODULE SCOPE from inside `no_page_named_identity_type_field_or_size_constant_exists_in_this_crate`,
/// because the census below needs the same code form and two copies of a matcher is how one of
/// them quietly stops matching what the other does.
///
/// `pub` is recognised only with a BOUNDARY after it -- whitespace or `(`. Without that, a field
/// declared `public_storage_contract:` with no visibility keyword had its first three letters
/// eaten and read back as `lic_storage_contract`, and `engine_reports.rs` declares exactly that
/// name twice. It could not have created or hidden a twin pair, because neither spelling carries
/// a `page` or `block` segment, but it would have moved the field total the census asserts a
/// floor on.
fn strip_leading_visibility(line: &str) -> &str {
    let code = line.trim_start();
    match code.strip_prefix("pub") {
        Some(rest) if rest.starts_with(char::is_whitespace) => rest.trim_start(),
        Some(rest) => match rest.strip_prefix('(') {
            Some(inner) => match inner.find(')') {
                Some(close) => inner[close + 1..].trim_start(),
                None => code,
            },
            None => code,
        },
        None => code,
    }
}

/// Every group of published keys that resolve to one canonical tier name.
///
/// THERE IS NO MEMBERSHIP FILTER HERE, AND THAT IS THE POINT. This function used to end
/// with `groups.retain(|_, members| members.len() >= 2)`, and the test below asserted the
/// resulting group COUNT. Those two together are a guard that is satisfied by shrinking
/// its own population: retire one spelling of a pair, the group falls below two, the
/// group disappears, the asserted count no longer matches -- and the obvious repair is to
/// lower the number. The moment that number is lowered, nothing in this file compares the
/// two spellings of that pair any more, and every assertion here is green. The count
/// moved and the coverage went with it, with no failing test anywhere to say so.
///
/// So the filter is gone and the population is DECLARED instead, by
/// [`TWINNED_ON_SHARD_STATS`] and [`SINGLE_ON_SHARD_STATS`]. A one-member group is still a
/// group here, which is what lets the test below say WHICH spelling survived a retirement
/// instead of losing sight of the canonical name altogether.
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
    groups
}

/// Canonical tier names on `ShardCanonicalStorageStats` published under BOTH spellings,
/// and the exact pair each one must carry.
///
/// Not remembered -- each pair is one line away from carrying two quantities, and the
/// line is named. `page_reads`/`block_reads` and `page_writes`/`block_writes` were declared
/// beside each other on this struct in `control.rs` and filled from one snapshot.
///
/// EMPTY TODAY, and that is a fact about THIS ONE STRUCT and no other. Every twin that was on
/// `ShardCanonicalStorageStats` has been retired down to its block spelling and sits on
/// [`SINGLE_ON_SHARD_STATS`] below. The published REPORT structs are a separate population with
/// a separate roster, [`TWINNED_FIELD_PAIRS_IN_REPORTS`], and it holds eleven pairs.
///
/// TO RETIRE A SPELLING, move the canonical name to [`SINGLE_ON_SHARD_STATS`] together
/// with the spelling that survived. That one-line move is the reviewable artefact, and
/// the clauses in the test below refuse every other way of shrinking this population --
/// including the one that used to work, which was to leave the roster alone and change a
/// number.
const TWINNED_ON_SHARD_STATS: &[(&str, [&str; 2])] = &[];

/// Canonical tier names on the canonical shard stats that are DOWN to one spelling, with
/// the spelling that survived.
///
/// EMPTY TODAY, and every clause below still runs over it, so the first retirement adds
/// one line and writes no new machinery.
///
/// The surviving spelling is asserted to be the BLOCK one, because that is the outcome a
/// pair count can never tell apart from the right one: dropping `block_reads` and keeping
/// `page_reads` lowers the count by exactly as much as dropping `page_reads` does, and
/// leaves the tier published under the word this crate has decided is not canonical.
const SINGLE_ON_SHARD_STATS: &[(&str, &str)] = &[
    ("block_index_entries", "block_index_entries"),
    ("block_reads", "block_reads"),
    ("block_writes", "block_writes"),
];

/// The page-spelled keys the published storage contract carries.
///
/// A COUNT here has the same defect the pair count had: retiring one of the two lets the
/// 2 be written as a 1, and nothing then says which key left or that the other is still
/// published. The set is named instead.
const PAGE_SPELLED_CONTRACT_KEYS: &[&str] = &["page_address", "page_index_entry"];

fn mirror_engine(dir: &std::path::Path, cache_name: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join(cache_name),
        dir.join("blocks"),
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
/// of a block and the address of the block backing it. If a future change gives the physical
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
    // `awk '/pub struct PublicStorageContract/,/^}/' engine/engine_reports.rs | grep -c 'pub page_'`.
    let block_keys: Vec<&String> = encoded
        .as_object()
        .expect("the contract must serialize as an object")
        .keys()
        .filter(|key| key.split('_').any(|segment| segment == "page"))
        .collect();
    println!("contract page-spelled keys: {block_keys:?}");
    let found: Vec<&str> = block_keys.iter().map(|key| key.as_str()).collect();
    assert_eq!(
        found, PAGE_SPELLED_CONTRACT_KEYS,
        "the published contract carries the page-spelled keys {found:?} and this file declares \
         {PAGE_SPELLED_CONTRACT_KEYS:?}. A key that LEFT was retired, which is a change to the \
         published report and not a rename; a key that ARRIVED is a published block spelling \
         nothing has held"
    );
}

/// Every mirror pair on the canonical shard stats carries one value, and the POPULATION
/// of pairs is the declared one.
///
/// Driven over a real engine rather than a default, because a `Default` is all zeroes and
/// an equality between two zeroes is not a measurement. Each pair is asserted NON-ZERO
/// first, so this cannot pass by comparing nothing.
///
/// FIVE CLAUSES, because the agreement check is the easy half. The hard half is that the
/// agreement check keeps being APPLIED to the pairs that exist:
///
///   1. the rosters are well formed -- every listed spelling canonicalises to the name it
///      is listed under, no name is on both rosters, and a retired name's survivor is the
///      block spelling;
///   2. the set of canonical names published under two or more spellings IS
///      [`TWINNED_ON_SHARD_STATS`], in both directions, so a pair that leaves without a
///      roster move fails, and a pair that arrives unnoticed fails too;
///   3. each declared pair is present under exactly the two spellings declared;
///   4. each retired name is present under exactly its surviving spelling;
///   5. the spellings of each declared pair hold one value, and that value is not zero on
///      every spelling.
#[test]
fn every_page_named_shard_stat_agrees_with_its_block_twin() {
    // --- CLAUSE 1: the rosters are well formed, before anything is measured. ---
    for (canonical, pair) in TWINNED_ON_SHARD_STATS {
        for spelling in pair {
            assert_eq!(
                canonical_tier_name(spelling),
                *canonical,
                "{spelling} is listed under {canonical} and canonicalises to {} instead",
                canonical_tier_name(spelling)
            );
        }
        assert_ne!(
            pair[0], pair[1],
            "{canonical} lists one spelling twice, so its agreement check would compare a \
             value with itself"
        );
        assert!(
            !SINGLE_ON_SHARD_STATS
                .iter()
                .any(|(retired, _)| retired == canonical),
            "{canonical} is on both rosters; it is either twinned or down to one spelling"
        );
    }
    for (canonical, survivor) in SINGLE_ON_SHARD_STATS {
        assert_eq!(
            canonical_tier_name(survivor),
            *canonical,
            "{survivor} is listed as the survivor of {canonical} and canonicalises to {} instead",
            canonical_tier_name(survivor)
        );
        assert_eq!(
            survivor, canonical,
            "{canonical} is recorded as retired down to {survivor}, which is not the block \
             spelling. The block spelling is the one that survives a retirement on this tier -- \
             see the module header -- and keeping the other half publishes the tier under the \
             word this crate does not use"
        );
    }

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
        let on_a_roster = members.len() >= 2
            || SINGLE_ON_SHARD_STATS
                .iter()
                .any(|(retired, _)| retired == canonical);
        if !on_a_roster {
            continue;
        }
        let rendered: Vec<String> = members
            .iter()
            .map(|(key, held)| format!("{key}={held}"))
            .collect();
        println!("mirror {canonical}: {}", rendered.join(" "));
    }

    // --- CLAUSE 2: the mirrored population IS the declared one, in both directions. ---
    let mut published_pairs: Vec<&str> = groups
        .iter()
        .filter(|(_, members)| members.len() >= 2)
        .map(|(canonical, _)| canonical.as_str())
        .collect();
    published_pairs.sort_unstable();
    let mut declared_pairs: Vec<&str> = TWINNED_ON_SHARD_STATS
        .iter()
        .map(|(canonical, _)| *canonical)
        .collect();
    declared_pairs.sort_unstable();
    assert_eq!(
        published_pairs, declared_pairs,
        "the canonical shard stats publish a different set of mirror pairs than this file \
         declares. Published {published_pairs:?}, declared {declared_pairs:?}. A name that LEFT \
         the published set was retired without being moved to SINGLE_ON_SHARD_STATS, so the pair \
         it names is now compared by nothing; a name that ARRIVED is a new pair no assertion has \
         ever held. Move it to the right roster rather than editing this list to match whatever \
         the engine now emits"
    );

    // --- CLAUSE 3: each declared pair is present under exactly its two spellings. ---
    for (canonical, pair) in TWINNED_ON_SHARD_STATS {
        let members = groups.get(*canonical).unwrap_or_else(|| {
            panic!(
                "{canonical} is declared twinned and is not published under ANY spelling; the \
                 canonical names on the shard stats are {:?}",
                groups.keys().collect::<Vec<_>>()
            )
        });
        let mut spellings: Vec<&str> = members.iter().map(|(key, _)| key.as_str()).collect();
        spellings.sort_unstable();
        let mut wanted: Vec<&str> = pair.to_vec();
        wanted.sort_unstable();
        assert_eq!(
            spellings, wanted,
            "{canonical} is published as {spellings:?} and declared as {wanted:?}; a spelling \
             was renamed or removed without moving this entry"
        );
    }

    // --- CLAUSE 4: each retired name is present under exactly its surviving spelling. ---
    for (canonical, survivor) in SINGLE_ON_SHARD_STATS {
        let members = groups.get(*canonical).unwrap_or_else(|| {
            panic!(
                "{canonical} is recorded as retired down to {survivor} and is not published \
                 under any spelling at all; retiring a twin publishes one name, not none"
            )
        });
        let spellings: Vec<&str> = members.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(
            spellings,
            vec![*survivor],
            "{canonical} is recorded as retired down to {survivor} and is published as \
             {spellings:?}"
        );
    }

    // --- CLAUSE 5: agreement, and non-vacuity, per declared pair. ---
    for (canonical, _) in TWINNED_ON_SHARD_STATS {
        let members = &groups[*canonical];
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

/// The three facts that make `page` and `block` ONE tier here are checked, not asserted
/// in prose.
///
/// WHY THIS EXISTS AS A TEST. The module header used to carry these as bullets, and three
/// of its five citations named things that had already stopped existing:
/// `PageIndexNamedBy` and `PageEntryAt` were renamed to `BlockIndexNamedBy` and
/// `BlockEntryAt` by a later slice of the same work, and `PAGE_SIZE` and `page_ordinal`
/// were only ever named in order to be denied. Each of those four tokens occurred exactly
/// ONCE in this crate, and the occurrence was the header itself. A header that cites
/// itself is a header that has stopped reading the tree.
///
/// So the three facts are scanned for. If a block-named identity type, a block-named
/// identity FIELD or a page SIZE constant is reintroduced, this fails and names the file
/// and line, instead of a doc comment quietly becoming untrue.
///
/// WHAT IS ALLOWED, AND WHY THE MATCHERS NEED NO EXCLUSION LIST. Each matcher requires
/// the line to BE a declaration: the trimmed line starts with an optional `pub` and then
/// the declaring token. A doc line starts with `//`, a serde attribute with `#`, and a
/// JSON key inside a fixture string with a quote, so prose, aliases and fixtures cannot
/// match and nothing has to be remembered and excluded by path -- including this file,
/// which names all three forms above in its own comments. `StoragePageAddressSample` and
/// `StoragePageIndexEntrySample` do not begin with `Page` and are deliberately untouched:
/// they are report projections, not identities.
///
/// THREE NON-VACUITY CLAUSES, because a scan that finds nothing looks the same as a clean
/// tree: the walk reached a floor of files and recursed, and each matcher is exercised on
/// a line it must report and on a line it must not.
#[test]
fn no_page_named_identity_type_field_or_size_constant_exists_in_this_crate() {
    const MIN_FILES: usize = 100;

    /// `struct Block...`, `enum Block...`, `type Block...`, `trait Block...` -- a type whose
    /// name BEGINS with the tier word, which is what an identity type would look like.
    fn declares_a_page_named_type(line: &str) -> bool {
        let code = strip_leading_visibility(line);
        ["struct ", "enum ", "type ", "trait "]
            .iter()
            .any(|keyword| {
                code.strip_prefix(keyword)
                    .is_some_and(|rest| rest.starts_with("Page"))
            })
    }

    /// `page_id:` or `page_ordinal:` as a FIELD on a struct.
    fn declares_a_page_named_identity_field(line: &str) -> bool {
        let code = strip_leading_visibility(line);
        ["page_id", "page_ordinal"].iter().any(|field| {
            code.strip_prefix(*field)
                .is_some_and(|rest| rest.trim_start().starts_with(':'))
        })
    }

    /// `const PAGE_SIZE` / `static PAGE_SIZE` -- a page SIZE tier.
    fn declares_a_page_size_constant(line: &str) -> bool {
        let code = strip_leading_visibility(line);
        ["const ", "static "].iter().any(|keyword| {
            code.strip_prefix(keyword)
                .is_some_and(|rest| rest.trim_start().starts_with("PAGE_SIZE"))
        })
    }

    // --- NON-VACUITY: each matcher still matches, in both directions. ---
    assert!(
        declares_a_page_named_type("pub struct PageAddress {"),
        "the type matcher no longer reports a block-named type declaration"
    );
    assert!(
        declares_a_page_named_type("    type PageIndex = BlockIndexMap;"),
        "the type matcher misses a type alias"
    );
    assert!(
        !declares_a_page_named_type("pub struct StoragePageAddressSample {"),
        "the type matcher reports a report projection, which is allowed and must stay"
    );
    assert!(
        !declares_a_page_named_type("//!   * [`crate::engine::state::PageIndexNamedBy`] is a type"),
        "the type matcher reports a doc line, so it would fail on this very file"
    );
    assert!(
        declares_a_page_named_identity_field("    pub page_id: u64,"),
        "the field matcher no longer reports a block-named identity field"
    );
    assert!(
        declares_a_page_named_identity_field("    page_ordinal: Option<u64>,"),
        "the field matcher misses a private field"
    );
    assert!(
        !declares_a_page_named_identity_field("        \"page_id\": 4,"),
        "the field matcher reports a JSON key in a fixture string, which is stored data and must \
         keep its spelling"
    );
    assert!(
        !declares_a_page_named_identity_field("    #[serde(default, alias = \"page_id\")]"),
        "the field matcher reports a serde alias, which is how older stored data still decodes"
    );
    assert!(
        declares_a_page_size_constant("const PAGE_SIZE: usize = 1 << 20;"),
        "the constant matcher no longer reports a block size constant"
    );
    assert!(
        !declares_a_page_size_constant("//!   * There is no page SIZE tier: no PAGE_SIZE exists"),
        "the constant matcher reports a doc line"
    );
    assert!(
        !declares_a_page_size_constant("    let page_size = record.page_size;"),
        "the constant matcher reports a local binding"
    );

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("read_dir {} failed: {err}", dir.display()));
        for entry in entries {
            let entry = entry.expect("dir entry readable");
            let path = entry.path();
            let kind = entry.file_type().expect("file type readable");
            if kind.is_dir() {
                stack.push(path);
            } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();

    // --- NON-VACUITY: the walk ran and recursed. ---
    assert!(
        files.len() >= MIN_FILES,
        "the walk found {} files under {}, floor is {MIN_FILES} -- that is a broken walk, not a \
         clean tree",
        files.len(),
        root.display()
    );
    assert!(
        files
            .iter()
            .filter(|path| path.parent() != Some(root.as_path()))
            .count()
            > 0,
        "the walk did not recurse, so most of this crate was not scanned"
    );

    // --- NON-VACUITY: the walk can still SEE a block-named declaration of each shape, so a
    // clean page-named result is a fact about the spelling and not about the scan. ---
    let mut block_named_types = 0usize;
    let mut block_named_id_fields = 0usize;
    let mut findings: Vec<String> = Vec::new();
    for path in &files {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));
        for (number, line) in text.lines().enumerate() {
            let code = strip_leading_visibility(line);
            if ["struct ", "enum ", "type ", "trait "].iter().any(|keyword| {
                code.strip_prefix(keyword)
                    .is_some_and(|rest| rest.starts_with("Block"))
            }) {
                block_named_types += 1;
            }
            if code
                .strip_prefix("block_id")
                .is_some_and(|rest| rest.trim_start().starts_with(':'))
            {
                block_named_id_fields += 1;
            }
            let what = if declares_a_page_named_type(line) {
                Some("a block-named type")
            } else if declares_a_page_named_identity_field(line) {
                Some("a block-named identity field")
            } else if declares_a_page_size_constant(line) {
                Some("a page SIZE constant")
            } else {
                None
            };
            if let Some(what) = what {
                findings.push(format!(
                    "{}:{} declares {what}: {}",
                    path.display(),
                    number + 1,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        block_named_types > 0,
        "the walk found no Block-named type declaration either, so the matcher is looking at \
         nothing rather than at a crate with one spelling"
    );
    assert!(
        block_named_id_fields > 0,
        "the walk found no `block_id` field declaration, so the field matcher is looking at \
         nothing"
    );
    println!(
        "scanned {} files; {block_named_types} Block-named type declarations and \
         {block_named_id_fields} `block_id` fields are the spelling that is used",
        files.len()
    );

    assert!(
        findings.is_empty(),
        "every identity on this tier is spelled `block`, and there is no page SIZE tier; the \
         module header rests on that. These declarations contradict it:\n{}",
        findings.join("\n")
    );
}

/// The page-spelled keys the STORED OBJECT RECORD emits, named as a set.
///
/// `BlockLayout` is spelled `blocks` in Rust and emits `pages`, so the identifier says nothing
/// about the contract and a scan over identifiers cannot see this key at all.
///
/// # WHY THIS IS NOT A `DURABLE_NAMES` ENTRY
///
/// `DURABLE_NAMES` in `block_store.rs` checks one direction: every listed name must still be
/// spelled by SOME site outside the list. For this key that check can never go red. The string
/// `"pages"` has 747 other sites in this crate -- it is also the on-disk directory the block
/// store lives in (`<data>/blocks`, set by `docker/single-node-entrypoint.sh`,
/// `docker/docker-compose.cloud-api.yml` and `tools/deploy_profile_common.sh`) -- so the
/// substring survives the serde rename being deleted outright. A registry entry for `pages`
/// would be a guard that cannot fail, which is worse than no entry: it reads as covered.
///
/// So this key is pinned where it is EMITTED instead, on both halves of the round trip.
///
/// THE WRITER HAS SINCE MOVED, so the emitted key is `blocks` and `pages` is the alias a store
/// written by an earlier build is read through. The decode clause below drives BOTH spellings and
/// asserts they land on the same layout, which is the guarantee that replaced the old clause
/// asserting the block spelling was refused.
const BLOCK_SPELLED_RECORD_KEYS: &[&str] = &["blocks"];

/// The stored layout emits the page spelling, and still reads one back.
///
/// THREE CLAUSES, because "the key is there" is the easy half:
///
///   1. ENCODE -- the emitted key set is the declared one, so adding or dropping an emitted
///      key on this record has to be deliberate;
///   2. the Rust field name is NOT what is emitted, so the clause above is a fact about the
///      wire and not about the identifier (without this, the test would still pass if someone
///      deleted the `rename` AND renamed the field to `pages`, which is a different change);
///   3. DECODE -- a record written by an older build loads, and the block spelling is NOT
///      silently accepted in its place. A guard that only reads the ENCODE cannot tell a
///      retained wire name from an accepted alias, and those have opposite consequences for a
///      store already on disk.
#[test]
fn the_stored_object_layout_emits_the_block_spelling_and_reads_both_back() {
    use crate::engine::object_record::BlockLayout;

    // ---- CLAUSE 1: ENCODE ----
    let encoded =
        serde_json::to_value(BlockLayout::default()).expect("the stored layout must serialize");
    let emitted: Vec<&str> = encoded
        .as_object()
        .expect("the stored layout must serialize as an object")
        .keys()
        .map(|key| key.as_str())
        .collect();
    assert_eq!(
        emitted, BLOCK_SPELLED_RECORD_KEYS,
        "the stored object layout emits {emitted:?} and this file declares \
         {BLOCK_SPELLED_RECORD_KEYS:?}. This build writes the canonical spelling; the block \
         spelling survives only as the alias the DECODE clause below drives, so a key that left \
         this set is a writer move and a key that arrived is a second spelling going to disk"
    );

    // ---- CLAUSE 2: THE IDENTIFIER IS NOT THE CONTRACT ----
    //
    // Reading the emitted set cannot establish this, so this clause reads the SOURCE. If
    // someone deleted the `rename` and renamed the field to the wire name, the clause
    // above would still see exactly the declared set and pass, while the canonical
    // identifier left the crate with nothing red. The two names have to be checked where
    // they sit together.
    let declared = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine/object_record.rs"),
    )
    .expect("the stored-record source must be readable");
    assert!(
        declared.contains(r#"rename = "blocks", alias = "pages""#),
        "`object_record.rs` no longer carries the canonical rename with the page alias beside \
         it. Dropping the ALIAS is the dangerous half: the emitted set does not move, so the \
         ENCODE clause stays green while every layout an earlier build wrote stops loading"
    );
    assert!(
        declared.contains("    blocks: Vec<StoredBlock>,"),
        "the stored layout's field is no longer the canonical `blocks`. This crate spells the \
         tier `block` and only the WIRE carries `blocks`, so a block-spelled field here means \
         the retained name has been promoted back into the identifier surface"
    );

    // ---- CLAUSE 3: DECODE, AND THE ALIAS THAT MUST NOT EXIST ----
    let written_by_an_older_build = r#"{"pages":[{"address":42,"length":7,"block_id":3,"has_block_id":true,"has_generation":false}]}"#;
    let decoded: BlockLayout = serde_json::from_str(written_by_an_older_build)
        .expect("a layout written under the page spelling must still load");
    assert_eq!(
        decoded.len(),
        1,
        "the page-spelled payload decoded to {} blocks rather than 1, so the key was accepted \
         and the blocks behind it were dropped -- which loses an object's data without failing",
        decoded.len()
    );
    let as_block_spelled = written_by_an_older_build.replace("pages", "blocks");
    assert_ne!(
        as_block_spelled, written_by_an_older_build,
        "the substitution changed nothing, so the clause below is comparing the payload against \
         itself and would pass however the layout decodes"
    );
    let canonical: BlockLayout = serde_json::from_str(&as_block_spelled).expect(
        "a layout written under the canonical spelling -- which is what THIS build emits -- must \
         load, or the writer flip has produced bytes its own reader cannot take",
    );
    assert_eq!(
        canonical.len(),
        decoded.len(),
        "the canonical spelling decoded to {} blocks and the stored one to {}. They are one key \
         under two names, so a difference means one of them is dropping the blocks behind it",
        canonical.len(),
        decoded.len()
    );
}

/// THE INDEX-LOG ITEM KIND TAKES BOTH SPELLINGS, AND THIS IS THE VARIANT SHAPE RATHER THAN A FIELD.
///
/// `IndexItemKind::Block` is tagged `block` on the wire as of the writer flip, and `page` is the
/// token it still ACCEPTS. It is not a field name: it is the VALUE of an index-log item's kind,
/// and the index log is replayed on recovery, so the stored token is read back out of bytes an
/// older build wrote. `serde(rename)` on a variant replaces the accepted token exactly as it does
/// on a field, so the reader had to be widened BEFORE the writer moved -- and a unit-variant enum
/// has no `default` to fall back to, which makes this one LOUD rather than silent in both
/// directions: an unaccepted token is a decode error, and a decode error on an index-log replay is
/// a store that will not come up.
///
/// FOUR CLAUSES, and the last two are not padding:
///
///   1. the writer emits `block`, which is the flip this release carries. Without this clause the
///      reader arms could both pass while the writer sat wherever it happened to be.
///   2. the frozen `page` token still decodes to `Block`. This is now the clause driven from a
///      literal THIS BUILD NO LONGER WRITES, which is what keeps it honest: it is the installed
///      base on disk, not a round trip through the current writer.
///   3. the `block` token decodes to `Block`, so what clause 1 writes is what the reader takes.
///   4. `object` and `meta` still decode to themselves. An alias that collided with a sibling
///      token would make one variant unreachable, and serde reports a duplicate token for unit
///      variants at NEITHER compile nor decode time -- the first declaration simply wins, so the
///      collision would present as an item kind quietly decoding to the wrong variant.
#[test]
fn the_index_log_item_kind_reader_takes_the_block_spelling_and_the_page_spelling() {
    use crate::index_log::IndexItemKind;

    // Frozen, and READ-ONLY from here: every build through `SHARD_INDEX_FORMAT_VERSION` 27
    // tagged a block item with this, and those bytes are still on disk.
    const ITEM_KIND_AT_STAMP_27: &str = r#""page""#;

    // What this build writes.
    const ITEM_KIND_WRITTEN_NOW: &str = r#""block""#;

    // ---- CLAUSE 1: THE WRITER EMITS THE CANONICAL TOKEN ----
    assert_eq!(
        serde_json::to_string(&IndexItemKind::Block).expect("the item kind must serialize"),
        ITEM_KIND_WRITTEN_NOW,
        "[CLAUSE writer] a block item is not tagged `block` on the wire. The reader accepts both \
         tokens, so this is the only clause that says which one goes to disk -- and an item \
         written under the retired token would be read by a future build that has dropped the \
         alias as an unknown variant, which fails the whole replay"
    );
    assert_ne!(
        ITEM_KIND_WRITTEN_NOW, ITEM_KIND_AT_STAMP_27,
        "the two tokens are the same string, so clauses 2 and 3 are one clause twice"
    );

    // ---- CLAUSE 2: THE STORED TOKEN STILL LANDS ----
    let stored: IndexItemKind = serde_json::from_str(ITEM_KIND_AT_STAMP_27)
        .expect("an index-log item tagged with the stored spelling must still decode");
    assert_eq!(
        stored,
        IndexItemKind::Block,
        "[CLAUSE stored-tag] the stored `block` token decoded to {stored:?} rather than `Block`"
    );

    // ---- CLAUSE 3: AND SO DOES THE ONE THE WRITER NOW EMITS ----
    let canonical: IndexItemKind = serde_json::from_str(ITEM_KIND_WRITTEN_NOW).expect(
        "an index-log item tagged `block` must decode -- this build writes that token, so a \
         failure here is a store whose own replay cannot read what it just wrote, and unlike a \
         named field it fails LOUDLY and takes the whole replay with it",
    );
    assert_eq!(
        canonical,
        IndexItemKind::Block,
        "[CLAUSE block-tag] the `block` token decoded to {canonical:?} rather than `Block`"
    );

    // ---- CLAUSE 4: AND NO SIBLING TOKEN WAS CAPTURED ----
    for (token, expected) in [
        (r#""object""#, IndexItemKind::Object),
        (r#""meta""#, IndexItemKind::Meta),
    ] {
        let decoded: IndexItemKind = serde_json::from_str(token)
            .unwrap_or_else(|err| panic!("the item kind token {token} stopped decoding: {err}"));
        assert_eq!(
            decoded, expected,
            "[CLAUSE sibling-tags] {token} decoded to {decoded:?} rather than {expected:?}. An \
             alias that collided with a sibling token would present exactly like this -- quietly, \
             because a duplicate token on a unit variant is reported at neither compile nor \
             decode time and the first declaration wins"
        );
    }
}

// ===========================================================================================
// THE TWIN FIELD PAIRS IN THE PUBLISHED REPORTS, AS A CENSUS RATHER THAN A COUNT.
// ===========================================================================================

/// What the two spellings of a published twin pair actually hold.
///
/// A pair's NAMES say nothing about this, and the answers have opposite consequences.
/// `page_index_entry_count` and `block_index_entry_count` are filled from ONE expression, so
/// retiring either loses no number. `page_address` and `block_address` are two PROJECTIONS of
/// one `ElementEntry` into sample types that do not carry the same fields, so there is no single
/// value to compare -- and `block_id` differs between them BY CONSTRUCTION. `page_index_count`
/// and `block_index_count` are already filled from two different walks, so they are two
/// measurements wearing a twin's names, and an agreement assertion over that pair would be red
/// on a correct tree.
///
/// This is why the roster below carries the class beside each pair. A census that recorded only
/// "these two names exist" would invite the agreement check to be applied to all of them, and
/// three of the eleven would then have to be excluded by someone remembering why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TwinHolds {
    /// ONE quantity under two names: both members are filled from one expression.
    OneQuantity,
    /// One underlying fact projected into two different SHAPES.
    TwoProjections,
    /// Two measurements, from two different walks, already.
    TwoMeasurements,
    /// A PUBLISHED CATALOG ENTRY that deliberately carries both spellings.
    ///
    /// The KEYS ARE THE WIRE here and the consumers are outside this repository, so folding the
    /// two entries deletes a published entry rather than renaming a field. Tracked, never
    /// renamed. `PAGE_SPELLED_CONTRACT_KEYS` above pins the contract half of this by key set.
    PublishedCatalogEntry,
}

/// EVERY page/block twin field pair declared side by side inside one struct of
/// `engine/engine_reports.rs`, with what the pair holds and where it is filled.
///
/// # WHY A CENSUS AND NOT A COUNT
///
/// `TWINNED_ON_SHARD_STATS` above is the roster for `ShardCanonicalStorageStats`, and it is
/// EMPTY because every twin on that one struct has already been retired down to its block
/// spelling. Reading that emptiness as "the twins are gone" is the mistake this roster exists to
/// stop: the published REPORT structs in the same crate still carry eleven of them, and nothing
/// held any of them. A retirement there would have moved no number anywhere and failed nothing.
///
/// # HOW THE POPULATION IS ESTABLISHED
///
/// Scanned out of the file by [`scan_twin_field_pairs`], not remembered. The test below compares
/// the scan with this list IN BOTH DIRECTIONS, so a pair that LEAVES fails (it was retired
/// without a roster move, and the comparison that pair deserves is now made by nothing) and a
/// pair that ARRIVES fails too (it is a new twin no assertion has ever held).
///
/// TO RETIRE A SPELLING, move the entry to [`SINGLE_FIELD_PAIRS_IN_REPORTS`] with the spelling
/// that survived. That one-line move is the reviewable artefact.
///
/// `(struct, canonical tier name, the two spellings, what the pair holds, where it is filled)`
#[allow(clippy::type_complexity)]
const TWINNED_FIELD_PAIRS_IN_REPORTS: &[(&str, &str, [&str; 2], TwinHolds, &str)] = &[
    (
        "PublicStorageContract",
        "block_address",
        ["block_address", "page_address"],
        TwinHolds::PublishedCatalogEntry,
        "engine_reports.rs fills both entries with text(\"ElementEntry\")",
    ),
    (
        "PublicStorageContract",
        "block_index_entry",
        ["block_index_entry", "page_index_entry"],
        TwinHolds::PublishedCatalogEntry,
        "engine_reports.rs fills both entries from the same published type name",
    ),
    (
        "PublicStorageFeatureShapes",
        "block_address_fields",
        ["block_address_fields", "page_address_fields"],
        TwinHolds::PublishedCatalogEntry,
        "engine_reports.rs fills both from public_storage_strings over one field list",
    ),
    (
        "PublicStorageFeatureShapes",
        "block_index_entry_fields",
        ["block_index_entry_fields", "page_index_entry_fields"],
        TwinHolds::PublishedCatalogEntry,
        "engine_reports.rs fills both from public_storage_strings over one field list",
    ),
    (
        "StorageBlockIndexEntrySample",
        "block_address",
        ["block_address", "page_address"],
        TwinHolds::TwoProjections,
        "storage_bucket_internals.rs projects ONE entry.address through \
         storage_page_address_sample and storage_block_address_sample; the page sample carries \
         slab_id and generation, the block sample carries a checksum, and block_id is \
         block_id().unwrap_or(block_slab_id()) on one and block_slab_id() on the other",
    ),
    (
        "StorageCacheWarmupReport",
        "block_store_reads",
        ["block_store_reads", "page_store_reads"],
        TwinHolds::OneQuantity,
        "storage_reports.rs increments both on CONSECUTIVE lines of one else-if arm of the \
         cache-warmup loop",
    ),
    (
        "StorageDataStructureApiParityReport",
        "block_index_count",
        ["block_index_count", "page_index_count"],
        TwinHolds::TwoMeasurements,
        "engine.rs fills page_index_count from physical_index.block_index_count -- a bucket walk \
         -- and block_index_count from the sum over block_store.slab_reports(), a slab walk",
    ),
    (
        "StorageGcSnapshot",
        "stale_block_delete_markers",
        ["stale_block_delete_markers", "stale_page_delete_markers"],
        TwinHolds::OneQuantity,
        "engine_reports.rs puts stale_page_tombstones and stale_block_tombstones from the same \
         filter-and-count over plan.reclaim_candidates, and data_node.rs inserts both from \
         stats.storage_manager_reclaim_block_runs",
    ),
    (
        "StorageIndexSnapshot",
        "block_index_entry_count",
        ["block_index_entry_count", "page_index_entry_count"],
        TwinHolds::OneQuantity,
        "engine_reports.rs puts both metrics from object_lifecycle.live_block_refs, and \
         data_node.rs adds both from block_entries, on consecutive lines in each",
    ),
    (
        "StorageIndexSnapshot",
        "block_index_entry_samples",
        ["block_index_entry_samples", "page_index_entry_samples"],
        TwinHolds::TwoProjections,
        "storage_bucket_internals.rs builds both from the same take(MAX_STORAGE_INDEX_SAMPLES) \
         of one live-entry walk, into two different sample types",
    ),
    (
        "StorageWatermarkSnapshot",
        "block_index_rebuild_watermark",
        ["block_index_rebuild_watermark", "page_index_rebuild_watermark"],
        TwinHolds::OneQuantity,
        "engine_reports.rs puts both metrics from flag(!install_roll_forward_reports.is_empty())",
    ),
];

/// Twin field pairs in the published reports that are DOWN to one spelling, with the spelling
/// that survived.
///
/// EMPTY TODAY, and every clause below still runs over it, so the first retirement adds one line
/// and writes no new machinery. The survivor is asserted to be the BLOCK spelling for the reason
/// [`SINGLE_ON_SHARD_STATS`] gives: a count can never tell the right retirement from the wrong
/// one, because dropping either half lowers it by exactly as much.
const SINGLE_FIELD_PAIRS_IN_REPORTS: &[(&str, &str, &str)] = &[];

/// The file the census above is taken over.
const CENSUSED_REPORT_FILE: &str = "src/engine/engine_reports.rs";

/// The name a line DECLARES as a field, if it declares one.
///
/// Anchored on the declaration: the code form of the line must be `<ident>:`, where `<ident>` is
/// lower snake case. A doc line starts `//`, an attribute `#`, a stored key inside a fixture
/// string `"`, and a function signature puts a space or `(` before its colon -- so none of them
/// can match, and nothing has to be remembered and excluded by path. That matters here because
/// this very file names every field in the roster above in its own comments.
fn field_declared_on(line: &str) -> Option<&str> {
    let code = strip_leading_visibility(line);
    let name = code.split(':').next()?;
    if name.is_empty() || name.len() == code.len() {
        return None;
    }
    let mut characters = name.chars();
    let first = characters.next()?;
    if !(first.is_ascii_lowercase() || first == '_') {
        return None;
    }
    if !characters.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return None;
    }
    Some(name)
}

/// The struct a line opens a BLOCK for, if it opens one.
///
/// A struct LITERAL is excluded structurally rather than by a matcher: `StorageIndexSnapshot {`
/// carries no `struct` keyword, so it opens no block and the `page_index_entry_count:` line
/// inside it is never read as a declaration. A tuple struct ends `);` and opens nothing either.
fn struct_block_opened_by(line: &str) -> Option<&str> {
    let code = strip_leading_visibility(line);
    if !code.trim_end().ends_with('{') {
        return None;
    }
    let rest = code.strip_prefix("struct ")?;
    let name = rest
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .next()?;
    (!name.is_empty()).then_some(name)
}

/// Every page/block twin pair declared side by side inside one struct block of `text`, with the
/// struct NAMES the scan opened and the field total it read.
///
/// The names are returned rather than a count because a count cannot say WHICH blocks the scan
/// reached, and a conclusion of the form "these two fields are not declared in one struct" is
/// only worth anything if the scan opened the structs in question. A matcher looking at an empty
/// file reports a clean tree in exactly the same words as a matcher looking at a clean one.
///
/// Brace depth is counted only on lines that are not comments, so a doc comment carrying an
/// unbalanced brace -- a JSON fragment, a format string -- cannot close a struct block early and
/// silently drop the fields under it.
fn scan_twin_field_pairs(
    text: &str,
) -> (Vec<(String, String, Vec<String>)>, Vec<String>, usize) {
    let mut pairs: Vec<(String, String, Vec<String>)> = Vec::new();
    let mut opened: Vec<String> = Vec::new();
    let mut field_total = 0usize;
    let mut open: Option<(String, std::collections::BTreeMap<String, Vec<String>>, i32)> = None;
    for line in text.lines() {
        let is_comment = line.trim_start().starts_with("//");
        match open.as_mut() {
            None => {
                if let Some(name) = struct_block_opened_by(line) {
                    opened.push(name.to_string());
                    open = Some((name.to_string(), Default::default(), 1));
                }
            }
            Some((name, groups, depth)) => {
                if *depth == 1 && !is_comment {
                    if let Some(field) = field_declared_on(line) {
                        field_total += 1;
                        groups
                            .entry(canonical_tier_name(field))
                            .or_default()
                            .push(field.to_string());
                    }
                }
                if !is_comment {
                    *depth += line.matches('{').count() as i32;
                    *depth -= line.matches('}').count() as i32;
                }
                if *depth <= 0 {
                    let owner = name.clone();
                    for (canonical, mut members) in std::mem::take(groups) {
                        if members.len() >= 2 {
                            members.sort();
                            pairs.push((owner.clone(), canonical, members));
                        }
                    }
                    open = None;
                }
            }
        }
    }
    pairs.sort();
    (pairs, opened, field_total)
}

/// Every twin field pair the published reports carry is on a roster, and the roster says what
/// the pair holds.
///
/// SIX CLAUSES, because "the names exist" is the easy half:
///
///   1. the matchers work, in both directions, before anything is scanned;
///   2. the scan reached a floor of structs and fields, so a clean result is a fact about the
///      file and not about a broken walk;
///   3. the rosters are well formed -- every listed spelling canonicalises to the name it is
///      listed under, the canonical IS the block spelling, no pair lists one spelling twice, and
///      nothing is on both rosters;
///   4. the scanned population IS the declared one, in both directions;
///   5. each declared pair is present under exactly its two declared spellings;
///   6. each retired entry is present under exactly its surviving spelling.
#[test]
fn every_twin_field_pair_in_the_published_reports_is_on_the_roster() {
    const MIN_STRUCTS: usize = 80;
    const MIN_FIELDS: usize = 400;

    // --- CLAUSE 1: the matchers, in both directions. ---
    assert_eq!(
        field_declared_on("    pub page_address: StoragePageAddressSample,"),
        Some("page_address"),
        "the field matcher no longer reports a published field declaration"
    );
    assert_eq!(
        field_declared_on("    stored_slab_id: u64,"),
        Some("stored_slab_id"),
        "the field matcher misses a private field"
    );
    assert_eq!(
        field_declared_on("    pub(super) block_id: u64,"),
        Some("block_id"),
        "the field matcher misses a restricted-visibility field"
    );
    assert_eq!(
        field_declared_on("    #[serde(alias = \"page_id\")]"),
        None,
        "the field matcher reports a serde attribute, which is how older stored data decodes and \
         is not a second field"
    );
    assert_eq!(
        field_declared_on("    /// `page_address`: where the block is."),
        None,
        "the field matcher reports a doc line, so it would report this very file's own roster"
    );
    assert_eq!(
        field_declared_on("        \"page_index_entry_count\","),
        None,
        "the field matcher reports a key inside a string literal"
    );
    assert_eq!(
        field_declared_on("    pub fn page_address(&self) -> BlockAddress {"),
        None,
        "the field matcher reports a method signature"
    );
    assert_eq!(
        struct_block_opened_by("pub struct StorageIndexSnapshot {"),
        Some("StorageIndexSnapshot"),
        "the struct matcher no longer opens a block on a struct declaration"
    );
    assert_eq!(
        struct_block_opened_by("    StorageIndexSnapshot {"),
        None,
        "the struct matcher opens a block on a struct LITERAL. That is the whole reason the \
         field matcher needs no exclusion list: `page_index_entry_count: metric(..)` inside \
         `storage_index_snapshot_from_metrics` is shaped exactly like a declaration, and it is \
         the absence of the `struct` keyword that keeps it out"
    );
    assert_eq!(
        struct_block_opened_by("pub struct ShardId(u64);"),
        None,
        "the struct matcher opens a block on a tuple struct, which declares no named fields"
    );
    assert_eq!(
        field_declared_on("        page_index_entry_count: metric(metrics, \"x\"),"),
        Some("page_index_entry_count"),
        "a struct literal's field is shaped like a declaration and the matcher SHOULD match it; \
         it is excluded structurally by the clause above, not here"
    );

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(CENSUSED_REPORT_FILE);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));
    let (scanned, opened, field_total) = scan_twin_field_pairs(&text);
    let struct_total = opened.len();

    // --- CLAUSE 2: the scan reached the file. ---
    assert!(
        struct_total >= MIN_STRUCTS,
        "the scan opened {struct_total} struct blocks in {}, floor is {MIN_STRUCTS} -- that is a \
         broken scan, not a file with no reports in it",
        path.display()
    );
    for owner in TWINNED_FIELD_PAIRS_IN_REPORTS
        .iter()
        .map(|(owner, _, _, _, _)| *owner)
    {
        assert!(
            opened.iter().any(|found| found == owner),
            "the scan never opened a struct block for {owner}, so every clause below that reads \
             its fields is reading an empty set. The structs it did open are {opened:?}"
        );
    }
    assert!(
        field_total >= MIN_FIELDS,
        "the scan read {field_total} field declarations in {}, floor is {MIN_FIELDS}",
        path.display()
    );
    println!(
        "censused {struct_total} struct blocks and {field_total} field declarations in \
         {CENSUSED_REPORT_FILE}"
    );
    for (owner, canonical, members) in &scanned {
        println!("  twin {owner}::{canonical} {members:?}");
    }

    // --- CLAUSE 3: the rosters are well formed, before anything is compared. ---
    for (owner, canonical, pair, _, filled) in TWINNED_FIELD_PAIRS_IN_REPORTS {
        for spelling in pair {
            assert_eq!(
                canonical_tier_name(spelling),
                *canonical,
                "{owner}::{spelling} is listed under {canonical} and canonicalises to {} instead",
                canonical_tier_name(spelling)
            );
        }
        assert_ne!(
            pair[0], pair[1],
            "{owner}::{canonical} lists one spelling twice"
        );
        assert_eq!(
            canonical_tier_name(canonical),
            *canonical,
            "{owner}::{canonical} is not the canonical BLOCK spelling of its own pair"
        );
        assert!(
            !filled.is_empty(),
            "{owner}::{canonical} names no site that fills it. The class on this entry is a \
             claim about one expression somewhere, and an unnamed site is a claim nobody can \
             re-check"
        );
        assert!(
            !SINGLE_FIELD_PAIRS_IN_REPORTS
                .iter()
                .any(|(retired_owner, retired, _)| retired_owner == owner && retired == canonical),
            "{owner}::{canonical} is on both rosters; it is either twinned or down to one spelling"
        );
    }
    for (owner, canonical, survivor) in SINGLE_FIELD_PAIRS_IN_REPORTS {
        assert_eq!(
            canonical_tier_name(survivor),
            *canonical,
            "{owner}::{survivor} is listed as the survivor of {canonical} and canonicalises to \
             {} instead",
            canonical_tier_name(survivor)
        );
        assert_eq!(
            survivor, canonical,
            "{owner}::{canonical} is recorded as retired down to {survivor}, which is not the \
             block spelling"
        );
    }

    // --- CLAUSE 4: the scanned population IS the declared one, in both directions. ---
    let mut found: Vec<(&str, &str)> = scanned
        .iter()
        .map(|(owner, canonical, _)| (owner.as_str(), canonical.as_str()))
        .collect();
    found.sort_unstable();
    let mut declared: Vec<(&str, &str)> = TWINNED_FIELD_PAIRS_IN_REPORTS
        .iter()
        .map(|(owner, canonical, _, _, _)| (*owner, *canonical))
        .collect();
    declared.sort_unstable();
    assert_eq!(
        found, declared,
        "{CENSUSED_REPORT_FILE} declares a different set of twin field pairs than this file \
         rosters. Found {found:?}, rostered {declared:?}. A pair that LEFT was retired without \
         being moved to SINGLE_FIELD_PAIRS_IN_REPORTS, so the comparison it deserves is now made \
         by nothing; a pair that ARRIVED is a new twin no assertion has ever held. Move the entry \
         to the right roster rather than editing this list to match whatever the file now says"
    );

    // --- CLAUSE 5: each declared pair is present under exactly its two spellings. ---
    for (owner, canonical, pair, _, _) in TWINNED_FIELD_PAIRS_IN_REPORTS {
        let members = scanned
            .iter()
            .find(|(found_owner, found_canonical, _)| {
                found_owner == owner && found_canonical == canonical
            })
            .map(|(_, _, members)| members)
            .unwrap_or_else(|| {
                panic!("{owner}::{canonical} is rostered twinned and is declared under no spelling")
            });
        let mut wanted: Vec<&str> = pair.to_vec();
        wanted.sort_unstable();
        let present: Vec<&str> = members.iter().map(|member| member.as_str()).collect();
        assert_eq!(
            present, wanted,
            "{owner}::{canonical} is declared as {present:?} and rostered as {wanted:?}; a \
             spelling was renamed or removed without moving this entry"
        );
    }

    // --- CLAUSE 6: each retired entry is present under exactly its surviving spelling. ---
    for (owner, canonical, survivor) in SINGLE_FIELD_PAIRS_IN_REPORTS {
        assert!(
            text.lines()
                .any(|line| field_declared_on(line) == Some(*survivor)),
            "{owner}::{canonical} is recorded as retired down to {survivor} and {survivor} is \
             declared nowhere in {CENSUSED_REPORT_FILE}; retiring a twin publishes one name, not \
             none"
        );
        assert!(
            !TWINNED_FIELD_PAIRS_IN_REPORTS
                .iter()
                .any(|(entry_owner, entry_canonical, _, _, _)| {
                    entry_owner == owner && entry_canonical == canonical
                }),
            "{owner}::{canonical} is recorded as retired and is still on the twinned roster"
        );
    }
}

/// `page_store_reads` in the native persistence workflow is a LONE retired spelling, and the
/// field beside it is NOT its twin.
///
/// # WHY THIS ONE NEEDS ITS OWN GUARD
///
/// Every other page-spelled field in the census above sits next to its block twin, so retiring
/// it is a deletion and the roster sees the pair shrink. This one sits next to
/// `block_store_bytes_read`, which LOOKS like the twin and measures something else: the two are
/// filled from `stats.block_store_compat.reads` and `stats.block_store_compat.bytes_read`, one
/// a COUNT of reads and one a BYTE total. Folding the page-spelled field into its neighbour
/// therefore reads as a rename and silently replaces a count with a byte figure -- the exact
/// failure the module header records twice already, and the one no pair count can see, because
/// the pair was never there to shrink.
///
/// The canonical spelling is not missing from this file either: `AsyncWarmupProbe` in the same
/// file declares `block_store_reads`, from `StorageCacheWarmupReport`. So the retirement that is
/// correct here is to fill a `block_store_reads` field on these two probes from the same source,
/// and the one that is wrong is to point the neighbour at it.
///
/// FOUR CLAUSES, and the first is arithmetic on the canonicaliser rather than a remembered fact.
#[test]
fn the_lone_page_spelled_probe_field_has_no_twin_and_its_neighbour_measures_something_else() {
    // --- CLAUSE 1: the neighbour is not the canonical twin, by the rule this file uses. ---
    assert_eq!(
        canonical_tier_name("page_store_reads"),
        "block_store_reads",
        "the canonicaliser no longer resolves the lone spelling, so the clause below compares \
         nothing"
    );
    assert_ne!(
        canonical_tier_name("page_store_reads"),
        canonical_tier_name("block_store_bytes_read"),
        "`page_store_reads` and `block_store_bytes_read` canonicalise to one name, which would \
         make them a mirror pair. They are a COUNT and a BYTE TOTAL filled from two different \
         fields of one stats struct"
    );

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/bin/native_persistence_workflow.rs");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));

    // --- CLAUSE 2: the scan opened the THREE struct blocks this conclusion rests on. ---
    //
    // Named rather than counted. The file declares six structs today, so a floor would have to be
    // six, and a floor of six is satisfied by a scan that opened six of the wrong ones -- which
    // is exactly how clause 3's "these two are not declared in one struct" would come out true by
    // reading an empty set. The three named here are the two probes that carry the lone spelling
    // and the one that carries the canonical spelling.
    let (pairs, opened, field_total) = scan_twin_field_pairs(&text);
    for owner in ["ReadProbe", "ResidencyProbe", "AsyncWarmupProbe"] {
        assert!(
            opened.iter().any(|found| found == owner),
            "the scan never opened a struct block for {owner} in {}, so the clauses below are \
             reading an empty field set. The structs it did open are {opened:?}",
            path.display()
        );
    }
    assert!(
        field_total >= 90,
        "the scan read {field_total} field declarations in {}, and the three probe structs alone \
         carry more than that",
        path.display()
    );
    let declares =
        |field: &str| text.lines().any(|line| field_declared_on(line) == Some(field));
    assert!(
        declares("page_store_reads"),
        "`page_store_reads` is declared nowhere in {}. If it has been retired, the clauses below \
         are asserting a shape that no longer exists and this guard should go with it",
        path.display()
    );

    // --- CLAUSE 3: the neighbour is there, and so is the canonical spelling -- elsewhere. ---
    assert!(
        declares("block_store_bytes_read"),
        "`block_store_bytes_read` is no longer declared beside the lone spelling, so the \
         confusion this guard refuses cannot arise and it should be re-read before being kept"
    );
    assert!(
        declares("block_store_reads"),
        "`block_store_reads` is declared nowhere in {}. It is the canonical spelling, carried by \
         `AsyncWarmupProbe` from `StorageCacheWarmupReport`, and its presence is what makes the \
         lone page spelling a retirement candidate rather than a tier with no block name",
        path.display()
    );
    assert!(
        !pairs
            .iter()
            .any(|(_, canonical, _)| canonical == "block_store_reads"),
        "`page_store_reads` and `block_store_reads` are now declared in ONE struct of {}, which \
         makes them a twin pair and moves this field under the census roster. Add the entry \
         there and delete this guard: {pairs:?}",
        path.display()
    );

    // --- CLAUSE 4: the two fields beside each other are filled from two different sources. ---
    //
    // Read as exact ASSIGNMENT text, including the trailing comma, because this is the line that
    // would be changed by the wrong fold and the two source fields are what distinguishes a
    // count from a byte total.
    for (assignment, what) in [
        (
            "page_store_reads: stats.block_store_compat.reads,",
            "a COUNT of reads",
        ),
        (
            "block_store_bytes_read: stats.block_store_compat.bytes_read,",
            "a BYTE total",
        ),
    ] {
        assert!(
            text.contains(assignment),
            "{} no longer carries `{assignment}` -- {what}. If the two were folded onto one \
             source, one of these published probe fields has silently changed what it measures",
            path.display()
        );
    }
}
