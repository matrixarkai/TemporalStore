// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `page` and `block` NAME ONE TIER HERE, AND THIS IS THE GUARD THAT SAYS SO.
//!
//! # THE CURRENT CONTRACT
//!
//! `docs/temporalstore_page_block_address_contract.md` describes one block tier.
//! `PublicStorageContract` maps both published address labels to `ElementEntry`.
//! `BlockIndexNamedBy` carries a `BlockIndexMap`, and `BlockEntryAt` writes its entries.
//! Older page-named report fields are retained for consumers, not a second storage tier.
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
/// line is named. `page_index_entries` and `block_index_entries` are assigned from one
/// expression five lines apart in `engine/persistence.rs`; `page_reads`/`block_reads` and
/// `page_writes`/`block_writes` are declared beside each other on this struct in
/// `control.rs` and filled from one snapshot.
///
/// TO RETIRE A SPELLING, move the canonical name to [`SINGLE_ON_SHARD_STATS`] together
/// with the spelling that survived. That one-line move is the reviewable artefact, and
/// the clauses in the test below refuse every other way of shrinking this population --
/// including the one that used to work, which was to leave the roster alone and change a
/// number.
const TWINNED_ON_SHARD_STATS: &[(&str, [&str; 2])] = &[
    (
        "block_index_entries",
        ["block_index_entries", "page_index_entries"],
    ),
    ("block_reads", ["block_reads", "page_reads"]),
    ("block_writes", ["block_writes", "page_writes"]),
];

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
const SINGLE_ON_SHARD_STATS: &[(&str, &str)] = &[];

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
         published report and not a rename; a key that ARRIVED is a published page spelling \
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
/// So the three facts are scanned for. If a page-named identity type, a page-named
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

    /// `struct Page...`, `enum Page...`, `type Page...`, `trait Page...` -- a type whose
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

    /// The line with leading whitespace and one leading `pub`/`pub(..)` removed.
    fn strip_leading_visibility(line: &str) -> &str {
        let code = line.trim_start();
        match code.strip_prefix("pub") {
            Some(rest) => {
                let rest = rest.trim_start();
                match rest.strip_prefix('(') {
                    Some(inner) => match inner.find(')') {
                        Some(close) => inner[close + 1..].trim_start(),
                        None => rest,
                    },
                    None => rest,
                }
            }
            None => code,
        }
    }

    // --- NON-VACUITY: each matcher still matches, in both directions. ---
    assert!(
        declares_a_page_named_type("pub struct PageAddress {"),
        "the type matcher no longer reports a page-named type declaration"
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
        "the field matcher no longer reports a page-named identity field"
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
        "the constant matcher no longer reports a page size constant"
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
                Some("a page-named type")
            } else if declares_a_page_named_identity_field(line) {
                Some("a page-named identity field")
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
/// store lives in (`<data>/pages`, set by `docker/single-node-entrypoint.sh`,
/// `docker/docker-compose.cloud-api.yml` and `tools/deploy_profile_common.sh`) -- so the
/// substring survives the serde rename being deleted outright. A registry entry for `pages`
/// would be a guard that cannot fail, which is worse than no entry: it reads as covered.
///
/// So this key is pinned where it is EMITTED instead, on both halves of the round trip.
const PAGE_SPELLED_RECORD_KEYS: &[&str] = &["pages"];

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
fn the_stored_object_layout_emits_the_page_spelling_and_reads_one_back() {
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
        emitted, PAGE_SPELLED_RECORD_KEYS,
        "the stored object layout emits {emitted:?} and this file declares \
         {PAGE_SPELLED_RECORD_KEYS:?}. A key that LEFT is a change to what is already on disk, \
         not a rename -- every multi-block object record written by every older build carries \
         the old spelling, and a reader that stops accepting it stops finding those blocks"
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
        declared.contains(r#"rename = "pages""#),
        "`object_record.rs` no longer carries an explicit rename to the page spelling. Either \
         the wire name moved -- which the ENCODE clause also catches -- or the FIELD was \
         renamed to the wire name, which leaves the emitted set unchanged and silently \
         retires the canonical identifier instead"
    );
    assert!(
        declared.contains("    blocks: Vec<StoredBlock>,"),
        "the stored layout's field is no longer the canonical `blocks`. This crate spells the \
         tier `block` and only the WIRE carries `pages`, so a page-spelled field here means \
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
    assert!(
        serde_json::from_str::<BlockLayout>(&as_block_spelled).is_err(),
        "the block spelling was also accepted, so `pages` is an ALIAS and not the wire name. \
         That is a different contract from the one this file documents: it would mean new \
         writes may already be going out under the canonical spelling, and the retirement \
         question becomes which spelling is on disk rather than whether to move it"
    );
}
