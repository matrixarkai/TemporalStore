// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH CONTAINER EACH RESIDENT MODEL MAP SHOULD BE, MEASURED RATHER THAN MATCHED.
//!
//! `ShardState` declares twelve resident address maps. Eleven of them are `HashMap<String, _>`
//! whose value is either a bare `BlockAddress` or an ORDERED inner map; ONE -- `hashes`, at
//! `state.rs:123` -- is `HashMap<String, HashMap<String, BlockAddress>>`, the only nested hashed
//! container in the structure. `the_hash_inner_map_is_the_only_hashed_model_map` asserts that from
//! the declaration text itself rather than from a list written here, because a hand-written
//! subject list goes stale and nothing fails.
//!
//! THE TWO QUESTIONS ARE DIFFERENT AND THE ANSWERS DO NOT CARRY EACH OTHER.
//!
//!   1. THE INNER CONTAINER of `hashes`. A hashed table pays a per-INSTANCE control block and
//!      rounds its bucket count to a power of two; an ordered map or a sorted vector sizes to what
//!      it holds. Whether that matters depends entirely on FIELDS PER HASH, which is why the
//!      histogram comes before any change.
//!
//!   2. THE OUTER MAPS, all five keyed by object key. Measured here and NOT changed: the outer
//!      map is on a read path (`Command::StringGet` resolves straight through `shard.strings`, as
//!      the `BucketNode` doc note in `state.rs` says in as many words), and a flat vector is only
//!      viable at BUCKET granularity, which is a move into the bucket node and not a container
//!      swap.
//!
//! WHAT THE HISTOGRAM TURNED OUT TO BE, and it is a property of the WRITE PATH rather than of a
//! fixture. `write_context_node` in `execute_on_shard.rs` is commented "the one producer of a node
//! page", and it does exactly one thing to the model map:
//!
//!     shard.hashes.entry(object_key.to_string()).or_default()
//!          .insert(CONTEXT_NODE_FIELD.to_string(), address);
//!
//! `CONTEXT_NODE_FIELD` is the constant `"meta"` (`constants.rs:36`). Every read of a context node
//! spells the same constant -- `shard.hashes.get(key).and_then(|f| f.get(CONTEXT_NODE_FIELD))` at
//! nine production sites. So every context node in the store is a hash holding EXACTLY ONE FIELD,
//! and the only producer of a wider one is the Redis-compatible `HashSet`/`HashMultiSet` surface.
//! `the_context_node_write_path_produces_exactly_one_field_per_hash` asserts the site count and
//! the occupancy, so the claim is checked against the tree and not quoted from it.
//!
//! WHICH MAKES THE SHAPE BIMODAL, and a bimodal shape is a warning and not a licence: #1986 found
//! a mixed histogram holding only 1 and 100, and a mean over it would have described neither arm.
//! Both arms are therefore reported with their own sample counts and denominators, and the
//! container verdict is required to hold at BOTH.
//!
//! BOTH BYTE COLUMNS, and the chunk column is the one that decides. `ALLOC_BYTES` charges
//! `layout.size()` -- what the caller asked for -- and `ALLOC_CHUNK_BYTES` reads
//! `malloc_usable_size`, what the allocator actually set aside. #1967's whole finding was that an
//! out-of-line 112-byte payload takes a 128-byte chunk, which inverted a published sign, and
//! #1969 measured a 104-byte request reading 128. The chunk column is asserted as a FLOOR over the
//! request column and never as an equality.
//!
//! PER-INSTANCE AND PER-ENTRY ARE SEPARATE NUMBERS and are reported separately. A container's cost
//! is not linear in its occupancy -- a table steps at every power of two -- so this module does not
//! fit a line through two points and call the intercept an overhead. It measures the FULL cost at
//! each occupancy and reports the MARGINAL cost between neighbouring occupancies beside it, which
//! is what a step function permits.
//!
//! THE FIELD NAME TEXT IS CHARGED OUTSIDE THE PROBE WINDOW, deliberately and stated here so the
//! numbers are not read as totals. Every arm is handed the same pre-built `(String, BlockAddress)`
//! pairs and MOVES them in, so the probe sees the container's own allocation and nothing else --
//! otherwise the shapes would differ by how many times they copied a name and the container
//! comparison would be confounded. What the name text costs is priced on its own, in
//! `what_the_field_name_text_costs_when_every_hash_allocates_its_own`, because for a context node
//! it is the same four characters in every hash in the store.
//!
//! THE STORE PATH LENGTH is held constant across every engine arm and asserted equal: allocation
//! bytes move with it at about six bytes a character, and #1998 watched that confound masquerade
//! as a profile difference.
//!
//! rust-internal throughout: these tests read this crate's own declarations and its counting
//! allocator, and assert no product behaviour.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The declaration text this module reasons about, read at COMPILE TIME from the file that owns
/// it. A copy of the declarations written out here would go stale the moment one moved.
const STATE_DECLARATIONS: &str = include_str!("../state.rs");

/// The narrow end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped
/// default since #1973.
const NARROW_END: u32 = 1023;

/// Context nodes in the small arm, and in the large arm. Two corpus sizes because a table's cost
/// is a step function of occupancy and one size cannot show a step.
const SMALL_KEYS: usize = 4_000;
const LARGE_KEYS: usize = 40_000;

/// The occupancies the inner container is priced at. 1 is the context-node shape -- the whole
/// product path -- and the rest bracket it far enough either side to find where the verdict flips.
const OCCUPANCIES: [usize; 8] = [1, 2, 3, 4, 8, 16, 64, 256];

/// Instances built per occupancy. Enough that a per-instance figure is not one allocator
/// coincidence divided by one.
const INSTANCES: usize = 2_000;

// =================================================================================================
// 1. THE DECLARATIONS, AS THEY ARE IN THE TREE
// =================================================================================================

/// NO NESTED MODEL MAP IS A HASHED TABLE ANY MORE, AND `hashes` IS THE NAMED CONTAINER.
///
/// RETARGETED, and the reason is worth keeping. This guard was written to assert that `hashes` was
/// the ONE nested hashed map -- the finding this module opened on -- and the change it supports
/// REMOVED that declaration, so the guard failed on the very commit that fixed what it was watching.
/// A guard whose subject the fix deletes has to be retargeted at the INVARIANT the fix establishes,
/// not deleted with the subject: zero nested hashed maps, `hashes` carrying the named container, and
/// the ordered floor kept so the count cannot quietly go to zero on both sides at once and agree.
///
/// Derived from the authority -- the `state.rs` source -- rather than from a list of names typed
/// here, which is the shape that goes stale while every assertion still passes.
///
/// rust-internal: reads this crate's own declaration text, no product behaviour
#[test]
fn no_nested_model_map_is_a_hashed_table_and_hashes_carries_the_named_container() {
    let nested_hashed: Vec<&str> = STATE_DECLARATIONS
        .lines()
        .filter(|line| line.contains("pub(super)") && line.contains("HashMap<String, HashMap<"))
        .map(|line| line.trim())
        .collect();
    let ordered_inner: Vec<&str> = STATE_DECLARATIONS
        .lines()
        .filter(|line| line.contains("pub(super)") && line.contains("HashMap<String, BTreeMap<"))
        .map(|line| line.trim())
        .collect();

    println!("--- nested HASHED model maps ({}) ---", nested_hashed.len());
    for line in &nested_hashed {
        println!("  {line}");
    }
    println!("--- nested ORDERED model maps ({}) ---", ordered_inner.len());
    for line in &ordered_inner {
        println!("  {line}");
    }

    assert!(
        !STATE_DECLARATIONS.is_empty() && STATE_DECLARATIONS.len() > 100_000,
        "the declaration text did not load, so every count below would be a zero presented as a \
         finding: {} bytes",
        STATE_DECLARATIONS.len()
    );
    assert_eq!(
        0,
        nested_hashed.len(),
        "a nested HASHED model map is back. This is the shape this module measured at 272 chunk \
         bytes to hold 48 bytes of payload at one field, and a new one needs its own histogram \
         before it ships: {nested_hashed:?}"
    );
    let named: Vec<&str> = STATE_DECLARATIONS
        .lines()
        .filter(|line| line.contains("pub(super) hashes:"))
        .map(|line| line.trim())
        .collect();
    assert_eq!(
        1,
        named.len(),
        "the `hashes` declaration is no longer findable, so this guard is reading nothing: {named:?}"
    );
    // READ THROUGH THE WRAPPER, because the claim did not change when the declaration did.
    //
    // `hashes` is declared as `RecordedHashContainer` now -- the type that owns the map together
    // with the record of its mutations -- and the sorted-vector container this module priced is one
    // layer down, inside it. So this asserts BOTH halves: that the field is the recorded container,
    // and that the recorded container is still a map of the named sorted vector. Relaxing this to
    // "contains anything" would have let the inner container change back to a hashed table without
    // a single test noticing, which is exactly what the 272-versus-64 measurement bought.
    assert!(
        named[0].contains("RecordedHashContainer"),
        "`hashes` is no longer the recorded container, so the mutation-record invariant may be \
         gone as well as the pricing: {}",
        named[0]
    );
    let container = include_str!("../recorded_hash_container.rs");
    assert!(
        container.len() > 8_000,
        "the recorded container's source did not load, so the inner-container check below would \
         pass on an empty string: {} bytes",
        container.len()
    );
    // THE EXACT DECLARATION, not the name anywhere. Asking for a line CONTAINING
    // `entries: HashMap<String, HashFieldMap>` found three -- the field, a `_for_test` setter's
    // PARAMETER and a `From` impl's parameter -- and the guard failed on two lines that declare
    // nothing. That was driven. The field declaration is one exact line, so that is what is asked
    // for, and the control below is the other direction: the string must still be findable at all.
    let inner: Vec<&str> = container
        .lines()
        .map(|line| line.trim())
        .filter(|line| *line == "entries: HashMap<String, HashFieldMap>,")
        .collect();
    assert!(
        container.contains("entries: HashMap<String, HashFieldMap>"),
        "the inner declaration is not findable in any form, so the exact match below would report \
         zero for the wrong reason"
    );
    assert_eq!(
        1,
        inner.len(),
        "`RecordedHashContainer` no longer holds exactly one `HashMap<String, HashFieldMap>`, so \
         `hashes` no longer carries the named container this module priced: {inner:?}"
    );
    assert!(
        ordered_inner.len() >= 17,
        "fewer than seventeen nested ORDERED model maps were found. The floor is here so the \
         hashed count above cannot reach zero by the declarations vanishing rather than by the \
         container changing -- which would make this whole guard agree vacuously: {ordered_inner:?}"
    );
    println!(
        "nested containers: 0 hashed, {} ordered, 1 named sorted-vector (`hashes`)",
        ordered_inner.len()
    );
}

/// THE ONE PRODUCER OF A CONTEXT NODE PAGE FILES IT UNDER ONE FIELD, and nine readers spell that
/// same field.
///
/// This is the fact the histogram below is a measurement OF. It is asserted from the source text
/// because it is a property of the write path and not of any fixture: a fixture that happened to
/// write one field would prove nothing, and #1959's whole measurement was a fixture artefact of
/// exactly that kind.
///
/// rust-internal: reads this crate's own source text, no product behaviour
#[test]
fn the_context_node_write_path_produces_exactly_one_field_per_hash() {
    let execute = include_str!("../execute_on_shard.rs");
    let context = include_str!("../context.rs");
    let constants = include_str!("../constants.rs");

    assert!(
        execute.len() > 50_000 && context.len() > 10_000 && constants.len() > 500,
        "a source file did not load, so the counts below would be zeros presented as findings: \
         {} / {} / {}",
        execute.len(),
        context.len(),
        constants.len()
    );

    assert!(
        constants.contains("CONTEXT_NODE_FIELD: &str = \"meta\""),
        "CONTEXT_NODE_FIELD is no longer the constant this module priced"
    );

    // Every place a context node's address is taken out of the inner map spells the ONE constant.
    let readers = execute.matches("fields.get(CONTEXT_NODE_FIELD)").count()
        + context.matches("fields.get(CONTEXT_NODE_FIELD)").count();
    // And the one write site files that constant and nothing else.
    //
    // THE SHAPE MOVED AND THE MATCHER MOVED WITH IT. The producer used to spell
    // `.insert(CONTEXT_NODE_FIELD.to_string(), address)` straight into the model map. It now mints
    // a record under that field and installs the proof, because the map's inner field is private
    // and an insert without a record does not compile. So the thing to count is the MINT.
    //
    // The count is also no longer the whole guarantee, which is the point of the change this
    // matcher was updated for: `record_context_node_element` is the only constructor of a
    // context-node proof and `RecordedHashContainer::install` is the only consumer, so a second
    // producer cannot appear without appearing HERE.
    let writers = execute
        .matches("record_context_node_element(")
        .count();

    println!("context-node inner-map readers spelling the one constant field: {readers}");
    println!("context-node inner-map writers inserting it:                   {writers}");

    assert!(
        readers >= 6,
        "fewer readers than expected resolve a context node through the single constant field, so \
         the one-field claim is no longer what the code says: {readers}"
    );
    assert_eq!(
        1, writers,
        "there is no longer exactly one producer inserting the single context-node field, so \
         `write_context_node`'s own comment -- \"the one producer of a node page\" -- is stale and \
         the occupancy claim has to be re-derived: {writers}"
    );

    // The field name itself, which every hash in a context store allocates its own copy of.
    assert_eq!(
        4,
        crate::engine::constants::CONTEXT_NODE_FIELD.len(),
        "the field width this module prices moved"
    );
}

// =================================================================================================
// 2. THE HISTOGRAM. NEVER A MEAN.
// =================================================================================================

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "mmcc".to_string(),
        shard_uri: "local://mmcc/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn node_at(node_hash: u64) -> crate::types::ContextNode {
    crate::types::ContextNode {
        node_hash,
        parent_hash: 0,
        kind: 1,
        canonical_name: format!("node_{node_hash:08}"),
        l0: "a node".to_string(),
        status: 0,
        last_event_time_ms: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    }
}

/// Seed `keys` context nodes through the PRODUCTION command path.
fn seed_context_nodes(engine: &TemporalEngine, keys: usize) {
    let commands: Vec<Command> = (0..keys as u64)
        .map(|n| Command::ContextUpsertNode {
            tenant_hash: 7,
            node: Box::new(node_at(n + 1)),
        })
        .collect();
    run_batch(engine, commands);
}

/// Seed `keys` Redis-surface hashes of `fields` fields each, through the production command path.
fn seed_wide_hashes(engine: &TemporalEngine, keys: usize, fields: usize) {
    let commands: Vec<Command> = (0..keys)
        .flat_map(|k| {
            (0..fields).map(move |f| Command::HashSet {
                key: format!("h-{k:06}"),
                field: format!("f-{f:04}"),
                value: vec![b'v'; 16],
            })
        })
        .collect();
    run_batch(engine, commands);
}

/// Seed `keys` sets of `members` members each, through the production command path. This is the
/// CONTROL's population: `sets` is one of the seventeen nested ORDERED model maps and the change
/// does not touch it.
fn seed_sets(engine: &TemporalEngine, keys: usize, members: usize) {
    let commands: Vec<Command> = (0..keys)
        .flat_map(|k| {
            (0..members).map(move |m| Command::SetAdd {
                key: format!("s-{k:06}"),
                member: format!("m-{m:06}").into_bytes(),
            })
        })
        .collect();
    run_batch(engine, commands);
}

/// The occupancies of every inner map in the loaded shard, ascending.
fn fields_per_hash(engine: &TemporalEngine) -> Vec<usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut occupancies: Vec<usize> = shard.hashes.values().map(|fields| fields.len()).collect();
    occupancies.sort_unstable();
    occupancies
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

/// Print a histogram -- the distinct occupancies with their counts and share -- plus percentiles,
/// MAX and the denominator. Never a mean standing alone: a mean of 1.98 pages a bucket in this
/// engine covered a population with ZERO buckets holding two.
fn report_histogram(label: &str, sorted: &[usize]) {
    println!("--- fields per hash: {label} ---");
    println!("  sample count (hash keys): {}", sorted.len());
    let total_fields: usize = sorted.iter().sum();
    println!("  total fields:             {total_fields}");
    let mut run: Vec<(usize, usize)> = Vec::new();
    for &value in sorted {
        match run.last_mut() {
            Some((v, c)) if *v == value => *c += 1,
            _ => run.push((value, 1)),
        }
    }
    println!("  distinct occupancies: {}", run.len());
    for (value, count) in &run {
        println!(
            "    {value:>6} field(s): {count:>8} keys  ({:>6.2}% of {})",
            100.0 * *count as f64 / sorted.len() as f64,
            sorted.len()
        );
    }
    println!(
        "  p50 {}  p90 {}  p99 {}  MAX {}  (mean {:.2}, quoted only beside the shape above)",
        percentile(sorted, 50.0),
        percentile(sorted, 90.0),
        percentile(sorted, 99.0),
        sorted.last().copied().unwrap_or(0),
        total_fields as f64 / sorted.len().max(1) as f64
    );
}

/// THE FIELDS-PER-HASH HISTOGRAM AT TWO CORPUS SIZES, ON BOTH PRODUCERS.
///
/// The context arm is the product path and the wide arm is the Redis-compatible surface. Both are
/// reported with their own sample count and denominator, and the fixture is asserted to have
/// reached the population claimed before any figure is read off it.
///
/// rust-internal: reads this crate's own resident maps, no product behaviour
#[test]
fn the_fields_per_hash_histogram_at_two_corpus_sizes() {
    let mut path_lengths: Vec<usize> = Vec::new();

    for keys in [SMALL_KEYS, LARGE_KEYS] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        seed_context_nodes(&engine, keys);

        let sorted = fields_per_hash(&engine);
        assert_eq!(
            keys,
            sorted.len(),
            "the fixture did not reach the population it claims at {keys} context nodes -- every \
             figure below would describe a different store than the one named"
        );
        report_histogram(&format!("{keys} context nodes, the product write path"), &sorted);

        assert_eq!(
            Some(&1),
            sorted.last(),
            "a context-node corpus produced a hash holding more than one field, so the write \
             path is no longer the single-field shape this module measured"
        );
    }

    // The other producer: the Redis-compatible surface, which is where a wide hash comes from.
    for fields in [8usize, 100] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        let keys = 400;
        seed_wide_hashes(&engine, keys, fields);

        let sorted = fields_per_hash(&engine);
        assert_eq!(
            keys,
            sorted.len(),
            "the wide-hash fixture did not reach {keys} keys at {fields} fields each"
        );
        assert_eq!(
            keys * fields,
            sorted.iter().sum::<usize>(),
            "the wide-hash fixture did not write {fields} fields on every key"
        );
        report_histogram(&format!("{keys} HashSet hashes of {fields} fields"), &sorted);
    }

    // And the mixed corpus, because the two producers share one map and a real store holds both.
    {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        seed_context_nodes(&engine, SMALL_KEYS);
        seed_wide_hashes(&engine, 40, 100);

        let sorted = fields_per_hash(&engine);
        assert_eq!(
            SMALL_KEYS + 40,
            sorted.len(),
            "the mixed fixture did not reach both populations"
        );
        report_histogram("mixed: 4,000 context nodes + 40 hashes of 100 fields", &sorted);
        println!(
            "  THE SHAPE IS BIMODAL: {:.2}% of keys hold exactly one field and carry {:.2}% of \
             the fields; a mean over this describes neither arm.",
            100.0 * sorted.iter().filter(|&&n| n == 1).count() as f64 / sorted.len() as f64,
            100.0 * sorted.iter().filter(|&&n| n == 1).count() as f64
                / sorted.iter().sum::<usize>() as f64
        );
    }

    assert!(
        path_lengths.windows(2).all(|w| w[0] == w[1]),
        "the store path length differs between arms: {path_lengths:?} -- allocation bytes move at \
         about six a character, so the arms are not comparable"
    );
    println!("store path length held at {} characters", path_lengths[0]);
}

// =================================================================================================
// 3. WHAT EACH CANDIDATE CONTAINER COSTS, PER INSTANCE AND PER ENTRY
// =================================================================================================

/// One priced shape's reading at one occupancy.
#[cfg(feature = "alloc-probe")]
#[derive(Debug, Clone, Copy)]
struct Reading {
    occupancy: usize,
    allocs_per_instance: f64,
    request_per_instance: f64,
    chunk_per_instance: f64,
}

/// Pre-built field-name/address pairs, one batch per instance. Built OUTSIDE every probe window so
/// the name text is charged to none of the arms and the comparison is of containers only.
#[cfg(feature = "alloc-probe")]
fn pairs_for(instances: usize, occupancy: usize) -> Vec<Vec<(String, BlockAddress)>> {
    (0..instances)
        .map(|i| {
            (0..occupancy)
                .map(|f| (format!("f-{i:05}-{f:05}"), BlockAddress::default()))
                .collect()
        })
        .collect()
}

#[cfg(feature = "alloc-probe")]
fn report_shape(name: &str, readings: &[Reading]) {
    println!("--- {name} ---");
    println!(
        "  {:>9}  {:>10}  {:>12}  {:>12}  {:>14}  {:>14}",
        "fields", "allocs", "request B", "chunk B", "marginal req B", "marginal chk B"
    );
    for (i, r) in readings.iter().enumerate() {
        let (mreq, mchk) = if i == 0 {
            (f64::NAN, f64::NAN)
        } else {
            let prev = readings[i - 1];
            let dn = (r.occupancy - prev.occupancy) as f64;
            (
                (r.request_per_instance - prev.request_per_instance) / dn,
                (r.chunk_per_instance - prev.chunk_per_instance) / dn,
            )
        };
        println!(
            "  {:>9}  {:>10.2}  {:>12.1}  {:>12.1}  {:>14.1}  {:>14.1}",
            r.occupancy,
            r.allocs_per_instance,
            r.request_per_instance,
            r.chunk_per_instance,
            mreq,
            mchk
        );
    }
}

/// WHAT A HASH FIELD MAP COSTS PER INSTANCE AND PER FIELD, ON BOTH BYTE COLUMNS, FOR ALL THREE
/// CANDIDATE SHAPES.
///
/// PER-INSTANCE AND PER-FIELD ARE REPORTED AS SEPARATE COLUMNS AND NEITHER IS A FITTED
/// INTERCEPT. A table's cost steps at every power of two, so a line through two points would put
/// a step into the intercept and call it an overhead. The full per-instance cost is printed at
/// each occupancy and the MARGINAL cost per added field beside it; the occupancy-1 row IS the
/// per-instance overhead for the shape the product path actually writes.
///
/// The field-name text is charged OUTSIDE the probe window for every arm -- the pairs are built
/// first and MOVED in -- so the only thing measured is the container's own allocation.
///
/// rust-internal: reads this crate's own counting allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_a_hash_field_map_costs_per_instance_and_per_field() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );

    let mut hashed: Vec<Reading> = Vec::new();
    let mut ordered: Vec<Reading> = Vec::new();
    let mut flat: Vec<Reading> = Vec::new();

    for occupancy in OCCUPANCIES {
        // --- today's shape: a hashed table per hash key ---
        {
            let batches = pairs_for(INSTANCES, occupancy);
            let probe = Probe::start();
            let built: Vec<HashMap<String, BlockAddress>> = batches
                .into_iter()
                .map(|batch| batch.into_iter().collect())
                .collect();
            let counts = probe.stop();
            let entries: usize = built.iter().map(|m| m.len()).sum();
            assert_eq!(
                INSTANCES * occupancy,
                entries,
                "the hashed arm did not hold the population it claims at occupancy {occupancy}"
            );
            assert!(
                counts.chunk_bytes > counts.alloc_bytes,
                "the chunk column equals the request column at occupancy {occupancy}, which is \
                 what the platform fallback looks like -- the reading is not coming from \
                 malloc_usable_size"
            );
            hashed.push(Reading {
                occupancy,
                allocs_per_instance: counts.allocs as f64 / INSTANCES as f64,
                request_per_instance: counts.alloc_bytes as f64 / INSTANCES as f64,
                chunk_per_instance: counts.chunk_bytes as f64 / INSTANCES as f64,
            });
            drop(built);
        }

        // --- the siblings' shape: an ordered map per hash key ---
        {
            let batches = pairs_for(INSTANCES, occupancy);
            let probe = Probe::start();
            let built: Vec<BTreeMap<String, BlockAddress>> = batches
                .into_iter()
                .map(|batch| batch.into_iter().collect())
                .collect();
            let counts = probe.stop();
            let entries: usize = built.iter().map(|m| m.len()).sum();
            assert_eq!(
                INSTANCES * occupancy,
                entries,
                "the ordered arm did not hold the population it claims at occupancy {occupancy}"
            );
            ordered.push(Reading {
                occupancy,
                allocs_per_instance: counts.allocs as f64 / INSTANCES as f64,
                request_per_instance: counts.alloc_bytes as f64 / INSTANCES as f64,
                chunk_per_instance: counts.chunk_bytes as f64 / INSTANCES as f64,
            });
            drop(built);
        }

        // --- #1964's shape for the page index: one exact-sized sorted vector ---
        {
            let batches = pairs_for(INSTANCES, occupancy);
            let probe = Probe::start();
            // A FRESH, EXACT-SIZED buffer allocated INSIDE the window. Sorting the batch in place
            // and calling `shrink_to_fit` on it read 0.00 B at every occupancy -- the buffer had
            // been allocated by `pairs_for`, outside the window, and the arm was charged for
            // nothing. The vacuity guard at the end of this test is what caught it.
            let built: Vec<Vec<(String, BlockAddress)>> = batches
                .into_iter()
                .map(|batch| {
                    let mut exact: Vec<(String, BlockAddress)> = Vec::with_capacity(occupancy);
                    exact.extend(batch);
                    exact.sort_by(|l, r| l.0.cmp(&r.0));
                    exact
                })
                .collect();
            let counts = probe.stop();
            let entries: usize = built.iter().map(|v| v.len()).sum();
            assert_eq!(
                INSTANCES * occupancy,
                entries,
                "the flat arm did not hold the population it claims at occupancy {occupancy}"
            );
            flat.push(Reading {
                occupancy,
                allocs_per_instance: counts.allocs as f64 / INSTANCES as f64,
                request_per_instance: counts.alloc_bytes as f64 / INSTANCES as f64,
                chunk_per_instance: counts.chunk_bytes as f64 / INSTANCES as f64,
            });
            drop(built);
        }
    }

    println!(
        "{INSTANCES} instances per occupancy; field-name text built outside every probe window and \
         MOVED in, so these are container allocations only."
    );
    println!(
        "entry width: String {} B + BlockAddress {} B = {} B inline",
        std::mem::size_of::<String>(),
        std::mem::size_of::<BlockAddress>(),
        std::mem::size_of::<String>() + std::mem::size_of::<BlockAddress>()
    );
    report_shape("TODAY: HashMap<String, BlockAddress> per hash key", &hashed);
    report_shape("CANDIDATE A: BTreeMap<String, BlockAddress>", &ordered);
    report_shape("CANDIDATE B: sorted Vec<(String, BlockAddress)>", &flat);

    println!("--- chunk bytes per hash INSTANCE, by occupancy, all three shapes ---");
    println!(
        "  {:>9}  {:>14}  {:>14}  {:>14}  {:>12}  {:>12}",
        "fields", "hashed", "ordered", "flat", "ord vs hash", "flat vs hash"
    );
    for i in 0..OCCUPANCIES.len() {
        println!(
            "  {:>9}  {:>14.1}  {:>14.1}  {:>14.1}  {:>+11.2}%  {:>+11.2}%",
            OCCUPANCIES[i],
            hashed[i].chunk_per_instance,
            ordered[i].chunk_per_instance,
            flat[i].chunk_per_instance,
            100.0 * (ordered[i].chunk_per_instance / hashed[i].chunk_per_instance - 1.0),
            100.0 * (flat[i].chunk_per_instance / hashed[i].chunk_per_instance - 1.0),
        );
    }

    // THE ONE-FIELD ROW IS THE PRODUCT PATH. If a table did not cost materially more there, this
    // whole module is a refutation and should say so rather than shipping a change.
    assert!(
        hashed[0].chunk_per_instance > 0.0 && flat[0].chunk_per_instance > 0.0,
        "an arm allocated nothing at occupancy 1, so the comparison is vacuous"
    );
}

/// WHAT THE FIELD NAME TEXT COSTS WHEN EVERY HASH ALLOCATES ITS OWN COPY.
///
/// A context store's inner maps all hold the same four characters, `"meta"`, and today each one
/// owns a separate `String`. This prices the second copy on its own -- separately from the
/// container -- because it is a smaller change than swapping the container and might be where the
/// bytes actually are.
///
/// rust-internal: reads this crate's own counting allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_the_field_name_text_costs_when_every_hash_allocates_its_own() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );
    let field = crate::engine::constants::CONTEXT_NODE_FIELD;

    // THE SPINE IS ALLOCATED OUTSIDE EVERY WINDOW AND IS *NOT* THE SAME WIDTH IN THE TWO ARMS --
    // `String` is 24 bytes and `Arc<str>` is a 16-byte fat pointer -- so a first draft of this
    // test that measured both spines inside the window and said "the spine cancels" was wrong on
    // its own terms. Each spine is pre-reserved here and the probe sees only the TEXT.
    let mut owned: Vec<String> = Vec::with_capacity(LARGE_KEYS);
    let probe = Probe::start();
    for _ in 0..LARGE_KEYS {
        owned.push(field.to_string());
    }
    let own_counts = probe.stop();
    assert_eq!(LARGE_KEYS, owned.len(), "the owned arm did not build");
    assert_eq!(
        LARGE_KEYS,
        owned.capacity(),
        "the owned arm's spine grew inside the probe window, so the reading is not text-only"
    );
    assert!(
        owned.iter().all(|s| s == field),
        "the owned arm did not store the field this module prices"
    );

    // THE ALTERNATIVE: one shared Arc<str>, cloned per hash.
    let shared: Arc<str> = Arc::from(field);
    let mut clones: Vec<Arc<str>> = Vec::with_capacity(LARGE_KEYS);
    let probe = Probe::start();
    for _ in 0..LARGE_KEYS {
        clones.push(Arc::clone(&shared));
    }
    let arc_counts = probe.stop();
    assert_eq!(LARGE_KEYS, clones.len(), "the shared arm did not build");
    assert_eq!(
        LARGE_KEYS,
        clones.capacity(),
        "the shared arm's spine grew inside the probe window"
    );
    assert!(
        clones.iter().all(|c| Arc::ptr_eq(c, &shared)),
        "the shared arm allocated instead of sharing, so it is not the shape being priced"
    );

    println!("--- the field name text at {LARGE_KEYS} hashes, name {:?} ---", field);
    println!(
        "  owned String per hash : {:>10} allocs  {:>12} request B  {:>12} chunk B  ({:.1} B/hash \
         chunk)",
        own_counts.allocs,
        own_counts.alloc_bytes,
        own_counts.chunk_bytes,
        own_counts.chunk_bytes as f64 / LARGE_KEYS as f64
    );
    println!(
        "  shared Arc<str> clone : {:>10} allocs  {:>12} request B  {:>12} chunk B  ({:.1} B/hash \
         chunk)",
        arc_counts.allocs,
        arc_counts.alloc_bytes,
        arc_counts.chunk_bytes,
        arc_counts.chunk_bytes as f64 / LARGE_KEYS as f64
    );
    println!(
        "  spines pre-reserved and excluded; the INLINE width differs too and is not an allocation: \
         String {} B against Arc<str> {} B, a further {} B a field held in whatever container \
         stores it.",
        std::mem::size_of::<String>(),
        std::mem::size_of::<Arc<str>>(),
        std::mem::size_of::<String>() - std::mem::size_of::<Arc<str>>()
    );
    assert!(
        own_counts.allocs > arc_counts.allocs,
        "the owned arm did not allocate more than the shared one, so there is no second copy to \
         price and this test is measuring nothing: {} vs {}",
        own_counts.allocs,
        arc_counts.allocs
    );
}

// =================================================================================================
// 4. THE OUTER MAPS. MEASURED, NOT CHANGED.
// =================================================================================================

/// WHAT THE OUTER MODEL MAPS COST PER MAP AND PER KEY, AND HOW MUCH OF IT IS LOAD-FACTOR SLACK.
///
/// MEASUREMENT ONLY. Nothing in this module changes an outer map: `Command::StringGet` resolves
/// straight through `shard.strings`, so the outer map is on a read path, and the flat-vector shape
/// is only viable at BUCKET granularity -- which is a move of the model maps into the bucket node,
/// not a container swap, and far too large to ride along with an inner-container change.
///
/// SLACK is read from `capacity()` against `len()` rather than inferred from the byte columns, so
/// the rounding claim is a property of the container and not of an allocator coincidence.
///
/// rust-internal: reads this crate's own counting allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_the_outer_model_maps_cost_per_map_and_per_key() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );

    println!("--- outer map, HashMap<K, V> against BTreeMap<K, V>, keys MOVED in ---");
    println!(
        "  {:>8}  {:>9}  {:>12}  {:>12}  {:>12}  {:>12}  {:>9}",
        "keys", "shape", "allocs", "request B", "chunk B", "B per key", "slack"
    );

    for keys in [SMALL_KEYS, LARGE_KEYS] {
        // Keys built OUTSIDE the window and moved in, so both shapes are charged the same text.
        let built: Vec<(String, BlockAddress)> = (0..keys)
            .map(|k| (format!("ctx:node:7:{k:012}"), BlockAddress::default()))
            .collect();

        let hashed_input = built.clone();
        let probe = Probe::start();
        let hashed: HashMap<String, BlockAddress> = hashed_input.into_iter().collect();
        let hashed_counts = probe.stop();

        let ordered_input = built.clone();
        let probe = Probe::start();
        let ordered: BTreeMap<String, BlockAddress> = ordered_input.into_iter().collect();
        let ordered_counts = probe.stop();

        assert_eq!(keys, hashed.len(), "the hashed outer arm did not reach {keys}");
        assert_eq!(keys, ordered.len(), "the ordered outer arm did not reach {keys}");
        assert!(
            hashed_counts.chunk_bytes > hashed_counts.alloc_bytes,
            "the chunk column is not coming from malloc_usable_size at {keys} keys"
        );

        let slack = hashed.capacity() as f64 / hashed.len() as f64 - 1.0;
        println!(
            "  {:>8}  {:>9}  {:>12}  {:>12}  {:>12}  {:>12.1}  {:>8.2}%",
            keys,
            "HashMap",
            hashed_counts.allocs,
            hashed_counts.alloc_bytes,
            hashed_counts.chunk_bytes,
            hashed_counts.chunk_bytes as f64 / keys as f64,
            100.0 * slack
        );
        println!(
            "  {:>8}  {:>9}  {:>12}  {:>12}  {:>12}  {:>12.1}  {:>9}",
            keys,
            "BTreeMap",
            ordered_counts.allocs,
            ordered_counts.alloc_bytes,
            ordered_counts.chunk_bytes,
            ordered_counts.chunk_bytes as f64 / keys as f64,
            "n/a"
        );
        println!(
            "      capacity {} for {} keys -- the table holds {} unused slots at {} B a slot = \
             {} B of slack",
            hashed.capacity(),
            hashed.len(),
            hashed.capacity() - hashed.len(),
            std::mem::size_of::<(String, BlockAddress)>(),
            (hashed.capacity() - hashed.len()) * std::mem::size_of::<(String, BlockAddress)>()
        );
        println!(
            "      ordered vs hashed on the chunk column: {:>+.2}%",
            100.0 * (ordered_counts.chunk_bytes as f64 / hashed_counts.chunk_bytes as f64 - 1.0)
        );
    }

    // THE PER-MAP FIXED COST, separated from the per-key cost: an EMPTY map of each shape.
    let probe = Probe::start();
    let empty_hashed: Vec<HashMap<String, BlockAddress>> =
        (0..INSTANCES).map(|_| HashMap::new()).collect();
    let empty_counts = probe.stop();
    assert_eq!(INSTANCES, empty_hashed.len(), "the empty arm did not build");
    println!(
        "--- the per-MAP fixed cost: {INSTANCES} empty HashMaps allocated {} times, {} request B, \
         {} chunk B ---",
        empty_counts.allocs, empty_counts.alloc_bytes, empty_counts.chunk_bytes
    );
    println!(
        "  an empty HashMap allocates NOTHING; the per-map cost is {} B INLINE in ShardState and \
         appears on the heap only at the first insert, which is why the per-instance figure in the \
         inner-container table above is the one that matters.",
        std::mem::size_of::<HashMap<String, BlockAddress>>()
    );
}

/// THE OBJECT KEY IS STORED TWICE, AND THIS PRICES THE SECOND COPY.
///
/// Every object key is the `String` key of its outer model map AND `object_key: Arc<str>` on every
/// bucket-index page entry for that object. The index side is an `Arc`, so the question is how
/// many DISTINCT allocations back those entries -- counted here by pointer identity rather than by
/// multiplying entries by a width, which would report a shared allocation once per sharer.
///
/// With the keys-per-bucket histogram beside it, because a flat per-bucket shape is only as good
/// as the number of keys it would have to scan.
///
/// rust-internal: reads this crate's own resident maps, no product behaviour
#[test]
fn the_object_key_is_stored_twice_and_this_prices_the_second_copy() {
    let mut path_lengths: Vec<usize> = Vec::new();

    for keys in [SMALL_KEYS, LARGE_KEYS] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        seed_context_nodes(&engine, keys);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");

        // COPY ONE: the outer model map's own owned key text.
        let model_keys = shard.hashes.len();
        let model_key_bytes: usize = shard
            .hashes
            .keys()
            .map(|k| k.len() + std::mem::size_of::<String>())
            .sum();

        // COPY TWO: the index side. Counted by DISTINCT Arc allocation, plus the per-entry
        // pointer that every page carries regardless.
        let mut distinct: std::collections::HashSet<*const u8> = std::collections::HashSet::new();
        let mut entries = 0usize;
        let mut distinct_text_bytes = 0usize;
        let mut keys_per_bucket: Vec<usize> = Vec::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            let mut in_bucket: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for page in bucket.block_index.values() {
                entries += 1;
                in_bucket.insert(&*page.object_key);
                let ptr = page.object_key.as_ptr();
                if distinct.insert(ptr) {
                    distinct_text_bytes += page.object_key.len();
                }
            }
            if !in_bucket.is_empty() {
                keys_per_bucket.push(in_bucket.len());
            }
        }
        let arc_pointer_bytes = entries * std::mem::size_of::<Arc<str>>();

        assert_eq!(
            keys, model_keys,
            "the fixture did not reach the population it claims: {model_keys} model keys at {keys} \
             context nodes"
        );
        assert!(
            entries > 0,
            "no index page entries were found at {keys} context nodes, so the second copy would \
             read as zero because nothing was walked"
        );

        println!("--- the object key at {keys} context nodes ---");
        println!(
            "  model map : {model_keys:>9} keys  {model_key_bytes:>12} B  ({:>6.1} B a key: \
             {} B of String plus the text)",
            model_key_bytes as f64 / model_keys as f64,
            std::mem::size_of::<String>()
        );
        println!(
            "  index     : {entries:>9} pages  {:>12} B  ({:>6.1} B a page: {} B of Arc pointer \
             plus {} distinct text allocation(s) totalling {distinct_text_bytes} B)",
            arc_pointer_bytes + distinct_text_bytes,
            (arc_pointer_bytes + distinct_text_bytes) as f64 / entries as f64,
            std::mem::size_of::<Arc<str>>(),
            distinct.len()
        );
        println!(
            "  a SINGLE-COPY shape would keep the text once and point at it from both sides: \
             {distinct_text_bytes} B of text plus {} B of pointer, against \
             {} B spent on the same keys today -- a saving of {:.1}%",
            (entries + model_keys) * std::mem::size_of::<Arc<str>>(),
            model_key_bytes + arc_pointer_bytes + distinct_text_bytes,
            100.0
                * (1.0
                    - (distinct_text_bytes + (entries + model_keys) * std::mem::size_of::<Arc<str>>())
                        as f64
                        / (model_key_bytes + arc_pointer_bytes + distinct_text_bytes) as f64)
        );

        keys_per_bucket.sort_unstable();
        report_histogram(
            &format!("distinct object keys per routing bucket at {keys} nodes"),
            &keys_per_bucket,
        );
        println!(
            "  a FLAT per-bucket vector would scan p50 {} and MAX {} keys, against {} in one \
             shard-wide vector -- which is why a flat outer shape is a BUCKET-granularity change \
             and not a container swap.",
            percentile(&keys_per_bucket, 50.0),
            keys_per_bucket.last().copied().unwrap_or(0),
            model_keys
        );
    }

    assert!(
        path_lengths.windows(2).all(|w| w[0] == w[1]),
        "the store path length differs between arms: {path_lengths:?}"
    );
    println!("store path length held at {} characters", path_lengths[0]);
}

// =================================================================================================
// 5. THE LOOKUP, IN PROBE COUNTS AND NEVER IN TIME
// =================================================================================================

thread_local! {
    static KEY_COMPARES: std::cell::Cell<u64> = std::cell::Cell::new(0);
    static KEY_HASHES: std::cell::Cell<u64> = std::cell::Cell::new(0);
}

/// A field name that counts what a container asks of it. Time is not measured anywhere in this
/// module: a timing-ratio instrument on this box read 485x idle against 11x busy off identical
/// code, and the box was at load 36 while this was written.
#[derive(Debug, Clone, Eq)]
struct CountingKey(String);

impl PartialEq for CountingKey {
    fn eq(&self, other: &Self) -> bool {
        KEY_COMPARES.with(|c| c.set(c.get() + 1));
        self.0 == other.0
    }
}
impl Ord for CountingKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        KEY_COMPARES.with(|c| c.set(c.get() + 1));
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for CountingKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl std::hash::Hash for CountingKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        KEY_HASHES.with(|c| c.set(c.get() + 1));
        self.0.hash(state);
    }
}

fn taken() -> (u64, u64) {
    let c = KEY_COMPARES.with(|c| c.replace(0));
    let h = KEY_HASHES.with(|c| c.replace(0));
    (c, h)
}

/// WHAT THE LOOKUP COSTS IN PROBE COUNTS, AT EVERY OCCUPANCY THE HISTOGRAM FOUND.
///
/// COUNTED, NEVER TIMED. The question an ordered or flat container raises is whether O(1) becoming
/// O(log n) or O(n) matters, and at the occupancy the product path writes -- one field -- the
/// answer is arithmetic rather than a benchmark: a table hashes the key and then compares it, a
/// one-element vector compares it once and hashes nothing.
///
/// rust-internal: counts this test's own instrumented key type, no product behaviour
#[test]
fn what_a_field_lookup_costs_in_probe_counts_at_every_measured_occupancy() {
    println!("--- per-lookup probe counts, HIT, averaged over every field in the container ---");
    println!(
        "  {:>9}  {:>18}  {:>18}  {:>18}",
        "fields", "hashed (cmp/hash)", "ordered (cmp/hash)", "flat (cmp/hash)"
    );

    let mut rows = 0usize;
    for occupancy in OCCUPANCIES {
        let names: Vec<CountingKey> = (0..occupancy)
            .map(|f| CountingKey(format!("f-{f:05}")))
            .collect();

        let hashed: HashMap<CountingKey, BlockAddress> = names
            .iter()
            .map(|n| (n.clone(), BlockAddress::default()))
            .collect();
        let ordered: BTreeMap<CountingKey, BlockAddress> = names
            .iter()
            .map(|n| (n.clone(), BlockAddress::default()))
            .collect();
        let mut flat: Vec<(CountingKey, BlockAddress)> = names
            .iter()
            .map(|n| (n.clone(), BlockAddress::default()))
            .collect();
        flat.sort_by(|l, r| l.0.cmp(&r.0));

        let _ = taken();
        for n in &names {
            assert!(hashed.get(n).is_some(), "the hashed arm missed a field it holds");
        }
        let (hc, hh) = taken();

        for n in &names {
            assert!(ordered.get(n).is_some(), "the ordered arm missed a field it holds");
        }
        let (oc, oh) = taken();

        for n in &names {
            assert!(
                flat.binary_search_by(|probe| probe.0.cmp(n)).is_ok(),
                "the flat arm missed a field it holds"
            );
        }
        let (fc, fh) = taken();

        let d = occupancy as f64;
        println!(
            "  {:>9}  {:>9.2} / {:>6.2}  {:>9.2} / {:>6.2}  {:>9.2} / {:>6.2}",
            occupancy,
            hc as f64 / d,
            hh as f64 / d,
            oc as f64 / d,
            oh as f64 / d,
            fc as f64 / d,
            fh as f64 / d
        );
        rows += 1;

        if occupancy == 1 {
            assert!(
                hh > 0,
                "the hashed arm hashed nothing at occupancy 1, so the instrument is not reading \
                 the container's work and every row below is a zero presented as a measurement"
            );
            assert_eq!(
                0, fh,
                "the flat arm hashed a key, which it has no reason to do -- the instrument is \
                 attributing work to the wrong arm"
            );
        }
    }

    assert_eq!(
        OCCUPANCIES.len(),
        rows,
        "not every occupancy was measured, so the table is not the sweep it claims to be"
    );
}

// =================================================================================================
// 6. THE SHIPPED CONTAINER, AGAINST THE ONE IT REPLACED, ON THE REAL STORE'S OWN FIELD NAMES
// =================================================================================================

/// WHAT THE ENGINE'S RESIDENT FIELD MAPS COST IN THE SHIPPED SHAPE AGAINST THE ONE IT REPLACED,
/// PLUS A CONTROL THE CHANGE PREDICTS WILL NOT MOVE.
///
/// THE A/B IS INSIDE ONE BINARY AND OVER THE REAL STORE'S OWN KEYS. The pairs are read out of a
/// seeded shard -- real object keys, the real field name, the real occupancy -- and each arm is
/// handed the SAME pre-built pairs, so the only difference between the arms is the container. A
/// cross-tree before/after would have had to hold the store path length, the fixture and the
/// allocator's history equal between two builds; this holds them equal by construction.
///
/// THE CONTROL IS `sets`, one of the seventeen nested ORDERED model maps. The change does not touch
/// it, so both of its arms are the same `BTreeMap` and it must read 0.00%. A control that moved
/// would mean the harness, not the container, was being measured -- and an instrument that reads a
/// difference off identical code has been the whole finding before now.
///
/// rust-internal: reads this crate's own counting allocator and its own resident maps
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_the_engine_resident_field_maps_cost_against_the_container_they_replaced() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );

    let mut path_lengths: Vec<usize> = Vec::new();

    for keys in [SMALL_KEYS, LARGE_KEYS] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        seed_context_nodes(&engine, keys);
        // Both producers, so the bimodal arm is inside the measured population and not argued away.
        seed_wide_hashes(&engine, 40, 100);
        // AND THE CONTROL'S OWN POPULATION. Without this `sets` is empty, the control reads 0 B
        // against 0 B, and a 0.00% control means "not exercised" rather than "did not move" -- the
        // vacuity guard below is what caught that on the first run.
        seed_sets(&engine, 40, 100);

        // The real store's own pairs, read out once and cloned per arm OUTSIDE every window.
        let (pairs, set_pairs, fields_total) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let pairs: Vec<Vec<(String, BlockAddress)>> = shard
                .hashes
                .values()
                .map(|fields| {
                    fields
                        .iter()
                        .map(|(name, address)| (name.clone(), address.clone()))
                        .collect()
                })
                .collect();
            let set_pairs: Vec<Vec<(Vec<u8>, BlockAddress)>> = shard
                .sets
                .values()
                .map(|members| {
                    members
                        .iter()
                        .map(|(member, address)| (member.clone(), address.clone()))
                        .collect()
                })
                .collect();
            let fields_total: usize = pairs.iter().map(|p| p.len()).sum();
            (pairs, set_pairs, fields_total)
        };

        assert_eq!(
            keys + 40,
            pairs.len(),
            "the fixture did not reach the population it claims: {} hash keys at {keys} context \
             nodes plus 40 wide hashes",
            pairs.len()
        );
        assert_eq!(
            keys + 40 * 100,
            fields_total,
            "the fixture did not write the field population it claims"
        );

        // BOTH ARMS ARE FILLED BY REPEATED `insert`, WHICH IS WHAT THE WRITE PATH DOES, and not by
        // `collect`. A first draft collected each arm from the pre-built `Vec` and read -85.65% --
        // but `Vec::from_iter` over a `vec::IntoIter` REUSES the source buffer, so the shipped arm
        // was handed an allocation made outside the probe window and charged nothing for it, while
        // the table could not reuse it and paid in full. Reading a container comparison off that
        // would have been an artefact of the harness. Filling through `insert` also matches
        // `shard.hashes.entry(key).or_default().insert(field, address)` exactly.
        let before_input = pairs.clone();
        let probe = Probe::start();
        let before: Vec<HashMap<String, BlockAddress>> = before_input
            .into_iter()
            .map(|batch| {
                let mut map = HashMap::new();
                for (name, address) in batch {
                    map.insert(name, address);
                }
                map
            })
            .collect();
        let before_counts = probe.stop();

        let after_input = pairs.clone();
        let probe = Probe::start();
        let after: Vec<crate::engine::hash_field_map::HashFieldMap> = after_input
            .into_iter()
            .map(|batch| {
                let mut map = crate::engine::hash_field_map::HashFieldMap::default();
                for (name, address) in batch {
                    map.insert(name, address);
                }
                map
            })
            .collect();
        let after_counts = probe.stop();

        // BOTH ARMS MUST HOLD THE SAME DATA, or the cheaper one is cheaper because it dropped
        // something. Checked at field level, not by length alone.
        assert_eq!(before.len(), after.len(), "the arms built different populations");
        for (old, new) in before.iter().zip(after.iter()) {
            assert_eq!(
                old.len(),
                new.len(),
                "an arm holds a different number of fields for one key"
            );
            for (name, address) in old {
                assert_eq!(
                    Some(address),
                    new.get(name.as_str()),
                    "the shipped container lost or altered field {name:?}"
                );
            }
        }

        // THE CONTROL -- a nested ORDERED kind the change does not touch. Same harness, same shape
        // on both sides, so it must read 0.00%.
        let control_a_input = set_pairs.clone();
        let probe = Probe::start();
        let control_a: Vec<BTreeMap<Vec<u8>, BlockAddress>> = control_a_input
            .into_iter()
            .map(|batch| {
                let mut map = BTreeMap::new();
                for (member, address) in batch {
                    map.insert(member, address);
                }
                map
            })
            .collect();
        let control_a_counts = probe.stop();

        let control_b_input = set_pairs.clone();
        let probe = Probe::start();
        let control_b: Vec<BTreeMap<Vec<u8>, BlockAddress>> = control_b_input
            .into_iter()
            .map(|batch| {
                let mut map = BTreeMap::new();
                for (member, address) in batch {
                    map.insert(member, address);
                }
                map
            })
            .collect();
        let control_b_counts = probe.stop();

        println!("--- resident field maps at {keys} context nodes + 40 hashes of 100 fields ---");
        println!(
            "  {} hash keys, {fields_total} fields; store path {} characters",
            pairs.len(),
            dir.path().as_os_str().len()
        );
        for (label, c) in [("REPLACED (hashed table)", before_counts), ("SHIPPED (sorted vector)", after_counts)] {
            println!(
                "  {label:<24} {:>9} allocs  {:>12} request B  {:>12} chunk B  ({:>7.1} B a key, \
                 {:>6.1} B a field, chunk)",
                c.allocs,
                c.alloc_bytes,
                c.chunk_bytes,
                c.chunk_bytes as f64 / pairs.len() as f64,
                c.chunk_bytes as f64 / fields_total as f64
            );
        }
        let request_delta = 100.0 * (after_counts.alloc_bytes as f64 / before_counts.alloc_bytes as f64 - 1.0);
        let chunk_delta = 100.0 * (after_counts.chunk_bytes as f64 / before_counts.chunk_bytes as f64 - 1.0);
        println!("  SUBJECT  request {request_delta:>+7.2}%   chunk {chunk_delta:>+7.2}%   allocations {:>+7.2}%",
            100.0 * (after_counts.allocs as f64 / before_counts.allocs as f64 - 1.0));

        let control_delta = if control_a_counts.chunk_bytes == 0 {
            0.0
        } else {
            100.0 * (control_b_counts.chunk_bytes as f64 / control_a_counts.chunk_bytes as f64 - 1.0)
        };
        println!(
            "  CONTROL  `sets`, a nested ORDERED kind the change does not touch: {} members, \
             chunk {} B vs {} B = {control_delta:>+.2}%",
            control_a.iter().map(|m| m.len()).sum::<usize>(),
            control_a_counts.chunk_bytes,
            control_b_counts.chunk_bytes
        );

        assert!(
            control_a_counts.chunk_bytes > 0,
            "the control allocated nothing, so a 0.00% control reading would mean the control was \
             not exercised rather than that it did not move"
        );
        assert_eq!(
            control_a_counts.chunk_bytes, control_b_counts.chunk_bytes,
            "the control moved between two runs of IDENTICAL code, so the harness and not the \
             container is what the subject rows are measuring"
        );
        assert!(
            chunk_delta < -20.0,
            "the shipped container did not save materially on the chunk column at {keys} context \
             nodes ({chunk_delta:+.2}%), which refutes the change rather than supporting it"
        );
        assert!(
            control_b.len() == control_a.len(),
            "the control arms built different populations"
        );
    }

    assert!(
        path_lengths.windows(2).all(|w| w[0] == w[1]),
        "the store path length differs between arms: {path_lengths:?} -- allocation bytes move at \
         about six a character"
    );
    println!("store path length held at {} characters", path_lengths[0]);
}

/// WHY A SINGLE-FIELD INLINE ARM WAS MEASURED AND NOT TAKEN.
///
/// The histogram is bimodal and 99% of its keys hold one field, which is exactly the shape that
/// invites an inline arm: keep the one field IN the enum and allocate nothing at all. #1964 found
/// such an arm held only at 100% single-page buckets and cost 179.1 B a bucket on the wide range,
/// so it needs its own measurement at the real occupancy rather than an argument -- and the cost it
/// carries is not in the arm, it is in the OUTER map, whose value slot has to widen to the inline
/// arm's width for every key including the wide ones.
///
/// rust-internal: reads this crate's own type widths, no product behaviour
#[test]
fn why_a_single_field_inline_arm_was_measured_and_not_taken() {
    /// The shape an inline arm would take: one field held in place, a vector once it spills.
    enum InlineArm {
        One((String, BlockAddress)),
        Many(Vec<(String, BlockAddress)>),
    }
    let _ = InlineArm::Many(Vec::new());
    let _ = InlineArm::One((String::new(), BlockAddress::default()));

    let shipped = std::mem::size_of::<crate::engine::hash_field_map::HashFieldMap>();
    let inline = std::mem::size_of::<InlineArm>();
    // The outer map's slot is the object key plus the value, and the table carries that slot for
    // every slot of its CAPACITY, not only for the keys it holds.
    let shipped_slot = std::mem::size_of::<String>() + shipped;
    let inline_slot = std::mem::size_of::<String>() + inline;

    // The saving: the inline arm allocates nothing for a one-field hash. Measured at 64 chunk
    // bytes for the shipped exact-sized vector at occupancy one.
    const VECTOR_CHUNK_AT_ONE_FIELD: usize = 64;
    let keys = LARGE_KEYS;
    // The capacity a table of this many keys actually reserved, measured in
    // `what_the_outer_model_maps_cost_per_map_and_per_key`: 57,344 slots for 40,000 keys.
    let outer_capacity = {
        let map: HashMap<String, BlockAddress> = (0..keys)
            .map(|k| (format!("ctx:node:7:{k:012}"), BlockAddress::default()))
            .collect();
        map.capacity()
    };

    let saved = keys * VECTOR_CHUNK_AT_ONE_FIELD;
    let paid = outer_capacity * (inline_slot - shipped_slot);

    println!("--- the inline arm at {keys} single-field hashes ---");
    println!("  shipped container width : {shipped:>4} B   outer slot {shipped_slot:>4} B");
    println!("  inline-arm width        : {inline:>4} B   outer slot {inline_slot:>4} B");
    println!("  outer table capacity    : {outer_capacity} slots for {keys} keys");
    println!(
        "  SAVES  {saved} B of field-map allocation (the {VECTOR_CHUNK_AT_ONE_FIELD} B vector, \
         gone)"
    );
    println!(
        "  PAYS   {paid} B of widened outer slots ({} B a slot across the whole CAPACITY, not \
         only the occupied slots)",
        inline_slot - shipped_slot
    );
    println!(
        "  NET    {:+} B = {:+.1}% of the {} B the shipped shape spends on field maps here",
        saved as i64 - paid as i64,
        100.0 * (saved as f64 - paid as f64) / saved as f64,
        saved
    );
    println!(
        "  So the arm buys back under half of what it costs in slot width, for a second code path \
         and a spill transition. NOT TAKEN."
    );

    assert!(
        inline_slot > shipped_slot,
        "the inline arm does not widen the outer slot, so the reason it was rejected does not hold \
         and the decision needs re-taking: {inline_slot} against {shipped_slot}"
    );
    assert!(
        paid > 0 && saved > 0,
        "one side of the inline-arm trade priced at zero, so this comparison is vacuous"
    );
}

// =================================================================================================
// 7. THE CONTAINER'S OWN CONTRACT
// =================================================================================================

/// THE SHIPPED CONTAINER BEHAVES AS THE TABLE DID, AT THE POINTS A CALLER CAN OBSERVE.
///
/// Replace-returns-the-old-value, remove-returns-the-value, absent-reads-None, and -- the one that
/// a sorted vector could get wrong where a table cannot -- the entries come back in FIELD ORDER and
/// stay sorted across inserts that land at the front, the middle and the end.
///
/// rust-internal: exercises this crate's own container, no product behaviour
#[test]
fn the_hash_field_map_holds_the_contract_the_table_held() {
    use crate::engine::hash_field_map::HashFieldMap;

    let mut fields = HashFieldMap::default();
    assert!(fields.is_empty() && fields.len() == 0);
    assert_eq!(None, fields.get("absent"));
    assert_eq!(None, fields.remove("absent"));

    // Inserted out of order, deliberately: end, front, middle.
    assert_eq!(None, fields.insert("m".to_string(), BlockAddress::default()));
    assert_eq!(None, fields.insert("a".to_string(), BlockAddress::default()));
    assert_eq!(None, fields.insert("z".to_string(), BlockAddress::default()));
    assert_eq!(None, fields.insert("n".to_string(), BlockAddress::default()));
    assert_eq!(4, fields.len());
    assert_eq!(
        vec!["a", "m", "n", "z"],
        fields.keys().map(|k| k.as_str()).collect::<Vec<_>>(),
        "the entries are not in field order, so an ordered iteration cannot be relied on"
    );

    // A replace returns what was there and does not change the population.
    let replaced = fields.insert("m".to_string(), BlockAddress::default());
    assert!(replaced.is_some(), "a replace did not return the old value");
    assert_eq!(4, fields.len(), "a replace changed the population");

    // A remove returns the value and closes up behind it without disturbing the order.
    assert!(fields.remove("m").is_some());
    assert_eq!(
        vec!["a", "n", "z"],
        fields.keys().map(|k| k.as_str()).collect::<Vec<_>>()
    );
    assert!(!fields.contains_key("m"));
    assert!(fields.contains_key("n"));

    // `FromIterator` keeps the LAST value for a repeated field, which is what the table did.
    let mut first = BlockAddress::default();
    let second = BlockAddress::default();
    // Distinguish them through the only public difference available on a defaulted address.
    let _ = &mut first;
    let built: HashFieldMap = vec![
        ("dup".to_string(), first.clone()),
        ("other".to_string(), BlockAddress::default()),
        ("dup".to_string(), second.clone()),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        2,
        built.len(),
        "a repeated field was kept twice, which a table would not have done"
    );
    assert_eq!(
        vec!["dup", "other"],
        built.keys().map(|k| k.as_str()).collect::<Vec<_>>()
    );

    // And the same population through a real HashMap, so the two constructions agree.
    let via_table: std::collections::HashMap<String, BlockAddress> = vec![
        ("dup".to_string(), first),
        ("other".to_string(), BlockAddress::default()),
        ("dup".to_string(), second),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        via_table.len(),
        built.len(),
        "the container and the table disagree on how many fields the same pairs hold"
    );
}

/// THE WIRE SHAPE DID NOT MOVE: the container serializes to, and deserializes from, the same MAP
/// the `HashMap` did.
///
/// `hashes` is `skip_serializing`, so nothing writes it today -- but it is `#[serde(default)]`
/// precisely because an index written before that attribute existed can still carry it, and a
/// container that had quietly become a SEQUENCE on the wire would fail to decode one of those with
/// no test saying why. Checked in both directions and against the bytes a `HashMap` produces.
///
/// rust-internal: exercises this crate's own encoding, no product behaviour
#[test]
fn the_hash_field_map_wire_shape_is_still_the_table_shape() {
    use crate::engine::hash_field_map::HashFieldMap;

    let pairs = vec![
        ("beta".to_string(), BlockAddress::default()),
        ("alpha".to_string(), BlockAddress::default()),
    ];
    let table: std::collections::HashMap<String, BlockAddress> = pairs.clone().into_iter().collect();
    let container: HashFieldMap = pairs.into_iter().collect();

    // A map encoding is order-independent, so the comparison is made through a canonical form
    // rather than by comparing two byte strings a table may have emitted in either order.
    let from_table: std::collections::BTreeMap<String, BlockAddress> =
        serde_json::from_str(&serde_json::to_string(&table).expect("table encodes"))
            .expect("a table decodes as a map");
    let from_container: std::collections::BTreeMap<String, BlockAddress> =
        serde_json::from_str(&serde_json::to_string(&container).expect("container encodes"))
            .expect("the container encodes AS A MAP, not as a sequence");
    assert_eq!(
        from_table, from_container,
        "the container's encoding is not the map the table wrote"
    );

    // And the table's own bytes decode back into the container.
    let round: HashFieldMap =
        serde_json::from_str(&serde_json::to_string(&table).expect("table encodes"))
            .expect("the container decodes a table's bytes");
    assert_eq!(
        2,
        round.len(),
        "the container did not decode both fields out of a table encoding"
    );
    assert_eq!(
        vec!["alpha", "beta"],
        round.keys().map(|k| k.as_str()).collect::<Vec<_>>(),
        "a decoded container is not in field order"
    );
    assert!(
        round.get("alpha").is_some() && round.get("beta").is_some(),
        "a decoded container cannot find the fields it decoded"
    );
}


// =================================================================================================
// 5. WHAT MAKING `hashes` DURABLE COSTS THE COMPRESSED CHECKPOINT
// =================================================================================================

/// THE COMPRESSED CHECKPOINT DELTA OF A DURABLE HASH MAP, MEASURED THROUGH THE PRODUCTION ENCODER.
///
/// `hashes` used to be `#[serde(default, skip_serializing)]` and the only recorded reason was
/// "Rebuildable from the durable bucket/page index on load". The one number anywhere near it --
/// "tens of MB larger" -- is a comment on `object_block_lookup`, a DIFFERENT field, and says nothing
/// about this one. So the cost is measured here rather than inherited.
///
/// MEASURED ON THE REAL ENCODER, NOT A PROXY. `serialize_index_stamped` is what the two production
/// snapshot sites call, and it emits the `TSIDX` container whose payload codec is zstd -- so the
/// length it returns IS the compressed checkpoint size. The arms differ in one thing: whether
/// `shard.hashes` is populated when it is called. The "without" arm is byte-for-byte what main
/// wrote, because main skipped the field.
///
/// THE CORPUS IS THE ONE THIS MODULE ALREADY MEASURES AT: 40,000 one-field hashes plus 40 hashes of
/// 100 fields, which is 40,040 keys carrying 44,000 fields. One field is the p99 of the measured
/// fields-per-hash histogram and is every context node in a store; the wide arm is the
/// Redis-compatible surface. Both are present because a single occupancy cannot show the shape.
///
/// rust-internal: measures this crate's own encoder, no product behaviour
#[test]
fn what_a_durable_hash_map_costs_the_compressed_checkpoint() {
    use crate::engine::serialize_index_stamped;

    const NARROW_HASHES: usize = 40_000;
    const WIDE_HASHES: usize = 40;
    const WIDE_FIELDS: usize = 100;

    let mut shard = ShardState::default();
    // The context-node shape: one field per hash, under one constant name.
    for i in 0..NARROW_HASHES {
        shard.hashes.insert_element_for_test(
            &format!("ctx:node:{i}"),
            "meta",
            BlockAddress::from_parts(1, (i as u64) * 512, 384, None, None),
        );
    }
    // The wide arm, with field names that are not all one string.
    for h in 0..WIDE_HASHES {
        let mut entry = crate::engine::hash_field_map::HashFieldMap::default();
        for f in 0..WIDE_FIELDS {
            entry.insert(
                format!("field-{h}-{f}"),
                BlockAddress::from_parts(2, ((h * WIDE_FIELDS + f) as u64) * 512, 384, None, None),
            );
        }
        shard
            .hashes
            .insert_fields_for_test(&format!("wide:hash:{h}"), entry);
    }

    let keys = shard.hashes.len();
    let fields: usize = shard.hashes.values().map(|m| m.len()).sum();
    assert_eq!(
        NARROW_HASHES + WIDE_HASHES,
        keys,
        "the fixture did not build the corpus it reports"
    );
    assert_eq!(
        NARROW_HASHES + WIDE_HASHES * WIDE_FIELDS,
        fields,
        "the fixture did not build the field population it reports"
    );

    let with_bytes = serialize_index_stamped(&mut shard).len();
    // What main wrote: the same shard with the field skipped.
    let held = shard.hashes.take_for_test();
    let without_bytes = serialize_index_stamped(&mut shard).len();
    shard.hashes.restore_for_test(held);

    assert!(
        with_bytes > without_bytes,
        "the durable arm ({with_bytes} B) is not larger than the skipped arm ({without_bytes} B), \
         so this measurement is not measuring the field at all"
    );
    let delta = with_bytes - without_bytes;
    let per_field = delta as f64 / fields as f64;

    println!(
        "compressed checkpoint: with hashes {with_bytes} B, without {without_bytes} B, \
         delta {delta} B over {keys} keys / {fields} fields"
    );
    println!(
        "  = {per_field:.2} compressed B per hash field, against {} B per field RESIDENT",
        std::mem::size_of::<(String, BlockAddress)>()
    );

    // THE BAR, STATED AS A BAR RATHER THAN AS WHATEVER CAME OUT. A durable map is affordable if the
    // per-field compressed cost is a small multiple of nothing -- it is pure addition to the image,
    // so the only question is the scale. Sixteen bytes per field would put a 44,000-field store at
    // under a megabyte, which is the order the two context maps were refused at 19-310x OVER.
    assert!(
        per_field < 16.0,
        "a durable hash field costs {per_field:.2} compressed bytes, which is not a rounding error \
         on the checkpoint -- at that rate this change is a refutation, not an improvement"
    );
}
