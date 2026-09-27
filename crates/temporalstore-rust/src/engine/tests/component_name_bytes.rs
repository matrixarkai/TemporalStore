// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A COMPONENT NAME COSTS, AND HOW MUCH OF IT IS A NUMBER SPELLED OUT.
//!
//! A component names one element inside a container object. Where the caller supplies the name it
//! is text; where this engine DERIVES it from content it used to spell the derivation out --
//! sixteen hexadecimal characters for a `u64`, two for every byte of a member, up to twenty decimal
//! characters for a timestamp. `crate::component_name` replaced that with one fixed-width spelling
//! over an ascending 64-character alphabet. This module measures what changed.
//!
//! # WHAT IS REPORTED, AND WHY EACH COLUMN IS THERE
//!
//! * THE COMPOSITION. How many of the characters a store's component names hold are spelling a
//!   NUMBER rather than carrying a caller's bytes. Split three ways -- number, member bytes,
//!   caller text -- over a printed denominator, per kind and in total.
//! * BOTH ROUTING RANGES. `0..1023` is `crate::DEFAULT_END_ROUTING_BUCKET`, the range a new store
//!   is built on, and it is the operator's. `0..u32::MAX` is what `TemporalEngine::load_shard`'s
//!   convenience still passes, and it gives every key a bucket of its own BY CONSTRUCTION, so any
//!   per-bucket figure taken on it is an artefact of the range and not a property of the store.
//!   Component names are per ELEMENT and routing is per OBJECT KEY, so the prediction is that the
//!   composition does not move between them at all -- which makes the pair a control on this
//!   module's own instrument as much as a measurement.
//! * TWO CORPUS SIZES. A per-element figure that only holds at one corpus size is not one.
//! * PERCENTILES AND A MAX, NEVER A MEAN ALONE. #1959 published a mean of 1.98 pages a bucket for a
//!   store holding no bucket with two. Every distribution here prints every row of its histogram,
//!   asserts the rows sum to the denominator, and prints the denominator on each line.
//! * PAYLOAD SEPARATED FROM ALLOCATION COUNT. The saving is a PAYLOAD saving. A name is an
//!   `Arc<str>` either way, so the number of allocations does not have to move at all, and the
//!   report says so rather than letting a byte figure imply it.
//! * BOTH ALLOCATOR COLUMNS. `ALLOC_BYTES` charges `layout.size()`; `ALLOC_CHUNK_BYTES` reads
//!   `malloc_usable_size` and charges what the allocator actually handed over. The chunk column
//!   decides. #1969: it is a FLOOR and not an equality -- a 104-byte request read 128 -- so it is
//!   asserted as a floor, as a multiple of sixteen, and as strictly greater than the request.
//! * THE STORE PATH LENGTH, held constant and printed. Allocation bytes move at about six bytes a
//!   character with it.
//! * A CONTROL ON THE EXPLANATION. The mechanism claimed is that the saving comes from DERIVED
//!   component names. `a_workload_with_no_components_saves_nothing` is the workload where that
//!   mechanism predicts no effect, and it is asserted at 0.00%.
//!
//! # THE GATE, AND THE PLANT THAT PROVES IT
//!
//! Every character of the hexadecimal spelling is a legal character of the new one, so a store
//! written at the old spelling does not fail to decode -- it decodes into names no lookup will ever
//! ask for. `a_store_written_at_the_hexadecimal_spelling_is_refused_not_misread` writes such a
//! store and asserts it is refused by version, naming it, BEFORE anything reads a component.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The operator's range: `crate::DEFAULT_END_ROUTING_BUCKET`, what a new store is built on.
/// Read from the constant rather than written down again, so a change to the shipped default
/// cannot leave this module measuring a range nothing runs.
const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

/// The whole keyspace, which `TemporalEngine::load_shard` still passes. An artefact: one bucket per
/// key by construction.
const WIDE_END: u32 = u32::MAX;

/// Container objects seeded, at two sizes.
const SMALL_OBJECTS: usize = 40;
const LARGE_OBJECTS: usize = 400;

/// Elements per container. The measured container shape is p50 100 components per object with a MAX
/// of 100, so the fixture is built to reach exactly that population and asserts that it did.
const COMPONENTS_PER_OBJECT: usize = 100;

/// A zset member's width. Wide enough that the member half of a zset name is the larger half, which
/// is the case the composition figure is most sensitive to.
const MEMBER_BYTES: usize = 20;

// =============================================================================================
// FIXTURE
// =============================================================================================

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "component-name-bytes".to_string(),
        shard_uri: "local://component-name-bytes/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(500) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// A member of a stated width, distinct per (object, element).
fn member_bytes(object: usize, element: usize) -> Vec<u8> {
    let mut bytes = format!("m{object:06}-{element:06}").into_bytes();
    bytes.resize(MEMBER_BYTES, b'.');
    bytes
}

/// EVERY DERIVED COMPONENT KIND, plus the one that is genuinely text.
///
/// Five kinds and one text kind, so the composition is measured over the whole surface rather than
/// over the one arm a change happened to touch. Each object holds `COMPONENTS_PER_OBJECT` elements.
fn seed_containers(engine: &TemporalEngine, objects: usize) {
    let mut commands = Vec::with_capacity(objects * COMPONENTS_PER_OBJECT * 4);
    for object in 0..objects {
        for element in 0..COMPONENTS_PER_OBJECT {
            let member = member_bytes(object, element);
            commands.push(Command::ZSetAdd {
                key: format!("cn-zset-{object:06}"),
                member: member.clone(),
                score: element as f64 + 0.5,
            });
            commands.push(Command::SetAdd {
                key: format!("cn-set-{object:06}"),
                member,
            });
            commands.push(Command::HashSet {
                key: format!("cn-hash-{object:06}"),
                field: format!("field-{element:06}"),
                value: b"v".to_vec(),
            });
            commands.push(Command::ListPush {
                key: format!("cn-list-{object:06}"),
                member: format!("e{element}").into_bytes(),
                left: false,
            });
        }
        commands.push(Command::FeatureAppend {
            key: format!("cn-feature-{object:06}"),
            points: (0..COMPONENTS_PER_OBJECT)
                .map(|element| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + element as u64 * 1_000,
                    value: b"p".to_vec(),
                })
                .collect(),
        });
        for element in 0..COMPONENTS_PER_OBJECT {
            commands.push(Command::ContextWriteExtractedEvent {
                tenant_hash: 41,
                node_hash: object as u64,
                event: Box::new(crate::types::ContextEvent {
                    event_id_hash: (object * COMPONENTS_PER_OBJECT + element) as u64 + 1,
                    event_time_ms: 1_787_270_075_000 + element as u64 * 1_000,
                    ingestion_time_ms: 1_787_270_075_000,
                    kind: 7,
                    event_type: 7,
                    actor_hash: 0,
                    status: 1,
                    valid_until_ms: 0,
                    confidence: 0.9,
                    importance: 0.8,
                    text: "c".to_string(),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                }),
                indexes: crate::types::ContextExtractedEventIndexes {
                    scope_hash: 3001,
                    entity_hashes: Vec::new(),
                    status_hash: 601,
                    source_hash: 701,
                    event_time_bucket_ms: 1_787_270_000_000,
                    disabled_indexes: Vec::new(),
                },
                first_write_only: false,
                cold_storage: false,
            });
        }
    }
    run_batch(engine, commands);
}

/// A workload with NO component at all: the control on the explanation.
fn seed_no_components(engine: &TemporalEngine, objects: usize) {
    run_batch(
        engine,
        (0..objects * COMPONENTS_PER_OBJECT)
            .map(|index| Command::StringSet {
                key: format!("cn-string-{index:08}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
}

fn store_path_length(dir: &std::path::Path) -> usize {
    dir.to_string_lossy().len()
}

// =============================================================================================
// THE CLASSIFIER: WHAT EACH CHARACTER OF A NAME IS DOING
// =============================================================================================

/// What the OLD spelling of this name would have been, and how its characters divide.
///
/// Recovered by PARSING the stored name with the shipped parser and re-rendering it with
/// `component_name::legacy`, rather than by a formula sitting beside the producer. A formula drifts;
/// a parse that fails is a name the shipped code cannot read either, and it is counted as such
/// instead of being silently skipped.
#[derive(Debug, Default, Clone, Copy)]
struct Split {
    /// Names counted.
    names: usize,
    /// Characters the name holds now.
    held: usize,
    /// Characters the OLD spelling held.
    legacy: usize,
    /// Of `legacy`, the characters spelling a NUMBER in hexadecimal.
    legacy_hex_number: usize,
    /// Of `legacy`, the characters spelling a number in DECIMAL.
    legacy_decimal_number: usize,
    /// Of `legacy`, the characters that are hexadecimal of a caller's MEMBER bytes.
    legacy_hex_member: usize,
    /// Of `legacy`, characters that are the caller's own text.
    legacy_text: usize,
    /// The bytes of information the name actually carries.
    information: usize,
}

impl Split {
    fn plus(&mut self, other: &Split) {
        self.names += other.names;
        self.held += other.held;
        self.legacy += other.legacy;
        self.legacy_hex_number += other.legacy_hex_number;
        self.legacy_decimal_number += other.legacy_decimal_number;
        self.legacy_hex_member += other.legacy_hex_member;
        self.legacy_text += other.legacy_text;
        self.information += other.information;
    }

    /// Every character of the old spelling is in exactly one of the four buckets, and that is
    /// checked rather than assumed: a classifier whose parts do not sum to its whole is measuring
    /// something other than what it prints.
    fn assert_partitioned(&self, label: &str) {
        let parts = self.legacy_hex_number
            + self.legacy_decimal_number
            + self.legacy_hex_member
            + self.legacy_text;
        assert_eq!(
            parts, self.legacy,
            "{label}: the four buckets sum to {parts} characters over a denominator of {}",
            self.legacy
        );
    }

    fn spelled_number_share(&self) -> f64 {
        if self.legacy == 0 {
            return 0.0;
        }
        100.0 * (self.legacy_hex_number + self.legacy_decimal_number) as f64 / self.legacy as f64
    }

    /// Everything that is a SPELLING rather than a caller's own text: the numbers, and the 2x on
    /// the member bytes.
    fn spelled_share(&self) -> f64 {
        if self.legacy == 0 {
            return 0.0;
        }
        100.0 * (self.legacy - self.legacy_text) as f64 / self.legacy as f64
    }

    fn saved_share(&self) -> f64 {
        if self.legacy == 0 {
            return 0.0;
        }
        100.0 * (self.legacy as f64 - self.held as f64) / self.legacy as f64
    }
}

/// Classify ONE stored name. `None` when the shipped parser cannot read it, which is itself
/// reported: a name the engine cannot parse is a defect, not a rounding.
fn classify(model_id: &str, component: &str) -> Option<Split> {
    let chars = component.len();
    let mut split = Split {
        names: 1,
        held: chars,
        ..Split::default()
    };
    match model_id {
        "zset" => {
            let (biased, member) = crate::engine::execute_on_shard::parse_zset_component(component)?;
            let legacy_number = crate::component_name::legacy::u64_text(biased);
            let legacy_member = crate::component_name::legacy::bytes_text(&member);
            split.legacy_hex_number = legacy_number.len();
            split.legacy_hex_member = legacy_member.len();
            split.legacy = legacy_number.len() + legacy_member.len();
            split.information = 8 + member.len();
        }
        "set" => {
            let member = crate::engine::execute_on_shard::parse_set_component(component)?;
            let legacy = crate::component_name::legacy::bytes_text(&member);
            split.legacy_hex_member = legacy.len();
            split.legacy = legacy.len();
            split.information = member.len();
        }
        "list" => {
            let sequence = crate::engine::execute_on_shard::parse_list_component(component)?;
            let legacy = crate::component_name::legacy::u64_text(
                (sequence as u64).wrapping_sub(i64::MIN as u64),
            );
            split.legacy_hex_number = legacy.len();
            split.legacy = legacy.len();
            split.information = 8;
        }
        "context_event" => {
            let (timeline, id) = component
                .split_at_checked(crate::component_name::U64_CHARS)
                .filter(|(_, id)| id.len() == crate::component_name::U64_CHARS)?;
            let timeline = crate::component_name::parse_u64(timeline)?;
            let id = crate::component_name::parse_u64(id)?;
            let legacy = crate::component_name::legacy::u64_text(timeline).len()
                + crate::component_name::legacy::u64_text(id).len();
            split.legacy_hex_number = legacy;
            split.legacy = legacy;
            split.information = 16;
        }
        // Every timestamped series, and the three control/context kinds that spell one number.
        "feature" | "context_index" | "context_audit" | "context_child" | "context_summary"
        | "context_compression" | "context_entity" | "control_counter" | "control_change" => {
            let value = crate::component_name::parse_u64(component)?;
            let legacy = crate::component_name::legacy::decimal_text(value);
            split.legacy_decimal_number = legacy.len();
            split.legacy = legacy.len();
            split.information = 8;
        }
        // The caller's own text. Held and legacy are the same characters: nothing to save.
        _ => {
            split.legacy_text = chars;
            split.legacy = chars;
            split.information = chars;
        }
    }
    Some(split)
}

/// A distribution reported as a histogram with percentiles, a MAX and a printed denominator.
#[derive(Debug, Default, Clone)]
struct Hist {
    counts: BTreeMap<usize, usize>,
}

impl Hist {
    fn observe(&mut self, value: usize) {
        *self.counts.entry(value).or_default() += 1;
    }

    fn samples(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn total(&self) -> usize {
        self.counts.iter().map(|(value, count)| value * count).sum()
    }

    fn mean(&self) -> f64 {
        let samples = self.samples();
        if samples == 0 {
            return 0.0;
        }
        self.total() as f64 / samples as f64
    }

    fn max(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    fn min(&self) -> usize {
        self.counts.keys().copied().next().unwrap_or_default()
    }

    /// A value off the histogram, not an interpolation: every answer is one some sample holds.
    fn percentile(&self, fraction: f64) -> usize {
        let samples = self.samples();
        if samples == 0 {
            return 0;
        }
        let target = ((samples as f64) * fraction).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (value, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *value;
            }
        }
        self.max()
    }

    fn line(&self, label: &str) {
        println!(
            "  {label:<44} n {:>7} | mean {:>8.3} min {:>5} p50 {:>5} p90 {:>5} p99 {:>5} \
             MAX {:>5}",
            self.samples(),
            self.mean(),
            self.min(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
        );
    }

    /// Every row, and the rows are asserted to sum to the denominator.
    fn report(&self, label: &str) {
        self.line(label);
        let samples = self.samples();
        let printed: usize = self.counts.values().copied().sum();
        assert_eq!(
            printed, samples,
            "{label}: the histogram's rows sum to {printed} over a denominator of {samples}"
        );
        for (value, count) in &self.counts {
            println!(
                "        {value:>8}: {count:>7} sample(s) ({:>7.3}% of {samples})",
                100.0 * *count as f64 / samples.max(1) as f64
            );
        }
    }
}

/// Everything one seeded store says about its component names.
struct Walk {
    /// The whole store.
    total: Split,
    /// Per model kind.
    by_kind: BTreeMap<String, Split>,
    /// Characters a name holds, over the NAME population.
    held_per_name: Hist,
    /// Characters the old spelling held, over the same population.
    legacy_per_name: Hist,
    /// Component names per container OBJECT, over the OBJECT population.
    components_per_object: Hist,
    /// Characters of component name per container object.
    held_per_object: Hist,
    legacy_per_object: Hist,
    /// Pages with a component whose name the shipped parser could not read. Must be zero.
    unparseable: usize,
    /// Pages with no component at all -- a string, a whole-series page.
    without_component: usize,
    /// Every distinct name, so the allocator table measures the real population.
    names: Vec<(String, String)>,
}

fn walk(engine: &TemporalEngine) -> Walk {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut out = Walk {
        total: Split::default(),
        by_kind: BTreeMap::new(),
        held_per_name: Hist::default(),
        legacy_per_name: Hist::default(),
        components_per_object: Hist::default(),
        held_per_object: Hist::default(),
        legacy_per_object: Hist::default(),
        unparseable: 0,
        without_component: 0,
        names: Vec::new(),
    };
    // Per object, so the per-container figures are over the OBJECT population and not over pages.
    let mut per_object: BTreeMap<(String, String), (usize, usize, usize)> = BTreeMap::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted {
                continue;
            }
            let Some(component) = page.component.as_deref() else {
                out.without_component += 1;
                continue;
            };
            match classify(page.model_id.as_str(), component) {
                None => out.unparseable += 1,
                Some(split) => {
                    out.total.plus(&split);
                    out.by_kind
                        .entry(page.model_id.as_str().to_string())
                        .or_default()
                        .plus(&split);
                    out.held_per_name.observe(split.held);
                    out.legacy_per_name.observe(split.legacy);
                    let entry = per_object
                        .entry((page.model_id.to_string(), page.object_key.to_string()))
                        .or_default();
                    entry.0 += 1;
                    entry.1 += split.held;
                    entry.2 += split.legacy;
                    out.names
                        .push((page.model_id.to_string(), component.to_string()));
                }
            }
        }
    }
    for (_object, (names, held, legacy)) in per_object {
        out.components_per_object.observe(names);
        out.held_per_object.observe(held);
        out.legacy_per_object.observe(legacy);
    }
    out
}

impl Walk {
    fn report(&self, label: &str) {
        println!("--- {label}");
        self.total.assert_partitioned(label);
        assert_eq!(
            self.unparseable, 0,
            "{label}: {} stored component name(s) could not be read back by the shipped parser",
            self.unparseable
        );
        println!(
            "  names {} | held {} chars | old spelling {} chars | saving {:.2}% | \
             information {} B | pages without a component {}",
            self.total.names,
            self.total.held,
            self.total.legacy,
            self.total.saved_share(),
            self.total.information,
            self.without_component,
        );
        println!(
            "  of the OLD spelling: hex-of-number {} ({:.2}%), decimal-of-number {} ({:.2}%), \
             hex-of-member {} ({:.2}%), caller text {} ({:.2}%) -- denominator {} chars",
            self.total.legacy_hex_number,
            100.0 * self.total.legacy_hex_number as f64 / self.total.legacy.max(1) as f64,
            self.total.legacy_decimal_number,
            100.0 * self.total.legacy_decimal_number as f64 / self.total.legacy.max(1) as f64,
            self.total.legacy_hex_member,
            100.0 * self.total.legacy_hex_member as f64 / self.total.legacy.max(1) as f64,
            self.total.legacy_text,
            100.0 * self.total.legacy_text as f64 / self.total.legacy.max(1) as f64,
            self.total.legacy,
        );
        println!(
            "  SPELLING of numbers {:.2}% of the old text | everything that is a spelling \
             (numbers + the 2x on members) {:.2}%",
            self.total.spelled_number_share(),
            self.total.spelled_share(),
        );
        for (kind, split) in &self.by_kind {
            split.assert_partitioned(&format!("{label}/{kind}"));
            println!(
                "    {kind:<22} names {:>7} | held {:>8} | old {:>8} | saving {:>7.2}% | \
                 numbers {:>7.2}% of old | information {:>8} B",
                split.names,
                split.held,
                split.legacy,
                split.saved_share(),
                split.spelled_number_share(),
                split.information,
            );
        }
        self.held_per_name.line("chars per name, now");
        self.legacy_per_name.line("chars per name, old spelling");
        self.components_per_object
            .line("component names per container object");
        self.held_per_object.line("chars per container, now");
        self.legacy_per_object.line("chars per container, old");
    }
}

// =============================================================================================
// 1. THE COMPOSITION, AT BOTH RANGES AND TWO CORPUS SIZES
// =============================================================================================

/// HOW MUCH OF A STORE'S COMPONENT TEXT IS A NUMBER SPELLED OUT, MEASURED.
///
/// The four arms are (operator range, wide range) x (small corpus, large corpus). The composition
/// is a per-ELEMENT property and routing is per OBJECT KEY, so the prediction is that it does not
/// move with the range at all -- and the pair is asserted equal, which makes the wide arm a control
/// on the instrument rather than a second data point.
///
/// THE FIXTURE'S POPULATION IS ASSERTED, not assumed: every container must hold exactly
/// `COMPONENTS_PER_OBJECT` names, at p50 and at MAX, or a percentile printed below is a percentile
/// over a shape nobody claimed.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds four stores of container objects; run by name"]
fn how_much_of_a_component_name_is_a_number_spelled_out() {
    let mut composition: BTreeMap<(usize, u32), f64> = BTreeMap::new();
    let mut path_lengths: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();

    for objects in [SMALL_OBJECTS, LARGE_OBJECTS] {
        for end_routing_bucket in [OPERATOR_END, WIDE_END] {
            let dir = tempfile::tempdir().unwrap();
            path_lengths.insert(store_path_length(dir.path()));
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            seed_containers(&engine, objects);
            let walked = walk(&engine);
            let label = if end_routing_bucket == WIDE_END {
                format!("0..u32::MAX (the whole keyspace, an artefact) at {objects} objects")
            } else {
                format!("0..{end_routing_bucket} (the operator's range) at {objects} objects")
            };
            walked.report(&label);
            assert!(
                walked.total.names > 0,
                "{label}: the fixture produced no component names at all"
            );
            assert_eq!(
                walked.components_per_object.percentile(0.50),
                COMPONENTS_PER_OBJECT,
                "{label}: the fixture claims {COMPONENTS_PER_OBJECT} components per object and its \
                 p50 is {}",
                walked.components_per_object.percentile(0.50)
            );
            assert_eq!(
                walked.components_per_object.max(),
                COMPONENTS_PER_OBJECT,
                "{label}: the fixture claims a MAX of {COMPONENTS_PER_OBJECT} components per \
                 object and its MAX is {}",
                walked.components_per_object.max()
            );
            composition.insert(
                (objects, end_routing_bucket),
                walked.total.spelled_number_share(),
            );
        }
    }

    println!("=== the composition does not move with the range, which is the prediction");
    for ((objects, end), share) in &composition {
        println!("  {objects} objects, 0..{end}: {share:.4}% of the old text spelled a number");
    }
    for objects in [SMALL_OBJECTS, LARGE_OBJECTS] {
        let operator = composition[&(objects, OPERATOR_END)];
        let wide = composition[&(objects, WIDE_END)];
        assert!(
            (operator - wide).abs() < 1e-9,
            "at {objects} objects the composition read {operator:.6}% on the operator's range and \
             {wide:.6}% on the whole keyspace; a component name is per element and routing is per \
             object key, so a difference here means the instrument is reading the range"
        );
    }

    assert_eq!(
        path_lengths.len(),
        1,
        "the arms were held at store paths of differing length {path_lengths:?}; allocation bytes \
         move at about six bytes a character"
    );
    println!(
        "store path length held at {} characters across every arm",
        path_lengths.iter().next().copied().unwrap_or_default()
    );
}

/// THE CONTROL ON THE EXPLANATION: no components, no saving.
///
/// The mechanism claimed is that the saving comes from DERIVED component names. A store of plain
/// string keys has no component on any page, so the mechanism predicts exactly nothing -- and a
/// measurement that showed a saving here would be measuring something else.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds a store of 4,000 string keys; run by name"]
fn a_workload_with_no_components_saves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    seed_no_components(&engine, SMALL_OBJECTS);
    let walked = walk(&engine);
    println!(
        "[control] pages without a component {} | names {} | held {} | old {} | saving {:.2}%",
        walked.without_component,
        walked.total.names,
        walked.total.held,
        walked.total.legacy,
        walked.total.saved_share(),
    );
    assert!(
        walked.without_component >= SMALL_OBJECTS * COMPONENTS_PER_OBJECT,
        "the control seeded {} keys and found only {} pages without a component; it has to reach \
         the population it is a control for",
        SMALL_OBJECTS * COMPONENTS_PER_OBJECT,
        walked.without_component
    );
    assert_eq!(
        walked.total.names, 0,
        "a store of plain string keys holds {} component name(s)",
        walked.total.names
    );
    assert_eq!(
        format!("{:.2}", walked.total.saved_share()),
        "0.00",
        "the control saved {:.4}%, where the mechanism predicts 0.00%",
        walked.total.saved_share()
    );
}

// =============================================================================================
// 0. CAN THE NAME BE AN ORDINAL? IT CANNOT, AND THE ORDINAL ALREADY EXISTS.
// =============================================================================================

/// A COMPONENT NAME CANNOT BECOME AN ORDINAL, AND THE REASON IS NOT THE SPELLING.
///
/// The question is worth asking first because an ordinal dominates any encoding: two bytes against
/// a name of fifty-six. The answer is no, for three reasons that are independent of each other, and
/// the first of them is that THE ORDINAL ALREADY EXISTS.
///
/// # 1. A PER-OBJECT PAGE ORDINAL IS ALREADY HERE, AND IT IS NOT WHAT A COMPONENT ANSWERS
///
/// `BlockAddress::block_id` is a `u32` index INSIDE its object -- its doc says exactly that, and
/// `next_block_index_for_object` hands out the next one by reading the blocks the object already
/// holds rather than from a counter, so it survives a restart with nothing persisted for it. Its
/// non-reuse is already a stated requirement: this store keeps a rewritten block's predecessor
/// live, because the points it holds that were not rewritten still point at it, so an object
/// numbering its new blocks from zero again would have two live blocks claiming the same position
/// and a reload would serve whichever it reached first.
///
/// So the component name does not exist for want of an ordinal. It exists BESIDE one, because the
/// two answer different questions. An ordinal answers "give me page seven of this object". A
/// component answers "which page holds MEMBER M", and `ObjectBlockRefs::position` is only ever
/// asked the second. A caller that holds a member and wants its page cannot compute an ordinal from
/// it; it would need a member-to-ordinal map, and that map is the names again, at the same size, in
/// a second structure.
///
/// # 2. REPLAY REBUILDS AN ELEMENT'S IDENTITY OUT OF THE NAME
///
/// Enumerated by the compiler rather than by grep: retyping the four fields that carry a component
/// name produced 114 error positions in 62 items, and `lifecycle.rs::apply_outcome_item` held
/// eighteen of them. NINE kinds reconstruct identity from the name there -- the set's member, the
/// list's sequence, the zset's score AND member, the series' stored key, the event's timeline key
/// and id, the entity's hash, the counter's bucket, the change's bucket. An ordinal carries none of
/// it.
///
/// The other copy of that identity is inside the PAGE the name points at, and replay deliberately
/// does not read pages -- that is the whole reason a record carries its outcomes. Making replay read
/// them turns installing an index into one page read per element.
///
/// # 3. THE OBJECT ID IS DERIVED FROM THE NAME, AND OBJECT IDS ARE ON DISK
///
/// Every producer computes `stable_block_object_id(shard, kind, key, Some(&component))`, and the
/// result is written into `BlockAddress::object_id`. Two elements of one container get two object
/// ids because their component names differ. Under an ordinal they would differ by assignment order,
/// so the same element written twice would get two different ids -- and the log's own
/// `item_object_id_to_write` drops the id when it can DERIVE it from the fields beside it, which
/// under an ordinal it no longer could.
///
/// # WHAT THIS TEST ACTUALLY CHECKS
///
/// Reasons 1 and 3 are checked directly. Reason 2 is checked where it bites: a recorded outcome for
/// a zset element carries the component and NO member bytes, so the name is the only copy of the
/// member in the record. That is the sentence the ordinal dies on, and it is asserted rather than
/// argued.
///
/// rust-internal: reads the engine's own log and index, no product behaviour
#[test]
fn a_component_name_cannot_become_an_ordinal_because_replay_rebuilds_identity_from_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let member = b"ordinal-question-member".to_vec();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetAdd {
            key: "ord-zset".to_string(),
            member: member.clone(),
            score: 12.5,
        },
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");

    // REASON 2, where it bites. The recorded outcome carries the component and no member bytes.
    let scanned = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("the write-ahead log scans");
    let mut zset_outcomes = 0usize;
    for (_log_id, line) in scanned.iter() {
        let Ok(record) = crate::wal::decode_wal_line(line) else {
            continue;
        };
        for item in record.outcomes.iter().filter(|item| item.kind == "zset") {
            zset_outcomes += 1;
            let component = item
                .component
                .as_deref()
                .expect("a zset outcome names its element");
            let (score_bits, recovered) =
                crate::engine::execute_on_shard::parse_zset_component(component)
                    .expect("the name parses");
            assert_eq!(
                recovered, member,
                "the recorded name held {recovered:?}, not the member that was written"
            );
            assert!(
                item.value.is_none(),
                "the zset outcome carries {} byte(s) of value beside its name. If the member now \
                 travels in the record on its own, reason 2 has gone away and the ordinal is worth \
                 re-asking -- change this assertion deliberately, not to make a run go green.",
                item.value.as_ref().map(|value| value.len()).unwrap_or_default()
            );
            println!(
                "[ordinal] a zset outcome carries name {component:?} ({} chars) and no member \
                 bytes; replay recovers score bits {score_bits} and {} member byte(s) from the name \
                 alone",
                component.len(),
                recovered.len()
            );
        }
    }
    assert!(
        zset_outcomes > 0,
        "the fixture recorded no zset outcome, so this test checked nothing"
    );

    // REASON 1. The per-object ordinal is already here, on the address, and the component sits
    // beside it rather than in place of it.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut zset_pages = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "zset" {
                continue;
            }
            zset_pages += 1;
            let ordinal = page.address.block_id();
            let object_id = page.address.object_id();
            let component = page.component.as_deref().expect("a zset page is named");
            println!(
                "[ordinal] a zset page already carries an ordinal block_id {ordinal:?} and an \
                 object id {object_id:?} BESIDE its {}-character component name",
                component.len()
            );
            assert!(
                ordinal.is_some(),
                "the page carries no block_id, so the claim that a per-object ordinal already \
                 exists is wrong and this test's reasoning has to be redone"
            );
            // REASON 3. The object id is the hash of the NAME, not of the ordinal.
            let derived = crate::engine::hashing::stable_block_object_id(
                1,
                "zset",
                &page.object_key,
                Some(component),
            );
            assert_eq!(
                object_id,
                Some(derived),
                "the page's object id is not the one derived from its component name, so the \
                 derivation this reasoning rests on has moved"
            );
            // And a DIFFERENT name gives a different id, which is what an ordinal would break.
            let other = crate::engine::hashing::stable_block_object_id(
                1,
                "zset",
                &page.object_key,
                Some(&crate::engine::execute_on_shard::zset_component(7, b"other")),
            );
            assert_ne!(
                derived, other,
                "two different component names of one object hashed to the same object id"
            );
        }
    }
    assert_eq!(
        zset_pages, 1,
        "the fixture wrote {zset_pages} zset page(s), not the one this test reasons about"
    );

    // THE CEILING, ours against theirs. Stated rather than assumed, because a ceiling nobody
    // states is a ceiling nobody checks.
    println!(
        "[ordinal] our existing per-object page ordinal is a u32 on the address: {} pages per \
         object. A two-byte ordinal would cap at {}. The measured container shape is p50 \
         {COMPONENTS_PER_OBJECT} pages per object with one observed case at 2,000, so neither \
         ceiling binds -- the ordinal is refuted on identity, not on width.",
        u32::MAX,
        u16::MAX
    );
}

// =============================================================================================
// 1b. THE NAMES THE SERVED INDEX NEVER HOLDS
// =============================================================================================

/// Component names as the recorded OUTCOMES carry them, which is a different population.
///
/// A timestamped page -- a feature point, a context event, a control bucket, a context entity -- is
/// registered in the bucket index WITH NO COMPONENT. `apply_outcome_item` says so in as many words:
/// "the write path registers a timestamped page with no component, one entry per address". So
/// `timestamped_component`'s output never reaches `BlockIndex.component`; it reaches the
/// write-ahead log's outcome item, the index log's item, and `stable_block_object_id`, and nothing
/// else. A walk of the served index cannot see it, and the first version of this module could not
/// either.
fn walk_recorded_outcomes(engine: &TemporalEngine) -> Walk {
    let mut out = Walk {
        total: Split::default(),
        by_kind: BTreeMap::new(),
        held_per_name: Hist::default(),
        legacy_per_name: Hist::default(),
        components_per_object: Hist::default(),
        held_per_object: Hist::default(),
        legacy_per_object: Hist::default(),
        unparseable: 0,
        without_component: 0,
        names: Vec::new(),
    };
    let mut per_object: BTreeMap<(String, String), (usize, usize, usize)> = BTreeMap::new();
    let scanned = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("the write-ahead log scans");
    for (_log_id, line) in scanned.iter() {
        let Ok(record) = crate::wal::decode_wal_line(line) else {
            continue;
        };
        for item in &record.outcomes {
            let Some(component) = item.component.as_deref() else {
                out.without_component += 1;
                continue;
            };
            match classify(&item.kind, component) {
                None => out.unparseable += 1,
                Some(split) => {
                    out.total.plus(&split);
                    out.by_kind.entry(item.kind.clone()).or_default().plus(&split);
                    out.held_per_name.observe(split.held);
                    out.legacy_per_name.observe(split.legacy);
                    let entry = per_object
                        .entry((item.kind.clone(), item.object_key.clone()))
                        .or_default();
                    entry.0 += 1;
                    entry.1 += split.held;
                    entry.2 += split.legacy;
                    out.names.push((item.kind.clone(), component.to_string()));
                }
            }
        }
    }
    for (_object, (names, held, legacy)) in per_object {
        out.components_per_object.observe(names);
        out.held_per_object.observe(held);
        out.legacy_per_object.observe(legacy);
    }
    out
}

/// WHAT `timestamped_component` COSTS, WHERE IT ACTUALLY LIVES.
///
/// The served index holds no component for a timestamped page, so this walks the recorded outcomes
/// instead. Both arms of `timestamped_component` are here -- a feature series takes the lone-key arm
/// and a context event takes the pair -- and so are the three kinds that spelled one number in
/// decimal without going through it at all.
///
/// THE TWO ARMS DID NOT SHARE AN ALPHABET. The pair was thirty-two hexadecimal characters for
/// sixteen bytes; the lone key was a DECIMAL string of up to twenty for eight, and at the thirteen
/// digits a real millisecond timestamp takes it was thirteen. So the pair loses ten characters and
/// the lone key loses two -- and the lone key also stops being variable width, which is what made
/// its order break across a change of digit count. The second is the smaller saving and the larger
/// correction, and the report says both rather than averaging them into one number.
///
/// rust-internal: reads the engine's own write-ahead log, no product behaviour
#[test]
#[ignore = "seeds a store and scans its whole write-ahead log; run by name"]
fn what_a_timestamped_component_costs_where_it_actually_lives() {
    for objects in [SMALL_OBJECTS, LARGE_OBJECTS] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        seed_containers(&engine, objects);

        let indexed = walk(&engine);
        let recorded = walk_recorded_outcomes(&engine);
        println!(
            "=== {objects} container objects, store path {} chars, 0..{OPERATOR_END}",
            store_path_length(dir.path())
        );
        println!(
            "  the SERVED INDEX holds {} component name(s) over kinds {:?}",
            indexed.total.names,
            indexed.by_kind.keys().collect::<Vec<_>>()
        );
        println!(
            "  the RECORDED OUTCOMES hold {} over kinds {:?}",
            recorded.total.names,
            recorded.by_kind.keys().collect::<Vec<_>>()
        );
        recorded.report(&format!("recorded outcomes at {objects} objects"));

        // The timestamped kinds must be present HERE and absent THERE; that asymmetry is the
        // finding, and asserting it is what keeps this module from quietly measuring one population
        // twice if the write path ever starts filing a component on a timestamped page.
        for kind in ["feature", "context_event"] {
            assert!(
                recorded.by_kind.contains_key(kind),
                "the recorded outcomes carry no {kind} component, so this test is not measuring \
                 the arm it claims to; the kinds present were {:?}",
                recorded.by_kind.keys().collect::<Vec<_>>()
            );
            assert!(
                !indexed.by_kind.contains_key(kind),
                "the SERVED INDEX now carries a {kind} component. That is a change to where a \
                 timestamped page is filed, not to this measurement -- correct the module's claim \
                 rather than this assertion."
            );
        }

        // Both arms, priced apart.
        let pair = recorded
            .by_kind
            .get("context_event")
            .copied()
            .expect("just asserted present");
        let lone = recorded
            .by_kind
            .get("feature")
            .copied()
            .expect("just asserted present");
        println!(
            "  THE PAIR ARM (context_event): {} names, {} chars now against {} old, saving {:.2}%; \
             {} B of information in {} chars, so {:.3} characters a byte",
            pair.names,
            pair.held,
            pair.legacy,
            pair.saved_share(),
            pair.information,
            pair.held,
            pair.held as f64 / pair.information.max(1) as f64
        );
        println!(
            "  THE LONE-KEY ARM (feature): {} names, {} chars now against {} old, saving {:.2}%; \
             {} B of information in {} chars, so {:.3} characters a byte",
            lone.names,
            lone.held,
            lone.legacy,
            lone.saved_share(),
            lone.information,
            lone.held,
            lone.held as f64 / lone.information.max(1) as f64
        );
        assert_eq!(
            pair.held / pair.names.max(1),
            2 * crate::component_name::U64_CHARS,
            "the pair arm spells {} characters a name, not two spelled numbers",
            pair.held / pair.names.max(1)
        );
        assert_eq!(
            lone.held / lone.names.max(1),
            crate::component_name::U64_CHARS,
            "the lone-key arm spells {} characters a name, not one spelled number",
            lone.held / lone.names.max(1)
        );
        assert!(
            pair.saved_share() > lone.saved_share(),
            "the pair arm saved {:.2}% and the lone-key arm {:.2}%; the pair replaced hexadecimal \
             and the lone key replaced decimal, so the pair has to be the larger saving",
            pair.saved_share(),
            lone.saved_share()
        );
        assert_eq!(
            recorded.unparseable, 0,
            "{} recorded component name(s) could not be read back by the shipped parser",
            recorded.unparseable
        );
    }
}

// =============================================================================================
// 2. BOTH ALLOCATOR COLUMNS, PAYLOAD SEPARATED FROM ALLOCATION COUNT
// =============================================================================================

/// What allocating one `Arc<str>` per name costs, charged both ways.
#[cfg(feature = "alloc-probe")]
fn arc_counts(names: &[String]) -> (u64, u64, u64) {
    let probe = Probe::start();
    let held: Vec<std::sync::Arc<str>> = names
        .iter()
        .map(|name| std::sync::Arc::from(name.as_str()))
        .collect();
    let counts = probe.stop();
    std::hint::black_box(&held);
    drop(held);
    (counts.alloc_bytes, counts.chunk_bytes, counts.allocs)
}

/// THE PLANTED MARKER. Recovered exactly, or every byte figure below is noise.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_instrument_this_module_uses_recovers_a_planted_megabyte() {
    const PLANTED: usize = 1 << 20;
    let probe = Probe::start();
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let counts = probe.stop();
    std::hint::black_box(&marker);
    drop(marker);
    println!(
        "planted {PLANTED} B: alloc_bytes {} chunk_bytes {} allocs {}",
        counts.alloc_bytes, counts.chunk_bytes, counts.allocs
    );
    assert_eq!(
        counts.alloc_bytes, PLANTED as u64,
        "the request column charged {} B for a planted {PLANTED} B",
        counts.alloc_bytes
    );
    // A FLOOR, not an equality. #1969: a 104-byte request read 128.
    assert!(
        counts.chunk_bytes >= counts.alloc_bytes,
        "the chunk column charged {} B, below the {} B requested; it reads malloc_usable_size and \
         cannot be under the request",
        counts.chunk_bytes,
        counts.alloc_bytes
    );
}

/// WHAT THE SPELLING SAVES IN BYTES, AND WHAT IT DOES NOT SAVE IN ALLOCATIONS.
///
/// The saving is a PAYLOAD saving. A component name is an `Arc<str>` at both spellings, so the
/// number of allocations is the number of names either way and does not move -- which is asserted,
/// because a byte figure printed on its own invites the reader to assume it did.
///
/// Both columns. The chunk column decides: allocator rounding is exactly what a change of payload
/// size may fail to cross, and a saving that does not cross a size class is a saving of nothing.
/// It is checked as a FLOOR on the request, as a multiple of sixteen, and as strictly greater than
/// the request -- #1969 corrected an earlier equality claim here.
///
/// rust-internal: allocates strings, reads no product path
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn what_the_spelling_saves_per_name_and_per_container_on_both_columns() {
    for objects in [SMALL_OBJECTS, LARGE_OBJECTS] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        seed_containers(&engine, objects);
        let walked = walk(&engine);
        assert!(walked.total.names > 0, "no names to charge for");

        // The same population, spelled both ways.
        let now: Vec<String> = walked
            .names
            .iter()
            .map(|(_kind, name)| name.clone())
            .collect();
        let old: Vec<String> = walked
            .names
            .iter()
            .map(|(kind, name)| legacy_spelling(kind, name))
            .collect();
        assert_eq!(now.len(), old.len(), "the two spellings must cover one population");

        let (now_bytes, now_chunk, now_allocs) = arc_counts(&now);
        let (old_bytes, old_chunk, old_allocs) = arc_counts(&old);

        println!("=== {objects} container objects, store path {} chars, 0..{OPERATOR_END}",
            store_path_length(dir.path()));
        println!(
            "  names {} | payload now {} chars, old {} chars",
            now.len(),
            walked.total.held,
            walked.total.legacy
        );
        println!(
            "  ALLOC_BYTES   now {now_bytes} old {old_bytes} -> {:.2}% | per name now {:.2} B \
             old {:.2} B",
            100.0 * (old_bytes as f64 - now_bytes as f64) / old_bytes.max(1) as f64,
            now_bytes as f64 / now.len() as f64,
            old_bytes as f64 / old.len() as f64,
        );
        println!(
            "  CHUNK_BYTES   now {now_chunk} old {old_chunk} -> {:.2}% | per name now {:.2} B \
             old {:.2} B   <- THIS COLUMN DECIDES",
            100.0 * (old_chunk as f64 - now_chunk as f64) / old_chunk.max(1) as f64,
            now_chunk as f64 / now.len() as f64,
            old_chunk as f64 / old.len() as f64,
        );
        println!(
            "  ALLOCATIONS   now {now_allocs} old {old_allocs} -- a payload saving, not a call \
             saving; per container {:.2} names at p50, {} at MAX",
            walked.components_per_object.mean(),
            walked.components_per_object.max()
        );
        println!(
            "  PER CONTAINER chars now p50 {} MAX {} | old p50 {} MAX {}",
            walked.held_per_object.percentile(0.50),
            walked.held_per_object.max(),
            walked.legacy_per_object.percentile(0.50),
            walked.legacy_per_object.max()
        );
        walked.components_per_object.report("component names per container object");

        assert_eq!(
            now_allocs, old_allocs,
            "the two spellings took {now_allocs} and {old_allocs} allocations for one population \
             of {} names; an Arc<str> is one allocation per name at either spelling, so a \
             difference means the two arms are not the same population",
            now.len()
        );
        // A FLOOR on the request, and the rounding is a real cost the request column hides.
        for (label, bytes, chunk) in [
            ("now", now_bytes, now_chunk),
            ("old", old_bytes, old_chunk),
        ] {
            assert!(
                chunk >= bytes,
                "{label}: the chunk column charged {chunk} B, below the {bytes} B requested"
            );
            assert!(
                chunk > bytes,
                "{label}: the chunk column charged exactly the {bytes} B requested over {} \
                 allocations; malloc_usable_size rounds, so an equality here means the column is \
                 not reading it",
                now.len()
            );
            assert_eq!(
                chunk % 16,
                0,
                "{label}: the chunk column charged {chunk} B, which is not a multiple of sixteen"
            );
        }
        assert!(
            now_chunk < old_chunk,
            "the new spelling charged {now_chunk} B of chunk against the old spelling's \
             {old_chunk} B over the same {} names; a spelling that does not cross a size class \
             saves nothing an allocator can see",
            now.len()
        );
    }
}

/// The OLD spelling of a name this store holds, for the arm that charges for it.
fn legacy_spelling(kind: &str, component: &str) -> String {
    match kind {
        "zset" => {
            let (biased, member) = crate::engine::execute_on_shard::parse_zset_component(component)
                .expect("a stored zset name parses");
            format!(
                "{}{}",
                crate::component_name::legacy::u64_text(biased),
                crate::component_name::legacy::bytes_text(&member)
            )
        }
        "set" => crate::component_name::legacy::bytes_text(
            &crate::engine::execute_on_shard::parse_set_component(component)
                .expect("a stored set name parses"),
        ),
        "list" => crate::component_name::legacy::u64_text(
            (crate::engine::execute_on_shard::parse_list_component(component)
                .expect("a stored list name parses") as u64)
                .wrapping_sub(i64::MIN as u64),
        ),
        "context_event" => {
            let (timeline, id) = component
                .split_at_checked(crate::component_name::U64_CHARS)
                .expect("a stored event name is two spelled numbers");
            format!(
                "{}{}",
                crate::component_name::legacy::u64_text(
                    crate::component_name::parse_u64(timeline).expect("the timeline key parses")
                ),
                crate::component_name::legacy::u64_text(
                    crate::component_name::parse_u64(id).expect("the id parses")
                )
            )
        }
        "feature" | "context_index" | "context_audit" | "context_child" | "context_summary"
        | "context_compression" | "context_entity" | "control_counter" | "control_change" => {
            crate::component_name::legacy::decimal_text(
                crate::component_name::parse_u64(component).expect("a stored number name parses"),
            )
        }
        _ => component.to_string(),
    }
}

// =============================================================================================
// 3. THE GATE: A STORE AT THE OLD SPELLING IS REFUSED, NOT MIS-READ
// =============================================================================================

/// A STORE WRITTEN AT THE HEXADECIMAL SPELLING MUST BE REFUSED BEFORE ANYTHING DECODES IT.
///
/// THE MUTANT THIS PLANTS. A real store is seeded and dumped, and the version stamp inside its
/// served index is then set back to what the hexadecimal spelling shipped as -- which is exactly
/// what an existing store on disk looks like to this binary. The load must REFUSE it by version
/// and fall through to replay, rather than decoding it and serving names no lookup will ask for.
///
/// WHY THIS IS THE DANGEROUS CASE. Every character of the old spelling is a legal character of the
/// new one, and an eighteen-character version-2 zset name is a WELL-FORMED version-3 name for a
/// five-byte member -- so `parse_zset_component` does not reject it, it returns a different member.
/// Both halves are asserted below: the pair of spellings collides, AND the version gate refuses
/// before that can matter.
///
/// rust-internal: reads and rewrites the engine's own index file, no product behaviour
#[test]
fn a_store_written_at_the_hexadecimal_spelling_is_refused_not_misread() {
    // HALF ONE: HOW OFTEN AN OLD NAME READS AS A DIFFERENT NEW NAME, MEASURED RATHER THAN
    // ASSUMED.
    //
    // This was first written asserting one hand-picked legacy name mis-parses, and that name is
    // REFUSED: `parse_u64` checks the two pad bits and `parse_bytes` checks the tail bits, so most
    // hexadecimal names are rejected outright. Most is not all. The rate is searched for here and
    // printed, because "usually refused" and "always refused" call for different guards, and the
    // version gate is only load-bearing if the second is false.
    let mut examined = 0usize;
    let mut collided = 0usize;
    let mut witness: Option<(u64, Vec<u8>, String, u64, usize)> = None;
    let mut seed = 0x853C_49E6_748F_EA9Bu64;
    for _ in 0..20_000 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let score = seed;
        let member: Vec<u8> = (0..(1 + (seed % 7) as usize))
            .map(|at| (seed >> ((at % 8) * 8)) as u8)
            .collect();
        let legacy = format!(
            "{}{}",
            crate::component_name::legacy::u64_text(score),
            crate::component_name::legacy::bytes_text(&member),
        );
        examined += 1;
        if let Some((read_score, read_member)) =
            crate::engine::execute_on_shard::parse_zset_component(&legacy)
        {
            collided += 1;
            if (read_score, read_member.len()) != (score, member.len()) && witness.is_none() {
                witness = Some((score, member, legacy, read_score, read_member.len()));
            }
        }
    }
    println!(
        "[collision] {collided} of {examined} hexadecimal zset names ({:.3}%) are ALSO well-formed \
         under the new spelling and read as a different (score, member)",
        100.0 * collided as f64 / examined as f64
    );
    let (score, member, legacy, read_score, read_member_len) = witness.expect(
        "no hexadecimal zset name in 20,000 was accepted by the new parser as a DIFFERENT name. If \
         that is now true for every input the version gate has stopped being the only guard, and \
         this test must be rewritten to say so rather than deleted -- a silently weakened guard is \
         worse than a failing one.",
    );
    println!(
        "[collision] witness: old name {legacy:?} named score {score} and a {}-byte member; the \
         new parser reads it as score {read_score} and a {read_member_len}-byte member",
        member.len()
    );
    assert!(
        collided > 0,
        "the search found no collision at all, which contradicts the witness above"
    );

    // HALF TWO: THE HEADER GATE, CHECKED BEFORE A BYTE OF PAYLOAD IS DECODED.
    //
    // The binary payload is addressed by field name but its STRUCT SHAPE is what the version
    // describes, so the check has to come first. That it does is provable rather than assertable:
    // the payload handed over here is deliberate garbage, so a refusal that names the VERSION can
    // only have happened before the decompression was attempted.
    let current = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    let aged = current - 1;
    let garbage = b"not a zstd frame and not a msgpack map";
    let aged_container = synthetic_binary_container(aged, garbage);
    let error = crate::engine::decode_index_bytes(&aged_container)
        .err()
        .unwrap_or_else(|| {
            panic!(
                "a container stamped at on-disk shape {aged} DECODED under a binary that reads \
                 {current}, from a payload that is not even a compressed frame"
            )
        });
    println!("[gate] aged header: {error}");
    assert!(
        error.contains(&aged.to_string()) && error.contains(&current.to_string()),
        "the refusal must name BOTH versions so an operator knows which store they are holding; it \
         said {error:?}"
    );

    // THE CONTROL ON THAT CLAIM. The same garbage at the CURRENT version must fail for a DIFFERENT
    // reason -- it gets as far as the decompression. Without this the version assertion above would
    // also pass for a reader that refused every container it was handed.
    let current_container = synthetic_binary_container(current, garbage);
    let control = crate::engine::decode_index_bytes(&current_container)
        .err()
        .expect("garbage must not decode at any version");
    println!("[gate] control, current header: {control}");
    assert!(
        !control.contains(&aged.to_string()),
        "the control refusal mentions the aged version {aged}, so the two cases are not being \
         distinguished: {control:?}"
    );
    assert!(
        control.contains("decompress"),
        "the control was expected to fail at the decompression, having passed the version check; it \
         said {control:?}"
    );

    // HALF THREE: A REAL STORE, AGED WHEREVER ITS OWN CODEC KEEPS THE STAMP.
    //
    // A store's first dump takes the version-stamping path, which writes the JSON payload, and that
    // payload carries the stamp INSIDE itself. So the aging is per codec, and which codec was
    // written is read off the file rather than assumed.
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        seed_containers(&engine, 2);
        // Materialise the base snapshot so there is an index file to age.
        engine.unload_shard(1);
    }
    let index_files: Vec<std::path::PathBuf> = std::fs::read_dir(&indexes)
        .expect("the index directory exists")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect();
    assert!(
        !index_files.is_empty(),
        "the fixture wrote no served index into {indexes:?}, so there is nothing to age and this \
         test would pass without testing anything"
    );

    let mut aged_files: Vec<(std::path::PathBuf, u8)> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    for path in &index_files {
        let bytes = std::fs::read(path).expect("the index reads");
        match aged_to(&bytes, aged) {
            None => skipped.push(
                path.file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_default(),
            ),
            Some((stamped, codec)) => {
                assert_ne!(
                    stamped, bytes,
                    "aging {path:?} produced identical bytes, so the stamp was not moved"
                );
                std::fs::write(path, &stamped).expect("the index rewrites");
                aged_files.push((path.clone(), codec));
            }
        }
    }
    println!(
        "[gate] aged {} of {} file(s) in the index directory (codecs {:?}); the {} with no stamp \
         were {skipped:?}",
        aged_files.len(),
        index_files.len(),
        aged_files.iter().map(|(_, codec)| *codec).collect::<Vec<_>>(),
        skipped.len(),
    );
    assert!(
        !aged_files.is_empty(),
        "no file in {indexes:?} carried a version stamp this test knows how to move, so nothing \
         was aged and this half would pass without testing anything"
    );

    // Whichever codec was written, the aged file must not come back claiming to be current: either
    // the header check refuses it outright, or the decode hands back a shard whose own stamp is the
    // aged one -- which is exactly what `load_index_inner` refuses on.
    for (path, codec) in &aged_files {
        let bytes = std::fs::read(path).expect("the aged index reads");
        match crate::engine::decode_index_bytes(&bytes) {
            Err(error) => {
                println!("[gate] codec {codec}: refused before the decode -- {error}");
                assert!(
                    error.contains(&aged.to_string()) && error.contains(&current.to_string()),
                    "the refusal must name both versions; it said {error:?}"
                );
            }
            Ok(shard) => {
                println!(
                    "[gate] codec {codec}: decoded, and its own stamp reads {} against this \
                     binary's {current} -- which is what the load path refuses on",
                    shard.index_format_version
                );
                assert_eq!(
                    shard.index_format_version, aged,
                    "the aged index came back stamped {} rather than {aged}; a stamp that survives \
                     as current is a store that will be trusted",
                    shard.index_format_version
                );
                assert!(
                    shard.index_format_version < current,
                    "a stamp of {} is not below {current}, so `load_index_inner` would accept it",
                    shard.index_format_version
                );
            }
        }
    }

    // And the shard still comes up, because a refusal falls through to replay rather than failing.
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let walked = walk(&engine);
    println!(
        "[gate] after refusing the aged index the shard rebuilt {} component name(s), {} of them \
         unreadable",
        walked.total.names, walked.unparseable
    );
    assert_eq!(
        walked.unparseable, 0,
        "the rebuild produced {} component name(s) the shipped parser cannot read",
        walked.unparseable
    );
}

/// A served-index binary container carrying `version` and whatever payload is handed in.
///
/// Built here rather than taken from a real store, because the property under test is that the
/// version is checked BEFORE the payload -- which can only be shown with a payload that could never
/// decode.
fn synthetic_binary_container(version: u32, payload: &[u8]) -> Vec<u8> {
    const MAGIC: &[u8] = b"TSIDX\x01";
    // Codec 2 is the binary payload: the only one that carries its version in the header.
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + 4 + payload.len());
    out.extend_from_slice(MAGIC);
    out.push(2);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// The same bytes with their version stamp moved to `version`, and the codec they carried.
///
/// `None` for a file that carries no stamp at all, which is what makes the caller's "aged at least
/// one" assertion mean something. The two codecs keep the stamp in different places and both are
/// handled: guessing one would age four bytes of a compressed frame and read the resulting
/// corruption as a refusal.
fn aged_to(bytes: &[u8], version: u32) -> Option<(Vec<u8>, u8)> {
    const MAGIC: &[u8] = b"TSIDX\x01";
    if !bytes.starts_with(MAGIC) {
        return None;
    }
    let codec = *bytes.get(MAGIC.len())?;
    let payload = &bytes[MAGIC.len() + 1..];
    match codec {
        // Binary: the stamp is the next four bytes.
        2 => {
            if payload.len() < 4 {
                return None;
            }
            let mut out = bytes.to_vec();
            out[MAGIC.len() + 1..MAGIC.len() + 5].copy_from_slice(&version.to_be_bytes());
            Some((out, codec))
        }
        // JSON: the stamp is a field inside the compressed payload.
        1 => {
            let json = zstd::stream::decode_all(payload).ok()?;
            let mut value: serde_json::Value = serde_json::from_slice(&json).ok()?;
            value
                .as_object_mut()?
                .insert("index_format_version".to_string(), version.into());
            let rewritten = serde_json::to_vec(&value).ok()?;
            let compressed = zstd::stream::encode_all(rewritten.as_slice(), 3).ok()?;
            let mut out = Vec::with_capacity(MAGIC.len() + 1 + compressed.len());
            out.extend_from_slice(MAGIC);
            out.push(codec);
            out.extend_from_slice(&compressed);
            Some((out, codec))
        }
        _ => None,
    }
}

// =============================================================================================
// 3a. THE LIVE PATH, DRIVEN: A STORE AT THE OLD SPELLING STILL SERVES, COMPLETE AND CORRECT
// =============================================================================================

/// A STORE WRITTEN AT THE OLD SPELLING MUST COME BACK COMPLETE, NOT MERELY COME BACK.
///
/// THERE IS ONE VERSION. The refusal is not a migration and there is no second decoder: a stale
/// index is treated exactly as an ABSENT one, and the engine rebuilds from the log. That is the
/// whole compatibility story, and it is only a safe one if the rebuild actually produces the store.
///
/// SO THIS ASSERTS THE CONTENTS, NOT THE EXIT CODE. A store that loads EMPTY and a store that loads
/// CORRECTLY return the same status from `load_shard`, and every `assert!(response.status.ok)` in the
/// suite is blind to the difference. Every value written here is read back and compared, and the
/// counts are asserted first so that a fixture which silently wrote nothing cannot pass.
///
/// The aging is real rather than simulated: the store is written, dumped, its version stamp moved
/// back to what the hexadecimal spelling shipped as, and then reopened by this binary. That is
/// exactly what an existing store on disk looks like here.
///
/// rust-internal: drives the engine's own load path, no external surface
#[test]
fn a_store_at_the_old_spelling_still_serves_every_value_after_the_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");

    // A known workload across every kind whose component name this change respells, plus a string
    // (no component) and a hash field (a caller's text) as the two controls.
    let members: Vec<Vec<u8>> = (0..12)
        .map(|element| member_bytes(0, element))
        .collect();
    let write = |engine: &TemporalEngine, command: Command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "the fixture write failed: {response:?}");
    };

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        write(&engine, Command::StringSet {
            key: "live-string".to_string(),
            value: b"string-value".to_vec(),
        });
        for (element, member) in members.iter().enumerate() {
            write(&engine, Command::ZSetAdd {
                key: "live-zset".to_string(),
                member: member.clone(),
                score: element as f64 + 0.25,
            });
            write(&engine, Command::SetAdd {
                key: "live-set".to_string(),
                member: member.clone(),
            });
            write(&engine, Command::ListPush {
                key: "live-list".to_string(),
                member: format!("element-{element}").into_bytes(),
                left: false,
            });
            write(&engine, Command::HashSet {
                key: "live-hash".to_string(),
                field: format!("field-{element}"),
                value: format!("hash-value-{element}").into_bytes(),
            });
        }
        write(&engine, Command::FeatureAppend {
            key: "live-feature".to_string(),
            points: (0..12)
                .map(|element| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + element as u64 * 1_000,
                    value: format!("point-{element}").into_bytes(),
                })
                .collect(),
        });
        // Materialise the base snapshot, so there is an index to age.
        engine.unload_shard(1);
    }

    // AGE IT. Every file that carries a stamp, and at least one must.
    let current = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    let aged_version = current - 1;
    let mut aged = 0usize;
    for entry in std::fs::read_dir(&indexes).expect("the index directory exists") {
        let path = entry.expect("a directory entry").path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).expect("the index reads");
        if let Some((stamped, codec)) = aged_to(&bytes, aged_version) {
            assert_ne!(stamped, bytes, "aging {path:?} moved nothing");
            std::fs::write(&path, &stamped).expect("the index rewrites");
            println!("[live] aged {:?} (codec {codec}) to shape {aged_version}", path.file_name());
            aged += 1;
        }
    }
    assert!(
        aged > 0,
        "nothing in {indexes:?} carried a version stamp, so the store was never aged and this test \
         would pass on a store that was current all along"
    );

    // REOPEN. The stale index is refused and the shard rebuilds from the log.
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // COUNTS FIRST. A rebuild that produced nothing must not reach the content comparisons and
    // pass them vacuously.
    let read = |command: Command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "a read failed after the rebuild: {response:?}");
        response.response
    };

    let zset_members = match read(Command::ZSetRange {
        key: "live-zset".to_string(),
        start: 0,
        stop: -1,
        rev: false,
    }) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a zset range answered {other:?}"),
    };
    println!("[live] the zset came back with {} entry(ies)", zset_members.len());
    assert!(
        zset_members.len() >= members.len(),
        "the zset held {} members before the rebuild and {} after; a store that loads EMPTY and a \
         store that loads CORRECTLY return the same status, which is why this counts first",
        members.len(),
        zset_members.len()
    );

    let set_members = match read(Command::SetMembers {
        key: "live-set".to_string(),
    }) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    println!("[live] the set came back with {} member(s)", set_members.len());
    assert_eq!(
        set_members.len(),
        members.len(),
        "the set held {} members before the rebuild and {} after",
        members.len(),
        set_members.len()
    );

    // CONTENTS. Every value, compared.
    for (element, member) in members.iter().enumerate() {
        match read(Command::ZSetScore {
            key: "live-zset".to_string(),
            member: member.clone(),
        }) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                let text = String::from_utf8_lossy(&bytes).to_string();
                let score: f64 = text.parse().unwrap_or_else(|_| {
                    panic!("a zset score came back as {text:?}, which is not a number")
                });
                assert!(
                    (score - (element as f64 + 0.25)).abs() < 1e-9,
                    "member {element} came back with score {score}, not {}",
                    element as f64 + 0.25
                );
            }
            other => panic!(
                "member {element} of the zset is GONE after the rebuild: {other:?}. Its component \
                 name was the only copy of its identity in the log, so losing it here is the \
                 failure this whole change has to not cause."
            ),
        }
        assert!(
            set_members.iter().any(|held| held == member),
            "member {element} of the set is gone after the rebuild"
        );
        let expected = format!("hash-value-{element}").into_bytes();
        assert!(
            matches!(
                read(Command::HashGet {
                    key: "live-hash".to_string(),
                    field: format!("field-{element}"),
                }),
                crate::types::CommandResponse::Bytes { value: Some(ref got) } if *got == expected
            ),
            "field {element} of the hash did not come back as it was written"
        );
    }

    // The two controls: a component-free record, and the series whose name lives only in the log.
    assert!(
        matches!(
            read(Command::StringGet { key: "live-string".to_string() }),
            crate::types::CommandResponse::Bytes { value: Some(ref got) } if got == b"string-value"
        ),
        "the string, which has no component at all, did not survive the rebuild"
    );
    let list = match read(Command::ListRange {
        key: "live-list".to_string(),
        start: 0,
        stop: -1,
    }) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a list range answered {other:?}"),
    };
    assert_eq!(
        list.len(),
        members.len(),
        "the list came back with {} element(s), not {}",
        list.len(),
        members.len()
    );
    for (element, got) in list.iter().enumerate() {
        assert_eq!(
            got.as_slice(),
            format!("element-{element}").as_bytes(),
            "list element {element} came back as {:?}",
            String::from_utf8_lossy(got)
        );
    }
    let points = match read(Command::FeatureQuery {
        key: "live-feature".to_string(),
        start_ms: 0,
        end_ms: u64::MAX,
        count: None,
    }) {
        crate::types::CommandResponse::FeaturePoints { points } => points,
        other => panic!("a feature range answered {other:?}"),
    };
    assert_eq!(
        points.len(),
        12,
        "the series came back with {} point(s), not 12 -- and its component name lives ONLY in the \
         log, so this is the arm the rebuild is most exposed on",
        points.len()
    );
    for (element, point) in points.iter().enumerate() {
        assert_eq!(
            point.value,
            format!("point-{element}").into_bytes(),
            "point {element} came back with the wrong value"
        );
    }

    // And nothing in the rebuilt index is a name the shipped parser cannot read.
    let walked = walk(&engine);
    assert_eq!(
        walked.unparseable, 0,
        "the rebuilt index holds {} component name(s) the shipped parser cannot read",
        walked.unparseable
    );
    println!(
        "[live] after refusing an index written at shape {aged_version}, the rebuild served \
         {} zset member(s), {} list element(s), {} series point(s), the string and every hash \
         field -- all compared, not counted",
        members.len(),
        list.len(),
        points.len()
    );
}

// =============================================================================================
// 3b. THE DOORS THAT ARE NOT CLOSED
// =============================================================================================

/// THREE DURABLE FORMATS CARRY A COMPONENT NAME, AND ONLY ONE OF THEM CAN REFUSE A STALE ONE.
///
/// The served index can: `SHARD_INDEX_FORMAT_VERSION` is stamped in the binary container's header
/// and checked before the payload is decoded, and stamped inside the JSON payload and checked by
/// `load_index_inner`. That is the door the spelling change walks through, and it is shut.
///
/// THE OTHER TWO ARE NOT, and this states why in a form that fails if either changes.
///
/// * THE WRITE-AHEAD LOG. `WriteAheadLogRecordMetadata::version` is skipped while it equals the
///   current version and DEFAULTS TO THE CURRENT VERSION when absent. So a record written by an
///   older build -- which omitted the field because it was current then -- reads back as current.
///   The field cannot identify a legacy record even in principle, and nothing in the crate compares
///   it to anything. Closing this door means always writing the field and defaulting it to a
///   sentinel, which changes the bytes of every record.
/// * THE INDEX LOG. Its container byte carries a codec and a record SHAPE -- whole, delta, anchor --
///   and no version. The decoder reads the codec and discards the shape.
///
/// WHY THAT MATTERS HERE RATHER THAN IN GENERAL. Refusing the served index falls through to log
/// replay, so the store that most needs these doors is exactly the store the served-index gate has
/// just redirected into them. A legacy record installs a hexadecimal component beside the new
/// spelling of the same element, and then the tombstone for that element is written under the new
/// name and does not find the old one.
///
/// THIS GUARD IS NOT A FIX. It is the statement of what is true so that the next change cannot
/// assume otherwise, and it FAILS if the write-ahead log's default stops being "current" or if the
/// index log's shape nibble grows a version -- either of which would mean a door has been closed and
/// this comment has gone stale.
///
/// rust-internal: reads two durable formats' own declarations, no product behaviour
#[test]
fn the_three_durable_carriers_of_a_component_name_and_which_can_refuse_a_stale_one() {
    // 1. THE SERVED INDEX: shut. Proved by the plant in
    //    `a_store_written_at_the_hexadecimal_spelling_is_refused_not_misread`, and named here so a
    //    reader of this list is not left to find it.
    println!(
        "[carriers] served index: stamped at {} and refused on a mismatch (see \
         a_store_written_at_the_hexadecimal_spelling_is_refused_not_misread)",
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );

    // 2. THE WRITE-AHEAD LOG: open, and open in a way a bump cannot fix on its own. A record whose
    //    metadata omits the version reads back AS THE CURRENT VERSION, which is what an older
    //    build's record looks like.
    let without_version = serde_json::json!({
        "t": 1_787_270_070_000u64,
    });
    let metadata: crate::wal::WriteAheadLogRecordMetadata =
        serde_json::from_value(without_version).expect("metadata with no version must decode");
    assert_eq!(
        metadata.version,
        crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION,
        "a write-ahead log record that states no version read back as {}, not as the current {}. \
         If that has changed, the write-ahead log can now identify a legacy record and the door \
         this guard describes has been closed -- update the description rather than this assertion.",
        metadata.version,
        crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION
    );
    println!(
        "[carriers] write-ahead log: a record stating NO version reads as {}, the current one, so a \
         legacy record is indistinguishable from a current one -- OPEN",
        metadata.version
    );

    // 3. THE INDEX LOG: open. Its container nibble is a record SHAPE, and the three shapes are a
    //    closed set with no version among them.
    let shapes = [
        ("whole", crate::index_log::INDEX_LOG_SHAPE_WHOLE),
        ("delta", crate::index_log::INDEX_LOG_SHAPE_DELTA),
        ("anchor", crate::index_log::INDEX_LOG_SHAPE_ANCHOR),
    ];
    let distinct: std::collections::BTreeSet<u8> = shapes.iter().map(|(_, id)| *id).collect();
    assert_eq!(
        distinct.len(),
        shapes.len(),
        "the index log's shape ids collide: {shapes:?}"
    );
    assert_eq!(
        distinct,
        [0u8, 1, 2].into_iter().collect::<std::collections::BTreeSet<u8>>(),
        "the index log's shape ids are {distinct:?}, not the three this guard describes. A fourth \
         shape may be the version this door needs -- say so here rather than leaving the list wrong."
    );
    println!(
        "[carriers] index log: container nibble carries a codec and one of {} record shapes {:?}, \
         no version -- OPEN",
        shapes.len(),
        shapes.iter().map(|(name, _)| *name).collect::<Vec<_>>()
    );
    println!(
        "[carriers] 1 of 3 durable carriers of a component name can refuse a stale spelling"
    );
}

// =============================================================================================
// 4. THE ORDER, WHICH IS LOAD-BEARING
// =============================================================================================

/// THE NEW SPELLING SORTS EXACTLY AS THE HEXADECIMAL ONE DID.
///
/// `ObjectBlockRefs::position` binary-searches `by_component`, and `zset_component`'s own doc
/// comment states that lexical order of the name is (score, member) order. Both claims are
/// properties of the SPELLING, so changing it can reorder a container's members silently.
///
/// Checked as an agreement between the two spellings over a randomised population rather than as a
/// property of one of them: the old spelling is the behaviour that shipped, so it is what the new
/// one has to match. Variable-length members are included deliberately -- equal lengths cannot
/// expose a padding mistake.
///
/// THE ONE PLACE THEY DISAGREE, ON PURPOSE. The series component was DECIMAL and variable width, so
/// its old order broke across a change of digit count. That is asserted as a disagreement, and as
/// the new spelling agreeing with the NUMBERS where the old one did not.
///
/// rust-internal: pure spelling, no product path
#[test]
fn the_new_spelling_sorts_exactly_as_the_hexadecimal_one_did() {
    // A deterministic spread: a mixed multiplier over the whole u64 range, and members of every
    // length from empty to seventeen bytes.
    let mut scores: Vec<u64> = Vec::new();
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    for _ in 0..600 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        scores.push(seed);
    }
    scores.extend([0, 1, u64::MAX, u64::MAX - 1, 1 << 63, (1 << 63) - 1]);

    let mut members: Vec<Vec<u8>> = Vec::new();
    for length in 0..18usize {
        for variant in 0..6usize {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            members.push(
                (0..length)
                    .map(|at| (seed >> ((at % 8) * 8)) as u8 ^ variant as u8)
                    .collect(),
            );
        }
    }

    // 1. A lone u64: the new spelling agrees with the old AND with the numbers.
    let mut pairs: Vec<(u64, String, String)> = scores
        .iter()
        .map(|score| {
            (
                *score,
                crate::component_name::legacy::u64_text(*score),
                crate::component_name::u64_text(*score),
            )
        })
        .collect();
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    for window in pairs.windows(2) {
        let (left, right) = (&window[0], &window[1]);
        assert!(
            left.1 < right.1,
            "the OLD spelling put {:?} at or after {:?} for {} < {}",
            left.1,
            right.1,
            left.0,
            right.0
        );
        assert!(
            left.2 < right.2,
            "the NEW spelling put {:?} at or after {:?} for {} < {}",
            left.2,
            right.2,
            left.0,
            right.0
        );
    }
    println!(
        "[order] {} u64 values: both spellings sort in the numbers' order, old {} chars each, \
         new {} chars each",
        pairs.len(),
        pairs[0].1.len(),
        pairs[0].2.len()
    );

    // 2. A byte string: the new spelling agrees with the old AND with the bytes, including across
    //    lengths.
    let mut byte_pairs: Vec<(Vec<u8>, String, String)> = members
        .iter()
        .map(|member| {
            (
                member.clone(),
                crate::component_name::legacy::bytes_text(member),
                crate::component_name::bytes_text(member),
            )
        })
        .collect();
    byte_pairs.sort_by(|left, right| left.0.cmp(&right.0));
    byte_pairs.dedup_by(|left, right| left.0 == right.0);
    let mut checked = 0usize;
    for window in byte_pairs.windows(2) {
        let (left, right) = (&window[0], &window[1]);
        assert!(
            left.1 < right.1,
            "the OLD spelling put {:?} at or after {:?} for {:?} < {:?}",
            left.1,
            right.1,
            left.0,
            right.0
        );
        assert!(
            left.2 < right.2,
            "the NEW spelling put {:?} at or after {:?} for {:?} < {:?}; a member whose name sorts \
             out of order reorders its container",
            left.2,
            right.2,
            left.0,
            right.0
        );
        checked += 1;
    }
    assert!(
        checked > 90,
        "only {checked} adjacent pairs of byte strings were compared; a randomised order check \
         over a handful of inputs is not one"
    );
    println!("[order] {checked} adjacent member pairs across lengths 0..18, both spellings agree");

    // 3. A whole zset component: (score, member) order, which is what the producer's doc claims.
    let mut composed: Vec<((u64, Vec<u8>), String)> = Vec::new();
    for score in scores.iter().take(40) {
        for member in members.iter().take(40) {
            composed.push((
                (*score, member.clone()),
                crate::engine::execute_on_shard::zset_component(*score, member),
            ));
        }
    }
    composed.sort_by(|left, right| left.0.cmp(&right.0));
    composed.dedup_by(|left, right| left.0 == right.0);
    for window in composed.windows(2) {
        assert!(
            window[0].1 < window[1].1,
            "a zset component sorted out of (score, member) order: {:?} then {:?}",
            window[0].1,
            window[1].1
        );
    }
    println!(
        "[order] {} zset components sort in (score, member) order",
        composed.len()
    );

    // 4. THE DISAGREEMENT, on purpose. The series component was decimal and variable width.
    let below = 9_999_999_999_999u64;
    let above = 10_000_000_000_000u64;
    assert!(below < above);
    assert!(
        crate::component_name::legacy::decimal_text(below)
            > crate::component_name::legacy::decimal_text(above),
        "the decimal spelling was expected to sort {below} AFTER {above}; that inversion is the \
         defect the fixed width removes, and if it is gone this assertion is the thing to correct"
    );
    assert!(
        crate::component_name::u64_text(below) < crate::component_name::u64_text(above),
        "the new spelling must sort {below} before {above}"
    );
    println!(
        "[order] the decimal series spelling inverted {below} against {above}; the fixed width \
         does not"
    );
}

/// THE PROTO REBUILDS THE COMPONENT THE PRODUCER SPELLED.
///
/// `wal_proto::numeric_component_text` mirrors `packed_pages::timestamped_component`: it rebuilds a
/// component from the numeric fields the wire carries. Two functions spelling one thing is a pair
/// that drifts, and a drift here is a component that comes back DIFFERENT from the one written --
/// which changes the derived object id and files the page under a name nothing asks for.
///
/// Both arms, and the round trip through the parser as well, so the three agree rather than two.
///
/// rust-internal: pure spelling, no product path
#[test]
fn the_proto_rebuilds_the_component_the_producer_spelled() {
    let cases: [(u64, Option<u64>); 8] = [
        (0, None),
        (0, Some(0)),
        (1_787_270_070_000, None),
        (1_787_270_070_000, Some(445)),
        (u64::MAX, None),
        (u64::MAX, Some(u64::MAX)),
        (1, Some(u64::MAX)),
        (u64::MAX, Some(1)),
    ];
    for (stored_key, identity) in cases {
        let produced =
            crate::engine::packed_pages::timestamped_component(stored_key, identity);
        let rebuilt = crate::wal_proto::numeric_component_text_for_test(stored_key, identity)
            .expect("a stored key always rebuilds a component");
        assert_eq!(
            produced, rebuilt,
            "the producer spelled ({stored_key}, {identity:?}) as {produced:?} and the proto \
             rebuilt it as {rebuilt:?}"
        );
        // And the width is what the parsers demand.
        let expected = match identity {
            Some(_) => 2 * crate::component_name::U64_CHARS,
            None => crate::component_name::U64_CHARS,
        };
        assert_eq!(
            produced.len(),
            expected,
            "({stored_key}, {identity:?}) spelled as {} characters, where its parsers demand {expected}",
            produced.len()
        );
    }
    println!("[proto] {} (stored key, identity) pairs spell identically on both sides", cases.len());
}
