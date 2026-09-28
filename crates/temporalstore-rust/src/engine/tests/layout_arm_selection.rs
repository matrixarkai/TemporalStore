// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT WOULD A LAYOUT STATE SELECT, IF IT SELECTED A REPRESENTATION?
//!
//! `BucketLayoutState` names five shapes and every reader of it is a report string or a counter.
//! The question this module answers is whether making it select a REPRESENTATION -- one arm
//! keeping a page inline, another keying its pages on a per-bucket object list so the entry can
//! carry a one-byte ordinal instead of a name -- would be worth the two representations.
//!
//! IT WOULD NOT, AND THE REASON IS ONE NUMBER: PAGES PER OBJECT IS 1.
//!
//! An object list amortises. It costs a row per object and pays for itself by letting every page
//! of that object carry an ordinal instead of a name. The saving is therefore
//! `(name - ordinal) * pages_per_object - row_cost`, and everything depends on
//! `pages_per_object` being greater than one. In this engine it is not, and not by accident: a
//! page's object identity is `stable_block_object_id(shard_id, kind, object_key, component)` --
//! THE COMPONENT IS IN THE HASH -- so a hash field, a set member, a list element and a sorted-set
//! member are each an object of their own holding exactly one page.
//!
//! That is why `MultiBlockObject` -- the one arm the proposal is for, `(1 object, many pages)` --
//! is not merely rare. It is UNREACHABLE for every component-bearing kind, because a bucket
//! holding many pages of one key holds many OBJECTS of one key, which classifies `MultiObject`.
//!
//! THE POSITIVE CONTROL IS WHAT MAKES THAT A MEASUREMENT RATHER THAN AN EMPTY FIXTURE. A
//! component-free kind CAN hold several pages under one object id, and the series kind does: a
//! timestamped series is appended under one `(kind, key, None)` identity and spills into more than
//! one block. If this module reported "no multi-page objects" without reaching one, the report
//! would be indistinguishable from a fixture that could not produce one -- so the series arm is
//! seeded, its pages-per-object MAX is asserted above one, and the refutation is stated only for
//! the kinds where the ceiling is structural.
//!
//! THE NULL CONTROL, AT 0.00%. Grouping pages by `object_id` (component IN the hash) and grouping
//! them by `(model_id, object_key)` (component OUT) must give byte-identical histograms for any
//! component-free kind, and must diverge for containers. The routed-string arm is that control: it
//! predicts exactly no difference, and a difference there would mean the two groupings are not
//! measuring what they are named for.
//!
//! EVERY ROW CARRIES ITS OWN DENOMINATOR and the arm rows are asserted to sum to the population,
//! because a run that wrote nothing reports a beautifully simple store. Percentiles and a MAX,
//! never a mean: #1959 published 1.98 pages a bucket over a store holding not one bucket with two.
//!
//! rust-internal: reads the engine's own bucket index and its own classifier; no product behaviour

use super::*;
use crate::engine::state::BucketLayoutState;
use crate::engine::storage_bucket_internals::classify_bucket_layout;
use std::collections::BTreeMap;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973).
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses by default.
const WIDE_END: u32 = u32::MAX;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// Every arm this module quotes a figure for.
const EVERY_ARM: [&str; 5] = [
    "empty",
    "single_object_no_page",
    "single_page_object",
    "multi_page_object",
    "multi_object",
];

fn arm_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object_no_page",
        BucketLayoutState::SingleBlockObject => "single_page_object",
        BucketLayoutState::MultiBlockObject => "multi_page_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

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
        table_name: "layout-arms".to_string(),
        shard_uri: "local://layout-arms/1".to_string(),
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
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn percentile(sorted: &[usize], fraction: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

/// THE REAL KIND MIX, in ONE store, so the arms are measured over a population a deployment could
/// actually have rather than over three separate single-kind stores.
///
/// Returns the number of pages each kind is expected to contribute, so the denominator is checked
/// against the fixture and not against itself.
struct SeedCounts {
    strings: usize,
    container_pages: usize,
    series_keys: usize,
}

fn seed_mixed(engine: &TemporalEngine, records: usize) -> SeedCounts {
    // Half plain strings: one page each, no component.
    let strings = records / 2;
    run_batch(
        engine,
        (0..strings)
            .map(|i| Command::StringSet {
                key: format!("mix-s-{i:06}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );

    // Two fifths containers, across all four container kinds: one page PER ELEMENT, each element a
    // component and therefore an object of its own.
    let container_pages = records * 2 / 5;
    let members = 100usize;
    let container_keys = container_pages / members;
    let mut commands = Vec::with_capacity(container_pages);
    for k in 0..container_keys {
        for m in 0..members {
            commands.push(match k % 4 {
                0 => Command::HashSet {
                    key: format!("mix-h-{k:06}"),
                    field: format!("f{m}"),
                    value: vec![b'v'; 32],
                },
                1 => Command::SetAdd {
                    key: format!("mix-t-{k:06}"),
                    member: format!("m{m}").into_bytes(),
                },
                2 => Command::ZSetAdd {
                    key: format!("mix-z-{k:06}"),
                    member: format!("m{m}").into_bytes(),
                    score: m as f64,
                },
                _ => Command::ListPush {
                    key: format!("mix-l-{k:06}"),
                    member: format!("m{m}").into_bytes(),
                    left: false,
                },
            });
        }
    }
    run_batch(engine, commands);

    // THE POSITIVE CONTROL. A timestamped series is appended under ONE (kind, key, None) identity
    // and spills into more than one block, so it is the one kind in this engine that can reach a
    // pages-per-object above one. Without it a null result here would be a fixture artefact.
    let series_keys = 20usize;
    let series_points = 2_000usize;
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("mix-f-{k:06}"),
                    points,
                },
            });
            assert!(response.status.ok, "series seed must ack: {:?}", response.status);
        }
    }

    SeedCounts { strings, container_pages, series_keys }
}

/// Everything read off one store: the arm population by bucket AND by page, and pages-per-object
/// under the two groupings whose difference is the whole question.
#[derive(Default)]
struct Population {
    /// arm -> (buckets in it, pages held by those buckets)
    arms: BTreeMap<&'static str, (usize, usize)>,
    pages_per_bucket: Vec<usize>,
    objects_per_bucket: Vec<usize>,
    /// Pages grouped by the engine's own object identity -- component IN the hash.
    by_object_id: BTreeMap<u64, usize>,
    /// Pages grouped by (kind, key) -- component OUT. What an object list would key on.
    by_kind_key: BTreeMap<String, usize>,
    /// Pages grouped by (kind, key) for COMPONENT-FREE pages only: the null control's population.
    component_free_by_kind_key: BTreeMap<String, usize>,
    component_free_by_object_id: BTreeMap<u64, usize>,
    total_buckets: usize,
    total_pages: usize,
    pages_without_object_id: usize,
}

fn population(engine: &TemporalEngine) -> Population {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut out = Population::default();
    for arm in EVERY_ARM {
        out.arms.insert(arm, (0, 0));
    }
    for node in shard.bucket_index.bucket_map.values() {
        let pages = node.block_index.len();
        let objects = node.object_index.len();
        let slot = out
            .arms
            .entry(arm_name(classify_bucket_layout(objects, pages)))
            .or_default();
        slot.0 += 1;
        slot.1 += pages;
        out.total_buckets += 1;
        out.total_pages += pages;
        if pages > 0 {
            out.pages_per_bucket.push(pages);
            out.objects_per_bucket.push(objects);
        }
        for entry in node.block_index.values() {
            match entry.address.object_id() {
                Some(id) => {
                    *out.by_object_id.entry(id).or_default() += 1;
                    if entry.component.is_none() {
                        *out.component_free_by_object_id.entry(id).or_default() += 1;
                    }
                }
                None => out.pages_without_object_id += 1,
            }
            let kind_key = format!("{:?}\u{1}{}", entry.model_id, entry.object_key);
            *out.by_kind_key.entry(kind_key.clone()).or_default() += 1;
            if entry.component.is_none() {
                *out.component_free_by_kind_key.entry(kind_key).or_default() += 1;
            }
        }
    }
    out.pages_per_bucket.sort_unstable();
    out.objects_per_bucket.sort_unstable();
    out
}

fn report_group(label: &str, groups: &BTreeMap<impl Ord, usize>) -> (usize, usize) {
    let mut counts: Vec<usize> = groups.values().copied().collect();
    counts.sort_unstable();
    let total: usize = counts.iter().sum();
    let max = counts.last().copied().unwrap_or_default();
    println!(
        "    {label:<34} {:>7} groups over {:>7} pages  p50 {:>4}  p90 {:>4}  p99 {:>4}  MAX {:>4}",
        counts.len(),
        total,
        percentile(&counts, 0.50),
        percentile(&counts, 0.90),
        percentile(&counts, 0.99),
        max
    );
    (counts.len(), max)
}

/// The width of a field, taken FROM THE FIELD. Naming the type at the call site compiles whatever
/// the field became, which is how `pages_per_bucket` came to charge two fat pointers for a
/// seventeen-byte group after `model_id` stopped being a pointer (#1994, paid down in #1997).
fn field_width<T>(_field: &T) -> usize {
    std::mem::size_of::<T>()
}

/// For each occupied bucket, the page count of every distinct `(kind, key)` inside it.
///
/// This is the breakdown a per-bucket dispatch would have to be uniform over, and the reason it
/// cannot be: the object list's win or loss is decided PER KEY and the layout state is PER BUCKET.
fn bucket_key_breakdown(engine: &TemporalEngine) -> Vec<Vec<usize>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut out: Vec<Vec<usize>> = Vec::new();
    for node in shard.bucket_index.bucket_map.values() {
        if node.block_index.len() == 0 {
            continue;
        }
        let mut per_key: BTreeMap<String, usize> = BTreeMap::new();
        for entry in node.block_index.values() {
            *per_key
                .entry(format!("{:?}\u{1}{}", entry.model_id, entry.object_key))
                .or_default() += 1;
        }
        out.push(per_key.into_values().collect());
    }
    out
}

/// The width of the name pointer a page entry spends today, read off a real entry's own field.
fn name_pointer_width(engine: &TemporalEngine) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    for node in shard.bucket_index.bucket_map.values() {
        for entry in node.block_index.values() {
            return field_width(&entry.object_key);
        }
    }
    panic!("no page entry to read a field width from");
}

/// THE ARM POPULATION, AND THE ONE NUMBER AN OBJECT LIST AMORTISES OVER.
///
/// Four stores: the real kind mix at two corpus sizes ten times apart, on both routing ranges.
/// Every arm row carries the bucket count, its share of buckets, the pages those buckets hold and
/// its share of pages, and both sets of rows are asserted to sum to the store's own totals.
#[test]
#[ignore = "seeds four mixed stores up to 40,000 records each; run by name"]
fn the_layout_arm_an_object_list_would_pay_for_is_under_one_percent_and_absent_at_the_shipped_range()
{
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut widest_bucket_share_of_multi_page_object = 0.0f64;
    let mut widest_object_by_id = 0usize;
    let mut widest_object_by_kind_key = 0usize;
    let mut series_max_seen = 0usize;
    let mut shipped_range_at_scale_multi_page_object: Option<usize> = None;
    let mut shipped_range_at_scale_live_arms: Option<usize> = None;
    let mut stores = 0usize;

    for records in [SMALL, LARGE] {
        for end in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end);
            let seeded = seed_mixed(&engine, records);
            let pop = population(&engine);
            stores += 1;

            let width = if end == WIDE_END {
                "the whole keyspace".to_string()
            } else {
                format!("0..{end}")
            };
            println!(
                "\n{records} records of the real kind mix on {width}: {} buckets, {} pages",
                pop.total_buckets, pop.total_pages
            );
            println!(
                "  (seeded {} string keys, {} container pages, {} series keys)",
                seeded.strings, seeded.container_pages, seeded.series_keys
            );

            // --- THE ARM POPULATION, BY BUCKET AND BY PAGE, EACH WITH ITS DENOMINATOR. ---
            let mut arm_buckets = 0usize;
            let mut arm_pages = 0usize;
            let mut live_arms = 0usize;
            for arm in EVERY_ARM {
                let (buckets, pages) = pop.arms.get(arm).copied().unwrap_or_default();
                arm_buckets += buckets;
                arm_pages += pages;
                if buckets > 0 {
                    live_arms += 1;
                }
                let bucket_share = if pop.total_buckets == 0 {
                    0.0
                } else {
                    buckets as f64 * 100.0 / pop.total_buckets as f64
                };
                let page_share = if pop.total_pages == 0 {
                    0.0
                } else {
                    pages as f64 * 100.0 / pop.total_pages as f64
                };
                println!(
                    "    {arm:<22} {buckets:>7} buckets ({bucket_share:>7.3}% of {})  {pages:>8} pages ({page_share:>7.3}% of {})",
                    pop.total_buckets, pop.total_pages
                );
                if arm == "multi_page_object" {
                    widest_bucket_share_of_multi_page_object =
                        widest_bucket_share_of_multi_page_object.max(bucket_share);
                    if records == LARGE && end == NARROW_END {
                        shipped_range_at_scale_multi_page_object = Some(buckets);
                    }
                }
            }
            if records == LARGE && end == NARROW_END {
                shipped_range_at_scale_live_arms = Some(live_arms);
            }
            // ROWS SUM TO THE POPULATION, both columns. A histogram that does not is not one.
            assert_eq!(
                arm_buckets, pop.total_buckets,
                "{records}/{end}: arm bucket rows sum to {arm_buckets} against {} buckets",
                pop.total_buckets
            );
            assert_eq!(
                arm_pages, pop.total_pages,
                "{records}/{end}: arm page rows sum to {arm_pages} against {} pages",
                pop.total_pages
            );
            // AND THE STORE HOLDS WHAT THE FIXTURE WROTE, so the denominator is not self-checked.
            assert!(
                pop.total_pages >= seeded.strings + seeded.container_pages,
                "{records}/{end}: the index holds {} pages, fewer than the {} strings plus {} container pages the fixture wrote -- the histogram is not over the fixture",
                pop.total_pages,
                seeded.strings,
                seeded.container_pages
            );
            println!("    live arms in this store: {live_arms} of {}", EVERY_ARM.len());

            println!(
                "    pages/bucket   p50 {:>5}  p90 {:>5}  p99 {:>5}  MAX {:>5}   over {} occupied",
                percentile(&pop.pages_per_bucket, 0.50),
                percentile(&pop.pages_per_bucket, 0.90),
                percentile(&pop.pages_per_bucket, 0.99),
                pop.pages_per_bucket.last().copied().unwrap_or_default(),
                pop.pages_per_bucket.len()
            );
            println!(
                "    objects/bucket p50 {:>5}  p90 {:>5}  p99 {:>5}  MAX {:>5}   over {} occupied",
                percentile(&pop.objects_per_bucket, 0.50),
                percentile(&pop.objects_per_bucket, 0.90),
                percentile(&pop.objects_per_bucket, 0.99),
                pop.objects_per_bucket.last().copied().unwrap_or_default(),
                pop.objects_per_bucket.len()
            );

            // --- PAGES PER OBJECT: THE NUMBER THE OBJECT LIST AMORTISES OVER. ---
            assert_eq!(
                pop.pages_without_object_id, 0,
                "{records}/{end}: {} pages carry no object id, so grouping by it would silently merge them under zero",
                pop.pages_without_object_id
            );
            let (_, max_by_id) = report_group("pages per object_id", &pop.by_object_id);
            let (_, max_by_kind_key) = report_group("pages per (kind, key)", &pop.by_kind_key);
            widest_object_by_id = widest_object_by_id.max(max_by_id);
            widest_object_by_kind_key = widest_object_by_kind_key.max(max_by_kind_key);

            // --- THE NULL CONTROL, AT 0.00%. ---
            let (cf_id_groups, cf_id_max) =
                report_group("CONTROL component-free by id", &pop.component_free_by_object_id);
            let (cf_key_groups, cf_key_max) = report_group(
                "CONTROL component-free by (kind,key)",
                &pop.component_free_by_kind_key,
            );
            assert!(
                cf_id_groups > 0,
                "{records}/{end}: the null control has no population at all"
            );
            assert_eq!(
                cf_id_groups, cf_key_groups,
                "CONTROL FAILED at {records}/{end}: component-free pages group into {cf_id_groups} objects by id and {cf_key_groups} by (kind, key). For a page with no component the two groupings ask the same question and must agree exactly; a difference here means the groupings are not measuring what they are named for"
            );
            assert_eq!(
                cf_id_max, cf_key_max,
                "CONTROL FAILED at {records}/{end}: component-free MAX pages per object is {cf_id_max} by id and {cf_key_max} by (kind, key)"
            );
            println!(
                "    CONTROL: component-free groupings agree exactly -- {cf_id_groups} groups, MAX {cf_id_max}, difference 0.00%"
            );
            series_max_seen = series_max_seen.max(cf_id_max);

            println!(
                "    divergence: MAX pages per object_id {max_by_id} against MAX pages per (kind, key) {max_by_kind_key}"
            );
        }
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); it shifts allocation bytes at about six bytes a character"
    );
    println!("\n  store path length held at {first} characters across all {stores} stores");

    // --- THE POSITIVE CONTROL: THE INSTRUMENT CAN SEE A MULTI-PAGE OBJECT. ---
    assert!(
        series_max_seen > 1,
        "THE FIXTURE NEVER REACHED A MULTI-PAGE OBJECT, so this module cannot tell an engine whose objects hold one page from a fixture that could not make one hold two. The series arm is seeded precisely to reach it and its MAX came back {series_max_seen}"
    );
    println!(
        "  POSITIVE CONTROL: a component-free (series) object reaches {series_max_seen} pages, so pages-per-object above one IS observable by this instrument"
    );

    // --- THE FINDING, ON THE ARM THE PROPOSAL IS FOR. ---
    assert!(
        widest_bucket_share_of_multi_page_object < 1.0,
        "`multi_page_object` reached {widest_bucket_share_of_multi_page_object:.3}% of buckets in some store. The finding is stated on it staying under one percent everywhere; re-price rather than loosening this"
    );
    let shipped = shipped_range_at_scale_multi_page_object
        .expect("the shipped range at scale must have been measured");
    assert_eq!(
        shipped, 0,
        "`multi_page_object` reached {shipped} buckets at 40,000 records on the shipped range, where the finding is stated on it being absent"
    );
    let live = shipped_range_at_scale_live_arms.expect("live arm count at scale");
    assert_eq!(
        live, 1,
        "at 40,000 records on the shipped range {live} layout arms are populated. The finding is stated on exactly ONE being live, which is what makes a five-way dispatch select nothing there"
    );
    println!(
        "\n  THE ARM: `multi_page_object` peaks at {widest_bucket_share_of_multi_page_object:.3}% of buckets and is EXACTLY ABSENT at 40,000 records on the shipped range, where {live} of {} arms is populated at all.",
        EVERY_ARM.len()
    );
    println!(
        "  Pages per object: MAX {widest_object_by_id} grouped by the engine's own object id (component IN the hash), MAX {widest_object_by_kind_key} grouped by (kind, key)."
    );
}

/// WHAT A PER-BUCKET DISPATCH CAN AND CANNOT CAPTURE, IN BYTES AND IN BRANCHES.
///
/// THE ARITHMETIC, AND IT IS THE WHOLE ARGUMENT. Let a bucket hold `P` pages over `K` distinct
/// `(kind, key)` objects, and let `W` be the width of the name pointer a page entry spends today,
/// READ OFF A REAL ENTRY'S FIELD rather than named at this call site.
///
///   today                 P * W
///   with an object list   K * W  +  P * 1  +  one list header per bucket
///
/// So the list pays for a single key exactly when `p * (W - 1) > W`, i.e. when that key holds at
/// least TWO pages. At `W = 16` a one-page key LOSES one byte and a hundred-page key saves
/// 1,484. THE DECISION IS THEREFORE PER KEY. `BucketLayoutState` is PER BUCKET.
///
/// THAT WOULD NOT MATTER IF BUCKETS WERE UNIFORM, AND AT THE SHIPPED RANGE THEY ARE NOT. This
/// test measures how many buckets hold BOTH a one-page key and a multi-page key -- a bucket a
/// single representation must be wrong about somewhere -- and prices the SHORTFALL between what a
/// per-bucket dispatch can capture and what a per-key decision could. That shortfall is the cost of
/// putting the choice on the layout state, and it is charged here rather than argued.
///
/// THE LIST IS CHARGED PER BUCKET AND DIVIDED BY THAT ARM'S OWN PAGES-PER-OBJECT, never by an
/// average across arms: that division is what #1997's routed arms failed on.
///
/// rust-internal: arithmetic over the engine's own bucket index; no product behaviour
#[test]
#[ignore = "seeds four mixed stores up to 40,000 records each; run by name"]
fn a_per_bucket_dispatch_cannot_separate_the_keys_the_object_list_wins_on_from_the_ones_it_loses_on()
{
    /// A Vec's header, charged once per bucket that grows an object list.
    const LIST_HEADER: usize = std::mem::size_of::<Vec<u8>>();
    /// The ordinal a page entry would carry instead of a name.
    const ORDINAL: usize = std::mem::size_of::<u8>();

    let mut any_mixed = false;
    for records in [SMALL, LARGE] {
        for end in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, end);
            seed_mixed(&engine, records);
            let breakdown = bucket_key_breakdown(&engine);
            let name_width = name_pointer_width(&engine);
            assert!(
                name_width > ORDINAL,
                "a name pointer is {name_width} B and an ordinal {ORDINAL} B; with no difference there is nothing to trade"
            );

            let width = if end == WIDE_END {
                "the whole keyspace".to_string()
            } else {
                format!("0..{end}")
            };

            let total_buckets = breakdown.len();
            let total_pages: usize = breakdown.iter().map(|keys| keys.iter().sum::<usize>()).sum();
            assert!(total_pages > 0, "{records}/{end}: nothing was written");

            let mut mixed_buckets = 0usize;
            let mut pages_in_mixed_buckets = 0usize;
            // What a PER-BUCKET dispatch can save: one representation for the whole bucket, and it
            // may pick whichever of the two is cheaper for that bucket -- the most generous
            // reading of the proposal.
            let mut per_bucket_saving: i64 = 0;
            // What a PER-KEY decision could save: each key picks for itself.
            let mut per_key_saving: i64 = 0;
            // Pages that a per-bucket dispatch is forced to represent the wrong way.
            let mut pages_represented_wrongly = 0usize;

            for keys in &breakdown {
                let pages: usize = keys.iter().sum();
                let distinct = keys.len();
                let singles = keys.iter().filter(|count| **count == 1).count();
                let multis = distinct - singles;
                if singles > 0 && multis > 0 {
                    mixed_buckets += 1;
                    pages_in_mixed_buckets += pages;
                }

                let today = (pages * name_width) as i64;
                let listed =
                    (distinct * name_width + pages * ORDINAL + LIST_HEADER) as i64;
                let bucket_best = today.min(listed);
                per_bucket_saving += today - bucket_best;

                let mut key_best: i64 = 0;
                let mut bucket_uses_list = false;
                for count in keys {
                    let key_today = (count * name_width) as i64;
                    let key_listed = (name_width + count * ORDINAL) as i64;
                    if key_listed < key_today {
                        bucket_uses_list = true;
                        key_best += key_listed;
                    } else {
                        key_best += key_today;
                    }
                }
                if bucket_uses_list {
                    key_best += LIST_HEADER as i64;
                }
                per_key_saving += today - key_best;

                // Under the bucket's single choice, how many pages sit in the wrong arm.
                if listed <= today {
                    // the whole bucket is listed: every one-page key is paying a list row it loses on
                    pages_represented_wrongly += singles;
                } else {
                    // the whole bucket stays inline: every multi-page key's pages keep a fat name
                    pages_represented_wrongly +=
                        keys.iter().filter(|count| **count > 1).copied().sum::<usize>();
                }
            }

            if mixed_buckets > 0 {
                any_mixed = true;
            }

            println!(
                "\n{records} records of the real kind mix on {width}: {total_buckets} occupied buckets, {total_pages} pages, name pointer {name_width} B read off the field"
            );
            println!(
                "    buckets holding BOTH a one-page key and a multi-page key: {mixed_buckets} ({:.3}% of {total_buckets}); they hold {pages_in_mixed_buckets} pages ({:.3}% of {total_pages})",
                mixed_buckets as f64 * 100.0 / total_buckets as f64,
                pages_in_mixed_buckets as f64 * 100.0 / total_pages as f64
            );
            println!(
                "    per-bucket dispatch (the layout state) saves {per_bucket_saving:>10} B = {:>7.3} B/page",
                per_bucket_saving as f64 / total_pages as f64
            );
            println!(
                "    per-key decision              saves {per_key_saving:>10} B = {:>7.3} B/page",
                per_key_saving as f64 / total_pages as f64
            );
            let captured = if per_key_saving == 0 {
                0.0
            } else {
                per_bucket_saving as f64 * 100.0 / per_key_saving as f64
            };
            println!(
                "    so the layout state captures {captured:.3}% of what the decision is actually worth"
            );
            println!(
                "    pages a single per-bucket representation must get wrong: {pages_represented_wrongly} ({:.3}% of {total_pages})",
                pages_represented_wrongly as f64 * 100.0 / total_pages as f64
            );

            // --- THE DISPATCH COST, IN PROBE COUNTS AND NOT IN TIME. ---
            // A timing ratio on this box read 485x idle against 11x busy off identical code, so the
            // dispatch is counted rather than timed. Two representations put one discriminant test
            // on EVERY page lookup and one on every walk; the number of those tests that reach a
            // DIFFERENT representation is what the dispatch buys.
            let arms_present = {
                let mut seen: std::collections::BTreeSet<&'static str> =
                    std::collections::BTreeSet::new();
                let pop = population(&engine);
                for arm in EVERY_ARM {
                    if pop.arms.get(arm).copied().unwrap_or_default().0 > 0 {
                        seen.insert(arm);
                    }
                }
                seen
            };
            println!(
                "    dispatch: 1 discriminant test per page lookup, paid on {total_pages} of {total_pages} pages (100.000%); layout arms actually present: {} ({:?})",
                arms_present.len(),
                arms_present
            );

            // THE VACUITY FLOOR, and it is here because a mutant survived without it. The
            // ordering assertion below is one-sided: a per-bucket saving of ZERO satisfies it,
            // so charging the object list per PAGE instead of per KEY -- which makes the listed
            // shape never cheaper and the saving identically nothing -- passed a guard whose
            // whole subject is how much the saving is. Both sides must be a real saving, and
            // the captured share must be materially above nothing, before the ordering means
            // anything at all.
            assert!(
                per_key_saving > 0 && per_bucket_saving > 0,
                "{records}/{end}: per-key saves {per_key_saving} B and per-bucket saves {per_bucket_saving} B. A saving of nothing is not a measurement of a saving, and the ordering assertion below would pass on it"
            );
            assert!(
                captured > 50.0,
                "{records}/{end}: a per-bucket dispatch captured {captured:.3}% of the per-key saving. This module reports 98.667% to 100.000%; a figure this low means the arithmetic is not measuring the dispatch"
            );
            assert!(
                per_bucket_saving <= per_key_saving,
                "{records}/{end}: a per-bucket dispatch cannot beat a per-key decision, yet it scored {per_bucket_saving} against {per_key_saving}; the arithmetic above is wrong"
            );
        }
    }

    assert!(
        any_mixed,
        "NO STORE HELD A MIXED BUCKET, so this test cannot tell a uniform population from a fixture that could not produce a mixed one. The mix seeds one-page string keys beside hundred-page container keys precisely to produce them"
    );
}

/// THE BUCKET ALREADY HOLDS AN OBJECT LIST. IT IS KEYED PER PAGE, WHICH IS WHY AN ORDINAL INTO IT
/// BUYS NOTHING.
///
/// `BucketNode::object_index` is exactly the per-bucket object list the proposal asks for, and it
/// is already durable. What it is not is keyed the way the proposal needs: a page's object identity
/// is `stable_block_object_id(shard_id, kind, object_key, component)` -- THE COMPONENT IS IN THE
/// HASH -- so a hash field, a set member, a list element and a sorted-set member are each an object
/// of their own. The list therefore holds very nearly one row PER PAGE, and a one-byte ordinal into
/// a list with one row per page cannot replace the page's name: the row it points at would have to
/// carry that name itself.
///
/// The saving priced in the test above is only available to a list keyed on `(kind, key)` -- which
/// is a DIFFERENT identity from the one the engine stores, so it is a SECOND durable structure
/// beside `object_index` rather than a re-use of it. That, and not the byte count, is what the
/// proposal actually costs.
///
/// MEASURED, not argued: this asserts the stored list's row count against the store's distinct
/// `(kind, key)` count, and refuses a run where they agree -- because if they agreed, the stored
/// list WOULD be the list the proposal wants and the finding here would be wrong.
///
/// rust-internal: reads the engine's own bucket index; no product behaviour
#[test]
#[ignore = "seeds two mixed stores; run by name"]
fn the_object_list_this_bucket_already_stores_holds_one_row_per_page_and_not_one_per_key() {
    for (records, end) in [(SMALL, NARROW_END), (LARGE, NARROW_END)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end);
        seed_mixed(&engine, records);

        let stored_rows: usize = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|node| node.object_index.len())
                .sum()
        };
        let pop = population(&engine);
        let distinct_keys = pop.by_kind_key.len();
        let distinct_object_ids = pop.by_object_id.len();

        println!(
            "\n{records} records on 0..{end}: {} pages",
            pop.total_pages
        );
        println!(
            "    rows in the STORED object_index            {stored_rows:>8}  ({:.4} rows per page)",
            stored_rows as f64 / pop.total_pages as f64
        );
        println!(
            "    distinct object ids among the pages        {distinct_object_ids:>8}  (component IN the identity)"
        );
        println!(
            "    distinct (kind, key) among the pages       {distinct_keys:>8}  (what the proposal would key on)"
        );
        println!(
            "    so the list the proposal wants is {:.2}x SHORTER than the one already stored",
            stored_rows as f64 / distinct_keys as f64
        );

        // The stored list tracks the component-bearing identity, so it is very nearly one row a
        // page -- NOT one row a key.
        assert!(
            stored_rows > distinct_keys,
            "the stored object_index holds {stored_rows} rows against {distinct_keys} distinct (kind, key). If these agreed, the stored list would already be the list the proposal wants and this module's finding would be wrong"
        );
        // And it is much closer to the page count than to the key count, which is the mechanism.
        let to_pages = (stored_rows as f64 - pop.total_pages as f64).abs();
        let to_keys = (stored_rows as f64 - distinct_keys as f64).abs();
        assert!(
            to_pages < to_keys,
            "the stored object_index row count {stored_rows} is closer to the distinct-key count {distinct_keys} than to the page count {}; the claim that it is keyed per page does not hold here",
            pop.total_pages
        );
    }
}
