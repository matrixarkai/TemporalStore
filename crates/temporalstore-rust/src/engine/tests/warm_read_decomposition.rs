// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE THE NINE GO.
//!
//! #1952 measured a warm serving read at 10.00 allocations and split it once: exactly one of the
//! ten is inside the cache, and nine are this engine's. It named those nine as the largest
//! remaining item on the read path and deliberately did not start on them. This module takes
//! them apart, one named site at a time, until the residual is ZERO -- not small, zero -- and
//! then removes the two that were buying nothing.
//!
//! MEASURED WHOLE, THEN DECOMPOSED, NEVER SUMMED. The figure that carries every claim here is
//! the SPAN total over 512 serving reads with real stored data, exactly as #1952 took it. The
//! per-site rows are an attribution of that total, charged inside the operation while it runs,
//! and the check that the attribution is complete is the residual reading zero. A decomposition
//! is a hypothesis about where a measured total went; per-statement measurements added up are
//! not a total at all, and MatrixCache #92 reported seven allocations for a line that costs nine
//! by doing exactly that.
//!
//! THE TEN, DECOMPOSED. Store path held at fifteen characters, key width twenty-five, 512
//! records, 512 serving reads, `residual 0 allocs 0 B`:
//!
//! ```text
//!   site                              allocs/read   B/read   whose
//!   the cache's own lookup                   1.00     1045   the cache crate's
//!   the request's key, built by the caller   1.00       32   the caller's
//!   command.clone() into execute_on_shard    1.00       25   ours
//!   the LRU recency stamp                    2.00       49   ours
//!   CacheKey::string                         2.00       30   the cache crate's
//!   decoding the cached answer, the tag      1.00        5   ours
//!   decoding the cached answer, the value    1.00     1024   ours
//!   Status::ok()'s "ok"                      1.00        2   ours
//!   ----------------------------------------------------------------
//!   total                                   10.00     2212
//! ```
//!
//! SO "NINE ARE OURS" IS TWO CLAIMS, AND ONLY ONE OF THEM HOLDS. One of the nine is the
//! caller's: `Command::StringGet` owns its key, the fixture builds that `String` inside the
//! measured span, and a production request builds it in the wire decoder instead. Two more are
//! the cache crate's: `CacheKey::string` is `MultiLayerCache`'s own key constructor and is no
//! more ours than the lookup it is handed to. The engine's own share of a warm read is SIX
//! allocations, not nine.
//!
//! AND THE TEN IS A FLOOR. `TemporalEngine::load_shard` -- what every arm in #1952 used --
//! writes `table_name: String::new()` and `shard_uri: String::new()`, and an empty `String`
//! clones without allocating. A shard loaded the way a metaserver loads one carries both, and
//! `execute_with_storage_override` clones the whole `ShardInfo` on EVERY command to read four
//! fields, three of which are `Copy`. Measured on the same fixture with the two names filled in:
//! **12.00 allocations, not 10.00**. That row is `shard_info_clone 2.00`, it is the single
//! largest avoidable item on this path, and no fixture in #1952 could see it.
//!
//! WHAT THIS CHANGES, AND WHAT IT PRICES AND LEAVES. The recency stamp -- 2.00 of the ten, 49
//! bytes -- built a `Vec<String>` and cloned each key into it so that a routing bucket could be
//! derived and a `u32 -> u64` inserted. It visits the keys now. Everything else is priced in the
//! pull request with the reason it stands, and the largest of them, the `ShardInfo` clone, is
//! named rather than taken: removing it means holding the `infos` read guard across the
//! admission check, and `admission_limits` reads `table_name` out of an owned
//! `&Option<ShardInfo>`.
//!
//! COUNTS CARRY EVERY CLAIM. The byte columns wobble between otherwise identical runs and the
//! counts do not -- #1952 observed that and so does this. The store path length is asserted
//! equal across arms rather than assumed, because allocation bytes move with it at about six
//! bytes per character.

// Unconditional: the guards at the foot of this file run in an ORDINARY build, where the
// counting allocator is not installed. The measurement apparatus above them is gated item by
// item instead.
#[allow(unused_imports)]
use super::*;

/// Records written in the small arm.
#[cfg(feature = "alloc-probe")]
const SMALL: usize = 512;
/// Records written in the large arm. Eight times the small one, so a per-read cost that grew
/// with the corpus would show as a ratio near eight rather than near one.
#[cfg(feature = "alloc-probe")]
const LARGE: usize = 4_096;
/// Serving reads measured in EVERY arm.
#[cfg(feature = "alloc-probe")]
const READS: usize = 512;
/// The stored value's width, in bytes.
#[cfg(feature = "alloc-probe")]
const VALUE_BYTES: usize = 1_024;
/// What a warm serving read cost before this change, on the `load_shard` fixture. The band below
/// EXCLUDES it: the assertion is strictly less than this, so no value can satisfy both.
#[cfg(feature = "alloc-probe")]
const ALLOCS_BEFORE: f64 = 10.0;
/// What the per-command `ShardInfo` clone costs a shard that carries the two names a metaserver
/// gives it: one allocation for the table name and one for the shard URI. This is the whole of
/// the difference between the two fixtures below, and it is asserted as an equality.
#[cfg(feature = "alloc-probe")]
const SHARD_INFO_CLONE_ALLOCS: f64 = 2.0;

/// The key for a record, twenty-five characters wide at every index this module uses -- the same
/// width #1952 held, so the two modules' figures are comparable.
#[cfg(feature = "alloc-probe")]
fn record_key(index: usize) -> String {
    format!("tenant/7/object/{index:09}")
}

/// What one arm observed, and what the cache itself says it did.
#[cfg(feature = "alloc-probe")]
struct WarmArm {
    counted: crate::alloc_probe::ClassifiedCounts,
    memory_hits: u64,
    disk_hits: u64,
    misses: u64,
}

#[cfg(feature = "alloc-probe")]
impl WarmArm {
    fn allocs_per_read(&self) -> f64 {
        self.counted.total.allocs as f64 / READS as f64
    }
    fn bytes_per_read(&self) -> f64 {
        self.counted.total.alloc_bytes as f64 / READS as f64
    }
    fn per_read(&self, class: crate::alloc_probe::AllocClass) -> f64 {
        self.counted.classes.row(class).allocs as f64 / READS as f64
    }
}

/// The `load_shard` fixture: the one every arm in #1952 used. Its `ShardInfo` carries two EMPTY
/// names, which clone without allocating, so every figure taken through it is a floor.
#[cfg(feature = "alloc-probe")]
fn engine_at(dir: &std::path::Path) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    (engine, dir.to_string_lossy().len())
}

/// The same engine loaded the way a metaserver loads one: a named table, a shard URI and a node.
/// Identical in every other respect -- same capacity, same directories, same path length -- so
/// the arms differ in the two names and in nothing else.
#[cfg(feature = "alloc-probe")]
fn engine_at_named(dir: &std::path::Path) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: Some(7),
        shard_uri: "ts://tenant-7/table-orders/shard-00001".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: "tenant_7_orders".to_string(),
    });
    assert!(
        response.status.ok,
        "the named shard must load, or this arm is measuring an unloaded shard: {:?}",
        response.status
    );
    (engine, dir.to_string_lossy().len())
}

/// Write `records` records, warm the command-answer cache, then measure `READS` warm reads.
#[cfg(feature = "alloc-probe")]
fn measure_warm_reads(engine: &TemporalEngine, records: usize) -> WarmArm {
    for index in 0..records {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: record_key(index),
                value: vec![118u8; VALUE_BYTES],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    for offset in 0..READS {
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: record_key(offset % records),
            },
        });
    }

    let before = engine.cache().stats();
    let span = crate::alloc_probe::ClassSpan::open();
    for offset in 0..READS {
        std::hint::black_box(engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: record_key(offset % records),
            },
        }));
    }
    let counted = span.close().expect("the counting allocator must be installed");
    let after = engine.cache().stats();

    WarmArm {
        counted,
        memory_hits: after.memory_hits.saturating_sub(before.memory_hits),
        disk_hits: after.disk_hits.saturating_sub(before.disk_hits),
        misses: after.misses.saturating_sub(before.misses),
    }
}

/// EVERY ARM MUST REACH A WARM MEMORY HIT AND NOTHING ELSE.
///
/// A read path that quietly misses is not a measurement of a warm hit: it is a measurement of
/// the 29.13-allocation absent case wearing the warm case's label, and every number taken from
/// it is wrong by a factor of three. Floored on the case it must reach and pinned to zero on the
/// two it must not, so no single value can satisfy the assertion and the failure at once.
/// The warm-hit floor as a PREDICATE, so a control can fire it on a shape no arm here produces.
///
/// Split out because the floor had nothing to discriminate while it was only ever applied to arms
/// that warm: weakening `>= READS` to `>= 0` changed no verdict in this module, and a floor that
/// cannot change a verdict is not a floor. The control below plants the one shape it exists to
/// catch -- an arm that served SOME answers from memory but fewer than it measured reads.
#[cfg(feature = "alloc-probe")]
fn warm_hit_floor_holds(memory_hits: u64, disk_hits: u64, misses: u64) -> bool {
    memory_hits >= READS as u64 && disk_hits == 0 && misses == 0
}

#[cfg(feature = "alloc-probe")]
fn assert_reached_a_warm_hit_and_only_that(label: &str, arm: &WarmArm) {
    // THE VERDICT COMES FROM THE PREDICATE, not from a second copy of the same three clauses
    // written out here. Two spellings of one rule is how one of them keeps a bug the other
    // fixed -- and mutation found exactly that: weakening the inline copy changed every arm's
    // verdict while the control over the predicate stayed green.
    assert!(
        warm_hit_floor_holds(arm.memory_hits, arm.disk_hits, arm.misses),
        "{label}: {READS} warm reads produced memory_hits {}, disk_hits {}, misses {}. A warm arm \
         serves every read it measures from memory and touches neither the SSD tier nor a miss; \
         an arm that does not is measuring one of the other two cases under this one's label",
        arm.memory_hits,
        arm.disk_hits,
        arm.misses,
    );
}

#[cfg(feature = "alloc-probe")]
fn report(label: &str, arm: &WarmArm) {
    println!(
        "  {label:<34} | {:>6.2} allocs {:>8.0} B per warm read | cache_read {:>5.2} \
         | recency_stamp {:>5.2} | residual {:>6} allocs | memory_hits {:>5} disk_hits {:>4} \
         misses {:>4}",
        arm.allocs_per_read(),
        arm.bytes_per_read(),
        arm.per_read(crate::alloc_probe::AllocClass::CacheRead),
        arm.per_read(crate::alloc_probe::AllocClass::RecencyStamp),
        arm.counted.residual_allocs(),
        arm.memory_hits,
        arm.disk_hits,
        arm.misses,
    );
}

/// WHAT A WARM SERVING READ COSTS NOW, ON BOTH FIXTURES, AT BOTH CORPUS SIZES.
///
/// The four arms differ in the corpus (512 against 4,096) and in whether the shard carries the
/// two names a metaserver gives it. Nothing else moves: same capacity, same key width, same
/// store path length, asserted equal at the end rather than trusted.
///
/// MEASURED, this change in:
///
/// ```text
///   fixture                 512 records        4,096 records     ratio
///   load_shard (unnamed)     8.00 allocs        8.00 allocs      1.00x
///   named shard             10.00 allocs       10.00 allocs      1.00x
/// ```
///
/// Against 10.00 and 12.00 before it. The 2.00 that comes off is the recency stamp, and the
/// 2.00 that separates the two fixtures is the `ShardInfo` clone, which stands.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn where_a_warm_serving_reads_allocations_go() {
    let mut path_lengths = Vec::new();
    let mut unnamed = Vec::new();
    let mut named = Vec::new();

    for &records in &[SMALL, LARGE] {
        println!("\nCORPUS {records} records, value {VALUE_BYTES} B, {READS} warm reads per arm");

        let plain_dir = tempfile::tempdir().unwrap();
        let (engine, len) = engine_at(plain_dir.path());
        path_lengths.push(len);
        let plain = measure_warm_reads(&engine, records);
        report("load_shard, two empty names", &plain);
        assert_reached_a_warm_hit_and_only_that("load_shard", &plain);
        drop(engine);

        let named_dir = tempfile::tempdir().unwrap();
        let (engine, len) = engine_at_named(named_dir.path());
        path_lengths.push(len);
        let with_names = measure_warm_reads(&engine, records);
        report("named shard, as a metaserver loads", &with_names);
        assert_reached_a_warm_hit_and_only_that("named shard", &with_names);
        drop(engine);

        // The named shard must cost MORE, and the difference is the clone of two names this
        // engine makes on every command to read four fields. An arm that did not show it would
        // mean the clone had stopped happening -- which is the change this file declines to make
        // and therefore the thing most worth noticing if someone else makes it.
        //
        // AN EQUALITY, NOT AN INEQUALITY. `ShardInfo` carries TWO names and the clone allocates
        // once for each, so the difference is exactly 2.00. Asserting only that it costs
        // MORE would stay true with one of the two names emptied -- the fixture would quietly
        // stop being the production shape it claims to be, and the headline figure would drift
        // from 12.00 to 11.00 without anything failing.
        let names_cost = with_names.allocs_per_read() - plain.allocs_per_read();
        assert_eq!(
            names_cost, SHARD_INFO_CLONE_ALLOCS,
            "the named shard cost {:.2} against the unnamed {:.2}, a difference of {names_cost:.2} \
             and not {SHARD_INFO_CLONE_ALLOCS:.2}. That difference IS the per-command ShardInfo \
             clone of a table name and a shard URI, and it is the whole of correction 2",
            with_names.allocs_per_read(),
            plain.allocs_per_read()
        );
        unnamed.push(plain.allocs_per_read());
        named.push(with_names.allocs_per_read());

        // THE RESIDUAL IS THE CHECK THAT THE ROWS ARE AN ATTRIBUTION OF THIS TOTAL rather than a
        // separate quantity that happens to look like it. It is never negative -- that is double
        // counting -- and the classes never claim more than the span made.
        assert!(
            plain.counted.residual_allocs() >= 0,
            "the classes claim {} allocations of a span that made {}",
            plain.counted.classes.summed().allocs,
            plain.counted.total.allocs
        );
    }

    // FLAT PER RECORD. Eight times the corpus, the same per-read cost, on both fixtures.
    assert_eq!(
        unnamed[0], unnamed[1],
        "the unnamed arm cost {:.2} at {SMALL} records and {:.2} at {LARGE}; a warm read that \
         moved with the corpus is not the flat cost this module reports",
        unnamed[0], unnamed[1]
    );
    assert_eq!(
        named[0], named[1],
        "the named arm cost {:.2} at {SMALL} records and {:.2} at {LARGE}",
        named[0], named[1]
    );

    // A BAND THAT EXCLUDES THE VALUE IT GUARDS AGAINST. 10.00 is what #1952 measured before the
    // recency stamp stopped collecting; the assertion is strictly below it, so the pre-change
    // figure cannot satisfy this and a silent revert cannot pass.
    for cost in &unnamed {
        assert!(
            *cost < ALLOCS_BEFORE,
            "a warm serving read costs {cost:.2} allocations against {ALLOCS_BEFORE:.2} before \
             the recency stamp stopped collecting a Vec<String> to walk once and drop"
        );
        assert!(
            *cost > 1.0,
            "a warm serving read costs {cost:.2} allocations, which is at or below the single \
             allocation the cache's own lookup makes; a fixture that stopped reading would \
             report exactly this"
        );
    }

    let first = path_lengths[0];
    for (index, length) in path_lengths.iter().enumerate() {
        assert_eq!(
            *length, first,
            "arm {index} ran against a store path of {length} characters, not {first}; \
             allocation bytes move with that length at about six bytes a character"
        );
    }
    println!(
        "\n  store path held at {first} characters across all {} arms; \
         unnamed {:.2}, named {:.2}, the difference is the per-command ShardInfo clone",
        path_lengths.len(),
        unnamed[0],
        named[0],
    );
}

/// THE RECENCY STAMP'S ROW, WITH THE CONTROL THAT PROVES IT IS BEING CHARGED.
///
/// A row that reads zero is exactly the shape of a class nobody enters, so zero on its own says
/// nothing. The control is the WRITE arm: the same scope, the same production site, on a command
/// whose keys `command_object_keys` SYNTHESISES and therefore has to build. It must be non-zero
/// there and zero on the read, on one run of one binary, or the zero is an instrument failure
/// rather than a saving.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn the_recency_stamp_allocates_on_a_write_and_not_on_a_read() {
    use crate::alloc_probe::AllocClass;

    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine_at(dir.path());

    // The control arm: writes, whose touched keys come from `command_object_keys`.
    let write_span = crate::alloc_probe::ClassSpan::open();
    for index in 0..READS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: record_key(index),
                value: vec![118u8; VALUE_BYTES],
            },
        });
    }
    let writes = write_span.close().expect("the counting allocator must be installed");

    let warm = measure_warm_reads(&engine, READS);
    assert_reached_a_warm_hit_and_only_that("recency control", &warm);

    let write_row = writes.classes.row(AllocClass::RecencyStamp).allocs;
    let read_row = warm.counted.classes.row(AllocClass::RecencyStamp).allocs;
    println!(
        "\n  recency_stamp: {READS} writes charged {write_row} allocs ({:.2} a command), \
         {READS} warm reads charged {read_row} ({:.2} a read)",
        write_row as f64 / READS as f64,
        read_row as f64 / READS as f64,
    );

    assert!(
        write_row > 0,
        "the write arm charged {write_row} allocations to the recency stamp. The scope is in \
         production code at both sites and a write's touched keys are synthesised, so zero here \
         means the class is not being charged at all and the read arm's zero below says nothing"
    );
    assert_eq!(
        read_row, 0,
        "a warm serving read charged {read_row} allocations to the recency stamp. It visits the \
         key the command already owns; collecting it into a Vec<String> again is the two \
         allocations this change took off"
    );
}

// ---------------------------------------------------------------------------------------------
// The guards below run in an ORDINARY build. Everything above needs the counting allocator,
// which a normal `cargo test` does not install, so nothing above would catch a regression on its
// own. These do, and they hold the BEHAVIOUR the change had to preserve rather than its cost.
// ---------------------------------------------------------------------------------------------

/// A READ STILL STAMPS ITS BUCKET'S RECENCY, AND STAMPS THE SAME BUCKET IT USED TO.
///
/// This is the failure the change could have caused and nothing else would have noticed. The
/// recency map is how eviction prefers a least-recently-used bucket, and nothing reads it back
/// on the serving path, so a `for_each_touched_key` visiting NOTHING on a read would leave every
/// answer byte-identical, every status ok, every cache counter unmoved, and only eviction's
/// choice of victim quietly wrong -- weeks later, under pressure, on a different machine.
///
/// COMPARED ELEMENT BY ELEMENT AGAINST A CONTROL, not merely asserted non-empty. The control is
/// the bucket each key would be stamped under, derived here from the same routing function the
/// engine uses, so a stamp landing under the WRONG bucket fails as loudly as no stamp at all.
#[test]
fn a_warm_read_stamps_the_recency_of_the_bucket_its_key_belongs_to() {
    const KEYS: usize = 32;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    // A NARROW routing range, so the thirty-two keys land in distinguishable buckets instead of
    // all collapsing onto one and making "the right bucket" unfalsifiable.
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: 1_023,
        readonly: false,
        table_name: String::new(),
    });
    assert!(response.status.ok, "load: {:?}", response.status);

    let names: Vec<String> = (0..KEYS)
        .map(|index| format!("tenant/7/object/{index:09}"))
        .collect();
    for name in &names {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: name.clone(),
                value: vec![118u8; 1_024],
            },
        });
    }

    // The control: what the recency map must contain after the reads below, derived from the
    // routing function rather than from the map itself.
    let mut expected: Vec<u32> = names
        .iter()
        .map(|name| crate::engine::hashing::block_routing_bucket(name, 0, 1_023))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    assert!(
        expected.len() > 1,
        "the fixture put all {KEYS} keys in one routing bucket, so a stamp under the wrong \
         bucket could not be told from a stamp under the right one"
    );

    // Clear what the WRITES stamped, so what is asserted below was put there by a READ.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        shard.bucket_recency.clear();
        assert!(shard.bucket_recency.is_empty());
    }

    for name in &names {
        let answer = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: name.clone() },
        });
        // A shed or empty answer would satisfy the recency assertion below while serving nothing.
        match answer.response {
            CommandResponse::Bytes { value: Some(ref bytes) } => {
                assert_eq!(bytes.len(), 1_024, "a served answer must carry the stored value");
            }
            other => panic!("a warm read of a present key answered {other:?}"),
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");
    let mut stamped: Vec<u32> = shard.bucket_recency.keys().copied().collect();
    stamped.sort_unstable();
    println!(
        "  {KEYS} warm reads stamped {} buckets; the keys belong to {}",
        stamped.len(),
        expected.len()
    );
    assert_eq!(
        stamped, expected,
        "the buckets a read stamped are not the buckets its keys belong to. Reads stamp recency \
         through for_each_touched_key, and nothing on the serving path reads this map back, so \
         this is the only place a read that stopped stamping -- or stamped the wrong bucket -- \
         is visible"
    );
}

/// A WRITE STILL STAMPS ITS BUCKET TOO, THROUGH THE SAME VISITOR.
///
/// The fork inside `for_each_touched_key` has two arms and the read test above drives only one.
/// A visitor that fired only on the borrowed arm would pass it and lose every write's stamp.
#[test]
fn a_write_stamps_the_recency_of_the_bucket_its_key_belongs_to() {
    const KEYS: usize = 32;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: 1_023,
        readonly: false,
        table_name: String::new(),
    });
    assert!(response.status.ok, "load: {:?}", response.status);

    let names: Vec<String> = (0..KEYS)
        .map(|index| format!("tenant/7/object/{index:09}"))
        .collect();
    let mut expected: Vec<u32> = names
        .iter()
        .map(|name| crate::engine::hashing::block_routing_bucket(name, 0, 1_023))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    assert!(expected.len() > 1, "the fixture collapsed every key onto one bucket");

    for name in &names {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: name.clone(),
                value: vec![118u8; 1_024],
            },
        });
        assert!(response.status.ok, "write: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");
    let mut stamped: Vec<u32> = shard.bucket_recency.keys().copied().collect();
    stamped.sort_unstable();
    assert_eq!(
        stamped, expected,
        "the buckets a write stamped are not the buckets its keys belong to; the owned arm of \
         for_each_touched_key is the one a write takes and it is not firing"
    );
}

/// THE VISITOR YIELDS EXACTLY THE KEYS THE COLLECTING FORM YIELDED, COMMAND BY COMMAND.
///
/// `for_each_touched_key` replaced a function that returned `Vec<String>`, and the shape of a
/// regression is a command whose keys it now skips or duplicates. Rather than trusting the
/// rewrite, this rebuilds the old answer from the two functions the old one was made of --
/// `command_object_keys`, unchanged, and `command_read_key` -- and compares element by element,
/// in order, on a command of each of the two arms plus one that touches no key at all.
#[test]
fn the_touched_key_visitor_yields_what_the_collecting_form_yielded() {
    use crate::engine::command_validation::{command_object_keys, command_read_key, for_each_touched_key};

    let cases: Vec<(&str, Command)> = vec![
        (
            "StringGet, the borrowed arm",
            Command::StringGet { key: "tenant/7/object/000000001".to_string() },
        ),
        (
            "StringSet, the owned arm",
            Command::StringSet {
                key: "tenant/7/object/000000002".to_string(),
                value: vec![1u8; 8],
            },
        ),
        ("LeaderEstablish, neither arm", Command::LeaderEstablish),
    ];

    for (label, command) in &cases {
        // The collecting form, rebuilt here from its own two halves.
        let object_keys = command_object_keys(command);
        let expected: Vec<String> = if object_keys.is_empty() {
            command_read_key(command).map(str::to_string).into_iter().collect()
        } else {
            object_keys
        };

        let mut visited: Vec<String> = Vec::new();
        for_each_touched_key(command, |key| visited.push(key.to_string()));

        assert_eq!(
            visited, expected,
            "{label}: the visitor yielded {visited:?} where the collecting form yielded \
             {expected:?}"
        );
    }

    // The three cases must not be one case wearing three labels: the first two must yield a key
    // and the third must yield none, or the comparison above holds vacuously.
    let mut counts = Vec::new();
    for (_, command) in &cases {
        let mut seen = 0usize;
        for_each_touched_key(command, |_| seen += 1);
        counts.push(seen);
    }
    assert_eq!(
        counts,
        vec![1, 1, 0],
        "the cases yielded {counts:?} keys; a table where every case yields nothing would pass \
         the comparison above without comparing anything"
    );
}

/// THE WARM-HIT FLOOR FIRES ON THE SHAPE IT EXISTS TO CATCH, AND ON NOTHING ELSE.
///
/// A control over the predicate rather than over an arm, because every arm this module builds
/// warms fully -- so the floor never changes a verdict here and a mutation that removes it is
/// invisible. The plant is the one shape a real fixture goes wrong in: reads that mostly hit,
/// with the cache quietly answering fewer of them than were measured.
#[test]
#[cfg(feature = "alloc-probe")]
fn the_warm_hit_floor_rejects_an_arm_that_served_fewer_answers_than_it_measured() {
    // Accepted: a fully warm arm.
    assert!(
        warm_hit_floor_holds(READS as u64, 0, 0),
        "a fully warm arm must satisfy the floor, or every arm in this module fails for the \
         wrong reason"
    );
    // THE PLANT: one answer short, with both other counters still clean. This is what the
    // `>= READS` half exists for, and nothing else in the predicate can see it.
    assert!(
        !warm_hit_floor_holds(READS as u64 - 1, 0, 0),
        "an arm that served {} of {READS} answers from memory satisfied the warm-hit floor; the \
         floor has been weakened to something that cannot fail",
        READS - 1
    );
    // And the other two failure shapes, so the predicate is not passing this test on one clause.
    assert!(!warm_hit_floor_holds(READS as u64, 1, 0), "an arm that reached the SSD tier passed");
    assert!(!warm_hit_floor_holds(READS as u64, 0, 1), "an arm that missed passed");
}

/// THE BATCH PATH STAMPS RECENCY TOO, THROUGH THE SAME VISITOR.
///
/// `for_each_touched_key` replaced a collecting helper at TWO call sites: the single-command
/// path and `batch_execute`'s post-write stamp. A guard covering one of two live copies lets the
/// other keep the bug, and mutation confirmed it -- emptying the batch site's visitor changed no
/// test in either width until this existed. The batch path is driven by several tests in this
/// tree; none of them asserts anything about the recency map.
#[test]
fn a_batched_write_stamps_the_recency_of_the_buckets_its_keys_belong_to() {
    const KEYS: usize = 32;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: 1_023,
        readonly: false,
        table_name: String::new(),
    });
    assert!(response.status.ok, "load: {:?}", response.status);

    let names: Vec<String> = (0..KEYS)
        .map(|index| format!("tenant/7/object/{index:09}"))
        .collect();
    let mut expected: Vec<u32> = names
        .iter()
        .map(|name| crate::engine::hashing::block_routing_bucket(name, 0, 1_023))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    assert!(
        expected.len() > 1,
        "the fixture collapsed every key onto one bucket, so a stamp under the wrong bucket \
         could not be told from a stamp under the right one"
    );

    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands: names
            .iter()
            .map(|name| Command::StringSet {
                key: name.clone(),
                value: vec![118u8; 1_024],
            })
            .collect(),
    });
    assert!(response.status.ok, "batch: {:?}", response.status);
    assert_eq!(
        response.responses.len(),
        KEYS,
        "the batch must have executed every command, or it stamped fewer buckets for a reason \
         that has nothing to do with the visitor"
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");
    let mut stamped: Vec<u32> = shard.bucket_recency.keys().copied().collect();
    stamped.sort_unstable();
    println!(
        "  a batch of {KEYS} writes stamped {} buckets; the keys belong to {}",
        stamped.len(),
        expected.len()
    );
    assert_eq!(
        stamped, expected,
        "the buckets a BATCHED write stamped are not the buckets its keys belong to. This is the \
         second of the two call sites the collecting helper was replaced at, and it is the one no \
         test in this tree reached"
    );
}

/// A READ COMMAND'S KEY IS BORROWED, NOT REBUILT.
///
/// The saving is that `command_read_key` hands back a pointer INTO the command rather than a
/// copy of it, and the only way to hold that without a counting allocator is to compare the
/// addresses. A `to_string()` slipped back into that function would leave every other test in
/// this file green and put both allocations straight back.
#[test]
fn a_read_commands_key_is_borrowed_from_the_command_itself() {
    use crate::engine::command_validation::command_read_key;

    let key = "tenant/7/object/000000003".to_string();
    let key_address = key.as_ptr();
    let command = Command::StringGet { key };

    let borrowed = command_read_key(&command).expect("a StringGet reads one key");
    assert_eq!(borrowed, "tenant/7/object/000000003");
    assert_eq!(
        borrowed.as_ptr(),
        key_address,
        "command_read_key returned a different buffer from the one the command owns, so it \
         copied the key rather than borrowing it and the two allocations are back"
    );
}
