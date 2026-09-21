// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A CACHE READ COSTS ON THE SERVING PATH.
//!
//! The write path has been measured and cut repeatedly: twenty-seven allocations a stored record
//! came off it through two cache changes and two dependency pin bumps. The second of those bumps
//! said plainly that the read-side savings it carries are not exercised by the write-span ladder,
//! and left the read side unmeasured. This module measures it.
//!
//! THE SERVING READ PATH, AS THE COMPILER NAMES IT rather than as a grep guesses it. Every read
//! door on `MultiLayerCache` -- thirty-one of them, counting the older spellings the crate still
//! carries -- was made private in a local copy of the pinned crate, and the call sites that
//! stopped resolving were collected and retargeted until a pass produced none. That enumeration
//! ends at EIGHT sites in the production library, five distinct doors. Grep cannot do this: the
//! engine calls `.get(` on maps several thousand times, and two of the eight are wrapped method
//! chains whose receiver is on a different line from the call.
//!
//! Of the eight, only these four READ A STORED ANSWER, and all four are charged to
//! `AllocClass::CacheRead`:
//!
//! ```text
//!   engine.rs              read_block_bytes        cache.get           the page, owned
//!   engine.rs              read_block_shared       cache.get_shared    the page, shared
//!   command_validation.rs  cached_response         cache.get           the command answer
//!   compaction.rs          block_memory_resident   cache.get_memory    a residency probe
//! ```
//!
//! The other four are not reads of a stored answer and are deliberately not charged to it:
//! `peek` in `invalidate_if_cached` and `entries_for_shard` in the batched sweep are
//! INVALIDATION, which `AllocClass::CacheInvalidation` already owns, and `peek_tier` with
//! `entries_for_shard` in the storage reports are REPORTING. Two further sites sit inside
//! `#[cfg(test)] mod eviction_round_scale` in a production FILE and are not production ITEMS --
//! classifying those by file rather than by item would have put two test helpers in this table.
//!
//! ONE SERVING READ IS NOT ONE CACHE READ. A `StringGet` enters `cached_response`, which reads the
//! command-answer cache. On a hit it stops there: one cache read. On a miss it calls the source,
//! which resolves an address and calls `read_block_bytes`, reading the page cache as well. The
//! tables below report cache reads per serving read so that the two are never confused, and the
//! measured arms all come out at 1.00 -- a key with no stored answer never reaches a page read,
//! so the absent arm is one cache read too.
//!
//! ABSOLUTE QUANTITIES, NEVER A SHARE. Every figure is allocations and bytes per serving read,
//! with the shape stated. A share re-scales with corpus size, with the regime, and with every
//! change landed since it was taken.
//!
//! THE STORE PATH IS HELD CONSTANT ACROSS EVERY ARM AND IT IS `/tmp/.tmpXXXXXX`, fifteen
//! characters. Allocation BYTES move with the length of the store's directory path at about six
//! bytes per character, an effect that once read as a one percent profile difference until the
//! pair was run properly. Allocation COUNTS are immune to it, which is why the counts carry every
//! claim here and the byte columns are printed beside them rather than asserted on. The arms
//! assert the path length is equal rather than trusting that it is -- and the byte columns do
//! wobble between otherwise identical runs while the counts do not, which is the effect showing
//! itself.
//!
//! THE KEY WIDTH IS HELD CONSTANT TOO: `tenant/7/object/{index:09}` is twenty-five characters at
//! every corpus size used here, so the corpus arms differ in the number of records and in nothing
//! else.

// Unconditional: the two guards at the foot of this file run in an ORDINARY build, where the
// counting allocator is not installed, and they still need the engine and its command types. The
// measurement apparatus above them is gated item by item instead.
#[allow(unused_imports)]
use super::*;

/// Records written in the small arm.
#[cfg(feature = "alloc-probe")]
const SMALL: usize = 512;
/// Records written in the large arm. Eight times the small one, so a per-read cost that grew with
/// the corpus would show as a ratio near eight rather than near one.
#[cfg(feature = "alloc-probe")]
const LARGE: usize = 4_096;
/// Serving reads measured in EVERY arm, held equal so the arms differ in corpus and not in how
/// much work was measured.
#[cfg(feature = "alloc-probe")]
const READS: usize = 512;
/// The stored value's width, in bytes.
#[cfg(feature = "alloc-probe")]
const VALUE_BYTES: usize = 1_024;

/// The key for a record, twenty-five characters wide at every index this module uses.
#[cfg(feature = "alloc-probe")]
fn record_key(index: usize) -> String {
    format!("tenant/7/object/{index:09}")
}

/// What one arm observed: the allocation span, and what the cache itself says it did.
#[cfg(feature = "alloc-probe")]
struct ReadArm {
    counted: crate::alloc_probe::ClassifiedCounts,
    memory_hits: u64,
    disk_hits: u64,
    misses: u64,
}

#[cfg(feature = "alloc-probe")]
impl ReadArm {
    fn cache_reads(&self) -> u64 {
        self.memory_hits + self.disk_hits + self.misses
    }
    fn allocs_per_read(&self) -> f64 {
        self.counted.total.allocs as f64 / READS as f64
    }
    fn bytes_per_read(&self) -> f64 {
        self.counted.total.alloc_bytes as f64 / READS as f64
    }
    fn cache_reads_per_read(&self) -> f64 {
        self.cache_reads() as f64 / READS as f64
    }
}

/// Build an engine on a store path of the fifteen-character `tempfile` shape, and report how long
/// that path is so the caller can assert it did not move between arms.
#[cfg(feature = "alloc-probe")]
fn engine_at(dir: &std::path::Path, memory_capacity_bytes: usize) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        memory_capacity_bytes,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    (engine, dir.to_string_lossy().len())
}

/// Write `records` records, then measure `READS` serving reads of the keys `pick` names.
///
/// The cache's own counters are sampled around the same span, so an arm can say which of the
/// three cases it actually reached instead of assuming it.
#[cfg(feature = "alloc-probe")]
fn measure_reads(
    engine: &TemporalEngine,
    records: usize,
    pick: impl Fn(usize) -> String,
    warm: bool,
) -> ReadArm {
    for index in 0..records {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: record_key(index),
                value: vec![118u8; VALUE_BYTES],
            },
        });
    }
    if warm {
        for offset in 0..READS {
            let _ = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: pick(offset) },
            });
        }
    }

    let before = engine.cache().stats();
    let span = crate::alloc_probe::ClassSpan::open();
    for offset in 0..READS {
        std::hint::black_box(engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: pick(offset) },
        }));
    }
    let counted = span.close().expect("the counting allocator must be installed");
    let after = engine.cache().stats();

    ReadArm {
        counted,
        memory_hits: after.memory_hits.saturating_sub(before.memory_hits),
        disk_hits: after.disk_hits.saturating_sub(before.disk_hits),
        misses: after.misses.saturating_sub(before.misses),
    }
}

#[cfg(feature = "alloc-probe")]
fn report(label: &str, arm: &ReadArm) {
    println!(
        "  {label:<34} | {:>6.2} allocs {:>9.0} B per serving read | cache reads/read {:>4.2} \
         | memory_hits {:>6} disk_hits {:>6} misses {:>6} | cache_read class {:>6} allocs \
         | classified {:>5.1}% of calls, residual {:>8} allocs",
        arm.allocs_per_read(),
        arm.bytes_per_read(),
        arm.cache_reads_per_read(),
        arm.memory_hits,
        arm.disk_hits,
        arm.misses,
        arm.counted.classes.row(crate::alloc_probe::AllocClass::CacheRead).allocs,
        arm.counted.classified_call_share() * 100.0,
        arm.counted.residual_allocs(),
    );
}

/// WHAT A SERVING READ COSTS, at two corpus sizes, in each of the three cases that differ.
///
/// The three cases are the three the cache crate's own read-path work separates, because they
/// cost very differently: an answer held in memory, a key with no answer, and an answer that
/// lives on the SSD tier. A fixture that only ever hits is not a measurement of the read path, so
/// each case is reached deliberately and the cache's own counters are asserted to prove it was.
///
/// MEASURED, both corpus sizes identical to two decimal places:
///
/// ```text
///   case                        allocs/read   cache reads/read   which counter moved
///   answer held in memory              8.00               1.00   memory_hits
///   no such key                       27.13               1.00   misses
///   answer on the SSD tier            37.00               1.00   disk_hits
/// ```
///
/// Flat per record: 512 records and 4,096 records give the same per-read figure, so none of this
/// is a corpus effect.
///
/// THESE READ 10.00, 29.13 AND 39.00 WHEN THIS FILE WAS WRITTEN. `warm_read_decomposition` took
/// the first of them apart site by site and stopped the per-command LRU recency stamp collecting
/// a `Vec<String>` of keys that it walks once and drops. That stamp runs on every command rather
/// than in any one of these three cases, so exactly 2.00 came off all three and the SHAPE of
/// this table -- which is what it exists to show -- is unchanged. Restated here rather than left
/// standing: a recorded measurement whose mechanism has since been fixed reads as a live one.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_cache_read_costs_on_the_serving_path() {
    let mut path_lengths = Vec::new();

    for &records in &[SMALL, LARGE] {
        println!("\nCORPUS {records} records, value {VALUE_BYTES} B, {READS} serving reads per arm");

        // HIT: generous memory, warmed, reading keys that are present.
        let hit_dir = tempfile::tempdir().unwrap();
        let (engine, len) = engine_at(hit_dir.path(), 256 * 1024 * 1024);
        path_lengths.push(len);
        let hit = measure_reads(&engine, records, |offset| record_key(offset % records), true);
        report("HIT, answer held in memory", &hit);
        drop(engine);

        // ABSENT: the same engine shape, reading keys that were never written.
        let absent_dir = tempfile::tempdir().unwrap();
        let (engine, len) = engine_at(absent_dir.path(), 256 * 1024 * 1024);
        path_lengths.push(len);
        let absent = measure_reads(
            &engine,
            records,
            |offset| format!("tenant/7/absent/{offset:09}"),
            false,
        );
        report("ABSENT, no such key", &absent);
        drop(engine);

        // SSD-RESIDENT: a memory tier far too small to hold the corpus, so entries are demoted to
        // the SSD tier and later read back from it. Worth stating because it is not obvious: the
        // command-answer cache is written with `put_memory_only`, and an entry put that way still
        // reaches the SSD tier -- not at put time, but when the memory tier evicts it.
        let ssd_dir = tempfile::tempdir().unwrap();
        let (engine, len) = engine_at(ssd_dir.path(), 64 * 1024);
        path_lengths.push(len);
        let ssd = measure_reads(&engine, records, |offset| record_key(offset % records), true);
        report("SSD-tier, memory too small", &ssd);
        drop(engine);

        println!(
            "  RATIO within this corpus: absent/hit allocs {:>5.2}x, ssd/hit allocs {:>5.2}x",
            absent.allocs_per_read() / hit.allocs_per_read().max(f64::MIN_POSITIVE),
            ssd.allocs_per_read() / hit.allocs_per_read().max(f64::MIN_POSITIVE),
        );

        // EACH CASE MUST BE REACHED, with a floor on how many times. Floors rather than
        // equalities: the engine is free to read more than the one entry a request names, and a
        // floor still fails on the thing that matters, which is an arm that reached the case
        // zero times and reported a number for it anyway.
        assert!(
            hit.memory_hits >= READS as u64,
            "the HIT arm must serve at least {READS} answers from memory, saw {}",
            hit.memory_hits
        );
        assert_eq!(
            hit.disk_hits, 0,
            "the HIT arm must not reach the SSD tier at all, saw {} disk hits",
            hit.disk_hits
        );
        assert!(
            absent.misses >= READS as u64,
            "the ABSENT arm must miss at least {READS} times, saw {}",
            absent.misses
        );
        assert!(
            ssd.disk_hits >= READS as u64,
            "the SSD arm must serve at least {READS} answers from the SSD tier, saw {}; \
             without that this arm is measuring a memory hit under a different label",
            ssd.disk_hits
        );

        // The three cases must actually COST differently, or they are one case wearing three
        // labels and the table above says nothing.
        assert!(
            absent.allocs_per_read() > hit.allocs_per_read() * 1.5,
            "absent {:.2} against hit {:.2}: these are not distinguishable cases",
            absent.allocs_per_read(),
            hit.allocs_per_read()
        );
        assert!(
            ssd.allocs_per_read() > absent.allocs_per_read(),
            "ssd {:.2} against absent {:.2}",
            ssd.allocs_per_read(),
            absent.allocs_per_read()
        );
    }

    // Every arm ran against the same store path length, so no byte column above is the path
    // length wearing a corpus label.
    let first = path_lengths[0];
    for (index, length) in path_lengths.iter().enumerate() {
        assert_eq!(
            *length, first,
            "arm {index} ran against a store path of {length} characters, not {first}; \
             allocation bytes move with that length and the arms would not be comparable"
        );
    }
    println!("\n  store path held at {first} characters across all {} arms", path_lengths.len());
}

/// HOW MUCH OF A SERVING READ THE CLASSES ACCOUNT FOR.
///
/// Before `AllocClass::CacheRead` existed this reconciled to EXACTLY ZERO. All nine classes were
/// charged inside WRITE primitives -- the page encode, the slab append, the bucket index, the
/// dirty-object index, the staged outcomes, the log record, the index-log delta, the carried page
/// and the invalidation -- and a read entered none of them, so every allocation a serving read
/// made landed in the residual and nothing could say what any of it was for. That is a stronger
/// statement than the 6.0% a replaying restore reconciles to, and it is the reason this class was
/// added rather than the numbers being reported with a hole in them.
///
/// Asserts that the class is actually charged rather than merely declared: a row that reads zero
/// while the span allocated is the exact failure the `Option` on `classified_now` exists to stop
/// being invisible.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn how_much_of_a_serving_read_the_classes_account_for() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine_at(dir.path(), 256 * 1024 * 1024);
    let arm = measure_reads(&engine, SMALL, |offset| record_key(offset % SMALL), true);

    println!(
        "\n  A SERVING READ: {:.2} allocs, {:.0} B; classes claim {} allocs ({:.2}% of calls, \
         {:.2}% of bytes); residual {} allocs {} B",
        arm.allocs_per_read(),
        arm.bytes_per_read(),
        arm.counted.classes.summed().allocs,
        arm.counted.classified_call_share() * 100.0,
        arm.counted.classified_byte_share() * 100.0,
        arm.counted.residual_allocs(),
        arm.counted.residual_bytes(),
    );
    for class in crate::alloc_probe::AllocClass::ALL {
        let row = arm.counted.classes.row(class);
        if row.allocs > 0 {
            println!("    {:<20} {:>8} allocs {:>10} B", class.label(), row.allocs, row.alloc_bytes);
        }
    }

    assert!(
        arm.counted.total.allocs > 0,
        "the probe must observe the reads, or the share below divides by a broken instrument"
    );
    assert!(
        arm.counted.classes.row(crate::alloc_probe::AllocClass::CacheRead).allocs > 0,
        "the read class is declared and charged in four production primitives, and a span of \
         {} serving reads entered none of them",
        READS
    );
    // A negative residual is double counting, and is worth failing on rather than clamping away.
    assert!(
        arm.counted.residual_allocs() >= 0,
        "the classes claim more allocations than the span made: {} against {}",
        arm.counted.classes.summed().allocs,
        arm.counted.total.allocs
    );
}

/// THE RESIDUAL INSTRUMENT, PROVED BY PLANTING A KNOWN ALLOCATION IN IT.
///
/// A residual that reads zero is the shape a broken span takes, so the instrument is made to
/// report a quantity known in advance and nothing else. Two spans, one plant:
///
///   * planted OUTSIDE any class -- the residual must move by exactly the planted bytes and
///     exactly one allocation, and no class row may move at all;
///   * planted INSIDE a class -- the class row must move by exactly the planted bytes and the
///     residual must not move.
///
/// The second half is what stops the first from passing on an instrument that simply counts
/// everything as unclassified, which is precisely the state the read path was in until the read
/// class was added.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn the_residual_recovers_a_planted_marker_exactly() {
    // A width no other allocation in the span is likely to take, so a recovered figure matching
    // it did not match by coincidence.
    const PLANT_BYTES: usize = 811_237;

    let span = crate::alloc_probe::ClassSpan::open();
    let plant: Vec<u8> = Vec::with_capacity(PLANT_BYTES);
    std::hint::black_box(&plant);
    drop(plant);
    let unclassified = span.close().expect("the counting allocator must be installed");

    println!(
        "\n  PLANT outside a class: residual {} allocs {} B, classes {} allocs {} B",
        unclassified.residual_allocs(),
        unclassified.residual_bytes(),
        unclassified.classes.summed().allocs,
        unclassified.classes.summed().alloc_bytes,
    );
    assert_eq!(
        unclassified.residual_bytes(),
        PLANT_BYTES as i64,
        "the residual must recover the planted bytes exactly"
    );
    assert_eq!(
        unclassified.residual_allocs(),
        1,
        "the planted vector is one allocation and the span must see exactly one"
    );
    assert_eq!(
        unclassified.classes.summed().alloc_bytes,
        0,
        "nothing was planted inside a class, so no class row may move"
    );

    // The control: the same plant, inside a class. It must land in the row and NOT in the
    // residual, which is what proves the residual measures attribution rather than volume.
    let span = crate::alloc_probe::ClassSpan::open();
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::CacheRead, || {
        let plant: Vec<u8> = Vec::with_capacity(PLANT_BYTES);
        std::hint::black_box(&plant);
        drop(plant);
    });
    let classified = span.close().expect("the counting allocator must be installed");

    println!(
        "  PLANT inside a class:  residual {} allocs {} B, cache_read {} allocs {} B",
        classified.residual_allocs(),
        classified.residual_bytes(),
        classified.classes.row(crate::alloc_probe::AllocClass::CacheRead).allocs,
        classified.classes.row(crate::alloc_probe::AllocClass::CacheRead).alloc_bytes,
    );
    assert_eq!(
        classified.classes.row(crate::alloc_probe::AllocClass::CacheRead).alloc_bytes,
        PLANT_BYTES as u64,
        "the planted bytes must land in the class that was entered"
    );
    assert_eq!(
        classified.residual_bytes(),
        0,
        "a plant inside a class must leave the residual untouched"
    );
}

// ---------------------------------------------------------------------------------------------
// The two guards below run in an ORDINARY build. Everything above needs the counting allocator,
// which a normal `cargo test` does not install, so nothing above would catch a regression on its
// own. These two do.
// ---------------------------------------------------------------------------------------------

/// A WARM SERVING READ IS ANSWERED FROM MEMORY AND NEVER REACHES THE SSD TIER.
///
/// This is the fact that decides whether the cache crate's read-side savings reach this store,
/// and it is the reason they largely do not. Those savings are in `ssd_store_key`, which builds
/// the SSD tier's key for an entry -- so they are paid back only by a read that descends to that
/// tier. A warm serving read does not descend, and a pin A/B measured the difference it makes as
/// exactly 0.00 allocations on this arm against exactly 8.00 on the two arms that do descend.
///
/// If a change ever made the warm path probe the SSD tier -- writing the command answer with a
/// tiered `put` instead of `put_memory_only`, say -- this fails, and the reading of every
/// allocation figure in this module changes with it.
///
/// THE BAND EXCLUDES THE VALUE IT GUARDS AGAINST: the failure is `disk_hits > 0`, and the
/// assertion is `disk_hits == 0`, so no value can satisfy both. `memory_hits` is floored above
/// zero for the same reason in the other direction -- an arm that served nothing at all would
/// otherwise pass the first half on a technicality.
#[test]
fn a_warm_serving_read_is_answered_from_memory_and_never_reaches_the_ssd_tier() {
    const KEYS: usize = 64;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let names: Vec<String> = (0..KEYS).map(|index| format!("tenant/7/object/{index:09}")).collect();
    for name in &names {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: name.clone(), value: vec![118u8; 1_024] },
        });
    }
    // Warm: after this every answer is in the command-answer cache.
    for name in &names {
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: name.clone() },
        });
    }

    let before = engine.cache().stats();
    for name in &names {
        let answer = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: name.clone() },
        });
        // The reads must actually be SERVING something. A shed or empty answer would satisfy
        // every counter assertion below while measuring nothing at all.
        match answer.response {
            CommandResponse::Bytes { value: Some(ref bytes) } => {
                assert_eq!(bytes.len(), 1_024, "a served answer must carry the stored value");
            }
            other => panic!("a warm read of a present key answered {other:?}"),
        }
    }
    let after = engine.cache().stats();

    let memory_hits = after.memory_hits.saturating_sub(before.memory_hits);
    let disk_hits = after.disk_hits.saturating_sub(before.disk_hits);
    let misses = after.misses.saturating_sub(before.misses);
    println!(
        "  warm serving reads: {KEYS} requests -> memory_hits {memory_hits}, \
         disk_hits {disk_hits}, misses {misses}"
    );

    assert!(
        memory_hits >= KEYS as u64,
        "{KEYS} warm reads produced {memory_hits} memory hits; the command-answer cache is not \
         answering and every read figure in this module is measuring a miss"
    );
    assert_eq!(
        disk_hits, 0,
        "a warm serving read reached the SSD tier {disk_hits} times. The cache crate's read-side \
         savings live in the SSD tier's key builder, so this is the difference between them \
         arriving and not arriving, and it has changed"
    );
    assert_eq!(
        misses, 0,
        "a warm serving read missed {misses} times, so the warm arm is not warm"
    );

    // ONE SERVING READ IS ONE CACHE READ when the answer is cached, and this is the assertion
    // that says so. It exists because a mutation found the hole: making `cached_response` throw
    // its hit away and recompute changes NEITHER the answer -- the source recomputes the same
    // bytes -- NOR `memory_hits`, because the lookup still happens. Two paths, one answer, and
    // nothing that compares answers can see the difference. What moves is the COUNT: the
    // recompute reaches `read_block_bytes` and the page cache, so the store answers one request
    // with two cache reads instead of one.
    let cache_reads = memory_hits + disk_hits + misses;
    assert_eq!(
        cache_reads, KEYS as u64,
        "{KEYS} warm requests made {cache_reads} cache reads. One cached answer is one cache \
         read; more than that means the command-answer cache is being consulted and then not \
         used, and the page underneath is being read anyway"
    );
}

/// EVERY PRODUCTION CACHE READ IS CHARGED TO THE READ CLASS, AND THERE IS NO EXEMPTION LIST.
///
/// The compiler cannot refuse an uncounted cache read outright -- `MultiLayerCache` is named in
/// several hundred signatures in this crate and its read methods are public -- so this enumerates
/// them instead, in the same shape as the counted-handle guard over the index log.
///
/// What it does NOT do is carry a list of sites that are allowed to be uncharged. Compaction's
/// residency probe was the one site that would have needed an entry, and it was charged instead,
/// because a list of excused sites is where a site goes to stop being looked at.
///
/// The scan joins wrapped method chains before matching. Two of the four sites put the receiver
/// on one line and the call on the next, and a line-at-a-time matcher reports those as absent --
/// which is a clean pass over a scan that cannot see half its subject.
///
/// `a_new_uncharged_cache_read_would_fail_this_guard` below is the negative control.
#[test]
fn every_production_cache_read_is_charged_to_the_read_class() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = 0usize;
    let mut lines = 0usize;
    let mut excluded = 0usize;
    let mut sites: Vec<(String, usize, bool, String)> = Vec::new();

    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let Ok(display) = path.strip_prefix(&root) else { continue };
            let display = display.to_string_lossy().replace('\\', "/");
            // The engine owns the MultiLayerCache; the compiler enumeration found every
            // production read door inside it. Test modules are taken out and the removals
            // counted, so an exclusion that started matching everything shows up as a collapsed
            // denominator rather than as a clean pass.
            let is_engine = display == "engine.rs" || display.starts_with("engine/");
            let is_test = display.contains("/tests/") || display.ends_with("tests.rs");
            if !is_engine {
                continue;
            }
            if is_test {
                excluded += 1;
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else { continue };
            files += 1;
            let joined = join_method_chains(&text);
            lines += joined.len();
            for (index, (number, content)) in joined.iter().enumerate() {
                if !names_a_cache_read(content) {
                    continue;
                }
                let start = index.saturating_sub(3);
                let charged = joined[start..=index]
                    .iter()
                    .any(|(_, near)| near.contains("AllocClass::CacheRead"));
                sites.push((display.clone(), *number, charged, content.clone()));
            }
        }
    }

    for (file, number, charged, content) in &sites {
        println!(
            "  {:<9} {file}:{number}  {}",
            if *charged { "CHARGED" } else { "UNCHARGED" },
            content.chars().take(90).collect::<String>()
        );
    }
    println!("  files {files}, lines {lines}, sites {}, excluded {excluded}", sites.len());

    // Denominators before verdicts. Every assertion below is about a set, and an empty set
    // satisfies all of them dishonestly.
    assert!(
        files >= 20,
        "the walk read {files} engine files; this guard would be passing over nothing"
    );
    assert!(
        lines >= 20_000,
        "the walk read {lines} lines, which is not this engine"
    );
    assert!(
        excluded >= 10,
        "only {excluded} files were excluded as tests; the exclusion has stopped matching and \
         this scan is now reading test helpers as production"
    );
    assert!(
        sites.len() >= 4,
        "the scan found {} cache read sites; the compiler enumeration behind this module found \
         four, and a scan that finds fewer has stopped seeing its subject",
        sites.len()
    );

    let uncharged: Vec<String> = sites
        .iter()
        .filter(|(_, _, charged, _)| !charged)
        .map(|(file, number, _, content)| format!("{file}:{number}  {content}"))
        .collect();
    assert!(
        uncharged.is_empty(),
        "these production cache reads are charged to no allocation class, so a serving read's \
         allocations land in the residual with nothing able to say what they were for:\n{}",
        uncharged.join("\n")
    );
}

/// The negative control for the guard above: it must fail on a read that is not charged, and
/// pass on one that is. Without this, a scan that had stopped matching anything at all would
/// report the same clean result as a tree with nothing wrong in it.
#[test]
fn a_new_uncharged_cache_read_would_fail_this_guard() {
    let uncharged = "fn added_later(cache: &MultiLayerCache) -> Option<Vec<u8>> {\n\
                     \x20   cache.get(&key).ok().flatten()\n\
                     }\n";
    let found = join_method_chains(uncharged)
        .into_iter()
        .filter(|(_, content)| names_a_cache_read(content))
        .count();
    assert_eq!(found, 1, "the scan must see a newly added cache read");

    let charged = "fn added_later(cache: &MultiLayerCache) -> Option<Vec<u8>> {\n\
                   \x20   crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::CacheRead, || {\n\
                   \x20       cache.get(&key)\n\
                   \x20   }).ok().flatten()\n\
                   }\n";
    let joined = join_method_chains(charged);
    let site = joined
        .iter()
        .position(|(_, content)| names_a_cache_read(content))
        .expect("the scan must see the charged read too");
    let start = site.saturating_sub(3);
    assert!(
        joined[start..=site].iter().any(|(_, near)| near.contains("AllocClass::CacheRead")),
        "a read inside the class scope must read as charged"
    );

    // And the matcher must not fire on a map named `..._cache`, which is what a left boundary is
    // for: `packed_block_cache.get(address)` is a HashMap lookup in `packed_pages.rs`.
    assert!(
        !names_a_cache_read("if let Some(points) = packed_block_cache.get(address) {"),
        "the matcher must not read a map whose name ends in `cache` as a cache read"
    );
    // A doc comment quoting the call is not a call.
    assert!(
        join_method_chains("///     if let Ok(Some(bytes)) = cache.get(&key) { }\n")
            .into_iter()
            .all(|(_, content)| !names_a_cache_read(&content)),
        "a line of documentation quoting the call must not read as one"
    );
}

/// Whether one logical line names a read of the serving cache.
///
/// The receiver must be `cache` with a non-word character before it, so `packed_block_cache.get`
/// and `route_cache.get` -- both maps -- are not read as cache reads. `self.cache.get` is, which
/// is why the boundary is "not a word character" rather than "start of line".
fn names_a_cache_read(content: &str) -> bool {
    for door in ["get", "get_shared", "get_memory"] {
        let needle = format!("cache.{door}(");
        let mut from = 0usize;
        while let Some(at) = content[from..].find(&needle) {
            let at = from + at;
            let boundary = at == 0
                || !content[..at]
                    .chars()
                    .next_back()
                    .map(|c| c.is_alphanumeric() || c == '_')
                    .unwrap_or(false);
            if boundary {
                return true;
            }
            from = at + 1;
        }
    }
    false
}

/// Source lines with rustfmt's wrapped method chains joined onto the line the chain starts on.
///
/// Returns one entry per FILE line so that reported numbers are file line numbers; a line that
/// was folded into its predecessor comes back empty rather than being dropped, which keeps the
/// three-line window above a site meaning three source lines.
fn join_method_chains(text: &str) -> Vec<(usize, String)> {
    let mut out: Vec<(usize, String)> = Vec::new();
    for (number, raw) in text.lines().enumerate() {
        let stripped = raw.trim();
        if stripped.starts_with("//") {
            out.push((number + 1, String::new()));
            continue;
        }
        if stripped.starts_with('.') {
            if let Some(last) = out.iter_mut().rev().find(|(_, content)| !content.is_empty()) {
                last.1.push_str(stripped);
                out.push((number + 1, String::new()));
                continue;
            }
        }
        out.push((number + 1, stripped.to_string()));
    }
    out
}
