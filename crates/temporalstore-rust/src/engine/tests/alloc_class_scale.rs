// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A STORED RECORD'S MEMORY IS SPENT ON, and whether each part is flat per record.
//!
//! The counting allocator reported one undifferentiated total. At four thousand records "6.0
//! allocations per record" is a fine summary; at a large corpus the same number describes a store
//! whose index is flat per record and one whose index is outgrowing its data, and those two want
//! opposite fixes. `alloc_probe::AllocClass` splits the same allocations by the sink they land in.
//!
//! THE BILL. Ingest of 1,024-byte incompressible values through `batch_execute`, 100 to a batch,
//! debug profile, counted with `--features alloc-probe`:
//!
//! ```text
//!   class                allocs/record   bytes/record   ratio 2,000 -> 20,000
//!   cache_invalidation          35.06            995           1.000
//!   slab_append                  6.00          1,192           1.000
//!   bucket_index                 5.63            676           1.000
//!   staged_outcome               2.06            252           1.000
//!   page_bytes                   2.00          2,115           1.000
//!   dirty_objects                1.31            121           0.999
//!   index_log_delta              1.26            372           1.013
//!   carried_page                 1.06          1,065           1.000
//!   log_record                   0.03          2,386           0.968
//!   nine classes summed         54.42          9,172
//!   SPAN TOTAL                  80.69         11,933           <- independent counter
//!   residual                    26.27          2,761           1.000
//! ```
//!
//! EVERY CLASS IS FLAT PER RECORD ACROSS A TEN-TIMES CORPUS, and so is the residual. Nothing in
//! this write path grows with the size of the store. That is the finding; the classes exist so a
//! later run can say which row stopped being flat.
//!
//! THE TWO COLUMNS DISAGREE ABOUT WHAT MATTERS, which is why both are kept. By CALLS the biggest
//! thing a write does is invalidate one serving-cache key -- 35.06 of the 80.69, more than the
//! other eight classes together. By BYTES that class is fifth, and the biggest is the write-ahead
//! record, which makes 0.03 allocations a record because it is built as whole buffers. A report
//! with only one of these columns gives the opposite answer about where to look.
//!
//! THE CLASSES WERE MEASURED, NOT CHOSEN. Four candidates were scoped, run, and dropped:
//!
//!   * `exec_apply`, everything in `execute_on_shard` outside the named classes: 2.00 allocations
//!     and 15 bytes a record. A catch-all, not a sink; it belongs in the residual.
//!   * `model_map`, the `shard.strings` insert: 1.00 allocations and 276 bytes a record, real but
//!     with NO CHOKE POINT -- fourteen command arms insert into their own map inline. A class
//!     counted at one arm of fourteen is the defect this exercise is about, not a measurement, so
//!     it is in the residual and named here instead.
//!   * `command_clone`, the batch path's write-ahead append around the record encode: 0.02
//!     allocations and 12 bytes a record. Near zero, so it would be noise in every future report.
//!   * per-key CONTEXT. This engine's `context_*` maps are not touched by a value write at all --
//!     they belong to the context commands -- so a class for them would be a row of zeros in every
//!     line of this table.
//!
//! and one candidate was moved rather than dropped: an `execute_on_shard` scope placed at the
//! single-command CALL SITE read exactly ZERO on this workload, because `batch_execute` is a
//! second live caller of the same function. That is the whole argument for charging inside the
//! callee, arrived at from the other direction.
//!
//! WHAT THE RESIDUAL IS. `ClassSpan` measures the span twice: once with the process-wide counter,
//! which the class rows do not feed, and once as the rows. The difference is a reading and can be
//! wrong. A residual computed from the rows it audits cannot fail, and this crate shipped one --
//! `restore_scale`'s allocation residual read zero "by construction" until it was measured and
//! turned out to be five.
//!
//! WHAT THIS ACCOUNTING CANNOT SEE, stated here because a total that silently omits something is
//! worse than a smaller total that says so:
//!
//!   * Anything not allocated through Rust's global allocator. The serving cache is a separate
//!     crate: the bytes this engine hands it are counted as it hands them over, and whatever that
//!     crate then does with them is not.
//!   * Memory-mapped regions and the OS page cache holding slab files. A slab read the kernel
//!     serves from cache costs this accounting the destination buffer and nothing else.
//!   * Stack, static and thread-local storage.
//!   * Allocator retention. These are allocation CALLS and the bytes asked for, not resident set:
//!     71% of one measured proxy's resident memory was memory the allocator had kept rather than
//!     live data, which is the reason this probe exists instead of a resident-size delta.
//!   * Other threads. The class tag is thread-local, so a background thread's allocations land in
//!     no class and inflate the residual rather than a row.
//!
//! and one it can see only approximately: a free is charged to the class in scope WHEN THE FREE
//! HAPPENS. Inside a narrow synchronous primitive that is the class that allocated it, but a block
//! allocated in one class and released in another is charged to the releasing one. The allocated
//! columns carry no such caveat; nothing here asserts on `outstanding` alone.
#![allow(clippy::all)]
// The measuring tests exist only with `alloc-probe`, so their sizes and helpers go unread in an
// ordinary build.
#![allow(dead_code)]
use super::*;

const SMALL: usize = 2_000;
const LARGE: usize = 20_000;
const VALUE_BYTES: usize = 1_024;
const BATCH: usize = 100;

/// xorshift64*, seeded by index, so no two records share a payload.
///
/// A repeated byte compresses, and the biggest class by bytes is a compressing encode: a corpus of
/// identical values would report the page-byte class at a fraction of its real size and call that
/// a measurement.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Guards that hold in EVERY build, including one with no counting allocator at all.
// ---------------------------------------------------------------------------------------------

/// Every class owns one row, the rows are dense, and the table is as long as the classes are.
///
/// The compile errors do most of this -- a class with no `slot` and no `label` does not build --
/// but neither of those catches two classes sharing a row, which would make one of them silently
/// report the other's numbers.
#[test]
fn alloc_class_slots_are_dense_and_in_order() {
    use crate::alloc_probe::AllocClass;
    let mut labels: Vec<&'static str> = Vec::new();
    for (index, class) in AllocClass::ALL.iter().enumerate() {
        assert_eq!(
            index,
            AllocClass::ALL
                .iter()
                .position(|candidate| candidate == class)
                .expect("a class in ALL is findable in ALL"),
            "{} appears twice in ALL, so one of its rows is unreachable",
            class.label()
        );
        labels.push(class.label());
    }
    labels.sort_unstable();
    let before = labels.len();
    labels.dedup();
    assert_eq!(
        before,
        labels.len(),
        "two allocation classes share a label, so a report cannot tell them apart"
    );
    assert!(
        AllocClass::ALL.len() >= 6,
        "there were nine classes when this was written; {} is not the table this file documents",
        AllocClass::ALL.len()
    );

    // AND THE SLOTS THEMSELVES, which this test is named for and did not look at.
    //
    // Everything above compares ENTRIES and LABELS. Two classes can be distinct entries with
    // distinct labels and still return the same slot, and then they share one counter row: the
    // later one's charges land on the earlier one, so a report double-counts the first and shows
    // the second as a flat zero in every column. Nothing here noticed. A mutant that pointed a
    // tenth class at the ninth's slot survived this test and every other test in a wide slice
    // around it, which is how it was found.
    //
    // Dense and in order, both asserted: slot `i` belongs to `ALL[i]`.
    for (index, class) in AllocClass::ALL.iter().enumerate() {
        assert_eq!(
            class.slot(),
            index,
            "{} sits at slot {} but is entry {index} of ALL; the ledger indexes its rows by \
             position, so this class and whichever one owns slot {} would share a row",
            class.label(),
            class.slot(),
            class.slot()
        );
    }
}

/// Every declared class is entered by production code.
///
/// THE DEFECT THIS EXISTS FOR. Six counters in this crate have been declared and then incremented
/// at a fraction of their call sites or at none of them; the most recent was declared, reset,
/// read, reported and never incremented, with a document naming the floor it was supposed to hold.
/// A class is worse than a bare counter in that respect, because its row still adds up -- it
/// contributes zero to a sum that reconciles perfectly without it.
///
/// So: the source is scanned for a scope naming each class, in a file that is neither a test nor
/// the module that declares them. A class nobody enters fails here, by name.
///
/// THE CONTROL IS THE PLANTED COUNT. Nine classes are declared and this scan must find a site for
/// every one of them; a scan that had broken finds none, and a scan whose exclusion had started
/// matching everything reports a denominator that collapsed. Both are asserted before the verdict.
#[test]
fn every_alloc_class_has_a_production_scope() {
    use crate::alloc_probe::AllocClass;
    use std::path::Path;

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![root];
    let mut files_scanned = 0usize;
    let mut lines_scanned = 0usize;
    let mut excluded = 0usize;
    let mut sites: Vec<(String, String)> = Vec::new();

    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
                continue;
            }
            if entry_path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let display = entry_path.display().to_string();
            // The declaring module names every class in its own `slot` and `label` matches, and a
            // test file naming one is not production code entering it. Both are taken out, and the
            // removals are counted so an exclusion that started matching everything shows up as a
            // denominator that collapsed rather than as a clean pass.
            let is_declaration =
                entry_path.file_name().and_then(|n| n.to_str()) == Some("alloc_probe.rs");
            let is_test = display.contains("/tests/")
                || display.ends_with("tests.rs")
                || display.contains("/tests.rs");
            if is_declaration || is_test {
                excluded += 1;
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&entry_path) else {
                continue;
            };
            files_scanned += 1;
            lines_scanned += text.lines().count();
            for class in AllocClass::ALL.iter() {
                let needle = format!("AllocClass::{}", variant_name(*class));
                if text.contains(&needle) {
                    sites.push((class.label().to_string(), display.clone()));
                }
            }
        }
    }

    // Denominators before verdicts. Every assertion below is about a set, and an empty set
    // satisfies none of them honestly.
    assert!(
        files_scanned > 100,
        "the source walk read {files_scanned} production files; this guard would pass over nothing"
    );
    assert!(
        lines_scanned > 50_000,
        "the source walk read {lines_scanned} lines, which is not this crate"
    );
    assert!(
        excluded >= 20,
        "only {excluded} files were excluded as tests or as the declaring module; the exclusion \
         has stopped matching and this scan is now reading its own declarations"
    );

    let mut missing: Vec<&'static str> = Vec::new();
    for class in AllocClass::ALL.iter() {
        if !sites.iter().any(|(label, _)| label == class.label()) {
            missing.push(class.label());
        }
    }
    assert!(
        missing.is_empty(),
        "{:?} are declared as allocation classes and no production file enters them, so their rows \
         read zero in every report and the sum reconciles without them. Scanned {files_scanned} \
         files, {lines_scanned} lines, found {} scope sites",
        missing,
        sites.len()
    );
    assert!(
        sites.len() >= AllocClass::ALL.len(),
        "found {} scope sites for {} classes",
        sites.len(),
        AllocClass::ALL.len()
    );
}

/// The enum variant's spelling, which is what a scope in the source says.
///
/// Exhaustive on purpose: a class added to the enum stops this file compiling until it is named
/// here, which is the same compile error the ledger's `slot` match raises.
fn variant_name(class: crate::alloc_probe::AllocClass) -> &'static str {
    use crate::alloc_probe::AllocClass;
    match class {
        AllocClass::PageBytes => "PageBytes",
        AllocClass::CarriedPage => "CarriedPage",
        AllocClass::SlabAppend => "SlabAppend",
        AllocClass::BucketIndex => "BucketIndex",
        AllocClass::DirtyObjects => "DirtyObjects",
        AllocClass::StagedOutcome => "StagedOutcome",
        AllocClass::LogRecord => "LogRecord",
        AllocClass::IndexLogDelta => "IndexLogDelta",
        AllocClass::CacheInvalidation => "CacheInvalidation",
        AllocClass::CacheRead => "CacheRead",
        AllocClass::RecencyStamp => "RecencyStamp",
    }
}

/// Without the counting allocator, the ledger says NOTHING rather than zero.
///
/// This is the trap the feature gate creates: every counter reads zero in a build that is not
/// counting, and a table of zeros looks exactly like a store that allocates nothing. The `Option`
/// is what makes the two distinguishable, and this holds that it really is `None` in the build
/// where it matters -- which is this one, the ordinary gate build.
#[cfg(not(feature = "alloc-probe"))]
#[test]
fn an_uninstrumented_build_reports_no_classes_rather_than_a_table_of_zeros() {
    assert!(
        crate::alloc_probe::classified_now().is_none(),
        "the counting allocator is not installed in this build, so the per-class ledger must \
         answer None; a Some full of zeros would be read as a measurement"
    );
    assert!(
        crate::alloc_probe::counted_now().is_none(),
        "the whole-span counter must answer None in the same build, or a span could report a \
         total with no classes under it and look like a total residual"
    );
    // And the scope still runs the work it is given, so a production path wrapped in one behaves
    // identically in the build that does not count.
    let mut sink = 0usize;
    let out = crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::PageBytes, || {
        sink += 1;
        7usize
    });
    assert_eq!(7, out);
    assert_eq!(1, sink);
}

// ---------------------------------------------------------------------------------------------
// The instrument's own controls, then the measurement.
// ---------------------------------------------------------------------------------------------

/// PLANT A KNOWN NUMBER AND RECOVER EXACTLY THAT NUMBER.
///
/// A probe that reports a plausible number for something whose answer is not in question is the
/// only evidence its numbers mean anything. `Vec::with_capacity` on a non-zero size is exactly one
/// allocation and its drop is exactly one free, so the count here is not approximate and the
/// assertion is an equality rather than a bound.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_class_probe_recovers_exactly_the_allocations_planted_in_a_class() {
    use crate::alloc_probe::{AllocClass, ClassSpan};
    const PLANTED: usize = 64;
    const EACH: usize = 4096;

    let span = ClassSpan::open();
    crate::alloc_probe::in_class(AllocClass::PageBytes, || {
        for _ in 0..PLANTED {
            let sink: Vec<u8> = Vec::with_capacity(EACH);
            assert_eq!(EACH, sink.capacity());
            drop(sink);
        }
    });
    let counts = span
        .close()
        .expect("built with `alloc-probe`, so the ledger is a measurement");

    let row = counts.classes.row(AllocClass::PageBytes);
    assert_eq!(
        PLANTED as u64, row.allocs,
        "planted {PLANTED} allocations in one class and the row recovered {}",
        row.allocs
    );
    assert_eq!(
        PLANTED as u64, row.frees,
        "planted {PLANTED} frees in one class and the row recovered {}",
        row.frees
    );
    assert_eq!(
        (PLANTED * EACH) as u64,
        row.alloc_bytes,
        "planted {} bytes and the row recovered {}",
        PLANTED * EACH,
        row.alloc_bytes
    );
    // The other rows must not have moved. A scope that leaked into its neighbours would still
    // reconcile against the total, so the total cannot catch this.
    for class in AllocClass::ALL.iter() {
        if *class == AllocClass::PageBytes {
            continue;
        }
        assert_eq!(
            0,
            counts.classes.row(*class).allocs,
            "{} moved during a span that only entered page_bytes",
            class.label()
        );
    }
}

/// NEGATIVE CONTROL: the same allocations OUTSIDE a scope move no row at all.
///
/// Without this, a probe that charged everything to the last class it saw would pass the positive
/// control exactly as a correct one does.
#[cfg(feature = "alloc-probe")]
#[test]
fn allocations_outside_every_class_move_no_row() {
    use crate::alloc_probe::{AllocClass, ClassSpan};
    const PLANTED: usize = 64;

    // Enter and leave a class first, so this also proves the scope is RESTORED rather than merely
    // set -- a scope that never put the previous class back would charge everything after it,
    // which is the failure this ordering is chosen to expose.
    crate::alloc_probe::in_class(AllocClass::BucketIndex, || {
        let sink: Vec<u8> = Vec::with_capacity(256);
        drop(sink);
    });

    let span = ClassSpan::open();
    for _ in 0..PLANTED {
        let sink: Vec<u8> = Vec::with_capacity(4096);
        drop(sink);
    }
    let counts = span.close().expect("built with `alloc-probe`");

    assert!(
        counts.total.allocs >= PLANTED as u64,
        "the whole-span counter saw {} allocations for {PLANTED} planted, so the span total is not \
         measuring either",
        counts.total.allocs
    );
    assert_eq!(
        0,
        counts.classes.summed().allocs,
        "{PLANTED} allocations made outside every scope were charged to a class: {}",
        counts.classes.report_line()
    );
    assert_eq!(
        counts.total.allocs as i64,
        counts.residual_allocs(),
        "everything in this span is residual by construction of the test"
    );
}

/// Nesting: the innermost scope owns the allocation, and the outer one resumes afterwards.
///
/// This is what lets the carried-page copy and the payload encode be told apart from the slab
/// append they both happen inside, which is the one place classes are genuinely nested in
/// production.
#[cfg(feature = "alloc-probe")]
#[test]
fn a_nested_class_takes_the_allocation_and_gives_the_scope_back() {
    use crate::alloc_probe::{AllocClass, ClassSpan};
    let span = ClassSpan::open();
    crate::alloc_probe::in_class(AllocClass::SlabAppend, || {
        let outer_first: Vec<u8> = Vec::with_capacity(1024);
        crate::alloc_probe::in_class(AllocClass::CarriedPage, || {
            let inner: Vec<u8> = Vec::with_capacity(2048);
            drop(inner);
        });
        let outer_second: Vec<u8> = Vec::with_capacity(1024);
        drop(outer_first);
        drop(outer_second);
    });
    let counts = span.close().expect("built with `alloc-probe`");
    assert_eq!(
        2,
        counts.classes.row(AllocClass::SlabAppend).allocs,
        "the outer class should hold its own two allocations and not the nested one"
    );
    assert_eq!(
        1,
        counts.classes.row(AllocClass::CarriedPage).allocs,
        "the nested class should hold exactly the allocation made inside it"
    );
    assert_eq!(
        2048,
        counts.classes.row(AllocClass::CarriedPage).alloc_bytes,
        "the nested class should hold exactly the bytes asked for inside it"
    );
}

/// Entering the same class twice, nested, charges it once.
///
/// The bucket-index primitive re-enters `staged_outcome` around the item it builds while
/// `stage_outcome` enters the same class again inside, so this is a production arrangement and not
/// a hypothetical.
#[cfg(feature = "alloc-probe")]
#[test]
fn re_entering_the_same_class_does_not_double_charge_it() {
    use crate::alloc_probe::{AllocClass, ClassSpan};
    let span = ClassSpan::open();
    crate::alloc_probe::in_class(AllocClass::StagedOutcome, || {
        crate::alloc_probe::in_class(AllocClass::StagedOutcome, || {
            let sink: Vec<u8> = Vec::with_capacity(1024);
            drop(sink);
        });
    });
    let counts = span.close().expect("built with `alloc-probe`");
    assert_eq!(
        1,
        counts.classes.row(AllocClass::StagedOutcome).allocs,
        "one allocation inside two nested scopes of the same class should be charged once"
    );
    assert_eq!(
        1024,
        counts.classes.row(AllocClass::StagedOutcome).alloc_bytes
    );
}

/// A panic inside a scoped primitive must not leave the class set for the rest of the thread.
#[cfg(feature = "alloc-probe")]
#[test]
fn a_panic_inside_a_class_gives_the_scope_back() {
    use crate::alloc_probe::{AllocClass, ClassSpan};
    let unwound = std::panic::catch_unwind(|| {
        crate::alloc_probe::in_class(AllocClass::DirtyObjects, || {
            let sink: Vec<u8> = Vec::with_capacity(512);
            drop(sink);
            panic!("deliberate");
        })
    });
    assert!(unwound.is_err(), "the panic should have propagated");
    let span = ClassSpan::open();
    let sink: Vec<u8> = Vec::with_capacity(4096);
    drop(sink);
    let counts = span.close().expect("built with `alloc-probe`");
    assert_eq!(
        0,
        counts.classes.summed().allocs,
        "a panic left a class in scope, so everything after it is charged to {}",
        counts.classes.report_line()
    );
}

// ---------------------------------------------------------------------------------------------
// The measurement.
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "alloc-probe")]
struct Ingest {
    records: usize,
    value_bytes: usize,
    counts: crate::alloc_probe::ClassifiedCounts,
}

/// Ingest `records` values of `value_bytes` into a fresh shard, measured as one span.
///
/// The engine is built and the shard loaded BEFORE the span opens, so the fixed cost of standing a
/// store up is not divided by the record count and reported as a per-record figure.
#[cfg(feature = "alloc-probe")]
fn ingest(
    records: usize,
    value_bytes: usize,
    batch: usize,
) -> (Ingest, tempfile::TempDir) {
    use crate::alloc_probe::ClassSpan;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // One write before the span, so a lazily built table created on first use is not charged to
    // whichever class happens to run first.
    engine.execute(crate::types::ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "warm".to_string(),
            value: incompressible(value_bytes, u64::MAX),
        },
    });

    // THE CORPUS IS BUILT BEFORE THE SPAN OPENS. A probe that builds its commands inside the
    // measured window charges the store for its own fixture: this crate's write-ahead budget read
    // three allocations a write too high for four rounds because the command was assembled inside
    // the window, and a measurement wrong by a constant still moves the right way after every
    // change, which feels like confirmation and is no evidence at all.
    let mut batches: Vec<Vec<Command>> = Vec::new();
    let mut index = 0usize;
    while index < records {
        let end = (index + batch).min(records);
        let mut commands = Vec::with_capacity(end - index);
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: incompressible(value_bytes, cursor as u64),
            });
            cursor += 1;
        }
        batches.push(commands);
        index = end;
    }

    let span = ClassSpan::open();
    for commands in batches.drain(..) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "ingest failed: {:?}", response.status);
    }
    let counts = span
        .close()
        .expect("built with `alloc-probe`, so these are measurements");
    drop(engine);
    (
        Ingest {
            records,
            value_bytes,
            counts,
        },
        dir,
    )
}

#[cfg(feature = "alloc-probe")]
fn print_table(measurement: &Ingest) {
    use crate::alloc_probe::AllocClass;
    let records = measurement.records as f64;
    println!(
        "\n{} records of {} bytes",
        measurement.records, measurement.value_bytes
    );
    println!(
        "  {:<20} {:>12} {:>9} {:>14} {:>11} {:>13}",
        "class", "allocs", "per rec", "alloc bytes", "B per rec", "outstanding"
    );
    for class in AllocClass::ALL.iter() {
        let row = measurement.counts.classes.row(*class);
        println!(
            "  {:<20} {:>12} {:>9.3} {:>14} {:>11.1} {:>13}",
            class.label(),
            row.allocs,
            row.allocs as f64 / records,
            row.alloc_bytes,
            row.alloc_bytes as f64 / records,
            row.outstanding()
        );
    }
    let summed = measurement.counts.classes.summed();
    println!(
        "  {:<20} {:>12} {:>9.3} {:>14} {:>11.1} {:>13}",
        "classes summed",
        summed.allocs,
        summed.allocs as f64 / records,
        summed.alloc_bytes,
        summed.alloc_bytes as f64 / records,
        summed.outstanding()
    );
    println!(
        "  {:<20} {:>12} {:>9.3} {:>14} {:>11.1} {:>13}   <- independent counter",
        "SPAN TOTAL",
        measurement.counts.total.allocs,
        measurement.counts.total.allocs as f64 / records,
        measurement.counts.total.alloc_bytes,
        measurement.counts.total.alloc_bytes as f64 / records,
        measurement.counts.total.outstanding()
    );
    println!(
        "  {:<20} {:>12} {:>9.3} {:>14} {:>11.1}                 ({:.1}% of bytes and {:.1}% of \
         calls classified)",
        "residual",
        measurement.counts.residual_allocs(),
        measurement.counts.residual_allocs() as f64 / records,
        measurement.counts.residual_bytes(),
        measurement.counts.residual_bytes() as f64 / records,
        measurement.counts.classified_byte_share() * 100.0,
        measurement.counts.classified_call_share() * 100.0
    );
}

/// What a whole ingest allocated, class by class, checked against a counter the classes do not
/// feed, at two corpus sizes.
///
/// Two sizes and not one, because a fixed cost divides away and genuine drift climbs: a residual
/// that is the same number per record at both sizes is the probe's own edges, and one that grows
/// with the corpus is a sink nobody wrote down.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "builds two corpora; run with --features alloc-probe --ignored --nocapture"]
fn what_a_stored_record_allocates_by_class_at_two_corpus_sizes() {
    use crate::alloc_probe::AllocClass;

    let (small, _small_dir) = ingest(SMALL, VALUE_BYTES, BATCH);
    let (large, _large_dir) = ingest(LARGE, VALUE_BYTES, BATCH);
    print_table(&small);
    print_table(&large);

    println!("\n  per-record allocations, and the ratio across a 10x corpus");
    println!(
        "  {:<20} {:>12} {:>12} {:>8}",
        "class", "2,000", "20,000", "ratio"
    );
    let mut ratios: Vec<(&'static str, f64)> = Vec::new();
    for class in AllocClass::ALL.iter() {
        let small_per = small.counts.classes.row(*class).allocs as f64 / small.records as f64;
        let large_per = large.counts.classes.row(*class).allocs as f64 / large.records as f64;
        let ratio = if small_per == 0.0 {
            0.0
        } else {
            large_per / small_per
        };
        ratios.push((class.label(), ratio));
        println!(
            "  {:<20} {:>12.3} {:>12.3} {:>8.3}",
            class.label(),
            small_per,
            large_per,
            ratio
        );
    }

    // THE RECONCILIATION. The rows are checked against a counter they do not feed.
    for measurement in [&small, &large] {
        assert!(
            measurement.counts.total.allocs > 0,
            "the span total is zero at {} records, so nothing was counting",
            measurement.records
        );
        assert!(
            measurement.counts.residual_allocs() >= 0,
            "the classes claim {} allocations out of a span total of {} at {} records; a negative \
             residual is double counting, not an omission",
            measurement.counts.classes.summed().allocs,
            measurement.counts.total.allocs,
            measurement.records
        );
        assert!(
            measurement.counts.classified_byte_share() > 0.6,
            "only {:.1}% of the allocated bytes landed in a named class at {} records: {}",
            measurement.counts.classified_byte_share() * 100.0,
            measurement.records,
            measurement.counts.classes.report_line()
        );
        assert!(
            measurement.counts.classified_call_share() > 0.6,
            "only {:.1}% of the allocation CALLS landed in a named class at {} records: {}",
            measurement.counts.classified_call_share() * 100.0,
            measurement.records,
            measurement.counts.classes.report_line()
        );
        // Non-vacuity: every class this table prints must have moved on this workload, or the
        // report carries a row that is not a measurement.
        for class in AllocClass::ALL.iter() {
            assert!(
                measurement.counts.classes.row(*class).allocs > 0,
                "{} recorded no allocation at {} records, so it is a row of zeros in a report that \
                 otherwise reconciles",
                class.label(),
                measurement.records
            );
        }
    }

    // FLATNESS, which is the finding. Every class was flat per record across a ten-times corpus
    // when this was written; a class that stops being flat is the next thread's subject and this
    // is where it announces itself.
    for (label, ratio) in &ratios {
        assert!(
            *ratio > 0.5 && *ratio < 2.0,
            "{label} allocated {ratio:.3}x as much per record at 20,000 records as at 2,000; it \
             was flat at 1.00x when this was measured, so either the store now scales with itself \
             on this path or the class has stopped covering what it used to"
        );
    }

    // The residual PER RECORD must not climb with the corpus either. A fixed cost at the edges of
    // the span divides away; a sink that grows with the store does not, and that is the thing this
    // is watching for.
    let small_residual = small.counts.residual_allocs() as f64 / small.records as f64;
    let large_residual = large.counts.residual_allocs() as f64 / large.records as f64;
    assert!(
        large_residual <= small_residual * 1.5 + 1.0,
        "unattributed allocations per record went {small_residual:.3} -> {large_residual:.3} across \
         a 10x corpus, so there is a sink outside every class that grows with the store"
    );
}
