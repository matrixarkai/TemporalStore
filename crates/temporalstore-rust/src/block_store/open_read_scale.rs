// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN OPEN OF THIS STORE READS OFF DISK, AND THE ROW THAT SAID IT WAS NOTHING.
//!
//! #1931 measured a restore and found 965,415,651 bytes read by a reader with no counter.
//! #1936 named the write-ahead reader and counted it. #1948 named the index-log reader, counted
//! it, and named the last row still reading zero: THE BLOCK STORE'S. This is that row.
//!
//! IT READ ZERO FOR TWO REASONS, AND ONLY ONE OF THEM WAS THE EXPECTED ONE.
//!
//! First, the reading is in the OPEN, not in the load. `restore_large_scale.rs` builds the engine
//! -- and with it the block store -- and only then takes `pages_before` and opens the span it
//! measures. Every byte this store reads on a cold start has already been read by the time that
//! span begins, so the row it prints is a zero taken after the fact.
//!
//! Second, and this is the part widening the span would NOT have fixed: the open fed no counter
//! at all. `BlockStoreStats::bytes_read` counts records handed back through the four read entry
//! points, and an open hands back no record. A span drawn around the whole cold start would still
//! have printed zero.
//!
//! ```text
//!   a cold open: the slab manifest, then the slab inspection, at two corpus sizes
//!
//!                                            50 slabs      200 slabs     ratio
//!     records appended                          1,000          4,000     4.000
//!     slab files                                   50            200     4.000
//!     slab bytes on disk (stat)               140,000        560,000     4.000
//!     slab manifest on disk (stat)             12,760         51,360     4.025
//!     THE ROW, BEFORE: stats.bytes_read              0              0        --
//!     the open's own counter, after           152,760        611,360     4.002
//!     the kernel, for this process            152,760        611,360     4.002
//!     RESIDUAL, kernel less the counter              0              0
//!     reads it was charged                         51            201
//! ```
//!
//! ABSOLUTE, AND THE SHAPE IT WAS MEASURED IN. 152,760 and 611,360 BYTES, on a store built by
//! appending 20 records of 128 bytes per slab and rolling, `TS_REVERIFY_ALL_SLABS` unset, the
//! FIRST reopen after the build. Not a share: a share of a restore re-scales with the corpus, the
//! piece count and every fix landed since, and this campaign has already carried one forward and
//! had it re-measured an order of magnitude away.
//!
//! THE INSTRUMENT THE LAST TWO PRs USED CANNOT SEE THIS READER. `/proc/thread-self/io` is what
//! #1936 and #1948 both took their kernel figure from, because a restore runs on one thread while
//! the suite runs many. The slab inspection reads on threads it SPAWNS. Across the same two cold
//! opens the calling thread was charged 12,871 and 51,478 bytes -- the manifest, and not one byte
//! of any slab, 8.4% of the truth both times. The figures above are `/proc/self/io`, which
//! aggregates the whole thread group including threads that have since exited.

use super::*;

// ---------------------------------------------------------------------------------------------
// THE FIXTURE
// ---------------------------------------------------------------------------------------------

const RECORDS_PER_SLAB: u64 = 20;
const PAYLOAD: &[u8] = &[7u8; 128];

/// A store built the way a store gets large: real appends, real rolls.
fn build_appended_store(root: &std::path::Path, slabs: u64) -> u64 {
    let store = BlockStore::new(root);
    let mut slab = 0_u64;
    let mut records = 0_u64;
    while slab < slabs {
        let mut record = 0_u64;
        while record < RECORDS_PER_SLAB {
            store.append(PAYLOAD).expect("append");
            records += 1;
            record += 1;
        }
        // The last slab is not rolled: that would mint an empty slab carrying no record.
        if slab + 1 < slabs {
            store.roll_slab().expect("roll");
        }
        slab += 1;
    }
    records
}

/// What `stat` says is on disk: the slab files, and the slab manifest beside them.
fn bytes_on_disk(root: &std::path::Path) -> (u64, u64, u64) {
    let mut slab_bytes = 0_u64;
    let mut slab_files = 0_u64;
    for id in slab_ids_at(root).expect("slab ids") {
        slab_bytes += std::fs::metadata(slab_path(root, id))
            .expect("a slab the store listed must be on disk")
            .len();
        slab_files += 1;
    }
    let manifest = std::fs::metadata(slab_manifest_path(root))
        .map(|meta| meta.len())
        .unwrap_or(0);
    (slab_bytes, manifest, slab_files)
}

/// One reading of `rchar` for this PROCESS, with the size of the text it came from.
///
/// NOT `/proc/thread-self/io`, which is what #1936 and #1948 both used: the slab inspection reads
/// on threads it spawns, so the calling thread is charged the manifest and nothing else.
/// `/proc/self/io` aggregates the whole thread group, threads that have since exited included,
/// which is the only kernel counter that can see this reader at all.
///
/// READING THE COUNTER IS ITSELF A READ, and the kernel charges it. procfs fills the buffer
/// before charging for it, so the OPENING reading's own bytes land inside any span it bounds and
/// the closing reading's do not. The length of the text is carried so that the instrument can be
/// subtracted from its own measurement EXACTLY rather than estimated -- the same correction
/// `restore_large_scale.rs` makes, and the reason the residual below is 0 and not 123.
#[derive(Debug, Clone, Copy)]
struct IoReading {
    rchar: u64,
    self_bytes: u64,
}

fn read_proc_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            return Some(IoReading {
                rchar: rest.trim().parse::<u64>().ok()?,
                self_bytes: text.len() as u64,
            });
        }
    }
    None
}

/// What the kernel charged this process across a span, with this instrument's own read removed.
fn kernel_rchar_span(before: Option<IoReading>, after: Option<IoReading>) -> Option<u64> {
    before
        .zip(after)
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes))
}

/// THE ONE READER INSIDE AN OPEN THAT IS NOT THIS STORE, measured rather than assumed.
///
/// `reconcile_slab_manifest_with_disk` asks `std::thread::available_parallelism()` how many
/// workers to inspect slabs with, and on Linux that reads the cgroup CPU quota off `/sys`. It is
/// a read, the kernel charges it, and it is not a block-store file -- so it is exactly the
/// residual a cold open leaves, and it is FIXED at one per open rather than growing with the
/// slab count.
///
/// Measured here rather than written down as a number: the file it reads is the box's, and a box
/// whose quota is spelled differently would make a hard-coded 5 a false failure. Warmed first, so
/// what is measured is the steady-state call the open makes and not a one-time initialisation.
fn available_parallelism_rchar() -> u64 {
    let _ = std::thread::available_parallelism();
    let before = read_proc_io();
    let _ = std::thread::available_parallelism();
    let after = read_proc_io();
    kernel_rchar_span(before, after).unwrap_or(0)
}

// ---------------------------------------------------------------------------------------------
// THE COUNTER IS RIGHT, NOT MERELY PRESENT
// ---------------------------------------------------------------------------------------------

/// The open's own figure is the bytes that are on disk, exactly, at two corpus sizes.
///
/// Two independent instruments -- `stat` on the files, and the counter inside the read -- and no
/// kernel, so this is safe to run beside anything: the figure is taken from the STORE, not from a
/// process-wide atomic another test could move.
// rust-internal: what an open reads off disk is an internal byte counter, not shared behaviour
#[test]
fn the_open_read_counter_is_the_bytes_on_disk_at_two_corpus_sizes() {
    let mut rows: Vec<(u64, u64, u64, u64)> = Vec::new();
    for slabs in [4_u64, 16] {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let records = build_appended_store(root, slabs);
        let (slab_bytes, manifest_bytes, slab_files) = bytes_on_disk(root);

        // FIXTURE FLOOR: more than one slab, every rolled slab a file, records in all of them.
        assert_eq!(
            slab_files, slabs,
            "the fixture must leave {slabs} slab files on disk, not {slab_files}"
        );
        assert!(slabs > 1, "a one-slab store cannot express what this measures");
        assert_eq!(records, slabs * RECORDS_PER_SLAB);
        assert!(slab_bytes > 0 && manifest_bytes > 0, "both kinds of file must exist");

        let process_before = block_store_file_read_counts();
        let store = BlockStore::new(root);
        let process_after = block_store_file_read_counts();
        let (open_bytes, open_reads) = store.open_file_read_counts();

        // THE PROCESS COUNTER TOOK THIS OPEN'S BYTES TOO. A LOWER BOUND, not an equality: the
        // suite opens stores in parallel and a shared atomic takes everyone's reads, so the only
        // thing that can be asserted about it here is that it did not miss these. A tally that
        // published nothing on drop -- the workers' only route to this counter -- would read 0.
        assert!(
            process_after.0 - process_before.0 >= open_bytes,
            "the process counter missed this open: it moved {} while the open read {open_bytes}",
            process_after.0 - process_before.0
        );
        assert!(
            process_after.1 - process_before.1 >= open_reads,
            "the process counter missed this open's reads: {} against {open_reads}",
            process_after.1 - process_before.1
        );

        assert_eq!(
            open_bytes,
            slab_bytes + manifest_bytes,
            "the open's counter must be the slabs plus the manifest, to the byte: counter \
             {open_bytes}, slabs {slab_bytes}, manifest {manifest_bytes}"
        );
        assert_eq!(
            open_reads,
            slabs + 1,
            "one read per slab plus one for the manifest"
        );
        // AND THE OLD ROW IS UNMOVED, which is the point of putting this on its own counter: an
        // open hands back no record, so the field that counts records handed back must not move.
        assert_eq!(
            store.stats().bytes_read,
            0,
            "an open reads no RECORD, so the record counter must still read zero"
        );
        rows.push((slabs, slab_bytes + manifest_bytes, open_bytes, open_reads));
    }

    // THE RATIO. Four times the store, four times the reading -- a cold open reads all of it.
    let (small_slabs, _, small_bytes, small_reads) = rows[0];
    let (large_slabs, _, large_bytes, large_reads) = rows[1];
    assert_eq!(large_slabs, small_slabs * 4);
    assert!(
        large_bytes > small_bytes * 3 && large_bytes < small_bytes * 5,
        "a cold open reads the whole store, so four times the store must be about four times \
         the reading: {small_bytes} -> {large_bytes}"
    );
    assert_eq!(
        large_reads - 1,
        (small_reads - 1) * 4,
        "one read per slab, so the slab reads must scale exactly with the slab count"
    );
}

/// The zero is DISTRUSTED: a store with nothing in it reads nothing, and one slab reads one slab.
///
/// A counter that only ever goes up looks right on every fixture that has something in it. This
/// is the input on which it MUST answer zero, and the smallest input on which it must not.
// rust-internal: the zero-input control for an internal byte counter
#[test]
fn an_open_of_an_empty_store_reads_nothing_and_one_slab_reads_exactly_that_slab() {
    let empty = tempfile::tempdir().expect("tempdir");
    let store = BlockStore::new(empty.path());
    assert_eq!(
        store.open_file_read_counts(),
        (0, 0),
        "there is nothing on disk to read, so the counter must say so"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    build_appended_store(dir.path(), 1);
    let (slab_bytes, manifest_bytes, slab_files) = bytes_on_disk(dir.path());
    assert_eq!(slab_files, 1);
    let store = BlockStore::new(dir.path());
    assert_eq!(
        store.open_file_read_counts(),
        (slab_bytes + manifest_bytes, 2),
        "one slab and one manifest, and nothing else"
    );
}

/// A PLANTED SLAB IS RECOVERED EXACTLY, and the difference is exactly what was planted.
///
/// The counter agreeing with `stat` could be two readings of one mistake if both came from the
/// same walk. This decides the byte count itself: a slab of a size chosen here, installed into
/// the store, must move the next open's figure by exactly that many bytes.
// rust-internal: planted-marker control for an internal byte counter
#[test]
fn the_open_read_counter_recovers_a_planted_slab_exactly() {
    // TWO STORES BUILT THE SAME WAY, compared on their COLD opens. Planting into one store and
    // opening it twice would not do: the second open skips the sealed slabs it already verified,
    // so the two figures would differ by the skip as well as by the plant.
    let plain = tempfile::tempdir().expect("tempdir");
    build_appended_store(plain.path(), 3);
    let (_, plain_manifest, _) = bytes_on_disk(plain.path());
    let plain_open = BlockStore::new(plain.path()).open_file_read_counts();

    let planted_dir = tempfile::tempdir().expect("tempdir");
    build_appended_store(planted_dir.path(), 3);
    // A slab whose length THIS TEST decides, written straight to disk so that no open happens
    // before the one being measured.
    let planted = vec![0_u8; 100_003];
    std::fs::write(slab_path(planted_dir.path(), 9_999), &planted).expect("plant a slab");
    let (_, planted_manifest, _) = bytes_on_disk(planted_dir.path());
    let planted_open = BlockStore::new(planted_dir.path()).open_file_read_counts();

    // The manifests are not the same size on both sides, so they are subtracted off rather than
    // assumed to cancel.
    let slab_growth =
        (planted_open.0 - planted_manifest) - (plain_open.0 - plain_manifest);
    assert_eq!(
        slab_growth,
        planted.len() as u64,
        "the planted slab must appear in the open's figure at exactly its own size: {slab_growth} \
         against {} planted",
        planted.len()
    );
    assert_eq!(
        planted_open.1 - plain_open.1,
        1,
        "and it must cost exactly one more read, not a read per anything else"
    );
}

/// THE SKIP ROUTE IS VISIBLE IN THE BYTE COUNT AND IN NOTHING ELSE.
///
/// The second open of a store skips re-inspecting sealed slabs whose file still matches what
/// their descriptor was verified against. It produces THE SAME DESCRIPTORS either way -- that is
/// what makes it a skip and not a behaviour change -- so nothing comparing the store's ANSWERS
/// can tell a skipping open from a re-reading one. The READ COUNT can, and this is the only thing
/// in the suite that would notice the skip silently ceasing to happen.
// rust-internal: the skip route is an internal read-cost property with no product-visible answer
#[test]
fn the_second_open_skips_sealed_slabs_and_only_the_byte_count_shows_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_appended_store(dir.path(), 8);

    let cold = BlockStore::new(dir.path());
    let (cold_bytes, cold_reads) = cold.open_file_read_counts();
    let cold_slabs = cold.slab_ids().expect("slab ids").len();
    drop(cold);

    let warm = BlockStore::new(dir.path());
    let (warm_bytes, warm_reads) = warm.open_file_read_counts();
    let skipped = warm.slabs_skipped_reinspection_on_open();

    // DENOMINATOR: the route has to have run before anything is asserted about it.
    assert!(
        skipped > 0,
        "the skip route did not run, so this asserts nothing about it"
    );
    assert_eq!(
        skipped,
        cold_slabs - 1,
        "every sealed slab is skipped; the active one is always inspected"
    );
    assert!(
        warm_bytes < cold_bytes,
        "a skipping open must read strictly less than the cold one: {warm_bytes} against \
         {cold_bytes}"
    );
    assert!(
        warm_reads < cold_reads,
        "and it must issue strictly fewer reads: {warm_reads} against {cold_reads}"
    );
    // The answers are the same either way, which is why the count is what has to be asserted.
    assert_eq!(
        warm.slab_ids().expect("slab ids").len(),
        cold_slabs,
        "the skipping open must still know about every slab -- the skip is not a behaviour change"
    );
}

// ---------------------------------------------------------------------------------------------
// THE KERNEL AGREES, TO THE BYTE
// ---------------------------------------------------------------------------------------------

/// The two-size table in the module doc, with the kernel as the third instrument.
///
/// `#[ignore]`d and run by name because it reads `/proc/self/io`, which is charged whatever any
/// other thread in the process reads. Run it as:
///
/// ```text
/// cargo test -p temporalstore-rust --lib \
///     what_a_cold_open_reads_at_two_corpus_sizes -- --ignored --nocapture --test-threads=1
/// ```
// rust-internal: a two-size cost table read from an internal counter and the kernel
#[test]
#[ignore = "takes its third instrument from a process-wide kernel counter; run by name"]
fn what_a_cold_open_reads_at_two_corpus_sizes() {
    let parallelism = available_parallelism_rchar();
    println!("  available_parallelism costs {parallelism} rchar per call on this box");
    for slabs in [50_u64, 200] {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let records = build_appended_store(root, slabs);
        let (slab_bytes, manifest_bytes, slab_files) = bytes_on_disk(root);

        // NOTHING BUT THE OPEN IS INSIDE THE SPAN. An instrument read placed between these two
        // lines is charged to the span and shows up as residual that is not the store's.
        let kernel_before = read_proc_io();
        let store = BlockStore::new(root);
        let kernel_after = read_proc_io();

        let (open_bytes, open_reads) = store.open_file_read_counts();
        let on_disk = slab_bytes + manifest_bytes;
        println!("  slabs {slabs}, records {records}, slab files {slab_files}");
        println!("    slab bytes on disk (stat)   {slab_bytes}");
        println!("    manifest on disk (stat)     {manifest_bytes}");
        println!("    on disk, both               {on_disk}");
        println!("    THE COUNTER INSIDE THE READ {open_bytes} in {open_reads} reads");
        println!("    the old row, stats          {}", store.stats().bytes_read);

        assert_eq!(open_bytes, on_disk, "the counter must be what is on disk");

        // THE KERNEL, WHERE /proc SPOKE. `None` is "the kernel was not counting", never
        // "the kernel counted zero".
        match kernel_rchar_span(kernel_before, kernel_after) {
            Some(kernel) => {
                let residual = kernel as i64 - open_bytes as i64;
                println!("    the kernel, this process    {kernel}");
                println!("    RESIDUAL                    {residual}");
                println!("      of it, available_parallelism {parallelism}");
                println!(
                    "    RESIDUAL, that named and removed {}",
                    residual - parallelism as i64
                );
                assert_eq!(
                    residual,
                    parallelism as i64,
                    "every byte the kernel charged this process across the open must be claimed: \
                     kernel {kernel}, counter {open_bytes}, and the only other reader in the span \
                     is available_parallelism at {parallelism}"
                );
            }
            None => println!("    the kernel said nothing; residual not taken"),
        }
    }
}

/// THE RESIDUAL IS A READING, and a read the counter cannot see proves it.
///
/// A residual of zero means "every byte the kernel charged is claimed" only if a byte the counter
/// CANNOT claim would have shown up. This plants a 1,000,003-byte read of a file that is not a
/// block-store file inside the span and requires the residual to come back as exactly that --
/// and the store's own counter not to move by a byte.
// rust-internal: residual control over an internal counter against the kernel
#[test]
#[ignore = "reads a process-wide kernel counter; run by name"]
fn the_residual_a_cold_open_leaves_is_zero_and_a_planted_read_proves_it_is_a_reading() {
    let parallelism = available_parallelism_rchar();
    let dir = tempfile::tempdir().expect("tempdir");
    build_appended_store(dir.path(), 8);

    let marker_dir = tempfile::tempdir().expect("tempdir");
    let marker = marker_dir.path().join("not-a-slab.bin");
    let planted = vec![3_u8; 1_000_003];
    std::fs::write(&marker, &planted).expect("plant");

    let kernel_before = read_proc_io();
    let store = BlockStore::new(dir.path());
    // DELIBERATELY NOT CHARGED TO THE COUNTER: it is not a block-store file, and it is read with
    // the plain primitive rather than through the counted door.
    let read_back = std::fs::read(&marker).expect("read the marker");
    let kernel_after = read_proc_io();

    assert_eq!(read_back.len(), planted.len());
    let (open_bytes, _) = store.open_file_read_counts();

    let Some(kernel) = kernel_rchar_span(kernel_before, kernel_after) else {
        println!("the kernel said nothing; residual not taken");
        return;
    };
    let residual = kernel as i64 - open_bytes as i64 - parallelism as i64;
    println!(
        "  planted {} bytes; residual {residual} (available_parallelism {parallelism} named and \
         removed)",
        planted.len()
    );
    assert_eq!(
        residual,
        planted.len() as i64,
        "the residual must be exactly the planted read -- if it is not, the residual is not \
         measuring what it claims to"
    );
}

// ---------------------------------------------------------------------------------------------
// EVERY DOOR, AND THE GUARD THAT SAYS SO
// ---------------------------------------------------------------------------------------------

/// Every module of the block store that can reach one of its files.
///
/// DERIVED FROM THE TREE RATHER THAN LISTED BY HAND: a file added to `block_store/` and left out
/// of a hand-written list is exactly the second door this campaign keeps finding.
const MODULES: &[(&str, &str)] = &[
    ("block_store.rs", include_str!("../block_store.rs")),
    ("append.rs", include_str!("append.rs")),
    ("gc.rs", include_str!("gc.rs")),
    ("paths.rs", include_str!("paths.rs")),
    ("read.rs", include_str!("read.rs")),
    ("record.rs", include_str!("record.rs")),
    ("slab_backend.rs", include_str!("slab_backend.rs")),
    ("slab_ids.rs", include_str!("slab_ids.rs")),
    ("slab_manifest.rs", include_str!("slab_manifest.rs")),
    ("slab_reports.rs", include_str!("slab_reports.rs")),
];

/// The production half of a module: everything before its first `#[cfg(test)] mod`.
///
/// CLASSIFIED BY ITEM, NOT BY FILE. `block_store.rs` carries three test modules and 56 of the
/// module's file primitives live in them; a guard that read the whole file would be reading test
/// fixtures as production reads.
fn production_half(source: &str) -> &str {
    match source.find("#[cfg(test)]\nmod ") {
        Some(at) => &source[..at],
        None => source,
    }
}

/// Lines that read the CONTENT of a file without going through the counted door.
///
/// A directory walk, a metadata probe and a write are not reads of content. A directory handle
/// opened only to `sync_all` it is not either, and there are three of those.
fn uncounted_reads_in(source: &str) -> Vec<String> {
    let mut offenders = Vec::new();
    for (number, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") {
            continue;
        }
        let reads = line.contains("fs::read(")
            || line.contains("fs::read_to_string(")
            || line.contains("File::open(")
            || line.contains(".read(true)");
        if !reads {
            continue;
        }
        // The counted door itself, and the two ranged reads that take a tally beside it.
        if line.contains("let bytes = std::fs::read(path)?;")
            || line.contains("std::fs::File::open(self.path(slab_id))?")
        {
            continue;
        }
        // A DIRECTORY handle, opened only to fsync it. No byte is read from any of these.
        if line.contains("File::open(parent)")
            || line.contains("File::open(path)")
            || line.contains("File::open(&root)")
        {
            continue;
        }
        offenders.push(format!("  line {}: {}", number + 1, trimmed));
    }
    offenders
}

/// No production read of a block-store file bypasses the counted door.
// rust-internal: a source-level guard over this crate's own read sites
#[test]
fn every_production_read_of_a_block_store_file_goes_through_the_counted_door() {
    // THE DENOMINATOR IS DERIVED FROM THE TREE, NOT FROM THIS LIST. `include_str!` needs literal
    // paths, so `MODULES` has to be written out by hand -- and a hand-written subject list goes
    // stale the moment someone adds a file, with nothing failing. So the authority is the
    // DIRECTORY: every non-test module in `block_store/` must be named above, and a new file
    // added without a row here fails right here rather than being silently unscanned.
    let module_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/block_store");
    let mut on_disk: Vec<String> = std::fs::read_dir(&module_dir)
        .expect("the block_store module directory must be readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".rs"))
        // The wholly-`#[cfg(test)]` modules are not production and carry their own imports.
        .filter(|name| !matches!(name.as_str(),
            "gc_scale.rs" | "large_store_scale.rs" | "open_read_scale.rs"))
        .collect();
    on_disk.sort();
    let mut named: Vec<String> = MODULES
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .filter(|name| name != "block_store.rs")
        .collect();
    named.sort();
    assert_eq!(
        named, on_disk,
        "the modules this guard scans must be exactly the non-test modules on disk; a file added \
         to block_store/ without a row in MODULES would otherwise go unscanned"
    );
    // VACUITY FLOOR: and the list must not have collapsed to nothing.
    assert!(
        MODULES.len() >= 10,
        "only {} modules named; the guard has lost its subject",
        MODULES.len()
    );
    let mut scanned_bytes = 0_usize;
    let mut offenders: Vec<String> = Vec::new();
    for (name, source) in MODULES {
        let production = production_half(source);
        assert!(
            !production.is_empty(),
            "{name}: the production half is empty -- the split is wrong, and an empty input \
             passes every assertion below"
        );
        scanned_bytes += production.len();
        for offender in uncounted_reads_in(production) {
            offenders.push(format!("{name}{offender}"));
        }
    }
    assert!(
        scanned_bytes > 100_000,
        "only {scanned_bytes} bytes of production source were scanned; the guard is reading the \
         wrong thing"
    );
    assert!(
        offenders.is_empty(),
        "these read a block-store file without going through the counted door:\n{}",
        offenders.join("\n")
    );

    // THE OTHER DENOMINATOR: the door has to be getting used.
    let counted: usize = MODULES
        .iter()
        .map(|(_, source)| {
            production_half(source).matches("read_block_store_file(").count()
                + production_half(source).matches("read_all(").count()
        })
        .sum();
    assert!(
        counted >= 6,
        "only {counted} reads go through the counted door -- either the guard is reading the \
         wrong files or the door has been bypassed wholesale"
    );
}

/// THE NEGATIVE CONTROL: the guard above must go red on an input that deserves it.
///
/// Nine checks in this campaign could not fail. A guard whose ability to fail is untested says
/// nothing at all.
// rust-internal: the negative control for a source-level guard over this crate
#[test]
fn a_new_uncounted_read_would_fail_this_guard() {
    let clean = "        let bytes = read_block_store_file(&path, tally)?;\n";
    assert!(
        uncounted_reads_in(clean).is_empty(),
        "APPARATUS: the guard flagged a counted read"
    );
    let dir_handle = "    if let Ok(dir) = File::open(parent) {\n";
    assert!(
        uncounted_reads_in(dir_handle).is_empty(),
        "APPARATUS: the guard flagged a directory handle opened only to fsync it"
    );

    for planted in [
        "        let bytes = fs::read(slab_path(&root, id))?;\n",
        "        let mut file = std::fs::File::open(&slab)?;\n",
        "        let raw = std::fs::read_to_string(&manifest)?;\n",
        "        let file = OpenOptions::new().read(true).open(&path)?;\n",
    ] {
        assert!(
            !uncounted_reads_in(planted).is_empty(),
            "the guard did not flag {planted:?} -- it cannot go red, so it says nothing"
        );
    }
}
