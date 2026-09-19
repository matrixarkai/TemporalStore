// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A BLOCK STORE COSTS ONCE IT IS LARGE, built the way a shard actually gets large.
//!
//! Every block-store figure in this crate was taken on 500-4,000 objects or 2,000-20,000 records,
//! and the two that were taken on a big slab count -- `the_roll_at_scale` and
//! `what_a_purge_round_examines_at_two_quarantine_sizes` -- build their slabs by CREATING EMPTY
//! FILES. That is the right fixture for what they measure (one roll, one purge round) and it
//! cannot answer what a store that took the writes costs, because in it no record was ever
//! appended, every descriptor carries a zero byte count, and there is no per-record column at all.
//!
//! This builds the store by APPENDING: `RECORDS_PER_SLAB` real appends, then a real roll, and
//! again, until the store holds the slab count asked for. Both arms are one process, one store,
//! counts only.
//!
//! THE BILL. Debug profile, 20 records of 128 bytes per slab, one process per arm, box at load
//! 6-28. Every column is a COUNT, and two independent runs of this test came back BYTE-IDENTICAL
//! in every one of them while the wall time moved from 1,446 s to 1,130 s -- which is the reason
//! none of this is timed.
//!
//! ```text
//!                                         1,000 slabs     10,000 slabs     ratio     raw
//!     records appended                         20,000          200,000     10.00x
//!     PER RECORD
//!       allocations                             7.000            7.000      FLAT    140,000 / 1,400,000
//!       root-directory entries walked          25.025          250.025      9.99x    500,499 / 50,004,999
//!       whole-manifest writes                   0.050            0.050      FLAT      1,000 / 10,000
//!       slab descriptors summarised             0.000            0.000      FLAT          0 / 0
//!     PER SLAB
//!       allocations (the rolls)             1,529.614       15,033.025      9.83x  1,529,614 / 150,330,249
//!       root-directory entries walked         500.499        5,000.500      9.99x    500,499 / 50,004,999
//!       whole-manifest writes                   1.000            1.000      FLAT      1,000 / 10,000
//!       manifest write(2) calls                 1.000            5.456      5.46x      1,000 / 54,556
//!       manifest bytes on disk                257.760          259.776      FLAT    257,760 / 2,597,760
//!       resident index bytes                  328.288          327.488      FLAT    328,288 / 3,274,880
//!     WHOLE STORE
//!       root-directory WALKS                    1,003           10,003      9.97x
//!         of which the open costs                   4                4      FIXED
//!       directory fsyncs                        1,999           19,999      2/roll + 1
//!       payload bytes appended              2,560,000       25,600,000     10.00x
//!       stored bytes with envelopes         2,800,000       28,000,000     10.00x
//!       resident index bytes                  328,288        3,274,880      9.98x
//!       index bytes per payload byte         0.128238         0.127925      FLAT
//!       RESIDUAL allocations                       60               59      FIXED
//!         per record                           0.0030           0.0003
//! ```
//!
//! THE RESIDUAL is the counting allocator read either side of the WHOLE build, minus the two
//! attributed rows -- never the rows against their own sum, which is a residual this crate has
//! shipped once and had to correct. It is 60 and 59: a FIXED cost, and it has a name, because the
//! outer span deliberately opens before `BlockStore::new` and no row claims the open. Ten times
//! the store leaves it where it was, so nothing has drifted out of the rows.
//!
//! WHAT IS FLAT AND WHAT GROWS, every quantity stated and not only the interesting one.
//!
//! FLAT PER RECORD: allocations (7.000 at both sizes, exactly), whole-manifest writes, slab
//! descriptors summarised (zero, and it is a measured zero -- see below). An append costs the
//! same on a ten-thousand-slab store as on a thousand-slab one, which is the result most worth
//! stating plainly: the write path does not read the store's size in any form.
//!
//! FLAT PER SLAB: whole-manifest writes (exactly one per roll), directory fsyncs (exactly two per
//! roll), manifest bytes per descriptor (257.8 against 259.8 -- THE MANIFEST STAYS LINEAR, and the
//! extra two bytes are the wider slab ids), resident index bytes per descriptor (328.3 against
//! 327.5). And the store OPEN is fixed: four directory walks and one manifest persist at both
//! sizes, over an empty directory both times.
//!
//! GROWS: root-directory entries walked, per record and per slab, by the factor the slab count
//! grew. And manifest `write(2)` calls, by 5.46x per slab. Per-slab ALLOCATIONS grow too, 9.83x,
//! and they are the same walk seen from the allocator -- the roll's `Vec<u64>` of slab ids.
//!
//! THE FIRST THING THAT DOES NOT HOLD AN ORDER OF MAGNITUDE UP IS THE DIRECTORY WALK, and it is
//! not a walk anybody added: `roll_slab_inner` derives the next slab id partly from
//! `slab_ids_at(root)`, which reads the whole store root. One roll paying that is already
//! measured and already documented (`the_roll_at_scale`: 10.31 ms of an 1,188 ms roll at 8,000
//! slabs). What was NOT written down is the INTEGRAL. A store that reaches S slabs has walked
//! S(S+1)/2 + S directory entries getting there -- 500,499 to build 1,000 slabs and 50,004,999 to
//! build 10,000, a hundred times the walk for ten times the store -- so the directory work per
//! RECORD grows with the store for as long as it keeps rolling. It is the ENTRIES and not the
//! WALKS: the walk count is linear, 1,003 against 10,003, one per roll plus the four an open
//! costs at either size.
//!
//! THE SECOND IS THE MANIFEST'S WRITE SYSCALLS, and it has a threshold rather than a slope. The
//! manifest is written through a 256 KiB buffer, so while the whole document fits in the buffer a
//! persist is one `write(2)`: 1,000 syscalls for 1,000 persists at 1,000 slabs, which is the shape
//! PR #1867 bought when it put the buffer there. At 10,000 slabs the manifest is 2.60 MB, ten
//! buffers, and the same number of persists costs 54,556 syscalls. The buffer did not stop working: it
//! bounds a persist to its bytes over 256 KiB instead of to one syscall per serde fragment, which
//! is the whole of what PR #1867 bought and is still worth several orders of magnitude here. What
//! is recorded is the SIZE AT WHICH "one syscall per persist" STOPS BEING TRUE -- somewhere
//! between these two arms -- because that sentence is how the buffered writer is described and it
//! is a sentence with a ceiling in it.
//!
//! BOTH ARE THE SAME HALF OF THE SAME OPERATION and both are MEASURED HERE, NOT FIXED HERE, and
//! the decline is PRICED rather than asserted.
//!
//! The obvious removal is to derive the roll's next slab id from `inner.slabs`, which already
//! holds every descriptor, instead of from `slab_ids_at(root)`. Run as a mutation against this
//! tree it takes 50,004,999 directory-entry stats off a 10,000-slab build -- and it is killed by
//! `block_store::tests::a_roll_never_mints_an_id_a_slab_file_already_holds`, which is the whole
//! argument in one test name. A slab file can exist on disk with no descriptor behind it: that is
//! what a process killed between the renames and the manifest persist leaves, and it is the shape
//! `gc_scale`'s crash fixture is built around. A roll that trusted the map would mint an id over
//! one of those files.
//!
//! So what the scan buys is a correctness property and what it costs is 0.9% of a roll --
//! `the_roll_at_scale`'s own split, 10.31 ms of an 1,188 ms roll at 8,000 slabs, the other 99%
//! being the whole-manifest rewrite. A 0.9% win that moves a recovery boundary is not this
//! change's trade. What is left behind is the COUNT, so the next person to price it has a number
//! rather than a stopwatch on a box that moves by more than the effect.
//!
//! THE ZEROS ARE MEASURED ZEROS. `slab_descriptors_summarised` is 0 across both builds, which
//! contradicts what `rolled_store_fixture` says about this path ("every append summarises the
//! slab set and periodically rewrites the manifest"). Neither half holds: `upsert_slab_after_append`
//! is one `BTreeMap` entry lookup and touches no other descriptor, and the manifest persist is
//! deferred off the append path entirely under the single-barrier default -- one write per ROLL,
//! never one per record, at both sizes. `an_append_walks_no_directory_and_summarises_no_descriptor`
//! holds that in the ordinary gate, with a control beside each zero proving the counter moves.
//!
//! THE MEMORY QUESTION, which is the one a small store genuinely cannot answer.
//!
//! A block store's resident cost is PER SLAB AND NOT PER RECORD. Two hundred thousand records and
//! twenty thousand cost the same 328 bytes per slab, because what the store holds in memory is
//! `BlockStoreInner::slabs` -- one `BlockStoreSlabDescriptor` per slab -- and nothing per record.
//! `the_resident_index_tracks_slabs_and_not_records` asserts both halves of that at CI size: ten
//! times the RECORDS in the same eight slabs costs the same resident bytes to the BYTE, and ten
//! times the SLABS costs ten times the bytes. The payload never becomes resident at all -- append
//! writes the encoded record straight at the file and keeps none of it, and a read decodes one
//! record at a time.
//!
//! That makes the split between index and payload a ratio of the SLAB TARGET, not of the corpus.
//! At the shipped 1 GiB target a slab's 328 resident bytes stand against a gigabyte of payload:
//! 3.1e-7 resident bytes per stored byte, and a store would have to hold about 3 PB before its
//! descriptor map reached a gigabyte. The 0.128 in the table above is what the same ratio looks
//! like with this fixture's deliberately tiny slabs, and is there to show that the ratio tracks
//! the slab target and not the record count -- it is not a figure about a deployment.
//!
//! So the block store's own index IS bounded, by store bytes over slab target, and it is bounded
//! well below anything that matters. WHAT THIS MEASUREMENT CANNOT SEE is the other index: the
//! engine holds a `BlockAddress` per live page, which IS per record, and it is a different
//! structure in a different module. `engine/tests/index_bytes_per_key.rs` measures that one at
//! 8,000 and 80,000 records and is where that half of the split lives. Nothing here contradicts
//! it and nothing here covers it.
//!
//! AGAINST THE CLASSIFIED ACCOUNTING, which landed while this was being measured. PR #1922 splits
//! a write's allocations by the sink they land in and reports `slab_append` at 6.000 per record,
//! flat at 2,000 and 20,000. That row and the 7.000 here are the same quantity over different
//! spans, and both are flat:
//!
//!   * `slab_append` is `engine::append_value`, reached through `batch_execute`, with the payload
//!     encode re-tagged out of it as `page_bytes` and the slab file opened once for the batch;
//!   * the 7.000 here is a direct single-record `BlockStore::append`, which carries its own record
//!     encode and opens the slab file per call.
//!
//! So they agree, they bracket each other, and between them the flatness of a block-store append
//! now runs from 2,000 records to 200,000 -- two orders of magnitude, measured twice by different
//! instruments. What #1922 does not have is the PER-SLAB half: its classes are all per record, and
//! the descriptor map is the one structure here that is not. That is what this file adds to it.
//!
//! WHAT THE COUNTING ALLOCATOR CANNOT SEE, stated because a zero from it is ambiguous: it counts
//! calls and requested bytes through the global allocator, so allocator retention, page cache and
//! anything mapped rather than allocated are all invisible. The resident-index figure is a CLONE
//! of the descriptor map charged to the allocator, which is exact for the map's own heap and
//! counts no spare `Vec` capacity -- there is none in a descriptor -- and no `Arc` payload, of
//! which a descriptor holds none either. And every probe here is floored: an uninstrumented build
//! reports zero for all of it, which is why the arms assert a floor before they divide.
//!
//! WHAT STOPPED THE ARMS GOING LARGER: TIME, and specifically the quadratic above. Neither disk
//! nor memory came close -- the two stores together are 10,000 slab files of 3.5 KB and a 2.6 MB
//! manifest, and the resident index of the larger one is 3.27 MB. What costs is that building
//! 10,000 slabs walks 50 million directory entries and rewrites the manifest 10,000 times, which
//! took 1,130-1,446 s for the pair in a debug build. A 100,000-slab arm would be about a hundred times
//! that by the same shape -- a day and a half -- so it is not attempted, and is not extrapolated
//! either: a size that was not run is reported as not run.
//!
//! RUN:
//!
//! ```text
//!   cargo test -p temporalstore-rust --features alloc-probe --lib \
//!       what_a_large_store_costs_at_two_slab_counts -- --ignored --nocapture --test-threads=1
//! ```
use super::*;

// Imported as a NAME, never spelled as a path outside a `#[test]`: the counting-allocator gate in
// `alloc_probe.rs` scans every source line for the probe's fully qualified path and walks back to
// the nearest `#[test]`, so a HELPER that spelled it out would be reported as reading the probe
// outside any test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// Payload every record in the fixture carries. Incompressible enough that the record encoder's
/// compression threshold is not what this measures; the width is fixed so payload bytes are
/// exactly records times this.
const PAYLOAD: &[u8; 128] = b"\
qX7mK2pR9wL4vB8nT6yH3cF5dG1sJ0aZ\
eU7iO2kM9xP4rV8tC6bN3hQ5lW1gY0fD\
zS7jA2nE9uI4oK8mR6pT3vX5yB1cH0lG\
wF7dQ2sN9bJ4kL8aV6eZ3iY5rM1tP0xU";

/// Records appended into each slab before the fixture rolls to the next one.
///
/// Twenty rather than one so the per-RECORD and per-SLAB columns cannot be read off each other:
/// with one record per slab every per-record figure is a per-slab figure wearing a different
/// label, and a quantity that is per-slab would pass a per-record flatness assertion.
const RECORDS_PER_SLAB: u64 = 20;

/// Everything one arm of the measurement read, so the two arms can be divided.
#[derive(Debug, Default, Clone, Copy)]
struct StoreBuildCost {
    slabs_asked: u64,
    slabs_observed: u64,
    records: u64,
    /// Outer counter, taken either side of the WHOLE build. Never a sum of the rows below.
    total_allocs: u64,
    append_allocs: u64,
    roll_allocs: u64,
    root_dir_entries: u64,
    root_dir_walks: u64,
    manifest_writes: u64,
    manifest_file_writes: u64,
    descriptors_summarised: u64,
    directory_fsyncs: u64,
    payload_bytes: u64,
    stored_bytes: u64,
    manifest_bytes_on_disk: u64,
    resident_index_bytes: u64,
}

impl StoreBuildCost {
    /// Allocations the outer counter saw that neither attributed span claims.
    ///
    /// THIS IS NOT THE SUM OF THE ROWS IT AUDITS. `total_allocs` is the counting allocator read
    /// once before the STORE IS OPENED and once after the last roll; the two rows are read inside
    /// that span, around the appends and around the rolls. A phase boundary that drifted past real
    /// work would leave allocations in no row and this number would move -- which is the whole
    /// point, because this crate has shipped a residual that compared phase rows against their own
    /// sum and therefore could not fail.
    ///
    /// It is deliberately not zero: the store open is inside the span and in no row, so the
    /// residual carries a real quantity at both sizes and "it did not move" is a reading rather
    /// than an identity.
    fn residual_allocs(&self) -> i64 {
        self.total_allocs as i64 - self.append_allocs as i64 - self.roll_allocs as i64
    }
}

/// Deep heap of a structure, as the allocator charges for a `Clone` of it.
///
/// A `Clone` allocates exactly the structure's own heap -- every B-tree node, once -- and nothing
/// else, so the alloc bytes across one clone IS the structure's resident footprint as a count.
/// Exact for a `BTreeMap<u64, BlockStoreSlabDescriptor>`, whose value type holds no `Vec`, no
/// `Arc` and, on this path, no `Some(String)`: nothing in it can be shared or over-reserved, which
/// is the only way this technique under-reports.
#[cfg(feature = "alloc-probe")]
fn deep_heap_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let clone = value.clone();
    let counts = probe.stop();
    drop(clone);
    counts.alloc_bytes
}

/// How many entries the store root has been walked for so far, process-wide.
fn root_dir_entries_now() -> u64 {
    crate::durability_metrics::snapshot()
        .get("block_store_root_dir_entries")
        .copied()
        .unwrap_or_default()
}

fn root_dir_walks_now() -> u64 {
    crate::durability_metrics::snapshot()
        .get("block_store_root_dir_walk")
        .copied()
        .unwrap_or_default()
}

/// Build a store of `slabs` slabs by appending `RECORDS_PER_SLAB` records into each and rolling.
///
/// The roll is EXPLICIT rather than target-driven, and that is not a shortcut: `roll_slab` and the
/// overflow inside `append` both run `roll_slab_inner`, which is the whole of what a roll costs.
/// Driving it by the configured target instead would mean mutating a process-wide env var, which
/// a test that fails an assertion before restoring it leaves behind for every later test in the
/// process.
#[cfg(feature = "alloc-probe")]
fn build_appended_store(root: &std::path::Path, slabs: u64) -> (BlockStore, StoreBuildCost) {
    let entries_before = root_dir_entries_now();
    let walks_before = root_dir_walks_now();
    let manifest_file_writes_before = manifest_file_writes();
    let summarised_before = slab_descriptors_summarised();
    let fsyncs_before = super::paths::directory_fsyncs();

    let mut append_allocs = 0u64;
    let mut roll_allocs = 0u64;
    let mut records = 0u64;
    let mut slabs_observed = 0u64;
    let mut last_slab: Option<u64> = None;

    // THE OUTER COUNTER, AND IT SPANS STRICTLY MORE THAN THE ROWS. It opens before the store is
    // opened -- which is work no row claims, and is the only such work in the span -- and closes
    // after the last roll. A residual taken this way has something real in it at both sizes, so a
    // phase boundary drifting past work would change a number that is not otherwise zero; had the
    // probe started after the open, the residual would read 0 at every size and a reader could
    // not tell that from "nothing escaped the rows".
    let outer = Probe::start();
    let store = BlockStore::new(root);
    let mut slab = 0u64;
    while slab < slabs {
        let mut record = 0u64;
        while record < RECORDS_PER_SLAB {
            let probe = Probe::start();
            let address = store.append(PAYLOAD).expect("append");
            append_allocs += probe.stop().allocs;
            if last_slab != Some(address.block_slab_id) {
                slabs_observed += 1;
                last_slab = Some(address.block_slab_id);
            }
            records += 1;
            record += 1;
        }
        // The last slab is not rolled: rolling after the final batch would mint an empty slab and
        // the store would hold `slabs + 1`, one of them carrying no record at all.
        if slab + 1 < slabs {
            let probe = Probe::start();
            store.roll_slab().expect("roll");
            roll_allocs += probe.stop().allocs;
        }
        slab += 1;
    }
    let total = outer.stop();

    let stats = store.stats();
    let resident_index_bytes = {
        let inner = store.inner.lock().expect("block store lock poisoned");
        deep_heap_bytes(&inner.slabs)
    };

    let cost = StoreBuildCost {
        slabs_asked: slabs,
        slabs_observed,
        records,
        total_allocs: total.allocs,
        append_allocs,
        roll_allocs,
        root_dir_entries: root_dir_entries_now() - entries_before,
        root_dir_walks: root_dir_walks_now() - walks_before,
        manifest_writes: stats.slab_manifest_writes,
        manifest_file_writes: manifest_file_writes() - manifest_file_writes_before,
        descriptors_summarised: slab_descriptors_summarised() - summarised_before,
        directory_fsyncs: super::paths::directory_fsyncs() - fsyncs_before,
        payload_bytes: records * PAYLOAD.len() as u64,
        stored_bytes: stats.bytes_written,
        manifest_bytes_on_disk: fs::metadata(slab_manifest_path(root))
            .expect("the manifest is on disk")
            .len(),
        resident_index_bytes,
    };
    (store, cost)
}

/// The two arms, with the ratio. Prints the table in the module doc.
///
///   cargo test -p temporalstore-rust --features alloc-probe --lib \
///       what_a_large_store_costs_at_two_slab_counts -- --ignored --nocapture --test-threads=1
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds a 1,000-slab and then a 10,000-slab store by appending 220,000 records; run by name"]
fn what_a_large_store_costs_at_two_slab_counts() {
    let sizes = [1_000u64, 10_000];
    let mut arms: Vec<StoreBuildCost> = Vec::new();
    let mut walk_dirs: Vec<tempfile::TempDir> = Vec::new();

    let mut index = 0usize;
    while index < sizes.len() {
        let slabs = sizes[index];
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, cost) = build_appended_store(dir.path(), slabs);

        // ---- THE FIXTURE CAN EXPRESS WHAT IS BEING MEASURED. -------------------------------
        // More than one slab, and every slab carrying records of its own. A fixture whose live
        // records all landed in slab 0 would satisfy every per-record assertion below while
        // comparing a one-slab store against a one-slab store.
        assert!(
            cost.slabs_observed > 1,
            "DENOMINATOR: the addresses must span more than one slab, not {}",
            cost.slabs_observed
        );
        assert_eq!(
            cost.slabs_observed, slabs,
            "every slab the fixture rolled to must have taken records of its own: {} slabs \
             appeared in the addresses against {slabs} rolled",
            cost.slabs_observed
        );
        assert_eq!(
            store.slab_ids().expect("slab ids").len() as u64,
            slabs,
            "and every one of them must be a file in the store root"
        );
        assert_eq!(
            cost.records,
            slabs * RECORDS_PER_SLAB,
            "and the record count is the two multiplied, or the arms are not comparable"
        );

        // ---- THE PROBE IS INSTALLED. A build with no counting allocator reports zero for
        // every allocation column, which reads exactly like a path that does not allocate.
        assert!(
            cost.total_allocs > cost.records,
            "FLOOR: {} records cannot have cost {} allocations in total -- the counting \
             allocator is not installed and every allocation column here is a zero that means \
             the opposite",
            cost.records,
            cost.total_allocs
        );
        assert!(
            cost.resident_index_bytes > 0,
            "FLOOR: the descriptor map for {slabs} slabs cannot clone into zero bytes"
        );

        arms.push(cost);
        walk_dirs.push(dir);
        index += 1;
    }

    let small = arms[0];
    let large = arms[1];
    let per = |value: u64, n: u64| value as f64 / n as f64;
    let ratio = |a: f64, b: f64| b / a;

    println!();
    println!(
        "  A BLOCK STORE BUILT BY APPENDING, {RECORDS_PER_SLAB} records per slab, debug profile"
    );
    println!(
        "  {:<38} {:>14} {:>16} {:>9}",
        "", format!("{} slabs", small.slabs_asked), format!("{} slabs", large.slabs_asked), "ratio"
    );
    println!(
        "  {:<38} {:>14} {:>16} {:>8.2}x",
        "records appended",
        small.records,
        large.records,
        ratio(small.records as f64, large.records as f64)
    );
    println!("  PER RECORD");
    let record_rows: [(&str, u64, u64); 4] = [
        ("allocations", small.append_allocs, large.append_allocs),
        ("root-directory entries walked", small.root_dir_entries, large.root_dir_entries),
        ("whole-manifest writes", small.manifest_writes, large.manifest_writes),
        ("slab descriptors summarised", small.descriptors_summarised, large.descriptors_summarised),
    ];
    for (label, small_value, large_value) in record_rows {
        let small_per = per(small_value, small.records);
        let large_per = per(large_value, large.records);
        println!(
            "    {:<36} {:>14.3} {:>16.3} {:>8.2}x    raw {small_value} / {large_value}",
            label,
            small_per,
            large_per,
            ratio(small_per.max(f64::MIN_POSITIVE), large_per)
        );
    }
    println!("  PER SLAB");
    let slab_rows: [(&str, u64, u64); 6] = [
        ("allocations", small.roll_allocs, large.roll_allocs),
        ("root-directory entries walked", small.root_dir_entries, large.root_dir_entries),
        ("whole-manifest writes", small.manifest_writes, large.manifest_writes),
        ("manifest write(2) calls", small.manifest_file_writes, large.manifest_file_writes),
        ("manifest bytes on disk", small.manifest_bytes_on_disk, large.manifest_bytes_on_disk),
        ("resident index bytes", small.resident_index_bytes, large.resident_index_bytes),
    ];
    for (label, small_value, large_value) in slab_rows {
        let small_per = per(small_value, small.slabs_asked);
        let large_per = per(large_value, large.slabs_asked);
        println!(
            "    {:<36} {:>14.3} {:>16.3} {:>8.2}x    raw {small_value} / {large_value}",
            label,
            small_per,
            large_per,
            ratio(small_per.max(f64::MIN_POSITIVE), large_per)
        );
    }
    println!("  WHOLE STORE");
    println!(
        "    {:<36} {:>14} {:>16} {:>8.2}x",
        "resident index bytes",
        small.resident_index_bytes,
        large.resident_index_bytes,
        ratio(small.resident_index_bytes as f64, large.resident_index_bytes as f64)
    );
    println!(
        "    {:<36} {:>14} {:>16} {:>8.2}x",
        "payload bytes appended",
        small.payload_bytes,
        large.payload_bytes,
        ratio(small.payload_bytes as f64, large.payload_bytes as f64)
    );
    println!(
        "    {:<36} {:>14.6} {:>16.6} {:>8.2}x",
        "index bytes per payload byte",
        per(small.resident_index_bytes, small.payload_bytes),
        per(large.resident_index_bytes, large.payload_bytes),
        ratio(
            per(small.resident_index_bytes, small.payload_bytes),
            per(large.resident_index_bytes, large.payload_bytes)
        )
    );
    println!(
        "    {:<36} {:>14} {:>16}",
        "directory fsyncs", small.directory_fsyncs, large.directory_fsyncs
    );
    println!(
        "    {:<36} {:>14} {:>16} {:>8.2}x",
        "root-directory WALKS (not entries)",
        small.root_dir_walks,
        large.root_dir_walks,
        ratio(small.root_dir_walks as f64, large.root_dir_walks as f64)
    );
    println!(
        "    {:<36} {:>14} {:>16}",
        "  of which the open costs",
        small.root_dir_walks - (small.slabs_asked - 1),
        large.root_dir_walks - (large.slabs_asked - 1)
    );
    println!(
        "    {:<36} {:>14} {:>16}",
        "stored bytes (payload + envelopes)", small.stored_bytes, large.stored_bytes
    );
    println!(
        "    {:<36} {:>14} {:>16}",
        "RESIDUAL allocations (outer - rows)",
        small.residual_allocs(),
        large.residual_allocs()
    );
    println!(
        "    {:<36} {:>14.4} {:>16.4}",
        "  per record",
        small.residual_allocs() as f64 / small.records as f64,
        large.residual_allocs() as f64 / large.records as f64
    );
    println!();

    // ---- WHAT IS FLAT. ---------------------------------------------------------------------
    let append_per_record_small = per(small.append_allocs, small.records);
    let append_per_record_large = per(large.append_allocs, large.records);
    assert!(
        (append_per_record_large - append_per_record_small).abs() < 1.0,
        "an append must cost the same however many slabs the store already holds: \
         {append_per_record_small:.3} at {} slabs against {append_per_record_large:.3} at {}",
        small.slabs_asked,
        large.slabs_asked
    );
    assert_eq!(
        small.manifest_writes, small.slabs_asked,
        "one whole-manifest write per roll and not one per record -- the manifest persist is \
         deferred on the append path"
    );
    assert_eq!(
        large.manifest_writes, large.slabs_asked,
        "and the same at ten times the slabs"
    );
    assert_eq!(
        0, small.descriptors_summarised,
        "NOT SUMMARISED: neither an append nor a roll walks the descriptor set, whatever \
         `rolled_store_fixture` says about the append path"
    );
    assert_eq!(0, large.descriptors_summarised, "and the same at ten times the slabs");
    // Per ROLL and not per slab, and asserted as an exact integer rather than a ratio: a store of
    // S slabs rolls S-1 times, each roll fsyncs the store root twice -- once for the new slab file
    // and once for the manifest rename -- and the OPEN fsyncs it once more for the manifest
    // persist its reconcile does. So 2(S-1)+1 exactly, at both sizes.
    // `a_roll_costs_the_same_directory_fsyncs_at_any_slab_count` already pins that a roll's
    // barrier count does not move with the store; this reads the same property at a thousand and
    // ten thousand slabs, where an accidental per-slab fsync inside the roll would be unmissable.
    let expected_fsyncs = |slabs: u64| 2 * (slabs - 1) + 1;
    assert_eq!(
        expected_fsyncs(small.slabs_asked),
        small.directory_fsyncs,
        "two directory fsyncs per roll plus the open's one, not {} over {} rolls",
        small.directory_fsyncs,
        small.slabs_asked - 1
    );
    assert_eq!(
        expected_fsyncs(large.slabs_asked),
        large.directory_fsyncs,
        "and exactly the same shape at ten times the slabs: {} over {} rolls",
        large.directory_fsyncs,
        large.slabs_asked - 1
    );

    // Resident index is per SLAB and flat in it -- this is the memory answer.
    let index_per_slab_small = per(small.resident_index_bytes, small.slabs_asked);
    let index_per_slab_large = per(large.resident_index_bytes, large.slabs_asked);
    assert!(
        (index_per_slab_large - index_per_slab_small).abs() < 32.0,
        "a slab descriptor costs the same resident bytes at any store size: \
         {index_per_slab_small:.1} against {index_per_slab_large:.1}"
    );
    let manifest_per_slab_small = per(small.manifest_bytes_on_disk, small.slabs_asked);
    let manifest_per_slab_large = per(large.manifest_bytes_on_disk, large.slabs_asked);
    assert!(
        (manifest_per_slab_large - manifest_per_slab_small).abs() < 16.0,
        "THE MANIFEST STAYS LINEAR IN THE SLAB COUNT: {manifest_per_slab_small:.1} bytes per \
         descriptor at {} slabs against {manifest_per_slab_large:.1} at {}",
        small.slabs_asked,
        large.slabs_asked
    );

    // ---- WHAT GROWS. -----------------------------------------------------------------------
    let entries_ratio = large.root_dir_entries as f64 / small.root_dir_entries as f64;
    assert!(
        entries_ratio > 50.0,
        "THE INTEGRAL OF THE ROLL'S DIRECTORY SCAN IS QUADRATIC IN THE SLAB COUNT: ten times \
         the store should walk about a hundred times the directory entries, saw {entries_ratio:.2}x \
         ({} against {})",
        small.root_dir_entries,
        large.root_dir_entries
    );
    // The SECOND quantity that does not hold an order of magnitude up, and it is on the other
    // half of the roll: the manifest buffer is 256 KiB, so while the manifest fits in it one
    // persist is one `write(2)` -- and once the manifest outgrows it, a persist costs a syscall
    // per buffer. The count per roll therefore goes from 1 to the manifest's size over 256 KiB,
    // and summed over the store's life it grows with the square of the slab count exactly as the
    // directory walk does.
    let manifest_syscall_ratio =
        large.manifest_file_writes as f64 / small.manifest_file_writes as f64;
    assert!(
        manifest_syscall_ratio > 10.0,
        "THE MANIFEST'S WRITE SYSCALLS OUTGROW THE BUFFER: ten times the store took \
         {manifest_syscall_ratio:.1}x the write(2) calls ({} against {}), so the 256 KiB buffer \
         stops being one syscall per persist somewhere between these two sizes",
        small.manifest_file_writes,
        large.manifest_file_writes
    );

    let walks_ratio = large.root_dir_walks as f64 / small.root_dir_walks as f64;
    assert!(
        (walks_ratio - 10.0).abs() < 0.2,
        "while the number of WALKS is LINEAR -- one per roll, plus the fixed few an open costs \
         -- so the growth is in the directory each walk faces and not in how often it is asked: \
         {} walks against {} is {walks_ratio:.3}x",
        small.root_dir_walks,
        large.root_dir_walks
    );

    // ---- THE INDEPENDENT RESIDUAL. ---------------------------------------------------------
    let small_residual_per_record = small.residual_allocs() as f64 / small.records as f64;
    let large_residual_per_record = large.residual_allocs() as f64 / large.records as f64;
    assert!(
        small.residual_allocs() > 0,
        "FLOOR: the store open is inside the outer span and in no row, so the residual must \
         carry something -- a residual of {} is an identity rather than a reading",
        small.residual_allocs()
    );
    assert!(
        large.residual_allocs() > 0,
        "and the same at ten times the slabs: {}",
        large.residual_allocs()
    );
    assert!(
        (large_residual_per_record - small_residual_per_record).abs() < 0.5,
        "THE RESIDUAL IS FLAT PER RECORD, so no phase boundary has drifted past real work: \
         {small_residual_per_record:.4} per record at {} slabs against \
         {large_residual_per_record:.4} at {}",
        small.slabs_asked,
        large.slabs_asked
    );
    // And the stronger reading the two arms allow: the residual is a FIXED cost, not a small
    // per-record one. Ten times the store leaves it where it was, which is what "the store open
    // and nothing else escaped the rows" means. A boundary that had drifted past real work would
    // put a number here that grew with the store; pinning it exactly would instead fail on an
    // unrelated allocation moving one statement, which is not what this watches for.
    assert!(
        (large.residual_allocs() - small.residual_allocs()).abs() <= 8,
        "THE RESIDUAL IS FIXED, not per record: {} allocations at {} slabs against {} at {}",
        small.residual_allocs(),
        small.slabs_asked,
        large.residual_allocs(),
        large.slabs_asked
    );
}

// -------------------------------------------------------------------------------------------
// The same properties at a size CI can afford. These run in the ordinary gate.
// -------------------------------------------------------------------------------------------

/// Build a small store the same way the scale arms do, with no probe and no counting.
fn small_appended_store(root: &std::path::Path, slabs: u64, records_per_slab: u64) -> BlockStore {
    let store = BlockStore::new(root);
    let mut slab = 0u64;
    while slab < slabs {
        let mut record = 0u64;
        while record < records_per_slab {
            store.append(PAYLOAD).expect("append");
            record += 1;
        }
        if slab + 1 < slabs {
            store.roll_slab().expect("roll");
        }
        slab += 1;
    }
    store
}

/// A roll walks the WHOLE store root, so the entries a store walks getting to N slabs grow with N.
///
/// THE COUNTER IS THE SUBJECT. `the_roll_at_scale` already prices one roll's scan with a
/// stopwatch, which cannot be asserted -- wall times on this box move by more than the effect.
/// `block_store_root_dir_entries` counts inside `slab_ids_at`, so the shape is assertable at four
/// slabs and the ignored arm above only has to confirm it holds at ten thousand.
///
/// TWO SIZES, because a single size cannot tell a per-roll constant from a walk of the directory.
#[test]
fn a_slab_roll_walks_every_slab_already_in_the_store_root() {
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();

    let before = root_dir_entries_now();
    let small = small_appended_store(small_dir.path(), 4, 3);
    let small_entries = root_dir_entries_now() - before;

    let before = root_dir_entries_now();
    let large = small_appended_store(large_dir.path(), 40, 3);
    let large_entries = root_dir_entries_now() - before;

    // DENOMINATORS. The two fixtures really differ by ten, and both really hold more than one
    // slab -- a fixture that collapsed into a single slab would compare {0} against {0}.
    let small_slabs = small.slab_ids().unwrap().len();
    let large_slabs = large.slab_ids().unwrap().len();
    assert_eq!(small_slabs, 4, "the small fixture really holds four slabs");
    assert_eq!(large_slabs, 40, "the large fixture really holds forty");
    assert!(small_slabs > 1, "and more than one, or there is nothing to walk");

    // VACUITY FLOOR. A counter that never moved reports zero entries at both sizes, which would
    // satisfy any ratio assertion written as an inequality on their difference.
    assert!(
        small_entries > 0,
        "the four-slab build must have walked the store root at all"
    );

    // THE SHAPE, derived rather than quoted. A store of S slabs rolls S-1 times, and the k'th
    // roll faces k slab files PLUS the slab manifest -- the walk is over every entry in the store
    // root, not over the ones whose names parse as a slab. So the entries a build walks are
    //
    //     sum over k in 1..=R of (k + 1)  =  R(R+1)/2 + R,   R = S - 1 rolls
    //
    // which is 9 at four slabs and 819 at forty. The manifest term is what made a first pass at
    // this test read 6 and 780 and fail: the +1 is a real entry the roll really stats, and
    // leaving it out would have understated the walk by one per roll at every size.
    let walked = |slabs: u64| {
        let rolls = slabs - 1;
        rolls * (rolls + 1) / 2 + rolls
    };
    assert_eq!(
        small_entries,
        walked(4),
        "three rolls over a store growing 1, 2, 3 -- each plus the manifest -- must walk {} \
         entries, not {small_entries}",
        walked(4)
    );
    assert_eq!(
        large_entries,
        walked(40),
        "thirty-nine rolls over a store growing 1..39 must walk {} entries, not {large_entries}",
        walked(40)
    );
    assert!(
        large_entries > small_entries * 50,
        "ten times the slabs must cost far more than ten times the walk: {small_entries} \
         against {large_entries}"
    );
}

/// An APPEND does not walk the store root and does not summarise the descriptor set.
///
/// The per-record half of the same question, and the one that says a large store's writes are not
/// slowed by its size. Asserted as an exact zero for both, each with a control beside it proving
/// the counter it reads does move -- a zero from a counter nobody increments is the failure this
/// pair exists to rule out.
#[test]
fn an_append_walks_no_directory_and_summarises_no_descriptor() {
    let dir = tempfile::tempdir().unwrap();
    let store = small_appended_store(dir.path(), 8, 3);
    assert!(
        store.slab_ids().unwrap().len() > 1,
        "DENOMINATOR: a store of one slab has no directory worth walking"
    );

    let entries_before = root_dir_entries_now();
    let summarised_before = slab_descriptors_summarised();
    let mut record = 0u64;
    while record < 64 {
        store.append(PAYLOAD).expect("append");
        record += 1;
    }
    let entries = root_dir_entries_now() - entries_before;
    let summarised = slab_descriptors_summarised() - summarised_before;

    assert_eq!(0, entries, "sixty-four appends must walk no directory entry");
    assert_eq!(
        0, summarised,
        "and must summarise no slab descriptor -- the append path does not touch the slab set \
         beyond its own entry"
    );

    // CONTROLS. Both counters move for an operation that really does the thing, so neither zero
    // above is the counter failing to count.
    let entries_before = root_dir_entries_now();
    store.roll_slab().expect("roll");
    assert!(
        root_dir_entries_now() > entries_before,
        "CONTROL: a roll DOES walk the store root, so the zero above is the append and not the \
         counter"
    );
    let summarised_before = slab_descriptors_summarised();
    let summary = store.slab_summary();
    assert!(
        slab_descriptors_summarised() > summarised_before,
        "CONTROL: a summary DOES walk the descriptors, so the zero above is the append and not \
         the counter"
    );
    assert!(
        summary.active_slabs + summary.sealed_slabs > 1,
        "and the summary it walked really covers more than one slab"
    );
}

/// The store's resident index is PER SLAB, not per record.
///
/// Ten times the records in the SAME number of slabs must not move the descriptor map at all;
/// ten times the slabs must move it by ten. Both halves, because either one alone is satisfied by
/// a map that does not grow with anything.
#[test]
#[cfg(feature = "alloc-probe")]
fn the_resident_index_tracks_slabs_and_not_records() {
    let few_dir = tempfile::tempdir().unwrap();
    let many_dir = tempfile::tempdir().unwrap();
    let wide_dir = tempfile::tempdir().unwrap();

    let few = small_appended_store(few_dir.path(), 8, 4);
    let many = small_appended_store(many_dir.path(), 80, 4);
    // Same slab count as `few`, ten times the records in each slab.
    let wide = small_appended_store(wide_dir.path(), 8, 40);

    let heap = |store: &BlockStore| {
        let inner = store.inner.lock().expect("block store lock poisoned");
        deep_heap_bytes(&inner.slabs)
    };
    let few_bytes = heap(&few);
    let many_bytes = heap(&many);
    let wide_bytes = heap(&wide);

    // DENOMINATORS and the probe FLOOR, before anything is divided.
    assert_eq!(few.slab_ids().unwrap().len(), 8);
    assert_eq!(many.slab_ids().unwrap().len(), 80);
    assert_eq!(wide.slab_ids().unwrap().len(), 8);
    assert!(
        few_bytes > 0,
        "FLOOR: eight descriptors cannot clone into zero bytes -- the counting allocator is not \
         installed and every figure here is a zero that means the opposite"
    );
    assert_eq!(
        few.stats().writes * 10,
        wide.stats().writes,
        "DENOMINATOR: the wide store really took ten times the records of the narrow one"
    );

    assert_eq!(
        few_bytes, wide_bytes,
        "ten times the RECORDS in the same eight slabs must not cost one resident byte more: \
         {few_bytes} against {wide_bytes}"
    );
    assert!(
        many_bytes > few_bytes * 5,
        "while ten times the SLABS must cost about ten times the bytes: {few_bytes} against \
         {many_bytes}"
    );
}

/// The root-directory walk counter sees the store's own `slab_ids()`, not only the private walk.
///
/// `BlockStore::slab_ids` used to be a SECOND copy of `slab_ids_at`'s loop, written out inline, so
/// a counter placed in one of them saw none of the other's callers -- the exact shape of a guard
/// that covers one of two live copies. The copy is gone; this is what keeps it gone.
#[test]
fn the_public_slab_id_listing_is_counted_like_the_private_one() {
    let dir = tempfile::tempdir().unwrap();
    let store = small_appended_store(dir.path(), 5, 2);

    let walks_before = root_dir_walks_now();
    let entries_before = root_dir_entries_now();
    let listed = store.slab_ids().unwrap().len() as u64;
    let walks = root_dir_walks_now() - walks_before;
    let entries = root_dir_entries_now() - entries_before;

    assert_eq!(5, listed, "DENOMINATOR: the listing really found five slabs");
    assert!(listed > 1, "and more than one, or the walk is trivial");
    assert_eq!(1, walks, "one listing is one walk, not {walks}");
    // THE WALK IS OVER THE DIRECTORY, NOT OVER THE SLABS. Five slab files and the slab manifest
    // are six entries; only five of them parse as a slab id. That difference is why the counter
    // is on ENTRIES rather than on the returned length -- the cost is what `read_dir` hands back,
    // and anything else the store root accumulates is charged to every walk of it.
    assert_eq!(
        listed + 1,
        entries,
        "the listing returned {listed} slabs and the walk stat'd {entries} entries; the extra \
         one is the slab manifest, which shares the directory"
    );
    assert!(
        fs::metadata(slab_manifest_path(dir.path())).is_ok(),
        "and that extra entry really is the manifest, which really is in the store root"
    );
}
