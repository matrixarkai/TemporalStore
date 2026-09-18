// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the DUMP path costs at two corpus sizes.
//!
//! THE RESULT. A bucket dump manifest embeds a whole-shard index image, so it is proportional to
//! the STORE and not to the buckets dumped. That is a durability decision and these tests do not
//! argue with it. What they measure is how many times that document is read back, parsed and
//! re-serialised by work that never looks inside it. Counted rather than timed, because the box
//! these run on is shared:
//!
//! ```text
//!   records   manifest on disk   bytes ONE round reads back   re-serialised for checksum
//!     2,000            338 KB                       10.2 MB                      10.9 MB
//!    20,000          3,810 KB                      114.9 MB                     122.6 MB
//! ```
//!
//! 11.28x over a 10.00x corpus, with ONE manifest on disk in both arms. The multiplier is not the
//! store -- it is that one round takes 27 manifest-directory listings, and every listing reads
//! every manifest file whole, parses it whole, and re-serialises it whole to verify its checksum.
//! What the round then reads off the result is `wal_sequence`, `index_log_sequence`, `bucket_ids`
//! and `block_slab_ids`: four small fields, none of which needs the index image.
//!
//! WHAT IS FIXED HERE: the re-serialisation used to COPY the manifest first, once per manifest per
//! listing, to clear one string field. `bucket_dump_manifest_checksum_in_place` empties the field
//! on the value it already owns and puts it back. Counted, not timed:
//! `a_listing_no_longer_copies_every_manifest_to_check_its_checksum` drives both arms in one
//! process on one fixture.
//!
//! WHAT IS NOT FIXED: the 27 listings, and the fact that a listing materialises an index image
//! nobody asked for. Both are reported with their numbers and their call sites. Collapsing the
//! listings needs an invalidation rule that survives a round which writes a manifest half way
//! through, and that is a durability decision, not a tuning one.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::StorageManagerCycleRequest;

const SMALL: usize = 2_000;
const LARGE: usize = 20_000;

fn dump_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn seed_in_batches(engine: &TemporalEngine, shard_id: ShardId, count: usize, batch: usize) {
    let mut index = 0usize;
    while index < count {
        let end = (index + batch).min(count);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; 128],
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

fn round_request() -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        ..StorageManagerCycleRequest::default()
    }
}

/// ONE DUMP'S MANIFEST BYTES, AT TWO CORPUS SIZES, AND WHAT ELSE GOES THROUGH SERDE TO PRODUCE IT.
///
/// Two halves, asserted separately: that the manifest is proportional to the store (it embeds the
/// whole-shard index, so it must be), and that producing it serialises that document more than
/// once -- the checksum pass and the write pass.
#[test]
fn a_dump_serialises_its_whole_manifest_more_than_once_at_every_corpus_size() {
    fn one_size(records: usize) -> (u64, u64, u64, u64, u64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = dump_engine(dir.path());
        seed_in_batches(&engine, 1, records, 100);
        crate::engine::reset_bucket_dump_manifest_io_counts();
        let started = std::time::Instant::now();
        let manifest = engine
            .create_bucket_dump_manifest(1, Vec::<u32>::new())
            .expect("dump");
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let counts = crate::engine::bucket_dump_manifest_io_counts();
        println!(
            "  {records:>6} records: manifest written {:>9} B in {} file write(s) ({} under a \
             shard guard), checksum re-serialised {:>9} B in {} pass(es), {} copy(ies); dump took \
             {elapsed_ms} ms over {} buckets",
            counts.bytes_written,
            counts.file_writes,
            counts.writes_under_guard,
            counts.checksum_bytes,
            counts.checksum_serializes,
            counts.checksum_clones,
            manifest.bucket_ids.len()
        );
        (
            counts.bytes_written,
            counts.checksum_bytes,
            counts.checksum_serializes,
            counts.file_writes,
            counts.writes_under_guard,
        )
    }

    let (small_written, small_checksum, small_passes, small_writes, small_guarded) = one_size(SMALL);
    let (large_written, large_checksum, large_passes, large_writes, large_guarded) = one_size(LARGE);

    // DENOMINATOR. A dump that wrote nothing satisfies every ratio below.
    assert!(
        small_written > 0 && large_written > 0,
        "a dump wrote no manifest bytes at one of the two sizes ({small_written} / \
         {large_written}), so every ratio below is vacuous"
    );
    assert_eq!(
        (small_writes, large_writes),
        (1, 1),
        "one dump must write exactly one manifest file at each size, got {small_writes} / \
         {large_writes}"
    );

    // HALF ONE: the manifest is proportional to the STORE, not to the dumped buckets.
    let byte_ratio = large_written as f64 / small_written as f64;
    let size_ratio = LARGE as f64 / SMALL as f64;
    println!(
        "  manifest bytes {small_written} -> {large_written} = {byte_ratio:.2}x over a \
         {size_ratio:.2}x corpus"
    );
    assert!(
        byte_ratio > size_ratio * 0.5,
        "manifest bytes grew only {byte_ratio:.2}x over a {size_ratio:.2}x corpus, so the \
         manifest has stopped being proportional to the store and this test has stopped \
         describing it"
    );

    // HALF TWO, ASSERTED SEPARATELY: the extra serde pass, and no copy behind it.
    assert_eq!(
        (small_passes, large_passes),
        (1, 1),
        "the create path runs the checksum serialise exactly once per dump; got {small_passes} / \
         {large_passes}"
    );
    let small_multiple = (small_written + small_checksum) as f64 / small_written as f64;
    let large_multiple = (large_written + large_checksum) as f64 / large_written as f64;
    println!(
        "  bytes through serde per byte written: {small_multiple:.2}x at {SMALL}, \
         {large_multiple:.2}x at {LARGE}"
    );
    assert!(
        small_multiple > 1.5 && large_multiple > 1.5,
        "the checksum pass no longer re-serialises the manifest ({small_multiple:.2}x / \
         {large_multiple:.2}x); if that was fixed, fix this number too"
    );

    // HALF THREE: the durable write is not taken under a shard-table guard.
    assert_eq!(
        (small_guarded, large_guarded),
        (0, 0),
        "a manifest write -- a whole-store document, fsync'd, plus a parent-directory fsync -- \
         was taken while this thread held a shard-table guard ({small_guarded} / \
         {large_guarded} of {small_writes} / {large_writes}). Every write on the shard waits \
         behind that."
    );
}

/// WHAT ONE STORAGE-MANAGER ROUND READS BACK OFF THE MANIFEST DIRECTORY.
///
/// Nothing in a round needs a manifest's index image. The round reads `wal_sequence`,
/// `index_log_sequence`, `bucket_ids` and `block_slab_ids` -- four small fields. It gets them by
/// listing the directory, reading every manifest file whole, parsing every one whole, and
/// re-serialising every one whole to verify its checksum. So the cost of the round's manifest
/// questions is |manifests| x |corpus|, and neither factor is in the question.
///
/// Two halves, asserted separately: how many times a round lists the directory (a count, and a
/// property of control flow, so FLAT in the store), and how many bytes that costs (proportional
/// to the store).
#[test]
fn a_round_reads_every_manifest_whole_to_answer_four_small_questions() {
    fn one_size(records: usize) -> (u64, u64, u64, u64, u64, u64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = dump_engine(dir.path());
        seed_in_batches(&engine, 1, records, 100);
        // One dump, so the directory holds exactly one manifest when the round starts.
        engine
            .create_bucket_dump_manifest(1, Vec::<u32>::new())
            .expect("dump");
        let manifests = engine.list_bucket_dump_manifests(1).len() as u64;
        crate::engine::reset_bucket_dump_manifest_io_counts();
        let started = std::time::Instant::now();
        let report = engine.run_storage_manager_cycle(round_request());
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let counts = crate::engine::bucket_dump_manifest_io_counts();
        println!(
            "  {records:>6} records, {manifests} manifest(s) on disk, round {elapsed_ms} ms: \
             listed the dir {} time(s), read {} file(s) = {} B ({} read under a shard guard), \
             re-serialised {} manifest(s) for checksum = {} B (round errors {})",
            counts.dir_listings,
            counts.file_reads,
            counts.bytes_read,
            counts.reads_under_guard,
            counts.checksum_serializes,
            counts.checksum_bytes,
            report.errors.len()
        );
        if records == LARGE {
            println!("  WHO ASKS FOR A LISTING, and how many each round:");
            for (site, times) in crate::engine::bucket_dump_manifest_listing_sites() {
                println!("    {times:>3}x  {site}");
            }
        }
        (
            manifests,
            counts.dir_listings,
            counts.file_reads,
            counts.bytes_read,
            counts.checksum_bytes,
            counts.reads_under_guard,
        )
    }

    let (small_manifests, small_listings, small_reads, small_bytes, small_checksum, small_guarded) =
        one_size(SMALL);
    let (large_manifests, large_listings, large_reads, large_bytes, large_checksum, large_guarded) =
        one_size(LARGE);

    // DENOMINATOR.
    assert_eq!(
        (small_manifests, large_manifests),
        (1, 1),
        "each arm must start with exactly one manifest, or the per-manifest reading below is not \
         comparable: {small_manifests} / {large_manifests}"
    );
    assert!(
        small_listings > 0 && large_listings > 0,
        "a round listed the manifest directory zero times at one size ({small_listings} / \
         {large_listings}); either the round stopped asking or the counter stopped counting, and \
         every number below would then read as a win"
    );

    // HALF ONE: how many whole-file reads per round, and is it flat in the store.
    println!(
        "  directory listings per round: {small_listings} at {SMALL}, {large_listings} at \
         {LARGE}; whole-file reads {small_reads} -> {large_reads}"
    );
    assert_eq!(
        small_listings, large_listings,
        "the number of manifest-directory listings a round takes changed with the corpus \
         ({small_listings} -> {large_listings}); it is a property of the round's control flow, \
         not of the store"
    );
    assert_eq!(
        small_reads, large_reads,
        "whole-manifest file reads per round changed with the corpus ({small_reads} -> \
         {large_reads}) with the same number of manifests on disk"
    );

    // HALF TWO, ASSERTED SEPARATELY: the bytes, which are NOT flat.
    let read_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    let size_ratio = LARGE as f64 / SMALL as f64;
    println!(
        "  bytes a round reads back: {small_bytes} -> {large_bytes} = {read_ratio:.2}x over a \
         {size_ratio:.2}x corpus; checksum re-serialisation on top: {small_checksum} -> \
         {large_checksum}"
    );
    assert!(
        read_ratio > size_ratio * 0.5,
        "the bytes a round reads back off the manifest directory grew only {read_ratio:.2}x over \
         a {size_ratio:.2}x corpus. If a round stopped materialising the embedded index to answer \
         its four small questions, that is the fix this test exists to notice -- update it."
    );

    // HALF THREE: none of that reading is taken under a shard-table guard.
    assert_eq!(
        (small_guarded, large_guarded),
        (0, 0),
        "manifest files were read while this thread held a shard-table guard ({small_guarded} of \
         {small_reads}, {large_guarded} of {large_reads}). Each read is a whole-store document off \
         disk, and every write on the shard waits behind it."
    );
}

/// A LISTING NO LONGER COPIES EVERY MANIFEST TO CHECK ITS CHECKSUM.
///
/// The checksum is taken over the manifest's own JSON with the checksum field emptied, and the
/// only way to produce that from a shared reference is to copy the whole document first -- the
/// whole-shard index image and one summary per bucket, once per manifest per listing. A listing
/// owns what it parsed, so it can empty the field in place and put it back.
///
/// TWO ARMS IN ONE PROCESS ON ONE FIXTURE. "No copies" is satisfied just as well by a listing that
/// stopped checking checksums, or by one that found no manifests, so the control arm forces the
/// copy back on and must produce a non-zero count -- and both arms must agree on the checksum,
/// which is what makes the removal a change of cost and not of meaning.
#[test]
fn a_listing_no_longer_copies_every_manifest_to_check_its_checksum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = dump_engine(dir.path());
    seed_in_batches(&engine, 1, SMALL, 100);
    engine
        .create_bucket_dump_manifest(1, Vec::<u32>::new())
        .expect("dump");

    // CONTROL FIRST, so a treatment that somehow removed the manifest cannot make the control
    // pass by finding nothing.
    crate::engine::bucket_dump_io::checksum_clones_manifest_for_test(true);
    crate::engine::reset_bucket_dump_manifest_io_counts();
    let control = engine.list_bucket_dump_manifests(1);
    let control_counts = crate::engine::bucket_dump_manifest_io_counts();
    crate::engine::bucket_dump_io::checksum_clones_manifest_for_test(false);

    crate::engine::reset_bucket_dump_manifest_io_counts();
    let treatment = engine.list_bucket_dump_manifests(1);
    let treatment_counts = crate::engine::bucket_dump_manifest_io_counts();

    println!(
        "  one listing of {} manifest(s): copying arm {} copy(ies) / {} serialise(s), in-place arm \
         {} copy(ies) / {} serialise(s)",
        control.len(),
        control_counts.checksum_clones,
        control_counts.checksum_serializes,
        treatment_counts.checksum_clones,
        treatment_counts.checksum_serializes
    );

    // DENOMINATOR: a listing that read nothing proves nothing.
    assert_eq!(
        control.len(),
        1,
        "the control listing found {} manifests, not the 1 this fixture wrote",
        control.len()
    );
    assert_eq!(
        control_counts.checksum_serializes, 1,
        "the control listing verified {} checksums, not 1; it is not doing the work this test \
         is about",
        control_counts.checksum_serializes
    );
    assert!(
        control_counts.checksum_clones >= 1,
        "the control arm produced {} copies, so 'the treatment produced none' says nothing",
        control_counts.checksum_clones
    );

    // SAME ANSWER. The point is that the cost went, not the check.
    //
    // Compared by identity and integrity field rather than by whole value: a manifest carries one
    // summary per bucket, and a failure that prints two of them buries the sentence saying what
    // went wrong under a megabyte of bucket ids.
    let identify = |manifests: &[BucketDumpManifest]| {
        manifests
            .iter()
            .map(|manifest| (manifest.manifest_id.clone(), manifest.checksum.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        identify(&control),
        identify(&treatment),
        "the two arms disagreed about what the directory holds, so this is not a cost change"
    );
    assert_eq!(
        treatment_counts.checksum_serializes, control_counts.checksum_serializes,
        "the in-place arm verified {} checksums against the control's {}; it skipped the check \
         rather than doing it more cheaply",
        treatment_counts.checksum_serializes, control_counts.checksum_serializes
    );
    assert_eq!(
        treatment_counts.checksum_bytes, control_counts.checksum_bytes,
        "the two arms hashed different payload sizes ({} vs {}), so the checksum is not the same \
         function it was",
        treatment_counts.checksum_bytes, control_counts.checksum_bytes
    );
    assert_eq!(
        treatment_counts.checksum_clones, 0,
        "a listing still copies {} whole manifest(s) to check a checksum",
        treatment_counts.checksum_clones
    );
}

/// HOW MANY MANIFESTS A SHARD ACCUMULATES, AND WHETHER THE PRUNE KEEPS UP.
///
/// The prune retains the newest manifest plus any older one that is still the only dump covering
/// some bucket. A round that dumps EVERY dirty bucket therefore supersedes everything before it,
/// and the directory should sit at one. Ten dumps, and this counts what is left -- each one of
/// them would be a whole-shard index image on disk, and a whole-shard index image through serde on
/// every listing of every later round.
#[test]
fn repeated_dumps_do_not_accumulate_manifests_when_each_covers_the_whole_shard() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = dump_engine(dir.path());
    seed_in_batches(&engine, 1, 2_000, 100);

    let mut counted = 0usize;
    let mut index = 0usize;
    while index < 10 {
        seed_in_batches(&engine, 1, 2_000 + (index + 1) * 50, 100);
        let report = engine.run_storage_manager_cycle(round_request());
        assert!(
            report.errors.is_empty(),
            "round {index} errored: {:?}",
            report.errors
        );
        counted += usize::from(
            report
                .lifecycle_report
                .as_ref()
                .map(|lifecycle| lifecycle.dump_manifest.is_some())
                .unwrap_or(false),
        );
        index += 1;
    }

    // DENOMINATOR: a run that never dumped proves nothing about the prune.
    assert!(
        counted >= 5,
        "only {counted} of 10 rounds produced a dump manifest; the prune below is not being \
         exercised"
    );

    let on_disk = engine.list_bucket_dump_manifests(1);
    let plan = engine.bucket_dump_manifest_prune_plan(1);
    println!(
        "  {counted} dumps over 10 rounds -> {} manifest(s) left on disk, retained {}, prunable \
         {}, blocked {}",
        on_disk.len(),
        plan.retained_manifest_ids.len(),
        plan.prunable_manifest_ids.len(),
        plan.blocked_manifest_ids.len()
    );
    assert!(
        on_disk.len() <= 2,
        "{counted} whole-shard dumps left {} manifests on disk; the prune is not keeping up, and \
         every one of them embeds a whole-shard index image",
        on_disk.len()
    );
}

/// WHICH QUANTITY THE DUMP BUDGET SHOULD COUNT, MEASURED ON FIVE WRITE SHAPES.
///
/// `min_undumped_wal_records` is compared against undumped LOG RECORDS, and a batch is one log
/// record however many objects it carries. `min_undumped_wal_bytes` is compared against
/// `undumped_len_since_dump`. NEITHER survives a change in how the writer grouped its objects --
/// which was not obvious, and the first version of this test asserted that the byte count did and
/// was wrong. The same 2,000 objects of 128 bytes:
///
/// ```text
///   grouping    log records   undumped bytes   dirty objects   object mutations
///   batch 1           2,000          377,652           2,000              2,000
///   batch 10            200           57,225           2,000              2,000
///   batch 100            20           32,246           2,000              2,000
///   batch 500             4           30,223           2,000              2,000
/// ```
///
/// Records move 500x and bytes 12.5x, because per-record framing dominates a 128-byte write.
///
/// DIRTY OBJECTS is invariant there and is still the wrong quantity, which is the other half of
/// this test: 2,000 writes to ONE key leave one dirty object and 2,000 mutations to replay, so a
/// budget counting dirty objects would delay that shard for ever in the mirror image of the defect
/// it fixed.
///
/// OBJECT MUTATIONS is invariant across all five. It is also exactly what the delay costs: replay
/// re-applies one per mutation, and reclaim cannot drop a record until a dump covers it. It is
/// reported as `undumped_wal_objects` and NOT yet compared against the threshold -- that is a
/// cadence decision with a default in a new unit behind it. The direction is safe either way: a
/// record carries at least one mutation, so the mutation count is never below the record count and
/// switching the gate can only make it fire EARLIER.
#[test]
fn the_dump_budget_moves_with_batching_in_records_and_in_bytes_but_not_in_object_mutations() {
    const RECORDS: usize = 2_000;

    fn shape(engine: &TemporalEngine) -> (u64, u64, u64, usize) {
        let plan = engine.storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            ..Default::default()
        });
        let undumped_bytes = engine.wal_store.undumped_len_since_dump(1);
        let dirty = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            shards
                .get(&1)
                .map(|shard| shard.dirty_objects.len())
                .unwrap_or(0)
        };
        (
            plan.undumped_wal_records,
            undumped_bytes,
            plan.undumped_wal_objects,
            dirty,
        )
    }

    fn one_batch_shape(batch: usize) -> (u64, u64, u64, usize) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = dump_engine(dir.path());
        seed_in_batches(&engine, 1, RECORDS, batch);
        let measured = shape(&engine);
        println!(
            "  {RECORDS} distinct keys, batches of {batch:<4} -> log records {:>5}, undumped \
             bytes {:>9}, dirty objects {:>5}, object mutations {:>5}",
            measured.0, measured.1, measured.3, measured.2
        );
        measured
    }

    let (records_1, bytes_1, objects_1, dirty_1) = one_batch_shape(1);
    let (records_10, bytes_10, objects_10, dirty_10) = one_batch_shape(10);
    let (records_100, bytes_100, objects_100, dirty_100) = one_batch_shape(100);
    let (records_500, bytes_500, objects_500, dirty_500) = one_batch_shape(500);

    // DENOMINATOR: the four arms must be the same corpus.
    assert_eq!(
        (dirty_1, dirty_10, dirty_100, dirty_500),
        (RECORDS, RECORDS, RECORDS, RECORDS),
        "the four batch arms are not the same corpus: dirty objects {dirty_1} / {dirty_10} / \
         {dirty_100} / {dirty_500}"
    );
    assert!(
        bytes_1 > 0 && objects_1 > 0,
        "an accumulator read 0 after {RECORDS} writes (bytes {bytes_1}, objects {objects_1}), so \
         every comparison below is vacuous"
    );

    // HALF ONE: neither quantity the budget can compare today survives batching.
    let record_spread = records_1 as f64 / records_500.max(1) as f64;
    let byte_spread = bytes_1.max(bytes_500) as f64 / bytes_1.min(bytes_500).max(1) as f64;
    println!(
        "  log records {records_1} / {records_10} / {records_100} / {records_500} = \
         {record_spread:.1}x spread; undumped bytes {bytes_1} / {bytes_10} / {bytes_100} / \
         {bytes_500} = {byte_spread:.2}x spread"
    );
    assert_eq!(
        records_1, RECORDS as u64,
        "one write per batch must produce one log record per object"
    );
    assert_eq!(
        records_500,
        (RECORDS / 500) as u64,
        "a batch must be ONE log record however many objects it carries -- that is the mechanism \
         this test is about"
    );
    assert!(
        byte_spread > 2.0,
        "undumped bytes moved only {byte_spread:.2}x across batch shapes carrying identical \
         objects ({bytes_1} vs {bytes_500}). If per-record framing stopped dominating a small \
         write, the byte threshold IS batching-invariant and the table in this test's doc comment \
         is stale."
    );

    // HALF TWO, ASSERTED SEPARATELY: the mutation count does survive it.
    println!(
        "  object mutations {objects_1} / {objects_10} / {objects_100} / {objects_500}"
    );
    assert_eq!(
        (objects_1, objects_10, objects_100, objects_500),
        (
            RECORDS as u64,
            RECORDS as u64,
            RECORDS as u64,
            RECORDS as u64
        ),
        "object mutations moved with the batch shape: {objects_1} / {objects_10} / {objects_100} \
         / {objects_500} against {RECORDS} objects written in every arm"
    );

    // HALF THREE: why the dirty-object count -- invariant above -- is still the wrong quantity.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = dump_engine(dir.path());
    let mut written = 0usize;
    while written < RECORDS {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: vec![Command::StringSet {
                key: "one-key".to_string(),
                value: vec![b'v'; 128],
            }],
        });
        assert!(response.status.ok, "overwrite failed: {:?}", response.status);
        written += 1;
    }
    let (overwrite_records, overwrite_bytes, overwrite_objects, overwrite_dirty) = shape(&engine);
    println!(
        "  {RECORDS} writes to ONE key -> log records {overwrite_records}, undumped bytes \
         {overwrite_bytes}, dirty objects {overwrite_dirty}, object mutations {overwrite_objects}"
    );
    assert_eq!(
        overwrite_dirty, 1,
        "the overwrite arm left {overwrite_dirty} dirty objects, not 1, so it is not the shape \
         that makes a dirty-object budget wrong"
    );
    assert_eq!(
        overwrite_objects, RECORDS as u64,
        "object mutations read {overwrite_objects} after {RECORDS} writes to one key; a budget \
         counting them would be as blind to overwrite as one counting dirty objects"
    );
}
