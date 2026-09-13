// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a write leaves behind for the reclaim floor to stand on.
//!
//! A dirty bucket that no dump manifest covers is allowed to hold the two logs only from its own
//! OLDEST UNDUMPED WRITE, and it has to be able to name that point in BOTH of them --
//! `first_dirty_wal_sequence` and `first_dirty_index_log_sequence`. A bucket that can name neither
//! is not treated as needing nothing; it is treated as unknown, and unknown refuses the whole
//! reclaim plan (`slot_generation_without_durable_dump`). So a write path that dirties buckets
//! without stamping the two claims stops the logs being reclaimed at all until a dump happens to
//! cover every bucket it touched -- and a dump is capped per round.

use super::*;

const RECLAIM_DUMP_SHARD: ShardId = 1;

/// Both halves of every bucket's claim, read straight off the bucket index.
///
/// Returned as two numbers per bucket and never as one total. The plan REQUIRES both halves and
/// they count in different sequences, so a state where one half is whole and the other is zero is
/// exactly the failure a combined count would report as "half full".
fn claims_by_bucket(engine: &TemporalEngine, shard_id: ShardId) -> Vec<(u32, u64, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let Some(shard) = shards.get(&shard_id) else {
        return Vec::new();
    };
    let mut out = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| bucket.dirty)
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                bucket.first_dirty_wal_sequence,
                bucket.first_dirty_index_log_sequence,
            )
        })
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn reclaim_dump_engine(dir: &std::path::Path, name: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join(format!("{name}-cache")),
        dir.join(format!("{name}-pages")),
        dir.join(format!("{name}-indexes")),
    )
}

fn reclaim_dump_keys(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("claim-{index:04}")).collect()
}

/// The two write paths, one command at a time and one batch.
fn write_one_at_a_time(engine: &TemporalEngine, keys: &[String]) {
    for key in keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![118u8; 64],
            },
        });
        assert!(response.status.ok, "write {key} failed: {:?}", response.status);
    }
}

fn write_as_batches(engine: &TemporalEngine, keys: &[String], batch: usize) {
    for chunk in keys.chunks(batch) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![118u8; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            commands,
        });
        assert!(response.status.ok, "batch write failed: {:?}", response.status);
    }
}

/// Dump every dirty bucket, so the claims clear the way a real round clears them.
fn dump_everything(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        ..StorageManagerCycleRequest::default()
    });
}

fn wal_record_count(engine: &TemporalEngine) -> usize {
    let (records, _truncated) = engine
        .wal_store()
        .scan_decoded(RECLAIM_DUMP_SHARD, 0, u64::MAX, u64::MAX)
        .expect("scan wal");
    records.len()
}

/// A bucket dirtied by a BATCH write can name its oldest undumped write, as one dirtied by a
/// single-command write already could.
///
/// The single-command path stamps both halves of the claim (engine.rs, after the WAL append and
/// after the index-log delta). The batch path marks the same buckets dirty through the same
/// `mark_async_dirty_object` and appends to both logs, and stamped neither -- so every bucket it
/// dirtied reported "no claim recorded", which the reclaim plan reads as unknown and refuses on.
///
/// The single-command arm is the CONTROL: it proves the fixture can produce claims at all, so a
/// zero on the batch arm is the path and not the fixture.
#[test]
fn a_bucket_dirtied_by_a_batch_write_can_name_its_oldest_undumped_write() {
    const KEYS: usize = 64;
    const BATCH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(KEYS);

    let one_at_a_time = reclaim_dump_engine(dir.path(), "single");
    one_at_a_time.load_shard(RECLAIM_DUMP_SHARD);
    write_one_at_a_time(&one_at_a_time, &keys);

    let batched = reclaim_dump_engine(dir.path(), "batch");
    batched.load_shard(RECLAIM_DUMP_SHARD);
    write_as_batches(&batched, &keys, BATCH);

    let single_claims = claims_by_bucket(&one_at_a_time, RECLAIM_DUMP_SHARD);
    let batch_claims = claims_by_bucket(&batched, RECLAIM_DUMP_SHARD);

    // DENOMINATORS FIRST. Without dirty buckets on both sides every count below is 0 == 0.
    assert!(
        !single_claims.is_empty(),
        "the control wrote {KEYS} keys and dirtied no bucket, so it proves nothing"
    );
    assert!(
        !batch_claims.is_empty(),
        "the batch arm wrote {KEYS} keys and dirtied no bucket, so it proves nothing"
    );
    assert_eq!(
        single_claims
            .iter()
            .map(|(bucket, _, _)| *bucket)
            .collect::<Vec<_>>(),
        batch_claims
            .iter()
            .map(|(bucket, _, _)| *bucket)
            .collect::<Vec<_>>(),
        "the two arms dirtied different buckets, so they are not comparable"
    );

    // THE CONTROL, both halves separately.
    let single_wal = single_claims
        .iter()
        .filter(|(_, wal, _)| *wal > 0)
        .count();
    let single_index_log = single_claims
        .iter()
        .filter(|(_, _, index_log)| *index_log > 0)
        .count();
    assert_eq!(
        single_wal,
        single_claims.len(),
        "the single-command control left a dirty bucket with no WAL claim: {single_claims:?}"
    );
    assert_eq!(
        single_index_log,
        single_claims.len(),
        "the single-command control left a dirty bucket with no index-log claim: \
         {single_claims:?}"
    );

    // THE SUBJECT, the same two halves, never added together.
    let batch_wal = batch_claims.iter().filter(|(_, wal, _)| *wal > 0).count();
    let batch_index_log = batch_claims
        .iter()
        .filter(|(_, _, index_log)| *index_log > 0)
        .count();
    assert_eq!(
        batch_wal,
        batch_claims.len(),
        "{} of {} batch-dirtied buckets could not name their oldest undumped write in the WAL, so \
         the reclaim plan reads them as unknown and refuses: {batch_claims:?}",
        batch_claims.len() - batch_wal,
        batch_claims.len()
    );
    assert_eq!(
        batch_index_log,
        batch_claims.len(),
        "{} of {} batch-dirtied buckets could not name their oldest undumped write in the index \
         log: {batch_claims:?}",
        batch_claims.len() - batch_index_log,
        batch_claims.len()
    );

    // A CLAIM MUST NOT OVERSTATE. `retain_from = claim - 1 + 1`, so a claim ABOVE the bucket's
    // oldest undumped write frees records that bucket still needs. Every claim must sit at or
    // below the first sequence the arm ever wrote, and no batch bucket may claim a point later
    // than the control's own bucket does.
    let batch_oldest = batch_claims
        .iter()
        .map(|(_, wal, _)| *wal)
        .min()
        .expect("a claim");
    assert_eq!(
        batch_oldest, 1,
        "the oldest batch claim is {batch_oldest}, not the shard's first record; a claim above a \
         bucket's oldest undumped write frees records it still needs"
    );
}

/// The consequence: a shard written in batches can have its logs reclaimed.
///
/// A bucket falls back on its own claims exactly when no dump manifest covers it, and on any real
/// shard a capped dump leaves buckets there every round. With no claims to fall back on every one
/// of them lands in `missing_bucket_generations`, `safe_to_reclaim` is false, the WAL frees
/// nothing and index-log GC is skipped for "durable WAL/index frontier not safe".
///
/// The fixture reaches that state the way a running shard does: write, let a cycle dump and clear
/// the claims, then write again. The second write is what the surviving floor has to stand on.
/// The single-command arm runs the IDENTICAL sequence and is the control -- it is what says the
/// numbers below come from the write path rather than from the fixture.
#[test]
fn a_batch_written_shard_reclaims_the_way_a_single_command_one_does() {
    const KEYS: usize = 64;
    const BATCH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(KEYS);
    let more = (0..KEYS)
        .map(|index| format!("second-{index:04}"))
        .collect::<Vec<_>>();

    let one_at_a_time = reclaim_dump_engine(dir.path(), "single");
    one_at_a_time.load_shard(RECLAIM_DUMP_SHARD);
    write_one_at_a_time(&one_at_a_time, &keys);
    dump_everything(&one_at_a_time);
    write_one_at_a_time(&one_at_a_time, &more);

    let batched = reclaim_dump_engine(dir.path(), "batch");
    batched.load_shard(RECLAIM_DUMP_SHARD);
    write_as_batches(&batched, &keys, BATCH);
    dump_everything(&batched);
    write_as_batches(&batched, &more, BATCH);

    let single_plan =
        one_at_a_time.storage_wal_reclaim_plan(RECLAIM_DUMP_SHARD, Vec::new(), Vec::new());
    let batch_plan = batched.storage_wal_reclaim_plan(RECLAIM_DUMP_SHARD, Vec::new(), Vec::new());

    // DENOMINATORS FIRST.
    //
    // A plan over a shard with no buckets, or one with nothing left in the log, would satisfy
    // every assertion below without the reclaim having anything to do.
    assert!(
        single_plan.covered_bucket_count + single_plan.uncovered_bucket_count > 0,
        "the control plan saw no buckets at all, so it proves nothing: {single_plan:?}"
    );
    assert!(
        batch_plan.covered_bucket_count + batch_plan.uncovered_bucket_count > 0,
        "the batch plan saw no buckets at all, so it proves nothing: {batch_plan:?}"
    );
    let single_records_before = wal_record_count(&one_at_a_time);
    let batch_records_before = wal_record_count(&batched);
    assert!(
        single_records_before > 0 && batch_records_before > 0,
        "one of the two arms holds no WAL records, so a reclaim freeing nothing would pass \
         vacuously (control {single_records_before}, batch {batch_records_before})"
    );
    // The second write must have left dirty buckets, or there is nothing for a claim to anchor.
    let batch_dirty = claims_by_bucket(&batched, RECLAIM_DUMP_SHARD);
    assert!(
        !batch_dirty.is_empty(),
        "the second batch dirtied no bucket, so the claim branch is never reached"
    );

    // THE CONTROL: the single-command arm anchors and reclaims.
    assert!(
        single_plan.missing_bucket_generations.is_empty(),
        "the single-command control could not anchor its own buckets: {:?}",
        single_plan.missing_bucket_generations
    );
    assert!(
        single_plan.safe_to_reclaim,
        "the single-command control refuses its own reclaim, so it cannot be a control \
         (blockers {:?})",
        single_plan.blocker_reasons
    );

    // THE SUBJECT.
    assert!(
        batch_plan.missing_bucket_generations.is_empty(),
        "{} of {} buckets on the batch-written shard cannot be anchored, so the whole reclaim is \
         refused: {:?}",
        batch_plan.missing_bucket_generations.len(),
        batch_plan.covered_bucket_count + batch_plan.uncovered_bucket_count,
        batch_plan.missing_bucket_generations
    );
    assert!(
        batch_plan.safe_to_reclaim,
        "the batch-written shard refuses reclaim while the identically-written single-command \
         shard allows it (blockers {:?})",
        batch_plan.blocker_reasons
    );

    // Both halves of the frontier, separately: a plan that anchored one log and not the other
    // would read as safe on a combined view and free nothing.
    assert!(
        batch_plan.retain_from_wal_sequence > 0,
        "the batch plan named no WAL retain point: {batch_plan:?}"
    );
    assert!(
        batch_plan.retain_from_index_log_sequence > 0,
        "the batch plan named no index-log retain point: {batch_plan:?}"
    );

    // And it actually frees, on both arms, counted separately.
    let single_removed = one_at_a_time
        .apply_storage_wal_reclaim(single_plan)
        .wal_records_removed;
    let batch_removed = batched.apply_storage_wal_reclaim(batch_plan).wal_records_removed;
    assert!(
        single_removed > 0,
        "the control reclaim freed nothing out of {single_records_before} records"
    );
    assert!(
        batch_removed > 0,
        "the batch-written shard freed {batch_removed} of {batch_records_before} WAL records \
         while the control freed {single_removed} of {single_records_before}"
    );
}

/// The same claim, at corpus scale.
///
/// #1623's healthy small column did not generalise, so this asks the question again at a size
/// where the dirty set is far larger than a round's dump cap -- which is the state the claim
/// branch exists for. `TS_RECLAIM_DUMP_SCALE` raises the corpus without editing the test; the
/// default is the size this runs at on every gate.
#[test]
fn a_batch_written_corpus_anchors_every_bucket_it_dirties() {
    let keys_count = std::env::var("TS_RECLAIM_DUMP_SCALE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8_000);
    const BATCH: usize = 500;

    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(keys_count);
    let engine = reclaim_dump_engine(dir.path(), "scale");
    engine.load_shard(RECLAIM_DUMP_SHARD);
    write_as_batches(&engine, &keys, BATCH);

    let claims = claims_by_bucket(&engine, RECLAIM_DUMP_SHARD);
    // DENOMINATOR: the corpus has to dirty more buckets than one round may dump, or the claim
    // branch is never the thing being tested.
    assert!(
        claims.len() > 64,
        "{keys_count} keys dirtied only {} buckets, which a single uncapped round would dump \
         whole -- the claim branch is never reached",
        claims.len()
    );

    // The two halves, counted apart.
    let missing_wal = claims.iter().filter(|(_, wal, _)| *wal == 0).count();
    let missing_index_log = claims
        .iter()
        .filter(|(_, _, index_log)| *index_log == 0)
        .count();
    assert_eq!(
        0, missing_wal,
        "{missing_wal} of {} dirty buckets at {keys_count} records hold no WAL claim",
        claims.len()
    );
    assert_eq!(
        0, missing_index_log,
        "{missing_index_log} of {} dirty buckets at {keys_count} records hold no index-log claim",
        claims.len()
    );

    // No claim may sit above the bucket's oldest undumped write. Every key was written once, in
    // ascending batches, so the oldest claim is the shard's first record.
    let oldest = claims.iter().map(|(_, wal, _)| *wal).min().expect("a claim");
    assert_eq!(
        1, oldest,
        "the oldest claim at {keys_count} records is {oldest}, not the shard's first record"
    );
    println!(
        "reclaim_dump scale: keys={keys_count} dirty_buckets={} wal_claims={} index_log_claims={}",
        claims.len(),
        claims.len() - missing_wal,
        claims.len() - missing_index_log
    );
}
