// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc::channel;
use std::time::Duration;

const DEAD: &str = "qualified-expired";
const INDEX_PREDECESSOR: &str = "qualified-index-predecessor";

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn exact(engine: &TemporalEngine, key: &str, value: Option<&[u8]>) {
    assert_eq!(run(engine, Command::StringGet { key: key.into() }),
        CommandResponse::Bytes { value: value.map(<[u8]>::to_vec) });
}

fn open(root: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn actual_index_receipt(engine: &TemporalEngine, key: &str, before: u64, wal: u64) -> u64 {
    let sequence = engine.index_log_store.stats(1).last_sequence;
    assert!(sequence > before);
    let records = engine.index_log_store.read_delta_records(1, before).unwrap();
    let receipt = records.iter().find(|record| record.sequence == sequence)
        .expect("the stats coordinate must name an actual emitted IndexLog record");
    assert_eq!(receipt.applied_wal_sequence, Some(wal));
    assert!(receipt.items.iter().any(|item| item.object_key.as_ref() == key));
    sequence
}

fn exact_index_predecessor(engine: &TemporalEngine) {
    assert_eq!(run(engine, Command::SetMembers { key: INDEX_PREDECESSOR.into() }),
        CommandResponse::Members { members: vec![b"anchor".to_vec()] });
}

fn seed(root: &Path) -> (TemporalEngine, u32, String) {
    let engine = open(root);
    assert_eq!(engine.capture_shard_checkpoint(1).unwrap().0.source_owner_range, Some((0, u32::MAX)));
    let before_index = engine.index_log_store.stats(1).last_sequence;
    let before_wal = engine.wal_store.stats_without_rescan(1).last_sequence;
    // Set writes do not take String's deferred value-once path.
    let predecessor = engine.execute_durable(ExecuteRequest { shard_id: 1,
        command: Command::SetAdd { key: INDEX_PREDECESSOR.into(), member: b"anchor".to_vec() } });
    assert!(predecessor.status.ok, "{:?}", predecessor.status);
    let predecessor_wal = engine.wal_store.stats_without_rescan(1).last_sequence;
    assert!(predecessor_wal > before_wal);
    let predecessor_index = actual_index_receipt(&engine, INDEX_PREDECESSOR, before_index, predecessor_wal);
    let predecessor_bucket = engine.routing_bucket_for_key(1, INDEX_PREDECESSOR);
    engine.shards.with_state(1, |state| {
        let node = state.unwrap().bucket_index.bucket_map.get(&predecessor_bucket).unwrap();
        assert_eq!(node.first_dirty_wal_sequence, predecessor_wal);
        assert_eq!(node.first_dirty_index_log_sequence, predecessor_index);
    });
    exact_index_predecessor(&engine);
    assert!(engine.set_config(SetConfigRequest {
        shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
    }).ok);
    run(&engine, Command::StringSet { key: "qualified-predecessor".into(), value: b"anchor".to_vec() });
    // The manifest validates portable slab dependencies. Move this real WAL-backed
    // value to a physical slab while preserving the async write and IndexLog cut.
    assert_eq!(engine.materialize_synthetic_blocks(1), 1);
    let prime = engine.create_bucket_dump_manifest_and_clear(
        1, vec![engine.routing_bucket_for_key(1, "qualified-predecessor")], true,
    ).unwrap();
    engine.validate_bucket_dump_manifest(&prime).unwrap();
    assert!(prime.wal_sequence > 0 && prime.index_log_sequence > 0);
    assert_eq!(prime.index_log_sequence, predecessor_index);
    let bucket = engine.routing_bucket_for_key(1, DEAD);
    assert_ne!(predecessor_bucket, bucket);
    let survivor = (0..32).map(|n| format!("qualified-survivor-{n}"))
        .find(|key| engine.routing_bucket_for_key(1, key) != bucket).unwrap();
    let expiry_index;
    {
        let _clock = ReplayClockGuard::enter(Some(1000));
        run(&engine, Command::StringSetEx { key: DEAD.into(), value: b"expired".to_vec(), ttl_ms: 10 });
        let before_index = engine.index_log_store.stats(1).last_sequence;
        let expiry = engine.execute_durable(ExecuteRequest { shard_id: 1,
            command: Command::CommonExpire { key: DEAD.into(), ttl_ms: 10 } });
        assert!(expiry.status.ok, "{:?}", expiry.status);
        assert_eq!(expiry.response, CommandResponse::Empty);
        expiry_index = actual_index_receipt(&engine, DEAD, before_index,
            engine.wal_store.stats_without_rescan(1).last_sequence);
        run(&engine, Command::StringSet { key: survivor.clone(), value: b"survivor".to_vec() });
    }
    {
        let _clock = ReplayClockGuard::enter(Some(1011));
        assert_eq!(run(&engine, Command::CommonTtl { key: DEAD.into() }),
            CommandResponse::Integer { value: -2 });
    }
    assert_eq!(engine.materialize_synthetic_blocks(1), 1);
    assert_eq!(engine.synthetic_address_count_for_test(1), 0);
    let state = engine.capture_shard_checkpoint(1).unwrap().0;
    assert!(!state.strings().contains_key(DEAD));
    assert!(!state.expires_at_ms().contains_key(DEAD));
    let node = state.bucket_index.bucket_map.get(&bucket).expect("real expiry retained pending authority");
    assert!(node.dirty() && node.deleted() && node.block_index.is_empty());
    assert!(!node.deleted_object_index.is_empty());
    assert!(node.first_dirty_wal_sequence > 0 && node.first_dirty_index_log_sequence > 0);
    assert_eq!(node.first_dirty_index_log_sequence, expiry_index);
    (engine, bucket, survivor)
}

#[derive(Debug, PartialEq, Eq)]
struct Authority {
    generation: u64,
    wal: u64,
    index: u64,
    dirty: bool,
    deleted: bool,
    ids: BTreeSet<u64>,
    objects: BTreeSet<u64>,
    debt: u64,
}

fn authority(engine: &TemporalEngine, bucket: u32) -> Authority {
    engine.shards.with_state(1, |state| {
        let state = state.unwrap();
        let node = state.bucket_index.bucket_map.get(&bucket).unwrap();
        Authority {
            generation: node.dirty_generation, wal: node.first_dirty_wal_sequence,
            index: node.first_dirty_index_log_sequence, dirty: node.dirty(), deleted: node.deleted(),
            ids: node.deleted_object_index.iter().copied().collect(),
            objects: node.object_index.iter().copied().collect(),
            debt: state.dirty_objects.bucket_count(bucket),
        }
    })
}

fn absent_owner(engine: &TemporalEngine, bucket: u32) {
    engine.shards.with_state(1, |state| {
        let state = state.unwrap();
        assert!(!state.bucket_index.bucket_map.contains_key(&bucket));
        assert_eq!(state.dirty_objects.bucket_count(bucket), 0);
    });
}

#[test]
fn real_expiry_equal_anchor_base_reopens_without_retired_owner_or_lost_survivor() {
    let root = tempfile::tempdir().unwrap();
    let (engine, bucket, survivor) = seed(root.path());
    let before = authority(&engine, bucket);
    let (anchor, _) = engine.dump_index_catalog_anchored(1).expect("real durable base checkpoint");
    assert_eq!(authority(&engine, bucket), before);
    let base_bytes = fs::read(engine.index_path(1)).unwrap();
    let base = decode_index_bytes(&base_bytes).unwrap();
    assert_eq!(base.source_owner_range, Some((0, u32::MAX)));
    assert!(base.bucket_index.bucket_map.contains_key(&bucket));
    let manifest = engine.create_bucket_dump_manifest_and_clear(1, vec![bucket], true).unwrap();
    assert_eq!(manifest.wal_sequence, anchor, "must exercise equal-anchor recovery, not a newer image");
    assert_eq!(manifest.bucket_ids, vec![bucket], "empty selected coverage must not expand to ALL");
    assert_eq!(manifest.live_block_refs, 0);
    assert_eq!(manifest.bucket_summaries.len(), 1);
    assert_eq!(manifest.bucket_summaries[0].object_count, 0);
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    let normalized = decode_index_bytes(&manifest.index_bytes).unwrap();
    assert!(!normalized.bucket_index.bucket_map.contains_key(&bucket));
    assert!(normalized.strings().contains_key(survivor.as_str()));
    absent_owner(&engine, bucket);
    assert_eq!(fs::read(engine.index_path(1)).unwrap(), base_bytes,
        "cleanup must not be hidden by another base-image write");
    drop(engine); // No public unload/flush that could rewrite the old base.
    let reopened = open(root.path());
    absent_owner(&reopened, bucket); // Check BEFORE a serving read could lazily repair anything.
    exact(&reopened, DEAD, None);
    exact(&reopened, &survivor, Some(b"survivor"));
    exact(&reopened, "qualified-predecessor", Some(b"anchor"));
    exact_index_predecessor(&reopened);
}

#[test]
fn chosen_prefix_cleanup_does_not_clear_a_readded_wal_suffix_owner() {
    let root = tempfile::tempdir().unwrap();
    let (engine, bucket, survivor) = seed(root.path());
    let (anchor, _) = engine.dump_index_catalog_anchored(1).unwrap();
    let base_bytes = fs::read(engine.index_path(1)).unwrap();
    engine.create_bucket_dump_manifest_and_clear(1, vec![bucket], true).unwrap();
    absent_owner(&engine, bucket);
    let before_index = engine.index_log_store.stats(1).last_sequence;
    run(&engine, Command::StringSet { key: DEAD.into(), value: b"suffix".to_vec() });
    let tail = engine.wal_store.stats_without_rescan(1).last_sequence;
    assert!(tail > anchor);
    assert_eq!(engine.index_log_store.stats(1).last_sequence, before_index,
        "this real async suffix has no IndexLog append receipt");
    engine.wal_store.flush(1).unwrap();
    let (records, truncated) = engine.wal_store.scan_decoded(1, 0, u64::MAX, 1 << 20).unwrap();
    assert!(!truncated);
    let suffix: Vec<_> = records.iter().filter(|(_, record)| record.sequence > anchor).collect();
    assert_eq!(suffix.len(), 1);
    assert_eq!(suffix[0].1.sequence, tail);
    assert!(suffix[0].1.outcomes.iter().any(|item| item.object_key == DEAD));
    assert_eq!(fs::read(engine.index_path(1)).unwrap(), base_bytes,
        "the fixture needs a genuine newer suffix, not a newer base");
    drop(engine);
    let reopened = open(root.path());
    let after = authority(&reopened, bucket);
    assert!(after.dirty && !after.deleted);
    assert_eq!(after.wal, tail, "the pending claim must name the actual replayed WAL record");
    assert_eq!(after.index, 0, "no IndexLog receipt may be inferred from a WAL sequence");
    assert!(after.debt > 0);
    assert!(after.ids.is_empty());
    exact(&reopened, DEAD, Some(b"suffix"));
    exact(&reopened, &survivor, Some(b"survivor"));
    exact_index_predecessor(&reopened);
    let cycle = reopened.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1, enable_prepare: false, enable_wal_reclaim: true,
        enable_evict: false, enable_expire: false, enable_block_reclaim: true,
        enable_block_compaction: false, enable_index_gc: false,
        min_undumped_wal_records: 0, min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        ..StorageManagerCycleRequest::default()
    });
    assert!(cycle.errors.is_empty(), "{cycle:?}");
    let published = cycle.lifecycle_report.as_ref().and_then(|stage| stage.dump_manifest.as_ref())
        .expect("replayed pending work must publish actual durable coverage");
    assert_eq!(published.wal_sequence, tail);
    reopened.validate_bucket_dump_manifest(published).unwrap();
    let persisted = reopened.list_bucket_dump_manifests(1).into_iter()
        .find(|manifest| manifest.manifest_id == published.manifest_id)
        .expect("the actual published checkpoint must be readable from durable metadata");
    assert_eq!(persisted.checksum, published.checksum);
    assert_eq!(persisted.index_sha256, published.index_sha256);
    assert_eq!(decode_index_bytes(installable_image(&persisted).unwrap()).unwrap().applied_wal_sequence, Some(tail));
    let reclaimed = cycle.wal_reclaim_report.as_ref().expect("normal cycle must report reclaim");
    assert!(reclaimed.applied, "a refusal is not a passing reclaim control: {reclaimed:?}");
    assert!(reclaimed.wal_records_removed > 0 || reclaimed.wal_segments_dropped > 0,
        "the physical synchronous prefix provides genuinely superseded WAL work: {reclaimed:?}");
    exact(&reopened, DEAD, Some(b"suffix"));
    exact(&reopened, &survivor, Some(b"survivor"));
    drop(reopened); // No unload that could mask the checkpoint/reclaim boundary.
    let final_reopen = open(root.path());
    exact(&final_reopen, DEAD, Some(b"suffix"));
    exact(&final_reopen, &survivor, Some(b"survivor"));
    exact(&final_reopen, "qualified-predecessor", Some(b"anchor"));
    exact_index_predecessor(&final_reopen);
}

#[test]
fn unknown_and_cross_range_authority_refuse_cleanup_without_changing_data_or_ttl() {
    let root = tempfile::tempdir().unwrap();
    let (engine, bucket, survivor) = seed(root.path());
    run(&engine, Command::StringSetEx {
        key: "qualified-live-ttl".into(), value: b"ttl".to_vec(), ttl_ms: 60_000,
    });
    let mut unknown = engine.capture_shard_checkpoint(1).unwrap().0;
    unknown.source_owner_range = None; // Unknown image control, NOT a claimed physical v32 fixture.
    let before = serialize_index(&unknown);
    qualified_empty_owners::reconcile_covered_image(&mut unknown);
    assert_eq!(serialize_index(&unknown), before);
    assert!(unknown.bucket_index.bucket_map.contains_key(&bucket));
    assert!(unknown.expires_at_ms().contains_key("qualified-live-ttl"));
    assert!(unknown.strings().contains_key(survivor.as_str()));
    let mut changed_range = engine.capture_shard_checkpoint(1).unwrap().0;
    changed_range.set_routing_range(0, 63);
    assert_eq!(changed_range.source_owner_range, None);
    let before = serialize_index(&changed_range);
    qualified_empty_owners::reconcile_covered_image(&mut changed_range);
    assert_eq!(serialize_index(&changed_range), before);
    assert!(changed_range.bucket_index.bucket_map.contains_key(&bucket));
    exact(&engine, &survivor, Some(b"survivor"));
    exact(&engine, "qualified-live-ttl", Some(b"ttl"));
}

struct ResetHooks;
impl Drop for ResetHooks {
    fn drop(&mut self) {
        BEFORE_DUMP_DIRTY_COMPLETION.with(|hook| { hook.borrow_mut().take(); });
        FAIL_EMPTY_OWNER_AFTER_METADATA.with(|fail| fail.set(false));
    }
}

#[test]
fn post_rename_publication_failure_preserves_exact_live_authority_for_retry() {
    let root = tempfile::tempdir().unwrap();
    let (engine, bucket, survivor) = seed(root.path());
    let before = authority(&engine, bucket);
    let _reset = ResetHooks;
    FAIL_EMPTY_OWNER_AFTER_METADATA.with(|fail| fail.set(true));
    assert!(engine.create_bucket_dump_manifest_and_clear(1, vec![bucket], true).is_err());
    assert_eq!(authority(&engine, bucket), before);
    exact(&engine, &survivor, Some(b"survivor"));
    let retried = engine.create_bucket_dump_manifest_and_clear(1, vec![bucket], true).unwrap();
    engine.validate_bucket_dump_manifest(&retried).unwrap();
    absent_owner(&engine, bucket);
}

#[test]
fn readd_with_real_ttl_after_capture_keeps_new_claims_and_owner() {
    let root = tempfile::tempdir().unwrap();
    let (engine, bucket, survivor) = seed(root.path());
    let (published_tx, published_rx) = channel();
    let (resume_tx, resume_rx) = channel();
    let worker_engine = engine.clone();
    let worker = std::thread::spawn(move || {
        let _reset = ResetHooks;
        BEFORE_DUMP_DIRTY_COMPLETION.with(|cell| {
            *cell.borrow_mut() = Some(Box::new(move || {
                published_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(20)).unwrap();
            }));
        });
        worker_engine.create_bucket_dump_manifest_and_clear(1, vec![bucket], true)
    });
    published_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    run(&engine, Command::StringSetEx { key: DEAD.into(), value: b"readded".to_vec(), ttl_ms: 60_000 });
    let before = authority(&engine, bucket);
    assert!(before.dirty && !before.deleted && before.ids.is_empty());
    assert!(before.wal > 0 && before.index > 0);
    resume_tx.send(()).unwrap();
    assert!(worker.join().unwrap().is_ok());
    assert_eq!(authority(&engine, bucket), before);
    engine.shards.with_state(1, |state| assert!(state.unwrap().expires_at_ms().contains_key(DEAD)));
    exact(&engine, DEAD, Some(b"readded"));
    exact(&engine, &survivor, Some(b"survivor"));
}

#[test]
fn current_format_roundtrips_source_authority_and_does_not_alias_string_version33() {
    let root = tempfile::tempdir().unwrap();
    let (engine, _, _) = seed(root.path());
    let image = engine.capture_shard_checkpoint(1).unwrap().0;
    let mut bytes = serialize_index(&image);
    let offset = INDEX_CONTAINER_MAGIC.len() + 1;
    assert_eq!(&bytes[offset..offset + 4], &SHARD_INDEX_FORMAT_VERSION.to_be_bytes());
    assert_eq!(decode_index_bytes(&bytes).unwrap().source_owner_range, Some((0, u32::MAX)));
    bytes[offset..offset + 4].copy_from_slice(&33_u32.to_be_bytes());
    assert!(decode_index_bytes(&bytes).is_err(), "String33 is not this payload schema");
}

#[test]
fn legacy32_header_cannot_mint_authority_from_an_added_payload_field() {
    let root = tempfile::tempdir().unwrap();
    let (engine, _, _) = seed(root.path());
    let image = engine.capture_shard_checkpoint(1).unwrap().0;
    let mut bytes = serialize_index(&image);
    let offset = INDEX_CONTAINER_MAGIC.len() + 1;
    // Deliberately forged extra-field negative control, NOT the original-v32 migration proof.
    bytes[offset..offset + 4].copy_from_slice(&32_u32.to_be_bytes());
    let decoded = decode_index_bytes(&bytes).unwrap();
    assert_eq!(decoded.source_owner_range, None);
    assert_eq!(decoded.index_format_version, SHARD_INDEX_FORMAT_VERSION);
}
