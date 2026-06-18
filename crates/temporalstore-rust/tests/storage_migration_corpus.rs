use std::fs;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use temporalstore_rust::engine::SlotDumpFollowerReplayCursor;
use temporalstore_rust::{
    execute_redis_command, Command, CommandResponse, ExecuteRequest, RaftCluster, RaftConfig,
    RespValue, SharedStoreReplicator, SharedStoreStorageMode, StorageLifecycleRequest,
    TemporalEngine,
};
use temporalstore_snapshot::FileObjectStore;

#[derive(Debug, Deserialize)]
struct StorageMigrationCorpus {
    schema_version: u32,
    name: String,
    source_format: String,
    format_compatibility: String,
    cases: Vec<StorageMigrationCase>,
}

#[derive(Debug, Deserialize)]
struct StorageMigrationCase {
    name: String,
    shard_id: u64,
    operations: Vec<StorageMigrationStep>,
    expected_reads: Vec<StorageMigrationStep>,
}

#[derive(Debug, Clone, Deserialize)]
struct StorageMigrationStep {
    name: String,
    #[serde(default)]
    storage_mutation: bool,
    command: Command,
    #[serde(default)]
    expect: Option<CommandResponse>,
}

#[tokio::test]
async fn rust_storage_replays_cpp_migration_corpus_across_lifecycle_paths() {
    let corpus = load_corpus();

    for case in &corpus.cases {
        verify_engine_dump_load_recovery(case);
        verify_shared_store_replay(case, SharedStoreStorageMode::Sync).await;
        verify_shared_store_replay(case, SharedStoreStorageMode::Async).await;
        verify_raft_replication(case);
    }
}

fn load_corpus() -> StorageMigrationCorpus {
    let corpus_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("compat/storage_migration_corpus.json");
    let corpus_bytes = fs::read(&corpus_path).expect("storage migration corpus should be readable");
    let corpus: StorageMigrationCorpus =
        serde_json::from_slice(&corpus_bytes).expect("storage migration corpus should deserialize");

    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.name, "temporalstore-storage-migration-corpus");
    assert_eq!(corpus.source_format, "cpp_exported_logical_artifacts_v1");
    assert_eq!(
        corpus.format_compatibility,
        "migration_only_rust_native_pages"
    );
    assert!(
        !corpus.cases.is_empty(),
        "storage corpus must contain cases"
    );
    corpus
}

fn verify_engine_dump_load_recovery(case: &StorageMigrationCase) {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let mut engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);

    execute_steps(&engine, case.shard_id, &case.operations, &case.name);

    let summaries = engine.slot_storage_summaries(case.shard_id);
    assert!(
        !summaries.is_empty(),
        "case={} should create slot ownership summaries",
        case.name
    );
    assert!(
        summaries
            .iter()
            .any(|summary| summary.dirty_generation > 0 && summary.page_ref_count > 0),
        "case={} should track dirty slot generations and page refs",
        case.name
    );
    let dirty_slots = summaries
        .iter()
        .filter(|summary| summary.dirty_generation > 0)
        .map(|summary| summary.routing_slot)
        .collect::<Vec<_>>();
    assert!(!dirty_slots.is_empty());
    let installable_manifest = engine
        .create_slot_dump_manifest(case.shard_id, Vec::new())
        .expect("explicit slot dump manifest should be created");
    assert!(
        !installable_manifest.index_bytes.is_empty(),
        "explicit slot dump manifest must include index bytes"
    );

    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: case.shard_id,
        selected_dump_slots: dirty_slots,
        max_dump_slots_per_round: 64,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: true,
        prune_slot_dump_manifests: true,
        roll_forward_slot_dump_installs: true,
        follower_replay_cursors: vec![SlotDumpFollowerReplayCursor {
            follower_id: "lagging-storage-corpus-follower".to_string(),
            shard_id: case.shard_id,
            oplog_sequence: 0,
            index_log_sequence: 0,
        }],
        invalidate_cache: true,
        warm_cache: true,
    });
    let report_manifest = lifecycle
        .dump_manifest
        .as_ref()
        .expect("storage lifecycle should write a slot dump manifest");
    assert!(!report_manifest.checksum.is_empty());
    assert!(!report_manifest.slot_summaries.is_empty());
    assert!(
        lifecycle.cache_warmup.considered_page_refs > 0,
        "case={} cache warmup should inspect page refs",
        case.name
    );
    assert_eq!(
        lifecycle.cache_warmup.failed_page_refs, 0,
        "case={} cache warmup should not fail page reads",
        case.name
    );

    assert_clean_recovery(&engine, case.shard_id, &case.name);

    drop(engine);
    engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);
    engine
        .install_slot_dump_manifest(&installable_manifest)
        .unwrap_or_else(|status| {
            panic!(
                "case={} slot dump manifest install failed after restart: {:?}",
                case.name, status
            )
        });
    assert_clean_recovery(&engine, case.shard_id, &case.name);
    execute_steps(&engine, case.shard_id, &case.expected_reads, &case.name);
    verify_redis_admin_replay(&engine, case);
}

async fn verify_shared_store_replay(case: &StorageMigrationCase, mode: SharedStoreStorageMode) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileObjectStore::new(
        dir.path().join(format!("objects-{mode:?}")),
    ));
    let replicator = SharedStoreReplicator::new("storage-migration-corpus", store);
    let writer = replicator.storage_writer(mode, 1);

    for step in case.operations.iter().filter(|step| step.storage_mutation) {
        writer
            .write(case.shard_id, step.command.clone())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "case={} step={} shared-store write failed: {error}",
                    case.name, step.name
                )
            });
    }
    if mode == SharedStoreStorageMode::Async {
        while writer.queued_len() > 0 {
            writer
                .flush_pending(1)
                .await
                .expect("async shared-store corpus flush should succeed");
        }
    }

    let follower_dir = tempfile::tempdir().unwrap();
    let follower = new_engine(
        follower_dir.path(),
        &follower_dir.path().join("pages"),
        &follower_dir.path().join("indexes"),
        case.shard_id,
    );
    let replay = replicator
        .replay_oplog_strict(case.shard_id, 0, &follower)
        .await
        .expect("shared-store corpus replay should succeed");
    assert_eq!(
        replay.applied,
        case.operations
            .iter()
            .filter(|step| step.storage_mutation)
            .count()
    );
    execute_steps(&follower, case.shard_id, &case.expected_reads, &case.name);
    assert_clean_recovery(&follower, case.shard_id, &case.name);
}

fn verify_raft_replication(case: &StorageMigrationCase) {
    let cluster =
        RaftCluster::new_single_shard_with_config(case.shard_id, [1, 2, 3], RaftConfig::default())
            .expect("corpus raft cluster should start");

    for step in case.operations.iter().filter(|step| step.storage_mutation) {
        let response = cluster
            .propose(step.command.clone())
            .unwrap_or_else(|error| {
                panic!(
                    "case={} step={} raft propose failed: {error}",
                    case.name, step.name
                )
            });
        if let Some(expected) = &step.expect {
            assert_eq!(
                &response, expected,
                "case={} step={} raft response mismatch",
                case.name, step.name
            );
        }
    }
    cluster
        .transfer_leader(2)
        .expect("raft leader transfer should succeed");
    for read in &case.expected_reads {
        let response = cluster
            .read_from_replica(2, read.command.clone())
            .unwrap_or_else(|error| {
                panic!(
                    "case={} step={} raft read failed: {error}",
                    case.name, read.name
                )
            });
        assert_eq!(
            Some(&response),
            read.expect.as_ref(),
            "case={} step={} raft read mismatch",
            case.name,
            read.name
        );
    }
}

fn execute_steps(
    engine: &TemporalEngine,
    shard_id: u64,
    steps: &[StorageMigrationStep],
    case_name: &str,
) {
    for step in steps {
        let response = engine.execute_durable(ExecuteRequest {
            shard_id,
            command: step.command.clone(),
        });
        assert!(
            response.status.ok,
            "case={} step={} failed status={:?}",
            case_name, step.name, response.status
        );
        if let Some(expected) = &step.expect {
            assert_eq!(
                &response.response, expected,
                "case={} step={} response mismatch",
                case_name, step.name
            );
        }
    }
}

fn verify_redis_admin_replay(engine: &TemporalEngine, case: &StorageMigrationCase) {
    assert!(
        !engine.slot_storage_summaries(case.shard_id).is_empty(),
        "case={} admin slot summaries should be populated",
        case.name
    );
    assert_clean_recovery(engine, case.shard_id, &case.name);

    for step in case.operations.iter().filter(|step| step.storage_mutation) {
        match &step.command {
            Command::StringSet { key, value } => {
                let response = redis(
                    engine,
                    case.shard_id,
                    vec!["GET".as_bytes(), key.as_bytes()],
                );
                assert_eq!(
                    response,
                    RespValue::Bulk(Some(value.clone())),
                    "case={} step={} Redis GET mismatch",
                    case.name,
                    step.name
                );
            }
            Command::HashMultiSet { key, entries } => {
                for (field, value) in entries {
                    let response = redis(
                        engine,
                        case.shard_id,
                        vec!["HGET".as_bytes(), key.as_bytes(), field.as_bytes()],
                    );
                    assert_eq!(
                        response,
                        RespValue::Bulk(Some(value.clone())),
                        "case={} step={} Redis HGET mismatch",
                        case.name,
                        step.name
                    );
                }
            }
            Command::SetAdd { key, member } => {
                let response = redis(
                    engine,
                    case.shard_id,
                    vec!["SISMEMBER".as_bytes(), key.as_bytes(), member.as_slice()],
                );
                assert_eq!(
                    response,
                    RespValue::Integer(1),
                    "case={} step={} Redis SISMEMBER mismatch",
                    case.name,
                    step.name
                );
            }
            _ => {}
        }
    }
}

fn redis(engine: &TemporalEngine, shard_id: u64, args: Vec<&[u8]>) -> RespValue {
    execute_redis_command(
        args.into_iter().map(|arg| arg.to_vec()).collect(),
        shard_id,
        |command| {
            let response = engine.execute_durable(ExecuteRequest { shard_id, command });
            if response.status.ok {
                Ok(response.response)
            } else {
                Err(response.status.message)
            }
        },
    )
}

fn assert_clean_recovery(engine: &TemporalEngine, shard_id: u64, case_name: &str) {
    let recovery = engine.storage_recovery_report(shard_id);
    assert!(
        recovery.all_live_pages_readable,
        "case={} live pages should be readable: {:?}",
        case_name, recovery.unreadable_page_refs
    );
    assert!(
        recovery.segment_integrity.integrity_ok,
        "case={} segment integrity failed: {:?}",
        case_name, recovery.segment_integrity
    );
    assert_eq!(recovery.segment_integrity.stale_page_ref_count, 0);
    assert_eq!(recovery.segment_integrity.corrupt_page_segment_count, 0);
    assert_eq!(recovery.segment_integrity.unreadable_page_ref_count, 0);
    assert_eq!(recovery.segment_integrity.owner_mismatch_page_ref_count, 0);
    assert_eq!(recovery.segment_integrity.missing_owner_page_ref_count, 0);
    assert_eq!(
        recovery
            .feature_page_layout
            .missing_indexed_timestamps
            .len(),
        0
    );
    assert_eq!(
        recovery.feature_page_layout.orphan_packed_timestamps.len(),
        0
    );
    assert_eq!(
        recovery
            .feature_page_layout
            .duplicate_packed_timestamps
            .len(),
        0
    );
}

fn new_engine(root: &Path, page_dir: &Path, index_dir: &Path, shard_id: u64) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(256, root.join("cache"), page_dir, index_dir);
    engine.load_shard(shard_id);
    engine
}
