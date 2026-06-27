use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use temporalstore_rust::{
    execute_redis_command, Command, CommandResponse, ExecuteRequest, RaftCluster, RaftConfig,
    RespValue, SharedStoreReplicator, SharedStoreStorageMode, SlotDumpFaultMatrixReport,
    SlotDumpFollowerReplayCursor, StorageLifecycleReport, StorageLifecycleRequest,
    StorageRecoveryReport, TemporalEngine,
};
use temporalstore_snapshot::FileObjectStore;

#[derive(Debug, Deserialize)]
struct StorageMigrationCorpus {
    schema_version: u32,
    name: String,
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

#[derive(Debug, Serialize)]
struct StorageProductionHarnessSummary {
    root: String,
    corpus_name: String,
    corpus_report: StorageCorpusEvidenceReport,
    cases: Vec<StorageProductionCaseSummary>,
}

#[derive(Debug, Serialize)]
struct StorageCorpusEvidenceReport {
    artifact_version: u32,
    converted_cases: usize,
    replay_paths: Vec<String>,
    logical_read_checks: usize,
    mismatches: Vec<String>,
    external_corpus_publication_ready: bool,
}

#[derive(Debug, Serialize)]
struct StorageProductionCaseSummary {
    case_name: String,
    shard_id: u64,
    mutation_count: usize,
    slot_dump_manifest_id: String,
    dumped_slot_count: usize,
    fault_matrix_ready: bool,
    fault_matrix_passed_count: usize,
    fault_matrix_scenario_count: usize,
    cache_warmup_page_refs: usize,
    cache_warmup_block_store_reads: usize,
    cache_warmup_failed_page_refs: usize,
    cache_memory_hits: u64,
    cache_disk_hits: u64,
    cache_puts: u64,
    cache_invalidations: u64,
    cache_memory_evictions: u64,
    cache_refill_failures: u64,
    recovery_ok_before_restart: bool,
    recovery_ok_after_restart: bool,
    recovery_error_summary_before_restart: StorageRecoveryErrorSummary,
    recovery_error_summary_after_restart: StorageRecoveryErrorSummary,
    shared_store_sync: SharedStoreReplayEvidence,
    shared_store_async: SharedStoreReplayEvidence,
    raft_leader_after_transfer: u64,
    redis_admin_replay_ok: bool,
}

#[derive(Debug, Serialize)]
struct StorageRecoveryErrorSummary {
    orphan_page_segment_count: usize,
    stale_page_ref_count: usize,
    corrupt_page_segment_count: usize,
    unreadable_page_ref_count: usize,
    unreadable_page_bytes: u64,
    owner_mismatch_page_ref_count: usize,
    missing_owner_page_ref_count: usize,
    corrupt_packed_feature_pages: usize,
    missing_indexed_timestamps: usize,
    orphan_packed_timestamps: usize,
    duplicate_packed_timestamps: usize,
    replay_boundary_oplog_sequence: u64,
    replay_boundary_index_log_sequence: u64,
}

#[derive(Debug, Serialize)]
struct SharedStoreReplayEvidence {
    mode: String,
    applied: usize,
    expected_reads_ok: bool,
    recovery_ok: bool,
    recovery_error_summary: StorageRecoveryErrorSummary,
}

#[tokio::main]
async fn main() {
    let root = parse_root();
    fs::create_dir_all(&root).expect("failed to create storage production harness root");
    let corpus = load_corpus();
    let mut cases = Vec::new();

    for case in &corpus.cases {
        cases.push(run_case(&root, case).await);
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&StorageProductionHarnessSummary {
            root: root.display().to_string(),
            corpus_name: corpus.name.clone(),
            corpus_report: StorageCorpusEvidenceReport {
                artifact_version: corpus.schema_version,
                converted_cases: corpus.cases.len(),
                replay_paths: vec![
                    "engine_restart".to_string(),
                    "redis_admin".to_string(),
                    "shared_store_sync".to_string(),
                    "shared_store_async".to_string(),
                    "cache_warmup".to_string(),
                    "raft_read".to_string(),
                ],
                logical_read_checks: corpus
                    .cases
                    .iter()
                    .map(|case| case.expected_reads.len())
                    .sum(),
                mismatches: Vec::new(),
                external_corpus_publication_ready: true,
            },
            cases,
        })
        .expect("storage production summary should serialize")
    );
}

async fn run_case(root: &Path, case: &StorageMigrationCase) -> StorageProductionCaseSummary {
    let case_root = root.join(&case.name);
    let page_dir = case_root.join("pages");
    let index_dir = case_root.join("indexes");
    let mut engine = new_engine(&case_root, &page_dir, &index_dir, case.shard_id);
    execute_steps(&engine, case.shard_id, &case.operations, &case.name);
    let dirty_slots = engine
        .slot_storage_summaries(case.shard_id)
        .into_iter()
        .filter(|summary| summary.dirty_generation > 0)
        .map(|summary| summary.routing_slot)
        .collect::<Vec<_>>();
    assert!(!dirty_slots.is_empty());
    let installable_manifest = engine
        .create_slot_dump_manifest(case.shard_id, Vec::new())
        .expect("storage production explicit dump manifest should be created");
    assert!(!installable_manifest.index_bytes.is_empty());

    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: case.shard_id,
        selected_dump_slots: dirty_slots,
        max_dump_slots_per_round: 64,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: true,
        prune_slot_dump_manifests: true,
        roll_forward_slot_dump_installs: true,
        follower_replay_cursors: vec![SlotDumpFollowerReplayCursor {
            follower_id: "storage-production-lagging-follower".to_string(),
            shard_id: case.shard_id,
            oplog_sequence: 0,
            index_log_sequence: 0,
        }],
        page_gc_shared_store_cursors: Vec::new(),
        page_gc_raft_snapshot_refs: Vec::new(),
        page_gc_checkpoint_floor_segment_id: None,
        page_gc_raft_install_floor_segment_id: None,
        page_gc_delayed_destroy_grace_ms: 0,
        invalidate_cache: true,
        warm_cache: true,
    });
    assert_lifecycle_ok(&lifecycle, &case.name);
    let recovery_before_restart = engine.storage_recovery_report(case.shard_id);
    assert_recovery_ok(&recovery_before_restart, &case.name);

    drop(engine);
    engine = new_engine(&case_root, &page_dir, &index_dir, case.shard_id);
    engine
        .install_slot_dump_manifest(&installable_manifest)
        .expect("storage production manifest install after restart should succeed");
    let recovery_after_restart = engine.storage_recovery_report(case.shard_id);
    assert_recovery_ok(&recovery_after_restart, &case.name);
    execute_steps(&engine, case.shard_id, &case.expected_reads, &case.name);

    let shared_store_root = case_root.join("shared-store");
    let sync_applied =
        run_shared_store_mode(&shared_store_root, case, SharedStoreStorageMode::Sync).await;
    let async_applied =
        run_shared_store_mode(&shared_store_root, case, SharedStoreStorageMode::Async).await;
    let raft_leader_after_transfer = run_raft(case);
    let redis_admin_replay_ok = validate_redis_admin_replay(&engine, case);
    let cache_stats = engine.storage_cache_inspection_report(case.shard_id).stats;
    let fault_matrix = engine.slot_dump_fault_matrix_report(case.shard_id);
    assert_fault_matrix_ok(&fault_matrix, &case.name);
    let manifest = lifecycle
        .dump_manifest
        .expect("storage production harness should create a dump manifest");

    StorageProductionCaseSummary {
        case_name: case.name.clone(),
        shard_id: case.shard_id,
        mutation_count: mutation_count(case),
        slot_dump_manifest_id: manifest.manifest_id,
        dumped_slot_count: manifest.slot_ids.len(),
        fault_matrix_ready: fault_matrix.production_ready_slice,
        fault_matrix_passed_count: fault_matrix.passed_count,
        fault_matrix_scenario_count: fault_matrix.scenario_count,
        cache_warmup_page_refs: lifecycle.cache_warmup.considered_page_refs,
        cache_warmup_block_store_reads: lifecycle.cache_warmup.block_store_reads,
        cache_warmup_failed_page_refs: lifecycle.cache_warmup.failed_page_refs,
        cache_memory_hits: cache_stats.memory_hits,
        cache_disk_hits: cache_stats.disk_hits,
        cache_puts: cache_stats.puts,
        cache_invalidations: cache_stats.invalidations,
        cache_memory_evictions: cache_stats.memory_evictions,
        cache_refill_failures: cache_stats.refill_failures,
        recovery_ok_before_restart: recovery_ok(&recovery_before_restart),
        recovery_ok_after_restart: recovery_ok(&recovery_after_restart),
        recovery_error_summary_before_restart: recovery_error_summary(&recovery_before_restart),
        recovery_error_summary_after_restart: recovery_error_summary(&recovery_after_restart),
        shared_store_sync: sync_applied,
        shared_store_async: async_applied,
        raft_leader_after_transfer,
        redis_admin_replay_ok,
    }
}

async fn run_shared_store_mode(
    shared_store_root: &Path,
    case: &StorageMigrationCase,
    mode: SharedStoreStorageMode,
) -> SharedStoreReplayEvidence {
    let store = Arc::new(FileObjectStore::new(
        shared_store_root.join(format!("{mode:?}")),
    ));
    let replicator = SharedStoreReplicator::new("storage-production-harness", store);
    let writer = replicator.storage_writer(mode, 1);
    for step in case.operations.iter().filter(|step| step.storage_mutation) {
        writer
            .write(case.shard_id, step.command.clone())
            .await
            .expect("storage production shared-store write should succeed");
    }
    if mode == SharedStoreStorageMode::Async {
        while writer.queued_len() > 0 {
            writer
                .flush_pending(1)
                .await
                .expect("storage production async shared-store flush should succeed");
        }
    }

    let follower_root = shared_store_root.join(format!("follower-{mode:?}"));
    let follower = new_engine(
        &follower_root,
        &follower_root.join("pages"),
        &follower_root.join("indexes"),
        case.shard_id,
    );
    let replay = replicator
        .replay_oplog_strict(case.shard_id, 0, &follower)
        .await
        .expect("storage production shared-store replay should succeed");
    assert_eq!(replay.applied, mutation_count(case));
    execute_steps(&follower, case.shard_id, &case.expected_reads, &case.name);
    let recovery = follower.storage_recovery_report(case.shard_id);
    assert_recovery_ok(&recovery, &case.name);
    SharedStoreReplayEvidence {
        mode: format!("{mode:?}"),
        applied: replay.applied,
        expected_reads_ok: true,
        recovery_ok: recovery_ok(&recovery),
        recovery_error_summary: recovery_error_summary(&recovery),
    }
}

fn run_raft(case: &StorageMigrationCase) -> u64 {
    let cluster =
        RaftCluster::new_single_shard_with_config(case.shard_id, [1, 2, 3], RaftConfig::default())
            .expect("storage production raft cluster should start");
    for step in case.operations.iter().filter(|step| step.storage_mutation) {
        cluster
            .propose(step.command.clone())
            .expect("storage production raft write should commit");
    }
    cluster
        .transfer_leader(2)
        .expect("storage production raft transfer should work");
    for read in &case.expected_reads {
        let response = cluster
            .read_from_replica(2, read.command.clone())
            .expect("storage production raft read should work");
        assert_eq!(Some(&response), read.expect.as_ref());
    }
    cluster.status().leader_id
}

fn assert_lifecycle_ok(report: &StorageLifecycleReport, case_name: &str) {
    let Some(manifest) = report.dump_manifest.as_ref() else {
        panic!("case={case_name} did not create slot dump manifest");
    };
    assert!(!manifest.checksum.is_empty());
    assert!(!manifest.slot_ids.is_empty());
    assert!(report.cache_warmup.considered_page_refs > 0);
    assert_eq!(report.cache_warmup.failed_page_refs, 0);
    assert!(
        report.cache_warmup.block_store_reads > 0
            || report.cache_warmup.already_cached_page_refs > 0,
        "case={case_name} did not prove cache warmup via block-store read or existing cache refs"
    );
    assert!(
        report.manifest_prune_plan.follower_blocks.is_empty()
            || report
                .manifest_prune_plan
                .reasons
                .iter()
                .any(|reason| reason.contains("follower")),
        "case={case_name} follower retention blocks must be explained"
    );
}

fn assert_fault_matrix_ok(report: &SlotDumpFaultMatrixReport, case_name: &str) {
    assert!(
        report.production_ready_slice,
        "case={case_name} slot dump fault matrix is not production ready: {:?}",
        report.failed_scenarios
    );
    assert!(report.scenario_count > 0);
    assert_eq!(report.passed_count, report.scenario_count);
    assert!(report.failed_scenarios.is_empty());
}

fn assert_recovery_ok(report: &StorageRecoveryReport, case_name: &str) {
    assert!(
        recovery_ok(report),
        "case={case_name} storage recovery failed: {:?}",
        report.segment_integrity
    );
    assert!(report.segment_integrity.orphan_page_segment_count <= 1);
    assert_eq!(
        report.feature_page_layout.duplicate_packed_timestamps.len(),
        0
    );
}

fn recovery_ok(report: &StorageRecoveryReport) -> bool {
    report.all_live_pages_readable
        && report.segment_integrity.integrity_ok
        && report.segment_integrity.stale_page_ref_count == 0
        && report.segment_integrity.corrupt_page_segment_count == 0
        && report.segment_integrity.unreadable_page_ref_count == 0
        && report.segment_integrity.owner_mismatch_page_ref_count == 0
        && report.segment_integrity.missing_owner_page_ref_count == 0
        && report
            .feature_page_layout
            .corrupt_packed_feature_pages
            .is_empty()
        && report
            .feature_page_layout
            .missing_indexed_timestamps
            .is_empty()
        && report
            .feature_page_layout
            .orphan_packed_timestamps
            .is_empty()
        && report
            .feature_page_layout
            .duplicate_packed_timestamps
            .is_empty()
}

fn recovery_error_summary(report: &StorageRecoveryReport) -> StorageRecoveryErrorSummary {
    StorageRecoveryErrorSummary {
        orphan_page_segment_count: report.segment_integrity.orphan_page_segment_count,
        stale_page_ref_count: report.segment_integrity.stale_page_ref_count,
        corrupt_page_segment_count: report.segment_integrity.corrupt_page_segment_count,
        unreadable_page_ref_count: report.segment_integrity.unreadable_page_ref_count,
        unreadable_page_bytes: report.segment_integrity.unreadable_page_bytes,
        owner_mismatch_page_ref_count: report.segment_integrity.owner_mismatch_page_ref_count,
        missing_owner_page_ref_count: report.segment_integrity.missing_owner_page_ref_count,
        corrupt_packed_feature_pages: report
            .feature_page_layout
            .corrupt_packed_feature_pages
            .len(),
        missing_indexed_timestamps: report.feature_page_layout.missing_indexed_timestamps.len(),
        orphan_packed_timestamps: report.feature_page_layout.orphan_packed_timestamps.len(),
        duplicate_packed_timestamps: report.feature_page_layout.duplicate_packed_timestamps.len(),
        replay_boundary_oplog_sequence: report.boundary.selected_replay_oplog_sequence,
        replay_boundary_index_log_sequence: report.boundary.selected_replay_index_log_sequence,
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

fn validate_redis_admin_replay(engine: &TemporalEngine, case: &StorageMigrationCase) -> bool {
    if engine.slot_storage_summaries(case.shard_id).is_empty() {
        return false;
    }
    if !recovery_ok(&engine.storage_recovery_report(case.shard_id)) {
        return false;
    }
    for step in case.operations.iter().filter(|step| step.storage_mutation) {
        match &step.command {
            Command::StringSet { key, value } => {
                if redis(
                    engine,
                    case.shard_id,
                    vec!["GET".as_bytes(), key.as_bytes()],
                ) != RespValue::Bulk(Some(value.clone()))
                {
                    return false;
                }
            }
            Command::HashMultiSet { key, entries } => {
                for (field, value) in entries {
                    if redis(
                        engine,
                        case.shard_id,
                        vec!["HGET".as_bytes(), key.as_bytes(), field.as_bytes()],
                    ) != RespValue::Bulk(Some(value.clone()))
                    {
                        return false;
                    }
                }
            }
            Command::SetAdd { key, member } => {
                if redis(
                    engine,
                    case.shard_id,
                    vec!["SISMEMBER".as_bytes(), key.as_bytes(), member.as_slice()],
                ) != RespValue::Integer(1)
                {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
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

fn new_engine(root: &Path, page_dir: &Path, index_dir: &Path, shard_id: u64) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(256, root.join("cache"), page_dir, index_dir);
    engine.load_shard(shard_id);
    engine
}

fn mutation_count(case: &StorageMigrationCase) -> usize {
    case.operations
        .iter()
        .filter(|step| step.storage_mutation)
        .count()
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
    corpus
}

fn parse_root() -> PathBuf {
    let mut root =
        std::env::temp_dir().join(format!("temporalstore-storage-production-{}", now_ms()));
    let mut args = std::env::args().skip(1);
    while let Some(key) = args.next() {
        let Some(value) = args.next() else {
            usage_and_exit();
        };
        match key.as_str() {
            "--root" => root = PathBuf::from(value),
            _ => usage_and_exit(),
        }
    }
    root
}

fn usage_and_exit() -> ! {
    eprintln!("usage: storage_production_harness [--root <path>]");
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
