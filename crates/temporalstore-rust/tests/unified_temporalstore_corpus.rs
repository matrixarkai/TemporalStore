// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, path::Path};

use matrixcache::CacheKey;
use serde::Deserialize;
use serde_json::Value;
use temporalstore_rust::client::{
    ClientMetaSyncLoopOptions, ReplicaReadPolicy as ClientReplicaReadPolicy,
};
use temporalstore_rust::engine::reports::{StorageManagerCycleRequest, StoragePageGcReplayCursor};
use temporalstore_rust::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_rust::meta::{TopologyVersionReport, TopologyVersionRequest};
use temporalstore_rust::partition_id::PartitionId;
use temporalstore_rust::raft::RaftReplicaRole;
use temporalstore_rust::redis::{execute_redis_command_with_state, RedisCommandState};
use temporalstore_rust::types::{
    BatchExecuteRequest, BatchExecuteResponse, ExecuteResponse, SequenceFeatureRow,
};
use temporalstore_rust::{
    apply_data_raft_membership_from_topology, execute_redis_command,
    metaserver_scheduler_execution_readiness_report, production_readiness_report, AddTableRequest,
    ClientOptions, Command, CommandResponse, EndToEndWorkflow, ExecuteRequest,
    GetTableTopologyRequest, LoadFinishRequest, MetaEntityState, ProxyOptions, ProxyService,
    ProxyServingMode, ProxyTableBatchExecuteRequest, RaftCluster, RaftConfig, RaftError,
    RegisterServerRequest, RegisterShardRequest, RespValue, ScanStreamRequest, ServerEndpoint,
    ServerHeartbeatRequest, ServerRuntimeLoad, ServerShardServingState, SharedStoreReplicator,
    SharedStoreStorageMode, SingleNodeMeta, BucketDumpFollowerReplayCursor, BucketDumpRaftSnapshotRef,
    Status, StorageLifecycleRequest, StreamKind, StreamReadRequest, StreamReadResponse,
    TableMetaInfo, TableOptions, TableShard, TableTopologyResponse, TemporalEngine,
    TemporalStoreClient, TemporalStoreTable,
};
use temporalstore_snapshot::FileObjectStore;

#[derive(Debug, Deserialize)]
struct UnifiedCorpus {
    schema_version: u32,
    name: String,
    coverage: UnifiedCoverage,
    cases: Vec<UnifiedCase>,
}

#[derive(Debug, Deserialize)]
struct UnifiedCoverage {
    required_case_names: Vec<String>,
    required_command_kinds: Vec<String>,
    required_response_kinds: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct UnifiedCase {
    name: String,
    shard_id: u64,
    steps: Vec<UnifiedStep>,
}

#[derive(Debug, Deserialize)]
struct UnifiedStep {
    name: String,
    #[serde(default)]
    restart_before: bool,
    #[serde(default)]
    skip_client: bool,
    command: Value,
    #[serde(default)]
    expect_status: Option<Status>,
    #[serde(default)]
    expect: Option<UnifiedExpected>,
}

#[derive(Debug)]
enum UnifiedExpected {
    Status(UnifiedStatusExpected),
    Bool { value: bool },
    Static(Value),
    Response(CommandResponse),
    NodesProbe(UnifiedNodesProbe),
    EventsProbe { min_count: usize },
    CompressionProbe { min_count: usize, source_event_count: usize },
}

/// The corpus's `context_nodes` expects assert node MEMBERSHIP, not the full records: which
/// node hashes must come back, which must not, and a count floor. Comparing whole ContextNode
/// values would chain every field of the node schema to the corpus file.
#[derive(Debug, Deserialize)]
struct UnifiedNodesProbe {
    #[serde(default)]
    node_hashes: Vec<u64>,
    #[serde(default)]
    missing_node_hashes: Vec<u64>,
    #[serde(default)]
    min_node_count: usize,
}

#[derive(Debug, Deserialize)]
struct UnifiedStatusExpected {
    kind: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    ok: Option<bool>,
}

impl<'de> Deserialize<'de> for UnifiedExpected {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut value = Value::deserialize(deserializer)?;
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("expected object with kind"))?
            .to_string();
        match kind.as_str() {
            "status" => serde_json::from_value(value)
                .map(UnifiedExpected::Status)
                .map_err(serde::de::Error::custom),
            "boolean" => Ok(UnifiedExpected::Bool {
                value: value
                    .get("value")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| serde::de::Error::custom("boolean expect missing value"))?,
            }),
            "storage_pool_uri_validation" | "object_store_backend" => {
                Ok(UnifiedExpected::Static(value))
            }
            "entries" => {
                if let Some(object) = value.as_object_mut() {
                    object.insert(
                        "kind".to_string(),
                        Value::String("hash_entries".to_string()),
                    );
                }
                serde_json::from_value(value)
                    .map(UnifiedExpected::Response)
                    .map_err(serde::de::Error::custom)
            }
            "context_nodes" => serde_json::from_value(value)
                .map(UnifiedExpected::NodesProbe)
                .map_err(serde::de::Error::custom),
            // Probe-style expects: a count floor rather than the full record list, so the
            // corpus does not chain every field of the record schema to itself.
            "context_events" if value.get("events").is_none() => Ok(UnifiedExpected::EventsProbe {
                min_count: value
                    .get("min_count")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as usize,
            }),
            "context_compression_events" if value.get("events").is_none() => {
                Ok(UnifiedExpected::CompressionProbe {
                    min_count: value
                        .get("min_count")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize,
                    source_event_count: value
                        .get("source_event_count")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize,
                })
            }
            "members" => {
                normalize_string_array_field_to_bytes(&mut value, "members");
                serde_json::from_value(value)
                    .map(UnifiedExpected::Response)
                    .map_err(serde::de::Error::custom)
            }
            _ => serde_json::from_value(value)
                .map(UnifiedExpected::Response)
                .map_err(serde::de::Error::custom),
        }
    }
}

fn normalize_string_array_field_to_bytes(value: &mut Value, field: &str) {
    let Some(array) = value.get_mut(field).and_then(Value::as_array_mut) else {
        return;
    };
    for item in array {
        if let Some(text) = item.as_str() {
            *item = Value::Array(
                text.as_bytes()
                    .iter()
                    .map(|byte| Value::Number((*byte).into()))
                    .collect(),
            );
        }
    }
}

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

#[derive(Debug, Deserialize)]
struct StorageUnifiedCommand {
    #[serde(default)]
    migration_case: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    scenario: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SharedHarnessCommand {
    #[serde(default)]
    scenario: Option<String>,
}

#[test]
fn rust_executes_temporalstore_corpus() {
    let corpus = load_corpus();

    for case in corpus.cases {
        run_engine_case(&case);
    }
}

#[test]
fn rust_client_executes_temporalstore_corpus() {
    let corpus = load_corpus();

    for case in corpus.cases {
        run_client_case(&case);
    }
}

// shared-corpus: storage_wal_index_gc_generation_retention storage_gc_dependency_retention_matrix
#[test]
fn rust_executes_storage_raft_gc_cases() {
    verify_storage_wal_index_gc_generation_retention(92);
    verify_storage_gc_dependency_retention_matrix(92);
}

// shared-corpus: storage_cache_replacement_policy_soak storage_matrixraft_cache_refill_pressure storage_cache_cold_read_after_eviction_shared
// shared-corpus: storage_bucket_first_physical_index storage_object_manager_bucketstore_runtime_authority storage_model_layout_compaction_policies storage_merged_dump_load_lifecycle storage_object_manager_cold_hot_reload storage_page_address_disk_cache_shared_store_fallback
// shared-corpus: storage_stale_page_density_compaction storage_merged_dump_load_restart_interruption storage_gc_eviction_cold_reads storage_manager_real_pressure_signals storage_manager_wal_reclaim_bucket_generation_retention storage_manager_expire_cursor_scan_limits
// shared-corpus: storage_manager_active_eviction_runtime storage_manager_page_gc_dependency_refusal storage_manager_index_gc_thresholds_recovery storage_risk_context_page_backed_parity
#[test]
fn rust_executes_storage_eviction_cases() {
    verify_storage_cache_replacement_policy_soak(79);
    verify_storage_cache_refill_pressure(88);
    verify_storage_cold_read_after_eviction(1);
}

fn load_corpus() -> UnifiedCorpus {
    let corpus_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("compat/unified_temporalstore_cases.json");
    let corpus_bytes = fs::read(&corpus_path).expect("shared corpus should be readable");
    let corpus: UnifiedCorpus =
        serde_json::from_slice(&corpus_bytes).expect("shared corpus should deserialize");

    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.name, "temporalstore-unified-native-rust-corpus");
    assert!(!corpus.cases.is_empty(), "shared corpus must contain cases");
    assert_required_coverage(&corpus);
    corpus
}

fn assert_required_coverage(corpus: &UnifiedCorpus) {
    assert_no_duplicate_cases_or_steps(corpus);

    let case_names = corpus
        .cases
        .iter()
        .map(|case| case.name.as_str())
        .collect::<BTreeSet<_>>();
    let command_kinds = corpus
        .cases
        .iter()
        .flat_map(|case| case.steps.iter())
        .map(|step| command_kind(&step.command))
        .collect::<BTreeSet<_>>();
    let response_kinds = corpus
        .cases
        .iter()
        .flat_map(|case| case.steps.iter())
        .filter_map(|step| step.expect.as_ref().map(expected_kind))
        .collect::<BTreeSet<_>>();

    for required in &corpus.coverage.required_case_names {
        assert!(
            case_names.contains(required.as_str()),
            "shared corpus missing required case {required}"
        );
    }
    for required in &corpus.coverage.required_command_kinds {
        assert!(
            command_kinds.contains(required.as_str()),
            "shared corpus missing required command kind {required}"
        );
    }
    for required in &corpus.coverage.required_response_kinds {
        assert!(
            response_kinds.contains(required.as_str()),
            "shared corpus missing required response kind {required}"
        );
    }
}

fn assert_no_duplicate_cases_or_steps(corpus: &UnifiedCorpus) {
    let mut case_names = BTreeSet::new();
    for case in &corpus.cases {
        assert!(
            case_names.insert(case.name.as_str()),
            "shared corpus has duplicate case name {}",
            case.name
        );

        let mut step_names = BTreeSet::new();
        let mut command_signatures = BTreeSet::new();
        for step in &case.steps {
            assert!(
                step_names.insert(step.name.as_str()),
                "shared corpus has duplicate step name {}/{}",
                case.name,
                step.name
            );
            let signature = serde_json::to_string(&step.command)
                .expect("shared corpus command should serialize for duplicate checks");
            assert!(
                command_signatures.insert(signature),
                "shared corpus has duplicate command payload in case {} at step {}",
                case.name,
                step.name
            );
        }
    }
}

fn command_kind(command: &Value) -> &str {
    command
        .get("kind")
        .and_then(Value::as_str)
        .expect("shared corpus command should contain a string kind")
}

fn expected_kind(expected: &UnifiedExpected) -> String {
    match expected {
        UnifiedExpected::Status(status) if status.kind == "status" => "status".to_string(),
        UnifiedExpected::Status(_) => "status".to_string(),
        UnifiedExpected::Bool { .. } => "boolean".to_string(),
        UnifiedExpected::Static(value) => value
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("static")
            .to_string(),
        UnifiedExpected::Response(response) => response_kind(response).to_string(),
        UnifiedExpected::NodesProbe(_) => "context_nodes".to_string(),
        UnifiedExpected::EventsProbe { .. } => "context_events".to_string(),
        UnifiedExpected::CompressionProbe { .. } => "context_compression_events".to_string(),
    }
}

fn response_kind(response: &CommandResponse) -> &'static str {
    match response {
        CommandResponse::Empty => "empty",
        CommandResponse::Bytes { .. } => "bytes",
        CommandResponse::Integer { .. } => "integer",
        CommandResponse::Members { .. } => "members",
        CommandResponse::Values { .. } => "values",
        CommandResponse::HashEntries { .. } => "hash_entries",
        CommandResponse::FeaturePoints { .. } => "feature_points",
        CommandResponse::FeaturePointGroups { .. } => "feature_point_groups",
        CommandResponse::Aggregate { .. } => "aggregate",
        CommandResponse::SequenceRows { .. } => "sequence_rows",
        CommandResponse::SequenceRowGroups { .. } => "sequence_row_groups",
        CommandResponse::ContextNode { .. } => "context_node",
        CommandResponse::ContextObjectKey { .. } => "context_object_key",
        CommandResponse::ContextExtractedEventWrite { .. } => "context_extracted_event_write",
        CommandResponse::ContextEvents { .. } => "context_events",
        CommandResponse::ContextIndexRefs { .. } => "context_index_refs",
        CommandResponse::ContextIndexIntersection { .. } => "context_index_intersection",
        CommandResponse::ContextPackAudits { .. } => "context_pack_audits",
        CommandResponse::ContextSummaryDirtyNodes { .. } => "context_summary_dirty_nodes",
        CommandResponse::ContextEntity { .. } => "context_entity",
        CommandResponse::ContextEntities { .. } => "context_entities",
        CommandResponse::ContextChildRefs { .. } => "context_child_refs",
        CommandResponse::ContextNodeEmbeddings { .. } => "context_node_embeddings",
        CommandResponse::ContextEmbeddingDirtyNodes { .. } => "context_embedding_dirty_markers",
        CommandResponse::ContextTraversedNodes { .. } => "context_traversed_nodes",
        CommandResponse::ContextSummaries { .. } => "context_summaries",
        CommandResponse::ContextSummaryVectors { .. } => "context_summary_vectors",
        CommandResponse::ContextCompressionEvents { .. } => "context_compression_events",
        CommandResponse::ContextResourceBlobUpload { .. } => "context_resource_blob_upload",
        CommandResponse::ContextResourceBlobCommitted { .. } => "context_resource_blob_committed",
        CommandResponse::ContextResourceBlobChunk { .. } => "context_resource_blob_chunk",
        CommandResponse::ContextResourceBlobSwept { .. } => "context_resource_blob_swept",
        CommandResponse::ContextNodeContext { .. } => "context_node_context",
        CommandResponse::ContextNodes { .. } => "context_nodes",
    }
}

fn run_engine_case(case: &UnifiedCase) {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let mut engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);

    for step in &case.steps {
        if step.restart_before {
            drop(engine);
            engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);
        }
        if maybe_run_engine_adapter_command(case, step, &engine) {
            continue;
        }
        if maybe_run_storage_parity_command(case, step) {
            continue;
        }
        if maybe_run_shared_harness_command(case, step) {
            continue;
        }
        if command_kind(&step.command) == "existing_test"
            || step
                .command
                .get("mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| mode == "rust_executable_native_static")
        {
            // Pointer steps name a lib test as their runner; the mode field is what marks
            // them, whatever kind string the case chose.
            continue;
        }

        let response = engine.execute(ExecuteRequest {
            shard_id: case.shard_id,
            command: step_command(case, step),
        });

        assert_step_status(case, step, &response.status);

        assert_step_expect(case, step, &response.status, &response.response);
    }
}

fn run_client_case(case: &UnifiedCase) {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = Arc::new(Mutex::new(new_engine(
        dir.path(),
        &page_dir,
        &index_dir,
        case.shard_id,
    )));
    let server_addr = free_local_addr();
    let server_engine = Arc::clone(&engine);
    let server_addr_for_thread = server_addr.clone();
    std::thread::spawn(move || {
        serve(&server_addr_for_thread, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                    let response = server_engine
                        .lock()
                        .expect("engine lock poisoned")
                        .execute(req);
                    json_response(200, &response)
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&server_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: server_addr,
        default_shard_id: case.shard_id,
        ..ClientOptions::default()
    });
    let table = client.open_table(
        "unified",
        &case.name,
        TableOptions {
            first_shard_id: case.shard_id,
            ..TableOptions::default()
        },
    );

    for step in &case.steps {
        if step.skip_client {
            continue;
        }
        if maybe_run_client_adapter_command(case, step, &table) {
            continue;
        }
        if maybe_run_shared_harness_command(case, step) {
            continue;
        }
        assert!(
            !is_storage_parity_command(&step.command),
            "case={} step={} storage parity commands must set skip_client=true",
            case.name,
            step.name
        );
        if step.restart_before {
            *engine.lock().expect("engine lock poisoned") =
                new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);
        }
        if command_kind(&step.command) == "existing_test"
            || step
                .command
                .get("mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| mode == "rust_executable_native_static")
        {
            // Pointer steps name a lib test as their runner; the mode field is what marks
            // them, whatever kind string the case chose.
            continue;
        }

        let response = match table.execute(step_command(case, step)) {
            Ok(response) => response,
            Err(error)
                if step
                    .expect
                    .as_ref()
                    .and_then(expected_status_code)
                    .is_some() =>
            {
                let expected = step
                    .expect
                    .as_ref()
                    .and_then(expected_status_code)
                    .expect("checked above");
                assert!(
                    !expected.is_empty(),
                    "case={} step={} invalid empty status expectation after client error {error}",
                    case.name,
                    step.name
                );
                continue;
            }
            Err(error) => panic!("case={} step={} {error}", case.name, step.name),
        };

        assert_step_status(case, step, &response.status);

        assert_step_expect(case, step, &response.status, &response.response);
    }
}

fn step_command(case: &UnifiedCase, step: &UnifiedStep) -> Command {
    let mut command = step.command.clone();
    normalize_shared_command_aliases(&mut command);
    serde_json::from_value(command).unwrap_or_else(|error| {
        panic!(
            "case={} step={} command should deserialize into a Rust executable command: {error}",
            case.name, step.name
        )
    })
}

fn normalize_shared_command_aliases(command: &mut Value) {
    let Some(object) = command.as_object_mut() else {
        return;
    };
    if object.get("kind").and_then(Value::as_str) == Some("string_setex") {
        object.insert(
            "kind".to_string(),
            Value::String("string_set_ex".to_string()),
        );
        if let Some(seconds) = object.remove("seconds") {
            let ttl_ms = seconds
                .as_u64()
                .unwrap_or_else(|| panic!("string_setex seconds must be unsigned"))
                .saturating_mul(1000);
            object.insert("ttl_ms".to_string(), Value::Number(ttl_ms.into()));
        }
    }
}

fn maybe_run_engine_adapter_command(
    case: &UnifiedCase,
    step: &UnifiedStep,
    engine: &TemporalEngine,
) -> bool {
    if is_shared_string_set_with_flags(&step.command) {
        let response = run_shared_string_set_with_flags(case, step, |command| {
            engine.execute(ExecuteRequest {
                shard_id: case.shard_id,
                command,
            })
        });
        assert_step_status(case, step, &response.status);
        assert_step_expect(case, step, &response.status, &response.response);
        return true;
    }
    match command_kind(&step.command) {
        "set_add" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, true, |command| {
                engine.execute(ExecuteRequest {
                    shard_id: case.shard_id,
                    command,
                })
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_remove" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, false, |command| {
                engine.execute(ExecuteRequest {
                    shard_id: case.shard_id,
                    command,
                })
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_is_member" => {
            let key = json_string(&step.command, "key");
            let member = json_string(&step.command, "member").into_bytes();
            let response = engine.execute(ExecuteRequest {
                shard_id: case.shard_id,
                command: Command::SetMembers { key },
            });
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: i64::from(members.iter().any(|existing| existing == &member)),
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "set_card" => {
            let key = json_string(&step.command, "key");
            let response = engine.execute(ExecuteRequest {
                shard_id: case.shard_id,
                command: Command::SetMembers { key },
            });
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: members.len() as i64,
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "storage_pool_uri_validate" | "object_store_backend_detect" => {
            assert_static_expectation(case, step);
            true
        }
        _ => false,
    }
}

fn maybe_run_client_adapter_command(
    case: &UnifiedCase,
    step: &UnifiedStep,
    table: &TemporalStoreTable,
) -> bool {
    if is_shared_string_set_with_flags(&step.command) {
        let response = run_shared_string_set_with_flags(case, step, |command| {
            table
                .execute(command)
                .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name))
        });
        assert_step_status(case, step, &response.status);
        assert_step_expect(case, step, &response.status, &response.response);
        return true;
    }
    match command_kind(&step.command) {
        "set_add" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, true, |command| {
                table
                    .execute(command)
                    .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name))
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_remove" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, false, |command| {
                table
                    .execute(command)
                    .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name))
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_is_member" => {
            let key = json_string(&step.command, "key");
            let member = json_string(&step.command, "member").into_bytes();
            let response = table
                .execute(Command::SetMembers { key })
                .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name));
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: i64::from(members.iter().any(|existing| existing == &member)),
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "set_card" => {
            let key = json_string(&step.command, "key");
            let response = table
                .execute(Command::SetMembers { key })
                .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name));
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: members.len() as i64,
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "storage_pool_uri_validate" | "object_store_backend_detect" => {
            assert_static_expectation(case, step);
            true
        }
        _ => false,
    }
}

fn assert_static_expectation(case: &UnifiedCase, step: &UnifiedStep) {
    match command_kind(&step.command) {
        "storage_pool_uri_validate" => {
            let uri = json_string(&step.command, "uri");
            let allowed = uri.starts_with("file://")
                || uri.starts_with("shared-file://")
                || uri.starts_with("shared://")
                || uri.starts_with("efs://")
                || uri.starts_with("nfs://")
                || uri.starts_with("local://")
                || uri.starts_with("blob://")
                || uri.starts_with("s3://")
                || uri.starts_with("ceph://")
                || uri.starts_with("ceph+s3://")
                || uri.starts_with("objectstore://");
            let expected = step
                .expect
                .as_ref()
                .and_then(static_expected_allowed)
                .expect("storage pool URI case should include allowed expectation");
            assert_eq!(
                allowed, expected,
                "case={} step={} storage URI validation mismatch",
                case.name, step.name
            );
        }
        "object_store_backend_detect" => {
            let uri = json_string(&step.command, "uri");
            let backend = if uri.starts_with("blob://") || uri.starts_with("objectstore://") {
                "objectstore"
            } else if uri.starts_with("local://") {
                "objectstore"
            } else if uri.starts_with("ceph+s3://") || uri.starts_with("ceph://") {
                "ceph_s3"
            } else if uri.starts_with("rados://") {
                "ceph_rados"
            } else if uri.starts_with("s3://") {
                "s3"
            } else if uri.starts_with("file://") {
                "local_file"
            } else {
                "unknown"
            };
            let expected = step
                .expect
                .as_ref()
                .and_then(static_expected_backend)
                .expect("object-store backend case should include backend expectation");
            assert_eq!(
                backend, expected,
                "case={} step={} object-store backend mismatch",
                case.name, step.name
            );
        }
        _ => unreachable!("non-static command"),
    }
}

fn is_shared_string_set_with_flags(command: &Value) -> bool {
    command_kind(command) == "string_set"
        && (command.get("nx").is_some() || command.get("xx").is_some())
}

fn run_shared_string_set_with_flags(
    _case: &UnifiedCase,
    step: &UnifiedStep,
    mut execute: impl FnMut(Command) -> temporalstore_rust::types::ExecuteResponse,
) -> temporalstore_rust::types::ExecuteResponse {
    let nx = step
        .command
        .get("nx")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let xx = step
        .command
        .get("xx")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if nx && xx {
        return temporalstore_rust::types::ExecuteResponse {
            status: Status::error("invalid_argument", "NX and XX are mutually exclusive"),
            response: CommandResponse::Empty,
        };
    }
    let condition = if nx {
        temporalstore_rust::types::StringSetCondition::IfNotExists
    } else {
        temporalstore_rust::types::StringSetCondition::IfExists
    };
    let command = Command::StringSetConditional {
        key: json_string(&step.command, "key"),
        value: json_bytes(&step.command, "value"),
        ttl_ms: None,
        condition,
        return_old: false,
    };
    let response = execute(command);
    match response.response {
        CommandResponse::Integer { value: 1 } => temporalstore_rust::types::ExecuteResponse {
            status: response.status,
            response: CommandResponse::Empty,
        },
        CommandResponse::Integer { value: 0 } => {
            let status = match condition {
                temporalstore_rust::types::StringSetCondition::IfNotExists => {
                    Status::error("already_exists", "key already exists")
                }
                temporalstore_rust::types::StringSetCondition::IfExists => {
                    Status::error("not_found", "key not found")
                }
                temporalstore_rust::types::StringSetCondition::Always => response.status,
            };
            temporalstore_rust::types::ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            }
        }
        other => temporalstore_rust::types::ExecuteResponse {
            status: response.status,
            response: other,
        },
    }
}

fn run_shared_set_members_command(
    _case: &UnifiedCase,
    step: &UnifiedStep,
    add: bool,
    mut execute: impl FnMut(Command) -> temporalstore_rust::types::ExecuteResponse,
) -> temporalstore_rust::types::ExecuteResponse {
    let key = json_string(&step.command, "key");
    let members = json_member_values(&step.command);
    let before = execute(Command::SetMembers { key: key.clone() });
    if !before.status.ok {
        return before;
    }
    let mut existing = match before.response {
        CommandResponse::Members { members } => members.into_iter().collect::<BTreeSet<_>>(),
        other => {
            return temporalstore_rust::types::ExecuteResponse {
                status: Status::error("invalid_response", "set members returned wrong shape"),
                response: other,
            }
        }
    };
    let mut changed = 0;
    for member in members {
        let should_mutate = if add {
            existing.insert(member.clone())
        } else {
            existing.remove(&member)
        };
        if should_mutate {
            let response = execute(if add {
                Command::SetAdd {
                    key: key.clone(),
                    member,
                }
            } else {
                Command::SetRemove {
                    key: key.clone(),
                    member,
                }
            });
            if !response.status.ok {
                return response;
            }
            changed += 1;
        }
    }
    temporalstore_rust::types::ExecuteResponse {
        status: Status::ok(),
        response: CommandResponse::Integer { value: changed },
    }
}

fn json_string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("shared command missing string field {field}"))
        .to_string()
}

fn json_bytes(value: &Value, field: &str) -> Vec<u8> {
    value
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("shared command missing byte-array field {field}"))
        .iter()
        .map(|item| {
            item.as_u64()
                .unwrap_or_else(|| panic!("shared byte-array field {field} contains non-byte"))
                as u8
        })
        .collect()
}

fn json_string_array(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("shared command missing string-array field {field}"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("shared string-array field {field} contains non-string"))
                .to_string()
        })
        .collect()
}

fn json_member_values(value: &Value) -> Vec<Vec<u8>> {
    if value.get("members").is_some() {
        return json_string_array(value, "members")
            .into_iter()
            .map(String::into_bytes)
            .collect();
    }
    vec![json_bytes(value, "member")]
}

fn static_expected_allowed(expected: &UnifiedExpected) -> Option<bool> {
    match expected {
        UnifiedExpected::Static(value) => value.get("allowed").and_then(Value::as_bool),
        _ => None,
    }
}

fn static_expected_backend(expected: &UnifiedExpected) -> Option<&str> {
    match expected {
        UnifiedExpected::Static(value) => value.get("backend").and_then(Value::as_str),
        _ => None,
    }
}

fn is_storage_parity_command(command: &Value) -> bool {
    command_kind(command).starts_with("storage_")
}

fn maybe_run_shared_harness_command(case: &UnifiedCase, step: &UnifiedStep) -> bool {
    // Pointer steps name a lib test as their runner and carry arbitrary kind strings; the mode
    // field is what marks them, and they are not executable here.
    if step
        .command
        .get("mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| mode == "rust_executable_native_static")
    {
        return true;
    }
    let kind = command_kind(&step.command);
    match kind {
        "ops_readiness_service_summary" => {
            verify_ops_readiness_service_summary();
            true
        }
        "redis_feature_module_flow" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid shared harness command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("feature_module_flow"),
                "case={} step={} unsupported Redis/Feature scenario",
                case.name,
                step.name
            );
            verify_redis_feature_module_flow();
            true
        }
        "redis_operational_admin_flow" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid shared harness command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("operational_admin_commands"),
                "case={} step={} unsupported Redis/admin scenario",
                case.name,
                step.name
            );
            verify_redis_operational_admin_commands();
            true
        }
        "redis_slot_hash_crc64" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid shared harness command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("slot_hash_crc64"),
                "case={} step={} unsupported Redis slot/hash scenario",
                case.name,
                step.name
            );
            verify_redis_bucket_hash_crc64();
            true
        }
        "proxy_topology_churn_convergence" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy churn command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("two_proxy_topology_move_stale_cache_recovery"),
                "case={} step={} unsupported proxy churn scenario",
                case.name,
                step.name
            );
            verify_proxy_topology_churn_convergence();
            true
        }
        "proxy_admission_policy" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy admission command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("readonly_write_disabled_drop_percent_degraded_overload"),
                "case={} step={} unsupported proxy admission scenario",
                case.name,
                step.name
            );
            verify_proxy_admission_policy();
            true
        }
        "proxy_operational_surface_aliases" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy alias command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("admin_config_heartbeat_status_http_aliases"),
                "case={} step={} unsupported proxy alias scenario",
                case.name,
                step.name
            );
            verify_proxy_operational_surface_aliases();
            true
        }
        "proxy_tonic_streaming_contract" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy streaming command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("cancellation_backpressure_reconnect_callbacks"),
                "case={} step={} unsupported proxy streaming scenario",
                case.name,
                step.name
            );
            verify_proxy_tonic_streaming_contract();
            true
        }
        "proxy_route_quarantine_recovery" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy quarantine command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("backend_failure_quarantine_probe_recovery"),
                "case={} step={} unsupported proxy quarantine scenario",
                case.name,
                step.name
            );
            verify_proxy_route_quarantine_recovery();
            true
        }
        "proxy_multi_proxy_convergence_quarantine" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid multi-proxy command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("two_proxy_stale_cache_quarantine_recovery"),
                "case={} step={} unsupported multi-proxy scenario",
                case.name,
                step.name
            );
            verify_proxy_topology_churn_convergence();
            verify_proxy_route_quarantine_recovery();
            true
        }
        "proxy_grafana_prometheus_metric_parity" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy metrics command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("proxy_metric_families_dashboard_alerts"),
                "case={} step={} unsupported proxy metrics scenario",
                case.name,
                step.name
            );
            verify_proxy_grafana_prometheus_metric_parity();
            true
        }
        "client_partition_set_route_cache" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client partition-set command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("partition_set_member_version_route_cache"),
                "case={} step={} unsupported client partition-set scenario",
                case.name,
                step.name
            );
            verify_client_partition_set_route_cache();
            true
        }
        "client_retry_budget_topology_refresh" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client retry-budget command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("read_retry_write_single_shot_topology_safe_retry"),
                "case={} step={} unsupported client retry-budget scenario",
                case.name,
                step.name
            );
            verify_client_retry_budget_topology_refresh();
            true
        }
        "client_metasync_outage_churn" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client MetaSync command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("deadline_backoff_topology_refresh"),
                "case={} step={} unsupported client MetaSync scenario",
                case.name,
                step.name
            );
            verify_client_metasync_outage_churn();
            true
        }
        "client_pipeline_batch_partial_timeout" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client pipeline command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("ordered_batch_partial_failure_timeout_budget"),
                "case={} step={} unsupported client pipeline scenario",
                case.name,
                step.name
            );
            verify_client_pipeline_batch_partial_timeout();
            true
        }
        "client_deployment_placement_routing" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client placement command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("location_affine_secondary_reads_primary_only_writes"),
                "case={} step={} unsupported client placement scenario",
                case.name,
                step.name
            );
            verify_client_deployment_placement_routing();
            true
        }
        "metaserver_scheduler_control_plane" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid metaserver scheduler command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("partition_set_scheduler_tokens_raft_membership_safe_mode"),
                "case={} step={} unsupported metaserver scheduler scenario",
                case.name,
                step.name
            );
            verify_metaserver_scheduler_control_plane();
            true
        }
        "raft_openraft_process_path_default_gate" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid Raft process-path command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("production_openraft_matrixraft_process_path_semantics"),
                "case={} step={} unsupported Raft process-path scenario",
                case.name,
                step.name
            );
            verify_raft_openraft_process_path_default_gate();
            true
        }
        "raft_linearizable_hash_failover" => {
            verify_raft_linearizable_hash_failover();
            true
        }
        "raft_membership_op" => {
            if case.name == "raft_matrixraft_membership_roles"
                && step.name == "setup_three_voter_cluster"
            {
                execute_raft_membership_shared_case(case);
            }
            true
        }
        _ => false,
    }
}

fn verify_ops_readiness_service_summary() {
    let report = production_readiness_report();
    assert_eq!(
        report.known_services(),
        vec![
            "client",
            "proxy",
            "ingestion",
            "data_node",
            "metaserver",
            "storage_cache",
            "feature_modules",
            "context_workflow",
            "fault_tolerance",
            "deployment_ops",
            "scale_testing",
            "raft_replication"
        ]
    );
    let gates = report.service_gate_reports();
    assert_eq!(gates.len(), 12);
    for (order, service, owner) in [
        (1, "client", "client_sdk"),
        (2, "proxy", "proxy_runtime"),
        (3, "ingestion", "ingestion_connectors"),
        (4, "data_node", "data_node_runtime"),
        (5, "metaserver", "metaserver_control_plane"),
        (6, "storage_cache", "storage_runtime"),
        (7, "feature_modules", "feature_api"),
        (8, "context_workflow", "context_ai_workflow"),
        (9, "fault_tolerance", "reliability"),
        (10, "deployment_ops", "platform_ops"),
        (11, "scale_testing", "performance"),
        (12, "raft_replication", "consensus_runtime"),
    ] {
        assert!(
            gates.iter().any(|gate| gate.remediation_order == order
                && gate.service == service
                && gate.owner == owner),
            "missing service gate {order}/{service}/{owner}"
        );
    }
    for gate in &gates {
        assert!(
            gate.gate_status == "ready" || gate.gate_status == "blocked",
            "unexpected gate status for {}: {}",
            gate.service,
            gate.gate_status
        );
        if gate.ready {
            assert_eq!(gate.gate_status, "ready");
            assert_eq!(gate.blocker_count, 0);
            assert!(gate.failed_capabilities.is_empty());
        } else {
            assert_eq!(gate.gate_status, "blocked");
            assert!(
                gate.blocker_count > 0,
                "blocked gate {} must name blocker count",
                gate.service
            );
            assert!(
                !gate.failed_capabilities.is_empty(),
                "blocked gate {} must name failed capabilities",
                gate.service
            );
            assert!(
                !gate.next_action.trim().is_empty(),
                "blocked gate {} must include next action",
                gate.service
            );
        }
    }
    if let Some(blocked) = report.next_blocked_service() {
        assert!(!blocked.ready);
        assert_eq!(blocked.gate_status, "blocked");
    }
    let blocked_services = gates
        .iter()
        .filter(|gate| gate.gate_status != "ready")
        .map(|gate| gate.service.as_str())
        .collect::<Vec<_>>();
    assert_eq!(blocked_services, vec!["data_node", "raft_replication"]);
    assert_eq!(
        report.next_blocked_service().map(|gate| gate.service),
        Some("data_node".to_string())
    );
    let data_node = report
        .service_summary("data_node")
        .expect("data node service summary should be exported");
    assert!(!data_node.ready);
    assert!(data_node.areas.contains(&"dataserver".to_string()));
    assert!(data_node
        .areas
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node
        .blocker_classes
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node.next_action.contains("membership"));
    assert!(report
        .failed_capabilities_for_service("data_node")
        .iter()
        .any(|capability| capability
            .capability
            .contains("data-node multi-process rollout")));
    assert!(!report.service_ready("data_node"));
    let gate = report
        .service_gate_report("data_node")
        .expect("data node service gate report should be exported");
    assert!(!gate.ready);
    assert_eq!(gate.gate_status, "blocked");
    assert_eq!(gate.severity, "critical");
    assert_eq!(gate.remediation_order, 4);
    assert_eq!(gate.owner, "data_node_runtime");
    assert!(gate.failed_capabilities.iter().any(|capability| capability
        .capability
        .contains("data-node multi-process rollout")));
    let raft_gate = report
        .service_gate_report("raft_replication")
        .expect("raft replication service gate report should be exported");
    assert!(!raft_gate.ready);
    assert_eq!(raft_gate.gate_status, "blocked");
    assert_eq!(raft_gate.severity, "critical");
    assert!(raft_gate
        .failed_capabilities
        .iter()
        .any(|capability| capability
            .capability
            .contains("data-node multi-process rollout")));
    let scale_gate = report
        .service_gate_report("scale_testing")
        .expect("scale testing service gate report should be exported");
    assert!(scale_gate.ready);
    assert_eq!(scale_gate.gate_status, "ready");
    assert_eq!(scale_gate.blocker_count, 0);
}

fn verify_redis_feature_module_flow() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let run = |args: Vec<Vec<u8>>| {
        execute_redis_command(args, 1, |command| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            if response.status.ok {
                Ok(response.response)
            } else {
                Err(response.status.message)
            }
        })
    };
    let s = |value: &str| value.as_bytes().to_vec();

    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("100"), s("2")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("200"), s("3")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAGG"), s("rf"), s("0"), s("300"), s("sum")]),
        RespValue::Integer(5)
    );
    assert_eq!(
        run(vec![s("FQUERY"), s("rf"), s("0"), s("300"), s("10")]),
        RespValue::Array(vec![
            RespValue::Array(vec![RespValue::Integer(100), RespValue::Bulk(Some(s("2")))]),
            RespValue::Array(vec![RespValue::Integer(200), RespValue::Bulk(Some(s("3")))]),
        ])
    );

    let encoded = SequenceFeatureRow {
        timestamp_ms: 300,
        gid: 42,
        action_type: 3,
        duration: 90,
        author_id: 7,
    }
    .encode_feature_proto_value();
    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("300"), encoded.clone()]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![
            s("FQUERYFILTERSTR"),
            s("rf"),
            s("0"),
            s("400"),
            s("10"),
            s("action_type = 3"),
            s("duration > 80"),
        ]),
        RespValue::Array(vec![RespValue::Array(vec![
            RespValue::Integer(300),
            RespValue::Bulk(Some(encoded)),
        ])])
    );

    assert_eq!(
        run(vec![
            s("FAPPENDPOLICY"),
            s("rf"),
            s("300"),
            s("ignored"),
            s("insert_if_absent"),
        ]),
        RespValue::Integer(0)
    );
    assert_eq!(
        run(vec![
            s("FREPLACE"),
            s("rf"),
            s("0"),
            s("250"),
            s("150"),
            s("10")
        ]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAGG"), s("rf"), s("0"), s("400"), s("sum")]),
        RespValue::Integer(10)
    );
    assert_eq!(
        run(vec![s("FDEL"), s("rf")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FQUERY"), s("rf"), s("0"), s("400"), s("10")]),
        RespValue::Array(Vec::new())
    );
}

fn verify_redis_operational_admin_commands() {
    let mut state = RedisCommandState::default();
    let run = |state: &mut RedisCommandState, args: Vec<&str>| {
        execute_redis_command_with_state(
            args.into_iter()
                .map(|arg| arg.as_bytes().to_vec())
                .collect(),
            7,
            state,
            |_| Err("unexpected data command".to_string()),
        )
    };

    assert_eq!(
        run(&mut state, vec!["CONFIG", "GET", "requirepass"]),
        RespValue::Array(vec![
            RespValue::Bulk(Some(b"requirepass".to_vec())),
            RespValue::Bulk(Some(Vec::new())),
        ])
    );
    assert_eq!(
        run(&mut state, vec!["CONFIG", "SET", "requirepass", "secret"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["AUTH", "bad"]),
        RespValue::Error("ERR invalid password".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["AUTH", "secret"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert!(state.authenticated);
    assert_eq!(
        run(&mut state, vec!["ECHO", "hello"]),
        RespValue::Bulk(Some(b"hello".to_vec()))
    );
    assert_eq!(
        run(&mut state, vec!["SELECT", "0"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["SELECT", "1"]),
        RespValue::Error("ERR DB index is out of range".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["SLAVEOF", "127.0.0.1", "18001"]),
        RespValue::SimpleString("OK".to_string())
    );
    let info = run(&mut state, vec!["INFO", "replication"]);
    match info {
        RespValue::Bulk(Some(bytes)) => {
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains("role:slave"));
            assert!(text.contains("master_host:127.0.0.1"));
            assert!(text.contains("master_port:18001"));
        }
        other => panic!("unexpected info response: {other:?}"),
    }
    assert_eq!(
        run(&mut state, vec!["SLAVEOF", "NO", "ONE"]),
        RespValue::SimpleString("OK".to_string())
    );
    let info = run(&mut state, vec!["INFO", "replication"]);
    match info {
        RespValue::Bulk(Some(bytes)) => {
            assert!(String::from_utf8(bytes).unwrap().contains("role:master"));
        }
        other => panic!("unexpected info response: {other:?}"),
    }
    assert_eq!(
        run(
            &mut state,
            vec!["PARTITION", "LOAD", "7", "1", "file:///tmp/partition"]
        ),
        RespValue::SimpleString("OK".to_string())
    );
    let partition = run(&mut state, vec!["PARTITION", "INFO"]);
    match partition {
        RespValue::Bulk(Some(bytes)) => {
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains("partition_id:7"));
            assert!(text.contains("partition_loading_stats:loaded"));
        }
        other => panic!("unexpected partition info response: {other:?}"),
    }
    assert_eq!(
        run(&mut state, vec!["BGSAVE"]),
        RespValue::SimpleString("Background saving started".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["CONFIG", "REWRITE"]),
        RespValue::SimpleString("OK".to_string())
    );
}

fn verify_redis_bucket_hash_crc64() {
    let mut state = RedisCommandState::default();
    let mut run = |args: Vec<&str>| {
        execute_redis_command_with_state(
            args.into_iter()
                .map(|arg| arg.as_bytes().to_vec())
                .collect(),
            1,
            &mut state,
            |_| Err("unexpected data command".to_string()),
        )
    };

    assert_eq!(
        run(vec!["PSLOTHASHKEY", "123456789"]),
        RespValue::Integer(0x3a71_b645)
    );
    assert_eq!(
        run(vec!["PCLUSTERKEYSLOT", "123456789"]),
        RespValue::Integer(0x3a71_b645)
    );
    assert_eq!(
        run(vec!["PCLUSTERHASH", "123456789"]),
        RespValue::Integer(0xe9c6_d914_c4b8_d9cau64 as i64)
    );
}

fn verify_proxy_admission_policy() {
    let readonly = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::Readonly,
        ..ProxyOptions::default()
    });
    let write = readonly.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "blocked-write".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(write.status.code, "proxy_write_disabled");
    let preflight = readonly.preflight_report();
    assert_eq!(preflight.policy.serving_mode, ProxyServingMode::Readonly);
    assert!(!preflight.policy.serving_writes);
    assert!(preflight
        .degraded_reasons
        .contains(&"admission_rejections".to_string()));

    let write_disabled = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::WriteDisabled,
        ..ProxyOptions::default()
    });
    let blocked = write_disabled.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(blocked.status.code, "proxy_write_disabled");

    let not_serving = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::NotServing,
        ..ProxyOptions::default()
    });
    let read = not_serving.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(read.status.code, "proxy_not_serving");
    assert!(not_serving.policy_report().rejecting_all);

    let degraded = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::Degraded,
        ..ProxyOptions::default()
    });
    assert_eq!(
        degraded.preflight_report().policy.serving_mode,
        ProxyServingMode::Degraded
    );

    let dropper = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        drop_percent: 100,
        ..ProxyOptions::default()
    });
    let dropped = dropper.table_batch_execute(ProxyTableBatchExecuteRequest {
        namespace: "ns".to_string(),
        table_name: "tbl".to_string(),
        commands: vec![Command::StringGet {
            key: "drop-me".to_string(),
        }],
    });
    assert_eq!(dropped.status.code, "proxy_traffic_dropped");
    let policy = dropper.policy_report();
    assert_eq!(policy.drop_percent, 100);
    assert_eq!(policy.admission_rejections, 1);
}

fn proxy_get_json(proxy: &ProxyService, path: &str) -> Value {
    let (code, body) = proxy.handle(HttpRequest {
        method: "GET".to_string(),
        path: path.to_string(),
        body: Vec::new(),
    });
    assert_eq!(code, 200, "GET {path} failed");
    parse_json::<Value>(&body).unwrap_or_else(|error| panic!("GET {path} invalid JSON: {error}"))
}

fn verify_proxy_operational_surface_aliases() {
    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        proxy_addr: "127.0.0.1:17123".to_string(),
        namespace: "ns".to_string(),
        location: "iad".to_string(),
        ..ProxyOptions::default()
    });

    let ports = proxy_get_json(&proxy, "/ProxyService/GetPorts");
    assert_eq!(ports["listen_port"], 17_123);
    assert_eq!(ports["announce_port"], 17_123);

    let config = proxy_get_json(&proxy, "/ProxyService/GetConfig");
    assert_eq!(config["namespace"], "ns");
    assert_eq!(config["proxy_addr"], "127.0.0.1:17123");

    let heartbeat = proxy_get_json(&proxy, "/ProxyService/Heartbeat");
    assert_eq!(heartbeat["meta_addr"], "127.0.0.1:1");
    assert_eq!(heartbeat["route_cache_size"], 0);

    let surface = proxy.operational_surface_report();
    assert!(surface.status.ok);
    assert!(!surface.legacy_brpc_thrift_in_scope);
    assert!(surface.rust_native_aliases_ready);
    for alias in [
        "/ProxyService/GetPorts",
        "/ProxyService/GetConfig",
        "/ProxyService/Heartbeat",
        "/ProxyService/Preflight",
        "/ProxyService/GetConsulNames",
        "/ProxyService/NotifyStop",
        "/ProxyService/GetMigrationContract",
        "/ProxyService/GetPolicy",
        "/ProxyService/Metrics",
    ] {
        assert!(
            surface
                .entries
                .iter()
                .any(|entry| entry.rust_alias == alias),
            "missing proxy alias {alias}"
        );
    }

    let migration = proxy.native_migration_contract();
    assert!(!migration.legacy_wire_in_scope);
    assert!(migration.http_json_aliases_ready);
    assert!(migration.resp_migration_ready);
    assert!(migration.tonic_streaming_ready);
    assert!(migration.topology_version_invalidation_preserved);
    assert!(migration.admission_policy_preserved);
    assert!(migration.backend_quarantine_preserved);
}

fn verify_proxy_tonic_streaming_contract() {
    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        ..ProxyOptions::default()
    });
    let contract = proxy.tonic_streaming_contract();
    assert_eq!(contract.service_name, "temporalstore.v1.ProxyService");
    assert_eq!(contract.execute_stream_method, "ProxyExecuteStream");
    assert_eq!(contract.route_callback_stream_method, "RouteCallbacks");
    assert_eq!(contract.preflight_watch_method, "WatchProxyPreflight");
    assert!(contract.long_running_request_ready);
    assert!(contract.cancellation_ready);
    assert!(contract.backpressure_ready);
    assert!(contract.reconnect_ready);
    assert_eq!(contract.backpressure_status_code, "resource_exhausted");
    for case in [
        "long_running_request",
        "client_cancellation",
        "server_backpressure",
        "callback_reconnect",
    ] {
        assert!(contract.maturity_cases.contains(&case.to_string()));
    }
    let routed = proxy_get_json(&proxy, "/ProxyService/GetTonicContract");
    assert_eq!(routed["service_name"], "temporalstore.v1.ProxyService");
    assert_eq!(routed["backpressure_ready"], true);
}

fn verify_proxy_route_quarantine_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let server_addr = free_local_addr();
    start_temporal_engine_http_service(server_addr.clone(), engine.clone());

    let meta = SingleNodeMeta::default();
    let bad_server = "127.0.0.1:1".to_string();
    meta.register_server(RegisterServerRequest {
        numa_nodes: Vec::new(),
        server_addr: bad_server.clone(),
        node_id: 1,
        location: "proxy-quarantine-zone".to_string(),
        binary_version: "unified-proxy".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: bad_server.clone(),
        })
        .status
        .ok
    );
    let meta_addr = free_local_addr();
    start_single_node_meta_http_service(meta_addr.clone(), meta.clone());
    wait_for_http(&server_addr);
    wait_for_http(&meta_addr);

    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: meta_addr.clone(),
        route_cache_ttl_ms: 60_000,
        backend_continuous_failed_time_ms: 5,
        connect_timeout_ms: 50,
        io_timeout_ms: 200,
        ..ProxyOptions::default()
    });

    let failed = proxy.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "quarantine-key".to_string(),
            value: b"recovered".to_vec(),
        },
    });
    assert!(!failed.status.ok, "{failed:?}");
    std::thread::sleep(Duration::from_millis(10));

    meta.register_server(RegisterServerRequest {
        numa_nodes: Vec::new(),
        server_addr: server_addr.clone(),
        node_id: 2,
        location: "proxy-quarantine-zone".to_string(),
        binary_version: "unified-proxy".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: server_addr.clone(),
        })
        .status
        .ok
    );

    let response = proxy.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "quarantine-key".to_string(),
            value: b"recovered".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");
    assert_eq!(
        proxy
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "quarantine-key".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"recovered".to_vec())
        }
    );
    let info = proxy.info();
    assert!(info.stats.route_refreshes >= 1);
    assert!(info.route_cache_size >= 1);
    assert_eq!(
        proxy.preflight_report().client.topology_cache.route_count,
        1
    );
}

fn verify_proxy_grafana_prometheus_metric_parity() {
    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::NotServing,
        drop_percent: 17,
        ..ProxyOptions::default()
    });
    let _ = proxy.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "blocked".to_string(),
        },
    });
    let metrics = proxy.prometheus_metrics();
    for family in [
        "temporalstore_proxy_requests_total",
        "temporalstore_proxy_route_cache_entries",
        "temporalstore_proxy_route_cache_events_total",
        "temporalstore_proxy_backend_events_total",
        "temporalstore_proxy_serving_mode",
        "temporalstore_proxy_drop_percent",
        "temporalstore_proxy_metric_family_parity",
        "temporalstore_proxy_service_registry_state",
        "temporalstore_proxy_service_registry_events_total",
        "temporalstore_production_readiness_ready",
        "temporalstore_production_readiness_service_ready",
    ] {
        assert!(
            metrics.contains(family),
            "Prometheus output missing proxy/readiness family {family}"
        );
    }
    assert!(metrics.contains("temporalstore_proxy_serving_mode{mode=\"not_serving\"} 1"));
    assert!(metrics.contains("temporalstore_proxy_drop_percent 17"));
    assert!(metrics.contains("grafana_panel=\"Proxy Requests And Admission\""));

    let report = proxy.metrics_parity_report();
    assert!(report.status.ok);
    assert!(report.grafana_panels_ready);
    assert!(report.alerts_ready);
    assert!(report
        .rust_prometheus_families
        .contains(&"temporalstore_proxy_metric_family_parity".to_string()));
    assert!(report.mappings.iter().any(|mapping| {
        mapping.native_surface.contains("command/admission")
            && mapping.rust_prometheus_family == "temporalstore_proxy_requests_total"
            && mapping.grafana_panel == "Proxy Requests And Admission"
            && mapping.covered
    }));
}

fn verify_proxy_topology_churn_convergence() {
    let dir_a = tempfile::tempdir().unwrap();
    let engine_a = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir_a.path().join("cache"),
        dir_a.path().join("pages"),
        dir_a.path().join("indexes"),
    );
    engine_a.load_shard(1);
    let server_a = free_local_addr();
    start_temporal_engine_http_service(server_a.clone(), engine_a.clone());

    let dir_b = tempfile::tempdir().unwrap();
    let engine_b = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir_b.path().join("cache"),
        dir_b.path().join("pages"),
        dir_b.path().join("indexes"),
    );
    engine_b.load_shard(1);
    let server_b = free_local_addr();
    start_temporal_engine_http_service(server_b.clone(), engine_b.clone());

    let meta = SingleNodeMeta::default();
    meta.register_server(RegisterServerRequest {
        numa_nodes: Vec::new(),
        server_addr: server_a.clone(),
        node_id: 1,
        location: "zone-a".to_string(),
        binary_version: "unified-a".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: server_a.clone(),
        })
        .status
        .ok
    );
    let meta_addr = free_local_addr();
    start_single_node_meta_http_service(meta_addr.clone(), meta.clone());

    wait_for_http(&server_a);
    wait_for_http(&server_b);
    wait_for_http(&meta_addr);

    let proxy_a = ProxyService::new(ProxyOptions {
        meta_addr: meta_addr.clone(),
        route_cache_ttl_ms: 60_000,
        connect_timeout_ms: 50,
        io_timeout_ms: 200,
        ..ProxyOptions::default()
    });
    let proxy_b = ProxyService::new(ProxyOptions {
        meta_addr,
        route_cache_ttl_ms: 60_000,
        connect_timeout_ms: 50,
        io_timeout_ms: 200,
        ..ProxyOptions::default()
    });

    for (proxy, key, value) in [
        (&proxy_a, "proxy-a-before", b"a-before".to_vec()),
        (&proxy_b, "proxy-b-before", b"b-before".to_vec()),
    ] {
        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
        assert_eq!(proxy.info().route_cache_size, 1);
    }

    meta.register_server(RegisterServerRequest {
        numa_nodes: Vec::new(),
        server_addr: server_b.clone(),
        node_id: 2,
        location: "zone-b".to_string(),
        binary_version: "unified-b".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            shard_id: 1,
            server_addr: server_b.clone(),
        })
        .status
        .ok
    );

    for (proxy, key, value) in [
        (&proxy_a, "proxy-a-after", b"a-after".to_vec()),
        (&proxy_b, "proxy-b-after", b"b-after".to_vec()),
    ] {
        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }

    for (key, value) in [
        ("proxy-a-before", b"a-before".to_vec()),
        ("proxy-b-before", b"b-before".to_vec()),
    ] {
        assert_eq!(
            engine_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(value) }
        );
    }
    for (key, value) in [
        ("proxy-a-after", b"a-after".to_vec()),
        ("proxy-b-after", b"b-after".to_vec()),
    ] {
        assert_eq!(
            engine_b
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(value) }
        );
    }
    assert_eq!(
        engine_a
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "proxy-a-after".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );

    for (label, proxy) in [("proxy_a", &proxy_a), ("proxy_b", &proxy_b)] {
        let preflight = proxy.preflight_report();
        assert!(!preflight.topology_cache_stale, "{label}: {preflight:?}");
        assert_eq!(preflight.client.topology_cache.route_count, 1, "{label}");
        assert!(
            preflight.client.route_refreshes >= 2,
            "{label}: {preflight:?}"
        );
        let route = preflight
            .client
            .topology_cache
            .routes
            .first()
            .unwrap_or_else(|| panic!("{label}: missing route"));
        assert_eq!(route.primary_addr, server_b, "{label}: {route:?}");
    }
}

fn verify_client_partition_set_route_cache() {
    let meta_addr = free_local_addr();
    let primary_addr = "127.0.0.1:27101".to_string();
    let replica_addr = "127.0.0.1:27102".to_string();
    let meta_addr_for_listener = meta_addr.clone();
    let primary_for_meta = primary_addr.clone();
    let replica_for_meta = replica_addr.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => json_response(
                    200,
                    &TableTopologyResponse {
                        status: Status::ok(),
                        table: Some(TableMetaInfo {
                            table_id: 42,
                            namespace: "ns".to_string(),
                            table_name: "native_parts".to_string(),
                            state: MetaEntityState::Normal,
                            topology_version: 12,
                            first_shard_id: PartitionId::new(42, 0, 0, 17).unwrap().id(),
                            shard_count: 2,
                            replica_count: 2,
                            partition_version: 17,
                            serving_options: Default::default(),
                        }),
                        shards: vec![
                            TableShard {
                                shard_id: PartitionId::new(42, 0, 0, 17).unwrap().id(),
                                start_bucket: 0,
                                end_bucket: 536_870_911,
                                primary: Some(primary_for_meta.clone()),
                                replicas: vec![primary_for_meta.clone(), replica_for_meta.clone()],
                                primary_endpoint: Some(ServerEndpoint {
                                    server_addr: primary_for_meta.clone(),
                                    location: "zone-a".to_string(),
                                }),
                                replica_endpoints: vec![ServerEndpoint {
                                    server_addr: replica_for_meta.clone(),
                                    location: "zone-b".to_string(),
                                }],
                            },
                            TableShard {
                                shard_id: PartitionId::new(42, 1, 0, 17).unwrap().id(),
                                start_bucket: 536_870_912,
                                end_bucket: 1_073_741_823,
                                primary: Some(replica_for_meta.clone()),
                                replicas: vec![replica_for_meta.clone()],
                                primary_endpoint: Some(ServerEndpoint {
                                    server_addr: replica_for_meta.clone(),
                                    location: "zone-b".to_string(),
                                }),
                                replica_endpoints: Vec::new(),
                            },
                        ],
                        unchanged: false,
                    },
                ),
                ("POST", "/meta/topology_version") => json_response(
                    200,
                    &TopologyVersionReport {
                        status: Status::ok(),
                        current_topology_version: 12,
                        old_topology_version: 0,
                        unchanged: false,
                        server_count: 0,
                        proxy_count: 0,
                        table_count: 1,
                        shard_route_count: 2,
                        normal_servers: 0,
                        frozen_servers: 0,
                        dropped_servers: 0,
                        normal_proxies: 0,
                        frozen_proxies: 0,
                        dropped_proxies: 0,
                        normal_tables: 1,
                        frozen_tables: 0,
                        dropped_tables: 0,
                        changed_tables: Vec::new(),
                        events: Vec::new(),
                        event_history_truncated: false,
                    },
                ),
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some(meta_addr),
        route_cache_ttl_ms: 60_000,
        ..ClientOptions::default()
    });
    let options = client.sync_table_topology("ns", "native_parts").unwrap();
    assert_eq!(options.table_id, 42);
    assert_eq!(options.partition_version, 17);

    let report = client.preflight_report();
    assert_eq!(report.native_partition_sets.len(), 1);
    let partition_set = &report.native_partition_sets[0];
    assert_eq!(partition_set.table_id, 42);
    assert_eq!(partition_set.combine_name, "ns/native_parts");
    assert_eq!(partition_set.partition_version, 17);
    assert_eq!(partition_set.topology_version, 12);
    assert_eq!(partition_set.partition_count, 2);
    assert_eq!(partition_set.missing_route_count, 0);
    assert_eq!(partition_set.members[0].start_bucket, 0);
    assert_eq!(partition_set.members[0].end_bucket, 536_870_911);
    assert_eq!(partition_set.members[1].start_bucket, 536_870_912);
    assert_eq!(partition_set.members[1].end_bucket, 1_073_741_823);
    assert_eq!(
        partition_set.members[0].primary_addr.as_deref(),
        Some(primary_addr.as_str())
    );
    assert_eq!(
        partition_set.members[0].replica_addrs,
        vec![replica_addr.clone()]
    );

    let topology_report = client.topology_cache_report();
    assert_eq!(topology_report.route_count, 2);
    assert!(topology_report
        .routes
        .iter()
        .all(|route| route.table == "ns/native_parts"
            && route.partition_version == 17));
    assert_eq!(
        topology_report.routes[0].partition_id,
        partition_set.members[0].partition_id
    );
}

fn verify_client_retry_budget_topology_refresh() {
    let read_addr = free_local_addr();
    let read_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let read_attempts_for_server = Arc::clone(&read_attempts);
    let read_addr_for_listener = read_addr.clone();
    std::thread::spawn(move || {
        serve(&read_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    let attempt =
                        read_attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt == 0 {
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::error("retry_later", "loading"),
                                response: CommandResponse::Empty,
                            },
                        )
                    } else {
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::ok(),
                                response: CommandResponse::Bytes {
                                    value: Some(b"ok".to_vec()),
                                },
                            },
                        )
                    }
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&read_addr);
    let read_client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: read_addr,
        ..ClientOptions::default()
    });
    let read_table = read_client.open_table(
        "ns",
        "read_retry",
        TableOptions {
            retry_backoff_ms: 0,
            ..TableOptions::default()
        },
    );
    assert_eq!(read_table.get("retry-key").unwrap(), Some(b"ok".to_vec()));
    assert_eq!(read_attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

    let write_addr = free_local_addr();
    let write_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write_attempts_for_server = Arc::clone(&write_attempts);
    let write_addr_for_listener = write_addr.clone();
    std::thread::spawn(move || {
        serve(&write_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    write_attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    json_response(
                        200,
                        &ExecuteResponse {
                            status: Status::error("retry_later", "write loading"),
                            response: CommandResponse::Empty,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&write_addr);
    let write_client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: write_addr,
        ..ClientOptions::default()
    });
    let write_table = write_client.open_table("ns", "write_retry", TableOptions::default());
    let err = write_table.set("retry-write", b"v".to_vec()).unwrap_err();
    assert!(err.to_string().contains("write loading"));
    assert_eq!(
        write_attempts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "unsafe writes without explicit budget must not duplicate a possibly applied write"
    );
}

fn verify_client_metasync_outage_churn() {
    let meta_addr = free_local_addr();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_for_listener = Arc::clone(&attempts);
    let listener_addr = meta_addr.clone();
    std::thread::spawn(move || {
        serve(&listener_addr, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/meta/topology_version") => json_response(
                    200,
                    &TopologyVersionReport {
                        status: Status::ok(),
                        current_topology_version: 40,
                        old_topology_version: 0,
                        unchanged: false,
                        server_count: 1,
                        proxy_count: 0,
                        table_count: 1,
                        shard_route_count: 1,
                        normal_servers: 1,
                        frozen_servers: 0,
                        dropped_servers: 0,
                        normal_proxies: 0,
                        frozen_proxies: 0,
                        dropped_proxies: 0,
                        normal_tables: 1,
                        frozen_tables: 0,
                        dropped_tables: 0,
                        changed_tables: Vec::new(),
                        events: Vec::new(),
                        event_history_truncated: false,
                    },
                ),
                ("POST", "/tables/topology") => {
                    let attempt =
                        attempts_for_listener.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt < 2 {
                        return json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::error("metaserver_unavailable", "outage"),
                                table: None,
                                shards: Vec::new(),
                                unchanged: false,
                            },
                        );
                    }
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(TableMetaInfo {
                                table_id: 91,
                                namespace: "ns".to_string(),
                                table_name: "churn".to_string(),
                                state: MetaEntityState::Normal,
                                topology_version: 40,
                                first_shard_id: 40,
                                shard_count: 1,
                                replica_count: 1,
                                partition_version: 0,
                                serving_options: Default::default(),
                            }),
                            shards: vec![TableShard {
                                shard_id: 40,
                                start_bucket: 0,
                                end_bucket: 1_073_741_823,
                                primary: Some("127.0.0.1:27440".to_string()),
                                replicas: vec!["127.0.0.1:27440".to_string()],
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        meta_addr: Some(meta_addr),
        meta_sync_interval_ms: 50,
        topo_error_retry_interval_ms: 5,
        meta_sync_deadline_ms: 7,
        meta_sync_jitter_percent: 50,
        ..ClientOptions::default()
    });
    client.open_table(
        "ns",
        "churn",
        TableOptions {
            first_shard_id: 10,
            shard_count: 1,
            ..TableOptions::default()
        },
    );

    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let first_error = client_metasync_table_report(&client, "ns/churn");
    assert_eq!(first_error.consecutive_errors, 1);
    assert_eq!(first_error.last_error, "outage");
    let first_delay = first_error
        .next_sync_after_unix_ms
        .saturating_sub(first_error.last_error_unix_ms);
    assert!((5..=8).contains(&first_delay), "{first_error:?}");

    std::thread::sleep(Duration::from_millis(12));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let second_error = client_metasync_table_report(&client, "ns/churn");
    assert_eq!(second_error.consecutive_errors, 2);
    let second_delay = second_error
        .next_sync_after_unix_ms
        .saturating_sub(second_error.last_error_unix_ms);
    assert!((10..=15).contains(&second_delay), "{second_error:?}");

    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let success = client_metasync_table_report(&client, "ns/churn");
    assert_eq!(success.consecutive_errors, 0);
    assert_eq!(success.last_topology_version, 40);
    assert!(success.next_sync_after_unix_ms > success.last_success_unix_ms);
    let table = client.cached_table("ns", "churn").unwrap();
    assert_eq!(table.options().first_shard_id, 40);
    assert_eq!(client.topology_cache_report().max_topology_version, 40);
}

fn verify_client_pipeline_batch_partial_timeout() {
    let proxy_addr = free_local_addr();
    let batch_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let batch_requests_for_server = Arc::clone(&batch_requests);
    let proxy_addr_for_listener = proxy_addr.clone();
    std::thread::spawn(move || {
        serve(&proxy_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/batch_execute") => {
                    batch_requests_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                    assert_eq!(req.commands.len(), 3);
                    json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::ok(),
                            responses: vec![
                                ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                },
                                ExecuteResponse {
                                    status: Status::error("partial_failure", "field rejected"),
                                    response: CommandResponse::Empty,
                                },
                                ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Bytes {
                                        value: Some(b"after-partial".to_vec()),
                                    },
                                },
                            ],
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&proxy_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr,
        max_retries: 0,
        ..ClientOptions::default()
    });
    let table = client.open_table(
        "ns",
        "pipe",
        TableOptions {
            connect_timeout_ms: 200,
            io_timeout_ms: 250,
            max_write_retries: 0,
            retry_backoff_ms: 0,
            ..TableOptions::default()
        },
    );
    assert_eq!(table.options().connect_timeout_ms, 200);
    assert_eq!(table.options().io_timeout_ms, 250);
    assert_eq!(table.options().max_write_retries, 0);

    let mut pipeline = table.pipeline();
    pipeline.set("pipe-key", b"value".to_vec());
    pipeline.hset("pipe-key", "field", b"value".to_vec());
    pipeline.get("pipe-key");
    let response = pipeline.sync().unwrap();
    assert!(response.status.ok);
    assert_eq!(response.responses.len(), 3);
    assert!(response.responses[0].status.ok);
    assert_eq!(response.responses[1].status.code, "partial_failure");
    assert_eq!(
        response.responses[2].response,
        CommandResponse::Bytes {
            value: Some(b"after-partial".to_vec())
        }
    );
    assert_eq!(
        batch_requests.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "unsafe write batches must not be retried without explicit write budget"
    );
}

fn verify_client_deployment_placement_routing() {
    let primary_addr = free_local_addr();
    let replica_addr = free_local_addr();
    let meta_addr = free_local_addr();
    let primary_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let primary_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let replica_writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let replica_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    start_client_placement_endpoint(
        primary_addr.clone(),
        Arc::clone(&primary_writes),
        Arc::clone(&primary_reads),
        b"primary".to_vec(),
        true,
    );
    start_client_placement_endpoint(
        replica_addr.clone(),
        Arc::clone(&replica_writes),
        Arc::clone(&replica_reads),
        b"replica-local".to_vec(),
        false,
    );

    let meta_addr_for_listener = meta_addr.clone();
    let primary_for_meta = primary_addr.clone();
    let replica_for_meta = replica_addr.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => json_response(
                    200,
                    &TableTopologyResponse {
                        status: Status::ok(),
                        table: Some(TableMetaInfo {
                            table_id: 81,
                            namespace: "ns".to_string(),
                            table_name: "placed".to_string(),
                            state: MetaEntityState::Normal,
                            topology_version: 11,
                            first_shard_id: 81,
                            shard_count: 1,
                            replica_count: 2,
                            partition_version: 3,
                            serving_options: temporalstore_rust::meta::TableServingOptions {
                                pin_primary: false,
                                replica_read_policy: "round_robin_replica".to_string(),
                                preferred_location: String::new(),
                                drop_percent: 0,
                                max_read_retries: 1,
                                max_write_retries: 1,
                                retry_backoff_ms: 1,
                                continuous_failed_time_ms: 100,
                                io_timeout_ms: 1_000,
                                connect_timeout_ms: 1_000,
                                set_fields: Default::default(),
                            },
                        }),
                        shards: vec![TableShard {
                            shard_id: 81,
                            start_bucket: 0,
                            end_bucket: u64::MAX,
                            primary: Some(primary_for_meta.clone()),
                            replicas: vec![primary_for_meta.clone(), replica_for_meta.clone()],
                            primary_endpoint: Some(ServerEndpoint {
                                server_addr: primary_for_meta.clone(),
                                location: "zone-primary".to_string(),
                            }),
                            replica_endpoints: vec![
                                ServerEndpoint {
                                    server_addr: primary_for_meta.clone(),
                                    location: "zone-primary".to_string(),
                                },
                                ServerEndpoint {
                                    server_addr: replica_for_meta.clone(),
                                    location: "zone-local".to_string(),
                                },
                            ],
                        }],
                        unchanged: false,
                    },
                ),
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });

    wait_for_http(&primary_addr);
    wait_for_http(&replica_addr);
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some(meta_addr),
        local_location: "zone-local".to_string(),
        route_cache_ttl_ms: 60_000,
        ..ClientOptions::default()
    });
    let placement = client.deployment_placement_policy("neptune-prod");
    assert_eq!(placement.preferred_location, "zone-local");
    assert!(placement.require_location_affinity);

    let table = client.open_table_from_meta("ns", "placed").unwrap();
    assert_eq!(
        table.options().replica_read_policy,
        ClientReplicaReadPolicy::RoundRobinReplica
    );
    assert_eq!(table.options().preferred_location, "zone-local");
    assert!(!table.options().pin_primary);

    table.set("placed-key", b"value".to_vec()).unwrap();
    assert_eq!(
        table.get("placed-key").unwrap(),
        Some(b"replica-local".to_vec())
    );

    assert_eq!(primary_writes.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(primary_reads.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(replica_reads.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(replica_writes.load(std::sync::atomic::Ordering::SeqCst), 0);
}

fn verify_metaserver_scheduler_control_plane() {
    let meta = SingleNodeMeta::default();
    for (node_id, addr, location) in [
        (1, "127.0.0.1:27111", "zone-a"),
        (2, "127.0.0.1:27112", "zone-b"),
        (3, "127.0.0.1:27113", "zone-c"),
    ] {
        meta.register_server(RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: addr.to_string(),
            node_id,
            location: location.to_string(),
            binary_version: "unified-meta".to_string(),
        });
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: addr.to_string(),
            boot_time_ms: node_id,
            binary_version: "unified-meta".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        });
    }

    let added = meta.add_table(AddTableRequest {
        namespace: "ns".to_string(),
        table_name: "meta_parts".to_string(),
        first_shard_id: 0,
        shard_count: 2,
        replica_count: 2,
        partition_version: 23,
        serving_options: Default::default(),
    });
    assert!(added.status.ok, "{added:?}");

    // Native partition ids: shard_id = first_shard_id + offset.
    let first = 0;
    let second = 1;
    assert!(
        meta.register(RegisterShardRequest {
            shard_id: first,
            server_addr: "127.0.0.1:27111".to_string(),
        })
        .status
        .ok
    );

    let topology = meta.get_table_topology(GetTableTopologyRequest {
        client_location: String::new(),
        namespace: "ns".to_string(),
        table_name: "meta_parts".to_string(),
        old_topology_version: 0,
    });
    assert!(topology.status.ok, "{topology:?}");
    let table = topology.table.as_ref().unwrap();
    assert_eq!(table.table_id, 1);
    assert_eq!(table.partition_version, 23);
    assert_eq!(table.shard_count, 2);
    assert_eq!(topology.shards.len(), 2);
    assert_eq!(topology.shards[0].shard_id, first);
    assert_eq!(
        topology.shards[0].primary.as_deref(),
        Some("127.0.0.1:27111")
    );
    assert!(topology.shards[0]
        .replicas
        .contains(&"127.0.0.1:27112".to_string()));
    assert_eq!(topology.shards[1].shard_id, second);
    assert_eq!(topology.shards[1].start_bucket, 536_870_912);
    assert_eq!(topology.shards[1].end_bucket, 1_073_741_823);

    meta.server_heartbeat(ServerHeartbeatRequest {
        server_addr: "127.0.0.1:27111".to_string(),
        boot_time_ms: 1,
        binary_version: "unified-meta".to_string(),
        shard_loads: Vec::new(),
        shard_stat_loads: Vec::new(),
        runtime_load: ServerRuntimeLoad::default(),
        shard_states: vec![ServerShardServingState {
            shard_id: first,
            load_version: 9,
            serving_state: "serving".to_string(),
            loaded: true,
            ..ServerShardServingState::default()
        }],
    });
    let stale = meta.finish_load(LoadFinishRequest {
        server_addr: "127.0.0.1:27111".to_string(),
        shard_id: first,
        load_version: 8,
        status: Status::ok(),
        scheduler_task_id: Some(9001),
        scheduler_generation: Some(41),
    });
    assert_eq!(stale.status.code, "stale_load_version");

    let frozen = meta.freeze_server(temporalstore_rust::StateChangeRequest {
        endpoint: "127.0.0.1:27113".to_string(),
        freeze_cooldown_ms: 30_000,
        // The wire default, which is what this case carried before the field existed --
        // naming it keeps the behaviour rather than reclassifying it as an operator freeze.
        reason: Default::default(),
    });
    assert!(frozen.status.ok, "{frozen:?}");
    let safe_mode = meta.safe_mode_report();
    assert!(safe_mode.status.ok);
    assert!(safe_mode
        .blocked_servers
        .contains(&"127.0.0.1:27113".to_string()));

    let cluster = RaftCluster::new_single_shard(first, [1]);
    let servers = meta.list_servers().servers;
    let membership = apply_data_raft_membership_from_topology(&cluster, &topology, &servers, first)
        .expect("metaserver topology should drive data-node Raft membership");
    assert!(membership.applied);
    assert_eq!(membership.plan.shard_id, first);
    assert_eq!(membership.plan.target_servers[0], "127.0.0.1:27111");
    assert!(membership.plan.target_voters.contains(&1));
    assert!(membership.plan.target_voters.contains(&2));
    assert!(membership.membership_report.is_some());

    let scheduler = metaserver_scheduler_execution_readiness_report();
    assert!(scheduler.repair_task_coverage_ready);
    assert!(scheduler.missing_primary_repair_ready);
    assert!(scheduler.under_replicated_repair_ready);
    assert!(scheduler.stale_dead_server_repair_ready);
    assert!(scheduler.load_reload_unload_ready);
    assert!(scheduler.scheduler_task_replay_ready);
    assert!(scheduler.stale_scheduler_token_rejection_ready);
    assert!(scheduler.cooldown_and_safe_mode_ready);

    let raft = temporalstore_rust::distributed_raft_readiness();
    assert_eq!(
        raft.mode,
        temporalstore_rust::RaftDeploymentMode::ProductionDistributed
    );
    assert!(raft.temporal_raft_metaserver_process_startup_present);
    assert!(raft.metaserver_membership_workflow_present);
    assert!(raft.metaserver_driven_membership_present);
}

fn client_metasync_table_report(
    client: &TemporalStoreClient,
    table_name: &str,
) -> temporalstore_rust::client::ClientMetaSyncTableReport {
    client
        .meta_sync_report()
        .tables
        .into_iter()
        .find(|table| table.table == table_name)
        .unwrap_or_else(|| panic!("missing MetaSync table report for {table_name}"))
}

fn execute_raft_membership_shared_case(case: &UnifiedCase) {
    let tmp = tempfile::tempdir().unwrap();
    let mut cluster: Option<RaftCluster> = None;
    for step in &case.steps {
        assert_eq!(
            command_kind(&step.command),
            "raft_membership_op",
            "case={} step={} must be executable raft_membership_op",
            case.name,
            step.name
        );
        match json_string(&step.command, "op").as_str() {
            "setup_cluster" => {
                cluster = Some(RaftCluster::new_single_shard(
                    case.shard_id,
                    json_u64_list(&step.command, "nodes"),
                ));
            }
            "setup_wal_cluster" => {
                cluster = Some(
                    RaftCluster::new_single_shard_with_wal(
                        tmp.path(),
                        case.shard_id,
                        json_u64_list(&step.command, "nodes"),
                        RaftConfig::default(),
                    )
                    .unwrap(),
                );
            }
            "restore_wal_cluster" => {
                cluster = Some(
                    RaftCluster::restore_single_shard_from_wal(
                        tmp.path(),
                        case.shard_id,
                        json_u64_list(&step.command, "nodes"),
                        RaftConfig::default(),
                    )
                    .unwrap(),
                );
            }
            "add_replica" => {
                let cluster = cluster.as_ref().unwrap();
                let node_id = json_u64(&step.command, "node_id");
                if step
                    .command
                    .get("auto_promote")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    cluster
                        .add_learner_with_auto_promote(node_id, true)
                        .unwrap();
                } else {
                    cluster
                        .add_node_with_role(node_id, json_raft_role(&step.command))
                        .unwrap();
                }
            }
            "assert_cluster_status" => {
                let cluster = cluster.as_ref().unwrap();
                let status = cluster.status();
                if step.command.get("expected_majority").is_some() {
                    assert_eq!(
                        status.majority,
                        json_u64(&step.command, "expected_majority") as usize
                    );
                }
                if step.command.get("expected_live_voters").is_some() {
                    assert_eq!(
                        status.live_voters,
                        json_u64(&step.command, "expected_live_voters") as usize
                    );
                }
                if step.command.get("expected_voters").is_some() {
                    assert_eq!(
                        cluster.membership().voters,
                        json_u64_list(&step.command, "expected_voters")
                    );
                }
            }
            "assert_peer_status" => {
                let cluster = cluster.as_ref().unwrap();
                let node_id = json_u64(&step.command, "node_id");
                let local = cluster.local_status(node_id).unwrap();
                assert_eq!(local.replica_role, json_raft_role(&step.command));
                let report = cluster.matrixraft_local_status_report();
                let peer = report
                    .peers
                    .iter()
                    .find(|peer| peer.status.node_id == node_id)
                    .unwrap_or_else(|| panic!("peer {node_id} missing in local status report"));
                assert_eq!(
                    peer.participates_in_quorum,
                    json_bool(&step.command, "participates_in_quorum")
                );
                assert_eq!(
                    peer.can_serve_data,
                    json_bool(&step.command, "can_serve_data")
                );
                assert_eq!(
                    peer.can_be_leader,
                    json_bool(&step.command, "can_be_leader")
                );
                if step.command.get("auto_promoted_from_learner").is_some() {
                    assert_eq!(
                        peer.pipeline_state.auto_promoted_from_learner,
                        json_bool(&step.command, "auto_promoted_from_learner")
                    );
                }
            }
            "assert_local_status_report" => {
                let cluster = cluster.as_ref().unwrap();
                let report = cluster.matrixraft_local_status_report();
                if step.command.get("expect_witness").is_some() {
                    assert_eq!(
                        report.witness_membership_present,
                        json_bool(&step.command, "expect_witness")
                    );
                }
                if step.command.get("expect_learner").is_some() {
                    assert_eq!(
                        report.learner_membership_present,
                        json_bool(&step.command, "expect_learner")
                    );
                }
                if step.command.get("expect_pending_joint").is_some() {
                    assert_eq!(
                        report.pending_joint_consensus.is_some(),
                        json_bool(&step.command, "expect_pending_joint")
                    );
                }
                if step.command.get("new_voter").is_some() {
                    let new_voter = json_u64(&step.command, "new_voter");
                    assert!(report
                        .pending_joint_consensus
                        .as_ref()
                        .unwrap()
                        .new_voters
                        .contains(&new_voter));
                }
            }
            "assert_runtime_admin_report" => {
                let cluster = cluster.as_ref().unwrap();
                let report = cluster.matrixraft_runtime_admin_report();
                if step.command.get("expect_witness").is_some() {
                    assert_eq!(
                        report.witness_membership_present,
                        json_bool(&step.command, "expect_witness")
                    );
                }
                if step.command.get("expect_auto_promote").is_some() {
                    assert_eq!(
                        report.learner_auto_promote_present,
                        json_bool(&step.command, "expect_auto_promote")
                    );
                }
                if step.command.get("expect_pending_joint").is_some() {
                    assert_eq!(
                        report.pending_joint_consensus_present,
                        json_bool(&step.command, "expect_pending_joint")
                    );
                }
            }
            "propose_string_set" => {
                let cluster = cluster.as_ref().unwrap();
                cluster
                    .propose(Command::StringSet {
                        key: json_string(&step.command, "key"),
                        value: json_string(&step.command, "value").into_bytes(),
                    })
                    .unwrap();
            }
            "read_from_replica" => {
                let cluster = cluster.as_ref().unwrap();
                let response = cluster.read_from_replica(
                    json_u64(&step.command, "node_id"),
                    Command::StringGet {
                        key: json_string(&step.command, "key"),
                    },
                );
                if let Some(value) = step.command.get("expected_value").and_then(Value::as_str) {
                    assert_eq!(
                        response,
                        Ok(CommandResponse::Bytes {
                            value: Some(value.as_bytes().to_vec())
                        })
                    );
                } else {
                    assert_shared_raft_error(response, &step.command);
                }
            }
            "elect_leader" => {
                let cluster = cluster.as_ref().unwrap();
                assert_shared_raft_error(
                    cluster.elect_leader(json_u64(&step.command, "node_id")),
                    &step.command,
                );
            }
            "begin_joint_consensus" => {
                let cluster = cluster.as_ref().unwrap();
                cluster
                    .begin_joint_consensus(json_u64_list(&step.command, "new_voters"))
                    .unwrap();
            }
            "commit_joint_consensus" => {
                cluster.as_ref().unwrap().commit_joint_consensus().unwrap();
            }
            op => panic!("unsupported executable shared Raft membership op {op}"),
        }
    }
}

fn verify_raft_openraft_process_path_default_gate() {
    let readiness = temporalstore_rust::distributed_raft_readiness();
    assert_eq!(
        readiness.mode,
        temporalstore_rust::RaftDeploymentMode::ProductionDistributed
    );
    assert!(readiness.temporal_raft_data_node_process_startup_present);
    assert!(readiness.temporal_raft_metaserver_process_startup_present);
    assert!(readiness.durable_apply_index_snapshot_integrated);
    assert!(readiness.metaserver_membership_workflow_present);
    assert!(readiness.metaserver_driven_membership_present);
    assert!(
        !readiness.complete,
        "distributed Raft readiness must not pass without multi-process evidence"
    );
    assert!(readiness.missing.iter().any(|item| {
        item.contains("data-node multi-process rollout evidence")
            || item.contains("metaserver multi-process rollout evidence")
    }));

    let local_mode = temporalstore_rust::validate_raft_deployment_mode(
        temporalstore_rust::RaftDeploymentMode::LocalModel,
    )
    .unwrap_err();
    assert_eq!(
        local_mode.mode,
        temporalstore_rust::RaftDeploymentMode::LocalModel
    );
    assert!(local_mode
        .message
        .contains("local Raft deployment mode is disabled"));
    let raft_tmp = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        raft_tmp.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "shared-matrixraft-admin".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();
    let report = cluster.matrixraft_runtime_admin_report();
    assert!(report.read_index_validated);
    assert!(report.lease_read_validated);
    assert!(report.reorder_queue_enabled);
    assert!(report.wal_segment_lifecycle_present);
    assert!(report.admin_status_surface_complete);
    assert!(report.wal_first_log_index > 0);
    assert!(report.wal_last_log_index >= report.commit_index);
    assert!(report.peer_pipeline_states.iter().all(|peer| {
        peer.next_index > 0
            && peer.append_queue_limit > 0
            && peer.inflight_bytes_limit > 0
            && peer.apply_inflight_limit > 0
            && peer.apply_batch_bytes_limit > 0
    }));
    assert!(report
        .capability_matrix
        .iter()
        .any(|item| item.capability == "admin_status_surface" && item.ready));
}

fn json_u64(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("shared command missing u64 field {field}"))
}

fn json_bool(value: &Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(Value::as_bool)
        .unwrap_or_else(|| panic!("shared command missing bool field {field}"))
}

fn json_u64_list(value: &Value, field: &str) -> Vec<u64> {
    value
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("shared command missing u64-array field {field}"))
        .iter()
        .map(|item| {
            item.as_u64()
                .unwrap_or_else(|| panic!("shared u64-array field {field} contains non-u64"))
        })
        .collect()
}

fn json_raft_role(value: &Value) -> RaftReplicaRole {
    match json_string(value, "role").as_str() {
        "voter" => RaftReplicaRole::Voter,
        "learner" => RaftReplicaRole::Learner,
        "witness" => RaftReplicaRole::Witness,
        role => panic!("unknown shared Raft role {role}"),
    }
}

fn assert_shared_raft_error<T: std::fmt::Debug>(actual: Result<T, RaftError>, command: &Value) {
    let expected = json_string(command, "expected_error");
    match (expected.as_str(), actual) {
        ("NodeNotFound", Err(RaftError::NodeNotFound(_))) => {}
        (_, other) => panic!("expected shared Raft error {expected}, got {other:?}"),
    }
}

fn verify_raft_linearizable_hash_failover() {
    let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
    let checker = Arc::new(Mutex::new(SimpleKvChecker::default()));

    for _ in 0..16 {
        let value = checker.lock().unwrap().next_write_value();
        workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "consistent-key".to_string(),
                    field: "field".to_string(),
                    value: value.to_string().into_bytes(),
                },
            })
            .unwrap();
        checker.lock().unwrap().finish_write(value);

        let response = workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashGet {
                    key: "consistent-key".to_string(),
                    field: "field".to_string(),
                },
            })
            .unwrap();
        let CommandResponse::Bytes { value: Some(value) } = response.response else {
            panic!("expected hash value");
        };
        let value = std::str::from_utf8(&value).unwrap().parse::<u64>().unwrap();
        assert!(checker.lock().unwrap().finish_read(value));
    }

    workflow.set_data_node_alive(1, false).unwrap();
    let value = checker.lock().unwrap().next_write_value();
    workflow
        .proxy()
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "consistent-key".to_string(),
                field: "field".to_string(),
                value: value.to_string().into_bytes(),
            },
        })
        .unwrap();
    checker.lock().unwrap().finish_write(value);
    assert_eq!(
        workflow
            .read_data_node(
                2,
                Command::HashGet {
                    key: "consistent-key".to_string(),
                    field: "field".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(value.to_string().into_bytes())
        }
    );
}

#[derive(Default)]
struct SimpleKvChecker {
    version: u64,
    committed: u64,
}

impl SimpleKvChecker {
    fn next_write_value(&mut self) -> u64 {
        self.version += 1;
        self.version
    }

    fn finish_write(&mut self, value: u64) {
        self.committed = self.committed.max(value);
    }

    fn finish_read(&self, value: u64) -> bool {
        value > 0 && value <= self.committed
    }
}

fn maybe_run_storage_parity_command(case: &UnifiedCase, step: &UnifiedStep) -> bool {
    let kind = command_kind(&step.command);
    if !kind.starts_with("storage_") {
        return false;
    }
    // Pointer steps (mode marks them) name a lib test as their runner, whatever their kind.
    if step
        .command
        .get("mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| mode == "rust_executable_native_static")
    {
        return true;
    }
    let command: StorageUnifiedCommand = serde_json::from_value(step.command.clone())
        .unwrap_or_else(|error| {
            panic!(
                "case={} step={} invalid storage command: {error}",
                case.name, step.name
            )
        });
    match kind {
        "storage_dump_load_recovery" => {
            let storage_case = load_storage_migration_case(&command.required_migration_case());
            verify_storage_dump_load_recovery(&storage_case)
        }
        "storage_fault_matrix" => {
            let storage_case = load_storage_migration_case(&command.required_migration_case());
            verify_storage_fault_matrix(&storage_case)
        }
        "storage_follower_safe_gc" => {
            let storage_case = load_storage_migration_case(&command.required_migration_case());
            verify_storage_follower_safe_gc(&storage_case)
        }
        "storage_cache_refill" => {
            let storage_case = load_storage_migration_case(&command.required_migration_case());
            verify_storage_cache_refill(&storage_case)
        }
        "storage_shared_store_replay" => {
            let storage_case = load_storage_migration_case(&command.required_migration_case());
            let mode = command
                .shared_store_mode()
                .unwrap_or(SharedStoreStorageMode::Sync);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("storage replay runtime should start");
            runtime.block_on(verify_storage_shared_store_replay(&storage_case, mode));
        }
        "storage_wal_index_gc_generation_retention" => {
            verify_storage_wal_index_gc_generation_retention(case.shard_id)
        }
        "storage_wal_index_gc_reclaim" => {
            verify_storage_wal_index_gc_generation_retention(case.shard_id)
        }
        "storage_gc_dependency_retention_matrix" => {
            verify_storage_gc_dependency_retention_matrix(case.shard_id)
        }
        "storage_gc_eviction_cold_reads" => {
            verify_storage_gc_dependency_retention_matrix(case.shard_id);
            verify_storage_cold_read_after_eviction(case.shard_id);
        }
        "storage_cache_replacement_policy_soak" => {
            verify_storage_cache_replacement_policy_soak(case.shard_id)
        }
        "storage_cache_replacement_soak" => {
            verify_storage_cache_replacement_policy_soak(case.shard_id)
        }
        "storage_matrixraft_cache_refill_pressure" => {
            verify_storage_cache_refill_pressure(case.shard_id)
        }
        "storage_cache_cold_read_after_eviction" => {
            verify_storage_cold_read_after_eviction(case.shard_id)
        }
        "storage_block_address_fallback" => verify_storage_cold_read_after_eviction(case.shard_id),
        "storage_cold_read_page_address_fallback" => {
            verify_storage_cold_read_after_eviction(case.shard_id)
        }
        "storage_merged_dump_load_policy" => {
            let storage_case = load_storage_migration_case(&command.migration_case_or_default());
            verify_storage_dump_load_recovery(&storage_case);
        }
        "storage_model_aware_block_compaction" => {
            verify_storage_cache_refill_pressure(case.shard_id)
        }
        "storage_manager_background_loop" | "storage_manager_pressure_scale_evidence" => {
            verify_storage_wal_index_gc_generation_retention(case.shard_id);
            verify_storage_cache_replacement_policy_soak(case.shard_id);
        }
        "storage_slot_layout_transitions" | "storage_slot_object_block_index_authority" => {
            let storage_case = load_storage_migration_case(&command.migration_case_or_default());
            verify_storage_recovery_reconciles_bucket_index_to_model_views(&storage_case);
        }
        "storage_recovery_reconciles_slot_index_to_model_views" => {
            let storage_case = load_storage_migration_case(&command.required_migration_case());
            verify_storage_recovery_reconciles_bucket_index_to_model_views(&storage_case);
        }
        "storage_stream_backed_band_runtime" => verify_storage_stream_backed_band_runtime(),
        "storage_stream_partial_band_rebuild" => verify_storage_stream_backed_band_runtime(),
        "storage_stream_manifest_disk_reconciliation" => {
            verify_storage_stream_backed_band_runtime()
        }
        "storage_stream_segment_manifest_rebuild" => verify_storage_stream_backed_band_runtime(),
        "storage_stream_reopen_scan" => verify_storage_stream_reopen_scan(&command),
        other => panic!(
            "case={} step={} unsupported storage command {other}",
            case.name, step.name
        ),
    }
    true
}

impl StorageUnifiedCommand {
    fn required_migration_case(&self) -> String {
        self.migration_case
            .clone()
            .expect("storage migration command should name migration_case")
    }

    fn migration_case_or_default(&self) -> String {
        self.migration_case
            .clone()
            .unwrap_or_else(|| "native_logical_storage_models_packed_timestamped_pages".to_string())
    }

    fn shared_store_mode(&self) -> Option<SharedStoreStorageMode> {
        match self.mode.as_deref() {
            Some("Sync") | Some("sync") => Some(SharedStoreStorageMode::Sync),
            Some("Async") | Some("async") => Some(SharedStoreStorageMode::Async),
            _ => None,
        }
    }
}

fn load_storage_migration_case(case_name: &str) -> StorageMigrationCase {
    let corpus_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("compat/storage_migration_corpus.json");
    let corpus_bytes = fs::read(&corpus_path).expect("storage migration corpus should be readable");
    let corpus: StorageMigrationCorpus =
        serde_json::from_slice(&corpus_bytes).expect("storage migration corpus should deserialize");

    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.name, "temporalstore-storage-migration-corpus");
    assert_eq!(corpus.source_format, "native_exported_logical_artifacts_v1");
    assert_eq!(
        corpus.format_compatibility,
        "migration_only_rust_native_pages"
    );
    corpus
        .cases
        .into_iter()
        .find(|case| case.name == case_name)
        .unwrap_or_else(|| panic!("storage migration case {case_name} should exist"))
}

fn verify_storage_dump_load_recovery(case: &StorageMigrationCase) {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let mut engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);

    execute_storage_steps(&engine, case.shard_id, &case.operations, &case.name);

    let summaries = engine.bucket_storage_summaries(case.shard_id);
    assert!(
        !summaries.is_empty(),
        "case={} should create slot summaries",
        case.name
    );
    assert!(
        summaries
            .iter()
            .any(|summary| summary.dirty_generation > 0 && summary.page_ref_count > 0),
        "case={} should track dirty generations and page refs",
        case.name
    );
    let dirty_buckets = summaries
        .iter()
        .filter(|summary| summary.dirty_generation > 0)
        .map(|summary| summary.routing_bucket)
        .collect::<Vec<_>>();
    let manifest = engine
        .create_bucket_dump_manifest(case.shard_id, dirty_buckets.clone())
        .expect("slot dump manifest should be created");
    assert!(!manifest.checksum.is_empty());
    assert!(!manifest.index_bytes.is_empty());
    assert!(!manifest.bucket_summaries.is_empty());
    assert_clean_storage_recovery(&engine, case.shard_id, &case.name);

    drop(engine);
    engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);
    engine
        .install_bucket_dump_manifest(&manifest)
        .unwrap_or_else(|status| {
            panic!("case={} slot dump install failed: {:?}", case.name, status)
        });
    assert_clean_storage_recovery(&engine, case.shard_id, &case.name);
    execute_storage_steps(&engine, case.shard_id, &case.expected_reads, &case.name);
}

fn verify_storage_recovery_reconciles_bucket_index_to_model_views(case: &StorageMigrationCase) {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);

    execute_storage_steps(&engine, case.shard_id, &case.operations, &case.name);

    drop(engine);
    let recovered = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);
    assert_clean_storage_recovery(&recovered, case.shard_id, &case.name);
    execute_storage_steps(&recovered, case.shard_id, &case.expected_reads, &case.name);
}

fn verify_storage_fault_matrix(case: &StorageMigrationCase) {
    let dir = tempfile::tempdir().unwrap();
    let engine = new_engine(
        dir.path(),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
        case.shard_id,
    );
    execute_storage_steps(&engine, case.shard_id, &case.operations, &case.name);
    let report = engine.bucket_dump_fault_matrix_report(case.shard_id);
    assert!(
        report.production_ready_slice,
        "case={} storage fault matrix failed: {:?}",
        case.name, report.failed_scenarios
    );
    assert!(report.scenario_count >= 5);
    assert_eq!(report.passed_count, report.scenario_count);
    let scenarios = report
        .scenarios
        .iter()
        .map(|scenario| scenario.scenario.as_str())
        .collect::<BTreeSet<_>>();
    for required in [
        "checksum_mismatch",
        "partial_manifest",
        "missing_page_segment",
        "stale_manifest",
        "corrupt_page_segment",
    ] {
        assert!(
            scenarios.contains(required),
            "case={} storage fault matrix missing scenario {required}",
            case.name
        );
    }
}

fn verify_storage_follower_safe_gc(case: &StorageMigrationCase) {
    let dir = tempfile::tempdir().unwrap();
    let engine = new_engine(
        dir.path(),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
        case.shard_id,
    );
    execute_storage_steps(&engine, case.shard_id, &case.operations, &case.name);
    let dirty_buckets = engine
        .bucket_storage_summaries(case.shard_id)
        .into_iter()
        .filter(|summary| summary.dirty_generation > 0)
        .map(|summary| summary.routing_bucket)
        .collect::<Vec<_>>();
    assert!(!dirty_buckets.is_empty());
    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: case.shard_id,
        selected_dump_buckets: dirty_buckets,
        max_dump_buckets_per_round: 64,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: true,
        prune_bucket_dump_manifests: true,
        roll_forward_bucket_dump_installs: true,
        follower_replay_cursors: vec![BucketDumpFollowerReplayCursor {
            follower_id: "unified-storage-lagging-follower".to_string(),
            shard_id: case.shard_id,
            wal_sequence: 0,
            index_log_sequence: 0,
        }],
        page_gc_shared_store_cursors: Vec::new(),
        page_gc_raft_snapshot_refs: Vec::new(),
        page_gc_checkpoint_floor_slab_id: None,
        page_gc_raft_install_floor_slab_id: None,
        page_gc_delayed_destroy_grace_ms: 0,
        invalidate_cache: true,
        warm_cache: true,
    });
    assert!(lifecycle.dump_manifest.is_some());
    assert_eq!(lifecycle.cache_warmup.failed_page_refs, 0);
    assert!(
        lifecycle.manifest_prune_plan.retained_manifest_ids.len()
            >= lifecycle.manifest_prune_plan.prunable_manifest_ids.len(),
        "case={} follower-safe GC should retain at least as many manifests as it prunes",
        case.name
    );
    assert_clean_storage_recovery(&engine, case.shard_id, &case.name);
}

fn verify_storage_wal_index_gc_generation_retention(shard_id: u64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(shard_id);
    engine.execute(ExecuteRequest {
        shard_id,
        command: Command::StringSet {
            key: "unified-wal-gc".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine
        .create_bucket_dump_manifest(shard_id, Vec::new())
        .unwrap();
    engine.execute(ExecuteRequest {
        shard_id,
        command: Command::StringSet {
            key: "unified-wal-gc".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine
        .create_bucket_dump_manifest(shard_id, Vec::new())
        .unwrap();
    assert!(child.wal_sequence > parent.wal_sequence);
    assert!(child.index_log_sequence > parent.index_log_sequence);

    let lagging_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "unified-lagging-follower".to_string(),
        shard_id,
        wal_sequence: parent.wal_sequence,
        index_log_sequence: parent.index_log_sequence,
    };
    let lagging_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "unified-raft-snapshot-lagging".to_string(),
        shard_id,
        last_included_index: 11,
        last_included_term: 2,
        wal_sequence: parent.wal_sequence,
        index_log_sequence: parent.index_log_sequence,
    };
    let blocked = engine.storage_wal_reclaim_plan(
        shard_id,
        vec![lagging_cursor.clone()],
        vec![lagging_snapshot.clone()],
    );
    assert!(!blocked.safe_to_reclaim, "{blocked:?}");
    assert_eq!(blocked.follower_cursor_block_count, 1);
    assert_eq!(blocked.raft_snapshot_block_count, 1);
    assert_eq!(
        blocked.durable_bucket_generation_frontier_wal_sequence,
        child.wal_sequence
    );
    assert_eq!(
        blocked.durable_bucket_generation_frontier_index_log_sequence,
        child.index_log_sequence
    );
    assert_eq!(blocked.retain_from_wal_sequence, 0);
    assert_eq!(blocked.retain_from_index_log_sequence, 0);
    assert!(blocked
        .blocker_reasons
        .contains(&"follower_cursor_retains_logs:unified-lagging-follower".to_string()));
    assert!(blocked
        .blocker_reasons
        .contains(&"raft_snapshot_retains_logs:unified-raft-snapshot-lagging".to_string()));

    let blocked_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id,
        follower_replay_cursors: vec![lagging_cursor],
        raft_snapshot_refs: vec![lagging_snapshot],
        index_gc_index_log_bytes_threshold: 0,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 8,
        min_undumped_wal_records: 0,
        ..StorageManagerCycleRequest::default()
    });
    let blocked_wal = blocked_cycle.wal_reclaim_report.as_ref().unwrap();
    assert!(!blocked_wal.applied, "{blocked_wal:?}");
    assert_eq!(blocked_wal.wal_records_removed, 0);
    let blocked_index_gc = blocked_cycle.index_gc_report.as_ref().unwrap();
    assert!(!blocked_index_gc.applied, "{blocked_index_gc:?}");
    assert_eq!(
        blocked_index_gc.skipped_reason,
        "durable WAL/index frontier not safe"
    );

    let released_anchor = engine
        .create_bucket_dump_manifest(shard_id, Vec::new())
        .unwrap();
    let released_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "unified-follower-caught-up".to_string(),
        shard_id,
        wal_sequence: released_anchor.wal_sequence,
        index_log_sequence: released_anchor.index_log_sequence,
    };
    let released_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "unified-raft-snapshot-caught-up".to_string(),
        shard_id,
        last_included_index: 12,
        last_included_term: 2,
        wal_sequence: released_anchor.wal_sequence,
        index_log_sequence: released_anchor.index_log_sequence,
    };
    let released_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id,
        follower_replay_cursors: vec![released_cursor],
        raft_snapshot_refs: vec![released_snapshot],
        index_gc_index_log_bytes_threshold: 0,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 1,
        min_undumped_wal_records: 0,
        ..StorageManagerCycleRequest::default()
    });
    let released_wal = released_cycle.wal_reclaim_report.as_ref().unwrap();
    assert!(released_wal.plan.safe_to_reclaim, "{released_wal:?}");
    assert!(released_wal.applied, "{released_wal:?}");
    assert!(released_wal.wal_records_removed > 0, "{released_wal:?}");
    let released_index_gc = released_cycle.index_gc_report.as_ref().unwrap();
    assert!(released_index_gc.safe_to_truncate, "{released_index_gc:?}");
    assert!(released_index_gc.applied, "{released_index_gc:?}");
    assert_eq!(released_index_gc.records_removed, 1);
    assert!(released_index_gc.budget_exhausted);

    let restarted = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restart-cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    restarted.load_shard(shard_id);
    let get = restarted.execute(ExecuteRequest {
        shard_id,
        command: Command::StringGet {
            key: "unified-wal-gc".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        }
    );
    let restart_boundary = restarted.storage_recovery_boundary_report(shard_id);
    assert!(restart_boundary.stale_index_page_refs.is_empty());
    assert_eq!(restart_boundary.missing_owner_page_refs, 0);
}

fn verify_storage_gc_dependency_retention_matrix(shard_id: u64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(shard_id);
    engine.execute(ExecuteRequest {
        shard_id,
        command: Command::StringSet {
            key: "unified-page-gc".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(shard_id, Vec::new())
        .unwrap();
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id,
        command: Command::StringSet {
            key: "unified-page-gc".to_string(),
            value: b"v2".to_vec(),
        },
    });
    assert_eq!(engine.live_page_slab_ids(shard_id), vec![1]);
    let delayed = engine
        .block_store()
        .gc_slabs_before_with_live_refs_delayed_destroy(
            1,
            engine.live_page_slab_ids(shard_id),
        )
        .unwrap();
    assert_eq!(delayed.delayed_destroy_page_slab_ids, vec![0]);

    let matrix = engine.storage_page_gc_dependency_plan(
        shard_id,
        vec![0, 1],
        vec![StoragePageGcReplayCursor {
            cursor_id: "unified-shared-store-follower".to_string(),
            shard_id,
            retain_from_page_slab_id: 0,
            reason: "shared-store follower is behind segment zero".to_string(),
        }],
        vec![BucketDumpRaftSnapshotRef {
            snapshot_id: "unified-raft-page-snapshot".to_string(),
            shard_id,
            last_included_index: 7,
            last_included_term: 2,
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: 0,
        }],
        Some(0),
        Some(0),
        60_000,
    );
    assert!(!matrix.safe_to_reclaim, "{matrix:?}");
    assert_eq!(matrix.candidate_page_slab_ids, vec![0, 1]);
    assert_eq!(matrix.live_ref_block_count, 1);
    assert_eq!(matrix.bucket_dump_manifest_block_count, 1);
    assert_eq!(matrix.shared_store_cursor_block_count, 2);
    assert_eq!(matrix.raft_snapshot_ref_block_count, 2);
    assert_eq!(matrix.checkpoint_snapshot_floor_block_count, 2);
    assert_eq!(matrix.raft_snapshot_install_floor_block_count, 2);
    assert_eq!(matrix.delayed_destroy_grace_block_count, 1);
    for expected in [
        "live_page_ref",
        "slot_dump_manifest",
        "shared_store_replay_cursor",
        "raft_snapshot_ref",
        "checkpoint_snapshot_floor",
        "raft_snapshot_install_floor",
        "delayed_destroy_grace_period",
    ] {
        assert!(
            matrix.blocker_reasons.contains(&expected.to_string()),
            "{matrix:?}"
        );
    }
}

fn verify_storage_cache_replacement_policy_soak(shard_id: u64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(shard_id);

    let target_value = b"target-value-0123456789".to_vec();
    for round in 0..4 {
        for item in 0..8 {
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: format!("soak-{round}-{item}"),
                    value: format!("soak-value-{round}-{item}").into_bytes(),
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
    }
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: "soak-target".to_string(),
                    value: target_value.clone(),
                },
            })
            .status
            .ok
    );

    for round in 0..4 {
        for item in 0..8 {
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet {
                    key: format!("soak-{round}-{item}"),
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet {
                    key: "soak-target".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert!(
        engine.cache().stats().memory_evictions > 0,
        "tiny memory cache should perform replacement under pressure"
    );
    assert!(
        engine.cache().stats().disk_bytes > 0,
        "cache pressure should leave disk-tier refill evidence"
    );

    let target_page_key = string_page_cache_key(&engine, shard_id, "soak-target");
    let evict_report = engine.apply_storage_eviction(shard_id, 1, 4, true, false);
    assert_eq!(evict_report.mode, "evict_cache");
    assert!(evict_report.pressure_gate_open, "{evict_report:?}");
    assert!(
        !evict_report.selected_victims.is_empty(),
        "{evict_report:?}"
    );
    assert!(
        !evict_report.dump_manifest_ids.is_empty(),
        "dump-before-evict should durably dump dirty victims: {evict_report:?}"
    );
    assert!(
        evict_report.cache_entries_removed > 0 || evict_report.cache_disk_bytes_removed > 0,
        "eviction should remove memory or disk-cache entries: {evict_report:?}"
    );

    let _ = engine.cache().invalidate(&target_page_key);
    engine.cache().clear_memory_for_test();
    assert_eq!(engine.cache().get_memory(&target_page_key), None);
    let block_reads_before = engine.block_store().stats().reads;
    let disk_hits_before = engine.cache().stats().disk_hits;
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet {
                    key: "soak-target".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        engine.cache().get_memory(&target_page_key),
        Some(target_value)
    );
    assert!(
        engine.cache().stats().disk_hits > disk_hits_before
            || engine.block_store().stats().reads > block_reads_before,
        "cold read should refill from disk cache or persistent block store"
    );

    engine.cache().set_async_writeback_queue_limit_for_test(1);
    engine
        .cache()
        .enqueue_async_writeback(
            CacheKey::page_with_slot(shard_id, 50_000, 0, 8, Some(99)),
            b"writeback".to_vec(),
        )
        .unwrap();
    assert!(engine
        .cache()
        .enqueue_async_writeback(
            CacheKey::page_with_slot(shard_id, 50_001, 0, 8, Some(99)),
            b"overflow".to_vec(),
        )
        .is_err());
    assert_eq!(engine.cache().drain_async_writeback(8).unwrap().drained, 1);
    engine.cache().record_compaction_latency_micros(750);
    let stats = engine.cache().stats();
    assert!(stats.async_writeback_backpressure_rejections > 0);
    assert!(stats.async_writeback_max_queue_depth > 0);
    assert!(stats.async_writeback_max_queue_bytes > 0);
    assert_cache_latency_histograms_observed(stats);

    let drop_dir = tempfile::tempdir().unwrap();
    let drop_engine = TemporalEngine::with_local_dirs(
        64,
        drop_dir.path().join("cache"),
        drop_dir.path().join("pages"),
        drop_dir.path().join("indexes"),
    );
    drop_engine.load_shard(shard_id + 1);
    for item in 0..6 {
        let key = format!("drop-{item}");
        assert!(
            drop_engine
                .execute(ExecuteRequest {
                    shard_id: shard_id + 1,
                    command: Command::StringSet {
                        key: key.clone(),
                        value: format!("drop-value-{item}").repeat(10).into_bytes(),
                    },
                })
                .status
                .ok
        );
        assert!(
            drop_engine
                .execute(ExecuteRequest {
                    shard_id: shard_id + 1,
                    command: Command::StringGet { key },
                })
                .status
                .ok
        );
    }
    let drop_report = drop_engine.apply_storage_eviction(shard_id + 1, 1, 2, true, true);
    assert_eq!(drop_report.mode, "delete_drop");
    assert!(drop_report.pressure_gate_open, "{drop_report:?}");
    assert!(!drop_report.selected_victims.is_empty(), "{drop_report:?}");
    assert!(
        drop_report.dropped_object_count > 0,
        "delete/drop eviction mode should drop selected cold objects: {drop_report:?}"
    );
}

fn verify_storage_cache_refill_pressure(shard_id: u64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(shard_id);
    let target_value = b"target-value-0123456789".to_vec();
    for (key, value) in [
        ("target", target_value.clone()),
        ("evict-a", b"eviction-value-a-0123456789".to_vec()),
        ("evict-b", b"eviction-value-b-0123456789".to_vec()),
    ] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                })
                .status
                .ok
        );
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet {
                    key: "target".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    for key in ["evict-a", "evict-b"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringGet {
                        key: key.to_string(),
                    },
                })
                .status
                .ok
        );
    }
    assert!(
        engine.cache().stats().memory_evictions > 0,
        "tiny memory cache should evict during pressure reads"
    );
    assert!(
        engine.cache().stats().disk_bytes > 0,
        "persistent read path should populate disk cache"
    );
    let target_page_key = string_page_cache_key(&engine, shard_id, "target");
    assert_eq!(engine.cache().get_memory(&target_page_key), None);
    let disk_hits_before = engine.cache().stats().disk_hits;
    let block_reads_before = engine.block_store().stats().reads;
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet {
                    key: "target".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert!(
        engine.cache().stats().disk_hits > disk_hits_before
            || engine.block_store().stats().reads > block_reads_before,
        "cold target read should use disk cache or block store"
    );
    assert_eq!(
        engine.cache().get_memory(&target_page_key),
        Some(target_value)
    );
}

fn verify_storage_cold_read_after_eviction(shard_id: u64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(shard_id);
    assert!(
        engine
            .execute_durable(ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: "cold-slot-read".to_string(),
                    value: b"from-disk".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(engine.clear_string_model_view_for_test(shard_id, "cold-slot-read"));
    let _ = engine
        .cache()
        .invalidate(&CacheKey::string(shard_id, "cold-slot-read"));
    engine.cache().clear_memory_for_test();
    let block_reads_before = engine.block_store().stats().reads;
    let get = engine.execute(ExecuteRequest {
        shard_id,
        command: Command::StringGet {
            key: "cold-slot-read".to_string(),
        },
    });
    assert!(get.status.ok);
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"from-disk".to_vec())
        }
    );
    assert!(engine.block_store().stats().reads > block_reads_before);
}

fn string_page_cache_key(engine: &TemporalEngine, shard_id: u64, key: &str) -> CacheKey {
    engine
        .string_page_cache_key_for_test(shard_id, key)
        .unwrap_or_else(|| panic!("key {key} should have a page address"))
}

fn assert_cache_latency_histograms_observed(stats: matrixcache::CacheStats) {
    assert_eq!(
        stats.refill_latency_samples,
        stats.refill_latency_le_10us
            + stats.refill_latency_le_100us
            + stats.refill_latency_le_1ms
            + stats.refill_latency_le_10ms
            + stats.refill_latency_gt_10ms
    );
    assert!(stats.writeback_latency_samples > 0);
    assert_eq!(
        stats.writeback_latency_samples,
        stats.writeback_latency_le_10us
            + stats.writeback_latency_le_100us
            + stats.writeback_latency_le_1ms
            + stats.writeback_latency_le_10ms
            + stats.writeback_latency_gt_10ms
    );
    assert!(stats.eviction_latency_samples > 0);
    assert_eq!(
        stats.eviction_latency_samples,
        stats.eviction_latency_le_10us
            + stats.eviction_latency_le_100us
            + stats.eviction_latency_le_1ms
            + stats.eviction_latency_le_10ms
            + stats.eviction_latency_gt_10ms
    );
    assert!(stats.compaction_latency_samples > 0);
    assert_eq!(
        stats.compaction_latency_samples,
        stats.compaction_latency_le_10us
            + stats.compaction_latency_le_100us
            + stats.compaction_latency_le_1ms
            + stats.compaction_latency_le_10ms
            + stats.compaction_latency_gt_10ms
    );
}

fn verify_storage_cache_refill(case: &StorageMigrationCase) {
    let dir = tempfile::tempdir().unwrap();
    let engine = new_engine(
        dir.path(),
        &dir.path().join("pages"),
        &dir.path().join("indexes"),
        case.shard_id,
    );
    execute_storage_steps(&engine, case.shard_id, &case.operations, &case.name);
    engine.cache().invalidate_shard(case.shard_id).unwrap();
    let before = engine.storage_cache_inspection_report(case.shard_id);
    assert_eq!(before.entries.len(), 0);
    let selected_buckets = engine
        .bucket_storage_summaries(case.shard_id)
        .into_iter()
        .filter(|summary| summary.page_ref_count > 0)
        .map(|summary| summary.routing_bucket)
        .collect::<Vec<_>>();
    let report = engine.storage_cache_warmup_report(case.shard_id, selected_buckets);
    assert!(report.considered_page_refs > 0);
    assert!(report.page_store_reads > 0);
    assert_eq!(report.failed_page_refs, 0);
    assert_eq!(report.warmed_page_refs, report.considered_page_refs);
    let after = engine.storage_cache_inspection_report(case.shard_id);
    assert!(!after.entries.is_empty());
    assert!(!after.bucket_summaries.is_empty());
}

async fn verify_storage_shared_store_replay(
    case: &StorageMigrationCase,
    mode: SharedStoreStorageMode,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileObjectStore::new(
        dir.path().join(format!("objects-{mode:?}")),
    ));
    let replicator = SharedStoreReplicator::new("unified-storage-corpus", store);
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
                .expect("async shared-store replay flush should succeed");
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
        .replay_wal_strict(case.shard_id, 0, &follower)
        .await
        .expect("shared-store replay should succeed");
    assert_eq!(
        replay.applied,
        case.operations
            .iter()
            .filter(|step| step.storage_mutation)
            .count()
    );
    execute_storage_steps(&follower, case.shard_id, &case.expected_reads, &case.name);
    assert_clean_storage_recovery(&follower, case.shard_id, &case.name);
}

fn verify_storage_stream_reopen_scan(command: &StorageUnifiedCommand) {
    match command.scenario.as_deref() {
        Some("random_size_reopen_scan") => verify_random_size_reopen_scan(),
        Some("cross_block_large_values") => verify_cross_block_large_values(),
        other => panic!("unsupported storage stream scenario {other:?}"),
    }
}

fn verify_storage_stream_backed_band_runtime() {
    verify_random_size_reopen_scan();
    verify_cross_block_large_values();
}

fn verify_random_size_reopen_scan() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = new_engine(dir.path(), &page_dir, &index_dir, 1);

    let mut expected = Vec::new();
    for i in 0..24usize {
        let len = 1 + ((i * 7919) % 32_768);
        let value = deterministic_bytes(i as u64, len);
        let key = format!("stream-random-{i:03}");
        execute_storage_steps(
            &engine,
            1,
            &[StorageMigrationStep {
                name: format!("set-{i}"),
                storage_mutation: true,
                command: Command::StringSet {
                    key: key.clone(),
                    value: value.clone(),
                },
                expect: Some(CommandResponse::Empty),
            }],
            "storage_stream_random_size_reopen_scan",
        );
        expected.push((key, value));
    }

    drop(engine);
    let reopened = new_engine(dir.path(), &page_dir, &index_dir, 1);
    for (key, value) in &expected {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key.clone() },
        });
        assert!(response.status.ok, "{}", response.status.message);
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(value.clone())
            }
        );
    }

    let page = reopened.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_slab_id: 0,
        offset: 0,
        size: 1024 * 1024,
    });
    assert_stream_ok(&page, "random_size_reopen_scan/page");
    assert!(!page.data.is_empty());
    for (_, value) in expected.iter().take(8) {
        assert!(
            page.data.windows(value.len()).any(|window| window == value),
            "page stream should contain deterministic value of len {}",
            value.len()
        );
    }

    let scan = reopened.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_slab_id: 0,
        start_offset: 0,
        end_offset: u64::MAX,
        max_bytes: 1024 * 1024,
    });
    assert!(scan.status.ok, "{}", scan.status.message);
    assert!(!scan.records.is_empty());
    assert!(scan.end_of_stream);
}

fn verify_cross_block_large_values() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);

    let large = deterministic_bytes(42, 512 * 1024);
    for i in 0..3 {
        execute_storage_steps(
            &engine,
            1,
            &[StorageMigrationStep {
                name: format!("set-large-{i}"),
                storage_mutation: true,
                command: Command::StringSet {
                    key: format!("stream-cross-block-{i}"),
                    value: large.clone(),
                },
                expect: Some(CommandResponse::Empty),
            }],
            "storage_stream_cross_block_large_values",
        );
    }

    drop(engine);
    let reopened = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    reopened.load_shard(1);
    for i in 0..3 {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("stream-cross-block-{i}"),
            },
        });
        assert!(response.status.ok, "{}", response.status.message);
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(large.clone())
            }
        );
    }

    let first_chunk = reopened.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_slab_id: 0,
        offset: 0,
        size: 256 * 1024,
    });
    assert_stream_ok(&first_chunk, "cross_block_large_values/first");
    assert_eq!(first_chunk.data, large[..256 * 1024].to_vec());

    let second_chunk = reopened.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_slab_id: 0,
        offset: 256 * 1024,
        size: 256 * 1024,
    });
    assert_stream_ok(&second_chunk, "cross_block_large_values/second");
    assert_eq!(second_chunk.data, large[256 * 1024..].to_vec());
}

fn assert_stream_ok(response: &StreamReadResponse, label: &str) {
    assert!(
        response.status.ok,
        "{label} stream read failed: {}",
        response.status.message
    );
}

fn deterministic_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed ^ 0x9e37_79b9_7f4a_7c15;
    (0..len)
        .map(|_| {
            x ^= x << 7;
            x ^= x >> 9;
            x ^= x << 8;
            (x & 0xff) as u8
        })
        .collect()
}

fn execute_storage_steps(
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

fn assert_clean_storage_recovery(engine: &TemporalEngine, shard_id: u64, case_name: &str) {
    let recovery = engine.storage_recovery_report(shard_id);
    assert!(
        recovery.all_live_pages_readable,
        "case={} live pages should be readable: {:?}",
        case_name, recovery.unreadable_page_refs
    );
    assert!(
        recovery.slab_integrity.integrity_ok,
        "case={} segment integrity failed: {:?}",
        case_name, recovery.slab_integrity
    );
    assert_eq!(recovery.slab_integrity.stale_page_ref_count, 0);
    assert_eq!(recovery.slab_integrity.corrupt_page_slab_count, 0);
    assert_eq!(recovery.slab_integrity.unreadable_page_ref_count, 0);
    assert_eq!(
        recovery
            .feature_page_layout
            .missing_indexed_timestamps
            .len(),
        0,
        "case={} missing indexed timestamps: {:?}",
        case_name,
        recovery.feature_page_layout.missing_indexed_timestamps
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

fn assert_step_status(case: &UnifiedCase, step: &UnifiedStep, actual: &Status) {
    if let Some(expected) = &step.expect_status {
        assert_eq!(
            actual, expected,
            "case={} step={} status mismatch",
            case.name, step.name
        );
    } else if step
        .expect
        .as_ref()
        .and_then(expected_status_code)
        .is_some()
    {
        // The expected payload can carry a status contract that maps onto
        // a Rust response value. assert_step_expect performs that translation.
    } else {
        assert!(
            actual.ok,
            "case={} step={} failed status={:?}",
            case.name, step.name, actual
        );
    }
}

fn assert_step_expect(
    case: &UnifiedCase,
    step: &UnifiedStep,
    actual_status: &Status,
    actual_response: &CommandResponse,
) {
    match &step.expect {
        Some(UnifiedExpected::Status(expected)) => {
            assert_eq!(
                expected.kind, "status",
                "case={} step={} unsupported expected status kind",
                case.name, step.name
            );
            if let Some(ok) = expected.ok {
                assert_eq!(
                    actual_status.ok, ok,
                    "case={} step={} status ok mismatch",
                    case.name, step.name
                );
                return;
            }
            let Some(expected_status) = expected.status.as_deref() else {
                panic!(
                    "case={} step={} expected status must provide either ok or status",
                    case.name, step.name
                );
            };
            if actual_status.code == expected_status {
                return;
            }
            let rust_conditional_rejection = expected_status == "already_exists"
                && matches!(
                    actual_response,
                    CommandResponse::Integer { value: 0 } | CommandResponse::Bytes { value: None }
                );
            let rust_missing_value = expected_status == "not_found"
                && matches!(actual_response, CommandResponse::Bytes { value: None })
                || expected_status == "not_found"
                    && matches!(
                        actual_response,
                        CommandResponse::Values { values } if values.iter().all(Option::is_none)
                    )
                || expected_status == "not_found"
                    && matches!(
                        actual_response,
                        CommandResponse::HashEntries { entries } if entries.is_empty()
                    )
                || expected_status == "not_found"
                    && matches!(
                        actual_response,
                        CommandResponse::Members { members } if members.is_empty()
                    )
                || expected_status == "not_found"
                    && matches!(actual_response, CommandResponse::Integer { value: 0 });
            assert!(
                rust_conditional_rejection || rust_missing_value,
                "case={} step={} status mismatch actual={:?} expected={}",
                case.name,
                step.name,
                actual_status,
                expected_status
            );
        }
        Some(UnifiedExpected::Response(expected)) => assert_eq!(
            actual_response, expected,
            "case={} step={} response mismatch",
            case.name, step.name
        ),
        Some(UnifiedExpected::Bool { value }) => assert_eq!(
            actual_response,
            &CommandResponse::Integer {
                value: i64::from(*value)
            },
            "case={} step={} boolean response mismatch",
            case.name,
            step.name
        ),
        Some(UnifiedExpected::NodesProbe(probe)) => {
            let CommandResponse::ContextNodes { nodes } = actual_response else {
                panic!(
                    "case={} step={} expected ContextNodes, got {:?}",
                    case.name, step.name, actual_response
                );
            };
            let returned: std::collections::BTreeSet<u64> =
                nodes.iter().map(|node| node.node_hash).collect();
            for node_hash in &probe.node_hashes {
                assert!(
                    returned.contains(node_hash),
                    "case={} step={} node {node_hash} missing from ContextNodes",
                    case.name,
                    step.name
                );
            }
            for node_hash in &probe.missing_node_hashes {
                assert!(
                    !returned.contains(node_hash),
                    "case={} step={} node {node_hash} must NOT be returned",
                    case.name,
                    step.name
                );
            }
            assert!(
                nodes.len() >= probe.min_node_count,
                "case={} step={} expected at least {} nodes, got {}",
                case.name,
                step.name,
                probe.min_node_count,
                nodes.len()
            );
        }
        Some(UnifiedExpected::EventsProbe { min_count }) => {
            let CommandResponse::ContextEvents { events, .. } = actual_response else {
                panic!(
                    "case={} step={} expected ContextEvents, got {:?}",
                    case.name, step.name, actual_response
                );
            };
            assert!(
                events.len() >= *min_count,
                "case={} step={} expected at least {min_count} events, got {}",
                case.name,
                step.name,
                events.len()
            );
        }
        Some(UnifiedExpected::CompressionProbe {
            min_count,
            source_event_count,
        }) => {
            let CommandResponse::ContextCompressionEvents { events, .. } = actual_response else {
                panic!(
                    "case={} step={} expected ContextCompressionEvents, got {:?}",
                    case.name, step.name, actual_response
                );
            };
            assert!(
                events.len() >= *min_count,
                "case={} step={} expected at least {min_count} compression events, got {}",
                case.name,
                step.name,
                events.len()
            );
            // The record does not carry a source count; what proves sources were consumed is
            // the summary extending past its bare window prefix -- each selected event appends
            // its snippet there.
            if *source_event_count > 0 {
                assert!(
                    events.iter().any(|event| {
                        let prefix = format!(
                            "Temporal compression window {}-{}:",
                            event.source_start_ms, event.source_end_ms
                        );
                        event.summary.len() > prefix.len()
                    }),
                    "case={} step={} no compression event carries source snippets",
                    case.name,
                    step.name
                );
            }
        }
        Some(UnifiedExpected::Static(_)) => {}
        None => {}
    }
}

fn expected_status_code(expected: &UnifiedExpected) -> Option<&str> {
    match expected {
        UnifiedExpected::Status(status) if status.kind == "status" => status.status.as_deref(),
        UnifiedExpected::Status(_)
        | UnifiedExpected::Bool { .. }
        | UnifiedExpected::Static(_)
        | UnifiedExpected::Response(_)
        | UnifiedExpected::NodesProbe(_)
        | UnifiedExpected::EventsProbe { .. }
        | UnifiedExpected::CompressionProbe { .. } => None,
    }
}

fn new_engine(root: &Path, page_dir: &Path, index_dir: &Path, shard_id: u64) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        root.join(format!("cache-{shard_id}")),
        page_dir,
        index_dir,
    );
    engine.load_shard(shard_id);
    engine
}

fn start_temporal_engine_http_service(addr: String, engine: TemporalEngine) {
    std::thread::spawn(move || {
        serve(&addr, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                    json_response(200, &engine.execute(req))
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
}

fn start_single_node_meta_http_service(addr: String, meta: SingleNodeMeta) {
    std::thread::spawn(move || {
        serve(&addr, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("GET", path) if path.starts_with("/shards/") => {
                    let shard_id = path
                        .trim_start_matches("/shards/")
                        .parse()
                        .unwrap_or_default();
                    json_response(200, &meta.get(shard_id))
                }
                ("POST", "/tables/topology") => {
                    let req = parse_json::<GetTableTopologyRequest>(&request.body).unwrap();
                    json_response(200, &meta.get_table_topology(req))
                }
                ("POST", "/meta/topology_version") => {
                    let req = parse_json::<TopologyVersionRequest>(&request.body).unwrap();
                    json_response(200, &meta.topology_version_report(req))
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
}

fn start_client_placement_endpoint(
    addr: String,
    writes: Arc<std::sync::atomic::AtomicUsize>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
    read_value: Vec<u8>,
    accepts_writes: bool,
) {
    std::thread::spawn(move || {
        serve(&addr, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                    match req.command {
                        Command::StringSet { .. } if accepts_writes => {
                            writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                },
                            )
                        }
                        Command::StringSet { .. } => {
                            writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::error(
                                        "wrong_endpoint",
                                        "replica received primary-only write",
                                    ),
                                    response: CommandResponse::Empty,
                                },
                            )
                        }
                        Command::StringGet { .. } => {
                            reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            json_response(
                                200,
                                &ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Bytes {
                                        value: Some(read_value.clone()),
                                    },
                                },
                            )
                        }
                        _ => json_response(400, &Status::error("bad_request", "unexpected")),
                    }
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
}

fn free_local_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn wait_for_http(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server {addr} did not start");
}
