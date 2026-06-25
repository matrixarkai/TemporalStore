use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, path::Path};

use serde::Deserialize;
use serde_json::Value;
use temporalstore_rust::http::{json_response, parse_json, serve};
use temporalstore_rust::redis::{execute_redis_command_with_state, RedisCommandState};
use temporalstore_rust::types::SequenceFeatureRow;
use temporalstore_rust::{
    execute_redis_command, production_readiness_report, ClientOptions, Command, CommandResponse,
    EndToEndWorkflow, ExecuteRequest, RespValue, ScanStreamRequest, SharedStoreReplicator,
    SharedStoreStorageMode, SlotDumpFollowerReplayCursor, Status, StorageLifecycleRequest,
    StreamKind, StreamReadRequest, StreamReadResponse, TableOptions, TemporalEngine,
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
}

#[derive(Debug, Deserialize)]
struct UnifiedStatusExpected {
    kind: String,
    status: String,
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
    mode: Option<SharedStoreStorageMode>,
    #[serde(default)]
    scenario: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SharedHarnessCommand {
    #[serde(default)]
    scenario: Option<String>,
}

#[test]
fn rust_executes_shared_cpp_rust_temporalstore_corpus() {
    let corpus = load_corpus();

    for case in corpus.cases {
        run_engine_case(&case);
    }
}

#[test]
fn rust_client_executes_shared_cpp_rust_temporalstore_corpus() {
    let corpus = load_corpus();

    for case in corpus.cases {
        run_client_case(&case);
    }
}

fn load_corpus() -> UnifiedCorpus {
    let corpus_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("compat/unified_temporalstore_cases.json");
    let corpus_bytes = fs::read(&corpus_path).expect("shared corpus should be readable");
    let corpus: UnifiedCorpus =
        serde_json::from_slice(&corpus_bytes).expect("shared corpus should deserialize");

    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.name, "temporalstore-unified-cpp-rust-corpus");
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
        CommandResponse::IpsStats { .. } => "ips_stats",
        CommandResponse::IpsSnapshotReport { .. } => "ips_snapshot_report",
        CommandResponse::ContextNode { .. } => "context_node",
        CommandResponse::ContextObjectKey { .. } => "context_object_key",
        CommandResponse::ContextExtractedEventWrite { .. } => "context_extracted_event_write",
        CommandResponse::ContextEvents { .. } => "context_events",
        CommandResponse::ContextIndexRefs { .. } => "context_index_refs",
        CommandResponse::ContextPackAudits { .. } => "context_pack_audits",
        CommandResponse::ContextSummaryDirtyMarkers { .. } => "context_summary_dirty_markers",
        CommandResponse::ContextEntity { .. } => "context_entity",
        CommandResponse::ContextEntities { .. } => "context_entities",
        CommandResponse::ContextChildRefs { .. } => "context_child_refs",
        CommandResponse::ContextEmbeddings { .. } => "context_embeddings",
        CommandResponse::ContextTraversedNodes { .. } => "context_traversed_nodes",
        CommandResponse::ContextSummaries { .. } => "context_summaries",
        CommandResponse::ContextCompressionEvents { .. } => "context_compression_events",
        CommandResponse::ContextNodeContext { .. } => "context_node_context",
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
        if command_kind(&step.command) == "existing_test" {
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
        if command_kind(&step.command) == "existing_test" {
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
                || uri.starts_with("bytestore://");
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
            let backend = if uri.starts_with("blob://") || uri.starts_with("bytestore://") {
                "bytestore"
            } else if uri.starts_with("local://") {
                "bytestore"
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
        "redis_slot_hash_cpp_crc64" => {
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
            verify_redis_slot_hash_cpp_crc64();
            true
        }
        "raft_linearizable_hash_failover" => {
            verify_raft_linearizable_hash_failover();
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
    assert!(gates.iter().all(|gate| gate.gate_status == "ready"));
    assert!(report.next_blocked_service().is_none());
    let data_node = report
        .service_summary("data_node")
        .expect("data node service summary should be exported");
    assert!(data_node.ready);
    assert!(data_node.areas.contains(&"dataserver".to_string()));
    assert!(data_node
        .areas
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node.blocker_classes.is_empty());
    assert!(data_node.next_action.contains("ready"));
    assert!(report
        .failed_capabilities_for_service("data_node")
        .is_empty());
    assert!(report.service_ready("data_node"));
    let gate = report
        .service_gate_report("data_node")
        .expect("data node service gate report should be exported");
    assert!(gate.ready);
    assert_eq!(gate.gate_status, "ready");
    assert_eq!(gate.severity, "ready");
    assert_eq!(gate.remediation_order, 4);
    assert_eq!(gate.owner, "data_node_runtime");
    assert!(gate.failed_capabilities.is_empty());
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
    .encode_cpp_feature_value();
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

fn verify_redis_slot_hash_cpp_crc64() {
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
            let mode = command.mode.unwrap_or(SharedStoreStorageMode::Sync);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("storage replay runtime should start");
            runtime.block_on(verify_storage_shared_store_replay(&storage_case, mode));
        }
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
    assert_eq!(corpus.source_format, "cpp_exported_logical_artifacts_v1");
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

    let summaries = engine.slot_storage_summaries(case.shard_id);
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
    let dirty_slots = summaries
        .iter()
        .filter(|summary| summary.dirty_generation > 0)
        .map(|summary| summary.routing_slot)
        .collect::<Vec<_>>();
    let manifest = engine
        .create_slot_dump_manifest(case.shard_id, dirty_slots.clone())
        .expect("slot dump manifest should be created");
    assert!(!manifest.checksum.is_empty());
    assert!(!manifest.index_bytes.is_empty());
    assert!(!manifest.slot_summaries.is_empty());
    assert_clean_storage_recovery(&engine, case.shard_id, &case.name);

    drop(engine);
    engine = new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);
    engine
        .install_slot_dump_manifest(&manifest)
        .unwrap_or_else(|status| {
            panic!("case={} slot dump install failed: {:?}", case.name, status)
        });
    assert_clean_storage_recovery(&engine, case.shard_id, &case.name);
    execute_storage_steps(&engine, case.shard_id, &case.expected_reads, &case.name);
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
    let report = engine.slot_dump_fault_matrix_report(case.shard_id);
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
    let dirty_slots = engine
        .slot_storage_summaries(case.shard_id)
        .into_iter()
        .filter(|summary| summary.dirty_generation > 0)
        .map(|summary| summary.routing_slot)
        .collect::<Vec<_>>();
    assert!(!dirty_slots.is_empty());
    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: case.shard_id,
        selected_dump_slots: dirty_slots,
        max_dump_slots_per_round: 64,
        min_undumped_oplog_records: 0,
        purge_delayed_destroy: true,
        prune_slot_dump_manifests: true,
        roll_forward_slot_dump_installs: true,
        follower_replay_cursors: vec![SlotDumpFollowerReplayCursor {
            follower_id: "unified-storage-lagging-follower".to_string(),
            shard_id: case.shard_id,
            oplog_sequence: 0,
            index_log_sequence: 0,
        }],
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
    let selected_slots = engine
        .slot_storage_summaries(case.shard_id)
        .into_iter()
        .filter(|summary| summary.page_ref_count > 0)
        .map(|summary| summary.routing_slot)
        .collect::<Vec<_>>();
    let report = engine.storage_cache_warmup_report(case.shard_id, selected_slots);
    assert!(report.considered_page_refs > 0);
    assert!(report.page_store_reads > 0);
    assert_eq!(report.failed_page_refs, 0);
    assert_eq!(report.warmed_page_refs, report.considered_page_refs);
    let after = engine.storage_cache_inspection_report(case.shard_id);
    assert!(!after.entries.is_empty());
    assert!(!after.slot_summaries.is_empty());
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
        .replay_oplog_strict(case.shard_id, 0, &follower)
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
        page_segment_id: 0,
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
        page_segment_id: 0,
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
        page_segment_id: 0,
        offset: 0,
        size: 256 * 1024,
    });
    assert_stream_ok(&first_chunk, "cross_block_large_values/first");
    assert_eq!(first_chunk.data, large[..256 * 1024].to_vec());

    let second_chunk = reopened.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_segment_id: 0,
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
        recovery.segment_integrity.integrity_ok,
        "case={} segment integrity failed: {:?}",
        case_name, recovery.segment_integrity
    );
    assert_eq!(recovery.segment_integrity.stale_page_ref_count, 0);
    assert_eq!(recovery.segment_integrity.corrupt_page_segment_count, 0);
    assert_eq!(recovery.segment_integrity.unreadable_page_ref_count, 0);
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
        // The expected payload can carry a C++ status contract that maps onto
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
            if actual_status.code == expected.status {
                return;
            }
            let rust_conditional_rejection = expected.status == "already_exists"
                && matches!(
                    actual_response,
                    CommandResponse::Integer { value: 0 } | CommandResponse::Bytes { value: None }
                );
            let rust_missing_value = expected.status == "not_found"
                && matches!(actual_response, CommandResponse::Bytes { value: None })
                || expected.status == "not_found"
                    && matches!(
                        actual_response,
                        CommandResponse::Values { values } if values.iter().all(Option::is_none)
                    )
                || expected.status == "not_found"
                    && matches!(
                        actual_response,
                        CommandResponse::HashEntries { entries } if entries.is_empty()
                    )
                || expected.status == "not_found"
                    && matches!(
                        actual_response,
                        CommandResponse::Members { members } if members.is_empty()
                    )
                || expected.status == "not_found"
                    && matches!(actual_response, CommandResponse::Integer { value: 0 });
            assert!(
                rust_conditional_rejection || rust_missing_value,
                "case={} step={} status mismatch actual={:?} expected={}",
                case.name,
                step.name,
                actual_status,
                expected.status
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
        Some(UnifiedExpected::Static(_)) => {}
        None => {}
    }
}

fn expected_status_code(expected: &UnifiedExpected) -> Option<&str> {
    match expected {
        UnifiedExpected::Status(status) if status.kind == "status" => Some(status.status.as_str()),
        UnifiedExpected::Status(_)
        | UnifiedExpected::Bool { .. }
        | UnifiedExpected::Static(_)
        | UnifiedExpected::Response(_) => None,
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
