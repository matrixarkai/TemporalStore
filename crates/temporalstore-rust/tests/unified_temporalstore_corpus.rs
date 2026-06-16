use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, path::Path};

use serde::Deserialize;
use temporalstore_rust::http::{json_response, parse_json, serve};
use temporalstore_rust::{
    ClientOptions, Command, CommandResponse, ExecuteRequest, Status, TableOptions, TemporalEngine,
    TemporalStoreClient,
};

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
    command: Command,
    #[serde(default)]
    expect_status: Option<Status>,
    #[serde(default)]
    expect: Option<CommandResponse>,
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
        .filter_map(|step| step.expect.as_ref().map(response_kind))
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

fn command_kind(command: &Command) -> &'static str {
    match command {
        Command::CommonDelete { .. } => "common_delete",
        Command::CommonExpire { .. } => "common_expire",
        Command::CommonTtl { .. } => "common_ttl",
        Command::CommonExists { .. } => "common_exists",
        Command::StringSet { .. } => "string_set",
        Command::StringSetEx { .. } => "string_set_ex",
        Command::StringSetConditional { .. } => "string_set_conditional",
        Command::StringGet { .. } => "string_get",
        Command::StringDelete { .. } => "string_delete",
        Command::HashSet { .. } => "hash_set",
        Command::HashGet { .. } => "hash_get",
        Command::HashMultiGet { .. } => "hash_multi_get",
        Command::HashMultiSet { .. } => "hash_multi_set",
        Command::HashIncrBy { .. } => "hash_incr_by",
        Command::HashGetAll { .. } => "hash_get_all",
        Command::HashLen { .. } => "hash_len",
        Command::HashDelete { .. } => "hash_delete",
        Command::SetAdd { .. } => "set_add",
        Command::SetMembers { .. } => "set_members",
        Command::SetRemove { .. } => "set_remove",
        Command::FeatureAppend { .. } => "feature_append",
        Command::FeatureAppendWithPolicy { .. } => "feature_append_with_policy",
        Command::FeatureQuery { .. } => "feature_query",
        Command::FeatureQueryFiltered { .. } => "feature_query_filtered",
        Command::FeatureReplace { .. } => "feature_replace",
        Command::FeatureDelete { .. } => "feature_delete",
        Command::FeatureAggQuery { .. } => "feature_agg_query",
        Command::SequenceAdd { .. } => "sequence_add",
        Command::SequenceQuery { .. } => "sequence_query",
        Command::SequenceBatchQuery { .. } => "sequence_batch_query",
        Command::IpsAdd { .. } => "ips_add",
        Command::IpsAddWithOptions { .. } => "ips_add_with_options",
        Command::IpsLoad { .. } => "ips_load",
        Command::IpsQueryLast { .. } => "ips_query_last",
        Command::IpsQueryRange { .. } => "ips_query_range",
        Command::IpsBatchQueryLast { .. } => "ips_batch_query_last",
        Command::IpsRemove { .. } => "ips_remove",
        Command::IpsDelete { .. } => "ips_delete",
        Command::IpsCount { .. } => "ips_count",
        Command::IpsQueryRangeWithOptions { .. } => "ips_query_range_with_options",
        Command::IpsSnapshot { .. } => "ips_snapshot",
        Command::IpsSnapshotReport { .. } => "ips_snapshot_report",
        Command::IpsStat { .. } => "ips_stat",
        Command::IpsFilter { .. } => "ips_filter",
        Command::RiskIncrement { .. } => "risk_increment",
        Command::RiskIncrementWithOptions { .. } => "risk_increment_with_options",
        Command::RiskChangeAdd { .. } => "risk_change_add",
        Command::RiskCount { .. } => "risk_count",
        Command::RiskQuery { .. } => "risk_query",
        Command::RiskDetail { .. } => "risk_detail",
        Command::RiskSet { .. } => "risk_set",
        Command::RiskSetAndGet { .. } => "risk_set_and_get",
        Command::RiskFamilyQuery { .. } => "risk_family_query",
        Command::RiskFolSet { .. } => "risk_fol_set",
        Command::RiskFolQuery { .. } => "risk_fol_query",
        Command::RiskManager { .. } => "risk_manager",
        Command::RiskDebug { .. } => "risk_debug",
        Command::ContextUpsertNode { .. } => "context_upsert_node",
        Command::ContextGetNode { .. } => "context_get_node",
        Command::ContextWriteEvent { .. } => "context_write_event",
        Command::ContextQueryEvents { .. } => "context_query_events",
        Command::ContextWriteIndexRef { .. } => "context_write_index_ref",
        Command::ContextQueryIndex { .. } => "context_query_index",
        Command::ContextWritePackAudit { .. } => "context_write_pack_audit",
        Command::ContextQueryPackAudit { .. } => "context_query_pack_audit",
        Command::ContextMarkSummaryDirty { .. } => "context_mark_summary_dirty",
        Command::ContextQuerySummaryDirty { .. } => "context_query_summary_dirty",
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
        CommandResponse::ContextEvents { .. } => "context_events",
        CommandResponse::ContextIndexRefs { .. } => "context_index_refs",
        CommandResponse::ContextPackAudits { .. } => "context_pack_audits",
        CommandResponse::ContextSummaryDirtyMarkers { .. } => "context_summary_dirty_markers",
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

        let response = engine.execute(ExecuteRequest {
            shard_id: case.shard_id,
            command: step.command.clone(),
        });

        assert_step_status(case, step, &response.status);

        if let Some(expected) = &step.expect {
            assert_eq!(
                &response.response, expected,
                "case={} step={} response mismatch",
                case.name, step.name
            );
        }
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
        if step.restart_before {
            *engine.lock().expect("engine lock poisoned") =
                new_engine(dir.path(), &page_dir, &index_dir, case.shard_id);
        }

        let response = table
            .execute(step.command.clone())
            .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name));

        assert_step_status(case, step, &response.status);

        if let Some(expected) = &step.expect {
            assert_eq!(
                &response.response, expected,
                "case={} step={} client response mismatch",
                case.name, step.name
            );
        }
    }
}

fn assert_step_status(case: &UnifiedCase, step: &UnifiedStep, actual: &Status) {
    if let Some(expected) = &step.expect_status {
        assert_eq!(
            actual, expected,
            "case={} step={} status mismatch",
            case.name, step.name
        );
    } else {
        assert!(
            actual.ok,
            "case={} step={} failed status={:?}",
            case.name, step.name, actual
        );
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
