use std::{fs, path::Path};

use serde::Deserialize;
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

#[derive(Debug, Deserialize)]
struct UnifiedCorpus {
    schema_version: u32,
    name: String,
    cases: Vec<UnifiedCase>,
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
    command: Command,
    #[serde(default)]
    expect: Option<CommandResponse>,
}

#[test]
fn rust_executes_shared_cpp_rust_temporalstore_corpus() {
    let corpus_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("compat/unified_temporalstore_cases.json");
    let corpus_bytes = fs::read(&corpus_path).expect("shared corpus should be readable");
    let corpus: UnifiedCorpus =
        serde_json::from_slice(&corpus_bytes).expect("shared corpus should deserialize");

    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.name, "temporalstore-unified-cpp-rust-corpus");
    assert!(!corpus.cases.is_empty(), "shared corpus must contain cases");

    for case in corpus.cases {
        run_case(&case);
    }
}

fn run_case(case: &UnifiedCase) {
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

        assert!(
            response.status.ok,
            "case={} step={} failed status={:?}",
            case.name, step.name, response.status
        );

        if let Some(expected) = &step.expect {
            assert_eq!(
                &response.response, expected,
                "case={} step={} response mismatch",
                case.name, step.name
            );
        }
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
