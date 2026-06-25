use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::{Command, ExecuteRequest, SlotDumpFaultMatrixReport, TemporalEngine};

#[derive(Debug, Serialize)]
struct StorageFaultMatrixHarnessReport {
    root: String,
    shard_id: u64,
    production_ready_slice: bool,
    report: SlotDumpFaultMatrixReport,
}

fn main() {
    let root = parse_root();
    std::fs::create_dir_all(&root).expect("failed to create storage fault matrix root");
    let shard_id = 1;
    let engine = TemporalEngine::with_local_dirs(
        256,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(shard_id);
    for (key, value) in [
        ("fault-matrix-string", b"value".to_vec()),
        ("fault-matrix-string-2", b"value-2".to_vec()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(
            response.status.ok,
            "storage fault matrix seed write failed: {:?}",
            response.status
        );
    }

    let report = engine.slot_dump_fault_matrix_report(shard_id);
    let harness = StorageFaultMatrixHarnessReport {
        root: root.display().to_string(),
        shard_id,
        production_ready_slice: report.production_ready_slice,
        report,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&harness).expect("storage fault matrix report serializes")
    );
    if !harness.production_ready_slice {
        std::process::exit(1);
    }
}

fn parse_root() -> PathBuf {
    let mut root =
        std::env::temp_dir().join(format!("temporalstore-storage-fault-matrix-{}", now_ms()));
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
    eprintln!("usage: storage_fault_matrix_harness [--root <path>]");
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
