use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::Serialize;
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, StorageRecoveryReport, TemporalEngine,
};

#[derive(Debug, Clone)]
struct HarnessOptions {
    root: PathBuf,
    mode: HarnessMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HarnessMode {
    WriteAbort,
    CorruptPage,
    Recover,
}

#[derive(Debug, Serialize)]
struct CrashHarnessSummary {
    root: String,
    before_value: Option<String>,
    after_value: Option<String>,
    recovery: StorageRecoveryReport,
}

fn main() {
    let options = parse_options();
    fs::create_dir_all(&options.root).expect("failed to create crash harness root");
    match options.mode {
        HarnessMode::WriteAbort => write_then_abort(options.root),
        HarnessMode::CorruptPage => corrupt_first_page_segment(options.root),
        HarnessMode::Recover => recover_and_print(options.root),
    }
}

fn write_then_abort(root: PathBuf) -> ! {
    let engine = open_engine(&root);
    engine.load_shard(1);
    write_string(&engine, "before", "before-value");
    engine.block_store().roll_segment().expect("segment roll");
    write_string(&engine, "after", "after-value");

    // Simulate abrupt process loss after durable appends and index/log syncs.
    std::process::abort();
}

fn recover_and_print(root: PathBuf) {
    let engine = open_engine(&root);
    engine.load_shard(1);
    let summary = CrashHarnessSummary {
        root: root.display().to_string(),
        before_value: read_string(&engine, "before"),
        after_value: read_string(&engine, "after"),
        recovery: engine.storage_recovery_report(1),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).expect("summary should serialize")
    );
}

fn corrupt_first_page_segment(root: PathBuf) {
    let segment_path = root
        .join("pages")
        .join("page_segment_00000000000000000000.seg");
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&segment_path)
        .expect("page segment should exist before corruption");
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).expect("read page segment");
    assert!(
        !bytes.is_empty(),
        "page segment should contain at least one durable page"
    );
    let offset = bytes.len().saturating_sub(1) as u64;
    let corrupted = bytes[offset as usize] ^ 0xff;
    file.seek(SeekFrom::Start(offset))
        .expect("seek corruption byte");
    file.write_all(&[corrupted]).expect("write corruption byte");
    file.sync_all().expect("sync corrupted page segment");
}

fn open_engine(root: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        256,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    )
}

fn write_string(engine: &TemporalEngine, key: &str, value: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.as_bytes().to_vec(),
        },
    });
    assert!(response.status.ok, "{}", response.status.message);
}

fn read_string(engine: &TemporalEngine, key: &str) -> Option<String> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: key.to_string(),
            },
        })
        .response
    {
        CommandResponse::Bytes { value: Some(bytes) } => {
            Some(String::from_utf8_lossy(&bytes).to_string())
        }
        _ => None,
    }
}

fn parse_options() -> HarnessOptions {
    let mut root = None;
    let mut mode = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => root = args.next().map(PathBuf::from),
            "--mode" => {
                mode = args.next().map(|mode| match mode.as_str() {
                    "write-abort" => HarnessMode::WriteAbort,
                    "corrupt-page" => HarnessMode::CorruptPage,
                    "recover" => HarnessMode::Recover,
                    other => panic!("unsupported --mode {other}"),
                });
            }
            "-h" | "--help" => {
                eprintln!(
                    "usage: storage_crash_harness --root <dir> --mode <write-abort|corrupt-page|recover>"
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument {other}"),
        }
    }
    HarnessOptions {
        root: root.expect("--root is required"),
        mode: mode.expect("--mode is required"),
    }
}
