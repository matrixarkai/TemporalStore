use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, RaftCluster, RaftConfig, ReplayReport,
    SharedStoreFlushReport, SharedStoreReplicator, SharedStoreStorageMode, SharedStoreWriteReport,
    TemporalEngine,
};
use temporalstore_snapshot::object_store::FileObjectStore;

#[derive(Debug, Clone)]
struct HarnessOptions {
    root: PathBuf,
    async_flush_limit: usize,
}

#[derive(Debug, Serialize)]
struct StorageModesSummary {
    root: String,
    shared_store_sync: SharedStoreModeSummary,
    shared_store_async: SharedStoreModeSummary,
    raft_local_file: RaftLocalFileSummary,
}

#[derive(Debug, Serialize)]
struct SharedStoreModeSummary {
    shard_id: u64,
    writes: Vec<SharedStoreWriteReport>,
    flushes: Vec<SharedStoreFlushReport>,
    replay: ReplaySummary,
    read_value: Option<String>,
}

#[derive(Debug, Serialize)]
struct ReplaySummary {
    applied: usize,
    last_oplog_index: u64,
}

impl From<ReplayReport> for ReplaySummary {
    fn from(report: ReplayReport) -> Self {
        Self {
            applied: report.applied,
            last_oplog_index: report.last_oplog_index,
        }
    }
}

#[derive(Debug, Serialize)]
struct RaftLocalFileSummary {
    wal_dir: String,
    leader_id: u64,
    commit_index_before_restore: u64,
    commit_index_after_restore: u64,
    read_value_after_restore: Option<String>,
    wal_files: Vec<String>,
}

#[tokio::main]
async fn main() {
    let options = parse_options();
    fs::create_dir_all(&options.root).expect("failed to create harness root");
    let store = Arc::new(FileObjectStore::new(options.root.join("shared-store")));
    let replicator = SharedStoreReplicator::new("storage-modes-harness", store);

    let sync = run_shared_store_mode(
        &replicator,
        SharedStoreStorageMode::Sync,
        1,
        "sync-key",
        "sync-value",
        options.async_flush_limit,
    )
    .await;
    let async_mode = run_shared_store_mode(
        &replicator,
        SharedStoreStorageMode::Async,
        2,
        "async-key",
        "async-value",
        options.async_flush_limit,
    )
    .await;
    let raft_local_file = run_raft_local_file(options.root.join("raft-wal"));

    println!(
        "{}",
        serde_json::to_string_pretty(&StorageModesSummary {
            root: options.root.display().to_string(),
            shared_store_sync: sync,
            shared_store_async: async_mode,
            raft_local_file,
        })
        .expect("summary should serialize")
    );
}

async fn run_shared_store_mode(
    replicator: &SharedStoreReplicator<FileObjectStore>,
    mode: SharedStoreStorageMode,
    shard_id: u64,
    key: &str,
    value: &str,
    flush_limit: usize,
) -> SharedStoreModeSummary {
    let writer = replicator.storage_writer(mode, 1);
    let mut writes = Vec::new();
    let first = writer
        .write(
            shard_id,
            Command::StringSet {
                key: key.to_string(),
                value: value.as_bytes().to_vec(),
            },
        )
        .await
        .expect("shared-store write should succeed");
    writes.push(first);
    let second = writer
        .write(
            shard_id,
            Command::HashSet {
                key: format!("{key}:hash"),
                field: "field".to_string(),
                value: b"hash-value".to_vec(),
            },
        )
        .await
        .expect("shared-store write should succeed");
    writes.push(second);

    let mut flushes = Vec::new();
    if mode == SharedStoreStorageMode::Async {
        while writer.queued_len() > 0 {
            flushes.push(
                writer
                    .flush_pending(flush_limit)
                    .await
                    .expect("async shared-store flush should succeed"),
            );
        }
    }

    let follower = TemporalEngine::with_local_dirs(
        1024,
        unique_child("storage-mode-cache"),
        unique_child("storage-mode-pages"),
        unique_child("storage-mode-index"),
    );
    follower.load_shard(shard_id);
    let replay = replicator
        .replay_oplog_strict(shard_id, 0, &follower)
        .await
        .expect("strict shared-store replay should succeed");
    let read_value = match follower
        .execute(ExecuteRequest {
            shard_id,
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
    };
    assert_eq!(read_value.as_deref(), Some(value));

    SharedStoreModeSummary {
        shard_id,
        writes,
        flushes,
        replay: replay.into(),
        read_value,
    }
}

fn run_raft_local_file(wal_dir: PathBuf) -> RaftLocalFileSummary {
    let cluster =
        RaftCluster::new_single_shard_with_wal(&wal_dir, 7, [1, 2, 3], RaftConfig::default())
            .expect("local-file raft cluster should start");
    cluster
        .propose(Command::StringSet {
            key: "raft-local".to_string(),
            value: b"wal-value".to_vec(),
        })
        .expect("raft write should commit");
    let before = cluster.status();
    let restored =
        RaftCluster::restore_single_shard_from_wal(&wal_dir, 7, [1, 2, 3], RaftConfig::default())
            .expect("local-file raft cluster should restore");
    let after = restored.status();
    let read_value = match restored
        .read_from_replica(
            after.leader_id,
            Command::StringGet {
                key: "raft-local".to_string(),
            },
        )
        .expect("restored local-file raft read should succeed")
    {
        CommandResponse::Bytes { value: Some(bytes) } => {
            Some(String::from_utf8_lossy(&bytes).to_string())
        }
        _ => None,
    };
    assert_eq!(read_value.as_deref(), Some("wal-value"));

    RaftLocalFileSummary {
        wal_dir: wal_dir.display().to_string(),
        leader_id: after.leader_id,
        commit_index_before_restore: before.commit_index,
        commit_index_after_restore: after.commit_index,
        read_value_after_restore: read_value,
        wal_files: list_files(&wal_dir),
    }
}

fn list_files(root: &PathBuf) -> Vec<String> {
    let mut out = Vec::new();
    collect_files(root, root, &mut out);
    out.sort();
    out
}

fn collect_files(root: &PathBuf, current: &PathBuf, out: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(current) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out);
        } else if let Ok(relative) = path.strip_prefix(root) {
            out.push(relative.display().to_string());
        }
    }
}

fn unique_child(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", now_ms()))
}

fn parse_options() -> HarnessOptions {
    let mut root = std::env::temp_dir().join(format!("temporalstore-storage-modes-{}", now_ms()));
    let mut async_flush_limit = 1usize;
    let mut args = std::env::args().skip(1);
    while let Some(key) = args.next() {
        let Some(value) = args.next() else {
            usage_and_exit();
        };
        match key.as_str() {
            "--root" => root = PathBuf::from(value),
            "--async-flush-limit" => async_flush_limit = parse(&value, &key),
            _ => usage_and_exit(),
        }
    }
    HarnessOptions {
        root,
        async_flush_limit,
    }
}

fn parse<T: std::str::FromStr>(value: &str, key: &str) -> T {
    value.parse().unwrap_or_else(|_| {
        eprintln!("invalid value for {key}: {value}");
        std::process::exit(2);
    })
}

fn usage_and_exit() -> ! {
    eprintln!("usage: storage_modes_harness [--root <path>] [--async-flush-limit <n>]");
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
