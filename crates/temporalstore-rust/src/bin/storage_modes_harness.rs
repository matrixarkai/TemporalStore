use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::{
    Command, CommandResponse, EventReplicationMode, ExecuteRequest, RaftCluster, RaftConfig,
    ReplayReport, SharedStoreFlushReport, SharedStoreReplicator, SharedStoreStorageMode,
    SharedStoreWriteReport, TemporalEngine,
};
use temporalstore_snapshot::object_store::{FileObjectStore, MatrixObjectStore, ObjectStore};

#[derive(Debug, Clone)]
struct HarnessOptions {
    root: PathBuf,
    shared_store_root: Option<PathBuf>,
    raft_wal_root: Option<PathBuf>,
    async_flush_limit: usize,
    shared_store_backend: SharedStoreBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedStoreBackend {
    LocalFs,
    MatrixObjectStore,
}

impl SharedStoreBackend {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "local_fs" | "file" | "file_object_store" => Some(Self::LocalFs),
            "matrixobject"
            | "matrix_object"
            | "matrixobject_local_compat"
            | "matrixobjectstore"
            | "matrix_object_store"
            | "matrixobjectstore_local_compat" => Some(Self::MatrixObjectStore),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::LocalFs => "local_fs",
            Self::MatrixObjectStore => "matrixobject_local_compat",
        }
    }
}

#[derive(Debug, Serialize)]
struct StorageModesSummary {
    root: String,
    shared_store_backend: String,
    shared_store_uri_scheme: String,
    shared_store_sync: SharedStoreModeSummary,
    shared_store_async: SharedStoreModeSummary,
    raft_local_file: RaftLocalFileSummary,
    dynamic_event_replication: DynamicEventReplicationSummary,
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

#[derive(Debug, Serialize)]
struct DynamicEventReplicationSummary {
    events: Vec<DynamicReplicationEventReport>,
    restart_required: bool,
}

#[derive(Debug, Serialize)]
struct DynamicReplicationEventReport {
    requested_mode: EventReplicationMode,
    effective_mode: EventReplicationMode,
    key: String,
    read_value: Option<String>,
    shared_store_write: Option<SharedStoreWriteReport>,
    shared_store_flush: Option<SharedStoreFlushReport>,
    raft_commit_index: Option<u64>,
}

#[tokio::main]
async fn main() {
    let options = parse_options();
    fs::create_dir_all(&options.root).expect("failed to create harness root");
    let shared_store_root = options
        .shared_store_root
        .clone()
        .unwrap_or_else(|| options.root.join("shared-store"));
    fs::create_dir_all(&shared_store_root).expect("failed to create shared-store root");
    let summary = match options.shared_store_backend {
        SharedStoreBackend::LocalFs => {
            let store = Arc::new(FileObjectStore::new(shared_store_root));
            run_storage_modes(options, store, "file").await
        }
        SharedStoreBackend::MatrixObjectStore => {
            let store = Arc::new(MatrixObjectStore::new(shared_store_root));
            run_storage_modes(options, store, "matrixobject").await
        }
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&summary).expect("summary should serialize")
    );
}

async fn run_storage_modes<O>(
    options: HarnessOptions,
    store: Arc<O>,
    shared_store_uri_scheme: &str,
) -> StorageModesSummary
where
    O: ObjectStore + 'static,
{
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
    let raft_wal_root = options
        .raft_wal_root
        .clone()
        .unwrap_or_else(|| options.root.join("raft-wal"));
    let dynamic_event_replication = run_dynamic_event_replication(
        &replicator,
        raft_wal_root.join("dynamic-events"),
        options.async_flush_limit,
    )
    .await;
    let raft_local_file = run_raft_local_file(raft_wal_root);

    StorageModesSummary {
        root: options.root.display().to_string(),
        shared_store_backend: options.shared_store_backend.as_str().to_string(),
        shared_store_uri_scheme: shared_store_uri_scheme.to_string(),
        shared_store_sync: sync,
        shared_store_async: async_mode,
        raft_local_file,
        dynamic_event_replication,
    }
}

async fn run_dynamic_event_replication<O>(
    replicator: &SharedStoreReplicator<O>,
    raft_wal_dir: PathBuf,
    flush_limit: usize,
) -> DynamicEventReplicationSummary
where
    O: ObjectStore + 'static,
{
    let mut events = Vec::new();
    let sync_writer = replicator.storage_writer(SharedStoreStorageMode::Sync, 1);
    let sync_write = sync_writer
        .write(
            91,
            Command::StringSet {
                key: "dynamic-sync".to_string(),
                value: b"sync".to_vec(),
            },
        )
        .await
        .expect("dynamic sync event should write");
    let sync_follower = replay_dynamic_shared_store(replicator, 91, "dynamic-sync").await;
    events.push(DynamicReplicationEventReport {
        requested_mode: EventReplicationMode::SyncStorage,
        effective_mode: EventReplicationMode::SyncStorage,
        key: "dynamic-sync".to_string(),
        read_value: sync_follower,
        shared_store_write: Some(sync_write),
        shared_store_flush: None,
        raft_commit_index: None,
    });

    let async_writer = replicator.storage_writer(SharedStoreStorageMode::Async, 1);
    let async_write = async_writer
        .write(
            92,
            Command::StringSet {
                key: "dynamic-async".to_string(),
                value: b"async".to_vec(),
            },
        )
        .await
        .expect("dynamic async event should queue");
    let async_flush = async_writer
        .flush_pending(flush_limit)
        .await
        .expect("dynamic async event should flush");
    let async_follower = replay_dynamic_shared_store(replicator, 92, "dynamic-async").await;
    events.push(DynamicReplicationEventReport {
        requested_mode: EventReplicationMode::AsyncStorage,
        effective_mode: EventReplicationMode::AsyncStorage,
        key: "dynamic-async".to_string(),
        read_value: async_follower,
        shared_store_write: Some(async_write),
        shared_store_flush: Some(async_flush),
        raft_commit_index: None,
    });

    let raft =
        RaftCluster::new_single_shard_with_wal(&raft_wal_dir, 93, [1, 2, 3], RaftConfig::default())
            .expect("dynamic raft event cluster should start");
    raft.propose(Command::StringSet {
        key: "dynamic-raft".to_string(),
        value: b"raft".to_vec(),
    })
    .expect("dynamic raft event should commit");
    let status = raft.status();
    let raft_read = match raft
        .read_from_replica(
            status.leader_id,
            Command::StringGet {
                key: "dynamic-raft".to_string(),
            },
        )
        .expect("dynamic raft event should read")
    {
        CommandResponse::Bytes { value: Some(bytes) } => {
            Some(String::from_utf8_lossy(&bytes).to_string())
        }
        _ => None,
    };
    events.push(DynamicReplicationEventReport {
        requested_mode: EventReplicationMode::Raft,
        effective_mode: EventReplicationMode::Raft,
        key: "dynamic-raft".to_string(),
        read_value: raft_read,
        shared_store_write: None,
        shared_store_flush: None,
        raft_commit_index: Some(status.commit_index),
    });

    DynamicEventReplicationSummary {
        events,
        restart_required: false,
    }
}

async fn replay_dynamic_shared_store<O>(
    replicator: &SharedStoreReplicator<O>,
    shard_id: u64,
    key: &str,
) -> Option<String>
where
    O: ObjectStore + 'static,
{
    let follower = TemporalEngine::with_local_dirs(
        1024,
        unique_child("dynamic-storage-mode-cache"),
        unique_child("dynamic-storage-mode-pages"),
        unique_child("dynamic-storage-mode-index"),
    );
    follower.load_shard(shard_id);
    replicator
        .replay_oplog_strict(shard_id, 0, &follower)
        .await
        .expect("dynamic shared-store replay should succeed");
    match follower
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
    }
}

async fn run_shared_store_mode<O>(
    replicator: &SharedStoreReplicator<O>,
    mode: SharedStoreStorageMode,
    shard_id: u64,
    key: &str,
    value: &str,
    flush_limit: usize,
) -> SharedStoreModeSummary
where
    O: ObjectStore + 'static,
{
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
    let mut shared_store_root = None;
    let mut raft_wal_root = None;
    let mut async_flush_limit = 1usize;
    let mut shared_store_backend = SharedStoreBackend::LocalFs;
    let mut args = std::env::args().skip(1);
    while let Some(key) = args.next() {
        let Some(value) = args.next() else {
            usage_and_exit();
        };
        match key.as_str() {
            "--root" => root = PathBuf::from(value),
            "--shared-store-root" => shared_store_root = Some(PathBuf::from(value)),
            "--raft-wal-root" => raft_wal_root = Some(PathBuf::from(value)),
            "--async-flush-limit" => async_flush_limit = parse(&value, &key),
            "--shared-store-backend" => {
                shared_store_backend = SharedStoreBackend::parse(&value).unwrap_or_else(|| {
                    eprintln!("invalid value for {key}: {value}");
                    usage_and_exit();
                })
            }
            _ => usage_and_exit(),
        }
    }
    HarnessOptions {
        root,
        shared_store_root,
        raft_wal_root,
        async_flush_limit,
        shared_store_backend,
    }
}

fn parse<T: std::str::FromStr>(value: &str, key: &str) -> T {
    value.parse().unwrap_or_else(|_| {
        eprintln!("invalid value for {key}: {value}");
        std::process::exit(2);
    })
}

fn usage_and_exit() -> ! {
    eprintln!(
        "usage: storage_modes_harness [--root <path>] [--shared-store-root <path>] [--raft-wal-root <path>] [--async-flush-limit <n>] [--shared-store-backend local_fs|matrixobject|matrixobject_local_compat]"
    );
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
