use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::control::{Config, SetConfigRequest};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::raft::{RaftCluster, RaftConfig, RaftNodeStatus};
use temporalstore_rust::shared_store::{SharedStoreReplicator, SharedStoreStorageMode};
use temporalstore_rust::types::{
    Command, CommandResponse, ExecuteRequest, FeatureFilter, FeatureFilterOp, SequenceFeatureRow,
};
use temporalstore_snapshot::object_store::FileObjectStore;

#[derive(Debug, Clone, Copy)]
struct HarnessOptions {
    shard_id: u64,
    nodes: u64,
    string_ops: usize,
    hash_ops: usize,
    sequence_keys: usize,
    sequence_len: usize,
    scale_events: usize,
    failover_every: usize,
    read_sample_every: usize,
    max_log_entry_bytes: u64,
    compare_shared_store: bool,
    shared_store_ops: usize,
    shared_store_flush_every: usize,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            shard_id: 1,
            nodes: 3,
            string_ops: 1_000,
            hash_ops: 250,
            sequence_keys: 4,
            sequence_len: 500,
            scale_events: 2,
            failover_every: 250,
            read_sample_every: 100,
            max_log_entry_bytes: RaftConfig::default().max_memory_replicate_log_bytes,
            compare_shared_store: false,
            shared_store_ops: 1_000,
            shared_store_flush_every: 25,
        }
    }
}

#[derive(Debug, Serialize)]
struct HarnessSummary {
    shard_id: u64,
    final_nodes: Vec<u64>,
    leader_id: u64,
    commit_index: u64,
    string_ops: usize,
    hash_ops: usize,
    sequence_rows: usize,
    sampled_reads: usize,
    raft_replica_read_latency: LatencySummary,
    shared_store: Option<SharedStoreComparisonSummary>,
    failovers: usize,
    scale_events: usize,
    elapsed_ms: u128,
    write_ops_per_sec: f64,
    replication_healthy: bool,
    max_replica_lag: u64,
    raft_node_statuses: Vec<RaftNodeStatus>,
    max_log_entry_bytes: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct LatencySummary {
    samples: usize,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
}

#[derive(Debug, Serialize)]
struct SharedStoreComparisonSummary {
    sync_ops: usize,
    async_ops: usize,
    sync_primary_write_latency: LatencySummary,
    async_primary_write_latency: LatencySummary,
    sync_storage_write_latency: LatencySummary,
    async_storage_write_latency: LatencySummary,
    async_storage_enqueue_latency: LatencySummary,
    async_storage_flush_latency: LatencySummary,
    sync_replica_read_latency: LatencySummary,
    async_replica_read_latency: LatencySummary,
    sync_max_lag: u64,
    async_max_lag: u64,
    async_flush_every: usize,
}

fn main() {
    let options = parse_options();
    if options.nodes == 0 {
        eprintln!("--nodes must be > 0");
        std::process::exit(2);
    }

    let start = Instant::now();
    let node_ids = (1..=options.nodes).collect::<Vec<_>>();
    let cluster = RaftCluster::new_single_shard_with_config(
        options.shard_id,
        node_ids.clone(),
        RaftConfig {
            enable_pre_vote: true,
            max_memory_replicate_log_bytes: options.max_log_entry_bytes,
            ..RaftConfig::default()
        },
    )
    .expect("raft config should be valid");

    let mut sampled_reads = 0usize;
    let mut raft_read_latencies = Vec::new();
    let mut failovers = 0usize;
    let mut rng = Lcg::new(0x5eed_cafe);

    for i in 0..options.string_ops {
        let key = format!("scale:string:{i}");
        let value = format!("value-{i}").into_bytes();
        cluster
            .propose(Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            })
            .expect("string write should commit");

        if options.read_sample_every > 0 && i % options.read_sample_every == 0 {
            sampled_reads += 1;
            let read_start = Instant::now();
            let response = cluster
                .read_from_replica(
                    choose_live_replica(&cluster, &mut rng),
                    Command::StringGet { key },
                )
                .expect("sample read should succeed");
            raft_read_latencies.push(read_start.elapsed());
            assert_eq!(response, CommandResponse::Bytes { value: Some(value) });
        }

        if options.failover_every > 0 && i > 0 && i % options.failover_every == 0 {
            rotate_leader(&cluster);
            failovers += 1;
        }
    }

    for i in 0..options.hash_ops {
        cluster
            .propose(Command::HashSet {
                key: format!("scale:hash:{}", i % 128),
                field: format!("field:{i}"),
                value: i.to_le_bytes().to_vec(),
            })
            .expect("hash write should commit");
    }

    for key_id in 0..options.sequence_keys {
        let rows = (0..options.sequence_len)
            .map(|offset| SequenceFeatureRow {
                timestamp_ms: 1_700_000_000_000 + offset as u64,
                gid: offset as u64,
                action_type: (offset % 8) as u32,
                duration: (offset % 600) as u32,
                author_id: (key_id as u64) * 100_000 + offset as u64,
            })
            .collect::<Vec<_>>();
        let key = format!("scale:sequence:{key_id}");
        cluster
            .propose(Command::SequenceAdd {
                key: key.clone(),
                rows,
            })
            .expect("sequence write should commit");
        let read_start = Instant::now();
        let response = cluster
            .read_from_replica(
                choose_live_replica(&cluster, &mut rng),
                Command::SequenceQuery {
                    key,
                    start_ms: 1_700_000_000_000,
                    end_ms: 1_700_000_999_999,
                    count: 32,
                    filters: vec![FeatureFilter {
                        field: "action_type".to_string(),
                        op: FeatureFilterOp::GreaterThan,
                        value: 2,
                    }],
                },
            )
            .expect("sequence sample should read");
        raft_read_latencies.push(read_start.elapsed());
        match response {
            CommandResponse::SequenceRows { rows } => assert!(!rows.is_empty()),
            other => panic!("unexpected sequence response: {other:?}"),
        }
        sampled_reads += 1;
    }

    let mut next_node_id = options.nodes + 1;
    let mut completed_scale_events = 0usize;
    for event in 0..options.scale_events {
        if event % 2 == 0 {
            cluster
                .add_node_safely(next_node_id)
                .expect("safe scale-up should catch up new replica");
            next_node_id += 1;
        } else {
            let candidates = cluster
                .membership()
                .voters
                .into_iter()
                .filter(|node_id| *node_id != cluster.leader_id())
                .collect::<Vec<_>>();
            if let Some(node_id) = candidates.first().copied() {
                cluster
                    .remove_node_safely(node_id)
                    .expect("safe scale-down should preserve majority");
            }
        }
        completed_scale_events += 1;
    }

    cluster
        .catch_up_live_followers()
        .expect("followers should catch up");
    let health = cluster.replication_health(0);
    let shared_store = if options.compare_shared_store {
        Some(run_shared_store_comparison(&options))
    } else {
        None
    };
    let elapsed_ms = start.elapsed().as_millis();
    let write_ops = options.string_ops + options.hash_ops + options.sequence_keys;
    let write_ops_per_sec = if elapsed_ms == 0 {
        write_ops as f64
    } else {
        write_ops as f64 / (elapsed_ms as f64 / 1_000.0)
    };
    let status = cluster.status();
    let summary = HarnessSummary {
        shard_id: options.shard_id,
        final_nodes: cluster.membership().voters,
        leader_id: cluster.leader_id(),
        commit_index: status.commit_index,
        string_ops: options.string_ops,
        hash_ops: options.hash_ops,
        sequence_rows: options.sequence_keys * options.sequence_len,
        sampled_reads,
        raft_replica_read_latency: summarize_latencies(&raft_read_latencies),
        shared_store,
        failovers,
        scale_events: completed_scale_events,
        elapsed_ms,
        write_ops_per_sec,
        replication_healthy: health.healthy,
        max_replica_lag: health.max_lag,
        raft_node_statuses: status.nodes.clone(),
        max_log_entry_bytes: options.max_log_entry_bytes,
    };

    println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    if !summary.replication_healthy {
        eprintln!(
            "replication health check failed: max lag {}",
            summary.max_replica_lag
        );
        std::process::exit(1);
    }
}

fn parse_options() -> HarnessOptions {
    let mut options = HarnessOptions::default();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0;
    while index < args.len() {
        let key = &args[index];
        let Some(value) = args.get(index + 1) else {
            usage_and_exit();
        };
        match key.as_str() {
            "--shard-id" => options.shard_id = parse(value, key),
            "--nodes" => options.nodes = parse(value, key),
            "--string-ops" => options.string_ops = parse(value, key),
            "--hash-ops" => options.hash_ops = parse(value, key),
            "--sequence-keys" => options.sequence_keys = parse(value, key),
            "--sequence-len" => options.sequence_len = parse(value, key),
            "--scale-events" => options.scale_events = parse(value, key),
            "--failover-every" => options.failover_every = parse(value, key),
            "--read-sample-every" => options.read_sample_every = parse(value, key),
            "--max-log-entry-bytes" => options.max_log_entry_bytes = parse(value, key),
            "--compare-shared-store" => {
                options.compare_shared_store = parse_bool(value, key);
            }
            "--shared-store-ops" => options.shared_store_ops = parse(value, key),
            "--shared-store-flush-every" => options.shared_store_flush_every = parse(value, key),
            "--help" | "-h" => usage_and_exit(),
            other => {
                eprintln!("unknown option: {other}");
                usage_and_exit();
            }
        }
        index += 2;
    }
    options
}

fn parse<T>(value: &str, key: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse().unwrap_or_else(|err| {
        eprintln!("invalid {key} value {value:?}: {err}");
        std::process::exit(2);
    })
}

fn usage_and_exit() -> ! {
    eprintln!("usage: scale_harness [options]");
    eprintln!("  --nodes <n>              default 3");
    eprintln!("  --string-ops <n>         default 1000");
    eprintln!("  --hash-ops <n>           default 250");
    eprintln!("  --sequence-keys <n>      default 4");
    eprintln!("  --sequence-len <n>       default 500");
    eprintln!("  --scale-events <n>       default 2");
    eprintln!("  --failover-every <n>     default 250, 0 disables");
    eprintln!("  --read-sample-every <n>  default 100, 0 disables");
    eprintln!("  --max-log-entry-bytes <n> default 32768");
    eprintln!("  --compare-shared-store <bool> default false");
    eprintln!("  --shared-store-ops <n> default 1000");
    eprintln!("  --shared-store-flush-every <n> default 25");
    std::process::exit(2);
}

fn rotate_leader(cluster: &RaftCluster) {
    let current = cluster.leader_id();
    let mut voters = cluster.membership().voters;
    voters.sort_unstable();
    let next = voters
        .into_iter()
        .find(|node_id| *node_id != current)
        .unwrap_or(current);
    cluster
        .transfer_leader(next)
        .expect("leader transfer should succeed");
}

fn choose_live_replica(cluster: &RaftCluster, rng: &mut Lcg) -> u64 {
    let mut nodes = cluster.live_replica_ids();
    nodes.sort_unstable();
    let index = (rng.next() as usize) % nodes.len().max(1);
    nodes[index]
}

fn run_shared_store_comparison(options: &HarnessOptions) -> SharedStoreComparisonSummary {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should start");
    runtime.block_on(async {
        let root =
            std::env::temp_dir().join(format!("temporalstore-shared-store-scale-{}", now_ms()));
        let store = Arc::new(FileObjectStore::new(root.join("objects")));
        let replicator = SharedStoreReplicator::new("scale-harness", store);

        let sync = run_shared_store_mode(
            &replicator,
            options.shard_id,
            SharedStoreStorageMode::Sync,
            options.shared_store_ops,
            1,
        )
        .await;
        let async_report = run_shared_store_mode(
            &replicator,
            options.shard_id + 1,
            SharedStoreStorageMode::Async,
            options.shared_store_ops,
            options.shared_store_flush_every,
        )
        .await;

        SharedStoreComparisonSummary {
            sync_ops: options.shared_store_ops,
            async_ops: options.shared_store_ops,
            sync_primary_write_latency: sync.primary_write_latency,
            async_primary_write_latency: async_report.primary_write_latency,
            sync_storage_write_latency: sync.storage_write_latency,
            async_storage_write_latency: async_report.storage_write_latency,
            async_storage_enqueue_latency: async_report.storage_write_latency,
            async_storage_flush_latency: async_report.flush_latency,
            sync_replica_read_latency: sync.read_latency,
            async_replica_read_latency: async_report.read_latency,
            sync_max_lag: sync.max_lag,
            async_max_lag: async_report.max_lag,
            async_flush_every: options.shared_store_flush_every.max(1),
        }
    })
}

#[derive(Debug)]
struct SharedStoreModeReport {
    primary_write_latency: LatencySummary,
    storage_write_latency: LatencySummary,
    flush_latency: LatencySummary,
    read_latency: LatencySummary,
    max_lag: u64,
}

async fn run_shared_store_mode<O>(
    replicator: &SharedStoreReplicator<O>,
    shard_id: u64,
    mode: SharedStoreStorageMode,
    ops: usize,
    flush_every: usize,
) -> SharedStoreModeReport
where
    O: temporalstore_snapshot::object_store::ObjectStore + 'static,
{
    let primary = TemporalEngine::default();
    let follower = TemporalEngine::default();
    primary.load_shard(shard_id);
    follower.load_shard(shard_id);
    if mode == SharedStoreStorageMode::Async {
        let config = Config {
            async_storage: true,
            ..Config::default()
        };
        assert!(
            primary
                .set_config(SetConfigRequest {
                    shard_id,
                    config: config.clone(),
                })
                .ok
        );
        assert!(
            follower
                .set_config(SetConfigRequest { shard_id, config })
                .ok
        );
    }
    let writer = replicator.storage_writer(mode, 1);
    let flush_every = flush_every.max(1);
    let mut last_written = 0;
    let mut last_replayed = 0;
    let mut max_lag = 0;
    let mut read_latencies = Vec::new();
    let mut primary_write_latencies = Vec::new();
    let mut storage_write_latencies = Vec::new();
    let mut flush_latencies = Vec::new();

    for i in 0..ops {
        let key = format!("shared:{shard_id}:{i}");
        let value = format!("value-{i}").into_bytes();
        let command = Command::StringSet {
            key: key.clone(),
            value: value.clone(),
        };
        let primary_write_start = Instant::now();
        let response = primary.execute(ExecuteRequest {
            shard_id,
            command: command.clone(),
        });
        assert!(response.status.ok, "primary shared-store write failed");
        let storage_write_start = Instant::now();
        let report = writer
            .write(shard_id, command)
            .await
            .expect("shared-store write should publish or queue");
        storage_write_latencies.push(storage_write_start.elapsed());
        primary_write_latencies.push(primary_write_start.elapsed());
        last_written = report.oplog_index;
        if mode == SharedStoreStorageMode::Async && (i + 1) % flush_every == 0 {
            let flush_start = Instant::now();
            writer
                .flush_pending(flush_every)
                .await
                .expect("shared-store async flush should publish");
            flush_latencies.push(flush_start.elapsed());
        }
        let replay = replicator
            .replay_oplog(shard_id, last_replayed, &follower)
            .await
            .expect("shared-store follower replay should succeed");
        last_replayed = replay.last_oplog_index;
        max_lag = max_lag.max(last_written.saturating_sub(last_replayed));
        if last_replayed == last_written {
            let read_start = Instant::now();
            let read = follower.execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet { key },
            });
            read_latencies.push(read_start.elapsed());
            assert_eq!(read.response, CommandResponse::Bytes { value: Some(value) });
        }
    }

    if mode == SharedStoreStorageMode::Async || writer.queued_len() > 0 {
        let flush_start = Instant::now();
        writer
            .flush_pending(usize::MAX)
            .await
            .expect("final shared-store async flush should publish");
        flush_latencies.push(flush_start.elapsed());
    }
    let replay = replicator
        .replay_oplog(shard_id, last_replayed, &follower)
        .await
        .expect("final shared-store replay should succeed");
    last_replayed = replay.last_oplog_index;
    max_lag = max_lag.max(last_written.saturating_sub(last_replayed));

    SharedStoreModeReport {
        primary_write_latency: summarize_latencies(&primary_write_latencies),
        storage_write_latency: summarize_latencies(&storage_write_latencies),
        flush_latency: summarize_latencies(&flush_latencies),
        read_latency: summarize_latencies(&read_latencies),
        max_lag,
    }
}

fn summarize_latencies(values: &[Duration]) -> LatencySummary {
    if values.is_empty() {
        return LatencySummary::default();
    }
    let mut micros = values
        .iter()
        .map(|duration| duration.as_micros())
        .collect::<Vec<_>>();
    micros.sort_unstable();
    LatencySummary {
        samples: micros.len(),
        p50_us: percentile(&micros, 50),
        p95_us: percentile(&micros, 95),
        p99_us: percentile(&micros, 99),
        max_us: *micros.last().unwrap_or(&0),
    }
}

fn percentile(sorted: &[u128], pct: usize) -> u128 {
    let index = ((sorted.len().saturating_sub(1)) * pct) / 100;
    sorted[index]
}

fn parse_bool(value: &str, key: &str) -> bool {
    match value {
        "1" | "true" | "TRUE" | "yes" | "YES" => true,
        "0" | "false" | "FALSE" | "no" | "NO" => false,
        _ => {
            eprintln!("invalid {key} boolean value {value:?}");
            std::process::exit(2);
        }
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[derive(Debug)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }
}
