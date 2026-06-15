use serde::{Deserialize, Serialize};

use crate::raft::distributed_raft_readiness;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessArea {
    pub area: String,
    pub ready: bool,
    pub covered: Vec<String>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionReadinessReport {
    pub production_ready: bool,
    pub cpp_parity_ready: bool,
    #[serde(default)]
    pub blocker_count: usize,
    #[serde(default)]
    pub failed_areas: Vec<String>,
    #[serde(default)]
    pub failed_capabilities: Vec<ReadinessCapabilityBlocker>,
    pub areas: Vec<ReadinessArea>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessCapabilityBlocker {
    pub area: String,
    pub capability: String,
}

impl ProductionReadinessReport {
    pub fn missing_count(&self) -> usize {
        self.areas.iter().map(|area| area.missing.len()).sum()
    }

    pub fn missing_by_area(&self, area: &str) -> Option<&[String]> {
        self.areas
            .iter()
            .find(|item| item.area == area)
            .map(|item| item.missing.as_slice())
    }

    pub fn exact_failed_capabilities(&self) -> &[ReadinessCapabilityBlocker] {
        self.failed_capabilities.as_slice()
    }
}

pub fn production_readiness_report() -> ProductionReadinessReport {
    let raft = distributed_raft_readiness();
    let areas = vec![
        ReadinessArea {
            area: "raft_replication".to_string(),
            ready: raft.production_ready,
            covered: vec![
                "separate raft_node binary with /raft/propose, /raft/read, /raft/status"
                    .to_string(),
                "HTTP AppendEntries, RequestVote, InstallSnapshot, and chunked snapshot endpoints"
                    .to_string(),
                "majority write checks, follower catch-up, safe reads, promotion, scale up/down"
                    .to_string(),
                "local WAL recovery and local separate-node replication test".to_string(),
            ],
            missing: raft.missing,
        },
        ReadinessArea {
            area: "client".to_string(),
            ready: false,
            covered: vec![
                "typed table client, pipeline, retries/timeouts, route refresh".to_string(),
                "primary routing for writes and optional secondary routing for reads".to_string(),
                "background topology sync and C++ crc64 slot formula".to_string(),
                "client preflight report exposes route/table cache, backend-failure backlog, stats, and options"
                    .to_string(),
            ],
            missing: vec![
                "tonic/prost SDK surface for the open-source production API".to_string(),
                "full C++ partition-set hierarchy and Neptune-specific routing"
                    .to_string(),
                "wire-compatible migration layer for existing C++ client callers".to_string(),
            ],
        },
        ReadinessArea {
            area: "proxy".to_string(),
            ready: false,
            covered: vec![
                "HTTP proxy execute/batch routes delegate through TemporalStoreClient for route cache, retries, backend-error refresh, and stats sync"
                    .to_string(),
                "background heartbeat loop and heartbeat auto-register".to_string(),
                "backend-error route refresh and failure-streak avoidance".to_string(),
                "namespace/table open path and table-routed proxy execute/batch routes"
                    .to_string(),
                "C++ service-name JSON aliases for ExecuteCmd, BatchExecuteCmd, OpenTable, and table execute/batch execute"
                    .to_string(),
                "C++ service-name admin aliases expose proxy info, heartbeat/config, and embedded client preflight"
                    .to_string(),
                "Rust-native service discovery replacement for consul via proxy auto-register, heartbeat TTL, admin inspection, and Prometheus stale/registered metrics"
                    .to_string(),
            ],
            missing: vec![
                "tonic proxy service and streaming/callback request shape".to_string(),
                "brpc/thrift wire-compatible command-specific proxy methods such as Get, Set, FeatureAdd, RiskHset, HMGet, HMSet, HGetAll, and HLen"
                    .to_string(),
            ],
        },
        ReadinessArea {
            area: "metaserver".to_string(),
            ready: false,
            covered: vec![
                "server/proxy inventory, heartbeat, namespace/table topology, meta stats/info"
                    .to_string(),
                "optional in-process Raft-backed mutation path".to_string(),
                "load-aware replica placement skeleton with location and host diversity"
                    .to_string(),
                "single-node metabase snapshot export/import and atomic local snapshot save/load"
                    .to_string(),
                "HTTP scheduler admin surface can submit, run-next, snapshot, and restore deterministic metaserver tasks"
                    .to_string(),
                "optional local scheduler snapshot persistence through TS_META_SCHEDULER_SNAPSHOT"
                    .to_string(),
                "metaserver preflight is exposed for both single-node and Raft-backed metadata runtimes, including MasterService aliases"
                    .to_string(),
            ],
            missing: vec![
                "networked multi-process metaserver Raft".to_string(),
                "C++ partition-set/member/version topology model".to_string(),
                "full background scheduler loop executing repair tasks against real data-node processes, cooldowns, and safe mode"
                    .to_string(),
                "durable shard membership changes coupled to data-node Raft groups".to_string(),
            ],
        },
        ReadinessArea {
            area: "data_node_distributed_raft".to_string(),
            ready: false,
            covered: vec![
                "separate raft_node binary can accept /raft/propose and peer raft messages"
                    .to_string(),
                "local model covers majority commit, catch-up, failover, safe scale up/down"
                    .to_string(),
                "data Raft supports C++ replicator-style bounded follower catch-up with lag/progress reports"
                    .to_string(),
                "production Raft timer loop uses bounded follower catch-up per heartbeat"
                    .to_string(),
                "WAL-backed local Raft state persists commits, leadership, and membership"
                    .to_string(),
                "local Raft WAL supports segmented append, sync, segment retention, ordered recovery, and corrupt-tail truncation"
                    .to_string(),
                "WAL-backed data Raft persists installed snapshot payload/floor so restart can recover trimmed pre-snapshot state"
                    .to_string(),
                "data-node Raft exposes planned add/remove/replace membership changes with joint-consensus, catch-up, quorum checks, and reports"
                    .to_string(),
                "metaserver Raft exposes matching planned add/remove/replace membership changes with catch-up, quorum checks, and reports"
                    .to_string(),
                "metaserver table topology can be converted into data-node Raft voter membership plans with no-op detection and server-state validation"
                    .to_string(),
                "metaserver topology membership plans can be applied to data-node Raft with joint-consensus catch-up and an applied/no-op report"
                    .to_string(),
                "raft_node and raft-enabled server expose /raft/membership/apply for networked safe membership changes, and the OS-process harness verifies scale down/up through that route"
                    .to_string(),
                "C++-style deterministic metaserver task scheduler model covers priority ordering, retry-later backoff, abort, and UpdateMembership task enqueue"
                    .to_string(),
                "scheduler task queue can be snapshotted/restored and freezing shard replicas can be repaired into UpdateMembership tasks"
                    .to_string(),
                "chunked snapshot message assembly and stale snapshot rejection are tested"
                    .to_string(),
                "distributed append failures fall back to post-commit snapshot install for lagging followers"
                    .to_string(),
                "external snapshot transfer policy, leader upload, metaserver snapshot-ref recording, URI download, install, and Raft catch-up are tested"
                    .to_string(),
                "metaserver Raft has a production runtime wrapper with validation, status, failover/catch-up timer, and stale-server detection"
                    .to_string(),
                "local OS-process raft_node harness covers secondary restart catch-up and surviving-node failover after leader crash"
                    .to_string(),
                "local OS-process raft_node harness covers network partition stale-read rejection, heal, and follower catch-up"
                    .to_string(),
                "local OS-process raft_node harness covers lagging-follower observation, majority-side writes, heal, and catch-up reads"
                    .to_string(),
                "local OS-process raft_node harness covers rolling restart of every voter with WAL recovery and post-restart replication"
                    .to_string(),
            ],
            missing: vec![
                "real OpenRaft or raft-rs data-node FSM/storage implementation".to_string(),
                "networked metaserver Raft transport and scheduler loop that automatically drives /raft/membership/apply across real data-node processes and persists task state"
                    .to_string(),
                "external multi-process packet-loss and disk-pressure tests".to_string(),
            ],
        },
        ReadinessArea {
            area: "dataserver".to_string(),
            ready: false,
            covered: vec![
                "TemporalEngine command execution, checked execute/batch, runtime queue"
                    .to_string(),
                "async jobs, cancellation status surface, dirty-object reporting".to_string(),
                "page-address persistence and Prometheus metrics endpoint".to_string(),
                "load rejects duplicate loaded shards and unload removes shard metadata with not-found semantics"
                    .to_string(),
                "config get/set follows C++ partition-map not-found semantics".to_string(),
                "data-node membership update rejects stale global/unit versions and reports whether local replica remains active"
                    .to_string(),
                "data-node runtime uses shard-affine worker lanes so one partition has FIFO single-lane execution while different partitions run in parallel"
                    .to_string(),
                "data-node scheduler prioritizes foreground execute work over background dump/compact/GC and applies a separate background queue admission limit"
                    .to_string(),
                "dirty shards can be discovered and scheduled as background dump tasks".to_string(),
                "stoppable periodic dirty-dump scheduler submits dirty shard dumps through the background queue without duplicating already queued shard dumps"
                    .to_string(),
                "stoppable expiry sweep scheduler removes expired records from loaded shards and reports sweep/removal counters"
                    .to_string(),
                "background dump/compact/GC honor in-flight cancellation checkpoints before destructive phases"
                    .to_string(),
                "local partition_info and object-manager stats report logical objects, page refs, dirty objects, dirty routing slots, and storage bytes"
                    .to_string(),
                "local shard/table/tenant read/write QPS admission is enforced from Config quota fields"
                    .to_string(),
                "C++ ServerService admin aliases expose runtime stats, preflight, dirty-object, and queued-worker state"
                    .to_string(),
            ],
            missing: vec![
                "tonic/gRPC data-node service and streaming callbacks".to_string(),
                "distributed admission policy shared across data-node processes".to_string(),
                "crash-safe recovery tests for oplog + index-log + page stream".to_string(),
            ],
        },
        ReadinessArea {
            area: "fault_tolerance".to_string(),
            ready: false,
            covered: vec![
                "local majority-loss rejection for reads and writes".to_string(),
                "local primary crash promotion and recovered follower catch-up".to_string(),
                "local stale candidate, lagging read-index, and stale snapshot guards".to_string(),
                "kill switches for proxy reads/writes, replication, async storage, and scale changes"
                    .to_string(),
            ],
            missing: vec![
                "external chaos suite for process kill, restart, network partition, packet loss, and disk full"
                    .to_string(),
                "crash-safe recovery proof across Raft WAL, oplog, index-log, and page files"
                    .to_string(),
                "rolling restart and rolling upgrade validation for proxy, client, metaserver, and data-node"
                    .to_string(),
                "production alerting/runbooks for stuck replica, split brain risk, slow follower, and disk pressure"
                    .to_string(),
            ],
        },
        ReadinessArea {
            area: "storage_cache".to_string(),
            ready: false,
            covered: vec![
                "local page segment files with page-address indexes".to_string(),
                "local page compaction rolls to a fresh segment, rewrites live page references, persists the compacted index, and makes old segments GC-eligible"
                    .to_string(),
                "memory plus disk read-through cache with zstd block envelope".to_string(),
                "shared-store checkpoint/oplog replay model and GC tests".to_string(),
                "storage recovery integrity report summarizes indexed/discovered/live/orphan/corrupt segments, stale refs, unreadable bytes, and ownership mismatches"
                    .to_string(),
                "storage lifecycle plan ranks reclaim candidates by stale bytes, live-ref density, orphan status, and delayed-destroy pressure"
                    .to_string(),
                "slot-dump install preflight reports stale sequence, missing/corrupt segments, unreadable refs, and safe install status before marker writes"
                    .to_string(),
                "shard index persistence and slot-dump install use temp-file fsync plus rename instead of direct overwrite"
                    .to_string(),
                "memory cache supports pinned hot/page blocks with eviction skip accounting, inspection, and Prometheus metrics"
                    .to_string(),
            ],
            missing: vec![
                "binary/protobuf oplog and index-log compatibility".to_string(),
                "atomic dump/load/install pipeline plus C++ zone/page-header compaction parity"
                    .to_string(),
                "production SSD cache admission, eviction, warmup, pinning, and observability"
                    .to_string(),
                "ByteStore/S3 live backend integration tied to follower cursors/Raft snapshots"
                    .to_string(),
            ],
        },
        ReadinessArea {
            area: "feature_modules".to_string(),
            ready: false,
            covered: vec![
                "common/string/hash/set plus Redis compatibility subset".to_string(),
                "feature append/query/replace/delete/agg and 5k sequence test".to_string(),
                "IPS load/snapshot/stat/filter subset and Risk subset with typed client and RESP coverage"
                    .to_string(),
            ],
            missing: vec![
                "exact C++ Feature nested point/proto semantics and aggregate edge cases"
                    .to_string(),
                "IPS production snap metadata and server aggregation behavior".to_string(),
                "Risk production CPC/list internals and full manager/debug APIs".to_string(),
                "C++ golden corpus compatibility suite".to_string(),
            ],
        },
        ReadinessArea {
            area: "deployment_ops".to_string(),
            ready: false,
            covered: vec![
                "Docker and existing-EKS Terraform skeleton".to_string(),
                "Prometheus text metrics for core local surfaces".to_string(),
                "local scale harness".to_string(),
                "C++-style membership update task model filters sibling replicas, applies success thresholds, treats not_found as acceptable reboot state, and gates FSM submit"
                    .to_string(),
            ],
            missing: vec![
                "autoscale controller and metaserver-driven shard rebalance loop".to_string(),
                "rolling upgrade and rollback runbooks".to_string(),
                "dashboards, alerts, tracing, auth/TLS for all service APIs".to_string(),
                "AWS multi-node E2E and performance benchmarks".to_string(),
            ],
        },
        ReadinessArea {
            area: "scale_testing".to_string(),
            ready: false,
            covered: vec![
                "local in-process scale_harness exercises writes, sampled replica reads, failover, and scale events"
                    .to_string(),
                "long sequence rows are covered in unit tests and in the scale harness".to_string(),
                "existing-EKS Terraform and Redis load script skeletons exist".to_string(),
            ],
            missing: vec![
                "multi-node AWS scale test that runs real metaserver, proxy, client, and data-node processes"
                    .to_string(),
                "distributed Raft scale test that verifies lag, catch-up, election, and membership under load"
                    .to_string(),
                "C++ workload replay/golden corpus for feature, IPS, Risk, Redis, and admin APIs"
                    .to_string(),
                "latency/throughput SLO report with p50/p95/p99, error budget, CPU, memory, disk, and network"
                    .to_string(),
            ],
        },
    ];
    let production_ready = areas.iter().all(|area| area.ready);
    let failed_areas = areas
        .iter()
        .filter(|area| !area.ready || !area.missing.is_empty())
        .map(|area| area.area.clone())
        .collect::<Vec<_>>();
    let failed_capabilities = areas
        .iter()
        .flat_map(|area| {
            area.missing
                .iter()
                .map(|capability| ReadinessCapabilityBlocker {
                    area: area.area.clone(),
                    capability: capability.clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let blocker_count = failed_capabilities.len();
    ProductionReadinessReport {
        production_ready,
        cpp_parity_ready: production_ready,
        blocker_count,
        failed_areas,
        failed_capabilities,
        areas,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_readiness_report_lists_blockers_for_all_major_services() {
        let report = production_readiness_report();
        assert!(!report.production_ready);
        assert!(!report.cpp_parity_ready);
        assert_eq!(report.blocker_count, report.missing_count());
        assert_eq!(report.blocker_count, report.failed_capabilities.len());
        assert!(report.failed_areas.contains(&"storage_cache".to_string()));
        assert!(report.failed_capabilities.iter().any(|blocker| {
            blocker.area == "storage_cache" && blocker.capability.contains("ByteStore")
        }));
        let proxy = report
            .areas
            .iter()
            .find(|area| area.area == "proxy")
            .expect("proxy area must exist");
        assert!(proxy
            .covered
            .iter()
            .any(|item| item.contains("service discovery replacement")));
        assert!(!proxy.missing.iter().any(|item| item.contains("consul")));
        for area in [
            "raft_replication",
            "client",
            "proxy",
            "metaserver",
            "data_node_distributed_raft",
            "dataserver",
            "fault_tolerance",
            "storage_cache",
            "feature_modules",
            "deployment_ops",
            "scale_testing",
        ] {
            let missing = report.missing_by_area(area).expect("area must exist");
            assert!(
                !missing.is_empty(),
                "{area} should list production blockers"
            );
        }
        assert!(report.missing_count() >= 20);
    }

    #[test]
    fn production_readiness_report_calls_out_distributed_data_node_and_scale_blockers() {
        let report = production_readiness_report();

        let data_raft = report
            .missing_by_area("data_node_distributed_raft")
            .expect("data-node raft area must exist");
        assert!(data_raft
            .iter()
            .any(|item| item.contains("OpenRaft") || item.contains("raft-rs")));
        assert!(data_raft
            .iter()
            .any(|item| item.contains("packet-loss") || item.contains("disk-pressure")));

        let covered = &report
            .areas
            .iter()
            .find(|area| area.area == "data_node_distributed_raft")
            .expect("data-node raft area must exist")
            .covered;
        assert!(covered
            .iter()
            .any(|item| item.contains("external snapshot transfer policy")));
        assert!(covered
            .iter()
            .any(|item| item.contains("network partition stale-read rejection")));
        assert!(covered
            .iter()
            .any(|item| item.contains("lagging-follower observation")));
        assert!(covered
            .iter()
            .any(|item| item.contains("rolling restart of every voter")));

        let scale = report
            .missing_by_area("scale_testing")
            .expect("scale testing area must exist");
        assert!(scale.iter().any(|item| item.contains("AWS scale test")));
        assert!(scale.iter().any(|item| item.contains("p50/p95/p99")));
        assert!(report
            .exact_failed_capabilities()
            .iter()
            .any(|blocker| blocker.area == "scale_testing"
                && blocker.capability.contains("latency/throughput")));

        let fault_tolerance = report
            .missing_by_area("fault_tolerance")
            .expect("fault tolerance area must exist");
        assert!(fault_tolerance
            .iter()
            .any(|item| item.contains("disk full")));
    }
}
