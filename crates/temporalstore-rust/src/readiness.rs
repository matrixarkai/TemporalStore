use serde::{Deserialize, Serialize};

use crate::ingestion::ingestion_readiness_report;
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
    #[serde(default)]
    pub service_summaries: Vec<ServiceReadinessSummary>,
    pub areas: Vec<ReadinessArea>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessCapabilityBlocker {
    pub area: String,
    pub capability: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceReadinessSummary {
    pub service: String,
    pub ready: bool,
    pub areas: Vec<String>,
    pub blocker_count: usize,
    #[serde(default)]
    pub blocker_classes: Vec<String>,
    #[serde(default)]
    pub next_action: String,
    pub failed_capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceReadinessGateReport {
    pub service: String,
    pub ready: bool,
    pub gate_status: String,
    pub severity: String,
    pub remediation_order: usize,
    pub owner: String,
    pub areas: Vec<String>,
    pub blocker_count: usize,
    pub blocker_classes: Vec<String>,
    pub next_action: String,
    #[serde(default)]
    pub primary_blocker: Option<ReadinessCapabilityBlocker>,
    pub failed_capabilities: Vec<ReadinessCapabilityBlocker>,
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

    pub fn service_summary(&self, service: &str) -> Option<&ServiceReadinessSummary> {
        self.service_summaries
            .iter()
            .find(|summary| summary.service == service)
    }

    pub fn service_ready(&self, service: &str) -> bool {
        self.service_summary(service)
            .map(|summary| summary.ready && summary.blocker_count == 0)
            .unwrap_or(false)
    }

    pub fn blocked_services(&self) -> Vec<&ServiceReadinessSummary> {
        self.service_summaries
            .iter()
            .filter(|summary| !summary.ready || summary.blocker_count > 0)
            .collect()
    }

    pub fn known_services(&self) -> Vec<&str> {
        self.service_summaries
            .iter()
            .map(|summary| summary.service.as_str())
            .collect()
    }

    pub fn failed_capabilities_for_service(
        &self,
        service: &str,
    ) -> Vec<&ReadinessCapabilityBlocker> {
        let Some(summary) = self.service_summary(service) else {
            return Vec::new();
        };
        self.failed_capabilities
            .iter()
            .filter(|blocker| summary.areas.iter().any(|area| area == &blocker.area))
            .collect()
    }

    pub fn service_gate_report(&self, service: &str) -> Option<ServiceReadinessGateReport> {
        let summary = self.service_summary(service)?;
        let ready = self.service_ready(service);
        let failed_capabilities = self
            .failed_capabilities_for_service(service)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        Some(ServiceReadinessGateReport {
            service: summary.service.clone(),
            ready,
            gate_status: if ready { "ready" } else { "blocked" }.to_string(),
            severity: service_gate_severity(ready, summary.blocker_count).to_string(),
            remediation_order: self
                .known_services()
                .iter()
                .position(|known| *known == service)
                .map(|index| index + 1)
                .unwrap_or(0),
            owner: service_owner(service).to_string(),
            areas: summary.areas.clone(),
            blocker_count: summary.blocker_count,
            blocker_classes: summary.blocker_classes.clone(),
            next_action: summary.next_action.clone(),
            primary_blocker: failed_capabilities.first().cloned(),
            failed_capabilities,
        })
    }

    pub fn service_gate_reports(&self) -> Vec<ServiceReadinessGateReport> {
        self.known_services()
            .into_iter()
            .filter_map(|service| self.service_gate_report(service))
            .collect()
    }

    pub fn next_blocked_service(&self) -> Option<ServiceReadinessGateReport> {
        self.service_gate_reports()
            .into_iter()
            .filter(|gate| !gate.ready)
            .min_by_key(|gate| gate.remediation_order)
    }
}

pub fn production_readiness_report() -> ProductionReadinessReport {
    let raft = distributed_raft_readiness();
    let ingestion = ingestion_readiness_report();
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
                "client retry classifier separates budget-free safe topology retries from unsafe write retries that require explicit write retry budget"
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
                "C++ command-shaped proxy HTTP/JSON aliases cover Get, Set, FeatureAdd, RiskHset, HMGet, HMSet, HGetAll, and HLen through the normal routed client path"
                    .to_string(),
                "Rust-native service discovery replacement for consul via proxy auto-register, heartbeat TTL, admin inspection, and Prometheus stale/registered metrics"
                    .to_string(),
            ],
            missing: vec![
                "tonic proxy service and streaming/callback request shape".to_string(),
                "brpc/thrift wire-compatible command-specific proxy transport for existing C++ callers"
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
                "chunked timestamped KV commands round-trip through the data-Raft command codec and snapshot install rebuilds packed page layout"
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
                "crash recovery reports and tests cover oplog, index-log, page stream, and zone-manifest ordering"
                    .to_string(),
            ],
            missing: vec![
                "tonic/gRPC data-node service and streaming callbacks".to_string(),
                "distributed admission policy shared across data-node processes".to_string(),
            ],
        },
        ReadinessArea {
            area: "ingestion".to_string(),
            ready: ingestion.production_ready,
            covered: ingestion.covered,
            missing: ingestion.missing,
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
                "local combined recovery proof covers Raft WAL restore plus oplog, index-log, page-file, and packed timestamped KV recovery"
                    .to_string(),
                "Prometheus alert rules and fault runbook cover stuck replica, split-brain risk, slow follower, and storage pressure triage"
                    .to_string(),
                "external_chaos_gate composes OS-process Raft kill/restart, stale-read partition, lag/heal, rolling restart, networked membership/snapshot, and storage replay harnesses"
                    .to_string(),
            ],
            missing: vec![
                "rolling restart and rolling upgrade validation for proxy, client, metaserver, and data-node"
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
                "storage lifecycle cache warmup returns selected slots, page-ref hits/fills/failures, page-store read count, and warmed bytes"
                    .to_string(),
                "storage production report exposes Rust JSONL oplog/index-log format, replay-safe status, sequence/record/byte counts, and C++ binary compatibility gaps"
                    .to_string(),
                "storage production report exposes Rust page-envelope version, checksum/object-id/routing-slot/compression support, zone bytes, and C++ page-header compatibility gaps"
                    .to_string(),
                "chunked timestamped KV page format is covered by sync and async shared-store replay plus Raft follower-read replication tests"
                    .to_string(),
                "chunked timestamped KV page recovery strictly rejects malformed or unsupported packed-page payloads"
                    .to_string(),
            ],
            missing: vec![
                "binary/protobuf oplog and index-log compatibility".to_string(),
                "atomic dump/load/install pipeline plus C++ zone/page-header compaction parity"
                    .to_string(),
                "production SSD cache tiering policy, admission tuning, and live pressure validation"
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
                "feature writes pack many timestamp/value entries into one page and the timestamp index shares that page address, matching the C++ storage shape"
                    .to_string(),
                "feature aggregate query covers count/events/sum/avg/min/max/first/last over selected timestamp windows"
                    .to_string(),
                "large timestamped KV writes split into persisted page chunks while preserving per-timestamp reads"
                    .to_string(),
                "oversized single timestamped values remain readable as one packed page without creating empty chunks"
                    .to_string(),
                "feature/sequence C++ protobuf golden corpus exercises filters, aggregates, sequence queries, and packed timestamped KV page layout"
                    .to_string(),
                "full Rust-local C++ API golden corpus covers feature, sequence, IPS, Risk, Redis-compatible core commands, and admin storage readiness"
                    .to_string(),
                "IPS load/snapshot/stat/filter subset and Risk subset with typed client and RESP coverage"
                    .to_string(),
                "IPS production snapshot report exposes range metadata, returned versus total counts, action/table server aggregations, and packed timestamped page evidence"
                    .to_string(),
                "Risk debug report exposes H/CPC/FOL full and window counters plus FOL selection metadata through engine, typed client, and RESP"
                    .to_string(),
            ],
            missing: vec![
                "exact C++ Feature nested point/proto semantics and deployment-specific time-range edge cases"
                    .to_string(),
                "Risk production CPC/list internals and deployment-specific manager/debug APIs"
                    .to_string(),
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
                "rolling upgrade and rollback runbook covers metaserver, data-node, proxy, client, storage, ingestion, preflight, quick chaos gate, and audit artifacts"
                    .to_string(),
            ],
            missing: vec![
                "autoscale controller and metaserver-driven shard rebalance loop".to_string(),
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
    let service_summaries = service_readiness_summaries(&areas);
    ProductionReadinessReport {
        production_ready,
        cpp_parity_ready: production_ready,
        blocker_count,
        failed_areas,
        failed_capabilities,
        service_summaries,
        areas,
    }
}

fn service_readiness_summaries(areas: &[ReadinessArea]) -> Vec<ServiceReadinessSummary> {
    [
        ("client", vec!["client"]),
        ("proxy", vec!["proxy"]),
        ("ingestion", vec!["ingestion"]),
        (
            "data_node",
            vec!["dataserver", "data_node_distributed_raft"],
        ),
        ("metaserver", vec!["metaserver"]),
    ]
    .into_iter()
    .map(|(service, area_names)| {
        let selected = area_names
            .iter()
            .filter_map(|name| areas.iter().find(|area| area.area == *name))
            .collect::<Vec<_>>();
        let failed_capabilities = selected
            .iter()
            .flat_map(|area| {
                area.missing
                    .iter()
                    .map(|capability| format!("{}: {capability}", area.area))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let blocker_classes = selected
            .iter()
            .filter(|area| !area.missing.is_empty())
            .map(|area| service_blocker_class(&area.area).to_string())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        ServiceReadinessSummary {
            service: service.to_string(),
            ready: !selected.is_empty()
                && selected
                    .iter()
                    .all(|area| area.ready && area.missing.is_empty()),
            areas: area_names.into_iter().map(str::to_string).collect(),
            blocker_count: failed_capabilities.len(),
            next_action: service_next_action(service, &blocker_classes).to_string(),
            blocker_classes,
            failed_capabilities,
        }
    })
    .collect()
}

fn service_blocker_class(area: &str) -> &'static str {
    match area {
        "client" => "client_sync_preflight",
        "proxy" => "proxy_topology_admission",
        "ingestion" => "ingestion_durability",
        "dataserver" => "data_node_local_lifecycle",
        "data_node_distributed_raft" => "data_node_distributed_raft",
        "metaserver" => "metaserver_control_plane",
        _ => "other",
    }
}

fn service_next_action(service: &str, blocker_classes: &[String]) -> &'static str {
    let Some(first_class) = blocker_classes.first().map(String::as_str) else {
        return "ready";
    };
    match (service, first_class) {
        ("client", "client_sync_preflight") => {
            "finish client MetaSyncer deadlines, stale-route invalidation, and retry classification"
        }
        ("proxy", "proxy_topology_admission") => {
            "finish proxy topology-version guarded cache invalidation and admission policy enforcement"
        }
        ("ingestion", "ingestion_durability") => {
            "finish network Kafka/Flink runtime failover, lag metrics, and dead-letter export"
        }
        ("data_node", "data_node_distributed_raft") => {
            "finish production data-node Raft FSM/storage and distributed fault validation"
        }
        ("data_node", "data_node_local_lifecycle") => {
            "finish data-node lifecycle restart barriers, distributed admission, and crash recovery"
        }
        ("metaserver", "metaserver_control_plane") => {
            "finish networked metaserver Raft, scheduler loop, and safe topology membership mutations"
        }
        _ => "inspect failed capabilities for this service",
    }
}

fn service_owner(service: &str) -> &'static str {
    match service {
        "client" => "client_sdk",
        "proxy" => "proxy_runtime",
        "ingestion" => "ingestion_connectors",
        "data_node" => "data_node_runtime",
        "metaserver" => "metaserver_control_plane",
        _ => "unknown",
    }
}

fn service_gate_severity(ready: bool, blocker_count: usize) -> &'static str {
    if ready {
        "ready"
    } else if blocker_count >= 3 {
        "critical"
    } else {
        "warning"
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
            "ingestion",
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
    fn production_readiness_report_summarizes_requested_service_readiness() {
        let report = production_readiness_report();
        let services = report
            .service_summaries
            .iter()
            .map(|summary| summary.service.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            services,
            vec!["client", "proxy", "ingestion", "data_node", "metaserver"]
        );

        for service in ["client", "proxy", "ingestion", "data_node", "metaserver"] {
            let summary = report
                .service_summary(service)
                .expect("service summary must exist");
            assert!(!summary.ready, "{service} should still have blockers");
            assert!(
                summary.blocker_count > 0,
                "{service} should expose exact failed capabilities"
            );
            assert_eq!(summary.blocker_count, summary.failed_capabilities.len());
            assert!(
                !summary.blocker_classes.is_empty(),
                "{service} should classify blockers for triage"
            );
            assert!(
                !summary.next_action.is_empty() && summary.next_action != "ready",
                "{service} should expose a concrete next action"
            );
            assert_eq!(
                report.failed_capabilities_for_service(service).len(),
                summary.blocker_count,
                "{service} typed blockers should match summary count"
            );
            let gate = report
                .service_gate_report(service)
                .expect("service gate report must exist");
            assert_eq!(gate.service, service);
            assert_eq!(gate.ready, summary.ready);
            assert_eq!(gate.gate_status, "blocked");
            assert_ne!(gate.severity, "ready");
            assert!(gate.remediation_order > 0);
            assert_ne!(gate.owner, "unknown");
            assert_eq!(gate.areas, summary.areas);
            assert_eq!(gate.blocker_count, summary.blocker_count);
            assert_eq!(gate.blocker_classes, summary.blocker_classes);
            assert_eq!(gate.next_action, summary.next_action);
            assert_eq!(
                gate.primary_blocker.as_ref(),
                gate.failed_capabilities.first()
            );
            assert_eq!(gate.failed_capabilities.len(), summary.blocker_count);
            assert!(
                !report.service_ready(service),
                "{service} should be false for service-level gates until blockers are closed"
            );
        }
        let blocked_services = report
            .blocked_services()
            .iter()
            .map(|summary| summary.service.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            blocked_services,
            vec!["client", "proxy", "ingestion", "data_node", "metaserver"]
        );
        assert_eq!(
            report.known_services(),
            vec!["client", "proxy", "ingestion", "data_node", "metaserver"]
        );
        let service_gates = report.service_gate_reports();
        assert_eq!(service_gates.len(), 5);
        assert_eq!(
            service_gates
                .iter()
                .map(|gate| (
                    gate.remediation_order,
                    gate.service.as_str(),
                    gate.owner.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, "client", "client_sdk"),
                (2, "proxy", "proxy_runtime"),
                (3, "ingestion", "ingestion_connectors"),
                (4, "data_node", "data_node_runtime"),
                (5, "metaserver", "metaserver_control_plane")
            ]
        );
        assert!(service_gates.iter().all(|gate| !gate.ready));
        assert!(service_gates
            .iter()
            .all(|gate| gate.gate_status == "blocked"));
        let next_blocked = report
            .next_blocked_service()
            .expect("next blocked service should exist");
        assert_eq!(next_blocked.service, "client");
        assert_eq!(next_blocked.remediation_order, 1);
        assert_eq!(next_blocked.owner, "client_sdk");
        assert_eq!(next_blocked.severity, "critical");
        assert_eq!(
            service_gates
                .iter()
                .map(|gate| (gate.service.as_str(), gate.severity.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("client", "critical"),
                ("proxy", "warning"),
                ("ingestion", "critical"),
                ("data_node", "critical"),
                ("metaserver", "critical")
            ]
        );
        let data_node_gate = service_gates
            .iter()
            .find(|gate| gate.service == "data_node")
            .expect("data node gate should exist");
        assert_eq!(
            data_node_gate.areas,
            vec![
                "dataserver".to_string(),
                "data_node_distributed_raft".to_string()
            ]
        );
        assert!(!report.service_ready("unknown_service"));
        assert!(report.service_gate_report("unknown_service").is_none());

        assert_eq!(
            report
                .service_summary("client")
                .expect("client summary")
                .blocker_classes,
            vec!["client_sync_preflight".to_string()]
        );
        assert!(report
            .service_summary("client")
            .expect("client summary")
            .next_action
            .contains("MetaSyncer"));
        assert!(report
            .service_summary("proxy")
            .expect("proxy summary")
            .next_action
            .contains("topology-version"));
        assert!(report
            .service_summary("ingestion")
            .expect("ingestion summary")
            .next_action
            .contains("Kafka/Flink"));
        assert!(report
            .service_summary("metaserver")
            .expect("metaserver summary")
            .next_action
            .contains("scheduler"));
        assert!(report
            .service_summary("data_node")
            .expect("data node summary")
            .next_action
            .contains("Raft"));
        assert_eq!(
            report
                .service_summary("proxy")
                .expect("proxy summary")
                .blocker_classes,
            vec!["proxy_topology_admission".to_string()]
        );
        assert_eq!(
            report
                .service_summary("ingestion")
                .expect("ingestion summary")
                .blocker_classes,
            vec!["ingestion_durability".to_string()]
        );
        assert_eq!(
            report
                .service_summary("metaserver")
                .expect("metaserver summary")
                .blocker_classes,
            vec!["metaserver_control_plane".to_string()]
        );

        let data_node = report
            .service_summary("data_node")
            .expect("data node summary must exist");
        assert_eq!(
            data_node.areas,
            vec![
                "dataserver".to_string(),
                "data_node_distributed_raft".to_string()
            ]
        );
        assert!(data_node
            .failed_capabilities
            .iter()
            .any(|capability| capability.starts_with("dataserver:")));
        assert!(data_node
            .failed_capabilities
            .iter()
            .any(|capability| capability.starts_with("data_node_distributed_raft:")));
        assert_eq!(
            data_node.blocker_classes,
            vec![
                "data_node_distributed_raft".to_string(),
                "data_node_local_lifecycle".to_string()
            ]
        );
        let data_node_blockers = report.failed_capabilities_for_service("data_node");
        assert!(data_node_blockers
            .iter()
            .any(|blocker| blocker.area == "dataserver"));
        assert!(data_node_blockers
            .iter()
            .any(|blocker| blocker.area == "data_node_distributed_raft"));
        assert!(report
            .failed_capabilities_for_service("unknown_service")
            .is_empty());
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

        let fault_tolerance = report
            .areas
            .iter()
            .find(|area| area.area == "fault_tolerance")
            .expect("fault tolerance area must exist");
        assert!(fault_tolerance
            .covered
            .iter()
            .any(|item| item.contains("Prometheus alert rules and fault runbook")));
        assert!(!fault_tolerance
            .missing
            .iter()
            .any(|item| item.contains("production alerting/runbooks")));

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

        let ingestion = report
            .areas
            .iter()
            .find(|area| area.area == "ingestion")
            .expect("ingestion area must exist");
        assert!(ingestion
            .covered
            .iter()
            .any(|item| item.contains("durable Kafka offset ledger")));
        assert!(ingestion
            .missing
            .iter()
            .any(|item| item.contains("consumer group runtime")));

        let fault_tolerance = report
            .areas
            .iter()
            .find(|area| area.area == "fault_tolerance")
            .expect("fault tolerance area must exist");
        assert!(fault_tolerance
            .covered
            .iter()
            .any(|item| item.contains("external_chaos_gate")));
        let fault_missing = report
            .missing_by_area("fault_tolerance")
            .expect("fault tolerance missing list must exist");
        assert!(fault_missing
            .iter()
            .any(|item| item.contains("rolling restart")));
        let distributed_raft = report
            .missing_by_area("data_node_distributed_raft")
            .expect("distributed raft area must exist");
        assert!(distributed_raft
            .iter()
            .any(|item| item.contains("packet-loss") || item.contains("disk-pressure")));
    }
}
