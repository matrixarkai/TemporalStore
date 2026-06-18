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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageCacheDependencyMatrixReport {
    pub local_file_store_ready: bool,
    pub shared_store_checkpoint_manifest_ready: bool,
    pub oplog_cursor_retention_ready: bool,
    pub page_segment_manifest_ready: bool,
    pub follower_cursor_retention_ready: bool,
    pub bytestore_live_backend_ready: bool,
    pub s3_live_backend_ready: bool,
    pub local_shared_store_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageSsdCachePressureReadinessReport {
    pub memory_read_through_ready: bool,
    pub disk_block_cache_ready: bool,
    pub admission_eviction_counters_ready: bool,
    pub slot_warmup_ready: bool,
    pub cache_invalidation_ready: bool,
    pub local_tiny_cache_pressure_harness_ready: bool,
    pub production_ssd_tiering_ready: bool,
    pub admission_tuning_ready: bool,
    pub long_running_pressure_validation_ready: bool,
    pub local_pressure_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageMigrationCorpusReadinessReport {
    pub rust_local_corpus_ready: bool,
    pub engine_replay_ready: bool,
    pub shared_store_replay_ready: bool,
    pub raft_read_replay_ready: bool,
    pub unified_runner_ready: bool,
    pub external_cpp_binary_exporter_ready: bool,
    pub ci_published_golden_artifacts_ready: bool,
    pub local_migration_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

pub fn storage_migration_corpus_readiness_report() -> StorageMigrationCorpusReadinessReport {
    let rust_local_corpus_ready = true;
    let engine_replay_ready = true;
    let shared_store_replay_ready = true;
    let raft_read_replay_ready = true;
    let unified_runner_ready = true;
    let external_cpp_binary_exporter_ready = false;
    let ci_published_golden_artifacts_ready = false;
    let local_migration_ready = rust_local_corpus_ready
        && engine_replay_ready
        && shared_store_replay_ready
        && raft_read_replay_ready
        && unified_runner_ready;
    let production_ready = local_migration_ready
        && external_cpp_binary_exporter_ready
        && ci_published_golden_artifacts_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "external C++ binary-artifact exporter plus CI-published golden corpus for the migration-only storage compatibility path"
                .to_string(),
        ]
    };

    StorageMigrationCorpusReadinessReport {
        rust_local_corpus_ready,
        engine_replay_ready,
        shared_store_replay_ready,
        raft_read_replay_ready,
        unified_runner_ready,
        external_cpp_binary_exporter_ready,
        ci_published_golden_artifacts_ready,
        local_migration_ready,
        production_ready,
        missing,
    }
}

pub fn storage_ssd_cache_pressure_readiness_report() -> StorageSsdCachePressureReadinessReport {
    let memory_read_through_ready = true;
    let disk_block_cache_ready = true;
    let admission_eviction_counters_ready = true;
    let slot_warmup_ready = true;
    let cache_invalidation_ready = true;
    let local_tiny_cache_pressure_harness_ready = true;
    let production_ssd_tiering_ready = false;
    let admission_tuning_ready = false;
    let long_running_pressure_validation_ready = false;
    let local_pressure_ready = memory_read_through_ready
        && disk_block_cache_ready
        && admission_eviction_counters_ready
        && slot_warmup_ready
        && cache_invalidation_ready
        && local_tiny_cache_pressure_harness_ready;
    let production_ready = local_pressure_ready
        && production_ssd_tiering_ready
        && admission_tuning_ready
        && long_running_pressure_validation_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "production SSD cache tiering policy, admission tuning, and long-running live pressure validation"
                .to_string(),
        ]
    };

    StorageSsdCachePressureReadinessReport {
        memory_read_through_ready,
        disk_block_cache_ready,
        admission_eviction_counters_ready,
        slot_warmup_ready,
        cache_invalidation_ready,
        local_tiny_cache_pressure_harness_ready,
        production_ssd_tiering_ready,
        admission_tuning_ready,
        long_running_pressure_validation_ready,
        local_pressure_ready,
        production_ready,
        missing,
    }
}

pub fn storage_cache_dependency_matrix_report() -> StorageCacheDependencyMatrixReport {
    let local_file_store_ready = true;
    let shared_store_checkpoint_manifest_ready = true;
    let oplog_cursor_retention_ready = true;
    let page_segment_manifest_ready = true;
    let follower_cursor_retention_ready = true;
    let bytestore_live_backend_ready = false;
    let s3_live_backend_ready = false;
    let local_shared_store_ready = local_file_store_ready
        && shared_store_checkpoint_manifest_ready
        && oplog_cursor_retention_ready
        && page_segment_manifest_ready
        && follower_cursor_retention_ready;
    let production_ready =
        local_shared_store_ready && bytestore_live_backend_ready && s3_live_backend_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "live ByteStore/S3 object-store manifest dependency matrix tied to follower cursors and Raft snapshots"
                .to_string(),
        ]
    };

    StorageCacheDependencyMatrixReport {
        local_file_store_ready,
        shared_store_checkpoint_manifest_ready,
        oplog_cursor_retention_ready,
        page_segment_manifest_ready,
        follower_cursor_retention_ready,
        bytestore_live_backend_ready,
        s3_live_backend_ready,
        local_shared_store_ready,
        production_ready,
        missing,
    }
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
    let storage_cache_dependency_matrix = storage_cache_dependency_matrix_report();
    let storage_ssd_cache_pressure = storage_ssd_cache_pressure_readiness_report();
    let storage_migration_corpus = storage_migration_corpus_readiness_report();
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
                "raft_node, raft-enabled server, and metaserver process startup select ProductionRaftEngineKind::OpenRaft by default"
                    .to_string(),
                "RaftStorageApplyFence is persisted in WAL records and rejects missing, corrupt, stale, or ahead-of-storage recovery state"
                    .to_string(),
                "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, and snapshot lifecycle reporting while keeping real storage-mutation and snapshot-install atomic commit integration fail-closed"
                    .to_string(),
                "RaftSnapshotInstallReport exposes freeze, flush, manifest verify, checksum verify, install, tail replay, and rollback status for snapshot installs"
                    .to_string(),
                "ProductionMetaRaftRuntime can drive a data-Raft membership workflow for learner add, catch-up verification, promotion, leader transfer, and voter removal"
                    .to_string(),
                "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, authenticated HTTP transport, and plaintext-only local chaos guardrails while keeping service-process mTLS enforcement fail-closed"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, and storage replay gates while keeping external packet-loss/disk-pressure/process-chaos fail-closed"
                    .to_string(),
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
                "shared C++/Rust corpus runs through the typed table client API and direct engine path for common, feature, sequence, IPS, risk, context, and restart reads"
                    .to_string(),
                "versioned Rust-native SDK contract committed in proto/temporalstore/v1 with validation in the local parity gate"
                    .to_string(),
                "tonic/prost client and server binding types are generated at build time from the committed v1 schema"
                    .to_string(),
                "generated tonic Execute and BatchExecute service adapters convert protobuf commands and delegate to the existing engine execution path"
                    .to_string(),
                "generated tonic OpenTable, SyncTopology, and ClientPreflight adapters delegate to the existing TemporalStoreClient table, topology, and preflight paths"
                    .to_string(),
                "client preflight exposes a C++-style partition-set compatibility view derived from cached table ranges and shard routes"
                    .to_string(),
            ],
            missing: vec![
                "Neptune-specific routing and deployment-specific partition placement policies"
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
                "OpenRaft-backed data-node and metaserver adapter is available behind the openraft-engine feature with durable log state, state-machine apply, snapshot metadata, read-index checks, membership changes, leader transfer, and restart recovery tests"
                    .to_string(),
                "raft_node, raft-enabled server, and metaserver process startup wire the production runtime options to ProductionRaftEngineKind::OpenRaft"
                    .to_string(),
                "RaftStorageApplyFence persists shard, term, committed/applied index, snapshot id, storage epoch, and checksum with WAL recovery validation"
                    .to_string(),
                "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, and snapshot lifecycle reporting while keeping real storage-mutation and snapshot-install atomic commit integration fail-closed"
                    .to_string(),
                "Raft snapshot lifecycle reports install, tail replay, and rollback decisions for data-node snapshot recovery paths"
                    .to_string(),
                "metaserver-owned data-Raft membership workflow reports learner add, catch-up verification, promotion, leader transfer, and voter removal"
                    .to_string(),
                "Raft metaserver membership readiness covers topology membership plans, data-Raft apply reports, learner catch-up/promotion, leader transfer, and voter removal while keeping networked scheduler transport and persisted real-group execution fail-closed"
                    .to_string(),
                "ByteRaft-style leader write authority, ReadIndex guards, learner catch-up/promotion checks, and fail-closed stale leader-transfer checks are modeled locally"
                    .to_string(),
                "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, authenticated HTTP transport, and plaintext-only local chaos guardrails while keeping service-process mTLS enforcement fail-closed"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, and storage replay gates while keeping external packet-loss/disk-pressure/process-chaos fail-closed"
                    .to_string(),
            ],
            missing: vec![
                "persist the data-node applied Raft index atomically with storage mutations and partition snapshot install"
                    .to_string(),
                "networked metaserver Raft transport and scheduler loop that automatically drives /raft/membership/apply across real data-node processes and persists task state"
                    .to_string(),
                "make metaserver own learner add, catch-up verification, promotion, leader movement, and voter removal against real data-node Raft groups"
                    .to_string(),
                "production mTLS transport implementation instead of validation-only config"
                    .to_string(),
                "external multi-process packet-loss, disk-pressure, and process-chaos tests"
                    .to_string(),
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
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, and storage replay gates while keeping external packet-loss/disk-pressure/process-chaos fail-closed"
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
                "storage compatibility decision is explicit: Rust log/page formats are migration-only versus C++ binary logs/page headers, with golden conversion/replay required before C++ migration"
                    .to_string(),
                "chunked timestamped KV page format is covered by sync and async shared-store replay plus Raft follower-read replication tests"
                    .to_string(),
                "chunked timestamped KV page recovery strictly rejects malformed or unsupported packed-page payloads"
                    .to_string(),
                "storage migration corpus converts C++ logical object/page/slot/index/oplog exports into Rust-native pages and replays through engine restart, slot dump, cache warmup, shared-store sync/async replay, and Raft leader-transfer reads"
                    .to_string(),
                "storage migration corpus readiness covers Rust-local converted corpus replay through engine, shared-store, Raft read paths, and the unified C++/Rust runner while keeping external C++ artifact publication fail-closed"
                    .to_string(),
                "local storage production harness combines dump, cache pressure, restart recovery, shared-store replay, and Raft movement into one repeatable gate"
                    .to_string(),
                "local storage dump/load fault matrix harness rejects checksum mismatch, partial manifests, missing segments, stale manifests, restart-during-install recovery, and corrupt page segments"
                    .to_string(),
                "local/shared-store object manifest dependency matrix covers local file objects, checkpoint manifests, oplog cursor retention, page segment manifests, and follower-cursor retention"
                    .to_string(),
                "storage cache dependency matrix keeps live ByteStore/S3 object-store readiness fail-closed"
                    .to_string(),
                "storage SSD cache pressure readiness covers local memory read-through, disk block cache, admission/eviction counters, slot warmup, cache invalidation, and tiny-cache pressure harness evidence"
                    .to_string(),
            ],
            missing: {
                let mut missing = vec![
                    "ByteStore/S3 live backend integration tied to follower cursors/Raft snapshots"
                        .to_string(),
                ];
                missing.extend(storage_migration_corpus.missing.clone());
                missing.extend(storage_cache_dependency_matrix.missing.clone());
                missing.extend(storage_ssd_cache_pressure.missing.clone());
                missing
            },
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
            area: "context_workflow".to_string(),
            ready: false,
            covered: vec![
                "Rust-native Context extraction/retrieval/injection workflow persists ContextNode, ContextEvent, ContextIndexRef, ContextSummaryDirtyMarker, and ContextPackAudit"
                    .to_string(),
                "OpenViking-style L0/L1/L2 context tiers are generated deterministically for local mocked sources"
                    .to_string(),
                "context model provider config can switch between mock and OpenAI-compatible provider shapes without changing API payloads"
                    .to_string(),
                "data-node server exposes /context/extract, /context/retrieve, /context/inject, /context/workflow/state, and provider inspection routes"
                    .to_string(),
                "context workflow harness validates mock extraction, retrieval, prompt injection, audit refs, Docker packaging, and parity-gate log validation"
                    .to_string(),
                "OpenAI-compatible context extraction can call a live HTTP provider with bounded deadlines, retries, Authorization header loaded from an environment variable, JSON response parsing, and fallback provider execution"
                    .to_string(),
            ],
            missing: vec![
                "C++/OpenViking golden context corpus replay through engine, client, proxy, Redis/admin, shared-store, and Raft paths"
                    .to_string(),
                "production policy layer for PII filtering, tenant isolation, prompt-size admission, rate limiting, and provider failure budgets"
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
                "Raft transport security readiness covers validation-only auth/TLS evidence and keeps real service-process mTLS enforcement blocked"
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
        ("storage_cache", vec!["storage_cache"]),
        ("feature_modules", vec!["feature_modules"]),
        ("context_workflow", vec!["context_workflow"]),
        ("fault_tolerance", vec!["fault_tolerance"]),
        ("deployment_ops", vec!["deployment_ops"]),
        ("scale_testing", vec!["scale_testing"]),
        ("raft_replication", vec!["raft_replication"]),
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
        "storage_cache" => "storage_cache_durability",
        "feature_modules" => "feature_module_cpp_parity",
        "context_workflow" => "context_model_provider_parity",
        "fault_tolerance" => "fault_tolerance_validation",
        "deployment_ops" => "deployment_ops_runtime",
        "scale_testing" => "scale_testing_evidence",
        "raft_replication" => "raft_replication_engine",
        _ => "other",
    }
}

fn service_next_action(service: &str, blocker_classes: &[String]) -> &'static str {
    let Some(first_class) = blocker_classes.first().map(String::as_str) else {
        return "ready";
    };
    match (service, first_class) {
        ("client", "client_sync_preflight") => {
            "finish Neptune-specific routing, deployment-specific partition placement policy, and wire-compatible migration for existing C++ client callers"
        }
        ("proxy", "proxy_topology_admission") => {
            "finish proxy topology-version guarded cache invalidation and admission policy enforcement"
        }
        ("ingestion", "ingestion_durability") => {
            "finish network Kafka/Flink runtime failover, lag metrics, and dead-letter export"
        }
        ("data_node", "data_node_distributed_raft") => {
            "finish Raft atomic applied-index storage persistence, metaserver-driven membership, production mTLS, and distributed fault validation"
        }
        ("data_node", "data_node_local_lifecycle") => {
            "finish data-node lifecycle restart barriers, distributed admission, and crash recovery"
        }
        ("metaserver", "metaserver_control_plane") => {
            "finish networked metaserver Raft, scheduler loop, and safe topology membership mutations"
        }
        ("storage_cache", "storage_cache_durability") => {
            "finish golden C++ log/page conversion replay, SSD cache pressure validation, and live object-store integration"
        }
        ("feature_modules", "feature_module_cpp_parity") => {
            "finish exact C++ feature/risk corpus coverage and deployment-specific module edge cases"
        }
        ("context_workflow", "context_model_provider_parity") => {
            "finish C++/OpenViking corpus replay and production policy controls"
        }
        ("fault_tolerance", "fault_tolerance_validation") => {
            "finish rolling restart and rolling upgrade validation across proxy, client, metaserver, and data-node processes"
        }
        ("deployment_ops", "deployment_ops_runtime") => {
            "finish autoscale/rebalance control, dashboards, tracing, auth/TLS, AWS E2E, and performance benchmarks"
        }
        ("scale_testing", "scale_testing_evidence") => {
            "finish multi-node AWS scale tests, distributed Raft load tests, workload replay, and SLO evidence"
        }
        ("raft_replication", "raft_replication_engine") => {
            "finish durable real-process OpenRaft rollout, production mTLS transport, and external chaos coverage"
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
        "storage_cache" => "storage_runtime",
        "feature_modules" => "feature_api",
        "context_workflow" => "context_ai_workflow",
        "fault_tolerance" => "reliability",
        "deployment_ops" => "platform_ops",
        "scale_testing" => "performance",
        "raft_replication" => "consensus_runtime",
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
            "context_workflow",
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
    fn storage_cache_dependency_matrix_splits_local_and_live_store_readiness() {
        let matrix = storage_cache_dependency_matrix_report();
        assert!(matrix.local_file_store_ready);
        assert!(matrix.shared_store_checkpoint_manifest_ready);
        assert!(matrix.oplog_cursor_retention_ready);
        assert!(matrix.page_segment_manifest_ready);
        assert!(matrix.follower_cursor_retention_ready);
        assert!(matrix.local_shared_store_ready);
        assert!(!matrix.bytestore_live_backend_ready);
        assert!(!matrix.s3_live_backend_ready);
        assert!(!matrix.production_ready);
        assert!(matrix
            .missing
            .iter()
            .any(|item| item.contains("ByteStore/S3 object-store manifest dependency matrix")));

        let report = production_readiness_report();
        let storage_cache = report
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .expect("storage cache area must exist");
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("local/shared-store object manifest dependency matrix")));
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("ByteStore/S3 object-store readiness fail-closed")));
        assert!(storage_cache
            .missing
            .iter()
            .any(|item| item.contains("ByteStore/S3 object-store manifest dependency matrix")));
    }

    #[test]
    fn storage_ssd_cache_pressure_report_keeps_production_tier_blocked() {
        let report = storage_ssd_cache_pressure_readiness_report();
        assert!(report.memory_read_through_ready);
        assert!(report.disk_block_cache_ready);
        assert!(report.admission_eviction_counters_ready);
        assert!(report.slot_warmup_ready);
        assert!(report.cache_invalidation_ready);
        assert!(report.local_tiny_cache_pressure_harness_ready);
        assert!(report.local_pressure_ready);
        assert!(!report.production_ssd_tiering_ready);
        assert!(!report.admission_tuning_ready);
        assert!(!report.long_running_pressure_validation_ready);
        assert!(!report.production_ready);
        assert!(report
            .missing
            .iter()
            .any(|item| item.contains("production SSD cache tiering")));

        let readiness = production_readiness_report();
        let storage_cache = readiness
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .expect("storage cache area must exist");
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("storage SSD cache pressure readiness")));
        assert!(storage_cache
            .missing
            .iter()
            .any(|item| item.contains("long-running live pressure validation")));
    }

    #[test]
    fn storage_migration_corpus_report_keeps_external_cpp_export_blocked() {
        let report = storage_migration_corpus_readiness_report();
        assert!(report.rust_local_corpus_ready);
        assert!(report.engine_replay_ready);
        assert!(report.shared_store_replay_ready);
        assert!(report.raft_read_replay_ready);
        assert!(report.unified_runner_ready);
        assert!(report.local_migration_ready);
        assert!(!report.external_cpp_binary_exporter_ready);
        assert!(!report.ci_published_golden_artifacts_ready);
        assert!(!report.production_ready);
        assert!(report
            .missing
            .iter()
            .any(|item| item.contains("external C++ binary-artifact exporter")));

        let readiness = production_readiness_report();
        let storage_cache = readiness
            .areas
            .iter()
            .find(|area| area.area == "storage_cache")
            .expect("storage cache area must exist");
        assert!(storage_cache
            .covered
            .iter()
            .any(|item| item.contains("storage migration corpus readiness")));
        assert!(storage_cache
            .missing
            .iter()
            .any(|item| item.contains("CI-published golden corpus")));
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

        for service in [
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
            "raft_replication",
        ] {
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
        let service_gates = report.service_gate_reports();
        assert_eq!(service_gates.len(), 12);
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
                (5, "metaserver", "metaserver_control_plane"),
                (6, "storage_cache", "storage_runtime"),
                (7, "feature_modules", "feature_api"),
                (8, "context_workflow", "context_ai_workflow"),
                (9, "fault_tolerance", "reliability"),
                (10, "deployment_ops", "platform_ops"),
                (11, "scale_testing", "performance"),
                (12, "raft_replication", "consensus_runtime")
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
        assert_eq!(next_blocked.severity, "warning");
        assert_eq!(
            service_gates
                .iter()
                .map(|gate| (gate.service.as_str(), gate.severity.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("client", "warning"),
                ("proxy", "warning"),
                ("ingestion", "critical"),
                ("data_node", "critical"),
                ("metaserver", "critical"),
                ("storage_cache", "critical"),
                ("feature_modules", "warning"),
                ("context_workflow", "warning"),
                ("fault_tolerance", "warning"),
                ("deployment_ops", "critical"),
                ("scale_testing", "critical"),
                ("raft_replication", "critical")
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
            .contains("Neptune"));
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
        assert!(report
            .service_summary("storage_cache")
            .expect("storage cache summary")
            .next_action
            .contains("SSD cache pressure"));
        assert!(report
            .service_summary("feature_modules")
            .expect("feature modules summary")
            .next_action
            .contains("C++ feature/risk corpus"));
        assert!(report
            .service_summary("context_workflow")
            .expect("context workflow summary")
            .next_action
            .contains("OpenViking corpus"));
        assert!(report
            .service_summary("fault_tolerance")
            .expect("fault tolerance summary")
            .next_action
            .contains("rolling restart"));
        assert!(report
            .service_summary("deployment_ops")
            .expect("deployment ops summary")
            .next_action
            .contains("auth/TLS"));
        assert!(report
            .service_summary("scale_testing")
            .expect("scale testing summary")
            .next_action
            .contains("SLO evidence"));
        assert!(report
            .service_summary("raft_replication")
            .expect("raft replication summary")
            .next_action
            .contains("OpenRaft"));
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
        assert_eq!(
            report
                .service_summary("storage_cache")
                .expect("storage cache summary")
                .blocker_classes,
            vec!["storage_cache_durability".to_string()]
        );
        assert_eq!(
            report
                .service_summary("feature_modules")
                .expect("feature modules summary")
                .blocker_classes,
            vec!["feature_module_cpp_parity".to_string()]
        );
        assert_eq!(
            report
                .service_summary("context_workflow")
                .expect("context workflow summary")
                .blocker_classes,
            vec!["context_model_provider_parity".to_string()]
        );
        assert_eq!(
            report
                .service_summary("fault_tolerance")
                .expect("fault tolerance summary")
                .blocker_classes,
            vec!["fault_tolerance_validation".to_string()]
        );
        assert_eq!(
            report
                .service_summary("deployment_ops")
                .expect("deployment ops summary")
                .blocker_classes,
            vec!["deployment_ops_runtime".to_string()]
        );
        assert_eq!(
            report
                .service_summary("scale_testing")
                .expect("scale testing summary")
                .blocker_classes,
            vec!["scale_testing_evidence".to_string()]
        );
        assert_eq!(
            report
                .service_summary("raft_replication")
                .expect("raft replication summary")
                .blocker_classes,
            vec!["raft_replication_engine".to_string()]
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
            .any(|item| item.contains("applied Raft index")));
        assert!(data_raft.iter().any(|item| item.contains("learner add")));
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
        assert!(covered
            .iter()
            .any(|item| item.contains("ByteRaft-style leader write authority")));
        assert!(covered
            .iter()
            .any(|item| item.contains("ProductionRaftEngineKind::OpenRaft")));
        assert!(covered.iter().any(|item| {
            item.contains("Raft atomic apply readiness")
                && item.contains("atomic commit integration fail-closed")
        }));
        assert!(covered.iter().any(|item| {
            item.contains("Raft metaserver membership readiness")
                && item.contains("real-group execution fail-closed")
        }));
        assert!(covered.iter().any(|item| {
            item.contains("Raft transport security readiness")
                && item.contains("service-process mTLS enforcement fail-closed")
        }));
        assert!(covered.iter().any(|item| {
            item.contains("Raft external chaos readiness")
                && item.contains("external packet-loss/disk-pressure/process-chaos fail-closed")
        }));
        assert!(!data_raft
            .iter()
            .any(|item| item.contains("roll out the adapter")));

        let deployment_ops = report
            .areas
            .iter()
            .find(|area| area.area == "deployment_ops")
            .expect("deployment ops area must exist");
        assert!(deployment_ops
            .covered
            .iter()
            .any(|item| item.contains("validation-only auth/TLS evidence")));

        let fault_tolerance = report
            .areas
            .iter()
            .find(|area| area.area == "fault_tolerance")
            .expect("fault tolerance area must exist");
        assert!(fault_tolerance
            .covered
            .iter()
            .any(|item| item.contains("Prometheus alert rules and fault runbook")));
        assert!(fault_tolerance
            .covered
            .iter()
            .any(|item| item.contains("Raft external chaos readiness")));
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
