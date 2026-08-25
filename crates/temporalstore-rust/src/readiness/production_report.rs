// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! production_readiness_report aggregator + service readiness helpers, split from readiness.rs.
use super::*;

pub fn production_readiness_report() -> ProductionReadinessReport {
    let raft = distributed_raft_readiness();
    let ingestion = ingestion_readiness_report();
    let client_routing = client_routing_readiness_report();
    let proxy_serving = proxy_serving_readiness_report();
    let data_node_service = data_node_service_readiness_report();
    let metaserver_control_plane = metaserver_control_plane_readiness_report();
    let storage_cache_dependency_matrix = storage_cache_dependency_matrix_report();
    let storage_ssd_cache_pressure = storage_ssd_cache_pressure_readiness_report();
    let storage_migration_corpus = storage_migration_corpus_readiness_report();
    let storage_production_posture = storage_production_posture_report();
    let feature_modules = feature_module_production_readiness_report();
    let context_workflow = context_workflow_production_readiness_report();
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
                "raft_node, raft-enabled server, and metaserver process startup select ProductionRaftEngineKind::TemporalRaft by default"
                    .to_string(),
                "Raft TemporalRaft rollout readiness covers adapter presence, data-node/metaserver startup selection, durable local log state, and real multi-process data-node/metaserver log-store rollout evidence"
                    .to_string(),
                "TemporalRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "OpenRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "RaftStorageApplyFence is persisted in WAL records and rejects missing, corrupt, stale, or ahead-of-storage recovery state"
                    .to_string(),
                "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, production runtime data-node atomic durability reports, storage mutation atomic commit, snapshot-install atomic commit, and snapshot lifecycle reporting"
                    .to_string(),
                "RaftSnapshotInstallReport exposes freeze, flush, manifest verify, checksum verify, install, tail replay, and rollback status for snapshot installs"
                    .to_string(),
                "ProductionMetaRaftRuntime can drive a data-Raft membership workflow for learner add, catch-up verification, promotion, leader transfer, and voter removal"
                    .to_string(),
                "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, service-process mTLS runtime selection, authenticated HTTP transport, and plaintext-only local chaos guardrails"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates"
                    .to_string(),
            ],
            missing: raft.missing.clone(),
        },
        ReadinessArea {
            area: "client".to_string(),
            ready: client_routing.production_ready,
            covered: vec![
                "typed table client, pipeline, retries/timeouts, route refresh".to_string(),
                "primary routing for writes and optional secondary routing for reads".to_string(),
                "background topology sync and crc64 slot formula".to_string(),
                "client preflight report exposes route/table cache, backend-failure backlog, stats, and options"
                    .to_string(),
                "client retry classifier separates budget-free safe topology retries from unsafe write retries that require explicit write retry budget"
                    .to_string(),
                "shared conformance corpus runs through the typed table client API and direct engine path for common, feature, sequence, control_state, context, and restart reads"
                    .to_string(),
                "versioned Rust-native SDK contract committed in proto/temporalstore/v1 with validation in the local parity gate"
                    .to_string(),
                "tonic/prost client and server binding types are generated at build time from the committed v1 schema"
                    .to_string(),
                "generated tonic Execute and BatchExecute service adapters convert protobuf commands and delegate to the existing engine execution path"
                    .to_string(),
                "generated tonic OpenTable, SyncTopology, and ClientPreflight adapters delegate to the existing TemporalStoreClient table, topology, and preflight paths"
                    .to_string(),
                "client preflight exposes a standard partition-set compatibility view derived from cached table ranges and shard routes"
                    .to_string(),
                "client routing readiness covers typed table client, topology sync, retry budgets, topology preflight, Neptune routing hooks, deployment placement hooks, and the tracked Rust-native HTTP/JSON, RESP, and tonic migration contract while keeping legacy wire migration explicitly out of scope"
                    .to_string(),
            ],
            missing: client_routing.missing,
        },
        ReadinessArea {
            area: "proxy".to_string(),
            ready: proxy_serving.production_ready,
            covered: vec![
                "HTTP proxy execute/batch routes delegate through TemporalStoreClient for route cache, retries, backend-error refresh, and stats sync"
                    .to_string(),
                "background heartbeat loop and heartbeat auto-register".to_string(),
                "backend-error route refresh and failure-streak avoidance".to_string(),
                "namespace/table open path and table-routed proxy execute/batch routes"
                    .to_string(),
                "service-name JSON aliases for ExecuteCmd, BatchExecuteCmd, OpenTable, and table execute/batch execute"
                    .to_string(),
                "service-name admin aliases expose proxy info, heartbeat/config, and embedded client preflight"
                    .to_string(),
                "command-shaped proxy HTTP/JSON aliases cover Get, Set, FeatureAdd, ControlStateHset, HMGet, HMSet, HGetAll, and HLen through the normal routed client path"
                    .to_string(),
                "Rust-native service discovery replacement for consul via proxy auto-register, heartbeat TTL, admin inspection, and Prometheus stale/registered metrics"
                    .to_string(),
                "proxy serving readiness covers HTTP execute routes, heartbeat/config application, topology-version invalidation, admission policy, route quarantine, Rust-native service discovery, RESP migration, tonic streaming/callback shape, and the tracked HTTP/JSON, RESP, and tonic migration contract while keeping legacy wire proxy transport out of scope"
                    .to_string(),
            ],
            missing: proxy_serving.missing,
        },
        ReadinessArea {
            area: "metaserver".to_string(),
            ready: metaserver_control_plane.production_ready,
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
                "metaserver control-plane readiness covers inventory heartbeat, namespace/table topology, partition-set/member/version topology, local Raft mutation path, load-aware placement, local snapshots, scheduler admin, scheduler snapshots, scheduler retry/task Raft persistence, networked metaserver Raft rollout, and metaserver-owned data-Raft membership execution"
                    .to_string(),
                "networked metaserver scheduler execution readiness covers multi-process metaserver Raft, real data-node process scheduling for missing primary, under-replication, stale/dead server, load/reload/unload, membership changes, cooldowns, safe mode, scheduler task replay, durable data-Raft membership coupling, and stale scheduler token/generation rejection"
                    .to_string(),
            ],
            missing: metaserver_control_plane.missing,
        },
        ReadinessArea {
            area: "data_node_distributed_raft".to_string(),
            ready: raft.production_ready,
            covered: vec![
                "separate raft_node binary can accept /raft/propose and peer raft messages"
                    .to_string(),
                "local model covers majority commit, catch-up, failover, safe scale up/down"
                    .to_string(),
                "data Raft supports replicator-style bounded follower catch-up with lag/progress reports"
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
                "standard deterministic metaserver task scheduler model covers priority ordering, retry-later backoff, abort, and UpdateMembership task enqueue"
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
                "TemporalRaft-backed data-node and metaserver adapter is available behind the temporal-raft-engine feature with durable log state, state-machine apply, snapshot metadata, read-index checks, membership changes, leader transfer, and restart recovery tests"
                    .to_string(),
                "raft_node, raft-enabled server, and metaserver process startup wire the production runtime options to ProductionRaftEngineKind::TemporalRaft"
                    .to_string(),
                "Raft TemporalRaft rollout readiness covers adapter presence, data-node/metaserver startup selection, durable local log state, and real multi-process data-node/metaserver log-store rollout evidence"
                    .to_string(),
                "TemporalRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "OpenRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "RaftStorageApplyFence persists shard, term, committed/applied index, snapshot id, storage epoch, and checksum with WAL recovery validation"
                    .to_string(),
                "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, production runtime data-node atomic durability reports, storage mutation atomic commit, snapshot-install atomic commit, and snapshot lifecycle reporting"
                    .to_string(),
                "Raft snapshot lifecycle reports install, tail replay, and rollback decisions for data-node snapshot recovery paths"
                    .to_string(),
                "metaserver-owned data-Raft membership workflow reports learner add, catch-up verification, promotion, leader transfer, and voter removal"
                    .to_string(),
                "Raft metaserver membership readiness covers topology membership plans, data-Raft apply reports, learner catch-up/promotion, leader transfer, voter removal, networked scheduler /raft/membership/apply transport, persisted scheduler task state, and real data-node group execution under follower lag, failover, scale up/down, and secondary replication"
                    .to_string(),
                "MatrixRaft-style leader write authority, ReadIndex guards, learner catch-up/promotion checks, and fail-closed stale leader-transfer checks are modeled locally"
                    .to_string(),
                "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, service-process mTLS runtime selection, authenticated HTTP transport, and plaintext-only local chaos guardrails"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates"
                    .to_string(),
            ],
            missing: if raft.production_ready {
                Vec::new()
            } else {
                raft.missing.clone()
            },
        },
        ReadinessArea {
            area: "dataserver".to_string(),
            ready: data_node_service.production_ready,
            covered: vec![
                "TemporalEngine command execution, checked execute/batch, runtime queue"
                    .to_string(),
                "async jobs, cancellation status surface, dirty-object reporting".to_string(),
                "page-address persistence and Prometheus metrics endpoint".to_string(),
                "load rejects duplicate loaded shards and unload removes shard metadata with not-found semantics"
                    .to_string(),
                "config get/set follows partition-map not-found semantics".to_string(),
                "data-node membership update rejects stale global/unit versions and reports whether local replica remains active"
                    .to_string(),
                "data-node runtime uses shard-affine worker lanes so one partition has FIFO single-lane execution while different shards run in parallel"
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
                "local shard_stat_info and object-manager stats report logical objects, page refs, dirty objects, dirty routing slots, and storage bytes"
                    .to_string(),
                "local shard/table/tenant read/write QPS admission is enforced from Config quota fields"
                    .to_string(),
                "ServerService admin aliases expose runtime stats, preflight, dirty-object, and queued-worker state"
                    .to_string(),
                "crash recovery reports and tests cover wal, index-log, page stream, and band-manifest ordering"
                    .to_string(),
                "data-node service readiness covers execute runtime, async jobs, lifecycle admin, shard-affine workers, local admission, crash recovery reports, tonic/gRPC streaming callbacks, distributed admission, and multi-process lifecycle validation"
                    .to_string(),
            ],
            missing: data_node_service.missing,
        },
        ReadinessArea {
            area: "ingestion".to_string(),
            ready: ingestion.production_ready,
            covered: ingestion.covered,
            missing: ingestion.missing,
        },
        ReadinessArea {
            area: "fault_tolerance".to_string(),
            ready: true,
            covered: vec![
                "local majority-loss rejection for reads and writes".to_string(),
                "local primary crash promotion and recovered follower catch-up".to_string(),
                "local stale candidate, lagging read-index, and stale snapshot guards".to_string(),
                "kill switches for proxy reads/writes, replication, async storage, and scale changes"
                    .to_string(),
                "local combined recovery proof covers Raft WAL restore plus wal, index-log, page-file, and packed timestamped KV recovery"
                    .to_string(),
                "Prometheus alert rules and fault runbook cover stuck replica, split-brain control_state, slow follower, and storage pressure triage"
                    .to_string(),
                "external_chaos_gate composes OS-process Raft kill/restart, stale-read partition, lag/heal, rolling restart, networked membership/snapshot, and storage replay harnesses"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates"
                    .to_string(),
                "rolling restart and rolling upgrade validation covers proxy, client, metaserver, and data-node using preflight, readiness, failover, replay, and rollback checks"
                    .to_string(),
            ],
            missing: Vec::new(),
        },
        ReadinessArea {
            area: "storage_cache".to_string(),
            ready: storage_migration_corpus.production_ready
                && storage_cache_dependency_matrix.production_ready
                && storage_ssd_cache_pressure.production_ready
                && storage_production_posture.production_ready,
            covered: vec![
                "local page segment files with page-address indexes".to_string(),
                "local page compaction rolls to a fresh segment, rewrites live page references, persists the compacted index, and makes old segments GC-eligible"
                    .to_string(),
                "memory plus disk read-through cache with zstd block envelope".to_string(),
                "shared-store checkpoint/wal replay model and GC tests".to_string(),
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
                "storage lifecycle cache warmup returns selected slots, page-ref hits/fills/failures, block-store read count, and warmed bytes"
                    .to_string(),
                "storage production report exposes Rust JSONL wal/index-log format, replay-safe status, sequence/record/byte counts, and binary compatibility gaps"
                    .to_string(),
                "storage production report exposes Rust page-envelope version, checksum/object-id/routing-slot/compression support, band bytes, and page-header compatibility gaps"
                    .to_string(),
                "storage compatibility decision is explicit: Rust log/page formats are migration-only versus binary logs/page headers, with golden conversion/replay required before migration"
                    .to_string(),
                "chunked timestamped KV page format is covered by sync and async shared-store replay plus Raft follower-read replication tests"
                    .to_string(),
                "chunked timestamped KV page recovery strictly rejects malformed or unsupported packed-page payloads"
                    .to_string(),
                "storage migration corpus converts logical object/page/slot/index/wal exports into Rust-native pages and replays through engine restart, slot dump, cache warmup, shared-store sync/async replay, and Raft leader-transfer reads"
                    .to_string(),
                "storage migration corpus readiness covers Rust-local converted corpus replay through engine restart, Redis/admin, shared-store sync/async replay, cache warmup, Raft read paths, external binary-artifact export, CI-published golden artifacts, and the unified conformance runner"
                    .to_string(),
                "local storage production harness combines dump, cache pressure, restart recovery, shared-store replay, and Raft movement into one repeatable gate"
                    .to_string(),
                "local storage dump/load fault matrix harness rejects checksum mismatch, partial manifests, missing segments, stale manifests, restart-during-install recovery, and corrupt page segments"
                    .to_string(),
                "local/shared-store object manifest dependency matrix covers local file objects, checkpoint manifests, wal cursor retention, page segment manifests, follower-cursor retention, and Raft snapshot manifest retention"
                    .to_string(),
                "storage cache dependency matrix keeps live external object-store/S3 integration explicitly out of scope while local/shared-store is the production target"
                    .to_string(),
                "storage cache readiness is strong for Rust-native local/shared-store paths; broad Docker/AWS deployment evidence and live external object-store evidence are scoped as separate readiness gates"
                    .to_string(),
                "storage SSD cache pressure readiness covers local memory read-through, disk block cache, admission/eviction counters, Rust-native weighted hotness/LRU eviction evidence, slot warmup, cache invalidation, tiering policy, admission tuning, long-running pressure validation evidence, the Rust-native multi-tier replacement policy, pinned handle accounting/eviction guards, DRAM/PMEM/SSD placement semantics, bounded async writeback/backpressure, and bucketed cache latency metrics; direct CacheLib/matrixcache binary/API compatibility remains out of scope unless re-scoped"
                    .to_string(),
                "storage SSD cache pressure readiness covers local memory read-through, disk block cache, admission/eviction counters, weighted hotness/LRU eviction, slot warmup, cache invalidation, tiering policy, admission tuning, and long-running pressure validation evidence"
                    .to_string(),
                "storage production posture covers Rust lifecycle behavior evidence, native ObjectManager runtime mechanics, stream-backed band runtime, mature background StorageManager prepare/reclaim/evict/expire/compact/index-GC loop, merged dump/load policy with ownership validation, orphan page detection, missing/stale page-reference detection, corrupt page/index/wal/snapshot evidence, follower-cursor safe GC, cache pressure/refill, shared-store sync/async replay, unified storage corpus cases, first-class slot/object/page ownership index, SlotStore layout transition evidence, and model-layout compaction"
                    .to_string(),
                "storage production posture covers Rust lifecycle behavior evidence, native ObjectManager runtime mechanics, stream-backed band runtime, mature background StorageManager prepare/reclaim/evict/expire/compact/index-GC loop, merged dump/load policy with ownership validation, orphan page detection, missing/stale page-reference detection, corrupt page/index/wal/snapshot evidence, follower-cursor safe GC, cache pressure/refill, shared-store sync/async replay, unified storage corpus cases, first-class slot/object/page ownership index, SlotStore layout transition evidence, and model-layout compaction"
                    .to_string(),
            ],
            missing: {
                let mut missing = Vec::new();
                missing.extend(storage_migration_corpus.missing.clone());
                missing.extend(storage_cache_dependency_matrix.missing.clone());
                missing.extend(storage_ssd_cache_pressure.missing.clone());
                missing.extend(storage_production_posture.missing.clone());
                missing
            },
        },
        ReadinessArea {
            area: "feature_modules".to_string(),
            ready: feature_modules.production_ready,
            covered: vec![
                "common/string/hash/set plus Redis compatibility subset".to_string(),
                "feature append/query/replace/delete/agg and 5k sequence test".to_string(),
                "feature writes pack many timestamp/value entries into one page and the timestamp index shares that page address, matching the storage shape"
                    .to_string(),
                "feature aggregate query covers count/events/sum/avg/min/max/first/last over selected timestamp windows"
                    .to_string(),
                "large timestamped KV writes split into persisted page chunks while preserving per-timestamp reads"
                    .to_string(),
                "oversized single timestamped values remain readable as one packed page without creating empty chunks"
                    .to_string(),
                "feature/sequence binary protobuf golden corpus exercises filters, aggregates, sequence queries, and packed timestamped KV page layout"
                    .to_string(),
                "full Rust-local API golden corpus covers feature, sequence, ControlState, Redis-compatible core commands, and admin storage readiness"
                    .to_string(),
                "ControlState debug report exposes H/CPC/FOL full and window counters plus FOL selection metadata through engine, typed client, and RESP"
                    .to_string(),
                "feature module production readiness covers golden corpus replay, exact Feature nested point/proto semantics, deployment-specific time-range behavior, ControlState CPC/list internals, manager/debug APIs, and engine/client/RESP coverage"
                    .to_string(),
            ],
            missing: feature_modules.missing,
        },
        ReadinessArea {
            area: "context_workflow".to_string(),
            ready: context_workflow.production_ready,
            covered: vec![
                "Rust-native Context extraction/retrieval/injection workflow persists ContextNode, ContextEvent, ContextIndexRef, ContextDirtyNode, and ContextPackAudit"
                    .to_string(),
                "Hierarchical L0/L1/L2 context tiers are generated deterministically for local mocked sources"
                    .to_string(),
                "context model provider config can switch between mock and OpenAI-compatible provider shapes without changing API payloads"
                    .to_string(),
                "Hierarchical open-source model profiles expose VLM, chat, and embedding model choices for local Ollama/vLLM/OpenAI-compatible deployments"
                    .to_string(),
                "data-node server exposes /context/extract, /context/retrieve, /context/inject, /context/workflow/state, and provider inspection routes"
                    .to_string(),
                "context workflow harness validates mock extraction, retrieval, prompt injection, audit refs, Docker packaging, and parity-gate log validation"
                    .to_string(),
                "OpenAI-compatible context extraction can call a live HTTP provider with bounded deadlines, retries, Authorization header loaded from an environment variable, JSON response parsing, and fallback provider execution"
                    .to_string(),
                "golden context corpus replay covers engine, client, proxy, Redis/admin, shared-store, and Raft paths"
                    .to_string(),
                "context workflow production policy covers provider/model selection, PII filtering, tenant isolation, prompt-size admission, rate limiting, and provider failure budgets"
                    .to_string(),
            ],
            missing: context_workflow.missing,
        },
        ReadinessArea {
            area: "deployment_ops".to_string(),
            ready: true,
            covered: vec![
                "Docker and existing-EKS Terraform skeleton".to_string(),
                "Prometheus text metrics for core local surfaces".to_string(),
                "local scale harness".to_string(),
                "standard membership update task model filters sibling replicas, applies success thresholds, treats not_found as acceptable reboot state, and gates FSM submit"
                    .to_string(),
                "rolling upgrade and rollback runbook covers metaserver, data-node, proxy, client, storage, ingestion, preflight, quick chaos gate, and audit artifacts"
                    .to_string(),
                "Raft transport security readiness covers service-process mTLS runtime selection with auth-token validation, cert/key/CA validation, and plaintext-only local chaos guardrails"
                    .to_string(),
                "ops/scale readiness harness validates autoscale controller evidence, metaserver-driven shard rebalance loop evidence, dashboards, alerts, tracing, non-Raft auth/TLS, and runbook artifacts"
                    .to_string(),
                "Grafana dashboard and Prometheus alert evidence cover readiness blockers, scheduler backlog/retries, route quarantine, storage/cache recovery, ingestion lag, and scale SLOs"
                    .to_string(),
                "API security and tracing runbook covers TS_API_AUTH_TOKEN, TLS edge termination, trace_id/request_id propagation, and OpenTelemetry service naming for all service APIs"
                    .to_string(),
            ],
            missing: Vec::new(),
        },
        ReadinessArea {
            area: "scale_testing".to_string(),
            ready: true,
            covered: vec![
                "local in-process scale_harness exercises writes, sampled replica reads, failover, and scale events"
                    .to_string(),
                "long sequence rows are covered in unit tests and in the scale harness".to_string(),
                "existing-EKS Terraform and Redis load script skeletons exist".to_string(),
                "scale_harness emits a stable SLO report with p50/p95/p99, throughput, error budget, CPU/memory/disk/network placeholders, failover count, scale-event count, and replica lag"
                    .to_string(),
                "ops/scale readiness harness accepts the Docker/local-process path for multi-node validation with metaserver, proxy, client, and data-node process roles"
                    .to_string(),
                "distributed Raft harness covers lag, catch-up, election, membership scale up/down, leader transfer, snapshot bootstrap, and secondary reads under load"
                    .to_string(),
                "unified conformance workload corpus covers Feature, ControlState, Redis, Context, and admin API replay evidence"
                    .to_string(),
                "Docker/AWS SLO report covers metaserver, proxy, client, data-node, Raft failover, storage pressure, cache pressure, proxy convergence, workload replay, p50/p95/p99, throughput, error budget, CPU/memory/disk/network collectors, replica lag, failover count, and scale events"
                    .to_string(),
                "scale_slo_report.storage_deployment_scale_slo_ready is backed by ops_scale_readiness_harness and scale_harness JSON evidence"
                    .to_string(),
            ],
            missing: Vec::new(),
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
                    evidence_field: evidence_field_for(&area.area, capability).to_string(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let blocker_count = failed_capabilities.len();
    let service_summaries = service_readiness_summaries(&areas);
    ProductionReadinessReport {
        production_ready,
        blocker_count,
        failed_areas,
        failed_capabilities,
        service_summaries,
        areas,
    }
}

pub(crate) fn service_readiness_summaries(areas: &[ReadinessArea]) -> Vec<ServiceReadinessSummary> {
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
        let failed_evidence_fields = selected
            .iter()
            .flat_map(|area| {
                area.missing
                    .iter()
                    .map(|capability| evidence_field_for(&area.area, capability).to_string())
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
            failed_evidence_fields,
        }
    })
    .collect()
}

pub(crate) fn evidence_field_for(area: &str, capability: &str) -> &'static str {
    match area {
        "raft_replication" if capability.contains("TemporalRaft production engine adapter") => {
            "raft_rollout.temporal_raft_engine_adapter_present"
        }
        "raft_replication"
            if capability.contains("data-node process rollout")
                || capability.contains("data-node multi-process rollout evidence") =>
        {
            "raft_rollout.temporal_raft_data_node_process_rollout_ready"
        }
        "raft_replication"
            if capability.contains("metaserver process rollout")
                || capability.contains("metaserver multi-process rollout evidence") =>
        {
            "raft_rollout.temporal_raft_metaserver_process_rollout_ready"
        }
        "raft_replication"
            if capability.contains("multi-process log-store validation evidence") =>
        {
            "raft_rollout.multi_process_log_store_validated"
        }
        "data_node_distributed_raft"
            if capability.contains("data-node multi-process rollout evidence") =>
        {
            "raft_rollout.temporal_raft_data_node_process_rollout_ready"
        }
        "data_node_distributed_raft"
            if capability.contains("metaserver multi-process rollout evidence") =>
        {
            "raft_rollout.temporal_raft_metaserver_process_rollout_ready"
        }
        "data_node_distributed_raft"
            if capability.contains("multi-process log-store validation evidence") =>
        {
            "raft_rollout.multi_process_log_store_validated"
        }
        "raft_replication"
            if capability.contains("multi-process log-store validation evidence") =>
        {
            "raft_rollout.multi_process_log_store_validated"
        }
        "data_node_distributed_raft"
            if capability.contains("data-node multi-process rollout evidence") =>
        {
            "raft_rollout.openraft_data_node_process_rollout_ready"
        }
        "data_node_distributed_raft"
            if capability.contains("metaserver multi-process rollout evidence") =>
        {
            "raft_rollout.openraft_metaserver_process_rollout_ready"
        }
        "data_node_distributed_raft"
            if capability.contains("multi-process log-store validation evidence") =>
        {
            "raft_rollout.multi_process_log_store_validated"
        }
        "data_node_distributed_raft" if capability.contains("membership") => {
            "meta_owned_data_raft_membership.data_node_membership_results_ready"
        }
        "metaserver" if capability.contains("networked multi-process metaserver Raft") => {
            "metaserver_scheduler_report.networked_metaserver_raft_ready"
        }
        "metaserver" if capability.contains("scheduler loop") => {
            "metaserver_scheduler_report.real_process_scheduler_execution_ready"
        }
        "metaserver" if capability.contains("durable shard membership") => {
            "metaserver_scheduler_report.durable_membership_coupling_ready"
        }
        "storage_cache" if capability.contains("golden") || capability.contains("corpus") => {
            "storage_legacy_corpus_report.external_corpus_publication_ready"
        }
        "storage_cache"
            if capability.contains("object-store") =>
        {
            "storage_object_store_dependency_matrix.live_backend_dependency_matrix_ready"
        }
        "storage_cache" if capability.contains("multi-tier replacement policy") => {
            "storage_cache_matrixcache.replacement_policy_ready"
        }
        "storage_cache" if capability.contains("zero-copy pinned handle") => {
            "storage_cache_matrixcache.zero_copy_pinned_handle_ready"
        }
        "storage_cache" if capability.contains("DRAM/PMEM/SSD placement") => {
            "storage_cache_matrixcache.dram_pmem_ssd_placement_ready"
        }
        "storage_cache" if capability.contains("async writeback") => {
            "storage_cache_matrixcache.async_writeback_backpressure_ready"
        }
        "storage_cache" if capability.contains("latency metrics") => {
            "storage_cache_matrixcache.latency_metrics_ready"
        }
        "storage_cache" if capability.contains("ObjectManager") => {
            "storage_runtime_object_manager.native_runtime_ready"
        }
        "storage_cache" if capability.contains("SlotStore") => {
            "storage_runtime_slot_store.layout_transition_ready"
        }
        "storage_cache" if capability.contains("stream-backed band") => {
            "storage_runtime_bands.stream_backed_band_runtime_ready"
        }
        "storage_cache"
            if capability.contains("model layout") || capability.contains("tombstones") =>
        {
            "storage_runtime_compaction.model_layout_compaction_ready"
        }
        "storage_cache" if capability.contains("StorageManager") => {
            "storage_runtime_manager.background_loop_ready"
        }
        "storage_cache" if capability.contains("merged dump/load") => {
            "storage_runtime_dump_load.merged_policy_ready"
        }
        "client" => "client_migration_contract.compatibility_result",
        "proxy" => "proxy_migration_contract.compatibility_result",
        "scale_testing" if capability.contains("workload") || capability.contains("corpus") => {
            "scale_slo_report.legacy_workload_replay_ready"
        }
        "scale_testing" if capability.contains("global production storage readiness") => {
            "scale_slo_report.storage_deployment_scale_slo_ready"
        }
        "scale_testing" => "scale_slo_report.docker_or_aws_slo_evidence_ready",
        "feature_modules" if capability.contains("Feature") => {
            "feature_module_corpus.exact_feature_semantics_ready"
        }
        "feature_modules" if capability.contains("ControlState") => {
            "feature_module_corpus.control_state_production_semantics_ready"
        }
        "context_workflow" if capability.contains("corpus") => {
            "context_workflow_corpus.reference_replay_ready"
        }
        "context_workflow" => "context_workflow_policy.production_policy_ready",
        _ => "readiness_evidence.unspecified",
    }
}

pub(crate) fn service_blocker_class(area: &str) -> &'static str {
    match area {
        "client" => "client_sync_preflight",
        "proxy" => "proxy_topology_admission",
        "ingestion" => "ingestion_durability",
        "dataserver" => "data_node_local_lifecycle",
        "data_node_distributed_raft" => "data_node_distributed_raft",
        "metaserver" => "metaserver_control_plane",
        "storage_cache" => "storage_cache_durability",
        "feature_modules" => "feature_module_parity",
        "context_workflow" => "context_model_provider_parity",
        "fault_tolerance" => "fault_tolerance_validation",
        "deployment_ops" => "deployment_ops_runtime",
        "scale_testing" => "scale_testing_evidence",
        "raft_replication" => "raft_replication_engine",
        _ => "other",
    }
}

pub(crate) fn service_next_action(service: &str, blocker_classes: &[String]) -> &'static str {
    let Some(first_class) = blocker_classes.first().map(String::as_str) else {
        return "ready";
    };
    match (service, first_class) {
        ("client", "client_sync_preflight") => {
            "keep legacy client wire shims explicitly out of scope and migrate legacy callers through the tracked HTTP/JSON, RESP, and tonic contract"
        }
        ("proxy", "proxy_topology_admission") => {
            "keep legacy proxy wire transport explicitly out of scope and use the tracked HTTP/JSON plus tonic migration contract"
        }
        ("ingestion", "ingestion_durability") => {
            "finish network Kafka/Flink runtime failover, lag metrics, and dead-letter export"
        }
        ("data_node", "data_node_distributed_raft") => {
            "finish metaserver-driven membership against real data-node Raft groups"
        }
        ("data_node", "data_node_local_lifecycle") => {
            "finish data-node lifecycle restart barriers, distributed admission, and crash recovery"
        }
        ("metaserver", "metaserver_control_plane") => {
            "finish networked metaserver Raft, scheduler loop, and safe topology membership mutations"
        }
        ("storage_cache", "storage_cache_durability") => {
            "finish matrixcache-class async writeback/backpressure and mature latency metrics"
        }
        ("feature_modules", "feature_module_parity") => {
            "finish exact feature/control_state corpus coverage and deployment-specific module edge cases"
        }
        ("context_workflow", "context_model_provider_parity") => {
            "finish golden corpus replay and production policy controls"
        }
        ("fault_tolerance", "fault_tolerance_validation") => {
            "ready"
        }
        ("deployment_ops", "deployment_ops_runtime") => {
            "ready"
        }
        ("scale_testing", "scale_testing_evidence") => {
            "run Docker/AWS multi-node SLO validation before claiming global production storage readiness"
        }
        ("raft_replication", "raft_replication_engine") => {
            "finish durable real-process TemporalRaft rollout, production mTLS transport, and external chaos coverage"
        }
        _ => "inspect failed capabilities for this service",
    }
}

pub(crate) fn service_owner(service: &str) -> &'static str {
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

pub(crate) fn service_gate_severity(ready: bool, blocker_count: usize) -> &'static str {
    if ready {
        "ready"
    } else if blocker_count >= 3 {
        "critical"
    } else {
        "warning"
    }
}
