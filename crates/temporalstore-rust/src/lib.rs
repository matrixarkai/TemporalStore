// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![doc = include_str!("../README.md")]

// COMPILED INTO EVERY BUILD, not only test builds.
//
// It used to be `#[cfg(test)]`, which was right while everything that read the counters was a
// test. The allocation-class scopes that say what a write's memory is spent on are not: they sit
// inside the primitives that own each sink, because that is the only placement a new call site
// cannot go around -- the lesson of #1882 and #1899, where counters at one call site of nine saw
// one write in twelve.
//
// The cost of that in a build without the `alloc-probe` feature below is nothing at all:
// `in_class` is its closure, with no atomic, no thread-local and no branch, and what remains is a
// few hundred bytes of never-incremented counters. What the feature changes is not whether the
// module exists but whether anything counts -- and `classified_now` answers `None` rather than a
// table of zeros when nothing does, so an uninstrumented build cannot be read as a clean one.
pub mod alloc_probe;

// Tests only. Counts what a raft snapshot walks, reads, copies and rebuilds, bumped inside the
// primitives that do the work rather than at the call sites that ask for it.
#[cfg(test)]
pub mod snapshot_probe;

// Tests only, and only under the `alloc-probe` feature. RSS cannot resolve a per-request
// allocation change -- 71% of the proxy's resident memory was measured as allocator retention
// rather than live data -- so memory claims about a request path need the allocations counted
// directly.
//
// Feature-gated rather than always-on in tests: with this installed unconditionally, a full
// single-threaded run turned 17 socket-bound proxy and raft tests red that pass individually, as a
// group, and in a full run without it. Two atomics on every allocation in the process is enough to
// disturb tests that wait on sockets, and an instrument that changes the result of the suite
// checking the change is worse than no instrument.
#[cfg(all(test, feature = "alloc-probe"))]
#[global_allocator]
static COUNTING_ALLOCATOR: alloc_probe::CountingAllocator = alloc_probe::CountingAllocator;

pub mod bytes_serde;
pub mod block_store;
pub mod checksum;
pub mod client;
pub mod context_workflow;
pub mod control;
pub mod data_node;
pub mod e2e;
pub mod durability_metrics;
pub mod flush_gate;
pub mod memory_trim;
pub mod engine;
pub mod env_flag;
pub mod http;
pub mod index_log;
pub mod ingestion;
pub(crate) mod log_framing;
#[cfg(feature = "matrixobject")]
pub mod matrixobject_store;
pub mod meta;
pub mod partition_id;
pub mod proxy;
pub mod raft;
pub mod readiness;
pub mod rebalance;
pub mod redis;
mod scratch;
pub mod sdk;
pub mod shared_store;
pub mod storage_backend;
pub mod storage_config;
pub mod storage_descriptor;
pub mod telemetry;
pub mod types;
pub mod fault;
pub mod wal;
/// What an append costs per record and what a replay costs per log -- tests only.
#[cfg(test)]
mod wal_batch_roll;
mod wal_replay_scale;
/// What a WINDOWED replay reads against the log it replays -- tests only.
mod wal_replay_windows;
/// What reading a log piece's base header COSTS -- tests only.
mod wal_base_header_read;
/// What a RESTORE reads, and what no reader claims -- tests only.
mod restore_read_residual;
mod wal_scale;
/// What one log RECORD costs to build and to decode -- tests only.
mod record_cost;
mod wal_proto;
pub mod record_framing;
pub mod index_log_record;
pub mod wal_record;

pub use block_store::{
    BlockAddress, BlockStoreSlabDescriptor, BlockStoreSlabState, BlockStoreSlabSummary,
    BlockStoreOptions, BlockStoreSlabReport, BlockStoreStats, BlockStore, SharedSlabSource,
};
pub use client::{
    crc64_jones, key_is_dropped_by_percent, shard_id_for_key, bucket_id_for_key, stable_key_hash,
    ClientError, ClientOptions, ClientPreflightReport, ClientStats, RequestOptions, TableOptions,
    TemporalStoreClient, TemporalStorePipeline, TemporalStoreTable,
};
pub use context_workflow::{
    context_backfill_embeddings, context_embedding_ref_hash, context_pipeline_manage_report,
    context_provider_from_env,
    context_pipeline_parity_evidence,
    context_skill_registry_from_parsed,
    context_workflow_state_report, default_context_model_providers, extract_context,
    ingest_extract_context, ingest_extract_context_with_l1_gate, ingest_resource_skill_context,
    inject_context,
    reference_open_source_model_profiles, parse_context_resource, parse_context_skill_markdown,
    retrieve_context, run_context_pipeline_benchmark, run_context_pipeline_benchmark_sweep,
    select_context_skills_for_retrieval, update_context_resource_lifecycle,
    update_context_skill_registry, validate_resource_skill_secondary_indexes, ContextBlock,
    ContextExtractReport, ContextExtractRequest, ContextIngestExtractReport,
    ContextIngestExtractRequest, ContextIngestExtractSummary, ContextIngestSourceFailure,
    ContextInjectReport, ContextInjectRequest, ContextModelProviderConfig,
    ContextReferenceModelProfile, ContextParsedResourceChunk, ContextPipelineBenchmarkQueryReport,
    ContextPipelineBenchmarkReport, ContextPipelineBenchmarkRequest,
    ContextPipelineBenchmarkSweepProfile, ContextPipelineBenchmarkSweepReport,
    ContextPipelineBenchmarkSweepRequest, ContextPipelineBenchmarkThresholds,
    ContextPipelineManageReport, ContextPipelineParityEvidence, ContextPipelineStageReport,
    ContextPrefilterCandidateDebug, ContextProviderKind, ContextQueryUnderstandingDebug,
    ContextResourceImportKind, ContextResourceLifecycleAction, ContextResourceLifecycleRecord,
    ContextResourceLifecycleReport, ContextResourceLifecycleUpdate, ContextResourceParseReport,
    ContextResourceParseRequest, ContextResourceSkillEmbeddingEvidenceReport,
    ContextResourceSkillIngestReport, ContextResourceSkillIngestRequest,
    ContextResourceSkillModelFanoutReport, ContextResourceSkillSecondaryIndexReport,
    ContextResourceSkillSecondaryIndexValidationReport,
    ContextResourceSkillSecondaryIndexValidationRequest, ContextRetrieveReport,
    ContextRetrieveRequest, ContextSecondaryIndexFamilyValidationReport, ContextSkillIngestInput,
    ContextSkillParseReport, ContextSkillPrecedence, ContextSkillRegistryEntry,
    ContextSkillRegistryReport, ContextSkillRegistryUpdate, ContextSkillSelectionCandidate,
    ContextSkillSelectionReport, ContextSkillSelectionRequest, ContextSourceKind, ContextTier,
    ContextTreeTraversalDebug, ContextWorkflowStateReport,
};
pub use control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, Config, GetConfigResponse, GetInfoResponse, GetStatsResponse,
    LoadShardRequest, LoadShardResponse, MembershipUpdateRequest, ScanStreamRequest,
    ScanStreamResponse, SetConfigRequest, ShardCanonicalStorageStats, StreamKind,
    StreamReadRequest, StreamReadResponse, UnloadShardRequest, UnloadShardResponse,
};
pub use data_node::{
    CompactionRequest, CompactionResponse, DataNodeLifecycleReport, DataNodePreflightReport,
    DataNodeRuntime, DataNodeRuntimeOptions, DataNodeRuntimeStats, DataNodeShardLifecycleState,
    DataNodeTaskKind, DataNodeTaskOutput, DataNodeTaskStatus, DirtyObjectInfo, DumpShardRequest,
    DumpShardResponse, GcRequest, GcResponse, RequestController, SharedWalSink, ShardWorkerInfo,
    StorageLifecycleResponse, StorageLifecycleScheduler,
};
pub use e2e::{
    AsyncStorageJournal, EndToEndWorkflow, EndToEndWorkflowOptions, KillSwitches, RaftWriteMode,
    ReplicaReadPolicy, ReplicationMode, RoutingClient, TemporalStoreClientOptions, WorkflowError,
    WorkflowProxy,
};
pub use engine::golden::{native_api_golden_corpus_report, native_feature_sequence_golden_corpus_report};
pub use engine::reports::{
    GoldenCaseReport, GoldenCorpusReport, RustStorageObservation, ShardCompactionReport,
    ShardCompactionUtilityReport, ShardExpirySweepReport, BucketDumpFaultMatrixReport,
    BucketDumpFaultScenarioReport, BucketDumpFollowerReplayCursor, BucketDumpFollowerRetentionBlock,
    BucketDumpInstallMarker, BucketDumpInstallPreflightReport, BucketDumpInstallRollForwardReport,
    BucketDumpManifest, BucketDumpManifestChainIssue, BucketDumpManifestPrunePlan,
    BucketDumpManifestPruneReport, BucketDumpRaftSnapshotRef, BucketDumpRaftSnapshotRetentionBlock,
    BucketStorageSummary, StorageCacheInspectionReport, StorageCacheInvalidateBucketRequest,
    StorageCacheBucketSummary, StorageCacheWarmupReport, StorageDataStructureApiParityReport,
    StorageFeatureBlockError, StorageFeatureBlockLayoutReport, StorageFeatureBlockTimestampMismatch,
    StorageLifecyclePlan, StorageLifecycleReport, StorageLifecycleRequest,
    StorageLogCompatibilityReport, StorageObjectLifecycleReport,
    StorageBlockFormatCompatibilityReport, StorageProductionReadinessPolicy,
    StorageProductionReadinessReport, StorageProductionReadinessRequest, StorageReclaimCandidate,
    StorageRecoveryBoundaryReport, StorageRecoveryBlockError, StorageRecoveryBlockOwnerMismatch,
    StorageRecoveryReport, StorageRecoverySlabLiveReport, StorageSlabIntegrityReport,
    StorageTimestampedBlockFamilyReport,
};
pub use engine::TemporalEngine;

/// THE FIRST ROUTING BUCKET A SHARD OWNS BY DEFAULT.
pub const DEFAULT_START_ROUTING_BUCKET: u32 = 0;

/// THE LAST ROUTING BUCKET A SHARD OWNS BY DEFAULT -- 1,024 buckets, CHOSEN BY MEASUREMENT.
///
/// A page's routing bucket is `start + FNV-1a-64(object_key) % (end - start + 1)`, so this value
/// sets the MODULUS. The previous default was `u32::MAX`: 4.29 billion buckets among a few thousand
/// keys, where every key lands alone in its own bucket by construction. That was never a
/// configuration anyone ran -- `docs/runtime_tuning.md` told an operator to set 1,024 buckets before
/// the first ingest -- so the shipped default disagreed with the shipped documentation.
///
/// SWEPT, NOT COPIED FROM THE DOCUMENT'S EXAMPLE. 255 / 1,023 / 4,095 / 65,535 at 4,000 and 40,000
/// routed records, every distribution as a histogram with percentiles and a MAX and every byte
/// figure in BOTH allocator columns, in `engine/tests/routing_range_default.rs`. What the sweep
/// says, on the CHUNK column that charges `malloc_usable_size`:
///
/// ```text
///   end      40,000 records          4,000 records        dump/release unit   read path
///   255      114.1 B/rec  -59.2%     132.6 B/rec -52.8%   168 pages           6.42 entries
///   1023     120.2 B/rec  -57.0%     191.9 B/rec -31.6%    50 pages           4.54 entries
///   4095     144.1 B/rec  -48.4%     256.2 B/rec  -8.7%    21 pages           2.91 entries
///   65535    252.7 B/rec   -9.5%     257.9 B/rec  -8.1%     6 pages           0.83 entries
///   u32::MAX 279.3 B/rec    ---      280.7 B/rec   ---      1 page            0.00 entries
/// ```
///
/// The byte saving SATURATES -- 1,023 is within 5% of the floor 255 reaches -- while the dump and
/// release unit grows LINEARLY in the corpus without bound, because the fill is
/// `records / bucket-count`. So the right choice is the WIDEST range that still reaches the
/// amortisation floor, which is this one, and not the narrowest range the byte column prefers.
///
/// ONLY NEW STORES GET IT. See `engine/routing_range_stamp.rs`: a store records the range it was
/// built under, an existing store is honoured on that range, and a store whose stamp disagrees with
/// the configured range is REFUSED rather than loaded with every page filed out of range.
pub const DEFAULT_END_ROUTING_BUCKET: u32 = 1023;
pub use index_log::{IndexLogRecord, IndexLogStats, LocalIndexLogStore};
pub use ingestion::{
    dead_letter_export_report, flink_production_checkpoint_handshake_report,
    ingestion_readiness_report, kafka_consumer_group_runtime_report,
    raft_failover_idempotence_report, FlinkCheckpointAction, FlinkCheckpointState,
    FlinkCheckpointStatus, FlinkCheckpointUpdate, FlinkProductionCheckpointHandshakeReport,
    IngestionBatchReport, IngestionBatchRequest, IngestionDeadLetter,
    IngestionDeadLetterExportReport, IngestionRaftFailoverIdempotenceReport,
    IngestionReadinessReport, IngestionRecord, IngestionRecordResult, IngestionSource,
    IngestionStateReport, IngestionStats, KafkaConsumerGroupMember,
    KafkaConsumerGroupRuntimeReport, KafkaHighWatermark, KafkaOffsetLedgerEntry,
};
pub use meta::{
    AckResponse, AddNamespaceRequest, AddTableRequest, FreezeStaleServersRequest, GetShardResponse,
    GetTableTopologyRequest, ListNamespacesResponse, ListProxiesResponse, ListServersResponse,
    ListTablesResponse, LoadFinishRequest, LocalMetaMutationLog, MetaEntityState, MetaInfo,
    MetaMutation, MetaPreflightReport, MetaStats, NamespaceMetaInfo, ShardStatLoad,
    ProxyHeartbeatRequest, ProxyHeartbeatResponse, ProxyMetaInfo, RegisterProxyRequest,
    RegisterServerRequest, RegisterShardRequest, RegisterShardResponse, ServerEndpoint,
    ServerHeartbeatRequest, ServerHeartbeatResponse, ServerMetaInfo, ServerRuntimeLoad,
    ServerShardServingState, ShardLoad, ShardLocation, SingleNodeMeta, StaleResourceReport,
    StaleServerReport, StateChangeRequest, TableMetaInfo, TableShard, TableTopologyResponse,
};
pub use partition_id::{
    validate_partition_count_per_set, validate_partition_set_count, PartitionId, PartitionIdError,
    MAX_PARTITION_SET_INDEX, MAX_TABLE_ID, MIN_BUCKETS_PER_PARTITION, PARTITION_INDEX_MASK,
    PARTITION_VERSION_MASK, BUCKET_COUNT, BUCKET_MASK,
};
pub use proxy::{
    ProxyClientPreflightReport, ProxyConfigUpdateReport, ProxyMigrationContract, ProxyInfo,
    ProxyOpenTableRequest, ProxyOpenTableResponse, ProxyOptions, ProxyPolicyReport,
    ProxyPreflightReport, ProxyReplicaReadPolicy, ProxyService, ProxyServingMode, ProxyStats,
    ProxyTableBatchExecuteRequest, ProxyTableExecuteRequest, ProxyTableOptionsView,
};
#[cfg(feature = "temporal-raft-engine")]
pub use raft::temporal_raft_integration::{
    new_temporal_raft_data_node_backend, new_temporal_raft_metaserver_backend,
    TemporalRaftBackendReport, TemporalRaftConfig, TemporalRaftConsensusBackend,
    TemporalRaftDurableLogRecord, TemporalRaftDurableSnapshot, TemporalRaftEntry,
    TemporalRaftEntryPayload, TemporalRaftLogId, TemporalRaftMembership, TemporalRaftNode,
    TemporalRaftRuntimeKind, TemporalRaftSnapshotMeta, TemporalRaftStoredMembership,
};
pub use raft::{
    apply_data_raft_membership_from_topology, distributed_raft_readiness,
    handle_authenticated_raft_http, handle_raft_http, production_raft_security_from_env,
    raft_process_path_readiness_report_from_reports, require_production_raft_ready,
    matrixraft_parity_contract, matrixraft_parity_report,
    matrixraft_parity_report_from_current_readiness, matrixraft_production_readiness_report,
    validate_raft_deployment_mode, AppendEntriesRequest, AppendEntriesResponse,
    MatrixRaftLeaderElectionParityReport, DataRaftConsensusBackend, DataRaftConsensusOptions,
    DataRaftPeer, DataRaftStatus, DataRaftTopologyApplyReport, DataRaftTopologyMembershipPlan,
    DistributedRaftCommandResponse, DistributedRaftProposeRequest, DistributedRaftReadRequest,
    HttpRaftTransport, InstallSnapshotRequest, InstallSnapshotResponse, LocalRaftWal, MetaCommand,
    MetaOwnedDataRaftMembershipReport, MetaRaftCluster, MetaState, ProductionMetaRaftRuntime,
    ProductionMetaRaftRuntimeOptions, ProductionRaftChaosPlan, ProductionRaftEngineKind,
    ProductionRaftNode, ProductionRaftProcessSpec, ProductionRaftRuntime,
    ProductionRaftRuntimeOptions, ProductionRaftSecurity, ProductionRaftSecurityEnv,
    ProductionRaftSecurityMode, ProductionRaftTimerHandle, RaftApplyHealth, RaftApplyLag,
    RaftCatchUpReport, RaftCluster, RaftClusterStatus, RaftConfig, RaftConfigError,
    RaftControlLeadershipRequest, RaftDataNodeAtomicDurabilityReport, RaftDeploymentMode,
    RaftDistributedReadiness, RaftError, RaftFailoverReport, RaftHardState, RaftMembership,
    RaftMembershipChangeKind, RaftMembershipChangePlan, RaftMembershipChangeReport, RaftNodeId,
    RaftNodeStatus, RaftProcessPathReadinessReport, RaftProductionReadinessError, RaftReadOptions,
    RaftReadStrategy, RaftRole, RaftRpcRuntimeOptions, RaftTickOutcome, RaftTransport,
    RaftWalRecord, RaftWalSegmentInfo, RaftWalSegmentReport, ReadIndexResponse,
    MatrixRaftParityContract, MatrixRaftParityReport, MatrixRaftProductionReadinessInput,
    MatrixRaftProductionReadinessReport, MatrixRaftSemanticRequirement,
    TemporalRaftDataNodeProcessRolloutReport, TemporalRaftMetaProcessRolloutReport,
    TemporalRaftProcessNodeEvidence, TemporalRaftProcessOperationalSemanticsEvidence,
    UnavailableDataRaftConsensusBackend, VoteRequest, VoteResponse,
};
pub use readiness::{
    metaserver_scheduler_execution_readiness_report, production_readiness_report,
    MetaServerSchedulerExecutionReadinessReport, ProductionReadinessReport, ReadinessArea,
    ReadinessCapabilityBlocker, ServiceReadinessGateReport, ServiceReadinessSummary,
};
pub use rebalance::{
    PartitionSetMember, PartitionSetTopology, MembershipUpdatePeerRequest,
    MembershipUpdatePeerStatus, MembershipUpdateTaskOptions, MembershipUpdateTaskPlan,
    MembershipUpdateTaskReport, NetworkSchedulerTaskExecution, RaftPersistedSchedulerState,
    RebalanceController, RebalanceError, RebalanceOptions, RebalanceRoundReport, RebalanceStep,
    SchedulerLifecycleToken, ShardMovePlan, ShardReplica, ShardReplicaState, ShardRole,
};
pub use redis::{execute_redis_command, read_command, serve_redis_proxy, RespValue};
pub use matrixcache::{CacheEntryInfo, CacheGcReport, CacheKey, CacheStats, MultiLayerCache};
#[cfg(feature = "matrixobject")]
pub use matrixobject_store::MatrixObjectObjectStore;
pub use shared_store::{
    ReplayReport, SharedStoreCheckpointManifest, SharedStoreFlushReport, SharedStoreGcReport,
    SharedStoreWalEntry, SharedStoreWalObject, SharedStoreBlockSlab, SharedStoreReplayCursor,
    MatrixObjectSlabSource, SharedPathSlabSource, SharedStoreReplicationError, SharedStoreReplicator,
    SharedStoreRetryPolicy, SharedStoreWalAppendMode,
    SharedStoreStorageMode, SharedStoreStorageWriter, SharedStoreWriteReport,
};
pub use storage_backend::{
    matrixobject_feature_compiled, StorageBackend, StorageBackendConfig,
};
pub use types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ContextAuditModel,
    ContextAuditRef, ContextChildModel, ContextChildRef, ContextCompressionEvent,
    ContextCompressionModel,
    ContextEntity, ContextEvent, ContextEventModel, ContextExtractedEventIndexes,
    ContextIndexModel, ContextIndexRef, ContextNode, ContextNodeModel, ContextPackAudit,
    ContextSlab, ContextSummary, ContextDirtyNode, ContextSummaryModel,
    ContextTraversedNode, ContextWire, EventReplicationMode, EventReplicationSelectionReport,
    ExecuteRequest, ExecuteResponse, FeaturePoint, InternalContextIndex,
    ReplicatedBatchExecuteRequest, ReplicatedBatchExecuteResponse, ReplicatedCommand,
    ReplicatedExecuteRequest, ShardId, Status,
};
pub use wal::{
    LocalWalStore, WalError, WalGcReport, WalRecord, WalStats,
};
pub use wal::{
    LocalWriteAheadLogStore, WriteAheadLogAppendReport, WriteAheadLogError,
    WriteAheadLogFlushReport, WriteAheadLogGcReport, WriteAheadLogInfo, WriteAheadLogItemKind,
    WriteAheadLogItemMetadata, WriteAheadLogModel, WriteAheadLogRecord,
    WriteAheadLogRecordMetadata, WriteAheadLogStats, WRITE_AHEAD_LOG_FORMAT_VERSION,
};
