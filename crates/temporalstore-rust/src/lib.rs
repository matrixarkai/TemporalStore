// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![doc = include_str!("../README.md")]

#[cfg(test)]
pub mod alloc_probe;

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
pub mod engine;
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
mod wal_proto;
pub mod record_framing;
pub mod index_log_record;
pub mod wal_record;

pub use block_store::{
    BlockAddress, BlockStoreBandDescriptor, BlockStoreBandState, BlockStoreBandSummary,
    BlockStoreOptions, BlockStoreSlabReport, BlockStoreStats, LocalBlockStore, SharedSlabSource,
};
pub use client::{
    crc64_jones, key_is_dropped_by_percent, shard_id_for_key, bucket_id_for_key, stable_key_hash,
    ClientError, ClientOptions, ClientPreflightReport, ClientStats, RequestOptions, TableOptions,
    TemporalStoreClient, TemporalStorePipeline, TemporalStoreTable,
};
pub use context_workflow::{
    context_backfill_embeddings, context_embedding_ref_hash, context_pipeline_manage_report,
    context_pipeline_parity_evidence,
    context_skill_registry_from_parsed,
    context_workflow_state_report, default_context_model_providers, extract_context,
    ingest_extract_context, ingest_resource_skill_context, inject_context,
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
    StorageFeaturePageError, StorageFeaturePageLayoutReport, StorageFeaturePageTimestampMismatch,
    StorageLifecyclePlan, StorageLifecycleReport, StorageLifecycleRequest,
    StorageLogCompatibilityReport, StorageObjectLifecycleReport,
    StoragePageFormatCompatibilityReport, StorageProductionReadinessPolicy,
    StorageProductionReadinessReport, StorageProductionReadinessRequest, StorageReclaimCandidate,
    StorageRecoveryBoundaryReport, StorageRecoveryPageError, StorageRecoveryPageOwnerMismatch,
    StorageRecoveryReport, StorageRecoverySlabLiveReport, StorageSlabIntegrityReport,
    StorageTimestampedPageFamilyReport,
};
pub use engine::TemporalEngine;
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
    SharedStoreWalEntry, SharedStoreWalObject, SharedStorePageSlab, SharedStoreReplayCursor,
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
