pub mod block_store;
pub mod cache;
pub mod client;
pub mod context_workflow;
pub mod control;
pub mod data_node;
pub mod e2e;
pub mod engine;
pub mod http;
pub mod index_log;
pub mod ingestion;
pub mod meta;
pub mod partition_id;
pub mod proxy;
pub mod raft;
pub mod readiness;
pub mod rebalance;
pub mod redis;
pub mod replica_replay;
pub mod sdk;
pub mod shared_store;
pub mod types;
pub mod wal;

#[allow(deprecated)]
pub mod page_store {
    pub use crate::block_store::{
        BlockAddress as PageAddress,
        BlockStoreDelayedDestroySegmentReport as PageStoreDelayedDestroySegmentReport,
        BlockStoreError as PageStoreError, BlockStoreExtentDescriptor as PageStoreExtentDescriptor,
        BlockStoreExtentDescriptor as PageStoreZoneDescriptor,
        BlockStoreExtentState as PageStoreExtentState, BlockStoreExtentState as PageStoreZoneState,
        BlockStoreExtentSummary as PageStoreExtentSummary,
        BlockStoreExtentSummary as PageStoreZoneSummary, BlockStoreGcPolicy as PageStoreGcPolicy,
        BlockStoreGcPolicyPlan as PageStoreGcPolicyPlan, BlockStoreGcReport as PageStoreGcReport,
        BlockStoreGcUtilityCandidate as PageStoreGcUtilityCandidate,
        BlockStoreOptions as PageStoreOptions,
        BlockStorePurgeDelayedDestroyReport as PageStorePurgeDelayedDestroyReport,
        BlockStoreRollReport as PageStoreRollReport,
        BlockStoreSegmentReport as PageStoreSegmentReport, BlockStoreStats as PageStoreStats,
        LocalBlockStore as LocalPageStore,
    };
}

#[allow(deprecated)]
pub mod oplog {
    pub use crate::wal::{LocalOplogStore, OplogError, OplogGcReport, OplogRecord, OplogStats};
}

pub use block_store::{
    BlockAddress, BlockStoreExtentDescriptor, BlockStoreExtentState, BlockStoreExtentSummary,
    BlockStoreOptions, BlockStoreSegmentReport, BlockStoreStats, LocalBlockStore,
};
pub use cache::{CacheEntryInfo, CacheGcReport, CacheKey, CacheStats, MultiLayerCache};
pub use client::{
    crc64_jones, key_is_dropped_by_percent, shard_id_for_key, slot_id_for_key, stable_key_hash,
    ClientError, ClientOptions, ClientPreflightReport, ClientStats, RequestOptions, TableOptions,
    TemporalStoreClient, TemporalStorePipeline, TemporalStoreTable,
};
pub use context_workflow::{
    context_pipeline_manage_report, context_pipeline_parity_evidence,
    context_resource_chunk_embedding, context_skill_registry_from_parsed,
    context_workflow_state_report, default_context_model_providers, extract_context,
    ingest_extract_context, ingest_resource_skill_context, inject_context,
    openviking_open_source_model_profiles, parse_context_resource, parse_context_skill_markdown,
    retrieve_context, run_context_pipeline_benchmark, run_context_pipeline_benchmark_sweep,
    select_context_skills_for_retrieval, update_context_resource_lifecycle,
    update_context_skill_registry, validate_resource_skill_secondary_indexes, ContextBlock,
    ContextExtractReport, ContextExtractRequest, ContextIngestExtractReport,
    ContextIngestExtractRequest, ContextIngestExtractSummary, ContextIngestSourceFailure,
    ContextInjectReport, ContextInjectRequest, ContextModelProviderConfig,
    ContextOpenVikingModelProfile, ContextParsedResourceChunk, ContextPipelineBenchmarkQueryReport,
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
    ScanStreamResponse, SetConfigRequest, StreamKind, StreamReadRequest, StreamReadResponse,
    UnloadShardRequest, UnloadShardResponse,
};
pub use data_node::{
    CompactionRequest, CompactionResponse, DataNodeLifecycleReport, DataNodePreflightReport,
    DataNodeRuntime, DataNodeRuntimeOptions, DataNodeRuntimeStats, DataNodeShardLifecycleState,
    DataNodeTaskKind, DataNodeTaskOutput, DataNodeTaskStatus, DirtyObjectInfo, DumpShardRequest,
    DumpShardResponse, GcRequest, GcResponse, RequestController, ShardWorkerInfo,
    StorageLifecycleResponse, StorageLifecycleScheduler,
};
pub use e2e::{
    AsyncStorageJournal, EndToEndWorkflow, EndToEndWorkflowOptions, KillSwitches, RaftWriteMode,
    ReplicaReadPolicy, ReplicationMode, RoutingClient, TemporalStoreClientOptions, WorkflowError,
    WorkflowProxy,
};
pub use engine::golden::{cpp_api_golden_corpus_report, cpp_feature_sequence_golden_corpus_report};
pub use engine::reports::{
    CppGoldenCaseReport, CppGoldenCorpusReport, RustStorageObservation, ShardCompactionReport,
    ShardCompactionUtilityReport, ShardExpirySweepReport, SlotDumpFaultMatrixReport,
    SlotDumpFaultScenarioReport, SlotDumpFollowerReplayCursor, SlotDumpFollowerRetentionBlock,
    SlotDumpInstallMarker, SlotDumpInstallPreflightReport, SlotDumpInstallRollForwardReport,
    SlotDumpManifest, SlotDumpManifestChainIssue, SlotDumpManifestPrunePlan,
    SlotDumpManifestPruneReport, SlotDumpRaftSnapshotRef, SlotDumpRaftSnapshotRetentionBlock,
    SlotStorageSummary, StorageCacheInspectionReport, StorageCacheInvalidateSlotRequest,
    StorageCacheSlotSummary, StorageCacheWarmupReport, StorageFeaturePageError,
    StorageFeaturePageLayoutReport, StorageFeaturePageTimestampMismatch, StorageLifecyclePlan,
    StorageLifecycleReport, StorageLifecycleRequest, StorageLogCompatibilityReport,
    StorageObjectLifecycleReport, StoragePageFormatCompatibilityReport,
    StorageProductionReadinessPolicy, StorageProductionReadinessReport,
    StorageProductionReadinessRequest, StorageReclaimCandidate, StorageRecoveryBoundaryReport,
    StorageRecoveryPageError, StorageRecoveryPageOwnerMismatch, StorageRecoveryReport,
    StorageRecoverySegmentLiveReport, StorageSegmentIntegrityReport,
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
    MetaMutation, MetaPreflightReport, MetaStats, NamespaceMetaInfo, PartitionLoad,
    ProxyHeartbeatRequest, ProxyHeartbeatResponse, ProxyMetaInfo, RegisterProxyRequest,
    RegisterServerRequest, RegisterShardRequest, RegisterShardResponse, ServerEndpoint,
    ServerHeartbeatRequest, ServerHeartbeatResponse, ServerMetaInfo, ServerRuntimeLoad,
    ServerShardServingState, ShardLoad, ShardLocation, SingleNodeMeta, StaleResourceReport,
    StaleServerReport, StateChangeRequest, TableMetaInfo, TablePartition, TableTopologyResponse,
};
#[allow(deprecated)]
pub use wal::{
    LocalOplogStore, LocalWalStore, OplogRecord, OplogStats, WalError, WalGcReport, WalRecord,
    WalStats,
};
#[allow(deprecated)]
pub use page_store::{
    LocalPageStore, PageAddress, PageStoreOptions, PageStoreSegmentReport, PageStoreStats,
};
pub use partition_id::{
    validate_partition_count_per_set, validate_partition_set_count, PartitionId, PartitionIdError,
    MAX_PARTITION_SET_INDEX, MAX_TABLE_ID, MIN_SLOTS_PER_PARTITION, PARTITION_INDEX_MASK,
    PARTITION_VERSION_MASK, SLOT_COUNT, SLOT_MASK,
};
pub use proxy::{
    ProxyClientPreflightReport, ProxyConfigUpdateReport, ProxyCppMigrationContract, ProxyInfo,
    ProxyOpenTableRequest, ProxyOpenTableResponse, ProxyOptions, ProxyPolicyReport,
    ProxyPreflightReport, ProxyReplicaReadPolicy, ProxyService, ProxyServingMode, ProxyStats,
    ProxyTableBatchExecuteRequest, ProxyTableExecuteRequest, ProxyTableOptionsView,
};
#[cfg(feature = "temporal-raft-engine")]
pub use raft::temporal_raft_integration::{
    new_temporal_raft_data_node_backend, new_temporal_raft_metaserver_backend,
    TemporalRaftBackendReport, TemporalRaftConfig, TemporalRaftConsensusBackend,
    TemporalRaftDurableLogRecord,
    TemporalRaftDurableSnapshot, TemporalRaftEntry, TemporalRaftEntryPayload, TemporalRaftLogId,
    TemporalRaftMembership, TemporalRaftNode, TemporalRaftRuntimeKind, TemporalRaftSnapshotMeta,
    TemporalRaftStoredMembership,
};
pub use raft::{
    apply_data_raft_membership_from_topology, distributed_raft_readiness,
    handle_authenticated_raft_http, handle_raft_http, production_raft_security_from_env,
    require_production_raft_ready, validate_raft_deployment_mode, AppendEntriesRequest,
    AppendEntriesResponse, DataRaftConsensusBackend, DataRaftConsensusOptions, DataRaftPeer,
    DataRaftStatus, DataRaftTopologyApplyReport, DataRaftTopologyMembershipPlan,
    DistributedRaftCommandResponse, DistributedRaftProposeRequest, DistributedRaftReadRequest,
    HttpRaftTransport, InstallSnapshotRequest, InstallSnapshotResponse, LocalRaftWal, MetaCommand,
    MetaOwnedDataRaftMembershipReport, MetaRaftCluster, MetaState,
    TemporalRaftDataNodeProcessRolloutReport, TemporalRaftMetaProcessRolloutReport,
    TemporalRaftProcessNodeEvidence, TemporalRaftProcessOperationalSemanticsEvidence,
    ProductionMetaRaftRuntime, ProductionMetaRaftRuntimeOptions, ProductionRaftChaosPlan,
    ProductionRaftEngineKind, ProductionRaftNode, ProductionRaftProcessSpec, ProductionRaftRuntime,
    ProductionRaftRuntimeOptions, ProductionRaftSecurity, ProductionRaftSecurityEnv,
    ProductionRaftSecurityMode, ProductionRaftTimerHandle, RaftApplyHealth, RaftApplyLag,
    RaftCatchUpReport, RaftCluster, RaftClusterStatus, RaftConfig, RaftConfigError,
    RaftControlLeadershipRequest, RaftDataNodeAtomicDurabilityReport, RaftDeploymentMode,
    RaftDistributedReadiness, RaftError, RaftFailoverReport, RaftHardState, RaftMembership,
    RaftMembershipChangeKind, RaftMembershipChangePlan, RaftMembershipChangeReport, RaftNodeId,
    RaftNodeStatus, RaftProductionReadinessError, RaftReadOptions, RaftReadStrategy, RaftRole,
    RaftRpcRuntimeOptions, RaftTickOutcome, RaftTransport, RaftWalRecord, RaftWalSegmentInfo,
    RaftWalSegmentReport, ReadIndexResponse, UnavailableDataRaftConsensusBackend, VoteRequest,
    VoteResponse,
};
pub use readiness::{
    metaserver_scheduler_execution_readiness_report, production_readiness_report,
    MetaServerSchedulerExecutionReadinessReport, ProductionReadinessReport, ReadinessArea,
    ReadinessCapabilityBlocker, ServiceReadinessGateReport, ServiceReadinessSummary,
};
pub use rebalance::{
    CppPartitionSetMember, CppPartitionSetTopology, MembershipUpdatePeerRequest,
    MembershipUpdatePeerStatus, MembershipUpdateTaskOptions, MembershipUpdateTaskPlan,
    MembershipUpdateTaskReport, NetworkSchedulerTaskExecution, RaftPersistedSchedulerState,
    RebalanceController, RebalanceError, RebalanceOptions, RebalanceRoundReport, RebalanceStep,
    SchedulerLifecycleToken, ShardMovePlan, ShardReplica, ShardReplicaState, ShardRole,
};
pub use redis::{execute_redis_command, read_command, serve_redis_proxy, RespValue};
pub use replica_replay::{
    HttpReplicaStreamSource, ReplicaReplayCursor, ReplicaReplayError, ReplicaReplayLoop,
    ReplicaReplayOptions, ReplicaReplayReport, ReplicaReplayRequest, ReplicaReplayResponse,
    ReplicaStreamSource,
};
pub use shared_store::{
    ReplayReport, SharedStoreCheckpointManifest, SharedStoreFlushReport, SharedStoreGcReport,
    SharedStoreOplogEntry, SharedStoreOplogObject, SharedStorePageSegment, SharedStoreReplayCursor,
    SharedStoreReplicationError, SharedStoreReplicator, SharedStoreRetryPolicy,
    SharedStoreStorageMode, SharedStoreStorageWriter, SharedStoreWriteReport,
};
pub use types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ContextAuditModel,
    ContextAuditRef, ContextChildModel, ContextChildRef, ContextCompressionEvent,
    ContextCompressionModel, ContextDirtyModel, ContextEmbedding, ContextEmbeddingModel,
    ContextEntity, ContextEvent, ContextEventModel, ContextExtractedEventIndexes,
    ContextIndexModel, ContextIndexRef, ContextNode, ContextNodeModel, ContextPackAudit,
    ContextSegment, ContextSummary, ContextSummaryDirtyMarker, ContextSummaryModel,
    ContextTraversedNode, ContextWire, EventReplicationMode, EventReplicationSelectionReport,
    ExecuteRequest, ExecuteResponse, FeaturePoint, InternalContextIndex, IpsSnapshotReport,
    IpsStats, ReplicatedBatchExecuteRequest, ReplicatedBatchExecuteResponse, ReplicatedCommand,
    ReplicatedExecuteRequest, ShardId, Status,
};
pub use wal::{
    LocalWriteAheadLogStore, WriteAheadLogError, WriteAheadLogGcReport, WriteAheadLogRecord,
    WriteAheadLogStats,
};
