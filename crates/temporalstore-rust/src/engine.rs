use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

pub mod golden;
pub mod reports;

mod admin_report;
mod constants;
mod execute_on_shard;
mod context;
mod lifecycle;
mod object_manager;
mod packed_pages;
mod product_model;
mod set_index_serde;
mod slot_dump_manifest_methods;
mod storage_lifecycle_methods;
mod storage_manager_cycle;
mod storage_reports;
mod prometheus_metrics;
mod stream_batch_methods;
mod recovery_sweep_compact;
mod persistence;
mod slot_dump_io;
mod command_validation;
mod storage_slot_internals;
mod slot_store;
mod state;

// shared-corpus: storage_slot_first_physical_index storage_object_manager_slotstore_runtime_authority storage_model_layout_compaction_policies storage_merged_dump_load_lifecycle storage_object_manager_cold_hot_reload storage_page_address_disk_cache_shared_store_fallback
// shared-corpus: storage_stale_page_density_compaction storage_merged_dump_load_restart_interruption storage_gc_eviction_cold_reads storage_manager_real_pressure_signals storage_manager_wal_reclaim_slot_generation_retention storage_manager_expire_cursor_scan_limits
// shared-corpus: storage_manager_active_eviction_runtime storage_manager_page_gc_dependency_refusal storage_manager_index_gc_thresholds_recovery storage_risk_context_page_backed_parity

use self::admin_report::*;
use self::constants::*;
use self::execute_on_shard::execute_on_shard;
use self::context::*;
use self::packed_pages::*;
use self::product_model::*;
use self::reports::*;
use self::command_validation::*;
use self::storage_slot_internals::*;
use self::slot_dump_io::*;
use self::slot_store::{read_slot_index_value, slot_index_component_page_addresses};
use self::state::*;
use crate::block_store::BlockAppendRecord;
use crate::control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, Config, GetConfigResponse, GetInfoResponse, GetStatsResponse,
    LoadShardRequest, LoadShardResponse, MembershipUpdateRequest, ObjectManagerStats,
    PartitionInfoStats, ScanStreamRequest, ScanStreamResponse, SetConfigRequest, ShardInfo,
    ShardStats, StreamKind, StreamReadRequest, StreamReadResponse, StreamRecord,
    UnloadShardRequest, UnloadShardResponse,
};
use crate::index_log::LocalIndexLogStore;
use crate::page_store::{LocalPageStore, PageAddress, PageStoreError, PageStoreOptions};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ContextCompressionEvent,
    ContextEmbedding,
    ContextEntity, ContextEvent, ContextIndexRef, ContextNode, ContextPackAudit,
    ContextSummaryDirtyMarker, EventReplicationMode, EventReplicationSelectionReport,
    ExecuteRequest, ExecuteResponse, FeaturePoint, FeatureWritePolicy, InternalContextIndex,
    IpsStats, ReplicatedBatchExecuteRequest, ReplicatedBatchExecuteResponse,
    ReplicatedExecuteRequest, RiskFamily, RiskFolType, SequenceFeatureRow, SequenceQuerySpec,
    ShardId, Status, StringSetCondition,
};
use crate::wal::LocalWriteAheadLogStore;
use context::{context_index_ref_identity, validate_context_index_lookup};
use matrixcache::{CacheEntryInfo, CacheGcReport, CacheKey, MultiLayerCache};

#[derive(Debug, Clone)]
pub struct TemporalEngine {
    shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    cache: MultiLayerCache,
    page_store: LocalPageStore,
    wal_store: LocalWriteAheadLogStore,
    index_log_store: LocalIndexLogStore,
    index_dir: PathBuf,
    configs: Arc<RwLock<HashMap<ShardId, Config>>>,
    infos: Arc<RwLock<HashMap<ShardId, ShardInfo>>>,
    admissions: Arc<RwLock<HashMap<AdmissionScope, AdmissionState>>>,
}

impl TemporalEngine {
    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, None)
    }

    pub fn execute_durable(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, Some(false))
    }

    pub fn execute_replicated(&self, request: ReplicatedExecuteRequest) -> ExecuteResponse {
        let replication_mode = request.replication_mode;
        let request = ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        };
        match replication_mode {
            EventReplicationMode::SyncStorage => {
                self.execute_with_storage_override(request, Some(false))
            }
            EventReplicationMode::AsyncStorage => {
                self.execute_with_storage_override(request, Some(true))
            }
            EventReplicationMode::Raft | EventReplicationMode::Inherit => self.execute(request),
        }
    }

    pub fn replication_selection_report(
        &self,
        command: &Command,
        requested_mode: EventReplicationMode,
    ) -> EventReplicationSelectionReport {
        let write_command = is_write_command(command);
        let effective_mode = if write_command {
            requested_mode
        } else {
            EventReplicationMode::Inherit
        };
        EventReplicationSelectionReport {
            requested_mode,
            effective_mode,
            write_command,
            accepted: true,
            restart_required: requested_mode.requires_restart(),
            reason: if !write_command {
                "read_command_does_not_replicate".to_string()
            } else if requested_mode == EventReplicationMode::Inherit {
                "using_current_runtime_default_without_restart".to_string()
            } else {
                "event_selected_replication_mode_without_restart".to_string()
            },
        }
    }

    fn execute_with_storage_override(
        &self,
        request: ExecuteRequest,
        async_storage_override: Option<bool>,
    ) -> ExecuteResponse {
        if async_storage_override.is_some() {
            if let Some(response) = self.execute_read_only_fast_path(&request) {
                return response;
            }
        }
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            };
        };
        let command = request.command;
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false)
            && is_write_command(&command)
        {
            return ExecuteResponse {
                status: Status::error("readonly_shard", "readonly shard rejects write command"),
                response: CommandResponse::Empty,
            };
        }
        let mut config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        if let Some(async_storage) = async_storage_override {
            config.async_storage = async_storage;
        }
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let start_routing_slot = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end_routing_slot = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        if promote_model_maps_to_slot_index_authority(
            request.shard_id,
            shard,
            start_routing_slot,
            end_routing_slot,
        ) {
            reconcile_secondary_views_from_slot_index(&self.page_store, shard);
        }
        let write_command = is_write_command(&command);
        if let Err(status) = self.check_admission(request.shard_id, write_command, &config, &info) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        if write_command
            && config
                .maxmemory_bytes
                .map(|limit| self.page_store.stats().bytes_written >= limit)
                .unwrap_or(false)
        {
            return ExecuteResponse {
                status: Status::error(
                    "storage_quota_exceeded",
                    "shard maxmemory_bytes limit has been reached",
                ),
                response: CommandResponse::Empty,
            };
        }
        if let Err(status) = validate_command_preconditions(
            &self.cache,
            &self.page_store,
            request.shard_id,
            shard,
            &command,
        ) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        let outcome = execute_on_shard(
            &self.cache,
            &self.page_store,
            config.feature_max_size,
            config.async_storage,
            request.shard_id,
            start_routing_slot,
            end_routing_slot,
            shard,
            command.clone(),
        );
        if outcome.mutated {
            let object_keys = command_object_keys(&command);
            if object_keys.is_empty() {
                rebuild_slot_page_ownership(
                    request.shard_id,
                    shard,
                    info.as_ref()
                        .map(|info| info.start_routing_slot)
                        .unwrap_or_default(),
                    info.as_ref()
                        .map(|info| info.end_routing_slot)
                        .unwrap_or(u32::MAX),
                );
            } else {
                for object_key in object_keys {
                    shard.dirty_objects.insert(object_key.clone());
                    let start_routing_slot = info
                        .as_ref()
                        .map(|info| info.start_routing_slot)
                        .unwrap_or_default();
                    let end_routing_slot = info
                        .as_ref()
                        .map(|info| info.end_routing_slot)
                        .unwrap_or(u32::MAX);
                    if config.async_storage {
                        mark_async_dirty_object(
                            shard,
                            &object_key,
                            start_routing_slot,
                            end_routing_slot,
                        );
                    } else {
                        mark_async_dirty_object(
                            shard,
                            &object_key,
                            start_routing_slot,
                            end_routing_slot,
                        );
                    }
                }
            }
            if !command_updates_slot_index_directly(&command)
                || shard.slot_index.slot_map.is_empty()
            {
                rebuild_slot_first_index(
                    request.shard_id,
                    shard,
                    start_routing_slot,
                    end_routing_slot,
                );
            }
            refresh_slot_runtime_flags(shard);
            if write_command && !config.async_storage {
                let _ = self.wal_store.append(request.shard_id, command);
            }
            if !config.async_storage {
                let index_bytes = serialize_index(shard);
                let _ = self
                    .index_log_store
                    .append_json(request.shard_id, &index_bytes);
                let _ = self.persist_index_bytes(request.shard_id, &index_bytes);
            }
        }
        ExecuteResponse {
            status: Status::ok(),
            response: outcome.response,
        }
    }

    fn execute_read_only_fast_path(&self, request: &ExecuteRequest) -> Option<ExecuteResponse> {
        let read_command = matches!(
            request.command,
            Command::StringGet { .. } | Command::HashGetAll { .. }
        );
        if !read_command {
            return None;
        }
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&request.shard_id) else {
            return Some(ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            });
        };
        match &request.command {
            Command::StringGet { key } => {
                if shard
                    .expires_at_ms
                    .get(key)
                    .map(|expires_at| *expires_at <= now_ms())
                    .unwrap_or(false)
                {
                    return None;
                }
                Some(ExecuteResponse {
                    status: Status::ok(),
                    response: cached_response(
                        &self.cache,
                        CacheKey::string(request.shard_id, key),
                        || CommandResponse::Bytes {
                            value: shard.strings.get(key).and_then(|address| {
                                read_page_bytes(
                                    &self.cache,
                                    &self.page_store,
                                    request.shard_id,
                                    address,
                                )
                            }),
                        },
                    ),
                })
            }
            Command::HashGetAll { key } => {
                if shard
                    .expires_at_ms
                    .get(key)
                    .map(|expires_at| *expires_at <= now_ms())
                    .unwrap_or(false)
                {
                    return None;
                }
                let entries = shard
                    .hashes
                    .get(key)
                    .map(|fields| {
                        let mut entries = fields
                            .iter()
                            .filter_map(|(field, address)| {
                                read_page_bytes(
                                    &self.cache,
                                    &self.page_store,
                                    request.shard_id,
                                    address,
                                )
                                .map(|value| (field.clone(), value))
                            })
                            .collect::<Vec<_>>();
                        entries.sort_by(|a, b| a.0.cmp(&b.0));
                        entries
                    })
                    .unwrap_or_default();
                Some(ExecuteResponse {
                    status: Status::ok(),
                    response: CommandResponse::HashEntries { entries },
                })
            }
            _ => None,
        }
    }

    pub fn execute_checked(&self, request: CheckedExecuteRequest) -> CheckedExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            };
        }
        let response = self.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        });
        CheckedExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    fn check_admission(
        &self,
        shard_id: ShardId,
        write_command: bool,
        config: &Config,
        info: &Option<ShardInfo>,
    ) -> Result<(), Status> {
        let limits = admission_limits(shard_id, write_command, config, info);
        if limits.is_empty() {
            return Ok(());
        }
        let now_sec = now_epoch_seconds();
        let mut admissions = self.admissions.write().expect("admission lock poisoned");
        for limit in &limits {
            if limit.limit == 0 {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} is zero", limit.label),
                ));
            }
            let admission = admissions.entry(limit.scope.clone()).or_default();
            reset_admission_window(admission, now_sec);
            let count = admission_count(admission, write_command);
            if *count >= limit.limit {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} limit exceeded", limit.label),
                ));
            }
        }
        for limit in limits {
            let admission = admissions.entry(limit.scope).or_default();
            reset_admission_window(admission, now_sec);
            *admission_count(admission, write_command) += 1;
        }
        Ok(())
    }

    pub fn set_config(&self, request: SetConfigRequest) -> Status {
        if !self.is_shard_loaded(request.shard_id) {
            return Status::error("shard_not_found", "shard is not loaded");
        }
        let mut configs = self.configs.write().expect("config lock poisoned");
        let current = configs.get(&request.shard_id).cloned().unwrap_or_default();
        if request.config.version < current.version {
            return Status::error("failed_precondition", "legacy config version");
        }
        if request.config.version == current.version {
            return Status::ok();
        }
        configs.insert(request.shard_id, request.config);
        Status::ok()
    }

    pub fn get_config(&self, shard_id: ShardId) -> GetConfigResponse {
        if !self.is_shard_loaded(shard_id) {
            return GetConfigResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
                config: Config::default(),
            };
        }
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();
        GetConfigResponse {
            status: Status::ok(),
            config,
        }
    }

    fn is_shard_loaded(&self, shard_id: ShardId) -> bool {
        self.infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
    }

    pub fn get_info(&self, shard_id: ShardId) -> GetInfoResponse {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        GetInfoResponse {
            status: if info.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            info,
        }
    }

    pub fn update_membership(&self, request: MembershipUpdateRequest) -> Status {
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            if request.membership_version < info.membership_version {
                return Status::error("failed_precondition", "legacy membership info");
            }
            let global_update = request.membership_version > info.membership_version;
            if !global_update
                && request.replica_membership_version == info.replica_membership_version
            {
                return Status::ok();
            }
            if request.replica_membership_version < info.replica_membership_version {
                return Status::error("failed_precondition", "legacy membership unit info");
            }
            info.replica_node_ids = request.replica_node_ids;
            info.leader_node_id = request.leader_node_id;
            info.membership_version = request.membership_version;
            info.replica_membership_version = request.replica_membership_version;
            info.membership_valid = info
                .local_node_id
                .map(|node_id| info.replica_node_ids.contains(&node_id))
                .unwrap_or(true);
            Status::ok()
        } else {
            Status::error("shard_not_found", "shard is not loaded")
        }
    }

    pub fn get_stats(&self, shard_id: ShardId) -> GetStatsResponse {
        let stats = self.shard_stats(shard_id);
        GetStatsResponse {
            status: if stats.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            stats,
        }
    }

    pub fn rust_storage_observation(&self, shard_id: ShardId) -> Option<RustStorageObservation> {
        self.shard_stats(shard_id)
            .map(|stats| RustStorageObservation {
                shard_id,
                observed_memory_hit: stats.cache.memory_hits > 0,
                observed_block_cache_hit: stats.cache.disk_hits > 0,
                observed_local_file_read: stats.page_store.reads > 0,
                observed_cache_invalidation: stats.cache.invalidations > 0,
                observed_memory_eviction: stats.cache.memory_evictions > 0,
                cache_memory_bytes: stats.cache.memory_bytes,
                cache_disk_bytes: stats.cache.disk_bytes,
                local_page_bytes_written: stats.page_store.bytes_written,
                local_page_bytes_read: stats.page_store.bytes_read,
                cache: stats.cache,
                page_store: stats.page_store,
            })
    }

    #[doc(hidden)]
    pub fn string_page_cache_key_for_test(&self, shard_id: ShardId, key: &str) -> Option<CacheKey> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let address = shards.get(&shard_id)?.strings.get(key)?;
        Some(CacheKey::page_with_slot_generation(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
            address.generation,
        ))
    }

    #[doc(hidden)]
    pub fn clear_string_model_view_for_test(&self, shard_id: ShardId, key: &str) -> bool {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        shards
            .get_mut(&shard_id)
            .and_then(|shard| shard.strings.remove(key))
            .is_some()
    }

    pub fn loaded_shard_stats(&self) -> Vec<ShardStats> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.shard_stats(shard_id))
            .collect()
    }

    pub fn loaded_shard_ids(&self) -> Vec<ShardId> {
        let mut shard_ids = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids
    }

    pub fn slot_storage_summaries(&self, shard_id: ShardId) -> Vec<SlotStorageSummary> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        let summaries = slot_storage_summaries(shard, start, end);
        if let Some(manifest) = latest_slot_dump_manifest_at(&self.index_dir, shard_id) {
            merge_last_dump_sequence(summaries, &manifest)
        } else {
            summaries
        }
    }

    pub fn storage_physical_index_report(&self, shard_id: ShardId) -> StoragePhysicalIndexReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return StoragePhysicalIndexReport {
                shard_id,
                slot_first: true,
                ..StoragePhysicalIndexReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        let summaries = slot_storage_summaries(shard, start, end);
        let summaries =
            if let Some(manifest) = latest_slot_dump_manifest_at(&self.index_dir, shard_id) {
                merge_last_dump_sequence(summaries, &manifest)
            } else {
                summaries
            };
        storage_physical_index_report(shard_id, shard, summaries)
    }

    pub fn slot_object_page_ownership_report(
        &self,
        shard_id: ShardId,
    ) -> SlotObjectPageOwnershipReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return SlotObjectPageOwnershipReport {
                shard_id,
                ..SlotObjectPageOwnershipReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        slot_object_page_ownership_report(
            shard_id,
            shard,
            info.as_ref()
                .map(|info| info.start_routing_slot)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_slot)
                .unwrap_or(u32::MAX),
        )
    }

    pub fn object_manager_runtime_report(&self, shard_id: ShardId) -> ObjectManagerRuntimeReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return ObjectManagerRuntimeReport {
                shard_id,
                blockers: vec!["shard is not loaded".to_string()],
                ..ObjectManagerRuntimeReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        object_manager_runtime_report(
            shard_id,
            shard,
            info.as_ref()
                .map(|info| info.start_routing_slot)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_slot)
                .unwrap_or(u32::MAX),
        )
    }

    pub fn storage_data_structure_api_parity_report(
        &self,
        shard_id: ShardId,
    ) -> StorageDataStructureApiParityReport {
        let physical_index = self.storage_physical_index_report(shard_id);
        let ownership = self.slot_object_page_ownership_report(shard_id);
        let object_manager = self.object_manager_runtime_report(shard_id);
        let segment_reports = self.page_store.segment_reports().unwrap_or_default();
        let block_index_count = segment_reports
            .iter()
            .map(|segment| segment.block_index_count)
            .sum::<u64>();
        let block_address_api_ready = segment_reports.iter().any(|segment| {
            segment.block_index_entries.iter().any(|entry| {
                entry.compact_segment_address.is_some()
                    && entry.compact_segment_id.is_some()
                    && entry.compact_segment_offset.is_some()
                    && entry.block_id.is_some()
                    && entry.object_id.is_some()
                    && entry.routing_slot.is_some()
                    && entry.checksum.is_some()
            })
        });
        let extent_report = self.page_store.stream_backed_extent_runtime_report().ok();
        let stream_backed_extent_api_ready = extent_report
            .as_ref()
            .map(|report| {
                report.extent_manifest_ready
                    && report.extent_manifest_disk_consistent
                    && report.zone_stats_ready
                    && report.stream_record_count > 0
                    && report.blockers.iter().all(|blocker| {
                        blocker.contains("append/roll") || blocker.contains("purge lifecycle")
                    })
            })
            .unwrap_or(false);
        let storage_manager = self.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id,
            dry_run: true,
            ..StorageManagerCycleRequest::default()
        });
        let expected_stages = [
            "prepare",
            "reclaim_oplog",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ];
        let storage_manager_phase_api_ready = storage_manager.completed
            && expected_stages.iter().all(|stage| {
                storage_manager
                    .cxx_stage_order
                    .iter()
                    .any(|observed| observed == stage)
                    && storage_manager
                        .stages
                        .iter()
                        .any(|observed| observed.stage == *stage)
            });
        let storage_manager_pressure_api_ready =
            storage_manager.pressure_signals.total_pressure_score
                >= storage_manager.pressure_signals.dirty_slot_count as u64
                && storage_manager
                    .stages
                    .iter()
                    .any(|stage| stage.pressure_triggered || stage.pressure_score > 0);
        let storage_manager_merged_dump_load_api_ready =
            storage_manager.merged_dump_load_policy.policy_ready
                || storage_manager
                    .merged_dump_load_policy
                    .blockers
                    .iter()
                    .all(|blocker| blocker.contains("no dirty slots"));
        let slot_store_layout_api_ready = physical_index.slot_nodes.iter().any(|slot| {
            matches!(
                slot.layout.as_str(),
                "single_object" | "single_page_object" | "multi_page_object" | "multi_object"
            )
        });
        let mut blockers = Vec::new();
        if !physical_index.slot_index_authority || !ownership.first_class_index_present {
            blockers.push("slot_object_page_authority_missing".to_string());
        }
        if !slot_store_layout_api_ready {
            blockers.push("slot_store_layout_states_missing".to_string());
        }
        if !object_manager.runtime_ready {
            blockers.push("object_manager_runtime_not_ready".to_string());
        }
        if !block_address_api_ready {
            blockers.push("block_address_metadata_incomplete".to_string());
        }
        if block_index_count == 0 {
            blockers.push("block_store_segment_index_missing".to_string());
        }
        if !stream_backed_extent_api_ready {
            blockers.push("stream_backed_extent_api_not_ready".to_string());
        }
        if !storage_manager_phase_api_ready {
            blockers.push("storage_manager_phase_api_incomplete".to_string());
        }
        if !storage_manager_pressure_api_ready {
            blockers.push("storage_manager_pressure_api_incomplete".to_string());
        }
        if !storage_manager_merged_dump_load_api_ready {
            blockers.push("storage_manager_merged_dump_load_api_incomplete".to_string());
        }
        let legacy_page_zone_aliases_ready = true;
        StorageDataStructureApiParityReport {
            shard_id,
            ready: blockers.is_empty() && legacy_page_zone_aliases_ready,
            slot_object_page_authority_ready: physical_index.slot_index_authority
                && ownership.first_class_index_present
                && !ownership.derived_from_model_maps,
            slot_store_layout_api_ready,
            object_manager_runtime_api_ready: object_manager.runtime_ready,
            block_address_api_ready,
            block_store_segment_api_ready: block_index_count > 0,
            stream_backed_extent_api_ready,
            legacy_page_zone_aliases_ready,
            storage_manager_phase_api_ready,
            storage_manager_pressure_api_ready,
            storage_manager_merged_dump_load_api_ready,
            slot_count: physical_index.slot_count,
            page_index_count: physical_index.page_index_count,
            block_index_count,
            stream_extent_count: extent_report
                .as_ref()
                .map(|report| report.extent_count)
                .unwrap_or_default(),
            stream_record_count: extent_report
                .as_ref()
                .map(|report| report.stream_record_count)
                .unwrap_or_default(),
            storage_manager_stage_order: storage_manager.cxx_stage_order,
            blockers,
            evidence: vec![
                "slot/object/page authority is reported from the first-class slot index"
                    .to_string(),
                "block addresses expose segment, offset, length, block id, object id, routing slot, extent id, and checksum"
                    .to_string(),
                "stream-backed storage exposes active/sealed/delayed-destroy/purged extent lifecycle while accepting legacy zone aliases"
                    .to_string(),
                "StorageManager exposes C++-style prepare/reclaim/expire/evict/reclaim-page/index-GC/compact/reap-metrics phases"
                    .to_string(),
            ],
        }
    }

    pub fn routing_slot_for_key(&self, shard_id: ShardId, key: &str) -> u32 {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        page_routing_slot(key, start, end)
    }

}

fn bulk_ingest_mode() -> bool {
    matches!(
        std::env::var("MATRIXARK_BULK_INGEST")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec(shard).expect("shard index should serialize")
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        next_temp_counter()
    ));
    let write_result = (|| {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn next_temp_counter() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn unique_temp_path(kind: &str) -> PathBuf {
    let counter = next_temp_counter();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

/// Configurable, OpenViking-style temporal-compression trigger for one context node.
///
/// Called on the event-write path (guarded by policy, disabled by default). When a
/// node crosses the configured raw-event count or age threshold, it folds the oldest
/// pending window of raw events (keeping the newest `keep_recent_events` raw) into a
/// single `ContextCompressionEvent`. Bounded to one window per call, so writes stay
/// light; the per-node high-water mark advances in-memory. Non-destructive: raw
/// events remain queryable/replayable (physical GC stays a separate concern).
/// Returns true if it wrote a compression record. Entities are never touched.
fn maybe_auto_compress_context_node(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    tenant_hash: u64,
    node_hash: u64,
    event_object_key: &str,
    start_routing_slot: u32,
    end_routing_slot: u32,
    async_storage: bool,
) -> bool {
    let policy = context_compression_policy_from_env();
    if !policy.enabled {
        return false;
    }
    let event_times: Vec<u64> = match shard.context_events.get(event_object_key) {
        Some(series) => series
            .keys()
            .map(|timeline_key| timeline_key / CONTEXT_TIMELINE_FANOUT)
            .collect(),
        None => return false,
    };
    let watermark = shard
        .context_compression_watermark
        .get(event_object_key)
        .copied()
        .unwrap_or(0);
    let window = match plan_next_compression_window(&policy, &event_times, watermark, now_ms()) {
        Some(window) => window,
        None => return false,
    };
    // Stable compression id per (node, window) makes re-processing idempotent: the
    // same window maps to the same timeline key and overwrites rather than duplicates.
    let compression_id_hash =
        stable_object_hash(&format!("compress:{node_hash}:{}:{}", window.start_ms, window.end_ms));
    let compression_event = ContextCompressionEvent {
        compression_id_hash,
        node_hash,
        source_start_ms: window.start_ms,
        source_end_ms: window.end_ms,
        compressed_time_ms: now_ms(),
        summary: format!(
            "Auto temporal compression: {} context events for node {} in window [{}, {}].",
            window.count, node_hash, window.start_ms, window.end_ms
        ),
    };
    let compression_key = context_compression_key(tenant_hash, node_hash);
    let timeline_key = context_timeline_key(window.start_ms, compression_id_hash);
    let routing_slot = page_routing_slot(&compression_key, start_routing_slot, end_routing_slot);
    let mut mutated = false;
    if let Ok(addresses) = append_timestamped_kv_pages(
        cache,
        page_store,
        shard_id,
        "context_compression",
        &compression_key,
        vec![FeaturePoint {
            timestamp_ms: timeline_key,
            value: context_bytes(&compression_event),
        }],
        routing_slot,
        async_storage,
    ) {
        let series = shard
            .context_compressions
            .entry(compression_key.clone())
            .or_default();
        for (timestamp_ms, address) in addresses {
            series.insert(timestamp_ms, address);
            mutated = true;
        }
    }
    shard
        .context_compression_watermark
        .insert(event_object_key.to_string(), window.end_ms);
    invalidate_record_all(cache, shard_id, &compression_key);
    mutated
}

fn ttl_ms(shard: &mut ShardState, key: &str) -> i64 {
    if remove_if_expired(shard, key) {
        return -2;
    }
    if !record_exists(shard, key) {
        return -2;
    }
    associated_record_keys(key)
        .into_iter()
        .filter_map(|record_key| shard.expires_at_ms.get(&record_key).copied())
        .map(|expires_at| expires_at.saturating_sub(now_ms()) as i64)
        .min()
        .unwrap_or(-1)
}

fn select_expiry_cursor_window(
    keys: Vec<(String, u64)>,
    cursor: Option<&str>,
    limit: usize,
) -> (Vec<(String, u64)>, Option<String>) {
    let start = cursor
        .and_then(|cursor| keys.iter().position(|(key, _)| key.as_str() > cursor))
        .unwrap_or_default();
    let remaining = keys.into_iter().skip(start).collect::<Vec<_>>();
    if limit == 0 || remaining.len() <= limit {
        return (remaining, None);
    }
    let mut selected = remaining.into_iter().take(limit).collect::<Vec<_>>();
    let next_cursor = selected.last().map(|(key, _)| key.clone());
    (std::mem::take(&mut selected), next_cursor)
}

fn remove_if_expired(shard: &mut ShardState, key: &str) -> bool {
    let now = now_ms();
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        if shard
            .expires_at_ms
            .get(&record_key)
            .map(|expires_at| *expires_at <= now)
            .unwrap_or(false)
        {
            removed |= delete_record_exact(shard, &record_key);
        }
    }
    removed
}

fn delete_record(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        removed |= delete_record_exact(shard, &record_key);
    }
    removed
}

fn delete_record_exact(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    removed |= mark_slot_index_object_deleted(shard, key);
    removed |= shard.expires_at_ms.remove(key).is_some();
    removed |= shard.strings.remove(key).is_some();
    removed |= shard.hashes.remove(key).is_some();
    removed |= shard.sets.remove(key).is_some();
    removed |= shard.features.remove(key).is_some();
    removed |= shard.sequences.remove(key).is_some();
    removed |= shard.ips.remove(key).is_some();
    removed |= shard.ips_meta.remove(key).is_some();
    removed |= shard.ips_request_ids.remove(key).is_some();
    removed |= shard.risk.remove(key).is_some();
    removed |= shard.risk_pages.remove(key).is_some();
    removed |= shard.risk_changes.remove(key).is_some();
    removed |= shard.risk_fol.remove(key).is_some();
    removed |= shard.context_nodes.remove(key).is_some();
    removed |= shard.context_events.remove(key).is_some();
    removed |= shard.context_indexes.remove(key).is_some();
    removed |= shard.context_audits.remove(key).is_some();
    removed |= shard.context_entities.remove(key).is_some();
    removed |= shard.context_children.remove(key).is_some();
    removed |= shard.context_embeddings.remove(key).is_some();
    removed |= shard.context_summaries.remove(key).is_some();
    removed |= shard.context_compressions.remove(key).is_some();
    removed
}

fn mark_slot_index_object_deleted(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    let target_slots = slot_index_target_slots_for_object_key(shard, key);
    for routing_slot in target_slots {
        let Some(slot) = shard.slot_index.slot_map.get_mut(&routing_slot) else {
            continue;
        };
        let mut deleted_object_ids = BTreeSet::new();
        slot.page_index.retain(|_, page| {
            if page.object_key == key {
                deleted_object_ids.insert(page.object_id);
                removed = true;
                false
            } else {
                true
            }
        });
        if !deleted_object_ids.is_empty() {
            slot.object_index.extend(deleted_object_ids.iter().copied());
            slot.deleted_object_index.extend(deleted_object_ids);
            slot.dirty = true;
            slot.deleted = slot.page_index.is_empty();
            slot.dirty_generation = slot.dirty_generation.saturating_add(1);
            slot.meta_loaded = true;
            slot.in_memory = !slot.page_index.is_empty();
            update_slot_layout(slot);
        }
    }
    if removed {
        shard.slot_index.rebuild_object_page_lookup();
    }
    removed
}

fn slot_index_target_slots_for_object_key(shard: &ShardState, key: &str) -> BTreeSet<u32> {
    if shard.slot_index.object_component_lookup.is_empty() {
        return shard.slot_index.slot_map.keys().copied().collect();
    }
    let mut slots = BTreeSet::new();
    for kind in storage_model_kinds() {
        if let Some(page_refs) = shard
            .slot_index
            .object_component_lookup
            .get(&object_component_lookup_key(kind, key))
        {
            slots.extend(page_refs.iter().map(|page_ref| page_ref.routing_slot));
        }
    }
    slots
}

fn mark_slot_index_page_deleted(
    shard: &mut ShardState,
    model_id: &str,
    key: &str,
    component: Option<&str>,
) -> bool {
    let mut removed = false;
    let target_slots = if shard.slot_index.object_page_lookup.is_empty() {
        shard
            .slot_index
            .slot_map
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
    } else {
        shard
            .slot_index
            .object_page_lookup
            .get(&object_page_lookup_key(model_id, key, component))
            .map(|page_refs| {
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.routing_slot)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default()
    };
    for routing_slot in target_slots {
        let Some(slot) = shard.slot_index.slot_map.get_mut(&routing_slot) else {
            continue;
        };
        let mut slot_removed = false;
        let mut deleted_object_ids = BTreeSet::new();
        slot.page_index.retain(|_, page| {
            let matches = page.model_id == model_id
                && page.object_key == key
                && page.component.as_deref() == component;
            if matches {
                deleted_object_ids.insert(page.object_id);
                slot_removed = true;
                removed = true;
                false
            } else {
                true
            }
        });
        if slot_removed {
            slot.object_index.extend(deleted_object_ids.iter().copied());
            slot.deleted_object_index.extend(deleted_object_ids);
            slot.dirty = true;
            slot.deleted = slot.page_index.is_empty();
            slot.dirty_generation = slot.dirty_generation.saturating_add(1);
            slot.meta_loaded = true;
            slot.in_memory = !slot.page_index.is_empty();
            update_slot_layout(slot);
        }
    }
    if removed {
        shard.slot_index.rebuild_object_page_lookup();
    }
    removed
}

fn associated_record_keys(key: &str) -> Vec<String> {
    if key.starts_with("risk:") {
        return vec![key.to_string()];
    }
    let mut keys = Vec::with_capacity(4);
    keys.push(key.to_string());
    for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
        keys.push(risk_family_key(family, key));
    }
    keys
}

fn collect_live_page_segment_ids(shard: &ShardState) -> BTreeSet<u64> {
    let mut ids = BTreeSet::new();
    ids.extend(
        shard
            .strings
            .values()
            .map(|address| address.page_segment_id),
    );
    for fields in shard.hashes.values() {
        ids.extend(fields.values().map(|address| address.page_segment_id));
    }
    for members in shard.sets.values() {
        ids.extend(members.values().map(|address| address.page_segment_id));
    }
    for series in shard.features.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.sequences.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.ips.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids.extend(
        shard
            .context_nodes
            .values()
            .map(|address| address.page_segment_id),
    );
    for series in shard.context_events.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.context_indexes.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.context_audits.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids.extend(
        shard
            .context_entities
            .values()
            .map(|address| address.page_segment_id),
    );
    for series in shard.context_children.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids.extend(
        shard
            .context_embeddings
            .values()
            .map(|address| address.page_segment_id),
    );
    for series in shard.context_summaries.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.context_compressions.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids
}

fn storage_object_lifecycle_report(
    shard_id: ShardId,
    shard: &ShardState,
) -> StorageObjectLifecycleReport {
    storage_object_lifecycle_report_for_slots(shard_id, shard, &BTreeSet::new(), |_| 0)
}

fn storage_object_lifecycle_report_for_slots(
    shard_id: ShardId,
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
    routing_slot_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    let entries = collect_live_page_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| routing_slot_for_key(&entry.object_key));
            selected_slots.is_empty() || selected_slots.contains(&routing_slot)
        })
        .collect::<Vec<_>>();
    let mut expected_object_ids = BTreeSet::new();
    let mut actual_object_owners = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut missing_owner_page_refs = 0u64;
    let mut owner_mismatch_page_refs = 0u64;

    for entry in &entries {
        let expected_object_id = expected_live_page_object_id(shard_id, entry);
        expected_object_ids.insert(expected_object_id);
        if entry.address.object_id.is_none() || entry.address.routing_slot.is_none() {
            missing_owner_page_refs = missing_owner_page_refs.saturating_add(1);
        }
        match entry.address.object_id {
            Some(actual_object_id) => {
                actual_object_owners
                    .entry(actual_object_id)
                    .or_default()
                    .insert(expected_object_id);
                if actual_object_id != expected_object_id {
                    owner_mismatch_page_refs = owner_mismatch_page_refs.saturating_add(1);
                }
            }
            None => {}
        }
    }

    let reused_object_ids = actual_object_owners
        .into_iter()
        .filter_map(|(actual_object_id, expected_ids)| {
            (expected_ids.len() > 1).then_some(actual_object_id)
        })
        .collect::<Vec<_>>();
    let tombstoned_object_keys = shard
        .dirty_objects
        .iter()
        .filter(|key| {
            let routing_slot = routing_slot_for_key(key);
            (selected_slots.is_empty() || selected_slots.contains(&routing_slot))
                && !record_exists(shard, key)
        })
        .cloned()
        .collect::<Vec<_>>();

    StorageObjectLifecycleReport {
        live_object_ids: expected_object_ids.len() as u64,
        live_page_refs: entries.len() as u64,
        stale_object_ids: 0,
        tombstoned_object_ids: tombstoned_object_keys.len() as u64,
        reused_object_id_conflicts: reused_object_ids.len() as u64,
        missing_owner_page_refs,
        owner_mismatch_page_refs,
        reused_object_ids,
        tombstoned_object_keys,
    }
}

fn slot_dump_entries_by_key(
    shard_id: ShardId,
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
    routing_slot_for_key: impl Fn(&str) -> u32,
) -> BTreeMap<String, PageAddress> {
    collect_live_page_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| routing_slot_for_key(&entry.object_key));
            selected_slots.is_empty() || selected_slots.contains(&routing_slot)
        })
        .map(|entry| {
            let component = entry.component.unwrap_or_default();
            let page_id = entry.address.page_id.unwrap_or_else(|| {
                stable_page_object_id(
                    shard_id,
                    &entry.kind,
                    &entry.object_key,
                    (!component.is_empty()).then_some(component.as_str()),
                )
            });
            (
                format!(
                    "{}:{}:{}:{}",
                    entry.kind, entry.object_key, component, page_id
                ),
                entry.address,
            )
        })
        .collect()
}

fn slot_storage_summaries(
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> Vec<SlotStorageSummary> {
    let mut slots = BTreeMap::<u32, SlotStorageSummary>::new();
    let page_segments_by_slot = BTreeMap::<u32, BTreeSet<u64>>::new();
    for entry in collect_live_page_entries(shard) {
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.page_ref_count = summary.page_ref_count.saturating_add(1);
        summary.physical_bytes = summary.physical_bytes.saturating_add(entry.address.length);
        summary.logical_bytes = summary.logical_bytes.saturating_add(entry.address.length);
        if let Some(zone_id) = entry.address.extent_id {
            summary.last_compacted_zone = Some(
                summary
                    .last_compacted_zone
                    .map_or(zone_id, |current| current.max(zone_id)),
            );
        }
    }
    for (routing_slot, slot) in &shard.slot_index.slot_map {
        if !slot.dirty {
            continue;
        }
        let summary = slots.entry(*routing_slot).or_insert(SlotStorageSummary {
            routing_slot: *routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.dirty_object_count = slot.object_index.len() as u64;
        summary.dirty_generation = slot.dirty_generation;
    }
    for key in &shard.dirty_objects {
        let routing_slot = page_routing_slot(key, start_routing_slot, end_routing_slot);
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.dirty_object_count = summary.dirty_object_count.saturating_add(1);
        summary.dirty_generation = summary.dirty_generation.saturating_add(1);
    }
    for (routing_slot, summary) in &mut slots {
        summary.page_segment_ids = page_segments_by_slot
            .get(routing_slot)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
    }
    slots.into_values().collect()
}

const CPP_PACKED_PAGE_INDEX_SIZE: usize = 17;
const CPP_PACKED_SLOT_NODE_SIZE: usize = 24;

fn storage_model_code(kind: &str) -> u8 {
    match kind {
        "string" => 1,
        "hash" => 2,
        "set" => 3,
        "feature" => 4,
        "sequence" => 5,
        "ips" => 6,
        "risk" => 7,
        "context_node" => 8,
        "context_event" => 9,
        "context_index" => 10,
        "context_audit" => 11,
        "context_entity" => 13,
        "context_child" => 14,
        "context_embedding" => 15,
        "context_summary" => 16,
        "context_compression" => 17,
        _ => 0,
    }
}

fn physical_address_word(address: &PageAddress) -> u64 {
    address.page_segment_id.wrapping_shl(32) | (address.offset & u32::MAX as u64)
}

fn cpp_packed_page_index_bytes(
    page: &StoragePhysicalPageIndex,
) -> [u8; CPP_PACKED_PAGE_INDEX_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_PAGE_INDEX_SIZE];
    bytes[0] = page.object_id.unwrap_or_default() as u8;
    bytes[1] = storage_model_code(&page.model_id);
    bytes[2..4].copy_from_slice(&(page.page_id.unwrap_or_default() as u16).to_le_bytes());
    bytes[4] = u8::from(page.dirty) | (u8::from(page.log_backed) << 1);
    let page_size = if page.deleted { 0 } else { page.length as u32 };
    bytes[5..9].copy_from_slice(&page_size.to_le_bytes());
    let address = physical_address_word(&PageAddress {
        page_segment_id: page.page_segment_id,
        offset: page.offset,
        length: page.length,
        page_id: page.page_id,
        object_id: page.object_id,
        routing_slot: Some(page.routing_slot),
        generation: page.page_id.or(page.object_id),
        extent_id: page.zone_id,
        sha256: page.checksum.clone(),
    });
    bytes[9..17].copy_from_slice(&address.to_le_bytes());
    bytes
}

fn cpp_packed_slot_node_bytes(slot: &StoragePhysicalSlotNode) -> [u8; CPP_PACKED_SLOT_NODE_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_SLOT_NODE_SIZE];
    let page_in_log = slot.page_indexes.iter().any(|page| page.log_backed);
    let trivial_page = slot.page_ref_count <= 1;
    let page_deleted = slot.page_ref_count == 0;
    let mut flags = 0u32;
    flags |= (slot.ttl_ms.is_some() as u32) << 1;
    flags |= (slot.dirty as u32) << 2;
    flags |= (slot.loading as u32) << 4;
    flags |= (slot.in_memory as u32) << 5;
    flags |= (slot.dirty as u32) << 6;
    flags |= (page_deleted as u32) << 7;
    flags |= (page_in_log as u32) << 8;
    flags |= (trivial_page as u32) << 9;
    let flag_bytes = flags.to_le_bytes();
    bytes[0..3].copy_from_slice(&flag_bytes[0..3]);
    bytes[3..7].copy_from_slice(&(slot.physical_bytes as u32).to_le_bytes());
    let model_code = slot
        .page_indexes
        .first()
        .map(|page| storage_model_code(&page.model_id))
        .unwrap_or_default();
    bytes[7] = model_code;
    bytes[8..16].copy_from_slice(&slot.ttl_ms.unwrap_or_default().to_le_bytes());
    let address = slot
        .page_indexes
        .first()
        .map(|page| page.page_segment_id.wrapping_shl(32) | (page.offset & u32::MAX as u64))
        .unwrap_or_default();
    bytes[16..24].copy_from_slice(&address.to_le_bytes());
    bytes
}

fn storage_physical_index_report(
    shard_id: ShardId,
    shard: &ShardState,
    summaries: Vec<SlotStorageSummary>,
) -> StoragePhysicalIndexReport {
    let summary_by_slot = summaries
        .into_iter()
        .map(|summary| (summary.routing_slot, summary))
        .collect::<BTreeMap<_, _>>();
    let mut slots = summary_by_slot
        .iter()
        .map(|(routing_slot, summary)| {
            (
                *routing_slot,
                StoragePhysicalSlotNode {
                    routing_slot: *routing_slot,
                    layout: "empty".to_string(),
                    dirty: summary.dirty_object_count > 0,
                    meta_loaded: true,
                    loading: false,
                    in_memory: summary.page_ref_count > 0,
                    ttl_ms: None,
                    object_count: summary.object_count,
                    page_ref_count: summary.page_ref_count,
                    logical_bytes: summary.logical_bytes,
                    physical_bytes: summary.physical_bytes,
                    dirty_generation: summary.dirty_generation,
                    last_dump_sequence: summary.last_dump_sequence,
                    cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                    cpp_packed_slot_node_hex: String::new(),
                    page_indexes: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut missing_routing_slot_count = 0usize;
    for entry in collect_live_page_entries(shard) {
        if entry.address.routing_slot.is_none() {
            missing_routing_slot_count = missing_routing_slot_count.saturating_add(1);
        }
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let slot = slots
            .entry(routing_slot)
            .or_insert(StoragePhysicalSlotNode {
                routing_slot,
                layout: "empty".to_string(),
                meta_loaded: true,
                in_memory: true,
                cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                ..StoragePhysicalSlotNode::default()
            });
        let mut page_index = StoragePhysicalPageIndex {
            object_key: entry.object_key.clone(),
            model_id: entry.kind.clone(),
            component: entry.component.clone(),
            routing_slot,
            page_segment_id: entry.address.page_segment_id,
            offset: entry.address.offset,
            length: entry.address.length,
            page_id: entry.address.page_id,
            object_id: entry.address.object_id,
            zone_id: entry.address.extent_id,
            checksum: entry.address.sha256.clone(),
            dirty: entry.dirty,
            deleted: entry.deleted,
            log_backed: entry.log_backed,
            cpp_packed_page_index_len: CPP_PACKED_PAGE_INDEX_SIZE,
            cpp_packed_page_index_hex: String::new(),
        };
        page_index.cpp_packed_page_index_hex =
            hex::encode(cpp_packed_page_index_bytes(&page_index));
        slot.page_indexes.push(page_index);
    }
    for (routing_slot, runtime_slot) in &shard.slot_index.slot_map {
        let slot = slots
            .entry(*routing_slot)
            .or_insert(StoragePhysicalSlotNode {
                routing_slot: *routing_slot,
                cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                ..StoragePhysicalSlotNode::default()
            });
        slot.layout = slot_layout_name(runtime_slot.layout).to_string();
        slot.dirty = runtime_slot.dirty;
        slot.meta_loaded = runtime_slot.meta_loaded;
        slot.loading = runtime_slot.loading;
        slot.in_memory = runtime_slot.in_memory;
        slot.ttl_ms = runtime_slot.ttl_ms;
        slot.object_count = runtime_slot.object_index.len() as u64;
        slot.page_ref_count = runtime_slot.page_index.len() as u64;
        slot.dirty_generation = runtime_slot.dirty_generation;
        slot.last_dump_sequence = runtime_slot.last_dump_sequence;
        for page in runtime_slot.page_index.values() {
            let already_present = slot.page_indexes.iter().any(|existing| {
                existing.object_key == page.object_key
                    && existing.model_id == page.model_id
                    && existing.component == page.component
                    && existing.page_segment_id == page.address.page_segment_id
                    && existing.offset == page.address.offset
            });
            if already_present {
                continue;
            }
            let mut page_index = StoragePhysicalPageIndex {
                object_key: page.object_key.clone(),
                model_id: page.model_id.clone(),
                component: page.component.clone(),
                routing_slot: *routing_slot,
                page_segment_id: page.address.page_segment_id,
                offset: page.address.offset,
                length: page.address.length,
                page_id: page.address.page_id,
                object_id: Some(page.object_id),
                zone_id: page.address.extent_id,
                checksum: page.address.sha256.clone(),
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed,
                cpp_packed_page_index_len: CPP_PACKED_PAGE_INDEX_SIZE,
                cpp_packed_page_index_hex: String::new(),
            };
            page_index.cpp_packed_page_index_hex =
                hex::encode(cpp_packed_page_index_bytes(&page_index));
            slot.page_indexes.push(page_index);
        }
    }
    for slot in slots.values_mut() {
        slot.page_indexes.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then(left.model_id.cmp(&right.model_id))
                .then(left.component.cmp(&right.component))
                .then(left.page_segment_id.cmp(&right.page_segment_id))
                .then(left.offset.cmp(&right.offset))
        });
        if !shard.slot_index.slot_map.contains_key(&slot.routing_slot) {
            let object_count = slot
                .page_indexes
                .iter()
                .filter_map(|page| page.object_id)
                .collect::<BTreeSet<_>>()
                .len();
            slot.layout =
                slot_layout_name(classify_slot_layout(object_count, slot.page_indexes.len()))
                    .to_string();
        }
        slot.cpp_packed_slot_node_len = CPP_PACKED_SLOT_NODE_SIZE;
        slot.cpp_packed_slot_node_hex = hex::encode(cpp_packed_slot_node_bytes(slot));
    }
    let page_index_count = slots
        .values()
        .map(|slot| slot.page_indexes.len())
        .sum::<usize>();
    let page_indexes = slots
        .values()
        .flat_map(|slot| slot.page_indexes.iter())
        .collect::<Vec<_>>();
    let missing_object_id_count = page_indexes
        .iter()
        .filter(|page| page.object_id.is_none())
        .count();
    let missing_page_id_count = page_indexes
        .iter()
        .filter(|page| page.page_id.is_none())
        .count();
    let missing_checksum_count = page_indexes
        .iter()
        .filter(|page| page.checksum.is_none())
        .count();
    StoragePhysicalIndexReport {
        shard_id,
        slot_first: true,
        slot_index_authority: !shard.slot_index.slot_map.is_empty(),
        secondary_views_reconciled_from_slot_index: !shard.slot_index.slot_map.is_empty(),
        slot_count: slots.len(),
        page_index_count,
        dirty_slot_count: slots.values().filter(|slot| slot.dirty).count(),
        missing_object_id_count,
        missing_routing_slot_count,
        missing_page_id_count,
        missing_checksum_count,
        cpp_packed_page_index_size: CPP_PACKED_PAGE_INDEX_SIZE,
        cpp_packed_slot_node_size: CPP_PACKED_SLOT_NODE_SIZE,
        cpp_packed_layout_compatible: true,
        slot_nodes: slots.into_values().collect(),
    }
}

fn object_manager_runtime_report(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> ObjectManagerRuntimeReport {
    let ownership =
        slot_object_page_ownership_report(shard_id, shard, start_routing_slot, end_routing_slot);
    let object_runtime = object_manager::runtime_report(shard);
    let mut report = ObjectManagerRuntimeReport {
        shard_id,
        routing_slot_count: shard.slot_index.slot_map.len() as u64,
        object_count: object_runtime.live_object_count as u64,
        page_ref_count: object_runtime.live_page_ref_count as u64,
        hot_object_count: object_runtime.hot_object_count as u64,
        cold_object_count: object_runtime.cold_object_count as u64,
        mixed_residency_object_count: object_runtime.mixed_residency_object_count as u64,
        tombstone_object_count: object_runtime.deleted_object_count as u64,
        dirty_object_count: object_runtime.dirty_object_count as u64,
        loading_object_count: object_runtime.loading_object_count as u64,
        ttl_object_count: object_runtime.ttl_object_count as u64,
        object_page_transition_count: object_runtime.object_page_transition_count as u64,
        dirty_slot_count: shard
            .slot_index
            .slot_map
            .values()
            .filter(|slot| slot.dirty)
            .count() as u64,
        max_dirty_generation: shard
            .slot_index
            .slot_map
            .values()
            .map(|slot| slot.dirty_generation)
            .max()
            .unwrap_or_default(),
        missing_owner_page_ref_count: ownership.missing_owner_page_ref_count,
        owner_mismatch_page_ref_count: ownership.owner_mismatch_page_ref_count,
        evidence: vec![
            "runtime owns page refs in the first-class slot index".to_string(),
            "runtime tracks dirty generations and dirty routing slots in SlotNode".to_string(),
            "runtime validates owner refs before reporting ready".to_string(),
            "runtime tracks hot/cold/tombstone object state and object-page ownership transitions"
                .to_string(),
        ],
        ..ObjectManagerRuntimeReport::default()
    };

    for slot in shard.slot_index.slot_map.values() {
        if let Some(state) = report
            .layout_states
            .iter_mut()
            .find(|state| state.state == slot_layout_name(slot.layout))
        {
            state.object_count = state
                .object_count
                .saturating_add(slot.object_index.len() as u64);
        } else {
            report.layout_states.push(SlotLayoutStateCount {
                state: slot_layout_name(slot.layout).to_string(),
                object_count: slot.object_index.len() as u64,
            });
        }
        if slot.meta_loaded {
            report.meta_object_count = report.meta_object_count.saturating_add(1);
        }
        match slot.layout {
            SlotLayoutState::Empty => {}
            SlotLayoutState::SingleObject | SlotLayoutState::SinglePageObject => {
                report.object_page_count = report.object_page_count.saturating_add(1);
            }
            SlotLayoutState::MultiPageObject => {
                report.multi_page_object_count = report.multi_page_object_count.saturating_add(1);
            }
            SlotLayoutState::MultiObject => {}
        }
    }

    if !ownership.first_class_index_present {
        report
            .blockers
            .push("first-class slot_objects runtime index is empty".to_string());
    }
    if ownership.missing_owner_page_ref_count > 0 {
        report
            .blockers
            .push("page refs are missing object/routing-slot ownership metadata".to_string());
    }
    if ownership.owner_mismatch_page_ref_count > 0 {
        report
            .blockers
            .push("page refs disagree with expected object owners".to_string());
    }
    report.runtime_ready = report.blockers.is_empty();
    report
}

fn slot_object_page_ownership_report(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> SlotObjectPageOwnershipReport {
    let mut report = SlotObjectPageOwnershipReport {
        shard_id,
        first_class_index_present: !shard.slot_index.slot_map.is_empty(),
        derived_from_model_maps: shard.slot_index.slot_map.is_empty(),
        ..SlotObjectPageOwnershipReport::default()
    };
    let entries = collect_live_page_entries(shard);
    report.page_ref_count = entries.len();
    for entry in entries {
        let routing_slot = entry.address.routing_slot.unwrap_or_default();
        if routing_slot < start_routing_slot || routing_slot > end_routing_slot {
            continue;
        }
        let expected_object_id = stable_page_object_id(
            shard_id,
            &entry.kind,
            &entry.object_key,
            entry.component.as_deref(),
        );
        let Some(slot) = shard.slot_index.slot_map.get(&routing_slot) else {
            report.missing_owner_page_ref_count =
                report.missing_owner_page_ref_count.saturating_add(1);
            continue;
        };
        if !slot.object_index.contains(&expected_object_id) {
            report.owner_mismatch_page_ref_count =
                report.owner_mismatch_page_ref_count.saturating_add(1);
        }
    }
    report
}

fn merge_last_dump_sequence(
    mut summaries: Vec<SlotStorageSummary>,
    manifest: &SlotDumpManifest,
) -> Vec<SlotStorageSummary> {
    let dumped_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
    for summary in &mut summaries {
        if dumped_slots.contains(&summary.routing_slot) {
            summary.last_dump_sequence = manifest.index_log_sequence;
        }
    }
    summaries
}

fn slot_dump_manifest_comparable_summaries(
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
) -> Vec<SlotStorageSummary> {
    comparable_slot_dump_summaries(
        slot_storage_summaries(shard, 0, u32::MAX)
            .into_iter()
            .filter(|summary| {
                selected_slots.is_empty() || selected_slots.contains(&summary.routing_slot)
            })
            .collect(),
    )
}

fn comparable_slot_dump_summaries(
    mut summaries: Vec<SlotStorageSummary>,
) -> Vec<SlotStorageSummary> {
    for summary in &mut summaries {
        summary.dirty_object_count = 0;
        summary.dirty_generation = 0;
        summary.last_dump_sequence = 0;
        summary.page_segment_ids.sort_unstable();
        summary.page_segment_ids.dedup();
    }
    summaries.retain(|summary| {
        summary.object_count > 0
            || summary.page_ref_count > 0
            || summary.logical_bytes > 0
            || summary.physical_bytes > 0
    });
    summaries.sort_by_key(|summary| summary.routing_slot);
    summaries
}

fn slot_dump_summary_matches_current_generation(
    manifest_summary: &SlotStorageSummary,
    current_summary: &SlotStorageSummary,
    manifest_slot_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
    current_slot_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
) -> bool {
    let mut manifest_segments = manifest_summary.page_segment_ids.clone();
    manifest_segments.sort_unstable();
    manifest_segments.dedup();
    let mut current_segments = current_summary.page_segment_ids.clone();
    current_segments.sort_unstable();
    current_segments.dedup();
    manifest_summary.routing_slot == current_summary.routing_slot
        && manifest_summary.dirty_generation == current_summary.dirty_generation
        && manifest_summary.object_count == current_summary.object_count
        && manifest_summary.page_ref_count == current_summary.page_ref_count
        && manifest_summary.logical_bytes == current_summary.logical_bytes
        && manifest_summary.physical_bytes == current_summary.physical_bytes
        && manifest_segments == current_segments
        && manifest_slot_fingerprints.get(&manifest_summary.routing_slot)
            == current_slot_fingerprints.get(&current_summary.routing_slot)
}

fn slot_generation_fingerprints_by_slot(shard: &ShardState) -> BTreeMap<u32, BTreeSet<String>> {
    let mut by_slot = BTreeMap::<u32, BTreeSet<String>>::new();
    for entry in collect_live_page_entries(shard) {
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        by_slot.entry(routing_slot).or_default().insert(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            entry.kind,
            entry.object_key,
            entry.component.unwrap_or_default(),
            entry.address.page_segment_id,
            entry.address.offset,
            entry.address.length,
            entry.address.page_id.unwrap_or_default(),
            entry.address.object_id.unwrap_or_default(),
            entry.address.routing_slot.unwrap_or(routing_slot),
            entry.address.generation.unwrap_or_default(),
            entry.address.sha256.unwrap_or_default()
        ));
    }
    by_slot
}

fn collect_live_page_addresses(shard: &ShardState) -> Vec<PageAddress> {
    collect_live_page_entries(shard)
        .into_iter()
        .map(|entry| entry.address)
        .collect()
}

fn unique_timestamped_kv_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    let mut addresses = series
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    addresses.sort_by(|left, right| {
        left.page_segment_id
            .cmp(&right.page_segment_id)
            .then(left.offset.cmp(&right.offset))
            .then(left.length.cmp(&right.length))
    });
    addresses
}

fn unique_feature_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    unique_timestamped_kv_page_addresses(series)
}

fn timestamped_kv_series<'a>(
    shard: &'a ShardState,
) -> Vec<(&'static str, &'a str, &'a BTreeMap<u64, PageAddress>)> {
    let mut series = Vec::new();
    for (key, timeline) in &shard.features {
        series.push(("feature", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.sequences {
        series.push(("sequence", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.ips {
        series.push(("ips", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_events {
        series.push(("context_event", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_indexes {
        series.push(("context_index", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_audits {
        series.push(("context_audit", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_children {
        series.push(("context_child", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_summaries {
        series.push(("context_summary", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_compressions {
        series.push(("context_compression", key.as_str(), timeline));
    }
    series
}

fn storage_feature_page_layout_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> StorageFeaturePageLayoutReport {
    let mut report = StorageFeaturePageLayoutReport::default();
    let mut family_reports = BTreeMap::<String, StorageTimestampedPageFamilyReport>::new();
    let mut inspected_addresses = HashSet::<PageAddress>::new();
    for (kind, key, series) in timestamped_kv_series(shard) {
        report.indexed_timestamped_points = report
            .indexed_timestamped_points
            .saturating_add(series.len());
        if kind == "feature" {
            report.indexed_feature_points =
                report.indexed_feature_points.saturating_add(series.len());
        }
        let family = family_reports.entry(kind.to_string()).or_insert_with(|| {
            StorageTimestampedPageFamilyReport {
                kind: kind.to_string(),
                ..StorageTimestampedPageFamilyReport::default()
            }
        });
        family.indexed_points = family.indexed_points.saturating_add(series.len());
        let mut timestamps_by_address = HashMap::<PageAddress, BTreeSet<u64>>::new();
        for (timestamp_ms, address) in series {
            timestamps_by_address
                .entry(address.clone())
                .or_default()
                .insert(*timestamp_ms);
        }
        report.unique_timestamped_page_refs = report
            .unique_timestamped_page_refs
            .saturating_add(timestamps_by_address.len());
        family.unique_page_refs = family
            .unique_page_refs
            .saturating_add(timestamps_by_address.len());
        if kind == "feature" {
            report.unique_feature_page_refs = report
                .unique_feature_page_refs
                .saturating_add(timestamps_by_address.len());
        }

        for (address, indexed_timestamps) in timestamps_by_address {
            inspected_addresses.insert(address.clone());
            match page_store.read(&address) {
                Ok(bytes) => match decode_feature_page_strict(&bytes) {
                    PackedFeaturePageDecode::Packed(points) => {
                        report.packed_timestamped_pages =
                            report.packed_timestamped_pages.saturating_add(1);
                        family.packed_pages = family.packed_pages.saturating_add(1);
                        if kind == "feature" {
                            report.packed_feature_pages =
                                report.packed_feature_pages.saturating_add(1);
                        }
                        let mut packed_timestamp_counts = BTreeMap::<u64, usize>::new();
                        for point in &points {
                            let count = packed_timestamp_counts
                                .entry(point.timestamp_ms)
                                .or_default();
                            if *count == 1 {
                                report.duplicate_packed_timestamps.push(
                                    feature_page_timestamp_mismatch(
                                        kind,
                                        key,
                                        point.timestamp_ms,
                                        &address,
                                    ),
                                );
                                family.mismatch_count = family.mismatch_count.saturating_add(1);
                            }
                            *count = (*count).saturating_add(1);
                        }
                        let packed_timestamps = points
                            .into_iter()
                            .map(|point| point.timestamp_ms)
                            .collect::<BTreeSet<_>>();
                        for timestamp_ms in
                            indexed_timestamps.difference(&packed_timestamps).copied()
                        {
                            report.missing_indexed_timestamps.push(
                                feature_page_timestamp_mismatch(kind, key, timestamp_ms, &address),
                            );
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                        for timestamp_ms in
                            packed_timestamps.difference(&indexed_timestamps).copied()
                        {
                            report
                                .orphan_packed_timestamps
                                .push(feature_page_timestamp_mismatch(
                                    kind,
                                    key,
                                    timestamp_ms,
                                    &address,
                                ));
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                    }
                    PackedFeaturePageDecode::Corrupt(error) => {
                        report
                            .corrupt_packed_feature_pages
                            .push(feature_page_error(kind, key, &address, error));
                        family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                    }
                    PackedFeaturePageDecode::Legacy => {
                        report.legacy_timestamped_value_pages =
                            report.legacy_timestamped_value_pages.saturating_add(1);
                        family.legacy_value_pages = family.legacy_value_pages.saturating_add(1);
                        if kind == "feature" {
                            report.legacy_feature_value_pages =
                                report.legacy_feature_value_pages.saturating_add(1);
                        }
                        if indexed_timestamps.len() > 1 {
                            report.corrupt_packed_feature_pages.push(feature_page_error(
                                kind,
                                key,
                                &address,
                                "legacy timestamped value page shared by multiple timestamps",
                            ));
                            family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                        }
                    }
                },
                Err(err) => {
                    report.corrupt_packed_feature_pages.push(feature_page_error(
                        kind,
                        key,
                        &address,
                        err.to_string(),
                    ));
                    family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                }
            }
        }
    }
    for entry in collect_slot_index_live_page_entries(shard) {
        if entry.deleted || inspected_addresses.contains(&entry.address) {
            continue;
        }
        if !matches!(
            entry.kind.as_str(),
            "feature"
                | "sequence"
                | "ips"
                | "context_event"
                | "context_index"
                | "context_audit"
                | "context_child"
                | "context_summary"
                | "context_compression"
        ) {
            continue;
        }
        let family = family_reports.entry(entry.kind.clone()).or_insert_with(|| {
            StorageTimestampedPageFamilyReport {
                kind: entry.kind.clone(),
                ..StorageTimestampedPageFamilyReport::default()
            }
        });
        report.unique_timestamped_page_refs = report.unique_timestamped_page_refs.saturating_add(1);
        family.unique_page_refs = family.unique_page_refs.saturating_add(1);
        if entry.kind == "feature" {
            report.unique_feature_page_refs = report.unique_feature_page_refs.saturating_add(1);
        }
        match page_store.read(&entry.address) {
            Ok(bytes) => match decode_feature_page_strict(&bytes) {
                PackedFeaturePageDecode::Packed(points) => {
                    report.packed_timestamped_pages =
                        report.packed_timestamped_pages.saturating_add(1);
                    family.packed_pages = family.packed_pages.saturating_add(1);
                    if entry.kind == "feature" {
                        report.packed_feature_pages = report.packed_feature_pages.saturating_add(1);
                    }
                    for point in points {
                        report
                            .orphan_packed_timestamps
                            .push(feature_page_timestamp_mismatch(
                                &entry.kind,
                                &entry.object_key,
                                point.timestamp_ms,
                                &entry.address,
                            ));
                        family.mismatch_count = family.mismatch_count.saturating_add(1);
                    }
                }
                PackedFeaturePageDecode::Corrupt(error) => {
                    report.corrupt_packed_feature_pages.push(feature_page_error(
                        &entry.kind,
                        &entry.object_key,
                        &entry.address,
                        error,
                    ));
                    family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                }
                PackedFeaturePageDecode::Legacy => {
                    report.legacy_timestamped_value_pages =
                        report.legacy_timestamped_value_pages.saturating_add(1);
                    family.legacy_value_pages = family.legacy_value_pages.saturating_add(1);
                    if entry.kind == "feature" {
                        report.legacy_feature_value_pages =
                            report.legacy_feature_value_pages.saturating_add(1);
                    }
                }
            },
            Err(err) => {
                report.corrupt_packed_feature_pages.push(feature_page_error(
                    &entry.kind,
                    &entry.object_key,
                    &entry.address,
                    err.to_string(),
                ));
                family.corrupt_pages = family.corrupt_pages.saturating_add(1);
            }
        }
    }
    report.families = family_reports.into_values().collect();
    report
}

fn feature_page_error(
    kind: &str,
    key: &str,
    address: &PageAddress,
    error: impl Into<String>,
) -> StorageFeaturePageError {
    StorageFeaturePageError {
        kind: kind.to_string(),
        key: key.to_string(),
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
        error: error.into(),
    }
}

fn feature_page_timestamp_mismatch(
    kind: &str,
    key: &str,
    timestamp_ms: u64,
    address: &PageAddress,
) -> StorageFeaturePageTimestampMismatch {
    StorageFeaturePageTimestampMismatch {
        kind: kind.to_string(),
        key: key.to_string(),
        timestamp_ms,
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
    }
}

fn compaction_utility_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> ShardCompactionUtilityReport {
    let entries = collect_live_page_entries(shard);
    let addresses = entries
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| entry.address.clone())
        .collect::<Vec<_>>();
    let live_page_segment_ids = addresses
        .iter()
        .map(|address| address.page_segment_id)
        .collect::<BTreeSet<_>>();
    let segment_page_counts = page_store
        .segment_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_segment_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
    let total_page_count = live_page_segment_ids
        .iter()
        .map(|page_segment_id| {
            segment_page_counts
                .get(page_segment_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let live_page_refs = addresses.len() as u64;
    let stale_page_estimate = total_page_count.saturating_sub(live_page_refs);
    let live_ref_density_basis_points = if total_page_count == 0 {
        0
    } else {
        live_page_refs.saturating_mul(10_000) / total_page_count
    };
    ShardCompactionUtilityReport {
        live_page_segment_count: live_page_segment_ids.len(),
        total_page_count,
        live_page_refs,
        stale_page_estimate,
        live_ref_density_basis_points,
        model_policies: model_compaction_policy_reports(shard, &entries, &segment_page_counts),
    }
}

fn model_compaction_policy_reports(
    shard: &ShardState,
    entries: &[LivePageEntry],
    segment_page_counts: &BTreeMap<u64, u64>,
) -> Vec<ModelCompactionPolicyReport> {
    #[derive(Default)]
    struct ModelStats {
        live_page_refs: u64,
        deleted_page_refs: u64,
        segment_ids: BTreeSet<u64>,
    }

    let mut by_model = BTreeMap::<String, ModelStats>::new();
    for entry in entries {
        let stats = by_model.entry(entry.kind.clone()).or_default();
        if entry.deleted {
            stats.deleted_page_refs = stats.deleted_page_refs.saturating_add(1);
        } else {
            stats.live_page_refs = stats.live_page_refs.saturating_add(1);
            stats.segment_ids.insert(entry.address.page_segment_id);
        }
    }
    for key in shard
        .dirty_objects
        .iter()
        .filter(|key| !record_exists(shard, key))
    {
        let model_id = if shard.hashes.contains_key(key) {
            "hash"
        } else if shard.sets.contains_key(key) {
            "set"
        } else if shard.features.contains_key(key) {
            "feature"
        } else if shard.sequences.contains_key(key) {
            "sequence"
        } else if shard.ips.contains_key(key) {
            "ips"
        } else if shard.risk_pages.contains_key(key) {
            "risk"
        } else if shard.context_nodes.contains_key(key) {
            "context_node"
        } else if shard.context_entities.contains_key(key) {
            "context_entity"
        } else if shard.context_embeddings.contains_key(key) {
            "context_embedding"
        } else {
            "string"
        };
        let stats = by_model.entry(model_id.to_string()).or_default();
        stats.deleted_page_refs = stats.deleted_page_refs.saturating_add(1);
    }

    by_model
        .into_iter()
        .map(|(model_id, stats)| {
            let total_segment_pages = stats
                .segment_ids
                .iter()
                .map(|segment_id| {
                    segment_page_counts
                        .get(segment_id)
                        .copied()
                        .unwrap_or_default()
                })
                .sum::<u64>();
            let stale_page_estimate = total_segment_pages.saturating_sub(stats.live_page_refs);
            let stale_density_basis_points = if total_segment_pages == 0 {
                0
            } else {
                stale_page_estimate.saturating_mul(10_000) / total_segment_pages
            };
            let total_refs = stats.live_page_refs.saturating_add(stats.deleted_page_refs);
            let tombstone_density_basis_points = if total_refs == 0 {
                0
            } else {
                stats.deleted_page_refs.saturating_mul(10_000) / total_refs
            };
            let layout_policy = compaction_layout_policy_for_model(&model_id);
            let stale_density_triggered = stale_density_basis_points > 0;
            let tombstone_compaction_triggered =
                stats.deleted_page_refs > 0 || tombstone_density_basis_points > 0;
            let object_page_packing_enabled = compaction_object_page_packing_enabled(&model_id);
            let layout_aware_rewrite_required = object_page_packing_enabled
                || matches!(
                    layout_policy,
                    "timestamped_chunked_pages" | "context_timeline_or_sidecar_pages"
                )
                || model_id == "risk";
            ModelCompactionPolicyReport {
                layout_policy: layout_policy.to_string(),
                object_page_packing_enabled,
                model_id,
                live_page_refs: stats.live_page_refs,
                deleted_page_refs: stats.deleted_page_refs,
                total_segment_pages,
                stale_page_estimate,
                stale_density_basis_points,
                tombstone_density_basis_points,
                object_page_pack_group_count: stats.segment_ids.len() as u64,
                cold_page_rewrite_eligible_refs: stats.live_page_refs,
                compaction_action: compaction_action_for_policy(
                    stats.live_page_refs,
                    stats.deleted_page_refs,
                    stale_density_basis_points,
                    tombstone_density_basis_points,
                )
                .to_string(),
                stale_density_triggered,
                tombstone_compaction_triggered,
                layout_aware_rewrite_required,
            }
        })
        .collect()
}

fn compaction_layout_policy_for_model(model_id: &str) -> &'static str {
    match model_id {
        "string" | "risk" | "context_node" | "context_entity" | "context_embedding" => {
            "single_page_object"
        }
        "hash" | "set" => "component_page_object",
        "feature" | "sequence" | "ips" => "timestamped_chunked_pages",
        model if model.starts_with("context_") => "context_timeline_or_sidecar_pages",
        _ => "generic_page_object",
    }
}

fn compaction_object_page_packing_enabled(model_id: &str) -> bool {
    matches!(
        compaction_layout_policy_for_model(model_id),
        "single_page_object" | "component_page_object"
    )
}

fn compaction_action_for_policy(
    live_page_refs: u64,
    deleted_page_refs: u64,
    stale_density_basis_points: u64,
    tombstone_density_basis_points: u64,
) -> &'static str {
    if live_page_refs == 0 && deleted_page_refs > 0 {
        "drop_tombstones"
    } else if tombstone_density_basis_points > 0 || deleted_page_refs > 0 {
        "rewrite_live_drop_tombstones"
    } else if stale_density_basis_points > 0 {
        "rewrite_stale_density"
    } else {
        "rewrite_cold_or_pack"
    }
}

#[derive(Debug, Default)]
struct CompactionRewriteStats {
    rewritten_page_refs: usize,
    cold_page_rewrite_refs: usize,
    by_model: BTreeMap<String, ModelCompactionRewriteStats>,
}

#[derive(Debug, Default)]
struct ModelCompactionRewriteStats {
    rewritten_page_refs: usize,
    cold_page_rewrite_refs: usize,
}

impl CompactionRewriteStats {
    fn record(&mut self, model_id: &str, cold_page: bool) {
        self.rewritten_page_refs = self.rewritten_page_refs.saturating_add(1);
        let model = self.by_model.entry(model_id.to_string()).or_default();
        model.rewritten_page_refs = model.rewritten_page_refs.saturating_add(1);
        if cold_page {
            self.cold_page_rewrite_refs = self.cold_page_rewrite_refs.saturating_add(1);
            model.cold_page_rewrite_refs = model.cold_page_rewrite_refs.saturating_add(1);
        }
    }

    fn into_reports(
        self,
        before: &ShardCompactionUtilityReport,
    ) -> Vec<ModelCompactionRewriteReport> {
        self.by_model
            .into_iter()
            .map(|(model_id, stats)| {
                let before_policy = before
                    .model_policies
                    .iter()
                    .find(|policy| policy.model_id == model_id);
                ModelCompactionRewriteReport {
                    layout_policy: compaction_layout_policy_for_model(&model_id).to_string(),
                    model_id,
                    rewritten_page_refs: stats.rewritten_page_refs,
                    cold_page_rewrite_refs: stats.cold_page_rewrite_refs,
                    object_page_pack_group_count: before_policy
                        .map(|policy| policy.object_page_pack_group_count as usize)
                        .unwrap_or_default(),
                    tombstone_density_basis_points: before_policy
                        .map(|policy| policy.tombstone_density_basis_points)
                        .unwrap_or_default(),
                    stale_density_basis_points: before_policy
                        .map(|policy| policy.stale_density_basis_points)
                        .unwrap_or_default(),
                }
            })
            .collect()
    }
}

fn page_memory_resident(cache: &MultiLayerCache, shard_id: ShardId, address: &PageAddress) -> bool {
    cache
        .get_memory(&CacheKey::page_with_slot_generation(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
            address.generation,
        ))
        .is_some()
}

fn compaction_model_layout_reports(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> Vec<ShardCompactionModelLayoutReport> {
    let segment_page_counts = page_store
        .segment_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_segment_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
    let mut reports = Vec::new();
    reports.push(compaction_layout_from_addresses(
        "string",
        shard.strings.len(),
        shard.strings.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "hash",
        shard.hashes.len(),
        shard
            .hashes
            .values()
            .flat_map(|fields| fields.values().cloned()),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "set",
        shard.sets.len(),
        shard
            .sets
            .values()
            .flat_map(|members| members.values().cloned()),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "feature",
        &shard.features,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "sequence",
        &shard.sequences,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "ips",
        &shard.ips,
        &segment_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_node",
        shard.context_nodes.len(),
        shard.context_nodes.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_event",
        &shard.context_events,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_index",
        &shard.context_indexes,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_audit",
        &shard.context_audits,
        &segment_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_entity",
        shard.context_entities.len(),
        shard.context_entities.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_child",
        &shard.context_children,
        &segment_page_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_embedding",
        shard.context_embeddings.len(),
        shard.context_embeddings.values().cloned(),
        &segment_page_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_summary",
        &shard.context_summaries,
        &segment_page_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_compression",
        &shard.context_compressions,
        &segment_page_counts,
    ));
    reports.retain(|report| report.object_count > 0 || report.index_refs > 0);
    reports
}

fn compaction_timestamped_layout(
    kind: &str,
    timelines: &HashMap<String, BTreeMap<u64, PageAddress>>,
    segment_page_counts: &BTreeMap<u64, u64>,
) -> ShardCompactionModelLayoutReport {
    let mut ref_counts = HashMap::<PageAddress, usize>::new();
    for address in timelines
        .values()
        .flat_map(|series| series.values().cloned())
    {
        *ref_counts.entry(address).or_default() += 1;
    }
    let packed_pages = ref_counts.values().filter(|count| **count > 1).count();
    compaction_layout_from_addresses(
        kind,
        timelines.len(),
        ref_counts.keys().cloned(),
        segment_page_counts,
        Some(packed_pages),
    )
    .with_index_refs(ref_counts.values().sum())
}

fn compaction_layout_from_addresses(
    kind: &str,
    object_count: usize,
    addresses: impl IntoIterator<Item = PageAddress>,
    segment_page_counts: &BTreeMap<u64, u64>,
    packed_pages: Option<usize>,
) -> ShardCompactionModelLayoutReport {
    let addresses = addresses.into_iter().collect::<Vec<_>>();
    let unique_addresses = addresses
        .iter()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let live_segment_ids = unique_addresses
        .iter()
        .map(|address| address.page_segment_id)
        .collect::<BTreeSet<_>>();
    let total_pages_in_live_segments = live_segment_ids
        .iter()
        .map(|segment_id| {
            segment_page_counts
                .get(segment_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let unique_page_refs = unique_addresses.len();
    let packed_timestamped_pages = packed_pages.unwrap_or_default();
    let live_ref_density_basis_points = if total_pages_in_live_segments == 0 {
        0
    } else {
        (unique_page_refs as u64).saturating_mul(10_000) / total_pages_in_live_segments
    };
    ShardCompactionModelLayoutReport {
        kind: kind.to_string(),
        object_count,
        index_refs: addresses.len(),
        unique_page_refs,
        packed_timestamped_pages,
        legacy_value_pages: unique_page_refs.saturating_sub(packed_timestamped_pages),
        stale_page_estimate: total_pages_in_live_segments.saturating_sub(unique_page_refs as u64),
        live_ref_density_basis_points,
    }
}

trait CompactionLayoutIndexRefs {
    fn with_index_refs(self, index_refs: usize) -> Self;
}

impl CompactionLayoutIndexRefs for ShardCompactionModelLayoutReport {
    fn with_index_refs(mut self, index_refs: usize) -> Self {
        self.index_refs = index_refs;
        self
    }
}

fn compact_page_addresses<'a>(
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    addresses: impl IntoIterator<Item = &'a mut PageAddress>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    for address in addresses {
        let cold_page = !page_memory_resident(cache, shard_id, address);
        let bytes = read_page_bytes(cache, page_store, shard_id, address).ok_or_else(|| {
            Status::error(
                "page_compaction_failed",
                "missing page bytes during compaction",
            )
        })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, address.object_id, address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
                new_address.generation,
            ),
            bytes,
        );
        rewrite_stats.record(model_id, cold_page);
    }
    Ok(())
}

fn compact_feature_page_addresses(
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    series: &mut BTreeMap<u64, PageAddress>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    let unique_addresses = unique_feature_page_addresses(series);
    let mut rewritten = HashMap::<PageAddress, PageAddress>::new();
    for old_address in unique_addresses {
        let cold_page = !page_memory_resident(cache, shard_id, &old_address);
        let bytes =
            read_page_bytes(cache, page_store, shard_id, &old_address).ok_or_else(|| {
                Status::error(
                    "page_compaction_failed",
                    "missing feature page bytes during compaction",
                )
            })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, old_address.object_id, old_address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
                new_address.generation,
            ),
            bytes,
        );
        rewritten.insert(old_address, new_address);
        rewrite_stats.record(model_id, cold_page);
    }
    for address in series.values_mut() {
        if let Some(new_address) = rewritten.get(address) {
            *address = new_address.clone();
        }
    }
    Ok(())
}

fn append_value(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    bytes: &[u8],
    object_id: Option<u64>,
    routing_slot: Option<u32>,
    async_storage: bool,
) -> Result<PageAddress, PageStoreError> {
    if !async_storage {
        return page_store.append_with_page_metadata(bytes, object_id, routing_slot);
    }
    let address = PageAddress {
        page_segment_id: HOT_PAGE_SEGMENT_ID,
        offset: HOT_PAGE_OFFSET.fetch_add(1, Ordering::Relaxed),
        length: bytes.len() as u64,
        page_id: None,
        object_id,
        routing_slot,
        generation: object_id,
        extent_id: None,
        sha256: None,
    };
    let bytes = bytes.to_vec();
    cache.put_memory_only(
        CacheKey::page_with_slot_generation(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
            address.generation,
        ),
        bytes,
    );
    Ok(address)
}

fn persist_risk_page(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
    start_routing_slot: u32,
    end_routing_slot: u32,
    async_storage: bool,
) -> bool {
    let Some(series) = shard.risk.get(key) else {
        shard.risk_pages.remove(key);
        return false;
    };
    let Ok(bytes) = serde_json::to_vec(series) else {
        return false;
    };
    let object_id = stable_page_object_id(shard_id, "risk", key, None);
    let routing_slot = page_routing_slot(key, start_routing_slot, end_routing_slot);
    if let Ok(address) = append_value(
        cache,
        page_store,
        shard_id,
        &bytes,
        Some(object_id),
        Some(routing_slot),
        async_storage,
    ) {
        upsert_slot_index_page(shard, shard_id, "risk", key, None, address.clone(), true);
        shard.risk_pages.insert(key.to_string(), address);
        true
    } else {
        false
    }
}

fn invalidate_cache_key(cache: &MultiLayerCache, key: CacheKey, memory_only: bool) {
    if memory_only {
        cache.invalidate_memory_only(&key);
    } else {
        let _ = cache.invalidate(&key);
    }
}

fn record_exists(shard: &ShardState, key: &str) -> bool {
    associated_record_keys(key)
        .iter()
        .any(|record_key| record_exists_exact(shard, record_key))
}

fn record_exists_exact(shard: &ShardState, key: &str) -> bool {
    let slot_index_exists = if shard.slot_index.object_component_lookup.is_empty() {
        shard.slot_index.slot_map.values().any(|slot| {
            slot.page_index
                .values()
                .any(|page| page.object_key == key && !page.deleted)
        })
    } else {
        storage_model_kinds().iter().any(|kind| {
            shard
                .slot_index
                .object_component_lookup
                .get(&object_component_lookup_key(kind, key))
                .map(|page_refs| {
                    page_refs.iter().any(|page_ref| {
                        shard
                            .slot_index
                            .slot_map
                            .get(&page_ref.routing_slot)
                            .and_then(|slot| slot.page_index.get(&page_ref.page_ref_key))
                            .map(|page| {
                                !page.deleted && page.model_id == *kind && page.object_key == key
                            })
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
        })
    };
    slot_index_exists
        || shard.strings.contains_key(key)
        || shard.hashes.contains_key(key)
        || shard.sets.contains_key(key)
        || shard.features.contains_key(key)
        || shard.sequences.contains_key(key)
        || shard.ips.contains_key(key)
        || shard.risk.contains_key(key)
        || shard.risk_pages.contains_key(key)
        || shard.risk_changes.contains_key(key)
        || shard.risk_fol.contains_key(key)
        || shard.context_nodes.contains_key(key)
        || shard.context_events.contains_key(key)
        || shard.context_indexes.contains_key(key)
        || shard.context_audits.contains_key(key)
        || shard.context_entities.contains_key(key)
        || shard.context_children.contains_key(key)
        || shard.context_embeddings.contains_key(key)
        || shard.context_summaries.contains_key(key)
        || shard.context_compressions.contains_key(key)
}

fn storage_model_kinds() -> &'static [&'static str] {
    &[
        "string",
        "hash",
        "set",
        "feature",
        "sequence",
        "ips",
        "risk",
        "context_node",
        "context_event",
        "context_index",
        "context_audit",
        "context_entity",
        "context_child",
        "context_embedding",
        "context_summary",
        "context_compression",
    ]
}

fn invalidate_record_all(cache: &MultiLayerCache, shard_id: ShardId, key: &str) {
    let _ = cache.invalidate(&CacheKey::string(shard_id, key));
    let _ = cache.invalidate_record(shard_id, "hash", key);
    let _ = cache.invalidate_record(shard_id, "set", key);
    let _ = cache.invalidate_record(shard_id, "feature", key);
}

fn read_page_bytes(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    address: &PageAddress,
) -> Option<Vec<u8>> {
    let cache_key = CacheKey::page_with_slot_generation(
        shard_id,
        address.page_segment_id,
        address.offset,
        address.length,
        address.routing_slot,
        address.generation,
    );
    if let Ok(Some(bytes)) = cache.get(&cache_key) {
        return Some(bytes);
    }
    let bytes = page_store.read(address).ok()?;
    let _ = cache.put(cache_key, bytes.clone());
    Some(bytes)
}

fn read_page_bytes_cold(page_store: &LocalPageStore, address: &PageAddress) -> Option<Vec<u8>> {
    page_store.read(address).ok()
}

fn dedupe_nonzero_u64_preserve_order(values: Vec<u64>) -> Vec<u64> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| *value != 0 && seen.insert(*value))
        .collect()
}

fn cache_entry_routing_slot(entry: &CacheEntryInfo) -> Option<u32> {
    entry
        .selector
        .strip_prefix("slot-")?
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn parse_i64(bytes: &Vec<u8>) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn object_manager_stats(
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> ObjectManagerStats {
    if !shard.slot_index.slot_map.is_empty() {
        let (slot_object_count, slot_page_ref_count, slot_dirty_object_count) =
            if !shard.slot_index.object_component_lookup.is_empty() {
                (
                    shard.slot_index.object_component_lookup.len(),
                    shard
                        .slot_index
                        .object_component_lookup
                        .values()
                        .map(BTreeSet::len)
                        .sum::<usize>(),
                    shard.dirty_objects.len(),
                )
            } else {
                let live_pages = shard
                    .slot_index
                    .slot_map
                    .values()
                    .flat_map(|slot| slot.page_index.values())
                    .filter(|page| !page.deleted)
                    .collect::<Vec<_>>();
                let slot_object_count = live_pages
                    .iter()
                    .map(|page| {
                        (
                            page.model_id.as_str(),
                            page.object_key.as_str(),
                            (page.model_id == "hash")
                                .then(|| page.component.as_deref())
                                .flatten(),
                        )
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                let slot_dirty_object_count = live_pages
                    .iter()
                    .filter(|page| page.dirty || shard.dirty_objects.contains(&page.object_key))
                    .map(|page| {
                        (
                            page.model_id.as_str(),
                            page.object_key.as_str(),
                            (page.model_id == "hash")
                                .then(|| page.component.as_deref())
                                .flatten(),
                        )
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                (slot_object_count, live_pages.len(), slot_dirty_object_count)
            };
        let secondary_object_count = shard.strings.len()
            + shard.hashes.len()
            + shard.sets.len()
            + shard.features.len()
            + shard.sequences.len()
            + shard.ips.len()
            + shard.risk.len()
            + shard.risk_changes.len()
            + shard.context_nodes.len()
            + shard.context_events.len()
            + shard.context_indexes.len()
            + shard.context_audits.len()
            + shard.context_entities.len()
            + shard.context_children.len()
            + shard.context_embeddings.len()
            + shard.context_summaries.len()
            + shard.context_compressions.len();
        let object_count = slot_object_count.max(secondary_object_count);
        let dirty_object_count = slot_dirty_object_count.max(shard.dirty_objects.len());
        let secondary_page_ref_count = shard.strings.len()
            + shard.hashes.values().map(HashMap::len).sum::<usize>()
            + shard.sets.values().map(BTreeMap::len).sum::<usize>()
            + shard.features.values().map(BTreeMap::len).sum::<usize>()
            + shard.sequences.values().map(BTreeMap::len).sum::<usize>()
            + shard.ips.values().map(BTreeMap::len).sum::<usize>()
            + shard.context_nodes.len()
            + shard
                .context_events
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard
                .context_indexes
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard
                .context_audits
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard.context_entities.len()
            + shard
                .context_children
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard.context_embeddings.len()
            + shard
                .context_summaries
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
            + shard
                .context_compressions
                .values()
                .map(BTreeMap::len)
                .sum::<usize>();
        let dirty_slot_count = if !shard.slot_index.object_component_lookup.is_empty() {
            let mut dirty_slots = shard
                .slot_index
                .slot_map
                .iter()
                .filter_map(|(slot_id, slot)| slot.dirty.then_some(*slot_id))
                .collect::<BTreeSet<_>>();
            for object_key in &shard.dirty_objects {
                dirty_slots.extend(slot_index_target_slots_for_object_key(shard, object_key));
            }
            dirty_slots.len()
        } else {
            shard
                .slot_index
                .slot_map
                .values()
                .filter(|slot| {
                    slot.dirty
                        || slot.page_index.values().any(|page| {
                            page.dirty || shard.dirty_objects.contains(&page.object_key)
                        })
                })
                .count()
        };
        return ObjectManagerStats {
            object_count,
            page_ref_count: slot_page_ref_count.max(secondary_page_ref_count),
            dirty_object_count,
            dirty_slot_count,
            routing_slot_count: routing_slot_count(start_routing_slot, end_routing_slot),
        };
    }

    let object_count = shard.strings.len()
        + shard.hashes.len()
        + shard.sets.len()
        + shard.features.len()
        + shard.sequences.len()
        + shard.ips.len()
        + shard.risk.len()
        + shard.context_nodes.len()
        + shard.context_events.len()
        + shard.context_indexes.len()
        + shard.context_audits.len()
        + shard.context_entities.len()
        + shard.context_children.len()
        + shard.context_embeddings.len()
        + shard.context_summaries.len()
        + shard.context_compressions.len();
    let page_ref_count = shard.strings.len()
        + shard.hashes.values().map(HashMap::len).sum::<usize>()
        + shard.sets.values().map(BTreeMap::len).sum::<usize>()
        + shard.features.values().map(BTreeMap::len).sum::<usize>()
        + shard.sequences.values().map(BTreeMap::len).sum::<usize>()
        + shard.ips.values().map(BTreeMap::len).sum::<usize>()
        + shard.context_nodes.len()
        + shard
            .context_events
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_indexes
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_audits
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard.context_entities.len()
        + shard
            .context_children
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard.context_embeddings.len()
        + shard
            .context_summaries
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_compressions
            .values()
            .map(BTreeMap::len)
            .sum::<usize>();
    let routing_slot_count = routing_slot_count(start_routing_slot, end_routing_slot);
    let mut dirty_slots = shard
        .slot_index
        .slot_map
        .iter()
        .filter_map(|(slot, node)| node.dirty.then_some(*slot))
        .collect::<BTreeSet<_>>();
    dirty_slots.extend(
        shard
            .dirty_objects
            .iter()
            .map(|key| slot_for_object(key, start_routing_slot, routing_slot_count)),
    );
    ObjectManagerStats {
        object_count,
        page_ref_count,
        dirty_object_count: shard.dirty_objects.len(),
        dirty_slot_count: dirty_slots.len(),
        routing_slot_count,
    }
}

fn routing_slot_count(start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    if end_routing_slot < start_routing_slot {
        return 0;
    }
    end_routing_slot
        .saturating_sub(start_routing_slot)
        .saturating_add(1)
}

fn slot_for_object(key: &str, start_routing_slot: u32, routing_slot_count: u32) -> u32 {
    if routing_slot_count == 0 {
        return start_routing_slot;
    }
    start_routing_slot + (stable_object_hash(key) % routing_slot_count as u64) as u32
}

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

fn stable_object_hash(key: &str) -> u64 {
    stable_object_hash_bytes(key.as_bytes())
}

fn stable_object_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = FNV1A64_OFFSET_BASIS;
    stable_object_hash_update(&mut hash, bytes);
    hash
}

fn stable_object_hash_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= *byte as u64;
        *hash = hash.wrapping_mul(FNV1A64_PRIME);
    }
}

fn stable_object_hash_update_u64_decimal(hash: &mut u64, mut value: u64) {
    let mut buf = [0_u8; 20];
    let mut pos = buf.len();
    if value == 0 {
        pos -= 1;
        buf[pos] = b'0';
    } else {
        while value > 0 {
            pos -= 1;
            buf[pos] = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }
    stable_object_hash_update(hash, &buf[pos..]);
}

fn stable_page_object_id(shard_id: ShardId, kind: &str, key: &str, component: Option<&str>) -> u64 {
    let mut hash = FNV1A64_OFFSET_BASIS;
    stable_object_hash_update_u64_decimal(&mut hash, shard_id as u64);
    stable_object_hash_update(&mut hash, b":");
    stable_object_hash_update(&mut hash, kind.as_bytes());
    stable_object_hash_update(&mut hash, b":");
    stable_object_hash_update(&mut hash, key.as_bytes());
    if let Some(component) = component {
        stable_object_hash_update(&mut hash, b":");
        stable_object_hash_update(&mut hash, component.as_bytes());
    }
    hash
}

fn page_routing_slot(key: &str, start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    slot_for_object(
        key,
        start_routing_slot,
        routing_slot_count(start_routing_slot, end_routing_slot),
    )
}

#[cfg(test)]
mod tests;
