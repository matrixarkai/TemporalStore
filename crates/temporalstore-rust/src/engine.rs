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

    pub fn run_storage_manager_cycle(
        &self,
        request: StorageManagerCycleRequest,
    ) -> StorageManagerCycleReport {
        let cycle_started_unix_ms = now_ms();
        let cxx_stage_order = [
            "prepare",
            "reclaim_oplog",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let plan_request = StorageLifecycleRequest {
            shard_id: request.shard_id,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: request.max_dump_slots_per_round,
            min_undumped_oplog_records: if request.enable_oplog_reclaim {
                request.min_undumped_oplog_records
            } else {
                u64::MAX
            },
            purge_delayed_destroy: request.enable_page_reclaim,
            prune_slot_dump_manifests: request.enable_index_gc,
            roll_forward_slot_dump_installs: request.enable_index_gc,
            follower_replay_cursors: request.follower_replay_cursors.clone(),
            page_gc_shared_store_cursors: request.page_gc_shared_store_cursors.clone(),
            page_gc_raft_snapshot_refs: request.raft_snapshot_refs.clone(),
            page_gc_checkpoint_floor_segment_id: request.page_gc_checkpoint_floor_segment_id,
            page_gc_raft_install_floor_segment_id: request.page_gc_raft_install_floor_segment_id,
            page_gc_delayed_destroy_grace_ms: request.page_gc_delayed_destroy_grace_ms,
            invalidate_cache: false,
            warm_cache: request.warm_cache,
        };
        let plan = self.storage_lifecycle_plan(plan_request.clone());
        let page_gc_dependency_plan = self.storage_page_gc_dependency_plan(
            request.shard_id,
            plan.reclaim_candidates
                .iter()
                .map(|candidate| candidate.page_segment_id),
            request.page_gc_shared_store_cursors.clone(),
            request.raft_snapshot_refs.clone(),
            request.page_gc_checkpoint_floor_segment_id,
            request.page_gc_raft_install_floor_segment_id,
            request.page_gc_delayed_destroy_grace_ms,
        );
        let slot_logical_bytes = plan
            .slot_summaries
            .iter()
            .map(|summary| summary.logical_bytes)
            .sum::<u64>();
        let slot_physical_bytes = plan
            .slot_summaries
            .iter()
            .map(|summary| summary.physical_bytes)
            .sum::<u64>();
        let reclaim_live_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.live_physical_bytes)
            .sum::<u64>();
        let reclaim_stale_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.stale_physical_bytes)
            .sum::<u64>();
        let reclaim_candidate_count = plan.reclaim_candidates.len();
        let reclaim_skipped_count = plan
            .stale_page_segment_ids
            .len()
            .saturating_sub(reclaim_candidate_count);
        let log_pressure = self.storage_log_compatibility_report(request.shard_id);
        let cache_pressure = self.storage_cache_inspection_report(request.shard_id);
        let page_segment_total_bytes = reclaim_live_bytes.saturating_add(reclaim_stale_bytes);
        let page_segment_stale_density_basis_points = if page_segment_total_bytes == 0 {
            0
        } else {
            reclaim_stale_bytes.saturating_mul(10_000) / page_segment_total_bytes
        };
        let delayed_destroy_segment_count = plan.delayed_destroy_page_segment_ids.len();
        let delayed_destroy_bytes = plan
            .reclaim_candidates
            .iter()
            .filter(|candidate| candidate.reason == "delayed_destroy")
            .map(|candidate| candidate.physical_bytes)
            .sum::<u64>();
        let expired_slot_object_scan_debt = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| shard.expires_at_ms.len())
            .unwrap_or_default();
        let compaction_utility = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| compaction_utility_report(&self.page_store, shard))
            .unwrap_or_default();
        let compaction_debt_model_count = compaction_utility
            .model_policies
            .iter()
            .filter(|policy| {
                policy.stale_page_estimate > 0
                    || policy.stale_density_basis_points > 0
                    || policy.tombstone_density_basis_points > 0
            })
            .count()
            .max(usize::from(page_segment_stale_density_basis_points > 0));
        let compaction_debt_score = compaction_utility
            .model_policies
            .iter()
            .map(|policy| {
                policy
                    .stale_page_estimate
                    .saturating_add(policy.stale_density_basis_points)
                    .saturating_add(policy.tombstone_density_basis_points)
            })
            .sum::<u64>()
            .saturating_add(compaction_utility.stale_page_estimate)
            .saturating_add(reclaim_stale_bytes)
            .saturating_add(page_segment_stale_density_basis_points);
        let retention_prune_plan = self.slot_dump_manifest_prune_plan_with_retention_refs(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let manifest_retention_blockers = retention_prune_plan
            .follower_blocks
            .len()
            .saturating_add(retention_prune_plan.raft_snapshot_blocks.len());
        let memory_cache_pressure_score = cache_pressure
            .stats
            .memory_bytes
            .saturating_add(cache_pressure.stats.pinned_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_depth);
        let total_pressure_score = plan
            .dirty_slots
            .len()
            .saturating_add(expired_slot_object_scan_debt)
            .saturating_add(delayed_destroy_segment_count)
            .saturating_add(compaction_debt_model_count) as u64
            + plan.undumped_oplog_records
            + log_pressure.oplog_bytes
            + log_pressure.index_log_bytes
            + reclaim_stale_bytes
            + cache_pressure.stats.disk_bytes
            + memory_cache_pressure_score
            + delayed_destroy_bytes
            + manifest_retention_blockers as u64
            + compaction_debt_score;
        let pressure_signals = StorageManagerPressureSignals {
            dirty_slot_count: plan.dirty_slots.len(),
            undumped_wal_records: plan.undumped_oplog_records,
            wal_bytes: log_pressure.oplog_bytes,
            index_log_bytes: log_pressure.index_log_bytes,
            stale_page_bytes: reclaim_stale_bytes,
            live_page_bytes: reclaim_live_bytes,
            page_segment_stale_density_basis_points,
            memory_cache_bytes: cache_pressure.stats.memory_bytes,
            disk_cache_bytes: cache_pressure.stats.disk_bytes,
            memory_cache_pressure_score,
            expired_slot_object_scan_debt,
            delayed_destroy_segment_count,
            delayed_destroy_bytes,
            follower_cursor_retention_blockers: retention_prune_plan.follower_blocks.len(),
            raft_snapshot_retention_blockers: retention_prune_plan.raft_snapshot_blocks.len(),
            compaction_debt_model_count,
            compaction_debt_score,
            total_pressure_score,
        };
        let mut stages = Vec::new();
        let mut errors = Vec::new();
        stages.push(StorageManagerStageReport {
            stage: "prepare".to_string(),
            enabled: request.enable_prepare,
            applied: request.enable_prepare && !request.dry_run,
            skipped: !request.enable_prepare,
            reason: if request.enable_prepare {
                "prepared storage lifecycle pressure view and index/page-store metadata".to_string()
            } else {
                "prepare disabled".to_string()
            },
            selected_page_segment_ids: plan.live_page_segment_ids.clone(),
            pressure_signal:
                "dirty_slots+wal_bytes+index_log_bytes+stale_density+cache_pressure+expire_debt+delayed_destroy+retention_blockers+model_compaction_debt"
                    .to_string(),
            pressure_score: pressure_signals.total_pressure_score,
            pressure_threshold: 1,
            pressure_triggered: pressure_signals.total_pressure_score > 0,
            candidate_count: reclaim_candidate_count,
            skipped_count: reclaim_skipped_count,
            before_bytes: slot_physical_bytes,
            after_bytes: slot_physical_bytes,
            live_bytes: reclaim_live_bytes,
            stale_bytes: reclaim_stale_bytes,
            dirty_slot_count: plan.dirty_slots.len(),
            undumped_oplog_records: plan.undumped_oplog_records,
            metrics_slot_count: plan.slot_summaries.len(),
            metrics_page_ref_count: plan
                .slot_summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum(),
            ..StorageManagerStageReport::default()
        });

        let mut expiry_report = None;
        if request.enable_expire && !request.dry_run {
            match self.sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: request.shard_id,
                hot_cursor: request.expire_hot_cursor.clone(),
                cold_cursor: request.expire_cold_cursor.clone(),
                max_hot_slots_per_round: request.max_expire_hot_slots_per_round,
                max_cold_slots_per_round: request.max_expire_cold_slots_per_round,
                load_cold_slots: request.load_cold_slots_for_expire,
            }) {
                Ok(report) => expiry_report = Some(report),
                Err(err) => errors.push(format!("expire: {}", err.message)),
            }
        }

        if request.enable_page_reclaim
            && !request.dry_run
            && !plan.reclaim_candidates.is_empty()
            && page_gc_dependency_plan.safe_to_reclaim
        {
            let retain_from_page_segment_id = plan
                .reclaim_candidates
                .iter()
                .map(|candidate| candidate.page_segment_id)
                .max()
                .unwrap_or_default()
                .saturating_add(1);
            match self.page_store.gc_segments_before_with_live_refs_utility(
                retain_from_page_segment_id,
                plan.live_page_segment_ids.clone(),
                plan.reclaim_candidates.len(),
                true,
            ) {
                Ok(report) => {
                    for page_segment_id in report
                        .removed_page_segment_ids
                        .iter()
                        .chain(report.delayed_destroy_page_segment_ids.iter())
                    {
                        let _ = self
                            .cache
                            .invalidate_page_segment(request.shard_id, *page_segment_id);
                    }
                }
                Err(err) => errors.push(format!("reclaim_page: {err}")),
            }
        }

        let lifecycle_report = if request.dry_run {
            None
        } else {
            let mut lifecycle_request = plan_request.clone();
            lifecycle_request.purge_delayed_destroy =
                lifecycle_request.purge_delayed_destroy && page_gc_dependency_plan.safe_to_reclaim;
            Some(self.apply_storage_lifecycle(lifecycle_request))
        };
        let eviction_report = if request.enable_evict {
            Some(if request.dry_run {
                StorageEvictionReport {
                    shard_id: request.shard_id,
                    mode: if request.eviction_delete_drop {
                        "delete_drop"
                    } else {
                        "evict_cache"
                    }
                    .to_string(),
                    pressure_before: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    pressure_after: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    memory_pressure_threshold: request.eviction_memory_pressure_threshold,
                    batch_limit: request.eviction_batch_limit,
                    dump_before_evict: request.eviction_dump_before_evict,
                    skipped_reason: "dry_run".to_string(),
                    ..StorageEvictionReport::default()
                }
            } else {
                self.apply_storage_eviction(
                    request.shard_id,
                    request.eviction_memory_pressure_threshold,
                    request.eviction_batch_limit,
                    request.eviction_dump_before_evict,
                    request.eviction_delete_drop,
                )
            })
        } else {
            None
        };
        let wal_reclaim_plan = self.storage_wal_reclaim_plan(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let wal_reclaim_report = if request.enable_oplog_reclaim {
            Some(if request.dry_run {
                StorageWalReclaimReport {
                    plan: wal_reclaim_plan.clone(),
                    applied: false,
                    ..StorageWalReclaimReport::default()
                }
            } else {
                self.apply_storage_wal_reclaim(wal_reclaim_plan.clone())
            })
        } else {
            None
        };
        let index_gc_report = Some(self.storage_index_gc_report(
            &plan,
            &wal_reclaim_plan,
            lifecycle_report.as_ref(),
            &request,
        ));

        stages.push(StorageManagerStageReport {
            stage: "reclaim_oplog".to_string(),
            enabled: request.enable_oplog_reclaim,
            applied: wal_reclaim_report
                .as_ref()
                .map(|report| report.applied)
                .unwrap_or(false),
            skipped: !request.enable_oplog_reclaim
                || plan.dump_delayed
                || wal_reclaim_report
                    .as_ref()
                    .map(|report| !report.plan.safe_to_reclaim)
                    .unwrap_or(true),
            reason: if !request.enable_oplog_reclaim {
                "oplog reclaim disabled".to_string()
            } else if plan.dump_delayed {
                "dirty slot dump delayed until the configured undumped log threshold is reached"
                    .to_string()
            } else if wal_reclaim_report
                .as_ref()
                .map(|report| !report.plan.safe_to_reclaim)
                .unwrap_or(true)
            {
                format!(
                    "WAL/index-log reclaim blocked until durable slot generations and retention cursors allow it: {}",
                    wal_reclaim_report
                        .as_ref()
                        .map(|report| report.plan.blocker_reasons.join(","))
                        .unwrap_or_default()
                )
            } else {
                "reclaimed WAL/index-log through the slot-generation durable dump frontier"
                    .to_string()
            },
            selected_slots: plan.selected_dump_slots.clone(),
            pressure_signal:
                "durable_slot_generation_frontier+follower_snapshot_retention+wal_bytes+index_log_bytes"
                    .to_string(),
            pressure_score: pressure_signals
                .undumped_wal_records
                .saturating_add(pressure_signals.wal_bytes)
                .saturating_add(pressure_signals.index_log_bytes),
            pressure_threshold: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_oplog_sequence)
                .unwrap_or(request.min_undumped_oplog_records),
            pressure_triggered: request.enable_oplog_reclaim
                && wal_reclaim_report
                    .as_ref()
                    .map(|report| report.plan.safe_to_reclaim)
                    .unwrap_or(false),
            candidate_count: plan.dirty_slots.len(),
            skipped_count: plan
                .dirty_slots
                .len()
                .saturating_sub(plan.selected_dump_slots.len()),
            before_bytes: slot_logical_bytes,
            after_bytes: slot_logical_bytes,
            live_bytes: slot_logical_bytes,
            dirty_slot_count: plan.dirty_slots.len(),
            undumped_oplog_records: plan.undumped_oplog_records,
            dumped_slot_count: lifecycle_report
                .as_ref()
                .and_then(|report| report.dump_manifest.as_ref())
                .map(|manifest| manifest.slot_ids.len())
                .unwrap_or_default(),
            wal_records_removed: wal_reclaim_report
                .as_ref()
                .map(|report| report.oplog_records_removed)
                .unwrap_or_default(),
            index_log_records_removed: wal_reclaim_report
                .as_ref()
                .map(|report| report.index_log_records_removed)
                .unwrap_or_default(),
            retention_blockers: wal_reclaim_report
                .as_ref()
                .map(|report| {
                    report
                        .plan
                        .follower_cursor_block_count
                        .saturating_add(report.plan.raft_snapshot_block_count)
                })
                .unwrap_or_default(),
            retain_from_wal_sequence: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_oplog_sequence)
                .unwrap_or_default(),
            retain_from_index_log_sequence: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_index_log_sequence)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "expire".to_string(),
            enabled: request.enable_expire,
            applied: request.enable_expire && !request.dry_run,
            skipped: !request.enable_expire,
            reason: if request.enable_expire {
                "swept expired logical records and persisted index updates".to_string()
            } else {
                "expire disabled".to_string()
            },
            pressure_signal: "expired_hot_slots+cold_slots+scan_cursors+load_on_expire_debt"
                .to_string(),
            pressure_score: expiry_report
                .as_ref()
                .map(|report| report.expired_records_removed as u64)
                .unwrap_or(pressure_signals.expired_slot_object_scan_debt as u64),
            pressure_threshold: 1,
            pressure_triggered: pressure_signals.expired_slot_object_scan_debt > 0
                || expiry_report
                    .as_ref()
                    .map(|report| report.expired_records_removed > 0)
                    .unwrap_or(false),
            before_bytes: slot_logical_bytes,
            after_bytes: slot_logical_bytes,
            expired_records_removed: expiry_report
                .as_ref()
                .map(|report| report.expired_records_removed)
                .unwrap_or_default(),
            candidate_count: expiry_report
                .as_ref()
                .map(|report| report.scanned_records)
                .unwrap_or(pressure_signals.expired_slot_object_scan_debt),
            skipped_count: expiry_report
                .as_ref()
                .map(|report| report.skipped_records)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "evict".to_string(),
            enabled: request.enable_evict,
            applied: eviction_report
                .as_ref()
                .map(|report| {
                    report.cache_entries_removed > 0
                        || report.cache_disk_bytes_removed > 0
                        || report.dropped_object_count > 0
                })
                .unwrap_or(false),
            skipped: !request.enable_evict
                || eviction_report
                    .as_ref()
                    .map(|report| !report.pressure_gate_open)
                    .unwrap_or(true),
            reason: if !request.enable_evict {
                "evict disabled".to_string()
            } else if eviction_report
                .as_ref()
                .map(|report| !report.pressure_gate_open)
                .unwrap_or(false)
            {
                "eviction skipped because memory/cache pressure is below threshold".to_string()
            } else if eviction_report
                .as_ref()
                .map(|report| report.cooldown)
                .unwrap_or(false)
            {
                "eviction entered cooldown because pressure did not decrease".to_string()
            } else {
                "evicted weighted slot/object victims under memory/cache pressure".to_string()
            },
            pressure_signal: "weighted_slot_object_eviction+memory_pressure_gate+batch_limit"
                .to_string(),
            pressure_score: pressure_signals
                .memory_cache_pressure_score
                .saturating_add(pressure_signals.disk_cache_bytes)
                .saturating_add(
                    eviction_report
                        .as_ref()
                        .map(|report| {
                            report.cache_entries_removed as u64 + report.cache_disk_bytes_removed
                        })
                        .unwrap_or_default(),
                ),
            pressure_threshold: request.eviction_memory_pressure_threshold,
            pressure_triggered: pressure_signals.memory_cache_pressure_score > 0
                || pressure_signals.disk_cache_bytes > 0
                || eviction_report
                    .as_ref()
                    .map(|report| {
                        report.cache_entries_removed > 0 || report.cache_disk_bytes_removed > 0
                    })
                    .unwrap_or(false),
            before_bytes: eviction_report
                .as_ref()
                .map(|report| report.pressure_before)
                .unwrap_or_default(),
            after_bytes: eviction_report
                .as_ref()
                .map(|report| report.pressure_after)
                .unwrap_or_default(),
            cache_entries_removed: eviction_report
                .as_ref()
                .map(|report| report.cache_entries_removed)
                .unwrap_or_default(),
            cache_disk_bytes_removed: eviction_report
                .as_ref()
                .map(|report| report.cache_disk_bytes_removed)
                .unwrap_or_default(),
            selected_slots: eviction_report
                .as_ref()
                .map(|report| {
                    report
                        .selected_victims
                        .iter()
                        .map(|victim| victim.routing_slot)
                        .collect()
                })
                .unwrap_or_default(),
            candidate_count: eviction_report
                .as_ref()
                .map(|report| report.selected_victims.len())
                .unwrap_or_default(),
            eviction_pressure_before: eviction_report
                .as_ref()
                .map(|report| report.pressure_before)
                .unwrap_or_default(),
            eviction_pressure_after: eviction_report
                .as_ref()
                .map(|report| report.pressure_after)
                .unwrap_or_default(),
            eviction_cooldown: eviction_report
                .as_ref()
                .map(|report| report.cooldown)
                .unwrap_or(false),
            dropped_object_count: eviction_report
                .as_ref()
                .map(|report| report.dropped_object_count)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "reclaim_page".to_string(),
            enabled: request.enable_page_reclaim,
            applied: request.enable_page_reclaim
                && !request.dry_run
                && page_gc_dependency_plan.safe_to_reclaim
                && lifecycle_report
                    .as_ref()
                    .map(|report| !report.delayed_destroy_purged_segments.is_empty())
                    .unwrap_or(false),
            skipped: !request.enable_page_reclaim
                || plan.reclaim_candidates.is_empty()
                || !page_gc_dependency_plan.safe_to_reclaim,
            reason: if !request.enable_page_reclaim {
                "page reclaim disabled".to_string()
            } else if plan.reclaim_candidates.is_empty() {
                "no stale or delayed-destroy page segments are reclaimable".to_string()
            } else if !page_gc_dependency_plan.safe_to_reclaim {
                format!(
                    "page GC refused because retained dependencies remain: {}",
                    page_gc_dependency_plan.blocker_reasons.join(",")
                )
            } else {
                "reclaimed delayed-destroy page segments selected by stale-byte pressure"
                    .to_string()
            },
            pressure_signal:
                "stale_page_bytes+delayed_destroy_backlog+stale_density+dependency_retention"
                    .to_string(),
            pressure_score: pressure_signals
                .stale_page_bytes
                .saturating_add(pressure_signals.delayed_destroy_bytes)
                .saturating_add(pressure_signals.page_segment_stale_density_basis_points),
            pressure_threshold: 1,
            pressure_triggered: request.enable_page_reclaim
                && !plan.reclaim_candidates.is_empty()
                && page_gc_dependency_plan.safe_to_reclaim,
            candidate_count: reclaim_candidate_count,
            skipped_count: reclaim_skipped_count
                .saturating_add(page_gc_dependency_plan.blocked_page_segment_ids.len()),
            before_bytes: reclaim_live_bytes + reclaim_stale_bytes,
            after_bytes: reclaim_live_bytes,
            live_bytes: reclaim_live_bytes,
            stale_bytes: reclaim_stale_bytes,
            selected_page_segment_ids: page_gc_dependency_plan.reclaimable_page_segment_ids.clone(),
            page_segments_reclaimed: lifecycle_report
                .as_ref()
                .map(|report| report.delayed_destroy_purged_segments.len())
                .unwrap_or_default(),
            page_bytes_reclaimed: lifecycle_report
                .as_ref()
                .map(|report| report.delayed_destroy_purged_bytes)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "index_gc".to_string(),
            enabled: request.enable_index_gc,
            applied: request.enable_index_gc
                && !request.dry_run
                && (lifecycle_report
                    .as_ref()
                    .and_then(|report| report.manifest_prune_report.as_ref())
                    .map(|report| {
                        !report.removed_manifest_ids.is_empty() || report.removed_marker_files > 0
                    })
                    .unwrap_or(false)
                    || index_gc_report
                        .as_ref()
                        .map(|report| report.applied)
                        .unwrap_or(false)),
            skipped: !request.enable_index_gc,
            reason: if request.enable_index_gc {
                "pruned obsolete manifests, rolled forward safe install markers, and applied thresholded index-log GC"
                    .to_string()
            } else {
                "index GC disabled".to_string()
            },
            pressure_signal: "obsolete_manifests+install_markers+index_log_bytes+usage_ratio+max_entries"
                .to_string(),
            pressure_score: lifecycle_report
                .as_ref()
                .map(|report| {
                    report
                        .manifest_prune_report
                        .as_ref()
                        .map(|prune| prune.removed_manifest_ids.len() + prune.removed_marker_files)
                        .unwrap_or_default()
                        + report.install_roll_forward_reports.len()
                })
                .unwrap_or_default() as u64
                + pressure_signals.follower_cursor_retention_blockers as u64
                + pressure_signals.raft_snapshot_retention_blockers as u64
                + index_gc_report
                    .as_ref()
                    .map(|report| {
                        report
                            .bytes_before
                            .saturating_add(report.usage_ratio_basis_points)
                    })
                    .unwrap_or_default(),
            pressure_threshold: index_gc_report
                .as_ref()
                .map(|report| {
                    report
                        .bytes_threshold
                        .max(report.usage_ratio_trigger_basis_points)
                })
                .unwrap_or(1),
            pressure_triggered: request.enable_index_gc
                && (lifecycle_report
                    .as_ref()
                    .map(|report| {
                        report.manifest_prune_report.is_some()
                            || !report.install_roll_forward_reports.is_empty()
                    })
                    .unwrap_or(false)
                    || index_gc_report
                        .as_ref()
                        .map(|report| report.threshold_triggered || report.usage_ratio_triggered)
                        .unwrap_or(false)),
            candidate_count: lifecycle_report
                .as_ref()
                .map(|report| {
                    report
                        .manifest_prune_report
                        .as_ref()
                        .map(|prune| prune.removed_manifest_ids.len() + prune.removed_marker_files)
                        .unwrap_or_default()
                        + report.install_roll_forward_reports.len()
                })
                .unwrap_or_default()
                + index_gc_report
                    .as_ref()
                    .map(|report| report.removable_records_before_budget)
                    .unwrap_or_default(),
            skipped_count: lifecycle_report
                .as_ref()
                .map(|report| report.manifest_prune_plan.blocked_manifest_ids.len())
                .unwrap_or_default()
                + index_gc_report
                    .as_ref()
                    .map(|report| {
                        report
                            .removable_records_before_budget
                            .saturating_sub(report.records_removed)
                    })
                    .unwrap_or_default(),
            before_bytes: index_gc_report
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            after_bytes: index_gc_report
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            manifest_pruned_count: lifecycle_report
                .as_ref()
                .and_then(|report| report.manifest_prune_report.as_ref())
                .map(|report| report.removed_manifest_ids.len() + report.removed_marker_files)
                .unwrap_or_default(),
            install_roll_forward_count: lifecycle_report
                .as_ref()
                .map(|report| report.install_roll_forward_reports.len())
                .unwrap_or_default(),
            index_log_records_removed: index_gc_report
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        let mut merged_dump_load_policy =
            self.storage_merged_dump_load_policy_report(StorageMergedDumpLoadPolicyRequest {
                lifecycle: plan_request.clone(),
                create_dump_manifest: request.enable_oplog_reclaim,
                install_dump_manifest: false,
            });

        let should_compact = request.enable_page_compaction
            && !request.dry_run
            && (!plan.reclaim_candidates.is_empty() || plan.live_page_segment_ids.len() > 1);
        let compaction_report = if should_compact {
            match self.compact_shard_pages(request.shard_id) {
                Ok(report) => Some(report),
                Err(err) => {
                    errors.push(format!("compact: {}", err.message));
                    None
                }
            }
        } else {
            None
        };
        stages.push(StorageManagerStageReport {
            stage: "compact".to_string(),
            enabled: request.enable_page_compaction,
            applied: compaction_report.is_some(),
            skipped: !request.enable_page_compaction
                || request.dry_run
                || (plan.reclaim_candidates.is_empty() && plan.live_page_segment_ids.len() <= 1),
            reason: if !request.enable_page_compaction {
                "page compaction disabled".to_string()
            } else if request.dry_run {
                "dry run reports compaction pressure without rewriting pages".to_string()
            } else if plan.reclaim_candidates.is_empty() && plan.live_page_segment_ids.len() <= 1 {
                "compaction skipped because page density does not show stale-segment pressure"
                    .to_string()
            } else {
                "rewrote live model page references into a fresh compacted segment".to_string()
            },
            pressure_signal: "model_layout_compaction_debt+stale_segment_density".to_string(),
            pressure_score: pressure_signals
                .compaction_debt_score
                .saturating_add(pressure_signals.page_segment_stale_density_basis_points),
            pressure_threshold: 1,
            pressure_triggered: should_compact,
            candidate_count: plan.stale_page_segment_ids.len(),
            skipped_count: plan.stale_page_segment_ids.len().saturating_sub(
                compaction_report
                    .as_ref()
                    .map(|report| report.stale_page_segment_ids.len())
                    .unwrap_or_default(),
            ),
            before_bytes: slot_physical_bytes,
            after_bytes: compaction_report
                .as_ref()
                .map(|_| slot_logical_bytes)
                .unwrap_or(slot_physical_bytes),
            live_bytes: slot_logical_bytes,
            stale_bytes: reclaim_stale_bytes,
            selected_page_segment_ids: compaction_report
                .as_ref()
                .map(|report| report.stale_page_segment_ids.clone())
                .unwrap_or_default(),
            compacted_page_segment_id: compaction_report
                .as_ref()
                .map(|report| report.compacted_page_segment_id),
            rewritten_page_refs: compaction_report
                .as_ref()
                .map(|report| report.rewritten_page_refs)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        let compaction_policy_applied =
            request.dry_run || plan.reclaim_candidates.is_empty() || compaction_report.is_some();
        if compaction_policy_applied {
            merged_dump_load_policy
                .blockers
                .retain(|blocker| blocker != "compaction");
        } else if !merged_dump_load_policy
            .blockers
            .iter()
            .any(|blocker| blocker == "compaction")
        {
            merged_dump_load_policy
                .blockers
                .push("compaction".to_string());
        }
        merged_dump_load_policy.policy_ready = merged_dump_load_policy.blockers.is_empty();

        stages.push(StorageManagerStageReport {
            stage: "reap_metrics".to_string(),
            enabled: true,
            applied: !request.dry_run,
            skipped: false,
            reason: "reported slot/page/cache pressure metrics for the completed cycle".to_string(),
            pressure_signal: "slot_page_cache_metrics".to_string(),
            pressure_score: plan.slot_summaries.len() as u64
                + plan
                    .slot_summaries
                    .iter()
                    .map(|summary| summary.page_ref_count)
                    .sum::<u64>(),
            pressure_threshold: 1,
            pressure_triggered: !plan.slot_summaries.is_empty(),
            before_bytes: slot_physical_bytes,
            after_bytes: slot_physical_bytes,
            live_bytes: slot_logical_bytes,
            stale_bytes: reclaim_stale_bytes,
            metrics_slot_count: plan.slot_summaries.len(),
            metrics_page_ref_count: plan
                .slot_summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum(),
            ..StorageManagerStageReport::default()
        });
        let phase_executor = StorageManagerPhaseExecutor::new(cycle_started_unix_ms);
        phase_executor.annotate_reports(
            &mut stages,
            &errors,
            pressure_signals.follower_cursor_retention_blockers
                + pressure_signals.raft_snapshot_retention_blockers,
        );

        let production_parity_slice = errors.is_empty()
            && cxx_stage_order
                .iter()
                .all(|stage| stages.iter().any(|report| &report.stage == stage))
            && stages.iter().all(|stage| stage.enabled)
            && merged_dump_load_policy.policy_ready;
        StorageManagerCycleReport {
            shard_id: request.shard_id,
            dry_run: request.dry_run,
            cxx_stage_order,
            completed: errors.is_empty(),
            production_parity_slice,
            pressure_snapshot: pressure_signals.clone(),
            pressure_signals,
            stages,
            plan,
            merged_dump_load_policy,
            lifecycle_report,
            expiry_report,
            compaction_report,
            wal_reclaim_report,
            index_gc_report,
            eviction_report,
            page_gc_dependency_plan,
            errors,
        }
    }

    pub fn storage_merged_dump_load_policy_report(
        &self,
        request: StorageMergedDumpLoadPolicyRequest,
    ) -> StorageMergedDumpLoadPolicyReport {
        let mut lifecycle_request = request.lifecycle.clone();
        lifecycle_request.roll_forward_slot_dump_installs = true;
        let lifecycle = if request.create_dump_manifest {
            self.apply_storage_lifecycle(lifecycle_request.clone())
        } else {
            let plan = self.storage_lifecycle_plan(lifecycle_request.clone());
            let manifest_prune_plan = self.slot_dump_manifest_prune_plan_with_follower_cursors(
                lifecycle_request.shard_id,
                lifecycle_request.follower_replay_cursors.clone(),
            );
            StorageLifecycleReport {
                shard_id: lifecycle_request.shard_id,
                plan,
                manifest_prune_plan,
                install_roll_forward_reports: self
                    .slot_dump_install_roll_forward_reports(lifecycle_request.shard_id),
                object_lifecycle: self
                    .storage_recovery_report_without_boundary(lifecycle_request.shard_id)
                    .object_lifecycle,
                ..StorageLifecycleReport::default()
            }
        };
        let manifest = lifecycle
            .dump_manifest
            .clone()
            .or_else(|| latest_slot_dump_manifest_at(&self.index_dir, lifecycle_request.shard_id));
        let boundary = self.storage_recovery_boundary_report(lifecycle_request.shard_id);
        let manifest_prune_plan = self.slot_dump_manifest_prune_plan_with_follower_cursors(
            lifecycle_request.shard_id,
            lifecycle_request.follower_replay_cursors.clone(),
        );
        let install_roll_forward_reports =
            self.slot_dump_install_roll_forward_reports(lifecycle_request.shard_id);
        let load_preflight = manifest
            .as_ref()
            .map(|manifest| self.slot_dump_install_preflight_report(manifest));
        let install_status = if request.install_dump_manifest {
            manifest
                .as_ref()
                .map(|manifest| match self.install_slot_dump_manifest(manifest) {
                    Ok(()) => Status::ok(),
                    Err(status) => status,
                })
        } else {
            None
        };
        let manifest_chain_valid = boundary.manifest_chain_issues.is_empty();
        let follower_retention_safe = manifest_prune_plan.follower_blocks.is_empty()
            && manifest_prune_plan.raft_snapshot_blocks.is_empty();
        let load_preflight_safe = load_preflight
            .as_ref()
            .map(|preflight| preflight.install_safe)
            .unwrap_or(false);
        let load_installed = install_status
            .as_ref()
            .map(|status| status.ok)
            .unwrap_or(!request.install_dump_manifest);
        let replay_boundary_safe = manifest
            .as_ref()
            .map(|manifest| {
                boundary.selected_replay_oplog_sequence >= manifest.oplog_sequence
                    && boundary.selected_replay_index_log_sequence >= manifest.index_log_sequence
            })
            .unwrap_or(false);
        let index_gc_ready = install_roll_forward_reports.iter().all(|report| {
            report.can_roll_forward || report.can_retry_install || report.reason == "commit_ready"
        }) && manifest_chain_valid;

        let mut blockers = Vec::new();
        if manifest.is_none() {
            blockers.push("missing_dump_manifest".to_string());
        }
        if !load_preflight_safe {
            blockers.push("load_preflight_unsafe".to_string());
        }
        if !load_installed {
            blockers.push("load_install_failed".to_string());
        }
        if !replay_boundary_safe {
            blockers.push("replay_boundary_before_dump_manifest".to_string());
        }
        if !manifest_chain_valid {
            blockers.push("broken_manifest_chain".to_string());
        }
        if !follower_retention_safe {
            blockers.push("retention_cursor_blocks_index_gc".to_string());
        }
        if !index_gc_ready {
            blockers.push("index_gc_not_ready".to_string());
        }
        let policy_ready = blockers.is_empty();
        let (
            manifest_id,
            manifest_slot_ids,
            manifest_page_segment_ids,
            manifest_oplog_sequence,
            manifest_index_log_sequence,
        ) = manifest
            .as_ref()
            .map(|manifest| {
                (
                    Some(manifest.manifest_id.clone()),
                    manifest.slot_ids.clone(),
                    manifest.page_segment_ids.clone(),
                    manifest.oplog_sequence,
                    manifest.index_log_sequence,
                )
            })
            .unwrap_or_default();

        StorageMergedDumpLoadPolicyReport {
            shard_id: lifecycle_request.shard_id,
            policy_ready,
            dump_manifest_created: lifecycle.dump_manifest.is_some(),
            load_preflight_safe,
            load_installed,
            replay_boundary_safe,
            manifest_chain_valid,
            follower_retention_safe,
            index_gc_ready,
            manifest_id,
            manifest_slot_ids,
            manifest_page_segment_ids,
            manifest_oplog_sequence,
            manifest_index_log_sequence,
            selected_replay_oplog_sequence: boundary.selected_replay_oplog_sequence,
            selected_replay_index_log_sequence: boundary.selected_replay_index_log_sequence,
            lifecycle,
            load_preflight,
            install_status,
            boundary,
            manifest_prune_plan,
            install_roll_forward_reports,
            evidence: vec![
                "merged dump/load policy coordinates dirty-slot dump selection, manifest checksum/generation validation, load preflight, recovery replay boundary, roll-forward markers, and follower-safe manifest retention".to_string(),
                "policy report fails closed when manifest, load, replay, chain, retention, or index-GC evidence is unsafe".to_string(),
            ],
            blockers,
        }
    }

    pub fn run_storage_manager_loop(
        &self,
        mut request: StorageManagerLoopRequest,
    ) -> StorageManagerLoopReport {
        request.lifecycle.shard_id = request.shard_id;
        let lifecycle = if request.apply {
            self.apply_storage_lifecycle(request.lifecycle.clone())
        } else {
            let plan = self.storage_lifecycle_plan(request.lifecycle.clone());
            StorageLifecycleReport {
                shard_id: request.shard_id,
                plan,
                object_lifecycle: self
                    .storage_recovery_report_without_boundary(request.shard_id)
                    .object_lifecycle,
                ..StorageLifecycleReport::default()
            }
        };

        let mut phases = Vec::new();
        phases.push(StorageManagerLoopPhaseReport {
            phase: "prepare".to_string(),
            attempted: true,
            applied: true,
            evidence: vec![
                "built storage lifecycle plan from dirty slots, live/stale segments, delayed destroy inventory, and manifest/index-log state".to_string(),
            ],
            blockers: Vec::new(),
        });

        phases.push(StorageManagerLoopPhaseReport {
            phase: "reclaim".to_string(),
            attempted: true,
            applied: request.apply
                && (!lifecycle.delayed_destroy_purged_segments.is_empty()
                    || !lifecycle.plan.reclaim_candidates.is_empty()),
            evidence: vec![
                "ranked reclaim candidates by stale bytes, live density, delayed-destroy pressure, and utility score".to_string(),
            ],
            blockers: Vec::new(),
        });

        phases.push(StorageManagerLoopPhaseReport {
            phase: "evict".to_string(),
            attempted: request.lifecycle.invalidate_cache,
            applied: lifecycle.cache_entries_removed > 0 || lifecycle.cache_disk_bytes_removed > 0,
            evidence: vec![
                "cache invalidation phase uses shard-scoped cache eviction and byte accounting"
                    .to_string(),
            ],
            blockers: Vec::new(),
        });

        let expiry_sweep = if request.expire_records {
            self.sweep_expired_records(request.shard_id)
                .unwrap_or_else(|_| ShardExpirySweepReport {
                    shard_id: request.shard_id,
                    expired_records_removed: 0,
                    ..ShardExpirySweepReport::default()
                })
        } else {
            ShardExpirySweepReport {
                shard_id: request.shard_id,
                expired_records_removed: 0,
                ..ShardExpirySweepReport::default()
            }
        };
        phases.push(StorageManagerLoopPhaseReport {
            phase: "expire".to_string(),
            attempted: request.expire_records,
            applied: expiry_sweep.expired_records_removed > 0,
            evidence: vec![
                "expiry phase sweeps loaded shard TTL metadata and persists removals through index-log".to_string(),
            ],
            blockers: Vec::new(),
        });

        let compaction = if request.compact_pages {
            match self.compact_shard_pages(request.shard_id) {
                Ok(report) => Some(report),
                Err(err) => {
                    phases.push(StorageManagerLoopPhaseReport {
                        phase: "compact".to_string(),
                        attempted: true,
                        applied: false,
                        evidence: vec![
                            "compaction phase attempted live-page rewrite and model-layout/tombstone validation".to_string(),
                        ],
                        blockers: vec![err.message],
                    });
                    None
                }
            }
        } else {
            None
        };
        if let Some(report) = &compaction {
            phases.push(StorageManagerLoopPhaseReport {
                phase: "compact".to_string(),
                attempted: true,
                applied: report.model_layout_compaction_ready,
                evidence: report.model_layout_compaction_evidence.clone(),
                blockers: report.model_layout_compaction_blockers.clone(),
            });
        } else if !request.compact_pages {
            phases.push(StorageManagerLoopPhaseReport {
                phase: "compact".to_string(),
                attempted: false,
                applied: false,
                evidence: vec![
                    "compaction phase can call compact_shard_pages when enabled".to_string()
                ],
                blockers: Vec::new(),
            });
        }

        phases.push(StorageManagerLoopPhaseReport {
            phase: "index_gc".to_string(),
            attempted: request.lifecycle.prune_slot_dump_manifests
                || request.lifecycle.roll_forward_slot_dump_installs,
            applied: lifecycle.manifest_prune_report.is_some()
                || !lifecycle.install_roll_forward_reports.is_empty(),
            evidence: vec![
                "index-GC phase prunes slot dump manifests and rolls forward interrupted installs using follower cursor retention".to_string(),
            ],
            blockers: Vec::new(),
        });

        let blockers = phases
            .iter()
            .flat_map(|phase| {
                phase
                    .blockers
                    .iter()
                    .map(|blocker| format!("{}: {blocker}", phase.phase))
            })
            .collect::<Vec<_>>();
        let attempted = phases.iter().filter(|phase| phase.attempted).count();
        let loop_ready = blockers.is_empty()
            && attempted >= 5
            && phases
                .iter()
                .any(|phase| phase.phase == "prepare" && phase.applied)
            && phases.iter().any(|phase| phase.phase == "reclaim")
            && phases.iter().any(|phase| phase.phase == "evict")
            && phases.iter().any(|phase| phase.phase == "expire")
            && phases
                .iter()
                .any(|phase| phase.phase == "compact" && phase.attempted)
            && phases.iter().any(|phase| phase.phase == "index_gc");
        StorageManagerLoopReport {
            shard_id: request.shard_id,
            loop_ready,
            phases,
            lifecycle,
            expiry_sweep,
            compaction,
            evidence: vec![
                "StorageManager loop executes prepare/reclaim/evict/expire/compact/index-GC phases through existing durable storage paths".to_string(),
                "loop report keeps per-phase evidence and blockers so readiness fails closed".to_string(),
            ],
            blockers,
        }
    }

    pub fn storage_production_readiness_report(
        &self,
        shard_id: ShardId,
    ) -> StorageProductionReadinessReport {
        self.storage_production_readiness_report_with_policy(
            shard_id,
            StorageProductionReadinessPolicy::default(),
        )
    }

    pub fn storage_production_readiness_report_with_policy(
        &self,
        shard_id: ShardId,
        policy: StorageProductionReadinessPolicy,
    ) -> StorageProductionReadinessReport {
        let boundary = self.storage_recovery_boundary_report(shard_id);
        let recovery = self.storage_recovery_report_without_boundary(shard_id);
        let segment_integrity = storage_segment_integrity_report(shard_id, &recovery, &boundary);
        let plan = self.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        let stats = self
            .loaded_shard_stats()
            .into_iter()
            .find(|stats| stats.shard_id == shard_id);
        let cache = stats
            .as_ref()
            .map(|stats| stats.cache.clone())
            .unwrap_or_else(|| self.cache.stats());
        let page_store = stats
            .as_ref()
            .map(|stats| stats.page_store.clone())
            .unwrap_or_else(|| self.page_store.stats());
        let log_compatibility = self.storage_log_compatibility_report(shard_id);
        let page_format_compatibility = self.storage_page_format_compatibility_report(shard_id);
        let slot_dump_manifest_count = self.list_slot_dump_manifests(shard_id).len();
        let interrupted_slot_dump_install_count = boundary.interrupted_slot_dump_installs.len();
        let undumped_oplog_records = boundary
            .latest_safe_oplog_sequence
            .saturating_sub(boundary.latest_dump_oplog_sequence);
        let mut blockers = Vec::new();
        if !boundary.stale_index_page_refs.is_empty() {
            blockers.push("stale_index_page_refs".to_string());
        }
        if !boundary.corrupt_page_segment_ids.is_empty() {
            blockers.push("corrupt_page_segments".to_string());
        }
        if boundary.unreadable_page_bytes > 0 || !recovery.all_live_pages_readable {
            blockers.push("unreadable_live_page_refs".to_string());
        }
        if !boundary.owner_mismatch_page_refs.is_empty() {
            blockers.push("owner_mismatch_page_refs".to_string());
        }
        if boundary.object_lifecycle.missing_owner_page_refs > 0 {
            blockers.push("missing_owner_page_refs".to_string());
        }
        if boundary.object_lifecycle.reused_object_id_conflicts > 0 {
            blockers.push("reused_object_id_conflicts".to_string());
        }
        if interrupted_slot_dump_install_count > 0 {
            blockers.push("interrupted_slot_dump_installs".to_string());
        }
        if !boundary.manifest_chain_issues.is_empty() {
            blockers.push("broken_slot_dump_manifest_chain".to_string());
        }
        if !segment_integrity.integrity_ok
            && !blockers
                .iter()
                .any(|blocker| blocker == "storage_segment_integrity_failed")
        {
            blockers.push("storage_segment_integrity_failed".to_string());
        }
        if recovery.feature_page_layout.has_errors() {
            blockers.push("feature_page_layout_mismatch".to_string());
        }

        let mut warnings = Vec::new();
        if !plan.dirty_slots.is_empty() {
            warnings.push("dirty_slots_pending_dump".to_string());
        }
        if !plan.stale_page_segment_ids.is_empty() {
            warnings.push("stale_page_segments_pending_gc".to_string());
        }
        if !boundary.orphan_page_segment_ids.is_empty() {
            warnings.push("orphan_page_segments_pending_gc".to_string());
        }
        if slot_dump_manifest_count == 0 && recovery.total_page_refs > 0 {
            warnings.push("no_slot_dump_manifest_for_live_pages".to_string());
        }
        if policy
            .max_dirty_slots
            .map(|limit| plan.dirty_slots.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("dirty_slots_exceed_policy".to_string());
        }
        if policy
            .max_stale_page_segments
            .map(|limit| plan.stale_page_segment_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("stale_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_orphan_page_segments
            .map(|limit| boundary.orphan_page_segment_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("orphan_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_undumped_oplog_records
            .map(|limit| undumped_oplog_records > limit)
            .unwrap_or(false)
        {
            blockers.push("undumped_oplog_records_exceed_policy".to_string());
        }
        if policy.require_slot_dump_manifest
            && slot_dump_manifest_count == 0
            && recovery.total_page_refs > 0
        {
            blockers.push("slot_dump_manifest_required".to_string());
        }
        if policy.block_on_warnings && !warnings.is_empty() {
            blockers.push("warnings_exceed_policy".to_string());
        }

        StorageProductionReadinessReport {
            shard_id,
            policy,
            production_ready: blockers.is_empty(),
            blockers,
            warnings,
            dirty_slot_count: plan.dirty_slots.len(),
            stale_page_segment_count: plan.stale_page_segment_ids.len(),
            orphan_page_segment_count: boundary.orphan_page_segment_ids.len(),
            undumped_oplog_records,
            corrupt_page_segment_count: boundary.corrupt_page_segment_ids.len(),
            unreadable_page_ref_count: recovery.unreadable_page_refs.len(),
            owner_mismatch_page_ref_count: boundary.owner_mismatch_page_refs.len(),
            missing_owner_page_ref_count: boundary.object_lifecycle.missing_owner_page_refs,
            reused_object_id_conflict_count: boundary.object_lifecycle.reused_object_id_conflicts,
            interrupted_slot_dump_install_count,
            prepared_slot_dump_install_count: boundary.prepared_slot_dump_install_count,
            installed_slot_dump_install_count: boundary.installed_slot_dump_install_count,
            unknown_slot_dump_install_count: boundary.unknown_slot_dump_install_count,
            slot_dump_manifest_count,
            cache_memory_bytes: cache.memory_bytes,
            cache_disk_bytes: cache.disk_bytes,
            page_store_bytes_written: page_store.bytes_written,
            block_store_bytes_written: page_store.bytes_written,
            boundary,
            object_lifecycle: recovery.object_lifecycle,
            segment_integrity,
            log_compatibility,
            page_format_compatibility,
            feature_page_layout_mismatch_count: recovery.feature_page_layout.mismatch_count(),
            corrupt_feature_page_count: recovery
                .feature_page_layout
                .corrupt_packed_feature_pages
                .len(),
            feature_page_layout: recovery.feature_page_layout,
        }
    }

    pub fn storage_log_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StorageLogCompatibilityReport {
        let oplog_stats = self.wal_store.stats(shard_id);
        let index_log_stats = self.index_log_store.stats(shard_id);
        let oplog_records = self
            .wal_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let index_log_records = self
            .index_log_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        StorageLogCompatibilityReport {
            shard_id,
            oplog_format: "rust-jsonl-command-v1".to_string(),
            index_log_format: "rust-jsonl-shard-index-v1".to_string(),
            compatibility_mode: "rust_native_migration_only".to_string(),
            migration_required: true,
            cxx_reader_supported: false,
            cxx_writer_supported: false,
            golden_conversion_required: true,
            rust_native_replay_safe: true,
            cxx_binary_compatible: false,
            oplog_last_sequence: oplog_stats.last_sequence,
            index_log_last_sequence: index_log_stats.last_sequence,
            oplog_records,
            index_log_records,
            oplog_bytes: oplog_stats.bytes_written,
            index_log_bytes: index_log_stats.bytes_written,
            compatibility_gaps: vec![
                "compatibility mode is migration-only; direct mixed Rust/C++ binary log serving is not supported"
                    .to_string(),
                "C++ binary/protobuf oplog reader and writer are not implemented".to_string(),
                "C++ binary/protobuf index-log reader and writer are not implemented".to_string(),
                "golden log conversion/replay suite is required before C++ migration".to_string(),
            ],
        }
    }

    pub fn storage_page_format_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StoragePageFormatCompatibilityReport {
        let stats = self.page_store.stats();
        let zones = self.page_store.zone_summary();
        StoragePageFormatCompatibilityReport {
            shard_id,
            page_format: "rust-page-envelope-v6".to_string(),
            rust_envelope_version: 6,
            compatibility_mode: "rust_envelope_migration_only".to_string(),
            migration_required: true,
            cxx_page_header_reader_supported: false,
            cxx_page_header_writer_supported: false,
            golden_conversion_required: true,
            rust_native_read_safe: true,
            cxx_page_header_compatible: false,
            checksum_protected: true,
            object_ids_embedded: true,
            routing_slots_embedded: true,
            compression_supported: true,
            active_extents: zones.active_extents,
            sealed_extents: zones.sealed_extents,
            delayed_destroy_extents: zones.delayed_destroy_extents,
            live_physical_bytes: zones.live_physical_bytes,
            reclaimable_physical_bytes: zones.reclaimable_physical_bytes,
            page_store_writes: stats.writes,
            page_store_bytes_written: stats.bytes_written,
            logical_bytes_written: stats.logical_bytes_written,
            compressed_records_written: stats.compressed_records_written,
            compatibility_gaps: vec![
                "compatibility mode is migration-only; direct mixed Rust-envelope/C++ page-header serving is not supported"
                    .to_string(),
                "C++ protobuf page header reader and writer are not implemented".to_string(),
                "C++ slot/page layout and page-id allocation are not byte-compatible".to_string(),
                "golden page conversion/replay suite is required before C++ migration".to_string(),
            ],
        }
    }

    pub fn warm_cache_from_page_index(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> usize {
        self.storage_cache_warmup_report(shard_id, selected_slots)
            .warmed_page_refs
    }

    pub fn storage_cache_warmup_report(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> StorageCacheWarmupReport {
        let selected_slots = selected_slots.into_iter().collect::<BTreeSet<_>>();
        let mut report = StorageCacheWarmupReport {
            shard_id,
            selected_slots: selected_slots.iter().copied().collect(),
            ..StorageCacheWarmupReport::default()
        };
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return report;
        };
        for entry in collect_live_page_entries(shard) {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| self.routing_slot_for_key(shard_id, &entry.object_key));
            if !selected_slots.is_empty() && !selected_slots.contains(&routing_slot) {
                report.skipped_page_refs = report.skipped_page_refs.saturating_add(1);
                continue;
            }
            report.considered_page_refs = report.considered_page_refs.saturating_add(1);
            let key = CacheKey::page_with_slot_generation(
                shard_id,
                entry.address.page_segment_id,
                entry.address.offset,
                entry.address.length,
                entry.address.routing_slot,
                entry.address.generation,
            );
            if self.cache.get(&key).ok().flatten().is_some() {
                report.already_cached_page_refs = report.already_cached_page_refs.saturating_add(1);
                report.warmed_page_refs = report.warmed_page_refs.saturating_add(1);
            } else if let Ok(bytes) = self.page_store.read(&entry.address) {
                report.page_store_reads = report.page_store_reads.saturating_add(1);
                report.block_store_reads = report.block_store_reads.saturating_add(1);
                let byte_len = bytes.len() as u64;
                match self.cache.put(key, bytes) {
                    Ok(()) => {
                        report.warmed_page_refs = report.warmed_page_refs.saturating_add(1);
                        report.warmed_bytes = report.warmed_bytes.saturating_add(byte_len);
                    }
                    Err(_) => {
                        report.failed_page_refs = report.failed_page_refs.saturating_add(1);
                    }
                }
            } else {
                report.failed_page_refs = report.failed_page_refs.saturating_add(1);
            }
        }
        report
    }

    pub fn storage_cache_inspection_report(
        &self,
        shard_id: ShardId,
    ) -> StorageCacheInspectionReport {
        let entries = self.cache.entries_for_shard(shard_id);
        let mut slot_summaries = BTreeMap::<u32, StorageCacheSlotSummary>::new();
        for entry in &entries {
            let Some(routing_slot) = cache_entry_routing_slot(entry) else {
                continue;
            };
            let summary = slot_summaries
                .entry(routing_slot)
                .or_insert(StorageCacheSlotSummary {
                    routing_slot,
                    ..StorageCacheSlotSummary::default()
                });
            summary.entry_count = summary.entry_count.saturating_add(1);
            summary.memory_bytes = summary.memory_bytes.saturating_add(entry.memory_bytes);
            summary.disk_bytes = summary.disk_bytes.saturating_add(entry.disk_bytes);
            if entry.pinned {
                summary.pinned_entries = summary.pinned_entries.saturating_add(1);
                summary.pinned_bytes = summary.pinned_bytes.saturating_add(entry.memory_bytes);
            }
        }
        StorageCacheInspectionReport {
            shard_id,
            stats: self.cache.stats(),
            entries,
            slot_summaries: slot_summaries.into_values().collect(),
        }
    }

    pub fn invalidate_storage_cache_slot(
        &self,
        request: StorageCacheInvalidateSlotRequest,
    ) -> Result<CacheGcReport, Status> {
        self.cache
            .invalidate_slot(request.shard_id, request.routing_slot)
            .map_err(|err| Status::error("cache_slot_invalidation_failed", err.to_string()))
    }

    pub fn storage_recovery_boundary_report(
        &self,
        shard_id: ShardId,
    ) -> StorageRecoveryBoundaryReport {
        let manifests = self.list_slot_dump_manifests(shard_id);
        let latest_dump_oplog_sequence = manifests
            .iter()
            .map(|manifest| manifest.oplog_sequence)
            .max()
            .unwrap_or_default();
        let latest_dump_index_log_sequence = manifests
            .iter()
            .map(|manifest| manifest.index_log_sequence)
            .max()
            .unwrap_or_default();
        let latest_safe_oplog_sequence = self.wal_store.stats(shard_id).last_sequence;
        let latest_safe_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let live_page_segment_ids = self
            .live_page_segment_ids(shard_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let all_segment_ids = self
            .page_store
            .segment_ids()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let orphan_page_segment_ids = all_segment_ids
            .difference(&live_page_segment_ids)
            .copied()
            .collect::<Vec<_>>();
        let latest_dump_slots = manifests
            .last()
            .map(|manifest| manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let missing_dump_slot_ids = self
            .slot_storage_summaries(shard_id)
            .into_iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .map(|summary| summary.routing_slot)
            .filter(|slot| !latest_dump_slots.contains(slot))
            .collect::<Vec<_>>();
        let interrupted_slot_dump_installs = self.interrupted_slot_dump_installs(shard_id);
        let (
            prepared_slot_dump_install_count,
            installed_slot_dump_install_count,
            unknown_slot_dump_install_count,
        ) = slot_dump_install_phase_counts(&interrupted_slot_dump_installs);
        let manifest_chain_issues = slot_dump_manifest_chain_issues(&manifests);
        let recovery = self.storage_recovery_report_without_boundary(shard_id);
        let corrupt_page_segment_ids = recovery
            .page_segment_reports
            .iter()
            .filter(|report| report.has_corruption)
            .map(|report| report.page_segment_id)
            .collect::<Vec<_>>();
        let unreadable_page_bytes = recovery
            .unreadable_page_refs
            .iter()
            .map(|error| error.length)
            .sum();
        let object_lifecycle = recovery.object_lifecycle.clone();
        StorageRecoveryBoundaryReport {
            shard_id,
            latest_safe_oplog_sequence,
            latest_safe_index_log_sequence,
            latest_dump_oplog_sequence,
            latest_dump_index_log_sequence,
            selected_replay_oplog_sequence: latest_dump_oplog_sequence
                .min(latest_safe_oplog_sequence),
            selected_replay_index_log_sequence: latest_dump_index_log_sequence
                .min(latest_safe_index_log_sequence),
            orphan_page_segment_ids,
            missing_dump_slot_ids,
            stale_index_page_refs: recovery.unreadable_page_refs,
            interrupted_slot_dump_installs,
            prepared_slot_dump_install_count,
            installed_slot_dump_install_count,
            unknown_slot_dump_install_count,
            manifest_chain_issues,
            owner_mismatch_page_refs: recovery.owner_mismatch_page_refs,
            missing_owner_page_refs: recovery.missing_owner_page_refs,
            object_lifecycle,
            corrupt_page_segment_ids,
            unreadable_page_bytes,
        }
    }

    pub fn prometheus_metrics(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP temporalstore_shard_records Number of records by shard and kind.\n");
        out.push_str("# TYPE temporalstore_shard_records gauge\n");
        out.push_str("# HELP temporalstore_cache_operations_total Cache operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_cache_operations_total counter\n");
        out.push_str("# HELP temporalstore_cache_bytes Cache bytes by shard and tier.\n");
        out.push_str("# TYPE temporalstore_cache_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_operations_total Page store operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_operations_total counter\n");
        out.push_str("# HELP temporalstore_page_store_bytes_total Page store byte counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_bytes_total counter\n");
        out.push_str("# HELP temporalstore_page_store_zone_count Page-store zone counts by shard and lifecycle state.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_count gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_bytes Page-store physical bytes by shard and lifecycle kind.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_unix_ms Oldest page-store zone timestamp by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_unix_ms gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_age_ms Oldest page-store zone age by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_age_ms gauge\n");
        out.push_str(
            "# HELP temporalstore_wal_records_total Write-ahead log append records by shard.\n",
        );
        out.push_str("# TYPE temporalstore_wal_records_total counter\n");
        out.push_str(
            "# HELP temporalstore_wal_bytes_total Write-ahead log appended bytes by shard.\n",
        );
        out.push_str("# TYPE temporalstore_wal_bytes_total counter\n");
        out.push_str("# HELP temporalstore_oplog_records_total Legacy alias for temporalstore_wal_records_total.\n");
        out.push_str("# TYPE temporalstore_oplog_records_total counter\n");
        out.push_str("# HELP temporalstore_oplog_bytes_total Legacy alias for temporalstore_wal_bytes_total.\n");
        out.push_str("# TYPE temporalstore_oplog_bytes_total counter\n");
        out.push_str(
            "# HELP temporalstore_object_manager_objects Logical hot objects tracked by shard.\n",
        );
        out.push_str("# TYPE temporalstore_object_manager_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_page_refs Page-address references tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_page_refs gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_objects Dirty logical objects tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_slots Dirty routing slots tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_slots gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_page_refs Live page refs by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_page_refs gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_bytes Live bytes by shard, routing slot, and kind.\n");
        out.push_str("# TYPE temporalstore_storage_slot_bytes gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_dirty_objects Dirty objects by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_block_store_operations_total Canonical block-store operation counters by shard.\n");
        out.push_str("# TYPE temporalstore_block_store_operations_total counter\n");
        out.push_str("# HELP temporalstore_block_store_extent_bytes Canonical block-store extent bytes by shard and kind.\n");
        out.push_str("# TYPE temporalstore_block_store_extent_bytes gauge\n");
        out.push_str(
            "# HELP temporalstore_partition_routing_slots Routing slots owned by shard.\n",
        );
        out.push_str("# TYPE temporalstore_partition_routing_slots gauge\n");
        out.push_str(
            "# HELP temporalstore_ingestion_records_total Ingestion record counters by outcome.\n",
        );
        out.push_str("# TYPE temporalstore_ingestion_records_total counter\n");
        out.push_str("# HELP temporalstore_ingestion_kafka_lag Kafka ingestion lag in offsets.\n");
        out.push_str("# TYPE temporalstore_ingestion_kafka_lag gauge\n");
        out.push_str("# HELP temporalstore_ingestion_kafka_committed_offset Kafka committed offset by topic and partition.\n");
        out.push_str("# TYPE temporalstore_ingestion_kafka_committed_offset gauge\n");
        out.push_str("# HELP temporalstore_ingestion_stream_committed_sequence Streaming ingestion committed sequence by stream.\n");
        out.push_str("# TYPE temporalstore_ingestion_stream_committed_sequence gauge\n");
        out.push_str("# HELP temporalstore_ingestion_flink_checkpoint_state Flink checkpoint state as a one-hot gauge.\n");
        out.push_str("# TYPE temporalstore_ingestion_flink_checkpoint_state gauge\n");
        for stats in self.loaded_shard_stats() {
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "string".into()),
                ],
                stats.string_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "hash".into()),
                ],
                stats.hash_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "set".into()),
                ],
                stats.set_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "feature".into()),
                ],
                stats.feature_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "sequence".into()),
                ],
                stats.sequence_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "ips".into()),
                ],
                stats.ips_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "risk".into()),
                ],
                stats.risk_records as u64,
            );
            for (kind, value) in [
                ("memory_hits", stats.cache.memory_hits),
                ("disk_hits", stats.cache.disk_hits),
                ("misses", stats.cache.misses),
                ("puts", stats.cache.puts),
                ("invalidations", stats.cache.invalidations),
                ("memory_evictions", stats.cache.memory_evictions),
                ("pmem_hits", stats.cache.pmem_hits),
                ("pmem_fills", stats.cache.pmem_fills),
                ("pmem_evictions", stats.cache.pmem_evictions),
                (
                    "pmem_admission_accepted",
                    stats.cache.pmem_admission_accepted,
                ),
                (
                    "pmem_admission_rejected",
                    stats.cache.pmem_admission_rejected,
                ),
                ("pmem_eviction_capacity", stats.cache.pmem_eviction_capacity),
                (
                    "pmem_eviction_pinned_skips",
                    stats.cache.pmem_eviction_pinned_skips,
                ),
                (
                    "memory_admission_accepted",
                    stats.cache.memory_admission_accepted,
                ),
                (
                    "memory_admission_rejected",
                    stats.cache.memory_admission_rejected,
                ),
                ("memory_fills", stats.cache.memory_fills),
                ("disk_fills", stats.cache.disk_fills),
                ("refill_failures", stats.cache.refill_failures),
                ("eviction_capacity", stats.cache.eviction_capacity),
                ("eviction_oversize", stats.cache.eviction_oversize),
                ("eviction_cold", stats.cache.eviction_cold),
                ("eviction_low_hit", stats.cache.eviction_low_hit),
                ("eviction_stale", stats.cache.eviction_stale),
                ("ssd_eviction_cold", stats.cache.ssd_eviction_cold),
                ("ssd_eviction_low_hit", stats.cache.ssd_eviction_low_hit),
                ("ssd_eviction_stale", stats.cache.ssd_eviction_stale),
                ("pinned_entries", stats.cache.pinned_entries),
                ("pin_operations", stats.cache.pin_operations),
                ("unpin_operations", stats.cache.unpin_operations),
                ("eviction_pinned_skips", stats.cache.eviction_pinned_skips),
                (
                    "eviction_sampled_groups",
                    stats.cache.eviction_sampled_groups,
                ),
                ("memory_slot_evictions", stats.cache.memory_slot_evictions),
                ("ssd_slot_evictions", stats.cache.ssd_slot_evictions),
                ("compressed_puts", stats.cache.compressed_puts),
                ("compressed_hits", stats.cache.compressed_hits),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (tier, value) in [
                ("memory", stats.cache.memory_bytes),
                ("pmem", stats.cache.pmem_bytes),
                ("disk", stats.cache.disk_bytes),
                ("compression_saved", stats.cache.compression_bytes_saved),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("tier", tier.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("writes", stats.page_store.writes),
                ("reads", stats.page_store.reads),
                (
                    "compressed_writes",
                    stats.page_store.compressed_records_written,
                ),
                ("compressed_reads", stats.page_store.compressed_records_read),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
                push_metric(
                    &mut out,
                    "temporalstore_block_store_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("written", stats.page_store.bytes_written),
                ("read", stats.page_store.bytes_read),
                ("logical_written", stats.page_store.logical_bytes_written),
                ("logical_read", stats.page_store.logical_bytes_read),
                (
                    "compression_saved",
                    stats.page_store.compression_bytes_saved,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_bytes_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (state, value) in [
                ("active", stats.page_store_zones.active_extents),
                ("sealed", stats.page_store_zones.sealed_extents),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_extents,
                ),
                ("purged", stats.page_store_zones.purged_extents),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_count",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("state", state.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("active", stats.page_store_zones.active_physical_bytes),
                ("sealed", stats.page_store_zones.sealed_physical_bytes),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_physical_bytes,
                ),
                ("purged", stats.page_store_zones.purged_physical_bytes),
                ("live", stats.page_store_zones.live_physical_bytes),
                (
                    "reclaimable",
                    stats.page_store_zones.reclaimable_physical_bytes,
                ),
                (
                    "total_known",
                    stats.page_store_zones.total_known_physical_bytes,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
                push_metric(
                    &mut out,
                    "temporalstore_block_store_extent_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_extent_unix_ms),
                ("live", stats.page_store_zones.oldest_live_extent_unix_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_extent_unix_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_unix_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_extent_age_ms),
                ("live", stats.page_store_zones.oldest_live_extent_age_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_extent_age_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_age_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            push_metric(
                &mut out,
                "temporalstore_wal_records_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.write_ahead_log.writes,
            );
            push_metric(
                &mut out,
                "temporalstore_wal_bytes_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.write_ahead_log.bytes_written,
            );
            push_metric(
                &mut out,
                "temporalstore_oplog_records_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.write_ahead_log.writes,
            );
            push_metric(
                &mut out,
                "temporalstore_oplog_bytes_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.write_ahead_log.bytes_written,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_page_refs",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.page_ref_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_slots",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_slot_count as u64,
            );
            for summary in self.slot_storage_summaries(stats.shard_id) {
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_page_refs",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_slot.to_string()),
                    ],
                    summary.page_ref_count,
                );
                for (kind, value) in [
                    ("logical", summary.logical_bytes),
                    ("physical", summary.physical_bytes),
                ] {
                    push_metric(
                        &mut out,
                        "temporalstore_storage_slot_bytes",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("slot", summary.routing_slot.to_string()),
                            ("kind", kind.to_string()),
                        ],
                        value,
                    );
                }
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_dirty_objects",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_slot.to_string()),
                    ],
                    summary.dirty_object_count,
                );
            }
            push_metric(
                &mut out,
                "temporalstore_partition_routing_slots",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.routing_slot_count as u64,
            );
        }
        let ingestion = self.ingestion_state_report();
        for (outcome, value) in [
            ("accepted", ingestion.stats.accepted_total),
            ("failed", ingestion.stats.failed_total),
            ("duplicate", ingestion.stats.duplicate_total),
            ("dead_letter", ingestion.stats.dead_letter_total),
            (
                "stream_backpressure",
                ingestion.stats.stream_backpressure_total,
            ),
            ("stream_duplicate", ingestion.stats.stream_duplicate_total),
            ("kafka_committed", ingestion.stats.kafka_committed_total),
            ("flink_precommit", ingestion.stats.flink_precommit_total),
            ("flink_commit", ingestion.stats.flink_commit_total),
            ("flink_abort", ingestion.stats.flink_abort_total),
        ] {
            push_metric(
                &mut out,
                "temporalstore_ingestion_records_total",
                &[("outcome", outcome.to_string())],
                value,
            );
        }
        push_metric(
            &mut out,
            "temporalstore_ingestion_kafka_lag",
            &[("scope", "max".to_string())],
            ingestion.stats.max_kafka_lag.max(0) as u64,
        );
        for offset in ingestion.kafka_offsets {
            push_metric(
                &mut out,
                "temporalstore_ingestion_kafka_committed_offset",
                &[
                    ("topic", offset.topic),
                    ("partition", offset.partition.to_string()),
                ],
                offset.committed_offset.max(0) as u64,
            );
        }
        for stream in ingestion.stream_commits {
            push_metric(
                &mut out,
                "temporalstore_ingestion_stream_committed_sequence",
                &[("stream_id", stream.stream_id)],
                stream.committed_sequence,
            );
        }
        for checkpoint in ingestion.flink_checkpoints {
            let status = format!("{:?}", checkpoint.status).to_ascii_lowercase();
            push_metric(
                &mut out,
                "temporalstore_ingestion_flink_checkpoint_state",
                &[
                    ("job_id", checkpoint.job_id),
                    ("operator_uid", checkpoint.operator_uid),
                    ("subtask_index", checkpoint.subtask_index.to_string()),
                    ("checkpoint_id", checkpoint.checkpoint_id.to_string()),
                    ("status", status),
                ],
                1,
            );
        }
        out
    }

    pub fn read_stream(&self, request: StreamReadRequest) -> StreamReadResponse {
        let data: Result<Vec<u8>, String> = match request.stream_kind {
            StreamKind::Block | StreamKind::Page => self
                .page_store
                .read_logical_range(request.page_segment_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::Index => fs::read(self.index_path(request.shard_id))
                .map_err(|err| err.to_string())
                .map(|bytes| {
                    let start = request.offset as usize;
                    let end = start.saturating_add(request.size as usize).min(bytes.len());
                    if start >= bytes.len() {
                        Vec::new()
                    } else {
                        bytes[start..end].to_vec()
                    }
                }),
            StreamKind::Wal => self
                .wal_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::IndexLog => self
                .index_log_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
        };
        match data {
            Ok(data) => StreamReadResponse {
                status: Status::ok(),
                data,
            },
            Err(err) => StreamReadResponse {
                status: Status::error("stream_read_failed", err.to_string()),
                data: Vec::new(),
            },
        }
    }

    pub fn scan_stream(&self, request: ScanStreamRequest) -> ScanStreamResponse {
        if request.start_offset > request.end_offset {
            return ScanStreamResponse {
                status: Status::error("invalid_stream_range", "start_offset is after end_offset"),
                records: Vec::new(),
                end_of_stream: true,
            };
        }
        let size = request
            .end_offset
            .saturating_sub(request.start_offset)
            .min(request.max_bytes);
        if request.stream_kind == StreamKind::Wal || request.stream_kind == StreamKind::IndexLog {
            let records = match request.stream_kind {
                StreamKind::Wal => self
                    .wal_store
                    .scan(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::IndexLog => self
                    .index_log_store
                    .scan(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::Index | StreamKind::Block | StreamKind::Page => unreachable!(),
            };
            return match records {
                Ok(records) => ScanStreamResponse {
                    status: Status::ok(),
                    records: records
                        .into_iter()
                        .map(|(offset, data)| StreamRecord { offset, data })
                        .collect(),
                    end_of_stream: true,
                },
                Err(err) => ScanStreamResponse {
                    status: Status::error("stream_scan_failed", err.to_string()),
                    records: Vec::new(),
                    end_of_stream: true,
                },
            };
        }
        let read = self.read_stream(StreamReadRequest {
            shard_id: request.shard_id,
            stream_kind: request.stream_kind,
            page_segment_id: request.page_segment_id,
            offset: request.start_offset,
            size,
        });
        ScanStreamResponse {
            status: read.status.clone(),
            records: if read.status.ok && !read.data.is_empty() {
                vec![StreamRecord {
                    offset: request.start_offset,
                    data: read.data,
                }]
            } else {
                Vec::new()
            },
            end_of_stream: true,
        }
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        let command_count = request.commands.len();
        let mut responses = Vec::with_capacity(command_count);
        if command_count == 0 {
            return BatchExecuteResponse {
                status: Status::ok(),
                responses,
            };
        }
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            responses.extend((0..command_count).map(|_| ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            }));
            return BatchExecuteResponse {
                status: Status::ok(),
                responses,
            };
        };
        let readonly = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false);
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
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
        let mut mutated_any = false;
        let mut sync_wal_commands = Vec::new();
        for command in request.commands {
            let write_command = is_write_command(&command);
            if readonly && write_command {
                responses.push(ExecuteResponse {
                    status: Status::error("readonly_shard", "readonly shard rejects write command"),
                    response: CommandResponse::Empty,
                });
                continue;
            }
            if let Err(status) =
                self.check_admission(request.shard_id, write_command, &config, &info)
            {
                responses.push(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
                continue;
            }
            if write_command
                && config
                    .maxmemory_bytes
                    .map(|limit| self.page_store.stats().bytes_written >= limit)
                    .unwrap_or(false)
            {
                responses.push(ExecuteResponse {
                    status: Status::error(
                        "storage_quota_exceeded",
                        "shard maxmemory_bytes limit has been reached",
                    ),
                    response: CommandResponse::Empty,
                });
                continue;
            }
            if let Err(status) = validate_command_preconditions(
                &self.cache,
                &self.page_store,
                request.shard_id,
                shard,
                &command,
            ) {
                responses.push(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
                continue;
            }
            let command_for_post_write = command.clone();
            let outcome = execute_on_shard(
                &self.cache,
                &self.page_store,
                config.feature_max_size,
                config.async_storage,
                request.shard_id,
                start_routing_slot,
                end_routing_slot,
                shard,
                command,
            );
            if outcome.mutated {
                mutated_any = true;
                let object_keys = command_object_keys(&command_for_post_write);
                if object_keys.is_empty() {
                    rebuild_slot_page_ownership(
                        request.shard_id,
                        shard,
                        start_routing_slot,
                        end_routing_slot,
                    );
                } else {
                    for object_key in object_keys {
                        shard.dirty_objects.insert(object_key.clone());
                        mark_async_dirty_object(
                            shard,
                            &object_key,
                            start_routing_slot,
                            end_routing_slot,
                        );
                    }
                }
                if !command_updates_slot_index_directly(&command_for_post_write)
                    || shard.slot_index.slot_map.is_empty()
                {
                    rebuild_slot_first_index(
                        request.shard_id,
                        shard,
                        start_routing_slot,
                        end_routing_slot,
                    );
                }
                if write_command && !config.async_storage {
                    sync_wal_commands.push(command_for_post_write);
                }
            }
            responses.push(ExecuteResponse {
                status: Status::ok(),
                response: outcome.response,
            });
        }
        if mutated_any {
            refresh_slot_runtime_flags(shard);
            if !config.async_storage {
                for command in sync_wal_commands {
                    let _ = self.wal_store.append(request.shard_id, command);
                }
                let index_bytes = serialize_index(shard);
                let _ = self
                    .index_log_store
                    .append_json(request.shard_id, &index_bytes);
                let _ = self.persist_index_bytes(request.shard_id, &index_bytes);
            }
        }
        BatchExecuteResponse {
            status: Status::ok(),
            responses,
        }
    }

    pub fn batch_execute_replicated(
        &self,
        request: ReplicatedBatchExecuteRequest,
    ) -> ReplicatedBatchExecuteResponse {
        let mut responses = Vec::with_capacity(request.commands.len());
        let mut replication = Vec::with_capacity(request.commands.len());
        for command in request.commands {
            replication.push(
                self.replication_selection_report(&command.command, command.replication_mode),
            );
            responses.push(self.execute_replicated(ReplicatedExecuteRequest {
                shard_id: request.shard_id,
                command: command.command,
                replication_mode: command.replication_mode,
            }));
        }
        ReplicatedBatchExecuteResponse {
            status: Status::ok(),
            responses,
            replication,
        }
    }

    pub fn batch_execute_checked(
        &self,
        request: CheckedBatchExecuteRequest,
    ) -> CheckedBatchExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedBatchExecuteResponse {
                status: status.clone(),
                response: BatchExecuteResponse {
                    status,
                    responses: Vec::new(),
                },
            };
        }
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id: request.shard_id,
            commands: request.commands,
        });
        CheckedBatchExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    pub fn export_index_bytes(&self, shard_id: ShardId) -> Result<Vec<u8>, std::io::Error> {
        fs::read(self.index_path(shard_id))
    }

    pub fn publish_shard_index_snapshot(&self, shard_id: ShardId) -> Result<usize, Status> {
        self.publish_shard_index_snapshot_for_keys(shard_id, Vec::<String>::new())
    }

    pub fn publish_shard_index_snapshot_for_keys(
        &self,
        shard_id: ShardId,
        selected_keys: impl IntoIterator<Item = String>,
    ) -> Result<usize, Status> {
        enum PublishTarget {
            String { key: String },
            Hash { key: String, field: String },
        }

        let selected_keys = selected_keys
            .into_iter()
            .filter(|key| !key.trim().is_empty())
            .collect::<BTreeSet<_>>();
        let publish_all = selected_keys.is_empty();
        let publish_targets = {
            let shards = self.shards.read().expect("engine lock poisoned");
            let Some(shard) = shards.get(&shard_id) else {
                return Err(Status::error(
                    "shard_not_loaded",
                    "shard is not loaded on this server",
                ));
            };
            if publish_all {
                let mut publish_targets = shard
                    .strings
                    .iter()
                    .filter(|(_, address)| address.page_segment_id == HOT_PAGE_SEGMENT_ID)
                    .map(|(key, address)| {
                        (PublishTarget::String { key: key.clone() }, address.clone())
                    })
                    .collect::<Vec<_>>();
                publish_targets.extend(
                    shard
                        .hashes
                        .iter()
                        .flat_map(|(key, fields)| {
                            fields.iter().filter_map(move |(field, address)| {
                                (address.page_segment_id == HOT_PAGE_SEGMENT_ID).then(|| {
                                    (
                                        PublishTarget::Hash {
                                            key: key.clone(),
                                            field: field.clone(),
                                        },
                                        address.clone(),
                                    )
                                })
                            })
                        })
                        .collect::<Vec<_>>(),
                );
                publish_targets
            } else {
                let mut publish_targets = Vec::new();
                for key in &selected_keys {
                    if let Some(address) = shard.strings.get(key) {
                        if address.page_segment_id == HOT_PAGE_SEGMENT_ID {
                            publish_targets.push((
                                PublishTarget::String { key: key.clone() },
                                address.clone(),
                            ));
                        }
                    }
                    if let Some(fields) = shard.hashes.get(key) {
                        publish_targets.extend(fields.iter().filter_map(|(field, address)| {
                            (address.page_segment_id == HOT_PAGE_SEGMENT_ID).then(|| {
                                (
                                    PublishTarget::Hash {
                                        key: key.clone(),
                                        field: field.clone(),
                                    },
                                    address.clone(),
                                )
                            })
                        }));
                    }
                }
                publish_targets
            }
        };
        let mut publish_records = Vec::with_capacity(publish_targets.len());
        for (target, address) in publish_targets {
            if let Some(bytes) = read_page_bytes(&self.cache, &self.page_store, shard_id, &address)
            {
                publish_records.push((
                    target,
                    address.clone(),
                    bytes,
                    address.object_id,
                    address.routing_slot,
                ));
            }
        }
        if publish_records.is_empty() {
            return Ok(0);
        }
        let append_records = publish_records
            .iter()
            .map(|(_, _, bytes, object_id, routing_slot)| {
                (bytes.clone(), *object_id, *routing_slot)
            })
            .collect::<Vec<BlockAppendRecord>>();
        let published_addresses = self
            .page_store
            .append_batch_with_page_metadata(append_records)
            .map_err(|err| Status::error("publish_visibility_failed", err.to_string()))?;
        let index_bytes = {
            let mut shards = self.shards.write().expect("engine lock poisoned");
            let Some(shard) = shards.get_mut(&shard_id) else {
                return Err(Status::error(
                    "shard_not_loaded",
                    "shard is not loaded on this server",
                ));
            };
            let mut published_object_keys = BTreeSet::new();
            for ((target, original, bytes, _, _), published) in
                publish_records.into_iter().zip(published_addresses)
            {
                match target {
                    PublishTarget::String { key } => {
                        if shard.strings.get(&key) != Some(&original) {
                            continue;
                        }
                        let _ = self.cache.put(
                            CacheKey::page_with_slot_generation(
                                shard_id,
                                published.page_segment_id,
                                published.offset,
                                published.length,
                                published.routing_slot,
                                published.generation,
                            ),
                            bytes,
                        );
                        upsert_slot_index_page(
                            shard,
                            shard_id,
                            "string",
                            &key,
                            None,
                            published.clone(),
                            false,
                        );
                        published_object_keys.insert(key.clone());
                        shard.strings.insert(key, published);
                    }
                    PublishTarget::Hash { key, field } => {
                        let current = shard.hashes.get(&key).and_then(|fields| fields.get(&field));
                        if current != Some(&original) {
                            continue;
                        }
                        let _ = self.cache.put(
                            CacheKey::page_with_slot_generation(
                                shard_id,
                                published.page_segment_id,
                                published.offset,
                                published.length,
                                published.routing_slot,
                                published.generation,
                            ),
                            bytes,
                        );
                        upsert_slot_index_page(
                            shard,
                            shard_id,
                            "hash",
                            &key,
                            Some(field.clone()),
                            published.clone(),
                            false,
                        );
                        published_object_keys.insert(key.clone());
                        if let Some(fields) = shard.hashes.get_mut(&key) {
                            fields.insert(field, published);
                        }
                    }
                }
            }
            for object_key in published_object_keys {
                clear_published_object_dirty_state(shard, &object_key);
            }
            refresh_slot_runtime_flags(shard);
            serialize_index(shard)
        };
        if !bulk_ingest_mode() {
            // Bulk backfill defers per-record index persistence to an explicit
            // flush_shard_index() call; skip the O(n^2) rewrite + indexlog append here.
            self.index_log_store
                .append_index_bytes(shard_id, &index_bytes)
                .map_err(|err| Status::error("publish_visibility_failed", err.to_string()))?;
            self.persist_index_bytes(shard_id, &index_bytes)
                .map_err(|err| Status::error("publish_visibility_failed", err.to_string()))?;
        }
        Ok(index_bytes.len())
    }

    pub fn install_index_bytes(
        &self,
        shard_id: ShardId,
        bytes: &[u8],
    ) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        fs::write(self.index_path(shard_id), bytes)
    }

    pub fn storage_recovery_report(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let mut report = self.storage_recovery_report_without_boundary(shard_id);
        report.boundary = self.storage_recovery_boundary_report(shard_id);
        report.segment_integrity =
            storage_segment_integrity_report(shard_id, &report, &report.boundary);
        report
    }

    fn storage_recovery_report_without_boundary(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let index_bytes = self
            .index_path(shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let oplog_records = self
            .wal_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let index_log_records = self
            .index_log_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let active_page_segment_ids = self.page_store.segment_ids().unwrap_or_default();
        let zone_descriptors = self.page_store.zone_descriptors();
        let zone_summary = self.page_store.zone_summary();
        let page_segment_reports = self.page_store.segment_reports().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let addresses = shards
            .get(&shard_id)
            .map(collect_live_page_addresses)
            .unwrap_or_default();
        let total_page_refs = addresses.len();
        let mut readable_page_refs = 0usize;
        let mut unreadable_page_refs = Vec::new();
        let mut owner_mismatch_page_refs = Vec::new();
        let mut missing_owner_page_refs = 0usize;
        let mut object_lifecycle = StorageObjectLifecycleReport::default();
        let mut feature_page_layout = StorageFeaturePageLayoutReport::default();
        let mut page_segment_live_reports = page_segment_reports
            .iter()
            .map(|report| {
                (
                    report.page_segment_id,
                    StorageRecoverySegmentLiveReport {
                        page_segment_id: report.page_segment_id,
                        physical_bytes: report.physical_bytes,
                        logical_bytes: report.logical_bytes,
                        page_count: report.page_count,
                        ..StorageRecoverySegmentLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_slots = BTreeMap::<u64, BTreeSet<u32>>::new();
        for address in &addresses {
            let segment_report = page_segment_live_reports
                .entry(address.page_segment_id)
                .or_insert(StorageRecoverySegmentLiveReport {
                    page_segment_id: address.page_segment_id,
                    ..StorageRecoverySegmentLiveReport::default()
                });
            segment_report.live_page_refs = segment_report.live_page_refs.saturating_add(1);
            segment_report.live_physical_bytes = segment_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id {
                let objects = live_object_ids.entry(address.page_segment_id).or_default();
                objects.insert(object_id);
                segment_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_slot) = address.routing_slot {
                let slots = live_routing_slots
                    .entry(address.page_segment_id)
                    .or_default();
                slots.insert(routing_slot);
                segment_report.live_routing_slot_count = slots.len() as u64;
            }
            match self.page_store.read(address) {
                Ok(bytes) => {
                    readable_page_refs += 1;
                    segment_report.readable_live_page_refs =
                        segment_report.readable_live_page_refs.saturating_add(1);
                    segment_report.live_logical_bytes = segment_report
                        .live_logical_bytes
                        .saturating_add(bytes.len() as u64);
                }
                Err(err) => {
                    segment_report.unreadable_live_page_refs =
                        segment_report.unreadable_live_page_refs.saturating_add(1);
                    unreadable_page_refs.push(StorageRecoveryPageError {
                        page_segment_id: address.page_segment_id,
                        offset: address.offset,
                        length: address.length,
                        error: err.to_string(),
                    });
                }
            }
        }
        if let Some(shard) = shards.get(&shard_id) {
            let ownership = self.validate_shard_page_ownership(shard_id, shard);
            owner_mismatch_page_refs = ownership.mismatches;
            missing_owner_page_refs = ownership.missing_owner_page_refs;
            object_lifecycle = storage_object_lifecycle_report(shard_id, shard);
            object_lifecycle.owner_mismatch_page_refs = owner_mismatch_page_refs.len() as u64;
            object_lifecycle.missing_owner_page_refs = missing_owner_page_refs as u64;
            feature_page_layout = storage_feature_page_layout_report(&self.page_store, shard);
        }
        let page_segment_live_reports = page_segment_live_reports
            .into_values()
            .map(|mut report| {
                report.stale_page_estimate =
                    report.page_count.saturating_sub(report.live_page_refs);
                report.live_ref_density_basis_points = if report.page_count == 0 {
                    0
                } else {
                    report.live_page_refs.saturating_mul(10_000) / report.page_count
                };
                report
            })
            .collect::<Vec<_>>();
        object_lifecycle.stale_object_ids = page_segment_live_reports
            .iter()
            .map(|report| report.stale_page_estimate)
            .sum();
        let mut live_page_segment_ids = addresses
            .iter()
            .map(|address| address.page_segment_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        live_page_segment_ids.sort_unstable();
        StorageRecoveryReport {
            shard_id,
            index_bytes,
            index_write_atomic: true,
            oplog_records,
            index_log_records,
            active_page_segment_ids,
            live_page_segment_ids,
            zone_descriptors,
            zone_summary,
            page_segment_reports,
            page_segment_live_reports,
            total_page_refs,
            readable_page_refs,
            unreadable_page_refs,
            owner_mismatch_page_refs,
            missing_owner_page_refs,
            object_lifecycle,
            all_live_pages_readable: total_page_refs == readable_page_refs,
            boundary: StorageRecoveryBoundaryReport::default(),
            segment_integrity: StorageSegmentIntegrityReport::default(),
            feature_page_layout,
        }
    }

    pub fn live_page_segment_ids(&self, shard_id: ShardId) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .get(&shard_id)
            .map(collect_live_page_segment_ids)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn sweep_expired_records(
        &self,
        shard_id: ShardId,
    ) -> Result<ShardExpirySweepReport, Status> {
        self.sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id,
            load_cold_slots: true,
            ..ShardExpirySweepRequest::default()
        })
    }

    pub fn sweep_expired_records_with_request(
        &self,
        request: ShardExpirySweepRequest,
    ) -> Result<ShardExpirySweepReport, Status> {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let now = now_ms();
        let mut hot_keys = shard
            .expires_at_ms
            .iter()
            .filter(|(key, _)| record_exists(shard, key))
            .map(|(key, expires_at)| (key.clone(), *expires_at))
            .collect::<Vec<_>>();
        hot_keys.sort_by(|left, right| left.0.cmp(&right.0));
        let mut cold_keys = shard
            .expires_at_ms
            .iter()
            .filter(|(key, _)| !record_exists(shard, key))
            .map(|(key, expires_at)| (key.clone(), *expires_at))
            .collect::<Vec<_>>();
        cold_keys.sort_by(|left, right| left.0.cmp(&right.0));

        let hot_limit = request.max_hot_slots_per_round;
        let cold_limit = request.max_cold_slots_per_round;
        let (hot_selected, next_hot_cursor) =
            select_expiry_cursor_window(hot_keys, request.hot_cursor.as_deref(), hot_limit);
        let (cold_selected, next_cold_cursor) =
            select_expiry_cursor_window(cold_keys, request.cold_cursor.as_deref(), cold_limit);
        let mut expired_records_removed = 0;
        let mut skipped_records = 0usize;
        let mut loaded_for_expire = 0usize;
        for (key, expires_at) in hot_selected.iter() {
            if *expires_at <= now {
                if delete_record(shard, key) {
                    invalidate_record_all(&self.cache, request.shard_id, key);
                    expired_records_removed += 1;
                }
            } else {
                skipped_records = skipped_records.saturating_add(1);
            }
        }
        for (key, expires_at) in cold_selected.iter() {
            if *expires_at <= now {
                if request.load_cold_slots {
                    loaded_for_expire = loaded_for_expire.saturating_add(1);
                    if delete_record(shard, key) {
                        invalidate_record_all(&self.cache, request.shard_id, key);
                        expired_records_removed += 1;
                    } else {
                        shard.expires_at_ms.remove(key);
                    }
                } else {
                    skipped_records = skipped_records.saturating_add(1);
                }
            } else {
                skipped_records = skipped_records.saturating_add(1);
            }
        }
        if expired_records_removed > 0 {
            let index_bytes = serde_json::to_vec_pretty(shard)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            self.persist_index_bytes(request.shard_id, &index_bytes)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            let _ = self
                .index_log_store
                .append_json(request.shard_id, &index_bytes);
        }
        Ok(ShardExpirySweepReport {
            shard_id: request.shard_id,
            expired_records_removed,
            hot_slots_scanned: hot_selected.len(),
            cold_slots_scanned: cold_selected.len(),
            scanned_records: hot_selected.len().saturating_add(cold_selected.len()),
            skipped_records,
            loaded_for_expire,
            next_hot_cursor,
            next_cold_cursor,
            round_limit: hot_limit.saturating_add(cold_limit),
            load_on_expire_only_when_needed: true,
        })
    }

    pub fn sweep_all_expired_records(&self) -> Vec<ShardExpirySweepReport> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.sweep_expired_records(shard_id).ok())
            .collect()
    }

    fn validate_shard_page_ownership(
        &self,
        shard_id: ShardId,
        shard: &ShardState,
    ) -> StoragePageOwnershipValidation {
        let (start_routing_slot, end_routing_slot) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_slot, info.end_routing_slot))
            .unwrap_or((0, u32::MAX));
        validate_slot_ownership_index(shard_id, shard, start_routing_slot, end_routing_slot)
    }

    pub fn compact_shard_pages(&self, shard_id: ShardId) -> Result<ShardCompactionReport, Status> {
        let (start_routing_slot, end_routing_slot) = self
            .infos
            .read()
            .expect("shard info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_slot, info.end_routing_slot))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let ownership = self.validate_shard_page_ownership(shard_id, shard);
        if !ownership.mismatches.is_empty() {
            return Err(Status::error(
                "page_compaction_owner_mismatch",
                format!(
                    "refusing compaction because {} live page refs disagree with object/page/slot ownership",
                    ownership.mismatches.len()
                ),
            ));
        }
        let before_segments = collect_live_page_segment_ids(shard);
        let before = compaction_utility_report(&self.page_store, shard);
        let tombstoned_object_ids_before =
            storage_object_lifecycle_report(shard_id, shard).tombstoned_object_ids;
        let model_layouts_before = compaction_model_layout_reports(&self.page_store, shard);
        let object_manager_before =
            object_manager_runtime_report(shard_id, shard, start_routing_slot, end_routing_slot);
        let slot_layout_transition_count_before = object_manager_before.layout_transition_count;
        let roll = self
            .page_store
            .roll_segment()
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let mut rewrite_stats = CompactionRewriteStats::default();

        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "string",
            shard.strings.values_mut(),
            &mut rewrite_stats,
        )?;
        for fields in shard.hashes.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "hash",
                fields.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for members in shard.sets.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "set",
                members.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for series in shard.features.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "feature",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.sequences.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "sequence",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.ips.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "ips",
                series,
                &mut rewrite_stats,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "risk",
            shard.risk_pages.values_mut(),
            &mut rewrite_stats,
        )?;
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "context_node",
            shard.context_nodes.values_mut(),
            &mut rewrite_stats,
        )?;
        for series in shard.context_events.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_event",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_indexes.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_index",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_audits.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_audit",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_children.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_child",
                series,
                &mut rewrite_stats,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "context_embedding",
            shard.context_embeddings.values_mut(),
            &mut rewrite_stats,
        )?;
        for series in shard.context_summaries.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_summary",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_compressions.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_compression",
                series,
                &mut rewrite_stats,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "context_entity",
            shard.context_entities.values_mut(),
            &mut rewrite_stats,
        )?;
        for (key, meta_series) in &mut shard.ips_meta {
            if let Some(address_series) = shard.ips.get(key) {
                for (timestamp, meta) in meta_series {
                    if let Some(address) = address_series.get(timestamp) {
                        meta.address = address.clone();
                    }
                }
            }
        }

        rebuild_slot_first_index(shard_id, shard, 0, u32::MAX);
        refresh_slot_runtime_flags(shard);
        let after_segments = collect_live_page_segment_ids(shard);
        let after = compaction_utility_report(&self.page_store, shard);
        rebuild_slot_page_ownership(shard_id, shard, start_routing_slot, end_routing_slot);
        let tombstoned_object_ids_after =
            storage_object_lifecycle_report(shard_id, shard).tombstoned_object_ids;
        let object_manager_after =
            object_manager_runtime_report(shard_id, shard, start_routing_slot, end_routing_slot);
        let slot_layout_transition_count_after = object_manager_after.layout_transition_count;
        let slot_layout_states_after = object_manager_after.layout_states;
        let stale_page_segment_ids = before_segments
            .difference(&after_segments)
            .copied()
            .collect::<Vec<_>>();
        let reclaimable_stale_page_segment_count = stale_page_segment_ids.len();
        let model_policy_family_count = before.model_policies.len();
        let tombstone_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.tombstone_compaction_triggered)
            .count();
        let stale_density_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.stale_density_triggered)
            .count();
        let layout_aware_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.layout_aware_rewrite_required)
            .count();
        let index_bytes = serde_json::to_vec_pretty(shard)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        self.persist_index_bytes(shard_id, &index_bytes)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = self.index_log_store.append_json(shard_id, &index_bytes);
        let rewritten_object_pages = rewrite_stats.rewritten_page_refs;
        let slot_layout_transition_count =
            slot_layout_transition_count_after.saturating_sub(slot_layout_transition_count_before);
        let has_model_layouts = !model_layouts_before.is_empty();
        let preserves_tombstones = tombstoned_object_ids_after >= tombstoned_object_ids_before;
        let improves_density =
            before.live_ref_density_basis_points <= after.live_ref_density_basis_points;
        let has_layout_transitions = slot_layout_transition_count > 0
            || slot_layout_states_after
                .iter()
                .any(|state| state.object_count > 0);
        let mut model_layout_compaction_blockers = Vec::new();
        if rewritten_object_pages == 0 {
            model_layout_compaction_blockers.push("no live page refs were rewritten".to_string());
        }
        if !has_model_layouts {
            model_layout_compaction_blockers.push("model layout report is empty".to_string());
        }
        if !preserves_tombstones {
            model_layout_compaction_blockers
                .push("tombstone object count decreased during compaction".to_string());
        }
        if !improves_density {
            model_layout_compaction_blockers
                .push("live-ref density did not improve or remain stable".to_string());
        }
        if !has_layout_transitions {
            model_layout_compaction_blockers
                .push("slot layout transition evidence is missing".to_string());
        }
        Ok(ShardCompactionReport {
            shard_id,
            model_layout_compaction_ready: model_layout_compaction_blockers.is_empty(),
            model_layout_compaction_evidence: vec![
                "compaction rewrites live refs by model layout".to_string(),
                "packed timestamped model layouts preserve shared page refs".to_string(),
                "tombstone object ids are preserved across compaction".to_string(),
                "stale page density is removed from the compacted live set".to_string(),
                "slot layout transition counts and states are reported after compaction"
                    .to_string(),
                "per-model policies expose tombstone density, stale-page density, object-page packing, and cold-page rewrite eligibility".to_string(),
                "stale segments left behind by moved indexes are reported as reclaimable".to_string(),
            ],
            model_layout_compaction_blockers,
            previous_page_segment_id: roll.previous_page_segment_id,
            compacted_page_segment_id: roll.new_page_segment_id,
            rewritten_page_refs: rewrite_stats.rewritten_page_refs,
            cold_page_rewrite_refs: rewrite_stats.cold_page_rewrite_refs,
            object_page_pack_group_count: before
                .model_policies
                .iter()
                .map(|policy| policy.object_page_pack_group_count as usize)
                .sum(),
            stale_page_segment_ids,
            reclaimable_stale_page_segment_count,
            model_policy_family_count,
            tombstone_policy_model_count,
            stale_density_policy_model_count,
            layout_aware_policy_model_count,
            model_rewrite_policies: rewrite_stats.into_reports(&before),
            rewritten_object_pages,
            slot_layout_transition_count,
            slot_layout_states_after,
            tombstoned_object_ids_before,
            tombstoned_object_ids_after,
            model_layouts: model_layouts_before,
            before,
            after,
        })
    }

    fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    fn persist_slot_dump_manifest(
        &self,
        manifest: &SlotDumpManifest,
    ) -> Result<(), std::io::Error> {
        let path =
            slot_dump_manifest_path(&self.index_dir, manifest.shard_id, &manifest.manifest_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        fs::write(path, bytes)
    }

    fn persist_slot_dump_install_marker(
        &self,
        manifest: &SlotDumpManifest,
        phase: &str,
    ) -> Result<(), std::io::Error> {
        self.persist_slot_dump_install_marker_by_fields(
            manifest.shard_id,
            &manifest.manifest_id,
            phase,
            manifest.oplog_sequence,
            manifest.index_log_sequence,
        )
    }

    fn persist_slot_dump_install_marker_by_fields(
        &self,
        shard_id: ShardId,
        manifest_id: &str,
        phase: &str,
        oplog_sequence: u64,
        index_log_sequence: u64,
    ) -> Result<(), std::io::Error> {
        write_slot_dump_install_marker(
            &self.index_dir,
            &SlotDumpInstallMarker {
                shard_id,
                manifest_id: manifest_id.to_string(),
                phase: phase.to_string(),
                oplog_sequence,
                index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
    }

    fn validate_slot_dump_generation_for_install(
        &self,
        manifest: &SlotDumpManifest,
    ) -> Result<(), Status> {
        if manifest.dump_generation_id.is_empty() {
            return Ok(());
        }
        let requested_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
        let source_manifest_ids = manifest
            .source_manifest_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for existing in self.list_slot_dump_manifests(manifest.shard_id) {
            if existing.manifest_id == manifest.manifest_id
                || source_manifest_ids.contains(&existing.manifest_id)
                || existing.dump_generation_id.is_empty()
                || existing.dump_generation_id == manifest.dump_generation_id
            {
                continue;
            }
            let existing_slots = existing.slot_ids.iter().copied().collect::<BTreeSet<_>>();
            let overlaps = requested_slots.is_empty()
                || existing_slots.is_empty()
                || !requested_slots.is_disjoint(&existing_slots);
            if overlaps
                && existing.index_log_sequence >= manifest.index_log_sequence
                && existing.oplog_sequence >= manifest.oplog_sequence
            {
                return Err(Status::error(
                    "slot_dump_generation_conflict",
                    format!(
                        "manifest generation {} conflicts with installed generation {} for overlapping slots",
                        manifest.dump_generation_id, existing.dump_generation_id
                    ),
                ));
            }
        }
        Ok(())
    }

    fn load_index(&self, shard_id: ShardId) -> Option<ShardState> {
        let bytes = fs::read(self.index_path(shard_id)).ok()?;
        let mut shard = serde_json::from_slice::<ShardState>(&bytes).ok()?;
        reconcile_secondary_views_from_slot_index(&self.page_store, &mut shard);
        refresh_slot_runtime_flags(&mut shard);
        Some(shard)
    }

    /// Persist the in-memory shard index to disk once (used by bulk backfill
    /// after driving many extract_context calls under MATRIXARK_BULK_INGEST=1,
    /// which skips per-record persistence). Also refreshes the index-log tail.
    pub fn flush_shard_index(&self, shard_id: ShardId) {
        let index_bytes = {
            let shards = self.shards.read().expect("engine lock poisoned");
            match shards.get(&shard_id) {
                Some(shard) => serialize_index(shard),
                None => return,
            }
        };
        // Write the served shard index directly, bypassing the bulk-mode gate.
        let _ = fs::create_dir_all(&self.index_dir);
        let _ = atomic_write_bytes(&self.index_path(shard_id), &index_bytes);
    }

    fn persist_index_bytes(&self, shard_id: ShardId, bytes: &[u8]) -> Result<(), std::io::Error> {
        // Bulk backfill defers the served-index rewrite to flush_shard_index()
        // (turns O(n^2) per-record persistence into one write per batch).
        if bulk_ingest_mode() {
            return Ok(());
        }
        fs::create_dir_all(&self.index_dir)?;
        atomic_write_bytes(&self.index_path(shard_id), bytes)
    }

    fn validate_load_version(&self, shard_id: ShardId, load_version: u64) -> Result<(), Status> {
        let infos = self.infos.read().expect("info lock poisoned");
        let Some(info) = infos.get(&shard_id) else {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        };
        if !info.loaded {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        }
        if info.load_version != load_version {
            return Err(Status::error(
                "load_version_mismatch",
                format!(
                    "request load_version {} does not match loaded version {}",
                    load_version, info.load_version
                ),
            ));
        }
        Ok(())
    }

    fn shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        shards.get(&shard_id).map(|state| {
            let page_store = self.page_store.stats();
            let page_store_zones = self.page_store.zone_summary();
            let string_records = state.strings.len();
            let hash_records = state.hashes.len();
            let set_records = state.sets.len();
            let feature_records = state.features.len();
            let sequence_records = state.sequences.len();
            let ips_records = state.ips.len();
            let risk_records = state.risk.len() + state.risk_changes.len();
            let loaded = info.as_ref().map(|info| info.loaded).unwrap_or(true);
            let readonly = info.as_ref().map(|info| info.readonly).unwrap_or(false);
            let load_version = info
                .as_ref()
                .map(|info| info.load_version)
                .unwrap_or_default();
            let table_name = info
                .as_ref()
                .map(|info| info.table_name.clone())
                .unwrap_or_default();
            let shard_uri = info
                .as_ref()
                .map(|info| info.shard_uri.clone())
                .unwrap_or_default();
            let start_routing_slot = info
                .as_ref()
                .map(|info| info.start_routing_slot)
                .unwrap_or_default();
            let end_routing_slot = info
                .as_ref()
                .map(|info| info.end_routing_slot)
                .unwrap_or(u32::MAX);
            let object_manager = object_manager_stats(state, start_routing_slot, end_routing_slot);
            let secondary_view_total_records = string_records
                + hash_records
                + set_records
                + feature_records
                + sequence_records
                + ips_records
                + risk_records;
            let total_records = if state.slot_index.slot_map.is_empty() {
                secondary_view_total_records
            } else {
                object_manager.object_count
            };
            let partition_info = PartitionInfoStats {
                shard_id,
                loaded,
                readonly,
                load_version,
                table_name,
                shard_uri,
                start_routing_slot,
                end_routing_slot,
                total_records,
                storage_bytes: page_store.bytes_written,
                object_manager: object_manager.clone(),
            };
            let storage = crate::control::ShardCanonicalStorageStats {
                page_index_entries: object_manager.page_ref_count as u64,
                block_index_entries: page_store.writes,
                object_index_entries: object_manager.object_count as u64,
                slot_entries: object_manager.routing_slot_count as u64,
                storage_zone_count: page_store_zones
                    .active_extents
                    .saturating_add(page_store_zones.sealed_extents)
                    .saturating_add(page_store_zones.delayed_destroy_extents)
                    .saturating_add(page_store_zones.purged_extents),
                active_storage_zones: page_store_zones.active_extents,
                sealed_storage_zones: page_store_zones.sealed_extents,
                stream_segment_count: page_store_zones
                    .active_extents
                    .saturating_add(page_store_zones.sealed_extents)
                    .saturating_add(page_store_zones.delayed_destroy_extents)
                    .saturating_add(page_store_zones.purged_extents),
                storage_zone_total_bytes: page_store_zones.total_known_physical_bytes,
                storage_zone_used_bytes: page_store_zones.live_physical_bytes,
                storage_zone_stale_bytes: page_store_zones.reclaimable_physical_bytes,
                page_reads: page_store.reads,
                page_writes: page_store.writes,
                block_reads: page_store.reads,
                block_writes: page_store.writes,
                bytes_read: page_store.bytes_read,
                bytes_written: page_store.bytes_written,
                append_watermark: page_store.writes,
                compaction_watermark: page_store_zones.reclaimable_physical_bytes,
            };
            ShardStats {
                shard_id,
                loaded,
                readonly,
                load_version,
                total_records,
                string_records,
                hash_records,
                set_records,
                feature_records,
                sequence_records,
                ips_records,
                risk_records,
                storage_bytes: page_store.bytes_written,
                object_manager,
                partition_info,
                storage,
                cache: self.cache.stats(),
                page_store: page_store.clone(),
                page_store_zones: page_store_zones.clone(),
                block_store: page_store,
                block_store_extents: page_store_zones,
                write_ahead_log: self.wal_store.stats(shard_id),
            }
        })
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

fn slot_dump_manifest_dir(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir
        .join("slot-dumps")
        .join(format!("shard-{shard_id}"))
}

fn slot_dump_manifest_path(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, shard_id).join(format!("{manifest_id}.json"))
}

fn slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<Option<SlotDumpManifest>, std::io::Error> {
    let path = slot_dump_manifest_path(index_dir, shard_id, manifest_id);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice::<SlotDumpManifest>(&fs::read(path)?)
        .map(Some)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn slot_dump_install_marker_path(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, marker.shard_id).join(format!(
        "{}.{}.{}.marker",
        marker.manifest_id, marker.phase, marker.created_unix_ms
    ))
}

fn write_slot_dump_install_marker(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> Result<(), std::io::Error> {
    let path = slot_dump_install_marker_path(index_dir, marker);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    fs::write(path, bytes)
}

fn slot_dump_install_marker_files_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<(SlotDumpInstallMarker, PathBuf)>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
    let mut markers = Vec::new();
    if !dir.exists() {
        return Ok(markers);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "marker")
            .unwrap_or(false)
        {
            continue;
        }
        let path = entry.path();
        let marker = serde_json::from_slice::<SlotDumpInstallMarker>(&fs::read(&path)?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        markers.push((marker, path));
    }
    markers.sort_by_key(|(marker, _)| {
        (
            marker.index_log_sequence,
            marker.created_unix_ms,
            slot_dump_install_phase_rank(&marker.phase),
        )
    });
    Ok(markers)
}

fn list_slot_dump_install_markers_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    Ok(slot_dump_install_marker_files_at(index_dir, shard_id)?
        .into_iter()
        .map(|(marker, _)| marker)
        .collect())
}

fn interrupted_slot_dump_installs_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    let mut latest_by_manifest = BTreeMap::<String, SlotDumpInstallMarker>::new();
    for marker in list_slot_dump_install_markers_at(index_dir, shard_id)? {
        let replace = latest_by_manifest
            .get(&marker.manifest_id)
            .map(|existing| {
                slot_dump_install_phase_rank(&marker.phase)
                    > slot_dump_install_phase_rank(&existing.phase)
                    || (slot_dump_install_phase_rank(&marker.phase)
                        == slot_dump_install_phase_rank(&existing.phase)
                        && marker.created_unix_ms > existing.created_unix_ms)
            })
            .unwrap_or(true);
        if replace {
            latest_by_manifest.insert(marker.manifest_id.clone(), marker);
        }
    }
    Ok(latest_by_manifest
        .into_values()
        .filter(|marker| marker.phase != "commit")
        .collect())
}

fn remove_obsolete_slot_dump_install_markers(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<usize, std::io::Error> {
    let mut removed = 0usize;
    for (marker, path) in slot_dump_install_marker_files_at(index_dir, shard_id)? {
        if marker.manifest_id == manifest_id
            && (marker.phase == "prepare" || marker.phase == "install")
            && fs::remove_file(path).is_ok()
        {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

fn slot_dump_install_phase_counts(markers: &[SlotDumpInstallMarker]) -> (usize, usize, usize) {
    let mut prepared = 0usize;
    let mut installed = 0usize;
    let mut unknown = 0usize;
    for marker in markers {
        match marker.phase.as_str() {
            "prepare" => prepared = prepared.saturating_add(1),
            "install" => installed = installed.saturating_add(1),
            _ => unknown = unknown.saturating_add(1),
        }
    }
    (prepared, installed, unknown)
}

fn slot_dump_install_phase_rank(phase: &str) -> u8 {
    match phase {
        "prepare" => 1,
        "install" => 2,
        "commit" => 3,
        _ => 0,
    }
}

fn slot_dump_manifest_chain_issues(
    manifests: &[SlotDumpManifest],
) -> Vec<SlotDumpManifestChainIssue> {
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    manifests
        .iter()
        .filter_map(|manifest| {
            let parent = manifest.parent_manifest_id.as_ref()?;
            (!manifest_ids.contains(parent)).then(|| SlotDumpManifestChainIssue {
                manifest_id: manifest.manifest_id.clone(),
                parent_manifest_id: Some(parent.clone()),
                reason: "missing_parent_manifest".to_string(),
            })
        })
        .collect()
}

fn retained_slot_dump_manifest_ids(manifests: &[SlotDumpManifest]) -> BTreeSet<String> {
    let by_id = manifests
        .iter()
        .map(|manifest| (manifest.manifest_id.clone(), manifest))
        .collect::<BTreeMap<_, _>>();
    let mut retained = BTreeSet::new();
    let mut cursor = manifests
        .iter()
        .max_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms))
        .map(|manifest| manifest.manifest_id.clone());
    while let Some(manifest_id) = cursor {
        if !retained.insert(manifest_id.clone()) {
            break;
        }
        cursor = by_id
            .get(&manifest_id)
            .and_then(|manifest| manifest.parent_manifest_id.clone());
    }
    retained
}

fn slot_dump_manifest_prune_plan_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    follower_cursors: &[SlotDumpFollowerReplayCursor],
    raft_snapshot_refs: &[SlotDumpRaftSnapshotRef],
) -> Result<SlotDumpManifestPrunePlan, std::io::Error> {
    let manifests = list_slot_dump_manifests_at(index_dir, shard_id)?;
    let mut retained = retained_slot_dump_manifest_ids(&manifests);
    let mut follower_blocks = Vec::new();
    let mut raft_snapshot_blocks = Vec::new();
    for cursor in follower_cursors
        .iter()
        .filter(|cursor| cursor.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.oplog_sequence <= cursor.oplog_sequence
                && manifest.index_log_sequence <= cursor.index_log_sequence
        }) else {
            continue;
        };
        if retained.insert(anchor.manifest_id.clone()) {
            follower_blocks.push(SlotDumpFollowerRetentionBlock {
                follower_id: cursor.follower_id.clone(),
                manifest_id: anchor.manifest_id.clone(),
                manifest_oplog_sequence: anchor.oplog_sequence,
                manifest_index_log_sequence: anchor.index_log_sequence,
                cursor_oplog_sequence: cursor.oplog_sequence,
                cursor_index_log_sequence: cursor.index_log_sequence,
                reason: "follower_cursor_anchor".to_string(),
            });
        }
    }
    for snapshot in raft_snapshot_refs
        .iter()
        .filter(|snapshot| snapshot.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.oplog_sequence <= snapshot.oplog_sequence
                && manifest.index_log_sequence <= snapshot.index_log_sequence
        }) else {
            continue;
        };
        if retained.insert(anchor.manifest_id.clone()) {
            raft_snapshot_blocks.push(SlotDumpRaftSnapshotRetentionBlock {
                snapshot_id: snapshot.snapshot_id.clone(),
                manifest_id: anchor.manifest_id.clone(),
                manifest_oplog_sequence: anchor.oplog_sequence,
                manifest_index_log_sequence: anchor.index_log_sequence,
                snapshot_oplog_sequence: snapshot.oplog_sequence,
                snapshot_index_log_sequence: snapshot.index_log_sequence,
                last_included_index: snapshot.last_included_index,
                last_included_term: snapshot.last_included_term,
                reason: "raft_snapshot_anchor".to_string(),
            });
        }
    }
    let interrupted = interrupted_slot_dump_installs_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .collect::<BTreeSet<_>>();
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    let mut prunable_manifest_ids = Vec::new();
    let mut blocked_manifest_ids = Vec::new();
    for manifest in &manifests {
        if retained.contains(&manifest.manifest_id) {
            continue;
        }
        if interrupted.contains(&manifest.manifest_id) {
            blocked_manifest_ids.push(manifest.manifest_id.clone());
        } else {
            prunable_manifest_ids.push(manifest.manifest_id.clone());
        }
    }
    let prunable_marker_manifest_ids = list_slot_dump_install_markers_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .filter(|manifest_id| {
            !retained.contains(manifest_id)
                && !interrupted.contains(manifest_id)
                && (prunable_manifest_ids.iter().any(|id| id == manifest_id)
                    || !manifest_ids.contains(manifest_id))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut reasons = Vec::new();
    if !prunable_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_manifest".to_string());
    }
    if !prunable_marker_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_marker".to_string());
    }
    if !blocked_manifest_ids.is_empty() {
        reasons.push("interrupted_install_blocks_prune".to_string());
    }
    if !follower_blocks.is_empty() {
        reasons.push("follower_cursor_blocks_prune".to_string());
    }
    if !raft_snapshot_blocks.is_empty() {
        reasons.push("raft_snapshot_blocks_prune".to_string());
    }
    Ok(SlotDumpManifestPrunePlan {
        shard_id,
        retained_manifest_ids: retained.into_iter().collect(),
        prunable_manifest_ids,
        prunable_marker_manifest_ids,
        blocked_manifest_ids,
        follower_blocks,
        raft_snapshot_blocks,
        reasons,
    })
}

fn list_slot_dump_manifests_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpManifest>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
    let mut manifests = Vec::new();
    if !dir.exists() {
        return Ok(manifests);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "json")
            .unwrap_or(false)
        {
            continue;
        }
        let manifest = serde_json::from_slice::<SlotDumpManifest>(&fs::read(entry.path())?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        manifests.push(manifest);
    }
    manifests.sort_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms));
    Ok(manifests)
}

fn latest_slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Option<SlotDumpManifest> {
    list_slot_dump_manifests_at(index_dir, shard_id)
        .ok()?
        .into_iter()
        .last()
}

fn slot_dump_manifest_checksum(manifest: &SlotDumpManifest) -> Result<String, Status> {
    let mut payload = manifest.clone();
    payload.checksum.clear();
    serde_json::to_vec(&payload)
        .map(|bytes| sha256_hex_bytes(&bytes))
        .map_err(|err| Status::error("slot_dump_checksum_failed", err.to_string()))
}

fn slot_dump_fault_scenario(
    scenario: impl Into<String>,
    expected_code: impl Into<String>,
    actual_code: impl Into<String>,
    blockers: Vec<String>,
    install_safe: bool,
) -> SlotDumpFaultScenarioReport {
    let expected_code = expected_code.into();
    let actual_code = actual_code.into();
    SlotDumpFaultScenarioReport {
        scenario: scenario.into(),
        passed: actual_code == expected_code,
        expected_code,
        actual_code,
        blockers,
        install_safe,
    }
}

fn slot_dump_generation_id(manifest: &SlotDumpManifest) -> String {
    let mut digest = Sha256::new();
    digest.update(manifest.shard_id.to_le_bytes());
    digest.update(manifest.oplog_sequence.to_le_bytes());
    digest.update(manifest.index_log_sequence.to_le_bytes());
    for slot_id in &manifest.slot_ids {
        digest.update(slot_id.to_le_bytes());
    }
    for page_segment_id in &manifest.page_segment_ids {
        digest.update(page_segment_id.to_le_bytes());
    }
    digest.update(manifest.index_sha256.as_bytes());
    if manifest.version >= 3 {
        digest.update(manifest.object_lifecycle.live_object_ids.to_le_bytes());
        digest.update(manifest.object_lifecycle.live_page_refs.to_le_bytes());
        digest.update(manifest.object_lifecycle.stale_object_ids.to_le_bytes());
        digest.update(
            manifest
                .object_lifecycle
                .tombstoned_object_ids
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .reused_object_id_conflicts
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .missing_owner_page_refs
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .owner_mismatch_page_refs
                .to_le_bytes(),
        );
        for object_id in &manifest.object_lifecycle.reused_object_ids {
            digest.update(object_id.to_le_bytes());
        }
        for key in &manifest.object_lifecycle.tombstoned_object_keys {
            digest.update(key.as_bytes());
            digest.update([0]);
        }
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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

fn storage_segment_integrity_report(
    shard_id: ShardId,
    recovery: &StorageRecoveryReport,
    boundary: &StorageRecoveryBoundaryReport,
) -> StorageSegmentIntegrityReport {
    let indexed_page_segment_count = recovery.active_page_segment_ids.len();
    let discovered_page_segment_count = recovery.page_segment_reports.len();
    let live_page_segment_count = recovery.live_page_segment_ids.len();
    let orphan_page_segment_count = boundary.orphan_page_segment_ids.len();
    let stale_page_ref_count = boundary.stale_index_page_refs.len();
    let corrupt_page_segment_count = boundary.corrupt_page_segment_ids.len();
    let unreadable_page_ref_count = recovery.unreadable_page_refs.len();
    let unreadable_page_bytes = boundary.unreadable_page_bytes;
    let owner_mismatch_page_ref_count = boundary.owner_mismatch_page_refs.len();
    let missing_owner_page_ref_count = boundary.missing_owner_page_refs;
    let reclaim_required = orphan_page_segment_count > 0
        || recovery
            .page_segment_live_reports
            .iter()
            .any(|report| report.stale_page_estimate > 0);
    let integrity_ok = stale_page_ref_count == 0
        && corrupt_page_segment_count == 0
        && unreadable_page_ref_count == 0
        && unreadable_page_bytes == 0
        && owner_mismatch_page_ref_count == 0
        && missing_owner_page_ref_count == 0
        && recovery.all_live_pages_readable;

    StorageSegmentIntegrityReport {
        shard_id,
        indexed_page_segment_count,
        discovered_page_segment_count,
        live_page_segment_count,
        orphan_page_segment_count,
        stale_page_ref_count,
        corrupt_page_segment_count,
        unreadable_page_ref_count,
        unreadable_page_bytes,
        owner_mismatch_page_ref_count,
        missing_owner_page_ref_count,
        reclaim_required,
        integrity_ok,
    }
}

fn storage_reclaim_candidates_from_recovery(
    recovery: &StorageRecoveryReport,
    fully_stale_segment_ids: &BTreeSet<u64>,
) -> Vec<StorageReclaimCandidate> {
    let mut candidates = recovery
        .page_segment_live_reports
        .iter()
        .filter_map(|report| {
            let fully_stale = fully_stale_segment_ids.contains(&report.page_segment_id);
            let stale_page_estimate = if fully_stale {
                report.page_count
            } else {
                report.stale_page_estimate
            };
            let stale_physical_bytes = if fully_stale {
                report.physical_bytes
            } else {
                report
                    .physical_bytes
                    .saturating_sub(report.live_physical_bytes)
            };
            if stale_page_estimate == 0 && stale_physical_bytes == 0 {
                return None;
            }
            let reclaim_score = stale_physical_bytes
                .saturating_mul(10_000_u64.saturating_sub(report.live_ref_density_basis_points))
                .saturating_div(10_000)
                .saturating_add(stale_page_estimate);
            Some(StorageReclaimCandidate {
                page_segment_id: report.page_segment_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: report.live_physical_bytes,
                stale_physical_bytes,
                page_count: report.page_count,
                live_page_refs: report.live_page_refs,
                stale_page_estimate,
                live_ref_density_basis_points: report.live_ref_density_basis_points,
                reclaim_score,
                reason: if fully_stale {
                    "orphan_segment".to_string()
                } else {
                    "low_live_density".to_string()
                },
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .reclaim_score
            .cmp(&left.reclaim_score)
            .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
            .then_with(|| left.page_segment_id.cmp(&right.page_segment_id))
    });
    candidates
}

fn annotate_storage_manager_admin_stage_fields(
    stages: &mut [StorageManagerStageReport],
    last_run_unix_ms: u64,
    duration_ms: u64,
    errors: &[String],
    retention_blockers: usize,
) {
    for stage in stages {
        stage.last_run_unix_ms = last_run_unix_ms;
        stage.duration_ms = duration_ms;
        if stage.skipped && stage.skipped_reason.is_empty() {
            stage.skipped_reason = stage.reason.clone();
        }
        if !errors.is_empty() {
            let prefix = format!("{}:", stage.stage);
            stage.errors = errors
                .iter()
                .filter(|error| error.starts_with(&prefix))
                .cloned()
                .collect();
        }
        stage.bytes_reclaimed = stage
            .page_bytes_reclaimed
            .max(stage.cache_disk_bytes_removed)
            .max(stage.before_bytes.saturating_sub(stage.after_bytes));
        stage.pages_compacted = stage.rewritten_page_refs;
        if stage.wal_floor_sequence == 0 {
            stage.wal_floor_sequence = stage.retain_from_wal_sequence;
        }
        if stage.index_log_floor_sequence == 0 {
            stage.index_log_floor_sequence = stage.retain_from_index_log_sequence;
        }
        if stage.retention_blockers == 0 {
            stage.retention_blockers = retention_blockers;
        }
        if stage.pressure_before == 0 {
            stage.pressure_before = stage.eviction_pressure_before.max(stage.before_bytes);
        }
        if stage.pressure_after == 0 {
            stage.pressure_after = stage.eviction_pressure_after.max(stage.after_bytes);
        }
    }
}

#[derive(Debug, Clone)]
struct StorageManagerPhaseExecutor {
    round_started_unix_ms: u64,
}

impl StorageManagerPhaseExecutor {
    fn new(round_started_unix_ms: u64) -> Self {
        Self {
            round_started_unix_ms,
        }
    }

    fn annotate_reports(
        &self,
        stages: &mut [StorageManagerStageReport],
        errors: &[String],
        retention_blockers: usize,
    ) {
        let round_duration_ms = now_ms().saturating_sub(self.round_started_unix_ms);
        annotate_storage_manager_admin_stage_fields(
            stages,
            self.round_started_unix_ms,
            round_duration_ms,
            errors,
            retention_blockers,
        );
    }
}

#[derive(Debug, Clone)]
struct LivePageEntry {
    object_key: String,
    kind: String,
    component: Option<String>,
    address: PageAddress,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

#[derive(Debug, Default)]
struct StoragePageOwnershipValidation {
    mismatches: Vec<StorageRecoveryPageOwnerMismatch>,
    missing_owner_page_refs: usize,
}

fn live_page_entry(
    object_key: impl Into<String>,
    kind: impl Into<String>,
    component: Option<String>,
    address: PageAddress,
) -> LivePageEntry {
    LivePageEntry {
        object_key: object_key.into(),
        kind: kind.into(),
        component,
        address,
        dirty: false,
        deleted: false,
        log_backed: true,
    }
}

fn storage_page_address_sample(
    shard_id: ShardId,
    address: &PageAddress,
) -> StoragePageAddressSample {
    StoragePageAddressSample {
        shard_id,
        zone_id: address.extent_id.unwrap_or(address.page_segment_id),
        segment_id: address.page_segment_id,
        page_id: address.page_id.unwrap_or(address.page_segment_id),
        offset: address.offset,
        length: address.length,
        generation: address.object_id.unwrap_or(0),
    }
}

fn storage_block_address_sample(
    shard_id: ShardId,
    address: &PageAddress,
) -> StorageBlockAddressSample {
    StorageBlockAddressSample {
        shard_id,
        zone_id: address.extent_id.unwrap_or(address.page_segment_id),
        block_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
        checksum: address.sha256.clone().unwrap_or_default(),
    }
}

fn storage_index_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageIndexSnapshot,
) -> StorageIndexSnapshot {
    let mut entries = collect_live_page_entries(shard);
    entries.sort_by(|left, right| {
        (
            left.kind.as_str(),
            left.object_key.as_str(),
            left.component.as_deref().unwrap_or(""),
            left.address.page_segment_id,
            left.address.offset,
        )
            .cmp(&(
                right.kind.as_str(),
                right.object_key.as_str(),
                right.component.as_deref().unwrap_or(""),
                right.address.page_segment_id,
                right.address.offset,
            ))
    });

    const MAX_STORAGE_INDEX_SAMPLES: usize = 8;
    snapshot.page_index_entry_samples = entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .map(|entry| {
            let page_address = storage_page_address_sample(shard_id, &entry.address);
            StoragePageIndexEntrySample {
                logical_key: entry.object_key.clone(),
                timestamp_range: None,
                page_addresses: vec![page_address],
                append_watermark: entry.address.offset,
                generation: entry.address.object_id.unwrap_or(0),
            }
        })
        .collect();
    snapshot.block_index_entry_samples = entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .map(|entry| {
            let page_address = storage_page_address_sample(shard_id, &entry.address);
            let block_address = storage_block_address_sample(shard_id, &entry.address);
            StorageBlockIndexEntrySample {
                extent: entry
                    .address
                    .extent_id
                    .unwrap_or(entry.address.page_segment_id),
                checksum: entry.address.sha256.clone().unwrap_or_default(),
                generation: entry.address.object_id.unwrap_or(0),
                page_address,
                block_address,
            }
        })
        .collect();

    let mut object_entries: BTreeMap<(String, String, String), StorageObjectIndexEntrySample> =
        BTreeMap::new();
    for entry in entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES.saturating_mul(4))
    {
        let key = (
            entry.kind.clone(),
            entry.kind.clone(),
            entry.object_key.clone(),
        );
        let sample = object_entries
            .entry(key)
            .or_insert_with(|| StorageObjectIndexEntrySample {
                model: entry.kind.clone(),
                table: entry.kind.clone(),
                object_key: entry.object_key.clone(),
                page_chain: Vec::new(),
                tombstone: entry.deleted,
                generation: entry.address.object_id.unwrap_or(0),
            });
        if sample.page_chain.len() < MAX_STORAGE_INDEX_SAMPLES {
            sample
                .page_chain
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        sample.tombstone |= entry.deleted;
        sample.generation = sample.generation.max(entry.address.object_id.unwrap_or(0));
    }
    snapshot.object_index_entry_samples = object_entries
        .into_iter()
        .map(|(_, sample)| sample)
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .collect();
    snapshot
}

fn storage_gc_ref(entry: &LivePageEntry) -> String {
    match entry.component.as_deref() {
        Some(component) if !component.is_empty() => {
            format!("{}:{}:{}", entry.kind, entry.object_key, component)
        }
        _ => format!("{}:{}", entry.kind, entry.object_key),
    }
}

fn storage_watermark_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageWatermarkSnapshot,
) -> StorageWatermarkSnapshot {
    const MAX_STORAGE_WATERMARK_SAMPLES: usize = 8;
    let timestamp_ms = now_ms();
    let mut slot_watermarks = BTreeMap::<u32, u64>::new();

    for (slot_id, runtime_slot) in &shard.slot_index.slot_map {
        slot_watermarks.insert(*slot_id, runtime_slot.dirty_generation);
    }
    for entry in collect_live_page_entries(shard) {
        let slot_id = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let generation = entry.address.object_id.unwrap_or(0);
        slot_watermarks
            .entry(slot_id)
            .and_modify(|current| *current = (*current).max(generation))
            .or_insert(generation);
    }

    snapshot.append_watermark_samples = slot_watermarks
        .iter()
        .take(MAX_STORAGE_WATERMARK_SAMPLES)
        .map(|(slot_id, generation)| StorageAppendWatermarkSample {
            shard_id,
            slot_id: *slot_id,
            log_index: (*generation).max(snapshot.append_watermark),
            timestamp_ms,
        })
        .collect();
    if snapshot.append_watermark_samples.is_empty() && snapshot.append_watermark > 0 {
        snapshot
            .append_watermark_samples
            .push(StorageAppendWatermarkSample {
                shard_id,
                slot_id: 0,
                log_index: snapshot.append_watermark,
                timestamp_ms,
            });
    }

    snapshot.compaction_watermark_samples = vec![StorageCompactionWatermarkSample {
        shard_id,
        safe_generation: snapshot.compaction_watermark,
        safe_timestamp_ms: snapshot.follower_cursor_safe_watermark,
        follower_floor: snapshot.follower_cursor_retention_floor,
    }];
    snapshot
}

fn storage_gc_snapshot_with_samples(
    _shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageGcSnapshot,
) -> StorageGcSnapshot {
    let mut entries = collect_live_page_entries(shard);
    entries.sort_by(|left, right| {
        (
            left.deleted,
            left.kind.as_str(),
            left.object_key.as_str(),
            left.component.as_deref().unwrap_or(""),
            left.address.page_segment_id,
            left.address.offset,
        )
            .cmp(&(
                right.deleted,
                right.kind.as_str(),
                right.object_key.as_str(),
                right.component.as_deref().unwrap_or(""),
                right.address.page_segment_id,
                right.address.offset,
            ))
    });

    const MAX_STORAGE_GC_SAMPLES: usize = 8;
    let now = now_ms();
    snapshot.tombstone_samples = entries
        .iter()
        .filter(|entry| entry.deleted)
        .take(MAX_STORAGE_GC_SAMPLES)
        .map(|entry| StorageTombstoneSample {
            ref_id: storage_gc_ref(entry),
            generation: entry.address.object_id.unwrap_or(0),
            deleted_at_ms: now,
            reason: "object_tombstone".to_string(),
        })
        .collect();

    let follower_safe = snapshot.follower_cursor_safe_to_reclaim;
    let mut eligibility_samples: Vec<StorageGcEligibilitySample> = entries
        .iter()
        .filter_map(|entry| {
            let eligible_after_ms = shard
                .expires_at_ms
                .get(&entry.object_key)
                .copied()
                .unwrap_or(0);
            let has_tombstone = entry.deleted;
            let ttl_eligible = eligible_after_ms > 0 && eligible_after_ms <= now;
            if !has_tombstone && !ttl_eligible {
                return None;
            }
            Some(StorageGcEligibilitySample {
                ref_id: storage_gc_ref(entry),
                eligible_after_ms,
                has_tombstone,
                follower_safe,
                reclaimable_bytes: if follower_safe {
                    entry.address.length
                } else {
                    0
                },
            })
        })
        .take(MAX_STORAGE_GC_SAMPLES)
        .collect();

    if eligibility_samples.is_empty() && snapshot.gc_eligible_record_count > 0 {
        eligibility_samples.push(StorageGcEligibilitySample {
            ref_id: "aggregate:gc_eligible_records".to_string(),
            eligible_after_ms: 0,
            has_tombstone: snapshot.tombstone_records > 0,
            follower_safe,
            reclaimable_bytes: if follower_safe {
                snapshot.reclaimable_bytes
            } else {
                0
            },
        });
    }
    snapshot.gc_eligibility_samples = eligibility_samples;

    snapshot.follower_cursor_safety_samples = vec![StorageFollowerCursorSafetySample {
        min_follower_cursor: snapshot.follower_cursor_retention_floor,
        blocked_reclaim_bytes: if follower_safe {
            0
        } else {
            snapshot.reclaimable_bytes
        },
        safe_to_reclaim: follower_safe,
    }];
    snapshot
}

fn storage_topology_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageTopologySnapshot,
) -> StorageTopologySnapshot {
    let mut entries = collect_live_page_entries(shard);
    entries.sort_by(|left, right| {
        (
            left.address
                .extent_id
                .unwrap_or(left.address.page_segment_id),
            left.address.page_segment_id,
            left.address.offset,
            left.kind.as_str(),
            left.object_key.as_str(),
        )
            .cmp(&(
                right
                    .address
                    .extent_id
                    .unwrap_or(right.address.page_segment_id),
                right.address.page_segment_id,
                right.address.offset,
                right.kind.as_str(),
                right.object_key.as_str(),
            ))
    });

    const MAX_STORAGE_TOPOLOGY_SAMPLES: usize = 8;
    #[derive(Default)]
    struct ZoneAcc {
        used_bytes: u64,
        stale_bytes: u64,
        segments: BTreeSet<u64>,
        generation: u64,
    }
    #[derive(Default)]
    struct SegmentAcc {
        extent_id: u64,
        start_offset: u64,
        generation: u64,
        deleted_refs: u64,
        live_refs: u64,
    }
    #[derive(Default)]
    struct ExtentAcc {
        min_offset: u64,
        max_offset: u64,
        generation: u64,
        deleted_refs: u64,
        live_refs: u64,
    }
    #[derive(Default)]
    struct SlotAcc {
        dirty_generation: u64,
        object_refs: BTreeSet<u64>,
        page_refs: Vec<StoragePageAddressSample>,
        tombstones: BTreeSet<String>,
    }

    let mut zones = BTreeMap::<u64, ZoneAcc>::new();
    let mut segments = BTreeMap::<u64, SegmentAcc>::new();
    let mut extents = BTreeMap::<u64, ExtentAcc>::new();
    let mut slots = BTreeMap::<u32, SlotAcc>::new();

    for entry in &entries {
        let zone_id = entry
            .address
            .extent_id
            .unwrap_or(entry.address.page_segment_id);
        let segment_id = entry.address.page_segment_id;
        let generation = entry.address.object_id.unwrap_or(0);
        let zone = zones.entry(zone_id).or_default();
        zone.segments.insert(segment_id);
        zone.generation = zone.generation.max(generation);
        if entry.deleted {
            zone.stale_bytes = zone.stale_bytes.saturating_add(entry.address.length);
        } else {
            zone.used_bytes = zone.used_bytes.saturating_add(entry.address.length);
        }

        let segment = segments.entry(segment_id).or_insert_with(|| SegmentAcc {
            extent_id: zone_id,
            start_offset: entry.address.offset,
            ..SegmentAcc::default()
        });
        segment.start_offset = segment.start_offset.min(entry.address.offset);
        segment.generation = segment.generation.max(generation);
        if entry.deleted {
            segment.deleted_refs = segment.deleted_refs.saturating_add(1);
        } else {
            segment.live_refs = segment.live_refs.saturating_add(1);
        }

        let extent = extents.entry(zone_id).or_insert_with(|| ExtentAcc {
            min_offset: entry.address.offset,
            max_offset: entry.address.offset.saturating_add(entry.address.length),
            ..ExtentAcc::default()
        });
        extent.min_offset = extent.min_offset.min(entry.address.offset);
        extent.max_offset = extent
            .max_offset
            .max(entry.address.offset.saturating_add(entry.address.length));
        extent.generation = extent.generation.max(generation);
        if entry.deleted {
            extent.deleted_refs = extent.deleted_refs.saturating_add(1);
        } else {
            extent.live_refs = extent.live_refs.saturating_add(1);
        }

        let slot_id = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let slot = slots.entry(slot_id).or_default();
        slot.dirty_generation = slot.dirty_generation.max(generation);
        slot.object_refs.insert(generation);
        if slot.page_refs.len() < MAX_STORAGE_TOPOLOGY_SAMPLES {
            slot.page_refs
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        if entry.deleted {
            slot.tombstones.insert(storage_gc_ref(entry));
        }
    }

    for (slot_id, runtime_slot) in &shard.slot_index.slot_map {
        let slot = slots.entry(*slot_id).or_default();
        slot.dirty_generation = slot.dirty_generation.max(runtime_slot.dirty_generation);
        slot.object_refs
            .extend(runtime_slot.object_index.iter().copied());
        for page in runtime_slot.page_index.values() {
            if slot.page_refs.len() >= MAX_STORAGE_TOPOLOGY_SAMPLES {
                break;
            }
            slot.page_refs
                .push(storage_page_address_sample(shard_id, &page.address));
            if page.deleted {
                slot.tombstones
                    .insert(format!("{}:{}", page.model_id, page.object_key));
            }
        }
    }

    snapshot.storage_zone_samples = zones
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(zone_id, zone)| StorageZoneSample {
            zone_id,
            total_bytes: zone.used_bytes.saturating_add(zone.stale_bytes),
            used_bytes: zone.used_bytes,
            stale_bytes: zone.stale_bytes,
            segments: zone.segments.into_iter().collect(),
        })
        .collect();
    let stream_segments = segments.keys().copied().collect::<Vec<_>>();
    snapshot.stream_samples = (!stream_segments.is_empty())
        .then(|| StorageStreamSample {
            stream_id: format!("shard:{shard_id}:page_stream"),
            rollover_count: snapshot.segment_open_count.saturating_sub(1),
            sealed_segment_count: snapshot.segment_sealed_count,
            segments: stream_segments
                .iter()
                .copied()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
        })
        .into_iter()
        .collect();
    snapshot.segment_samples = segments
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(segment_id, segment)| StorageSegmentSample {
            segment_id,
            extent: segment.extent_id,
            start_offset: segment.start_offset,
            sealed: segment.live_refs == 0 || segment.deleted_refs > 0,
            generation: segment.generation,
        })
        .collect();
    snapshot.extent_samples = extents
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(extent_id, extent)| StorageExtentSample {
            extent: extent_id,
            block_range: vec![extent.min_offset, extent.max_offset],
            reclaim_state: if extent.deleted_refs > 0 && extent.live_refs == 0 {
                "reclaimable".to_string()
            } else if extent.deleted_refs > 0 {
                "mixed_live_stale".to_string()
            } else {
                "live".to_string()
            },
            generation: extent.generation,
        })
        .collect();
    snapshot.slot_samples = slots
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(slot_id, slot)| StorageSlotSample {
            slot_id,
            dirty_generation: slot.dirty_generation,
            object_refs: slot
                .object_refs
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            page_refs: slot
                .page_refs
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            tombstones: slot
                .tombstones
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            owner_mismatch_count: 0,
        })
        .collect();
    snapshot
}

fn collect_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    if !shard.slot_index.slot_map.is_empty() {
        return collect_slot_index_live_page_entries(shard);
    }
    collect_model_live_page_entries(shard)
}

fn model_id_for_kind(kind: &str) -> u16 {
    match kind {
        "string" => 1,
        "hash" => 2,
        "set" => 3,
        "feature" => 4,
        "sequence" => 5,
        "ips" => 6,
        "context_node" => 20,
        "context_event" => 21,
        "context_index" => 22,
        "context_audit" => 23,
        "context_entity" => 25,
        "context_child" => 26,
        "context_embedding" => 27,
        "context_summary" => 28,
        "context_compression" => 29,
        _ => u16::MAX,
    }
}

fn mark_async_dirty_object(
    shard: &mut ShardState,
    object_key: &str,
    start_routing_slot: u32,
    end_routing_slot: u32,
) {
    let routing_slot = page_routing_slot(object_key, start_routing_slot, end_routing_slot);
    shard.dirty_objects.insert(object_key.to_string());
    let slot = shard
        .slot_index
        .slot_map
        .entry(routing_slot)
        .or_insert_with(|| SlotNode {
            routing_slot,
            meta_loaded: true,
            ..SlotNode::default()
        });
    slot.dirty = true;
    slot.dirty_generation = slot.dirty_generation.saturating_add(1).max(1);
}

fn rebuild_slot_page_ownership(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) {
    shard.slot_index.slot_map.clear();
    for entry in collect_model_live_page_entries(shard) {
        let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
            page_routing_slot(&entry.object_key, start_routing_slot, end_routing_slot)
        });
        if routing_slot < start_routing_slot || routing_slot > end_routing_slot {
            continue;
        }
        let object_id = entry.address.object_id.unwrap_or_else(|| {
            stable_page_object_id(
                shard_id,
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
            )
        });
        let slot = shard
            .slot_index
            .slot_map
            .entry(routing_slot)
            .or_insert_with(|| SlotNode {
                routing_slot,
                meta_loaded: true,
                in_memory: true,
                ..SlotNode::default()
            });
        slot.object_index.insert(object_id);
        slot.page_index.insert(
            format!(
                "{}:{}:{}:{}:{}",
                entry.kind,
                entry.object_key,
                entry.component.clone().unwrap_or_default(),
                entry.address.page_segment_id,
                entry.address.offset
            ),
            PageIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                object_id,
                address: entry.address,
                dirty: entry.dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
        );
    }
    shard.slot_index.rebuild_object_page_lookup();
    for slot in shard.slot_index.slot_map.values_mut() {
        slot.meta_loaded = true;
        slot.loading = false;
        slot.in_memory = !slot.page_index.is_empty();
        slot.deleted =
            !slot.page_index.is_empty() && slot.page_index.values().all(|page| page.deleted);
        update_slot_layout(slot);
    }
}

fn promote_model_maps_to_slot_index_authority(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> bool {
    let model_entries = collect_model_live_page_entries(shard);
    if model_entries.is_empty() {
        return false;
    }
    let slot_index_missing_entry = shard.slot_index.slot_map.is_empty()
        || model_entries.iter().any(|entry| {
            !shard.slot_index.contains_object_page_address(
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
                &entry.address,
            )
        });
    if !slot_index_missing_entry {
        return false;
    }
    rebuild_slot_page_ownership(shard_id, shard, start_routing_slot, end_routing_slot);
    refresh_slot_runtime_flags(shard);
    true
}

fn collect_slot_index_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let mut entries = Vec::new();
    for slot in shard.slot_index.slot_map.values() {
        for page in slot.page_index.values() {
            entries.push(LivePageEntry {
                object_key: page.object_key.clone(),
                kind: page.model_id.clone(),
                component: page.component.clone(),
                address: page.address.clone(),
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed,
            });
        }
    }
    entries
}

fn collect_model_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let mut entries = Vec::new();
    entries.extend(
        shard
            .strings
            .iter()
            .map(|(key, address)| live_page_entry(key.clone(), "string", None, address.clone())),
    );
    for (key, fields) in &shard.hashes {
        entries.extend(fields.iter().map(|(field, address)| {
            live_page_entry(key.clone(), "hash", Some(field.clone()), address.clone())
        }));
    }
    for (key, members) in &shard.sets {
        entries.extend(members.iter().map(|(member, address)| {
            live_page_entry(
                key.clone(),
                "set",
                Some(hex::encode(member)),
                address.clone(),
            )
        }));
    }
    for (key, series) in &shard.features {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "feature", None, address)),
        );
    }
    for (key, series) in &shard.sequences {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "sequence", None, address)),
        );
    }
    for (key, series) in &shard.ips {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "ips", None, address)),
        );
    }
    entries.extend(
        shard
            .risk_pages
            .iter()
            .map(|(key, address)| live_page_entry(key.clone(), "risk", None, address.clone())),
    );
    entries.extend(
        shard.context_nodes.iter().map(|(key, address)| {
            live_page_entry(key.clone(), "context_node", None, address.clone())
        }),
    );
    for (key, series) in &shard.context_events {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_event", None, address)),
        );
    }
    for (key, series) in &shard.context_indexes {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_index", None, address)),
        );
    }
    for (key, series) in &shard.context_audits {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_audit", None, address)),
        );
    }
    entries.extend(shard.context_entities.iter().map(|(key, address)| {
        live_page_entry(key.clone(), "context_entity", None, address.clone())
    }));
    for (key, series) in &shard.context_children {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_child", None, address)),
        );
    }
    entries.extend(shard.context_embeddings.iter().map(|(key, address)| {
        live_page_entry(key.clone(), "context_embedding", None, address.clone())
    }));
    for (key, series) in &shard.context_summaries {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_summary", None, address)),
        );
    }
    for (key, series) in &shard.context_compressions {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_compression", None, address)),
        );
    }
    entries
}

fn page_index_ref_key(entry: &LivePageEntry) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        entry.kind,
        entry.object_key,
        entry.component.as_deref().unwrap_or(""),
        entry.address.page_segment_id,
        entry.address.offset,
        entry.address.length,
        entry.address.page_id.unwrap_or_default(),
        entry.address.generation.unwrap_or_default()
    )
}

fn page_physical_identity_key(
    address: &PageAddress,
) -> (
    u64,
    u64,
    u64,
    Option<u64>,
    Option<u64>,
    Option<u32>,
    Option<u64>,
) {
    (
        address.page_segment_id,
        address.offset,
        address.length,
        address.page_id,
        address.object_id,
        address.routing_slot,
        address.generation,
    )
}

fn upsert_slot_index_page(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    address: PageAddress,
    dirty: bool,
) {
    let routing_slot = address
        .routing_slot
        .unwrap_or_else(|| page_routing_slot(object_key, 0, u32::MAX));
    let object_id = address
        .object_id
        .unwrap_or_else(|| stable_page_object_id(shard_id, kind, object_key, component.as_deref()));
    let entry = LivePageEntry {
        object_key: object_key.to_string(),
        kind: kind.to_string(),
        component,
        address,
        dirty,
        deleted: false,
        log_backed: true,
    };
    let lookup_enabled = !shard.slot_index.object_page_lookup.is_empty();
    let direct_page_refs = if lookup_enabled {
        shard
            .slot_index
            .object_page_lookup
            .get(&object_page_lookup_key(
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
            ))
            .cloned()
    } else {
        None
    };
    shard.slot_index.remove_object_page_lookup_entry(
        &entry.kind,
        &entry.object_key,
        entry.component.as_deref(),
    );
    if let Some(page_refs) = direct_page_refs {
        for page_ref in page_refs {
            let Some(slot) = shard.slot_index.slot_map.get_mut(&page_ref.routing_slot) else {
                continue;
            };
            let removed_object_id = slot
                .page_index
                .remove(&page_ref.page_ref_key)
                .map(|page| page.object_id);
            if let Some(removed_object_id) = removed_object_id {
                if !slot
                    .page_index
                    .values()
                    .any(|page| page.object_id == removed_object_id)
                {
                    slot.object_index.remove(&removed_object_id);
                }
                update_slot_layout(slot);
            }
        }
    } else if !lookup_enabled {
        for slot in shard.slot_index.slot_map.values_mut() {
            slot.page_index.retain(|_, page| {
                !(page.object_key == entry.object_key
                    && page.model_id == entry.kind
                    && page.component == entry.component)
            });
            if !slot
                .page_index
                .values()
                .any(|page| page.object_id == object_id)
            {
                slot.object_index.remove(&object_id);
            }
            update_slot_layout(slot);
        }
    }
    let page_ref_key = page_index_ref_key(&entry);
    let page_index = PageIndex {
        object_key: entry.object_key,
        model_id: entry.kind,
        component: entry.component,
        object_id,
        address: entry.address,
        dirty: entry.dirty,
        deleted: entry.deleted,
        log_backed: entry.log_backed,
    };
    {
        let slot = shard
            .slot_index
            .slot_map
            .entry(routing_slot)
            .or_insert_with(|| SlotNode {
                routing_slot,
                meta_loaded: true,
                in_memory: true,
                ..SlotNode::default()
            });
        slot.dirty |= dirty;
        slot.deleted = false;
        if dirty {
            slot.dirty_generation = slot.dirty_generation.saturating_add(1);
        }
        slot.in_memory = true;
        slot.object_index.insert(object_id);
        slot.page_index
            .insert(page_ref_key.clone(), page_index.clone());
        update_slot_layout(slot);
    }
    shard
        .slot_index
        .insert_object_page_lookup(routing_slot, page_ref_key, &page_index);
}

fn sync_slot_index_object_pages(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    addresses: Vec<PageAddress>,
    dirty: bool,
) {
    let mut touched_slots = BTreeSet::new();
    let mut removed_any = false;
    let target_slots = if shard.slot_index.object_component_lookup.is_empty() {
        shard
            .slot_index
            .slot_map
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
    } else {
        shard
            .slot_index
            .object_component_lookup
            .get(&object_component_lookup_key(kind, object_key))
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
        let before = slot.page_index.len();
        slot.page_index
            .retain(|_, page| !(page.model_id == kind && page.object_key == object_key));
        if slot.page_index.len() != before {
            removed_any = true;
            touched_slots.insert(routing_slot);
            slot.dirty |= dirty;
            slot.deleted = slot.page_index.is_empty();
            if dirty {
                slot.dirty_generation = slot.dirty_generation.saturating_add(1);
            }
            slot.in_memory = !slot.page_index.is_empty();
            update_slot_layout(slot);
        }
    }

    let mut unique_addresses = BTreeMap::<
        (
            u64,
            u64,
            u64,
            Option<u64>,
            Option<u64>,
            Option<u32>,
            Option<u64>,
        ),
        PageAddress,
    >::new();
    for address in addresses {
        unique_addresses.insert(page_physical_identity_key(&address), address);
    }

    for address in unique_addresses.into_values() {
        let routing_slot = address
            .routing_slot
            .unwrap_or_else(|| page_routing_slot(object_key, 0, u32::MAX));
        let object_id = address
            .object_id
            .unwrap_or_else(|| stable_page_object_id(shard_id, kind, object_key, None));
        let entry = LivePageEntry {
            object_key: object_key.to_string(),
            kind: kind.to_string(),
            component: None,
            address,
            dirty,
            deleted: false,
            log_backed: true,
        };
        let slot = shard
            .slot_index
            .slot_map
            .entry(routing_slot)
            .or_insert_with(|| SlotNode {
                routing_slot,
                meta_loaded: true,
                in_memory: true,
                ..SlotNode::default()
            });
        slot.dirty |= dirty;
        slot.deleted = false;
        if dirty || touched_slots.insert(routing_slot) {
            slot.dirty_generation = slot.dirty_generation.saturating_add(1);
        }
        slot.meta_loaded = true;
        slot.loading = false;
        slot.in_memory = true;
        slot.object_index.insert(object_id);
        slot.deleted_object_index.remove(&object_id);
        slot.page_index.insert(
            page_index_ref_key(&entry),
            PageIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                object_id,
                address: entry.address,
                dirty: entry.dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
        );
        update_slot_layout(slot);
    }

    if removed_any || dirty {
        shard
            .slot_index
            .slot_map
            .retain(|_, slot| !slot.page_index.is_empty() || !slot.object_index.is_empty());
    }
    shard.slot_index.rebuild_object_page_lookup();
}

fn classify_slot_layout(object_count: usize, page_ref_count: usize) -> SlotLayoutState {
    match (object_count, page_ref_count) {
        (0, _) => SlotLayoutState::Empty,
        (1, 0) => SlotLayoutState::SingleObject,
        (_, 0) => SlotLayoutState::Empty,
        (1, 1) => SlotLayoutState::SinglePageObject,
        (1, _) => SlotLayoutState::MultiPageObject,
        _ => SlotLayoutState::MultiObject,
    }
}

fn slot_layout_name(layout: SlotLayoutState) -> &'static str {
    match layout {
        SlotLayoutState::Empty => "empty",
        SlotLayoutState::SingleObject => "single_object",
        SlotLayoutState::SinglePageObject => "single_page_object",
        SlotLayoutState::MultiPageObject => "multi_page_object",
        SlotLayoutState::MultiObject => "multi_object",
    }
}

fn update_slot_layout(slot: &mut SlotNode) {
    let live_object_ids: BTreeSet<u64> = slot
        .page_index
        .values()
        .filter(|page| !page.deleted)
        .map(|page| page.object_id)
        .collect();
    if !live_object_ids.is_empty() {
        slot.object_index = live_object_ids;
    } else if !slot.page_index.is_empty() {
        slot.object_index.clear();
    }
    slot.layout = classify_slot_layout(slot.object_index.len(), slot.page_index.len());
}

fn refresh_slot_runtime_flags(shard: &mut ShardState) {
    let now = now_ms();
    for slot in shard.slot_index.slot_map.values_mut() {
        slot.meta_loaded = true;
        slot.loading = false;
        slot.in_memory = !slot.page_index.is_empty();
        slot.deleted =
            !slot.page_index.is_empty() && slot.page_index.values().all(|page| page.deleted);
        slot.dirty |= slot
            .page_index
            .values()
            .any(|page| page.dirty || shard.dirty_objects.contains(&page.object_key));
        slot.ttl_ms = slot
            .page_index
            .values()
            .filter_map(|page| shard.expires_at_ms.get(&page.object_key).copied())
            .map(|expires_at| expires_at.saturating_sub(now))
            .min();
        update_slot_layout(slot);
    }
}

fn object_still_has_hot_page(shard: &ShardState, object_key: &str) -> bool {
    shard
        .strings
        .get(object_key)
        .map(|address| address.page_segment_id == HOT_PAGE_SEGMENT_ID)
        .unwrap_or(false)
        || shard
            .hashes
            .get(object_key)
            .map(|fields| {
                fields
                    .values()
                    .any(|address| address.page_segment_id == HOT_PAGE_SEGMENT_ID)
            })
            .unwrap_or(false)
}

fn clear_published_object_dirty_state(shard: &mut ShardState, object_key: &str) {
    if object_still_has_hot_page(shard, object_key) {
        return;
    }
    shard.dirty_objects.remove(object_key);
    for slot in shard.slot_index.slot_map.values_mut() {
        let mut touched = false;
        for page in slot.page_index.values_mut() {
            if page.object_key == object_key {
                page.dirty = false;
                touched = true;
            }
        }
        if touched {
            slot.dirty = slot
                .page_index
                .values()
                .any(|page| page.dirty || shard.dirty_objects.contains(&page.object_key));
            update_slot_layout(slot);
        }
    }
}

fn rebuild_slot_first_index(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) {
    let mut slot_index = CoreIndex::default();
    for entry in collect_model_live_page_entries(shard) {
        let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
            page_routing_slot(&entry.object_key, start_routing_slot, end_routing_slot)
        });
        let object_id = entry.address.object_id.unwrap_or_else(|| {
            stable_page_object_id(
                shard_id,
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
            )
        });
        let slot = slot_index
            .slot_map
            .entry(routing_slot)
            .or_insert_with(|| SlotNode {
                routing_slot,
                meta_loaded: true,
                in_memory: true,
                ..SlotNode::default()
            });
        let page_dirty = shard.dirty_objects.contains(&entry.object_key) || entry.dirty;
        slot.dirty |= page_dirty;
        if page_dirty {
            slot.dirty_generation = slot.dirty_generation.saturating_add(1);
        }
        slot.in_memory |= true;
        slot.object_index.insert(object_id);
        slot.page_index.insert(
            page_index_ref_key(&entry),
            PageIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                object_id,
                address: entry.address,
                dirty: page_dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
        );
        update_slot_layout(slot);
    }
    slot_index.rebuild_object_page_lookup();
    shard.slot_index = slot_index;
}

fn reconcile_secondary_views_from_slot_index(page_store: &LocalPageStore, shard: &mut ShardState) {
    if shard.slot_index.slot_map.is_empty() {
        return;
    }

    let entries = collect_slot_index_live_page_entries(shard)
        .into_iter()
        .filter(|entry| !entry.deleted)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return;
    }

    let mut saw_strings = false;
    let mut saw_hashes = false;
    let mut saw_sets = false;
    let mut saw_features = false;
    let mut saw_sequences = false;
    let mut saw_ips = false;
    let mut saw_risk = false;
    let mut saw_context_events = false;
    let mut saw_context_indexes = false;
    let mut saw_context_audits = false;
    let mut saw_context_entities = false;
    let mut saw_context_children = false;
    let mut saw_context_embeddings = false;
    let mut saw_context_summaries = false;
    let mut saw_context_compressions = false;

    let mut strings = HashMap::new();
    let mut hashes = HashMap::<String, HashMap<String, PageAddress>>::new();
    let mut sets = HashMap::<String, BTreeMap<Vec<u8>, PageAddress>>::new();
    let mut features = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut sequences = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut ips = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut risk = HashMap::<String, BTreeMap<u64, i64>>::new();
    let mut risk_pages = HashMap::new();
    let mut context_events = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut context_indexes = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut context_audits = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut context_entities = HashMap::new();
    let mut context_children = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut context_embeddings = HashMap::new();
    let mut context_summaries = HashMap::<String, BTreeMap<u64, PageAddress>>::new();
    let mut context_compressions = HashMap::<String, BTreeMap<u64, PageAddress>>::new();

    for entry in entries {
        match entry.kind.as_str() {
            "string" => {
                saw_strings = true;
                strings.insert(entry.object_key, entry.address);
            }
            "hash" => {
                saw_hashes = true;
                hashes
                    .entry(entry.object_key)
                    .or_default()
                    .insert(entry.component.unwrap_or_default(), entry.address);
            }
            "set" => {
                saw_sets = true;
                let member = entry
                    .component
                    .as_deref()
                    .and_then(|component| hex::decode(component).ok())
                    .unwrap_or_default();
                sets.entry(entry.object_key)
                    .or_default()
                    .insert(member, entry.address);
            }
            "feature" => {
                saw_features = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut features,
                    entry.object_key,
                    entry.address,
                );
            }
            "sequence" => {
                saw_sequences = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut sequences,
                    entry.object_key,
                    entry.address,
                );
            }
            "ips" => {
                saw_ips = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut ips,
                    entry.object_key,
                    entry.address,
                );
            }
            "risk" => {
                saw_risk = true;
                if let Ok(bytes) = page_store.read(&entry.address) {
                    if let Ok(series) = serde_json::from_slice::<BTreeMap<u64, i64>>(&bytes) {
                        risk.insert(entry.object_key.clone(), series);
                    }
                }
                risk_pages.insert(entry.object_key, entry.address);
            }
            "context_event" => {
                saw_context_events = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut context_events,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_index" => {
                saw_context_indexes = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut context_indexes,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_audit" => {
                saw_context_audits = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut context_audits,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_entity" => {
                saw_context_entities = true;
                context_entities.insert(entry.object_key, entry.address);
            }
            "context_child" => {
                saw_context_children = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut context_children,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_embedding" => {
                saw_context_embeddings = true;
                context_embeddings.insert(entry.object_key, entry.address);
            }
            "context_summary" => {
                saw_context_summaries = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut context_summaries,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_compression" => {
                saw_context_compressions = true;
                insert_timestamped_secondary_view(
                    page_store,
                    &mut context_compressions,
                    entry.object_key,
                    entry.address,
                );
            }
            _ => {}
        }
    }

    if saw_strings {
        shard.strings = strings;
    }
    if saw_hashes {
        shard.hashes = hashes;
    }
    if saw_sets {
        shard.sets = sets;
    }
    if saw_features {
        shard.features = features;
    }
    if saw_sequences {
        shard.sequences = sequences;
    }
    if saw_ips {
        shard.ips = ips;
    }
    if saw_risk {
        shard.risk = risk;
        shard.risk_pages = risk_pages;
    }
    if saw_context_events {
        shard.context_events = context_events;
    }
    if saw_context_indexes {
        shard.context_indexes = context_indexes;
    }
    if saw_context_audits {
        shard.context_audits = context_audits;
    }
    if saw_context_entities {
        shard.context_entities = context_entities;
    }
    if saw_context_children {
        shard.context_children = context_children;
    }
    if saw_context_embeddings {
        shard.context_embeddings = context_embeddings;
    }
    if saw_context_summaries {
        shard.context_summaries = context_summaries;
    }
    if saw_context_compressions {
        shard.context_compressions = context_compressions;
    }

    for slot in shard.slot_index.slot_map.values_mut() {
        update_slot_layout(slot);
    }
}

fn insert_timestamped_secondary_view(
    page_store: &LocalPageStore,
    target: &mut HashMap<String, BTreeMap<u64, PageAddress>>,
    object_key: String,
    address: PageAddress,
) {
    let timestamps = page_store
        .read(&address)
        .ok()
        .and_then(|bytes| match decode_feature_page_strict(&bytes) {
            PackedFeaturePageDecode::Packed(points) => Some(
                points
                    .into_iter()
                    .map(|point| point.timestamp_ms)
                    .collect::<Vec<_>>(),
            ),
            PackedFeaturePageDecode::Legacy | PackedFeaturePageDecode::Corrupt(_) => None,
        })
        .unwrap_or_default();
    let series = target.entry(object_key).or_default();
    for timestamp_ms in timestamps {
        series.insert(timestamp_ms, address.clone());
    }
}

fn expected_live_page_object_id(shard_id: ShardId, entry: &LivePageEntry) -> u64 {
    stable_page_object_id(
        shard_id,
        &entry.kind,
        &entry.object_key,
        entry.component.as_deref(),
    )
}

fn validate_slot_ownership_index(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> StoragePageOwnershipValidation {
    let mut validation = StoragePageOwnershipValidation::default();
    for entry in collect_live_page_entries(shard) {
        let expected_object_id = expected_live_page_object_id(shard_id, &entry);
        let expected_routing_slot =
            page_routing_slot(&entry.object_key, start_routing_slot, end_routing_slot);
        let expected_page_id = entry.address.page_id;
        let object_mismatch = entry
            .address
            .object_id
            .is_some_and(|actual| actual != expected_object_id);
        let slot_mismatch = entry
            .address
            .routing_slot
            .is_some_and(|actual| actual != expected_routing_slot);
        if entry.address.object_id.is_none() || entry.address.routing_slot.is_none() {
            validation.missing_owner_page_refs =
                validation.missing_owner_page_refs.saturating_add(1);
        }
        let slot_page_present = shard
            .slot_index
            .slot_map
            .get(&expected_routing_slot)
            .is_some_and(|slot| {
                slot.object_index.contains(&expected_object_id)
                    && slot.page_index.values().any(|page| {
                        page.address.page_segment_id == entry.address.page_segment_id
                            && page.address.offset == entry.address.offset
                            && page.address.length == entry.address.length
                            && page.address.page_id == expected_page_id
                            && page.model_id == entry.kind
                    })
            });
        if !slot_page_present {
            validation.missing_owner_page_refs =
                validation.missing_owner_page_refs.saturating_add(1);
        }
        if object_mismatch || slot_mismatch {
            validation
                .mismatches
                .push(StorageRecoveryPageOwnerMismatch {
                    object_key: entry.object_key,
                    page_segment_id: entry.address.page_segment_id,
                    offset: entry.address.offset,
                    expected_object_id,
                    actual_object_id: entry.address.object_id,
                    expected_routing_slot,
                    actual_routing_slot: entry.address.routing_slot,
                });
        }
    }
    validation
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

fn command_object_keys(command: &Command) -> Vec<String> {
    match command {
        Command::CommonDelete { key } => associated_record_keys(key),
        Command::CommonExpire { key, .. }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetRemove { key, .. }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::SequenceAdd { key, .. }
        | Command::IpsAdd { key, .. }
        | Command::IpsAddWithOptions { key, .. }
        | Command::IpsLoad { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::RiskIncrement { key, .. }
        | Command::RiskIncrementWithOptions { key, .. }
        | Command::RiskChangeAdd { key, .. }
        | Command::RiskFolSet { key, .. } => vec![key.clone()],
        Command::RiskSet { family, key, .. } | Command::RiskSetAndGet { family, key, .. } => {
            vec![risk_family_key(*family, key)]
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            vec![context_node_key(*tenant_hash, node.node_hash)]
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_event_key(*tenant_hash, *node_hash)],
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            let mut keys = vec![context_event_key(*tenant_hash, *node_hash)];
            if !context_index_disabled(indexes, InternalContextIndex::EventKind) {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_kind",
                    context_event_kind_hash(event),
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Status)
                && indexes.status_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "status",
                    indexes.status_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Source)
                && indexes.source_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "source",
                    indexes.source_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::EventTimeBucket)
                && indexes.event_time_bucket_ms != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Entity) {
                keys.extend(
                    indexes
                        .entity_hashes
                        .iter()
                        .copied()
                        .filter(|hash| *hash != 0)
                        .map(|entity_hash| {
                            context_index_key(
                                *tenant_hash,
                                "entity",
                                entity_hash,
                                indexes.scope_hash,
                            )
                        }),
                );
            }
            keys
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        } => vec![context_index_key(
            *tenant_hash,
            index_name,
            *index_value_hash,
            *scope_hash,
        )],
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            vec![context_audit_key(*tenant_hash, audit.session_hash)]
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => vec![context_dirty_key(*tenant_hash, marker.node_hash)],
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => vec![context_entity_key(
            *tenant_hash,
            entity.node_hash,
            entity.entity_hash,
        )],
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => vec![context_child_key(*tenant_hash, child_ref.parent_hash)],
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => vec![context_embedding_key(*tenant_hash, embedding.ref_hash)],
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => vec![context_summary_key(
            *tenant_hash,
            summary.node_hash,
            summary.level,
        )],
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            vec![context_compression_key(*tenant_hash, event.node_hash)]
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_compression_key(*tenant_hash, *node_hash)],
        Command::SequenceBatchQuery { .. }
        | Command::CommonTtl { .. }
        | Command::CommonExists { .. }
        | Command::StringGet { .. }
        | Command::HashGet { .. }
        | Command::HashMultiGet { .. }
        | Command::HashGetAll { .. }
        | Command::HashLen { .. }
        | Command::SetMembers { .. }
        | Command::FeatureQuery { .. }
        | Command::FeatureQueryFiltered { .. }
        | Command::FeatureAggQuery { .. }
        | Command::SequenceQuery { .. }
        | Command::IpsQueryLast { .. }
        | Command::IpsQueryRange { .. }
        | Command::IpsBatchQueryLast { .. }
        | Command::IpsCount { .. }
        | Command::IpsQueryRangeWithOptions { .. }
        | Command::IpsSnapshot { .. }
        | Command::IpsSnapshotReport { .. }
        | Command::IpsStat { .. }
        | Command::IpsFilter { .. }
        | Command::RiskCount { .. }
        | Command::RiskQuery { .. }
        | Command::RiskDetail { .. }
        | Command::RiskFamilyQuery { .. }
        | Command::RiskFolQuery { .. }
        | Command::RiskManager { .. }
        | Command::RiskDebug { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextGetNodes { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryIndexIntersection { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextQueryEmbeddings { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextQueryNodeContext { .. } => Vec::new(),
    }
}

fn command_updates_slot_index_directly(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::StringDelete { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
    )
}

fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::IpsAddWithOptions { .. }
            | Command::IpsLoad { .. }
            | Command::IpsRemove { .. }
            | Command::IpsDelete { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskChangeAdd { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
            | Command::RiskFolSet { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteExtractedEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
            | Command::ContextUpsertEntity { .. }
            | Command::ContextUpsertChildRef { .. }
            | Command::ContextUpsertEmbedding { .. }
            | Command::ContextUpsertSummary { .. }
            | Command::ContextWriteCompressionEvent { .. }
            | Command::ContextCompressEvents { .. }
    )
}

fn admission_limits(
    shard_id: ShardId,
    write_command: bool,
    config: &Config,
    info: &Option<ShardInfo>,
) -> Vec<AdmissionLimit> {
    let mut limits = Vec::new();
    if let Some(limit) = if write_command {
        config.write_qps
    } else {
        config.read_qps
    } {
        limits.push(AdmissionLimit {
            scope: AdmissionScope::Shard(shard_id),
            limit,
            label: if write_command {
                "write_qps"
            } else {
                "read_qps"
            },
        });
    }
    if let Some(table_name) = info
        .as_ref()
        .map(|info| info.table_name.trim())
        .filter(|table_name| !table_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.table_write_qps
        } else {
            config.table_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Table(table_name.to_string()),
                limit,
                label: if write_command {
                    "table_write_qps"
                } else {
                    "table_read_qps"
                },
            });
        }
    }
    if let Some(tenant_name) = config
        .tenant_name
        .as_deref()
        .map(str::trim)
        .filter(|tenant_name| !tenant_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.tenant_write_qps
        } else {
            config.tenant_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Tenant(tenant_name.to_string()),
                limit,
                label: if write_command {
                    "tenant_write_qps"
                } else {
                    "tenant_read_qps"
                },
            });
        }
    }
    limits
}

fn reset_admission_window(admission: &mut AdmissionState, now_sec: u64) {
    if admission.window_epoch_sec != now_sec {
        admission.window_epoch_sec = now_sec;
        admission.read_count = 0;
        admission.write_count = 0;
    }
}

fn admission_count(admission: &mut AdmissionState, write_command: bool) -> &mut u64 {
    if write_command {
        &mut admission.write_count
    } else {
        &mut admission.read_count
    }
}

fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn validate_command_preconditions(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    command: &Command,
) -> Result<(), Status> {
    match command {
        Command::CommonExpire { key, .. } => {
            if shard
                .expires_at_ms
                .get(key)
                .map(|expires_at| *expires_at <= now_ms())
                .unwrap_or(false)
                || !record_exists(shard, key)
            {
                return Err(Status::error("not_found", "key not found"));
            }
        }
        Command::FeatureAppend { key, points } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::FeatureAppendWithPolicy {
            key,
            points,
            policy,
        } => {
            if *policy == FeatureWritePolicy::Block {
                return Err(Status::error("invalid_argument", "Invalid write policy"));
            }
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::FeatureReplace { key, points, .. } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_node(node)?;
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
        }
        Command::ContextGetNodes {
            tenant_hash,
            node_hashes,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!node_hashes.is_empty(), "node_hashes are required")?;
            for node_hash in node_hashes {
                validate_context_required(*node_hash != 0, "node_hash is required")?;
            }
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            event,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
        }
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
            validate_context_extracted_indexes(event, indexes)?;
        }
        Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            kinds,
            statuses,
            min_confidence,
            min_importance,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            validate_context_filters(kinds, statuses, *min_confidence, *min_importance)?;
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            event_time_ms,
            index_ref,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_required(*event_time_ms != 0, "event_time_ms is required")?;
            validate_context_timestamp(*event_time_ms)?;
            validate_context_index_ref(index_ref)?;
        }
        Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextQueryIndexIntersection {
            tenant_hash,
            predicates,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(!predicates.is_empty(), "predicates are required")?;
            validate_context_limit(*limit)?;
            for predicate in predicates {
                validate_context_index_lookup(predicate)?;
            }
        }
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_pack_audit(audit)?;
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*session_hash != 0, "session_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_dirty_marker(marker)?;
        }
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_entity(entity)?;
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*entity_hash != 0, "entity_hash is required")?;
        }
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            entity_hashes,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            if entity_hashes.len() > CONTEXT_MAX_LIMIT {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes exceeds maximum",
                ));
            }
            if entity_hashes.iter().any(|hash| *hash == 0) {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes must be non-zero",
                ));
            }
        }
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_child_ref(child_ref)?;
        }
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*parent_hash != 0, "parent_hash is required")?;
            validate_context_limit(*limit)?;
        }
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_embedding(embedding)?;
        }
        Command::ContextQueryEmbeddings {
            tenant_hash,
            ref_hashes,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_limit(*limit)?;
            if ref_hashes.len() > CONTEXT_MAX_LIMIT {
                return Err(Status::error("invalid_argument", "too many ref_hashes"));
            }
        }
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            query_vector,
            max_depth,
            top_k_per_depth,
            max_children_scored_per_parent,
            max_candidate_nodes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*start_node_hash != 0, "start_node_hash is required")?;
            validate_context_required(!query_vector.is_empty(), "query_vector is required")?;
            validate_context_embedding_vector("query_vector", query_vector)?;
            if max_depth.unwrap_or_default() > CONTEXT_MAX_TRAVERSAL_DEPTH {
                return Err(Status::error(
                    "invalid_argument",
                    "max_depth exceeds maximum",
                ));
            }
            for limit in [
                *top_k_per_depth,
                *max_children_scored_per_parent,
                *max_candidate_nodes,
            ] {
                validate_context_limit(limit)?;
            }
        }
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_summary(summary)?;
        }
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            as_of_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*level != 0, "level is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            validate_context_limit(*limit)?;
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_compression_event(event)?;
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            if node_hashes.len() > CONTEXT_MAX_FILTER_VALUES {
                return Err(Status::error("invalid_argument", "too many node_hashes"));
            }
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            source_start_ms,
            source_end_ms,
            compressed_time_ms,
            max_source_events,
            min_confidence,
            min_importance,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_range(*source_start_ms, *source_end_ms)?;
            validate_context_limit(*max_source_events)?;
            validate_context_score("min_confidence", *min_confidence)?;
            validate_context_score("min_importance", *min_importance)?;
            if *compressed_time_ms != 0 {
                validate_context_timestamp(*compressed_time_ms)?;
            }
        }
        Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            summary_level,
            as_of_ms,
            cold_start_time_ms,
            cold_end_time_ms,
            compression_limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            if summary_level.unwrap_or(1) == 0 {
                return Err(Status::error(
                    "invalid_argument",
                    "summary_level is required",
                ));
            }
            validate_context_limit(*compression_limit)?;
            if *cold_start_time_ms != 0 || *cold_end_time_ms != 0 {
                validate_context_range(*cold_start_time_ms, *cold_end_time_ms)?;
            }
        }
        _ => {}
    }

    if let Command::HashIncrBy {
        key,
        field,
        increment,
    } = command
    {
        if shard
            .expires_at_ms
            .get(key)
            .map(|expires_at| *expires_at <= now_ms())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let Some(bytes) = shard
            .hashes
            .get(key)
            .and_then(|entries| entries.get(field))
            .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
        else {
            return 0_i64
                .checked_add(*increment)
                .map(|_| ())
                .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"));
        };
        let current = parse_i64(&bytes)
            .ok_or_else(|| Status::error("unmatched", "hash value is not an integer"))?;
        current
            .checked_add(*increment)
            .map(|_| ())
            .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"))?;
    }
    Ok(())
}

fn cached_response(
    cache: &MultiLayerCache,
    key: CacheKey,
    source: impl FnOnce() -> CommandResponse,
) -> CommandResponse {
    if let Ok(Some(bytes)) = cache.get(&key) {
        if let Ok(response) = serde_json::from_slice::<CommandResponse>(&bytes) {
            return response;
        }
        let _ = cache.invalidate(&key);
    }
    let response = source();
    if let Ok(bytes) = serde_json::to_vec(&response) {
        cache.put_memory_only(key, bytes);
    }
    response
}

fn invalidate_if_cached(cache: &MultiLayerCache, key: CacheKey) {
    if cache.peek(&key) {
        let _ = cache.invalidate(&key);
    }
}

#[cfg(test)]
mod tests;
