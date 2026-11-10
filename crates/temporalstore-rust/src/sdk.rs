// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub mod v1 {
    tonic::include_proto!("temporalstore.v1");
}

use std::sync::Arc;

use tonic::{Request, Response, Status as TonicStatus};

use crate::client::TemporalStoreTable;
use crate::engine::TemporalEngine;
use crate::types;
use crate::{ClientError, TableOptions, TemporalStoreClient};

pub trait TemporalStoreSdkExecutor: Send + Sync + 'static {
    fn execute_sdk(&self, request: types::ExecuteRequest) -> types::ExecuteResponse;

    fn batch_execute_sdk(
        &self,
        request: types::BatchExecuteRequest,
    ) -> types::BatchExecuteResponse {
        let responses = request
            .commands
            .into_iter()
            .map(|command| {
                self.execute_sdk(types::ExecuteRequest {
                    shard_id: request.shard_id,
                    command,
                })
            })
            .collect::<Vec<_>>();
        let status = responses
            .iter()
            .find(|response| !response.status.ok)
            .map(|response| response.status.clone())
            .unwrap_or_else(types::Status::ok);
        types::BatchExecuteResponse { status, responses }
    }

    fn open_table_sdk(&self, _request: v1::OpenTableRequest) -> v1::OpenTableResponse {
        v1::OpenTableResponse {
            status: Some(error_status(
                "not_implemented",
                "open_table tonic adapter is not wired to metaserver yet",
            )),
            topology: None,
        }
    }

    fn sync_topology_sdk(&self, _request: v1::SyncTopologyRequest) -> v1::SyncTopologyResponse {
        v1::SyncTopologyResponse {
            status: Some(error_status(
                "not_implemented",
                "sync_topology tonic adapter is not wired to metaserver yet",
            )),
            topologies: Vec::new(),
            topology_version: 0,
        }
    }

    fn client_preflight_sdk(
        &self,
        _request: v1::ClientPreflightRequest,
    ) -> v1::ClientPreflightResponse {
        v1::ClientPreflightResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            route_cache_entries: 0,
            table_cache_entries: 0,
            backend_failure_entries: 0,
            topology_version: 0,
            degraded: false,
            warnings: Vec::new(),
        }
    }
}

impl TemporalStoreSdkExecutor for TemporalEngine {
    fn execute_sdk(&self, request: types::ExecuteRequest) -> types::ExecuteResponse {
        self.execute(request)
    }

    fn batch_execute_sdk(
        &self,
        request: types::BatchExecuteRequest,
    ) -> types::BatchExecuteResponse {
        self.batch_execute(request)
    }
}

impl TemporalStoreSdkExecutor for TemporalStoreClient {
    fn execute_sdk(&self, request: types::ExecuteRequest) -> types::ExecuteResponse {
        match self.execute_with_options(request, crate::RequestOptions::default()) {
            Ok(response) => response,
            Err(err) => client_error_execute_response(err),
        }
    }

    fn batch_execute_sdk(
        &self,
        request: types::BatchExecuteRequest,
    ) -> types::BatchExecuteResponse {
        match self.batch_execute_with_options(request, crate::RequestOptions::default()) {
            Ok(response) => response,
            Err(err) => types::BatchExecuteResponse {
                status: client_error_status(err),
                responses: Vec::new(),
            },
        }
    }

    fn open_table_sdk(&self, request: v1::OpenTableRequest) -> v1::OpenTableResponse {
        let namespace = request.namespace_name;
        let table_name = request.table_name;
        let table = match self.open_table_from_meta(namespace.clone(), table_name.clone()) {
            Ok(table) => table,
            Err(ClientError::Status(message)) if message.contains("meta_addr is required") => self
                .open_table(
                    namespace.clone(),
                    table_name.clone(),
                    TableOptions::default(),
                ),
            Err(err) => {
                return v1::OpenTableResponse {
                    status: Some(client_error_status_to_sdk(err)),
                    topology: None,
                };
            }
        };
        v1::OpenTableResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            topology: Some(table_to_sdk_topology(self, &table)),
        }
    }

    fn sync_topology_sdk(&self, request: v1::SyncTopologyRequest) -> v1::SyncTopologyResponse {
        let table_keys = if request.table_keys.is_empty() {
            self.open_table_keys()
        } else {
            request.table_keys
        };
        let mut topologies = Vec::new();
        let mut failures = Vec::new();
        for key in table_keys {
            let Some((namespace, table_name)) = split_table_key(&key) else {
                failures.push(format!("{key}:invalid_table_key"));
                continue;
            };
            match self.sync_table_topology(namespace.clone(), table_name.clone()) {
                Ok(_) => {
                    if let Some(table) = self.cached_table(namespace, table_name) {
                        topologies.push(table_to_sdk_topology(self, &table));
                    }
                }
                Err(err) => {
                    if let Some(table) = self.cached_table(namespace, table_name) {
                        topologies.push(table_to_sdk_topology(self, &table));
                    } else {
                        failures.push(format!("{key}:{}", client_error_code(&err)));
                    }
                }
            }
        }
        let topology_version = self.topology_cache_report().max_topology_version;
        let status = if failures.is_empty() {
            types::Status::ok()
        } else {
            types::Status::error("partial_sync_topology", failures.join(","))
        };
        v1::SyncTopologyResponse {
            status: Some(types_status_to_sdk(status)),
            topologies,
            topology_version,
        }
    }

    fn client_preflight_sdk(
        &self,
        _request: v1::ClientPreflightRequest,
    ) -> v1::ClientPreflightResponse {
        let report = self.preflight_report();
        v1::ClientPreflightResponse {
            status: Some(types_status_to_sdk(report.status)),
            route_cache_entries: report.route_cache_size as u64,
            table_cache_entries: report.table_cache_size as u64,
            backend_failure_entries: report.backend_failure_count as u64,
            topology_version: report.topology_cache.max_topology_version,
            degraded: !report.degraded_reasons.is_empty(),
            warnings: report.degraded_reasons,
        }
    }
}

#[derive(Clone)]
pub struct TemporalStoreTonicAdapter {
    executor: Arc<dyn TemporalStoreSdkExecutor>,
}

impl TemporalStoreTonicAdapter {
    pub fn new(executor: impl TemporalStoreSdkExecutor) -> Self {
        Self {
            executor: Arc::new(executor),
        }
    }

    pub fn from_arc(executor: Arc<dyn TemporalStoreSdkExecutor>) -> Self {
        Self { executor }
    }
}

#[tonic::async_trait]
impl v1::temporal_store_service_server::TemporalStoreService for TemporalStoreTonicAdapter {
    async fn execute(
        &self,
        request: Request<v1::ExecuteRequest>,
    ) -> Result<Response<v1::ExecuteResponse>, TonicStatus> {
        let request = sdk_execute_request_to_types(request.into_inner())?;
        let response = self.executor.execute_sdk(request);
        Ok(Response::new(types_execute_response_to_sdk(response)))
    }

    async fn batch_execute(
        &self,
        request: Request<v1::BatchExecuteRequest>,
    ) -> Result<Response<v1::BatchExecuteResponse>, TonicStatus> {
        let request = sdk_batch_request_to_types(request.into_inner())?;
        let response = self.executor.batch_execute_sdk(request);
        Ok(Response::new(types_batch_response_to_sdk(response)))
    }

    async fn open_table(
        &self,
        request: Request<v1::OpenTableRequest>,
    ) -> Result<Response<v1::OpenTableResponse>, TonicStatus> {
        Ok(Response::new(
            self.executor.open_table_sdk(request.into_inner()),
        ))
    }

    async fn sync_topology(
        &self,
        request: Request<v1::SyncTopologyRequest>,
    ) -> Result<Response<v1::SyncTopologyResponse>, TonicStatus> {
        Ok(Response::new(
            self.executor.sync_topology_sdk(request.into_inner()),
        ))
    }

    async fn get_client_preflight(
        &self,
        request: Request<v1::ClientPreflightRequest>,
    ) -> Result<Response<v1::ClientPreflightResponse>, TonicStatus> {
        Ok(Response::new(
            self.executor.client_preflight_sdk(request.into_inner()),
        ))
    }
}

pub fn sdk_execute_request_to_types(
    request: v1::ExecuteRequest,
) -> Result<types::ExecuteRequest, TonicStatus> {
    Ok(types::ExecuteRequest {
        shard_id: request.shard_id,
        command: sdk_command_to_types(required_command(request.command)?)?,
    })
}

pub fn sdk_batch_request_to_types(
    request: v1::BatchExecuteRequest,
) -> Result<types::BatchExecuteRequest, TonicStatus> {
    let commands = request
        .commands
        .into_iter()
        .map(sdk_command_to_types)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(types::BatchExecuteRequest {
        shard_id: request.shard_id,
        commands,
    })
}

fn required_command(command: Option<v1::Command>) -> Result<v1::Command, TonicStatus> {
    command.ok_or_else(|| TonicStatus::invalid_argument("missing command"))
}

pub fn sdk_command_to_types(command: v1::Command) -> Result<types::Command, TonicStatus> {
    let kind = command
        .kind
        .ok_or_else(|| TonicStatus::invalid_argument("missing command kind"))?;
    Ok(match kind {
        v1::command::Kind::StringSet(command) => {
            if command.ttl_ms > 0 {
                types::Command::StringSetEx {
                    key: command.key,
                    value: command.value,
                    ttl_ms: command.ttl_ms,
                }
            } else {
                types::Command::StringSet {
                    key: command.key,
                    value: command.value,
                }
            }
        }
        v1::command::Kind::StringGet(command) => types::Command::StringGet { key: command.key },
        v1::command::Kind::HashDelete(command) => types::Command::HashDelete {
            key: command.key,
            field: command.field,
        },
        v1::command::Kind::HashIncrBy(command) => types::Command::HashIncrBy {
            key: command.key,
            field: command.field,
            increment: command.increment,
        },
        v1::command::Kind::ListPush(command) => types::Command::ListPush {
            key: command.key,
            member: command.member,
            left: command.left,
        },
        v1::command::Kind::ListPop(command) => types::Command::ListPop {
            key: command.key,
            left: command.left,
        },
        v1::command::Kind::SetRemove(command) => types::Command::SetRemove {
            key: command.key,
            member: command.member,
        },
        v1::command::Kind::ZsetAdd(command) => types::Command::ZSetAdd {
            key: command.key,
            member: command.member,
            score: command.score,
        },
        v1::command::Kind::ZsetRemove(command) => types::Command::ZSetRemove {
            key: command.key,
            member: command.member,
        },
        v1::command::Kind::ZsetIncrBy(command) => types::Command::ZSetIncrBy {
            key: command.key,
            member: command.member,
            increment: command.increment,
        },
        v1::command::Kind::ZsetPop(command) => types::Command::ZSetPop {
            key: command.key,
            min: command.min,
            count: command.count,
        },
        v1::command::Kind::StringDelete(command) => {
            types::Command::StringDelete { key: command.key }
        }
        v1::command::Kind::HashSet(command) => types::Command::HashSet {
            key: command.key,
            field: command.field,
            value: command.value,
        },
        v1::command::Kind::HashGet(command) => types::Command::HashGet {
            key: command.key,
            field: command.field,
        },
        v1::command::Kind::HashMultiSet(command) => types::Command::HashMultiSet {
            key: command.key,
            entries: command
                .entries
                .into_iter()
                .map(|entry| (entry.field, entry.value))
                .collect(),
        },
        v1::command::Kind::HashMultiSetIfTimestampOlder(command) => {
            types::Command::HashMultiSetIfTimestampOlder {
                key: command.key,
                timestamp_field: command.timestamp_field,
                timestamp_seconds: command.timestamp_seconds,
                timestamp_nanos: command.timestamp_nanos,
                ttl_ms: command.ttl_ms,
                entries: command
                    .entries
                    .into_iter()
                    .map(|entry| (entry.field, entry.value))
                    .collect(),
            }
        }
        v1::command::Kind::HashMultiGet(command) => types::Command::HashMultiGet {
            key: command.key,
            fields: command.fields,
        },
        v1::command::Kind::SetAdd(command) => types::Command::SetAdd {
            key: command.key,
            member: command.member,
        },
        v1::command::Kind::SetMembers(command) => types::Command::SetMembers { key: command.key },
        v1::command::Kind::SetScan(command) => types::Command::SetScan {
            key: command.key,
            after: command.after,
            limit: usize::try_from(command.limit)
                .map_err(|_| TonicStatus::invalid_argument("set scan limit is too large"))?,
        },
        v1::command::Kind::FeatureAppend(command) => types::Command::FeatureAppend {
            key: command.key,
            points: command
                .points
                .into_iter()
                .map(sdk_feature_point_to_types)
                .collect(),
        },
        v1::command::Kind::FeatureAppendWithTtl(command) => {
            types::Command::FeatureAppendWithTtl {
                key: command.key,
                points: command
                    .points
                    .into_iter()
                    .map(sdk_feature_point_to_types)
                    .collect(),
                ttl_ms: command.ttl_ms,
            }
        }
        v1::command::Kind::FeatureQuery(command) => types::Command::FeatureQuery {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            count: nonzero_limit(command.limit),
        },
        v1::command::Kind::SequenceAppend(command) => types::Command::SequenceAdd {
            key: command.key,
            rows: command
                .rows
                .into_iter()
                .map(|row| types::SequenceFeatureRow {
                    timestamp_ms: row.timestamp_ms,
                    gid: row.gid,
                    action_type: row.action_type,
                    duration: row.duration,
                    author_id: row.author_id,
                })
                .collect(),
        },
        v1::command::Kind::SequenceQuery(command) => types::Command::SequenceQuery {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            count: command.limit.max(1) as usize,
            filters: Vec::new(),
        },
        v1::command::Kind::ControlStateIncrement(command) => types::Command::ControlStateIncrement {
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            amount: command.delta,
        },
        v1::command::Kind::ControlStateQuery(command) => types::Command::ControlStateQuery {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            aggregator: command.family,
        },
        v1::command::Kind::ContextNodeUpsert(command) => {
            let node = command
                .node
                .ok_or_else(|| TonicStatus::invalid_argument("context node missing"))?;
            types::Command::ContextUpsertNode {
                tenant_hash: stable_hash(&command.key),
                node: Box::new(sdk_context_node_to_types(node)),
            }
        }
        v1::command::Kind::ContextNodeGet(command) => types::Command::ContextGetNode {
            tenant_hash: stable_hash(&command.key),
            node_hash: stable_hash(&command.node_id),
        },
        v1::command::Kind::CommonExpire(command) => types::Command::CommonExpire {
            key: command.key,
            ttl_ms: command.ttl_ms,
        },
        v1::command::Kind::CommonExists(command) => {
            types::Command::CommonExists { key: command.key }
        }
        v1::command::Kind::ControlStateIncrementWithOptions(command) => {
            types::Command::ControlStateIncrementWithOptions {
                key: command.key,
                timestamp_ms: command.timestamp_ms,
                amount: command.amount,
                precision_ms: command.precision_ms,
                ttl_ms: command.ttl_ms,
            }
        }
        v1::command::Kind::ControlStateChangeAdd(command) => {
            types::Command::ControlStateChangeAdd {
                key: command.key,
                timestamp_ms: command.timestamp_ms,
                value: command.value,
                precision_ms: command.precision_ms,
                ttl_ms: command.ttl_ms,
            }
        }
        v1::command::Kind::ControlStateSet(command) => types::Command::ControlStateSet {
            family: control_state_family_from_proto(command.family)?,
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            amount: command.amount,
        },
        v1::command::Kind::ControlStateSetAndGet(command) => {
            types::Command::ControlStateSetAndGet {
                family: control_state_family_from_proto(command.family)?,
                key: command.key,
                timestamp_ms: command.timestamp_ms,
                amount: command.amount,
                start_ms: command.start_ms,
                end_ms: command.end_ms,
                aggregator: command.aggregator,
            }
        }
        v1::command::Kind::ControlStateSetAndGetWithOptions(command) => {
            types::Command::ControlStateSetAndGetWithOptions {
                family: control_state_family_from_proto(command.family)?,
                key: command.key,
                timestamp_ms: command.timestamp_ms,
                amount: command.amount,
                start_ms: command.start_ms,
                end_ms: command.end_ms,
                aggregator: command.aggregator,
                precision_ms: command.precision_ms,
                ttl_ms: command.ttl_ms,
                uuid: command.uuid,
            }
        }
        v1::command::Kind::ControlStateSelectionSet(command) => {
            types::Command::ControlStateSelectionSet {
                key: command.key,
                value: command.value,
                occur_time_ms: command.occur_time_ms,
                ttl_ms: command.ttl_ms,
                selection_type: control_state_selection_type_from_proto(command.selection_type)?,
            }
        }
        v1::command::Kind::BucketTake(command) => types::Command::BucketTake {
            key: command.key,
            tokens: command.tokens,
            capacity: command.capacity,
            refill_per_sec: command.refill_per_sec,
        },
        v1::command::Kind::CommonDelete(command) => {
            types::Command::CommonDelete { key: command.key }
        }
        v1::command::Kind::CommonPersist(command) => {
            types::Command::CommonPersist { key: command.key }
        }
        v1::command::Kind::FeatureDelete(command) => {
            types::Command::FeatureDelete { key: command.key }
        }
        v1::command::Kind::FeatureReplace(command) => types::Command::FeatureReplace {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            points: command
                .points
                .into_iter()
                .map(sdk_feature_point_to_types)
                .collect(),
        },
        v1::command::Kind::FeatureAppendWithPolicy(command) => {
            types::Command::FeatureAppendWithPolicy {
                key: command.key,
                points: command
                    .points
                    .into_iter()
                    .map(sdk_feature_point_to_types)
                    .collect(),
                policy: feature_write_policy_from_proto(command.policy)?,
            }
        }
        v1::command::Kind::SeenCheck(command) => types::Command::SeenCheck {
            key: command.key,
            member: command.member,
            window_ms: command.window_ms,
        },
        v1::command::Kind::StringSetConditional(command) => {
            types::Command::StringSetConditional {
                key: command.key,
                value: command.value,
                // Straight across: `None` means discard the deadline, `Some(0)` means arm zero.
                ttl_ms: command.ttl_ms,
                condition: string_set_condition_from_proto(command.condition)?,
                return_old: command.return_old,
                // Absent decodes to false, which is what every record written before this field
                // existed was recorded with. Reading absent as "leave the deadline alone" would
                // change those records' meaning on replay.
                keep_ttl: command.keep_ttl,
            }
        }
        v1::command::Kind::ContextResourceBlobBegin(command) => {
            types::Command::ContextResourceBlobBegin {
                tenant_hash: command.tenant_hash,
            }
        }
        v1::command::Kind::ContextResourceBlobAppend(command) => {
            types::Command::ContextResourceBlobAppend {
                tenant_hash: command.tenant_hash,
                upload_token: command.upload_token,
                payload: command.payload,
            }
        }
        v1::command::Kind::ContextResourceBlobCommit(command) => {
            types::Command::ContextResourceBlobCommit {
                tenant_hash: command.tenant_hash,
                upload_token: command.upload_token,
            }
        }
        v1::command::Kind::ContextResourceBlobPut(command) => {
            types::Command::ContextResourceBlobPut {
                tenant_hash: command.tenant_hash,
                payload: command.payload,
            }
        }
        v1::command::Kind::ContextResourceBlobSweep(command) => {
            types::Command::ContextResourceBlobSweep {
                tenant_hash: command.tenant_hash,
                referenced_content_hashes: command.referenced_content_hashes,
                min_age_ms: command.min_age_ms,
            }
        }
        v1::command::Kind::ContextMarkEmbeddingDirty(command) => {
            types::Command::ContextMarkEmbeddingDirty {
                tenant_hash: command.tenant_hash,
                node_hash: command.node_hash,
                event_time_ms: command.event_time_ms,
                reason: command.reason,
                propagate_depth: command.propagate_depth,
                clear: command.clear,
            }
        }
        v1::command::Kind::ContextMarkSummaryDirty(command) => {
            types::Command::ContextMarkSummaryDirty {
                tenant_hash: command.tenant_hash,
                node_hash: command.node_hash,
                event_time_ms: command.event_time_ms,
                reason: command.reason,
                propagate_depth: command.propagate_depth,
            }
        }
        v1::command::Kind::ContextCompressEvents(command) => {
            types::Command::ContextCompressEvents {
                tenant_hash: command.tenant_hash,
                node_hash: command.node_hash,
                source_start_ms: command.source_start_ms,
                source_end_ms: command.source_end_ms,
                compressed_time_ms: command.compressed_time_ms,
                // Absent stays absent: "no cap" must not arrive as a cap of zero.
                max_source_events: command.max_source_events.map(|count| count as usize),
                min_confidence: command.min_confidence,
                min_importance: command.min_importance,
            }
        }
        v1::command::Kind::ContextUpsertChildRef(command) => {
            // A nested message is always optional on the wire, so an absent one is REFUSED by
            // name. The write has no meaning without the record it is upserting, and defaulting
            // it would file a child ref of all zeros.
            let child_ref = command
                .child_ref
                .ok_or_else(|| TonicStatus::invalid_argument("context child ref missing"))?;
            types::Command::ContextUpsertChildRef {
                tenant_hash: command.tenant_hash,
                child_ref: types::ContextChildRef {
                    parent_hash: child_ref.parent_hash,
                    child_hash: child_ref.child_hash,
                    updated_at_ms: child_ref.updated_at_ms,
                },
            }
        }
        v1::command::Kind::ContextUpsertNode(command) => {
            // Refused rather than defaulted: an upsert with no node would file a record of all
            // zeros, and a fabricated zero on disk cannot be told from a real one.
            let node = command
                .node
                .ok_or_else(|| TonicStatus::invalid_argument("context node record missing"))?;
            types::Command::ContextUpsertNode {
                tenant_hash: command.tenant_hash,
                node: Box::new(context_node_record_from_proto(node)),
            }
        }
        v1::command::Kind::ContextSetNodeEmbedding(command) => {
            types::Command::ContextSetNodeEmbedding {
                tenant_hash: command.tenant_hash,
                node_hash: command.node_hash,
                model_hash: command.model_hash,
                vector: command.vector,
                updated_at_ms: command.updated_at_ms,
            }
        }
        v1::command::Kind::ContextWriteEvent(command) => {
            // Refused rather than defaulted: a write with no event would file a record of all
            // zeros, and a fabricated zero on disk cannot be told from a real one.
            let event = command
                .event
                .ok_or_else(|| TonicStatus::invalid_argument("context event record missing"))?;
            types::Command::ContextWriteEvent {
                tenant_hash: command.tenant_hash,
                node_hash: command.node_hash,
                event: Box::new(context_event_record_from_proto(event)),
                first_write_only: command.first_write_only,
                cold_storage: command.cold_storage,
            }
        }
        v1::command::Kind::ContextUpsertEntity(command) => {
            // Refused rather than defaulted: an upsert with no entity would file a record of all
            // zeros, and a fabricated zero on disk cannot be told from a real one.
            let entity = command
                .entity
                .ok_or_else(|| TonicStatus::invalid_argument("context entity record missing"))?;
            types::Command::ContextUpsertEntity {
                tenant_hash: command.tenant_hash,
                entity: context_entity_record_from_proto(entity),
            }
        }
        v1::command::Kind::ContextUpsertSummary(command) => {
            let summary = command
                .summary
                .ok_or_else(|| TonicStatus::invalid_argument("context summary record missing"))?;
            types::Command::ContextUpsertSummary {
                tenant_hash: command.tenant_hash,
                summary: context_summary_record_from_proto(summary),
            }
        }
        v1::command::Kind::ContextWriteCompressionEvent(command) => {
            let event = command.event.ok_or_else(|| {
                TonicStatus::invalid_argument("context compression event record missing")
            })?;
            types::Command::ContextWriteCompressionEvent {
                tenant_hash: command.tenant_hash,
                event: context_compression_event_record_from_proto(event),
            }
        }
        v1::command::Kind::ContextWritePackAudit(command) => {
            let audit = command
                .audit
                .ok_or_else(|| TonicStatus::invalid_argument("context pack audit record missing"))?;
            types::Command::ContextWritePackAudit {
                tenant_hash: command.tenant_hash,
                audit: context_pack_audit_record_from_proto(audit),
            }
        }
        v1::command::Kind::ContextWriteExtractedEvent(command) => {
            let event = command
                .event
                .ok_or_else(|| TonicStatus::invalid_argument("context event record missing"))?;
            // DEFAULTED, not refused, and the difference is read off the Rust declaration rather
            // than decided here: `indexes` is `#[serde(default)]` over a type with a `Default`, so
            // the JSON reader this replaces already takes an absent block as the empty default.
            // `event` above has no default and no absence the JSON form accepts.
            let indexes = match command.indexes {
                Some(indexes) => context_extracted_event_indexes_from_proto(indexes)?,
                None => types::ContextExtractedEventIndexes::default(),
            };
            types::Command::ContextWriteExtractedEvent {
                tenant_hash: command.tenant_hash,
                node_hash: command.node_hash,
                event: Box::new(context_event_record_from_proto(event)),
                indexes,
                first_write_only: command.first_write_only,
                cold_storage: command.cold_storage,
            }
        }
        v1::command::Kind::ContextWriteIndexRef(command) => {
            let index_ref = command
                .index_ref
                .ok_or_else(|| TonicStatus::invalid_argument("context index ref missing"))?;
            types::Command::ContextWriteIndexRef {
                tenant_hash: command.tenant_hash,
                index_name: command.index_name,
                index_value_hash: command.index_value_hash,
                scope_hash: command.scope_hash,
                event_time_ms: command.event_time_ms,
                index_ref: types::ContextIndexRef {
                    primary_node_hash: index_ref.primary_node_hash,
                    primary_event_time_ms: index_ref.primary_event_time_ms,
                    event_id_hash: index_ref.event_id_hash,
                },
            }
        }
    })
}

/// THE DURABLE ENTITY, RUST TO WIRE. ALL ELEVEN FIELDS.
pub(crate) fn context_entity_record_to_proto(
    entity: &types::ContextEntity,
) -> v1::ContextEntityRecord {
    v1::ContextEntityRecord {
        entity_hash: entity.entity_hash,
        node_hash: entity.node_hash,
        entity_type: entity.entity_type,
        name: entity.name.clone(),
        value: entity.value.clone(),
        updated_at_ms: entity.updated_at_ms,
        valid_from_ms: entity.valid_from_ms,
        confidence: entity.confidence,
        source_event_hashes: entity.source_event_hashes.clone(),
        vector: entity.vector.clone(),
        embedding_model_hash: entity.embedding_model_hash,
    }
}

/// THE DURABLE ENTITY, WIRE TO RUST. ALL ELEVEN FIELDS.
pub(crate) fn context_entity_record_from_proto(
    entity: v1::ContextEntityRecord,
) -> types::ContextEntity {
    types::ContextEntity {
        entity_hash: entity.entity_hash,
        node_hash: entity.node_hash,
        entity_type: entity.entity_type,
        name: entity.name,
        value: entity.value,
        updated_at_ms: entity.updated_at_ms,
        valid_from_ms: entity.valid_from_ms,
        confidence: entity.confidence,
        source_event_hashes: entity.source_event_hashes,
        vector: entity.vector,
        embedding_model_hash: entity.embedding_model_hash,
    }
}

/// THE DURABLE SUMMARY, RUST TO WIRE. ALL SIX FIELDS, GUARD INCLUDED.
///
/// `embedding_model_hash` travels with the vector it describes and is its OWN field, not shared
/// with the node's two or with `ContextSetNodeEmbedding.model_hash`. Zeroing it would disarm the
/// check that stops this summary being scored under a replaced encoder.
pub(crate) fn context_summary_record_to_proto(
    summary: &types::ContextSummary,
) -> v1::ContextSummaryRecord {
    v1::ContextSummaryRecord {
        node_hash: summary.node_hash,
        level: summary.level,
        text: summary.text.clone(),
        valid_from_ms: summary.valid_from_ms,
        vector: summary.vector.clone(),
        embedding_model_hash: summary.embedding_model_hash,
    }
}

/// THE DURABLE SUMMARY, WIRE TO RUST. ALL SIX FIELDS, GUARD INCLUDED.
pub(crate) fn context_summary_record_from_proto(
    summary: v1::ContextSummaryRecord,
) -> types::ContextSummary {
    types::ContextSummary {
        node_hash: summary.node_hash,
        level: summary.level,
        text: summary.text,
        valid_from_ms: summary.valid_from_ms,
        vector: summary.vector,
        embedding_model_hash: summary.embedding_model_hash,
    }
}

/// The durable compression event, Rust to wire.
pub(crate) fn context_compression_event_record_to_proto(
    event: &types::ContextCompressionEvent,
) -> v1::ContextCompressionEventRecord {
    v1::ContextCompressionEventRecord {
        compression_id_hash: event.compression_id_hash,
        node_hash: event.node_hash,
        source_start_ms: event.source_start_ms,
        source_end_ms: event.source_end_ms,
        compressed_time_ms: event.compressed_time_ms,
        summary: event.summary.clone(),
    }
}

/// The durable compression event, wire to Rust.
pub(crate) fn context_compression_event_record_from_proto(
    event: v1::ContextCompressionEventRecord,
) -> types::ContextCompressionEvent {
    types::ContextCompressionEvent {
        compression_id_hash: event.compression_id_hash,
        node_hash: event.node_hash,
        source_start_ms: event.source_start_ms,
        source_end_ms: event.source_end_ms,
        compressed_time_ms: event.compressed_time_ms,
        summary: event.summary,
    }
}

/// One audit ref, Rust to wire.
fn context_audit_ref_to_proto(audit_ref: &types::ContextAuditRef) -> v1::ContextAuditRefRecord {
    v1::ContextAuditRefRecord {
        node_hash: audit_ref.node_hash,
        event_time_ms: audit_ref.event_time_ms,
        reason: audit_ref.reason.clone(),
    }
}

/// One audit ref, wire to Rust.
fn context_audit_ref_from_proto(audit_ref: v1::ContextAuditRefRecord) -> types::ContextAuditRef {
    types::ContextAuditRef {
        node_hash: audit_ref.node_hash,
        event_time_ms: audit_ref.event_time_ms,
        reason: audit_ref.reason,
    }
}

/// THE DURABLE PACK AUDIT, RUST TO WIRE. ALL EIGHT FIELDS.
///
/// The two ref lists stay separate all the way through. Selected and blocked are the two halves of
/// the verdict, and a crossing would reverse every decision the audit records while every field
/// still round-tripped.
pub(crate) fn context_pack_audit_record_to_proto(
    audit: &types::ContextPackAudit,
) -> v1::ContextPackAuditRecord {
    v1::ContextPackAuditRecord {
        query_id: audit.query_id.clone(),
        session_hash: audit.session_hash,
        request_time_ms: audit.request_time_ms,
        query_hash: audit.query_hash,
        max_prompt_tokens: audit.max_prompt_tokens,
        selected_tokens: audit.selected_tokens,
        selected_refs: audit
            .selected_refs
            .iter()
            .map(context_audit_ref_to_proto)
            .collect(),
        blocked_refs: audit
            .blocked_refs
            .iter()
            .map(context_audit_ref_to_proto)
            .collect(),
    }
}

/// THE DURABLE PACK AUDIT, WIRE TO RUST. ALL EIGHT FIELDS.
pub(crate) fn context_pack_audit_record_from_proto(
    audit: v1::ContextPackAuditRecord,
) -> types::ContextPackAudit {
    types::ContextPackAudit {
        query_id: audit.query_id,
        session_hash: audit.session_hash,
        request_time_ms: audit.request_time_ms,
        query_hash: audit.query_hash,
        max_prompt_tokens: audit.max_prompt_tokens,
        selected_tokens: audit.selected_tokens,
        selected_refs: audit
            .selected_refs
            .into_iter()
            .map(context_audit_ref_from_proto)
            .collect(),
        blocked_refs: audit
            .blocked_refs
            .into_iter()
            .map(context_audit_ref_from_proto)
            .collect(),
    }
}

/// THE DURABLE EVENT, RUST TO WIRE. ALL SIXTEEN FIELDS.
///
/// There is no client projection of `ContextEvent` to be tempted by, but the rule is the one
/// `context_node_record_to_proto` below follows: a durable record gets its own message. Seven of
/// the sixteen are `skip_serializing` in the JSON form and are carried anyway, because the WAL's
/// contract is to reproduce the COMMAND rather than the JSON form's write policy.
///
/// `vector` passes through unchanged -- no quantisation.
#[allow(deprecated)]
pub(crate) fn context_event_record_to_proto(
    event: &types::ContextEvent,
) -> v1::ContextEventRecord {
    v1::ContextEventRecord {
        event_id_hash: event.event_id_hash,
        event_time_ms: event.event_time_ms,
        ingestion_time_ms: event.ingestion_time_ms,
        kind: event.kind,
        event_type: event.event_type,
        actor_hash: event.actor_hash,
        status: event.status,
        valid_until_ms: event.valid_until_ms,
        confidence: event.confidence,
        importance: event.importance,
        text: event.text.clone(),
        source_ref: event.source_ref.clone(),
        related_node_hashes: event.related_node_hashes.clone(),
        compact_attrs: event.compact_attrs.clone(),
        vector: event.vector.clone(),
        embedding_model_hash: event.embedding_model_hash,
    }
}

/// THE DURABLE EVENT, WIRE TO RUST. ALL SIXTEEN FIELDS.
#[allow(deprecated)]
pub(crate) fn context_event_record_from_proto(
    event: v1::ContextEventRecord,
) -> types::ContextEvent {
    types::ContextEvent {
        event_id_hash: event.event_id_hash,
        event_time_ms: event.event_time_ms,
        ingestion_time_ms: event.ingestion_time_ms,
        kind: event.kind,
        event_type: event.event_type,
        actor_hash: event.actor_hash,
        status: event.status,
        valid_until_ms: event.valid_until_ms,
        confidence: event.confidence,
        importance: event.importance,
        text: event.text,
        source_ref: event.source_ref,
        related_node_hashes: event.related_node_hashes,
        compact_attrs: event.compact_attrs,
        vector: event.vector,
        embedding_model_hash: event.embedding_model_hash,
    }
}

/// One of the five internal secondary indexes, Rust to wire.
fn internal_context_index_to_proto(
    index: types::InternalContextIndex,
) -> v1::InternalContextIndex {
    match index {
        types::InternalContextIndex::EventKind => v1::InternalContextIndex::EventKind,
        types::InternalContextIndex::Entity => v1::InternalContextIndex::Entity,
        types::InternalContextIndex::Status => v1::InternalContextIndex::Status,
        types::InternalContextIndex::Source => v1::InternalContextIndex::Source,
        types::InternalContextIndex::EventTimeBucket => {
            v1::InternalContextIndex::EventTimeBucket
        }
    }
}

/// One of the five, wire to Rust, with every other number REFUSED.
///
/// A repeated proto enum is open and arrives as `i32`. Mapping an unknown number onto the first
/// variant would switch off whichever index happens to sort first rather than the one asked for,
/// and UNSPECIFIED -- which exists only because proto3 requires a zero value -- is not one of the
/// five at all.
fn internal_context_index_from_proto(
    index: i32,
) -> Result<types::InternalContextIndex, TonicStatus> {
    match v1::InternalContextIndex::try_from(index) {
        Ok(v1::InternalContextIndex::EventKind) => Ok(types::InternalContextIndex::EventKind),
        Ok(v1::InternalContextIndex::Entity) => Ok(types::InternalContextIndex::Entity),
        Ok(v1::InternalContextIndex::Status) => Ok(types::InternalContextIndex::Status),
        Ok(v1::InternalContextIndex::Source) => Ok(types::InternalContextIndex::Source),
        Ok(v1::InternalContextIndex::EventTimeBucket) => {
            Ok(types::InternalContextIndex::EventTimeBucket)
        }
        Ok(v1::InternalContextIndex::Unspecified) | Err(_) => Err(
            TonicStatus::invalid_argument(format!("unknown internal context index {index}")),
        ),
    }
}

/// The extracted-event index block, Rust to wire.
pub(crate) fn context_extracted_event_indexes_to_proto(
    indexes: &types::ContextExtractedEventIndexes,
) -> v1::ContextExtractedEventIndexesRecord {
    v1::ContextExtractedEventIndexesRecord {
        scope_hash: indexes.scope_hash,
        entity_hashes: indexes.entity_hashes.clone(),
        status_hash: indexes.status_hash,
        source_hash: indexes.source_hash,
        event_time_bucket_ms: indexes.event_time_bucket_ms,
        disabled_indexes: indexes
            .disabled_indexes
            .iter()
            .map(|index| internal_context_index_to_proto(*index) as i32)
            .collect(),
    }
}

/// The extracted-event index block, wire to Rust.
pub(crate) fn context_extracted_event_indexes_from_proto(
    indexes: v1::ContextExtractedEventIndexesRecord,
) -> Result<types::ContextExtractedEventIndexes, TonicStatus> {
    let mut disabled_indexes = Vec::with_capacity(indexes.disabled_indexes.len());
    for index in indexes.disabled_indexes {
        disabled_indexes.push(internal_context_index_from_proto(index)?);
    }
    Ok(types::ContextExtractedEventIndexes {
        scope_hash: indexes.scope_hash,
        entity_hashes: indexes.entity_hashes,
        status_hash: indexes.status_hash,
        source_hash: indexes.source_hash,
        event_time_bucket_ms: indexes.event_time_bucket_ms,
        disabled_indexes,
    })
}

/// THE DURABLE NODE, RUST TO WIRE. ALL FIFTEEN FIELDS.
///
/// NOT `sdk_context_node_to_types`, which converts the four-field CLIENT projection and
/// fabricates the rest. This one is field for field, including the three the JSON form no longer
/// writes -- `status`, `l1_ref`, `raw_metadata_ref` are `skip_serializing` but still read for
/// legacy inputs, and the WAL's contract is to reproduce the COMMAND rather than to re-apply the
/// JSON form's write policy.
///
/// Both vectors keep their own model hash. A vector read under the wrong encoder yields no error,
/// only a cosine across two vector spaces, so the hash is the check and dropping it disarms it.
pub(crate) fn context_node_record_to_proto(node: &types::ContextNode) -> v1::ContextNodeRecord {
    v1::ContextNodeRecord {
        node_hash: node.node_hash,
        parent_hash: node.parent_hash,
        kind: node.kind,
        canonical_name: node.canonical_name.clone(),
        l0: node.l0.clone(),
        status: node.status,
        last_event_time_ms: node.last_event_time_ms,
        l1_ref: node.l1_ref.clone(),
        raw_metadata_ref: node.raw_metadata_ref.clone(),
        vector: node.vector.clone(),
        embedding_model_hash: node.embedding_model_hash,
        embedding_updated_at_ms: node.embedding_updated_at_ms,
        summary_vector: node.summary_vector.clone(),
        summary_vector_valid_from_ms: node.summary_vector_valid_from_ms,
        summary_vector_model_hash: node.summary_vector_model_hash,
    }
}

pub(crate) fn context_node_record_from_proto(node: v1::ContextNodeRecord) -> types::ContextNode {
    types::ContextNode {
        node_hash: node.node_hash,
        parent_hash: node.parent_hash,
        kind: node.kind,
        canonical_name: node.canonical_name,
        l0: node.l0,
        status: node.status,
        last_event_time_ms: node.last_event_time_ms,
        l1_ref: node.l1_ref,
        raw_metadata_ref: node.raw_metadata_ref,
        vector: node.vector,
        embedding_model_hash: node.embedding_model_hash,
        embedding_updated_at_ms: node.embedding_updated_at_ms,
        summary_vector: node.summary_vector,
        summary_vector_valid_from_ms: node.summary_vector_valid_from_ms,
        summary_vector_model_hash: node.summary_vector_model_hash,
    }
}

pub(crate) fn string_set_condition_to_proto(
    condition: types::StringSetCondition,
) -> v1::StringSetCondition {
    match condition {
        types::StringSetCondition::Always => v1::StringSetCondition::Always,
        types::StringSetCondition::IfExists => v1::StringSetCondition::IfExists,
        types::StringSetCondition::IfNotExists => v1::StringSetCondition::IfNotExists,
    }
}

/// REFUSED, NOT DEFAULTED. Guessing `Always` would turn a create-only write into one that
/// overwrites a value somebody else put there.
pub(crate) fn string_set_condition_from_proto(
    condition: i32,
) -> Result<types::StringSetCondition, TonicStatus> {
    match v1::StringSetCondition::try_from(condition) {
        Ok(v1::StringSetCondition::Always) => Ok(types::StringSetCondition::Always),
        Ok(v1::StringSetCondition::IfExists) => Ok(types::StringSetCondition::IfExists),
        Ok(v1::StringSetCondition::IfNotExists) => Ok(types::StringSetCondition::IfNotExists),
        Ok(v1::StringSetCondition::Unspecified) | Err(_) => {
            Err(TonicStatus::invalid_argument(format!(
                "string-set condition {condition} is not one this build can apply"
            )))
        }
    }
}

pub(crate) fn feature_write_policy_to_proto(
    policy: types::FeatureWritePolicy,
) -> v1::FeatureWritePolicy {
    match policy {
        types::FeatureWritePolicy::Upsert => v1::FeatureWritePolicy::Upsert,
        types::FeatureWritePolicy::InsertIfAbsent => v1::FeatureWritePolicy::InsertIfAbsent,
        types::FeatureWritePolicy::ReplaceExisting => v1::FeatureWritePolicy::ReplaceExisting,
        types::FeatureWritePolicy::Block => v1::FeatureWritePolicy::Block,
        types::FeatureWritePolicy::MergeReduce => v1::FeatureWritePolicy::MergeReduce,
    }
}

/// REFUSED, NOT DEFAULTED. `Block` and `Upsert` are opposite answers to the same question, so a
/// default would silently pick one of them for a record written by a build that knew better.
pub(crate) fn feature_write_policy_from_proto(
    policy: i32,
) -> Result<types::FeatureWritePolicy, TonicStatus> {
    match v1::FeatureWritePolicy::try_from(policy) {
        Ok(v1::FeatureWritePolicy::Upsert) => Ok(types::FeatureWritePolicy::Upsert),
        Ok(v1::FeatureWritePolicy::InsertIfAbsent) => {
            Ok(types::FeatureWritePolicy::InsertIfAbsent)
        }
        Ok(v1::FeatureWritePolicy::ReplaceExisting) => {
            Ok(types::FeatureWritePolicy::ReplaceExisting)
        }
        Ok(v1::FeatureWritePolicy::Block) => Ok(types::FeatureWritePolicy::Block),
        Ok(v1::FeatureWritePolicy::MergeReduce) => Ok(types::FeatureWritePolicy::MergeReduce),
        Ok(v1::FeatureWritePolicy::Unspecified) | Err(_) => {
            Err(TonicStatus::invalid_argument(format!(
                "feature write policy {policy} is not one this build can apply"
            )))
        }
    }
}

/// THE FAMILY TAG, RUST SPELLING TO WIRE NUMBER.
///
/// Infallible in this direction on purpose: every Rust variant has a number, so an encoder cannot
/// fail here and `command_to_sdk_proto` keeps its `Option` for "no mapping exists" alone rather
/// than overloading it with "the mapping broke".
pub(crate) fn control_state_family_to_proto(
    family: types::ControlStateFamily,
) -> v1::ControlStateFamily {
    match family {
        types::ControlStateFamily::Counter => v1::ControlStateFamily::Counter,
        types::ControlStateFamily::Distinct => v1::ControlStateFamily::Distinct,
        types::ControlStateFamily::Selection => v1::ControlStateFamily::Selection,
    }
}

/// THE FAMILY TAG BACK, AND IT REFUSES RATHER THAN GUESSING.
///
/// Zero is UNSPECIFIED and is not a family. A record carrying it was written by a build that did
/// not know the family, and defaulting it to Counter would file somebody else's distinct-count
/// write under the counter family silently. An unknown number is the same case from the other
/// side -- a record from a NEWER build -- and gets the same refusal, by number, so the message
/// says what was actually on the wire.
pub(crate) fn control_state_family_from_proto(
    family: i32,
) -> Result<types::ControlStateFamily, TonicStatus> {
    match v1::ControlStateFamily::try_from(family) {
        Ok(v1::ControlStateFamily::Counter) => Ok(types::ControlStateFamily::Counter),
        Ok(v1::ControlStateFamily::Distinct) => Ok(types::ControlStateFamily::Distinct),
        Ok(v1::ControlStateFamily::Selection) => Ok(types::ControlStateFamily::Selection),
        Ok(v1::ControlStateFamily::Unspecified) | Err(_) => {
            Err(TonicStatus::invalid_argument(format!(
                "control-state family {family} is not a family this build can file a write under"
            )))
        }
    }
}

pub(crate) fn control_state_selection_type_to_proto(
    selection_type: types::ControlStateSelectionType,
) -> v1::ControlStateSelectionType {
    match selection_type {
        types::ControlStateSelectionType::First => v1::ControlStateSelectionType::First,
        types::ControlStateSelectionType::Last => v1::ControlStateSelectionType::Last,
    }
}

/// Same refusal rule as the family tag: UNSPECIFIED and unknown are both "this build cannot say
/// which end of the window the write keeps", and keeping the wrong end is a silent wrong answer.
pub(crate) fn control_state_selection_type_from_proto(
    selection_type: i32,
) -> Result<types::ControlStateSelectionType, TonicStatus> {
    match v1::ControlStateSelectionType::try_from(selection_type) {
        Ok(v1::ControlStateSelectionType::First) => Ok(types::ControlStateSelectionType::First),
        Ok(v1::ControlStateSelectionType::Last) => Ok(types::ControlStateSelectionType::Last),
        Ok(v1::ControlStateSelectionType::Unspecified) | Err(_) => {
            Err(TonicStatus::invalid_argument(format!(
                "control-state selection type {selection_type} is not one this build can keep"
            )))
        }
    }
}

fn nonzero_limit(limit: u32) -> Option<usize> {
    (limit > 0).then_some(limit as usize)
}

fn sdk_feature_point_to_types(point: v1::FeaturePoint) -> types::FeaturePoint {
    types::FeaturePoint {
        timestamp_ms: point.timestamp_ms,
        value: point.value,
    }
}

fn types_feature_point_to_sdk(point: types::FeaturePoint) -> v1::FeaturePoint {
    v1::FeaturePoint {
        timestamp_ms: point.timestamp_ms,
        value: point.value,
    }
}

fn sdk_context_node_to_types(node: v1::ContextNode) -> types::ContextNode {
    types::ContextNode {
        node_hash: stable_hash(&node.node_id),
        parent_hash: 0,
        kind: 0,
        canonical_name: node.node_id,
        l0: node.model,
        status: 0,
        last_event_time_ms: node.updated_at_ms,
        l1_ref: String::new(),
        raw_metadata_ref: String::from_utf8_lossy(&node.payload).into_owned(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    }
}

fn types_context_node_to_sdk(node: types::ContextNode) -> v1::ContextNode {
    v1::ContextNode {
        node_id: node.canonical_name,
        model: node.l0,
        payload: node.raw_metadata_ref.into_bytes(),
        updated_at_ms: node.last_event_time_ms,
    }
}

pub fn types_execute_response_to_sdk(response: types::ExecuteResponse) -> v1::ExecuteResponse {
    v1::ExecuteResponse {
        status: Some(types_status_to_sdk(response.status)),
        response: Some(types_command_response_to_sdk(response.response)),
        topology_version: 0,
    }
}

pub fn types_batch_response_to_sdk(
    response: types::BatchExecuteResponse,
) -> v1::BatchExecuteResponse {
    v1::BatchExecuteResponse {
        status: Some(types_status_to_sdk(response.status)),
        responses: response
            .responses
            .into_iter()
            .map(types_execute_response_to_sdk)
            .filter_map(|response| response.response)
            .collect(),
        topology_version: 0,
    }
}

pub fn types_command_response_to_sdk(response: types::CommandResponse) -> v1::CommandResponse {
    match response {
        types::CommandResponse::Empty => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            ..Default::default()
        },
        types::CommandResponse::Bytes { value } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: value.unwrap_or_default(),
            ..Default::default()
        },
        types::CommandResponse::Integer { value } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            count: value.max(0) as u64,
            ..Default::default()
        },
        types::CommandResponse::CellMergeReceipt {
            snapshots_written,
            cells_created,
            cells_merged,
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            count: snapshots_written,
            cells_created,
            cells_merged,
            ..Default::default()
        },
        types::CommandResponse::Members { members } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            values: members,
            ..Default::default()
        },
        types::CommandResponse::Values { values } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            values: values
                .into_iter()
                .map(|value| value.unwrap_or_default())
                .collect(),
            ..Default::default()
        },
        types::CommandResponse::HashEntries { entries } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            values: entries.into_iter().map(|(_, value)| value).collect(),
            ..Default::default()
        },
        types::CommandResponse::FeaturePoints { points } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            feature_points: points.into_iter().map(types_feature_point_to_sdk).collect(),
            ..Default::default()
        },
        types::CommandResponse::SequenceRows { rows } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            sequence_rows: rows
                .into_iter()
                .map(|row| v1::SequenceFeatureRow {
                    timestamp_ms: row.timestamp_ms,
                    gid: row.gid,
                    action_type: row.action_type,
                    duration: row.duration,
                    author_id: row.author_id,
                })
                .collect(),
            ..Default::default()
        },
        types::CommandResponse::ContextNode { node, .. } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            context_nodes: node.into_iter().map(types_context_node_to_sdk).collect(),
            ..Default::default()
        },
        other => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::error(
                "unsupported_sdk_response",
                format!("SDK response conversion missing for {other:?}"),
            ))),
            ..Default::default()
        },
    }
}

fn types_status_to_sdk(status: types::Status) -> v1::Status {
    v1::Status {
        ok: status.ok,
        code: status.code,
        message: status.message,
    }
}

fn error_status(code: impl Into<String>, message: impl Into<String>) -> v1::Status {
    types_status_to_sdk(types::Status::error(code, message))
}

fn stable_hash(value: &str) -> u64 {
    crate::client::stable_key_hash(value)
}

fn table_to_sdk_topology(
    client: &TemporalStoreClient,
    table: &TemporalStoreTable,
) -> v1::TableTopology {
    let options = table.options();
    let cache = client.topology_cache_report();
    let mut shards = Vec::new();
    for index in 0..options.shard_count {
        let shard_id = options.first_shard_id.saturating_add(index);
        let route = cache.routes.iter().find(|route| route.shard_id == shard_id);
        shards.push(v1::ShardTopology {
            shard_id,
            primary: route.and_then(|route| sdk_endpoint_from_addr(&route.primary_addr)),
            replicas: Vec::new(),
            load_generation: route
                .map(|route| route.topology_version)
                .unwrap_or_default(),
            lifecycle_state: "serving".to_string(),
        });
    }
    v1::TableTopology {
        namespace_name: table.namespace().to_string(),
        table_name: table.table_name().to_string(),
        state: "serving".to_string(),
        readonly: false,
        write_disabled: false,
        drop_percent: options.drop_percent as u32,
        topology_version: cache.max_topology_version,
        shards,
    }
}

fn sdk_endpoint_from_addr(addr: &str) -> Option<v1::ServerEndpoint> {
    let (host, port) = addr.rsplit_once(':')?;
    Some(v1::ServerEndpoint {
        server_id: addr.to_string(),
        host: host.to_string(),
        port: port.parse::<u32>().unwrap_or_default(),
        location: String::new(),
    })
}

fn split_table_key(key: &str) -> Option<(String, String)> {
    key.split_once('/')
        .or_else(|| key.split_once('.'))
        .map(|(namespace, table_name)| (namespace.to_string(), table_name.to_string()))
}

fn client_error_execute_response(err: ClientError) -> types::ExecuteResponse {
    types::ExecuteResponse {
        status: client_error_status(err),
        response: types::CommandResponse::Empty,
    }
}

fn client_error_status_to_sdk(err: ClientError) -> v1::Status {
    types_status_to_sdk(client_error_status(err))
}

fn client_error_status(err: ClientError) -> types::Status {
    let code = client_error_code(&err);
    types::Status::error(code, err.to_string())
}

fn client_error_code(err: &ClientError) -> &'static str {
    match err {
        ClientError::Http(_) => "http_error",
        ClientError::Status(_) => "status_error",
        ClientError::InvalidRequest(_) => "invalid_request",
        ClientError::UnexpectedResponse { .. } => "unexpected_response",
        // Distinct on purpose: the caller has to be able to tell a write that provably did not
        // apply from one that may have. Retrying the first is free; retrying the second is how
        // a write gets applied twice.
        ClientError::WriteOutcomeUnknown(_) => "write_outcome_unknown",
    }
}

#[cfg(test)]
mod tests {
    use tonic::Request;

    use crate::engine::TemporalEngine;
    use matrixcache::MultiLayerCache;

    use super::v1::{
        temporal_store_service_client::TemporalStoreServiceClient,
        temporal_store_service_server::TemporalStoreService, BatchExecuteRequest,
        ClientPreflightRequest, Command, ExecuteRequest, OpenTableRequest, SyncTopologyRequest,
    };
    use super::*;

    #[test]
    fn generated_sdk_bindings_cover_required_client_surface() {
        let execute = ExecuteRequest {
            shard_id: 7,
            trace_id: 42,
            command: Some(Command {
                kind: Some(super::v1::command::Kind::StringGet(super::v1::StringGet {
                    key: "sdk-key".to_string(),
                })),
            }),
        };
        assert_eq!(execute.shard_id, 7);
        assert!(execute.command.is_some());

        let batch = BatchExecuteRequest {
            shard_id: 7,
            trace_id: 43,
            commands: vec![execute.command.expect("command")],
        };
        assert_eq!(batch.commands.len(), 1);

        let open = OpenTableRequest {
            namespace_name: "default".to_string(),
            table_name: "table".to_string(),
            local_location: "local".to_string(),
        };
        assert_eq!(open.table_name, "table");

        let sync = SyncTopologyRequest {
            table_keys: vec!["default.table".to_string()],
            min_topology_version: 3,
            deadline_ms: 200,
        };
        assert_eq!(sync.deadline_ms, 200);

        let preflight = ClientPreflightRequest {
            include_routes: true,
            include_backend_failures: true,
        };
        assert!(preflight.include_backend_failures);

        let _client_type =
            std::any::type_name::<TemporalStoreServiceClient<tonic::transport::Channel>>();
        let _server_trait = std::any::type_name::<dyn TemporalStoreService>();
    }

    #[tokio::test]
    async fn tonic_adapter_delegates_execute_to_engine_path() {
        let engine = TemporalEngine::new(MultiLayerCache::default());
        engine.load_shard(1);
        let adapter = TemporalStoreTonicAdapter::new(engine);
        let set = ExecuteRequest {
            shard_id: 1,
            trace_id: 10,
            command: Some(Command {
                kind: Some(v1::command::Kind::StringSet(v1::StringSet {
                    key: "sdk-adapter-key".to_string(),
                    value: b"value".to_vec(),
                    ttl_ms: 0,
                })),
            }),
        };
        let set_response = adapter
            .execute(Request::new(set))
            .await
            .expect("set response")
            .into_inner();
        assert!(set_response.status.expect("status").ok);

        let get = ExecuteRequest {
            shard_id: 1,
            trace_id: 11,
            command: Some(Command {
                kind: Some(v1::command::Kind::StringGet(v1::StringGet {
                    key: "sdk-adapter-key".to_string(),
                })),
            }),
        };
        let get_response = adapter
            .execute(Request::new(get))
            .await
            .expect("get response")
            .into_inner();
        assert_eq!(get_response.response.expect("response").value, b"value");
    }

    #[tokio::test]
    async fn tonic_adapter_delegates_batch_execute_to_engine_path() {
        let engine = TemporalEngine::new(MultiLayerCache::default());
        engine.load_shard(1);
        let adapter = TemporalStoreTonicAdapter::new(engine);
        let response = adapter
            .batch_execute(Request::new(BatchExecuteRequest {
                shard_id: 1,
                trace_id: 12,
                commands: vec![
                    Command {
                        kind: Some(v1::command::Kind::StringSet(v1::StringSet {
                            key: "sdk-batch-key".to_string(),
                            value: b"batch".to_vec(),
                            ttl_ms: 0,
                        })),
                    },
                    Command {
                        kind: Some(v1::command::Kind::StringGet(v1::StringGet {
                            key: "sdk-batch-key".to_string(),
                        })),
                    },
                ],
            }))
            .await
            .expect("batch response")
            .into_inner();
        assert!(response.status.expect("status").ok);
        assert_eq!(response.responses.len(), 2);
        assert_eq!(response.responses[1].value, b"batch");
    }

    #[tokio::test]
    async fn tonic_adapter_exposes_client_open_sync_and_preflight_paths() {
        let client = TemporalStoreClient::with_options(crate::ClientOptions {
            default_shard_id: 9,
            ..crate::ClientOptions::proxy("127.0.0.1:1")
        });
        client.insert_cached_route_for_test(1, "127.0.0.1:19009");
        let adapter = TemporalStoreTonicAdapter::new(client);

        let open = adapter
            .open_table(Request::new(OpenTableRequest {
                namespace_name: "default".to_string(),
                table_name: "sdk_table".to_string(),
                local_location: "local".to_string(),
            }))
            .await
            .expect("open table")
            .into_inner();
        assert!(open.status.expect("open status").ok);
        let topology = open.topology.expect("open topology");
        assert_eq!(topology.namespace_name, "default");
        assert_eq!(topology.table_name, "sdk_table");
        assert_eq!(topology.shards[0].shard_id, 1);
        assert_eq!(
            topology.shards[0]
                .primary
                .as_ref()
                .expect("primary endpoint")
                .port,
            19009
        );

        let sync = adapter
            .sync_topology(Request::new(SyncTopologyRequest {
                table_keys: vec!["default/sdk_table".to_string()],
                min_topology_version: 0,
                deadline_ms: 100,
            }))
            .await
            .expect("sync topology")
            .into_inner();
        assert!(sync.status.expect("sync status").ok);
        assert_eq!(sync.topologies.len(), 1);

        let preflight = adapter
            .get_client_preflight(Request::new(ClientPreflightRequest {
                include_routes: true,
                include_backend_failures: true,
            }))
            .await
            .expect("client preflight")
            .into_inner();
        assert_eq!(preflight.route_cache_entries, 1);
        assert_eq!(preflight.table_cache_entries, 1);
    }
}
