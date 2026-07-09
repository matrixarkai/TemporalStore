pub mod v1 {
    tonic::include_proto!("temporalstore.v1");
}

use std::sync::Arc;

use serde::Serialize;
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

    fn open_table_sdk(&self, request: v1::OpenTableRequest) -> v1::OpenTableResponse {
        v1::OpenTableResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            topology: Some(local_sdk_topology(
                request.namespace_name,
                request.table_name,
                request.local_location,
            )),
        }
    }

    fn sync_topology_sdk(&self, request: v1::SyncTopologyRequest) -> v1::SyncTopologyResponse {
        let mut topologies = Vec::new();
        let mut failures = Vec::new();
        for key in request.table_keys {
            match split_table_key(&key) {
                Some((namespace, table_name)) => {
                    topologies.push(local_sdk_topology(namespace, table_name, String::new()));
                }
                None => failures.push(format!("{key}:invalid_table_key")),
            }
        }
        let status = if failures.is_empty() {
            types::Status::ok()
        } else {
            types::Status::error("partial_sync_topology", failures.join(","))
        };
        v1::SyncTopologyResponse {
            status: Some(types_status_to_sdk(status)),
            topologies,
            topology_version: 1,
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
        v1::command::Kind::HashDelete(command) => types::Command::HashDelete {
            key: command.key,
            field: command.field,
        },
        v1::command::Kind::HashGetAll(command) => types::Command::HashGetAll { key: command.key },
        v1::command::Kind::HashLen(command) => types::Command::HashLen { key: command.key },
        v1::command::Kind::HashMultiSet(command) => types::Command::HashMultiSet {
            key: command.key,
            entries: command
                .entries
                .into_iter()
                .map(|entry| (entry.field, entry.value))
                .collect(),
        },
        v1::command::Kind::HashMultiGet(command) => types::Command::HashMultiGet {
            key: command.key,
            fields: command.fields,
        },
        v1::command::Kind::SetAdd(command) => types::Command::SetAdd {
            key: command.key,
            member: command.member,
        },
        v1::command::Kind::SetRemove(command) => types::Command::SetRemove {
            key: command.key,
            member: command.member,
        },
        v1::command::Kind::SetMembers(command) => types::Command::SetMembers { key: command.key },
        v1::command::Kind::FeatureAppend(command) => types::Command::FeatureAppend {
            key: command.key,
            points: command
                .points
                .into_iter()
                .map(sdk_feature_point_to_types)
                .collect(),
        },
        v1::command::Kind::FeatureQuery(command) => types::Command::FeatureQuery {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            count: nonzero_limit(command.limit),
        },
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
        v1::command::Kind::FeatureDelete(command) => {
            types::Command::FeatureDelete { key: command.key }
        }
        v1::command::Kind::FeatureAggQuery(command) => types::Command::FeatureAggQuery {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            aggregator: command.aggregator,
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
        v1::command::Kind::SequenceBatchQuery(command) => types::Command::SequenceBatchQuery {
            queries: command
                .queries
                .into_iter()
                .map(|query| types::SequenceQuerySpec {
                    key: query.key,
                    start_ms: query.start_ms,
                    end_ms: query.end_ms,
                    count: query.limit.max(1) as usize,
                    filters: Vec::new(),
                })
                .collect(),
        },
        v1::command::Kind::IpsAdd(command) => types::Command::IpsAdd {
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            instance: command.payload,
        },
        v1::command::Kind::IpsAddWithOptions(command) => types::Command::IpsAddWithOptions {
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            instance: command.payload,
            action_type: command.action_type,
            table_id: command.table_id,
            request_id: (!command.request_id.is_empty()).then_some(command.request_id),
        },
        v1::command::Kind::IpsQuery(command) => types::Command::IpsQueryRange {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            count: nonzero_limit(command.limit),
        },
        v1::command::Kind::IpsQueryLast(command) => types::Command::IpsQueryLast {
            key: command.key,
            count: command.limit.max(1) as usize,
        },
        v1::command::Kind::RiskIncrement(command) => types::Command::RiskIncrement {
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            amount: command.delta,
        },
        v1::command::Kind::RiskIncrementWithOptions(command) => {
            types::Command::RiskIncrementWithOptions {
                key: command.key,
                timestamp_ms: command.timestamp_ms,
                amount: command.delta,
                precision_ms: command.precision_ms,
                ttl_ms: command.ttl_ms,
            }
        }
        v1::command::Kind::RiskCount(command) => types::Command::RiskCount {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
        },
        v1::command::Kind::RiskChangeAdd(command) => types::Command::RiskChangeAdd {
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            value: command.value,
            precision_ms: command.precision_ms,
            ttl_ms: command.ttl_ms,
        },
        v1::command::Kind::RiskDetail(command) => types::Command::RiskDetail {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            count: nonzero_limit(command.limit),
        },
        v1::command::Kind::RiskQuery(command) => types::Command::RiskQuery {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            aggregator: command.family,
        },
        v1::command::Kind::RiskFamilySet(command) => types::Command::RiskSet {
            family: sdk_risk_family_to_types(command.family)?,
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            amount: command.amount,
        },
        v1::command::Kind::RiskFamilyQuery(command) => types::Command::RiskFamilyQuery {
            family: sdk_risk_family_to_types(command.family)?,
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            aggregator: command.aggregator,
        },
        v1::command::Kind::RiskFamilySetAndGet(command) => types::Command::RiskSetAndGet {
            family: sdk_risk_family_to_types(command.family)?,
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            amount: command.amount,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            aggregator: command.aggregator,
        },
        v1::command::Kind::RiskFolSet(command) => types::Command::RiskFolSet {
            key: command.key,
            value: command.value,
            occur_time_ms: command.occur_time_ms,
            ttl_ms: command.ttl_ms,
            fol_type: sdk_risk_fol_type_to_types(command.fol_type)?,
        },
        v1::command::Kind::RiskFolQuery(command) => {
            types::Command::RiskFolQuery { key: command.key }
        }
        v1::command::Kind::RiskManager(command) => types::Command::RiskManager { key: command.key },
        v1::command::Kind::ContextNodeUpsert(command) => {
            let node = command
                .node
                .ok_or_else(|| TonicStatus::invalid_argument("context node missing"))?;
            types::Command::ContextUpsertNode {
                tenant_hash: stable_hash(&command.key),
                node: sdk_context_node_to_types(node),
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
        v1::command::Kind::CommonTtl(command) => types::Command::CommonTtl { key: command.key },
        v1::command::Kind::CommonExists(command) => {
            types::Command::CommonExists { key: command.key }
        }
    })
}

fn nonzero_limit(limit: u32) -> Option<usize> {
    (limit > 0).then_some(limit as usize)
}

fn sdk_risk_family_to_types(family: i32) -> Result<types::RiskFamily, TonicStatus> {
    match v1::RiskFamily::try_from(family) {
        Ok(v1::RiskFamily::H) => Ok(types::RiskFamily::H),
        Ok(v1::RiskFamily::Cpc) => Ok(types::RiskFamily::Cpc),
        Ok(v1::RiskFamily::Fol) => Ok(types::RiskFamily::Fol),
        Ok(v1::RiskFamily::Unspecified) | Err(_) => Err(TonicStatus::invalid_argument(
            "risk family missing or invalid",
        )),
    }
}

fn sdk_risk_fol_type_to_types(fol_type: i32) -> Result<types::RiskFolType, TonicStatus> {
    match v1::RiskFolType::try_from(fol_type) {
        Ok(v1::RiskFolType::First) => Ok(types::RiskFolType::First),
        Ok(v1::RiskFolType::Last) => Ok(types::RiskFolType::Last),
        Ok(v1::RiskFolType::Unspecified) | Err(_) => Err(TonicStatus::invalid_argument(
            "risk fol_type missing or invalid",
        )),
    }
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
        summary_dirty: false,
        l1_ref: String::new(),
        raw_metadata_ref: String::from_utf8_lossy(&node.payload).into_owned(),
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
        types::CommandResponse::FeaturePointGroups { groups } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            feature_points: groups
                .into_iter()
                .flat_map(|(_, points)| points)
                .map(types_feature_point_to_sdk)
                .collect(),
            ..Default::default()
        },
        types::CommandResponse::Aggregate { value } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            count: value.max(0) as u64,
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
        types::CommandResponse::SequenceRowGroups { groups } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            sequence_rows: groups
                .into_iter()
                .flat_map(|(_, rows)| rows)
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
        types::CommandResponse::ContextNodes { nodes } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            context_nodes: nodes.into_iter().map(types_context_node_to_sdk).collect(),
            ..Default::default()
        },
        types::CommandResponse::ContextObjectKey { object_key } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            ..Default::default()
        },
        types::CommandResponse::ContextExtractedEventWrite {
            event_object_key,
            index_object_keys,
            written_index_count,
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: event_object_key.into_bytes(),
            values: index_object_keys
                .into_iter()
                .map(String::into_bytes)
                .collect(),
            count: written_index_count as u64,
            ..Default::default()
        },
        types::CommandResponse::ContextEvents { object_key, events } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: events.len() as u64,
            values: json_values(events),
            ..Default::default()
        },
        types::CommandResponse::ContextIndexRefs { object_key, refs } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: refs.len() as u64,
            values: json_values(refs),
            ..Default::default()
        },
        types::CommandResponse::ContextIndexIntersection {
            refs,
            deduped_ref_count,
            ..
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            count: deduped_ref_count as u64,
            values: json_values(refs),
            ..Default::default()
        },
        types::CommandResponse::ContextPackAudits { object_key, audits } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: audits.len() as u64,
            values: json_values(audits),
            ..Default::default()
        },
        types::CommandResponse::ContextSummaryDirtyMarkers {
            object_key,
            markers,
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: markers.len() as u64,
            values: json_values(markers),
            ..Default::default()
        },
        types::CommandResponse::ContextEntity { object_key, entity } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: u64::from(entity.is_some()),
            values: entity.into_iter().map(json_bytes).collect(),
            ..Default::default()
        },
        types::CommandResponse::ContextEntities {
            object_key,
            entities,
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: entities.len() as u64,
            values: json_values(entities),
            ..Default::default()
        },
        types::CommandResponse::ContextChildRefs {
            object_key, refs, ..
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: refs.len() as u64,
            values: json_values(refs),
            ..Default::default()
        },
        types::CommandResponse::ContextEmbeddings { embeddings } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            count: embeddings.len() as u64,
            values: json_values(embeddings),
            ..Default::default()
        },
        types::CommandResponse::ContextTraversedNodes { nodes } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            count: nodes.len() as u64,
            values: json_values(nodes),
            ..Default::default()
        },
        types::CommandResponse::ContextSummaries {
            object_key,
            summaries,
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: summaries.len() as u64,
            values: json_values(summaries),
            ..Default::default()
        },
        types::CommandResponse::ContextCompressionEvents {
            object_key, events, ..
        } => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            value: object_key.into_bytes(),
            count: events.len() as u64,
            values: json_values(events),
            ..Default::default()
        },
        types::CommandResponse::ContextNodeContext {
            node,
            overall_summary,
            cold_window_summaries,
            ..
        } => {
            let mut values = Vec::new();
            if let Some(summary) = overall_summary {
                values.push(json_bytes(summary));
            }
            values.extend(json_values(cold_window_summaries));
            v1::CommandResponse {
                status: Some(types_status_to_sdk(types::Status::ok())),
                context_nodes: node.into_iter().map(types_context_node_to_sdk).collect(),
                count: values.len() as u64,
                values,
                ..Default::default()
            }
        }
        other => v1::CommandResponse {
            status: Some(types_status_to_sdk(types::Status::error(
                "unsupported_sdk_response",
                format!("SDK response conversion missing for {other:?}"),
            ))),
            ..Default::default()
        },
    }
}

fn json_values<T: Serialize>(values: Vec<T>) -> Vec<Vec<u8>> {
    values.into_iter().map(json_bytes).collect()
}

fn json_bytes<T: Serialize>(value: T) -> Vec<u8> {
    serde_json::to_vec(&value).unwrap_or_default()
}

fn types_status_to_sdk(status: types::Status) -> v1::Status {
    v1::Status {
        ok: status.ok,
        code: status.code,
        message: status.message,
    }
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

fn local_sdk_topology(
    namespace_name: String,
    table_name: String,
    local_location: String,
) -> v1::TableTopology {
    v1::TableTopology {
        namespace_name,
        table_name,
        state: "serving".to_string(),
        readonly: false,
        write_disabled: false,
        drop_percent: 0,
        topology_version: 1,
        shards: vec![v1::ShardTopology {
            shard_id: 1,
            primary: Some(v1::ServerEndpoint {
                server_id: "local-engine".to_string(),
                host: "local-engine".to_string(),
                port: 0,
                location: local_location,
            }),
            replicas: Vec::new(),
            load_generation: 1,
            lifecycle_state: "serving".to_string(),
        }],
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
    }
}

#[cfg(test)]
mod tests {
    use tonic::Request;

    use crate::engine::TemporalEngine;
    use rustmtcache::MultiLayerCache;

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
    async fn tonic_adapter_exposes_engine_open_and_sync_topology_paths() {
        let engine = TemporalEngine::new(MultiLayerCache::default());
        let adapter = TemporalStoreTonicAdapter::new(engine);

        let open = adapter
            .open_table(Request::new(OpenTableRequest {
                namespace_name: "local".to_string(),
                table_name: "embedded".to_string(),
                local_location: "dev-zone".to_string(),
            }))
            .await
            .expect("open table")
            .into_inner();
        assert!(open.status.expect("open status").ok);
        let topology = open.topology.expect("open topology");
        assert_eq!(topology.namespace_name, "local");
        assert_eq!(topology.table_name, "embedded");
        assert_eq!(topology.topology_version, 1);
        assert_eq!(
            topology.shards[0].primary.as_ref().unwrap().location,
            "dev-zone"
        );

        let sync = adapter
            .sync_topology(Request::new(SyncTopologyRequest {
                table_keys: vec!["local/embedded".to_string()],
                min_topology_version: 0,
                deadline_ms: 100,
            }))
            .await
            .expect("sync topology")
            .into_inner();
        assert!(sync.status.expect("sync status").ok);
        assert_eq!(sync.topologies.len(), 1);
        assert_eq!(sync.topologies[0].namespace_name, "local");
        assert_eq!(sync.topologies[0].table_name, "embedded");
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
