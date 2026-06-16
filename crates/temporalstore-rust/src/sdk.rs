pub mod v1 {
    tonic::include_proto!("temporalstore.v1");
}

use std::sync::Arc;

use tonic::{Request, Response, Status as TonicStatus};

use crate::engine::TemporalEngine;
use crate::types;

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
        v1::command::Kind::IpsAdd(command) => types::Command::IpsAdd {
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            instance: command.payload,
        },
        v1::command::Kind::IpsQuery(command) => types::Command::IpsQueryRange {
            key: command.key,
            start_ms: command.start_ms,
            end_ms: command.end_ms,
            count: nonzero_limit(command.limit),
        },
        v1::command::Kind::RiskIncrement(command) => types::Command::RiskIncrement {
            key: command.key,
            timestamp_ms: command.timestamp_ms,
            amount: command.delta,
        },
        v1::command::Kind::RiskQuery(command) => types::Command::RiskQuery {
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
        v1::command::Kind::CommonExists(command) => {
            types::Command::CommonExists { key: command.key }
        }
    })
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

#[cfg(test)]
mod tests {
    use tonic::Request;

    use crate::{engine::TemporalEngine, MultiLayerCache};

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
}
