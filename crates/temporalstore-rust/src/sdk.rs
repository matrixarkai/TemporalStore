pub mod v1 {
    tonic::include_proto!("temporalstore.v1");
}

#[cfg(test)]
mod tests {
    use super::v1::{
        temporal_store_service_client::TemporalStoreServiceClient,
        temporal_store_service_server::TemporalStoreService, BatchExecuteRequest,
        ClientPreflightRequest, Command, ExecuteRequest, OpenTableRequest, SyncTopologyRequest,
    };

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
}
