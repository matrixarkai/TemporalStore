# Cross Storage, Control, And Agent Parity

Shared case: `cross_storage_control_agent_parity`.

This case is the umbrella parity contract for workflows that cross storage/cache,
client/proxy routing, data-node lifecycle, metaserver scheduling, and Context agent
resource/skill parsing. It does not duplicate the family-specific C++ source-path gates.
Those remain owned by the lower-level cases:

- Storage/cache: `storage_dump_load_recovery`, `storage_fault_matrix`,
  `storage_follower_safe_gc`, `storage_cache_refill`,
  `storage_shared_store_sync_replay`, and `storage_shared_store_async_replay`.
- Client/proxy/control plane: `control_topology_version_change`,
  `control_stale_route_invalidation`, `control_proxy_admission_policy`,
  `control_route_quarantine_recovery`, `control_data_node_load_reload_unload_lifecycle`,
  and `control_metaserver_scheduler_lifecycle_workflow`.
- Context agent workflow: `context_resource_skill_parser_openviking_parity`,
  `context_management_ingest_retrieve_pipeline`, and
  `context_compression_secondary_index_query_debug_flow`.

## Rust Evidence

The Rust validator requires concrete implementation evidence across:

- `engine.rs`: slot dump manifests, lifecycle reports, and recovery boundary reports.
- `client.rs`: stale-route refresh, safe write retry, and background MetaSyncer handles.
- `proxy.rs`: topology refresh, admission policy, and Rust-native C++ migration contract.
- `data_node.rs`: persisted lifecycle snapshots and write barriers during transitions.
- `metaserver.rs`: scheduler token restore and load/reload/unload lifecycle callbacks.
- `context_workflow.rs`: resource/skill parser, chunk embeddings, and agent skill metadata.

## C++ Status

The current C++ side is a static surface gate until a native cross-subsystem corpus runner exists.
The lower-level shared cases still list the exact C++ storage, client, proxy, server,
metaserver, and context source files. This umbrella case prevents the parity report from
claiming end-to-end readiness from isolated subsystem checks alone.
