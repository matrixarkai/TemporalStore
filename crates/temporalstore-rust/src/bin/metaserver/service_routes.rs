// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Master/manage/heartbeat/query service-route handlers, split from metaserver.rs
// (textual include!, shared flat scope + use-imports; no mod wrapper).

fn default_master_replica_count() -> u64 {
    1
}

fn master_add_table_request(req: MasterCreateTableRequest) -> AddTableRequest {
    let shard_count = if req.shard_count > 0 {
        req.shard_count
    } else if req.partition_set_num > 0 {
        req.partition_set_num
    } else {
        req.table_options
            .as_ref()
            .map(|options| options.partition_num)
            .unwrap_or_default()
            .max(1)
    };
    AddTableRequest {
        namespace: req.namespace,
        table_name: req.table_name,
        first_shard_id: req.first_shard_id,
        shard_count,
        replica_count: req.replica_count.max(1),
        partition_version: req.partition_version,
        serving_options: req
            .table_options
            .as_ref()
            .map(MasterTableOptionsRequest::serving_options)
            .unwrap_or_default(),
    }
}

fn master_update_table_request(req: MasterUpdateTableRequest) -> UpdateTableRequest {
    let shard_count = req.shard_count.or_else(|| {
        req.table_options
            .as_ref()
            .and_then(|options| (options.partition_num > 0).then_some(options.partition_num))
    });
    UpdateTableRequest {
        namespace: req.namespace,
        table_name: req.table_name,
        shard_count,
        replica_count: req.replica_count,
        first_shard_id: req.first_shard_id,
        partition_version: req.partition_version,
        serving_options: req
            .table_options
            .as_ref()
            .map(MasterTableOptionsRequest::serving_options_patch),
    }
}

fn master_delete_table_request(req: MasterTableRequest) -> DeleteTableRequest {
    DeleteTableRequest {
        namespace: req.namespace,
        table_name: req.table_name,
    }
}

fn handle_manage_service_route(
    meta: &MetaBackend,
    request: &HttpRequest,
) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/ManageService/AddServer") => {
            parse_or(&request.body, |req: HeartbeatServerRequest| {
                backend_call!(
                    meta,
                    register_server,
                    RegisterServerRequest {
                        registered_at_ms: 0,
                        numa_nodes: Vec::new(),
                        server_addr: heartbeat_server_addr(&req),
                        node_id: 0,
                        location: req.location,
                        binary_version: req.binary_version,
                    }
                )
            })
        }
        ("POST", "/ManageService/FreezeServer") => {
            parse_or(&request.body, |req: HeartbeatServerRequest| {
                backend_call!(
                    meta,
                    freeze_server,
                    StateChangeRequest {
                        endpoint: heartbeat_server_addr(&req),
                        freeze_cooldown_ms: 0,
                        reason: FreezeReason::Operator,
                    }
                )
            })
        }
        ("POST", "/ManageService/DropServer") => {
            parse_or(&request.body, |req: HeartbeatServerRequest| {
                backend_call!(
                    meta,
                    drop_server,
                    StateChangeRequest {
                        endpoint: heartbeat_server_addr(&req),
                        freeze_cooldown_ms: 0,
                        reason: FreezeReason::Operator,
                    }
                )
            })
        }
        ("POST", "/ManageService/AddProxy") => {
            parse_or(&request.body, |req: HeartbeatProxyRequest| {
                let proxy_addr = heartbeat_proxy_addr(&req);
                let namespace = if req.namespace_name.is_empty() {
                    req.namespace
                } else {
                    req.namespace_name
                };
                backend_call!(
                    meta,
                    register_proxy,
                    RegisterProxyRequest {
                        registered_at_ms: 0,
                        proxy_addr,
                        namespace,
                        location: req.location,
                        config_version: req.config_version,
                        binary_version: req.binary_version,
                    }
                )
            })
        }
        ("POST", "/ManageService/FreezeProxy") => {
            parse_or(&request.body, |req: HeartbeatProxyRequest| {
                backend_call!(
                    meta,
                    freeze_proxy,
                    StateChangeRequest {
                        reason: FreezeReason::Unspecified,
                        endpoint: heartbeat_proxy_addr(&req),
                        freeze_cooldown_ms: 0,
                    }
                )
            })
        }
        ("POST", "/ManageService/DropProxy") => {
            parse_or(&request.body, |req: HeartbeatProxyRequest| {
                backend_call!(
                    meta,
                    drop_proxy,
                    StateChangeRequest {
                        reason: FreezeReason::Unspecified,
                        endpoint: heartbeat_proxy_addr(&req),
                        freeze_cooldown_ms: 0,
                    }
                )
            })
        }
        ("POST", "/ManageService/AddNamespace") => {
            parse_or(&request.body, |req: ManageNamespaceRequest| {
                backend_call!(
                    meta,
                    add_namespace,
                    AddNamespaceRequest {
                        namespace: req.namespace,
                    }
                )
            })
        }
        ("POST", "/ManageService/AddTable") => {
            parse_or(&request.body, |req: MasterCreateTableRequest| {
                backend_call!(meta, add_table, master_add_table_request(req))
            })
        }
        ("POST", "/ManageService/UpdateTable") => {
            parse_or(&request.body, |req: MasterUpdateTableRequest| {
                backend_call!(meta, update_table, master_update_table_request(req))
            })
        }
        ("POST", "/ManageService/FreezeTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                backend_call!(meta, freeze_table, master_delete_table_request(req))
            })
        }
        ("POST", "/ManageService/DropTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                backend_call!(meta, delete_table, master_delete_table_request(req))
            })
        }
        _ => return None,
    };
    Some(response)
}

fn handle_query_service_route(meta: &MetaBackend, request: &HttpRequest) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/QueryService/QueryManageInfo") | ("POST", "/QueryService/QueryManageInfo") => {
            json_response(200, &backend_call!(meta, info))
        }
        ("GET", "/QueryService/QueryClusterStatus")
        | ("POST", "/QueryService/QueryClusterStatus") => {
            json_response(200, &backend_call!(meta, preflight_report))
        }
        ("GET", "/QueryService/ListServer") | ("POST", "/QueryService/ListServer") => {
            json_response(200, &backend_call!(meta, list_servers))
        }
        ("GET", "/QueryService/ListProxy") | ("POST", "/QueryService/ListProxy") => {
            json_response(200, &backend_call!(meta, list_proxies))
        }
        ("GET", "/QueryService/ListNamespace") | ("POST", "/QueryService/ListNamespace") => {
            json_response(200, &backend_call!(meta, list_namespaces))
        }
        ("GET", "/QueryService/ListTable") | ("POST", "/QueryService/ListTable") => {
            json_response(200, &backend_call!(meta, list_tables))
        }
        _ => return None,
    };
    Some(response)
}

fn handle_heartbeat_service_route(
    meta: &MetaBackend,
    request: &HttpRequest,
) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/HeartbeatService/ServerHeartbeat") => {
            parse_or(&request.body, |req: HeartbeatServerRequest| {
                backend_call!(
                    meta,
                    server_heartbeat,
                    ServerHeartbeatRequest {
                        server_addr: heartbeat_server_addr(&req),
                        boot_time_ms: heartbeat_boot_time_ms(req.boot_time_ms, req.boot_time_us),
                        binary_version: req.binary_version,
                        shard_loads: Vec::new(),
                        shard_stat_loads: Vec::new(),
                        runtime_load: Default::default(),
                        shard_states: Vec::new(),
                    }
                )
            })
        }
        ("POST", "/HeartbeatService/ServerNotifyStop") => {
            parse_or(&request.body, |req: HeartbeatServerRequest| {
                let endpoint = heartbeat_server_addr(&req);
                backend_call!(
                    meta,
                    drop_server,
                    StateChangeRequest {
                        reason: FreezeReason::Unspecified,
                        endpoint,
                        freeze_cooldown_ms: 0,
                    }
                )
            })
        }
        ("POST", "/HeartbeatService/ProxyHeartbeat") => {
            parse_or(&request.body, |req: HeartbeatProxyRequest| {
                let proxy_addr = heartbeat_proxy_addr(&req);
                let namespace = if req.namespace_name.is_empty() {
                    req.namespace
                } else {
                    req.namespace_name
                };
                backend_call!(
                    meta,
                    proxy_heartbeat,
                    ProxyHeartbeatRequest {
                        proxy_addr,
                        boot_time_ms: req.boot_time_us / 1_000,
                        namespace,
                        config_version: req.config_version,
                        binary_version: req.binary_version,
                    }
                )
            })
        }
        ("POST", "/HeartbeatService/ProxyNotifyStop") => {
            parse_or(&request.body, |req: HeartbeatProxyRequest| {
                let endpoint = heartbeat_proxy_addr(&req);
                backend_call!(
                    meta,
                    drop_proxy,
                    StateChangeRequest {
                        reason: FreezeReason::Unspecified,
                        endpoint,
                        freeze_cooldown_ms: 0,
                    }
                )
            })
        }
        _ => return None,
    };
    Some(response)
}

fn heartbeat_server_addr(request: &HeartbeatServerRequest) -> String {
    if !request.server_addr.is_empty() {
        request.server_addr.clone()
    } else {
        heartbeat_addr(&request.host, request.port, request.endpoint.as_ref())
    }
}

fn heartbeat_proxy_addr(request: &HeartbeatProxyRequest) -> String {
    if !request.proxy_addr.is_empty() {
        request.proxy_addr.clone()
    } else {
        heartbeat_addr(&request.host, request.port, request.endpoint.as_ref())
    }
}

fn heartbeat_addr(host: &str, port: u32, endpoint: Option<&HeartbeatEndpoint>) -> String {
    if let Some(endpoint) = endpoint {
        let host = if !endpoint.ip4.is_empty() {
            &endpoint.ip4
        } else {
            &endpoint.ip6
        };
        if endpoint.port > 0 {
            return format!("{}:{}", host, endpoint.port);
        }
        return host.to_string();
    }
    if port > 0 {
        format!("{host}:{port}")
    } else {
        host.to_string()
    }
}

fn heartbeat_boot_time_ms(boot_time_ms: u64, boot_time_us: u64) -> u64 {
    if boot_time_ms > 0 {
        boot_time_ms
    } else {
        boot_time_us / 1000
    }
}

fn handle_master_service_route(
    meta: &MetaBackend,
    request: &HttpRequest,
) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/MasterService/CreateTable") => {
            parse_or(&request.body, |req: MasterCreateTableRequest| {
                let shard_count = if req.shard_count > 0 {
                    req.shard_count
                } else {
                    req.table_options
                        .as_ref()
                        .map(|options| options.partition_num)
                        .unwrap_or_default()
                        .max(1)
                };
                backend_call!(
                    meta,
                    add_table,
                    AddTableRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                        first_shard_id: req.first_shard_id,
                        shard_count,
                        replica_count: req.replica_count.max(1),
                        partition_version: req.partition_version,
                        serving_options: req
                            .table_options
                            .as_ref()
                            .map(MasterTableOptionsRequest::serving_options)
                            .unwrap_or_default(),
                    }
                )
            })
        }
        ("POST", "/MasterService/DeleteTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                backend_call!(
                    meta,
                    delete_table,
                    DeleteTableRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                    }
                )
            })
        }
        ("POST", "/MasterService/UpdateTable") | ("POST", "/MasterService/AlterTable") => {
            parse_or(&request.body, |req: MasterUpdateTableRequest| {
                let shard_count = req.shard_count.or_else(|| {
                    req.table_options.as_ref().and_then(|options| {
                        (options.partition_num > 0).then_some(options.partition_num)
                    })
                });
                backend_call!(
                    meta,
                    update_table,
                    UpdateTableRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                        shard_count,
                        replica_count: req.replica_count,
                        first_shard_id: req.first_shard_id,
                        partition_version: req.partition_version,
                        serving_options: req
                            .table_options
                            .as_ref()
                            .map(MasterTableOptionsRequest::serving_options_patch),
                    }
                )
            })
        }
        ("POST", "/MasterService/OpenTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                // Only the version is read out of this, so it does not ask
                // for the shard list.
                let topology = backend_call!(
                    meta,
                    get_table_topology,
                    GetTableTopologyRequest::status_only(req.namespace, req.table_name)
                );
                MasterOpenTableResponse {
                    open_version: topology
                        .table
                        .as_ref()
                        .map(|table| table.topology_version)
                        .unwrap_or_default(),
                    status: topology.status,
                }
            })
        }
        ("POST", "/MasterService/CloseTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                // Only the status is read out of this, so it does not ask
                // for the shard list.
                let topology = backend_call!(
                    meta,
                    get_table_topology,
                    GetTableTopologyRequest::status_only(req.namespace, req.table_name)
                );
                AckResponse {
                    status: topology.status,
                }
            })
        }
        ("POST", "/MasterService/GetTableTopo") => {
            parse_or(&request.body, |req: MasterGetTableTopoRequest| {
                backend_call!(
                    meta,
                    get_table_topology,
                    GetTableTopologyRequest {
                        // The caller has been sending this on every topology
                        // request all along; it was being dropped here.
                        client_location: req.idc,
                        namespace: req.namespace,
                        table_name: req.table_name,
                        old_topology_version: req.old_topology_version.max(req.old_topo_version),
                    }
                )
            })
        }
        ("POST", "/MasterService/RegisterServer") => {
            parse_or(&request.body, |req: MasterRegisterServerRequest| {
                let server_addr = master_server_addr(&req);
                backend_call!(
                    meta,
                    register_server,
                    RegisterServerRequest {
                        registered_at_ms: 0,
                        numa_nodes: Vec::new(),
                        server_addr,
                        node_id: req.node_id,
                        location: req.location,
                        binary_version: req.binary_version,
                    }
                )
            })
        }
        ("GET", "/MasterService/GetInfo") => json_response(200, &backend_call!(meta, info)),
        ("GET", "/MasterService/Preflight") => {
            json_response(200, &backend_call!(meta, preflight_report))
        }
        ("POST", "/MasterService/UnRegisterServer") => {
            parse_or(&request.body, |req: MasterRegisterServerRequest| {
                backend_call!(
                    meta,
                    drop_server,
                    StateChangeRequest {
                        reason: FreezeReason::Unspecified,
                        endpoint: master_server_addr(&req),
                        freeze_cooldown_ms: 0,
                    }
                )
            })
        }
        _ => return None,
    };
    Some(response)
}

fn master_server_addr(request: &MasterRegisterServerRequest) -> String {
    if !request.server_addr.is_empty() {
        request.server_addr.clone()
    } else if request.port > 0 {
        format!("{}:{}", request.host, request.port)
    } else {
        request.host.clone()
    }
}
