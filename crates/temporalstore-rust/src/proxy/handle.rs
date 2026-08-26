// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ProxyService::handle request dispatcher, split from proxy.rs.
use super::*;

impl ProxyService {
    pub fn handle(&self, request: HttpRequest) -> (u16, Vec<u8>) {
        use crate::http::{json_response, parse_json};
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", "/metrics") | ("GET", "/ProxyService/Metrics") => {
                (200, self.prometheus_metrics().into_bytes())
            }
            ("GET", "/readiness") => {
                let (status, response) = self.readiness_response();
                json_response(status, &response)
            }
            ("GET", "/proxy/info") | ("GET", "/ProxyService/GetInfo") => {
                json_response(200, &self.info())
            }
            ("GET", "/proxy/heartbeat") | ("GET", "/ProxyService/Heartbeat") => {
                json_response(200, &self.heartbeat_report())
            }
            ("GET", "/proxy/preflight") | ("GET", "/ProxyService/Preflight") => {
                json_response(200, &self.preflight_report())
            }
            ("GET", "/proxy/policy") | ("GET", "/ProxyService/GetPolicy") => {
                json_response(200, &self.policy_report())
            }
            ("GET", "/proxy/tonic_contract") | ("GET", "/ProxyService/GetTonicContract") => {
                json_response(200, &self.tonic_streaming_contract())
            }
            ("GET", "/proxy/metrics_parity") | ("GET", "/ProxyService/GetMetricsParity") => {
                json_response(200, &self.metrics_parity_report())
            }
            ("GET", "/proxy/native_migration_contract")
            | ("GET", "/ProxyService/GetMigrationContract") => {
                json_response(200, &self.native_migration_contract())
            }
            ("GET", "/proxy/service_discovery") | ("GET", "/ProxyService/GetServiceDiscovery") => {
                json_response(200, &self.service_discovery_report())
            }
            ("GET", "/proxy/ports") | ("GET", "/ProxyService/GetPorts") => {
                json_response(200, &self.ports_report())
            }
            ("GET", "/proxy/consul_names") | ("GET", "/ProxyService/GetConsulNames") => {
                json_response(200, &self.consul_names_report())
            }
            ("POST", "/proxy/notify_stop") | ("POST", "/ProxyService/NotifyStop") => {
                json_response(200, &self.notify_stop_report())
            }
            ("GET", "/proxy/operational_surface")
            | ("GET", "/ProxyService/GetOperationalSurface") => {
                json_response(200, &self.operational_surface_report())
            }
            ("GET", "/proxy/client_preflight") | ("GET", "/ProxyService/ClientPreflight") => {
                json_response(200, &self.client().preflight_report())
            }
            ("POST", "/proxy/topology/refresh") | ("POST", "/ProxyService/RefreshTopology") => {
                match self.admit_topology_refresh() {
                    Ok(_admitted) => json_response(200, &self.refresh_topology_from_meta()),
                    Err(status) => {
                        json_response(proxy_rejection_http_status(&status.code), &status)
                    }
                }
            }
            ("GET", "/proxy/config") | ("GET", "/ProxyService/GetConfig") => {
                let options = self
                    .inner
                    .options
                    .read()
                    .expect("proxy options lock poisoned")
                    .clone();
                json_response(200, &*options)
            }
            ("POST", "/proxy/config") | ("POST", "/ProxyService/UpdateConfig") => {
                match self.merge_config_push(&request.body) {
                    Ok(options) => json_response(200, &self.update_options_report(options)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err))
                    }
                }
            }
            ("GET", path) if path.starts_with("/shards/") => {
                let _admitted = match self.admit_shard_lookup() {
                    Ok(guard) => guard,
                    Err(status) => {
                        return json_response(proxy_rejection_http_status(&status.code), &status)
                    }
                };
                let shard_id = path
                    .trim_start_matches("/shards/")
                    .parse()
                    .unwrap_or_default();
                match self.get_shard(shard_id, true) {
                    Ok(response) => json_response(200, &response),
                    Err(status) => json_response(502, &status),
                }
            }
            ("POST", "/execute") | ("POST", "/ProxyService/ExecuteCmd") => {
                match parse_json::<ExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &execute_error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/batch_execute") | ("POST", "/ProxyService/BatchExecuteCmd") => {
                match parse_json::<BatchExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.batch_execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/proxy/open_table")
            | ("POST", "/tables/open")
            | ("POST", "/ProxyService/OpenTable") => {
                match parse_json::<ProxyOpenTableRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.open_table(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(
                            400,
                            &ProxyOpenTableResponse {
                                status: Status::error("bad_request", err.to_string()),
                                options: None,
                            },
                        )
                    }
                }
            }
            ("POST", "/proxy/table_execute")
            | ("POST", "/table_execute")
            | ("POST", "/ProxyService/TableExecuteCmd")
            | ("POST", "/ProxyService/ExecuteTableCmd") => {
                match parse_json::<ProxyTableExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.table_execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &execute_error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/proxy/table_batch_execute")
            | ("POST", "/table_batch_execute")
            | ("POST", "/ProxyService/TableBatchExecuteCmd")
            | ("POST", "/ProxyService/BatchExecuteTableCmd") => {
                match parse_json::<ProxyTableBatchExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &self.table_batch_execute(req)),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/ProxyService/Get") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::StringGet { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Set") => {
                match parse_json::<ProxySetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::StringSet {
                            key: req.key,
                            value: req.value,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SetEx") => {
                match parse_json::<ProxySetExCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::StringSetEx {
                            key: req.key,
                            value: req.value,
                            ttl_ms: req.ttl_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Delete") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::CommonDelete { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Expire") => {
                match parse_json::<ProxyExpireCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::CommonExpire {
                            key: req.key,
                            ttl_ms: req.ttl_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Ttl") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::CommonTtl { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/Exists") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::CommonExists { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureAdd") => {
                match parse_json::<ProxyFeatureAddCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = if let Some(policy) = req.policy {
                            Command::FeatureAppendWithPolicy {
                                key: req.key,
                                points: req.points,
                                policy,
                            }
                        } else {
                            Command::FeatureAppend {
                                key: req.key,
                                points: req.points,
                            }
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureQuery") => {
                match parse_json::<ProxyFeatureQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = if req.filters.is_empty() {
                            Command::FeatureQuery {
                                key: req.key,
                                start_ms: req.start_ms,
                                end_ms: req.end_ms,
                                count: req.count,
                            }
                        } else {
                            Command::FeatureQueryFiltered {
                                key: req.key,
                                start_ms: req.start_ms,
                                end_ms: req.end_ms,
                                count: req.count,
                                filters: req.filters,
                            }
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureReplace") => {
                match parse_json::<ProxyFeatureReplaceCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::FeatureReplace {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            points: req.points,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureDelete") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::FeatureDelete { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/FeatureAggQuery") => {
                match parse_json::<ProxyFeatureAggQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::FeatureAggQuery {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            aggregator: req.aggregator,
                            count: req.count,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SequenceAdd") => {
                match parse_json::<ProxySequenceAddCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SequenceAdd {
                            key: req.key,
                            rows: req.rows,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SequenceQuery") => {
                match parse_json::<ProxySequenceQueryCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SequenceQuery {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                            count: req.count,
                            filters: req.filters,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/ControlStateIncrement") => {
                match parse_json::<ProxyControlStateIncrementCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = if req.precision_ms.is_some() || req.ttl_ms.is_some() {
                            Command::ControlStateIncrementWithOptions {
                                key: req.key,
                                timestamp_ms: req.timestamp_ms,
                                amount: req.amount,
                                precision_ms: req.precision_ms,
                                ttl_ms: req.ttl_ms,
                            }
                        } else {
                            Command::ControlStateIncrement {
                                key: req.key,
                                timestamp_ms: req.timestamp_ms,
                                amount: req.amount,
                            }
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/ControlStateCount") => {
                match parse_json::<ProxyControlStateCountCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::ControlStateCount {
                            key: req.key,
                            start_ms: req.start_ms,
                            end_ms: req.end_ms,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/ControlStateHset") => {
                match parse_json::<ProxyControlStateHsetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::ControlStateSet {
                            family: crate::types::ControlStateFamily::Counter,
                            key: req.key,
                            timestamp_ms: req.timestamp_ms,
                            amount: req.amount,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/ControlStateSelectionSet")
            | ("POST", "/ProxyService/ControlStateFolSet") => {
                match parse_json::<ProxyControlStateSelectionSetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::ControlStateSelectionSet {
                            key: req.key,
                            value: req.value,
                            occur_time_ms: req.occur_time_ms,
                            ttl_ms: req.ttl_ms,
                            selection_type: req.selection_type,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/ControlStateSelectionQuery")
            | ("POST", "/ProxyService/ControlStateFolQuery") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::ControlStateSelectionQuery { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/ControlStateManager") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::ControlStateManager {
                            key,
                            op_type: None,
                            field_list: Vec::new(),
                            start_offset: String::new(),
                            end_offset: String::new(),
                            is_distinct: false,
                        }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HGet") => {
                match parse_json::<ProxyHashCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashGet {
                            key: req.key,
                            field: req.field,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HSet") => {
                match parse_json::<ProxyHashSetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashSet {
                            key: req.key,
                            field: req.field,
                            value: req.value,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HDel") => {
                match parse_json::<ProxyHashCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashDelete {
                            key: req.key,
                            field: req.field,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HMGet") => {
                match parse_json::<ProxyHashMultiGetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashMultiGet {
                            key: req.key,
                            fields: req.fields,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HMSet") => {
                match parse_json::<ProxyHashMultiSetCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::HashMultiSet {
                            key: req.key,
                            entries: req.entries,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HGetAll") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::HashGetAll { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/HLen") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::HashLen { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SAdd") => {
                match parse_json::<ProxySetMemberCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SetAdd {
                            key: req.key,
                            member: req.member,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SMembers") => {
                match parse_json::<ProxyKeyCommandRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &self.table_command(req, |key| Command::SetMembers { key }),
                    ),
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/ProxyService/SRem") => {
                match parse_json::<ProxySetMemberCommandRequest>(&request.body) {
                    Ok(req) => {
                        let command = Command::SetRemove {
                            key: req.key,
                            member: req.member,
                        };
                        json_response(
                            200,
                            &self.table_execute(ProxyTableExecuteRequest {
                                namespace: req.namespace,
                                table_name: req.table_name,
                                command,
                            }),
                        )
                    }
                    Err(err) => self.bad_execute_request(err),
                }
            }
            ("POST", "/context/ingest") | ("POST", "/ProxyService/ContextIngest") => {
                match parse_json::<context::ProxyContextIngestRequest>(&request.body) {
                    Ok(req) => self.context_ingest(req),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/context/extract")
            | ("POST", "/context/ingest_extract")
            | ("POST", "/ProxyService/ContextExtract") => {
                match parse_json::<context::ProxyContextIngestRequest>(&request.body) {
                    Ok(req) => self.context_extract(req),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            ("POST", "/context/retrieve") | ("POST", "/ProxyService/ContextRetrieve") => {
                match parse_json::<context::ProxyContextRetrieveRequest>(&request.body) {
                    Ok(req) => self.context_retrieve(req),
                    Err(err) => {
                        self.inc_bad_request();
                        json_response(400, &Status::error("bad_request", err.to_string()))
                    }
                }
            }
            _ => json_response(404, &Status::error("not_found", "unknown proxy route")),
        }
    }
}
