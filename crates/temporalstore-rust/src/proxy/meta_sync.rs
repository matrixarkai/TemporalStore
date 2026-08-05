//! ProxyService meta topology refresh + heartbeat/service-discovery, split from proxy.rs.
use super::*;

impl ProxyService {
    pub fn refresh_topology_from_meta(&self) -> ProxyTopologyRefreshResponse {
        match self.client().refresh_stale_routes_from_meta() {
            Ok(report) => ProxyTopologyRefreshResponse {
                status: report.status.clone(),
                report: Some(report),
            },
            Err(err) => {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .metaserver_errors += 1;
                ProxyTopologyRefreshResponse {
                    status: Status::error("refresh_failed", err.to_string()),
                    report: None,
                }
            }
        }
    }

    pub(super) fn fetch_meta_topology_version(
        &self,
        old_topology_version: u64,
    ) -> Result<TopologyVersionReport, Status> {
        let options = self.options();
        post_json_with_options::<_, TopologyVersionReport>(
            &options.meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            options.http_options(),
        )
        .map_err(|err| Status::error("topology_check_failed", err.to_string()))
    }

    pub fn heartbeat_to_meta(&self) -> ProxyHeartbeatResponse {
        let options = self.options();
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .heartbeat_total += 1;
        let request = ProxyHeartbeatRequest {
            proxy_addr: options.proxy_addr.clone(),
            namespace: options.namespace.clone(),
            config_version: proxy_config_version(&options),
            binary_version: options.binary_version.clone(),
        };
        match post_json_with_options::<_, ProxyHeartbeatResponse>(
            &options.meta_addr,
            "/proxies/heartbeat",
            &request,
            options.http_options(),
        ) {
            Ok(response) if response.status.ok || response.status.code == "resource_frozen" => {
                self.record_service_discovery_heartbeat(&response.status);
                self.apply_heartbeat_config(&response);
                response
            }
            Ok(response) if response.status.code == "not_found" => {
                if self.auto_register_proxy(&options).status.ok {
                    let response = post_json_with_options::<_, ProxyHeartbeatResponse>(
                        &options.meta_addr,
                        "/proxies/heartbeat",
                        &request,
                        options.http_options(),
                    )
                    .unwrap_or_else(|err| ProxyHeartbeatResponse {
                        status: Status::error("metaserver_error", err.to_string()),
                        config_changed: false,
                        namespace: String::new(),
                        config_version: 0,
                        serving_mode: "not_serving".to_string(),
                        drop_percent: 0,
                    });
                    if response.status.ok {
                        self.record_service_discovery_heartbeat(&response.status);
                        self.apply_heartbeat_config(&response);
                    } else {
                        self.record_service_discovery_error(&response.status);
                    }
                    response
                } else {
                    self.record_service_discovery_error(&response.status);
                    response
                }
            }
            Ok(response) => {
                self.record_service_discovery_error(&response.status);
                response
            }
            Err(err) => {
                let status = Status::error("metaserver_error", err.to_string());
                self.record_service_discovery_error(&status);
                ProxyHeartbeatResponse {
                    status,
                    config_changed: false,
                    namespace: String::new(),
                    config_version: 0,
                    serving_mode: "not_serving".to_string(),
                    drop_percent: 0,
                }
            }
        }
    }

    pub fn start_heartbeat_loop(&self, interval_ms: u64) -> thread::JoinHandle<()> {
        let service = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            let _ = service.heartbeat_to_meta();
            thread::sleep(interval);
        })
    }

    pub(super) fn auto_register_proxy(&self, options: &ProxyOptions) -> AckResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .auto_register_total += 1;
        let response = post_json_with_options::<_, AckResponse>(
            &options.meta_addr,
            "/proxies/register",
            &RegisterProxyRequest {
                proxy_addr: options.proxy_addr.clone(),
                namespace: options.namespace.clone(),
                location: options.location.clone(),
                config_version: proxy_config_version(options),
                binary_version: options.binary_version.clone(),
            },
            options.http_options(),
        )
        .unwrap_or_else(|err| AckResponse {
            status: Status::error("metaserver_error", err.to_string()),
        });
        self.record_service_discovery_registration(&response.status);
        response
    }

    pub(super) fn apply_heartbeat_config(&self, response: &ProxyHeartbeatResponse) {
        let serving_mode = proxy_serving_mode_from_meta(&response.serving_mode);
        let policy_changed = {
            let options = self.options();
            serving_mode.is_some_and(|mode| mode != options.serving_mode)
                || response.drop_percent <= 100 && response.drop_percent != options.drop_percent
        };
        if !response.config_changed && !policy_changed {
            return;
        }
        let mut options = self.options();
        if !response.namespace.is_empty() {
            options.namespace = response.namespace.clone();
        }
        if response.config_version != 0 {
            options.config_version = response.config_version;
        }
        if let Some(serving_mode) = serving_mode {
            options.serving_mode = serving_mode;
        }
        if response.drop_percent <= 100 {
            options.drop_percent = response.drop_percent;
        }
        let _ = self.update_options_report(options);
    }

    pub(super) fn record_service_discovery_heartbeat(&self, status: &Status) {
        let mut state = self
            .inner
            .service_discovery
            .write()
            .expect("proxy service discovery lock poisoned");
        if status.ok || status.code == "resource_frozen" {
            state.registered = true;
            state.last_success_ms = Some(now_ms());
            state.last_error = None;
            state.stats.heartbeat_success_total += 1;
        } else {
            state.last_error_ms = Some(now_ms());
            state.last_error = Some(status.clone());
            state.stats.heartbeat_failure_total += 1;
        }
    }

    pub(super) fn record_service_discovery_registration(&self, status: &Status) {
        let mut state = self
            .inner
            .service_discovery
            .write()
            .expect("proxy service discovery lock poisoned");
        if status.ok {
            state.registered = true;
            state.last_success_ms = Some(now_ms());
            state.last_error = None;
            state.stats.registration_success_total += 1;
        } else {
            state.registered = false;
            state.last_error_ms = Some(now_ms());
            state.last_error = Some(status.clone());
            state.stats.registration_failure_total += 1;
        }
    }

    pub(super) fn record_service_discovery_error(&self, status: &Status) {
        let mut state = self
            .inner
            .service_discovery
            .write()
            .expect("proxy service discovery lock poisoned");
        state.last_error_ms = Some(now_ms());
        state.last_error = Some(status.clone());
        state.stats.heartbeat_failure_total += 1;
    }

}
