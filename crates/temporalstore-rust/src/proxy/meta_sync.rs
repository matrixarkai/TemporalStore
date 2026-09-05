// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

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
        post_json_with_options_and_headers::<_, TopologyVersionReport>(
            &options.meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            &crate::meta::admin_auth_header(),
            options.control_http_options(),
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
            boot_time_ms: self.inner.boot_time_ms,
            namespace: options.namespace.clone(),
            config_version: proxy_config_version(&options),
            binary_version: options.binary_version.clone(),
        };
        match post_json_with_options_and_headers::<_, ProxyHeartbeatResponse>(
            &options.meta_addr,
            "/proxies/heartbeat",
            &request,
            &crate::meta::admin_auth_header(),
            options.control_http_options(),
        ) {
            Ok(response) if response.status.ok || response.status.code == "resource_frozen" => {
                // The metaserver is reachable and answering, so this is the cheapest moment to
                // learn how many shards the cluster has.
                self.refresh_cluster_shard_count();
                self.record_service_discovery_heartbeat(&response.status);
                self.apply_heartbeat_config(&response);
                response
            }
            Ok(response) if response.status.code == "not_found" => {
                // The metaserver does not know this proxy, and registering again is the right
                // answer -- but not on every single heartbeat. A metaserver that keeps saying
                // not_found would otherwise take a registration plus a second heartbeat from
                // every proxy, every interval, which is the heaviest load at the worst moment.
                if !self.auto_register_is_due() {
                    self.inner
                        .stats
                        .write()
                        .expect("proxy stats lock poisoned")
                        .auto_register_throttled += 1;
                    self.record_service_discovery_error(&response.status);
                    return response;
                }
                if self.auto_register_proxy(&options).status.ok {
                    let response = post_json_with_options_and_headers::<_, ProxyHeartbeatResponse>(
                        &options.meta_addr,
                        "/proxies/heartbeat",
                        &request,
                        &crate::meta::admin_auth_header(),
                        options.control_http_options(),
                    )
                    .unwrap_or_else(|err| ProxyHeartbeatResponse {
                        status: Status::error("metaserver_error", err.to_string()),
                        config_changed: false,
                        namespace: String::new(),
                        config_version: 0,
                        serving_mode: "not_serving".to_string(),
                        drop_percent: None,
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
                // The metaserver REACHED us and said no -- the proxy was dropped, re-scoped,
                // or is otherwise not the owner of what it thinks it owns. Drop the local
                // namespace/config authority so it stops acting on a grant that has been
                // withdrawn; the next accepted heartbeat hands back the real one, and until
                // then reporting an empty namespace is what makes the metaserver mark the
                // config changed and re-send it.
                //
                // Note this is the EXPLICIT-rejection branch only. A transport failure lands
                // in `Err` below and deliberately changes nothing: an unreachable or slow
                // metaserver must not cost every proxy its configuration.
                self.clear_config_authority();
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
                    drop_percent: None,
                }
            }
        }
    }

    pub fn start_heartbeat_loop(&self) -> thread::JoinHandle<()> {
        let service = self.clone();
        thread::spawn(move || loop {
            // Read per pass, not captured: pushing a new interval should not need a restart.
            let interval = Duration::from_millis(
                service
                    .with_options(|options| options.heartbeat_interval_ms)
                    .max(1),
            );
            let started = std::time::Instant::now();
            let _ = service.heartbeat_to_meta();
            let elapsed = started.elapsed();
            // Fixed RATE, not a fixed gap. Sleeping a whole interval AFTER the beat makes the
            // real period `interval + beat_duration`, so the heartbeat rate degrades exactly
            // when the metaserver is slow -- which is precisely when the liveness margin
            // matters. With a 5s control-plane budget one slow beat would stretch a 10s
            // interval to 15s and eat the margin the metaserver allows before declaring this
            // proxy dead.
            //
            // A beat slower than the whole interval sleeps not at all, matching the reference
            // loop. That does not hammer a struggling metaserver: the request rate is bounded
            // by the beat duration itself, which is what is already slow.
            if let Some(remaining) = interval.checked_sub(elapsed) {
                thread::sleep(remaining);
            } else {
                service
                    .inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .heartbeat_slow_total += 1;
            }
        })
    }

    pub(super) fn auto_register_proxy(&self, options: &ProxyOptions) -> AckResponse {
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .auto_register_total += 1;
        let response = post_json_with_options_and_headers::<_, AckResponse>(
            &options.meta_addr,
            "/proxies/register",
            &RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: options.proxy_addr.clone(),
                namespace: options.namespace.clone(),
                location: options.location.clone(),
                config_version: proxy_config_version(options),
                binary_version: options.binary_version.clone(),
            },
            &crate::meta::admin_auth_header(),
            options.control_http_options(),
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
                || response
                    .drop_percent
                    .is_some_and(|percent| percent <= 100 && percent != options.drop_percent)
        };
        if !response.config_changed && !policy_changed {
            return;
        }
        let mut options = self.options_owned();
        if !response.namespace.is_empty() {
            options.namespace = response.namespace.clone();
        }
        if response.config_version != 0 {
            options.config_version = response.config_version;
        }
        if let Some(serving_mode) = serving_mode {
            options.serving_mode = serving_mode;
        }
        // Only when the metaserver actually spoke for it. The three fields above are each
        // guarded the same way -- namespace when non-empty, config_version when non-zero,
        // serving_mode when it parses -- and this one was not, so every heartbeat carried a
        // hardcoded 0 into the field an operator sets to drain the proxy.
        if let Some(percent) = response.drop_percent {
            if percent <= 100 {
                options.drop_percent = percent;
            }
        }
        let _ = self.update_options_report(options);
    }

    /// Forget the namespace/config this proxy believes it was granted. Called when the
    /// metaserver explicitly rejects a heartbeat. Serving policy is deliberately left alone:
    /// the metaserver drives that through `serving_mode`, and a rejection is not by itself an
    /// instruction to start or stop serving.
    pub(super) fn clear_config_authority(&self) {
        let options = self.options();
        if options.namespace.is_empty() && options.config_version == 0 {
            return;
        }
        let mut cleared = (*options).clone();
        cleared.namespace = String::new();
        cleared.config_version = 0;
        let _ = self.update_options_report(cleared);
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
