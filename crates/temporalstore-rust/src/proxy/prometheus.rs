// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ProxyService::prometheus_metrics, split from proxy.rs.
use super::*;

impl ProxyService {
    pub fn prometheus_metrics(&self) -> String {
        self.sync_client_stats();
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        let client = self.client().stats();
        let mut out = String::new();
        out.push_str("# HELP temporalstore_proxy_requests_total Proxy request counters by kind.\n");
        out.push_str("# TYPE temporalstore_proxy_requests_total counter\n");
        for (kind, value) in [
            ("execute", stats.execute_requests),
            ("batch_execute", stats.batch_execute_requests),
            ("bad_request", stats.bad_requests),
            ("admission_rejection", stats.admission_rejections),
            ("heartbeat", stats.heartbeat_total),
            ("auto_register", stats.auto_register_total),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_requests_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str(
            "# HELP temporalstore_proxy_route_cache_entries Current proxy route cache entries.\n",
        );
        out.push_str("# TYPE temporalstore_proxy_route_cache_entries gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_route_cache_entries",
            &[],
            self.client().route_cache_size() as u64,
        );

        out.push_str("# HELP temporalstore_proxy_route_cache_events_total Proxy route cache and refresh events.\n");
        out.push_str("# TYPE temporalstore_proxy_route_cache_events_total counter\n");
        for (kind, value) in [
            ("hit", stats.route_cache_hits),
            ("miss", stats.route_cache_misses),
            ("refresh", stats.route_refreshes),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_route_cache_events_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str("# HELP temporalstore_proxy_backend_events_total Proxy backend and metaserver failure counters.\n");
        out.push_str("# TYPE temporalstore_proxy_backend_events_total counter\n");
        for (kind, value) in [
            ("backend_error", stats.backend_errors),
            (
                "continuous_backend_failure",
                stats.continuous_backend_failures,
            ),
            ("metaserver_error", stats.metaserver_errors),
            ("client_meta_sync_error", client.meta_sync_errors),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_backend_events_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str("# HELP temporalstore_proxy_serving_mode Current proxy serving mode as a one-hot gauge.\n");
        out.push_str("# TYPE temporalstore_proxy_serving_mode gauge\n");
        for mode in [
            ProxyServingMode::Serving,
            ProxyServingMode::Readonly,
            ProxyServingMode::WriteDisabled,
            ProxyServingMode::Degraded,
            ProxyServingMode::NotServing,
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_serving_mode",
                &[("mode", proxy_serving_mode_label(mode))],
                u64::from(options.serving_mode == mode),
            );
        }

        out.push_str("# HELP temporalstore_proxy_drop_percent Current proxy deterministic drop percentage.\n");
        out.push_str("# TYPE temporalstore_proxy_drop_percent gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_drop_percent",
            &[],
            options.drop_percent as u64,
        );

        out.push_str("# HELP temporalstore_proxy_metric_family_parity proxy operational metric surface mapped to Rust Prometheus families.\n");
        out.push_str("# TYPE temporalstore_proxy_metric_family_parity gauge\n");
        for mapping in proxy_metrics_parity_mappings() {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_metric_family_parity",
                &[
                    ("cpp_surface", mapping.cpp_surface.as_str()),
                    ("rust_family", mapping.rust_prometheus_family.as_str()),
                    ("grafana_panel", mapping.grafana_panel.as_str()),
                ],
                u64::from(mapping.covered),
            );
        }

        let service_discovery = self.service_discovery_report_with_options(&options);
        out.push_str("# HELP temporalstore_proxy_service_registry_state Proxy service-discovery registration state.\n");
        out.push_str("# TYPE temporalstore_proxy_service_registry_state gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_service_registry_state",
            &[("state", "registered")],
            u64::from(service_discovery.registered),
        );
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_service_registry_state",
            &[("state", "stale")],
            u64::from(service_discovery.stale),
        );

        out.push_str("# HELP temporalstore_proxy_service_registry_events_total Proxy service-discovery heartbeat and registration events.\n");
        out.push_str("# TYPE temporalstore_proxy_service_registry_events_total counter\n");
        for (kind, value) in [
            (
                "heartbeat_success",
                service_discovery.stats.heartbeat_success_total,
            ),
            (
                "heartbeat_failure",
                service_discovery.stats.heartbeat_failure_total,
            ),
            (
                "registration_success",
                service_discovery.stats.registration_success_total,
            ),
            (
                "registration_failure",
                service_discovery.stats.registration_failure_total,
            ),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_service_registry_events_total",
                &[("kind", kind)],
                value,
            );
        }

        let readiness = crate::production_readiness_report();
        out.push_str(
            "# HELP temporalstore_production_readiness_ready Production readiness gate state.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_ready gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_production_readiness_ready",
            &[],
            u64::from(readiness.production_ready),
        );

        out.push_str(
            "# HELP temporalstore_production_readiness_blockers Production readiness blockers.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_blockers gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_production_readiness_blockers",
            &[],
            readiness.blocker_count as u64,
        );
        for area in &readiness.areas {
            push_proxy_metric(
                &mut out,
                "temporalstore_production_readiness_blockers",
                &[("area", area.area.as_str())],
                area.missing.len() as u64,
            );
        }
        out.push_str(
            "# HELP temporalstore_production_readiness_service_ready Production readiness by service.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_service_ready gauge\n");
        out.push_str(
            "# HELP temporalstore_production_readiness_service_blockers Production readiness blockers by service.\n",
        );
        out.push_str("# TYPE temporalstore_production_readiness_service_blockers gauge\n");
        for service in &readiness.service_summaries {
            push_proxy_metric(
                &mut out,
                "temporalstore_production_readiness_service_ready",
                &[("service", service.service.as_str())],
                u64::from(service.ready),
            );
            push_proxy_metric(
                &mut out,
                "temporalstore_production_readiness_service_blockers",
                &[("service", service.service.as_str())],
                service.blocker_count as u64,
            );
        }
        out
    }

}
