// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{control_state_precision_ms, control_state_window_ms};
use crate::{ControlStatePrecision, ControlStateWindow, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_cpc_set(
        &self,
        key: &str,
        values: &[&str],
        timestamp_ms: u64,
        ttl_ms: u64,
        precision: ControlStatePrecision,
        dont_upgrade_cpc: bool,
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("values", serde_json::json!(values)),
                ("timestamp_ms", serde_json::json!(timestamp_ms)),
                ("ttl_ms", serde_json::json!(ttl_ms)),
                (
                    "precision_ms",
                    serde_json::json!(control_state_precision_ms(precision)),
                ),
                ("dont_upgrade_cpc", serde_json::json!(dont_upgrade_cpc)),
            ],
        );
        self.proxy_service_execute("/ProxyService/ControlStateCPCSet", body)
            .map(|_| ())
    }

    pub fn control_state_cpc_query(
        &self,
        key: &str,
        precision: ControlStatePrecision,
        window: ControlStateWindow,
    ) -> Result<Vec<i64>> {
        let (start_ms, end_ms) = control_state_window_ms(window);
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
                (
                    "precision_ms",
                    serde_json::json!(control_state_precision_ms(precision)),
                ),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/ControlStateCPCQuery", body)?;
        Ok(response
            .get("count_list")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|value| value.as_i64().unwrap_or_default())
            .collect())
    }
}
