// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{json_byte_array_to_string, json_error};
use crate::{ControlStateFolType, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_fol_set(
        &self,
        key: &str,
        value: &str,
        occur_time_ms: u64,
        ttl_ms: u64,
        fol_type: ControlStateFolType,
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("value", serde_json::json!(value)),
                ("value_bytes", serde_json::json!(value.as_bytes())),
                ("occur_time", serde_json::json!(occur_time_ms / 1000)),
                ("occur_time_ms", serde_json::json!(occur_time_ms)),
                ("ttl", serde_json::json!(ttl_ms / 1000)),
                ("ttl_ms", serde_json::json!(ttl_ms)),
                (
                    "fol_type",
                    serde_json::to_value(fol_type).map_err(json_error)?,
                ),
            ],
        );
        self.proxy_service_execute("/ProxyService/ControlStateFolSet", body)
            .map(|_| ())
    }

    pub fn control_state_fol_query(&self, key: &str) -> Result<Option<String>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/ControlStateFolQuery", body)?;
        Ok(response
            .get("value")
            .cloned()
            .filter(|value| !value.is_null())
            .map(json_byte_array_to_string))
    }
}
