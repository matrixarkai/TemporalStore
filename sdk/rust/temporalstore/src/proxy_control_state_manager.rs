// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::response_hash_entries_to_strings;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_manager(&self, key: &str) -> Result<Vec<(String, String)>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/ControlStateManager", body)?;
        Ok(response_hash_entries_to_strings(response))
    }

    pub fn control_state_manager_with_options(
        &self,
        key: &str,
        op_type: &str,
        field_list: &[(&str, &str)],
        start_offset: &str,
        end_offset: &str,
        is_cpc: bool,
    ) -> Result<Vec<(String, String)>> {
        let field_list = field_list
            .iter()
            .map(|(key, value)| serde_json::json!({"key": key, "value": value}))
            .collect::<Vec<_>>();
        let body = self.proxy_service_body(
            key,
            &[
                ("op_type", serde_json::json!(op_type)),
                ("field_list", serde_json::json!(field_list)),
                ("start_offset", serde_json::json!(start_offset)),
                ("end_offset", serde_json::json!(end_offset)),
                ("is_cpc", serde_json::json!(is_cpc)),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/ControlStateManager", body)?;
        Ok(response_hash_entries_to_strings(response))
    }
}
