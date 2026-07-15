use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{
    control_state_precision_ms, control_state_window_ms, json_byte_array_to_string, json_error,
    proxy_timestamp_ms, response_hash_entries_to_strings,
};
use crate::{
    ControlStateFolType, ControlStateHType, ControlStatePrecision, ControlStateWindow, Result,
};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_hset(&self, key: &str, timestamp_ms: u64, amount: i64) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("timestamp_ms", serde_json::json!(timestamp_ms)),
                ("amount", serde_json::json!(amount)),
            ],
        );
        self.proxy_service_execute("/ProxyService/ControlStateHset", body)
            .map(|_| ())
    }

    pub fn control_state_hset_with_options(
        &self,
        key: &str,
        value: &str,
        ttl_seconds: u64,
        htype: ControlStateHType,
        occur_time_seconds: u64,
        precision: ControlStatePrecision,
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("value", serde_json::json!(value)),
                ("ttl", serde_json::json!(ttl_seconds)),
                (
                    "ttl_ms",
                    serde_json::json!(ttl_seconds.saturating_mul(1000)),
                ),
                ("htype", serde_json::to_value(htype).map_err(json_error)?),
                ("occur_time", serde_json::json!(occur_time_seconds)),
                (
                    "timestamp_ms",
                    serde_json::json!(proxy_timestamp_ms(occur_time_seconds)),
                ),
                (
                    "precision_ms",
                    serde_json::json!(control_state_precision_ms(precision)),
                ),
            ],
        );
        self.proxy_service_execute("/ProxyService/ControlStateHset", body)
            .map(|_| ())
    }

    pub fn control_state_hquery(
        &self,
        key: &str,
        precision: ControlStatePrecision,
        window: ControlStateWindow,
        aggregator: &str,
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
                ("aggregator", serde_json::json!(aggregator)),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/ControlStateHquery", body)?;
        Ok(response
            .get("result_list")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|item| {
                item.get("result")
                    .and_then(|value| value.as_i64())
                    .unwrap_or_default()
            })
            .collect())
    }

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
