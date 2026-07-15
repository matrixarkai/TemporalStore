use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{
    control_state_precision_ms, control_state_window_ms, json_error, proxy_timestamp_ms,
};
use crate::{ControlStateHType, ControlStatePrecision, ControlStateWindow, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_increment(
        &self,
        key: &str,
        amount: i64,
        ttl_seconds: u64,
        precision: ControlStatePrecision,
        _uuid: &str,
        occur_time_seconds: u64,
    ) -> Result<()> {
        let timestamp_ms = proxy_timestamp_ms(occur_time_seconds);
        let body = self.proxy_service_body(
            key,
            &[
                ("timestamp_ms", serde_json::json!(timestamp_ms)),
                ("amount", serde_json::json!(amount)),
                (
                    "precision_ms",
                    serde_json::json!(control_state_precision_ms(precision)),
                ),
                (
                    "ttl_ms",
                    serde_json::json!(ttl_seconds.saturating_mul(1000)),
                ),
            ],
        );
        self.proxy_service_execute("/ProxyService/ControlStateIncrement", body)
            .map(|_| ())
    }

    pub fn control_state_count(
        &self,
        key: &str,
        _precision: ControlStatePrecision,
        window: ControlStateWindow,
    ) -> Result<i64> {
        let (start_ms, end_ms) = control_state_window_ms(window);
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/ControlStateCount", body)?;
        Ok(response
            .get("value")
            .or_else(|| response.get("count"))
            .and_then(|value| value.as_i64())
            .unwrap_or_default())
    }

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

}
