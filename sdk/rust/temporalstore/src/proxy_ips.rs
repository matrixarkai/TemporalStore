use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{json_error, response_feature_points};
use crate::{IpsFeatureStat, IpsInstance, IpsLastQuery, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn add_ips_instance(&self, instance: &IpsInstance) -> Result<()> {
        let encoded = serde_json::to_vec(&instance.features).map_err(json_error)?;
        let body = self.proxy_service_body(
            &instance.table,
            &[
                (
                    "timestamp_ms",
                    serde_json::json!((instance.timestamp_us.max(0) as u64) / 1000),
                ),
                ("instance", serde_json::json!(encoded)),
                (
                    "action_type",
                    serde_json::json!(instance.action_type.max(0) as u32),
                ),
                (
                    "table_id",
                    serde_json::json!(instance.logical_table.max(0) as u64),
                ),
            ],
        );
        self.proxy_service_execute("/ProxyService/IpsAdd", body)
            .map(|_| ())
    }

    pub fn query_ips_last_instances(&self, query: &IpsLastQuery) -> Result<Vec<IpsFeatureStat>> {
        let body = self.proxy_service_body(
            &query.table,
            &[(
                "count",
                serde_json::json!(query.last_instances.max(0) as usize),
            )],
        );
        let response = self.proxy_service_execute("/ProxyService/IpsQueryLast", body)?;
        let points = response_feature_points(response)?;
        let mut features = Vec::new();
        for point in points {
            let decoded: Vec<IpsFeatureStat> =
                serde_json::from_slice(&point.value).map_err(json_error)?;
            features.extend(decoded);
        }
        Ok(features)
    }
}
