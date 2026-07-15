use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{
    control_state_precision_ms, control_state_window_ms, json_error, proxy_timestamp_ms,
    response_feature_points,
};
use crate::{
    ControlStatePrecision, ControlStateWindow, FeatureFilter, FeaturePoint, FeatureWritePolicy,
    IpsFeatureStat, IpsInstance, IpsLastQuery, Result, SequenceFeatureRow,
};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[("rows", serde_json::to_value(rows).map_err(json_error)?)],
        );
        self.proxy_service_execute("/ProxyService/SequenceAdd", body)
            .map(|_| ())
    }

    pub fn query_sequence_feature_rows(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<SequenceFeatureRow>> {
        let encoded_filters: Vec<serde_json::Value> = filters
            .iter()
            .map(|filter| {
                serde_json::json!({
                    "field": filter.field,
                    "op": filter.op as i32,
                    "value": filter.value,
                })
            })
            .collect();
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ts)),
                ("end_ms", serde_json::json!(end_ts)),
                ("count", serde_json::json!(count)),
                ("filters", serde_json::json!(encoded_filters)),
            ],
        );
        let data = self.proxy_service_execute("/ProxyService/SequenceQuery", body)?;
        serde_json::from_value(
            data.get("rows")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        )
        .map_err(json_error)
    }

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

    pub fn add_feature_points(&self, key: &str, points: &[FeaturePoint]) -> Result<()> {
        self.feature_add(key, points)
    }

    pub fn add_feature_points_with_policy(
        &self,
        key: &str,
        points: &[FeaturePoint],
        policy: FeatureWritePolicy,
    ) -> Result<()> {
        self.feature_add_with_policy(key, points, Some(policy))
    }

    pub fn query_feature_points(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
    ) -> Result<Vec<FeaturePoint>> {
        self.feature_query(key, start_ts, end_ts, Some(count as usize))
    }

    pub fn query_feature_points_filtered(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<FeaturePoint>> {
        self.feature_query_filtered(key, start_ts, end_ts, Some(count as usize), filters)
    }

    pub fn feature_add(&self, key: &str, points: &[FeaturePoint]) -> Result<()> {
        self.feature_add_with_policy(key, points, None)
    }

    pub fn feature_add_with_policy(
        &self,
        key: &str,
        points: &[FeaturePoint],
        policy: Option<FeatureWritePolicy>,
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("format", serde_json::json!("protobuf")),
                ("points", serde_json::to_value(points).map_err(json_error)?),
                ("policy", serde_json::to_value(policy).map_err(json_error)?),
            ],
        );
        self.proxy_service_execute("/ProxyService/FeatureAdd", body)
            .map(|_| ())
    }

    pub fn feature_query(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>> {
        self.feature_query_filtered(key, start_ms, end_ms, count, &[])
    }

    pub fn feature_query_filtered(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: &[FeatureFilter],
    ) -> Result<Vec<FeaturePoint>> {
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
                ("count", serde_json::json!(count)),
                ("format", serde_json::json!("protobuf")),
                (
                    "filters",
                    serde_json::to_value(filters).map_err(json_error)?,
                ),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/FeatureQuery", body)?;
        response_feature_points(response)
    }

    pub fn feature_replace(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        points: &[FeaturePoint],
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
                ("points", serde_json::to_value(points).map_err(json_error)?),
            ],
        );
        self.proxy_service_execute("/ProxyService/FeatureReplace", body)
            .map(|_| ())
    }

    pub fn feature_delete(&self, key: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[]);
        self.proxy_service_execute("/ProxyService/FeatureDelete", body)
            .map(|_| ())
    }

    pub fn feature_aggregate(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        aggregator: &str,
        count: Option<usize>,
    ) -> Result<i64> {
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
                ("aggregator", serde_json::json!(aggregator)),
                ("count", serde_json::json!(count)),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/FeatureAggQuery", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_i64())
            .unwrap_or_default())
    }
}
