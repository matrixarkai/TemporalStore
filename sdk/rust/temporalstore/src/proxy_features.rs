use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{json_error, response_feature_points};
use crate::{FeatureFilter, FeaturePoint, FeatureWritePolicy, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
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

}
