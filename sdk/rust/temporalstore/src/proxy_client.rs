use crate::proxy_helpers::{
    control_state_precision_ms, control_state_window_ms, json_byte_array_to_string, json_error,
    proxy_timestamp_ms, response_feature_points, response_hash_entries_to_strings,
};
use crate::{
    ControlStateFolType, ControlStateHType, ControlStatePrecision, ControlStateWindow,
    FeatureFilter, FeaturePoint, FeatureWritePolicy, IpsFeatureStat, IpsInstance, IpsLastQuery,
    Result, SequenceFeatureRow,
};

#[cfg(feature = "proxy")]
#[derive(Clone, Debug)]
pub struct ProxyOptions {
    pub endpoint: String,
    pub namespace_name: String,
    pub table_name: String,
    pub api_key: String,
}

#[cfg(feature = "proxy")]
impl ProxyOptions {
    pub fn new(
        endpoint: impl Into<String>,
        namespace_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            api_key: String::new(),
        }
    }
}

#[cfg(feature = "proxy")]
pub struct ProxyClient {
    pub(crate) endpoint: String,
    pub(crate) options: ProxyOptions,
}

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn connect(options: ProxyOptions) -> Self {
        let endpoint = options.endpoint.trim_end_matches('/').to_string();
        Self { endpoint, options }
    }

    pub fn open_table(&self) -> Result<serde_json::Value> {
        self.open_table_with_options(None, None)
    }

    pub fn open_table_with_options(
        &self,
        pin_primary: Option<bool>,
        replica_read_policy: Option<&str>,
    ) -> Result<serde_json::Value> {
        let mut body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "pin_primary": pin_primary,
        });
        if let Some(replica_read_policy) = replica_read_policy {
            body.as_object_mut().expect("object body").insert(
                "replica_read_policy".to_string(),
                serde_json::json!(replica_read_policy),
            );
        }
        self.post_raw("/ProxyService/OpenTable", body)
    }

    pub fn get_proxy_config(&self) -> Result<serde_json::Value> {
        self.get_raw("/ProxyService/GetConfig")
    }

    pub fn update_proxy_config(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        self.post_raw("/ProxyService/UpdateConfig", options)
    }

    pub fn refresh_topology(&self) -> Result<serde_json::Value> {
        self.post_raw("/ProxyService/RefreshTopology", serde_json::json!({}))
    }

    pub fn execute_command(
        &self,
        shard_id: u64,
        command: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "shard_id": shard_id,
            "command": command,
        });
        self.proxy_service_request("/ProxyService/ExecuteCmd", body)
    }

    pub fn batch_execute_commands(
        &self,
        shard_id: u64,
        commands: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "shard_id": shard_id,
            "commands": commands,
        });
        self.proxy_service_request("/ProxyService/BatchExecuteCmd", body)
    }

    pub fn table_execute_command(&self, command: serde_json::Value) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "command": command,
        });
        self.proxy_service_request("/ProxyService/TableExecuteCmd", body)
    }

    pub fn table_batch_execute_commands(
        &self,
        commands: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "commands": commands,
        });
        self.proxy_service_request("/ProxyService/TableBatchExecuteCmd", body)
    }

    pub fn put_string(&self, key: &str, value: &str) -> Result<()> {
        self.set(key, value)
    }

    pub fn put_string_with_ttl(&self, key: &str, value: &str, ttl_ms: u64) -> Result<()> {
        self.set_ex(key, value, ttl_ms)
    }

    pub fn get_string(&self, key: &str) -> Result<String> {
        self.get(key).map(|value| value.unwrap_or_default())
    }

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

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[("value", serde_json::json!(value.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/Set", body)
            .map(|_| ())
    }

    pub fn set_ex(&self, key: &str, value: &str, ttl_ms: u64) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("value", serde_json::json!(value.as_bytes())),
                ("ttl_ms", serde_json::json!(ttl_ms)),
            ],
        );
        self.proxy_service_execute("/ProxyService/SetEx", body)
            .map(|_| ())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/Get", body)?;
        Ok(response
            .get("value")
            .cloned()
            .filter(|value| !value.is_null())
            .map(json_byte_array_to_string))
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

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("field", serde_json::json!(field)),
                ("value", serde_json::json!(value.as_bytes())),
            ],
        );
        self.proxy_service_execute("/ProxyService/HSet", body)
            .map(|_| ())
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<String> {
        let body = self.proxy_service_body(key, &[("field", serde_json::json!(field))]);
        let response = self.proxy_service_execute("/ProxyService/HGet", body)?;
        Ok(json_byte_array_to_string(
            response
                .get("value")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        ))
    }

    pub fn hmset(&self, key: &str, entries: &[(&str, &str)]) -> Result<()> {
        let entries = entries
            .iter()
            .map(|(field, value)| serde_json::json!([field, value.as_bytes()]))
            .collect::<Vec<_>>();
        let body = self.proxy_service_body(key, &[("entries", serde_json::json!(entries))]);
        self.proxy_service_execute("/ProxyService/HMSet", body)
            .map(|_| ())
    }

    pub fn hmget(&self, key: &str, fields: &[&str]) -> Result<Vec<Option<String>>> {
        let body = self.proxy_service_body(key, &[("fields", serde_json::json!(fields))]);
        let response = self.proxy_service_execute("/ProxyService/HMGet", body)?;
        let raw_values = response
            .get("values")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let exists = response
            .get("exists")
            .and_then(|value| value.as_array())
            .cloned();
        let values = raw_values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let present = exists
                    .as_ref()
                    .and_then(|items| items.get(index))
                    .and_then(|value| value.as_bool())
                    .unwrap_or(!value.is_null());
                if present {
                    Some(json_byte_array_to_string(value))
                } else {
                    None
                }
            })
            .collect();
        Ok(values)
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/HGetAll", body)?;
        let entries = response
            .get("entries")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.as_array()?;
                let field = entry.first()?.as_str()?.to_string();
                let value = json_byte_array_to_string(entry.get(1)?.clone());
                Some((field, value))
            })
            .collect();
        Ok(entries)
    }

    pub fn scan_hash(&self, key: &str) -> Result<Vec<(String, String)>> {
        self.hgetall(key)
    }

    pub fn hlen(&self, key: &str) -> Result<u64> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/HLen", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_u64())
            .unwrap_or_default())
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[("field", serde_json::json!(field))]);
        self.proxy_service_execute("/ProxyService/HDel", body)
            .map(|_| ())
    }

    pub fn sadd(&self, key: &str, member: &str) -> Result<()> {
        let body =
            self.proxy_service_body(key, &[("member", serde_json::json!(member.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/SAdd", body)
            .map(|_| ())
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/SMembers", body)?;
        let members = response
            .get("members")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|member| {
                let bytes = member
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|item| item.as_u64().unwrap_or_default() as u8)
                    .collect::<Vec<_>>();
                String::from_utf8_lossy(&bytes).into_owned()
            })
            .collect();
        Ok(members)
    }

    pub fn srem(&self, key: &str, member: &str) -> Result<()> {
        let body =
            self.proxy_service_body(key, &[("member", serde_json::json!(member.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/SRem", body)
            .map(|_| ())
    }

    pub fn delete_object(&self, key: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[]);
        self.proxy_service_execute("/ProxyService/Delete", body)
            .map(|_| ())
    }

    pub fn expire(&self, key: &str, ttl_ms: u64) -> Result<()> {
        let body = self.proxy_service_body(key, &[("ttl_ms", serde_json::json!(ttl_ms))]);
        self.proxy_service_execute("/ProxyService/Expire", body)
            .map(|_| ())
    }

    pub fn ttl(&self, key: &str) -> Result<u64> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/Ttl", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_u64())
            .unwrap_or_default())
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/Exists", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_i64())
            .unwrap_or_default()
            != 0)
    }

}
