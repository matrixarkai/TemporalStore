// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::Result;

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

}
