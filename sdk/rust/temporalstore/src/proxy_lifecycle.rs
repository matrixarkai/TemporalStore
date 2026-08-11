// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
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
