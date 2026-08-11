// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
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
}
