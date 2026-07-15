use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::json_byte_array_to_string;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
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
}
