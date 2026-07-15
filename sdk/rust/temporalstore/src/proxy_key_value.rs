use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::json_byte_array_to_string;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
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
