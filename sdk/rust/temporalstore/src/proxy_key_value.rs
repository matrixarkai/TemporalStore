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

}
