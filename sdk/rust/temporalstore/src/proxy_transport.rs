use std::io::{Read, Write};
use std::net::TcpStream;

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{io_error, json_error, parse_http_endpoint};
use crate::{Error, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub(crate) fn proxy_service_body(
        &self,
        key: &str,
        extra: &[(&str, serde_json::Value)],
    ) -> serde_json::Value {
        let mut body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "key": key,
        });
        let object = body.as_object_mut().expect("object body");
        for (name, value) in extra {
            object.insert((*name).to_string(), value.clone());
        }
        body
    }

    pub(crate) fn proxy_service_execute(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let response = self.proxy_service_request(path, body)?;
        Ok(response
            .get("response")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})))
    }

    pub(crate) fn proxy_service_request(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let response = self.post_raw(path, body)?;
        let status_ok = response
            .get("status")
            .and_then(|status| status.get("ok"))
            .and_then(|ok| ok.as_bool())
            .unwrap_or(false);
        if !status_ok {
            return Err(Error {
                code: 0,
                message: response
                    .get("status")
                    .and_then(|status| status.get("message"))
                    .and_then(|value| value.as_str())
                    .unwrap_or("proxy service request failed")
                    .to_string(),
            });
        }
        Ok(response)
    }

    pub(crate) fn get_raw(&self, path: &str) -> Result<serde_json::Value> {
        self.http_json("GET", path, None)
    }

    pub(crate) fn post_raw(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.http_json("POST", path, Some(body))
    }

    fn http_json(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let (host, port, base_path) = parse_http_endpoint(&self.endpoint)?;
        let request_path = format!("{base_path}{path}");
        let payload = match body {
            Some(body) => serde_json::to_string(&body).map_err(json_error)?,
            None => String::new(),
        };
        let mut headers = format!(
            "{method} {request_path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            payload.len()
        );
        if !self.options.api_key.is_empty() {
            headers.push_str(&format!(
                "Authorization: Bearer {}\r\n",
                self.options.api_key
            ));
        }

        headers.push_str("\r\n");
        let mut stream = TcpStream::connect(format!("{host}:{port}")).map_err(io_error)?;
        stream.write_all(headers.as_bytes()).map_err(io_error)?;
        stream.write_all(payload.as_bytes()).map_err(io_error)?;
        stream.flush().map_err(io_error)?;

        let mut response = String::new();
        stream.read_to_string(&mut response).map_err(io_error)?;
        let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| Error {
            code: 0,
            message: "invalid proxy HTTP response".to_string(),
        })?;
        if !head.starts_with("HTTP/1.1 2") && !head.starts_with("HTTP/1.0 2") {
            return Err(Error {
                code: 0,
                message: head
                    .lines()
                    .next()
                    .unwrap_or("proxy HTTP request failed")
                    .to_string(),
            });
        }
        serde_json::from_str(body).map_err(json_error)
    }
}
