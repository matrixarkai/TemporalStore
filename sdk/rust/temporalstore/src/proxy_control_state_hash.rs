// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{control_state_precision_ms, json_error, proxy_timestamp_ms};
use crate::{ControlStateHType, ControlStatePrecision, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
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

    // There was a `control_state_hquery` here, posting to `/ProxyService/ControlStateHquery`.
    // The dispatcher answers 404 for that route (it matches ControlStateHset, Increment, Count,
    // Manager and the Fol/Selection pair, and nothing else), so the call could only ever fail.
    // #1040 removed every unserved route from this SDK and named this one as unserved; it is left
    // off rather than re-added. Adding the route to the proxy is the other way to resolve it, and
    // `proxy_route_contract` is the test that will fail if the method comes back first.
}
