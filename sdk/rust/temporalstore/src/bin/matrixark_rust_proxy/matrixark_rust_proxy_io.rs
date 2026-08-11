// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::time::Instant;

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;

pub(crate) fn print_result(result: Result<Value, String>, engine_ms: u128) -> (bool, u128) {
    match result {
        Ok(mut value) => {
            if let Some(object) = value.as_object_mut() {
                object.insert("rust_engine_time_ms".to_string(), json!(engine_ms));
            }
            let serialize_started = Instant::now();
            let _ = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
            let serialization_ms = serialize_started.elapsed().as_millis();
            let total_ms = engine_ms + serialization_ms;
            if let Some(object) = value.as_object_mut() {
                object.insert("serialization_time_ms".to_string(), json!(serialization_ms));
                object.insert("elapsed_ms".to_string(), json!(total_ms));
            }
            println!("{}", value);
            (true, total_ms)
        }
        Err(err) => {
            let mut value = json!({
                "ok": false,
                "error": err,
                "rust_engine_time_ms": engine_ms
            });
            let serialize_started = Instant::now();
            let _ = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
            let serialization_ms = serialize_started.elapsed().as_millis();
            let total_ms = engine_ms + serialization_ms;
            if let Some(object) = value.as_object_mut() {
                object.insert("serialization_time_ms".to_string(), json!(serialization_ms));
                object.insert("elapsed_ms".to_string(), json!(total_ms));
            }
            println!("{}", value);
            (false, total_ms)
        }
    }
}

pub(crate) fn export_metrics_if_configured(metrics: &MetricsSnapshot) {
    let Ok(path) = std::env::var("MATRIXARK_RUST_METRICS_PATH") else {
        return;
    };
    if path.trim().is_empty() {
        return;
    }
    let _ = std::fs::write(path, metrics.render_prometheus());
}
