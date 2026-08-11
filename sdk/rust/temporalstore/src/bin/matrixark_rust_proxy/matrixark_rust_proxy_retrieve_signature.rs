// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde_json::Value;

use crate::matrixark_rust_proxy_candidates::record_ref_hash;

pub(crate) fn selected_ref_signature(record: &Value, context_class: &str) -> String {
    format!(
        "{}:{}",
        context_class,
        record_ref_hash(record).unwrap_or_else(|| {
            record
                .get("record_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        })
    )
}
