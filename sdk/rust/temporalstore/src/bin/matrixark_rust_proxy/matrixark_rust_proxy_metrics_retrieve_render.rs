// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;
use crate::matrixark_rust_proxy_metrics_format::{line, metric_header};

pub(crate) fn append_retrieve_metrics(out: &mut String, snapshot: &MetricsSnapshot) {
    metric_header(
        out,
        "matrixark_backend_rust_engine_time_ms_total",
        "counter",
        "Total Rust engine execution time in milliseconds.",
    );
    line(
        out,
        "matrixark_backend_rust_engine_time_ms_total",
        "{backend=\"rust\"}",
        snapshot.rust_engine_time_ms_sum,
    );
    metric_header(
        out,
        "matrixark_backend_serialization_time_ms_total",
        "counter",
        "Total Rust proxy response serialization time in milliseconds.",
    );
    line(
        out,
        "matrixark_backend_serialization_time_ms_total",
        "{backend=\"rust\"}",
        snapshot.serialization_time_ms_sum,
    );
    metric_header(
        out,
        "matrixark_retrieve_scan_count_total",
        "counter",
        "Total records scanned by native MatrixArk retrieval calls.",
    );
    line(
        out,
        "matrixark_retrieve_scan_count_total",
        "{backend=\"rust\"}",
        snapshot.scan_count_total,
    );
    metric_header(
        out,
        "matrixark_retrieve_cache_hits_total",
        "counter",
        "Total native MatrixArk retrieval cache hits.",
    );
    line(
        out,
        "matrixark_retrieve_cache_hits_total",
        "{backend=\"rust\"}",
        snapshot.cache_hit_total,
    );
    metric_header(
        out,
        "matrixark_context_pack_selected_refs_total",
        "counter",
        "Total refs selected by native MatrixArk ContextPack assembly.",
    );
    line(
        out,
        "matrixark_context_pack_selected_refs_total",
        "{backend=\"rust\"}",
        snapshot.selected_refs_total,
    );
    metric_header(
        out,
        "matrixark_context_pack_dropped_refs_total",
        "counter",
        "Total refs dropped by native MatrixArk ContextPack assembly.",
    );
    line(
        out,
        "matrixark_context_pack_dropped_refs_total",
        "{backend=\"rust\"}",
        snapshot.dropped_refs_total,
    );
}
