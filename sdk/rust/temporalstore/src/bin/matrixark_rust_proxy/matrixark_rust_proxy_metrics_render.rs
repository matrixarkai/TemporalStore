// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;

use crate::matrixark_rust_proxy_metrics_backend_render::append_backend_metrics;
use crate::matrixark_rust_proxy_metrics_core_render::{
    append_command_metrics, append_process_metrics,
};
use crate::matrixark_rust_proxy_metrics_io_render::append_proxy_io_metrics;
use crate::matrixark_rust_proxy_metrics_retrieve_render::append_retrieve_metrics;

impl MetricsSnapshot {
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        append_process_metrics(&mut out, self);
        append_command_metrics(&mut out, self);
        append_retrieve_metrics(&mut out, self);
        append_proxy_io_metrics(&mut out, self);
        append_backend_metrics(&mut out, self);
        out
    }
}
