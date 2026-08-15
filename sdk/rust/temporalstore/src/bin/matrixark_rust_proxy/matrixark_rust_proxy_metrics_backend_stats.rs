// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::matrixark_rust_proxy_clock::unix_ms;
use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;

pub(crate) fn elapsed_seconds(snapshot: &MetricsSnapshot) -> f64 {
    ((unix_ms().saturating_sub(snapshot.started_at_unix_ms)) as f64 / 1000.0).max(0.001)
}

pub(crate) fn latency_le_100_count(snapshot: &MetricsSnapshot) -> u64 {
    snapshot
        .op
        .values()
        .map(|metrics| {
            if metrics.latency_ms_max <= 100 {
                metrics.ok + metrics.failed
            } else {
                0
            }
        })
        .sum()
}

pub(crate) fn max_command_latency_ms(snapshot: &MetricsSnapshot) -> u128 {
    snapshot
        .op
        .values()
        .map(|metrics| metrics.latency_ms_max)
        .max()
        .unwrap_or(0)
}
