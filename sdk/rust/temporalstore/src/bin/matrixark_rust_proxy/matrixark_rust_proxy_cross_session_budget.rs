// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn default_cross_session_budget_ratio(question_type: &str) -> f64 {
    if matches!(
        question_type,
        "current_state" | "latest" | "multi_hop" | "date"
    ) {
        0.20
    } else if matches!(question_type, "broad_exploration" | "evidence") {
        0.15
    } else {
        0.12
    }
}
