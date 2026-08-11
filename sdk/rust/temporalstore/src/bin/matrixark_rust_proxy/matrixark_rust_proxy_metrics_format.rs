// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn metric_header(out: &mut String, name: &str, metric_type: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
}

pub(crate) fn line<T: std::fmt::Display>(out: &mut String, name: &str, labels: &str, value: T) {
    out.push_str(name);
    out.push_str(labels);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

pub(crate) fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub(crate) fn matrixark_rust_storage_mode() -> &'static str {
    "rust-proxy"
}
