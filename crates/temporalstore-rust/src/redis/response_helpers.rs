// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis RESP response encoders and argument parsers, extracted from redis.rs.

use super::*;
use crate::types::{CommandResponse, FeatureFilterOp, FeaturePoint, FeatureWritePolicy};

pub(crate) fn bytes_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::Bytes { value }) => RespValue::Bulk(value),
        Ok(_) => RespValue::Error("ERR invalid bulk response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn integer_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::Integer { value }) => RespValue::Integer(value),
        Ok(_) => RespValue::Error("ERR invalid integer response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn status_ok(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(_) => RespValue::SimpleString("OK".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn feature_points_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::FeaturePoints { points }) => feature_points_value(points),
        Ok(_) => RespValue::Error("ERR invalid feature points response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn hash_entries_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::HashEntries { entries }) => RespValue::Array(
            entries
                .into_iter()
                .flat_map(|(field, value)| {
                    [
                        RespValue::Bulk(Some(field.into_bytes())),
                        RespValue::Bulk(Some(value)),
                    ]
                })
                .collect(),
        ),
        Ok(_) => RespValue::Error("ERR invalid hash entries response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn feature_points_value(points: Vec<FeaturePoint>) -> RespValue {
    RespValue::Array(
        points
            .into_iter()
            .map(|point| {
                RespValue::Array(vec![
                    RespValue::Integer(point.timestamp_ms as i64),
                    RespValue::Bulk(Some(point.value)),
                ])
            })
            .collect(),
    )
}

pub(crate) fn parse_u64(value: &[u8], name: &str) -> Result<u64, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("ERR {name} must be an unsigned integer"))
}

pub(crate) fn parse_usize(value: &[u8], name: &str) -> Result<usize, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("ERR {name} must be an unsigned integer"))
}

pub(crate) fn parse_i64_arg(value: &[u8], name: &str) -> Result<i64, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("ERR {name} must be an integer"))
}

pub(crate) fn parse_feature_write_policy(value: &[u8]) -> Result<FeatureWritePolicy, String> {
    match upper(value).as_str() {
        "UPSERT" => Ok(FeatureWritePolicy::Upsert),
        "FIRST" | "NX" | "INSERT_IF_ABSENT" => Ok(FeatureWritePolicy::InsertIfAbsent),
        "UPDATE" | "XX" | "REPLACE_EXISTING" => Ok(FeatureWritePolicy::ReplaceExisting),
        "BLOCK" => Ok(FeatureWritePolicy::Block),
        _ => Err("ERR policy must be UPSERT, FIRST/NX, UPDATE/XX, or BLOCK".to_string()),
    }
}

pub(crate) fn parse_feature_filter_op(value: &str) -> Result<FeatureFilterOp, String> {
    match value.to_ascii_uppercase().as_str() {
        "=" | "==" | "EQ" => Ok(FeatureFilterOp::Equal),
        "!=" | "<>" | "NE" => Ok(FeatureFilterOp::NotEqual),
        ">" | "GT" => Ok(FeatureFilterOp::GreaterThan),
        ">=" | "GE" | "GTE" => Ok(FeatureFilterOp::GreaterOrEqual),
        "<" | "LT" => Ok(FeatureFilterOp::LessThan),
        "<=" | "LE" | "LTE" => Ok(FeatureFilterOp::LessOrEqual),
        _ => Err("ERR filter op must be =, !=, >, >=, <, or <=".to_string()),
    }
}

pub(crate) fn string_arg(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

pub(crate) fn upper(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_ascii_uppercase()
}


pub(crate) fn normalize_range(start: i64, stop: i64, len: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let len_i64 = len as i64;
    let mut start = if start < 0 { len_i64 + start } else { start };
    let mut stop = if stop < 0 { len_i64 + stop } else { stop };
    if start < 0 {
        start = 0;
    }
    if stop < 0 || start >= len_i64 {
        return (0, 0);
    }
    if stop >= len_i64 {
        stop = len_i64 - 1;
    }
    if start > stop {
        (0, 0)
    } else {
        (start as usize, stop as usize + 1)
    }
}

pub(crate) fn format_redis_score(score: f64) -> String {
    if score.fract() == 0.0 {
        format!("{score:.0}")
    } else {
        score.to_string()
    }
}

pub(crate) fn parse_f64_arg(value: &[u8], name: &str) -> Result<f64, String> {
    let value = std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .ok_or_else(|| format!("ERR {name} must be a float"))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(format!("ERR {name} must be finite"))
    }
}
