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
                        RespValue::Bulk(Some(decode_or_raw(&field))),
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
        "MERGE_REDUCE" | "MERGE" => Ok(FeatureWritePolicy::MergeReduce),
        _ => Err("ERR policy must be UPSERT, FIRST/NX, UPDATE/XX, BLOCK, or MERGE_REDUCE".to_string()),
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

/// A NAME, SPELLED SO THAT NO BYTE OF IT IS LOST.
///
/// THIS IS THE CHOKE POINT, AND THAT IS THE POINT. A key and a hash field are binary-safe by
/// specification, and this function used to be `String::from_utf8_lossy`, which is not a
/// limitation but a wrong answer: every invalid byte became U+FFFD, so two distinct names landed
/// on one stored name and the second write silently overwrote the first. Measured, twice by
/// independent methods: 91.06-91.08% of four-byte names are not valid text, and two distinct ones
/// collide 1 in 1,042 against 1 in 4.29 billion for a correct store.
///
/// WHY ONE FUNCTION RATHER THAN THE 123 CALL SITES THAT NEEDED IT. A missed call site is a silent
/// wrong answer -- the very defect being removed -- and there is no signal that finds one. A
/// choke point cannot be missed. The sites that are NOT names (an aggregator, a host, a port, a
/// glob pattern) take [`text_arg`] instead, and the diff says which is which at every site.
///
/// IT IS IDENTITY ON EVERY NAME THIS STORE ALREADY HOLDS. `encode_name` returns valid UTF-8 that
/// does not begin with the tag BYTE FOR BYTE, so every ASCII key and field keeps the exact
/// spelling it has today and no stored data changes meaning. Only a name that was previously
/// being mangled travels differently.
pub(crate) fn name_arg(value: &[u8]) -> String {
    crate::binary_safe_name::encode_name(value)
}

/// TEXT THAT IS NOT A NAME, and is allowed to be lossy because nothing is stored under it.
///
/// An aggregator, a replication host and port, a `CLIENT`/`OBJECT` subcommand, an `INFO` section,
/// a glob pattern: each is compared against a literal or parsed as a number, so a client that
/// sends bytes which are not text gets a value that matches nothing -- which is what it got
/// before. Keeping these on a lossy decode is deliberate, and keeping them SEPARATE is what lets
/// a reviewer see that the name path is complete.
pub(crate) fn text_arg(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

/// A STORED NAME, GIVEN BACK AS THE BYTES IT STANDS FOR.
///
/// This is the read half of the binary-safe name sequence, and on every name this build writes
/// today it is a NO-OP: an untagged name decodes to its own bytes, which is `decode_name`'s first
/// arm. Its only effect is to make a node ABLE to read a tagged name, which is what has to be
/// deployed everywhere before anything starts writing one. Shipping that write step in the same
/// commit is the rolling-restart hazard the encoding module's own doc names.
///
/// THE ERROR ARM IS UNREACHABLE FROM OUR OWN ENCODER, and that is why this falls back rather than
/// failing. `encode_name` never emits a tagged name whose tail is not base64, so an error here
/// means the stored bytes did not come from it -- corrupt, or written by something else. Answering
/// with the name AS STORED is exactly what every reader did before this encoding existed, so the
/// fallback is the old behaviour rather than a new guess, and it keeps one unreadable name from
/// failing a listing of a thousand good ones.
pub(crate) fn decode_or_raw(stored: &str) -> Vec<u8> {
    crate::binary_safe_name::decode_name(stored).unwrap_or_else(|_| stored.as_bytes().to_vec())
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

#[cfg(test)]
mod merge_reduce_policy_tests {
    use super::*;

    #[test]
    fn the_text_door_spells_the_merge_reduce_policy_two_ways_and_refuses_a_guess() {
        for spelling in [&b"merge_reduce"[..], b"MERGE", b"Merge_Reduce"] {
            assert_eq!(
                FeatureWritePolicy::MergeReduce,
                parse_feature_write_policy(spelling).expect("a known spelling")
            );
        }
        assert!(parse_feature_write_policy(b"merge_reduc").is_err());
        assert!(parse_feature_write_policy(b"").is_err());
    }
}
