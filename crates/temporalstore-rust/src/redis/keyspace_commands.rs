// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis keyspace commands (KEYS/SCAN/TYPE + pattern matching), extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse};

pub(super) fn redis_keys_response(pattern: &[u8], state: &RedisCommandState) -> RespValue {
    let pattern = string_arg(pattern);
    RespValue::Array(
        sorted_matching_keys(&pattern, state)
            .into_iter()
            .map(|key| RespValue::Bulk(Some(key.into_bytes())))
            .collect(),
    )
}

pub(super) fn redis_scan_response(args: &[Vec<u8>], state: &RedisCommandState) -> RespValue {
    let cursor = match parse_usize(&args[1], "cursor") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let mut pattern = "*".to_string();
    let mut count = 10usize;
    let mut index = 2;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "MATCH" => {
                let Some(value) = args.get(index + 1) else {
                    return RespValue::Error("ERR syntax error".to_string());
                };
                pattern = string_arg(value);
                index += 2;
            }
            "COUNT" => {
                let Some(value) = args.get(index + 1) else {
                    return RespValue::Error("ERR syntax error".to_string());
                };
                count = match parse_usize(value, "count") {
                    Ok(value) => value.max(1),
                    Err(err) => return RespValue::Error(err),
                };
                index += 2;
            }
            _ => return RespValue::Error("ERR syntax error".to_string()),
        }
    }
    let keys = sorted_matching_keys(&pattern, state);
    let selected = keys
        .iter()
        .skip(cursor)
        .take(count)
        .cloned()
        .collect::<Vec<_>>();
    let next_cursor = cursor.saturating_add(selected.len());
    let next_cursor = if next_cursor >= keys.len() {
        0
    } else {
        next_cursor
    };
    RespValue::Array(vec![
        RespValue::Bulk(Some(next_cursor.to_string().into_bytes())),
        RespValue::Array(
            selected
                .into_iter()
                .map(|key| RespValue::Bulk(Some(key.into_bytes())))
                .collect(),
        ),
    ])
}

pub(super) fn parse_scan_tail_options(args: &[Vec<u8>]) -> Result<(String, usize), String> {
    let mut pattern = "*".to_string();
    let mut count = 10usize;
    let mut index = 0;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "MATCH" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                pattern = string_arg(value);
                index += 2;
            }
            "COUNT" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                count = parse_usize(value, "count")?.max(1);
                index += 2;
            }
            _ => return Err("ERR syntax error".to_string()),
        }
    }
    Ok((pattern, count))
}

pub(super) fn redis_cursor_page_response(cursor: usize, count: usize, values: Vec<RespValue>) -> RespValue {
    let selected = values
        .iter()
        .skip(cursor)
        .take(count)
        .cloned()
        .collect::<Vec<_>>();
    let next_cursor = cursor.saturating_add(selected.len());
    let next_cursor = if next_cursor >= values.len() {
        0
    } else {
        next_cursor
    };
    RespValue::Array(vec![
        RespValue::Bulk(Some(next_cursor.to_string().into_bytes())),
        RespValue::Array(selected),
    ])
}

pub(super) fn sorted_matching_keys(pattern: &str, state: &RedisCommandState) -> Vec<String> {
    let mut keys = state
        .keyspace
        .iter()
        .filter(|key| redis_pattern_matches(pattern, key))
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

pub(super) fn redis_pattern_matches(pattern: &str, value: &str) -> bool {
    fn matches_parts(pattern: &[u8], value: &[u8]) -> bool {
        match pattern.split_first() {
            None => value.is_empty(),
            Some((&b'*', rest)) => {
                matches_parts(rest, value)
                    || (!value.is_empty() && matches_parts(pattern, &value[1..]))
            }
            Some((&b'?', rest)) => !value.is_empty() && matches_parts(rest, &value[1..]),
            Some((&expected, rest)) => value.split_first().is_some_and(|(&actual, value_rest)| {
                actual == expected && matches_parts(rest, value_rest)
            }),
        }
    }
    matches_parts(pattern.as_bytes(), value.as_bytes())
}

pub(super) fn redis_type_response(
    key: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(key);
    match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value: Some(_) }) => {
            return RespValue::SimpleString("string".to_string());
        }
        Ok(CommandResponse::Bytes { value: None }) => {}
        Ok(_) => return RespValue::Error("ERR invalid type string response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    }
    match execute(Command::HashLen { key: key.clone() }) {
        Ok(CommandResponse::Integer { value }) if value > 0 => {
            return RespValue::SimpleString("hash".to_string());
        }
        Ok(CommandResponse::Integer { .. }) => {}
        Ok(_) => return RespValue::Error("ERR invalid type hash response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    }
    match execute(Command::ZSetCard { key: key.clone() }) {
        Ok(CommandResponse::Integer { value }) if value > 0 => {
            return RespValue::SimpleString("zset".to_string());
        }
        Ok(CommandResponse::Integer { .. }) => {}
        Ok(_) => return RespValue::Error("ERR invalid type zset response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    }
    match execute(Command::ListLen { key: key.clone() }) {
        Ok(CommandResponse::Integer { value }) if value > 0 => {
            return RespValue::SimpleString("list".to_string());
        }
        Ok(CommandResponse::Integer { .. }) => {}
        Ok(_) => return RespValue::Error("ERR invalid type list response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    }
    match execute(Command::SetMembers { key }) {
        Ok(CommandResponse::Members { members }) if !members.is_empty() => {
            RespValue::SimpleString("set".to_string())
        }
        Ok(CommandResponse::Members { .. }) => RespValue::SimpleString("none".to_string()),
        Ok(_) => RespValue::Error("ERR invalid type set response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}
