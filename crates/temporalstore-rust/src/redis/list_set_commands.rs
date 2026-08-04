//! Redis list commands + set-algebra (SINTER/SUNION/SDIFF) + list storage, extracted from redis.rs.

use super::*;
use std::collections::HashSet;
use crate::types::{Command, CommandResponse};

pub(crate) fn list_push_response(
    args: &[Vec<u8>],
    left: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    if left {
        for value in args.iter().skip(2) {
            values.insert(0, value.clone());
        }
    } else {
        values.extend(args.iter().skip(2).cloned());
    }
    let len = values.len() as i64;
    match store_redis_list(&key, &values, execute) {
        Ok(()) => RespValue::Integer(len),
        Err(err) => RespValue::Error(err),
    }
}

pub(crate) fn list_pop_response(
    args: &[Vec<u8>],
    left: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let count = match args.get(2) {
        Some(value) => match parse_usize(value, "count") {
            Ok(value) => Some(value),
            Err(err) => return RespValue::Error(err),
        },
        None => None,
    };
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    if values.is_empty() {
        return if count.is_some() {
            RespValue::Array(Vec::new())
        } else {
            RespValue::Bulk(None)
        };
    }
    let pop_count = count.unwrap_or(1).min(values.len());
    let popped = if left {
        values.drain(..pop_count).collect::<Vec<_>>()
    } else {
        let split_at = values.len() - pop_count;
        values.split_off(split_at)
    };
    if let Err(err) = store_redis_list(&key, &values, execute) {
        return RespValue::Error(err);
    }
    match count {
        Some(_) => RespValue::Array(
            popped
                .into_iter()
                .map(|value| RespValue::Bulk(Some(value)))
                .collect(),
        ),
        None => RespValue::Bulk(popped.into_iter().next()),
    }
}

pub(crate) fn list_rem_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let count = match parse_i64_arg(&args[2], "count") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let target = &args[3];
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let before = values.len();
    if count == 0 {
        values.retain(|value| value != target);
    } else if count > 0 {
        let mut remaining = count as usize;
        values.retain(|value| {
            if remaining > 0 && value == target {
                remaining -= 1;
                false
            } else {
                true
            }
        });
    } else {
        let mut remaining = count.unsigned_abs() as usize;
        let mut reversed = values.into_iter().rev().collect::<Vec<_>>();
        reversed.retain(|value| {
            if remaining > 0 && value == target {
                remaining -= 1;
                false
            } else {
                true
            }
        });
        values = reversed.into_iter().rev().collect();
    }
    let removed = before.saturating_sub(values.len()) as i64;
    match store_redis_list(&key, &values, execute) {
        Ok(()) => RespValue::Integer(removed),
        Err(err) => RespValue::Error(err),
    }
}

pub(crate) fn parse_list_side(value: &[u8]) -> Result<bool, String> {
    match upper(value).as_str() {
        "LEFT" => Ok(true),
        "RIGHT" => Ok(false),
        _ => Err("ERR syntax error".to_string()),
    }
}

pub(crate) fn list_move_response(
    source: &[u8],
    destination: &[u8],
    from_left: bool,
    to_left: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let source_key = string_arg(source);
    let destination_key = string_arg(destination);
    let mut source_values = match load_redis_list(&source_key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    if source_values.is_empty() {
        return RespValue::Bulk(None);
    }
    let value = if from_left {
        source_values.remove(0)
    } else {
        source_values.pop().unwrap_or_default()
    };
    let mut destination_values = if source_key == destination_key {
        source_values.clone()
    } else {
        match load_redis_list(&destination_key, execute) {
            Ok(values) => values,
            Err(err) => return RespValue::Error(err),
        }
    };
    if to_left {
        destination_values.insert(0, value.clone());
    } else {
        destination_values.push(value.clone());
    }
    if source_key == destination_key {
        match store_redis_list(&source_key, &destination_values, execute) {
            Ok(()) => RespValue::Bulk(Some(value)),
            Err(err) => RespValue::Error(err),
        }
    } else if let Err(err) = store_redis_list(&source_key, &source_values, execute) {
        RespValue::Error(err)
    } else {
        match store_redis_list(&destination_key, &destination_values, execute) {
            Ok(()) => RespValue::Bulk(Some(value)),
            Err(err) => RespValue::Error(err),
        }
    }
}

pub(crate) fn list_insert_response(
    key: &[u8],
    placement: &[u8],
    pivot: &[u8],
    value: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let before = match upper(placement).as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return RespValue::Error("ERR syntax error".to_string()),
    };
    let key = string_arg(key);
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let Some(index) = values.iter().position(|existing| existing == pivot) else {
        return RespValue::Integer(-1);
    };
    let index = if before { index } else { index + 1 };
    values.insert(index, value.to_vec());
    let len = values.len() as i64;
    match store_redis_list(&key, &values, execute) {
        Ok(()) => RespValue::Integer(len),
        Err(err) => RespValue::Error(err),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetAlgebraOp {
    Diff,
    Inter,
    Union,
}

pub(crate) fn sorted_set_members(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<Vec<u8>>, String> {
    match execute(Command::SetMembers {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Members { mut members }) => {
            members.sort();
            Ok(members)
        }
        Ok(_) => Err("ERR invalid smembers response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

pub(crate) fn set_algebra_response(
    keys: &[Vec<u8>],
    op: SetAlgebraOp,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let mut sets = Vec::new();
    for key in keys {
        match sorted_set_members(&string_arg(key), execute) {
            Ok(members) => sets.push(members.into_iter().collect::<HashSet<_>>()),
            Err(err) => return RespValue::Error(err),
        }
    }
    let mut result = match op {
        SetAlgebraOp::Diff => sets.first().cloned().unwrap_or_default(),
        SetAlgebraOp::Inter => sets.first().cloned().unwrap_or_default(),
        SetAlgebraOp::Union => HashSet::new(),
    };
    match op {
        SetAlgebraOp::Diff => {
            for set in sets.iter().skip(1) {
                result.retain(|member| !set.contains(member));
            }
        }
        SetAlgebraOp::Inter => {
            for set in sets.iter().skip(1) {
                result.retain(|member| set.contains(member));
            }
        }
        SetAlgebraOp::Union => {
            for set in sets {
                result.extend(set);
            }
        }
    }
    let mut result = result.into_iter().collect::<Vec<_>>();
    result.sort();
    RespValue::Array(
        result
            .into_iter()
            .map(|member| RespValue::Bulk(Some(member)))
            .collect(),
    )
}

pub(crate) fn load_redis_list(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<Vec<u8>>, String> {
    match execute(Command::StringGet {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Bytes { value: None }) => Ok(Vec::new()),
        Ok(CommandResponse::Bytes { value: Some(value) }) => decode_redis_list(&value),
        Ok(_) => Err("ERR invalid list backing response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

pub(crate) fn store_redis_list(
    key: &str,
    values: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<(), String> {
    execute(Command::StringSet {
        key: key.to_string(),
        value: encode_redis_list(values),
    })
    .map(|_| ())
    .map_err(|err| format!("ERR {err}"))
}

pub(crate) fn encode_redis_list(values: &[Vec<u8>]) -> Vec<u8> {
    let mut out = REDIS_LIST_ENCODING_PREFIX.to_vec();
    out.extend_from_slice(&(values.len() as u64).to_be_bytes());
    for value in values {
        out.extend_from_slice(&(value.len() as u64).to_be_bytes());
        out.extend_from_slice(value);
    }
    out
}

pub(crate) fn decode_redis_list(value: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if !value.starts_with(REDIS_LIST_ENCODING_PREFIX) {
        return Err(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        );
    }
    let mut offset = REDIS_LIST_ENCODING_PREFIX.len();
    let count = read_u64_be(value, &mut offset)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u64_be(value, &mut offset)? as usize;
        let Some(end) = offset.checked_add(len) else {
            return Err("ERR corrupt list encoding".to_string());
        };
        if end > value.len() {
            return Err("ERR corrupt list encoding".to_string());
        }
        out.push(value[offset..end].to_vec());
        offset = end;
    }
    if offset != value.len() {
        return Err("ERR corrupt list encoding".to_string());
    }
    Ok(out)
}
