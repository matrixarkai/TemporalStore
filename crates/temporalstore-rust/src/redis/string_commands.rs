//! Redis string/expire/increment command handlers, extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse};

#[derive(Debug, Clone, Copy)]
pub(crate) struct SetOptions {
    pub(crate) ttl_ms: Option<u64>,
    pub(crate) condition: StringSetCondition,
    pub(crate) return_old: bool,
}

pub(crate) fn parse_set_options(args: &[Vec<u8>]) -> Result<SetOptions, String> {
    let mut options = SetOptions {
        ttl_ms: None,
        condition: StringSetCondition::Always,
        return_old: false,
    };
    let mut index = 0;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "EX" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                if options
                    .ttl_ms
                    .replace(parse_u64(value, "seconds")?.saturating_mul(1000))
                    .is_some()
                {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "PX" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                if options
                    .ttl_ms
                    .replace(parse_u64(value, "milliseconds")?)
                    .is_some()
                {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "NX" => {
                if options.condition != StringSetCondition::Always {
                    return Err("ERR syntax error".to_string());
                }
                options.condition = StringSetCondition::IfNotExists;
                index += 1;
            }
            "XX" => {
                if options.condition != StringSetCondition::Always {
                    return Err("ERR syntax error".to_string());
                }
                options.condition = StringSetCondition::IfExists;
                index += 1;
            }
            "GET" => {
                if options.return_old {
                    return Err("ERR syntax error".to_string());
                }
                options.return_old = true;
                index += 1;
            }
            _ => return Err("ERR syntax error".to_string()),
        }
    }
    Ok(options)
}

pub(crate) fn expire_response(
    args: &[Vec<u8>],
    factor: u64,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match parse_u64(&args[2], "ttl") {
        Ok(ttl) => match execute(Command::CommonExpire {
            key: string_arg(&args[1]),
            ttl_ms: ttl.saturating_mul(factor),
        }) {
            Ok(_) => RespValue::Integer(1),
            Err(err) if err.contains("not_found") || err.contains("key not found") => {
                RespValue::Integer(0)
            }
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        Err(err) => RespValue::Error(err),
    }
}

pub(crate) fn expire_at_response(
    args: &[Vec<u8>],
    factor: u64,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let deadline_ms = match parse_u64(&args[2], "timestamp") {
        Ok(value) => value.saturating_mul(factor),
        Err(err) => return RespValue::Error(err),
    };
    let ttl_ms = deadline_ms.saturating_sub(unix_time_ms()).max(1);
    match execute(Command::CommonExpire {
        key: string_arg(&args[1]),
        ttl_ms,
    }) {
        Ok(_) => RespValue::Integer(1),
        Err(err) if err.contains("not_found") || err.contains("key not found") => {
            RespValue::Integer(0)
        }
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn expire_time_response(
    key: &[u8],
    divisor_ms: u64,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match execute(Command::CommonTtl {
        key: string_arg(key),
    }) {
        Ok(CommandResponse::Integer { value }) if value < 0 => RespValue::Integer(value),
        Ok(CommandResponse::Integer { value }) => {
            RespValue::Integer((unix_time_ms() as i64).saturating_add(value) / divisor_ms as i64)
        }
        Ok(_) => RespValue::Error("ERR invalid expiretime response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn parse_getex_ttl_ms(args: &[Vec<u8>]) -> Result<Option<u64>, String> {
    if args.is_empty() {
        return Ok(None);
    }
    if args.len() == 1 && upper(&args[0]) == "PERSIST" {
        return Ok(None);
    }
    if args.len() != 2 {
        return Err("ERR syntax error".to_string());
    }
    match upper(&args[0]).as_str() {
        "EX" => parse_u64(&args[1], "seconds").map(|value| Some(value.saturating_mul(1000))),
        "PX" => parse_u64(&args[1], "milliseconds").map(Some),
        "EXAT" => {
            let deadline = parse_u64(&args[1], "timestamp")?.saturating_mul(1000);
            Ok(Some(deadline.saturating_sub(unix_time_ms()).max(1)))
        }
        "PXAT" => {
            let deadline = parse_u64(&args[1], "timestamp")?;
            Ok(Some(deadline.saturating_sub(unix_time_ms()).max(1)))
        }
        _ => Err("ERR syntax error".to_string()),
    }
}

pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub(crate) fn string_increment_response(
    key: &[u8],
    increment: i64,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(key);
    let current = match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value: None }) => 0,
        Ok(CommandResponse::Bytes { value: Some(value) }) => match parse_i64_arg(&value, "value") {
            Ok(value) => value,
            Err(_) => return RespValue::Error("ERR value is not an integer".to_string()),
        },
        Ok(_) => return RespValue::Error("ERR invalid incr response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    };
    let Some(next) = current.checked_add(increment) else {
        return RespValue::Error("ERR increment or decrement would overflow".to_string());
    };
    if let Err(err) = execute(Command::StringSet {
        key,
        value: next.to_string().into_bytes(),
    }) {
        return RespValue::Error(format!("ERR {err}"));
    }
    RespValue::Integer(next)
}

pub(crate) fn string_increment_float_response(
    key: &[u8],
    increment: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let increment = match parse_f64_arg(increment, "increment") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(key);
    let current = match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value: None }) => 0.0,
        Ok(CommandResponse::Bytes { value: Some(value) }) => match parse_f64_arg(&value, "value") {
            Ok(value) => value,
            Err(_) => return RespValue::Error("ERR value is not a valid float".to_string()),
        },
        Ok(_) => return RespValue::Error("ERR invalid incrbyfloat response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    };
    let value = format_redis_score(current + increment).into_bytes();
    match execute(Command::StringSet {
        key,
        value: value.clone(),
    }) {
        Ok(_) => RespValue::Bulk(Some(value)),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn hash_increment_float_response(
    key: &[u8],
    field: &[u8],
    increment: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let increment = match parse_f64_arg(increment, "increment") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(key);
    let field = string_arg(field);
    let current = match execute(Command::HashGet {
        key: key.clone(),
        field: field.clone(),
    }) {
        Ok(CommandResponse::Bytes { value: None }) => 0.0,
        Ok(CommandResponse::Bytes { value: Some(value) }) => match parse_f64_arg(&value, "value") {
            Ok(value) => value,
            Err(_) => return RespValue::Error("ERR hash value is not a valid float".to_string()),
        },
        Ok(_) => return RespValue::Error("ERR invalid hincrbyfloat response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    };
    let value = format_redis_score(current + increment).into_bytes();
    match execute(Command::HashSet {
        key,
        field,
        value: value.clone(),
    }) {
        Ok(_) => RespValue::Bulk(Some(value)),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}


pub(crate) fn string_getrange_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let start = match parse_i64_arg(&args[2], "start") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let stop = match parse_i64_arg(&args[3], "end") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    match execute(Command::StringGet {
        key: string_arg(&args[1]),
    }) {
        Ok(CommandResponse::Bytes { value }) => {
            let value = value.unwrap_or_default();
            let (start, stop) = normalize_range(start, stop, value.len());
            RespValue::Bulk(Some(value[start..stop].to_vec()))
        }
        Ok(_) => RespValue::Error("ERR invalid getrange response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn string_setrange_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let offset = match parse_usize(&args[2], "offset") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value }) => {
            let mut value = value.unwrap_or_default();
            let Some(end) = offset.checked_add(args[3].len()) else {
                return RespValue::Error("ERR string exceeds maximum allowed size".to_string());
            };
            if value.len() < end {
                value.resize(end, 0);
            }
            value[offset..end].copy_from_slice(&args[3]);
            let len = value.len() as i64;
            match execute(Command::StringSet { key, value }) {
                Ok(_) => RespValue::Integer(len),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        Ok(_) => RespValue::Error("ERR invalid setrange response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}


