// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis string/expire/increment command handlers, extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse};

#[derive(Debug, Clone, Copy)]
pub(crate) struct SetOptions {
    pub(crate) ttl_ms: Option<u64>,
    pub(crate) condition: StringSetCondition,
    pub(crate) return_old: bool,
    /// `KEEPTTL`: replace the value, leave the deadline alone.
    ///
    /// Without it, `SET` has only two answers for the deadline on the key it is replacing --
    /// arm this new one, or throw the old one away -- and a caller who wanted the value
    /// changed and the countdown kept had to read the remaining time and write it back, which
    /// races the countdown it is trying to preserve and loses whatever elapsed in between.
    pub(crate) keep_ttl: bool,
}

pub(crate) fn parse_set_options(args: &[Vec<u8>]) -> Result<SetOptions, String> {
    let mut options = SetOptions {
        ttl_ms: None,
        condition: StringSetCondition::Always,
        return_old: false,
        keep_ttl: false,
    };
    let mut index = 0;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "EX" => {
                // `KEEPTTL` and an arming TTL are contradictory requests, so the pair is a
                // syntax error rather than a silent precedence rule. Checked BEFORE the
                // number is parsed so `SET k v KEEPTTL EX 0` is refused for the reason it is
                // actually wrong, not for the expire time.
                if options.keep_ttl {
                    return Err("ERR syntax error".to_string());
                }
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                let seconds = parse_u64(value, "seconds")?;
                // rejects a non-positive expiry rather than writing an already-expired key.
                if seconds == 0 {
                    return Err("ERR invalid expire time in set".to_string());
                }
                if options.ttl_ms.replace(seconds.saturating_mul(1000)).is_some() {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "PX" => {
                if options.keep_ttl {
                    return Err("ERR syntax error".to_string());
                }
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                let milliseconds = parse_u64(value, "milliseconds")?;
                if milliseconds == 0 {
                    return Err("ERR invalid expire time in set".to_string());
                }
                if options.ttl_ms.replace(milliseconds).is_some() {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "KEEPTTL" => {
                // Both directions of the contradiction are refused: `EX`/`PX` beside
                // `KEEPTTL` (checked in those arms) and `KEEPTTL` beside one already parsed
                // (checked here). A repeat of `KEEPTTL` alone is a syntax error too, matching
                // how `EX`, `NX` and `GET` each refuse their own repetition above and below.
                if options.ttl_ms.is_some() || options.keep_ttl {
                    return Err("ERR syntax error".to_string());
                }
                options.keep_ttl = true;
                index += 1;
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

/// A deadline that has ALREADY PASSED is a deletion, not a deadline.
///
/// The three commands below can all be handed a moment that is not in the future: `EXPIRE` and
/// `PEXPIRE` through a negative or zero relative time, `EXPIREAT` / `PEXPIREAT` and `GETEX
/// EXAT` / `PXAT` through an absolute timestamp that is already behind us. There is no such
/// thing as a key that expires in the past, so the answer is the same in every case -- the key
/// goes now -- and the integer reply is about whether there WAS a key, exactly as it is when
/// the deadline is in the future.
///
/// WHAT THIS REPLACES, AND WHY IT WAS WORSE THAN IT LOOKED. The absolute forms used to clamp
/// the computed remaining time with `.max(1)`, arming a deadline one millisecond out instead.
/// That is not a rounding difference: it leaves a window in which the key is still there and
/// still readable, so `EXPIREAT k 1` followed immediately by `GET k` could hand back a value
/// the caller had just asked to be rid of, and whether it did depended on how fast the next
/// command arrived. The relative forms were worse still -- a negative time failed `parse_u64`
/// and came back as a syntax error, so a caller using `EXPIRE key -1` to discard a key was
/// told their command was malformed rather than having it obeyed.
///
/// The integer is deliberately 0 for a key that is not there, and NOT an error: asking to
/// discard something already gone is a no-op that succeeded, which is what every other
/// deletion on this surface answers.
fn discard_key_now(
    key: &str,
    state: &mut RedisCommandState,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match execute(Command::CommonExists {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Integer { value }) if value > 0 => {
            if let Err(err) = execute(Command::CommonDelete {
                key: key.to_string(),
            }) {
                return RespValue::Error(format!("ERR {err}"));
            }
            state.keyspace.remove(key);
            RespValue::Integer(1)
        }
        Ok(CommandResponse::Integer { .. }) => RespValue::Integer(0),
        Ok(_) => RespValue::Error("ERR invalid exists response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn expire_response(
    args: &[Vec<u8>],
    factor: i64,
    state: &mut RedisCommandState,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    // Signed on purpose: a negative relative time is a legal request to discard the key, not
    // a malformed number. `parse_u64` rejected it.
    let ttl = match parse_i64_arg(&args[2], "ttl") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    if ttl <= 0 {
        return discard_key_now(&key, state, execute);
    }
    match execute(Command::CommonExpire {
        key,
        ttl_ms: ttl.saturating_mul(factor) as u64,
    }) {
        Ok(_) => RespValue::Integer(1),
        Err(err) if err.contains("not_found") || err.contains("key not found") => {
            RespValue::Integer(0)
        }
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn expire_at_response(
    args: &[Vec<u8>],
    factor: i64,
    state: &mut RedisCommandState,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let deadline_ms = match parse_i64_arg(&args[2], "timestamp") {
        Ok(value) => value.saturating_mul(factor),
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    let remaining_ms = deadline_ms.saturating_sub(unix_time_ms() as i64);
    if remaining_ms <= 0 {
        return discard_key_now(&key, state, execute);
    }
    match execute(Command::CommonExpire {
        key,
        ttl_ms: remaining_ms as u64,
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

/// What the option words on a `GETEX` ask for.
///
/// THREE outcomes, not two. `Option<u64>` could only spell two of them, and it spelled the
/// wrong pair: `PERSIST` and "no option words at all" both came back as `None`, and the caller
/// read `None` as "leave the deadline alone". So `GETEX key PERSIST` returned the value,
/// reported success, and left the key counting down -- the caller asked for the key to be kept
/// forever and got a key that disappears, with no error anywhere to say so.
///
/// Keeping the three apart in the TYPE is what stops that from coming back: a new option word
/// has to say which of the three it is, and a caller cannot silently collapse two of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GetExDeadline {
    /// No option words: `GETEX key` reads like `GET key` and touches no deadline.
    Unchanged,
    /// `PERSIST`: remove the deadline, making the key permanent.
    Persist,
    /// `EX` / `PX` / `EXAT` / `PXAT`: arm this deadline, as milliseconds from now.
    Arm(u64),
    /// `EXAT` / `PXAT` naming a moment that has already passed. There is no deadline to arm;
    /// the key goes now. Folding this into `Arm(1)` is what the code used to do, and it left
    /// the key readable for as long as it took the next command to arrive.
    Discard,
}

pub(crate) fn parse_getex_ttl_ms(args: &[Vec<u8>]) -> Result<GetExDeadline, String> {
    if args.is_empty() {
        return Ok(GetExDeadline::Unchanged);
    }
    if args.len() == 1 && upper(&args[0]) == "PERSIST" {
        return Ok(GetExDeadline::Persist);
    }
    if args.len() != 2 {
        return Err("ERR syntax error".to_string());
    }
    match upper(&args[0]).as_str() {
        "EX" => match parse_u64(&args[1], "seconds")? {
            0 => Err("ERR invalid expire time in getex".to_string()),
            seconds => Ok(GetExDeadline::Arm(seconds.saturating_mul(1000))),
        },
        "PX" => match parse_u64(&args[1], "milliseconds")? {
            0 => Err("ERR invalid expire time in getex".to_string()),
            milliseconds => Ok(GetExDeadline::Arm(milliseconds)),
        },
        "EXAT" => Ok(absolute_deadline(
            parse_i64_arg(&args[1], "timestamp")?.saturating_mul(1000),
        )),
        "PXAT" => Ok(absolute_deadline(parse_i64_arg(&args[1], "timestamp")?)),
        _ => Err("ERR syntax error".to_string()),
    }
}

/// An absolute deadline, resolved against one reading of the clock: still ahead, so arm what
/// is left of it; or already behind, so discard the key.
fn absolute_deadline(deadline_ms: i64) -> GetExDeadline {
    let remaining_ms = deadline_ms.saturating_sub(unix_time_ms() as i64);
    if remaining_ms <= 0 {
        GetExDeadline::Discard
    } else {
        GetExDeadline::Arm(remaining_ms as u64)
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


