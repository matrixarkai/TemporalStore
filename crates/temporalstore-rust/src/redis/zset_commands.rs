//! Redis sorted-set (ZSET) command handlers, extracted from redis.rs.

use super::*;
use std::collections::{HashMap, HashSet};
use crate::types::{Command, CommandResponse};

pub(super) fn zrange_response(
    args: &[Vec<u8>],
    reverse: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let start = match parse_i64_arg(&args[2], "start") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let stop = match parse_i64_arg(&args[3], "stop") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let with_scores = args
        .get(4)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            if reverse {
                values.reverse();
            }
            let (start, stop) = normalize_range(start, stop, values.len());
            let selected = &values[start..stop];
            if with_scores {
                RespValue::Array(
                    selected
                        .iter()
                        .flat_map(|(member, score)| {
                            [
                                RespValue::Bulk(Some(member.clone())),
                                RespValue::Bulk(Some(format_redis_score(*score).into_bytes())),
                            ]
                        })
                        .collect(),
                )
            } else {
                RespValue::Array(
                    selected
                        .iter()
                        .map(|(member, _)| RespValue::Bulk(Some(member.clone())))
                        .collect(),
                )
            }
        }
        Err(err) => RespValue::Error(err),
    }
}

pub(super) fn zrank_response(
    args: &[Vec<u8>],
    reverse: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            if reverse {
                values.reverse();
            }
            values
                .iter()
                .position(|(member, _)| member == &args[2])
                .map(|rank| RespValue::Integer(rank as i64))
                .unwrap_or(RespValue::Bulk(None))
        }
        Err(err) => RespValue::Error(err),
    }
}

pub(super) fn zincrby_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let increment = match parse_f64_arg(&args[2], "increment") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let next_score =
        if let Some((_, score)) = values.iter_mut().find(|(member, _)| member == &args[3]) {
            *score += increment;
            *score
        } else {
            values.push((args[3].clone(), increment));
            increment
        };
    // C++ ZIncrBy rejects a non-finite result ("resulting score is not a valid float") and
    // leaves the set unchanged; Rust previously stored inf and returned "inf".
    if !next_score.is_finite() {
        return RespValue::Error("ERR resulting score is not a valid float".to_string());
    }
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Bulk(Some(format_redis_score(next_score).into_bytes())),
        Err(err) => RespValue::Error(err),
    }
}

pub(super) fn zrange_by_score_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let min = match parse_score_bound(&args[2], "min") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let max = match parse_score_bound(&args[3], "max") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let with_scores = args
        .get(4)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            let selected = values
                .into_iter()
                .filter(|(_, score)| *score >= min && *score <= max);
            if with_scores {
                RespValue::Array(
                    selected
                        .flat_map(|(member, score)| {
                            [
                                RespValue::Bulk(Some(member)),
                                RespValue::Bulk(Some(format_redis_score(score).into_bytes())),
                            ]
                        })
                        .collect(),
                )
            } else {
                RespValue::Array(
                    selected
                        .map(|(member, _)| RespValue::Bulk(Some(member)))
                        .collect(),
                )
            }
        }
        Err(err) => RespValue::Error(err),
    }
}

pub(super) fn zpop_response(
    args: &[Vec<u8>],
    reverse: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let count = match args.get(2) {
        Some(value) => match parse_usize(value, "count") {
            Ok(value) => value,
            Err(err) => return RespValue::Error(err),
        },
        None => 1,
    };
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    sort_zset_values(&mut values);
    if reverse {
        values.reverse();
    }
    let selected = values.iter().take(count).cloned().collect::<Vec<_>>();
    values.retain(|(member, _)| !selected.iter().any(|(selected, _)| selected == member));
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Array(
            selected
                .into_iter()
                .flat_map(|(member, score)| {
                    [
                        RespValue::Bulk(Some(member)),
                        RespValue::Bulk(Some(format_redis_score(score).into_bytes())),
                    ]
                })
                .collect(),
        ),
        Err(err) => RespValue::Error(err),
    }
}

pub(super) fn zremrangebyscore_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let min = match parse_score_bound(&args[2], "min") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let max = match parse_score_bound(&args[3], "max") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let before = values.len();
    values.retain(|(_, score)| *score < min || *score > max);
    let removed = before.saturating_sub(values.len()) as i64;
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Integer(removed),
        Err(err) => RespValue::Error(err),
    }
}

pub(super) fn zremrangebyrank_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let start = match parse_i64_arg(&args[2], "start") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let stop = match parse_i64_arg(&args[3], "stop") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    sort_zset_values(&mut values);
    let (start, stop) = normalize_range(start, stop, values.len());
    let removed_members = values[start..stop]
        .iter()
        .map(|(member, _)| member.clone())
        .collect::<HashSet<_>>();
    let removed = removed_members.len() as i64;
    values.retain(|(member, _)| !removed_members.contains(member));
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Integer(removed),
        Err(err) => RespValue::Error(err),
    }
}

pub(super) fn zrandmember_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let count = match args.get(2) {
        Some(value) => match parse_i64_arg(value, "count") {
            Ok(value) => Some(value),
            Err(err) => return RespValue::Error(err),
        },
        None => None,
    };
    let with_scores = args
        .get(3)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            match count {
                Some(count) => {
                    let mut selected = Vec::new();
                    let mut push = |member: Vec<u8>, score: f64| {
                        selected.push(RespValue::Bulk(Some(member)));
                        if with_scores {
                            selected
                                .push(RespValue::Bulk(Some(format_redis_score(score).into_bytes())));
                        }
                    };
                    if count >= 0 {
                        // Positive: up to `count` DISTINCT members, naturally bounded by the
                        // set size (iterating `values`), so a huge count can't busy-loop.
                        for (member, score) in values.into_iter().take(count as usize) {
                            push(member, score);
                        }
                    } else if !values.is_empty() {
                        // Negative: |count| members WITH repetition. checked_neg guards
                        // i64::MIN (C++ n=-count stays negative -> zero fill) instead of
                        // unsigned_abs()'s 2^63 unbounded allocation (OOM DoS).
                        let repeat = count.checked_neg().map(|n| n as usize).unwrap_or(0);
                        for index in 0..repeat {
                            let (member, score) = values[index % values.len()].clone();
                            push(member, score);
                        }
                    }
                    RespValue::Array(selected)
                }
                None => values
                    .into_iter()
                    .next()
                    .map(|(member, _)| RespValue::Bulk(Some(member)))
                    .unwrap_or(RespValue::Bulk(None)),
            }
        }
        Err(err) => RespValue::Error(err),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ZSetAlgebraOp {
    Diff,
    Inter,
    Union,
}

pub(super) fn zset_algebra_response(
    args: &[Vec<u8>],
    op: ZSetAlgebraOp,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key_count = match parse_usize(&args[1], "numkeys") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    if key_count == 0 || args.len() < key_count + 2 {
        return RespValue::Error("ERR syntax error".to_string());
    }
    let with_scores = args
        .get(key_count + 2)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    let mut sets = Vec::new();
    for key in args.iter().skip(2).take(key_count) {
        match load_redis_zset(&string_arg(key), execute) {
            Ok(values) => sets.push(values.into_iter().collect::<HashMap<_, _>>()),
            Err(err) => return RespValue::Error(err),
        }
    }
    let mut result = match op {
        ZSetAlgebraOp::Diff | ZSetAlgebraOp::Inter => sets.first().cloned().unwrap_or_default(),
        ZSetAlgebraOp::Union => HashMap::new(),
    };
    match op {
        ZSetAlgebraOp::Diff => {
            for set in sets.iter().skip(1) {
                result.retain(|member, _| !set.contains_key(member));
            }
        }
        ZSetAlgebraOp::Inter => {
            for set in sets.iter().skip(1) {
                result.retain(|member, score| {
                    if let Some(other_score) = set.get(member) {
                        *score += *other_score;
                        true
                    } else {
                        false
                    }
                });
            }
        }
        ZSetAlgebraOp::Union => {
            for set in sets {
                for (member, score) in set {
                    *result.entry(member).or_insert(0.0) += score;
                }
            }
        }
    }
    let mut values = result.into_iter().collect::<Vec<_>>();
    sort_zset_values(&mut values);
    if with_scores {
        RespValue::Array(
            values
                .into_iter()
                .flat_map(|(member, score)| {
                    [
                        RespValue::Bulk(Some(member)),
                        RespValue::Bulk(Some(format_redis_score(score).into_bytes())),
                    ]
                })
                .collect(),
        )
    } else {
        RespValue::Array(
            values
                .into_iter()
                .map(|(member, _)| RespValue::Bulk(Some(member)))
                .collect(),
        )
    }
}
