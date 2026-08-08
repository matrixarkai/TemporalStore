//! Redis sorted-set (ZSET) storage encoding + index/range/numeric helpers, extracted from redis.rs.

use super::*;
use super::encoding::REDIS_ZSET_ENCODING_PREFIX;

pub(crate) fn load_redis_zset(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<(Vec<u8>, f64)>, String> {
    match execute(Command::StringGet {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Bytes { value: None }) => Ok(Vec::new()),
        Ok(CommandResponse::Bytes { value: Some(value) }) => decode_redis_zset(&value),
        Ok(_) => Err("ERR invalid zset backing response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

pub(crate) fn store_redis_zset(
    key: &str,
    values: &[(Vec<u8>, f64)],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<(), String> {
    let mut sorted = values.to_vec();
    sort_zset_values(&mut sorted);
    execute(Command::StringSet {
        key: key.to_string(),
        value: encode_redis_zset(&sorted),
    })
    .map(|_| ())
    .map_err(|err| format!("ERR {err}"))
}

pub(crate) fn encode_redis_zset(values: &[(Vec<u8>, f64)]) -> Vec<u8> {
    let mut out = REDIS_ZSET_ENCODING_PREFIX.to_vec();
    out.extend_from_slice(&(values.len() as u64).to_be_bytes());
    for (member, score) in values {
        out.extend_from_slice(&score.to_be_bytes());
        out.extend_from_slice(&(member.len() as u64).to_be_bytes());
        out.extend_from_slice(member);
    }
    out
}

pub(crate) fn decode_redis_zset(value: &[u8]) -> Result<Vec<(Vec<u8>, f64)>, String> {
    if !value.starts_with(REDIS_ZSET_ENCODING_PREFIX) {
        return Err(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        );
    }
    let mut offset = REDIS_ZSET_ENCODING_PREFIX.len();
    let count = read_u64_be(value, &mut offset)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let score = read_f64_be(value, &mut offset)?;
        let len = read_u64_be(value, &mut offset)? as usize;
        let Some(end) = offset.checked_add(len) else {
            return Err("ERR corrupt zset encoding".to_string());
        };
        if end > value.len() {
            return Err("ERR corrupt zset encoding".to_string());
        }
        out.push((value[offset..end].to_vec(), score));
        offset = end;
    }
    if offset != value.len() {
        return Err("ERR corrupt zset encoding".to_string());
    }
    Ok(out)
}

pub(crate) fn upsert_zset_member(values: &mut Vec<(Vec<u8>, f64)>, member: Vec<u8>, score: f64) -> bool {
    if let Some((_, existing_score)) = values.iter_mut().find(|(existing, _)| existing == &member) {
        *existing_score = score;
        false
    } else {
        values.push((member, score));
        true
    }
}

pub(crate) fn sort_zset_values(values: &mut [(Vec<u8>, f64)]) {
    values.sort_by(|(left_member, left_score), (right_member, right_score)| {
        left_score
            .total_cmp(right_score)
            .then_with(|| left_member.cmp(right_member))
    });
}

pub(crate) fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let index = if index < 0 { len + index } else { index };
    if index < 0 || index >= len {
        None
    } else {
        Some(index as usize)
    }
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

pub(crate) fn read_u64_be(value: &[u8], offset: &mut usize) -> Result<u64, String> {
    let Some(end) = offset.checked_add(8) else {
        return Err("ERR corrupt redis compatibility encoding".to_string());
    };
    let bytes = value
        .get(*offset..end)
        .ok_or_else(|| "ERR corrupt redis compatibility encoding".to_string())?;
    *offset = end;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("slice length checked"),
    ))
}

pub(crate) fn read_f64_be(value: &[u8], offset: &mut usize) -> Result<f64, String> {
    let Some(end) = offset.checked_add(8) else {
        return Err("ERR corrupt zset encoding".to_string());
    };
    let bytes = value
        .get(*offset..end)
        .ok_or_else(|| "ERR corrupt zset encoding".to_string())?;
    *offset = end;
    Ok(f64::from_be_bytes(
        bytes.try_into().expect("slice length checked"),
    ))
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

/// Parse a ZSet score-RANGE bound. Unlike a stored score (parse_f64_arg, must be finite),
/// range bounds accept the canonical -inf / +inf / inf tokens (C++ ParseZRangeBound), so the
/// idiomatic `ZRANGEBYSCORE key -inf +inf` (and ZCOUNT / ZREMRANGEBYSCORE) work.
pub(crate) fn parse_score_bound(value: &[u8], name: &str) -> Result<f64, String> {
    let text = std::str::from_utf8(value).map_err(|_| format!("ERR {name} must be a float"))?;
    match text.trim().to_ascii_lowercase().as_str() {
        "-inf" => Ok(f64::NEG_INFINITY),
        "+inf" | "inf" => Ok(f64::INFINITY),
        other => other
            .parse::<f64>()
            .ok()
            .filter(|parsed| parsed.is_finite())
            .ok_or_else(|| format!("ERR {name} must be a float")),
    }
}

