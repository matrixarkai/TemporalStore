// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis set-algebra commands (SDIFF / SINTER / SUNION), extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse};
use std::collections::HashSet;

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

/// Whether `member` is in the set at `key`: one lookup in the member map, not a listing.
pub(crate) fn set_has_member(
    key: &str,
    member: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<bool, String> {
    match execute(Command::SetIsMember {
        key: key.to_string(),
        member: member.to_vec(),
    }) {
        Ok(CommandResponse::Integer { value }) => Ok(value != 0),
        Ok(_) => Err("ERR invalid sismember response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

/// The first `wanted` members of the set in byte order, read a page at a time from the member
/// map. A set smaller than `wanted` answers all of it. Nothing is read past the members asked for,
/// where listing the set and taking a prefix read every one.
pub(crate) fn first_set_members(
    key: &str,
    wanted: usize,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<Vec<u8>>, String> {
    // The scan command refuses a page over this many members.
    const PAGE: usize = 10_000;
    let mut out: Vec<Vec<u8>> = Vec::new();
    while out.len() < wanted {
        let limit = (wanted - out.len()).min(PAGE);
        let after = out.last().cloned().unwrap_or_default();
        match execute(Command::SetScan {
            key: key.to_string(),
            after,
            limit,
        }) {
            Ok(CommandResponse::Members { members }) => {
                let short = members.len() < limit;
                out.extend(members);
                if short {
                    break;
                }
            }
            Ok(_) => return Err("ERR invalid sscan response".to_string()),
            Err(err) => return Err(format!("ERR {err}")),
        }
    }
    Ok(out)
}

pub(crate) fn set_algebra_response(
    keys: &[Vec<u8>],
    op: SetAlgebraOp,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let mut sets = Vec::new();
    for key in keys {
        match sorted_set_members(&name_arg(key), execute) {
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
