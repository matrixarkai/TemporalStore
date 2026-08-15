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
