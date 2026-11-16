// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `REMOVE` and `DELETE` of the cell protocol as the one command that removes cells
//! ([`Command::CellRemove`]), and the engine's receipt back as a wire response. Like the rest of
//! the translator it reads no engine state and writes nothing; a refused request yields no command.
//!
//! # REMOVE
//!
//! The entity, and the `kind` and `channel` of the key filter, name the key; the span names the
//! samples (an absent span is every sample); the key filter's label, when the filter is
//! present, is an equality predicate on the table's label, exactly as the selector label is for a
//! read; `members` and `excluded_members` are the member set. A span in microseconds covers the
//! samples whose stamp, in whole milliseconds, lies inside it: the start rounds up and the end
//! rounds down, both inclusive, and a span that holds no whole millisecond is refused.
//!
//! # DELETE
//!
//! The selector names the key (`kind`, `channel`), the label predicate and the member set, over
//! every sample. It maps to the same command and never to the deletion of the key: a delete that
//! names no label and no member would empty the key and is refused, the way a removal is.
//!
//! Every translated spec also passes the engine's own admission ([`window_reduce_remove::validate_spec`]),
//! so a request the translator accepts is one the command accepts.

use crate::engine::window_reduce_remove;
use super::cell_set::{within, EntityValue};
use super::{invalid, key_of, entity_of, wire, WindowReduceStatusCode, Refusal, TableProfile, MAX_MEMBER_LIST};
use crate::types::{
    CellMemberFilter, CellMemberId, CellRemoveSpec, Command, FeatureFilter, FeatureFilterOp,
};

/// The members of a selector or a remove request: `members` is the include set (present when
/// flagged or when it holds anything) and `excluded` the exclude set.
fn members_of(members: &[i64], flagged: bool, excluded: &[i64]) -> CellMemberFilter {
    let id = |v: &i64| CellMemberId::U64(*v as u64);
    CellMemberFilter {
        include: (flagged || !members.is_empty()).then(|| members.iter().map(id).collect()),
        exclude: excluded.iter().map(id).collect(),
    }
}

fn label_filter(profile: &TableProfile, label: i32) -> Result<Option<FeatureFilter>, Refusal> {
    match profile.labels.first() {
        Some(name) => Ok(Some(FeatureFilter {
            field: name.clone(),
            op: FeatureFilterOp::Equal,
            value: u64::try_from(label).map_err(|_| invalid(format!("label {label} is negative")))?,
        })),
        None if label != 0 => Err(invalid("this table declares no label, so the label must be 0")),
        None => Ok(None),
    }
}

/// The stamps `[start, end]` of a span in microseconds, as whole milliseconds.
fn span_ms(span: &wire::TimeSpan) -> Result<(u64, u64), Refusal> {
    if span.start_us < 0 || span.end_us < 0 {
        return Err(invalid(format!("span {}..{} holds a negative time", span.start_us, span.end_us)));
    }
    if span.start_us > span.end_us {
        return Err(invalid(format!("span starts at {}, after it ends at {}", span.start_us, span.end_us)));
    }
    let first = span.start_us.div_euclid(1000) + i64::from(span.start_us.rem_euclid(1000) != 0);
    let last = span.end_us.div_euclid(1000);
    if first > last {
        return Err(invalid("span holds no whole millisecond"));
    }
    Ok((first as u64, last as u64))
}

fn command_of(key: String, spec: CellRemoveSpec) -> Result<Command, Refusal> {
    window_reduce_remove::validate_spec(&spec).map_err(invalid)?;
    Ok(Command::CellRemove { key, spec })
}

/// A remove request as the engine's removal.
pub(crate) fn translate_remove(
    profile: &TableProfile,
    request: &wire::WindowReduceRemoveRequest,
) -> Result<Command, Refusal> {
    within("members", request.members.len(), MAX_MEMBER_LIST).map_err(invalid)?;
    within("excluded members", request.excluded_members.len(), MAX_MEMBER_LIST).map_err(invalid)?;
    let entity: EntityValue = entity_of(profile, request.entity, "", &[])?;
    let (channel, kind, label) = request.key_filter.as_ref().map_or((0, 0, None), |f| (f.channel, f.kind, Some(f.label)));
    let key = key_of(profile, entity, channel, kind)?;
    let (start_ms, end_ms) = match request.span.as_ref() {
        Some(span) => {
            let (first, last) = span_ms(span)?;
            (Some(first), Some(last))
        }
        None => (None, None),
    };
    let filters = match label {
        Some(label) => label_filter(profile, label)?.into_iter().collect(),
        None => Vec::new(),
    };
    command_of(
        key,
        CellRemoveSpec {
            start_ms,
            end_ms,
            members: members_of(&request.members, request.has_members, &request.excluded_members),
            filters,
        },
    )
}

/// A delete request as the engine's removal.
pub(crate) fn translate_delete(
    profile: &TableProfile,
    request: &wire::WindowReduceDeleteRequest,
) -> Result<Command, Refusal> {
    let selector = request.selector.as_ref().ok_or_else(|| invalid("a delete needs a selector"))?;
    within("members", selector.members.len(), MAX_MEMBER_LIST).map_err(invalid)?;
    within("excluded members", selector.excluded_members.len(), MAX_MEMBER_LIST).map_err(invalid)?;
    let entity = entity_of(profile, request.entity, "", &[])?;
    let key = key_of(profile, entity, selector.channel, selector.kind)?;
    command_of(
        key,
        CellRemoveSpec {
            start_ms: None,
            end_ms: None,
            members: members_of(&selector.members, selector.has_members, &selector.excluded_members),
            filters: label_filter(profile, selector.label)?.into_iter().collect(),
        },
    )
}

/// The counts of the engine's receipt, in the order the wire carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Receipt {
    pub(crate) snapshots_before: u64,
    pub(crate) snapshots_after: u64,
    pub(crate) cells_before: u64,
    pub(crate) cells_removed: u64,
}

pub(crate) fn remove_response(request: &wire::WindowReduceRemoveRequest, receipt: Receipt) -> wire::WindowReduceRemoveResponse {
    wire::WindowReduceRemoveResponse {
        code: WindowReduceStatusCode::Ok as i32,
        span: request.span.clone(),
        samples_before: receipt.snapshots_before,
        samples_after: receipt.snapshots_after,
        cells_before: receipt.cells_before,
        cells_removed: receipt.cells_removed,
        ..Default::default()
    }
}

pub(crate) fn refused_remove_response(request: &wire::WindowReduceRemoveRequest, refusal: &Refusal) -> wire::WindowReduceRemoveResponse {
    wire::WindowReduceRemoveResponse {
        code: refusal.code as i32,
        span: request.span.clone(),
        message: refusal.message.clone(),
        has_message: true,
        ..Default::default()
    }
}

pub(crate) fn delete_response(receipt: Receipt) -> wire::WindowReduceDeleteResponse {
    wire::WindowReduceDeleteResponse {
        code: WindowReduceStatusCode::Ok as i32,
        samples_before: receipt.snapshots_before,
        samples_after: receipt.snapshots_after,
        cells_before: receipt.cells_before,
        cells_removed: receipt.cells_removed,
        ..Default::default()
    }
}

pub(crate) fn refused_delete_response(refusal: &Refusal) -> wire::WindowReduceDeleteResponse {
    wire::WindowReduceDeleteResponse { code: refusal.code as i32, message: refusal.message.clone(), ..Default::default() }
}

#[cfg(test)]
#[path = "window_reduce_protocol_remove_tests.rs"]
mod tests;
