// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::super::{LabelRule, Shape};
use crate::engine::cell_set::{
    encode_key, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue, LaneReducer, LaneSpec,
};
use super::*;
use prost::Message;
use wire::{WindowReduceDeleteRequest, WindowReduceRemoveRequest, WindowReduceSelector, KeyFilter, TimeSpan};

fn profile(labels: &[&str]) -> TableProfile {
    TableProfile {
        name: "stats".to_string(),
        entity: EntityType::U64,
        dimensions: vec![
            DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true },
            DimSpec { name: "kind".to_string(), ty: DimType::U16, required: false },
        ],
        channel: Some(0),
        kind: Some(1),
        labels: labels.iter().map(|l| l.to_string()).collect(),
        label_rule: LabelRule::FromRequest,
        lanes: vec![
            LaneSpec { name: "impressions".to_string(), reducer: LaneReducer::Sum },
            LaneSpec { name: "clicks".to_string(), reducer: LaneReducer::Sum },
        ],
        shape: Shape::Pair,
        refuse_duplicates: false,
        max_past_us: 5 * 365 * 24 * 3600 * 1_000_000,
        max_future_us: 2 * 24 * 3600 * 1_000_000,
    }
}

fn key(profile: &TableProfile, entity: u64, channel: u16, kind: u16) -> String {
    encode_key(
        &profile.header().key,
        &CellKey {
            entity: EntityValue::U64(entity),
            dims: vec![Some(DimValue::U16(channel)), Some(DimValue::U16(kind))],
        },
    )
    .unwrap()
}

fn request(span: Option<(i64, i64)>, filter: Option<KeyFilter>) -> WindowReduceRemoveRequest {
    WindowReduceRemoveRequest {
        entity: 7,
        span: span.map(|(start_us, end_us)| TimeSpan { start_us, end_us }),
        table: b"stats".to_vec(),
        key_filter: filter,
        ..Default::default()
    }
}

fn filter(label: i32) -> Option<KeyFilter> {
    Some(KeyFilter { label, kind: 3, channel: 4 })
}

fn spec_of(command: Command) -> CellRemoveSpec {
    match command {
        Command::CellRemove { spec, .. } => spec,
        other => panic!("not a removal: {other:?}"),
    }
}

fn slot(value: u64) -> FeatureFilter {
    FeatureFilter { field: "slot".to_string(), op: FeatureFilterOp::Equal, value }
}

#[test]
fn a_remove_names_its_key_its_span_in_whole_milliseconds_and_its_label_predicate() {
    let table = profile(&["slot"]);
    let command = translate_remove(&table, &request(Some((1_500, 2_999)), filter(5))).unwrap();
    // 1 500 us is 1.5 ms and rounds up to the stamp 2; 2 999 us rounds down to the stamp 2.
    let Command::CellRemove { key: named, spec } = command else { panic!("not a removal") };
    assert_eq!(named, key(&table, 7, 4, 3));
    assert_eq!(spec, CellRemoveSpec { start_ms: Some(2), end_ms: Some(2), members: Default::default(), filters: vec![slot(5)] });

    // An exact millisecond on both ends is that one stamp; a span of two is two.
    let one = spec_of(translate_remove(&table, &request(Some((1_000, 1_000)), filter(5))).unwrap());
    assert_eq!((one.start_ms, one.end_ms), (Some(1), Some(1)));
    let two = spec_of(translate_remove(&table, &request(Some((0, 1_000)), filter(0))).unwrap());
    assert_eq!((two.start_ms, two.end_ms), (Some(0), Some(1)));
    // The label is an equality even at 0, as it is for a read, and only when the filter is present.
    assert_eq!(two.filters, vec![slot(0)]);
    let bare = spec_of(translate_remove(&table, &request(Some((0, 1_000)), None)).unwrap());
    assert!(bare.filters.is_empty());
}

#[test]
fn a_span_that_holds_no_whole_millisecond_or_runs_backwards_or_is_negative_is_refused() {
    let table = profile(&["slot"]);
    let refused = |span: (i64, i64)| translate_remove(&table, &request(Some(span), filter(1))).unwrap_err();
    assert!(refused((1, 999)).message.contains("no whole millisecond"));
    // One microsecond either side of a millisecond is a hole; the millisecond itself is not.
    assert!(refused((1_001, 1_999)).message.contains("no whole millisecond"));
    assert!(translate_remove(&table, &request(Some((1_000, 1_999)), filter(1))).is_ok());
    assert!(refused((5, 4)).message.contains("after it ends"));
    assert!(refused((-1, 5_000)).message.contains("negative"));
    assert!(refused((0, -1)).message.contains("negative"));
    assert_eq!(refused((5, 4)).code, WindowReduceStatusCode::InvalidArgument);
}

#[test]
fn a_remove_that_restricts_nothing_is_refused_and_a_member_set_or_a_span_restricts() {
    let table = profile(&["slot"]);
    // No span, no label filter, no member: the whole key would go.
    let err = translate_remove(&table, &request(None, None)).unwrap_err();
    assert!(err.message.contains("would empty the key"), "{err:?}");
    // Each of the three alone is enough.
    assert!(translate_remove(&table, &request(Some((0, 5_000)), None)).is_ok());
    assert!(translate_remove(&table, &request(None, filter(2))).is_ok());
    let members = WindowReduceRemoveRequest { members: vec![9, 3], ..request(None, None) };
    let spec = spec_of(translate_remove(&table, &members).unwrap());
    assert_eq!(spec.members.include, Some(vec![CellMemberId::U64(9), CellMemberId::U64(3)]));
    let spare = WindowReduceRemoveRequest { excluded_members: vec![4], ..request(None, None) };
    assert_eq!(spec_of(translate_remove(&table, &spare).unwrap()).members.exclude, vec![CellMemberId::U64(4)]);
    // A flagged, empty include set is refused as it is on a read; an unflagged one restricts nothing.
    let flagged = WindowReduceRemoveRequest { has_members: true, ..request(Some((0, 5_000)), None) };
    assert!(translate_remove(&table, &flagged).unwrap_err().message.contains("names no member"));
    assert!(translate_remove(&table, &request(Some((0, 5_000)), None)).is_ok());
}

#[test]
fn member_lists_are_accepted_at_1024_and_refused_at_1025() {
    let table = profile(&["slot"]);
    let list = |n: i64| (1..=n).collect::<Vec<_>>();
    assert!(translate_remove(&table, &WindowReduceRemoveRequest { members: list(1024), ..request(None, None) }).is_ok());
    assert!(translate_remove(&table, &WindowReduceRemoveRequest { members: list(1025), ..request(None, None) }).is_err());
    assert!(translate_remove(&table, &WindowReduceRemoveRequest { excluded_members: list(1024), ..request(None, None) }).is_ok());
    let over = translate_remove(&table, &WindowReduceRemoveRequest { excluded_members: list(1025), ..request(None, None) });
    assert!(over.unwrap_err().message.contains("over the limit"));
}

#[test]
fn a_delete_is_a_removal_by_label_and_member_over_every_snapshot_and_never_of_the_key() {
    let table = profile(&["slot"]);
    let delete = |selector: WindowReduceSelector| WindowReduceDeleteRequest {
        entity: 7,
        selector: Some(selector),
        table: b"stats".to_vec(),
    };
    let selector = WindowReduceSelector { channel: 4, kind: 3, label: 5, members: vec![11, 12], excluded_members: vec![12], ..Default::default() };
    let Command::CellRemove { key: named, spec } = translate_delete(&table, &delete(selector)).unwrap() else { panic!() };
    assert_eq!(named, key(&table, 7, 4, 3));
    assert_eq!((spec.start_ms, spec.end_ms), (None, None));
    assert_eq!(spec.filters, vec![slot(5)]);
    assert_eq!(spec.members.include, Some(vec![CellMemberId::U64(11), CellMemberId::U64(12)]));
    assert_eq!(spec.members.exclude, vec![CellMemberId::U64(12)]);

    // The label alone restricts, at 0 too, as it does on a read.
    let label_only = WindowReduceSelector { channel: 4, kind: 3, ..Default::default() };
    assert_eq!(spec_of(translate_delete(&table, &delete(label_only.clone())).unwrap()).filters, vec![slot(0)]);
    // A table with no label and a selector with no member would empty the key.
    let unlabelled = profile(&[]);
    let err = translate_delete(&unlabelled, &delete(label_only.clone())).unwrap_err();
    assert!(err.message.contains("would empty the key"), "{err:?}");
    let named_members = WindowReduceSelector { members: vec![1], ..label_only.clone() };
    assert!(translate_delete(&unlabelled, &delete(named_members)).is_ok());
    // A label the table cannot carry, a missing selector and an oversize list are refused.
    assert!(translate_delete(&unlabelled, &delete(WindowReduceSelector { label: 2, members: vec![1], ..label_only.clone() })).is_err());
    assert!(translate_delete(&table, &WindowReduceDeleteRequest { selector: None, ..delete(label_only.clone()) }).is_err());
    let big = |n: i64| WindowReduceSelector { members: (1..=n).collect(), ..label_only.clone() };
    assert!(translate_delete(&table, &delete(big(1024))).is_ok());
    assert!(translate_delete(&table, &delete(big(1025))).is_err());
}

#[test]
fn the_responses_carry_the_receipt_on_the_additive_fields_and_the_established_ones_unchanged() {
    let receipt = Receipt { snapshots_before: 5, snapshots_after: 4, cells_before: 30, cells_removed: 7 };
    let asked = request(Some((1_000, 9_000)), filter(1));
    let ok = remove_response(&asked, receipt);
    assert_eq!(ok.code, WindowReduceStatusCode::Ok as i32);
    assert_eq!(ok.span, asked.span, "the span is echoed");
    assert!(!ok.has_message && ok.message.is_empty());
    assert_eq!((ok.samples_before, ok.samples_after, ok.cells_before, ok.cells_removed), (5, 4, 30, 7));
    // Field numbers 16 to 19, written as varints: tag 0x80 0x01 for 16 and 0x98 0x01 for 19.
    let bytes = ok.encode_to_vec();
    assert!(bytes.windows(3).any(|w| w == [0x80, 0x01, 5]), "{bytes:?}");
    assert!(bytes.windows(3).any(|w| w == [0x98, 0x01, 7]), "{bytes:?}");
    assert_eq!(wire::WindowReduceRemoveResponse::decode(bytes.as_slice()).unwrap(), ok);

    let refusal = invalid("no good");
    let refused = refused_remove_response(&asked, &refusal);
    assert_eq!((refused.code, refused.message.as_str(), refused.has_message), (WindowReduceStatusCode::InvalidArgument as i32, "no good", true));
    assert_eq!(refused.cells_removed, 0);

    let deleted = delete_response(receipt);
    assert_eq!((deleted.code, deleted.samples_after, deleted.cells_removed), (0, 4, 7));
    assert!(deleted.encode_to_vec().windows(3).any(|w| w == [0x98, 0x01, 7]));
    let refused = refused_delete_response(&refusal);
    assert_eq!((refused.code, refused.message.as_str()), (WindowReduceStatusCode::InvalidArgument as i32, "no good"));
}

#[test]
fn a_remove_request_round_trips_its_additive_fields_and_an_older_reader_skips_them() {
    let sent = WindowReduceRemoveRequest {
        members: vec![3, 1],
        has_members: true,
        excluded_members: vec![2],
        ..request(Some((10, 20)), filter(4))
    };
    assert_eq!(WindowReduceRemoveRequest::decode(sent.encode_to_vec().as_slice()).unwrap(), sent);
    // The established fields alone, as an older sender writes them, translate as they did.
    let older = request(Some((0, 5_000)), filter(4));
    assert!(older.members.is_empty() && !older.has_members && older.excluded_members.is_empty());
    assert_eq!(WindowReduceRemoveRequest::decode(older.encode_to_vec().as_slice()).unwrap(), older);
}
