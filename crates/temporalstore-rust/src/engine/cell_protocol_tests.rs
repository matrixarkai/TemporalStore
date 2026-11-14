// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::wire::*;
use super::*;
use crate::engine::cell_set::{decode_value, LaneReducer};
use prost::Message;

const NOW_US: i64 = 1_700_000_000_000_000;

fn pair_profile() -> TableProfile {
    TableProfile {
        name: "stats".to_string(),
        entity: EntityType::U64,
        dimensions: vec![
            DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true },
            DimSpec { name: "kind".to_string(), ty: DimType::U16, required: false },
        ],
        channel: Some(0),
        kind: Some(1),
        labels: vec!["slot".to_string()],
        label_rule: LabelRule::FromMember { shift: 54, bits: 10 },
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

fn list_profile(lanes: usize) -> TableProfile {
    let mut p = pair_profile();
    p.shape = Shape::List;
    p.lanes = (0..lanes)
        .map(|i| LaneSpec { name: format!("l{i}"), reducer: LaneReducer::Sum })
        .collect();
    p
}

fn pair_entry(member: i64, first: i64, second: i64) -> CellEntry {
    CellEntry {
        member,
        pair: Some(PairLanes { first, second, timestamp_us: 0 }),
        label: 5,
        has_label: true,
        ..Default::default()
    }
}

fn observation(entity: i64, ts_us: i64, entries: Vec<CellEntry>) -> Observation {
    Observation { entity, timestamp_us: ts_us, kind: 3, channel: 4, entries, ..Default::default() }
}

fn add(observations: Vec<Observation>) -> CellAddRequest {
    CellAddRequest { table: b"stats".to_vec(), observations, ..Default::default() }
}

fn query(window: Window, selector: CellSelector) -> CellQuery {
    CellQuery { entity: 7, window: Some(window), selector: Some(selector), table: b"stats".to_vec(), ..Default::default() }
}

fn selector(mode: RankMode) -> CellSelector {
    CellSelector { channel: 4, kind: 3, label: 5, mode: mode as i32, limit: 10, has_limit: true, ..Default::default() }
}

fn trailing() -> Window {
    Window { kind: WindowKind::TrailingFromNow as i32, span: 60_000_000, start_us: 0 }
}

// ---------------------------------------------------------------- wire bytes

#[test]
fn the_add_request_wire_bytes_follow_the_established_field_numbers() {
    let request = CellAddRequest {
        table: b"t".to_vec(),
        observations: vec![Observation {
            entity: 7,
            timestamp_us: 1,
            kind: 3,
            channel: 4,
            entries: vec![CellEntry {
                member: 9,
                pair: Some(PairLanes { first: 1, second: 2, timestamp_us: 0 }),
                label: 5,
                has_label: true,
                ..Default::default()
            }],
            ..Default::default()
        }],
        group_on_server: true,
        skip_existing: true,
        retry_token: 0,
    };
    let entry: Vec<u8> = [&[0x08, 0x09][..], &[0x2a, 0x04, 0x08, 0x01, 0x10, 0x02], &[0x38, 0x05, 0x40, 0x01]].concat();
    let mut obs = vec![0x08, 0x07, 0x10, 0x01, 0x18, 0x03, 0x20, 0x04, 0x2a, entry.len() as u8];
    obs.extend(&entry);
    let mut expected = vec![0x0a, 0x01, b't', 0x12, obs.len() as u8];
    expected.extend(&obs);
    expected.extend([0x18, 0x01, 0x20, 0x01]);
    assert_eq!(request.encode_to_vec(), expected);
}

#[test]
fn an_unknown_field_is_skipped_and_the_rest_reads_the_same() {
    let request = add(vec![observation(7, NOW_US, vec![pair_entry(9, 1, 2)])]);
    let mut bytes = request.encode_to_vec();
    bytes.extend([0xf8, 0x06, 0x2a]); // field 111, varint
    bytes.extend([0x92, 0x06, 0x02, 0xaa, 0xbb]); // field 98, length-delimited
    assert_eq!(CellAddRequest::decode(bytes.as_slice()).unwrap(), request);
}

#[test]
fn a_query_and_its_answer_round_trip_through_the_wire() {
    let q = query(trailing(), selector(RankMode::Ratio));
    assert_eq!(CellQuery::decode(q.encode_to_vec().as_slice()).unwrap(), q);
    let a = CellAnswer { entity: 7, first_us: 3, last_us: 4, code: 8, message: "m".into(), ..Default::default() };
    assert_eq!(CellAnswer::decode(a.encode_to_vec().as_slice()).unwrap(), a);
    // Field numbers 3 and 4 are the answer's time span, 16 and 17 its own status.
    assert_eq!(
        a.encode_to_vec(),
        vec![0x08, 0x07, 0x18, 0x03, 0x20, 0x04, 0x80, 0x01, 0x08, 0x8a, 0x01, 0x01, b'm']
    );
}

#[test]
fn a_wide_lane_value_keeps_its_low_32_bits_for_a_narrow_reader() {
    let wide = PairLanes { first: 5_000_000_000, second: -1, timestamp_us: 0 };
    let bytes = wide.encode_to_vec();
    let back = PairLanes::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.first, 5_000_000_000);
    assert_eq!(back.first as i32, 705_032_704);
    assert_eq!(back.second as i32, -1);
}

// ---------------------------------------------------------------- add

#[test]
fn an_add_becomes_one_merge_reduce_append_per_key_that_decodes_back_to_its_cells() {
    let profile = pair_profile();
    let member = (7i64 << 54) | 42;
    let mut e1 = pair_entry(member, 10, 1);
    e1.has_label = false;
    let request = add(vec![
        observation(7, NOW_US, vec![e1, pair_entry(9, 3, 2)]),
        observation(7, NOW_US + 500, vec![pair_entry(9, 4, 1)]),
        observation(8, NOW_US, vec![pair_entry(9, 1, 1)]),
    ]);
    let commands = translate_add(&profile, &request, NOW_US).unwrap();
    assert_eq!(commands.len(), 2, "two entities are two keys");
    let mut seen = 0;
    for command in &commands {
        let Command::FeatureAppendWithPolicy { key, points, policy, .. } = command else { panic!("{command:?}") };
        assert_eq!(*policy, FeatureWritePolicy::MergeReduce);
        assert_eq!(points.len(), 1, "500 microseconds share one millisecond");
        assert_eq!(points[0].timestamp_ms, (NOW_US / 1000) as u64);
        let (header, cells) = decode_value(&points[0].value).unwrap();
        assert_eq!(header, profile.header());
        let want = cell_set::encode_key(
            &profile.header().key,
            &CellKey {
                entity: EntityValue::U64(if cells.len() == 3 { 7 } else { 8 }),
                dims: vec![Some(DimValue::U16(4)), Some(DimValue::U16(3))],
            },
        )
        .unwrap();
        assert_eq!(*key, want);
        if cells.len() == 3 {
            seen += 1;
            // the label of the first cell comes from the member's top bits, the second was sent
            let derived = cells.iter().find(|c| c.member == Member::U64(member as u64)).unwrap();
            assert_eq!(derived.labels, vec![7]);
            assert_eq!(derived.lanes, vec![10, 1]);
            let sent: Vec<_> = cells.iter().filter(|c| c.member == Member::U64(9)).collect();
            assert_eq!(sent.len(), 2);
            assert!(sent.iter().all(|c| c.labels == vec![5]));
        }
    }
    assert_eq!(seen, 1);
}

#[test]
fn a_shorthand_pair_fills_the_pair_and_a_list_shorter_than_the_lanes_is_padded_with_zeros() {
    let profile = pair_profile();
    let entry = CellEntry { member: 1, shorthand_first: 6, shorthand_second: 7, ..Default::default() };
    let commands = translate_add(&profile, &add(vec![observation(7, NOW_US, vec![entry])]), NOW_US).unwrap();
    let Command::FeatureAppendWithPolicy { points, .. } = &commands[0] else { panic!() };
    assert_eq!(decode_value(&points[0].value).unwrap().1[0].lanes, vec![6, 7]);

    let lists = list_profile(4);
    let entry = CellEntry {
        member: 1,
        shape: CellShape::List as i32,
        list: Some(LaneList { timestamp_us: 0, values: vec![9, 8] }),
        ..Default::default()
    };
    let commands = translate_add(&lists, &add(vec![observation(7, NOW_US, vec![entry])]), NOW_US).unwrap();
    let Command::FeatureAppendWithPolicy { points, .. } = &commands[0] else { panic!() };
    assert_eq!(decode_value(&points[0].value).unwrap().1[0].lanes, vec![9, 8, 0, 0]);
}

#[test]
fn a_request_with_a_wrong_shape_an_unknown_table_or_a_duplicate_is_refused_with_its_own_code() {
    let profile = pair_profile();
    let mut strict = pair_profile();
    strict.refuse_duplicates = true;
    let twice = add(vec![observation(7, NOW_US, vec![pair_entry(9, 1, 1), pair_entry(9, 1, 1)])]);
    assert!(translate_add(&profile, &twice, NOW_US).is_ok());
    assert_eq!(translate_add(&strict, &twice, NOW_US).unwrap_err().code, CellStatusCode::DuplicateRefused);

    let list_entry = CellEntry { member: 1, shape: CellShape::List as i32, ..Default::default() };
    let err = translate_add(&profile, &add(vec![observation(7, NOW_US, vec![list_entry])]), NOW_US).unwrap_err();
    assert_eq!(err.code, CellStatusCode::InvalidArgument);

    let profiles = Profiles::new(vec![profile]).unwrap();
    assert_eq!(profiles.lookup(b"absent").unwrap_err().code, CellStatusCode::NoSuchTable);
    assert!(profiles.lookup(b"stats").is_ok());
}

#[test]
fn an_unreadable_channel_or_label_is_refused_not_wrapped() {
    let profile = pair_profile();
    for channel in [-1, 65_536] {
        let mut o = observation(7, NOW_US, vec![pair_entry(9, 1, 1)]);
        o.channel = channel;
        assert!(translate_add(&profile, &add(vec![o]), NOW_US).is_err(), "channel {channel}");
    }
    let mut e = pair_entry(9, 1, 1);
    e.label = -3;
    assert!(translate_add(&profile, &add(vec![observation(7, NOW_US, vec![e])]), NOW_US).is_err());
    let mut o = observation(7, NOW_US, vec![pair_entry(9, 1, 1)]);
    o.channel = 65_535;
    assert!(translate_add(&profile, &add(vec![o]), NOW_US).is_ok());
}

// ---------------------------------------------------------------- bounds: refuse past, accept at

fn many(cells: usize) -> CellAddRequest {
    add(vec![observation(7, NOW_US, (0..cells as i64).map(|m| pair_entry(m + 1, 1, 1)).collect())])
}

#[test]
fn cells_per_request_are_accepted_at_the_limit_and_refused_one_past_it() {
    let profile = pair_profile();
    assert!(translate_add(&profile, &many(MAX_CELLS_PER_REQUEST), NOW_US).is_ok());
    let err = translate_add(&profile, &many(MAX_CELLS_PER_REQUEST + 1), NOW_US).unwrap_err();
    assert!(err.message.contains("over the limit"), "{err:?}");
}

#[test]
fn queries_per_batch_are_accepted_at_the_limit_and_refused_one_past_it() {
    let profiles = Profiles::new(vec![pair_profile()]).unwrap();
    let q = query(trailing(), selector(RankMode::FirstLane));
    let batch = |n: usize| CellBatchQuery { queries: vec![q.clone(); n] };
    assert_eq!(translate_batch(&profiles, &batch(MAX_BATCH_QUERIES)).unwrap().len(), MAX_BATCH_QUERIES);
    assert!(translate_batch(&profiles, &batch(MAX_BATCH_QUERIES + 1)).unwrap_err().message.contains("over the limit"));
    assert!(translate_batch(&profiles, &batch(0)).is_err());
}

#[test]
fn a_batch_names_the_position_of_the_query_that_refused_it() {
    let profiles = Profiles::new(vec![pair_profile()]).unwrap();
    let good = query(trailing(), selector(RankMode::FirstLane));
    let mut bad = good.clone();
    bad.table = b"absent".to_vec();
    let err = translate_batch(&profiles, &CellBatchQuery { queries: vec![good.clone(), bad] }).unwrap_err();
    assert_eq!(err.code, CellStatusCode::NoSuchTable);
    let mut worse = good.clone();
    worse.selector.as_mut().unwrap().limit = 0;
    let err = translate_batch(&profiles, &CellBatchQuery { queries: vec![good.clone(), good, worse] }).unwrap_err();
    assert!(err.message.starts_with("query 2:"), "{err:?}");
}

#[test]
fn the_table_name_is_accepted_at_64_bytes_and_refused_at_65() {
    let mut p = pair_profile();
    p.name = "n".repeat(64);
    let profiles = Profiles::new(vec![p]).unwrap();
    assert!(profiles.lookup("n".repeat(64).as_bytes()).is_ok());
    assert_eq!(profiles.lookup("n".repeat(65).as_bytes()).unwrap_err().code, CellStatusCode::InvalidArgument);
}

#[test]
fn the_limit_of_a_read_is_accepted_at_1000_and_refused_at_1001() {
    let profile = pair_profile();
    let mut s = selector(RankMode::FirstLane);
    s.limit = MAX_TOP_K as i32;
    assert_eq!(translate_query(&profile, &query(trailing(), s.clone())).unwrap().spec.top_k, MAX_TOP_K);
    s.limit = MAX_TOP_K as i32 + 1;
    assert!(translate_query(&profile, &query(trailing(), s)).unwrap_err().message.contains("over the limit"));
}

#[test]
fn weighted_lanes_are_accepted_at_8_and_refused_at_9() {
    let profile = list_profile(16);
    let weighted = |n: usize| CellSelector {
        has_weighted_lanes: true,
        has_weights: true,
        weighted_lanes: (0..n as i32).collect(),
        weights: vec![1; n],
        ..selector(RankMode::Weighted)
    };
    let plan = translate_query(&profile, &query(trailing(), weighted(MAX_WEIGHTED))).unwrap();
    assert!(matches!(plan.spec.score, CellScore::Weighted { ref lanes, .. } if lanes.len() == 8));
    assert!(translate_query(&profile, &query(trailing(), weighted(MAX_WEIGHTED + 1))).unwrap_err().message.contains("over the limit"));
}

#[test]
fn member_lists_and_extra_entities_are_bounded_before_they_are_refused_as_untranslated() {
    let profile = pair_profile();
    let with_members = |n: usize| CellSelector { members: vec![1; n], has_members: true, ..selector(RankMode::MembersOnly) };
    let at = translate_query(&profile, &query(trailing(), with_members(MAX_MEMBER_LIST))).unwrap_err();
    assert!(at.message.contains("not translated yet"), "{at:?}");
    let over = translate_query(&profile, &query(trailing(), with_members(MAX_MEMBER_LIST + 1))).unwrap_err();
    assert!(over.message.contains("over the limit"), "{over:?}");
    let mut q = query(trailing(), selector(RankMode::FirstLane));
    q.more_entities = vec![1; MAX_MORE_ENTITIES];
    assert!(translate_query(&profile, &q).unwrap_err().message.contains("not translated yet"));
    q.more_entities = vec![1; MAX_MORE_ENTITIES + 1];
    assert!(translate_query(&profile, &q).unwrap_err().message.contains("over the limit"));
}

#[test]
fn the_accepted_timestamp_range_is_open_at_both_ends() {
    let p = pair_profile();
    let at = |ts: i64| translate_add(&p, &add(vec![observation(7, ts, vec![pair_entry(9, 1, 1)])]), NOW_US);
    assert!(at(NOW_US - p.max_past_us + 1).is_ok());
    assert!(at(NOW_US - p.max_past_us).is_err());
    assert!(at(NOW_US + p.max_future_us - 1).is_ok());
    assert!(at(NOW_US + p.max_future_us).is_err());
    // a millisecond stamp sent where microseconds belong is far in the past
    assert!(at(NOW_US / 1000).is_err());
}

// ---------------------------------------------------------------- read

#[test]
fn each_translated_rank_mode_becomes_the_score_it_names() {
    let profile = pair_profile();
    let score = |s: CellSelector| translate_query(&profile, &query(trailing(), s)).unwrap().spec.score;
    assert_eq!(score(selector(RankMode::FirstLane)), CellScore::Lane { lane: "impressions".into() });
    assert_eq!(score(selector(RankMode::SecondLane)), CellScore::Lane { lane: "clicks".into() });
    assert_eq!(score(selector(RankMode::Member)), CellScore::Member);
    assert_eq!(
        score(selector(RankMode::Ratio)),
        CellScore::Ratio { numerator: "impressions".into(), denominator: "clicks".into() }
    );
    assert_eq!(
        score(selector(RankMode::Wilson)),
        CellScore::Wilson { successes: "impressions".into(), trials: "clicks".into(), z: DEFAULT_WILSON_Z }
    );
    let z = CellSelector { has_wilson_z: true, wilson_z: 2.5, ..selector(RankMode::Wilson) };
    assert!(matches!(score(z), CellScore::Wilson { z, .. } if z == 2.5));
    let named = CellSelector { has_sort_lane: true, sort_lane: 1, ..selector(RankMode::NamedLane) };
    assert_eq!(score(named), CellScore::Lane { lane: "clicks".into() });
    let off = CellSelector { has_sort_lane: true, sort_lane: 2, ..selector(RankMode::NamedLane) };
    assert!(translate_query(&profile, &query(trailing(), off)).is_err());
}

#[test]
fn the_selector_label_becomes_an_equality_predicate_and_the_key_names_channel_and_kind() {
    let profile = pair_profile();
    let plan = translate_query(&profile, &query(trailing(), selector(RankMode::FirstLane))).unwrap();
    assert_eq!(plan.spec.filters, vec![FeatureFilter { field: "slot".into(), op: FeatureFilterOp::Equal, value: 5 }]);
    assert_eq!(plan.spec.top_k, 10);
    let want = cell_set::encode_key(
        &profile.header().key,
        &CellKey { entity: EntityValue::U64(7), dims: vec![Some(DimValue::U16(4)), Some(DimValue::U16(3))] },
    )
    .unwrap();
    assert_eq!(plan.key, want);
}

#[test]
fn a_mode_or_window_that_is_not_built_yet_is_refused_and_says_so() {
    let profile = pair_profile();
    for mode in [RankMode::Recent, RankMode::MatchLane, RankMode::MembersOnly] {
        let err = translate_query(&profile, &query(trailing(), selector(mode))).unwrap_err();
        assert!(err.message.contains("not translated yet"), "{mode:?}: {err:?}");
    }
    for kind in [WindowKind::TrailingFromNewest, WindowKind::NewestRows, WindowKind::TrailingFromNewestOfKind] {
        let w = Window { kind: kind as i32, span: 1, start_us: 0 };
        let err = translate_query(&profile, &query(w, selector(RankMode::FirstLane))).unwrap_err();
        assert!(err.message.contains("not translated yet"), "{kind:?}: {err:?}");
    }
}

#[test]
fn a_window_from_a_start_covers_whole_milliseconds_and_excludes_its_end() {
    let w = |start_us, span| Window { kind: WindowKind::FromStart as i32, span, start_us };
    assert_eq!(window_of(&w(1_000, 3_000)).unwrap(), CellWindow::Between { start_ms: 1, end_ms: 3 });
    assert_eq!(window_of(&w(1_001, 3_000)).unwrap(), CellWindow::Between { start_ms: 2, end_ms: 4 });
    assert_eq!(window_of(&w(0, 1_000)).unwrap(), CellWindow::Between { start_ms: 0, end_ms: 0 });
    assert!(window_of(&w(5, 0)).is_err());
    assert!(window_of(&w(5, -1)).is_err());
    assert_eq!(
        window_of(&Window { kind: WindowKind::TrailingFromNow as i32, span: 61_999, start_us: 0 }).unwrap(),
        CellWindow::Last { duration_ms: 61 }
    );
}

#[test]
fn an_answer_carries_wide_lanes_the_member_the_label_and_the_newest_time() {
    let profile = pair_profile();
    let ranked = RankedCells {
        label_columns: vec!["slot".into()],
        lane_columns: vec!["impressions".into(), "clicks".into()],
        snapshots: 2,
        keys: 1,
        unscored: 1,
        rows: vec![crate::types::RankedCellRow {
            member: CellMemberId::U64(9),
            score: 1.0,
            last_ms: 1_700_000_000_123,
            labels: vec![5],
            lanes: vec![5_000_000_000, 2],
        }],
        refused: String::new(),
    };
    let q = CellQuery { echo: true, has_echo: true, ..query(trailing(), selector(RankMode::Ratio)) };
    let a = answer_of(&profile, &q, &ranked);
    assert_eq!(a.code, CellStatusCode::Ok as i32);
    assert_eq!((a.snapshots, a.unscored), (2, 1));
    assert_eq!(a.entries[0].member, 9);
    assert!(a.entries[0].has_label && a.entries[0].label == 5);
    let pair = a.entries[0].pair.as_ref().unwrap();
    assert_eq!((pair.first, pair.second, pair.timestamp_us), (5_000_000_000, 2, 1_700_000_000_123_000));
    assert_eq!(a.first_us, 1_700_000_000_123_000);
    assert!(a.echoed_selector.is_some() && a.echoed_window.is_some());
    let refused = answer_of(&profile, &q, &RankedCells { refused: "why".into(), ..Default::default() });
    assert_eq!((refused.code, refused.message.as_str()), (CellStatusCode::InvalidArgument as i32, "why"));
}

#[test]
fn the_skip_existing_flag_is_carried_and_refused_with_a_message_that_says_what_to_use() {
    let mut request = add(vec![observation(7, NOW_US, vec![pair_entry(9, 1, 1)])]);
    assert!(translate_add(&pair_profile(), &request, NOW_US).is_ok());
    request.skip_existing = true;
    let err = translate_add(&pair_profile(), &request, NOW_US).unwrap_err();
    assert_eq!(err.code, CellStatusCode::InvalidArgument);
    assert!(err.message.contains("retry token"), "{err:?}");
    let restore = CellRestoreRequest {
        table: b"stats".to_vec(),
        skip_existing: true,
        observations: vec![RestoredObservation { entity: 7, latest_us: NOW_US, earliest_us: NOW_US, entries: vec![pair_entry(9, 1, 1)], ..Default::default() }],
        ..Default::default()
    };
    assert!(translate_restore(&pair_profile(), &restore, NOW_US).is_err());
}

fn tokens_of(commands: &[Command]) -> Vec<Option<u64>> {
    commands
        .iter()
        .map(|c| match c {
            Command::FeatureAppendWithPolicy { retry_token, .. } => *retry_token,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn a_retry_token_on_the_wire_rides_on_every_key_of_the_append_and_a_zero_token_is_none() {
    let profile = pair_profile();
    let mut request = add(vec![
        observation(7, NOW_US, vec![pair_entry(9, 1, 1)]),
        observation(8, NOW_US, vec![pair_entry(9, 1, 1)]),
    ]);
    assert_eq!(tokens_of(&translate_add(&profile, &request, NOW_US).unwrap()), vec![None, None]);
    let wide: u64 = (3u64 << 40) + 77;
    request.retry_token = wide;
    let sent = CellAddRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        tokens_of(&translate_add(&profile, &sent, NOW_US).unwrap()),
        vec![Some(wide), Some(wide)]
    );
    request.retry_token = u64::MAX;
    assert_eq!(tokens_of(&translate_add(&profile, &request, NOW_US).unwrap())[0], Some(u64::MAX));
}

#[test]
fn the_token_and_the_replayed_flag_have_the_established_field_numbers() {
    let request = CellAddRequest { table: b"t".to_vec(), retry_token: 300, ..Default::default() };
    assert_eq!(request.encode_to_vec(), vec![0x0a, 0x01, b't', 0x80, 0x01, 0xac, 0x02]);
    let response = add_response(0, 0, 0, true);
    assert_eq!(response.encode_to_vec(), vec![0x98, 0x01, 0x01]);
}

#[test]
fn a_replayed_receipt_is_reported_with_the_counts_of_the_first_application() {
    let first = add_response(2, 5, 1, false);
    let replay = add_response(2, 5, 1, true);
    assert!(!first.replayed && replay.replayed);
    assert_eq!((replay.snapshots_written, replay.cells_created, replay.cells_merged), (2, 5, 1));
    assert_eq!(replay.code, CellStatusCode::Ok as i32);
    let encoded = replay.encode_to_vec();
    assert_eq!(CellAddResponse::decode(encoded.as_slice()).unwrap(), replay);
}

#[test]
fn a_restore_never_carries_a_token() {
    let restore = CellRestoreRequest {
        table: b"stats".to_vec(),
        observations: vec![RestoredObservation { entity: 7, latest_us: NOW_US, earliest_us: NOW_US, entries: vec![pair_entry(9, 1, 1)], ..Default::default() }],
        ..Default::default()
    };
    assert_eq!(tokens_of(&translate_restore(&pair_profile(), &restore, NOW_US).unwrap()), vec![None]);
}
