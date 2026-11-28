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

fn add(observations: Vec<Observation>) -> WindowReduceAddRequest {
    WindowReduceAddRequest { table: b"stats".to_vec(), observations, ..Default::default() }
}

fn query(window: Window, selector: WindowReduceSelector) -> WindowReduceQuery {
    WindowReduceQuery { entity: 7, window: Some(window), selector: Some(selector), table: b"stats".to_vec(), ..Default::default() }
}

fn selector(mode: RankMode) -> WindowReduceSelector {
    WindowReduceSelector { channel: 4, kind: 3, label: 5, mode: mode as i32, limit: 10, has_limit: true, ..Default::default() }
}

fn trailing() -> Window {
    Window { kind: WindowKind::TrailingFromNow as i32, span: 60_000_000, start_us: 0 }
}

// ---------------------------------------------------------------- wire bytes

#[test]
fn the_add_request_wire_bytes_follow_the_established_field_numbers() {
    let request = WindowReduceAddRequest {
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
    assert_eq!(WindowReduceAddRequest::decode(bytes.as_slice()).unwrap(), request);
}

#[test]
fn a_query_and_its_answer_round_trip_through_the_wire() {
    let q = query(trailing(), selector(RankMode::Ratio));
    assert_eq!(WindowReduceQuery::decode(q.encode_to_vec().as_slice()).unwrap(), q);
    let a = WindowReduceAnswer { entity: 7, first_us: 3, last_us: 4, code: 8, message: "m".into(), ..Default::default() };
    assert_eq!(WindowReduceAnswer::decode(a.encode_to_vec().as_slice()).unwrap(), a);
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
    assert_eq!(translate_add(&strict, &twice, NOW_US).unwrap_err().code, WindowReduceStatusCode::DuplicateRefused);

    let list_entry = CellEntry { member: 1, shape: CellShape::List as i32, ..Default::default() };
    let err = translate_add(&profile, &add(vec![observation(7, NOW_US, vec![list_entry])]), NOW_US).unwrap_err();
    assert_eq!(err.code, WindowReduceStatusCode::InvalidArgument);

    let profiles = Profiles::new(vec![profile]).unwrap();
    assert_eq!(profiles.lookup(b"absent").unwrap_err().code, WindowReduceStatusCode::NoData);
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

fn many(cells: usize) -> WindowReduceAddRequest {
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
    let batch = |n: usize| WindowReduceBatchQuery { queries: vec![q.clone(); n] };
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
    let err = translate_batch(&profiles, &WindowReduceBatchQuery { queries: vec![good.clone(), bad] }).unwrap_err();
    assert_eq!(err.code, WindowReduceStatusCode::NoData);
    let mut worse = good.clone();
    worse.selector.as_mut().unwrap().limit = 0;
    let err = translate_batch(&profiles, &WindowReduceBatchQuery { queries: vec![good.clone(), good, worse] }).unwrap_err();
    assert!(err.message.starts_with("query 2:"), "{err:?}");
}

#[test]
fn the_table_name_is_accepted_at_64_bytes_and_refused_at_65() {
    let mut p = pair_profile();
    p.name = "n".repeat(64);
    let profiles = Profiles::new(vec![p]).unwrap();
    assert!(profiles.lookup("n".repeat(64).as_bytes()).is_ok());
    assert_eq!(profiles.lookup("n".repeat(65).as_bytes()).unwrap_err().code, WindowReduceStatusCode::InvalidArgument);
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
    let weighted = |n: usize| WindowReduceSelector {
        has_weighted_lanes: true,
        has_weights: true,
        weighted_lanes: (0..n as i32).collect(),
        weights: vec![1; n],
        ..selector(RankMode::Weighted)
    };
    let plan = translate_query(&profile, &query(trailing(), weighted(MAX_WEIGHTED))).unwrap();
    assert!(matches!(plan.spec.score, WindowReduceScore::Weighted { ref lanes, .. } if lanes.len() == 8));
    assert!(translate_query(&profile, &query(trailing(), weighted(MAX_WEIGHTED + 1))).unwrap_err().message.contains("over the limit"));
}

#[test]
fn member_lists_and_extra_entities_are_accepted_at_the_limit_and_refused_one_past_it() {
    let profile = pair_profile();
    let with_members = |n: usize, excluded: usize| WindowReduceSelector {
        members: vec![1; n],
        has_members: true,
        excluded_members: vec![2; excluded],
        ..selector(RankMode::FirstLane)
    };
    let at = translate_query(&profile, &query(trailing(), with_members(MAX_MEMBER_LIST, MAX_MEMBER_LIST))).unwrap();
    // As sent: duplicates stay, the engine compiles the set.
    assert_eq!(at.spec.members.include.as_ref().map(Vec::len), Some(MAX_MEMBER_LIST));
    assert_eq!(at.spec.members.exclude.len(), MAX_MEMBER_LIST);
    let over = translate_query(&profile, &query(trailing(), with_members(MAX_MEMBER_LIST + 1, 0))).unwrap_err();
    assert!(over.message.contains("members 1025 is over the limit of 1024"), "{over:?}");
    let over = translate_query(&profile, &query(trailing(), with_members(0, MAX_MEMBER_LIST + 1))).unwrap_err();
    assert!(over.message.contains("excluded members 1025 is over the limit of 1024"), "{over:?}");
    // 63 more entities with the first make 64 keys, the merged read's limit.
    let mut q = query(trailing(), selector(RankMode::FirstLane));
    q.more_entities = (100..100 + MAX_MORE_ENTITIES as i64).collect();
    let at = translate_query(&profile, &q).unwrap();
    assert_eq!(at.more_keys.len(), MAX_MORE_ENTITIES);
    q.more_entities = (100..101 + MAX_MORE_ENTITIES as i64).collect();
    assert!(translate_query(&profile, &q).unwrap_err().message.contains("more entities 64 is over the limit of 63"));
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
    let score = |s: WindowReduceSelector| translate_query(&profile, &query(trailing(), s)).unwrap().spec.score;
    assert_eq!(score(selector(RankMode::FirstLane)), WindowReduceScore::Lane { lane: "impressions".into() });
    assert_eq!(score(selector(RankMode::SecondLane)), WindowReduceScore::Lane { lane: "clicks".into() });
    assert_eq!(score(selector(RankMode::Member)), WindowReduceScore::Member);
    assert_eq!(
        score(selector(RankMode::Ratio)),
        WindowReduceScore::Ratio { numerator: "impressions".into(), denominator: "clicks".into() }
    );
    assert_eq!(
        score(selector(RankMode::Wilson)),
        WindowReduceScore::Wilson { successes: "impressions".into(), trials: "clicks".into(), z: DEFAULT_WILSON_Z }
    );
    let z = WindowReduceSelector { has_wilson_z: true, wilson_z: 2.5, ..selector(RankMode::Wilson) };
    assert!(matches!(score(z), WindowReduceScore::Wilson { z, .. } if z == 2.5));
    let named = WindowReduceSelector { has_sort_lane: true, sort_lane: 1, ..selector(RankMode::NamedLane) };
    assert_eq!(score(named), WindowReduceScore::Lane { lane: "clicks".into() });
    let off = WindowReduceSelector { has_sort_lane: true, sort_lane: 2, ..selector(RankMode::NamedLane) };
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
fn a_newest_rows_window_counts_rows_and_its_bounds_are_exact() {
    let max = crate::engine::window_reduce_rank::MAX_WINDOW_ROWS as i64;
    let profile = pair_profile();
    let rows = |span: i64| Window { kind: WindowKind::NewestRows as i32, span, start_us: 0 };
    let plan = |span: i64| translate_query(&profile, &query(rows(span), selector(RankMode::FirstLane)));

    // `span` is a count of rows, whatever its size, and the read is anchored at the request.
    let planned = plan(250).unwrap();
    assert_eq!(WindowReduceWindow::NewestRows { n: 250 }, planned.spec.window);
    assert_eq!(WindowReduceAnchor::Request, planned.spec.anchor);
    assert_eq!(WindowReduceWindow::NewestRows { n: 1 }, plan(1).unwrap().spec.window);
    // The recent read takes the same window.
    let recent = translate_query(&profile, &query(rows(5), selector(RankMode::Recent))).unwrap();
    assert_eq!(WindowReduceWindow::NewestRows { n: 5 }, recent.spec.window);
    assert_eq!(WindowReduceScore::Recent, recent.spec.score);

    // No rows and a negative count are refused, and the bound is accepted at the limit and refused
    // one past it, in the words every other bound uses.
    assert!(plan(0).unwrap_err().message.contains("0 rows"));
    assert!(plan(-1).unwrap_err().message.contains("negative"));
    assert_eq!(WindowReduceWindow::NewestRows { n: max as usize }, plan(max).unwrap().spec.window);
    let over = plan(max + 1).unwrap_err();
    assert!(over.message.contains("rows in the window"), "{over:?}");
}

#[test]
fn a_members_only_read_asks_for_exactly_the_members_it_names_ordered_by_time() {
    let profile = pair_profile();
    let plan = |s: WindowReduceSelector| translate_query(&profile, &query(trailing(), s));
    // The limit of the request is ignored, even a limit of zero: the read asks for as many rows as
    // the set holds, and orders them by the stamp of each member's newest sample.
    let only = |members: Vec<i64>| WindowReduceSelector {
        has_members: true,
        members,
        limit: 0,
        has_limit: false,
        ..selector(RankMode::MembersOnly)
    };
    let planned = plan(only(vec![3, 4, 5])).unwrap();
    assert_eq!(3, planned.spec.top_k, "the limit of the request was used");
    assert_eq!(WindowReduceScore::Time, planned.spec.score);
    assert_eq!(
        Some(vec![CellMemberId::U64(3), CellMemberId::U64(4), CellMemberId::U64(5)]),
        planned.spec.members.include
    );
    // The label predicate every read carries is still there.
    assert_eq!(
        vec![FeatureFilter { field: "slot".into(), op: FeatureFilterOp::Equal, value: 5 }],
        planned.spec.filters
    );
    // No members named is refused, and the set is bounded by what a read can return: accepted at
    // exactly 1000, refused at 1001.
    assert!(plan(only(Vec::new())).is_err());
    assert!(plan(selector(RankMode::MembersOnly)).is_err());
    assert_eq!(MAX_TOP_K, plan(only((0..MAX_TOP_K as i64).collect())).unwrap().spec.top_k);
    let over = plan(only((0..=MAX_TOP_K as i64).collect())).unwrap_err();
    assert!(over.message.contains("over the limit"), "{over:?}");
    // CONTROL: the same selector in another mode keeps its limit.
    let ranked = WindowReduceSelector { has_members: true, members: vec![3, 4, 5], ..selector(RankMode::FirstLane) };
    assert_eq!(10, plan(ranked).unwrap().spec.top_k);
}

#[test]
fn a_window_from_a_start_covers_whole_milliseconds_and_excludes_its_end() {
    let w = |start_us, span| Window { kind: WindowKind::FromStart as i32, span, start_us };
    assert_eq!(window_of(&w(1_000, 3_000), false).unwrap(), WindowReduceWindow::Between { start_ms: 1, end_ms: 3 });
    assert_eq!(window_of(&w(1_001, 3_000), false).unwrap(), WindowReduceWindow::Between { start_ms: 2, end_ms: 4 });
    assert_eq!(window_of(&w(0, 1_000), false).unwrap(), WindowReduceWindow::Between { start_ms: 0, end_ms: 0 });
    assert!(window_of(&w(5, 0), false).is_err());
    assert!(window_of(&w(5, -1), false).is_err());
    assert_eq!(
        window_of(&Window { kind: WindowKind::TrailingFromNow as i32, span: 61_999, start_us: 0 }, false).unwrap(),
        WindowReduceWindow::Last { duration_ms: 61 }
    );
}

#[test]
fn a_window_from_a_start_includes_its_end_for_the_reads_that_select_by_member_or_lane_value() {
    let w = |start_us, span| Window { kind: WindowKind::FromStart as i32, span, start_us };
    // [1,000, 4,000] microseconds: the stamp at 4 ms is inside.
    assert_eq!(window_of(&w(1_000, 3_000), true).unwrap(), WindowReduceWindow::Between { start_ms: 1, end_ms: 4 });
    assert_eq!(window_of(&w(1_001, 3_000), true).unwrap(), WindowReduceWindow::Between { start_ms: 2, end_ms: 4 });
    // A span of 0 is the one instant at the start.
    assert_eq!(window_of(&w(5_000, 0), true).unwrap(), WindowReduceWindow::Between { start_ms: 5, end_ms: 5 });
    assert!(window_of(&w(5, 0), true).is_err(), "no whole millisecond lies in [5, 5] microseconds");
    // Through a query, by mode: the same window ends one ms later for MEMBERS_ONLY and MATCH_LANE.
    let from = Window { kind: WindowKind::FromStart as i32, span: 3_000, start_us: 1_000 };
    let end_of = |mode: RankMode| match translate_query(
        &pair_profile(),
        &query(from.clone(), WindowReduceSelector { has_members: true, members: vec![1], ..selector(mode) }),
    )
    .unwrap()
    .spec
    .window
    {
        WindowReduceWindow::Between { end_ms, .. } => end_ms,
        other => panic!("{other:?}"),
    };
    assert_eq!(end_of(RankMode::FirstLane), 3);
    assert_eq!(end_of(RankMode::Recent), 3);
    assert_eq!(end_of(RankMode::MembersOnly), 4);
    assert_eq!(end_of(RankMode::MatchLane), 4);
}

#[test]
fn an_answer_carries_wide_lanes_the_member_the_label_and_the_newest_time() {
    let profile = pair_profile();
    let ranked = RankedCells {
        label_columns: vec!["slot".into()],
        lane_columns: vec!["impressions".into(), "clicks".into()],
        samples: 2,
        window_first_ms: 1_700_000_000_100,
        window_last_ms: 1_700_000_000_150,
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
        sample_info: Vec::new(),
        matched_first_ms: 0,
        matched_last_ms: 0,
    };
    let q = WindowReduceQuery { echo: true, has_echo: true, ..query(trailing(), selector(RankMode::Ratio)) };
    let a = answer_of(&profile, &q, &ranked);
    assert_eq!(a.code, WindowReduceStatusCode::Ok as i32);
    assert_eq!((a.samples, a.unscored), (2, 1));
    assert_eq!(a.entries[0].member, 9);
    assert!(a.entries[0].has_label && a.entries[0].label == 5);
    let pair = a.entries[0].pair.as_ref().unwrap();
    assert_eq!((pair.first, pair.second, pair.timestamp_us), (5_000_000_000, 2, 1_700_000_000_123_000));
    // The span of what the read scanned, not of the one row it kept.
    assert_eq!((a.first_us, a.last_us), (1_700_000_000_100_000, 1_700_000_000_150_000));
    assert!(a.echoed_selector.is_some() && a.echoed_window.is_some());
    let refused = answer_of(&profile, &q, &RankedCells { refused: "why".into(), ..Default::default() });
    assert_eq!((refused.code, refused.message.as_str()), (WindowReduceStatusCode::InvalidArgument as i32, "why"));
}

#[test]
fn the_skip_existing_flag_is_ignored_by_a_write_so_the_request_translates_to_the_same_commands() {
    let mut request = add(vec![observation(7, NOW_US, vec![pair_entry(9, 1, 1)])]);
    let plain = translate_add(&pair_profile(), &request, NOW_US).unwrap();
    request.skip_existing = true;
    assert_eq!(format!("{:?}", translate_add(&pair_profile(), &request, NOW_US).unwrap()), format!("{plain:?}"));
    let restore = |skip_existing: bool| WindowReduceRestoreRequest {
        table: b"stats".to_vec(),
        skip_existing,
        observations: vec![RestoredObservation { entity: 7, latest_us: NOW_US, earliest_us: NOW_US, entries: vec![pair_entry(9, 1, 1)], ..Default::default() }],
        ..Default::default()
    };
    assert_eq!(
        format!("{:?}", translate_restore(&pair_profile(), &restore(true), NOW_US).unwrap()),
        format!("{:?}", translate_restore(&pair_profile(), &restore(false), NOW_US).unwrap())
    );
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
    let sent = WindowReduceAddRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        tokens_of(&translate_add(&profile, &sent, NOW_US).unwrap()),
        vec![Some(wide), Some(wide)]
    );
    request.retry_token = u64::MAX;
    assert_eq!(tokens_of(&translate_add(&profile, &request, NOW_US).unwrap())[0], Some(u64::MAX));
}

#[test]
fn the_token_and_the_replayed_flag_have_the_established_field_numbers() {
    let request = WindowReduceAddRequest { table: b"t".to_vec(), retry_token: 300, ..Default::default() };
    assert_eq!(request.encode_to_vec(), vec![0x0a, 0x01, b't', 0x80, 0x01, 0xac, 0x02]);
    let response = add_response(0, 0, 0, true);
    assert_eq!(response.encode_to_vec(), vec![0x98, 0x01, 0x01]);
}

#[test]
fn a_replayed_receipt_is_reported_with_the_counts_of_the_first_application() {
    let first = add_response(2, 5, 1, false);
    let replay = add_response(2, 5, 1, true);
    assert!(!first.replayed && replay.replayed);
    assert_eq!((replay.samples_written, replay.cells_created, replay.cells_merged), (2, 5, 1));
    assert_eq!(replay.code, WindowReduceStatusCode::Ok as i32);
    let encoded = replay.encode_to_vec();
    assert_eq!(WindowReduceAddResponse::decode(encoded.as_slice()).unwrap(), replay);
}

#[test]
fn a_restore_never_carries_a_token() {
    let restore = WindowReduceRestoreRequest {
        table: b"stats".to_vec(),
        observations: vec![RestoredObservation { entity: 7, latest_us: NOW_US, earliest_us: NOW_US, entries: vec![pair_entry(9, 1, 1)], ..Default::default() }],
        ..Default::default()
    };
    assert_eq!(tokens_of(&translate_restore(&pair_profile(), &restore, NOW_US).unwrap()), vec![None]);
}

// ---------------------------------------------------------------- read options, fan-in, batch

fn plan(selector: WindowReduceSelector) -> ReadPlan {
    translate_query(&pair_profile(), &query(trailing(), selector)).unwrap()
}

fn refusal(selector: WindowReduceSelector) -> Refusal {
    translate_query(&pair_profile(), &query(trailing(), selector)).unwrap_err()
}

#[test]
fn a_member_set_round_trips_the_wire_and_becomes_the_include_and_exclude_filter() {
    let sent = WindowReduceSelector {
        members: vec![30, 10, 30],
        has_members: true,
        excluded_members: vec![9, -1],
        ..selector(RankMode::FirstLane)
    };
    // The exclude list is field 40 (tag 0xc2 0x02), packed.
    assert_eq!(
        WindowReduceSelector { excluded_members: vec![9], ..Default::default() }.encode_to_vec(),
        vec![0xc2, 0x02, 0x01, 0x09]
    );
    let back = WindowReduceSelector::decode(sent.encode_to_vec().as_slice()).unwrap();
    assert_eq!(back, sent);
    let planned = plan(back);
    assert_eq!(
        planned.spec.members,
        CellMemberFilter {
            include: Some(vec![CellMemberId::U64(30), CellMemberId::U64(10), CellMemberId::U64(30)]),
            exclude: vec![CellMemberId::U64(9), CellMemberId::U64(u64::MAX)],
        }
    );
    assert_eq!(planned.clone().command(), Command::WindowReduceRank { key: planned.key, spec: planned.spec });
    // An exclusion alone leaves the include set absent: nothing is restricted but those members.
    let only_exclude = plan(WindowReduceSelector { excluded_members: vec![4], ..selector(RankMode::FirstLane) });
    assert_eq!(only_exclude.spec.members.include, None);
    assert_eq!(only_exclude.spec.members.exclude, vec![CellMemberId::U64(4)]);
    // No member list at all is the read as it was before the set existed.
    assert!(plan(selector(RankMode::FirstLane)).spec.members.is_open());
}

#[test]
fn a_flagged_empty_include_set_is_refused_and_an_unflagged_one_restricts_nothing() {
    let flagged = WindowReduceSelector { has_members: true, ..selector(RankMode::FirstLane) };
    let err = refusal(flagged);
    assert_eq!(err.code, WindowReduceStatusCode::InvalidArgument);
    assert!(err.message.contains("include set names no member"), "{err:?}");
    assert!(plan(selector(RankMode::FirstLane)).spec.members.include.is_none());
    // A list that holds members is the include set even when the flag is left off.
    let unflagged = WindowReduceSelector { members: vec![5], ..selector(RankMode::FirstLane) };
    assert_eq!(plan(unflagged).spec.members.include, Some(vec![CellMemberId::U64(5)]));
}

#[test]
fn a_window_anchored_on_the_newest_sample_becomes_the_newest_anchor_and_the_clock_one_does_not() {
    let profile = pair_profile();
    let read = |kind: WindowKind| {
        let w = Window { kind: kind as i32, span: 5_000_000, start_us: 0 };
        translate_query(&profile, &query(w, selector(RankMode::FirstLane)))
    };
    for kind in [WindowKind::TrailingFromNewest, WindowKind::TrailingFromNewestOfKind] {
        let planned = read(kind).unwrap();
        assert_eq!(planned.spec.window, WindowReduceWindow::Last { duration_ms: 5_000 }, "{kind:?}");
        assert_eq!(planned.spec.anchor, WindowReduceAnchor::Newest, "{kind:?}");
    }
    let clock = read(WindowKind::TrailingFromNow).unwrap();
    assert_eq!((clock.spec.window, clock.spec.anchor), (WindowReduceWindow::Last { duration_ms: 5_000 }, WindowReduceAnchor::Request));
    // A window from a start has no clock to replace: it stays on the request anchor.
    let from = Window { kind: WindowKind::FromStart as i32, span: 2_000, start_us: 1_000 };
    let from = translate_query(&profile, &query(from, selector(RankMode::FirstLane))).unwrap();
    assert_eq!(from.spec.anchor, WindowReduceAnchor::Request);
    let negative = Window { kind: WindowKind::TrailingFromNewest as i32, span: -1, start_us: 0 };
    assert!(translate_query(&profile, &query(negative, selector(RankMode::FirstLane))).is_err());
}

#[test]
fn a_quota_names_lanes_by_position_and_its_first_count_is_also_the_others_count() {
    let sent = WindowReduceSelector { quota_lanes: vec![1, 0], quota_counts: vec![2, 3], ..selector(RankMode::FirstLane) };
    let back = WindowReduceSelector::decode(sent.encode_to_vec().as_slice()).unwrap();
    assert_eq!(back, sent);
    let quota = plan(back).spec.quota;
    assert_eq!(
        quota,
        WindowReduceQuota {
            lanes: vec![
                WindowReduceLaneQuota { lane: "clicks".into(), count: 2 },
                WindowReduceLaneQuota { lane: "impressions".into(), count: 3 },
            ],
            others: 2,
        }
    );
    // One count and no lane asks for members that fit no lane, and nothing else.
    let others_only = plan(WindowReduceSelector { quota_counts: vec![4], ..selector(RankMode::FirstLane) }).spec.quota;
    assert_eq!(others_only, WindowReduceQuota { lanes: Vec::new(), others: 4 });
    assert!(plan(selector(RankMode::FirstLane)).spec.quota.is_none());
}

/// KNOWN DOUBLE COUNT, pinned so it cannot change unnoticed: the wire has one list of counts, and
/// the documented mapping reads its first entry both as the count of members that fit no lane and
/// as the count of the first lane. A valid wire request whose counts sum to 500 therefore reserves
/// 1000 members, and one that sums to 501 is refused for reserving 1002.
#[test]
fn a_quota_whose_wire_counts_sum_to_500_reserves_1000_and_one_more_is_refused() {
    let quota = |count: i32| WindowReduceSelector { quota_lanes: vec![0], quota_counts: vec![count], ..selector(RankMode::FirstLane) };
    let planned = plan(quota(500)).spec.quota;
    assert_eq!((planned.lanes[0].count, planned.others), (500, 500));
    let reserved = planned.lanes.iter().map(|l| l.count).sum::<usize>() + planned.others;
    assert_eq!(reserved, MAX_TOP_K);
    let err = refusal(quota(501));
    assert!(err.message.contains("members a quota reserves 1002 is over the limit of 1000"), "{err:?}");
}

#[test]
fn a_quota_is_accepted_at_16_lanes_and_refused_at_17_and_a_malformed_one_is_refused() {
    let profile = list_profile(17);
    let quota = |lanes: usize| WindowReduceSelector {
        quota_lanes: (0..lanes as i32).collect(),
        quota_counts: vec![1; lanes],
        ..selector(RankMode::FirstLane)
    };
    let at = translate_query(&profile, &query(trailing(), quota(16))).unwrap();
    assert_eq!(at.spec.quota.lanes.len(), 16);
    let over = translate_query(&profile, &query(trailing(), quota(17))).unwrap_err();
    assert!(over.message.contains("lanes in a quota 17 is over the limit of 16"), "{over:?}");
    let short = WindowReduceSelector { quota_lanes: vec![0, 1], quota_counts: vec![1], ..selector(RankMode::FirstLane) };
    assert!(refusal(short).message.contains("one count for each lane"));
    let negative = WindowReduceSelector { quota_lanes: vec![0], quota_counts: vec![-1], ..selector(RankMode::FirstLane) };
    assert!(refusal(negative).message.contains("negative"));
    let zero = WindowReduceSelector { quota_lanes: vec![0], quota_counts: vec![0], ..selector(RankMode::FirstLane) };
    assert!(refusal(zero).message.contains("asks for no member"));
    let unknown = WindowReduceSelector { quota_lanes: vec![2], quota_counts: vec![1], ..selector(RankMode::FirstLane) };
    assert!(refusal(unknown).message.contains("is not a lane of this table"));
    let twice = WindowReduceSelector { quota_lanes: vec![1, 1], quota_counts: vec![1, 1], ..selector(RankMode::FirstLane) };
    assert!(refusal(twice).message.contains("in the quota twice"));
}

#[test]
fn a_decayed_lane_takes_its_half_life_from_decay_made_whole_by_its_rounding() {
    let decayed = |decay: f64, rounding: Rounding| WindowReduceSelector {
        decay,
        decay_rounding: rounding as i32,
        has_sort_lane: true,
        sort_lane: 1,
        ..selector(RankMode::DecayedLane)
    };
    let half_life = |s: WindowReduceSelector| match plan(s).spec.score {
        WindowReduceScore::Decay { lane, half_life_ms } => {
            assert_eq!(lane, "clicks");
            half_life_ms
        }
        other => panic!("{other:?}"),
    };
    assert_eq!(half_life(decayed(1500.5, Rounding::Floor)), 1500);
    assert_eq!(half_life(decayed(1500.5, Rounding::Ceil)), 1501);
    assert_eq!(half_life(decayed(1500.5, Rounding::Round)), 1501);
    assert_eq!(half_life(decayed(1500.4, Rounding::Round)), 1500);
    // The lane defaults to the first, and the field numbers are the established ones.
    let first = plan(WindowReduceSelector { decay: 7.0, ..selector(RankMode::DecayedLane) });
    assert_eq!(first.spec.score, WindowReduceScore::Decay { lane: "impressions".into(), half_life_ms: 7 });
    assert_eq!(WindowReduceSelector { decay: 1.0, ..Default::default() }.encode_to_vec(), vec![0x51, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f]);
    // The mode is a new enum value; the other modes still ignore `decay`.
    assert_eq!(plan(WindowReduceSelector { decay: 99.0, ..selector(RankMode::FirstLane) }).spec.score, plan(selector(RankMode::FirstLane)).spec.score);
}

#[test]
fn a_half_life_is_accepted_at_one_and_at_ten_years_and_refused_outside_them() {
    use crate::engine::window_reduce_rank::MAX_HALF_LIFE_MS;
    let read = |decay: f64| translate_query(&pair_profile(), &query(trailing(), WindowReduceSelector { decay, ..selector(RankMode::DecayedLane) }));
    assert!(read(1.0).is_ok());
    assert!(read(MAX_HALF_LIFE_MS as f64).is_ok());
    assert!(read(MAX_HALF_LIFE_MS as f64 + 1.0).unwrap_err().message.contains("half-life"));
    assert!(read(0.0).unwrap_err().message.contains("half-life 0 ms is outside"));
    for bad in [f64::NAN, f64::INFINITY, -1.0, 1.0e19] {
        assert_eq!(read(bad).unwrap_err().code, WindowReduceStatusCode::InvalidArgument, "{bad}");
    }
}

#[test]
fn more_entities_become_one_merged_read_over_every_key_in_request_order() {
    let profile = pair_profile();
    let mut q = query(trailing(), selector(RankMode::FirstLane));
    q.more_entities = vec![8, 9];
    let q = WindowReduceQuery::decode(q.encode_to_vec().as_slice()).unwrap();
    let key_of_entity = |id: u64| {
        cell_set::encode_key(
            &profile.header().key,
            &CellKey { entity: EntityValue::U64(id), dims: vec![Some(DimValue::U16(4)), Some(DimValue::U16(3))] },
        )
        .unwrap()
    };
    let planned = translate_query(&profile, &q).unwrap();
    match planned.clone().command() {
        Command::WindowReduceFanInRank { keys, spec } => {
            assert_eq!(keys, vec![key_of_entity(7), key_of_entity(8), key_of_entity(9)]);
            assert_eq!(spec, planned.spec);
        }
        other => panic!("{other:?}"),
    }
    // A read of one key stays a one-key read, not a merged read of a single key.
    let single = translate_query(&profile, &query(trailing(), selector(RankMode::FirstLane))).unwrap();
    assert!(matches!(single.command(), Command::WindowReduceRank { .. }));
}

#[test]
fn a_merged_read_refuses_a_key_named_twice_and_an_entity_that_is_not_an_id() {
    let profile = pair_profile();
    let mut q = query(trailing(), selector(RankMode::FirstLane));
    q.more_entities = vec![8, 8];
    assert!(translate_query(&profile, &q).unwrap_err().message.contains("repeats key"));
    // The first entity is 7: naming it again is the same key twice.
    q.more_entities = vec![7];
    assert!(translate_query(&profile, &q).unwrap_err().message.contains("repeats key 0"));
    let mut text = pair_profile();
    text.entity = EntityType::Text;
    q.more_entities = vec![8];
    q.entity = 0;
    q.entity_text = "a".into();
    assert!(translate_query(&text, &q).unwrap_err().message.contains("64 bit ids"));
}

#[test]
fn a_merged_answer_reports_how_many_keys_held_a_sample() {
    let ranked = RankedCells { samples: 4, keys: 3, ..Default::default() };
    let a = answer_of(&pair_profile(), &query(trailing(), selector(RankMode::FirstLane)), &ranked);
    assert_eq!((a.samples, a.keys), (4, 3));
    // Field 20, varint: tag 0xa0 0x01.
    assert_eq!(WindowReduceAnswer { keys: 3, ..Default::default() }.encode_to_vec(), vec![0xa0, 0x01, 0x03]);
    assert_eq!(WindowReduceAnswer::decode(a.encode_to_vec().as_slice()).unwrap(), a);
}

#[test]
fn a_batch_becomes_one_batch_command_that_the_engine_admits_and_a_merged_read_has_no_place_in_it() {
    let profiles = Profiles::new(vec![pair_profile()]).unwrap();
    let first = query(trailing(), selector(RankMode::FirstLane));
    let second = query(trailing(), WindowReduceSelector { excluded_members: vec![3], ..selector(RankMode::Ratio) });
    let batch = WindowReduceBatchQuery { queries: vec![first, second] };
    let batch = WindowReduceBatchQuery::decode(batch.encode_to_vec().as_slice()).unwrap();
    let plans = translate_batch(&profiles, &batch).unwrap();
    let expected: Vec<WindowReduceRankBatchItem> =
        plans.iter().cloned().map(|p| WindowReduceRankBatchItem { key: p.key, spec: p.spec }).collect();
    assert_eq!(batch_command(plans).unwrap(), Command::WindowReduceRankBatch { queries: expected.clone() });
    assert_eq!(expected[1].spec.members.exclude, vec![CellMemberId::U64(3)]);
    let mut merged = batch.queries[0].clone();
    merged.more_entities = vec![8];
    let plans = translate_batch(&profiles, &WindowReduceBatchQuery { queries: vec![batch.queries[0].clone(), merged] }).unwrap();
    assert!(batch_command(plans).unwrap_err().message.starts_with("query 1: a read over several entities"));
    // At the engine's limit of reads the command is admitted.
    let full = translate_batch(&profiles, &WindowReduceBatchQuery { queries: vec![batch.queries[0].clone(); MAX_BATCH_QUERIES] }).unwrap();
    assert!(batch_command(full).is_ok());
}

#[test]
fn a_batch_answer_holds_one_answer_per_query_in_order_each_with_its_own_status() {
    let profiles = Profiles::new(vec![pair_profile()]).unwrap();
    let mut other = query(trailing(), selector(RankMode::FirstLane));
    other.entity = 8;
    let batch = WindowReduceBatchQuery { queries: vec![query(trailing(), selector(RankMode::FirstLane)), other] };
    let rankings = vec![
        RankedCells { samples: 2, keys: 1, ..Default::default() },
        RankedCells { refused: "why".into(), ..Default::default() },
    ];
    let answer = batch_answer(&profiles, &batch, &rankings);
    assert_eq!(answer.code, WindowReduceStatusCode::Ok as i32);
    assert_eq!(answer.answers.len(), 2);
    assert_eq!((answer.answers[0].entity, answer.answers[0].code, answer.answers[0].samples), (7, 0, 2));
    assert_eq!((answer.answers[1].entity, answer.answers[1].code), (8, WindowReduceStatusCode::InvalidArgument as i32));
    assert_eq!(answer.answers[1].message, "why");
    assert_eq!(WindowReduceBatchAnswer::decode(answer.encode_to_vec().as_slice()).unwrap(), answer);
    let short = batch_answer(&profiles, &batch, &rankings[..1]);
    assert_eq!(short.code, WindowReduceStatusCode::Error as i32);
    assert!(short.answers.is_empty());
}

// ---------------------------------------------------------------- options no read honours

#[test]
fn limit_members_is_the_row_count_of_a_stored_cell_read_that_names_no_limit_and_nothing_else() {
    let recent = |limit: i32, limit_members: i32, flagged: bool| {
        translate_query(
            &pair_profile(),
            &query(trailing(), WindowReduceSelector { limit, limit_members, has_limit_members: flagged, ..selector(RankMode::Recent) }),
        )
    };
    // No limit and a flagged member count: the count is the rows.
    assert_eq!(recent(0, 25, true).unwrap().spec.top_k, 25);
    assert_eq!(recent(-3, 25, true).unwrap().spec.top_k, 25);
    // A limit wins over it, and an unflagged or non-positive count asks nothing.
    assert_eq!(recent(7, 25, true).unwrap().spec.top_k, 7);
    assert!(recent(0, 25, false).is_err());
    assert!(recent(0, 0, true).is_err());
    assert!(recent(0, -4, true).is_err());
    // The bound on rows still holds.
    assert!(recent(0, 1000, true).is_ok());
    assert!(recent(0, 1001, true).unwrap_err().message.contains("over the limit"));

    // Every other mode ignores it: the read is the one without it.
    for mode in [RankMode::FirstLane, RankMode::SecondLane, RankMode::Member, RankMode::MatchLane] {
        let plain = plan(selector(mode));
        let asked = plan(WindowReduceSelector { limit_members: 3, has_limit_members: true, ..selector(mode) });
        assert_eq!(asked.spec, plain.spec, "{mode:?}");
    }
}

#[test]
fn a_selector_that_wants_sample_info_asks_the_read_for_it_and_changes_nothing_else() {
    let plain = plan(selector(RankMode::FirstLane));
    assert!(!plain.spec.sample_info, "the read asks for nothing it was not asked for");
    // Wanted, with or without its presence flag, for every mode that reads.
    for flagged in [false, true] {
        for mode in [RankMode::FirstLane, RankMode::SecondLane, RankMode::Ratio, RankMode::Member, RankMode::Recent] {
            let base = plan(selector(mode));
            let asked = plan(WindowReduceSelector { want_sample_info: true, has_want_sample_info: flagged, ..selector(mode) });
            assert!(asked.spec.sample_info, "{mode:?} flagged {flagged}");
            assert_eq!(WindowReduceRankSpec { sample_info: false, ..asked.spec }, base.spec, "{mode:?}");
            assert_eq!(asked.key, base.key);
        }
    }
    // A query inside a batch carries it too.
    let profiles = Profiles::new(vec![pair_profile()]).unwrap();
    let mut wants = query(trailing(), selector(RankMode::FirstLane));
    wants.selector.as_mut().unwrap().want_sample_info = true;
    let plans = translate_batch(&profiles, &WindowReduceBatchQuery { queries: vec![query(trailing(), selector(RankMode::FirstLane)), wants] })
        .unwrap();
    assert_eq!(plans.iter().map(|p| p.spec.sample_info).collect::<Vec<_>>(), vec![false, true]);
}

#[test]
fn an_answer_says_which_samples_the_read_used_in_microseconds_and_leaves_the_field_out_when_not_asked() {
    let profile = pair_profile();
    let q = query(trailing(), selector(RankMode::FirstLane));
    let used = vec![
        WindowReduceSampleInfo { timestamp_ms: 1_700_000_000_123, cells: 4 },
        WindowReduceSampleInfo { timestamp_ms: 1_700_000_000_456, cells: 2 },
    ];
    let asked = answer_of(&profile, &q, &RankedCells { samples: 2, sample_info: used, ..Default::default() });
    let info = asked.sample_info.as_ref().expect("the read asked for it");
    // A sample is one instant: its start and its end are the same.
    assert_eq!(info.starts_us, vec![1_700_000_000_123_000, 1_700_000_000_456_000]);
    assert_eq!(info.ends_us, info.starts_us);
    assert_eq!(info.member_counts, vec![4, 2]);
    // Not asked: no message at all.
    assert!(answer_of(&profile, &q, &RankedCells { samples: 2, ..Default::default() }).sample_info.is_none());

    // The bytes: the entity (field 1, the query's 7), then one sample at 1 ms holding 3 cells as
    // field 7 (starts, ends, counts, each packed), then the established `samples` field. An answer
    // that did not ask has no field 7.
    let one = RankedCells {
        samples: 1,
        sample_info: vec![WindowReduceSampleInfo { timestamp_ms: 1, cells: 3 }],
        ..Default::default()
    };
    assert_eq!(
        answer_of(&profile, &q, &one).encode_to_vec(),
        vec![0x08, 0x07, 0x3a, 0x0b, 0x0a, 0x02, 0xe8, 0x07, 0x12, 0x02, 0xe8, 0x07, 0x1a, 0x01, 0x03, 0x90, 0x01, 0x01]
    );
    assert_eq!(
        answer_of(&profile, &q, &RankedCells { samples: 1, ..Default::default() }).encode_to_vec(),
        vec![0x08, 0x07, 0x90, 0x01, 0x01]
    );
}

#[test]
fn a_selector_that_sets_them_off_is_translated_to_exactly_the_read_it_was_without_them() {
    let plain = plan(selector(RankMode::FirstLane));
    // A false `want_sample_info` and a limit of 0 are statements that nothing is wanted, even
    // when their presence flags are on; they change no byte of the read.
    let off = plan(WindowReduceSelector {
        want_sample_info: false,
        has_want_sample_info: true,
        limit_members: 0,
        has_limit_members: true,
        ..selector(RankMode::FirstLane)
    });
    assert_eq!(off.spec, plain.spec);
    assert_eq!(off.key, plain.key);
}

// ---------------------------------------------------------------- stored cells, newest first

fn slot_is_five() -> FeatureFilter {
    FeatureFilter { field: "slot".into(), op: FeatureFilterOp::Equal, value: 5 }
}

#[test]
fn recent_is_the_stored_cell_read_with_the_selectors_label_and_limit() {
    let recent = plan(selector(RankMode::Recent));
    assert_eq!(recent.spec.score, WindowReduceScore::Recent);
    assert_eq!(recent.spec.filters, vec![slot_is_five()]);
    assert_eq!(recent.spec.top_k, 10);
    // The limit is the read's: accepted at 1000, refused at 1001 and at 0.
    let limited = |limit: i32| translate_query(&pair_profile(), &query(trailing(), WindowReduceSelector { limit, ..selector(RankMode::Recent) }));
    assert_eq!(limited(1000).unwrap().spec.top_k, 1000);
    assert!(limited(1001).unwrap_err().message.contains("over the limit"));
    assert!(limited(0).is_err());
    // A quota with a lane is the walk of stored cells, and its window ends at its end stamp too.
    let quota = WindowReduceSelector { quota_lanes: vec![0], quota_counts: vec![1], ..selector(RankMode::Recent) };
    let planned = plan(quota);
    assert_eq!((planned.spec.quota.lanes.len(), planned.spec.quota.others), (1, 1));
    // A quota of `others` alone has no lane to fill, so it is refused with the engine's own words.
    let others_only = WindowReduceSelector { quota_lanes: vec![], quota_counts: vec![1], ..selector(RankMode::Recent) };
    assert!(refusal(others_only).message.contains("needs at least one lane"));
}

#[test]
fn match_lane_adds_an_equality_on_the_lane_it_names_and_matches_nothing_on_a_lane_the_table_lacks() {
    let matched = |lane: Option<i64>, value: Option<i64>| {
        translate_query(
            &pair_profile(),
            &query(
                trailing(),
                WindowReduceSelector {
                    has_match_lane: lane.is_some(),
                    match_lane: lane.unwrap_or(0),
                    has_match_value: value.is_some(),
                    match_value: value.unwrap_or(0),
                    ..selector(RankMode::MatchLane)
                },
            ),
        )
    };
    let on = |lane: &str, value: u64| FeatureFilter { field: lane.into(), op: FeatureFilterOp::Equal, value };
    let first = matched(Some(0), Some(42)).unwrap();
    assert_eq!(first.spec.score, WindowReduceScore::Recent);
    assert_eq!(first.spec.filters, vec![slot_is_five(), on("impressions", 42)]);
    assert_eq!(matched(Some(1), Some(7)).unwrap().spec.filters, vec![slot_is_five(), on("clicks", 7)]);
    // An absent lane is the first and an absent value is 0, as a sort lane is.
    assert_eq!(matched(None, None).unwrap().spec.filters, vec![slot_is_five(), on("impressions", 0)]);
    // The table has two lanes. A lane it does not declare (a third, a negative one, one too wide
    // for an i32) holds nothing: the test no cell passes, and the read answers nothing.
    let nothing = FeatureFilter { field: "member".into(), op: FeatureFilterOp::LessThan, value: 0 };
    for lane in [2, -1, i64::MAX, i64::MIN] {
        assert_eq!(matched(Some(lane), Some(1)).unwrap().spec.filters, vec![slot_is_five(), nothing.clone()], "lane {lane}");
    }
    // A lane is signed, so a negative value is a value: it travels as the 64 bits it was sent in.
    assert_eq!(matched(Some(0), Some(-1)).unwrap().spec.filters, vec![slot_is_five(), on("impressions", u64::MAX)]);
    assert_eq!(matched(Some(1), Some(i64::MIN)).unwrap().spec.filters, vec![slot_is_five(), on("clicks", 1 << 63)]);
    // The match fields mean nothing to any other mode: its filters are the label alone.
    let ignored = WindowReduceSelector { has_match_lane: true, match_lane: 1, has_match_value: true, match_value: 9, ..selector(RankMode::FirstLane) };
    assert_eq!(plan(ignored).spec.filters, vec![slot_is_five()]);
}

#[test]
fn a_stored_cell_answer_is_one_entry_per_row_stamped_with_its_own_sample() {
    let profile = pair_profile();
    let q = query(trailing(), selector(RankMode::Recent));
    let ranked = RankedCells {
        samples: 3,
        window_first_ms: 50,
        window_last_ms: 400,
        keys: 1,
        rows: vec![
            crate::types::RankedCellRow { member: CellMemberId::U64(7), score: 300.0, last_ms: 300, labels: vec![5], lanes: vec![1, 2] },
            crate::types::RankedCellRow { member: CellMemberId::U64(7), score: 100.0, last_ms: 100, labels: vec![5], lanes: vec![3, 4] },
        ],
        ..Default::default()
    };
    let answer = answer_of(&profile, &q, &ranked);
    assert_eq!(answer.entries.len(), 2);
    assert_eq!((answer.entries[0].member, answer.entries[1].member), (7, 7));
    assert_eq!(answer.entries[0].pair.as_ref().unwrap().timestamp_us, 300_000);
    assert_eq!(answer.entries[1].pair.as_ref().unwrap().timestamp_us, 100_000);
    // A stored-cell read reports the span of the window it scanned, not of the rows it kept.
    assert_eq!((answer.first_us, answer.last_us), (50_000, 400_000));
}

#[test]
fn first_and_last_say_what_the_window_scanned_except_for_the_reads_that_select_by_member_or_lane_value() {
    let profile = pair_profile();
    let ranked = RankedCells {
        samples: 4,
        window_first_ms: 10,
        window_last_ms: 90,
        matched_first_ms: 20,
        matched_last_ms: 80,
        keys: 1,
        rows: vec![crate::types::RankedCellRow { member: CellMemberId::U64(7), score: 1.0, last_ms: 60, labels: vec![5], lanes: vec![1, 2] }],
        ..Default::default()
    };
    let span = |mode: RankMode, ranked: &RankedCells| {
        let a = answer_of(&profile, &query(trailing(), selector(mode)), ranked);
        (a.first_us, a.last_us)
    };
    for mode in [RankMode::FirstLane, RankMode::Ratio, RankMode::Wilson, RankMode::Member, RankMode::Recent, RankMode::DecayedLane] {
        assert_eq!(span(mode, &ranked), (10_000, 90_000), "{mode:?}");
    }
    // MEMBERS_ONLY: the oldest and newest sample that held a selected member (20 and 80), not the
    // newest sample of each returned member; MATCH_LANE: the rows it returned.
    assert_eq!(span(RankMode::MembersOnly, &ranked), (20_000, 80_000));
    assert_eq!(span(RankMode::MatchLane, &ranked), (60_000, 60_000));
    // A window that selected no sample is 0 and 0.
    assert_eq!(span(RankMode::FirstLane, &RankedCells::default()), (0, 0));
}
