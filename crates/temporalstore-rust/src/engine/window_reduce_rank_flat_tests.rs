// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The ranked read over the flat form of a sample answers exactly what the same read over the
//! plain `Vec<Cell>` form answers. The plain form is the statement of the format; the flat form
//! is what the engine reads, with no allocation per cell, so the two are compared on random data
//! with every option a read can carry.

use super::*;
use crate::engine::cell_set::{
    decode_sample, encode_value, split_value, DimSpec, DimType, EntityType, KeySpec, LaneSpec, HEADER_VERSION,
};
use crate::types::{WindowReduceLaneQuota, CellMemberFilter, WindowReduceQuota};

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Four labels (the most a cell holds) and six lanes: more lanes than the four a small vector holds without growing, so a
/// decode that grows its vectors and one that does not both come out the same here.
fn wide_header() -> CellHeader {
    use LaneReducer::*;
    CellHeader {
        version: HEADER_VERSION,
        key: KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
        },
        labels: (0..4).map(|i| format!("g{i}")).collect(),
        lanes: [("a", Sum), ("b", Max), ("c", Min), ("d", First), ("e", Last), ("f", Count)]
            .into_iter()
            .map(|(name, reducer)| LaneSpec { name: name.to_string(), reducer })
            .collect(),
    }
}

/// Members 0..48 as numbers plus a few bytes members, each sample holding a random subset.
fn random_samples(state: &mut u64, count: usize) -> Vec<(u64, Vec<Cell>)> {
    let mut out = Vec::new();
    for at in 0..count {
        let mut cells: Vec<Cell> = Vec::new();
        for m in 0..48u64 {
            if xorshift(state) % 3 != 0 {
                let labels = (0..4).map(|_| xorshift(state) % 6).collect();
                let lanes = (0..6).map(|_| (xorshift(state) % 41) as i64 - 8).collect();
                cells.push(Cell { member: Member::U64(m), labels, lanes });
            }
        }
        for tag in [&b"aa"[..], &b"zz"[..], &b"zzz"[..]] {
            if xorshift(state) % 2 == 0 {
                let labels = (0..4).map(|_| xorshift(state) % 6).collect();
                let lanes = (0..6).map(|_| (xorshift(state) % 41) as i64 - 8).collect();
                cells.push(Cell { member: Member::Bytes(tag.to_vec()), labels, lanes });
            }
        }
        out.push((100 * (at as u64 + 1), cells));
    }
    out
}

fn decoded_both(header: &CellHeader, snaps: &[(u64, Vec<Cell>)]) -> (Vec<(u64, Vec<Cell>)>, Vec<(u64, FlatCells)>) {
    let mut plain = Vec::new();
    let mut flat = Vec::new();
    for (stamp, cells) in snaps {
        let value = encode_value(header, cells).unwrap();
        let (own, packed) = split_value(&value).unwrap();
        assert_eq!(own, *header);
        plain.push((*stamp, decode_sample(header, packed).unwrap()));
        flat.push((*stamp, FlatCells::decode(header, packed, 0).unwrap()));
    }
    (plain, flat)
}

fn specs() -> Vec<WindowReduceRankSpec> {
    let all = WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX };
    let lane = |name: &str| WindowReduceScore::Lane { lane: name.to_string() };
    let mut out = vec![
        WindowReduceRankSpec::new(all.clone(), lane("a"), 8),
        WindowReduceRankSpec::new(all.clone(), lane("b"), 100),
        WindowReduceRankSpec::new(all.clone(), WindowReduceScore::Member, 5),
        WindowReduceRankSpec::new(all.clone(), WindowReduceScore::Time, 5),
        WindowReduceRankSpec::new(all.clone(), WindowReduceScore::Ratio { numerator: "a".to_string(), denominator: "c".to_string() }, 7),
        WindowReduceRankSpec::new(
            all.clone(),
            WindowReduceScore::Weighted { lanes: vec!["a".to_string(), "e".to_string()], weights: vec![1.5, -0.25] },
            9,
        ),
        WindowReduceRankSpec::new(all.clone(), WindowReduceScore::Decay { lane: "a".to_string(), half_life_ms: 250 }, 6),
        WindowReduceRankSpec::new(WindowReduceWindow::LastN { n: 3 }, lane("f"), 10),
        WindowReduceRankSpec::new(WindowReduceWindow::Between { start_ms: 200, end_ms: 500 }, lane("d"), 10),
    ];
    let mut filtered = WindowReduceRankSpec::new(all.clone(), lane("a"), 10);
    filtered.filters = vec![
        FeatureFilter { field: "g2".to_string(), op: FeatureFilterOp::GreaterOrEqual, value: 2 },
        FeatureFilter { field: "b".to_string(), op: FeatureFilterOp::LessThan, value: 20 },
        FeatureFilter { field: "member".to_string(), op: FeatureFilterOp::GreaterThan, value: 3 },
    ];
    filtered.project = vec!["g3".to_string(), "e".to_string(), "a".to_string()];
    out.push(filtered);
    let mut members = WindowReduceRankSpec::new(all.clone(), lane("a"), 20);
    members.members = CellMemberFilter {
        include: Some(vec![
            CellMemberId::U64(3),
            CellMemberId::U64(11),
            CellMemberId::U64(40),
            CellMemberId::Bytes(b"zz".to_vec()),
            CellMemberId::Bytes(b"aa".to_vec()),
        ]),
        exclude: vec![CellMemberId::U64(11)],
    };
    out.push(members);
    let mut open_exclude = WindowReduceRankSpec::new(all.clone(), lane("c"), 12);
    open_exclude.members =
        CellMemberFilter { include: None, exclude: vec![CellMemberId::Bytes(b"zzz".to_vec()), CellMemberId::U64(0)] };
    out.push(open_exclude);
    let mut quota = WindowReduceRankSpec::new(all, lane("a"), 12);
    quota.quota = WindowReduceQuota {
        lanes: vec![WindowReduceLaneQuota { lane: "b".to_string(), count: 2 }],
        others: 3,
    };
    out.push(quota);
    out
}

#[test]
fn a_read_over_flat_samples_answers_what_a_read_over_plain_samples_answers() {
    let header = wide_header();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut compared = 0;
    let mut rows_seen = 0;
    let mut refused = 0;
    for count in [1usize, 2, 7, 12] {
        let snaps = random_samples(&mut state, count);
        let (plain, flat) = decoded_both(&header, &snaps);
        let plain_views: Vec<SampleView<'_>> =
            plain.iter().map(|(timestamp_ms, cells)| SampleView { timestamp_ms: *timestamp_ms, cells }).collect();
        let flat_views: Vec<FlatView<'_>> =
            flat.iter().map(|(timestamp_ms, cells)| FlatView { timestamp_ms: *timestamp_ms, cells }).collect();
        for spec in specs() {
            let plan = Plan::compile(&header, &spec).unwrap();
            // The answers are compared as results: the `member` score refuses a bytes member, and
            // both forms must refuse it in the same words.
            let want = plan.run(1_000, &plain_views);
            let got = plan.run(1_000, &flat_views);
            assert_eq!(want, got, "{count} samples, spec {spec:?}");
            match &got {
                Ok(ranked) => rows_seen += ranked.rows.len(),
                Err(_) => refused += 1,
            }
            compared += 1;
        }
    }
    // The control: the comparison saw real answers, not a run of empty ones.
    assert!(compared >= 48, "{compared}");
    assert!(rows_seen > 300, "the reads ranked only {rows_seen} rows in all");
    // And the one read the data cannot serve (a `member` score over bytes members) was refused by
    // both forms, once per sample count that held a bytes member.
    assert!(refused >= 1, "no read was refused, so the refusal arm was not compared");
}

#[test]
fn a_fan_in_over_flat_samples_answers_what_a_fan_in_over_plain_samples_answers() {
    let header = wide_header();
    let mut state = 0x1234_5678_9abc_def1u64;
    let keys: Vec<Vec<(u64, Vec<Cell>)>> = (0..4).map(|_| random_samples(&mut state, 5)).collect();
    let decoded: Vec<_> = keys.iter().map(|snaps| decoded_both(&header, snaps)).collect();
    let plain_views: Vec<Vec<SampleView<'_>>> = decoded
        .iter()
        .map(|(plain, _)| {
            plain.iter().map(|(timestamp_ms, cells)| SampleView { timestamp_ms: *timestamp_ms, cells }).collect()
        })
        .collect();
    let flat_views: Vec<Vec<FlatView<'_>>> = decoded
        .iter()
        .map(|(_, flat)| {
            flat.iter().map(|(timestamp_ms, cells)| FlatView { timestamp_ms: *timestamp_ms, cells }).collect()
        })
        .collect();
    let plain_slices: Vec<&[SampleView<'_>]> = plain_views.iter().map(Vec::as_slice).collect();
    let flat_slices: Vec<&[FlatView<'_>]> = flat_views.iter().map(Vec::as_slice).collect();
    for spec in specs() {
        let plan = Plan::compile(&header, &spec).unwrap();
        let want = plan.run_fan_in(1_000, &plain_slices);
        let got = plan.run_fan_in(1_000, &flat_slices);
        assert_eq!(want, got, "spec {spec:?}");
        if let Ok(ranked) = &got {
            assert_eq!(4, ranked.keys);
        }
    }
}

#[test]
fn a_flat_sample_out_of_order_is_refused_as_a_plain_one_is() {
    let header = wide_header();
    let unordered = vec![
        Cell { member: Member::U64(5), labels: vec![0; 4], lanes: vec![1; 6] },
        Cell { member: Member::U64(2), labels: vec![0; 4], lanes: vec![1; 6] },
    ];
    let (plain, flat) = decoded_both(&header, &[(100, unordered)]);
    let spec = WindowReduceRankSpec::new(WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX }, WindowReduceScore::Member, 5);
    let plan = Plan::compile(&header, &spec).unwrap();
    let plain_views = [SampleView { timestamp_ms: plain[0].0, cells: &plain[0].1 }];
    let flat_views = [FlatView { timestamp_ms: flat[0].0, cells: &flat[0].1 }];
    let want = plan.run(1_000, &plain_views).unwrap_err();
    let got = plan.run(1_000, &flat_views).unwrap_err();
    assert_eq!(want, got);
    assert!(got.contains("strictly ascending"), "{got}");
}
