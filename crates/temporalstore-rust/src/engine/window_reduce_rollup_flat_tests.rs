// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A rollup over samples held flat rolls up exactly what the same rollup over `Vec<Cell>`
//! samples rolls up: the same samples, to the byte once encoded, and the same counts. The
//! plain form is the statement of the semantics; the flat form is what the engine runs, so the
//! two are compared on random data under every control a rollup has.

use super::*;
use crate::engine::cell_set::{
    decode_sample, encode_value, split_value, DimSpec, DimType, EntityType, KeySpec, LaneSpec, Member,
    HEADER_VERSION,
};
use crate::types::{WindowReduceCapSpec, WindowReduceRollupSpec, WindowReduceRollupTier, WindowReduceScore};

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Four labels (the most a cell holds) and six lanes, every reducer once: more lanes than a small vector holds, so a path that
/// grows its vectors and one that does not come out the same.
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

/// `count` samples stamped 100, 200, ...; members 0..48 as numbers plus three bytes members,
/// each sample holding a random subset.
fn random_samples(state: &mut u64, count: usize) -> Vec<(u64, Vec<Cell>)> {
    let mut out = Vec::new();
    for at in 0..count {
        let mut cells: Vec<Cell> = Vec::new();
        let mut cell = |state: &mut u64, member: Member| Cell {
            member,
            labels: (0..4).map(|_| xorshift(state) % 4).collect(),
            lanes: (0..6).map(|_| (xorshift(state) % 41) as i64 - 8).collect(),
        };
        for m in 0..48u64 {
            if xorshift(state) % 3 != 0 {
                cells.push(cell(state, Member::U64(m)));
            }
        }
        for tag in [&b"aa"[..], &b"zz"[..], &b"zzz"[..]] {
            if xorshift(state) % 2 == 0 {
                cells.push(cell(state, Member::Bytes(tag.to_vec())));
            }
        }
        out.push((100 * (at as u64 + 1), cells));
    }
    out
}

fn spec(older_than_ms: u64, bucket_ms: u64, cap: Option<WindowReduceCapSpec>) -> WindowReduceRollupSpec {
    WindowReduceRollupSpec {
        older_than_ms,
        bucket_ms,
        cap,
        anchor: Default::default(),
        coarser_tiers: Vec::new(),
        truncate: None,
    }
}

fn cap(group_by: Option<&str>, keep: usize, protect_newest_ppm: u32, trigger_over: usize) -> WindowReduceCapSpec {
    WindowReduceCapSpec {
        group_by: group_by.map(str::to_string),
        score: WindowReduceScore::Lane { lane: "a".to_string() },
        keep,
        protect_newest_ppm,
        trigger_over,
    }
}

fn specs() -> Vec<WindowReduceRollupSpec> {
    let mut tiered = spec(1_800, 300, None);
    tiered.coarser_tiers = vec![WindowReduceRollupTier { older_than_ms: 900, bucket_ms: 900 }];
    let mut tiered_capped = spec(1_800, 300, Some(cap(Some("g1"), 6, 500_000, 2)));
    tiered_capped.coarser_tiers = vec![WindowReduceRollupTier { older_than_ms: 900, bucket_ms: 900 }];
    vec![
        spec(1_800, 300, None),
        spec(1_800, 100, None),
        spec(2_500, 600, Some(cap(None, 10, 0, 0))),
        spec(2_500, 600, Some(cap(Some("g0"), 4, 0, 0))),
        spec(2_500, 600, Some(cap(Some("g2"), 5, 400_000, 0))),
        spec(2_500, 1_200, Some(cap(None, 12, 250_000, 3))),
        tiered,
        tiered_capped,
        // Nothing below the cutoff: nothing is rolled.
        spec(50, 100, None),
    ]
}

#[test]
fn a_rollup_over_flat_samples_rolls_up_what_a_rollup_over_plain_samples_rolls_up() {
    let header = wide_header();
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let (mut combined, mut dropped, mut compared_cells) = (0u64, 0u64, 0u64);
    for count in [1usize, 3, 12, 24] {
        let snaps = random_samples(&mut state, count);
        for spec in specs() {
            let mut plain = Vec::new();
            let mut flat = Vec::new();
            for (stamp, cells) in &snaps {
                let value = encode_value(&header, cells).unwrap();
                let (own, packed) = split_value(&value).unwrap();
                assert_eq!(own, header);
                plain.push(Sample { timestamp_ms: *stamp, cells: decode_sample(&header, packed).unwrap() });
                flat.push(FlatSample { timestamp_ms: *stamp, cells: FlatCells::decode(&header, packed, 0).unwrap() });
            }
            let newest = snaps.last().map_or(0, |(stamp, _)| *stamp);
            let want = rollup_spec(&header, plain, &spec, newest).unwrap();
            let got = rollup_flat_spec(&header, flat, &spec, newest).unwrap();
            assert_eq!(want.stats, got.stats, "{count} samples, spec {spec:?}");
            assert_eq!(want.samples.len(), got.samples.len(), "{count} samples, spec {spec:?}");
            for (w, g) in want.samples.iter().zip(&got.samples) {
                assert_eq!(w.timestamp_ms, g.timestamp_ms, "{count} samples, spec {spec:?}");
                assert_eq!(
                    encode_value(&header, &w.cells).unwrap(),
                    g.cells.encode_value(&header).unwrap(),
                    "{count} samples, stamp {}, spec {spec:?}",
                    w.timestamp_ms
                );
                compared_cells += w.cells.len() as u64;
            }
            combined += got.stats.cells_combined;
            dropped += got.stats.cells_dropped;
        }
    }
    // The controls: the comparison saw real folds and real caps, not a run of empty answers.
    assert!(combined > 500, "only {combined} cells were folded in all");
    assert!(dropped > 100, "the caps removed only {dropped} cells in all");
    assert!(compared_cells > 1_000, "only {compared_cells} cells were compared");
}

#[test]
fn a_flat_sample_out_of_order_is_refused_as_a_plain_one_is() {
    let header = wide_header();
    let unordered = vec![
        Cell { member: Member::U64(5), labels: vec![0; 4], lanes: vec![1; 6] },
        Cell { member: Member::U64(2), labels: vec![0; 4], lanes: vec![1; 6] },
    ];
    let value = encode_value(&header, &unordered).unwrap();
    let (_, packed) = split_value(&value).unwrap();
    let plain = vec![Sample { timestamp_ms: 100, cells: decode_sample(&header, packed).unwrap() }];
    let flat = vec![FlatSample { timestamp_ms: 100, cells: FlatCells::decode(&header, packed, 0).unwrap() }];
    let spec = spec(1_000, 100, None);
    let want = rollup_spec(&header, plain, &spec, 100).unwrap_err();
    let got = rollup_flat_spec(&header, flat, &spec, 100).unwrap_err();
    assert_eq!(want, got);
    assert!(got.contains("strictly ascending"), "{got}");
}
