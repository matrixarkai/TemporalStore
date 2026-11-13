// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the cell rollup and the capped group. Every expected number is worked out by hand in
//! a comment beside it, or comes from a fold written separately in this file.

use super::*;
use crate::engine::cell_set::{
    decode_snapshot, encode_snapshot, DimSpec, DimType, EntityType, KeySpec, LaneSpec, Member, HEADER_VERSION,
};
use std::collections::BTreeMap;

/// Lane positions. The reducers differ on purpose: a rollup that folded every lane one way could
/// not produce the numbers asserted below.
const SUM: usize = 0;
const COUNT: usize = 1;
const PEAK: usize = 2;
const LOW: usize = 3;
const FIRST: usize = 4;
const LAST: usize = 5;

fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
        },
        labels: vec!["slot".to_string()],
        lanes: [
            ("shown", LaneReducer::Sum),
            ("events", LaneReducer::Count),
            ("peak", LaneReducer::Max),
            ("low", LaneReducer::Min),
            ("opened", LaneReducer::First),
            ("seen", LaneReducer::Last),
        ]
        .into_iter()
        .map(|(name, reducer)| LaneSpec { name: name.to_string(), reducer })
        .collect(),
    }
}

fn cell(member: u64, slot: u64, lanes: [i64; 6]) -> Cell {
    Cell { member: Member::U64(member), labels: vec![slot], lanes: lanes.to_vec() }
}

fn snap(timestamp_ms: u64, cells: Vec<Cell>) -> Snapshot {
    Snapshot { timestamp_ms, cells }
}

fn spec(older_than_ms: u64, bucket_ms: u64) -> RollupSpec {
    RollupSpec { older_than_ms, bucket_ms, cap: None }
}

fn lane_score(name: &str) -> CellScore {
    CellScore::Lane { lane: name.to_string() }
}

fn cap(group_by: Option<&str>, score: CellScore, keep: usize) -> CapSpec {
    CapSpec { group_by: group_by.map(str::to_string), score, keep }
}

fn bytes_of(snapshots: &[Snapshot]) -> Vec<(u64, Vec<u8>)> {
    snapshots
        .iter()
        .map(|s| (s.timestamp_ms, encode_snapshot(&header(), &s.cells).unwrap()))
        .collect()
}

// ------------------------------------------------------------------------------------------
// An independent fold
// ------------------------------------------------------------------------------------------

/// Small deterministic generator, so a failure reproduces.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

/// Snapshots at irregular stamps over members 0..60, each member present about half the time.
/// Lane values are small (|v| < 1000) so no sum can approach a limit.
fn series(seed: u64, count: usize) -> Vec<Snapshot> {
    let mut rng = Lcg(seed);
    let mut ts = 5;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        ts += 1 + rng.next() % 40;
        let mut cells = Vec::new();
        for m in 0..60u64 {
            if rng.next() % 2 != 0 {
                continue;
            }
            let mut lanes = [0i64; 6];
            for lane in lanes.iter_mut() {
                *lane = (rng.next() % 1999) as i64 - 999;
            }
            cells.push(cell(m, (m + ts) % 5, lanes));
        }
        out.push(snap(ts, cells));
    }
    out
}

/// What a rollup must produce, written without the module's fold: the occurrences of each member
/// of each bucket are gathered in stamp order and each lane is computed from its own list.
fn independent_fold(snaps: &[Snapshot], cutoff: u64, bucket: u64) -> Vec<Snapshot> {
    let mut buckets: BTreeMap<u64, BTreeMap<u64, Vec<(Vec<u64>, Vec<i64>)>>> = BTreeMap::new();
    let mut untouched = Vec::new();
    for s in snaps {
        if s.timestamp_ms >= cutoff {
            untouched.push(s.clone());
            continue;
        }
        let start = (s.timestamp_ms / bucket) * bucket;
        for c in &s.cells {
            let Member::U64(m) = c.member else { panic!("u64 members only") };
            buckets.entry(start).or_default().entry(m).or_default().push((c.labels.clone(), c.lanes.clone()));
        }
    }
    let mut out: Vec<Snapshot> = buckets
        .into_iter()
        .map(|(start, members)| {
            let cells = members
                .into_iter()
                .map(|(m, seen)| {
                    let column = |i: usize| seen.iter().map(|(_, lanes)| lanes[i]).collect::<Vec<i64>>();
                    let mut lanes = vec![0i64; 6];
                    lanes[SUM] = column(SUM).iter().sum();
                    lanes[COUNT] = column(COUNT).iter().sum();
                    lanes[PEAK] = *column(PEAK).iter().max().unwrap();
                    lanes[LOW] = *column(LOW).iter().min().unwrap();
                    lanes[FIRST] = column(FIRST)[0];
                    lanes[LAST] = *column(LAST).last().unwrap();
                    Cell { member: Member::U64(m), labels: seen.last().unwrap().0.clone(), lanes }
                })
                .collect();
            Snapshot { timestamp_ms: start, cells }
        })
        .collect();
    out.extend(untouched);
    out
}

#[test]
fn a_rollup_equals_an_independent_fold_for_every_bucket_size_and_cutoff() {
    let snaps = series(7, 300);
    let last_ts = snaps.last().unwrap().timestamp_ms;
    let mut compared_cells = 0usize;
    let mut buckets_with_several = 0usize;
    for &bucket in &[1u64, 7, 100, 1000, 100_000] {
        for &cutoff in &[0u64, 1, last_ts / 3, last_ts / 2, last_ts, last_ts + 1, u64::MAX] {
            let want = independent_fold(&snaps, cutoff, bucket);
            let got = rollup(&header(), snaps.clone(), &spec(cutoff, bucket)).unwrap();
            assert_eq!(got.snapshots, want, "bucket {bucket} cutoff {cutoff}");
            let s = got.stats;
            assert_eq!(s.snapshots_before, snaps.len() as u64);
            assert_eq!(s.snapshots_after, want.len() as u64);
            assert_eq!(s.cells_before, snaps.iter().map(|x| x.cells.len() as u64).sum::<u64>());
            assert_eq!(s.cells_after, want.iter().map(|x| x.cells.len() as u64).sum::<u64>());
            assert_eq!(s.cells_dropped, 0);
            assert_eq!(s.cells_before, s.cells_after + s.cells_combined + s.cells_dropped);
            compared_cells += want.iter().map(|x| x.cells.len()).sum::<usize>();
            buckets_with_several += (s.snapshots_after < s.snapshots_before) as usize;
        }
    }
    assert!(compared_cells > 20_000, "only {compared_cells} cells were compared");
    assert!(buckets_with_several > 15, "only {buckets_with_several} runs folded anything");
}

// ------------------------------------------------------------------------------------------
// Combining
// ------------------------------------------------------------------------------------------

/// Three snapshots of one bucket, one member:
///   shown   10, 20, 30  Sum   -> 60          events 3, 4, 5  Count -> 12 (counts add)
///   peak     5,  9,  2  Max   -> 9           low    5, 9, 2  Min   -> 2
///   opened  11, 22, 33  First -> 11          seen   1, 2, 3  Last  -> 3
#[test]
fn each_lane_combines_oldest_first_and_a_count_is_the_sum_of_counts_not_the_number_of_snapshots() {
    let snaps = vec![
        snap(10, vec![cell(1, 4, [10, 3, 5, 5, 11, 1])]),
        snap(20, vec![cell(1, 5, [20, 4, 9, 9, 22, 2])]),
        snap(30, vec![cell(1, 6, [30, 5, 2, 2, 33, 3])]),
    ];
    let got = rollup(&header(), snaps, &spec(100, 100)).unwrap();
    assert_eq!(got.snapshots.len(), 1);
    assert_eq!(got.snapshots[0].timestamp_ms, 0, "a bucket is stored at its start");
    // Labels are the newest cell's.
    assert_eq!(got.snapshots[0].cells, vec![cell(1, 6, [60, 12, 9, 2, 11, 3])]);

    // THE DIFFERENCE: the write-time merge of the same three values counts merge events.
    let write_time = [3i64, 4, 5].iter().skip(1).fold(3, |existing, incoming| LaneReducer::Count.combine(existing, *incoming));
    assert_eq!(write_time, 5, "folding as writes counts events, not the counts carried");
    assert_ne!(write_time, 12);
    // And a member present in one snapshot only keeps its stored count: it is not reset to 1.
    let alone = rollup(&header(), vec![snap(10, vec![cell(2, 0, [1, 7, 1, 1, 1, 1])])], &spec(100, 100)).unwrap();
    assert_eq!(alone.snapshots[0].cells[0].lanes[COUNT], 7);
}

#[test]
fn members_that_meet_merge_and_members_that_do_not_pass_through_in_order() {
    let snaps = vec![
        snap(1, vec![cell(1, 0, [1, 1, 1, 1, 1, 1]), cell(3, 0, [3, 1, 3, 3, 3, 3])]),
        snap(2, vec![cell(2, 0, [2, 1, 2, 2, 2, 2]), cell(3, 1, [4, 1, 4, 4, 4, 4])]),
    ];
    let got = rollup(&header(), snaps, &spec(10, 10)).unwrap();
    assert_eq!(
        got.snapshots[0].cells,
        vec![
            cell(1, 0, [1, 1, 1, 1, 1, 1]),
            cell(2, 0, [2, 1, 2, 2, 2, 2]),
            cell(3, 1, [7, 2, 4, 3, 3, 4]),
        ]
    );
    assert_eq!(got.stats.cells_combined, 1);
    assert_eq!(got.stats.cells_before, 4);
    assert_eq!(got.stats.cells_after, 3);
}

#[test]
fn sums_and_counts_saturate_and_never_wrap() {
    assert_eq!(combine_aggregated(LaneReducer::Sum, i64::MAX, 1), i64::MAX);
    assert_eq!(combine_aggregated(LaneReducer::Sum, i64::MAX - 1, 1), i64::MAX, "exactly at the limit is exact");
    assert_eq!(combine_aggregated(LaneReducer::Sum, i64::MIN, -1), i64::MIN);
    assert_eq!(combine_aggregated(LaneReducer::Count, i64::MAX, 5), i64::MAX);
    // Through the whole function: two snapshots whose sums add past the limit.
    let snaps = vec![
        snap(1, vec![cell(1, 0, [i64::MAX, i64::MAX, 0, 0, 0, 0])]),
        snap(2, vec![cell(1, 0, [10, 10, 0, 0, 0, 0])]),
    ];
    let got = rollup(&header(), snaps, &spec(10, 10)).unwrap();
    assert_eq!(got.snapshots[0].cells[0].lanes[SUM], i64::MAX);
    assert_eq!(got.snapshots[0].cells[0].lanes[COUNT], i64::MAX);
    // The other four reducers have no overflow to guard.
    assert_eq!(combine_aggregated(LaneReducer::Max, i64::MIN, i64::MAX), i64::MAX);
    assert_eq!(combine_aggregated(LaneReducer::Min, i64::MIN, i64::MAX), i64::MIN);
    assert_eq!(combine_aggregated(LaneReducer::First, 3, 9), 3);
    assert_eq!(combine_aggregated(LaneReducer::Last, 3, 9), 9);
}

// ------------------------------------------------------------------------------------------
// Idempotence, the cutoff and replay
// ------------------------------------------------------------------------------------------

#[test]
fn rolling_a_rolled_result_again_changes_no_byte_and_the_same_request_twice_agrees() {
    let snaps = series(11, 200);
    let last_ts = snaps.last().unwrap().timestamp_ms;
    let request = RollupSpec {
        older_than_ms: last_ts / 2,
        bucket_ms: 250,
        cap: Some(cap(Some("slot"), lane_score("shown"), 6)),
    };
    let once = rollup(&header(), snaps.clone(), &request).unwrap();
    let twice_from_scratch = rollup(&header(), snaps, &request).unwrap();
    assert_eq!(bytes_of(&once.snapshots), bytes_of(&twice_from_scratch.snapshots));

    let again = rollup(&header(), once.snapshots.clone(), &request).unwrap();
    assert_eq!(bytes_of(&again.snapshots), bytes_of(&once.snapshots), "a rolled bucket rolled again is a no-op");
    assert_eq!(again.stats.cells_combined, 0);
    assert_eq!(again.stats.cells_dropped, 0);
    assert_eq!(again.stats.snapshots_before, again.stats.snapshots_after);
    assert!(once.stats.cells_dropped > 0 && once.stats.cells_combined > 0, "the first run did something to be idempotent about");
}

#[test]
fn a_snapshot_exactly_at_the_cutoff_is_not_rolled_and_one_below_it_is() {
    // Bucket 50, cutoff 100. 99 is below: it moves to its bucket start, 50. 100 is AT the cutoff:
    // it stays at 100, and 101 stays at 101 (it is not merged into it).
    let snaps = vec![
        snap(99, vec![cell(1, 0, [1, 1, 1, 1, 1, 1])]),
        snap(100, vec![cell(1, 0, [2, 1, 2, 2, 2, 2])]),
        snap(101, vec![cell(1, 0, [3, 1, 3, 3, 3, 3])]),
    ];
    let got = rollup(&header(), snaps, &spec(100, 50)).unwrap();
    let stamps: Vec<u64> = got.snapshots.iter().map(|s| s.timestamp_ms).collect();
    assert_eq!(stamps, vec![50, 100, 101]);
    assert_eq!(got.snapshots[1].cells[0].lanes[SUM], 2);
    // One past: a cutoff of 101 rolls 100 too, into bucket 100, beside the untouched 101.
    let snaps = vec![snap(99, vec![]), snap(100, vec![cell(1, 0, [2, 1, 2, 2, 2, 2])]), snap(101, vec![])];
    let got = rollup(&header(), snaps, &spec(101, 50)).unwrap();
    assert_eq!(got.snapshots.iter().map(|s| s.timestamp_ms).collect::<Vec<_>>(), vec![50, 100, 101]);
    assert_eq!(got.snapshots[1].cells.len(), 1);
}

#[test]
fn a_late_snapshot_in_an_already_rolled_bucket_folds_in_as_the_newest_contribution() {
    let first = rollup(&header(), vec![snap(10, vec![cell(1, 0, [1, 1, 1, 1, 1, 5])])], &spec(100, 100)).unwrap();
    assert_eq!(first.snapshots[0].timestamp_ms, 0);
    let mut later = first.snapshots;
    later.push(snap(30, vec![cell(1, 0, [1, 1, 1, 1, 9, 9])]));
    let got = rollup(&header(), later, &spec(200, 100)).unwrap();
    let lanes = &got.snapshots[0].cells[0].lanes;
    assert_eq!((lanes[SUM], lanes[COUNT], lanes[FIRST], lanes[LAST]), (2, 2, 1, 9));
}

#[test]
fn a_decoded_replay_reaches_the_bytes_the_first_run_reached() {
    let snaps = series(3, 120);
    let stored: Vec<(u64, Vec<u8>)> = bytes_of(&snaps);
    let request = RollupSpec {
        older_than_ms: u64::MAX,
        bucket_ms: 400,
        cap: Some(cap(None, CellScore::Ratio { numerator: "shown".to_string(), denominator: "events".to_string() }, 20)),
    };
    let leader = rollup(&header(), snaps, &request).unwrap();
    // A follower or a replay starts from the stored bytes, not from the leader's values.
    let replayed: Vec<Snapshot> = stored
        .iter()
        .map(|(ts, bytes)| snap(*ts, decode_snapshot(&header(), bytes).unwrap()))
        .collect();
    let follower = rollup(&header(), replayed, &request).unwrap();
    assert_eq!(bytes_of(&leader.snapshots), bytes_of(&follower.snapshots));
    assert_eq!(leader.stats, follower.stats);
}

#[test]
fn nothing_to_roll_returns_the_snapshots_unchanged() {
    let empty = rollup(&header(), Vec::new(), &spec(100, 10)).unwrap();
    assert!(empty.snapshots.is_empty());
    assert_eq!(empty.stats, RollupStats::default());

    let snaps = vec![snap(500, vec![cell(1, 0, [1, 1, 1, 1, 1, 1])]), snap(700, vec![])];
    let got = rollup(&header(), snaps.clone(), &spec(500, 10)).unwrap();
    assert_eq!(got.snapshots, snaps, "cutoff at the oldest stamp rolls nothing");
    assert_eq!(got.stats.snapshots_before, 2);
    assert_eq!(got.stats.snapshots_after, 2);
}

// ------------------------------------------------------------------------------------------
// The cap
// ------------------------------------------------------------------------------------------

/// Slot 1: m1 shown 5, m2 shown 9, m3 shown 9, m4 shown 1.   Slot 2: m5 shown 3.
/// Slot 3: m6, m7, m8 all shown 7.
fn group_data() -> Vec<Cell> {
    vec![
        cell(1, 1, [5, 1, 0, 0, 0, 0]),
        cell(2, 1, [9, 1, 0, 0, 0, 0]),
        cell(3, 1, [9, 1, 0, 0, 0, 0]),
        cell(4, 1, [1, 1, 0, 0, 0, 0]),
        cell(5, 2, [3, 1, 0, 0, 0, 0]),
        cell(6, 3, [7, 1, 0, 0, 0, 0]),
        cell(7, 3, [7, 1, 0, 0, 0, 0]),
        cell(8, 3, [7, 1, 0, 0, 0, 0]),
    ]
}

fn members(cells: &[Cell]) -> Vec<u64> {
    cells.iter().map(|c| match c.member { Member::U64(v) => v, _ => unreachable!() }).collect()
}

#[test]
fn a_cap_keeps_the_best_of_each_group_breaks_ties_by_member_and_reports_the_drops() {
    // keep 2 per slot. Slot 1 scores 5, 9, 9, 1: m2 and m3 (a tie, both kept); m1 and m4 go.
    // Slot 2 has one cell: kept. Slot 3 scores 7, 7, 7: the tie breaks by member, so m6 and m7
    // stay and m8 goes. Dropped: m1, m4, m8 = 3.
    let mut cells = group_data();
    let dropped = cap_cells(&header(), &cap(Some("slot"), lane_score("shown"), 2), &mut cells).unwrap();
    assert_eq!(members(&cells), vec![2, 3, 5, 6, 7]);
    assert_eq!(dropped, 3);

    // keep 1 per slot makes a tie between m2 and m3 decide: the smaller member wins.
    let mut cells = group_data();
    let dropped = cap_cells(&header(), &cap(Some("slot"), lane_score("shown"), 1), &mut cells).unwrap();
    assert_eq!(members(&cells), vec![2, 5, 6]);
    assert_eq!(dropped, 5);

    // No group named: every cell is one group. Scores 9, 9, 7, 7, 7 lead: m2, m3, then the first
    // of the sevens by member, m6.
    let mut cells = group_data();
    let dropped = cap_cells(&header(), &cap(None, lane_score("shown"), 3), &mut cells).unwrap();
    assert_eq!(members(&cells), vec![2, 3, 6]);
    assert_eq!(dropped, 5);

    // A keep at least as large as every group drops nothing and changes nothing.
    let mut cells = group_data();
    assert_eq!(cap_cells(&header(), &cap(Some("slot"), lane_score("shown"), 4), &mut cells).unwrap(), 0);
    assert_eq!(cells, group_data());
}

#[test]
fn a_cell_whose_score_is_undefined_ranks_after_every_scored_cell() {
    // shown / events: m1 = 4/2 = 2, m2 has no events (undefined), m3 = 1/1 = 1. keep 2 keeps m1
    // and m3 although m2 has the smallest member after m1.
    let mut cells = vec![
        cell(1, 0, [4, 2, 0, 0, 0, 0]),
        cell(2, 0, [100, 0, 0, 0, 0, 0]),
        cell(3, 0, [1, 1, 0, 0, 0, 0]),
    ];
    let ratio = CellScore::Ratio { numerator: "shown".to_string(), denominator: "events".to_string() };
    assert_eq!(cap_cells(&header(), &cap(None, ratio, 2), &mut cells).unwrap(), 1);
    assert_eq!(members(&cells), vec![1, 3]);
}

#[test]
fn the_cap_runs_on_the_folded_bucket_not_on_the_snapshots_that_make_it() {
    // m1 shows 1 then 100; m2 shows 50 then 0. Folded: m1 = 101, m2 = 50, so keep 1 keeps m1.
    // Capping each snapshot first would have kept m2 in the first and m1 in the second.
    let snaps = vec![
        snap(1, vec![cell(1, 0, [1, 1, 0, 0, 0, 0]), cell(2, 0, [50, 1, 0, 0, 0, 0])]),
        snap(2, vec![cell(1, 0, [100, 1, 0, 0, 0, 0]), cell(2, 0, [0, 1, 0, 0, 0, 0])]),
    ];
    let request = RollupSpec { older_than_ms: 10, bucket_ms: 10, cap: Some(cap(None, lane_score("shown"), 1)) };
    let got = rollup(&header(), snaps, &request).unwrap();
    assert_eq!(got.snapshots.len(), 1);
    assert_eq!(got.snapshots[0].cells, vec![cell(1, 0, [101, 2, 0, 0, 0, 0])]);
    assert_eq!(got.stats.cells_dropped, 1);
    assert_eq!(got.stats.cells_combined, 2);
    assert_eq!(got.stats.cells_before, 4);
    assert_eq!(got.stats.cells_after, 1);
}

#[test]
fn a_snapshot_the_cap_does_not_reach_keeps_every_cell() {
    let snaps = vec![snap(5, group_data()), snap(500, group_data())];
    let request = RollupSpec { older_than_ms: 100, bucket_ms: 100, cap: Some(cap(Some("slot"), lane_score("shown"), 1)) };
    let got = rollup(&header(), snaps, &request).unwrap();
    assert_eq!(members(&got.snapshots[0].cells), vec![2, 5, 6]);
    assert_eq!(got.snapshots[1].cells, group_data(), "at the cutoff and after it: untouched, uncapped");
    assert_eq!(got.stats.cells_dropped, 5);
}

// ------------------------------------------------------------------------------------------
// Refusals and bounds
// ------------------------------------------------------------------------------------------

fn run_ok(spec: &RollupSpec) -> bool {
    rollup(&header(), vec![snap(1, vec![cell(1, 0, [1, 1, 1, 1, 1, 1])])], spec).is_ok()
}

#[test]
fn a_bucket_width_is_bounded_below_at_exactly_one() {
    assert!(run_ok(&spec(10, 1)), "a width of 1 is accepted");
    let err = rollup(&header(), Vec::new(), &spec(10, 0)).unwrap_err();
    assert!(err.contains("bucket_ms must be at least 1"), "{err}");
    assert!(validate_spec(&spec(10, 0)).is_err());
}

#[test]
fn keep_is_bounded_at_one_below_and_the_snapshot_limit_above() {
    let with_keep = |keep: usize| RollupSpec { older_than_ms: 10, bucket_ms: 10, cap: Some(cap(None, lane_score("shown"), keep)) };
    assert!(run_ok(&with_keep(1)));
    assert!(run_ok(&with_keep(MAX_KEEP)));
    let zero = validate_spec(&with_keep(0)).unwrap_err();
    assert!(zero.contains("at least 1"), "{zero}");
    let over = validate_spec(&with_keep(MAX_KEEP + 1)).unwrap_err();
    assert!(over.contains(&MAX_KEEP.to_string()) && over.contains("over the limit"), "{over}");
}

#[test]
fn snapshots_below_the_cutoff_are_bounded_at_exactly_the_limit() {
    let many = |n: usize| (0..n as u64).map(|i| snap(i + 1, Vec::new())).collect::<Vec<_>>();
    let at = rollup(&header(), many(MAX_ROLLUP_SNAPSHOTS), &spec(u64::MAX, 1_000_000)).unwrap();
    assert_eq!(at.snapshots.len(), 1, "all of them land in one bucket");
    let err = rollup(&header(), many(MAX_ROLLUP_SNAPSHOTS + 1), &spec(u64::MAX, 1_000_000)).unwrap_err();
    assert!(err.contains("over the limit") && err.contains(&MAX_ROLLUP_SNAPSHOTS.to_string()), "{err}");
    // The bound counts the snapshots BELOW the cutoff: one more, past the cutoff, is not touched.
    let mut past = many(MAX_ROLLUP_SNAPSHOTS);
    past.push(snap(MAX_ROLLUP_SNAPSHOTS as u64 + 1, Vec::new()));
    assert!(rollup(&header(), past, &spec(MAX_ROLLUP_SNAPSHOTS as u64 + 1, 1_000_000)).is_ok());
}

#[test]
fn a_bucket_over_the_snapshot_cell_limit_is_refused_and_one_at_the_limit_is_folded() {
    let range = |from: u64, to: u64| (from..to).map(|m| cell(m, 0, [1, 1, 1, 1, 1, 1])).collect::<Vec<_>>();
    let half = MAX_CELLS_PER_SNAPSHOT as u64 / 2;
    let full = MAX_CELLS_PER_SNAPSHOT as u64;
    let at = rollup(&header(), vec![snap(1, range(0, half)), snap(2, range(half, full))], &spec(10, 10)).unwrap();
    assert_eq!(at.snapshots[0].cells.len(), MAX_CELLS_PER_SNAPSHOT);
    let err = rollup(&header(), vec![snap(1, range(0, half)), snap(2, range(half, full + 1))], &spec(10, 10)).unwrap_err();
    assert!(err.contains("over the limit"), "{err}");
}

#[test]
fn a_request_the_key_cannot_serve_is_refused_naming_what_was_wrong() {
    let good = vec![snap(1, vec![cell(1, 0, [1, 1, 1, 1, 1, 1])])];
    let with = |c: CapSpec| RollupSpec { older_than_ms: 10, bucket_ms: 10, cap: Some(c) };

    let err = rollup(&header(), good.clone(), &with(cap(Some("nope"), lane_score("shown"), 1))).unwrap_err();
    assert!(err.contains("`nope`") && err.contains("slot"), "{err}");
    let err = rollup(&header(), good.clone(), &with(cap(None, lane_score("nope"), 1))).unwrap_err();
    assert!(err.contains("unknown name `nope`"), "{err}");
    let err = rollup(&header(), good.clone(), &with(cap(None, lane_score("slot"), 1))).unwrap_err();
    assert!(err.contains("not a lane"), "a label is not a lane: {err}");
    let err = rollup(&header(), good.clone(), &with(cap(None, CellScore::Time, 1))).unwrap_err();
    assert!(err.contains("time"), "{err}");
    let wilson = CellScore::Wilson { successes: "shown".to_string(), trials: "events".to_string(), z: f64::NAN };
    assert!(rollup(&header(), good.clone(), &with(cap(None, wilson, 1))).is_err());

    let two = vec![snap(2, Vec::new()), snap(1, Vec::new())];
    assert!(rollup(&header(), two, &spec(10, 10)).unwrap_err().contains("ascending stamp"));
    let same = vec![snap(1, Vec::new()), snap(1, Vec::new())];
    assert!(rollup(&header(), same, &spec(10, 10)).is_err());
    let unordered = vec![snap(1, vec![cell(2, 0, [1; 6]), cell(1, 0, [1; 6])])];
    assert!(rollup(&header(), unordered, &spec(10, 10)).unwrap_err().contains("strictly ascending by member"));
    let short = vec![snap(1, vec![Cell { member: Member::U64(1), labels: vec![0], lanes: vec![1] }])];
    assert!(rollup(&header(), short, &spec(10, 10)).unwrap_err().contains("1 lanes"));
    // A snapshot past the cutoff is returned as given: it is not inspected.
    let later = vec![snap(50, vec![cell(2, 0, [1; 6]), cell(1, 0, [1; 6])])];
    assert!(rollup(&header(), later, &spec(10, 10)).is_ok());
}

#[test]
fn a_bytes_member_cannot_be_scored_by_member_and_is_otherwise_rolled_like_any_other() {
    let bytes = |label: &[u8], lanes: [i64; 6]| Cell { member: Member::Bytes(label.to_vec()), labels: vec![0], lanes: lanes.to_vec() };
    let snaps = vec![snap(1, vec![cell(1, 0, [1; 6]), bytes(b"zz", [2; 6])]), snap(2, vec![bytes(b"zz", [3; 6])])];
    let got = rollup(&header(), snaps.clone(), &spec(10, 10)).unwrap();
    assert_eq!(got.snapshots[0].cells.len(), 2);
    assert_eq!(got.snapshots[0].cells[1].lanes[SUM], 5);
    let request = RollupSpec { older_than_ms: 10, bucket_ms: 10, cap: Some(cap(None, CellScore::Member, 1)) };
    let err = rollup(&header(), snaps, &request).unwrap_err();
    assert!(err.contains("u64 members"), "{err}");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    /// `snapshots` snapshots of `members` cells, every member in every snapshot, all in one bucket.
    fn dataset(members: u64, snapshots: u64) -> Vec<Snapshot> {
        (1..=snapshots)
            .map(|ts| {
                let cells = (0..members)
                    .map(|m| cell(m, m % 7, [(m * 31 + ts) as i64 % 1000, 1, 3, 4, 5, ts as i64]))
                    .collect();
                snap(ts, cells)
            })
            .collect()
    }

    fn allocations_of_a_rollup(members: u64, snapshots: u64, capped: bool) -> u64 {
        let snaps = dataset(members, snapshots);
        let request = RollupSpec {
            older_than_ms: u64::MAX,
            bucket_ms: 1 << 40,
            cap: capped.then(|| cap(Some("slot"), lane_score("shown"), 5)),
        };
        let header = header();
        let probe = Probe::start();
        let got = rollup(&header, snaps, &request).unwrap();
        let spent = probe.stop().allocs;
        assert_eq!(got.snapshots.len(), 1);
        assert_eq!(got.snapshots[0].cells.len(), if capped { 35.min(members as usize) } else { members as usize });
        spent
    }

    /// What an obvious implementation does: clone every cell into a table keyed by member.
    fn allocations_of_a_cloning_table(members: u64, snapshots: u64) -> u64 {
        let snaps = dataset(members, snapshots);
        let probe = Probe::start();
        let mut table: BTreeMap<Member, Vec<Cell>> = BTreeMap::new();
        for s in &snaps {
            for c in &s.cells {
                table.entry(c.member.clone()).or_default().push(c.clone());
            }
        }
        let spent = probe.stop().allocs;
        assert_eq!(table.len(), members as usize);
        spent
    }

    /// The allocations of a rollup do not move with the number of snapshots in a bucket or with
    /// the number of members. THE CONTROL: the table an obvious implementation builds allocates in
    /// proportion to the members and to the snapshots, so the probe can see both dimensions.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_rollup_allocates_the_same_over_few_snapshots_and_many_and_few_members_and_many() {
        for capped in [false, true] {
            let base = allocations_of_a_rollup(50, 4, capped);
            let more_snapshots = allocations_of_a_rollup(50, 64, capped);
            let more_members = allocations_of_a_rollup(4000, 4, capped);
            let both = allocations_of_a_rollup(4000, 64, capped);
            println!("capped {capped}: 50x4 {base}, 50x64 {more_snapshots}, 4000x4 {more_members}, 4000x64 {both}");
            assert!(base > 0, "the probe saw no allocation at all, so it is not installed");
            // Measured: 3 uncapped (the result vector, the fold buffer, the rest split) and 5
            // capped (the two cap buffers as well).
            assert!(base <= if capped { 6 } else { 4 }, "a rollup spent {base} allocations (capped {capped})");
            assert_eq!(base, more_snapshots, "allocations moved with the snapshots in a bucket (capped {capped})");
            assert_eq!(base, more_members, "allocations moved with the members (capped {capped})");
            assert_eq!(base, both, "capped {capped}");
        }
        let small = allocations_of_a_cloning_table(50, 4);
        let wide = allocations_of_a_cloning_table(4000, 4);
        let deep = allocations_of_a_cloning_table(50, 64);
        println!("table: 50x4 {small}, 4000x4 {wide}, 50x64 {deep}");
        assert!(wide > small + 5000, "CONTROL: the table did not grow with the members ({small} -> {wide})");
        assert!(deep > small, "CONTROL: the table did not grow with the snapshots ({small} -> {deep})");
    }
}
