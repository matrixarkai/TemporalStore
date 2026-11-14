// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the rollup extension: tiers, the anchor, truncation and the cap controls. Every
//! expected number is worked out by hand in a comment beside it, or comes from a fold written
//! separately in this file. The single-tier request is tested in `cell_rollup_tests`, and still
//! runs through the same code: a request that names none of the new controls rolls exactly as it
//! did.

use super::*;
use crate::engine::cell_set::{
    encode_snapshot, DimSpec, DimType, EntityType, KeySpec, LaneSpec, Member, HEADER_VERSION,
};
use std::collections::BTreeMap;

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

/// A cell whose `shown` lane is `shown` and whose other lanes are 1.
fn shown(member: u64, slot: u64, shown: i64) -> Cell {
    cell(member, slot, [shown, 1, 1, 1, 1, 1])
}

fn snap(timestamp_ms: u64, cells: Vec<Cell>) -> Snapshot {
    Snapshot { timestamp_ms, cells }
}

fn lane_score(name: &str) -> CellScore {
    CellScore::Lane { lane: name.to_string() }
}

fn tier(older_than: u64, bucket_ms: u64) -> TierSpec {
    TierSpec { older_than, bucket_ms }
}

fn plan(anchor: Anchor, tiers: &[(u64, u64)]) -> PlanSpec {
    PlanSpec {
        anchor,
        tiers: tiers.iter().map(|&(older_than, bucket_ms)| tier(older_than, bucket_ms)).collect(),
        truncate: None,
        cap: None,
    }
}

fn with_cap(mut spec: PlanSpec, group_by: Option<&str>, keep: usize, ppm: u32, over: usize) -> PlanSpec {
    spec.cap = Some(CapPlanSpec {
        group_by: group_by.map(str::to_string),
        score: lane_score("shown"),
        keep,
        protect_newest_ppm: ppm,
        trigger_over: over,
    });
    spec
}

fn bytes_of(snapshots: &[Snapshot]) -> Vec<(u64, Vec<u8>)> {
    snapshots.iter().map(|s| (s.timestamp_ms, encode_snapshot(&header(), &s.cells).unwrap())).collect()
}

fn members(cells: &[Cell]) -> Vec<u64> {
    cells.iter().map(|c| match c.member { Member::U64(v) => v, _ => unreachable!() }).collect()
}

// ------------------------------------------------------------------------------------------
// An independent fold
// ------------------------------------------------------------------------------------------

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

/// Snapshots at irregular stamps over members 0..60, each present about half the time, with small
/// lane values so no sum can approach a limit.
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

/// What a tiered rollup must produce, written without the module's fold. Each snapshot below the
/// first cutoff takes the bucket width of the tier with the SMALLEST cutoff above its stamp; the
/// occurrences of each member of each bucket are gathered in stamp order and each lane is computed
/// from its own list.
fn independent_fold(snaps: &[Snapshot], given: &[(u64, u64)]) -> Vec<Snapshot> {
    // A coarser tier's cutoff is aligned down to the finer tier's width; the first is as given.
    let cutoffs: Vec<(u64, u64)> = given
        .iter()
        .enumerate()
        .map(|(i, &(cutoff, width))| (if i == 0 { cutoff } else { cutoff - cutoff % given[i - 1].1 }, width))
        .collect();
    let first_cutoff = cutoffs[0].0;
    let mut buckets: BTreeMap<u64, BTreeMap<u64, Vec<(Vec<u64>, Vec<i64>)>>> = BTreeMap::new();
    let mut untouched = Vec::new();
    for s in snaps {
        if s.timestamp_ms >= first_cutoff {
            untouched.push(s.clone());
            continue;
        }
        let width = cutoffs
            .iter()
            .filter(|(cutoff, _)| s.timestamp_ms < *cutoff)
            .min_by_key(|(cutoff, _)| *cutoff)
            .map(|(_, width)| *width)
            .unwrap();
        let start = (s.timestamp_ms / width) * width;
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
fn a_tiered_rollup_equals_an_independent_fold_with_a_bucket_per_tier() {
    let snaps = series(5, 300);
    let newest = snaps.last().unwrap().timestamp_ms;
    // Request anchors: cutoffs fall down the list, each bucket a multiple of the one before.
    // Newest anchor: ages grow down the list, resolved against the newest stamp.
    let cases: Vec<(PlanSpec, Vec<(u64, u64)>)> = vec![
        (plan(Anchor::Request, &[(5000, 100), (3000, 500), (1000, 1000)]), vec![(5000, 100), (3000, 500), (1000, 1000)]),
        (plan(Anchor::Request, &[(6000, 50), (4000, 200), (2000, CATCH_ALL_BUCKET_MS)]), vec![(6000, 50), (4000, 200), (2000, CATCH_ALL_BUCKET_MS)]),
        (plan(Anchor::Request, &[(10_000_000, 10)]), vec![(10_000_000, 10)]),
        (
            plan(Anchor::NewestMinusAge, &[(500, 100), (2000, 1000), (4000, 4000)]),
            vec![(newest - 500, 100), (newest - 2000, 1000), (newest - 4000, 4000)],
        ),
        (plan(Anchor::Request, &[(u64::MAX, 7), (3000, 14), (1500, 98)]), vec![(u64::MAX, 7), (3000, 14), (1500, 98)]),
    ];
    let (mut compared_cells, mut folded_runs) = (0usize, 0usize);
    for (spec, cutoffs) in &cases {
        let want = independent_fold(&snaps, cutoffs);
        let got = rollup_tiered(&header(), snaps.clone(), spec, newest).unwrap();
        assert_eq!(got.snapshots, want, "{spec:?}");
        let s = got.stats;
        assert_eq!(s.snapshots_after, want.len() as u64);
        assert_eq!(s.cells_before, snaps.iter().map(|x| x.cells.len() as u64).sum::<u64>());
        assert_eq!(s.cells_before, s.cells_after + s.cells_combined + s.cells_dropped);
        compared_cells += want.iter().map(|x| x.cells.len()).sum::<usize>();
        folded_runs += (s.snapshots_after < s.snapshots_before) as usize;
    }
    assert!(compared_cells > 5_000, "only {compared_cells} cells were compared");
    assert_eq!(folded_runs, cases.len(), "every case folded something");
}

// ------------------------------------------------------------------------------------------
// Tiers
// ------------------------------------------------------------------------------------------

#[test]
fn a_snapshot_takes_the_coarsest_tier_whose_cutoff_is_above_it_and_each_cutoff_is_strict() {
    // Tiers: below 1000 buckets of 100; below 500 buckets of 500.
    //   499 is below both: the coarser tier, start 0.   500 is AT the second cutoff: tier 0, start 500.
    //   999 is below only the first cutoff: start 900.   1000 is AT the first cutoff: not rolled.
    let spec = plan(Anchor::Request, &[(1000, 100), (500, 500)]);
    let snaps = vec![
        snap(499, vec![shown(1, 0, 1)]),
        snap(500, vec![shown(1, 0, 2)]),
        snap(999, vec![shown(1, 0, 3)]),
        snap(1000, vec![shown(1, 0, 4)]),
    ];
    let got = rollup_tiered(&header(), snaps, &spec, 0).unwrap();
    let stamps: Vec<u64> = got.snapshots.iter().map(|s| s.timestamp_ms).collect();
    assert_eq!(stamps, vec![0, 500, 900, 1000]);
    assert_eq!(got.snapshots.iter().map(|s| s.cells[0].lanes[SUM]).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    assert_eq!(got.stats.cells_combined, 0);
}

#[test]
fn a_cutoff_that_is_not_a_multiple_of_the_finer_width_is_aligned_down_to_it() {
    // The second cutoff, 520, is aligned down to 500, a multiple of the first tier's width of 100.
    // 490 is below it (buckets of 500, start 0); 510 and 530 are not: they are tier 0 (buckets of
    // 100) and both start at 500, so they fold, oldest first: Sum 10 + 20, Count 2, Max 2, Min 1,
    // First 1 (the older), Last 2 (the newer). Aligned, a bucket never straddles the boundary.
    let spec = plan(Anchor::Request, &[(1000, 100), (520, 500)]);
    assert_eq!(resolve_tiers(&spec, 0), vec![(1000, 100), (500, 500)]);
    let snaps = vec![
        snap(490, vec![shown(9, 0, 5)]),
        snap(510, vec![cell(1, 0, [10, 1, 1, 1, 1, 1])]),
        snap(530, vec![cell(1, 0, [20, 1, 2, 2, 2, 2])]),
    ];
    let got = rollup_tiered(&header(), snaps.clone(), &spec, 0).unwrap();
    assert_eq!(
        got.snapshots,
        vec![snap(0, vec![shown(9, 0, 5)]), snap(500, vec![cell(1, 0, [30, 2, 2, 1, 1, 2])])]
    );
    // The same request with the cutoff written already aligned is the same request.
    assert_eq!(rollup_tiered(&header(), snaps, &plan(Anchor::Request, &[(1000, 100), (500, 500)]), 0).unwrap(), got);
    // Aligned down, never up: a cutoff already on the grid is unchanged, one just under it moves a width.
    assert_eq!(resolve_tiers(&plan(Anchor::Request, &[(1000, 100), (499, 500)]), 0)[1].0, 400);
    assert_eq!(resolve_tiers(&plan(Anchor::NewestMinusAge, &[(10, 100), (1234, 500)]), 5000)[1].0, 3700);
}

#[test]
fn the_catch_all_tier_holds_everything_older_in_one_snapshot_at_stamp_zero() {
    // Below 1000 buckets of 100; below 400 everything in one snapshot. 5, 150 and 399 are below
    // 400 and fold into stamp 0; 400 and 450 are tier 0 and fold into 400; 1200 is not rolled.
    let spec = plan(Anchor::Request, &[(1000, 100), (400, CATCH_ALL_BUCKET_MS)]);
    let snaps = vec![
        snap(5, vec![shown(1, 0, 1)]),
        snap(150, vec![shown(1, 0, 2), shown(2, 0, 7)]),
        snap(399, vec![shown(1, 0, 4)]),
        snap(400, vec![shown(1, 0, 8)]),
        snap(450, vec![shown(1, 0, 16)]),
        snap(1200, vec![shown(1, 0, 32)]),
    ];
    let got = rollup_tiered(&header(), snaps, &spec, 0).unwrap();
    assert_eq!(
        got.snapshots,
        vec![
            snap(0, vec![cell(1, 0, [7, 3, 1, 1, 1, 1]), cell(2, 0, [7, 1, 1, 1, 1, 1])]),
            snap(400, vec![cell(1, 0, [24, 2, 1, 1, 1, 1])]),
            snap(1200, vec![shown(1, 0, 32)]),
        ]
    );
}

// ------------------------------------------------------------------------------------------
// The anchor
// ------------------------------------------------------------------------------------------

#[test]
fn an_age_resolves_against_the_newest_stamp_and_a_request_cutoff_ignores_it() {
    let snaps: Vec<Snapshot> = (0..=40u64).map(|i| snap(i * 250, vec![shown(1, 0, i as i64)])).collect();
    // Ages (1000, 5000) against newest 10 000 are the cutoffs (9000, 5000).
    let by_age = plan(Anchor::NewestMinusAge, &[(1000, 100), (5000, 1000)]);
    let by_stamp = plan(Anchor::Request, &[(9000, 100), (5000, 1000)]);
    let want = rollup_tiered(&header(), snaps.clone(), &by_stamp, 0).unwrap();
    assert_eq!(rollup_tiered(&header(), snaps.clone(), &by_age, 10_000).unwrap(), want);
    assert!(want.stats.snapshots_after < want.stats.snapshots_before, "DENOMINATOR: it rolled something");
    // The same ages against another newest stamp are other cutoffs.
    let later = rollup_tiered(&header(), snaps.clone(), &by_age, 10_500).unwrap();
    assert_ne!(later, want);
    assert_eq!(later, rollup_tiered(&header(), snaps.clone(), &plan(Anchor::Request, &[(9500, 100), (5500, 1000)]), 0).unwrap());
    // A request cutoff does not read the newest stamp.
    assert_eq!(rollup_tiered(&header(), snaps.clone(), &by_stamp, 99_999).unwrap(), want);
    // An age longer than the series is old saturates at cutoff 0: nothing is below it.
    let too_old = plan(Anchor::NewestMinusAge, &[(20_000, 100)]);
    let untouched = rollup_tiered(&header(), snaps.clone(), &too_old, 10_000).unwrap();
    assert_eq!(untouched.snapshots, snaps);
}

// ------------------------------------------------------------------------------------------
// Idempotence
// ------------------------------------------------------------------------------------------

#[test]
fn a_tiered_capped_result_rolled_again_changes_no_byte() {
    let snaps = series(9, 200);
    let newest = snaps.last().unwrap().timestamp_ms;
    for anchor in [Anchor::Request, Anchor::NewestMinusAge] {
        let tiers: &[(u64, u64)] = match anchor {
            Anchor::Request => &[(newest - 500, 200), (newest / 2, 1000)],
            Anchor::NewestMinusAge => &[(500, 200), (newest / 2, 1000)],
        };
        let spec = with_cap(plan(anchor, tiers), Some("slot"), 3, 300_000, 0);
        let once = rollup_tiered(&header(), snaps.clone(), &spec, newest).unwrap();
        assert!(once.stats.cells_dropped > 0 && once.stats.cells_combined > 0, "{anchor:?} did something to be idempotent about");
        let again = rollup_tiered(&header(), once.snapshots.clone(), &spec, newest).unwrap();
        assert_eq!(bytes_of(&again.snapshots), bytes_of(&once.snapshots), "{anchor:?}");
        assert_eq!((again.stats.cells_combined, again.stats.cells_dropped), (0, 0));
        assert_eq!(again.stats.snapshots_before, again.stats.snapshots_after);
    }
}

#[test]
fn a_request_that_names_every_new_control_at_its_default_is_the_single_tier_request() {
    let snaps = series(3, 150);
    let legacy = RollupSpec {
        older_than_ms: 2000,
        bucket_ms: 300,
        cap: Some(CapSpec { group_by: Some("slot".to_string()), score: lane_score("shown"), keep: 2 }),
    };
    let spelled_out = PlanSpec {
        anchor: Anchor::Request,
        tiers: vec![tier(2000, 300)],
        truncate: None,
        cap: Some(CapPlanSpec {
            group_by: Some("slot".to_string()),
            score: lane_score("shown"),
            keep: 2,
            protect_newest_ppm: 0,
            trigger_over: 0,
        }),
    };
    assert_eq!(PlanSpec::from_legacy(&legacy), spelled_out);
    let old = rollup(&header(), snaps.clone(), &legacy).unwrap();
    assert!(old.stats.cells_dropped > 0);
    assert_eq!(rollup_tiered(&header(), snaps, &spelled_out, 123).unwrap(), old);
}

// ------------------------------------------------------------------------------------------
// Truncation
// ------------------------------------------------------------------------------------------

#[test]
fn a_truncation_acts_only_over_its_trigger_and_then_keeps_the_newest_n() {
    let t = |keep_last, trigger| TruncateSpec { keep_last, trigger };
    // (snapshots held, keep_last, trigger) -> how many of the oldest go
    for (total, keep, trigger, want) in [
        (10, 5, None, 5),       // no trigger: acts above keep_last
        (5, 5, None, 0),        // exactly keep_last: nothing to do
        (6, 5, None, 1),
        (3, 5, None, 0),        // fewer than keep_last
        (10, 5, Some(10), 0),   // AT the trigger: not over it
        (11, 5, Some(10), 6),   // one over: down to keep_last, not to the trigger
        (10, 5, Some(5), 5),
        (0, 1, Some(1), 0),
    ] {
        assert_eq!(truncation_drop(total, &t(keep, trigger)), want, "{total} held, keep {keep}, trigger {trigger:?}");
    }
}

#[test]
fn truncating_a_rolled_series_keeps_the_newest_snapshots_of_what_the_rollup_left() {
    let snaps = series(11, 120);
    let newest = snaps.last().unwrap().timestamp_ms;
    let spec = plan(Anchor::Request, &[(newest / 2, 400)]);
    let rolled = rollup_tiered(&header(), snaps, &spec, newest).unwrap();
    let stamps: Vec<u64> = rolled.snapshots.iter().map(|s| s.timestamp_ms).collect();
    let total = stamps.len();
    assert!(total > 10, "{total}");
    // Over a trigger one short of the count: acts, keeps the newest 7.
    let acts = TruncateSpec { keep_last: 7, trigger: Some(total - 1) };
    let drop = truncation_drop(total, &acts);
    assert_eq!(drop, total - 7);
    assert_eq!(&stamps[drop..], &stamps[total - 7..]);
    // AT the trigger it leaves the series alone.
    let at = TruncateSpec { keep_last: 7, trigger: Some(total) };
    assert_eq!(truncation_drop(total, &at), 0);
}

// ------------------------------------------------------------------------------------------
// Cap controls
// ------------------------------------------------------------------------------------------

/// Six members in one bucket. Shown, by member: 1 -> 100, 2 -> 90, 3 -> 80, 4 -> 70, then 5 and 6
/// are written again in a later snapshot of the bucket and finish at 61 and 51. By score the top
/// four are members 1 to 4; by recency members 5 and 6 are the newest (stamp 20), the others 10.
fn recency_snapshots() -> Vec<Snapshot> {
    vec![
        snap(10, (1..=6u64).map(|m| shown(m, 0, 110 - 10 * m as i64)).collect()),
        snap(20, vec![shown(5, 0, 1), shown(6, 0, 1)]),
    ]
}

#[test]
fn a_protected_share_of_the_newest_members_survives_whatever_its_score() {
    let roll = |ppm: u32| {
        let spec = with_cap(plan(Anchor::Request, &[(1000, 100)]), None, 4, ppm, 0);
        rollup_tiered(&header(), recency_snapshots(), &spec, 0).unwrap()
    };
    // Shown after the fold: m1 100, m2 90, m3 80, m4 70, m5 61, m6 51.
    // No protection: the four best by score.
    let none = roll(0);
    assert_eq!(members(&none.snapshots[0].cells), vec![1, 2, 3, 4]);
    assert_eq!(none.stats.cells_dropped, 2);
    // 500 000 ppm of keep 4 is 2: the two newest (5 and 6) are kept, then the best two of the rest
    // (1 and 2) fill the other slots. 3 and 4 go although they outscore 5 and 6.
    let half = roll(500_000);
    assert_eq!(members(&half.snapshots[0].cells), vec![1, 2, 5, 6]);
    assert_eq!(half.stats.cells_dropped, 2);
    // 300 000 ppm of 4 is 1.2, rounded DOWN to 1: the newest by stamp, ties by the smaller member,
    // is m5; then the best three of the rest.
    assert_eq!(members(&roll(300_000).snapshots[0].cells), vec![1, 2, 3, 5]);
    // The whole of keep protected: newest first (5, 6), then the stamp-10 ties by member (1, 2).
    assert_eq!(members(&roll(1_000_000).snapshots[0].cells), vec![1, 2, 5, 6]);
    // The protected members' lanes are the folded ones.
    assert_eq!(half.snapshots[0].cells[2], cell(5, 0, [61, 2, 1, 1, 1, 1]));
}

#[test]
fn a_member_is_as_new_as_the_newest_snapshot_that_wrote_it() {
    // m3 is written at 10 and again at 20, so it is as new as m5 and m6; m1 and m2 only at 10.
    // Protecting two of keep 3 (666 666 ppm of 3 is 1.99, rounded down to 1) keeps one: the newest
    // of the three at stamp 20 is the smallest member, m3.
    let snaps = vec![
        snap(10, vec![shown(1, 0, 100), shown(2, 0, 90), shown(3, 0, 1), shown(5, 0, 1), shown(6, 0, 1)]),
        snap(20, vec![shown(3, 0, 1), shown(5, 0, 1), shown(6, 0, 1)]),
    ];
    let spec = with_cap(plan(Anchor::Request, &[(1000, 100)]), None, 3, 666_666, 0);
    let got = rollup_tiered(&header(), snaps.clone(), &spec, 0).unwrap();
    // m3 protected; then m1 and m2 by score.
    assert_eq!(members(&got.snapshots[0].cells), vec![1, 2, 3]);
    // With 999 999 ppm of 3 (2.99, rounded down to 2) m3 and m5 are protected, then m1.
    let spec = with_cap(plan(Anchor::Request, &[(1000, 100)]), None, 3, 999_999, 0);
    let got = rollup_tiered(&header(), snaps, &spec, 0).unwrap();
    assert_eq!(members(&got.snapshots[0].cells), vec![1, 3, 5]);
}

#[test]
fn a_group_is_cut_only_when_it_is_over_its_keep_plus_the_trigger_and_then_down_to_keep() {
    // Slot 0 holds 5 members and slot 1 holds 6; keep 3 with trigger_over 2. Slot 0 (5 <= 5) is
    // left alone, slot 1 (6 > 5) is cut to its best 3.
    let cells: Vec<Cell> = (1..=5u64)
        .map(|m| shown(m, 0, m as i64))
        .chain((6..=11u64).map(|m| shown(m, 1, m as i64)))
        .collect();
    let spec = with_cap(plan(Anchor::Request, &[(1000, 100)]), Some("slot"), 3, 0, 2);
    let got = rollup_tiered(&header(), vec![snap(10, cells.clone())], &spec, 0).unwrap();
    assert_eq!(members(&got.snapshots[0].cells), vec![1, 2, 3, 4, 5, 9, 10, 11]);
    assert_eq!(got.stats.cells_dropped, 3);
    // Without the trigger both groups are cut to 3.
    let spec = with_cap(plan(Anchor::Request, &[(1000, 100)]), Some("slot"), 3, 0, 0);
    let got = rollup_tiered(&header(), vec![snap(10, cells)], &spec, 0).unwrap();
    assert_eq!(members(&got.snapshots[0].cells), vec![3, 4, 5, 9, 10, 11]);
    assert_eq!(got.stats.cells_dropped, 5);
    // One group of no label: 6 members, keep 3, trigger 2: cut; 5 members: left alone.
    let flat = |n: u64| (1..=n).map(|m| shown(m, 0, m as i64)).collect::<Vec<_>>();
    let spec = with_cap(plan(Anchor::Request, &[(1000, 100)]), None, 3, 0, 2);
    assert_eq!(rollup_tiered(&header(), vec![snap(10, flat(5))], &spec, 0).unwrap().stats.cells_dropped, 0);
    assert_eq!(rollup_tiered(&header(), vec![snap(10, flat(6))], &spec, 0).unwrap().stats.cells_dropped, 3);
}

// ------------------------------------------------------------------------------------------
// Refusals and bounds
// ------------------------------------------------------------------------------------------

fn refuses(spec: &PlanSpec, needle: &str) {
    let err = validate_plan(spec).unwrap_err();
    assert!(err.contains(needle), "`{err}` does not name `{needle}`");
}

/// `n` tiers, each bucket twice the one before and each cutoff 1000 lower: valid.
fn ladder(n: usize) -> PlanSpec {
    plan(Anchor::Request, &(0..n).map(|i| (1_000_000 - 1000 * i as u64, 1u64 << i)).collect::<Vec<_>>())
}

#[test]
fn the_tier_count_the_order_and_the_widths_are_bounded_at_exactly_the_limit() {
    // A first tier and eight coarser ones are accepted; nine coarser are refused.
    assert!(validate_plan(&ladder(1 + MAX_COARSER_TIERS)).is_ok());
    refuses(&ladder(2 + MAX_COARSER_TIERS), "coarser tiers");
    refuses(&PlanSpec { tiers: Vec::new(), ..ladder(1) }, "at least one tier");

    // Widths: 1 is accepted in every tier, 0 is refused naming the tier.
    assert!(validate_plan(&plan(Anchor::Request, &[(1000, 1), (500, 1)])).is_ok());
    refuses(&plan(Anchor::Request, &[(1000, 0)]), "bucket_ms must be at least 1");
    refuses(&plan(Anchor::Request, &[(1000, 1), (500, 0)]), "tier 1 bucket_ms must be at least 1");

    // Order: request cutoffs must fall, ages must grow, strictly.
    assert!(validate_plan(&plan(Anchor::Request, &[(1000, 1), (999, 1)])).is_ok());
    refuses(&plan(Anchor::Request, &[(1000, 1), (1000, 1)]), "further back");
    refuses(&plan(Anchor::Request, &[(1000, 1), (1001, 1)]), "further back");
    assert!(validate_plan(&plan(Anchor::NewestMinusAge, &[(1000, 1), (1001, 1)])).is_ok());
    refuses(&plan(Anchor::NewestMinusAge, &[(1000, 1), (1000, 1)]), "further back");
    refuses(&plan(Anchor::NewestMinusAge, &[(1000, 1), (999, 1)]), "further back");

    // A coarser width is a multiple of the finer, or the catch-all and last.
    assert!(validate_plan(&plan(Anchor::Request, &[(1000, 100), (500, 300)])).is_ok());
    refuses(&plan(Anchor::Request, &[(1000, 100), (500, 150)]), "not a multiple");
    assert!(validate_plan(&plan(Anchor::Request, &[(1000, 100), (500, CATCH_ALL_BUCKET_MS)])).is_ok());
    assert!(validate_plan(&plan(Anchor::Request, &[(1000, CATCH_ALL_BUCKET_MS)])).is_ok());
    refuses(
        &plan(Anchor::Request, &[(1000, 100), (500, CATCH_ALL_BUCKET_MS), (400, CATCH_ALL_BUCKET_MS)]),
        "only the last tier",
    );
}

#[test]
fn truncation_and_cap_controls_are_bounded_at_exactly_the_limit() {
    let truncated = |keep_last, trigger| PlanSpec { truncate: Some(TruncateSpec { keep_last, trigger }), ..ladder(1) };
    assert!(validate_plan(&truncated(1, None)).is_ok());
    assert!(validate_plan(&truncated(MAX_TRUNCATE_KEEP, None)).is_ok());
    refuses(&truncated(0, None), "at least 1");
    refuses(&truncated(MAX_TRUNCATE_KEEP + 1, None), "over the limit");
    // The trigger: AT keep_last is accepted, below it is refused, and it has its own ceiling.
    assert!(validate_plan(&truncated(5, Some(5))).is_ok());
    refuses(&truncated(5, Some(4)), "below keep_last");
    assert!(validate_plan(&truncated(1, Some(MAX_TRUNCATE_TRIGGER))).is_ok());
    refuses(&truncated(1, Some(MAX_TRUNCATE_TRIGGER + 1)), "over the limit");

    let capped = |ppm, over| with_cap(ladder(1), None, 1, ppm, over);
    assert!(validate_plan(&capped(0, 0)).is_ok());
    assert!(validate_plan(&capped(PPM, MAX_TRIGGER_OVER)).is_ok());
    refuses(&capped(PPM + 1, 0), "protect_newest_ppm");
    refuses(&capped(0, MAX_TRIGGER_OVER + 1), "trigger_over");
}

#[test]
fn snapshots_below_the_first_cutoff_are_bounded_at_exactly_the_limit_whatever_the_tiers() {
    let many = |n: usize| (0..n as u64).map(|i| snap(i + 1, Vec::new())).collect::<Vec<_>>();
    let spec = plan(Anchor::Request, &[(u64::MAX, 1_000_000), (u64::MAX - 1, 4_000_000)]);
    assert!(rollup_tiered(&header(), many(MAX_ROLLUP_SNAPSHOTS), &spec, 0).is_ok());
    let err = rollup_tiered(&header(), many(MAX_ROLLUP_SNAPSHOTS + 1), &spec, 0).unwrap_err();
    assert!(err.contains("over the limit") && err.contains(&MAX_ROLLUP_SNAPSHOTS.to_string()), "{err}");
    // Under the newest anchor the first cutoff is newest minus the age, and it is what is counted.
    let aged = plan(Anchor::NewestMinusAge, &[(10, 1_000_000)]);
    assert!(rollup_tiered(&header(), many(MAX_ROLLUP_SNAPSHOTS + 1), &aged, MAX_ROLLUP_SNAPSHOTS as u64 + 10).is_ok());
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    /// `snapshots` snapshots of `members` cells, every member in every snapshot, all stamped below
    /// both cutoffs so they fall into the coarse tier's one bucket.
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

    fn allocations(members: u64, snapshots: u64, protect: bool) -> u64 {
        let snaps = dataset(members, snapshots);
        let base = plan(Anchor::Request, &[(1_000_000, 1 << 20), (500_000, 1 << 40)]);
        let spec = if protect { with_cap(base, Some("slot"), 5, 400_000, 1) } else { base };
        let header = header();
        let probe = Probe::start();
        let got = rollup_tiered(&header, snaps, &spec, 0).unwrap();
        let spent = probe.stop().allocs;
        assert_eq!(got.snapshots.len(), 1);
        spent
    }

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

    /// A tiered rollup, with the recency buffers a protected share needs, allocates the same over 4
    /// and 64 snapshots in a bucket and over 50 and 4,000 members. THE CONTROL: the table of cloned
    /// cells grows with both.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_tiered_rollup_allocates_the_same_over_few_snapshots_and_many_and_few_members_and_many() {
        for protect in [false, true] {
            let base = allocations(50, 4, protect);
            let more_snapshots = allocations(50, 64, protect);
            let more_members = allocations(4000, 4, protect);
            let both = allocations(4000, 64, protect);
            println!("protect {protect}: 50x4 {base}, 50x64 {more_snapshots}, 4000x4 {more_members}, 4000x64 {both}");
            assert!(base > 0, "the probe saw no allocation at all, so it is not installed");
            assert_eq!(base, more_snapshots, "allocations moved with the snapshots in a bucket (protect {protect})");
            assert_eq!(base, more_members, "allocations moved with the members (protect {protect})");
            assert_eq!(base, both, "protect {protect}");
            // Measured: 3 without a cap and 9 with one that protects its newest members.
            assert!(base <= if protect { 10 } else { 4 }, "a tiered rollup spent {base} allocations (protect {protect})");
        }
        let small = allocations_of_a_cloning_table(50, 4);
        let wide = allocations_of_a_cloning_table(4000, 4);
        let deep = allocations_of_a_cloning_table(50, 64);
        assert!(wide > small + 5000, "CONTROL: the table did not grow with the members ({small} -> {wide})");
        assert!(deep > small, "CONTROL: the table did not grow with the snapshots ({small} -> {deep})");
    }
}
