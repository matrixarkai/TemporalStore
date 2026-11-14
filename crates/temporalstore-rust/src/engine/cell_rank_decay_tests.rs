// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the decay score of the ranked cell read. A member's score is
//! `sum(value * 0.5^(age / half_life))` over the values its snapshots store for one lane, where
//! `age` is the distance from a snapshot to the NEWEST snapshot of the window. Every expected number
//! is worked out by hand beside it; ages that are whole half-lives give weights 1, 1/2, 1/4, ... so
//! the sums are exact and are asserted with `==`.

use super::*;
use crate::engine::cell_rank::{MAX_HALF_LIFE_MS, MIN_HALF_LIFE_MS};

fn decay(lane_name: &str, half_life_ms: u64) -> CellScore {
    CellScore::Decay { lane: lane_name.to_string(), half_life_ms }
}

fn ranked_by(score: CellScore, window: CellWindow, snaps: &Snapshots, now_ms: u64) -> RankedCells {
    run_on(&spec(window, score, 10), snaps, now_ms).expect("the query is valid")
}

fn pairs(ranked: &RankedCells) -> Vec<(u64, f64)> {
    order(ranked).into_iter().zip(ranked.rows.iter().map(|r| r.score)).collect()
}

#[test]
fn recency_outweighs_volume_exactly_when_the_half_life_says_it_does() {
    // m1 stored 1000 at 100, m2 stored 100 at 200; the newest snapshot is 200.
    let snaps: Snapshots = vec![(100, vec![cell(1, 0, [1000, 0, 0, 0])]), (200, vec![cell(2, 0, [100, 0, 0, 0])])];
    // Half-life 25: m1 is four half-lives old, 1000 / 16 = 62.5, behind m2's 100.
    assert_eq!(pairs(&ranked_by(decay("shown", 25), ALL, &snaps, 0)), vec![(2, 100.0), (1, 62.5)]);
    // Half-life 50: two half-lives, 1000 / 4 = 250, ahead of m2.
    assert_eq!(pairs(&ranked_by(decay("shown", 50), ALL, &snaps, 0)), vec![(1, 250.0), (2, 100.0)]);
    // The lane's own sum puts m1 first whatever the half-life: only the decay can reorder them.
    assert_eq!(order(&ranked_by(lane("shown"), ALL, &snaps, 0)), vec![1, 2]);
}

#[test]
fn a_decayed_sum_is_the_hand_computed_total_as_of_the_newest_snapshot_of_the_window() {
    // Half-life 100, newest 400: weights 400 -> 1, 300 -> 1/2, 200 -> 1/4, 100 -> 1/8. The `shown`
    // values each member's snapshots store:
    //   m1  10@100 10@200 10@300         10 * (1/8 + 1/4 + 1/2)     = 8.75
    //   m2  20@100 30@200 10@400         20/8 + 30/4 + 10           = 20
    //   m3  100@100 100@300              100/8 + 100/2              = 62.5
    //   m4  8@200 2@400                  8/4 + 2                    = 4
    //   zz  5@300                        5/2                        = 2.5
    let all = ranked_by(decay("shown", 100), ALL, &data(), 400);
    assert_eq!(pairs(&all), vec![(3, 62.5), (2, 20.0), (1, 8.75), (4, 4.0), (99, 2.5)]);
    assert_eq!((all.snapshots, all.unscored), (4, 0));

    // A window that stops at 300 is as of 300: weights 300 -> 1, 200 -> 1/2, 100 -> 1/4.
    //   m1 10/4 + 10/2 + 10 = 17.5   m2 20/4 + 30/2 = 20   m3 100/4 + 100 = 125   m4 8/2 = 4   zz 5
    // Taken as of 400 instead, m3 would be 62.5: this is the number only the window's own newest
    // snapshot can produce.
    let early = ranked_by(decay("shown", 100), CellWindow::Between { start_ms: 100, end_ms: 300 }, &data(), 400);
    assert_eq!(pairs(&early), vec![(3, 125.0), (2, 20.0), (1, 17.5), (99, 5.0), (4, 4.0)]);

    // The clock never enters: any `now` gives the same answer.
    for now_ms in [0, 399, 10_000_000, u64::MAX] {
        assert_eq!(ranked_by(decay("shown", 100), ALL, &data(), now_ms), all, "now = {now_ms}");
    }
}

#[test]
fn a_decay_reads_the_values_the_snapshots_store_and_not_the_lanes_reduction() {
    // `peak` is a Max lane, whose reduction over the window is m1 9, m2 8, m3 6, m4 2, zz 1. Decayed
    // from what each snapshot stored (half-life 100, newest 400):
    //   m1  5@100 9@200 4@300     5/8 + 9/4 + 4/2        = 4.875
    //   m2  7@100 3@200 8@400     7/8 + 3/4 + 8          = 9.625
    //   m3  2@100 6@300           2/8 + 6/2              = 3.25
    //   m4  1@200 2@400           1/4 + 2                = 2.25
    //   zz  1@300                 1/2                    = 0.5
    let ranked = ranked_by(decay("peak", 100), ALL, &data(), 0);
    assert_eq!(pairs(&ranked), vec![(2, 9.625), (1, 4.875), (3, 3.25), (4, 2.25), (99, 0.5)]);
    assert_eq!(order(&ranked_by(lane("peak"), ALL, &data(), 0)), vec![1, 2, 3, 4, 99]);
}

#[test]
fn a_decay_equals_an_independent_fold_exactly_on_whole_half_lives_and_closely_otherwise() {
    let mut rng = Rng(0xDECA_F123_4567_89AB);
    let (mut exact_rows, mut close_rows) = (0usize, 0usize);
    for round in 0..80 {
        let half_life = [64u64, 1000, 4096][round % 3];
        let dyadic = round % 2 == 0;
        let count = 1 + rng.next() % 10;
        let mut stamp = 0u64;
        let snapshots: Snapshots = (1..=count)
            .map(|_| {
                // Whole half-lives apart for the exact rounds, arbitrary distances for the others.
                stamp += if dyadic { half_life * (1 + rng.next() % 3) } else { 1 + rng.next() % (2 * half_life) };
                let mut cells = Vec::new();
                for member in 0..25u64 {
                    if rng.next() % 2 == 0 {
                        cells.push(cell(member, 0, [(rng.next() % 1000) as i64, 0, 0, 0]));
                    }
                }
                (stamp, cells)
            })
            .collect();
        let as_of = snapshots.last().unwrap().0;

        // The fold: one table per member, every stored value weighted from its own age.
        let mut table: std::collections::BTreeMap<u64, f64> = Default::default();
        for (ts, cells) in &snapshots {
            let age = as_of - ts;
            for c in cells {
                let Member::U64(m) = c.member else { unreachable!() };
                let weight = if dyadic {
                    // Whole half-lives: divide by a power of two, a formula that shares nothing with `exp2`.
                    1.0 / (1u64 << (age / half_life)) as f64
                } else {
                    0.5f64.powf(age as f64 / half_life as f64)
                };
                *table.entry(m).or_insert(0.0) += c.lanes[0] as f64 * weight;
            }
        }
        // k past the members, so every member is ranked and compared.
        let got = run_on(&spec(ALL, decay("shown", half_life), 1000), &snapshots, 0).unwrap();
        assert_eq!(got.rows.len(), table.len(), "round {round}");
        for (member, score) in pairs(&got) {
            let want = table[&member];
            if dyadic {
                assert_eq!(score, want, "round {round} member {member}");
                exact_rows += 1;
            } else {
                assert!((score - want).abs() <= 1e-9 * want.abs() + 1e-12, "round {round} member {member}: {score} vs {want}");
                close_rows += 1;
            }
        }
        // Best first, equal scores by member.
        let listed = pairs(&got);
        assert!(listed.windows(2).all(|w| w[0].1 > w[1].1 || (w[0].1 == w[1].1 && w[0].0 < w[1].0)), "round {round}");
    }
    assert!(exact_rows > 300 && close_rows > 300, "{exact_rows} exact rows, {close_rows} close rows");
}

#[test]
fn a_half_life_is_bounded_at_exactly_its_limits() {
    let compiles = |half_life_ms: u64| Plan::compile(&header(), &spec(ALL, decay("shown", half_life_ms), 1));
    assert!(compiles(MIN_HALF_LIFE_MS).is_ok());
    assert!(compiles(MAX_HALF_LIFE_MS).is_ok());
    let below = compiles(MIN_HALF_LIFE_MS - 1).err().unwrap();
    assert!(below.contains("half-life 0 ms is outside"), "{below}");
    let above = compiles(MAX_HALF_LIFE_MS + 1).err().unwrap();
    assert!(above.contains("is outside"), "{above}");
    assert_eq!((MIN_HALF_LIFE_MS, MAX_HALF_LIFE_MS), (1, 315_360_000_000));
    // Admission refuses the same values without a header.
    assert!(crate::engine::cell_rank::validate_score(&decay("shown", MIN_HALF_LIFE_MS - 1)).is_err());
    assert!(crate::engine::cell_rank::validate_score(&decay("shown", MAX_HALF_LIFE_MS + 1)).is_err());
    assert!(crate::engine::cell_rank::validate_score(&decay("shown", MAX_HALF_LIFE_MS)).is_ok());

    // Both ends are real half-lives, not just accepted numbers. At the shortest, a snapshot one
    // millisecond old counts half: 8 + 8 / 2 = 12. At the longest, a snapshot 100 ms old counts
    // 2^(-100 / 3.1536e11) of itself, which is 1.0 at f64 precision except in the 11th digit.
    let two: Snapshots = vec![(1, vec![cell(1, 0, [8, 0, 0, 0])]), (2, vec![cell(1, 0, [8, 0, 0, 0])])];
    assert_eq!(ranked_by(decay("shown", MIN_HALF_LIFE_MS), ALL, &two, 0).rows[0].score, 12.0);
    let far: Snapshots = vec![(1, vec![cell(1, 0, [1 << 20, 0, 0, 0])]), (101, vec![cell(1, 0, [1 << 20, 0, 0, 0])])];
    let slow = ranked_by(decay("shown", MAX_HALF_LIFE_MS), ALL, &far, 0).rows[0].score;
    assert!(slow < 2.0 * f64::from(1u32 << 20) && slow > 2.0 * f64::from(1u32 << 20) - 1e-3, "{slow}");
}

#[test]
fn a_total_past_the_integer_range_neither_wraps_nor_goes_unranked() {
    let max = i64::MAX as f64; // 2^63 once rounded
    assert_eq!(max, 9_223_372_036_854_775_808.0);
    let value = |v: i64| cell(1, 0, [v, 0, 0, 0]);
    let score_of = |snaps: &Snapshots| ranked_by(decay("shown", 100), ALL, snaps, 0).rows[0].score;

    // One value at the integer maximum, weight 1: the score is 2^63, not a wrapped negative.
    assert_eq!(score_of(&vec![(200, vec![value(i64::MAX)])]), max);
    // MAX at weight 1 and MAX one half-life older at weight 1/2: 1.5 * 2^63, past i64::MAX and exact.
    let past = score_of(&vec![(100, vec![value(i64::MAX)]), (200, vec![value(i64::MAX)])]);
    assert_eq!(past, 1.5 * max);
    assert!(past > max);
    // The same at the negative end.
    assert_eq!(score_of(&vec![(200, vec![value(i64::MIN)])]), -max);
    assert_eq!(score_of(&vec![(100, vec![value(i64::MIN)]), (200, vec![value(i64::MIN)])]), -1.5 * max);
    // MAX at weight 1/2 and MIN at weight 1 meet at -2^62.
    assert_eq!(score_of(&vec![(100, vec![value(i64::MAX)]), (200, vec![value(i64::MIN)])]), -0.5 * max);

    // Ranking agrees with the arithmetic: the member past the integer range leads, and the one
    // below it trails, with nothing counted as unscored.
    let snaps: Snapshots = vec![
        (100, vec![cell(1, 0, [i64::MAX, 0, 0, 0]), cell(2, 0, [i64::MAX, 0, 0, 0]), cell(3, 0, [i64::MIN, 0, 0, 0])]),
        (200, vec![cell(1, 0, [i64::MAX, 0, 0, 0]), cell(3, 0, [i64::MIN, 0, 0, 0])]),
    ];
    let ranked = ranked_by(decay("shown", 100), ALL, &snaps, 0);
    assert_eq!(pairs(&ranked), vec![(1, 1.5 * max), (2, 0.5 * max), (3, -1.5 * max)]);
    assert_eq!(ranked.unscored, 0);
}

#[test]
fn a_member_whose_weights_have_all_vanished_is_ranked_at_zero_not_dropped() {
    // m1's only value is 2000 half-lives old: 2^-2000 underflows to exactly 0.
    let snaps: Snapshots = vec![(1, vec![cell(1, 0, [1_000_000, 0, 0, 0])]), (2001, vec![cell(2, 0, [3, 0, 0, 0])])];
    let ranked = ranked_by(decay("shown", 1), ALL, &snaps, 0);
    assert_eq!(pairs(&ranked), vec![(2, 3.0), (1, 0.0)]);
    assert_eq!(ranked.unscored, 0);
}

#[test]
fn a_decay_names_a_lane_and_has_no_value_for_a_single_folded_cell() {
    let header = header();
    for (what, name) in [("a label", "slot"), ("an unknown name", "views"), ("member", "member")] {
        let error = Plan::compile(&header, &spec(ALL, decay(name, 10), 1)).err().unwrap();
        assert!(error.contains(name), "{what}: {error}");
    }
    // A score over one folded cell (a rollup's capped group) has no snapshots to decay.
    let scorer = crate::engine::cell_rank::Scorer::compile(&header, &decay("shown", 10)).unwrap();
    let error = scorer.score(&Member::U64(1), 0, &[1, 2, 3, 4]).err().unwrap();
    assert!(error.contains("snapshots"), "{error}");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn allocations(members: u64) -> u64 {
        let snaps: Snapshots = (1..=8u64)
            .map(|ts| {
                let cells = (0..members)
                    .map(|m| Cell { member: Member::U64(m), labels: vec![m % 7], lanes: vec![((m * 31 + ts) % 1000) as i64, 3, 0, 0] })
                    .collect();
                (ts * 10, cells)
            })
            .collect();
        let views = views(&snaps);
        let query = spec(CellWindow::LastN { n: 8 }, decay("shown", 25), 10);
        let plan = Plan::compile(&header(), &query).unwrap();
        let probe = Probe::start();
        let ranked = plan.run(100, &views).unwrap();
        let spent = probe.stop().allocs;
        assert_eq!(ranked.rows.len(), 10);
        spent
    }

    /// A decay is folded in place: the read allocates the same number of times over 1 000 members
    /// and over 20 000. THE CONTROL: a table that collects each member's points before folding them
    /// allocates in proportion to the members.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_decay_read_allocates_the_same_over_a_thousand_members_and_over_twenty_thousand() {
        let (small, large) = (allocations(1_000), allocations(20_000));
        let control = |members: u64| {
            let probe = Probe::start();
            let mut table: std::collections::BTreeMap<u64, Vec<(u64, i64)>> = Default::default();
            for ts in 1..=8u64 {
                for m in 0..members {
                    table.entry(m).or_default().push((ts * 10, ((m * 31 + ts) % 1000) as i64));
                }
            }
            let spent = probe.stop().allocs;
            assert_eq!(table.len() as u64, members);
            spent
        };
        println!("decay read: {small} @1000, {large} @20000; table control: {} @1000, {} @20000", control(1_000), control(20_000));
        assert!(small > 0, "the probe saw no allocation at all, so it is not installed");
        assert_eq!(small, large, "the decay read's allocations moved with the number of members");
        assert!(control(20_000) > control(1_000) + 10_000, "CONTROL: the table did not grow with the members");
    }
}
