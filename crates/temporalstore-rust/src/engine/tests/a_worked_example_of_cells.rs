// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WORKED EXAMPLE OF CELLS: WRITE MERGED, WINDOWED STATISTICS, THEN RANK THEM.
//!
//! This test is also the documentation. It follows one user (the ENTITY) through two named
//! DIMENSIONS of the key, `channel` and `kind`, and stores for each item the user was shown (the
//! MEMBER) two counters (the LANES), `impressions` and `clicks`, plus one LABEL, `slot`, that
//! says where the item was placed. It then asks the question a ranking service asks: of the items
//! shown in the last minute, which has the best click rate?
//!
//! Nothing in the engine knows the words user, channel, kind, impression or click. They are names
//! a deployment declares once, in the header the first write carries, and every later write is
//! checked against it.
//!
//! WHAT IT SHOWS, IN ORDER
//!   1. the key: an entity and two dimensions become ONE stored feature key;
//!   2. the write: cells merged lane by lane under a declared verb (`Sum`), across writes that
//!      share a timestamp, with a receipt of what landed;
//!   3. the windowed read: only the snapshots of the last minute count;
//!   4. two scores that order the same members differently: the plain click rate, and the Wilson
//!      lower bound, which does not trust a rate it has seen once;
//!   5. a selection by label, cut before the member's snapshots are reduced together;
//!   6. a name the key did not declare is refused.
//!
//! WHAT IS NOT HERE: rollup and shrink of old snapshots, and a time-to-live per group. A key has
//! one time-to-live, as every feature key does.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType,
    EntityValue, KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{
    CellMemberId, CellRankSpec, CellScore, CellWindow, FeatureFilter, FeatureFilterOp, RankedCells,
};

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

/// The key family: an entity that is a `u64` (a user id) and two required `u16` dimensions.
fn key_spec() -> KeySpec {
    KeySpec {
        namespace: "shown".to_string(),
        entity: EntityType::U64,
        dimensions: vec![
            DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true },
            DimSpec { name: "kind".to_string(), ty: DimType::U16, required: true },
        ],
    }
}

/// The header every write of this key family carries: one label and two lanes, each lane summed
/// when two writes meet on a member.
fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: key_spec(),
        labels: vec!["slot".to_string()],
        lanes: vec![
            LaneSpec { name: "impressions".to_string(), reducer: LaneReducer::Sum },
            LaneSpec { name: "clicks".to_string(), reducer: LaneReducer::Sum },
        ],
    }
}

/// A cell: the member (an item id), its label (the placement, here `member % 7`) and its lanes.
fn cell(member: u64, impressions: i64, clicks: i64) -> Cell {
    Cell { member: Member::U64(member), labels: vec![member % 7], lanes: vec![impressions, clicks] }
}

/// One merge-reduce write of `cells` at `timestamp_ms`. Answers what landed:
/// (snapshots written, cells created, cells merged into a member already there).
fn write(engine: &TemporalEngine, key: &str, timestamp_ms: u64, cells: &[Cell]) -> (u64, u64, u64) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: key.to_string(),
            points: vec![FeaturePoint { timestamp_ms, value: encode_value(&header(), cells).unwrap() }],
            policy: FeatureWritePolicy::MergeReduce,
            retry_token: None,
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::CellMergeReceipt { snapshots_written, cells_created, cells_merged, .. } => {
            (snapshots_written, cells_created, cells_merged)
        }
        other => panic!("a merge-reduce append answers a receipt, got {other:?}"),
    }
}

/// The ranked read: the last minute, optionally cut by a predicate, ordered by `score`.
fn rank(engine: &TemporalEngine, key: &str, filters: Vec<FeatureFilter>, score: CellScore) -> RankedCells {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CellRankQuery {
            key: key.to_string(),
            spec: CellRankSpec {
                window: CellWindow::Last { duration_ms: 60_000 },
                filters,
                score,
                top_k: 10,
                project: Vec::new(),
            },
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::CellRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "refused: {}", ranked.refused);
            ranked
        }
        other => panic!("not a ranking: {other:?}"),
    }
}

fn members(ranked: &RankedCells) -> Vec<u64> {
    ranked
        .rows
        .iter()
        .map(|row| match &row.member {
            CellMemberId::U64(item) => *item,
            CellMemberId::Bytes(_) => unreachable!("this example uses u64 members"),
        })
        .collect()
}

fn click_rate() -> CellScore {
    CellScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() }
}

fn wilson_bound() -> CellScore {
    CellScore::Wilson { successes: "clicks".to_string(), trials: "impressions".to_string(), z: 1.96 }
}

#[test]
fn a_worked_example_of_cells() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let now = crate::engine::resolve_now_ms();

    // ---- 1. THE KEY ---------------------------------------------------------------------------
    // User 42, on channel 3, of kind 2. The spec turns the entity and the two dimension values
    // into one stored feature key. Every user, channel and kind has its own key, so a read by
    // those three is one lookup.
    let user = 42u64;
    let key = encode_key(
        &key_spec(),
        &CellKey {
            entity: EntityValue::U64(user),
            dims: vec![Some(DimValue::U16(3)), Some(DimValue::U16(2))],
        },
    )
    .unwrap();

    // ---- 2. THE WRITES ------------------------------------------------------------------------
    // An old snapshot, ten minutes ago: item 1 was shown 1 000 times and never clicked. The last
    // minute's window below will not see it.
    let ten_minutes_ago = now - 600_000;
    assert_eq!(write(&engine, &key, ten_minutes_ago, &[cell(1, 1000, 0)]), (1, 1, 0));

    // A snapshot from half a minute ago holds two cells: item 1 shown once and clicked once, and
    // item 2 shown 60 times and clicked 50. One snapshot written, two cells created, none merged.
    let half_a_minute_ago = now - 30_000;
    assert_eq!(
        write(&engine, &key, half_a_minute_ago, &[cell(1, 1, 1), cell(2, 60, 50)]),
        (1, 2, 0)
    );
    // A second write to the SAME timestamp merges into it, lane by lane, under `Sum`: item 2 was
    // shown 40 more times and clicked 30 more. Nothing is created and one cell is merged, so the
    // snapshot at that timestamp now holds item 2 at 100 impressions and 80 clicks.
    assert_eq!(write(&engine, &key, half_a_minute_ago, &[cell(2, 40, 30)]), (1, 0, 1));

    // A later snapshot, twenty seconds ago, introduces item 3: shown 10 times, clicked 9.
    let twenty_seconds_ago = now - 20_000;
    assert_eq!(write(&engine, &key, twenty_seconds_ago, &[cell(3, 10, 9)]), (1, 1, 0));

    // ---- 3. THE RANKED READ, BY CLICK RATE ----------------------------------------------------
    // window Last{60 s} -> reduce per item across the snapshots the window holds -> score the
    // click rate -> the best first. The ten-minute-old snapshot is outside the window, so item 1
    // is 1 click in 1 impression, not 1 click in 1 001.
    let by_rate = rank(&engine, &key, Vec::new(), click_rate());
    assert_eq!(by_rate.snapshots, 2, "the window holds the half-minute and the twenty-second snapshots");
    assert_eq!(members(&by_rate), vec![1, 3, 2]);
    let rates: Vec<f64> = by_rate.rows.iter().map(|row| row.score).collect();
    for (got, want) in rates.iter().zip([1.0, 0.9, 0.8]) {
        assert!((got - want).abs() < 1e-12, "{rates:?}");
    }
    // The row carries what the merge produced: item 2's merged lanes, in the header's order
    // (impressions, clicks), and its label.
    let item_2 = by_rate.rows.iter().find(|row| row.member == CellMemberId::U64(2)).unwrap();
    assert_eq!(item_2.lanes, vec![100, 80]);
    assert_eq!(item_2.labels, vec![2]);
    assert_eq!(by_rate.lane_columns, vec!["impressions".to_string(), "clicks".to_string()]);

    // ---- 4. THE SAME QUESTION, BY THE WILSON LOWER BOUND ---------------------------------------
    // A click rate of 1 out of 1 is a perfect rate and almost no evidence. The Wilson lower bound
    // at z = 1.96 (a 95% interval) is the rate the data can support: 80 of 100 is about 0.711,
    // 9 of 10 about 0.596, and 1 of 1 about 0.207. The same three items, ordered the other way.
    let by_bound = rank(&engine, &key, Vec::new(), wilson_bound());
    assert_eq!(members(&by_bound), vec![2, 3, 1]);
    let bounds: Vec<f64> = by_bound.rows.iter().map(|row| row.score).collect();
    for (got, want) in bounds.iter().zip([0.7112, 0.5959, 0.2065]) {
        assert!((got - want).abs() < 1e-3, "{bounds:?}");
    }

    // ---- 5. A SELECTION BY LABEL --------------------------------------------------------------
    // Only the placements below slot 3: items 1 and 2 (item 3 sits in slot 3). The selection cuts
    // each cell as its snapshot stores it, before an item's snapshots are reduced together.
    let first_two_slots =
        || vec![FeatureFilter { field: "slot".to_string(), op: FeatureFilterOp::LessThan, value: 3 }];
    assert_eq!(members(&rank(&engine, &key, first_two_slots(), click_rate())), vec![1, 2]);
    assert_eq!(members(&rank(&engine, &key, first_two_slots(), wilson_bound())), vec![2, 1]);

    // ---- 6. A NAME THE KEY DID NOT DECLARE IS REFUSED, NOT GUESSED ----------------------------
    let refused = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CellRankQuery {
            key: key.clone(),
            spec: CellRankSpec {
                window: CellWindow::Last { duration_ms: 60_000 },
                filters: Vec::new(),
                score: CellScore::Lane { lane: "views".to_string() },
                top_k: 10,
                project: Vec::new(),
            },
        },
    });
    match refused.response {
        CommandResponse::CellRanking { ranked } => {
            assert!(ranked.refused.contains("views"), "{}", ranked.refused);
            assert!(ranked.rows.is_empty());
        }
        other => panic!("not a ranking: {other:?}"),
    }
}
