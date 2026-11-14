// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE MERGE-REDUCE WRITE POLICY OF THE FEATURE APPEND, DRIVEN THROUGH THE REAL `execute` PATH.
//!
//! A merge-reduce point carries its own request: the key spec, the label and lane names, the
//! per-lane reducers and the cells. The engine reads the snapshot stored at that timestamp,
//! merges the cells in lane by lane, and writes one merged snapshot per timestamp.
//!
//! WHAT EACH GUARD CAN ONLY PASS ON THE NEW PATH.
//!   * `every_reducer_on_every_lane_matches_an_independent_fold` runs 36 headers (each of the six
//!     reducers on each of the six lane positions) against a fold written in the test. A count
//!     lane that kept the written value, or a reducer wired to the wrong lane, fails one of them.
//!   * the `Count` lane reads 3 after three writes that each carried 999. Only a path that merges
//!     can say 3, and only one that ignores the carried value says 3 rather than 2997.
//!   * a refused request writes nothing: the series is read back unchanged after each refusal,
//!     and the refusal names the field that differs.

#![allow(clippy::all)]
use super::*;

use crate::engine::cell_set::{
    self, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue, KeySpec, LaneReducer,
    LaneSpec, Member, HEADER_VERSION, MAX_CELLS_PER_REQUEST, MAX_CELLS_PER_SNAPSHOT,
};

const SIX: [LaneReducer; 6] = [
    LaneReducer::Sum,
    LaneReducer::Max,
    LaneReducer::Min,
    LaneReducer::First,
    LaneReducer::Last,
    LaneReducer::Count,
];

fn spec() -> KeySpec {
    KeySpec {
        namespace: "ns".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

fn header(reducers: &[LaneReducer]) -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: spec(),
        labels: vec!["slot".to_string()],
        lanes: reducers
            .iter()
            .enumerate()
            .map(|(i, reducer)| LaneSpec { name: format!("l{i}"), reducer: *reducer })
            .collect(),
    }
}

fn key_for(entity: u64) -> String {
    cell_set::encode_key(
        &spec(),
        &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(3))] },
    )
    .expect("the key fits the spec")
}

fn cell(member: u64, lanes: &[i64]) -> Cell {
    Cell { member: Member::U64(member), labels: vec![member % 7], lanes: lanes.to_vec() }
}

fn point(header: &CellHeader, timestamp_ms: u64, cells: &[Cell]) -> FeaturePoint {
    FeaturePoint {
        timestamp_ms,
        value: cell_set::encode_value(header, cells).expect("the request encodes"),
    }
}

fn engine_on(dir: &std::path::Path, bound: Option<usize>) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    if let Some(bound) = bound {
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 2,
                        feature_max_size: bound,
                        ..Default::default()
                    },
                })
                .ok,
            "the bound has to be the one under test"
        );
    }
    engine
}

fn merge(engine: &TemporalEngine, key: &str, points: Vec<FeaturePoint>) -> ExecuteResponse {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: key.to_string(),
            points,
            policy: FeatureWritePolicy::MergeReduce,
            retry_token: None,
        },
    })
}

fn landed(response: &ExecuteResponse) -> (u64, u64, u64) {
    assert!(response.status.ok, "the request was refused: {:?}", response.status);
    match &response.response {
        CommandResponse::CellMergeReceipt { snapshots_written, cells_created, cells_merged, .. } => {
            (*snapshots_written, *cells_created, *cells_merged)
        }
        other => panic!("a merge-reduce append answers a receipt, got {other:?}"),
    }
}

fn raw_series(engine: &TemporalEngine, key: &str) -> Vec<FeaturePoint> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(1_000_000),
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

fn snapshots(engine: &TemporalEngine, key: &str) -> Vec<(u64, CellHeader, Vec<Cell>)> {
    raw_series(engine, key)
        .into_iter()
        .map(|p| {
            let (h, cells) = cell_set::decode_value(&p.value).expect("a stored value is a cell snapshot");
            (p.timestamp_ms, h, cells)
        })
        .collect()
}

fn lanes_of(engine: &TemporalEngine, key: &str, timestamp_ms: u64, member: u64) -> Vec<i64> {
    snapshots(engine, key)
        .into_iter()
        .find(|(at, _, _)| *at == timestamp_ms)
        .and_then(|(_, _, cells)| cells.into_iter().find(|c| c.member == Member::U64(member)))
        .map(|c| c.lanes)
        .unwrap_or_else(|| panic!("member {member} is not stored at {timestamp_ms}"))
}

/// The fold a reducer is DEFINED by, spelled without `LaneReducer::combine`.
fn oracle(reducer: LaneReducer, values: &[i64]) -> i64 {
    match reducer {
        LaneReducer::Sum => values.iter().sum(),
        LaneReducer::Max => *values.iter().max().unwrap(),
        LaneReducer::Min => *values.iter().min().unwrap(),
        LaneReducer::First => values[0],
        LaneReducer::Last => *values.last().unwrap(),
        LaneReducer::Count => values.len() as i64,
    }
}

#[test]
fn every_reducer_on_every_lane_matches_an_independent_fold() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let bases = [5i64, -3, 9, 2];
    let mut combos = 0;
    for (r, reducer) in SIX.iter().enumerate() {
        for lane in 0..6usize {
            let mut reducers = [LaneReducer::Sum; 6];
            reducers[lane] = *reducer;
            let h = header(&reducers);
            let entity = (r * 6 + lane) as u64;
            let key = key_for(entity);
            for base in bases {
                let lanes: Vec<i64> = (0..6).map(|i| base * (i as i64 + 1) + i as i64).collect();
                landed(&merge(&engine, &key, vec![point(&h, 100, &[cell(10, &lanes)])]));
            }
            let got = lanes_of(&engine, &key, 100, 10);
            for i in 0..6usize {
                let seen: Vec<i64> = bases.iter().map(|b| b * (i as i64 + 1) + i as i64).collect();
                assert_eq!(
                    oracle(reducers[i], &seen),
                    got[i],
                    "reducer {} on lane {i} (the reducer under test sits on lane {lane})",
                    reducers[i].name()
                );
            }
            combos += 1;
        }
    }
    assert_eq!(36, combos, "DENOMINATOR: six reducers on six lane positions");
}

#[test]
fn a_count_lane_counts_writes_and_ignores_the_value_it_carries() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let h = header(&SIX);
    let key = key_for(1);
    for v in [5i64, -3, 9] {
        landed(&merge(&engine, &key, vec![point(&h, 100, &[cell(10, &[v, v * 10, v + 100, v, v, 999])])]));
    }
    // Sum 11, Max 90, Min 97, First 5, Last 9 -- and the count lane is 3, not 2997 and not 999.
    assert_eq!(vec![11, 90, 97, 5, 9, 3], lanes_of(&engine, &key, 100, 10));
}

#[test]
fn write_order_changes_first_and_last_and_nothing_else_and_one_request_equals_three() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let h = header(&SIX);
    let write = |key: &str, order: &[i64]| {
        for v in order {
            landed(&merge(&engine, key, vec![point(&h, 7, &[cell(1, &[*v, *v, *v, *v, *v, *v])])]));
        }
    };
    let (a, b) = (key_for(10), key_for(11));
    write(&a, &[4, -8, 6]);
    write(&b, &[6, 4, -8]);
    let (la, lb) = (lanes_of(&engine, &a, 7, 1), lanes_of(&engine, &b, 7, 1));
    // Sum, Max, Min and Count do not depend on the order writes arrive in.
    assert_eq!(la[..3], lb[..3]);
    assert_eq!(la[5], lb[5]);
    // First and Last do, and name the first and the last write in each order.
    assert_eq!((4, 6), (la[3], la[4]));
    assert_eq!((6, -8), (lb[3], lb[4]));

    // One request holding the member three times merges three times, in request order.
    let c = key_for(12);
    let cells = [4i64, -8, 6].map(|v| cell(1, &[v, v, v, v, v, v]));
    assert_eq!((1, 1, 2), landed(&merge(&engine, &c, vec![point(&h, 7, &cells)])));
    assert_eq!(la, lanes_of(&engine, &c, 7, 1));

    // A re-delivered request: Max, Min, First and Last are unchanged; Sum and Count move.
    let before = lanes_of(&engine, &c, 7, 1);
    landed(&merge(&engine, &c, vec![point(&h, 7, &cells)]));
    let after = lanes_of(&engine, &c, 7, 1);
    assert_eq!(before[1..5], after[1..5]);
    assert_eq!(before[0] * 2, after[0]);
    assert_eq!(before[5] * 2, after[5]);
}

#[test]
fn the_receipt_says_what_landed() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let h = header(&SIX);
    let key = key_for(20);
    let lanes = [1i64; 6];
    assert_eq!(
        (1, 3, 0),
        landed(&merge(&engine, &key, vec![point(&h, 50, &[cell(1, &lanes), cell(2, &lanes), cell(3, &lanes)])]))
    );
    // Member 3 is stored, member 4 is new, and member 4 appears twice.
    assert_eq!(
        (1, 1, 2),
        landed(&merge(&engine, &key, vec![point(&h, 50, &[cell(3, &lanes), cell(4, &lanes), cell(4, &lanes)])]))
    );
    // Two timestamps, one of them new: two snapshots written.
    assert_eq!(
        (2, 1, 1),
        landed(&merge(
            &engine,
            &key,
            vec![point(&h, 50, &[cell(1, &lanes)]), point(&h, 60, &[cell(9, &lanes)])]
        ))
    );
    // Two points at one timestamp are one snapshot.
    assert_eq!(
        (1, 0, 2),
        landed(&merge(
            &engine,
            &key,
            vec![point(&h, 60, &[cell(9, &lanes)]), point(&h, 60, &[cell(9, &lanes)])]
        ))
    );
}

#[test]
fn a_merged_snapshot_survives_a_cold_reload_and_keeps_merging() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), None);
    let h = header(&SIX);
    let key = key_for(30);
    for v in [5i64, -3, 9] {
        landed(&merge(&engine, &key, vec![point(&h, 100, &[cell(10, &[v, v, v, v, v, 0])])]));
    }
    let before = snapshots(&engine, &key);
    assert_eq!(vec![11, 9, -3, 5, 9, 3], before[0].2[0].lanes);
    engine.unload_shard(1);
    drop(engine);

    let reloaded = engine_on(dir.path(), None);
    assert_eq!(before, snapshots(&reloaded, &key), "a cold reload served another snapshot");
    landed(&merge(&reloaded, &key, vec![point(&h, 100, &[cell(10, &[1, 1, 1, 1, 1, 0])])]));
    // The merge continued from the reloaded value: sum 12, count 4, max still 9.
    assert_eq!(vec![12, 9, -3, 5, 1, 4], lanes_of(&reloaded, &key, 100, 10));
}

fn refused(response: &ExecuteResponse, needle: &str) {
    assert!(!response.status.ok, "the request was accepted");
    assert_eq!("invalid_argument", response.status.code);
    assert!(response.status.message.contains(needle), "{:?} does not name `{needle}`", response.status.message);
    assert_eq!(CommandResponse::Empty, response.response);
}

#[test]
fn a_request_that_disagrees_with_the_stored_header_is_refused_naming_the_field_and_writes_nothing() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let stored = header(&[LaneReducer::Sum, LaneReducer::Max]);
    let key = key_for(40);
    landed(&merge(&engine, &key, vec![point(&stored, 10, &[cell(1, &[3, 4])])]));
    let before = raw_series(&engine, &key);
    assert_eq!(1, before.len(), "DENOMINATOR: the first write recorded the header");

    let mut other_reducer = stored.clone();
    other_reducer.lanes[0].reducer = LaneReducer::Max;
    refused(&merge(&engine, &key, vec![point(&other_reducer, 10, &[cell(1, &[3, 4])])]), "lane `l0` reducer differs");

    let mut other_name = stored.clone();
    other_name.lanes[1].name = "renamed".to_string();
    refused(&merge(&engine, &key, vec![point(&other_name, 20, &[cell(1, &[3, 4])])]), "lane 1 name differs");

    let one_lane = header(&[LaneReducer::Sum]);
    refused(&merge(&engine, &key, vec![point(&one_lane, 20, &[cell(1, &[3])])]), "lane `l1` is missing");

    let mut other_label = stored.clone();
    other_label.labels[0] = "other".to_string();
    refused(&merge(&engine, &key, vec![point(&other_label, 20, &[cell(1, &[3, 4])])]), "label 0 name differs");

    assert_eq!(before, raw_series(&engine, &key), "a refused request wrote something");

    // A request that only ADDS a trailing lane is accepted, and the stored snapshot reads it as 0
    // until the merge sets it.
    let extended = header(&[LaneReducer::Sum, LaneReducer::Max, LaneReducer::Sum]);
    landed(&merge(&engine, &key, vec![point(&extended, 10, &[cell(1, &[1, 1, 5])])]));
    assert_eq!(vec![4, 4, 5], lanes_of(&engine, &key, 10, 1));
}

#[test]
fn a_key_that_does_not_fit_its_spec_a_junk_value_a_plain_value_and_an_empty_request_are_refused() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let h = header(&[LaneReducer::Sum]);
    let key = key_for(50);

    refused(
        &merge(&engine, "plain-key", vec![point(&h, 1, &[cell(1, &[1])])]),
        "key does not match the declared key spec",
    );
    refused(
        &merge(&engine, &key, vec![FeaturePoint { timestamp_ms: 1, value: b"junk".to_vec() }]),
        "point at 1:",
    );
    refused(&merge(&engine, &key, Vec::new()), "needs at least one point");
    let mut other_ns = h.clone();
    other_ns.key.namespace = "other".to_string();
    refused(&merge(&engine, &key, vec![point(&other_ns, 1, &[cell(1, &[1])])]), "namespace");
    assert!(raw_series(&engine, &key).is_empty(), "DENOMINATOR: every refusal above wrote nothing");

    // A key holding plain feature values cannot be merged into.
    assert!(engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: key.clone(),
                points: vec![FeaturePoint { timestamp_ms: 5, value: b"plain".to_vec() }],
            },
        })
        .status
        .ok);
    refused(&merge(&engine, &key, vec![point(&h, 5, &[cell(1, &[1])])]), "not a cell snapshot");
    assert_eq!(b"plain".to_vec(), raw_series(&engine, &key)[0].value);

    // `Block` is still refused where it always was.
    let blocked = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: key.clone(),
            points: vec![],
            policy: FeatureWritePolicy::Block,
            retry_token: None,
        },
    });
    assert!(!blocked.status.ok);
    assert_eq!("Invalid write policy", blocked.status.message);
}

fn distinct(from: u64, count: usize) -> Vec<Cell> {
    (from..from + count as u64).map(|m| cell(m, &[1])).collect()
}

#[test]
fn cells_per_request_and_per_snapshot_refuse_one_past_and_accept_the_limit() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let h = header(&[LaneReducer::Sum]);
    let key = key_for(60);

    refused(
        &merge(&engine, &key, vec![point(&h, 1, &distinct(0, MAX_CELLS_PER_REQUEST + 1))]),
        &format!("cells per request {} is over the limit of {}", MAX_CELLS_PER_REQUEST + 1, MAX_CELLS_PER_REQUEST),
    );
    assert!(raw_series(&engine, &key).is_empty());
    assert_eq!(
        (1, MAX_CELLS_PER_REQUEST as u64, 0),
        landed(&merge(&engine, &key, vec![point(&h, 1, &distinct(0, MAX_CELLS_PER_REQUEST))]))
    );

    // Fill the snapshot to exactly its limit, one request at a time.
    let mut next = MAX_CELLS_PER_REQUEST as u64;
    while (next as usize) < MAX_CELLS_PER_SNAPSHOT {
        landed(&merge(&engine, &key, vec![point(&h, 1, &distinct(next, MAX_CELLS_PER_REQUEST))]));
        next += MAX_CELLS_PER_REQUEST as u64;
    }
    assert_eq!(MAX_CELLS_PER_SNAPSHOT, snapshots(&engine, &key)[0].2.len(), "DENOMINATOR: at the limit");
    // At the limit a request that only merges is accepted ...
    assert_eq!((1, 0, 2), landed(&merge(&engine, &key, vec![point(&h, 1, &distinct(0, 2))])));
    // ... and one new member is one too many, with nothing changed.
    let before = raw_series(&engine, &key);
    refused(
        &merge(&engine, &key, vec![point(&h, 1, &distinct(next, 1))]),
        &format!("cells per snapshot {} is over the limit of {}", MAX_CELLS_PER_SNAPSHOT + 1, MAX_CELLS_PER_SNAPSHOT),
    );
    assert_eq!(before, raw_series(&engine, &key));
    // Another timestamp is another snapshot, so the same member is fine there.
    landed(&merge(&engine, &key, vec![point(&h, 2, &distinct(next, 1))]));
}

#[test]
fn the_feature_ceiling_holds_for_a_merge_reduce_append() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let h = header(&[LaneReducer::Sum]);
    let key = key_for(70);
    let many = |count: usize| -> Vec<FeaturePoint> {
        (0..count as u64).map(|at| point(&h, 10 + at, &[cell(1, &[1])])).collect()
    };
    let over = merge(&engine, &key, many(FEATURE_ADD_HARD_MAX_SIZE + 1));
    assert!(!over.status.ok);
    assert_eq!("invalid_argument", over.status.code);
    assert!(over.status.message.contains(&format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}")));
    assert!(raw_series(&engine, &key).is_empty(), "a request past the ceiling wrote something");
    let at = landed(&merge(&engine, &key, many(FEATURE_ADD_HARD_MAX_SIZE)));
    assert_eq!((FEATURE_ADD_HARD_MAX_SIZE as u64, FEATURE_ADD_HARD_MAX_SIZE as u64, 0), at);
}

#[test]
fn the_trim_keeps_the_newest_snapshots_and_each_still_carries_its_header() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), Some(3));
    let h = header(&[LaneReducer::Sum]);
    let key = key_for(80);
    for at in 1..=5u64 {
        landed(&merge(&engine, &key, vec![point(&h, at, &[cell(1, &[at as i64])])]));
    }
    let live = |e: &TemporalEngine| snapshots(e, &key).into_iter().map(|(at, hd, c)| (at, hd == h, c[0].lanes[0])).collect::<Vec<_>>();
    assert_eq!(vec![(3, true, 3), (4, true, 4), (5, true, 5)], live(&engine));
    // Merging into a snapshot already in the series adds nothing to trim.
    landed(&merge(&engine, &key, vec![point(&h, 4, &[cell(1, &[10])])]));
    assert_eq!(vec![(3, true, 3), (4, true, 14), (5, true, 5)], live(&engine));
    let before = live(&engine);
    engine.unload_shard(1);
    drop(engine);
    assert_eq!(before, live(&engine_on(dir.path(), Some(3))), "a reload brought back an evicted snapshot");
}

#[test]
fn a_key_past_its_deadline_merges_as_a_new_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let h = header(&[LaneReducer::Sum]);
    let key = key_for(90);
    landed(&merge(&engine, &key, vec![point(&h, 1, &[cell(1, &[5]), cell(2, &[5])])]));
    assert!(engine
        .execute(ExecuteRequest { shard_id: 1, command: Command::CommonExpire { key: key.clone(), ttl_ms: 1 } })
        .status
        .ok);
    std::thread::sleep(std::time::Duration::from_millis(30));
    // Member 1 is written again: if the expired snapshot were merged into, its sum would be 12.
    assert_eq!((1, 1, 0), landed(&merge(&engine, &key, vec![point(&h, 1, &[cell(1, &[7])])])));
    let cells = &snapshots(&engine, &key)[0].2;
    assert_eq!(1, cells.len(), "the expired snapshot's other member came back");
    assert_eq!(vec![7], cells[0].lanes);
}

/// `SequenceAdd` writes bytes this change must not move: the value a row is stored as.
#[test]
fn a_sequence_add_still_stores_exactly_the_bytes_it_always_did() {
    let row = crate::types::SequenceFeatureRow {
        timestamp_ms: 5,
        gid: 300,
        action_type: 2,
        duration: 7,
        author_id: 1,
    };
    let expected: Vec<u8> = vec![0x08, 0xAC, 0x02, 0x10, 0x02, 0x18, 0x07, 0x20, 0x01];
    assert_eq!(expected, row.encode_feature_proto_value());

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd { key: "seq".to_string(), rows: vec![row] },
        })
        .status
        .ok);
    let stored = raw_series(&engine, "seq");
    assert_eq!(1, stored.len());
    assert_eq!(5, stored[0].timestamp_ms);
    assert_eq!(expected, stored[0].value, "the stored row bytes moved");
}

#[test]
fn the_policy_has_a_wire_number_a_text_spelling_and_survives_the_proto_round_trip() {
    assert_eq!(
        5,
        crate::sdk::feature_write_policy_to_proto(FeatureWritePolicy::MergeReduce) as i32
    );
    assert_eq!(
        FeatureWritePolicy::MergeReduce,
        crate::sdk::feature_write_policy_from_proto(5).unwrap()
    );
    let json = serde_json::to_string(&FeatureWritePolicy::MergeReduce).unwrap();
    assert_eq!("\"merge_reduce\"", json);
    assert_eq!(FeatureWritePolicy::MergeReduce, serde_json::from_str(&json).unwrap());
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn text_spec(entity_bytes: usize) -> (KeySpec, String) {
        let spec = KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::Text,
            dimensions: vec![],
        };
        let key = cell_set::encode_key(
            &spec,
            &CellKey { entity: EntityValue::Text("e".repeat(entity_bytes)), dims: vec![] },
        )
        .unwrap();
        (spec, key)
    }

    /// (allocations, bytes) of one merge of `cells` cells into a snapshot of the same members.
    fn per_write(entity_bytes: usize, cells: usize) -> (f64, f64) {
        const OPS: usize = 20;
        let (spec, key) = text_spec(entity_bytes);
        let h = CellHeader {
            version: HEADER_VERSION,
            key: spec,
            labels: vec![],
            lanes: vec![LaneSpec { name: "n".to_string(), reducer: LaneReducer::Sum }],
        };
        let members: Vec<Cell> = (0..cells as u64)
            .map(|m| Cell { member: Member::U64(m), labels: vec![], lanes: vec![1] })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path(), None);
        landed(&merge(&engine, &key, vec![point(&h, 100, &members)]));
        let requests: Vec<FeaturePoint> = (0..OPS).map(|_| point(&h, 100, &members)).collect();
        let probe = Probe::start();
        for request in requests {
            landed(&merge(&engine, &key, vec![request]));
        }
        let counts = probe.stop();
        (counts.allocs as f64 / OPS as f64, counts.alloc_bytes as f64 / OPS as f64)
    }

    /// Allocations a write spends do not depend on how long the key is: the key is parsed and
    /// carried as one string, never spelled out per cell. The CONTROL is the same two writes read
    /// in BYTES, which must grow with the key, or the probe cannot see the dimension varied.
    /// A per-cell cost that did depend on the key would show as a difference in the count.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_merge_does_not_allocate_more_for_a_longer_key() {
        let (short_allocs, short_bytes) = per_write(8, 50);
        let (long_allocs, long_bytes) = per_write(200, 50);
        eprintln!("MERGE key=8B: {short_allocs} allocs {short_bytes} B; key=200B: {long_allocs} allocs {long_bytes} B");
        assert!(short_allocs > 0.0 && long_allocs > 0.0, "the probe is blind");
        assert!(
            long_bytes - short_bytes >= 192.0,
            "CONTROL: bytes did not grow with the key ({short_bytes} -> {long_bytes}), so key length is not a dimension this probe sees"
        );
        // Within one allocation per write: background threads add a tenth of one, a per-cell copy
        // of the key adds one per cell (50 here).
        assert!(
            (short_allocs - long_allocs).abs() < 1.0,
            "the allocation count moved with the key length ({short_allocs} -> {long_allocs}): a per-write or per-cell copy of the key"
        );
        // And per CELL: the slope between 50 and 150 cells is the same at both key lengths.
        let (short_more, _) = per_write(8, 150);
        let (long_more, _) = per_write(200, 150);
        assert!(((short_more - short_allocs) - (long_more - long_allocs)).abs() < 1.0);
    }

    /// A merge-reduce write spends a fixed number of allocations whatever the cell count: the
    /// snapshot is decoded, merged and encoded in a few vectors for the whole snapshot, not a few
    /// allocations per cell. Measured: 130.9 per write at 50, 150 and 1,000 cells (it was 687.8,
    /// 1,699.7 and 10,211.8). The slope is asserted between 50 and 1,000 cells, where a per-cell
    /// allocation of even one would add 950; the absolute bound is the measured 131 plus margin.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_merge_write_does_not_allocate_per_cell() {
        let (few, few_bytes) = per_write(8, 50);
        let (many, many_bytes) = per_write(8, 1000);
        eprintln!("MERGE_PER_CELL 50 cells: {few} allocs {few_bytes} B; 1000 cells: {many} allocs {many_bytes} B");
        // The control: the probe sees SIZE, so a flat count is not a blind instrument.
        assert!(many_bytes > few_bytes * 4.0, "the probe cannot see the cell count: {few_bytes} -> {many_bytes}");
        assert!(few > 20.0, "the probe is blind: {few}");
        assert!(
            (many - few).abs() < 10.0,
            "the allocations of a write moved with the cell count ({few} at 50, {many} at 1000): a per-cell allocation is back"
        );
        assert!(many < 160.0, "a write at 1000 cells spends {many} allocations; measured 131");
    }

    /// Prints the allocations one write spends at several cell counts, for the before/after table.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn merge_write_allocation_table() {
        for cells in [50usize, 150, 1000] {
            let (a, b) = per_write(8, cells);
            eprintln!("MERGE_TABLE cells={cells} allocs/write={a:.1} allocs/cell={:.2} bytes/write={b:.0}", a / cells as f64);
        }
    }
}
