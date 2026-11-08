// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SEQUENCE WRITE STATES THE ROWS IT WROTE, NOT EVERY BLOCK THE SERIES HOLDS.
//!
//! A sequence row files the same block a feature point does: one per timestamp, in the same
//! series, under the same element name. A feature append states the blocks it wrote in the
//! index-log record; a sequence write was missing from that list, so its record restated one row
//! per block the series already held. Per write, with the allocation probe, adding one row to a
//! 100-row series: 462 allocation blocks and 316 KB.
//!
//! WHAT MOVES ON DISK: the index-log record of a sequence write carries one block row per
//! timestamp written where it carried one per block of the series. Nothing else in the record
//! moves, and the shipped load path reads none of those rows (the index-log fold is gated behind
//! the legacy-recovery switch).
//!
//! THE TRIM IS THE PART THAT HAD TO BE ESTABLISHED. A write that pushes a series past its bound
//! evicts the oldest points, and a rebuild from blocks would resurrect them unless the record pins
//! the membership. The record pins it when the membership SHRANK, and an append that evicts exactly
//! as many points as it adds does not shrink it -- which is why a feature append asks
//! `command_will_trim_feature_membership` first. A sequence write now asks it too.
//!
//! TWO GUARDS, BECAUSE THEY FAIL ON DISJOINT MUTATIONS.
//!   * `a_sequence_write_states_only_the_rows_it_wrote` reads the record a write left: one row per
//!     timestamp written, for a lone row, a batch, a replaced timestamp and a duplicate inside one
//!     command, with a control (the feature append's record) that must read the same way.
//!   * `a_sequence_write_that_evicts_as_many_as_it_adds_pins_the_membership` fills a series to its
//!     bound and adds one row: the series does not shrink, and the record must still carry the
//!     pinned membership. A trim prediction that skipped sequences passes the first guard and fails
//!     this one.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{CommandResponse, SequenceFeatureRow};

const BOUND: usize = 8;

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

fn row(timestamp_ms: u64) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms,
        gid: timestamp_ms,
        action_type: 1,
        duration: 2,
        author_id: 3,
    }
}

fn add(engine: &TemporalEngine, timestamps: &[u64]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "seq".to_string(),
            rows: timestamps.iter().map(|&at| row(at)).collect(),
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn live(engine: &TemporalEngine) -> Vec<u64> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceQuery {
                key: "seq".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: 1000,
                filters: Vec::new(),
            },
        })
        .response
    {
        CommandResponse::SequenceRows { rows } => {
            let mut at = rows.iter().map(|row| row.timestamp_ms).collect::<Vec<_>>();
            at.sort_unstable();
            at
        }
        other => panic!("expected SequenceRows, got {other:?}"),
    }
}

/// (rows, key states) of the newest index-log record, and how many records the log holds.
fn newest(engine: &TemporalEngine) -> (usize, usize, usize) {
    let records = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("delta records readable");
    let last = records.last().expect("the log holds a record");
    (records.len(), last.items.len(), last.key_states.len())
}

#[test]
fn a_sequence_write_states_only_the_rows_it_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), None);
    for at in 0..40u64 {
        add(&engine, &[1000 + at]);
    }
    assert_eq!(live(&engine).len(), 40, "the series did not fill");

    // THE CONTROL: a feature append is the record this one is meant to read like.
    let control = {
        let other = tempfile::tempdir().unwrap();
        let engine = engine_on(other.path(), None);
        for at in 0..40u64 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: "seq".to_string(),
                    points: vec![FeaturePoint { timestamp_ms: 1000 + at, value: vec![1] }],
                },
            });
            assert!(response.status.ok);
        }
        newest(&engine).1
    };
    assert_eq!(control, 1, "a feature append's record states {control} rows; the reader cannot see rows");

    let (records_before, _, _) = newest(&engine);
    add(&engine, &[2000]);
    let (records, rows, _) = newest(&engine);
    assert_eq!(records, records_before + 1, "a lone row did not write exactly one record");
    assert_eq!(rows, 1, "adding one row to forty restated {rows} block rows");

    add(&engine, &[2001, 2002, 2003]);
    assert_eq!(newest(&engine).1, 3, "a batch of three rows must state three rows");

    add(&engine, &[1005]);
    assert_eq!(newest(&engine).1, 1, "replacing a timestamp must state the one block that replaced it");

    add(&engine, &[3000, 3000, 3001]);
    assert_eq!(newest(&engine).1, 2, "a duplicate timestamp inside one command states one row");
    assert_eq!(live(&engine).len(), 40 + 1 + 3 + 2, "the writes did not all land");
}

#[test]
fn a_sequence_write_that_evicts_as_many_as_it_adds_pins_the_membership() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), Some(BOUND));
    for at in 0..BOUND as u64 {
        add(&engine, &[100 + at]);
    }
    assert_eq!(live(&engine).len(), BOUND, "the series did not fill to its bound");
    // BELOW THE BOUND NOTHING IS EVICTED, so nothing is pinned: the control for the assertion below.
    let (_, _, pinned_below_bound) = newest(&engine);
    assert_eq!(pinned_below_bound, 0, "a write below the bound pinned the membership");

    // One more row: the oldest goes and the length is unchanged, so only the prediction sees it.
    add(&engine, &[500]);
    let (_, rows, pinned) = newest(&engine);
    assert_eq!(rows, 1, "the trimming write states the row it wrote");
    assert!(
        pinned > 0,
        "a sequence write that evicted as many points as it added left the record without the \
         pinned membership, so a rebuild from blocks would resurrect the evicted point"
    );
    let kept = live(&engine);
    assert_eq!(kept.len(), BOUND);
    assert!(!kept.contains(&100) && kept.contains(&500), "the eviction did not take the oldest: {kept:?}");

    // The same history, reloaded cold: what is served is what was live.
    for at in [501u64, 502, 503] {
        add(&engine, &[at]);
    }
    let before = live(&engine);
    engine.unload_shard(1);
    drop(engine);
    let reloaded = engine_on(dir.path(), Some(BOUND));
    assert_eq!(live(&reloaded), before, "a cold reload served another series");
}

/// Allocation and profiler driver, not a gate: `SEQ_E=<rows> SEQ_K=<writes>`. The allocation probe
/// prints per-write blocks and bytes; for instructions run it under `valgrind --tool=callgrind
/// --toggle-collect='*sequence_span*'`.
#[inline(never)]
fn sequence_span(engine: &TemporalEngine, base: u64, writes: usize) {
    for index in 0..writes as u64 {
        add(engine, &[base + index]);
    }
}

#[test]
#[ignore]
fn a_sequence_add_for_the_profiler() {
    let rows = std::env::var("SEQ_E").ok().and_then(|v| v.parse().ok()).unwrap_or(1000usize);
    let writes = std::env::var("SEQ_K").ok().and_then(|v| v.parse().ok()).unwrap_or(20usize);
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), None);
    for index in 0..rows as u64 {
        add(&engine, &[1000 + index]);
    }
    #[cfg(feature = "alloc-probe")]
    let probe = crate::alloc_probe::Probe::start();
    sequence_span(&engine, 1_000_000, writes);
    #[cfg(feature = "alloc-probe")]
    {
        let counts = probe.stop();
        eprintln!(
            "SEQADD rows={rows} writes={writes}: {} allocs {} bytes per write",
            counts.allocs as f64 / writes as f64,
            counts.alloc_bytes as f64 / writes as f64
        );
    }
    let (_, record_rows, _) = newest(&engine);
    eprintln!("SEQADD rows={rows}: newest record states {record_rows} rows");
}
