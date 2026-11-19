// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A rollup call folds ONE bucket, and calling until none is left gives the series one call used to.
//!
//! One call folded every sample below the cutoff: it read all of them, decoded them, folded them and
//! wrote the lot, so its cost grew with the number of samples below the cutoff. It now folds the
//! oldest unsettled bucket (more than one sample, or one that is not stamped at its bucket start),
//! counts the unsettled buckets that remain from the address map without reading a block, and says
//! both in the receipt. A call that folds none is the one that applies a truncation.
//!
//! The differential folds the same series with the pure whole-series function and compares it with
//! what the loop left. The guard compares one call's allocations at two numbers of buckets with the
//! growth of THE CONTROL, the whole-series fold of the same samples, in the same test.

#![allow(clippy::all)]
use super::*;
use super::the_window_reduce_bytes_are_counted_per_operation::{header, open, stored_key, value_of};
use crate::engine::cell_set::decode_value;
use crate::engine::window_reduce_rollup::{rollup_spec, Sample};
use crate::types::WindowReduceRollupSpec;
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

fn spec(older_than_ms: u64, bucket_ms: u64) -> WindowReduceRollupSpec {
    WindowReduceRollupSpec {
        older_than_ms,
        bucket_ms,
        cap: None,
        anchor: Default::default(),
        coarser_tiers: Vec::new(),
        truncate: None,
    }
}

fn append(engine: &TemporalEngine, key: &str, stamps: &[u64]) {
    let points = stamps
        .iter()
        .map(|at| FeaturePoint { timestamp_ms: *at, value: value_of(6, *at + 1) })
        .collect();
    let response =
        engine.execute(ExecuteRequest { shard_id: 1, command: Command::FeatureAppend { key: key.to_string(), points } });
    assert!(response.status.ok, "{:?}", response.status);
}

fn stored(engine: &TemporalEngine, key: &str) -> Vec<FeaturePoint> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery { key: key.to_string(), start_ms: 0, end_ms: u64::MAX, count: None },
    });
    match response.response {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("not points: {other:?}"),
    }
}

/// (buckets folded, buckets remaining, samples after) of one call.
fn call(engine: &TemporalEngine, key: &str, spec: &WindowReduceRollupSpec) -> (u64, u64, u64) {
    let response =
        engine.execute(ExecuteRequest { shard_id: 1, command: Command::WindowReduceRollup { key: key.to_string(), spec: spec.clone() } });
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::WindowReduceRollupReceipt { buckets_folded, buckets_remaining, samples_after, .. } => {
            (buckets_folded, buckets_remaining, samples_after)
        }
        other => panic!("not a receipt: {other:?}"),
    }
}

/// THE BEHAVIOUR: one bucket a call, the remaining count falls by one, and a call that folds none
/// changes nothing.
#[test]
fn a_rollup_call_folds_one_bucket_and_says_how_many_remain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    // Buckets of 100: {10,20}, {110,120}, {210,220}, a settled single at 300, and one past the cutoff.
    append(&engine, &key, &[10, 20, 110, 120, 210, 220, 300, 1_000]);
    let request = spec(500, 100);
    assert_eq!(call(&engine, &key, &request), (1, 2, 7));
    assert_eq!(call(&engine, &key, &request), (1, 1, 6));
    assert_eq!(call(&engine, &key, &request), (1, 0, 5));
    assert_eq!(call(&engine, &key, &request), (0, 0, 5), "nothing is left to fold");
    let stamps: Vec<u64> = stored(&engine, &key).iter().map(|point| point.timestamp_ms).collect();
    assert_eq!(stamps, vec![0, 100, 200, 300, 1_000]);
}

/// THE DIFFERENTIAL: calling until none is left leaves the series the whole-series fold gives.
#[test]
fn calling_until_none_is_left_equals_the_whole_series_fold() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    let stamps = [5u64, 30, 70, 100, 160, 205, 260, 300, 410, 415, 450, 470, 900, 950];
    append(&engine, &key, &stamps);
    let request = spec(600, 100);

    let before = stored(&engine, &key);
    let header = header();
    let samples: Vec<Sample> = before
        .iter()
        .map(|point| Sample { timestamp_ms: point.timestamp_ms, cells: decode_value(&point.value).expect("a cell sample").1 })
        .collect();
    let newest = before.last().unwrap().timestamp_ms;
    let expected = rollup_spec(&header, samples, &request, newest).expect("the whole-series fold");

    let mut calls = 0;
    while call(&engine, &key, &request).0 > 0 {
        calls += 1;
        assert!(calls < 50, "the loop did not end");
    }
    assert!(calls > 1, "a single call folded everything, so this proves nothing about the loop");
    let after: Vec<(u64, Vec<crate::engine::cell_set::Cell>)> = stored(&engine, &key)
        .iter()
        .map(|point| (point.timestamp_ms, decode_value(&point.value).unwrap().1))
        .collect();
    let wanted: Vec<(u64, Vec<crate::engine::cell_set::Cell>)> =
        expected.samples.into_iter().map(|sample| (sample.timestamp_ms, sample.cells)).collect();
    assert_eq!(after, wanted);
}

/// Allocations of the first rollup call over `buckets` unsettled buckets of two samples.
#[cfg(feature = "alloc-probe")]
fn call_allocs(buckets: usize) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    let stamps: Vec<u64> = (0..buckets as u64).flat_map(|bucket| [bucket * 100 + 10, bucket * 100 + 20]).collect();
    append(&engine, &key, &stamps);
    append(&engine, &key, &[buckets as u64 * 100 + 1_000]);
    let request = spec(buckets as u64 * 100 + 500, 100);
    let probe = Probe::start();
    let folded = call(&engine, &key, &request);
    let allocs = probe.stop().allocs;
    assert_eq!(folded.0, 1);
    assert_eq!(folded.1, buckets as u64 - 1);
    allocs
}

/// THE CONTROL: the whole-series fold of the same samples, decoded and folded in one go.
#[cfg(feature = "alloc-probe")]
fn whole_fold_allocs(buckets: usize) -> u64 {
    let header = header();
    let request = spec(buckets as u64 * 100 + 500, 100);
    let encoded: Vec<(u64, Vec<u8>)> = (0..buckets as u64)
        .flat_map(|bucket| [bucket * 100 + 10, bucket * 100 + 20])
        .map(|at| (at, value_of(6, at + 1)))
        .collect();
    let probe = Probe::start();
    let samples: Vec<crate::engine::window_reduce_rollup::FlatSample> = encoded
        .iter()
        .map(|(at, bytes)| {
            let (_, packed) = crate::engine::cell_set::split_value(bytes).unwrap();
            crate::engine::window_reduce_rollup::FlatSample {
                timestamp_ms: *at,
                cells: crate::engine::cell_set::FlatCells::decode(&header, packed, 0).unwrap(),
            }
        })
        .collect();
    let result = crate::engine::window_reduce_rollup::rollup_flat_spec(&header, samples, &request, buckets as u64 * 100 + 1_000);
    std::hint::black_box(&result);
    probe.stop().allocs
}

#[cfg(feature = "alloc-probe")]
#[test]
fn a_rollup_call_does_not_allocate_more_as_the_samples_below_the_cutoff_grow() {
    let canary = Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(canary.stop().allocs > 0, "counting allocator not installed");
    drop(sink);

    let (few, many) = (16usize, 256usize);
    let _ = call_allocs(few);
    let (call_few, call_many) = (call_allocs(few), call_allocs(many));
    let (whole_few, whole_many) = (whole_fold_allocs(few), whole_fold_allocs(many));
    println!("one call: {call_few} -> {call_many}; whole-series fold: {whole_few} -> {whole_many}");
    let control_growth = whole_many.saturating_sub(whole_few);
    assert!(
        control_growth >= ((many - few) * 2) as u64,
        "the whole-series control grew by {control_growth} for {} more samples, so this instrument cannot see per-sample work",
        (many - few) * 2
    );
    let growth = call_many.saturating_sub(call_few);
    assert!(
        growth * 20 < control_growth,
        "one call allocated {call_few} over {few} buckets and {call_many} over {many}; the whole fold grows by {control_growth}"
    );
}
