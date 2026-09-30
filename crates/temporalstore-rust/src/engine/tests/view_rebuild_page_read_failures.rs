// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A PAGE A VIEW REBUILD CANNOT READ IS COUNTED AND SKIPPED, NOT TURNED INTO AN EMPTY SERIES.
//!
//! # THE CLAIM
//!
//! `insert_timestamped_secondary_view` and `insert_context_event_views` rebuild a
//! `timestamp -> address` map by READING each page and decoding the points in it. Both began
//! `block_store.read(&address).ok()` and ended `.unwrap_or_default()`, so three different outcomes
//! -- a read that failed, a payload from before the packed format, and a payload that is packed and
//! corrupt -- all became the SAME empty vector, and the key still got an entry.
//!
//! # WHY IT IS LOSS AND NOT DEGRADATION, WHICH IS THE PART WORTH DRIVING
//!
//! `reconcile_timestamped_series_membership` keeps a persisted series the derived view could not
//! produce, and its comment says exactly that: "so a transient read failure never drops a durable
//! series". That consolation is REAL for `features`, whose map is serialized.
//!
//! It does not exist for `context_events` or `context_indexes`. Both are `skip_serializing` on
//! `ShardState`, so the persisted map is EMPTY BY CONSTRUCTION on every load and the derived view is
//! the only copy there is. A swallowed read on those does not weaken an answer, it removes the
//! points. That is the same argument #2016 made for `hashes` -- "THE CONSOLATION THE OTHER THREE ARMS
//! RELY ON DOES NOT EXIST HERE" -- reaching two more maps by a different route.
//!
//! So the fixtures below CLEAR the durable map before reconciling, which is not a trick: it is what a
//! load does for a `skip_serializing` map, every time.
//!
//! # HOW THE READ IS MADE TO FAIL, AND WHY THERE IS NO SEAM
//!
//! The slab files the store was written into are TRUNCATED TO ZERO, so `block_store.read` fails for
//! real, on the arm that runs in production.
//!
//! A thread-local seam was written first, modelled on `fail_compaction_block_read_after_for_test`.
//! A mutant that deleted the counter from the REAL `Err(_)` arm then SURVIVED -- every fixture failed
//! through the seam, so nothing exercised the production arm at all, and the seam was buying a
//! `cfg(test)` hook to cover a path it was not on. Truncation covers the real arm and the seam is
//! gone from the engine.
//!
//! # THE CONTROL, AT ZERO, WITH ITS DENOMINATOR
//!
//! A clean load of the same fixture must leave BOTH counters at zero AND must have rebuilt a
//! non-empty series from a non-zero number of live pages. A control over a store with no pages
//! reports zero for the wrong reason, so the page count is asserted first -- and the fault arm below
//! reconciles CLEANLY FIRST on its own fixture, so the before/after pair is the same store rather
//! than two stores that might differ for some other reason.
#![allow(clippy::all)]
use super::*;

/// Points per series. Several, so the series is non-empty by more than one and a partial rebuild is
/// visible as a count rather than only as presence.
const POINTS: u64 = 12;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "view-rebuild-reads".to_string(),
        shard_uri: "local://view-rebuild-reads/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

/// A feature series of `POINTS` points under one key, written through the engine's own command
/// surface.
fn seed_series(engine: &TemporalEngine, key: &str) {
    for index in 0..POINTS {
        write(
            engine,
            Command::FeatureAppend {
                key: key.to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 1_000 + index,
                    value: format!("v{index:04}").into_bytes(),
                }],
            },
        );
    }
}

/// Truncate every file under the store's page directory to zero bytes, so `block_store.read` fails
/// for real. Returns how many files it tore, which is a DENOMINATOR: tearing nothing leaves the
/// fault arm reading a healthy store.
fn tear_the_pages(dir: &std::path::Path) -> usize {
    let mut torn = 0usize;
    let mut stack = vec![dir.join("pages")];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_dir() {
                stack.push(child);
            } else if std::fs::File::create(&child).is_ok() {
                torn += 1;
            }
        }
    }
    torn
}

/// Live, undeleted pages in the settled index. The DENOMINATOR for every arm: a reconcile over an
/// empty index rebuilds nothing and would make both a clean control and a fault arm look right.
fn live_pages(shard: &ShardState) -> usize {
    shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| {
            bucket
                .block_index
                .values()
                .filter(|page| !page.deleted)
                .count()
        })
        .sum()
}

fn rebuilt_points(shard: &ShardState, key: &str) -> usize {
    shard.features.get(key).map(|s| s.len()).unwrap_or(0)
}

/// rust-internal: drives the engine's own command surface
#[test]
fn a_clean_view_rebuild_reports_zero_unreadable_pages_over_a_store_that_has_some() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_series(&engine, "clean");

    crate::engine::storage_bucket_internals::reset_view_rebuild_page_failure_counts();
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    let pages = live_pages(shard);
    assert!(
        pages > 0,
        "DENOMINATOR: {pages} live pages, so this control is a zero because nothing was read"
    );

    // What a `skip_serializing` map always looks like on load: empty. So the rebuild below is the
    // only source of the series, which is what makes the fault arm in the next test a loss.
    shard.features.clear();

    crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
        &engine.block_store,
        shard,
        None,
    );

    let (read_failures, decode_failures) =
        crate::engine::storage_bucket_internals::view_rebuild_page_failure_counts();
    let rebuilt = rebuilt_points(shard, "clean");
    println!("=== a clean view rebuild ===");
    println!(
        "  {pages} live pages, {rebuilt} of {POINTS} points rebuilt, \
         {read_failures} unreadable, {decode_failures} undecodable"
    );
    assert_eq!(
        0, read_failures,
        "a clean load reported {read_failures} unreadable pages"
    );
    assert_eq!(
        0, decode_failures,
        "a clean load reported {decode_failures} undecodable pages"
    );
    assert_eq!(
        POINTS as usize, rebuilt,
        "the rebuild recovered {rebuilt} of {POINTS} points, so the fault arm would be comparing \
         against a broken control"
    );
}

/// rust-internal: drives the engine's own command surface
#[test]
fn a_torn_page_is_counted_and_leaves_no_entry_rather_than_an_empty_series() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_series(&engine, "torn");

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    let pages = live_pages(shard);
    assert!(pages > 0, "DENOMINATOR: {pages} live pages");

    // ARM A -- the same store, read cleanly. This is what the fault arm is compared against, and it
    // is the SAME fixture rather than a second one that might differ for another reason.
    crate::engine::storage_bucket_internals::reset_view_rebuild_page_failure_counts();
    shard.features.clear();
    crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
        &engine.block_store,
        shard,
        None,
    );
    let (clean_failures, _) =
        crate::engine::storage_bucket_internals::view_rebuild_page_failure_counts();
    let clean_points = rebuilt_points(shard, "torn");

    // ARM B -- tear the slab files and rebuild from the same index.
    let torn = tear_the_pages(dir.path());
    assert!(
        torn > 0,
        "DENOMINATOR: {torn} page files torn, so arm B read a healthy store and is arm A again"
    );
    crate::engine::storage_bucket_internals::reset_view_rebuild_page_failure_counts();
    shard.features.clear();
    crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
        &engine.block_store,
        shard,
        None,
    );
    let (torn_failures, torn_decode_failures) =
        crate::engine::storage_bucket_internals::view_rebuild_page_failure_counts();
    let torn_points = rebuilt_points(shard, "torn");
    let key_present = shard.features.contains_key("torn");

    println!("=== the same store, read cleanly and then torn ===");
    println!("  clean: {clean_points} of {POINTS} points, {clean_failures} unreadable");
    println!(
        "  torn:  {torn_points} points, {torn_failures} unreadable, \
         {torn_decode_failures} undecodable, over {torn} torn files; key present = {key_present}"
    );

    assert_eq!(0, clean_failures, "arm A reported failures on a healthy store");
    assert_eq!(
        POINTS as usize, clean_points,
        "arm A recovered {clean_points} of {POINTS}"
    );
    // THE FAILURE HAPPENED, asserted before anything downstream is read. A fault arm whose fault
    // never fired leaves every assertion below passing for the wrong reason.
    assert!(
        torn_failures > 0,
        "the torn store reported {torn_failures} read failures, so the tearing did nothing and the \
         two arms are one arm"
    );
    // AND THE KEY IS NOT PRESENT-AND-EMPTY. `record_exists_exact` reads `contains_key` on this map,
    // so an empty series under a live key is an EXISTS of 1 over nothing -- #2017's shape by reload.
    let phantom = key_present && torn_points == 0;
    assert!(
        !phantom,
        "the key is present holding an EMPTY series, which is the defect this guard exists for"
    );
    // The two arms must DIFFER, or the fault proved nothing.
    assert!(
        torn_points < clean_points,
        "the torn arm recovered {torn_points} points and the clean arm {clean_points}, so tearing \
         the pages changed nothing and this comparison is empty"
    );
}
