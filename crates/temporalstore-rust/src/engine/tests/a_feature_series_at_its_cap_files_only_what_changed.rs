// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FEATURE SERIES AT ITS CAP FILES THE BLOCKS A WRITE CHANGED, NOT EVERY BLOCK THE SERIES HOLDS.
//!
//! A feature append that replaced a point or trimmed one used to restate the whole live address
//! list: it copied every address of the series into a vector and re-filed every block of the
//! object in the bucket index. A series at its cap trims on every append, so every append paid the
//! length of the series (measured: 830 allocations at 100 points, 7,376 at 1,000, 1.45 MB per
//! append at 1,000 one-point blocks, against 61 below the cap). The append, the policy append, the
//! sequence add and the range replace now name the addresses their dropped, replaced and trimmed
//! points held, ask the series which of those blocks no point maps to any more, and remove
//! exactly those.
//!
//! WHAT MOVES: the allocations and the work per write at a cap. WHAT DOES NOT: the points kept,
//! the blocks written, the log record's outcome items, and the set of blocks the index files for
//! the object.
//!
//! GUARDS.
//!   * `the_blocks_filed_are_the_blocks_the_points_name_after_every_write` drives all four writers
//!     at a small bound over ascending, interleaved, replaced and range-replaced timestamps, and
//!     requires after EVERY write that the series equals a model, and that the set of blocks the
//!     index files for the key equals the set the series' points name. A block kept that no point
//!     names, or dropped that one does, fails here.
//!   * `a_write_that_leaves_a_block_one_live_point_keeps_it` pins the case that makes the liveness
//!     question necessary: a block whose first points are trimmed while its last still live.
//!   * `the_old_and_the_new_filing_store_the_same` replays one sequence through the restating path
//!     (a test switch) and the narrow path, and compares the answers, the log record of every
//!     write (outcome items and all) and the filed block set.
//!   * `probe::an_append_at_the_cap_allocates_the_same_whatever_the_series_length`
//!     (alloc-probe, `--ignored`) holds the allocations of an append at a cap flat in the series
//!     length, with the restating path as the control that must grow.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::block_store::ElementEntry;
use crate::engine::storage_bucket_internals::{block_physical_identity_key, RESTATE_SERIES_BLOCKS};
use crate::engine::timestamped_series::TimestampedSeries as _;
use crate::types::CommandResponse;
use std::collections::{BTreeMap, BTreeSet};

pub(super) const KEY: &str = "k";

pub(super) fn open(dir: &std::path::Path, bound: usize) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
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
    engine
}

pub(super) fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn point(timestamp_ms: u64, tag: usize) -> FeaturePoint {
    FeaturePoint {
        timestamp_ms,
        value: format!("value-{tag}-at-{timestamp_ms}").into_bytes(),
    }
}

fn row(timestamp_ms: u64, tag: usize) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms,
        gid: tag as u64,
        action_type: 1,
        duration: 2,
        author_id: 3,
    }
}

fn series(engine: &TemporalEngine) -> BTreeMap<u64, Vec<u8>> {
    match run(
        engine,
        Command::FeatureQuery {
            key: KEY.to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: Some(1_000_000),
        },
    ) {
        CommandResponse::FeaturePoints { points } => points
            .into_iter()
            .map(|point| (point.timestamp_ms, point.value))
            .collect(),
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

type Identity = (u64, u64, u64, Option<u64>, Option<u64>);

/// The blocks the points of the series name, and the blocks the bucket index files for the key.
fn blocks(engine: &TemporalEngine) -> (BTreeSet<Identity>, Vec<Identity>, usize) {
    engine.shards.with_state(1, |owner| {
        let shard = owner.expect("shard 1 is loaded");
        let named: BTreeSet<Identity> = shard
            .features()
            .get(KEY)
            .map(|series| series.addresses().map(block_physical_identity_key).collect())
            .unwrap_or_default();
        let mut filed: Vec<Identity> = Vec::new();
        if let Some(refs) = shard.bucket_index.object_block_refs("feature", KEY) {
            for block_ref in refs.iter() {
                let bucket = shard
                    .bucket_index
                    .bucket_map
                    .get(&block_ref.routing_bucket)
                    .expect("a ref names a bucket the index holds");
                let block = bucket
                    .block_index
                    .get(&block_ref.block_ref_key)
                    .expect("a ref names a block its bucket holds");
                filed.push(block_physical_identity_key(&block.address));
            }
        }
        filed.sort();
        // Every block of every bucket, so a block the refs forgot is counted too.
        let in_buckets: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.block_index.len())
            .sum();
        (named, filed, in_buckets)
    })
}

fn assert_filing_matches_points(engine: &TemporalEngine, context: &str) {
    let (named, filed, in_buckets) = blocks(engine);
    let filed_set: BTreeSet<Identity> = filed.iter().copied().collect();
    assert_eq!(filed.len(), filed_set.len(), "{context}: a block is filed twice");
    assert_eq!(named, filed_set, "{context}: the blocks filed are not the blocks the points name");
    assert_eq!(
        filed.len(),
        in_buckets,
        "{context}: the buckets hold {in_buckets} blocks where the object's refs name {}",
        filed.len()
    );
}

pub(super) struct Rng(pub(super) u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }
}

/// One write of the sequence, as the command to send and what it does to the model.
pub(super) fn step(rng: &mut Rng, n: usize, newest: &mut u64, model: &mut BTreeMap<u64, Vec<u8>>, bound: usize) -> Command {
    let command = match rng.next(100) {
        // A few points anywhere in a window the series has already filled: replaces and
        // interleaves, so one block's points are not contiguous.
        0..=39 => {
            let points: Vec<FeaturePoint> = (0..1 + rng.next(3))
                .map(|i| point(1_000 + rng.next(60) * 10, n * 10 + i as usize))
                .collect();
            for p in &points {
                model.insert(p.timestamp_ms, p.value.clone());
            }
            Command::FeatureAppend { key: KEY.to_string(), points }
        }
        // A run beyond the newest point: multi-point blocks that later trim away from the front.
        40..=64 => {
            let points: Vec<FeaturePoint> = (0..8 + rng.next(20))
                .map(|i| {
                    *newest += 10;
                    point(*newest, n * 100 + i as usize)
                })
                .collect();
            for p in &points {
                model.insert(p.timestamp_ms, p.value.clone());
            }
            Command::FeatureAppend { key: KEY.to_string(), points }
        }
        65..=79 => {
            let points: Vec<FeaturePoint> = (0..1 + rng.next(5))
                .map(|i| point(1_000 + rng.next(90) * 10, n * 10 + i as usize))
                .collect();
            for p in &points {
                model.insert(p.timestamp_ms, p.value.clone());
            }
            Command::FeatureAppendWithPolicy {
                key: KEY.to_string(),
                points,
                policy: FeatureWritePolicy::Upsert,
                retry_token: None,
            }
        }
        80..=89 => {
            let start = 1_000 + rng.next(60) * 10;
            let end = start + rng.next(8) * 10;
            let points: Vec<FeaturePoint> = (0..rng.next(5))
                .map(|i| point(1_000 + rng.next(80) * 10, n * 10 + i as usize))
                .collect();
            model.retain(|at, _| *at < start || *at > end);
            for p in &points {
                model.insert(p.timestamp_ms, p.value.clone());
            }
            Command::FeatureReplace { key: KEY.to_string(), start_ms: start, end_ms: end, points }
        }
        _ => {
            let rows: Vec<SequenceFeatureRow> = (0..1 + rng.next(3))
                .map(|i| row(1_000 + rng.next(70) * 10, n * 10 + i as usize))
                .collect();
            for r in &rows {
                model.insert(r.timestamp_ms, r.encode_feature_proto_value());
            }
            Command::SequenceAdd { key: KEY.to_string(), rows }
        }
    };
    while model.len() > bound {
        let oldest = *model.keys().next().unwrap();
        model.remove(&oldest);
    }
    command
}

#[test]
fn the_blocks_filed_are_the_blocks_the_points_name_after_every_write() {
    const BOUND: usize = 40;
    let mut shared_blocks_seen = 0usize;
    let mut writes_that_trimmed = 0usize;
    for seed in 1..=4u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), BOUND);
        let mut rng = Rng(seed);
        let mut model = BTreeMap::new();
        let mut newest = 1_000 + 90 * 10;
        for n in 0..160usize {
            let before = model.len();
            let command = step(&mut rng, n, &mut newest, &mut model, BOUND);
            run(&engine, command);
            assert_eq!(series(&engine), model, "seed {seed}, write {n}");
            assert_filing_matches_points(&engine, &format!("seed {seed}, write {n}"));
            let (named, _, _) = blocks(&engine);
            if named.len() < model.len() {
                shared_blocks_seen += 1;
            }
            if before >= BOUND {
                writes_that_trimmed += 1;
            }
        }
        assert_eq!(model.len(), BOUND, "seed {seed}: the series never reached its cap");
    }
    // THE CONTROLS: the equality above holds for a reason. Blocks were shared by several points
    // (so a trimmed point did not always retire its block) and writes did reach the cap.
    assert!(shared_blocks_seen > 100, "only {shared_blocks_seen} writes left shared blocks");
    assert!(writes_that_trimmed > 100, "only {writes_that_trimmed} writes were at the cap");
}

#[test]
fn a_write_that_leaves_a_block_one_live_point_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), 4);
    // One block of four points, then single points that trim it from the front.
    run(
        &engine,
        Command::FeatureAppend {
            key: KEY.to_string(),
            points: (1..=4).map(|at| point(at * 10, 0)).collect(),
        },
    );
    let (named, _, _) = blocks(&engine);
    assert_eq!(1, named.len(), "the four points were meant to share one block");
    for (n, at) in [50u64, 60, 70].into_iter().enumerate() {
        run(&engine, Command::FeatureAppend { key: KEY.to_string(), points: vec![point(at, 1)] });
        assert_filing_matches_points(&engine, &format!("single point {n}"));
        let (named, filed, _) = blocks(&engine);
        // The first block still holds one live point after each of the first three trims.
        assert_eq!(named.len(), filed.len());
        assert_eq!(named.len(), 2 + n, "after {} single points", n + 1);
    }
    // The fourth single point trims the last point of the first block: it goes, and only it.
    run(&engine, Command::FeatureAppend { key: KEY.to_string(), points: vec![point(80, 1)] });
    assert_filing_matches_points(&engine, "the first block's last point went");
    let (named, _, _) = blocks(&engine);
    assert_eq!(4, named.len(), "the first block should be gone and four one-point blocks remain");
    // A point written again over the only point of a block retires that block.
    run(&engine, Command::FeatureAppend { key: KEY.to_string(), points: vec![point(80, 2)] });
    assert_filing_matches_points(&engine, "a replaced point's block");
    let (named, _, _) = blocks(&engine);
    assert_eq!(4, named.len());
}

/// Every record of the shard's log, decoded.
fn log(engine: &TemporalEngine) -> Vec<crate::wal::WriteAheadLogRecord> {
    engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .into_iter()
        .map(|(_, line)| crate::wal::decode_wal_line(&line).unwrap())
        .collect()
}

#[test]
fn the_old_and_the_new_filing_store_the_same() {
    const BOUND: usize = 40;
    let dir_old = tempfile::tempdir().unwrap();
    let dir_new = tempfile::tempdir().unwrap();
    let old = open(dir_old.path(), BOUND);
    let new = open(dir_new.path(), BOUND);
    let mut rng = Rng(11);
    let mut model = BTreeMap::new();
    let mut newest = 1_000 + 90 * 10;
    let mut trimmed_writes = 0usize;
    for n in 0..200usize {
        let before = model.len();
        let command = step(&mut rng, n, &mut newest, &mut model, BOUND);
        use_the_old_paths(true);
        run(&old, command.clone());
        use_the_old_paths(false);
        run(&new, command);
        trimmed_writes += usize::from(before >= BOUND);
        assert_eq!(series(&old), series(&new), "write {n}: the answers differ");
        assert_eq!(series(&new), model, "write {n}");
        let (old_named, old_filed, _) = blocks(&old);
        let (new_named, new_filed, _) = blocks(&new);
        assert_eq!(old_named, new_named, "write {n}: the blocks the points name differ");
        assert_eq!(old_filed, new_filed, "write {n}: the blocks filed differ");
    }
    assert!(trimmed_writes > 100, "only {trimmed_writes} writes reached the cap");
    // THE LOG: every record, command and outcome items alike, is what the restating path wrote.
    let (old_log, new_log) = (log(&old), log(&new));
    assert_eq!(old_log.len(), new_log.len());
    assert!(old_log.len() >= 150, "the log holds {} records", old_log.len());
    let mut outcome_items = 0usize;
    for (n, (a, b)) in old_log.iter().zip(&new_log).enumerate() {
        assert_eq!(a.outcomes, b.outcomes, "record {n}: the outcome items differ");
        assert_eq!(a.command, b.command, "record {n}: the command differs");
        outcome_items += a.outcomes.len();
    }
    assert!(outcome_items > 400, "only {outcome_items} outcome items were compared");
    // THE FILES: the index log the writes appended (every delta record, key-state blobs and
    // all) and the block slabs, byte for byte.
    let (old_files, new_files) = (tree_files(dir_old.path()), tree_files(dir_new.path()));
    assert_eq!(
        old_files.keys().collect::<Vec<_>>(),
        new_files.keys().collect::<Vec<_>>(),
        "the two engines wrote different files"
    );
    let mut compared_bytes = 0usize;
    for (name, bytes) in &old_files {
        // The cache is not stored state, and the slab manifest stamps the time it was written.
        if name.starts_with("cache") || name.ends_with("slab_manifest.pb") {
            continue;
        }
        let other = &new_files[name];
        let at = bytes.iter().zip(other).position(|(a, b)| a != b);
        assert!(
            at.is_none() && bytes.len() == other.len(),
            "{name}: {} bytes against {}, first difference at {at:?}",
            bytes.len(),
            other.len()
        );
        compared_bytes += bytes.len();
    }
    assert!(compared_bytes > 100_000, "only {compared_bytes} bytes of files were compared");
    let index_log_bytes: usize = old_files
        .iter()
        .filter(|(name, _)| name.starts_with("indexes"))
        .map(|(_, bytes)| bytes.len())
        .sum();
    assert!(index_log_bytes > 50_000, "the index log held only {index_log_bytes} bytes");
    // AND AFTER A FLUSH, the stored index image. (The flush stamps a time into the image, so a
    // difference is reported with where it is rather than failing on the stamp alone.)
    old.flush_shard_index(1);
    new.flush_shard_index(1);
    let (old_files, new_files) = (tree_files(dir_old.path()), tree_files(dir_new.path()));
    assert_eq!(old_files.keys().collect::<Vec<_>>(), new_files.keys().collect::<Vec<_>>());
    let differing: Vec<&String> = old_files
        .iter()
        .filter(|(name, bytes)| {
            !name.starts_with("cache") && !name.ends_with("slab_manifest.pb") && new_files[*name] != **bytes
        })
        .map(|(name, _)| name)
        .collect();
    assert!(differing.is_empty(), "after a flush these files differ: {differing:?}");
}

/// Every file under `root`, by path relative to it.
fn tree_files(root: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else if let Ok(bytes) = std::fs::read(&path) {
                let name = path.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
                out.insert(name, bytes);
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// The restating filing and the `Value` capture together: what every write did before.
fn use_the_old_paths(on: bool) {
    RESTATE_SERIES_BLOCKS.with(|switch| switch.set(on));
    crate::engine::CAPTURE_SERIES_AS_VALUES.with(|switch| switch.set(on));
}

/// A series and a few unrelated fields, as the blob the write used to build: a `Value` object
/// with the series turned into a `Value` first.
fn blob_as_values(
    key: &str,
    series: &BTreeMap<u64, ElementEntry>,
    extra: &[(&str, serde_json::Value)],
) -> serde_json::Value {
    let mut blob = serde_json::Map::new();
    blob.insert("key".to_string(), serde_json::Value::String(key.to_string()));
    for (name, value) in extra {
        blob.insert(name.to_string(), value.clone());
    }
    blob.insert("features".to_string(), serde_json::to_value(series).unwrap());
    serde_json::Value::Object(blob)
}

fn blob_as_captured(
    key: &str,
    series: &BTreeMap<u64, ElementEntry>,
    extra: &[(&str, serde_json::Value)],
) -> crate::index_log::KeyState {
    let mut rest = serde_json::Map::new();
    rest.insert("key".to_string(), serde_json::Value::String(key.to_string()));
    for (name, value) in extra {
        rest.insert(name.to_string(), value.clone());
    }
    let mut features: Vec<(u64, ElementEntry)> =
        series.iter().map(|(at, address)| (*at, address.clone())).collect();
    features.sort_unstable_by(|l, r| crate::index_log::compare_instants_as_text(l.0, r.0));
    crate::index_log::KeyState::Captured { rest, features }
}

#[test]
fn a_captured_series_is_written_in_the_bytes_the_value_was() {
    let entry = |slab: u64, offset: u64, length: u64, block: Option<u64>| {
        ElementEntry::from_parts(slab, offset, length, block, None)
    };
    let mut corpus: Vec<(&str, BTreeMap<u64, ElementEntry>)> = vec![
        ("empty", BTreeMap::new()),
        ("one point", BTreeMap::from([(1_787_270_070_123, entry(3, 4096, 88, Some(0)))])),
        (
            "instants whose text order is not their numeric order",
            [9u64, 10, 99, 100, 101, 1_000, 19, 2, 20, 200, 3]
                .into_iter()
                .enumerate()
                .map(|(n, at)| (at, entry(1, n as u64 * 64, 64, Some(n as u64))))
                .collect(),
        ),
        (
            "large instants",
            BTreeMap::from([
                (0, entry(0, 0, 1, None)),
                (u64::MAX, entry(1, 1, 2, Some(65_535))),
                (u64::MAX - 1, entry(2, 5, 7, None)),
                (10_000_000_000_000_000_000, entry(4, 8, 9, Some(7))),
                (9_999_999_999_999_999_999, entry(5, 9, 10, Some(8))),
            ]),
        ),
    ];
    // Replaced points: one block named by many instants, then a later block over some of them.
    let mut replaced = BTreeMap::new();
    for at in 0..200u64 {
        replaced.insert(1_000 + at * 10, entry(1, 0, 4096, Some(0)));
    }
    for at in (0..200u64).step_by(7) {
        replaced.insert(1_000 + at * 10, entry(1, 4096, 100, Some(1)));
    }
    corpus.push(("replaced points", replaced));
    let extras: Vec<(&str, Vec<(&str, serde_json::Value)>)> = vec![
        ("nothing else", vec![]),
        (
            "fields on both sides of `features` in sort order, with a float and strings",
            vec![
                ("expires_at_ms", serde_json::json!(1.5e12)),
                ("context_nodes", serde_json::json!("a \"quoted\" \u{e9} string")),
                ("control_state_changes", serde_json::json!({"x": [1, 2.25, null, "s"]})),
                ("zzz_after", serde_json::json!(-7)),
            ],
        ),
    ];
    let mut compared = 0usize;
    for (name, series) in &corpus {
        for (extra_name, extra) in &extras {
            let old = blob_as_values("the-key", series, extra);
            let new = blob_as_captured("the-key", series, extra);
            assert_eq!(
                serde_json::to_vec(&old).unwrap(),
                serde_json::to_vec(&new).unwrap(),
                "{name} / {extra_name}: the JSON text differs"
            );
            assert_eq!(
                rmp_serde::to_vec(&old).unwrap(),
                rmp_serde::to_vec(&new).unwrap(),
                "{name} / {extra_name}: the msgpack differs"
            );
            let mut named = Vec::new();
            let mut ser = rmp_serde::Serializer::new(&mut named).with_struct_map();
            serde::Serialize::serialize(&new, &mut ser).unwrap();
            let mut named_old = Vec::new();
            let mut ser = rmp_serde::Serializer::new(&mut named_old).with_struct_map();
            serde::Serialize::serialize(&old, &mut ser).unwrap();
            assert_eq!(named_old, named, "{name} / {extra_name}: the named msgpack differs");
            assert_eq!(new.to_value(), old, "{name} / {extra_name}");
            compared += 1;
        }
    }
    // Not vacuous: a planted difference in one point changes the bytes this compares.
    let (_, series) = &corpus[1];
    let mut moved = series.clone();
    moved.insert(1_787_270_070_123, entry(3, 4096, 89, Some(0)));
    assert_ne!(
        serde_json::to_vec(&blob_as_values("the-key", series, &[])).unwrap(),
        serde_json::to_vec(&blob_as_captured("the-key", &moved, &[])).unwrap()
    );
    assert_eq!(compared, corpus.len() * extras.len());
}

#[test]
fn the_capture_of_a_key_names_the_series_it_holds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), 10);
    for at in [40u64, 5, 300, 20] {
        run(&engine, Command::FeatureAppend { key: KEY.to_string(), points: vec![point(at, 0)] });
    }
    engine.shards.with_state(1, |owner| {
        let shard = owner.unwrap();
        let held: BTreeMap<u64, ElementEntry> = shard
            .features()
            .get(KEY)
            .unwrap()
            .points()
            .map(|(at, address)| (at, address.clone()))
            .collect();
        assert_eq!(4, held.len());
        let captured = capture_key_states(shard, &[KEY.to_string(), "absent".to_string()]);
        assert_eq!(2, captured.len());
        assert_eq!(blob_as_values(KEY, &held, &[]), captured[0].to_value());
        assert_eq!(serde_json::json!({"key": "absent"}), captured[1].to_value());
    });
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 200;

    /// Allocations per append, of `OPS` single-point appends to a series that holds `held` points
    /// at a bound of `held`, filled in blocks of `block_points` points.
    fn per_append(held: usize, block_points: usize, restate: bool) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = open(dir.path(), held);
        let mut at = 1_000u64;
        let mut filled = 0;
        while filled < held {
            let take = block_points.min(held - filled);
            let points: Vec<FeaturePoint> = (0..take)
                .map(|_| {
                    at += 10;
                    point(at, 0)
                })
                .collect();
            run(&engine, Command::FeatureAppend { key: KEY.to_string(), points });
            filled += take;
        }
        let commands: Vec<Command> = (0..OPS)
            .map(|_| {
                at += 10;
                Command::FeatureAppend { key: KEY.to_string(), points: vec![point(at, 1)] }
            })
            .collect();
        use_the_old_paths(restate);
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok);
        }
        let allocs = probe.stop().allocs as f64 / OPS as f64;
        use_the_old_paths(false);
        assert_filing_matches_points(&engine, "after the measured appends");
        allocs
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn an_append_at_the_cap_allocates_the_same_whatever_the_series_length() {
        let canary = crate::alloc_probe::Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`"
        );
        drop(sink);

        let mut table = String::new();
        let mut narrow = BTreeMap::new();
        let mut restating = BTreeMap::new();
        for (label, block_points) in [("batched", 50usize), ("one-point", 1usize)] {
            for held in [100usize, 1_000, 10_000] {
                if block_points == 1 && held == 10_000 {
                    continue;
                }
                let new = per_append(held, block_points, false);
                let old = per_append(held, block_points, true);
                table.push_str(&format!(
                    "  [cap] {label:>9} blocks, {held:>6} points: {new:>9.1} allocs/append narrow, {old:>9.1} restating\n"
                ));
                narrow.insert((label, held), new);
                restating.insert((label, held), old);
            }
        }
        eprint!("{table}");
        for label in ["batched", "one-point"] {
            let (short, long) = (narrow[&(label, 100)], narrow[&(label, 1_000)]);
            assert!(
                long - short < 12.0,
                "{label}: an append at the cap allocates {short:.1} at 100 points and {long:.1} at 1,000; \
                 the cost still grows with the series\n{table}"
            );
            // THE CONTROL: the restating path, through the same harness, must show the growth this
            // guard exists to catch, or a flat line proves nothing.
            let (old_short, old_long) = (restating[&(label, 100)], restating[&(label, 1_000)]);
            assert!(
                old_long > old_short * 3.0,
                "{label}: the restating control read {old_short:.1} at 100 points and {old_long:.1} at \
                 1,000, so this instrument cannot see the growth\n{table}"
            );
        }
        let (short, long) = (narrow[&("batched", 1_000)], narrow[&("batched", 10_000)]);
        assert!(long - short < 12.0, "batched, 1,000 -> 10,000 points: {short:.1} -> {long:.1}\n{table}");
    }
}

mod logical_edit_generation {
    use super::*;
    use crate::engine::hashing::block_routing_bucket;
    use crate::engine::state::{BlockLookupRef, BucketFlags, BucketNode, CoreIndex, StaleCause};
    use crate::engine::storage_bucket_internals::{
        file_series_blocks_after_write, sync_bucket_index_object_blocks_with_mode,
    };

    type OwnerRows = BTreeMap<u32, (u64, u64, u64, bool, bool, Vec<(u64, bool, bool, Identity)>)>;

    fn owners(engine: &TemporalEngine) -> OwnerRows {
        engine.shards.with_state(1, |state| {
            let state = state.unwrap();
            state.bucket_index.bucket_map.iter().map(|(id, bucket)| {
                let mut blocks: Vec<_> = bucket.block_index.iter().map(|(handle, block)| (
                    *handle, block.dirty(), block.deleted(), block_physical_identity_key(&block.address),
                )).collect();
                blocks.sort();
                (*id, (bucket.dirty_generation, bucket.first_dirty_wal_sequence,
                    bucket.first_dirty_index_log_sequence, bucket.dirty(), bucket.deleted(), blocks))
            }).collect()
        })
    }

    fn refs(engine: &TemporalEngine, key: &str) -> BTreeSet<(u32, u64)> {
        engine.shards.with_state(1, |state| {
            state.unwrap().bucket_index.object_block_refs("feature", key)
                .map(|refs| refs.iter().map(|r| (r.routing_bucket, r.block_ref_key)).collect())
                .unwrap_or_default()
        })
    }

    struct ResetOldPath;
    impl Drop for ResetOldPath {
        fn drop(&mut self) { use_the_old_paths(false); }
    }

    #[test]
    fn real_series_commands_preserve_generation_refs_and_independent_claims_in_both_modes() {
        let _reset = ResetOldPath;
        let old_dir = tempfile::tempdir().unwrap();
        let new_dir = tempfile::tempdir().unwrap();
        let old = open(old_dir.path(), 40);
        let new = open(new_dir.path(), 40);
        let mut rng = Rng(11);
        let mut model = BTreeMap::new();
        let mut newest = 1_900;
        let mut edits = 0;
        for n in 0..200 {
            let command = step(&mut rng, n, &mut newest, &mut model, 40);
            use_the_old_paths(true);
            run(&old, command.clone());
            use_the_old_paths(false);
            run(&new, command);
            assert_eq!(owners(&old), owners(&new), "owner metadata after write {n}");
            assert_eq!(refs(&old, KEY), refs(&new, KEY), "exact refs after write {n}");
            assert_eq!(blocks(&old), blocks(&new), "physical identities after write {n}");
            assert_eq!(series(&new), model);
            edits += 1;
        }
        assert_eq!(edits, 200);
        old.flush_shard_index(1);
        new.flush_shard_index(1);
        assert_eq!(owners(&old), owners(&new));
        assert_eq!(refs(&old, KEY), refs(&new, KEY));
    }

    #[test]
    fn empty_clean_series_edits_and_external_helpers_keep_their_generation_rules() {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), 8);
        let empty = owners(&engine);
        engine.shards.with_state_mut(1, |state| {
            file_series_blocks_after_write(state.unwrap(), 1, "feature", KEY, vec![], vec![], true);
        });
        assert_eq!(owners(&engine), empty, "an empty edit must not invent an owner");
        for at in [10, 20] {
            run(&engine, Command::FeatureAppend { key: KEY.into(), points: vec![point(at, 1)] });
        }
        let before_refs = refs(&engine, KEY);
        engine.shards.with_state_mut(1, |state| {
            let shard = state.unwrap();
            let (start, end) = shard.routing_range();
            let id = block_routing_bucket(KEY, start, end);
            let addresses: Vec<_> = shard.features().get(KEY).unwrap().addresses().cloned().collect();
            assert_eq!(addresses.len(), 2);
            let generation = shard.bucket_index.bucket_map.get(&id).unwrap().dirty_generation;
            let bucket_count = shard.bucket_index.bucket_map.len();
            file_series_blocks_after_write(shard, 1, "feature", KEY, vec![], vec![], false);
            assert_eq!(shard.bucket_index.bucket_map.len(), bucket_count);
            assert_eq!(shard.bucket_index.bucket_map.get(&id).unwrap().dirty_generation, generation);
            // A real clean insertion is still an edit; the old helper also advanced on its first address.
            file_series_blocks_after_write(shard, 1, "feature", KEY, addresses.clone(), vec![], false);
            assert_eq!(shard.bucket_index.bucket_map.get(&id).unwrap().dirty_generation, generation + 1);
            // External dirty callers retain one immediate advance per unique filed address.
            sync_bucket_index_object_blocks_with_mode(shard, 1, "feature", KEY, addresses.clone(), true, false);
            assert_eq!(shard.bucket_index.bucket_map.get(&id).unwrap().dirty_generation, generation + 3);
            // External clean callers retain the existing one-per-call first-address advance.
            sync_bucket_index_object_blocks_with_mode(shard, 1, "feature", KEY, addresses, false, false);
            assert_eq!(shard.bucket_index.bucket_map.get(&id).unwrap().dirty_generation, generation + 4);
        });
        assert_eq!(refs(&engine, KEY), before_refs);
    }

    #[test]
    fn membership_only_removal_from_a_shared_block_has_the_same_epoch_in_both_modes() {
        let _reset = ResetOldPath;
        let left_dir = tempfile::tempdir().unwrap();
        let right_dir = tempfile::tempdir().unwrap();
        let left = open(left_dir.path(), 8);
        let right = open(right_dir.path(), 8);
        for engine in [&left, &right] {
            run(engine, Command::FeatureAppend { key: KEY.into(), points: vec![point(10, 1), point(20, 2)] });
        }
        assert_eq!(blocks(&left).0.len(), 1, "the control must share one physical block");
        let remove = Command::FeatureReplace { key: KEY.into(), start_ms: 10, end_ms: 10, points: vec![] };
        use_the_old_paths(true);
        run(&left, remove.clone());
        use_the_old_paths(false);
        run(&right, remove);
        assert_eq!(series(&left), series(&right));
        assert_eq!(series(&right).keys().copied().collect::<Vec<_>>(), vec![20]);
        assert_eq!(owners(&left), owners(&right));
        assert_eq!(refs(&left, KEY), refs(&right, KEY));
        assert_eq!(blocks(&right).0.len(), 1);
    }

    #[test]
    fn a_real_captured_dump_cannot_clear_a_later_series_edit_in_either_mode() {
        let _reset = ResetOldPath;
        for restate in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), 3);
            for at in [10, 20, 30] {
                run(&engine, Command::FeatureAppend { key: KEY.into(), points: vec![point(at, 1)] });
            }
            let captured = engine.create_bucket_dump_manifest(1, []).unwrap();
            assert!(!captured.bucket_ids.is_empty());
            use_the_old_paths(restate);
            run(&engine, Command::FeatureAppend { key: KEY.into(), points: vec![point(40, 2)] });
            use_the_old_paths(false);
            let changed = owners(&engine);
            assert!(changed.values().any(|row| row.3));
            let changed_refs = refs(&engine, KEY);
            engine.clear_dumped_bucket_dirty_state(1, &captured);
            assert_eq!(owners(&engine), changed, "stale completion changed the later owner, restate={restate}");
            assert_eq!(refs(&engine, KEY), changed_refs);
            let fresh = engine.create_bucket_dump_manifest(1, []).unwrap();
            engine.clear_dumped_bucket_dirty_state(1, &fresh);
            assert!(owners(&engine).values().all(|row| !row.3 && row.1 == 0 && row.2 == 0));
            assert_eq!(refs(&engine, KEY), changed_refs);
            assert_eq!(series(&engine).keys().copied().collect::<Vec<_>>(), vec![20, 30, 40]);
        }
    }

    // Deliberately seed an old filing location with real stored bytes and canonical ref updates.
    fn move_filing(shard: &mut ShardState, key: &str, address: &ElementEntry, destination: u32) {
        let (source, handle) = shard.bucket_index.object_block_refs("feature", key).unwrap().iter()
            .find_map(|r| {
                let bucket = shard.bucket_index.bucket_map.get(&r.routing_bucket)?;
                let block = bucket.block_index.get(&r.block_ref_key)?;
                (block_physical_identity_key(&block.address) == block_physical_identity_key(address))
                    .then_some((r.routing_bucket, r.block_ref_key))
            }).unwrap();
        let (block, generation, wal, index) = {
            let CoreIndex { bucket_map, block_slab_live, .. } = &mut shard.bucket_index;
            let mut bucket = bucket_map.get_mut(&source).unwrap();
            let block = bucket.block_index.remove(&handle, block_slab_live, StaleCause::Overwrite).unwrap();
            (block, bucket.dirty_generation, bucket.first_dirty_wal_sequence, bucket.first_dirty_index_log_sequence)
        };
        shard.bucket_index.remove_object_block_lookup_ref("feature", key, None,
            &BlockLookupRef { routing_bucket: source, block_ref_key: handle });
        let key: std::sync::Arc<str> = key.into();
        let placed = {
            let CoreIndex { bucket_map, block_slab_live, .. } = &mut shard.bucket_index;
            let mut bucket = bucket_map.entry(destination).or_insert_with(|| BucketNode {
                routing_bucket: destination,
                flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
                ..BucketNode::default()
            });
            bucket.dirty_generation = bucket.dirty_generation.max(generation);
            bucket.first_dirty_wal_sequence = wal;
            bucket.first_dirty_index_log_sequence = index;
            bucket.set_dirty(true);
            bucket.object_index.insert(block.object_id(1, &key));
            bucket.insert_block(&key, block.clone(), block_slab_live)
        };
        shard.bucket_index.insert_object_block_lookup(destination, placed, &key, &block);
    }

    #[test]
    fn partial_retirement_then_fallback_advances_each_actual_historical_bucket_once() {
        let _reset = ResetOldPath;
        for (restate, force_scan) in [(false, false), (true, false), (false, true), (true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), 8);
            for at in [10, 20, 30] {
                run(&engine, Command::FeatureAppend { key: KEY.into(), points: vec![point(at, 1)] });
            }
            run(&engine, Command::FeatureAppend { key: "peer-kept".into(), points: vec![point(90, 7)] });
            engine.shards.with_state_mut(1, |state| {
                let shard = state.unwrap();
                let addresses: Vec<_> = shard.features().get(KEY).unwrap().addresses().cloned().collect();
                let peer = shard.features().get("peer-kept").unwrap().addresses().next().unwrap().clone();
                let (start, end) = shard.routing_range();
                let landing = block_routing_bucket(KEY, start, end);
                let peer_landing = block_routing_bucket("peer-kept", start, end);
                let mut historical = landing.wrapping_add(1);
                while historical == landing || historical == peer_landing { historical = historical.wrapping_add(1); }
                move_filing(shard, KEY, &addresses[1], historical);
                move_filing(shard, "peer-kept", &peer, historical);
                let before: BTreeMap<_, _> = shard.bucket_index.bucket_map.iter().map(|(id, bucket)|
                    (*id, (bucket.dirty_generation, bucket.first_dirty_wal_sequence, bucket.first_dirty_index_log_sequence))).collect();
                {
                    let features = shard.features_mut();
                    let held = features.get_mut(KEY).unwrap();
                    crate::engine::timestamped_series::TimestampedSeriesWrite::clear(held, 10);
                    crate::engine::timestamped_series::TimestampedSeriesWrite::clear(held, 20);
                }
                let retired = crate::engine::packed_blocks::addresses_no_point_names(
                    shard.features().get(KEY).unwrap(), addresses[..2].to_vec(),
                );
                assert_eq!(retired.len(), 2);
                assert_eq!(block_physical_identity_key(&retired[0]), block_physical_identity_key(&addresses[0]),
                    "the control must remove a present block before it encounters the historical miss");
                if force_scan {
                    // Keep real ref identities; force only the helper's existing cannot-target branch.
                    shard.bucket_index.object_element_name_block_refs = None;
                }
                use_the_old_paths(restate);
                // The first address is removable at landing; the second is only at the historical location.
                file_series_blocks_after_write(shard, 1, "feature", KEY, vec![], addresses[..2].to_vec(), true);
                use_the_old_paths(false);
                for id in [landing, historical] {
                    let bucket = shard.bucket_index.bucket_map.get(&id).unwrap();
                    assert_eq!(bucket.dirty_generation, before[&id].0 + 1, "bucket {id}, restate={restate}");
                    assert_eq!((bucket.first_dirty_wal_sequence, bucket.first_dirty_index_log_sequence),
                        (before[&id].1, before[&id].2));
                    assert!(bucket.dirty());
                }
                assert_eq!(shard.bucket_index.bucket_map.get(&peer_landing).unwrap().dirty_generation, before[&peer_landing].0);
                let held_refs = shard.bucket_index.object_block_refs("feature", KEY).unwrap();
                assert_eq!(held_refs.len(), 1);
                assert!(held_refs.iter().all(|r| r.routing_bucket == landing));
                assert_eq!(shard.bucket_index.object_block_refs("feature", "peer-kept").unwrap().len(), 1);
            });
            assert_eq!(series(&engine).keys().copied().collect::<Vec<_>>(), vec![30]);
        }
    }
}
