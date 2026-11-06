// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// What a long sequence costs per command once it is SITTING AT ITS BOUND, and what a trim that
// states its boundary recovers.
//
// A trim drops the oldest points and records that they went. Recording each one costs an outcome
// item per point -- the kind, the key, the instant as a decimal component, a derived object id --
// so a series below its bound records P appends per command, and the same series AT its bound
// records P appends AND P removals. The removal count tracks the APPEND WIDTH, not anything the
// caller asked for, and a bounded series is at its bound essentially always, because that is what a
// bound means.
//
// The trim is always a prefix removal over a series whose `points()` promises ascending order, so
// those P records say exactly what one boundary record says. `trim_boundary_record` writes the one.
//
// MEASURED IN WAL FILE GROWTH, i.e. the RECORD cost, which is what the boundary form changes. It
// is not the device cost: a durable append costs about one block whatever the record holds.
// Run with TS_WAL_PREALLOCATE=0, or file length measures preallocation steps instead of records --
// at the default a 256 KiB segment is claimed up front and a phase that crosses no boundary
// reports 0 B/command, which is how this harness first read 0.00x and -524 B/point.
//
// Run:
//   TS_WAL_PREALLOCATE=0 cargo run --release --bin sequence_trim_cost_harness -- [bound] [points]

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use temporalstore_rust::control::{Config, SetConfigRequest};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::types::{Command, CommandResponse, ExecuteRequest, FeaturePoint};

fn wal_bytes(index_dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(index_dir.join("wals")) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

fn live_points(engine: &TemporalEngine, key: &str, limit: usize) -> usize {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(limit),
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => points.len(),
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

struct Arm {
    /// WAL bytes per command from the FILE's growth.
    below: f64,
    at: f64,
    /// The same two, from the engine's own record-byte counter. Reported beside the file figures
    /// rather than instead of them: they disagreed by 5x once, and an average of two instruments
    /// is not a measurement.
    below_counted: f64,
    at_counted: f64,
}

fn counted_bytes(engine: &TemporalEngine) -> u64 {
    engine.write_ahead_log_store().stats(1).bytes_written
}

fn measure(bound: usize, per_command: usize, boundary_record: bool) -> Arm {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let root: PathBuf =
        std::env::temp_dir().join(format!("ts-trim-{}-{}", std::process::id(), stamp));
    std::fs::create_dir_all(&root).expect("work dir");
    let index_dir = root.join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        root.join("cache"),
        root.join("blocks"),
        &index_dir,
    );
    engine.load_shard(1);

    let mut extend_config = std::collections::BTreeMap::new();
    if boundary_record {
        extend_config.insert("trim_boundary_record".to_string(), "on".to_string());
    }
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    feature_max_size: bound,
                    extend_config,
                    ..Config::default()
                },
            })
            .ok,
        "the bound and the gate have to be the ones being measured"
    );

    let mut next_command: u64 = 0;
    let append = |next_command: &mut u64| {
        let base = 1_000_000 + *next_command * per_command as u64;
        let points: Vec<FeaturePoint> = (0..per_command as u64)
            .map(|offset| FeaturePoint {
                timestamp_ms: base + offset,
                value: vec![(offset % 251) as u8],
            })
            .collect();
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend { key: "seq".to_string(), points },
        });
        assert!(
            outcome.status.ok,
            "append {} failed: {:?}",
            *next_command, outcome.response
        );
        *next_command += 1;
    };

    // PHASE A -- filling. Exactly bound/per_command commands, so not one of them trims.
    let fill_commands = bound / per_command;
    let a_start = wal_bytes(&index_dir);
    let a_start_counted = counted_bytes(&engine);
    for _ in 0..fill_commands {
        append(&mut next_command);
    }
    let a_end = wal_bytes(&index_dir);
    let a_end_counted = counted_bytes(&engine);

    // A DENOMINATOR BEFORE ATTRIBUTING ANYTHING: if the series is not exactly at its bound then
    // phase B measures what phase A did, and the difference is noise rather than the trim.
    assert_eq!(
        live_points(&engine, "seq", bound * 4),
        bound,
        "the series must be exactly at its bound before phase B"
    );

    // PHASE B -- at the bound, so every command appends per_command and trims per_command.
    let b_commands = 20;
    let b_start = wal_bytes(&index_dir);
    let b_start_counted = counted_bytes(&engine);
    for _ in 0..b_commands {
        append(&mut next_command);
    }
    let b_end = wal_bytes(&index_dir);
    let b_end_counted = counted_bytes(&engine);

    // Still at the bound, which is what proves the trim ran in phase B at all.
    assert_eq!(
        live_points(&engine, "seq", bound * 4),
        bound,
        "phase B must have trimmed, holding the series at its bound"
    );

    let below = (a_end - a_start) as f64 / fill_commands as f64;
    let at = (b_end - b_start) as f64 / b_commands as f64;

    // A PREALLOCATED SEGMENT IS NOT A RECORD. At the default the WAL claims 256 KiB up front, and
    // file length then reports a step function: refuse rather than publish that as a record cost.
    assert!(
        below < 100.0 * per_command as f64,
        "{below:.1} WAL B/command for {per_command} points is a preallocation step, not records \
         -- run with TS_WAL_PREALLOCATE=0"
    );

    let below_counted = (a_end_counted - a_start_counted) as f64 / fill_commands as f64;
    let at_counted = (b_end_counted - b_start_counted) as f64 / b_commands as f64;

    let _ = std::fs::remove_dir_all(&root);
    Arm { below, at, below_counted, at_counted }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let bound: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(500);
    let per_command: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(50);
    assert!(
        bound % per_command == 0,
        "bound must be a whole number of commands so phase A ends exactly at it"
    );

    let per_point = measure(bound, per_command, false);
    let boundary = measure(bound, per_command, true);

    println!("\nONE LONG-SEQUENCE APPEND OF {per_command} POINTS, bound {bound}");
    println!("  {:<34} {:>12} {:>12}", "", "per point", "boundary");
    println!(
        "  {:<34} {:>12.1} {:>12.1}",
        "below the bound (no trim)", per_point.below, boundary.below
    );
    println!(
        "  {:<34} {:>12.1} {:>12.1}",
        "at the bound (WAL B/command)", per_point.at, boundary.at
    );
    println!(
        "  {:<34} {:>12.1} {:>12.1}",
        "the trim adds",
        per_point.at - per_point.below,
        boundary.at - boundary.below
    );
    let saved = per_point.at - boundary.at;
    println!(
        "\n  AT THE BOUND: {:.1} -> {:.1} WAL B/command, {:.1} B saved ({:.1}%)",
        per_point.at,
        boundary.at,
        saved,
        if per_point.at > 0.0 { 100.0 * saved / per_point.at } else { 0.0 }
    );
    println!(
        "  the trim's own share: {:.0}% -> {:.0}% of the command",
        100.0 * (per_point.at - per_point.below) / per_point.at,
        100.0 * (boundary.at - boundary.below) / boundary.at
    );

    // THE SECOND INSTRUMENT, side by side. The file holds what was written; the counter holds what
    // the engine says it wrote. Where they disagree, the difference is itself the finding.
    println!("\n  SAME RUN, the engine's own record-byte counter:");
    println!(
        "  {:<34} {:>12.1} {:>12.1}",
        "below the bound", per_point.below_counted, boundary.below_counted
    );
    println!(
        "  {:<34} {:>12.1} {:>12.1}",
        "at the bound", per_point.at_counted, boundary.at_counted
    );
    let saved_counted = per_point.at_counted - boundary.at_counted;
    println!(
        "  {:<34} {:>12.1} {:>12.1}   saved {:.1} B ({:.1}%)",
        "the trim adds",
        per_point.at_counted - per_point.below_counted,
        boundary.at_counted - boundary.below_counted,
        saved_counted,
        if per_point.at_counted > 0.0 { 100.0 * saved_counted / per_point.at_counted } else { 0.0 }
    );
    println!(
        "\n  file growth says the trim is {:.0}% of the command; the counter says {:.0}%.",
        100.0 * (per_point.at - per_point.below) / per_point.at,
        100.0 * (per_point.at_counted - per_point.below_counted) / per_point.at_counted
    );
    println!("  Quote whichever is defended, never the average of the two.");
}
