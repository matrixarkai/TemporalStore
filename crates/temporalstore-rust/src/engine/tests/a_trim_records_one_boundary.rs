// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A TRIM RECORDS ITS BOUNDARY, NOT ONE RECORD FOR EVERY POINT IT DROPPED.
//!
//! A bounded series is at its bound essentially always -- that is what a bound means -- and at the
//! bound every command appends P points and drops P. Recording each dropped point costs an outcome
//! item apiece, so the removals track the APPEND WIDTH rather than anything the caller asked for.
//!
//! Measured by `sequence_trim_cost_harness`, with WAL preallocation off so the file's growth tracks
//! records rather than 256 KiB segment steps. The engine's own record-byte counter agrees with the
//! file to the decimal on every row, which is why one number is quoted and not two:
//!
//! ```text
//! bound  points/cmd  below   at bound: per-point   at bound: boundary   trim's share
//!    10          10  163.0                 207.3                187.2    21% ->  13%
//!   500          50  301.9                 396.9                326.4    24% ->   8%
//!  1000         200  836.4                1154.8                862.0    28% ->   3%
//! ```
//!
//! The shape is the point: the per-point form's trim cost grows with the append (44 -> 95 -> 318 B)
//! while the boundary form's stays at **~25 B whatever the width**, because it is one record.
//!
//! # WHAT THESE TESTS HOLD, AND WHY EACH IS NEEDED
//!
//! Replay equivalence alone would pass even if the writer had quietly kept staging per-point
//! records; a byte comparison alone would pass on a writer that recorded nothing at all. Neither
//! is sufficient and both are cheap.
//!
//! The spelling is pinned separately because it is the contract between the writer and the apply
//! path, and because an older build has to DECLINE the item rather than mis-apply it: an absent
//! component on a removal already means "the whole series went", and a build that read a boundary
//! that way would delete a live series instead of leaving points to linger.

use super::*;

/// Divisible by both widths below, so each fills the series exactly and phase A never trims.
const BOUND: usize = 40;
/// The two append widths the scaling claim is measured over, the wider four times the narrower so
/// the step cannot be noise.
const NARROW: usize = 5;
const WIDE: usize = 20;
const FIRST_INSTANT: u64 = 1_000_000;
/// One instant from the batch that gets dropped. The middle rather than an edge: an off-by-one at
/// either end would still leave this one dropped.
const DROPPED_NEEDLE: u64 = FIRST_INSTANT + 2;

struct Arm {
    engine: TemporalEngine,
    index_dir: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

fn arm(boundary_record: bool) -> Arm {
    let dir = tempfile::tempdir().unwrap();
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        &index_dir,
    );
    engine.load_shard(1);
    let mut extend_config = std::collections::BTreeMap::new();
    if boundary_record {
        extend_config.insert("trim_boundary_record".to_string(), "on".to_string());
    }
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    feature_max_size: BOUND,
                    extend_config,
                    ..Default::default()
                },
            })
            .ok,
        "the bound and the gate have to be the ones under test"
    );
    Arm { engine, index_dir, _dir: dir }
}

fn append_n(engine: &TemporalEngine, base: u64, count: usize) {
    let points = (0..count as u64)
        .map(|offset| FeaturePoint { timestamp_ms: base + offset, value: vec![offset as u8] })
        .collect();
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend { key: "seq".to_string(), points },
    });
    assert!(outcome.status.ok, "append at {base} failed: {:?}", outcome.response);
}

fn live_instants(engine: &TemporalEngine) -> Vec<u64> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "seq".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(BOUND * 8),
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => {
            points.iter().map(|point| point.timestamp_ms).collect()
        }
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

/// Occurrences of a byte pattern in the WAL.
///
/// Sound ONLY for a pattern stored as text. A numeric component is not: `numeric_component` renders
/// it into the protobuf `timestamp_ms` field as a varint, so a per-point removal's instant never
/// appears here as digits while the non-numeric boundary spelling does. So this shows that the
/// boundary form FIRED and is never used to count per-point removals -- counting those this way
/// would be asymmetric by construction and would report an encoding difference as a saving. The
/// first version of this test did exactly that and failed on its own denominator.
fn wal_occurrences(index_dir: &std::path::Path, needle: &str) -> usize {
    let path = index_dir.join("wals").join("shard-1.wal.bin");
    let bytes = std::fs::read(&path).unwrap_or_else(|err| panic!("reading {path:?}: {err}"));
    let needle = needle.as_bytes();
    if needle.is_empty() || bytes.len() < needle.len() {
        return 0;
    }
    bytes.windows(needle.len()).filter(|window| *window == needle).count()
}

/// The engine's own count of record bytes written to this shard's WAL.
///
/// Not the file's length: the WAL preallocates a 256 KiB segment, so length is a step function and
/// a phase crossing no boundary measures as zero -- which is how the harness first reported a
/// negative trim cost.
fn wal_record_bytes(engine: &TemporalEngine) -> u64 {
    engine.write_ahead_log_store().stats(1).bytes_written
}

/// Record bytes for the one command that trims, at a given append width.
fn trimming_command_bytes(per_command: usize, boundary_record: bool) -> u64 {
    assert_eq!(BOUND % per_command, 0, "the width has to fill the bound exactly");
    let a = arm(boundary_record);
    for round in 0..(BOUND / per_command) {
        append_n(&a.engine, FIRST_INSTANT + (round * per_command) as u64, per_command);
    }
    let filled = wal_record_bytes(&a.engine);
    assert_eq!(
        live_instants(&a.engine).len(),
        BOUND,
        "the series must be exactly at its bound before the trimming command \
         (width={per_command}, gate={boundary_record})"
    );

    append_n(&a.engine, FIRST_INSTANT + BOUND as u64, per_command);
    let trimmed = wal_record_bytes(&a.engine);
    assert_eq!(
        live_instants(&a.engine).len(),
        BOUND,
        "the trim has to have run, or this measures two identical commands \
         (width={per_command}, gate={boundary_record})"
    );
    // DENOMINATOR: the counter has to advance for both commands, or the difference is between two
    // nothings.
    assert!(
        trimmed > filled && filled > 0,
        "the record-byte counter must advance (width={per_command}, gate={boundary_record}): \
         {filled} then {trimmed}"
    );
    // And the mechanism, so a byte reduction cannot be credited to the boundary form when
    // something else moved: exactly one boundary record with the gate on, none with it off.
    assert_eq!(
        wal_occurrences(&a.index_dir, packed_blocks::TRIM_BOUNDARY_MARK),
        usize::from(boundary_record),
        "one boundary record when the gate is on, none when it is off \
         (width={per_command}, gate={boundary_record})"
    );
    trimmed - filled
}

/// The saving SCALES WITH THE APPEND WIDTH, which is what the mechanism claims.
///
/// Not "the saving is X% of the command": that share moves with the bound and the width (21% to 28%
/// across the measured rows), and an absolute threshold would be a number pinned for no reason.
/// What the mechanism says is width-proportional -- P removal records become ONE -- so widening the
/// append must widen the saving while the boundary form's own trim cost stays flat.
#[test]
fn a_trim_records_one_boundary_not_one_record_per_dropped_point() {
    let narrow_per_point = trimming_command_bytes(NARROW, false);
    let narrow_boundary = trimming_command_bytes(NARROW, true);
    let wide_per_point = trimming_command_bytes(WIDE, false);
    let wide_boundary = trimming_command_bytes(WIDE, true);

    assert!(
        narrow_boundary < narrow_per_point && wide_boundary < wide_per_point,
        "stating the boundary must write fewer record bytes at BOTH widths: \
         {NARROW} points {narrow_per_point} -> {narrow_boundary}, \
         {WIDE} points {wide_per_point} -> {wide_boundary}"
    );

    let narrow_saving = narrow_per_point - narrow_boundary;
    let wide_saving = wide_per_point - wide_boundary;
    // WIDE is four times NARROW. A saving that did NOT grow with it would mean the boundary record
    // replaces something of fixed size -- not the removals -- and the claim would be wrong.
    assert!(
        wide_saving > narrow_saving * 2,
        "the saving must grow with the append width, because the removals it replaces do: \
         {NARROW} points saved {narrow_saving} B, {WIDE} points saved {wide_saving} B \
         (expected more than {} B)",
        narrow_saving * 2
    );
}

/// The correctness half: a boundary record has to rebuild the same series the per-point records do.
#[test]
fn a_boundary_record_replays_to_the_same_series() {
    let mut rebuilt = Vec::new();
    let mut lived = Vec::new();
    for boundary_record in [false, true] {
        let a = arm(boundary_record);
        append_n(&a.engine, FIRST_INSTANT, BOUND);
        append_n(&a.engine, FIRST_INSTANT + BOUND as u64, BOUND);

        let before = live_instants(&a.engine);
        assert_eq!(
            before.len(),
            BOUND,
            "the series must be AT its bound before the reload, or the trim never ran and this \
             test is about nothing (gate={boundary_record})"
        );
        assert!(
            !before.contains(&DROPPED_NEEDLE),
            "the first batch must have been trimmed away (gate={boundary_record})"
        );

        // Rebuild the shard from what was RECORDED, which is the whole question.
        a.engine.unload_shard(1);
        a.engine.load_shard(1);
        let after = live_instants(&a.engine);

        assert_eq!(
            after, before,
            "replay must rebuild exactly the live series (gate={boundary_record})"
        );
        assert!(
            !after.contains(&DROPPED_NEEDLE),
            "a trimmed point must not come back on reload -- the specific failure a boundary \
             record risks, and why the apply path resolves it to the same per-instant operation \
             (gate={boundary_record})"
        );
        rebuilt.push(after);
        lived.push(before);
    }
    assert_eq!(
        rebuilt[0], rebuilt[1],
        "the two record shapes must replay to the same series, or they do not say the same thing"
    );
    assert_eq!(lived[0], lived[1], "and the live shards must have matched to begin with");
}

/// The spelling is the contract between the writer and the apply path, and it is what makes an
/// older build DECLINE the record instead of mis-reading it.
#[test]
fn a_boundary_component_is_not_read_as_an_instant() {
    assert_eq!(
        packed_blocks::timestamped_boundary_through(Some(
            &packed_blocks::timestamped_boundary_component(42)
        )),
        Some(42),
        "a boundary component has to round trip"
    );
    // A plain instant is NOT a boundary, which keeps every existing per-point removal on the
    // per-point path.
    assert_eq!(packed_blocks::timestamped_boundary_through(Some("42")), None);
    assert_eq!(packed_blocks::timestamped_boundary_through(Some("")), None);
    assert_eq!(packed_blocks::timestamped_boundary_through(None), None);
    // Marked but not a number is not a boundary either, rather than a boundary at zero.
    assert_eq!(packed_blocks::timestamped_boundary_through(Some("<=x")), None);

    // AND IT IS NOT AN ABSENT COMPONENT, which already means "the whole series went". A build
    // without the apply path reads this component, fails to parse it as an instant and declines the
    // item, so the trimmed points linger until the next trim drops them again. Reading it as a
    // whole-series removal would delete a LIVE series, which is why the boundary is spelled into
    // the component rather than left out of it.
    assert!(
        !packed_blocks::timestamped_boundary_component(42).is_empty(),
        "a boundary record must carry a component"
    );
    assert!(
        packed_blocks::timestamped_boundary_component(42).parse::<u64>().is_err(),
        "an older build must FAIL to read a boundary as an instant, not read it as a different one"
    );
}
