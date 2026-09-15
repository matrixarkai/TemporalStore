// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the serving read fast path does while it holds the shard-table read guard.
#![allow(clippy::all)]
use super::*;

/// Fields on the one hash the measurement reads. Chosen larger than any plausible per-call
/// constant so a hold that scales with the value's WIDTH cannot be mistaken for a fixed one.
const FIELDS: usize = 64;

/// A shard holding one wide hash whose pages are all OUT of cache.
fn cold_wide_hash(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..FIELDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: format!("f{index:04}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Without this every page is already resident and the read below never reaches the block
    // store -- the count would be zero and the claim would hold for the wrong reason.
    let _ = engine.cache.invalidate_shard(1);
    engine
}

/// One `HashGetAll` down the durable serving route, with the page reads counted.
///
/// Returns the tally and the FIELD COUNT the response actually carried. Those are two different
/// claims -- "N reads happened under the guard" and "the value had N fields" -- and a single
/// total passes when two errors cancel, so they are asserted apart below.
fn measured_hash_get_all(
    engine: &TemporalEngine,
) -> (crate::engine::MaintenanceBlockReadCounts, usize) {
    crate::engine::reset_maintenance_block_read_counts();
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGetAll {
            key: "wide".to_string(),
        },
    });
    assert!(response.status.ok, "read: {:?}", response.status);
    let fields = match response.response {
        CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("expected HashEntries, got {other:?}"),
    };
    (crate::engine::maintenance_block_read_counts(), fields)
}

/// The serving read fast path reads its pages AFTER the shard-table read guard drops.
///
/// A read guard is the easy one to leave in place, because admitting other readers does not look
/// like exclusion. It is: it excludes every WRITER on the shard for as long as it is held, and
/// this path held it across one block-store read PER FIELD. The window therefore scaled with the
/// WIDTH of the value being served -- a 64-field hash stopped all writes on the shard for 64
/// reads -- which is a property of the request, not of any per-round budget.
///
/// TWO arms, ONE process, the same fixture:
///   * UNDER THE GUARD -- where the reads used to be. The positive control: the counter has to
///     produce a non-zero number inside the region, or the zero below is a broken counter rather
///     than a shortened hold.
///   * SHIPPED -- the addresses are chosen under the guard, the pages are read after it drops.
/// Both arms must read the same pages, which is what stops this from being satisfied by a read
/// that found everything cached or served nothing.
#[test]
fn the_serving_fast_path_reads_pages_after_the_shard_guard_drops() {
    let control_dir = tempfile::tempdir().unwrap();
    let control_engine = cold_wide_hash(control_dir.path());
    crate::engine::shard_write_guard::serve_fast_path_under_shard_guard_for_test(true);
    let (control, control_fields) = measured_hash_get_all(&control_engine);
    crate::engine::shard_write_guard::serve_fast_path_under_shard_guard_for_test(false);

    let shipped_dir = tempfile::tempdir().unwrap();
    let shipped_engine = cold_wide_hash(shipped_dir.path());
    let (shipped, shipped_fields) = measured_hash_get_all(&shipped_engine);

    eprintln!(
        "[serving read] under the guard: {} of {} page read(s) under the guard, \
{control_fields} field(s) served",
        control.block_reads_under_guard, control.block_reads_total,
    );
    eprintln!(
        "[serving read] shipped:         {} of {} page read(s) under the guard, \
{shipped_fields} field(s) served",
        shipped.block_reads_under_guard, shipped.block_reads_total,
    );

    // DENOMINATOR ONE -- the width. Asserted on its own: a hold that scales with the field count
    // is only demonstrated against a value that HAS fields, and "0 reads under the guard" is
    // satisfied perfectly by a hash that served none.
    assert_eq!(
        control_fields, FIELDS,
        "the control arm served {control_fields} field(s), not {FIELDS}",
    );
    assert_eq!(
        shipped_fields, FIELDS,
        "the shipped arm served {shipped_fields} field(s), not {FIELDS}",
    );

    // DENOMINATOR TWO -- the reads. Asserted apart from the width above, so the two cannot
    // cancel, and apart from the under-guard tally below, so a path that read NOTHING cannot
    // satisfy the claim that it read nothing under the guard.
    assert_eq!(
        control.block_reads_total, FIELDS as u64,
        "the control arm went to the block store {} time(s) for a {FIELDS}-field hash; if this \
is zero the fixture was still cached and neither arm is a subject",
        control.block_reads_total,
    );
    assert_eq!(
        shipped.block_reads_total, FIELDS as u64,
        "the shipped arm went to the block store {} time(s) for a {FIELDS}-field hash",
        shipped.block_reads_total,
    );

    // POSITIVE CONTROL: the counter can see a read inside the region, because here are 64.
    assert_eq!(
        control.block_reads_under_guard, FIELDS as u64,
        "the control arm reads every page inside the guarded region and the counter saw {} of \
{}. The measurement is broken, not the code under it: the assertion below would pass against an \
engine that had stopped counting entirely",
        control.block_reads_under_guard, control.block_reads_total,
    );

    // THE ASSERTION THE REGION CHANGE MADE.
    assert_eq!(
        shipped.block_reads_under_guard, 0,
        "the serving read fast path read {} of {} pages off the block store while still holding \
the shard-table read guard. A read guard excludes every writer, so serving one wide value stops \
all writes on the shard for the length of that I/O -- once per FIELD, so the window grows with \
the value. Copy the addresses out under the guard, drop it, then read. If a new step genuinely \
needs the shard itself, say why here rather than widening the region back",
        shipped.block_reads_under_guard,
        shipped.block_reads_total,
    );
}

/// Releasing the guard before the reads does not let a concurrent writer corrupt a served value.
///
/// This is the other half of the change: the reads moved OUT of the excluded region, so a writer
/// can now land between the address lookup and the page read. What must still hold is that every
/// field is present and every value is a WHOLE value some writer actually wrote -- never absent,
/// never another record's bytes, never a mix.
#[test]
fn a_concurrent_writer_cannot_tear_a_value_the_fast_path_serves() {
    const ROUNDS: usize = 40;

    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(cold_wide_hash(dir.path()));

    let writer_engine = std::sync::Arc::clone(&engine);
    let writer = std::thread::spawn(move || {
        for round in 0..ROUNDS {
            let marker = if round % 2 == 0 { b'w' } else { b'v' };
            for index in 0..FIELDS {
                let response = writer_engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "wide".to_string(),
                        field: format!("f{index:04}"),
                        value: vec![marker; 64],
                    },
                });
                assert!(response.status.ok, "writer: {:?}", response.status);
            }
        }
    });

    let mut reads = 0usize;
    let mut values_checked = 0usize;
    while !writer.is_finished() {
        let response = engine.execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGetAll {
                key: "wide".to_string(),
            },
        });
        assert!(response.status.ok, "reader: {:?}", response.status);
        let CommandResponse::HashEntries { entries } = response.response else {
            panic!("expected HashEntries");
        };
        assert_eq!(
            entries.len(),
            FIELDS,
            "a read served {} of {FIELDS} field(s) while a writer was replacing them -- the \
address snapshot is no longer atomic across fields",
            entries.len(),
        );
        for (field, value) in &entries {
            assert_eq!(
                value.len(),
                64,
                "field {field} came back {} byte(s), not 64 -- a partial or foreign record",
                value.len(),
            );
            assert!(
                value.iter().all(|byte| *byte == b'v')
                    || value.iter().all(|byte| *byte == b'w'),
                "field {field} came back as a MIX of two writes, so the bytes served did not \
come from a single record",
            );
            values_checked += 1;
        }
        reads += 1;
    }
    writer.join().expect("writer thread panicked");

    eprintln!(
        "[serving read] {reads} concurrent read(s) of a {FIELDS}-field hash, \
{values_checked} value(s) checked, against {ROUNDS} rounds of rewrites",
    );

    // DENOMINATOR: a loop that never ran proves nothing about concurrency.
    assert!(
        reads > 0 && values_checked > 0,
        "the reader served {reads} read(s) and checked {values_checked} value(s), so nothing \
above was exercised against a concurrent writer",
    );
}
