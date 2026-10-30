// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE USER-BYTE WRITE RATIO: COMPRESSIBILITY CANCELS, AND COVERAGE IS PUBLISHED.
//!
//! `user_write_amplification_ppm` is `logical_bytes_written / live_user_bytes` -- uncompressed
//! bytes written per uncompressed byte kept. It is the one write ratio whose halves are both in
//! the caller's units, which is what makes it measure REWRITE where
//! `storage_write_amplification_ppm` measures the CODEC.
//!
//! # THE PROPERTY THAT DISTINGUISHES IT, ASSERTED ON TWO STORES
//!
//! Two stores are written with the SAME user bytes and wildly different compressibility. Their
//! storage-write figures must differ -- that is the codec showing up -- and this ratio must be
//! IDENTICAL, because compression appears in neither of its halves. One store alone cannot show
//! that: a single compressibility makes every ratio agree by construction, which is the trap the
//! denominator arm hit first.
//!
//! # AND COVERAGE IS A FIRST-CLASS TERM
//!
//! The size class is installed by the string write path. An object written another way, or decoded
//! from an image predating the field, carries none and contributes to neither term -- which makes
//! the ratio read HIGH, not low, and nothing in the ppm value says so. So the arm asserts the
//! coverage count equals the live object count on a corpus written entirely through that path, and
//! asserts it FALLS when a row is deleted. A ratio whose denominator can be silently partial needs
//! its completeness published beside it.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_user_byte_write_ratio -- --nocapture

use crate::engine::TemporalEngine;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 300;
/// EXACTLY REPRESENTABLE AS A SIZE CLASS -- `704 == (16 + 6) << 5` -- so every figure below is an
/// equality rather than a tolerance.
///
/// This is deliberate and it narrows what the arm covers: at 700 bytes the class rounds to 704 and
/// the ratio reads 2.983x instead of 3.000x, which is the encoding behaving correctly and would
/// force every assertion here into a band. The rounding itself is measured by
/// `what_a_live_user_bytes_denominator_would_cost` on lengths chosen to fall BETWEEN classes; this
/// arm tests the plumbing -- that the halves are in user units, that coverage is counted, that a
/// delete subtracts -- and wants exact arithmetic to do it.
const VALUE_LEN: usize = 704;

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000_000) / denominator
}

fn noisy(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

fn engine_in(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

/// Write `rounds` passes over the same `KEYS` keys, each value `VALUE_LEN` bytes.
fn seed(engine: &TemporalEngine, rounds: usize, compressible: bool) {
    for round in 0..rounds {
        for index in 0..KEYS {
            let value = if compressible {
                vec![b'v'; VALUE_LEN]
            } else {
                noisy((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ round as u64, VALUE_LEN)
            };
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: format!("uw-{index:06}"), value },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    }
}

#[test]
fn the_user_byte_write_ratio() {
    const ROUNDS: usize = 3;
    let live_user_bytes_expected = (KEYS * VALUE_LEN) as u64;

    let flat_dir = tempfile::tempdir().expect("tempdir");
    let noisy_dir = tempfile::tempdir().expect("tempdir");
    let flat = engine_in(flat_dir.path());
    let sharp = engine_in(noisy_dir.path());
    seed(&flat, ROUNDS, true);
    seed(&sharp, ROUNDS, false);

    let flat_stats = flat.shard_stats(1).expect("stats").storage.clone();
    let sharp_stats = sharp.shard_stats(1).expect("stats").storage.clone();

    println!();
    println!("  THE SAME USER BYTES, TWO COMPRESSIBILITIES");
    println!(
        "    {:<42} {:>14} {:>14}",
        "", "compressible", "incompressible"
    );
    for (label, a, b) in [
        ("logical (user) bytes written", flat_stats.logical_bytes_written, sharp_stats.logical_bytes_written),
        ("live_user_bytes", flat_stats.live_user_bytes, sharp_stats.live_user_bytes),
        ("live_objects_with_logical_size", flat_stats.live_objects_with_logical_size, sharp_stats.live_objects_with_logical_size),
        ("live_block_bytes (stored)", flat_stats.live_block_bytes, sharp_stats.live_block_bytes),
    ] {
        println!("    {:<42} {:>14} {:>14}", label, a, b);
    }
    println!(
        "    {:<42} {:>13.3}x {:>13.3}x",
        "storage_write_amplification (the CODEC)",
        flat_stats.storage_write_amplification_ppm as f64 / 1_000_000.0,
        sharp_stats.storage_write_amplification_ppm as f64 / 1_000_000.0
    );
    println!(
        "    {:<42} {:>13.3}x {:>13.3}x",
        "user_write_amplification (the REWRITE)",
        flat_stats.user_write_amplification_ppm as f64 / 1_000_000.0,
        sharp_stats.user_write_amplification_ppm as f64 / 1_000_000.0
    );
    println!();

    // --- THE TWO STORES REALLY DID COMPRESS DIFFERENTLY, or the invariance below is vacuous.
    assert!(
        sharp_stats.storage_write_amplification_ppm
            > flat_stats.storage_write_amplification_ppm * 2,
        "the two stores reported storage-write figures of {} and {} ppm. They must differ \
         materially, or this pair cannot show that compression cancels out of the user ratio.",
        flat_stats.storage_write_amplification_ppm,
        sharp_stats.storage_write_amplification_ppm
    );

    // --- AND THE USER RATIO IS THE SAME ON BOTH. This is the property; everything else here
    // --- supports it.
    assert_eq!(
        flat_stats.user_write_amplification_ppm, sharp_stats.user_write_amplification_ppm,
        "the user-byte write ratio read {} ppm on the compressible store and {} on the \
         incompressible one. Both wrote the same user bytes over the same live set, so compression \
         must cancel -- if it does not, one of the halves is in stored units.",
        flat_stats.user_write_amplification_ppm, sharp_stats.user_write_amplification_ppm
    );

    // --- IT READS THE NUMBER OF ROUNDS, because each round rewrote every key once.
    assert_eq!(
        (ROUNDS as u64) * 1_000_000,
        sharp_stats.user_write_amplification_ppm,
        "{ROUNDS} passes over the same keys must read {ROUNDS}.000x and {} ppm was published",
        sharp_stats.user_write_amplification_ppm
    );

    // --- COVERAGE IS COMPLETE on a corpus written entirely through the string path, and the
    // --- denominator is the user bytes rather than the stored ones.
    assert_eq!(
        KEYS as u64, sharp_stats.live_objects_with_logical_size,
        "{KEYS} keys were written through the string path, so every live object should carry a \
         size class and {} do",
        sharp_stats.live_objects_with_logical_size
    );
    assert_eq!(
        live_user_bytes_expected, sharp_stats.live_user_bytes,
        "the live user bytes should be {KEYS} x {VALUE_LEN} = {live_user_bytes_expected} and \
         {} was published. {VALUE_LEN} is exactly representable as a size class, so this is an \
         equality rather than a tolerance.",
        sharp_stats.live_user_bytes
    );

    // --- RE-DERIVABLE FROM ITS OWN PUBLISHED TERMS.
    assert_eq!(
        ppm(sharp_stats.logical_bytes_written, sharp_stats.live_user_bytes),
        sharp_stats.user_write_amplification_ppm,
        "the published ratio is not its own published terms: {} over {} should be {} ppm",
        sharp_stats.logical_bytes_written,
        sharp_stats.live_user_bytes,
        ppm(sharp_stats.logical_bytes_written, sharp_stats.live_user_bytes)
    );

    // --- A DELETE TAKES ITS BYTES AND ITS COVERAGE WITH IT. Without this the denominator would
    // --- only ever rise and a deleted object would keep contributing to a figure that claims to
    // --- describe what is live.
    let response = sharp.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringDelete { key: format!("uw-{:06}", 0) },
    });
    assert!(response.status.ok, "delete: {:?}", response.status);
    let after = sharp.shard_stats(1).expect("stats").storage.clone();
    println!(
        "  after deleting one key: live_user_bytes {} -> {}, covered {} -> {}",
        sharp_stats.live_user_bytes,
        after.live_user_bytes,
        sharp_stats.live_objects_with_logical_size,
        after.live_objects_with_logical_size
    );
    println!();
    assert_eq!(
        sharp_stats.live_user_bytes - VALUE_LEN as u64,
        after.live_user_bytes,
        "deleting one {VALUE_LEN}-byte object should drop the live user bytes by exactly that, \
         {} -> {}",
        sharp_stats.live_user_bytes,
        after.live_user_bytes
    );
    assert_eq!(
        sharp_stats.live_objects_with_logical_size - 1,
        after.live_objects_with_logical_size,
        "deleting one object should drop the coverage count by one, {} -> {}",
        sharp_stats.live_objects_with_logical_size,
        after.live_objects_with_logical_size
    );
}

/// NO STRING ROW IS REMOVED OUTSIDE ITS DOOR.
///
/// `remove_string_row` maintains `live_user_bytes` and `live_objects_with_logical_size`. A caller
/// that reaches `strings_mut().remove(..)` instead removes the row and leaves both totals claiming
/// an object that is gone -- which makes `user_write_amplification_ppm` read LOW, and nothing in
/// the published value says so. That is how this started: the totals were hooked on
/// `take_string_address` alone and a `StringDelete` subtracted nothing.
///
/// A SOURCE SCAN CATCHES A SPELLING, NOT A CONCEPT, and that is stated rather than hoped. It does
/// not stop a caller binding the map first (`let t = shard.strings_mut(); t.remove(..)`), and the
/// enforced version is the `RecordedMap` shape `hashes`, `sets`, `zsets` and `lists` already use,
/// where the mutation cannot be spelled without a witness. This holds the convention until the map
/// is closed that way.
///
/// THE POSITIVE CONTROL IS THE DOOR ITSELF. If the needle stops matching the one legitimate call,
/// the scan has gone blind and would report zero for a crate full of violations -- so the test
/// asserts it finds that one before it asserts it finds nothing else.
#[test]
fn no_string_row_is_removed_outside_its_door() {
    use std::path::Path;

    const NEEDLE: &str = "strings_mut().remove(";
    /// The door's own definition, which is what makes its file the one allowed to spell the needle.
    ///
    /// THE DOOR IS FOUND BY ITS FUNCTION, NOT BY A FILE NAME. This held
    /// `const DOOR: &str = "engine/state.rs"`, and a suite-wide rename moved that file to
    /// `engine_storage_state.rs` -- which did not make the guard wrong, it made the guard FAIL,
    /// because its positive control could no longer find the one legitimate call. That is the
    /// control doing its job, and it is also a maintenance burden nobody signed up for. So the
    /// door is now whichever file defines `remove_string_row`, which a rename carries with it.
    const DOOR_DEFINITION: &str = "fn remove_string_row";

    fn walk(dir: &Path, out: &mut Vec<(String, usize, String)>) {
        for entry in std::fs::read_dir(dir).expect("the crate source is readable") {
            let path = entry.expect("a readable dir entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                for (number, line) in text.lines().enumerate() {
                    let trimmed = line.trim_start();
                    // A COMMENT IS NOT A CALL, and this arm's own prose names the needle twice --
                    // as does the declaration below it. Skipping comments and the declaration
                    // keeps this file IN SCOPE for real code rather than excluding it wholesale,
                    // which would have made the one file most likely to grow a violation the one
                    // place the scan could not see.
                    if trimmed.starts_with("//") || trimmed.contains("const NEEDLE") {
                        continue;
                    }
                    if line.contains(NEEDLE) {
                        out.push((
                            path.to_string_lossy().to_string(),
                            number + 1,
                            line.trim().to_string(),
                        ));
                    }
                }
            }
        }
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    walk(&root, &mut hits);

    // WHICH FILE IS THE DOOR, resolved from the tree rather than remembered.
    fn find_door(dir: &Path, needle: &str, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("the crate source is readable") {
            let path = entry.expect("a readable dir entry").path();
            if path.is_dir() {
                find_door(&path, needle, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs")
                && std::fs::read_to_string(&path).unwrap_or_default().contains(needle)
            {
                out.push(path.to_string_lossy().to_string());
            }
        }
    }
    let mut door_files: Vec<String> = Vec::new();
    find_door(&root, DOOR_DEFINITION, &mut door_files);
    assert_eq!(
        1,
        door_files.len(),
        "expected exactly one file to define `{DOOR_DEFINITION}` and found {}: {:?}. The door must be a single function, or 'outside the door' is not a well-defined set.",
        door_files.len(),
        door_files
    );
    let door = door_files[0].clone();
    println!("  the door is defined in {door}");

    let in_door: Vec<_> = hits.iter().filter(|(p, _, _)| *p == door).collect();
    let outside: Vec<_> = hits.iter().filter(|(p, _, _)| *p != door).collect();

    println!();
    println!("  {} spelling(s) of `{NEEDLE}` in the crate", hits.len());
    for (path, line, text) in &hits {
        let where_ = if path.contains(DOOR) { "DOOR" } else { "OUTSIDE" };
        println!("    [{where_}] {}:{} {}", path.rsplit('/').next().unwrap_or(path), line, text);
    }
    println!();

    // --- THE CONTROL FIRST: the scan must still find the door, or it is blind.
    assert!(
        !in_door.is_empty(),
        "the scan found no `{NEEDLE}` in {door}, where `remove_string_row` spells it. The needle \
         has gone stale, so a run reporting no violations elsewhere would prove nothing."
    );
    assert!(
        outside.is_empty(),
        "a string row is removed outside `remove_string_row`, so `live_user_bytes` and \
         `live_objects_with_logical_size` will keep counting an object that is gone:\n{}",
        outside
            .iter()
            .map(|(p, l, t)| format!("  {p}:{l}  {t}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
