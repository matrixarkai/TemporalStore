// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What `install_latest_manifest_if_newer_on_load` does to the durable index when it is about to
//! refuse the load anyway.
//!
//! The function decides whether a durable dump manifest is newer than the served index, and
//! installs it if so -- and installing overwrites `shard-{id}.index.json`. It read its
//! `served_anchor` through `load_index`, which is documented as the TOLERANT wrapper: a corrupt
//! served-index delta comes back as `None`. `None` became an anchor of 0, an anchor of 0 made
//! every manifest look newer, and the install overwrote the durable index. Only then did
//! `load_shard_with` reach its own `load_index_checked` and refuse the load -- so the load failed
//! AFTER destroying what it was refusing to read, in precisely the case where that file is the
//! last good copy.
//!
//! This is the `TS_WAL_LEGACY_RECOVERY` arm, which nothing in a shipped configuration selects.
//! It is the emergency escape hatch an operator flips in the field, which makes its population
//! exactly the shards already in trouble -- the ones most likely to have a corrupt delta.
//!
//! Driven directly rather than through `load_shard`, because selecting that arm means setting a
//! process-wide environment variable and the gate runs every test in one process. The function is
//! the unit under test either way.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;

/// The index-log file for `SHARD`, whichever of the two names this store uses. The log lives
/// under `index_dir/indexlogs`, not `index_dir` itself.
fn index_log_file(index_dir: &std::path::Path) -> std::path::PathBuf {
    let root = index_dir.join("indexlogs");
    let framed = root.join(format!("shard-{SHARD}.indexlog.bin"));
    if framed.exists() {
        return framed;
    }
    root.join(format!("shard-{SHARD}.indexlog.jsonl"))
}

/// A durable index holding two keys, a manifest that predates the second, and a corrupt delta.
///
/// The manifest is created BEFORE the second key so that installing it would visibly roll the
/// durable index backwards -- if the two were byte-identical, an install that happened anyway
/// would leave no trace and this test would pass without measuring anything.
#[test]
fn a_refused_install_on_load_leaves_the_durable_index_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "before-manifest".to_string(),
            value: b"a".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("manifest should persist");
    assert!(
        manifest.wal_sequence > 0,
        "vacuous: the manifest must carry a non-zero wal_sequence, or it could never compare as \
         newer than an anchor of 0 and the destructive path could not be entered; it carried {}",
        manifest.wal_sequence
    );
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "after-manifest".to_string(),
            value: b"b".to_vec(),
        },
    });
    // Materialize the durable index, so the file on disk holds BOTH keys while the manifest holds
    // only the first. Unload persists it and does not truncate the index-log.
    engine.unload_shard(SHARD);
    engine.load_shard(SHARD);

    let index_path = engine.index_path(SHARD);
    let log_path = index_log_file(&engine.index_dir);
    assert!(
        log_path.exists(),
        "fixture must have produced an index-log at {log_path:?}"
    );

    // Corrupt the tail of the delta log. The last bytes are inside the final record's payload and
    // its integrity envelope, so flipping them fails the decode rather than the frame walk.
    let mut log_bytes = fs::read(&log_path).expect("index log should read");
    let log_len = log_bytes.len();
    assert!(
        log_len > 16,
        "vacuous: the index log must be long enough to corrupt, it is {log_len} bytes"
    );
    let flip_from = log_len - 8;
    for byte in log_bytes[flip_from..].iter_mut() {
        *byte ^= 0xFF;
    }
    fs::write(&log_path, &log_bytes).expect("index log should write");

    // The corruption actually took. Without this the whole test would pass vacuously against a
    // build where the flipped bytes happened to still decode.
    assert!(
        engine.load_index_checked(SHARD, false).is_err(),
        "vacuous: the {log_len}-byte index log must read as corrupt after flipping its last 8 \
         bytes, but the checked loader accepted it"
    );

    let index_before = fs::read(&index_path).expect("durable index should read");
    assert!(
        !index_before.is_empty(),
        "vacuous: the durable index must be non-empty to notice it being overwritten"
    );

    let outcome = engine.install_latest_manifest_if_newer_on_load(SHARD);
    let index_after = fs::read(&index_path).expect("durable index should read");
    println!(
        "INSTALL-ON-LOAD  index_bytes_before={} index_bytes_after={} manifest_wal_sequence={} \
         outcome={:?}",
        index_before.len(),
        index_after.len(),
        manifest.wal_sequence,
        outcome.as_ref().map(|watermark| *watermark).map_err(|status| status.code.clone()),
    );

    // HALF ONE: it refuses, and with the same status the load would have refused with anyway.
    let status = outcome.expect_err("a corrupt served-index delta must refuse the install");
    assert_eq!(
        status.code, "index_log_delta_corruption",
        "and must refuse with the status `load_shard_with` itself would have returned"
    );

    // HALF TWO: and the durable index is byte-for-byte what it was. This is the half that fails
    // when the anchor is read through the tolerant loader: the install runs first and rolls the
    // index back to the manifest's, which predates `after-manifest`.
    assert_eq!(
        index_after, index_before,
        "the refused install must not have rewritten the durable index: {} bytes before, {} after",
        index_before.len(),
        index_after.len()
    );
}
