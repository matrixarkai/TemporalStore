// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scale reload-equality for upsert index-log delta records (TS_INDEXLOG_UPSERT_DELTAS).
//!
//! A ~45K-record production store built through batch-committing ingest reconstructed
//! EMPTY on reload while the small-store proofs passed, so this drives the same shape at
//! a scale the unit tests never reach: thousands of hash batches (every batch emits one
//! upsert delta record), a durably-logged config (config-log present), threshold dumps
//! mid-stream, SIGKILL-grade restarts between generations, and reload-equality asserted
//! over the full served view -- under BOTH recovery modes (default base-only + WAL
//! replay, and the TS_WAL_LEGACY_RECOVERY delta-fold path that the flat-log format needs
//! at scale). Every phase runs in its own subprocess so the upsert-emission gate and the
//! recovery-mode env never leak into the rest of the suite.

use std::path::Path;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_upsert_reload_harness")
}

const GEN1_BATCHES: u64 = 2600;
const GEN2_BATCHES: u64 = 400;

fn run(mode: &str, root: &Path, batches: u64, extend: Option<u64>, legacy_recovery: bool) -> (bool, String) {
    let mut cmd = Command::new(bin());
    cmd.args([
        "--mode",
        mode,
        "--root",
        root.to_str().unwrap(),
        "--batches",
        &batches.to_string(),
    ]);
    if let Some(extend) = extend {
        cmd.args(["--extend", &extend.to_string()]);
    }
    // The emission gate under test: every phase runs with upsert delta records ON.
    cmd.env("TS_INDEXLOG_UPSERT_DELTAS", "1");
    if legacy_recovery {
        cmd.env("TS_WAL_LEGACY_RECOVERY", "1");
    }
    let out = cmd.output().expect("harness should run");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr);
    (
        out.status.success(),
        format!("stdout={stdout} stderr={stderr}"),
    )
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

#[test]
fn upsert_delta_store_reloads_to_the_view_it_acked() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    std::fs::create_dir_all(&root).unwrap();

    // Generation 1: thousands of upsert batches, threshold dumps, then abort.
    let (ok, log) = run("build", &root, GEN1_BATCHES, None, false);
    assert!(!ok, "build must end in the simulated crash, not exit cleanly: {log}");
    assert!(
        log.contains("\"ok\":true"),
        "generation-1 pre-crash view must verify before the crash counts: {log}"
    );

    // Generation 2: reload (verifies gen 1 inside), extend, abort. This is where the
    // production stores lived: upsert records interleaved across restart generations.
    let (ok, log) = run("extend", &root, GEN1_BATCHES, Some(GEN2_BATCHES), false);
    assert!(!ok, "extend must end in the simulated crash: {log}");
    assert!(
        log.contains("\"ok\":true"),
        "reload equality failed inside the extend generation: {log}"
    );

    // Freeze the crashed store so each recovery mode folds the same artifacts.
    let fold_root = dir.path().join("store-fold");
    copy_dir(&root, &fold_root);

    // Default recovery: base-only checkpoint + WAL tail replay.
    let (ok, log) = run("verify", &root, GEN1_BATCHES + GEN2_BATCHES, None, false);
    assert!(
        ok,
        "default (base + WAL replay) recovery lost acked writes: {log}"
    );

    // Delta-fold recovery: base + upsert-record fold, no WAL re-execution. This is the
    // path the flat-log format relies on at scale.
    let (ok, log) = run("verify", &fold_root, GEN1_BATCHES + GEN2_BATCHES, None, true);
    assert!(
        ok,
        "delta-fold recovery of the upsert record stream lost acked writes: {log}"
    );
}
