// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WAL-replay recovery under TS_INDEXLOG_DEFER_SYNC (the index-log delta fdatasync is dropped
//! from the ack path; the WAL + data pages stay durable per write). After a crash, EVERY acked
//! write must be reconstructed -- the WAL is self-sufficient (it embeds the full command
//! payload), so even if the deferred-fsync index-log tail AND, as a stronger stress, the pages
//! are lost to a simulated power cut, replay rebuilds every key. Each phase runs in its own
//! subprocess so the durability-mode env var never leaks into the rest of the suite.

use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_wal_single_barrier_crash_harness")
}

fn populate(root: &str, keys: &str, flush_at: Option<&str>) {
    let mut cmd = Command::new(bin());
    cmd.env("TS_INDEXLOG_DEFER_SYNC", "1")
        .env("TS_WAL_ONLY_SYNC", "1")
        .env("TS_GROUP_COMMIT", "1")
        .args(["--mode", "populate", "--root", root, "--keys", keys]);
    if let Some(f) = flush_at {
        cmd.args(["--flush-at", f]);
    }
    let out = cmd.output().expect("populate should run");
    assert!(
        !out.status.success(),
        "populate must end in an abrupt abort (crash simulation)"
    );
}

fn powerloss(root: &str, scope: &str) {
    let out = Command::new(bin())
        .env("TS_INDEXLOG_DEFER_SYNC", "1")
        .args(["--mode", "powerloss", "--root", root, "--scope", scope])
        .output()
        .expect("powerloss should run");
    assert!(
        out.status.success(),
        "powerloss failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn recover_ok(root: &str, keys: &str) {
    let out = Command::new(bin())
        .env("TS_INDEXLOG_DEFER_SYNC", "1")
        .env("TS_WAL_ONLY_SYNC", "1")
        .args(["--mode", "recover", "--root", root, "--keys", keys])
        .output()
        .expect("recover should run");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "recover reported data loss: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("\"ok\":true"),
        "recover report not ok: {stdout}"
    );
    assert!(
        stdout.contains("\"missing\":[]") && stdout.contains("\"mismatched\":[]"),
        "recover lost or corrupted acked writes: {stdout}"
    );
}

#[test]
fn deferred_indexlog_loss_never_drops_an_ack_after_a_dump() {
    // The mode's ONLY relaxation is the deferred delta-log fdatasync, so its worst-case loss is
    // the whole un-synced delta log. A dump anchors a durable base at key 150; the post-dump
    // deltas (151..) are then lost. The durable pages + WAL replay from the base watermark must
    // still restore every acked write.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate(root, "300", Some("150"));
    powerloss(root, "drop-indexlog");
    recover_ok(root, "300");
}

#[test]
fn deferred_indexlog_loss_never_drops_an_ack_without_a_dump() {
    // No dump: the entire delta log is lost and there is no base index. Recovery replays the whole
    // WAL from zero and rebuilds every key.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate(root, "300", None);
    powerloss(root, "drop-indexlog");
    recover_ok(root, "300");
}

#[test]
fn wal_is_self_sufficient_when_all_non_wal_state_is_wiped() {
    // Strongest stress (beyond what the mode actually defers): drop the pages AND the served index
    // AND the delta log; only the fsync'd WAL remains. The WAL embeds the full command payload, so
    // replay rebuilds every acked write from scratch.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate(root, "300", None);
    powerloss(root, "wipe-nondurable");
    recover_ok(root, "300");
}
