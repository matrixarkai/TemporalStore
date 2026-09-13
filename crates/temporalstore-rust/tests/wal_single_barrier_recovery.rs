// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WAL-replay recovery under the single write-path durability barrier -- now the DEFAULT (only the
//! WAL takes a synchronous fdatasync per write; the data-page fdatasync and the served-index delta
//! fdatasync are both deferred, and recovery is base-only). After a crash, EVERY acked write must
//! be reconstructed -- the WAL is self-sufficient (a record states its RESULTS: an outcome item
//! naming the page's address and, when no page backs it, carrying the bytes themselves), so even if
//! the deferred-fsync index-log tail AND, as a stronger stress, the pages are lost to a simulated
//! power cut, replay rebuilds every key. A final phase exercises the TS_WAL_LEGACY_RECOVERY escape
//! hatch (legacy multi-barrier write + delta-fold recovery) so the fallback stays covered. Each
//! phase runs in its own subprocess so any mode env var never leaks into the rest of the suite.
//!
//! # The hole this header used to describe is FIXED
//!
//! This header used to open "the premise above no longer holds, and five of these fail because of
//! it", and described a default-path hole in which every acked write was unrecoverable after a
//! power cut. It then told the reader the CI step carried `continue-on-error: true`, which made it
//! read as a suppressed data-loss bug sitting on main.
//!
//! IT IS FIXED. Measured 2026-09-12 on this file, with no mode env set:
//!
//! ```text
//! test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
//! ```
//!
//! including `wal_is_self_sufficient_when_all_non_wal_state_is_wiped`, which is the strongest case
//! in the file -- every non-WAL byte removed, replay rebuilding each acked key. A passing run of
//! that test IS the premise the old header said had stopped being true.
//!
//! What closed it: `WalOutcomeItem` carries a `value` beside its `address` (#380, "a record can now
//! say what a write DID, and a shard can be rebuilt from that alone"). The old analysis was right
//! that an outcome naming only an address states nothing recoverable when the page write is
//! deferred and then lost -- the answer was to let the outcome carry the bytes, which is what the
//! design being followed does with a `value` beside its `page` and a `meta_log` flag to tell them
//! apart.
//!
//! The CI note was accurate and remains so, but it is about the BASELINE, not about this file:
//! `continue-on-error` is set on the whole suite step while a small number of pre-existing lib
//! failures stand, and the workflow says to flip it off once the baseline is green. None of those
//! failures is here.
//!
//! Left as a warning rather than deleted, because the failure mode it describes is real whenever
//! an outcome can name a page that is not yet durable. If a future change makes outcomes
//! address-only again on the synchronous path, this is the file that will catch it, and
//! `examples/wal_scan_probe.rs` prints exactly what a record is carrying:
//!
//! ```text
//! carrying a COMMAND to re-run / carrying OUTCOMES / of those items, carrying a VALUE
//! ```
//!
//! A run where outcomes carry no value and no command is the shape of the old bug.

use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_wal_single_barrier_crash_harness")
}

// Default-path helpers (single-barrier + group-commit are the DEFAULT, so no mode env is set).
// These focus on losing the deferred served-index delta log (its `drop-indexlog` kill point).
fn populate(root: &str, keys: &str, flush_at: Option<&str>) {
    let mut cmd = Command::new(bin());
    cmd.args(["--mode", "populate", "--root", root, "--keys", keys]);
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

// Single-barrier helpers: the TRUE single barrier (per-write data-page fdatasync also deferred)
// with base-only recovery -- the unconditional write/recovery path. Each phase runs in its own
// subprocess.
fn populate_sb(root: &str, keys: &str, flush_at: Option<&str>) {
    let mut cmd = Command::new(bin());
    cmd.args(["--mode", "populate", "--root", root, "--keys", keys]);
    if let Some(f) = flush_at {
        cmd.args(["--flush-at", f]);
    }
    let out = cmd.output().expect("populate_sb should run");
    assert!(
        !out.status.success(),
        "populate_sb must end in an abrupt abort (crash simulation)"
    );
}

fn powerloss_sb(root: &str, scope: &str) {
    let out = Command::new(bin())
        .args(["--mode", "powerloss", "--root", root, "--scope", scope])
        .output()
        .expect("powerloss_sb should run");
    assert!(
        out.status.success(),
        "powerloss_sb failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn recover_sb_ok(root: &str, keys: &str) {
    let out = Command::new(bin())
        .args(["--mode", "recover", "--root", root, "--keys", keys])
        .output()
        .expect("recover_sb should run");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("\"ok\":true"),
        "single-barrier recover reported data loss: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("\"missing\":[]") && stdout.contains("\"mismatched\":[]"),
        "single-barrier recover lost or corrupted an acked write: {stdout}"
    );
}

#[test]
fn single_barrier_data_page_loss_after_dump_rebuilds_from_wal() {
    // THE data-page kill-point case for the true single barrier. The per-write data-page fdatasync
    // is deferred, so pages become durable only at the dump. A dump at key 150 fsyncs pages 0..150
    // and anchors the watermark; writes 151..300 then append pages that are NEVER fsync'd. Model a
    // real power cut of exactly those un-synced page tails (truncate each slab back to its recorded
    // post-dump durable length). Base-only recovery replays 151..300 from the dump watermark and
    // rebuilds every lost page from its WAL command -> zero data loss.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate_sb(root, "300", Some("150"));
    powerloss_sb(root, "truncate-tail");
    recover_sb_ok(root, "300");
}

fn populate_counter_sb(root: &str, incrs: &str, flush_at: Option<&str>) {
    let mut cmd = Command::new(bin());
    cmd.args(["--mode", "populate-counter", "--root", root, "--keys", incrs]);
    if let Some(f) = flush_at {
        cmd.args(["--flush-at", f]);
    }
    let out = cmd.output().expect("populate-counter should run");
    assert!(!out.status.success(), "populate-counter must abort");
}

fn recover_counter_sb_ok(root: &str, expected: &str) {
    let out = Command::new(bin())
        .args(["--mode", "recover-counter", "--root", root, "--keys", expected])
        .output()
        .expect("recover-counter should run");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("\"ok\":true"),
        "counter mis-applied on recovery (double-apply or loss): stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn single_barrier_non_idempotent_counter_applies_exactly_once() {
    // A hash counter incremented 200 times, dumped at 100, then crashed with the un-synced tail
    // pages lost. Base-only recovery replays 101..200 from the dump watermark EXACTLY ONCE (no
    // delta fold that would re-apply the tail on top of the base), so the counter must be exactly
    // 200 -- not doubled to 300, and not short.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate_counter_sb(root, "200", Some("100"));
    powerloss_sb(root, "truncate-tail");
    recover_counter_sb_ok(root, "200");
}

#[test]
fn single_barrier_full_page_loss_no_dump_rebuilds_from_wal() {
    // No dump: every data page is un-synced. Wipe all non-WAL state (pages + served index + delta);
    // only the fsync'd WAL survives. Base-only replay from 0 rebuilds all 300 keys.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate_sb(root, "300", None);
    powerloss_sb(root, "wipe-nondurable");
    recover_sb_ok(root, "300");
}

fn populate_feature(root: &str) {
    // `TS_GROUP_COMMIT=1` used to be set here. Nothing reads it -- `group_commit_configured`
    // returns true unconditionally -- so passing it made this look like it configured the child
    // when it configured nothing.
    let out = Command::new(bin())
        .args(["--mode", "populate-feature", "--root", root])
        .output()
        .expect("populate-feature should run");
    assert!(
        !out.status.success(),
        "populate-feature must end in an abrupt abort (crash simulation)"
    );
}

fn recover_feature_ok(root: &str) {
    let out = Command::new(bin())
        .args(["--mode", "recover-feature", "--root", root])
        .output()
        .expect("recover-feature should run");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("\"ok\":true"),
        "single-barrier feature recovery failed: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("\"feature_timestamps\":[30, 40, 50]"),
        "recovery resurrected/lost feature points: {stdout}"
    );
}

#[test]
fn single_barrier_evict_then_crash_before_dump_does_not_resurrect() {
    // THE decisive single-barrier case: a config-driven feature_max_size trim (eviction) happens,
    // then the process is lost BEFORE any dump -- so the trim is recorded only in memory, never in
    // a served-index checkpoint. Only the fsync'd WAL + config-log survive the power cut. Recovery
    // must re-derive the trim from the WAL-ordered config-log and keep exactly the newest 3 points
    // (no resurrection of the 2 evicted points, no loss of an acked point).
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate_feature(root);
    powerloss(root, "wipe-nondurable");
    recover_feature_ok(root);
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

// Legacy escape-hatch helpers: TS_WAL_LEGACY_RECOVERY=1 restores the legacy multi-barrier write
// path (WAL + data-page + delta all fsync'd per write) + delta-fold recovery. Kept covered so the
// operator fallback does not rot.
fn populate_legacy(root: &str, keys: &str, flush_at: Option<&str>) {
    let mut cmd = Command::new(bin());
    cmd.env("TS_WAL_LEGACY_RECOVERY", "1")
        .args(["--mode", "populate", "--root", root, "--keys", keys]);
    if let Some(f) = flush_at {
        cmd.args(["--flush-at", f]);
    }
    let out = cmd.output().expect("populate_legacy should run");
    assert!(
        !out.status.success(),
        "populate_legacy must end in an abrupt abort (crash simulation)"
    );
}

fn recover_legacy_ok(root: &str, keys: &str) {
    let out = Command::new(bin())
        .env("TS_WAL_LEGACY_RECOVERY", "1")
        .args(["--mode", "recover", "--root", root, "--keys", keys])
        .output()
        .expect("recover_legacy should run");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("\"ok\":true"),
        "legacy delta-fold recover reported data loss: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("\"missing\":[]") && stdout.contains("\"mismatched\":[]"),
        "legacy delta-fold recover lost or corrupted an acked write: {stdout}"
    );
}

#[test]
fn legacy_recovery_escape_hatch_delta_fold_recovers_every_ack() {
    // The TS_WAL_LEGACY_RECOVERY fallback. A dump anchors a durable base at key 150 (pages fsync'd
    // per write in legacy mode); the served-index delta is then lost. Delta-fold recovery folds the
    // durable base and replays the WAL tail 151..300, so every acked write is restored -- proving
    // the operator escape hatch still recovers cleanly.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    populate_legacy(root, "300", Some("150"));
    powerloss(root, "drop-indexlog");
    recover_legacy_ok(root, "300");
}
