// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WAL-replay recovery under the single write-path durability barrier -- now the DEFAULT (only the
//! WAL takes a synchronous fdatasync per write; the data-page fdatasync and the served-index delta
//! fdatasync are both deferred, and recovery is base-only). After a crash, EVERY acked write must
//! be reconstructed -- the WAL is self-sufficient (it embeds the full command payload), so even if
//! the deferred-fsync index-log tail AND, as a stronger stress, the pages are lost to a simulated
//! power cut, replay rebuilds every key. A final phase exercises the TS_WAL_LEGACY_RECOVERY escape
//! hatch (legacy multi-barrier write + delta-fold recovery) so the fallback stays covered. Each
//! phase runs in its own subprocess so any mode env var never leaks into the rest of the suite.
//!
//! # The premise above no longer holds, and five of these fail because of it
//!
//! "The WAL is self-sufficient (it embeds the full command payload)" was true when this was
//! written. It is not true now, and nothing here was changed to say so -- these tests have been
//! failing on main ever since, and the CI step that runs them carries `continue-on-error: true`.
//!
//! Three defaults meet to produce it:
//!
//!   * single-barrier acks once the WAL is fsynced; the data-page fdatasync is deferred BY DESIGN,
//!     which is the whole point of the mode and is what the power-loss step models by deleting the
//!     pages;
//!   * `TS_WAL_OUTCOME_ITEMS` (default ON) records what a write DID rather than what it was;
//!   * `TS_WAL_DATA_ONLY` (default ON) then removes the command -- "stop writing the operation into
//!     a record that already states its results ... the operation is consulted only when there are
//!     none."
//!
//! An outcome states "this object's page is at this address". With the page write deferred and then
//! lost, the address names nothing and the command that could have re-derived the value is gone.
//! Staged pages (`TS_BLOCK_IN_WAL`) would carry the bytes, but staging happens on the ASYNC storage
//! path; a synchronous write stages nothing.
//!
//! Measured on the store these tests leave behind, with `examples/wal_scan_probe.rs`:
//!
//! ```text
//! scan returned 300 record(s)
//!   carrying a COMMAND to re-run          0
//!   carrying OUTCOMES                     300  (300 items)
//!      of those items, carrying a VALUE   0
//!   carrying STAGED PAGES                 0  (0 bytes)
//!   undecodable                           0
//! ```
//!
//! So the log is intact and complete -- 300 records, all decoding, `last_sequence` agreeing -- and
//! holds nothing any replay could rebuild a value from. The failure is not the harness: its
//! `abort()` models process loss faithfully, the WAL survives it, and the records are all there.
//!
//! `legacy_recovery_escape_hatch_delta_fold_recovers_every_ack` still passes, which is the shape of
//! the thing: the hatch recovers where the default does not.
//!
//! Fixing it is a choice about what an ack means, not a tidy-up -- an outcome could carry the value
//! when the page it names is not yet durable; the sync path could stage pages as the async path
//! does; the command could stay while single-barrier is on; or the page fsync could stop being
//! deferred in this mode. Each trades write cost against the promise. Whoever takes it should start
//! from the probe output above rather than from the assertion message, which only says a key was
//! missing.

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
