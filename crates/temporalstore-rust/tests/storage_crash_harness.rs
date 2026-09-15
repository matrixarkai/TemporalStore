// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::process::Command;

use serde::Deserialize;
use temporalstore_rust::StorageRecoveryReport;

#[derive(Debug, Deserialize)]
struct CrashHarnessSummary {
    before_value: Option<String>,
    after_value: Option<String>,
    recovery: StorageRecoveryReport,
}

#[test]
fn storage_crash_harness_recovers_after_abrupt_process_abort() {
    let bin = env!("CARGO_BIN_EXE_storage_crash_harness");
    let dir = tempfile::tempdir().unwrap();

    let aborted = Command::new(bin)
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "--mode",
            "write-abort",
        ])
        .output()
        .expect("write-abort harness should run");
    assert!(
        !aborted.status.success(),
        "write-abort should terminate the process abruptly"
    );

    let recovered = Command::new(bin)
        .args(["--root", dir.path().to_str().unwrap(), "--mode", "recover"])
        .output()
        .expect("recover harness should run");
    assert!(
        recovered.status.success(),
        "recover failed: stdout={} stderr={}",
        String::from_utf8_lossy(&recovered.stdout),
        String::from_utf8_lossy(&recovered.stderr)
    );

    let summary: CrashHarnessSummary =
        serde_json::from_slice(&recovered.stdout).expect("recover output should be JSON");
    assert_eq!(summary.before_value.as_deref(), Some("before-value"));
    assert_eq!(summary.after_value.as_deref(), Some("after-value"));
    assert_eq!(summary.recovery.wal_records, 2);
    assert_eq!(summary.recovery.index_log_records, 2);
    assert_eq!(summary.recovery.active_block_slab_ids, vec![0, 1]);
    assert_eq!(summary.recovery.live_block_slab_ids, vec![0, 1]);
    assert_eq!(summary.recovery.total_block_refs, 2);
    assert_eq!(summary.recovery.readable_block_refs, 2);
    assert!(summary.recovery.all_live_blocks_readable);
    assert_eq!(summary.recovery.slab_descriptors.len(), 2);
    assert_eq!(summary.recovery.block_slab_reports.len(), 2);
    assert!(summary
        .recovery
        .block_slab_reports
        .iter()
        .all(|report| report.first_error.is_none()));
}

#[test]
fn storage_crash_harness_reports_corrupt_block_after_process_abort() {
    let bin = env!("CARGO_BIN_EXE_storage_crash_harness");
    let dir = tempfile::tempdir().unwrap();

    let aborted = Command::new(bin)
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "--mode",
            "write-abort",
        ])
        .output()
        .expect("write-abort harness should run");
    assert!(
        !aborted.status.success(),
        "write-abort should terminate the process abruptly"
    );

    let corrupted = Command::new(bin)
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "--mode",
            "corrupt-page",
        ])
        .output()
        .expect("corrupt-page harness should run");
    assert!(
        corrupted.status.success(),
        "corrupt-page failed: stdout={} stderr={}",
        String::from_utf8_lossy(&corrupted.stdout),
        String::from_utf8_lossy(&corrupted.stderr)
    );

    let recovered = Command::new(bin)
        .args(["--root", dir.path().to_str().unwrap(), "--mode", "recover"])
        .output()
        .expect("recover harness should run");
    assert!(
        recovered.status.success(),
        "recover failed: stdout={} stderr={}",
        String::from_utf8_lossy(&recovered.stdout),
        String::from_utf8_lossy(&recovered.stderr)
    );

    let summary: CrashHarnessSummary =
        serde_json::from_slice(&recovered.stdout).expect("recover output should be JSON");
    // WHAT THIS TEST IS FOR, which is what its name says: the corrupt page is REPORTED. That is
    // the block of assertions below, and none of it is about what a read answers.
    //
    // This line used to read `assert_eq!(summary.before_value, None)` -- "a corrupt page is a
    // read miss". That was true when a load went to the page store for its answers. It is not
    // the design any more: reconstruction on the default path is the durable base plus a replay
    // of the WAL tail past its anchor (see the header of `delta_index_log_gc.rs`), and the WAL
    // here still holds both records, so `before` comes back from the replay while its page stays
    // corrupt and unreadable. The engine is not hiding the damage -- it reports it below -- it is
    // answering from the log, which is the authority the page only materialises.
    //
    // The old form was also satisfied by ANY absent answer: a shard that failed to load, a
    // harness whose recover mode did nothing, a key that was never written. A test that passes
    // when its own setup dies is the shape that hid a rename for six days in #1639, so assert the
    // value that must be there and say loudly what a miss would and would not prove.
    assert!(
        summary.recovery.wal_records >= 2,
        "the WAL must still hold both records for the replay to serve `before` past its corrupt \
         page -- the report says {} record(s), so this is no longer a test about corruption, it \
         is a test about a truncated WAL",
        summary.recovery.wal_records
    );
    assert_eq!(
        summary.before_value.as_deref(),
        Some("before-value"),
        "`before` must come back from the WAL replay even though the page holding it is corrupt. \
         Getting nothing here would NOT prove the corruption was detected -- it is equally what a \
         shard that never loaded answers. Detection is proved by the unreadable-page-ref \
         assertions below, and those are this test's subject."
    );
    assert_eq!(summary.after_value.as_deref(), Some("after-value"));
    assert_eq!(summary.recovery.total_block_refs, 2);
    assert_eq!(summary.recovery.readable_block_refs, 1);
    assert!(!summary.recovery.all_live_blocks_readable);
    assert_eq!(summary.recovery.unreadable_block_refs.len(), 1);
    assert_eq!(summary.recovery.unreadable_block_refs[0].block_slab_id, 0);
    assert_eq!(summary.recovery.block_slab_reports.len(), 2);
    assert!(summary
        .recovery
        .block_slab_reports
        .iter()
        .any(|report| report.first_error.is_some()));
    assert!(
        summary.recovery.unreadable_block_refs[0]
            .error
            .contains("checksum")
            || summary.recovery.unreadable_block_refs[0]
                .error
                .contains("corrupt page envelope")
    );
}
