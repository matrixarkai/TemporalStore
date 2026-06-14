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
    assert_eq!(summary.recovery.oplog_records, 2);
    assert_eq!(summary.recovery.index_log_records, 2);
    assert_eq!(summary.recovery.active_page_segment_ids, vec![0, 1]);
    assert_eq!(summary.recovery.live_page_segment_ids, vec![0, 1]);
    assert_eq!(summary.recovery.total_page_refs, 2);
    assert_eq!(summary.recovery.readable_page_refs, 2);
    assert!(summary.recovery.all_live_pages_readable);
    assert_eq!(summary.recovery.zone_descriptors.len(), 2);
}
