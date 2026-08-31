// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scratch directories: minting, ownership, and reclamation.
//!
//! # The leak this closes
//!
//! Every `Default` store constructor mints a unique directory under the system temp dir, and
//! nothing ever removed one: not the store (dropping it left the directory behind), and not
//! the next boot (the name is unique per process, so no later run ever reuses it). A box
//! that builds engines often accumulates these without bound — tens of thousands were found
//! on one shared machine.
//!
//! # Ownership
//!
//! A store built by `Default` OWNS its directory: it holds an [`ScratchDirGuard`] whose drop
//! removes the tree. The stores are `Clone` with shared inner state, so the guard is held as
//! an `Arc` inside that shared state — the directory lives exactly as long as the last
//! clone. A store built on a caller-supplied directory never holds a guard, so a real data
//! directory is never deleted.
//!
//! # Reclamation
//!
//! Drop cannot run on a killed process, so minting any scratch path also arms a
//! once-per-process background sweep: the minted name embeds the owning process id, and the
//! sweep removes `temporalstore-rust-*` directories whose owner is no longer alive. A live
//! owner's directories are always kept, as is anything whose name does not parse.

use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Mint a unique path under the system temp dir. The name embeds the process id, which is
/// what lets [`sweep_dead_scratch_dirs`] tell an abandoned directory (owner exited) from one
/// a live process is still using. Minting also arms the once-per-process background sweep.
pub(crate) fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    arm_background_sweep();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

/// A scratch directory the minting store owns: removed from disk when the guard drops.
/// Held as an `Arc` inside the owning store's shared inner state, so the directory outlives
/// every clone and dies with the last one.
#[derive(Debug)]
pub(crate) struct ScratchDirGuard {
    path: PathBuf,
}

impl ScratchDirGuard {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Mint a scratch directory together with its owning guard.
pub(crate) fn owned_scratch_dir(kind: &str) -> Arc<ScratchDirGuard> {
    Arc::new(ScratchDirGuard {
        path: unique_temp_path(kind),
    })
}

/// TS_SCRATCH_SWEEP: reclaim scratch directories whose owning process is gone. Default ON;
/// set to a falsey value to leave abandoned directories in place.
fn sweep_enabled() -> bool {
    !matches!(
        std::env::var("TS_SCRATCH_SWEEP")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

fn arm_background_sweep() {
    static ARMED: std::sync::Once = std::sync::Once::new();
    ARMED.call_once(|| {
        if !sweep_enabled() {
            return;
        }
        std::thread::spawn(|| {
            let report = sweep_dead_scratch_dirs(&std::env::temp_dir());
            if report.removed > 0 || report.failed > 0 {
                eprintln!(
                    "temporalstore_scratch_sweep removed={} failed={} kept_live={}",
                    report.removed, report.failed, report.kept_live
                );
            }
        });
    });
}

#[derive(Debug, Default)]
struct SweepReport {
    removed: u64,
    failed: u64,
    kept_live: u64,
}

/// Remove `temporalstore-rust-*` directories under `dir` whose embedded process id is no
/// longer alive. Directories of live processes are kept; so is anything whose name does not
/// parse, and everything on platforms where liveness cannot be checked.
fn sweep_dead_scratch_dirs(dir: &Path) -> SweepReport {
    let mut report = SweepReport::default();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return report;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix("temporalstore-rust-") else {
            continue;
        };
        // Layout: {kind}-{pid}-{nanos}-{counter}. The kind may itself contain dashes
        // ("index-logs"), so parse from the right.
        let mut fields = rest.rsplitn(4, '-');
        let (Some(_counter), Some(_nanos), Some(pid)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let Ok(pid) = pid.parse::<u32>() else { continue };
        if pid == std::process::id() {
            report.kept_live += 1;
            continue;
        }
        match process_is_alive(pid) {
            Some(false) => {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if std::fs::remove_dir_all(entry.path()).is_ok() {
                        report.removed += 1;
                    } else {
                        report.failed += 1;
                    }
                }
            }
            // A live owner, or a platform where liveness cannot be checked: never guess
            // about deleting.
            Some(true) | None => report.kept_live += 1,
        }
    }
    report
}

#[cfg(target_os = "linux")]
fn process_is_alive(pid: u32) -> Option<bool> {
    Some(Path::new("/proc").join(pid.to_string()).exists())
}

#[cfg(not(target_os = "linux"))]
fn process_is_alive(_pid: u32) -> Option<bool> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_scratch_dir_survives_clones_and_dies_with_the_last() {
        let guard = owned_scratch_dir("scratch-guard-test");
        let path = guard.path().to_path_buf();
        std::fs::create_dir_all(&path).expect("create scratch dir");
        std::fs::write(path.join("probe"), b"x").expect("write probe");
        let clone = Arc::clone(&guard);
        drop(guard);
        assert!(path.exists(), "a live clone must keep the directory");
        drop(clone);
        assert!(!path.exists(), "the last drop must remove the directory");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sweep_removes_dead_owners_and_keeps_live_ones() {
        let base = tempfile::tempdir().expect("tempdir");
        // pid_max caps real pids far below u32::MAX, so this owner can never be alive.
        let dead = base.path().join("temporalstore-rust-pages-4294967295-1-2");
        let live = base
            .path()
            .join(format!("temporalstore-rust-index-logs-{}-3-4", std::process::id()));
        let unrelated = base.path().join("unrelated-dir");
        for dir in [&dead, &live, &unrelated] {
            std::fs::create_dir_all(dir).expect("create test dir");
        }
        let report = sweep_dead_scratch_dirs(base.path());
        assert!(!dead.exists(), "a dead owner's directory must be reclaimed");
        assert!(live.exists(), "this live process's directory must be kept");
        assert!(unrelated.exists(), "unrelated names must never be touched");
        assert_eq!(report.removed, 1);
    }
}
