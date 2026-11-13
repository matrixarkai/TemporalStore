// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::cell::RefCell;
use std::path::{Path, PathBuf};

type ScanHook = Box<dyn FnOnce()>;
type WriteHook = Box<dyn FnMut(&BucketDumpManifest)>;
thread_local! {
    static BEFORE_SCAN: RefCell<Option<ScanHook>> = const { RefCell::new(None) };
    static BEFORE_WRITE: RefCell<Option<WriteHook>> = const { RefCell::new(None) };
}

pub(super) fn before_scan() {
    if let Some(hook) = BEFORE_SCAN.with(|slot| slot.borrow_mut().take()) {
        hook();
    }
}

pub(super) fn before_write(manifest: &BucketDumpManifest) {
    BEFORE_WRITE.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(manifest);
        }
    });
}

struct Hooks;
impl Drop for Hooks {
    fn drop(&mut self) {
        BEFORE_SCAN.with(|slot| slot.borrow_mut().take());
        BEFORE_WRITE.with(|slot| slot.borrow_mut().take());
    }
}

// Keep the original bytes intact while making the real filesystem operation fail.
struct Obstruction {
    path: PathBuf,
    saved: PathBuf,
    replacement_is_dir: bool,
}

impl Obstruction {
    fn install(path: &Path, replacement_is_dir: bool) -> Self {
        let saved = path.with_extension("prune-test-saved");
        assert!(!saved.exists());
        fs::rename(path, &saved).unwrap();
        let obstruction = Self {
            path: path.to_owned(),
            saved,
            replacement_is_dir,
        };
        if replacement_is_dir {
            fs::create_dir(path).unwrap();
        } else {
            fs::write(path, b"not a directory").unwrap();
        }
        obstruction
    }
}

impl Drop for Obstruction {
    fn drop(&mut self) {
        let removed = if self.replacement_is_dir {
            fs::remove_dir(&self.path)
        } else {
            fs::remove_file(&self.path)
        };
        removed.expect("remove only the owned obstruction");
        fs::rename(&self.saved, &self.path).expect("restore the original manifest files");
    }
}

fn engine_at(root: &Path, cache: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1024,
        root.join(cache),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn chain(root: &Path) -> (TemporalEngine, Vec<BucketDumpManifest>) {
    let engine = engine_at(root, "cache");
    let mut manifests = Vec::new();
    for n in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("prune-{n}"),
                value: vec![n],
            },
        });
        assert!(response.status.ok, "{response:?}");
        let manifest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
        engine
            .persist_bucket_dump_install_marker(&manifest, "commit")
            .unwrap();
        manifests.push(manifest);
    }
    assert_eq!(
        manifests[1].parent_manifest_id.as_ref(),
        Some(&manifests[0].manifest_id)
    );
    assert_eq!(
        manifests[2].parent_manifest_id.as_ref(),
        Some(&manifests[1].manifest_id)
    );
    (engine, manifests)
}

fn assert_intact(engine: &TemporalEngine, expected: &[BucketDumpManifest]) {
    let actual = list_bucket_dump_manifests_at(&engine.index_dir, 1).unwrap();
    assert_eq!(actual.len(), expected.len());
    let present: BTreeSet<_> = actual
        .iter()
        .map(|manifest| &manifest.manifest_id)
        .collect();
    for manifest in &actual {
        if let Some(parent) = &manifest.parent_manifest_id {
            assert!(
                present.contains(parent),
                "missing parent for {}",
                manifest.manifest_id
            );
        }
        assert!(expected
            .iter()
            .any(|original| original.manifest_id == manifest.manifest_id
                && original.index_bytes == manifest.index_bytes));
        let mut checked = manifest.clone();
        assert_eq!(
            bucket_dump_manifest_checksum_in_place(&mut checked).unwrap(),
            manifest.checksum
        );
    }
    assert!(engine
        .storage_recovery_boundary_report(1)
        .manifest_chain_issues
        .is_empty());
}

fn assert_cold_values(root: &Path) {
    let reopened = engine_at(root, "cold-cache");
    for n in 0..3 {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("prune-{n}"),
            },
        });
        assert!(response.status.ok, "{response:?}");
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(vec![n])
            }
        );
    }
}

#[test]
fn prune_stops_before_unlink_when_any_detach_write_fails() {
    for fail_at in [1, 2] {
        let root = tempfile::tempdir().unwrap();
        let (engine, manifests) = chain(root.path());
        let markers = bucket_dump_install_marker_files_at(&engine.index_dir, 1).unwrap();
        let plan = engine.bucket_dump_manifest_prune_plan(1);
        assert_eq!(plan.prunable_manifest_ids.len(), 2);
        assert_eq!(plan.prunable_marker_manifest_ids.len(), 2);
        let obstruction = std::rc::Rc::new(RefCell::new(None));
        let observed = obstruction.clone();
        let index_dir = engine.index_dir.clone();
        let mut writes = 0;
        let _hooks = Hooks;
        BEFORE_WRITE.with(|slot| {
            assert!(slot
                .replace(Some(Box::new(move |manifest| {
                    writes += 1;
                    if writes == fail_at {
                        let path = bucket_dump_manifest_path(&index_dir, 1, &manifest.manifest_id);
                        *observed.borrow_mut() = Some(Obstruction::install(&path, true));
                    }
                })))
                .is_none());
        });
        let report = engine.apply_bucket_dump_manifest_prune(1);
        assert!(
            obstruction.borrow().is_some(),
            "the filesystem failure must be reached"
        );
        drop(obstruction.borrow_mut().take());
        drop(_hooks);
        assert!(report.removed_manifest_ids.is_empty(), "{report:?}");
        assert_eq!(report.removed_marker_files, 0);
        assert_eq!(
            report.error.as_ref().unwrap().code,
            "slot_dump_prune_detach_failed"
        );
        assert!(
            report
                .plan
                .reasons
                .iter()
                .any(|reason| reason.starts_with("slot_dump_prune_detach_failed:")),
            "{report:?}"
        );
        for (_, path) in &markers {
            assert!(
                path.is_file(),
                "a failed detach must not remove install markers"
            );
        }
        assert_intact(&engine, &manifests);
        let listed = list_bucket_dump_manifests_at(&engine.index_dir, 1).unwrap();
        assert_eq!(
            listed[1].parent_manifest_id.is_none(),
            fail_at == 2,
            "the second failure must follow a genuinely committed first detach"
        );
        let retried = engine.apply_bucket_dump_manifest_prune(1);
        assert!(retried.error.is_none(), "{retried:?}");
        assert_eq!(retried.removed_manifest_ids.len(), 2, "{retried:?}");
        assert_eq!(retried.removed_marker_files, 2, "{retried:?}");
        assert_intact(&engine, &manifests[2..]);
        drop(engine);
        assert_cold_values(root.path());
    }
}

#[test]
fn prune_reports_a_detach_listing_failure_without_removing_files() {
    let root = tempfile::tempdir().unwrap();
    let (engine, manifests) = chain(root.path());
    let obstruction = std::rc::Rc::new(RefCell::new(None));
    let observed = obstruction.clone();
    let manifest_dir = bucket_dump_manifest_dir(&engine.index_dir, 1);
    let _hooks = Hooks;
    BEFORE_SCAN.with(|slot| {
        assert!(slot
            .replace(Some(Box::new(move || {
                *observed.borrow_mut() = Some(Obstruction::install(&manifest_dir, false));
            })))
            .is_none());
    });
    let report = engine.apply_bucket_dump_manifest_prune(1);
    assert!(obstruction.borrow().is_some());
    drop(obstruction.borrow_mut().take());
    assert!(report.removed_manifest_ids.is_empty(), "{report:?}");
    assert_eq!(report.removed_marker_files, 0);
    assert_eq!(
        report.error.as_ref().unwrap().code,
        "slot_dump_prune_detach_failed"
    );
    assert!(
        report
            .plan
            .reasons
            .iter()
            .any(|reason| reason.starts_with("slot_dump_prune_detach_failed:")),
        "{report:?}"
    );
    assert_intact(&engine, &manifests);
    drop(engine);
    assert_cold_values(root.path());
}

#[test]
fn prune_reports_a_plan_listing_failure_without_claiming_success() {
    let root = tempfile::tempdir().unwrap();
    let (engine, manifests) = chain(root.path());
    let manifest_dir = bucket_dump_manifest_dir(&engine.index_dir, 1);
    let obstruction = Obstruction::install(&manifest_dir, false);
    let report = engine.apply_bucket_dump_manifest_prune(1);
    drop(obstruction);
    assert_eq!(
        report.error.as_ref().unwrap().code,
        "slot_dump_prune_plan_failed"
    );
    assert!(report.removed_manifest_ids.is_empty());
    assert_eq!(report.removed_marker_files, 0);
    assert_intact(&engine, &manifests);
    drop(engine);
    assert_cold_values(root.path());
}

#[test]
fn prune_failure_is_visible_in_the_storage_manager_round() {
    let root = tempfile::tempdir().unwrap();
    let (engine, manifests) = chain(root.path());
    let obstruction = std::rc::Rc::new(RefCell::new(None));
    let observed = obstruction.clone();
    let target = bucket_dump_manifest_path(&engine.index_dir, 1, &manifests[1].manifest_id);
    let target_id = manifests[1].manifest_id.clone();
    let _hooks = Hooks;
    BEFORE_WRITE.with(|slot| {
        assert!(slot
            .replace(Some(Box::new(move |manifest| {
                if manifest.manifest_id == target_id {
                    *observed.borrow_mut() = Some(Obstruction::install(&target, true));
                }
            })))
            .is_none());
    });
    let round = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: false,
        enable_wal_reclaim: false,
        enable_evict: false,
        enable_expire: false,
        enable_block_reclaim: false,
        enable_block_compaction: false,
        enable_index_gc: true,
        ..StorageManagerCycleRequest::default()
    });
    assert!(
        obstruction.borrow().is_some(),
        "the round must reach the failing write"
    );
    drop(obstruction.borrow_mut().take());
    drop(_hooks);
    assert!(!round.completed, "{round:?}");
    assert!(
        round
            .errors
            .iter()
            .any(|error| error.contains("slot_dump_prune_detach_failed")),
        "{round:?}"
    );
    let stage = round
        .stages
        .iter()
        .find(|stage| stage.stage == "index_gc")
        .unwrap();
    assert!(
        stage.reason.starts_with("manifest prune failed:"),
        "{stage:?}"
    );
    let prune = round
        .lifecycle_report
        .as_ref()
        .unwrap()
        .manifest_prune_report
        .as_ref()
        .unwrap();
    assert_eq!(
        prune.error.as_ref().unwrap().code,
        "slot_dump_prune_detach_failed"
    );
    assert!(prune.removed_manifest_ids.is_empty());
    assert_eq!(prune.removed_marker_files, 0);
    assert_intact(&engine, &manifests);
    drop(engine);
    assert_cold_values(root.path());
}

#[test]
fn prune_reports_keep_the_success_wire_shape_and_read_older_reports() {
    let report = BucketDumpManifestPruneReport::default();
    let wire = serde_json::to_value(&report).unwrap();
    assert!(wire.get("error").is_none());
    let restored: BucketDumpManifestPruneReport = serde_json::from_value(wire).unwrap();
    assert!(restored.error.is_none());
    let failed = BucketDumpManifestPruneReport {
        error: Some(Status::error(
            "slot_dump_prune_detach_failed",
            "test failure",
        )),
        ..report
    };
    let wire = serde_json::to_value(&failed).unwrap();
    assert_eq!(wire["error"]["code"], "slot_dump_prune_detach_failed");
    let restored: BucketDumpManifestPruneReport = serde_json::from_value(wire).unwrap();
    assert_eq!(restored, failed);
}
