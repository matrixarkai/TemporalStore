// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Every directory fsync this module issues, counted for the life of the process.
///
/// COUNTED RATHER THAN TIMED. What an fsync costs depends on the device, on the page cache and on
/// what else the box is doing; how MANY of them a round issues is the shape itself, and it reads
/// the same number on a loaded machine as on an idle one.
///
/// THE COUNTER SITS ON THE PRIMITIVE, NOT ON THE CALL SITE. A count taken inside
/// `move_slab_to_delayed_destroy` would stop seeing the fsyncs the moment they moved out of it --
/// which is exactly the change this exists to measure, so the instrument would go blind on the
/// one edit it is watching for. At `sync_dir`/`sync_parent_dir` it cannot: every directory fsync
/// the block store performs goes through one of these two functions, wherever it is called from.
///
/// `wal.rs`, `index_log.rs` and `engine.rs` each keep their own private `sync_parent_dir`, so this
/// counts block-store directory fsyncs and nothing else.
static DIRECTORY_FSYNCS: AtomicU64 = AtomicU64::new(0);

/// Directory fsyncs issued by the block store so far.
///
/// A caller measuring a stage must take a delta across it, never an absolute: this is
/// process-global and every store in the process contributes.
pub(crate) fn directory_fsyncs() -> u64 {
    DIRECTORY_FSYNCS.load(Ordering::Relaxed)
}

pub(super) fn slab_path(root: &Path, block_slab_id: u64) -> PathBuf {
    root.join(format!("block_segment_{block_slab_id:020}.seg"))
}

pub(super) fn slab_manifest_path(root: &Path) -> PathBuf {
    root.join("block_extent_manifest.json")
}

pub(super) fn legacy_zone_manifest_path(root: &Path) -> PathBuf {
    root.join("page_zone_manifest.json")
}

pub(super) fn delayed_destroy_dir(root: &Path) -> PathBuf {
    root.join(".block_segment_trash")
}

pub(super) fn delayed_destroy_path(root: &Path, block_slab_id: u64) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    delayed_destroy_dir(root).join(format!(
        "block_segment_{block_slab_id:020}.seg.deleted.{nanos}"
    ))
}

pub(super) fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            dir.sync_all()?;
            DIRECTORY_FSYNCS.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

pub(super) fn sync_dir(path: &Path) -> std::io::Result<()> {
    if let Ok(dir) = File::open(path) {
        dir.sync_all()?;
        DIRECTORY_FSYNCS.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

pub(super) fn now_unix_ms() -> u64 {
    system_time_unix_ms(std::time::SystemTime::now()).unwrap_or_default()
}

pub(super) fn file_created_unix_ms(path: &Path) -> Option<u64> {
    path.metadata()
        .ok()
        .and_then(|metadata| metadata.created().ok())
        .and_then(system_time_unix_ms)
}

pub(super) fn file_modified_unix_ms(path: &Path) -> Option<u64> {
    path.metadata()
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(system_time_unix_ms)
}

pub(super) fn system_time_unix_ms(time: std::time::SystemTime) -> Option<u64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}

