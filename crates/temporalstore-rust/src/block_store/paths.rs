use std::fs::File;
use std::path::{Path, PathBuf};

pub(super) fn segment_path(root: &Path, page_segment_id: u64) -> PathBuf {
    root.join(format!("page_segment_{page_segment_id:020}.seg"))
}

pub(super) fn extent_manifest_path(root: &Path) -> PathBuf {
    root.join("page_extent_manifest.json")
}

pub(super) fn legacy_zone_manifest_path(root: &Path) -> PathBuf {
    root.join("page_zone_manifest.json")
}

pub(super) fn delayed_destroy_dir(root: &Path) -> PathBuf {
    root.join(".page_segment_trash")
}

pub(super) fn delayed_destroy_path(root: &Path, page_segment_id: u64) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    delayed_destroy_dir(root).join(format!(
        "page_segment_{page_segment_id:020}.seg.deleted.{nanos}"
    ))
}

pub(super) fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            dir.sync_all()?;
        }
    }
    Ok(())
}

pub(super) fn sync_dir(path: &Path) -> std::io::Result<()> {
    if let Ok(dir) = File::open(path) {
        dir.sync_all()?;
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

pub(super) fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
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
