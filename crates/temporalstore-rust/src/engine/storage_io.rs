// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use super::state::ShardState;

pub(super) fn serialize_index(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec(shard).expect("shard index should serialize")
}

pub(super) fn atomic_write_bytes(
    path: &Path,
    bytes: &[u8],
    sync_contents: bool,
) -> Result<(), std::io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        next_temp_counter()
    ));
    let write_result = (|| {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        if sync_contents {
            file.sync_all()?;
        }
        drop(file);
        fs::rename(&temp_path, path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn next_temp_counter() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub(super) fn unique_temp_path(kind: &str) -> PathBuf {
    let counter = next_temp_counter();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}
