// SPDX-License-Identifier: Apache-2.0

use std::collections::BinaryHeap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use temporalstore_rust::http::{json_response, parse_json};
use temporalstore_rust::types::Status;
use temporalstore_snapshot::object_store::FileObjectStore;

const PAGE_SIZE: usize = 512;

#[derive(Deserialize)]
struct InventoryRequest {
    namespace: String,
    partition: Option<String>,
    after: Option<String>,
}

#[derive(Serialize)]
struct BlobEntry {
    id: String,
    size: u64,
    modified_ms: u64,
}

#[derive(Serialize)]
struct Page {
    entries: Vec<BlobEntry>,
    next: Option<String>,
}

fn hex_identifier(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn children(path: &Path) -> io::Result<Option<fs::ReadDir>> {
    match fs::read_dir(path) {
        Ok(entries) => Ok(Some(entries)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn partitions(path: &Path) -> io::Result<Vec<String>> {
    let mut partitions = Vec::new();
    for dir in children(path)?.into_iter().flatten() {
        let dir = dir?;
        if let Ok(name) = dir.file_name().into_string() {
            if hex_identifier(&name, 2) && dir.file_type()?.is_dir() {
                partitions.push(name);
            }
        }
    }
    partitions.sort_unstable();
    Ok(partitions)
}

fn inventory_page(path: &Path, partition: &str, after: &str) -> io::Result<Page> {
    let mut names = BinaryHeap::with_capacity(PAGE_SIZE + 1);
    for file in children(path)?.into_iter().flatten() {
        let file = file?;
        let Ok(name) = file.file_name().into_string() else {
            continue;
        };
        if name.as_str() <= after || !hex_identifier(&name, 64) || !name.starts_with(partition) {
            continue;
        }
        if names.len() < PAGE_SIZE + 1 || names.peek().is_some_and(|last| &name < last) {
            names.push(name);
            if names.len() > PAGE_SIZE + 1 {
                names.pop();
            }
        }
    }

    let mut names = names.into_sorted_vec();
    let next = if names.len() > PAGE_SIZE {
        names.pop();
        names.last().cloned()
    } else {
        None
    };
    let mut entries = Vec::with_capacity(names.len());
    for id in names {
        let metadata = match fs::symlink_metadata(path.join(&id)) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let modified_ms = metadata
            .modified()?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        entries.push(BlobEntry {
            id,
            size: metadata.len(),
            modified_ms,
        });
    }
    Ok(Page { entries, next })
}

fn inventory_path(store: &FileObjectStore, request: &InventoryRequest) -> Option<PathBuf> {
    let valid = hex_identifier(&request.namespace, 64)
        && request
            .partition
            .as_deref()
            .is_none_or(|p| hex_identifier(p, 2))
        && request.after.as_deref().is_none_or(|id| {
            hex_identifier(id, 64)
                && request
                    .partition
                    .as_deref()
                    .is_some_and(|p| id.starts_with(p))
        });
    valid
        .then(|| {
            store
                .object_path(&format!("lmcache/v2/{}", request.namespace))
                .ok()
        })
        .flatten()
}

pub(super) fn handle(body: &[u8], store: &FileObjectStore) -> (u16, Vec<u8>) {
    let bad_request = || {
        json_response(
            400,
            &Status::error("bad_request", "invalid LMCache inventory request"),
        )
    };
    if body.len() > 1024 {
        return bad_request();
    }
    let Ok(request) = parse_json::<InventoryRequest>(body) else {
        return bad_request();
    };
    let Some(path) = inventory_path(store, &request) else {
        return bad_request();
    };
    let result = match request.partition {
        Some(partition) => inventory_page(
            &path.join(&partition),
            &partition,
            request.after.as_deref().unwrap_or(""),
        )
        .map(|page| json_response(200, &page)),
        None => partitions(&path)
            .map(|partitions| json_response(200, &serde_json::json!({ "partitions": partitions }))),
    };
    result.unwrap_or_else(|error| {
        tracing::warn!(%error, "could not read LMCache blob inventory");
        json_response(
            500,
            &Status::error("inventory_failed", "could not read LMCache blob inventory"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lmcache_inventory_pages_sorted_ids_and_ignores_staging_files() {
        let work = tempfile::tempdir().unwrap();
        let ns = "0".repeat(64);
        let path = work.path().join("lmcache/v2").join(&ns);
        let partition = path.join("ab");
        fs::create_dir_all(&partition).unwrap();
        fs::write(partition.join(".blob-upload-incomplete"), b"incomplete").unwrap();
        for n in (0..PAGE_SIZE + 2).rev() {
            fs::write(partition.join(format!("ab{n:062x}")), b"data").unwrap();
        }
        assert_eq!(partitions(&path).unwrap(), ["ab"]);
        let page = inventory_page(&partition, "ab", "").unwrap();
        assert_eq!(page.entries.len(), PAGE_SIZE);
        assert_eq!(page.entries[0].id, format!("ab{:062x}", 0));
        assert_eq!(page.entries[0].size, 4);
        let last = page.next.unwrap();
        assert_eq!(last, format!("ab{:062x}", PAGE_SIZE - 1));
        let tail = inventory_page(&partition, "ab", &last).unwrap();
        assert_eq!(tail.entries.len(), 2);
        assert!(tail.next.is_none());
        let store = FileObjectStore::new(work.path().to_path_buf());
        let (status, _) = handle(br#"{"namespace":"../outside","partition":"ab"}"#, &store);
        assert_eq!(status, 400);
    }
}
