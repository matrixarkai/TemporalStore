// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Slab id / address helpers + delayed-destroy slab scanning, extracted from block_store.rs.

use super::*;
use std::path::Path;

pub(crate) fn page_slab_utility_score(below_retention_floor: bool, is_current: bool, is_live: bool) -> u64 {
    if is_current || is_live {
        100
    } else if below_retention_floor {
        0
    } else {
        50
    }
}

pub(crate) fn move_slab_to_delayed_destroy(
    root: &Path,
    page_slab_id: u64,
) -> Result<(), BlockStoreError> {
    let source = slab_path(root, page_slab_id);
    let trash_dir = delayed_destroy_dir(root);
    fs::create_dir_all(&trash_dir)?;
    let destination = delayed_destroy_path(root, page_slab_id);
    fs::rename(&source, &destination)?;
    sync_parent_dir(&source)?;
    sync_parent_dir(&destination)?;
    Ok(())
}

pub(crate) fn delayed_destroy_slab_ids_at(root: &Path) -> Result<Vec<u64>, BlockStoreError> {
    Ok(delayed_destroy_slab_reports_at(root)?
        .into_iter()
        .map(|report| report.page_slab_id)
        .collect())
}

pub(crate) fn delayed_destroy_slab_reports_at(
    root: &Path,
) -> Result<Vec<BlockStoreDelayedDestroySlabReport>, BlockStoreError> {
    let trash_dir = delayed_destroy_dir(root);
    let mut reports = Vec::new();
    if !trash_dir.exists() {
        return Ok(reports);
    }
    for entry in fs::read_dir(trash_dir)? {
        let entry = entry?;
        if let Some(id) = delayed_destroy_slab_id_from_name(&entry.file_name()) {
            let metadata = entry.metadata().ok();
            reports.push(BlockStoreDelayedDestroySlabReport {
                page_slab_id: id,
                physical_bytes: metadata
                    .as_ref()
                    .map(|metadata| metadata.len())
                    .unwrap_or_default(),
                modified_unix_ms: metadata
                    .as_ref()
                    .and_then(|metadata| metadata.modified().ok())
                    .and_then(system_time_unix_ms),
            });
        }
    }
    reports.sort_by_key(|report| report.page_slab_id);
    Ok(reports)
}

pub(crate) fn delayed_destroy_slab_id_from_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let id = name
        .strip_prefix("page_segment_")?
        .strip_suffix(name.split_once(".seg.deleted.")?.1)?
        .strip_suffix(".seg.deleted.")?;
    id.parse::<u64>().ok()
}

pub(crate) fn band_id_for_slab(page_slab_id: u64) -> u64 {
    let slab_target_bytes = effective_block_slab_target_bytes().max(1);
    let storage_zone_size = storage_zone_size_bytes().max(1);
    page_slab_id
        .saturating_mul(slab_target_bytes)
        .saturating_div(storage_zone_size)
}

pub(crate) fn compact_slab_address_from_parts(page_slab_id: u64, offset: u64) -> Option<u64> {
    let band_id = u32::try_from(page_slab_id).ok()?;
    let band_offset = u32::try_from(offset).ok()?;
    Some(((band_id as u64) << 32) | band_offset as u64)
}

pub(crate) fn compact_extract_band_id(address: u64) -> u32 {
    (address >> 32) as u32
}

pub(crate) fn compact_extract_band_offset(address: u64) -> u32 {
    (address & 0xFFFF_FFFF) as u32
}

pub(crate) fn slab_ids_at(root: &Path) -> Result<Vec<u64>, BlockStoreError> {
    let mut ids = Vec::new();
    if !root.exists() {
        return Ok(ids);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if let Some(id) = name
            .strip_prefix("page_segment_")
            .and_then(|name| name.strip_suffix(".seg"))
            .and_then(|id| id.parse::<u64>().ok())
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

pub(crate) fn latest_slab_id_at(root: &Path) -> Result<u64, BlockStoreError> {
    Ok(slab_ids_at(root)?.into_iter().max().unwrap_or_default())
}

pub(crate) fn next_page_id_at(root: &Path) -> Result<u64, BlockStoreError> {
    let mut max_page_id = None;
    for page_slab_id in slab_ids_at(root)? {
        // Header-only walk. This used to `fs::read` the whole slab and hand it to `inspect_slab`,
        // which decodes and hashes every page to build a report that is discarded but for one
        // field. Opening the file and stepping it by record length reads the headers and nothing
        // else, so the cost stops tracking the size of the pages.
        let file = File::open(slab_path(root, page_slab_id))?;
        let slab_len = file.metadata()?.len();
        if let Some(slab_max) = max_page_id_in_slab_file(file, slab_len, page_slab_id)? {
            max_page_id =
                Some(max_page_id.map_or(slab_max, |current: u64| current.max(slab_max)));
        }
    }
    Ok(max_page_id
        .map(|page_id| page_id.saturating_add(1))
        .unwrap_or_default())
}

#[cfg(test)]
mod next_page_id_scan_tests {
    use super::*;
    use tempfile::tempdir;

    /// Short enough to stay under the compression floor, so a flipped byte fails the CHECKSUM
    /// rather than the decompressor. The subject is a record whose HEADER is perfectly intact.
    fn small_payload(tag: u8) -> Vec<u8> {
        vec![tag; 64]
    }

    /// Write one slab holding a record per page id, and report where each record starts.
    fn slab_with(root: &Path, page_slab_id: u64, page_ids: &[u64]) -> Vec<usize> {
        let mut bytes = Vec::new();
        let mut starts = Vec::new();
        for (index, page_id) in page_ids.iter().enumerate() {
            starts.push(bytes.len());
            let encoded = encode_page_record(
                &small_payload(index as u8 + 1),
                *page_id,
                None,
                None,
                0,
                BlockStoreOptions::default(),
            )
            .expect("encode page record");
            bytes.extend_from_slice(&encoded.bytes);
        }
        fs::create_dir_all(root).expect("create slab root");
        fs::write(slab_path(root, page_slab_id), &bytes).expect("write slab");
        starts
    }

    /// A corrupt PAYLOAD must not lower the next page id.
    ///
    /// This is the case that tells the two scans apart, and it is a correctness point rather
    /// than a speed one. Taking the id from `inspect_slab` meant decoding every record, so a
    /// single unreadable payload ended the walk and every page id behind it went uncounted.
    /// `next_page_id` then came back LOW -- which is page-id reuse, and the stale reads that
    /// follow it. A header walk never consults the payload, so the ids behind it still stand.
    #[test]
    fn a_corrupt_payload_does_not_lower_the_next_page_id() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();
        let starts = slab_with(root, 0, &[4, 9]);

        // Flip a byte inside the SECOND record's payload, past its header. Magic, version,
        // lengths and page id are untouched, so the walk can still step over the record.
        let path = slab_path(root, 0);
        let mut bytes = fs::read(&path).expect("read slab");
        let payload_at = starts[1] + PAGE_RECORD_HEADER_LEN;
        bytes[payload_at] ^= 0xff;
        fs::write(&path, &bytes).expect("write slab");

        // The highest id is still 9, so the next one is 10. Decoding the payloads would have
        // halted at the damaged record and answered 5.
        assert_eq!(next_page_id_at(root).expect("scan"), 10);
    }

    /// A torn HEADER still ends the walk.
    ///
    /// The active-slab fencing in `with_options` is built on this scan halting early, so the
    /// change above must not turn into "read past anything". Once the magic is gone the file is
    /// no longer a chain of records and there is nothing further to trust.
    #[test]
    fn a_torn_header_halts_the_walk() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();
        let starts = slab_with(root, 0, &[4, 9]);

        let path = slab_path(root, 0);
        let mut bytes = fs::read(&path).expect("read slab");
        bytes[starts[1]] ^= 0xff;
        fs::write(&path, &bytes).expect("write slab");

        assert_eq!(next_page_id_at(root).expect("scan"), 5);
    }

    /// The ordinary case, which had no coverage at all: the counter clears the highest id
    /// across every slab, not merely the last one written.
    #[test]
    fn the_next_page_id_clears_every_slab() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();
        slab_with(root, 0, &[1, 6]);
        slab_with(root, 1, &[3, 2]);
        assert_eq!(next_page_id_at(root).expect("scan"), 7);
    }

    /// An empty root has no ids to clear, and must not answer as though it did.
    #[test]
    fn an_empty_root_starts_at_zero() {
        let dir = tempdir().expect("tempdir");
        assert_eq!(next_page_id_at(dir.path()).expect("scan"), 0);
    }
}
