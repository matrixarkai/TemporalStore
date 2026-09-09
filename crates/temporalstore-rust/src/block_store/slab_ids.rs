// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Slab id / address helpers + delayed-destroy slab scanning, extracted from block_store.rs.

use super::*;
use std::path::Path;

pub(crate) fn block_slab_utility_score(below_retention_floor: bool, is_current: bool, is_live: bool) -> u64 {
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
    block_slab_id: u64,
) -> Result<(), BlockStoreError> {
    let source = slab_path(root, block_slab_id);
    let trash_dir = delayed_destroy_dir(root);
    fs::create_dir_all(&trash_dir)?;
    let destination = delayed_destroy_path(root, block_slab_id);
    fs::rename(&source, &destination)?;
    sync_parent_dir(&source)?;
    sync_parent_dir(&destination)?;
    Ok(())
}

pub(crate) fn delayed_destroy_slab_ids_at(root: &Path) -> Result<Vec<u64>, BlockStoreError> {
    Ok(delayed_destroy_slab_reports_at(root)?
        .into_iter()
        .map(|report| report.block_slab_id)
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
                block_slab_id: id,
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
    reports.sort_by_key(|report| report.block_slab_id);
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

pub(crate) fn band_id_for_slab(block_slab_id: u64) -> u64 {
    let slab_target_bytes = effective_block_slab_target_bytes().max(1);
    let storage_band_size = storage_band_size_bytes().max(1);
    block_slab_id
        .saturating_mul(slab_target_bytes)
        .saturating_div(storage_band_size)
}

pub(crate) fn compact_slab_address_from_parts(block_slab_id: u64, offset: u64) -> Option<u64> {
    let band_id = u32::try_from(block_slab_id).ok()?;
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

/// `next_page_id_at`, but taking each sealed slab's answer from the band manifest.
///
/// The walk in `next_page_id_at` reads every page record header in every slab. Attributed on a
/// live-store copy, that is 1,723 MB of a 1,916 MB steady-state open -- 90% -- to compute one
/// integer, because live records average 148 bytes and a header walk over records that small never
/// leaves the read buffer.
///
/// The manifest already records `last_page_id` per slab. This uses it ONLY on evidence that the
/// file has not changed since it was recorded, and falls back to walking any slab it cannot prove:
///
///   * the active slab is always walked -- it is being appended to, so the manifest lags it
///   * a sealed band must not be corrupt, must record a `last_page_id`, and must still match the
///     file's size AND its verified mtime
///
/// The direction of error matters here and only one direction is safe. A value that is too HIGH
/// wastes page ids; a value that is too LOW re-uses them, which the open path calls out as leading
/// to stale reads. Every check above can only remove a slab from the fast path, never lower an
/// answer the walk would have given.
pub(crate) fn next_page_id_from_bands(
    root: &Path,
    bands: &BTreeMap<u64, BlockStoreBandDescriptor>,
    active_block_slab_id: u64,
) -> Result<u64, BlockStoreError> {
    let mut max_page_id: Option<u64> = None;
    for block_slab_id in slab_ids_at(root)? {
        let recorded = if block_slab_id == active_block_slab_id {
            None
        } else {
            bands.get(&block_slab_id).and_then(|band| {
                if band.has_corruption {
                    return None;
                }
                let last_page_id = band.last_page_id?;
                let verified_mtime = band.verified_source_mtime_unix_ms?;
                let path = slab_path(root, block_slab_id);
                let meta = fs::metadata(&path).ok()?;
                if meta.len() != band.physical_bytes {
                    return None;
                }
                if file_modified_unix_ms(&path) != Some(verified_mtime) {
                    return None;
                }
                Some(last_page_id)
            })
        };
        let slab_max = match recorded {
            Some(last_page_id) => Some(last_page_id),
            None => {
                let file = File::open(slab_path(root, block_slab_id))?;
                let slab_len = file.metadata()?.len();
                max_page_id_in_slab_file(file, slab_len, block_slab_id)?
            }
        };
        if let Some(slab_max) = slab_max {
            max_page_id = Some(max_page_id.map_or(slab_max, |current: u64| current.max(slab_max)));
        }
    }
    Ok(max_page_id
        .map(|page_id| page_id.saturating_add(1))
        .unwrap_or_default())
}

pub(crate) fn next_page_id_at(root: &Path) -> Result<u64, BlockStoreError> {
    let mut max_page_id = None;
    for block_slab_id in slab_ids_at(root)? {
        // Header-only walk. This used to `fs::read` the whole slab and hand it to `inspect_slab`,
        // which decodes and hashes every page to build a report that is discarded but for one
        // field. Opening the file and stepping it by record length reads the headers and nothing
        // else, so the cost stops tracking the size of the pages.
        let file = File::open(slab_path(root, block_slab_id))?;
        let slab_len = file.metadata()?.len();
        if let Some(slab_max) = max_page_id_in_slab_file(file, slab_len, block_slab_id)? {
            max_page_id =
                Some(max_page_id.map_or(slab_max, |current: u64| current.max(slab_max)));
        }
    }
    Ok(max_page_id
        .map(|page_id| page_id.saturating_add(1))
        .unwrap_or_default())
}

#[cfg(test)]
mod next_page_id_from_bands_tests {
    use super::*;
    use tempfile::tempdir;

    fn payload(tag: u8) -> Vec<u8> {
        vec![tag; 64]
    }

    fn write_slab(root: &Path, block_slab_id: u64, page_ids: &[u64]) {
        let mut bytes = Vec::new();
        for (index, page_id) in page_ids.iter().enumerate() {
            let encoded = encode_page_record(
                &payload(index as u8 + 1),
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
        fs::write(slab_path(root, block_slab_id), &bytes).expect("write slab");
    }

    /// A band that claims to describe the slab exactly as it currently is on disk.
    fn band_matching_disk(root: &Path, block_slab_id: u64, last_page_id: Option<u64>)
        -> BlockStoreBandDescriptor
    {
        let path = slab_path(root, block_slab_id);
        let meta = fs::metadata(&path).expect("slab metadata");
        BlockStoreBandDescriptor {
            band_id: block_slab_id,
            block_slab_id,
            state: BlockStoreBandState::Sealed,
            physical_bytes: meta.len(),
            logical_bytes: meta.len(),
            created_unix_ms: None,
            updated_unix_ms: None,
            first_page_id: None,
            last_page_id,
            readable_prefix_physical_bytes: meta.len(),
            verified_source_mtime_unix_ms: file_modified_unix_ms(&path),
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        }
    }

    #[test]
    fn a_current_manifest_gives_the_same_answer_as_the_walk() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        write_slab(root, 0, &[0, 1, 2]);
        write_slab(root, 1, &[3, 9, 4]);
        let mut bands = BTreeMap::new();
        bands.insert(0, band_matching_disk(root, 0, Some(2)));
        bands.insert(1, band_matching_disk(root, 1, Some(9)));
        // Active slab id 2 does not exist, so every slab here is sealed and provable.
        assert_eq!(
            next_page_id_from_bands(root, &bands, 2).expect("from bands"),
            next_page_id_at(root).expect("walk"),
            "the fast path disagreed with the walk it replaces"
        );
    }

    #[test]
    fn a_slab_appended_to_since_the_manifest_falls_back_to_the_walk() {
        // The dangerous case: a stale manifest that under-reports. If it were trusted, the store
        // would re-use page ids 3..9 and serve stale pages.
        let dir = tempdir().unwrap();
        let root = dir.path();
        write_slab(root, 0, &[0, 1, 2]);
        let stale = band_matching_disk(root, 0, Some(2));
        write_slab(root, 0, &[0, 1, 2, 9]);
        let mut bands = BTreeMap::new();
        bands.insert(0, stale);
        assert_eq!(
            next_page_id_from_bands(root, &bands, 99).expect("from bands"),
            10,
            "a manifest describing an older, shorter slab was trusted"
        );
    }

    #[test]
    fn a_band_without_a_recorded_page_id_falls_back() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        write_slab(root, 0, &[0, 1, 7]);
        let mut bands = BTreeMap::new();
        bands.insert(0, band_matching_disk(root, 0, None));
        assert_eq!(next_page_id_from_bands(root, &bands, 99).expect("from bands"), 8);
    }

    #[test]
    fn the_active_slab_is_walked_even_when_the_manifest_describes_it() {
        // Its entry cannot be current by construction: appends are landing in it.
        let dir = tempdir().unwrap();
        let root = dir.path();
        write_slab(root, 0, &[0, 1, 2]);
        let mut bands = BTreeMap::new();
        bands.insert(0, band_matching_disk(root, 0, Some(0)));
        assert_eq!(
            next_page_id_from_bands(root, &bands, 0).expect("from bands"),
            3,
            "the active slab was answered from the manifest instead of being walked"
        );
    }

    /// The two computations must agree on a real store's own manifest and slabs.
    ///
    /// A fixture only exercises the shapes I thought to build. A live manifest carries bands
    /// written by older engines, bands with no `last_page_id`, and mtimes that moved for reasons
    /// no fixture models -- and the failure this guards against (an answer that is too LOW) is
    /// invisible in any read-path result.
    #[test]
    fn the_manifest_and_the_walk_agree_on_a_real_store() {
        let Ok(pages) = std::env::var("MATRIXARK_LIVE_STORE_PAGES") else {
            println!("  set MATRIXARK_LIVE_STORE_PAGES to a pages/ directory to check this");
            return;
        };
        let root = Path::new(&pages);
        if !root.exists() {
            println!("  {pages} does not exist");
            return;
        }
        let bands = crate::block_store::band_manifest::load_band_manifest_at(root)
            .expect("load the band manifest");
        let active = latest_slab_id_at(root).expect("latest slab id");
        let walked = next_page_id_at(root).expect("walk");
        let from_bands = next_page_id_from_bands(root, &bands, active).expect("from bands");
        let provable = bands
            .values()
            .filter(|band| {
                band.last_page_id.is_some() && band.verified_source_mtime_unix_ms.is_some()
            })
            .count();
        println!(
            "  {} bands, {provable} carry both fields; active slab {active}; walk={walked} manifest={from_bands}",
            bands.len()
        );
        assert_eq!(
            walked, from_bands,
            "the manifest-backed answer disagrees with the walk on a real store"
        );
    }

    #[test]
    fn an_empty_root_is_zero_either_way() {
        let dir = tempdir().unwrap();
        let bands = BTreeMap::new();
        assert_eq!(next_page_id_from_bands(dir.path(), &bands, 0).expect("from bands"), 0);
    }
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
    fn slab_with(root: &Path, block_slab_id: u64, page_ids: &[u64]) -> Vec<usize> {
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
        fs::write(slab_path(root, block_slab_id), &bytes).expect("write slab");
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

        // Flip the LAST byte of the slab, which is the last byte of the SECOND record and so
        // inside its payload. A header is variable length now, so there is no constant to add
        // to the record start; the end of the last record is the one place guaranteed to be
        // past its header. Magic, lengths and page id are untouched, so the walk still steps
        // over the record.
        let path = slab_path(root, 0);
        let mut bytes = fs::read(&path).expect("read slab");
        let payload_at = bytes.len() - 1;
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
