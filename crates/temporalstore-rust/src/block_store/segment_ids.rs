//! Segment id / address helpers + delayed-destroy segment scanning, extracted from block_store.rs.

use super::*;
use std::ffi::OsStr;
use std::path::Path;

pub(crate) fn page_segment_utility_score(below_retention_floor: bool, is_current: bool, is_live: bool) -> u64 {
    if is_current || is_live {
        100
    } else if below_retention_floor {
        0
    } else {
        50
    }
}

pub(crate) fn move_segment_to_delayed_destroy(
    root: &Path,
    page_segment_id: u64,
) -> Result<(), BlockStoreError> {
    let source = segment_path(root, page_segment_id);
    let trash_dir = delayed_destroy_dir(root);
    fs::create_dir_all(&trash_dir)?;
    let destination = delayed_destroy_path(root, page_segment_id);
    fs::rename(&source, &destination)?;
    sync_parent_dir(&source)?;
    sync_parent_dir(&destination)?;
    Ok(())
}

pub(crate) fn delayed_destroy_segment_ids_at(root: &Path) -> Result<Vec<u64>, BlockStoreError> {
    Ok(delayed_destroy_segment_reports_at(root)?
        .into_iter()
        .map(|report| report.page_segment_id)
        .collect())
}

pub(crate) fn delayed_destroy_segment_reports_at(
    root: &Path,
) -> Result<Vec<BlockStoreDelayedDestroySegmentReport>, BlockStoreError> {
    let trash_dir = delayed_destroy_dir(root);
    let mut reports = Vec::new();
    if !trash_dir.exists() {
        return Ok(reports);
    }
    for entry in fs::read_dir(trash_dir)? {
        let entry = entry?;
        if let Some(id) = delayed_destroy_segment_id_from_name(&entry.file_name()) {
            let metadata = entry.metadata().ok();
            reports.push(BlockStoreDelayedDestroySegmentReport {
                page_segment_id: id,
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
    reports.sort_by_key(|report| report.page_segment_id);
    Ok(reports)
}

pub(crate) fn delayed_destroy_segment_id_from_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let id = name
        .strip_prefix("page_segment_")?
        .strip_suffix(name.split_once(".seg.deleted.")?.1)?
        .strip_suffix(".seg.deleted.")?;
    id.parse::<u64>().ok()
}

pub(crate) fn extent_id_for_segment(page_segment_id: u64) -> u64 {
    let segment_target_bytes = effective_block_segment_target_bytes().max(1);
    let storage_zone_size = storage_zone_size_bytes().max(1);
    page_segment_id
        .saturating_mul(segment_target_bytes)
        .saturating_div(storage_zone_size)
}

pub(crate) fn compact_segment_address_from_parts(page_segment_id: u64, offset: u64) -> Option<u64> {
    let extent_id = u32::try_from(page_segment_id).ok()?;
    let extent_offset = u32::try_from(offset).ok()?;
    Some(((extent_id as u64) << 32) | extent_offset as u64)
}

pub(crate) fn compact_extract_extent_id(address: u64) -> u32 {
    (address >> 32) as u32
}

pub(crate) fn compact_extract_extent_offset(address: u64) -> u32 {
    (address & 0xFFFF_FFFF) as u32
}

pub(crate) fn segment_ids_at(root: &Path) -> Result<Vec<u64>, BlockStoreError> {
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

pub(crate) fn latest_segment_id_at(root: &Path) -> Result<u64, BlockStoreError> {
    Ok(segment_ids_at(root)?.into_iter().max().unwrap_or_default())
}

pub(crate) fn next_page_id_at(root: &Path) -> Result<u64, BlockStoreError> {
    let mut max_page_id = None;
    for page_segment_id in segment_ids_at(root)? {
        let bytes = fs::read(segment_path(root, page_segment_id))?;
        if let Some(segment_max) = inspect_segment(&bytes, page_segment_id).last_page_id {
            max_page_id =
                Some(max_page_id.map_or(segment_max, |current: u64| current.max(segment_max)));
        }
    }
    Ok(max_page_id
        .map(|page_id| page_id.saturating_add(1))
        .unwrap_or_default())
}

