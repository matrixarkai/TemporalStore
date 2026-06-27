use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

mod paths;
mod record;

use paths::{
    delayed_destroy_dir, delayed_destroy_path, extent_manifest_path, file_created_unix_ms,
    file_modified_unix_ms, legacy_zone_manifest_path, now_unix_ms, segment_path, sync_dir,
    sync_parent_dir, system_time_unix_ms, unique_temp_path,
};
use record::{
    decode_page_record, default_page_record_compression_enabled,
    default_page_record_compression_level, default_page_record_compression_min_bytes,
    encode_page_record, inspect_segment, logical_range_from_segment, sha256_hex, summarize_segment,
    PageRecordCompression,
};
#[cfg(test)]
use record::{
    PAGE_RECORD_COMPRESSION_NONE, PAGE_RECORD_COMPRESSION_ZSTD, PAGE_RECORD_HEADER_LEN,
    PAGE_RECORD_MAGIC, PAGE_RECORD_VERSION,
};

#[derive(Debug, Error)]
pub enum BlockStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "page checksum mismatch for segment {page_segment_id} offset {offset} length {length}: expected {expected}, got {actual}"
    )]
    ChecksumMismatch {
        page_segment_id: u64,
        offset: u64,
        length: u64,
        expected: String,
        actual: String,
    },
    #[error("corrupt page envelope for segment {page_segment_id} offset {offset}: {reason}")]
    CorruptPageEnvelope {
        page_segment_id: u64,
        offset: u64,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockAddress {
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_slot: Option<u32>,
    #[serde(default, alias = "zone_id", skip_serializing_if = "Option::is_none")]
    pub extent_id: Option<u64>,
    #[serde(default, alias = "checksum", skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreStats {
    pub writes: u64,
    pub reads: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    #[serde(default)]
    pub logical_bytes_written: u64,
    #[serde(default)]
    pub logical_bytes_read: u64,
    #[serde(default)]
    pub compressed_records_written: u64,
    #[serde(default)]
    pub compressed_records_read: u64,
    #[serde(default)]
    pub compression_bytes_saved: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreOptions {
    #[serde(default = "default_page_record_compression_enabled")]
    pub compression_enabled: bool,
    #[serde(default = "default_page_record_compression_min_bytes")]
    pub compression_min_bytes: usize,
    #[serde(default = "default_page_record_compression_level")]
    pub compression_level: i32,
}

impl Default for BlockStoreOptions {
    fn default() -> Self {
        Self {
            compression_enabled: default_page_record_compression_enabled(),
            compression_min_bytes: default_page_record_compression_min_bytes(),
            compression_level: default_page_record_compression_level(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcReport {
    pub retain_from_page_segment_id: u64,
    pub removed_page_segment_ids: Vec<u64>,
    pub retained_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub removed_physical_bytes: u64,
    #[serde(default)]
    pub retained_physical_bytes: u64,
    #[serde(default)]
    pub delayed_destroy_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub delayed_destroy_physical_bytes: u64,
    #[serde(default)]
    pub retained_live_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub retained_live_physical_bytes: u64,
    #[serde(default)]
    pub retained_current_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub retained_current_physical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcUtilityCandidate {
    pub page_segment_id: u64,
    pub bytes: u64,
    pub utility_score: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcPolicy {
    #[serde(default)]
    pub max_destroy_segments: usize,
    #[serde(default)]
    pub max_destroy_physical_bytes: u64,
    #[serde(default)]
    pub max_utility_score: Option<u64>,
    #[serde(default)]
    pub min_age_ms: Option<u64>,
}

impl BlockStoreGcPolicy {
    pub fn max_segments(max_destroy_segments: usize) -> Self {
        Self {
            max_destroy_segments,
            max_destroy_physical_bytes: 0,
            max_utility_score: None,
            min_age_ms: None,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcPolicyPlan {
    pub retain_from_page_segment_id: u64,
    pub selected_page_segment_ids: Vec<u64>,
    pub selected_physical_bytes: u64,
    pub candidate_count: usize,
    pub candidate_physical_bytes: u64,
    pub skipped_by_policy_count: usize,
    pub skipped_by_policy_physical_bytes: u64,
    pub skipped_by_budget_count: usize,
    pub skipped_by_budget_physical_bytes: u64,
    pub candidates: Vec<BlockStoreGcUtilityCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreDelayedDestroySegmentReport {
    pub page_segment_id: u64,
    pub physical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStorePurgeDelayedDestroyReport {
    pub purged_page_segment_ids: Vec<u64>,
    pub purged_physical_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockStoreExtentState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreExtentDescriptor {
    #[serde(alias = "zone_id")]
    pub extent_id: u64,
    pub page_segment_id: u64,
    pub state: BlockStoreExtentState,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page_id: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreExtentSummary {
    #[serde(alias = "active_zones")]
    pub active_extents: u64,
    #[serde(alias = "sealed_zones")]
    pub sealed_extents: u64,
    #[serde(alias = "delayed_destroy_zones")]
    pub delayed_destroy_extents: u64,
    #[serde(alias = "purged_zones")]
    pub purged_extents: u64,
    pub active_physical_bytes: u64,
    pub sealed_physical_bytes: u64,
    pub delayed_destroy_physical_bytes: u64,
    pub purged_physical_bytes: u64,
    pub live_physical_bytes: u64,
    pub reclaimable_physical_bytes: u64,
    pub total_known_physical_bytes: u64,
    #[serde(
        default,
        alias = "oldest_known_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_known_extent_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_known_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_known_extent_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_live_extent_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_live_extent_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_reclaimable_extent_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_reclaimable_extent_age_ms: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBackedExtentRuntimeReport {
    pub runtime_ready: bool,
    #[serde(alias = "zone_count")]
    pub extent_count: u64,
    #[serde(alias = "active_zones")]
    pub active_extents: u64,
    #[serde(alias = "sealed_zones")]
    pub sealed_extents: u64,
    #[serde(alias = "delayed_destroy_zones")]
    pub delayed_destroy_extents: u64,
    #[serde(alias = "purged_zones")]
    pub purged_extents: u64,
    pub stream_segment_count: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub logical_stream_read_ready: bool,
    pub append_roll_ready: bool,
    #[serde(alias = "zone_manifest_ready")]
    pub extent_manifest_ready: bool,
    pub envelope_checksum_ready: bool,
    pub compression_stream_ready: bool,
    pub delayed_destroy_ready: bool,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSegmentReport {
    pub page_segment_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub page_count: u64,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    #[serde(default)]
    pub has_corruption: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error_offset: Option<u64>,
    #[serde(default)]
    pub object_count: u64,
    #[serde(default)]
    pub routing_slot_count: u64,
    pub compressed_records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_routing_slot: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_routing_slot: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BlockStoreExtentManifest {
    version: u32,
    #[serde(alias = "zones")]
    extents: Vec<BlockStoreExtentDescriptor>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreRollReport {
    pub previous_page_segment_id: u64,
    pub new_page_segment_id: u64,
}

#[derive(Debug, Clone)]
pub struct LocalBlockStore {
    inner: Arc<Mutex<BlockStoreInner>>,
}

#[derive(Debug)]
struct BlockStoreInner {
    root: PathBuf,
    page_segment_id: u64,
    write_offset: u64,
    next_page_id: u64,
    options: BlockStoreOptions,
    extents: BTreeMap<u64, BlockStoreExtentDescriptor>,
    stats: BlockStoreStats,
}

impl LocalBlockStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_options(root, BlockStoreOptions::default())
    }

    pub fn with_options(root: impl Into<PathBuf>, options: BlockStoreOptions) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        let page_segment_id = latest_segment_id_at(&root).unwrap_or_default();
        let write_offset = segment_path(&root, page_segment_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let next_page_id = next_page_id_at(&root).unwrap_or_default();
        let manifest_exists =
            extent_manifest_path(&root).exists() || legacy_zone_manifest_path(&root).exists();
        let mut extents = if manifest_exists {
            load_extent_manifest_at(&root)
                .or_else(|_| rebuild_extent_manifest_at(&root))
                .unwrap_or_default()
        } else {
            rebuild_extent_manifest_at(&root).unwrap_or_default()
        };
        ensure_extent_descriptor(
            &mut extents,
            &root,
            page_segment_id,
            BlockStoreExtentState::Active,
        );
        if !manifest_exists {
            let _ = persist_extent_manifest(&root, &extents);
        }
        Self {
            inner: Arc::new(Mutex::new(BlockStoreInner {
                root,
                page_segment_id,
                write_offset,
                next_page_id,
                options,
                extents,
                stats: BlockStoreStats::default(),
            })),
        }
    }

    pub fn append(&self, bytes: &[u8]) -> Result<BlockAddress, BlockStoreError> {
        self.append_with_object_id(bytes, None)
    }

    pub fn append_with_object_id(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
    ) -> Result<BlockAddress, BlockStoreError> {
        self.append_with_page_metadata(bytes, object_id, None)
    }

    pub fn append_with_page_metadata(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
        routing_slot: Option<u32>,
    ) -> Result<BlockAddress, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = segment_path(&inner.root, inner.page_segment_id);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let page_id = inner.next_page_id;
        let extent_id = extent_id_for_segment(inner.page_segment_id);
        let record = encode_page_record(
            bytes,
            page_id,
            object_id,
            routing_slot,
            extent_id,
            inner.options,
        )?;
        let address = BlockAddress {
            page_segment_id: inner.page_segment_id,
            offset: inner.write_offset,
            length: record.bytes.len() as u64,
            page_id: Some(page_id),
            object_id,
            routing_slot,
            extent_id: Some(extent_id),
            sha256: Some(sha256_hex(bytes)),
        };
        file.write_all(&record.bytes)?;
        file.flush()?;
        file.sync_data()?;
        inner.next_page_id = inner.next_page_id.saturating_add(1);
        inner.write_offset += address.length;
        let page_segment_id = inner.page_segment_id;
        let write_offset = inner.write_offset;
        upsert_extent_after_append(
            &mut inner.extents,
            page_segment_id,
            write_offset,
            record.logical_len as u64,
            page_id,
        );
        persist_extent_manifest(&inner.root, &inner.extents)?;
        inner.stats.writes += 1;
        inner.stats.bytes_written += address.length;
        inner.stats.logical_bytes_written += record.logical_len as u64;
        if record.compression == PageRecordCompression::Zstd {
            inner.stats.compressed_records_written += 1;
            inner.stats.compression_bytes_saved +=
                record.logical_len.saturating_sub(record.stored_len) as u64;
        }
        Ok(address)
    }

    pub fn roll_segment(&self) -> Result<BlockStoreRollReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let previous_page_segment_id = inner.page_segment_id;
        let next_from_current = inner.page_segment_id.saturating_add(1);
        let next_from_disk = segment_ids_at(&inner.root)?
            .into_iter()
            .max()
            .map(|id| id.saturating_add(1))
            .unwrap_or_default();
        inner.page_segment_id = next_from_current.max(next_from_disk);
        inner.write_offset = 0;
        let path = segment_path(&inner.root, inner.page_segment_id);
        let file = File::create(&path)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
        let transition_unix_ms = now_unix_ms();
        if let Some(previous) = inner.extents.get_mut(&previous_page_segment_id) {
            previous.state = BlockStoreExtentState::Sealed;
            previous.updated_unix_ms = Some(transition_unix_ms);
        }
        let new_zone = BlockStoreExtentDescriptor {
            extent_id: extent_id_for_segment(inner.page_segment_id),
            page_segment_id: inner.page_segment_id,
            state: BlockStoreExtentState::Active,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(transition_unix_ms),
            updated_unix_ms: Some(transition_unix_ms),
            first_page_id: None,
            last_page_id: None,
        };
        let page_segment_id = inner.page_segment_id;
        inner.extents.insert(page_segment_id, new_zone);
        persist_extent_manifest(&inner.root, &inner.extents)?;
        Ok(BlockStoreRollReport {
            previous_page_segment_id,
            new_page_segment_id: inner.page_segment_id,
        })
    }

    pub fn read(&self, address: &BlockAddress) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = segment_path(&inner.root, address.page_segment_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(address.offset))?;
        let mut bytes = vec![0; address.length as usize];
        file.read_exact(&mut bytes)?;
        let decoded = decode_page_record(&bytes, address)?;
        let bytes = decoded.payload;
        if let Some(expected) = &address.sha256 {
            let actual = sha256_hex(&bytes);
            if &actual != expected {
                return Err(BlockStoreError::ChecksumMismatch {
                    page_segment_id: address.page_segment_id,
                    offset: address.offset,
                    length: address.length,
                    expected: expected.clone(),
                    actual,
                });
            }
        }
        inner.stats.reads += 1;
        inner.stats.bytes_read += address.length;
        inner.stats.logical_bytes_read += decoded.logical_len as u64;
        if decoded.compression == PageRecordCompression::Zstd {
            inner.stats.compressed_records_read += 1;
        }
        Ok(bytes)
    }

    pub fn read_range(
        &self,
        page_segment_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = segment_path(&inner.root, page_segment_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; size as usize];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        inner.stats.reads += 1;
        inner.stats.bytes_read += read as u64;
        Ok(bytes)
    }

    pub fn read_logical_range(
        &self,
        page_segment_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = segment_path(&inner.root, page_segment_id);
        let segment = fs::read(path)?;
        let range = logical_range_from_segment(&segment, page_segment_id, offset, size)?;
        let bytes = range.bytes;
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        inner.stats.logical_bytes_read += bytes.len() as u64;
        inner.stats.compressed_records_read += range.compressed_records_read;
        Ok(bytes)
    }

    pub fn read_segment(&self, page_segment_id: u64) -> Result<Vec<u8>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        Ok(fs::read(segment_path(&root, page_segment_id))?)
    }

    pub fn install_segment(
        &self,
        page_segment_id: u64,
        bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = segment_path(&inner.root, page_segment_id);
        let temp_path = path.with_extension(format!(
            "seg.tmp.{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        ));
        {
            let mut temp = File::create(&temp_path)?;
            temp.write_all(bytes)?;
            temp.flush()?;
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        if page_segment_id >= inner.page_segment_id {
            inner.page_segment_id = page_segment_id;
            inner.write_offset = bytes.len() as u64;
        }
        let extent_summary = summarize_segment(bytes, page_segment_id)?;
        if let Some(max_page_id) = extent_summary.last_page_id {
            inner.next_page_id = inner.next_page_id.max(max_page_id.saturating_add(1));
        }
        let is_current_segment = page_segment_id == inner.page_segment_id;
        let now = now_unix_ms();
        inner.extents.insert(
            page_segment_id,
            BlockStoreExtentDescriptor {
                extent_id: extent_id_for_segment(page_segment_id),
                page_segment_id,
                state: if is_current_segment {
                    BlockStoreExtentState::Active
                } else {
                    BlockStoreExtentState::Sealed
                },
                physical_bytes: bytes.len() as u64,
                logical_bytes: extent_summary.logical_bytes,
                created_unix_ms: Some(
                    file_modified_unix_ms(&path)
                        .or_else(|| file_created_unix_ms(&path))
                        .unwrap_or(now),
                ),
                updated_unix_ms: Some(now),
                first_page_id: extent_summary.first_page_id,
                last_page_id: extent_summary.last_page_id,
            },
        );
        if is_current_segment {
            for extent in inner.extents.values_mut() {
                if extent.page_segment_id != page_segment_id
                    && extent.state == BlockStoreExtentState::Active
                {
                    extent.state = BlockStoreExtentState::Sealed;
                }
            }
        }
        persist_extent_manifest(&inner.root, &inner.extents)?;
        Ok(())
    }

    pub fn segment_ids(&self) -> Result<Vec<u64>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
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

    pub fn gc_segments_before(
        &self,
        retain_from_page_segment_id: u64,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs(retain_from_page_segment_id, std::iter::empty())
    }

    pub fn gc_segments_before_with_live_refs(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs_mode(
            retain_from_page_segment_id,
            live_page_segment_ids,
            false,
        )
    }

    pub fn gc_segments_before_with_live_refs_delayed_destroy(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs_mode(
            retain_from_page_segment_id,
            live_page_segment_ids,
            true,
        )
    }

    pub fn gc_segments_before_with_live_refs_utility(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        max_destroy_segments: usize,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        if max_destroy_segments == 0 {
            return self.gc_segments_before_with_live_refs_selected(
                retain_from_page_segment_id,
                live_page_segment_ids,
                delayed_destroy,
                Some(BTreeSet::new()),
            );
        }
        self.gc_segments_before_with_live_refs_policy(
            retain_from_page_segment_id,
            live_page_segment_ids,
            BlockStoreGcPolicy::max_segments(max_destroy_segments),
            delayed_destroy,
        )
    }

    pub fn gc_segments_before_with_live_refs_policy(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        policy: BlockStoreGcPolicy,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let live_page_segment_ids = live_page_segment_ids.into_iter().collect::<BTreeSet<_>>();
        let selected = self
            .gc_policy_plan(
                retain_from_page_segment_id,
                live_page_segment_ids.iter().copied(),
                &policy,
            )?
            .selected_page_segment_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        self.gc_segments_before_with_live_refs_selected(
            retain_from_page_segment_id,
            live_page_segment_ids,
            delayed_destroy,
            Some(selected),
        )
    }

    pub fn gc_policy_plan(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        policy: &BlockStoreGcPolicy,
    ) -> Result<BlockStoreGcPolicyPlan, BlockStoreError> {
        let candidates =
            self.gc_utility_candidates(retain_from_page_segment_id, live_page_segment_ids)?;
        let mut selected_page_segment_ids = Vec::new();
        let mut selected_physical_bytes = 0_u64;
        let candidate_physical_bytes = candidates.iter().map(|candidate| candidate.bytes).sum();
        let mut skipped_by_policy_count = 0_usize;
        let mut skipped_by_policy_physical_bytes = 0_u64;
        let mut skipped_by_budget_count = 0_usize;
        let mut skipped_by_budget_physical_bytes = 0_u64;

        for candidate in &candidates {
            let utility_allowed = policy
                .max_utility_score
                .map(|max_score| candidate.utility_score <= max_score)
                .unwrap_or(true);
            let age_allowed = policy
                .min_age_ms
                .map(|min_age| candidate.age_ms.unwrap_or_default() >= min_age)
                .unwrap_or(true);
            if !utility_allowed || !age_allowed {
                skipped_by_policy_count += 1;
                skipped_by_policy_physical_bytes =
                    skipped_by_policy_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }

            if policy.max_destroy_segments > 0
                && selected_page_segment_ids.len() >= policy.max_destroy_segments
            {
                skipped_by_budget_count += 1;
                skipped_by_budget_physical_bytes =
                    skipped_by_budget_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }
            if policy.max_destroy_physical_bytes > 0
                && selected_physical_bytes.saturating_add(candidate.bytes)
                    > policy.max_destroy_physical_bytes
            {
                skipped_by_budget_count += 1;
                skipped_by_budget_physical_bytes =
                    skipped_by_budget_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }

            selected_page_segment_ids.push(candidate.page_segment_id);
            selected_physical_bytes = selected_physical_bytes.saturating_add(candidate.bytes);
        }

        Ok(BlockStoreGcPolicyPlan {
            retain_from_page_segment_id,
            selected_page_segment_ids,
            selected_physical_bytes,
            candidate_count: candidates.len(),
            candidate_physical_bytes,
            skipped_by_policy_count,
            skipped_by_policy_physical_bytes,
            skipped_by_budget_count,
            skipped_by_budget_physical_bytes,
            candidates,
        })
    }

    pub fn gc_utility_candidates(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<BlockStoreGcUtilityCandidate>, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let current_page_segment_id = inner.page_segment_id;
        let live_page_segment_ids = live_page_segment_ids.into_iter().collect::<BTreeSet<_>>();
        let mut candidates = Vec::new();
        let now = now_unix_ms();
        for page_segment_id in segment_ids_at(&inner.root)? {
            let below_retention_floor = page_segment_id < retain_from_page_segment_id;
            let is_current = page_segment_id == current_page_segment_id;
            let is_live = live_page_segment_ids.contains(&page_segment_id);
            if below_retention_floor && !is_current && !is_live {
                let bytes = segment_path(&inner.root, page_segment_id)
                    .metadata()
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                let extent = inner.extents.get(&page_segment_id);
                let created_unix_ms = extent.and_then(|extent| extent.created_unix_ms);
                let updated_unix_ms = extent.and_then(|extent| extent.updated_unix_ms);
                let age_ms = updated_unix_ms
                    .or(created_unix_ms)
                    .map(|timestamp| now.saturating_sub(timestamp));
                candidates.push(BlockStoreGcUtilityCandidate {
                    page_segment_id,
                    bytes,
                    utility_score: page_segment_utility_score(
                        below_retention_floor,
                        is_current,
                        is_live,
                    ),
                    created_unix_ms,
                    updated_unix_ms,
                    age_ms,
                });
            }
        }
        candidates.sort_by(|left, right| {
            left.utility_score
                .cmp(&right.utility_score)
                .then_with(|| right.bytes.cmp(&left.bytes))
                .then_with(|| {
                    right
                        .age_ms
                        .unwrap_or_default()
                        .cmp(&left.age_ms.unwrap_or_default())
                })
                .then_with(|| left.page_segment_id.cmp(&right.page_segment_id))
        });
        Ok(candidates)
    }

    fn gc_segments_before_with_live_refs_mode(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_segments_before_with_live_refs_selected(
            retain_from_page_segment_id,
            live_page_segment_ids,
            delayed_destroy,
            None,
        )
    }

    fn gc_segments_before_with_live_refs_selected(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        delayed_destroy: bool,
        selected_page_segment_ids: Option<BTreeSet<u64>>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        if delayed_destroy {
            fs::create_dir_all(delayed_destroy_dir(&inner.root))?;
        }
        let current_page_segment_id = inner.page_segment_id;
        let live_page_segment_ids = live_page_segment_ids.into_iter().collect::<BTreeSet<_>>();
        let mut removed = Vec::new();
        let mut retained = Vec::new();
        let mut delayed_destroy_ids = Vec::new();
        let mut retained_live = Vec::new();
        let mut retained_current = Vec::new();
        let mut removed_physical_bytes = 0;
        let mut retained_physical_bytes = 0;
        let mut delayed_destroy_physical_bytes = 0;
        let mut retained_live_physical_bytes = 0;
        let mut retained_current_physical_bytes = 0;
        for page_segment_id in segment_ids_at(&inner.root)? {
            let segment_physical_bytes = segment_path(&inner.root, page_segment_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let below_retention_floor = page_segment_id < retain_from_page_segment_id;
            let is_current = page_segment_id == current_page_segment_id;
            let is_live = live_page_segment_ids.contains(&page_segment_id);
            let is_selected = selected_page_segment_ids
                .as_ref()
                .map(|selected| selected.contains(&page_segment_id))
                .unwrap_or(true);
            if below_retention_floor && !is_current && !is_live && is_selected {
                removed_physical_bytes += segment_physical_bytes;
                if delayed_destroy {
                    move_segment_to_delayed_destroy(&inner.root, page_segment_id)?;
                    set_extent_state(
                        &mut inner.extents,
                        page_segment_id,
                        BlockStoreExtentState::DelayedDestroy,
                    );
                    delayed_destroy_ids.push(page_segment_id);
                    delayed_destroy_physical_bytes += segment_physical_bytes;
                } else {
                    fs::remove_file(segment_path(&inner.root, page_segment_id))?;
                    set_extent_state(
                        &mut inner.extents,
                        page_segment_id,
                        BlockStoreExtentState::Purged,
                    );
                }
                removed.push(page_segment_id);
            } else {
                if below_retention_floor && is_current {
                    retained_current.push(page_segment_id);
                    retained_current_physical_bytes += segment_physical_bytes;
                }
                if below_retention_floor && is_live {
                    retained_live.push(page_segment_id);
                    retained_live_physical_bytes += segment_physical_bytes;
                }
                retained_physical_bytes += segment_physical_bytes;
                retained.push(page_segment_id);
            }
        }
        persist_extent_manifest(&inner.root, &inner.extents)?;
        Ok(BlockStoreGcReport {
            retain_from_page_segment_id,
            removed_page_segment_ids: removed,
            retained_page_segment_ids: retained,
            removed_physical_bytes,
            retained_physical_bytes,
            delayed_destroy_page_segment_ids: delayed_destroy_ids,
            delayed_destroy_physical_bytes,
            retained_live_page_segment_ids: retained_live,
            retained_live_physical_bytes,
            retained_current_page_segment_ids: retained_current,
            retained_current_physical_bytes,
        })
    }

    pub fn delayed_destroy_segment_ids(&self) -> Result<Vec<u64>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        delayed_destroy_segment_ids_at(&root)
    }

    pub fn delayed_destroy_segment_reports(
        &self,
    ) -> Result<Vec<BlockStoreDelayedDestroySegmentReport>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        delayed_destroy_segment_reports_at(&root)
    }

    pub fn purge_delayed_destroy_segments(&self) -> Result<Vec<u64>, BlockStoreError> {
        Ok(self
            .purge_delayed_destroy_segments_with_report()?
            .purged_page_segment_ids)
    }

    pub fn purge_delayed_destroy_segments_with_report(
        &self,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let trash_dir = delayed_destroy_dir(&inner.root);
        let mut purged = Vec::new();
        let mut purged_physical_bytes = 0;
        if !trash_dir.exists() {
            return Ok(BlockStorePurgeDelayedDestroyReport::default());
        }
        for entry in fs::read_dir(&trash_dir)? {
            let entry = entry?;
            let Some(id) = delayed_destroy_segment_id_from_name(&entry.file_name()) else {
                continue;
            };
            purged_physical_bytes += entry
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            fs::remove_file(entry.path())?;
            set_extent_state(&mut inner.extents, id, BlockStoreExtentState::Purged);
            purged.push(id);
        }
        purged.sort_unstable();
        sync_dir(&trash_dir)?;
        persist_extent_manifest(&inner.root, &inner.extents)?;
        Ok(BlockStorePurgeDelayedDestroyReport {
            purged_page_segment_ids: purged,
            purged_physical_bytes,
        })
    }

    pub fn extent_descriptors(&self) -> Vec<BlockStoreExtentDescriptor> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .extents
            .values()
            .cloned()
            .collect()
    }

    pub fn extent_summary(&self) -> BlockStoreExtentSummary {
        summarize_extents(
            &self
                .inner
                .lock()
                .expect("block store lock poisoned")
                .extents,
        )
    }

    pub fn stream_backed_extent_runtime_report(
        &self,
    ) -> Result<StreamBackedExtentRuntimeReport, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let extents = inner.extents.clone();
        let root = inner.root.clone();
        let options = inner.options;
        drop(inner);

        let summary = summarize_extents(&extents);
        let segment_reports = {
            let mut reports = Vec::new();
            for id in segment_ids_at(&root)? {
                reports.push(inspect_segment(&fs::read(segment_path(&root, id))?, id));
            }
            reports
        };
        let stream_segment_count = segment_reports
            .iter()
            .filter(|report| report.page_count > 0 || report.physical_bytes > 0)
            .count() as u64;
        let physical_bytes = segment_reports
            .iter()
            .map(|report| report.physical_bytes)
            .sum::<u64>();
        let logical_bytes = segment_reports
            .iter()
            .map(|report| report.logical_bytes)
            .sum::<u64>();
        let logical_stream_read_ready = segment_reports.iter().any(|report| report.page_count > 0);
        let append_roll_ready = summary.active_extents == 1
            && summary
                .sealed_extents
                .saturating_add(summary.delayed_destroy_extents)
                > 0;
        let extent_manifest_ready = extent_manifest_path(&root).exists()
            && !extents.is_empty()
            && extents
                .values()
                .all(|extent| extent.extent_id == extent_id_for_segment(extent.page_segment_id));
        let envelope_checksum_ready = segment_reports
            .iter()
            .filter(|report| report.page_count > 0)
            .all(|report| !report.has_corruption && report.logical_bytes > 0);
        let compression_stream_ready = options.compression_enabled
            && segment_reports
                .iter()
                .any(|report| report.compressed_records > 0);
        let delayed_destroy_ready =
            summary.delayed_destroy_extents > 0 || summary.purged_extents > 0;

        let mut blockers = Vec::new();
        if !logical_stream_read_ready {
            blockers.push("no readable page stream records found".to_string());
        }
        if !append_roll_ready {
            blockers.push(
                "append/roll extent lifecycle has not produced active plus sealed extents"
                    .to_string(),
            );
        }
        if !extent_manifest_ready {
            blockers.push("extent manifest is missing or inconsistent".to_string());
        }
        if !envelope_checksum_ready {
            blockers.push("stream record envelope/checksum inspection is not clean".to_string());
        }

        let runtime_ready = blockers.is_empty();
        Ok(StreamBackedExtentRuntimeReport {
            runtime_ready,
            extent_count: extents.len() as u64,
            active_extents: summary.active_extents,
            sealed_extents: summary.sealed_extents,
            delayed_destroy_extents: summary.delayed_destroy_extents,
            purged_extents: summary.purged_extents,
            stream_segment_count,
            physical_bytes,
            logical_bytes,
            logical_stream_read_ready,
            append_roll_ready,
            extent_manifest_ready,
            envelope_checksum_ready,
            compression_stream_ready,
            delayed_destroy_ready,
            blockers,
            evidence: vec![
                "page records are appended as self-describing stream envelopes".to_string(),
                "logical stream reads span records while skipping envelopes and decompression"
                    .to_string(),
                "segment roll seals the previous extent and opens a new active extent".to_string(),
                "extent manifest persists active/sealed/delayed-destroy/purged state".to_string(),
            ],
        })
    }

    pub fn segment_reports(&self) -> Result<Vec<BlockStoreSegmentReport>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        let mut reports = Vec::new();
        for page_segment_id in segment_ids_at(&root)? {
            let bytes = fs::read(segment_path(&root, page_segment_id))?;
            reports.push(inspect_segment(&bytes, page_segment_id));
        }
        Ok(reports)
    }

    pub fn stats(&self) -> BlockStoreStats {
        self.inner.lock().expect("block store lock poisoned").stats
    }
}

impl Default for LocalBlockStore {
    fn default() -> Self {
        Self::new(unique_temp_path("pages"))
    }
}

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreError; page naming remains only for legacy compatibility"
)]
pub type PageStoreError = BlockStoreError;

#[deprecated(
    since = "0.1.0",
    note = "use BlockAddress; page naming remains only for legacy compatibility"
)]
pub type PageAddress = BlockAddress;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreStats; page naming remains only for legacy compatibility"
)]
pub type PageStoreStats = BlockStoreStats;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreOptions; page naming remains only for legacy compatibility"
)]
pub type PageStoreOptions = BlockStoreOptions;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreGcReport; page naming remains only for legacy compatibility"
)]
pub type PageStoreGcReport = BlockStoreGcReport;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreGcUtilityCandidate; page naming remains only for legacy compatibility"
)]
pub type PageStoreGcUtilityCandidate = BlockStoreGcUtilityCandidate;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreGcPolicy; page naming remains only for legacy compatibility"
)]
pub type PageStoreGcPolicy = BlockStoreGcPolicy;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreGcPolicyPlan; page naming remains only for legacy compatibility"
)]
pub type PageStoreGcPolicyPlan = BlockStoreGcPolicyPlan;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreDelayedDestroySegmentReport; page naming remains only for legacy compatibility"
)]
pub type PageStoreDelayedDestroySegmentReport = BlockStoreDelayedDestroySegmentReport;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStorePurgeDelayedDestroyReport; page naming remains only for legacy compatibility"
)]
pub type PageStorePurgeDelayedDestroyReport = BlockStorePurgeDelayedDestroyReport;

#[deprecated(since = "0.1.0", note = "use BlockStoreExtentState")]
pub type BlockStoreZoneState = BlockStoreExtentState;

#[deprecated(since = "0.1.0", note = "use BlockStoreExtentDescriptor")]
pub type BlockStoreZoneDescriptor = BlockStoreExtentDescriptor;

#[deprecated(since = "0.1.0", note = "use BlockStoreExtentSummary")]
pub type BlockStoreZoneSummary = BlockStoreExtentSummary;

#[deprecated(since = "0.1.0", note = "use StreamBackedExtentRuntimeReport")]
pub type StreamBackedZoneRuntimeReport = StreamBackedExtentRuntimeReport;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreExtentState; page naming remains only for legacy compatibility"
)]
pub type PageStoreExtentState = BlockStoreExtentState;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreExtentState; zone naming remains only for legacy compatibility"
)]
pub type PageStoreZoneState = BlockStoreExtentState;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreExtentDescriptor; page naming remains only for legacy compatibility"
)]
pub type PageStoreExtentDescriptor = BlockStoreExtentDescriptor;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreExtentDescriptor; zone naming remains only for legacy compatibility"
)]
pub type PageStoreZoneDescriptor = BlockStoreExtentDescriptor;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreExtentSummary; page naming remains only for legacy compatibility"
)]
pub type PageStoreExtentSummary = BlockStoreExtentSummary;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreExtentSummary; zone naming remains only for legacy compatibility"
)]
pub type PageStoreZoneSummary = BlockStoreExtentSummary;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreSegmentReport; page naming remains only for legacy compatibility"
)]
pub type PageStoreSegmentReport = BlockStoreSegmentReport;

#[deprecated(
    since = "0.1.0",
    note = "use BlockStoreRollReport; page naming remains only for legacy compatibility"
)]
pub type PageStoreRollReport = BlockStoreRollReport;

#[deprecated(
    since = "0.1.0",
    note = "use LocalBlockStore; page naming remains only for legacy compatibility"
)]
pub type LocalPageStore = LocalBlockStore;

fn load_extent_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreExtentDescriptor>, BlockStoreError> {
    let current_path = extent_manifest_path(root);
    let legacy_path = legacy_zone_manifest_path(root);
    let path = if current_path.exists() {
        current_path
    } else {
        legacy_path
    };
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let manifest: BlockStoreExtentManifest =
        serde_json::from_slice(&fs::read(path)?).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                page_segment_id: 0,
                offset: 0,
                reason: format!("corrupt extent manifest: {err}"),
            }
        })?;
    Ok(manifest
        .extents
        .into_iter()
        .map(|extent| (extent.page_segment_id, extent))
        .collect())
}

fn rebuild_extent_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreExtentDescriptor>, BlockStoreError> {
    let mut extents = BTreeMap::new();
    let latest = latest_segment_id_at(root)?;
    for page_segment_id in segment_ids_at(root)? {
        let path = segment_path(root, page_segment_id);
        let bytes = fs::read(&path)?;
        let summary = summarize_segment(&bytes, page_segment_id)?;
        extents.insert(
            page_segment_id,
            BlockStoreExtentDescriptor {
                extent_id: extent_id_for_segment(page_segment_id),
                page_segment_id,
                state: if page_segment_id == latest {
                    BlockStoreExtentState::Active
                } else {
                    BlockStoreExtentState::Sealed
                },
                physical_bytes: bytes.len() as u64,
                logical_bytes: summary.logical_bytes,
                created_unix_ms: file_created_unix_ms(&path)
                    .or_else(|| file_modified_unix_ms(&path)),
                updated_unix_ms: file_modified_unix_ms(&path)
                    .or_else(|| file_created_unix_ms(&path)),
                first_page_id: summary.first_page_id,
                last_page_id: summary.last_page_id,
            },
        );
    }
    for delayed in delayed_destroy_segment_reports_at(root)? {
        extents
            .entry(delayed.page_segment_id)
            .and_modify(|extent| {
                extent.state = BlockStoreExtentState::DelayedDestroy;
                extent.updated_unix_ms = delayed.modified_unix_ms;
                extent.physical_bytes = delayed.physical_bytes;
            })
            .or_insert(BlockStoreExtentDescriptor {
                extent_id: extent_id_for_segment(delayed.page_segment_id),
                page_segment_id: delayed.page_segment_id,
                state: BlockStoreExtentState::DelayedDestroy,
                physical_bytes: delayed.physical_bytes,
                logical_bytes: 0,
                created_unix_ms: delayed.modified_unix_ms,
                updated_unix_ms: delayed.modified_unix_ms,
                first_page_id: None,
                last_page_id: None,
            });
    }
    Ok(extents)
}

fn persist_extent_manifest(
    root: &Path,
    extents: &BTreeMap<u64, BlockStoreExtentDescriptor>,
) -> Result<(), BlockStoreError> {
    fs::create_dir_all(root)?;
    let path = extent_manifest_path(root);
    let temp_path = path.with_extension(format!(
        "json.tmp.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));
    let manifest = BlockStoreExtentManifest {
        version: 1,
        extents: extents.values().cloned().collect(),
    };
    {
        let mut temp = File::create(&temp_path)?;
        serde_json::to_writer_pretty(&mut temp, &manifest).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                page_segment_id: 0,
                offset: 0,
                reason: format!("serialize extent manifest: {err}"),
            }
        })?;
        temp.write_all(b"\n")?;
        temp.flush()?;
        temp.sync_all()?;
    }
    fs::rename(&temp_path, &path)?;
    sync_parent_dir(&path)?;
    Ok(())
}

fn summarize_extents(
    extents: &BTreeMap<u64, BlockStoreExtentDescriptor>,
) -> BlockStoreExtentSummary {
    let mut summary = BlockStoreExtentSummary::default();
    let now = now_unix_ms();
    for extent in extents.values() {
        update_oldest_extent_timestamp(&mut summary.oldest_known_extent_unix_ms, extent);
        summary.total_known_physical_bytes = summary
            .total_known_physical_bytes
            .saturating_add(extent.physical_bytes);
        match extent.state {
            BlockStoreExtentState::Active => {
                update_oldest_extent_timestamp(&mut summary.oldest_live_extent_unix_ms, extent);
                summary.active_extents = summary.active_extents.saturating_add(1);
                summary.active_physical_bytes = summary
                    .active_physical_bytes
                    .saturating_add(extent.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
            BlockStoreExtentState::Sealed => {
                update_oldest_extent_timestamp(&mut summary.oldest_live_extent_unix_ms, extent);
                summary.sealed_extents = summary.sealed_extents.saturating_add(1);
                summary.sealed_physical_bytes = summary
                    .sealed_physical_bytes
                    .saturating_add(extent.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
            BlockStoreExtentState::DelayedDestroy => {
                update_oldest_extent_timestamp(
                    &mut summary.oldest_reclaimable_extent_unix_ms,
                    extent,
                );
                summary.delayed_destroy_extents = summary.delayed_destroy_extents.saturating_add(1);
                summary.delayed_destroy_physical_bytes = summary
                    .delayed_destroy_physical_bytes
                    .saturating_add(extent.physical_bytes);
                summary.reclaimable_physical_bytes = summary
                    .reclaimable_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
            BlockStoreExtentState::Purged => {
                summary.purged_extents = summary.purged_extents.saturating_add(1);
                summary.purged_physical_bytes = summary
                    .purged_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
        }
    }
    summary.oldest_known_extent_age_ms = summary
        .oldest_known_extent_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_live_extent_age_ms = summary
        .oldest_live_extent_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_reclaimable_extent_age_ms = summary
        .oldest_reclaimable_extent_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary
}

fn update_oldest_extent_timestamp(target: &mut Option<u64>, extent: &BlockStoreExtentDescriptor) {
    let Some(timestamp) = extent.updated_unix_ms.or(extent.created_unix_ms) else {
        return;
    };
    if target.map(|current| timestamp < current).unwrap_or(true) {
        *target = Some(timestamp);
    }
}

fn ensure_extent_descriptor(
    extents: &mut BTreeMap<u64, BlockStoreExtentDescriptor>,
    root: &Path,
    page_segment_id: u64,
    state: BlockStoreExtentState,
) {
    extents.entry(page_segment_id).or_insert_with(|| {
        let physical_bytes = segment_path(root, page_segment_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        BlockStoreExtentDescriptor {
            extent_id: extent_id_for_segment(page_segment_id),
            page_segment_id,
            state,
            physical_bytes,
            logical_bytes: physical_bytes,
            created_unix_ms: file_created_unix_ms(&segment_path(root, page_segment_id))
                .or_else(|| file_modified_unix_ms(&segment_path(root, page_segment_id))),
            updated_unix_ms: file_modified_unix_ms(&segment_path(root, page_segment_id)),
            first_page_id: None,
            last_page_id: None,
        }
    });
    let transition_unix_ms = now_unix_ms();
    for extent in extents.values_mut() {
        if extent.page_segment_id == page_segment_id {
            extent.state = state;
            extent.updated_unix_ms = Some(transition_unix_ms);
        } else if extent.state == BlockStoreExtentState::Active {
            extent.state = BlockStoreExtentState::Sealed;
            extent.updated_unix_ms = Some(transition_unix_ms);
        }
    }
}

fn upsert_extent_after_append(
    extents: &mut BTreeMap<u64, BlockStoreExtentDescriptor>,
    page_segment_id: u64,
    physical_bytes: u64,
    logical_bytes_written: u64,
    page_id: u64,
) {
    let extent = extents
        .entry(page_segment_id)
        .or_insert(BlockStoreExtentDescriptor {
            extent_id: extent_id_for_segment(page_segment_id),
            page_segment_id,
            state: BlockStoreExtentState::Active,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_page_id: Some(page_id),
            last_page_id: Some(page_id),
        });
    let updated_unix_ms = now_unix_ms();
    extent.state = BlockStoreExtentState::Active;
    extent.physical_bytes = physical_bytes;
    extent.logical_bytes = extent.logical_bytes.saturating_add(logical_bytes_written);
    if extent.created_unix_ms.is_none() {
        extent.created_unix_ms = Some(updated_unix_ms);
    }
    extent.updated_unix_ms = Some(updated_unix_ms);
    extent.first_page_id = Some(
        extent
            .first_page_id
            .map_or(page_id, |first| first.min(page_id)),
    );
    extent.last_page_id = Some(
        extent
            .last_page_id
            .map_or(page_id, |last| last.max(page_id)),
    );
}

fn set_extent_state(
    extents: &mut BTreeMap<u64, BlockStoreExtentDescriptor>,
    page_segment_id: u64,
    state: BlockStoreExtentState,
) {
    extents
        .entry(page_segment_id)
        .and_modify(|extent| {
            extent.state = state;
            extent.updated_unix_ms = Some(now_unix_ms());
        })
        .or_insert(BlockStoreExtentDescriptor {
            extent_id: extent_id_for_segment(page_segment_id),
            page_segment_id,
            state,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_page_id: None,
            last_page_id: None,
        });
}

fn page_segment_utility_score(below_retention_floor: bool, is_current: bool, is_live: bool) -> u64 {
    if is_current || is_live {
        100
    } else if below_retention_floor {
        0
    } else {
        50
    }
}

fn move_segment_to_delayed_destroy(
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

fn delayed_destroy_segment_ids_at(root: &Path) -> Result<Vec<u64>, BlockStoreError> {
    Ok(delayed_destroy_segment_reports_at(root)?
        .into_iter()
        .map(|report| report.page_segment_id)
        .collect())
}

fn delayed_destroy_segment_reports_at(
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

fn delayed_destroy_segment_id_from_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let id = name
        .strip_prefix("page_segment_")?
        .strip_suffix(name.split_once(".seg.deleted.")?.1)?
        .strip_suffix(".seg.deleted.")?;
    id.parse::<u64>().ok()
}

fn extent_id_for_segment(page_segment_id: u64) -> u64 {
    page_segment_id
}

fn segment_ids_at(root: &Path) -> Result<Vec<u64>, BlockStoreError> {
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

fn latest_segment_id_at(root: &Path) -> Result<u64, BlockStoreError> {
    Ok(segment_ids_at(root)?.into_iter().max().unwrap_or_default())
}

fn next_page_id_at(root: &Path) -> Result<u64, BlockStoreError> {
    let mut max_page_id = None;
    for page_segment_id in segment_ids_at(root)? {
        let bytes = fs::read(segment_path(root, page_segment_id))?;
        if let Some(segment_max) = summarize_segment(&bytes, page_segment_id)?.last_page_id {
            max_page_id =
                Some(max_page_id.map_or(segment_max, |current: u64| current.max(segment_max)));
        }
    }
    Ok(max_page_id
        .map(|page_id| page_id.saturating_add(1))
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_segments_removes_old_non_current_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_segment(0, b"current").unwrap();
        store.install_segment(1, b"old").unwrap();
        store.install_segment(2, b"keep").unwrap();

        let report = store.gc_segments_before(2).unwrap();
        assert_eq!(report.removed_page_segment_ids, vec![0, 1]);
        assert_eq!(report.retained_page_segment_ids, vec![2]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"old".len()) as u64
        );
        assert_eq!(report.retained_physical_bytes, b"keep".len() as u64);
        assert!(report.retained_current_page_segment_ids.is_empty());
        assert!(report.retained_live_page_segment_ids.is_empty());
        assert_eq!(store.segment_ids().unwrap(), vec![2]);
    }

    #[test]
    fn roll_segment_moves_future_appends_to_fresh_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        assert_eq!(first.page_segment_id, 0);

        let roll = store.roll_segment().unwrap();
        assert_eq!(roll.previous_page_segment_id, 0);
        assert_eq!(roll.new_page_segment_id, 1);
        let second = store.append(b"second").unwrap();
        assert_eq!(second.page_segment_id, 1);
        assert_eq!(second.offset, 0);
        assert_eq!(store.read(&first).unwrap(), b"first");
        assert_eq!(store.read(&second).unwrap(), b"second");
    }

    #[test]
    fn reopened_store_appends_to_latest_existing_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        let roll = store.roll_segment().unwrap();
        let second = store.append(b"second").unwrap();
        assert_eq!(roll.new_page_segment_id, second.page_segment_id);

        let reopened = LocalBlockStore::new(dir.path());
        let third = reopened.append(b"third").unwrap();

        assert_eq!(third.page_segment_id, second.page_segment_id);
        assert!(third.offset > second.offset);
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&second).unwrap(), b"second");
        assert_eq!(reopened.read(&third).unwrap(), b"third");
    }

    #[test]
    fn installed_higher_segment_becomes_current_for_future_appends() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_segment(3, b"restored-segment").unwrap();

        let next = store.append(b"after-restore").unwrap();

        assert_eq!(next.page_segment_id, 3);
        assert_eq!(next.offset, b"restored-segment".len() as u64);
        assert_eq!(store.read(&next).unwrap(), b"after-restore");
    }

    #[test]
    fn page_address_checksum_rejects_corrupt_segment_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(b"verified-page").unwrap();
        assert_eq!(address.sha256, Some(sha256_hex(b"verified-page")));
        assert_eq!(store.read(&address).unwrap(), b"verified-page");

        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        fs::write(path, segment).unwrap();
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::ChecksumMismatch { .. }));
    }

    // shared-corpus: cpp_storage_object_page_slot_parity_surfaces;
    #[test]
    fn page_address_matches_cpp_metadata_contract_and_checksum_alias() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store
            .append_with_page_metadata(b"address-contract", Some(4242), Some(17))
            .unwrap();

        assert_eq!(address.page_segment_id, 0);
        assert_eq!(address.offset, 0);
        assert!(address.length > b"address-contract".len() as u64);
        assert_eq!(address.page_id, Some(0));
        assert_eq!(address.object_id, Some(4242));
        assert_eq!(address.routing_slot, Some(17));
        assert_eq!(address.extent_id, Some(0));
        assert_eq!(address.sha256, Some(sha256_hex(b"address-contract")));
        assert_eq!(store.read(&address).unwrap(), b"address-contract");

        let cpp_style_json = serde_json::json!({
            "page_segment_id": address.page_segment_id,
            "offset": address.offset,
            "length": address.length,
            "page_id": address.page_id,
            "object_id": address.object_id,
            "routing_slot": address.routing_slot,
            "extent_id": address.extent_id,
            "checksum": address.sha256,
        });
        let from_checksum_alias: BlockAddress = serde_json::from_value(cpp_style_json).unwrap();
        assert_eq!(from_checksum_alias, address);
        assert_eq!(
            serde_json::to_value(&address).unwrap()["sha256"],
            serde_json::json!(sha256_hex(b"address-contract"))
        );
    }

    #[test]
    fn page_segment_records_have_self_describing_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(b"enveloped-page").unwrap();
        let raw = store.read_segment(address.page_segment_id).unwrap();

        assert!(raw.starts_with(PAGE_RECORD_MAGIC));
        assert_eq!(raw[8], PAGE_RECORD_VERSION);
        assert_eq!(address.page_id, Some(0));
        assert_eq!(store.read(&address).unwrap(), b"enveloped-page");
    }

    #[test]
    fn page_ids_are_persisted_and_continue_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        let second = store.append(b"second").unwrap();
        assert_eq!(first.page_id, Some(0));
        assert_eq!(second.page_id, Some(1));

        let reopened = LocalBlockStore::new(dir.path());
        let third = reopened.append(b"third").unwrap();

        assert_eq!(third.page_id, Some(2));
        assert_eq!(reopened.read(&third).unwrap(), b"third");
    }

    #[test]
    fn installed_segment_page_ids_advance_future_allocations() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let source = LocalBlockStore::new(source_dir.path());
        let _ = source.append(b"first").unwrap();
        let restored = source.append(b"restored").unwrap();
        let restored_bytes = source.read_segment(restored.page_segment_id).unwrap();

        let store = LocalBlockStore::new(dir.path());
        store.install_segment(4, &restored_bytes).unwrap();
        let next = store.append(b"next").unwrap();

        assert_eq!(next.page_id, Some(2));
        assert_eq!(store.read(&next).unwrap(), b"next");
    }

    #[test]
    fn page_id_mismatch_rejects_corrupt_address_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let mut address = store.append(b"identity-checked-page").unwrap();
        address.page_id = Some(address.page_id.unwrap() + 1);

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn object_ids_are_persisted_and_checked_in_page_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let mut address = store
            .append_with_page_metadata(b"object-page", Some(42), Some(7))
            .unwrap();

        assert_eq!(address.object_id, Some(42));
        assert_eq!(address.routing_slot, Some(7));
        assert_eq!(address.extent_id, Some(0));
        assert_eq!(store.read(&address).unwrap(), b"object-page");

        address.object_id = Some(43);
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));

        address.object_id = Some(42);
        address.routing_slot = Some(8);
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));

        address.routing_slot = Some(7);
        address.extent_id = Some(1);
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn rolled_segments_stamp_new_extent_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first-extent").unwrap();
        let roll = store.roll_segment().unwrap();
        let second = store.append(b"second-extent").unwrap();

        assert_eq!(first.extent_id, Some(first.page_segment_id));
        assert_eq!(second.extent_id, Some(second.page_segment_id));
        assert_eq!(second.extent_id, Some(roll.new_page_segment_id));
        assert_ne!(first.extent_id, second.extent_id);
    }

    #[test]
    fn extent_manifest_tracks_roll_reopen_gc_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first-extent").unwrap();
        store.roll_segment().unwrap();
        let second = store.append(b"second-extent").unwrap();

        let extents = store.extent_descriptors();
        assert_eq!(extents.len(), 2);
        assert_eq!(extents[0].page_segment_id, first.page_segment_id);
        assert_eq!(extents[0].state, BlockStoreExtentState::Sealed);
        assert_eq!(extents[0].first_page_id, first.page_id);
        assert_eq!(extents[0].last_page_id, first.page_id);
        assert!(extents[0].created_unix_ms.is_some());
        assert!(extents[0].updated_unix_ms.is_some());
        assert_eq!(extents[1].page_segment_id, second.page_segment_id);
        assert_eq!(extents[1].state, BlockStoreExtentState::Active);
        assert_eq!(extents[1].first_page_id, second.page_id);
        assert_eq!(extents[1].last_page_id, second.page_id);
        assert!(extents[1].created_unix_ms.is_some());
        assert!(extents[1].updated_unix_ms.is_some());
        assert!(extent_manifest_path(dir.path()).exists());
        let initial_summary = store.extent_summary();
        assert_eq!(initial_summary.sealed_extents, 1);
        assert_eq!(initial_summary.active_extents, 1);
        assert_eq!(initial_summary.delayed_destroy_extents, 0);
        assert_eq!(initial_summary.purged_extents, 0);
        assert_eq!(
            initial_summary.sealed_physical_bytes,
            extents[0].physical_bytes
        );
        assert_eq!(
            initial_summary.active_physical_bytes,
            extents[1].physical_bytes
        );
        assert_eq!(
            initial_summary.live_physical_bytes,
            extents[0].physical_bytes + extents[1].physical_bytes
        );
        assert_eq!(initial_summary.reclaimable_physical_bytes, 0);
        assert!(initial_summary.oldest_known_extent_unix_ms.is_some());
        assert!(initial_summary.oldest_known_extent_age_ms.is_some());
        assert!(initial_summary.oldest_live_extent_unix_ms.is_some());
        assert!(initial_summary.oldest_live_extent_age_ms.is_some());
        assert!(initial_summary.oldest_reclaimable_extent_unix_ms.is_none());
        assert!(initial_summary.oldest_reclaimable_extent_age_ms.is_none());

        let reopened = LocalBlockStore::new(dir.path());
        let reopened_extents = reopened.extent_descriptors();
        assert_eq!(reopened_extents.len(), extents.len());
        assert_eq!(reopened_extents[0], extents[0]);
        assert_eq!(
            reopened_extents[1].page_segment_id,
            extents[1].page_segment_id
        );
        assert_eq!(reopened_extents[1].state, extents[1].state);
        assert_eq!(
            reopened_extents[1].physical_bytes,
            extents[1].physical_bytes
        );
        assert_eq!(reopened_extents[1].logical_bytes, extents[1].logical_bytes);
        assert_eq!(
            reopened_extents[1].created_unix_ms,
            extents[1].created_unix_ms
        );
        assert!(reopened_extents[1].updated_unix_ms >= extents[1].updated_unix_ms);

        let report = reopened
            .gc_segments_before_with_live_refs_delayed_destroy(1, std::iter::empty())
            .unwrap();
        assert_eq!(report.delayed_destroy_page_segment_ids, vec![0]);
        let delayed = reopened.extent_descriptors();
        assert_eq!(delayed[0].state, BlockStoreExtentState::DelayedDestroy);
        assert!(delayed[0].physical_bytes > 0);
        assert_eq!(delayed[0].created_unix_ms, extents[0].created_unix_ms);
        assert!(delayed[0].updated_unix_ms >= extents[0].updated_unix_ms);
        assert_eq!(delayed[1].state, BlockStoreExtentState::Active);
        let delayed_summary = reopened.extent_summary();
        assert_eq!(delayed_summary.delayed_destroy_extents, 1);
        assert_eq!(delayed_summary.active_extents, 1);
        assert_eq!(
            delayed_summary.delayed_destroy_physical_bytes,
            delayed[0].physical_bytes
        );
        assert_eq!(
            delayed_summary.reclaimable_physical_bytes,
            delayed[0].physical_bytes
        );
        assert_eq!(
            delayed_summary.live_physical_bytes,
            delayed[1].physical_bytes
        );
        assert!(delayed_summary.oldest_known_extent_unix_ms.is_some());
        assert!(delayed_summary.oldest_live_extent_unix_ms.is_some());
        assert_eq!(
            delayed_summary.oldest_reclaimable_extent_unix_ms,
            delayed[0].updated_unix_ms
        );
        assert!(delayed_summary.oldest_reclaimable_extent_age_ms.is_some());

        let purge = reopened
            .purge_delayed_destroy_segments_with_report()
            .unwrap();
        assert_eq!(purge.purged_page_segment_ids, vec![0]);
        assert!(purge.purged_physical_bytes > 0);
        let purged = LocalBlockStore::new(dir.path()).extent_descriptors();
        assert_eq!(purged[0].state, BlockStoreExtentState::Purged);
        assert_eq!(purged[0].created_unix_ms, extents[0].created_unix_ms);
        assert!(purged[0].updated_unix_ms >= delayed[0].updated_unix_ms);
        assert_eq!(purged[1].state, BlockStoreExtentState::Active);
        let purged_summary = LocalBlockStore::new(dir.path()).extent_summary();
        assert_eq!(purged_summary.purged_extents, 1);
        assert_eq!(purged_summary.active_extents, 1);
        assert_eq!(
            purged_summary.purged_physical_bytes,
            purged[0].physical_bytes
        );
        assert_eq!(purged_summary.live_physical_bytes, purged[1].physical_bytes);
        assert_eq!(purged_summary.reclaimable_physical_bytes, 0);
        assert!(purged_summary.oldest_known_extent_unix_ms.is_some());
        assert!(purged_summary.oldest_live_extent_unix_ms.is_some());
        assert!(purged_summary.oldest_reclaimable_extent_unix_ms.is_none());
        assert!(purged_summary.oldest_reclaimable_extent_age_ms.is_none());
    }

    #[test]
    fn missing_extent_manifest_rebuilds_from_existing_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first-extent").unwrap();
        store.roll_segment().unwrap();
        let second = store.append(b"second-extent").unwrap();
        fs::remove_file(extent_manifest_path(dir.path())).unwrap();

        let rebuilt = LocalBlockStore::new(dir.path());
        let extents = rebuilt.extent_descriptors();

        assert_eq!(extents.len(), 2);
        assert_eq!(extents[0].page_segment_id, first.page_segment_id);
        assert_eq!(extents[0].state, BlockStoreExtentState::Sealed);
        assert_eq!(extents[0].first_page_id, first.page_id);
        assert_eq!(extents[0].last_page_id, first.page_id);
        assert!(extents[0].created_unix_ms.is_some());
        assert!(extents[0].updated_unix_ms.is_some());
        assert_eq!(extents[1].page_segment_id, second.page_segment_id);
        assert_eq!(extents[1].state, BlockStoreExtentState::Active);
        assert_eq!(extents[1].first_page_id, second.page_id);
        assert_eq!(extents[1].last_page_id, second.page_id);
        assert!(extents[1].created_unix_ms.is_some());
        assert!(extents[1].updated_unix_ms.is_some());
    }

    #[test]
    fn logical_page_range_skips_record_envelopes_across_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.append(b"abc").unwrap();
        store.append(b"def").unwrap();

        assert_eq!(store.read_logical_range(0, 1, 4).unwrap(), b"bcde");
    }

    #[test]
    fn compressed_page_records_round_trip_and_remain_logical() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first_payload = b"prefix-".repeat(80);
        let second_payload = b"suffix-".repeat(80);
        let first = store.append(&first_payload).unwrap();
        let second = store.append(&second_payload).unwrap();
        let raw = store.read_segment(first.page_segment_id).unwrap();

        assert!(first.length < (PAGE_RECORD_HEADER_LEN + first_payload.len()) as u64);
        assert!(second.length < (PAGE_RECORD_HEADER_LEN + second_payload.len()) as u64);
        assert_eq!(store.read(&first).unwrap(), first_payload);
        assert_eq!(store.read(&second).unwrap(), second_payload);

        let logical_offset = first_payload.len() as u64 - 3;
        let logical = store
            .read_logical_range(first.page_segment_id, logical_offset, 12)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first_payload[first_payload.len() - 3..]);
        expected.extend_from_slice(&second_payload[..9]);
        assert_eq!(logical, expected);
        assert_eq!(raw[8], PAGE_RECORD_VERSION);
        assert_eq!(raw[92], PAGE_RECORD_COMPRESSION_ZSTD);

        let stats = store.stats();
        assert_eq!(stats.writes, 2);
        assert_eq!(
            stats.logical_bytes_written,
            (first_payload.len() + second_payload.len()) as u64
        );
        assert_eq!(stats.compressed_records_written, 2);
        assert_eq!(stats.compressed_records_read, 4);
        assert!(stats.compression_bytes_saved > 0);
        assert!(stats.bytes_written < stats.logical_bytes_written);
        assert!(stats.logical_bytes_read >= stats.bytes_read);
    }

    // shared-corpus: storage_stream_backed_extent_runtime;
    #[test]
    fn stream_backed_extent_runtime_report_covers_roll_read_manifest_and_delayed_destroy() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first_payload = b"extent-stream-first-".repeat(96);
        let second_payload = b"extent-stream-second-".repeat(96);
        let first = store
            .append_with_page_metadata(&first_payload, Some(11), Some(7))
            .unwrap();
        let second = store
            .append_with_page_metadata(&second_payload, Some(12), Some(7))
            .unwrap();
        assert_eq!(first.page_segment_id, second.page_segment_id);

        let logical_offset = first_payload.len() as u64 - 8;
        let logical = store
            .read_logical_range(first.page_segment_id, logical_offset, 16)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first_payload[first_payload.len() - 8..]);
        expected.extend_from_slice(&second_payload[..8]);
        assert_eq!(logical, expected);

        let roll = store.roll_segment().unwrap();
        let third_payload = b"extent-stream-third-".repeat(96);
        let third = store
            .append_with_page_metadata(&third_payload, Some(13), Some(8))
            .unwrap();
        assert_eq!(third.page_segment_id, roll.new_page_segment_id);

        let delayed = store
            .gc_segments_before_with_live_refs_delayed_destroy(
                roll.new_page_segment_id,
                [roll.new_page_segment_id],
            )
            .unwrap();
        assert_eq!(
            delayed.delayed_destroy_page_segment_ids,
            vec![roll.previous_page_segment_id]
        );

        let reopened = LocalBlockStore::new(dir.path());
        assert_eq!(reopened.read(&third).unwrap(), third_payload);
        let report = reopened.stream_backed_extent_runtime_report().unwrap();
        assert!(report.runtime_ready, "{report:?}");
        assert_eq!(report.active_extents, 1);
        assert_eq!(report.delayed_destroy_extents, 1);
        assert!(report.extent_count >= 2);
        assert!(report.stream_segment_count >= 1);
        assert!(report.logical_stream_read_ready);
        assert!(report.append_roll_ready);
        assert!(report.extent_manifest_ready);
        assert!(report.envelope_checksum_ready);
        assert!(report.compression_stream_ready);
        assert!(report.delayed_destroy_ready);
        assert!(report.logical_bytes >= third_payload.len() as u64);
        assert!(report.blockers.is_empty());
        assert!(report
            .evidence
            .iter()
            .any(|item| item.contains("logical stream reads span records")));
    }

    #[test]
    fn segment_reports_describe_page_counts_bytes_and_compression() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first_payload = b"prefix-".repeat(80);
        let second_payload = b"suffix-".repeat(80);
        let first = store.append(&first_payload).unwrap();
        let second = store.append(&second_payload).unwrap();

        let reports = store.segment_reports().unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].page_segment_id, first.page_segment_id);
        assert_eq!(reports[0].physical_bytes, first.length + second.length);
        assert_eq!(
            reports[0].logical_bytes,
            (first_payload.len() + second_payload.len()) as u64
        );
        assert_eq!(reports[0].page_count, 2);
        assert_eq!(reports[0].compressed_records, 2);
        assert_eq!(
            reports[0].readable_prefix_physical_bytes,
            reports[0].physical_bytes
        );
        assert!(!reports[0].has_corruption);
        assert_eq!(reports[0].first_error_offset, None);
        assert_eq!(reports[0].first_page_id, first.page_id);
        assert_eq!(reports[0].last_page_id, second.page_id);
        assert_eq!(reports[0].first_error, None);
    }

    #[test]
    fn segment_reports_describe_object_and_routing_slot_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store
            .append_with_page_metadata(b"slot-7-object-100", Some(100), Some(7))
            .unwrap();
        store
            .append_with_page_metadata(b"slot-11-object-101", Some(101), Some(11))
            .unwrap();
        store
            .append_with_page_metadata(b"slot-7-object-100-again", Some(100), Some(7))
            .unwrap();

        let reports = store.segment_reports().unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].page_count, 3);
        assert_eq!(reports[0].object_count, 2);
        assert_eq!(reports[0].routing_slot_count, 2);
        assert_eq!(reports[0].first_routing_slot, Some(7));
        assert_eq!(reports[0].last_routing_slot, Some(11));
        assert_eq!(
            reports[0].readable_prefix_physical_bytes,
            reports[0].physical_bytes
        );
        assert!(!reports[0].has_corruption);
        assert_eq!(reports[0].first_error, None);
    }

    #[test]
    fn segment_reports_capture_first_corrupt_record_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"healthy").unwrap();
        let second = store.append(b"damaged").unwrap();
        let path = segment_path(dir.path(), second.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        fs::write(path, segment).unwrap();

        let reports = store.segment_reports().unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].page_count, 1);
        assert_eq!(reports[0].logical_bytes, b"healthy".len() as u64);
        assert_eq!(reports[0].readable_prefix_physical_bytes, first.length);
        assert!(reports[0].has_corruption);
        assert_eq!(reports[0].first_error_offset, Some(first.length));
        assert_eq!(reports[0].first_page_id, first.page_id);
        assert_eq!(reports[0].last_page_id, first.page_id);
        let error = reports[0]
            .first_error
            .as_ref()
            .expect("corrupt second record should be reported");
        assert!(error.contains("checksum") || error.contains("corrupt page envelope"));
    }

    #[test]
    fn page_record_compression_policy_can_disable_or_raise_threshold() {
        let payload = b"policy-controlled-".repeat(80);

        let disabled_dir = tempfile::tempdir().unwrap();
        let disabled_store = LocalBlockStore::with_options(
            disabled_dir.path(),
            BlockStoreOptions {
                compression_enabled: false,
                ..BlockStoreOptions::default()
            },
        );
        let disabled_address = disabled_store.append(&payload).unwrap();
        let disabled_raw = disabled_store
            .read_segment(disabled_address.page_segment_id)
            .unwrap();

        assert_eq!(
            disabled_address.length,
            (PAGE_RECORD_HEADER_LEN + payload.len()) as u64
        );
        assert_eq!(disabled_raw[92], PAGE_RECORD_COMPRESSION_NONE);
        assert_eq!(disabled_store.read(&disabled_address).unwrap(), payload);
        assert_eq!(disabled_store.stats().compressed_records_written, 0);
        assert_eq!(disabled_store.stats().compression_bytes_saved, 0);

        let threshold_dir = tempfile::tempdir().unwrap();
        let threshold_store = LocalBlockStore::with_options(
            threshold_dir.path(),
            BlockStoreOptions {
                compression_min_bytes: payload.len() + 1,
                ..BlockStoreOptions::default()
            },
        );
        let threshold_address = threshold_store.append(&payload).unwrap();
        let threshold_raw = threshold_store
            .read_segment(threshold_address.page_segment_id)
            .unwrap();

        assert_eq!(
            threshold_address.length,
            (PAGE_RECORD_HEADER_LEN + payload.len()) as u64
        );
        assert_eq!(threshold_raw[92], PAGE_RECORD_COMPRESSION_NONE);
        assert_eq!(threshold_store.read(&threshold_address).unwrap(), payload);
        assert_eq!(threshold_store.stats().compressed_records_written, 0);
        assert_eq!(threshold_store.stats().compression_bytes_saved, 0);
    }

    #[test]
    fn page_envelope_rejects_corrupt_compressed_payload() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(&b"compress-me-".repeat(80)).unwrap();
        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        fs::write(path, segment).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(
            err,
            BlockStoreError::ChecksumMismatch { .. } | BlockStoreError::CorruptPageEnvelope { .. }
        ));
    }

    #[test]
    fn page_envelope_rejects_corrupt_header_lengths() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(b"header-checked-page").unwrap();
        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        segment[10] = 1;
        segment[11] = 0;
        fs::write(path, segment).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn page_address_without_checksum_keeps_legacy_read_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let legacy_address = BlockAddress {
            page_segment_id: 0,
            offset: 0,
            length: b"alteredpage".len() as u64,
            page_id: None,
            object_id: None,
            routing_slot: None,
            extent_id: None,
            sha256: None,
        };
        fs::write(
            segment_path(dir.path(), legacy_address.page_segment_id),
            b"alteredpage",
        )
        .unwrap();

        assert_eq!(store.read(&legacy_address).unwrap(), b"alteredpage");
    }

    #[test]
    fn gc_segments_retains_live_index_references_below_floor() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_segment(0, b"current").unwrap();
        store.install_segment(1, b"live").unwrap();
        store.install_segment(2, b"stale").unwrap();
        store.install_segment(3, b"keep").unwrap();

        let report = store.gc_segments_before_with_live_refs(3, [1_u64]).unwrap();
        assert_eq!(report.removed_page_segment_ids, vec![0, 2]);
        assert_eq!(report.retained_page_segment_ids, vec![1, 3]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert_eq!(
            report.retained_physical_bytes,
            (b"live".len() + b"keep".len()) as u64
        );
        assert!(report.retained_current_page_segment_ids.is_empty());
        assert_eq!(report.retained_live_page_segment_ids, vec![1]);
        assert_eq!(report.retained_live_physical_bytes, b"live".len() as u64);
        assert_eq!(store.segment_ids().unwrap(), vec![1, 3]);
    }

    #[test]
    fn delayed_destroy_gc_quarantines_stale_segments_before_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_segment(0, b"current").unwrap();
        store.install_segment(1, b"stale").unwrap();
        store.install_segment(2, b"live").unwrap();
        store.install_segment(3, b"keep").unwrap();

        let report = store
            .gc_segments_before_with_live_refs_delayed_destroy(3, [2_u64])
            .unwrap();

        assert_eq!(report.removed_page_segment_ids, vec![0, 1]);
        assert_eq!(report.delayed_destroy_page_segment_ids, vec![0, 1]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert_eq!(
            report.delayed_destroy_physical_bytes,
            report.removed_physical_bytes
        );
        assert_eq!(report.retained_page_segment_ids, vec![2, 3]);
        assert_eq!(report.retained_live_page_segment_ids, vec![2]);
        assert_eq!(report.retained_live_physical_bytes, b"live".len() as u64);
        assert_eq!(store.segment_ids().unwrap(), vec![2, 3]);
        assert_eq!(store.delayed_destroy_segment_ids().unwrap(), vec![0, 1]);
        let delayed_reports = store.delayed_destroy_segment_reports().unwrap();
        assert_eq!(delayed_reports.len(), 2);
        assert_eq!(delayed_reports[0].page_segment_id, 0);
        assert_eq!(delayed_reports[0].physical_bytes, b"current".len() as u64);
        assert!(delayed_reports[0].modified_unix_ms.is_some());
        assert_eq!(delayed_reports[1].page_segment_id, 1);
        assert_eq!(delayed_reports[1].physical_bytes, b"stale".len() as u64);
        assert!(delayed_reports[1].modified_unix_ms.is_some());

        let purge = store.purge_delayed_destroy_segments_with_report().unwrap();
        assert_eq!(purge.purged_page_segment_ids, vec![0, 1]);
        assert_eq!(
            purge.purged_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert!(store.delayed_destroy_segment_ids().unwrap().is_empty());
        assert!(store.delayed_destroy_segment_reports().unwrap().is_empty());
        assert_eq!(store.segment_ids().unwrap(), vec![2, 3]);
    }

    #[test]
    fn utility_gc_selects_low_utility_stale_segments_with_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_segment(0, b"small").unwrap();
        store.install_segment(1, b"largest-stale-segment").unwrap();
        store.install_segment(2, b"live-segment").unwrap();
        store.install_segment(3, b"current-segment").unwrap();

        let candidates = store.gc_utility_candidates(3, [2_u64]).unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.page_segment_id)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert!(candidates
            .iter()
            .all(|candidate| candidate.utility_score == 0));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.created_unix_ms.is_some()));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.updated_unix_ms.is_some()));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.age_ms.is_some()));

        let no_op = store
            .gc_segments_before_with_live_refs_utility(3, [2_u64], 0, true)
            .unwrap();
        assert!(no_op.removed_page_segment_ids.is_empty());
        assert_eq!(no_op.removed_physical_bytes, 0);
        assert_eq!(store.segment_ids().unwrap(), vec![0, 1, 2, 3]);

        let report = store
            .gc_segments_before_with_live_refs_utility(3, [2_u64], 1, true)
            .unwrap();
        assert_eq!(report.removed_page_segment_ids, vec![1]);
        assert_eq!(report.delayed_destroy_page_segment_ids, vec![1]);
        assert_eq!(
            report.removed_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(
            report.delayed_destroy_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(report.retained_page_segment_ids, vec![0, 2, 3]);
        assert_eq!(report.retained_live_page_segment_ids, vec![2]);
        assert_eq!(
            report.retained_live_physical_bytes,
            b"live-segment".len() as u64
        );
        assert_eq!(store.segment_ids().unwrap(), vec![0, 2, 3]);
        assert_eq!(store.delayed_destroy_segment_ids().unwrap(), vec![1]);
        let delayed_reports = store.delayed_destroy_segment_reports().unwrap();
        assert_eq!(delayed_reports.len(), 1);
        assert_eq!(delayed_reports[0].page_segment_id, 1);
        assert_eq!(
            delayed_reports[0].physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert!(delayed_reports[0].modified_unix_ms.is_some());
    }

    #[test]
    fn policy_gc_plans_and_applies_byte_bounded_destroy() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_segment(0, b"small").unwrap();
        store.install_segment(1, b"largest-stale-segment").unwrap();
        store.install_segment(2, b"live-segment").unwrap();
        store.install_segment(3, b"current-segment").unwrap();

        let policy = BlockStoreGcPolicy {
            max_destroy_segments: 2,
            max_destroy_physical_bytes: b"small".len() as u64,
            max_utility_score: Some(0),
            min_age_ms: Some(0),
        };
        let plan = store.gc_policy_plan(3, [2_u64], &policy).unwrap();
        assert_eq!(plan.retain_from_page_segment_id, 3);
        assert_eq!(plan.candidate_count, 2);
        assert_eq!(
            plan.candidate_physical_bytes,
            (b"small".len() + b"largest-stale-segment".len()) as u64
        );
        assert_eq!(plan.selected_page_segment_ids, vec![0]);
        assert_eq!(plan.selected_physical_bytes, b"small".len() as u64);
        assert_eq!(plan.skipped_by_policy_count, 0);
        assert_eq!(plan.skipped_by_policy_physical_bytes, 0);
        assert_eq!(plan.skipped_by_budget_count, 1);
        assert_eq!(
            plan.skipped_by_budget_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(
            plan.candidates
                .iter()
                .map(|candidate| candidate.page_segment_id)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );

        let report = store
            .gc_segments_before_with_live_refs_policy(3, [2_u64], policy, true)
            .unwrap();
        assert_eq!(report.removed_page_segment_ids, vec![0]);
        assert_eq!(report.delayed_destroy_page_segment_ids, vec![0]);
        assert_eq!(report.retained_page_segment_ids, vec![1, 2, 3]);
        assert_eq!(store.segment_ids().unwrap(), vec![1, 2, 3]);
        assert_eq!(store.delayed_destroy_segment_ids().unwrap(), vec![0]);
    }
}
