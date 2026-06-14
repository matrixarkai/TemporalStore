use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageStoreError {
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
pub struct PageAddress {
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_slot: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreStats {
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
pub struct PageStoreOptions {
    #[serde(default = "default_page_record_compression_enabled")]
    pub compression_enabled: bool,
    #[serde(default = "default_page_record_compression_min_bytes")]
    pub compression_min_bytes: usize,
    #[serde(default = "default_page_record_compression_level")]
    pub compression_level: i32,
}

impl Default for PageStoreOptions {
    fn default() -> Self {
        Self {
            compression_enabled: default_page_record_compression_enabled(),
            compression_min_bytes: default_page_record_compression_min_bytes(),
            compression_level: default_page_record_compression_level(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreGcReport {
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
pub struct PageStoreGcUtilityCandidate {
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
pub struct PageStoreGcPolicy {
    #[serde(default)]
    pub max_destroy_segments: usize,
    #[serde(default)]
    pub max_destroy_physical_bytes: u64,
    #[serde(default)]
    pub max_utility_score: Option<u64>,
    #[serde(default)]
    pub min_age_ms: Option<u64>,
}

impl PageStoreGcPolicy {
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
pub struct PageStoreGcPolicyPlan {
    pub retain_from_page_segment_id: u64,
    pub selected_page_segment_ids: Vec<u64>,
    pub selected_physical_bytes: u64,
    pub candidate_count: usize,
    pub skipped_by_policy_count: usize,
    pub skipped_by_budget_count: usize,
    pub candidates: Vec<PageStoreGcUtilityCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreDelayedDestroySegmentReport {
    pub page_segment_id: u64,
    pub physical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStorePurgeDelayedDestroyReport {
    pub purged_page_segment_ids: Vec<u64>,
    pub purged_physical_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageStoreZoneState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreZoneDescriptor {
    pub zone_id: u64,
    pub page_segment_id: u64,
    pub state: PageStoreZoneState,
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
pub struct PageStoreZoneSummary {
    pub active_zones: u64,
    pub sealed_zones: u64,
    pub delayed_destroy_zones: u64,
    pub purged_zones: u64,
    pub active_physical_bytes: u64,
    pub sealed_physical_bytes: u64,
    pub delayed_destroy_physical_bytes: u64,
    pub purged_physical_bytes: u64,
    pub live_physical_bytes: u64,
    pub reclaimable_physical_bytes: u64,
    pub total_known_physical_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreSegmentReport {
    pub page_segment_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub page_count: u64,
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
struct PageStoreZoneManifest {
    version: u32,
    zones: Vec<PageStoreZoneDescriptor>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreRollReport {
    pub previous_page_segment_id: u64,
    pub new_page_segment_id: u64,
}

#[derive(Debug, Clone)]
pub struct LocalPageStore {
    inner: Arc<Mutex<PageStoreInner>>,
}

#[derive(Debug)]
struct PageStoreInner {
    root: PathBuf,
    page_segment_id: u64,
    write_offset: u64,
    next_page_id: u64,
    options: PageStoreOptions,
    zones: BTreeMap<u64, PageStoreZoneDescriptor>,
    stats: PageStoreStats,
}

impl LocalPageStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_options(root, PageStoreOptions::default())
    }

    pub fn with_options(root: impl Into<PathBuf>, options: PageStoreOptions) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        let page_segment_id = latest_segment_id_at(&root).unwrap_or_default();
        let write_offset = segment_path(&root, page_segment_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let next_page_id = next_page_id_at(&root).unwrap_or_default();
        let manifest_exists = zone_manifest_path(&root).exists();
        let mut zones = if manifest_exists {
            load_zone_manifest_at(&root)
                .or_else(|_| rebuild_zone_manifest_at(&root))
                .unwrap_or_default()
        } else {
            rebuild_zone_manifest_at(&root).unwrap_or_default()
        };
        ensure_zone_descriptor(
            &mut zones,
            &root,
            page_segment_id,
            PageStoreZoneState::Active,
        );
        if !manifest_exists {
            let _ = persist_zone_manifest(&root, &zones);
        }
        Self {
            inner: Arc::new(Mutex::new(PageStoreInner {
                root,
                page_segment_id,
                write_offset,
                next_page_id,
                options,
                zones,
                stats: PageStoreStats::default(),
            })),
        }
    }

    pub fn append(&self, bytes: &[u8]) -> Result<PageAddress, PageStoreError> {
        self.append_with_object_id(bytes, None)
    }

    pub fn append_with_object_id(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
    ) -> Result<PageAddress, PageStoreError> {
        self.append_with_page_metadata(bytes, object_id, None)
    }

    pub fn append_with_page_metadata(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
        routing_slot: Option<u32>,
    ) -> Result<PageAddress, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = segment_path(&inner.root, inner.page_segment_id);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let page_id = inner.next_page_id;
        let zone_id = zone_id_for_segment(inner.page_segment_id);
        let record = encode_page_record(
            bytes,
            page_id,
            object_id,
            routing_slot,
            zone_id,
            inner.options,
        )?;
        let address = PageAddress {
            page_segment_id: inner.page_segment_id,
            offset: inner.write_offset,
            length: record.bytes.len() as u64,
            page_id: Some(page_id),
            object_id,
            routing_slot,
            zone_id: Some(zone_id),
            sha256: Some(sha256_hex(bytes)),
        };
        file.write_all(&record.bytes)?;
        file.flush()?;
        file.sync_data()?;
        inner.next_page_id = inner.next_page_id.saturating_add(1);
        inner.write_offset += address.length;
        let page_segment_id = inner.page_segment_id;
        let write_offset = inner.write_offset;
        upsert_zone_after_append(
            &mut inner.zones,
            page_segment_id,
            write_offset,
            record.logical_len as u64,
            page_id,
        );
        persist_zone_manifest(&inner.root, &inner.zones)?;
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

    pub fn roll_segment(&self) -> Result<PageStoreRollReport, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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
        if let Some(previous) = inner.zones.get_mut(&previous_page_segment_id) {
            previous.state = PageStoreZoneState::Sealed;
            previous.updated_unix_ms = Some(transition_unix_ms);
        }
        let new_zone = PageStoreZoneDescriptor {
            zone_id: zone_id_for_segment(inner.page_segment_id),
            page_segment_id: inner.page_segment_id,
            state: PageStoreZoneState::Active,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(transition_unix_ms),
            updated_unix_ms: Some(transition_unix_ms),
            first_page_id: None,
            last_page_id: None,
        };
        let page_segment_id = inner.page_segment_id;
        inner.zones.insert(page_segment_id, new_zone);
        persist_zone_manifest(&inner.root, &inner.zones)?;
        Ok(PageStoreRollReport {
            previous_page_segment_id,
            new_page_segment_id: inner.page_segment_id,
        })
    }

    pub fn read(&self, address: &PageAddress) -> Result<Vec<u8>, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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
                return Err(PageStoreError::ChecksumMismatch {
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
    ) -> Result<Vec<u8>, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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
    ) -> Result<Vec<u8>, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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

    pub fn read_segment(&self, page_segment_id: u64) -> Result<Vec<u8>, PageStoreError> {
        let root = self
            .inner
            .lock()
            .expect("page store lock poisoned")
            .root
            .clone();
        Ok(fs::read(segment_path(&root, page_segment_id))?)
    }

    pub fn install_segment(
        &self,
        page_segment_id: u64,
        bytes: &[u8],
    ) -> Result<(), PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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
        let zone_summary = summarize_segment(bytes, page_segment_id)?;
        if let Some(max_page_id) = zone_summary.last_page_id {
            inner.next_page_id = inner.next_page_id.max(max_page_id.saturating_add(1));
        }
        let is_current_segment = page_segment_id == inner.page_segment_id;
        let now = now_unix_ms();
        inner.zones.insert(
            page_segment_id,
            PageStoreZoneDescriptor {
                zone_id: zone_id_for_segment(page_segment_id),
                page_segment_id,
                state: if is_current_segment {
                    PageStoreZoneState::Active
                } else {
                    PageStoreZoneState::Sealed
                },
                physical_bytes: bytes.len() as u64,
                logical_bytes: zone_summary.logical_bytes,
                created_unix_ms: Some(
                    file_modified_unix_ms(&path)
                        .or_else(|| file_created_unix_ms(&path))
                        .unwrap_or(now),
                ),
                updated_unix_ms: Some(now),
                first_page_id: zone_summary.first_page_id,
                last_page_id: zone_summary.last_page_id,
            },
        );
        if is_current_segment {
            for zone in inner.zones.values_mut() {
                if zone.page_segment_id != page_segment_id
                    && zone.state == PageStoreZoneState::Active
                {
                    zone.state = PageStoreZoneState::Sealed;
                }
            }
        }
        persist_zone_manifest(&inner.root, &inner.zones)?;
        Ok(())
    }

    pub fn segment_ids(&self) -> Result<Vec<u64>, PageStoreError> {
        let root = self
            .inner
            .lock()
            .expect("page store lock poisoned")
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
    ) -> Result<PageStoreGcReport, PageStoreError> {
        self.gc_segments_before_with_live_refs(retain_from_page_segment_id, std::iter::empty())
    }

    pub fn gc_segments_before_with_live_refs(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<PageStoreGcReport, PageStoreError> {
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
    ) -> Result<PageStoreGcReport, PageStoreError> {
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
    ) -> Result<PageStoreGcReport, PageStoreError> {
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
            PageStoreGcPolicy::max_segments(max_destroy_segments),
            delayed_destroy,
        )
    }

    pub fn gc_segments_before_with_live_refs_policy(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
        policy: PageStoreGcPolicy,
        delayed_destroy: bool,
    ) -> Result<PageStoreGcReport, PageStoreError> {
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
        policy: &PageStoreGcPolicy,
    ) -> Result<PageStoreGcPolicyPlan, PageStoreError> {
        let candidates =
            self.gc_utility_candidates(retain_from_page_segment_id, live_page_segment_ids)?;
        let mut selected_page_segment_ids = Vec::new();
        let mut selected_physical_bytes = 0_u64;
        let mut skipped_by_policy_count = 0_usize;
        let mut skipped_by_budget_count = 0_usize;

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
                continue;
            }

            if policy.max_destroy_segments > 0
                && selected_page_segment_ids.len() >= policy.max_destroy_segments
            {
                skipped_by_budget_count += 1;
                continue;
            }
            if policy.max_destroy_physical_bytes > 0
                && selected_physical_bytes.saturating_add(candidate.bytes)
                    > policy.max_destroy_physical_bytes
            {
                skipped_by_budget_count += 1;
                continue;
            }

            selected_page_segment_ids.push(candidate.page_segment_id);
            selected_physical_bytes = selected_physical_bytes.saturating_add(candidate.bytes);
        }

        Ok(PageStoreGcPolicyPlan {
            retain_from_page_segment_id,
            selected_page_segment_ids,
            selected_physical_bytes,
            candidate_count: candidates.len(),
            skipped_by_policy_count,
            skipped_by_budget_count,
            candidates,
        })
    }

    pub fn gc_utility_candidates(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<PageStoreGcUtilityCandidate>, PageStoreError> {
        let inner = self.inner.lock().expect("page store lock poisoned");
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
                let zone = inner.zones.get(&page_segment_id);
                let created_unix_ms = zone.and_then(|zone| zone.created_unix_ms);
                let updated_unix_ms = zone.and_then(|zone| zone.updated_unix_ms);
                let age_ms = updated_unix_ms
                    .or(created_unix_ms)
                    .map(|timestamp| now.saturating_sub(timestamp));
                candidates.push(PageStoreGcUtilityCandidate {
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
    ) -> Result<PageStoreGcReport, PageStoreError> {
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
    ) -> Result<PageStoreGcReport, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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
                    set_zone_state(
                        &mut inner.zones,
                        page_segment_id,
                        PageStoreZoneState::DelayedDestroy,
                    );
                    delayed_destroy_ids.push(page_segment_id);
                    delayed_destroy_physical_bytes += segment_physical_bytes;
                } else {
                    fs::remove_file(segment_path(&inner.root, page_segment_id))?;
                    set_zone_state(
                        &mut inner.zones,
                        page_segment_id,
                        PageStoreZoneState::Purged,
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
        persist_zone_manifest(&inner.root, &inner.zones)?;
        Ok(PageStoreGcReport {
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

    pub fn delayed_destroy_segment_ids(&self) -> Result<Vec<u64>, PageStoreError> {
        let root = self
            .inner
            .lock()
            .expect("page store lock poisoned")
            .root
            .clone();
        delayed_destroy_segment_ids_at(&root)
    }

    pub fn delayed_destroy_segment_reports(
        &self,
    ) -> Result<Vec<PageStoreDelayedDestroySegmentReport>, PageStoreError> {
        let root = self
            .inner
            .lock()
            .expect("page store lock poisoned")
            .root
            .clone();
        delayed_destroy_segment_reports_at(&root)
    }

    pub fn purge_delayed_destroy_segments(&self) -> Result<Vec<u64>, PageStoreError> {
        Ok(self
            .purge_delayed_destroy_segments_with_report()?
            .purged_page_segment_ids)
    }

    pub fn purge_delayed_destroy_segments_with_report(
        &self,
    ) -> Result<PageStorePurgeDelayedDestroyReport, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
        let trash_dir = delayed_destroy_dir(&inner.root);
        let mut purged = Vec::new();
        let mut purged_physical_bytes = 0;
        if !trash_dir.exists() {
            return Ok(PageStorePurgeDelayedDestroyReport::default());
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
            set_zone_state(&mut inner.zones, id, PageStoreZoneState::Purged);
            purged.push(id);
        }
        purged.sort_unstable();
        sync_dir(&trash_dir)?;
        persist_zone_manifest(&inner.root, &inner.zones)?;
        Ok(PageStorePurgeDelayedDestroyReport {
            purged_page_segment_ids: purged,
            purged_physical_bytes,
        })
    }

    pub fn zone_descriptors(&self) -> Vec<PageStoreZoneDescriptor> {
        self.inner
            .lock()
            .expect("page store lock poisoned")
            .zones
            .values()
            .cloned()
            .collect()
    }

    pub fn zone_summary(&self) -> PageStoreZoneSummary {
        summarize_zones(&self.inner.lock().expect("page store lock poisoned").zones)
    }

    pub fn segment_reports(&self) -> Result<Vec<PageStoreSegmentReport>, PageStoreError> {
        let root = self
            .inner
            .lock()
            .expect("page store lock poisoned")
            .root
            .clone();
        let mut reports = Vec::new();
        for page_segment_id in segment_ids_at(&root)? {
            let bytes = fs::read(segment_path(&root, page_segment_id))?;
            reports.push(inspect_segment(&bytes, page_segment_id));
        }
        Ok(reports)
    }

    pub fn stats(&self) -> PageStoreStats {
        self.inner.lock().expect("page store lock poisoned").stats
    }
}

impl Default for LocalPageStore {
    fn default() -> Self {
        Self::new(unique_temp_path("pages"))
    }
}

fn segment_path(root: &Path, page_segment_id: u64) -> PathBuf {
    root.join(format!("page_segment_{page_segment_id:020}.seg"))
}

fn zone_manifest_path(root: &Path) -> PathBuf {
    root.join("page_zone_manifest.json")
}

fn load_zone_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, PageStoreZoneDescriptor>, PageStoreError> {
    let path = zone_manifest_path(root);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let manifest: PageStoreZoneManifest =
        serde_json::from_slice(&fs::read(path)?).map_err(|err| {
            PageStoreError::CorruptPageEnvelope {
                page_segment_id: 0,
                offset: 0,
                reason: format!("corrupt zone manifest: {err}"),
            }
        })?;
    Ok(manifest
        .zones
        .into_iter()
        .map(|zone| (zone.page_segment_id, zone))
        .collect())
}

fn rebuild_zone_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, PageStoreZoneDescriptor>, PageStoreError> {
    let mut zones = BTreeMap::new();
    let latest = latest_segment_id_at(root)?;
    for page_segment_id in segment_ids_at(root)? {
        let path = segment_path(root, page_segment_id);
        let bytes = fs::read(&path)?;
        let summary = summarize_segment(&bytes, page_segment_id)?;
        zones.insert(
            page_segment_id,
            PageStoreZoneDescriptor {
                zone_id: zone_id_for_segment(page_segment_id),
                page_segment_id,
                state: if page_segment_id == latest {
                    PageStoreZoneState::Active
                } else {
                    PageStoreZoneState::Sealed
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
        zones
            .entry(delayed.page_segment_id)
            .and_modify(|zone| {
                zone.state = PageStoreZoneState::DelayedDestroy;
                zone.updated_unix_ms = delayed.modified_unix_ms;
                zone.physical_bytes = delayed.physical_bytes;
            })
            .or_insert(PageStoreZoneDescriptor {
                zone_id: zone_id_for_segment(delayed.page_segment_id),
                page_segment_id: delayed.page_segment_id,
                state: PageStoreZoneState::DelayedDestroy,
                physical_bytes: delayed.physical_bytes,
                logical_bytes: 0,
                created_unix_ms: delayed.modified_unix_ms,
                updated_unix_ms: delayed.modified_unix_ms,
                first_page_id: None,
                last_page_id: None,
            });
    }
    Ok(zones)
}

fn persist_zone_manifest(
    root: &Path,
    zones: &BTreeMap<u64, PageStoreZoneDescriptor>,
) -> Result<(), PageStoreError> {
    fs::create_dir_all(root)?;
    let path = zone_manifest_path(root);
    let temp_path = path.with_extension(format!(
        "json.tmp.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));
    let manifest = PageStoreZoneManifest {
        version: 1,
        zones: zones.values().cloned().collect(),
    };
    {
        let mut temp = File::create(&temp_path)?;
        serde_json::to_writer_pretty(&mut temp, &manifest).map_err(|err| {
            PageStoreError::CorruptPageEnvelope {
                page_segment_id: 0,
                offset: 0,
                reason: format!("serialize zone manifest: {err}"),
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

fn summarize_zones(zones: &BTreeMap<u64, PageStoreZoneDescriptor>) -> PageStoreZoneSummary {
    let mut summary = PageStoreZoneSummary::default();
    for zone in zones.values() {
        summary.total_known_physical_bytes = summary
            .total_known_physical_bytes
            .saturating_add(zone.physical_bytes);
        match zone.state {
            PageStoreZoneState::Active => {
                summary.active_zones = summary.active_zones.saturating_add(1);
                summary.active_physical_bytes = summary
                    .active_physical_bytes
                    .saturating_add(zone.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(zone.physical_bytes);
            }
            PageStoreZoneState::Sealed => {
                summary.sealed_zones = summary.sealed_zones.saturating_add(1);
                summary.sealed_physical_bytes = summary
                    .sealed_physical_bytes
                    .saturating_add(zone.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(zone.physical_bytes);
            }
            PageStoreZoneState::DelayedDestroy => {
                summary.delayed_destroy_zones = summary.delayed_destroy_zones.saturating_add(1);
                summary.delayed_destroy_physical_bytes = summary
                    .delayed_destroy_physical_bytes
                    .saturating_add(zone.physical_bytes);
                summary.reclaimable_physical_bytes = summary
                    .reclaimable_physical_bytes
                    .saturating_add(zone.physical_bytes);
            }
            PageStoreZoneState::Purged => {
                summary.purged_zones = summary.purged_zones.saturating_add(1);
                summary.purged_physical_bytes = summary
                    .purged_physical_bytes
                    .saturating_add(zone.physical_bytes);
            }
        }
    }
    summary
}

fn ensure_zone_descriptor(
    zones: &mut BTreeMap<u64, PageStoreZoneDescriptor>,
    root: &Path,
    page_segment_id: u64,
    state: PageStoreZoneState,
) {
    zones.entry(page_segment_id).or_insert_with(|| {
        let physical_bytes = segment_path(root, page_segment_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        PageStoreZoneDescriptor {
            zone_id: zone_id_for_segment(page_segment_id),
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
    for zone in zones.values_mut() {
        if zone.page_segment_id == page_segment_id {
            zone.state = state;
            zone.updated_unix_ms = Some(transition_unix_ms);
        } else if zone.state == PageStoreZoneState::Active {
            zone.state = PageStoreZoneState::Sealed;
            zone.updated_unix_ms = Some(transition_unix_ms);
        }
    }
}

fn upsert_zone_after_append(
    zones: &mut BTreeMap<u64, PageStoreZoneDescriptor>,
    page_segment_id: u64,
    physical_bytes: u64,
    logical_bytes_written: u64,
    page_id: u64,
) {
    let zone = zones
        .entry(page_segment_id)
        .or_insert(PageStoreZoneDescriptor {
            zone_id: zone_id_for_segment(page_segment_id),
            page_segment_id,
            state: PageStoreZoneState::Active,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_page_id: Some(page_id),
            last_page_id: Some(page_id),
        });
    let updated_unix_ms = now_unix_ms();
    zone.state = PageStoreZoneState::Active;
    zone.physical_bytes = physical_bytes;
    zone.logical_bytes = zone.logical_bytes.saturating_add(logical_bytes_written);
    if zone.created_unix_ms.is_none() {
        zone.created_unix_ms = Some(updated_unix_ms);
    }
    zone.updated_unix_ms = Some(updated_unix_ms);
    zone.first_page_id = Some(
        zone.first_page_id
            .map_or(page_id, |first| first.min(page_id)),
    );
    zone.last_page_id = Some(zone.last_page_id.map_or(page_id, |last| last.max(page_id)));
}

fn set_zone_state(
    zones: &mut BTreeMap<u64, PageStoreZoneDescriptor>,
    page_segment_id: u64,
    state: PageStoreZoneState,
) {
    zones
        .entry(page_segment_id)
        .and_modify(|zone| {
            zone.state = state;
            zone.updated_unix_ms = Some(now_unix_ms());
        })
        .or_insert(PageStoreZoneDescriptor {
            zone_id: zone_id_for_segment(page_segment_id),
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

fn delayed_destroy_dir(root: &Path) -> PathBuf {
    root.join(".page_segment_trash")
}

fn delayed_destroy_path(root: &Path, page_segment_id: u64) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    delayed_destroy_dir(root).join(format!(
        "page_segment_{page_segment_id:020}.seg.deleted.{nanos}"
    ))
}

fn move_segment_to_delayed_destroy(
    root: &Path,
    page_segment_id: u64,
) -> Result<(), PageStoreError> {
    let source = segment_path(root, page_segment_id);
    let trash_dir = delayed_destroy_dir(root);
    fs::create_dir_all(&trash_dir)?;
    let destination = delayed_destroy_path(root, page_segment_id);
    fs::rename(&source, &destination)?;
    sync_parent_dir(&source)?;
    sync_parent_dir(&destination)?;
    Ok(())
}

fn delayed_destroy_segment_ids_at(root: &Path) -> Result<Vec<u64>, PageStoreError> {
    Ok(delayed_destroy_segment_reports_at(root)?
        .into_iter()
        .map(|report| report.page_segment_id)
        .collect())
}

fn delayed_destroy_segment_reports_at(
    root: &Path,
) -> Result<Vec<PageStoreDelayedDestroySegmentReport>, PageStoreError> {
    let trash_dir = delayed_destroy_dir(root);
    let mut reports = Vec::new();
    if !trash_dir.exists() {
        return Ok(reports);
    }
    for entry in fs::read_dir(trash_dir)? {
        let entry = entry?;
        if let Some(id) = delayed_destroy_segment_id_from_name(&entry.file_name()) {
            let metadata = entry.metadata().ok();
            reports.push(PageStoreDelayedDestroySegmentReport {
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

fn zone_id_for_segment(page_segment_id: u64) -> u64 {
    page_segment_id
}

const PAGE_RECORD_MAGIC: &[u8; 8] = b"TSPAGE01";
const PAGE_RECORD_VERSION: u8 = 6;
const PAGE_RECORD_V1_HEADER_LEN: usize = 8 + 1 + 1 + 2 + 8 + 8 + 32;
const PAGE_RECORD_V2_HEADER_LEN: usize = PAGE_RECORD_V1_HEADER_LEN + 8;
const PAGE_RECORD_V3_HEADER_LEN: usize = PAGE_RECORD_V2_HEADER_LEN + 8;
const PAGE_RECORD_V4_HEADER_LEN: usize = PAGE_RECORD_V3_HEADER_LEN + 8;
const PAGE_RECORD_V5_HEADER_LEN: usize = PAGE_RECORD_V4_HEADER_LEN + 8;
const PAGE_RECORD_HEADER_LEN: usize = PAGE_RECORD_V5_HEADER_LEN + 16;
const PAGE_RECORD_COMPRESSION_MIN_BYTES: usize = 256;
const PAGE_RECORD_COMPRESSION_LEVEL: i32 = 0;
const PAGE_RECORD_COMPRESSION_NONE: u8 = 0;
const PAGE_RECORD_COMPRESSION_ZSTD: u8 = 1;

fn default_page_record_compression_enabled() -> bool {
    true
}

fn default_page_record_compression_min_bytes() -> usize {
    PAGE_RECORD_COMPRESSION_MIN_BYTES
}

fn default_page_record_compression_level() -> i32 {
    PAGE_RECORD_COMPRESSION_LEVEL
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageRecordCompression {
    None,
    Zstd,
}

#[derive(Debug, Clone, Copy)]
struct PageRecordHeader {
    header_len: usize,
    payload_len: usize,
    stored_len: usize,
    expected_sha256: [u8; 32],
    page_id: Option<u64>,
    object_id: Option<u64>,
    routing_slot: Option<u32>,
    zone_id: Option<u64>,
    compression: PageRecordCompression,
}

#[derive(Debug)]
struct EncodedPageRecord {
    bytes: Vec<u8>,
    logical_len: usize,
    stored_len: usize,
    compression: PageRecordCompression,
}

#[derive(Debug)]
struct DecodedPageRecord {
    payload: Vec<u8>,
    logical_len: usize,
    compression: PageRecordCompression,
}

#[derive(Debug)]
struct LogicalRangeRead {
    bytes: Vec<u8>,
    compressed_records_read: u64,
}

fn encode_page_record(
    payload: &[u8],
    page_id: u64,
    object_id: Option<u64>,
    routing_slot: Option<u32>,
    zone_id: u64,
    options: PageStoreOptions,
) -> Result<EncodedPageRecord, PageStoreError> {
    let digest = Sha256::digest(payload);
    let (stored_payload, compression) = encode_page_record_payload(payload, options)?;
    let stored_len = stored_payload.len();
    let mut record = Vec::with_capacity(PAGE_RECORD_HEADER_LEN + stored_payload.len());
    record.extend_from_slice(PAGE_RECORD_MAGIC);
    record.push(PAGE_RECORD_VERSION);
    record.push(0);
    record.extend_from_slice(&(PAGE_RECORD_HEADER_LEN as u16).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&digest);
    record.extend_from_slice(&page_id.to_le_bytes());
    record.extend_from_slice(&object_id.unwrap_or_default().to_le_bytes());
    record.push(u8::from(routing_slot.is_some()));
    record.extend_from_slice(&[0, 0, 0]);
    record.extend_from_slice(&routing_slot.unwrap_or_default().to_le_bytes());
    record.extend_from_slice(&zone_id.to_le_bytes());
    record.push(match compression {
        PageRecordCompression::None => PAGE_RECORD_COMPRESSION_NONE,
        PageRecordCompression::Zstd => PAGE_RECORD_COMPRESSION_ZSTD,
    });
    record.extend_from_slice(&[0; 7]);
    record.extend_from_slice(&(stored_len as u64).to_le_bytes());
    record.extend_from_slice(&stored_payload);
    Ok(EncodedPageRecord {
        bytes: record,
        logical_len: payload.len(),
        stored_len,
        compression,
    })
}

fn encode_page_record_payload(
    payload: &[u8],
    options: PageStoreOptions,
) -> Result<(Vec<u8>, PageRecordCompression), PageStoreError> {
    if !options.compression_enabled || payload.len() < options.compression_min_bytes {
        return Ok((payload.to_vec(), PageRecordCompression::None));
    }
    let compressed = zstd::stream::encode_all(
        Cursor::new(payload),
        options.compression_level.clamp(-7, 22),
    )?;
    if compressed.len() < payload.len() {
        Ok((compressed, PageRecordCompression::Zstd))
    } else {
        Ok((payload.to_vec(), PageRecordCompression::None))
    }
}

fn decode_page_record(
    record: &[u8],
    address: &PageAddress,
) -> Result<DecodedPageRecord, PageStoreError> {
    if !record.starts_with(PAGE_RECORD_MAGIC) {
        return Ok(DecodedPageRecord {
            payload: record.to_vec(),
            logical_len: record.len(),
            compression: PageRecordCompression::None,
        });
    }
    if record.len() < PAGE_RECORD_V1_HEADER_LEN {
        return Err(corrupt_page_envelope(address, "short header"));
    }
    let header = parse_page_record_header(record, address)?;
    if let (Some(address_page_id), Some(record_page_id)) = (address.page_id, header.page_id) {
        if address_page_id != record_page_id {
            return Err(corrupt_page_envelope(
                address,
                format!("page id mismatch: address {address_page_id}, record {record_page_id}"),
            ));
        }
    }
    if let (Some(address_object_id), Some(record_object_id)) = (address.object_id, header.object_id)
    {
        if address_object_id != record_object_id {
            return Err(corrupt_page_envelope(
                address,
                format!(
                    "object id mismatch: address {address_object_id}, record {record_object_id}"
                ),
            ));
        }
    }
    if let (Some(address_routing_slot), Some(record_routing_slot)) =
        (address.routing_slot, header.routing_slot)
    {
        if address_routing_slot != record_routing_slot {
            return Err(corrupt_page_envelope(
                address,
                format!(
                    "routing slot mismatch: address {address_routing_slot}, record {record_routing_slot}"
                ),
            ));
        }
    }
    if let (Some(address_zone_id), Some(record_zone_id)) = (address.zone_id, header.zone_id) {
        if address_zone_id != record_zone_id {
            return Err(corrupt_page_envelope(
                address,
                format!("zone id mismatch: address {address_zone_id}, record {record_zone_id}"),
            ));
        }
    }
    if record.len() != header.header_len + header.stored_len {
        return Err(corrupt_page_envelope(
            address,
            "payload length mismatch".to_string(),
        ));
    }
    let payload = decode_page_record_payload(&record[header.header_len..], &header, address)?;
    verify_page_record_checksum(&payload, &header.expected_sha256, address)?;
    Ok(DecodedPageRecord {
        payload,
        logical_len: header.payload_len,
        compression: header.compression,
    })
}

fn logical_range_from_segment(
    segment: &[u8],
    page_segment_id: u64,
    offset: u64,
    size: u64,
) -> Result<LogicalRangeRead, PageStoreError> {
    if size == 0 {
        return Ok(LogicalRangeRead {
            bytes: Vec::new(),
            compressed_records_read: 0,
        });
    }
    if !segment.starts_with(PAGE_RECORD_MAGIC) {
        let start = offset as usize;
        let end = start.saturating_add(size as usize).min(segment.len());
        let bytes = if start >= segment.len() {
            Vec::new()
        } else {
            segment[start..end].to_vec()
        };
        return Ok(LogicalRangeRead {
            bytes,
            compressed_records_read: 0,
        });
    }

    let requested_start = offset as usize;
    let requested_end = requested_start.saturating_add(size as usize);
    let mut physical_offset = 0usize;
    let mut logical_offset = 0usize;
    let mut out = Vec::with_capacity(size as usize);
    let mut compressed_records_read = 0_u64;

    while physical_offset < segment.len() && out.len() < size as usize {
        let remaining = &segment[physical_offset..];
        let address = PageAddress {
            page_segment_id,
            offset: physical_offset as u64,
            length: 0,
            page_id: None,
            object_id: None,
            routing_slot: None,
            zone_id: None,
            sha256: None,
        };
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < PAGE_RECORD_V1_HEADER_LEN {
            return Err(corrupt_page_envelope(&address, "short header"));
        }
        let header = parse_page_record_header(remaining, &address)?;
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            return Err(corrupt_page_envelope(
                &address,
                "payload length mismatch".to_string(),
            ));
        }
        let address = PageAddress {
            length: record_len as u64,
            page_id: header.page_id,
            object_id: header.object_id,
            routing_slot: header.routing_slot,
            zone_id: header.zone_id,
            ..address
        };
        let payload = decode_page_record_payload(
            &remaining[header.header_len..record_len],
            &header,
            &address,
        )?;
        verify_page_record_checksum(&payload, &header.expected_sha256, &address)?;
        if header.compression == PageRecordCompression::Zstd {
            compressed_records_read += 1;
        }

        let logical_end = logical_offset.saturating_add(header.payload_len);
        let overlap_start = requested_start.max(logical_offset);
        let overlap_end = requested_end.min(logical_end);
        if overlap_start < overlap_end {
            let payload_start = overlap_start - logical_offset;
            let payload_end = overlap_end - logical_offset;
            out.extend_from_slice(&payload[payload_start..payload_end]);
        }

        physical_offset = physical_offset.saturating_add(record_len);
        logical_offset = logical_end;
    }

    Ok(LogicalRangeRead {
        bytes: out,
        compressed_records_read,
    })
}

fn parse_page_record_header(
    record: &[u8],
    address: &PageAddress,
) -> Result<PageRecordHeader, PageStoreError> {
    let version = record[8];
    if !matches!(version, 1..=PAGE_RECORD_VERSION) {
        return Err(corrupt_page_envelope(
            address,
            format!("unsupported version {version}"),
        ));
    }
    let header_len = u16::from_le_bytes(
        record[10..12]
            .try_into()
            .expect("page envelope header length slice"),
    ) as usize;
    let expected_header_len = if version == 1 {
        PAGE_RECORD_V1_HEADER_LEN
    } else if version == 2 {
        PAGE_RECORD_V2_HEADER_LEN
    } else if version == 3 {
        PAGE_RECORD_V3_HEADER_LEN
    } else if version == 4 {
        PAGE_RECORD_V4_HEADER_LEN
    } else if version == 5 {
        PAGE_RECORD_V5_HEADER_LEN
    } else {
        PAGE_RECORD_HEADER_LEN
    };
    if header_len != expected_header_len {
        return Err(corrupt_page_envelope(
            address,
            format!("unexpected header length {header_len}"),
        ));
    }
    if record.len() < expected_header_len {
        return Err(corrupt_page_envelope(address, "short header"));
    }
    let payload_len = u64::from_le_bytes(
        record[12..20]
            .try_into()
            .expect("page envelope payload length slice"),
    ) as usize;
    let raw_len = u64::from_le_bytes(
        record[20..28]
            .try_into()
            .expect("page envelope raw length slice"),
    ) as usize;
    if raw_len != payload_len {
        return Err(corrupt_page_envelope(
            address,
            format!("raw length {raw_len} does not match payload length {payload_len}"),
        ));
    }
    let expected_sha256 = record[28..60]
        .try_into()
        .expect("page envelope sha256 slice");
    let page_id = if version >= 2 {
        Some(u64::from_le_bytes(
            record[60..68]
                .try_into()
                .expect("page envelope page id slice"),
        ))
    } else {
        None
    };
    let object_id = if version >= 3 {
        let object_id = u64::from_le_bytes(
            record[68..76]
                .try_into()
                .expect("page envelope object id slice"),
        );
        (object_id != 0).then_some(object_id)
    } else {
        None
    };
    let routing_slot = if version >= 4 {
        if record[76] == 1 {
            Some(u32::from_le_bytes(
                record[80..84]
                    .try_into()
                    .expect("page envelope routing slot slice"),
            ))
        } else {
            None
        }
    } else {
        None
    };
    let zone_id = if version >= 5 {
        Some(u64::from_le_bytes(
            record[84..92]
                .try_into()
                .expect("page envelope zone id slice"),
        ))
    } else {
        None
    };
    let (compression, stored_len) = if version >= 6 {
        let compression = match record[92] {
            PAGE_RECORD_COMPRESSION_NONE => PageRecordCompression::None,
            PAGE_RECORD_COMPRESSION_ZSTD => PageRecordCompression::Zstd,
            codec => {
                return Err(corrupt_page_envelope(
                    address,
                    format!("unsupported compression codec {codec}"),
                ));
            }
        };
        let stored_len = u64::from_le_bytes(
            record[100..108]
                .try_into()
                .expect("page envelope stored length slice"),
        ) as usize;
        (compression, stored_len)
    } else {
        (PageRecordCompression::None, payload_len)
    };
    if compression == PageRecordCompression::None && stored_len != payload_len {
        return Err(corrupt_page_envelope(
            address,
            format!("stored length {stored_len} does not match payload length {payload_len}"),
        ));
    }
    Ok(PageRecordHeader {
        header_len,
        payload_len,
        stored_len,
        expected_sha256,
        page_id,
        object_id,
        routing_slot,
        zone_id,
        compression,
    })
}

fn decode_page_record_payload(
    stored_payload: &[u8],
    header: &PageRecordHeader,
    address: &PageAddress,
) -> Result<Vec<u8>, PageStoreError> {
    match header.compression {
        PageRecordCompression::None => Ok(stored_payload.to_vec()),
        PageRecordCompression::Zstd => {
            let payload = zstd::stream::decode_all(Cursor::new(stored_payload)).map_err(|err| {
                corrupt_page_envelope(address, format!("zstd decompression failed: {err}"))
            })?;
            if payload.len() != header.payload_len {
                return Err(corrupt_page_envelope(
                    address,
                    format!(
                        "decompressed length {} does not match payload length {}",
                        payload.len(),
                        header.payload_len
                    ),
                ));
            }
            Ok(payload)
        }
    }
}

fn verify_page_record_checksum(
    payload: &[u8],
    expected_sha256: &[u8; 32],
    address: &PageAddress,
) -> Result<(), PageStoreError> {
    let actual_sha256 = Sha256::digest(payload);
    if &actual_sha256[..] != expected_sha256 {
        return Err(PageStoreError::ChecksumMismatch {
            page_segment_id: address.page_segment_id,
            offset: address.offset,
            length: address.length,
            expected: hex::encode(expected_sha256),
            actual: hex::encode(actual_sha256),
        });
    }
    Ok(())
}

fn corrupt_page_envelope(address: &PageAddress, reason: impl Into<String>) -> PageStoreError {
    PageStoreError::CorruptPageEnvelope {
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        reason: reason.into(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn segment_ids_at(root: &Path) -> Result<Vec<u64>, PageStoreError> {
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

fn latest_segment_id_at(root: &Path) -> Result<u64, PageStoreError> {
    Ok(segment_ids_at(root)?.into_iter().max().unwrap_or_default())
}

fn next_page_id_at(root: &Path) -> Result<u64, PageStoreError> {
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SegmentSummary {
    logical_bytes: u64,
    first_page_id: Option<u64>,
    last_page_id: Option<u64>,
}

fn summarize_segment(
    segment: &[u8],
    page_segment_id: u64,
) -> Result<SegmentSummary, PageStoreError> {
    if !segment.starts_with(PAGE_RECORD_MAGIC) {
        return Ok(SegmentSummary {
            logical_bytes: segment.len() as u64,
            first_page_id: None,
            last_page_id: None,
        });
    }
    let mut physical_offset = 0usize;
    let mut summary = SegmentSummary::default();
    while physical_offset < segment.len() {
        let remaining = &segment[physical_offset..];
        let address = PageAddress {
            page_segment_id,
            offset: physical_offset as u64,
            length: 0,
            page_id: None,
            object_id: None,
            routing_slot: None,
            zone_id: None,
            sha256: None,
        };
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < PAGE_RECORD_V1_HEADER_LEN {
            return Err(corrupt_page_envelope(&address, "short header"));
        }
        let header = parse_page_record_header(remaining, &address)?;
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            return Err(corrupt_page_envelope(
                &address,
                "payload length mismatch".to_string(),
            ));
        }
        if let Some(page_id) = header.page_id {
            summary.first_page_id = Some(
                summary
                    .first_page_id
                    .map_or(page_id, |current| current.min(page_id)),
            );
            summary.last_page_id = Some(
                summary
                    .last_page_id
                    .map_or(page_id, |current| current.max(page_id)),
            );
        }
        summary.logical_bytes = summary
            .logical_bytes
            .saturating_add(header.payload_len as u64);
        physical_offset = physical_offset.saturating_add(record_len);
    }
    Ok(summary)
}

fn inspect_segment(segment: &[u8], page_segment_id: u64) -> PageStoreSegmentReport {
    let mut report = PageStoreSegmentReport {
        page_segment_id,
        physical_bytes: segment.len() as u64,
        ..PageStoreSegmentReport::default()
    };
    let mut object_ids = BTreeSet::new();
    let mut routing_slots = BTreeSet::new();
    if segment.is_empty() {
        return report;
    }
    if !segment.starts_with(PAGE_RECORD_MAGIC) {
        report.logical_bytes = segment.len() as u64;
        report.page_count = 1;
        return report;
    }

    let mut physical_offset = 0usize;
    while physical_offset < segment.len() {
        let remaining = &segment[physical_offset..];
        let mut address = PageAddress {
            page_segment_id,
            offset: physical_offset as u64,
            length: 0,
            page_id: None,
            object_id: None,
            routing_slot: None,
            zone_id: None,
            sha256: None,
        };
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            report.first_error = Some(
                corrupt_page_envelope(&address, "mixed raw bytes after page envelope").to_string(),
            );
            break;
        }
        if remaining.len() < PAGE_RECORD_V1_HEADER_LEN {
            report.first_error = Some(corrupt_page_envelope(&address, "short header").to_string());
            break;
        }
        let header = match parse_page_record_header(remaining, &address) {
            Ok(header) => header,
            Err(err) => {
                report.first_error = Some(err.to_string());
                break;
            }
        };
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            report.first_error = Some(
                corrupt_page_envelope(&address, "payload length mismatch".to_string()).to_string(),
            );
            break;
        }
        address.length = record_len as u64;
        address.page_id = header.page_id;
        address.object_id = header.object_id;
        address.routing_slot = header.routing_slot;
        address.zone_id = header.zone_id;
        match decode_page_record(&remaining[..record_len], &address) {
            Ok(decoded) => {
                report.page_count = report.page_count.saturating_add(1);
                report.logical_bytes = report
                    .logical_bytes
                    .saturating_add(decoded.logical_len as u64);
                if decoded.compression == PageRecordCompression::Zstd {
                    report.compressed_records = report.compressed_records.saturating_add(1);
                }
                if let Some(object_id) = header.object_id {
                    object_ids.insert(object_id);
                    report.object_count = object_ids.len() as u64;
                }
                if let Some(routing_slot) = header.routing_slot {
                    routing_slots.insert(routing_slot);
                    report.routing_slot_count = routing_slots.len() as u64;
                    report.first_routing_slot = routing_slots.first().copied();
                    report.last_routing_slot = routing_slots.last().copied();
                }
                if let Some(page_id) = header.page_id {
                    report.first_page_id = Some(
                        report
                            .first_page_id
                            .map_or(page_id, |current| current.min(page_id)),
                    );
                    report.last_page_id = Some(
                        report
                            .last_page_id
                            .map_or(page_id, |current| current.max(page_id)),
                    );
                }
            }
            Err(err) => {
                report.first_error = Some(err.to_string());
                break;
            }
        }
        physical_offset = physical_offset.saturating_add(record_len);
    }
    report
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            dir.sync_all()?;
        }
    }
    Ok(())
}

fn sync_dir(path: &Path) -> std::io::Result<()> {
    if let Ok(dir) = File::open(path) {
        dir.sync_all()?;
    }
    Ok(())
}

fn now_unix_ms() -> u64 {
    system_time_unix_ms(std::time::SystemTime::now()).unwrap_or_default()
}

fn file_created_unix_ms(path: &Path) -> Option<u64> {
    path.metadata()
        .ok()
        .and_then(|metadata| metadata.created().ok())
        .and_then(system_time_unix_ms)
}

fn file_modified_unix_ms(path: &Path) -> Option<u64> {
    path.metadata()
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(system_time_unix_ms)
}

fn system_time_unix_ms(time: std::time::SystemTime) -> Option<u64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}

fn unique_temp_path(kind: &str) -> PathBuf {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_segments_removes_old_non_current_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
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
        let store = LocalPageStore::new(dir.path());
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
        let store = LocalPageStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        let roll = store.roll_segment().unwrap();
        let second = store.append(b"second").unwrap();
        assert_eq!(roll.new_page_segment_id, second.page_segment_id);

        let reopened = LocalPageStore::new(dir.path());
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
        let store = LocalPageStore::new(dir.path());
        store.install_segment(3, b"restored-segment").unwrap();

        let next = store.append(b"after-restore").unwrap();

        assert_eq!(next.page_segment_id, 3);
        assert_eq!(next.offset, b"restored-segment".len() as u64);
        assert_eq!(store.read(&next).unwrap(), b"after-restore");
    }

    #[test]
    fn page_address_checksum_rejects_corrupt_segment_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let address = store.append(b"verified-page").unwrap();
        assert_eq!(address.sha256, Some(sha256_hex(b"verified-page")));
        assert_eq!(store.read(&address).unwrap(), b"verified-page");

        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        fs::write(path, segment).unwrap();
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::ChecksumMismatch { .. }));
    }

    #[test]
    fn page_segment_records_have_self_describing_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
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
        let store = LocalPageStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        let second = store.append(b"second").unwrap();
        assert_eq!(first.page_id, Some(0));
        assert_eq!(second.page_id, Some(1));

        let reopened = LocalPageStore::new(dir.path());
        let third = reopened.append(b"third").unwrap();

        assert_eq!(third.page_id, Some(2));
        assert_eq!(reopened.read(&third).unwrap(), b"third");
    }

    #[test]
    fn installed_segment_page_ids_advance_future_allocations() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let source = LocalPageStore::new(source_dir.path());
        let _ = source.append(b"first").unwrap();
        let restored = source.append(b"restored").unwrap();
        let restored_bytes = source.read_segment(restored.page_segment_id).unwrap();

        let store = LocalPageStore::new(dir.path());
        store.install_segment(4, &restored_bytes).unwrap();
        let next = store.append(b"next").unwrap();

        assert_eq!(next.page_id, Some(2));
        assert_eq!(store.read(&next).unwrap(), b"next");
    }

    #[test]
    fn page_id_mismatch_rejects_corrupt_address_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let mut address = store.append(b"identity-checked-page").unwrap();
        address.page_id = Some(address.page_id.unwrap() + 1);

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn object_ids_are_persisted_and_checked_in_page_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let mut address = store
            .append_with_page_metadata(b"object-page", Some(42), Some(7))
            .unwrap();

        assert_eq!(address.object_id, Some(42));
        assert_eq!(address.routing_slot, Some(7));
        assert_eq!(address.zone_id, Some(0));
        assert_eq!(store.read(&address).unwrap(), b"object-page");

        address.object_id = Some(43);
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::CorruptPageEnvelope { .. }));

        address.object_id = Some(42);
        address.routing_slot = Some(8);
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::CorruptPageEnvelope { .. }));

        address.routing_slot = Some(7);
        address.zone_id = Some(1);
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn rolled_segments_stamp_new_zone_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let first = store.append(b"first-zone").unwrap();
        let roll = store.roll_segment().unwrap();
        let second = store.append(b"second-zone").unwrap();

        assert_eq!(first.zone_id, Some(first.page_segment_id));
        assert_eq!(second.zone_id, Some(second.page_segment_id));
        assert_eq!(second.zone_id, Some(roll.new_page_segment_id));
        assert_ne!(first.zone_id, second.zone_id);
    }

    #[test]
    fn zone_manifest_tracks_roll_reopen_gc_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let first = store.append(b"first-zone").unwrap();
        store.roll_segment().unwrap();
        let second = store.append(b"second-zone").unwrap();

        let zones = store.zone_descriptors();
        assert_eq!(zones.len(), 2);
        assert_eq!(zones[0].page_segment_id, first.page_segment_id);
        assert_eq!(zones[0].state, PageStoreZoneState::Sealed);
        assert_eq!(zones[0].first_page_id, first.page_id);
        assert_eq!(zones[0].last_page_id, first.page_id);
        assert!(zones[0].created_unix_ms.is_some());
        assert!(zones[0].updated_unix_ms.is_some());
        assert_eq!(zones[1].page_segment_id, second.page_segment_id);
        assert_eq!(zones[1].state, PageStoreZoneState::Active);
        assert_eq!(zones[1].first_page_id, second.page_id);
        assert_eq!(zones[1].last_page_id, second.page_id);
        assert!(zones[1].created_unix_ms.is_some());
        assert!(zones[1].updated_unix_ms.is_some());
        assert!(zone_manifest_path(dir.path()).exists());
        let initial_summary = store.zone_summary();
        assert_eq!(initial_summary.sealed_zones, 1);
        assert_eq!(initial_summary.active_zones, 1);
        assert_eq!(initial_summary.delayed_destroy_zones, 0);
        assert_eq!(initial_summary.purged_zones, 0);
        assert_eq!(
            initial_summary.sealed_physical_bytes,
            zones[0].physical_bytes
        );
        assert_eq!(
            initial_summary.active_physical_bytes,
            zones[1].physical_bytes
        );
        assert_eq!(
            initial_summary.live_physical_bytes,
            zones[0].physical_bytes + zones[1].physical_bytes
        );
        assert_eq!(initial_summary.reclaimable_physical_bytes, 0);

        let reopened = LocalPageStore::new(dir.path());
        let reopened_zones = reopened.zone_descriptors();
        assert_eq!(reopened_zones.len(), zones.len());
        assert_eq!(reopened_zones[0], zones[0]);
        assert_eq!(reopened_zones[1].page_segment_id, zones[1].page_segment_id);
        assert_eq!(reopened_zones[1].state, zones[1].state);
        assert_eq!(reopened_zones[1].physical_bytes, zones[1].physical_bytes);
        assert_eq!(reopened_zones[1].logical_bytes, zones[1].logical_bytes);
        assert_eq!(reopened_zones[1].created_unix_ms, zones[1].created_unix_ms);
        assert!(reopened_zones[1].updated_unix_ms >= zones[1].updated_unix_ms);

        let report = reopened
            .gc_segments_before_with_live_refs_delayed_destroy(1, std::iter::empty())
            .unwrap();
        assert_eq!(report.delayed_destroy_page_segment_ids, vec![0]);
        let delayed = reopened.zone_descriptors();
        assert_eq!(delayed[0].state, PageStoreZoneState::DelayedDestroy);
        assert!(delayed[0].physical_bytes > 0);
        assert_eq!(delayed[0].created_unix_ms, zones[0].created_unix_ms);
        assert!(delayed[0].updated_unix_ms >= zones[0].updated_unix_ms);
        assert_eq!(delayed[1].state, PageStoreZoneState::Active);
        let delayed_summary = reopened.zone_summary();
        assert_eq!(delayed_summary.delayed_destroy_zones, 1);
        assert_eq!(delayed_summary.active_zones, 1);
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

        let purge = reopened
            .purge_delayed_destroy_segments_with_report()
            .unwrap();
        assert_eq!(purge.purged_page_segment_ids, vec![0]);
        assert!(purge.purged_physical_bytes > 0);
        let purged = LocalPageStore::new(dir.path()).zone_descriptors();
        assert_eq!(purged[0].state, PageStoreZoneState::Purged);
        assert_eq!(purged[0].created_unix_ms, zones[0].created_unix_ms);
        assert!(purged[0].updated_unix_ms >= delayed[0].updated_unix_ms);
        assert_eq!(purged[1].state, PageStoreZoneState::Active);
        let purged_summary = LocalPageStore::new(dir.path()).zone_summary();
        assert_eq!(purged_summary.purged_zones, 1);
        assert_eq!(purged_summary.active_zones, 1);
        assert_eq!(
            purged_summary.purged_physical_bytes,
            purged[0].physical_bytes
        );
        assert_eq!(purged_summary.live_physical_bytes, purged[1].physical_bytes);
        assert_eq!(purged_summary.reclaimable_physical_bytes, 0);
    }

    #[test]
    fn missing_zone_manifest_rebuilds_from_existing_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let first = store.append(b"first-zone").unwrap();
        store.roll_segment().unwrap();
        let second = store.append(b"second-zone").unwrap();
        fs::remove_file(zone_manifest_path(dir.path())).unwrap();

        let rebuilt = LocalPageStore::new(dir.path());
        let zones = rebuilt.zone_descriptors();

        assert_eq!(zones.len(), 2);
        assert_eq!(zones[0].page_segment_id, first.page_segment_id);
        assert_eq!(zones[0].state, PageStoreZoneState::Sealed);
        assert_eq!(zones[0].first_page_id, first.page_id);
        assert_eq!(zones[0].last_page_id, first.page_id);
        assert!(zones[0].created_unix_ms.is_some());
        assert!(zones[0].updated_unix_ms.is_some());
        assert_eq!(zones[1].page_segment_id, second.page_segment_id);
        assert_eq!(zones[1].state, PageStoreZoneState::Active);
        assert_eq!(zones[1].first_page_id, second.page_id);
        assert_eq!(zones[1].last_page_id, second.page_id);
        assert!(zones[1].created_unix_ms.is_some());
        assert!(zones[1].updated_unix_ms.is_some());
    }

    #[test]
    fn logical_page_range_skips_record_envelopes_across_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        store.append(b"abc").unwrap();
        store.append(b"def").unwrap();

        assert_eq!(store.read_logical_range(0, 1, 4).unwrap(), b"bcde");
    }

    #[test]
    fn compressed_page_records_round_trip_and_remain_logical() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
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

    #[test]
    fn segment_reports_describe_page_counts_bytes_and_compression() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
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
        assert_eq!(reports[0].first_page_id, first.page_id);
        assert_eq!(reports[0].last_page_id, second.page_id);
        assert_eq!(reports[0].first_error, None);
    }

    #[test]
    fn segment_reports_describe_object_and_routing_slot_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
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
        assert_eq!(reports[0].first_error, None);
    }

    #[test]
    fn segment_reports_capture_first_corrupt_record_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
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
        let disabled_store = LocalPageStore::with_options(
            disabled_dir.path(),
            PageStoreOptions {
                compression_enabled: false,
                ..PageStoreOptions::default()
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
        let threshold_store = LocalPageStore::with_options(
            threshold_dir.path(),
            PageStoreOptions {
                compression_min_bytes: payload.len() + 1,
                ..PageStoreOptions::default()
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
        let store = LocalPageStore::new(dir.path());
        let address = store.append(&b"compress-me-".repeat(80)).unwrap();
        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        fs::write(path, segment).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(
            err,
            PageStoreError::ChecksumMismatch { .. } | PageStoreError::CorruptPageEnvelope { .. }
        ));
    }

    #[test]
    fn page_envelope_rejects_corrupt_header_lengths() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let address = store.append(b"header-checked-page").unwrap();
        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        segment[10] = 1;
        segment[11] = 0;
        fs::write(path, segment).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn page_address_without_checksum_keeps_legacy_read_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let legacy_address = PageAddress {
            page_segment_id: 0,
            offset: 0,
            length: b"alteredpage".len() as u64,
            page_id: None,
            object_id: None,
            routing_slot: None,
            zone_id: None,
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
        let store = LocalPageStore::new(dir.path());
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
        let store = LocalPageStore::new(dir.path());
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
        let store = LocalPageStore::new(dir.path());
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
        let store = LocalPageStore::new(dir.path());
        store.install_segment(0, b"small").unwrap();
        store.install_segment(1, b"largest-stale-segment").unwrap();
        store.install_segment(2, b"live-segment").unwrap();
        store.install_segment(3, b"current-segment").unwrap();

        let policy = PageStoreGcPolicy {
            max_destroy_segments: 2,
            max_destroy_physical_bytes: b"small".len() as u64,
            max_utility_score: Some(0),
            min_age_ms: Some(0),
        };
        let plan = store.gc_policy_plan(3, [2_u64], &policy).unwrap();
        assert_eq!(plan.retain_from_page_segment_id, 3);
        assert_eq!(plan.candidate_count, 2);
        assert_eq!(plan.selected_page_segment_ids, vec![0]);
        assert_eq!(plan.selected_physical_bytes, b"small".len() as u64);
        assert_eq!(plan.skipped_by_policy_count, 0);
        assert_eq!(plan.skipped_by_budget_count, 1);
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
