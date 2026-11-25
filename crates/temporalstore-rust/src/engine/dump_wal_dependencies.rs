// SPDX-License-Identifier: Apache-2.0

use super::*;
use super::storage_bucket_internals::LiveBlockEntry;

pub(super) struct VerifiedDumpWal {
    pub(super) commitments: Vec<DumpWalFrameCommitment>,
    floor: u64,
    store: crate::wal::LocalWriteAheadLogStore,
    shard: ShardId,
    _leases: Vec<crate::wal::WriteAheadLogReadLease>,
    placements: BTreeSet<u64>,
    adopt: bool,
}

impl VerifiedDumpWal {
    pub(super) fn floor(&self) -> Option<u64> {
        (!self.commitments.is_empty()).then_some(self.floor)
    }

    pub(super) fn prune_capture_placements(&self, capture: &mut ShardState) -> bool {
        let before = capture.wal_resident_blocks.len();
        capture.wal_resident_blocks.retain(|key, _| self.placements.contains(key));
        before != capture.wal_resident_blocks.len()
    }

    pub(super) fn arm_adoption(&mut self) {
        self.adopt = true;
    }
}

impl Drop for VerifiedDumpWal {
    fn drop(&mut self) {
        if let Some(floor) = self.floor().filter(|_| self.adopt) {
            self.store.adopt_manifest_floor(self.shard, floor);
        }
    }
}

// Used by the Dump verifier after taking a read lease covering the group.
fn verify_hot_tombstone(
    state: &ShardState,
    row: &super::state::TombstoneElement,
    kind: &str,
    address: &crate::block_store::ElementEntry,
) -> bool {
    use super::container_blocks::{member_from_element_name, tombstone_block};
    let key = row.object_key.as_ref();
    let name = row.element_name.as_ref();
    if row.model_id.as_str() != kind
        || row.block != super::live_block_key(address)
        || !tombstone_block(kind, name).is_some_and(|bytes| bytes.len() as u64 == address.length())
    {
        return false;
    }
    // A tombstone cannot name an element still present in the durable model.
    // This also covers a new physical block for the same element.
    match kind {
        "hash" => !state.bucket_index.hashes.get(key)
            .is_some_and(|fields| fields.contains_key(name)),
        "set" => member_from_element_name(name).is_some_and(|member| {
            !state.bucket_index.sets.get(key).is_some_and(|members| members.contains_key(&member))
        }),
        "zset" => member_from_element_name(name).is_some_and(|member| {
            !state.bucket_index.zsets.get(key).is_some_and(|members| members.contains_key(&member))
        }),
        "list" => u64::from_str_radix(name, 16).ok().is_some_and(|sequence| {
            let sequence = sequence.wrapping_add(i64::MIN as u64) as i64;
            !state.bucket_index.lists.get(key).is_some_and(|list| list.contains_key(&sequence))
        }),
        _ => false,
    }
}

fn packed_hash_selectors(block: &crate::wal::StagedBlock)
    -> Option<(String, BTreeSet<String>)>
{
    use super::container_blocks::{decode_container_block, ContainerBlockDecode, ElementKeySpelling};
    let ContainerBlockDecode::Framed { spelling: ElementKeySpelling::Utf8, items, .. } =
        decode_container_block(&block.bytes)
    else {
        return None;
    };
    let mut names = BTreeSet::new();
    let mut first = None;
    for item in items {
        let name = String::from_utf8(item.key.to_vec()).ok()?;
        if item.deleted || !names.insert(name.clone()) {
            return None;
        }
        if first.is_none() {
            first = Some(name);
        }
    }
    Some((first?, names))
}

fn verify_dump_atomic_group(
    store: &crate::wal::LocalWriteAheadLogStore,
    shard_id: ShardId,
    anchor: u64,
    carrier_id: u64,
    carrier_end: crate::wal::ReplayPosition,
    carrier_bytes: &[u8],
    carrier: &crate::wal::WriteAheadLogRecord,
    after_previous_group: Option<crate::wal::ReplayPosition>,
) -> Result<(u64, Vec<DumpWalFrameCommitment>), Status> {
    let bad = || Status::error(
        "slot_dump_invalid_wal_binding",
        "resident atomic WAL group is incomplete or inconsistent",
    );
    let metadata = carrier.metadata.as_ref().ok_or_else(bad)?;
    let (Some(batch_id), Some(size), Some(index)) =
        (metadata.batch_id, metadata.batch_size, metadata.batch_index)
    else {
        return Err(bad());
    };
    if size < 2 || index != size || carrier.sequence > anchor || carrier.command.is_none() {
        return Err(bad());
    }
    let first = carrier.sequence.checked_sub(u64::from(size) - 1)
        .filter(|first| *first != 0).ok_or_else(bad)?;
    if carrier_end.log_id() <= carrier_id {
        return Err(bad());
    }
    // The atomic producer rolls only after the final record, so the
    // carrier's segment contains the entire group unless it was reclaimed.
    let mut position = match after_previous_group {
        Some(position) if position.log_id() <= carrier_id => position,
        Some(_) => return Err(bad()),
        None => store.dump_segment_start_at(shard_id, carrier_id)
            .map_err(|_| bad())?.ok_or_else(bad)?,
    };
    let mut expected = first;
    let mut commitments = Vec::new();
    loop {
        let (records, more, next) = store.scan_dump_frame_window(
            shard_id, position, carrier_end.log_id(), 4 * 1024 * 1024,
        ).map_err(|_| bad())?;
        for (log_id, bytes, record) in records {
            if record.sequence < first {
                continue;
            }
            if log_id > carrier_id || record.shard_id != shard_id
                || record.sequence != expected || record.command.is_none()
            {
                return Err(bad());
            }
            let record_index = u32::try_from(expected - first + 1).map_err(|_| bad())?;
            if !record.metadata.as_ref().is_some_and(|metadata| {
                metadata.batch_id == Some(batch_id) && metadata.batch_size == Some(size)
                    && metadata.batch_index == Some(record_index)
            }) {
                return Err(bad());
            }
            commitments.push(DumpWalFrameCommitment {
                log_id,
                sha256: sha256_hex_bytes(&bytes),
            });
            if record_index == size {
                if log_id != carrier_id || bytes != carrier_bytes {
                    return Err(bad());
                }
                return Ok((first, commitments));
            }
            if !record.staged_blocks.is_empty() || !record.outcomes.is_empty() {
                return Err(bad());
            }
            expected += 1;
        }
        if !more || next.log_id() <= position.log_id() {
            return Err(bad());
        }
        position = next;
    }
}

impl TemporalEngine {
    /// Check dependencies in the local WAL without rehydrating its registry.
    pub(super) fn read_local_dump_wal_dependencies(
        &self,
        shard_id: ShardId,
        anchor: u64,
        restored: &ShardState,
        entries: &[LiveBlockEntry],
        committed_frames: Option<&std::collections::BTreeMap<u64, String>>,
    ) -> Result<Option<VerifiedDumpWal>, Status> {
        use crate::engine::{live_block_key, storage_bucket_internals::ModelWalkTally};
        use crate::wal_record::is_wal_resident;

        let bad = |detail| Status::error("slot_dump_invalid_wal_binding", detail);
        // The on-disk bucket supplies the owner's filing. Never infer it using the target range.
        let mut filed = std::collections::BTreeMap::new();
        let mut named_tombstones = BTreeSet::new();
        for entry in entries.iter().filter(|entry| is_wal_resident(entry.address.block_slab_id())) {
            let Some(owner) = entry.filed_bucket() else { return Err(bad("resident block has no filed owner")); };
            if entry.address.block_slab_id() != crate::engine::HOT_BLOCK_SLAB_ID {
                return Err(bad("dump contains an unsupported or inconsistent resident binding"));
            }
            let identity = (entry.kind.as_str().to_owned(), entry.object_key.to_string(), live_block_key(&entry.address));
            if entry.deleted {
                if !named_tombstones.insert((owner, identity)) {
                    return Err(bad("duplicate named resident tombstone"));
                }
                continue;
            }
            if filed.insert(identity, owner).is_some_and(|prior| prior != owner) {
                return Err(bad("dump contains an unsupported or inconsistent resident binding"));
            }
        }
        // The bucket walker can omit unnameable rows. Require coverage of every encoded resident
        // block before treating the manifest's sentinel as a WAL dependency.
        let mut raw = BTreeSet::new();
        let mut raw_tombstones = BTreeSet::new();
        let mut dead_addresses = BTreeSet::new();
        for (owner, node) in &restored.bucket_index.bucket_map {
            let mut rows = std::collections::BTreeMap::new();
            for row in node.tombstone_elements.iter().filter(|row| is_wal_resident(row.block.0)) {
                if rows.insert(row.block, row).is_some() {
                    return Err(bad("duplicate resident tombstone metadata"));
                }
            }
            for block in node.block_index.values().filter(|entry| is_wal_resident(entry.address.block_slab_id())) {
                let address = live_block_key(&block.address);
                let kind = block.model_id().as_str();
                if block.address.block_slab_id() != crate::engine::HOT_BLOCK_SLAB_ID {
                    return Err(bad("unsupported direct WAL locator"));
                }
                if block.deleted() {
                    let row = rows.remove(&address).ok_or_else(|| bad("resident tombstone metadata is missing"))?;
                    if !verify_hot_tombstone(restored, row, kind, &block.address)
                        || !dead_addresses.insert(address)
                    {
                        return Err(bad("resident tombstone disagrees with its model, address or deletion state"));
                    }
                    raw_tombstones.insert((*owner, (kind.to_owned(), row.object_key.to_string(), address)));
                } else if !raw.insert((*owner, kind, address)) {
                    return Err(bad("duplicate resident block address and owner"));
                }
            }
            if !rows.is_empty() {
                return Err(bad("resident tombstone metadata has no encoded deleted row"));
            }
        }
        if raw_tombstones != named_tombstones || raw.iter().any(|(_, _, address)| dead_addresses.contains(address)) {
            return Err(bad("resident live and deleted inventories disagree"));
        }
        let named: BTreeSet<_> = filed.iter()
            .map(|((kind, _, address), owner)| (*owner, kind.as_str(), *address)).collect();
        if raw != named {
            return Err(bad("resident block inventory differs from named bucket rows"));
        }

        use super::timestamped_series::TimestampedSeries;
        let mut selectors = std::collections::BTreeMap::<_, Vec<_>>::new();
        let mut add = |kind, key: &str, name, address: &crate::block_store::ElementEntry| {
            selectors.entry((kind, key.to_owned(), live_block_key(address)))
                .or_default().push(name);
        };
        for (key, fields) in &restored.bucket_index.hashes {
            for (field, address) in fields.iter() {
                if is_wal_resident(address.block_slab_id()) {
                    add("hash", key, field.to_string(), address);
                }
            }
        }
        for (key, members) in &restored.bucket_index.sets {
            for (member, address) in members.iter() {
                if is_wal_resident(address.block_slab_id()) {
                    add("set", key, super::execute_on_shard::set_element_name(member), address);
                }
            }
        }
        let mut zset_scores = std::collections::BTreeMap::new();
        for (key, members) in &restored.bucket_index.zsets {
            for (member, (score, address)) in members.iter() {
                if is_wal_resident(address.block_slab_id()) {
                    let name = super::execute_on_shard::zset_element_name(member);
                    let identity = (stable_block_object_id(shard_id, "zset", key),
                                    live_block_key(address), name.clone());
                    if zset_scores.insert(identity, score.to_be_bytes()).is_some() {
                        return Err(bad("ambiguous resident Zset member score"));
                    }
                    add("zset", key, name, address);
                }
            }
        }
        for (key, elements) in &restored.bucket_index.lists {
            for (sequence, address) in elements.iter() {
                if is_wal_resident(address.block_slab_id()) {
                    let sequence = (*sequence as u64).wrapping_sub(i64::MIN as u64);
                    add("list", key, format!("{sequence:016x}"), address);
                }
            }
        }
        for (key, series) in restored.features() {
            for (timestamp, address) in series.points() {
                if is_wal_resident(address.block_slab_id()) {
                    add("feature", key, super::packed_blocks::timestamped_element_name(timestamp, None), address);
                }
            }
        }
        let mut models = Vec::new();
        crate::engine::storage_bucket_internals::visit_model_live_blocks(
            restored, ModelWalkTally::WholeShardEntries,
            |_, address| is_wal_resident(address.block_slab_id()),
            |kind, key, field, address| {
                let key = key.as_str();
                let locator = live_block_key(address);
                if matches!(kind.as_str(), "hash" | "set" | "zset" | "list" | "feature") {
                    if let Some(names) = selectors.remove(&(kind.as_str(), key.to_owned(), locator)) {
                        for name in names {
                            models.push((kind.as_str().to_owned(), key.to_owned(), Some(name), locator));
                        }
                    }
                    return;
                }
                models.push((kind.as_str().to_owned(), key.to_owned(), field.map(str::to_owned), locator));
            },
        );
        if !selectors.is_empty() {
            return Err(bad("resident model selectors are outside the block inventory"));
        }
        let mut found = BTreeSet::new();
        let mut expected_placements = BTreeSet::new();
        let mut requirements = std::collections::BTreeMap::<u64, Vec<_>>::new();
        let mut floor = u64::MAX;
        for (kind, key, field, locator) in models {
            let Some(&owner) = filed.get(&(kind.clone(), key.clone(), locator)) else { return Err(bad("dump contains an unsupported or inconsistent resident binding")); };
            let object = stable_block_object_id(shard_id, &kind, &key);
            let registration = if kind == "feature" { None } else { field.as_deref() };
            let folded = block_in_wal::wal_resident_key(object, registration);
            let logical = restored.wal_resident_blocks.get(&folded);
            let address_fold = block_in_wal::wal_resident_address_key(object, locator);
            let by_address = restored.wal_resident_blocks.get(&address_fold);
            if let (Some(logical), Some(by_address)) = (logical, by_address) {
                if logical.log_id != by_address.log_id || logical.sequence != by_address.sequence {
                    return Err(bad("logical and address-folded resident placements disagree"));
                }
            }
            if logical.is_some() {
                expected_placements.insert(folded);
            }
            if by_address.is_some() {
                expected_placements.insert(address_fold);
            }
            let placement = by_address.or(logical)
                .ok_or_else(|| bad("resident model entry has no placement"))?;
            if placement.sequence == 0 || placement.sequence > anchor {
                return Err(bad("dump contains an unsupported or inconsistent resident binding"));
            }
            found.insert((kind.clone(), key.clone(), locator));
            floor = floor.min(placement.sequence);
            requirements.entry(placement.log_id).or_default().push(
                (placement.sequence, owner, object, kind, key, field, locator));
        }
        if filed.keys().ne(found.iter())
            || (committed_frames.is_some()
                && restored.wal_resident_blocks.keys().ne(expected_placements.iter()))
        {
            return Err(bad("resident placements, model entries or committed frame inventory differ"));
        }
        if requirements.is_empty() {
            if committed_frames.is_some_and(|frames| !frames.is_empty()) {
                return Err(bad("metadata-only image carries unexpected WAL commitments"));
            }
            return Ok((!raw_tombstones.is_empty()).then(|| VerifiedDumpWal {
                commitments: Vec::new(), floor: u64::MAX, store: self.wal_store.clone(),
                shard: shard_id, _leases: Vec::new(), placements: expected_placements, adopt: false,
            }));
        }
        // A sealed-segment GC already in flight planned its unlinks before this new lease.
        let lease = self.wal_store.try_retain_unprotected_read_sequence(shard_id, floor)
            .ok_or_else(|| Status::error("slot_dump_wal_gc_busy", "retry validation after WAL collection"))?;
        let mut leases = vec![lease];
        // Requirements are ordered by log id. A proved group leaves a verified
        // physical cursor, so later groups do not rescan the same WAL prefix.
        let mut after_previous_group = None;
        let mut commitments = std::collections::BTreeMap::new();
        let requirements: Vec<_> = requirements.into_iter().collect();
        for batch in requirements.chunks(256) {
            let ids = batch.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            let frames = self.wal_store.read_frames_with_ends(shard_id, &ids).map_err(|_| bad("dump contains an unsupported or inconsistent resident binding"))?;
            if frames.len() != batch.len() { return Err(bad("dump contains an unsupported or inconsistent resident binding")); }
            for ((id, wants), frame) in batch.iter().zip(frames) {
                let (bytes, frame_end) = frame.ok_or_else(|| bad("resident frame is missing"))?;
                let record = crate::wal::decode_wal_line(&bytes)
                    .map_err(|_| bad("resident frame does not decode"))?;
                if record.shard_id != shard_id {
                    return Err(bad("resident frame belongs to another shard"));
                }
                let batch = record.metadata.as_ref()
                    .map(|metadata| (metadata.batch_id, metadata.batch_size, metadata.batch_index));
                if matches!(batch, None | Some((None, None, None))) {
                    commitments.insert(*id, sha256_hex_bytes(&bytes));
                } else {
                    let Some((Some(_), Some(size), Some(index))) = batch else {
                        return Err(bad("resident frame has incomplete atomic metadata"));
                    };
                    if size < 2 || index != size || record.sequence > anchor {
                        return Err(bad("resident frame is not a committed atomic carrier"));
                    }
                    let first = record.sequence.checked_sub(u64::from(size) - 1)
                        .filter(|first| *first > 0)
                        .ok_or_else(|| bad("resident atomic group has an invalid start"))?;
                    if first < floor {
                        let lease = self.wal_store.try_retain_unprotected_read_sequence(shard_id, first)
                            .ok_or_else(|| Status::error("slot_dump_wal_gc_busy", "retry validation after WAL collection"))?;
                        leases.push(lease);
                    }
                    let (start, group) = verify_dump_atomic_group(
                        &self.wal_store, shard_id, anchor, *id, frame_end, &bytes, &record,
                        after_previous_group,
                    )?;
                    after_previous_group = Some(frame_end);
                    floor = floor.min(start);
                    for frame in group {
                        if commitments.insert(frame.log_id, frame.sha256).is_some() {
                            return Err(bad("resident atomic groups overlap"));
                        }
                    }
                }
                let bindings = block_in_wal::outcome_block_bindings(&record.outcomes, &record.staged_blocks);
                let mut packed_hashes = std::collections::BTreeMap::new();
                let mut feature_points = std::collections::BTreeMap::new();
                for (sequence, owner, object, kind, key, field, locator) in wants {
                    if record.sequence != *sequence {
                        return Err(bad("dump contains an unsupported or inconsistent resident binding"));
                    }
                    let row = bindings.get(&(*object, *locator))
                        .ok_or_else(|| bad("resident locator has no unique carried block"))?;
                    let block = &record.staged_blocks[*row];
                    let packed = if kind == "hash" && block.element_name.is_none() {
                        Some(packed_hashes.entry(*row).or_insert_with(|| packed_hash_selectors(block))
                            .as_ref().ok_or_else(|| bad("invalid packed Hash field inventory"))?)
                    } else {
                        None
                    };
                    let matches_field = if kind == "feature" {
                        let timestamps = feature_points.entry(*row).or_insert_with(|| {
                            let super::state::PackedFeatureBlockDecode::Packed(points) =
                                super::packed_blocks::decode_feature_block_strict(&block.bytes)
                            else {
                                return None;
                            };
                            let mut timestamps = BTreeSet::new();
                            for point in points {
                                if !timestamps.insert(super::packed_blocks::timestamped_element_name(
                                    point.timestamp_ms, None,
                                )) {
                                    return None;
                                }
                            }
                            Some(timestamps)
                        }).as_ref().ok_or_else(|| bad("invalid resident Feature point inventory"))?;
                        block.element_name.is_none()
                            && field.as_deref().is_some_and(|field| timestamps.contains(field))
                    } else { match packed {
                        Some((_, names)) => field.as_deref().is_some_and(|field| names.contains(field)),
                        None => block_in_wal::block_has_exact_manifest_element(
                            &record, block, kind, field.as_deref(),
                        ),
                    }};
                    let score = if kind == "zset" {
                        let name = field.as_ref().ok_or_else(|| bad("missing resident Zset member"))?;
                        Some(zset_scores.get(&(*object, *locator, name.clone()))
                            .ok_or_else(|| bad("missing resident Zset score"))?)
                    } else {
                        None
                    };
                    let mut matching_items = record.outcomes.iter().filter(|item| {
                        !item.deleted && !item.meta && item.object_id == *object
                            && item.routing_bucket == *owner && item.kind.as_str() == kind.as_str()
                            && item.object_key == *key
                            && score.is_none_or(|score| item.value.as_deref() == Some(score.as_slice()))
                            && (kind != "feature" || item.element_name.is_some())
                            && item.element_name.as_deref().is_none_or(|name| {
                                match packed {
                                    Some((first, _)) => first == name,
                                    None => field.as_deref() == Some(name),
                                }
                            })
                            && item.resolved_address()
                                .is_some_and(|address| live_block_key(&address) == *locator)
                    });
                    let first = matching_items.next();
                    let (matching, duplicates_identical) = matching_items.fold(
                        (usize::from(first.is_some()), true),
                        |(count, identical), item| (count + 1, identical && first == Some(item)),
                    );
                    // Older ListPush filed the same mutation twice. Accept
                    // only that semantically identical historical pair. An
                    // outcome claiming the same address or list position
                    // cannot hide by disagreeing on one filtered attribute.
                    let uniquely_bound = if kind == "list" {
                        let candidates = record.outcomes.iter().filter(|item| {
                            item.resolved_address().is_some_and(|address| live_block_key(&address) == *locator)
                                || (item.kind == "list" && item.object_key == *key
                                    && item.element_name.as_deref() == field.as_deref())
                        }).count();
                        candidates == matching && (matching == 1 || (matching == 2 && duplicates_identical))
                    } else {
                        matching == 1
                    };
                    if !uniquely_bound || !matches_field {
                        return Err(Status::error("slot_dump_invalid_wal_binding", format!(
                            "resident binding failed for model {kind}: selector={}, matching_outcomes={matching}, carried_selector_valid={matches_field}",
                            if field.is_some() { "named" } else { "whole" },
                        )));
                    }
                }
            }
        }
        if committed_frames.is_some_and(|frames| *frames != commitments) {
            return Err(bad("resident frame commitments differ from verified coverage"));
        }
        Ok(Some(VerifiedDumpWal {
            commitments: commitments.into_iter()
                .map(|(log_id, sha256)| DumpWalFrameCommitment { log_id, sha256 }).collect(),
            floor, store: self.wal_store.clone(), shard: shard_id, _leases: leases,
            placements: expected_placements, adopt: false,
        }))
    }

    pub(super) fn verify_committed_dump_wal(
        &self, manifest: &BucketDumpManifest, restored: &ShardState, entries: &[LiveBlockEntry],
    ) -> Result<Option<VerifiedDumpWal>, Status> {
        if manifest.version < 4 && manifest.wal_frames.is_empty() {
            return Ok(None);
        }
        if manifest.version != 4
            || manifest.wal_frames.windows(2).any(|pair| pair[0].log_id >= pair[1].log_id)
        {
            return Err(Status::error("slot_dump_invalid_wal_binding", "invalid WAL commitments"));
        }
        let commitments = manifest.wal_frames.iter()
            .map(|frame| (frame.log_id, frame.sha256.clone())).collect();
        self.read_local_dump_wal_dependencies(
            manifest.shard_id, manifest.wal_sequence, restored, entries, Some(&commitments),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{Config, SetConfigRequest};
    use crate::types::{Command, CommandResponse, ExecuteRequest};
    use std::path::Path;

    fn engine_at(root: &Path) -> TemporalEngine {
        TemporalEngine::with_local_dirs(
            1 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
        )
    }

    #[test]
    fn atomic_dump_proof_binds_every_record_and_rejects_a_cut_group() {
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        let command = |key: &str| Command::StringSet {
            key: key.into(),
            value: vec![7; 64],
        };
        engine.wal_store.append_batch_atomic_recorded(
            1, vec![command("prefix")], true, Vec::new(), Vec::new(),
        ).unwrap();
        let (records, carrier) = engine.wal_store.append_batch_atomic_recorded(
            1, vec![command("a"), command("b")], true, Vec::new(), Vec::new(),
        ).unwrap();
        let id = carrier.unwrap();
        let last = records.last().unwrap();
        let first = records.first().unwrap().sequence;
        let read_carrier = || engine.wal_store.read_frames_with_ends(1, &[id]).unwrap()
            .pop().unwrap().unwrap();
        let (bytes, end) = read_carrier();
        let (floor, hashes) = verify_dump_atomic_group(
            &engine.wal_store, 1, last.sequence, id, end, &bytes, last, None,
        ).unwrap();
        assert_eq!(floor, first);
        assert_eq!(hashes.len(), 2);
        assert_eq!(hashes[1].log_id, id);
        assert_eq!(hashes[1].sha256, sha256_hex_bytes(&bytes));
        assert!(verify_dump_atomic_group(
            &engine.wal_store, 1, first, id, end, &bytes, last, None,
        ).is_err(), "the carrier must be within the captured anchor");
        let mut wrong = last.clone();
        wrong.metadata.as_mut().unwrap().batch_id = Some(u64::MAX);
        assert!(verify_dump_atomic_group(
            &engine.wal_store, 1, last.sequence, id, end, &bytes, &wrong, None,
        ).is_err(), "an unrelated group cannot prove this carrier");

        let anchor = crate::wal::DurableIndexAnchor::proven_durable_through(1, last.sequence);
        let report = engine.wal_store.gc_before_sequence(1, last.sequence, &anchor).unwrap();
        assert!(report.records_removed >= 2, "the preceding member must actually be removed");
        let (retained, retained_end) = read_carrier();
        assert_eq!((retained.as_slice(), retained_end), (bytes.as_slice(), end),
                   "the carrier's address and contents survive prefix reclaim");
        assert!(verify_dump_atomic_group(
            &engine.wal_store, 1, last.sequence, id, retained_end, &retained, last, None,
        ).is_err(), "a retained carrier does not prove the missing prefix");
    }

    #[test]
    fn atomic_dump_proof_stops_at_the_physical_end_of_a_packed_carrier() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        let command = |key: &str, mut state: u64| Command::StringSet {
            key: key.into(),
            value: (0..crate::stream_format::BLOCK_SIZE * 3).map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            }).collect(),
        };
        let (records, carrier) = engine.wal_store.append_batch_atomic_recorded(
            1, vec![command("first", 7), command("carrier", 98)], true, Vec::new(), Vec::new(),
        ).unwrap();
        let id = carrier.unwrap();
        let last = records.last().unwrap();
        let (bytes, end) = engine.wal_store.read_frames_with_ends(1, &[id]).unwrap()
            .pop().unwrap().unwrap();
        assert_eq!(bytes.first().copied(), Some(0xB4), "carrier must use packed transport");
        assert!(end.log_id() - id > crate::stream_format::BLOCK_SIZE as u64,
                "packed carrier must cross a footer");
        assert_eq!(end.log_id() - id, bytes.len() as u64, "the reader retains transport bytes");
        let (next_records, next_carrier) = engine.wal_store.append_batch_atomic_recorded(
            1, vec![command("next-first", 222), command("next-carrier", 444)], true,
            Vec::new(), Vec::new(),
        ).unwrap();
        let next_id = next_carrier.unwrap();
        let next_last = next_records.last().unwrap();
        let (next_bytes, next_end) = engine.wal_store.read_frames_with_ends(1, &[next_id]).unwrap()
            .pop().unwrap().unwrap();
        let (_, unrelated) = engine.wal_store.append_batch_atomic_recorded(
            1, vec![Command::StringSet { key: "later".into(), value: vec![1] }],
            true, Vec::new(), Vec::new(),
        ).unwrap();
        let unrelated = unrelated.unwrap();
        assert!(unrelated >= next_end.log_id());
        let offset = engine.wal_store.resolve_log_id(1, unrelated).unwrap().unwrap();
        let log = root.path().join("indexes/wals/shard-1.wal.bin");
        let mut writer = std::fs::OpenOptions::new().read(true).write(true).open(log).unwrap();
        writer.seek(SeekFrom::Start(offset)).unwrap();
        writer.write_all(&[0xB4, 0xff, 0xff, 0xff, 0xff]).unwrap();
        writer.sync_all().unwrap();
        let length = writer.metadata().unwrap().len();
        let (floor, hashes) = verify_dump_atomic_group(
            &engine.wal_store, 1, last.sequence, id, end, &bytes, last, None,
        ).unwrap();
        assert_eq!(floor, records[0].sequence);
        assert_eq!(hashes.len(), 2);
        let before = engine.wal_store.stats_without_rescan(1).bytes_read;
        let (next_floor, next_hashes) = verify_dump_atomic_group(
            &engine.wal_store, 1, next_last.sequence, next_id, next_end, &next_bytes, next_last,
            Some(end),
        ).unwrap();
        let forward_bytes = engine.wal_store.stats_without_rescan(1).bytes_read - before;
        assert_eq!(next_floor, next_records[0].sequence);
        assert_eq!(next_hashes.len(), 2);
        assert!(forward_bytes > 0 && forward_bytes <= next_end.log_id() - end.log_id(),
                "the second proof may only read its interval, got {forward_bytes}");
        assert_eq!(writer.metadata().unwrap().len(), length);
        writer.seek(SeekFrom::Start(offset)).unwrap();
        let mut unchanged = [0; 5];
        writer.read_exact(&mut unchanged).unwrap();
        assert_eq!(unchanged, [0xB4, 0xff, 0xff, 0xff, 0xff]);
    }

    #[test]
    fn atomic_resident_manifest_keeps_the_group_through_reclaim_and_reopen() {
        const CHILD: &str = "TS_TEST_ATOMIC_RESIDENT_DUMP_CHILD";
        const TEST: &str = "engine::dump_wal_dependencies::tests::atomic_resident_manifest_keeps_the_group_through_reclaim_and_reopen";
        if std::env::var_os(CHILD).is_none() {
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD, "1")
                .env("TS_WAL_DATA_ONLY", "0")
                .output().unwrap();
            assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stdout));
            assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed"));
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        assert!(engine.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        let write = |key: &str, byte| Command::StringSet { key: key.into(), value: vec![byte; 64] };
        assert!(engine.execute(ExecuteRequest { shard_id: 1, command: write("prefix", 1) }).status.ok);
        assert_eq!(engine.materialize_synthetic_blocks(1), 1);
        assert!(engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1, commands: vec![write("first", 2), write("second", 3)],
        }).status.ok);
        let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
        assert_eq!(manifest.version, 4);
        assert_eq!(manifest.wal_frames.len(), 2, "both producer group members must be committed");
        engine.install_bucket_dump_manifest(&manifest).unwrap();
        engine.refresh_wal_retention_floor(1).unwrap();
        let anchor = crate::wal::DurableIndexAnchor::proven_durable_through(1, manifest.wal_sequence);
        let report = engine.wal_store.gc_before_sequence(1, manifest.wal_sequence, &anchor).unwrap();
        assert!(report.records_removed > 0, "the earlier unreferenced prefix must be reclaimed");
        assert_eq!(report.effective_retain_from_sequence, manifest.wal_sequence - 1);
        drop(engine);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        reopened.validate_bucket_dump_manifest(&manifest).unwrap();
        for (key, byte) in [("prefix", 1), ("first", 2), ("second", 3)] {
            let response = reopened.execute(ExecuteRequest {
                shard_id: 1, command: Command::StringGet { key: key.into() },
            });
            assert!(response.status.ok);
            assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![byte; 64]) });
        }
    }

    #[test]
    fn unused_direct_dump_wal_locator_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        assert!(engine.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        assert!(engine.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringSet { key: "hot".into(), value: vec![5; 64] },
        }).status.ok);
        let (capture, _) = engine.capture_shard_checkpoint(1).unwrap();
        let mut entries = super::super::storage_bucket_internals::collect_live_block_entries(&capture);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].address.block_slab_id(), super::super::HOT_BLOCK_SLAB_ID);
        let rows = engine.wal_store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(rows.len(), 1);
        let (id, bytes) = &rows[0];
        let item = crate::wal_record::WalItem::default();
        entries[0].address = crate::wal_record::block_address_from_item(
            *id, bytes.len() as u64, &item,
        ).unwrap();
        let status = engine.read_local_dump_wal_dependencies(
            1, capture.applied_wal_sequence.unwrap(), &capture, &entries, None,
        ).err().expect("an offset into the current log is not a supported direct item");
        assert_eq!(status.code, "slot_dump_invalid_wal_binding");
        assert!(engine.list_bucket_dump_manifests(1).is_empty());
    }

    #[test]
    fn resident_feature_proof_keeps_the_live_subset_of_each_packed_frame() {
        use super::super::timestamped_series::{TimestampedSeries, TimestampedSeriesWrite};
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        assert!(engine.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        let point = |timestamp_ms, value| crate::types::FeaturePoint {
            timestamp_ms, value: vec![value; 64],
        };
        let append = |points| {
            assert!(engine.execute(ExecuteRequest {
                shard_id: 1, command: Command::FeatureAppend { key: "series".into(), points },
            }).status.ok);
        };
        append(vec![point(5, 1), point(9, 2), point(15, 3)]);
        {
            let shards = engine.shards.read().unwrap();
            let series = &shards[&1].features()["series"];
            assert_eq!(series.get(5), series.get(9));
            assert_eq!(series.get(9), series.get(15));
            assert_eq!(series.get(5).unwrap().block_slab_id(), super::super::HOT_BLOCK_SLAB_ID);
        }
        append(vec![point(9, 4)]);
        let (mut invalid, _) = engine.capture_shard_checkpoint(1).unwrap();
        let series = invalid.features_mut().get_mut("series").unwrap();
        let address = series.get(5).unwrap().clone();
        assert_ne!(series.get(9), Some(&address));
        series.clear(5);
        series.set(500, address);
        let entries = super::super::storage_bucket_internals::collect_live_block_entries(&invalid);
        let status = engine.read_local_dump_wal_dependencies(
            1, invalid.applied_wal_sequence.unwrap(), &invalid, &entries, None,
        ).err().expect("a live timestamp must belong to the claimed carried frame");
        assert_eq!(status.code, "slot_dump_invalid_wal_binding");
        let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
        assert_eq!(manifest.version, 4);
        assert_eq!(manifest.wal_frames.len(), 2);
        engine.install_bucket_dump_manifest(&manifest).unwrap();
        drop(engine);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1, command: Command::FeatureQuery {
                key: "series".into(), start_ms: 0, end_ms: 999, count: None,
            },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::FeaturePoints {
            points: vec![point(5, 1), point(9, 4), point(15, 3)],
        });
    }

    #[test]
    fn resident_zset_proof_rejects_wrong_score_for_the_same_member_and_page() {
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        assert!(engine.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        let member = vec![0xff, 0, 7];
        assert!(engine.execute(ExecuteRequest {
            shard_id: 1, command: Command::ZSetAdd {
                key: "rank".into(), member: member.clone(), score: -7.5,
            },
        }).status.ok);
        let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
        assert_eq!(manifest.version, 4);
        engine.validate_bucket_dump_manifest(&manifest).unwrap();
        let (mut capture, _) = engine.capture_shard_checkpoint(1).unwrap();
        let address = capture.bucket_index.zsets.get("rank").unwrap()[&member].1.clone();
        assert_eq!(address.block_slab_id(), super::super::HOT_BLOCK_SLAB_ID);
        let score = super::super::execute_on_shard::zset_score_bits(4.25);
        let name = super::super::execute_on_shard::zset_element_name(&member);
        super::super::recorded_map::install_element::<super::super::recorded_map::ZSetKind>(
            &mut capture, 1, "rank", Some(name), member.clone(), (score, address.clone()),
            false, address,
        );
        let entries = super::super::storage_bucket_internals::collect_live_block_entries(&capture);
        let status = engine.verify_committed_dump_wal(&manifest, &capture, &entries).err()
            .expect("same member and page cannot authorize a different durable score");
        assert_eq!(status.code, "slot_dump_invalid_wal_binding");
        engine.install_bucket_dump_manifest(&manifest).unwrap();
        drop(engine);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        let answer = reopened.execute(ExecuteRequest {
            shard_id: 1, command: Command::ZSetScore { key: "rank".into(), member },
        });
        assert!(answer.status.ok);
        assert_eq!(answer.response, CommandResponse::Bytes { value: Some(b"-7.5".to_vec()) });
    }

    #[test]
    fn resident_hash_tombstone_is_portable_without_its_wal_payload() {
        use super::super::storage_bucket_internals::{collect_live_block_entries, StoredModelKind};
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        assert!(engine.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        for command in [
            Command::HashSet { key: "gone".into(), field: "member".into(), value: vec![7; 64] },
            Command::HashDelete { key: "gone".into(), field: "member".into() },
        ] {
            assert!(engine.execute(ExecuteRequest { shard_id: 1, command }).status.ok);
        }
        let owner = engine.routing_bucket_for_key(1, "gone");
        let manifest = engine.create_bucket_dump_manifest(1, [owner]).unwrap();
        assert_eq!(manifest.version, 4);
        assert!(manifest.wal_frames.is_empty());
        assert_eq!(manifest.block_slab_ids, [super::super::HOT_BLOCK_SLAB_ID]);
        let mut capture = super::super::decode_index_bytes(crate::engine::installable_image(&manifest).unwrap()).unwrap();
        assert!(capture.wal_resident_blocks.is_empty());
        let entries = collect_live_block_entries(&capture);
        assert_eq!(entries.len(), 1);
        let marker = &entries[0];
        assert!(marker.deleted && marker.filed_bucket() == Some(owner));
        {
            let mut node = capture.bucket_index.bucket_map.get_mut(&owner).unwrap();
            let row = node.tombstone_element_at(&marker.address).unwrap();
            assert_eq!(row.element_name.as_ref(), "member");
            node.forget_tombstone_element(&marker.object_key, marker.kind, "member");
            node.record_tombstone_element(&marker.object_key, StoredModelKind::Set, "member", &marker.address);
        }
        let invalid = engine.verify_committed_dump_wal(&manifest, &capture, &entries)
            .err().expect("the address cannot attest a different deleted model");
        assert_eq!(invalid.code, "slot_dump_invalid_wal_binding");
        {
            let mut node = capture.bucket_index.bucket_map.get_mut(&owner).unwrap();
            node.forget_tombstone_element(&marker.object_key, StoredModelKind::Set, "member");
            let shifted = crate::block_store::ElementEntry::try_from_parts(
                marker.address.block_slab_id(), marker.address.offset() + 1,
                marker.address.length(), None, None,
            ).unwrap();
            node.record_tombstone_element(&marker.object_key, marker.kind, "member", &shifted);
        }
        assert!(engine.verify_committed_dump_wal(&manifest, &capture, &entries).is_err());
        let foreign_root = tempfile::tempdir().unwrap();
        let foreign = engine_at(foreign_root.path());
        foreign.load_shard(1);
        assert_eq!(foreign.wal_store.stats_without_rescan(1).last_sequence, 0);
        foreign.validate_bucket_dump_manifest(&manifest).unwrap();
        foreign.install_bucket_dump_manifest(&manifest).unwrap();
        assert_eq!(foreign.persisted_images_wal_floor(1).unwrap(), None);
        drop(foreign);
        let reopened = engine_at(foreign_root.path());
        reopened.load_shard(1);
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1, command: Command::HashGet { key: "gone".into(), field: "member".into() },
        });
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Bytes { value: None });
        let (capture, _) = reopened.capture_shard_checkpoint(1).unwrap();
        assert!(collect_live_block_entries(&capture).iter().any(|entry| {
            entry.object_key.as_ref() == "gone" && entry.deleted
                && super::super::live_block_key(&entry.address) == super::super::live_block_key(&marker.address)
        }));

        assert!(engine.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringSet { key: "stays".into(), value: vec![4; 64] },
        }).status.ok);
        let mut mixed = engine.create_bucket_dump_manifest(1, []).unwrap();
        assert_eq!(mixed.version, 4);
        assert_eq!(mixed.wal_frames.len(), 1, "only the live String frame is required");
        reopened.validate_bucket_dump_manifest(&mixed).unwrap_err();
        let state = super::super::decode_index_bytes(&mixed.index_bytes).unwrap();
        let entries = collect_live_block_entries(&state);
        mixed.wal_frames.clear();
        assert!(engine.verify_committed_dump_wal(&mixed, &state, &entries).is_err());
    }

    #[test]
    fn resident_list_push_files_one_outcome_and_old_identical_pairs_still_bind() {
        let root = tempfile::tempdir().unwrap();
        let engine = engine_at(root.path());
        engine.load_shard(1);
        assert!(engine.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        for (member, left) in [(vec![1, 0], false), (vec![0xff, 0], true)] {
            assert!(engine.execute(ExecuteRequest {
                shard_id: 1, command: Command::ListPush { key: "list".into(), member, left },
            }).status.ok);
        }
        let (records, truncated) = engine.wal_store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
        assert!(!truncated);
        assert_eq!(records.len(), 2);
        for (_, record) in &records {
            assert_eq!(record.outcomes.len(), 1);
            assert_eq!(record.staged_blocks.len(), 1);
        }
        let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
        assert_eq!(manifest.version, 4);
        assert_eq!(manifest.wal_frames.len(), 2);
        engine.install_bucket_dump_manifest(&manifest).unwrap();
        drop(engine);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        let result = reopened.execute(ExecuteRequest {
            shard_id: 1, command: Command::ListRange { key: "list".into(), start: 0, stop: -1 },
        });
        assert!(result.status.ok);
        assert_eq!(result.response, CommandResponse::Members { members: vec![vec![0xff, 0], vec![1, 0]] });

        let old = &records[0].1;
        let item = &old.outcomes[0];
        let (record, id) = reopened.wal_store.append_batch_as_one_record_at(
            1, vec![item.clone(), item.clone()], old.staged_blocks.clone(), true,
        ).unwrap();
        let mut snapshot = super::super::decode_index_bytes(crate::engine::installable_image(&manifest).unwrap()).unwrap();
        let placement = super::super::block_in_wal::wal_resident_key(item.object_id, item.element_name.as_deref());
        snapshot.wal_resident_blocks.get_mut(&placement).unwrap().sequence = record.sequence;
        snapshot.wal_resident_blocks.get_mut(&placement).unwrap().log_id = id;
        let entries = super::super::storage_bucket_internals::collect_live_block_entries(&snapshot);
        assert!(reopened.read_local_dump_wal_dependencies(
            1, record.sequence, &snapshot, &entries, None,
        ).unwrap().is_some());
        let mut conflicting = Vec::new();
        for change in 0..10 {
            let mut altered = item.clone();
            match change {
                0 => altered.ttl = Some(91),
                1 => altered.value = Some(vec![91]),
                2 => altered.element_name = Some("8000000000000010".into()),
                3 => altered.routing_bucket ^= 1,
                4 => altered.object_id ^= 1,
                5 => altered.deleted = true,
                6 => altered.meta = true,
                7 => altered.kind = "hash".into(),
                8 => altered.object_key = "different".into(),
                9 => {
                    let address = altered.resolved_address().unwrap();
                    altered.address = Some(crate::block_store::ElementEntry::try_from_parts(
                        address.block_slab_id(), address.offset() + 1, address.length(), None, None,
                    ).unwrap());
                }
                _ => unreachable!(),
            }
            conflicting.push(vec![item.clone(), altered]);
        }
        conflicting.push(vec![item.clone(), item.clone(), item.clone()]);
        for (case, outcomes) in conflicting.into_iter().enumerate() {
            let (record, id) = reopened.wal_store.append_batch_as_one_record_at(
                1, outcomes, old.staged_blocks.clone(), true,
            ).unwrap();
            snapshot.wal_resident_blocks.get_mut(&placement).unwrap().sequence = record.sequence;
            snapshot.wal_resident_blocks.get_mut(&placement).unwrap().log_id = id;
            assert!(reopened.read_local_dump_wal_dependencies(
                1, record.sequence, &snapshot, &entries, None,
            ).is_err(), "invalid List outcome case {case} must be rejected");
        }
    }

    #[test]
    fn periodic_string_dump_keeps_the_wal_frame_and_writes_no_pages() {
        use crate::data_node::{DataNodeRuntime, DataNodeRuntimeOptions, StorageManagerOptions};
        let root = tempfile::tempdir().unwrap();
        let source = engine_at(root.path());
        source.load_shard(1);
        assert!(source.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        assert!(source.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringSet { key: "selected".into(), value: vec![7; 64] },
        }).status.ok);
        let resident = source.shards.read().unwrap().get(&1).unwrap().string_address("selected").unwrap();
        assert!(crate::wal_record::is_wal_resident(resident.block_slab_id()));
        let runtime = DataNodeRuntime::new(source.clone(), DataNodeRuntimeOptions {
            worker_threads: 1, max_queue_depth: 4, max_background_queue_depth: 2,
        });
        let mut options = StorageManagerOptions::default();
        options.dirty_bucket_pressure = 1;
        options.max_dump_buckets_per_round = 1;
        options.min_undumped_wal_bytes = 0;
        options.min_undumped_wal_records = 0;
        let before = source.block_store.stats();
        let result = runtime.run_wal_reclaim_slot_for_test(1, &options);
        let after = source.block_store.stats();
        assert!(result.status.ok && result.wal_reclaim_round.dumped);
        assert_eq!(result.wal_reclaim_round.moved_blocks, 0);
        assert_eq!((before.writes, before.bytes_written), (after.writes, after.bytes_written));
        assert_eq!(source.shards.read().unwrap().get(&1).unwrap().string_address("selected"), Some(resident));
        let manifests = source.list_bucket_dump_manifests(1);
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].bucket_ids, [source.routing_bucket_for_key(1, "selected")]);
        assert_eq!(manifests[0].version, 4);
        assert_eq!(manifests[0].wal_frames.len(), 1);
        source.validate_bucket_dump_manifest(&manifests[0]).unwrap();
        let floor = source.wal_store.block_retention_floor(1);
        assert!(floor.is_some());
        for _ in 0..32 {
            let idle = runtime.run_wal_reclaim_slot_for_test(1, &options);
            assert!(idle.status.ok);
            assert!(!idle.wal_reclaim_round.dumped);
            assert_eq!(idle.wal_reclaim_round.moved_blocks, 0);
            assert!(idle.skipped_stages.iter().any(|stage| stage == "reclaim_wal_no_pressure"));
        }
        assert_eq!(source.list_bucket_dump_manifests(1), manifests);
        assert_eq!(source.wal_store.block_retention_floor(1), floor);
        let idle_after = source.block_store.stats();
        assert_eq!((after.writes, after.bytes_written), (idle_after.writes, idle_after.bytes_written));
        drop(runtime);
        drop(source);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        reopened.validate_bucket_dump_manifest(&manifests[0]).unwrap();
        let got = reopened.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: "selected".into() },
        });
        assert!(got.status.ok);
        assert_eq!(got.response, CommandResponse::Bytes { value: Some(vec![7; 64]) });
    }

    #[test]
    fn resident_create_refuses_unbound_capture_without_advancing_dump() {
        let root = tempfile::tempdir().unwrap();
        let source = engine_at(root.path());
        source.load_shard(1);
        assert!(source.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        let put = |field: &str, value| {
            assert!(source.execute(ExecuteRequest {
                shard_id: 1, command: Command::HashSet {
                    key: "selected".into(), field: field.into(), value: vec![value; 64],
                },
            }).status.ok);
        };
        put("base", 1);
        let owner = source.routing_bucket_for_key(1, "selected");
        let base = source.create_bucket_dump_manifest(1, [owner]).unwrap();
        assert_eq!(base.version, 4);
        put("pending", 2);
        let object = stable_block_object_id(1, "hash", "selected");
        let folded = block_in_wal::wal_resident_key(object, Some("pending"));
        let before_dirty = {
            let mut shards = source.shards.write().unwrap();
            let shard = shards.get_mut(&1).unwrap();
            let placement = shard.wal_resident_blocks.get_mut(&folded)
                .expect("test must corrupt a real resident placement");
            placement.log_id = base.wal_frames[0].log_id;
            shard.dirty_objects.bucket_counts().collect::<Vec<_>>()
        };
        assert!(!before_dirty.is_empty());
        let bytes = source.wal_store.undumped_len_since_dump(1);
        let objects = source.wal_store.undumped_objects_since_dump(1);
        assert!(bytes > 0);
        let before = source.list_bucket_dump_manifests(1);
        assert_eq!(before.as_slice(), std::slice::from_ref(&base));
        let served_index = std::fs::read(source.index_path(1)).ok();
        let error = source.create_bucket_dump_manifest(1, [owner]).unwrap_err();
        assert_eq!(error.code, "slot_dump_invalid_wal_binding");
        assert_eq!(source.wal_store.undumped_len_since_dump(1), bytes);
        assert_eq!(source.wal_store.undumped_objects_since_dump(1), objects);
        assert_eq!(source.list_bucket_dump_manifests(1), before);
        assert_eq!(std::fs::read(source.index_path(1)).ok(), served_index);
        assert_eq!(
            source.shards.read().unwrap().get(&1).unwrap()
                .dirty_objects.bucket_counts().collect::<Vec<_>>(), before_dirty,
        );
        source.validate_bucket_dump_manifest(&base).unwrap();
        let read = source.execute(ExecuteRequest {
            shard_id: 1, command: Command::HashGet {
                key: "selected".into(), field: "base".into(),
            },
        });
        assert!(read.status.ok);
        assert_eq!(read.response, CommandResponse::Bytes { value: Some(vec![1; 64]) });
    }

    #[test]
    fn resident_hash_frames_are_verified_without_registry_mutation() {
        let root = tempfile::tempdir().unwrap();
        let source = engine_at(root.path());
        source.load_shard(1);
        assert!(source.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        for (field, value) in [("old", 1), ("old", 2), ("second", 3)] {
            assert!(source.execute(ExecuteRequest { shard_id: 1, command: Command::HashSet {
                key: "selected".into(), field: field.into(), value: vec![value; 64],
            }}).status.ok);
        }
        assert!(source.execute(ExecuteRequest { shard_id: 1, command: Command::SeenCheck {
            key: "window".into(), member: b"member".to_vec(), window_ms: 3_600_000,
        }}).status.ok);
        assert!(source.execute(ExecuteRequest { shard_id: 1, command: Command::CommonExpire {
            key: "selected".into(), ttl_ms: 3_600_000,
        }}).status.ok);
        let owner = source.routing_bucket_for_key(1, "selected");
        let manifest = source.create_bucket_dump_manifest(1, [owner]).unwrap();
        assert_eq!(manifest.version, 4);
        assert_eq!(manifest.wal_frames.len(), 2);
        source.validate_bucket_dump_manifest(&manifest).unwrap();
        let state = crate::engine::decode_index_bytes(crate::engine::installable_image(&manifest).unwrap()).unwrap();
        let entries = crate::engine::storage_bucket_internals::collect_live_block_entries(&state);
        let mut commitments = std::collections::BTreeMap::new();
        for field in ["old", "second"] {
            let object = stable_block_object_id(1, "hash", "selected");
            let folded = block_in_wal::wal_resident_key(object, Some(field));
            let log_id = state.wal_resident_blocks[&folded].log_id;
            let frame = source.wal_store.read_frames_at_log_ids(1, &[log_id]).unwrap()
                .pop().flatten().expect("published resident frame");
            commitments.insert(log_id, sha256_hex_bytes(&frame));
        }
        assert!(!commitments.is_empty());
        let count = source.registration_count_for_test(1);
        let verified = source.read_local_dump_wal_dependencies(
            1, manifest.wal_sequence, &state, &entries, Some(&commitments),
        ).unwrap().expect("resident dump must hold a WAL lease");
        assert!(verified.floor > 0);
        assert_eq!(source.registration_count_for_test(1), count);
        drop(verified);
        let mut changed = commitments.clone();
        changed.values_mut().next().unwrap().push('0');
        assert!(source.read_local_dump_wal_dependencies(
            1, manifest.wal_sequence, &state, &entries, Some(&changed),
        ).is_err());
        let foreign_root = tempfile::tempdir().unwrap();
        let foreign = engine_at(foreign_root.path());
        assert!(foreign.read_local_dump_wal_dependencies(
            1, manifest.wal_sequence, &state, &entries, Some(&commitments),
        ).is_err());
        assert_eq!(foreign.registration_count_for_test(1), 0);
        let status = foreign.install_bucket_dump_manifest(&manifest).unwrap_err();
        assert_eq!(status.code, "slot_dump_invalid_wal_binding");
        assert!(foreign.list_bucket_dump_manifests(1).is_empty());
        assert!(!foreign.index_path(1).exists());
        foreign.load_shard(1);
        assert!(foreign.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        for field in ["old", "old", "second"] {
            assert!(foreign.execute(ExecuteRequest { shard_id: 1, command: Command::HashSet {
                key: "selected".into(), field: field.into(), value: vec![9; 64],
            }}).status.ok);
        }
        foreign.wal_store.flush(1).unwrap();
        for (&id, expected) in &commitments {
            let bytes = foreign.wal_store.read_frames_at_log_ids(1, &[id]).unwrap()
                .pop().flatten().expect("same-shaped foreign frame exists at the same log ID");
            crate::wal::decode_wal_line(&bytes).unwrap();
            assert_ne!(sha256_hex_bytes(&bytes), *expected);
        }
        let status = foreign.install_bucket_dump_manifest(&manifest).unwrap_err();
        assert_eq!(status.code, "slot_dump_invalid_wal_binding");
        assert!(foreign.list_bucket_dump_manifests(1).is_empty());
        let value = foreign.execute(ExecuteRequest { shard_id: 1, command: Command::HashGet {
            key: "selected".into(), field: "second".into(),
        }});
        assert_eq!(value.response, CommandResponse::Bytes { value: Some(vec![9; 64]) });
        let mut wrong = manifest.clone();
        wrong.wal_frames[0].sha256.replace_range(..1, "f");
        if wrong.wal_frames == manifest.wal_frames {
            wrong.wal_frames[0].sha256.replace_range(..1, "0");
        }
        wrong.dump_generation_id = crate::engine::bucket_dump_io::bucket_dump_generation_id(&wrong);
        wrong.checksum = crate::engine::bucket_dump_io::bucket_dump_manifest_checksum(&wrong).unwrap();
        assert_eq!(source.install_bucket_dump_manifest(&wrong).unwrap_err().code,
                   "slot_dump_invalid_wal_binding");
        source.install_bucket_dump_manifest(&manifest).unwrap();
        source.refresh_wal_retention_floor(1).unwrap();
        let anchor = crate::wal::DurableIndexAnchor::proven_durable_through(1, manifest.wal_sequence);
        let report = source.wal_store.gc_before_sequence(1, manifest.wal_sequence, &anchor).unwrap();
        assert!(report.records_removed > 0, "test must reclaim an eligible prefix");
        drop(source);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        for (field, value) in [("old", 2), ("second", 3)] {
            let response = reopened.execute(ExecuteRequest { shard_id: 1, command: Command::HashGet {
                key: "selected".into(), field: field.into(),
            }});
            assert!(response.status.ok);
            assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![value; 64]) });
        }
        {
            let shards = reopened.shards.read().unwrap();
            assert!(shards[&1].seen.contains_key("window"));
            assert!(shards[&1].expires_at_ms().contains_key("selected"));
        }
        let base = reopened.index_path(1);
        let original = std::fs::read(&base).unwrap();
        drop(reopened);
        let wal = root.path().join("indexes/wals/shard-1.wal.bin");
        let file = std::fs::OpenOptions::new().write(true).open(wal).unwrap();
        file.set_len(file.metadata().unwrap().len() / 2).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let torn = engine_at(root.path());
        let result = torn.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1, start_routing_bucket: 0, end_routing_bucket: u32::MAX,
            load_version: 0, readonly: false, table_name: String::new(),
            local_node_id: None, shard_uri: String::new(),
        });
        assert_eq!(result.status.code, "slot_dump_invalid_wal_binding");
        assert!(!torn.shards.read().unwrap().contains_key(&1));
        assert_eq!(std::fs::read(base).unwrap(), original);
    }

    #[test]
    fn committed_packed_hash_checks_each_field_and_reopens_exactly() {
        use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue};
        let root = tempfile::tempdir().unwrap();
        let source = engine_at(root.path());
        source.load_shard(1);
        assert!(source.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        let (response, completed) = source.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
            shard_id: 1, fields: vec!["a".into(), "b".into()],
            rows: vec![BatchHashMultiSetRow("packed".into(), vec![
                BatchHashMultiSetValue(vec![1; 64]), BatchHashMultiSetValue(vec![2; 64]),
            ])], ttl_ms: None,
        });
        assert!(response.status.ok);
        assert_eq!(completed, ["packed"]);
        let owner = source.routing_bucket_for_key(1, "packed");
        {
            let shards = source.shards.read().unwrap();
            let shard = &shards[&1];
            let fields = shard.bucket_index.hashes.get("packed").unwrap();
            let a = super::super::live_block_key(fields.get("a").unwrap());
            let b = super::super::live_block_key(fields.get("b").unwrap());
            assert_eq!(a, b, "the two fields must share a carried page");
            let object = super::super::stable_block_object_id(1, "hash", "packed");
            let address_key = super::super::block_in_wal::wal_resident_address_key(object, a);
            assert!(shard.wal_resident_blocks.contains_key(&address_key),
                    "the shared Hash writer must persist the address-folded placement");
        }
        let manifest = source.create_bucket_dump_manifest(1, [owner]).unwrap();
        assert_eq!(manifest.version, 4, "packed positive control must prove every field");
        assert_eq!(manifest.wal_frames.len(), 1);
        source.install_bucket_dump_manifest(&manifest).unwrap();
        drop(source);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        for (field, value) in [("a", 1), ("b", 2)] {
            let read = reopened.execute(ExecuteRequest {
                shard_id: 1, command: Command::HashGet { key: "packed".into(), field: field.into() },
            });
            assert!(read.status.ok);
            assert_eq!(read.response, CommandResponse::Bytes { value: Some(vec![value; 64]) });
        }
    }

    #[test]
    fn mixed_released_packed_dump_preserves_ttl_tombstones_and_encoded_inventory() {
        use crate::types::{BatchHashMultiSetRequest, BatchHashMultiSetRow, BatchHashMultiSetValue};
        let root = tempfile::tempdir().unwrap();
        let source = engine_at(root.path());
        source.load_shard(1);
        assert!(source.set_config(SetConfigRequest {
            shard_id: 1, config: Config { version: 2, async_storage: true, ..Config::default() },
        }).ok);
        for (key, value) in [("released-scalar", 8), ("deleted-scalar", 9)] {
            assert!(source.execute(ExecuteRequest {
                shard_id: 1, command: Command::StringSet { key: key.into(), value: vec![value; 64] },
            }).status.ok);
        }
        assert_eq!(source.materialize_synthetic_blocks(1), 2);
        assert!(source.execute(ExecuteRequest {
            shard_id: 1, command: Command::CommonDelete { key: "deleted-scalar".into() },
        }).status.ok);
        let (response, completed) = source.batch_execute_shared_hash_rows(BatchHashMultiSetRequest {
            shard_id: 1, fields: vec!["a".into(), "b".into()],
            rows: vec![BatchHashMultiSetRow("packed".into(), vec![
                BatchHashMultiSetValue(vec![1; 64]), BatchHashMultiSetValue(vec![2; 64]),
            ])], ttl_ms: Some(3_600_000),
        });
        assert!(response.status.ok);
        assert_eq!(completed, ["packed"]);
        let owner = source.routing_bucket_for_key(1, "packed");
        let released = source.routing_bucket_for_key(1, "released-scalar");
        // Publish the scalar bucket for release without covering the packed
        // owner whose separate generation will be installed below.
        let clean = source.create_bucket_dump_manifest(1, [released]).unwrap();
        source.clear_dumped_bucket_dirty_state(1, &clean);
        {
            let mut shards = source.shards.write().unwrap();
            let outcome = super::super::storage_bucket_internals::release_bucket_blocks(
                shards.get_mut(&1).unwrap(), &[released],
            );
            assert_eq!(outcome.released_buckets, [released]);
        }
        assert_ne!(owner, released);
        {
            let shards = source.shards.read().unwrap();
            let shard = &shards[&1];
            let fields = shard.bucket_index.hashes.get("packed").unwrap();
            let a = super::super::live_block_key(fields.get("a").unwrap());
            let b = super::super::live_block_key(fields.get("b").unwrap());
            assert_eq!(a, b, "the two fields must share a carried page");
            let object = super::super::stable_block_object_id(1, "hash", "packed");
            let address_key = super::super::block_in_wal::wal_resident_address_key(object, a);
            assert!(shard.wal_resident_blocks.contains_key(&address_key),
                    "the shared Hash writer must persist the address-folded placement");
        }
        assert!(source.shards.read().unwrap()[&1].bucket_index.released_buckets.contains(&released));
        let (capture, _) = source.capture_shard_checkpoint(1).unwrap();
        assert!(source.shards.read().unwrap()[&1].bucket_index.released_buckets.contains(&released));
        assert!(!capture.bucket_index.released_buckets.contains(&released));
        let captured_rows = super::super::storage_bucket_internals::collect_live_block_entries(&capture);
        assert!(captured_rows.iter().any(|row| row.object_key.as_ref() == "released-scalar"
            && row.filed_bucket() == Some(released) && !row.deleted));
        let bytes = super::super::serialize_index(&capture);
        // A historical unreachable registration may survive in serving state.
        // The durable image must exclude it without mutating the live registry.
        let unused = {
            let mut shards = source.shards.write().unwrap();
            let placements = &mut shards.get_mut(&1).unwrap().wal_resident_blocks;
            let unused = (0..).find(|key| !placements.contains_key(key)).unwrap();
            placements.insert(unused, super::super::state::WalResidentBlock {
                log_id: u64::MAX, sequence: 1,
            });
            unused
        };
        let manifest = source.create_bucket_dump_manifest(1, [owner]).unwrap();
        assert_eq!(crate::engine::installable_image(&manifest).unwrap(), bytes.as_slice(), "completed release image must encode identically");
        assert!(source.shards.read().unwrap()[&1].bucket_index.released_buckets.contains(&released));
        assert!(source.shards.read().unwrap()[&1].wal_resident_blocks.contains_key(&unused));
        let decoded = super::super::decode_index_bytes(crate::engine::installable_image(&manifest).unwrap()).unwrap();
        assert!(!decoded.wal_resident_blocks.contains_key(&unused));
        let entries = super::super::storage_bucket_internals::collect_live_block_entries(&decoded);
        let slabs = entries.iter().map(|entry| entry.address.block_slab_id())
            .collect::<BTreeSet<_>>();
        assert_eq!(manifest.block_slab_ids, slabs.into_iter().collect::<Vec<_>>());
        assert!(entries.iter().any(|row| row.object_key.as_ref() == "released-scalar"
            && row.filed_bucket() == Some(released)));
        assert_eq!(manifest.version, 4, "packed positive control must prove every field");
        assert_eq!(manifest.wal_frames.len(), 1);
        let frame = source.wal_store.read_frames_at_log_ids(1, &[manifest.wal_frames[0].log_id])
            .unwrap().pop().unwrap().unwrap();
        let record = crate::wal::decode_wal_line(&frame).unwrap();
        assert_eq!(record.staged_blocks.len(), 1);
        let carried = &record.staged_blocks[0];
        for field in ["a", "b"] {
            assert!(super::super::block_in_wal::block_has_exact_manifest_element(
                &record, carried, "hash", Some(field),
            ));
        }
        assert!(!super::super::block_in_wal::block_has_exact_manifest_element(
            &record, carried, "hash", Some("wrong-field"),
        ));
        source.install_bucket_dump_manifest(&manifest).unwrap();
        drop(source);
        let reopened = engine_at(root.path());
        reopened.load_shard(1);
        assert!(reopened.shards.read().unwrap()[&1].expires_at_ms().contains_key("packed"));
        for (key, value) in [("released-scalar", Some(vec![8; 64])), ("deleted-scalar", None)] {
            let read = reopened.execute(ExecuteRequest {
                shard_id: 1, command: Command::StringGet { key: key.into() },
            });
            assert!(read.status.ok);
            assert_eq!(read.response, CommandResponse::Bytes { value });
        }
        for (field, value) in [("a", 1), ("b", 2)] {
            let read = reopened.execute(ExecuteRequest {
                shard_id: 1, command: Command::HashGet { key: "packed".into(), field: field.into() },
            });
            assert!(read.status.ok);
            assert_eq!(read.response, CommandResponse::Bytes { value: Some(vec![value; 64]) });
        }
    }
}
