// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Logical/TTL evidence on an owned capture. This does not authorize physical retirement.
use std::collections::BTreeSet;

use super::state::{DirtyFlavour, DirtyMark, RollupDomain, ShardState, SideTable, SideTableData, SideTableShape};
use super::storage_bucket_internals::StoredModelKind;
use super::timestamped_series::TimestampedSeries as _;

#[cfg(test)]
thread_local! {
    static VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn note_visit() {
    #[cfg(test)]
    VISITS.with(|count| count.set(count.get().saturating_add(1)));
}

struct Walk {
    eligible: BTreeSet<u32>,
    start: u32,
    end: u32,
}

impl Walk {
    fn key(&mut self, key: &str) {
        note_visit();
        self.eligible.remove(&super::hashing::block_routing_bucket(key, self.start, self.end));
    }

    fn entity(&mut self, collection: &str, entity_hash: u64) {
        note_visit();
        // The canonical emitter spells "{collection}:{entity_hash}". Hash the same fields
        // without formatting or allocating a key, including decimal (not binary) entity IDs.
        let mut hash = super::hashing::stable_object_hash_begin();
        super::hashing::stable_object_hash_update(&mut hash, collection.as_bytes());
        super::hashing::stable_object_hash_update(&mut hash, b":");
        super::hashing::stable_object_hash_update_u64_decimal(&mut hash, entity_hash);
        self.reject_hash(hash);
    }

    fn reject_hash(&mut self, hash: u64) {
        let count = super::hashing::routing_bucket_count(self.start, self.end);
        let bucket = if count == 0 { self.start } else { self.start + (hash % u64::from(count)) as u32 };
        self.eligible.remove(&bucket);
    }

    fn uuid(&mut self, composite: &str) {
        note_visit();
        // Both original key and UUID can contain the separator. Every possible boundary
        // includes the actual producer boundary; never choose just the first or last one.
        let bytes = composite.as_bytes();
        let mut hash = super::hashing::stable_object_hash_begin();
        let mut start = 0;
        let mut found = false;
        for (offset, byte) in bytes.iter().enumerate() {
            if *byte == 1 {
                super::hashing::stable_object_hash_update(&mut hash, &bytes[start..offset]);
                self.reject_hash(hash);
                if self.eligible.is_empty() { return; }
                // The previous separator belongs to every longer possible key.
                start = offset;
                found = true;
            }
        }
        if !found { self.eligible.clear(); }
    }

    fn dirty(&mut self, mark: &DirtyMark) {
        note_visit();
        let prefix: &[u8] = match mark.flavour {
            DirtyFlavour::Summary => b"ctx:dirty:",
            DirtyFlavour::Embedding => super::context::EMBEDDING_DIRTY_PREFIX.as_bytes(),
        };
        let mut hash = super::hashing::stable_object_hash_begin();
        super::hashing::stable_object_hash_update(&mut hash, prefix);
        super::hashing::stable_object_hash_update_u64_decimal(&mut hash, mark.tenant_hash);
        super::hashing::stable_object_hash_update(&mut hash, b":");
        super::hashing::stable_object_hash_update_u64_decimal(&mut hash, mark.node_hash);
        self.reject_hash(hash);
    }

    fn unknown(&mut self) {
        note_visit();
        self.eligible.clear();
    }
}

/// Return only candidates with no logical/TTL evidence in this exact capture.
///
/// One borrowed walk covers all candidates. Empty input touches no records. Rows count as
/// authority even when their series are empty or their blocks were released. Opaque metadata
/// refuses ALL candidates; it is never routed by hashing an unclassified composite key.
/// The caller still needs independent physical-root, generation and publication fences.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn selected_buckets_without_logical_authority(
    snapshot: &ShardState,
    candidates: &BTreeSet<u32>,
) -> BTreeSet<u32> {
    if candidates.is_empty() || !snapshot.routing_range_known {
        return BTreeSet::new();
    }
    let (start, end) = snapshot.routing_range();
    let mut walk = Walk { eligible: candidates.clone(), start, end };
    macro_rules! visit {
        ($e:expr) => {{
            $e;
            if walk.eligible.is_empty() { return walk.eligible; }
        }};
    }
    for (kind, objects) in &snapshot.bucket_index.objects {
        for (key, row) in objects {
            match kind {
                StoredModelKind::ContextEntity => {
                    // The collection row itself can answer record_exists_exact, while its
                    // elements are filed under distinct canonical entity keys.
                    visit!(walk.key(key));
                    for (entity, _) in row.points() {
                        visit!(walk.entity(key, entity));
                    }
                }
                // Containers live in their own maps, ContextNode in its address side table;
                // phantom carrier rows and retired kinds do not establish a supported route.
                StoredModelKind::Sequence | StoredModelKind::ContextEmbedding
                | StoredModelKind::Hash | StoredModelKind::Set | StoredModelKind::Zset
                | StoredModelKind::List | StoredModelKind::ContextNode => visit!(walk.unknown()),
                StoredModelKind::String | StoredModelKind::Feature | StoredModelKind::ControlState
                | StoredModelKind::ContextEvent
                | StoredModelKind::ContextIndex | StoredModelKind::ContextAudit
                | StoredModelKind::ContextChild | StoredModelKind::ContextSummary
                | StoredModelKind::ContextCompression => visit!(walk.key(key)),
            }
        }
    }
    for key in snapshot.bucket_index.hashes.keys() { visit!(walk.key(key)); }
    for key in snapshot.bucket_index.sets.keys() { visit!(walk.key(key)); }
    for key in snapshot.bucket_index.zsets.keys() { visit!(walk.key(key)); }
    for key in snapshot.bucket_index.lists.keys() { visit!(walk.key(key)); }
    for key in snapshot.seen.keys() { visit!(walk.key(key)); }
    for key in snapshot.buckets.keys() { visit!(walk.key(key)); }
    for key in snapshot.control_state_changes.keys() { visit!(walk.key(key)); }
    for key in snapshot.control_state_selection.keys() { visit!(walk.key(key)); }

    for (domain, data) in &snapshot.side_tables {
        let shape = match data {
            SideTableData::Addresses(_) => SideTableShape::Addresses,
            SideTableData::Counters(_) => SideTableShape::Counters,
            SideTableData::U64s(_) => SideTableShape::U64s,
            SideTableData::Rings(_) => SideTableShape::Rings,
        };
        let composite_uuid = match domain {
            SideTable::ContextNodes | SideTable::ControlStateCounters
            | SideTable::FeatureCounters | SideTable::Expiry | SideTable::RetryRings
            | SideTable::ContextCompressionWatermark => false,
            SideTable::ControlStateUuid => true,
        };
        let nonempty = match data {
            SideTableData::Addresses(table) => !table.is_empty(),
            SideTableData::Counters(table) => !table.is_empty(),
            SideTableData::U64s(table) => !table.is_empty(),
            SideTableData::Rings(table) => !table.is_empty(),
        };
        if shape != domain.shape() && nonempty { visit!(walk.unknown()); }
        macro_rules! side_key {
            ($key:expr) => {
                if composite_uuid { visit!(walk.uuid($key)); } else { visit!(walk.key($key)); }
            };
        }
        match data {
            SideTableData::Addresses(table) => { for key in table.keys() { side_key!(key); } }
            SideTableData::Counters(table) => { for key in table.keys() { side_key!(key); } }
            SideTableData::U64s(table) => { for key in table.keys() { side_key!(key); } }
            SideTableData::Rings(table) => { for key in table.keys() { side_key!(key); } }
        }
    }
    for key in snapshot.context_event_timeline.keys() { visit!(walk.key(key)); }
    for mark in snapshot.context_dirty_index.keys() { visit!(walk.dirty(mark)); }
    for (domain, table) in &snapshot.rollups {
        match domain {
            RollupDomain::ControlState | RollupDomain::Feature => {
                for key in table.keys() { visit!(walk.key(key)); }
            }
        }
    }
    // Block maps/refs/layouts, WAL placements and dirty/recency caches are separate physical
    // authority or derived bookkeeping, not additional logical record/TTL domains.
    walk.eligible
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::engine::timestamped_series::TimestampedSeriesWrite as _;
    use crate::types::{Command, ExecuteRequest};

    fn engine() -> (tempfile::TempDir, TemporalEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            4 << 20, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
        );
        assert!(engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1, table_name: "absence".into(), shard_uri: "local://absence/1".into(),
            start_routing_bucket: 0, end_routing_bucket: 63, readonly: false,
            load_version: 1, local_node_id: Some(1),
        }).status.ok);
        (dir, engine)
    }

    fn run(engine: &TemporalEngine, command: Command) {
        let result = engine.execute(ExecuteRequest { shard_id: 1, command });
        assert!(result.status.ok, "{:?}", result.status);
    }

    fn capture(engine: &TemporalEngine) -> ShardState {
        engine.shards.with_state(1, |state| state.unwrap().clone())
    }

    fn bucket(key: &str) -> u32 {
        super::super::hashing::block_routing_bucket(key, 0, 63)
    }

    fn key_at(prefix: &str, wanted: u32) -> String {
        (0..10_000).map(|index| format!("{prefix}-{index}"))
            .find(|key| bucket(key) == wanted).unwrap()
    }

    #[test]
    fn real_blockless_seen_bucket_and_control_neighbors_refuse_only_their_bucket() {
        let (_dir, engine) = engine();
        let seen = key_at("seen", 7);
        let quota = key_at("quota", 7);
        let control = key_at("control_state:cpc:counter", 7);
        run(&engine, Command::SeenCheck { key: seen.clone(), member: b"member".to_vec(), window_ms: 1000 });
        run(&engine, Command::BucketTake { key: quota.clone(), tokens: 1.0, capacity: 2.0, refill_per_sec: 0.0 });
        run(&engine, Command::ControlStateChangeAdd {
            key: control.clone(), timestamp_ms: 1, value: b"distinct".to_vec(), precision_ms: None, ttl_ms: None,
        });
        let mut snapshot = capture(&engine);
        assert!(snapshot.seen.contains_key(seen.as_str()));
        assert!(snapshot.buckets.contains_key(quota.as_str()));
        assert!(snapshot.control_state_changes.contains_key(control.as_str()));
        // Removing this derived backlog from the COPY must not hide the live metadata.
        snapshot.dirty_objects.clear();
        assert_eq!(selected_buckets_without_logical_authority(&snapshot, &BTreeSet::from([7, 13])),
            BTreeSet::from([13]));
    }

    #[test]
    fn real_deadline_remains_authority_without_a_model_or_known_key_list() {
        let (_dir, engine) = engine();
        let key = key_at("ttl-only", 19);
        run(&engine, Command::StringSetEx { key: key.clone(), value: b"value".to_vec(), ttl_ms: 60_000 });
        let mut snapshot = capture(&engine);
        assert!(snapshot.expires_at_ms().contains_key(key.as_str()));
        // Retain the real authoritative deadline while erasing the COPY's model projection.
        snapshot.bucket_index.objects.clear();
        snapshot.bucket_index.bucket_map.clear();
        snapshot.bucket_index.clear_object_blocks();
        snapshot.bucket_index.released_buckets.clear();
        snapshot.wal_resident_blocks.clear();
        snapshot.expiry_by_deadline.clear();
        snapshot.dirty_objects.clear();
        assert_eq!(selected_buckets_without_logical_authority(&snapshot, &BTreeSet::from([19, 23])),
            BTreeSet::from([23]));
    }

    #[test]
    fn entity_routing_uses_decimal_field_not_just_collection_prefix() {
        let (_dir, engine) = engine();
        let mut snapshot = capture(&engine);
        let collection = "ctx:entity:11:23";
        let entity = 42;
        let routed = bucket(&format!("{collection}:{entity}"));
        assert_ne!(routed, bucket(collection));
        snapshot.context_entities_mut().entry(collection.into()).or_default()
            .set(entity, crate::block_store::ElementEntry::default());
        assert!(!selected_buckets_without_logical_authority(&snapshot, &BTreeSet::from([routed])).contains(&routed));
    }

    #[test]
    fn malformed_composite_and_mismatched_shape_never_grant_absence() {
        let (_dir, engine) = engine();
        let mut snapshot = capture(&engine);
        snapshot.control_state_uuid_mut().insert("missing-separator".into(), 100);
        let selected = BTreeSet::from([3, 11, 47]);
        assert!(selected_buckets_without_logical_authority(&snapshot, &selected).is_empty());
        snapshot.side_tables.clear();
        snapshot.side_tables.insert(SideTable::FeatureCounters,
            SideTableData::U64s(std::collections::BTreeMap::from([("key".into(), 1)])));
        assert!(selected_buckets_without_logical_authority(&snapshot, &selected).is_empty());
    }

    #[test]
    fn real_uuid_producer_refuses_every_ambiguous_boundary_not_the_complete_composite() {
        let (_dir, engine) = engine();
        let key = "tenant\u{1}object\u{1}child";
        let uuid = "arbitrary\u{1}suffix\u{1}not-a-uuid";
        run(&engine, Command::ControlStateSetAndGetWithOptions {
            family: crate::types::ControlStateFamily::Counter, key: key.into(),
            timestamp_ms: 1000, amount: 1, start_ms: 0, end_ms: 2000,
            aggregator: "sum".into(), precision_ms: None, ttl_ms: None,
            uuid: Some(uuid.into()),
        });
        let mut snapshot = capture(&engine);
        let canonical = snapshot.control_state().keys().next().unwrap().to_string();
        assert!(canonical.contains(key));
        let composite = format!("{canonical}\u{1}{uuid}");
        assert!(snapshot.control_state_uuid().contains_key(composite.as_str()));
        let mut expected: BTreeSet<_> = (0..64).collect();
        for (offset, _) in composite.match_indices('\u{1}') {
            expected.remove(&bucket(&composite[..offset]));
        }
        assert!(!expected.is_empty());
        assert!(!expected.contains(&bucket(&canonical)));
        // Isolate the real ledger in the COPY so its primary counter cannot hide an error.
        snapshot.bucket_index.objects.clear();
        snapshot.rollups.clear();
        snapshot.side_tables.retain(|domain, _| *domain == SideTable::ControlStateUuid);
        VISITS.with(|count| count.set(0));
        assert_eq!(selected_buckets_without_logical_authority(&snapshot, &(0..64).collect()), expected);
        assert_eq!(VISITS.with(|count| count.replace(0)), 1);
    }

    #[test]
    fn real_dirty_commands_route_typed_fields_without_using_entry_payload_tenant() {
        let (_dir, engine) = engine();
        for (tenant_hash, node_hash) in [(1, 1), (9, 10), (u64::MAX, u64::MAX)] {
            for embedding in [false, true] {
                let command = if embedding {
                    Command::ContextMarkEmbeddingDirty { tenant_hash, node_hash,
                        event_time_ms: 1000, reason: 1, propagate_depth: 0, clear: false }
                } else {
                    Command::ContextMarkSummaryDirty { tenant_hash, node_hash,
                        event_time_ms: 1000, reason: 1, propagate_depth: 0 }
                };
                let result = engine.execute(ExecuteRequest { shard_id: 1, command });
                assert!(result.status.ok, "{:?}", result.status);
                let crate::types::CommandResponse::ContextObjectKey { object_key } = result.response
                    else { panic!("dirty command did not return its canonical key"); };
                let mark = if embedding { DirtyMark::embedding(tenant_hash, node_hash) }
                    else { DirtyMark::summary(tenant_hash, node_hash) };
                let mut snapshot = capture(&engine);
                snapshot.context_dirty_index.retain(|key, _| *key == mark);
                assert_eq!(snapshot.context_dirty_index.len(), 1);
                if !embedding { assert_eq!(snapshot.context_dirty_index[&mark].tenant_hash, 0); }
                let mut expected: BTreeSet<_> = (0..64).collect();
                expected.remove(&bucket(&object_key));
                assert_eq!(selected_buckets_without_logical_authority(&snapshot, &(0..64).collect()), expected);
            }
        }
    }

    #[test]
    fn event_auxiliary_keys_and_real_rollup_builder_keep_raw_object_routes() {
        let (_dir, engine) = engine();
        let empty = capture(&engine);
        let event_key = super::super::context::context_event_key(13, u64::MAX);
        for timeline in [false, true] {
            let mut snapshot = empty.clone();
            if timeline {
                snapshot.context_event_timeline.entry(event_key.as_str().into()).or_default().insert(1000, 7);
            } else {
                snapshot.context_compression_watermark_mut().insert(event_key.as_str().into(), 1000);
            }
            let mut expected: BTreeSet<_> = (0..64).collect();
            expected.remove(&bucket(&event_key));
            assert_eq!(selected_buckets_without_logical_authority(&snapshot, &(0..64).collect()), expected);
        }
        for domain in [RollupDomain::ControlState, RollupDomain::Feature] {
            let mut snapshot = empty.clone();
            let key = "raw-series\u{1}not-a-composite";
            let table = snapshot.rollups.entry(domain).or_default();
            assert_eq!(super::super::control_rollup::windowed_sum_series(table,
                &std::collections::BTreeMap::from([(1000, 3)]), key, 0, 2000), 3);
            let mut expected: BTreeSet<_> = (0..64).collect();
            expected.remove(&bucket(key));
            assert_eq!(selected_buckets_without_logical_authority(&snapshot, &(0..64).collect()), expected);
        }
    }

    #[test]
    fn zero_candidates_visit_nothing_and_candidate_count_does_not_multiply_walk() {
        let (_dir, engine) = engine();
        for index in 0..32 {
            run(&engine, Command::SeenCheck { key: format!("live-{index}"), member: b"member".to_vec(), window_ms: 1000 });
        }
        let snapshot = capture(&engine);
        VISITS.with(|count| count.set(0));
        assert!(selected_buckets_without_logical_authority(&snapshot, &BTreeSet::new()).is_empty());
        assert_eq!(VISITS.with(|count| count.replace(0)), 0);
        let occupied: BTreeSet<_> = snapshot.seen.keys().map(|key| bucket(key)).collect();
        let unoccupied = (0..64).find(|id| !occupied.contains(id)).unwrap();
        assert_eq!(selected_buckets_without_logical_authority(&snapshot, &BTreeSet::from([unoccupied])),
            BTreeSet::from([unoccupied]));
        let one = VISITS.with(|count| count.replace(0));
        selected_buckets_without_logical_authority(&snapshot, &(0..64).collect());
        let many = VISITS.with(|count| count.replace(0));
        assert_eq!(one, 32);
        assert_eq!(many, one);
    }

    #[test]
    fn first_owner_refuses_without_visiting_later_records() {
        let (_dir, engine) = engine();
        for index in 0..32 {
            run(&engine, Command::SeenCheck { key: format!("live-{index}"), member: b"member".to_vec(), window_ms: 1000 });
        }
        let snapshot = capture(&engine);
        let first = snapshot.seen.keys().next().unwrap();
        VISITS.with(|count| count.set(0));
        assert!(selected_buckets_without_logical_authority(&snapshot, &BTreeSet::from([bucket(first)])).is_empty());
        assert_eq!(VISITS.with(|count| count.replace(0)), 1);
    }

    #[test]
    fn opaque_domain_size_does_not_multiply_refusal_work() {
        let (_dir, engine) = engine();
        let mut snapshot = capture(&engine);
        for index in 0..10_000 {
            snapshot.control_state_uuid_mut().insert(format!("opaque-{index}").into(), 100);
        }
        VISITS.with(|count| count.set(0));
        assert!(selected_buckets_without_logical_authority(&snapshot, &(0..64).collect()).is_empty());
        assert_eq!(VISITS.with(|count| count.replace(0)), 1);
    }

    #[test]
    fn every_stored_domain_is_examined_even_without_a_block() {
        let (_dir, engine) = engine();
        let empty = capture(&engine);
        let key = "domain-owner";
        let owner = bucket(key);
        for &kind in StoredModelKind::ALL {
            let mut snapshot = empty.clone();
            snapshot.bucket_index.objects.entry(kind).or_default().insert(
                key.into(), super::super::object_record::ObjectSlot::default(),
            );
            assert!(selected_buckets_without_logical_authority(&snapshot, &BTreeSet::from([owner])).is_empty(),
                "domain {kind:?} must not disappear because it has no block ref");
        }
    }

    #[test]
    fn capture_schema_changes_require_an_explicit_authority_review() {
        fn fields<'a>(source: &'a str, name: &str) -> BTreeSet<&'a str> {
            let marker = format!("struct {name} {{");
            let body = source.split_once(marker.as_str()).unwrap().1.split_once("\n}").unwrap().0;
            body.lines().filter(|line| line.starts_with("    ") && !line.starts_with("        "))
                .map(str::trim).filter(|line| !line.starts_with('/') && !line.starts_with('#'))
                .filter_map(|line| line.split_once(':').map(|(name, _)| name.split_whitespace().last().unwrap()))
                .collect()
        }
        let source = include_str!("engine_storage_state.rs");
        let shard = "index_format_version wal_resident_blocks buckets_pending_flag_refresh
            live_user_bytes live_objects_with_logical_size durable_items expiry_by_deadline
            expiry_scan_positions side_tables seen buckets object_block_layouts control_state_changes
            control_state_selection rollups control_coalesce_persist control_state_max_size
            control_distinct_sketch set_listing_from_map feature_max_age_ms context_event_timeline
            context_dirty_index bucket_index applied_wal_sequence file_restore_frontier
            captured_relocation_sequence wal_apply_discontinuous bucket_recency dirty_objects
            index_flush_countdown publish_index_flush_countdown promote_scan_done evict_sampler
            routing_range_start routing_range_end routing_range_known shard_id shard_id_known source_owner_range";
        let core = "bucket_map objects block_slab_live object_block_lookup kind_pool
            object_element_name_block_refs released_buckets bucket_owners hashes sets zsets lists";
        assert_eq!(fields(source, "ShardState"), shard.split_whitespace().collect::<BTreeSet<_>>());
        assert_eq!(fields(source, "CoreIndex"), core.split_whitespace().collect::<BTreeSet<_>>());
    }
}
