// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The base-only load folds DURABLE RELOCATION RECORDS, conditionally.
//!
//! A relocation record says where a block moved. The default recovery loads only the base image and
//! re-executes the WAL tail, so it cannot fold ordinary deltas; it folds these, and only where the
//! base still names the block at the old address. These tests are the reader: no compaction writes
//! such a record yet, so each test appends one itself, after writing the moved block to disk.

use super::*;
use crate::block_store::ElementEntry;
use crate::index_log::IndexItem;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: Config { version: 2, async_storage: false, ..Config::default() },
            })
            .ok
    );
    engine
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: value.to_vec() },
    });
    assert!(response.status.ok);
}

fn read(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: key.to_string() },
    });
    assert!(response.status.ok);
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response {other:?}"),
    }
}

fn address_of(engine: &TemporalEngine, key: &str) -> ElementEntry {
    let shards = engine.shards_write_marked();
    shards.get(&1).unwrap().string_address(key).expect("the key has a block")
}

/// The index item the write path would log for this string's current block.
fn string_item(engine: &TemporalEngine, key: &str) -> IndexItem {
    let shards = engine.shards_write_marked();
    let shard = shards.get(&1).unwrap();
    let mut items = crate::engine::collect_upsert_index_items(
        shard,
        1,
        &[("string", key.to_string(), None)],
        0,
        u32::MAX,
    );
    assert_eq!(items.len(), 1, "the string {key} names one block");
    items.remove(0)
}

/// Copy the block `from` names to a new address, as a compaction would, and make it durable. Returns
/// the item naming the NEW placement. Nothing is repointed in memory: the test appends the record.
fn moved_copy(engine: &TemporalEngine, from: &IndexItem) -> IndexItem {
    let old = from.entry.clone().expect("the item names a block");
    let bytes = engine.block_store.read(&old).expect("the old block reads");
    let new = engine
        .block_store
        .append_block_of_object(
            &bytes,
            Some(from.object_id),
            Some(from.routing_bucket),
            old.block_id().unwrap_or_default() as u32,
        )
        .expect("the copy is written");
    // THE ORDER THE WRITER MUST KEEP: the new block is durable before the record that names it.
    engine.block_store.sync_durable().expect("the new block is synced");
    let mut to = from.clone();
    to.entry = Some(new);
    to
}

fn relocate(engine: &TemporalEngine, from: &IndexItem, to: &IndexItem) {
    relocate_element(engine, from, to, "");
}

fn relocate_element(engine: &TemporalEngine, from: &IndexItem, to: &IndexItem, element: &str) {
    let sequence = engine
        .index_log_store
        .append_relocation(1, vec![from.clone()], vec![to.clone()], vec![element.to_string()])
        .expect("the relocation record is appended");
    assert!(sequence > 0, "the record was not written (bulk ingest or a disabled log)");
}

/// The index item the write path would log for one element of a container.
fn element_item(engine: &TemporalEngine, kind: &'static str, key: &str, element: &str) -> IndexItem {
    let shards = engine.shards_write_marked();
    let shard = shards.get(&1).unwrap();
    let mut items = crate::engine::collect_upsert_index_items(
        shard,
        1,
        &[(kind, key.to_string(), Some(element.to_string()))],
        0,
        u32::MAX,
    );
    assert_eq!(items.len(), 1, "the {kind} element {element} of {key} names one block");
    items.remove(0)
}

fn run_with_codec(protobuf: bool, body: impl FnOnce()) {
    crate::index_log::PROTOBUF_WRITER_ON_THIS_THREAD.with(|flag| flag.set(protobuf));
    body();
    crate::index_log::PROTOBUF_WRITER_ON_THIS_THREAD.with(|flag| flag.set(false));
}

/// (i) BASE + A DURABLE RELOCATION: the reload that loads only the base serves the key at its NEW
/// address, and the unrelocated key is untouched.
fn relocation_applies_when_the_base_names_the_old_block(protobuf: bool) {
    run_with_codec(protobuf, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        set(&engine, "moved", b"the moved value");
        set(&engine, "stayed", b"the other value");
        engine.flush_shard_index(1);
        let from = string_item(&engine, "moved");
        let stayed_before = address_of(&engine, "stayed");
        let to = moved_copy(&engine, &from);
        relocate(&engine, &from, &to);
        drop(engine);

        let reloaded = open(dir.path());
        assert_eq!(
            address_of(&reloaded, "moved"),
            to.entry.clone().unwrap(),
            "the base-only load did not apply the relocation record"
        );
        assert_ne!(address_of(&reloaded, "moved"), from.entry.clone().unwrap());
        assert_eq!(address_of(&reloaded, "stayed"), stayed_before, "an unrelated key moved");
        assert_eq!(read(&reloaded, "moved").as_deref(), Some(&b"the moved value"[..]));
        assert_eq!(read(&reloaded, "stayed").as_deref(), Some(&b"the other value"[..]));

        // The fold-everything path (the legacy recovery) reads the same record the same way.
        let folded = reloaded
            .load_index_checked(1, false)
            .expect("the fold load succeeds")
            .expect("an index exists");
        assert_eq!(
            folded.string_address("moved").unwrap(),
            to.entry.clone().unwrap(),
            "the full fold did not treat the record as a relocation"
        );
    });
}

#[test]
fn a_durable_relocation_is_applied_by_the_base_only_load() {
    relocation_applies_when_the_base_names_the_old_block(false);
}

#[test]
fn a_durable_relocation_is_applied_in_the_protobuf_codec_too() {
    relocation_applies_when_the_base_names_the_old_block(true);
}

fn checkpoint_reclaims_a_relocation_after_persisting_its_destination(protobuf: bool) {
    run_with_codec(protobuf, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        set(&engine, "moved", b"checkpointed value");
        engine.flush_shard_index(1);
        let from = string_item(&engine, "moved");
        let to = moved_copy(&engine, &from);
        relocate(&engine, &from, &to);
        let moves = engine.index_log_store.read_delta_records(1, 0).unwrap();
        let sequence = moves.iter().find(|record| !record.relocated_from.is_empty())
            .expect("the fixture wrote a move").sequence;
        assert_eq!(address_of(&engine, "moved"), from.entry.clone().unwrap());

        engine.dump_and_reclaim_index_logs_with_min_reclaimable(1, 0)
            .expect("the checkpoint was published");
        let image = decode_index_bytes(&std::fs::read(engine.index_path(1)).unwrap()).unwrap();
        assert_eq!(image.string_address("moved"), to.entry);
        assert!(image.captured_relocation_sequence >= sequence);
        assert!(engine.index_log_store.read_delta_records(1, 0).unwrap().iter()
            .all(|record| record.relocated_from.is_empty()));
        assert_eq!(read(&engine, "moved").as_deref(), Some(&b"checkpointed value"[..]));
        drop(engine);

        let reloaded = open(dir.path());
        assert_eq!(address_of(&reloaded, "moved"), to.entry.unwrap());
        assert_eq!(read(&reloaded, "moved").as_deref(), Some(&b"checkpointed value"[..]));
    });
}

#[test]
fn a_checkpoint_reclaims_a_move_only_after_persisting_its_destination() {
    checkpoint_reclaims_a_relocation_after_persisting_its_destination(false);
}

#[test]
fn a_protobuf_checkpoint_reclaims_a_move_only_after_persisting_its_destination() {
    checkpoint_reclaims_a_relocation_after_persisting_its_destination(true);
}

fn item_for_live_entry(entry: &LiveBlockEntry) -> IndexItem {
    IndexItem {
        kind: crate::index_log::IndexItemKind::Block,
        routing_bucket: block_routing_bucket(&entry.object_key, 0, u32::MAX),
        block_ref_key: String::new(),
        object_key: entry.object_key.clone(),
        model_id: entry.kind.as_str().to_string(),
        object_id: stable_block_object_id(1, entry.kind.as_str(), &entry.object_key),
        entry: Some(entry.address.clone()),
        deleted: false,
    }
}

#[test]
fn relocation_repoints_every_live_model_and_is_idempotent() {
    use storage_bucket_internals::{model_kind_registry_guards, ModelKind};
    let mut shard = model_kind_registry_guards::shard_holding_one_block_of_every_kind();
    let entries = collect_model_live_block_entries(&shard);
    let emitted = entries.iter().map(|entry| entry.kind.as_str()).collect::<BTreeSet<_>>();
    let declared = ModelKind::ALL.iter().map(|kind| kind.as_str()).collect::<BTreeSet<_>>();
    assert!(declared.len() >= 15);
    assert_eq!(emitted, declared);
    let from = entries.iter().map(item_for_live_entry).collect::<Vec<_>>();
    let mut to = from.clone();
    let destination = ElementEntry::from_parts(99, 256, 64, Some(0), None);
    for item in &mut to { item.entry = Some(destination.clone()); }
    let elements = entries.iter().map(|entry| entry.element_name.as_deref()
        .unwrap_or_default().to_string()).collect::<Vec<_>>();
    assert_eq!(fold_relocation_record(&mut shard, &from, &to, &elements, false), entries.len());
    let moved = collect_model_live_block_entries(&shard);
    assert_eq!(moved.len(), entries.len());
    assert!(moved.iter().all(|entry| entry.address == destination), "{moved:?}");
    assert_eq!(fold_relocation_record(&mut shard, &from, &to, &elements, false), 0);
}

#[test]
fn series_relocation_moves_shared_points_in_order_without_overwriting_newer_points() {
    use timestamped_series::TimestampedSeries;
    let mut shard = ShardState::default();
    let old = ElementEntry::from_parts(7, 128, 64, Some(0), None);
    let middle = ElementEntry::from_parts(8, 128, 64, Some(0), None);
    let destination = ElementEntry::from_parts(9, 128, 64, Some(0), None);
    let newer = ElementEntry::from_parts(10, 128, 64, Some(0), None);
    let mut points = (0..10_000).map(|at| (at, old.clone())).collect::<BTreeMap<_, _>>();
    points.insert(10_001, newer.clone());
    shard.features_mut().insert("sequence".into(), points.into());
    shard.features_mut().insert("unrelated".into(), BTreeMap::from([(1, old.clone())]).into());
    let mut first = IndexItem {
        kind: crate::index_log::IndexItemKind::Block,
        routing_bucket: 1, block_ref_key: String::new(), object_key: "sequence".into(),
        model_id: "feature".into(), object_id: 1, entry: Some(old.clone()), deleted: false,
    };
    let mut second = first.clone();
    second.entry = Some(middle.clone());
    let mut last = first.clone();
    last.entry = Some(destination.clone());
    let from = [first.clone(), second.clone()];
    let to = [second, last];
    assert_eq!(fold_relocation_record(&mut shard, &from, &to, &[String::new(), String::new()], false), 2);
    let series = shard.features().get("sequence").unwrap();
    for (at, address) in series.points() {
        assert_eq!(*address, if at == 10_001 { newer.clone() } else { destination.clone() });
    }
    assert_eq!(TimestampedSeries::get(shard.features().get("unrelated").unwrap(), 1), Some(&old));
    first.entry = Some(old);
    assert_eq!(fold_relocation_record(&mut shard, &[first], &to[..1], &[String::new()], false), 0);
}

/// (ii) THE KEY WAS WRITTEN AFTER THE BASE. The relocation names the block the increment made, which
/// the base never named, so applying it would put the incremented value at the new address and the
/// WAL replay would then increment it again. The record must be skipped and the key must end at the
/// value ONE increment gives. Dropping the identity check turns this red.
#[test]
fn a_relocation_of_a_block_the_base_never_named_is_skipped_and_the_write_applies_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    set(&engine, "counter", b"5");
    engine.flush_shard_index(1);
    let incremented = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringIncrBy { key: "counter".to_string(), increment: 1 },
    });
    assert!(incremented.status.ok);
    assert_eq!(read(&engine, "counter").as_deref(), Some(&b"6"[..]));
    // The compaction of the post-base block: from = the block the increment wrote.
    let from = string_item(&engine, "counter");
    let to = moved_copy(&engine, &from);
    relocate(&engine, &from, &to);
    drop(engine);

    let reloaded = open(dir.path());
    assert_ne!(
        address_of(&reloaded, "counter"),
        to.entry.clone().unwrap(),
        "the relocation of a block the base never named was applied"
    );
    assert_eq!(
        read(&reloaded, "counter").as_deref(),
        Some(&b"6"[..]),
        "the increment applied more or fewer than once"
    );
}

/// (iv) A CHAIN a -> b -> c: the second record's source is the state the first left, so both apply
/// and the key ends at c.
#[test]
fn a_chain_of_relocations_is_applied_in_log_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    set(&engine, "chained", b"chain value");
    engine.flush_shard_index(1);
    let a = string_item(&engine, "chained");
    let b = moved_copy(&engine, &a);
    relocate(&engine, &a, &b);
    let c = moved_copy(&engine, &b);
    relocate(&engine, &b, &c);
    drop(engine);

    let reloaded = open(dir.path());
    assert_eq!(address_of(&reloaded, "chained"), c.entry.clone().unwrap(), "the chain stopped early");
    assert_eq!(read(&reloaded, "chained").as_deref(), Some(&b"chain value"[..]));
}

/// THE CONTAINER KINDS: a hash field and a set member. Their elements live ONLY in the durable model
/// maps at load (the derived view is empty and the persisted map is filtered against the live block
/// set), so a relocation that repointed the bucket index alone would leave the persisted element at an
/// address the index no longer holds and the load would DROP it. The reload must serve the elements
/// at their new addresses and still read them.
#[test]
fn a_relocated_container_element_survives_the_reload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for (field, value) in [("moved", &b"moved field"[..]), ("kept", &b"kept field"[..])] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "h".into(),
                        field: field.into(),
                        value: value.to_vec(),
                    },
                })
                .status
                .ok
        );
    }
    engine.flush_shard_index(1);
    let from = element_item(&engine, "hash", "h", "moved");
    let to = moved_copy(&engine, &from);
    relocate_element(&engine, &from, &to, "moved");
    drop(engine);

    let reloaded = open(dir.path());
    let field = |name: &str| {
        let response = reloaded.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGet { key: "h".into(), field: name.into() },
        });
        match response.response {
            CommandResponse::Bytes { value } => value,
            other => panic!("unexpected response {other:?}"),
        }
    };
    assert_eq!(field("moved").as_deref(), Some(&b"moved field"[..]), "the relocated field was lost");
    assert_eq!(field("kept").as_deref(), Some(&b"kept field"[..]), "an unrelocated field was lost");
    let shards = reloaded.shards_write_marked();
    let address = shards
        .get(&1)
        .unwrap()
        .bucket_index
        .hashes
        .get("h")
        .and_then(|fields| fields.get("moved"))
        .cloned()
        .expect("the field is in the map");
    assert_eq!(address, to.entry.clone().unwrap(), "the field was not repointed");
}

/// A relocation whose OLD address appears in no base is applied to nothing: a record written before
/// a later base, or by a node that never had the key, cannot create a block.
#[test]
fn a_relocation_of_a_block_no_base_names_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    set(&engine, "present", b"present value");
    engine.flush_shard_index(1);
    // Build an item for a key that is not in the base by writing it afterwards.
    set(&engine, "latecomer", b"late value");
    let from = string_item(&engine, "latecomer");
    let to = moved_copy(&engine, &from);
    relocate(&engine, &from, &to);
    drop(engine);

    let reloaded = open(dir.path());
    let to_address = to.entry.clone().unwrap();
    let names_the_new_block = {
        let shards = reloaded.shards_write_marked();
        crate::engine::collect_live_block_entries(shards.get(&1).unwrap())
            .into_iter()
            .any(|entry| entry.address.block_slab_id() == to_address.block_slab_id()
                && entry.address.offset() == to_address.offset())
    };
    assert!(!names_the_new_block, "a relocation created a block the base never named");
    assert_eq!(read(&reloaded, "present").as_deref(), Some(&b"present value"[..]));
}

/// A RELOCATION RECORD IS NEVER REFLECTED BY A BASE: it carries no WAL anchor, so the post-dump sweep
/// that drops records a base already covers cannot drop it, however high the base's anchor is.
#[test]
fn a_relocation_record_is_never_reflected_by_any_base_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    set(&engine, "k", b"v");
    engine.flush_shard_index(1);
    let from = string_item(&engine, "k");
    let to = moved_copy(&engine, &from);
    relocate(&engine, &from, &to);
    let records = engine.index_log_store.read_delta_records(1, 0).expect("the log reads");
    let relocation = records
        .iter()
        .find(|record| !record.relocated_from.is_empty())
        .expect("the relocation record is in the log");
    assert_eq!(relocation.relocated_from.len(), relocation.items.len());
    assert!(relocation.applied_wal_sequence.is_none(), "the record carries a WAL anchor");
    for base_anchor in [1, 1_000, u64::MAX - 1] {
        assert!(
            !crate::index_log::delta_record_is_reflected_by(relocation, base_anchor),
            "a base anchored at {base_anchor} reflects the relocation record"
        );
    }
}
