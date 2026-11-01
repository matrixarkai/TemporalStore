// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The three claims `index_log_wire` exists to hold: the round trip is exact, a reader built from
//! the plain schema reads the bytes, and every field the live record adds is load-bearing.
#![allow(clippy::all)]

use super::*;
use crate::index_log::{encode_index_payload, INDEX_LOG_SHAPE_DELTA};

fn slab_entry(slab: u64, offset: u64, length: u64, block: Option<u64>) -> ElementEntry {
    ElementEntry::from_parts(slab, offset, length, block, block)
}

#[allow(clippy::too_many_arguments)]
fn item(
    kind: IndexItemKind,
    routing_bucket: u32,
    key: &str,
    model: &str,
    object_id: u64,
    entry: Option<ElementEntry>,
    deleted: bool,
    handle: &str,
) -> IndexItem {
    IndexItem {
        kind,
        routing_bucket,
        block_ref_key: handle.to_string(),
        object_key: std::sync::Arc::from(key),
        model_id: model.to_string(),
        object_id,
        entry,
        deleted,
    }
}

/// Every shape of item the log carries, one of each.
fn corpus_items() -> Vec<IndexItem> {
    let no_generation =
        ElementEntry::try_from_wire_parts(4, 8192, 77, Some(2), false).expect("entry");
    vec![
        // An ordinary slab block, strips applied: no handle, no object id.
        item(IndexItemKind::Block, 17, "user:42", "hash", 0, Some(slab_entry(7, 4096, 1234, Some(3))), false, ""),
        // Log-resident: no block id, so the address is a log position.
        item(IndexItemKind::Block, 0, "log:resident", "string", 0, Some(slab_entry(9, 77, 500, None)), false, ""),
        // A tombstone with no entry at all, of the object kind.
        item(IndexItemKind::Object, 1_000, "gone", "zset", 0xDEAD_BEEF_CAFE_F00D, None, true, ""),
        // An object id wider than the schema's 32 bits.
        item(IndexItemKind::Block, 5, "wide", "list", 0x1_0000_0001, Some(slab_entry(1, 1, 1, Some(0))), false, "123456"),
        // A model the table does not list.
        item(IndexItemKind::Meta, 6, "odd", "future_model", 7, Some(slab_entry(2, 2, 2, Some(1))), false, "a:b:c"),
        // An entry that carries no generation.
        item(IndexItemKind::Block, 8, "ünï:cødé", "set", 3, Some(no_generation), false, ""),
        // The widest address word and a deleted flag.
        item(
            IndexItemKind::Block,
            u32::MAX,
            "",
            "context_node",
            u64::MAX,
            Some(slab_entry(
                u64::from(u32::MAX),
                u64::from(u32::MAX),
                u64::from(u32::MAX),
                Some(0xFFFF),
            )),
            true,
            "",
        ),
    ]
}

fn catalog() -> Vec<SlabCatalogEntry> {
    let entry = |id: u64, state, created| SlabCatalogEntry {
        block_slab_id: id,
        state,
        physical_bytes: id * 1_000 + 1,
        logical_bytes: id * 10,
        created_unix_ms: created,
        updated_unix_ms: Some(id + 5),
        first_block_id: if id % 2 == 0 { Some(id) } else { None },
        last_block_id: Some(id + 100),
        version: id + 9,
    };
    vec![
        entry(0, SlabCatalogState::Active, Some(1)),
        entry(5, SlabCatalogState::Sealed, None),
        entry(6, SlabCatalogState::DelayedDestroy, Some(0)),
        entry(7, SlabCatalogState::Purged, Some(7)),
        // Two ids that agree in their low 32 bits: the map key collides, and the overflow list has
        // to carry the second.
        entry(0x1_0000_0000 + 5, SlabCatalogState::Active, Some(3)),
    ]
}

fn record(items: Vec<IndexItem>) -> IndexDeltaRecord {
    let mut slabs = catalog();
    slabs.sort_by_key(|entry| entry.block_slab_id);
    IndexDeltaRecord {
        shard_id: 3,
        sequence: 41,
        items,
        meta: Some(MetaItem {
            version: 1,
            start_wal_sequence: 900,
            timestamp_ms: 1_700_000_000_123,
            slabs,
            slab_version: 900,
        }),
        applied_wal_sequence: Some(0),
        key_states: vec![serde_json::json!({"ttl": [1, 2, 3]}), serde_json::json!("x")],
        upsert: true,
        shared_object_key: Some(String::new()),
    }
}

fn round_trip(record: &IndexDeltaRecord) -> IndexDeltaRecord {
    decode_delta_record(&encode_delta_record(record).expect("encode")).expect("decode")
}

/// CLAIM 1: THE ROUND TRIP IS EXACT, field for field, for every shape.
#[test]
fn the_delta_record_survives_the_protobuf_schema_exactly() {
    let original = record(corpus_items());
    assert_eq!(round_trip(&original), original);
    // Each item alone, so a failure names the shape that broke.
    for (position, one) in corpus_items().into_iter().enumerate() {
        let alone = IndexDeltaRecord {
            items: vec![one],
            meta: None,
            key_states: Vec::new(),
            applied_wal_sequence: None,
            shared_object_key: None,
            upsert: false,
            ..original.clone()
        };
        assert_eq!(round_trip(&alone), alone, "item {position} did not round-trip");
    }
    // The presence distinctions the schema's scalars cannot make by themselves.
    for wal in [None, Some(0), Some(1), Some(u64::MAX)] {
        for shared in [None, Some(String::new()), Some("k".to_string())] {
            let probe = IndexDeltaRecord {
                applied_wal_sequence: wal,
                shared_object_key: shared.clone(),
                items: Vec::new(),
                meta: None,
                key_states: Vec::new(),
                ..original.clone()
            };
            assert_eq!(round_trip(&probe), probe, "wal {wal:?} shared {shared:?}");
        }
    }
}

/// CLAIM 2: A READER BUILT FROM THE PLAIN SCHEMA READS OUR BYTES, with the schema's meaning.
#[test]
fn the_plain_schema_reads_the_bytes_the_live_record_writes() {
    use crate::index_log_record as plain;
    let original = record(corpus_items());
    let bytes = encode_delta_record(&original).expect("encode");
    let seen = plain::IndexLogRecord::decode(bytes.as_slice()).expect("the plain schema decodes");
    assert_eq!(seen.version, 1);
    assert_eq!(seen.sequence, original.sequence);
    assert_eq!(seen.wal_sequence, original.applied_wal_sequence.unwrap());
    assert_eq!(seen.items.len(), original.items.len());
    for (theirs, ours) in seen.items.iter().zip(&original.items) {
        assert_eq!(theirs.deleted, ours.deleted);
        assert_eq!(theirs.object_id, ours.object_id as u32, "the low 32 bits, as it can hold");
        if let Some(entry) = &ours.entry {
            assert_eq!(theirs.address, entry.address_word());
            assert_eq!(u64::from(theirs.size), entry.length());
            assert_eq!(theirs.in_wal, entry.log_resident());
            assert_eq!(u64::from(theirs.block_id), entry.block_id().unwrap_or_default());
        }
    }
    let meta = seen.meta_item.expect("the meta item");
    assert_eq!(meta.start_wal_id, 900);
    assert_eq!(meta.slab_version, 900);
    let fold_state = |state: SlabCatalogState| match state {
        SlabCatalogState::Active => 1,
        SlabCatalogState::Sealed => 2,
        _ => 4,
    };
    for entry in catalog()
        .into_iter()
        .filter(|entry| entry.block_slab_id <= u64::from(u32::MAX) && entry.block_slab_id != 5)
    {
        let info = meta.slabs.get(&(entry.block_slab_id as u32)).expect("slab in the map");
        assert_eq!(info.total_bytes, entry.physical_bytes);
        assert_eq!(info.state, fold_state(entry.state));
        assert_eq!(info.version, entry.version);
    }
}

/// And the other direction: bytes a writer of the plain schema produces are read here.
#[test]
fn bytes_from_a_writer_of_the_plain_schema_are_read_here() {
    use crate::index_log_record as plain;
    let written = plain::IndexLogRecord {
        version: 1,
        routing_bucket: 0,
        items: vec![plain::IndexItem {
            object_id: 12,
            block_id: 3,
            address: (7u64 << 32) | 4096,
            size: 1234,
            in_wal: false,
            deleted: false,
            model_id: 0,
        }],
        meta_item: None,
        object_items: Vec::new(),
        sequence: 9,
        wal_sequence: 5,
        timestamp_ms: 0,
    };
    let read = decode_delta_record(&written.encode_to_vec()).expect("read");
    assert_eq!(read.sequence, 9);
    assert_eq!(read.applied_wal_sequence, Some(5));
    let first = &read.items[0];
    assert_eq!(first.object_id, 12);
    let entry = first.entry.as_ref().expect("an entry");
    assert_eq!(entry.block_slab_id(), 7);
    assert_eq!(entry.offset(), 4096);
    assert_eq!(entry.length(), 1234);
    assert_eq!(entry.block_id(), Some(3));
}

/// CLAIM 3: EACH FIELD THIS MODULE ADDS IS LOAD-BEARING. Unwriting it must change the answer --
/// otherwise the field is decoration and the round trip was passing without it.
#[test]
fn every_field_the_live_record_adds_is_load_bearing() {
    let original = record(corpus_items());
    let wire = ProtoIndexLog::from_record(&original).expect("wire");
    let decoded_with = |mutate: &dyn Fn(&mut ProtoIndexLog)| {
        let mut changed = wire.clone();
        mutate(&mut changed);
        changed.into_record()
    };
    let mutations: Vec<(&str, Box<dyn Fn(&mut ProtoIndexLog)>)> = vec![
        ("object_id_wide", Box::new(|w| w.items.iter_mut().for_each(|i| i.object_id_wide = 0))),
        ("routing_bucket", Box::new(|w| w.items.iter_mut().for_each(|i| i.routing_bucket = 0))),
        ("object_key", Box::new(|w| w.items.iter_mut().for_each(|i| i.object_key.clear()))),
        ("block_ref_key", Box::new(|w| w.items.iter_mut().for_each(|i| i.block_ref_key.clear()))),
        ("kind", Box::new(|w| w.items.iter_mut().for_each(|i| i.kind = 0))),
        ("no_entry", Box::new(|w| w.items.iter_mut().for_each(|i| i.no_entry = false))),
        (
            "without_generation",
            Box::new(|w| w.items.iter_mut().for_each(|i| i.without_generation = false)),
        ),
        ("model_name", Box::new(|w| w.items.iter_mut().for_each(|i| i.model_name.clear()))),
        ("shard_id", Box::new(|w| w.shard_id = 0)),
        ("upsert", Box::new(|w| w.upsert = false)),
        ("shared_object_key", Box::new(|w| w.shared_object_key = None)),
        ("key_states", Box::new(|w| w.key_states.clear())),
        ("wal_sequence presence", Box::new(|w| w.wal_sequence = None)),
        ("overflow_slabs", Box::new(|w| w.meta_item.as_mut().unwrap().overflow_slabs.clear())),
        (
            "purged",
            Box::new(|w| {
                for info in w.meta_item.as_mut().unwrap().slabs.values_mut() {
                    info.purged = false;
                }
            }),
        ),
        (
            "slab_id_wide",
            Box::new(|w| {
                for info in w.meta_item.as_mut().unwrap().overflow_slabs.iter_mut() {
                    info.slab_id_wide = 0;
                }
            }),
        ),
    ];
    for (name, mutate) in &mutations {
        let outcome = decoded_with(mutate.as_ref());
        assert_ne!(
            outcome.as_ref().ok(),
            Some(&original),
            "unwriting `{name}` changed nothing, so it is not carrying anything"
        );
    }
    // THE CONTROL FOR THE CONTROL: an unrelated mutation (the record-level timestamp, which no
    // producer sets) must NOT change the answer, or the assertion above could not tell a
    // load-bearing field from any other edit.
    let untouched = decoded_with(&|w| w.timestamp_ms = 99).expect("decode");
    assert_eq!(untouched, original);
}

/// WHAT THE SCHEMA COSTS, against the positional row, on a record shaped like a feature append:
/// eight items of ONE object, strips applied, key hoisted. Printed; the assertion is a ceiling, so
/// a regression is a failing test and not a surprise in a footprint report.
#[test]
fn what_the_schema_costs_against_the_positional_row() {
    let items = (0..8u64)
        .map(|index| {
            item(
                IndexItemKind::Block,
                300 + index as u32,
                "",
                "hash",
                0,
                Some(slab_entry(7, 4096 * (index + 1), 900 + index, Some(index))),
                false,
                "",
            )
        })
        .collect::<Vec<_>>();
    let record = IndexDeltaRecord {
        shard_id: 1,
        sequence: 1_000,
        items,
        meta: None,
        applied_wal_sequence: Some(5_000),
        key_states: Vec::new(),
        upsert: true,
        shared_object_key: Some("feature:series:42".to_string()),
    };
    let positional = encode_index_payload(&record, INDEX_LOG_SHAPE_DELTA).expect("encode").len();
    let protobuf = encode_delta_record(&record).expect("encode").len();
    println!("  eight-item delta: positional {positional} B, protobuf {protobuf} B");
    assert!(
        protobuf * 10 <= positional * 16,
        "the schema costs {protobuf} B against {positional} B positional, more than 1.6x"
    );
}
