// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE READER OF A SEGMENTED STRING, DRIVEN AGAINST A CHAIN BUILT BY HAND.
//!
//! Nothing writes a segment yet, so these tests file the blocks and the slot's owned array
//! themselves, which is exactly the state the writer will leave: a base block at the object's
//! address, segment `n` as another block of the same object stamped with ordinal `n`, and the
//! slot's owned array listing a length card at 0 and each segment at its ordinal.
use super::string_chain::{read_string_assembled, StringChainError};
use super::*;

fn engine_in(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1024,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

/// File `segments` as blocks 1.. of `key`'s object and list them, with a length card for a base of
/// `card_len` bytes, in the slot's owned array.
fn file_chain(engine: &TemporalEngine, key: &str, card_len: u64, segments: &[&[u8]]) {
    // A segment is its OWN object id (see `SEGMENT_KIND`), never the base's.
    let object_id = crate::engine::hashing::stable_block_object_id(
        1,
        super::string_chain::SEGMENT_KIND,
        key,
    );
    let mut owned = vec![(
        0u64,
        crate::block_store::ElementEntry::from_parts(0, 0, card_len, None, None),
    )];
    let (start, end) = engine
        .shards
        .read()
        .expect("engine lock poisoned")
        .get(&1)
        .expect("shard loaded")
        .routing_range();
    let bucket = crate::engine::hashing::block_routing_bucket(key, start, end);
    for (index, bytes) in segments.iter().enumerate() {
        let ordinal = index as u32 + 1;
        let entry = append_value_of_object(
            &engine.cache,
            &engine.block_store,
            1,
            bytes,
            Some(object_id),
            None,
            Some(bucket),
            false,
            ordinal,
        )
        .expect("segment block written");
        owned.push((u64::from(ordinal), entry));
    }
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard loaded");
    shard
        .strings_mut()
        .get_mut(key)
        .expect("the base was set first")
        .set_owned_elements(
            super::object_record::OwnedElementTenancy::DurableOnTheWire {
                kind: super::storage_bucket_internals::StoredModelKind::String,
            },
            owned,
        );
}

fn read(engine: &TemporalEngine, key: &str) -> Result<Option<Vec<u8>>, StringChainError> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard loaded");
    read_string_assembled(&engine.cache, &engine.block_store, 1, shard, key)
}

#[test]
fn a_key_with_no_segments_reads_exactly_as_it_always_has() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "plain", b"just one block");
    assert_eq!(read(&engine, "plain"), Ok(Some(b"just one block".to_vec())));
    assert_eq!(read(&engine, "absent"), Ok(None));
}

#[test]
fn a_base_and_its_segments_read_back_as_one_value_in_ordinal_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "k", b"base-");
    file_chain(&engine, "k", 5, &[b"one-", b"two-", b"three"]);
    // POSITIVE CONTROL: the slot really lists the segments, or this would pass over a plain key.
    {
        let shards = engine.shards.read().unwrap();
        let slot = shards.get(&1).unwrap().strings().get("k").unwrap();
        assert_eq!(slot.owned_element_count(), 4);
    }
    assert_eq!(read(&engine, "k"), Ok(Some(b"base-one-two-three".to_vec())));
}

#[test]
fn a_segment_that_cannot_be_read_is_an_error_and_never_a_shorter_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "k", b"base-");
    file_chain(&engine, "k", 5, &[b"one-", b"two-"]);
    // Point segment 2 at a slab that does not exist.
    {
        let mut shards = engine.shards.write().unwrap();
        let slot = shards.get_mut(&1).unwrap().strings_mut().get_mut("k").unwrap();
        let mut owned = slot.owned_elements().unwrap().to_vec();
        let last = owned.len() - 1;
        owned[last].1 = crate::block_store::ElementEntry::from_parts(999_999, 0, 4, Some(2), None);
        slot.set_owned_elements(
            super::object_record::OwnedElementTenancy::DurableOnTheWire {
                kind: super::storage_bucket_internals::StoredModelKind::String,
            },
            owned,
        );
    }
    assert_eq!(
        read(&engine, "k"),
        Err(StringChainError::SegmentMissing { ordinal: 2 })
    );
}

#[test]
fn a_base_that_disagrees_with_its_length_card_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "k", b"base-");
    // The card says 9 bytes; the base is 5.
    file_chain(&engine, "k", 9, &[b"one-"]);
    assert_eq!(
        read(&engine, "k"),
        Err(StringChainError::LengthMismatch { card: 9, read: 5 })
    );
}

#[test]
fn a_whole_value_write_drops_the_chain_it_replaces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "k", b"base-");
    file_chain(&engine, "k", 5, &[b"one-", b"two-"]);
    assert_eq!(read(&engine, "k"), Ok(Some(b"base-one-two-".to_vec())));
    // SET replaces the value. A segment left listed under the new base would be read back onto a
    // value it was never part of, so the new value must read as itself.
    set(&engine, "k", b"fresh");
    assert_eq!(read(&engine, "k"), Ok(Some(b"fresh".to_vec())));
    let shards = engine.shards.read().unwrap();
    let slot = shards.get(&1).unwrap().strings().get("k").unwrap();
    assert_eq!(slot.owned_element_count(), 0, "the replaced chain is still listed");
}

#[test]
fn deleting_the_key_takes_the_chain_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "k", b"base-");
    file_chain(&engine, "k", 5, &[b"one-"]);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringDelete { key: "k".to_string() },
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert_eq!(read(&engine, "k"), Ok(None));
    // A key written again afterwards starts a fresh value with no inherited segments.
    set(&engine, "k", b"again");
    assert_eq!(read(&engine, "k"), Ok(Some(b"again".to_vec())));
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

#[test]
fn get_strlen_and_getrange_answer_from_the_whole_chain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "k", b"base-");
    file_chain(&engine, "k", 5, &[b"one-", b"two-"]);
    assert_eq!(
        run(&engine, Command::StringGet { key: "k".into() }),
        CommandResponse::Bytes {
            value: Some(b"base-one-two-".to_vec())
        },
        "GET must answer the whole chain, not the base the row names"
    );
    assert_eq!(
        run(&engine, Command::StringLen { key: "k".into() }),
        CommandResponse::Integer { value: 13 }
    );
    assert_eq!(
        run(
            &engine,
            Command::StringGetRange {
                key: "k".into(),
                start: 0,
                end: -1
            }
        ),
        CommandResponse::Bytes {
            value: Some(b"base-one-two-".to_vec())
        }
    );
}

#[test]
fn an_append_onto_a_chain_keeps_every_byte_the_chain_held() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_in(dir.path());
    set(&engine, "k", b"base-");
    file_chain(&engine, "k", 5, &[b"one-", b"two-"]);
    // The read-modify-write must start from the WHOLE value, or it would write back the base alone
    // and lose the segments.
    assert_eq!(
        run(
            &engine,
            Command::StringAppend {
                key: "k".into(),
                value: b"end".to_vec()
            }
        ),
        CommandResponse::Integer { value: 16 }
    );
    assert_eq!(
        run(&engine, Command::StringGet { key: "k".into() }),
        CommandResponse::Bytes {
            value: Some(b"base-one-two-end".to_vec())
        }
    );
}
