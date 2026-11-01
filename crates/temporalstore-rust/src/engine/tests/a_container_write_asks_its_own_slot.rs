// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A container write asks the object's own lookup slot whether it is still in a bucket, not the
//! whole shard.
//!
//! WHAT IT COST. Per superseded block, `upsert_bucket_index_block_inner` decided whether to drop
//! the object from `bucket.object_index` by building `BlockKeysByPosition::from_lookup` over the
//! WHOLE SHARD'S lookup -- two HashMaps, keyed `(u32,u64)` and `u64` -- then `block_identities_in`
//! over the bucket, a third, and then scanning the bucket's entire `block_index`.
//!
//! That is O(ALL BLOCKS IN THE SHARD) per write, to answer one question.
//!
//! MEASURED, one hash key, milliseconds per field inserted:
//!
//!   fields     100     200     400     800
//!   before    5.87   15.49   33.29  182.65
//!   after     1.38    1.58    3.12    9.94
//!
//! 18.4x at 800 fields, and the 800-field build fell from about 146 s to 8 s. Callgrind put
//! `from_lookup` at 52.97% inclusive, `block_identities_in` at 34.54%, and ~58% of all
//! instructions in SipHash -- the two maps' own keys.
//!
//! WHY THE SLOT IS THE RIGHT PLACE TO ASK. Every `remove_object_block_lookup_ref` has already run
//! by that point, so the slot is final; and the lookup never holds a deleted block, so a ref
//! naming a bucket means a LIVE block in that bucket -- exactly the question. The buckets are
//! collected rather than assumed to be one, because a block routes by object key but the shard's
//! routing RANGE can change, so blocks filed under an older range can sit in a bucket this key
//! would not derive today.
//!
//! WHAT THIS PINS. The failure modes of getting that decision wrong are both silent, and they are
//! opposite, so both are asserted:
//!
//!   * REMOVING TOO EAGERLY loses the object from its bucket's list while blocks of it remain --
//!     so every field written must still read back.
//!   * NEVER REMOVING leaves the list naming an object with no blocks -- so a deleted key must
//!     leave nothing readable behind.
//!
//! The reconcile oracle covers the same invariant from the other side:
//! `the_entry_cannot_name_its_object_by_slot` and `the_slot_survives_a_reload` read
//! `object_index_divergence_report` as a delta around a reload, and both still pass.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;
const KEY: &str = "slot:container";
/// Enough fields that many writes supersede a block and reach the decision under test.
const FIELDS: u64 = 200;

fn set(engine: &TemporalEngine, field: u64) {
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::HashSet {
                    key: KEY.to_string(),
                    field: format!("f{field}"),
                    value: field.to_le_bytes().to_vec(),
                },
            })
            .status
            .ok,
        "the write has to land"
    );
}

fn get(engine: &TemporalEngine, field: u64) -> Option<Vec<u8>> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::HashGet {
                key: KEY.to_string(),
                field: format!("f{field}"),
            },
        })
        .response
    {
        CommandResponse::Bytes { value } => value,
        other => panic!("hash get answered {other:?}"),
    }
}

#[test]
fn every_field_survives_a_long_run_of_writes() {
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD);
    for field in 0..FIELDS {
        set(&engine, field);
    }

    // REMOVING TOO EAGERLY would drop the object from its bucket's list while its blocks remain,
    // and the fields filed under it stop being found. Every one of them is asserted, not a sample,
    // because an off-by-one in the decision loses SOME of them.
    let mut missing = Vec::new();
    for field in 0..FIELDS {
        if get(&engine, field).as_deref() != Some(&field.to_le_bytes()[..]) {
            missing.push(field);
        }
    }
    assert!(
        missing.is_empty(),
        "every field written must still read back; missing {} of {FIELDS}: {:?}",
        missing.len(),
        &missing[..missing.len().min(8)]
    );
}

#[test]
fn deleting_the_key_leaves_nothing_readable() {
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD);
    for field in 0..FIELDS {
        set(&engine, field);
    }

    // DENOMINATOR: there is something to delete, so the assertion below cannot pass against an
    // object that was never written.
    assert_eq!(
        Some(0u64.to_le_bytes().to_vec()),
        get(&engine, 0),
        "DENOMINATOR: the object has to exist before its deletion means anything"
    );

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::CommonDelete {
                    key: KEY.to_string(),
                },
            })
            .status
            .ok,
        "the delete has to be accepted"
    );

    // NEVER REMOVING would leave the bucket's list naming an object with no blocks. The observable
    // consequence is a field still answering after the key is gone.
    for field in [0, FIELDS / 2, FIELDS - 1] {
        assert_eq!(
            None,
            get(&engine, field),
            "field f{field} still answers after the key was deleted"
        );
    }
}
