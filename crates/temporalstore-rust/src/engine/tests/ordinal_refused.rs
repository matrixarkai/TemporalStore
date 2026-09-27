// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHY A PAGE ENTRY CANNOT NAME ITS ELEMENT WITH AN ORDINAL, ON A GROUND THE FIRST THREE MISSED.
//!
//! #1976 refuted the ordinal on three grounds. #1982 lifted one of them and weakened another, and
//! concluded the ordinal was UNBLOCKED. That conclusion was wrong, and this module is where it is
//! corrected: there is a FOURTH reader, it is the LOAD path, and it is stronger than any of the
//! three.
//!
//! # THE READER THAT DECIDES
//!
//! `reconcile_secondary_views_from_bucket_index` rebuilds the model maps FROM the bucket index on
//! load. For three kinds it reconstructs identity purely from the component name, without reading
//! the page at all:
//!
//! ```text
//!     set    parse_set_component(name)    -> the member's BYTES
//!     zset   parse_zset_component(name)   -> the SCORE BITS and the member's bytes
//!     list   parse_list_component(name)   -> the SEQUENCE
//! ```
//!
//! and then it does not merge the result -- it ASSIGNS it:
//!
//! ```text
//!     if saw_lists { shard.lists = lists; }
//!     if saw_zsets { shard.zsets = zsets; }
//!     if saw_sets  { shard.sets  = sets;  }
//! ```
//!
//! So on any load where the bucket index holds one zset page, the PERSISTED zset map -- which
//! `zset_index_serde` writes as `(member bytes, (score, address))` -- is thrown away and replaced by
//! a view derived from the names. The name is the authoritative source of a zset's score on the load
//! path.
//!
//! # WHY THAT IS FATAL TO AN ORDINAL AND THE OTHER THREE WERE NOT
//!
//! A zset's SCORE exists in exactly two places: the component name, and the persisted map this
//! function discards. It is not in the page -- the page holds the MEMBER. So an ordinal does not
//! merely make the score expensive to recover, it makes it unrecoverable: no number of page reads
//! brings back a score that was only ever spelled in a name.
//!
//! Contrast the two kinds immediately below them in the same function. `features` and
//! `control_state` MERGE, and `control_state`'s own comment says why: "the serialized i64 series is
//! authoritative (the page is a copy of it)". Three kinds discard a durable map in favour of a view
//! derived from a name; two do not. That asymmetry is the finding, and it is the prerequisite: until
//! the load path trusts `zset_index_serde` the way it already trusts the control-state series, an
//! ordinal cannot be introduced at all.
//!
//! # AND A SECOND REFUTATION, WHICH CORRECTS #1976 ITSELF
//!
//! #1976 opened with "a per-object page ordinal already exists -- `BlockAddress::block_id`". Measured
//! here: three live zset pages of ONE object all carry block_id 0. Every one of the sixteen callers
//! of `next_block_index_for_object` is a TIMESTAMPED kind, so the ordinal exists for `feature` and
//! `context_*` and NOT for `zset`, `set`, `list` or `hash` -- exactly the kinds with a hundred
//! components an object and the 80 bytes a name. There is nothing to reuse: an ordinal would have to
//! be invented, assigned and PERSISTED, and a persisted counter is what this store already rejected
//! for page handles because it hands out numbers that disagree with the refs already on disk.
//!
//! # WHAT THIS MODULE LANDS
//!
//! Not the ordinal. Two refutations, driven rather than argued; three defects found while asking the
//! question, one of them fixed and two recorded; and the collision rate measured rather than assumed,
//! because the last two times this campaign assumed a rate was zero it was wrong in both directions.
//!
//! # THE THIRD DEFECT: REUSE IS NOT PREVENTED
//!
//! `next_block_index_for_object`'s doc says a block id "must never be REUSED". A delete removes an
//! object's pages rather than tombstoning them, so the `max` drops back and the next write takes the
//! same number. Harmless while the number names a POSITION in an object whose pages went with it;
//! silent corruption the moment it names an ELEMENT.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "ordinal-refused".to_string(),
        shard_uri: "local://ordinal-refused/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

// =============================================================================================
// 1. THE REFUTATION: THE LOAD PATH REBUILDS A SCORE FROM THE NAME AND DISCARDS THE DURABLE MAP
// =============================================================================================

/// A ZSET'S SCORE COMES BACK FROM ITS COMPONENT NAME, AND THE PERSISTED MAP IS THROWN AWAY.
///
/// This is the fourth reader, and the one that refutes the ordinal outright.
///
/// THE EXPERIMENT. Write a zset. Reload the shard so the load path runs. Then MUTATE the component
/// name in the served index -- changing only the score half, leaving the member half and the
/// persisted `zsets` map untouched -- and reload again. If the score that comes back is the mutated
/// one, the NAME is authoritative and the durable map was discarded. If the original comes back, the
/// map is authoritative and an ordinal would be survivable here.
///
/// It is the mutated one. That is what makes the name load-bearing for LOAD, which no amount of
/// stating the element on an outcome record can fix -- #1982 put the member in the record, and a
/// score is not a member.
///
/// rust-internal: mutates the engine's own served index, no external surface
#[test]
fn a_zset_score_comes_back_from_its_component_name_not_from_the_persisted_map() {
    let dir = tempfile::tempdir().unwrap();
    let member = b"score-source-member".to_vec();
    let original_score = 7.5f64;

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        write(
            &engine,
            Command::ZSetAdd {
                key: "zs-key".to_string(),
                member: member.clone(),
                score: original_score,
            },
        );
        engine.unload_shard(1);
    }

    // What the name spells, and what a DIFFERENT score would spell. Built through the shipped
    // producer so the mutation is a name this engine could itself have written.
    let original_bits = crate::engine::execute_on_shard::zset_score_bits(original_score);
    let other_score = 99.25f64;
    let other_bits = crate::engine::execute_on_shard::zset_score_bits(other_score);
    let original_name = crate::engine::execute_on_shard::zset_component(original_bits, &member);
    let mutated_name = crate::engine::execute_on_shard::zset_component(other_bits, &member);
    assert_eq!(
        original_name.len(),
        mutated_name.len(),
        "the two names differ in length, so the byte swap below would not be a pure substitution"
    );
    assert_ne!(original_name, mutated_name, "the two scores spell the same name");
    println!("[source] original name {original_name:?}, mutated {mutated_name:?}");

    // Swap the name inside the served index, leaving everything else alone -- including the
    // persisted `zsets` map, which still holds the ORIGINAL score beside the member.
    let indexes = dir.path().join("indexes");
    let mut swapped_files = 0usize;
    for entry in std::fs::read_dir(&indexes).expect("the index directory exists") {
        let path = entry.expect("a directory entry").path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).expect("the index reads");
        let Some(swapped) = swap_inside_index(&bytes, &original_name, &mutated_name) else {
            continue;
        };
        std::fs::write(&path, &swapped).expect("the index rewrites");
        println!("[source] swapped the name inside {:?}", path.file_name());
        swapped_files += 1;
    }
    assert!(
        swapped_files > 0,
        "the component name was not found in any file under {indexes:?}, so nothing was mutated and \
         this test would pass without testing anything"
    );

    // Reload. The load path runs, and whichever source it trusts is the one that answers.
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetScore {
            key: "zs-key".to_string(),
            member: member.clone(),
        },
    });
    assert!(response.status.ok, "the read failed: {response:?}");
    let came_back = match response.response {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!(
            "the member did not come back at all after the reload: {other:?}. That would be a \
             different finding than this test is for -- the score's SOURCE -- so investigate rather \
             than adjusting the assertion."
        ),
    };
    let score: f64 = came_back
        .parse()
        .unwrap_or_else(|_| panic!("a score came back as {came_back:?}"));
    println!(
        "[source] wrote {original_score}, mutated the NAME to spell {other_score}, left the \
         persisted map at {original_score}; the reload answered {score}"
    );
    assert!(
        (score - other_score).abs() < 1e-9,
        "the reload answered {score}. If it answered {original_score} the PERSISTED map is \
         authoritative and an ordinal is survivable on the load path -- which would overturn this \
         module's refutation, so check `reconcile_secondary_views_from_bucket_index` before \
         believing it."
    );
    println!(
        "[source] so the COMPONENT NAME is the authoritative source of a zset's score on the load \
         path, and `shard.zsets = zsets` discards the durable map. An ordinal cannot carry a score, \
         and no page read recovers one: the page holds the MEMBER."
    );
}

/// Replace one occurrence of a name inside a served index, whatever payload codec it uses.
///
/// `None` when the file carries no such name. The JSON payload is compressed, so the swap has to
/// happen inside the decompressed body and be recompressed; a byte search over the compressed frame
/// would find nothing and silently report success.
fn swap_inside_index(bytes: &[u8], from: &str, to: &str) -> Option<Vec<u8>> {
    const MAGIC: &[u8] = b"TSIDX\x01";
    if !bytes.starts_with(MAGIC) {
        let text = String::from_utf8(bytes.to_vec()).ok()?;
        if !text.contains(from) {
            return None;
        }
        return Some(text.replace(from, to).into_bytes());
    }
    let codec = *bytes.get(MAGIC.len())?;
    let payload = &bytes[MAGIC.len() + 1..];
    // Codec 1 is zstd-JSON; codec 2 is zstd-msgpack with a four-byte version header.
    let (header, body) = match codec {
        1 => (0usize, payload),
        2 => (4usize, payload.get(4..)?),
        _ => return None,
    };
    let plain = zstd::stream::decode_all(body).ok()?;
    // EVERY occurrence. `block_index_written_key` renders the component into the map key this
    // index is serialized under, so the name appears at least twice -- and replacing only the first
    // would leave the key and the entry naming different elements, which is a corrupt index rather
    // than a controlled experiment.
    let mut swaps = 0usize;
    let mut swapped = Vec::with_capacity(plain.len());
    let needle = from.as_bytes();
    let mut at = 0usize;
    while at < plain.len() {
        if plain[at..].starts_with(needle) {
            swapped.extend_from_slice(to.as_bytes());
            at += needle.len();
            swaps += 1;
            continue;
        }
        swapped.push(plain[at]);
        at += 1;
    }
    if swaps == 0 {
        return None;
    }
    println!("[source] replaced {swaps} occurrence(s) of the name inside one payload");
    let recompressed = zstd::stream::encode_all(swapped.as_slice(), 3).ok()?;
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + header + recompressed.len());
    out.extend_from_slice(MAGIC);
    out.push(codec);
    if header == 4 {
        out.extend_from_slice(&payload[..4]);
    }
    out.extend_from_slice(&recompressed);
    Some(out)
}

// =============================================================================================
// 2. THE TWO DEFECTS FOUND WHILE ASKING THE QUESTION
// =============================================================================================

/// ORDINAL REUSE IS NOT PREVENTED: A DELETE THEN A WRITE TAKES THE SAME NUMBER.
///
/// `next_block_index_for_object`'s doc says a block id "must never be REUSED". This test was written
/// to drive the mechanism that supposedly prevented it -- a tombstoned block keeping its ordinal in
/// the `max` -- and found there is no such mechanism. A delete REMOVES the object's pages from the
/// bucket index, so the `max` drops back and the next write is handed the same number.
///
/// WHAT IT RECORDS is therefore the behaviour, not the wish. Asserting the wish would have produced a
/// red test describing a store that does not exist; asserting nothing would have left the next reader
/// to believe the doc.
///
/// WHY IT IS BENIGN TODAY, EXACTLY. A block id names a POSITION inside an object, and the pages that
/// shared the reused number went away with the object, so no live predecessor is still answering for
/// it. That is narrower than "never reused" and it is the whole of the safety.
///
/// WHY IT REFUTES THE ORDINAL. Under an ordinal the number names an ELEMENT, not a position. The same
/// delete-then-write then gives two elements one identity, and a reload serves whichever it reaches
/// first -- silent corruption, reachable by an ordinary sequence of two commands rather than by a
/// 4-billion-block ceiling.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
fn ordinal_reuse_is_not_prevented_and_a_delete_then_write_takes_the_same_number() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let ordinals = |engine: &TemporalEngine, key: &str| -> Vec<(u64, bool)> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let mut held = Vec::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            for page in bucket.block_index.values() {
                if &*page.object_key != key {
                    continue;
                }
                if let Some(block_id) = page.address.block_id() {
                    held.push((block_id, page.deleted));
                }
            }
        }
        held.sort();
        held
    };

    // ---------------------------------------------------------------- a WHOLE-OBJECT delete
    write(
        &engine,
        Command::FeatureAppend {
            key: "ord-feature".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_070_000,
                value: b"first".to_vec(),
            }],
        },
    );
    let after_first = ordinals(&engine, "ord-feature");
    assert!(
        !after_first.is_empty(),
        "the first append produced no block with an ordinal, so this test cannot measure reuse"
    );
    let first = after_first
        .iter()
        .map(|(block_id, _)| *block_id)
        .max()
        .expect("just asserted non-empty");

    write(
        &engine,
        Command::FeatureDelete {
            key: "ord-feature".to_string(),
        },
    );
    let after_delete = ordinals(&engine, "ord-feature");

    write(
        &engine,
        Command::FeatureAppend {
            key: "ord-feature".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_071_000,
                value: b"second".to_vec(),
            }],
        },
    );
    let after_second = ordinals(&engine, "ord-feature");
    let second = after_second
        .iter()
        .map(|(block_id, _)| *block_id)
        .max()
        .expect("the second append produced no block");

    println!(
        "[reuse] whole-object delete: first append took {first}, after the delete the index held \
         {after_delete:?}, the second append took {second}"
    );
    assert!(
        after_delete.is_empty(),
        "the delete left {after_delete:?} in the index. If a delete now TOMBSTONES rather than \
         removing, the tombstone does reserve the ordinal after all and this module's second reason \
         for stopping is gone -- which is a better world, so correct the reasoning rather than the \
         assertion."
    );
    assert_eq!(
        second, first,
        "the second append took {second} where the first took {first}. Reuse has stopped happening, \
         which would be a change to what a delete does to the index -- check it rather than \
         adjusting this number."
    );
    println!(
        "[reuse] so a block ordinal IS reused, in two ordinary commands. Harmless while it names a \
         POSITION in an object whose pages went with it; silent corruption the moment it names an \
         ELEMENT."
    );

    // ------------------------------------------- and a PARTIAL removal, which is the harder case
    //
    // Here a live predecessor DOES remain: one member is removed from a zset that still holds
    // another. If the removal frees an ordinal that the next write then takes, two pages of ONE live
    // object share a position -- which is the case the doc's own warning describes.
    for element in 0..3u64 {
        write(
            &engine,
            Command::ZSetAdd {
                key: "ord-zset".to_string(),
                member: format!("member-{element}").into_bytes(),
                score: element as f64,
            },
        );
    }
    let zset_before = ordinals(&engine, "ord-zset");
    write(
        &engine,
        Command::ZSetRemove {
            key: "ord-zset".to_string(),
            member: b"member-1".to_vec(),
        },
    );
    let zset_after_removal = ordinals(&engine, "ord-zset");
    write(
        &engine,
        Command::ZSetAdd {
            key: "ord-zset".to_string(),
            member: b"member-99".to_vec(),
            score: 99.0,
        },
    );
    let zset_after_add = ordinals(&engine, "ord-zset");
    println!(
        "[reuse] partial removal on a LIVE object: before {zset_before:?}, after removing one \
         {zset_after_removal:?}, after adding one {zset_after_add:?}"
    );
    let distinct: std::collections::BTreeSet<u64> =
        zset_after_add.iter().map(|(block_id, _)| *block_id).collect();
    println!(
        "[reuse] {} live page(s) of one zset hold {} DISTINCT ordinal(s): {:?}",
        zset_after_add.len(),
        distinct.len(),
        distinct
    );

    // THE FINDING THAT ENDS THE ORDINAL LINE OF WORK, and it corrects #1976's own first sentence.
    //
    // #1976 opened with "a per-object page ordinal already exists -- `BlockAddress::block_id`". That
    // is true for the TIMESTAMPED kinds: every one of the sixteen callers of
    // `next_block_index_for_object` is `feature` or `context_*`. It is FALSE here. Three live pages
    // of one zset all carry block_id 0, because a container kind's write path never asks for a next
    // index -- so for `zset`, `set`, `list` and `hash`, which are exactly the kinds with a hundred
    // components an object and the 80 bytes a name, THERE IS NO ORDINAL TO USE.
    //
    // One would have to be invented, assigned and PERSISTED. A persisted counter is the thing this
    // codebase already rejected for page handles, in as many words: a counter hands out different
    // numbers than the ones already written into the refs on disk, "which is what a counter did,
    // silently, until a reload lost an object".
    assert_eq!(
        distinct.len(),
        1,
        "the {} live pages of one zset now hold {} distinct ordinals rather than sharing one. If a container kind has started asking `next_block_index_for_object` for a per-element number, the premise of the ordinal work is true after all and this module's conclusion has to be redone -- which would be good news. Pages: {zset_after_add:?}",
        zset_after_add.len(),
        distinct.len()
    );
    assert!(
        zset_after_add.len() > 1,
        "only {} live page(s), so this test cannot show that several share one ordinal",
        zset_after_add.len()
    );
    println!(
        "[reuse] so there is NO per-element ordinal for a container kind: every page of this zset shares block_id 0. The ordinal #1976 pointed at belongs to the timestamped kinds, whose component is already a pure number. For the kinds worth 80 B a name, an ordinal would have to be invented, assigned and persisted -- and a persisted counter is what this store already rejected for handles."
    );
}

/// THE ORDINAL ASSIGNER REFUSES AT ITS CEILING RATHER THAN REISSUING.
///
/// It used to end `.unwrap_or(u32::MAX).saturating_add(1)`, which answers `u32::MAX` for every block
/// past the ceiling -- so they share an ordinal, which is exactly the uniqueness this function exists
/// to uphold. Saturating is the arithmetic that looks safest and is worst here.
///
/// Driven with a planted block at the ceiling. `#[should_panic]` on the MESSAGE, not merely on a
/// panic: a test that accepts any panic passes when the fixture itself is broken.
///
/// rust-internal: builds an index entry directly, no external surface
#[test]
#[should_panic(expected = "has used every block ordinal a u32 holds")]
fn the_ordinal_assigner_refuses_at_its_ceiling_rather_than_reissuing() {
    // The object already holds the highest ordinal a u32 can express, so there is no next one.
    let index = index_holding_ordinal(u64::from(u32::MAX));
    let _ = crate::engine::state::next_block_index_for_object(&index, 0, "feature", "ceiling-object");
}

/// THE CONTROL ON THAT PANIC: one ordinal below the ceiling still answers.
///
/// A `#[should_panic]` test says nothing on its own about where the boundary is -- it would pass just
/// as well if the function panicked on every input. This is the neighbouring case that must NOT
/// panic.
///
/// rust-internal: builds an index entry directly, no external surface
#[test]
fn one_ordinal_below_the_ceiling_still_answers() {
    let index = index_holding_ordinal(u64::from(u32::MAX - 1));
    let next =
        crate::engine::state::next_block_index_for_object(&index, 0, "feature", "ceiling-object");
    assert_eq!(
        next,
        u32::MAX,
        "one below the ceiling answered {next}, not {}",
        u32::MAX
    );
    println!("[ceiling] one below the ceiling answers {next}; the ceiling itself refuses");
}

/// A one-bucket index holding one feature page of `ceiling-object` at the given ordinal.
///
/// Built through `BlockIndexMap::insert` rather than by reaching into the map, so the fixture takes
/// the same door a write does and cannot drift from the shape a real index has.
fn index_holding_ordinal(block_id: u64) -> crate::engine::state::CoreIndex {
    let mut node = crate::engine::state::BucketNode {
        routing_bucket: 0,
        ..crate::engine::state::BucketNode::default()
    };
    let mut live = crate::engine::state::BlockSlabLiveIndex::default();
    node.block_index.insert(
        crate::engine::state::BlockIndex {
            object_key: std::sync::Arc::from("ceiling-object"),
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::Feature,
            component: None,
            address: crate::block_store::BlockAddress::from_parts(
                7,
                0,
                64,
                Some(block_id),
                Some(1),
                Some(0),
            ),
            dirty: false,
            deleted: false,
            log_backed: false,
        },
        &mut live,
    );
    let mut index = crate::engine::state::CoreIndex::default();
    index.bucket_map.insert(0, node);
    index
}

// =============================================================================================
// 3. WHAT WOULD HAVE WORKED, CONFIRMED RATHER THAN ASSUMED
// =============================================================================================

/// THE WHOLE-OBJECT READ ENUMERATES BY KEY AND WOULD HAVE SERVED ORDINALS PERFECTLY WELL.
///
/// `bucket_index_component_block_addresses` is "every component of this object". Its caller holds a
/// key and no component list, so it cannot look one up -- it filters the bucket's entries on the key
/// and takes what it finds. An ordinal is as enumerable as a name, so this reader was never the
/// obstacle, and confirming that is what leaves the refutation resting on the load path alone.
///
/// Driven on a SET, whose members are read whole through exactly this path, and asserted to answer
/// every element without any by-component lookup.
///
/// rust-internal: reads the engine's own read path, no product behaviour
#[test]
fn the_whole_object_read_enumerates_by_key_and_would_serve_ordinals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let members: Vec<Vec<u8>> = (0..9)
        .map(|element| format!("enumerated-member-{element:03}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: "en-set".to_string(),
                member: member.clone(),
            },
        );
    }

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: "en-set".to_string(),
        },
    });
    assert!(response.status.ok, "the read failed: {response:?}");
    let answered = match response.response {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    assert_eq!(
        answered.len(),
        members.len(),
        "the enumeration answered {} of {} members",
        answered.len(),
        members.len()
    );
    for member in &members {
        assert!(
            answered.iter().any(|held| held == member),
            "the enumeration lost {:?}",
            String::from_utf8_lossy(member)
        );
    }
    println!(
        "[enumerate] the whole-object read answered all {} members by filtering on the KEY; it \
         never asked for a component, so an ordinal would have served it",
        answered.len()
    );
}

/// HOW OFTEN A SPELLED COMPONENT NAME IS ALSO A VALID ORDINAL. MEASURED, NOT ASSUMED.
///
/// The last two times this campaign assumed a rate was zero it was wrong: 387 of 20,000 hexadecimal
/// names were also well-formed under the new spelling, and a first draft of the outcome-shape rate
/// test asserted a tautology. So the rate is measured here in the direction that matters for an
/// ordinal -- a store carrying spelled names, read by a binary expecting ordinals.
///
/// An ordinal on the wire would be a small number. The question is how many spelled names PARSE as
/// one, because each that does is a silent mis-read rather than a refusal.
///
/// rust-internal: pure spelling, no product path
#[test]
fn how_often_a_spelled_component_name_would_also_parse_as_an_ordinal() {
    let mut examined = 0usize;
    let mut parses_as_ordinal = 0usize;
    let mut widest = 0usize;
    let mut seed = 0x853C_49E6_748F_EA9Bu64;
    for _ in 0..20_000 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let member: Vec<u8> = (0..(1 + (seed % 24) as usize))
            .map(|at| (seed >> ((at % 8) * 8)) as u8)
            .collect();
        let name = crate::engine::execute_on_shard::zset_component(seed, &member);
        examined += 1;
        widest = widest.max(name.len());
        // An ordinal would be spelled as one `u64`'s worth of characters at most -- and every one of
        // these names is longer than that, because a zset name is a score AND a member. A reader
        // expecting an ordinal would take the first `U64_CHARS` characters and get a number that has
        // nothing to do with any ordinal, with the member silently discarded.
        if crate::component_name::parse_u64(&name).is_some() {
            parses_as_ordinal += 1;
        }
    }
    println!(
        "[collision] {parses_as_ordinal} of {examined} spelled zset names ({:.4}%) parse as a lone \
         number; the widest was {widest} characters against the {} an ordinal takes",
        100.0 * parses_as_ordinal as f64 / examined as f64,
        crate::component_name::U64_CHARS
    );
    assert_eq!(
        parses_as_ordinal, 0,
        "{parses_as_ordinal} of {examined} spelled names parse as a lone number, so a reader \
         expecting an ordinal would accept them and read a different element. The width is what \
         refuses them today; if that has changed the refusal has to become explicit."
    );

    // AND THE ONE THAT DOES COLLIDE, which is why a width check is not a refusal. A LIST name is a
    // single spelled u64 -- exactly the shape an ordinal would take -- so every list name parses as
    // an ordinal and means something else entirely.
    let mut list_collisions = 0usize;
    for sequence in [0i64, 1, -1, i64::MIN, i64::MAX, 4_096, -4_096] {
        let name = crate::engine::execute_on_shard::list_component(sequence);
        let as_ordinal = crate::component_name::parse_u64(&name);
        if let Some(number) = as_ordinal {
            list_collisions += 1;
            println!(
                "[collision] a LIST name for sequence {sequence} is {name:?}, which parses as the \
                 number {number} -- indistinguishable from an ordinal of that value"
            );
        }
    }
    assert_eq!(
        list_collisions, 7,
        "expected every one of the seven list names to parse as a number; {list_collisions} did. A \
         list component IS a single spelled u64, which is exactly an ordinal's shape."
    );
    println!(
        "[collision] so 100% of LIST names are well-formed ordinals with a different meaning. A \
         width check refuses a zset name and CANNOT refuse a list one, which is why the stored-shape \
         version -- not the shape of the name -- has to carry the answer."
    );
}
