// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHO ELSE READS THE RESIDENT CONTAINER MAP, AND WHICH OF THEM ANSWERS WRONG.
//!
//! # WHERE THIS STARTS
//!
//! #2017 drove that a resident container map can hold an element the live page index says is gone:
//! after a removal and a reconcile against a snapshot that predates it, `shard.sets` held 2 members
//! and the live page index held 1. It refused to serve `SMEMBERS` from that map for exactly that
//! reason, and enumerated the map's OTHER readers without asking any of them anything:
//!
//! ```text
//!     record_exists_exact                 EXISTS / TTL / EXPIRE / PERSIST
//!     collect_live_block_slab_ids          the reclaim live-slab set
//!     compact_shard_blocks_relocating      rewrites page addresses in place
//!     collect_upsert_index_items           resolves a member to build its WAL index item
//!     visit_model_live_blocks              re-renders the component; bucket release and rebuild
//! ```
//!
//! This module asks each of them, on a driven over-complete state rather than from the source. Two
//! of the five turn out to read the map at a granularity the over-completeness cannot reach; two
//! read it and are held off by something else, asserted here so a change that removes it fails the
//! build; and the FIRST one answers wrong today -- but not for the reason #2017's enumeration
//! suggests, and not through the state #2017 built.
//!
//! # THE LIVE ONE, AND IT IS NOT A MEMBER-LEVEL DEFECT
//!
//! `record_exists_exact` reads `shard.sets` at KEY granularity -- `shard.sets.contains_key(key)` --
//! so an extra MEMBER in the map cannot move its answer. The key is in both populations and EXISTS
//! is 1 either way. Asked on #2017's state it is correct, and this module asserts that rather than
//! implying it.
//!
//! What moves it is an EMPTY member map left under a live key, and `SetRemove` left one on every
//! removal:
//!
//! ```text
//!     if let Some(set) = shard.sets.get_mut(&key) {
//!         mutated |= set.remove(&member).is_some();
//!     }
//! ```
//!
//! The member goes, the key stays, and `record_exists_exact`'s `contains_key` then answers 1 for a
//! set with no members -- a key every listing reports as empty, `SCARD` as 0, and `EXISTS` as
//! present. `CommonExpire` gates on the same function, so a deadline is accepted for it too, and
//! `ttl_ms` reports that deadline rather than the -2 of a missing key.
//!
//! THREE OF FOUR KINDS ALREADY DID THIS, which is what makes it an omission rather than a design.
//! `HashDelete` removes the key when its last field goes, `ZSetRemove` when its last member goes,
//! `ListPop` when its last element goes. Only `SetRemove` did not -- and the note that explains the
//! cleanup at `HashDelete` said, in a parenthesis, that sets did not need one. They do: the reader
//! that makes a phantom hash observable is `record_exists_exact`, and it reads `shard.sets` in the
//! very next line of the same expression.
//!
//! THE DISCRIMINATOR IS A COUNT ACROSS THE FOUR KINDS, not one key's answer. A single-kind fixture
//! cannot tell "sets are the outlier" from "this is how containers behave", which is the shape
//! #2016's first mutation run was defeated by. `an_emptied_container_key_does_not_report_as_present`
//! drives all four, asserts each one was genuinely emptied, and fails on the COUNT of kinds that
//! still answer 1. Before the fix that count is 1 of 4; it is 0 of 4 now, with the four-kind
//! denominator asserted so a fixture that silently wrote nothing cannot pass.
//!
//! # THE OTHER FOUR, ASKED ON #2017's STATE
//!
//! `the_resident_map_readers_asked_in_the_over_complete_state` builds the divergence on TWO keys so
//! the states differ by a COUNT and not by which address wins a walk, and asks every reader.
//!
//! `visit_model_live_blocks` EMITTED THE GHOST. It walks `shard.sets` and re-renders each member's
//! component, so a resurrected member is emitted as a live page entry with a component naming it.
//! That feeds `collect_model_live_block_entries`, bucket release and the bucket-index rebuild.
//!
//! `collect_live_block_slab_ids` ABSORBED THE GHOST'S SLAB. It walks the model maps directly -- the
//! comment in `compact_shard_blocks_relocating` says so, as the reason it cannot share the
//! compaction preamble's walk -- so the ghost's `block_slab_id` joins the reclaim live set and a
//! slab is held against reclamation for a page nothing can reach. Conservative, so not data loss,
//! and real.
//!
//! `compact_shard_blocks_relocating` RELOCATED IT, and this is the one the source reads the other
//! way round until you find the right loop. Its preamble builds `collect_live_block_entries` reports,
//! and that function dispatches to the BUCKET-INDEX walk whenever the index is non-empty -- which is
//! what makes it look page-index-bounded. The relocation itself is not those reports. It is
//! `for (key, members) in shard.sets.iter_mut()`, one arm per model, handing every address the map
//! names to `compact_block_addresses` to be read off the old slab, appended to the fresh one and
//! rewritten in place. So the RESIDENT MAP is compaction's work list: a ghost is a page compaction
//! reads, copies onto every slab it ever rolls, and keeps alive for good. The test asserts the
//! property that makes the loop safe -- every address the maps name is a live page -- over all five
//! element-bearing models, because the loop has an arm for each.
//!
//! `collect_upsert_index_items` WOULD build an item for the ghost -- asked directly with the ghost's
//! component it resolves the address straight out of `shard.sets` and emits a page item, which on
//! replay would re-install an index entry for a page that was removed. Nothing asks it: the
//! component list it is driven from comes from `command_upsert_components`, which has arms for the
//! six ADD/SET commands and none for any removal. `SetRemove`'s component is produced by
//! `command_removed_component` instead, on the `collect_command_index_items_for` path. LATENT, and
//! the test asserts the exact condition -- `command_upsert_components` answers `None` for
//! `SetRemove` -- so an arm added there fails this module.
//!
//! # WHETHER THE MERGE CAN BE MADE UNABLE TO RESURRECT
//!
//! It can, and #2005 already wrote the question. #2005 fixed a resurrection on this same function
//! by applying the delta fold's carried elements ONCE, after the page index had settled, asking
//! "is there still a page at the address this element was carried with?" -- `live_page_key` over the
//! finished bucket index. That fix was complete FOR THE CARRY. It left the other input to the same
//! merge unasked.
//!
//! SO THIS IS A DIFFERENT MECHANISM, NOT AN INCOMPLETE FIX, and the distinction is the whole reason
//! the question was worth asking. #2005's resurrection came from `fold_carried_container_elements`
//! putting an element into the durable map that a later record's page removal then contradicted --
//! an input BUILT during the load. #2017's comes from the PERSISTED map itself,
//! `set_index_serde`'s `(member bytes, address)` pairs, being older than the page index it is merged
//! with -- an input READ off disk. One is a fold ordering bug and it is fixed; the other is a
//! snapshot-age property of the durable map and `fill_absent_elements` had no filter for it at all.
//! `both_inputs_to_the_merge_now_answer_the_same_live_page_question` drives the two side by side.
//!
//! `fill_absent_elements` now asks #2005's question of the persisted map too. That KEEPS #1989's
//! element and DROPS #2017's, which is what makes it the right question rather than a narrowing:
//! #1989's case is a page whose component cannot be decoded, so the derived view cannot name the
//! element while the page is RIGHT THERE in the index -- live at its address, kept. #2017's case is
//! a page that is gone from the index -- not live at its address, dropped. The function's stated job,
//! "keep every durable element the derived view could not produce", becomes "keep every durable
//! element the derived view could not produce AND whose page is still there".
//!
//! A PERSISTED KEY WITH NO SURVIVING ELEMENT NOW GETS NO ENTRY AT ALL. The old body ran
//! `derived.entry(key).or_default()` before looking at a single element, so a persisted key whose
//! every page was gone -- and a persisted key holding an empty map -- installed an EMPTY member map
//! under a live key: the same phantom `SetRemove` was leaving, arriving by reload instead. The entry
//! is created only when an element survives now.
//!
//! # THE BRANCH THAT DID NOT REACH THE MERGE AT ALL
//!
//! The three merges were gated on a `saw_*` flag, and the flag was not the protection it looks like:
//!
//! ```text
//!     if saw_sets {
//!         let persisted = std::mem::take(&mut shard.sets);
//!         shard.sets = fill_absent_elements(sets, persisted);
//!     }
//! ```
//!
//! `saw_sets` is assigned at the TOP of the set arm, BEFORE the component decode. So it never meant
//! "at least one set entry decoded" -- `saw_hashes` means that, because #2016 moved it inside its
//! match for a reason that applies to the one arm with no durable map behind it. `saw_sets == false`
//! means the settled page index holds NO SET PAGE AT ALL, and the skip then left the deserialized
//! persisted map standing WHOLE: unfiltered, every stale entry intact. A store whose set pages were
//! all removed after its last base-index write reloaded with a full resident map and an empty page
//! index, which is the over-complete state by the one route the merge never saw.
//!
//! The three merges are UNCONDITIONAL now, so the filter answers that arm too. Running them with an
//! empty derived view is safe for the same reason the filter is safe at all: it drops only an element
//! whose address matches no live page in the finished index, which trusts the index exactly as far as
//! this function already trusts it two arms up, where `shard.strings = strings` and
//! `shard.hashes = hashes` assign the derived view WHOLESALE.
//! `the_merge_runs_even_when_no_set_page_survived` drives both arms.
//!
//! # AND STILL NO SCAN SHIPS, FOR A SMALLER REASON THAN THE ONE THIS OPENED WITH
//!
//! The map can no longer hold a member the page index does not, which was #2017's whole objection,
//! and every reader above now agrees with the index. It does not follow that `SMEMBERS` can be served
//! from it, and the residual is in the other direction: a live page whose component cannot be decoded
//! is in the index and, unless the persisted map happens to hold it, in NO map -- #1989's case, where
//! the derived view cannot name the element. A listing served from `shard.sets` would MISS it, and a
//! listing that under-reports is worse than the reads it saves. Nothing in this engine writes a
//! non-hex set component, so that is latent, and it is latent on the write path rather than on this
//! function.
//!
//! So the saving #2017 measured -- `SMEMBERS` at 1.00 page reads per member, flat across a 32x width
//! change, against a `zset` control at 0.00 -- is now UNBLOCKED rather than taken. It is a
//! serving-path change with its own measurement and its own gate, and it does not belong in a commit
//! that moves a recovery path. Stated here so the next reader does not have to re-derive whether the
//! map is ready: it is, on the over-reporting side, which is the side that made it unsafe.
//!
//! # NOT COVERED, stated so the cover is not read as total
//!
//! The `zset` and `list` merges take the same new filter and the same two tests exercise the set
//! arm only for the reader sweep; `both_inputs_to_the_merge_now_answer_the_same_live_page_question`
//! covers all three arms for the filter itself. `hashes` has no durable map and no merge, so it has
//! no persisted input to filter. The page-read saving is quoted from #2017 and not re-derived here.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "resident-map-readers".to_string(),
        shard_uri: "local://resident-map-readers/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
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

fn run(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(
        response.status.ok,
        "the fixture command failed: {response:?}"
    );
    response.response
}

/// The same, WITHOUT asserting the status, for the one call whose refusal is the finding. Returns
/// `Ok(response)` or `Err(status code)` so a refusal can be asserted by name rather than by absence.
fn try_run(engine: &TemporalEngine, command: Command) -> Result<crate::types::CommandResponse, String> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    if response.status.ok {
        Ok(response.response)
    } else {
        Err(response.status.code)
    }
}

/// The integer a command answered, or a panic naming what came back instead -- an `unwrap_or(0)`
/// here would read a shape change as a zero and every count below would quietly agree with it.
fn integer_of(response: &crate::types::CommandResponse) -> i64 {
    match response {
        crate::types::CommandResponse::Integer { value } => *value,
        other => panic!("expected an integer response, got {other:?}"),
    }
}

/// How many members a listing returned, for the four container kinds.
fn listed_len(engine: &TemporalEngine, kind: &str, key: &str) -> i64 {
    match kind {
        "hash" => integer_of(&run(
            engine,
            Command::HashLen {
                key: key.to_string(),
            },
        )),
        "zset" => integer_of(&run(
            engine,
            Command::ZSetCard {
                key: key.to_string(),
            },
        )),
        "list" => integer_of(&run(
            engine,
            Command::ListLen {
                key: key.to_string(),
            },
        )),
        "set" => match run(
            engine,
            Command::SetMembers {
                key: key.to_string(),
            },
        ) {
            crate::types::CommandResponse::Members { members } => members.len() as i64,
            other => panic!("SetMembers answered {other:?}"),
        },
        other => panic!("no listing for kind {other}"),
    }
}

fn exists(engine: &TemporalEngine, key: &str) -> i64 {
    integer_of(&run(
        engine,
        Command::CommonExists {
            key: key.to_string(),
        },
    ))
}

/// The members `shard.sets` holds for one key, straight off the resident map.
fn resident_members(engine: &TemporalEngine, key: &str) -> BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .sets
        .get(key)
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default()
}

/// The members the LIVE PAGE INDEX holds for one set key, decoded out of the component each page is
/// filed under -- the population a listing walks.
fn page_index_members(shard: &ShardState, key: &str) -> BTreeSet<Vec<u8>> {
    crate::engine::bucket_store::bucket_index_component_block_addresses(shard, "set", key)
        .iter()
        .filter_map(|(component, _)| {
            component
                .as_ref()
                .and_then(|name| hex::decode(name.to_string()).ok())
        })
        .collect()
}

// =================================================================================================
// 1. THE LIVE READER: A KEY THAT HOLDS NOTHING AND STILL REPORTS AS PRESENT
// =================================================================================================

/// AN EMPTIED CONTAINER KEY MUST NOT REPORT AS PRESENT, AND A SET'S DID.
///
/// `record_exists_exact` -- behind `EXISTS`, `TTL`, `EXPIRE` and `PERSIST` -- ORs
/// `shard.sets.contains_key(key)` in beside its bucket-index answer. `SetRemove` removed the member
/// from the inner map and left the outer key, so after the last member went the key was still in
/// `shard.sets` with an empty member map and `EXISTS` answered 1 for it.
///
/// ALL FOUR KINDS, AND THE ASSERTION IS ON THE COUNT. One kind cannot distinguish "sets are the
/// outlier" from "this is what containers do here", and `HashDelete`'s own note asserted the former
/// in a parenthesis. Each row asserts its container was genuinely emptied first -- a fixture whose
/// removal did nothing would report `EXISTS = 1` for the honest reason and pass a weaker check.
///
/// rust-internal: command surface only
#[test]
fn an_emptied_container_key_does_not_report_as_present() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // (kind, key, add, remove)
    let kinds: [(&str, &str, Command, Command); 4] = [
        (
            "hash",
            "phantom-hash",
            Command::HashSet {
                key: "phantom-hash".to_string(),
                field: "f".to_string(),
                value: b"v".to_vec(),
            },
            Command::HashDelete {
                key: "phantom-hash".to_string(),
                field: "f".to_string(),
            },
        ),
        (
            "zset",
            "phantom-zset",
            Command::ZSetAdd {
                key: "phantom-zset".to_string(),
                member: b"m".to_vec(),
                score: 1.5,
            },
            Command::ZSetRemove {
                key: "phantom-zset".to_string(),
                member: b"m".to_vec(),
            },
        ),
        (
            "list",
            "phantom-list",
            Command::ListPush {
                key: "phantom-list".to_string(),
                member: b"m".to_vec(),
                left: false,
            },
            Command::ListPop {
                key: "phantom-list".to_string(),
                left: false,
            },
        ),
        (
            "set",
            "phantom-set",
            Command::SetAdd {
                key: "phantom-set".to_string(),
                member: b"m".to_vec(),
            },
            Command::SetRemove {
                key: "phantom-set".to_string(),
                member: b"m".to_vec(),
            },
        ),
    ];

    let mut rows: Vec<(&str, i64, i64, i64, i64)> = Vec::new();
    let mut phantom_kinds: Vec<&str> = Vec::new();

    for (kind, key, add, remove) in kinds {
        run(&engine, add);
        // THE CONTROL SIDE OF EVERY ROW: the fixture wrote something and the key is present for the
        // honest reason.
        let len_before = listed_len(&engine, kind, key);
        let exists_before = exists(&engine, key);
        assert_eq!(
            1, len_before,
            "{kind}: the fixture wrote {len_before} element(s), not 1, so the removal below has \
             nothing to prove"
        );
        assert_eq!(
            1, exists_before,
            "{kind}: EXISTS answered {exists_before} for a key holding one element, so this \
             fixture's EXISTS says nothing"
        );

        run(&engine, remove);
        let len_after = listed_len(&engine, kind, key);
        assert_eq!(
            0, len_after,
            "{kind}: the listing still reports {len_after} element(s) after the removal, so this \
             row never emptied its container"
        );
        let exists_after = exists(&engine, key);
        if exists_after != 0 {
            phantom_kinds.push(kind);
        }
        rows.push((kind, len_before, exists_before, len_after, exists_after));
    }

    println!("\n=== EXISTS after the last element of a container is removed ===");
    println!("  kind  len before  EXISTS before  len after  EXISTS after");
    for (kind, len_before, exists_before, len_after, exists_after) in &rows {
        println!(
            "  {kind:<5} {len_before:>11} {exists_before:>14} {len_after:>10} {exists_after:>13}"
        );
    }
    println!(
        "  {} of {} kind(s) report a key with no elements as present",
        phantom_kinds.len(),
        rows.len()
    );

    // THE DENOMINATOR. Four kinds exercised, each proven to have emptied.
    assert_eq!(
        4,
        rows.len(),
        "DENOMINATOR: {} kind(s) exercised, not the 4 this claim is about",
        rows.len()
    );
    assert!(
        phantom_kinds.is_empty(),
        "{:?} report a container key with no elements as present. `record_exists_exact` reads \
         `shard.<kind>.contains_key(key)`, so a removal that leaves an empty inner map behind \
         leaves a key that EXISTS answers 1 for and every listing answers empty for",
        phantom_kinds
    );
}

/// AND THE SAME PHANTOM ACCEPTS A DEADLINE, which is the reader behind `EXPIRE` rather than
/// `EXISTS`.
///
/// `CommonExpire` records a deadline only `for record_key in associated_record_keys(&key)` where
/// `record_exists_exact(shard, &record_key)`, and `ttl_ms` gates on the same function before it
/// reads `expires_at_ms`. So the phantom is not one reader answering oddly: it puts a live deadline
/// into `shard.expires_at_ms` for a key with nothing under it, and `TTL` reports that deadline
/// instead of the -2 that means "no such key".
///
/// THE CONTROL IS THE SAME KEY WITH A MEMBER STILL IN IT: `EXPIRE` must be accepted there, or this
/// test would pass against an `EXPIRE` that had simply stopped working.
///
/// rust-internal: command surface only
#[test]
fn an_emptied_set_does_not_accept_a_deadline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // THE CONTROL: a set that still holds a member takes a deadline.
    run(
        &engine,
        Command::SetAdd {
            key: "ttl-control".to_string(),
            member: b"stays".to_vec(),
        },
    );
    run(
        &engine,
        Command::CommonExpire {
            key: "ttl-control".to_string(),
            ttl_ms: 600_000,
        },
    );
    let control_ttl = integer_of(&run(
        &engine,
        Command::CommonTtl {
            key: "ttl-control".to_string(),
        },
    ));
    assert!(
        control_ttl > 0,
        "the control set with a live member reports TTL {control_ttl}, so EXPIRE is not working \
         here and the subject below would pass for the wrong reason"
    );

    // THE SUBJECT: the same shape, emptied first.
    run(
        &engine,
        Command::SetAdd {
            key: "ttl-phantom".to_string(),
            member: b"goes".to_vec(),
        },
    );
    run(
        &engine,
        Command::SetRemove {
            key: "ttl-phantom".to_string(),
            member: b"goes".to_vec(),
        },
    );
    assert_eq!(
        0,
        listed_len(&engine, "set", "ttl-phantom"),
        "the subject set is not empty, so this test never built a phantom"
    );
    // NOT asserted ok: the refusal IS the fixed behaviour, and it is stronger than "no deadline was
    // recorded". Before the cleanup this answered ok and `TTL` then reported the deadline it had
    // recorded; now the key is genuinely absent and `CommonExpire` refuses it by name.
    let phantom_expire = try_run(
        &engine,
        Command::CommonExpire {
            key: "ttl-phantom".to_string(),
            ttl_ms: 600_000,
        },
    );
    let phantom_ttl = match try_run(
        &engine,
        Command::CommonTtl {
            key: "ttl-phantom".to_string(),
        },
    ) {
        Ok(response) => integer_of(&response),
        // A refusal here says the same thing -2 says, and more loudly.
        Err(_) => -2,
    };

    println!(
        "\n=== EXPIRE then TTL ===\n  control (one member) EXPIRE ok, TTL {control_ttl} ms\n  \
         emptied set         EXPIRE {:?}, TTL {phantom_ttl} (-2 means no such key)",
        phantom_expire.as_ref().map(|_| "ok")
    );

    assert!(
        phantom_expire.is_err(),
        "EXPIRE was ACCEPTED for a set with no members, so `record_exists_exact` can still see the \
         key in `shard.sets` and a deadline is now recorded against nothing"
    );
    assert_eq!(
        -2, phantom_ttl,
        "TTL answered {phantom_ttl} for a set with no members, so a deadline stands against a key \
         every listing reports as empty"
    );
}

// =================================================================================================
// 2. THE OTHER FOUR READERS, ASKED ON #2017's OVER-COMPLETE STATE
// =================================================================================================

/// One key's divergence fixture: `population` members written, `removed` of them removed, and the
/// pre-removal durable map captured so it can be put back the way a stale snapshot would.
struct Divergence {
    key: String,
    members: Vec<Vec<u8>>,
    removed: Vec<Vec<u8>>,
    persisted_before_removal: BTreeMap<Vec<u8>, BlockAddress>,
}

fn build_divergence(
    engine: &TemporalEngine,
    key: &str,
    population: usize,
    remove_count: usize,
) -> Divergence {
    let members: Vec<Vec<u8>> = (0..population)
        .map(|index| format!("{key}-member-{index:03}").into_bytes())
        .collect();
    for member in &members {
        run(
            engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    // What `set_index_serde` would have serialized at this moment: the map as it stands, before any
    // removal. Captured off the live map, which is exactly what it serializes.
    let persisted_before_removal = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        shard
            .sets
            .get(key)
            .expect("the seeded key is in the resident map")
            .clone()
    };
    assert_eq!(
        population,
        persisted_before_removal.len(),
        "{key}: the pre-removal snapshot holds {} member(s), not the {population} written",
        persisted_before_removal.len()
    );
    let removed: Vec<Vec<u8>> = members.iter().take(remove_count).cloned().collect();
    for member in &removed {
        run(
            engine,
            Command::SetRemove {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let live_now = resident_members(engine, key).len();
    assert_eq!(
        population - remove_count,
        live_now,
        "{key}: {live_now} member(s) in the map after removing {remove_count} of {population}, so \
         the fixture's removals did not land"
    );
    Divergence {
        key: key.to_string(),
        members,
        removed,
        persisted_before_removal,
    }
}

/// EVERY READER OF THE RESIDENT MAP, ASKED ON THE STATE #2017 BUILT.
///
/// TWO KEYS, AND THE DISCRIMINATOR IS A COUNT. `ghost` gets a stale snapshot put back under it, so
/// the merge is asked to resurrect; `control` gets its own real snapshot put back, so the merge is
/// asked for something and resurrects nothing. A one-key fixture would differ from its own fix only
/// in which address won a walk, which is the shape #2016's first mutation run survived.
///
/// rust-internal: calls the engine's own reconcile and its map readers directly
#[test]
fn the_resident_map_readers_asked_in_the_over_complete_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // 4 written, 2 removed -> the stale snapshot is over-complete by 2.
    let ghost = build_divergence(&engine, "ghost", 4, 2);
    // 4 written, 0 removed -> its own snapshot is exact. THE CONTROL.
    let control = build_divergence(&engine, "control", 4, 0);

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");

    // The load path's input: the durable map as an index snapshot predating the removals.
    shard
        .sets
        .insert(ghost.key.clone(), ghost.persisted_before_removal.clone());
    shard
        .sets
        .insert(control.key.clone(), control.persisted_before_removal.clone());

    let live_pages: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| {
            bucket
                .block_index
                .values()
                .filter(|page| !page.deleted)
                .count()
        })
        .sum();
    assert!(
        live_pages >= 6,
        "DENOMINATOR: {live_pages} live page(s), so the derived view is too thin for the merge \
         below to be doing anything"
    );

    crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
        &engine.block_store,
        shard,
        None,
    );

    // ---------------------------------------------------------------------------------------------
    // The two populations, per key.
    // ---------------------------------------------------------------------------------------------
    let mut rows: Vec<(&str, usize, usize)> = Vec::new();
    for divergence in [&ghost, &control] {
        let in_map = shard
            .sets
            .get(&divergence.key)
            .map(|members| members.keys().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let in_index = page_index_members(shard, &divergence.key);
        rows.push((
            if divergence.key == "ghost" {
                "ghost"
            } else {
                "control"
            },
            in_map.len(),
            in_index.len(),
        ));
        // Every row's control: the members that were never removed are on BOTH sides, or the merge
        // dropped everything and nothing below measures what it claims.
        for member in divergence.members.iter().filter(|member| {
            !divergence
                .removed
                .iter()
                .any(|removed| removed == *member)
        }) {
            assert!(
                in_map.contains(member),
                "{}: a member that was never removed is absent from the resident map",
                divergence.key
            );
            assert!(
                in_index.contains(member),
                "{}: a member that was never removed is absent from the live page index",
                divergence.key
            );
        }
        // The removals landed in the page index -- otherwise there is no divergence to find.
        for member in &divergence.removed {
            assert!(
                !in_index.contains(member),
                "{}: a removed member still has a live page entry, so its removal did not drop \
                 its page",
                divergence.key
            );
        }
    }

    println!("\n=== resident map vs live page index, after a reconcile against a stale snapshot ===");
    println!("  key      map members  index members  over-complete by");
    for (label, in_map, in_index) in &rows {
        println!(
            "  {label:<8} {in_map:>12} {in_index:>14} {:>17}",
            in_map.saturating_sub(*in_index)
        );
    }

    // ---------------------------------------------------------------------------------------------
    // READER 1: record_exists_exact -- KEY granularity, so an extra MEMBER cannot move it.
    // ---------------------------------------------------------------------------------------------
    assert!(
        crate::engine::record_exists_exact(shard, "ghost"),
        "the ghost key has a live member and a live page, so EXISTS must be true here -- if this \
         fails the fixture is not the over-complete state this module is about"
    );
    println!(
        "  reader record_exists_exact       LATENT: key granularity (`shard.sets.contains_key`), \
         the key is in both populations"
    );

    // ---------------------------------------------------------------------------------------------
    // READER 2: visit_model_live_blocks -- walks the map and re-renders the component.
    // ---------------------------------------------------------------------------------------------
    let emitted_set_entries = crate::engine::storage_bucket_internals::collect_model_live_block_entries(shard)
        .into_iter()
        .filter(|entry| entry.kind.as_str() == "set" && &*entry.object_key == "ghost")
        .count();
    let ghost_index_members = page_index_members(shard, "ghost").len();
    println!(
        "  reader visit_model_live_blocks   emitted {emitted_set_entries} live set entr(ies) for \
         `ghost`, page index holds {ghost_index_members}"
    );

    // ---------------------------------------------------------------------------------------------
    // READER 3: collect_live_block_slab_ids -- walks the model maps directly.
    // ---------------------------------------------------------------------------------------------
    crate::snapshot_probe::reset();
    let _ = crate::engine::collect_live_block_slab_ids(shard);
    let map_walk_addresses = crate::snapshot_probe::counts().live_slab_scan_addresses;
    println!(
        "  reader collect_live_block_slab_ids visited {map_walk_addresses} model-map address(es) \
         over {live_pages} live page(s)"
    );

    // ---------------------------------------------------------------------------------------------
    // READER 4: compact_shard_blocks_relocating -- it relocates WHAT THE MODEL MAPS NAME.
    //
    // Its relocation loop is `for (key, members) in shard.sets.iter_mut()` (and one arm per other
    // model), handing each address to `compact_block_addresses` to be read off the old slab,
    // appended to the fresh one and rewritten IN PLACE. The preamble's `collect_live_block_entries`
    // reports are a different walk and do not bound it. So the map IS compaction's work list, and a
    // member the page index no longer holds is a page compaction reads, copies onto every new slab
    // it rolls, and keeps alive for good.
    //
    // THE PROPERTY THAT MAKES THAT SAFE IS THE ONE ASSERTED BELOW: every address the resident maps
    // name is a live page in the index. Checked over all five element-bearing models, not just the
    // set arm, because the relocation loop has an arm for each.
    // ---------------------------------------------------------------------------------------------
    let live_addresses: BTreeSet<(u64, u64, u64)> = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| !page.deleted)
        .map(|page| {
            (
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            )
        })
        .collect();
    let mut named_by_map = 0usize;
    let mut named_but_not_live: Vec<String> = Vec::new();
    {
        let mut check = |model: &str, key: &str, address: &BlockAddress| {
            named_by_map += 1;
            let identity = (
                address.block_slab_id(),
                address.offset(),
                address.length(),
            );
            if !live_addresses.contains(&identity) {
                named_but_not_live.push(format!("{model}/{key}"));
            }
        };
        for (key, address) in &shard.strings {
            check("string", key, address);
        }
        for (key, fields) in &shard.hashes {
            for address in fields.values() {
                check("hash", key, address);
            }
        }
        for (key, members) in &shard.sets {
            for address in members.values() {
                check("set", key, address);
            }
        }
        for (key, elements) in &shard.lists {
            for address in elements.values() {
                check("list", key, address);
            }
        }
        for (key, members) in &shard.zsets {
            for (_, address) in members.values() {
                check("zset", key, address);
            }
        }
    }
    println!(
        "  reader compact_shard_blocks_relocating relocates from the model maps: \
         {named_by_map} address(es) named, {} naming no live page",
        named_but_not_live.len()
    );
    // DENOMINATOR: 4 + 4 members written, 2 removed, so six addresses is the whole live population
    // this fixture built. A floor below it would let a fixture that wrote nothing pass.
    assert_eq!(
        6, named_by_map,
        "DENOMINATOR: the resident maps name {named_by_map} address(es), not the 6 this fixture \
         leaves live (8 members written, 2 removed)"
    );
    assert!(
        named_but_not_live.is_empty(),
        "the resident maps name {} address(es) that are not live pages in the index ({:?}). \
         `compact_shard_blocks_relocating` iterates those maps to build its relocation work list, \
         so each of these is a page compaction reads off the old slab, copies onto the fresh one \
         and keeps alive for good",
        named_but_not_live.len(),
        named_but_not_live
    );

    // ---------------------------------------------------------------------------------------------
    // READER 5: collect_upsert_index_items -- would resolve a ghost, and nothing asks it to.
    // ---------------------------------------------------------------------------------------------
    // Nothing asks: every arm of `command_upsert_components` is an ADD/SET. A removal's component
    // goes down `collect_command_index_items_for` instead.
    for removal in [
        Command::SetRemove {
            key: "ghost".to_string(),
            member: ghost.removed[0].clone(),
        },
        Command::ZSetRemove {
            key: "ghost-z".to_string(),
            member: b"m".to_vec(),
        },
        Command::ListPop {
            key: "ghost-l".to_string(),
            left: true,
        },
        Command::HashDelete {
            key: "ghost-h".to_string(),
            field: "f".to_string(),
        },
    ] {
        assert!(
            crate::engine::command_upsert_components(&removal, shard).is_none(),
            "`command_upsert_components` now names a component for a REMOVAL. \
             `collect_upsert_index_items` resolves that component's address out of the resident \
             map, so a removal reaching it would build a WAL index item pinning a page the removal \
             dropped -- and a replay would re-install it"
        );
    }
    // Asked outright, it does resolve one: the mechanism is live, only unreached. Driven with the
    // ADD command's own component spelling, so this is the item a reachable caller would get.
    let ghost_component = hex::encode(&ghost.removed[0]);
    let items_for_a_ghost = crate::engine::collect_upsert_index_items(
        shard,
        1,
        &[("set", "ghost".to_string(), Some(ghost_component.clone()))],
        0,
        1023,
    );
    println!(
        "  reader collect_upsert_index_items LATENT: no removal reaches it (4 removal commands \
         answer None from `command_upsert_components`); asked outright with the ghost's component \
         it builds {} item(s)",
        items_for_a_ghost.len()
    );

    // ---------------------------------------------------------------------------------------------
    // THE FINDING. The map and the page index hold the same population, per key.
    // ---------------------------------------------------------------------------------------------
    let control_row = rows
        .iter()
        .find(|(label, _, _)| *label == "control")
        .expect("the control row is in the table");
    assert_eq!(
        control_row.1, control_row.2,
        "THE CONTROL DIVERGED: the control key, whose snapshot is exact, holds {} member(s) in the \
         map against {} in the page index. Nothing below is about the stale snapshot then",
        control_row.1, control_row.2
    );
    assert!(
        control_row.2 >= 4,
        "CONTROL FLOOR: the control key has {} page-index member(s), so a control at 0 \
         over-complete members means nothing was exercised",
        control_row.2
    );

    let ghost_row = rows
        .iter()
        .find(|(label, _, _)| *label == "ghost")
        .expect("the ghost row is in the table");
    assert_eq!(
        ghost_row.1, ghost_row.2,
        "the resident map holds {} member(s) for `ghost` and the live page index holds {}: \
         `fill_absent_elements` resurrected {} member(s) from a snapshot older than the page \
         index. Every reader that walks `shard.sets` -- `visit_model_live_blocks` and \
         `collect_live_block_slab_ids` among them -- then sees a page that is gone",
        ghost_row.1,
        ghost_row.2,
        ghost_row.1.saturating_sub(ghost_row.2)
    );
    assert_eq!(
        ghost_index_members, emitted_set_entries,
        "`visit_model_live_blocks` emitted {emitted_set_entries} live set entr(ies) for `ghost` \
         against {ghost_index_members} in the page index, so it is re-rendering a component for a \
         member whose page was removed"
    );
    println!(
        "  the resident map and the live page index now hold one population, per key, and the two \
         map walkers agree with the index"
    );
}

// =================================================================================================
// 3. THE TWO INPUTS TO ONE MERGE, AND WHY #2005's FIX DID NOT COVER BOTH
// =================================================================================================

/// BOTH INPUTS TO THE MERGE NOW ANSWER THE SAME LIVE-PAGE QUESTION.
///
/// #2005 fixed a resurrection on `fill_absent_elements` from the CARRY side: the delta fold's
/// carried elements are applied once, after the page index settles, and only where a page is still
/// at the carried address. #2017 reached the same over-complete state through the other input --
/// the PERSISTED map read off disk, older than the page index it merges with -- which that fix
/// never looked at. Different mechanism, and the reason the second one survived the first fix.
///
/// THE TWO SIDE BY SIDE, one key each, all three merged kinds. Each subject's element has a
/// FABRICATED address that names no page -- which is what a stale snapshot's entry is, once its page
/// is gone -- and each control's element has its real live address. A subject dropped and a control
/// kept is the only outcome that distinguishes the filter from either "keep everything" or "keep
/// nothing".
///
/// rust-internal: calls the engine's own reconcile directly
#[test]
fn both_inputs_to_the_merge_now_answer_the_same_live_page_question() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    run(
        &engine,
        Command::SetAdd {
            key: "s".to_string(),
            member: b"set-control".to_vec(),
        },
    );
    run(
        &engine,
        Command::ZSetAdd {
            key: "z".to_string(),
            member: b"zset-control".to_vec(),
            score: 2.5,
        },
    );
    run(
        &engine,
        Command::ListPush {
            key: "l".to_string(),
            member: b"list-control".to_vec(),
            left: false,
        },
    );

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");

    // A page address that is in NO bucket: an offset far past anything this fixture wrote, on a
    // slab id nothing uses. This is the shape of a persisted entry whose page has been removed.
    let live_control_address = shard
        .sets
        .get("s")
        .and_then(|members| members.values().next())
        .cloned()
        .expect("the control set member is in the resident map");
    // A page address in NO bucket, built off a real one so every field is in range by construction
    // (`try_from_parts` refuses an out-of-range slab or offset, and a refusal here would read as a
    // test bug rather than as the state being modelled). Only the slab id moves, far past anything
    // this fixture rolled: this is the shape of a persisted entry whose page has been removed.
    let dead_address = BlockAddress::try_from_parts(
        live_control_address.block_slab_id().saturating_add(4_096),
        live_control_address.offset(),
        live_control_address.length(),
        None,
        None,
    )
    .expect("the fabricated address is in range");
    assert!(
        !shard.bucket_index.bucket_map.values().any(|bucket| bucket
            .block_index
            .values()
            .any(|page| !page.deleted
                && page.address.block_slab_id() == dead_address.block_slab_id()
                && page.address.offset() == dead_address.offset())),
        "the fabricated address matches a live page, so the subjects below would be kept for the \
         honest reason"
    );

    // The stale-snapshot entries: one subject per merged kind, beside each kind's real element.
    shard
        .sets
        .get_mut("s")
        .expect("set key")
        .insert(b"set-subject".to_vec(), dead_address.clone());
    shard
        .zsets
        .get_mut("z")
        .expect("zset key")
        .insert(b"zset-subject".to_vec(), (9, dead_address.clone()));
    shard
        .lists
        .get_mut("l")
        .expect("list key")
        .insert(i64::MAX - 3, dead_address.clone());

    crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
        &engine.block_store,
        shard,
        None,
    );

    let set_members: BTreeSet<Vec<u8>> = shard
        .sets
        .get("s")
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default();
    let zset_members: BTreeSet<Vec<u8>> = shard
        .zsets
        .get("z")
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default();
    let list_seqs: BTreeSet<i64> = shard
        .lists
        .get("l")
        .map(|entries| entries.keys().copied().collect())
        .unwrap_or_default();

    println!(
        "\n=== a persisted entry naming a page that is gone ===\n  set   {:?}\n  zset  {:?}\n  \
         list  {:?}",
        set_members
            .iter()
            .map(|member| String::from_utf8_lossy(member).to_string())
            .collect::<Vec<_>>(),
        zset_members
            .iter()
            .map(|member| String::from_utf8_lossy(member).to_string())
            .collect::<Vec<_>>(),
        list_seqs
    );

    // THE CONTROLS, all three: an element whose page IS live survives the merge. This is #1989's
    // rule and the filter must not touch it.
    assert!(
        set_members.contains(&b"set-control".to_vec()),
        "the set control was dropped, so the filter is keeping nothing rather than keeping live \
         pages"
    );
    assert!(
        zset_members.contains(&b"zset-control".to_vec()),
        "the zset control was dropped"
    );
    assert_eq!(
        1,
        list_seqs
            .iter()
            .filter(|seq| **seq != i64::MAX - 3)
            .count(),
        "the list control was dropped"
    );

    // THE SUBJECTS: an element whose page is gone is not resurrected.
    assert!(
        !set_members.contains(&b"set-subject".to_vec()),
        "`fill_absent_elements` resurrected a set member whose page is in no bucket"
    );
    assert!(
        !zset_members.contains(&b"zset-subject".to_vec()),
        "`fill_absent_elements` resurrected a zset member whose page is in no bucket"
    );
    assert!(
        !list_seqs.contains(&(i64::MAX - 3)),
        "`fill_absent_elements` resurrected a list element whose page is in no bucket"
    );
    println!("  all three subjects dropped, all three controls kept");
}

/// A BUCKET HOLDING A CONTAINER PAGE IS NEVER RELEASED, WHICH IS WHAT KEEPS THE FILTER FROM LOSING
/// DATA.
///
/// This is the one place the filter could have DESTROYED something rather than declined to resurrect
/// it, so the property it rests on is driven rather than read.
///
/// A RELEASED BUCKET'S PAGES ARE ABSENT FROM `bucket_index.bucket_map` ON PURPOSE while its elements
/// are still live -- that is what a release IS. `collect_bucket_index_live_block_entries` supplements
/// exactly those back in from the model maps, and says so: "what this returns is what the bucket index
/// WOULD say if nothing were released". A live-page set built by walking `bucket_map` directly -- the
/// shape that reads correctly and is wrong -- omits every released page, so if a container page could
/// ever sit in a released bucket, the filter would drop it. Four of the reconcile's six call sites run
/// on a live shard where a release can already have happened.
///
/// IT CANNOT, AND THE REASON IS AN ALLOW-LIST. `released_model_kind_is_addressable` admits exactly
/// `string` and `context_node`, and names `set`, `zset` and `list` among the kinds held out: they are
/// read whole through `bucket_index_component_block_addresses`, which has no point-lookup equivalent,
/// so a released page of those kinds could not be resolved. A candidate bucket holding one is refused
/// with `BlockKindNotAddressable`. The allow-list and the three merged kinds are disjoint.
///
/// SO THE FILTER'S LIVE SET IS STILL TAKEN FROM `entries` AND NOT FROM `bucket_map`, because resting
/// a data-loss argument on a disjointness that lives in another function is how the two drift apart.
/// Taking it from the same `entries` the derived view is built from means the filter and its subject
/// read ONE population, and the filter can only ever remove what the derived view also lacks -- true
/// whatever that allow-list later says.
///
/// THE CONTROL IS A STRING BUCKET, RELEASED IN THE SAME CALL. Without it, "the set bucket was not
/// released" is satisfied just as well by a fixture in which nothing can be released at all, which is
/// what the first version of this test hit.
///
/// rust-internal: calls the engine's own release actuator and reconcile directly
#[test]
fn a_bucket_holding_a_container_page_is_never_released() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let members: Vec<Vec<u8>> = (0..4)
        .map(|index| format!("released-member-{index:03}").into_bytes())
        .collect();
    for member in &members {
        run(
            &engine,
            Command::SetAdd {
                key: "released".to_string(),
                member: member.clone(),
            },
        );
    }
    // THE CONTROL's object: a `string`, which the allow-list DOES admit. Several, under keys chosen
    // to land in buckets of their own, so at least one bucket holds strings and no container page.
    for index in 0..6 {
        run(
            &engine,
            Command::StringSet {
                key: format!("str-{index}"),
                value: b"v".to_vec(),
            },
        );
    }
    // A release refuses a DIRTY bucket -- "at the shipped `eviction_dump_before_evict` false a
    // freshly written bucket is dirty and every candidate is refused on `bucket_dirty`". The index
    // is dumped, then the shard is put in the state a RELOAD leaves it in, which is the state the
    // actuator is shipped to act on: `load_index_inner` clears the per-bucket and per-page dirty
    // flags on the stated ground that reloaded data is durable, hence clean, and then refreshes the
    // runtime flags from the live (empty on load) `dirty_objects` set.
    engine.flush_shard_index(1);
    let _ = engine.dump_index_catalog(1);

    let (released_count, kind_refusals, set_pages_still_indexed, set_bucket_released) = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        shard.dirty_objects.clear();
        for bucket in shard.bucket_index.bucket_map.values_mut() {
            bucket.set_dirty(false);
            for page in bucket.block_index.blocks_mut_unaccounted() {
                page.dirty = false;
            }
        }
        crate::engine::storage_bucket_internals::refresh_bucket_runtime_flags(shard);
        // The bucket the set's pages are filed in, before anything is released.
        let (start, end) = shard.routing_range();
        let set_bucket = crate::engine::block_routing_bucket("released", start, end);
        let candidates: Vec<u32> = shard.bucket_index.bucket_map.keys().copied().collect();
        assert!(
            !candidates.is_empty(),
            "no bucket to release, so this fixture wrote nothing into the index"
        );
        let outcome =
            crate::engine::storage_bucket_internals::release_bucket_blocks(shard, &candidates);
        let set_pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.block_index.values())
            .filter(|page| !page.deleted && page.model_id.as_str() == "set")
            .count();
        (
            outcome.released_buckets.len(),
            outcome.refusals.block_kind_not_addressable,
            set_pages,
            shard.bucket_index.released_buckets.contains(&set_bucket),
        )
    };

    println!(
        "\n=== releasing every bucket, with a set among them ===\n  released \
         {released_count} bucket(s); {kind_refusals} refused on BlockKindNotAddressable\n  the \
         set's own bucket released: {set_bucket_released}; set pages still in `bucket_map`: \
         {set_pages_still_indexed}"
    );

    // THE CONTROL. A fixture in which nothing can be released would satisfy the finding below
    // trivially.
    assert!(
        released_count >= 1,
        "the release actuator released {released_count} bucket(s), so NOTHING in this fixture can \
         be released and the refusal below says nothing about the set"
    );
    // THE FINDING: the set's bucket is refused, by KIND, and its pages stay in the index.
    assert!(
        kind_refusals >= 1,
        "no bucket was refused on `BlockKindNotAddressable`, so the set's bucket was not held back \
         by its kind"
    );
    assert!(
        !set_bucket_released,
        "the bucket holding the set's pages was RELEASED. `released_model_kind_is_addressable` has \
         admitted a container kind, so a container page can now sit in a released bucket -- and the \
         live-page set the merge filters against must come from \
         `collect_bucket_index_live_block_entries`, which supplements released pages back in, and \
         never from `bucket_index.bucket_map`, which does not"
    );
    assert_eq!(
        4, set_pages_still_indexed,
        "the index holds {set_pages_still_indexed} live set page(s), not the 4 written, so the \
         release took some after all"
    );

    // Now the reconcile, on a shard whose page index is deliberately missing the released pages.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
            &engine.block_store,
            shard,
            None,
        );
    }

    let survived = resident_members(&engine, "released");
    println!(
        "  resident map holds {} of the {} member(s) written",
        survived.len(),
        members.len()
    );
    for member in &members {
        assert!(
            survived.contains(member),
            "a member in a RELEASED bucket was dropped by the merge. The live-page set is being \
             built from `bucket_index.bucket_map`, which omits a released bucket's pages by design; \
             it must come from `collect_bucket_index_live_block_entries`, which supplements them \
             back in from the model maps"
        );
    }
    assert_eq!(
        members.len(),
        survived.len(),
        "the resident map holds {} member(s) against the {} written",
        survived.len(),
        members.len()
    );
}

/// THE BRANCH THAT USED TO SKIP THE MERGE ENTIRELY, WHICH IS THE ONE A SCAN WOULD HAVE FALLEN
/// THROUGH.
///
/// The three merges were gated on `saw_sets` / `saw_zsets` / `saw_lists`. Each is set at the TOP of
/// its arm, BEFORE the component decode -- which is what distinguishes them from `saw_hashes`, moved
/// INSIDE its match by #2016 for a reason that applies to an arm with no durable map behind it. So
/// `saw_sets == false` never meant "no set entry decoded"; it meant the settled page index held NO
/// SET PAGE AT ALL, and skipping the merge there left the deserialized persisted map standing WHOLE.
///
/// A store whose every set page was removed after its last base-index write therefore reloaded with
/// a full resident map and an empty page index -- the over-complete state again, reached by the one
/// route the merge never saw, and the one a listing served from `shard.sets` would have fallen
/// straight through. The merges are unconditional now and the filter answers this arm too.
///
/// BOTH ARMS DRIVEN, and they differ by a COUNT rather than by which arm ran: the subject removes
/// every set page (no set entry survives, the old skip branch), the control leaves one alive (the
/// old merge branch). Both must end with the map equal to the page index.
///
/// rust-internal: calls the engine's own reconcile directly
#[test]
fn the_merge_runs_even_when_no_set_page_survived() {
    for (label, keep_one_member) in [("no set page decoded", false), ("one set page alive", true)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine);

        // A hash keeps the bucket index non-empty whichever arm runs, so the reconcile's own
        // `bucket_map.is_empty()` early return is never what decides this.
        run(
            &engine,
            Command::HashSet {
                key: "anchor".to_string(),
                field: "f".to_string(),
                value: b"v".to_vec(),
            },
        );
        let members: Vec<Vec<u8>> = (0..3)
            .map(|index| format!("member-{index}").into_bytes())
            .collect();
        for member in &members {
            run(
                &engine,
                Command::SetAdd {
                    key: "s".to_string(),
                    member: member.clone(),
                },
            );
        }
        let persisted = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1 is loaded");
            shard.sets.get("s").expect("set key").clone()
        };
        let removed_count = if keep_one_member { 2 } else { 3 };
        for member in members.iter().take(removed_count) {
            run(
                &engine,
                Command::SetRemove {
                    key: "s".to_string(),
                    member: member.clone(),
                },
            );
        }

        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        shard.sets.insert("s".to_string(), persisted);
        assert!(
            !shard.bucket_index.bucket_map.is_empty(),
            "{label}: the bucket index is empty, so the reconcile returns before any arm and this \
             says nothing about `saw_sets`"
        );

        crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
            &engine.block_store,
            shard,
            None,
        );

        let in_map = shard.sets.get("s").map_or(0, |members| members.len());
        let in_index = page_index_members(shard, "s").len();
        println!(
            "\n=== {label} ===\n  resident map {in_map} member(s), live page index {in_index} \
             member(s), over-complete by {}",
            in_map.saturating_sub(in_index)
        );

        if keep_one_member {
            // THE CONTROL ARM. One set page survives, so this is the branch that always merged.
            assert_eq!(
                1, in_index,
                "the control arm's page index holds {in_index} member(s), not the 1 it left alive"
            );
        } else {
            // THE SUBJECT ARM. No set page survives -- the branch that used to skip the merge.
            assert_eq!(
                0, in_index,
                "the subject arm's page index still holds {in_index} set member(s), so a set page \
                 survived and this arm is not the one it means to drive"
            );
        }
        assert_eq!(
            in_index, in_map,
            "{label}: the resident map holds {in_map} member(s) against {in_index} in the page \
             index. The merge is unconditional so that this holds on BOTH arms; a skip here leaves \
             the deserialized persisted map standing whole, which is the over-complete state by the \
             one route the merge never used to see"
        );
    }
}
