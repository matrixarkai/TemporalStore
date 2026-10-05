// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! EVERY ELEMENT OF A CONTAINER SURVIVES AN UNLOAD AND A RELOAD, FOR THE FOUR CONTAINER KINDS.
//!
//! # WHAT THIS MODULE DOES NOT ESTABLISH, AND ITS ORIGINAL TITLE SAID IT DID
//!
//! This landed as #2067 under the title "a container's resident map is the AUTHORITY a length can be
//! rerouted to". That is wrong, and the correction belongs here rather than in a note somewhere
//! else, because the file name still says `authority`.
//!
//! The reconcile on the load path is `RecordedMap::reconcile` -> `fill_absent_elements(derived,
//! persisted, live, ..)`, and its own doc states the rule: "The derived view wins where both have an
//! element -- it reflects the delta fold, which the persisted map does not". `derived` is built from
//! the bucket index AFTER `fold_index_log_deltas` has replayed the delta suffix over it; `persisted`
//! is the container map read out of the base index, which is only as new as the last compaction or
//! unload. So where the two disagree the INDEX wins, and the container supplies only what the
//! derived view could not name -- filtered by whether its block is still live.
//!
//! The reason is resurrection, and it is measured: a member removed after the last base-index write
//! is gone from the block index and present in the persisted map, and handing it back is the defect.
//! #2017 drove exactly that, at resident map 2 members against live block index 1, and it is why a
//! set listing could not be served from `shard.sets`.
//!
//! So the container is DURABLE but NOT AUTHORITATIVE, and pointing a length at it would read from
//! the losing side of that merge. #2093 is the shape that is actually correct: serve BOTH sources and
//! count the divergence, rather than reroute to one.
//!
//! # WHY THE BODY BELOW CANNOT TELL THE TWO APART, WHICH IS THE DEEPER FAULT
//!
//! The fixture writes, unloads and reloads, and both sources AGREE at every point -- the written
//! population is in the base index and in the container, and nothing diverges them. A fixture in
//! which two sources agree cannot establish a claim about WHICH source answered. The original title
//! made exactly that claim, so the name asserted something the body never measured.
//!
//! THE GENERAL RULE, worth more than this instance: if a test's name says "X is the authority", its
//! fixture must contain a case where X and the alternative DISAGREE. Otherwise the name is an
//! assertion the body does not make.
//!
//! # WHAT IT DOES ESTABLISH, which is true and worth keeping
//!
//! That no element is LOST across a reload, for all four container kinds, by length and by
//! membership. #2054 drove one case -- a page entry carrying no component, where only the durable map
//! can supply the element. This drives the ordinary case for four kinds, which is a different and
//! still useful fact: it is a no-loss guard, not an authority claim.
//!
//! THE DISCRIMINATING ARM IS NOT HERE YET, and that is stated rather than left unsaid. It needs a
//! fixture where the base index is older than the block index -- write, unload, reload, remove one
//! element, then load a fresh engine over the same directories WITHOUT an intervening unload, so the
//! base still names the removed element and the delta does not -- and it must assert that the INDEX
//! wins. Driven to fail by making the persisted map win, so it has shown it can discriminate.
//!
//! WHY LENGTH AND NOT MEMBERSHIP. A reroute of `HashLen` reads `len()`, so `len()` is what has to be
//! right. Membership is asserted beside it so a map that came back with the right COUNT of the wrong
//! elements cannot pass -- a count and a set are different questions and this module needs both.
//!
//! THE DENOMINATOR IS TAKEN BEFORE THE UNLOAD. Every arm asserts the map holds what was written
//! while the engine is still up, so a failure after the reload is about the reload and not about the
//! write. Without that, a write path that never populated the map would read as a durability hole.
//!
//! AND THE ENTRY COUNT IS REPORTED BESIDE THE MAP LENGTH, because the whole point is whether the two
//! agree. Where they disagree, the reroute CHANGES AN ANSWER, and that is a fact a reviewer needs
//! rather than a surprise after the fact.
#![allow(clippy::all)]
use super::*;

/// The shipped default since #1973, and the range an operator is told to set.
const NARROW_END: u32 = 1023;

/// Elements per container. Small: this module is about durability, not scale.
const ELEMENTS: usize = 12;

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
        table_name: "reload-authority".to_string(),
        shard_uri: "local://reload-authority/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the shard must load for any figure below to mean anything: {:?}",
        response.status
    );
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write must ack: {:?}", response.status);
}

/// (map length, map's element names) for one container, read straight off the resident map.
fn map_state(engine: &TemporalEngine, kind: &str, key: &str) -> (usize, Vec<String>) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    match kind {
        "hash" => shard
            .hashes
            .get(key)
            .map(|fields| {
                let mut names: Vec<String> = fields.keys().map(|k| k.to_string()).collect();
                names.sort();
                (fields.len(), names)
            })
            .unwrap_or((0, Vec::new())),
        "set" => shard
            .sets
            .get(key)
            .map(|members| {
                let mut names: Vec<String> = members
                    .keys()
                    .map(|m| String::from_utf8_lossy(m).to_string())
                    .collect();
                names.sort();
                (members.len(), names)
            })
            .unwrap_or((0, Vec::new())),
        "zset" => shard
            .zsets
            .get(key)
            .map(|members| {
                let mut names: Vec<String> = members
                    .keys()
                    .map(|m| String::from_utf8_lossy(m).to_string())
                    .collect();
                names.sort();
                (members.len(), names)
            })
            .unwrap_or((0, Vec::new())),
        "list" => shard
            .lists
            .get(key)
            .map(|elements| (elements.len(), Vec::new()))
            .unwrap_or((0, Vec::new())),
        other => panic!("unclassified kind {other}"),
    }
}

/// Live page-index entries for one container -- what `HashLen` answers from TODAY.
fn live_entries(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.block_index.values())
        .filter(|page| !page.deleted && page.model_id.as_str() == kind && &*page.object_key == key)
        .count()
}

fn hash_len_served(engine: &TemporalEngine, key: &str) -> i64 {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashLen {
            key: key.to_string(),
        },
    });
    assert!(response.status.ok, "HashLen must ack: {:?}", response.status);
    match response.response {
        CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?} instead of an integer"),
    }
}

/// NO ELEMENT OF A CONTAINER IS LOST ACROSS AN UNLOAD AND A RELOAD.
///
/// Four kinds, `ELEMENTS` elements each, the population asserted BEFORE the unload so a failure
/// afterwards is about the reload. The page-index entry count is printed beside the map length at
/// both points.
///
/// THE TWO NUMBERS AGREE HERE, AND THAT IS THE LIMIT OF WHAT THIS DRIVES. Agreement cannot say which
/// source answered, so this is a no-loss guard and not a statement about authority -- see the module
/// header for why the index wins where the two disagree, and for the arm that would measure it.
///
/// rust-internal: drives the four container writes and one reload
#[test]
fn a_containers_resident_map_still_holds_its_elements_after_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let kinds = [("hash", "rl-h"), ("set", "rl-t"), ("zset", "rl-z"), ("list", "rl-l")];
    for index in 0..ELEMENTS {
        write_to(
            &engine,
            Command::HashSet {
                key: "rl-h".to_string(),
                field: format!("f{index:04}"),
                value: vec![b'v'; 24],
            },
        );
        write_to(
            &engine,
            Command::SetAdd {
                key: "rl-t".to_string(),
                member: format!("m{index:04}").into_bytes(),
            },
        );
        write_to(
            &engine,
            Command::ZSetAdd {
                key: "rl-z".to_string(),
                member: format!("m{index:04}").into_bytes(),
                score: index as f64,
            },
        );
        write_to(
            &engine,
            Command::ListPush {
                key: "rl-l".to_string(),
                member: format!("m{index:04}").into_bytes(),
                left: false,
            },
        );
    }

    println!("=== before the unload ===");
    let mut before = Vec::new();
    for (kind, key) in kinds {
        let (len, names) = map_state(&engine, kind, key);
        let entries = live_entries(&engine, kind, key);
        println!("  {kind:<5} map len {len:>3}   live page entries {entries:>3}");
        assert_eq!(
            ELEMENTS, len,
            "DENOMINATOR: {kind}'s resident map holds {len} of {ELEMENTS} BEFORE any reload, so a \
             failure after one would not be about the reload"
        );
        before.push((kind, key, len, names, entries));
    }
    let hlen_before = hash_len_served(&engine, "rl-h");
    println!("  HLEN served (from the ENTRY count today): {hlen_before}");

    engine.unload_shard(1);
    load_on(&engine);

    println!("=== after unload + reload ===");
    let mut holes = Vec::new();
    for (kind, key, len_before, names_before, entries_before) in &before {
        let (len, names) = map_state(&engine, kind, key);
        let entries = live_entries(&engine, kind, key);
        println!(
            "  {kind:<5} map len {len:>3} (was {len_before})   live page entries {entries:>3} (was {entries_before})"
        );
        if len != *len_before {
            holes.push(format!(
                "{kind}: resident map came back with {len} of {len_before} elements"
            ));
        }
        if kind != &"list" && &names != names_before {
            holes.push(format!(
                "{kind}: the map came back with a DIFFERENT element set, not merely a different count"
            ));
        }
    }
    let hlen_after = hash_len_served(&engine, "rl-h");
    println!("  HLEN served after the reload: {hlen_after}");

    // An absent key must answer 0 rather than erroring -- the reroute would use
    // `.map(|m| m.len()).unwrap_or(0)` and that zero has to be the right answer.
    let (absent_len, _) = map_state(&engine, "hash", "no-such-key");
    assert_eq!(0, absent_len, "an absent hash key must read as length 0");
    assert_eq!(
        0,
        hash_len_served(&engine, "no-such-key"),
        "HLEN on an absent key must answer 0"
    );

    assert!(
        holes.is_empty(),
        "A CONTAINER'S RESIDENT MAP DID NOT SURVIVE A RELOAD, which is the property a reroute of \
         HashLen to `shard.hashes` would depend on:\n  {}",
        holes.join("\n  ")
    );
    assert_eq!(
        ELEMENTS as i64, hlen_after,
        "HLEN answered {hlen_after} for {ELEMENTS} fields after a reload"
    );
}
