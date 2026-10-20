// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE KEY, ONE ALLOCATION -- the one-copy invariant, in a test a DEFAULT run executes.
//!
//! # WHY THIS FILE EXISTS BESIDE THE PROBES THAT ALREADY CHECK THIS
//!
//! `index_bytes_per_key` carries three guards on this invariant and a default `cargo test` runs
//! NONE of them: each is `#[cfg(feature = "alloc-probe")]` AND `#[ignore]`, so it needs a feature
//! flag and `--ignored` to execute. Of that file's 21 tests, 14 are ignored. An assertion has three
//! states -- true, false, and never evaluated -- and those were in the third.
//!
//! That matters more here than usual because of HOW the invariant breaks. A map that could share a
//! key allocation but is missing from `adopt_object_key_allocations` keeps its own copy and
//! **nothing fails**: the data is correct, every read answers, and a clone-based byte probe cannot
//! see the duplicate, because cloning an `Arc` is a refcount bump. Twice in one session a map was
//! found holding a second copy only because a probe was widened by hand -- first
//! `control_state_selection`, then `seen` and `buckets`. A guard that only runs when someone
//! remembers to pass `--ignored` would not have caught either.
//!
//! So this is the cheap version: eight keys, no counting allocator, no corpus, nothing ignored. It
//! measures by POINTER, which is the only way to tell one shared allocation from two equal copies.
//! The expensive probes keep their breakdowns and their byte figures; this one keeps the invariant
//! reachable.
//!
//! rust-internal: measures this crate's own resident structures, no product behaviour

use crate::types::{Command, ExecuteRequest};
use crate::TemporalEngine;
use std::sync::Arc;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

#[test]
fn a_key_held_by_several_maps_is_one_allocation() {
    const KEYS: usize = 8;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // The same raw key through every family that keys a per-key map by it. Each command is here
    // because it puts the key in a DIFFERENT map; drop one and this stops covering that map.
    for index in 0..KEYS {
        let raw = format!("one-copy-{index}");
        for command in [
            Command::StringSet {
                key: raw.clone(),
                value: vec![b'v'; 16],
            },
            Command::FeatureAppend {
                key: raw.clone(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index as u64,
                    value: vec![b'f'; 8],
                }],
            },
            Command::ControlStateSelectionSet {
                key: raw.clone(),
                value: vec![b's'; 8],
                occur_time_ms: 1_787_270_070_000 + index as u64,
                ttl_ms: 0,
                selection_type: crate::types::ControlStateSelectionType::Last,
            },
            Command::SeenCheck {
                key: raw.clone(),
                member: vec![b'm'; 8],
                window_ms: 60_000,
            },
            Command::BucketTake {
                key: raw.clone(),
                tokens: 1.0,
                capacity: 10.0,
                refill_per_sec: 1.0,
            },
            // The counter command, and it is here because its ABSENCE is what let a duplicate
            // live: with nothing writing `control_state`, that map reported zero keys, which
            // reads exactly like a map with nothing to share. It was in fact holding a second
            // copy of these characters, for every key.
            Command::ControlStateIncrement {
                key: raw.clone(),
                timestamp_ms: 1_787_270_070_000 + index as u64,
                amount: 1,
            },
            // THE SEVENTH HOLDER, and it was invisible to this guard until it was named. The
            // distinct-change map is filled by `hll::record_change` with
            // `.entry(key.to_string())` -- a fresh allocation per key, from a `&str` -- and its
            // key is this same raw object key. A guard that collects from a list of map names
            // cannot see a holder that is not on the list, which is the standing risk its own
            // doc names and this is that risk arriving.
            Command::ControlStateChangeAdd {
                key: raw.clone(),
                timestamp_ms: 1_787_270_070_000 + index as u64,
                value: format!("member-{index}").into_bytes(),
                precision_ms: None,
                ttl_ms: None,
            },
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // (text, address of its characters) from every map keyed by the raw key. A `String` key and an
    // `Arc<str>` key both expose `as_ptr()`, so this needs no list of which map is keyed by what --
    // and an allowlist of map NAMES is what went stale the first time this guard was written.
    let mut held: std::collections::BTreeMap<&str, Vec<(&str, usize)>> =
        std::collections::BTreeMap::new();
    for (name, keys) in [
        (
            "strings",
            shard
                .strings
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect::<Vec<_>>(),
        ),
        (
            "features",
            shard
                .features
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "control_state_selection",
            shard
                .control_state_selection
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "seen",
            shard
                .seen
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "buckets",
            shard
                .buckets
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        // Only the RAW-keyed entries of this map can ever share. The family operators file a
        // decorated `control_state:<family>:<key>` that no other map holds, so those are
        // filtered out rather than counted as holders that failed to share.
        (
            "control_state",
            shard
                .control_state
                .keys()
                .filter(|k| !k.contains(':'))
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "control_state_changes",
            shard
                .control_state_changes
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
    ] {
        for (text, ptr) in keys {
            held.entry(text).or_default().push((name, ptr));
        }
    }

    // --- DENOMINATORS, because every claim below divides by them. ---
    let multi: Vec<_> = held.iter().filter(|(_, h)| h.len() > 1).collect();
    assert_eq!(
        KEYS,
        multi.len(),
        "the fixture wrote {KEYS} keys through seven families but only {} ended up in more than \
         one map, so this run could not have detected a duplicate at all. Held: {:?}",
        multi.len(),
        held
    );
    assert!(
        multi.iter().all(|(_, h)| h.len() == 7),
        "a key reached fewer than seven maps, so one of the seven commands stopped writing its \
         map and this guard silently stopped covering it: {:?}",
        multi
    );

    // --- THE INVARIANT. ---
    let duplicated: Vec<_> = held
        .iter()
        .filter(|(_, h)| {
            h.iter()
                .map(|(_, p)| *p)
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 1
        })
        .collect();
    assert!(
        duplicated.is_empty(),
        "{} key texts are stored more than once -- their holders report different addresses for \
         the same characters: {:?}. A map holding a key another map already holds must take a \
         `ModelKey` and join `ShardState::adopt_object_key_allocations`; nothing else fails when \
         it does not, because cloning an `Arc` is a refcount bump.",
        duplicated.len(),
        &duplicated[..duplicated.len().min(3)]
    );
}

/// EVERY ADOPTED CONTEXT MAP HOLDS THE PAGE INDEX'S OWN KEY -- the other half of the invariant,
/// and the half the guard above cannot reach.
///
/// # WHY A SECOND TEST AND NOT MORE COMMANDS IN THE FIRST
///
/// The guard above works by finding one key text in several maps and comparing their addresses.
/// That shape cannot cover the context family at all, and the reason is structural rather than a
/// shortfall in the fixture: each context map is keyed by its OWN COMPOSED KEY -- `ctx:node:`,
/// `ctx:event:`, `ctxidx:`, `ctx:audit:`, `ctx:entity:`, `ctx:child:`, `ctx:summary:`,
/// `ctx:compress:` -- so no two of them ever hold the same characters, and a cross-map address
/// comparison has nothing to compare. Only the event pair shares one text by construction.
///
/// So the invariant has to be stated against the thing they all borrow FROM.
/// `adopt_object_key_allocations` takes its handle from
/// `bucket_index.object_block_lookup.any_shared_object_key`, which means a correctly adopted map
/// does not merely avoid duplicating a SIBLING -- its key IS the page index's allocation. That is
/// checkable per map with no sibling needed, and it is strictly stronger: a single-holder map that
/// mints its own copy duplicates the page index's key, and this catches it where a sibling
/// comparison would report nothing at all.
///
/// # WHAT THIS CLOSES
///
/// `ShardState` has 27 resident maps and the adoption list names 21. Before this, a default `cargo
/// test` measured 6 of them -- the context maps were measured in the changes that converted them
/// and by nothing standing since. An invariant whose only evidence is a past measurement has no
/// guard: a map can be re-keyed, an adoption line deleted, or a write arm moved to mint its own
/// handle, and every read still answers.
///
/// # AND THREE MAPS THIS FIXTURE CANNOT SPEAK FOR, WHICH IT NAMES RATHER THAN PASSES OVER
///
/// `NOT_REACHABLE` below names them with the measured reason, and each exclusion carries an
/// assertion that FAILS if the reason stops being true -- an excused map that is silently excused
/// forever is the same third state of an assertion this file exists to remove.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn every_adopted_context_map_holds_the_page_index_own_key() {
    use crate::types::{
        ContextChildRef, ContextCompressionEvent, ContextEntity, ContextEvent, ContextIndexRef,
        ContextNode, ContextPackAudit, ContextSummary,
    };

    const NODES: u64 = 4;
    const TENANT: u64 = 7;
    const NOW_MS: u64 = 1_787_270_070_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // One write per context family per node. Each command is here because it files a DIFFERENT
    // map; drop one and this stops covering that map, which is the failure mode the first guard's
    // doc describes.
    for node in 1..=NODES {
        for command in [
            Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash: node,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: format!("node-{node}"),
                    l0: format!("summary of node {node}"),
                    status: 0,
                    last_event_time_ms: NOW_MS,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
            Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: node,
                event: Box::new(ContextEvent {
                    event_id_hash: 100 + node,
                    event_time_ms: NOW_MS + node,
                    ingestion_time_ms: NOW_MS + node,
                    kind: 0,
                    event_type: 1,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 1.0,
                    importance: 1.0,
                    text: format!("event on node {node}"),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                }),
                first_write_only: false,
                cold_storage: false,
            },
            Command::ContextWriteIndexRef {
                tenant_hash: TENANT,
                index_name: "kind".to_string(),
                index_value_hash: 200 + node,
                scope_hash: 0,
                event_time_ms: NOW_MS + node,
                index_ref: ContextIndexRef {
                    primary_node_hash: node,
                    primary_event_time_ms: NOW_MS + node,
                    event_id_hash: 100 + node,
                },
            },
            Command::ContextWritePackAudit {
                tenant_hash: TENANT,
                audit: ContextPackAudit {
                    query_id: format!("q-{node}"),
                    session_hash: 300 + node,
                    request_time_ms: NOW_MS + node,
                    query_hash: 400 + node,
                    max_prompt_tokens: 1_024,
                    selected_tokens: 16,
                    selected_refs: Vec::new(),
                    blocked_refs: Vec::new(),
                },
            },
            Command::ContextUpsertEntity {
                tenant_hash: TENANT,
                entity: ContextEntity {
                    entity_hash: 500 + node,
                    node_hash: node,
                    entity_type: 1,
                    name: format!("entity-{node}"),
                    value: format!("value-{node}"),
                    updated_at_ms: NOW_MS + node,
                    valid_from_ms: NOW_MS,
                    confidence: 1.0,
                    source_event_hashes: Vec::new(),
                    vector: Vec::new(),
                },
            },
            Command::ContextUpsertChildRef {
                tenant_hash: TENANT,
                child_ref: ContextChildRef {
                    parent_hash: node,
                    child_hash: 600 + node,
                    updated_at_ms: NOW_MS + node,
                },
            },
            Command::ContextUpsertSummary {
                tenant_hash: TENANT,
                summary: ContextSummary {
                    node_hash: node,
                    level: 1,
                    text: format!("level-1 summary of node {node}"),
                    valid_from_ms: NOW_MS + node,
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                },
            },
            Command::ContextWriteCompressionEvent {
                tenant_hash: TENANT,
                event: ContextCompressionEvent {
                    compression_id_hash: 700 + node,
                    node_hash: node,
                    source_start_ms: NOW_MS,
                    source_end_ms: NOW_MS + node,
                    compressed_time_ms: NOW_MS + node,
                    summary: format!("compressed node {node}"),
                },
            },
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(
                response.status.ok,
                "write node {node}: {:?}",
                response.status
            );
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // Every map named in `adopt_object_key_allocations`'s context half, with the address of each
    // key's characters. A `ModelKey` exposes `as_ptr()` through its deref, so this needs no
    // knowledge of what any map is keyed by -- which is what kept the first guard from going
    // stale when a map changed key type.
    let holders: Vec<(&str, Vec<(&str, usize)>)> = vec![
        (
            "context_nodes",
            shard
                .context_nodes
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_events_series",
            shard
                .context_events()
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_event_timeline",
            shard
                .context_event_timeline
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_indexes",
            shard
                .context_indexes
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_audits",
            shard
                .context_audits()
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_entities",
            shard
                .context_entities
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_children",
            shard
                .context_children()
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_summaries",
            shard
                .context_summaries()
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_compressions",
            shard
                .context_compressions()
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "context_compression_watermark",
            shard
                .context_compression_watermark
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
    ];

    // --- WHAT THIS RUN ACTUALLY FILED, printed before anything divides by it. ---
    #[derive(Default)]
    struct Tally {
        keys: usize,
        borrowed: usize,
        minted: usize,
        unfiled: usize,
    }
    let mut tally: std::collections::BTreeMap<&str, Tally> = std::collections::BTreeMap::new();
    let mut minted: Vec<(&str, &str)> = Vec::new();
    for (name, keys) in &holders {
        let row = tally.entry(name).or_default();
        row.keys = keys.len();
        for (text, ptr) in keys {
            match shard
                .bucket_index
                .object_block_lookup
                .any_shared_object_key(text)
            {
                None => row.unfiled += 1,
                Some(canonical) if canonical.as_ptr() as usize == *ptr => row.borrowed += 1,
                Some(_) => {
                    row.minted += 1;
                    minted.push((name, text));
                }
            }
        }
    }
    println!();
    println!("    map                              keys   borrowed   minted   no page entry");
    for (name, row) in &tally {
        println!(
            "    {name:<32} {:>4}   {:>8}   {:>6}   {:>13}",
            row.keys, row.borrowed, row.minted, row.unfiled
        );
    }

    // --- THE TWO MAPS THIS FIXTURE CANNOT SPEAK FOR, AND WHY. ---
    //
    // Each is excused for a reason that was MEASURED, and each excuse is checked below so that it
    // cannot outlive the fact it rests on.
    //
    // `context_nodes`: there is no production write to it at all. Grepped from the crate root for
    //   every `insert`, `entry(`, `remove`, `retain` and `clear` naming it: one test fixture in
    //   `storage_bucket_internals.rs`, and one `remove` in the expiry path. `ContextUpsertNode`
    //   files the node into `hashes` under `CONTEXT_NODE_FIELD`; this map is the PRE-HASHES
    //   location, still read so a stored image written before that change still serves, and filled
    //   only by `Deserialize`. Its sharing is therefore a decode-path question, which
    //   `adopt_canonical_key_allocations_in_hashed` and the reload tests answer, not a write-path
    //   one this fixture could ask.
    //
    // `context_entities`: the write arm files NO page entry under the key this map holds. The map
    //   is keyed by the node's COLLECTION key (`ctx:entity:{tenant}:{node}`) while the block is
    //   addressed per entity, so the staged outcome and the stored page entry both carry
    //   `ctx:entity:{tenant}:{node}:{entity_hash}`. `any_shared_object_key` on a collection key
    //   therefore answers None, there is no handle to borrow, and -- checked -- no other resident
    //   structure holds that text either, so the minted key is the ONLY copy rather than a second
    //   one. The `adopt_key_in` line for this map cannot fire in either direction.
    //
    // `context_compression_watermark`: its only writer is `maybe_auto_compress_context_node`, on
    //   the event-write path, behind `MATRIXARK_CONTEXT_COMPRESSION_ENABLED` --
    //   `ContextCompressionPolicy::default()` has `enabled: false`, so the trigger does not run.
    //   A default-run test cannot turn it on: `set_var` is process-global and every other test in
    //   this binary would inherit it. `Command::ContextCompressEvents` is a DIFFERENT path and
    //   files the series, not the watermark -- checked by putting it in this fixture and watching
    //   the map stay empty, which is why it is not here.
    const NOT_REACHABLE: &[&str] = &[
        "context_nodes",
        "context_entities",
        "context_compression_watermark",
    ];

    let nodes = tally.get("context_nodes").expect("named above");
    assert_eq!(
        0, nodes.keys,
        "`context_nodes` now holds {} keys after a write-only fixture, so the reason it is excused \
         -- that no production write files it -- has stopped being true. Move it into the checked \
         set rather than leaving it excused.",
        nodes.keys
    );

    let entities = tally.get("context_entities").expect("named above");
    assert_eq!(
        entities.keys,
        entities.unfiled,
        "{} of `context_entities`'s {} keys now HAVE a page entry, so the reason it is excused -- \
         that the block is addressed per entity while the map is keyed per collection -- has \
         stopped being true. It can borrow now, so move it into the checked set.",
        entities.keys - entities.unfiled,
        entities.keys
    );
    assert!(
        entities.keys > 0,
        "`context_entities` is empty, so the fixture stopped writing entities and neither the \
         exclusion above nor anything else was actually checked"
    );

    let watermark = tally
        .get("context_compression_watermark")
        .expect("named above");
    assert_eq!(
        0, watermark.keys,
        "`context_compression_watermark` now holds {} keys without the environment flag that          gates its only writer, so the reason it is excused has stopped being true. Move it into          the checked set.",
        watermark.keys
    );

    // --- DENOMINATORS for the maps that ARE checked. ---
    let checked: Vec<(&&str, &Tally)> = tally
        .iter()
        .filter(|(name, _)| !NOT_REACHABLE.contains(name))
        .collect();
    assert_eq!(
        7,
        checked.len(),
        "the checked set is {} maps, not the 7 this fixture drives -- a map was added to or \
         removed from the list above without the count following it",
        checked.len()
    );
    let empty: Vec<&&str> = checked
        .iter()
        .filter(|(_, row)| row.keys == 0)
        .map(|(name, _)| *name)
        .collect();
    assert!(
        empty.is_empty(),
        "these maps are EMPTY after one write per family, so this run could not have checked them \
         at all -- either the command that files one stopped doing so, or it was never in this \
         fixture: {empty:?}"
    );
    let unfiled: Vec<(&&str, usize)> = checked
        .iter()
        .filter(|(_, row)| row.unfiled > 0)
        .map(|(name, row)| (*name, row.unfiled))
        .collect();
    assert!(
        unfiled.is_empty(),
        "these maps hold keys with no page entry, so the adoption had no handle to take and this \
         run proves nothing about them: {unfiled:?}. A map that CANNOT borrow belongs in \
         NOT_REACHABLE with the reason, not in the checked set reporting a pass."
    );

    // --- THE INVARIANT. ---
    assert!(
        minted
            .iter()
            .all(|(name, _)| NOT_REACHABLE.contains(name)),
        "{} keys are a SECOND copy of characters the page index already holds -- the map's key and \
         the page index's handle report different addresses for the same text: {:?}. A map keyed \
         by a `ModelKey` must take its handle from \
         `object_block_lookup::any_shared_object_key`, which is what \
         `ShardState::adopt_object_key_allocations` does; nothing else fails when it does not, \
         because the data is correct either way.",
        minted.len(),
        &minted[..minted.len().min(4)]
    );
}

/// ONE FIELD NAME, ONE ALLOCATION -- across every object that uses it, and across a decode.
///
/// # WHY THE FIELD NAME NEEDS ITS OWN GUARD
///
/// Every other test in this file is about the object KEY. A hash's FIELD name is a second axis of
/// the same invariant and it behaves differently in the way that matters: the product write path
/// files every context node under ONE constant, `CONTEXT_NODE_FIELD` (`"meta"`), so the question
/// is not whether two maps agree on a key but whether ten thousand objects agree on one
/// four-character name. Before the conversion they did not -- one allocation per object, measured
/// at 32.0 bytes and one allocation per field in
/// `what_the_one_constant_field_name_costs_at_the_shape_the_product_writes`.
///
/// # AND THE DECODE HALF IS THE HALF THAT WOULD HAVE BEEN MISSED
///
/// The wire shape is a map of OWNED names -- that is what keeps the index format from moving -- so
/// a decode mints a fresh handle per field with the pool taking no part in it. A guard that only
/// looked at a freshly written shard would pass while every recovered shard held the duplication
/// back, with nothing failing, because the names are equal either way. So this asserts the
/// invariant on both sides of a dump and install, and asserts that the decode pass RAN -- a
/// sharing mechanism that is wired and never fires looks exactly like one that works.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn one_field_name_is_one_allocation_across_objects_and_across_a_decode() {
    use crate::types::ContextNode;

    const NODES: u64 = 16;
    const TENANT: u64 = 7;
    const NOW_MS: u64 = 1_787_270_070_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    for node in 1..=NODES {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash: node,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: format!("node-{node}"),
                    l0: format!("summary of node {node}"),
                    status: 0,
                    last_event_time_ms: NOW_MS,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        });
        assert!(response.status.ok, "node {node}: {:?}", response.status);
    }

    // How many DISTINCT addresses hold each field text, over every hash in the shard. A text in one
    // allocation reports 1 however many objects carry it; a text each object owns reports one per
    // object. Contents cannot tell those apart.
    let per_text = |engine: &TemporalEngine| -> std::collections::BTreeMap<String, usize> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let mut addresses: std::collections::BTreeMap<String, std::collections::HashSet<usize>> =
            std::collections::BTreeMap::new();
        for (_, fields) in shard.hashes.iter() {
            for name in fields.keys() {
                addresses
                    .entry(name.to_string())
                    .or_default()
                    .insert(name.as_ptr() as usize);
            }
        }
        addresses
            .into_iter()
            .map(|(text, ptrs)| (text, ptrs.len()))
            .collect()
    };

    let field = crate::engine::constants::CONTEXT_NODE_FIELD;

    // --- DENOMINATORS, because every claim below divides by them. ---
    let fresh = per_text(&engine);
    println!();
    println!("  fresh:  {fresh:?}");
    let total_fields: usize = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.hashes.iter().map(|(_, fields)| fields.len()).sum()
    };
    assert_eq!(
        NODES as usize, total_fields,
        "the fixture wrote {NODES} nodes but the shard holds {total_fields} hash fields, so this \
         is not the one-field-per-node shape the product write path produces: {fresh:?}"
    );
    assert!(
        fresh.contains_key(field),
        "no field is named {field:?}, which is the constant `write_context_node` files under, so \
         this guard is measuring something else: {fresh:?}"
    );

    // --- THE INVARIANT, ON THE WRITE PATH. ---
    assert_eq!(
        Some(&1usize),
        fresh.get(field),
        "{NODES} objects share the field name {field:?} and it is held in {:?} separate \
         allocations. One per object is what the pooled handle removed, and nothing fails when it \
         comes back, because the names are equal either way",
        fresh.get(field)
    );

    // --- AND ACROSS A DECODE, which mints its own handles by construction. ---
    let calls_before =
        crate::engine::state::REPACK_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let adopted_before =
        crate::engine::state::REPACK_ADOPTED_FIELDS.load(std::sync::atomic::Ordering::Relaxed);
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("the dump manifest persists");
    engine
        .install_bucket_dump_manifest(&manifest)
        .expect("the dump manifest installs");
    let calls_after = crate::engine::state::REPACK_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let adopted_after =
        crate::engine::state::REPACK_ADOPTED_FIELDS.load(std::sync::atomic::Ordering::Relaxed);

    let after = per_text(&engine);
    println!("  after:  {after:?}");
    println!(
        "  decode pass ran {} time(s); field names adopted: {}",
        calls_after - calls_before,
        adopted_after - adopted_before
    );

    // The install has to have gone through a decode, or the assertion below is about the shard the
    // write path left and says nothing about recovery.
    assert!(
        calls_after > calls_before,
        "the install did not run the decode pass, so nothing here measures what a recovery holds"
    );
    let total_after: usize = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.hashes.iter().map(|(_, fields)| fields.len()).sum()
    };
    assert_eq!(
        NODES as usize, total_after,
        "the decode served {total_after} hash fields for {NODES} nodes, so the recovery lost \
         or duplicated rows and the allocation count below would be about the wrong \
         population: {after:?}"
    );
    assert_eq!(
        Some(&1usize),
        after.get(field),
        "after a dump and install the field name {field:?} is held in {:?} separate allocations. \
         The wire shape is a map of owned names, so a decode mints one handle per field; \
         `adopt_element_names` on the decode pass points them back at the pool, and this is what \
         fails when that pass stops running, or runs before the pool it borrows from exists",
        after.get(field)
    );

    // AND THE ORDER THE ADOPTION MUST NOT DISTURB, checked rather than argued. `HashFieldMap`
    // keeps its entries sorted by field name and every lookup in it is a binary search, so an
    // adoption that re-pointed a name and left the vector unsorted would not fail here -- it
    // would fail later, as a field that is present and cannot be found. A handle to an equal
    // string sorts identically, which is why this holds; reasoning of that shape is worth one
    // assertion, and `adopt_field_names`'s own doc says this caller makes it.
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let unsorted: Vec<String> = shard
            .hashes
            .iter()
            .filter(|(_, fields)| !fields.is_sorted_by_field())
            .map(|(key, _)| key.to_string())
            .collect();
        assert!(
            unsorted.is_empty(),
            "the field map of {} object(s) is no longer in field-name order after the adoption, \
             so a binary search in it can miss a field that is present: {unsorted:?}",
            unsorted.len()
        );
    }
}

/// THE EVENT SERIES IS KEYED BY EVENT ID, NOT BY TIMELINE KEY -- and the auto-compression trigger
/// read it as though it were the other one.
///
/// # THE BUG THIS PINS
///
/// The write arm files two things per context event, into two maps, under two different key spaces:
///
///   `context_events_series`   point key = `event_id_hash`
///   `context_event_timeline`  key = `context_timeline_key(event_time_ms, ..)` -> event id
///
/// `maybe_auto_compress_context_node` built its event times from the FIRST map and divided its keys
/// by `CONTEXT_TIMELINE_FANOUT`, naming the variable `timeline_key`. An event id hash below the
/// fanout divides to ZERO, so a node's whole history read as one repeated instant. The planner then
/// takes the events strictly newer than the watermark; with every time 0 and the watermark 0 that
/// slice is empty, so it returned None and the trigger never fired -- at any event count, for any
/// node, on any store. It was invisible because the policy also defaults off, so nothing reached
/// the line to be wrong.
///
/// # WHY THIS TEST IS PURE
///
/// The engine reads its policy from the environment, so an end-to-end check has to set a
/// process-global and is `#[ignore]`d. This needs none of that: it builds both key spaces exactly
/// as the write arm does and asks the planner the same question twice. It runs on a plain
/// `cargo test`, which is what the dead path lacked.
///
/// rust-internal: exercises this crate's own key encoding, no product behaviour
#[test]
fn the_event_series_is_keyed_by_event_id_not_by_timeline_key() {
    use crate::engine::constants::CONTEXT_TIMELINE_FANOUT;
    use crate::engine::context::{context_timeline_key, plan_next_compression_window,
        ContextCompressionPolicy};

    const EVENTS: u64 = 120;
    const NOW_MS: u64 = 1_787_270_070_000;

    let policy = ContextCompressionPolicy {
        enabled: true,
        ..ContextCompressionPolicy::default()
    };

    // The two key spaces, built as the write arm builds them.
    let event_ids: Vec<u64> = (0..EVENTS).map(|index| 1_000 + index).collect();
    let timeline_keys: Vec<u64> = (0..EVENTS)
        .map(|index| context_timeline_key(NOW_MS + index * 1_000, 1_000 + index))
        .collect();

    // WHAT THE OLD READ PRODUCED: every id below the fanout divides to zero.
    let from_series: Vec<u64> = event_ids
        .iter()
        .map(|id| id / CONTEXT_TIMELINE_FANOUT)
        .collect();
    // WHAT THE TIMELINE PRODUCES: the times back, in order.
    let from_timeline: Vec<u64> = timeline_keys
        .iter()
        .map(|key| key / CONTEXT_TIMELINE_FANOUT)
        .collect();

    println!();
    println!("  {EVENTS} events, keep {}", policy.keep_recent_events);
    println!(
        "    from the SERIES   : {} distinct times, first {:?}",
        {
            let mut seen = from_series.clone();
            seen.dedup();
            seen.len()
        },
        &from_series[..3]
    );
    println!(
        "    from the TIMELINE : {} distinct times, first {:?}",
        {
            let mut seen = from_timeline.clone();
            seen.dedup();
            seen.len()
        },
        &from_timeline[..3]
    );

    // --- THE KEY SPACES ARE NOT INTERCHANGEABLE, which is the whole finding. ---
    assert!(
        from_series.iter().all(|time| *time == 0),
        "an event id no longer divides to zero under the fanout, so the specific failure this \
         pins has changed shape -- re-read the trigger before trusting it: {:?}",
        &from_series[..5]
    );
    assert_eq!(
        EVENTS as usize,
        {
            let mut seen = from_timeline.clone();
            seen.dedup();
            seen.len()
        },
        "the timeline keys no longer recover one distinct time per event, so the fanout encoding \
         has moved and the trigger's division is wrong again"
    );
    assert!(
        from_timeline.windows(2).all(|pair| pair[0] < pair[1]),
        "the timeline-derived times are not strictly ascending, and \
         `plan_next_compression_window` requires sorted input"
    );

    // --- AND THE PLANNER'S ANSWER TO EACH, WHICH IS WHY THE TRIGGER WAS DEAD. ---
    let planned_from_series = plan_next_compression_window(&policy, &from_series, 0, NOW_MS);
    assert!(
        planned_from_series.is_none(),
        "the planner found a window in the series-derived times. It should not: with every time 0 \
         and the watermark 0, the events strictly newer than the watermark are none. If this now \
         answers Some, the planner's pending slice has changed and the trigger's old read was not \
         the only thing broken"
    );

    let planned = plan_next_compression_window(&policy, &from_timeline, 0, NOW_MS)
        .expect("the planner must find a window in the timeline-derived times");
    println!(
        "    planner on timeline times: start={} end={} count={}",
        planned.start_ms, planned.end_ms, planned.count
    );
    assert_eq!(
        NOW_MS, planned.start_ms,
        "the window does not start at the oldest event, so the fold is not taking from the old end"
    );
    assert_eq!(
        policy.window_size.min(EVENTS as usize - policy.keep_recent_events),
        planned.count,
        "the window is not bounded by `window_size`, so the one-window-per-call bound is not holding"
    );
}

/// THE AUTO-COMPRESSION TRIGGER DOES WHAT ITS DOC CLAIMS, now that it runs by default.
///
/// # WHY THIS TEST EXISTS AT ALL
///
/// Nothing tested this path. No test in the tree set `MATRIXARK_CONTEXT_COMPRESSION_ENABLED`, and
/// no test was named for auto-compression -- the only other mention of the flag was a comment in
/// this very file explaining why `context_compression_watermark` is unreachable WHILE the flag is
/// off. So when the default moved, the three claims the policy's doc makes had never been checked
/// by anything. Each is checked here:
///
///   1  IT FIRES ONLY PAST THE THRESHOLD. `max_raw_events_per_node` is 64, so a node with fewer
///      raw events is untouched. That is what keeps the default from acting on an ordinary node,
///      and it is why the sibling guard in this file still measures an empty watermark map.
///   2  IT KEEPS THE RECENT RAW EVENTS. `keep_recent_events` is 32, so the fold takes from the
///      OLD end and leaves the newest alone.
///   3  IT IS NON-DESTRUCTIVE. The doc says "raw events remain queryable/replayable (physical GC
///      stays a separate concern)". That is the claim with the most riding on it -- a compression
///      that quietly dropped what it folded would be data loss wearing the name of a retention
///      policy -- so the raw events are read back AFTER the fold and counted.
///
/// # WHY THIS ONE IS IGNORED AND THE GUARD ABOVE IS NOT
///
/// The engine reads its compression policy from the environment, and the policy defaults OFF --
/// for a measured reason, recorded beside the default. So driving the trigger end to end means
/// setting a process-global variable, which every other test in this binary would inherit. That
/// is the same hazard the allocation probes carry and it gets the same treatment: run by name,
/// and alone.
///
/// `the_event_series_is_keyed_by_event_id_not_by_timeline_key` above needs none of that, which
/// is why it is the standing guard and this is the demonstration.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[ignore = "sets MATRIXARK_CONTEXT_COMPRESSION_ENABLED, which is process-global; run by name, \
            and ALONE"]
fn the_auto_compression_trigger_folds_a_window_and_keeps_the_raw_events() {
    use crate::types::{CommandResponse, ContextEvent, ContextNode};

    const TENANT: u64 = 7;
    const NODE: u64 = 1;
    const NOW_MS: u64 = 1_787_270_070_000;
    // Past `max_raw_events_per_node` (64) so the trigger has a window to fold, and past
    // `keep_recent_events` (32) by enough that the fold leaves the recent ones alone.
    const EVENTS: u64 = 120;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // TURN IT ON, AND READ IT BACK OUT OF THE RUN rather than assuming the variable took. A
    // guessed or mistyped setting leaves the arm silently not running, and a null result then
    // reads as a refutation of the thing being measured.
    std::env::set_var("MATRIXARK_CONTEXT_COMPRESSION_ENABLED", "1");
    let policy = crate::engine::context::context_compression_policy_from_env();
    assert!(
        policy.enabled,
        "setting MATRIXARK_CONTEXT_COMPRESSION_ENABLED did not enable the policy, so this test \
         would pass by doing nothing"
    );
    assert!(
        EVENTS > policy.max_raw_events_per_node as u64,
        "the fixture writes {EVENTS} events and the threshold is {}, so the trigger would never \
         fire and nothing below is about compression",
        policy.max_raw_events_per_node
    );

    let upsert = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node: Box::new(ContextNode {
                node_hash: NODE,
                parent_hash: 0,
                kind: 1,
                canonical_name: "compressed-node".to_string(),
                l0: String::new(),
                status: 0,
                last_event_time_ms: NOW_MS,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: Vec::new(),
                embedding_model_hash: 0,
                embedding_updated_at_ms: 0,
                summary_vector: Vec::new(),
                summary_vector_valid_from_ms: 0,
                summary_vector_model_hash: 0,
            }),
        },
    });
    assert!(upsert.status.ok, "the node upsert failed: {:?}", upsert.status);

    for index in 0..EVENTS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: Box::new(ContextEvent {
                    event_id_hash: 1_000 + index,
                    event_time_ms: NOW_MS + index * 1_000,
                    ingestion_time_ms: NOW_MS + index * 1_000,
                    kind: 0,
                    event_type: 1,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 1.0,
                    importance: 1.0,
                    text: format!("event {index}"),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                }),
                first_write_only: false,
                cold_storage: false,
            },
        });
        assert!(response.status.ok, "event {index}: {:?}", response.status);
    }

    // --- WHAT THE TRIGGER LEFT BEHIND. ---
    let (watermark, compressions, raw_points) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let event_key = format!("ctx:event:{TENANT}:{NODE}");
        let watermark = shard
            .context_compression_watermark
            .get(&event_key[..])
            .copied();
        let compressions: usize = shard
            .context_compressions()
            .values()
            .map(|series| series.owned_element_count())
            .sum();
        let raw_points = shard
            .context_events()
            .get(&event_key[..])
            .map(|series| series.len())
            .unwrap_or(0);
        (watermark, compressions, raw_points)
    };

    println!();
    println!("  {EVENTS} raw events on one node, threshold {} keep {}",
        policy.max_raw_events_per_node, policy.keep_recent_events);
    println!("    compression records written : {compressions}");
    println!("    watermark                   : {watermark:?}");
    println!("    raw event points still held : {raw_points}");

    // --- CLAIM 1: IT FIRED. ---
    assert!(
        compressions > 0,
        "no compression record was written for a node {EVENTS} events past a threshold of {}, so \
         the trigger does not fire even with the policy on -- the state this fix corrected, so it has regressed",
        policy.max_raw_events_per_node
    );
    assert!(
        watermark.is_some(),
        "the compression watermark was never set, so a later round cannot tell which window it \
         already folded and the fold is not idempotent across calls"
    );

    // --- CLAIM 2: IT IS BOUNDED, NOT A SWEEP. ---
    //
    // The doc says "Bounded to one window per call, so writes stay light". `window_size` is 32, so
    // {EVENTS} events cannot have been folded into one record -- and if they had, the bound would
    // be the thing that broke.
    // AND THE FIGURE THAT DECIDED THE DEFAULT, asserted so it cannot drift out of the prose
    // beside `ContextCompressionPolicy::default`. One record per write once the node is past
    // `keep_recent_events`: measured 88 at 120 events and 168 at 200, both exactly
    // `EVENTS - keep_recent_events`. The fold is non-destructive, so that state is ADDED to the
    // raw events rather than exchanged for them -- which is why this is a retention policy a
    // deployment may want and not a footprint reduction to default on.
    assert_eq!(
        EVENTS as usize - policy.keep_recent_events,
        compressions,
        "the record count is no longer `events - keep_recent_events`. That relationship is the \
         cost argument recorded beside the policy default, so if it has changed, that argument \
         needs re-reading rather than this assertion needs updating"
    );

    // --- CLAIM 3: NON-DESTRUCTIVE, WHICH IS THE ONE THAT MATTERS. ---
    assert_eq!(
        EVENTS as usize, raw_points,
        "the raw event series holds {raw_points} of {EVENTS} points after the fold. The policy's \
         doc says raw events remain queryable and replayable, with physical reclaim a separate \
         concern -- so a fold that removed them is data loss wearing the name of a retention \
         policy, not the behaviour this default was turned on for"
    );

    // AND READ BACK THROUGH THE QUERY, not just off the resident map, because what a caller sees
    // is the claim. The resident series could be intact while the read path filtered the folded
    // range out, and that would be the same loss from the caller's side.
    let served = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            start_time_ms: NOW_MS,
            end_time_ms: NOW_MS + EVENTS * 1_000 + 1,
            limit: Some(EVENTS as usize * 2),
            max_scan: Some(EVENTS as usize * 4),
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.0,
            min_importance: 0.0,
        },
    });
    assert!(served.status.ok, "the event query failed: {:?}", served.status);
    let served_count = match served.response {
        CommandResponse::ContextEvents { ref events, .. } => events.len(),
        ref other => panic!("the query answered with {other:?} rather than events"),
    };
    println!("    raw events served by a query: {served_count}");
    assert_eq!(
        EVENTS as usize, served_count,
        "a query over the whole window served {served_count} of {EVENTS} events after the fold, so \
         the compression is destructive FROM THE CALLER'S SIDE even though the resident series \
         still holds the points"
    );
}
