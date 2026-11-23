// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The maintained slab tally must cover the model maps and retained deletion markers.
//!
//! Model maps expose visible data. The bucket index also retains tombstones, which
//! may still need their blocks after all visible data has moved off a slab.
//! Compare the exact union, then compare the tally's per-slab counts and bytes
//! with the bucket index before reconciliation can hide an accounting error.
//!
//! Each model goes through writes, removal, compaction, dump, release, reload,
//! restart, and final deletion. A model without live records in the fixture is
//! called out explicitly.
//!
//! PLANTED FAULTS (the gate must go red): a tally that forgot a live slab, and a tally that kept
//! a slab nothing references.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{CommandResponse, ControlStateFamily, ControlStateSelectionType, SequenceFeatureRow};

fn open(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> bool {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    let _: &CommandResponse = &response.response;
    response.status.ok
}

fn must(engine: &TemporalEngine, what: &str, command: Command) {
    assert!(run(engine, command), "{what} was refused");
}

/// The comparison. Walk first (read only), then the tally, then the drift check (which CORRECTS
/// the tally, so it must come last or it would hide a disagreement from the set comparison).
fn agree(engine: &TemporalEngine, model: &str, stage: &str) -> Vec<u64> {
    let walked = engine.live_block_slab_ids_all_shards();
    let tombstones: BTreeSet<_> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        crate::engine::storage_bucket_internals::collect_live_block_entries(&shards[&1])
            .into_iter()
            .filter(|entry| entry.deleted)
            .map(|entry| entry.address.block_slab_id())
            .collect()
    };
    let expected: Vec<_> = walked.iter().copied()
        .chain(tombstones.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(walked, expected,
        "{model} at {stage}: the ownership walk must include retained removal blocks");
    let tallied = engine.live_block_slab_ids_all_shards_from_tally();
    assert_eq!(
        tallied.as_ref(),
        Some(&expected),
        "{model} at {stage}: slab tally differs from visible blocks {walked:?} \
         and retained deletion markers {tombstones:?}"
    );
    let drift = engine.block_slab_live_drift_check(1);
    assert!(drift.is_clean(), "{model} at {stage}: the tally drifted from the index walk: {drift:?}");
    walked
}

struct Model {
    name: &'static str,
    /// Write generation `g` of object `i`.
    put: fn(u64, u8) -> Command,
    /// Remove just what `put(i, g)` added, if the model has an element-level remove.
    unput: Option<fn(u64, u8) -> Command>,
    /// Remove object `i` whole.
    wipe: fn(u64) -> Command,
}

fn key(prefix: &str, i: u64) -> String {
    format!("{prefix}-{i}")
}

fn context_json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).expect("context record")
}

fn models() -> Vec<Model> {
    vec![
        Model {
            name: "string",
            put: |i, g| Command::StringSet { key: key("s", i), value: vec![g; 64] },
            unput: Some(|i, _| Command::StringDelete { key: key("s", i) }),
            wipe: |i| Command::CommonDelete { key: key("s", i) },
        },
        Model {
            name: "hash",
            put: |i, g| Command::HashSet { key: key("h", i), field: format!("f{g}"), value: vec![g; 64] },
            unput: Some(|i, g| Command::HashDelete { key: key("h", i), field: format!("f{g}") }),
            wipe: |i| Command::CommonDelete { key: key("h", i) },
        },
        Model {
            name: "set",
            put: |i, g| Command::SetAdd { key: key("st", i), member: vec![g; 48] },
            unput: Some(|i, g| Command::SetRemove { key: key("st", i), member: vec![g; 48] }),
            wipe: |i| Command::CommonDelete { key: key("st", i) },
        },
        Model {
            name: "zset",
            put: |i, g| Command::ZSetAdd { key: key("z", i), member: vec![g; 48], score: g as f64 },
            unput: Some(|i, g| Command::ZSetRemove { key: key("z", i), member: vec![g; 48] }),
            wipe: |i| Command::CommonDelete { key: key("z", i) },
        },
        Model {
            name: "list",
            put: |i, g| Command::ListPush { key: key("l", i), member: vec![g; 48], left: true },
            unput: Some(|i, _| Command::ListPop { key: key("l", i), left: true }),
            wipe: |i| Command::CommonDelete { key: key("l", i) },
        },
        Model {
            name: "feature",
            put: |i, g| Command::FeatureAppend {
                key: key("ft", i),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_000_000 + u64::from(g) * 1_000,
                    value: vec![g; 48],
                }],
            },
            unput: None,
            wipe: |i| Command::FeatureDelete { key: key("ft", i) },
        },
        Model {
            name: "sequence",
            put: |i, g| Command::SequenceAdd {
                key: key("sq", i),
                rows: vec![SequenceFeatureRow {
                    timestamp_ms: 1_787_270_000_000 + u64::from(g) * 1_000,
                    gid: u64::from(g),
                    action_type: 1,
                    duration: 5,
                    author_id: i,
                }],
            },
            unput: None,
            wipe: |i| Command::FeatureDelete { key: key("sq", i) },
        },
        Model {
            name: "control_state_counter",
            put: |i, g| Command::ControlStateIncrement {
                key: key("cs", i),
                timestamp_ms: 1_787_270_000_000 + u64::from(g) * 1_000,
                amount: 1,
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("cs", i) },
        },
        Model {
            name: "control_state_distinct",
            put: |i, g| Command::ControlStateSet {
                family: ControlStateFamily::Distinct,
                key: key("cd", i),
                timestamp_ms: 1_787_270_000_000 + u64::from(g) * 1_000,
                amount: i64::from(g),
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("cd", i) },
        },
        Model {
            name: "control_state_selection",
            put: |i, g| Command::ControlStateSelectionSet {
                key: key("sel", i),
                value: vec![g; 48],
                occur_time_ms: 1_787_270_000_000 + u64::from(g) * 1_000,
                ttl_ms: 0,
                selection_type: ControlStateSelectionType::Last,
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("sel", i) },
        },
        Model {
            name: "context_node",
            put: |i, g| Command::ContextUpsertNode {
                tenant_hash: 7,
                node: Box::new(context_json(serde_json::json!({
                    "node_hash": 100 + i, "canonical_name": "node", "l0": format!("text-{g}")
                }))),
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-node", i) },
        },
        Model {
            name: "context_event",
            put: |i, g| Command::ContextWriteEvent {
                tenant_hash: 7,
                node_hash: 100 + i,
                first_write_only: false,
                cold_storage: false,
                event: Box::new(crate::types::ContextEvent {
                    event_id_hash: 1_000 * (i + 1) + u64::from(g),
                    event_time_ms: 1_787_270_000_000 + u64::from(g),
                    ingestion_time_ms: 1_787_270_000_000,
                    kind: 7,
                    event_type: 7,
                    actor_hash: 0,
                    status: 1,
                    valid_until_ms: 0,
                    confidence: 0.9,
                    importance: 0.5,
                    text: format!("event {i} generation {g}"),
                    source_ref: String::new(),
                    related_node_hashes: vec![100 + i],
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                }),
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-event", i) },
        },
        Model {
            name: "context_index",
            put: |i, g| Command::ContextWriteIndexRef {
                tenant_hash: 7,
                index_name: "actor".to_string(),
                index_value_hash: 77 + i,
                scope_hash: 3,
                event_time_ms: 1_787_270_000_000 + u64::from(g),
                index_ref: crate::types::ContextIndexRef {
                    primary_node_hash: 9,
                    primary_event_time_ms: 1_787_270_000_000 + u64::from(g),
                    event_id_hash: 1_234 + u64::from(g),
                },
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-index", i) },
        },
        Model {
            name: "context_audit",
            put: |i, g| Command::ContextWritePackAudit {
                tenant_hash: 7 + i,
                audit: crate::types::ContextPackAudit {
                    query_id: format!("q-{i}-{g}"),
                    session_hash: 5,
                    request_time_ms: 1_787_270_000_000 + u64::from(g),
                    query_hash: 6,
                    max_prompt_tokens: 100,
                    selected_tokens: 40,
                    selected_refs: Vec::new(),
                    blocked_refs: Vec::new(),
                },
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-audit", i) },
        },
        Model {
            name: "context_entity",
            put: |i, g| Command::ContextUpsertEntity {
                tenant_hash: 7,
                entity: context_json(serde_json::json!({
                    "entity_hash": 500 + i, "node_hash": 100 + i,
                    "name": format!("entity-{g}"), "updated_at_ms": 1_787_270_000_000u64 + u64::from(g)
                })),
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-entity", i) },
        },
        Model {
            name: "context_child",
            put: |i, g| Command::ContextUpsertChildRef {
                tenant_hash: 7,
                child_ref: crate::types::ContextChildRef {
                    parent_hash: 9 + i,
                    child_hash: 10 + u64::from(g),
                    updated_at_ms: 1_787_270_000_000 + u64::from(g),
                },
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-child", i) },
        },
        Model {
            name: "context_summary",
            put: |i, g| Command::ContextUpsertSummary {
                tenant_hash: 7,
                summary: crate::types::ContextSummary {
                    node_hash: 9 + i,
                    level: 1,
                    text: format!("summary {g}"),
                    valid_from_ms: 1_787_270_000_000 + u64::from(g),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                },
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-summary", i) },
        },
        Model {
            name: "context_compression",
            put: |i, g| Command::ContextWriteCompressionEvent {
                tenant_hash: 7,
                event: crate::types::ContextCompressionEvent {
                    compression_id_hash: 21 + i * 10 + u64::from(g),
                    node_hash: 9 + i,
                    source_start_ms: 1_787_270_000_000,
                    source_end_ms: 1_787_270_003_000,
                    compressed_time_ms: 1_787_270_004_000 + u64::from(g),
                    summary: format!("compressed {g}"),
                },
            },
            unput: None,
            wipe: |i| Command::CommonDelete { key: key("ctx-compression", i) },
        },
    ]
}

fn scenario(model: &Model) {
    let name = model.name;
    let dir = tempfile::tempdir().unwrap();
    let mut engine = open(dir.path());
    for i in 0..3u64 {
        must(&engine, "first write", (model.put)(i, 1));
    }
    let first = agree(&engine, name, "written");
    // A model whose first write leaves no block (written to the log only, or coalesced until it is
    // due) compares two empty sets: that proves nothing, so it is NAMED rather than passed quietly.
    // Only the models known to write a block at once are required to.
    const MAY_LEAVE_NO_BLOCK: &[&str] = &["control_state_selection"];
    if first.is_empty() {
        assert!(
            MAY_LEAVE_NO_BLOCK.contains(&name),
            "{name}: the workload left no live block, so this proves nothing"
        );
        println!("NOT COVERED (no block written): {name}");
        return;
    }
    println!("covered: {name} first={first:?}");

    // A second slab, so a disagreement about WHICH slab is visible and not only whether any is.
    engine.block_store().roll_slab().unwrap();
    must(&engine, "overwrite", (model.put)(0, 2));
    let rolled = agree(&engine, name, "after a roll and a second generation");
    // A control-state series writes its block only when due, so a second generation lands in no
    // new slab; the agreement above is still checked, only the two-slab expectation does not hold.
    if !name.starts_with("control_state") {
        assert!(rolled.len() >= 2, "{name}: expected two live slabs after the roll, got {rolled:?}");
    }

    if let Some(unput) = model.unput {
        let _ = run(&engine, unput(1, 1));
        agree(&engine, name, "after an element removal");
    }
    let _ = run(&engine, (model.wipe)(2));
    agree(&engine, name, "after a whole-key removal");

    // Maintenance arms: each rewrites or drops blocks without a client write, which is exactly
    // where a maintained tally can drift from the maps.
    let _ = engine.compact_shard_blocks(1);
    agree(&engine, name, "after a compaction");
    let _ = engine.dump_index_catalog(1);
    agree(&engine, name, "after an index dump");
    let (released_buckets, _, _) = engine.release_all_releasable_bucket_index_blocks(1);
    agree(&engine, name, "after a bucket release");
    println!("maintenance arms ran: {name} released_buckets={released_buckets}");

    engine.unload_shard(1);
    engine.load_shard(1);
    agree(&engine, name, "after unload and load");

    drop(engine);
    engine = open(dir.path());
    agree(&engine, name, "after a restart with no unload");

    for i in 0..3u64 {
        let _ = run(&engine, (model.wipe)(i));
    }
    agree(&engine, name, "after everything was removed");
}

#[test]
fn the_tally_names_the_slabs_the_walk_does_in_every_model() {
    for model in models() {
        scenario(&model);
    }
}

#[test]
fn reclaim_keeps_a_removal_block_after_compaction_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for field in ["kept", "removed"] {
        must(&engine, "write field", Command::HashSet {
            key: "owner".into(), field: field.into(), value: field.as_bytes().to_vec(),
        });
    }
    engine.block_store().roll_slab().unwrap();
    must(&engine, "remove field", Command::HashDelete {
        key: "owner".into(), field: "removed".into(),
    });
    let removal = {
        let shards = engine.shards.read().unwrap();
        let mut markers = collect_live_block_entries(shards.get(&1).unwrap()).into_iter()
            .filter(|entry| entry.deleted && entry.kind == "hash" && entry.object_key.as_ref() == "owner");
        let marker = markers.next().expect("the removal must leave a physical marker");
        assert!(markers.next().is_none(), "the fixture must hold exactly one removal marker");
        marker.address
    };
    let payload = engine.block_store.read(&removal).unwrap();
    assert!(!payload.is_empty(), "the marker must contain a removal payload");
    engine.compact_shard_blocks(1).expect("compaction must finish");
    {
        let shards = engine.shards.read().unwrap();
        assert!(collect_model_live_block_entries(shards.get(&1).unwrap()).iter()
            .all(|entry| entry.address.block_slab_id() != removal.block_slab_id()),
            "the marker must occupy a slab no serving-map entry retains");
    }
    let retained = engine.live_block_slab_ids_all_shards();
    assert!(retained.contains(&removal.block_slab_id()), "GC must retain the removal-only slab");
    assert_eq!(engine.live_block_slab_ids_all_shards_from_tally(), Some(retained.clone()));
    let newest = *engine.block_store.slab_ids().unwrap().iter().max().unwrap();
    engine.block_store.gc_slabs_before_with_live_refs_delayed_destroy(newest, retained.clone()).unwrap();
    engine.block_store.purge_delayed_destroy_slabs_selected(0, retained, None).unwrap();
    assert_eq!(engine.block_store.read(&removal).unwrap(), payload);
    drop(engine);

    let reopened = open(dir.path());
    assert_eq!(reopened.block_store.read(&removal).unwrap(), payload,
        "a cold reader must still find the physical removal marker after reclaim");
    for (field, expected) in [("kept", Some(b"kept".to_vec())), ("removed", None)] {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1, command: Command::HashGet { key: "owner".into(), field: field.into() },
        });
        assert!(response.status.ok);
        match response.response {
            CommandResponse::Bytes { value } => assert_eq!(value, expected),
            other => panic!("unexpected hash response: {other:?}"),
        }
    }
}

/// PLANTED FAULT 1: a tally that forgot a slab holding live blocks. The comparison above must
/// see it.
#[test]
fn a_tally_that_forgot_a_live_slab_is_seen() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    must(&engine, "write", Command::SetAdd { key: "p".into(), member: vec![1; 64] });
    let live = agree(&engine, "plant", "before the fault");
    assert!(!live.is_empty());
    engine.forget_tally_slab_for_test(1, live[0]);
    let walked = engine.live_block_slab_ids_all_shards();
    let tallied = engine.live_block_slab_ids_all_shards_from_tally();
    assert_ne!(tallied.as_ref(), Some(&walked), "the differential did not see a forgotten slab");
    assert!(!engine.block_slab_live_drift_check(1).is_clean());
}

/// PLANTED FAULT 2: a tally that holds a slab nothing references.
#[test]
fn a_tally_that_kept_a_dead_slab_is_seen() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    must(&engine, "write", Command::SetAdd { key: "p".into(), member: vec![1; 64] });
    agree(&engine, "plant", "before the fault");
    engine.block_store().roll_slab().unwrap();
    let orphan = engine.block_store().append(&[9; 64]).unwrap();
    engine
        .shards
        .write()
        .unwrap()
        .get_mut(&1)
        .unwrap()
        .bucket_index
        .block_slab_live
        .add_address(&orphan);
    let walked = engine.live_block_slab_ids_all_shards();
    let tallied = engine.live_block_slab_ids_all_shards_from_tally();
    assert_ne!(tallied.as_ref(), Some(&walked), "the differential did not see an extra slab");
    assert!(!engine.block_slab_live_drift_check(1).is_clean());
}
