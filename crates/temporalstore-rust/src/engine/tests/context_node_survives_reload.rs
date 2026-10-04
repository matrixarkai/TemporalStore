// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DOES A CONTAINER-ONLY ELEMENT SURVIVE THE RECONCILE? Asked of the one element the engine
//! deliberately keeps in `shard.hashes` and NOT in the bucket index.
//!
//! A context node is written through `install_element_staged_only::<HashKind>` by
//! `execute_on_shard.rs`, whose own comment says it "writes a hash block and -- unlike HashSet --
//! never registers it in the bucket index". The replay arm at `lifecycle.rs` says the same of
//! itself and installs through `replay_install_element`, the exception only `HashKind` declares.
//!
//! So the element exists in the resident map and nowhere in the index. On a load,
//! `rebuild_unserialized_model_maps_from_bucket_index` derives its base map FROM the index, builds
//! the live-address set FROM index entries, and `fill_absent_elements` drops any persisted element
//! whose address is not in that set -- counting it as `resurrections_refused`. The model-map
//! supplement that would put the address back is gated on `!released_buckets.is_empty()`.
//!
//! That predicts the node is DISCARDED on the first load after it is written. This test asks.

use crate::engine::TemporalEngine;
use crate::types::Command;

const NARROW_END: u32 = 1023;

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
        table_name: "cnsr".to_string(),
        shard_uri: "local://cnsr/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
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

fn node_at(node_hash: u64) -> crate::types::ContextNode {
    crate::types::ContextNode {
        node_hash,
        parent_hash: 0,
        kind: 1,
        canonical_name: format!("node_{node_hash:08}"),
        l0: "a node".to_string(),
        status: 0,
        last_event_time_ms: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    }
}

fn run(engine: &TemporalEngine, commands: Vec<Command>) {
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(response.status.ok, "the batch must ack: {:?}", response.status);
}

/// Does `ContextGetNode` answer for this node?
fn node_is_served(engine: &TemporalEngine, node_hash: u64) -> bool {
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands: vec![Command::ContextGetNode {
            tenant_hash: 7,
            node_hash,
        }],
    });
    assert!(
        response.status.ok,
        "the read itself must ack: {:?}",
        response.status
    );
    // A served node comes back as a payload; an absent one comes back empty. Asking the RESPONSE
    // rather than an exit status, and printed either way so the verdict is the thing measured.
    let served = response.responses.iter().any(|r| {
        matches!(
            r.response,
            crate::types::CommandResponse::ContextNode { node: Some(_), .. }
        )
    });
    println!("[probe] node {node_hash} served={served} raw={:?}", response.responses);
    served
}

/// Does `HashGet` answer for this field? The CONTROL: a hash field written through `HashSet` IS
/// registered in the bucket index, so it must survive the same reload the node is asked about.
/// Without it, a reload that loses everything would look exactly like the defect being probed.
fn field_is_served(engine: &TemporalEngine, key: &str, field: &str) -> bool {
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands: vec![Command::HashGet {
            key: key.to_string(),
            field: field.to_string(),
        }],
    });
    assert!(
        response.status.ok,
        "the control read must ack: {:?}",
        response.status
    );
    let served = response.responses.iter().any(|r| {
        matches!(
            &r.response,
            crate::types::CommandResponse::Bytes { value: Some(bytes) } if !bytes.is_empty()
        )
    });
    println!("[probe] control field {key}/{field} served={served}");
    served
}

#[test]
fn a_context_node_is_still_served_after_an_unload_and_load() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine);

    // A HashSet field FIRST, for two reasons. It is the control below, and it guarantees
    // `bucket_index.bucket_map` is non-empty -- which is the only thing that stops
    // `rebuild_unserialized_model_maps_from_bucket_index` returning early. A store holding only a
    // context node would skip the reconcile and pass this test for the wrong reason.
    run(
        &engine,
        vec![Command::HashSet {
            key: "control-hash".to_string(),
            field: "f-0".to_string(),
            value: b"control-value".to_vec(),
        }],
    );
    run(
        &engine,
        vec![Command::ContextUpsertNode {
            tenant_hash: 7,
            node: Box::new(node_at(1)),
        }],
    );

    // PRECONDITION, asserted rather than assumed: if the node is not served BEFORE the reload the
    // fixture never created the state this test is about.
    assert!(
        node_is_served(&engine, 1),
        "the node was not served before any reload, so this fixture cannot ask its question"
    );
    assert!(
        field_is_served(&engine, "control-hash", "f-0"),
        "the control field was not served before any reload either -- the fixture is wrong, not the engine"
    );

    engine.unload_shard(1);
    load_on(&engine);

    // THE CONTROL FIRST. If this fails the reload lost everything and the node tells us nothing.
    let control_after = field_is_served(&engine, "control-hash", "f-0");
    let node_after = node_is_served(&engine, 1);

    assert!(
        control_after,
        "the CONTROL hash field did not survive the reload, so this run cannot attribute anything \
         to the container-only path -- fix the fixture before reading the node result"
    );
    assert!(
        node_after,
        "THE CONTAINER-ONLY ELEMENT WAS DISCARDED. The context node was served before the reload \
         and is not served after it, while a HashSet field written to the same shard survived. \
         That is the live-address refusal in `fill_absent_elements`: the node's block is never \
         registered in the bucket index, so the derived base cannot name it and the live set \
         cannot hold its address, and the reconcile drops it."
    );
}

/// WHY DID IT SURVIVE? The question the first test forces, because its prediction was wrong.
///
/// Three candidates, and they have different consequences for the per-element collapse:
///   1. the node's block IS in the bucket index after all, so the derived base names it;
///   2. the reconcile is not on this load path at all, so nothing filters the container;
///   3. the reconcile DOES drop it and the WAL replay puts it back afterwards.
///
/// This asks the index directly, then runs the reconcile BY HAND against the loaded shard and
/// looks at the map on both sides of it. Calling the function by name is the whole point: it
/// separates "the filter is harmless" from "the filter is not reached on this path".
#[test]
fn whether_the_reconcile_keeps_a_container_only_element_when_it_is_run_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine);

    run(
        &engine,
        vec![Command::HashSet {
            key: "control-hash".to_string(),
            field: "f-0".to_string(),
            value: b"control-value".to_vec(),
        }],
    );
    run(
        &engine,
        vec![Command::ContextUpsertNode {
            tenant_hash: 7,
            node: Box::new(node_at(1)),
        }],
    );

    let node_key = "ctx:node:7:1";
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");

    // 1. IS THE NODE IN THE BUCKET INDEX? Asked of the index itself rather than of a comment.
    let mut index_entries_for_node = Vec::new();
    let mut index_entries_for_control = 0usize;
    for (_bucket_id, bucket) in &shard.bucket_index.bucket_map {
        for page in bucket.block_index.values() {
            if page.object_key.as_ref() == node_key {
                index_entries_for_node.push((
                    page.model_id.to_string(),
                    page.component.as_ref().map(|c| c.to_string()),
                    page.deleted,
                ));
            }
            if page.object_key.as_ref() == "control-hash" {
                index_entries_for_control += 1;
            }
        }
    }
    println!(
        "[why] bucket-index entries naming {node_key}: {index_entries_for_node:?}  \
         (control-hash entries: {index_entries_for_control})"
    );
    assert!(
        index_entries_for_control > 0,
        "CONTROL: the index holds no entry for a HashSet field either, so this walk is not reading \
         the index correctly and its answer about the node means nothing"
    );

    // 2. IS IT IN THE RESIDENT MAP before the reconcile?
    let in_map_before = shard
        .hashes
        .get(node_key)
        .map(|fields| fields.get("meta").is_some())
        .unwrap_or(false);
    println!("[why] resident hashes holds {node_key}/meta before the reconcile: {in_map_before}");
    assert!(
        in_map_before,
        "the node is not in the resident map before the reconcile, so this test cannot ask what \
         the reconcile does to it"
    );

    // 3. RUN THE RECONCILE BY NAME, which is what no load path here is proven to do.
    crate::engine::storage_bucket_internals::rebuild_unserialized_model_maps_from_bucket_index(
        shard,
    );

    let in_map_after = shard
        .hashes
        .get(node_key)
        .map(|fields| fields.get("meta").is_some())
        .unwrap_or(false);
    let control_after = shard
        .hashes
        .get("control-hash")
        .map(|fields| fields.get("f-0").is_some())
        .unwrap_or(false);
    println!(
        "[why] after the reconcile: node={in_map_after} control={control_after}"
    );
    assert!(
        control_after,
        "CONTROL: the reconcile dropped the index-registered HashSet field too, so it is not \
         behaving as its own documentation describes and nothing here is attributable"
    );

    // No assertion on `in_map_after` either way -- this test REPORTS it. Which value is correct is
    // the open question; pinning today's answer as an expectation would freeze the thing under
    // discussion.
    println!(
        "[why] VERDICT: a container-only element {} the reconcile when it is run by name",
        if in_map_after { "SURVIVES" } else { "DOES NOT SURVIVE" }
    );
}

/// SO IS A CONTAINER-ONLY ELEMENT REACHABLE AT ALL, AND WHAT HAPPENS TO ONE?
///
/// The test above refuted the premise that a context node is container-only: the index holds a
/// `("hash", Some("meta"))` entry for it, so the derived base names it like any other hash field.
/// That leaves the question the per-element collapse actually depends on, which no production path
/// here is shown to produce: if an element IS in the resident map and NOT in the index, does the
/// reconcile keep it or refuse it?
///
/// Built through the production exception rather than by hand -- `replay_install_element` is the
/// method the `context_node` replay arm uses, and it is the one path that installs without filing.
/// The element is given an object key the index has never heard of, which is what makes it
/// container-only.
#[test]
fn a_container_only_element_survives_the_reconcile_when_its_page_is_still_live() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine);

    // An index-registered field, so `bucket_map` is non-empty (or the reconcile returns early) and
    // so there is a control that must survive whatever happens to the subject.
    run(
        &engine,
        vec![Command::HashSet {
            key: "control-hash".to_string(),
            field: "f-0".to_string(),
            value: b"control-value".to_vec(),
        }],
    );

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");

    // Borrow a real, live address from the control's own block so the subject cannot be refused
    // merely for pointing at a page that does not exist. The ONLY thing that differs between the
    // control and the subject is whether the INDEX names it.
    let live_address = shard
        .hashes
        .get("control-hash")
        .and_then(|fields| fields.get("f-0"))
        .cloned()
        .expect("the control field must be in the resident map to lend its address");

    shard
        .hashes
        .replay_install_element("orphan-object", "meta".to_string(), live_address);

    let orphan_before = shard
        .hashes
        .get("orphan-object")
        .map(|fields| fields.get("meta").is_some())
        .unwrap_or(false);
    let mut index_names_orphan = false;
    for (_bucket_id, bucket) in &shard.bucket_index.bucket_map {
        for page in bucket.block_index.values() {
            if page.object_key.as_ref() == "orphan-object" {
                index_names_orphan = true;
            }
        }
    }
    println!(
        "[orphan] before the reconcile: in resident map={orphan_before} named by index={index_names_orphan}"
    );
    assert!(
        orphan_before && !index_names_orphan,
        "the fixture failed to build a CONTAINER-ONLY element (map={orphan_before}, \
         index={index_names_orphan}), so this test cannot ask its question"
    );

    crate::engine::storage_bucket_internals::rebuild_unserialized_model_maps_from_bucket_index(
        shard,
    );

    let orphan_after = shard
        .hashes
        .get("orphan-object")
        .map(|fields| fields.get("meta").is_some())
        .unwrap_or(false);
    let control_after = shard
        .hashes
        .get("control-hash")
        .map(|fields| fields.get("f-0").is_some())
        .unwrap_or(false);
    println!("[orphan] after the reconcile: orphan={orphan_after} control={control_after}");
    assert!(
        control_after,
        "CONTROL: the index-registered field was dropped too, so the reconcile is not doing what \
         its documentation says and the orphan result is not attributable to the live filter"
    );
    println!(
        "[orphan] VERDICT: a container-only element {} the reconcile",
        if orphan_after { "SURVIVES" } else { "IS REFUSED BY" }
    );

    // THE COMPLEMENTARY CONTROL, and without it the result above is unreadable: if the filter
    // never fires at all, "survives" says nothing about why. A second orphan gets an address for a
    // page NOTHING holds, so the only live-set lookup that can decide it must fail.
    shard.hashes.replay_install_element(
        "orphan-dead-page",
        "meta".to_string(),
        crate::engine::BlockAddress::from_parts(9_999, 1_234_567, 64, None, None),
    );
    assert!(
        shard
            .hashes
            .get("orphan-dead-page")
            .map(|fields| fields.get("meta").is_some())
            .unwrap_or(false),
        "the dead-page control was not installed, so it cannot test the filter"
    );
    crate::engine::storage_bucket_internals::rebuild_unserialized_model_maps_from_bucket_index(
        shard,
    );
    let dead_after = shard
        .hashes
        .get("orphan-dead-page")
        .map(|fields| fields.get("meta").is_some())
        .unwrap_or(false);
    println!("[orphan] dead-page control after the reconcile: {dead_after} (must be false)");
    assert!(
        !dead_after,
        "THE LIVE FILTER NEVER FIRES: an element pointing at a page nothing holds also survived,          so the first result above cannot be read as `the page was live`"
    );
    // MEASURED, and it REFUTED the prediction this test was written to confirm: a container-only
    // element whose PAGE is live SURVIVES. `LiveBlockKey` is `(slab_id, offset, length)` -- a page
    // identity carrying no element, component or object key -- so the filter asks only whether the
    // page is still there, never whether the index names this element.
    assert!(
        orphan_after,
        "a container-only element on a LIVE page was dropped, contradicting the measured behaviour          this assertion records"
    );
}
