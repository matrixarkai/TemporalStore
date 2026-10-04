// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE RECONCILE DOES TO AN ELEMENT THE BUCKET INDEX DOES NOT NAME.
//!
//! Every test here was written to CONFIRM a hazard and every one refuted it. They are kept because
//! the refutation is the result: a later change that makes the load path name-sensitive breaks
//! these, loudly, instead of quietly emptying the resident maps.
//!
//! THE PREDICTION THAT FAILED. A context node is installed through
//! `install_element_staged_only::<HashKind>`, whose emitter files a staged outcome and no outcome
//! record of kind `hash`; the recovery arm installs through `replay_install_element`, the exception
//! only `HashKind` declares. Both were documented as writing a block that "is never registered in
//! the bucket index". If that were true the element would be container-only, the index-derived base
//! could not name it, and `fill_absent_elements` would refuse it as a resurrection.
//!
//! IT IS NOT TRUE, AND THE INDEX SAYS SO. Asked directly, `bucket_index` holds
//! `("hash", Some("meta"), deleted=false)` for a node written through the command path --
//! `append_value` files the block from the object id, `CONTEXT_NODE_FIELD` and the routing bucket.
//! What is deliberately absent is the OUTCOME record under kind `hash`, which is a different thing
//! and was being conflated with the index entry.
//!
//! AND A GENUINELY CONTAINER-ONLY ELEMENT SURVIVES ANYWAY, which is the finding the collapse work
//! depends on. `LiveBlockKey` is `(slab_id, offset, length)`: a PAGE identity carrying no element,
//! component or object key. So `fill_absent_elements` asks only whether the page is still there,
//! never whether the index names this element -- and an element the index has never heard of is
//! kept as long as its page is live.
//!
//! WHY EACH CONTROL IS HERE, because a survival test without them asserts nothing:
//!
//!   * AN INDEX-REGISTERED FIELD, in every arm. Without it a reload that lost EVERYTHING would look
//!     identical to the defect being probed.
//!   * A DEAD-PAGE ADDRESS, in the arms that claim survival. An inert filter and a permissive one
//!     are indistinguishable from a single passing case; the dead-page element must be REFUSED, and
//!     that is what proves the filter discriminates rather than waving everything through.
//!
//! ONE ASSERTION HERE WAS INVERTED RATHER THAN RE-GOLDENED, and the distinction is the point.
//! `a_container_only_element_survives_the_reconcile_when_its_page_is_still_live` was first written
//! as `..._is_refused_by_the_reconciles_live_address_filter`, asserting refusal. It failed. The
//! assertion was changed to the measured behaviour AND THE NAME WAS CHANGED WITH IT. A test that
//! keeps its name through a semantic inversion is how `wal.rs` once ended up asserting that one
//! equals two: the name went on describing the old belief while the body asserted the opposite, and
//! nothing in the suite could see the contradiction. Restate, and rename; do not re-golden.
//!
//! SCOPE, LABELLED. The end-to-end path is measured for `hashes` only, because
//! `replay_install_element` is gated on `ReplaysInstallsUnrecorded` and `HashKind` is the only kind
//! that declares it. For the other three kinds the DECIDING FUNCTION is measured directly, with
//! each kind's own element and value type. That the whole load path then behaves the same for them
//! follows by inference from shared generic code, and is left as an inference rather than claimed
//! as a measurement.
//!
//! STILL OPEN, AND DELIBERATELY NOT TESTED HERE. Whether the collision rule in
//! `fill_absent_elements` -- `insert_element_if_absent`, under which the index-derived address wins
//! -- is reachable at all. It needs the same `(key, element)` in both sources at DIFFERENT
//! addresses, and no path has been shown to produce that. A test asserting today's collision
//! outcome would pass because the state is unreachable, which is a guard over an unreachable defect
//! wearing the costume of a correctness test. Unproven is written down as unproven.

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

/// THE SAME QUESTION FOR THE OTHER THREE KINDS, ASKED OF THE FUNCTION THAT DECIDES.
///
/// `replay_install_element` is gated on `ReplaysInstallsUnrecorded` and only `HashKind` declares
/// it, so the end-to-end arm above cannot be written for `sets`, `zsets` or `lists`. What CAN be
/// measured per kind is `fill_absent_elements` itself -- the function both reconcile sites call
/// through `RecordedMap::reconcile_from_durable`, and the only place the live-address refusal
/// lives. Each kind is driven with its OWN element and value type, because the whole question is
/// whether the filter looks at anything but the page, and a single type could not show that.
///
/// Two elements per kind, and the pair is the test: one on a page the live set holds, one on a page
/// it does not. The first must be KEPT and the second REFUSED. Either outcome alone would be
/// unreadable -- all-kept means the filter is inert, all-refused means it ignores liveness.
#[test]
fn the_live_filter_keeps_an_unnamed_element_on_a_live_page_for_every_kind() {
    use std::collections::{BTreeMap, HashMap, HashSet};

    let live_address = crate::engine::BlockAddress::from_parts(1, 4_096, 64, None, None);
    let dead_address = crate::engine::BlockAddress::from_parts(9_999, 1_234_567, 64, None, None);
    let live: HashSet<(u64, u64, u64)> = [crate::engine::live_page_key(&live_address)]
        .into_iter()
        .collect();
    assert!(
        !live.contains(&crate::engine::live_page_key(&dead_address)),
        "the fixture's two addresses collide in the live set, so nothing here can distinguish \
         kept from refused"
    );

    // HASHES -- a sorted vector keyed by field name.
    {
        let mut persisted: HashMap<String, crate::engine::hash_field_map::HashFieldMap> =
            HashMap::new();
        let fields = persisted.entry("obj".to_string()).or_default();
        fields.insert("on-live-page".to_string(), live_address.clone());
        fields.insert("on-dead-page".to_string(), dead_address.clone());
        let mut refused = 0usize;
        let merged = crate::engine::storage_bucket_internals::fill_absent_elements(
            HashMap::new(),
            persisted,
            &live,
            &mut refused,
        );
        let kept = merged.get("obj");
        println!(
            "[per-kind] hashes: kept_live={} kept_dead={} refused={refused}",
            kept.map(|f| f.get("on-live-page").is_some()).unwrap_or(false),
            kept.map(|f| f.get("on-dead-page").is_some()).unwrap_or(false),
        );
        assert!(
            kept.map(|f| f.get("on-live-page").is_some()).unwrap_or(false),
            "hashes: an element the derived map never named was dropped even though its page is live"
        );
        assert!(
            !kept.map(|f| f.get("on-dead-page").is_some()).unwrap_or(false),
            "hashes: THE FILTER IS INERT -- an element on a page nothing holds was kept too, so the \
             line above proves nothing about liveness"
        );
        assert_eq!(1, refused, "hashes: exactly the dead-page element should be refused");
    }

    // SETS -- a B-tree keyed by member bytes, the kind the per-element collapse is being built on.
    {
        let mut persisted: HashMap<String, BTreeMap<Vec<u8>, crate::engine::BlockAddress>> =
            HashMap::new();
        let members = persisted.entry("obj".to_string()).or_default();
        members.insert(b"on-live-page".to_vec(), live_address.clone());
        members.insert(b"on-dead-page".to_vec(), dead_address.clone());
        let mut refused = 0usize;
        let merged = crate::engine::storage_bucket_internals::fill_absent_elements(
            HashMap::new(),
            persisted,
            &live,
            &mut refused,
        );
        let kept = merged.get("obj");
        println!(
            "[per-kind] sets: kept_live={} kept_dead={} refused={refused}",
            kept.map(|m| m.contains_key(b"on-live-page".as_slice())).unwrap_or(false),
            kept.map(|m| m.contains_key(b"on-dead-page".as_slice())).unwrap_or(false),
        );
        assert!(
            kept.map(|m| m.contains_key(b"on-live-page".as_slice())).unwrap_or(false),
            "sets: an unnamed member on a live page was dropped"
        );
        assert!(
            !kept.map(|m| m.contains_key(b"on-dead-page".as_slice())).unwrap_or(false),
            "sets: THE FILTER IS INERT for this kind"
        );
        assert_eq!(1, refused, "sets: exactly the dead-page member should be refused");
    }

    // ZSETS -- a B-tree keyed by member bytes whose VALUE carries the score beside the address.
    {
        let mut persisted: HashMap<
            String,
            BTreeMap<Vec<u8>, (u64, crate::engine::BlockAddress)>,
        > = HashMap::new();
        let members = persisted.entry("obj".to_string()).or_default();
        members.insert(b"on-live-page".to_vec(), (7, live_address.clone()));
        members.insert(b"on-dead-page".to_vec(), (9, dead_address.clone()));
        let mut refused = 0usize;
        let merged = crate::engine::storage_bucket_internals::fill_absent_elements(
            HashMap::new(),
            persisted,
            &live,
            &mut refused,
        );
        let kept = merged.get("obj");
        println!(
            "[per-kind] zsets: kept_live={} kept_dead={} refused={refused}",
            kept.map(|m| m.contains_key(b"on-live-page".as_slice())).unwrap_or(false),
            kept.map(|m| m.contains_key(b"on-dead-page".as_slice())).unwrap_or(false),
        );
        assert!(
            kept.map(|m| m.contains_key(b"on-live-page".as_slice())).unwrap_or(false),
            "zsets: an unnamed member on a live page was dropped -- note the address is reached \
             through the VALUE here, so this also checks `CarriedValue` for the pair"
        );
        assert!(
            !kept.map(|m| m.contains_key(b"on-dead-page".as_slice())).unwrap_or(false),
            "zsets: THE FILTER IS INERT for this kind"
        );
        assert_eq!(1, refused, "zsets: exactly the dead-page member should be refused");
    }

    // LISTS -- a B-tree keyed by an `i64` sequence rather than by bytes.
    {
        let mut persisted: HashMap<String, BTreeMap<i64, crate::engine::BlockAddress>> =
            HashMap::new();
        let seqs = persisted.entry("obj".to_string()).or_default();
        seqs.insert(1, live_address.clone());
        seqs.insert(2, dead_address.clone());
        let mut refused = 0usize;
        let merged = crate::engine::storage_bucket_internals::fill_absent_elements(
            HashMap::new(),
            persisted,
            &live,
            &mut refused,
        );
        let kept = merged.get("obj");
        println!(
            "[per-kind] lists: kept_live={} kept_dead={} refused={refused}",
            kept.map(|m| m.contains_key(&1)).unwrap_or(false),
            kept.map(|m| m.contains_key(&2)).unwrap_or(false),
        );
        assert!(
            kept.map(|m| m.contains_key(&1)).unwrap_or(false),
            "lists: an unnamed sequence on a live page was dropped"
        );
        assert!(
            !kept.map(|m| m.contains_key(&2)).unwrap_or(false),
            "lists: THE FILTER IS INERT for this kind"
        );
        assert_eq!(1, refused, "lists: exactly the dead-page sequence should be refused");
    }

    println!(
        "[per-kind] so for all four kinds the refusal is decided by the PAGE and not by whether \
         the index names the element. The end-to-end path is measured for hashes only; for the \
         other three this is the deciding function, and the full path following suit is an \
         inference from shared generic code"
    );
}
