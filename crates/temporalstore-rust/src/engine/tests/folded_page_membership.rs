// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FOLDED PAGE IS NOT AUTHORITATIVE FOR MEMBERSHIP, AND THAT BOUNDS WHAT THE NEXT STAGE CAN DO.
//!
//! # WHY THIS EXISTS
//!
//! #2027 folds a container's element pages into one at compaction. The stage after it was to rebuild
//! the resident container maps FROM those pages -- one page read instead of one index entry per
//! element -- because that is the step that would let the page index stop carrying an entry per
//! element at all. `insert_timestamped_secondary_view` already works that way for the timestamped
//! kinds, so the shape was there to copy.
//!
//! IT IS NOT SOUND FOR THE CONTAINER KINDS, and this module is the driven reason, not an argument.
//! A removal reaches the resident map and the page INDEX; it does not reach the PAGE. A folded page
//! stays live because its other elements are live, so the removed element's bytes are still inside it.
//! Rebuilding membership from that page puts the element back.
//!
//! That is exactly #2017's over-complete state -- resident map holding an element the live index says
//! is gone -- which #2025 enumerated five readers for and fixed. Deriving membership from pages would
//! reintroduce it from the other side, and #2025's own conclusion is what says how much it costs:
//! four of those five readers are LIVE.
//!
//! # WHAT THE NEXT STAGE HAS TO DO FIRST, stated so it does not have to be re-derived
//!
//! One of these, and none of them is free:
//!
//!   * a removal REWRITES the page it was in. That is a read-modify-write on the removal path, which
//!     is the cost #2027 exists to avoid paying -- it folds only pages a round was already rewriting.
//!   * the frame carries a TOMBSTONE item, so the page records the removal without being rewritten
//!     whole. That is a page-format change, and therefore a stored-format change.
//!   * the page supplies only VALUES and the index stays the membership authority. That is sound and
//!     buys nothing: it still needs one index entry per element, which is the thing the stage was
//!     for.
//!
//! # WHAT THIS ASSERTS, AND WHAT A RED HERE MEANS
//!
//! It asserts the CURRENT truth: after a fold and one removal, the index names one fewer element than
//! the page holds. If this goes red because the page no longer holds the removed element, the
//! constraint has been LIFTED -- a removal now reaches the page -- and the rebuild above becomes
//! sound. That is a result and not a regression, and the assertion message says so, because a
//! recorded constraint whose mechanism was fixed is otherwise read as a break.
//!
//! # DENOMINATORS
//!
//! Both sides are counted and both are asserted non-zero before they are compared: a fixture that
//! folded nothing, or that read no page, would make the two sides agree for the wrong reason. The
//! fold's own counters are asserted first, because "one page per container" is also what an object
//! with one element looks like.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

/// Members written. More than one page's worth would obscure the point; twelve fold into one page,
/// which is the state the next stage would be reading.
const MEMBERS: usize = 12;

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
        table_name: "folded-page-membership".to_string(),
        shard_uri: "local://folded-page-membership/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// rust-internal: drives the engine's own command surface
#[test]
fn a_folded_page_still_holds_a_member_the_live_index_no_longer_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let members: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|i| format!("member-{i:04}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: "folded".to_string(),
                member: member.clone(),
            },
        );
    }

    // FOLD FIRST. The counters are the instrument, because "one page per container" is also what an
    // idle round over a one-element object reports.
    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    assert!(
        batches > 0 && folded > 0,
        "nothing folded ({batches} batches, {folded} pages), so this fixture is not in the state it \
         claims to be asking about"
    );

    let victim = members[3].clone();
    write(
        &engine,
        Command::SetRemove {
            key: "folded".to_string(),
            member: victim.clone(),
        },
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // SIDE 1: what the live page INDEX names for this object.
    let named: BTreeSet<Vec<u8>> =
        crate::engine::bucket_store::bucket_index_component_block_addresses(shard, "set", "folded")
            .iter()
            .filter_map(|(component, _)| component.as_deref().and_then(|c| hex::decode(c).ok()))
            .collect();

    // SIDE 2: what the PAGES this object resolves to actually contain. Deduplicated by address,
    // because after a fold many entries name one page and reading it twice would double the count.
    let mut in_pages: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut pages_read = 0usize;
    let mut framed_pages = 0usize;
    let mut seen_addresses = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != "set" || &*page.object_key != "folded" {
                continue;
            }
            let identity = (
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            );
            if !seen_addresses.insert(identity) {
                continue;
            }
            let Ok(bytes) = engine.block_store.read(&page.address) else {
                continue;
            };
            pages_read += 1;
            if let crate::engine::container_pages::ContainerPageDecode::Framed { items, .. } =
                crate::engine::container_pages::decode_container_page(&bytes)
            {
                framed_pages += 1;
                for item in items {
                    in_pages.insert(item.key);
                }
            }
        }
    }

    println!("=== after a fold and one removal ===");
    println!(
        "  {pages_read} distinct page(s) read, {framed_pages} framed; the live index names {} \
         member(s), the pages hold {}",
        named.len(),
        in_pages.len()
    );
    println!(
        "  the removed member: named by the index = {}, present in the pages = {}",
        named.contains(&victim),
        in_pages.contains(&victim)
    );

    // DENOMINATORS FIRST. A zero on either side makes the comparison below empty.
    assert!(pages_read > 0, "DENOMINATOR: no pages read for this object");
    assert!(framed_pages > 0, "DENOMINATOR: no framed pages, so the page names no elements at all");
    assert_eq!(
        MEMBERS - 1,
        named.len(),
        "the live index names {} of {MEMBERS} members after one removal, so the removal did not do \
         what this fixture assumes",
        named.len()
    );
    assert!(
        !named.contains(&victim),
        "the live index still names the removed member"
    );

    // THE CONSTRAINT.
    assert!(
        in_pages.contains(&victim),
        "THE CONSTRAINT IS LIFTED, WHICH IS A RESULT AND NOT A BREAK: the folded page no longer \
         holds the removed member, so a removal now reaches the page and rebuilding the resident \
         container maps FROM the pages has become sound. Update this module's header and the stage \
         it blocks."
    );
    assert_eq!(
        MEMBERS,
        in_pages.len(),
        "the pages hold {} members where {MEMBERS} were written and one was removed",
        in_pages.len()
    );
}
