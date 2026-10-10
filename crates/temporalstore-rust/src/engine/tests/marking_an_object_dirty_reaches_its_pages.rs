// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MARKING AN OBJECT DIRTY REACHES THE PAGES IT ALREADY HAS, AT MARK TIME.
//!
//! # THE INVARIANT, AND IT IS THE WHOLE OF THE CHANGE
//!
//! A bucket is dirty when any page in it is dirty. It used to also be dirty when any page's
//! OBJECT was dirty, which asked a page which object it belonged to -- on the write path, in a
//! scope holding nothing but the bucket. The dirtiness is pushed onto the pages at mark time now,
//! so the predicate is `any(page.dirty)` and the question is never asked.
//!
//! That is only equivalent if EVERY SITE THAT MARKS AN OBJECT DIRTY ALSO PUSHES. There are exactly
//! two, and the fail-first for this module is omitting the push at one of them: the bucket then
//! reads clean while its object is dirty, which is a silent wrong answer rather than a failure.
//!
//! # WHY THE NEAREST EXISTING GUARD CANNOT STAND IN FOR THIS
//!
//! `bucket_runtime_flags_match_full_sweep` compares the targeted refresh against the sweep and
//! does include dirtiness -- as a BOOLEAN, `bucket.dirty()`. It can see THAT a bucket is dirty and
//! not WHY, so it passes whether the dirtiness arrived through the object set or through the pages.
//! It is insensitive to this change rather than wrong about it, and it was cited in prose as
//! holding the equivalence, which is the worse failure: it stops the next reader looking. This
//! module asserts the mechanism it cannot see.
//!
//! # AND THE PREMISE THAT FAILED, KEPT BECAUSE IT IS THE REASON THE CODE IS SHAPED THIS WAY
//!
//! The push can only find pages through the object lookup, which is derived and empty until
//! something builds it. The first version asserted that a mark could not reach a shard holding
//! buckets with an unbuilt lookup, reasoning that a load rebuilds before returning the shard. TWO
//! TESTS FIRED THAT ASSERTION IMMEDIATELY. The reasoning was about one path in and the window
//! belongs to the others. So the push establishes the lookup on the first mark that finds it
//! unbuilt -- the same bargain the delete path already makes -- and the case is asserted below
//! rather than reasoned about.

use std::sync::Arc;

use crate::engine::hashing::block_routing_bucket;
use crate::engine::state::{BlockIndex, BlockSlabLiveIndex, BucketNode, ShardState};
use crate::engine::storage_bucket_internals::{
    mark_async_dirty_object, refresh_bucket_runtime_flags,
};

const WHOLE_RANGE_END: u32 = u32::MAX;

fn a_shard_holding_one_clean_page(object_key: &str) -> (ShardState, u32) {
    let routing_bucket = block_routing_bucket(object_key, 0, WHOLE_RANGE_END);
    let mut shard = ShardState::default();
    let mut bucket = BucketNode::default();
    bucket.routing_bucket = routing_bucket;
    bucket.set_meta_loaded(true);
    bucket.set_in_memory(true);
    let mut live = BlockSlabLiveIndex::default();
    bucket.insert_page(
        BlockIndex {
            object_key: Arc::from(object_key),
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                "string",
            )
            .expect("`string` is a stored model spelling"),
            address: Default::default(),
            // CLEAN, which is the only interesting starting state: a page that is already dirty
            // would satisfy every assertion below whatever the mark did.
            dirty: false,
            deleted: false,
        },
        &mut live,
    );
    shard.bucket_index.bucket_map.insert(routing_bucket, bucket);
    (shard, routing_bucket)
}

fn any_page_dirty(shard: &ShardState, routing_bucket: u32) -> bool {
    shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("the fixture bucket must exist")
        .block_index
        .values()
        .any(|page| page.dirty)
}

#[test]
fn marking_an_object_dirty_sets_the_dirty_bit_on_the_pages_it_already_has() {
    let (mut shard, routing_bucket) = a_shard_holding_one_clean_page("pushed-object");

    // THE STARTING STATE IS ASSERTED, or "it is dirty afterwards" could be true of a fixture that
    // was dirty all along and the mark could be doing nothing.
    assert!(
        !any_page_dirty(&shard, routing_bucket),
        "the fixture page must start clean"
    );

    mark_async_dirty_object(&mut shard, "pushed-object", 0, WHOLE_RANGE_END);

    assert!(
        any_page_dirty(&shard, routing_bucket),
        "marking the object must reach the page it already has; otherwise the bucket predicate, \
         which now reads only `page.dirty`, answers false for a dirty object"
    );

    // AND THE PREDICATE THE PUSH EXISTS TO SERVE actually answers true through it. The sweep
    // recomputes the bucket flag from the pages alone.
    refresh_bucket_runtime_flags(&mut shard);
    assert!(
        shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("bucket")
            .dirty(),
        "the sweep must see the bucket as dirty, reading only the pages"
    );
}

#[test]
fn an_unmarked_object_leaves_its_pages_clean() {
    // THE CONTROL FOR THE MODULE. If marking is what makes a page dirty, NOT marking must leave it
    // clean -- otherwise the assertion above would hold on a shard where everything is dirty.
    let (mut shard, routing_bucket) = a_shard_holding_one_clean_page("untouched-object");

    mark_async_dirty_object(&mut shard, "a-different-object", 0, WHOLE_RANGE_END);

    assert!(
        !any_page_dirty(&shard, routing_bucket),
        "marking a different object must not reach this one's pages"
    );
    refresh_bucket_runtime_flags(&mut shard);
    assert!(
        !shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("bucket")
            .dirty(),
        "a bucket whose pages are all clean must read clean"
    );
}

#[test]
fn the_push_builds_the_object_lookup_when_it_finds_it_unbuilt() {
    // THE CASE THE FAILED PREMISE WAS ABOUT. The fixture never builds the lookup, so the push has
    // to establish it before it can find anything. This is the state two existing tests reached
    // and the first version of the push asserted was unreachable.
    let (mut shard, routing_bucket) = a_shard_holding_one_clean_page("unbuilt-lookup-object");
    assert!(
        shard.bucket_index.object_block_lookup.is_empty(),
        "the fixture must start with an unbuilt lookup, or this test is about the other path"
    );
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "and it must hold a bucket, or there would be nothing for the push to miss"
    );

    mark_async_dirty_object(&mut shard, "unbuilt-lookup-object", 0, WHOLE_RANGE_END);

    assert!(
        !shard.bucket_index.object_block_lookup.is_empty(),
        "the push must establish the lookup rather than silently finding nothing"
    );
    assert!(
        any_page_dirty(&shard, routing_bucket),
        "and having established it, the push must reach the page"
    );
}
