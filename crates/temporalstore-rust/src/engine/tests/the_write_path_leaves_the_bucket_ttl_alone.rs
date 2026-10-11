// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WRITE PATH NO LONGER RECOMPUTES A BUCKET'S TTL MINIMUM, AND THE SWEEP STILL DOES.
//!
//! # WHY THE TERM CAME OFF THE WRITE PATH
//!
//! The minimum is one map lookup per page in the bucket, keyed by the page's object key, and it is
//! the last per-page pass in bucket maintenance that does not short-circuit. Every reader of the
//! field it produces is a REPORT or the serializer; real expiry runs off the shard's expiry map and
//! its deadline-ordered mirror instead, and no expiry, eviction or serving decision consults a
//! bucket's TTL.
//!
//! # WHAT BOUNDS THE STALENESS, which is the claim a later reader needs
//!
//! The index flush runs the FULL sweep immediately before it serializes, so THE PERSISTED VALUE IS
//! NEVER STALE -- it is recomputed right before being written. What can lag is the in-memory value
//! the reports read, and it lags by at most one index flush. A load recomputes it as well.
//!
//! # WHY THE EXISTING EQUIVALENCE TEST DOES NOT COVER THIS
//!
//! `bucket_runtime_flags_match_full_sweep` compares the targeted path against the sweep and does
//! include the TTL -- but as `ttl_ms.is_some()`, a BOOLEAN. A value that is stale and still present
//! compares equal to a fresh one, so that test is insensitive to exactly the difference this change
//! introduces, and it passes either way. MEASURED: it passes with this change in place. That is not
//! evidence the change is correct; it is evidence the test cannot see it. Hence this module, which
//! asserts the VALUE.
//!
//! # THE CONTROL IS THE SECOND HALF OF EACH PAIR
//!
//! "The write path left it alone" is also what a broken computation looks like, so each case asserts
//! the sweep then COMPUTES it on the same shard. Without that second half, deleting the computation
//! outright would pass.
//!
//! Both write-path arms are covered, because the write path is not always the targeted traversal:
//! the pending refresh hands its work to the sweep once the touched set reaches half the buckets,
//! and at the routing range production runs it essentially always does. Gating the TTL on the
//! traversal would therefore have left the cost where it was on the configuration that matters,
//! which is why it rides its own parameter -- and why both arms are asserted here.

use std::sync::Arc;

use crate::engine::state::{BlockIndex, BlockSlabLiveIndex, BucketNode, ShardState};
use crate::engine::storage_bucket_internals::{
    note_bucket_flags_stale, refresh_bucket_runtime_flags, refresh_pending_bucket_runtime_flags,
};

/// Far enough ahead that the remaining time is non-zero after the subtraction.
///
/// It has to be: the field is `Option<NonZeroU64>`, so a remaining time of zero is ABSENT and a
/// fixture whose expiry has already passed would assert nothing at all.
const FAR_FUTURE_MS: u64 = u64::MAX / 2;

fn a_bucket_holding_one_page(routing_bucket: u32, object_key: &str) -> BucketNode {
    let mut bucket = BucketNode::default();
    bucket.routing_bucket = routing_bucket;
    bucket.set_meta_loaded(true);
    bucket.set_in_memory(true);
    let mut live = BlockSlabLiveIndex::default();
    bucket.insert_page(&object_key, 
        BlockIndex {
            object_key: Arc::from(object_key),
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                "string",
            )
            .expect("`string` is a stored model spelling"),
            address: Default::default(),
            dirty: false,
            deleted: false,
        },
        &mut live,
    );
    bucket
}

/// A shard with `buckets` buckets, each holding one page, and an expiry on every one of them.
fn a_shard_with_expiring_pages(buckets: u32) -> ShardState {
    let mut shard = ShardState::default();
    for routing_bucket in 0..buckets {
        let object_key = format!("expiring-{routing_bucket}");
        let bucket = a_bucket_holding_one_page(routing_bucket, &object_key);
        // THE PLACED PAGES, READ OUT BEFORE THE BUCKET IS HANDED OVER, so the lookup can be filed
        // from the same (handle, page) pairs the map keyed them under.
        let placed: Vec<(u64, BlockIndex)> = bucket
            .block_index
            .iter()
            .map(|(handle, page)| (*handle, page.clone()))
            .collect();
        shard.bucket_index.bucket_map.insert(routing_bucket, bucket);
        // AND THE LOOKUP IS FILED, AS EVERY PRODUCTION PLACEMENT FILES IT.
        //
        // The TTL rollup selects pages whose OBJECT is in `expires_at_ms`, and the object a page
        // belongs to is stated beside the page, not on it. A hand-built index that files nothing
        // names no page, so the rollup selected nothing and the control below -- "the sweep must
        // still compute the minimum" -- failed. That is the fixture being unlike production, not
        // the rollup being broken: it is the same shape the page-index encoder refuses outright.
        let key: std::sync::Arc<str> = std::sync::Arc::from(object_key.as_str());
        for (handle, page) in &placed {
            shard
                .bucket_index
                .insert_object_block_lookup(routing_bucket, *handle, &key, page);
        }
        crate::engine::set_expiry(&mut shard, object_key, FAR_FUTURE_MS);
    }
    assert!(
        !shard.expires_at_ms.is_empty(),
        "the fixture must have expiries, or the computation under test short-circuits and both \
         halves of every pair below would agree for the wrong reason"
    );
    shard
}

fn ttl_of(shard: &ShardState, routing_bucket: u32) -> Option<u64> {
    shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("the fixture bucket must exist")
        .ttl_ms
        .ms()
}

#[test]
fn the_write_path_leaves_the_bucket_ttl_alone_where_it_takes_the_sweep() {
    // ONE bucket pending of one, so `pending * 2 >= bucket_count` and the pending refresh hands its
    // work to the full sweep. This is the arm production takes, and it is the one that would have
    // been missed by gating on the traversal instead of on the parameter.
    let mut shard = a_shard_with_expiring_pages(1);
    note_bucket_flags_stale(&mut shard, 0);
    refresh_pending_bucket_runtime_flags(&mut shard);
    assert_eq!(
        ttl_of(&shard, 0),
        None,
        "a write-path refresh must leave the TTL as it stands, even when it delegates to the sweep"
    );

    // THE CONTROL. The same shard, swept: the value must now be computed. Without this, deleting
    // the computation entirely would satisfy the assertion above.
    refresh_bucket_runtime_flags(&mut shard);
    assert!(
        ttl_of(&shard, 0).is_some(),
        "the sweep must still compute the minimum, or the assertion above is about a broken \
         computation rather than about the write path"
    );
}

#[test]
fn the_write_path_leaves_the_bucket_ttl_alone_where_it_takes_the_targeted_loop() {
    // THREE buckets, ONE pending, so `1 * 2 >= 3` is false and the pending refresh walks only the
    // touched bucket. The other arm of the same decision.
    let mut shard = a_shard_with_expiring_pages(3);
    note_bucket_flags_stale(&mut shard, 1);
    refresh_pending_bucket_runtime_flags(&mut shard);
    assert_eq!(
        ttl_of(&shard, 1),
        None,
        "a write-path refresh must leave the TTL as it stands on the targeted traversal too"
    );
    // The untouched buckets are untouched for a different reason -- they were never visited -- so
    // they say nothing about the parameter and are asserted only to show the fixture is shaped as
    // described.
    assert_eq!(ttl_of(&shard, 0), None, "never visited");
    assert_eq!(ttl_of(&shard, 2), None, "never visited");

    // THE CONTROL, as above.
    refresh_bucket_runtime_flags(&mut shard);
    for routing_bucket in 0..3 {
        assert!(
            ttl_of(&shard, routing_bucket).is_some(),
            "the sweep must compute every bucket's minimum, bucket {routing_bucket}"
        );
    }
}
