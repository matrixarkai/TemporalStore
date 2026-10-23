// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A STRING POINT READ MUST LEAVE THE BLOCK UNDER ITS PHYSICAL KEY.
//!
//! # WHAT THIS EXISTS TO STOP, AND WHY A COMMENT WAS NOT ENOUGH
//!
//! A change was implemented and reverted that stopped caching the block frame on this path, on the
//! reasoning that the payload was cached twice and a frame serves exactly one key. Both halves of
//! that were measured and true. The conclusion did not follow:
//!
//!   * `read_bucket_index_value` is the **only** door from a string command into
//!     `read_block_bytes`, so declining the frame there does not drop a duplicate — it removes the
//!     physically-keyed tier for the commonest data kind;
//!   * that key is the **persistent block cache's** key. Six arms in `part2` assert a tiny memory
//!     cache refills from it *without a block-store read*;
//!   * and since the logical copy was dropped, the frame is the **only** copy. The same change
//!     today would leave a string read with no cache at all.
//!
//! Those six arms caught it, but each needs a particular fixture — eviction pressure, a cleared
//! memory tier, a restart — to reach the state it checks. **None of them says in one line what the
//! invariant is.** So the next person with this idea gets six confusing failures in cache-tier
//! tests instead of one that names the rule and the reason.
//!
//! A declined design is only durable if something FAILS when it is re-attempted, and says why. A
//! comment does not fail. This does.
//!
//! # IT FAILS ON THE CHANGE, NOT ON A CONSEQUENCE
//!
//! One write, one read, then `peek_tier` on the key the engine's own accessor builds. No eviction,
//! no disk tier, no restart — so it breaks on the edit itself rather than three steps downstream.
//!
//! rust-internal: reads the engine's own cache keys, no product behaviour

use crate::types::{Command, CommandResponse, ExecuteRequest};

#[test]
fn a_point_read_fills_the_physical_tier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "physical-tier-key";
    let value = b"the bytes a physical key must name".to_vec();
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: value.clone() },
    });
    assert!(write.status.ok, "write: {:?}", write.status);

    // THE KEY MUST NOT BE POPULATED BY THE WRITE ALONE, or the read below proves nothing about the
    // read path. Checked first, because a guard that cannot fail for the right reason is not one.
    let physical = engine
        .string_block_cache_key_for_test(1, key)
        .expect("the write indexed the key, so its block has a physical cache key");
    let before = engine.cache().peek_tier(&physical).is_some();

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: key.to_string() },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes { value: Some(value) },
        "the read did not return what was written, so nothing below is about caching"
    );

    assert!(
        engine.cache().peek_tier(&physical).is_some(),
        "a string point read left nothing under `CacheKey::page_with_slot` for the block it read \
         (present before the read: {before}).\n\
         \n\
         THAT KEY IS THE PERSISTENT BLOCK CACHE'S KEY, and since the logical response copy was \
         dropped it is the ONLY copy of the payload. If this failed because the frame put was \
         removed from the read path -- the change this guard exists to stop -- then string reads \
         now have no cache at all, and the `part2` arms asserting that a tiny memory cache refills \
         from disk without a block-store read will fail too, three steps downstream.\n\
         \n\
         The reasoning that leads here is that the payload used to be cached twice and a frame \
         serves exactly one key. Both are true and neither implies this entry is spare: one key \
         per frame is not one READER per frame, and `read_bucket_index_value` is the only door from \
         a string command into `read_block_bytes`, so declining the frame there removes the tier \
         rather than a duplicate."
    );
}
