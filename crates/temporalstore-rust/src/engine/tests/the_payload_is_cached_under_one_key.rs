// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAYLOAD IS CACHED UNDER EXACTLY ONE KEY, which is what "remove the duplication" meant.
//!
//! # THE ITEM THIS CLOSES
//!
//! A point read used to leave the payload in the cache **twice**: the encoded command response under
//! `CacheKey::string(shard, key)`, and the block frame under `CacheKey::page_with_slot(...)`. That
//! cost `2 * value + envelope` bytes an entry at exactly 2.00 fills per key, and removing it was the
//! open work.
//!
//! The logical copy is gone, and two existing arms *imply* this — `B/entry == value` exactly, and
//! one fill per key. But implied by a byte law is not the same as asserted, so here it is in the
//! form the item was written in: for a key just read, **exactly one** of the two cache keys that
//! could hold its payload does.
//!
//! # TOGETHER WITH ITS NEIGHBOUR IT PINS BOTH HALVES
//!
//! `a_point_read_fills_the_physical_tier` asserts the **physical** copy exists, because removing it
//! would leave string reads with no cache at all — the measured regression that reverted the
//! original design. This asserts the **logical** copy does not, because that is the duplication the
//! item set out to remove.
//!
//! **Either alone permits the wrong state:** the first is satisfied by two copies, the second by
//! none. Only the pair says "exactly one, and which one."
//!
//! # WHY NOT BY COUNTING CACHE ENTRIES
//!
//! `entries_for_shard` returns keys without values, so it cannot tell whether two entries hold the
//! *same* payload — only how many exist, which a response cache for some other command would
//! inflate. Asking the two specific keys answers exactly the question.
//!
//! rust-internal: reads the engine's own cache keys, no product behaviour

use crate::types::{Command, CommandResponse, ExecuteRequest};

#[test]
fn the_payload_is_cached_under_one_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "one-key-only";
    let value = b"a payload that must be resident exactly once".to_vec();
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: value.clone() },
    });
    assert!(write.status.ok, "write: {:?}", write.status);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: key.to_string() },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes { value: Some(value.clone()) },
        "the read did not return what was written, so nothing below is about caching"
    );

    let logical = matrixcache::CacheKey::string(1, key);
    let physical = engine
        .string_block_cache_key_for_test(1, key)
        .expect("the key is indexed, so its block has a physical cache key");

    let logical_held = engine.cache().peek_tier(&logical).is_some();
    let physical_held = engine.cache().peek_tier(&physical).is_some();

    println!();
    println!("  WHERE THE PAYLOAD IS RESIDENT AFTER ONE READ");
    println!();
    println!("    logical  CacheKey::string(shard, key)     {}", if logical_held { "HELD" } else { "empty" });
    println!("    physical CacheKey::page_with_slot(...)    {}", if physical_held { "HELD" } else { "empty" });
    println!();

    // ---- EXACTLY ONE. Stated as the count, so both failure directions report the same way.
    let copies = usize::from(logical_held) + usize::from(physical_held);
    assert_eq!(
        copies, 1,
        "the payload is resident under {copies} cache keys after one read (logical: \
         {logical_held}, physical: {physical_held}).\n\
         \n\
         TWO means the duplication this item removed is back -- a point read is filling both the \
         command response under the logical key and the block frame under the physical one, at \
         `2 * value + envelope` bytes an entry instead of `value`.\n\
         \n\
         ZERO means the read path has stopped caching altogether, which is the measured regression \
         that reverted the original design for this item: `read_bucket_index_value` is the only \
         door from a string command into `read_block_bytes`, so declining the frame there removes \
         the physically-keyed persistent tier rather than a duplicate. \
         `a_point_read_fills_the_physical_tier` is the arm that says so in one line."
    );

    // ---- AND IT IS THE PHYSICAL ONE, which is the half that has readers and a disk tier. A single
    // ---- copy under the LOGICAL key would satisfy the count above and still be wrong: it is
    // ---- memory-only, has one reader, and is larger by the response envelope.
    assert!(
        physical_held,
        "the single copy is under the LOGICAL key, not the physical one. The count is right and \
         the choice is wrong: the logical copy is memory-only with a single reader and is larger by \
         the msgpack response envelope, while the physical key is the persistent block cache's and \
         is read by every caller of `read_block_bytes`"
    );
    assert!(
        !logical_held,
        "the logical key still holds the payload. That is the copy this item removed"
    );
}
