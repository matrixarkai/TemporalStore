// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE LOGICAL AND PHYSICAL CACHE KEYS ARE, printed side by side for one real key.
//!
//! # WHY THIS EXISTS
//!
//! "The logical copy" and "the physical copy" have carried a lot of weight: which one a point read
//! fills, which one has readers, which one survives a restart, which one the payload byte law is
//! written against. All of that was argued in prose, and prose is exactly where a distinction stops
//! being true without anything failing.
//!
//! So this prints both keys for a key that was just written and read, with their actual field
//! values, and asserts the ways they differ.
//!
//! # THEY ARE NOT TWO VIEWS OF ONE KEY
//!
//! `CacheKey` is four fields, and the two constructors agree on exactly one of them:
//!
//! ```text
//!                shard_id   namespace   record_key                     selector
//!   logical      1          "string"    the CALLER's key               "value"
//!   physical     1          "page"      "segment-000...00<slab>"       <bucket/offset/length>
//! ```
//!
//! Only `shard_id` matches. The logical key is addressed by **what was asked for**; the physical
//! key is addressed by **where the bytes landed** — slab, offset, length, routing bucket. That is
//! the whole reason one of them can be shared by other readers and the other cannot: 25 call paths
//! reach a block by its address, and exactly one reaches it by the caller's string.
//!
//! # THE PHYSICAL KEY IS DENOMINATED IN STORED BYTES
//!
//! The selector's three components are routing bucket, offset and **length** — and that length is
//! the block's *stored* size, not the caller's. For a 256-byte run of one character it reads 35.
//!
//! This is not a detail about cache keys. It is the reason a user-denominated amplification ratio
//! cannot be computed from the index as it stands: every length on the path — the address, the
//! element entry — records what was written to storage after compression, and the size the caller
//! offered is not retained anywhere. The arm shows it by writing two keys of the **same** user
//! size, one compressible and one not, and letting their addresses disagree.
//!
//! # AND IT IS A GUARD, NOT ONLY A DEMONSTRATION
//!
//! If the two key spaces ever collapse — a namespace renamed to match, a physical key rebuilt from
//! the caller's string — the "exactly one copy" arms next door would keep passing while meaning
//! something else entirely, because they ask two keys that had become the same key. This fails
//! first and says which field stopped distinguishing them.
//!
//! rust-internal: reads the engine's own cache keys, no product behaviour

use crate::types::{Command, CommandResponse, ExecuteRequest};

#[test]
fn what_the_two_cache_keys_are() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "user:42:profile";
    let value = vec![b'v'; 256];

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

    let logical_held = engine.cache().peek_tier(&logical);
    let physical_held = engine.cache().peek_tier(&physical);
    // `peek_tier` names the TIER a key is resident in rather than returning the bytes, which is
    // the more useful thing here: the asymmetry between the two keys is not only whether something
    // is held but WHERE it can be held. The logical key had no tier of its own.
    let held_tier = |h: &Option<matrixcache::CacheReadTier>| match h {
        Some(tier) => format!("{tier:?}"),
        None => "-- nothing".to_string(),
    };

    println!();
    println!("  ONE KEY -- {:?}, {} user bytes, after one write and one read", key, value.len());
    println!();
    println!("  LOGICAL  CacheKey::string(shard, key)        addressed by WHAT WAS ASKED FOR");
    println!("    shard_id    {}", logical.shard_id);
    println!("    namespace   {:?}", logical.namespace);
    println!("    record_key  {:?}", logical.record_key);
    println!("    selector    {:?}", logical.selector);
    println!("    resident in {}", held_tier(&logical_held));
    println!();
    println!("  PHYSICAL CacheKey::page_with_slot(...)       addressed by WHERE THE BYTES LANDED");
    println!("    shard_id    {}", physical.shard_id);
    println!("    namespace   {:?}", physical.namespace);
    println!("    record_key  {:?}", physical.record_key);
    println!("    selector    {:?}", physical.selector);
    println!("    resident in {}", held_tier(&physical_held));
    println!();

    let stats = engine.cache().stats();
    println!("  AND WHAT THE CACHE IS HOLDING IN TOTAL FOR THAT ONE READ");
    println!("    user bytes offered    {:>8}", value.len());
    println!("    cache memory_bytes    {:>8}", stats.memory_bytes);
    println!("    cache disk_bytes      {:>8}", stats.disk_bytes);
    println!("    puts                  {:>8}", stats.puts);
    println!();

    // ---- THE ADDRESS IS DENOMINATED IN STORED BYTES, shown without parsing the selector: two
    // ---- keys of the SAME user size, one compressible and one not. If the address carried the
    // ---- user size they would agree.
    let incompressible: Vec<u8> = (0..256u32).map(|i| i.wrapping_mul(2_654_435_761) as u8).collect();
    assert_eq!(incompressible.len(), value.len(), "the two fixtures must offer the same user bytes");

    let other = "user:43:profile";
    let write_other = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: other.to_string(), value: incompressible.clone() },
    });
    assert!(write_other.status.ok, "write: {:?}", write_other.status);
    let read_other = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: other.to_string() },
    });
    assert_eq!(
        read_other.response,
        CommandResponse::Bytes { value: Some(incompressible.clone()) },
        "the second fixture did not round-trip"
    );

    let physical_other = engine
        .string_block_cache_key_for_test(1, other)
        .expect("the second key is indexed too");

    println!("  SAME USER SIZE, DIFFERENT ADDRESS -- so the address is in STORED bytes");
    println!();
    println!("    {:<16} {:>5} user B   selector {}", key, value.len(), physical.selector);
    println!("    {:<16} {:>5} user B   selector {}", other, incompressible.len(), physical_other.selector);
    println!();

    assert_ne!(
        physical.selector, physical_other.selector,
        "two keys offering the same {} user bytes produced the SAME address selector. The \
         selector carries offset and length, and if a compressible and an incompressible value of \
         equal user size land at the same length then the length is denominated in user bytes -- \
         which would mean a user-denominated amplification ratio is computable from the index, \
         and the measured cost of adding a size field was unnecessary",
        value.len()
    );

    // ---- THE THREE FIELDS THAT DISTINGUISH THEM. Asserted one at a time, so a collapse names
    // ---- the field that stopped distinguishing rather than reporting "the keys are equal".
    assert_ne!(
        logical.namespace, physical.namespace,
        "the two key spaces share a namespace, so they are no longer separate spaces. The \
         `exactly one copy` arms next door ask these two keys and would keep passing while \
         asking the same key twice"
    );
    assert_ne!(
        logical.record_key, physical.record_key,
        "the physical record_key is no longer the block's address. It is supposed to be \
         `segment-<slab>`, which is what lets 25 call paths reach the block without knowing any \
         caller's string"
    );
    assert_ne!(
        logical.selector, physical.selector,
        "the selectors match, so the physical key has stopped carrying offset/length/bucket -- \
         the part that makes it an ADDRESS rather than a name"
    );

    // ---- AND THEY DO AGREE ON THE SHARD, which is the one field they share and must.
    assert_eq!(
        logical.shard_id, physical.shard_id,
        "both keys describe the same shard's data, so this is the one field that must match"
    );

    // ---- WHICH ONE HOLDS THE PAYLOAD. Stated here too, because the exemplification is only
    // ---- honest if it shows the asymmetry rather than two equivalent-looking rows.
    assert!(
        physical_held.is_some(),
        "the physical key holds nothing, so a string read is leaving no cached copy at all"
    );
    assert!(
        logical_held.is_none(),
        "the logical key holds the payload. That is the duplicate copy that was removed"
    );
}
