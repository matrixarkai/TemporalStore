// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A CACHE KEY COSTS, against the information it actually carries.
//!
//! # THE QUESTION
//!
//! `CacheKey` is four fields — `shard_id`, `record_key`, `namespace`, `selector`. The question is
//! whether all four are needed. **The fields are not the problem.** Shard, which namespace, which
//! record, and which part of that record are four real distinctions, and dropping any one of them
//! merges keys that must not merge: `what_the_two_cache_keys_are` shows the logical and physical
//! keys agreeing on `shard_id` alone and differing in the other three.
//!
//! The **representation** is the problem, and only for the page namespace. Every part of a page key
//! is an integer, and the key carries them as formatted decimal strings:
//!
//! ```text
//!   record_key   "segment-00000000000000000000"   a u64 slab id, zero-padded to 20 digits
//!   selector     "slot-970485159:0:35"            a u32 bucket and two u64s, colon-joined
//! ```
//!
//! # WHY IT IS NOT A COSMETIC POINT
//!
//! This key is constructed on **every page cache get and put** — the upstream constructors say so
//! and were already optimised once to drop a second allocation inside the padded-integer path. So
//! the two heap allocations below are paid per block read, not per key stored, and the bytes are
//! paid again for every resident entry's key.
//!
//! # THE PRE-SIZED CAPACITY IS WRONG BOTH WAYS, AND IS ALREADY FIXED UPSTREAM
//!
//! At the pinned revision `page_selector` pre-sizes its `String` to a flat 64 and names the shape
//! it is sizing for: `"slot-" + u32 + ":gen-" + u64 + ":" + u64 + ":" + u64`. That shape's maximum
//! is **82**:
//!
//! ```text
//!   slot-  5    u32 10    : 1    gen- 4    u64 20    : 1    u64 20    : 1    u64 20   = 82
//! ```
//!
//! So 64 is wrong in both directions at once: it wastes 45 bytes on the common path, where the
//! selector is 19 bytes, and it still reallocates on the generation path — the one allocation a
//! pre-size exists to prevent — which this crate reaches from compaction at four sites. The arm
//! builds a maximal generation key and reads the capacity back, so the realloc is observed rather
//! than deduced from arithmetic.
//!
//! **This is not a defect to report; it is a fix this crate does not yet have.** MatrixCache's own
//! `main` already computes the capacity per component and says why in as many words. The pin here
//! is 105 commits behind that. So the two capacity assertions below are a **tripwire on the pin**:
//! when it moves they fail, and their messages say to delete them.
//!
//! The representation point is separate and survives the bump — see below, where the page key is
//! still five integers rendered into two strings.
//!
//! # WHAT THIS ARM DOES AND DOES NOT CLAIM
//!
//! It reports the numbers and asserts the margin is real and large. It does **not** assert a particular
//! replacement: `CacheKey` is MatrixCache's type, reached here through a pinned revision, so
//! changing it is a change to that crate plus a pin bump and is a separate piece of work. What this
//! fixes is that the cost was never written down, so "do we need all this" had no number attached.
//!
//! rust-internal: measures the engine's own cache key, no product behaviour

use crate::types::{Command, ExecuteRequest};

#[test]
fn what_a_cache_key_costs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "user:42:profile";
    let wrote = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: vec![b'v'; 256] },
    });
    assert!(wrote.status.ok, "write: {:?}", wrote.status);

    let logical = matrixcache::CacheKey::string(1, key);
    let physical = engine
        .string_block_cache_key_for_test(1, key)
        .expect("the key is indexed, so it has a page key");

    let struct_bytes = std::mem::size_of::<matrixcache::CacheKey>();

    // ---- HEAP BEHIND EACH KEY. `namespace` is a `Cow::Borrowed` for both constructors, so it
    // ---- holds no heap; the two `String`s do.
    let heap_of = |k: &matrixcache::CacheKey| k.record_key.capacity() + k.selector.capacity();
    let allocs_of = |k: &matrixcache::CacheKey| {
        usize::from(k.record_key.capacity() > 0) + usize::from(k.selector.capacity() > 0)
    };

    // ---- WHAT A PAGE KEY ACTUALLY SAYS, as integers: shard, slab, offset, length, bucket.
    const PAGE_INFORMATION_BYTES: usize =
        std::mem::size_of::<u32>()   // shard
        + std::mem::size_of::<u64>() // slab id
        + std::mem::size_of::<u64>() // offset
        + std::mem::size_of::<u64>() // length
        + std::mem::size_of::<u32>(); // routing bucket

    println!();
    println!("  THE STRUCT");
    println!("    size_of::<CacheKey>()            {struct_bytes:>5} B   (4 fields, 2 of them String)");
    println!();
    println!("  AND THE HEAP BEHIND EACH KIND");
    println!();
    println!("    kind      record_key                       len/cap   selector                   len/cap   heap   allocs   total");
    for (name, k) in [("logical", &logical), ("physical", &physical)] {
        println!(
            "    {:<8}  {:<32} {:>3}/{:<3}   {:<25} {:>3}/{:<3}   {:>4}   {:>6}   {:>5}",
            name,
            format!("{:?}", k.record_key),
            k.record_key.len(),
            k.record_key.capacity(),
            format!("{:?}", k.selector),
            k.selector.len(),
            k.selector.capacity(),
            heap_of(k),
            allocs_of(k),
            struct_bytes + heap_of(k),
        );
    }
    println!();
    println!("  AND THE GENERATION PATH, WHICH THE PRE-SIZE WAS MEANT TO COVER");
    println!();
    let widest = matrixcache::CacheKey::page_with_slot_generation(
        1,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        Some(u32::MAX),
        Some(u64::MAX),
    );
    println!(
        "    selector {:?}\n      len {} / cap {}",
        widest.selector,
        widest.selector.len(),
        widest.selector.capacity()
    );
    println!("    the pre-size is 64, and this shape's maximum is 82");
    println!("    MatrixCache main already computes this per component; the pin is behind the fix");
    println!();

    // ---- THE PRE-SIZE DOES NOT COVER THE SHAPE IT NAMES. A capacity above the 64 it asked for
    // ---- is the reallocation it was written to avoid, observed rather than argued.
    assert!(
        widest.selector.len() > 64,
        "the widest generation selector is {} bytes, which fits the 64-byte pre-size after all. \
         Then the shape changed -- re-derive the maximum from `page_selector` before trusting the \
         claim below",
        widest.selector.len()
    );
    assert!(
        widest.selector.capacity() > 64,
        "the widest generation selector is {} bytes and its capacity is {}, so it no longer \
         reallocates past a 64-byte pre-size.\n\
         \n\
         THIS IS THE GOOD OUTCOME AND THIS ARM SHOULD NOW SHRINK. The MatrixCache pin has been \
         bumped to a revision that computes the selector capacity per component, which their main \
         already did while this crate pinned a revision 105 commits behind it. Delete both \
         capacity assertions and the generation block above; keep the information-content \
         comparison, which the bump does not change -- the page key is still five integers \
         rendered into two strings, and after the bump that is 127 B against 32 B rather than \
         172 B against 32 B",
        widest.selector.len(),
        widest.selector.capacity()
    );

    println!("  THE PAGE KEY AGAINST ITS INFORMATION CONTENT");
    println!("    carried as integers              {PAGE_INFORMATION_BYTES:>5} B   shard+slab+offset+length+bucket, no allocation");
    println!("    carried as it is today           {:>5} B   struct {struct_bytes} + heap {}", struct_bytes + heap_of(&physical), heap_of(&physical));
    println!(
        "    ratio                            {:>5.2}x  and {} allocations per construction",
        (struct_bytes + heap_of(&physical)) as f64 / PAGE_INFORMATION_BYTES as f64,
        allocs_of(&physical),
    );
    println!();
    // ---- WHAT THE PIN BUMP WOULD BUY, and what it would not. The exact-capacity fix upstream
    // ---- removes the slack; it does not remove the rendering.
    let exact_heap = physical.record_key.len() + physical.selector.len();
    println!("  AND WHAT THE PENDING PIN BUMP CHANGES");
    println!("    today                            {:>5} B   selector capacity {} for {} bytes used",
             struct_bytes + heap_of(&physical), physical.selector.capacity(), physical.selector.len());
    println!("    with the exact-capacity fix      {:>5} B   {:.2}x, still two allocations",
             struct_bytes + exact_heap,
             (struct_bytes + exact_heap) as f64 / PAGE_INFORMATION_BYTES as f64);
    println!("    carried as integers              {PAGE_INFORMATION_BYTES:>5} B   1.00x, no allocation");
    println!();

    // ---- THE PARTS ARE ALL INTEGERS. Asserted by parsing them back out, so "it is just integers
    // ---- rendered as text" is demonstrated rather than asserted from reading the constructor.
    let slab_digits = physical
        .record_key
        .strip_prefix("segment-")
        .expect("a page record_key is the segment prefix plus digits");
    assert!(
        slab_digits.parse::<u64>().is_ok(),
        "the page record_key's tail {slab_digits:?} is not a u64, so it carries something other \
         than a slab id and the comparison below would be measuring the wrong thing"
    );
    let selector_parts: Vec<&str> = physical
        .selector
        .strip_prefix("slot-")
        .expect("a page selector starts with the slot prefix")
        .split(':')
        .collect();
    assert_eq!(
        selector_parts.len(),
        3,
        "the page selector {:?} did not split into bucket:offset:length",
        physical.selector
    );
    for part in &selector_parts {
        assert!(
            part.parse::<u64>().is_ok(),
            "selector component {part:?} is not an integer, so the selector carries something \
             other than the three numbers this arm prices"
        );
    }

    // ---- THE MARGIN IS REAL AND LARGE. Pinned as a floor rather than an exact figure, because the
    // ---- struct size is a layout detail of someone else's crate and the capacities depend on the
    // ---- slab id's digit count. A floor still fails if the representation is ever made compact.
    let page_total = struct_bytes + heap_of(&physical);
    assert!(
        page_total >= 3 * PAGE_INFORMATION_BYTES,
        "a page cache key now costs {page_total} B against {PAGE_INFORMATION_BYTES} B of integers, \
         under 3x. If the representation was made compact that is the good outcome and this floor \
         should be deleted along with the arm -- but check `size_of::<CacheKey>()` first, because \
         a smaller struct with the same two String fields means the layout changed and not the \
         representation"
    );
    assert_eq!(
        allocs_of(&physical),
        2,
        "a page key made {} heap allocations, not the 2 this arm prices. These are paid on every \
         page cache get and put, so the count is the part that matters most",
        allocs_of(&physical)
    );

    // ---- AND THE FOUR FIELDS ARE NOT THE WASTE. The logical key carries the caller's own string,
    // ---- which has to be a string; its heap is the key itself, not a rendering of integers.
    assert_eq!(
        logical.record_key, key,
        "the logical record_key is no longer the caller's key, which is the one case where a \
         String is carrying something that genuinely is text"
    );
}
