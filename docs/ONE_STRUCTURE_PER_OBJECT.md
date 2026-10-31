<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# The resident index: one structure per object

The shard holds several resident structures keyed per object. This is the design note for holding
one, with the arithmetic for what that is worth. Every figure is labelled **measured** or
**modelled**, because mixing those is how this work has gone wrong before.

## The published figure, and what it divides by

`the_amplification_set_at_two_arms_four_times_apart` is the authoritative reading: resident bytes
per key over **the index's own encoded bytes** per key, taken on two corpora 4.0x apart with both
hash tables at exactly 100% usable load, after a full dump round. **Measured today:**

```
marginal: 212.4 B/key resident over 30.2 B/key encoded
ASYMPTOTE: 7.04x        4.0x allows 120.7 resident B/key, so it needs 91.7 B/key removed (43%)
```

The steady-state rows, as marginal slopes:

| row | B/key | share |
|---|---|---|
| `bucket_index.object_block_lookup` | 78.4 | 36.9% |
| `bucket_index.bucket_map` | 55.2 | 26.0% |
| `strings` | 46.9 | 22.1% |
| `Arc<str>` key characters | 32.0 | 15.1% |
| **total** | **212.4** | |

`dirty_objects`, `features` and `bucket_recency` all have a **zero slope** at the steady state.
`dirty_objects` measures 73.1 B/key on a freshly seeded store and **0 after a dump** -- the arm
asserts `dirty_objects.len() == 0` and refuses to take a reading otherwise. So a 314.4 B/key figure
is describing a store that has never persisted anything; the per-key subject is **212.4**.

## THE TRIGGER CANNOT REACH 4.0x, AND THAT IS NOW MEASURED

The obvious lever is to release the derived index rather than shrink it. It does not work, and the
reason is structural: **the denominator is the index's own encoded image, so content removed from
memory is removed from the image too.**

`what_a_release_does_to_the_published_amplification_ratio` drives the production dump-then-release
over 40,000 objects and weighs both sides. **Measured:**

| | numerator (resident) | denominator (encoded) | ratio |
|---|---|---|---|
| seeded | 12,587,360 | 1,231,388 | **10.22x** |
| after dump+release | 4,752,368 | 712,446 | **6.67x** |
| after reading all 40,000 keys back | 4,752,368 | 712,446 | **6.67x** |

The release frees **62.2% of the numerator** -- a real memory win -- but it also takes **42.1% off
the denominator**, so the ratio moves only 10.22x -> 6.67x. Counting recovered bytes would have
called this a 62% win; the ratio says 35%.

Two further results from the same run:

* **The release is durable under reads.** A full sweep of all 40,000 keys put back **0.0%**. Every
  key answered; `pages` stayed at 1,580 of the original 40,080. Existing arms check readability on a
  16-key sample -- 0.04% of this corpus -- so this is the first evidence at full coverage.
* It is paid in **read latency**, not memory: the swept reads are served without repopulating the
  index, and that run took 33 minutes.

So the release stays worth having as a memory lever. It is not a route to 4.0x. **Only numerator-side
levers move this ratio**, because they hold the same content in fewer bytes rather than holding less
content.

## The design: one entry per object

```rust
struct ObjectEntry {
    slot: ObjectSlot,   // 24 B, pinned -- the address
    refs: BlockRefs,    // 24 B, pinned -- One inline | Many out of line
}                       // 48 B
```

`object_block_lookup` goes. The representation is already compact: `BlockRefs` is a tagged union,
`One(BlockLookupRef)` inline and `Many(Vec<_>)` out of line, pinned at 24 bytes with
`size_of::<BlockRefs>() == size_of::<Vec<BlockLookupRef>>()` pinned beside it. Nothing here changes
it. What costs is **where it is held**: a second B-tree keyed by the same object key the slot map is
already keyed by, holding 40 bytes of payload at 78.4 B/key -- so roughly half that row is node.

Page-to-object identity is neither dropped nor re-represented. It stays keyed by the object key and
moves into the entry the slot map already holds. That matters because the identity is load-bearing:
`impl Serialize for CoreIndex` builds `BlockKeysByPosition::from_lookup` in every build and composes
each block's written key from it, so an index written without it names no object.
`ObjectBlockLookup`'s doc refuses two changes on three pinned grounds -- addressing the identity by a
`u64`, and dropping it. **Relocation is neither**, and the written field can be composed from the
slot map instead.

This also keeps the denominator fixed: the same identity and the same refs are serialized, so the
encoded image does not move and the whole saving lands on the ratio.

## The arithmetic

| step | B/key | status |
|---|---|---|
| steady-state marginal | **212.4** | measured |
| 1. fold `BlockRefs` into the slot entry | **-51.0** | **measured** |
| | **161.4 -> 5.34x** | |
| 4.0x needs | **120.7** | measured |
| still to find | **-40.7** | |

Step 1 measured by `what_folding_the_block_refs_into_the_slot_would_save`: the bucket goes from
`(16 + 24) + 1 = 41` to `(16 + 48) + 1 = 65` bytes, so the slot row rises 46.9 -> 74.3 while 78.4
goes to zero. **50.9 B/key at 1,792 keys and 50.9 at 7,168, 51.0 marginal** -- the two corpus sizes
agree, which is what distinguishes a per-key saving from a constant divided by keys. The arm's
isolated construction reproduces the live engine's slopes for both rows to 0.1 B/key.

The remaining 40.7 B/key has to come from the two rows left, both numerator-side:

* **`bucket_map`, 55.2 B/key.** The entry is a `(u64 handle, BlockIndex)` pair at 32 bytes in a
  sorted `Vec`; the rest is `BucketNode` container. Unpriced.
* **`Arc<str>` key characters, 32.0 B/key.** `arc_str_bytes` charges
  `((16 + len + 7) / 8) * 8`, so a ~12-character key costs 32 where its bytes are 12. An arena would
  pay the bytes plus a span. Ceiling is roughly -12 to -20 B/key, and it needs a raw-entry table to
  hash a span against external bytes -- this crate depends on none, and std's `HashMap` has no
  stable raw-entry API. It also needs arena compaction, where an `Arc` refcount frees characters for
  nothing today.

Neither is priced. **4.0x is not yet a reachable number; it is 40.7 B/key away with two candidate
rows and no measurement on either.**

## What is not assumed

**`features` is not folded in.** It is keyed by feature name, not object key, so the full-overlap
argument that makes step 1 a win does not apply, and `what_consolidating_three_holders_would_cost`
shows consolidation **costs** below 78% key overlap against a measured 50%. Its steady-state slope
is zero in any case.

**The fold is not free.** The lookup is nested, model then key, and is walked by model where a flat
per-object entry needs a scan. That cost is real and unpriced, which makes -51.0 a **ceiling**. The
write path and the serializer must both take their identity from the entry.

**The sawtooth makes any single reading unsafe.** A hashbrown table costs per bucket over
`next_pow2(ceil(n / (7/8)))` buckets, so the slot row is 46.9 B/key at full load and 93.8 just past
a doubling. At 40,040 keys it sits at 63.7% load. Quote a per-key figure without its table phase and
it carries up to 2x of noise -- which is why every arm here uses two corpora at a matched phase.

## Status

Step 1 is measured and not implemented. The release is measured and ships off. The two rows that
would close the remaining 40.7 B/key are unpriced.
