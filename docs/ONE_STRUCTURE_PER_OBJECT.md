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
  key answered; `blocks` stayed at 1,580 of the original 40,080. Existing arms check readability on a
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

## Measured: every row now has a figure

The note above left two rows modelled. Both are measured now, and the measurement **killed one
lever outright, reclassified another, and found two that nothing had priced**.

### `bucket_map` decomposes exactly, and its container saturates

`which_term_carries_the_bucket_map_slope` reproduces the authoritative **55.2 B/key** with a
residual of **0.0**, so nothing in the row is unaccounted:

| term | B/key | unit |
|---|---|---|
| `block_index Many: Vec<(u64, BlockIndex)>` | **33.8** | blocks in a list (exactly 32.0 a block) |
| `object_index Many: Box<ObjectSlots>` | **19.4** | multi-object buckets |
| `BTreeMap node arrays (BucketNode held INLINE)` | 3.3 | buckets |
| `block_index One: Box<BlockIndex>` | -1.3 | single-page buckets (empties as buckets fill) |
| residual | **0.0** | |

**The container cannot be reclaimed, and this is the number that says so.** The node-array term is
the largest in absolute bytes -- 138,312 B at 1,792 keys -- and its unit is **buckets**, which the
shipped range caps at 1,024. Its per-key cost runs **77.2 at 1,792 keys to 21.8 at 7,168**, heading
to zero. A row that vanishes from the asymptote offers nothing to a target set against the
asymptote. The earlier guess that ~23 B/key of this row was reclaimable container was wrong **in
kind**, not only in size.

### Boxing the wide arm of `BlockRefs` saves nothing

Measured **0.0 B/key in both holders**: 78.4 -> 78.4 in the lookup that ships, 74.3 -> 74.3 in the
folded entry. The union is 24 bytes either way.

Its width comes from the **inline** arm -- `One(BlockLookupRef)` at 16 bytes plus a tag, aligned to
8 -- not from the `Vec`. The pin `size_of::<BlockRefs>() == size_of::<Vec<BlockLookupRef>>()` reads
as though it names a cause; both are simply 24. Narrowing the union needs `BlockLookupRef` at 12
bytes, and its own pin already refuses that: a `u32` beside a `u64` pads to the alignment of the
`u64`, so narrowing either field moves nothing. The arm asserts **equality**, so the dead lever
fails loudly if a change ever opens it.

### Two levers nothing had priced, both inside `bucket_map`

**The object slot is 16 bytes where 8 would do.** `ObjectSlots` holds `Vec<Option<u64>>`, and
`Option<u64>` has no niche. `what_a_slot_array_costs_under_churn` measures `32 + 16 a slot` against
`32 + 8`, and proves the placeholders cost nothing that persists -- 1.000 slots an object at both
widths across four churn shapes, transient at most 2.000 while a hole is open. So the cost **is the
width**, worth roughly **8 B/key**. That arm names a reserved id value as the lever and object ids
being full-range FNV hashes as what forbids it. A separate validity bitmap is a different mechanism
-- 8 bytes and a bit -- and nothing refuses it.

**The per-page handle is derivable.** The list pair is `(u64 handle, BlockIndex)` at exactly 32.0
bytes a block. The handle is `block_index_handle(object_key, &block)`, which the code already
recomputes when it needs to know which object owns a block. So the stored `u64` is a **cache, not an
identity**: droppable for a rescan of the bucket's objects, worth another **8 B/key**. The trade is
bytes against a rescan, not a correctness question.

### What the baseline itself understates

`list capacity (unseen)` read **8,584 against 7,168** blocks in the list arm -- about **20%** of the
block lists is reserved capacity. A clone probe weighs a `Vec` at its LENGTH, while the production
resident model charges `capacity * size_of::<(u64, BlockIndex)>()`. So that row's true resident cost
is nearer **40.5 B/key than 33.8**, and the **212.4 B/key baseline understates the numerator**. This
cuts against the target rather than for it, and the arm prints the capacity so it cannot be quietly
omitted.

### The arithmetic, with every row measured

| lever | B/key | status |
|---|---|---|
| fold `BlockRefs` into the slot entry | **-51.0** | measured |
| the object slot width, 16 -> 8 | **~-8** | measured basis |
| the per-page handle, derivable | **~-8** | measured basis, rescan unpriced |
| key characters in an arena | ~-29 | ceiling (payload plus a narrower table key) |
| boxing the wide arm | **0** | **measured -- dead** |
| `bucket_map` container | **0** | **measured -- saturates, dead** |
| | **~-96** | against **-91.7** needed |

So the target lands at roughly **3.85x** with both `bucket_map` levers taken, and **~4.12x** without
the handle. **4.0x turns on the handle**, and on the arena figure being a ceiling rather than a
promise.

None of it is implemented. And the ~20% reserved capacity above means the numerator these levers are
subtracted from is itself understated, so a figure that lands just under 4.0x on this baseline is
not yet a figure that lands under 4.0x in production.
