// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER A BLOCK'S BUCKET SHOULD BE A FUNCTION OF ITS KEY ALONE. IT SHOULD NOT.
//!
//! # THE QUESTION
//!
//! A block's routing bucket is `start + FNV-1a-64(object_key) % (end - start + 1)`
//! (`engine/hashing.rs`), so the key is folded INTO the configured range and a key's bucket moves
//! when the range moves. #1977 measured that as a refutation of recomputation: 4,000 keys written
//! on `0..4294967295` and reopened on `0..1023` gave 4,000 of 4,000 recomputed buckets wrong. This
//! module asks the stronger question underneath it -- should the dependence exist at all? -- and
//! answers no, on four measurements that each stand on their own.
//!
//! # 1. THE RANGE IS A SIZING KNOB, NOT AN OWNERSHIP BOUNDARY
//!
//! Read the declarations rather than the names. `DEFAULT_END_ROUTING_BUCKET` (`lib.rs`) says the
//! value "sets the MODULUS"; `Engine::shard_routing_range` says the range "is not a filter over a
//! fixed answer, it is an argument to the placement function"; `docs/runtime_tuning.md` titles the
//! section "a large lever on resident memory" and sweeps the end bucket as a tuning candidate.
//! Nothing anywhere filters a key out of a shard for falling outside the range.
//!
//! And the engine already HAS a range-independent id, one layer up: `client::routing`'s
//! `bucket_id_for_key` is `crc64_jones(key) >> 34` over `1 << 30` buckets, a function of the key
//! and nothing else, and the metaserver publishes a contiguous slice of it per shard. Which shard
//! a key lives on is range-independent today. What `block_routing_bucket` decides is something
//! else entirely: how many keys share one unit of dump, eviction, invalidation and log-reclaim
//! work INSIDE one shard. The two are different concepts at different layers, and treating the
//! intra-shard filing bucket as though it were the inter-shard id is the mistake this module
//! exists to stop.
//!
//! # 2. AT THE SHIPPED DEFAULT A RANGE-FREE ID IS THE SAME FUNCTION, SO IT BUYS NOTHING
//!
//! `bucket_for_object` is `start + hash % count`. At the shipped default `start` is 0 and `count`
//! is 1,024, so `block_routing_bucket(key, 0, 1023)` and a range-free `hash(key) % 1024` are the
//! SAME ARITHMETIC. `a_range_free_id_is_the_same_function_as_the_shipped_default` asserts that
//! key for key at both corpus sizes, and it is why the pages-per-bucket distribution is not
//! measured again here: it cannot differ. `routing_range_default.rs` already reports that
//! distribution as a histogram with p50/p90/p99/MAX and an asserted denominator over five ranges
//! at 4,000 and 40,000 records, and the published table is `docs/runtime_tuning.md` -- 39 pages in
//! the p50 bucket and 50 in the MAX at 40,000 records on `0..1023`, against 1 and 1 on the whole
//! keyspace. A range-free id at a fixed count of 1,024 reproduces the `0..1023` column exactly,
//! because it IS that column.
//!
//! # 3. WHAT IT COSTS IS THE ONLY FILL LEVER THE OPERATOR HAS
//!
//! The fill is `records / bucket-count`, and `docs/runtime_tuning.md` states the consequence
//! plainly: "no fixed bucket count is right for every corpus ... `1023` sits at 3.91 pages a
//! bucket for a 4,000-record store and 39.06 for a 40,000-record one." A range-free id needs a
//! FIXED count to keep buckets full, and a fixed count is exactly what that sentence rules out.
//! `a_fixed_bucket_count_cannot_reproduce_the_fills_the_range_produces` prints the four fills the
//! shipped candidates produce from one key set and shows no single count reaches them.
//!
//! So a range-free id does not remove the knob. It SPLITS it into two -- a count that sizes and a
//! window that owns -- and the engine would ship both where it ships one.
//!
//! # 4. TWO OF THE THREE DUPLICATIONS IT WOULD PAY FOR RECOVER NO BYTES
//!
//! `BucketNode::routing_bucket` duplicates the `BucketMap` key and `BlockAddress::routing_bucket`
//! duplicates it again per block, so removing them looks like the saving that pays for the change.
//! It is zero for EACH FIELD ON ITS OWN, and for the reason both structures already have written
//! down: only a change that takes the tail to eight bytes or fewer, or a whole word out of the
//! eight-aligned group, moves either one. `BucketNode` is 80 bytes of eight-aligned field and a
//! 6-byte tail rounded to 8 -- take four out of the tail and 2 still rounds to 8, so that one is zero
//! and stays zero. `BlockAddress` WAS a 29-byte payload in 32 bytes, and taking four out leaves 25,
//! which rounds back to 32 -- also zero.
//!
//! IT IS NOT ZERO FOR THE PAIR, and the address is 24 bytes now because of it. The routing bucket left
//! in the same change that narrowed `block_id` from 32 bits to 16: four bytes off 29 is 25 and rounds
//! back, two bytes off is 27 and rounds back, and SIX bytes off is 23, which rounds to 24. A verdict on
//! a field in isolation is not a verdict on the field, and this is the third alignment step in this
//! campaign crossed only by a combination.
//! `removing_the_routing_bucket_recovers_no_bytes_alone_and_eight_in_combination` asserts all three
//! counterfactuals over the widths the compiler reports, not over a hand-written list.
//!
//! THE THIRD CARRIER IS THE EXCEPTION, AND IT IS THE ONE USEFUL NUMBER HERE.
//! `BlockLookupRef` -- the entry `object_block_lookup` holds, and the one
//! `bucket_index_target_buckets_for_object_key` reads the bucket out of -- is a `u32` bucket beside
//! a `u64` ref key, 12 bytes of field in 16. Drop the bucket and a bare `u64` is 8: the width
//! HALVES, with no wire change, because that lookup is not persisted.
//!
//! AND THAT SAVING DOES NOT NEED A RANGE-FREE ID. #1980 retracted #1977's reader claim --
//! `ShardState::routing_range()` is on the state the function already takes, and a recomputed
//! bucket matched 408 of 408 objects -- so the 8 bytes sit behind recomputation within a KNOWN
//! range, already measured correct, not behind changing what a bucket id is. The dangerous change
//! and the only real saving are not the same change, and that is the most useful sentence in this
//! module.
//!
//! # WHAT A RE-FILE WOULD ACTUALLY COST, AND IT IS THE ONE PLEASANT SURPRISE
//!
//! It is a REINDEX, not a data move, and by a wider margin than the `BlockAddress` field suggests.
//! A block's bytes live at `(slab, offset)` in the address word, chosen by the single active slab
//! and rolled by SIZE alone (`block_store/append.rs`) -- the bucket takes no part in placement. And
//! the on-disk block record carries neither the bucket nor the object id nor the key:
//! `encode_block_record` takes both identities and discards them (`let _ = (object_id,
//! routing_bucket);`), and `parse_block_record_header` hardcodes both absent. So no slab byte would
//! move and none would go stale.
//!
//! The bucket comes back from disk in exactly three places -- the index snapshot (as the
//! `BucketMap` key; `BlockAddressWire`'s `rs` was a fourth until the slot was retired, and an
//! address carries no bucket at all now), the index log's `IndexItem`, and the WAL's
//! `WalOutcomeItem` -- and all three carry `object_key` as a string beside it. A re-file is
//! therefore a walk of the index recomputing one `u32` per page.
//!
//! THAT IS NOT A REASON TO DO IT. The migration being cheap is not an argument that the
//! destination is better, and #1973's stamp already collects the whole benefit a re-file could:
//! the range a store was built under is recorded beside its index, read BEFORE the decode, and a
//! disagreeing one is refused. The range is already known at load. What the stamp does NOT give is
//! a bucket that is stable ACROSS ranges, and the measurements above say the engine does not want
//! one.
//!
//! # AND THE FIELD IS NOT ONLY A DUPLICATE
//!
//! `validate_bucket_ownership_index_from_entries` compares the bucket a page CLAIMS on its address
//! against the bucket the index FILED it under. Those are two independent statements of one fact,
//! and the comparison is the only thing that can notice them diverging -- the mis-filing #1973 and
//! #1977 both measured. Dropping the stamped copy to save bytes it does not save would also retire
//! the cross-check that finds the defect class.

use super::*;
use crate::engine::hashing::{block_routing_bucket, routing_bucket_count, stable_object_hash};
use crate::block_store::BlockAddress;
use crate::engine::state::{BlockLookupRef, BucketNode};
use std::collections::BTreeMap;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973),
/// the shipped default and the operator's range.
const OPERATOR_END: u32 = 1_023;

/// The buckets that end produces, and the fixed count a range-free id would have to choose.
const OPERATOR_COUNT: u32 = 1_024;

/// The only range a store built before #1973's stamp can have been built on, frozen for ever as
/// `routing_range_stamp::LEGACY_END_ROUTING_BUCKET`.
const LEGACY_END: u32 = u32::MAX;

/// The corpus sizes every figure in this campaign is taken at.
const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// The key set `routing_range_default.rs` sweeps and `docs/runtime_tuning.md` publishes, so a
/// figure here is comparable with the figure there.
fn routed_keys(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("fill-{i:06}")).collect()
}

/// A bucket id derived from the key ALONE, over a fixed count. The scheme under study.
fn range_free_bucket(key: &str, fixed_count: u32) -> u32 {
    (stable_object_hash(key) % u64::from(fixed_count)) as u32
}

/// Pages held per bucket, as bucket counts keyed by pages held, with the percentiles taken over
/// the BUCKET population and the denominator printed on every line.
///
/// One page per key, which is what a routed string key produces -- so this is the same quantity
/// `routing_range_default.rs` measures through a populated engine, computed from the id function
/// instead. `the_fill_instrument_reproduces_the_published_figure` is what says the two agree.
#[derive(Debug, Default)]
struct Fill {
    counts: BTreeMap<usize, usize>,
}

impl Fill {
    fn of(keys: &[String], bucket: impl Fn(&str) -> u32) -> Self {
        let mut per_bucket: BTreeMap<u32, usize> = BTreeMap::new();
        for key in keys {
            *per_bucket.entry(bucket(key)).or_default() += 1;
        }
        let mut counts: BTreeMap<usize, usize> = BTreeMap::new();
        for held in per_bucket.values() {
            *counts.entry(*held).or_default() += 1;
        }
        Self { counts }
    }

    /// Occupied buckets. The denominator, and it is printed everywhere it is used.
    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn pages(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn max(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    /// A count off the histogram, never an interpolation: every value it returns is a page count
    /// some bucket actually holds.
    fn percentile(&self, fraction: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let target = ((buckets as f64) * fraction).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *held;
            }
        }
        self.max()
    }

    fn line(&self, label: &str) {
        let buckets = self.buckets();
        println!(
            "  {label:<44} occupied {buckets:>6} | pages {:>6} | p50 {:>4} p90 {:>4} p99 {:>4} \
             MAX {:>4}",
            self.pages(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
        );
        let printed: usize = self.counts.values().copied().sum();
        assert_eq!(
            printed, buckets,
            "{label}: the histogram's rows sum to {printed} over a denominator of {buckets}"
        );
    }
}

/// THE SHIPPED DEFAULT AND A RANGE-FREE ID ARE THE SAME FUNCTION, KEY FOR KEY.
///
/// `bucket_for_object` is `start + hash % count`, `start` is 0 at the shipped default and
/// `routing_bucket_count(0, 1023)` is 1,024 -- so there is nothing left for a range-free
/// `hash % 1024` to differ on. This is the measurement that makes the pages-per-bucket comparison
/// unnecessary rather than unreported: the distributions cannot differ because the ids do not.
///
/// Stated over both corpus sizes with the denominator printed, because a value-identity assertion
/// over an empty key set passes for the wrong reason.
#[test]
fn a_range_free_id_is_the_same_function_as_the_shipped_default() {
    assert_eq!(
        OPERATOR_COUNT,
        routing_bucket_count(0, OPERATOR_END),
        "the shipped default's bucket count moved, and every figure in this module assumes 1,024"
    );

    println!("\n=== a range-free id against the shipped default 0..{OPERATOR_END} ===");
    for records in [SMALL, LARGE] {
        let keys = routed_keys(records);
        let mut agreed = 0usize;
        for key in &keys {
            if block_routing_bucket(key, 0, OPERATOR_END) == range_free_bucket(key, OPERATOR_COUNT)
            {
                agreed += 1;
            }
        }
        println!("  {records:>6} keys: agreed on {agreed} of {}", keys.len());
        assert_eq!(
            keys.len(),
            records,
            "the fixture produced {} keys and not {records}",
            keys.len()
        );
        assert_eq!(
            agreed,
            keys.len(),
            "at the shipped default the two schemes are the same arithmetic, so they must agree \
             on every one of {} keys; they agreed on {agreed}",
            keys.len()
        );
    }
}

/// THE CONTROL ON THE EXPLANATION: the mechanism is the MODULUS, so a comparison that changes no
/// modulus must find no disagreement at all.
///
/// Every disagreement figure below is a count of keys placed differently by two schemes. If that
/// counter fired on the key set rather than on the scheme, it would fire here too -- the same
/// scheme against itself, over the same keys. It reports 0.00%, and the assertion below requires
/// exactly that rather than merely requiring it to be small.
#[test]
fn the_same_scheme_against_itself_disagrees_on_nothing() {
    println!("\n=== control: one scheme against itself ===");
    for records in [SMALL, LARGE] {
        let keys = routed_keys(records);
        let disagreements = keys
            .iter()
            .filter(|key| {
                block_routing_bucket(key, 0, OPERATOR_END)
                    != block_routing_bucket(key, 0, OPERATOR_END)
            })
            .count();
        let percent = 100.0 * disagreements as f64 / keys.len() as f64;
        println!(
            "  {records:>6} keys: {disagreements} of {} placed differently ({percent:.2}%)",
            keys.len()
        );
        assert!(!keys.is_empty(), "the control ran over no keys at all");
        assert_eq!(
            0, disagreements,
            "the disagreement counter fired {disagreements} times comparing a scheme with \
             itself, so it is measuring the key set and not the scheme"
        );
    }
}

/// NO ARITHMETIC MAPS A LEGACY BUCKET ONTO A FIXED-COUNT ONE, SO A RE-FILE NEEDS THE KEY.
///
/// A store with no stamp is honoured on the whole keyspace, whose count is `u32::MAX` -- not a
/// multiple of 1,024. So `(hash % 4294967295) % 1024` is not `hash % 1024`, and a legacy store's
/// recorded bucket carries no information about where a range-free scheme would put the same key.
///
/// The witness is the one `shard_carried_range.rs` already pins: `inner-000000` sits in bucket 398
/// on `0..1023` and 1,422,005,296 on the whole keyspace, and folding the wide bucket into 1,024
/// gives 48 -- neither the narrow bucket nor anything else useful.
///
/// MEASURED, and the two corpus sizes do not agree on a rate: folding the stored legacy bucket
/// into 1,024 recovers the range-free bucket for 155 of 4,000 keys (3.88%) and 255 of 40,000
/// (0.64%). So 96.12% and 99.36% of a legacy store's pages could not be re-filed from what is
/// stored.
///
/// THE RESIDUAL IS NOT A NESTING RELATION AND MUST NOT BE READ AS ONE. Nested schemes would
/// recover EVERY key, not a twenty-fifth of them -- and a relation that held would not produce two
/// different rates on two key sets. It is consistent with `% 1024` depending only on the hash's
/// low ten bits while FNV-1a avalanches its high bits incompletely over a key family differing in
/// six characters, but that mechanism is not what this test rests on: the figure that matters is
/// the fraction that does NOT convert, and the assertion requires only a strict majority so that
/// it cannot be satisfied by a rate chosen after seeing the answer.
///
/// This is what makes the migration a walk of the index rather than a computation over the
/// buckets already stored, and it is also the half of #1977's refutation that generalises: the two
/// schemes are not nested, so no reader can convert between them.
#[test]
fn no_arithmetic_maps_a_legacy_bucket_onto_a_fixed_count_bucket() {
    const WITNESS: &str = "inner-000000";
    let narrow = block_routing_bucket(WITNESS, 0, OPERATOR_END);
    let wide = block_routing_bucket(WITNESS, 0, LEGACY_END);
    let folded = wide % OPERATOR_COUNT;
    println!("\n=== the two schemes are not nested ===");
    println!(
        "  {WITNESS}: narrow bucket {narrow}, legacy bucket {wide}, legacy folded into \
         {OPERATOR_COUNT} = {folded}"
    );
    assert_eq!(398, narrow, "the pinned narrow bucket for {WITNESS} moved");
    assert_eq!(
        1_422_005_296, wide,
        "the pinned legacy bucket for {WITNESS} moved"
    );
    assert_ne!(
        narrow, folded,
        "folding the legacy bucket into the fixed count recovered the narrow bucket, which would \
         make the two schemes nested and this whole test pointless"
    );

    for records in [SMALL, LARGE] {
        let keys = routed_keys(records);
        let recoverable = keys
            .iter()
            .filter(|key| {
                block_routing_bucket(key, 0, LEGACY_END) % OPERATOR_COUNT
                    == range_free_bucket(key, OPERATOR_COUNT)
            })
            .count();
        println!(
            "  {records:>6} keys: {recoverable} of {} range-free buckets recoverable from the \
             stored legacy bucket by arithmetic ({:.2}%)",
            keys.len(),
            100.0 * recoverable as f64 / keys.len() as f64
        );
        // A STRICT MAJORITY MUST BE UNRECOVERABLE, which is the claim that matters and the only
        // one this comparison can support. Nested schemes would recover the range-free bucket for
        // EVERY key; a residual minority is not a nesting relation and cannot be used as one,
        // because a re-file that is wrong for most keys is wrong. The threshold is a strict
        // majority rather than a rate, so that it cannot be a number chosen after seeing the
        // answer.
        let unrecoverable = keys.len() - recoverable;
        assert!(
            unrecoverable > keys.len() / 2,
            "arithmetic over the stored legacy bucket recovered the range-free bucket for \
             {recoverable} of {} keys, leaving only {unrecoverable} unrecoverable -- at a strict \
             majority the two schemes would be usefully convertible and the migration claim in \
             this module would be wrong",
            keys.len()
        );
    }
}

/// A FIXED BUCKET COUNT CANNOT REPRODUCE THE FILLS THE RANGE PRODUCES, WHICH IS THE COST.
///
/// The range's value to an operator is that it sizes the bucket population for the corpus: the
/// fill is `records / bucket-count`, and `docs/runtime_tuning.md` says in as many words that no
/// fixed count is right for every corpus. A range-free id must pick one count and live with it.
///
/// Printed as the four shipped candidates over one key set at both corpus sizes, with p50, p99 and
/// MAX and the occupied-bucket denominator on every row, so the spread is visible rather than
/// asserted in prose. The assertion is that the four candidates produce four DIFFERENT p50 fills
/// at the large corpus -- if a single count could serve them all, they would not.
#[test]
fn a_fixed_bucket_count_cannot_reproduce_the_fills_the_range_produces() {
    const CANDIDATES: [u32; 4] = [255, 1_023, 4_095, 65_535];

    println!("\n=== the fills the shipped candidate ranges produce ===");
    for records in [SMALL, LARGE] {
        println!("  --- {records} routed keys ---");
        let keys = routed_keys(records);
        let mut p50s = Vec::new();
        for end in CANDIDATES {
            let fill = Fill::of(&keys, |key| block_routing_bucket(key, 0, end));
            fill.line(&format!("0..{end} ({} buckets)", routing_bucket_count(0, end)));
            assert!(
                fill.buckets() > 0,
                "0..{end} at {records} records reached no bucket at all, so its row is not a \
                 measurement"
            );
            assert_eq!(
                fill.pages(),
                keys.len(),
                "0..{end} at {records} records accounted for {} pages over {} keys",
                fill.pages(),
                keys.len()
            );
            p50s.push(fill.percentile(0.50));
        }
        // And the whole keyspace, which is where the old default sat.
        let wide = Fill::of(&keys, |key| block_routing_bucket(key, 0, LEGACY_END));
        wide.line("0..4294967295 (the legacy range)");
        assert_eq!(
            1,
            wide.max(),
            "on the whole keyspace every key lands alone by construction, so the widest bucket \
             must hold one page and not {}",
            wide.max()
        );

        if records == LARGE {
            let distinct: std::collections::BTreeSet<usize> = p50s.iter().copied().collect();
            println!("  p50 fills at {records} records: {p50s:?}");
            assert_eq!(
                CANDIDATES.len(),
                distinct.len(),
                "the four candidate ranges produced {} distinct p50 fills, not {} -- if one \
                 fixed bucket count served them all, the range would not be a sizing lever",
                distinct.len(),
                CANDIDATES.len()
            );
        }
    }
}

/// THE FILL INSTRUMENT HERE REPRODUCES THE PUBLISHED FIGURE, WHICH IS WHAT LETS IT BE COMPARED.
///
/// `docs/runtime_tuning.md` publishes 39 pages in the p50 bucket and 50 in the MAX at 40,000
/// routed records on `0..1023`, measured through a populated engine by `routing_range_default.rs`.
/// This module computes the same quantity from the id function alone. If the two did not agree the
/// figures above would not be comparable with anything, and a pure-function instrument that
/// silently measured something else would look exactly like one that worked.
#[test]
fn the_fill_instrument_reproduces_the_published_figure() {
    let keys = routed_keys(LARGE);
    let fill = Fill::of(&keys, |key| block_routing_bucket(key, 0, OPERATOR_END));
    fill.line("0..1023 at 40,000 records");
    assert_eq!(
        LARGE,
        fill.pages(),
        "the instrument accounted for {} pages over {LARGE} keys",
        fill.pages()
    );
    assert_eq!(
        OPERATOR_COUNT as usize,
        fill.buckets(),
        "at 40,000 keys over 1,024 buckets every bucket is occupied, so the denominator must be \
         1,024 and not {}",
        fill.buckets()
    );
    assert_eq!(
        39,
        fill.percentile(0.50),
        "the published p50 fill at 40,000 records on 0..1023 is 39 pages; this instrument says {}",
        fill.percentile(0.50)
    );
    assert_eq!(
        50,
        fill.max(),
        "the published MAX fill at 40,000 records on 0..1023 is 50 pages; this instrument says {}",
        fill.max()
    );
}

/// REMOVING THE ROUTING BUCKET RECOVERS ZERO BYTES ON ITS OWN, AND EIGHT WHEN A SECOND NARROWING
/// LANDS BESIDE IT.
///
/// Both structures state the rule that decides this, and both state it because it was got wrong
/// before: only a change that takes the tail to eight bytes or fewer, or that takes a whole word out
/// of the eight-aligned group, moves either one. `routing_bucket` is a four-byte field in the tail of
/// both, and neither tail crosses a step when that field alone leaves.
///
/// THAT IS A VERDICT ON THE FIELD IN ISOLATION, WHICH IS NOT A VERDICT ON THE FIELD. The address is
/// 24 bytes now, not 32, because the bucket left in the same change that narrowed `block_id` from 32
/// bits to 16. Four bytes off a 29-byte payload is 25 and rounds back to 32; two bytes off is 27 and
/// rounds back to 32; SIX bytes off is 23 and rounds to 24. Each narrowing alone is worth exactly the
/// zero this test was written to report, and the pair is worth a whole word -- which is the third
/// time in this campaign that an alignment step has only been crossed by a combination, after the
/// bucket node's five bools and the address's two location words.
///
/// The three counterfactuals are kept below as arithmetic over the shape #1983 measured, so its
/// finding survives verbatim rather than being replaced by the one that followed it.
///
/// ASSERTED AGAINST THE WIDTHS THE COMPILER REPORTS. The group figures are cross-checked by
/// reconstructing `size_of` from them first, so a field moving between the groups fails here rather
/// than leaving the arithmetic adding up for the wrong reason. `BlockAddress`'s fields are private to
/// `block_store`, so its payload is what `every_byte_of_a_block_address_is_accounted_for` measures
/// with `offset_of!` and asserts; this test reconstructs the width from it and fails if either moved.
#[test]
fn removing_the_routing_bucket_recovers_no_bytes_alone_and_eight_in_combination() {
    use std::mem::{align_of, size_of};

    println!("\n=== what recovering the duplicated bucket id would free ===");

    // --- BlockAddress, LIVE: the merged slab word, a 32-bit length, a 16-bit block id and the
    // --- presence byte -- 15 bytes of payload in 16. This is the drift check, so it has to
    // --- describe the declaration as it stands.
    // ---
    // --- THE OBJECT ID LEFT THIS LIST, and this assertion is the reason the removal could not be
    // --- done by scanning for width pins: it names no literal width. It pairs a hand-listed field
    // --- SUM with `size_of`, so a sweep looking for a stale 24 beside `size_of::<BlockAddress>()`
    // --- passes straight over it and `cargo check` cannot see it either. It failed on the first
    // --- run of the suite, which is the only thing that could have found it.
    const ADDRESS_PAYLOAD: usize = 8 + 4 + 2 + 1;
    let address_width = size_of::<BlockAddress>();
    let address_align = align_of::<BlockAddress>();
    assert_eq!(
        15, ADDRESS_PAYLOAD,
        "the derived address payload is {ADDRESS_PAYLOAD}, not the 15 bytes \
         `every_byte_of_a_block_address_is_accounted_for` measures"
    );
    assert_eq!(
        ADDRESS_PAYLOAD.div_ceil(address_align) * address_align,
        address_width,
        "the address payload plus one rounding must reconstruct its width exactly, or the field \
         list above has drifted from the declaration"
    );

    // --- AND THE THREE COUNTERFACTUALS, over the shape this module measured: a 29-byte payload in
    // --- 32, carrying a four-byte routing bucket and a 32-bit block id.
    const ADDRESS_PAYLOAD_BEFORE: usize = 8 + 8 + 4 + 4 + 4 + 1;
    const WIDTH_BEFORE: usize = 32;
    /// What the pair below PRODUCED, which is not the width today.
    ///
    /// These three counterfactuals are about the 32 -> 24 step and they were anchored to
    /// `size_of::<BlockAddress>()`, which was 24 when they were written. A third step has landed
    /// since -- the object id left the address, 24 -> 16 -- and two of the assertions below broke
    /// on it. They were not wrong; they were pinned to a value that moves. So the era's result is
    /// named here and the counterfactuals are compared against THAT, with one assertion at the end
    /// connecting this era to the live width so the arithmetic still reaches the present.
    const WIDTH_AFTER_THE_PAIR: usize = 24;
    assert_eq!(29, ADDRESS_PAYLOAD_BEFORE, "the shape this module measured was 29 bytes of payload");
    assert_eq!(
        ADDRESS_PAYLOAD_BEFORE.div_ceil(address_align) * address_align,
        WIDTH_BEFORE,
        "the shape this module measured must reconstruct to 32, or the counterfactuals below are \
         about a structure this engine never had"
    );
    let round = |payload: usize| payload.div_ceil(address_align) * address_align;
    let without_bucket = ADDRESS_PAYLOAD_BEFORE - size_of::<u32>();
    let without_narrow_id = ADDRESS_PAYLOAD_BEFORE - 2;
    let without_both = ADDRESS_PAYLOAD_BEFORE - size_of::<u32>() - 2;
    println!(
        "  BlockAddress  {address_width} B now, {WIDTH_BEFORE} B before (payload \
         {ADDRESS_PAYLOAD_BEFORE} B)"
    );
    println!(
        "    without the routing bucket alone : payload {without_bucket} B -> width {} B  (saves {})",
        round(without_bucket),
        WIDTH_BEFORE - round(without_bucket)
    );
    println!(
        "    with a 16-bit block id alone     : payload {without_narrow_id} B -> width {} B  (saves {})",
        round(without_narrow_id),
        WIDTH_BEFORE - round(without_narrow_id)
    );
    println!(
        "    both together                    : payload {without_both} B -> width {} B  (saves {})",
        round(without_both),
        WIDTH_BEFORE - round(without_both)
    );
    assert_eq!(
        WIDTH_BEFORE,
        round(without_bucket),
        "removing the routing bucket ALONE must not change the width -- that is this module's \
         finding, and the payload arithmetic says it is impossible for it to"
    );
    assert_eq!(
        WIDTH_BEFORE,
        round(without_narrow_id),
        "narrowing the block id ALONE must not change the width either, for the same reason"
    );
    assert_eq!(
        WIDTH_AFTER_THE_PAIR,
        round(without_both),
        "the two TOGETHER must reconstruct the width that step produced; if they do not, the eight \
         bytes the address shed at that step are not the eight this arithmetic describes"
    );
    assert_eq!(
        8,
        WIDTH_BEFORE - WIDTH_AFTER_THE_PAIR,
        "the pair is worth a whole eight-byte step, and each half of it is worth zero"
    );
    // AND THE ARITHMETIC REACHES THE PRESENT, which is what stops the block above from becoming a
    // museum piece that no longer describes this type. One further step has landed since: the
    // object id left the address, which is another whole word and the only kind of change that
    // moves this number. Asserted against the compiler's width rather than restated, so a fourth
    // step fails here instead of leaving the chain looking complete.
    assert_eq!(
        16, address_width,
        "the live address width is {address_width}; the chain below describes 32 -> 24 -> 16 and a \
         further step has to be added to it rather than silently widening this number"
    );
    assert_eq!(
        8,
        WIDTH_AFTER_THE_PAIR - address_width,
        "the object id leaving is worth a whole eight-byte step too, which is why the pair above \
         and this one are each a word and not a fraction of one"
    );

    // --- BucketNode: 80 bytes of eight-aligned field and a 6-byte tail rounded to 8. Four out of
    // the tail leaves 2, which still rounds to 8.
    const NODE_EIGHT_ALIGNED: usize = 80;
    const NODE_TAIL: usize = 6;
    let node_width = size_of::<BucketNode>();
    let node_align = align_of::<BucketNode>();
    assert_eq!(
        8, node_align,
        "BucketNode's alignment moved and the arithmetic here assumes 8"
    );
    assert!(
        align_of::<u32>() < node_align,
        "the routing bucket is a u32, which must be less aligned than the node for it to sit in \
         the tail at all"
    );
    assert_eq!(
        NODE_EIGHT_ALIGNED + NODE_TAIL.div_ceil(node_align) * node_align,
        node_width,
        "the two group figures plus one rounding must reconstruct BucketNode's width exactly, or \
         a field has moved between the groups"
    );
    let node_tail_without = NODE_TAIL - size_of::<u32>();
    println!(
        "  BucketNode    {node_width} B, {NODE_EIGHT_ALIGNED} B eight-aligned + {NODE_TAIL} B \
         tail -> without the routing bucket tail {node_tail_without} B, width {} B",
        NODE_EIGHT_ALIGNED + node_tail_without.div_ceil(node_align) * node_align
    );
    assert_eq!(
        NODE_EIGHT_ALIGNED + node_tail_without.div_ceil(node_align) * node_align,
        node_width,
        "removing the routing bucket from BucketNode changed its width, which the tail arithmetic \
         says is impossible"
    );

    // --- What the duplication nominally costs, so the zero above is read against something.
    println!(
        "  nominal duplication at the shipped default: {} B over {OPERATOR_COUNT} buckets and \
         {} B over {LARGE} blocks -- both recovered in full by the aligner already",
        OPERATOR_COUNT as usize * size_of::<u32>(),
        LARGE * size_of::<u32>()
    );
}

/// AND ONE CARRIER WHERE DROPPING THE BUCKET ID DOES PAY: 16 BYTES BECOME 8.
///
/// `BlockLookupRef` is the entry `object_block_lookup` holds, and it is the structure
/// `bucket_index_target_buckets_for_object_key` reads the bucket out of. It carries a `u32` bucket
/// beside a `u64` ref key -- 12 bytes of field in 16, because the `u32` pads to the `u64`'s
/// alignment. `state.rs` pins that width with a const assertion saying the waste is "pinned so the
/// waste is visible, not because it can be reclaimed here".
///
/// It CAN be reclaimed, and this is the one place in the study where it can. Take the bucket out
/// and what is left is a bare `u64`: the struct is 8 bytes, not 16. That is the only footprint
/// argument anywhere in this module, and unlike the other two carriers it costs no wire change at
/// all, because `object_block_lookup` is a derived serving accelerator that is NOT PERSISTED --
/// `persistence.rs` rebuilds it on every load before reconcile.
///
/// AND IT DOES NOT NEED A RANGE-FREE ID, WHICH IS THE POINT. The reason this field is here is that
/// the lookup's return value IS the bucket set, so #1977 held that the function could not be
/// threaded what it exists to compute. #1980 retracted that: `ShardState::routing_range()` is on
/// the state the function already takes, and a recomputed bucket matched 408 of 408 objects. So the
/// 8 bytes sit behind recomputation WITHIN a known range -- already measured correct -- and not
/// behind making the id range-free. The dangerous change and the only real saving are not the same
/// change.
///
/// THE MIRROR IS VALIDATED BEFORE IT IS MUTATED. A hand-written field list with `size_of` of the
/// wrong type still compiles, so the mirror is first required to reproduce the real width exactly;
/// only then is the counterfactual one measured.
#[test]
fn dropping_the_bucket_from_a_lookup_ref_is_the_one_place_it_pays() {
    use std::mem::size_of;

    /// The same fields as `BlockLookupRef`, in the same order.
    #[allow(dead_code)]
    struct MirrorLookupRef {
        routing_bucket: u32,
        block_ref_key: u64,
    }

    /// The same, with the duplicated bucket id gone.
    #[allow(dead_code)]
    struct MirrorLookupRefWithoutBucket {
        block_ref_key: u64,
    }

    let real = size_of::<BlockLookupRef>();
    let mirror = size_of::<MirrorLookupRef>();
    let without = size_of::<MirrorLookupRefWithoutBucket>();

    println!("\n=== the lookup ref, the one carrier where the bucket id is not free ===");
    println!("  BlockLookupRef            {real} B");
    println!("  mirror of it              {mirror} B  (must equal the real width)");
    println!("  mirror without the bucket {without} B");

    assert_eq!(
        16, real,
        "BlockLookupRef is {real} B, not the 16 `state.rs` pins -- the arithmetic below assumes 16"
    );
    assert_eq!(
        real, mirror,
        "the mirror is {mirror} B against a real {real} B, so it is not a faithful copy and the \
         counterfactual below would be fiction"
    );
    assert_eq!(
        8, without,
        "dropping the bucket id should leave a bare u64 of 8 B; the mirror says {without}"
    );
    assert_eq!(
        8,
        real - without,
        "the saving is {} B, not the whole 8 the alignment implies",
        real - without
    );
    println!(
        "  saving: {} B a lookup ref, {:.1}% of the structure -- and it is in-memory only, \
         because object_block_lookup is rebuilt on load and never written",
        real - without,
        100.0 * (real - without) as f64 / real as f64
    );
}
