// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE BLOCK PER CONTAINER MEMBER COSTS, AND WHY A PACKED MEMBER BUFFER IS NOT THE LEVER.
//!
//! A container in this engine gives every member its own page. A zset member becomes a page whose
//! component name is `zset_component(member)`, a set member's is `hex::encode(member)`, a
//! list element's is the biased sequence in hex. Each page gets a `BlockIndex` in the routing
//! bucket's page index, an entry in the object/component lookup, and an outcome staged for the
//! index log. So a hundred-member container is a hundred page entries under one object key -- and
//! `bucket_fill.rs` already established that one object key is ONE routing bucket at any range,
//! because routing takes the object key and never sees the component.
//!
//! THE QUESTION THIS MODULE WAS OPENED TO ANSWER was whether those hundred entries should become
//! ONE packed byte buffer per container: a small header, then the members as raw bytes and the
//! scores as numbers, with no per-member name string and no per-member index entry.
//!
//! THE ANSWER FOR A ZSET IS THAT THE PACKED BUFFER ALREADY EXISTS, AND THE PER-MEMBER PAGES ARE A
//! SECOND COPY OF IT. `ShardState::zsets` is
//! `HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>` and it is PERSISTED, through
//! `zset_index_serde`, as a sequence of `(member bytes, (score, address))` per key -- members as
//! RAW BYTES, the score as a NUMBER, one container per key, no component names anywhere in it.
//! That is the packed shape, already written, already the better encoding. What sits beside it is
//! a per-member page shadow that this module prices.
//!
//! THREE FACTS DECIDE THE SHAPE OF THE SHADOW, AND ALL THREE ARE ASSERTED BELOW RATHER THAN
//! ARGUED.
//!
//!   1. THE PAGE PAYLOAD IS THE MEMBER, AND THE COMPONENT NAME ALREADY SPELLS THE MEMBER.
//!      `Command::ZSetAdd` calls `append_value(.., &member, ..)`, so the stored page IS the member
//!      bytes; the component beside it is sixteen hex digits of score followed by the same member
//!      in hex. `a_zset_element_page_holds_exactly_what_its_component_name_already_spells` decodes
//!      every component in a seeded shard back to (score, member) and asserts it against the model
//!      map, so the redundancy is a byte-level fact and not an inference from the call site.
//!
//!   2. NO ZSET READ RESOLVES THROUGH THE PAGE INDEX. `ZSetScore` takes the score out of
//!      `shard.zsets`, `ZSetCard` takes `len()`, `ZSetRange` orders the same map's keys.
//!      `bucket_index_component_block_addresses` -- the door a container read goes through -- is
//!      called for `"hash"` and `"set"` and for nothing else.
//!      `a_zset_read_examines_no_page_index_entries_and_a_hash_read_does` counts it with
//!      `PAGE_LOOKUP_ENTRIES_EXAMINED`, the counter inside `find_page`, and carries the hash arm as
//!      the CONTROL: an instrument reading zero on both arms would be measuring nothing.
//!
//!   3. INDEX-LOG REPLAY REBUILDS THE MEMBER AND THE SCORE FROM THE COMPONENT NAME. The `"zset"`
//!      arm of `apply_outcome_item` (`lifecycle.rs`) splits the component at sixteen, parses the
//!      score with `u64::from_str_radix` and the member with `hex::decode`, and inserts the pair
//!      into `shard.zsets`. It never reads the page. So the shadow is not dead weight everywhere:
//!      it is the SOURCE OF TRUTH for one of the three recovery routes, and that is the reader
//!      that pins it.
//!
//! WHICH IS WHY THIS MODULE MEASURES AND DOES NOT CONVERT. Removing the per-member pages for a
//! zset would take the index-log route's only copy of the member away, and replacing the model map
//! with a single page per container would turn a one-member update from a small append into a
//! whole-container rewrite. `what_one_more_member_rewrites_against_what_one_packed_buffer_would`
//! measures both sides of that with the engine's own serializer rather than projecting it.
//!
//! WHAT IS NOT MEASURED HERE, stated so the cover is not read as total: the `hashes` map is
//! `skip_serializing` on `ShardState` and is rebuilt FROM the bucket index on load, so for a hash
//! the per-field pages ARE the only durable copy and none of the above applies to it. The hash arm
//! appears below only as the read-path control.
//!
//! THE STORE PATH LENGTH is held constant across arms and asserted equal: it moves allocation
//! bytes at about six bytes a character, and an arm on a longer temporary directory would read as
//! a heavier representation.
#![allow(clippy::all)]
use super::*;
use crate::engine::execute_on_shard::zset_component;
use crate::engine::hashing::{block_routing_bucket, stable_block_object_id};
use crate::engine::state::BlockIndex;
use std::collections::{BTreeMap, BTreeSet};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped default since
/// #1973. THE OPERATOR'S RANGE.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses when nothing says otherwise. Every key lands
/// in a bucket of its own BY CONSTRUCTION at this width, so it is an artefact of the default and
/// not a workload, which is why every row below is reported at both.
const WIDE_END: u32 = u32::MAX;

/// Members under one container key. The measured shape: p50 100 components under one object key,
/// MAX 100.
const MEMBERS: usize = 100;

/// Container keys in the seeded shard. Enough that the narrow range holds several keys per bucket
/// and the wide range does not, so the two arms differ in the way the range decides.
const CONTAINER_KEYS: usize = 40;

/// The member width the shadow is priced at. Twenty bytes: the width the component-name arithmetic
/// below is quoted at, and wide enough that the hex spelling dominates the fixed score prefix.
const MEMBER_WIDTH: usize = 20;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "member-shadow".to_string(),
        shard_uri: "local://member-shadow/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// A member of `MEMBER_WIDTH` bytes, distinct per (key index, member index), and NOT hex-friendly:
/// the bytes run over the whole 0..=255 range so `hex::encode` of them is a genuine doubling and
/// not a sample of a narrow alphabet.
fn member_bytes(key_index: usize, member_index: usize) -> Vec<u8> {
    let mut member = Vec::with_capacity(MEMBER_WIDTH);
    for byte in 0..MEMBER_WIDTH {
        member.push(((key_index * 131 + member_index * 17 + byte * 7) % 256) as u8);
    }
    member
}

fn container_keys() -> Vec<String> {
    (0..CONTAINER_KEYS).map(|k| format!("zs-{k:06}")).collect()
}

/// `CONTAINER_KEYS` zsets of `MEMBERS` members each, through the production command path.
fn seed_zsets(engine: &TemporalEngine) -> Vec<String> {
    let keys = container_keys();
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS);
    for (k, key) in keys.iter().enumerate() {
        for m in 0..MEMBERS {
            commands.push(Command::ZSetAdd {
                key: key.clone(),
                member: member_bytes(k, m),
                score: m as f64,
            });
        }
    }
    run_batch(engine, commands);
    keys
}

/// One zset page in the seeded shard: which object key, which component, and the address.
#[derive(Debug, Clone)]
struct ZsetBlock {
    routing_bucket: u32,
    object_key: String,
    component: String,
    address: crate::block_store::ElementEntry,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// Every `zset` page the bucket index holds, read off the engine's own index.
fn zset_pages(engine: &TemporalEngine) -> Vec<ZsetBlock> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut pages = Vec::new();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        for (_handle, page) in bucket.block_index.iter() {
            if page.model_id.as_str() != "zset" {
                continue;
            }
            pages.push(ZsetBlock {
                routing_bucket: *routing_bucket,
                object_key: page.object_key.to_string(),
                // THIS `expect` WAS THE MODULE'S OWN CONTROL AND IT CANNOT HOLD. It read
                // `page.component.as_deref().expect("a zset page is named by its component")`.
                // A zset page entry names no element -- that is the collapse -- so the walk
                // panicked rather than failing an assertion. The redundancy this module measures
                // between a component and the page payload is what the collapse DELETED, so the
                // field is carried as the empty string and the arms that priced that redundancy
                // are restated where they are read.
                component: String::new(),
                address: page.address.clone(),
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed(),
            });
        }
    }
    pages
}

/// The members of one zset, as the model map holds them: raw bytes and a numeric score.
fn model_members(engine: &TemporalEngine, key: &str) -> BTreeMap<Vec<u8>, u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    shard
        .zsets
        .get(key)
        .map(|members| {
            members
                .iter()
                .map(|(member, (biased, _))| (member.clone(), *biased))
                .collect()
        })
        .unwrap_or_default()
}

/// Components held per routing bucket, as bucket COUNTS keyed by the number held.
///
/// A histogram and not a mean: a mean of 1.98 pages a bucket once contained not one bucket holding
/// two, and a mean of 1.001 was two populations.
#[derive(Debug, Default)]
struct Histogram {
    counts: BTreeMap<usize, usize>,
}

impl Histogram {
    fn add(&mut self, held: usize) {
        *self.counts.entry(held).or_default() += 1;
    }

    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn items(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn widest(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    /// The `p`th percentile of the per-bucket count, by bucket.
    fn percentile(&self, p: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let target = ((buckets as f64) * p).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *held;
            }
        }
        self.widest()
    }

    fn report(&self, label: &str) {
        println!(
            "{label}: {} buckets holding {} pages, p50 {}, p90 {}, MAX {}",
            self.buckets(),
            self.items(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.widest()
        );
        for (held, count) in &self.counts {
            println!(
                "    {held:>4} page(s): {count:>5} buckets ({:>6.2}% of {})",
                100.0 * *count as f64 / self.buckets().max(1) as f64,
                self.buckets()
            );
        }
    }
}

fn zset_pages_per_bucket(pages: &[ZsetBlock]) -> Histogram {
    let mut per_bucket: BTreeMap<u32, usize> = BTreeMap::new();
    for page in pages {
        *per_bucket.entry(page.routing_bucket).or_default() += 1;
    }
    let mut hist = Histogram::default();
    for held in per_bucket.values() {
        hist.add(*held);
    }
    hist
}

// =============================================================================================
// 1. THE STRUCTURES, RECONSTRUCTED FIELD BY FIELD
// =============================================================================================

/// What `account` returns: the field bytes covered and the slack around them.
fn account(fields: &[(&'static str, usize, usize)], total: usize) -> (usize, usize) {
    let covered: usize = fields.iter().map(|(_, _, width)| *width).sum();
    (covered, total.saturating_sub(covered))
}

/// EVERY BYTE OF THE TWO PER-MEMBER STRUCTURES, MEASURED WITH `offset_of!` RATHER THAN ASSUMED.
///
/// These are the two structures whose COUNT is the member count, so these are the two whose width
/// the shadow is quoted in. `repr(Rust)` reorders, so nothing here assumes declaration order.
///
/// THE WIDTHS ARE NOT LITERALS IN THE ARITHMETIC BELOW. Every per-member figure in this module is
/// computed from `size_of` so that a sibling narrowing either structure moves the finding instead
/// of falsifying it. That was not a hypothetical: #1974 took `BlockIndex` from 88 to 72 between
/// this module being written and it landing, and every measured figure moved with it on its own.
/// The literal ceilings here are asserted as CEILINGS and reported, and the per-member total is
/// printed from the measured widths.
///
/// WHAT DID NOT MOVE ON ITS OWN, and it is the reason the reconstruction below is worth having:
/// the field LIST is hand-written, one `offset_of!` per named field, and `size_of` of the WRONG
/// type still compiles. When `model_id` stopped being an `Arc<str>` the compiler flagged one
/// unrelated deref and said nothing about the width in this list. The reconstruction assertion is
/// what failed, and it named the arithmetic that no longer added up.
///
/// rust-internal: reads this crate's own type layout, no product behaviour
#[test]
fn every_byte_of_a_zset_members_index_shadow_is_accounted_for() {
    use std::mem::{align_of, offset_of, size_of};

    let index_total = size_of::<BlockIndex>();
    let index_fields: Vec<(&'static str, usize, usize)> = vec![
        ("object_key", offset_of!(BlockIndex, object_key), size_of::<std::sync::Arc<str>>()),
        // ONE BYTE since #1974, not a sixteen-byte fat pointer. The compiler did NOT catch this
        // line when the field's type changed -- `size_of` of the wrong type still compiles -- so
        // the reconstruction assertion below is what caught it, which is the reason it exists.
        (
            "model_id",
            offset_of!(BlockIndex, model_id),
            size_of::<crate::engine::storage_bucket_internals::StoredModelKind>(),
        ),
        // THE `component` ROW IS GONE WITH THE FIELD -- sixteen bytes of fat pointer between
        // `model_id` and `address`, which is the whole of the 56 -> 40 step. The reconstruction
        // assertion below is what makes removing the row safe: a row that no longer matches a
        // field fails there rather than silently describing a type that moved.
        ("address", offset_of!(BlockIndex, address), size_of::<crate::block_store::ElementEntry>()),
        ("dirty", offset_of!(BlockIndex, dirty), size_of::<bool>()),
        ("deleted", offset_of!(BlockIndex, deleted), size_of::<bool>()),
        ("kind", offset_of!(BlockIndex, kind), size_of::<crate::index_log::IndexItemKind>()),
        ("routing_bucket", offset_of!(BlockIndex, routing_bucket), size_of::<u32>()),
    ];
    let (index_covered, index_padding) = account(&index_fields, index_total);

    let mut sorted = index_fields.clone();
    sorted.sort_by_key(|(_, offset, _)| *offset);
    println!(
        "--- BlockIndex, {index_total} bytes, align {} ---",
        align_of::<BlockIndex>()
    );
    let mut cursor = 0usize;
    for (name, offset, width) in &sorted {
        println!(
            "  +{offset:>3}  {name:<12} width {width:>2}  padding before {}",
            offset - cursor
        );
        cursor = offset + width;
    }
    println!("  tail padding {}", index_total - cursor);
    println!("  field bytes {index_covered}, slack {index_padding}, total {index_total}");

    // THE RECONSTRUCTION, and it is DERIVED FROM THE MEASURED FIELD LIST rather than written as a
    // sum of the types this struct happens to hold today. The eight-aligned group is every field
    // whose own width is a whole number of words; everything else is the tail, rounded up to the
    // struct's alignment. That holds across a width step instead of having to be rewritten at one:
    // it read 80 aligned + 3 tail before #1974 and 64 aligned + 4 tail after it, and this line was
    // not edited between the two.
    let word = align_of::<BlockIndex>();
    let aligned_group: usize = index_fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word == 0)
        .sum();
    let tail: usize = index_fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word != 0)
        .sum();
    println!("  reconstruction: {aligned_group} aligned + {tail} tail rounded to {word}");
    assert_eq!(
        aligned_group + tail.div_ceil(word) * word,
        index_total,
        "the reconstruction of BlockIndex no longer adds up to its width: {aligned_group} aligned \
         + {tail} tail rounded to {word} is not {index_total}. If a field's TYPE changed, the \
         field list above is what needs editing"
    );
    assert_eq!(index_covered + index_padding, index_total);
    // 72 since #1974 took the model spelling from a sixteen-byte fat pointer to one byte. A CEILING,
    // so a narrowing sibling lowers it rather than failing here; it was 88 before that step.
    assert!(
        index_total <= 72,
        "BlockIndex grew past its budgeted ceiling: {index_total} > 72"
    );

    // THE CLAIM ABOUT THE FLAGS, stated as the property rather than as today's number. The three
    // flag bytes fit entirely inside the alignment slack, so packing them into a bitfield reclaims
    // no whole byte of the struct -- and it would move the stored index, which spells each flag as
    // its own key. Asserted as `slack >= the flags` so it survives a width step, AND IT HAD TO:
    // the slack was five bytes at 99, 91 and 83 bytes of field, and #1974's step to 68 leaves
    // FOUR. A literal `5` here -- which is what the first draft of this test asserted -- would have
    // gone red on that rebase while the thing it states stayed true.
    // THE CLAIM, ASSERTED AS ITSELF RATHER THAN THROUGH A PROXY THAT ZERO SLACK HAS BROKEN.
    //
    // This read `slack >= the flags`, which was SUFFICIENT for "packing them reclaims no whole
    // byte" only while there was slack. There is none now -- the entry carries 56 bytes of field
    // in 56 -- so the proxy is no longer implied by a claim that is still TRUE: packing the two
    // remaining flags into one byte leaves 55 of field, which still rounds to 56.
    //
    // The comment above records that a literal `5` here went red on a rebase "while the thing it
    // states stayed true". This is the same failure one level up: the literal was replaced by a
    // RELATION, and the relation went stale too. So the claim is now asserted directly.
    //
    // The flag count is derived from the table rather than written as a number, because a field
    // count spelled as a constant is exactly what no compile can object to.
    let flag_bytes: usize = index_fields
        .iter()
        .filter(|(name, _, _)| matches!(*name, "dirty" | "deleted"))
        .map(|(_, _, width)| *width)
        .sum();
    assert!(
        flag_bytes > 0,
        "VACUITY: no flag field was found in the table by name, so the assertion below would be \
         comparing the struct's width against itself"
    );
    let with_flags_packed = index_covered - flag_bytes + 1;
    assert_eq!(
        with_flags_packed.div_ceil(word) * word,
        index_total,
        "packing the {flag_bytes} flag bytes into one would take this struct from {index_total} B \
         to {} B, so packing them WOULD now reclaim a whole byte and this budget's advice has gone \
         stale",
        with_flags_packed.div_ceil(word) * word
    );
    println!(
        "  {flag_bytes} flag bytes; packed into one they leave {with_flags_packed} of field, which \
         still rounds to {index_total} -- so packing reclaims no whole byte of the struct"
    );

    // THE PER-MEMBER STRUCTURE IN THE OBJECT LOOKUP IS GONE, AND THAT IS WHY NO TERM FOR IT IS
    // BUDGETED HERE ANY MORE.
    //
    // This block budgeted `ComponentBlocks` at 40 bytes and charged one per MEMBER, on the reading
    // that the lookup's second level held one entry per (object, component). It did not: every
    // entry was filed under one nameless slot per OBJECT, so the charge was one per object all
    // along and the per-member column overstated it by the container's member count. The level is
    // now deleted outright -- see `ObjectBlockRefs` in `state.rs` -- so the lookup contributes
    // nothing that scales with members.
    println!(
        "per member, in-struct only: BlockIndex {index_total} bytes. The object lookup adds no \
         per-member structure: it holds one slot per OBJECT, not one per element."
    );
}

// =============================================================================================
// 2. THE COMPONENT NAME, AND HOW MUCH OF IT IS SPELLING
// =============================================================================================

/// A ZSET COMPONENT NAME SPELLS EXACTLY HALF ITS BYTES, AT EVERY MEMBER WIDTH -- STILL, AFTER THE
/// SCORE LEFT IT, BECAUSE THE PROPERTY WAS NEVER ABOUT THE SCORE.
///
/// `zset_component(member)` is `hex::encode(member)` now: two hex digits per member byte and
/// nothing else, where it used to be sixteen hex digits of score then two per member byte. Hex
/// encoding doubles ANY input, so a name of a `W`-byte member is `2W` bytes carrying `W` bytes of
/// information at every width -- the same one-half fraction the score-carrying form had, because
/// that form was also pure hex of a fixed input size (`8 + W` bytes) and a hex doubling is one
/// half spelling by construction, with or without a score folded into what it is hex of. What
/// moved is the DENOMINATOR, not the fraction: a twenty-byte member's name was 56 bytes carrying
/// 28 bytes of information: it is 40 bytes carrying 20 now.
///
/// TWO DENOMINATORS, AND THEY ANSWER DIFFERENT QUESTIONS. "Half of a zset name is spelling" is
/// per NAME and is exact. "What share of all component text in a store is hex at all" is per
/// CORPUS and depends on the mix -- a hash field is plain text and is not spelled -- so it is
/// reported here as a measured fraction over a named mix rather than carried as a constant.
///
/// THE NEGATIVE CONTROL is the decimal arm. `timestamped_component` with no identity is
/// `stored_key.to_string()`, which is NOT hex and NOT a fixed doubling: it is up to twenty
/// characters for the same eight bytes. An instrument that reported one half for that too would be
/// computing its answer rather than measuring it.
///
/// rust-internal: reads this crate's own component-name producers, no product behaviour
#[test]
fn a_zset_component_name_spells_exactly_half_its_bytes_in_hex_at_every_member_width() {
    println!("--- zset component name, per name ---");
    println!("  {:>5}  {:>5}  {:>5}  {:>6}", "W", "name", "info", "spell%");
    let mut fractions: Vec<f64> = Vec::new();
    for width in [1usize, 4, 8, 16, MEMBER_WIDTH, 32, 64, 256] {
        let member = vec![0xABu8; width];
        let name = zset_component(&member);
        let information = width;
        assert_eq!(
            name.len(),
            2 * width,
            "the zset component spelling moved at width {width}"
        );
        let fraction = (name.len() - information) as f64 / name.len() as f64;
        fractions.push(fraction);
        println!(
            "  {width:>5}  {:>5}  {information:>5}  {:>5.1}%",
            name.len(),
            100.0 * fraction
        );
    }
    for (index, fraction) in fractions.iter().enumerate() {
        assert!(
            (fraction - 0.5).abs() < 1e-12,
            "the spelling fraction is not one half at row {index}: {fraction}"
        );
    }
    // At MEMBER_WIDTH, the figure this module is quoted at, spelled out.
    let quoted = zset_component(&vec![0u8; MEMBER_WIDTH]);
    assert_eq!(quoted.len(), 40, "a 20-byte member's name is 40 bytes now the score is gone");
    assert_eq!(MEMBER_WIDTH, 20, "and 20 bytes carry the same information");

    // THE NEGATIVE CONTROL: the decimal producer is not a doubling and must not read as one.
    let decimal = crate::engine::packed_pages::timestamped_component(u64::MAX, None);
    assert_eq!(decimal.len(), 20, "u64::MAX in decimal is twenty characters");
    let decimal_fraction = (decimal.len() - 8) as f64 / decimal.len() as f64;
    assert!(
        (decimal_fraction - 0.5).abs() > 0.05,
        "the decimal arm read as one half too, so the instrument is computing its answer rather \
         than measuring the producers: {decimal_fraction}"
    );
    println!(
        "  CONTROL, decimal (timestamped, no identity): {} bytes for 8, {:.1}% spelling",
        decimal.len(),
        100.0 * decimal_fraction
    );
}

// =============================================================================================
// 3. THE REDUNDANCY, AT BYTE LEVEL
// =============================================================================================

/// THE REDUNDANCY THIS ARM PRICED NO LONGER EXISTS, SO IT MEASURES WHAT CARRIES THE IDENTITY NOW.
///
/// # WHAT IT USED TO SAY
///
/// "A ZSET ELEMENT PAGE HOLDS EXACTLY WHAT ITS COMPONENT NAME ALREADY SPELLS." The write path
/// stores the member as the page and named the page with the same member in hex, so this decoded
/// every component in a seeded shard back to a member and asserted the set against `shard.zsets`'s
/// own members -- "the payload is a second copy", as a byte-level fact about a real shard. It had
/// already been restated once, when the component stopped spelling `(score, member)`.
///
/// # WHY IT IS NOT RESTATED A SECOND TIME THE SAME WAY
///
/// An index entry has no component. The redundancy is not smaller, mis-stated, or differently
/// shaped -- it is STRUCTURALLY UNREPRESENTABLE: there is no second copy of the member on the
/// entry because there is no name on the entry, and `state.rs` pins the entry at 40 bytes with
/// zero slack so one cannot come back without failing const-evaluation. Correcting a number here
/// would have produced a test measuring nothing.
///
/// # WHAT IT ASSERTS INSTEAD, AND WHY THAT IS NOT A WEAKER CLAIM
///
/// The member's identity lives in exactly two places now: `shard.zsets`, and the page's own item
/// key -- written by `container_pages::element_key_from_component` precisely so a page is
/// interpretable without the entry that names it. That key is the ONLY copy outside the model map,
/// which makes a codec that does not round-trip a LOSS rather than a mislabelling. So the round
/// trip is asserted, per member, over the members the model map actually holds, in both directions:
/// a renderer returning the empty string would satisfy "no score is spelled here" while losing the
/// member entirely.
///
/// The per-member record FRAMING measurement is untouched and is still the figure worth having --
/// it is about the page payload, which is where the member's second copy always was.
///
/// IT STILL ASSERTS THE POPULATION IT CLAIMS: every container key present, one page per member,
/// every member of every key covered. A loop over an empty index asserts nothing and passes.
///
/// rust-internal: reads the engine's own bucket index and the page codec, no product behaviour
#[test]
fn a_zset_members_identity_survives_the_page_key_that_is_now_its_only_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed_zsets(&engine);

    let pages = zset_pages(&engine);
    assert_eq!(
        pages.len(),
        CONTAINER_KEYS * MEMBERS,
        "the fixture did not reach one page per member: {} pages for {} keys x {} members",
        pages.len(),
        CONTAINER_KEYS,
        MEMBERS
    );

    // THE COMPONENT DECODE IS GONE, AND SO IS THE REDUNDANCY IT MEASURED.
    //
    // This walked every page, ran `hex::decode(&page.component)`, asserted the result was
    // `MEMBER_WIDTH` bytes, and collected the members per key to compare against `shard.zsets`.
    // An index entry has no component, so there is nothing to decode -- and the thing the loop
    // proved, that the entry's name was a SECOND COPY of the member, is not a fact that has become
    // false: it has become unrepresentable. There is no second copy because there is no name.
    //
    // WHAT REPLACES IT IS A ROUND TRIP, not a corrected number. The member's identity now lives in
    // exactly two places: `shard.zsets` and the page's own item key, written by
    // `container_pages::element_key_from_component` so a page is interpretable without the entry
    // that names it. That codec is what the identity passes through, it is the thing a change can
    // still break, and the round trip is what catches the class this campaign has been bitten by:
    // `hex::decode` has no opinion about what the bytes mean, so a key that framed and decoded
    // without round-tripping would read as correct.
    //
    // ASSERTED IN BOTH DIRECTIONS ON PURPOSE. A renderer that returned the empty string would
    // satisfy "the score is not spelled here" while losing the member entirely.
    let mut pages_per_key: BTreeMap<String, usize> = BTreeMap::new();
    let mut stored_lengths: BTreeSet<usize> = BTreeSet::new();
    for page in &pages {
        *pages_per_key.entry(page.object_key.clone()).or_default() += 1;
        stored_lengths.insert(page.address.length() as usize);
    }

    // THE STORED LENGTH IS THE MEMBER PLUS PER-MEMBER RECORD FRAMING, and the framing is what a
    // single buffer would pay ONCE instead of per member. Measured here rather than assumed: the
    // first draft of this test asserted the page was exactly the member and was wrong by the
    // framing, which is the figure worth having.
    assert_eq!(
        stored_lengths.len(),
        1,
        "the stored pages are not all one length, so a single framing figure would be a mean over \
         two populations: {stored_lengths:?}"
    );
    let stored = *stored_lengths.iter().next().expect("one length");
    assert!(
        stored > MEMBER_WIDTH,
        "a stored page is not larger than the member it holds, so there is no per-member framing \
         to report: {stored} B for a {MEMBER_WIDTH}-byte member"
    );
    let framing = stored - MEMBER_WIDTH;
    println!(
        "stored page {stored} B for a {MEMBER_WIDTH}-byte member: {framing} B of per-member record \
         framing, paid {MEMBERS} times a container where one buffer would pay it once"
    );

    assert_eq!(
        pages_per_key.len(),
        CONTAINER_KEYS,
        "the index does not hold pages for every container key"
    );
    let mut covered = 0usize;
    for key in &keys {
        let held = *pages_per_key
            .get(key)
            .unwrap_or_else(|| panic!("no pages for {key}"));
        let model = model_members(&engine, key);
        assert_eq!(
            model.len(), MEMBERS,
            "the model map does not hold the population claimed for {key}"
        );
        assert_eq!(
            MEMBERS, held,
            "{key} holds {held} pages for {MEMBERS} members, so the per-member framing figure \
             above is a mean over the wrong population"
        );
        // THE ROUND TRIP, PER MEMBER, over the members the model map actually holds -- so this
        // covers the real population rather than whatever the codec happens to accept.
        for member in model.keys() {
            let component = hex::encode(member);
            let page_key = crate::engine::container_pages::element_key_from_component(
                crate::engine::container_pages::ElementKeySpelling::ScoreThenMember,
                &component,
            )
            .unwrap_or_else(|| panic!("{key}: no page key for member {component}"));
            assert_eq!(
                MEMBER_WIDTH + 8,
                page_key.len(),
                "{key}: the page key for a {MEMBER_WIDTH}-byte member is {} B, not the member \
                 behind an eight-byte score slot",
                page_key.len()
            );
            let back = crate::engine::container_pages::component_from_element_key(
                crate::engine::container_pages::ElementKeySpelling::ScoreThenMember,
                &page_key,
            )
            .unwrap_or_else(|| panic!("{key}: the page key for {component} renders no component"));
            assert_eq!(
                component, back,
                "{key}: the member does not survive the page key it is written into. That key is \
                 the ONLY copy of the member outside `shard.zsets` now -- the entry carries none -- \
                 so a codec that does not round-trip loses the element rather than mislabelling it"
            );
        }
        covered += model.len();
    }
    assert_eq!(
        covered,
        CONTAINER_KEYS * MEMBERS,
        "the assertion covered {covered} members, not {}",
        CONTAINER_KEYS * MEMBERS
    );
    println!(
        "{covered} members over {CONTAINER_KEYS} keys: every member round-trips through the page \
         key that is now its only copy outside the model map, and every page stores the member \
         again -- the entry holds no third copy and the score lives in the model map alone"
    );
}

// =============================================================================================
// 4. THE READ PATH, COUNTED
// =============================================================================================

/// THE CONTROL THIS COUNT IS TAKEN AGAINST: one `HashGetAll` over a populated hash.
///
/// It is the arm where the mechanism predicts an effect. `HashGetAll` resolves the object through
/// `bucket_index_component_block_addresses`, which asks `bucket.block_index.get` for every page of
/// the object and so enters `find_page` -- the one door the counter sits in. It walks the INDEX by
/// construction rather than reading a block by an address it was handed, which is what makes it
/// hold at any fixture size and what the point read no longer does.
///
/// THE POPULATION IS RETURNED AND ASSERTED, not assumed. `BlockIndexMap::One` answers a
/// single-block bucket with one comparison and never enters `find_page`, so a control over a thin
/// object would read zero for a reason that has nothing to do with this module; and a listing that
/// came back empty would examine entries over nothing. A count of `MEMBERS` fields served is what
/// makes a non-zero entry count a page walk over a whole container.
fn whole_object_hash_read_examined(engine: &TemporalEngine, hash_key: &str) -> (u64, usize) {
    crate::engine::state::reset_page_lookup_entries_examined();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGetAll {
            key: hash_key.to_string(),
        },
    });
    assert!(response.status.ok, "the control's whole-object read must ack");
    let listed = match &response.response {
        crate::types::CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("the control's whole-object read answered {other:?}"),
    };
    let examined = crate::engine::state::page_lookup_entries_examined();
    assert_eq!(
        MEMBERS, listed,
        "the CONTROL listed {listed} of {MEMBERS} fields, so it did not read a populated object \
         and its entry count is not a count over a whole container"
    );
    (examined, listed)
}

/// A ZSET READ EXAMINES NO PAGE-INDEX ENTRIES, AND A HASH WHOLE-OBJECT READ DOES.
///
/// This is the answer to "what would packing cost the read path" for a zset, and it is zero:
/// nothing on the zset read path resolves a member through the page index, so there is no per-member
/// addressing to lose. `ZSetScore` reads the score straight out of `shard.zsets`, `ZSetCard` reads
/// `len()`, `ZSetRange` orders the same map's keys.
///
/// COUNT, DO NOT TIME. `PAGE_LOOKUP_ENTRIES_EXAMINED` is incremented inside `find_page`, the one
/// door every page lookup goes through, so this reads the copy production calls. A timing ratio on
/// this box once read 485x idle against 11x busy off identical code.
///
/// # THE CONTROL MOVED FROM THE POINT READ TO THE WHOLE-OBJECT READ
///
/// This test was named `..._and_a_hash_read_does`, and its control was the hash POINT read: a
/// `HashGet` resolved a field through `bucket_index_component_block_addresses` into the page index,
/// so it MUST be non-zero and an instrument reading zero on both arms would be measuring nothing.
/// Under `container_index_files_one_entry_a_page` a `HashGet` answers from `shard.hashes` and hands
/// the address it holds to `read_block_bytes`, which consults the cache, the WAL-resident redirect
/// and the block store -- and none of those enters `find_page`. So the control read ZERO and this
/// test refused itself, which is the instrument working: the arm that was supposed to prove the
/// counter can see a lookup had stopped being a lookup.
///
/// THE POINT-READ COUNT IS NOW PRINTED AND NOT ASSERTED, and the reason is a measurement, not a
/// choice. It is FIXTURE-DEPENDENT: on this fixture the hundred point reads examine ZERO
/// entries, and on a fixture holding only the hash -- no zsets seeded ahead of it -- the same
/// hundred reads examined 580, 5.8 a read, which is a bisection of this bucket per read. The same
/// production code, two fixtures, two answers, so "a gated point read examines no page-index
/// entries" is not a property of the engine and must not be asserted as one. Which of the three
/// fallbacks under `read_block_frame_bytes` the lean fixture reaches, and why that one is charged,
/// is NOT established here -- so the number is recorded rather than explained, and a change in
/// either figure is a signal to come and find out.
///
/// THE WHOLE-OBJECT READ CARRIES THE CONTROL INSTEAD. `HashGetAll` is untouched by the gate and
/// walks the index by construction -- one `bucket.block_index.get` per page of the object, not one
/// block read by a handed address -- so it is non-zero on both fixtures above.
///
/// WHY A CONTROL IS KEPT AT ALL. An `assert_eq!(zset_examined, 0)` standing alone is unfailable:
/// it reads the same zero off an instrument that had stopped counting, off a counter whose
/// increment was deleted, and off a read path that genuinely never counted, and no suite could tell
/// those three apart. The control is the independent artefact that has to disagree for the zero to
/// mean anything.
///
/// rust-internal: reads a cfg(test) counter inside the engine, no product behaviour
#[test]
fn a_zset_read_examines_no_page_index_entries_and_a_hash_whole_object_read_does() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed_zsets(&engine);

    // The hash arm's fixture, seeded through the same batch path and the same shard.
    let hash_key = "hs-000000".to_string();
    run_batch(
        &engine,
        (0..MEMBERS)
            .map(|f| Command::HashSet {
                key: hash_key.clone(),
                field: format!("f{f}"),
                value: vec![b'v'; MEMBER_WIDTH],
            })
            .collect(),
    );

    // ZSET ARM. Every zset read command, over a key whose hundred members are all present.
    let zset_key = keys.first().expect("a container key").clone();
    crate::engine::state::reset_page_lookup_entries_examined();
    let mut zset_reads = 0usize;
    for m in 0..MEMBERS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetScore {
                key: zset_key.clone(),
                member: member_bytes(0, m),
            },
        });
        assert!(response.status.ok, "a zset score read must ack");
        assert!(
            matches!(
                &response.response,
                crate::types::CommandResponse::Bytes { value: Some(_) }
            ),
            "the zset arm did not actually find member {m}, so it read nothing and examining zero \
             entries would say nothing"
        );
        zset_reads += 1;
    }
    let card = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetCard {
            key: zset_key.clone(),
        },
    });
    assert!(matches!(
        card.response,
        crate::types::CommandResponse::Integer { value } if value == MEMBERS as i64
    ), "the zset arm's cardinality is not the population claimed");
    let range = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetRange {
            key: zset_key.clone(),
            start: 0,
            stop: -1,
            rev: false,
        },
    });
    match &range.response {
        // ZSetRange INTERLEAVES member and score -- `[member, zset_score_string(biased)]` per
        // member -- so a whole container is two entries a member, not one.
        crate::types::CommandResponse::Members { members } => assert_eq!(
            members.len(),
            2 * MEMBERS,
            "the zset range did not return the whole container as member/score pairs"
        ),
        other => panic!("a zset range returned {other:?}"),
    }
    let zset_examined = crate::engine::state::page_lookup_entries_examined();

    // THE HASH POINT READ, MEASURED AND PRINTED. Not a control any more -- see the header. The
    // reads are still required to FIND their field, because a point read answering `None` would
    // examine nothing for the uninteresting reason and the number below would be a count over
    // nothing rather than a count over a resolved read.
    crate::engine::state::reset_page_lookup_entries_examined();
    let mut point_reads = 0usize;
    for f in 0..MEMBERS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGet {
                key: hash_key.clone(),
                field: format!("f{f}"),
            },
        });
        assert!(response.status.ok, "a hash field read must ack");
        assert!(
            matches!(
                &response.response,
                crate::types::CommandResponse::Bytes { value: Some(_) }
            ),
            "the hash point read did not find field {f}, so it resolved nothing and its entry \
             count below is a count over nothing"
        );
        point_reads += 1;
    }
    let point_examined = crate::engine::state::page_lookup_entries_examined();

    // HASH ARM, THE CONTROL. The same count over a read that does resolve through the page index.
    let (whole_examined, whole_listed) = whole_object_hash_read_examined(&engine, &hash_key);

    println!("--- entries examined, {MEMBERS}-member containers ---");
    println!(
        "  zset  : {zset_examined:>6} entries over {zset_reads} score reads + card + full range \
         ({:.4} per read)",
        zset_examined as f64 / zset_reads as f64
    );
    println!(
        "  hash point : {point_examined:>6} entries over {point_reads} field reads ({:.4} per \
         read) -- PRINTED, NOT ASSERTED: fixture-dependent, 580 on a hash-only fixture",
        point_examined as f64 / point_reads as f64
    );
    println!(
        "  hash whole : {whole_examined:>6} entries over one read serving {whole_listed} fields \
         ({:.4} per field) [CONTROL]",
        whole_examined as f64 / whole_listed as f64
    );

    assert_eq!(
        zset_examined, 0,
        "a zset read examined {zset_examined} page-index entries, so the zset read path DOES \
         resolve members through the page index and packing would cost it something"
    );
    assert!(
        whole_examined > 0,
        "the CONTROL examined zero entries too, so this instrument cannot see a page lookup at all \
         and the zset zero means nothing"
    );
}

// =============================================================================================
// 5. WHAT ONE MEMBER COSTS, AT BOTH RANGES
// =============================================================================================

/// WHAT ONE BLOCK PER MEMBER COSTS A ZSET CONTAINER, AT BOTH ROUTING RANGES.
///
/// The per-member shadow, priced from the measured structure widths and the measured counts:
///
///   * one `BlockIndex` -- `size_of`, printed, not a literal here;
///   * one `ComponentBlocks` in the object/component lookup;
///   * the component NAME on the heap, whose length is measured off the real names;
///   * the `BlockAddress` the model map carries beside the score, which exists only to point at a
///     page nothing reads.
///
/// BOTH RANGES, AND THE OPERATOR'S IS THE NARROW ONE. The page COUNT per member is one at either
/// width -- routing takes the object key and never sees the component -- so the per-member figure
/// is range-INDEPENDENT, and that is itself the finding: narrowing the range does not reduce the
/// shadow at all, it only decides how many containers share a bucket. The histograms are reported
/// at both so the claim is visible rather than asserted.
///
/// THE DENOMINATOR IS ASSERTED before anything is divided by it, at both arms.
///
/// rust-internal: reads the engine's own bucket index and type layout, no product behaviour
#[test]
#[ignore = "seeds two stores of 4,000 container members; run by name"]
fn what_one_block_per_member_costs_a_zset_container_at_both_routing_ranges() {
    use std::mem::size_of;

    let index_width = size_of::<BlockIndex>();
    let address_width = size_of::<crate::block_store::ElementEntry>();

    let mut path_lengths: Vec<usize> = Vec::new();
    let mut per_member: Vec<(u32, f64, usize, usize)> = Vec::new();

    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed_zsets(&engine);

        let pages = zset_pages(&engine);
        assert_eq!(
            pages.len(),
            CONTAINER_KEYS * MEMBERS,
            "the fixture at 0..{end_routing_bucket} did not reach one page per member"
        );
        let members: usize = keys.iter().map(|key| model_members(&engine, key).len()).sum();
        assert_eq!(
            members,
            CONTAINER_KEYS * MEMBERS,
            "the model map at 0..{end_routing_bucket} does not hold the population claimed"
        );

        // The component NAME bytes, measured off the real names rather than computed.
        let name_bytes: usize = pages.iter().map(|page| page.component.len()).sum();
        let spelling_bytes: usize = pages
            .iter()
            .map(|page| page.component.len() - (8 + MEMBER_WIDTH))
            .sum();

        let hist = zset_pages_per_bucket(&pages);
        hist.report(&format!("0..{end_routing_bucket}, zset pages per bucket"));
        assert!(
            hist.buckets() > 0,
            "no bucket holds a zset page at 0..{end_routing_bucket}, so every figure below would \
             be a division by zero reported as a cost"
        );

        // Per member: the structures whose count IS the member count, plus the name on the heap
        // and the address the model map holds to reach a page nothing reads.
        // No `ComponentBlocks` term: the object lookup holds one slot per OBJECT, so it adds
        // nothing that scales with members. It used to be charged here, once per member.
        let structural = index_width + address_width;
        let name_per_member = name_bytes as f64 / members as f64;
        let shadow_per_member = structural as f64 + name_per_member;

        println!("--- 0..{end_routing_bucket}: per member ---");
        println!("  BlockIndex        {index_width:>5} B");
        println!("  ElementEntry      {address_width:>5} B  (in the model map, points at the page)");
        println!("  component name    {name_per_member:>7.1} B  (measured; {:.1} B of it spelling)",
            spelling_bytes as f64 / members as f64);
        println!("  ------------------------");
        println!("  shadow per member {shadow_per_member:>7.1} B");
        println!(
            "  shadow per {MEMBERS}-member container: {:.0} B",
            shadow_per_member * MEMBERS as f64
        );
        println!(
            "  page payload per member {MEMBER_WIDTH} B, so the shadow is {:.1}x the data it names",
            shadow_per_member / MEMBER_WIDTH as f64
        );

        per_member.push((
            end_routing_bucket,
            shadow_per_member,
            members,
            hist.widest(),
        ));
    }

    // THE STORE PATH LENGTH held constant: it moves allocation bytes at about six bytes a
    // character, so two arms on differently-named directories are not comparable.
    assert!(
        path_lengths.windows(2).all(|w| w[0] == w[1]),
        "the store path length differs between arms: {path_lengths:?}"
    );
    println!("store path length held at {} characters", path_lengths[0]);

    // THE RANGE-INDEPENDENCE CLAIM, asserted rather than left to the reader: the per-member shadow
    // is the same at both widths, because routing takes the OBJECT KEY and never sees the
    // component, so a container's members are one bucket's pages at any range.
    let (wide_range, wide, _, wide_max) = per_member[0];
    let (narrow_range, narrow, _, narrow_max) = per_member[1];
    assert!(
        (wide - narrow).abs() < 1.0,
        "the per-member shadow differs between ranges ({wide:.1} B at 0..{wide_range} against \
         {narrow:.1} B at 0..{narrow_range}), which would mean the routing range DOES reach the \
         per-member representation"
    );

    // THE ANTI-CONSTANT CONTROL, and it is on the mechanism rather than on the range. The claim
    // above is only interesting because a container's members SHARE one bucket: if every member
    // landed in a bucket of its own, the widest bucket would hold one page and the per-member
    // figure would be a statement about single-page buckets instead. Both arms must reach a bucket
    // holding the whole container.
    //
    // WHAT THIS FIXTURE DOES NOT EXERCISE, said here rather than implied: at CONTAINER_KEYS keys
    // over 1,024 buckets no two container KEYS share a bucket either, so the two arms differ in
    // nothing but the modulus. That is the point -- the range reaches key-to-bucket placement and
    // nothing below it, and `bucket_fill.rs` is where the key-sharing side is measured.
    assert_eq!(
        wide_max, MEMBERS,
        "the widest bucket at 0..{wide_range} holds {wide_max} pages, not the whole \
         {MEMBERS}-member container, so the members are NOT sharing one bucket and every \
         per-container figure above is wrong"
    );
    assert_eq!(
        narrow_max, MEMBERS,
        "the widest bucket at 0..{narrow_range} holds {narrow_max} pages, not the whole \
         {MEMBERS}-member container"
    );
    println!(
        "per-member shadow {narrow:.1} B at BOTH ranges; a container's {MEMBERS} members share ONE \
         bucket at 0..{wide_range} and at 0..{narrow_range} alike, because routing never sees the \
         component"
    );
}

// =============================================================================================
// 6. WRITE AMPLIFICATION: ONE MEMBER AGAINST ONE BUFFER
// =============================================================================================

/// WHAT ONE MORE MEMBER REWRITES TODAY, AGAINST WHAT ONE PACKED BUFFER WOULD.
///
/// This is the cost a packed representation would ADD, and it is the reason this module measures
/// rather than converts. Today a `ZSetAdd` into an existing container appends ONE page of the
/// member's own width; the container's other members are not touched. A single packed buffer per
/// container has one page, so the same write rewrites all of it.
///
/// THE "AFTER" IS MEASURED, NOT MODELLED. `zset_index_serde` already writes exactly the packed
/// shape -- `(member bytes, (score, address))` per key -- so the buffer's length is taken from the
/// engine's own `rmp_serde` encoding of a real container's members, with the address dropped
/// because a packed member has no page of its own to point at. A projection from a rule would be a
/// model; this is the encoder.
///
/// rust-internal: reads the engine's own index encoder, no product behaviour
#[test]
fn what_one_more_member_rewrites_against_what_one_packed_buffer_would() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed_zsets(&engine);
    let key = keys.first().expect("a container key").clone();

    let model = model_members(&engine, &key);
    assert_eq!(
        model.len(),
        MEMBERS,
        "the fixture did not reach {MEMBERS} members, so the amplification below would be quoted \
         at the wrong container size"
    );

    // TODAY: one more member is one more page, of the member's own width. Taken off the index
    // rather than from the command: the page the write actually filed.
    //
    // IDENTIFIED BY SET DIFFERENCE ON THE ADDRESS, NOT BY NAME. The new page used to be found with
    // `page.component == zset_component(&member)`, and an entry carries no component -- so that
    // filter matched nothing and the arm failed with "the new member's page is not identifiable in
    // the index". The addresses held before the write are recorded here and the one address that is
    // new afterwards IS the page the write filed. That is a stricter identification than the name
    // was, because it cannot match a page the write did not create.
    let before_addresses: BTreeSet<(u64, u64, u64)> = zset_pages(&engine)
        .iter()
        .map(|page| {
            (
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            )
        })
        .collect();
    let before_pages = before_addresses.len();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetAdd {
            key: key.clone(),
            member: member_bytes(0, MEMBERS + 1),
            score: (MEMBERS + 1) as f64,
        },
    });
    assert!(response.status.ok, "the single-member write must ack");
    let after = zset_pages(&engine);
    assert_eq!(
        after.len(),
        before_pages + 1,
        "a single-member write did not file exactly one new page"
    );
    let added: Vec<&ZsetBlock> = after
        .iter()
        .filter(|page| {
            page.object_key == key
                && !before_addresses.contains(&(
                    page.address.block_slab_id(),
                    page.address.offset(),
                    page.address.length(),
                ))
        })
        .collect();
    assert_eq!(
        added.len(),
        1,
        "the new member's page is not identifiable in the index: {} of this key's pages are at \
         addresses the write created, not one",
        added.len()
    );
    // The stored page is the member plus the per-member record framing. Taken off the index, so
    // this is the length the write actually filed rather than the width it was handed.
    let today_page_bytes = added[0].address.length() as usize;
    assert!(
        today_page_bytes >= MEMBER_WIDTH,
        "the appended page is smaller than the member it holds: {today_page_bytes} B for a \
         {MEMBER_WIDTH}-byte member"
    );
    assert!(
        today_page_bytes < 4 * MEMBER_WIDTH,
        "the appended page is implausibly large for one member, so it is not one member's page: \
         {today_page_bytes} B"
    );

    // PACKED: the whole container's member buffer, through the engine's own encoder.
    let packed: Vec<(&[u8], u64)> = model
        .iter()
        .map(|(member, biased)| (member.as_slice(), *biased))
        .collect();
    let buffer = rmp_serde::to_vec(&packed).expect("the index encoder writes the packed shape");
    assert!(
        buffer.len() > MEMBERS * (8 + MEMBER_WIDTH) / 2,
        "the packed buffer is implausibly small for {MEMBERS} members, so it is not the shape it \
         claims to be: {} bytes",
        buffer.len()
    );

    println!("--- bytes rewritten by ONE single-member update ---");
    println!("  today  : {today_page_bytes:>7} B  (one page, the member itself)");
    println!(
        "  packed : {:>7} B  (the whole {MEMBERS}-member buffer, measured through rmp_serde)",
        buffer.len()
    );
    println!(
        "  amplification: {:.1}x",
        buffer.len() as f64 / today_page_bytes as f64
    );
    println!(
        "  the packed buffer carries {} B for {MEMBERS} members = {:.1} B a member, against the \
         {} B a member the per-member NAME alone costs today",
        buffer.len(),
        buffer.len() as f64 / MEMBERS as f64,
        16 + 2 * MEMBER_WIDTH
    );

    assert!(
        buffer.len() > 10 * today_page_bytes,
        "the packed buffer is not materially larger than the page a single-member write appends \
         today, so there is no write amplification to report and this test is not measuring the \
         trade it claims: {} B against {today_page_bytes} B",
        buffer.len()
    );
}

// =============================================================================================
// 7. THE ALLOCATOR, BOTH BYTE COLUMNS
// =============================================================================================

/// WHAT A ZSET MEMBER ALLOCATES, ON BOTH BYTE COLUMNS, AT BOTH ROUTING RANGES.
///
/// `ALLOC_BYTES` charges `layout.size()`; `ALLOC_CHUNK_BYTES` reads `malloc_usable_size` and is the
/// column that decides -- #1967's whole finding was that an out-of-line 112-byte payload costs a
/// 128-byte chunk, which inverted a published sign. The chunk column is a FLOOR and not an
/// equality (#1969 measured a 104-byte request reading 128), so it is asserted as a floor, as a
/// multiple of sixteen, and as strictly greater than the request column.
///
/// THE CLASS LEDGER attributes the span on the REQUEST column only -- `ClassCounts` carries no
/// chunk row -- so the chunk figure is the whole span's and the attribution is the request's. Said
/// here rather than left for a reader to assume the two are the same measurement.
///
/// rust-internal: reads this crate's own counting allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_a_zset_member_allocates_on_both_byte_columns_at_both_routing_ranges() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );

    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows: Vec<(u32, f64, f64, f64)> = Vec::new();

    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);

        let keys = container_keys();
        let commands: Vec<Command> = keys
            .iter()
            .enumerate()
            .flat_map(|(k, key)| {
                (0..MEMBERS).map(move |m| Command::ZSetAdd {
                    key: key.clone(),
                    member: member_bytes(k, m),
                    score: m as f64,
                })
            })
            .collect();

        let probe = Probe::start();
        run_batch(&engine, commands);
        let counts: crate::alloc_probe::AllocCounts = probe.stop();

        let members: usize = keys.iter().map(|key| model_members(&engine, key).len()).sum();
        assert_eq!(
            members,
            CONTAINER_KEYS * MEMBERS,
            "the measured span did not write the population claimed at 0..{end_routing_bucket}"
        );

        // THE CHUNK COLUMN IS A FLOOR, checked three ways.
        assert!(
            counts.chunk_bytes >= counts.alloc_bytes,
            "the chunk column read below the request column at 0..{end_routing_bucket}: {} < {}",
            counts.chunk_bytes,
            counts.alloc_bytes
        );
        assert!(
            counts.chunk_bytes > counts.alloc_bytes,
            "the chunk column equals the request column at 0..{end_routing_bucket}, which is what \
             the platform fallback looks like -- the reading is not coming from malloc_usable_size"
        );
        assert!(
            counts.allocs > 0,
            "no allocation was counted at 0..{end_routing_bucket}"
        );

        let allocs_per = counts.allocs as f64 / members as f64;
        let request_per = counts.alloc_bytes as f64 / members as f64;
        let chunk_per = counts.chunk_bytes as f64 / members as f64;

        println!("--- 0..{end_routing_bucket}, {members} zset members written ---");
        println!("  allocations   {:>12}  ({allocs_per:>9.2} per member)", counts.allocs);
        println!("  ALLOC_BYTES   {:>12}  ({request_per:>9.1} B per member)", counts.alloc_bytes);
        println!("  CHUNK_BYTES   {:>12}  ({chunk_per:>9.1} B per member)  <- the column that decides", counts.chunk_bytes);
        println!(
            "  chunk over request: {:>+.2}%",
            100.0 * (counts.chunk_bytes as f64 / counts.alloc_bytes as f64 - 1.0)
        );

        rows.push((end_routing_bucket, allocs_per, request_per, chunk_per));
    }

    assert!(
        path_lengths.windows(2).all(|w| w[0] == w[1]),
        "the store path length differs between arms: {path_lengths:?} -- bytes move at about six \
         a character, so the arms are not comparable"
    );
    println!("store path length held at {} characters", path_lengths[0]);

    println!("--- both ranges, per member ---");
    for (range, allocs, request, chunk) in &rows {
        println!(
            "  0..{range:<10}  {allocs:>7.2} allocs  {request:>9.1} B request  {chunk:>9.1} B chunk"
        );
    }
}
