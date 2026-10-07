// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What an in-memory `BlockAddress` costs, and what a compact form would buy.
//!
//! WHY THIS EXISTS. `BlockAddress` is 24 bytes. Two of its fields -- the packed slab-and-offset
//! `address` word and `length` -- are always meaningful. Two more -- `page_id` and `object_id` --
//! are OPTIONAL, gated by a `present` bitmask, and the struct allocates both whether or not the
//! bitmask says they are set. That is 10 bytes of optional payload plus one byte of bitmask, and
//! the shard holds one of these per stored point: a 1,000-point feature series holds 1,000 of them.
//!
//! It was 48, with a FOURTH optional field, until `generation` was shown to be a copy of
//! `page_id.or(object_id)` on every live address this census walked and was made derived. It was
//! 40 until the slab id and the offset merged into one 64-bit word, the slab in the high half and
//! the offset in the low. It was 32 until `routing_bucket` -- a THIRD optional field, and 4 bytes
//! of the optional payload -- stopped being held at all: a block's bucket is
//! `block_routing_bucket(object_key, ..)` over the range the store is STAMPED with, so the
//! container a block is read through answers it and the block does not have to carry it.
//!
//! So the obvious question is whether to pack what is left. The answer turns on a single number:
//! how many live addresses actually carry NONE of the two optional fields. If most carry none, the
//! optional payload is dead weight and a compact form reclaims it. If most carry both, there is
//! nothing to reclaim and packing buys only the bitmask byte and the padding.
//!
//! AND THE ANSWER HAS A SECOND HALF NOW, because the struct is 24 and not 32. The payload without
//! `object_id` is 15 bytes, so `object_id` at ONE byte would take the struct to 16 and at TWO bytes
//! would leave it at 24. Packing the optional half is therefore not a question about two fields any
//! more: it is a question about whether an object identity fits in eight bits.
//!
//! AND IT DOES NOT HAVE TO, because the identity does not have to be STORED to be available.
//! Nothing bounds the objects in a routing bucket at 256, so an eight-bit identity is not on offer
//! -- but `the_object_id_on_a_live_page_entry_is_the_hash_of_fields_beside_it` measures the other
//! shape the arithmetic allows, the field GONE, and finds the stored id is
//! `stable_block_object_id(shard, kind, key)` on 2,524 of 2,524 live block entries and on
//! 20,632 of 20,632, differing on none. So the question the width turns on is not how narrow an
//! identity can be. It is what a read site that cannot recompute one would do, and `block_store`'s
//! note on `BlockAddress` carries that answer: read MISSING for an acked write, silently.
//!
//! `the_optional_payload_is_paid_for_on_every_live_address` measures that number. It is the whole
//! task, and it is measured before anything else here.
//!
//! WHAT THE ROUTING BUCKET'S DEPARTURE DID TO THIS FILE, stated rather than left to be inferred:
//! every count of "how many optional fields are set" went from four to three, the tamper probe
//! `only_one_of_the_two_address_cross_checks_on_a_read_can_fire` lost an arm that could never fire,
//! and `differing_fields` lost a row it could never report. None of the VERDICTS moved.
//!
//! WHAT THESE PROBES ARE NOT. They are `#[ignore]`d because they seed tens of thousands of
//! records and read process RSS, which is neither fast nor meaningful under a parallel test run.
//! Run them by name. Two tests here are NOT ignored, and both are cheap:
//! `an_address_is_twenty_four_bytes_and_ten_of_them_are_optional` pins the width so that
//! widening the struct is noticed rather than absorbed, and
//! `only_one_of_the_two_address_cross_checks_on_a_read_can_fire` pins how many of the address
//! cross-checks on the read path can actually fire, which is the count that decides what dropping
//! an optional field would cost.
//!
//! NON-VACUITY. Every count below is printed beside its denominator, and every census asserts its
//! maps are populated BEFORE it reports an occupancy. A census that walked an empty shard would
//! report "0 of 0 carry an optional field", which reads exactly like the answer that would justify
//! packing. `the_census_reads_every_map_that_holds_an_address` is the control: it fails if the
//! fixture stops populating any map the census claims to cover.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

/// Resident set size of this process, in bytes. Field 2 of `/proc/self/statm` is resident pages.
fn resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("procfs is mounted");
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .expect("statm has a resident field")
        .parse()
        .expect("resident field is a number");
    pages * 4096
}

/// Every live address in a shard, counted by where it lives and by what it carries.
#[derive(Default, Debug)]
struct AddressCensus {
    /// (map name, how many addresses that map holds), in walk order.
    per_map: Vec<(&'static str, usize)>,
    total: usize,
    with_block_id: usize,
    with_object_id: usize,
    with_generation: usize,
    /// How many addresses carry exactly 0, 1, 2 or 3 of the optional fields.
    ///
    /// THREE, NOT FOUR. `routing_bucket` was the fourth and is not a field any more; `generation`
    /// is the third and has no bytes of its own -- it is a presence bit over a derived value --
    /// which is why `dead_optional_bytes` below counts BYTES directly rather than scaling this
    /// histogram by an average width.
    optional_field_histogram: [usize; 4],
    /// Bytes of optional payload that are allocated and NOT set, summed over every address.
    ///
    /// Counted per field rather than as `absent_count * average_width`. The average was defensible
    /// while the two optional fields were 8 and 4 bytes wide; at 8 and 2 it would report a number
    /// no address ever pays.
    dead_optional_bytes: usize,
    /// Entries held in a `BTreeMap<_, BlockAddress>` rather than a `HashMap`, which is the
    /// population a node-overhead figure applies to.
    in_btree: usize,
    /// The largest value observed in each field, in walk order:
    /// block_slab_id, offset, length, page_id, object_id, generation.
    ///
    /// This is what decides whether a LOSSLESS narrower form exists at all. Presence tells you
    /// whether a field can be omitted; width tells you whether it can be shrunk. A field that is
    /// always set AND uses its full 64 bits cannot be made smaller without dropping information.
    widest: [u64; 6],
    // THE COPY COUNTER THAT USED TO SIT HERE IS GONE, BECAUSE IT CAN NO LONGER FAIL.
    //
    // It counted addresses whose `generation` equalled `page_id.or(object_id)`, and it answered
    // 12,008 of 12,008 at 8,000 records and 120,080 of 120,080 at 80,000 -- differing on none.
    // That number is what justified dropping the field: it carried no information of its own, so
    // deriving it cost 8 bytes and imposed no capacity ceiling, which narrowing always does.
    //
    // `generation` is now computed as exactly that expression, so a counter comparing the two
    // would be asserting `x == x` on every row and reporting 100% whatever the engine did. A
    // tautology printed beside real measurements is worse than no measurement, because it reads
    // like confirmation. What replaced it:
    //   * the derivation itself, in `BlockAddress::generation`, which no caller can bypass;
    //   * `block_store::address_size_tests::an_index_whose_generation_disagrees_is_refused_loudly`,
    //     which drives a STORED generation that disagrees and requires the load to be refused --
    //     the only place a disagreement can still arise, and the only place it can still be seen.
}

impl AddressCensus {
    fn observe(&mut self, address: &BlockAddress) {
        self.total += 1;
        self.widest[0] = self.widest[0].max(address.block_slab_id());
        self.widest[1] = self.widest[1].max(address.offset());
        self.widest[2] = self.widest[2].max(address.length());
        self.widest[3] = self.widest[3].max(address.block_id().unwrap_or(0));
        // Slot 4 held the object id and the address no longer carries one. The slot is left unused
        // rather than renumbered: every reader of `widest` indexes it by position, and a shift would
        // silently re-label five other rows.
        self.widest[5] = self.widest[5].max(address.generation().unwrap_or(0));
        let mut set = 0usize;
        if address.block_id().is_some() {
            self.with_block_id += 1;
            set += 1;
        } else {
            // The block id is TWO bytes now, not four: the encoder has always refused anything
            // above `u16::MAX`, so the field never needed more.
            self.dead_optional_bytes += 2;
        }
        // `with_object_id` STOOD HERE. There is no optional object id to be present or absent, so
        // there are no dead bytes to charge for it either: the eight it used to charge when absent
        // are not in the struct at all now, which is the whole of this change.
        if address.generation().is_some() {
            self.with_generation += 1;
            set += 1;
        }
        self.optional_field_histogram[set] += 1;
    }

    fn note(&mut self, name: &'static str, count: usize) {
        self.per_map.push((name, count));
    }

    /// The optional payload is 10 bytes. An address carrying none of it pays 10 bytes for
    /// nothing; one carrying both pays nothing for nothing.
    ///
    /// SUMMED PER FIELD AS IT IS WALKED, not reconstructed from the histogram. The two optional
    /// fields are 8 and 2 bytes wide, so "how many are unset" cannot price them: an address missing
    /// only the block id wastes 2 bytes and one missing only the object id wastes 8, and both
    /// appear in the same histogram bucket.
    fn dead_optional_bytes(&self) -> usize {
        self.dead_optional_bytes
    }

    fn report(&self, label: &str) {
        println!("--- address census: {label} ---");
        for (name, count) in &self.per_map {
            if *count > 0 {
                println!("  {name}: {count}");
            }
        }
        println!("  TOTAL live addresses: {} (of which {} sit in a BTreeMap)", self.total, self.in_btree);
        println!(
            "  optional fields SET, of {} addresses: page_id {} ({:.1}%), object_id {} ({:.1}%), generation {} ({:.1}%)",
            self.total,
            self.with_block_id,
            100.0 * self.with_block_id as f64 / self.total.max(1) as f64,
            self.with_object_id,
            100.0 * self.with_object_id as f64 / self.total.max(1) as f64,
            self.with_generation,
            100.0 * self.with_generation as f64 / self.total.max(1) as f64,
        );
        for (set, count) in self.optional_field_histogram.iter().enumerate() {
            println!(
                "  carrying exactly {set} of 3 optional fields: {count} ({:.1}% of {})",
                100.0 * *count as f64 / self.total.max(1) as f64,
                self.total
            );
        }
        // Read off the type rather than written down. A hand-written width here went stale the
        // moment the struct moved, and this line would have kept reporting the old figure.
        let width = std::mem::size_of::<BlockAddress>();
        println!(
            "  address payload resident: {} x {} = {} bytes ({:.2} MiB)",
            self.total,
            width,
            self.total * width,
            (self.total * width) as f64 / (1024.0 * 1024.0)
        );
        println!(
            "  of which optional payload NEVER SET: {} bytes ({:.2} MiB) -- the whole packing prize",
            self.dead_optional_bytes(),
            self.dead_optional_bytes() as f64 / (1024.0 * 1024.0)
        );

        println!(
            "  generation is DERIVED as page_id.or(object_id) -- not stored, so not counted here"
        );

        // Presence says whether a field can be OMITTED. Width says whether it can be SHRUNK.
        // Both have to fail before the remaining 32 bytes are justified.
        let names = [
            "block_slab_id", "offset", "length",
            "page_id", "object_id", "generation",
        ];
        let mut lossless_bits = 0u32;
        println!("  observed field widths (the bits a LOSSLESS narrower form would still need):");
        for (i, name) in names.iter().enumerate() {
            let bits = 64 - self.widest[i].leading_zeros();
            lossless_bits += bits.max(1);
            println!("    {name}: max {} -> {} bit(s)", self.widest[i], bits);
        }
        println!(
            "  a form sized to the values ACTUALLY observed here needs {lossless_bits} bits = {} bytes, \
             but those maxima are a property of THIS fixture, not of the type: every one of these \
             fields is read as u64 and a production shard is free to use the range",
            (lossless_bits + 7) / 8,
        );
    }
}

/// Walk every map on a shard that holds a `BlockAddress`, including the bucket index.
///
/// The bucket index matters and is easy to miss: `BlockIndex` carries its own `BlockAddress`, so
/// every block addressed by a model map is addressed a SECOND time here. A census that read only
/// the model maps would under-count the population by about half.
fn census(engine: &TemporalEngine, shard_id: ShardId) -> AddressCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&shard_id).expect("shard is loaded");
    let mut c = AddressCensus::default();

    // HashMap<String, BlockAddress> -- one address per key.
    for (name, map) in [
        ("strings", &shard.strings),
        ("control_state_pages", &shard.control_state_blocks),
        ("context_nodes", &shard.context_nodes),
    ] {
        let before = c.total;
        for address in map.values() {
            c.observe(address);
        }
        c.note(name, c.total - before);
    }

    // HashMap<String, HashMap<String, BlockAddress>>
    {
        let before = c.total;
        for fields in shard.hashes.values() {
            for address in fields.values() {
                c.observe(address);
            }
        }
        c.note("hashes", c.total - before);
    }

    // HashMap<String, BTreeMap<Vec<u8>, BlockAddress>>
    {
        let before = c.total;
        for members in shard.sets.values() {
            for address in members.values() {
                c.observe(address);
            }
        }
        c.in_btree += c.total - before;
        c.note("sets", c.total - before);
    }

    // HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>
    {
        let before = c.total;
        for members in shard.zsets.values() {
            for (_, address) in members.values() {
                c.observe(address);
            }
        }
        c.in_btree += c.total - before;
        c.note("zsets", c.total - before);
    }

    // HashMap<String, BTreeMap<i64, BlockAddress>>
    {
        let before = c.total;
        for elements in shard.lists.values() {
            for address in elements.values() {
                c.observe(address);
            }
        }
        c.in_btree += c.total - before;
        c.note("lists", c.total - before);
    }

    // The time-keyed series maps: HashMap<String, BTreeMap<u64, BlockAddress>>. One address PER
    // POINT, which is the population the whole question is about.
    for (name, map) in [
        ("features", &shard.features),
        ("sequences", &shard.sequences),
        ("context_events", &shard.context_events),
        ("context_indexes", &shard.context_indexes),
        ("context_audits", &shard.context_audits),
        ("context_entities", &shard.context_entities),
        ("context_children", &shard.context_children),
        ("context_summaries", &shard.context_summaries),
        ("context_compressions", &shard.context_compressions),
    ] {
        let before = c.total;
        for series in map.values() {
            for address in series.values() {
                c.observe(address);
            }
        }
        c.in_btree += c.total - before;
        c.note(name, c.total - before);
    }

    // The bucket index -- the SECOND copy of every address.
    {
        let before = c.total;
        for bucket in shard.bucket_index.bucket_map.values() {
            for (_, page) in bucket.block_index.iter() {
                c.observe(&page.address);
            }
        }
        c.note("bucket_index (BlockIndex.address)", c.total - before);
    }

    c
}

/// An engine with NO shard loaded, so the caller can load one on a chosen routing range.
///
/// `new_engine` above loads shard 1 on the engine's default range, which is what every census here
/// wants; a probe that has to vary the RANGE cannot use it, because a second `load_shard_with` on the
/// same shard answers `already_exists` and the arm silently measures the default instead.
fn engine_without_a_shard(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ))
}

fn new_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

/// `strings_n` single-address keys, plus `series_keys` feature series of `series_points` points
/// each. Returns (strings written, feature points written).
fn seed(engine: &TemporalEngine, strings_n: usize, series_keys: usize, series_points: usize) -> (usize, usize) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
    }

    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }

    (strings_n, series_keys * series_points)
}

/// THE NUMBER THIS TASK TURNS ON, at both scales.
///
/// If most live addresses carry none of the three optional fields, packing reclaims 10 bytes each
/// and is worth doing. If most carry all three, packing reclaims the bitmask byte and the padding
/// and is not.
#[test]
#[ignore = "seeds 80,000 records; run by name"]
fn the_optional_payload_is_paid_for_on_every_live_address() {
    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = new_engine(dir.path());
        let (strings_written, points_written) = seed(&engine, strings_n, series_keys, series_points);

        let c = census(&engine, 1);

        // NON-VACUITY, asserted before any occupancy is read. Eight defects in this tree have
        // been a sweep reporting a clean number off an empty set.
        assert!(
            c.total >= strings_written + points_written,
            "{label}: census must see at least the {} addresses seeded, saw {}",
            strings_written + points_written,
            c.total
        );
        let strings_seen = c.per_map.iter().find(|(n, _)| *n == "strings").expect("strings walked").1;
        let features_seen = c.per_map.iter().find(|(n, _)| *n == "features").expect("features walked").1;
        let buckets_seen = c
            .per_map
            .iter()
            .find(|(n, _)| n.starts_with("bucket_index"))
            .expect("bucket index walked")
            .1;
        assert_eq!(
            strings_written, strings_seen,
            "{label}: strings map must hold one address per key"
        );
        assert_eq!(
            points_written, features_seen,
            "{label}: features must hold one address PER POINT"
        );
        assert!(
            buckets_seen > 0,
            "{label}: the bucket index holds a second address per page and must not read zero"
        );

        c.report(label);

        // The claim, stated as a number rather than a direction.
        let none_set = c.optional_field_histogram[0];
        let all_three = c.optional_field_histogram[3];
        println!(
            "{label}: {none_set} of {} addresses ({:.1}%) carry NO optional field; {all_three} ({:.1}%) carry all three",
            c.total,
            100.0 * none_set as f64 / c.total as f64,
            100.0 * all_three as f64 / c.total as f64,
        );
        println!(
            "{label}: packing the optional payload away entirely would reclaim at most {} of {} resident address bytes ({:.1}%)",
            c.dead_optional_bytes(),
            c.total * std::mem::size_of::<BlockAddress>(),
            100.0 * c.dead_optional_bytes() as f64
                / (c.total * std::mem::size_of::<BlockAddress>()) as f64,
        );
    }
}

/// What a `BTreeMap` entry costs on top of the value it carries, measured rather than derived.
///
/// A `BTreeMap<u64, BlockAddress>` does not cost one address width per entry. Its leaf node is sized for
/// eleven entries and is allocated whole, so the per-entry cost is the node divided by how full
/// the node actually ends up -- which depends on insertion order and is not something to assume.
/// This measures it by RSS delta, and prices three value widths side by side so the container
/// cost and the value cost can be told apart.
///
/// EVERY ARM IS HELD ALIVE TO THE END, and that is load-bearing rather than tidy. Dropping an arm
/// before measuring the next one returns its heap to the allocator's free lists but NOT to the
/// kernel, so the next arm allocates into pages that are already resident and its RSS delta reads
/// as approximately zero. That is a vacuous measurement that looks exactly like a free container:
/// an earlier draft of this test reported "0.3 bytes/entry" for the packed arm and it meant
/// nothing. Keeping every map live forces each arm to fault in new pages.
///
/// The `Vec` arm is the POSITIVE CONTROL: a `Vec<(u64, BlockAddress)>` has a known, tight
/// footprint (56 bytes per element, no node), so if the harness cannot see that it cannot see
/// anything and every other number here is noise.
#[test]
#[ignore = "reads process RSS; run alone"]
fn a_btree_entry_costs_more_than_the_address_it_holds() {
    const N: usize = 400_000;

    // THE BLOCK ID IS MASKED TO SIXTEEN BITS, and that is not cosmetic. `try_from_parts` REFUSES a
    // block id wider than `u16::MAX` rather than truncating it -- a truncated block id names a
    // different block of the same object -- so this fixture's synthetic `i` panicked at 65,536 of its
    // 400,000 entries the moment the field narrowed. Masking keeps the footprint identical (the field
    // is a fixed width whatever value it holds) and keeps every value legal, which is what the
    // refusal is for. `the_container_shapes_priced_against_the_population_each_one_pays_in` carries
    // the same fixture at 200,000 and the same mask.
    fn address(i: u64) -> BlockAddress {
        BlockAddress::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i))
    }

    let control_before = resident_bytes();
    let control: Vec<(u64, BlockAddress)> = (0..N as u64).map(|i| (i, address(i))).collect();
    let control_after = resident_bytes();
    let control_per_entry = (control_after - control_before) as f64 / N as f64;
    assert_eq!(N, control.len(), "denominator: the control really holds N entries");
    println!(
        "CONTROL Vec<(u64, BlockAddress)>: {:.1} bytes/entry over {N} entries (size_of is {})",
        control_per_entry,
        std::mem::size_of::<(u64, BlockAddress)>()
    );
    // THE FLOOR IS DERIVED FROM THE ELEMENT, not written down. It read `> 40.0` beside a doc
    // comment claiming 56 bytes an element, and the element is now 32 -- so the literal was already
    // describing a shape this engine did not have, and would have turned a narrower address into a
    // blind-harness report. Half the element width is the vacuity guard: a near-zero reading fails,
    // and a correct reading cannot.
    let control_floor = size_of::<(u64, BlockAddress)>() as f64 / 2.0;
    assert!(
        control_per_entry > control_floor,
        "positive control must see the Vec it just built: {control_per_entry:.1} bytes/entry \
         against a floor of {control_floor:.1} for a {} B element -- if this is near zero the RSS \
         harness is blind and every arm below is meaningless",
        size_of::<(u64, BlockAddress)>()
    );

    // Packed widths FIRST, fat value last: if the allocator were recycling anything, the fat arm
    // measured last would read low, and it does not.
    let before_packed = resident_bytes();
    let mut packed: BTreeMap<u64, [u8; 17]> = BTreeMap::new();
    for i in 0..N as u64 {
        packed.insert(i, [i as u8; 17]);
    }
    let after_packed = resident_bytes();
    let packed_per = (after_packed - before_packed) as f64 / N as f64;
    assert_eq!(N, packed.len(), "denominator: the packed map really holds N entries");

    let before_word = resident_bytes();
    let mut word: BTreeMap<u64, u128> = BTreeMap::new();
    for i in 0..N as u64 {
        word.insert(i, i as u128);
    }
    let after_word = resident_bytes();
    let word_per = (after_word - before_word) as f64 / N as f64;
    assert_eq!(N, word.len(), "denominator: the word map really holds N entries");

    let before_fat = resident_bytes();
    let mut fat: BTreeMap<u64, BlockAddress> = BTreeMap::new();
    for i in 0..N as u64 {
        fat.insert(i, address(i));
    }
    let after_fat = resident_bytes();
    let fat_per = (after_fat - before_fat) as f64 / N as f64;
    assert_eq!(N, fat.len(), "denominator: the address map really holds N entries");

    // Every arm still alive here, which is what makes the three deltas independent.
    std::hint::black_box((&control, &packed, &word, &fat));

    println!("--- BTreeMap cost per entry, {N} ascending inserts, measured by RSS delta ---");
    for (label, value_width, per) in [
        ("BTreeMap<u64, BlockAddress> (the 56-byte value we have)", 56.0, fat_per),
        ("BTreeMap<u64, [u8; 17]>     (the packed width already written down)", 17.0, packed_per),
        ("BTreeMap<u64, u128>         (a 16-byte value)", 16.0, word_per),
    ] {
        println!(
            "  {label}: {per:.1} bytes/entry -- value {value_width:.0}, container overhead {:.1} ({:.0}% of the entry)",
            per - value_width,
            100.0 * (per - value_width) / per,
        );
    }

    // Each arm must have cost SOMETHING, or its zero is the allocator talking and not the map.
    assert!(
        packed_per > 20.0 && word_per > 20.0 && fat_per > 60.0,
        "no arm may read as free: fat {fat_per:.1}, packed {packed_per:.1}, word {word_per:.1} \
         bytes/entry -- a near-zero arm means the allocator recycled and the arm measured nothing"
    );

    println!(
        "  a 56 -> 17 byte value moves the ENTRY from {fat_per:.1} to {packed_per:.1} bytes, \
         a {:.0}% saving -- next to the {:.0}% the value widths alone suggest, because the node \
         header and the key ride along either way",
        100.0 * (fat_per - packed_per) / fat_per,
        100.0 * (56.0 - 17.0) / 56.0,
    );
    println!(
        "  the container alone costs {:.1} bytes/entry with a 56-byte value: a BTreeMap leaf is \
         sized for eleven entries and ascending inserts split it 6/5, so it settles about half \
         full and the empty half is resident",
        fat_per - 56.0,
    );
}

/// What fraction of the shard's resident memory the addresses are -- and what `load_memory_bytes`
/// says about them, which is nothing.
#[test]
#[ignore = "seeds 80,000 records; run by name"]
fn the_addresses_are_a_measurable_slice_of_a_shard_that_nothing_counts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let baseline = resident_bytes();
    let engine = new_engine(dir.path());
    let (strings_written, points_written) = seed(&engine, 40_000, 40, 1_000);
    let after = resident_bytes();
    let shard_resident = after.saturating_sub(baseline);

    let c = census(&engine, 1);
    assert!(
        c.total >= strings_written + points_written,
        "denominator: census must see the {} addresses seeded, saw {}",
        strings_written + points_written,
        c.total
    );
    assert!(shard_resident > 0, "denominator: the engine must have grown RSS at all");

    let address_bytes = c.total * 56;
    println!(
        "80,000 records: shard grew RSS by {} bytes ({:.1} MiB); {} addresses x 56 = {} bytes ({:.1} MiB) = {:.1}% of it",
        shard_resident,
        shard_resident as f64 / (1024.0 * 1024.0),
        c.total,
        address_bytes,
        address_bytes as f64 / (1024.0 * 1024.0),
        100.0 * address_bytes as f64 / shard_resident as f64,
    );
    println!(
        "  the optional payload never set within that: {} bytes = {:.2}% of shard RSS",
        c.dead_optional_bytes(),
        100.0 * c.dead_optional_bytes() as f64 / shard_resident as f64,
    );
    c.report("80,000 records, against shard RSS");
}

/// The control for the census itself.
///
/// The census claims to walk sixteen model maps plus the bucket index. If the fixture stops populating one of them -- or a
/// rename moves a field out from under the walk -- the census would quietly report a smaller
/// population, and a smaller population makes the packing case look weaker than it is. This
/// asserts the fixture really reaches the two shapes the argument distinguishes: a one-address
/// key and a many-address series.
#[test]
#[ignore = "seeds records; run by name"]
fn the_census_reads_every_map_that_holds_an_address() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = new_engine(dir.path());
    let (strings_written, points_written) = seed(&engine, 200, 2, 300);
    let c = census(&engine, 1);

    let named: Vec<&str> = c.per_map.iter().map(|(n, _)| *n).collect();
    // Sixteen model maps PLUS the bucket index, which holds a second address per block.
    assert_eq!(
        17,
        named.len(),
        "the census must walk every address-holding map; it walked {named:?}"
    );
    let populated = c.per_map.iter().filter(|(_, count)| *count > 0).count();
    assert!(
        populated >= 3,
        "denominator: at least strings, features and the bucket index must be populated; \
         {populated} of {} maps held anything: {:?}",
        named.len(),
        c.per_map
    );
    assert_eq!(strings_written, c.per_map.iter().find(|(n, _)| *n == "strings").unwrap().1);
    assert_eq!(points_written, c.per_map.iter().find(|(n, _)| *n == "features").unwrap().1);
    assert!(c.in_btree >= points_written, "the series population must be counted as BTree-held");
    println!("census walks {} maps, {populated} populated by this fixture: {:?}", named.len(), c.per_map);
}

/// Which of the address's optional fields actually DO anything on a read -- measured by tampering
/// with each one and seeing whether the read notices.
///
/// This is the fourth thing the task asks about. `decode_block_record` cross-checks the address's
/// `page_id`, `object_id` and `routing_bucket` against the record header, each written
/// `if let (Some(from_address), Some(from_record))`. Reading that source alone, all three look
/// like live corruption detectors, and three live detectors would be a strong reason to keep the
/// fields regardless of what they cost.
///
/// They are not three. The record header carries only the block id; `object_id` and
/// `routing_bucket` are deliberately NOT in it, because the index holds them -- so both of those
/// `if let` pairs can never match and the checks they guard never run. This test pins which is
/// which, because the difference decides what a compact form would actually be giving up.
///
/// A FOURTH arm used to sit beside them, comparing `address.slab_id()` against a slab id in the
/// record header, and it is gone. It was not merely unable to fire: `BlockAddress::slab_id()`
/// returns the address's own `block_slab_id`, which is the slab file `BlockStore::read` opened to
/// get these bytes, so the address side was never an independent claim. An assertion below pins
/// that, because it is the reason reinstating the field would have bought nothing.
///
/// It cannot pass by finding nothing: the honest address must read back first, the live check must
/// REFUSE a tampered address, and the inert ones must ACCEPT one. An arm that stopped firing would
/// flip a count, not fall silent.
///
/// NOT ignored. It writes ONE block into a tempdir and asserts three halves separately, in
/// hundredths of a second -- no seeding, no RSS reading, no timing, so nothing about it ever
/// needed a run-by-name budget. It was parked with the measurement probes around it and stayed
/// there, which left the claim it carries -- that of the address cross-checks on the read path
/// exactly ONE can fire -- documented and unenforced. Every argument about dropping an optional
/// field from the address rests on that count, so it belongs in the gate.
///
/// IT WAS THREE ARMS AND IS NOW TWO, and the arm that went is the one that could never fire. The
/// routing-bucket cross-check compared `address.routing_bucket()` against `header.routing_bucket`,
/// and `parse_block_record_header` returns that field as `None` unconditionally because the record
/// header HAS NO SUCH FIELD -- `encode_block_record` is handed the value and discards it. So there
/// was no address-versus-record check on the routing bucket to lose: its address side is gone with
/// the field and its record side never existed. The two arms below are unchanged, and the one that
/// CAN fire is driven with a mismatched record rather than asserted about.
#[test]
fn only_one_of_the_two_address_cross_checks_on_a_read_can_fire() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = BlockStore::new(dir.path());

    let payload = b"a page whose header states which block of its object it is".to_vec();
    let object_id = 0x0123_4567_89ab_cdefu64;
    let routing_bucket = 4_155_475_953u32;

    // The block under test is block THREE of its object, not block zero. Stripping the optional
    // field leaves the address carrying None, and if the block it names were block zero then an
    // implementation that DEFAULTED the absent field to 0 rather than skipping the check would
    // compare 0 against 0 and read through -- indistinguishable from the presence-gated behaviour
    // the last assertion claims to pin. A non-zero ordinal is what lets that half tell them apart.
    let good = store
        .append_block_of_object(&payload, Some(object_id), Some(routing_bucket), 3)
        .expect("append");
    assert_eq!(
        Some(3),
        good.block_id(),
        "denominator: the page under test must not be block zero, or the presence half below          cannot tell a skipped check from one that defaults the absent field to zero"
    );

    // DENOMINATOR: the honest address reads back, and really carries all three fields under test.
    assert_eq!(
        payload,
        store.read(&good).expect("the honest address must read back"),
        "denominator: the unmodified address reads its page"
    );
    assert!(good.block_id().is_some(), "fixture must produce an address carrying page_id");

    // Tamper with each field in turn and record whether the read noticed.
    let mut noticed: Vec<&str> = Vec::new();
    let mut ignored: Vec<&str> = Vec::new();

    let mut tampered = good.clone();
    tampered.set_block_id(Some(good.block_id().unwrap() ^ 0xffff));
    match store.read(&tampered) {
        Err(error) => {
            noticed.push("page_id");
            println!("  page_id wrong -> REFUSED: {error}");
        }
        Ok(_) => ignored.push("page_id"),
    }

    // THE OBJECT-ID ARM IS GONE WITH THE FIELD, and that is the stronger form of what it showed.
    // It tampered with an id the address carried and recorded that the read did not notice, because
    // the record header carries no object id to compare against. There is now no id on the address
    // to tamper with either, so the cross-check is not merely unfireable -- neither side of it
    // exists. One cross-check remains, and the block-id arm above is it.
    #[allow(unused_mut)]
    let mut tampered = good.clone();
    match store.read(&tampered) {
        Err(_) => noticed.push("object_id"),
        Ok(bytes) => {
            assert_eq!(payload, bytes);
            ignored.push("object_id");
            println!("  object_id cannot be made wrong: the address carries none");
        }
    }

    // THE ROUTING-BUCKET ARM CANNOT BE WRITTEN ANY MORE, and that is the strongest form of the
    // statement it used to make. `routing_bucket` was passed to `append_block_of_object` above and
    // the encoder discarded it; there is no accessor to tamper with and no header field to compare
    // against, so no record exists that could make such a check fire. The `routing_bucket` binding
    // is deliberately still handed to the append: it is what a caller passes, and a record that
    // started carrying it would make this arm expressible again.
    let _ = routing_bucket;

    // The arm that was deleted, and why it is not the same case as the two above. `slab_id()` is
    // derived, not stored: it hands back the address's own `block_slab_id`, and that is the slab
    // file the read just opened. A cross-check against it could only ever have asked whether the
    // slab a reader opened is the slab a reader opened -- true in every state, including one where
    // a slab id has been reused and a record in the new slab stamps the reused number.
    assert_eq!(
        Some(good.block_slab_id()),
        good.slab_id(),
        "the address's slab id IS its block_slab_id, so it cannot disagree with the file it named"
    );

    // The one live check is presence-gated: strip the field and it stops running altogether.
    let mut stripped = good.clone();
    stripped.set_block_id(None);
    assert!(stripped.block_id().is_none());
    let stripped_reads = store.read(&stripped).is_ok();

    println!(
        "of 2 address cross-checks on the read path, {} can fire ({:?}) and {} cannot ({:?})",
        noticed.len(),
        noticed,
        ignored.len(),
        ignored,
    );

    assert_eq!(
        vec!["page_id"],
        noticed,
        "exactly one cross-check is live today -- if this grew, a header gained a field and the \
         cost of dropping an optional field went up"
    );
    assert_eq!(
        vec!["object_id"],
        ignored,
        "object_id is not in the record header, so the check guarding it is unreachable in the \
         current format"
    );
    assert!(
        stripped_reads,
        "the live check is presence-gated: an address that omits page_id reads through UNCHECKED \
         rather than failing, so dropping the field would disable the detector silently"
    );
}

/// The payload checksum cannot tell one record from another record at the same address.
///
/// WHY THIS EXISTS. The safety argument for releasing the shard read guard across serving block
/// I/O is that compaction relocates by append-and-repoint, that slab ids are strictly monotonic so
/// a stale address can never resolve to a different record, and that a lost race "fails block-id +
/// checksum and answers absent". The last clause is the one worth testing, because
/// `only_one_of_the_two_address_cross_checks_on_a_read_can_fire` has already shown that the
/// object-id arm cannot fire -- which leaves the block ordinal and the checksum holding the whole
/// of it.
///
/// The checksum holds none of it. `block_record_checksum_field` is a CRC32C over the payload
/// alone, stored in that record's own header. It binds a record to ITSELF. Any intact record
/// verifies, whoever it belongs to and whoever asked for it, so it detects corruption and is
/// structurally incapable of detecting misdirection. This test shows that with two stores: an
/// address minted against one record is served a DIFFERENT record, in full, with no error, from
/// the same (slab, offset, length) in another store.
///
/// WHAT THIS DOES AND DOES NOT SAY. It does NOT say the read path is unsafe today. Slab ids are
/// monotonic -- `a_roll_never_mints_an_id_a_slab_file_already_holds` in `block_store.rs` now pins
/// the half of that derivation which was unguarded -- so the state this test constructs by using
/// two stores is not reachable through one. What it says is WHERE the safety comes from: from
/// monotonic slab ids, from the stored-length check, and from the block ordinal. Not from the
/// checksum. Anything that weakens monotonicity has no second line behind it, and a reader who
/// takes "checksum" in that argument to mean "the bytes are bound to the address they came from"
/// is reading a guarantee that was never written.
///
/// NON-VACUITY. The two payloads must differ and must be the same length; the two addresses must
/// agree on slab, offset, length and block ordinal and must DISAGREE on object id -- that last is
/// the field whose cross-check is dead, so it is what a live one would have caught. Each store
/// must also read its own record back first, or the test would be asserting against a read path
/// that cannot serve anything.
#[test]
fn the_payload_checksum_cannot_tell_one_record_from_another_at_the_same_address() {
    let first_dir = tempfile::tempdir().expect("tempdir");
    let second_dir = tempfile::tempdir().expect("tempdir");
    let first_store = BlockStore::new(first_dir.path());
    let second_store = BlockStore::new(second_dir.path());

    // Equal length, different content, both under the compression floor so the stored length is
    // the payload length and nothing here depends on what zstd decides to do.
    let first_payload = b"the record an address was minted for---".to_vec();
    let second_payload = b"a different record, same length, same!!".to_vec();
    assert_eq!(
        first_payload.len(),
        second_payload.len(),
        "denominator: the two payloads must be the same length, or the addresses cannot collide"
    );
    assert_ne!(
        first_payload, second_payload,
        "denominator: the two payloads must differ, or a wrong read would look like a right one"
    );

    let stale = first_store
        .append_block_of_object(&first_payload, Some(111), Some(7), 3)
        .expect("append");
    let live = second_store
        .append_block_of_object(&second_payload, Some(222), Some(9), 3)
        .expect("append");

    // The two addresses agree on everything a read uses to FIND bytes.
    assert_eq!(
        stale.block_slab_id(), live.block_slab_id(),
        "denominator: same slab id"
    );
    assert_eq!(stale.offset(), live.offset(), "denominator: same offset");
    assert_eq!(stale.length(), live.length(), "denominator: same length");
    assert_eq!(
        stale.block_id(),
        live.block_id(),
        "denominator: same block ordinal, so the one live cross-check reads through"
    );
    // They used to disagree on exactly the field whose cross-check could not fire. That field is
    // gone from the address, so the denominator is now the slab coordinates the two share and the
    // payloads that differ, both asserted below.

    // Denominator: each address reads its own record back before anything is crossed over.
    assert_eq!(
        first_payload,
        first_store.read(&stale).expect("the first address reads its own record"),
        "denominator: the honest read works in the first store"
    );
    assert_eq!(
        second_payload,
        second_store.read(&live).expect("the second address reads its own record"),
        "denominator: the honest read works in the second store"
    );

    let served = second_store
        .read(&stale)
        .expect("the misdirected read SUCCEEDS -- that is the finding, not a failure of the test");
    assert_eq!(
        second_payload, served,
        "an address minted for object 111 was served object 222's record, in full, with no error: \
         the CRC32C verified that record against itself and nothing compared it to the address"
    );
    assert_ne!(
        first_payload, served,
        "and what came back is not the record the address was minted for"
    );
}

/// The width guard. Not ignored: it is free, and it is the thing that makes a future widening
/// visible.
///
/// `block_store.rs` already asserts the 32, and a `const _` beside the declaration makes a
/// widening a BUILD failure. This adds the decomposition, because 32 on its own does not say
/// WHERE it goes, and the whole packing argument is about the optional payload inside it. If a
/// field is added, or an optional field is promoted to always-present, this fails with a number
/// that names which half moved.
///
/// THE OPTIONAL SHARE HAS MOVED TWICE, BOTH TIMES BECAUSE THE ALWAYS-PRESENT HALF SHRANK. It was
/// 48 bytes with half of it optional; `generation` -- one of four optional fields and a copy of
/// `block_id.or(object_id)` at every write site -- became derived, taking eight bytes off the
/// OPTIONAL half and leaving 40 with two fifths optional. Then the slab id and the offset merged
/// into one word, taking eight off the ALWAYS half, and the share went back to a half of a smaller
/// struct. Then the ROUTING BUCKET left -- four bytes off the OPTIONAL half, this time, the first
/// step since `generation` to take any -- beside the block id narrowing from 32 bits to 16, which
/// takes two more off the same half.
///
/// SO THE OPTIONAL HALF HAS FINALLY MOVED, from 16 bytes to 10, and the share it holds has fallen
/// from a half to 41%. Both numbers are asserted, and asserted separately, because they move for
/// different reasons: the 10 is what this change did to the optional payload, and the 41% is that
/// 10 against a struct the same change shrank. `block_store.rs` has the byte-by-byte accounting in
/// `every_byte_of_a_block_address_is_accounted_for`, where the fields are still visible.
#[test]
fn an_address_is_sixteen_bytes_and_two_of_them_are_optional() {
    // Always meaningful: the packed slab-and-offset word and the 32-bit byte count.
    const ALWAYS: usize = 8 + 4;
    // ONE optional field now: the 16-bit block id. The u64 identity beside it was the object id,
    // and it is derived from the terms rather than stored, so it is not a field to be present or
    // absent any more.
    const OPTIONAL: usize = 2;
    // The presence bitmask.
    const BITMASK: usize = 1;

    assert_eq!(16, std::mem::size_of::<BlockAddress>(), "the address width moved");
    assert_eq!(8, std::mem::align_of::<BlockAddress>());
    assert_eq!(12, ALWAYS);
    assert_eq!(2, OPTIONAL);
    assert_eq!(
        16,
        ALWAYS + OPTIONAL + BITMASK + 1,
        "12 always + 2 optional + 1 bitmask + 1 padding = 16; if this stops adding up, a field \
         changed shape and the packing arithmetic in this module is stale"
    );

    // The optional payload is 12% of the struct, down from 41%. BOTH halves moved this time and in
    // the same direction: the payload lost the eight bytes of the object id, and the struct lost the
    // same eight -- which is why the share fell rather than rose. The 2 above states the absolute
    // quantity separately, because a share moves when either half does.
    assert_eq!(
        12,
        100 * OPTIONAL / std::mem::size_of::<BlockAddress>(),
        "the optional payload is 12% of the address"
    );

    // An address built with no optional field is the same 16 bytes as one built with both.
    // This is the fact that makes the question worth asking at all.
    let bare = BlockAddress::from_parts(1, 0, 64, None, None);
    let full = BlockAddress::from_parts(1, 0, 64, Some(1), Some(2));
    assert_eq!(std::mem::size_of_val(&bare), std::mem::size_of_val(&full));
    assert!(bare.block_id().is_none() && full.block_id().is_some());

    // The derived generation follows its identity, in both directions.
    assert_eq!(bare.generation(), None);
    assert_eq!(full.generation(), Some(1));
}

// ---------------------------------------------------------------------------------------------
// Is the same descriptor stored more than once? (the interning premise)
// ---------------------------------------------------------------------------------------------

/// Every live address, grouped by the physical location it names and by its exact value.
///
/// INTERNING'S PREMISE, stated as something that can be false. A handle table only pays if the
/// same descriptor is STORED more than once -- if the bucket index's address for a block and the
/// model map's address for that same block are equal. They are both built on the write path from
/// the same parts, so they look like they must be. They are not obliged to be: the two are
/// written by different call sites, and a block rewritten in place keeps one entry in the bucket
/// index while every point that landed in it keeps whatever it was given.
///
/// So this counts MATCHING against DIFFERING with a denominator, and when they differ it says
/// which field moved. A census that reported only "120,080 addresses" would make interning look
/// like a 2x saving whether or not a single pair actually matches.
#[derive(Default)]
struct DuplicationCensus {
    /// How many times each distinct address VALUE is stored anywhere on the shard.
    stores_per_value: std::collections::HashMap<BlockAddress, usize>,
    /// The distinct address values a model map holds for one physical location.
    model_values_at: std::collections::HashMap<(u64, u64), std::collections::HashSet<BlockAddress>>,
    /// The address the bucket index holds for one physical location.
    bucket_value_at: std::collections::HashMap<(u64, u64), BlockAddress>,
    /// How many physical locations the bucket index named twice with different values. Nonzero
    /// would mean "the bucket-index address" is not well defined and the pairing below is wrong.
    bucket_location_collisions: usize,
    model_total: usize,
    bucket_total: usize,
}

impl DuplicationCensus {
    fn observe_model(&mut self, address: &BlockAddress) {
        self.model_total += 1;
        *self.stores_per_value.entry(address.clone()).or_default() += 1;
        self.model_values_at
            .entry((address.block_slab_id(), address.offset()))
            .or_default()
            .insert(address.clone());
    }

    fn observe_bucket(&mut self, address: &BlockAddress) {
        self.bucket_total += 1;
        *self.stores_per_value.entry(address.clone()).or_default() += 1;
        let key = (address.block_slab_id(), address.offset());
        if let Some(existing) = self.bucket_value_at.get(&key) {
            if existing != address {
                self.bucket_location_collisions += 1;
            }
        }
        self.bucket_value_at.insert(key, address.clone());
    }

    fn total(&self) -> usize {
        self.model_total + self.bucket_total
    }

    fn distinct_values(&self) -> usize {
        self.stores_per_value.len()
    }
}

/// Which fields two addresses for the same physical location disagree on.
fn differing_fields(a: &BlockAddress, b: &BlockAddress) -> Vec<&'static str> {
    let mut out = Vec::new();
    if a.length() != b.length() {
        out.push("length");
    }
    if a.block_id() != b.block_id() {
        out.push("page_id");
    }
    // No `object_id` row: an address does not carry one, so it cannot be the field that differs.
    // Leaving the row in would offer a name this function can never report, which reads like
    // coverage and is not -- the same reason `generation` has no row.
    // No `generation` row: it is derived as `page_id.or(object_id)` and both of those are
    // compared above, so it cannot be the field that differs. Leaving the row in would offer a
    // name this function can never report, which reads like coverage and is not.
    //
    // No `routing_bucket` row either, and for the same reason one step further along: it is not a
    // field at all. Where a block is filed is the key of the bucket map holding it, which two
    // addresses cannot disagree about because neither of them carries it.
    out
}

/// The same walk `census` does, but tagging each address with whether a MODEL map or the BUCKET
/// INDEX holds it, because the pairing between those two is the whole question.
fn duplication_census(engine: &TemporalEngine, shard_id: ShardId) -> DuplicationCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&shard_id).expect("shard is loaded");
    let mut d = DuplicationCensus::default();

    for map in [&shard.strings, &shard.control_state_blocks, &shard.context_nodes] {
        for address in map.values() {
            d.observe_model(address);
        }
    }
    for fields in shard.hashes.values() {
        for address in fields.values() {
            d.observe_model(address);
        }
    }
    for members in shard.sets.values() {
        for address in members.values() {
            d.observe_model(address);
        }
    }
    for members in shard.zsets.values() {
        for (_, address) in members.values() {
            d.observe_model(address);
        }
    }
    for elements in shard.lists.values() {
        for address in elements.values() {
            d.observe_model(address);
        }
    }
    for map in [
        &shard.features,
        &shard.sequences,
        &shard.context_events,
        &shard.context_indexes,
        &shard.context_audits,
        &shard.context_entities,
        &shard.context_children,
        &shard.context_summaries,
        &shard.context_compressions,
    ] {
        for series in map.values() {
            for address in series.values() {
                d.observe_model(address);
            }
        }
    }
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, page) in bucket.block_index.iter() {
            d.observe_bucket(&page.address);
        }
    }
    d
}

/// THE NUMBER CANDIDATE A TURNS ON: matching against differing, with a denominator.
#[test]
#[ignore = "seeds 80,000 records; run by name"]
fn a_model_map_address_and_the_bucket_index_address_for_one_page_are_not_the_same_value() {
    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = new_engine(dir.path());
        let (strings_written, points_written) = seed(&engine, strings_n, series_keys, series_points);

        let d = duplication_census(&engine, 1);

        // NON-VACUITY FIRST. A pairing over an empty bucket index reports "0 differing", which
        // reads exactly like "they all match" -- the answer that would justify interning.
        assert!(
            d.model_total >= strings_written + points_written,
            "{label}: denominator -- model maps must hold at least the {} addresses seeded, hold {}",
            strings_written + points_written,
            d.model_total
        );
        assert!(
            d.bucket_total > 0,
            "{label}: denominator -- the bucket index must hold addresses, holds {}",
            d.bucket_total
        );
        assert_eq!(
            0, d.bucket_location_collisions,
            "{label}: the bucket index named one physical location with two different addresses \
             {} times; 'the bucket-index address for a page' would not be well defined and the \
             pairing below would be meaningless",
            d.bucket_location_collisions
        );

        // Pair every location the bucket index knows against the model addresses at that same
        // location. Three outcomes, and the denominator is printed for each.
        let mut paired = 0usize;
        let mut matching = 0usize;
        let mut differing = 0usize;
        let mut bucket_only = 0usize;
        let mut field_tally: std::collections::BTreeMap<String, usize> = Default::default();
        for (location, bucket_address) in &d.bucket_value_at {
            match d.model_values_at.get(location) {
                None => bucket_only += 1,
                Some(model_values) => {
                    paired += 1;
                    if model_values.len() == 1 && model_values.contains(bucket_address) {
                        matching += 1;
                    } else {
                        differing += 1;
                        let sample = model_values.iter().next().expect("non-empty");
                        let fields = differing_fields(bucket_address, sample);
                        let key = if fields.is_empty() {
                            "(equal to the sampled one; the location holds several values)".to_string()
                        } else {
                            fields.join("+")
                        };
                        *field_tally.entry(key).or_default() += 1;
                    }
                }
            }
        }
        let model_only = d
            .model_values_at
            .keys()
            .filter(|location| !d.bucket_value_at.contains_key(*location))
            .count();

        println!("--- descriptor duplication: {label} ---");
        println!(
            "  live addresses: {} total = {} in model maps + {} in the bucket index",
            d.total(),
            d.model_total,
            d.bucket_total
        );
        println!(
            "  DISTINCT address values: {} of {} stores ({:.1}% of stores are a repeat of a value \
             already stored elsewhere)",
            d.distinct_values(),
            d.total(),
            100.0 * (d.total() - d.distinct_values()) as f64 / d.total() as f64,
        );
        println!(
            "  physical locations (slab, offset): {} named by a model map, {} named by the bucket index",
            d.model_values_at.len(),
            d.bucket_value_at.len()
        );
        println!(
            "  PAIRED locations (named by both): {paired} of {} bucket-index locations",
            d.bucket_value_at.len()
        );
        println!(
            "    MATCHING (model value identical to the bucket-index value): {matching} of {paired} ({:.1}%)",
            if paired == 0 { 0.0 } else { 100.0 * matching as f64 / paired as f64 },
        );
        println!(
            "    DIFFERING: {differing} of {paired} ({:.1}%)",
            if paired == 0 { 0.0 } else { 100.0 * differing as f64 / paired as f64 },
        );
        for (fields, count) in &field_tally {
            println!("      differ on {fields}: {count}");
        }
        println!("  bucket-index locations no model map names: {bucket_only}");
        println!("  model-map locations the bucket index does not name: {model_only}");

        // What interning would actually buy, priced off the distinct count rather than off the
        // total. One table slot per distinct value (56 B) plus one handle per store.
        for handle_width in [4usize, 8usize] {
            let now = d.total() * 56;
            let interned = d.distinct_values() * 56 + d.total() * handle_width;
            println!(
                "  a {handle_width}-byte handle over a dense table: {} B -> {} B ({:+.1}%)",
                now,
                interned,
                100.0 * (interned as f64 - now as f64) / now as f64,
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// What the CONTAINER would buy (the other candidate)
// ---------------------------------------------------------------------------------------------

/// How long the timestamped series on a shard actually are.
///
/// WHY THE SHAPE OF THIS MATTERS. `BlockIndexMap::Empty`/`One`/`Many` pays exactly once: on a map
/// holding ONE entry, where a `BTreeMap` node sized for eleven is allocated to carry a single
/// value. It buys nothing at all on a map holding a thousand. So "apply the `One` shape to the
/// model maps" is only a saving if the model maps are mostly short, and that is a fact about the
/// workload rather than about the type.
///
/// The fixture is seeded in two arms on purpose: long series (the feature workload #1730
/// measured) and single-point series. Reporting the histogram off long series alone would say
/// "no series is short, `One` buys nothing" -- which is a property of the seed, not of the store.
#[derive(Default)]
struct SeriesLengthCensus {
    /// series length -> how many series have it, for lengths 1..=4; everything longer is `long`.
    exactly: [usize; 5],
    long: usize,
    series: usize,
    entries: usize,
}

impl SeriesLengthCensus {
    fn observe(&mut self, len: usize) {
        self.series += 1;
        self.entries += len;
        if len <= 4 {
            self.exactly[len] += 1;
        } else {
            self.long += 1;
        }
    }

    fn report(&self, label: &str) {
        println!(
            "  {label}: {} series holding {} entries",
            self.series, self.entries
        );
        if self.series == 0 {
            return;
        }
        for len in 1..=4usize {
            println!(
                "    exactly {len} entry/entries: {} of {} series ({:.1}%)",
                self.exactly[len],
                self.series,
                100.0 * self.exactly[len] as f64 / self.series as f64
            );
        }
        println!(
            "    5 or more: {} of {} series ({:.1}%)",
            self.long,
            self.series,
            100.0 * self.long as f64 / self.series as f64
        );
    }
}

fn series_lengths(engine: &TemporalEngine, shard_id: ShardId) -> SeriesLengthCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&shard_id).expect("shard is loaded");
    let mut c = SeriesLengthCensus::default();
    for map in [
        &shard.features,
        &shard.sequences,
        &shard.context_events,
        &shard.context_indexes,
        &shard.context_audits,
        &shard.context_entities,
        &shard.context_children,
        &shard.context_summaries,
        &shard.context_compressions,
    ] {
        for series in map.values() {
            c.observe(series.len());
        }
    }
    c
}

/// The `Empty`/`One`/`Many` shape, written out here so it can be PRICED before it is adopted.
/// This is the same three-way split `BlockIndexMap` already uses in the bucket index.
enum PricedSeries {
    #[allow(dead_code)]
    Empty,
    One(u64, BlockAddress),
    Many(BTreeMap<u64, BlockAddress>),
}

impl PricedSeries {
    fn insert(&mut self, key: u64, value: BlockAddress) {
        match self {
            PricedSeries::Empty => *self = PricedSeries::One(key, value),
            PricedSeries::One(existing, _) if *existing == key => {
                *self = PricedSeries::One(key, value)
            }
            PricedSeries::One(..) => {
                let mut map = BTreeMap::new();
                if let PricedSeries::One(first_key, first) =
                    std::mem::replace(self, PricedSeries::Empty)
                {
                    map.insert(first_key, first);
                }
                map.insert(key, value);
                *self = PricedSeries::Many(map);
            }
            PricedSeries::Many(map) => {
                map.insert(key, value);
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            PricedSeries::Empty => 0,
            PricedSeries::One(..) => 1,
            PricedSeries::Many(map) => map.len(),
        }
    }
}

/// THE PRICE OF BOTH CONTAINER SHAPES, measured by RSS delta with every arm held alive.
///
/// Two populations, because the two shapes pay in different ones:
///
///   SHORT: 200,000 series of one entry. This is where `Empty`/`One`/`Many` pays -- a `BTreeMap`
///   node sized for eleven, allocated to carry one value.
///
///   LONG: 200 series of 1,000 entries. This is the feature workload, where the node is full and
///   `One` buys nothing -- so the only lever left is the WIDTH of the value, priced here as a
///   per-series block table: `BTreeMap<u64, u32>` beside a `Vec<BlockAddress>` the series owns.
///
/// The `Vec` arm is the POSITIVE CONTROL for the harness, as in the sibling probe: if it cannot
/// see a container with a known footprint, no number below means anything.
#[test]
#[ignore = "reads process RSS; run alone"]
fn the_container_shapes_priced_against_the_population_each_one_pays_in() {
    const SHORT_SERIES: usize = 200_000;
    const LONG_SERIES: usize = 200;
    const LONG_POINTS: usize = 1_000;

    fn address(i: u64) -> BlockAddress {
        BlockAddress::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i))
    }

    // POSITIVE CONTROL first.
    let control_before = resident_bytes();
    let control: Vec<(u64, BlockAddress)> = (0..SHORT_SERIES as u64).map(|i| (i, address(i))).collect();
    let control_after = resident_bytes();
    let control_per = (control_after - control_before) as f64 / SHORT_SERIES as f64;
    assert_eq!(SHORT_SERIES, control.len(), "denominator: the control holds every element");
    assert!(
        control_per > 40.0,
        "positive control must see the Vec it just built: {control_per:.1} bytes/entry -- \
         if this reads near zero the RSS harness is blind and every arm below is noise"
    );
    println!("CONTROL Vec<(u64, BlockAddress)>: {control_per:.1} bytes/entry");

    // --- SHORT: one entry per series, which is the population `One` pays in. ---
    let short_btree_before = resident_bytes();
    let short_btree: Vec<BTreeMap<u64, BlockAddress>> = (0..SHORT_SERIES as u64)
        .map(|i| {
            let mut map = BTreeMap::new();
            map.insert(i, address(i));
            map
        })
        .collect();
    let short_btree_after = resident_bytes();
    let short_btree_per = (short_btree_after - short_btree_before) as f64 / SHORT_SERIES as f64;
    assert!(
        short_btree.iter().all(|map| map.len() == 1),
        "denominator: every short BTreeMap arm really holds one entry"
    );

    let short_split_before = resident_bytes();
    let short_split: Vec<PricedSeries> = (0..SHORT_SERIES as u64)
        .map(|i| {
            let mut series = PricedSeries::Empty;
            series.insert(i, address(i));
            series
        })
        .collect();
    let short_split_after = resident_bytes();
    let short_split_per = (short_split_after - short_split_before) as f64 / SHORT_SERIES as f64;
    assert!(
        short_split.iter().all(|series| series.len() == 1),
        "denominator: every split arm really holds one entry"
    );

    // --- LONG: 1,000 entries per series, which is the feature workload. ---
    let long_entries = LONG_SERIES * LONG_POINTS;

    let long_btree_before = resident_bytes();
    let long_btree: Vec<BTreeMap<u64, BlockAddress>> = (0..LONG_SERIES as u64)
        .map(|s| {
            let mut map = BTreeMap::new();
            for t in 0..LONG_POINTS as u64 {
                // The real shape: MANY consecutive timestamps naming ONE block. A feature series
                // coalesces, so the same address value is stored for a run of points.
                map.insert(t, address(s * 16 + t / 500));
            }
            map
        })
        .collect();
    let long_btree_after = resident_bytes();
    let long_btree_per = (long_btree_after - long_btree_before) as f64 / long_entries as f64;
    assert!(
        long_btree.iter().all(|map| map.len() == LONG_POINTS),
        "denominator: every long BTreeMap arm really holds {LONG_POINTS} entries"
    );

    // The per-series block table: the series owns its addresses, the map holds an index into them.
    // NOTE the lifetime: the table is owned BY the series, so an index cannot outlive the table
    // that gives it meaning -- there is no shard-wide handle here and nothing to free separately.
    let long_table_before = resident_bytes();
    let long_table: Vec<(BTreeMap<u64, u32>, Vec<BlockAddress>)> = (0..LONG_SERIES as u64)
        .map(|s| {
            let mut pages: Vec<BlockAddress> = Vec::new();
            let mut map = BTreeMap::new();
            for t in 0..LONG_POINTS as u64 {
                let wanted = address(s * 16 + t / 500);
                let slot = match pages.iter().position(|held| *held == wanted) {
                    Some(slot) => slot,
                    None => {
                        pages.push(wanted);
                        pages.len() - 1
                    }
                };
                map.insert(t, slot as u32);
            }
            (map, pages)
        })
        .collect();
    let long_table_after = resident_bytes();
    let long_table_per = (long_table_after - long_table_before) as f64 / long_entries as f64;
    assert!(
        long_table.iter().all(|(map, _)| map.len() == LONG_POINTS),
        "denominator: every table arm really holds {LONG_POINTS} entries"
    );
    let distinct_pages: usize = long_table.iter().map(|(_, pages)| pages.len()).sum();
    assert!(
        distinct_pages < long_entries,
        "denominator: the table arm must actually be sharing -- {distinct_pages} distinct pages \
         over {long_entries} entries"
    );

    // Every arm still live, which is what makes the deltas independent rather than the
    // allocator handing the next arm pages the last one just freed.
    std::hint::black_box((&control, &short_btree, &short_split, &long_btree, &long_table));

    println!("--- SHORT population: {SHORT_SERIES} series of ONE entry ---");
    println!("  BTreeMap<u64, BlockAddress>: {short_btree_per:.1} bytes/entry");
    println!("  Empty/One/Many split:        {short_split_per:.1} bytes/entry");
    println!(
        "  the One shape saves {:.1} bytes/entry ({:.1}%) on a single-entry series",
        short_btree_per - short_split_per,
        100.0 * (short_btree_per - short_split_per) / short_btree_per,
    );

    println!("--- LONG population: {LONG_SERIES} series of {LONG_POINTS} entries ---");
    println!("  BTreeMap<u64, BlockAddress>:          {long_btree_per:.1} bytes/entry");
    println!(
        "  BTreeMap<u64, u32> + per-series pages: {long_table_per:.1} bytes/entry \
         ({distinct_pages} distinct pages over {long_entries} entries)"
    );
    println!(
        "  the per-series page table saves {:.1} bytes/entry ({:.1}%) on a coalescing series",
        long_btree_per - long_table_per,
        100.0 * (long_btree_per - long_table_per) / long_btree_per,
    );

    // No arm may read as free, or its zero is the allocator talking and not the container.
    assert!(
        short_btree_per > 40.0 && short_split_per > 20.0,
        "no short arm may read as free: btree {short_btree_per:.1}, split {short_split_per:.1}"
    );
    assert!(
        long_btree_per > 60.0 && long_table_per > 10.0,
        "no long arm may read as free: btree {long_btree_per:.1}, table {long_table_per:.1}"
    );
}

/// Where the fixture's series lengths actually fall -- the fact that decides whether the `One`
/// shape is worth adopting for the model maps at all.
#[test]
#[ignore = "seeds two shards; run by name"]
fn the_feature_workload_has_no_short_series_for_the_one_shape_to_help() {
    // ARM 1: the workload #1730 measured -- long, coalescing series.
    let long_dir = tempfile::tempdir().expect("tempdir");
    let long_engine = new_engine(long_dir.path());
    let (_, long_points) = seed(&long_engine, 0, 40, 1_000);
    let long = series_lengths(&long_engine, 1);
    assert!(
        long.series > 0 && long.entries >= long_points,
        "denominator: the long arm must hold series -- {} series, {} entries, {long_points} seeded",
        long.series,
        long.entries
    );

    // ARM 2: single-point series, so the `One` population is NOT empty and the histogram is not
    // reporting a property of the seed as a property of the store.
    let short_dir = tempfile::tempdir().expect("tempdir");
    let short_engine = new_engine(short_dir.path());
    let (_, short_points) = seed(&short_engine, 0, 4_000, 1);
    let short = series_lengths(&short_engine, 1);
    assert!(
        short.series > 0 && short.entries >= short_points,
        "denominator: the short arm must hold series -- {} series, {} entries, {short_points} seeded",
        short.series,
        short.entries
    );

    println!("--- timestamped series lengths ---");
    long.report("40 feature series of 1,000 points");
    short.report("4,000 feature series of 1 point");

    // The control on the two arms: they must land in DIFFERENT halves of the histogram, or the
    // seed is not producing the two populations this is supposed to tell apart.
    assert_eq!(
        0, long.exactly[1],
        "the long arm must produce no single-entry series; it produced {}",
        long.exactly[1]
    );
    assert!(
        short.exactly[1] > 0,
        "the short arm must produce single-entry series; it produced {} of {}",
        short.exactly[1],
        short.series
    );
    println!(
        "  so the One shape helps {} of {} series in the short arm and {} of {} in the long one",
        short.exactly[1], short.series, long.exactly[1], long.series,
    );
}

/// THE FREE GUARD over the decision above: the bucket index holds ONE block inline, and the
/// timestamped series maps do not.
///
/// WHY THIS IS THE THING TO PIN. The MEASUREMENT probes in this module are `#[ignore]`d -- they
/// read process RSS and seed tens of thousands of records -- so on a normal run none of those
/// execute. This one is free and always runs, and it pins the two structural facts the
/// recommendation rests on:
///
///   1. `BlockIndexMap` gives a one-entry index a SMALL ALLOCATION instead of a tree node. That is
///      what makes it cost about 120 bytes for a one-entry index where a
///      `BTreeMap<u64, BlockAddress>` costs 764.3 -- a `BTreeMap` leaf is allocated whole and sized
///      for eleven entries whether one is filed in it or eleven are.
///
///      THIS USED TO BE PINNED AS "WIDER INLINE THAN THE MAP IT REPLACES", and that clause is no
///      longer true or needed. The single-block arm held its whole entry inline, so the enum was
///      wider than a `BTreeMap` and the guard watched that width. The entry is behind a pointer now
///      and the enum is exactly a container header, so the SAVING no longer comes from the width at
///      all: it comes from the ALLOCATION SIZE, one entry against a node sized for eleven. The
///      predicate is therefore restated as the thing that still carries the saving, and the old one
///      is recorded here rather than deleted, because a guard whose claim quietly changed meaning is
///      worse than one that was rewritten.
///
///   2. All six timestamped series maps are still the SAME container. `timestamped_series_mut`
///      hands out one type for six kinds, so the shape cannot be adopted for one of them alone --
///      which is exactly why the recommendation is stated for the six together. The binding
///      below is a compile-time proof of that: if any one of the six is narrowed and the others
///      are not, this stops compiling rather than passing on a stale assumption.
///
/// MUTATION. Widening the block index past a container header -- putting an entry back inline --
/// fires the first assert. Changing any one of the six map types fires the second as a compile error
/// rather than a failure.
#[test]
fn the_bucket_index_holds_one_page_out_of_line_and_the_series_maps_hold_none() {
    use crate::engine::state::{BlockIndex, BlockIndexMap};

    // (1) The split shape is no wider than the CONTAINER HEADER it has to carry anyway, and a
    // single block costs one allocation of the entry rather than a node sized for eleven.
    let split = std::mem::size_of::<BlockIndexMap>();
    let map = std::mem::size_of::<BTreeMap<u64, BlockAddress>>();
    let page = std::mem::size_of::<BlockIndex>();
    let list = std::mem::size_of::<Vec<(u64, BlockIndex)>>();
    println!(
        "BlockIndexMap is {split} B, BTreeMap<u64, BlockAddress> is {map} B, a page list header is \
         {list} B, BlockIndex is {page} B"
    );
    assert_eq!(
        list, split,
        "BlockIndexMap must be exactly its page-list header -- it is {split} B against {list} B. \
         Any excess means an arm has stopped riding a pointer niche and the node is paying for a \
         tag again"
    );
    assert!(
        split < page,
        "BlockIndexMap must NOT be able to hold a whole {page}-byte BlockIndex inline any more; it \
         is {split} B. If it has grown to fit one, the inline arm has come back and every node in \
         the bucket map is paying for it"
    );

    // POSITIVE CONTROL for the predicate above, so a passing assert is not just a narrow enum.
    // This is exactly what the mutation would produce -- the `One` variant holding its entry INLINE
    // again -- and it must FAIL the same test the real shape passes. Without this, `split < page`
    // would keep passing on any enum that happened to be narrow for an unrelated reason, and the
    // guard would stop watching the thing it names.
    enum InlineShape {
        #[allow(dead_code)]
        Empty,
        #[allow(dead_code)]
        One(u64, BlockIndex),
        #[allow(dead_code)]
        Many(Vec<(u64, BlockIndex)>),
    }
    let inline = std::mem::size_of::<InlineShape>();
    println!("  positive control: the same shape with One held inline is {inline} B");
    assert!(
        inline >= page,
        "the control must HOLD a page inline: an inline One is {inline} B against a {page}-byte \
         page. If this ever reads as narrow, the predicate above cannot tell an inline page from a \
         pointer to one and the guard is vacuous"
    );
    assert!(
        !(inline == list && inline < page),
        "the control must FAIL the predicate the real shape passes ({inline} B inline, {list} B \
         list header, {page} B page)"
    );

    // (2) All six timestamped series maps are one container. This binding is the guard: it is a
    // compile-time proof, and the count below is its denominator.
    fn one_container(_: &std::collections::HashMap<String, BTreeMap<u64, BlockAddress>>) {}
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = new_engine(dir.path());
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let six = [
        "features",
        "context_indexes",
        "context_audits",
        "context_children",
        "context_summaries",
        "context_compressions",
    ];
    one_container(&shard.features);
    one_container(&shard.context_indexes);
    one_container(&shard.context_audits);
    one_container(&shard.context_children);
    one_container(&shard.context_summaries);
    one_container(&shard.context_compressions);
    assert_eq!(
        6,
        six.len(),
        "denominator: the six kinds timestamped_series_mut dispatches over are {six:?}"
    );
    println!(
        "  {} timestamped series maps share one container type, so the split shape has to be \
         adopted for all of them or none",
        six.len()
    );
}

/// The CAPACITY CEILING each proposed narrowing would impose, measured on a fixture built to
/// push on it rather than on the one that happens to exist.
///
/// WHY A SECOND FIXTURE. The seed helper writes one small block per key, so the census it feeds
/// reports a page_id maximum of 1 and a length maximum of 712. Both clear a 16-bit and a 32-bit
/// field by four orders of magnitude, and both numbers are properties of THAT fixture rather
/// than of the type. Narrowing a field on the strength of them would be the empty-denominator
/// mistake with extra steps: the measurement cannot fail, so it cannot authorise anything.
///
/// This fixture pushes on each ceiling separately:
///   * many HASH FIELDS under ONE key, which is one object with many components, for the
///     blocks-per-object ceiling,
///   * a LARGE value, for the block-length ceiling,
///   * many distinct keys, for the objects-per-bucket ceiling.
///
/// NON-VACUITY: each arm asserts it actually moved its own maximum off the floor the default
/// fixture sits at, BEFORE any ceiling is reported. An arm that stopped writing would otherwise
/// report a comfortable maximum that only means nothing was written.
#[test]
#[ignore = "seeds a wide fixture; run by name"]
fn the_capacity_ceilings_each_narrowing_would_impose() {
    const FIELDS_PER_OBJECT: usize = 4_000;
    const BIG_VALUE: usize = 1 << 20;
    const DISTINCT_KEYS: usize = 2_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = new_engine(dir.path());

    for chunk_start in (0..FIELDS_PER_OBJECT).step_by(500) {
        let commands = (chunk_start..(chunk_start + 500).min(FIELDS_PER_OBJECT))
            .map(|i| Command::HashSet {
                key: "wide_object".to_string(),
                field: format!("component{i}"),
                value: vec![104u8; 16],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "hash seed must ack: {:?}", response.status);
    }

    // INCOMPRESSIBLE, and that is the whole point of this arm.
    //
    // An earlier draft wrote vec![66u8; 1 MiB] -- a run of one byte -- and the index recorded a
    // length of 66. The address does not hold the VALUE length, it holds the framed RECORD
    // length, and the record is compressed: a megabyte of one repeated byte is 66 bytes on disk.
    // Measuring the length ceiling against that would have reported 56-million-fold headroom for
    // a field whose real maximum is set by incompressible payloads. A cheap LCG defeats the
    // compressor without pulling in a dependency.
    let mut seed_state = 0x2545_F491_4F6C_DD1Du64;
    let mut big = Vec::with_capacity(BIG_VALUE);
    for _ in 0..BIG_VALUE {
        seed_state = seed_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        big.push((seed_state >> 33) as u8);
    }
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "big_value".to_string(),
            value: big,
        },
    });
    assert!(response.status.ok, "big value must ack: {:?}", response.status);

    // An EMPTY value. This is the whole of the tombstone question: if a live, undeleted block can
    // carry length 0, then a length-0 tombstone would read a live block as deleted.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "empty_value".to_string(),
            value: Vec::new(),
        },
    });
    assert!(response.status.ok, "empty value must ack: {:?}", response.status);

    for chunk_start in (0..DISTINCT_KEYS).step_by(500) {
        let commands = (chunk_start..(chunk_start + 500).min(DISTINCT_KEYS))
            .map(|i| Command::StringSet {
                key: format!("k{i}"),
                value: vec![118u8; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "key seed must ack: {:?}", response.status);
    }

    // The bucket index and the model maps hold DIFFERENT lengths for the same write, and only one
    // of them is the payload. Walk both: the census covers every model map, and the loop below
    // covers the bucket index. Reading only the bucket index reported a 1 MiB write as 66 bytes.
    let wide = census(&engine, 1);
    println!("--- wide fixture, model-map census ---");
    wide.report("wide fixture");

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    let empty_in_model_map = shard.strings.get("empty_value").map(|a| a.length());
    let big_in_model_map = shard.strings.get("big_value").map(|a| a.length());
    println!(
        "  MODEL MAP lengths -- empty_value: {empty_in_model_map:?}   big_value: {big_in_model_map:?}"
    );

    let mut max_objects_in_a_bucket = 0usize;
    let mut max_blocks_in_a_bucket = 0usize;
    let mut blocks_per_object: BTreeMap<u64, usize> = BTreeMap::new();
    let mut model_ids: std::collections::BTreeSet<String> = Default::default();
    let mut max_length = 0u64;
    let mut max_page_id = 0u64;
    let mut live_zero_length: Vec<(String, String)> = Vec::new();
    let mut deleted_zero_length = 0usize;
    let mut live_blocks = 0usize;
    let mut named: BTreeMap<String, (u64, bool)> = BTreeMap::new();
    let mut longest: Vec<(u64, String)> = Vec::new();

    for bucket in shard.bucket_index.bucket_map.values() {
        max_objects_in_a_bucket = max_objects_in_a_bucket.max(bucket.object_index.object_count());
        let mut blocks_here = 0usize;
        for (_, page) in bucket.block_index.iter() {
            blocks_here += 1;
            model_ids.insert(page.model_id.to_string());
            *blocks_per_object.entry(page.object_id(1)).or_default() += 1;
            max_length = max_length.max(page.address.length());
            max_page_id = max_page_id.max(page.address.block_id().unwrap_or(0));
            longest.push((page.address.length(), page.object_key.to_string()));
            if page.object_key.as_ref() == "big_value" || page.object_key.as_ref() == "empty_value"
            {
                named.insert(
                    page.object_key.to_string(),
                    (page.address.length(), page.deleted),
                );
            }
            if !page.deleted {
                live_blocks += 1;
                if page.address.length() == 0 {
                    live_zero_length
                        .push((page.model_id.to_string(), page.object_key.to_string()));
                }
            } else if page.address.length() == 0 {
                deleted_zero_length += 1;
            }
        }
        max_blocks_in_a_bucket = max_blocks_in_a_bucket.max(blocks_here);
    }
    let max_blocks_in_an_object = blocks_per_object.values().copied().max().unwrap_or(0);

    // WHAT A NARROWING ACTUALLY BUYS, which is not what its field width suggests.
    //
    // BlockAddress is 8-byte aligned because it contains u64s, so its size is its payload rounded
    // UP to a multiple of 8. That payload was 6*8 + 4 + 1 = 53, rounded to 56, with three bytes
    // of padding already being paid for -- so narrowing ONE 8-byte field to 4 took the payload to
    // 49, still rounded to 56, and would have bought NOTHING. The struct only got smaller once a
    // change removed at least six bytes of payload, and that is what `length` and `block_id`
    // moving to 32 bits together did: 4*8 + 3*4 + 1 = 45, rounded to 48. The per-field question
    // was never "can this be narrower" but "does this cross an alignment step".
    //
    // 48 -> 40 is the same lesson a third time, and the cleanest instance of it: no field was
    // narrowed at all. `generation` was REMOVED -- derived from `block_id.or(object_id)`, which it
    // equalled on every address this census has ever walked -- taking a whole eight-byte step off
    // the payload in one go: 3*8 + 3*4 + 1 = 37, rounded to 40. Three bytes of padding again,
    // because the step size did not change.
    //
    // 40 -> 32 is the FOURTH, and it is why the payload below is DERIVED rather than written down.
    // The slab id and the offset merged into one word, taking the payload from 37 to 29 -- and the
    // literal 37 stayed here, so this line computed `32 - 37` and the whole probe aborted on an
    // unsigned underflow before printing a single ceiling. It is `#[ignore]`d, so no gate ever ran
    // it and nothing said so. A payload figure beside a struct that moves is a hand-written
    // subject list: it goes stale and nothing fails.
    //
    // 32 -> 24 IS THE FIFTH, and it went stale the same way -- this constant read 29 against a
    // 24-byte struct, and the assertion below is what said so rather than an underflow. It is also
    // the clearest instance of the lesson: `routing_bucket` LEFT (payload 25, still rounding to 32,
    // worth zero) and `block_id` narrowed to sixteen bits (payload 27, still rounding to 32, worth
    // zero), and only the two TOGETHER reach 23 and cross. Neither field could have been read as
    // worth anything on its own.
    println!("--- what the struct actually costs ---");
    // Derived from the field widths, in declaration order, so the next step cannot leave it stale:
    // one merged address word, the object id, a 32-bit length, a 16-bit block id and the presence
    // byte.
    const ADDRESS_PAYLOAD_BYTES: usize = 8 + 8 + 4 + 2 + 1;
    assert!(
        std::mem::size_of::<BlockAddress>() >= ADDRESS_PAYLOAD_BYTES,
        "the derived payload {ADDRESS_PAYLOAD_BYTES} exceeds size_of BlockAddress {}, so a field \
         has changed width and the derivation above has to change with it",
        std::mem::size_of::<BlockAddress>()
    );
    println!(
        "  size_of BlockAddress = {} (payload 2*8 + 4 + 2 + 1 = {ADDRESS_PAYLOAD_BYTES}, so {} \
         bytes are padding)",
        std::mem::size_of::<BlockAddress>(),
        std::mem::size_of::<BlockAddress>() - ADDRESS_PAYLOAD_BYTES
    );
    println!(
        "  align_of BlockAddress = {}",
        std::mem::align_of::<BlockAddress>()
    );
    println!(
        "  size_of BlockIndex = {} (it holds a BlockAddress, an Arc<str>, an Option<Arc<str>>, a \
         one-byte model spelling and 3 bools)",
        std::mem::size_of::<BlockIndex>()
    );
    println!(
        "  size_of Arc<str> = {}, size_of Option<Arc<str>> = {}, size_of bool = {}",
        std::mem::size_of::<Arc<str>>(),
        std::mem::size_of::<Option<Arc<str>>>(),
        std::mem::size_of::<bool>()
    );

    println!("--- capacity ceilings, wide fixture ---");
    println!("  live blocks walked: {live_blocks}");
    println!(
        "  max objects in one bucket: {max_objects_in_a_bucket}  (8-bit object_id ceiling 255, headroom {:.1}x)",
        255.0 / max_objects_in_a_bucket.max(1) as f64
    );
    println!("  max blocks in one bucket: {max_blocks_in_a_bucket}");
    println!(
        "  max blocks in one object: {max_blocks_in_an_object}  (16-bit page_id ceiling 65535, headroom {:.1}x)",
        65535.0 / max_blocks_in_an_object.max(1) as f64
    );
    println!(
        "  max page_id observed: {max_page_id}  (16-bit ceiling 65535, headroom {:.1}x)",
        65535.0 / max_page_id.max(1) as f64
    );
    println!(
        "  max block length: {max_length} bytes  (32-bit ceiling 4294967295, headroom {:.1}x)",
        4294967295.0 / max_length.max(1) as f64
    );
    longest.sort_by(|a, b| b.0.cmp(&a.0));
    println!("  five longest blocks (length, key):");
    for (len, key) in longest.iter().take(5) {
        println!("    {len} bytes  key={key}");
    }
    println!(
        "  the 1 MiB write landed as: {:?}   the empty write landed as: {:?}   (length, deleted)",
        named.get("big_value"),
        named.get("empty_value")
    );
    println!(
        "  distinct model_id values in the tree: {} -> {:?}",
        model_ids.len(),
        model_ids
    );
    println!(
        "  LIVE blocks carrying length 0: {} (deleted blocks carrying length 0: {deleted_zero_length})",
        live_zero_length.len()
    );
    for (model, key) in live_zero_length.iter().take(5) {
        println!("    live zero-length block: model={model} key={key}");
    }
    println!(
        "  VERDICT on the length-0 tombstone: {}",
        if live_zero_length.is_empty() {
            "no live block carries length 0 in this fixture"
        } else {
            "A LIVE BLOCK CARRIES LENGTH 0, so the length-0 tombstone encoding is UNAVAILABLE"
        }
    );

    // NON-VACUITY, asserted AFTER the report so a guard can never hide the numbers that explain
    // why it fired. An earlier draft asserted first, and the blocks-per-object arm aborted the
    // whole probe before a single maximum was printed -- which is the same failure as an empty
    // sweep: the run says something is wrong and nothing about what.
    assert!(
        live_blocks > DISTINCT_KEYS,
        "fixture must produce more live blocks ({live_blocks}) than the {DISTINCT_KEYS} plain keys"
    );
    // The large-value arm is reported, not asserted on its MAXIMUM, because what it revealed is
    // that a value does not become a block of its own size: the entry below names the length the
    // index actually holds for a 1 MiB write. Asserting a maximum here would have turned a fact
    // about where big values live into a red test.
    assert!(
        named.contains_key("big_value") && named.contains_key("empty_value"),
        "both probe keys must reach the bucket index, saw {:?}",
        named.keys().collect::<Vec<_>>()
    );
    assert!(
        max_objects_in_a_bucket >= 1 && !model_ids.is_empty(),
        "the bucket walk must see objects and models, saw {max_objects_in_a_bucket} and {}",
        model_ids.len()
    );

    // BLOCKS PER OBJECT IS THE ANSWER, AND THE ANSWER CHANGED.
    //
    // This arm writes 4,000 hash fields under ONE key. It used to report 4,000 objects of one
    // block each, because the object identity folded the component in and a field was therefore
    // its own OBJECT rather than another block inside one. Since the component left
    // `stable_block_object_id`, one key is one object and those fields are blocks of it.
    //
    // Reported and not asserted, deliberately: an earlier draft asserted first and aborted the
    // whole probe before a single maximum was printed, which is the same failure as an empty
    // sweep -- the run says something is wrong and nothing about what.
    println!(
        "  NOTE: blocks-per-object is {max_blocks_in_an_object} over {FIELDS_PER_OBJECT} fields \
         of one key; the component is no longer part of the object id, so a field is another \
         block of one object rather than an object of its own"
    );
}

// =============================================================================================
// THE BUCKET IS AN ARGUMENT NOW: what that costs, and what it stops being able to go wrong
// =============================================================================================

/// THE WARM BLOCK IS FOUND UNDER THE KEY THE OTHER SIDE WROTE.
///
/// `CacheKey::page_with_slot` is built by SEVERAL paths and read by one, and its slot is the block's
/// routing bucket. While the bucket was a field of the `BlockAddress` every one of those paths read
/// it off the same struct and could not disagree. It is an ARGUMENT now, so they agree only because
/// each names it the same way -- `block_routing_bucket(object_key, start, end)` over the range the
/// shard is stamped with.
///
/// A DISAGREEMENT HERE IS SILENT. The read would miss, go to the block store, answer correctly, and
/// put the block back under its own key: every test still green, every read paying an I/O it should
/// not, and the cache holding two copies of every block. So the agreement is DRIVEN rather than
/// reasoned about.
///
/// TWO ARMS, because they are two different pairs of paths:
///
///   * WITHIN ONE PROCESS -- a read populates the cache and the next read must find it. This fails if
///     the read path's own key is not a function of the shard state alone.
///   * ACROSS A RESTART -- the LOAD path's `reconcile_secondary_views_from_bucket_index` warms the
///     cache from the index, and a read afterwards must find what IT wrote. That is a genuine
///     cross-path agreement: two functions, two argument lists, one key.
///
/// THE SECOND ARM USES A FEATURE SERIES, AND IT HAS TO. The reconcile walk warms the cache only for
/// the kinds it rebuilds a secondary VIEW for -- features, control state and the context series --
/// and `insert_timestamped_secondary_view` is one of the sites this change had to thread the bucket
/// through. A plain string block is not warmed by that walk at all: a cold read of one goes to the
/// block store on this tree today (`part4`'s tier probe measured one store read per warm read and
/// says so in as many words), so an arm written over strings would assert a property the engine does
/// not have and fail for a reason that has nothing to do with the key.
///
/// WITH THE WRONG BUCKET AS THE CONTROL. A test that only asserted "the read hits" would pass on a
/// cache that hit for any key at all, so the same block is also looked up under a DIFFERENT bucket and
/// that lookup must MISS.
///
/// rust-internal: reads the engine's own cache keys, no product behaviour
#[test]
fn the_warm_page_is_found_under_the_key_the_other_side_wrote() {
    let dir = tempfile::tempdir().expect("tempdir");
    const KEYS: usize = 16;
    let keys: Vec<String> = (0..KEYS).map(|index| format!("bucket-arg-{index:03}")).collect();

    {
        let engine = new_engine(dir.path());
        for key in &keys {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.clone(),
                    value: b"a page read through its bucket".to_vec(),
                },
            });
            assert!(response.status.ok, "write {key}: {:?}", response.status);
        }

        // ARM ONE: the read path against itself. The first read populates; the second must find it.
        for key in &keys {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: key.clone() },
            });
            assert!(response.status.ok, "first read {key}: {:?}", response.status);
        }
        let before = engine.block_store().stats().reads;
        for key in &keys {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: key.clone() },
            });
            assert!(response.status.ok, "second read {key}: {:?}", response.status);
        }
        let warm_reads = engine.block_store().stats().reads - before;
        println!("  within one process: {warm_reads} block-store read(s) for {KEYS} warm reads");
        assert_eq!(
            0, warm_reads,
            "{warm_reads} of {KEYS} warm reads went to the block store. The read path built a key \
             the read path had just written, so a miss here means the key is not a function of the \
             shard state alone"
        );

        // AND THE ENGINE'S OWN ACCESSOR NAMES THE SAME BLOCK, which is what every other test in this
        // tree reaches for when it wants "the key this block is cached under".
        let (address, start, end) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let (start, end) = shard.routing_range();
            (shard.strings.get(keys[0].as_str()).expect("indexed").clone(), start, end)
        };
        let derived = crate::engine::hashing::block_routing_bucket(&keys[0], start, end);
        let read_key = matrixcache::CacheKey::page_with_slot(
            1,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            Some(derived),
        );
        assert_eq!(
            engine
                .string_block_cache_key_for_test(1, &keys[0])
                .expect("the page is indexed"),
            read_key,
            "the engine's own page-cache-key accessor and the key the read path builds disagree"
        );
        assert!(
            engine.cache().peek_tier(&read_key).is_some(),
            "nothing is cached under the key both sides built, so the equality above compares two \
             keys neither of which names anything"
        );

        // THE CONTROL: the same block under a DIFFERENT bucket must not be found.
        let wrong = matrixcache::CacheKey::page_with_slot(
            1,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            Some(derived.wrapping_add(1)),
        );
        assert_ne!(read_key, wrong, "the slot must be part of the key");
        assert!(
            engine.cache().peek_tier(&wrong).is_none(),
            "the cache answered for a bucket nothing wrote, so the slot is not part of the key and \
             this test cannot see a disagreement"
        );

        engine.flush_shard_index(1);
    }

    // ARM TWO: ACROSS A RESTART, with a FRESH cache directory so nothing survives except what the
    // load path itself writes. The reconcile walk warms the cache from the index through
    // `insert_timestamped_secondary_view`; a read afterwards has to find what that walk wrote, and
    // the two build their keys in different functions with different argument lists.
    const POINTS: u64 = 8;
    let series: Vec<String> = (0..KEYS).map(|index| format!("bucket-arg-series-{index:03}")).collect();
    {
        let engine = new_engine(dir.path());
        for key in &series {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: key.clone(),
                    points: (0..POINTS)
                        .map(|point| crate::types::FeaturePoint {
                            timestamp_ms: 1_000 + point,
                            value: format!("{point}").into_bytes(),
                        })
                        .collect(),
                },
            });
            assert!(response.status.ok, "feature write {key}: {:?}", response.status);
        }
        engine.flush_shard_index(1);
    }

    let reopened = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-b"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    ));
    reopened.load_shard(1);

    let before = reopened.block_store().stats().reads;
    let mut served = 0usize;
    for key in &series {
        let response = reopened.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: key.clone(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        });
        if let crate::types::CommandResponse::FeaturePoints { points } = &response.response {
            if !points.is_empty() {
                served += 1;
            }
        }
    }
    let cold_reads = reopened.block_store().stats().reads - before;
    println!(
        "  across a restart: {served} of {KEYS} series served, {cold_reads} block-store read(s)"
    );
    assert_eq!(
        KEYS, served,
        "{served} of {KEYS} series came back after the restart; a key disagreement would not lose a \
         record, so this is a different defect and the count below would be measuring it"
    );
    assert_eq!(
        0, cold_reads,
        "{cold_reads} block-store read(s) for {KEYS} series read after the restart. The LOAD path's \
         reconcile walk warmed the cache from the index under the key IT built and the read path \
         built its own; both derive the bucket from the object key over the shard's stamped range, so \
         a read that misses means the two derivations disagree -- silently, because the read still \
         answers."
    );
}

/// WHAT DERIVING THE BUCKET COSTS ON A READ, COUNTED, AGAINST THE ALTERNATIVE ITEM 2 PROPOSED.
///
/// Two numbers, one instrument each, over the SAME workload:
///
///   * `ROUTING_BUCKET_KEY_BYTES` -- the bytes FNV-1a walks to derive a block's bucket. The hash is
///     one xor and one multiply per byte, so the byte total is the work up to a constant.
///   * `OBJECT_INDEX_ENTRIES_EXAMINED` -- the entries a membership question in a bucket's own object
///     list touches. That list is `ObjectIndex`: `One` inline, `Many` a sorted `Vec` bisected, so the
///     count is logarithmic in the objects the bucket holds.
///
/// WHY BOTH, IN ONE TEST. Item 2 of this change proposed replacing the 64-bit `object_id` hash with a
/// per-bucket ORDINAL, on the grounds that `BucketNode::object_index` is already a key-to-ordinal
/// map. It is not -- it is a sorted set of the HASHES, with no key in it -- but the cost comparison it
/// asked for is still the right question to answer, and this is the answer: what a lookup in that
/// list costs against what the hash costs.
///
/// rust-internal: reads the engine's own counters, no product behaviour
#[test]
#[ignore = "seeds a shard and reads process-wide counters; run by name"]
fn what_consulting_the_object_index_costs_against_computing_the_hash() {
    for (label, end_routing_bucket) in [("whole keyspace", u32::MAX), ("the operator's 1023", 1023)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_without_a_shard(dir.path());
        {
            let response = engine.load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "object-index-cost".to_string(),
                shard_uri: "local://object-index-cost/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            });
            assert!(response.status.ok, "{label}: {:?}", response.status);
        }

        const RECORDS: usize = 2_000;
        for index in 0..RECORDS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cost-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
            assert!(response.status.ok, "{label} write {index}: {:?}", response.status);
        }

        // THE READS, COUNTED FROM ZERO. Both counters are process-wide, so they are reset here and
        // this test is `#[ignore]`d for the same reason every process-wide probe in this tree is.
        crate::engine::hashing::reset_routing_bucket_derivations();
        crate::engine::state::reset_object_index_entries_examined();
        let mut read = 0usize;
        for index in 0..RECORDS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("cost-{index:06}"),
                },
            });
            if response.status.ok {
                read += 1;
            }
        }
        let (derivations, key_bytes) = crate::engine::hashing::routing_bucket_derivations();
        let entries = crate::engine::state::object_index_entries_examined();

        assert_eq!(read, RECORDS, "{label}: {read} of {RECORDS} reads answered");
        assert!(
            derivations > 0,
            "{label}: the read path derived no bucket at all, so neither number below is about it"
        );

        // The objects a bucket actually holds, which is what the alternative's cost is a function of.
        let (max_objects, buckets) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let max = shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.object_index.object_count())
                .max()
                .unwrap_or(0);
            (max, shard.bucket_index.bucket_map.len())
        };

        println!("--- {label}: what a read pays for its bucket ---");
        println!("  reads: {read}, buckets: {buckets}, max objects in one bucket: {max_objects}");
        println!(
            "  bucket derivations: {derivations} ({:.2} a read), key bytes hashed: {key_bytes} \
             ({:.2} a read)",
            derivations as f64 / read as f64,
            key_bytes as f64 / read as f64,
        );
        println!(
            "  object-index entries examined: {entries} ({:.2} a read); a bisection of the widest \
             bucket would touch {}",
            entries as f64 / read as f64,
            crate::engine::state::entries_a_bisection_examines(max_objects),
        );

        // THE FLOOR THAT MAKES THE COMPARISON MEAN SOMETHING. A key of this fixture is 11 bytes, so
        // a derivation walks 11 bytes; a bisection of a bucket holding `max_objects` touches
        // `log2 + 1` entries. Both are stated as counts rather than as a verdict, because which is
        // cheaper depends on what an entry costs against a byte -- and an entry here is a cache-line
        // probe into a heap vector while a byte is a register operation.
        assert!(
            key_bytes >= derivations * 8,
            "{label}: {key_bytes} bytes over {derivations} derivations is under 8 bytes a key, which \
             is shorter than any key this fixture writes"
        );

        // AND THE OTHER SIDE OF THE COMPARISON HAS TO BE A REAL NUMBER. A bisection cost that read
        // zero would make the alternative look free, which is the direction that flatters the
        // proposal -- so the widest bucket's cost is asserted non-zero over a non-empty bucket.
        assert!(
            max_objects > 0,
            "{label}: no bucket holds an object, so the bisection cost below is over an empty list"
        );
        assert!(
            crate::engine::state::entries_a_bisection_examines(max_objects) > 0,
            "{label}: a bisection of a bucket holding {max_objects} objects was priced at zero \
             entries, which would make a lookup look free against the hash"
        );

        // THE READ PATH CONSULTS THE OBJECT INDEX ZERO TIMES TODAY, which is the finding that
        // decides the direction: replacing the hash with a lookup is not a substitution, it is NEW
        // work on a path that does not touch that list at all.
        assert_eq!(
            0, entries,
            "{label}: the read path examined {entries} object-index entries. It examines none -- an \
             object's identity is computed from its key, never looked up -- and if that ever changes \
             the comparison this test makes is between two things the read path both does."
        );
    }
}

/// HOW MANY OBJECTS A BUCKET HOLDS, AS PERCENTILES AND A MAX, AT BOTH RANGES.
///
/// THE NUMBER ITEM 2 TURNS ON, and the reason it is measured rather than assumed. #1973 measured
/// BLOCKS per bucket at the operator's range (p50 39, MAX 50 at 40,000 records); OBJECTS per bucket is
/// a different distribution. It USED to be the larger of the two, because the object identity
/// folded the COMPONENT in -- a hash field was its own object rather than another block of one --
/// so one key with many components contributed many objects to one bucket. The component has
/// since left the identity, so a key contributes exactly ONE object however many elements it
/// holds, and this distribution collapses onto the KEY count. That makes an eight-bit ordinal
/// considerably more viable than the figures below used to say, which is a consequence worth
/// reading off this histogram rather than re-deriving later.
///
/// WHY IT DECIDES ANYTHING. A per-bucket ordinal has to fit in a field, and the field width is the
/// whole proposal: `BlockAddress` is 24 bytes with a 15-byte payload beside `object_id`, so an
/// ordinal at EIGHT bits would take the struct to 16 and one at sixteen bits would leave it at 24.
/// Eight bits is 255 objects to a bucket. So this histogram is not a curiosity -- it is the question
/// of whether the proposal is worth anything at all.
///
/// NEVER A MEAN: percentiles, MAX and the denominator, for the reason #1959 established.
///
/// rust-internal: reads the engine's own index, no product behaviour
#[test]
#[ignore = "seeds two stores of 40,000 records; run by name"]
fn how_many_objects_a_bucket_holds_as_percentiles_and_max() {
    const RECORDS: usize = 40_000;
    const COMPONENTS: usize = 8;

    for (label, end_routing_bucket) in [("the operator's 1023", 1023u32), ("whole keyspace", u32::MAX)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_without_a_shard(dir.path());
        let response = engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1,
            table_name: "objects-per-bucket".to_string(),
            shard_uri: "local://objects-per-bucket/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        });
        assert!(response.status.ok, "{label}: {:?}", response.status);

        // MANY COMPONENTS UNDER ONE KEY, which is what puts many OBJECTS in one bucket: routing takes
        // the object key and never the component, while the object id folds the component in.
        let keys = RECORDS / COMPONENTS;
        for key_index in 0..keys {
            let commands = (0..COMPONENTS)
                .map(|component| Command::HashSet {
                    key: format!("obj-{key_index:06}"),
                    field: format!("f{component}"),
                    value: vec![b'h'; 32],
                })
                .collect::<Vec<_>>();
            let response = engine.batch_execute(crate::types::BatchExecuteRequest {
                shard_id: 1,
                commands,
            });
            assert!(response.status.ok, "{label}: seed: {:?}", response.status);
        }

        let mut per_bucket: Vec<usize> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.object_index.object_count())
                .filter(|count| *count > 0)
                .collect()
        };
        per_bucket.sort_unstable();

        // DENOMINATOR FIRST.
        assert!(
            !per_bucket.is_empty(),
            "{label}: no bucket holds an object, so every percentile below is over an empty set"
        );
        let total_objects: usize = per_bucket.iter().sum();
        // ONE OBJECT PER KEY, NOT PER COMPONENT -- WHICH IS WHAT THIS TEST USED TO ASSERT.
        //
        // It required `total_objects >= RECORDS`: one object per written COMPONENT, because the
        // identity folded the component in. Since it no longer does, a key's components are
        // elements of ONE object and the count is the key count. The assertion is inverted to
        // the exact number rather than relaxed to an inequality that would pass either way.
        assert_eq!(
            total_objects, keys,
            "{label}: {total_objects} objects for {keys} keys ({RECORDS} written components). \
             One key is one object now, so these must be equal -- {RECORDS} would mean the \
             component is still in the identity"
        );
        assert!(
            RECORDS > keys,
            "DENOMINATOR: {RECORDS} components over {keys} keys. A fixture writing one component \
             per key cannot tell the two identities apart at all"
        );

        let at = |q: f64| -> usize {
            let index = ((per_bucket.len() as f64 - 1.0) * q).round() as usize;
            per_bucket[index]
        };
        let max = *per_bucket.last().expect("non-empty");
        println!("--- {label}: objects per occupied bucket, {RECORDS} components ---");
        println!(
            "  occupied buckets {}, objects {total_objects}, p50 {}, p90 {}, p99 {}, MAX {max}",
            per_bucket.len(),
            at(0.50),
            at(0.90),
            at(0.99),
        );
        println!(
            "  an 8-bit ordinal holds 255: {} of {} occupied buckets are already over it",
            per_bucket.iter().filter(|count| **count > 255).count(),
            per_bucket.len()
        );
        println!(
            "  a 16-bit ordinal holds 65535: {} of {} occupied buckets are over it",
            per_bucket.iter().filter(|count| **count > 65_535).count(),
            per_bucket.len()
        );

        // THE VERDICT, STATED AS A CEILING AND NOT AS A WIDTH.
        //
        // An 8-bit ordinal FITS AT THIS CORPUS -- 0 of 1,022 occupied buckets are over 255 -- so the
        // 16-byte form of `BlockAddress` is arithmetically reachable. What it would cost is not a
        // width but a HARD CAPACITY LIMIT: objects per bucket is `records x objects-per-record /
        // buckets`, which contains the RECORD COUNT, so a fixed bucket count crosses 255 at a
        // computable store size. That number is what decides the proposal, and it is computed here
        // from the measured fill rather than asserted as a comfort.
        if end_routing_bucket == 1023 {
            assert!(
                max <= 255,
                "{label}: the widest bucket already holds {max} objects, over the 255 an 8-bit \
                 ordinal holds, so the 16-byte form is not reachable even at this corpus"
            );
            let buckets = 1024u64;
            let objects_per_record = total_objects as f64 / RECORDS as f64;
            // The records at which the WIDEST bucket would reach 255, scaled from the measured max:
            // the fill is linear in the record count at a fixed bucket count.
            let records_at_the_ceiling = (RECORDS as f64) * 255.0 / max as f64;
            println!(
                "  {objects_per_record:.2} objects a record over {buckets} buckets: the widest \
                 bucket reaches 255 objects at about {records_at_the_ceiling:.0} records"
            );
            // AND THE CEILING HAS MOVED OUT OF REACH, WHICH IS THE ANSWER CHANGING.
            //
            // This asserted the ceiling was UNDER a million records, and said exactly what a
            // larger number would mean: "it is headroom rather than a limit, and the argument
            // against the 16-byte form has to be made on something else". The component leaving
            // the object identity is that something else arriving -- a key is one object now, so
            // objects per bucket fell from one per written component to one per KEY, and the
            // ceiling moved out by the average element count.
            //
            // So the assertion is inverted to the claim the measurement now supports, with the
            // same 1,000,000 boundary rather than a new one chosen to fit.
            assert!(
                records_at_the_ceiling >= 1_000_000.0,
                "the widest bucket would reach 255 objects at {records_at_the_ceiling:.0} \
                 records. Since a key is one object this is supposed to be far out of reach; a \
                 ceiling still inside a million records would mean objects per bucket did not \
                 fall and the identity did not collapse"
            );
            println!(
                "  VERDICT: an 8-bit ordinal reaches 16 bytes and its ceiling is now about \
                 {records_at_the_ceiling:.0} records a shard -- headroom rather than a limit, \
                 because a key is ONE object. A 16-bit ordinal is still worth NOTHING, because \
                 8 + 2 + 4 + 2 + 1 = 17 rounds back to 24"
            );
        }
    }
}

/// TWO BUCKETS HOLDING ONE OBJECT ID ARE REPORTED AS ONE OBJECT, IN THE BUCKET SEEN FIRST.
///
/// FOUND WHILE ASKING WHETHER A PER-BUCKET ORDINAL COULD BE READ BACK, and it is a defect in its own
/// right whether or not anything is ever narrowed. `object_manager::runtime_report` walks every
/// bucket of a shard into ONE `BTreeMap<u64, ObjectRuntimeState>` keyed by object id, and records
/// `routing_bucket` with `or_insert_with` -- so the second bucket's blocks are folded into the first
/// bucket's entry and the report names a bucket that holds only some of them.
///
/// `reused_object_ids` does not catch it: that counts ids with more than one BLOCK REF, which is the
/// ordinary multi-block object. There is no term in the report for the same id in two buckets.
///
/// NOT `#[allow(dead_code)]` EITHER, though it is marked so: `storage_reporting.rs` calls it, which
/// `native_persistence_workflow` reaches through `object_manager_runtime_report`.
///
/// rust-internal: reads the engine's own report, no product behaviour
#[test]
fn two_buckets_holding_one_object_id_are_reported_as_one_object() {
    use crate::engine::state::{BlockIndex, BucketNode, ShardState};

    let mut shard = ShardState::default();
    // THE SHARD IS STAMPED, because the id below is DERIVED from it. `runtime_report` returns an
    // empty report for a state that carries no shard id rather than deriving on a guessed zero, so an
    // unstamped fixture would report nothing and this test would fold zero objects into zero.
    shard.set_shard_id(1);
    // AND THE ID IS DERIVED, NOT PLANTED. It used to be an arbitrary literal written onto each
    // address; an address carries no object id now, so there is nothing to plant and the id is what
    // the terms produce. That makes the fold below happen for the REAL reason -- both entries carry
    // the same object key, so they derive the same id -- rather than because a literal was copied
    // into two places.
    let object_id = crate::engine::hashing::stable_block_object_id(1, "string", "one-key");

    // The same object id filed in two different buckets. Reachable without any tampering: the
    // routing range is the MODULUS, so a store re-ranged between writes files one key's blocks under
    // two buckets, and `bucket_map` holds both.
    for (routing_bucket, offset) in [(11u32, 0u64), (2_222u32, 4_096u64)] {
        let mut bucket = BucketNode {
            routing_bucket,
            ..BucketNode::default()
        };
        bucket.object_index.insert(object_id);
        bucket.block_index.insert(
            BlockIndex {
                kind: crate::index_log::IndexItemKind::Page,
                routing_bucket: routing_bucket,
                object_key: std::sync::Arc::from("one-key"),
                model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
                component: None,
                address: BlockAddress::from_parts(1, offset, 64, Some(0), None),
                dirty: false,
                deleted: false,
            },
            &mut Default::default(),
        );
        shard.bucket_index.bucket_map.insert(routing_bucket, bucket);
    }

    // DENOMINATOR: the fixture really does hold the id twice, in two buckets.
    let holders: Vec<u32> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| bucket.object_index.contains(&object_id))
        .map(|(routing_bucket, _)| *routing_bucket)
        .collect();
    assert_eq!(
        holders,
        vec![11, 2_222],
        "the fixture must file one object id in two buckets, or the report below has nothing to fold"
    );

    let report = crate::engine::object_manager::runtime_report(&shard);
    println!(
        "  buckets holding the id: {holders:?}; report says {} object(s), {} block ref(s), \
         reused_object_ids {}",
        report.live_object_count, report.live_block_ref_count, report.reused_object_ids
    );
    for object in &report.objects {
        println!(
            "    object {} -> routing_bucket {}, block_refs {}",
            object.object_id, object.routing_bucket, object.block_ref_count
        );
    }

    assert_eq!(
        1, report.live_object_count,
        "the report folds the two buckets' entries into ONE object, which is the finding: a \
         per-bucket ordinal cannot be read back out of a report keyed on the ordinal alone"
    );
    assert_eq!(
        2, report.live_block_ref_count,
        "both pages must be counted, or the fold is not what this test claims"
    );
    let state = report.objects.first().expect("one object");
    assert_eq!(
        11, state.routing_bucket,
        "the report records the bucket it saw FIRST -- the lowest bucket id, `bucket_map` being \
         ordered -- so the object it describes is reported in a bucket holding half its pages"
    );
    assert_eq!(
        2, state.block_ref_count,
        "and it carries both buckets' refs under that one bucket"
    );

    // AND THE REPORT HAS NO TERM THAT NOTICES. `reused_object_ids` counts an id with more than one
    // BLOCK REF, which every multi-block object has, so it cannot be the detector for this.
    assert_eq!(
        1, report.reused_object_ids,
        "`reused_object_ids` counts ids with more than one block ref. It reads 1 here -- and it \
         would read 1 for one object with two pages in ONE bucket too, which is why it is not a \
         detector for the same id in two buckets."
    );
}

// =================================================================================================
// IS THE OBJECT ID A CACHE OF A PURE FUNCTION OF FIELDS THE ENTRY ALREADY HOLDS?
//
// The header above says the only two shapes that reach 16 bytes are `object_id` at ONE byte or
// `object_id` GONE, because the payload without it is 15. An eight-bit object identity is not
// available -- nothing bounds the objects in a routing bucket at 256 -- so the question is whether
// the field can LEAVE, which is the question of whether anything reads a value it could not
// recompute.
//
// `BlockIndex` holds `object_key`, `model_id` and `component`, and `ShardState` holds the shard, so
// `stable_block_object_id(shard, kind, key)` is computable at every block entry. #1974
// established that derivation at about thirty sites, `index_log` already STRIPS a derivable id from
// the row it writes (`a_row_does_not_write_the_object_id_it_can_derive`), and the entry's own
// `object_id()` doc says the write path puts the computed id into the address. So the claim is that
// the in-memory field is the last copy of a value three other layers already derive.
//
// This census is the measurement of that claim over live block entries, at two corpus sizes. It is
// the same shape as the census that retired `generation`: the field is a copy on every live
// address, or it is not.
// =================================================================================================

/// What a walk of the bucket index found about the stored id against the derived one.
#[derive(Default, Debug)]
struct DerivationCensus {
    /// Live block entries walked. The denominator of every row below.
    entries: usize,
    /// The stored id is present and equals `stable_block_object_id(shard, kind, key)`.
    agree: usize,
    /// The stored id is present and DIFFERS from the derivation. Every one of these is a block the
    /// field could not be removed from.
    differ: usize,
    /// No stored id at all. These are already answered by the fallback the read sites carry.
    absent: usize,
    /// Entries carrying a component. The component is no longer a term of the id, but it is
    /// still a field of the entry, and a census whose entries all carry `None` would not
    /// exercise the container population this change acts on at all.
    with_component: usize,
    /// Distinct derived ids, so a fixture that gave every block the same identity cannot report
    /// agreement as a property of the derivation.
    distinct_derived: std::collections::HashSet<u64>,
}

impl DerivationCensus {
    fn report(&self, label: &str) {
        let pct = |n: usize| {
            if self.entries == 0 {
                0.0
            } else {
                100.0 * n as f64 / self.entries as f64
            }
        };
        println!(
            "[{label}] entries={} agree={} ({:.2}%) differ={} ({:.2}%) absent={} ({:.2}%) \
             with_component={} distinct_derived_ids={}",
            self.entries,
            self.agree,
            pct(self.agree),
            self.differ,
            pct(self.differ),
            self.absent,
            pct(self.absent),
            self.with_component,
            self.distinct_derived.len(),
        );
    }
}

/// THE STORED-AGAINST-DERIVED CENSUS IS RETIRED, AND ITS RETIREMENT IS WHAT IT LICENSED.
///
/// `derivation_census` walked every live page entry, derived `stable_block_object_id(shard, kind,
/// key)` and compared it against the id the ADDRESS stored, reporting agree / differ / absent. It
/// measured 100.00% agreement with 0 differing over 2,524 and 20,632 entries, and that measurement
/// is the whole reason the stored copy could go.
///
/// There is nothing left to compare. An address carries no object id, so the census has only one
/// side; a version comparing the derivation against itself would report 100.00% for ANY derivation,
/// which is the most flattering number in this file and the least informative. The helper and its
/// two callers are retired together rather than rewritten into that shape.
///
/// THE NON-CIRCULAR FORM SURVIVES, in
/// `shard_carried_identity::the_id_a_served_shard_carries_derives_the_object_id_already_stored_on_its_pages`:
/// it derives from each shard's own stamp and checks the answer against `bucket.object_index`,
/// which the WRITE PATH populated from the id it actually used. Two independent answers, and it
/// outlives the field.
#[test]
fn the_stored_against_derived_census_is_retired_because_its_other_side_is_gone() {
    // Asserted rather than narrated, so it cannot quietly stop being true: if an object id ever
    // returns to a block address, `generation()` starts answering `Some(42)` here and this fires.
    let address = crate::block_store::BlockAddress::from_parts(1, 0, 16, Some(7), Some(42));
    assert_eq!(
        address.generation(),
        Some(7),
        "the generation is the block id alone; Some(42) would mean an object id is stored again"
    );
}



/// Hash keys and fields the census seeds, so the component-bearing denominator is derived from the
/// fixture rather than restated as a literal beside the assertion that reads it.
const HASH_KEYS: usize = 64;
const HASH_FIELDS: usize = 8;

/// RETIRED WITH THE CENSUS IT READ. Its verdict -- the stored id equals the derivation on every
/// live page entry, so the field is a cache and the address can shed it -- has been acted on. The
/// address has shed it, so the measurement cannot be repeated and does not need to be.
#[test]
#[ignore = "retired: the stored side of this census no longer exists"]
fn the_object_id_on_a_live_page_entry_is_the_hash_of_fields_beside_it() {
    println!("retired: see the_stored_against_derived_census_is_retired_because_its_other_side_is_gone");
}


/// RETIRED WITH THE CENSUS IT WAS THE POSITIVE CONTROL FOR. It planted a differing stored id and
/// asserted the census reported it as DIFFERING -- the arm that stopped a census comparing a value
/// against itself from reading as success. There is no stored id to plant, so neither the census nor
/// its control has a subject.
#[test]
#[ignore = "retired: there is no stored id to plant a difference in"]
fn the_derivation_census_reports_a_differing_id_when_one_exists() {
    println!("retired: the census it controlled has no second side left");
}



// =================================================================================================
// WHAT THE RECOMPUTATION COSTS, AND THE CONTROL THAT SAYS IT IS THE SAME NUMBER
//
// The read path takes a `PageIdentity` and derives the object id from the terms rather than reading
// `address.object_id()`. Two questions follow and neither is answerable by reading the code: what
// that derivation COSTS, and whether the value it produces is the one the field held.
// =================================================================================================

/// WHAT DERIVING THE IDENTITY COSTS AT THE READ PATH, IN COUNTS.
///
/// NOT A TIMING, and this tree has the receipt: an instrument on this path read 485x idle against
/// 11x busy off identical code, and three sibling gates are usually running on this box. A count of
/// hash passes and a count of bytes walked do not move with what is building next door.
///
/// THE COMPARISON THAT MAKES THE NUMBER MEAN SOMETHING is the bucket derivation beside it. A read
/// ALREADY walks its key once, to place the block in a routing bucket for the cache key -- that cost
/// was accepted when the bucket stopped being a field. The identity walks the same key a second
/// time. So the question is not "what does one FNV-1a pass cost" in the abstract; it is whether this
/// read now does one pass or two, and the two counters answer it side by side.
///
/// THE BYTE TOTALS ARE NOT A RATIO. The bucket hash walks the KEY; the identity hash walks the kind
/// AND the key. Both are printed with their own denominator rather than divided into each other.
#[test]
#[ignore = "seeds a shard and reads process-wide counters; run by name"]
fn what_deriving_the_page_identity_costs_at_the_read_path() {
    const RECORDS: usize = 2_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = new_engine(dir.path());
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("ident-{index:06}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    // BOTH COUNTERS FROM ZERO. They are process-wide, which is why this probe is `#[ignore]`d like
    // every other process-wide probe in this module.
    crate::engine::hashing::reset_routing_bucket_derivations();
    crate::engine::hashing::reset_object_id_derivations();
    let mut read = 0usize;
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("ident-{index:06}"),
            },
        });
        if response.status.ok {
            read += 1;
        }
    }
    let (bucket_passes, bucket_bytes) = crate::engine::hashing::routing_bucket_derivations();
    let (identity_passes, identity_bytes) = crate::engine::hashing::object_id_derivations();

    // NON-VACUITY FIRST. A probe over zero answered reads reports two zeros, which reads exactly
    // like "the derivation is free" -- the direction that flatters the change.
    assert_eq!(read, RECORDS, "{read} of {RECORDS} reads answered");
    assert!(
        bucket_passes > 0,
        "the read path derived no routing bucket at all, so there is nothing to compare the \
         identity against and neither number below is about a read"
    );
    assert!(
        identity_passes > 0,
        "the read path derived no identity at all. Either these reads are not going through \
         `read_block_bytes`, or the identity is being built somewhere this counter cannot see -- \
         and in both cases the cost printed below is not the cost of the change"
    );

    println!("--- what a read pays to name its own page, over {read} reads ---");
    println!(
        "  routing-bucket hash passes: {bucket_passes} ({:.2} a read), key bytes walked: \
         {bucket_bytes} ({:.2} a read)",
        bucket_passes as f64 / read as f64,
        bucket_bytes as f64 / read as f64,
    );
    println!(
        "  identity hash passes:       {identity_passes} ({:.2} a read), kind+key bytes walked: \
         {identity_bytes} ({:.2} a read)",
        identity_passes as f64 / read as f64,
        identity_bytes as f64 / read as f64,
    );

    // THE SHAPE OF THE ANSWER, asserted rather than left to the printout: the identity is derived at
    // most once per read on this path. More than that would mean a caller building one per candidate
    // inside a loop it could have hoisted out of, which is the mistake this assertion exists to
    // catch -- and it is a mistake the printout alone would not make obvious.
    assert!(
        identity_passes <= read as u64,
        "{identity_passes} identity derivations over {read} reads is more than one a read: some \
         caller is building an identity per candidate rather than per page"
    );
    // And the bytes it walks are the terms it says it walks: a key of this fixture is 12 bytes and
    // the kind is "string", so a pass is 18 bytes. Asserted as a floor rather than an equality,
    // because a read may derive for a block this loop did not ask for.
    assert!(
        identity_bytes >= identity_passes * 8,
        "{identity_bytes} bytes over {identity_passes} passes is under 8 bytes a term-set, which is \
         shorter than any kind-and-key this fixture writes"
    );
}

/// RETIRED WITH THE FIELD, for the same reason as the census above: there is no id the address
/// held. It compared `PageIdentity::of`'s answer against the address's stored id at 0.00% divergence
/// and that control is what licensed the read path to stop consulting the field. The read path no
/// longer consults it and the field no longer exists, so both halves of the comparison are now the
/// same expression. `shard_carried_identity` is the surviving non-circular control.
#[test]
#[ignore = "retired: the address no longer holds an id to compare a read's identity against"]
fn the_identity_a_read_builds_is_the_id_the_address_held() {
    println!("retired: see the_derivation_census_is_retired_because_its_other_side_is_gone");
}


/// THE IDENTITY CANNOT BE BUILT FROM AN ID, WHICH IS THE WHOLE OF WHY IT IS A TYPE.
///
/// An `Option<u64>` threaded to the read sites would compile with `None` and lose a block in silence;
/// a bare `u64` would compile with a stale one. Both hazards come back the moment a constructor
/// accepts the ANSWER instead of the TERMS, and that is a one-line change someone will make for a
/// caller that "already has the id". So it is asserted, over the source text, at NAME level.
///
/// SCOPED TO THE IMPL BLOCK, with its length asserted first. A `contains` over a whole file scores
/// every claim below as a pass the moment the slice comes back empty.
///
/// rust-internal: reads this crate's own source text, no product behaviour
#[test]
fn the_page_identity_has_no_constructor_that_takes_an_id() {
    const HASHING: &str = include_str!("../hashing.rs");
    assert!(
        HASHING.len() > 3_000,
        "hashing.rs read back as {} bytes, so every assertion below is over nothing",
        HASHING.len()
    );

    let start = HASHING
        .find("impl<'a> PageIdentity<'a> {")
        .expect("the identity's impl block is still spelled `impl<'a> PageIdentity<'a>`");
    let rest = &HASHING[start..];
    let end = rest
        .find("\n}\n")
        .expect("the identity's impl block still ends at column zero");
    let block = &rest[..end];
    assert!(
        block.len() > 400,
        "DENOMINATOR: the impl block read back as {} bytes; a short slice passes every claim below",
        block.len()
    );

    // EXACTLY ONE constructor, counted rather than assumed absent.
    let constructors = block.matches("-> Self").count();
    assert_eq!(
        1, constructors,
        "the identity has {constructors} constructors, not one. Every one of them is a way to build \
         an identity, and this test can only vouch for the one that takes the terms"
    );
    assert!(
        block.contains("pub(super) fn of(shard_id: ShardId, kind: &str, key: &str, component: Option<&'a str>) -> Self"),
        "the one constructor is no longer `of(shard, kind, key, component)`. If it now takes an id, \
         a caller can pass a stale one and the compiler cannot say so -- which is the entire reason \
         this type exists rather than a bare `u64`"
    );

    // AND NO OTHER DOOR. `From`/`TryFrom`/`new` would each be a second constructor that the count
    // above cannot see if it is written outside this impl block.
    for door in ["fn from_object_id", "impl From<u64> for PageIdentity", "fn new(", "fn with_id"] {
        assert!(
            !HASHING.contains(door),
            "hashing.rs now contains {door:?}: an identity that can be built from a value rather \
             than from the terms is an identity a caller can get wrong"
        );
    }

    // The field is private, so nothing outside this module can construct the struct literally.
    assert!(
        HASHING.contains("pub(super) struct PageIdentity<'a> {\n    object_id: u64,"),
        "PageIdentity's object_id is no longer a private field. A `pub` field is a constructor \
         with no name on it"
    );
}
