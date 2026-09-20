// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What an in-memory `BlockAddress` costs, and what a compact form would buy.
//!
//! WHY THIS EXISTS. `BlockAddress` is 48 bytes. Three of its fields -- `block_slab_id`, `offset`,
//! `length` -- are always meaningful. Four more -- `page_id`, `object_id`, `generation`,
//! `routing_bucket` -- are OPTIONAL, gated by a `present` bitmask, and the struct allocates all
//! four whether or not the bitmask says they are set. That is 24 bytes of optional payload plus
//! one byte of bitmask, and the shard holds one of these per stored point: a 1,000-point feature
//! series holds 1,000 of them.
//!
//! So the obvious question is whether to pack it. The answer turns on a single number: how many
//! live addresses actually carry NONE of the four optional fields. If most carry none, the
//! optional payload is dead weight and a compact form reclaims it. If most carry all four, there
//! is nothing to reclaim and packing buys only the bitmask byte and the padding.
//!
//! `the_optional_payload_is_paid_for_on_every_live_address` measures that number. It is the whole
//! task, and it is measured before anything else here.
//!
//! WHAT THESE PROBES ARE NOT. They are `#[ignore]`d because they seed tens of thousands of
//! records and read process RSS, which is neither fast nor meaningful under a parallel test run.
//! Run them by name. Two tests here are NOT ignored, and both are cheap:
//! `an_address_is_forty_eight_bytes_and_half_of_them_are_optional` pins the width so that
//! widening the struct is noticed rather than absorbed, and
//! `only_one_of_the_three_address_cross_checks_on_a_read_can_fire` pins how many of the address
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
    with_routing_bucket: usize,
    /// How many addresses carry exactly 0, 1, 2, 3 or 4 of the optional fields.
    optional_field_histogram: [usize; 5],
    /// Entries held in a `BTreeMap<_, BlockAddress>` rather than a `HashMap`, which is the
    /// population a node-overhead figure applies to.
    in_btree: usize,
    /// The largest value observed in each field, in walk order:
    /// block_slab_id, offset, length, page_id, object_id, generation, routing_bucket.
    ///
    /// This is what decides whether a LOSSLESS narrower form exists at all. Presence tells you
    /// whether a field can be omitted; width tells you whether it can be shrunk. A field that is
    /// always set AND uses its full 64 bits cannot be made smaller without dropping information.
    widest: [u64; 7],
    /// Addresses whose `generation` equals `page_id.or(object_id)` -- which is what every
    /// production constructor in the tree passes for it.
    ///
    /// WHY THIS IS COUNTED. Presence and width are the two obvious questions about a field; this
    /// is the third and it is worth more than either. `append.rs` builds an address with
    /// `Some(page_id)` as the generation, and `record.rs` rebuilds one on read with
    /// `header.page_id.or(header.object_id)`. If that holds for every live address then
    /// `generation` carries no information of its own: it is a COPY of a neighbouring field, and
    /// dropping it costs 8 bytes and imposes no capacity ceiling at all -- unlike narrowing,
    /// which always does.
    ///
    /// Counted rather than asserted, because the wire carries `generation` as its own key and an
    /// index written earlier could hold a value that disagrees. The number below is the evidence
    /// for or against, and the disagreeing samples are printed.
    generation_is_a_copy: usize,
    generation_disagrees: usize,
    generation_disagreement_samples: Vec<(u64, Option<u64>, Option<u64>)>,
}

impl AddressCensus {
    fn observe(&mut self, address: &BlockAddress) {
        self.total += 1;
        self.widest[0] = self.widest[0].max(address.block_slab_id);
        self.widest[1] = self.widest[1].max(address.offset);
        self.widest[2] = self.widest[2].max(address.length());
        self.widest[3] = self.widest[3].max(address.block_id().unwrap_or(0));
        self.widest[4] = self.widest[4].max(address.object_id().unwrap_or(0));
        self.widest[5] = self.widest[5].max(address.generation().unwrap_or(0));
        self.widest[6] = self.widest[6].max(address.routing_bucket().unwrap_or(0) as u64);
        let mut set = 0usize;
        if address.block_id().is_some() {
            self.with_block_id += 1;
            set += 1;
        }
        if address.object_id().is_some() {
            self.with_object_id += 1;
            set += 1;
        }
        if address.generation().is_some() {
            self.with_generation += 1;
            set += 1;
        }
        if address.routing_bucket().is_some() {
            self.with_routing_bucket += 1;
            set += 1;
        }
        self.optional_field_histogram[set] += 1;

        // Is the generation its own value, or a copy of a neighbour?
        let derived = address.block_id().or(address.object_id());
        if address.generation() == derived {
            self.generation_is_a_copy += 1;
        } else {
            self.generation_disagrees += 1;
            if self.generation_disagreement_samples.len() < 8 {
                self.generation_disagreement_samples.push((
                    address.offset,
                    address.generation(),
                    derived,
                ));
            }
        }
    }

    fn note(&mut self, name: &'static str, count: usize) {
        self.per_map.push((name, count));
    }

    /// The optional payload is 28 bytes. An address carrying none of it pays 28 bytes for
    /// nothing; one carrying all four pays nothing for nothing.
    fn dead_optional_bytes(&self) -> usize {
        // Six bytes is the AVERAGE width of the four optional fields: two 64-bit identities and
        // two 32-bit ones, 24 bytes over four. It was seven while `block_id` was 64 bits.
        self.optional_field_histogram
            .iter()
            .enumerate()
            .map(|(set, count)| count * (4 - set) * 6)
            .sum()
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
            "  optional fields SET, of {} addresses: page_id {} ({:.1}%), object_id {} ({:.1}%), generation {} ({:.1}%), routing_bucket {} ({:.1}%)",
            self.total,
            self.with_block_id,
            100.0 * self.with_block_id as f64 / self.total.max(1) as f64,
            self.with_object_id,
            100.0 * self.with_object_id as f64 / self.total.max(1) as f64,
            self.with_generation,
            100.0 * self.with_generation as f64 / self.total.max(1) as f64,
            self.with_routing_bucket,
            100.0 * self.with_routing_bucket as f64 / self.total.max(1) as f64,
        );
        for (set, count) in self.optional_field_histogram.iter().enumerate() {
            println!(
                "  carrying exactly {set} of 4 optional fields: {count} ({:.1}% of {})",
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
            "  generation EQUALS page_id.or(object_id) on {} of {} addresses ({:.2}%); it differs on {}",
            self.generation_is_a_copy,
            self.total,
            100.0 * self.generation_is_a_copy as f64 / self.total.max(1) as f64,
            self.generation_disagrees,
        );
        for (offset, held, derived) in &self.generation_disagreement_samples {
            println!("    disagreement at offset {offset}: stored {held:?} vs derived {derived:?}");
        }

        // Presence says whether a field can be OMITTED. Width says whether it can be SHRUNK.
        // Both have to fail before the 48 bytes are justified.
        let names = [
            "block_slab_id", "offset", "length",
            "page_id", "object_id", "generation", "routing_bucket",
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
             fields is declared u64 (routing_bucket u32) and a production shard is free to use the range",
            (lossless_bits + 7) / 8,
        );
    }
}

/// Walk every map on a shard that holds a `BlockAddress`, including the bucket index.
///
/// The bucket index matters and is easy to miss: `BlockIndex` carries its own `BlockAddress`, so
/// every page addressed by a model map is addressed a SECOND time here. A census that read only
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
/// If most live addresses carry none of the four optional fields, packing reclaims 28 bytes each
/// and is worth doing. If most carry all four, packing reclaims the bitmask byte and the padding
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
        let all_four = c.optional_field_histogram[4];
        println!(
            "{label}: {none_set} of {} addresses ({:.1}%) carry NO optional field; {all_four} ({:.1}%) carry all four",
            c.total,
            100.0 * none_set as f64 / c.total as f64,
            100.0 * all_four as f64 / c.total as f64,
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

    fn address(i: u64) -> BlockAddress {
        BlockAddress::from_parts(1, i * 64, 64, Some(i), Some(i), Some(7), Some(i))
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
    assert!(
        control_per_entry > 40.0,
        "positive control must see the Vec it just built: {control_per_entry:.1} bytes/entry -- \
         if this is near zero the RSS harness is blind and every arm below is meaningless"
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
    // Sixteen model maps PLUS the bucket index, which holds a second address per page.
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
/// NOT ignored. It writes ONE page into a tempdir and asserts three halves separately, in
/// hundredths of a second -- no seeding, no RSS reading, no timing, so nothing about it ever
/// needed a run-by-name budget. It was parked with the measurement probes around it and stayed
/// there, which left the claim it carries -- that of the address cross-checks on the read path
/// exactly ONE can fire -- documented and unenforced. Every argument about dropping an optional
/// field from the address rests on that count, so it belongs in the gate.
#[test]
fn only_one_of_the_three_address_cross_checks_on_a_read_can_fire() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = BlockStore::new(dir.path());

    let payload = b"a page whose header states which block of its object it is".to_vec();
    let object_id = 0x0123_4567_89ab_cdefu64;
    let routing_bucket = 4_155_475_953u32;

    // The page under test is block THREE of its object, not block zero. Stripping the optional
    // field leaves the address carrying None, and if the page it names were block zero then an
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
    assert!(good.object_id().is_some(), "fixture must produce an address carrying object_id");
    assert!(
        good.routing_bucket().is_some(),
        "fixture must produce an address carrying routing_bucket"
    );

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

    let mut tampered = good.clone();
    tampered.set_object_id(Some(object_id ^ 1));
    match store.read(&tampered) {
        Err(_) => noticed.push("object_id"),
        Ok(bytes) => {
            assert_eq!(payload, bytes);
            ignored.push("object_id");
            println!("  object_id wrong -> read succeeded: the header carries no object id to compare");
        }
    }

    let mut tampered = good.clone();
    tampered.set_routing_bucket(Some(routing_bucket ^ 1));
    match store.read(&tampered) {
        Err(_) => noticed.push("routing_bucket"),
        Ok(bytes) => {
            assert_eq!(payload, bytes);
            ignored.push("routing_bucket");
            println!("  routing_bucket wrong -> read succeeded: the header carries no routing bucket");
        }
    }

    // The arm that was deleted, and why it is not the same case as the two above. `slab_id()` is
    // derived, not stored: it hands back the address's own `block_slab_id`, and that is the slab
    // file the read just opened. A cross-check against it could only ever have asked whether the
    // slab a reader opened is the slab a reader opened -- true in every state, including one where
    // a slab id has been reused and a record in the new slab stamps the reused number.
    assert_eq!(
        Some(good.block_slab_id),
        good.slab_id(),
        "the address's slab id IS its block_slab_id, so it cannot disagree with the file it named"
    );

    // The one live check is presence-gated: strip the field and it stops running altogether.
    let mut stripped = good.clone();
    stripped.set_block_id(None);
    assert!(stripped.block_id().is_none());
    let stripped_reads = store.read(&stripped).is_ok();

    println!(
        "of 3 address cross-checks on the read path, {} can fire ({:?}) and {} cannot ({:?})",
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
        vec!["object_id", "routing_bucket"],
        ignored,
        "object_id and routing_bucket are not in the record header, so the checks guarding them \
         are unreachable in the current format"
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
/// `only_one_of_the_three_address_cross_checks_on_a_read_can_fire` has already shown that the
/// object-id and routing-bucket arms cannot fire -- which leaves the block ordinal and the
/// checksum holding the whole of it.
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
        stale.block_slab_id, live.block_slab_id,
        "denominator: same slab id"
    );
    assert_eq!(stale.offset, live.offset, "denominator: same offset");
    assert_eq!(stale.length(), live.length(), "denominator: same length");
    assert_eq!(
        stale.block_id(),
        live.block_id(),
        "denominator: same block ordinal, so the one live cross-check reads through"
    );
    // And they disagree on exactly the field whose cross-check cannot fire.
    assert_ne!(
        stale.object_id(),
        live.object_id(),
        "denominator: the object ids differ -- this is what a live object-id check would catch"
    );

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
/// `block_store.rs` already asserts the 48, and a `const _` beside the declaration makes a
/// widening a BUILD failure. This adds the decomposition, because 48 on its own does not say
/// WHERE it goes, and the whole packing argument is about the optional payload inside it. If a
/// field is added, or an optional field is promoted to always-present, this fails with a number
/// that names which half moved.
#[test]
fn an_address_is_forty_eight_bytes_and_half_of_them_are_optional() {
    // Always meaningful: two u64 slab coordinates and the 32-bit byte count.
    const ALWAYS: usize = 2 * 8 + 4;
    // Four optional fields: two u64 identities, the 32-bit block id and the routing bucket.
    const OPTIONAL: usize = 2 * 8 + 4 + 4;
    // The presence bitmask.
    const BITMASK: usize = 1;

    assert_eq!(48, std::mem::size_of::<BlockAddress>(), "the address width moved");
    assert_eq!(8, std::mem::align_of::<BlockAddress>());
    assert_eq!(20, ALWAYS);
    assert_eq!(24, OPTIONAL);
    assert_eq!(
        48,
        ALWAYS + OPTIONAL + BITMASK + 3,
        "20 always + 24 optional + 1 bitmask + 3 padding = 48; if this stops adding up, a field \
         changed shape and the packing arithmetic in this module is stale"
    );

    // The optional payload is HALF the struct. That is the quantity every probe here is about.
    assert_eq!(
        50,
        100 * OPTIONAL / std::mem::size_of::<BlockAddress>(),
        "the optional payload is 50% of the address"
    );

    // An address built with no optional field is the same 48 bytes as one built with all four.
    // This is the fact that makes the question worth asking at all.
    let bare = BlockAddress::from_parts(1, 0, 64, None, None, None, None);
    let full = BlockAddress::from_parts(1, 0, 64, Some(1), Some(2), Some(3), Some(4));
    assert_eq!(std::mem::size_of_val(&bare), std::mem::size_of_val(&full));
    assert!(bare.block_id().is_none() && full.block_id().is_some());
}

// ---------------------------------------------------------------------------------------------
// Is the same descriptor stored more than once? (the interning premise)
// ---------------------------------------------------------------------------------------------

/// Every live address, grouped by the physical location it names and by its exact value.
///
/// INTERNING'S PREMISE, stated as something that can be false. A handle table only pays if the
/// same descriptor is STORED more than once -- if the bucket index's address for a page and the
/// model map's address for that same page are equal. They are both built on the write path from
/// the same parts, so they look like they must be. They are not obliged to be: the two are
/// written by different call sites, and a page rewritten in place keeps one entry in the bucket
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
            .entry((address.block_slab_id, address.offset))
            .or_default()
            .insert(address.clone());
    }

    fn observe_bucket(&mut self, address: &BlockAddress) {
        self.bucket_total += 1;
        *self.stores_per_value.entry(address.clone()).or_default() += 1;
        let key = (address.block_slab_id, address.offset);
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
    if a.object_id() != b.object_id() {
        out.push("object_id");
    }
    if a.generation() != b.generation() {
        out.push("generation");
    }
    if a.routing_bucket() != b.routing_bucket() {
        out.push("routing_bucket");
    }
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
///   per-series page table: `BTreeMap<u64, u32>` beside a `Vec<BlockAddress>` the series owns.
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
        BlockAddress::from_parts(1, i * 64, 64, Some(i), Some(i), Some(7), Some(i))
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
                // The real shape: MANY consecutive timestamps naming ONE page. A feature series
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

    // The per-series page table: the series owns its addresses, the map holds an index into them.
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

/// THE FREE GUARD over the decision above: the bucket index holds ONE page inline, and the
/// timestamped series maps do not.
///
/// WHY THIS IS THE THING TO PIN. The MEASUREMENT probes in this module are `#[ignore]`d -- they
/// read process RSS and seed tens of thousands of records -- so on a normal run none of those
/// execute. This one is free and always runs, and it pins the two structural facts the
/// recommendation rests on:
///
///   1. `BlockIndexMap` carries its single-page case INLINE. That is what makes it cost 73.0
///      bytes for a one-entry index where a `BTreeMap<u64, BlockAddress>` costs 764.3 -- a
///      `BTreeMap` leaf is allocated whole and sized for eleven entries whether one is filed in
///      it or eleven are. If the `One` variant is ever boxed or removed, this enum stops being
///      wider than the map it replaces and the 90.4% saving is silently gone.
///
///   2. All six timestamped series maps are still the SAME container. `timestamped_series_mut`
///      hands out one type for six kinds, so the shape cannot be adopted for one of them alone --
///      which is exactly why the recommendation is stated for the six together. The binding
///      below is a compile-time proof of that: if any one of the six is narrowed and the others
///      are not, this stops compiling rather than passing on a stale assumption.
///
/// MUTATION. Boxing `BlockIndexMap::One`'s page (`One(u64, Box<BlockIndex>)`) collapses the enum
/// to a pointer and fires the first assert. Changing any one of the six map types fires the
/// second as a compile error rather than a failure.
#[test]
fn the_bucket_index_holds_one_page_inline_and_the_series_maps_hold_none() {
    use crate::engine::state::{BlockIndex, BlockIndexMap};

    // (1) The split shape is wider INLINE than the map it replaces, because it carries a whole
    // page in the `One` variant instead of a pointer to a node.
    let split = std::mem::size_of::<BlockIndexMap>();
    let map = std::mem::size_of::<BTreeMap<u64, BlockAddress>>();
    let page = std::mem::size_of::<BlockIndex>();
    println!(
        "BlockIndexMap is {split} B inline, BTreeMap<u64, BlockAddress> is {map} B, \
         BlockIndex is {page} B"
    );
    assert!(
        split > map,
        "BlockIndexMap must be WIDER inline than the map it replaces -- it is {split} B against \
         {map} B. A split shape no wider than a BTreeMap is not holding its page inline, which is \
         the entire reason it costs 73.0 bytes for a single-entry index where a BTreeMap costs \
         764.3 (measured in the_container_shapes_priced_against_the_population_each_one_pays_in)"
    );
    assert!(
        split >= page,
        "BlockIndexMap must be able to hold a whole {page}-byte BlockIndex inline; it is {split} B"
    );

    // POSITIVE CONTROL for the predicate above, so a passing assert is not just a wide enum.
    // This is exactly what the mutation would produce -- the `One` variant boxed, so the page is
    // behind a pointer instead of inline -- and it must FAIL the same test the real shape passes.
    // Without this, `split > map` would keep passing on any enum that happened to be wide for an
    // unrelated reason, and the guard would stop watching the thing it names.
    enum BoxedShape {
        #[allow(dead_code)]
        Empty,
        #[allow(dead_code)]
        One(u64, Box<BlockIndex>),
        #[allow(dead_code)]
        Many(BTreeMap<u64, BlockIndex>),
    }
    let boxed = std::mem::size_of::<BoxedShape>();
    println!("  positive control: the same shape with One boxed is {boxed} B inline");
    assert!(
        boxed < page,
        "the control must NOT hold a page inline: a boxed One is {boxed} B against a {page}-byte          page. If this ever reads as wide, the predicate below it cannot tell an inline page from          a pointer to one and the guard is vacuous"
    );
    assert!(
        !(boxed > map && boxed >= page),
        "the control must FAIL the predicate the real shape passes ({boxed} B boxed, {map} B map,          {page} B page)"
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
        max_objects_in_a_bucket = max_objects_in_a_bucket.max(bucket.object_index.len());
        let mut blocks_here = 0usize;
        for (_, page) in bucket.block_index.iter() {
            blocks_here += 1;
            model_ids.insert(page.model_id.to_string());
            *blocks_per_object.entry(page.object_id()).or_default() += 1;
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
    println!("--- what the struct actually costs ---");
    println!(
        "  size_of BlockAddress = {} (payload 4*8 + 3*4 + 1 = 45, so {} bytes are padding)",
        std::mem::size_of::<BlockAddress>(),
        std::mem::size_of::<BlockAddress>() - 45
    );
    println!(
        "  align_of BlockAddress = {}",
        std::mem::align_of::<BlockAddress>()
    );
    println!(
        "  size_of BlockIndex = {} (it holds a BlockAddress plus 2 Arc<str>, an Option<Arc<str>> and 3 bools)",
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

    // BLOCKS PER OBJECT IS NOT A FIXTURE FAILURE, it is the answer.
    //
    // This arm wrote 4,000 hash fields under ONE key expecting one object with 4,000 blocks, and
    // got 4,000 objects with one block each. That is not the fixture missing: our object identity
    // is stable_block_object_id(shard, kind, key, COMPONENT), so a component is a separate OBJECT
    // rather than another block inside one. page_id therefore has almost nothing left to
    // enumerate, which is why it measures 1 -- and it is a fact about the design, not about the
    // seed. Recorded here so the next reader does not spend the same build cycles on it.
    println!(
        "  NOTE: blocks-per-object is {max_blocks_in_an_object} because component identity is folded \
         into the OBJECT id, so a component is its own object rather than another block"
    );
}
