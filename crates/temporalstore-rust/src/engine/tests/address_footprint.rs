// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What an in-memory `BlockAddress` costs, and what a compact form would buy.
//!
//! WHY THIS EXISTS. `BlockAddress` is 56 bytes. Three of its fields -- `block_slab_id`, `offset`,
//! `length` -- are always meaningful. Four more -- `page_id`, `object_id`, `generation`,
//! `routing_bucket` -- are OPTIONAL, gated by a `present` bitmask, and the struct allocates all
//! four whether or not the bitmask says they are set. That is 28 bytes of optional payload plus
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
//! Run them by name. The one test here that is NOT ignored is
//! `an_address_is_fifty_six_bytes_and_twenty_eight_of_them_are_optional`, which is a cheap guard:
//! it pins the width so that widening the struct is noticed rather than absorbed.
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
    with_page_id: usize,
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
}

impl AddressCensus {
    fn observe(&mut self, address: &BlockAddress) {
        self.total += 1;
        self.widest[0] = self.widest[0].max(address.block_slab_id);
        self.widest[1] = self.widest[1].max(address.offset);
        self.widest[2] = self.widest[2].max(address.length);
        self.widest[3] = self.widest[3].max(address.page_id().unwrap_or(0));
        self.widest[4] = self.widest[4].max(address.object_id().unwrap_or(0));
        self.widest[5] = self.widest[5].max(address.generation().unwrap_or(0));
        self.widest[6] = self.widest[6].max(address.routing_bucket().unwrap_or(0) as u64);
        let mut set = 0usize;
        if address.page_id().is_some() {
            self.with_page_id += 1;
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
    }

    fn note(&mut self, name: &'static str, count: usize) {
        self.per_map.push((name, count));
    }

    /// The optional payload is 28 bytes. An address carrying none of it pays 28 bytes for
    /// nothing; one carrying all four pays nothing for nothing.
    fn dead_optional_bytes(&self) -> usize {
        self.optional_field_histogram
            .iter()
            .enumerate()
            .map(|(set, count)| count * (4 - set) * 7)
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
            self.with_page_id,
            100.0 * self.with_page_id as f64 / self.total.max(1) as f64,
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
        println!(
            "  address payload resident: {} x 56 = {} bytes ({:.2} MiB)",
            self.total,
            self.total * 56,
            (self.total * 56) as f64 / (1024.0 * 1024.0)
        );
        println!(
            "  of which optional payload NEVER SET: {} bytes ({:.2} MiB) -- the whole packing prize",
            self.dead_optional_bytes(),
            self.dead_optional_bytes() as f64 / (1024.0 * 1024.0)
        );

        // Presence says whether a field can be OMITTED. Width says whether it can be SHRUNK.
        // Both have to fail before the 56 bytes are justified.
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
        ("control_state_pages", &shard.control_state_pages),
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
            for (_, page) in bucket.page_index.iter() {
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
            c.total * 56,
            100.0 * c.dead_optional_bytes() as f64 / (c.total * 56) as f64,
        );
    }
}

/// What a `BTreeMap` entry costs on top of the value it carries, measured rather than derived.
///
/// A `BTreeMap<u64, BlockAddress>` does not cost 56 bytes per entry. Its leaf node is sized for
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
/// footprint (64 bytes per element, no node), so if the harness cannot see that it cannot see
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
/// This is the fourth thing the task asks about. `decode_page_record` cross-checks the address's
/// `page_id`, `object_id` and `routing_bucket` against the record header, each written
/// `if let (Some(from_address), Some(from_record))`. Reading that source alone, all three look
/// like live corruption detectors, and three live detectors would be a strong reason to keep the
/// fields regardless of what they cost.
///
/// They are not three. The record header carries only the block id; `object_id`, `routing_bucket`
/// and `slab_id` are deliberately NOT in it, because the index holds them -- so two of those three
/// `if let` pairs can never match and the checks they guard never run. This test pins which is
/// which, because the difference decides what a compact form would actually be giving up.
///
/// It cannot pass by finding nothing: the honest address must read back first, the live check must
/// REFUSE a tampered address, and the inert ones must ACCEPT one. An arm that stopped firing would
/// flip a count, not fall silent.
#[test]
#[ignore = "touches the filesystem; run by name"]
fn only_one_of_the_three_address_cross_checks_on_a_read_can_fire() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalBlockStore::new(dir.path());

    let payload = b"a page whose header states which block of its object it is".to_vec();
    let object_id = 0x0123_4567_89ab_cdefu64;
    let routing_bucket = 4_155_475_953u32;
    let good = store
        .append_with_page_metadata(&payload, Some(object_id), Some(routing_bucket))
        .expect("append");

    // DENOMINATOR: the honest address reads back, and really carries all three fields under test.
    assert_eq!(
        payload,
        store.read(&good).expect("the honest address must read back"),
        "denominator: the unmodified address reads its page"
    );
    assert!(good.page_id().is_some(), "fixture must produce an address carrying page_id");
    assert!(good.object_id().is_some(), "fixture must produce an address carrying object_id");
    assert!(
        good.routing_bucket().is_some(),
        "fixture must produce an address carrying routing_bucket"
    );

    // Tamper with each field in turn and record whether the read noticed.
    let mut noticed: Vec<&str> = Vec::new();
    let mut ignored: Vec<&str> = Vec::new();

    let mut tampered = good.clone();
    tampered.set_page_id(Some(good.page_id().unwrap() ^ 0xffff));
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

    // The one live check is presence-gated: strip the field and it stops running altogether.
    let mut stripped = good.clone();
    stripped.set_page_id(None);
    assert!(stripped.page_id().is_none());
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

/// The width guard. Not ignored: it is free, and it is the thing that makes a future widening
/// visible.
///
/// `block_store.rs` already asserts the 56. This adds the decomposition, because 56 on its own
/// does not say WHERE it goes, and the whole packing argument is about the 28 bytes of optional
/// payload inside it. If a field is added, or an optional field is promoted to always-present,
/// this fails with a number that names which half moved.
#[test]
fn an_address_is_fifty_six_bytes_and_twenty_eight_of_them_are_optional() {
    // Three always-meaningful u64s.
    const ALWAYS: usize = 3 * 8;
    // Four optional fields: three u64 and one u32.
    const OPTIONAL: usize = 3 * 8 + 4;
    // The presence bitmask.
    const BITMASK: usize = 1;

    assert_eq!(56, std::mem::size_of::<BlockAddress>(), "the address width moved");
    assert_eq!(8, std::mem::align_of::<BlockAddress>());
    assert_eq!(24, ALWAYS);
    assert_eq!(28, OPTIONAL);
    assert_eq!(
        56,
        ALWAYS + OPTIONAL + BITMASK + 3,
        "24 always + 28 optional + 1 bitmask + 3 padding = 56; if this stops adding up, a field \
         changed shape and the packing arithmetic in this module is stale"
    );

    // The optional payload is HALF the struct. That is the quantity every probe here is about.
    assert_eq!(
        50,
        100 * OPTIONAL / std::mem::size_of::<BlockAddress>(),
        "the optional payload is 50% of the address"
    );

    // An address built with no optional field is the same 56 bytes as one built with all four.
    // This is the fact that makes the question worth asking at all.
    let bare = BlockAddress::from_parts(1, 0, 64, None, None, None, None);
    let full = BlockAddress::from_parts(1, 0, 64, Some(1), Some(2), Some(3), Some(4));
    assert_eq!(std::mem::size_of_val(&bare), std::mem::size_of_val(&full));
    assert!(bare.page_id().is_none() && full.page_id().is_some());
}
