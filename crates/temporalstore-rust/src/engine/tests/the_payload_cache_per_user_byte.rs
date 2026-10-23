// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE PAYLOAD CACHE HOLDS PER USER BYTE, at four value sizes and three corpus sizes.
//!
//! # THE PUBLISHED PAYLOAD FIGURE DIVIDES UNLIKE BY UNLIKE
//!
//! `index_bytes_per_key::the_amplification_set_in_the_published_format` reports the payload's
//! space-in-memory as `ppm(cache.memory_bytes, live_physical_bytes)` and gets 15.229x against a
//! 5.0x ceiling. The two halves are not the same kind of byte:
//!
//!   * the numerator is the cache's own accounting, which is `memory_bytes += value.len()` over
//!     resident entries -- USER bytes, exactly as the caller wrote them;
//!   * the denominator is live block lengths, which is what was STORED, after compression.
//!
//! So the quotient is the cache's real overhead multiplied by the compression factor. On this
//! corpus the compression factor is 7.31x, which is most of the 15.229x.
//!
//! # BUT THE OVERHEAD IS NOT 1.0x EITHER, AND THAT IS THE SUBJECT HERE
//!
//! The cache's figure sums value lengths and nothing else -- not keys, not per-entry metadata, not
//! the table. So 2,000 values of 256 bytes should read 512,000 B. It reads 1,066,000. Something
//! holds 533 bytes per 256-byte value and the accounting charges every one of them as a value
//! byte. Two laws fit that single point:
//!
//!   * a CONSTANT per entry -- `bytes/entry - value` is flat in the value size, and the ratio
//!     falls as values grow;
//!   * a FACTOR -- the ratio is flat in the value size, and `bytes/entry - value` grows with it.
//!
//! They have different remedies, and one point at one value size cannot tell them apart. That is
//! what the value-size sweep is for. The corpus sweep is beside it because a per-entry figure that
//! does not move with the entry count is not per entry at all.
//!
//! # MEASURED: IT IS A FACTOR OF TWO, AND IT IS TWO CACHE LAYERS
//!
//! `B/entry = 2 * value + 21` at every value size measured -- 149 at 64, 533 at 256, 2,069 at
//! 1,024, 8,213 at 4,096, each exact -- and identical at 500, 1,000, 2,000 and 4,000 keys. So the
//! overhead is a FACTOR, not a per-entry constant, and it is a clean per-entry law with no
//! table-step in it (`memory_bytes` sums value lengths, so no hash table is being weighed).
//!
//! The counters say which factor. `puts` and `memory_fills` are exactly **2.00 per key** with zero
//! evictions, and the read loop touches each key once -- so both puts land on the READ:
//!
//!   * `cached_response` wraps the command behind `CacheKey::string(shard, key)` -- the LOGICAL
//!     key -- and on a miss caches the encoded `CommandResponse`;
//!   * inside that miss, `read_block_frame_bytes` consults `CacheKey::page_with_slot(shard, slab,
//!     offset, length, bucket)` -- the PHYSICAL address -- and on its own miss caches the frame.
//!
//! Both entries hold the payload. The +21 B is the response envelope over the raw value.
//!
//! # AND THE SECOND ENTRY'S SIZE DEPENDS ON THE PAYLOAD'S BYTES
//!
//! The 2.0x law is NOT structural, which an assertion written to the contrary is what found. At the
//! same value size the cache charges 4,138,000 B for a run of `'v'` and 5,161,820 B for
//! incompressible noise -- a residual of exactly half the value size (0.5008x at 256 B, 0.4999x at
//! 1,024 B) with `fills/key` still exactly 2.00, so the entry count is unchanged and the per-entry
//! bytes grew.
//!
//! That is msgpack encoding a `Vec<u8>` as an ARRAY OF INTEGERS rather than a byte string: a byte
//! below 0x80 is a positive fixint costing one byte, a byte at or above it takes a uint8 marker and
//! costs two. Uniform random data is half above the line and averages 1.5 bytes per byte; `'v'` is
//! 0x76 and costs 1.0. `encode_cached_response` builds an `rmp_serde::Serializer`,
//! `CommandResponse::value` is a plain `Vec<u8>`, and `serde_bytes` appears nowhere in this crate.
//!
//!     frame entry    = value                 (the block's own bytes, uncompressed)
//!     response entry = encoded(value) + 21   (msgpack, plus the response envelope)
//!
//!     => 2.0 * value + 21   for payloads below 0x80   (exact; asserted as an equality)
//!     => 2.0 * value + c    for BOTH fixtures since #2279, c = 20 below 256 B and 21 from 256
//!                              up (msgpack bin8 vs bin16 length prefix); 148.0 B/entry at 64
//!                              against 533.0 at 256. Before #2279 the payload was a
//!                              msgpack byte string that charges by length alone. It was
//!                              2.5 * value + 21 before that, because the response copy was an
//!                              array of integers costing two bytes for every byte at or above
//!                              0x80. The assertion requiring noise to cost MORE fired the first
//!                              time this arm ran after that fix -- see the note beside it.
//!
//! A `#[serde(with = "serde_bytes")]` on that field would make it a `bin` blob and charge 1.0 for
//! any payload. That is a wire-format decision about a cached encoding and not one this arm makes.
//!
//! **I CALLED THIS "NOT A DEFECT" AND THAT WAS WRONG — see the correction below.** The reasoning
//! was that the block entry earns its place on multi-element blocks, where one frame answers
//! several keys, while the response entry saves re-selecting an element on a point read. The
//! premise was asserted and never measured.
//!
//! `how_many_keys_a_cached_frame_serves` measured it: at 500 keys there are **500 distinct frames**
//! at value sizes 64, 256, 1,024 and 4,096 B — one key per frame, largest group 1, **zero** frames
//! serving more than one key at any size. Two keys share a frame only if they share
//! `(slab, offset, length)`, and none do.
//!
//! # AND THE CORRECTION ITSELF WAS WRONG, FOR THE SAME REASON TWICE OVER
//!
//! I then wrote that the frame entry "duplicates the response entry entirely" and that the 2.0x is
//! "waste rather than a trade". **That does not follow from what was measured.** Grouping rows by
//! `(slab, offset, length)` answers *do two KEYS share a frame*. I used it to answer *can anything
//! else reach this frame*, which is a different question — the first correction replaced an
//! unmeasured claim with a measured one about the wrong thing.
//!
//! The frame key is read by the 23 callers of `read_block_bytes` and the two of
//! `read_block_frame_bytes`, so it is read by one KEY and by **25 call paths**. And it is the key
//! of the PERSISTENT BLOCK CACHE: six arms in `part2` assert that a tiny memory cache refills from
//! that tier without a block-store read —
//! `memory_miss_reads_local_block_file_using_index_address`,
//! `tiny_memory_cache_eviction_refills_from_persistence_then_block_cache`,
//! `restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache`,
//! `block_reads_fill_compressed_block_cache`, `cache_replacement_policy_soak` and
//! `three_layer_cache_reads_memory_then_block_cache_then_local_file`. Removing the frame put on the
//! string read path broke all six, which is how the error was found.
//!
//! `read_bucket_index_value` is the only door from a string command into `read_block_bytes`, so
//! declining the frame there does not drop a duplicate — it removes that tier entirely for the
//! commonest data kind.
//!
//! # WHICH HALF IS THE DUPLICATE, THEN
//!
//! The physically-keyed frame is the tier with readers and persistence. The LOGICAL copy — the
//! encoded `CommandResponse` under `CacheKey::string(shard, key)` — is the one with a single reader
//! and no tier of its own, and it is the larger of the two by the envelope. So the saving this arm
//! prices is real and it is on the other side of the pair from where the earlier conclusion put it.
//!
//! `what_the_frame_layer_saves_under_eviction` measured what the second copy costs once the cache
//! is under pressure: both layers deliver **identical service** — 2,000 of 2,000 warm reads from
//! the disk tier, zero block-store reads with the frame put or without it — while the frame put
//! doubles `puts` and more than doubles evictions. A second copy that changes no outcome and
//! doubles the eviction rate is a cost; which copy to drop is settled by which one has readers.
//!
//! # A HYPOTHESIS THIS REFUTED, LEFT NAMED ON PURPOSE
//!
//! The first guess was that the WRITE path caches under a synthetic hot address
//! (`HOT_BLOCK_SLAB_ID` plus a ticket) while the read path keys off the real one, leaving a stale
//! unreachable copy. `put_memory_only` on that path makes it plausible. It is wrong: that would
//! put once on each side, and both puts are on the read. Recorded because a probe that keeps only
//! its answer invites the next reader to re-run the same guess.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_payload_cache_per_user_byte -- --ignored --nocapture

/// Values that cannot compress, so a stored-byte denominator means something.
///
/// An inlined xorshift64 keyed by the key index. It has to be unpredictable to a COMPRESSOR, not
/// statistically sound, and pulling in an RNG to make fixture bytes adds a dependency whose only
/// job is to produce noise.
fn incompressible_value(index: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0x1234_5678_9ABC_DEF0)
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

/// One reading: the cache's accounting against the user bytes that produced it.
struct Reading {
    keys: usize,
    value: usize,
    compressible: bool,
    user_bytes: u64,
    cache_bytes: u64,
    live_stored: u64,
    logical_written: u64,
    stored_written: u64,
    // THE COUNTERS THAT SETTLE WHY, not just how much. The byte ratio alone cannot distinguish a
    // double charge from a double-length value; fills PER KEY can, and they came out at exactly
    // 2.00 with no evictions. See the module docs for what that refuted.
    puts: u64,
    memory_fills: u64,
    memory_hits: u64,
    misses: u64,
    memory_evictions: u64,
}

fn weigh(keys: usize, value: usize) -> Reading {
    weigh_with(keys, value, true)
}

fn weigh_with(keys: usize, value: usize, compressible: bool) -> Reading {
    use crate::types::{Command, ExecuteRequest};

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("amp-{index:06}"),
                value: if compressible {
                    vec![b'v'; value]
                } else {
                    incompressible_value(index, value)
                },
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // READ EVERY KEY BACK. The cache fills on reads, so a write-only probe measures an empty cache
    // and reports a vacuous 0 -- which is how the published arm's memory quadrant first came out.
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("amp-{index:06}") },
        });
        assert!(response.status.ok, "read {index}: {:?}", response.status);
    }

    let reported = engine.shard_stats(1).expect("a loaded shard reports stats");
    let store = engine.block_store.stats();
    let slabs = engine.block_store.slab_summary();
    Reading {
        keys,
        value,
        compressible,
        user_bytes: (keys as u64).saturating_mul(value as u64),
        cache_bytes: reported.cache.memory_bytes,
        live_stored: slabs.live_physical_bytes,
        logical_written: store.logical_bytes_written,
        stored_written: store.bytes_written,
        puts: reported.cache.puts,
        memory_fills: reported.cache.memory_fills,
        memory_hits: reported.cache.memory_hits,
        misses: reported.cache.misses,
        memory_evictions: reported.cache.memory_evictions,
    }
}

#[test]
#[ignore = "seeds seven stores and reads every key back; run by name"]
fn what_the_payload_cache_holds_per_user_byte() {
    // THE VALUE SWEEP separates the two laws; the CORPUS sweep checks the figure is per entry at
    // all. 2,000 x 256 is the published corpus and appears in both.
    let readings: Vec<Reading> = [
        (2_000, 64),
        (2_000, 256),
        (2_000, 1_024),
        (2_000, 4_096),
        (500, 256),
        (1_000, 256),
        (4_000, 256),
    ]
    .into_iter()
    .map(|(keys, value)| weigh(keys, value))
    // THE SAME CORPUS WITH VALUES THAT CANNOT COMPRESS. Without these rows the stored-byte
    // denominator reads as a property of the store when it is a property of the letter 'v'.
    .chain(
        [(2_000_usize, 256_usize), (2_000, 1_024)]
            .into_iter()
            .map(|(keys, value)| weigh_with(keys, value, false)),
    )
    .collect();

    println!();
    println!("  THE PAYLOAD CACHE'S ACCOUNTING AGAINST THE USER BYTES THAT PRODUCED IT");
    println!();
    println!(
        "    {:>6} {:>6} {:>7} {:>10} {:>10} {:>9} {:>9} {:>9}",
        "keys", "value", "values", "user B", "cache B", "cache/user", "B/entry", "over/entry"
    );
    for reading in &readings {
        let per_entry = reading.cache_bytes as f64 / reading.keys as f64;
        println!(
            "    {:>6} {:>6} {:>7} {:>10} {:>10} {:>9.3} {:>9.1} {:>9.1}",
            reading.keys,
            reading.value,
            if reading.compressible { "run" } else { "noise" },
            reading.user_bytes,
            reading.cache_bytes,
            reading.cache_bytes as f64 / reading.user_bytes as f64,
            per_entry,
            per_entry - reading.value as f64,
        );
    }

    println!();
    println!("  AND THE DENOMINATOR THE PUBLISHED FIGURE USES, beside the one in the same unit");
    println!();
    println!(
        "    {:>6} {:>6} {:>7} {:>11} {:>11} {:>11} {:>11} {:>11}",
        "keys", "value", "values", "live stored", "logical wr", "stored wr", "mem/stored",
        "mem/user"
    );
    for reading in &readings {
        println!(
            "    {:>6} {:>6} {:>7} {:>11} {:>11} {:>11} {:>11.3} {:>11.3}",
            reading.keys,
            reading.value,
            if reading.compressible { "run" } else { "noise" },
            reading.live_stored,
            reading.logical_written,
            reading.stored_written,
            if reading.live_stored == 0 {
                0.0
            } else {
                reading.cache_bytes as f64 / reading.live_stored as f64
            },
            reading.cache_bytes as f64 / reading.user_bytes as f64,
        );
    }
    println!();
    println!("  WHY: THE CACHE'S OWN HIT AND FILL COUNTERS, per reading");
    println!();
    println!(
        "    {:>6} {:>6} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "keys", "value", "puts", "fills", "hits", "misses", "evicts", "fills/key"
    );
    for reading in &readings {
        println!(
            "    {:>6} {:>6} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8.2}",
            reading.keys,
            reading.value,
            reading.puts,
            reading.memory_fills,
            reading.memory_hits,
            reading.misses,
            reading.memory_evictions,
            reading.memory_fills as f64 / reading.keys as f64,
        );
    }
    println!();
    println!(
        "    TWO fills per key, no evictions: a point read populates BOTH cache layers -- the"
    );
    println!(
        "    command response under CacheKey::string(shard, key), and the block frame under"
    );
    println!(
        "    CacheKey::page_with_slot(address). Both hold the payload, so a single-element block"
    );
    println!("    is resident twice. That is the 2.0x; the +21 B is the response envelope.");
    println!();
    println!(
        "    WHICH COPY IS THE DUPLICATE IS NOT WHAT THIS ARM MEASURES, and an earlier reading of"
    );
    println!(
        "    it got that wrong: the frame key is read by 25 call paths and is the key of the"
    );
    println!(
        "    PERSISTENT block cache, so it is not the spare copy. The response copy has one reader"
    );
    println!("    and no tier of its own. See the module doc.");
    println!();

    // NON-VACUITY BEFORE ANY READING IS QUOTED. A cache that filled with nothing would satisfy a
    // ratio assertion at 0 and report an overhead of minus the value size.
    for reading in &readings {
        assert!(
            reading.cache_bytes > 0,
            "{} keys x {} B left the cache empty, so every figure on this row is vacuous -- the \
             read-back loop is not filling it",
            reading.keys,
            reading.value
        );
        assert!(
            reading.user_bytes > 0,
            "the denominator is zero, so the ratio on this row cannot be read"
        );
    }

    // THE LAWS, STATED AS THE ARM'S OWN FINDING. Both are printed above; this pins which one the
    // numbers actually support, so a change to the cache's accounting makes it speak rather than
    // silently moving a figure somebody is quoting.
    let by_value: Vec<&Reading> =
        readings.iter().filter(|r| r.keys == 2_000 && r.compressible).collect();
    let smallest = by_value.first().expect("the value sweep ran");
    let largest = by_value.last().expect("the value sweep ran");
    let small_ratio = smallest.cache_bytes as f64 / smallest.user_bytes as f64;
    let large_ratio = largest.cache_bytes as f64 / largest.user_bytes as f64;
    let small_over = smallest.cache_bytes as f64 / smallest.keys as f64 - smallest.value as f64;
    let large_over = largest.cache_bytes as f64 / largest.keys as f64 - largest.value as f64;
    println!(
        "    value {} -> {}:  ratio {small_ratio:.3} -> {large_ratio:.3},  over/entry \
         {small_over:.1} -> {large_over:.1}",
        smallest.value, largest.value
    );
    println!(
        "    a CONSTANT per entry holds the right column flat; a FACTOR holds the left one flat"
    );
    println!();

    assert!(
        (small_ratio - 1.0).abs() > f64::EPSILON || (large_ratio - 1.0).abs() > f64::EPSILON,
        "the cache reports exactly the user bytes at both value sizes, so there is no overhead to \
         attribute and this arm has nothing to measure"
    );

    // WHICHEVER WORLD WE ARE IN, THE ARM SAYS SO. The read-back loop touches every key exactly
    // once, so hits and misses on the read side sum to the key count; which side they land on is
    // the finding. Asserted as a disjunction with both messages spelled out, so a future change
    // that fixes the keying makes this speak rather than quietly passing with half the cache gone.
    let published = readings
        .iter()
        .find(|r| r.keys == 2_000 && r.value == 256 && r.compressible)
        .expect("the published corpus is in the sweep");
    let served_from_cache = published.memory_hits >= published.keys as u64;
    println!(
        "    published corpus (2,000 x 256): {} hits, {} misses, {} fills over {} keys -- reads \
         were served {}",
        published.memory_hits,
        published.misses,
        published.memory_fills,
        published.keys,
        if served_from_cache { "FROM THE CACHE" } else { "FROM STORAGE" }
    );
    assert!(
        published.memory_fills > 0,
        "the cache recorded no memory fill at all, so nothing populated it and every byte figure \
         above is measuring an empty cache"
    );

    // THE FIXTURE CAVEAT, ASSERTED. A run of one byte compresses to nothing, so the compressible
    // rows' stored-byte denominator is framing rather than content -- which is why the same 2.0x
    // cache overhead reported 1.961x and 234.657x in the mem/stored column. The noise rows exist
    // so that cannot be read as a property of the store, and this pins that they really do differ:
    // if the two fixtures ever store the same number of bytes, the noise rows have stopped being
    // incompressible and the caveat they document is no longer being demonstrated.
    let run_row = readings
        .iter()
        .find(|r| r.keys == 2_000 && r.value == 1_024 && r.compressible)
        .expect("the compressible 1 KiB row ran");
    let noise_row = readings
        .iter()
        .find(|r| r.keys == 2_000 && r.value == 1_024 && !r.compressible)
        .expect("the incompressible 1 KiB row ran");
    println!(
        "    1 KiB x 2,000:  run stores {} B, noise stores {} B  ({:.1}x more)",
        run_row.live_stored,
        noise_row.live_stored,
        noise_row.live_stored as f64 / run_row.live_stored.max(1) as f64
    );
    assert!(
        noise_row.live_stored > run_row.live_stored,
        "incompressible values stored {} B and a run of one byte stored {} B, so the fixture is \
         not demonstrating the compression caveat it exists to demonstrate",
        noise_row.live_stored,
        run_row.live_stored
    );
    // THE CACHE'S LAW IS **NOT** INDIFFERENT TO THE CONTENTS, and asserting that it was is what
    // found this. `memory_bytes` does sum value lengths -- but one of the two entries holds the
    // msgpack ENCODING of the value, and `rmp_serde` writes a `Vec<u8>` as an array of integers
    // rather than a byte string. A byte below 0x80 is a positive fixint and costs one byte; a byte
    // at or above 0x80 takes a uint8 marker and costs two. Uniform random data sits half above the
    // line and averages 1.5 bytes per byte; 'v' is 0x76 and costs 1.0.
    //
    // Read in the source, not inferred from the fit: `encode_cached_response` builds an
    // `rmp_serde::Serializer`, `CommandResponse::value` is a plain `Vec<u8>`, and `serde_bytes`
    // appears nowhere in this crate. A `#[serde(with = "serde_bytes")]` on that field would make
    // it a msgpack `bin` blob and charge 1.0 for any payload -- which is a wire-format decision
    // about a cached encoding, not a change this measuring arm should make.
    // THIS USED TO REQUIRE NOISE TO COST MORE, and it was right when written: the response
    // payload was a msgpack ARRAY OF INTEGERS, one byte below 0x80 and two at or above, so a
    // uniform payload cost ~1.5x a run of 'v'. #2279 made that field a byte string, which charges
    // by LENGTH ALONE -- so the two fixtures now cost the SAME, and that equality is the fix
    // landing rather than a regression.
    //
    // The old assertion fired the moment this arm was next run. It survived on main because this
    // arm is `#[ignore]`d and #2279's verification used the filter `the_cached_payload`, which does
    // not match this module; the three check arms COMPILE ignored tests without running them. A fix
    // that changes a measured law owes a run of every arm that measured it -- a filter is not that.
    assert_eq!(
        noise_row.cache_bytes, run_row.cache_bytes,
        "noise charged {} B and a run charged {} B at the same value size. Since #2279 the payload          is a msgpack byte string, which charges by length alone, so the byte distribution must not          matter -- if these differ again the field has reverted to an element-wise encoding",
        noise_row.cache_bytes,
        run_row.cache_bytes
    );

    // THE TWO LAWS, one per fixture, each stated as arithmetic the reader can check.
    //
    //     frame entry    = value                 (the block's own bytes, uncompressed)
    //     response entry = encoded(value) + 21   (msgpack, plus the response envelope)
    //
    // The run figures are exact integers, so they are asserted as equalities. The noise figures are
    // an expectation over a byte distribution, so they get a tolerance -- asserting a sampled mean
    // exactly would make this fail on a different seed, which is a fixture detail masquerading as
    // a property.
    // `c` IS NOT ONE CONSTANT: msgpack's length prefix widens at 256 bytes. A payload below 256
    // fits `bin8`, a 2-byte header; from 256 up it takes `bin16`, 3 bytes. So the envelope is 20
    // below the step and 21 above it -- measured 148.0 B/entry at value 64 against 533.0 at 256.
    //
    // The flat +21 that stood here was fitted on four value sizes, three of which shared a prefix
    // width. That is the same error as reading a per-key law off two counts inside one phase of a
    // sawtooth: the sizes agreed because they were all on one side of a step.
    for reading in readings.iter().filter(|r| r.compressible) {
        let envelope: u64 = if reading.value < 256 { 20 } else { 21 };
        let expected = (reading.keys as u64)
            .saturating_mul(2u64.saturating_mul(reading.value as u64).saturating_add(envelope));
        assert_eq!(
            reading.cache_bytes, expected,
            "{} keys x {} B of a sub-0x80 run: the cache holds {} B where 2*value+{} per entry is              {} B. Both layers charge the payload and a byte string charges by length alone, so              this is an exact law -- a mismatch means an entry was added, dropped, or the length              prefix stepped at a boundary this envelope does not know about",
            reading.keys, reading.value, reading.cache_bytes, envelope, expected
        );
    }
    for reading in readings.iter().filter(|r| !r.compressible) {
        let per_entry = reading.cache_bytes as f64 / reading.keys as f64;
        // 2.0, NOT 2.5: the response copy's 1.5x was the integer-array encoding, gone since #2279.
        let expected =
            2.0 * reading.value as f64 + if reading.value < 256 { 20.0 } else { 21.0 };
        let slack = 0.02 * reading.value as f64;
        println!(
            "    noise {} B: {per_entry:.1} B/entry against 2.0*value+21 = {expected:.1}",
            reading.value
        );
        assert!(
            (per_entry - expected).abs() < slack,
            "{} keys x {} B of incompressible bytes: {per_entry:.1} B/entry against an expected \
             {expected:.1} (frame = value, response = 1.5*value + 21 for a uniform byte \
             distribution). Outside {slack:.1} B of slack, so either the encoding changed or the \
             generator stopped producing a uniform distribution",
            reading.keys,
            reading.value
        );
    }

    // THE ENTRY COUNT IS THE SAME IN BOTH WORLDS, which is what makes the byte difference an
    // ENCODING difference rather than an extra copy. Asserted across every reading, because it is
    // the one figure that distinguishes "a third layer appeared" from "the same two layers charged
    // more", and those have nothing in common as problems.
    for reading in &readings {
        assert_eq!(
            reading.memory_fills,
            2 * reading.keys as u64,
            "{} keys x {} B ({}) filled the cache {} times, not twice per key. The two-layer \
             account below rests on exactly two fills per read: a third layer, or a layer that \
             stopped filling, changes what every byte figure here means",
            reading.keys,
            reading.value,
            if reading.compressible { "run" } else { "noise" },
            reading.memory_fills
        );
    }
}
