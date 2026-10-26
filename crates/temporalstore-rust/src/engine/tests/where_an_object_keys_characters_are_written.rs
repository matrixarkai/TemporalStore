// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW MANY TIMES THE DURABLE INDEX WRITES ONE OBJECT KEY'S CHARACTERS, AND WHAT THAT COSTS.
//!
//! WHY THIS EXISTS. The base served index costs hundreds of bytes per resident key and almost all
//! of that cost is per-object, so the standing question is which of those bytes are the OBJECT KEY
//! and how many times over it is written. "The index is big" is not an answer that can be acted on;
//! "the key's characters are written N times, so a fixed-width stand-in would remove N-1 of them"
//! is. This module answers the counting question exactly, and prices it on each of the three
//! denominators separately, because the three disagree by more than an order of magnitude and
//! quoting the wrong one is how a field figure becomes fiction.
//!
//! # THE THREE DENOMINATORS, NAMED EVERY TIME A NUMBER IS QUOTED
//!
//!   * `json_stream` -- the struct's JSON serialization. The one stream that PARTITIONS exactly: a
//!     JSON object is its entries plus two braces and the commas between them, so every byte lands
//!     under a named key. Every per-part attribution here is on this and nowhere else.
//!   * `msgpack_stream` -- the pre-compression payload the DEFAULT codec writes, pulled back out of
//!     the shipped container by decompressing it. This is the production shape, so the copy count
//!     is confirmed here as well as on the JSON.
//!   * `container` -- what lands on disk: the magic, the codec id, the stamp and a ZSTD FRAME.
//!     **A byte figure taken on this cannot name a field**, so no attribution below is taken on it.
//!     What IS taken on it is a DERIVATIVE -- how the shipped size moves when the key's width moves
//!     -- which measures the thing varied rather than attributing bytes to a field, and it is
//!     labelled as such at every appearance.
//!
//! # WHAT IT MEASURED
//!
//! THE COUNT IS TWO, uniform over every object in the corpus and identical on the JSON stream and
//! on the msgpack payload the default codec writes. The two homes:
//!
//! ```text
//!     objects/string                                     the model map's key          1.000
//!     slot_index/bucket_map/*/page_index                 the page entry's map key     1.000
//!                                                                                     -----
//!                                                                                     2.000
//! ```
//!
//! AND IT WAS THREE UNTIL THE THIRD HOME WAS CLOSED, WHICH IS THIS MODULE'S OWN PROPOSAL CLOSING.
//! The third row was `slot_index/bucket_map/*/page_index/*/object_key`, the page entry's own field,
//! at 1.000 per object. The entry stopped writing it at `SHARD_INDEX_FORMAT_VERSION` 27, when the
//! decoder began parsing the composite key the page is filed under -- and this module is where the
//! removal was CONFIRMED to have landed in the place it said the copy lived: the clause asserting
//! that term non-zero failed on the first suite run after the change, and it is inverted now, to
//! assert the entry holds no copy at all. Every surface figure below was taken at three copies and
//! is 14 B/object of key characters higher than the module now prints at the published width; the
//! arms compute the live ones, the header does not.
//!
//! THE SURFACE, at the 14-byte key the published per-object figures were taken on, fitted over 499
//! -> 1,999 -> 7,999 objects:
//!
//! ```text
//!     json_stream      500.35 B/object        42 B of key characters =  8.39%
//!     msgpack_stream   350.09 B/object        42 B of key characters = 12.00%
//!     container         38.26 B/object        compressed -- names no field
//! ```
//!
//! AND THE ROUTING RANGE DECIDES THE SHARE, SO BOTH ARE MEASURED. The table above is the range
//! `load_shard` leaves behind, `0..u32::MAX`, on which buckets came out EQUAL to objects at every
//! size -- so every per-bucket cost divided by one object. On the shipped `0..1023`, where 7,999
//! objects share 1,024 buckets at 7.81 per bucket, the bucket terms amortise and the same 42 B is a
//! share of a smaller total:
//!
//! ```text
//!     range          json_stream       msgpack_stream     key share (json / msgpack)
//!     0..u32::MAX    500.35 B/object   350.09 B/object      8.39%  /  12.00%
//!     0..1023        274.75 B/object   190.82 B/object     15.29%  /  22.01%
//! ```
//!
//! The COUNT is three on both ranges; only the total it is a share of moves. `objects` does not
//! amortise at all -- 59.03 B/object on either -- while `slot_index` falls 441.02 -> 226.35, and
//! that fall is the whole of the difference. Quoting either range without naming it is how this
//! figure would travel wrong.
//!
//! SO THE KEY IS NOT THE DOMINANT TERM ON EITHER RANGE, though it is twice the share on the one a
//! deployment runs. The byte partition says what the dominant term is: `slot_index` is **88.19%** of
//! the stream unbounded and **79.30%** on the shipped range, against `objects` at 11.80% and 20.68%.
//! Two of the three key instances live inside that `slot_index` and are 28 B of its 226 to 441. A
//! route that replaced the key with a fixed-width stand-in would move tens of bytes per object out
//! of two to five hundred; it cannot reach a per-entry figure in the tens, because the bytes are not
//! in the key.
//!
//! AND THE DUPLICATION IS ALREADY FREE WHERE IT IS PAID. One key byte cost 3.000 B of msgpack
//! payload -- the three copies, exactly -- and **0.651 B of shipped container** with keys that share
//! no structure. The frame collapsed the three copies to under two thirds of ONE, a 4.6x collapse,
//! so "stored three times" is a statement about the payload the encoder builds and not about the
//! artefact that lands. That is the finding this module is for, and it survives the third copy
//! leaving: the per-byte payload cost is the COPY COUNT, so it is 2.000 now, while the shipped
//! figure barely moves. Only shortening the key itself moves the artefact, and at the published
//! width there is nothing to shorten it to.
//!
//! # THE COUNT IS MEASURED, NOT READ OFF THE STRUCTURE
//!
//! A count derived by reading the serde attributes would be a document that rots, and it would
//! have been wrong in the case that mattered: the page entry does NOT hold the key -- `BlockIndex`
//! is `model_id`, `address`, `dirty`, `deleted` and nothing else -- and yet the page entry's
//! WRITTEN form carried `object_key`, injected from beside it by `impl Serialize for CoreIndex`. A
//! reader auditing the struct would have concluded the characters were not there; they were, twice
//! over inside the core index alone.
//!
//! AND THE SAME HAZARD NOW POINTS THE OTHER WAY, WHICH IS WHY THE RULE IS KEPT RATHER THAN RELAXED.
//! The entry's written form no longer carries the key -- so a reader auditing `BlockEntryAt`'s three
//! `serialize_field` calls would conclude correctly, by luck, and a reader auditing `BlockIndex`
//! would still be reading the wrong thing. Nothing here is classified by name either way. Each
//! instance is found by scanning the encoded bytes for the key's own characters and attributed to
//! the part it was found in.
//!
//! # TWO KEY SHAPES, BECAUSE ONE OF THEM WOULD HAVE BEEN A FIXTURE ARTEFACT
//!
//! The content figures do not care what the key's characters ARE -- three instances of a 48-byte
//! key is 144 bytes whatever the bytes spell. The SHIPPED figure cares enormously, because the
//! container is a zstd frame: a fixture whose keys are a constant run plus a sequential ordinal is
//! near-perfectly compressible, and measuring the shipped cost of key width on it reports
//! approximately zero for a reason that belongs to the fixture and not to the store. So every
//! shipped figure is taken twice:
//!
//!   * [`KeyShape::Padded`] -- a constant run plus the ordinal, which is the shape the published
//!     per-key figures were taken on. Its container figure is a LOWER bound and is reported, not
//!     asserted.
//!   * [`KeyShape::Scattered`] -- the same width, the same ordinal, and pseudorandom letters in
//!     place of the run, so the keys share no structure. Its container figure is the upper bound,
//!     and it is the one with an assertion on it.
//!
//! Production keys are neither, so the honest statement is a RANGE with both ends measured, and
//! neither end may be quoted on its own.
//!
//! # THE SCAN'S OWN CONTROLS, BECAUSE A KEY-PRESENCE SCAN IS THE EASIEST THING TO GET VACUOUSLY
//! # RIGHT
//!
//! The fixture writes every ordinal in its range EXCEPT ONE, and that hole is the negative control:
//! a key of the same length and the same shape, differing from a written neighbour in its LAST
//! CHARACTER ONLY, which was never stored. A prefix match, a substring match or a hash of the
//! leading bytes finds it and fails. Its two written neighbours are the positive control, so a scan
//! that finds nothing at all cannot read as "the characters are not durable" -- the recorded failure
//! mode from an earlier attempt, where a raw-byte scan of the shipped files found a key in none of
//! them because the container is COMPRESSED.
//!
//! # NOTHING IS A REMAINDER
//!
//! The per-part counts are asserted to SUM to the count taken on the whole stream, and the per-part
//! BYTES to sum to the stream plus its punctuation. A decomposition whose last term is "everything
//! else" can be wrong in every other term and still add up, because the last term absorbs the error
//! -- so there is no such term here. The split of a page entry into its map KEY and its field is
//! taken as a difference between two measured totals rather than by looking at either one's name,
//! because no walk over a map's values can see what its keys cost.
//!
//! THE THIRD COPY HAS SINCE BEEN REMOVED AND THE STAMP MOVED 26 -> 27 FOR IT. When this module was
//! written there were no production changes beside it and no stored shape moved; the count it
//! reports is a measurement of the index and it moved when the index did.
//!
//! rust-internal: measures this crate's own index codec, no product behaviour

use super::*;
use std::collections::BTreeMap;

const SHARD: u64 = 41;

/// The corpus the census and the width sweep run on. The copy count is a per-object property, so
/// this is sized for a stable encode rather than for a large one.
const CENSUS_KEYS: usize = 1_000;
/// The one ordinal every fixture here does NOT write. Its two neighbours ARE written and differ
/// from it in their last character only, which is what makes it a negative control rather than a
/// different-looking string. It is below the smallest corpus so that every arm withholds exactly
/// one key and `written == requested - 1` everywhere.
const WITHHELD_ORDINAL: usize = 123;
/// The key width the census is taken at.
const CENSUS_WIDTH: usize = 48;
/// The widths the derivative is taken over. All of them are at least 32 bytes and at most 255, so
/// msgpack writes every one behind a `str8` header and the JSON behind two quotes -- the
/// per-instance overhead is CONSTANT across the sweep and the slope is the key's characters alone.
const WIDTHS: [usize; 5] = [32, 40, 48, 56, 64];
/// The store sizes the per-object cost is swept over. A per-key figure taken at one point over a
/// hash table is a sawtooth, so the slope is fitted over a range and the range is published with it.
const SIZES: [usize; 3] = [500, 2_000, 8_000];
/// The shipped routing range's end bucket -- the default an operator is told to set.
///
/// WHY THIS IS A SEPARATE ARM AND NOT A DIFFERENT CONSTANT. `load_shard` leaves the range at
/// `0..u32::MAX`, on which a key's routing bucket is effectively unique: the store sweep measures
/// buckets EQUAL to objects at every size, so every per-bucket cost divides by one object. That is
/// the regime the published per-object figures were taken in, which is why this module reproduces
/// them there -- and it is NOT the regime a meta-server-managed shard runs on. On `0..1023` the
/// bucket-level terms amortise over the objects sharing a bucket, so the same key characters are a
/// LARGER share of a SMALLER total. Quoting either range without naming it is how this figure would
/// travel wrong, so both are measured and both are named.
const SHIPPED_END_BUCKET: u32 = 1023;
/// The key width the published per-key index figures were taken at: `"made-of-{item:06}"`.
const PUBLISHED_WIDTH: usize = 14;
/// The byte a [`KeyShape::Padded`] key's run is made of.
const PAD_BYTE: u8 = b'q';
/// `-` and six digits.
const TAIL_WIDTH: usize = 7;
/// An object with at most this many entries is a STRUCT and is worth descending into; one with more
/// is a COLLECTION and descending prints a line per element. Derived from the shape in hand.
const STRUCT_ENTRY_CEILING: usize = 24;

/// What a fixture key's leading bytes are made of.
///
/// The two shapes exist to bracket the one figure that depends on the key's CONTENT rather than its
/// width: what a key costs the compressed container. See the module header.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyShape {
    /// A constant run. The shape the published per-key figures were taken on, and compressible.
    Padded,
    /// Pseudorandom letters, derived from the ordinal so the fixture is reproducible between runs
    /// and between the seed and the scan.
    Scattered,
}

impl KeyShape {
    fn label(self) -> &'static str {
        match self {
            KeyShape::Padded => "padded",
            KeyShape::Scattered => "scattered",
        }
    }
}

/// The leading bytes of the key for `ordinal`, which is what a candidate is CONFIRMED against.
///
/// Derived from the ordinal rather than stored, so the scan can rebuild the expected bytes for
/// whatever ordinal it reads out of a candidate's digits -- which is what makes a near miss land in
/// its own slot instead of being counted as a hit on a neighbour.
fn pad_of(ordinal: usize, width: usize, shape: KeyShape) -> Vec<u8> {
    let pad = width - TAIL_WIDTH;
    match shape {
        KeyShape::Padded => vec![PAD_BYTE; pad],
        KeyShape::Scattered => {
            let mut state = (ordinal as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ 0xD1B5_4A32_D192_ED03;
            state |= 1;
            (0..pad)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    b'a' + (state % 26) as u8
                })
                .collect()
        }
    }
}

/// A key of exactly `width` bytes whose last six characters are `ordinal`.
///
/// The ordinal is fixed-width so that no key is a substring of another -- two distinct strings of
/// equal length cannot contain one another, which is what makes the instance count exact rather
/// than an upper bound.
fn key_text(ordinal: usize, width: usize, shape: KeyShape) -> String {
    let tail = format!("-{ordinal:06}");
    assert_eq!(tail.len(), TAIL_WIDTH, "the ordinal tail is a fixed width");
    assert!(
        width > TAIL_WIDTH,
        "a {width}-byte key has no room for a {TAIL_WIDTH}-byte ordinal tail"
    );
    let mut text = String::from_utf8(pad_of(ordinal, width, shape))
        .expect("the padding is ASCII by construction");
    text.push_str(&tail);
    assert_eq!(text.len(), width, "the key must be exactly the width asked for");
    text
}

/// Every instance of a fixture key's characters in `bytes`, counted per ORDINAL, in one pass.
///
/// A candidate is found by its hyphen-and-six-digits tail, its ordinal is read out of those digits,
/// and it is then CONFIRMED by comparing the preceding bytes against the padding that ordinal's key
/// would have. So an instance of a key that was never written lands in its OWN slot rather than
/// being counted as a near miss of one that was -- which is what lets the withheld ordinal be a
/// control instead of an absence.
fn instances_by_ordinal(
    bytes: &[u8],
    width: usize,
    ordinals: usize,
    shape: KeyShape,
) -> Vec<usize> {
    let pad = width - TAIL_WIDTH;
    let mut found = vec![0usize; ordinals];
    if bytes.len() < width {
        return found;
    }
    for at in pad..=bytes.len() - TAIL_WIDTH {
        if bytes[at] != b'-' {
            continue;
        }
        let digits = &bytes[at + 1..at + TAIL_WIDTH];
        if !digits.iter().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let mut ordinal = 0usize;
        for byte in digits {
            ordinal = ordinal * 10 + usize::from(byte - b'0');
        }
        if ordinal >= ordinals {
            continue;
        }
        if bytes[at - pad..at] != pad_of(ordinal, width, shape)[..] {
            continue;
        }
        found[ordinal] += 1;
    }
    found
}

/// Total instances of the fixture's keys in `bytes`.
fn instances(bytes: &[u8], width: usize, ordinals: usize, shape: KeyShape) -> usize {
    instances_by_ordinal(bytes, width, ordinals, shape)
        .into_iter()
        .sum()
}

/// THE PRE-COMPRESSION PAYLOAD THE DEFAULT CODEC WRITES, pulled back out of the container.
///
/// Decompressed from the shipped artefact rather than re-serialized, so it is the stream the
/// container actually holds. The frame is found by its OWN magic rather than by trusting a header
/// length, and the position is bounded so a payload byte that happens to match cannot be mistaken
/// for the frame.
fn msgpack_stream_of(container: &[u8]) -> Vec<u8> {
    const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
    let start = container
        .windows(4)
        .position(|window| window == ZSTD_MAGIC)
        .expect("the container must hold a zstd frame, or this is not the shipped artefact");
    assert!(
        start < 32,
        "the zstd magic is {start} bytes into the container, too far to be its header -- the \
         position found is probably payload that happens to match"
    );
    zstd::stream::decode_all(&container[start..]).expect("the container's frame must decompress")
}

/// One store's reading: the three totals, and the encoded forms the scan runs on.
struct Reading {
    keys: usize,
    width: usize,
    shape: KeyShape,
    /// The ordinal ceiling the scan runs to, DERIVED FROM THIS READING'S OWN CORPUS. It was a
    /// constant, and a constant is wrong the moment one arm sweeps the store size: ordinals above
    /// the ceiling are skipped, so a larger store silently reported a smaller count per object.
    /// The `size-count-stable` clause is what caught that.
    ordinals: usize,
    buckets: usize,
    container: usize,
    msgpack: Vec<u8>,
    json: Vec<u8>,
    value: serde_json::Value,
}

/// Seed `keys` string objects of the given key width and shape, and take every reading off the
/// result.
///
/// The withheld ordinal is skipped here, which is the one place the negative control is created --
/// so the control is a property of the fixture rather than of the matcher.
fn reading_at(keys: usize, width: usize, shape: KeyShape) -> Reading {
    reading_on(keys, width, shape, None)
}

/// As above, on a named routing range. `None` is the range `load_shard` leaves behind.
fn reading_on(
    keys: usize,
    width: usize,
    shape: KeyShape,
    end_routing_bucket: Option<u32>,
) -> Reading {
    assert!(
        WITHHELD_ORDINAL < keys,
        "a {keys}-key corpus does not reach the withheld ordinal {WITHHELD_ORDINAL}, so this \
         fixture carries no negative control"
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    match end_routing_bucket {
        None => engine.load_shard(SHARD),
        Some(end) => {
            let response = engine.load_shard_with(crate::control::LoadShardRequest {
                shard_id: SHARD,
                table_name: "where-an-object-keys-characters-are-written".to_string(),
                shard_uri: "local://where-an-object-keys-characters-are-written/41".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: end,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            });
            assert!(
                response.status.ok,
                "[CLAUSE seed-landed] the fixture could not load shard {SHARD} on 0..{end}: {:?}",
                response.status
            );
        }
    }

    let mut written = 0usize;
    for ordinal in 0..keys {
        if ordinal == WITHHELD_ORDINAL {
            continue;
        }
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: key_text(ordinal, width, shape),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "the seed must ack at {ordinal}: {response:?}");
        written += 1;
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    assert_eq!(
        written,
        shard.strings().len(),
        "[CLAUSE seed-landed] the fixture acked {written} string object(s) and the index holds \
         {}, so the denominator every figure below is taken against is not the corpus",
        shard.strings().len()
    );

    let container = crate::engine::encode_index_bytes(shard);
    let msgpack = msgpack_stream_of(&container);
    let json = serde_json::to_vec(shard).expect("the index must serialize as JSON");
    let value = serde_json::to_value(shard).expect("the index must serialize as a value");
    Reading {
        keys: written,
        width,
        shape,
        ordinals: keys + 2,
        buckets: shard.bucket_index.bucket_map.len(),
        container: container.len(),
        msgpack,
        json,
        value,
    }
}

impl Reading {
    fn object(&self) -> &serde_json::Map<String, serde_json::Value> {
        self.value
            .as_object()
            .expect("the served index serializes as a JSON OBJECT, or there are no parts to name")
    }

    /// Instances of the fixture's keys in one part's own serialization.
    fn instances_in(&self, value: &serde_json::Value) -> usize {
        let bytes = serde_json::to_vec(value).expect("a part serializes");
        instances(&bytes, self.width, self.ordinals, self.shape)
    }

    fn instances_everywhere(&self) -> usize {
        instances(&self.json, self.width, self.ordinals, self.shape)
    }

    /// The per-object instance count, refused rather than rounded when it is not whole.
    fn copies(&self) -> usize {
        let found = self.instances_everywhere();
        assert_eq!(
            found % self.keys,
            0,
            "[CLAUSE count-uniform] {found} instance(s) over {} object(s) is not a whole number \
             per object, so a division here would invent one",
            self.keys
        );
        found / self.keys
    }
}

/// Walk `object`, recording each struct-shaped path's instance count.
fn walk(
    prefix: &str,
    object: &serde_json::Map<String, serde_json::Value>,
    reading: &Reading,
    depth: usize,
    out: &mut BTreeMap<String, usize>,
) {
    for (name, value) in object.iter() {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        out.insert(path.clone(), reading.instances_in(value));
        if depth < 3 {
            if let serde_json::Value::Object(inner) = value {
                if inner.len() <= STRUCT_ENTRY_CEILING {
                    walk(&path, inner, reading, depth + 1, out);
                }
            }
        }
    }
}

// =================================================================================================

/// WHERE AN OBJECT KEY'S CHARACTERS ARE WRITTEN IN THE DURABLE INDEX, AND HOW MANY TIMES.
///
/// One conclusion: the instance count per object, with every part that holds one named and
/// non-zero, and the per-part counts summing to the count on the whole stream.
///
/// THE ASSERTION ORDER IS LOAD-BEARING. A plant stops at the first failing assertion, so the
/// fixture-regime clauses come first: a corpus that did not land, a matcher that matches a key that
/// was never written, and a matcher that matches nothing at all are each refused BEFORE any count
/// is read as a finding. Each message carries a `[CLAUSE name]` tag, so a plant's verdict is derived
/// from the panic text rather than from the fact that something failed.
#[test]
fn where_an_object_keys_characters_are_written_in_the_durable_index() {
    let reading = reading_at(CENSUS_KEYS, CENSUS_WIDTH, KeyShape::Padded);
    let per_ordinal = instances_by_ordinal(
        &reading.json,
        reading.width,
        reading.ordinals,
        reading.shape,
    );

    // ---- FIXTURE REGIME FIRST: the controls, before any count is a finding ----
    assert_eq!(
        per_ordinal[WITHHELD_ORDINAL], 0,
        "[CLAUSE negative-control] ordinal {WITHHELD_ORDINAL} was never written, and it differs \
         from the written ordinal {} in its LAST CHARACTER ONLY -- finding {} instance(s) of it \
         means the scan matches a prefix, a substring or a hash of the leading bytes rather than \
         the key",
        WITHHELD_ORDINAL + 1,
        per_ordinal[WITHHELD_ORDINAL]
    );
    for neighbour in [WITHHELD_ORDINAL - 1, WITHHELD_ORDINAL + 1] {
        assert!(
            per_ordinal[neighbour] > 0,
            "[CLAUSE positive-control] ordinal {neighbour} WAS written and the scan finds no \
             instance of it, so a count of zero anywhere below would say nothing about the store \
             -- this is the recorded failure where a scan of the shipped bytes found no key \
             because the container is compressed"
        );
    }
    assert_eq!(
        per_ordinal[CENSUS_KEYS], 0,
        "[CLAUSE negative-control] ordinal {CENSUS_KEYS} is outside the corpus and must not be \
         found"
    );

    // ---- THE COUNT, AND IT IS ONE NUMBER OR IT IS NOT A COUNT ----
    let mut spread: BTreeMap<usize, usize> = BTreeMap::new();
    for ordinal in 0..CENSUS_KEYS {
        if ordinal == WITHHELD_ORDINAL {
            continue;
        }
        *spread.entry(per_ordinal[ordinal]).or_default() += 1;
    }
    println!();
    println!(
        "================ INSTANCES PER OBJECT KEY, {} shape, width {} ================",
        reading.shape.label(),
        reading.width
    );
    println!("  {:>12} {:>14}", "instances", "objects");
    for (count, objects) in spread.iter() {
        println!("  {count:>12} {objects:>14}");
    }
    assert_eq!(
        spread.len(),
        1,
        "[CLAUSE count-uniform] the {} object key(s) are written {} DIFFERENT numbers of times \
         ({:?}), so there is no single per-object count and a per-key byte figure derived from one \
         would be an average wearing the name of a count",
        reading.keys,
        spread.len(),
        spread
    );
    let copies = *spread.keys().next().expect("one count");
    assert!(
        copies > 0,
        "[CLAUSE count-uniform] every object key is written 0 times, which the positive control \
         above should already have refused"
    );

    // ---- THE PARTITION: every part counted explicitly, and the sum is checked ----
    let whole = reading.instances_everywhere();
    assert_eq!(
        whole,
        copies * reading.keys,
        "[CLAUSE count-uniform] the whole-stream count {whole} is not {copies} per object over {} \
         objects",
        reading.keys
    );

    let object = reading.object();
    let mut top: Vec<(String, usize)> = object
        .iter()
        .map(|(name, value)| (name.clone(), reading.instances_in(value)))
        .collect();
    let summed: usize = top.iter().map(|(_, count)| *count).sum();
    assert_eq!(
        whole, summed,
        "[CLAUSE part-sum] the {} top-level part(s) hold {summed} instance(s) against {whole} on \
         the whole stream, so a part was counted twice or not at all -- and a difference here is \
         exactly the error an \"everything else\" column would have absorbed",
        object.len()
    );

    top.sort_by(|left, right| right.1.cmp(&left.1));
    let holders: Vec<&(String, usize)> = top.iter().filter(|(_, count)| *count > 0).collect();
    println!();
    println!("================ WHICH PARTS HOLD THEM (json_stream) ================");
    println!(
        "  {} top-level part(s) in all, {} of them holding a key instance",
        top.len(),
        holders.len()
    );
    println!(
        "  {:<34} {:>12} {:>14} {:>12}",
        "part", "instances", "per object", "% of all"
    );
    for (name, count) in holders.iter() {
        println!(
            "  {name:<34} {count:>12} {:>14.3} {:>11.1}%",
            *count as f64 / reading.keys as f64,
            100.0 * *count as f64 / whole as f64
        );
    }
    assert!(
        !holders.is_empty(),
        "[CLAUSE part-sum] no top-level part holds a key instance while the whole stream holds \
         {whole}"
    );
    for (name, count) in holders.iter() {
        assert!(
            *count > 0,
            "[CLAUSE each-term-nonzero] `{name}` is in the holders class with {count} \
             instance(s), so the class is not what its name says"
        );
    }

    // ---- THE DRILL-DOWN, BY THE SAME RULE ----
    let mut paths = BTreeMap::new();
    walk("", object, &reading, 0, &mut paths);
    let deep_total = paths.len();
    let mut deep: Vec<(&String, &usize)> = paths.iter().filter(|(_, count)| **count > 0).collect();
    deep.sort_by(|left, right| right.1.cmp(left.1));
    println!();
    println!("================ EVERY STRUCT-SHAPED PATH THAT HOLDS ONE ================");
    println!(
        "  {deep_total} path(s) walked, {} of them holding a key instance",
        deep.len()
    );
    println!("  {:<54} {:>12} {:>14}", "path", "instances", "per object");
    for (path, count) in deep.iter() {
        println!(
            "  {path:<54} {count:>12} {:>14.3}",
            **count as f64 / reading.keys as f64
        );
    }

    // ---- ONE PAGE INDEX, SPLIT INTO ITS MAP KEYS AND ITS FIELDS BY SUBTRACTION ----
    //
    // THE SPLIT IS A DIFFERENCE BETWEEN TWO MEASURED TOTALS, not a reading of either one's name: a
    // map's instances are its keys' plus its values', and the values are counted directly, so the
    // keys' share is what is left. That is the only way to price a map KEY, which no walk over
    // values can see.
    let slot_index = object
        .get("slot_index")
        .expect("the core index is written under `slot_index`");
    let bucket_map = slot_index
        .as_object()
        .expect("the core index writes an object")
        .get("bucket_map")
        .expect("the core index writes `bucket_map`")
        .as_object()
        .expect("the bucket map writes an object");
    assert_eq!(
        bucket_map.len(),
        reading.buckets,
        "[CLAUSE part-sum] the written bucket map holds {} node(s) against {} resident",
        bucket_map.len(),
        reading.buckets
    );

    let mut node_counts: BTreeMap<usize, usize> = BTreeMap::new();
    let mut block_key_instances = 0usize;
    let mut block_field_instances = 0usize;
    // THE COMPOSITE AROUND THE CHARACTERS, counted beside them. A page entry is filed under a
    // COMPOSED name -- the kind, the object key and five address terms -- so the key's characters
    // are only part of what naming the page costs. A change replacing the key with a fixed-width
    // stand-in would move the whole composite, not just the characters inside it, and a report
    // counting only the characters would understate it.
    let mut block_name_bytes = 0usize;
    let mut block_name_key_chars = 0usize;
    for node in bucket_map.values() {
        *node_counts.entry(reading.instances_in(node)).or_default() += 1;
        let page_index = node
            .as_object()
            .expect("a bucket node writes an object")
            .get("page_index")
            .expect("a bucket node writes `page_index`");
        let in_whole_map = reading.instances_in(page_index);
        let blocks = page_index
            .as_object()
            .expect("the page index writes an object");
        let in_values: usize = blocks.values().map(|entry| reading.instances_in(entry)).sum();
        assert!(
            in_whole_map >= in_values,
            "[CLAUSE part-sum] a page index holds {in_whole_map} instance(s) while its entries \
             alone hold {in_values}, so the map's own serialization is not its keys plus its \
             values and the subtraction below is not a split"
        );
        for name in blocks.keys() {
            block_name_bytes += serde_json::to_vec(name).expect("a name serializes").len();
            block_name_key_chars += instances(
                name.as_bytes(),
                reading.width,
                reading.ordinals,
                reading.shape,
            ) * reading.width;
        }
        block_field_instances += in_values;
        block_key_instances += in_whole_map - in_values;
        // THE CHARACTERS MOVED RATHER THAN LEFT, PER BUCKET. The entry's copy is gone, so a page
        // index's whole count must now be its KEYS' count -- asserted here, bucket by bucket,
        // because a total asserted once could be satisfied by some buckets holding a field copy and
        // others holding no key at all.
        assert_eq!(
            in_whole_map, in_whole_map - in_values,
            "[CLAUSE the-entry-holds-no-copy] a page index holds {in_whole_map} key instance(s) of \
             which {in_values} are inside its ENTRIES, so this bucket's entries still carry a copy \
             of the object key"
        );
    }
    println!();
    println!("================ INSIDE THE BUCKET MAP ================");
    println!("  {:>12} {:>14}", "instances", "bucket nodes");
    for (count, nodes) in node_counts.iter() {
        println!("  {count:>12} {nodes:>14}");
    }
    println!(
        "  page-entry MAP KEYS       {block_key_instances:>8} instance(s), {:.3} per object",
        block_key_instances as f64 / reading.keys as f64
    );
    println!(
        "  page-entry `object_key`   {block_field_instances:>8} instance(s), {:.3} per object",
        block_field_instances as f64 / reading.keys as f64
    );
    assert!(
        block_key_instances > 0,
        "[CLAUSE each-term-nonzero] the page-entry map keys hold no key characters, so this term \
         is a zero that would have been absorbed into another"
    );
    // AND THIS TERM IS NOW EXACTLY ZERO, WHICH IS A SUBJECT CHANGE AND NOT A RELAXED CLAUSE.
    //
    // It read `block_field_instances > 0` under `each-term-nonzero`, and it FAILED on the first
    // suite run after the page entry stopped writing its own copy of the object key -- which is
    // how the removal was confirmed to have landed where this module says the copy lived. The
    // clause is INVERTED rather than deleted: the term has to be zero now, and a non-zero here
    // would mean the field came back.
    //
    // THE NON-ZERO DISCIPLINE MOVES ONTO THE IDENTITY BESIDE IT. A term asserted zero says nothing
    // about whether the characters are still durable, so the two statements that carry the weight
    // are that the map KEYS hold them (asserted above) and that the page index's instances are now
    // ENTIRELY in its keys -- the characters moved, they did not leave the index.
    assert_eq!(
        0, block_field_instances,
        "[CLAUSE the-entry-holds-no-copy] the page entries hold {block_field_instances} instance(s) \
         of an object key's characters. `object_key` left the entry at \
         `SHARD_INDEX_FORMAT_VERSION` 27 because the composite key the entry is filed under \
         already spells it, so a copy on the entry means the field has come back"
    );
    println!(
        "  page-entry names          {block_name_bytes:>8} B in all, {:.2} B/object as written",
        block_name_bytes as f64 / reading.keys as f64
    );
    println!(
        "    of which the object key's characters are {block_name_key_chars} B ({:.1}%); the rest \
         is the kind and the five address terms the composite also names",
        100.0 * block_name_key_chars as f64 / block_name_bytes as f64
    );
    assert!(
        block_name_key_chars > 0 && block_name_key_chars < block_name_bytes,
        "[CLAUSE each-term-nonzero] the page-entry names are {block_name_bytes} B of which \
         {block_name_key_chars} B are key characters -- a share of none says the composite does not \
         name the object, a share of all says it names nothing else, and neither matches a \
         composed name"
    );

    // ---- AND THE SAME COUNT ON THE PRODUCTION PAYLOAD ----
    let in_msgpack = instances(
        &reading.msgpack,
        reading.width,
        reading.ordinals,
        reading.shape,
    );
    assert_eq!(
        in_msgpack, whole,
        "[CLAUSE production-payload] the JSON stream holds {whole} key instance(s) and the \
         msgpack payload the default codec actually writes holds {in_msgpack} -- the count is a \
         property of the index's content and must not depend on which codec renders it"
    );

    println!();
    println!("================ THE ANSWER ================");
    println!(
        "  ONE OBJECT KEY'S CHARACTERS ARE WRITTEN {copies} TIME(S) PER OBJECT, on the JSON \
         stream and on the msgpack payload alike."
    );
    println!(
        "  At a {}-byte key that is {} B/object of key characters, against json_stream {:.2} \
         B/object = {:.2}%.",
        reading.width,
        copies * reading.width,
        reading.json.len() as f64 / reading.keys as f64,
        100.0 * (copies * reading.width) as f64
            / (reading.json.len() as f64 / reading.keys as f64)
    );
    println!();
}

// =================================================================================================

/// WHAT ONE BYTE OF AN OBJECT KEY COSTS THE DURABLE INDEX, ON EACH OF THE THREE DENOMINATORS.
///
/// One conclusion: the marginal cost of key width, measured by VARYING the width rather than by
/// attributing bytes -- which is why it is the one figure that may legitimately be taken on the
/// compressed container.
///
/// THE EXACT CLAUSE AND THE TOLERANT ONE ARE BOTH HERE, AND THE EXACT ONE IS NOT DERIVED FROM THE
/// TOLERANT ONE. The census count at each width is an integer and must be the SAME integer at every
/// width and in both shapes -- that is exact. The fitted slope is a float over a store whose bucket
/// occupancy moves a little with the key text, so it carries a tolerance; a tolerance wide enough
/// to survive that movement is wider than some drifts worth catching, so it is a cross-check on the
/// exact clause and never a substitute for it.
///
/// AND THE CONTAINER IS ASSERTED ON THE SCATTERED SHAPE ONLY. The padded shape's keys share a
/// constant run and a sequential ordinal, so the frame compresses them to nearly nothing and its
/// slope is a property of the fixture. It is printed as the lower bound and carries no assertion;
/// the scattered shape carries it.
#[test]
fn what_one_byte_of_an_object_key_costs_the_durable_index() {
    let mut counts: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    let mut fits: Vec<(KeyShape, f64, f64, f64, usize, usize, usize)> = Vec::new();

    for shape in [KeyShape::Padded, KeyShape::Scattered] {
        let readings: Vec<Reading> = WIDTHS
            .into_iter()
            .map(|width| reading_at(CENSUS_KEYS, width, shape))
            .collect();
        println!();
        println!(
            "================ THE WIDTH SWEEP, {} OBJECTS, {} KEYS ================",
            readings[0].keys,
            shape.label()
        );
        println!(
            "  {:>8} {:>10} {:>14} {:>16} {:>14} {:>12}",
            "width", "buckets", "json_stream", "msgpack_stream", "container", "instances"
        );
        for reading in readings.iter() {
            let copies = reading.copies();
            counts
                .entry(copies)
                .or_default()
                .push(format!("{} w{}", shape.label(), reading.width));
            println!(
                "  {:>8} {:>10} {:>14} {:>16} {:>14} {:>12}",
                reading.width,
                reading.buckets,
                reading.json.len(),
                reading.msgpack.len(),
                reading.container,
                copies
            );
        }
        let first = &readings[0];
        let last = &readings[readings.len() - 1];
        let span = (last.width - first.width) as f64 * first.keys as f64;
        fits.push((
            shape,
            (last.json.len() as f64 - first.json.len() as f64) / span,
            (last.msgpack.len() as f64 - first.msgpack.len() as f64) / span,
            (last.container as f64 - first.container as f64) / span,
            last.width,
            last.json.len() / last.keys,
            last.container / last.keys,
        ));
    }

    // ---- THE EXACT CLAUSE ----
    assert_eq!(
        counts.len(),
        1,
        "[CLAUSE width-count-stable] the per-object instance count is not one number across the \
         sweep -- {counts:?} -- so the index stores a key a number of times that depends on how \
         long the key is or on what it spells, and no single slope describes it"
    );
    let copies = *counts.keys().next().expect("one count");

    println!();
    println!("================ WHAT ONE KEY BYTE COSTS, PER OBJECT ================");
    println!(
        "  {:<12} {:>16} {:>18} {:>16}",
        "key shape", "json_stream", "msgpack_stream", "container"
    );
    for (shape, json_slope, msgpack_slope, container_slope, _, _, _) in fits.iter() {
        println!(
            "  {:<12} {json_slope:>16.3} {msgpack_slope:>18.3} {container_slope:>16.3}",
            shape.label()
        );
    }
    println!(
        "  the census counts {copies} instance(s) per object, so the two content columns are the \
         same quantity reached a second way; the container column is a DERIVATIVE on a compressed \
         frame and names no field"
    );

    for (shape, json_slope, msgpack_slope, container_slope, width, json_per, container_per) in
        fits.iter()
    {
        assert!(
            (json_slope - copies as f64).abs() < 0.25,
            "[CLAUSE derivative-agrees] with {} keys the JSON stream grows {json_slope:.3} B per \
             key byte per object while the census counts {copies} instance(s) per object -- two \
             independent methods on the same quantity disagree, so one of them is measuring \
             something else",
            shape.label()
        );
        assert!(
            (msgpack_slope - copies as f64).abs() < 0.25,
            "[CLAUSE derivative-agrees] with {} keys the msgpack payload grows \
             {msgpack_slope:.3} B per key byte per object against a census of {copies}",
            shape.label()
        );
        println!();
        println!(
            "  {} keys at width {width}: key characters {} B/object against json_stream \
             {json_per} B/object = {:.1}%",
            shape.label(),
            copies * width,
            100.0 * (copies * width) as f64 / *json_per as f64
        );
        println!(
            "    shipped {container_per} B/object; the width-dependent part of it is \
             {:.2} B/object = {:.1}% -- a DERIVATIVE, stated as such",
            container_slope * *width as f64,
            100.0 * container_slope * *width as f64 / *container_per as f64
        );
        if *shape == KeyShape::Scattered {
            assert!(
                *container_slope > 0.5,
                "[CLAUSE scattered-container] with keys that share no structure the shipped \
                 container grows only {container_slope:.3} B per key byte per object -- a stream \
                 storing the characters {copies} time(s) cannot compress unrelated keys away this \
                 well, so this is a measurement fault rather than a finding"
            );
        } else {
            println!(
                "    NOT ASSERTED for the padded shape: a constant run plus a sequential ordinal \
                 compresses, so this end is a lower bound set by the fixture."
            );
        }
    }
    println!();
}

// =================================================================================================

/// THE PER-OBJECT INDEX COST OVER A SWEPT STORE, AND THE KEY'S SHARE OF IT AT THE PUBLISHED WIDTH.
///
/// One conclusion: the slope and the constant of each of the three totals, at the key width the
/// published per-key figures were taken at, so the key's share is quoted against the same surface
/// those figures were -- together with the byte partition that says what the rest of the slope is.
///
/// A PER-KEY FIGURE OVER A HASH TABLE IS A SAWTOOTH, so this fits a slope over a swept range rather
/// than dividing one reading, and it prints every point it fitted.
#[test]
fn the_per_object_index_cost_and_the_keys_share_of_it() {
    let shape = KeyShape::Padded;
    let readings: Vec<Reading> = SIZES
        .into_iter()
        .map(|keys| reading_at(keys, PUBLISHED_WIDTH, shape))
        .collect();

    println!();
    println!(
        "================ THE STORE SWEEP AT THE PUBLISHED {PUBLISHED_WIDTH}-BYTE KEY, {} KEYS \
         ================",
        shape.label()
    );
    println!(
        "  {:>8} {:>10} {:>14} {:>16} {:>14} {:>12}",
        "objects", "buckets", "json_stream", "msgpack_stream", "container", "instances"
    );
    let mut counts: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for reading in readings.iter() {
        let copies = reading.copies();
        counts.entry(copies).or_default().push(reading.keys);
        println!(
            "  {:>8} {:>10} {:>14} {:>16} {:>14} {:>12}",
            reading.keys,
            reading.buckets,
            reading.json.len(),
            reading.msgpack.len(),
            reading.container,
            copies
        );
    }
    assert_eq!(
        counts.len(),
        1,
        "[CLAUSE size-count-stable] the per-object instance count moves with the store size \
         ({counts:?}), so it is not a per-object property and the share below has no single value"
    );
    let copies = *counts.keys().next().expect("one count");

    let small = &readings[0];
    let large = &readings[readings.len() - 1];
    let span = (large.keys - small.keys) as f64;
    let fit = |small_bytes: usize, large_bytes: usize| -> (f64, f64) {
        let slope = (large_bytes as f64 - small_bytes as f64) / span;
        (slope, small_bytes as f64 - slope * small.keys as f64)
    };
    let (json_slope, json_constant) = fit(small.json.len(), large.json.len());
    let (msgpack_slope, msgpack_constant) = fit(small.msgpack.len(), large.msgpack.len());
    let (container_slope, container_constant) = fit(small.container, large.container);

    println!();
    println!("================ THE SLOPE AND THE CONSTANT ================");
    println!(
        "  json_stream      slope={json_slope:>8.2} B/object  constant={json_constant:>10.0} B"
    );
    println!(
        "  msgpack_stream   slope={msgpack_slope:>8.2} B/object  \
         constant={msgpack_constant:>10.0} B"
    );
    println!(
        "  container        slope={container_slope:>8.2} B/object  \
         constant={container_constant:>10.0} B   (COMPRESSED -- names no field)"
    );
    assert!(
        json_slope > 0.0 && msgpack_slope > 0.0 && container_slope > 0.0,
        "[CLAUSE slope-positive] a total that does not grow with the store contradicts the \
         per-object cost this measurement exists to decompose: json {json_slope:.2}, msgpack \
         {msgpack_slope:.2}, container {container_slope:.2}"
    );

    let key_bytes = (copies * PUBLISHED_WIDTH) as f64;
    println!();
    println!("================ THE KEY'S SHARE AT THE PUBLISHED WIDTH ================");
    println!(
        "  {copies} instance(s) x {PUBLISHED_WIDTH} B = {key_bytes:.0} B/object of key characters"
    );
    println!(
        "  against json_stream    {json_slope:.2} B/object  = {:.2}%",
        100.0 * key_bytes / json_slope
    );
    println!(
        "  against msgpack_stream {msgpack_slope:.2} B/object  = {:.2}%",
        100.0 * key_bytes / msgpack_slope
    );
    println!(
        "  the container's share is NOT computed here: a byte figure on a compressed frame cannot \
         name a field, and the only legitimate figure for it is the width DERIVATIVE, which \
         `what_one_byte_of_an_object_key_costs_the_durable_index` takes on two key shapes."
    );

    // ---- AND WHERE THE REST OF THE BYTES ARE, ON THE ONE STREAM THAT PARTITIONS EXACTLY ----
    //
    // A JSON object is its entries plus two braces and the commas between them, so a per-part byte
    // figure here is EXACT, and the identity below is what says no byte went unnamed. This sits
    // beside the instance counts rather than instead of them: "how many bytes are the key" is only
    // answerable against a total whose other terms are named too, and a share quoted against a
    // short denominator is the recorded way this goes wrong.
    let object = large.object();
    let entry_bytes = |name: &str, value: &serde_json::Value| -> usize {
        serde_json::to_vec(name).expect("a name serializes").len()
            + 1
            + serde_json::to_vec(value).expect("a value serializes").len()
    };
    let mut parts: Vec<(String, usize, usize)> = object
        .iter()
        .map(|(name, value)| (name.clone(), entry_bytes(name, value), large.instances_in(value)))
        .collect();
    let named: usize = parts.iter().map(|(_, bytes, _)| *bytes).sum();
    let punctuation = 2 + object.len().saturating_sub(1);
    assert_eq!(
        large.json.len(),
        named + punctuation,
        "[CLAUSE part-sum] the per-part byte partition sums to {named} + {punctuation} \
         punctuation against a {}-byte stream, so some bytes are unattributed and every share \
         quoted above would be taken against a short denominator",
        large.json.len()
    );
    parts.sort_by(|left, right| right.1.cmp(&left.1));
    println!();
    println!(
        "================ WHERE THE BYTES ARE AT {} OBJECTS (json_stream) ================",
        large.keys
    );
    println!(
        "  {} top-level part(s) partition {} B exactly; the rows below are those holding a key \
         instance or more than 64 B",
        parts.len(),
        large.json.len()
    );
    println!(
        "  {:<34} {:>14} {:>12} {:>12} {:>14}",
        "part", "bytes", "B/object", "% stream", "key instances"
    );
    for (name, bytes, found) in parts.iter() {
        if *bytes <= 64 && *found == 0 {
            continue;
        }
        println!(
            "  {name:<34} {bytes:>14} {:>12.2} {:>11.2}% {found:>14}",
            *bytes as f64 / large.keys as f64,
            100.0 * *bytes as f64 / large.json.len() as f64
        );
    }
    println!();
}


// =================================================================================================

/// THE SAME QUESTION ON THE SHIPPED ROUTING RANGE, WHERE THE BUCKET TERMS AMORTISE.
///
/// One conclusion: the key's share of the per-object cost on `0..1023`, beside the same share on the
/// range `load_shard` leaves behind -- because the two differ by enough that a figure quoted without
/// its range is not a figure.
///
/// THE REGIME IS ASSERTED, NOT ASSUMED. An arm that exists to measure amortisation and silently
/// fails to produce any is a probe that never entered the regime it is arguing about, and its number
/// would read as a result. So the first clause here refuses a fixture in which no bucket holds more
/// than one object.
#[test]
fn the_keys_share_of_the_per_object_cost_on_the_shipped_routing_range() {
    let shape = KeyShape::Padded;
    let readings: Vec<Reading> = SIZES
        .into_iter()
        .map(|keys| reading_on(keys, PUBLISHED_WIDTH, shape, Some(SHIPPED_END_BUCKET)))
        .collect();

    println!();
    println!(
        "================ THE STORE SWEEP ON 0..{SHIPPED_END_BUCKET}, {PUBLISHED_WIDTH}-BYTE \
         {} KEYS ================",
        shape.label()
    );
    println!(
        "  {:>8} {:>10} {:>12} {:>14} {:>16} {:>14} {:>11}",
        "objects", "buckets", "obj/bucket", "json_stream", "msgpack_stream", "container",
        "instances"
    );
    let mut counts: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for reading in readings.iter() {
        let copies = reading.copies();
        counts.entry(copies).or_default().push(reading.keys);
        println!(
            "  {:>8} {:>10} {:>12.2} {:>14} {:>16} {:>14} {:>11}",
            reading.keys,
            reading.buckets,
            reading.keys as f64 / reading.buckets as f64,
            reading.json.len(),
            reading.msgpack.len(),
            reading.container,
            copies
        );
    }

    // ---- THE REGIME CLAUSE FIRST: this arm exists for amortisation, so it must have some ----
    let large = &readings[readings.len() - 1];
    assert!(
        large.buckets < large.keys,
        "[CLAUSE amortised-regime] the shipped-range fixture put {} object(s) in {} bucket(s), so \
         no bucket holds more than one and this arm is measuring the same regime as the unbounded \
         one -- the amortisation it exists to price never happened",
        large.keys,
        large.buckets
    );
    assert_eq!(
        counts.len(),
        1,
        "[CLAUSE size-count-stable] the per-object instance count moves with the store size \
         ({counts:?}) on the shipped range, so it is not a per-object property here either"
    );
    let copies = *counts.keys().next().expect("one count");

    let small = &readings[0];
    let span = (large.keys - small.keys) as f64;
    let fit = |small_bytes: usize, large_bytes: usize| -> (f64, f64) {
        let slope = (large_bytes as f64 - small_bytes as f64) / span;
        (slope, small_bytes as f64 - slope * small.keys as f64)
    };
    let (json_slope, json_constant) = fit(small.json.len(), large.json.len());
    let (msgpack_slope, msgpack_constant) = fit(small.msgpack.len(), large.msgpack.len());
    let (container_slope, container_constant) = fit(small.container, large.container);

    println!();
    println!("================ THE SLOPE AND THE CONSTANT ON THE SHIPPED RANGE ================");
    println!(
        "  json_stream      slope={json_slope:>8.2} B/object  constant={json_constant:>10.0} B"
    );
    println!(
        "  msgpack_stream   slope={msgpack_slope:>8.2} B/object  \
         constant={msgpack_constant:>10.0} B"
    );
    println!(
        "  container        slope={container_slope:>8.2} B/object  \
         constant={container_constant:>10.0} B   (COMPRESSED -- names no field)"
    );
    assert!(
        json_slope > 0.0 && msgpack_slope > 0.0,
        "[CLAUSE slope-positive] a content total that does not grow with the store contradicts \
         the per-object cost this arm decomposes: json {json_slope:.2}, msgpack {msgpack_slope:.2}"
    );

    let key_bytes = (copies * PUBLISHED_WIDTH) as f64;
    println!();
    println!("================ THE KEY'S SHARE, BY RANGE ================");
    println!(
        "  {copies} instance(s) x {PUBLISHED_WIDTH} B = {key_bytes:.0} B/object of key characters, \
         on either range -- the COUNT does not depend on the range, only the TOTAL it is a share of"
    );
    println!(
        "  on 0..{SHIPPED_END_BUCKET}: json_stream {json_slope:.2} B/object = {:.2}%, \
         msgpack_stream {msgpack_slope:.2} B/object = {:.2}%",
        100.0 * key_bytes / json_slope,
        100.0 * key_bytes / msgpack_slope
    );
    println!(
        "  the unbounded range is measured by \
         `the_per_object_index_cost_and_the_keys_share_of_it`; read the two together or neither."
    );

    // ---- AND THE BYTE PARTITION, SO THE SMALLER TOTAL'S TERMS ARE NAMED TOO ----
    let object = large.object();
    let entry_bytes = |name: &str, value: &serde_json::Value| -> usize {
        serde_json::to_vec(name).expect("a name serializes").len()
            + 1
            + serde_json::to_vec(value).expect("a value serializes").len()
    };
    let mut parts: Vec<(String, usize, usize)> = object
        .iter()
        .map(|(name, value)| (name.clone(), entry_bytes(name, value), large.instances_in(value)))
        .collect();
    let named: usize = parts.iter().map(|(_, bytes, _)| *bytes).sum();
    let punctuation = 2 + object.len().saturating_sub(1);
    assert_eq!(
        large.json.len(),
        named + punctuation,
        "[CLAUSE part-sum] the per-part byte partition sums to {named} + {punctuation} \
         punctuation against a {}-byte stream on the shipped range",
        large.json.len()
    );
    parts.sort_by(|left, right| right.1.cmp(&left.1));
    println!();
    println!(
        "================ WHERE THE BYTES ARE AT {} OBJECTS ON 0..{SHIPPED_END_BUCKET} \
         ================",
        large.keys
    );
    println!(
        "  {} top-level part(s) partition {} B exactly",
        parts.len(),
        large.json.len()
    );
    println!(
        "  {:<34} {:>14} {:>12} {:>12} {:>14}",
        "part", "bytes", "B/object", "% stream", "key instances"
    );
    for (name, bytes, found) in parts.iter() {
        if *bytes <= 64 && *found == 0 {
            continue;
        }
        println!(
            "  {name:<34} {bytes:>14} {:>12.2} {:>11.2}% {found:>14}",
            *bytes as f64 / large.keys as f64,
            100.0 * *bytes as f64 / large.json.len() as f64
        );
    }
    println!();
}
