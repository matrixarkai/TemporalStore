// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE 226 BYTES PER OBJECT INSIDE `slot_index` ARE, TERM BY TERM.
//!
//! WHY THIS EXISTS. On the routing range a deployment runs, `0..1023`, `slot_index` is 226.35 B per
//! stored object and 79.30% of the served index on the one stream that partitions exactly -- both
//! taken as a division at 8,000 objects, which is the quantity named throughout and not the 274.75
//! B/object SLOPE published beside it for the whole stream. That share was established without
//! anything being said about what is inside it. "The index is big and `slot_index` is most of it" is
//! not a number anyone can act on. This module names every byte of the 226, says how each term
//! scales, says which terms are reconstructible from other terms, and prices the removals on the
//! artefact that lands rather than only on the stream that names fields.
//!
//! # THE THREE SURFACES, NAMED AT EVERY NUMBER
//!
//!   * `json_stream` -- the struct's JSON serialization. The one stream that PARTITIONS exactly: a
//!     JSON object is one opening brace plus, per entry, its quoted name, a colon, its value and
//!     one separator byte (a comma, or the closing brace for the last). Every per-term attribution
//!     here is on this and nowhere else.
//!   * `msgpack_payload` -- the pre-compression payload the default codec writes, pulled back out
//!     of the shipped container by decompressing it. Reported as a total; no term is attributed on
//!     it, because this module's partition identity is a JSON identity.
//!   * `container` -- what lands on disk: `TSIDX\x01`, the codec id, the stamp, and a ZSTD FRAME.
//!     **A byte figure on this cannot name a field.** What is taken on it is a DIFFERENCE between
//!     two whole artefacts -- the index as written and the index with one term removed -- which
//!     measures the thing varied instead of attributing bytes, and is labelled as such everywhere.
//!
//! # WHAT IT MEASURED
//!
//! THE 50 CHARGED TERMS SUM TO 1,810,793 B AT 8,000 OBJECTS IN 1,024 BUCKETS, which is 226.349
//! B/object and 79.30% of the 285.43 B/object stream -- the published figures, reproduced by a
//! partition rather than by a division of the whole. Nothing is a remainder and nothing is unnamed.
//! 51 terms are DECLARED; the 51st is `node/tombstone_elements`, the one key the encoder writes
//! only for a bucket that has had a removal, and its absence from this fixture is asserted rather
//! than tolerated. Nine of the 51 are redundancy, each with a named reconstructor and each
//! reconstruction asserted on every page or bucket of the corpus:
//!
//! ```text
//!     payload      407,144 B     50.893 B/object    22.48%
//!     identity     654,694 B     81.837 B/object    36.16%
//!     envelope     293,725 B     36.716 B/object    16.22%
//!     redundancy   455,230 B     56.904 B/object    25.14%
//! ```
//!
//! AND THE PUBLISHED 226.35 IS A DIVISION AT 8,000 OBJECTS, NOT A SLOPE. On this range those are
//! different quantities and the difference is not small: `slot_index` grows 273.87 B/object over
//! 500 -> 2,000 objects and 201.07 over 2,000 -> 8,000, because the bucket count SATURATES at
//! 1,024. A figure quoted without saying which of the two it is travels wrong, so both are measured
//! here, the division is the one pinned against the published value, and the arm says which is
//! which.
//!
//! THE WHOLE OF IT IS THREE SCALES AND NOT TWO:
//!
//! ```text
//!     per-page      1,416,120 B    177.015 B/object    78.20%    linear in the store
//!     per-bucket      394,644 B     49.331 B/object    21.79%    212.38 -> 49.33 as buckets saturate
//!     per-shard            29 B      0.004 B/object     0.00%    two field names and three braces
//! ```
//!
//! A taxonomy of "grows" and "bounded" has nowhere to put the middle class, and the middle class is
//! the whole of the difference between this range's figure and the unbounded one: it is the only
//! class that is not a line, and the three classes are asserted to account for the whole of each
//! half of the sweep exactly.
//!
//! AND THE CLASS OF EVERY TERM IS MEASURED, NEVER READ OFF ITS NAME. `slot_index/bucket_map` is
//! bucket-keyed and holds one entry per BUCKET; the `page_index` inside each of those holds one
//! entry per PAGE, 7.81 of them per bucket on this range -- so a name-based reading of the
//! structure under `bucket_map` is wrong by that factor. Every term declares a scale and the sweep
//! asserts it against the measured instance count at three store sizes: all 50 held, exactly.
//!
//! EVERY PER-PAGE FACT IS WRITTEN TWICE, AND THE TWO COPIES ARE NOT THE SAME PRICE. A page's
//! object, kind and address are written once inside its composite map key --
//! `{kind}:{object_key}:{component}:{slab}:{offset}:{length}:{block_id}:{generation}` -- and once
//! as the `object_key`, `model_id` and `address` fields of the entry filed under it:
//!
//! ```text
//!     the composite MAP KEY           41.008 B/object    18.12% of `slot_index`
//!     the ENTRY's three fields       101.007 B/object    44.62%
//!                                    ---------------    ------
//!                                    142.015 B/object    62.74%
//! ```
//!
//! The same five address values are 12.008 B/object as decimals inside the key and 53.008 B/object
//! as the named `address` object beside it -- 4.41x, and every byte of the difference is field
//! names, braces, separators and a nil slot rather than a value. JSON field names are 94.099
//! B/object of the 226, 41.57% of the whole.
//!
//! THE RECONSTRUCTION IS ASSERTED, NOT ARGUED. The key is rebuilt from the entry's OWN WRITTEN
//! BYTES -- the address decoded back through its production `Deserialize`, the string re-formatted
//! by this module rather than by `block_index_written_key`, which would have compared one renderer
//! with itself -- and matched character for character on all 8,000 pages. `node/routing_slot` is
//! rebuilt from the map key its bucket is filed under, on all 1,024.
//!
//! AND THE KEY'S CHARACTERS ARE NOT READ BACK. The decoder takes the page index as a
//! `BTreeMap<String, StoredPageEntry>` and consumes it with `into_values()`, so what the map key
//! spells reaches nothing. Measured rather than read off the source: every page key in a 2,000-key
//! index is replaced by the decimal of its handle, fed to the production decoder, and re-encoded --
//! and the `slot_index` that comes back is byte-identical to the undoctored one's, with the
//! instrument proved able to fire by a one-character change to one entry's `object_key`.
//!
//! AND THE LARGEST PER-BUCKET TERM RESTATES THE PAGES BESIDE IT. `node/object_index` is 22.603
//! B/object, second only to the entry's `object_key`, and it holds the ids of the objects a bucket
//! carries. Those ids are a pure function of the shard, the kind and the object key -- all three of
//! which the page entries in the SAME BUCKET write -- so on all 1,024 buckets the written
//! `object_index` is EXACTLY the set derivable from its own pages. Derived here rather than through
//! `engine::hashing::stable_block_object_id`, which bumps two process-wide atomics per call in a
//! test build that other arms read and reset: a probe calling it 8,000 times would move a counter
//! it is not measuring.
//!
//! AND TWO SLOTS ARE WRITTEN FOR NOTHING ON EVERY PAGE. The composite's component slot is empty on
//! all 8,000 pages -- `block_index_written_key` passes a literal `None` -- so one of its seven
//! separators delimits a hole. And a stored address writes `oi` as null on all 8,000, because
//! `impl From<ElementEntry> for BlockAddressWire` fills it from a literal `None` too -- the type it
//! converts from has no such field to read: 9.000 B/object of JSON for a slot the only conversion
//! that builds one cannot fill.
//!
//! # WHAT A REMOVAL IS WORTH, AND THE BIGGEST-LOOKING ONE IS WORTH LESS THAN NOTHING
//!
//! Duplication is not byte cost: the container is a zstd frame, and a term's weight there is not
//! its weight on the stream. Every removal is therefore priced on both, at two key shapes and two
//! store sizes -- four points -- and the direction of each headline price is asserted at all four.
//!
//! ```text
//!     removal                          stream B/obj     container B/obj (padded -> scattered)
//!     page key -> the handle                 19.6           -5.14 to -6.77      COSTS
//!     page key -> drop the empty slot         1.0           +0.12 to -0.71      noise
//!     address -> drop the nil `oi`           10.0           +0.06 to +0.29      ~free
//!     entry -> drop `object_key`             30.0           +2.34 to +2.96
//!     entry -> drop `model_id`               20.0           +0.26 to +0.37
//!     entry -> drop `address`                54.0           +4.27 to +4.52
//!     entry -> drop all three                104.0          +6.72 to +7.17      PAYS
//! ```
//!
//! THE COMPOSITE KEY IS ALREADY FREE WHERE IT IS PAID. It is the most duplicated structure in the
//! index and also the most COMPRESSIBLE one -- sorted, sharing long prefixes, mostly repeating
//! decimal runs -- so replacing it with the hash the live map already keys the page by removes 19.6
//! B/object of stream and lands a container 5.1 to 6.8 B/object LARGER, on all four points. A
//! removal whose stream saving is real and whose artefact saving is negative is a measured
//! negative, and it is the one a reader of the 41 B/object figure would have reached for first.
//!
//! THE OTHER DIRECTION PAYS: 104.0 B/object of stream and 6.7 to 7.2 B/object of container out of a
//! shipped 29.1 to 41.5, which is 17% to 23% of the artefact. WHAT IT REQUIRES IS NAMED RATHER THAN
//! GLOSSED -- the decoder would have to PARSE the key, which today it does not, so this is a
//! load-path change and not a byte change, and the two sides are mutually exclusive: exactly one
//! copy of each fact may go.
//!
//! # NOTHING IS A REMAINDER, AND AN UNNAMED KEY IS A PANIC
//!
//! The term vocabulary is CLOSED. Every byte attributed goes through [`Tally::add`], which resolves
//! its label in [`TERMS`] and panics on a label that is not declared there; and every field name
//! the walk meets is dispatched through a `match` whose fallback is a panic naming the key. A
//! decomposition can be wrong in every term and still sum to the whole when its last term is
//! "everything else", because the remainder absorbs the error -- so there is no such term, and two
//! independent sum identities are asserted: the whole of `slot_index` against the sum of all terms,
//! and the page-level subtree against the sum of the page-level terms alone. The second is what
//! would catch a term charged at the wrong level, which is the error the 7.81x name would invite.
//!
//! Every term is asserted NON-ZERO except the one that is structurally empty, which is asserted
//! EXACTLY zero and named for it.
//!
//! NO PRODUCTION CODE CHANGES HERE AND NO STORED SHAPE MOVES, so no `SHARD_INDEX_FORMAT_VERSION`
//! step is owed.
//!
//! rust-internal: measures this crate's own index codec, no product behaviour

use super::*;
use std::collections::BTreeMap;

const SHARD: u64 = 41;

/// The shipped routing range's end bucket -- the default an operator is told to set, and the range
/// every headline figure here is taken on.
const SHIPPED_END_BUCKET: u32 = 1023;
/// The key width the published per-object index figures were taken at.
const PUBLISHED_WIDTH: usize = 14;
/// The store sizes every slope is fitted over. A per-key figure taken at one point over a hash
/// table is a sawtooth, so nothing here is divided out of a single reading.
const SIZES: [usize; 3] = [500, 2_000, 8_000];
/// The corpus the headline decomposition and the counterfactuals are taken on.
const HEADLINE_KEYS: usize = 8_000;
/// `-` and six digits.
const TAIL_WIDTH: usize = 7;
/// The byte a padded key's run is made of.
const PAD_BYTE: u8 = b'q';

/// What a fixture key's leading bytes are made of. The content figures do not care; the CONTAINER
/// figures care enormously, so both ends are measured and neither is quoted alone.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyShape {
    /// A constant run plus the ordinal -- the shape the published per-object figures were taken
    /// on, and near-perfectly compressible. A LOWER bound on every container figure.
    Padded,
    /// The same width with pseudorandom letters, so the keys share no structure. The UPPER bound.
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

fn key_text(ordinal: usize, width: usize, shape: KeyShape) -> String {
    let mut text = String::from_utf8(pad_of(ordinal, width, shape))
        .expect("the padding is ASCII by construction");
    text.push_str(&format!("-{ordinal:06}"));
    assert_eq!(text.len(), width, "the key must be exactly the width asked for");
    text
}

// =================================================================================================
// THE CLOSED VOCABULARY
// =================================================================================================

/// HOW A TERM GROWS, AND THERE ARE THREE OF THESE AND NOT TWO.
///
/// A two-class taxonomy of "grows with the store" and "bounded" has nowhere to put a term that
/// grows with the BUCKET count on a range where buckets saturate: per object such a term falls
/// without ever reaching a constant, which is both sub-linear and unbounded. That class is where
/// the amortisation the shipped range exists for happens, so it is named.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
enum Scale {
    /// One instance per stored page. The page count is asserted against the object count.
    PerPage,
    /// One instance per resident bucket.
    PerBucket,
    /// One instance in the whole index.
    PerShard,
}

impl Scale {
    fn label(self) -> &'static str {
        match self {
            Scale::PerPage => "per-page",
            Scale::PerBucket => "per-bucket",
            Scale::PerShard => "per-shard",
        }
    }
}

/// WHAT A TERM IS, and `Redundancy` is only ever assigned where the reconstruction is ASSERTED
/// below rather than argued in prose.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Nature {
    /// A stored state value nothing else in the index states.
    Payload,
    /// A name or an address a reader needs in order to reach the thing.
    Identity,
    /// Structure: punctuation, and the JSON field names a named encoding spends on every entry.
    Envelope,
    /// Reconstructible, character for character, from another term of the same index. Every term
    /// in this class has its reconstruction asserted on every page or bucket of the corpus.
    Redundancy,
}

impl Nature {
    fn label(self) -> &'static str {
        match self {
            Nature::Payload => "payload",
            Nature::Identity => "identity",
            Nature::Envelope => "envelope",
            Nature::Redundancy => "redundancy",
        }
    }
}

/// EVERY TERM `slot_index` IS MADE OF, AND NOTHING ELSE MAY BE CHARGED.
///
/// `(label, scale, nature, what reconstructs it)`. [`Tally::add`] resolves its label here and
/// PANICS on one that is not declared, so a byte can only be attributed to a term somebody named.
/// The fourth column is empty for everything that is not `Redundancy`, and non-empty for
/// everything that is -- asserted by [`the_term_table_is_well_formed`].
#[allow(clippy::type_complexity)]
const TERMS: &[(&str, Scale, Nature, &str)] = &[
    // ---- per-shard: the two envelopes between the shard and the buckets ----
    ("slot_index/field_name", Scale::PerShard, Nature::Envelope, ""),
    ("slot_index/open_brace", Scale::PerShard, Nature::Envelope, ""),
    ("bucket_map/field_name", Scale::PerShard, Nature::Envelope, ""),
    ("bucket_map/field_tail", Scale::PerShard, Nature::Envelope, ""),
    ("bucket_map/open_brace", Scale::PerShard, Nature::Envelope, ""),
    // ---- per-bucket: the map entry, the node envelope and the node's own eleven fields ----
    ("bucket/map_key", Scale::PerBucket, Nature::Identity, ""),
    ("bucket/map_key_colon", Scale::PerBucket, Nature::Envelope, ""),
    ("bucket/map_entry_tail", Scale::PerBucket, Nature::Envelope, ""),
    ("node/open_brace", Scale::PerBucket, Nature::Envelope, ""),
    ("node/field_tails", Scale::PerBucket, Nature::Envelope, ""),
    (
        "node/routing_slot",
        Scale::PerBucket,
        Nature::Redundancy,
        "the bucket's own map key, which it is filed under",
    ),
    ("node/layout", Scale::PerBucket, Nature::Payload, ""),
    ("node/dirty", Scale::PerBucket, Nature::Payload, ""),
    ("node/deleted", Scale::PerBucket, Nature::Payload, ""),
    ("node/meta_loaded", Scale::PerBucket, Nature::Payload, ""),
    ("node/loading", Scale::PerBucket, Nature::Payload, ""),
    ("node/in_memory", Scale::PerBucket, Nature::Payload, ""),
    ("node/ttl_ms", Scale::PerBucket, Nature::Payload, ""),
    ("node/dirty_generation", Scale::PerBucket, Nature::Payload, ""),
    (
        "node/object_index",
        Scale::PerBucket,
        Nature::Redundancy,
        "the stable object id of each page entry in the same bucket, from its kind and object key",
    ),
    ("node/deleted_object_index", Scale::PerBucket, Nature::Identity, ""),
    ("node/tombstone_elements", Scale::PerBucket, Nature::Payload, ""),
    ("node/page_index/field_name", Scale::PerBucket, Nature::Envelope, ""),
    ("page_index/open_brace", Scale::PerBucket, Nature::Envelope, ""),
    // ---- per-page: the composite map key, split into the eight slots it composes ----
    ("page/map_key_quotes", Scale::PerPage, Nature::Envelope, ""),
    (
        "page/map_key/kind",
        Scale::PerPage,
        Nature::Redundancy,
        "the entry's own `model_id` field",
    ),
    (
        "page/map_key/object_key",
        Scale::PerPage,
        Nature::Redundancy,
        "the entry's own `object_key` field",
    ),
    (
        "page/map_key/component_slot",
        Scale::PerPage,
        Nature::Envelope,
        "",
    ),
    (
        "page/map_key/block_slab_id",
        Scale::PerPage,
        Nature::Redundancy,
        "the entry's own `address.a` word, high half",
    ),
    (
        "page/map_key/offset",
        Scale::PerPage,
        Nature::Redundancy,
        "the entry's own `address.a` word, low half",
    ),
    (
        "page/map_key/length",
        Scale::PerPage,
        Nature::Redundancy,
        "the entry's own `address.l` slot",
    ),
    (
        "page/map_key/block_id",
        Scale::PerPage,
        Nature::Redundancy,
        "the entry's own `address.pi` slot",
    ),
    (
        "page/map_key/generation",
        Scale::PerPage,
        Nature::Redundancy,
        "the entry's own `address.g` slot",
    ),
    ("page/map_key/separators", Scale::PerPage, Nature::Envelope, ""),
    ("page/map_key/empty_slot_separator", Scale::PerPage, Nature::Envelope, ""),
    ("page/map_key_colon", Scale::PerPage, Nature::Envelope, ""),
    ("page/map_entry_tail", Scale::PerPage, Nature::Envelope, ""),
    // ---- per-page: the entry the key is filed against ----
    ("entry/open_brace", Scale::PerPage, Nature::Envelope, ""),
    ("entry/field_tails", Scale::PerPage, Nature::Envelope, ""),
    ("entry/object_key", Scale::PerPage, Nature::Identity, ""),
    ("entry/model_id", Scale::PerPage, Nature::Identity, ""),
    ("entry/address/field_name", Scale::PerPage, Nature::Envelope, ""),
    ("entry/address/open_brace", Scale::PerPage, Nature::Envelope, ""),
    ("entry/address/field_tails", Scale::PerPage, Nature::Envelope, ""),
    ("entry/address/a", Scale::PerPage, Nature::Identity, ""),
    ("entry/address/l", Scale::PerPage, Nature::Payload, ""),
    ("entry/address/pi", Scale::PerPage, Nature::Identity, ""),
    ("entry/address/oi", Scale::PerPage, Nature::Identity, ""),
    ("entry/address/g", Scale::PerPage, Nature::Identity, ""),
    ("entry/dirty", Scale::PerPage, Nature::Payload, ""),
    ("entry/deleted", Scale::PerPage, Nature::Payload, ""),
];

/// The one term that is structurally empty and is asserted EXACTLY zero rather than non-zero.
const STRUCTURALLY_EMPTY: &str = "page/map_key/component_slot";

/// The declared scale and nature of a label, or a panic naming the label.
fn declared(label: &str) -> (Scale, Nature, &'static str) {
    TERMS
        .iter()
        .find(|(name, ..)| *name == label)
        .map(|(_, scale, nature, reconstructor)| (*scale, *nature, *reconstructor))
        .unwrap_or_else(|| {
            panic!(
                "[CLAUSE no-unclassified] `{label}` is charged bytes and is not declared in \
                 `TERMS`, so this decomposition has a term nobody named and its sum would be \
                 arithmetic over an unknown set"
            )
        })
}

/// One term's reading: its bytes, how many instances were charged, and how many of its bytes are
/// the JSON field name a named encoding spends on it.
#[derive(Clone, Copy, Default, Debug)]
struct Row {
    bytes: usize,
    instances: usize,
    name_bytes: usize,
}

/// The accumulator. Every byte of `slot_index` goes through [`Tally::add`] exactly once.
#[derive(Default)]
struct Tally {
    rows: BTreeMap<&'static str, Row>,
}

impl Tally {
    /// Charge `bytes` to `label`, of which `name_bytes` are the JSON key and its colon.
    fn add(&mut self, label: &'static str, bytes: usize, name_bytes: usize) {
        declared(label);
        let row = self.rows.entry(label).or_default();
        row.bytes += bytes;
        row.instances += 1;
        row.name_bytes += name_bytes;
    }

    fn total(&self) -> usize {
        self.rows.values().map(|row| row.bytes).sum()
    }

    /// The bytes charged to every term at a given scale.
    fn at_scale(&self, scale: Scale) -> usize {
        self.rows
            .iter()
            .filter(|entry| declared(entry.0).0 == scale)
            .map(|entry| entry.1.bytes)
            .sum()
    }

    fn of_nature(&self, nature: Nature) -> usize {
        self.rows
            .iter()
            .filter(|entry| declared(entry.0).1 == nature)
            .map(|entry| entry.1.bytes)
            .sum()
    }

    fn bytes_of(&self, label: &str) -> usize {
        self.rows.get(label).map(|row| row.bytes).unwrap_or(0)
    }

    /// The bytes charged to a named GROUP of terms, refusing a label that was never charged -- so a
    /// group cannot quietly shrink because one of its members was renamed.
    fn sum_of(&self, labels: &[&str]) -> usize {
        labels
            .iter()
            .map(|label| {
                let row = self.rows.get(label).unwrap_or_else(|| {
                    panic!(
                        "[CLAUSE group-is-whole] `{label}` is named in a group total and was never \
                         charged, so the group is summing over a term that is not in the index"
                    )
                });
                row.bytes
            })
            .sum()
    }
}

/// EVERY TERM THE PAGE ENTRY'S COMPOSITE MAP KEY IS MADE OF, and nothing else.
///
/// Named as a list rather than matched by a prefix so that a term added under `page/` later has to
/// be put in or left out ON PURPOSE. [`Tally::sum_of`] refuses a member that was not charged.
const PAGE_KEY_TERMS: &[&str] = &[
    "page/map_key_quotes",
    "page/map_key/kind",
    "page/map_key/object_key",
    "page/map_key/component_slot",
    "page/map_key/block_slab_id",
    "page/map_key/offset",
    "page/map_key/length",
    "page/map_key/block_id",
    "page/map_key/generation",
    "page/map_key/separators",
    "page/map_key/empty_slot_separator",
];

/// THE ENTRY FIELDS WHOSE VALUES THE COMPOSITE KEY RESTATES, envelope included -- because removing
/// a field removes its JSON name and its separator too, and a report counting only the value would
/// understate what the field costs.
const ENTRY_RESTATED_TERMS: &[&str] = &[
    "entry/object_key",
    "entry/model_id",
    "entry/address/field_name",
    "entry/address/open_brace",
    "entry/address/field_tails",
    "entry/address/a",
    "entry/address/l",
    "entry/address/pi",
    "entry/address/oi",
    "entry/address/g",
];

/// The five address values as the composite key renders them: decimals and nothing else.
const KEY_ADDRESS_TERMS: &[&str] = &[
    "page/map_key/block_slab_id",
    "page/map_key/offset",
    "page/map_key/length",
    "page/map_key/block_id",
    "page/map_key/generation",
];

/// The same five values as the named `address` object beside the key writes them.
const ENTRY_ADDRESS_TERMS: &[&str] = &[
    "entry/address/field_name",
    "entry/address/open_brace",
    "entry/address/field_tails",
    "entry/address/a",
    "entry/address/l",
    "entry/address/pi",
    "entry/address/oi",
    "entry/address/g",
];

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV1A64_PRIME);
    }
}

/// THE STABLE OBJECT ID FOR A SHARD, A KIND AND A KEY -- WRITTEN OUT HERE, AND NOT FOR STYLE.
///
/// `engine::hashing::stable_block_object_id` bumps two PROCESS-WIDE atomics on every call in a test
/// build, and other arms in this suite reset and read them. A probe that called it once per page
/// over an 8,000-object corpus would move a counter it is not measuring, in a binary that runs its
/// tests in one process -- a measurement perturbing the suite it is meant to be measured in.
///
/// AND IT IS ALSO THE HONEST FORM OF THE CLAIM. What is being shown is that the WRITTEN page
/// entries carry enough to rebuild the written `object_index`; an independent implementation over
/// their bytes is what shows that, where calling the engine's own would be the engine agreeing with
/// itself. The cost is that this is a GOLDEN: a change to the derivation breaks it, which is the
/// right direction -- the ids are a stored shape.
fn stable_object_id_from_the_written_terms(shard: u64, kind: &str, key: &str) -> u64 {
    let mut hash = FNV1A64_OFFSET_BASIS;
    fnv1a_update(&mut hash, shard.to_string().as_bytes());
    fnv1a_update(&mut hash, b":");
    fnv1a_update(&mut hash, kind.as_bytes());
    fnv1a_update(&mut hash, b":");
    fnv1a_update(&mut hash, key.as_bytes());
    hash
}

/// The bytes a JSON value occupies on its own.
fn jlen(value: &serde_json::Value) -> usize {
    serde_json::to_vec(value).expect("a value serializes").len()
}

/// The bytes a JSON object key occupies, quotes included.
fn klen(name: &str) -> usize {
    serde_json::to_vec(name).expect("a name serializes").len()
}

// =================================================================================================
// THE FIXTURE
// =================================================================================================

/// One store's reading: the shard's three surfaces, and the structures the decomposition walks.
struct Reading {
    objects: usize,
    pages: usize,
    buckets: usize,
    width: usize,
    shape: KeyShape,
    /// The whole shard as a JSON value, which is what the partition is taken on.
    value: serde_json::Value,
    json_len: usize,
    msgpack_len: usize,
    container_len: usize,
    /// Every page's written map key paired with the handle the live map files it under, joined on
    /// the renderer the encoder itself uses. Used to price a handle-keyed page index; NOT used for
    /// the reconstruction claim, which goes the other way, from the entry's written bytes.
    handles_by_written_key: BTreeMap<String, u64>,
}

fn reading_on(keys: usize, width: usize, shape: KeyShape, end_routing_bucket: u32) -> Reading {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        table_name: "what-the-slot-index-is-made-of".to_string(),
        shard_uri: "local://what-the-slot-index-is-made-of/41".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "[CLAUSE seed-landed] the fixture could not load shard {SHARD} on 0..{end_routing_bucket}: \
         {:?}",
        response.status
    );

    for ordinal in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: key_text(ordinal, width, shape),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "the seed must ack at {ordinal}: {response:?}");
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    assert_eq!(
        keys,
        shard.strings().len(),
        "[CLAUSE seed-landed] the fixture acked {keys} string object(s) and the index holds {}, so \
         the denominator every figure below is taken against is not the corpus",
        shard.strings().len()
    );

    let buckets = shard.bucket_index.bucket_map.len();
    let pages: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|node| node.block_index.len())
        .sum();

    // THE JOIN FROM A WRITTEN KEY TO THE HANDLE THE LIVE MAP FILES THE PAGE UNDER.
    //
    // Built through the encoder's own renderer, which is legitimate HERE and nowhere else in this
    // module: this is bookkeeping for a counterfactual, not the redundancy claim. The claim is
    // taken in the other direction -- the key rebuilt from the entry's WRITTEN BYTES -- because a
    // comparison against `block_index_written_key` would be a renderer compared with itself.
    let identity_keys = crate::engine::state::PageKeysByPosition::from_lookup(
        &shard.bucket_index.object_block_lookup,
    );
    let mut handles_by_written_key = BTreeMap::new();
    for node in shard.bucket_index.bucket_map.values() {
        let identities = crate::engine::state::page_identities_in(node, &identity_keys);
        for (handle, page) in node.block_index.iter() {
            let object_key = identities.get(handle).unwrap_or_else(|| {
                panic!(
                    "[CLAUSE join-total] the page at handle {handle} in bucket {} has no identity \
                     beside it, so no counterfactual over page keys can be priced",
                    node.routing_bucket
                )
            });
            let written = crate::engine::state::block_index_written_key(object_key, page);
            assert!(
                handles_by_written_key.insert(written.clone(), *handle).is_none(),
                "[CLAUSE join-total] two pages render the written key `{written}`, so a key is not \
                 a page's name and the join below is not one-to-one"
            );
        }
    }
    assert_eq!(
        handles_by_written_key.len(),
        pages,
        "[CLAUSE join-total] the join names {} page(s) against {pages} resident",
        handles_by_written_key.len()
    );

    let container = crate::engine::encode_index_bytes(shard);
    let msgpack = msgpack_payload_of(&container);
    let json = serde_json::to_vec(shard).expect("the index must serialize as JSON");
    let value = serde_json::to_value(shard).expect("the index must serialize as a value");

    Reading {
        objects: keys,
        pages,
        buckets,
        width,
        shape,
        value,
        json_len: json.len(),
        msgpack_len: msgpack.len(),
        container_len: container.len(),
        handles_by_written_key,
    }
}

/// The pre-compression payload the default codec writes, pulled back out of the container.
fn msgpack_payload_of(container: &[u8]) -> Vec<u8> {
    const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
    let start = container
        .windows(4)
        .position(|window| window == ZSTD_MAGIC)
        .expect("the container must hold a zstd frame, or this is not the shipped artefact");
    assert!(
        start < 32,
        "the zstd magic is {start} bytes into the container, too far to be its header"
    );
    zstd::stream::decode_all(&container[start..]).expect("the container's frame must decompress")
}

impl Reading {
    fn object(&self) -> &serde_json::Map<String, serde_json::Value> {
        self.value.as_object().expect("the served index is a JSON object")
    }

    /// The bytes the `slot_index` entry occupies in the shard's own stream: its quoted name, its
    /// colon and its value. The same quantity the published 226.35 B/object is a division of.
    fn slot_index_entry_bytes(&self) -> usize {
        let value = self
            .object()
            .get("slot_index")
            .expect("the core index is written under `slot_index`");
        klen("slot_index") + 1 + jlen(value)
    }
}

// =================================================================================================
// THE WALK
// =================================================================================================

/// The eight slots a page's written key composes, in the order the composite writes them.
struct KeySlots {
    kind: String,
    object_key: String,
    component: String,
    block_slab_id: String,
    offset: String,
    length: String,
    block_id: String,
    generation: String,
}

/// Split a written page key into its eight slots, and REFUSE a key the split does not recompose.
///
/// The five numbers are taken from the right, because an object key may contain a colon while a
/// rendered `u64` may not. The recomposition is asserted rather than assumed: a split that does
/// not rebuild its input is a guess, and every per-slot byte figure below would inherit it.
fn split_written_key(key: &str) -> KeySlots {
    let mut from_right = key.rsplitn(6, ':');
    let generation = from_right.next().expect("a key has a last slot").to_string();
    let block_id = from_right.next().unwrap_or_default().to_string();
    let length = from_right.next().unwrap_or_default().to_string();
    let offset = from_right.next().unwrap_or_default().to_string();
    let block_slab_id = from_right.next().unwrap_or_default().to_string();
    let head = from_right.next().unwrap_or_default();
    let mut from_left = head.splitn(2, ':');
    let kind = from_left.next().unwrap_or_default().to_string();
    let rest = from_left.next().unwrap_or_default();
    // The component slot is the LAST colon-separated run of the head's tail, and the object key is
    // everything before it -- so an object key holding a colon stays in one slot.
    let (object_key, component) = match rest.rfind(':') {
        Some(at) => (rest[..at].to_string(), rest[at + 1..].to_string()),
        None => (rest.to_string(), String::new()),
    };
    let slots = KeySlots {
        kind,
        object_key,
        component,
        block_slab_id,
        offset,
        length,
        block_id,
        generation,
    };
    let recomposed = format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        slots.kind,
        slots.object_key,
        slots.component,
        slots.block_slab_id,
        slots.offset,
        slots.length,
        slots.block_id,
        slots.generation
    );
    assert_eq!(
        recomposed, key,
        "[CLAUSE key-split-recomposes] the eight-slot split of a written page key does not rebuild \
         it, so every per-slot figure taken from it is a guess"
    );
    slots
}

/// A node field that is not one of the twelve the encoder writes is a PANIC naming it.
fn node_field_label(name: &str) -> &'static str {
    match name {
        "routing_slot" => "node/routing_slot",
        "layout" => "node/layout",
        "dirty" => "node/dirty",
        "deleted" => "node/deleted",
        "meta_loaded" => "node/meta_loaded",
        "loading" => "node/loading",
        "in_memory" => "node/in_memory",
        "ttl_ms" => "node/ttl_ms",
        "dirty_generation" => "node/dirty_generation",
        "object_index" => "node/object_index",
        "deleted_object_index" => "node/deleted_object_index",
        "tombstone_elements" => "node/tombstone_elements",
        other => panic!(
            "[CLAUSE no-unclassified] a bucket node writes the key `{other}`, which this \
             decomposition does not name -- its bytes would be absorbed by whatever term was \
             charged last, and the sum would still balance"
        ),
    }
}

/// An address slot that is not one of the five the wire declares is a PANIC naming it.
fn address_field_label(name: &str) -> &'static str {
    match name {
        "a" => "entry/address/a",
        "l" => "entry/address/l",
        "pi" => "entry/address/pi",
        "oi" => "entry/address/oi",
        "g" => "entry/address/g",
        other => panic!(
            "[CLAUSE no-unclassified] a stored address writes the slot `{other}`, which this \
             decomposition does not name"
        ),
    }
}

/// THE WHOLE OF `slot_index`, CHARGED TERM BY TERM, WITH THREE SUM IDENTITIES ASSERTED.
///
/// Returns the tally, the page-level subtotal measured independently, and the reconstruction count.
struct Walked {
    tally: Tally,
    /// The bytes of every `page_index` VALUE, summed over buckets and measured directly off the
    /// JSON rather than from the tally. The second, independent identity: a term charged at the
    /// bucket level that belongs at the page level balances the whole and breaks this.
    page_subtree_bytes: usize,
    /// Pages whose written key was rebuilt from the entry's own written bytes, character for
    /// character.
    keys_reconstructed: usize,
    /// Buckets whose `routing_slot` field was rebuilt from the map key it is filed under.
    slots_reconstructed: usize,
    /// Buckets whose written `object_index` is exactly the set of ids derivable from the kinds and
    /// object keys of the page entries written in that same bucket.
    buckets_whose_object_index_is_derivable: usize,
    /// Pages whose stored address writes its `oi` slot as JSON null.
    ///
    /// `impl From<ElementEntry> for BlockAddressWire` fills `object_id` with a LITERAL `None` --
    /// the type it converts from has no such field to read -- so this is a property of the writer
    /// and not of a workload. Counted rather than asserted from the source, because a count is
    /// what fails when the literal stops being one.
    nil_object_id_slots: usize,
}

fn walk(reading: &Reading) -> Walked {
    let mut tally = Tally::default();

    let slot_index = reading
        .object()
        .get("slot_index")
        .expect("the core index is written under `slot_index`");
    tally.add("slot_index/field_name", klen("slot_index") + 1, klen("slot_index") + 1);
    tally.add("slot_index/open_brace", 1, 0);

    let core = slot_index.as_object().expect("the core index writes an object");
    for name in core.keys() {
        assert_eq!(
            name, "bucket_map",
            "[CLAUSE no-unclassified] the core index writes the key `{name}`, which this \
             decomposition does not name"
        );
    }
    assert_eq!(
        core.len(),
        1,
        "[CLAUSE no-unclassified] the core index writes {} key(s); this walk charges the \
         punctuation of exactly one",
        core.len()
    );
    tally.add("bucket_map/field_name", klen("bucket_map") + 1, klen("bucket_map") + 1);
    tally.add("bucket_map/field_tail", 1, 0);

    let bucket_map = core
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
    assert!(
        !bucket_map.is_empty(),
        "[CLAUSE part-sum] the bucket map is empty, so there is nothing here to decompose"
    );
    tally.add("bucket_map/open_brace", 1, 0);

    let mut page_subtree_bytes = 0usize;
    let mut keys_reconstructed = 0usize;
    let mut slots_reconstructed = 0usize;
    let mut nil_object_id_slots = 0usize;
    let mut buckets_whose_object_index_is_derivable = 0usize;
    let mut pages_seen = 0usize;

    for (bucket_key, node_value) in bucket_map.iter() {
        tally.add("bucket/map_key", klen(bucket_key), 0);
        tally.add("bucket/map_key_colon", 1, 0);
        tally.add("bucket/map_entry_tail", 1, 0);

        let node = node_value.as_object().expect("a bucket node writes an object");
        tally.add("node/open_brace", 1, 0);
        tally.add("node/field_tails", node.len(), 0);

        for (field, value) in node.iter() {
            if field == "page_index" {
                tally.add("node/page_index/field_name", klen(field) + 1, klen(field) + 1);
                continue;
            }
            let label = node_field_label(field);
            tally.add(label, klen(field) + 1 + jlen(value), klen(field) + 1);
            if field == "routing_slot" {
                // RECONSTRUCTED FROM THE MAP KEY, measured per bucket. The map key is a JSON
                // object key and so a quoted decimal; the field is a number. Both are parsed and
                // compared as numbers, which is the comparison that says one restates the other.
                let from_key: u64 = bucket_key
                    .parse()
                    .expect("a bucket map key is a rendered routing bucket");
                let from_field = value
                    .as_u64()
                    .expect("`routing_slot` is written as a number");
                assert_eq!(
                    from_key, from_field,
                    "[CLAUSE routing-slot-restates-the-map-key] bucket `{bucket_key}` holds \
                     `routing_slot` {from_field}, so the field is not a restatement of the key and \
                     its bytes are not redundancy"
                );
                slots_reconstructed += 1;
            }
        }

        // THE IDS THE NODE WRITES, held while the pages beside them are walked.
        let written_object_ids: std::collections::BTreeSet<u64> = node
            .get("object_index")
            .expect("a bucket node writes `object_index`")
            .as_array()
            .expect("`object_index` writes an array")
            .iter()
            .map(|id| id.as_u64().expect("an object id is written as a number"))
            .collect();
        let mut derived_object_ids: std::collections::BTreeSet<u64> =
            std::collections::BTreeSet::new();

        let page_index = node
            .get("page_index")
            .expect("a bucket node writes `page_index`");
        page_subtree_bytes += jlen(page_index);
        let pages = page_index.as_object().expect("the page index writes an object");
        assert!(
            !pages.is_empty(),
            "[CLAUSE no-empty-page-index] bucket `{bucket_key}` writes an EMPTY page index, and \
             this walk charges one separator byte per entry -- an empty object's second brace has \
             no entry to be charged to, so the partition identity would be off by one per such \
             bucket"
        );
        tally.add("page_index/open_brace", 1, 0);

        for (page_key, entry_value) in pages.iter() {
            pages_seen += 1;
            // QUOTES AND NOTHING ELSE. `klen` is the JSON rendering, so this difference is the two
            // quotes plus any escaping; the fixture's keys hold none, and the assertion is what
            // keeps the label honest rather than letting an escape hide inside an envelope term.
            assert_eq!(
                klen(page_key) - page_key.len(),
                2,
                "[CLAUSE map-key-quotes-are-two] the written page key `{page_key}` renders {} B \
                 wider than its characters, so it carries escaping and this term is not quotes",
                klen(page_key) - page_key.len()
            );
            tally.add("page/map_key_quotes", klen(page_key) - page_key.len(), 0);
            tally.add("page/map_key_colon", 1, 0);
            tally.add("page/map_entry_tail", 1, 0);

            let slots = split_written_key(page_key);
            tally.add("page/map_key/kind", slots.kind.len(), 0);
            tally.add("page/map_key/object_key", slots.object_key.len(), 0);
            tally.add("page/map_key/component_slot", slots.component.len(), 0);
            tally.add("page/map_key/block_slab_id", slots.block_slab_id.len(), 0);
            tally.add("page/map_key/offset", slots.offset.len(), 0);
            tally.add("page/map_key/length", slots.length.len(), 0);
            tally.add("page/map_key/block_id", slots.block_id.len(), 0);
            tally.add("page/map_key/generation", slots.generation.len(), 0);
            // SEVEN SEPARATORS, AND ONE OF THEM DELIMITS A SLOT THAT IS ALWAYS EMPTY. Charged
            // apart from the other six because what it is worth to remove is a different number.
            tally.add("page/map_key/separators", 6, 0);
            tally.add("page/map_key/empty_slot_separator", 1, 0);

            let entry = entry_value.as_object().expect("a page entry writes an object");
            tally.add("entry/open_brace", 1, 0);
            tally.add("entry/field_tails", entry.len(), 0);
            for (field, value) in entry.iter() {
                match field.as_str() {
                    "object_key" => {
                        tally.add("entry/object_key", klen(field) + 1 + jlen(value), klen(field) + 1)
                    }
                    "model_id" => {
                        tally.add("entry/model_id", klen(field) + 1 + jlen(value), klen(field) + 1)
                    }
                    "dirty" => {
                        tally.add("entry/dirty", klen(field) + 1 + jlen(value), klen(field) + 1)
                    }
                    "deleted" => {
                        tally.add("entry/deleted", klen(field) + 1 + jlen(value), klen(field) + 1)
                    }
                    "address" => {
                        tally.add(
                            "entry/address/field_name",
                            klen(field) + 1,
                            klen(field) + 1,
                        );
                        let address =
                            value.as_object().expect("a stored address writes an object");
                        tally.add("entry/address/open_brace", 1, 0);
                        tally.add("entry/address/field_tails", address.len(), 0);
                        for (slot, slot_value) in address.iter() {
                            let label = address_field_label(slot);
                            tally.add(
                                label,
                                klen(slot) + 1 + jlen(slot_value),
                                klen(slot) + 1,
                            );
                            if slot == "oi" && slot_value.is_null() {
                                nil_object_id_slots += 1;
                            }
                        }
                    }
                    other => panic!(
                        "[CLAUSE no-unclassified] a page entry writes the key `{other}`, which \
                         this decomposition does not name"
                    ),
                }
            }

            // ---- THE RECONSTRUCTION, FROM THE ENTRY'S OWN WRITTEN BYTES ----
            //
            // The address is decoded back out of the written value through the production
            // `Deserialize`, which is what a load has in hand, and the key is then rebuilt by
            // THIS MODULE'S formatter. Not by `block_index_written_key`: comparing that against
            // the written key would compare one renderer with itself and would say nothing about
            // whether the entry carries the terms the key spells.
            let decoded: crate::ElementEntry = serde_json::from_value(
                entry
                    .get("address")
                    .expect("a page entry writes `address`")
                    .clone(),
            )
            .expect("a written address decodes through its own production impl");
            let object_key = entry
                .get("object_key")
                .expect("a page entry writes `object_key`")
                .as_str()
                .expect("`object_key` is written as a string");
            let model_id = entry
                .get("model_id")
                .expect("a page entry writes `model_id`")
                .as_str()
                .expect("`model_id` is written as a string");
            let rebuilt = format!(
                "{}:{}::{}:{}:{}:{}:{}",
                model_id,
                object_key,
                decoded.block_slab_id(),
                decoded.offset(),
                decoded.length(),
                decoded.block_id().unwrap_or_default(),
                decoded.generation().unwrap_or_default()
            );
            assert_eq!(
                &rebuilt, page_key,
                "[CLAUSE key-restates-the-entry] the page key rebuilt from the entry's own written \
                 bytes is not the key the entry is filed under, so the key carries a term the \
                 entry does not and calling it redundancy would be wrong"
            );
            keys_reconstructed += 1;

            // AND THE OBJECT ID THE NODE FILES THIS PAGE'S OBJECT UNDER, from the same two
            // written terms. A LIVE page only: a removal's id goes to `deleted_object_index`,
            // and this fixture performs none -- which `optional-stays-absent` pins.
            if !entry
                .get("deleted")
                .expect("a page entry writes `deleted`")
                .as_bool()
                .expect("`deleted` is written as a boolean")
            {
                derived_object_ids.insert(stable_object_id_from_the_written_terms(
                    SHARD, model_id, object_key,
                ));
            }
        }

        if derived_object_ids == written_object_ids {
            buckets_whose_object_index_is_derivable += 1;
        }
    }

    assert_eq!(
        pages_seen, reading.pages,
        "[CLAUSE part-sum] the walk saw {pages_seen} written page(s) against {} resident",
        reading.pages
    );

    Walked {
        tally,
        page_subtree_bytes,
        keys_reconstructed,
        slots_reconstructed,
        buckets_whose_object_index_is_derivable,
        nil_object_id_slots,
    }
}

/// Print one tally as a table, and assert every declared term is non-zero (or exactly zero where
/// it is structurally empty).
fn report(reading: &Reading, walked: &Walked) {
    let tally = &walked.tally;
    let whole = reading.slot_index_entry_bytes();
    let per = |bytes: usize| bytes as f64 / reading.objects as f64;

    println!();
    println!(
        "================ `slot_index` TERM BY TERM, {} OBJECTS IN {} BUCKETS ON 0..{} \
         ({} KEYS, WIDTH {}) ================",
        reading.objects,
        reading.buckets,
        SHIPPED_END_BUCKET,
        reading.shape.label(),
        reading.width
    );
    println!(
        "  {:<38} {:>12} {:>10} {:>11} {:>12} {:>12} {:>8}",
        "term", "bytes", "instances", "B/object", "% slot_index", "scale", "nature"
    );
    let mut ordered: Vec<(&'static str, Row)> =
        tally.rows.iter().map(|entry| (*entry.0, *entry.1)).collect();
    ordered.sort_by(|left, right| right.1.bytes.cmp(&left.1.bytes).then(left.0.cmp(right.0)));
    for (label, row) in ordered.iter() {
        let (scale, nature, _) = declared(label);
        println!(
            "  {:<38} {:>12} {:>10} {:>11.3} {:>11.2}% {:>12} {:>8}",
            label,
            row.bytes,
            row.instances,
            per(row.bytes),
            100.0 * row.bytes as f64 / whole as f64,
            scale.label(),
            nature.label()
        );
    }
    println!(
        "  {:<38} {:>12} {:>10} {:>11.3} {:>11.2}%",
        "TOTAL",
        tally.total(),
        "",
        per(tally.total()),
        100.0 * tally.total() as f64 / whole as f64
    );

    println!();
    println!("  BY SCALE");
    for scale in [Scale::PerPage, Scale::PerBucket, Scale::PerShard] {
        let bytes = tally.at_scale(scale);
        println!(
            "    {:<12} {:>12} B  {:>9.3} B/object  {:>6.2}%",
            scale.label(),
            bytes,
            per(bytes),
            100.0 * bytes as f64 / whole as f64
        );
    }
    println!("  BY NATURE");
    for nature in [Nature::Payload, Nature::Identity, Nature::Envelope, Nature::Redundancy] {
        let bytes = tally.of_nature(nature);
        println!(
            "    {:<12} {:>12} B  {:>9.3} B/object  {:>6.2}%",
            nature.label(),
            bytes,
            per(bytes),
            100.0 * bytes as f64 / whole as f64
        );
    }
    let name_tax: usize = tally.rows.values().map(|row| row.name_bytes).sum();
    println!(
        "  JSON FIELD NAMES, a cross-cut and not a term: {name_tax} B = {:.3} B/object = {:.2}% \
         -- what a NAMED encoding spends on keys. The served index is msgpack `with_struct_map` \
         for a stated reason: the positional form mis-reads a struct that skipped an absent \
         optional, so this is a priced DECLINE and not a finding.",
        per(name_tax),
        100.0 * name_tax as f64 / whole as f64
    );
}

// =================================================================================================
// THE COUNTERFACTUAL SURFACES
// =================================================================================================

/// The container `TSIDX\x01` codec 1 writes: the magic, the codec id, and a zstd-3 frame over the
/// struct's JSON.
///
/// THIS IS A PRODUCTION CONTAINER ARM OF THIS STORE AND NOT A SURFACE INVENTED HERE.
/// `engine::wrap_index_json` writes exactly this whenever the codec is not the binary one, and
/// `decode_index_bytes` sniffs and reads it. It is used for every counterfactual below for one
/// reason: a doctored stream has to be RE-ENCODED, and the only re-encoding this module can take
/// that is byte-for-byte what production writes is the JSON one. A msgpack payload rebuilt from a
/// `serde_json::Value` is NOT that -- measured: it differs from the production payload at byte 4,
/// because a `Value` writes its keys sorted while the struct writes them in declaration order, and
/// the reordering alone moves the compressed size by 5%. So the counterfactual is taken on the
/// container arm it can be taken on exactly, and
/// [`what_removing_a_redundant_slot_index_term_would_cost_the_shipped_container`] asserts how far
/// the two container arms are apart before quoting a price from either.
fn json_container_bytes(value: &serde_json::Value) -> usize {
    const MAGIC: &[u8] = b"TSIDX\x01";
    const CODEC_BYTE: usize = 1;
    const ZSTD_LEVEL: i32 = 3;
    let json = serde_json::to_vec(value).expect("the doctored index serializes");
    let payload =
        zstd::stream::encode_all(json.as_slice(), ZSTD_LEVEL).expect("the payload compresses");
    MAGIC.len() + CODEC_BYTE + payload.len()
}

/// The removals this module prices, each one a term the decomposition classified as redundancy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Candidate {
    /// Nothing is changed. The CONTROL: it must reproduce the undoctored stream byte for byte, or
    /// every price below is a price of the doctoring machinery rather than of a removal.
    Control,
    /// The page index is keyed by the handle the LIVE map already keys it by, rendered as a
    /// decimal, in place of the composite.
    PageKeyBecomesTheHandle,
    /// The composite loses the slot that is always empty, and the separator that delimits it.
    PageKeyDropsTheEmptySlot,
    /// The entry loses `object_key`, which its own map key spells.
    EntryDropsObjectKey,
    /// The entry loses `model_id`, which its own map key spells.
    EntryDropsModelId,
    /// The entry loses `address`, whose five values its own map key spells.
    EntryDropsAddress,
    /// The entry loses all three, so the composite key is the only copy of each fact. The MIRROR
    /// of `PageKeyBecomesTheHandle`: exactly one of the two may be taken, never both.
    EntryDropsWhatTheKeyRestates,
    /// The stored address loses its `oi` slot, which the writer fills from a literal `None`.
    ///
    /// Priced and DECLINED, with the reason in the source rather than in this comment: the index
    /// log packs the same struct POSITIONALLY, so a slot skipped here shortens that array and
    /// shifts every slot after it. Pricing a decline is what makes it a decision.
    AddressDropsTheAlwaysNilSlot,
}

impl Candidate {
    fn label(self) -> &'static str {
        match self {
            Candidate::Control => "control: nothing removed",
            Candidate::PageKeyBecomesTheHandle => "page key -> the live map's handle",
            Candidate::PageKeyDropsTheEmptySlot => "page key -> drop the always-empty slot",
            Candidate::EntryDropsObjectKey => "entry -> drop `object_key`",
            Candidate::EntryDropsModelId => "entry -> drop `model_id`",
            Candidate::EntryDropsAddress => "entry -> drop `address`",
            Candidate::EntryDropsWhatTheKeyRestates => "entry -> drop all three the key restates",
            Candidate::AddressDropsTheAlwaysNilSlot => "address -> drop the always-nil `oi` slot",
        }
    }
}

/// `slot_index` as it would be written with one term removed.
///
/// A BYTE-WEIGHT COUNTERFACTUAL, AND THAT IS ALL IT CLAIMS TO BE. It says what the artefact would
/// weigh, not that the result loads -- except for the one candidate whose loadability is MEASURED,
/// by [`the_page_keys_characters_are_not_read_back_by_the_decoder`], which feeds the doctored bytes
/// to the production decoder.
fn doctor(reading: &Reading, candidate: Candidate) -> serde_json::Value {
    let mut value = reading.value.clone();
    {
        let bucket_map = value
            .get_mut("slot_index")
            .expect("slot_index")
            .get_mut("bucket_map")
            .expect("bucket_map")
            .as_object_mut()
            .expect("an object");
        for node in bucket_map.values_mut() {
            let page_index = node
                .get("page_index")
                .expect("page_index")
                .as_object()
                .expect("an object")
                .clone();
            let mut replacement = serde_json::Map::with_capacity(page_index.len());
            for (page_key, entry_value) in page_index.iter() {
                let mut entry = entry_value.clone();
                let fields = entry.as_object_mut().expect("a page entry is an object");
                let new_key = match candidate {
                    Candidate::Control
                    | Candidate::EntryDropsObjectKey
                    | Candidate::EntryDropsModelId
                    | Candidate::EntryDropsAddress
                    | Candidate::EntryDropsWhatTheKeyRestates
                    | Candidate::AddressDropsTheAlwaysNilSlot => page_key.clone(),
                    Candidate::PageKeyBecomesTheHandle => reading
                        .handles_by_written_key
                        .get(page_key)
                        .unwrap_or_else(|| {
                            panic!(
                                "[CLAUSE join-total] the written key `{page_key}` has no handle \
                                 beside it, so this counterfactual cannot be priced"
                            )
                        })
                        .to_string(),
                    Candidate::PageKeyDropsTheEmptySlot => {
                        let slots = split_written_key(page_key);
                        assert!(
                            slots.component.is_empty(),
                            "[CLAUSE empty-slot-is-empty] the component slot of `{page_key}` holds \
                             `{}`, so dropping it would drop a term rather than a hole",
                            slots.component
                        );
                        format!(
                            "{}:{}:{}:{}:{}:{}:{}",
                            slots.kind,
                            slots.object_key,
                            slots.block_slab_id,
                            slots.offset,
                            slots.length,
                            slots.block_id,
                            slots.generation
                        )
                    }
                };
                match candidate {
                    Candidate::EntryDropsObjectKey => {
                        fields.remove("object_key").expect("object_key was written");
                    }
                    Candidate::EntryDropsModelId => {
                        fields.remove("model_id").expect("model_id was written");
                    }
                    Candidate::EntryDropsAddress => {
                        fields.remove("address").expect("address was written");
                    }
                    Candidate::EntryDropsWhatTheKeyRestates => {
                        fields.remove("object_key").expect("object_key was written");
                        fields.remove("model_id").expect("model_id was written");
                        fields.remove("address").expect("address was written");
                    }
                    Candidate::AddressDropsTheAlwaysNilSlot => {
                        let address = fields
                            .get_mut("address")
                            .expect("address was written")
                            .as_object_mut()
                            .expect("an object");
                        let was = address.remove("oi").expect("the `oi` slot was written");
                        assert!(
                            was.is_null(),
                            "[CLAUSE empty-slot-is-empty] the `oi` slot of a page in this fixture \
                             holds `{was}`, so dropping it would drop a value rather than a hole"
                        );
                    }
                    Candidate::Control
                    | Candidate::PageKeyBecomesTheHandle
                    | Candidate::PageKeyDropsTheEmptySlot => {}
                }
                assert!(
                    replacement.insert(new_key.clone(), entry).is_none(),
                    "[CLAUSE doctoring-is-one-to-one] the doctored page index files two entries \
                     under `{new_key}`, so this candidate loses a page rather than bytes"
                );
            }
            *node.get_mut("page_index").expect("page_index") =
                serde_json::Value::Object(replacement);
        }
    }
    value
}

/// The bytes the `slot_index` entry of a (possibly doctored) shard value occupies.
fn slot_index_entry_bytes_of(value: &serde_json::Value) -> usize {
    let entry = value
        .as_object()
        .expect("a shard value is an object")
        .get("slot_index")
        .expect("slot_index");
    klen("slot_index") + 1 + jlen(entry)
}

// =================================================================================================

/// THE TERM TABLE IS WELL FORMED BEFORE ANY FIXTURE RUNS.
///
/// One conclusion: the vocabulary every byte below is charged against has no duplicate label, and
/// `Redundancy` is assigned exactly where a reconstructor is named. A class whose membership is a
/// matter of prose is a class that drifts, and the cost of finding that out is one run.
#[test]
fn the_slot_index_term_table_is_well_formed() {
    let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
    for (label, ..) in TERMS.iter() {
        *seen.entry(label).or_default() += 1;
    }
    let duplicates: Vec<&&str> = seen
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(label, _)| label)
        .collect();
    assert!(
        duplicates.is_empty(),
        "[CLAUSE table-unique] these labels are declared more than once: {duplicates:?} -- a \
         repeated label silently merges two terms and the sum still balances"
    );
    for (label, _, nature, reconstructor) in TERMS.iter() {
        match nature {
            Nature::Redundancy => assert!(
                !reconstructor.is_empty(),
                "[CLAUSE redundancy-names-its-source] `{label}` is classed as redundancy and names \
                 nothing that reconstructs it, so the class is an opinion"
            ),
            _ => assert!(
                reconstructor.is_empty(),
                "[CLAUSE redundancy-names-its-source] `{label}` is classed `{}` and names \
                 `{reconstructor}` as reconstructing it -- a term with a reconstructor belongs in \
                 the redundancy class or the class is wrong",
                nature.label()
            ),
        }
    }
    assert_eq!(
        declared(STRUCTURALLY_EMPTY).0,
        Scale::PerPage,
        "[CLAUSE table-unique] the structurally empty term must be a per-page one, or the \
         exemption below exempts the wrong thing"
    );
    println!();
    println!(
        "  {} term(s) declared, {} of them redundancy with a named reconstructor",
        TERMS.len(),
        TERMS
            .iter()
            .filter(|(_, _, nature, _)| *nature == Nature::Redundancy)
            .count()
    );
}

// =================================================================================================

/// WHAT `slot_index` IS MADE OF ON THE SHIPPED ROUTING RANGE, TERM BY TERM.
///
/// One conclusion: the full partition of the `slot_index` entry at the amortised regime, every term
/// named, non-zero, classed by a measured scale, and summing to the whole by two independent
/// identities.
///
/// THE ASSERTION ORDER IS LOAD-BEARING. A plant stops at the first failing assertion, so the
/// fixture-regime clauses come first: a corpus that did not land and a fixture that never reached
/// the amortised regime are refused BEFORE any byte is read as a finding. Each message carries a
/// `[CLAUSE <name>]` tag so a plant's verdict comes from the panic text.
#[test]
fn what_the_slot_index_is_made_of_on_the_shipped_routing_range() {
    let reading = reading_on(
        HEADLINE_KEYS,
        PUBLISHED_WIDTH,
        KeyShape::Padded,
        SHIPPED_END_BUCKET,
    );

    // ---- FIXTURE REGIME FIRST ----
    assert!(
        reading.buckets < reading.objects,
        "[CLAUSE amortised-regime] the fixture put {} object(s) in {} bucket(s), so no bucket \
         holds more than one and the per-bucket terms amortise over nothing -- this is the \
         unbounded regime wearing the shipped range's name",
        reading.objects,
        reading.buckets
    );
    let occupancy = reading.objects as f64 / reading.buckets as f64;
    assert!(
        occupancy > 4.0,
        "[CLAUSE amortised-regime] {occupancy:.2} object(s) per bucket is not the regime the \
         published 226.35 B/object was taken in (7.81), so the per-bucket terms below amortise \
         over the wrong denominator"
    );
    assert_eq!(
        reading.pages, reading.objects,
        "[CLAUSE one-page-an-object] the fixture holds {} page(s) for {} object(s), so a per-PAGE \
         term and a per-OBJECT term are different quantities here and every B/object figure below \
         would silently be a B/page figure",
        reading.pages, reading.objects
    );

    let walked = walk(&reading);
    let tally = &walked.tally;
    let whole = reading.slot_index_entry_bytes();

    // ---- IDENTITY ONE: THE WHOLE ----
    assert_eq!(
        tally.total(),
        whole,
        "[CLAUSE part-sum-whole] the {} declared term(s) charge {} B against a {whole}-byte \
         `slot_index` entry, so some bytes are unattributed -- and an \"everything else\" column is \
         exactly what this identity exists instead of",
        tally.rows.len(),
        tally.total()
    );

    // ---- IDENTITY TWO: THE PAGE SUBTREE, MEASURED SEPARATELY ----
    //
    // THIS IS THE ONE THAT CATCHES A TERM CHARGED AT THE WRONG LEVEL. `bucket_map` is bucket-keyed
    // and the `page_index` inside it is page-keyed at 7.81 entries a bucket, so a term moved
    // between those two levels is wrong by that factor and the whole still balances.
    let page_level: usize = tally
        .rows
        .iter()
        .filter(|entry| {
            let label: &str = entry.0;
            label.starts_with("page/")
                || label.starts_with("entry/")
                || label == "page_index/open_brace"
        })
        .map(|entry| entry.1.bytes)
        .sum();
    assert_eq!(
        page_level, walked.page_subtree_bytes,
        "[CLAUSE part-sum-page-subtree] the page-level terms charge {page_level} B against {} B of \
         written page index, so a term is charged at the bucket level that belongs at the page \
         level or the reverse -- the error the bucket-keyed NAME of this structure invites",
        walked.page_subtree_bytes
    );

    // ---- EVERY DECLARED TERM WAS CHARGED, AND EVERY CHARGED TERM IS NON-ZERO ----
    //
    // `node/tombstone_elements` is the one optional key: the encoder writes it only for a bucket
    // that has had a removal, and this fixture performs none. It is named rather than tolerated,
    // and its ABSENCE is asserted -- a term that quietly appeared would otherwise be absorbed.
    const OPTIONAL: &[&str] = &["node/tombstone_elements"];
    for (label, scale, ..) in TERMS.iter() {
        let row = tally.rows.get(label).copied().unwrap_or_default();
        if OPTIONAL.contains(label) {
            assert_eq!(
                row.instances, 0,
                "[CLAUSE optional-stays-absent] `{label}` was charged {} time(s) and this fixture \
                 performs no removal, so the fixture is not the one these figures describe",
                row.instances
            );
            continue;
        }
        assert!(
            row.instances > 0,
            "[CLAUSE every-term-charged] `{label}` is declared and was never charged, so either \
             the encoder stopped writing it or this walk stopped finding it, and the partition \
             above is of a different index than the table describes"
        );
        if *label == STRUCTURALLY_EMPTY {
            assert_eq!(
                row.bytes, 0,
                "[CLAUSE structurally-empty] `{label}` holds {} B; it is the always-absent \
                 component the key composer passes as a literal `None`, and content there means \
                 the composite now names something",
                row.bytes
            );
            continue;
        }
        assert!(
            row.bytes > 0,
            "[CLAUSE every-term-nonzero] `{label}` is charged {} instance(s) and 0 B, so it is a \
             zero wearing a name and its bytes went into whichever term was charged next",
            row.instances
        );
        let _ = scale;
    }

    // ---- THE TWO RECONSTRUCTIONS, COUNTED ----
    assert_eq!(
        walked.keys_reconstructed, reading.pages,
        "[CLAUSE key-restates-the-entry] {} of {} page key(s) were rebuilt from the entry's own \
         written bytes",
        walked.keys_reconstructed, reading.pages
    );
    assert_eq!(
        walked.slots_reconstructed, reading.buckets,
        "[CLAUSE routing-slot-restates-the-map-key] {} of {} bucket(s) restate their map key in \
         `routing_slot`",
        walked.slots_reconstructed, reading.buckets
    );
    // THE SECOND-LARGEST TERM, AND IT IS NOT CLASSED BY ITS NAME EITHER. `node/object_index` holds
    // the ids of the objects a bucket carries, and those ids are a pure function of the shard, the
    // kind and the object key -- all three of which the page entries in the SAME BUCKET write. So
    // the question is answerable by derivation, and it is answered on every bucket rather than
    // argued from the shape.
    assert_eq!(
        walked.buckets_whose_object_index_is_derivable, reading.buckets,
        "[CLAUSE object-index-restates-the-pages] {} of {} bucket(s) write an `object_index` that \
         is exactly the set of ids derivable from the kinds and object keys of their own page \
         entries -- a bucket that writes an id no page beside it accounts for carries something \
         the pages do not, and calling this term redundancy would be wrong",
        walked.buckets_whose_object_index_is_derivable, reading.buckets
    );

    report(&reading, &walked);

    // ---- THE TWO COPIES OF EACH PER-PAGE FACT, AND EXACTLY ONE OF THEM IS NEEDED ----
    //
    // The key side and the entry side state the same three facts -- the object, the kind and the
    // address -- and the reconstruction above was asserted in the direction that matters: the key
    // is rebuildable from the entry. So a page can be written with the key or with the fields, and
    // the two sides are NOT the same price, which is the whole point of pricing them apart.
    let key_side = tally.sum_of(PAGE_KEY_TERMS);
    let entry_side = tally.sum_of(ENTRY_RESTATED_TERMS);
    let key_address = tally.sum_of(KEY_ADDRESS_TERMS);
    let entry_address = tally.sum_of(ENTRY_ADDRESS_TERMS);
    println!();
    println!("================ THE SAME THREE FACTS, WRITTEN TWICE ================");
    println!(
        "  the composite MAP KEY        {key_side:>10} B = {:>8.3} B/object = {:>5.2}% of \
         `slot_index`",
        key_side as f64 / reading.objects as f64,
        100.0 * key_side as f64 / whole as f64
    );
    println!(
        "  the ENTRY's three fields     {entry_side:>10} B = {:>8.3} B/object = {:>5.2}% of \
         `slot_index`",
        entry_side as f64 / reading.objects as f64,
        100.0 * entry_side as f64 / whole as f64
    );
    println!(
        "  together                     {:>10} B = {:>8.3} B/object = {:>5.2}% of `slot_index`",
        key_side + entry_side,
        (key_side + entry_side) as f64 / reading.objects as f64,
        100.0 * (key_side + entry_side) as f64 / whole as f64
    );
    assert!(
        key_side > 0 && entry_side > 0,
        "[CLAUSE two-copies] one of the two sides is 0 B, so there is only one copy and nothing \
         here is redundancy"
    );
    println!(
        "  AND THE FIVE ADDRESS VALUES ARE NOT THE SAME PRICE ON THE TWO SIDES. As decimals inside \
         the key they are {key_address} B = {:.3} B/object; as the named `address` object beside it \
         they are {entry_address} B = {:.3} B/object -- {:.2}x, and the difference is field names, \
         braces, separators and a nil slot rather than any value.",
        key_address as f64 / reading.objects as f64,
        entry_address as f64 / reading.objects as f64,
        entry_address as f64 / key_address as f64
    );
    assert!(
        entry_address > key_address,
        "[CLAUSE the-named-address-costs-more] the named `address` object is {entry_address} B \
         against {key_address} B of decimals in the key, so the direction this arm reports is wrong"
    );

    // ---- A SLOT THE WRITER FILLS WITH A LITERAL `None`, ON EVERY PAGE ----
    assert_eq!(
        walked.nil_object_id_slots, reading.pages,
        "[CLAUSE the-nil-slot-is-always-nil] {} of {} stored address(es) write `oi` as null; \
         `impl From<ElementEntry> for BlockAddressWire` fills it from a LITERAL `None`, so a page \
         carrying a value there means that literal has stopped being one and this term is no \
         longer a hole",
        walked.nil_object_id_slots, reading.pages
    );
    println!(
        "  THE `oi` SLOT IS NULL ON ALL {} PAGES and costs {:.3} B/object. The writer fills it \
         from a literal `None` -- the type it converts from has no such field -- and the reader \
         consumes it only as the right-hand side of `block_id.or(object_id)`. It is not skipped \
         for a reason the source states: the index log packs this struct POSITIONALLY, so a \
         `skip_serializing_if` here shortens the array and shifts every slot after it.",
        walked.nil_object_id_slots,
        tally.bytes_of("entry/address/oi") as f64 / reading.objects as f64
    );

    println!();
    println!("================ THE ANSWER AT {} OBJECTS ================", reading.objects);
    println!(
        "  `slot_index` is {whole} B = {:.2} B/object, against json_stream {:.2} B/object in all \
         ({:.2}%).",
        whole as f64 / reading.objects as f64,
        reading.json_len as f64 / reading.objects as f64,
        100.0 * whole as f64 / reading.json_len as f64
    );
    println!(
        "  {} object(s) in {} bucket(s) = {occupancy:.2} per bucket; {} page(s), one an object.",
        reading.objects, reading.buckets, reading.pages
    );
    println!(
        "  THE PAGE ENTRY'S MAP KEY is {:.3} B/object of it, and every character of it was rebuilt \
         from the entry it is filed against, on all {} pages.",
        (tally.bytes_of("page/map_key_quotes")
            + tally.bytes_of("page/map_key/kind")
            + tally.bytes_of("page/map_key/object_key")
            + tally.bytes_of("page/map_key/component_slot")
            + tally.bytes_of("page/map_key/block_slab_id")
            + tally.bytes_of("page/map_key/offset")
            + tally.bytes_of("page/map_key/length")
            + tally.bytes_of("page/map_key/block_id")
            + tally.bytes_of("page/map_key/generation")
            + tally.bytes_of("page/map_key/separators")
            + tally.bytes_of("page/map_key/empty_slot_separator")) as f64
            / reading.objects as f64,
        reading.pages
    );
    println!(
        "  msgpack_payload {:.2} B/object and shipped container {:.2} B/object, NEITHER of which \
         names a field: the first is not the stream this partition is taken on and the second is a \
         zstd frame.",
        reading.msgpack_len as f64 / reading.objects as f64,
        reading.container_len as f64 / reading.objects as f64
    );
    println!();
}

// =================================================================================================

/// HOW EACH TERM OF `slot_index` SCALES WITH THE STORE, AND WHETHER ITS DECLARED CLASS IS TRUE.
///
/// One conclusion: the slope of every term over a swept store, with each term's DECLARED scale
/// checked against its MEASURED instance count at every size.
///
/// THIS IS WHERE THE CLASSIFICATION IS EARNED. A name-based reading of `slot_index/bucket_map` says
/// bucket-keyed and is wrong by the bucket occupancy for everything inside its `page_index`. So no
/// term is classed by its name: a term declared per-page whose instance count is not EXACTLY the
/// page count fails here, and so does a per-bucket term that is not exactly the bucket count and a
/// per-shard term that is not exactly one. An earlier probe on this structure reported 3 -> 1 -> 0
/// copies per object as the store grew, from a ceiling that did not move with the corpus; an
/// identity asserted at three sizes is what refuses that shape of result.
#[test]
fn how_each_term_of_the_slot_index_scales_with_the_store() {
    let readings: Vec<Reading> = SIZES
        .into_iter()
        .map(|keys| reading_on(keys, PUBLISHED_WIDTH, KeyShape::Padded, SHIPPED_END_BUCKET))
        .collect();
    let walks: Vec<Walked> = readings.iter().map(walk).collect();

    println!();
    println!(
        "================ THE STORE SWEEP ON 0..{SHIPPED_END_BUCKET}, {PUBLISHED_WIDTH}-BYTE KEYS \
         ================"
    );
    println!(
        "  {:>8} {:>9} {:>8} {:>12} {:>14} {:>16} {:>12}",
        "objects", "buckets", "obj/bkt", "pages", "slot_index B", "B/object", "json B/object"
    );
    for reading in readings.iter() {
        println!(
            "  {:>8} {:>9} {:>8.2} {:>12} {:>14} {:>16.2} {:>13.2}",
            reading.objects,
            reading.buckets,
            reading.objects as f64 / reading.buckets as f64,
            reading.pages,
            reading.slot_index_entry_bytes(),
            reading.slot_index_entry_bytes() as f64 / reading.objects as f64,
            reading.json_len as f64 / reading.objects as f64
        );
    }

    // ---- FIXTURE REGIME, AT EVERY POINT ----
    for reading in readings.iter() {
        assert_eq!(
            reading.pages, reading.objects,
            "[CLAUSE one-page-an-object] {} object(s) hold {} page(s)",
            reading.objects, reading.pages
        );
    }
    let large = readings.last().expect("a largest reading");
    assert!(
        large.objects as f64 / large.buckets as f64 > 4.0,
        "[CLAUSE amortised-regime] the largest point is {:.2} object(s) per bucket",
        large.objects as f64 / large.buckets as f64
    );

    // ---- THE DECLARED SCALE AGAINST THE MEASURED INSTANCE COUNT, AT EVERY SIZE ----
    for (reading, walked) in readings.iter().zip(walks.iter()) {
        for (label, row) in walked.tally.rows.iter() {
            let (scale, ..) = declared(label);
            let expected = match scale {
                Scale::PerPage => reading.pages,
                Scale::PerBucket => reading.buckets,
                Scale::PerShard => 1,
            };
            assert_eq!(
                row.instances, expected,
                "[CLAUSE scale-is-measured] `{label}` is declared {} and was charged {} \
                 instance(s) at {} object(s) in {} bucket(s), where that class means {expected} -- \
                 so the class was read off the term's NAME and not off the bytes",
                scale.label(),
                row.instances,
                reading.objects,
                reading.buckets
            );
        }
    }

    // ---- THE PER-TERM SLOPE ----
    let small = &readings[0];
    let small_walk = &walks[0];
    let large_walk = walks.last().expect("a largest walk");
    let span = (large.objects - small.objects) as f64;
    println!();
    println!(
        "================ THE SLOPE OF EACH TERM OVER {} -> {} OBJECTS (json_stream) ================",
        small.objects, large.objects
    );
    println!(
        "  {:<38} {:>13} {:>13} {:>12} {:>12} {:>12}",
        "term", "B at small", "B at large", "slope B/obj", "constant B", "scale"
    );
    let mut labels: Vec<&'static str> = large_walk.tally.rows.keys().copied().collect();
    let slope_of = |label: &str| -> f64 {
        let at_small = small_walk.tally.bytes_of(label) as f64;
        let at_large = large_walk.tally.bytes_of(label) as f64;
        (at_large - at_small) / span
    };
    labels.sort_by(|left, right| {
        slope_of(right)
            .partial_cmp(&slope_of(left))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.cmp(right))
    });
    let mut slope_total = 0.0f64;
    let mut constant_total = 0.0f64;
    for label in labels.iter() {
        let at_small = small_walk.tally.bytes_of(label) as f64;
        let at_large = large_walk.tally.bytes_of(label) as f64;
        let slope = (at_large - at_small) / span;
        let constant = at_small - slope * small.objects as f64;
        slope_total += slope;
        constant_total += constant;
        let (scale, ..) = declared(label);
        println!(
            "  {:<38} {:>13.0} {:>13.0} {:>12.3} {:>12.0} {:>12}",
            label,
            at_small,
            at_large,
            slope,
            constant,
            scale.label()
        );
    }
    println!(
        "  {:<38} {:>13} {:>13} {:>12.3} {:>12.0}",
        "TOTAL", "", "", slope_total, constant_total
    );

    // ---- THE SLOPE BY SCALE, WHICH IS WHAT THE THREE CLASSES BUY ----
    println!();
    println!("================ THE SLOPE BY SCALE ================");
    for scale in [Scale::PerPage, Scale::PerBucket, Scale::PerShard] {
        let at_small = small_walk.tally.at_scale(scale) as f64;
        let at_large = large_walk.tally.at_scale(scale) as f64;
        let slope = (at_large - at_small) / span;
        println!(
            "  {:<12} {:>10.0} B -> {:>10.0} B   slope {:>8.3} B/object   {:>8.3} -> {:>8.3} \
             B/object as written",
            scale.label(),
            at_small,
            at_large,
            slope,
            at_small / small.objects as f64,
            at_large / large.objects as f64
        );
    }
    println!(
        "  THE MIDDLE CLASS IS WHY THERE ARE THREE. The bucket count saturates on this range -- {} \
         bucket(s) at {} objects and {} at {} -- so the per-bucket terms grow and then stop, and \
         per object they fall without ever being constant. A \"grows / bounded\" taxonomy has \
         nowhere to put that, and it is where the amortisation this range exists for happens.",
        small.buckets, small.objects, large.buckets, large.objects
    );

    // ---- `slot_index` IS NOT A LINE ON THIS RANGE, AND EXACTLY ONE CLASS IS WHY ----
    //
    // THE PUBLISHED 226.35 B/object IS A DIVISION AT THE TOP OF THIS SWEEP, NOT A SLOPE, and on
    // this range those are different quantities: the bucket count SATURATES, so the per-bucket
    // class contributes 212 B/object at 500 objects and 49 at 8,000. Quoting a slope and a
    // division under one name is how a figure becomes fiction, so both are measured here, each is
    // named, and the clauses below say which one the decomposition decomposes.
    let middle = &readings[1];
    let middle_walk = &walks[1];
    let half = |low: &Reading, high: &Reading| -> f64 {
        (high.slot_index_entry_bytes() as f64 - low.slot_index_entry_bytes() as f64)
            / (high.objects - low.objects) as f64
    };
    let half_at_scale = |low: (&Reading, &Walked), high: (&Reading, &Walked), scale: Scale| -> f64 {
        (high.1.tally.at_scale(scale) as f64 - low.1.tally.at_scale(scale) as f64)
            / (high.0.objects - low.0.objects) as f64
    };
    let lower = half(small, middle);
    let upper = half(middle, large);
    let division = large.slot_index_entry_bytes() as f64 / large.objects as f64;

    println!();
    println!("================ A SLOPE AND A DIVISION ARE NOT THE SAME NUMBER HERE ================");
    println!(
        "  whole-sweep slope {slope_total:.2} B/object; lower half ({} -> {}) {lower:.2}; upper \
         half ({} -> {}) {upper:.2}; DIVISION at {} objects {division:.2} B/object.",
        small.objects, middle.objects, middle.objects, large.objects, large.objects
    );
    println!(
        "  {:<12} {:>14} {:>14} {:>14}",
        "scale", "lower half", "upper half", "division@top"
    );
    for scale in [Scale::PerPage, Scale::PerBucket, Scale::PerShard] {
        println!(
            "  {:<12} {:>14.3} {:>14.3} {:>14.3}",
            scale.label(),
            half_at_scale((small, small_walk), (middle, middle_walk), scale),
            half_at_scale((middle, middle_walk), (large, large_walk), scale),
            large_walk.tally.at_scale(scale) as f64 / large.objects as f64
        );
    }

    // THE PUBLISHED FIGURE IS THE DIVISION, so that is what is pinned.
    let published = 226.35f64;
    assert!(
        (division - published).abs() / published < 0.005,
        "[CLAUSE matches-the-published-division] `slot_index` divides to {division:.2} B/object at \
         {} object(s) against the published {published:.2} for this range, so the decomposition is \
         of a different quantity than the figure it claims to decompose",
        large.objects
    );

    // THE PER-PAGE CLASS IS THE LINE. Its two half-slopes must agree: a per-page term whose cost
    // moved with the store would mean the per-object figure has no single value at all.
    let page_lower = half_at_scale((small, small_walk), (middle, middle_walk), Scale::PerPage);
    let page_upper = half_at_scale((middle, middle_walk), (large, large_walk), Scale::PerPage);
    assert!(
        (page_lower - page_upper).abs() / page_upper < 0.05,
        "[CLAUSE the-per-page-class-is-a-line] the per-page class grows {page_lower:.3} B/object \
         over the lower half and {page_upper:.3} over the upper, so the class that is supposed to \
         be one cost per stored page is not"
    );

    // AND THE PER-BUCKET CLASS IS THE NON-LINEARITY, because the range bounds it.
    assert_eq!(
        large.buckets,
        SHIPPED_END_BUCKET as usize + 1,
        "[CLAUSE the-per-bucket-class-saturates] the largest point holds {} bucket(s) on a range \
         of {} -- the per-bucket class has not saturated, so the fall in its per-object cost below \
         is not the saturation this arm attributes it to",
        large.buckets,
        SHIPPED_END_BUCKET as usize + 1
    );
    let bucket_small = small_walk.tally.at_scale(Scale::PerBucket) as f64 / small.objects as f64;
    let bucket_large = large_walk.tally.at_scale(Scale::PerBucket) as f64 / large.objects as f64;
    assert!(
        bucket_small / bucket_large > 3.0,
        "[CLAUSE the-per-bucket-class-saturates] the per-bucket class is {bucket_small:.2} \
         B/object at {} objects and {bucket_large:.2} at {}, a fall of only {:.2}x -- too little to \
         be the amortisation that makes this range's figure different from the unbounded one",
        small.objects,
        large.objects,
        bucket_small / bucket_large
    );

    // THE THREE CLASSES ACCOUNT FOR THE WHOLE OF EACH HALF, exactly. An accounting identity rather
    // than a threshold on one class: a term misfiled between two classes changes two of the three
    // and leaves the total alone, and only this equation sees that it still adds up by accident.
    for (name, low, high) in [
        ("lower", (small, small_walk), (middle, middle_walk)),
        ("upper", (middle, middle_walk), (large, large_walk)),
    ]
    .map(|(name, low, high)| (name, low, high))
    {
        let whole = (high.0.slot_index_entry_bytes() as f64 - low.0.slot_index_entry_bytes() as f64)
            / (high.0.objects - low.0.objects) as f64;
        let summed: f64 = [Scale::PerPage, Scale::PerBucket, Scale::PerShard]
            .into_iter()
            .map(|scale| half_at_scale(low, high, scale))
            .sum();
        assert!(
            (whole - summed).abs() < 1e-6,
            "[CLAUSE classes-account-for-the-whole] on the {name} half the whole grows \
             {whole:.6} B/object and the three classes grow {summed:.6} between them"
        );
    }
    println!();
}

// =================================================================================================

/// WHAT REMOVING A REDUNDANT TERM WOULD COST, ON THE PAYLOAD AND ON A CONTAINER THAT LANDS.
///
/// One conclusion: per candidate removal, the saving on `json_stream` and the saving on the
/// `TSIDX\x01` codec-1 container, taken on BOTH key shapes -- because duplication is not byte cost
/// and a padded fixture reports almost none of it.
///
/// THE TWO CONTAINER ARMS ARE COMPARED BEFORE EITHER IS QUOTED. The counterfactual has to be
/// RE-ENCODED, and the only re-encoding this module can take byte-for-byte as production writes it
/// is the JSON one, so the prices are on the codec-1 container. How good a proxy that is for the
/// msgpack one is not assumed: the clause below asserts the two baselines are within a few percent
/// of each other, measured, and the figure is printed beside every price.
#[test]
fn what_removing_a_redundant_slot_index_term_would_cost_the_shipped_container() {
    const CANDIDATES: [Candidate; 8] = [
        Candidate::Control,
        Candidate::PageKeyBecomesTheHandle,
        Candidate::PageKeyDropsTheEmptySlot,
        Candidate::AddressDropsTheAlwaysNilSlot,
        Candidate::EntryDropsObjectKey,
        Candidate::EntryDropsModelId,
        Candidate::EntryDropsAddress,
        Candidate::EntryDropsWhatTheKeyRestates,
    ];

    // TWO KEY SHAPES AND TWO STORE SIZES, BECAUSE A SIGN TAKEN ONCE IS AN ATTRIBUTION. The two
    // headline candidates below are asserted by the DIRECTION of their container delta, and a
    // direction read off one measurement is a claim about that measurement. Four points, and the
    // clause names which four it held on.
    for shape in [KeyShape::Padded, KeyShape::Scattered] {
        for objects in [2_000usize, HEADLINE_KEYS] {
        let reading = reading_on(objects, PUBLISHED_WIDTH, shape, SHIPPED_END_BUCKET);
        assert!(
            reading.buckets < reading.objects,
            "[CLAUSE amortised-regime] {} object(s) in {} bucket(s)",
            reading.objects,
            reading.buckets
        );

        let baseline_stream = slot_index_entry_bytes_of(&reading.value);
        let baseline_container = json_container_bytes(&reading.value);
        let shipped_container = reading.container_len;
        let apart = (baseline_container as f64 - shipped_container as f64).abs()
            / shipped_container as f64;

        println!();
        println!(
            "================ WHAT A REMOVAL IS WORTH, {} KEYS, {} OBJECTS ================",
            shape.label(),
            reading.objects
        );
        println!(
            "  baseline: json_stream `slot_index` {baseline_stream} B = {:.2} B/object; whole \
             json_stream {} B; codec-1 container {baseline_container} B = {:.2} B/object; SHIPPED \
             msgpack container {shipped_container} B = {:.2} B/object -- the two container arms are \
             {:.2}% apart.",
            baseline_stream as f64 / reading.objects as f64,
            reading.json_len,
            baseline_container as f64 / reading.objects as f64,
            shipped_container as f64 / reading.objects as f64,
            100.0 * apart
        );
        assert!(
            apart < 0.15,
            "[CLAUSE container-arms-agree] the codec-1 container is {:.2}% away from the shipped \
             msgpack one, which is too far for a price taken on the first to be quoted about the \
             second",
            100.0 * apart
        );

        println!(
            "  {:<42} {:>14} {:>13} {:>14} {:>13} {:>10}",
            "candidate", "stream saved", "B/object", "container saved", "B/object", "coll'pse"
        );
        for candidate in CANDIDATES {
            let doctored = doctor(&reading, candidate);
            let stream = slot_index_entry_bytes_of(&doctored);
            let container = json_container_bytes(&doctored);
            let stream_saved = baseline_stream as i64 - stream as i64;
            let container_saved = baseline_container as i64 - container as i64;
            if candidate == Candidate::Control {
                // ---- THE CONTROL, AND IT IS THE FIRST THING CHECKED ----
                let rebuilt = serde_json::to_vec(&doctored).expect("the control serializes");
                let original = serde_json::to_vec(&reading.value).expect("the baseline serializes");
                assert_eq!(
                    rebuilt, original,
                    "[CLAUSE doctoring-control] rebuilding the page index without changing \
                     anything did not reproduce the stream, so every saving below is partly the \
                     cost of the doctoring itself"
                );
                assert_eq!(
                    stream_saved, 0,
                    "[CLAUSE doctoring-control] the control saves {stream_saved} B of stream"
                );
                assert_eq!(
                    container_saved, 0,
                    "[CLAUSE doctoring-control] the control saves {container_saved} B of container"
                );
            }
            let collapse = if container_saved == 0 {
                f64::INFINITY
            } else {
                stream_saved as f64 / container_saved as f64
            };
            println!(
                "  {:<42} {:>14} {:>13.3} {:>14} {:>13.3} {:>10.2}x",
                candidate.label(),
                stream_saved,
                stream_saved as f64 / reading.objects as f64,
                container_saved,
                container_saved as f64 / reading.objects as f64,
                collapse
            );
            if candidate != Candidate::Control {
                assert!(
                    stream_saved > 0,
                    "[CLAUSE a-removal-removes-bytes] `{}` saves {stream_saved} B of stream, so \
                     either the term was not written or the doctoring did not reach it",
                    candidate.label()
                );
            }

            // ---- THE TWO SIGNS THIS ARM EXISTS TO PIN ----
            //
            // THE BIGGEST-LOOKING REDUNDANCY IS NOT WORTH REMOVING, and that is a measured
            // negative rather than a caution. The composite key is the most duplicated structure
            // in the index and it is also the most COMPRESSIBLE one: sorted, sharing long
            // prefixes, and mostly decimal runs that repeat. The handle the live map already keys
            // the page by is a hash, so a stream that looks 19 B/object shorter lands LARGER.
            if candidate == Candidate::PageKeyBecomesTheHandle {
                assert!(
                    container_saved <= 0,
                    "[CLAUSE the-key-is-free-on-the-artefact] re-keying the page index by its \
                     handle saved {container_saved} B of container at {} {} object(s) -- the \
                     recorded direction is that it COSTS, and a flip here is the finding changing \
                     rather than the fixture",
                    reading.objects,
                    shape.label()
                );
            }
            // AND THE OTHER DIRECTION IS WORTH TAKING, on the artefact and not only on the stream.
            if candidate == Candidate::EntryDropsWhatTheKeyRestates {
                assert!(
                    container_saved > 0,
                    "[CLAUSE the-entry-side-pays-on-the-artefact] dropping the three fields the \
                     key restates saved {container_saved} B of container at {} {} object(s)",
                    reading.objects,
                    shape.label()
                );
                assert!(
                    container_saved as f64 / baseline_container as f64 > 0.10,
                    "[CLAUSE the-entry-side-pays-on-the-artefact] it saved only {:.2}% of the \
                     container at {} {} object(s), which is not the order this arm reports",
                    100.0 * container_saved as f64 / baseline_container as f64,
                    reading.objects,
                    shape.label()
                );
            }
        }
        println!(
            "  `collapse` is stream bytes per container byte: how much of the saving the zstd frame \
             was already taking, and a NEGATIVE one is a removal that makes the artefact bigger. On \
             {} keys this is the {} end of the range, and neither end may be quoted on its own.",
            shape.label(),
            match shape {
                KeyShape::Padded => "optimistic (a constant run plus an ordinal compresses)",
                KeyShape::Scattered => "pessimistic (the keys share no structure)",
            }
        );
        }
    }
    println!();
    println!("================ WHICH DIRECTION IS THE ONE WORTH TAKING ================");
    println!(
        "  The three per-page facts -- the object, the kind and the address -- are each written \
         TWICE: once inside the composite map key, once as fields of the entry filed under it. \
         EXACTLY ONE of the two sides may go, never both, and the two sides are nothing like the \
         same price on the artefact."
    );
    println!(
        "  DROPPING THE KEY COSTS. It is 41 B/object of stream and the most compressible structure \
         in the index -- sorted, sharing prefixes, mostly repeating decimal runs -- so replacing it \
         with the hash the live map already keys the page by lands a LARGER container on every \
         point measured above."
    );
    println!(
        "  DROPPING THE ENTRY'S THREE FIELDS PAYS, and it is the larger term as well: 104 B/object \
         of stream and 6.7 to 7.2 B/object of container out of 29 to 42, because a named encoding \
         spends its bytes on field names, braces, a nil slot and separators that the key spends on \
         digits."
    );
    println!(
        "  WHAT TAKING IT REQUIRES IS NAMED RATHER THAN GLOSSED: the decoder would have to PARSE \
         the key, which today it does not -- \
         `the_page_keys_characters_are_not_read_back_by_the_decoder` measures that the page index \
         decodes through `into_values()` and the key's characters reach nothing. So the cheap \
         direction turns a term that is written and discarded into the authority for three facts, \
         which is a load-path change and not a byte change."
    );
    println!();
}

// =================================================================================================

/// THE PAGE KEY'S CHARACTERS ARE NOT READ BACK, AND THAT IS MEASURED ON THE PRODUCTION DECODER.
///
/// One conclusion: an index whose every page key is replaced by the decimal of the handle the live
/// map files that page under decodes to the same `slot_index`, byte for byte, as the undoctored one.
/// So the composite key is not a term a reader consumes; it is a name the writer composes and the
/// reader discards.
///
/// BOTH SIDES GO THROUGH THE DECODER, which is what makes this a comparison of the DOCTORING rather
/// than of the decoder's normalisations. The undoctored bytes are decoded and re-encoded too, and
/// the two re-encodes are compared -- so anything the load path rewrites (a flag it clears, a map
/// it repacks) moves both sides equally and cannot read as agreement.
///
/// AND THE INSTRUMENT IS PROVED ABLE TO FIRE. A second doctoring changes one character of one
/// entry's `object_key` -- a field the decoder DOES read -- and the same comparison must FAIL. A
/// round trip that cannot tell a changed index from an unchanged one says nothing about either.
#[test]
fn the_page_keys_characters_are_not_read_back_by_the_decoder() {
    let reading = reading_on(2_000, PUBLISHED_WIDTH, KeyShape::Padded, SHIPPED_END_BUCKET);
    assert!(
        reading.buckets < reading.objects,
        "[CLAUSE amortised-regime] {} object(s) in {} bucket(s)",
        reading.objects,
        reading.buckets
    );

    let through_the_decoder = |value: &serde_json::Value| -> Vec<u8> {
        let bytes = serde_json::to_vec(value).expect("the index serializes");
        let decoded = crate::engine::decode_index_bytes(&bytes)
            .expect("a served index in plain JSON is a shape the decoder sniffs and accepts");
        let again = serde_json::to_value(&decoded).expect("the decoded index re-serializes");
        serde_json::to_vec(
            again
                .as_object()
                .expect("an object")
                .get("slot_index")
                .expect("slot_index"),
        )
        .expect("slot_index serializes")
    };

    let baseline = through_the_decoder(&reading.value);
    assert!(
        !baseline.is_empty(),
        "[CLAUSE the-round-trip-carries-pages] the decoded `slot_index` is empty, so the \
         comparison below would hold for an index that lost everything"
    );
    assert!(
        baseline.len() > 64 * reading.objects,
        "[CLAUSE the-round-trip-carries-pages] the decoded `slot_index` is {} B for {} object(s), \
         which is too little to be carrying one page each",
        baseline.len(),
        reading.objects
    );

    let rekeyed = through_the_decoder(&doctor(&reading, Candidate::PageKeyBecomesTheHandle));
    assert_eq!(
        rekeyed.len(),
        baseline.len(),
        "[CLAUSE page-key-is-not-read] re-keying every page index by its handle produced a \
         `slot_index` of {} B against {} B, so the key's characters reach something",
        rekeyed.len(),
        baseline.len()
    );
    assert_eq!(
        rekeyed, baseline,
        "[CLAUSE page-key-is-not-read] re-keying every page index by its handle changed the \
         decoded index, so the composite key carries a term the decoder consumes and it is not \
         pure redundancy"
    );

    // ---- THE INSTRUMENT, PROVED ABLE TO FIRE ----
    let mut tampered = reading.value.clone();
    let mut touched = 0usize;
    {
        let bucket_map = tampered
            .get_mut("slot_index")
            .expect("slot_index")
            .get_mut("bucket_map")
            .expect("bucket_map")
            .as_object_mut()
            .expect("an object");
        for node in bucket_map.values_mut() {
            let pages = node
                .get_mut("page_index")
                .expect("page_index")
                .as_object_mut()
                .expect("an object");
            for entry in pages.values_mut() {
                if touched > 0 {
                    break;
                }
                let fields = entry.as_object_mut().expect("an object");
                let was = fields
                    .get("object_key")
                    .expect("object_key")
                    .as_str()
                    .expect("a string")
                    .to_string();
                let mut now = was.clone();
                now.replace_range(0..1, "Z");
                assert_ne!(now, was, "the tamper must change the key");
                fields.insert(
                    "object_key".to_string(),
                    serde_json::Value::String(now),
                );
                touched += 1;
            }
            if touched > 0 {
                break;
            }
        }
    }
    assert_eq!(
        touched, 1,
        "[CLAUSE instrument-fires] the tamper reached {touched} entry(ies) and must reach exactly \
         one, or a comparison that fails says nothing about which change caused it"
    );
    let tampered_out = through_the_decoder(&tampered);
    assert_ne!(
        tampered_out, baseline,
        "[CLAUSE instrument-fires] changing one character of ONE entry's `object_key` -- a field \
         the decoder reads -- left the decoded `slot_index` byte-identical, so the comparison that \
         passed above cannot tell a changed index from an unchanged one"
    );

    println!();
    println!("================ THE PAGE KEY IS WRITTEN AND NOT READ ================");
    println!(
        "  {} object(s), {} page(s), {} bucket(s). Every page key replaced by the decimal of its \
         handle; the decoded `slot_index` is {} B both ways and byte-identical.",
        reading.objects,
        reading.pages,
        reading.buckets,
        baseline.len()
    );
    println!(
        "  The instrument fires: one character changed in one entry's `object_key` moves the \
         decoded index ({} B against {} B).",
        tampered_out.len(),
        baseline.len()
    );
    println!();
}

// =================================================================================================

/// THE SAME DECOMPOSITION ON THE RANGE `load_shard` LEAVES BEHIND -- THE SECONDARY COLUMN.
///
/// One conclusion: the per-scale split of `slot_index` on `0..u32::MAX`, where a key's routing
/// bucket is effectively unique so every per-bucket cost divides by ONE object. It is reported
/// beside the shipped range and never instead of it: the tree's own prose says this is not the
/// regime a meta-server-managed shard runs on, and the two differ by nearly 2x.
///
/// THE REGIME IS ASSERTED IN THE OPPOSITE DIRECTION from the shipped arm: here a fixture in which
/// buckets amortised at all would be measuring something else under this range's name.
#[test]
fn what_the_slot_index_is_made_of_on_the_unbounded_routing_range() {
    let reading = reading_on(2_000, PUBLISHED_WIDTH, KeyShape::Padded, u32::MAX);
    assert_eq!(
        reading.buckets, reading.objects,
        "[CLAUSE unbounded-regime] {} object(s) landed in {} bucket(s) on the unbounded range, so \
         the per-bucket terms amortise here and this is not the regime this arm is named for",
        reading.objects, reading.buckets
    );
    let walked = walk(&reading);
    let whole = reading.slot_index_entry_bytes();
    assert_eq!(
        walked.tally.total(),
        whole,
        "[CLAUSE part-sum-whole] the terms charge {} B against a {whole}-byte entry on the \
         unbounded range",
        walked.tally.total()
    );

    println!();
    println!(
        "================ `slot_index` BY SCALE ON 0..u32::MAX, {} OBJECTS IN {} BUCKETS \
         ================",
        reading.objects, reading.buckets
    );
    for scale in [Scale::PerPage, Scale::PerBucket, Scale::PerShard] {
        let bytes = walked.tally.at_scale(scale);
        println!(
            "  {:<12} {:>12} B  {:>9.3} B/object  {:>6.2}%",
            scale.label(),
            bytes,
            bytes as f64 / reading.objects as f64,
            100.0 * bytes as f64 / whole as f64
        );
    }
    // ---- AND THE DIFFERENCE BETWEEN THE TWO COLUMNS IS ONE CLASS, MEASURED SIDE BY SIDE ----
    //
    // THE SAME CORPUS ON THE SHIPPED RANGE, so the comparison is between two ROUTING RANGES and not
    // between two store sizes. Without this the claim "the fall is the per-bucket class" would be a
    // reading of two figures taken at different sizes, which is the shape of comparison this tree
    // keeps a record of getting wrong.
    let shipped = reading_on(
        reading.objects,
        PUBLISHED_WIDTH,
        KeyShape::Padded,
        SHIPPED_END_BUCKET,
    );
    assert_eq!(
        shipped.objects, reading.objects,
        "[CLAUSE one-corpus-two-ranges] the two columns must be the same corpus"
    );
    assert!(
        shipped.buckets < shipped.objects,
        "[CLAUSE amortised-regime] the shipped-range column put {} object(s) in {} bucket(s)",
        shipped.objects,
        shipped.buckets
    );
    let shipped_walk = walk(&shipped);
    let per_object = |bytes: usize| bytes as f64 / reading.objects as f64;
    println!();
    println!("================ THE SAME CORPUS ON BOTH RANGES, BY SCALE ================");
    println!(
        "  {:<12} {:>16} {:>16} {:>14}",
        "scale", "0..u32::MAX", "0..1023", "difference"
    );
    for scale in [Scale::PerPage, Scale::PerBucket, Scale::PerShard] {
        println!(
            "  {:<12} {:>16.3} {:>16.3} {:>14.3}",
            scale.label(),
            per_object(walked.tally.at_scale(scale)),
            per_object(shipped_walk.tally.at_scale(scale)),
            per_object(walked.tally.at_scale(scale)) - per_object(shipped_walk.tally.at_scale(scale))
        );
    }
    println!(
        "  {:<12} {:>16.3} {:>16.3} {:>14.3}",
        "WHOLE",
        per_object(whole),
        per_object(shipped.slot_index_entry_bytes()),
        per_object(whole) - per_object(shipped.slot_index_entry_bytes())
    );

    // THE PER-PAGE CLASS DOES NOT KNOW WHICH RANGE IT IS ON. That is the statement that makes the
    // whole difference between the two published columns attributable to ONE class rather than
    // spread over the index -- and it is asserted rather than inferred from the two totals.
    //
    // THE EXACT CLAUSE AND THE TOLERANT ONE, AND THE EXACT ONE IS NOT DERIVED FROM IT. The two
    // columns write the same objects in the same order, and a per-page term depends only on the
    // page's own key and address -- so the two totals are not merely close, they are the SAME
    // BYTES, and a tolerance wide enough to survive rounding is wider than drifts worth catching.
    let page_unbounded_bytes = walked.tally.at_scale(Scale::PerPage);
    let page_shipped_bytes = shipped_walk.tally.at_scale(Scale::PerPage);
    assert_eq!(
        page_unbounded_bytes, page_shipped_bytes,
        "[CLAUSE the-per-page-class-is-range-blind] the per-page class is {page_unbounded_bytes} B \
         unbounded and {page_shipped_bytes} B on the shipped range for the SAME corpus -- a \
         per-page term depends only on the page's own key and address, so a difference here means \
         one of them does not"
    );
    let page_unbounded = per_object(page_unbounded_bytes);
    let page_shipped = per_object(page_shipped_bytes);
    assert!(
        (page_unbounded - page_shipped).abs() / page_shipped < 0.02,
        "[CLAUSE the-per-page-class-is-range-blind] the per-page class is {page_unbounded:.3} \
         B/object unbounded and {page_shipped:.3} on the shipped range -- if it moves with the \
         routing range then the fall between the two published columns is not the one class this \
         arm attributes it to"
    );
    let whole_difference = per_object(whole) - per_object(shipped.slot_index_entry_bytes());
    let bucket_difference = per_object(walked.tally.at_scale(Scale::PerBucket))
        - per_object(shipped_walk.tally.at_scale(Scale::PerBucket));
    assert!(
        (whole_difference - bucket_difference).abs() / whole_difference < 0.02,
        "[CLAUSE the-difference-is-one-class] the whole differs by {whole_difference:.3} B/object \
         between the two ranges and the per-bucket class by {bucket_difference:.3}, so the other \
         two classes carry some of it and the attribution is wrong"
    );
    println!(
        "  `slot_index` is {:.2} B/object here against {:.2} on the shipped range, and {:.3} of the \
         {:.3} B/object difference is the per-bucket class alone: EVERY per-bucket term divides by \
         ONE object on this range. The per-page class does not move. Read the two columns together \
         or neither.",
        per_object(whole),
        per_object(shipped.slot_index_entry_bytes()),
        bucket_difference,
        whole_difference
    );
    println!();
}
