// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAGE ENTRY STOPS RESTATING THE KEY IT IS FILED UNDER -- AND ONE OF THE THREE FACTS CANNOT GO.
//!
//! # WHAT #2350 LEFT ON THE TABLE, AND WHICH HALF OF IT IS TAKEN HERE
//!
//! #2350 decomposed `slot_index` into 50 charged terms at 226.349 B/object and found that EVERY
//! per-page fact is written twice -- once as a decimal inside the composite map key and once as a
//! named field on the entry -- at wildly different prices. The same five address values are 12.008
//! B/object inside the key and 53.008 B/object as the named `address` object, 4.41x, all of it
//! field names, braces, separators and a nil slot.
//!
//! It priced both directions of removing the duplication and found them MUTUALLY EXCLUSIVE and of
//! opposite sign on the shipped artefact:
//!
//! ```text
//!     removal                                   stream B/obj     container B/obj
//!     composite page key -> the live handle            19.6       -5.14 to -6.77   COSTS
//!     entry -> drop `object_key`                       30.0       +2.34 to +2.96
//!     entry -> drop `model_id`                         20.0       +0.26 to +0.37
//!     entry -> drop `address`                          54.0       +4.27 to +4.52
//!     entry -> drop all three the key restates        104.0       +6.72 to +7.17   PAYS
//! ```
//!
//! The intuitive change is the wrong one: the composite key is the most compressible structure in
//! the index -- sorted, long shared prefixes, repeating decimal runs -- so replacing it with the
//! handle the live map already keys the page by lands a container 5 to 7 B/object LARGER at every
//! measured point. Exactly one copy of each fact may go, and it is the ENTRY side.
//!
//! What #2350 named as the whole cost of that direction, and deliberately did not pay, is that THE
//! DECODER WOULD HAVE TO PARSE THE KEY, which it did not. That is this change.
//!
//! # AND ONE OF THE THREE CANNOT BE RECONSTRUCTED. THAT IS THE RESULT, NOT A SETBACK
//!
//! `object_key` and `model_id` invert EXACTLY. `address` does NOT, and the field that loses it is
//! not one of the five values -- it is the two PRESENCE bits beside them:
//!
//!   * `block_index_written_key` renders `page.address.block_id().unwrap_or_default()` and
//!     `page.address.generation().unwrap_or_default()`. A `None` renders `0`, and so does a
//!     `Some(0)`.
//!   * `ElementEntry::block_id()` is `None` EXACTLY while the block is log-resident
//!     (`log_backed()` is that derivation and has five production readers), and `Some(0)` is the
//!     first page of an object in a slab. Both are ordinary states of a live page index.
//!   * So THREE distinct addresses render one key, and
//!     [`the_presence_bits_of_an_address_are_not_in_the_key_it_renders`] builds all three and
//!     asserts the keys IDENTICAL while the entries compare unequal.
//!
//! A reconstruction that guessed the BLOCK-ID bit would hand a log-resident page a slab-backed
//! address, and a read would then go looking on a slab for bytes that are still in the log --
//! `log_backed()` is that derivation and it has five production readers. So the address STAYS, and
//! 17-23% of the artefact becomes 2.6-3.3% of it.
//!
//! AND THE SECOND BIT IS WEAKER THAN THIS NOTE FIRST CLAIMED, which is recorded rather than
//! quietly corrected. It read "or move the generation that `block_index_handle` hashes", and the
//! arm asserting that FAILED on its first run: the handle hashes
//! `generation().unwrap_or_default()`, exactly as the key renders it, so at a block id of 0 the
//! presence bit reaches neither. The field's own doc says the generation "is hashed into
//! `block_index_handle`", which is true of its VALUE and not of its presence at zero. One
//! demonstrated lossy bit is what blocks the field; the generation bit is now asserted as what it
//! measurably is, with a control at a non-zero block id showing it is load-bearing where it can be
//! seen.
//!
//! AND THERE IS A SECOND, INDEPENDENT REASON IT CANNOT BE TRIMMED SLOT-WISE instead. Keeping only
//! the two presence slots and dropping `a`, `l` and `oi` would be exact -- and `BlockAddressWire`
//! is SHARED with the index log, which packs it POSITIONALLY (`encode_index_payload_into` uses a
//! plain `rmp_serde::Serializer`), so a slot skipped here shortens that array and shifts every
//! slot after it. That is recorded beside the wire struct itself and it is why the nil `oi` is
//! still written. A served-index-only address wire is a separate change with its own stamp.
//!
//! # WHAT IS MEASURED HERE, AND ON WHICH SURFACE
//!
//! Three surfaces, never mixed, because duplication is not byte cost -- #2350 measured one key
//! byte at 3.000 B of payload and 0.651 B of shipped container:
//!
//!   * `stream` -- the shard as JSON, where a field name is countable.
//!   * `payload` -- the msgpack the shipped codec compresses.
//!   * `container` -- what lands on disk: `TSIDX\x01`, the codec id, and a ZSTD FRAME. A byte
//!     figure on it is a COMPRESSED size and cannot name a field.
//!
//! The headline is on the CONTAINER at the shipped routing range `0..1023`, with the amortised
//! regime (~7.81 objects a bucket) asserted BEFORE anything is reported -- the unbounded range
//! puts one bucket per object and is not what a deployment runs. Both container codecs are
//! reported: codec 2 (msgpack) is what production writes, and codec 1 (JSON) is the surface
//! #2350's prediction was taken on, so that is the arm the prediction is checked against. Two key
//! shapes at both ends, because padded keys measured 0.009 B/byte in one fixture -- an artefact of
//! the fixture -- which is why no figure here is quoted on one shape.
//!
//! # CORRECTNESS, WHICH IS THE REAL RISK IN DELETING STORED DATA
//!
//! `the_renderer_and_the_parse_are_inverse_on_every_block_of_the_shipped_corpus` compares the
//! reconstruction field by field against the LIVE value the encoder was handed, on all 8,000 pages
//! -- not against a re-render of itself, and not on a sample. `a_reload_reads_every_object_back`
//! writes a corpus, drops the engine, reloads from the same directories and reads every object
//! back, with a key never written asserted ABSENT in the same moment. The old-format reader is
//! proved against COMMITTED BYTES in `page_entry_names`, whose `OLD_STORE_INDEX` is the index a
//! pre-change binary really wrote.
//!
//! rust-internal: measures this crate's own index codec, no product behaviour

use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// The shard every fixture here loads.
const SHARD: u64 = 41;
/// The shipped routing range's end bucket -- the range every headline figure is taken on.
const SHIPPED_END_BUCKET: u32 = 1023;
/// The key width #2350's published per-object figures were taken at.
const PUBLISHED_WIDTH: usize = 14;
/// The corpus the headline figures are taken on.
const HEADLINE_KEYS: usize = 8_000;
/// `-` and six digits.
const TAIL_WIDTH: usize = 7;
/// The byte a padded key's run is made of.
const PAD_BYTE: u8 = b'q';
/// The bucket count the shipped range saturates at, and the denominator of the regime assertion.
const SHIPPED_BUCKETS: usize = 1_024;
/// The floor the amortised regime is asserted against: #2350 measured 7.81 objects a bucket.
const REGIME_FLOOR_BLOCKS_PER_BUCKET: f64 = 7.0;

/// What a fixture key's leading bytes are made of -- #2350's two shapes, kept identical so the
/// figures are comparable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyShape {
    /// A constant run plus the ordinal: near-perfectly compressible, a LOWER bound on a container.
    Padded,
    /// The same width with pseudorandom letters: the UPPER bound.
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
            let mut state =
                (ordinal as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
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
// THE FIXTURE
// =================================================================================================

/// A seeded store, held open so the shard table can be walked, with its three surfaces measured.
struct Seeded {
    dir: tempfile::TempDir,
    engine: TemporalEngine,
    objects: usize,
    shape: KeyShape,
}

fn seed(keys: usize, width: usize, shape: KeyShape, end_routing_bucket: u32) -> Seeded {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        table_name: "the-key-a-page-is-filed-under".to_string(),
        shard_uri: "local://the-key-a-page-is-filed-under/41".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
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
    Seeded { dir, engine, objects: keys, shape }
}

/// The three surfaces of a seeded store, with the regime it was taken in asserted first.
struct Surfaces {
    objects: usize,
    pages: usize,
    buckets: usize,
    shape: KeyShape,
    stream: usize,
    payload: usize,
    container: usize,
    json_container: usize,
    slot_index_stream: usize,
}

/// The pre-compression payload the shipped codec writes, pulled back out of the container.
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

/// The `TSIDX\x01` codec-1 container: the magic, the codec id, and a zstd-3 frame over the JSON.
///
/// THE ARM #2350'S PREDICTION IS ON, and the only one re-encodable byte for byte as production
/// writes it. A msgpack payload rebuilt from a `serde_json::Value` diverges at byte 4, because a
/// `Value` writes its keys sorted while the struct writes them in declaration order, and that
/// reordering alone moves the compressed size 5%.
fn json_container_bytes(json: &[u8]) -> usize {
    const MAGIC: &[u8] = b"TSIDX\x01";
    const CODEC_BYTE: usize = 1;
    const ZSTD_LEVEL: i32 = 3;
    let payload = zstd::stream::encode_all(json, ZSTD_LEVEL).expect("the payload compresses");
    MAGIC.len() + CODEC_BYTE + payload.len()
}

fn jlen(value: &serde_json::Value) -> usize {
    serde_json::to_vec(value).expect("a shard value re-serializes").len()
}

impl Seeded {
    fn surfaces(&self) -> Surfaces {
        let shards = self.engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("shard is loaded");
        assert_eq!(
            self.objects,
            shard.strings().len(),
            "[CLAUSE seed-landed] the fixture acked {} string object(s) and the index holds {}, so \
             the denominator every figure below is taken against is not the corpus",
            self.objects,
            shard.strings().len()
        );
        let buckets = shard.bucket_index.bucket_map.len();
        let pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|node| node.block_index.len())
            .sum();
        let container = crate::engine::encode_index_bytes(shard);
        let payload = msgpack_payload_of(&container);
        let json = serde_json::to_vec(shard).expect("the index must serialize as JSON");
        let value: serde_json::Value =
            serde_json::to_value(shard).expect("the index must serialize as a value");
        let slot_index_stream = value
            .as_object()
            .expect("a shard value is an object")
            .get("slot_index")
            .map(|entry| "slot_index".len() + 2 + 1 + jlen(entry))
            .expect("the core index is written under `slot_index`");
        Surfaces {
            objects: self.objects,
            pages,
            buckets,
            shape: self.shape,
            stream: json.len(),
            payload: payload.len(),
            container: container.len(),
            json_container: json_container_bytes(&json),
            slot_index_stream,
        }
    }
}

impl Surfaces {
    /// THE REGIME, ASSERTED BEFORE ANYTHING IS REPORTED.
    ///
    /// A figure taken with one bucket per object is a figure from the unbounded range, which no
    /// deployment runs, and it would be 4x the shipped one on the per-bucket class alone. So the
    /// saturation is a PRECONDITION of every number below it and not a remark beside them.
    fn assert_amortised_regime(&self) {
        assert_eq!(
            SHIPPED_BUCKETS, self.buckets,
            "[CLAUSE amortised-regime] the fixture resident bucket count is {} rather than {}, so \
             the shipped range did not saturate and this is not the regime a deployment runs",
            self.buckets, SHIPPED_BUCKETS
        );
        let per_bucket = self.pages as f64 / self.buckets as f64;
        assert!(
            per_bucket >= REGIME_FLOOR_BLOCKS_PER_BUCKET,
            "[CLAUSE amortised-regime] the fixture holds {per_bucket:.3} page(s) a bucket, under \
             the {REGIME_FLOOR_BLOCKS_PER_BUCKET} this regime is defined by -- every per-object \
             figure below would be dominated by the per-bucket class instead of amortising it"
        );
        assert_eq!(
            self.objects, self.pages,
            "[CLAUSE amortised-regime] the fixture holds {} page(s) for {} object(s); a string \
             object is one page here, and a figure divided by objects over a corpus where it is \
             not would not be a per-page figure at all",
            self.pages, self.objects
        );
    }

    fn report(&self, label: &str) {
        self.assert_amortised_regime();
        let n = self.objects as f64;
        println!(
            "\n=== {label} :: {} keys, {} objects, {} buckets, {:.3} pages/bucket ===",
            self.shape.label(),
            self.objects,
            self.buckets,
            self.pages as f64 / self.buckets as f64
        );
        println!("  stream           (json)            {:>12} B  {:>9.3} B/obj", self.stream, self.stream as f64 / n);
        println!("  stream slot_index                  {:>12} B  {:>9.3} B/obj", self.slot_index_stream, self.slot_index_stream as f64 / n);
        println!("  payload          (msgpack)         {:>12} B  {:>9.3} B/obj", self.payload, self.payload as f64 / n);
        println!("  container codec 2 (SHIPPED)        {:>12} B  {:>9.3} B/obj", self.container, self.container as f64 / n);
        println!("  container codec 1 (json, #2350's)  {:>12} B  {:>9.3} B/obj", self.json_container, self.json_container as f64 / n);
    }
}

// =================================================================================================
// THE REGISTRY PRECONDITION
// =================================================================================================

/// NO STORED MODEL SPELLING HOLDS THE SEPARATOR, SO THE KIND IS THE KEY'S LEFTMOST FIELD.
///
/// The parse takes the kind from the LEFT, at the first colon, and everything between that and the
/// six right-hand fields is the object key -- colons included. That split is only the renderer's
/// inverse if no kind can contain a colon, and "they are all lowercase identifiers" is a reading of
/// a declaration rather than a property of it. Asserted over `StoredModelKind::ALL`, both halves,
/// so a retired row added later is covered without being told to be.
///
/// AND THE ROUND TRIP THROUGH THE REGISTRY IS ASSERTED TOO, because the reconstruction reads
/// `from_stored_name` where the writer wrote `as_str`: a spelling that does not come back is a page
/// filed under the wrong kind, which is the silent corruption the registry's own refusal exists for.
///
/// rust-internal: asserts a property of this crate's kind registry, no product behaviour
#[test]
fn no_stored_model_spelling_holds_the_separator() {
    use crate::engine::storage_bucket_internals::StoredModelKind;
    assert!(
        StoredModelKind::ALL.len() >= 17,
        "[CLAUSE kind-is-one-field] the registry declares {} spelling(s); a floor under it is what \
         stops this arm passing over an empty set",
        StoredModelKind::ALL.len()
    );
    let mut spellings = BTreeSet::new();
    for kind in StoredModelKind::ALL {
        let spelling = kind.as_str();
        assert!(
            !spelling.contains(':'),
            "[CLAUSE kind-is-one-field] the stored spelling {spelling:?} holds the key separator, \
             so a page of that kind would put its kind in two of the key's fields and the parse \
             would read the object key short"
        );
        assert!(
            !spelling.is_empty(),
            "[CLAUSE kind-is-one-field] a stored spelling is empty, so its key would open with a \
             separator and the kind field would be indistinguishable from an absent one"
        );
        assert_eq!(
            Some(*kind),
            StoredModelKind::from_stored_name(spelling),
            "[CLAUSE kind-is-one-field] the spelling {spelling:?} does not come back through \
             `from_stored_name`, so a page of that kind would be refused or filed under another"
        );
        assert!(
            spellings.insert(spelling),
            "[CLAUSE kind-is-one-field] two kinds share the stored spelling {spelling:?}, so the \
             parse cannot name which one a key means"
        );
    }
    println!(
        "[kind-is-one-field] {} distinct colon-free spellings, each inverted by `from_stored_name`",
        spellings.len()
    );
}

// =================================================================================================
// THE RECONSTRUCTION, EXACT AND ON EVERY PAGE
// =================================================================================================

/// THE PARSE IS THE RENDERER'S INVERSE ON EVERY PAGE OF THE SHIPPED CORPUS, FIELD BY FIELD.
///
/// # WHAT IS COMPARED AGAINST WHAT, WHICH IS THE WHOLE WEIGHT OF THIS ARM
///
/// The left side is the reconstruction: the object key and the kind read back out of the key the
/// page is filed under. The right side is the LIVE value the encoder is handed -- the `object_key`
/// off `PageKeysByPosition`, which is what `serialize_page_index` composes the key from, and the
/// `model_id` off the page itself. So this is a parse compared with a SOURCE, not a renderer
/// compared with itself.
///
/// EVERY PAGE, NOT A SAMPLE, and the count is asserted against the corpus so a walk that reached
/// none of them cannot pass. Both of the two key shapes, at the shipped routing range.
///
/// AND THE FIVE NUMERIC SLOTS ARE COMPARED TOO, although they are not being removed: they are what
/// says the key really does carry the address values #2350 priced, and a parse that got them wrong
/// would be a parse whose text slots cannot be trusted either.
///
/// WITH A CONTROL. A one-character change to one object key must make the comparison fail, or the
/// 8,000 equalities above are equalities the instrument cannot tell from inequalities.
///
/// rust-internal: measures this crate's own index codec, no product behaviour
#[test]
fn the_renderer_and_the_parse_are_inverse_on_every_block_of_the_shipped_corpus() {
    for shape in [KeyShape::Padded, KeyShape::Scattered] {
        let seeded = seed(HEADLINE_KEYS, PUBLISHED_WIDTH, shape, SHIPPED_END_BUCKET);
        seeded.surfaces().assert_amortised_regime();
        let shards = seeded.engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("shard is loaded");
        let pages = super::pages_with_identity(&shard.bucket_index);
        assert_eq!(
            HEADLINE_KEYS,
            pages.len(),
            "[CLAUSE every-page-inverts] the walk reached {} page(s) of {HEADLINE_KEYS}; a \
             reconstruction claim over a subset is not the claim this change needs",
            pages.len()
        );
        let mut checked = 0usize;
        for (_, _, object_key, page) in &pages {
            let key = crate::engine::state::block_index_written_key(object_key, page);
            let parts = crate::index_log::block_ref_key_parts(&key)
                .unwrap_or_else(|error| panic!("[CLAUSE every-page-inverts] {error}"));
            assert_eq!(
                &**object_key, parts.object_key,
                "[CLAUSE every-page-inverts] the object key read back out of {key:?} is \
                 {:?} and the encoder was handed {:?}",
                parts.object_key, &**object_key
            );
            assert_eq!(
                Some(page.model_id),
                crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                    parts.kind
                ),
                "[CLAUSE every-page-inverts] the kind read back out of {key:?} is {:?} and the \
                 page holds {:?}",
                parts.kind,
                page.model_id.as_str()
            );
            assert_eq!(
                "", parts.component,
                "[CLAUSE every-page-inverts] the component slot of {key:?} holds {:?}; the \
                 renderer passes a literal `None`, so a non-empty slot means the key was composed \
                 by something else",
                parts.component
            );
            assert_eq!(
                page.address.block_slab_id(),
                parts.block_slab_id,
                "[CLAUSE every-page-inverts] the slab id slot of {key:?} disagrees with the address"
            );
            assert_eq!(
                page.address.offset(),
                parts.offset,
                "[CLAUSE every-page-inverts] the offset slot of {key:?} disagrees with the address"
            );
            assert_eq!(
                page.address.length(),
                parts.length,
                "[CLAUSE every-page-inverts] the length slot of {key:?} disagrees with the address"
            );
            assert_eq!(
                page.address.block_id().unwrap_or_default(),
                parts.block_id,
                "[CLAUSE every-page-inverts] the block id slot of {key:?} disagrees with the address"
            );
            assert_eq!(
                page.address.generation().unwrap_or_default(),
                parts.generation,
                "[CLAUSE every-page-inverts] the generation slot of {key:?} disagrees with the \
                 address"
            );
            checked += 1;
        }
        assert_eq!(
            HEADLINE_KEYS, checked,
            "[CLAUSE every-page-inverts] {checked} page(s) were compared rather than {HEADLINE_KEYS}"
        );
        println!(
            "[every-page-inverts] {shape:?}: {checked} page(s), object key and kind read back out \
             of the key and equal to what the encoder was handed on every one"
        );

        // --- THE CONTROL. The comparison must be able to FAIL. ---
        let (_, _, object_key, page) = &pages[0];
        let key = crate::engine::state::block_index_written_key(object_key, page);
        let mut doctored = key.clone();
        let at = doctored.find('-').expect("a fixture key holds its ordinal tail");
        doctored.replace_range(at..at + 1, "_");
        let doctored_parts = crate::index_log::block_ref_key_parts(&doctored)
            .expect("the doctored key is still an eight-slot composite");
        assert_ne!(
            &**object_key, doctored_parts.object_key,
            "[CLAUSE every-page-inverts] a one-character change to an object key did not change \
             what the parse reads back, so the 8,000 equalities above are equalities this \
             instrument cannot distinguish from inequalities"
        );
    }
}

/// A KEY THAT WOULD NOT ROUND-TRIP IS REFUSED, NOT GUESSED -- AND THE HARD CASES ARE THE POINT.
///
/// # THE CASE THE DESIGN EXISTS FOR
///
/// An OBJECT KEY ARRIVES FROM A CALLER AND MAY HOLD A COLON. The kind may not and a rendered `u64`
/// may not, so the six right-hand fields are cut from the RIGHT and the kind from the LEFT, and
/// what is between them is the object key whatever it contains. The adversarial case is an object
/// key that SPELLS A PLAUSIBLE TAIL -- `x:0:0:1:2:3` -- which a parse that searched for its fields
/// rather than counting them from the end would split in the wrong place and return a short object
/// key with nothing failing. It is driven here, with a colon-laden key and an empty one beside it.
///
/// # AND THE REFUSALS ARE THE SPELLINGS `u64::from_str` WOULD HAVE ACCEPTED
///
/// `+5` and `007` both parse as integers and both re-render as something else, so accepting them
/// would make the parse the renderer's inverse on keys whose round trip is a DIFFERENT STRING.
/// Each is asserted refused, beside the structural refusals -- too few separators, and a head with
/// no separator in it.
///
/// rust-internal: asserts a property of this crate's key codec, no product behaviour
#[test]
fn a_key_that_does_not_round_trip_is_refused_rather_than_guessed() {
    use crate::index_log::{block_ref_key_from_parts, block_ref_key_parts};

    // --- THE OBJECT KEYS THAT MUST SURVIVE THE SPLIT WHOLE. ---
    let awkward = [
        "plain",
        "",
        "a:b",
        "x:0:0:1:2:3",
        "::::",
        "ends:",
        ":starts",
        "0",
        "007:+5",
    ];
    for object_key in awkward {
        let key = block_ref_key_from_parts("string", object_key, None, 7, 9, 126, 4, 4);
        let parts = block_ref_key_parts(&key)
            .unwrap_or_else(|error| panic!("[CLAUSE awkward-keys-survive] {error}"));
        assert_eq!(
            object_key, parts.object_key,
            "[CLAUSE awkward-keys-survive] the object key {object_key:?} rendered to {key:?} and \
             came back as {:?}",
            parts.object_key
        );
        assert_eq!("string", parts.kind, "[CLAUSE awkward-keys-survive] the kind moved");
        assert_eq!("", parts.component, "[CLAUSE awkward-keys-survive] the component slot moved");
        assert_eq!(
            (7, 9, 126, 4, 4),
            (
                parts.block_slab_id,
                parts.offset,
                parts.length,
                parts.block_id,
                parts.generation
            ),
            "[CLAUSE awkward-keys-survive] a numeric slot moved for object key {object_key:?}"
        );
        // AND THE RECOMPOSITION IS CHECKED HERE RATHER THAN IN PRODUCTION, which is where the
        // allocation per page belongs: the parse is exact by index arithmetic, and this is the arm
        // that says so on the shapes that could break it.
        let recomposed = format!(
            "{}:{}:{}:{}:{}:{}:{}:{}",
            parts.kind,
            parts.object_key,
            parts.component,
            parts.block_slab_id,
            parts.offset,
            parts.length,
            parts.block_id,
            parts.generation
        );
        assert_eq!(
            key, recomposed,
            "[CLAUSE awkward-keys-survive] the eight slots of {key:?} do not rebuild it"
        );
    }

    // AND A NON-EMPTY COMPONENT IS CARRIED TOO, because an index written before the element name
    // left the entry spells one -- `page_entry_names::OLD_STORE_INDEX` holds `hash:k:f0:1:9:3:4:4`
    // -- and the object key in such a key must still come back whole rather than absorbing it.
    let named = block_ref_key_from_parts("hash", "a:b", Some("f0"), 1, 9, 3, 4, 4);
    let parts = block_ref_key_parts(&named).expect("an element-named key parses");
    assert_eq!(
        ("hash", "a:b", "f0"),
        (parts.kind, parts.object_key, parts.component),
        "[CLAUSE awkward-keys-survive] an element-named key splits in the wrong place: {named:?}"
    );

    // --- THE REFUSALS, EACH NAMED. ---
    for (key, what) in [
        ("string:k::7:9:126:4", "six separators rather than seven"),
        ("", "nothing at all"),
        ("::::::", "six empty slots and no eighth field"),
        ("string:k::7:9:126:4:+4", "a generation spelled with a leading plus"),
        ("string:k::7:9:126:004:4", "a block id with a leading zero"),
        ("string:k::7:9:0126:4:4", "a length with a leading zero"),
        ("string:k::7:9:126:4:", "an empty generation slot"),
        ("string:k::7:9:126:4:4x", "a generation with a trailing letter"),
        ("string:k::7:9:126:4:-4", "a negative generation"),
        ("stringk::7:9:126:4:4", "a head with no separator in it"),
        (
            "string:k::7:9:126:4:18446744073709551616",
            "a generation one past what a u64 holds",
        ),
    ] {
        let outcome = block_ref_key_parts(key);
        assert!(
            outcome.is_err(),
            "[CLAUSE unparsable-is-refused] the key {key:?} holds {what} and parsed anyway, to \
             {outcome:?} -- a key the renderer could not have written must be refused rather than \
             read as a guess at an object's identity"
        );
        let message = outcome.err().expect("asserted an error above").to_string();
        assert!(
            message.contains(key) || key.is_empty(),
            "[CLAUSE unparsable-is-refused] the refusal of {key:?} does not name the key: {message}"
        );
        println!("[unparsable-is-refused] {what:<48} -> refused");
    }

    // --- AND A CONTROL ON THE REFUSALS: the SAME shape with the defect removed must parse, or
    //     these eleven arms would pass against a parse that refused everything. ---
    assert!(
        block_ref_key_parts("string:k::7:9:126:4:4").is_ok(),
        "[CLAUSE unparsable-is-refused] the well-formed control was refused, so every refusal \
         above is a refusal of everything"
    );
    assert!(
        block_ref_key_parts("string:k::0:0:0:0:0").is_ok(),
        "[CLAUSE unparsable-is-refused] an all-zero key was refused; a single `0` is the one \
         leading zero that is canonical and the slots a first page renders are exactly these"
    );
}

// =================================================================================================
// THE OLD FORMAT, READ BY THE NEW READER, FROM A FIXTURE COMMITTED AS BYTES
// =================================================================================================

/// AN INDEX WRITTEN BEFORE THIS CHANGE IS READ BY THIS BINARY, THROUGH THE WHOLE DOOR.
///
/// # WHY A ROUND TRIP WOULD NOT HAVE BEEN EVIDENCE
///
/// A round trip writes and reads with the SAME build, so both halves are wrong together and agree.
/// What has to be proved is that the reconstruction reads what a PREVIOUS binary wrote, and the
/// only form of a previous binary's output that outlives the field is BYTES.
///
/// `page_entry_names::UNGATED_STORE_INDEX_AT_STAMP_8` is such bytes: a whole shard index a
/// pre-change binary really wrote, captured by that module's own capture instrument, whose three
/// page entries each spell `object_key` and `model_id` as named fields -- and whose composite keys
/// each spell the same two values. This arm takes it through `decode_index_bytes`, the one door
/// every served-index reader goes through, and asserts the identity the new reader reconstructs is
/// the identity the OLD entry carried, page by page.
///
/// # AND IT IS THE HARDEST AVAILABLE CASE, WHICH IS WHY IT IS THIS FIXTURE AND NOT A NEWER ONE
///
/// Its keys carry a NON-EMPTY component slot -- `hash:ungated/hash:f0:0:0:27:0:0` and
/// `zset:ungated/zset:6d30:0:54:34:0:0` -- from an era when a page entry named its element. A parse
/// that searched for its fields rather than counting them from the END would read the object key
/// short on exactly these keys and would still look right on every key this binary writes. Nothing
/// a current store contains would catch that.
///
/// # WHAT THIS ARM DOES NOT CLAIM
///
/// It is not a claim that such a store is SERVED. A stamp-8 image is refused by `persistence`'s `<`
/// comparison before anything reads it, which
/// `page_entry_names::an_ungated_store_is_refused_before_it_is_served` drives. This is a claim
/// about the DECODE, which happens first and which is where a reconstruction can go silently wrong.
///
/// rust-internal: decodes committed bytes through this crate's own door, no product behaviour
#[test]
fn the_new_reader_reads_the_old_formats_bytes_and_rebuilds_the_identity_it_carried() {
    let bytes = super::page_entry_names::UNGATED_STORE_INDEX_AT_STAMP_8;

    // ---- THE FIXTURE REALLY CARRIES THE TWO FIELDS, ASSERTED ON THE TEXT ----
    //
    // On the TEXT and not on the decoded value, because what this binary can still read off an
    // entry is exactly what the change took away. If the bytes did not spell them, this arm would
    // be about an index that never had them and the reconstruction would have nothing to agree
    // with.
    for spelling in [
        r#""object_key":"ungated/hash""#,
        r#""object_key":"ungated/zset""#,
        r#""model_id":"hash""#,
        r#""model_id":"zset""#,
    ] {
        assert!(
            bytes.contains(spelling),
            "[CLAUSE old-bytes-carry-the-fields] the committed fixture does not spell {spelling}, \
             so it is not an index whose entries name their object and this arm tests no \
             reconstruction"
        );
    }

    let decoded = crate::engine::decode_index_bytes(bytes.as_bytes()).expect(
        "[CLAUSE old-bytes-decode] an index written by a pre-change binary must still DECODE -- \
         the stamp refuses it one layer up, and a decode that failed here would mean the new \
         reader cannot read the old shape at all",
    );
    assert_eq!(
        8, decoded.index_format_version,
        "[CLAUSE old-bytes-decode] the fixture's stamp is {} rather than 8, so it is not the \
         pre-change store this guard is about",
        decoded.index_format_version
    );

    // ---- EVERY PAGE, AND THE IDENTITY IT CAME BACK WITH ----
    let pages = super::pages_with_identity(&decoded.bucket_index);
    let mut rebuilt: Vec<(String, String)> = pages
        .iter()
        .map(|(_, _, object_key, page)| {
            (object_key.to_string(), page.model_id.as_str().to_string())
        })
        .collect();
    rebuilt.sort();
    println!("\n=== a pre-change store, decoded and re-identified by this binary ===");
    for row in &rebuilt {
        println!("  {row:?}");
    }

    // THE EXPECTATION IS WHAT THE OLD ENTRIES SPELL, written out independently of the fixture text
    // -- three rows, one per page, which is the set `an_ungated_store_is_refused_before_it_is_served`
    // counts in the same bytes.
    let expected: Vec<(String, String)> = {
        let mut rows = vec![
            ("ungated/hash".to_string(), "hash".to_string()),
            ("ungated/hash".to_string(), "hash".to_string()),
            ("ungated/zset".to_string(), "zset".to_string()),
        ];
        rows.sort();
        rows
    };
    assert_eq!(
        expected.len(),
        rebuilt.len(),
        "[CLAUSE old-bytes-rebuild-exactly] the fixture holds {} page(s) and {} came back",
        expected.len(),
        rebuilt.len()
    );
    for (at, (want, got)) in expected.iter().zip(rebuilt.iter()).enumerate() {
        assert_eq!(
            want, got,
            "[CLAUSE old-bytes-rebuild-exactly] page {at} came back with the identity {got:?} \
             where the stored entry spelled {want:?} -- a reconstruction that disagrees with what \
             the old binary wrote is data loss wearing an optimisation's clothes"
        );
    }

    // ---- THE CONTROL: THE COMPARISON MUST BE ABLE TO FAIL ----
    let mut injected = rebuilt.clone();
    injected[0].0.push('Z');
    assert_ne!(
        injected, rebuilt,
        "[CLAUSE old-bytes-rebuild-exactly] the comparison cannot report a difference when one is \
         injected, so the equalities above are equalities it cannot distinguish from inequalities"
    );

    // ---- AND THE COMPONENT-BEARING KEY IS THE CASE THIS FIXTURE EXISTS FOR ----
    assert!(
        bytes.contains("hash:ungated/hash:f0:0:0:27:0:0"),
        "[CLAUSE old-bytes-carry-the-fields] the fixture carries no element-named page key, so the \
         split that could have read an object key short is not exercised"
    );
    let named = crate::index_log::block_ref_key_parts("hash:ungated/hash:f0:0:0:27:0:0")
        .expect("an element-named key from a real pre-change store must parse");
    assert_eq!(
        ("hash", "ungated/hash", "f0"),
        (named.kind, named.object_key, named.component),
        "[CLAUSE old-bytes-rebuild-exactly] an element-named key splits in the wrong place, so a \
         pre-change store's object keys would come back short"
    );
}

// =================================================================================================
// THE FIELD THAT CANNOT GO: THE PRESENCE BITS
// =================================================================================================

/// THE PRESENCE BITS OF AN ADDRESS ARE NOT IN THE KEY IT RENDERS, SO `address` STAYS.
///
/// # THE NON-INJECTIVITY, BY CONSTRUCTION RATHER THAN BY A CORPUS
///
/// A corpus can only say that two states did not COLLIDE in that corpus. This builds the three
/// states and renders their keys, so the collision is a property of the renderer and not of a
/// fixture:
///
///   * a LOG-RESIDENT page -- `block_id` absent, which is the shape a staged page has before a
///     dump, and which `log_backed()` is the derivation of;
///   * the FIRST page of an object in a slab, built the way every production site builds one, so
///     `block_id` is `Some(0)` and the generation is present;
///   * the same address with the generation bit CLEAR, which is the shape an index written before
///     the generation existed loads as -- the one state `try_from_wire_parts` exists to let the
///     wire state independently of the identity.
///
/// All three render ONE key. `unwrap_or_default()` on each of `block_id()` and `generation()` is
/// where the bits go, and zero is a legal value of both -- which is precisely the reason the
/// presence lives in a byte rather than in a sentinel, stated beside the field itself.
///
/// # WHY GUESSING EITHER BIT IS DATA LOSS AND NOT AN APPROXIMATION
///
/// `generation` is hashed into `block_index_handle` and rendered into the block-ref key, and those
/// handles are written to disk inside the lookup's refs -- so deriving a different value re-keys
/// every page and points the stored refs at nothing. `block_id`'s absence IS log-residency, so
/// guessing it present claims a slab for a block that is still in the log, and a read then goes
/// looking on a slab for bytes that are not there.
///
/// # AND THE REACHABILITY IS MEASURED, NOT ARGUED
///
/// The second arm asserts the two states are both ordinary: a constructor that production calls
/// produces each, and the pair that renders identically differs in `log_backed()` -- which has
/// five production readers, so the two are not interchangeable anywhere.
///
/// rust-internal: asserts a property of this crate's address codec, no product behaviour
#[test]
fn the_presence_bits_of_an_address_are_not_in_the_key_it_renders() {
    use crate::block_store::ElementEntry;

    const SLAB: u64 = 42;
    const OFFSET: u64 = 1_048_576;
    const LENGTH: u64 = 4_096;

    // THE THREE STATES, each built through the constructor the state it represents comes from.
    let log_resident = ElementEntry::from_parts(SLAB, OFFSET, LENGTH, None, None);
    let first_block_in_a_slab = ElementEntry::from_parts(SLAB, OFFSET, LENGTH, Some(0), None);
    let first_block_with_no_generation =
        ElementEntry::try_from_wire_parts(SLAB, OFFSET, LENGTH, Some(0), false)
            .expect("the wire constructor accepts a present block id with no generation");

    // VACUITY FIRST: the three must really be three.
    assert!(
        log_resident.block_id().is_none(),
        "[CLAUSE presence-is-not-in-the-key] the log-resident fixture carries a block id, so the \
         first of the three states is not represented"
    );
    assert_eq!(
        Some(0),
        first_block_in_a_slab.block_id(),
        "[CLAUSE presence-is-not-in-the-key] the slab-backed fixture is not the FIRST page, so the \
         collision this arm is about is not the one being measured"
    );
    assert_eq!(
        Some(0),
        first_block_in_a_slab.generation(),
        "[CLAUSE presence-is-not-in-the-key] the slab-backed fixture has no generation, so the \
         third state below is not distinct from it"
    );
    assert_eq!(
        None,
        first_block_with_no_generation.generation(),
        "[CLAUSE presence-is-not-in-the-key] the generation-free fixture carries one"
    );
    assert_ne!(
        log_resident, first_block_in_a_slab,
        "[CLAUSE presence-is-not-in-the-key] two of the three states compare EQUAL, so there is no \
         information for a key to lose"
    );
    assert_ne!(
        first_block_in_a_slab, first_block_with_no_generation,
        "[CLAUSE presence-is-not-in-the-key] two of the three states compare EQUAL"
    );

    // AND THE THREE RENDER ONE KEY.
    let keys: Vec<String> = [&log_resident, &first_block_in_a_slab, &first_block_with_no_generation]
        .iter()
        .map(|address| {
            let page = crate::engine::state::BlockIndex {
                model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
                address: (*address).clone(),
                dirty: false,
                deleted: false,
            };
            crate::engine::state::block_index_written_key("k", &page)
        })
        .collect();
    println!("\n=== three distinct addresses, one rendered key ===");
    for (which, key) in ["log-resident", "first page in a slab", "no generation"]
        .iter()
        .zip(keys.iter())
    {
        println!("  {which:<22} -> {key}");
    }
    assert_eq!(
        keys[0], keys[1],
        "[CLAUSE presence-is-not-in-the-key] a log-resident address and the first page of an \
         object in a slab render DIFFERENT keys, so the block-id presence bit IS recoverable from \
         the key and `address` could be reconstructed after all -- which would make this whole \
         arm, and the decision to keep the field, wrong"
    );
    assert_eq!(
        keys[1], keys[2],
        "[CLAUSE presence-is-not-in-the-key] the generation presence bit reaches the key, so it is \
         recoverable and this arm is wrong about the second bit"
    );

    // THE CONTROL: the renderer is not simply constant. An address whose block id is non-zero must
    // render a DIFFERENT key, or the three equalities above say nothing.
    let later_block = ElementEntry::from_parts(SLAB, OFFSET, LENGTH, Some(3), None);
    let later_key = crate::engine::state::block_index_written_key(
        "k",
        &crate::engine::state::BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
            address: later_block,
            dirty: false,
            deleted: false,
        },
    );
    assert_ne!(
        keys[0], later_key,
        "[CLAUSE presence-is-not-in-the-key] a block id of 3 renders the same key as an absent \
         one, so the renderer ignores the slot entirely and the equalities above are vacuous"
    );

    // AND THE TWO COLLIDING STATES ARE NOT INTERCHANGEABLE, which is what makes the collision a
    // data-loss bug rather than a representation detail.
    let as_block = |address: &ElementEntry| crate::engine::state::BlockIndex {
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
        address: address.clone(),
        dirty: false,
        deleted: false,
    };
    assert_ne!(
        as_block(&log_resident).log_backed(),
        as_block(&first_block_in_a_slab).log_backed(),
        "[CLAUSE presence-is-not-in-the-key] the two states that render one key answer the SAME \
         log-residency, so nothing downstream could tell them apart and the loss would be free"
    );
    // AND THE SECOND BIT IS SHARPER THAN THE FIRST DRAFT OF THIS ARM CLAIMED, WHICH IS WHY THE
    // MEASUREMENT IS HERE RATHER THAN THE CLAIM.
    //
    // This asserted that the generation presence bit reaches `block_index_handle`, so that losing
    // it would re-key a page. IT DOES NOT, at a block id of 0: the handle hashes
    // `generation().unwrap_or_default()`, exactly as the key renders it, so `Some(0)` and `None`
    // hash the same word -- measured here, 1416957377325452083 both ways on the first run of this
    // arm. The claim was read off the field's own doc ("the generation is hashed into
    // `block_index_handle`"), which is true of its VALUE and not of its presence at zero.
    //
    // So the bit whose loss is OBSERVABLE is the block id's, through `log_backed()`, which the
    // assertion above this one is about. The generation bit is asserted here as what it is -- at a
    // block id of 0 it reaches neither the key nor the handle -- and the control below is what says
    // it is nonetheless load-bearing where it CAN be seen, so this is not an argument for dropping
    // it either.
    let handle_with_generation =
        crate::engine::state::block_index_handle("k", &as_block(&first_block_in_a_slab));
    let handle_without =
        crate::engine::state::block_index_handle("k", &as_block(&first_block_with_no_generation));
    println!(
        "  generation bit at block id 0: handle {handle_with_generation} with it, \
         {handle_without} without -- it reaches neither the key nor the handle THERE"
    );
    assert_eq!(
        handle_with_generation, handle_without,
        "[CLAUSE presence-is-not-in-the-key] the generation presence bit DOES reach the handle at a \
         block id of 0, so the note above is wrong and the bit is recoverable from the refs after \
         all"
    );
    // THE CONTROL ON THAT: the generation's VALUE does reach both, so the bit is not dead weight
    // and the arm above is not an argument for retiring it.
    let later_with = ElementEntry::from_parts(SLAB, OFFSET, LENGTH, Some(3), None);
    let later_without = ElementEntry::try_from_wire_parts(SLAB, OFFSET, LENGTH, Some(3), false)
        .expect("the wire constructor accepts a present block id with no generation");
    assert_ne!(
        crate::engine::state::block_index_written_key("k", &as_block(&later_with)),
        crate::engine::state::block_index_written_key("k", &as_block(&later_without)),
        "[CLAUSE presence-is-not-in-the-key] the generation presence bit reaches no key at ANY \
         block id, which would make it unobservable everywhere -- it reaches the key at a non-zero \
         block id, which is what makes it a stored fact rather than a leftover"
    );
    assert_ne!(
        crate::engine::state::block_index_handle("k", &as_block(&later_with)),
        crate::engine::state::block_index_handle("k", &as_block(&later_without)),
        "[CLAUSE presence-is-not-in-the-key] the generation presence bit reaches no handle at any \
         block id"
    );
}

// =================================================================================================
// THE PRICE ON THE SHIPPED ARTEFACT
// =================================================================================================

/// WHAT THE ENTRY'S RESTATEMENT COSTS THE SHIPPED CONTAINER, AT THE SHIPPED ROUTING RANGE.
///
/// # NOT A COUNTERFACTUAL
///
/// #2350's table was a byte-weight counterfactual: a doctored `serde_json::Value` re-encoded. This
/// is the production encoder on the production path, before and after, at the same corpus and the
/// same range -- so the only thing the two readings differ in is the change. The BEFORE figures are
/// pinned below as constants captured from this same module run on `ea5b278a9`, which is what makes
/// the delta a measurement rather than a memory.
///
/// # WHICH SURFACE EVERY NUMBER IS ON
///
/// Four are printed and they are not interchangeable: the JSON stream, the msgpack payload, the
/// SHIPPED container (codec 2), and the codec-1 container that #2350's prediction was taken on.
/// The prediction is checked against the codec-1 arm because that is the arm it was made on.
///
/// Two key shapes, because a padded corpus is a lower bound on every compressed figure and a
/// scattered one an upper bound, and #2350 took every figure on both for exactly that reason.
///
/// rust-internal: measures this crate's own index codec, no product behaviour
#[test]
fn what_the_entry_restating_its_key_costs_the_shipped_container() {
    println!(
        "\n################ THE ENTRY'S RESTATEMENT, PRICED ON THE SHIPPED ARTEFACT ################"
    );
    println!(
        "  BEFORE is this module run on `ea5b278a9` with the production change absent and nothing \
         else different -- same corpus, same range, same fixture code. The deltas below are a \
         measurement of the change and not of a doctoring instrument."
    );
    println!(
        "  {:<10} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "shape", "surface", "before", "after", "saved", "share"
    );
    let mut checked = 0usize;
    for (shape, before) in [
        (KeyShape::Padded, BEFORE_PADDED),
        (KeyShape::Scattered, BEFORE_SCATTERED),
    ] {
        let seeded = seed(HEADLINE_KEYS, PUBLISHED_WIDTH, shape, SHIPPED_END_BUCKET);
        let surfaces = seeded.surfaces();
        surfaces.report("shipped range 0..1023");
        let n = HEADLINE_KEYS as f64;
        for (label, was, now) in [
            ("stream", before.stream, surfaces.stream),
            ("slot_index", before.slot_index, surfaces.slot_index_stream),
            ("payload", before.payload, surfaces.payload),
            ("container c2", before.container, surfaces.container),
            ("container c1", before.json_container, surfaces.json_container),
        ] {
            println!(
                "  {:<10} {label:>12} {:>12.3} {:>12.3} {:>12.3} {:>11.2}%",
                shape.label(),
                was as f64 / n,
                now as f64 / n,
                (was - now) as f64 / n,
                100.0 * (was - now) as f64 / was as f64
            );
            assert!(
                now < was,
                "[CLAUSE the-entry-side-pays-on-the-artefact] on {} keys the {label} surface is \
                 {now} B after against {was} B before, so this change did not make it smaller -- a \
                 flip on any of the five surfaces is the finding changing rather than the fixture",
                shape.label()
            );
            checked += 1;
        }
        // ---- THE STREAM SAVING IS EXACTLY THE 50.0 B/OBJECT #2350 PREDICTED ----
        //
        // 30.0 for `entry/object_key` and 20.0 for `entry/model_id`, each the quoted field name,
        // its colon, its value and its separator. An EXACT clause, not a tolerance: the stream is
        // the one surface that partitions, these are whole fields, and nothing rounds.
        let stream_saved = (before.stream - surfaces.stream) as f64 / n;
        assert!(
            (stream_saved - PREDICTED_STREAM_SAVING).abs() < 1e-9,
            "[CLAUSE the-prediction-closes] the stream saving on {} keys is {stream_saved:.6} \
             B/object against the {PREDICTED_STREAM_SAVING:.1} #2350 predicted for these two \
             fields; a difference here is bytes leaving that nobody named, or a field this change \
             did not mean to touch",
            shape.label()
        );
        // ---- AND THE CONTAINER SAVING AGAINST THE PREDICTED BAND, WHICH IT UNDERSHOOTS ----
        //
        // #2350 predicted +2.34 to +2.96 for `object_key` and +0.26 to +0.37 for `model_id` on the
        // codec-1 container, which is +2.60 to +3.33 for the pair. Measured: 2.418 padded and
        // 3.177 scattered -- UNDER the band at both ends, by 0.18 and 0.15 B/object.
        //
        // THE DIRECTION OF THE MISS IS WORTH MORE THAN THE SIZE OF IT. The counterfactual was a
        // delta between two `serde_json::Value` re-encodes, and a `Value` writes its keys SORTED
        // while the struct writes declaration order -- #2350 measured that reordering alone moving
        // a compressed size 5%, and named its container arms a proxy good to within 15%. A
        // counterfactual on a re-ordered document therefore prices a removal 5-7% optimistically,
        // and that is the whole of this gap. The clause is a BAND and not an equality for exactly
        // that reason, and it is asserted on both ends.
        let container_saved = (before.json_container - surfaces.json_container) as f64 / n;
        assert!(
            (2.0..=3.33).contains(&container_saved),
            "[CLAUSE the-prediction-closes] the codec-1 container saving on {} keys is \
             {container_saved:.3} B/object, outside the 2.0 to 3.33 this change was costed \
             against (#2350's +2.60 to +3.33 for the pair, less the 5-7% a `Value`-rebuilt \
             counterfactual overstates by)",
            shape.label()
        );
        drop(seeded);
    }
    assert_eq!(
        10, checked,
        "[CLAUSE the-entry-side-pays-on-the-artefact] {checked} surface(s) were compared rather \
         than five on each of two key shapes"
    );
    println!(
        "\n  THE SHIPPED CODEC IS THE SMALLER SAVING, AND IT IS THE ONE THAT MATTERS. Codec 2 is \
         what production writes; #2350's prediction was taken on codec 1 because that is the arm a \
         counterfactual can re-encode byte-for-byte. The two disagree by about 0.5 B/object here, \
         so a figure quoted on codec 1 is optimistic about the artefact by that much."
    );
}

/// The five surfaces of one key shape, as a single reading.
struct Before {
    stream: usize,
    slot_index: usize,
    payload: usize,
    container: usize,
    json_container: usize,
}

/// WHAT THIS CORPUS WEIGHED BEFORE THE CHANGE, AND WHERE THE NUMBERS CAME FROM.
///
/// Captured by running THIS MODULE on `ea5b278a9` with the production change absent -- the
/// measurement module committed first and the entry's two fields still written -- so the before and
/// the after differ in the change and in nothing else. Not a doctored `serde_json::Value`: the
/// production encoder on the production path, at 8,000 objects in 1,024 buckets on `0..1023`, with
/// the amortised regime asserted on both readings.
///
/// A CONSTANT AND NOT A SECOND FIXTURE RUN, deliberately. The alternative is to build the old shape
/// here, and the old shape is what this change deletes: a fixture that could still produce it would
/// be a second encoder to keep agreeing with the first, which is the hazard this file's own
/// `serialize_page_index` keeps a record of.
const BEFORE_PADDED: Before = Before {
    stream: 2_283_431,
    slot_index: 1_810_793,
    payload: 1_587_811,
    container: 250_657,
    json_container: 258_432,
};

const BEFORE_SCATTERED: Before = Before {
    stream: 2_282_145,
    slot_index: 1_809_507,
    payload: 1_587_853,
    container: 328_520,
    json_container: 336_433,
};

/// The stream saving #2350 priced for these two fields: 30.0 for `object_key`, 20.0 for `model_id`.
const PREDICTED_STREAM_SAVING: f64 = 50.0;

// =================================================================================================
// CORRECTNESS ACROSS A RELOAD
// =================================================================================================

/// EVERY OBJECT IS STILL FINDABLE AND READS BACK BYTE-IDENTICAL, ACROSS A DROP AND A RELOAD.
///
/// # THE DIRECTION NOTHING REPORTS
///
/// A page the load path cannot find does not fail: the block is still on its slab and nothing looks
/// for it. So the assertion is a COUNT equal to the corpus with zero missing and zero wrong, taken
/// after the engine is dropped and a second one is pointed at the same directories -- not a spot
/// check and not a non-emptiness.
///
/// # WITH A NEGATIVE CONTROL IN THE SAME MOMENT
///
/// A key never written must read ABSENT on the same build at the same instant, or a reader that
/// answered every lookup would pass every assertion above.
///
/// # AND THE RELOAD IS ASSERTED TO HAVE READ THE INDEX, WHICH A PLANT IS WHY
///
/// This arm was planted on: the reconstruction was made to answer a CONSTANT kind for every page,
/// and this test PASSED. Two things were wrong with it and both are fixed here rather than noted.
///
///   1. IT NEVER ASSERTED WHICH PATH THE RELOAD TOOK. "The base index file is non-empty on disk" is
///      not "the base index was read": `load_index_inner` answers `Ok(None)` for a stale stamp, an
///      undecodable image and an absent file alike, and the caller then replays the log -- slower,
///      correct, and reading not one stored page entry. `index_load_path_counts` is asserted
///      instead, as the four-tuple it is, with `accepted` non-zero and `refused_stale` and
///      `undecodable` at zero over the same reset window.
///   2. A STRING READ DOES NOT GO THROUGH THE PAGE INDEX'S KIND. A `StringGet` resolves through
///      the string row in `objects`, so 600 correct reads said nothing about the `model_id` the
///      bucket's page entries came back with. The identities are therefore compared DIRECTLY,
///      page by page, against what the pre-unload engine held -- which is the comparison the plant
///      walked straight through.
///
/// rust-internal: drives this crate's own reload path, no product behaviour
#[test]
fn a_reload_reads_every_object_back_and_a_key_never_written_is_absent() {
    use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};
    const OBJECTS: usize = 600;
    const WIDTH: usize = PUBLISHED_WIDTH;

    let dir = tempfile::tempdir().expect("tempdir");
    let cache = dir.path().join("cache");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");

    let index_path = indexes.join(format!("shard-{SHARD}.index.json"));
    let mut expected: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    // THE IDENTITY OF EVERY PAGE AS THE WRITER HELD IT, so the reload can be compared against it
    // rather than against itself. Keyed by the handle, which is what the map files a page under in
    // both processes.
    let mut identities_before: BTreeMap<u64, (String, String)> = BTreeMap::new();
    {
        let engine =
            TemporalEngine::with_local_dirs(64, cache.clone(), pages.clone(), indexes.clone());
        let response = engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: SHARD,
            table_name: "the-key-a-page-is-filed-under".to_string(),
            shard_uri: "local://the-key-a-page-is-filed-under/41".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket: SHIPPED_END_BUCKET,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        });
        assert!(response.status.ok, "[CLAUSE reload-reads-everything] the seed shard must load");
        for ordinal in 0..OBJECTS {
            let key = key_text(ordinal, WIDTH, KeyShape::Scattered);
            // A DISTINCT VALUE PER KEY, so a reader that served one object's bytes for another's
            // lookup is a FAILURE here rather than a pass.
            let value: Vec<u8> = format!("value-for-{key}-{ordinal}").into_bytes();
            let response = engine.execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet { key: key.clone(), value: value.clone() },
            });
            assert!(response.status.ok, "the seed must ack at {ordinal}: {response:?}");
            expected.insert(key, value);
        }
        {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&SHARD).expect("shard is loaded");
            for (_, handle, object_key, page) in super::pages_with_identity(&shard.bucket_index) {
                identities_before.insert(
                    handle,
                    (object_key.to_string(), page.model_id.as_str().to_string()),
                );
            }
        }
        assert_eq!(
            OBJECTS,
            identities_before.len(),
            "[CLAUSE reload-reads-everything] the writer held {} page identity(ies) for {OBJECTS} \
             object(s), so the comparison after the reload is not over the corpus",
            identities_before.len()
        );

        // UNLOADED RATHER THAN DROPPED, so the base index is MATERIALIZED on disk: a dropped engine
        // leaves whatever the last dump wrote, and a reload that replayed the log instead would
        // exercise the WAL path and say nothing about the stored page entries.
        engine.unload_shard(SHARD);
        let written = std::fs::metadata(&index_path)
            .map(|meta| meta.len())
            .unwrap_or_default();
        assert!(
            written > 0,
            "[CLAUSE reload-reads-everything] the unload wrote no base index at {index_path:?}, so \
             the reload below would replay the log and this arm would not read a stored page entry \
             at all"
        );
        println!("[reload-reads-everything] the unload wrote a {written}-byte base index");
    }

    // --- THE RELOAD: a second engine on the same directories. ---
    reset_index_load_path_counts();
    let engine = TemporalEngine::with_local_dirs(64, cache, pages, indexes);
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        table_name: "the-key-a-page-is-filed-under".to_string(),
        shard_uri: "local://the-key-a-page-is-filed-under/41".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "[CLAUSE reload-reads-everything] the reloaded shard must load: {:?}",
        response.status
    );

    // --- THE DECODE ITSELF, BEFORE ANY REBUILD PASS TOUCHES IT ---
    //
    // THIS ARM EXISTS BECAUSE A PLANT WALKED THROUGH THE ONE BELOW IT. The reconstruction was made
    // to answer a CONSTANT kind for every page and the whole reload arm -- 600 reads, the accepted
    // counter, and a page-by-page identity comparison on the LIVE index after the load -- passed.
    // So the live index after a load is not where the reconstruction's answer survives: the load
    // path has rebuild passes over the bucket index, and for a string store the authoritative map
    // is `objects`, so a wrong `model_id` on a decoded page entry is overwritten before anything
    // can see it.
    //
    // THAT IS A FACT ABOUT THE GUARD'S REACH AND NOT A REASON TO WEAKEN THE CLAIM. The decode is
    // where the reconstruction's answer IS observable, so it is read here: the bytes the unload
    // wrote, through `decode_index_bytes`, with every page's rebuilt identity compared against what
    // the writer held -- which is what the plant moves and what this arm fails on.
    {
        let stored = std::fs::read(&index_path)
            .expect("the unload wrote a base index and this arm just asserted it non-empty");
        let decoded = crate::engine::decode_index_bytes(&stored).expect(
            "[CLAUSE the-decode-rebuilds-every-identity] the bytes this binary wrote must decode \
             through the same door every reader goes through",
        );
        let mut at_the_decode: BTreeMap<u64, (String, String)> = BTreeMap::new();
        for (_, handle, object_key, page) in super::pages_with_identity(&decoded.bucket_index) {
            at_the_decode.insert(
                handle,
                (object_key.to_string(), page.model_id.as_str().to_string()),
            );
        }
        println!(
            "[the-decode-rebuilds-every-identity] {} page identity(ies) came straight out of the \
             decode",
            at_the_decode.len()
        );
        assert_eq!(
            identities_before, at_the_decode,
            "[CLAUSE the-decode-rebuilds-every-identity] a page came out of the DECODE under a \
             different object key or kind than the writer held. The identity is read back out of \
             the stored key now, so this is the reconstruction being wrong -- and it is read here \
             rather than off the live index after a load, because the load's rebuild passes \
             overwrite it and a plant proved they do"
        );
    }

    // --- THE PROBE ENTERED ITS REGIME: THE INDEX WAS READ, NOT REPLAYED AROUND. ---
    //
    // BOUND BY POSITION AGAINST THE SIGNATURE, which is (accepted, refused_stale, absent,
    // undecodable). A tuple and not a sum, because a sum cannot tell a store that read its index
    // from one that replayed -- and replaying reads not one stored page entry, so every assertion
    // below it would be about the write-ahead log instead.
    let (accepted, refused_stale, _absent, undecodable) = index_load_path_counts();
    println!(
        "[reload-reads-everything] load paths after the reload: accepted={accepted} \
         refused_stale={refused_stale} undecodable={undecodable}"
    );
    assert!(
        accepted > 0,
        "[CLAUSE the-index-was-read] the reload accepted no base index (accepted={accepted}, \
         refused_stale={refused_stale}, undecodable={undecodable}), so it rebuilt from the log and \
         read no stored page entry -- every assertion below would be about the write-ahead log"
    );
    assert_eq!(
        (0, 0),
        (refused_stale, undecodable),
        "[CLAUSE the-index-was-read] the reload refused {refused_stale} image(s) as stale and \
         found {undecodable} undecodable; an accepted load beside a refused one would make the \
         clause above pass over a store that also replayed"
    );

    // --- AND EVERY PAGE CAME BACK WITH THE IDENTITY THE WRITER HELD. ---
    //
    // THE COMPARISON A PLANT WALKED THROUGH WITHOUT THIS. A `StringGet` resolves through the string
    // row and not through the page index's kind, so the 600 reads below are silent about the
    // `model_id` the reconstruction produces. This is the arm that is not.
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("shard is loaded");
        let mut after: BTreeMap<u64, (String, String)> = BTreeMap::new();
        for (_, handle, object_key, page) in super::pages_with_identity(&shard.bucket_index) {
            after.insert(
                handle,
                (object_key.to_string(), page.model_id.as_str().to_string()),
            );
        }
        assert_eq!(
            identities_before.len(),
            after.len(),
            "[CLAUSE reload-rebuilds-every-identity] {} page identity(ies) were written and {} came \
             back",
            identities_before.len(),
            after.len()
        );
        assert_eq!(
            identities_before, after,
            "[CLAUSE reload-rebuilds-every-identity] a page came back under a different object key \
             or a different kind than the writer held -- the identity is reconstructed from the \
             stored key now, so a disagreement here is the reconstruction being wrong"
        );
        // THE CONTROL: the comparison must be able to fail.
        let mut injected = after.clone();
        if let Some((_, row)) = injected.iter_mut().next() {
            row.1.push('Z');
        }
        assert_ne!(
            injected, identities_before,
            "[CLAUSE reload-rebuilds-every-identity] the comparison cannot report a difference when \
             one is injected"
        );
    }

    let mut found = 0usize;
    let mut missing: Vec<String> = Vec::new();
    let mut wrong: Vec<String> = Vec::new();
    for (key, want) in &expected {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: key.clone() },
        });
        match response.response {
            crate::types::CommandResponse::Bytes { value: Some(ref got) } if got == want => {
                found += 1
            }
            crate::types::CommandResponse::Bytes { value: Some(_) } => wrong.push(key.clone()),
            crate::types::CommandResponse::Bytes { value: None } => missing.push(key.clone()),
            ref other => panic!(
                "[CLAUSE reload-reads-everything] a string read answered {other:?} rather than \
                 bytes, so this walk is not reading objects back at all"
            ),
        }
    }
    println!(
        "[reload-reads-everything] {found} of {} object(s) read back byte-identical after a drop \
         and a reload; {} missing, {} wrong",
        expected.len(),
        missing.len(),
        wrong.len()
    );
    assert!(
        missing.is_empty(),
        "[CLAUSE reload-reads-everything] {} object(s) were not found after the reload, first \
         {:?} -- a page filed under a reconstructed identity that is wrong is exactly this",
        missing.len(),
        missing.iter().take(5).collect::<Vec<_>>()
    );
    assert!(
        wrong.is_empty(),
        "[CLAUSE reload-reads-everything] {} object(s) read back DIFFERENT bytes, first {:?}",
        wrong.len(),
        wrong.iter().take(5).collect::<Vec<_>>()
    );
    assert_eq!(
        expected.len(),
        found,
        "[CLAUSE reload-reads-everything] {found} of {} read back",
        expected.len()
    );
    assert_eq!(
        OBJECTS, found,
        "[CLAUSE reload-reads-everything] the corpus was {OBJECTS} and {found} were checked"
    );

    // --- THE NEGATIVE CONTROL, SAME BUILD, SAME MOMENT. ---
    let never_written = key_text(OBJECTS + 50_000, WIDTH, KeyShape::Scattered);
    assert!(
        !expected.contains_key(&never_written),
        "[CLAUSE reload-reads-everything] the control key was written by the seed, so it cannot \
         say what an absent key does"
    );
    let control = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet { key: never_written.clone() },
    });
    assert!(
        matches!(
            control.response,
            crate::types::CommandResponse::Bytes { value: None }
        ),
        "[CLAUSE reload-reads-everything] the key {never_written:?} was never written and the \
         store answered with bytes, so the {found} reads above are not evidence that anything was \
         found correctly"
    );
    println!("[reload-reads-everything] a key never written reads absent on the same build");
}
