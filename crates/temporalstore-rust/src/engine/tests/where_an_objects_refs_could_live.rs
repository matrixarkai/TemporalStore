// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE AN OBJECT'S BLOCK REFS COULD LIVE IF `object_block_lookup` WERE FOLDED AWAY.
//!
//! `index_bytes_per_key::what_folding_the_block_refs_into_the_slot_would_save` prices the
//! one-structure shape at **50.9 B/key at 1,792 keys and 50.9 at 7,168** -- a genuine per-key
//! saving and the largest single lever measured on the resident index. It says in as many words
//! what it does not claim: "Not that the fold is implementable as it stands."
//!
//! THIS MEASURES THE PART THAT DECIDES THAT, over the real write path rather than over two maps
//! built in a test. The fold needs somewhere to put each object's refs, and the holder the price
//! was taken against is the consolidated object map -- `ShardState::objects`, keyed kind then
//! object key, valued `ObjectSlot`. So the question is whether every object the lookup files refs
//! for HAS a row in that map, and the answer is per KIND.
//!
//! WHAT THE ANSWER IS USED FOR. A kind that files refs for an object with no row has nowhere to
//! fold them into. Giving it one means either a new row that the wire would then carry -- the
//! consolidated map is durable, and its iteration feeds the dump manifest digest -- or a second
//! home beside the slot, which is the two-sources-of-truth shape `ObjectBlockLookup`'s own doc
//! refuses for the integer-addressed variant.
//!
//! THE CONTROL IS THE OTHER HALF OF THE SAME TABLE. A probe that reported "no row" for every kind
//! would be indistinguishable from one that cannot see a row at all, so the kinds that DO have a
//! row are asserted as well, and the test fails if neither column is populated.

use super::*;

/// WHERE EACH KIND'S REFS ACTUALLY END UP: THE SLOT, OR THE LOOKUP.
///
/// # WHAT THIS ARM USED TO ASSERT, AND WHY IT INVERTED
///
/// It was a tripwire on a BLOCKED change: it asserted that some live kind files refs for an object
/// the consolidated map holds no row for, and said to delete itself the day that stopped being
/// true. The fold shipped instead, with those kinds served by the lookup -- so the useful question
/// is no longer "is anything unhoused" but "did the housed kinds actually make it into their
/// slots". A single-block object whose refs stayed in the lookup is the fold's silent failure mode,
/// because both homes read correctly and only the footprint differs.
///
/// # THE TWO CONTROLS ARE THE POINT
///
/// A reading with an empty SLOT column would mean the fold saved nothing. A reading with an empty
/// LOOKUP column would mean the lookup is dead code and the four unhoused kinds were not exercised
/// -- which would make the first column look better than it is. Both are asserted, so neither can
/// go quietly empty.
///
/// rust-internal: measures this crate's own structures, no product behaviour
#[test]
fn where_each_kinds_refs_end_up() {
    use crate::engine::storage_bucket_internals::StoredModelKind;
    use crate::types::{Command, ExecuteRequest};

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = std::sync::Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "where-refs-end-up".to_string(),
        shard_uri: "local://where-refs-end-up/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(loaded.status.ok, "load: {:?}", loaded.status);

    for i in 0..4 {
        for command in [
            Command::StringSet { key: format!("s{i}"), value: vec![b'v'; 64] },
            Command::HashSet { key: format!("h{i}"), field: "f".to_string(), value: vec![b'v'; 8] },
            Command::SetAdd { key: format!("t{i}"), member: b"m".to_vec() },
            Command::ZSetAdd { key: format!("z{i}"), member: b"m".to_vec(), score: 1.0 },
            Command::ListPush { key: format!("l{i}"), member: b"m".to_vec(), left: false },
            Command::FeatureAppend {
                key: format!("f{i}"),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_000,
                    value: b"p".to_vec(),
                }],
            },
            Command::ControlStateIncrement {
                key: format!("c{i}"),
                timestamp_ms: 1_000,
                amount: 1,
            },
        ] {
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok, "seed must ack: {:?}", response.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    let mut in_slot: std::collections::BTreeMap<&'static str, usize> =
        std::collections::BTreeMap::new();
    for (kind, table) in &shard.bucket_index.objects {
        for slot in table.values() {
            if let Some(refs) = slot.refs() {
                *in_slot.entry(kind.as_str()).or_insert(0) += refs.len();
            }
        }
    }
    let mut in_lookup: std::collections::BTreeMap<&'static str, usize> =
        std::collections::BTreeMap::new();
    for (model_id, _object_key, refs) in shard.bucket_index.object_block_lookup.iter() {
        let kind = StoredModelKind::from_stored_name(model_id)
            .unwrap_or_else(|| panic!("the lookup holds an undeclared spelling: {model_id:?}"));
        *in_lookup.entry(kind.as_str()).or_insert(0) += refs.len();
    }

    let mut kinds: Vec<&'static str> = in_slot.keys().chain(in_lookup.keys()).copied().collect();
    kinds.sort_unstable();
    kinds.dedup();
    println!();
    println!("  {:<22} {:>12} {:>12}", "KIND", "in the slot", "in the lookup");
    for kind in &kinds {
        println!(
            "  {kind:<22} {:>12} {:>12}",
            in_slot.get(kind).copied().unwrap_or(0),
            in_lookup.get(kind).copied().unwrap_or(0)
        );
    }
    println!();
    println!("  refs served from a slot:   {:?}", in_slot.keys().collect::<Vec<_>>());
    println!("  refs served from the lookup: {:?}", in_lookup.keys().collect::<Vec<_>>());
    println!();

    assert!(
        !in_slot.is_empty(),
        "NOT ONE ref reached a slot, so the fold saved nothing on this corpus. Both homes read \
         correctly, so nothing else in the suite would notice -- check that \
         `CoreIndex::adopt_refs_into_slot` is still called by the door that installs a row."
    );
    assert!(
        !in_lookup.is_empty(),
        "no ref is in the lookup, which would make the slot column above look better than it is: \
         the four kinds whose rows live in `RecordedMap` have nowhere to fold into and MUST still \
         be served from the lookup. An empty lookup here means those kinds were not exercised."
    );
    // AND THE SINGLE-BLOCK STRING CASE SPECIFICALLY, because it is the one the published figure is
    // measured on and the one the adoption exists for. Each `s{i}` above is written ONCE.
    assert_eq!(
        0,
        in_lookup.get("string").copied().unwrap_or(0),
        "a string's refs are still in the lookup after a single write. That is exactly the case \
         `adopt_refs_into_slot` exists for -- the block is filed before the row is installed -- so \
         the fold would measure no saving on the corpus the published asymptote uses"
    );
    assert!(
        in_slot.get("string").copied().unwrap_or(0) >= 4,
        "each of the four string keys should name at least one page from its slot, got {:?}",
        in_slot.get("string")
    );
}

/// THE ENCODER'S IDENTITY SOURCE IS A FIELD OF `CoreIndex`, AND IT REFUSES WITHOUT ONE.
///
/// # WHY THIS IS THE OTHER PRECONDITION ON THE FOLD
///
/// `impl Serialize for CoreIndex` composes every live block's written key from
/// [`BlockKeysByPosition::from_lookup`] over `self.object_block_lookup`, because the block entry
/// carries no identity of its own. Fold the refs into the consolidated object map and that field
/// is gone -- and the consolidated map is `ShardState::objects`, a SIBLING of `bucket_index` on a
/// struct that derives `Serialize`. `BucketNodeAt`'s note says why no derive and no
/// `serialize_with` can hand it down: both see one field at a time.
///
/// SO THE CONSEQUENCE IS NOT A BAD INDEX, IT IS NO INDEX. `serialize_block_index` returns `Err`
/// for a block with nothing beside it, naming which of the two states it is in. This drives that
/// refusal, which nothing in the suite did -- the state was reached only incidentally, by a
/// fixture that forgot to file, and the message is quoted in `block_entry_names` as a thing that
/// once happened rather than as a thing that is checked.
///
/// THE CONTROL IS THE SAME INDEX WITH THE FILING DONE. Without it an `Err` from anywhere at all
/// would pass, and the test would assert that a `CoreIndex` does not serialize rather than that an
/// UNFILED one does not.
///
/// rust-internal: measures this crate's own encoder, no product behaviour
#[test]
fn the_index_encoder_refuses_a_page_whose_refs_are_not_in_its_own_field() {
    use crate::engine::state::{
        block_index_handle, BlockIndex, BlockSlabLiveIndex, BucketNode, CoreIndex,
    };
    use crate::engine::storage_bucket_internals::StoredModelKind;
    use std::sync::Arc;

    let object_key: Arc<str> = Arc::from("an-object");
    let entry = BlockIndex {
        model_id: StoredModelKind::String,
        address: Default::default(),
        dirty: false,
        deleted: false,
    };

    let placed = || -> CoreIndex {
        let mut node = BucketNode::default();
        node.routing_bucket = 3;
        node.set_meta_loaded(true);
        let mut live = BlockSlabLiveIndex::default();
        node.insert_block(&object_key, entry.clone(), &mut live);
        let mut index = CoreIndex::default();
        index.bucket_map.insert(3, node);
        index
    };

    // --- THE STATE A FOLD OUT OF `CoreIndex` LEAVES THE ENCODER IN: the block is placed and this
    // --- index holds nothing that names its object.
    let unfiled = placed();
    let refused = serde_json::to_string(&unfiled);
    let message = refused
        .expect_err(
            "an index holding a placed page that nothing in it names must REFUSE to encode. If \
             this now succeeds, the encoder found an identity somewhere else -- which is the \
             change the fold needs, and this test's premise with it.",
        )
        .to_string();
    println!("the encoder refused with: {message}");
    assert!(
        message.contains("would name no object"),
        "the refusal must say what is wrong with the index, not merely fail: {message}"
    );
    assert!(
        message.contains("names NO position at all"),
        "the refusal must say WHICH of the two states it is in -- an unestablished inversion, or \
         one that does not cover this bucket. The two have different causes and different fixes: \
         {message}"
    );

    // --- THE CONTROL. The same placed block, with the filing a production placement does, encodes.
    // --- Without this the assertion above would hold for an index that cannot encode at all.
    let mut filed = placed();
    let handle = block_index_handle(&object_key, &entry);
    filed.insert_object_block_lookup(3, handle, &object_key, &entry);
    let encoded = serde_json::to_string(&filed)
        .expect("the same index encodes once its own field names the page's object");
    println!("and encoded, once filed, as: {encoded}");
    assert!(
        encoded.contains("an-object"),
        "the written key is composed from the filing, so the object's characters must be in the \
         encode -- that is the field the fold has to keep composable: {encoded}"
    );
}

/// DOES A SLOT ROW EXIST AT THE MOMENT ITS BLOCK IS FILED? IT DOES NOT, FOR A FIRST WRITE.
///
/// This chose the fold's routing rule, and it is KEPT rather than deleted with the question
/// because the answer is the whole reason `CoreIndex::adopt_refs_into_slot` exists. A first
/// `StringSet` files its block and only then installs the address -- the address IS where the block
/// landed -- so a rule that required the row would fire on every first write, and a rule that
/// merely preferred the row would leave every single-block object in the lookup and measure no
/// saving at all.
///
/// So this is the evidence for a door that would otherwise look redundant, and it reddens if
/// someone reorders the write path such that the row comes first -- at which point the adoption
/// becomes dead code and should be removed deliberately rather than left to rot.
#[test]
fn whether_a_slot_row_exists_when_its_page_is_filed() {
    use crate::engine::state::FILING_ROW_CENSUS;
    use crate::types::{Command, ExecuteRequest};

    *FILING_ROW_CENSUS.lock().expect("census lock") = Some(std::collections::BTreeMap::new());

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = std::sync::Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "filing-order".to_string(),
        shard_uri: "local://filing-order/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(loaded.status.ok, "load: {:?}", loaded.status);

    // Several objects per kind, and each string written TWICE -- a first write and an overwrite
    // are different cases for this question and only the first can find no row.
    for i in 0..8 {
        for command in [
            Command::StringSet { key: format!("s{i}"), value: vec![b'v'; 64] },
            Command::StringSet { key: format!("s{i}"), value: vec![b'w'; 64] },
            Command::HashSet { key: format!("h{i}"), field: "f".to_string(), value: vec![b'v'; 8] },
            Command::SetAdd { key: format!("t{i}"), member: b"m".to_vec() },
            Command::ZSetAdd { key: format!("z{i}"), member: b"m".to_vec(), score: 1.0 },
            Command::ListPush { key: format!("l{i}"), member: b"m".to_vec(), left: false },
            Command::FeatureAppend {
                key: format!("f{i}"),
                points: vec![crate::types::FeaturePoint { timestamp_ms: 1_000, value: b"p".to_vec() }],
            },
            Command::ControlStateIncrement {
                key: format!("c{i}"),
                timestamp_ms: 1_000,
                amount: 1,
            },
        ] {
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok, "seed must ack: {:?}", response.status);
        }
    }

    let census = FILING_ROW_CENSUS
        .lock()
        .expect("census lock")
        .take()
        .expect("the census was enabled at the top of this test");

    println!();
    println!("  {:<22} {:>12} {:>12}", "KIND", "row present", "row ABSENT");
    let mut absent_kinds: Vec<&str> = Vec::new();
    let mut present_kinds: Vec<&str> = Vec::new();
    for (kind, (present, absent)) in &census {
        println!("  {kind:<22} {present:>12} {absent:>12}");
        if *absent > 0 {
            absent_kinds.push(kind);
        }
        if *present > 0 {
            present_kinds.push(kind);
        }
    }
    println!();
    println!("  filings that found a row:    {present_kinds:?}");
    println!("  filings that found NO row:   {absent_kinds:?}");
    println!();

    assert!(
        !census.is_empty(),
        "no page was filed at all, so this says nothing about ordering"
    );
}

/// WHETHER A NICHE CAN TAKE THE REFS UNION TO SIXTEEN, AND WHAT THE `Option` DOES TO THAT.
///
/// `BlockRefs` is 24 B and `what_the_folded_slot_weighs` pins the slot at
/// `16 + 8 + size_of::<Option<BlockRefs>>()`, so eight bytes off the union is eight off every
/// object slot -- the `strings` row is the largest on the resident index. Boxing the wide arm
/// alone measures ZERO, because the union is sized by the INLINE arm: `BlockLookupRef` is a
/// `u32` beside a `u64`, twelve bytes of field in sixteen, every bit pattern valid. **Padding
/// is not a niche** -- rustc puts a discriminant in forbidden VALUES, not in spare bytes.
///
/// SO THE ONLY ROUTE TO SIXTEEN IS A FORBIDDEN VALUE IN THE INLINE ARM, and the obvious
/// candidate is `block_ref_key: NonZeroU64`. THE ARITHMETIC THAT DECIDES IT, which is why this
/// arm exists rather than a paragraph: a two-arm union needs ONE spare value for its own tag,
/// and the shipped field is an `Option`, which needs a SECOND. `NonZeroU64` forbids exactly
/// one value. If that is the whole budget then `Option<union>` spills back to 24 and the slot
/// never moves, so the saving at the home that matters is zero even though the union itself
/// shrank.
///
/// THE THREE-ARM SHAPE IS THE ESCAPE THAT LOOKS FREE: fold the `Option`'s `None` into the
/// union so only one tag is needed. That still wants two spare values for three arms, so it is
/// measured rather than assumed.
///
/// THE CONTROL is the same two shapes with a PLAIN `u64`. Without it, a reading of 24
/// everywhere cannot tell "the niche bought nothing" from "this probe cannot see a niche".
///
/// rust-internal: measures this crate's own layout, no product behaviour
///
///   cargo test -p temporalstore-rust --lib whether_a_niche_can_take_the_refs_union_to_sixteen -- --exact --nocapture
#[test]
fn whether_a_niche_can_take_the_refs_union_to_sixteen() {
    use crate::engine::state::{BlockLookupRef, BlockRefs};
    use std::mem::size_of;

    // --- the candidate: a forbidden value in the inline arm ---
    #[allow(dead_code)]
    struct NicheRef {
        routing_bucket: u32,
        block_ref_key: std::num::NonZeroU64,
    }
    #[allow(dead_code)]
    enum NicheTwoArm {
        One(NicheRef),
        Many(Box<[NicheRef]>),
    }
    #[allow(dead_code)]
    enum NicheThreeArm {
        Absent,
        One(NicheRef),
        Many(Box<[NicheRef]>),
    }

    // --- THE CONTROL: identical shapes, plain `u64`, so a flat 24 is interpretable ---
    #[allow(dead_code)]
    struct PlainRef {
        routing_bucket: u32,
        block_ref_key: u64,
    }
    #[allow(dead_code)]
    enum PlainTwoArm {
        One(PlainRef),
        Many(Box<[PlainRef]>),
    }

    println!();
    println!("  {:<42} {:>6} {:>14}", "shape", "B", "as an Option");
    for (name, bare, wrapped) in [
        ("BlockRefs (ships)", size_of::<BlockRefs>(), size_of::<Option<BlockRefs>>()),
        ("PlainTwoArm, Box<[T]>, no niche (control)", size_of::<PlainTwoArm>(), size_of::<Option<PlainTwoArm>>()),
        ("NicheTwoArm, NonZeroU64 key", size_of::<NicheTwoArm>(), size_of::<Option<NicheTwoArm>>()),
        ("NicheThreeArm, None folded in", size_of::<NicheThreeArm>(), size_of::<Option<NicheThreeArm>>()),
    ] {
        println!("  {name:<42} {bare:>6} {wrapped:>14}");
    }

    // The niche must not change the FIELD width, or the comparison is between two different
    // structures rather than two layouts of one.
    assert_eq!(
        size_of::<BlockLookupRef>(),
        size_of::<NicheRef>(),
        "the mirror ref is not the same width as the shipped one, so nothing below compares"
    );
    // THE CONTROL MUST SHOW NO SAVING, or a niche reading of 16 proves nothing.
    assert_eq!(
        size_of::<BlockRefs>(),
        size_of::<PlainTwoArm>(),
        "boxing the wide arm WITHOUT a niche changed the union's width; the control that makes \
         a niche reading interpretable does not hold"
    );

    // --- WHAT THE SLOT WOULD DO, which is the only home that pays ---
    let slot_today = 16 + 8 + size_of::<Option<BlockRefs>>();
    let slot_two = 16 + 8 + size_of::<Option<NicheTwoArm>>();
    let slot_three = 16 + 8 + size_of::<NicheThreeArm>();
    println!();
    println!("  the slot is 16 + 8 + whatever holds the refs:");
    println!("    ships                       {slot_today} B");
    println!("    NonZeroU64 + Option         {slot_two} B");
    println!("    NonZeroU64, None folded in  {slot_three} B");
    println!();
    println!(
        "  a bucket of (Arc<str>, slot) + 1 control byte: {} today, {} folded",
        16 + slot_today + 1,
        16 + slot_three + 1
    );

    // THE VERDICT, PINNED EITHER WAY. This is not an assertion that the lever is dead; it is an
    // assertion that the TABLE above is what the compiler says. If a future rustc gives a
    // multi-value niche to one of these shapes, this fails and the lever is reopened with a
    // measurement rather than discovered by someone re-deriving it.
    assert_eq!(
        24,
        size_of::<BlockRefs>(),
        "the shipped union moved; every figure here is relative to it"
    );
    assert!(
        slot_two >= slot_today || slot_three < slot_today,
        "the niche shapes measured {slot_two} and {slot_three} against the shipped {slot_today}; \
         if either is genuinely smaller the refusal recorded beside this arm is stale and the \
         eight-bytes-an-object lever is available"
    );
}
/// WHAT THE FOLDED SLOT ACTUALLY WEIGHS, AND WHAT THE `Option` COSTS OVER THE UNION.
///
/// The fold was priced against a bare `BlockRefs` at 24 B, giving a 48-byte entry and 51.0 B/key.
/// The shipped field is an `Option` -- because absent and empty must stay distinguishable, see
/// `ObjectSlot::refs` -- so the real width decides how much of that survives. Printed rather than
/// described, and asserted so the figure in the change's own write-up cannot drift from the layout
/// the compiler chose.
#[test]
fn what_the_folded_slot_weighs() {
    use crate::engine::object_record::ObjectSlot;
    use crate::engine::state::{BlockLookupRef, BlockRefs};
    use std::mem::size_of;
    use std::sync::Arc;

    let option_over_union = size_of::<Option<BlockRefs>>() - size_of::<BlockRefs>();
    println!();
    for (name, w) in [
        ("BlockRefs", size_of::<BlockRefs>()),
        ("Option<BlockRefs>", size_of::<Option<BlockRefs>>()),
        ("BlockLookupRef", size_of::<BlockLookupRef>()),
        ("ObjectSlot (folded)", size_of::<ObjectSlot>()),
        ("(key, slot) + 1, the bucket", size_of::<(Arc<str>, ObjectSlot)>() + 1),
    ] {
        println!("    {name:<30} {w:>4} B");
    }
    println!();
    println!("    the Option costs {option_over_union} B over the union it wraps");
    println!(
        "    so the folded bucket is {} B against the 41 the slot map had and the 65 the \
         bare-union model assumed",
        size_of::<(Arc<str>, ObjectSlot)>() + 1
    );
    println!();

    // The union itself must not have moved -- every figure here is relative to it.
    assert_eq!(24, size_of::<BlockRefs>(), "the union's own width moved");
    // And the slot is exactly its three terms, which is the claim the const pins make too. Stated
    // again as a RUNTIME assertion so a reader of the output can check the arithmetic.
    assert_eq!(
        size_of::<ObjectSlot>(),
        16 + 8 + size_of::<Option<BlockRefs>>(),
        "the slot is the record, the optional element array and the optional refs; a width that \
         is not their sum means a fourth term or padding appeared"
    );
}
