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

/// WHICH KINDS FILE REFS FOR AN OBJECT THE CONSOLIDATED MAP HAS NO ROW FOR.
///
/// Drives one object of every kind this probe can reach through the public command surface, then
/// walks `object_block_lookup` and asks the consolidated map for each entry's row.
///
/// # WHY THIS FAILS WHEN THE BLOCKER IS REMOVED, AND WHY THAT IS THE POINT
///
/// The assertion is that the unhoused column is NOT EMPTY, which is a tripwire rather than a
/// property worth having: it fails the day every kind that files refs also holds a row, which is
/// exactly the day the fold becomes implementable. Its deletion condition is therefore stated
/// rather than left to a reader -- see the message, which names the change to make.
///
/// A comment would not fail. This does, which is the whole reason it is a test and not a paragraph
/// on `ObjectBlockLookup`.
///
/// rust-internal: measures this crate's own structures, no product behaviour
#[test]
fn which_kinds_file_refs_for_an_object_the_slot_map_has_no_row_for() {
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
        table_name: "where-an-objects-refs-could-live".to_string(),
        shard_uri: "local://where-an-objects-refs-could-live/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(loaded.status.ok, "the fixture could not load shard 1: {:?}", loaded.status);

    // ONE OBJECT OF EACH KIND THE COMMAND SURFACE REACHES. Each is a kind
    // `visit_model_live_blocks` has an arm for, so each files refs through the same
    // kind-agnostic `insert_object_block_lookup` the write path uses for a string.
    let commands = vec![
        Command::StringSet { key: "a-string".to_string(), value: vec![b'v'; 64] },
        Command::HashSet {
            key: "a-hash".to_string(),
            field: "f".to_string(),
            value: vec![b'v'; 64],
        },
        Command::SetAdd { key: "a-set".to_string(), member: b"m".to_vec() },
        Command::ZSetAdd { key: "a-zset".to_string(), member: b"m".to_vec(), score: 1.0 },
        Command::ListPush { key: "a-list".to_string(), member: b"m".to_vec(), left: false },
        Command::FeatureAppend {
            key: "a-feature".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_000,
                value: b"one".to_vec(),
            }],
        },
        Command::ControlStateIncrement {
            key: "a-control-state".to_string(),
            timestamp_ms: 1_000,
            amount: 1,
        },
    ];
    for command in commands {
        let response = engine.execute(ExecuteRequest { shard_id: 1, command: command.clone() });
        assert!(response.status.ok, "seed {command:?} must ack: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // --- THE WALK. Per kind: how many objects the lookup files refs for, and how many of those
    // --- the consolidated map holds a row for.
    let mut housed: std::collections::BTreeMap<&'static str, (usize, usize)> =
        std::collections::BTreeMap::new();
    for (kind_name, object_key, _refs) in shard.bucket_index.object_block_lookup.iter() {
        let kind = StoredModelKind::from_stored_name(kind_name).unwrap_or_else(|| {
            panic!(
                "the lookup is keyed by a spelling the registry does not declare: {kind_name:?}"
            )
        });
        let row = shard.objects_of_kind(kind).contains_key(object_key.as_ref());
        let counts = housed.entry(kind.as_str()).or_insert((0, 0));
        counts.0 += 1;
        if row {
            counts.1 += 1;
        }
    }

    println!();
    println!("  {:<22} {:>8} {:>10} {:>10}", "KIND", "objects", "with a row", "unhoused");
    let mut unhoused_kinds: Vec<&'static str> = Vec::new();
    let mut with_a_row_kinds: Vec<&'static str> = Vec::new();
    for (kind, (objects, with_row)) in &housed {
        println!("  {kind:<22} {objects:>8} {with_row:>10} {:>10}", objects - with_row);
        if objects > with_row {
            unhoused_kinds.push(kind);
        }
        if *with_row > 0 {
            with_a_row_kinds.push(kind);
        }
    }
    println!();
    println!("  kinds whose refs have a row to fold into: {with_a_row_kinds:?}");
    println!("  kinds whose refs have NO row to fold into: {unhoused_kinds:?}");
    println!();

    // --- NON-VACUITY FIRST. A walk over an empty lookup prints an empty table, which reads
    // --- exactly like a store in which every kind is housed.
    assert!(
        !housed.is_empty(),
        "the lookup filed refs for no object at all, so neither column below says anything. The \
         seed commands did not reach the block index -- check that they acked and that this \
         fixture's routing range admits them."
    );
    // --- THE CONTROL. At least one kind MUST be housed, or this probe cannot see a row and the
    // --- unhoused column is an artefact of the instrument rather than a fact about the store.
    assert!(
        !with_a_row_kinds.is_empty(),
        "no kind at all has a row in the consolidated map, so this probe cannot distinguish \
         `unhoused` from `cannot see a row`. Every figure above is uninformative until the \
         control is populated: {housed:?}"
    );
    // --- AND THE BLOCKER ITSELF, as a tripwire with its deletion condition in the message.
    assert!(
        !unhoused_kinds.is_empty(),
        "EVERY kind that files block refs now holds a row in the consolidated object map, so the \
         fold priced at 50.9 B/key by \
         `index_bytes_per_key::what_folding_the_block_refs_into_the_slot_would_save` has a home \
         for every object's refs and this tripwire has done its job. DELETE THIS TEST and re-open \
         the fold -- see `ObjectBlockLookup`'s fourth ground for the second precondition, which is \
         that the index encoder must be able to reach the holder: \
         `serialize_block_index` REFUSES a page whose identity is missing, and it draws that \
         identity from a field of `CoreIndex`. Housed: {with_a_row_kinds:?}"
    );
}
/// THE ENCODER'S IDENTITY SOURCE IS A FIELD OF `CoreIndex`, AND IT REFUSES WITHOUT ONE.
///
/// # WHY THIS IS THE OTHER PRECONDITION ON THE FOLD
///
/// `impl Serialize for CoreIndex` composes every live page's written key from
/// [`BlockKeysByPosition::from_lookup`] over `self.object_block_lookup`, because the page entry
/// carries no identity of its own. Fold the refs into the consolidated object map and that field
/// is gone -- and the consolidated map is `ShardState::objects`, a SIBLING of `bucket_index` on a
/// struct that derives `Serialize`. `BucketNodeAt`'s note says why no derive and no
/// `serialize_with` can hand it down: both see one field at a time.
///
/// SO THE CONSEQUENCE IS NOT A BAD INDEX, IT IS NO INDEX. `serialize_block_index` returns `Err`
/// for a page with nothing beside it, naming which of the two states it is in. This drives that
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

    // --- THE STATE A FOLD OUT OF `CoreIndex` LEAVES THE ENCODER IN: the page is placed and this
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

    // --- THE CONTROL. The same placed page, with the filing a production placement does, encodes.
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
