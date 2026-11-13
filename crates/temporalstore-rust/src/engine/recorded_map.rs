// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE RECORDED MAP FOR EVERY MODEL KIND.
//!
//! `RecordedHashContainer` and `RecordedSetContainer` were two types with one shape. This is that
//! shape, once, generic over the kind -- and the two of them are gone, not kept beside it.
//!
//! # WHAT MADE THIS A GENERALISATION RATHER THAN A REWRITE
//!
//! [`super::ElementMap`] already existed and already spanned every kind. It is the level-2
//! abstraction -- the elements of ONE object -- and the tree ships three implementations that
//! between them cover all five maps this type is for:
//!
//!   * `HashFieldMap` -- hash, element `String`, value `BlockAddress`
//!   * the blanket `BTreeMap<E, V>` -- set (`Vec<u8>`), zset (value `(u64, BlockAddress)`),
//!     list (`i64`), features (`u64`)
//!   * `HashMap<String, V>`
//!
//! So nothing in the key types resists, and a zset needs no composite key: it is a `BTreeMap`
//! whose VALUE is the score-plus-address pair. More importantly `merge_container_elements` and
//! `fill_absent_elements` are ALREADY generic over `ElementMap`, so the two heaviest exceptions --
//! the delta fold and the reconcile -- generalise for free rather than being written twice more.
//!
//! # WHAT WE TAKE FROM THE SHAPE, AND THE TWO THINGS WE DELIBERATELY DO NOT
//!
//! We take one generic map, three mutators, and deletion expressed as a value rather than a flag.
//!
//! WE DO NOT TAKE A MONOTONIC TIMESTAMP IN THE WRAPPER. Our index log already carries sequence
//! numbers, so a stamp here would make one fact answerable from two places, and the two could
//! disagree. The thing a stamp would order is already ordered.
//!
//! WE DO NOT TAKE THE SINGLE-FUNCTION EMISSION. The shape being generalised here logs and then
//! assigns inside ONE function, which means the ordering is a convention inside that function: a
//! future edit can delete the log call and the code still compiles. Ours emits FIRST and the
//! emitter returns a WITNESS -- a struct whose single field is private to the emitter's own module,
//! so nothing else can construct one -- and the mutator REQUIRES it. Delete the emission and the
//! witness is gone, the proof cannot be built, and the call stops compiling.
//!
//! THAT DISTINCTION IS NOT THEORETICAL AND IT COST A ROUND TO FIND. The first version of the
//! one-call surface built the proof from the CALLER'S OWN ARGUMENTS. Deleting the emission compiled
//! cleanly and the defect surfaced only at run time, as `staged 0 records, not 1`, while the doc
//! comment claimed the compiler would catch it -- a guard holding a false claim. The witness exists
//! because driving that test to fail exposed the gap between the claim and the code. It is the one
//! place this design is deliberately stricter than the shape it follows, and it should not be
//! described as matching it.
//!
//! # THE EXCEPTIONS ARE PER-KIND DATA, NOT A COMMENT
//!
//! Five paths mutate a model map WITHOUT recording, because on each the durable record is the
//! SOURCE of the change rather than its consequence. Three are shared by every kind. Two are NOT,
//! and the asymmetry is the thing two concrete containers taught that one could not have:
//!
//!   * [`ReplaysInstallsUnrecorded`] -- hash only. Recovery's `context_node` arm installs a hash
//!     element and files NO record of its own, because a context node is never registered in the
//!     bucket index. The `set` replay arm re-files its block, so it needs no such path.
//!   * [`RepacksAfterDecode`] -- set, zset, list, features; NOT hash. A decode fills a `BTreeMap`
//!     by ascending insertion, the one order that leaves it half empty. `HashFieldMap` is a sorted
//!     vector whose length is its capacity, so it has nothing to pack.
//!
//! These are MARKER TRAITS, so the asymmetry is two `impl` lines and the compiler enforces it: a
//! kind that does not declare the marker cannot call the method at all. Flattening them into one
//! surface with a runtime check, or into a comment saying "only hash does this", would have thrown
//! away the only part of this that two instances established.

use super::state::ModelKey;
use std::collections::{HashMap, HashSet};

use crate::block_store::ElementEntry;
use crate::ShardId;

use super::state::ShardState;
use super::storage_bucket_internals::{BlockFiled, ObjectDeletionFiled};
use super::{ElementMap, LiveBlockKey};

#[path = "container_ordinal_cache.rs"]
mod ordinals;
use ordinals::{OrdinalCache, OrdinalSummary};

/// What distinguishes one model kind from another, and nothing else.
pub(super) trait RecordedKind: Sized + 'static {
    /// The elements of one object, which is the level-2 container.
    ///
    /// THE FOUR EXTRA BOUNDS ARE STATED HERE RATHER THAN INHERITED FROM A DERIVE. `RecordedMap`
    /// has always needed them -- it derives `Debug`, `Clone`, `PartialEq` and `Eq` -- but it got
    /// them for free: when a field's type is an associated-type projection like
    /// `HashMap<ModelKey, K::Elements>`, the derive adds a `where K::Elements: Debug` predicate of
    /// its own. The value is a `RecordedRow<K>` now, which is not a projection, so that free
    /// predicate disappears and the requirement has to be said out loud.
    ///
    /// Nothing is tightened in practice: all four concrete element maps already satisfy these, and
    /// a kind added later that does not would have failed on the derive anyway -- it would just
    /// have failed somewhere less obvious than the line that states the requirement.
    type Elements: ElementMap<Value: super::CarriedValue> + AddressedElementMap + RemovableElementMap + std::fmt::Debug + Clone + PartialEq + Eq;

    /// The model kind as the bucket index spells it. Spelled ONCE per kind here, so no arm can
    /// misspell it and no two arms can disagree.
    const KIND: &'static str;

    /// How this kind's map is encoded, and there is deliberately NO DEFAULT.
    ///
    /// THIS EXISTS BECAUSE ITS ABSENCE WAS A WIRE REGRESSION, caught by three reload tests. The
    /// first version of this generic type had one blanket `Serialize` doing
    /// `self.entries.serialize(..)` for every kind. That is correct for `hashes`, whose field was a
    /// plain `#[serde(default)]` map -- and WRONG for `sets`, whose field carried
    /// `#[serde(default, with = "super::set_index_serde")]` precisely because a
    /// `BTreeMap<Vec<u8>, _>` does not round-trip through a JSON object key. Collapsing the two
    /// containers silently dropped that codec: `set_index_serde` was left referenced only from
    /// comments, and a set's map stopped surviving a reload.
    ///
    /// So the codec is a REQUIRED associated function. A kind added later cannot inherit a wrong
    /// default; it has to say which encoding it uses, and saying "the plain map" is one line.
    /// TAKES THE MAP, NOT ITS INNER `HashMap`, because the inner value is a row now and the row is
    /// private to this module. Every impl writes the ELEMENTS only, so the stored bytes are what
    /// they were before the row existed.
    fn serialize_entries<S>(map: &RecordedMap<Self>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer;

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<ModelKey, Self::Elements>, D::Error>
    where
        D: serde::Deserializer<'de>;

    /// Where this kind's map lives on the shard.
    ///
    /// Rust cannot abstract over a struct FIELD, so this is the one thing each kind must supply by
    /// hand -- and supplying it is what lets the operations below be written once for all kinds
    /// instead of once per kind.
    fn resident(shard: &mut ShardState) -> &mut RecordedMap<Self>;

    /// Bytes to carry on the WAL outcome's `value` slot for one install, beside the block address
    /// every kind already carries -- and deliberately NO DEFAULT, for the same reason
    /// `serialize_entries` has none.
    ///
    /// THIS EXISTS BECAUSE A ZSET'S ELEMENT NAME STOPPED SPELLING THE SCORE. Every other kind's level-2
    /// value is fully named by its element name and its address -- a hash field, a set member, a list
    /// sequence -- so `None` costs them nothing. A zset's value is `(u64, BlockAddress)`, and the
    /// `u64` had been riding the element name as `{biased:016x}` until that string collapsed to the
    /// member alone; this is where it rides instead, on replay's only remaining path to it. A kind
    /// added later that also needs a byte string beside its address has to say so here rather than
    /// inherit silence -- the same defect class `serialize_entries`'s own doc comment describes.
    fn outcome_value(value: &<Self::Elements as ElementMap>::Value) -> Option<Vec<u8>>;
}

/// THIS KIND HAS A RECOVERY PATH THAT INSTALLS AN ELEMENT AND FILES NOTHING.
///
/// A marker, so the exception is data the compiler reads. Only `hashes` declares it, because only
/// its `context_node` replay arm installs without re-filing a block.
pub(super) trait ReplaysInstallsUnrecorded: RecordedKind {}

/// THIS KIND'S LEVEL-2 CONTAINER IS A B-TREE THAT A DECODE LEAVES HALF EMPTY.
///
/// A marker, as above. `hashes` does NOT declare it: a sorted vector has no half-empty nodes.
pub(super) trait RepacksAfterDecode: RecordedKind {}

/// The resident model map of one kind, readable from anywhere in `engine` and writable only through
/// the recorded operations below.
///
/// `entries` is private to this module. That single fact is the invariant: `rustc` refuses a
/// `&mut shard.<kind>.entries` from any other module, so the complete set of operations that can
/// change a model map is the set declared here -- once, for every kind.
#[derive(Debug)]
pub(super) struct RecordedMap<K: RecordedKind> {
    /// PRIVATE, AND THE WHOLE POINT. Do not add an accessor returning `&mut` to this.
    /// `engine::tests::recorded_map_invariant` fails if the module grows one.
    ///
    /// THE KEY IS A SHARED HANDLE. All four kinds were measured holding their OWN copy of a key
    /// text four other maps had already agreed on -- eight holders of one text reporting FIVE
    /// allocations, at 8 of 8 keys, in
    /// `which_string_keyed_holders_duplicate_a_key_another_map_holds`. Being private is what kept
    /// that true for so long: the key could not be converted from outside this module, so the
    /// adoption had to arrive as a declared operation here. See `adopt_key` below.
    entries: HashMap<ModelKey, RecordedRow<K>>,
    ordinals: OrdinalCache,
    timestamp_hints: HashMap<ModelKey, TimestampHint>,
}

/// ONE ROW PER OBJECT: this object's elements AND its per-object facts, in a single entry.
///
/// # WHY THIS TYPE EXISTS
///
/// `string`, `control_state` and `feature` already keep their block refs on their own row, in
/// `ObjectSlot::refs`. These four kinds could not, because this map's value WAS the elements and
/// nothing else -- so their refs live in `CoreIndex::object_block_lookup`, a second structure keyed
/// by the same object text. `where_an_objects_refs_could_live::where_each_kinds_refs_end_up`
/// measures that split: 4 of 4 objects in the lookup for every one of `hash`, `set`, `zset` and
/// `list`, against 4 of 4 in the row for the three kinds that have one.
///
/// # IT IS NOT ON THE WIRE, AND THAT IS WHY IT OWES NO FORMAT STAMP
///
/// `serialize_entries` writes the ELEMENTS only -- exactly the bytes it wrote before -- and the
/// decode re-files every block it reads. That is the same bargain `ObjectSlot::refs` makes for the
/// kinds that already have a row, and the reason the fold that built that field owed no stamp
/// either.
///
/// "Owes no stamp" is a claim about an encoder, so it is CHECKED rather than argued:
/// `recorded_map_invariant::the_four_kind_encodings_are_byte_for_byte_what_they_were` pins all four
/// kinds' exact stored bytes AND their map-versus-sequence shapes, so this type cannot change what
/// any kind writes without that arm failing by name.
struct RecordedRow<K: RecordedKind> {
    elements: K::Elements,
    /// THE BLOCKS THIS OBJECT'S DATA LIVES IN, or nothing when it names none.
    ///
    /// `Option` and not a bare `BlockRefs` for the reason `ObjectSlot::refs` gives: `BlockRefs` has
    /// no empty arm, so "names no block" would have to be spelled as an empty `Many`, and a caller
    /// that distinguishes "names nothing" by `None` would take the wrong branch.
    refs: Option<crate::engine::state::BlockRefs>,
}

/// Hand-written, for the reason `RecordedMap`'s own is: a derive would demand `K: Default`, and the
/// kind markers are types nobody constructs.
impl<K: RecordedKind> Default for RecordedRow<K> {
    fn default() -> Self {
        Self {
            elements: K::Elements::default(),
            refs: None,
        }
    }
}

impl<K: RecordedKind> RecordedRow<K> {
    /// NO ELEMENTS, SO NOT AN OBJECT. Such a row survives only while its refs do: the blocks of an
    /// object outlive its last element until the deletion that follows clears them, and those blocks
    /// are found through these refs. The slot a row sits in is the one place they are kept, so the
    /// row is not dropped while it holds any -- and is not an object either, which is what every
    /// existence reader of the map asks this for.
    fn is_vacant(&self) -> bool {
        self.elements.is_empty_map()
    }

    /// Whether nothing at all is left to keep this row for.
    fn is_spent(&self) -> bool {
        self.refs.is_none() && self.is_vacant()
    }
}

// THE FOUR IMPLS BELOW ARE HAND-WRITTEN BECAUSE A DERIVE BOUNDS THE TYPE PARAMETER AND NOT THE
// ASSOCIATED TYPE. `#[derive(Debug)]` on this struct emits `impl<K: Debug> Debug`, which says
// nothing about `K::Elements` -- the type the field actually is -- so it fails to satisfy
// `RecordedMap`'s own derive. Written out, each carries the bound its field needs.

impl<K: RecordedKind> std::fmt::Debug for RecordedRow<K>
where
    K::Elements: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordedRow")
            .field("elements", &self.elements)
            .field("refs", &self.refs)
            .finish()
    }
}

impl<K: RecordedKind> Clone for RecordedRow<K>
where
    K::Elements: Clone,
{
    fn clone(&self) -> Self {
        Self {
            elements: self.elements.clone(),
            refs: self.refs.clone(),
        }
    }
}

/// EQUALITY IS OVER THE ELEMENTS AND NOT THE REFS, and that is deliberate rather than an omission.
///
/// The refs are DERIVED: they are absent from the stored bytes and re-filed by the load path, so a
/// decoded map legitimately holds `None` where the live map it was written from held `Some`.
/// Comparing them would make `assert_eq!(decoded, original)` fail for every object that names a
/// block -- a round-trip arm reporting the codec wrong when the codec is right.
///
/// This is the same correction `ObjectRecord` needed once its flag word began carrying bits that
/// were derived rather than identity: equality there runs over the identity terms only. The rule is
/// that a derived field must not enter equality, because the two sides of a reload disagree about
/// it by design.
impl<K: RecordedKind> PartialEq for RecordedRow<K>
where
    K::Elements: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.elements == other.elements
    }
}

impl<K: RecordedKind> Eq for RecordedRow<K> where K::Elements: Eq {}

#[derive(Debug)]
struct TimestampHint {
    // User-field data only, never an index or WAL ordering stamp.
    field: super::hash_field_map::FieldName,
    address: ElementEntry,
    value: TimestampHintValue,
}

#[derive(Debug)]
pub(super) enum TimestampHintValue {
    Inline { bytes: [u8; 24], length: u8 },
    Parsed(Option<(i64, i32)>),
}

impl TimestampHintValue {
    pub(super) fn from_bytes(value: &[u8]) -> Self {
        if value.len() <= 24 {
            let mut bytes = [0; 24];
            bytes[..value.len()].copy_from_slice(value);
            Self::Inline { bytes, length: value.len() as u8 }
        } else {
            Self::Parsed(super::execute_on_shard::decode_hash_timestamp(value))
        }
    }

    pub(super) fn parsed(&self) -> Option<(i64, i32)> {
        match self {
            Self::Inline { bytes, length } => super::execute_on_shard::decode_hash_timestamp(&bytes[..*length as usize]),
            Self::Parsed(value) => *value,
        }
    }

    pub(super) fn raw_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline { bytes, length } => Some(&bytes[..*length as usize]),
            Self::Parsed(_) => None,
        }
    }
}

impl<K: RecordedKind> Clone for RecordedMap<K> where K::Elements: Clone {
    fn clone(&self) -> Self {
        // THE CACHES ARE NOT CARRIED: they are derived, and a clone rebuilds them on demand.
        Self { entries: self.entries.clone(), ordinals: OrdinalCache::default(), timestamp_hints: HashMap::new() }
    }
}

impl<K: RecordedKind> PartialEq for RecordedMap<K> where K::Elements: PartialEq {
    fn eq(&self, other: &Self) -> bool { self.entries == other.entries }
}

impl<K: RecordedKind> Eq for RecordedMap<K> where K::Elements: Eq {}

impl<K: RecordedKind> Default for RecordedMap<K> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            ordinals: OrdinalCache::default(),
            timestamp_hints: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE PROOFS. One set, generic over the kind, each carrying the identity of what was recorded so
// the mutator writes the element the record names and cannot be handed a proof for another.
// ---------------------------------------------------------------------------------------------

/// PROOF THAT THE DURABLE RECORD FOR ONE ELEMENT IS ALREADY OUT.
#[must_use = "a recorded element that is never installed leaves the record describing a block the \
              resident map does not hold"]
pub(super) struct RecordedElement<K: RecordedKind> {
    /// The emitter's witness. Not read -- HOLDING it is the point, because it cannot be obtained
    /// without the record having been filed.
    #[allow(dead_code)]
    filed: BlockFiled,
    object_key: String,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
}

/// PROOF THAT THE REMOVAL OF ONE ELEMENT IS ALREADY RECORDED.
///
/// NO WITNESS, and that is stated rather than hidden. Its emitter
/// `execute_on_shard::remove_container_element` is shared with six other arms, so a witness return
/// means changing all seven. Measured, not guessed, and the same is true for every kind -- which is
/// one small argument for this type: the gap is now in ONE place instead of once per kind.
#[must_use = "a recorded element removal that is never applied leaves the element resident after \
              its block was tombstoned"]
pub(super) struct RecordedElementRemoval<K: RecordedKind> {
    object_key: String,
    element: <K::Elements as ElementMap>::Element,
}

/// PROOF THAT THE DELETION OF A WHOLE OBJECT IS ALREADY RECORDED IN THE BUCKET INDEX.
#[must_use = "a recorded object deletion that is never applied leaves the object resident after the \
              index says it is gone"]
pub(super) struct RecordedObjectRemoval<K: RecordedKind> {
    #[allow(dead_code)]
    filed: ObjectDeletionFiled,
    object_key: String,
    kind: std::marker::PhantomData<K>,
}

// ---------------------------------------------------------------------------------------------
// THE OPERATIONS. One call each, written ONCE for every kind: emit the record, then mutate through
// the proof.
//
// FREE FUNCTIONS, NOT METHODS, and that is forced rather than stylistic.
// `upsert_bucket_index_block` takes `&mut ShardState` -- the WHOLE shard -- because filing a block
// reloads a released bucket, interns the element name and touches the pending-flag set. A model
// map is a FIELD of `ShardState`, so a method holding `&mut self` cannot lend the whole shard to
// the emitter, and a closure capturing the shard fails for the same reason. A free function clears
// it because the two borrows are SEQUENTIAL: the emitter's `&mut shard` ends when it returns, and
// only then is the field borrowed to apply the proof.
// ---------------------------------------------------------------------------------------------

// =============================================================================================
// THE THREE DOORS THAT KEEP THE ROW HOME HONEST, and why there are exactly three.
//
// A ref has three homes now and each one needs a WRITER, a READER and a DELETER. The reader is
// `state::object_block_refs`, which chains all three. These are the row's writer and deleters:
//
//   * `adopt_refs_into_row` -- the per-object writer, called from `install_element` after the
//     install that creates the row, mirroring `CoreIndex::adopt_refs_into_slot`.
//   * `adopt_every_lookup_ref_into_its_row` -- the same thing swept over the whole lookup, for the
//     door that installs a reconstructed index, mirroring
//     `CoreIndex::adopt_every_lookup_ref_into_its_slot`.
//   * `prune_row_refs_to_the_blocks_the_index_holds` -- the deleter, mirroring
//     `CoreIndex::prune_refs_to_the_pages_the_index_holds`.
//
// THE DELETER IS WHY THIS TOOK THREE ATTEMPTS TO SCOPE. `CoreIndex` cannot reach a `RecordedMap`
// -- siblings on `ShardState` -- so the index's own prune cannot see a row, and a reconstruct
// REPLACES the index wholesale. Wire the writer without the deleter and a reconstruct leaves refs
// naming blocks the new index does not hold: the same shape as the defect where an install
// replaced a slot row and dropped its refs, except this is the half nothing reports.
//
// Both callers that needed a `ShardState` turned out to have one. `install_reconstructed` has a
// single production caller, `rebuild_bucket_first_index`, which already holds `shard`; and
// `fold_delta_block_items` takes a `&mut CoreIndex` but its only caller,
// `fold_index_log_deltas(.., shard: &mut ShardState)`, holds the shard. **I had read both
// signatures and concluded no shard was in scope -- the signature is not the call site.**
// =============================================================================================

/// ADOPT EVERY LOOKUP REF WHOSE ROW NOW EXISTS, across the four kinds, and answer how many moved.
///
/// The row counterpart of `CoreIndex::adopt_every_lookup_ref_into_its_slot`, called from the same
/// place: the door that installs a reconstructed index. A reconstruct derives its filings from the
/// model maps and files them all into the lookup, so without this sweep every ref of these four
/// kinds would sit in the lookup again after every reconstruct.
pub(super) fn adopt_every_lookup_ref_into_its_row(shard: &mut ShardState) -> usize {
    // COLLECTED FIRST, because the adoption mutates the lookup it would otherwise be iterating.
    let pairs: Vec<(std::sync::Arc<str>, std::sync::Arc<str>)> = shard
        .bucket_index
        .object_block_lookup
        .iter()
        .map(|(model_id, object_key, _refs)| {
            (std::sync::Arc::clone(model_id), std::sync::Arc::clone(object_key))
        })
        .collect();
    let mut moved = 0usize;
    for (model_id, object_key) in pairs {
        let key = object_key.as_ref();
        let did = if &*model_id == HashKind::KIND {
            adopt_refs_into_row::<HashKind>(shard, key)
        } else if &*model_id == SetKind::KIND {
            adopt_refs_into_row::<SetKind>(shard, key)
        } else if &*model_id == ZSetKind::KIND {
            adopt_refs_into_row::<ZSetKind>(shard, key)
        } else if &*model_id == ListKind::KIND {
            adopt_refs_into_row::<ListKind>(shard, key)
        } else {
            false
        };
        if did {
            moved += 1;
        }
    }
    moved
}

/// PRUNE EVERY ROW'S REFS TO THE BLOCKS THE INDEX ACTUALLY HOLDS, and answer how many went.
///
/// THE DELETER THE ROW HOME WOULD OTHERWISE NOT HAVE. `CoreIndex::prune_refs_to_the_pages_the_index_holds`
/// does this for the slot and the lookup and cannot reach a row. A reconstruct replaces the index,
/// so a row ref naming a block the NEW index does not hold names nothing at all -- and a reader
/// that chains the row home would hand that dead ref out.
pub(super) fn prune_row_refs_to_the_blocks_the_index_holds(shard: &mut ShardState) -> usize {
    // DESTRUCTURED so `bucket_map` can be borrowed immutably while the four maps are borrowed
    // mutably. They are disjoint fields, which the compiler can only see if they are named.
    //
    // IT IS `CoreIndex` THAT IS TAKEN APART NOW, NOT `ShardState`. This read
    // `let ShardState { bucket_index, hashes, sets, zsets, lists, .. }` while the four container
    // maps were top-level `ShardState` fields, siblings of `bucket_index`. They are fields OF the
    // index since `SHARD_INDEX_FORMAT_VERSION` 31, so the sibling destructure names fields that no
    // longer exist there and the disjoint borrow has to be taken one level down.
    //
    // AND THE PREDICATE IS A FREE FUNCTION FOR THAT REASON: `CoreIndex::holds_block_ref` is
    // unavailable here precisely because the struct it is a method on has just been destructured,
    // so both callers share `bucket_map_holds_block_ref` rather than the two lines being copied.
    let crate::engine::state::CoreIndex {
        bucket_map,
        hashes,
        sets,
        zsets,
        lists,
        ..
    } = &mut shard.bucket_index;
    let holds = |block_ref: &crate::engine::state::BlockLookupRef| -> bool {
        crate::engine::state::CoreIndex::bucket_map_holds_block_ref(bucket_map, block_ref)
    };
    hashes.retain_refs(&holds)
        + sets.retain_refs(&holds)
        + zsets.retain_refs(&holds)
        + lists.retain_refs(&holds)
}

/// MOVE THIS OBJECT'S REFS OUT OF THE LOOKUP AND ONTO ITS ROW.
///
/// The counterpart of [`CoreIndex::adopt_refs_into_slot`] for the four kinds whose rows are not
/// `ObjectSlot`s, and it exists for the same reason that one does: a first write files its block
/// BEFORE the element naming it is installed, because the filer runs first and the row is created
/// by the install. So at filing time there is no row, the refs go to the lookup, and this is what
/// moves them once the row exists.
///
/// Called from `install_element` below, immediately after the install that creates the row -- which
/// is where the slot route calls its own adoption from.
pub(super) fn adopt_refs_into_row<K: RecordedKind>(shard: &mut ShardState, object_key: &str) -> bool {
    if !K::resident(shard).contains_key(object_key) {
        return false;
    }
    let Some(adopted) = shard
        .bucket_index
        .object_block_lookup
        .remove(K::KIND, object_key)
    else {
        return false;
    };
    // MERGED RATHER THAN REPLACED, which is the rule `adopt_refs_into_slot` states at its own
    // merge: an overwrite can have filed a block into the row already, so replacing would drop it
    // -- one block of an object going missing with no error anywhere.
    K::resident(shard).merge_refs(object_key, adopted)
}

pub(super) fn install_element<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    element_name: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) where
    K::Elements: AddressedElementMap,
{
    install_element_with_policy::<false, K>(
        shard, shard_id, object_key, element_name, element, value, dirty, address,
    );
}

/// Only for a frame just returned by an append, never for replay or refiling an existing address.
pub(super) fn install_appended_element<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    element_name: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) where
    K::Elements: AddressedElementMap,
{
    install_element_with_policy::<true, K>(
        shard, shard_id, object_key, element_name, element, value, dirty, address,
    );
}

fn install_element_with_policy<const APPENDED: bool, K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    element_name: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) where
    K::Elements: AddressedElementMap,
{
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket = super::hashing::block_routing_bucket(
        object_key,
        start_routing_bucket,
        end_routing_bucket,
    );
    let object_id = super::stable_block_object_id(shard_id, K::KIND, object_key);
    install_element_at_bucket_with_policy::<APPENDED, K>(
        shard,
        shard_id,
        routing_bucket,
        object_id,
        object_key,
        element_name,
        element,
        value,
        dirty,
        address,
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn install_element_at_bucket<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    routing_bucket: u32,
    object_id: u64,
    object_key: &str,
    element_name: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) where
    K::Elements: AddressedElementMap,
{
    install_element_at_bucket_with_policy::<false, K>(
        shard, shard_id, routing_bucket, object_id, object_key, element_name, element, value, dirty, address,
    );
}

#[allow(clippy::too_many_arguments)]
fn install_element_at_bucket_with_policy<const APPENDED: bool, K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    routing_bucket: u32,
    object_id: u64,
    object_key: &str,
    element_name: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) where
    K::Elements: AddressedElementMap,
{
    // COMPUTED BEFORE `value` MOVES INTO THE RECORD BELOW. The only producer of a `Some` today is
    // `ZSetKind`, whose score has nowhere else to ride now that the element name does not spell it.
    let outcome_value = K::outcome_value(&value);
    // WHICH BLOCK THIS WRITE REPLACES, ASKED WHILE THE MAP STILL HOLDS THE OLD ANSWER.
    //
    // The filer cannot derive this. Under one entry a block an entry carries no element name, so
    // the only discriminator it has is the address -- and a rewrite lands at a NEW address, so the
    // landing key names no existing entry and the element's previous entry is never superseded.
    // See `AddressedElementMap` for the two measured losses that leaves.
    //
    // ASKED ONLY WHEN THE ENTRY IS BLOCK-NAMED. While an entry names its element the filer's own
    // element-name-keyed supersede already takes the predecessor, so there is nothing to tell it and
    // nothing to pay for: the ungated path does not reach the scan at all.
    let replaces = if super::storage_bucket_internals::index_entry_names_a_block(K::KIND) {
        K::resident(shard).block_an_element_vacates(object_key, &element, &address)
    } else {
        None
    };
    // THE ROW BEFORE THE FILING, so the first block's ref lands on it and not in a staging home.
    K::resident(shard).ensure_row(object_key);
    // THE RECORD FIRST. The proof below cannot be built without the witness this returns, so
    // deleting this call is a compile error rather than a silent unrecorded write.
    let replaced = if APPENDED && replaces.is_none() {
        super::storage_bucket_internals::ReplacedBlock::Appended
    } else {
        super::storage_bucket_internals::ReplacedBlock::of(replaces)
    };
    let filed = super::storage_bucket_internals::upsert_bucket_index_block_filed_at_bucket(
        shard,
        shard_id,
        routing_bucket,
        object_id,
        K::KIND,
        object_key,
        element_name,
        address,
        dirty,
        outcome_value,
        replaced,
    );
    // THEN THE MUTATION, through the proof.
    K::resident(shard).install(RecordedElement::<K> {
        filed,
        object_key: object_key.to_string(),
        element,
        value,
    });
    // AND THEN THE ADOPTION, because the row exists only now.
    //
    // The filer above ran BEFORE this install, so at filing time this object had no row and its
    // refs went to `object_block_lookup`. This moves them onto the row, which is what makes the row
    // retire lookup entries rather than duplicate them. It was held back while the block-to-object
    // inversion read only two homes -- a deletion marked nothing for refs that had moved -- and the
    // inversion spans all three now, so the move can be made.
    adopt_refs_into_row::<K>(shard, object_key);
}

enum WrittenHashFields<'a, 'b> {
    Shared(&'b super::container_blocks::SharedHashFields),
    Owned(HashSet<&'a str>),
}

impl WrittenHashFields<'_, '_> {
    fn contains(&self, field: &str) -> bool {
        match self {
            Self::Shared(fields) => fields.contains(field),
            Self::Owned(fields) => fields.contains(field),
        }
    }
}

fn written_hash_fields<'a, 'b>(
    fields: impl Iterator<Item = &'a str> + Clone,
    shared: Option<&'b super::container_blocks::SharedHashFields>,
) -> WrittenHashFields<'a, 'b> {
    match shared.filter(|shared| shared.matches(fields.clone())) {
        Some(shared) => WrittenHashFields::Shared(shared),
        None => WrittenHashFields::Owned(fields.collect()),
    }
}

pub(super) fn install_packed_hash_block_at_bucket<'a>(
    cache: &matrixcache::MultiLayerCache,
    shard: &mut ShardState,
    shard_id: ShardId,
    routing_bucket: u32,
    object_id: u64,
    object_key: &str,
    fields: impl Iterator<Item = &'a str> + Clone,
    address: ElementEntry,
    shared: Option<&super::container_blocks::SharedHashFields>,
) {
    let Some(first) = fields.clone().next() else {
        return;
    };
    super::storage_bucket_internals::reload_released_bucket(shard, shard_id, routing_bucket);
    let existing = shard.bucket_index.hashes.get(object_key);
    let shared = shared.filter(|shared| shared.matches(fields.clone()));
    let complete = existing.zip(shared).is_some_and(|(existing, shared)| {
        existing.len() == shared.names().len()
            && existing.keys().map(|name| name.as_ref())
                .eq(shared.sorted_names().iter().map(|name| name.as_ref()))
    });
    let mut written = None;
    let replaces = if let Some(existing) = existing {
        let landing = super::live_block_key(&address);
        let mut vacated = if complete {
            super::storage_bucket_internals::ReplacedBlock::from_keys(existing.values()
                .map(super::live_block_key).filter(|key| *key != landing))
        } else {
            super::storage_bucket_internals::ReplacedBlock::from_keys(fields.clone()
                .filter_map(|field| existing.get(field))
                .map(super::live_block_key).filter(|key| *key != landing))
        };
        if !complete && !vacated.is_empty() {
            let names = written_hash_fields(fields.clone(), shared);
            for (field, old) in existing.iter() {
                if !names.contains(field.as_ref()) {
                    vacated.remove(super::live_block_key(old));
                    if vacated.is_empty() {
                        break;
                    }
                }
            }
            written = Some(names);
        }
        vacated
    } else {
        super::storage_bucket_internals::ReplacedBlock::none()
    };
    // Only frames unused by surviving fields are retired; partial updates keep shared frames.
    replaces.visit(|(slab, offset, length)| {
        super::command_validation::invalidate_if_cached(
            cache,
            matrixcache::CacheKey::page_with_slot(shard_id, slab, offset, length, Some(routing_bucket)),
        );
    });
    // Replay expands the framed block into every field; one filed outcome records the whole row.
    let filed = super::storage_bucket_internals::upsert_bucket_index_block_filed_at_bucket(
        shard,
        shard_id,
        routing_bucket,
        object_id,
        HashKind::KIND,
        object_key,
        Some(first.to_string()),
        address.clone(),
        true,
        None,
        replaces,
    );
    // A re-add must clear every written field's tombstone, not just the outcome's first name.
    if shard.bucket_index.bucket_map.get(&routing_bucket)
        .is_some_and(|bucket| !bucket.tombstone_elements.is_empty())
    {
        let written = written.get_or_insert_with(|| written_hash_fields(fields.clone(), shared));
        super::storage_bucket_internals::sweep_packed_hash_tombstones(
            shard, routing_bucket, object_key, |field| written.contains(field),
        );
    }
    install_packed_hash_fields(shard, object_key, fields, address, filed, complete);
}

fn install_packed_hash_fields<'a>(
    shard: &mut ShardState,
    object_key: &str,
    fields: impl Iterator<Item = &'a str>,
    address: ElementEntry,
    _filed: BlockFiled,
    complete: bool,
) {
    // Exact names were checked under this exclusive shard borrow before the block was filed.
    if complete {
        if let Some(existing) = HashKind::resident(shard).entries.get_mut(object_key) {
            for held in existing.elements.values_mut() {
                *held = address.clone();
            }
            return;
        }
    }
    for field in fields {
        let handle = super::state::intern_shared(&mut shard.bucket_index.kind_pool, field);
        HashKind::resident(shard).install_into(object_key, handle, address.clone());
    }
}

/// The same, for a record that is a STAGED OUTCOME and no bucket-index entry.
///
/// A context node's OUTCOME is deliberately never recorded under kind `hash` -- doing so would have
/// a rebuild add an entry naming a block the outcome never described -- so the record this emitter
/// files is the staged outcome alone. That is a different record, which is why it is a different
/// emitter rather than a boolean on the one above: a boolean would have let an arm pick the wrong
/// record silently.
///
/// CORRECTED: this said the node is "never registered in the bucket index". The BLOCK is registered
/// -- `append_value` files it from the object id, `CONTEXT_NODE_FIELD` and the routing bucket, and
/// `context_node_survives_reload` reads `("hash", Some("meta"), deleted=false)` back out of the
/// index. Only the outcome's KIND differs.
#[allow(clippy::too_many_arguments)]
pub(super) fn install_element_staged_only<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    staged_kind: &str,
    object_key: &str,
    element_name: &str,
    routing_bucket: u32,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    address: ElementEntry,
) {
    let staged = super::block_in_wal::stage_outcome_attested(crate::wal::WalOutcomeItem {
        kind: staged_kind.to_string(),
        object_key: object_key.to_string(),
        element_name: Some(element_name.to_string()),
        object_id: super::stable_block_object_id(shard_id, staged_kind, object_key),
        routing_bucket,
        address: Some(address),
        value: None,
        ttl: None,
        deleted: false,
        meta: false,
    });
    K::resident(shard).install(RecordedElement::<K> {
        filed: staged.into_block_filed(),
        object_key: object_key.to_string(),
        element,
        value,
    });
}

/// Tombstone the element's block, clear its bucket-index entry, and drop it from the resident map.
///
/// Returns whether anything changed -- the removal's own answer OR the resident drop's. Both are
/// folded here because every caller OR-ed them into one `mutated` flag, and a removal that found no
/// block still has to reach the resident map: the resident map is exactly where a stale copy would
/// survive.
#[allow(clippy::too_many_arguments)]
pub(super) fn remove_element<K: RecordedKind>(
    cache: &matrixcache::MultiLayerCache,
    block_store: &crate::block_store::BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    element_name: &str,
    element: <K::Elements as ElementMap>::Element,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool
where
    K::Elements: RemovableElementMap + AddressedElementMap,
{
    let vacated = K::resident(shard).block_an_element_removal_vacates(object_key, &element);
    let removal = super::execute_on_shard::remove_container_element(
        cache,
        block_store,
        shard,
        shard_id,
        K::KIND,
        object_key,
        element_name,
        start_routing_bucket,
        end_routing_bucket,
        async_storage,
    );
    let dropped = K::resident(shard).remove_element(RecordedElementRemoval::<K> {
        object_key: object_key.to_string(),
        element,
    });
    if removal.tombstone_written && dropped {
        if let Some(address) = vacated {
            super::storage_bucket_internals::drop_vacated_live_block(shard, K::KIND, object_key, &address);
        }
    }
    // AND WHEN THAT WAS THE LAST ELEMENT, THE INDEX HAS TO LET GO TOO.
    //
    // EXISTENCE HAS TWO SOURCES AND EMPTYING ONE DOES NOT EMPTY THE KEY. `record_exists_exact` ORs
    // the resident map's answer together with the bucket index's, so an object is gone only once
    // BOTH are. The resident removal above is unconditional -- which is why membership is right
    // under either projection -- but under one entry a block the removal deliberately keeps the live
    // block entry, on the stated grounds that the block still holds the object's other members.
    //
    // That justification is false for the LAST element: there are none. The retained entry then
    // keeps the key enumerable with nothing in it, and KEYS, SCAN, DBSIZE, EXISTS, TYPE and EXPIRE
    // all answer for it -- `redis::tests::redis_core_api_extensions_use_engine_and_state` is the
    // arm that caught it, through those surfaces rather than through the index.
    //
    // ASKED OF THE RESIDENT MAP RATHER THAN OF THE INDEX, because the resident map is the authority
    // for existence and the index is the derived view. And asked AFTER the removal above, so it
    // reads the post-removal state rather than predicting it.
    if K::resident(shard).get(object_key).is_none() {
        // THE LIVE ENTRIES ONLY. Not `mark_bucket_index_object_deleted`, which takes the tombstones
        // too: one live entry and one tombstone are different facts. The live entry claims this
        // object has a block with members on it, which is false once the last member goes. The
        // tombstone records that a NAMED element was removed, which stays true and is what makes
        // the removal win a fold by append position -- taking it along would undo the removal it
        // records, and `container_tombstone_entry::a_re_add_clears_the_tombstone_entry_so_churn_on_
        // one_element_does_not_accumulate` is the arm that holds the difference at one live and one
        // tombstone per cycle.
        super::storage_bucket_internals::drop_live_object_entries(shard, K::KIND, object_key);
    }
    removal.index_removed || dropped
}

/// Return a vacated block so replay can file its tombstone before retiring that block.
pub(super) fn remove_replayed_element<K: RecordedKind>(
    shard: &mut ShardState,
    object_key: &str,
    element: &<K::Elements as ElementMap>::Element,
) -> Option<ElementEntry>
where
    K::Elements: RemovableElementMap + AddressedElementMap,
{
    let vacated = K::resident(shard).block_an_element_removal_vacates(object_key, element);
    K::resident(shard).replay_remove_element(object_key, element).then_some(vacated).flatten()
}

/// Drop a recorded object's whole element map.
///
/// TAKES the deletion witness rather than minting one, because ONE object deletion authorises a
/// drop in EVERY recorded map. `delete_record_exact` marks once and hands a clone to each kind;
/// minting per kind would file a second record of one deletion, and `mark_bucket_index_object_deleted`
/// settles a released bucket by reading the block's address out of the model map WHILE THE MAP STILL
/// HOLDS IT, so the mark must also precede every drop. That is the one call site in the engine where
/// a token is threaded, and it is threaded because one record genuinely authorises several mutations.
pub(super) fn drop_object<K: RecordedKind>(
    shard: &mut ShardState,
    object_key: &str,
    filed: ObjectDeletionFiled,
) -> bool {
    K::resident(shard).remove_object(RecordedObjectRemoval::<K> {
        filed,
        object_key: object_key.to_string(),
        kind: std::marker::PhantomData,
    })
}

/// Removing one element needs the level-2 container to be able to remove one, which `ElementMap`
/// does not require -- it is the FOLD's trait and a fold only inserts.
///
/// Declared here rather than widened onto `ElementMap` so that the fold's contract stays the fold's
/// contract: a kind that can be folded into but never has an element removed is a legitimate shape,
/// and widening would have forced it to implement something no caller needs.
pub(super) trait RemovableElementMap: ElementMap {
    fn remove_element(&mut self, element: &Self::Element) -> bool;
    fn is_empty_map(&self) -> bool;
}

// ---------------------------------------------------------------------------------------------
// READS. Plain delegation: no allocation, no copy, no clone.
// ---------------------------------------------------------------------------------------------

impl<K: RecordedKind> RecordedMap<K> {
    /// Move this object's entry onto the shared key handle the block index owns, returning whether
    /// anything moved.
    ///
    /// A DECLARED OPERATION RATHER THAN A `&mut` ACCESSOR. The invariant this module exists to
    /// hold is that the complete set of things which can change `entries` is declared here; an
    /// accessor handing out `&mut` would end that, and `recorded_map_invariant` would fail. So
    /// adoption is one named operation with one job.
    ///
    /// It re-keys and does not touch the value, so it cannot change what any read answers -- the
    /// text is equal by definition, or the lookup that found it would not have. What changes is
    /// how many allocations hold that text: before this existed, all four kinds owned their own
    /// copy of a key the adopted maps already shared.
    ///
    /// Call it AFTER the block is filed. Before that the block index holds no handle to borrow, the
    /// caller has nothing to pass, and the entry keeps the copy it minted at its write site --
    /// which is exactly how a wired sharing mechanism went 0 for 40,040 once before.
    pub(super) fn adopt_key(&mut self, object_key: &str, canonical: &ModelKey) -> bool {
        let moves = match self.entries.get_key_value(object_key) {
            Some((held, _)) => !std::sync::Arc::ptr_eq(held, canonical),
            None => false,
        };
        if !moves {
            return false;
        }
        match self.entries.remove(object_key) {
            Some(value) => {
                self.entries.insert(std::sync::Arc::clone(canonical), value);
                self.ordinals.adopt_key(canonical);
                if let Some(hint) = self.timestamp_hints.remove(object_key) {
                    self.timestamp_hints.insert(canonical.clone(), hint);
                }
                true
            }
            None => false,
        }
    }

    /// THE ELEMENTS OF A LIVE OBJECT. A row with no elements is not an object: it exists only to
    /// carry the refs of blocks that outlive the last element, and every existence answer below
    /// skips it, so the row changes no answer this engine gives about a key.
    pub(super) fn get(&self, object_key: &str) -> Option<&K::Elements> {
        self.entries
            .get(object_key)
            .filter(|row| !row.is_vacant())
            .map(|row| &row.elements)
    }

    pub(super) fn highest_block_id(&self, object_key: &str) -> Option<u64> {
        let Some((key, row)) = self.entries.get_key_value(object_key) else {
            #[cfg(test)]
            super::state::SIBLING_SCAN_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        };
        let elements = &row.elements;
        self.ordinals.highest(key, || {
            #[cfg(test)]
            super::state::SIBLING_SCAN_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut summary = OrdinalSummary::default();
            elements.visit_addresses(|address| {
                #[cfg(test)]
                super::state::SIBLING_SCAN_ELEMENTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Some(id) = address.block_id() { summary.insert(id); }
            });
            summary
        })
    }

    /// Point every ELEMENT name in every entry at the pool's copy, and answer how many moved.
    ///
    /// Declared here for the same reason `adopt_key` is: `entries` is private to this module,
    /// so the decode pass cannot reach into it from outside. Only `hashes` has a pooled element
    /// name today -- the other three kinds are keyed by bytes -- so this is bounded by the
    /// trait rather than written per kind.
    pub(super) fn adopt_element_names(
        &mut self,
        pool: &mut std::collections::HashSet<std::sync::Arc<str>>,
    ) -> usize
    where
        K::Elements: PoolableElementNames,
    {
        self.entries
            .values_mut()
            .map(|row| row.elements.adopt_element_names(pool))
            .sum()
    }

    pub(super) fn contains_key(&self, object_key: &str) -> bool {
        self.entries
            .get(object_key)
            .is_some_and(|row| !row.is_vacant())
    }

    pub(super) fn len(&self) -> usize {
        self.entries.values().filter(|row| !row.is_vacant()).count()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.values().all(RecordedRow::is_vacant)
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &ModelKey> {
        self.iter().map(|(key, _)| key)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &K::Elements> {
        self.iter().map(|(_, elements)| elements)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&ModelKey, &K::Elements)> {
        self.entries
            .iter()
            .filter(|(_, row)| !row.is_vacant())
            .map(|(key, row)| (key, &row.elements))
    }

    /// THE BLOCKS ONE OBJECT'S DATA LIVES IN, read from the row the object is already filed under.
    ///
    /// This is the door that makes `CoreIndex::object_block_lookup` unnecessary for these kinds:
    /// the answer is a field of the entry this map already holds, rather than an entry in a second
    /// structure keyed by the same text.
    pub(super) fn refs(&self, object_key: &str) -> Option<&crate::engine::state::BlockRefs> {
        self.entries.get(object_key)?.refs.as_ref()
    }

    /// THE ROW'S OWN KEY AND ITS REFS, for an inversion that names the object by the allocation the
    /// map already holds rather than by a copy of its text.
    pub(super) fn refs_with_key(
        &self,
        object_key: &str,
    ) -> Option<(&ModelKey, &crate::engine::state::BlockRefs)> {
        let (key, row) = self.entries.get_key_value(object_key)?;
        row.refs.as_ref().map(|refs| (key, refs))
    }

    /// Put this object's refs on its row, and answer whether there was a row to put them on.
    ///
    /// FALSE IS A REAL ANSWER AND NOT A FAILURE. A first write files its block BEFORE the element
    /// that names it is installed, so at that moment the object has no row -- the same window
    /// `ObjectSlot` has, which is why the slot route needed an adoption step as well as a filing
    /// one. A caller that gets `false` must keep the ref where it kept it before.
    pub(super) fn set_refs(
        &mut self,
        object_key: &str,
        refs: crate::engine::state::BlockRefs,
    ) -> bool {
        match self.entries.get_mut(object_key) {
            Some(row) => {
                row.refs = Some(refs);
                true
            }
            None => false,
        }
    }

    /// FILE ONE REF ON THIS OBJECT'S ROW, answering whether it was new, or `None` when the object
    /// has no row yet -- in which case the caller keeps the ref where it kept it before, and the
    /// install that creates the row adopts it. A row with no refs yet takes its first one built
    /// into it, so there is no empty state in between.
    pub(super) fn file_ref(
        &mut self,
        object_key: &str,
        value: crate::engine::state::BlockLookupRef,
    ) -> Option<bool> {
        let row = self.entries.get_mut(object_key)?;
        Some(match row.refs.as_mut() {
            Some(refs) => refs.insert(value),
            None => {
                row.refs = Some(crate::engine::state::BlockRefs::One(value));
                true
            }
        })
    }

    /// WHETHER THE OBJECT HAS A ROW, vacant or not -- which is NOT whether it exists as an object.
    pub(super) fn has_row(&self, object_key: &str) -> bool {
        self.entries.contains_key(object_key)
    }

    /// GIVE AN OBJECT A ROW BEFORE ITS FIRST BLOCK IS FILED, so the filer has somewhere to put the
    /// ref and no object ever has refs anywhere but its row.
    ///
    /// The row it makes is VACANT -- no elements -- and so is not an object by any existence reader;
    /// the install that follows in the same call fills it. That is what lets the staging home go
    /// away: a first write used to file before its row existed, so the ref waited in the lookup for
    /// an adoption to move it.
    pub(super) fn ensure_row(&mut self, object_key: &str) {
        if !self.entries.contains_key(object_key) {
            self.entries
                .insert(std::sync::Arc::from(object_key), RecordedRow::default());
        }
    }

    /// Take this object's refs off its row, leaving the row and its elements in place.
    pub(super) fn take_refs(
        &mut self,
        object_key: &str,
    ) -> Option<crate::engine::state::BlockRefs> {
        let row = self.entries.get_mut(object_key)?;
        let taken = row.refs.take();
        if row.is_spent() {
            self.entries.remove(object_key);
        }
        taken
    }

    /// MERGE refs onto this object's row, or install them when it carries none.
    ///
    /// MERGED AND NOT ASSIGNED, for the reason `CoreIndex::adopt_refs_into_slot` gives at its own
    /// merge: an overwrite can have filed a block into the row already, so assigning would drop
    /// it, which is one block of an object going missing with nothing raised anywhere.
    ///
    /// Answers whether there was a row to merge onto. `false` is a real answer, not a failure --
    /// see `set_refs`.
    pub(super) fn merge_refs(
        &mut self,
        object_key: &str,
        adopted: crate::engine::state::BlockRefs,
    ) -> bool {
        let Some(row) = self.entries.get_mut(object_key) else {
            return false;
        };
        match row.refs.as_mut() {
            Some(existing) => {
                for block_ref in adopted.iter() {
                    existing.insert(block_ref.clone());
                }
            }
            None => row.refs = Some(adopted),
        }
        true
    }

    /// DROP EVERY REF THE PREDICATE REJECTS, and answer how many went.
    ///
    /// The row counterpart of `CoreIndex::prune_refs_to_the_pages_the_index_holds`, which cannot
    /// reach these rows: a reconstruct replaces the index, and a ref naming a block the NEW index
    /// does not hold has to go or it names nothing.
    ///
    /// CLEARS A ROW'S REFS TO `None` AND NOT TO AN EMPTY SET, which is the rule the slot home
    /// states at its own removal: `BlockRefs::remove` turns the inline arm into an empty spilled
    /// one, and an empty `Some` makes a reader answer `Some(&[])` where callers distinguish "names
    /// nothing" by `None` -- after which a re-add takes the wrong branch and leaks a tombstone.
    pub(super) fn retain_refs(
        &mut self,
        keep: &dyn Fn(&crate::engine::state::BlockLookupRef) -> bool,
    ) -> usize {
        let mut dropped = 0usize;
        for row in self.entries.values_mut() {
            let Some(refs) = row.refs.as_mut() else {
                continue;
            };
            let stale: Vec<crate::engine::state::BlockLookupRef> =
                refs.iter().filter(|r| !keep(r)).cloned().collect();
            for block_ref in &stale {
                if refs.remove(block_ref) {
                    dropped += 1;
                }
            }
            if refs.is_empty() {
                row.refs = None;
            }
        }
        self.entries.retain(|_, row| !row.is_spent());
        dropped
    }

    /// DROP ONE REF FROM ONE OBJECT'S ROW, clearing to `None` when it was the last.
    ///
    /// For the un-file half of a delta fold, which drops a block and may place another at the same
    /// handle in the same call. Clears to `None` for the reason `retain_refs` gives.
    pub(super) fn remove_ref(
        &mut self,
        object_key: &str,
        block_ref: &crate::engine::state::BlockLookupRef,
    ) -> bool {
        let Some(row) = self.entries.get_mut(object_key) else {
            return false;
        };
        let Some(refs) = row.refs.as_mut() else {
            return false;
        };
        let removed = refs.remove(block_ref);
        if refs.is_empty() {
            row.refs = None;
        }
        if row.is_spent() {
            self.entries.remove(object_key);
        }
        removed
    }

    /// EVERY ROW THAT CARRIES REFS, for the INVERSION to file.
    ///
    /// `BlockKeysByPosition` maps a block handle back to its object key, and it builds that by
    /// walking the ref homes. It had two to walk; this is the third. Without it the inversion stops
    /// naming any block whose ref sits on a row, and a caller that asks "which object owns this
    /// block" gets `None` -- which an object deletion reads as "not mine" and skips.
    pub(super) fn rows_holding_refs(
        &self,
    ) -> impl Iterator<Item = (&ModelKey, &crate::engine::state::BlockRefs)> {
        self.entries
            .iter()
            .filter_map(|(key, row)| row.refs.as_ref().map(|refs| (key, refs)))
    }

    /// DROP EVERY ROW'S REFS, for the whole-shard rebuild that is about to re-file all of them.
    pub(super) fn take_every_refs(&mut self) {
        for row in self.entries.values_mut() {
            row.refs = None;
        }
        self.entries.retain(|_, row| !row.is_spent());
    }

    /// WHETHER ANY ROW CARRIES REFS, stopping at the first that does -- the question the per-write
    /// census asks, which must not cost a walk of every row the way a count does.
    pub(super) fn any_row_holds_refs(&self) -> bool {
        self.entries.values().any(|row| row.refs.is_some())
    }

    /// HOW MANY ROWS CARRY REFS, which is what a probe needs to tell a populated row apart from a
    /// row that merely exists.
    ///
    /// Without it an arm asserting "the refs are on the row now" passes just as well when every
    /// row's `refs` is `None` -- the failure mode an empty-population guard always has.
    pub(super) fn rows_with_refs(&self) -> usize {
        self.entries.values().filter(|row| row.refs.is_some()).count()
    }

    /// IMMUTABLE access for the serde adapters in this module, which have to hand the inner shape
    /// to an existing codec.
    ///
    /// PRIVATE, not `pub(super)`, which is a tightening the row forced rather than a choice: the
    /// value is a `RecordedRow` now and the row is private to this module, so a `pub(super)`
    /// return would leak a private type. `sets` and `zsets` need this because their field carries a
    /// `with =` codec that a `transparent` wrapper would have silently changed; those codecs now
    /// take an ITERATOR of `(key, elements)`, which is what `iter()` already yields.
    fn rows(&self) -> &HashMap<ModelKey, RecordedRow<K>> {
        &self.entries
    }

    /// THE BLOCK THIS WRITE VACATES, or nothing when it vacates none.
    ///
    /// `Some` only when all three hold:
    ///
    ///   * the map already holds `element`, so this is a REWRITE and not a first write;
    ///   * the address it holds names a DIFFERENT block from the one the write is landing on -- a
    ///     rewrite that lands back on its own block is superseded through the landing key and needs
    ///     nothing extra;
    ///   * and NO OTHER element of the same object is still on the block being left, because an
    ///     entry under the gate stands for the BLOCK and a block with a sibling still on it is still
    ///     live.
    ///
    /// ASKED BEFORE THE MUTATION, which is the only reason it can be asked at all: the caller files
    /// the record first, so the map still holds the pre-write address here.
    ///
    /// THE SCAN IS THE SAME ORDER AS WHAT THE FILER ALREADY PAYS. The third test is a
    /// short-circuiting `any` over ONE OBJECT's elements, where `upsert_bucket_index_block_inner`
    /// already walks the whole BUCKET's blocks twice per write -- once to decide whether the object
    /// still has a block there and once for the tombstone probe. One object's elements are a subset
    /// of its bucket's blocks, so this does not change the order of a container write.
    pub(super) fn block_an_element_vacates(
        &self,
        object_key: &str,
        element: &<K::Elements as ElementMap>::Element,
        landing: &ElementEntry,
    ) -> Option<ElementEntry>
    where
        K::Elements: AddressedElementMap,
    {
        let elements = &self.entries.get(object_key)?.elements;
        let previous = elements.element_address(element)?;
        if super::live_block_key(previous) == super::live_block_key(landing) {
            return None;
        }
        if elements.any_element_other_than_at(element, previous) {
            return None;
        }
        Some(previous.clone())
    }

    fn block_an_element_removal_vacates(
        &self,
        object_key: &str,
        element: &<K::Elements as ElementMap>::Element,
    ) -> Option<ElementEntry>
    where
        K::Elements: AddressedElementMap,
    {
        let elements = &self.entries.get(object_key)?.elements;
        let previous = elements.element_address(element)?;
        (!elements.any_element_other_than_at(element, previous)).then(|| previous.clone())
    }
}

/// The projecting iterator `&RecordedMap<K>` hands out.
///
/// A NAMED TYPE because `IntoIterator::IntoIter` is an associated type and cannot be `impl
/// Iterator`. It existed implicitly before as `hash_map::Iter`, which stopped describing the
/// iteration once the value became a row.
pub(super) struct RowElements<'a, K: RecordedKind> {
    inner: std::collections::hash_map::Iter<'a, ModelKey, RecordedRow<K>>,
}

impl<'a, K: RecordedKind> Iterator for RowElements<'a, K> {
    type Item = (&'a ModelKey, &'a K::Elements);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .by_ref()
            .find(|(_, row)| !row.is_vacant())
            .map(|(key, row)| (key, &row.elements))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.inner.size_hint().1)
    }
}

impl<'a, K: RecordedKind> IntoIterator for &'a RecordedMap<K> {
    type Item = (&'a ModelKey, &'a K::Elements);
    type IntoIter = RowElements<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        RowElements {
            inner: self.entries.iter(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE RECORDED MUTATORS. PRIVATE, which is the second half of the invariant: `entries` being
// private stops a direct write, and these being private stop a write through a proof someone got
// hold of. The only callers are the operations above, each of which has already recorded.
// ---------------------------------------------------------------------------------------------

impl<K: RecordedKind> RecordedMap<K> {
    /// Install an element, reaching an already-live object through a BORROW.
    ///
    /// `Arc::from(&str)` always mallocs and copies, and `entry()` takes its key BY VALUE -- so
    /// handing it a freshly built `Arc` for a slot that already exists allocates the key, copies
    /// the text into it, and drops it again. Measured at the map, 400 installs into ONE existing
    /// object: 64.9 B per write at an 8-byte key against 264.9 B at a 208-byte key -- a 200.0 B
    /// difference for a 200 B key, with the allocation COUNT identical in both arms, which is the
    /// signature of a copy rather than an extra allocation. `remove_element` below has always done
    /// the borrowed thing.
    ///
    /// ONE HOME FOR THE RULE. Every install arm routes here, so the borrow cannot be right in one
    /// and forgotten in another -- a second copy of a rule is wrong even while it agrees. The MISS
    /// path still builds the `Arc`, because then the map genuinely needs to own the key; that costs
    /// one extra hash of the key, once per object instead of once per element.
    fn install_into(
        &mut self,
        object_key: &str,
        element: <K::Elements as ElementMap>::Element,
        value: <K::Elements as ElementMap>::Value,
    ) {
        if self.ordinals.contains(object_key) {
            let previous = self.entries.get(object_key)
                .and_then(|row| row.elements.element_address(&element))
                .and_then(ElementEntry::block_id);
            self.ordinals.replace(object_key, previous, super::CarriedValue::carried_address(&value).block_id());
        }
        if let Some(row) = self.entries.get_mut(object_key) {
            row.elements.insert_element(element, value);
            return;
        }
        self.entries
            .entry(std::sync::Arc::from(object_key))
            .or_default()
            .elements
            .insert_element(element, value);
    }

    fn install(&mut self, recorded: RecordedElement<K>) {
        let RecordedElement {
            filed: _,
            object_key,
            element,
            value,
        } = recorded;
        self.install_into(&object_key, element, value);
    }

    /// Drop a recorded element, and the key with it when that element was its last.
    ///
    /// THE KEY GOES WHEN THE ELEMENT MAP EMPTIES, and that is not tidiness. `record_exists_exact`
    /// ORs each model map's `contains_key` in beside its bucket-index answer, so an empty element
    /// map under a live key is a key that EXISTS answers 1 for while every listing answers empty
    /// for -- and `CommonExpire` gates on the same function, so it accepted a deadline for such a
    /// key and `ttl_ms` then reported that deadline instead of the -2 of a missing key.
    ///
    /// Written once here, which is the point: it used to be a rule four arms had to remember, and
    /// the note at `HashDelete` said in a parenthesis that sets did not need it. They did.
    fn remove_element(&mut self, recorded: RecordedElementRemoval<K>) -> bool
    where
        K::Elements: RemovableElementMap,
    {
        let RecordedElementRemoval {
            object_key,
            element,
        } = recorded;
        let Some(row) = self.entries.get_mut(&object_key[..]) else {
            return false;
        };
        let previous = self.ordinals.contains(&object_key)
            .then(|| row.elements.element_address(&element).and_then(ElementEntry::block_id))
            .flatten();
        let removed = row.elements.remove_element(&element);
        if removed { self.ordinals.replace(&object_key, previous, None); }
        // AN OBJECT WITH NO ELEMENTS HAS NOTHING FOR THE CACHES TO DESCRIBE, whether or not its row
        // is kept for the refs it still holds.
        if row.is_vacant() {
            self.ordinals.invalidate(&object_key);
            self.timestamp_hints.remove(&object_key[..]);
        }
        if row.is_spent() {
            // THE ROW GOES WHEN NOTHING IS LEFT TO KEEP IT FOR: no elements and no refs. A row that
            // still holds refs stays, because those refs name blocks that outlive the last element
            // and are found through them; it is VACANT, which every existence reader skips. A row
            // kept alive WITHOUT that skip would make `contains_key` answer true for a
            // key every listing reports empty, which is exactly the defect the key removal here
            // exists to prevent: `record_exists_exact` ORs this map's `contains_key` into its
            // answer, and `CommonExpire` gates on that, so such a key accepted a deadline and
            // `ttl_ms` then reported it instead of the -2 of a missing key.
            //
            // So a row's lifetime is its ELEMENTS' OR ITS REFS' lifetime, whichever is longer, and
            // existence is answered by the elements alone. That keeps every existence answer in
            // this engine unchanged by the row.
            self.entries.remove(&object_key[..]);
            self.ordinals.invalidate(&object_key);
            self.timestamp_hints.remove(&object_key[..]);
        }
        removed
    }

    /// Empty an object's element map, keeping the row while it still holds refs (see
    /// [`RecordedRow::is_vacant`]). Answers whether there WAS an object to drop.
    fn remove_object(&mut self, recorded: RecordedObjectRemoval<K>) -> bool {
        self.ordinals.invalidate(&recorded.object_key);
        self.timestamp_hints.remove(&recorded.object_key[..]);
        let key = &recorded.object_key[..];
        let Some(row) = self.entries.get_mut(key) else {
            return false;
        };
        let was_live = !row.is_vacant();
        row.elements = K::Elements::default();
        if row.is_spent() {
            self.entries.remove(key);
        }
        was_live
    }
}

// ---------------------------------------------------------------------------------------------
// THE EXCEPTIONS. Each mutates WITHOUT a record because on that path the durable record is the
// SOURCE of the change. None hands out `&mut` to the map. Three are shared; two are per-kind and
// gated by a marker trait, so a kind that does not declare one cannot call it.
// ---------------------------------------------------------------------------------------------

impl<K: RecordedKind> RecordedMap<K> {
    /// SHARED EXCEPTION 1 -- THE DECODE, which is where a stored map comes back.
    ///
    /// Deserialization is not a recorded mutation: the decode IS the record. This was INVISIBLE
    /// while each kind had its own container -- `hashes` got it from `#[serde(transparent)]` and
    /// `sets` from a hand-written impl, so neither named it -- and collapsing the two is what
    /// surfaced it as a path. Naming it is strictly better than a derive doing it silently.
    /// THE REFS ARRIVE ABSENT, which is the whole reason this change owes no format stamp: they
    /// are not in the stored bytes, and the load path re-files every block it reads. Same bargain
    /// `ObjectSlot::refs` makes on the kinds that already have a row.
    pub(super) fn from_decoded(entries: HashMap<ModelKey, K::Elements>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(key, elements)| (key, RecordedRow { elements, refs: None }))
                .collect(),
            ordinals: OrdinalCache::default(),
            timestamp_hints: HashMap::new(),
        }
    }

    /// SHARED EXCEPTION 2 -- THE RECONCILE, where the durable index DECIDES and this map is the
    /// derived view of it.
    ///
    /// The persisted map is the OLDER of the two inputs and supplies only what the derived view
    /// could not produce, which is why the merge inserts where ABSENT rather than overwriting:
    /// letting the older map win would invert the rule `durable_outranks_derived` exists to hold.
    /// Logging here would be backwards -- the record is what this read FROM.
    pub(super) fn reconcile_from_durable(
        &mut self,
        derived: HashMap<ModelKey, K::Elements>,
        live: &HashSet<LiveBlockKey>,
        resurrections_refused: &mut usize,
    ) where
        K::Elements: IntoIterator<
            Item = (
                <K::Elements as ElementMap>::Element,
                <K::Elements as ElementMap>::Value,
            ),
        >,
        <K::Elements as ElementMap>::Element: std::hash::Hash + Eq + Clone,
        // `fill_absent_elements` drops a durable element whose block the settled index does not
        // hold, and it asks the VALUE for that address -- so the live-block filter needs this too.
        <K::Elements as ElementMap>::Value: super::CarriedValue,
    {
        self.ordinals.clear();
        self.timestamp_hints = HashMap::new();
        // THE REFS ARE CARRIED ACROSS THE REPLACEMENT RATHER THAN RE-DERIVED. This method takes
        // the whole map out and builds a new one from `fill_absent_elements`, so every row's refs
        // would be dropped BY CONSTRUCTION -- for every key, silently, with nothing failing and no
        // reader able to tell a lost ref from an object that never named a block.
        //
        // Dropping them would be sound only if this ran at load alone, where a refile follows. It
        // is not worth resting a whole shard's block refs on that being true, so they are set aside
        // by key and re-attached to whichever rows survive the merge. A key the merge drops keeps
        // its refs on a vacant row (below): the object is gone, its blocks are not.
        let mut carried_refs: HashMap<ModelKey, crate::engine::state::BlockRefs> = self
            .entries
            .iter_mut()
            .filter_map(|(key, row)| {
                row.refs
                    .take()
                    .map(|refs| (std::sync::Arc::clone(key), refs))
            })
            .collect();
        let persisted: HashMap<ModelKey, K::Elements> = std::mem::take(&mut self.entries)
            .into_iter()
            .map(|(key, row)| (key, row.elements))
            .collect();
        self.entries = super::storage_bucket_internals::fill_absent_elements(
            derived,
            persisted,
            live,
            resurrections_refused,
        )
        .into_iter()
        .map(|(key, elements)| {
            let refs = carried_refs.remove(&key);
            (key, RecordedRow { elements, refs })
        })
        .collect();
        // A KEY THE MERGE DROPPED KEEPS ITS REFS, on a vacant row: the object has no elements but
        // its blocks are still resident, and the refs are how a later deletion finds them.
        for (key, refs) in carried_refs {
            self.entries.insert(
                key,
                RecordedRow {
                    elements: K::Elements::default(),
                    refs: Some(refs),
                },
            );
        }
    }

    /// SHARED EXCEPTION 3 -- THE DELTA FOLD, where the carried blob IS the record.
    ///
    /// The carry is the newer statement, so here it WINS a collision -- the opposite of the
    /// reconcile above, which is why these are two methods and not one with a flag.
    pub(super) fn fold_carried_elements(
        &mut self,
        object_key: &str,
        carried: Option<&serde_json::Value>,
        live: &HashSet<LiveBlockKey>,
        skipped: &mut usize,
    ) where
        <K::Elements as ElementMap>::Value: super::CarriedValue,
    {
        self.ordinals.invalidate(object_key);
        self.timestamp_hints.remove(object_key);
        // ONE OBJECT'S ELEMENTS, LENT THROUGH A TEMPORARY MAP. `merge_container_elements` takes a
        // map of ELEMENTS and this map's values are rows now. The fold touches exactly the one
        // object it is given, so the temporary holds at most one entry -- it is not a copy of the
        // map.
        //
        // The object's REFS survive the lend: a fold changes which elements exist, not which blocks
        // the object names. `take()` is what stops them being attached twice if the helper ever
        // returns more than the key it was handed.
        let mut lent: HashMap<ModelKey, K::Elements> = HashMap::new();
        let mut held_refs = None;
        let mut held_key = None;
        if let Some((key, row)) = self.entries.remove_entry(object_key) {
            held_refs = row.refs;
            held_key = Some(std::sync::Arc::clone(&key));
            lent.insert(key, row.elements);
        }
        super::merge_container_elements(&mut lent, object_key, carried, live, skipped);
        for (key, elements) in lent {
            self.entries.insert(
                key,
                RecordedRow {
                    elements,
                    refs: held_refs.take(),
                },
            );
        }
        // A MERGE THAT LEFT NO ELEMENTS STILL LEAVES THE BLOCKS: the refs ride a vacant row.
        if let (Some(key), Some(refs)) = (held_key, held_refs) {
            self.entries.insert(
                key,
                RecordedRow {
                    elements: K::Elements::default(),
                    refs: Some(refs),
                },
            );
        }
    }

    /// SHARED EXCEPTION 4 -- WAL REPLAY OF A REMOVAL THAT IS ALREADY IN THE RECORD.
    ///
    /// Recovery is REPLAYING a removal the log already holds. Recording it again would write the
    /// recovery itself into the log as a fresh removal, so the next replay would have two removals
    /// where the store had one. Deliberately narrow: one element of one object, and it cannot
    /// install or drop a key.
    pub(super) fn replay_remove_element(
        &mut self,
        object_key: &str,
        element: &<K::Elements as ElementMap>::Element,
    ) -> bool
    where
        K::Elements: RemovableElementMap,
    {
        self.ordinals.invalidate(object_key);
        self.entries
            .get_mut(object_key)
            .map(|row| row.elements.remove_element(element))
            .unwrap_or(false)
    }

    /// SHARED EXCEPTION 5 -- COMPACTION REWRITES WHERE A BLOCK LIVES, NEVER WHICH BLOCKS LIVE.
    ///
    /// The move is what the compactor records; the address rewrite is bookkeeping that follows it,
    /// and a record of its own would say a block was installed when none was.
    ///
    /// Narrowed by the lend: each item yields the object key and an [`ElementValuesMut`], which
    /// lends `&mut` to the VALUES and nothing else -- no insert, no remove, no retain -- so this
    /// provably cannot change the membership of the map, only where its members point. That is
    /// strictly less than the `iter_mut` it replaces.
    pub(super) fn element_values_mut(
        &mut self,
    ) -> impl Iterator<Item = (&str, ElementValuesMut<'_, K>)> {
        self.ordinals.clear();
        self.entries.iter_mut().map(|(object_key, row)| {
            (
                &**object_key,
                ElementValuesMut {
                    elements: &mut row.elements,
                },
            )
        })
    }
}

/// PER-KIND EXCEPTION, hash only.
impl<K: ReplaysInstallsUnrecorded> RecordedMap<K> {
    /// Recovery installs an element and files NOTHING, because this kind has a replay arm that
    /// files no OUTCOME record of its own: the log item being replayed IS the record.
    ///
    /// CORRECTED: this said the arm's block "is never registered in the bucket index", and that is
    /// false -- the index does hold an entry for it. What the arm does not do is file an outcome,
    /// which is the thing this exception exempts it from.
    ///
    /// GATED BY A MARKER TRAIT, so a kind that has not declared the path cannot call this. `sets`
    /// has not: its replay arm re-files its block and goes through [`install_element`] like any
    /// other recorded write. That asymmetry is what two concrete containers established.
    pub(super) fn replay_install_element(
        &mut self,
        object_key: &str,
        element: <K::Elements as ElementMap>::Element,
        value: <K::Elements as ElementMap>::Value,
    ) {
        self.install_into(object_key, element, value);
    }
}

/// PER-KIND EXCEPTION, every kind whose level-2 container is a B-tree.
impl<K: RepacksAfterDecode> RecordedMap<K> {
    /// Repack each element map a decode filled one entry at a time.
    ///
    /// `serde`'s `Deserialize` for `BTreeMap` inserts in a loop over bytes written in key order, so
    /// every nested map in a decoded index is built by ASCENDING insertion -- the one order that
    /// leaves a B-tree half empty, because each full leaf splits and the left half is never filled
    /// again. NOTHING IS RECORDED BECAUSE NOTHING CHANGES: the same elements map to the same values
    /// afterwards, only the node occupancy moves.
    ///
    /// GATED, and `hashes` does not declare it: `HashFieldMap` is a sorted vector whose length is
    /// its capacity, so it has no half-empty nodes. This site was named by the COMPILER rather than
    /// by a survey -- `repack_decoded_btrees` reaches the field by destructuring the whole
    /// `ShardState`, so it has no `.sets` for any name-based search to find.
    pub(super) fn repack_decoded(&mut self)
    where
        K::Elements: RepackableElementMap,
    {
        for row in self.entries.values_mut() {
            row.elements.repack();
        }
    }
}

/// What a repack needs of a level-2 container, so the exception above does not have to know which
/// concrete B-tree it is holding.
pub(super) trait RepackableElementMap: ElementMap {
    fn repack(&mut self);
}

/// A lend of one object's element VALUES, and of nothing else.
///
/// It exists so the compaction exception cannot change membership: there is no `insert`, no
/// `remove`, no `retain`, and no way to recover the `&mut K::Elements` it holds.
pub(super) struct ElementValuesMut<'a, K: RecordedKind> {
    elements: &'a mut K::Elements,
}

impl<K: RecordedKind> ElementValuesMut<'_, K> {
    pub(super) fn iter_mut(
        &mut self,
    ) -> impl Iterator<
        Item = (
            &<K::Elements as ElementMap>::Element,
            &mut <K::Elements as ElementMap>::Value,
        ),
    >
    where
        K::Elements: IterableMutElementMap,
    {
        self.elements.iter_values_mut()
    }
}

/// What the compaction walk needs of a level-2 container.
pub(super) trait IterableMutElementMap: ElementMap {
    fn iter_values_mut(
        &mut self,
    ) -> impl Iterator<Item = (&Self::Element, &mut Self::Value)>;
}

/// WHICH BLOCK AN ELEMENT IS MOVING OFF, read from the map that is keyed BY THE ELEMENT.
///
/// # WHY THE FILER CANNOT WORK THIS OUT FOR ITSELF
///
/// Under one entry a block an index entry carries NO element name, so the entry a rewrite
/// supersedes cannot be picked out by naming the element. Keying the supersede on the address the
/// write LANDS AT finds the entry over the block it lands on -- which is right for an element
/// appended to a block that already has an entry, and finds NOTHING AT ALL for an element rewritten
/// IN PLACE, because a rewrite is written to a NEW address.
///
/// MEASURED, not reasoned: with the supersede keyed on the landing address alone, a gated hash
/// served ONE ELEMENT MORE than was ever written -- out of the stale live entry the rewrite did not
/// supersede -- and a zset rescore left TWO live entries for one member at one score. `set` and
/// `list` are immune only because neither can rewrite an element in place.
///
/// THE CALLER DOES KNOW. A write that overwrites an element has that element's PREVIOUS address in
/// the resident map, which is keyed by the element and still holds the old address at filing time:
/// [`install_element`] files the record BEFORE it mutates the map. This trait is how the generic
/// emitter asks for it without naming a concrete container.
///
/// # AND THE PREVIOUS ADDRESS ALONE IS NOT THE ANSWER
///
/// An entry under the gate stands for a BLOCK, not for an element. Retiring the entry over the block
/// an element vacates is WRONG whenever a SIBLING element of the same object is still on that block:
/// the block is still live, and its entry is what keeps the siblings reachable. So the question has
/// two halves, and [`AddressedElementMap::any_element_other_than_at`] is the second.
pub(super) trait AddressedElementMap: ElementMap {
    fn visit_addresses(&self, visit: impl FnMut(&ElementEntry));
    /// The address this map currently holds for `element`, if it holds the element at all.
    fn element_address(&self, element: &Self::Element) -> Option<&ElementEntry>;

    /// Whether any element OTHER THAN `skip` currently sits on the page `page` names.
    ///
    /// Compared by BLOCK KEY -- slab, offset and length -- and not by the whole address, because
    /// that triple is what an index entry's identity is under the gate.
    fn any_element_other_than_at(&self, skip: &Self::Element, block: &ElementEntry) -> bool;
}

// =============================================================================================
// THE KINDS. Each is a zero-sized marker plus the three or four facts that distinguish it, and the
// per-kind exception markers it declares. This is the whole per-kind surface: everything else above
// is written once.
// =============================================================================================

/// `shard.bucket_index.hashes` -- field name to block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HashKind;

impl RecordedMap<HashKind> {
    /// A hint is usable only while the resident field still names the same immutable block.
    fn timestamp_hint(&self, object_key: &str, field: &str) -> Option<&TimestampHint> {
        let hint = self.timestamp_hints.get(object_key)?;
        if &*hint.field != field { return None; }
        let current = self.entries.get(object_key)?.elements.get(field)?;
        (current == &hint.address).then_some(hint)
    }

    pub(super) fn cached_timestamp(&self, object_key: &str, field: &str) -> Option<Option<(i64, i32)>> {
        self.timestamp_hint(object_key, field).map(|hint| hint.value.parsed())
    }

    pub(super) fn cached_timestamp_bytes(&self, object_key: &str, field: &str) -> Option<&[u8]> {
        self.timestamp_hint(object_key, field)?.value.raw_bytes()
    }

    /// At most one comparator per resident hash; clone/serialization do not carry these hints.
    pub(super) fn remember_timestamp_value(&mut self, object_key: &str, field: &str, value: TimestampHintValue) {
        let Some((key, row)) = self.entries.get_key_value(object_key) else { return };
        let Some((name, address)) = row.elements.named_entry(field) else { return };
        if let Some(hint) = self.timestamp_hints.get_mut(object_key) {
            hint.field = name.clone();
            hint.address = address.clone();
            hint.value = value;
        } else {
            self.timestamp_hints.insert(key.clone(), TimestampHint {
                field: name.clone(), address: address.clone(), value,
            });
        }
    }
}

impl RecordedKind for HashKind {
    type Elements = super::hash_field_map::HashFieldMap;
    const KIND: &'static str = "hash";

    /// THE PLAIN MAP, which is what this field always was: a `#[serde(default)]` map of
    /// `String` to `HashFieldMap`, and `HashFieldMap` itself round-trips through the same MAP shape
    /// the bare `HashMap` used. So the bytes are unchanged by construction.
    fn serialize_entries<S>(map: &RecordedMap<Self>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_elements_as_map(map, serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<ModelKey, Self::Elements>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }

    fn resident(shard: &mut ShardState) -> &mut RecordedMap<Self> {
        &mut shard.bucket_index.hashes
    }

    /// A hash field's value is its address, which the outcome already carries whole. Nothing else
    /// to say.
    fn outcome_value(_value: &<Self::Elements as ElementMap>::Value) -> Option<Vec<u8>> {
        None
    }
}

/// HASH DECLARES THE REPLAY-INSTALL PATH, and it is the only kind that does.
///
/// Recovery's `context_node` arm installs a hash element and files no record of its own, because the
/// log item being replayed is already the record. (Not because the index lacks an entry for the
/// block -- it has one; see the method's own note.)
impl ReplaysInstallsUnrecorded for HashKind {}

// AND IT DELIBERATELY DOES NOT DECLARE `RepacksAfterDecode`: `HashFieldMap` is a sorted vector
// whose length IS its capacity below its growth threshold, so a decode leaves it exactly packed.

/// `shard.bucket_index.sets` -- member bytes to block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SetKind;

impl RecordedKind for SetKind {
    type Elements = std::collections::BTreeMap<Vec<u8>, ElementEntry>;
    const KIND: &'static str = "set";

    /// `set_index_serde`, WHICH IS THE CODEC THIS FIELD HAS ALWAYS USED, and the one the first
    /// version of the generic type silently dropped.
    ///
    /// A member is `Vec<u8>` and a JSON object key is a string, so a plain map encoding does not
    /// round-trip -- three reload tests said so. This module encodes each set's members as a
    /// sequence of pairs instead, and it also sorts the outer keys into a `BTreeMap`, which is why
    /// this field's encoding is deterministic where `hashes`'s is not.
    fn serialize_entries<S>(map: &RecordedMap<Self>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        super::set_index_serde::serialize(map.iter(), serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<ModelKey, Self::Elements>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        super::set_index_serde::deserialize(deserializer)
    }

    fn resident(shard: &mut ShardState) -> &mut RecordedMap<Self> {
        &mut shard.bucket_index.sets
    }

    /// A set member's value is its address, which the outcome already carries whole. Nothing else
    /// to say.
    fn outcome_value(_value: &<Self::Elements as ElementMap>::Value) -> Option<Vec<u8>> {
        None
    }
}

/// SET DECLARES THE POST-DECODE REPACK, because its level-2 container is a B-tree.
impl RepacksAfterDecode for SetKind {}

// AND IT DELIBERATELY DOES NOT DECLARE `ReplaysInstallsUnrecorded`: its replay arm re-files the
// block through the bucket index before installing, so it goes through `install_element` like any
// other recorded write. One exception each way, and neither was predictable from the other kind.

/// `shard.bucket_index.zsets` -- member bytes to a score-and-block pair.
///
/// THE ONE MOST LIKELY TO HAVE RESISTED, AND IT DID NOT. A zset element is a member AND a score, so
/// a composite key would have been the obvious guess -- and it is wrong: the score lives in the
/// VALUE, `(u64, BlockAddress)`, because the map is ordered by member and the score is what the
/// member maps to. So the blanket `ElementMap for BTreeMap<E, V>` covers it with no new bound, and
/// `CarriedValue for (u64, BlockAddress)` already existed, so the fold needed nothing either.
///
/// Nothing was widened to fit this kind. That matters: a trait grown to fit one kind is how a
/// generic type acquires a member that belongs to nobody.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ZSetKind;

impl RecordedKind for ZSetKind {
    type Elements = std::collections::BTreeMap<Vec<u8>, (u64, ElementEntry)>;
    const KIND: &'static str = "zset";

    /// `zset_index_serde`, for the same reason `sets` needs its own: a member is `Vec<u8>` and a
    /// JSON object key is a string. STATED rather than inherited -- the codec is a required
    /// associated function with no default precisely so a new kind cannot acquire the wrong one in
    /// silence, which is the defect a blanket impl produced for `sets`.
    fn serialize_entries<S>(map: &RecordedMap<Self>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        super::zset_index_serde::serialize(map.iter(), serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<ModelKey, Self::Elements>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        super::zset_index_serde::deserialize(deserializer)
    }

    fn resident(shard: &mut ShardState) -> &mut RecordedMap<Self> {
        &mut shard.bucket_index.zsets
    }

    /// EIGHT BIG-ENDIAN BYTES OF THE BIASED SCORE -- the one kind that answers `Some` here, and
    /// the whole reason the function exists. `element_name` no longer spells this; replay's only
    /// other source for a zset insert's score, so a value-less install on that path is a replay
    /// that cannot recover what the write did and must refuse rather than guess (see
    /// `lifecycle`'s `"zset"` replay arm).
    fn outcome_value(value: &<Self::Elements as ElementMap>::Value) -> Option<Vec<u8>> {
        Some(value.0.to_be_bytes().to_vec())
    }
}

/// A B-TREE LEVEL-2 CONTAINER, SO IT REPACKS. Declared, not inherited.
impl RepacksAfterDecode for ZSetKind {}

// AND IT DOES NOT DECLARE `ReplaysInstallsUnrecorded`: recovery's `zset` arm re-files its block
// through `upsert_bucket_index_block` before installing, so it goes through the recorded path.
// Read from the arm rather than assumed.

/// `shard.bucket_index.lists` -- a biased sequence number to a block.
///
/// THIS KIND FOLDS IN FOR UNIFORMITY, NOT FOR BYTES, and the body of its change says so in those
/// words. #2087 measured the list shape at **499.5 moves per insert**, which is n/2 by construction
/// for a left push into a vector-backed run and 45.4x a B-tree's bound. Nothing here changes that
/// and nothing here should be read as claiming it does.
///
/// WHAT IT DOES GET is the invariant every other kind now has, plus one latent defect closed by
/// construction -- see the note on `ListPush` in `execute_on_shard`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ListKind;

impl RecordedKind for ListKind {
    type Elements = std::collections::BTreeMap<i64, ElementEntry>;
    const KIND: &'static str = "list";

    /// THE PLAIN MAP, which is what this field always was -- a `#[serde(default)]` map with no
    /// `with =`. An `i64` key has a string form a JSON object can carry, which is exactly why this
    /// kind never needed the codec `sets` and `zsets` do. Stated explicitly all the same.
    fn serialize_entries<S>(map: &RecordedMap<Self>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_elements_as_map(map, serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<ModelKey, Self::Elements>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }

    fn resident(shard: &mut ShardState) -> &mut RecordedMap<Self> {
        &mut shard.bucket_index.lists
    }

    /// A list element's value is its address, which the outcome already carries whole. Nothing
    /// else to say.
    fn outcome_value(_value: &<Self::Elements as ElementMap>::Value) -> Option<Vec<u8>> {
        None
    }
}

/// A B-TREE LEVEL-2 CONTAINER, SO IT REPACKS.
impl RepacksAfterDecode for ListKind {}

// AND IT DOES NOT DECLARE `ReplaysInstallsUnrecorded`: recovery's `list` arm re-files its block too.

// =============================================================================================
// WHAT THE LEVEL-2 CONTAINERS HAVE TO SUPPLY. Three small traits, each declared beside the
// operation that needs it rather than widened onto `ElementMap` -- which is the FOLD's trait, and
// a fold only ever inserts.
// =============================================================================================

/// AN ELEMENT MAP WHOSE ELEMENT NAME CAN COME FROM A POOL.
///
/// One implementor, and that is the point of the trait rather than an argument against it: it
/// is what keeps the decode pass from naming `hashes` directly and from needing a `match` on
/// the kind. The other three kinds are keyed by BYTES, which no pool of strings can hold.
pub(super) trait PoolableElementNames {
    fn adopt_element_names(
        &mut self,
        pool: &mut std::collections::HashSet<std::sync::Arc<str>>,
    ) -> usize;
}

impl PoolableElementNames for super::hash_field_map::HashFieldMap {
    fn adopt_element_names(
        &mut self,
        pool: &mut std::collections::HashSet<std::sync::Arc<str>>,
    ) -> usize {
        self.adopt_field_names(pool)
    }
}

impl RemovableElementMap for super::hash_field_map::HashFieldMap {
    fn remove_element(&mut self, element: &super::hash_field_map::FieldName) -> bool {
        self.remove(&element[..]).is_some()
    }
    fn is_empty_map(&self) -> bool {
        self.is_empty()
    }
}

impl<E, V> RemovableElementMap for std::collections::BTreeMap<E, V>
where
    E: Ord + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
{
    fn remove_element(&mut self, element: &E) -> bool {
        self.remove(element).is_some()
    }
    fn is_empty_map(&self) -> bool {
        self.is_empty()
    }
}

impl<E, V> RepackableElementMap for std::collections::BTreeMap<E, V>
where
    E: Ord + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
{
    fn repack(&mut self) {
        super::state::repack_btree_map(self);
    }
}

impl IterableMutElementMap for super::hash_field_map::HashFieldMap {
    fn iter_values_mut(&mut self) -> impl Iterator<Item = (&super::hash_field_map::FieldName, &mut ElementEntry)> {
        self.iter_mut()
    }
}

impl<E, V> IterableMutElementMap for std::collections::BTreeMap<E, V>
where
    E: Ord + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
{
    fn iter_values_mut(&mut self) -> impl Iterator<Item = (&E, &mut V)> {
        self.iter_mut()
    }
}

impl AddressedElementMap for super::hash_field_map::HashFieldMap {
    fn visit_addresses(&self, visit: impl FnMut(&ElementEntry)) { self.values().for_each(visit); }
    fn element_address(&self, element: &super::hash_field_map::FieldName) -> Option<&ElementEntry> {
        self.get(&element[..])
    }

    fn any_element_other_than_at(&self, skip: &super::hash_field_map::FieldName, block: &ElementEntry) -> bool {
        let block = super::live_block_key(block);
        // COMPARED BY TEXT, NOT BY HANDLE. Two handles for one field name are equal strings
        // and unequal pointers, and `Arc<str>`'s `PartialEq` compares the strings -- but an
        // explicit slice comparison says so at the site rather than relying on the reader
        // knowing which of the two it is.
        self.iter().any(|(field, address)| {
            super::live_block_key(address) == block && field[..] != skip[..]
        })
    }
}

/// `sets`, `zsets` and `lists`, through the one bound that tells a level-2 value where its block is.
///
/// `CarriedValue` is already the trait that answers "which address does this value name" -- the
/// delta fold reads it to decide whether a carried element's block is still there. Reusing it is
/// what keeps a zset's `(score, block)` from needing an arm of its own here: the pair already
/// declares which half is the block.
impl<E, V> AddressedElementMap for std::collections::BTreeMap<E, V>
where
    E: Ord + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned + super::CarriedValue,
{
    fn visit_addresses(&self, mut visit: impl FnMut(&ElementEntry)) {
        self.values().for_each(|value| visit(super::CarriedValue::carried_address(value)));
    }
    fn element_address(&self, element: &E) -> Option<&ElementEntry> {
        self.get(element).map(super::CarriedValue::carried_address)
    }

    fn any_element_other_than_at(&self, skip: &E, block: &ElementEntry) -> bool {
        let block = super::live_block_key(block);
        self.iter().any(|(element, value)| {
            super::live_block_key(super::CarriedValue::carried_address(value)) == block
                && element != skip
        })
    }
}

#[cfg(test)]
#[path = "block_owner_query_tests.rs"]
mod block_owner_query_tests;

// =============================================================================================
// THE WIRE. One impl pair, generic, delegating to whatever codec the kind's field already named.
//
// THE CODEC IS A REQUIRED ASSOCIATED FUNCTION, NOT A BLANKET IMPL, and the reason is a regression
// this change introduced and its own gate caught. The first version had one blanket `Serialize`
// calling `self.entries.serialize(..)`. That is right for `hashes`, a plain `#[serde(default)]` map,
// and WRONG for `sets`, whose field carried `with = "super::set_index_serde"` exactly because a
// `BTreeMap<Vec<u8>, _>` has no JSON object-key representation. The collapse dropped that codec --
// `set_index_serde` was left referenced only from comments -- and three reload tests failed:
// `conformance_random_sequences_never_panic_and_survive_reload`,
// `container_block_element_key::a_framed_block_reads_back_after_a_reload` and
// `container_block_ordinal::a_reloaded_container_still_reads_every_element`.
//
// With the codec required per kind, a kind added later cannot inherit a wrong default. Each one
// names its encoding, the bytes are identical by construction rather than by resemblance, and no
// format stamp moves.
// =============================================================================================

/// Write a row map as the MAP OF ELEMENTS it was before the row existed.
///
/// BYTE-FOR-BYTE WHAT `HashMap<ModelKey, K::Elements>` WROTE. `serde`'s own map impl emits one
/// entry per key in the table's iteration order, which is exactly what this does -- the only
/// difference is that the value comes from a field of the row rather than being the value itself.
/// So the row is invisible on the wire, which is the claim
/// `recorded_map_invariant::the_four_kind_encodings_are_byte_for_byte_what_they_were` checks.
///
/// NO INTERMEDIATE MAP, because there is no need for one: `serialize_entry` is fed straight from
/// the iteration.
fn serialize_elements_as_map<K, S>(
    map: &RecordedMap<K>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    K: RecordedKind,
    K::Elements: serde::Serialize,
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut out = serializer.serialize_map(Some(map.len()))?;
    for (key, row) in map.rows() {
        if !row.is_vacant() {
            out.serialize_entry(key, &row.elements)?;
        }
    }
    out.end()
}

impl<K: RecordedKind> serde::Serialize for RecordedMap<K> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        K::serialize_entries(self, serializer)
    }
}

impl<'de, K: RecordedKind> serde::Deserialize<'de> for RecordedMap<K> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::from_decoded(K::deserialize_entries(deserializer)?))
    }
}

// ---------------------------------------------------------------------------------------------
#[cfg(test)]
#[path = "tests/shared_hash_fields.rs"]
mod shared_hash_fields_tests;

#[cfg(test)]
#[path = "recorded_ordinal_tests.rs"]
mod recorded_ordinal_tests;

#[cfg(test)]
#[path = "resident_timestamp_hint_tests.rs"]
mod resident_timestamp_hint_tests;

#[cfg(test)]
impl RecordedMap<HashKind> {
    fn remember_timestamp(&mut self, object_key: &str, field: &str, value: Option<(i64, i32)>) {
        self.remember_timestamp_value(object_key, field, TimestampHintValue::Parsed(value));
    }
}

// TEST FIXTURES. `#[cfg(test)]`, so none of this exists in a shipped binary, and each is named
// `_for_test` so a production use cannot be mistaken for an ordinary write.
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
impl<K: RecordedKind> RecordedMap<K> {
    pub(super) fn insert_element_for_test(
        &mut self,
        object_key: &str,
        element: <K::Elements as ElementMap>::Element,
        value: <K::Elements as ElementMap>::Value,
    ) {
        self.install_into(object_key, element, value);
    }

    /// EVERY DOOR BELOW KEEPS THE SIGNATURE IT HAD, in `K::Elements` and not in the row. The row is
    /// private to this module, so a test fixture that spoke in rows would leak it -- and more to the
    /// point, no existing arm should have to be rewritten to say the same thing. The refs a row
    /// carries are not a fixture's business: they are installed by the write path and re-filed by
    /// the load path, so replacing a row's ELEMENTS keeps whatever refs it already had -- a row
    /// whose elements were all removed still names the blocks they were in, and a stale snapshot
    /// merged back over it brings elements back without changing which blocks exist. A key with no
    /// row gets `None` refs, which is the state a decode produces too.
    pub(super) fn insert_elements_for_test(&mut self, object_key: &str, elements: K::Elements) {
        self.ordinals.invalidate(object_key);
        let refs = self.entries.get(object_key).and_then(|row| row.refs.clone());
        self.entries.insert(
            std::sync::Arc::from(object_key),
            RecordedRow { elements, refs },
        );
    }

    pub(super) fn remove_for_test(&mut self, object_key: &str) -> Option<K::Elements> {
        self.ordinals.invalidate(object_key);
        self.timestamp_hints.remove(object_key);
        self.entries.remove(object_key).map(|row| row.elements)
    }

    pub(super) fn clear_for_test(&mut self) {
        self.ordinals.clear();
        self.timestamp_hints = HashMap::new();
        self.entries.clear();
    }

    pub(super) fn elements_mut_for_test(&mut self, object_key: &str) -> Option<&mut K::Elements> {
        self.ordinals.invalidate(object_key);
        self.entries
            .get_mut(object_key)
            .map(|row| &mut row.elements)
    }

    /// TAKES THE ELEMENTS AND DROPS THE REFS, which is safe only because its partner below puts
    /// rows back with `None` refs: the pair is used to move a map aside and restore it within one
    /// test, never across a reload, so there is nothing for the refs to be wrong about.
    pub(super) fn take_for_test(&mut self) -> HashMap<ModelKey, K::Elements> {
        self.ordinals.clear();
        self.timestamp_hints = HashMap::new();
        std::mem::take(&mut self.entries)
            .into_iter()
            .map(|(key, row)| (key, row.elements))
            .collect()
    }

    pub(super) fn restore_for_test(&mut self, entries: HashMap<ModelKey, K::Elements>) {
        self.ordinals.clear();
        self.timestamp_hints = HashMap::new();
        self.entries = entries
            .into_iter()
            .map(|(key, elements)| {
                (
                    key,
                    RecordedRow {
                        elements,
                        refs: None,
                    },
                )
            })
            .collect();
    }

    /// Put refs on a row from a test, so an arm can assert the row CARRIES them rather than only
    /// that the type has a field for them.
    pub(super) fn set_refs_for_test(
        &mut self,
        object_key: &str,
        refs: crate::engine::state::BlockRefs,
    ) -> bool {
        self.set_refs(object_key, refs)
    }
}

/// WHAT AN ELEMENT WRITE INTO A LIVE OBJECT ALLOCATES, measured at the map and not at the engine.
///
/// Every install arm here reaches the resident map through
/// `entries.entry(Arc::from(object_key))`. `Arc::from(&str)` ALWAYS mallocs and copies, and
/// `entry()` takes its key BY VALUE, so on an OCCUPIED slot the fresh `Arc` is built, copied into
/// and dropped immediately: a malloc, a memcpy and a free for a key the map already holds.
/// `remove_element` in this same file does the borrowed thing and allocates nothing.
///
/// KEY LENGTH IS THE DIMENSION, because a key COPY costs its own length. While the defect is
/// present the per-write `alloc_bytes` of a long-keyed object exceeds a short-keyed one by about
/// the key-length difference; once the install borrows, per-write bytes are INDEPENDENT of key
/// length. That is a value only the fixed path can produce.
///
/// AND IT IS MEASURED AT THE MAP, NOT THROUGH `engine.execute`. Driving the public write path was
/// tried first and cannot attribute: a write there allocated ~254 KB and grew 4,687 B per write
/// when the key grew 200 B, because the key is carried about twenty-three times across the WAL
/// record, the block record and the index entries. No fix to the install alone could flatten that,
/// so the number proved nothing about this line. Varying a dimension that reaches far more than the
/// function under test is not a control.
///
/// Every element is built BEFORE the probe opens; `format!` and `Arc::from` on the FIELD allocate
/// too, and although a field name's length is equal in both arms, building them inside would put
/// the measurement's own noise inside the window.
#[cfg(test)]
mod element_write_key_allocation {
    use super::{HashKind, RecordedMap};

    const WRITES: usize = 400;
    const SHORT_KEY: usize = 8;
    const LONG_KEY: usize = 208;

    fn per_write(key_len: usize) -> (u64, f64) {
        let key = "k".repeat(key_len);
        let mut map: RecordedMap<HashKind> = RecordedMap::default();
        // The FIRST install creates the slot. Everything measured below lands on an occupied one.
        map.insert_element_for_test(
            &key,
            std::sync::Arc::from("seed"),
            crate::block_store::ElementEntry::default(),
        );
        let elements: Vec<std::sync::Arc<str>> = (0..WRITES)
            .map(|index| std::sync::Arc::from(format!("f{index:06}").as_str()))
            .collect();
        let probe = crate::alloc_probe::Probe::start();
        for element in elements {
            map.insert_element_for_test(
                &key,
                element,
                crate::block_store::ElementEntry::default(),
            );
        }
        let counts = probe.stop();
        (counts.allocs, counts.alloc_bytes as f64 / WRITES as f64)
    }

    #[test]
    #[ignore = "allocation measurement; needs --features alloc-probe"]
    fn an_element_write_into_a_live_object_does_not_copy_its_key() {
        // THE CANARY. Without `--features alloc-probe` the counting allocator is not installed,
        // every counter reads zero, and the assertion below would pass on a zero that means
        // nothing. Fail loudly on a known allocation instead.
        let canary = crate::alloc_probe::Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`, or \
             every number this test prints is a zero that means nothing"
        );
        drop(sink);

        let (short_allocs, short_per) = per_write(SHORT_KEY);
        let (long_allocs, long_per) = per_write(LONG_KEY);
        let delta = long_per - short_per;
        let key_delta = (LONG_KEY - SHORT_KEY) as f64;
        eprintln!(
            "  [element-write] {WRITES} installs into ONE existing object\n    \
             short key ({SHORT_KEY} B): {short_allocs} allocs, {short_per:.1} B/write\n    \
             long  key ({LONG_KEY} B): {long_allocs} allocs, {long_per:.1} B/write\n    \
             per-write delta {delta:.1} B against a key-length delta of {key_delta:.0} B"
        );

        assert!(
            delta < key_delta / 4.0,
            "per-write allocation grew {delta:.1} B when the key grew {key_delta:.0} B, so an \
             install into an EXISTING object is still copying the key it already has. Reach an \
             occupied slot through a borrow, the way `remove_element` does, instead of building an \
             `Arc` for `entry()` to drop"
        );
    }
}
