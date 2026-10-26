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

/// What distinguishes one model kind from another, and nothing else.
pub(super) trait RecordedKind: Sized + 'static {
    /// The elements of one object, which is the level-2 container.
    type Elements: ElementMap;

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
    fn serialize_entries<S>(
        entries: &HashMap<ModelKey, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
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
    /// THIS EXISTS BECAUSE A ZSET'S COMPONENT STOPPED SPELLING THE SCORE. Every other kind's level-2
    /// value is fully named by its component and its address -- a hash field, a set member, a list
    /// sequence -- so `None` costs them nothing. A zset's value is `(u64, BlockAddress)`, and the
    /// `u64` had been riding the component as `{biased:016x}` until that string collapsed to the
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    entries: HashMap<ModelKey, K::Elements>,
}

impl<K: RecordedKind> Default for RecordedMap<K> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE PROOFS. One set, generic over the kind, each carrying the identity of what was recorded so
// the mutator writes the element the record names and cannot be handed a proof for another.
// ---------------------------------------------------------------------------------------------

/// PROOF THAT THE DURABLE RECORD FOR ONE ELEMENT IS ALREADY OUT.
#[must_use = "a recorded element that is never installed leaves the record describing a page the \
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
              its page was tombstoned"]
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
// reloads a released bucket, interns the component name and touches the pending-flag set. A model
// map is a FIELD of `ShardState`, so a method holding `&mut self` cannot lend the whole shard to
// the emitter, and a closure capturing the shard fails for the same reason. A free function clears
// it because the two borrows are SEQUENTIAL: the emitter's `&mut shard` ends when it returns, and
// only then is the field borrowed to apply the proof.
// ---------------------------------------------------------------------------------------------

/// Record an element's block and put it in the resident map, in that order, in one call.
///
/// `component` is the element rendered the way the page index names it; `element` is the level-2
/// key. BOTH are taken rather than one derived from the other, because every caller has already
/// computed the component to derive the block ordinal and the page frame from it, and deriving it
/// again here would be a second rendering of one identity -- the shape that let a claim about a
/// hash function be relayed as a claim about a hash path.
///
/// # A CORRECTION TO THE VERSION OF THIS NOTE THAT LANDED FIRST
///
/// The paragraph that stood here said reader (1) below is "the only durable spelling of a hash
/// field, a set member, a zset member or a list position that anything READS on the replay
/// path". That was true of the code it was written against and is FALSE now, which is why it is
/// quoted rather than quietly overwritten: `apply_outcome_item`'s four container arms read the
/// element off the PAGE, live and removed, so nothing on the replay path reads the outcome's
/// copy for those kinds any more. What the earlier note got right is the conclusion -- the
/// parameter stays -- and it got it right for a reason that has since changed, which is exactly
/// the shape this tree records rather than deletes.
///
/// # THIS PARAMETER WAS PUT UP FOR REMOVAL TOO, AND IT IS NOT A RECONSTRUCTION OF ANYTHING
///
/// The reading it was proposed on was that it is "the resident map's own key spelling, so removing
/// it removes the authority". IT IS NOT THAT, and the correction is worth having even though the
/// conclusion survives: the map's key is `element`, the parameter beside it, and the two are
/// different VALUES rather than two spellings of one. For `SetKind` the caller passes
/// `Some(hex::encode(member))` here and the raw `member` bytes as `element`; the map is keyed by
/// the bytes.
///
/// WHAT IT FEEDS, AFTER THE REPLAY PATH STOPPED ASKING THE OUTCOME. Both readers are inside
/// `upsert_bucket_index_block_inner`, which splits it into `ElementComponent` and
/// `FiledComponent`:
///
///   1. `WalOutcomeItem::component`. The replay path no longer READS that field for a container
///      kind -- it reads the page -- so this reader is now the record's own copy rather than
///      something a recovery depends on. It is still written, because the field cannot be removed
///      while six timestamped kinds read it and its absence is a meaning; the argument is set out
///      in full at the field.
///   2. THE TOMBSTONE SWEEP, which is the reader that makes this parameter stay. `element.as_deref()`
///      keys `tombstone_element_block`, and that is how a re-add clears the tombstone ITS OWN element
///      left. Narrow on purpose: the predicate it replaced widened to every tombstone of the object
///      the moment the filed name went to `None`, which is the resurrection
///      `engine::tests::write_after_fold::a_re_add_after_a_removal_does_not_bring_the_removed_member_back`
///      holds.
///
/// # AND IT COULD BE RESOLVED FROM THE PAGE, WHICH IS EXACTLY WHY IT SHOULD NOT BE
///
/// The sweep runs on the WRITE path, where the element name is the caller's own input -- already
/// computed, already in hand, one `&str` away. Decoding the page being written to recover a value
/// the caller passed in would be the same rendering done twice, which is what the paragraph above
/// this one exists to prevent. The rule that retires a derivable stored field does not reach a
/// plain argument: nothing here is STORED and nothing is RECONSTRUCTED, so there is no omission to
/// make a promise about.
///
/// AND NO CALLER PASSES `None`. Every arm that reaches here -- hash, set, zset and list, across
/// `execute_on_shard`, `stream_batch_methods` and the two replay paths in `lifecycle` -- passes
/// `Some(..)`, so the `Option` is a shape shared with the filer rather than a case this function
/// has.
pub(super) fn install_element<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    component: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) where
    K::Elements: AddressedElementMap,
{
    // COMPUTED BEFORE `value` MOVES INTO THE RECORD BELOW. The only producer of a `Some` today is
    // `ZSetKind`, whose score has nowhere else to ride now that the component does not spell it.
    let outcome_value = K::outcome_value(&value);
    // WHICH PAGE THIS WRITE REPLACES, ASKED WHILE THE MAP STILL HOLDS THE OLD ANSWER.
    //
    // The filer cannot derive this. Under one entry a page an entry carries no element name, so
    // the only discriminator it has is the address -- and a rewrite lands at a NEW address, so the
    // landing key names no existing entry and the element's previous entry is never superseded.
    // See `AddressedElementMap` for the two measured losses that leaves.
    //
    // ASKED ONLY WHEN THE ENTRY IS PAGE-NAMED. While an entry names its element the filer's own
    // component-keyed supersede already takes the predecessor, so there is nothing to tell it and
    // nothing to pay for: the ungated path does not reach the scan at all.
    let replaces = if super::storage_bucket_internals::index_entry_names_a_block(K::KIND) {
        K::resident(shard).block_an_element_vacates(object_key, &element, &address)
    } else {
        None
    };
    // THE RECORD FIRST. The proof below cannot be built without the witness this returns, so
    // deleting this call is a compile error rather than a silent unrecorded write.
    let filed = super::storage_bucket_internals::upsert_bucket_index_block_filed(
        shard,
        shard_id,
        K::KIND,
        object_key,
        component,
        address,
        dirty,
        outcome_value,
        super::storage_bucket_internals::ReplacedBlock::of(replaces),
    );
    // THEN THE MUTATION, through the proof.
    K::resident(shard).install(RecordedElement::<K> {
        filed,
        object_key: object_key.to_string(),
        element,
        value,
    });
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
    component: &str,
    routing_bucket: u32,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    address: ElementEntry,
) {
    let staged = super::block_in_wal::stage_outcome_attested(crate::wal::WalOutcomeItem {
        kind: staged_kind.to_string(),
        object_key: object_key.to_string(),
        component: Some(component.to_string()),
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

/// Tombstone the element's page, clear its bucket-index entry, and drop it from the resident map.
///
/// Returns whether anything changed -- the removal's own answer OR the resident drop's. Both are
/// folded here because every caller OR-ed them into one `mutated` flag, and a removal that found no
/// page still has to reach the resident map: the resident map is exactly where a stale copy would
/// survive.
#[allow(clippy::too_many_arguments)]
pub(super) fn remove_element<K: RecordedKind>(
    cache: &matrixcache::MultiLayerCache,
    block_store: &crate::block_store::BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    component: &str,
    element: <K::Elements as ElementMap>::Element,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool
where
    K::Elements: RemovableElementMap,
{
    let removed = super::execute_on_shard::remove_container_element(
        cache,
        block_store,
        shard,
        shard_id,
        K::KIND,
        object_key,
        component,
        start_routing_bucket,
        end_routing_bucket,
        async_storage,
    );
    let dropped = K::resident(shard).remove_element(RecordedElementRemoval::<K> {
        object_key: object_key.to_string(),
        element,
    });
    // AND WHEN THAT WAS THE LAST ELEMENT, THE INDEX HAS TO LET GO TOO.
    //
    // EXISTENCE HAS TWO SOURCES AND EMPTYING ONE DOES NOT EMPTY THE KEY. `record_exists_exact` ORs
    // the resident map's answer together with the bucket index's, so an object is gone only once
    // BOTH are. The resident removal above is unconditional -- which is why membership is right
    // under either projection -- but under one entry a page the removal deliberately keeps the live
    // page entry, on the stated grounds that the page still holds the object's other members.
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
        // object has a page with members on it, which is false once the last member goes. The
        // tombstone records that a NAMED element was removed, which stays true and is what makes
        // the removal win a fold by append position -- taking it along would undo the removal it
        // records, and `container_tombstone_entry::a_re_add_clears_the_tombstone_entry_so_churn_on_
        // one_element_does_not_accumulate` is the arm that holds the difference at one live and one
        // tombstone per cycle.
        super::storage_bucket_internals::drop_live_object_entries(shard, K::KIND, object_key);
    }
    removed || dropped
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
    /// Move this object's entry onto the shared key handle the page index owns, returning whether
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
    /// Call it AFTER the page is filed. Before that the page index holds no handle to borrow, the
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
                true
            }
            None => false,
        }
    }

    pub(super) fn get(&self, object_key: &str) -> Option<&K::Elements> {
        self.entries.get(object_key)
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
            .map(|elements| elements.adopt_element_names(pool))
            .sum()
    }

    pub(super) fn contains_key(&self, object_key: &str) -> bool {
        self.entries.contains_key(object_key)
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &ModelKey> {
        self.entries.keys()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &K::Elements> {
        self.entries.values()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&ModelKey, &K::Elements)> {
        self.entries.iter()
    }

    /// IMMUTABLE access to the whole map, for a serde adapter that has to hand the inner shape to
    /// an existing codec module.
    ///
    /// This is a READ and not an escape hatch: nothing can be written through a `&`, and the
    /// surface test's matcher is anchored on `&mut` returns for exactly that reason. `sets` and
    /// `zsets` need it because their field carried a `with =` codec that a `transparent` wrapper
    /// would have silently changed.
    pub(super) fn entries(&self) -> &HashMap<ModelKey, K::Elements> {
        &self.entries
    }

    /// THE PAGE THIS WRITE VACATES, or nothing when it vacates none.
    ///
    /// `Some` only when all three hold:
    ///
    ///   * the map already holds `element`, so this is a REWRITE and not a first write;
    ///   * the address it holds names a DIFFERENT page from the one the write is landing on -- a
    ///     rewrite that lands back on its own page is superseded through the landing key and needs
    ///     nothing extra;
    ///   * and NO OTHER element of the same object is still on the page being left, because an
    ///     entry under the gate stands for the PAGE and a page with a sibling still on it is still
    ///     live.
    ///
    /// ASKED BEFORE THE MUTATION, which is the only reason it can be asked at all: the caller files
    /// the record first, so the map still holds the pre-write address here.
    ///
    /// THE SCAN IS THE SAME ORDER AS WHAT THE FILER ALREADY PAYS. The third test is a
    /// short-circuiting `any` over ONE OBJECT's elements, where `upsert_bucket_index_block_inner`
    /// already walks the whole BUCKET's pages twice per write -- once to decide whether the object
    /// still has a page there and once for the tombstone probe. One object's elements are a subset
    /// of its bucket's pages, so this does not change the order of a container write.
    pub(super) fn block_an_element_vacates(
        &self,
        object_key: &str,
        element: &<K::Elements as ElementMap>::Element,
        landing: &ElementEntry,
    ) -> Option<ElementEntry>
    where
        K::Elements: AddressedElementMap,
    {
        let elements = self.entries.get(object_key)?;
        let previous = elements.element_address(element)?;
        if super::live_block_key(previous) == super::live_block_key(landing) {
            return None;
        }
        if elements.any_element_other_than_at(element, previous) {
            return None;
        }
        Some(previous.clone())
    }
}

impl<'a, K: RecordedKind> IntoIterator for &'a RecordedMap<K> {
    type Item = (&'a ModelKey, &'a K::Elements);
    type IntoIter = std::collections::hash_map::Iter<'a, ModelKey, K::Elements>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

// ---------------------------------------------------------------------------------------------
// THE RECORDED MUTATORS. PRIVATE, which is the second half of the invariant: `entries` being
// private stops a direct write, and these being private stop a write through a proof someone got
// hold of. The only callers are the operations above, each of which has already recorded.
// ---------------------------------------------------------------------------------------------

impl<K: RecordedKind> RecordedMap<K> {
    fn install(&mut self, recorded: RecordedElement<K>) {
        let RecordedElement {
            filed: _,
            object_key,
            element,
            value,
        } = recorded;
        self.entries
            .entry(std::sync::Arc::from(&object_key[..]))
            .or_default()
            .insert_element(element, value);
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
        let Some(elements) = self.entries.get_mut(&object_key[..]) else {
            return false;
        };
        let removed = elements.remove_element(&element);
        if elements.is_empty_map() {
            self.entries.remove(&object_key[..]);
        }
        removed
    }

    fn remove_object(&mut self, recorded: RecordedObjectRemoval<K>) -> bool {
        self.entries.remove(&recorded.object_key[..]).is_some()
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
    pub(super) fn from_decoded(entries: HashMap<ModelKey, K::Elements>) -> Self {
        Self { entries }
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
        // `fill_absent_elements` drops a durable element whose page the settled index does not
        // hold, and it asks the VALUE for that address -- so the live-page filter needs this too.
        <K::Elements as ElementMap>::Value: super::CarriedValue,
    {
        let persisted = std::mem::take(&mut self.entries);
        self.entries = super::storage_bucket_internals::fill_absent_elements(
            derived,
            persisted,
            live,
            resurrections_refused,
        );
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
        super::merge_container_elements(&mut self.entries, object_key, carried, live, skipped);
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
        self.entries
            .get_mut(object_key)
            .map(|elements| elements.remove_element(element))
            .unwrap_or(false)
    }

    /// SHARED EXCEPTION 5 -- COMPACTION REWRITES WHERE A PAGE LIVES, NEVER WHICH PAGES LIVE.
    ///
    /// The move is what the compactor records; the address rewrite is bookkeeping that follows it,
    /// and a record of its own would say a page was installed when none was.
    ///
    /// Narrowed by the lend: each item yields the object key and an [`ElementValuesMut`], which
    /// lends `&mut` to the VALUES and nothing else -- no insert, no remove, no retain -- so this
    /// provably cannot change the membership of the map, only where its members point. That is
    /// strictly less than the `iter_mut` it replaces.
    pub(super) fn element_values_mut(
        &mut self,
    ) -> impl Iterator<Item = (&str, ElementValuesMut<'_, K>)> {
        self.entries
            .iter_mut()
            .map(|(object_key, elements)| (&**object_key, ElementValuesMut { elements }))
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
        self.entries
            .entry(std::sync::Arc::from(object_key))
            .or_default()
            .insert_element(element, value);
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
        for elements in self.entries.values_mut() {
            elements.repack();
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

/// WHICH PAGE AN ELEMENT IS MOVING OFF, read from the map that is keyed BY THE ELEMENT.
///
/// # WHY THE FILER CANNOT WORK THIS OUT FOR ITSELF
///
/// Under one entry a page an index entry carries NO element name, so the entry a rewrite
/// supersedes cannot be picked out by naming the element. Keying the supersede on the address the
/// write LANDS AT finds the entry over the page it lands on -- which is right for an element
/// appended to a page that already has an entry, and finds NOTHING AT ALL for an element rewritten
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
/// An entry under the gate stands for a PAGE, not for an element. Retiring the entry over the page
/// an element vacates is WRONG whenever a SIBLING element of the same object is still on that page:
/// the page is still live, and its entry is what keeps the siblings reachable. So the question has
/// two halves, and [`AddressedElementMap::any_element_other_than_at`] is the second.
pub(super) trait AddressedElementMap: ElementMap {
    /// The address this map currently holds for `element`, if it holds the element at all.
    fn element_address(&self, element: &Self::Element) -> Option<&ElementEntry>;

    /// Whether any element OTHER THAN `skip` currently sits on the page `page` names.
    ///
    /// Compared by PAGE KEY -- slab, offset and length -- and not by the whole address, because
    /// that triple is what an index entry's identity is under the gate.
    fn any_element_other_than_at(&self, skip: &Self::Element, page: &ElementEntry) -> bool;
}

// =============================================================================================
// THE KINDS. Each is a zero-sized marker plus the three or four facts that distinguish it, and the
// per-kind exception markers it declares. This is the whole per-kind surface: everything else above
// is written once.
// =============================================================================================

/// `shard.hashes` -- field name to block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HashKind;

impl RecordedKind for HashKind {
    type Elements = super::hash_field_map::HashFieldMap;
    const KIND: &'static str = "hash";

    /// THE PLAIN MAP, which is what this field always was: a `#[serde(default)]` map of
    /// `String` to `HashFieldMap`, and `HashFieldMap` itself round-trips through the same MAP shape
    /// the bare `HashMap` used. So the bytes are unchanged by construction.
    fn serialize_entries<S>(
        entries: &HashMap<ModelKey, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(entries, serializer)
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
        &mut shard.hashes
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

/// `shard.sets` -- member bytes to block.
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
    fn serialize_entries<S>(
        entries: &HashMap<ModelKey, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        super::set_index_serde::serialize(entries, serializer)
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
        &mut shard.sets
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

/// `shard.zsets` -- member bytes to a score-and-block pair.
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
    fn serialize_entries<S>(
        entries: &HashMap<ModelKey, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        super::zset_index_serde::serialize(entries, serializer)
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
        &mut shard.zsets
    }

    /// EIGHT BIG-ENDIAN BYTES OF THE BIASED SCORE -- the one kind that answers `Some` here, and
    /// the whole reason the function exists. `component` no longer spells this; replay's only
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

/// `shard.lists` -- a biased sequence number to a block.
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
    fn serialize_entries<S>(
        entries: &HashMap<ModelKey, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(entries, serializer)
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
        &mut shard.lists
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
    fn element_address(&self, element: &super::hash_field_map::FieldName) -> Option<&ElementEntry> {
        self.get(&element[..])
    }

    fn any_element_other_than_at(&self, skip: &super::hash_field_map::FieldName, page: &ElementEntry) -> bool {
        let page = super::live_block_key(page);
        // COMPARED BY TEXT, NOT BY HANDLE. Two handles for one field name are equal strings
        // and unequal pointers, and `Arc<str>`'s `PartialEq` compares the strings -- but an
        // explicit slice comparison says so at the site rather than relying on the reader
        // knowing which of the two it is.
        self.iter().any(|(field, address)| {
            field[..] != skip[..] && super::live_block_key(address) == page
        })
    }
}

/// `sets`, `zsets` and `lists`, through the one bound that tells a level-2 value where its page is.
///
/// `CarriedValue` is already the trait that answers "which address does this value name" -- the
/// delta fold reads it to decide whether a carried element's block is still there. Reusing it is
/// what keeps a zset's `(score, page)` from needing an arm of its own here: the pair already
/// declares which half is the page.
impl<E, V> AddressedElementMap for std::collections::BTreeMap<E, V>
where
    E: Ord + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned + super::CarriedValue,
{
    fn element_address(&self, element: &E) -> Option<&ElementEntry> {
        self.get(element).map(super::CarriedValue::carried_address)
    }

    fn any_element_other_than_at(&self, skip: &E, page: &ElementEntry) -> bool {
        let page = super::live_block_key(page);
        self.iter().any(|(element, value)| {
            element != skip
                && super::live_block_key(super::CarriedValue::carried_address(value)) == page
        })
    }
}

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

impl<K: RecordedKind> serde::Serialize for RecordedMap<K> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        K::serialize_entries(&self.entries, serializer)
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
        self.entries
            .entry(std::sync::Arc::from(object_key))
            .or_default()
            .insert_element(element, value);
    }

    pub(super) fn insert_elements_for_test(&mut self, object_key: &str, elements: K::Elements) {
        self.entries.insert(std::sync::Arc::from(object_key), elements);
    }

    pub(super) fn remove_for_test(&mut self, object_key: &str) -> Option<K::Elements> {
        self.entries.remove(object_key)
    }

    pub(super) fn clear_for_test(&mut self) {
        self.entries.clear();
    }

    pub(super) fn elements_mut_for_test(&mut self, object_key: &str) -> Option<&mut K::Elements> {
        self.entries.get_mut(object_key)
    }

    pub(super) fn take_for_test(&mut self) -> HashMap<ModelKey, K::Elements> {
        std::mem::take(&mut self.entries)
    }

    pub(super) fn restore_for_test(&mut self, entries: HashMap<ModelKey, K::Elements>) {
        self.entries = entries;
    }
}
