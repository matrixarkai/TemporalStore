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
        entries: &HashMap<String, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer;

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<String, Self::Elements>, D::Error>
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
    entries: HashMap<String, K::Elements>,
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
pub(super) fn install_element<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    component: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) {
    // COMPUTED BEFORE `value` MOVES INTO THE RECORD BELOW. The only producer of a `Some` today is
    // `ZSetKind`, whose score has nowhere else to ride now that the component does not spell it.
    let outcome_value = K::outcome_value(&value);
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
    pub(super) fn get(&self, object_key: &str) -> Option<&K::Elements> {
        self.entries.get(object_key)
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

    pub(super) fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &K::Elements> {
        self.entries.values()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&String, &K::Elements)> {
        self.entries.iter()
    }

    /// IMMUTABLE access to the whole map, for a serde adapter that has to hand the inner shape to
    /// an existing codec module.
    ///
    /// This is a READ and not an escape hatch: nothing can be written through a `&`, and the
    /// surface test's matcher is anchored on `&mut` returns for exactly that reason. `sets` and
    /// `zsets` need it because their field carried a `with =` codec that a `transparent` wrapper
    /// would have silently changed.
    pub(super) fn entries(&self) -> &HashMap<String, K::Elements> {
        &self.entries
    }
}

impl<'a, K: RecordedKind> IntoIterator for &'a RecordedMap<K> {
    type Item = (&'a String, &'a K::Elements);
    type IntoIter = std::collections::hash_map::Iter<'a, String, K::Elements>;

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
            .entry(object_key)
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
        let Some(elements) = self.entries.get_mut(&object_key) else {
            return false;
        };
        let removed = elements.remove_element(&element);
        if elements.is_empty_map() {
            self.entries.remove(&object_key);
        }
        removed
    }

    fn remove_object(&mut self, recorded: RecordedObjectRemoval<K>) -> bool {
        self.entries.remove(&recorded.object_key).is_some()
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
    pub(super) fn from_decoded(entries: HashMap<String, K::Elements>) -> Self {
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
        derived: HashMap<String, K::Elements>,
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
            .map(|(object_key, elements)| (object_key.as_str(), ElementValuesMut { elements }))
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
            .entry(object_key.to_string())
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
        entries: &HashMap<String, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(entries, serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<String, Self::Elements>, D::Error>
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
        entries: &HashMap<String, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        super::set_index_serde::serialize(entries, serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<String, Self::Elements>, D::Error>
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
        entries: &HashMap<String, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        super::zset_index_serde::serialize(entries, serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<String, Self::Elements>, D::Error>
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
        entries: &HashMap<String, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(entries, serializer)
    }

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<String, Self::Elements>, D::Error>
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

impl RemovableElementMap for super::hash_field_map::HashFieldMap {
    fn remove_element(&mut self, element: &String) -> bool {
        self.remove(element).is_some()
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
    fn iter_values_mut(&mut self) -> impl Iterator<Item = (&String, &mut ElementEntry)> {
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
// `container_page_element_key::a_framed_page_reads_back_after_a_reload` and
// `container_page_ordinal::a_reloaded_container_still_reads_every_element`.
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
            .entry(object_key.to_string())
            .or_default()
            .insert_element(element, value);
    }

    pub(super) fn insert_elements_for_test(&mut self, object_key: &str, elements: K::Elements) {
        self.entries.insert(object_key.to_string(), elements);
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

    pub(super) fn take_for_test(&mut self) -> HashMap<String, K::Elements> {
        std::mem::take(&mut self.entries)
    }

    pub(super) fn restore_for_test(&mut self, entries: HashMap<String, K::Elements>) {
        self.entries = entries;
    }
}
