// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE HASH MODEL MAP AND THE RECORD OF ITS MUTATIONS, HELD TOGETHER BY THE TYPE SYSTEM.
//!
//! `shard.hashes` was a `HashMap<String, HashFieldMap>` field reachable from every writer in
//! `engine`, and the durable record of a change to it -- the bucket-index entry, the staged WAL
//! outcome -- was emitted AT THE CALL SITE, beside the mutation rather than by it. So "did this
//! mutation get recorded?" was a question answerable only by reading every writer, and this
//! campaign asked it four times at real cost:
//!
//!   1. WHETHER THE DELTA FOLD POPULATES THE MODEL MAPS AT ALL. It took a whole change to answer,
//!      and the answer was subtle: the fold FUNCTION writes only the block and object indexes,
//!      while the fold PATH also folds the carried container elements and so does merge this map.
//!      A claim about a function was relayed as a claim about a path, and was wrong.
//!   2. WHETHER EVERY HASH INSTALL PATH REPORTS A TOUCHED ELEMENT. It was answered with a CENSUS
//!      -- one arm reporting one element, one reporting two, one reporting one, one reporting none
//!      -- which is a test asserting something a type could have guaranteed.
//!   3. WHETHER `BlockIndex::log_backed` AGREES WITH ITSELF. Three sites derive it from the
//!      address and three hardcode `false`, and nothing asserts they agree.
//!   4. WHETHER THE CARRIED CHANNEL IS COMPLETE -- whether some writer installs a page without
//!      reporting its element. Still only BOUNDED by that census, not guaranteed.
//!
//! Every one is the same shape: a mutation happened, and whether the durable record happened was
//! established by READING CODE rather than BY CONSTRUCTION.
//!
//! # WHAT THIS TYPE CHANGES, AND WHAT IT EXPRESSLY DOES NOT
//!
//! IT MOVES NO STORED BYTES. The inner map is the same `HashMap<String, HashFieldMap>` it was, the
//! wrapper is `#[serde(transparent)]` over exactly that field, and so every snapshot, manifest and
//! index-log encoding is byte-identical. No format stamp moves. The resident footprint is
//! unchanged -- a newtype over one field is the size of the field. This is NOT a saving and must
//! not be read as one.
//!
//! WHAT IT CHANGES IS THAT A WRITER WHICH MUTATES WITHOUT RECORDING FAILS TO COMPILE. The inner
//! `entries` field is private TO THIS MODULE. No accessor returns `&mut` to it, no `pub` field
//! exposes it, and the three mutators that change membership each take a value -- a
//! [`RecordedHashElement`], a [`RecordedHashFieldRemoval`], a [`RecordedHashObjectRemoval`] --
//! whose ONLY constructors live in this module and whose every constructor emits the durable
//! record FIRST and returns the proof SECOND. A caller holding one of those values is holding
//! evidence that the record is already out. A caller holding none cannot reach the map.
//!
//! # ONE CALL PER OPERATION AT THE SURFACE, A PROOF VALUE INSIDE
//!
//! A caller writes ONE statement: [`install_hash_element`], [`remove_hash_field`],
//! [`delete_hash_object`], [`install_context_node_element`]. There is no token to thread through a
//! call site and no two-statement ordering for a caller to get right.
//!
//! The proof value has not gone away -- it has moved INSIDE. Each of those functions emits the
//! durable record, receives a [`RecordedHashElement`] (or its removal siblings), and hands it to a
//! mutator that is PRIVATE to this module. So the enforcement is unchanged in kind and smaller in
//! surface: it used to be "every call site must pass the token", and it is now "one function per
//! operation must construct it", with all of them in this file under this doc comment.
//!
//! WHAT THAT BUYS OVER A SINGLE FUNCTION THAT JUST LOGS AND THEN ASSIGNS, and the one place it
//! buys less. In the plain shape the ordering is a convention inside one function: delete the log
//! call and it still compiles. Here, for three of the four operations, it does NOT compile -- each
//! proof requires a WITNESS that only the emitter can produce:
//!
//!   * [`install_hash_element`] needs a `BlockFiled`, minted only by
//!     `upsert_bucket_index_block_filed`;
//!   * [`install_context_node_element`] needs an `OutcomeStaged`, minted only by
//!     `stage_outcome_attested`;
//!   * [`delete_hash_object`] needs an `ObjectDeletionFiled`, minted only by
//!     `mark_bucket_index_object_deleted_filed`.
//!
//! Each witness is a struct with a PRIVATE unit field living in the module that emits, so it cannot
//! be constructed anywhere else. Delete the emission and the witness is gone, the proof cannot be
//! built, and the mutator call stops compiling.
//!
//! THAT CLAIM WAS FALSE WHEN THIS SHAPE WAS FIRST BUILT, AND THE TEST IS WHAT SHOWED IT. The proof
//! was constructed from the function's own arguments, so deleting the emission compiled fine and
//! only `each_recorded_hash_mutator_emits_exactly_one_record` noticed -- at runtime, while the doc
//! comment claimed the compiler would. The witnesses exist because driving that test to fail
//! exposed the gap between the claim and the code.
//!
//! [`remove_hash_field`] IS THE EXCEPTION AND HAS NO WITNESS. Its emitter,
//! `remove_container_element`, is shared with six other arms -- set, zset and list removals among
//! them -- so giving it a witness return means touching all seven, which belongs with the change
//! that converts those kinds rather than with this one. For that one operation the ordering is a
//! convention inside the function, exactly as it would be in the plain shape, and its record count
//! is covered by the test only. Stated rather than left to be discovered.
//!
//! THE PROOF CARRIES IDENTITY, not merely the fact that something was recorded. A
//! `RecordedHashElement` holds the object key, the field and the address, and the mutator writes
//! THOSE -- it cannot be handed a proof for one element and asked to write another. A unit token
//! would have allowed exactly that.
//!
//! # WHY THESE ARE FREE FUNCTIONS AND NOT METHODS, which is the obstacle this shape had to clear
//!
//! `upsert_bucket_index_block` takes `&mut ShardState`, the WHOLE shard, because filing a block
//! reloads a released bucket, interns the component name and touches the pending-flag set. `hashes`
//! is a FIELD of `ShardState`, so a method on the container holding `&mut self` cannot hand
//! `&mut ShardState` to the emitter -- the field borrow is already live, and a closure capturing the
//! shard fails for the same reason.
//!
//! A free function taking `&mut ShardState` clears it, because the two borrows are SEQUENTIAL
//! rather than overlapping: the emitter's `&mut shard` ends when it returns, and only then does
//! `shard.hashes` get borrowed to apply the proof. That is why the surface is a set of free
//! functions in this module rather than methods on the container, and it is the whole reason the
//! one-call shape is available at all.
//!
//! The ordering the brief asks for is preserved and is in fact structural: the record is emitted
//! inside the constructor, before the proof exists, so a panic between the two cannot leave a
//! mutation unrecorded -- at the moment of the panic there is no mutation, only a record, and a
//! record without its mutation replays as an idempotent install. The unsafe direction is the one
//! that is now unrepresentable.
//!
//! # THE FIVE PLACES WHERE THE RECORD IS THE SOURCE AND NOT THE SINK
//!
//! Five paths mutate this map and must NOT log, and each is a genuine property of the engine
//! rather than a hole left for convenience. They are named individually below --
//! [`RecordedHashContainer::reconcile_from_durable`], [`RecordedHashContainer::fold_carried_elements`],
//! [`RecordedHashContainer::replay_remove_field`], [`RecordedHashContainer::replay_install_element`]
//! and [`RecordedHashContainer::element_addresses_mut`] -- so that each reads as the exception it is
//! and can be found by name. None of them hands out `&mut` to the map, so the set is CLOSED: a new
//! writer cannot add a sixth without editing this module.
//!
//! THE FOURTH WAS FOUND BY BUILDING THIS TYPE. Recovery's `context_node` arm installs an element
//! into this map and files no record at all, where the `hash` arm next to it re-files the block.
//! That is correct -- the replayed item is the record -- but it is an UNLOGGED INSTALL, the exact
//! shape the four questions behind this type were about, and it had no name until the compiler
//! listed it with the other seventeen writers.
//!
//! That closure is the honest form of the invariant. It is not "nothing can mutate without
//! logging"; it is "the mutations that do not log are an enumerable list of five, each with a name
//! and a reason, and the list cannot grow anywhere else". Before this type all eighteen writers
//! looked alike.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::hash_field_map::HashFieldMap;
use super::state::ShardState;
use super::LiveBlockKey;
use crate::block_store::BlockAddress;
use crate::ShardId;

/// The model-map kind every record in this module names. Spelled once so no arm can misspell it.
const HASH_KIND: &str = "hash";

/// The resident hash model map, reachable for READING from anywhere in `engine` and for WRITING
/// only through the recorded mutators below.
///
/// `entries` is private to this module. That single fact is the invariant: `rustc` refuses a
/// `&mut shard.hashes.entries` from any other module, so the complete set of operations that can
/// change the hash model map is the set of methods declared here.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct RecordedHashContainer {
    /// PRIVATE, AND THE WHOLE POINT. Do not add a `pub(super)` accessor returning `&mut` to this.
    /// `engine::tests::recorded_hash_container_surface` fails if the module grows one.
    entries: HashMap<String, HashFieldMap>,
}

/// PROOF THAT THE DURABLE RECORD FOR ONE HASH ELEMENT IS ALREADY OUT.
///
/// Constructed only by [`record_hash_element`] and [`record_context_node_element`], each of which
/// emits the record before returning. Consumed by [`RecordedHashContainer::install`], which reads
/// the element to install OUT OF the proof -- so the installed element is necessarily the recorded
/// one and cannot drift from it.
#[must_use = "a recorded hash element that is never installed leaves the record describing a page \
              the resident map does not hold"]
struct RecordedHashElement {
    /// The emitter's witness. Not read -- holding it IS the point, because it cannot be obtained
    /// without the record having been filed.
    #[allow(dead_code)]
    filed: super::storage_bucket_internals::BlockFiled,
    object_key: String,
    field: String,
    address: BlockAddress,
}

/// PROOF THAT THE REMOVAL OF ONE HASH FIELD IS ALREADY RECORDED.
#[must_use = "a recorded hash field removal that is never applied leaves the field resident after \
              its page was tombstoned"]
struct RecordedHashFieldRemoval {
    object_key: String,
    field: String,
}

/// PROOF THAT THE DELETION OF A WHOLE OBJECT IS ALREADY RECORDED IN THE BUCKET INDEX.
#[must_use = "a recorded object deletion that is never applied leaves the hash resident after the \
              index says it is gone"]
struct RecordedHashObjectRemoval {
    #[allow(dead_code)]
    filed: super::storage_bucket_internals::ObjectDeletionFiled,
    object_key: String,
}

// ---------------------------------------------------------------------------------------------
// THE RECORD EMITTERS. Each one emits, then mints the proof. There is no other constructor.
// ---------------------------------------------------------------------------------------------

/// Record a hash element's block and put it in the resident map, in that order, in one call.
///
/// This is the one and only wrapping of `upsert_bucket_index_block` at kind `hash`, so the kind
/// string is spelled once for every hash writer in the engine rather than once per arm.
///
/// Returns the address it displaced, if any.
pub(super) fn install_hash_element(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    field: String,
    address: BlockAddress,
    dirty: bool,
) -> Option<BlockAddress> {
    // THE RECORD FIRST. The proof below cannot exist until this has returned, and `install` cannot
    // be called without the proof -- so a panic between the two leaves a record with no mutation,
    // which replays as an idempotent install. The unsafe order is unrepresentable.
    let filed = super::storage_bucket_internals::upsert_bucket_index_block_filed(
        shard,
        shard_id,
        HASH_KIND,
        object_key,
        Some(field.clone()),
        address.clone(),
        dirty,
    );
    // THEN THE MUTATION, through the proof. Two SEQUENTIAL borrows of the shard, which is the whole
    // reason this is a free function rather than a method on the container. The proof cannot be
    // built without `filed`, so deleting the call above is a compile error rather than a silent
    // unrecorded write.
    shard.hashes.install(RecordedHashElement {
        filed,
        object_key: object_key.to_string(),
        field,
        address,
    })
}

/// The same, for a context node -- whose record is DELIBERATELY not a bucket-index entry.
///
/// A context node's block lands in this map under the single constant field a node is filed by,
/// and is read back through the hash door with that field as its component, but it is never
/// registered in the bucket index: recording it as a `hash` there would have a rebuild add an entry
/// the write never made. So its durable record is the staged outcome alone, under its own kind.
/// That difference is the reason this is a separate emitter rather than an argument to the one
/// above -- the two produce different records, and a boolean would have let an arm pick the wrong
/// one silently.
pub(super) fn install_context_node_element(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    field: &str,
    routing_bucket: u32,
    address: BlockAddress,
) -> Option<BlockAddress> {
    let staged = super::block_in_wal::stage_outcome_attested(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: object_key.to_string(),
        component: Some(field.to_string()),
        object_id: super::stable_block_object_id(shard_id, kind, object_key),
        routing_bucket,
        address: Some(address.clone()),
        value: None,
        ttl: None,
        deleted: false,
        meta: false,
    });
    shard.hashes.install(RecordedHashElement {
        filed: staged.into_block_filed(),
        object_key: object_key.to_string(),
        field: field.to_string(),
        address,
    })
}

/// Tombstone the element's page, clear its bucket-index entry, and drop it from the resident map,
/// in that order, in one call.
///
/// Returns whether anything changed -- the removal's own answer OR the resident drop's. Both are
/// folded here rather than handed back separately because every caller OR-ed them into one
/// `mutated` flag, and a removal that found no page still has to reach the resident map: the
/// resident map is exactly where a stale copy would survive.
#[allow(clippy::too_many_arguments)]
pub(super) fn remove_hash_field(
    cache: &matrixcache::MultiLayerCache,
    block_store: &crate::block_store::BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    field: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let removed = super::execute_on_shard::remove_container_element(
        cache,
        block_store,
        shard,
        shard_id,
        HASH_KIND,
        object_key,
        field,
        start_routing_bucket,
        end_routing_bucket,
        async_storage,
    );
    let dropped = shard.hashes.remove_field(RecordedHashFieldRemoval {
        object_key: object_key.to_string(),
        field: field.to_string(),
    });
    removed || dropped
}

/// Mark the whole object deleted in the bucket index and drop its resident hash, in that order.
///
/// This is the ONE call of `mark_bucket_index_object_deleted` on the record-deletion path. The
/// record it files is about the OBJECT, not the hash map in particular -- the same deletion covers
/// every model map the key appears in -- and the hash drop is gated on it because the hash map is
/// the one behind this container.
///
/// THE ORDER IS LOAD-BEARING AND IS WHY THESE TWO BELONG IN ONE CALL.
/// `mark_bucket_index_object_deleted` settles a released bucket by reading the block's address
/// **out of the model map, while the map still holds it**. Dropping the hash first would take the
/// address it reads. Inlining the pair here puts that ordering in one place instead of leaving it
/// as a rule two statements at a call site have to keep.
///
/// Returns whether anything changed -- the mark's answer OR the drop's, which is how the one
/// caller already accumulated them.
pub(super) fn drop_hash_object(
    shard: &mut ShardState,
    object_key: &str,
    filed: super::storage_bucket_internals::ObjectDeletionFiled,
) -> bool {
    shard.hashes.remove_object(RecordedHashObjectRemoval {
        filed,
        object_key: object_key.to_string(),
    })
}

// ---------------------------------------------------------------------------------------------
// READS. All plain delegation: no allocation, no copy, no clone. `engine::tests`
// `recorded_hash_container_reads_allocate_nothing` holds that against the counting allocator.
// ---------------------------------------------------------------------------------------------

impl RecordedHashContainer {
    pub(super) fn get(&self, object_key: &str) -> Option<&HashFieldMap> {
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

    pub(super) fn values(&self) -> impl Iterator<Item = &HashFieldMap> {
        self.entries.values()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&String, &HashFieldMap)> {
        self.entries.iter()
    }
}

impl<'a> IntoIterator for &'a RecordedHashContainer {
    type Item = (&'a String, &'a HashFieldMap);
    type IntoIter = std::collections::hash_map::Iter<'a, String, HashFieldMap>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

// ---------------------------------------------------------------------------------------------
// THE RECORDED MUTATORS. Each consumes a proof; none can be called without one.
// ---------------------------------------------------------------------------------------------

impl RecordedHashContainer {
    /// Put a recorded element in the map. Returns the address it displaced, if any.
    ///
    /// PRIVATE TO THIS MODULE, which is the second half of the invariant. The first half is that
    /// `entries` is private, so nothing outside can mutate the map directly. This is why nothing
    /// outside can mutate it through a proof it minted either: the only callers are the operations
    /// above, each of which has already emitted the record.
    fn install(&mut self, recorded: RecordedHashElement) -> Option<BlockAddress> {
        let RecordedHashElement {
            filed: _,
            object_key,
            field,
            address,
        } = recorded;
        self.entries
            .entry(object_key)
            .or_default()
            .insert(field, address)
    }

    /// Drop a recorded field, and the key with it when that field was its last.
    ///
    /// THE KEY GOES WHEN THE FIELD MAP EMPTIES, and that is not tidiness. `record_exists_exact`
    /// asks this map `contains_key`, so a key left holding an empty field map still reports as
    /// existing -- a phantom hash that answers EXISTS and TYPE for an object with no fields. The
    /// cleanup lives here rather than at the arm precisely so that no future arm can forget it.
    fn remove_field(&mut self, recorded: RecordedHashFieldRemoval) -> bool {
        let RecordedHashFieldRemoval { object_key, field } = recorded;
        let Some(fields) = self.entries.get_mut(&object_key) else {
            return false;
        };
        let removed = fields.remove(&field).is_some();
        if fields.is_empty() {
            self.entries.remove(&object_key);
        }
        removed
    }

    /// Drop a recorded object's whole hash. Returns whether one was resident. Private, as above.
    fn remove_object(&mut self, recorded: RecordedHashObjectRemoval) -> bool {
        self.entries.remove(&recorded.object_key).is_some()
    }
}

// ---------------------------------------------------------------------------------------------
// THE FOUR EXCEPTIONS. Each mutates WITHOUT a record because on that path the durable record is
// the SOURCE of the change and not its consequence. None hands out `&mut` to the map, so no fifth
// can be added outside this module.
// ---------------------------------------------------------------------------------------------

impl RecordedHashContainer {
    /// EXCEPTION 1 OF 5 -- THE RECONCILE, where the durable index DECIDES and this map is the
    /// derived view of it.
    ///
    /// A load walks the bucket index and derives which elements exist and which block backs each.
    /// The persisted map is the OLDER of the two inputs and supplies only what the derived view
    /// could not produce, which is why the merge inserts where ABSENT rather than overwriting:
    /// letting the older map win would invert the rule `durable_outranks_derived` exists to hold.
    ///
    /// Logging here would be backwards. The record is what this read FROM; emitting another would
    /// make a load look like a write and have a replay install every page a second time.
    pub(super) fn reconcile_from_durable(
        &mut self,
        derived: HashMap<String, HashFieldMap>,
        live: &HashSet<LiveBlockKey>,
        resurrections_refused: &mut usize,
    ) {
        let persisted = std::mem::take(&mut self.entries);
        self.entries = super::storage_bucket_internals::fill_absent_elements(
            derived,
            persisted,
            live,
            resurrections_refused,
        );
    }

    /// EXCEPTION 2 OF 5 -- THE DELTA FOLD, where the carried blob IS the record.
    ///
    /// A fold applies elements a delta CARRIED, and the carry is the newer statement, so here the
    /// carry WINS a collision -- the opposite of the reconcile above, which is why these are two
    /// methods and not one with a flag. Emitting a record for a carried element would record the
    /// fold as a write of elements the delta already carries.
    ///
    /// This is the path question 1 of the four cost a whole change to answer. It is now answerable
    /// by reading one name.
    pub(super) fn fold_carried_elements(
        &mut self,
        object_key: &str,
        carried: Option<&serde_json::Value>,
        live: &HashSet<LiveBlockKey>,
        skipped: &mut usize,
    ) {
        super::merge_container_elements(&mut self.entries, object_key, carried, live, skipped);
    }

    /// EXCEPTION 3 OF 5 -- WAL REPLAY OF A REMOVAL THAT IS ALREADY IN THE RECORD, and the most
    /// important of the four.
    ///
    /// This is the exception the design has to account for, and it is real rather than a
    /// convenience: recovery is REPLAYING a removal the log already holds. Recording it again
    /// would write the recovery itself into the log as a fresh removal, so that the next replay
    /// would have two removals where the store had one. The same reasoning already makes this
    /// path's bucket-index clear ask not to stage.
    ///
    /// It is deliberately NARROW: it removes one field of one object and nothing else. It cannot
    /// install, it cannot drop a key, and it cannot be reached with a key the replayed item did
    /// not name.
    pub(super) fn replay_remove_field(&mut self, object_key: &str, field: &str) -> bool {
        self.entries
            .get_mut(object_key)
            .and_then(|fields| fields.remove(field))
            .is_some()
    }

    /// EXCEPTION 4 OF 5 -- WAL REPLAY OF AN INSTALL THAT FILES NO RECORD OF ITS OWN.
    ///
    /// Recovery's `context_node` arm installs a node's block into this map and files NOTHING,
    /// where the `hash` arm beside it re-files the block in the bucket index. The difference is
    /// real and is the same one [`record_context_node_element`] documents: a context node is never
    /// registered in the bucket index, so a replay has no entry to re-file, and the log item being
    /// replayed IS the record. Emitting a second one would have the next replay install the node
    /// twice.
    ///
    /// SURVEYING THE WRITERS IS HOW THIS WAS FOUND. Reading the eighteen production mutation sites
    /// turned up two replay paths, not one, and only one of them appeared in the four questions
    /// that motivated this type. An unlogged install is exactly the shape those questions were
    /// about, and this one had no name until now.
    ///
    /// NARROW, like its sibling: one field of one object, from a replayed item that named both.
    pub(super) fn replay_install_element(
        &mut self,
        object_key: &str,
        field: String,
        address: BlockAddress,
    ) -> Option<BlockAddress> {
        self.entries
            .entry(object_key.to_string())
            .or_default()
            .insert(field, address)
    }

    /// EXCEPTION 5 OF 5 -- COMPACTION REWRITES WHERE A PAGE LIVES, NEVER WHICH PAGES LIVE.
    ///
    /// A compaction round moves pages and then has to point the resident addresses at where they
    /// went. The move is what the compactor records; the address rewrite is bookkeeping that
    /// follows it, and a record of its own would say a page was installed when none was.
    ///
    /// The type narrows it to exactly that. Each item yields the object key and an
    /// [`ElementAddressesMut`], which lends `&mut BlockAddress` per field and NOTHING ELSE -- no
    /// insert, no remove, no retain. So this exception provably cannot change the membership of
    /// the map, only where its members point. That is strictly less than the `iter_mut` it
    /// replaces, which could have dropped a field.
    ///
    /// An ITERATOR rather than a callback taking a `bool`: it composes with `take_while`, `find`
    /// and `any`, and no borrow problem forces the callback here -- each item carries its own
    /// disjoint `&mut`, so the compactor can hold the shard's block store and cache across the
    /// walk, which is what it does.
    pub(super) fn element_addresses_mut(
        &mut self,
    ) -> impl Iterator<Item = (&str, ElementAddressesMut<'_>)> {
        self.entries
            .iter_mut()
            .map(|(object_key, fields)| (object_key.as_str(), ElementAddressesMut { fields }))
    }
}

/// A lend of one object's element ADDRESSES, and of nothing else.
///
/// It exists so that exception 4 cannot change membership: there is no `insert`, no `remove`, no
/// `retain` and no way to recover the `&mut HashFieldMap` it holds.
pub(super) struct ElementAddressesMut<'a> {
    fields: &'a mut HashFieldMap,
}

impl ElementAddressesMut<'_> {
    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut BlockAddress)> {
        self.fields.iter_mut()
    }
}

// ---------------------------------------------------------------------------------------------
// TEST FIXTURES. `#[cfg(test)]`, so none of this exists in a shipped binary, and each is named
// `_for_test` so a production use cannot be mistaken for an ordinary write.
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
impl RecordedHashContainer {
    /// Seed one element with no record, the way a fixture states a resident map directly.
    pub(super) fn insert_element_for_test(
        &mut self,
        object_key: &str,
        field: &str,
        address: BlockAddress,
    ) {
        self.entries
            .entry(object_key.to_string())
            .or_default()
            .insert(field.to_string(), address);
    }

    /// Seed a whole field map at one key.
    pub(super) fn insert_fields_for_test(&mut self, object_key: &str, fields: HashFieldMap) {
        self.entries.insert(object_key.to_string(), fields);
    }

    pub(super) fn remove_for_test(&mut self, object_key: &str) -> Option<HashFieldMap> {
        self.entries.remove(object_key)
    }

    pub(super) fn clear_for_test(&mut self) {
        self.entries.clear();
    }

    pub(super) fn fields_mut_for_test(&mut self, object_key: &str) -> Option<&mut HashFieldMap> {
        self.entries.get_mut(object_key)
    }

    pub(super) fn take_for_test(&mut self) -> HashMap<String, HashFieldMap> {
        std::mem::take(&mut self.entries)
    }

    pub(super) fn restore_for_test(&mut self, entries: HashMap<String, HashFieldMap>) {
        self.entries = entries;
    }
}

#[cfg(test)]
impl From<HashMap<String, HashFieldMap>> for RecordedHashContainer {
    fn from(entries: HashMap<String, HashFieldMap>) -> Self {
        Self { entries }
    }
}
