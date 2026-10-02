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
//! # WHY A TOKEN AND NOT A METHOD THAT LOGS
//!
//! The obvious shape -- `shard.hashes.install_and_log(..)` -- does not compile here, and the reason
//! is structural rather than incidental. The record emitter `upsert_bucket_index_block` takes
//! `&mut ShardState`, the WHOLE shard, because filing a block reloads a released bucket, interns
//! the component name and touches the pending-flag set. `hashes` is a FIELD of `ShardState`. A
//! method on the field holding `&mut self` cannot hand `&mut ShardState` to the emitter: the field
//! borrow is already live. Taking the emitter as a closure fails for the same reason -- the closure
//! would have to capture the shard this method is borrowing out of.
//!
//! So the emission and the mutation are two statements, and what joins them is a value that cannot
//! exist unless the first one ran. That is strictly stronger than a method that logs, because it
//! also leaves the record EXACTLY where each call site emits it today -- nothing is reordered, so
//! no arm's staging order changes -- while still making the mutation unreachable without it.
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
pub(super) struct RecordedHashElement {
    object_key: String,
    field: String,
    address: BlockAddress,
}

/// PROOF THAT THE REMOVAL OF ONE HASH FIELD IS ALREADY RECORDED.
#[must_use = "a recorded hash field removal that is never applied leaves the field resident after \
              its page was tombstoned"]
pub(super) struct RecordedHashFieldRemoval {
    object_key: String,
    field: String,
}

/// PROOF THAT THE DELETION OF A WHOLE OBJECT IS ALREADY RECORDED IN THE BUCKET INDEX.
#[must_use = "a recorded object deletion that is never applied leaves the hash resident after the \
              index says it is gone"]
pub(super) struct RecordedHashObjectRemoval {
    object_key: String,
}

// ---------------------------------------------------------------------------------------------
// THE RECORD EMITTERS. Each one emits, then mints the proof. There is no other constructor.
// ---------------------------------------------------------------------------------------------

/// File a hash element's block in the bucket index -- which stages its WAL outcome -- and return
/// the proof needed to put it in the resident map.
///
/// This is the one and only wrapping of `upsert_bucket_index_block` at kind `hash`, so the kind
/// string is spelled once for every hash writer in the engine rather than once per arm.
pub(super) fn record_hash_element(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    field: String,
    address: BlockAddress,
    dirty: bool,
) -> RecordedHashElement {
    super::storage_bucket_internals::upsert_bucket_index_block(
        shard,
        shard_id,
        HASH_KIND,
        object_key,
        Some(field.clone()),
        address.clone(),
        dirty,
    );
    RecordedHashElement {
        object_key: object_key.to_string(),
        field,
        address,
    }
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
pub(super) fn record_context_node_element(
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    field: &str,
    routing_bucket: u32,
    address: BlockAddress,
) -> RecordedHashElement {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
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
    RecordedHashElement {
        object_key: object_key.to_string(),
        field: field.to_string(),
        address,
    }
}

/// Tombstone the element's page and clear its bucket-index entry, then return the proof needed to
/// drop it from the resident map.
///
/// The returned flag is what the removal itself reported -- whether anything was there to remove --
/// and is handed back rather than folded into the proof because the caller's `mutated` accounting
/// needs it and the proof must be usable either way: a removal that found nothing still has to
/// reach the resident map, because the resident map is exactly where a stale copy would survive.
#[allow(clippy::too_many_arguments)]
pub(super) fn record_hash_field_removal(
    cache: &matrixcache::MultiLayerCache,
    block_store: &crate::block_store::BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    field: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> (bool, RecordedHashFieldRemoval) {
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
    (
        removed,
        RecordedHashFieldRemoval {
            object_key: object_key.to_string(),
            field: field.to_string(),
        },
    )
}

/// Mark the whole object deleted in the bucket index and return the proof needed to drop its hash.
///
/// This is the ONE call of `mark_bucket_index_object_deleted` on the record-deletion path. The
/// record it files is about the OBJECT, not about the hash map in particular -- the same deletion
/// covers every model map the key appears in -- and the hash proof is minted from it because the
/// hash map is the one whose drop is now gated. The flag it returns is the one that call returned
/// before, so the caller's accounting is unchanged.
pub(super) fn record_hash_object_removal(
    shard: &mut ShardState,
    object_key: &str,
) -> (bool, RecordedHashObjectRemoval) {
    let marked = super::mark_bucket_index_object_deleted(shard, object_key);
    (
        marked,
        RecordedHashObjectRemoval {
            object_key: object_key.to_string(),
        },
    )
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
    pub(super) fn install(&mut self, recorded: RecordedHashElement) -> Option<BlockAddress> {
        let RecordedHashElement {
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
    pub(super) fn remove_field(&mut self, recorded: RecordedHashFieldRemoval) -> bool {
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

    /// Drop a recorded object's whole hash. Returns whether one was resident.
    pub(super) fn remove_object(&mut self, recorded: RecordedHashObjectRemoval) -> bool {
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
