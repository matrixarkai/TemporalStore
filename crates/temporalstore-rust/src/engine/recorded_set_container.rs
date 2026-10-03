// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SET MODEL MAP AND THE RECORD OF ITS MUTATIONS, HELD TOGETHER BY THE TYPE SYSTEM.
//!
//! The second of the four container kinds to get this treatment, after `hashes`. The shape is the
//! one #2086 settled: one call per operation, the proof value and the mutators PRIVATE to this
//! module, and a witness from each emitter so that deleting an emission fails to COMPILE rather
//! than at run time.
//!
//! # WHY, AND WHY THE ANSWER IS NOT INHERITED FROM `hashes`
//!
//! `shard.sets` was a plain field reachable from every writer in `engine`, and the durable record
//! of a change to it -- the bucket-index entry, the staged WAL outcome -- was emitted AT THE CALL
//! SITE. So "did this mutation get recorded?" was answerable only by reading every writer.
//!
//! The ENUMERATION of those writers is what a container needs, and for `sets` it was done from the
//! engine's own source rather than from a grep, twice over:
//!
//!   * the `Command` enum names three Set-family variants -- `SetAdd`, `SetMembers`, `SetRemove`.
//!     A command absent from the enum cannot be sent.
//!   * `execute_on_shard`'s match has three arms, one per variant. An arm absent from the match
//!     falls to the catch-all and performs no write at all.
//!
//! Of those three, exactly ONE installs a member: `SetAdd`. It reports a
//! `TouchedContainerElement::Set`, there is exactly one producer of that variant, and the WAL
//! replay arm re-files its block rather than installing silently. **There is no unrecorded install
//! path for `sets`** -- no equivalent of `write_context_node`, which is the writer hash's own
//! enumeration turned up installing a hash element with no record at all. So this kind is cleaner
//! going in than hash was, and that is a fact about `sets` rather than an assumption carried over.
//!
//! # WHAT THIS CHANGES, AND WHAT IT EXPRESSLY DOES NOT
//!
//! IT MOVES NO STORED BYTES. The inner map is the same `HashMap<String, BTreeMap<Vec<u8>,
//! BlockAddress>>` it was, and the wire is produced by the SAME `set_index_serde` functions the
//! field already used -- see the `Serialize`/`Deserialize` impls below, which delegate to them
//! rather than reimplementing them. No format stamp moves. A newtype over one field is the size of
//! the field, so the resident footprint is unchanged. This is NOT a saving.
//!
//! WHAT CHANGES IS THAT A WRITER WHICH MUTATES WITHOUT RECORDING FAILS TO COMPILE.
//!
//! # THE WIRE, AND WHY IT IS HAND-WRITTEN HERE WHERE `hashes` COULD USE `transparent`
//!
//! `hashes` was a plain `#[serde(default)]` field, so its container is `#[serde(transparent)]` over
//! the inner map and the derived impls forward verbatim. `sets` is not: its field carried
//! `#[serde(default, with = "super::set_index_serde")]`, because a `BTreeMap<Vec<u8>, _>` does not
//! round-trip through a JSON object key. A `transparent` wrapper would have silently dropped that
//! and changed the wire.
//!
//! So the impls below CALL `set_index_serde::serialize` and `::deserialize` on the inner map. The
//! encoding is therefore byte-identical by construction rather than by resemblance, and the field
//! drops its `with =` because the container now carries it.
//!
//! A SECOND THING FALLS OUT OF READING THAT MODULE, and it is worth recording because it makes an
//! instrument note per-kind: `set_index_serde::serialize` collects into a `BTreeMap`, so the OUTER
//! key order of this field's encoding is sorted and deterministic. For `hashes` it is not -- a
//! `HashMap`'s iteration order is randomised per instance, so two encodings of identical content
//! differ by key order alone and a byte digest compares orders rather than bytes. For `sets` a
//! digest would actually work. The test still uses round-trip equivalence, for uniformity with the
//! other kinds and because it also exercises the decode direction.
//!
//! # THE FIVE PLACES WHERE THE RECORD IS THE SOURCE AND NOT THE SINK
//!
//! FIVE, LIKE HASH, BUT NOT THE SAME FIVE -- which is the point of enumerating per kind instead of
//! inheriting. Hash needed a `replay_install_element` because recovery's `context_node` arm
//! installs a hash element and files nothing; the `set` replay arm DOES re-file its block, so it is
//! not an exception at all. In its place this kind has a POST-DECODE REPACK that hash does not
//! have, because `HashFieldMap` is a sorted vector with no half-empty B-tree nodes to pack. One
//! exception each way, and neither was predictable from the other kind.
//!
//! The five are [`RecordedSetContainer::reconcile_from_durable`],
//! [`RecordedSetContainer::fold_carried_elements`],
//! [`RecordedSetContainer::replay_remove_member`],
//! [`RecordedSetContainer::repack_decoded`] and
//! [`RecordedSetContainer::member_addresses_mut`]. None hands out `&mut` to the map, so the set is
//! CLOSED: a sixth cannot be added outside this module.
//!
//! THE REPACK WAS FOUND BY THE COMPILER, NOT BY THE SURVEY. The survey read `.sets` accesses;
//! `repack_decoded_btrees` reaches the field by destructuring the whole `ShardState`, so it had no
//! `.sets` to find. Eleven writer sites were predicted and the compiler named twelve.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::state::ShardState;
use super::storage_bucket_internals::{BlockFiled, ObjectDeletionFiled};
use super::LiveBlockKey;
use crate::block_store::BlockAddress;
use crate::ShardId;

/// The model-map kind every record in this module names. Spelled once so no arm can misspell it.
const SET_KIND: &str = "set";

/// One set's members: the member bytes, each mapped to the block that holds it.
pub(super) type SetMemberMap = BTreeMap<Vec<u8>, BlockAddress>;

/// The resident set model map, readable from anywhere in `engine` and writable only through the
/// recorded operations below.
///
/// `entries` is private to this module. That single fact is the invariant: `rustc` refuses a
/// `&mut shard.sets.entries` from any other module, so the complete set of operations that can
/// change the set model map is the set of methods declared here.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct RecordedSetContainer {
    /// PRIVATE, AND THE WHOLE POINT. Do not add an accessor returning `&mut` to this.
    /// `engine::tests::recorded_set_container_invariant` fails if the module grows one.
    entries: HashMap<String, SetMemberMap>,
}

// ---------------------------------------------------------------------------------------------
// THE WIRE. Delegated to the module the field already used, so the bytes are identical by
// construction. See the module doc for why this is not `#[serde(transparent)]`.
// ---------------------------------------------------------------------------------------------

impl Serialize for RecordedSetContainer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        super::set_index_serde::serialize(&self.entries, serializer)
    }
}

impl<'de> Deserialize<'de> for RecordedSetContainer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self {
            entries: super::set_index_serde::deserialize(deserializer)?,
        })
    }
}

/// PROOF THAT THE DURABLE RECORD FOR ONE SET MEMBER IS ALREADY OUT.
///
/// Holds the emitter's witness plus the identity of what was recorded, so the mutator writes the
/// member the record names and cannot be handed a proof for one member and asked to write another.
#[must_use = "a recorded set member that is never installed leaves the record describing a page the \
              resident map does not hold"]
struct RecordedSetMember {
    /// The emitter's witness. Not read -- holding it IS the point, because it cannot be obtained
    /// without the record having been filed.
    #[allow(dead_code)]
    filed: BlockFiled,
    object_key: String,
    member: Vec<u8>,
    address: BlockAddress,
}

/// PROOF THAT THE REMOVAL OF ONE SET MEMBER IS ALREADY RECORDED.
///
/// NO WITNESS, and that is stated rather than hidden. Its emitter is
/// `execute_on_shard::remove_container_element`, which is shared with six other arms -- the hash,
/// zset and list removals among them -- so giving it a witness return means changing all seven.
/// That blast radius was measured for `remove_hash_field` in #2086 and the same reasoning holds
/// here: it belongs with a change that owns all seven arms, not with one kind's container. For this
/// one operation the ordering is a convention inside [`remove_set_member`], and its record count is
/// covered by the test only.
#[must_use = "a recorded set member removal that is never applied leaves the member resident after \
              its page was tombstoned"]
struct RecordedSetMemberRemoval {
    object_key: String,
    member: Vec<u8>,
}

/// PROOF THAT THE DELETION OF A WHOLE OBJECT IS ALREADY RECORDED IN THE BUCKET INDEX.
#[must_use = "a recorded object deletion that is never applied leaves the set resident after the \
              index says it is gone"]
struct RecordedSetObjectRemoval {
    #[allow(dead_code)]
    filed: ObjectDeletionFiled,
    object_key: String,
}

// ---------------------------------------------------------------------------------------------
// THE OPERATIONS. One call each: emit the durable record, then mutate through the proof.
//
// FREE FUNCTIONS, NOT METHODS, and that is forced rather than stylistic.
// `upsert_bucket_index_block` takes `&mut ShardState` -- the WHOLE shard -- because filing a block
// reloads a released bucket, interns the component name and touches the pending-flag set. `sets` is
// a FIELD of `ShardState`, so a method holding `&mut self` cannot lend the whole shard to the
// emitter, and a closure capturing the shard fails for the same reason. A free function clears it
// because the two borrows are SEQUENTIAL: the emitter's `&mut shard` ends when it returns, and only
// then is `shard.sets` borrowed to apply the proof.
// ---------------------------------------------------------------------------------------------

/// Record a set member's block and put it in the resident map, in that order, in one call.
///
/// This is the one and only wrapping of `upsert_bucket_index_block` at kind `set`, so the kind
/// string is spelled once for every set writer in the engine rather than once per arm.
///
/// `member_component` is the member rendered the way the page index names it -- `hex::encode` of the
/// bytes -- and `member` is the bytes themselves. BOTH are taken rather than one derived from the
/// other, because the caller has already computed the component to derive the block ordinal and the
/// page frame from it, and deriving it a second time here would be a second rendering of one
/// identity. That is the shape that let a claim about a hash function be relayed as a claim about a
/// hash path.
///
/// Returns the address it displaced, if any.
pub(super) fn install_set_member(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    member_component: String,
    member: Vec<u8>,
    address: BlockAddress,
    dirty: bool,
) -> Option<BlockAddress> {
    // THE RECORD FIRST. The proof below cannot be built without the witness this returns, so
    // deleting this call is a compile error rather than a silent unrecorded write.
    let filed = super::storage_bucket_internals::upsert_bucket_index_block_filed(
        shard,
        shard_id,
        SET_KIND,
        object_key,
        Some(member_component),
        address.clone(),
        dirty,
    );
    // THEN THE MUTATION, through the proof.
    shard.sets.install(RecordedSetMember {
        filed,
        object_key: object_key.to_string(),
        member,
        address,
    })
}

/// Tombstone the member's page, clear its bucket-index entry, and drop it from the resident map.
///
/// Returns whether anything changed -- the removal's own answer OR the resident drop's. Both are
/// folded here rather than handed back separately because the caller OR-ed them into one `mutated`
/// flag, and a removal that found no page still has to reach the resident map: the resident map is
/// exactly where a stale copy would survive.
///
/// NO WITNESS. See [`RecordedSetMemberRemoval`] for why, and for the blast radius that would be
/// involved in giving it one.
#[allow(clippy::too_many_arguments)]
pub(super) fn remove_set_member(
    cache: &matrixcache::MultiLayerCache,
    block_store: &crate::block_store::BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    member_component: &str,
    member: &[u8],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let removed = super::execute_on_shard::remove_container_element(
        cache,
        block_store,
        shard,
        shard_id,
        SET_KIND,
        object_key,
        member_component,
        start_routing_bucket,
        end_routing_bucket,
        async_storage,
    );
    let dropped = shard.sets.remove_member(RecordedSetMemberRemoval {
        object_key: object_key.to_string(),
        member: member.to_vec(),
    });
    removed || dropped
}

/// Mark the whole object deleted in the bucket index and drop its resident set, in that order.
///
/// THE ORDER IS LOAD-BEARING AND IS WHY THESE TWO BELONG IN ONE CALL.
/// `mark_bucket_index_object_deleted` settles a released bucket by reading the block's address
/// **out of the model map, while the map still holds it**. Dropping the set first would take the
/// address it reads. Putting the pair in one function puts that ordering in one place instead of
/// leaving it as a rule two statements at a call site have to keep.
///
/// Returns whether anything changed -- the mark's answer OR the drop's.
pub(super) fn drop_set_object(
    shard: &mut ShardState,
    object_key: &str,
    filed: ObjectDeletionFiled,
) -> bool {
    shard.sets.remove_object(RecordedSetObjectRemoval {
        filed,
        object_key: object_key.to_string(),
    })
}

// ---------------------------------------------------------------------------------------------
// READS. Plain delegation: no allocation, no copy, no clone.
// ---------------------------------------------------------------------------------------------

impl RecordedSetContainer {
    pub(super) fn get(&self, object_key: &str) -> Option<&SetMemberMap> {
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

    pub(super) fn values(&self) -> impl Iterator<Item = &SetMemberMap> {
        self.entries.values()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&String, &SetMemberMap)> {
        self.entries.iter()
    }
}

impl<'a> IntoIterator for &'a RecordedSetContainer {
    type Item = (&'a String, &'a SetMemberMap);
    type IntoIter = std::collections::hash_map::Iter<'a, String, SetMemberMap>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

// ---------------------------------------------------------------------------------------------
// THE RECORDED MUTATORS. PRIVATE, which is the second half of the invariant: the first half is that
// `entries` is private, and this is why nothing outside can mutate the map through a proof it got
// hold of either. The only callers are the operations above, each of which has already recorded.
// ---------------------------------------------------------------------------------------------

impl RecordedSetContainer {
    fn install(&mut self, recorded: RecordedSetMember) -> Option<BlockAddress> {
        let RecordedSetMember {
            filed: _,
            object_key,
            member,
            address,
        } = recorded;
        self.entries
            .entry(object_key)
            .or_default()
            .insert(member, address)
    }

    /// Drop a recorded member, and the key with it when that member was its last.
    ///
    /// THE KEY GOES WHEN THE MEMBER MAP EMPTIES, and that is not tidiness. `record_exists_exact`
    /// ORs `shard.sets.contains_key(key)` in beside its bucket-index answer, so an empty member map
    /// under a live key is a key that EXISTS answers 1 for while every listing answers empty for --
    /// and `CommonExpire` gates on the same function, so it accepted a deadline for such a key and
    /// `ttl_ms` then reported that deadline instead of the -2 of a missing key. The cleanup lives
    /// here rather than at the arm precisely so that no future arm can forget it.
    fn remove_member(&mut self, recorded: RecordedSetMemberRemoval) -> bool {
        let RecordedSetMemberRemoval { object_key, member } = recorded;
        let Some(members) = self.entries.get_mut(&object_key) else {
            return false;
        };
        let removed = members.remove(&member).is_some();
        if members.is_empty() {
            self.entries.remove(&object_key);
        }
        removed
    }

    fn remove_object(&mut self, recorded: RecordedSetObjectRemoval) -> bool {
        self.entries.remove(&recorded.object_key).is_some()
    }
}

// ---------------------------------------------------------------------------------------------
// THE FOUR EXCEPTIONS. Each mutates WITHOUT a record because on that path the durable record is the
// SOURCE of the change and not its consequence. None hands out `&mut` to the map.
// ---------------------------------------------------------------------------------------------

impl RecordedSetContainer {
    /// EXCEPTION 1 OF 4 -- THE RECONCILE, where the durable index DECIDES and this map is the
    /// derived view of it.
    ///
    /// A load walks the bucket index and derives which members exist and which block backs each.
    /// The persisted map is the OLDER of the two inputs and supplies only what the derived view
    /// could not produce, which is why the merge inserts where ABSENT rather than overwriting.
    ///
    /// Logging here would be backwards: the record is what this read FROM, and emitting another
    /// would make a load look like a write.
    pub(super) fn reconcile_from_durable(
        &mut self,
        derived: HashMap<String, SetMemberMap>,
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

    /// EXCEPTION 2 OF 4 -- THE DELTA FOLD, where the carried blob IS the record.
    ///
    /// A fold applies members a delta CARRIED, and the carry is the newer statement, so here the
    /// carry WINS a collision -- the opposite of the reconcile above, which is why these are two
    /// methods and not one with a flag.
    pub(super) fn fold_carried_elements(
        &mut self,
        object_key: &str,
        carried: Option<&serde_json::Value>,
        live: &HashSet<LiveBlockKey>,
        skipped: &mut usize,
    ) {
        super::merge_container_elements(&mut self.entries, object_key, carried, live, skipped);
    }

    /// EXCEPTION 3 OF 4 -- WAL REPLAY OF A REMOVAL THAT IS ALREADY IN THE RECORD.
    ///
    /// Recovery is REPLAYING a removal the log already holds. Recording it again would write the
    /// recovery itself into the log as a fresh removal, so that the next replay would have two
    /// removals where the store had one. The same reasoning already makes this path's bucket-index
    /// clear ask not to stage.
    ///
    /// Deliberately NARROW: it removes one member of one object and nothing else. It cannot
    /// install, it cannot drop a key, and it cannot be reached with a key the replayed item did not
    /// name.
    ///
    /// THERE IS NO REPLAY-INSTALL EXCEPTION FOR THIS KIND, which is the per-kind difference from
    /// `hashes`. Recovery's `set` arm re-files its block through `upsert_bucket_index_block` before
    /// installing, so it goes through [`install_set_member`] like any other recorded write. Hash
    /// needed a fourth exception because its `context_node` arm installs and files nothing.
    pub(super) fn replay_remove_member(&mut self, object_key: &str, member: &[u8]) -> bool {
        self.entries
            .get_mut(object_key)
            .and_then(|members| members.remove(member))
            .is_some()
    }

    /// EXCEPTION 4 OF 5 -- THE POST-DECODE REPACK, which changes REPRESENTATION and not content.
    ///
    /// `serde`'s `Deserialize` for `BTreeMap` fills the map with `insert` in a loop, and the bytes
    /// it reads were written in key order, so every nested member map in a decoded index is built
    /// by ASCENDING insertion -- the one order that leaves a B-tree half empty, because each full
    /// leaf splits and the left half is never filled again. `repack_decoded_btrees` rebuilds each
    /// one at load, which is the only moment the whole structure is in hand and already paid for.
    ///
    /// NOTHING IS RECORDED BECAUSE NOTHING CHANGES. The same members map to the same addresses
    /// afterwards; only the tree's node occupancy moves. A record here would say a page was
    /// installed when none was, and the decode this follows IS the record.
    ///
    /// `hashes` HAS NO EQUIVALENT, which is why this exception appears for this kind and not that
    /// one: `HashFieldMap` is a sorted vector whose length is its capacity, so it has no half-empty
    /// nodes to pack, and `repack_decoded_btrees` lists it under "nothing to pack". The compiler
    /// named this site -- it was not in the survey that preceded the conversion, because that
    /// survey read `.sets` accesses and this one reaches the field by destructuring the whole
    /// `ShardState`.
    ///
    /// Narrow by construction: it takes no arguments, reaches no key, and cannot change membership.
    pub(super) fn repack_decoded(&mut self) {
        for members in self.entries.values_mut() {
            super::state::repack_btree_map(members);
        }
    }

    /// EXCEPTION 5 OF 5 -- COMPACTION REWRITES WHERE A PAGE LIVES, NEVER WHICH PAGES LIVE.
    ///
    /// A compaction round moves pages and then points the resident addresses at where they went.
    /// The move is what the compactor records; the address rewrite is bookkeeping that follows it,
    /// and a record of its own would say a page was installed when none was.
    ///
    /// The type narrows it to exactly that. Each item yields the object key and a
    /// [`MemberAddressesMut`], which lends `&mut BlockAddress` per member and NOTHING else -- no
    /// insert, no remove, no retain. So this exception provably cannot change the membership of the
    /// map, only where its members point. That is strictly less than the `iter_mut` it replaces,
    /// which could have dropped a member.
    ///
    /// An ITERATOR rather than a callback: it composes, and no borrow problem forces a callback
    /// because each item carries its own disjoint `&mut`.
    pub(super) fn member_addresses_mut(
        &mut self,
    ) -> impl Iterator<Item = (&str, MemberAddressesMut<'_>)> {
        self.entries
            .iter_mut()
            .map(|(object_key, members)| (object_key.as_str(), MemberAddressesMut { members }))
    }
}

/// A lend of one object's member ADDRESSES, and of nothing else.
///
/// It exists so that exception 4 cannot change membership: there is no `insert`, no `remove`, no
/// `retain` and no way to recover the `&mut SetMemberMap` it holds.
pub(super) struct MemberAddressesMut<'a> {
    members: &'a mut SetMemberMap,
}

impl MemberAddressesMut<'_> {
    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&Vec<u8>, &mut BlockAddress)> {
        self.members.iter_mut()
    }
}

// ---------------------------------------------------------------------------------------------
// TEST FIXTURES. `#[cfg(test)]`, so none of this exists in a shipped binary, and each is named
// `_for_test` so a production use cannot be mistaken for an ordinary write.
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
impl RecordedSetContainer {
    pub(super) fn insert_member_for_test(
        &mut self,
        object_key: &str,
        member: Vec<u8>,
        address: BlockAddress,
    ) {
        self.entries
            .entry(object_key.to_string())
            .or_default()
            .insert(member, address);
    }

    pub(super) fn insert_members_for_test(&mut self, object_key: &str, members: SetMemberMap) {
        self.entries.insert(object_key.to_string(), members);
    }

    pub(super) fn remove_for_test(&mut self, object_key: &str) -> Option<SetMemberMap> {
        self.entries.remove(object_key)
    }

    pub(super) fn clear_for_test(&mut self) {
        self.entries.clear();
    }

    pub(super) fn members_mut_for_test(&mut self, object_key: &str) -> Option<&mut SetMemberMap> {
        self.entries.get_mut(object_key)
    }

    pub(super) fn take_for_test(&mut self) -> HashMap<String, SetMemberMap> {
        std::mem::take(&mut self.entries)
    }

    pub(super) fn restore_for_test(&mut self, entries: HashMap<String, SetMemberMap>) {
        self.entries = entries;
    }
}

#[cfg(test)]
impl From<HashMap<String, SetMemberMap>> for RecordedSetContainer {
    fn from(entries: HashMap<String, SetMemberMap>) -> Self {
        Self { entries }
    }
}
