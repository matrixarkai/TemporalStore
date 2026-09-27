// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE FIELD MAP OF ONE HASH, SIZED TO WHAT IT HOLDS.
//!
//! `ShardState` declares seventeen nested model maps whose inner container is an ORDERED map and,
//! until this type, exactly one -- `hashes` -- whose inner container was a HASHED table. This is
//! that one, and it is a sorted vector rather than either.
//!
//! WHY NOT A TABLE, WHICH IS WHAT IT WAS. A hashed table rounds its bucket count up to a power of
//! two and carries the rounding as resident slack, so its cost is a step function of occupancy and
//! its smallest step is already large. Measured on this crate's counting allocator, at ONE field --
//! which is what the product write path produces, see below -- the table took 272 chunk bytes to
//! hold 48 bytes of payload. A vector of one entry took 64.
//!
//! WHY NOT AN ORDERED MAP, WHICH IS WHAT THE OTHER SEVENTEEN USE AND WHAT CONSISTENCY WOULD SAY.
//! Because consistency is not the measurement, and the measurement refutes it: a `BTreeMap` node is
//! a fixed-width block sized for eleven entries whether it holds eleven or one, and at one field it
//! took 560 chunk bytes -- MORE THAN TWICE the table it would have replaced, and 8.75x the vector.
//! It is the worst of the three shapes at every occupancy below eight. That candidate was measured
//! and lost; `engine::tests::model_map_container_cost` carries the table.
//!
//! WHY THE OCCUPANCY IS ONE, AND WHY THAT IS A PROPERTY OF THE WRITE PATH AND NOT OF A FIXTURE.
//! `write_context_node` in `execute_on_shard.rs` is the one producer of a context-node page, and it
//! files that page under the single constant field `CONTEXT_NODE_FIELD` (`"meta"`). Seven readers
//! spell the same constant back. So every context node in a store is a hash of exactly one field,
//! and measured at 4,000 and at 40,000 nodes the fields-per-hash histogram was p50 = p90 = p99 =
//! MAX = 1 with a single distinct occupancy, 100% of keys, at both sizes. The only producer of a
//! wider hash is the Redis-compatible `HashSet`/`HashMultiSet` surface.
//!
//! THE SHAPE IS THEREFORE BIMODAL IN A REAL STORE that uses both, and a bimodal histogram is a
//! warning rather than a licence -- the mixed corpus measured a mean of 1.98 fields per hash over a
//! population containing NO hash holding two, which is the same trap a mean of 1.98 pages per
//! bucket set in this engine before. So the verdict was required to hold at BOTH arms, and it does:
//! the vector is cheaper than the table at every occupancy measured from 1 to 256, by 76.5% at one
//! field and by 51.4% at sixty-four.
//!
//! WHAT IT COSTS, STATED RATHER THAN HIDDEN. An insert is O(n): a binary search for the slot and a
//! memmove of the tail. At one field that is one comparison and no move. At the wide arm it is a
//! shift of a 48-byte entry per displaced field, no allocation, and it replaces a hash of the field
//! name. A lookup is a binary search, which at one field is ONE comparison and ZERO hashes against
//! the table's one comparison AND one hash -- so at the occupancy the product path writes, the
//! lookup is strictly cheaper too, counted rather than timed.
//!
//! ORDER. The entries are kept sorted by field name, so iteration is ordered where the table's was
//! arbitrary. Nothing depended on the arbitrary order -- `HashGetAll` and `HashLen` do not read
//! this map at all, they resolve through `bucket_index_component_block_addresses`, which sorts its
//! own result -- and making the order defined can only remove a source of run-to-run difference.
//!
//! THE WIRE FORMAT IS UNCHANGED, BY CONSTRUCTION AND BY TEST. `hashes` is `skip_serializing`, so
//! nothing writes it; but an index written before that attribute existed can still carry it, and
//! `#[serde(default)]` means a reader must cope either way. This type therefore deserializes from
//! and serializes to exactly the MAP shape the `HashMap` used, through `serde(from/into)`, so no
//! snapshot, manifest or index-log encoding moves a byte.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::block_store::BlockAddress;

/// Up to this many entries the vector is grown to EXACTLY what it holds; at or above it, it doubles
/// like any other `Vec`.
///
/// Eight, because the measured fields-per-hash histogram has its p99 at ONE on the product write
/// path -- `write_context_node` files every context node under a single constant field -- so eight
/// puts the entire dominant arm of the population inside the exact region with room to spare, while
/// leaving the wide arm, which only the Redis-compatible `HashSet` surface produces, to amortize.
/// Below it, slack is the thing being removed; at and above it, the reallocation count is.
const EXACT_GROWTH_BELOW: usize = 8;

/// The field map of one hash: field name -> the page that holds the field's value, kept sorted by
/// field name.
///
/// The inner vector is PRIVATE. Every reader goes through the methods below, which is what lets the
/// container be reconsidered again later without another fifty-site rewrite -- and what stops a
/// caller reaching in and leaving the entries unsorted, which every lookup here assumes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "HashMap<String, BlockAddress>", into = "HashMap<String, BlockAddress>")]
pub(super) struct HashFieldMap {
    entries: Vec<(String, BlockAddress)>,
}

impl HashFieldMap {
    /// Where `field` is, or where it would go.
    fn slot(&self, field: &str) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|(name, _)| name.as_str().cmp(field))
    }

    pub(super) fn get(&self, field: &str) -> Option<&BlockAddress> {
        self.slot(field).ok().map(|at| &self.entries[at].1)
    }

    pub(super) fn get_mut(&mut self, field: &str) -> Option<&mut BlockAddress> {
        match self.slot(field) {
            Ok(at) => Some(&mut self.entries[at].1),
            Err(_) => None,
        }
    }

    pub(super) fn contains_key(&self, field: &str) -> bool {
        self.slot(field).is_ok()
    }

    /// Insert or replace, returning the address that was there. Keeps the vector sorted.
    ///
    /// THE GROWTH IS EXACT WHILE THE MAP IS SMALL, AND AMORTIZED ONCE IT IS NOT, and both halves of
    /// that were measured rather than chosen.
    ///
    /// `Vec::insert` on a full vector grows by DOUBLING from a minimum capacity of FOUR. A
    /// one-field hash -- which is every context node in a store -- would therefore sit in a
    /// four-slot buffer carrying three empty 48-byte slots, which is the same rounding slack the
    /// hashed table was replaced for. Letting `Vec` choose took only 32.22% off the table where
    /// reserving exactly took 75.82%, so the exactness is most of the change.
    ///
    /// But reserving exactly on EVERY insert costs a reallocation per field, and measured over a
    /// corpus carrying forty 100-field hashes that read +88.63% on the allocation column at the
    /// small corpus size. So exactness is held only up to [`EXACT_GROWTH_BELOW`] entries -- which is
    /// above the p99 of the measured fields-per-hash histogram, so the whole dominant arm of the
    /// population is sized exactly -- and above it the vector doubles as it normally would, where
    /// one more doubling is a rounding error on a map that is already wide and the reallocation
    /// count is what matters instead.
    ///
    /// WHAT THE THRESHOLD BUYS, measured at 40,040 hash keys carrying 44,000 fields: the hybrid
    /// gives up 0.47 of a percentage point of the byte saving against reserving exactly everywhere
    /// (75.35% against 75.82%) and takes the allocation column from +9.34% to +0.59%. At the small
    /// corpus size, where forty wide hashes sit against a smaller denominator, it gives up 3.38
    /// points (68.42% against 71.80%) and takes allocations from +88.63% to +5.64%.
    ///
    /// Both columns are reported, at both corpus sizes, in
    /// `what_the_engine_resident_field_maps_cost_against_the_container_they_replaced`.
    pub(super) fn insert(&mut self, field: String, address: BlockAddress) -> Option<BlockAddress> {
        match self.slot(&field) {
            Ok(at) => Some(std::mem::replace(&mut self.entries[at].1, address)),
            Err(at) => {
                if self.entries.len() == self.entries.capacity()
                    && self.entries.len() < EXACT_GROWTH_BELOW
                {
                    self.entries.reserve_exact(1);
                }
                self.entries.insert(at, (field, address));
                None
            }
        }
    }

    pub(super) fn remove(&mut self, field: &str) -> Option<BlockAddress> {
        match self.slot(field) {
            Ok(at) => Some(self.entries.remove(at).1),
            Err(_) => None,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&String, &BlockAddress)> {
        self.entries.iter().map(|(name, address)| (name, address))
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut BlockAddress)> {
        self.entries
            .iter_mut()
            .map(|(name, address)| (&*name, address))
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.iter().map(|(name, _)| name)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &BlockAddress> {
        self.entries.iter().map(|(_, address)| address)
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut BlockAddress> {
        self.entries.iter_mut().map(|(_, address)| address)
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&String, &mut BlockAddress) -> bool) {
        self.entries.retain_mut(|(name, address)| keep(name, address));
    }

    /// The exact-sized shape this container exists for: no spare capacity to carry.
    pub(super) fn shrink_to_fit(&mut self) {
        self.entries.shrink_to_fit();
    }
}

/// THE DELTA FOLD'S ONE OPERATION, so `merge_container_elements` does not have to name this map.
///
/// `sets`, `zsets` and `lists` satisfy this through the blanket `BTreeMap` implementation; this map
/// is the fourth container and needs its own, which is the whole cost of not being a standard
/// collection. `insert_element` is the ordinary `insert`, so a folded element lands in field order
/// like any other and the fold cannot leave the entries unsorted.
impl super::ElementMap for HashFieldMap {
    type Element = String;
    type Value = BlockAddress;

    fn insert_element(&mut self, element: String, value: BlockAddress) {
        self.insert(element, value);
    }
}

impl FromIterator<(String, BlockAddress)> for HashFieldMap {
    fn from_iter<I: IntoIterator<Item = (String, BlockAddress)>>(iter: I) -> Self {
        let mut entries: Vec<(String, BlockAddress)> = iter.into_iter().collect();
        // Sort, then drop earlier duplicates of a field so the result matches what repeated
        // `insert` would have left: the LAST value for a field wins, as it does in a table.
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|later, earlier| {
            if later.0 == earlier.0 {
                *earlier = later.clone();
                true
            } else {
                false
            }
        });
        entries.shrink_to_fit();
        Self { entries }
    }
}

impl<'a> IntoIterator for &'a HashFieldMap {
    type Item = (&'a String, &'a BlockAddress);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, (String, BlockAddress)>,
        fn(&'a (String, BlockAddress)) -> (&'a String, &'a BlockAddress),
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn split<'b>(pair: &'b (String, BlockAddress)) -> (&'b String, &'b BlockAddress) {
            (&pair.0, &pair.1)
        }
        self.entries.iter().map(split as fn(_) -> _)
    }
}

impl IntoIterator for HashFieldMap {
    type Item = (String, BlockAddress);
    type IntoIter = std::vec::IntoIter<(String, BlockAddress)>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

/// THE WIRE SHAPE IS THE TABLE'S. Both directions go through `HashMap`, so an encoding that
/// carried this field before it became `skip_serializing` still decodes, and anything that
/// serializes a `ShardState` through a path that does not skip it writes the same map it always
/// wrote.
impl From<HashMap<String, BlockAddress>> for HashFieldMap {
    fn from(map: HashMap<String, BlockAddress>) -> Self {
        map.into_iter().collect()
    }
}

impl From<HashFieldMap> for HashMap<String, BlockAddress> {
    fn from(fields: HashFieldMap) -> Self {
        fields.entries.into_iter().collect()
    }
}
