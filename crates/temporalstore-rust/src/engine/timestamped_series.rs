// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a timestamped series has to be able to do, named so the helpers over it stop naming one
//! container.
//!
//! # WHY THIS EXISTS
//!
//! Eight resident maps hold `object key -> (timestamp -> page address)`. Four helpers walked and
//! reconciled them, and all four spelled the series out concretely as
//! `BTreeMap<u64, ElementEntry>`. One of them, the apply path's kind dispatch, handed that single
//! type out for six different kinds.
//!
//! The consequence was recorded in the tree before this module existed, in
//! `the_bucket_index_holds_one_page_out_of_line_and_the_series_maps_hold_none`: the six "cannot be
//! adopted for one of them alone", so any change of representation was all six or none. That is
//! not a reviewable change. This trait is the seam that makes them independent -- one kind can move
//! to a different series representation while the other seven keep theirs, because the helpers now
//! require the OPERATIONS rather than the container.
//!
//! # WHY THE TRAIT IS THIS SMALL
//!
//! It is derived from what the four helpers' bodies actually touch, not from `BTreeMap`'s surface.
//! That distinction is the whole point rather than tidiness: a trait reproducing `BTreeMap`'s API
//! would make the next representation conform to the OLD CONTAINER instead of to the operations,
//! and a seam that only one shape can fit through is a rename. So there is no `entry`, no range
//! query, no `first_key_value`, no `retain` -- none of which any helper here calls.
//!
//! Five methods, each with the caller that needs it named:
//!
//!   * `len` -- the reports path, which prints a point count per key.
//!   * `get` -- the reconcile, which refreshes a persisted point from the block-derived view.
//!   * `addresses` -- the live-block walk, which asks "does any point of this key pass the
//!     filter" before paying for the per-point pass.
//!   * `points` -- the address collector, which needs each point once.
//!   * `points_mut` -- the reconcile again, which overwrites addresses in place.
//!
//! A sixth would need a caller to justify it.
//!
//! # WRITES ARE A SECOND, SEPARATE TRAIT, AND THAT IS NOT TIDINESS
//!
//! `TimestampedSeries` has no way to insert a point. The walk and the reconcile never add one --
//! the reconcile overwrites addresses at instants that are already there, and rebuilds a whole
//! absent series by moving it in -- so an insert on that trait would be reachable from code that
//! has no business adding points, and a bound is a statement about what the caller may do.
//!
//! The apply path does add points, so it takes `TimestampedSeriesWrite`. Keeping them apart means
//! the live-block walk cannot write through a series it was handed to read.

use crate::block_store::ElementEntry;
use std::collections::BTreeMap;

/// One object's timestamped page addresses, whatever holds them.
pub(super) trait TimestampedSeries {
    /// How many points this series holds.
    fn len(&self) -> usize;

    /// Whether it holds none. Spelled out because clippy asks for it beside `len`, and because a
    /// representation may answer it without computing a length.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The address recorded at one instant, if any.
    fn get(&self, at: u64) -> Option<&ElementEntry>;

    /// Every address, in no promised order. For the "does ANY point pass" question, which must not
    /// pay for ordering it does not use.
    fn addresses(&self) -> impl Iterator<Item = &ElementEntry>;

    /// Every point, ascending by instant. The order IS promised here: callers collecting addresses
    /// for a stored image depend on it, and a representation that cannot promise it is not a
    /// timestamped series.
    fn points(&self) -> impl Iterator<Item = (u64, &ElementEntry)>;

    /// The points in a window of instants, ascending.
    ///
    /// # THE TEST FOR WHETHER A METHOD BELONGS ON THIS TRAIT
    ///
    /// Not "do callers want it" -- they wanted this, and that is not the argument. The test is:
    /// **can a plausible alternative representation implement it without becoming the thing the
    /// trait exists to escape?** A range over a sorted array is two binary searches and a slice,
    /// which is cheaper than a tree's version rather than a concession to it. So it passes, and
    /// the trait stays honest.
    ///
    /// WHAT WOULD FAIL THE SAME TEST, so the line is visible rather than implied: a `range_mut`
    /// handing out a cursor that assumes stable node addresses, or anything implying O(log n)
    /// INSERTION in the middle. A sorted array cannot provide either without becoming a tree.
    ///
    /// AND WHY A SIXTH METHOD IS EXPECTED RATHER THAN A SURPRISE. This trait was sized to what the
    /// four shared HELPERS touch, which was a principled sample and still the wrong population:
    /// the helpers never read a window, and the first tenant to migrate immediately did. A seam is
    /// only as general as the dimension it varied -- and that applies to WHICH CALLERS WERE
    /// SAMPLED, not only to which type parameter was abstracted.
    fn range(
        &self,
        bounds: impl std::ops::RangeBounds<u64>,
    ) -> impl Iterator<Item = (u64, &ElementEntry)>;

    /// Every point, ascending, for overwriting addresses in place. The instant is handed over by
    /// value because no caller mutates it -- an instant is this map's key, and a representation
    /// that let a walker change it could silently reorder itself.
    fn points_mut(&mut self) -> impl Iterator<Item = (u64, &mut ElementEntry)>;
}

/// THE REPRESENTATION EVERY SERIES STILL USES, and the only one for now.
///
/// This impl is what makes the change above a seam and not a migration: nothing moves to a new
/// representation here, so every behaviour is the behaviour the concrete type already had and every
/// method is a forward. A second impl arrives with the first kind that actually moves.
impl TimestampedSeries for BTreeMap<u64, ElementEntry> {
    fn len(&self) -> usize {
        BTreeMap::len(self)
    }

    fn get(&self, at: u64) -> Option<&ElementEntry> {
        BTreeMap::get(self, &at)
    }

    fn addresses(&self) -> impl Iterator<Item = &ElementEntry> {
        self.values()
    }

    fn points(&self) -> impl Iterator<Item = (u64, &ElementEntry)> {
        self.iter().map(|(at, address)| (*at, address))
    }

    fn range(
        &self,
        bounds: impl std::ops::RangeBounds<u64>,
    ) -> impl Iterator<Item = (u64, &ElementEntry)> {
        BTreeMap::range(self, bounds).map(|(at, address)| (*at, address))
    }

    fn points_mut(&mut self) -> impl Iterator<Item = (u64, &mut ElementEntry)> {
        self.iter_mut().map(|(at, address)| (*at, address))
    }
}

/// A series the apply path may add points to.
///
/// Separate from [`TimestampedSeries`] so that a bound says what a caller may do: the walk and the
/// reconcile take the read trait and therefore CANNOT add a point, which is a property of the
/// signature rather than of the body.
///
/// `Default` is required because the apply path creates a series for a key that has none -- the
/// `entry(..).or_default()` the old dispatch did at its call site, moved in here where the
/// representation is known.
pub(super) trait TimestampedSeriesWrite: TimestampedSeries + Default {
    /// Record an address at one instant, replacing whatever was there.
    fn set(&mut self, at: u64, address: ElementEntry);

    /// Forget the point at one instant, if any.
    fn clear(&mut self, at: u64);
}

impl TimestampedSeriesWrite for BTreeMap<u64, ElementEntry> {
    fn set(&mut self, at: u64, address: ElementEntry) {
        self.insert(at, address);
    }

    fn clear(&mut self, at: u64) {
        self.remove(&at);
    }
}

/// Set or clear one point of one object's series, and hand back the addresses that SURVIVE.
///
/// The surviving set is returned rather than read back by the caller because the caller needs it
/// for the bucket-index sync, and reaching back into the map for it would be a second lookup
/// against a series this function may just have removed.
///
/// AN EMPTIED SERIES TAKES ITS KEY WITH IT, which is the behaviour the apply path had at its call
/// site and is kept here so that no arm can forget it.
/// GENERIC OVER THE KEY TYPE TOO, and that was an omission rather than a flourish. The seam that made
/// the series abstract left `String` hard-coded here, while the consolidated map these tenants
/// migrate INTO is keyed by `ModelKey = Box<str>` -- sixteen bytes against twenty-four, chosen
/// deliberately. A seam is only as general as the dimension it varied, and pointing it at the real
/// destination is what found this.
///
/// `Borrow<str>` to look a key up and remove it from a `&str`; `From<&str>` to create one. Both
/// `String` and `Box<str>` satisfy the pair.
pub(super) fn set_point_and_collect_live<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    object_key: &str,
    at: u64,
    address: Option<ElementEntry>,
) -> Vec<ElementEntry>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + for<'a> From<&'a str>,
    S: TimestampedSeriesWrite,
{
    let series = map.entry(K::from(object_key)).or_default();
    match address {
        Some(address) => series.set(at, address),
        None => series.clear(at),
    }
    let live = series.addresses().cloned().collect::<Vec<_>>();
    if series.is_empty() {
        map.remove(object_key);
    }
    live
}

// =================================================================================================
// THE OWNED SORTED ARRAY, AND WHY A SECOND REPRESENTATION EARNS ITS PLACE
// =================================================================================================

/// One object's timestamped page addresses, held as ONE OWNED SORTED ARRAY instead of a B-tree.
///
/// # WHAT IT REPLACES, AND WHAT THAT COSTS TODAY
///
/// The representation above -- `BTreeMap<u64, ElementEntry>` behind a `HashMap` key -- allocates a
/// 280-byte root node before a single element byte is stored, and it pays that per key. Measured
/// in this tree at one element: **312 B in 4 allocations for the B-tree against 48 B in 2 for this
/// array** (`Vec` header 24 B, buffer 24 B for one 24-byte pair), and the owning field itself is a
/// thin pointer at eight bytes rather than a 24-byte `BTreeMap` header. Those are two different
/// components and are reported as two numbers, never one standing in for the other.
///
/// The access pattern is what makes the array the right shape and not a compromise: these series
/// are append-mostly, read by one instant or walked whole. An array appends in amortised O(1),
/// binary-searches in O(log N), and walks in order with no pointer chasing. Nothing here ranges
/// over a sub-interval of a series -- the time-windowed context reads range over
/// `context_event_timeline` and resolve single instants into the series -- so the ordered-range
/// strength of a B-tree is not being given up.
///
/// # SORTED IS AN INVARIANT, NOT A CONVENTION
///
/// [`TimestampedSeries::points`] promises ascending order and its callers depend on it for a
/// stored image. So every insert lands by binary search and every removal preserves order; the
/// array is never sorted lazily and there is no "needs sorting" state for a reader to meet. The
/// test over this type drives out-of-order and duplicate inserts and asserts the walk is ascending
/// after each one, because an invariant that only holds on sorted input is not an invariant.
///
/// # THE SAME PAYLOAD TYPE THE OBJECT SLOT CARRIES
///
/// `ObjectSlot` already holds `Option<Box<Vec<(u64, ElementEntry)>>>` as its owned element array,
/// and this is deliberately that same type rather than a parallel one. When a map moves under the
/// consolidated object map its series becomes the payload of a slot with no change of
/// representation, and the assert below is what keeps the two from drifting apart in the meantime.
///
/// # NOT SERIALIZED, AND THAT IS WHY NO FORMAT STAMP IS OWED
///
/// There is no `Serialize`/`Deserialize` here at all. A map adopting this representation keeps its
/// stored image in a compatibility field under the wire key it has always had, and the array is
/// rebuilt from that field by a decode fold. A serde impl on this type would be the thing that
/// turned an adoption into a format change, so its absence is load-bearing rather than an omission.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct OwnedTimestampedSeries {
    /// `None` and an empty array mean the same thing to every reader here, and `None` is what this
    /// type produces: an emptied series drops its allocation rather than keeping a zero-length
    /// buffer, so a map that filled and drained does not hold one `Vec` header per key forever.
    points: Option<Box<Vec<(u64, ElementEntry)>>>,
}

/// EIGHT BYTES, PINNED AS AN EQUALITY. An inequality against the B-tree header it replaces would
/// pass at every width below 24.
const _: () = assert!(std::mem::size_of::<OwnedTimestampedSeries>() == 8);
const _: () = assert!(std::mem::align_of::<OwnedTimestampedSeries>() == 8);

/// THE WIDTH IS PINNED AGAINST THE SPELLING THE SLOT USES -- and this assert is stated for exactly
/// what it checks, which is less than "the two cannot drift". It compares this type against the
/// payload type written out in full; it does NOT read `ObjectSlot`, so a change on the slot's side
/// would not fire HERE. What fires there is the slot's own assert, which pins the same payload at
/// eight. The two together hold both ends at eight, and neither on its own proves the types are the
/// same type.
const _: () = assert!(
    std::mem::size_of::<OwnedTimestampedSeries>()
        == std::mem::size_of::<Option<Box<Vec<(u64, ElementEntry)>>>>()
);

impl OwnedTimestampedSeries {
    /// How many points the array holds.
    pub(super) fn len(&self) -> usize {
        self.points.as_ref().map_or(0, |points| points.len())
    }

    /// Whether it holds none -- true both for an absent array and, defensively, for an empty one,
    /// so a reader never has to know which of the two this type is in.
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many heap allocations this series is currently holding. Reported SEPARATELY from its
    /// byte footprint and never folded into it: the win of the array over the B-tree is two
    /// numbers, bytes and allocation count, and one of them standing in for the other is how a
    /// footprint claim gets published wrong.
    pub(super) fn allocation_count(&self) -> usize {
        match self.points.as_ref() {
            // The boxed `Vec` is one allocation for its header; the buffer is a second, and only
            // once there is something in it. A `Vec` at capacity 0 has no buffer.
            None => 0,
            Some(points) if points.capacity() == 0 => 1,
            Some(_) => 2,
        }
    }

    /// Where an instant sits, or where it would be inserted.
    fn locate(&self, at: u64) -> Result<usize, usize> {
        match self.points.as_ref() {
            None => Err(0),
            Some(points) => points.binary_search_by_key(&at, |(instant, _)| *instant),
        }
    }

    /// The address recorded at one instant, if any.
    pub(super) fn get(&self, at: &u64) -> Option<&ElementEntry> {
        let index = self.locate(*at).ok()?;
        self.points.as_ref().map(|points| &points[index].1)
    }

    /// Whether an instant is recorded. Spelled out because the maps adopting this type are read by
    /// membership as often as by value, and `get(..).is_some()` at a call site reads as a value
    /// fetch that was thrown away.
    pub(super) fn contains_key(&self, at: &u64) -> bool {
        self.locate(*at).is_ok()
    }

    /// Record an address at one instant, replacing whatever was there, and hand back what it
    /// replaced.
    ///
    /// SORT ORDER IS MAINTAINED HERE rather than fixed up later: the insert position comes from the
    /// same binary search that answers `get`, so the array is ordered after every call whatever
    /// order the caller writes in.
    pub(super) fn insert(&mut self, at: u64, address: ElementEntry) -> Option<ElementEntry> {
        match self.locate(at) {
            Ok(index) => {
                let points = self
                    .points
                    .as_mut()
                    .expect("a located index means the array exists");
                Some(std::mem::replace(&mut points[index].1, address))
            }
            Err(index) => {
                self.points
                    .get_or_insert_with(|| Box::new(Vec::new()))
                    .insert(index, (at, address));
                None
            }
        }
    }

    /// Forget the point at one instant, if any.
    ///
    /// AN EMPTIED ARRAY DROPS ITS ALLOCATION. A map that filled and drained would otherwise hold a
    /// `Vec` header per key for the life of the shard, which is exactly the per-key constant cost
    /// this representation exists to remove.
    pub(super) fn remove(&mut self, at: &u64) -> Option<ElementEntry> {
        let index = self.locate(*at).ok()?;
        let points = self
            .points
            .as_mut()
            .expect("a located index means the array exists");
        let (_, address) = points.remove(index);
        if points.is_empty() {
            self.points = None;
        }
        Some(address)
    }

    /// Every point, ascending.
    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = (&u64, &ElementEntry)> {
        self.points
            .as_deref()
            .map_or(&[][..], |points| points.as_slice())
            .iter()
            .map(|(at, address)| (at, address))
    }

    /// Every point, ascending, with the address mutable. The instant is handed over as a shared
    /// borrow because a walker able to change it could silently reorder the array.
    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&u64, &mut ElementEntry)> {
        self.points
            .as_deref_mut()
            .map_or(&mut [][..], |points| points.as_mut_slice())
            .iter_mut()
            .map(|(at, address)| (&*at, address))
    }

    /// Every instant, ascending. The compression policy reads these without their addresses.
    pub(super) fn keys(&self) -> impl DoubleEndedIterator<Item = &u64> {
        self.iter().map(|(at, _)| at)
    }

    /// Every address, ascending by instant.
    pub(super) fn values(&self) -> impl DoubleEndedIterator<Item = &ElementEntry> {
        self.iter().map(|(_, address)| address)
    }

    /// The B-tree view of this series, for the reporting and capture paths whose own output shape
    /// is a `BTreeMap` and which must not change because the representation behind them did.
    pub(super) fn to_btree(&self) -> BTreeMap<u64, ElementEntry> {
        self.iter()
            .map(|(at, address)| (*at, address.clone()))
            .collect()
    }
}

/// Built from the B-tree it replaces, which is what a decode fold has in hand. Ascending already,
/// so this is a copy and not a sort -- but the invariant does not depend on that being true, since
/// [`OwnedTimestampedSeries::insert`] places by search either way.
impl From<&BTreeMap<u64, ElementEntry>> for OwnedTimestampedSeries {
    fn from(series: &BTreeMap<u64, ElementEntry>) -> Self {
        if series.is_empty() {
            return Self::default();
        }
        Self {
            points: Some(Box::new(
                series
                    .iter()
                    .map(|(at, address)| (*at, address.clone()))
                    .collect(),
            )),
        }
    }
}

impl From<BTreeMap<u64, ElementEntry>> for OwnedTimestampedSeries {
    fn from(series: BTreeMap<u64, ElementEntry>) -> Self {
        if series.is_empty() {
            return Self::default();
        }
        Self {
            points: Some(Box::new(series.into_iter().collect())),
        }
    }
}

impl From<&OwnedTimestampedSeries> for BTreeMap<u64, ElementEntry> {
    fn from(series: &OwnedTimestampedSeries) -> Self {
        series.to_btree()
    }
}

impl TimestampedSeries for OwnedTimestampedSeries {
    fn len(&self) -> usize {
        OwnedTimestampedSeries::len(self)
    }

    /// TWO BINARY SEARCHES AND A SLICE -- the implementation the trait's own doc predicts for a
    /// sorted array, rather than a concession to the tree this replaces. Each bound is resolved
    /// once against the array, so a window costs O(log N) and then one pass over the points
    /// inside it and nothing per point outside it.
    ///
    /// AND THE TYPE DOC ABOVE SAYS NOTHING RANGES OVER A SERIES, WHICH WAS TRUE WHEN WRITTEN AND
    /// IS NOT A REASON TO LEAVE THIS UNIMPLEMENTED. The trait grew this method because the first
    /// kind to migrate read a window immediately; a representation that answered "no caller needs
    /// it" would be the seam narrowing to whatever the current tenant happens to do.
    fn range(
        &self,
        bounds: impl std::ops::RangeBounds<u64>,
    ) -> impl Iterator<Item = (u64, &ElementEntry)> {
        let points: &[(u64, ElementEntry)] =
            self.points.as_deref().map_or(&[], |points| points.as_slice());
        let start = match bounds.start_bound() {
            std::ops::Bound::Unbounded => 0,
            std::ops::Bound::Included(at) => points.partition_point(|(instant, _)| instant < at),
            std::ops::Bound::Excluded(at) => points.partition_point(|(instant, _)| instant <= at),
        };
        let end = match bounds.end_bound() {
            std::ops::Bound::Unbounded => points.len(),
            std::ops::Bound::Included(at) => points.partition_point(|(instant, _)| instant <= at),
            std::ops::Bound::Excluded(at) => points.partition_point(|(instant, _)| instant < at),
        };
        // An inverted or empty window yields nothing rather than panicking on the slice index,
        // which is what `BTreeMap::range` does for the bounds it accepts.
        points[start..end.max(start)]
            .iter()
            .map(|(at, address)| (*at, address))
    }

    fn get(&self, at: u64) -> Option<&ElementEntry> {
        OwnedTimestampedSeries::get(self, &at)
    }

    fn addresses(&self) -> impl Iterator<Item = &ElementEntry> {
        self.values()
    }

    fn points(&self) -> impl Iterator<Item = (u64, &ElementEntry)> {
        self.iter().map(|(at, address)| (*at, address))
    }

    fn points_mut(&mut self) -> impl Iterator<Item = (u64, &mut ElementEntry)> {
        self.iter_mut().map(|(at, address)| (*at, address))
    }
}

impl TimestampedSeriesWrite for OwnedTimestampedSeries {
    fn set(&mut self, at: u64, address: ElementEntry) {
        self.insert(at, address);
    }

    fn clear(&mut self, at: u64) {
        self.remove(&at);
    }
}
