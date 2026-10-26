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
//! `the_bucket_index_holds_one_block_out_of_line_and_the_series_maps_hold_none`: the six "cannot be
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
use std::borrow::Borrow as _;
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
    /// DOUBLE-ENDED, because a reader of a time window wants its NEWEST point as often as its
    /// oldest: `load_context_summary_as_of` walks `.range(..).rev()` to decode one record instead of
    /// the window. Both representations can do it -- `BTreeMap::range` and a slice's `iter` are each
    /// double-ended, and `map` preserves that -- so a bare `impl Iterator` here was a bound narrower
    /// than either implementation, and it cost a caller its reverse walk the moment a map converted.
    fn range(
        &self,
        bounds: impl std::ops::RangeBounds<u64>,
    ) -> impl DoubleEndedIterator<Item = (u64, &ElementEntry)>;

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
    ) -> impl DoubleEndedIterator<Item = (u64, &ElementEntry)> {
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
/// migrate INTO is keyed by `ModelKey = Arc<str>` -- sixteen bytes against twenty-four, chosen
/// deliberately, and shareable where a `Box<str>` is not: two maps can hold ONE `Arc`
/// allocation, which is the whole reason this seam is worth pointing at the real destination. A seam is only as general as the dimension it varied, and pointing it at the real
/// destination is what found this.
///
/// `Borrow<str>` to look a key up and remove it from a `&str`, and `Clone` for the removal path --
/// a refcount bump for a shared handle, the copy it always was for an owned `String`. Creating a
/// key from a `&str` is NOT among the bounds any more: that is what minted a second allocation on
/// every call, and it now lives in `set_point_for_owned_key` for the kinds that need it.
pub(super) fn set_point_and_collect_live<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    key: K,
    at: u64,
    address: Option<ElementEntry>,
) -> Vec<ElementEntry>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + Clone,
    S: TimestampedSeriesWrite,
{
    set_point_inner(map, key, at, address, true)
}

/// THE SAME WRITE WITHOUT THE SURVIVING SET, FOR THE CALLERS THAT DO NOT WANT IT.
///
/// # WHY THIS EXISTS
///
/// The surviving set is a clone of EVERY address the series holds, so asking for it on a write to
/// a 3,250-point series copies 3,250 addresses into a fresh vector. That is the right price for
/// the one caller that needs them, and it was being paid by two that threw the vector away: both
/// context-event write arms in `execute_on_shard` called the collecting door and bound the result
/// to nothing. Every context event written to a node copied the node entire event series and
/// dropped it.
///
/// MEASURED on `what_one_context_event_append_costs_at_two_series_lengths`, whose signature is
/// what pointed here: a flat 122 allocations per append with bytes still climbing 2.66x over an
/// 8x series -- one allocation, sized by the entries held, which is exactly one whole-series
/// clone. That same probe had already ruled out the two causes assumed before it, the whole-object
/// index record and the shard-wide map in the ordinal allocator, the second by moving it and
/// re-measuring.
///
/// # ONE BODY, NOT TWO
///
/// Both doors run `set_point_inner`, because the empty-series rule is the kind of thing that gets
/// forgotten in a copy: AN EMPTIED SERIES TAKES ITS KEY WITH IT, and a second body would be one
/// more place for that to drift. `Vec::new()` does not allocate, so the non-collecting door moves
/// no bytes at all.
pub(super) fn set_point<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    key: K,
    at: u64,
    address: Option<ElementEntry>,
) where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + Clone,
    S: TimestampedSeriesWrite,
{
    let dropped = set_point_inner(map, key, at, address, false);
    debug_assert!(
        dropped.is_empty(),
        "the non-collecting door returned addresses, so it collected the series it exists not to collect"
    );
}

fn set_point_inner<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    key: K,
    at: u64,
    address: Option<ElementEntry>,
    collect_live: bool,
) -> Vec<ElementEntry>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + Clone,
    S: TimestampedSeriesWrite,
{
    // THE KEY ARRIVES OWNED, AND THAT IS THE WHOLE CHANGE. This took a `&str` and minted its own
    // key with `K::from(object_key)` on every call -- so a map whose key CAN be shared got a fresh
    // allocation anyway, and converting the map's key type would have shared nothing. The caller
    // knows whether it already holds a handle; this does not, and should not guess.
    //
    // The clone is for the removal below: `entry` consumes the key, and the empty-series path still
    // needs the characters to remove by. For a shared handle that clone is a refcount bump; for an
    // owned `String` it is the copy it always was.
    let for_removal = key.clone();
    let series = map.entry(key).or_default();
    match address {
        Some(address) => series.set(at, address),
        None => series.clear(at),
    }
    // COLLECTED BEFORE THE REMOVAL BELOW, AND THAT ORDER IS LOAD-BEARING: a write that empties
    // the series takes its key out of the map, and the surviving set the collecting caller wants
    // is the one from before that happened -- which for an emptied series is empty anyway, and
    // for every other write is the series still there.
    let live = if collect_live {
        series.addresses().cloned().collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if series.is_empty() {
        map.remove(for_removal.borrow());
    }
    live
}

/// As above, for a map whose key this call has to allocate.
///
/// The kind dispatch in `lifecycle` walks maps keyed by `String`, which owns its characters and so
/// cannot share one: there is nothing for a caller to hand in, and making each of its six arms
/// spell the allocation would say the same thing six times. Kept as its own door so that a map which CAN
/// share does not reach the allocating path by accident -- which is exactly how the generic helper
/// came to defeat sharing in the first place.
pub(super) fn set_point_for_owned_key<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    object_key: &str,
    at: u64,
    address: Option<ElementEntry>,
) -> Vec<ElementEntry>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + Clone + for<'a> From<&'a str>,
    S: TimestampedSeriesWrite,
{
    set_point_and_collect_live(map, K::from(object_key), at, address)
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

/// Where exact growth stops and `Vec`'s doubling takes over, in points.
///
/// EIGHT, AND IT IS THE SAME EIGHT `HashFieldMap` USES. Both types are sorted owned arrays standing
/// in for a per-key map, both pay a recopy to hold capacity equal to length, and the bound on that
/// recopy is what the threshold buys: at most seven pairs, 168 bytes. Above it the saving is a
/// shrinking fraction of a buffer that is already amortised, and the recopy is not.
const EXACT_GROWTH_BELOW: usize = 8;

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
                let points = self.points.get_or_insert_with(|| Box::new(Vec::new()));
                // EXACT CAPACITY WHILE THE SERIES IS SHORT, AMORTISED ONCE IT IS NOT.
                //
                // `Vec`'s first allocation is capacity FOUR, so a one-point series holds 24 bytes
                // of payload in a 96-byte buffer. That is 72 bytes of nothing per key, and the
                // one-point key is the common case in a series map whose occupancy is bimodal --
                // a handful of fixed indexes holding thousands, plus one per hash holding one.
                //
                // WHY NOT A BOXED SLICE, WHICH WOULD BE EXACT BY CONSTRUCTION. `Box<[T]>` is a fat
                // pointer: it would take this type from eight bytes to sixteen, and -- the reason
                // that is not merely a trade -- it carries no spare capacity, so EVERY append
                // reallocates and copies the whole array. That is quadratic in total copying for a
                // series appended to n times, and `context_events` occupancy is lifetime event
                // count per node, with no bound. Exactness is worth having where the series is
                // short and not worth buying with a realloc per append where it is long.
                //
                // So the threshold: below it, grow to exactly what is needed, which costs one
                // recopy of at most EXACT_GROWTH_BELOW - 1 pairs; at or above it, `Vec`'s doubling
                // takes over untouched. Same policy and same threshold as `HashFieldMap`, which
                // arrived at it from the same measurement rather than by being copied.
                if points.len() < EXACT_GROWTH_BELOW && points.len() == points.capacity() {
                    points.reserve_exact(1);
                }
                points.insert(index, (at, address));
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
    ) -> impl DoubleEndedIterator<Item = (u64, &ElementEntry)> {
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

#[cfg(test)]
mod a_short_series_does_not_hold_a_buffer_it_is_not_using {
    use super::{OwnedTimestampedSeries, EXACT_GROWTH_BELOW};
    use crate::block_store::ElementEntry;

    /// Capacity equals length below the threshold, and does NOT at it -- a BRACKETING PAIR.
    ///
    /// The first half alone would pass if exact growth were applied at every length, which is the
    /// shape this deliberately does not have: it would buy a realloc per append for a series with
    /// no bound on its occupancy. The second half is what makes the threshold a threshold, so a
    /// change that quietly extended exactness upward reddens here instead of showing up as a
    /// throughput regression nothing in this crate measures.
    #[test]
    fn the_threshold_is_exact_and_not_merely_an_upper_bound() {
        let mut series = OwnedTimestampedSeries::default();

        for point in 1..=EXACT_GROWTH_BELOW {
            series.insert(point as u64, ElementEntry::default());
            let held = series.points.as_ref().expect("a written series has a buffer");
            assert_eq!(
                held.len(),
                point,
                "insert {point} did not land"
            );
            assert_eq!(
                held.capacity(),
                held.len(),
                "at {point} points, below the threshold of {EXACT_GROWTH_BELOW}, the buffer holds \
                 spare capacity: len {} capacity {}",
                held.len(),
                held.capacity()
            );
        }

        // One past the threshold: `Vec`'s doubling takes over and capacity must OVERSHOOT.
        series.insert(EXACT_GROWTH_BELOW as u64 + 1, ElementEntry::default());
        let held = series.points.as_ref().expect("a written series has a buffer");
        assert_eq!(held.len(), EXACT_GROWTH_BELOW + 1);
        assert!(
            held.capacity() > held.len(),
            "at {} points, AT OR ABOVE the threshold, growth should be amortised and capacity \
             should overshoot -- it is {}, equal to the length, so exact growth did not stop \
             where it was supposed to",
            held.len(),
            held.capacity()
        );
    }

    /// What the spare capacity cost, in the allocator's own units.
    ///
    /// `chunk(req) = max(32, align16(req + 8))` for glibc, which is what production uses -- the
    /// crate's only `#[global_allocator]` is behind `cfg(all(test, feature = "alloc-probe"))`, so
    /// the chunk model is the production model and not a test artefact.
    ///
    /// A pair is 24 bytes: an eight-byte instant and a sixteen-byte entry, both pinned elsewhere
    /// and re-derived here rather than assumed.
    #[test]
    fn a_one_point_series_no_longer_pays_for_four() {
        fn chunk(request: usize) -> usize {
            let rounded = (request + 8 + 15) / 16 * 16;
            if rounded < 32 {
                32
            } else {
                rounded
            }
        }

        let pair = std::mem::size_of::<(u64, ElementEntry)>();
        assert_eq!(pair, 24, "a point is an instant plus an entry");

        let mut series = OwnedTimestampedSeries::default();
        series.insert(1, ElementEntry::default());
        let held = series.points.as_ref().expect("a written series has a buffer");

        let now = chunk(pair * held.capacity());
        let before = chunk(pair * 4); // `Vec`'s first allocation, which is what this replaced
        assert_eq!(held.capacity(), 1, "a one-point series should hold one point's worth");
        assert!(
            now < before,
            "the exact buffer ({now} B) should be smaller than the capacity-four one ({before} B)"
        );
        println!(
            "  one-point series buffer: {before} B at capacity 4 -> {now} B exact, {} B saved",
            before - now
        );

        // THE WHOLE CURVE, NOT ONE POINT, because the saving decays to nothing by the threshold
        // and a single figure invites quoting the best cell as the result. `Vec`'s doubling from
        // capacity four goes 4, 8, 16, so the old buffer is flat across each doubling band while
        // the exact one steps every point -- which is why the saving is sawtoothed rather than
        // monotone, and why n=1 is the best case and n=4 and n=8 the worst.
        println!("  points   exact B   capacity-4 doubling B   saved");
        let mut worst = usize::MAX;
        for points in 1..=EXACT_GROWTH_BELOW {
            let mut series = OwnedTimestampedSeries::default();
            for point in 1..=points {
                series.insert(point as u64, ElementEntry::default());
            }
            let held = series.points.as_ref().expect("a written series has a buffer");
            assert_eq!(held.capacity(), points, "exact growth should hold capacity at length");
            // What `Vec` alone would have held: 4, then doubling, which is the shape this replaced.
            let mut doubling = 4;
            while doubling < points {
                doubling *= 2;
            }
            let exact_bytes = chunk(pair * points);
            let doubling_bytes = chunk(pair * doubling);
            let saved = doubling_bytes - exact_bytes;
            worst = worst.min(saved);
            println!(
                "  {points:>6}   {exact_bytes:>7}   {doubling_bytes:>21}   {saved:>5}"
            );
        }
        assert_eq!(
            worst, 0,
            "the saving should reach zero somewhere below the threshold -- where length meets a              doubling boundary the two shapes agree, and a non-zero worst case here would mean              this comparison is not modelling `Vec`'s growth"
        );
    }

    /// An emptied series still drops its buffer. Exact growth touches the insert path only, and
    /// this is the property on the remove path that it could have broken by accident.
    #[test]
    fn emptying_a_series_still_releases_the_buffer() {
        let mut series = OwnedTimestampedSeries::default();
        series.insert(7, ElementEntry::default());
        assert!(series.points.is_some());
        series.remove(&7);
        assert!(
            series.points.is_none(),
            "an emptied series must drop its allocation rather than keep a zero-length buffer"
        );
    }
}

/// Serialize a map of owned arrays in the B-TREE SHAPE IT HAS ALWAYS HAD ON THE WIRE.
///
/// # WHY THIS EXISTS RATHER THAN A COMPATIBILITY FIELD
///
/// [`OwnedTimestampedSeries`] carries no `Serialize`/`Deserialize`, on purpose, so the usual way
/// for a map to adopt it is a second field under the old wire key plus a decode fold -- which is a
/// FORMAT change, with an old image and a new one to keep straight. A map whose stored shape is
/// `{key: {timestamp: address}}` does not need that: the array holds exactly the same pairs in
/// exactly the same order, so it can be written AS that object and read back from it. The image is
/// byte-identical before and after, which is the whole point -- no stamp is owed, no fold runs, and
/// an older binary reads a newer image.
///
/// THE WRITE ALLOCATES NOTHING. `collect_map` is handed the array's own iterator, so a dump does
/// not build a temporary `BTreeMap` per series on the way out. Going through `to_btree()` would
/// have been shorter and would have put one allocation per series into every dump.
pub(super) mod as_btree_maps {
    use super::{ElementEntry, OwnedTimestampedSeries};
    use std::collections::{BTreeMap, HashMap};

    struct AsStoredMap<'a>(&'a OwnedTimestampedSeries);

    impl serde::Serialize for AsStoredMap<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.collect_map(self.0.iter())
        }
    }

    pub(in crate::engine) fn serialize<S>(
        map: &HashMap<std::sync::Arc<str>, OwnedTimestampedSeries>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(map.iter().map(|(key, series)| (key, AsStoredMap(series))))
    }

    pub(in crate::engine) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<std::sync::Arc<str>, OwnedTimestampedSeries>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let stored: HashMap<std::sync::Arc<str>, BTreeMap<u64, ElementEntry>> =
            serde::Deserialize::deserialize(deserializer)?;
        Ok(stored
            .into_iter()
            .map(|(key, series)| (key, OwnedTimestampedSeries::from(series)))
            .collect())
    }
}
