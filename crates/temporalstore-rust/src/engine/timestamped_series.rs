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
pub(super) fn set_point_and_collect_live<S: TimestampedSeriesWrite>(
    map: &mut std::collections::HashMap<String, S>,
    object_key: &str,
    at: u64,
    address: Option<ElementEntry>,
) -> Vec<ElementEntry> {
    let series = map.entry(object_key.to_string()).or_default();
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
