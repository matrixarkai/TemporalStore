// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A timestamped series map that groups its objects by routing bucket and names each object's
//! identity ONCE.
//!
//! NOTHING USES THIS YET. It is the first of three steps: the container and its wire adapter land
//! alone so the design can be reviewed without a call-site conversion riding on it, then the shared
//! timestamped-series helpers are generalised, then one map is converted. A half-finished conversion
//! of that size would leave the tree worse than not starting.
//!
//! WHAT IT IS, AND WHY THIS SHAPE. Today a series map is `HashMap<key, BTreeMap<at, address>>`: one
//! whole B-tree per object key, however few points that key holds, and a one-entry B-tree charges 280
//! bytes of node to carry 16 bytes of value. This groups every object that routes to the same bucket
//! into ONE allocation: an `objects` array sorted by key, saying where each object's rows begin and
//! how many there are, and one contiguous fixed-width `rows` run holding them all in that order.
//!
//! Measured against what ships, bytes per row, at six occupancies (#2075):
//!
//! | points a key | a B-tree a key | this shape |
//! |---|---|---|
//! | 1 | 366.0 | 74.9 |
//! | 2 | 182.9 | 50.9 |
//! | 4 | 91.3 | 38.9 |
//! | 10 | 38.5 | 31.7 |
//! | 100 | 29.6 | 25.2 |
//! | 1,000 | 26.6 | 24.1 |
//!
//! Better at every occupancy, with no crossover. A row here is a `u64` and a 16-byte address -- 24
//! bytes of data -- so 24.1 at density is 1.004x the data itself: the container overhead is a tenth of
//! a byte a row and 24 is the floor. Going below it would mean shrinking the ROW, not the container.
//!
//! WHAT IS DELIBERATELY NOT COPIED. The rows are FIXED WIDTH and kept sorted, so a lookup is two
//! binary searches. A variable-length payload searched by walking it is refuted twice over -- on the
//! read path at 3.0-17.8x on misses, and from the footprint side -- and the reason is structural:
//! variable-length packing is both what makes such a buffer compact and what stops it being
//! binary-searchable. The two-level SHAPE is what is being taken; the walk is not.
//!
//! WHY A PER-OBJECT START AND LENGTH DOES NOT GIVE THE SAVING BACK. An offset table costs per ENTRY
//! only when entries are not the same width. These are, so finding a row inside an object's run is
//! arithmetic and the only thing needing to be named is where each OBJECT's run starts: eight bytes
//! against a whole run. #2075 measured that rather than arguing it.
//!
//! THE RANGE IS NOT KNOWN WHEN THE BYTES ARE DECODED, which is why the bucketing is a method rather
//! than a constructor. `ShardState::routing_range` answers the whole keyspace until
//! `install_shard_state` stamps it, and on that range every key gets a bucket of its own by construction -- precisely the regime
//! where the amortisation vanishes. So a decoded container is CORRECT but uncompacted, and
//! [`GroupedSeriesIndex::rebucket`] is what compacts it once the range is known. Correct in every
//! state, compact once told the range; there is no invalid state to get wrong.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::block_store::ElementEntry;

/// Where one object's rows sit inside its bucket's run.
///
/// The key is here and NOWHERE ELSE: not in a map key, not in the rows. That is the whole point --
/// a row is 24 bytes of data with no identity in it, and identity is paid once an object.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectRun {
    key: Arc<str>,
    /// First row of this object, as an index into its bucket's `rows`.
    start: u32,
    /// How many rows this object owns.
    len: u32,
}

/// One bucket: its objects in key order, and the single run their rows share.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BucketRun {
    /// Sorted by key, so an object is found by binary search.
    objects: Vec<ObjectRun>,
    /// Grouped by object in `objects` order, and sorted by timestamp inside each group.
    rows: Vec<(u64, ElementEntry)>,
}

/// What an insert did, so a caller and a guard can both see it without a global counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct InsertOutcome {
    /// Whether a row for that timestamp was already there and was overwritten.
    pub(super) replaced: bool,
    /// Rows shifted to make room. Zero when the row landed past the end of its bucket, which is
    /// what an ascending timestamp does. Reported rather than inferred, because the write cost of
    /// this shape is the question #2080 and #2081 exist to answer and a guard should read it
    /// directly.
    pub(super) rows_moved: usize,
}

/// A timestamped series map grouped by routing bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct GroupedSeriesIndex {
    buckets: BTreeMap<u32, BucketRun>,
    /// The range `buckets` is keyed on. `(0, u32::MAX)` until told otherwise, which is what a
    /// decoded container has and what `ShardState::routing_range` answers before install.
    start_routing_bucket: u32,
    end_routing_bucket: u32,
}

impl GroupedSeriesIndex {
    /// An empty index on the whole keyspace, which is what an unstamped state routes on.
    pub(super) fn new() -> Self {
        GroupedSeriesIndex {
            buckets: BTreeMap::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
        }
    }

    /// The range this index is currently grouped on.
    pub(super) fn routing_range(&self) -> (u32, u32) {
        (self.start_routing_bucket, self.end_routing_bucket)
    }

    fn bucket_of(&self, key: &str) -> u32 {
        crate::engine::hashing::block_routing_bucket(
            key,
            self.start_routing_bucket,
            self.end_routing_bucket,
        )
    }

    /// How many OBJECTS the index holds. Named `object_count` rather than `len` on purpose: this
    /// container has two plausible lengths and a bare `len` would be read as whichever the caller
    /// happened to mean. The sibling is [`Self::point_count`].
    pub(super) fn object_count(&self) -> usize {
        self.buckets.values().map(|bucket| bucket.objects.len()).sum()
    }

    /// How many POINTS the index holds, across every object.
    pub(super) fn point_count(&self) -> usize {
        self.buckets.values().map(|bucket| bucket.rows.len()).sum()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.buckets.values().all(|bucket| bucket.objects.is_empty())
    }

    /// Where `key`'s run is: its bucket, and its position in that bucket's `objects`.
    fn locate(&self, key: &str) -> Option<(u32, usize)> {
        let bucket_id = self.bucket_of(key);
        let bucket = self.buckets.get(&bucket_id)?;
        let at = bucket
            .objects
            .binary_search_by(|object| object.key.as_ref().cmp(key))
            .ok()?;
        Some((bucket_id, at))
    }

    pub(super) fn contains_key(&self, key: &str) -> bool {
        self.locate(key).is_some()
    }

    /// One object's rows, as a contiguous slice. The slice IS the storage -- no copy, no
    /// materialised map.
    fn run(&self, key: &str) -> Option<&[(u64, ElementEntry)]> {
        let (bucket_id, at) = self.locate(key)?;
        let bucket = self.buckets.get(&bucket_id)?;
        let object = &bucket.objects[at];
        Some(&bucket.rows[object.start as usize..(object.start + object.len) as usize])
    }

    /// How many points `key` holds, or zero if it holds none.
    pub(super) fn series_len(&self, key: &str) -> usize {
        self.run(key).map(|run| run.len()).unwrap_or(0)
    }

    /// One point, by its timestamp. TWO BINARY SEARCHES AND NO WALK: the objects array for the key,
    /// then the object's own run for the timestamp.
    pub(super) fn get_point(&self, key: &str, at: u64) -> Option<&ElementEntry> {
        let run = self.run(key)?;
        let index = run.binary_search_by(|(existing, _)| existing.cmp(&at)).ok()?;
        Some(&run[index].1)
    }

    /// Every point of one object, in timestamp order.
    pub(super) fn points(&self, key: &str) -> impl Iterator<Item = (u64, &ElementEntry)> + '_ {
        self.run(key)
            .unwrap_or(&[])
            .iter()
            .map(|(at, address)| (*at, address))
    }

    /// One object's points within an inclusive-exclusive timestamp window, in order.
    ///
    /// Bounded by binary search at both ends rather than by filtering the run, so a narrow window
    /// over a long series does not read the series.
    pub(super) fn range(
        &self,
        key: &str,
        start_at: u64,
        end_at: u64,
    ) -> impl Iterator<Item = (u64, &ElementEntry)> + '_ {
        let run = self.run(key).unwrap_or(&[]);
        let from = run.partition_point(|(at, _)| *at < start_at);
        let to = run.partition_point(|(at, _)| *at < end_at);
        run[from..to].iter().map(|(at, address)| (*at, address))
    }

    /// Every object's key, in no particular order across buckets but sorted within each.
    pub(super) fn objects(&self) -> impl Iterator<Item = &str> + '_ {
        self.buckets
            .values()
            .flat_map(|bucket| bucket.objects.iter().map(|object| object.key.as_ref()))
    }

    /// Every object with its points, for the callers that walk the whole map.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, &[(u64, ElementEntry)])> + '_ {
        self.buckets.values().flat_map(|bucket| {
            bucket.objects.iter().map(move |object| {
                (
                    object.key.as_ref(),
                    &bucket.rows[object.start as usize..(object.start + object.len) as usize],
                )
            })
        })
    }

    /// Insert or overwrite one point.
    ///
    /// A row lands inside its own object's run, in timestamp order. Everything after it in the
    /// bucket moves, and every later object's start moves with it -- which is why an ASCENDING
    /// timestamp is the cheap case: it lands at the end of its own run, and only objects sorting
    /// after it in the same bucket shift. #2080 measured that at 12.4 rows moved an insert at the
    /// sparsest occupancy falling to 0.0 at the densest; #2081 measured the opposite landing, a left
    /// push, at 499.5 and growing with the key's length, which is why a list map is not given this
    /// shape.
    pub(super) fn insert(&mut self, key: &str, at: u64, address: ElementEntry) -> InsertOutcome {
        let bucket_id = self.bucket_of(key);
        let bucket = self.buckets.entry(bucket_id).or_default();
        match bucket
            .objects
            .binary_search_by(|object| object.key.as_ref().cmp(key))
        {
            Ok(object_at) => {
                let (start, len) = {
                    let object = &bucket.objects[object_at];
                    (object.start as usize, object.len as usize)
                };
                let run = &bucket.rows[start..start + len];
                match run.binary_search_by(|(existing, _)| existing.cmp(&at)) {
                    Ok(inner) => {
                        // Same timestamp: overwrite in place. Nothing moves, and nothing about the
                        // objects array changes.
                        bucket.rows[start + inner].1 = address;
                        InsertOutcome { replaced: true, rows_moved: 0 }
                    }
                    Err(inner) => {
                        let index = start + inner;
                        let moved = bucket.rows.len() - index;
                        bucket.rows.insert(index, (at, address));
                        bucket.objects[object_at].len += 1;
                        for later in bucket.objects[object_at + 1..].iter_mut() {
                            later.start += 1;
                        }
                        InsertOutcome { replaced: false, rows_moved: moved }
                    }
                }
            }
            Err(object_at) => {
                // A new object. Its run begins where the next object's run begins, or at the end.
                let index = bucket
                    .objects
                    .get(object_at)
                    .map(|object| object.start as usize)
                    .unwrap_or(bucket.rows.len());
                let moved = bucket.rows.len() - index;
                bucket.rows.insert(index, (at, address));
                bucket.objects.insert(
                    object_at,
                    ObjectRun { key: Arc::from(key), start: index as u32, len: 1 },
                );
                for later in bucket.objects[object_at + 1..].iter_mut() {
                    later.start += 1;
                }
                InsertOutcome { replaced: false, rows_moved: moved }
            }
        }
    }

    /// Drop one point. Returns whether it was there.
    pub(super) fn remove_point(&mut self, key: &str, at: u64) -> bool {
        let Some((bucket_id, object_at)) = self.locate(key) else {
            return false;
        };
        let bucket = self.buckets.get_mut(&bucket_id).expect("located above");
        let (start, len) = {
            let object = &bucket.objects[object_at];
            (object.start as usize, object.len as usize)
        };
        let run = &bucket.rows[start..start + len];
        let Ok(inner) = run.binary_search_by(|(existing, _)| existing.cmp(&at)) else {
            return false;
        };
        bucket.rows.remove(start + inner);
        bucket.objects[object_at].len -= 1;
        if bucket.objects[object_at].len == 0 {
            bucket.objects.remove(object_at);
        }
        for later in bucket.objects[object_at..].iter_mut() {
            if later.start as usize > start {
                later.start -= 1;
            }
        }
        true
    }

    /// Drop a whole object and every point it holds. Returns whether it was there.
    pub(super) fn remove_object(&mut self, key: &str) -> bool {
        let Some((bucket_id, object_at)) = self.locate(key) else {
            return false;
        };
        let bucket = self.buckets.get_mut(&bucket_id).expect("located above");
        let object = bucket.objects.remove(object_at);
        let start = object.start as usize;
        let len = object.len as usize;
        bucket.rows.drain(start..start + len);
        for later in bucket.objects[object_at..].iter_mut() {
            later.start -= object.len;
        }
        if bucket.objects.is_empty() {
            // A bucket that empties is dropped rather than kept as an empty entry, for the reason
            // the dirty index records about its own sets: an empty container held forever is a node
            // paid for nothing.
            self.buckets.remove(&bucket_id);
        }
        true
    }

    /// Regroup every object onto `start..=end`.
    ///
    /// THIS IS WHAT RIDES INSTALL. A decoded container is grouped on `(0, u32::MAX)`, where every key
    /// has a bucket to itself and the amortisation this shape exists for is absent. The range becomes
    /// known in `install_shard_state`, and this is the one O(n) pass that moves the objects onto it.
    /// Calling it with the range already in force is a no-op beyond the walk.
    pub(super) fn rebucket(&mut self, start_routing_bucket: u32, end_routing_bucket: u32) {
        if (start_routing_bucket, end_routing_bucket) == self.routing_range()
            && !self.buckets.is_empty()
        {
            return;
        }
        let drained = std::mem::take(&mut self.buckets);
        self.start_routing_bucket = start_routing_bucket;
        self.end_routing_bucket = end_routing_bucket;
        // Collect every object once, then rebuild bucket by bucket so each bucket's run is built in
        // one piece rather than grown by insert -- the same reason a decoded B-tree is repacked
        // rather than refilled.
        let mut by_bucket: BTreeMap<u32, Vec<(Arc<str>, Vec<(u64, ElementEntry)>)>> =
            BTreeMap::new();
        for (_, bucket) in drained {
            let BucketRun { objects, rows } = bucket;
            for object in objects {
                let start = object.start as usize;
                let len = object.len as usize;
                let points = rows[start..start + len].to_vec();
                let bucket_id = self.bucket_of(object.key.as_ref());
                by_bucket.entry(bucket_id).or_default().push((object.key, points));
            }
        }
        for (bucket_id, mut objects) in by_bucket {
            objects.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
            let total: usize = objects.iter().map(|(_, points)| points.len()).sum();
            let mut built = BucketRun {
                objects: Vec::with_capacity(objects.len()),
                rows: Vec::with_capacity(total),
            };
            for (key, points) in objects {
                let start = built.rows.len() as u32;
                let len = points.len() as u32;
                built.rows.extend(points);
                built.objects.push(ObjectRun { key, start, len });
            }
            self.buckets.insert(bucket_id, built);
        }
    }

    /// The historical shape, materialised. Used by the wire adapter and by nothing else -- a caller
    /// that wants an object's points should ask for them rather than build this.
    fn to_plain(&self) -> BTreeMap<&str, BTreeMap<u64, &ElementEntry>> {
        self.iter()
            .map(|(key, run)| {
                (
                    key,
                    run.iter().map(|(at, address)| (*at, address)).collect(),
                )
            })
            .collect()
    }

    /// Build from the historical shape, on the whole keyspace.
    ///
    /// Grouped on whatever range the fresh index carries -- the whole keyspace, because that is what
    /// `ShardState::routing_range` answers before install -- and [`Self::rebucket`] is what moves it
    /// onto the real one.
    ///
    /// IT FILES THROUGH [`Self::bucket_of`] RATHER THAN SPELLING THE RANGE, and that is not a way
    /// around `every_site_that_hard_codes_the_whole_routing_range_is_accounted_for`; it is what that
    /// guard asks for. Its own record says the sites that merely ATTRIBUTE an already-filed page keep
    /// their literal as a last resort, while the two that PLACE a page left the list by reading the
    /// shard's carried range instead -- a placed bucket has no filing to fall back to, so the range
    /// itself had to change. This places, so it reads the range this container carries, which
    /// `rebucket` can correct. Spelling `0, u32::MAX` here would have been a bucket no caller could
    /// put right.
    fn from_plain(plain: HashMap<String, BTreeMap<u64, ElementEntry>>) -> Self {
        let mut index = GroupedSeriesIndex::new();
        let mut by_bucket: BTreeMap<u32, Vec<(Arc<str>, Vec<(u64, ElementEntry)>)>> =
            BTreeMap::new();
        for (key, series) in plain {
            let bucket_id = index.bucket_of(&key);
            by_bucket
                .entry(bucket_id)
                .or_default()
                .push((Arc::from(key), series.into_iter().collect()));
        }
        for (bucket_id, mut objects) in by_bucket {
            objects.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
            let total: usize = objects.iter().map(|(_, points)| points.len()).sum();
            let mut built = BucketRun {
                objects: Vec::with_capacity(objects.len()),
                rows: Vec::with_capacity(total),
            };
            for (key, points) in objects {
                let start = built.rows.len() as u32;
                let len = points.len() as u32;
                built.rows.extend(points);
                built.objects.push(ObjectRun { key, start, len });
            }
            index.buckets.insert(bucket_id, built);
        }
        index
    }
}

// =================================================================================================
// THE WIRE
// =================================================================================================

/// Serialize as the historical shape: a map of object key to a map of timestamp to address.
///
/// THE WIRE DOES NOT LEARN ABOUT THE BUCKETING. The grouping is a resident arrangement, and the
/// routing range it is grouped on is not even known when the bytes are read -- so putting a bucket
/// into the encoding would store a number the decoder cannot check and does not need. This is the
/// `set_index_serde` pattern: the in-memory shape changes and the encoding does not.
impl Serialize for GroupedSeriesIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_plain().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GroupedSeriesIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let plain = HashMap::<String, BTreeMap<u64, ElementEntry>>::deserialize(deserializer)?;
        Ok(GroupedSeriesIndex::from_plain(plain))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(id: u64) -> ElementEntry {
        ElementEntry::from_parts(id, 0, 64, Some(1), Some(id))
    }

    /// The historical shape, for comparing against.
    fn plain(entries: &[(&str, &[u64])]) -> HashMap<String, BTreeMap<u64, ElementEntry>> {
        entries
            .iter()
            .map(|(key, times)| {
                (
                    (*key).to_string(),
                    times.iter().map(|at| (*at, address(*at))).collect(),
                )
            })
            .collect()
    }

    fn filled(entries: &[(&str, &[u64])]) -> GroupedSeriesIndex {
        let mut index = GroupedSeriesIndex::new();
        for (key, times) in entries {
            for at in *times {
                index.insert(key, *at, address(*at));
            }
        }
        index
    }

    /// Every object's points come back in timestamp order whatever order they went in, and the two
    /// counts mean what they say.
    #[test]
    fn it_holds_what_was_put_in_and_answers_in_order() {
        let mut index = GroupedSeriesIndex::new();
        // Deliberately out of order, and across several keys, so ordering is the container's job
        // rather than the caller's.
        for (key, at) in [
            ("b", 30u64), ("a", 20), ("b", 10), ("a", 10), ("c", 99), ("a", 30), ("b", 20),
        ] {
            index.insert(key, at, address(at));
        }
        assert_eq!(3, index.object_count(), "three distinct objects went in");
        assert_eq!(7, index.point_count(), "seven points went in");
        assert_eq!(3, index.series_len("a"));
        assert_eq!(3, index.series_len("b"));
        assert_eq!(1, index.series_len("c"));
        assert_eq!(0, index.series_len("absent"), "a key never written holds nothing");
        for key in ["a", "b", "c"] {
            let times: Vec<u64> = index.points(key).map(|(at, _)| at).collect();
            let mut sorted = times.clone();
            sorted.sort_unstable();
            assert_eq!(sorted, times, "{key} must come back in timestamp order");
        }
        assert_eq!(Some(&address(20)), index.get_point("a", 20));
        assert_eq!(None, index.get_point("a", 21), "a timestamp never written is absent");
        assert_eq!(None, index.get_point("absent", 20), "a key never written is absent");
    }

    /// The window is bounded at both ends, and its edges are the ones a range read expects.
    #[test]
    fn a_window_takes_the_points_inside_it_and_no_others() {
        let index = filled(&[("k", &[10, 20, 30, 40, 50])]);
        let got: Vec<u64> = index.range("k", 20, 40).map(|(at, _)| at).collect();
        assert_eq!(vec![20, 30], got, "start is inclusive and end is exclusive");
        assert!(index.range("k", 0, 10).next().is_none(), "a window below every point is empty");
        assert!(index.range("k", 51, 99).next().is_none(), "a window above every point is empty");
        assert_eq!(5, index.range("k", 0, u64::MAX).count(), "the whole window is every point");
        assert!(index.range("absent", 0, u64::MAX).next().is_none());
    }

    /// Overwriting a timestamp replaces its address and moves nothing.
    #[test]
    fn the_same_timestamp_twice_is_one_point() {
        let mut index = filled(&[("k", &[10, 20])]);
        let outcome = index.insert("k", 20, address(777));
        assert!(outcome.replaced, "the second write at 20 must report it replaced");
        assert_eq!(0, outcome.rows_moved, "an overwrite in place moves nothing");
        assert_eq!(2, index.series_len("k"), "and does not add a point");
        assert_eq!(Some(&address(777)), index.get_point("k", 20));
    }

    /// A removal leaves every other object's rows reachable -- which is the invariant a shared run
    /// makes possible to break, since one object's removal shifts another object's start.
    #[test]
    fn a_removal_leaves_every_other_object_intact() {
        let mut index = filled(&[("a", &[1, 2, 3]), ("b", &[4, 5]), ("c", &[6])]);
        let before = index.point_count();
        assert!(index.remove_object("b"), "b was there");
        assert!(!index.remove_object("b"), "and is not there twice");
        assert_eq!(before - 2, index.point_count());
        assert_eq!(2, index.object_count());
        assert_eq!(vec![1, 2, 3], index.points("a").map(|(at, _)| at).collect::<Vec<_>>());
        assert_eq!(vec![6], index.points("c").map(|(at, _)| at).collect::<Vec<_>>());
        assert_eq!(Some(&address(6)), index.get_point("c", 6), "c is still addressable");
        assert!(index.remove_point("a", 2));
        assert!(!index.remove_point("a", 2), "and not twice");
        assert_eq!(vec![1, 3], index.points("a").map(|(at, _)| at).collect::<Vec<_>>());
        assert_eq!(Some(&address(6)), index.get_point("c", 6), "still addressable after a point go");
        // An object whose last point goes stops being an object.
        assert!(index.remove_point("c", 6));
        assert!(!index.contains_key("c"), "an object with no points is not an object");
    }

    /// Regrouping onto a narrower range keeps every point and finds every one of them afterwards.
    ///
    /// This is the pass that rides install, and the thing it could get wrong is losing an object
    /// while moving it -- so the check is that the whole contents are equal before and after, not
    /// merely that the counts are.
    #[test]
    fn regrouping_onto_the_shipped_range_keeps_every_point_findable() {
        // THREE THOUSAND objects onto 1,024 buckets, so sharing is guaranteed by PIGEONHOLE rather
        // than by how a particular hash happens to spread a particular set of keys. The first
        // version of this test used 200 and asserted "fewer than 200 buckets"; it failed, and it
        // failed because it was asserting a property of the hash function on those 200 names, not a
        // property of the regroup. A count that can only be met one way is the better guard.
        let entries: Vec<(String, Vec<u64>)> = (0..3_000)
            .map(|k| (format!("key{k}"), vec![10u64, 20, 30]))
            .collect();
        let mut index = GroupedSeriesIndex::new();
        for (key, times) in &entries {
            for at in times {
                index.insert(key, *at, address(*at));
            }
        }
        assert_eq!((0, u32::MAX), index.routing_range(), "a fresh index is on the whole keyspace");
        let before: BTreeMap<String, Vec<u64>> = index
            .iter()
            .map(|(key, run)| (key.to_string(), run.iter().map(|(at, _)| *at).collect()))
            .collect();
        assert_eq!(3_000, before.len(), "denominator: three thousand objects before");
        let wide_ids: Vec<u32> = index.buckets.keys().copied().collect();
        assert!(!wide_ids.is_empty(), "denominator: there are buckets before the regroup");

        index.rebucket(0, 1023);
        assert_eq!((0, 1023), index.routing_range());
        let after: BTreeMap<String, Vec<u64>> = index
            .iter()
            .map(|(key, run)| (key.to_string(), run.iter().map(|(at, _)| *at).collect()))
            .collect();
        assert_eq!(before, after, "regrouping must not change one point of the contents");
        for (key, times) in &entries {
            for at in times {
                assert_eq!(
                    Some(&address(*at)),
                    index.get_point(key, *at),
                    "{key} at {at} must still be findable after the regroup"
                );
            }
        }
        // AND IT MUST ACTUALLY HAVE GROUPED, checked two ways that do not depend on the hash.
        //
        // First, every bucket id must now be INSIDE the new range. Before the regroup the ids are
        // spread over the whole u32 keyspace, so this is the direct witness that the objects moved
        // onto `0..=1023` rather than merely surviving.
        assert!(
            index.buckets.keys().all(|bucket| *bucket <= 1023),
            "an object is still filed outside 0..=1023 after regrouping onto it: {:?}",
            index.buckets.keys().filter(|b| **b > 1023).take(4).collect::<Vec<_>>()
        );
        assert!(
            wide_ids.iter().any(|bucket| *bucket > 1023),
            "no object was filed above 1023 BEFORE the regroup, so the check above would pass on a \
             no-op and proves nothing"
        );
        // Second, they must share: 3,000 objects cannot occupy more than 1,024 buckets, so this
        // holds by pigeonhole whatever the hash does.
        assert!(
            index.buckets.len() <= 1024,
            "3,000 objects on 1,024 buckets occupy {} of them, which is more than exist",
            index.buckets.len()
        );
        assert!(
            index.buckets.len() < 3_000,
            "3,000 objects still occupy {} buckets, so they are not sharing",
            index.buckets.len()
        );
        assert!(index.buckets.len() > 1, "and they must not all have collapsed into one");
    }

    /// The wire is the historical shape, in both directions, and the comparison can see a planted
    /// difference.
    #[test]
    fn it_encodes_and_decodes_as_the_shape_it_replaces() {
        let entries: &[(&str, &[u64])] = &[("a", &[1, 2, 3]), ("b", &[7]), ("c", &[4, 5])];
        let index = filled(entries);
        let expected = plain(entries);

        // Encoding this container and decoding it as the OLD type must give the old value.
        let bytes = serde_json::to_vec(&index).expect("the container serializes");
        let as_old: HashMap<String, BTreeMap<u64, ElementEntry>> =
            serde_json::from_slice(&bytes).expect("and decodes as the shape it replaces");
        assert_eq!(expected, as_old, "the encoding is not the historical value");

        // And the other direction: the OLD type's bytes must decode into this container.
        let old_bytes = serde_json::to_vec(&expected).expect("the old shape serializes");
        let back: GroupedSeriesIndex =
            serde_json::from_slice(&old_bytes).expect("old bytes decode into the container");
        assert_eq!(index, back, "a round trip through the historical bytes is not the identity");

        // THE CONTROL. If the comparison above cannot see a difference, its agreement means nothing.
        let mut planted = expected.clone();
        planted
            .get_mut("b")
            .expect("b is there")
            .insert(999, address(999));
        assert_ne!(
            planted, as_old,
            "the comparison cannot detect a planted extra point, so it proves nothing about the wire"
        );
        let planted_bytes = serde_json::to_vec(&planted).expect("serializes");
        let planted_back: GroupedSeriesIndex =
            serde_json::from_slice(&planted_bytes).expect("decodes");
        assert_ne!(
            index, planted_back,
            "the container comparison cannot detect a planted extra point either"
        );
    }

    /// An ascending timestamp lands at the end of its own run, and a descending one does not.
    ///
    /// This is #2080's and #2081's result restated against the real container rather than against a
    /// model of it: the shape is cheap to append to and dear to prepend to, and the two landings are
    /// what decide which maps can take it.
    #[test]
    fn an_ascending_write_moves_less_than_a_descending_one() {
        const POINTS: u64 = 200;
        let mut ascending = GroupedSeriesIndex::new();
        let mut ascending_moves = 0usize;
        for at in 0..POINTS {
            ascending_moves += ascending.insert("k", 1_000 + at, address(at)).rows_moved;
        }
        let mut descending = GroupedSeriesIndex::new();
        let mut descending_moves = 0usize;
        for at in 0..POINTS {
            descending_moves += descending.insert("k", 1_000 - at, address(at)).rows_moved;
        }
        println!(
            "over {POINTS} writes to one object: ascending moved {ascending_moves} rows, \
             descending moved {descending_moves}"
        );
        assert_eq!(POINTS as usize, ascending.series_len("k"));
        assert_eq!(POINTS as usize, descending.series_len("k"));
        assert_eq!(
            0, ascending_moves,
            "an ascending timestamp lands past the end of its run, so nothing should move"
        );
        // n(n-1)/2 by construction: prepending to a run of length i moves i rows.
        let expected = (POINTS as usize * (POINTS as usize - 1)) / 2;
        assert_eq!(
            expected, descending_moves,
            "prepending {POINTS} times should move n(n-1)/2 rows"
        );
        assert!(
            descending_moves > ascending_moves,
            "the two landings are supposed to differ, and this fixture is not seeing it"
        );
    }

    /// An empty index divides by nothing and claims nothing.
    #[test]
    fn an_empty_index_is_empty_rather_than_absent() {
        let index = GroupedSeriesIndex::new();
        assert!(index.is_empty());
        assert_eq!(0, index.object_count());
        assert_eq!(0, index.point_count());
        assert_eq!(0, index.objects().count());
        assert_eq!(0, index.iter().count());
        assert!(!index.contains_key("anything"));
        assert_eq!(None, index.get_point("anything", 0));
        // And it survives being told a range, which install does unconditionally.
        let mut index = index;
        index.rebucket(0, 1023);
        assert!(index.is_empty(), "an empty index regroups to an empty index");
        assert_eq!((0, 1023), index.routing_range());
    }
}
