// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Hierarchical server locations.
//!
//! A server's `location` is a plain `String` that the metaserver only ever
//! compares for exact equality. Real deployments write a path into it —
//! `us-east/dc1/az1/rack7` — and the metaserver reads that as one opaque token,
//! which costs correctness in two places.
//!
//! **Replica spread happens at the wrong granularity.** [`build_shards`] refuses
//! to place two replicas of a shard in the same `location`, comparing whole
//! strings. `us-east/dc1/az1/rack1` and `us-east/dc1/az1/rack2` are different
//! strings, so both are accepted — and the shard ends up with two replicas in
//! one availability unit. Losing that unit loses both. The check looks like it
//! is spreading across failure domains while it is only spreading across the
//! deepest one.
//!
//! **A table can only be pinned to exactly one location.** A `preferred_location`
//! is matched by string equality, so "keep this table in dc1" is not
//! expressible: you can name one rack, or nothing.
//!
//! [`Location`] parses the string into its `/`-separated levels, coarsest first,
//! and supplies the two comparisons those cases need:
//!
//! * [`Location::belongs_to`] — prefix containment. A preference naming fewer
//!   levels matches every location beneath it, so `dc1` covers every rack in it.
//! * [`Location::shared_prefix_len`] — how much of a failure domain two
//!   locations have in common, which is what lets replica placement prefer the
//!   *widest* separation available instead of merely a different leaf.
//!
//! A location with no separator has a single level, so a deployment using flat
//! tags (`zone-a`, `zone-b`) behaves exactly as it does today: different strings
//! differ at level 0, and neither is a prefix of the other.
//!
//! This generalises the reference's fixed four-field location
//! (region / datacenter / availability unit / tag) to a path of any depth. The
//! reference compares its last field exactly and the earlier ones only when the
//! pattern sets them; a variable-depth path expresses the same intent as plain
//! prefix matching, and does not force every deployment onto exactly four
//! levels.

use super::*;

/// A parsed server location: `/`-separated levels, coarsest first.
///
/// Empty segments are dropped, so leading, trailing and doubled separators are
/// tolerated — an operator writing `/dc1/az1/` means the same thing as
/// `dc1/az1`.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Location {
    levels: Vec<String>,
}

impl Location {
    /// Parse a location string into its levels.
    pub fn parse(raw: &str) -> Self {
        Self {
            levels: raw
                .split('/')
                .map(str::trim)
                .filter(|level| !level.is_empty())
                .map(str::to_string)
                .collect(),
        }
    }

    /// True when no level was declared at all.
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// How many levels this location names.
    pub fn depth(&self) -> usize {
        self.levels.len()
    }

    pub fn levels(&self) -> &[String] {
        &self.levels
    }

    /// The `depth`-level prefix of this location, or the whole thing when it is
    /// shallower. Used to name the failure domain a server sits in at a given
    /// granularity.
    pub fn ancestor(&self, depth: usize) -> Location {
        Location {
            levels: self.levels.iter().take(depth).cloned().collect(),
        }
    }

    /// True when `self` sits at or beneath `pattern`.
    ///
    /// An empty pattern matches everything ("anywhere"), and a pattern naming
    /// fewer levels than `self` matches every location beneath it — so a table
    /// pinned to `dc1` accepts a server in `dc1/az2/rack9`. A pattern deeper
    /// than `self` never matches: `dc1` does not belong to `dc1/az1`.
    pub fn belongs_to(&self, pattern: &Location) -> bool {
        if pattern.levels.len() > self.levels.len() {
            return false;
        }
        pattern
            .levels
            .iter()
            .zip(self.levels.iter())
            .all(|(want, have)| want == have)
    }

    /// How many leading levels two locations share. Zero means they diverge at
    /// the coarsest level, which is the widest separation available.
    pub fn shared_prefix_len(&self, other: &Location) -> usize {
        self.levels
            .iter()
            .zip(other.levels.iter())
            .take_while(|(left, right)| left == right)
            .count()
    }

    /// Render back to the canonical `a/b/c` form.
    pub fn to_path(&self) -> String {
        self.levels.join("/")
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_path())
    }
}

/// Does `candidate` diverge from everything in `placed` within the first
/// `separation` levels?
///
/// `separation` is how far down the hierarchy the divergence must occur, so
/// *smaller is stricter*: 1 demands a different top-level domain, while 4
/// accepts any difference in the first four levels. Placement walks it upward,
/// taking the widest separation the topology can actually provide.
pub fn separated_from(placed: &[Location], candidate: &Location, separation: usize) -> bool {
    if candidate.is_empty() {
        // A server that declares no location cannot be reasoned about; leave the
        // decision to the host and identity checks the caller also applies.
        return true;
    }
    placed.iter().all(|existing| {
        if existing.is_empty() {
            return true;
        }
        let shared = existing.shared_prefix_len(candidate);
        // Two identical locations are the same failure domain at every
        // granularity, so they conflict however weak the requirement is.
        if shared == existing.depth() && shared == candidate.depth() {
            return false;
        }
        shared < separation
    })
}

/// The separation levels to try when placing replicas, widest first.
///
/// Placement walks these in order and takes the first level at which a candidate
/// is acceptable, so replicas are spread as far apart as the topology allows
/// rather than merely onto a different leaf. Always yields at least one rung, so
/// a caller with no locations at all still has a pass to run.
pub fn separation_ladder(locations: &[Location]) -> Vec<usize> {
    let deepest = locations
        .iter()
        .map(Location::depth)
        .max()
        .unwrap_or_default();
    (1..=deepest.max(1)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(raw: &str) -> Location {
        Location::parse(raw)
    }

    #[test]
    fn a_path_parses_into_levels_coarsest_first() {
        let parsed = loc("us-east/dc1/az1/rack7");
        assert_eq!(parsed.depth(), 4);
        assert_eq!(parsed.levels(), ["us-east", "dc1", "az1", "rack7"]);
        assert_eq!(parsed.to_path(), "us-east/dc1/az1/rack7");
    }

    #[test]
    fn separators_are_forgiving() {
        assert_eq!(loc("/dc1//az1/ ").levels(), ["dc1", "az1"]);
        assert!(loc("").is_empty());
        assert!(loc("///").is_empty());
    }

    #[test]
    fn a_flat_tag_is_a_single_level() {
        // The behaviour existing deployments already rely on: two flat tags are
        // simply different, and neither contains the other.
        let a = loc("zone-a");
        let b = loc("zone-b");
        assert_eq!(a.depth(), 1);
        assert!(!a.belongs_to(&b));
        assert!(!b.belongs_to(&a));
        assert_eq!(a.shared_prefix_len(&b), 0);
    }

    #[test]
    fn a_shallower_pattern_matches_everything_beneath_it() {
        // The point of the hierarchy: "keep this table in dc1" becomes
        // expressible, instead of having to name one exact rack.
        let server = loc("us-east/dc1/az1/rack7");
        assert!(server.belongs_to(&loc("us-east")));
        assert!(server.belongs_to(&loc("us-east/dc1")));
        assert!(server.belongs_to(&loc("us-east/dc1/az1")));
        assert!(server.belongs_to(&loc("us-east/dc1/az1/rack7")));
        assert!(!server.belongs_to(&loc("us-east/dc2")));
        assert!(!server.belongs_to(&loc("us-west")));
    }

    #[test]
    fn an_empty_pattern_means_anywhere() {
        assert!(loc("us-east/dc1").belongs_to(&loc("")));
        assert!(loc("").belongs_to(&loc("")));
    }

    #[test]
    fn a_deeper_pattern_never_matches_a_shallower_location() {
        // A server that only declares `dc1` is not known to be in az1.
        assert!(!loc("us-east/dc1").belongs_to(&loc("us-east/dc1/az1")));
    }

    #[test]
    fn shared_prefix_measures_the_common_failure_domain() {
        assert_eq!(
            loc("us-east/dc1/az1/rack1").shared_prefix_len(&loc("us-east/dc1/az1/rack2")),
            3
        );
        assert_eq!(
            loc("us-east/dc1/az1/rack1").shared_prefix_len(&loc("us-east/dc1/az2/rack1")),
            2
        );
        assert_eq!(
            loc("us-east/dc1/az1").shared_prefix_len(&loc("us-west/dc9/az9")),
            0
        );
    }

    #[test]
    fn separation_prefers_the_widest_split_available() {
        let placed = vec![loc("us-east/dc1/az1/rack1")];
        // A different region is separated at every level.
        assert!(separated_from(&placed, &loc("us-west/dc9/az9/rack9"), 1));
        // A different availability unit is not separated at the region level...
        assert!(!separated_from(&placed, &loc("us-east/dc1/az2/rack1"), 1));
        // ...but is at the availability-unit level.
        assert!(separated_from(&placed, &loc("us-east/dc1/az2/rack1"), 3));
    }

    #[test]
    fn two_racks_in_one_availability_unit_are_not_separated_at_that_level() {
        // The bug this exists to prevent: these are different strings, so the
        // old whole-string check accepted both and put two replicas in one unit.
        let placed = vec![loc("us-east/dc1/az1/rack1")];
        assert!(!separated_from(&placed, &loc("us-east/dc1/az1/rack2"), 3));
        // At the deepest level they are distinguishable, which is the fallback.
        assert!(separated_from(&placed, &loc("us-east/dc1/az1/rack2"), 4));
    }

    #[test]
    fn an_identical_location_is_never_separated() {
        let placed = vec![loc("us-east/dc1/az1/rack1")];
        for separation in 1..=6 {
            assert!(
                !separated_from(&placed, &loc("us-east/dc1/az1/rack1"), separation),
                "identical locations must conflict at separation {separation}"
            );
        }
    }

    #[test]
    fn flat_tags_still_separate_at_every_level() {
        // Existing deployments must be unaffected.
        let placed = vec![loc("zone-a")];
        assert!(separated_from(&placed, &loc("zone-b"), 1));
        assert!(!separated_from(&placed, &loc("zone-a"), 1));
    }

    #[test]
    fn a_server_without_a_location_never_blocks_placement() {
        let placed = vec![loc(""), loc("us-east/dc1")];
        assert!(separated_from(&placed, &loc(""), 1));
        assert!(separated_from(&placed, &loc("us-west/dc2"), 1));
    }

    #[test]
    fn the_ladder_walks_from_widest_to_narrowest() {
        // Smaller separation is stricter, so widest-first means ascending.
        let locations = vec![loc("us-east/dc1/az1/rack1"), loc("us-east/dc1")];
        assert_eq!(separation_ladder(&locations), vec![1, 2, 3, 4]);
        // Flat tags give a single rung.
        assert_eq!(separation_ladder(&[loc("zone-a")]), vec![1]);
        // No locations at all still yields one rung rather than none.
        assert_eq!(separation_ladder(&[]), vec![1]);
    }

    #[test]
    fn ancestor_names_the_domain_at_a_given_depth() {
        let server = loc("us-east/dc1/az1/rack7");
        assert_eq!(server.ancestor(0).to_path(), "");
        assert_eq!(server.ancestor(2).to_path(), "us-east/dc1");
        assert_eq!(server.ancestor(99).to_path(), "us-east/dc1/az1/rack7");
    }
}
