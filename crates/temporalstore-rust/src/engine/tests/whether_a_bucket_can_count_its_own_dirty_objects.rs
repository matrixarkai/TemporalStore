// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN A BUCKET COUNT ITS OWN DIRTY OBJECTS? The per-bucket count, derived two ways and compared.
//!
//! # WHAT THIS IS FOR
//!
//! `BucketStorageSummary::dirty_object_count` is populated from exactly one place -- the
//! `shard.dirty_objects.bucket_counts()` loop at the foot of `bucket_storage_summaries` -- and that
//! set is object-keyed and grows with the ingest. The dirty-BUCKET question has already stopped
//! needing it: `the_page_bit_answers_the_dirty_bucket_question` measures the bucket's own `dirty`
//! bit and its pages' bits answering it exactly. The obvious next step is to answer the per-bucket
//! COUNT the same way, from each bucket's own pages, and retire the set.
//!
//! This measures whether that is possible, and the answer is different for the two questions the
//! field is asked. THE COUNT IS NOT DERIVABLE. THE `> 0` IS, but not from the pages -- from the
//! `dirty` flag on the bucket's own node, which `mark_object_dirty` sets after CREATING the node
//! when the key has none. So a key with no page at all still leaves a node reading dirty, and the
//! three consumers that only ask `> 0` can move where the count cannot. Both results are measured
//! below on one shard.
//!
//! A COUNT OF DIRTY PAGES IS NOT A COUNT OF DIRTY OBJECTS, so the derived figure counts DISTINCT
//! objects having at least one dirty page in the bucket -- identity taken from
//! `PageKeysByPosition`, which is how a production derivation would have to take it, because a page
//! entry holds no identity of its own. The fixture is built so a page count and an object count are
//! DIFFERENT NUMBERS; otherwise this could not tell a correct derivation from a page tally.
//!
//! # WHAT IT MEASURES
//!
//!     48 keys on a 16-bucket routing range, dumped clean, then three re-dirtied and two removed
//!
//!       buckets with a count, via the SET        8     total via the SET       11
//!       buckets with a count, via the PAGES      3     total via the PAGES      3
//!                                                     total dirty PAGES        9
//!       set members with NO live page at all     8     widest object 3 pages
//!
//!       bucket        0   2   3   6   7  11  12  15
//!       set           1   1   1   1   3   2   1   1
//!       node.dirty()  t   t   t   t   t   t   t   t
//!       objects       0   0   1   0   1   0   0   1
//!
//! The page-derived figure reports 3 of the authority's 11 marks, and FIVE OF THE EIGHT DIRTY
//! BUCKETS report zero. Bucket 7 shows the shortfall is not confined to the buckets the page walk
//! cannot see at all: it holds three marks and the walk finds one, so this is not a factor that
//! could be corrected for.
//!
//! THE NODE FLAG, THOUGH, AGREES WITH THE SET ON EVERY ONE OF THE SIXTEEN BUCKETS -- in both
//! directions, and including the five that hold no page at all. That is asserted here, so the
//! three `> 0` consumers rest on a measurement rather than on this paragraph.
//!
//! # AND THE FLAG IS AN INDEPENDENT RECORD, NOT A CACHE OF THE SET
//!
//! That needed its own experiment, because two sites keep the flag in step with the set --
//! `refresh_one_bucket_runtime_flags` ORs in `dirty_objects.has_bucket(..)`, and the ownership
//! reconstruct ORs in `dirty_objects.contains(key)` -- so the agreement above could have been
//! circular, with the flag merely echoing the set on the very buckets that carry the result. The
//! `has_bucket` doc is itself a reason to suspect it: those OR terms exist because the index "is a
//! strict SUPERSET of what that fold can find", which is this same non-page-backed population.
//!
//! Six arms, each neutralising maintainers of the flag and re-running this test:
//!
//!     A  mark's own `set_dirty(true)` removed ................. PASSES
//!     B  the `has_bucket` OR neutralised ..................... PASSES
//!     C  the `contains` OR neutralised ....................... PASSES
//!     D  all three at once ................................... FAILS, buckets [0, 11, 12]
//!     E  BOTH set-derived ORs off, the mark KEPT .............. PASSES, no-page buckets intact
//!     F  both ORs off AND the mark off (= D) ................. FAILS, buckets [0, 11, 12]
//!
//! A, B and C passing alone says only that the three maintainers are REDUNDANT -- no single
//! removal can decide which record is authoritative, because the survivors cover. E is the arm that
//! decides it: with both set-derived terms gone, the agreement still holds on all sixteen buckets
//! and the five no-page buckets are still reported. So the mark alone maintains the flag, and F
//! differing from E is what shows E is isolating the mark rather than agreeing with nothing.
//!
//! The consequence is the useful part: the node's flag and the set are two records of the same
//! fact, and the flag is not derived from the set. So the `> 0` consumers can move to it and the
//! move survives the set being retired -- while the COUNT still cannot be answered from the bucket
//! at all.
//!
//! # WHY, AND BOTH REASONS WERE REACHED HERE
//!
//!   1. A DIRTY OBJECT NEED NOT HAVE A PAGE. A removal marks a `control_state:cpc:*` record
//!      alongside the data object, and control state is not page-backed -- so there is no `dirty`
//!      bit anywhere that could carry its dirtiness.
//!      `dirty_objects_versus_the_blocks_own_dirty_flags` measures the same thing from the other
//!      end, at 80 of 160. Eight of the eleven marks here are of this kind.
//!   2. THOSE RECORDS ROUTE TO BUCKETS OF THEIR OWN. `DirtyObjectIndex::insert` files a key under
//!      `block_routing_bucket(key, start, end)`, so a `control_state:cpc:*` record contributes 1 to
//!      ITS bucket -- which holds no page at all. That is where the five invisible buckets come
//!      from, and it is why the shortfall is not a uniform factor that could be corrected for.
//!
//! # WHY A PAGE-DERIVED COUNT IS A LOSS AND NOT A WOBBLE
//!
//! Dump selection filters on `summary.dirty_object_count > 0`. A bucket a page walk reads as zero
//! is a bucket that STOPS BEING DUMPED while it holds undumped marks, so the five buckets above are
//! unflushed state that no round would ever pick up. The figure is also a MAGNITUDE downstream --
//! the per-bucket gauge publishes it, and the summary is SERIALIZED into a dump manifest -- and
//! `bucket_storage_summaries` adds the count ONTO `summary.dirty_generation`, which
//! `bucket_dump_summary_matches_current_generation` compares EXACTLY when it validates a stored
//! dump manifest. So moving this count by one moves a durability comparison, not just a report
//! field. What that generation IS is asserted here as an identity -- `node + count` -- rather than
//! left as the observation that the two differ.
//!
//! # AND WHY THE TWO `> 0` DIRECTIONS ARE NOT INTERCHANGEABLE
//!
//! The three presence consumers do not tolerate the same error. The dump filter over-reporting
//! costs an extra dump. The two reclaim checks are the opposite: `dirty_object_count == 0` is the
//! arm that counts a bucket as covered and contributing no WAL floor, so a bucket wrongly reading
//! dirty skips it, cannot name a claim -- its `first_dirty_wal_sequence` was cleared alongside
//! `dirty = false` -- and blocks the whole reclaim plan, which is the regression recorded at that
//! site as 0 of 10,666 records freed. So the node-flag agreement is asserted in BOTH directions,
//! and the over-reporting direction is the one that licenses moving the reclaim checks.
//!
//! A report with non-triviality assertions, plus the clauses that pin the conclusion: the two
//! methods DISAGREE, and they disagree for the stated reason. If a later change makes them agree,
//! this fails and says so -- at which point the producer can move to the derivation.
//!
//! rust-internal: reads the engine's own index structures, no product behaviour

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;
const END_ROUTING_BUCKET: u32 = 15;

/// Per-bucket dirty-object counts the way the producer takes them: off the object-keyed set.
fn authority(shard: &crate::engine::state::ShardState) -> BTreeMap<u32, u64> {
    shard.dirty_objects.bucket_counts().collect()
}

/// Per-bucket counts derived from the buckets themselves: DISTINCT objects with a dirty page.
///
/// Identity comes from the object lookup, which is where a page's identity lives now. A page
/// entry carries `dirty` but no key, so a per-bucket walk cannot count objects without it.
fn derived_distinct_objects(shard: &crate::engine::state::ShardState) -> BTreeMap<u32, u64> {
    let mut per_bucket: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
    for (routing_bucket, _handle, object_key, page) in
        super::pages_with_identity(&shard.bucket_index)
    {
        if page.dirty {
            per_bucket
                .entry(routing_bucket)
                .or_default()
                .insert(object_key.to_string());
        }
    }
    per_bucket
        .into_iter()
        .map(|(bucket, keys)| (bucket, keys.len() as u64))
        .collect()
}

/// Per-bucket PRESENCE and generation, read off the NODE.
///
/// `mark_object_dirty` does four things, and two of them are here: it creates the routing bucket's
/// node when the key has none -- so even a key with no page at all gets a node -- then sets `dirty`
/// and bumps `dirty_generation`. That makes the node flag a candidate for the three consumers that
/// only ask `> 0`, which a page walk cannot serve.
fn node_state(shard: &crate::engine::state::ShardState) -> BTreeMap<u32, (bool, u64)> {
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(bucket, node)| (*bucket, (node.dirty(), node.dirty_generation)))
        .collect()
}

/// Per-bucket counts of dirty PAGES -- the number a careless derivation would produce.
fn derived_dirty_blocks(shard: &crate::engine::state::ShardState) -> BTreeMap<u32, u64> {
    let mut per_bucket: BTreeMap<u32, u64> = BTreeMap::new();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        let dirty = bucket.block_index.values().filter(|page| page.dirty).count() as u64;
        if dirty > 0 {
            per_bucket.insert(*routing_bucket, dirty);
        }
    }
    per_bucket
}

#[test]
fn whether_a_bucket_can_count_its_own_dirty_objects() {
    // ---- A NARROW ROUTING RANGE, so keys SHARE buckets and an object's pages can land outside
    // ---- its own routing bucket. On the default whole-keyspace range every key gets a bucket to
    // ---- itself by construction and the case that distinguishes the two methods never arises.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "dirty-count-equivalence".to_string(),
        shard_uri: "local://dirty-count-equivalence/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        loaded.status.ok,
        "the fixture could not load shard 1 on 0..{END_ROUTING_BUCKET}: {:?}",
        loaded.status
    );

    // ---- THREE WRITES A KEY, OF THREE KINDS, so an object holds SEVERAL PAGES. A fixture where
    // ---- every object is single-page makes a page count and an object count the same number, and
    // ---- then this test cannot tell one from the other.
    for index in 0..KEYS {
        let key = format!("dirty-count-{index:04}");
        for command in [
            Command::StringSet { key: key.clone(), value: vec![b'v'; 96] },
            Command::HashSet {
                key: key.clone(),
                field: "f".to_string(),
                value: vec![b'h'; 96],
            },
            Command::SetAdd { key: key.clone(), member: vec![b's'; 96] },
        ] {
            let out = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(out.status.ok, "fixture write {index}: {:?}", out.status);
        }
    }

    // ---- DUMP CLEAN FIRST, then dirty a FEW. Comparing the two methods while everything is dirty
    // ---- would agree on the bucket SET and could still agree on counts for the wrong reason:
    // ---- both would be reporting the whole shard. The interesting regime is most buckets clean.
    let _dumped = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });

    // ---- THE FIRST OF TWO MEASUREMENT POINTS, and it is the one that tests the NODE FLAG rather
    // ---- than the page walk. Right after a dump the set is empty, so every node still reading
    // ---- dirty here is reading dirty about nothing: `clear_published_object_dirty_state`
    // ---- reassigns `set_dirty(any_page_dirty)` only for the buckets that held a PAGE of the
    // ---- published object, while dropping the key from the set unconditionally -- so a bucket
    // ---- whose only dirt was a non-page-backed key is never recomputed.
    let (after_dump, after_dump_nodes_dirty, after_dump_set_buckets) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        (
            shard.dirty_objects.len(),
            shard.bucket_index.bucket_map.values().filter(|node| node.dirty()).count(),
            shard.dirty_objects.bucket_counts().count(),
        )
    };

    for index in [1usize, 5, 9] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dirty-count-{index:04}"),
                value: vec![b'w'; 96],
            },
        });
        assert!(out.status.ok, "re-dirtying {index}: {:?}", out.status);
    }
    // ---- AND TWO REMOVALS, which is what puts a dirty object with NO PAGE in the set. A removal
    // ---- marks a `control_state:cpc:*` record alongside the data object, control state is not
    // ---- page-backed, and that record routes to a bucket of its OWN. Without this the fixture
    // ---- holds only page-backed dirty objects and cannot see the term that decides the question.
    for index in [2usize, 6] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("dirty-count-{index:04}") },
        });
        assert!(out.status.ok, "removing {index}: {:?}", out.status);
    }

    // ---- WHAT THE PRODUCER ITSELF REPORTS, read BEFORE the shard lock is taken because
    // ---- `bucket_storage_summaries` takes that lock on its own. `DIRTY_SUMMARY_VISITS` counts
    // ---- the entries its dirty half looks at, so the work it does is measured here rather than
    // ---- argued from the shape of the loop.
    let visits_before = crate::engine::storage_reporting::DIRTY_SUMMARY_VISITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let produced_summaries: BTreeMap<u32, (u64, u64)> = engine
        .bucket_storage_summaries(1)
        .into_iter()
        .map(|summary| {
            (
                summary.routing_bucket,
                (summary.dirty_object_count, summary.dirty_generation),
            )
        })
        .collect();
    let produced: BTreeMap<u32, u64> = produced_summaries
        .iter()
        .map(|(bucket, (count, _))| (*bucket, *count))
        .collect();
    let producer_visits = crate::engine::storage_reporting::DIRTY_SUMMARY_VISITS
        .load(std::sync::atomic::Ordering::Relaxed)
        .saturating_sub(visits_before);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");

    let by_set = authority(shard);
    let by_objects = derived_distinct_objects(shard);
    let by_blocks = derived_dirty_blocks(shard);
    let by_node = node_state(shard);

    let bucket_total = shard.bucket_index.bucket_map.len();
    // What a per-bucket page walk would have to look at: a page's own `dirty` bit is the only
    // thing that marks it, so finding the dirty ones means visiting all of them.
    let block_total_in_shard: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.block_index.len())
        .sum();
    let set_total: u64 = by_set.values().sum();
    let object_total: u64 = by_objects.values().sum();
    let page_total: u64 = by_blocks.values().sum();

    // How many pages the busiest dirty object holds, so "not every object is single-page" is a
    // measured property of this fixture rather than an expectation about the write path.
    let mut pages_per_object: BTreeMap<String, u64> = BTreeMap::new();
    for (_bucket, _handle, object_key, _page) in super::pages_with_identity(&shard.bucket_index) {
        *pages_per_object.entry(object_key.to_string()).or_default() += 1;
    }
    let widest_object = pages_per_object.values().copied().max().unwrap_or(0);

    // How much of the set has no live page anywhere -- the term no page walk can ever reach.
    let any_block: BTreeSet<String> = super::pages_with_identity(&shard.bucket_index)
        .into_iter()
        .map(|(_, _, object_key, _)| object_key.to_string())
        .collect();
    let set_members_with_no_block = shard
        .dirty_objects
        .iter()
        .filter(|key| !any_block.contains(*key))
        .count();

    println!();
    println!("  {KEYS} keys on a {}-bucket routing range", END_ROUTING_BUCKET + 1);
    println!();
    println!("    buckets in the map                      {bucket_total:>6}");
    println!("    dirty objects left by the dump          {after_dump:>6}");
    println!("    pages held by the widest object         {widest_object:>6}");
    println!();
    println!("    buckets with a count, via the SET       {:>6}", by_set.len());
    println!("    buckets with a count, via the PAGES     {:>6}", by_objects.len());
    println!("    total, via the SET (the authority)      {set_total:>6}");
    println!("    total, distinct objects via the PAGES   {object_total:>6}");
    println!("    total, dirty PAGES                      {page_total:>6}");
    println!("    set members with NO live page at all    {set_members_with_no_block:>6}");
    println!();
    println!("    the producer's loop visited              {producer_visits:>6}  (one per dirty bucket)");
    println!("    a page walk would have to visit         {block_total_in_shard:>6}  (every page in the shard)");
    println!();
    println!("    after the dump: {after_dump} dirty object(s) in {after_dump_set_buckets} bucket(s), {after_dump_nodes_dirty} node(s) still reading dirty");
    println!();
    println!("    bucket   set  set>0  node.dirty  page>0   objects  pages   node.gen  summ.gen");
    // THE UNION OF EVERY BUCKET EITHER SIDE NAMES, so a bucket one method knows and the other
    // does not shows up as a row rather than being absent from the comparison.
    let every_bucket: BTreeSet<u32> = by_set
        .keys()
        .chain(by_objects.keys())
        .chain(by_blocks.keys())
        .chain(by_node.keys())
        .chain(produced_summaries.keys())
        .copied()
        .collect();
    for bucket in &every_bucket {
        let set_count = by_set.get(bucket).copied().unwrap_or(0);
        let (node_dirty, node_gen) = by_node.get(bucket).copied().unwrap_or((false, 0));
        let pages = by_blocks.get(bucket).copied().unwrap_or(0);
        println!(
            "    {bucket:>6} {set_count:>5} {:>6} {:>11} {:>7} {:>9} {pages:>6} {node_gen:>10} {:>9}",
            set_count > 0,
            node_dirty,
            pages > 0,
            by_objects.get(bucket).copied().unwrap_or(0),
            produced_summaries.get(bucket).map(|(_, gen)| *gen).unwrap_or(0),
        );
    }
    println!();

    // WHERE THE THREE PRESENCE SIGNALS DISAGREE, over that same union.
    let node_says_dirty_set_does_not: Vec<u32> = every_bucket
        .iter()
        .copied()
        .filter(|bucket| {
            by_node.get(bucket).map(|(dirty, _)| *dirty).unwrap_or(false)
                && by_set.get(bucket).copied().unwrap_or(0) == 0
        })
        .collect();
    let set_says_dirty_node_does_not: Vec<u32> = every_bucket
        .iter()
        .copied()
        .filter(|bucket| {
            by_set.get(bucket).copied().unwrap_or(0) > 0
                && !by_node.get(bucket).map(|(dirty, _)| *dirty).unwrap_or(false)
        })
        .collect();
    println!(
        "    node reads dirty where the set holds nothing: {:?}",
        node_says_dirty_set_does_not
    );
    println!(
        "    set holds a mark where the node reads clean:  {:?}",
        set_says_dirty_node_does_not
    );
    let generation_differs: Vec<u32> = every_bucket
        .iter()
        .copied()
        .filter(|bucket| {
            let node_gen = by_node.get(bucket).map(|(_, gen)| *gen).unwrap_or(0);
            let summary_gen = produced_summaries.get(bucket).map(|(_, gen)| *gen).unwrap_or(0);
            node_gen != summary_gen
        })
        .collect();
    println!(
        "    bucket(s) where the node's generation and the summary's differ: {:?}",
        generation_differs
    );
    println!();

    // The buckets the node reports with no dirty page of their own -- computed here, beside the
    // table it belongs with, and ASSERTED much further down. See the ordering note below.
    let node_carries_what_no_block_does: Vec<u32> = by_set
        .keys()
        .copied()
        .filter(|bucket| {
            by_blocks.get(bucket).copied().unwrap_or(0) == 0
                && by_node.get(bucket).map(|(dirty, _)| *dirty).unwrap_or(false)
        })
        .collect();
    println!(
        "    dirty bucket(s) with NO dirty page that the node still reports: {:?}",
        node_carries_what_no_block_does
    );
    println!();

    // ---- THE ORDER OF THE ASSERTIONS BELOW IS LOAD-BEARING, NOT STYLISTIC.
    //
    // A plant stops at the FIRST assertion it trips, so an assertion placed above another one
    // SHADOWS it: the plant written for the lower clause never reaches it, and the lower clause
    // ends up guarded by its neighbour rather than proven. That is not a theory -- these clauses
    // were once ordered with the node block first, and three plants landed on the wrong clause:
    // the two fixture-regime plants both stopped on `node_carries_what_no_block_does`, and the
    // plant that switches the producer stopped on the generation identity instead of the pin,
    // which is the one clause whose whole purpose is to catch that switch.
    //
    // So the blocks run in this order, and a new assertion goes in the block it belongs to rather
    // than at the end:
    //
    //   1. THE FIXTURE REACHED ITS REGIME -- every clause that stops this test passing for the
    //      wrong reason. These come first because a plant that breaks the fixture should say so
    //      rather than failing somewhere downstream.
    //   2. THE COUNT IS NOT DERIVABLE -- the conclusion about the page walk.
    //   3. PRESENCE IS, FROM THE NODE -- the conclusion about the flag.
    //   4. THE PRODUCER IS PINNED -- before the identity, because switching the producer breaks
    //      both and this is the clause that names the re-attempt.
    //   5. THE GENERATION IDENTITY -- last, so dropping the count term from the summary reaches
    //      it with the pin still passing.
    //
    // ---- 1. THE FIXTURE HAS TO REACH THE REGIME, and each clause is asserted on its own so a
    // ---- failure says which one did not hold.
    assert!(
        bucket_total < KEYS,
        "{KEYS} keys landed in {bucket_total} buckets, so each key may still have a bucket to \
         itself and the shared-bucket case never arose"
    );
    assert!(
        widest_object > 1,
        "the widest object holds {widest_object} page(s), so every object is single-page and a \
         page count and an object count are the same number on this fixture -- which means this \
         test cannot tell a correct derivation from a page tally. Write more kinds per key"
    );
    assert!(
        set_total > 0,
        "nothing is dirty, so both methods report nothing and agreeing means nothing"
    );
    assert!(
        by_set.len() < bucket_total,
        "all {bucket_total} buckets carry a dirty count, so both methods are answering \
         \"the whole shard\" and a method that always said so would pass. The dump above was \
         supposed to leave most of them clean -- check what it cleared before reading anything \
         below"
    );
    assert_ne!(
        object_total, page_total,
        "distinct dirty objects and dirty pages come to the same total ({object_total}), so this \
         fixture cannot distinguish an object count from a page count even though some object \
         holds {widest_object} pages"
    );

    // STILL BLOCK 1: the term that makes a page walk unsound at all, and a property of the
    // FIXTURE rather than a conclusion -- which is why it belongs here, above the node clause that
    // used to shadow it. Without it the conclusions below would rest on bucket attribution alone,
    // which is a weaker claim than this module states.
    assert!(
        set_members_with_no_block > 0,
        "every dirty object has a live page on this fixture, so the term that decides the question \
         did not arise here. The fixture's removals are what put a non-page-backed \
         `control_state:cpc:*` record in the set -- check that they still do before reading the \
         clauses below as the general case"
    );

    // ---- 2. THE COUNT IS NOT DERIVABLE, one clause at a time so a failure says which part moved
    // ---- rather than that "the conclusion changed".

    // The shortfall itself.
    assert!(
        object_total < set_total,
        "a page-derived walk totals {object_total} against the object-keyed set's {set_total}, so \
         it is no longer SHORT of the authority. If it has become equal, re-read the two reasons \
         in this module's header: should both have stopped holding, `bucket_storage_summaries` can \
         move to the derivation and this test becomes the equality that licenses it"
    );

    // THE DATA-LOSS CLAUSE, and the reason this is not merely a reporting difference. Dump
    // selection filters on `dirty_object_count > 0`, so a dirty bucket the derivation reads as
    // zero is a bucket that stops being dumped while it still holds undumped marks.
    let invisible_buckets = by_set
        .iter()
        .filter(|(bucket, _)| by_objects.get(bucket).copied().unwrap_or(0) == 0)
        .map(|(bucket, count)| format!("{bucket}:{count}"))
        .collect::<Vec<_>>();
    assert!(
        !invisible_buckets.is_empty(),
        "every bucket the set reports dirty is also found by the page walk, so the \
         stops-being-dumped case did not arise on this fixture and the clause above is the whole \
         of the finding. Say so rather than leaving this assertion asserting nothing"
    );
    println!(
        "    {} of {} dirty bucket(s) the page walk cannot see at all: {}",
        invisible_buckets.len(),
        by_set.len(),
        invisible_buckets.join(" ")
    );

    // AND IT IS WRONG WHERE THE DERIVATION DOES SEE THE BUCKET, which the clause above cannot
    // show: a shortfall confined to invisible buckets would be a different, smaller finding.
    let short_but_visible = by_set
        .iter()
        .filter(|(bucket, count)| {
            let derived = by_objects.get(bucket).copied().unwrap_or(0);
            derived > 0 && derived < **count
        })
        .map(|(bucket, count)| {
            format!("{bucket}:{}<{count}", by_objects.get(bucket).copied().unwrap_or(0))
        })
        .collect::<Vec<_>>();
    assert!(
        !short_but_visible.is_empty(),
        "every bucket the page walk DOES see, it counts exactly. The shortfall would then be \
         confined to buckets holding no page at all, which is a narrower finding than this module \
         states -- a bucket holding several marks and one dirty object is what shows the count \
         itself is wrong, not just its support"
    );
    println!(
        "    dirty bucket(s) the page walk sees and still under-counts: {}",
        short_but_visible.join(" ")
    );
    println!();

    // ---- 3. PRESENCE IS A DIFFERENT QUESTION FROM THE COUNT, AND THE NODE ANSWERS IT.
    //
    // `mark_object_dirty` creates the routing bucket's node when the key has none and sets `dirty`
    // on it, so a key with NO PAGE AT ALL still leaves a node reading dirty. That is why the five
    // buckets a page walk cannot see are nonetheless visible here, and it is the whole reason the
    // three `> 0` consumers can move while the count cannot.
    //
    // ASSERTED IN BOTH DIRECTIONS, because one direction holding is not agreement and the two
    // failures are not interchangeable. A node reading dirty where the set holds nothing refuses a
    // release and keeps a bucket pinning the logs; the set holding a mark the node cannot see is a
    // bucket that stops being dumped.
    assert!(
        set_says_dirty_node_does_not.is_empty(),
        "bucket(s) {set_says_dirty_node_does_not:?} hold a mark in the object-keyed set while the \
         node reads CLEAN, so `bucket.dirty()` is NOT a safe stand-in for \
         `dirty_object_count > 0`: a bucket missing from the node view is a bucket that stops \
         being dumped while it holds undumped writes. Check that every site marking an object \
         dirty still sets the flag on the routing bucket's node"
    );
    assert!(
        node_says_dirty_set_does_not.is_empty(),
        "bucket(s) {node_says_dirty_set_does_not:?} read dirty on the node while the set holds \
         nothing for them. That is the SAFE direction for the dump filter -- an extra bucket is \
         dumped -- and the UNSAFE one for the two reclaim checks, where `dirty_object_count == 0` \
         is the arm that counts a bucket as covered and contributing no WAL floor. A bucket \
         wrongly reading dirty skips that arm, cannot name a claim because its \
         `first_dirty_wal_sequence` was cleared alongside `dirty = false`, and blocks the whole \
         reclaim plan. So this clause is what licenses moving the RECLAIM consumers, not just the \
         dump filter"
    );
    // AND IT AGREES WHERE A PAGE WALK CANNOT, which is the case that makes the agreement worth
    // something: without it the two columns could be agreeing only on page-backed buckets.
    assert!(
        !node_carries_what_no_block_does.is_empty(),
        "every dirty bucket here has a dirty page, so the node flag and the page walk cannot be \
         told apart on this fixture and the agreement above says nothing about the case that \
         matters. The fixture's removals are what produce a dirty bucket holding no page"
    );

    // ---- 4. AND THE PRODUCER IS PINNED TO THE AUTHORITY, which is what makes the decline above
    // ---- DURABLE. Everything before this compares two derivations and would go on passing while
    // ---- someone switched `bucket_storage_summaries` to the losing one; a comment saying "do not"
    // ---- cannot fail. This asserts the producer's own output, bucket for bucket, so the
    // ---- re-attempt fails HERE with the numbers that refuse it.
    for (bucket, count) in &by_set {
        assert_eq!(
            produced.get(bucket).copied().unwrap_or(0),
            *count,
            "`bucket_storage_summaries` reports {} dirty object(s) for bucket {bucket} where the \
             object-keyed set holds {count}, so the producer has stopped reading the set.\n\n  via \
             the set:   {by_set:?}\n  as produced: {produced:?}\n\nIf that was deliberate, read \
             this module's header first. The derivation it has most likely moved to reports \
             {object_total} of the set's {set_total} marks ON THIS FIXTURE, reads {} of {} dirty \
             buckets as clean -- which stops them being dumped while they hold undumped marks -- \
             and this count is also added onto `summary.dirty_generation`, which \
             `bucket_dump_summary_matches_current_generation` compares EXACTLY when it validates a \
             stored dump manifest",
            produced.get(bucket).copied().unwrap_or(0),
            invisible_buckets.len(),
            by_set.len(),
        );
    }

    // ---- 5. AND WHAT THE MANIFEST-VALIDATED GENERATION ACTUALLY IS: an accounting identity, not
    // ---- a figure that happens to agree. `bucket_storage_summaries` seeds the summary from the
    // ---- node's own `dirty_generation` and then adds the dirty COUNT onto it, so the value
    // ---- `bucket_dump_summary_matches_current_generation` compares EXACTLY is a composite of a
    // ---- monotonic per-mark counter and a live population count. Stated as the identity rather
    // ---- than as "they differ on these buckets", which a threshold could pass by accident.
    //
    // LAST, AND DELIBERATELY BELOW THE PIN. Switching the producer breaks this identity too, so
    // with this above the pin, the plant that re-attempts the switch stopped here and the pin --
    // the clause written for exactly that re-attempt -- was never reached.
    for bucket in &every_bucket {
        if !produced_summaries.contains_key(bucket) {
            continue;
        }
        let node_gen = by_node.get(bucket).map(|(_, gen)| *gen).unwrap_or(0);
        let set_count = by_set.get(bucket).copied().unwrap_or(0);
        let summary_gen = produced_summaries.get(bucket).map(|(_, gen)| *gen).unwrap_or(0);
        assert_eq!(
            summary_gen,
            node_gen.saturating_add(set_count),
            "bucket {bucket}: the summary's dirty_generation is {summary_gen} where the node holds \
             {node_gen} and the set holds {set_count} marks, so it is no longer \
             `node + count`. That sum is what a stored dump manifest is validated against, so this \
             identity is a durability contract: if the count term has been dropped, every manifest \
             written before the change stops matching the generation computed after it"
        );
    }
}
