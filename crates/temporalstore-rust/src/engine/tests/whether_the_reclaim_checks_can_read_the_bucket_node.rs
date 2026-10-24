// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN THE WAL-RECLAIM CHECKS READ THE BUCKET'S OWN DIRTY FLAG? The over-reporting direction,
//! measured at every state a dump cycle passes through.
//!
//! # WHAT THIS IS FOR
//!
//! The dirty-BUCKET question has one authority -- the bucket's own node -- and the presence
//! readers were asking a side index of dirty OBJECT KEYS instead.
//! `whether_a_bucket_can_count_its_own_dirty_objects` measures the two records agreeing on every
//! bucket in both directions and measures the node's flag to be an independent record rather than
//! a cache of the set. `which_record_the_dirty_bucket_consumers_read` moves the three readers whose
//! error costs a report or a dump.
//!
//! THE TWO WAL-RECLAIM CHECKS ARE NOT THOSE, AND THIS IS THE TEST THAT DECIDES THEM.
//! `dirty_object_count == 0` is the arm that counts a bucket as COVERED and contributing no log
//! floor. A bucket wrongly reading dirty skips that arm, cannot name a claim -- its
//! `first_dirty_wal_sequence` was cleared to 0 in the same breath as `dirty = false` -- lands in
//! `missing_bucket_generations`, and blocks the whole reclaim plan. That regression is recorded at
//! the site itself as measured: 0 of 10,666 records freed and `persistent_bytes` flat at 1,344,852
//! for ever.
//!
//! So agreement is not the bar here. ONE DIRECTION IS, and it is the opposite of the dump filter's:
//!
//!     node reads dirty where the set holds NOTHING    must be EMPTY, or reclaim must not move
//!     set holds a mark where the node reads clean     the dump filter's hazard, not this one
//!
//! # WHY A FULL DUMP IS NOT THE CASE TO MEASURE
//!
//! `clear_dumped_bucket_dirty_state` walks `manifest.bucket_ids` and nothing else, so only a
//! DUMPED bucket has `set_dirty(false)` and `first_dirty_wal_sequence = 0` applied to it, and only
//! a dumped bucket's keys are drained from the set. A dump covering every dirty bucket therefore
//! clears both records together and cannot separate them -- which is the state the agreement above
//! was measured in.
//!
//! A PARTIAL dump is the case that can. It is also the ordinary one:
//! `max_dump_buckets_per_round` exists to ask for it, and the selection is truncated to that bound
//! every round on a shard with more dirty buckets than the bound. So this measures the direction
//! after a dump of ONE bucket, with other buckets left dirty, as well as after a full one.
//!
//! The sibling path is `clear_published_object_dirty_state`, which drops a key from the set
//! unconditionally and reassigns the bucket flag only for buckets holding a PAGE of the published
//! object. A bucket whose only dirt is a non-page-backed `control_state:cpc:*` record holds no
//! page, so it is never recomputed -- and a dirty key leaving the set while the flag stays is
//! exactly the over-reporting state. That is why the figure below is taken after each phase rather
//! than once at the end.
//!
//! # WHAT IT MEASURES
//!
//! Printed per phase, and asserted per phase so a failure names the phase that produced it. The
//! figure is the one that licenses or refuses the move; the assertion is the license.
//!
//! rust-internal: reads the engine's own index structures, no product behaviour

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;
const END_ROUTING_BUCKET: u32 = 15;

/// Every bucket's two records of the same fact, plus the claim it could name over the log.
fn phase_state(engine: &crate::TemporalEngine) -> BTreeMap<u32, (bool, u64, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                (
                    bucket.dirty(),
                    shard.dirty_objects.bucket_count(*routing_bucket),
                    bucket.first_dirty_wal_sequence,
                ),
            )
        })
        .collect()
}

/// The buckets whose node reads dirty while the object-keyed set holds nothing for them.
///
/// THE ONE FIGURE THIS TEST EXISTS FOR. Each such bucket is one the reclaim plan would stop
/// counting as covered, and -- because its claim was cleared alongside its flag -- one that cannot
/// name a WAL or index-log position either, so it lands in `missing_bucket_generations` and blocks
/// the plan outright.
fn over_reporting(state: &BTreeMap<u32, (bool, u64, u64)>) -> Vec<(u32, u64)> {
    state
        .iter()
        .filter(|(_bucket, (node_dirty, count, _claim))| *node_dirty && *count == 0)
        .map(|(bucket, (_node_dirty, _count, claim))| (*bucket, *claim))
        .collect()
}

/// The buckets the set marks while their node reads clean -- the dump filter's hazard, reported
/// here so the two directions are never read off one number.
fn under_reporting(state: &BTreeMap<u32, (bool, u64, u64)>) -> Vec<(u32, u64)> {
    state
        .iter()
        .filter(|(_bucket, (node_dirty, count, _claim))| !*node_dirty && *count > 0)
        .map(|(bucket, (_node_dirty, count, _claim))| (*bucket, *count))
        .collect()
}

fn print_phase(label: &str, state: &BTreeMap<u32, (bool, u64, u64)>) {
    let node_dirty = state.values().filter(|(dirty, _, _)| *dirty).count();
    let set_marked = state.values().filter(|(_, count, _)| *count > 0).count();
    let over = over_reporting(state);
    let under = under_reporting(state);
    println!(
        "    {label:<34} buckets {:>3}  node.dirty {node_dirty:>3}  set-marked {set_marked:>3}  \
         OVER {:>3}  under {:>3}",
        state.len(),
        over.len(),
        under.len()
    );
    if !over.is_empty() {
        println!("        OVER-reporting (bucket, first_dirty_wal_sequence): {over:?}");
    }
    if !under.is_empty() {
        println!("        under-reporting (bucket, set count): {under:?}");
    }
}

#[test]
fn whether_the_reclaim_checks_can_read_the_bucket_node() {
    // ---- A NARROW ROUTING RANGE, so the removals' non-page-backed records route to buckets of
    // ---- their own -- the population `clear_published_object_dirty_state` cannot recompute, and
    // ---- so the one that could leave a flag standing over an empty set.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "reclaim-presence".to_string(),
        shard_uri: "local://reclaim-presence/1".to_string(),
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

    let mut phases: Vec<(String, BTreeMap<u32, (bool, u64, u64)>)> = Vec::new();

    for index in 0..KEYS {
        let key = format!("reclaim-presence-{index:04}");
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
    phases.push(("after the ingest".to_string(), phase_state(&engine)));

    // ---- A FULL DUMP: every dirty bucket selected, so both records are cleared together.
    let _full = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    phases.push(("after a FULL dump".to_string(), phase_state(&engine)));

    // ---- RE-DIRTY A FEW AND REMOVE TWO. The removals are what put a non-page-backed record in
    // ---- the set, routed to a bucket holding no page.
    for index in [1usize, 5, 9] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("reclaim-presence-{index:04}"),
                value: vec![b'w'; 96],
            },
        });
        assert!(out.status.ok, "re-dirtying {index}: {:?}", out.status);
    }
    for index in [2usize, 6] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("reclaim-presence-{index:04}") },
        });
        assert!(out.status.ok, "removing {index}: {:?}", out.status);
    }
    phases.push(("after re-dirtying and removals".to_string(), phase_state(&engine)));

    // ---- A PARTIAL DUMP, one bucket at a time, which is the case a full dump cannot produce:
    // ---- `clear_dumped_bucket_dirty_state` touches only the manifest's own bucket ids, so every
    // ---- OTHER dirty bucket keeps both its flag and its keys while this one loses both. Each
    // ---- round is measured, because the state that separates the two records may exist only
    // ---- between two of them.
    let dirty_now: Vec<u32> = phase_state(&engine)
        .iter()
        .filter(|(_bucket, (node_dirty, _count, _claim))| *node_dirty)
        .map(|(bucket, _)| *bucket)
        .collect();
    assert!(
        !dirty_now.is_empty(),
        "[clause: regime_partial_dump_has_buckets] nothing is dirty before the partial dumps, so \
         the phase that can separate the two records never runs and the figure below is about a \
         shard that was never in the interesting state"
    );
    for (round, bucket) in dirty_now.iter().copied().enumerate() {
        let report = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: vec![bucket],
            ..Default::default()
        });
        let _ = report;
        phases.push((
            format!("after a PARTIAL dump of bucket {bucket} (round {})", round + 1),
            phase_state(&engine),
        ));
    }

    // ---- THEN ADVANCE THE LOG PAST THE MANIFEST THAT COVERS MOST BUCKETS. The floor arm's only
    // ---- observable is the frontier, and it holds a dirty bucket's floor at the sequence of the
    // ---- manifest covering it -- so if that manifest sits AT the current log position, the arm
    // ---- runs and moves nothing, and a probe reading the frontier cannot see it. Measured: with
    // ---- every bucket's newest manifest written by the last partial round, forcing a bucket dirty
    // ---- left retain(wal, index-log) at (150, 150) exactly.
    //
    // One more write and one more dump fixes that. The dump names only the bucket that write
    // dirtied, so every OTHER bucket's newest covering manifest now predates the current position
    // and a floor held at it is a figure that differs.
    let tail_key = format!("reclaim-presence-{:04}", 3);
    let out = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: tail_key, value: vec![b't'; 96] },
    });
    assert!(out.status.ok, "the tail write failed: {:?}", out.status);
    let tail_dirty: BTreeSet<u32> = phase_state(&engine)
        .iter()
        .filter(|(_bucket, (node_dirty, _count, _claim))| *node_dirty)
        .map(|(bucket, _)| *bucket)
        .collect();
    let _tail_dump = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    phases.push((
        "after a tail write and its dump".to_string(),
        phase_state(&engine),
    ));

    // ---- AND A WAL RECLAIM PLAN, which is the consumer under discussion. Run last so the figure
    // ---- above is not taken on a shard this call has already changed.
    let reclaim = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    phases.push(("after a reclaim plan".to_string(), phase_state(&engine)));

    println!();
    println!("  {KEYS} keys on a {}-bucket routing range", END_ROUTING_BUCKET + 1);
    println!();
    for (label, state) in &phases {
        print_phase(label, state);
    }
    println!();
    println!("    reclaim plan: blockers {:?}", reclaim.blocker_reasons);
    println!(
        "    reclaim plan: missing_bucket_generations {:?}",
        reclaim.missing_bucket_generations
    );
    println!();

    // ---- AND WHICH RECORD THE TWO CHECKS ACTUALLY READ. The figures above are the LICENCE: they
    // ---- say the state that would break this does not arise. They are also why no ordinary
    // ---- assertion can say which record the plan consulted -- while the two agree, a plan reading
    // ---- either one satisfies any comparison against the truth. So force ONE bucket's node dirty
    // ---- over an empty set and read the plan again.
    //
    // The shard is fully dumped at this point: every node reads clean, the set is empty, and the
    // plan above found every bucket covered with the floor at the CURRENT log position. Forcing one
    // flag changes exactly one thing, and the two arms answer it differently, so each is pinned on
    // its own below. Taken AFTER `phases` is captured, so the measurement above is untouched by it.
    // A BUCKET THE NEWEST MANIFEST DOES NOT NAME, so the manifest covering it predates the current
    // log position and a floor held at it is a figure that differs from the settled one.
    let settled = phase_state(&engine);
    let a_bucket = *settled
        .keys()
        .find(|bucket| !tail_dirty.contains(bucket))
        .expect("the tail dump did not name every bucket");
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        shards
            .get_mut(&1)
            .expect("shard 1")
            .bucket_index
            .bucket_map
            .get_mut(&a_bucket)
            .expect("the bucket chosen above is in the map")
            .set_dirty(true);
    }
    let floor_forced = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let floor_state = phase_state(&engine);

    // ---- THEN THE SAME BUCKET WITH NO MANIFEST THAT MATCHES IT. Bumping the node's generation
    // ---- makes `bucket_dump_summary_matches_current_generation` stop matching every stored
    // ---- manifest for this bucket, which is the branch where `== 0` is the COVERED arm. Its claim
    // ---- is still 0 because the dump cleared it alongside the flag, so a plan reading the node
    // ---- must report it as unable to name one -- and that is the regression this move is gated
    // ---- against, reached deliberately here to show which record decides it.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let bucket = shards
            .get_mut(&1)
            .expect("shard 1")
            .bucket_index
            .bucket_map
            .get_mut(&a_bucket)
            .expect("the bucket chosen above is in the map");
        bucket.set_dirty(true);
        bucket.dirty_generation = bucket.dirty_generation.saturating_add(10_000);
    }
    let covered_forced = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    let covered_state = phase_state(&engine);

    let triple = |plan: &crate::engine::reports::StorageWalReclaimPlan| {
        (
            plan.retain_from_wal_sequence,
            plan.retain_from_index_log_sequence,
            plan.covered_bucket_count,
        )
    };
    println!("    forcing bucket {a_bucket} to read dirty over an EMPTY set:");
    println!(
        "      settled      retain(wal,index-log) + covered {:?}  missing {:?}",
        triple(&reclaim), reclaim.missing_bucket_generations
    );
    println!(
        "      floor arm    retain(wal,index-log) + covered {:?}  missing {:?}",
        triple(&floor_forced), floor_forced.missing_bucket_generations
    );
    println!(
        "      covered arm  retain(wal,index-log) + covered {:?}  missing {:?}",
        triple(&covered_forced), covered_forced.missing_bucket_generations
    );
    println!();

    // ---- THE FIXTURE HAS TO REACH THE REGIME, each clause on its own and all of them before any
    // ---- conclusion, because a plant stops at the first failing assertion.
    let ingest = &phases[0].1;
    assert!(
        ingest.len() < KEYS,
        "[clause: regime_shared_buckets] {KEYS} keys landed in {} buckets, so each key may still \
         have a bucket to itself and the shared-bucket case never arose",
        ingest.len()
    );
    assert!(
        phases.len() >= 5,
        "[clause: regime_phases_ran] only {} phases were measured, so the partial-dump rounds -- \
         the case a full dump cannot produce -- did not run",
        phases.len()
    );
    let partial_rounds = phases
        .iter()
        .filter(|(label, _state)| label.contains("PARTIAL"))
        .count();
    assert!(
        partial_rounds > 1,
        "[clause: regime_several_partial_dumps] {partial_rounds} partial dump round(s) ran, so no \
         round left ANOTHER bucket dirty while clearing one -- which is the whole shape this test \
         exists to measure"
    );
    let any_non_page_backed_mark = phases
        .iter()
        .any(|(_label, state)| state.values().any(|(_dirty, count, _claim)| *count > 0));
    assert!(
        any_non_page_backed_mark,
        "[clause: regime_set_was_marked] the object-keyed set held nothing in any phase, so there \
         was never a mark for a flag to outlive and the direction below is empty for want of a \
         fixture"
    );

    // ---- AND THE ONE CONCLUSION, PER PHASE, so the figure that refuses the move names the phase
    // ---- that produced it rather than the whole run.
    for (label, state) in &phases {
        let over = over_reporting(state);
        assert!(
            over.is_empty(),
            "[clause: over_reporting_is_empty] {} bucket(s) read DIRTY on their own node while the \
             object-keyed set holds nothing for them, {label}: {over:?} (bucket, \
             first_dirty_wal_sequence).\n\nThat is the direction the two WAL-reclaim checks cannot \
             tolerate. Each of those buckets would stop being counted as covered, and a claim of 0 \
             is NO CLAIM RECORDED -- so it lands in `missing_bucket_generations` and blocks the \
             whole reclaim plan, which is the regression recorded at that site as 0 of 10,666 \
             records freed. If this is now reachable, the two reclaim checks must go back to \
             `dirty_object_count`, and the dump filter may stay where it is because its error in \
             this direction only costs an extra dump",
            over.len()
        );
    }

    // AND THE PLAN ITSELF IS NOT BLOCKED, which is the same statement one level out: the figure
    // above is about the records, this is about what the consumer did with them.
    assert!(
        reclaim.missing_bucket_generations.is_empty(),
        "[clause: reclaim_plan_not_blocked] the reclaim plan reports bucket(s) {:?} as unable to \
         name a durable dump, so the plan is blocked on this fixture. Read the per-phase figures \
         above: a bucket reading dirty over an empty set is the state that produces this",
        reclaim.missing_bucket_generations
    );

    // ---- THE FLOOR ARM'S PROBE CAN SEE THE ARM AT ALL. Asserted before the conclusion, because a
    // ---- probe whose forced bucket is covered by the NEWEST manifest holds its floor at the
    // ---- current log position and moves nothing -- and would then read as "the arm does not read
    // ---- the node" when in fact the fixture cannot tell.
    assert!(
        !tail_dirty.contains(&a_bucket),
        "[clause: regime_forced_bucket_predates_newest_manifest] bucket {a_bucket} was dirtied by \
         the tail write, so the newest manifest names it and a floor held at that manifest is the \
         current position. The floor-arm conclusion below cannot distinguish the two records on \
         this bucket"
    );
    assert!(
        tail_dirty.len() < settled.len(),
        "[clause: regime_tail_dump_was_partial] the tail write dirtied all {} bucket(s), so every \
         bucket is covered by the newest manifest and there is nowhere to put a forced flag whose \
         floor differs. Write to FEWER keys",
        tail_dirty.len()
    );

    // ---- AND THE FORCED PAIR LANDED, before either arm's conclusion is read.
    assert!(
        floor_state.get(&a_bucket).map(|(dirty, _, _)| *dirty).unwrap_or(false),
        "[clause: plant_floor_arm_landed] bucket {a_bucket} was forced to read dirty and does not, \
         so the floor-arm conclusion below is about a shard that never held the divergence"
    );
    assert_eq!(
        floor_state.get(&a_bucket).map(|(_, count, _)| *count),
        Some(0),
        "[clause: plant_floor_arm_set_empty] bucket {a_bucket} was forced dirty over an EMPTY set \
         and the set now holds a mark for it, so the two records are no longer separated and the \
         conclusion below could be satisfied by either one"
    );
    assert!(
        covered_state.get(&a_bucket).map(|(dirty, _, _)| *dirty).unwrap_or(false),
        "[clause: plant_covered_arm_landed] bucket {a_bucket} does not read dirty after the second \
         forcing, so the covered-arm conclusion below is about a shard without the divergence"
    );
    assert_eq!(
        covered_state.get(&a_bucket).map(|(_, _, claim)| *claim),
        Some(0),
        "[clause: plant_covered_arm_names_no_claim] bucket {a_bucket} can name a WAL claim of {:?}, \
         so it is no longer the case this arm is about: a bucket that CAN place itself in the log \
         is covered by naming its claim, whichever record said it was dirty",
        covered_state.get(&a_bucket).map(|(_, _, claim)| *claim)
    );

    // ---- THE FLOOR ARM. A dirty bucket with a matching manifest holds the floor at that
    // ---- manifest's sequence, which is below the current log position; a plan reading the count
    // ---- sees 0 for this bucket and leaves the floor where it was. So the figures move only if
    // ---- the node is what was read.
    assert_ne!(
        triple(&floor_forced),
        triple(&reclaim),
        "[clause: reclaim_floor_arm_reads_the_node] forcing bucket {a_bucket} to read dirty over an \
         empty set moved nothing: retain(wal, index-log) and covered_bucket_count are \
         {:?} both before and after. The floor arm is therefore still reading \
         `dirty_object_count`, which is 0 for this bucket",
        triple(&reclaim)
    );

    // ---- THE COVERED ARM, which is the other of the two and the one whose error blocks the plan.
    assert!(
        covered_forced.missing_bucket_generations.contains(&a_bucket),
        "[clause: reclaim_covered_arm_reads_the_node] bucket {a_bucket} reads dirty on its own node, \
         names no claim in either log, and no stored manifest matches its generation -- so a plan \
         reading the node must report it as unable to name a durable dump. It reports {:?}, so the \
         covered arm is still reading `dirty_object_count`, which is 0 for this bucket",
        covered_forced.missing_bucket_generations
    );
}
