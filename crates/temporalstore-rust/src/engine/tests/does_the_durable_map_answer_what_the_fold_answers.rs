// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! UNVERIFIED: WRITTEN WITHOUT A COMPILER. The box was at 5.9 G free with 28 foreign build
//! processes when this was written, below the abort floor, so nothing here has been built or run.
//! Treat every signature as a guess until it compiles.
//!
//! # THE QUESTION THIS EXISTS TO SETTLE
//!
//! `Command::SetMembers` folds every block of the object, tombstones included, on every COLD
//! listing. #2491 measured what that costs: cold block reads equal the object's block count
//! exactly, so a listing answering 64 live members performs 1,033 block reads and 71.4 ms once
//! 1,024 members have been removed, against 614 us for the identical answer with no churn.
//!
//! The durable member map `shard.sets` is keyed BY MEMBER BYTES, so it holds the member identity
//! the bucket index cannot. Three independent places in the tree already state that it answers
//! correctly:
//!
//!   * `execute_on_shard.rs`, on the decline path: "The durable member map is the authority --
//!     `reconcile_from_durable` treats it as such, and a gated removal was already shown to
//!     survive a reload in it";
//!   * `gated_listing_folds_the_blocks`'s module note: "The gated branch declines an INCOMPLETE
//!     fold and answers from the durable map instead. That fallback returns the right members";
//!   * that module's own floor message: "The decline path answers from the durable map and
//!     returns the same members, so this assertion is what distinguishes a tested fold from an
//!     untested one".
//!
//! **Those are three claims by previous authors, not three measurements.** Serving the listing
//! from the map on the strength of comments is exactly how a removed member gets resurrected --
//! which is the failure a hand-rolled walk of the payload produced before `derive_membership`
//! owned the ordering. So this module drives the two answers side by side and asserts they agree,
//! on the scenarios where they could plausibly diverge. It changes no production code: it is the
//! evidence the change needs, and it must be GREEN before the primary is switched.
//!
//! # WHY THESE SCENARIOS AND NOT A SINGLE ONE
//!
//! Each is a different way the map could be wrong while the blocks are right:
//!
//!   1. `plain_adds` -- the trivial case, as a control. If this disagrees the fixture is broken
//!      and nothing below means anything.
//!   2. `one_removed` -- the module's own fixture, and the case the fold was made safe for.
//!   3. `removed_then_readded` -- THE PRECEDENCE CASE. A member stated live, then tombstoned,
//!      then stated live again. The fold resolves it by `append_position`, so the last block
//!      wins; the map resolves it by having the key present. These are different mechanisms
//!      reaching the same answer, which is precisely why they have to be compared rather than
//!      assumed to agree.
//!   4. `after_a_fold` -- compaction rewrites live blocks and drops tombstones. It may leave the
//!      map's ADDRESSES stale; the listing reads only the map's KEYS, so this arm is here to
//!      prove that distinction rather than to trust it.
//!   5. `after_a_reload` -- the map is rebuilt from the serialised shard index (`set_index_serde`)
//!      and from WAL replay, and `SetKind` deliberately does not declare
//!      `ReplaysInstallsUnrecorded`, so replay installs through `install_element`. If either
//!      route is lossy, the map after a reload is where it shows.
//!
//! # WHAT A GREEN RUN LICENSES, AND WHAT IT DOES NOT
//!
//! Green licenses serving `SMEMBERS` from the map, which takes the cold cost from O(live + dead)
//! block reads to ZERO. It does NOT license deleting the fold: `derive_membership` stays the
//! authority for the decline path and for anything that must read membership out of blocks
//! without a loaded map.
//!
//! AND IT BREAKS TWO EXISTING FLOORS ON PURPOSE, which must be retargeted deliberately rather
//! than deleted -- they are coupled to the fold SERVING, not to the answer:
//!   * `gated_listing_folds_the_blocks.rs` ~line 155: `assert_eq!(1, derived, ..)`
//!   * `gated_listing_folds_the_blocks.rs` ~line 162: `assert_eq!(0, declined, ..)`
//! With the map primary, `derived` becomes 0 because the fold no longer runs on that path. The
//! property those floors protect -- that the fold, when it runs, does not resurrect a removed
//! member -- is still worth holding, so they should be pointed at `derive_membership` directly
//! (as `an_unframed_block_makes_the_fold_decline_rather_than_answer_short` already is) rather
//! than at a listing that no longer calls it.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

const MEMBERS: usize = 40;
const KEY: &str = "map-against-fold";

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "map-against-fold".to_string(),
        shard_uri: "local://map-against-fold/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
    // THE GATE IS OFF FOR THIS FIXTURE, AND THAT IS THE SUBJECT RATHER THAN A DETAIL. The
    // durable member map is the preferred answer now, so a listing driven with the gate on
    // never reaches the fold -- and every assertion in this file is about what the fold does.
    let mut extend_config = std::collections::BTreeMap::new();
    extend_config.insert("set_listing_from_map".to_string(), "off".to_string());
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    extend_config,
                    ..Default::default()
                },
            })
            .ok,
        "the gate under test has to be the one that is set"
    );
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("member-{index:03}").into_bytes()
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn add(engine: &TemporalEngine, member: Vec<u8>) {
    write(
        engine,
        Command::SetAdd {
            key: KEY.to_string(),
            member,
        },
    );
}

fn remove(engine: &TemporalEngine, member: Vec<u8>) {
    write(
        engine,
        Command::SetRemove {
            key: KEY.to_string(),
            member,
        },
    );
}

/// What `SMEMBERS` answers today -- i.e. the FOLD, since the fold is the primary and only declines
/// on an unreadable block. The counters say which path actually served, so this is not an
/// assumption: a scenario where the fold declined would make the comparison below compare the map
/// against itself and pass for the wrong reason.
fn fold_answer(engine: &TemporalEngine) -> (BTreeSet<Vec<u8>>, u64, u64) {
    crate::engine::execute_on_shard::reset_gated_listing_counts();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    assert!(response.status.ok, "SMEMBERS failed: {response:?}");
    let members = match response.response {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("expected Members, got {other:?}"),
    };
    // from_map is read and asserted zero: with the gate off it must be, and if it is not then
    // `fold_answer` returned the map's answer and the comparison below is the map against
    // itself -- agreement for a reason that has nothing to do with the blocks.
    let (derived, declined, from_map) =
        crate::engine::execute_on_shard::gated_listing_counts();
    assert_eq!(
        0, from_map,
        "the durable map answered this listing, so what this function returned is not the \
         fold's answer and nothing downstream compares two mechanisms"
    );
    (members, derived, declined)
}

/// What the durable member map holds, read the way the decline path reads it -- KEYS only, which
/// is the whole point: the map's inner key IS the member identity, and its value (the element
/// address) is never consulted for a listing.
fn map_answer(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
    // The accessor pattern `hash_container_reload_authority` uses, rather than a helper that does
    // not exist: the engine's shard table is read-locked and the shard taken out of it.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    shard
        .sets
        .get(KEY)
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default()
}

/// Drive one scenario and require the two mechanisms to agree.
///
/// THE CACHE IS THE TRAP HERE. `SMEMBERS` is behind a response cache that writes invalidate, so a
/// second listing in the same scenario would read the cache and the fold would never run -- the
/// counters are what catch that, and a scenario that reports `derived == 0` is void rather than
/// passing.
fn the_two_agree(label: &str, engine: &TemporalEngine, expected: &BTreeSet<Vec<u8>>) {
    let (fold, derived, declined) = fold_answer(engine);
    let map = map_answer(engine);
    println!(
        "  {label}: fold={} map={} expected={} (derived={derived} declined={declined})",
        fold.len(),
        map.len(),
        expected.len()
    );
    assert_eq!(
        1, derived,
        "{label}: THE FOLD DID NOT SERVE ({derived} derived, {declined} declined), so this \
         scenario compared the durable map against itself and would agree for a reason that has \
         nothing to do with the blocks. Either a block was unreadable or the response cache \
         answered -- both make the figure void"
    );
    assert_eq!(
        *expected, fold,
        "{label}: the FOLD does not answer the membership the commands left"
    );
    assert_eq!(
        *expected, map,
        "{label}: THE DURABLE MAP DISAGREES WITH THE COMMANDS. This is the arm that decides \
         whether the listing may be served from the map at all -- the map is the cheap answer and \
         this says it is also the right one"
    );
    assert_eq!(
        fold, map,
        "{label}: THE FOLD AND THE DURABLE MAP DISAGREE. Serving the listing from the map would \
         change the answer, not just its cost, and on a set that is the resurrection of a removed \
         member. Do NOT switch the primary while this is red; the fold is load-bearing in this \
         scenario and the reason it is must be understood first"
    );
}

#[test]
fn the_durable_map_answers_what_the_fold_answers_on_every_scenario_that_could_diverge() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // 1. THE CONTROL. If this disagrees the fixture is broken and nothing below means anything.
    let mut expected: BTreeSet<Vec<u8>> = (0..MEMBERS).map(member_bytes).collect();
    for index in 0..MEMBERS {
        add(&engine, member_bytes(index));
    }
    the_two_agree("plain_adds", &engine, &expected);

    // 2. ONE REMOVED -- the fixture the fold was made safe for.
    let victim = member_bytes(7);
    remove(&engine, victim.clone());
    expected.remove(&victim);
    the_two_agree("one_removed", &engine, &expected);

    // 3. THE PRECEDENCE CASE: live, then tombstoned, then live again. The fold decides by
    //    `append_position`; the map decides by the key being present. Two mechanisms, and this is
    //    where they would part company.
    add(&engine, victim.clone());
    expected.insert(victim.clone());
    the_two_agree("removed_then_readded", &engine, &expected);

    // 4. AFTER A FOLD. Compaction rewrites live blocks and drops tombstones, which may leave the
    //    map's ADDRESSES stale. The listing reads only its KEYS -- proved here, not assumed.
    let second_victim = member_bytes(11);
    remove(&engine, second_victim.clone());
    expected.remove(&second_victim);
    engine
        .compact_shard_blocks(1)
        .expect("compaction must succeed, or the arm below is about a store in an unknown state");
    the_two_agree("after_a_fold", &engine, &expected);

    // 5. AFTER A RELOAD, DONE WITH A SECOND ENGINE ON THE SAME DIRECTORY -- the shape
    //    `gated_listing_folds_the_blocks` uses, and it needs no unload entry point. The map comes
    //    back from the serialised shard index and from WAL replay, and `SetKind` deliberately does
    //    not declare `ReplaysInstallsUnrecorded`, so replay installs through `install_element`. If
    //    either route is lossy, here is where it shows.
    drop(engine);
    let reloaded = engine_on(dir.path());
    load_on(&reloaded);
    the_two_agree("after_a_reload", &reloaded, &expected);
}
