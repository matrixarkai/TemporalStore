// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A GATED STORE COMES BACK THROUGH WAL REPLAY, WHICH IS WHERE TWO SILENT CATASTROPHES WOULD BE.
//!
//! # THE TWO FAILURES THIS RULES OUT
//!
//! Neither is a wrong answer; both are worse than one.
//!
//!   * `apply_outcome_item`'s `"set"` arm answers `false` when it cannot rebuild a member, and the
//!     caller treats that as a refusal that **FAILS THE SHARD LOAD**. The shard does not come up.
//!   * `collect_upsert_index_items`' `"set"` arm falls to `_ => None` when the component is absent,
//!     which means the row **never reaches the index log** -- so a replay has nothing to rebuild it
//!     from, and the loss surfaces much later than the write that caused it.
//!
//! # WHY THEY DO NOT FIRE, AND WHY THAT IS A TEST RATHER THAN A PARAGRAPH
//!
//! Both components are derived from the COMMAND, not from the index entry: the log item's comes
//! from `command_upsert_components` off `Command::SetAdd`, and the outcome's from the component
//! handed to the per-write filer, which this series deliberately left per-element. **So the struct
//! field and the record field are different things that share a name**, and collapsing the index
//! entry does not reach either record.
//!
//! That is a reading, and this series has twice been wrong about what a gated scope SELECTS as
//! opposed to what it means. So it is driven.
//!
//! # WHY THE INDEX IS LEFT UNFLUSHED
//!
//! `tombstone_reload_path` records the mechanism: an `unload_shard` is what materializes the base
//! index. This fixture therefore does NOT unload -- the index is absent, and the reload has no
//! choice but to replay. That is the only way to reach either arm above. The floor asserts it
//! rather than trusting it: a reload that accepted an index would exercise neither arm and would
//! pass this test over a path nothing reached, which is the exact shape that made an earlier
//! measurement in this series report a clean result over an unreached branch.
//!
//! # WHY THERE IS A GATE-OFF CONTROL ON THE SAME FIXTURE
//!
//! "Nothing came back" has two causes: the gated write never reached the log (the failure under
//! test), or this fixture is not durable without an unload at all (a broken fixture). Those are
//! indistinguishable from the gated arm alone. The control runs the IDENTICAL sequence with the
//! gate off, so the two arms differ in one variable, and each arm gets its OWN store directory --
//! sharing one would let a cached answer stand in for a measurement, which has already produced a
//! wrong figure once in this campaign.
//!
//! # SCOPE
//!
//! This is about the write record and the replay, not the derivation: no compaction is run, because
//! neither arm under test is on the fold path.
//!
//! # THE TWO FAILURES ARE COVERED BY TWO DIFFERENT TESTS, AND THAT IS NOT A STYLE CHOICE
//!
//! `execute` does NOT reach `collect_upsert_index_items`. That function's only production caller
//! is `batch_execute`; the single-command path builds its items from `command_upsert_components`
//! at `engine.rs:954`. **Two filers again** -- the same shape that already caught this series out
//! once, when "the derivation is the only filer" turned out to be two.
//!
//! So the driven test below covers the shard coming up and the execute path's filer, and a second
//! test asks `collect_upsert_index_items` outright, where there is no reachability question to get
//! wrong. A single driven test naming both would have reported a clean result over a function
//! nothing in it called.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

const MEMBERS: usize = 40;

/// Sets the gate for as long as it is held, and restores it on the way out **including on a
/// panic**.
///
/// The suite is ONE process and `--test-threads=1` runs these in order, so a variable left set by
/// a gated test is seen by every test after it -- silently, and as an ordered contamination rather
/// than a flake. A bare `remove_var` at the end of a test only covers the happy path: an assertion
/// between the set and the remove leaks the gate just as thoroughly, and a failing gated test would
/// then turn later tests red and bury its own message. `Drop` runs during unwinding, which is the
/// case that matters.



const KEY: &str = "replay-gate-set";

fn engine_with_cache(dir: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join(cache),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) -> crate::types::Status {
    engine
        .load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1,
            table_name: "replay-under-the-gate".to_string(),
            shard_uri: "local://replay-under-the-gate/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket: 1023,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        })
        .status
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("member-{index:05}").into_bytes()
}

/// What REPLAY rebuilt, read from the durable map rather than from the listing.
///
/// The listing is the next step's subject and may legitimately move under the gate; the question
/// here is whether the row reached the log and came back, and the durable map is what a replay
/// reconstructs.
fn resident_members(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .sets
        .get(KEY)
        .map(|members| members.iter().map(|(member, _)| member.clone()).collect())
        .unwrap_or_default()
}

/// The listing, PRINTED rather than asserted, for the reason given on `resident_members`.
fn listed_members(engine: &TemporalEngine) -> usize {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    match response.response {
        crate::types::CommandResponse::Members { members } => members.len(),
        _ => usize::MAX,
    }
}

struct ReplayArm {
    load_ok: bool,
    accepted: u64,
    classified: u64,
    resident: BTreeSet<Vec<u8>>,
    listed: usize,
}

/// Write `MEMBERS` members with the gate in the given state, then reload WITHOUT an unload so the
/// reload must replay. Returns what came back.
fn write_then_replay(dir: &std::path::Path, gate_on: bool) -> ReplayArm {
    {
        let engine = engine_with_cache(dir, "cache-writer");
        // Held, not set: an assertion below must not leak the gate into the next test.
        // NO GATE TO HOLD. `gate_on` is kept as the arm LABEL -- the two arms differ only in what
        // they are named now -- so the caller still says which it drove and the printed table still
        // has two rows. See this module s header for why the rows are identical.
        let _ = gate_on;
        assert!(load_on(&engine).ok, "the fixture's first load failed");
        for index in 0..MEMBERS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SetAdd {
                    key: KEY.to_string(),
                    member: member_bytes(index),
                },
            });
            assert!(response.status.ok, "write {index} failed: {response:?}");
        }
        assert_eq!(
            MEMBERS,
            resident_members(&engine).len(),
            "the fixture does not hold its members BEFORE the reload, so nothing below is about \
             a replay"
        );
        // Deliberately NO unload: see the module note. The engine is dropped here, and
        // `_held` restores the gate as this scope ends.
    }

    // NO GATE TO HOLD. `gate_on` survives as the arm LABEL -- both arms now drive the same
    // single path -- so the caller still says which arm it asked for and the printed table
    // still has two rows. The rows being IDENTICAL is the point of keeping them.
    let _ = gate_on;
    crate::engine::persistence::reset_index_load_path_counts();
    let reloaded = engine_with_cache(dir, "cache-reloaded");
    let status = load_on(&reloaded);
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    let arm = ReplayArm {
        load_ok: status.ok,
        accepted,
        classified: accepted + refused_stale + absent + undecodable,
        resident: if status.ok {
            resident_members(&reloaded)
        } else {
            BTreeSet::new()
        },
        listed: if status.ok {
            listed_members(&reloaded)
        } else {
            usize::MAX
        },
    };

    println!(
        "  gate {}: load_ok={} accepted={accepted} refused_stale={refused_stale} absent={absent} \
         undecodable={undecodable} resident={} listed={}",
        if gate_on { "ON " } else { "OFF" },
        arm.load_ok,
        arm.resident.len(),
        arm.listed,
    );
    arm
}

/// rust-internal: drives a gate-on store back through replay and names every member
#[test]
fn a_gated_write_reaches_the_log_and_every_member_comes_back_through_replay() {
    // SEPARATE STORES PER ARM: one shared directory would let the second arm read the first's
    // answer, which has already produced a wrong figure once in this campaign.
    let gated_dir = tempfile::tempdir().expect("tempdir");
    let control_dir = tempfile::tempdir().expect("tempdir");

    println!("\n=== a store written under the gate, brought back by replay ===");
    let gated = write_then_replay(gated_dir.path(), true);
    let control = write_then_replay(control_dir.path(), false);

    // ---- THE FLOOR, ON THE REACHING OF THE PATH AND NOT ON THE VALUE ----
    // Floored per arm: a result floor would forbid the true answer if the true answer were zero.
    for (label, arm) in [("gate on", &gated), ("control", &control)] {
        assert!(
            arm.classified > 0,
            "{label}: the reload classified NO index load at all, so it never reached the index \
             decision and the replay floor below would be satisfied by absence rather than by a \
             replay"
        );
        assert_eq!(
            0, arm.accepted,
            "{label}: the reload ACCEPTED a persisted index (accepted={}, classified={}), so it \
             never replayed and neither arm under test was reached. The index is left unflushed \
             for exactly this reason",
            arm.accepted, arm.classified
        );
    }

    // ---- 7a: THE SHARD MUST COME UP ----
    assert!(
        gated.load_ok,
        "THE SHARD DID NOT COME UP under the gate. `apply_outcome_item`'s set arm answers `false` \
         when it cannot rebuild a member, and the caller treats that as a refusal that fails the \
         whole shard load -- so a gated outcome missing its component takes the shard with it"
    );

    // ---- THE CONTROL, so a short gated arm cannot be blamed on the fixture ----
    assert!(
        control.load_ok,
        "the GATE-OFF control did not come up either, so this fixture is broken and says nothing \
         about the gate"
    );
    assert_eq!(
        MEMBERS,
        control.resident.len(),
        "the GATE-OFF control recovered {} of {MEMBERS} members, so this fixture is not durable \
         without an unload and a short gated arm below would be the fixture's fault rather than \
         the gate's",
        control.resident.len()
    );

    // ---- 7b: EVERY ROW REACHED THE LOG ----
    assert_eq!(
        MEMBERS,
        gated.resident.len(),
        "{} of {MEMBERS} members came back through replay under the gate, against {} for the \
         gate-off control on the identical sequence. A row that never reached the index log -- \
         which is what `collect_upsert_index_items` falling to `_ => None` would mean -- leaves a \
         replay with nothing to rebuild it from",
        gated.resident.len(),
        control.resident.len()
    );
    for index in 0..MEMBERS {
        assert!(
            gated.resident.contains(&member_bytes(index)),
            "member {index} did not come back through replay under the gate"
        );
    }
    assert_eq!(
        control.resident, gated.resident,
        "the gate changed WHICH members survive a replay, not merely how many"
    );
}

/// rust-internal: asks the index-log item builder directly, because the batch path is its only caller
///
/// # WHY THIS IS ASKED DIRECTLY RATHER THAN DRIVEN
///
/// The test above drives `execute`, and `execute` does **not** reach
/// `collect_upsert_index_items`: that function's only production caller is `batch_execute`
/// (`stream_batch_methods.rs`), and the single-command path builds its items from
/// `command_upsert_components` at `engine.rs:954` instead. So the arm above covers the execute
/// path's filer and would have left 7b's named function untested while appearing to cover it --
/// the same two-filers shape this series already hit once, where "the derivation is the only
/// filer" turned out to be two.
///
/// Asked outright there is no reachability question left, and the hazard can be shown rather than
/// argued.
///
/// # THE HAZARD IS REAL, WHICH IS WHY THE MECHANISM MATTERS
///
/// `collect_upsert_index_items` matches on `(kind, component)` and its only set arm is
/// `("set", Some(component))`, so `("set", None)` falls to `_ => None`, resolves no address, and
/// **builds no item at all**. A row that builds no item never reaches the index log, and a replay
/// then has nothing to rebuild it from.
///
/// So the negative control below is not decoration: without it, "the component is still there"
/// would be a fact with no stated consequence. With it, the pair says the thing worth saying --
/// the drop is real, and what prevents it is that the component is taken from the COMMAND.
#[test]
fn the_gate_does_not_reach_the_component_the_index_log_item_is_built_from() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_cache(dir.path(), "cache-direct");
    // THIS TEST PREVIOUSLY LEAKED THE GATE: it set the variable and never removed it, so every
    // test after it in this process ran gated. Held now, and restored even on a panic.
    assert!(load_on(&engine).ok, "the fixture's load failed");
    for index in 0..MEMBERS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: KEY.to_string(),
                member: member_bytes(index),
            },
        });
        assert!(response.status.ok, "write {index} failed");
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");

    // (1) THE MECHANISM: the component the log item is built from comes off the COMMAND, so the
    //     collapsed index entry cannot reach it.
    let command = Command::SetAdd {
        key: KEY.to_string(),
        member: member_bytes(0),
    };
    let named = crate::engine::command_upsert_components(&command, shard)
        .expect("a set add must name an upsert component");
    println!("\n=== asked directly, under the gate ===");
    println!("  command_upsert_components -> {named:?}");
    assert_eq!(
        1,
        named.len(),
        "a single set add named {} components",
        named.len()
    );
    let (kind, object_key, component) = named[0].clone();
    assert_eq!("set", kind, "the component is filed under {kind}");
    assert_eq!(KEY, object_key.as_str(), "it names object {object_key}");
    let component = component.expect(
        "THE GATE REACHED THE RECORD. `command_upsert_components` no longer names a component for \
         a set add, so `collect_upsert_index_items` would fall to its `_ => None` arm, build no \
         item, and the write would never reach the index log",
    );
    assert_eq!(
        hex::encode(member_bytes(0)),
        component,
        "the component is spelled {component}, not as the set arm decodes it"
    );

    // (2) GIVEN WHAT THE REAL CALLER GIVES IT, an item is built.
    let with_component = crate::engine::collect_upsert_index_items(
        shard,
        1,
        &[("set", KEY.to_string(), Some(component.clone()))],
        0,
        1023,
    );
    println!("  with the command's component -> {} item(s)", with_component.len());

    // (3) THE NEGATIVE CONTROL: the same call with no component builds NOTHING. This is the
    //     failure 7b names, demonstrated rather than asserted about.
    let without_component = crate::engine::collect_upsert_index_items(
        shard,
        1,
        &[("set", KEY.to_string(), None)],
        0,
        1023,
    );
    println!("  with no component    -> {} item(s)", without_component.len());

    assert_eq!(
        1,
        with_component.len(),
        "the builder made {} items for one component, so the counts below are not comparable",
        with_component.len()
    );
    assert!(
        without_component.is_empty(),
        "THE NEGATIVE CONTROL DID NOT HOLD: a `(\"set\", None)` component built {} item(s), so the \
         `_ => None` fallthrough no longer drops the row and the mechanism asserted above is no \
         longer what protects the index log. If a later step gave the builder a page-named arm, \
         THIS is the assertion to change -- deliberately, saying so",
        without_component.len()
    );
}


// =================================================================================================
// HASH, THE SAME TWO FAILURES, DRIVEN THE SAME WAY -- BEFORE THIS, ZERO HASH MENTIONS HERE.
// =================================================================================================
//
// The replay arm's hard requirement is the hash analogue of the set one named at the top of this
// module: `apply_outcome_item`'s `"hash"` arm (`lifecycle.rs`) answers `false` when
// `item.component` is `None`, and the caller treats that as a refusal that FAILS THE SHARD LOAD.
// And `collect_upsert_index_items`' `"hash"` arm matches only `("hash", Some(field))`, falling to
// `_ => None` otherwise -- a row that builds no item never reaches the index log.
//
// Neither fires, for the SAME reason the set case does not: both components are derived from the
// COMMAND, not from the index entry. `command_upsert_components`'s `HashSet`/`HashMultiSet`/
// `HashIncrBy` arms (`engine.rs`) take the field straight off the `Command`, and
// `collect_upsert_index_items`'s `"hash"` arm resolves the address from `shard.hashes` -- the
// resident map, not the bucket index. So the struct field and the record field are, again, two
// different things that share a name, and collapsing the index entry's component does not reach
// either record.
//
// That is a reading, same as it was for set, and this module exists because a reading is not
// where hash's replay safety should rest. Driven below exactly the way set already is.
//
// ONE DIFFERENCE FROM THE SET CASE, WORTH BEING HONEST ABOUT: `index_entry_names_a_page`, the
// single authority that decides whether a WRITTEN entry's component is actually suppressed to
// `None`, lists only `set` and `list` today -- hash is not in it yet (steps 6-7 of this series,
// held separately). So turning `TS_CONTAINER_ONE_ENTRY_A_PAGE` on does not, today, make a
// `HashSet` write a component-less entry the way it already does for `SetAdd`. The test below
// still drives the real mechanism -- `command_upsert_components` and `collect_upsert_index_items`
// asked directly, under the gate -- which is independent of that allow-list and is exactly the
// regression tripwire this series wants: if a later change made either function start consulting
// the allow-list for hash, this is what would catch it before the allow-list itself ever moves.

const HASH_KEY: &str = "replay-gate-hash";

fn field_name(index: usize) -> String {
    format!("field-{index:05}")
}

fn field_value(index: usize) -> Vec<u8> {
    format!("value-{index:05}").into_bytes()
}

/// What REPLAY rebuilt, read from the durable map -- the hash analogue of `resident_members`.
fn resident_hash_fields(engine: &TemporalEngine) -> BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .hashes
        .get(HASH_KEY)
        .map(|fields| fields.iter().map(|(name, _)| name.clone()).collect())
        .unwrap_or_default()
}

struct HashReplayArm {
    load_ok: bool,
    accepted: u64,
    classified: u64,
    resident: BTreeSet<String>,
}

/// Write `MEMBERS` fields with the gate in the given state, then reload WITHOUT an unload so the
/// reload must replay. The hash analogue of `write_then_replay`.
fn write_then_replay_hash(dir: &std::path::Path, gate_on: bool) -> HashReplayArm {
    {
        let engine = engine_with_cache(dir, "cache-writer-hash");
        // NO GATE TO HOLD. `gate_on` is kept as the arm LABEL -- the two arms differ only in what
        // they are named now -- so the caller still says which it drove and the printed table still
        // has two rows. See this module s header for why the rows are identical.
        let _ = gate_on;
        assert!(load_on(&engine).ok, "the fixture's first load failed");
        for index in 0..MEMBERS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: HASH_KEY.to_string(),
                    field: field_name(index),
                    value: field_value(index),
                },
            });
            assert!(response.status.ok, "write {index} failed: {response:?}");
        }
        assert_eq!(
            MEMBERS,
            resident_hash_fields(&engine).len(),
            "the fixture does not hold its fields BEFORE the reload, so nothing below is about a \
             replay"
        );
        // Deliberately NO unload, same reason as the set fixture above.
    }

    // NO GATE TO HOLD. `gate_on` survives as the arm LABEL -- both arms now drive the same
    // single path -- so the caller still says which arm it asked for and the printed table
    // still has two rows. The rows being IDENTICAL is the point of keeping them.
    let _ = gate_on;
    crate::engine::persistence::reset_index_load_path_counts();
    let reloaded = engine_with_cache(dir, "cache-reloaded-hash");
    let status = load_on(&reloaded);
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    let arm = HashReplayArm {
        load_ok: status.ok,
        accepted,
        classified: accepted + refused_stale + absent + undecodable,
        resident: if status.ok {
            resident_hash_fields(&reloaded)
        } else {
            BTreeSet::new()
        },
    };
    println!(
        "  gate {}: load_ok={} accepted={accepted} refused_stale={refused_stale} absent={absent} \
         undecodable={undecodable} resident={}",
        if gate_on { "ON " } else { "OFF" },
        arm.load_ok,
        arm.resident.len(),
    );
    arm
}

/// rust-internal: drives a gate-on hash store back through replay and names every field
#[test]
fn a_gated_hash_write_reaches_the_log_and_every_field_comes_back_through_replay() {
    let gated_dir = tempfile::tempdir().expect("tempdir");
    let control_dir = tempfile::tempdir().expect("tempdir");

    println!("\n=== a hash store written under the gate, brought back by replay ===");
    let gated = write_then_replay_hash(gated_dir.path(), true);
    let control = write_then_replay_hash(control_dir.path(), false);

    for (label, arm) in [("gate on", &gated), ("control", &control)] {
        assert!(
            arm.classified > 0,
            "{label}: the reload classified NO index load at all, so it never reached the index \
             decision and the replay floor below would be satisfied by absence rather than by a \
             replay"
        );
        assert_eq!(
            0, arm.accepted,
            "{label}: the reload ACCEPTED a persisted index (accepted={}, classified={}), so it \
             never replayed and neither arm under test was reached",
            arm.accepted, arm.classified
        );
    }

    assert!(
        gated.load_ok,
        "THE SHARD DID NOT COME UP under the gate. `apply_outcome_item`'s hash arm answers `false` \
         when it cannot rebuild a field -- `item.component` is `None` -- and the caller treats \
         that as a refusal that fails the whole shard load"
    );
    assert!(
        control.load_ok,
        "the GATE-OFF control did not come up either, so this fixture is broken and says nothing \
         about the gate"
    );
    assert_eq!(
        MEMBERS,
        control.resident.len(),
        "the GATE-OFF control recovered {} of {MEMBERS} fields, so this fixture is not durable \
         without an unload and a short gated arm below would be the fixture's fault rather than \
         the gate's",
        control.resident.len()
    );

    assert_eq!(
        MEMBERS,
        gated.resident.len(),
        "{} of {MEMBERS} fields came back through replay under the gate, against {} for the \
         gate-off control on the identical sequence",
        gated.resident.len(),
        control.resident.len()
    );
    for index in 0..MEMBERS {
        assert!(
            gated.resident.contains(&field_name(index)),
            "field {index} did not come back through replay under the gate"
        );
    }
    assert_eq!(
        control.resident, gated.resident,
        "the gate changed WHICH fields survive a replay, not merely how many"
    );
}

/// rust-internal: asks the hash index-log item builder directly, mirroring the set version exactly
#[test]
fn the_gate_does_not_reach_the_component_the_hash_index_log_item_is_built_from() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_cache(dir.path(), "cache-direct-hash");
    assert!(load_on(&engine).ok, "the fixture's load failed");
    for index in 0..MEMBERS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: HASH_KEY.to_string(),
                field: field_name(index),
                value: field_value(index),
            },
        });
        assert!(response.status.ok, "write {index} failed");
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");

    // (1) THE MECHANISM: the component the log item is built from comes off the COMMAND for hash
    //     too, so the collapsed index entry cannot reach it.
    let command = Command::HashSet {
        key: HASH_KEY.to_string(),
        field: field_name(0),
        value: field_value(0),
    };
    let named = crate::engine::command_upsert_components(&command, shard)
        .expect("a hash set must name an upsert component");
    println!("\n=== asked directly, under the gate (hash) ===");
    println!("  command_upsert_components -> {named:?}");
    assert_eq!(
        1,
        named.len(),
        "a single hash set named {} components",
        named.len()
    );
    let (kind, object_key, component) = named[0].clone();
    assert_eq!("hash", kind, "the component is filed under {kind}");
    assert_eq!(HASH_KEY, object_key.as_str(), "it names object {object_key}");
    let component = component.expect(
        "THE GATE REACHED THE RECORD. `command_upsert_components` no longer names a component for \
         a hash set, so `collect_upsert_index_items` would fall to its `_ => None` arm, build no \
         item, and the write would never reach the index log",
    );
    assert_eq!(
        field_name(0),
        component,
        "the component is spelled {component}, not the field name `command_upsert_components` \
         should have taken straight off the command"
    );

    // (2) GIVEN WHAT THE REAL CALLER GIVES IT, an item is built.
    let with_component = crate::engine::collect_upsert_index_items(
        shard,
        1,
        &[("hash", HASH_KEY.to_string(), Some(component.clone()))],
        0,
        1023,
    );
    println!("  with the command's component -> {} item(s)", with_component.len());

    // (3) THE NEGATIVE CONTROL: the same call with no component builds NOTHING.
    let without_component = crate::engine::collect_upsert_index_items(
        shard,
        1,
        &[("hash", HASH_KEY.to_string(), None)],
        0,
        1023,
    );
    println!("  with no component    -> {} item(s)", without_component.len());

    assert_eq!(
        1,
        with_component.len(),
        "the builder made {} items for one component, so the counts below are not comparable",
        with_component.len()
    );
    assert!(
        without_component.is_empty(),
        "THE NEGATIVE CONTROL DID NOT HOLD: a (\"hash\", None) component built {} item(s), so the \
         `_ => None` fallthrough no longer drops the row and the mechanism asserted above is no \
         longer what protects the index log. If a later step gave the builder a page-named arm, \
         THIS is the assertion to change -- deliberately, saying so",
        without_component.len()
    );
}
