// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A RESTART AFTER MAINTENANCE ROUNDS MUST SERVE EVERY KEY AND EVERY LIST ELEMENT IT ACKNOWLEDGED.
//!
//! With async storage on, a block's only copy can be inside a write-ahead-log record until a dump
//! and a move into the block store make it durable somewhere else. A round of maintenance that
//! trims the log past such a record leaves a restart unable to find the block, and the reopen is
//! refused (`wal_resident_block_missing`) or, for containers, the elements read back empty. The
//! trim is bounded by the log store's block-retention floor, so the question this guards is
//! whether EVERY path that trims the log has set that floor from what the shard needs at that
//! moment -- the periodic loop's reclaim stage, its separate log-trim stage, and the on-demand
//! cycle each reach the trim by their own route.
//!
//! WHAT THIS DRIVES. The data node's own rounds (`run_storage_manager_once`, the loop a server
//! runs) and the on-demand cycle, over a durable engine with async storage on, a routing range of
//! 0..1023, strings, hashes, lists and keys with deadlines. Writes go between rounds, deadlines pass
//! while rounds are running, and then the engine is dropped and a new one is opened over the same
//! directories -- which is what a restart is.
//!
//! WHAT IT ASSERTS, AND WHAT KEEPS THE ASSERTION FROM BEING VACUOUS.
//!   * the reopen is accepted (a refusal's text is the failure message);
//!   * every string is served with its own last value, every list with every element in order,
//!     every hash with every field, every long-deadline key still present, every short-deadline
//!     key gone;
//!   * DENOMINATORS: some round actually moved the log's retain point (otherwise nothing was
//!     reclaimed and a missing hole proves nothing), and some key actually expired during the
//!     rounds.
#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 1;
const STRINGS: usize = 1_500;
const HASHES: usize = 120;
const LISTS: usize = 120;
const SHORT_DEADLINES: usize = 200;
const LONG_DEADLINES: usize = 100;
const MIN_ROUNDS: usize = 4;
const MAX_ROUNDS: usize = 40;

fn open(dir: &std::path::Path, first: bool) -> Result<TemporalEngine, String> {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        table_name: "reopen".to_string(),
        shard_uri: "local://reopen/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    if !loaded.status.ok {
        if !first {
            let wal = engine.write_ahead_log_store();
            let full_walk = wal.record_count(SHARD);
            let head_window = wal
                .scan_decoded_window(
                    SHARD,
                    crate::wal::ReplayPosition::at_start_of_log(),
                    16 * 1024 * 1024,
                    true,
                )
                .map(|(records, more, _)| (records.len(), more));
            eprintln!("WAL restart probe: full_walk={full_walk:?}; verified_head_window={head_window:?}");
        }
        return Err(format!("{:?}", loaded.status));
    }
    if first {
        assert!(
            engine
                .set_config(crate::SetConfigRequest {
                    shard_id: SHARD,
                    config: crate::control::Config {
                        version: 2,
                        async_storage: true,
                        ..Default::default()
                    },
                })
                .ok,
            "the durability mode under test has to be the one that is set"
        );
    }
    Ok(engine)
}

fn resp(engine: &TemporalEngine, args: &[&str]) -> crate::RespValue {
    let argv: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
    crate::execute_redis_command(argv, SHARD, |command| {
        let response = engine.execute(ExecuteRequest { shard_id: SHARD, command });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message.clone())
        }
    })
}

fn ok(engine: &TemporalEngine, args: &[&str]) {
    let reply = resp(engine, args);
    assert!(
        !matches!(reply, crate::RespValue::Error(_)),
        "the fixture's write was rejected, so it has not loaded what the model expects: {args:?} -> {reply:?}"
    );
}

fn bulk(value: &crate::RespValue) -> Option<String> {
    match value {
        crate::RespValue::Bulk(Some(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

fn texts(value: &crate::RespValue) -> Vec<String> {
    match value {
        crate::RespValue::Array(items) => items.iter().filter_map(bulk).collect(),
        _ => Vec::new(),
    }
}

fn string_value(item: usize, generation: usize) -> String {
    format!("string-{item:05}-g{generation}-{}", "p".repeat(96))
}

struct Model {
    strings: Vec<String>,
    lists: Vec<Vec<String>>,
    hashes: Vec<std::collections::BTreeMap<String, String>>,
}

/// Which kinds of write a run carries. The shipped test carries all of them; the ignored probe
/// below drops one at a time to say which kind a failure needs.
#[derive(Clone, Copy, Debug)]
struct Variant {
    deadlines: bool,
    lists: bool,
    hashes: bool,
}

const ALL: Variant = Variant { deadlines: true, lists: true, hashes: true };

fn load(engine: &TemporalEngine, model: &mut Model, variant: Variant) {
    for item in 0..STRINGS {
        let value = string_value(item, 0);
        ok(engine, &["SET", &format!("s:{item}"), &value]);
        model.strings.push(value);
    }
    for list in 0..(if variant.lists { LISTS } else { 0 }) {
        let mut elements = Vec::new();
        for element in 0..4 {
            let value = format!("l{list}-e{element}");
            ok(engine, &["RPUSH", &format!("l:{list}"), &value]);
            elements.push(value);
        }
        model.lists.push(elements);
    }
    for hash in 0..(if variant.hashes { HASHES } else { 0 }) {
        let mut fields = std::collections::BTreeMap::new();
        for field in 0..6 {
            let (name, value) = (format!("f{field}"), format!("h{hash}-f{field}-g0"));
            ok(engine, &["HSET", &format!("h:{hash}"), &name, &value]);
            fields.insert(name, value);
        }
        model.hashes.push(fields);
    }
    // The deadline keys: short ones expire while the rounds below are running, long ones must
    // outlive the whole test.
    for item in 0..(if variant.deadlines { SHORT_DEADLINES } else { 0 }) {
        let key = format!("short:{item}");
        ok(engine, &["SET", &key, "gone-soon"]);
        ok(engine, &["PEXPIRE", &key, "700"]);
    }
    for item in 0..(if variant.deadlines { LONG_DEADLINES } else { 0 }) {
        let key = format!("long:{item}");
        ok(engine, &["SET", &key, &format!("stays-{item}")]);
        ok(engine, &["PEXPIRE", &key, "600000"]);
    }
}

/// Writes between rounds: a moving slice of strings is overwritten, every list grows by one
/// element, every hash field is rewritten.
fn mutate(engine: &TemporalEngine, model: &mut Model, generation: usize, variant: Variant) {
    // Heavy for the first rounds, then a small slice. A burst that dirties more buckets every
    // round than one round's dump cap can take never lets the dumps catch up, and reclaim then
    // (correctly) never frees anything. The small slice still gives every later round a fresh
    // undumped write that reclaim has to account for.
    let heavy = generation <= 2;
    let (string_count, mut list_count, mut hash_count) = if heavy { (400, LISTS, HASHES) } else { (30, 8, 8) };
    if !variant.lists { list_count = 0; }
    if !variant.hashes { hash_count = 0; }
    let start = (generation * 331) % STRINGS;
    for step in 0..string_count {
        let item = (start + step) % STRINGS;
        let value = string_value(item, generation);
        ok(engine, &["SET", &format!("s:{item}"), &value]);
        model.strings[item] = value;
    }
    for list in 0..list_count {
        let value = format!("l{list}-g{generation}");
        ok(engine, &["RPUSH", &format!("l:{list}"), &value]);
        model.lists[list].push(value);
    }
    for hash in 0..hash_count {
        let value = format!("h{hash}-f0-g{generation}");
        ok(engine, &["HSET", &format!("h:{hash}"), "f0", &value]);
        model.hashes[hash].insert("f0".to_string(), value);
    }
}

/// Every `blocker_reasons` list and every `uncovered_bucket_count` anywhere in a report, so a
/// failure names what held reclaim back instead of printing the report.
fn reclaim_blockers(value: &serde_json::Value) -> String {
    fn walk(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, inner) in map {
                    if key == "blocker_reasons" || key == "uncovered_bucket_count" || key == "covered_bucket_count"
                        || key == "retain_from_wal_sequence" || key == "current_wal_sequence"
                    {
                        out.push(format!("{key}={inner}"));
                    } else {
                        walk(inner, out);
                    }
                }
            }
            serde_json::Value::Array(items) => items.iter().for_each(|item| walk(item, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(value, &mut out);
    out.join(" ")
}

/// The lowest sequence the write-ahead log still holds. Above 1 means reclaim has freed records.
/// Read from the log itself, so the denominator does not depend on which report a route returns.
fn first_retained_wal_sequence(engine: &TemporalEngine) -> u64 {
    engine
        .wal_store()
        .scan(SHARD, 0, u64::MAX, u64::MAX)
        .ok()
        .and_then(|records| records.first().map(|(id, _)| *id))
        .unwrap_or(0)
}

fn highest_retain_point(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(key, inner)| {
                if key == "retain_from_wal_sequence" {
                    inner.as_u64().unwrap_or(0)
                } else {
                    highest_retain_point(inner)
                }
            })
            .max()
            .unwrap_or(0),
        serde_json::Value::Array(items) => items.iter().map(highest_retain_point).max().unwrap_or(0),
        _ => 0,
    }
}

#[derive(Clone, Copy, Debug)]
enum Route {
    /// `DataNodeRuntime::run_storage_manager_once`, the loop a server starts with, with the
    /// per-round cap on blocks moved out of the log lifted so the log drains inside the rounds.
    PeriodicLoop,
    /// The same loop at its DEFAULT cap. A round moves only a few blocks out of the log, so most of
    /// what the shard wrote is still named by a retained dump when the round's own log trim runs;
    /// the trim must leave those records alone. Nothing here requires the log to shrink, only that
    /// whatever it does shrink by still serves every acknowledged write after a restart.
    PeriodicLoopDefaultCap,
    /// `TemporalEngine::run_storage_manager_cycle`, the on-demand cycle.
    OnDemandCycle,
}

fn drive(route: Route) {
    if let Err(failure) = drive_with(route, ALL) {
        panic!("{route:?}: {failure}");
    }
}

fn drive_with(route: Route, variant: Variant) -> Result<(), String> {
    // Whether this route has to have shrunk the log for the run to prove anything.
    let needs_trim = !matches!(route, Route::PeriodicLoopDefaultCap);
    let dir = tempfile::tempdir().expect("tempdir");
    let mut model = Model { strings: Vec::new(), lists: Vec::new(), hashes: Vec::new() };
    let retain_moved;
    let diagnostic;
    {
        let engine = open(dir.path(), true).expect("the first open is accepted");
        load(&engine, &mut model, variant);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine.clone(),
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let one_round = |engine: &TemporalEngine| -> (u64, String) {
            let json = match route {
                Route::PeriodicLoop | Route::PeriodicLoopDefaultCap => serde_json::to_value(
                    // The shipped loop takes a dump only after 128 MiB of undumped log (the record
                    // trigger is off), so a fixture this size would never reach a dump and reclaim
                    // would have nothing to trim. The trigger is lowered so the rounds below
                    // exercise dump and reclaim together. The per-round cap on blocks moved out of
                    // the log is lifted too: at its default a round moves a handful while this
                    // fixture writes dozens, so the log would never drain inside a test's rounds
                    // and no trim would happen for a reopen to be wrong about. Nothing else is
                    // changed from the default.
                    &runtime.run_storage_manager_once(
                        SHARD,
                        StorageManagerOptions {
                            min_undumped_wal_records: 1,
                            min_undumped_wal_bytes: 1,
                            max_dump_buckets_per_round: if matches!(route, Route::PeriodicLoop) {
                                0
                            } else {
                                StorageManagerOptions::default().max_dump_buckets_per_round
                            },
                            ..StorageManagerOptions::default()
                        },
                    ),
                ),
                Route::OnDemandCycle => {
                    let request: StorageManagerCycleRequest = serde_json::from_str(
                        r#"{"shard_id":1,"block_gc_delayed_destroy_grace_ms":0}"#,
                    )
                    .expect("the cycle request parses");
                    serde_json::to_value(&engine.run_storage_manager_cycle(request))
                }
            }
            .expect("the report serialises");
            (highest_retain_point(&json), reclaim_blockers(&json))
        };
        // Rounds, with writes between them, until reclaim has moved the log's retain point AFTER
        // the deadlines have passed -- the order the failure needed. The dump cap spreads a
        // store's dirty buckets over several rounds, so how many rounds that takes is not
        // knowable up front; the bound is what stops a store that never reclaims.
        //
        // "Moved" is read two ways, either of which counts: the retain point a round's report
        // names, and the lowest sequence the log still physically holds. A report names the
        // decision; the log is cut in whole segments, so it can lag the decision.
        let mut deadlines_passed = false;
        let mut moved_after_deadlines = false;
        let mut rounds_run = 0;
        let mut last_report = String::new();
        for generation in 0..MAX_ROUNDS {
            if generation == 1 {
                // The deadlines pass during the rounds, so the sweep that removes them runs
                // inside the loop under test rather than before it.
                std::thread::sleep(std::time::Duration::from_millis(900));
                deadlines_passed = true;
            }
            let (reported, text) = one_round(&engine);
            last_report = text;
            rounds_run += 1;
            if deadlines_passed && (reported > 1 || first_retained_wal_sequence(&engine) > 1) {
                moved_after_deadlines = true;
            }
            mutate(&engine, &mut model, generation + 1, variant);
            if (moved_after_deadlines || !needs_trim) && deadlines_passed && rounds_run >= MIN_ROUNDS {
                break;
            }
        }
        retain_moved = moved_after_deadlines;
        diagnostic = format!("{rounds_run} rounds; last report: {last_report}");
        // One more round after the last writes, so the final writes are the ones reclaim sees last.
        let _ = one_round(&engine);
    }

    // DENOMINATOR: reclaim moved the log's retain point at least once. Without it a store that
    // never trimmed would pass this test for the wrong reason.
    if needs_trim && !retain_moved {
        return Err(format!(
            "no round moved the write-ahead log's retain point, so a missing replay hole proves \
             nothing about reclaim ({diagnostic})"
        ));
    }

    // What a restart is: nothing of the first engine survives but its directories.
    let reopened = open(dir.path(), false).map_err(|refusal| format!("the reopen was refused: {refusal}"))?;

    let mut missing = Vec::new();
    for (item, want) in model.strings.iter().enumerate() {
        let got = bulk(&resp(&reopened, &["GET", &format!("s:{item}")]));
        if got.as_deref() != Some(want.as_str()) {
            missing.push(format!("s:{item}"));
        }
    }
    if !missing.is_empty() {
        return Err(format!("{} strings did not come back with their last value, first {:?}", missing.len(), &missing[..missing.len().min(5)]));
    }

    let mut missing = Vec::new();
    for (list, want) in model.lists.iter().enumerate() {
        let got = texts(&resp(&reopened, &["LRANGE", &format!("l:{list}"), "0", "-1"]));
        if &got != want {
            missing.push(format!("l:{list} ({} of {})", got.len(), want.len()));
        }
    }
    if !missing.is_empty() {
        return Err(format!("{} lists did not come back whole, first {:?}", missing.len(), &missing[..missing.len().min(5)]));
    }

    let mut missing = Vec::new();
    for (hash, want) in model.hashes.iter().enumerate() {
        let reply = resp(&reopened, &["HGETALL", &format!("h:{hash}")]);
        let flat = texts(&reply);
        let got: std::collections::BTreeMap<String, String> =
            flat.chunks(2).filter(|pair| pair.len() == 2).map(|pair| (pair[0].clone(), pair[1].clone())).collect();
        if &got != want {
            missing.push(format!("h:{hash} ({} of {})", got.len(), want.len()));
        }
    }
    if !missing.is_empty() {
        return Err(format!("{} hashes did not come back whole, first {:?}", missing.len(), &missing[..missing.len().min(5)]));
    }

    if variant.deadlines {
        // The long deadlines are all still there, the short ones are all gone.
        let survivors = (0..LONG_DEADLINES)
            .filter(|item| bulk(&resp(&reopened, &["GET", &format!("long:{item}")])).is_some())
            .count();
        if survivors != LONG_DEADLINES {
            return Err(format!("only {survivors} of {LONG_DEADLINES} long-deadline keys survived the reopen"));
        }
        let still_there = (0..SHORT_DEADLINES)
            .filter(|item| bulk(&resp(&reopened, &["GET", &format!("short:{item}")])).is_some())
            .count();
        if still_there != 0 {
            return Err(format!("{still_there} of {SHORT_DEADLINES} keys past their deadline were served after the reopen"));
        }
    }
    Ok(())
}

#[test]
fn a_reopen_after_loop_rounds_with_deadlines_serves_every_key_and_list_element() {
    drive(Route::PeriodicLoop);
}

#[test]
fn a_reopen_after_default_cap_loop_rounds_with_deadlines_serves_every_key_and_list_element() {
    drive(Route::PeriodicLoopDefaultCap);
}

#[test]
fn a_reopen_after_cycle_rounds_with_deadlines_serves_every_key_and_list_element() {
    drive(Route::OnDemandCycle);
}

/// Which kind of write a failing run needs: the same drive with one kind removed at a time.
///
///   cargo test --release -p temporalstore-rust --lib which_writes_a_failing_reopen_needs -- --ignored --nocapture
#[test]
#[ignore]
fn which_writes_a_failing_reopen_needs() {
    let variants = [
        ("all", ALL),
        ("no deadlines", Variant { deadlines: false, ..ALL }),
        ("no lists", Variant { lists: false, ..ALL }),
        ("no hashes", Variant { hashes: false, ..ALL }),
        ("strings only", Variant { deadlines: false, lists: false, hashes: false }),
    ];
    for route in [Route::PeriodicLoop, Route::PeriodicLoopDefaultCap, Route::OnDemandCycle] {
        for (name, variant) in variants {
            let outcome = std::panic::catch_unwind(|| drive_with(route, variant));
            let text = match outcome {
                Ok(Ok(())) => "PASS".to_string(),
                Ok(Err(failure)) => format!("FAIL: {}", failure.chars().take(260).collect::<String>()),
                Err(_) => "PANIC".to_string(),
            };
            println!("PROBE {route:?} / {name}: {text}");
        }
    }
}
