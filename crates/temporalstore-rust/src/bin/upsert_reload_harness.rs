// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scale reload-equality harness for the upsert index-log delta records
//! (TS_INDEXLOG_UPSERT_DELTAS). Builds a store the way a batch-committing ingest does --
//! thousands of hash batches (HashMultiSet + StringSet only, so every batch emits ONE
//! upsert delta record), a durably-logged shard config (config-log present), threshold
//! dumps part-way through (anchored base + durable pages), and an abrupt abort in place
//! of a clean shutdown -- then reloads and asserts the SERVED view equals the view every
//! acked write implies. Each phase runs in its own process so the caller controls the
//! recovery-mode environment per phase.
//!
//! Modes:
//!   build         --root R --batches N
//!                   generation 1: write batches 0..N (each ack'd), threshold-dump at
//!                   N/3 and 2N/3, then abort.
//!   extend        --root R --batches N --extend M
//!                   generation 2: load (recovers gen 1), VERIFY the gen-1 view, write
//!                   batches N..N+M on top, then abort.
//!   verify        --root R --batches T
//!                   load (recovers everything) and assert the served view equals the
//!                   final view of batches 0..T; print a JSON report; exit nonzero on
//!                   any missing/mismatched/extra entry.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use temporalstore_rust::{
    BatchExecuteRequest, Command, CommandResponse, Config, ExecuteRequest, SetConfigRequest,
    TemporalEngine,
};

const USERS: u64 = 120;
const KEYS_PER_USER: u64 = 3;
const FIELDS_PER_KEY: u64 = 40;
const SETS_PER_BATCH: u64 = 8;
const ENTRIES_PER_SET: u64 = 3;

fn hash_key(user: u64, slot: u64) -> String {
    format!("harness:mem:{user}:{slot}")
}

fn placement_key(user: u64) -> String {
    format!("harness:placement:{user}")
}

fn field_name(index: u64) -> String {
    format!("f{index:04}")
}

fn value_of(user: u64, slot: u64, field: u64, batch: u64) -> Vec<u8> {
    // Batch-stamped payload so a stale generation of an overwritten field cannot pass,
    // padded so thousands of batches produce a store that dwarfs the cache.
    format!(
        "val-u{user}-s{slot}-f{field}-b{batch}-{}",
        "x".repeat(140)
    )
    .into_bytes()
}

fn placement_value(user: u64, batch: u64) -> Vec<u8> {
    format!("placement-u{user}-b{batch}-{}", "y".repeat(60)).into_bytes()
}

/// The commands batch `b` writes. Every command carries upsert components, so the whole
/// batch emits a single upsert delta record (the shape the scale ingest produced).
fn batch_commands(b: u64) -> Vec<Command> {
    let user = b % USERS;
    let mut commands = Vec::new();
    for s in 0..SETS_PER_BATCH {
        let slot = (b / USERS + s) % KEYS_PER_USER;
        let entries = (0..ENTRIES_PER_SET)
            .map(|e| {
                let field = (b * SETS_PER_BATCH * ENTRIES_PER_SET + s * ENTRIES_PER_SET + e)
                    % FIELDS_PER_KEY;
                (field_name(field), value_of(user, slot, field, b))
            })
            .collect();
        commands.push(Command::HashMultiSet {
            key: hash_key(user, slot),
            entries,
        });
    }
    commands.push(Command::StringSet {
        key: placement_key(user),
        value: placement_value(user, b),
    });
    commands
}

/// The exact served view batches 0..total imply: last write per (key, field) wins.
fn expected_view(
    total: u64,
) -> (
    BTreeMap<String, BTreeMap<String, Vec<u8>>>,
    BTreeMap<String, Vec<u8>>,
) {
    let mut hashes: BTreeMap<String, BTreeMap<String, Vec<u8>>> = BTreeMap::new();
    let mut strings: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for b in 0..total {
        for command in batch_commands(b) {
            match command {
                Command::HashMultiSet { key, entries } => {
                    let map = hashes.entry(key).or_default();
                    for (field, value) in entries {
                        map.insert(field, value);
                    }
                }
                Command::StringSet { key, value } => {
                    strings.insert(key, value);
                }
                _ => unreachable!("harness batches hold only upsert-component commands"),
            }
        }
    }
    (hashes, strings)
}

fn open_engine(root: &Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    )
}

fn write_batches(engine: &TemporalEngine, from: u64, to: u64, dump_at: &[u64]) {
    for b in from..to {
        let response = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands: batch_commands(b),
        });
        assert!(
            response.status.ok,
            "batch {b} not acked: {}",
            response.status.message
        );
        for (i, r) in response.responses.iter().enumerate() {
            assert!(r.status.ok, "batch {b} command {i}: {}", r.status.message);
        }
        if dump_at.contains(&b) {
            // Background threshold dump: fsync pages + persist the anchored base index,
            // the checkpoint base-only fold recovery starts from.
            engine.flush_shard_index(1);
        }
    }
}

fn verify_view(engine: &TemporalEngine, total: u64) -> bool {
    let (expected_hashes, expected_strings) = expected_view(total);
    let mut missing = 0u64;
    let mut mismatched = 0u64;
    let mut extra = 0u64;
    let mut fields_ok = 0u64;
    for (key, expected_fields) in &expected_hashes {
        let served: BTreeMap<String, Vec<u8>> = match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashGetAll { key: key.clone() },
            })
            .response
        {
            CommandResponse::HashEntries { entries } => entries.into_iter().collect(),
            _ => BTreeMap::new(),
        };
        for (field, expected_value) in expected_fields {
            match served.get(field) {
                None => missing += 1,
                Some(value) if value != expected_value => mismatched += 1,
                Some(_) => fields_ok += 1,
            }
        }
        extra += served.len() as u64 - served.keys().filter(|f| expected_fields.contains_key(*f)).count() as u64;
    }
    let mut strings_ok = 0u64;
    for (key, expected_value) in &expected_strings {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: key.clone() },
            })
            .response
        {
            CommandResponse::Bytes { value: Some(value) } if &value == expected_value => {
                strings_ok += 1
            }
            CommandResponse::Bytes { value: Some(_) } => mismatched += 1,
            _ => missing += 1,
        }
    }
    let ok = missing == 0 && mismatched == 0 && extra == 0;
    println!(
        "{{\"ok\":{ok},\"missing\":{missing},\"mismatched\":{mismatched},\"extra\":{extra},\"fields_ok\":{fields_ok},\"strings_ok\":{strings_ok},\"expected_keys\":{}}}",
        expected_hashes.len()
    );
    ok
}

fn arg(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn main() {
    let mode = arg("--mode").expect("--mode");
    let root = PathBuf::from(arg("--root").expect("--root"));
    let batches: u64 = arg("--batches").expect("--batches").parse().unwrap();
    match mode.as_str() {
        "build" => {
            let engine = open_engine(&root);
            engine.load_shard(1);
            // Durably log the shard config so the config-log is present on reload, the
            // way the production stores that hit the defect all had it.
            let mut config = Config::default();
            config.version = 2;
            let status = engine.set_config(SetConfigRequest {
                shard_id: 1,
                config,
            });
            assert!(status.ok, "set_config: {}", status.message);
            write_batches(&engine, 0, batches, &[batches / 3, 2 * batches / 3]);
            assert!(verify_view(&engine, batches), "pre-crash view already wrong");
            // Abrupt loss (the production restarts were SIGKILL-grade), never a clean
            // unload: recovery must reconstruct from the durable artifacts alone.
            std::process::abort();
        }
        "extend" => {
            let extend: u64 = arg("--extend").expect("--extend").parse().unwrap();
            let engine = open_engine(&root);
            engine.load_shard(1);
            assert!(
                verify_view(&engine, batches),
                "generation-1 view wrong after first reload"
            );
            write_batches(&engine, batches, batches + extend, &[]);
            assert!(
                verify_view(&engine, batches + extend),
                "generation-2 view wrong before crash"
            );
            std::process::abort();
        }
        "verify" => {
            let engine = open_engine(&root);
            engine.load_shard(1);
            let ok = verify_view(&engine, batches);
            std::process::exit(if ok { 0 } else { 1 });
        }
        other => panic!("unknown mode {other}"),
    }
}
