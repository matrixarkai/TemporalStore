// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A REOPEN AFTER MAINTENANCE MUST FIND EVERY WRITE THE STORE ACKNOWLEDGED.
//!
//! An older tree, driven by a scale run with async storage on, refused to reopen after a few
//! maintenance rounds (the run drives the maintenance cycle, not these lib calls):
//! a WAL replay hole refusal, "expected sequence 50682, found 54618" -- the latest dump manifest
//! anchored the replay at 50681 while WAL reclaim had already freed everything below 54618. The
//! records in between were acknowledged writes that no dump held and no log kept.
//!
//! WHAT THE RECLAIM PLAN RELIES ON. A bucket holding an undumped write names where that write sits
//! in the log (its first dirty WAL sequence), and the floor is the lowest of those. A bucket that was
//! written and never stamped looks clean, so it holds nothing back. This test writes through BOTH
//! write paths -- the single-command async path and the batch path -- to enough buckets that one
//! capped dump cannot cover them, runs several lifecycle cycles with more writes between them, and
//! then does what a restart does: drops the engine and opens a new one over the same directories.
//!
//! EVERY KEY MUST COME BACK WITH ITS OWN VALUE. A reopen that is refused serves nothing, so the
//! count of served keys is the assertion; comparing values byte for byte rules out a reopen that
//! served stale copies.
#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 1;
/// Well past the per-round dump cap in buckets, so some dirty bucket is always left undumped.
const KEYS: usize = 3_000;
const CYCLES: usize = 4;

fn key_at(item: usize) -> String {
    format!("held-{item:06}")
}

fn value_for(item: usize, generation: usize) -> Vec<u8> {
    format!("value-{item:06}-g{generation}-{}", "p".repeat(96)).into_bytes()
}

fn engine_over(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}


/// An engine whose shard is switched to async storage the way the scale run switches it: one
/// shard-level setting, after which every command is a plain command. This is a different route
/// from the per-request flag, which names the mode on one command at a time.
fn async_engine_over(dir: &std::path::Path) -> TemporalEngine {
    let engine = engine_over(dir);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: SHARD,
                config: crate::control::Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok,
        "the durability setting under test has to be the one that is set"
    );
    engine
}

/// Write through the single-command async path.
fn write_single(engine: &TemporalEngine, range: std::ops::Range<usize>, generation: usize) {
    for item in range {
        let response = engine.execute(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: key_at(item),
                    value: value_for(item, generation),
                },
            }
        );
        assert!(response.status.ok, "single write failed at {item}: {response:?}");
    }
}

/// Write through the batch path, 250 commands to a batch.
fn write_batched(engine: &TemporalEngine, range: std::ops::Range<usize>, generation: usize) {
    let mut start = range.start;
    while start < range.end {
        let end = (start + 250).min(range.end);
        let commands = (start..end)
            .map(|item| Command::StringSet {
                key: key_at(item),
                value: value_for(item, generation),
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARD,
            commands,
        });
        assert!(response.status.ok, "batch write failed at {start}: {:?}", response.status);
        start = end;
    }
}

/// The request the scale run sends every round: the shard and a delayed-destroy grace of zero,
/// everything else at the shipped bound. Built from JSON exactly as that run builds it, so a field
/// this test sets that the run does not cannot be what makes the two differ.
fn cycle(engine: &TemporalEngine) {
    let request: StorageManagerCycleRequest = serde_json::from_str(
        r#"{"shard_id":1,"block_gc_delayed_destroy_grace_ms":0}"#,
    )
    .expect("the cycle request parses");
    engine.run_storage_manager_cycle(request);
}

/// Every key as the engine serves it.
fn served(engine: &TemporalEngine) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut served = std::collections::BTreeMap::new();
    for item in 0..KEYS {
        let key = key_at(item);
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: key.clone() },
        });
        if !response.status.ok {
            continue;
        }
        if let CommandResponse::Bytes { value: Some(bytes) } = response.response {
            served.insert(key, bytes);
        }
    }
    served
}

/// Seed, then alternate cycles with churn over a moving slice, then reopen and compare.
fn drive(write: fn(&TemporalEngine, std::ops::Range<usize>, usize)) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = async_engine_over(dir.path());
    write(&engine, 0..KEYS, 0);
    let mut latest = vec![0usize; KEYS];
    for round in 0..CYCLES {
        cycle(&engine);
        let from = round * (KEYS / CYCLES);
        let to = from + KEYS / CYCLES / 2;
        write(&engine, from..to, round + 1);
        for item in from..to {
            latest[item] = round + 1;
        }
    }
    cycle(&engine);
    drop(engine);

    let reopened = engine_over(dir.path());
    let got = served(&reopened);
    // CONTROL: an empty answer must not pass for "nothing to find".
    assert!(KEYS > 0);
    assert_eq!(
        got.len(),
        KEYS,
        "the reopened store serves {} of {KEYS} acknowledged keys",
        got.len()
    );
    for item in 0..KEYS {
        assert_eq!(
            got.get(&key_at(item)),
            Some(&value_for(item, latest[item])),
            "key {item} came back with the wrong value"
        );
    }
}

#[test]
fn a_reopen_finds_every_key_written_through_the_single_command_async_path() {
    drive(write_single);
}

#[test]
fn a_reopen_finds_every_key_written_through_the_batch_path() {
    drive(write_batched);
}

const DUE_KEYS: usize = 300;

fn due_key(item: usize) -> String {
    format!("due-{item:06}")
}

/// The trigger the two arms above do not carry: keys with a deadline that fall due BETWEEN cycles,
/// so the expiry sweep removes them and anchors the served index past the tombstones it wrote.
/// A scale run refused to reopen only when such keys were present (removing them from the mix made
/// the refusal vanish; removing any other model did not).
#[test]
fn a_reopen_finds_every_live_key_after_async_deadline_keys_expire_between_cycles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = async_engine_over(dir.path());
    write_single(&engine, 0..KEYS, 0);
    for item in 0..DUE_KEYS {
        let response = engine.execute(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSetEx {
                    key: due_key(item),
                    value: vec![0x64u8; 16],
                    ttl_ms: 1_000,
                },
            }
        );
        assert!(response.status.ok, "deadline write failed at {item}: {response:?}");
    }
    let mut latest = vec![0usize; KEYS];
    cycle(&engine);
    // Let every deadline pass, so the next cycle expires them all.
    std::thread::sleep(std::time::Duration::from_millis(1_400));
    for round in 0..CYCLES {
        let from = round * (KEYS / CYCLES);
        let to = from + KEYS / CYCLES / 2;
        write_single(&engine, from..to, round + 1);
        for item in from..to {
            latest[item] = round + 1;
        }
        cycle(&engine);
    }
    // CONTROL: the arm only means something if the deadline keys really were expired by the
    // sweep, not still sitting there waiting for the reopen to drop them.
    for item in 0..DUE_KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: due_key(item) },
        });
        let present = matches!(
            response.response,
            CommandResponse::Bytes { value: Some(_) }
        );
        assert!(!present, "deadline key {item} is still served before the reopen");
    }
    drop(engine);

    let reopened = engine_over(dir.path());
    let got = served(&reopened);
    assert_eq!(
        got.len(),
        KEYS,
        "the reopened store serves {} of {KEYS} live keys",
        got.len()
    );
    for item in 0..KEYS {
        assert_eq!(
            got.get(&key_at(item)),
            Some(&value_for(item, latest[item])),
            "key {item} came back with the wrong value"
        );
    }
    for item in 0..DUE_KEYS {
        let response = reopened.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: due_key(item) },
        });
        assert!(
            !matches!(response.response, CommandResponse::Bytes { value: Some(_) }),
            "deadline key {item} came back after the reopen"
        );
    }
}

/// The deadline arrives the way the scale run delivers it: the key is written plain, a cycle dumps
/// it clean, and a LATER command gives the existing key a deadline. That command changes nothing
/// but the deadline, so whether it marks its bucket dirty is the whole question.
#[test]
fn a_reopen_finds_every_live_key_after_async_expire_commands_on_clean_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = async_engine_over(dir.path());
    write_single(&engine, 0..KEYS, 0);
    for item in 0..DUE_KEYS {
        let response = engine.execute(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: due_key(item),
                    value: vec![0x64u8; 16],
                },
            }
        );
        assert!(response.status.ok, "plain write failed at {item}: {response:?}");
    }
    cycle(&engine);
    cycle(&engine);
    for item in 0..DUE_KEYS {
        let response = engine.execute(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::CommonExpire {
                    key: due_key(item),
                    ttl_ms: 1_000,
                },
            }
        );
        assert!(response.status.ok, "expire command failed at {item}: {response:?}");
    }
    let mut latest = vec![0usize; KEYS];
    cycle(&engine);
    std::thread::sleep(std::time::Duration::from_millis(1_400));
    for round in 0..CYCLES {
        let from = round * (KEYS / CYCLES);
        let to = from + KEYS / CYCLES / 2;
        write_single(&engine, from..to, round + 1);
        for item in from..to {
            latest[item] = round + 1;
        }
        cycle(&engine);
    }
    drop(engine);

    let reopened = engine_over(dir.path());
    let got = served(&reopened);
    assert_eq!(
        got.len(),
        KEYS,
        "the reopened store serves {} of {KEYS} live keys",
        got.len()
    );
    for item in 0..KEYS {
        assert_eq!(
            got.get(&key_at(item)),
            Some(&value_for(item, latest[item])),
            "key {item} came back with the wrong value"
        );
    }
    for item in 0..DUE_KEYS {
        let response = reopened.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: due_key(item) },
        });
        assert!(
            !matches!(response.response, CommandResponse::Bytes { value: Some(_) }),
            "deadline key {item} came back after the reopen"
        );
    }
}
