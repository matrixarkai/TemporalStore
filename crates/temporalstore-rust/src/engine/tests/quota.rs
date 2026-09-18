// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Per-shard rate limit, exercised through the engine.
#![allow(clippy::all)]
use super::*;
use crate::engine::quota::ShardQuotaConfig;

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn write(engine: &TemporalEngine, key: &str) -> Status {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            },
        })
        .status
}

fn read(engine: &TemporalEngine, key: &str) -> Status {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: key.to_string(),
            },
        })
        .status
}

/// With nothing configured a shard is not limited, which is what every existing deployment gets.
#[test]
fn a_shard_with_no_limit_set_is_not_limited() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..500 {
        assert!(
            write(&engine, &format!("k{index}")).ok,
            "an unconfigured shard refused a write"
        );
    }
    assert!(engine.shard_quota(1).is_none());
}

/// A write limit refuses writes past the burst, and says why.
#[test]
fn a_write_limit_refuses_writes_past_the_burst() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.set_shard_quota(
        1,
        ShardQuotaConfig {
            write_qps: 10,
            write_burst: 5,
            ..Default::default()
        },
    );

    let mut allowed = 0;
    let mut refused = None;
    for index in 0..100 {
        let status = write(&engine, &format!("k{index}"));
        if status.ok {
            allowed += 1;
        } else {
            refused = Some(status);
            break;
        }
    }
    assert!(
        (1..=6).contains(&allowed),
        "the burst is what may be spent before any time passes, saw {allowed}"
    );
    let refused = refused.expect("the limit should have refused something");
    assert_eq!(refused.code, "quota_exhausted");
    assert!(
        refused.message.contains("write"),
        "the refusal should say which direction ran out: {}",
        refused.message
    );
}

/// A write limit does not touch reads.
#[test]
fn a_write_limit_leaves_reads_alone() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.set_shard_quota(
        1,
        ShardQuotaConfig {
            write_qps: 1,
            write_burst: 1,
            ..Default::default()
        },
    );
    // Spend the write credit.
    write(&engine, "k");
    write(&engine, "k");
    // Reads are a separate direction and were never limited.
    for _ in 0..200 {
        assert!(read(&engine, "k").ok, "a write limit refused a read");
    }
}

/// Limits can be changed on a running engine.
#[test]
fn a_limit_can_be_changed_while_the_engine_runs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.set_shard_quota(
        1,
        ShardQuotaConfig {
            write_qps: 1,
            write_burst: 1,
            ..Default::default()
        },
    );
    let mut refused = 0;
    for index in 0..20 {
        if !write(&engine, &format!("a{index}")).ok {
            refused += 1;
        }
    }
    assert!(refused > 0, "the tight limit should have refused something");

    // Lift it. The bucket is rebuilt, so the new limit applies from now rather than inheriting a
    // debt run up under the old one.
    engine.set_shard_quota(1, ShardQuotaConfig::default());
    for index in 0..200 {
        assert!(
            write(&engine, &format!("b{index}")).ok,
            "a lifted limit still refused a write"
        );
    }
    assert_eq!(engine.shard_quota(1), Some(ShardQuotaConfig::default()));
}

/// A replicated apply is never refused, however tight the limit is.
///
/// This is the property the whole thing has to preserve. A follower that rejects an entry its
/// leader committed has not shed load -- it has diverged, and the divergence shows up later as a
/// shard that disagrees with its peers about what it contains.
#[test]
fn a_replicated_apply_is_never_refused_by_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.set_shard_quota(
        1,
        ShardQuotaConfig {
            write_qps: 1,
            write_burst: 1,
            read_qps: 1,
            read_burst: 1,
        },
    );
    // Exhaust both directions through the caller-driven path.
    for index in 0..10 {
        write(&engine, &format!("spend{index}"));
        read(&engine, &format!("spend{index}"));
    }
    assert!(
        !write(&engine, "over").ok,
        "the limit should be exhausted for this test to mean anything"
    );

    // Every one of these must land regardless.
    for index in 0..200 {
        let response = engine.execute_raft_apply(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("applied{index}"),
                value: b"v".to_vec(),
            },
        });
        assert!(
            response.status.ok,
            "a committed entry was refused by the rate limit: {}",
            response.status.message
        );
    }
    // And they really are in the shard, not merely acked.
    let found = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "applied199".to_string(),
        },
    });
    assert!(found.status.ok || found.status.code == "quota_exhausted");
}

/// A limited shard reports what it allowed and refused; an unlimited one reports nothing.
///
/// The absence matters as much as the numbers: zeros for an unlimited shard would read as "this
/// limit refused nothing", which is a different statement from "this shard has no limit".
#[test]
fn the_rate_limit_is_visible_in_the_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());

    // With no limit, the shard reports no counters at all.
    for index in 0..20 {
        write(&engine, &format!("free{index}"));
    }
    assert!(engine.shard_quota_counters(1).is_none());
    assert!(!engine
        .prometheus_metrics()
        .contains("temporalstore_shard_rate_limit_total{shard_id=\"1\""));

    engine.set_shard_quota(
        1,
        ShardQuotaConfig {
            write_qps: 5,
            write_burst: 2,
            ..Default::default()
        },
    );
    for index in 0..40 {
        write(&engine, &format!("k{index}"));
    }

    let counters = engine.shard_quota_counters(1).expect("the shard is limited");
    assert!(counters.write_refused > 0, "the limit should have refused some");
    assert!(counters.write_allowed > 0, "and allowed some");
    assert_eq!(
        counters.write_allowed + counters.write_refused,
        40,
        "every command should be counted on one side or the other"
    );

    let rendered = engine.prometheus_metrics();
    assert!(rendered.contains("# TYPE temporalstore_shard_rate_limit_total counter"));
    assert!(rendered.contains("kind=\"write_refused\""), "{rendered}");
    assert!(rendered.contains("kind=\"write_allowed\""));
    assert_eq!(engine.rate_limited_shards(), vec![1]);
}

/// How far the durable index trails the log is visible, and is zero when it has caught up.
#[test]
fn how_far_the_index_trails_the_log_is_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..25 {
        write(&engine, &format!("k{index}"));
    }
    let lags = engine.shard_index_lags();
    let (shard_id, lag) = lags
        .iter()
        .find(|(shard_id, _)| *shard_id == 1)
        .copied()
        .expect("the loaded shard should report a distance");
    assert_eq!(shard_id, 1);
    assert_eq!(lag, 0, "a shard with nothing outstanding should report zero");
    let rendered = engine.prometheus_metrics();
    assert!(rendered.contains("# TYPE temporalstore_shard_index_lag_records gauge"));
    assert!(
        rendered.contains("temporalstore_shard_index_lag_records{shard_id=\"1\"}"),
        "a loaded shard should report it: {rendered}"
    );
}

/// Reporting it does not deadlock against the lock the metrics loop already holds.
#[test]
fn reporting_the_distance_does_not_deadlock_the_metrics_loop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..10 {
        write(&engine, &format!("k{index}"));
    }
    let writer = {
        let engine = engine.clone();
        std::thread::spawn(move || {
            for index in 0..200 {
                write(&engine, &format!("w{index}"));
            }
        })
    };
    for _ in 0..50 {
        let _ = engine.prometheus_metrics();
    }
    writer.join().unwrap();
}

/// How many keys are waiting to expire is visible, and tracks what was actually set.
#[test]
fn how_many_keys_are_waiting_to_expire_is_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    let backlog_of = |engine: &TemporalEngine| -> u64 {
        engine
            .shard_expiry_backlogs()
            .into_iter()
            .find(|(shard_id, _)| *shard_id == 1)
            .map(|(_, waiting)| waiting)
            .expect("a loaded shard should report a backlog")
    };
    for index in 0..10 {
        write(&engine, &format!("plain{index}"));
    }
    assert_eq!(backlog_of(&engine), 0, "keys without a deadline are not waiting");
    for index in 0..7 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: format!("ttl{index}"),
                value: b"v".to_vec(),
                ttl_ms: 600_000,
            },
        });
        assert!(response.status.ok, "{}", response.status.message);
    }
    assert_eq!(backlog_of(&engine), 7, "every key given a deadline should be counted");
    let rendered = engine.prometheus_metrics();
    assert!(rendered.contains("# TYPE temporalstore_shard_expiring_keys gauge"));
    assert!(
        rendered.contains("temporalstore_shard_expiring_keys{shard_id=\"1\"} 7"),
        "the gauge should carry the count: {rendered}"
    );
}

// ---------------------------------------------------------------------------------------------
// What a limit may refuse, and what it may never refuse.
//
// `execute_with_storage_override` already draws this line for the token-bucket limit: a command
// arriving under `raft_applying()` or `replaying_wal()` is NOT charged, because refusing it is not
// shedding load. A follower that rejects what its leader committed diverges from the leader, and a
// replay that rejects a record already in the log cannot rebuild the shard -- the replay loop turns
// any failed response into `wal_replay_failed` and aborts the whole load.
//
// The config-driven limits (`write_qps` / `read_qps` and the table and tenant scopes) and the
// `maxmemory_bytes` storage ceiling sit in the same function and did not draw it.

/// A shard whose config carries a write limit must still apply every committed entry.
#[test]
fn a_committed_entry_is_not_refused_by_the_configured_write_limit() {
    const ENTRIES: usize = 40;
    const LIMIT: u64 = 3;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            write_qps: Some(LIMIT),
            ..Config::default()
        },
    });
    // The DENOMINATOR: the limit is real on the client path, so "nothing was refused below" is
    // not the vacuous answer of a limit that was never in force.
    wait_for_fresh_admission_second();
    let mut client_refused = 0;
    for index in 0..ENTRIES {
        let status = engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("client{index}"),
                    value: b"v".to_vec(),
                },
            })
            .status;
        if status.code == "admission_rejected" {
            client_refused += 1;
        }
    }
    assert!(
        client_refused > 0,
        "the client path refused none of {ENTRIES} writes, so this test would prove nothing"
    );

    let mut applied = 0;
    let mut refused = Vec::new();
    for index in 0..ENTRIES {
        let response = engine.execute_raft_apply(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("committed{index}"),
                value: b"v".to_vec(),
            },
        });
        if response.status.ok {
            applied += 1;
        } else {
            refused.push(response.status.code.clone());
        }
    }
    assert_eq!(
        applied, ENTRIES,
        "only {applied} of {ENTRIES} committed entries applied; refused as {refused:?} -- a \
         follower that refuses a committed entry diverges from its leader"
    );
}


/// The same shard must still rebuild itself from its own log.
///
/// `replay_wal_into_shard` is the production rebuild: `load_shard_with` calls it, and a single
/// failed response inside it becomes `wal_replay_failed`, which unwinds the shard and REFUSES the
/// load. A shard that cannot be loaded is not degraded, it is gone.
///
/// The records here carry a COMMAND and no recorded outcomes, which is the shape replay
/// re-executes rather than installs. Records written through the engine carry outcomes under the
/// default and are installed without executing anything, so the re-execute fallback -- the one
/// that exists so "a kind that records nothing recovers correctly instead of silently recovering
/// as nothing" -- has to be written into the log directly to be exercised at all.
#[test]
fn wal_replay_is_not_refused_by_the_configured_write_limit() {
    const RECORDS: usize = 40;
    const LIMIT: u64 = 3;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..RECORDS {
        engine
            .wal_store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: format!("replayed{index}"),
                    value: b"v".to_vec(),
                },
                true,
            )
            .expect("the log should accept a record");
    }
    // Two DENOMINATORS. The log really holds the records...
    let last_sequence = engine.wal_store.stats(1).last_sequence;
    assert!(
        last_sequence >= RECORDS as u64,
        "the log holds only {last_sequence} records, so replaying it would prove nothing"
    );
    // ...and the shard does NOT, so anything readable afterwards came from the replay.
    let readable = |engine: &TemporalEngine| {
        (0..RECORDS)
            .filter(|index| {
                matches!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet {
                                key: format!("replayed{index}"),
                            },
                        })
                        .response,
                    CommandResponse::Bytes { value: Some(_) }
                )
            })
            .count()
    };
    assert_eq!(
        readable(&engine),
        0,
        "the records were appended to the log only; the shard should hold none of them yet"
    );

    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            write_qps: Some(LIMIT),
            ..Config::default()
        },
    });
    wait_for_fresh_admission_second();

    // From sequence zero: re-drive every record the log holds, exactly as a load with no usable
    // checkpoint does.
    let replayed = engine.replay_wal_into_shard(1, 0);
    assert!(
        replayed.is_ok(),
        "replay was refused: {:?} -- a replay that rejects a record already in the log cannot \
         rebuild the shard, and the load that called it refuses the shard outright",
        replayed.err()
    );
    assert_eq!(
        readable(&engine),
        RECORDS,
        "the replay was reported as successful but did not bring every record back"
    );
}

// ---------------------------------------------------------------------------------------------
// The storage ceiling is the other gate in this function that refuses a write, and what it reads
// is a shared number: the known physical bytes of the whole store, not of this shard. Once it is
// over it stays over until something reclaims -- and a shard that cannot replay cannot be loaded,
// so it never reaches the maintenance that would bring it back under.

/// A shard over its `maxmemory_bytes` must still apply every committed entry.
#[test]
fn a_committed_entry_is_not_refused_by_the_storage_ceiling() {
    const ENTRIES: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    // Something has to be stored before the ceiling has anything to be over.
    for index in 0..10 {
        assert!(write(&engine, &format!("seed{index}")).ok);
    }
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            maxmemory_bytes: Some(1),
            ..Config::default()
        },
    });
    // The DENOMINATOR: the ceiling is genuinely over on the client path.
    let client = write(&engine, "client");
    assert_eq!(
        client.code, "storage_quota_exceeded",
        "the ceiling did not refuse a client write, so this test would prove nothing"
    );

    let mut applied = 0;
    let mut refused = Vec::new();
    for index in 0..ENTRIES {
        let response = engine.execute_raft_apply(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("committed{index}"),
                value: b"v".to_vec(),
            },
        });
        if response.status.ok {
            applied += 1;
        } else {
            refused.push(response.status.code.clone());
        }
    }
    assert_eq!(
        applied, ENTRIES,
        "only {applied} of {ENTRIES} committed entries applied; refused as {refused:?} -- a \
         follower that refuses a committed entry diverges from its leader, and the ceiling it \
         refused on is not something the follower can act on"
    );
}

/// And it must still rebuild itself from its own log.
#[test]
fn wal_replay_is_not_refused_by_the_storage_ceiling() {
    const RECORDS: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    for index in 0..10 {
        assert!(write(&engine, &format!("seed{index}")).ok);
    }
    for index in 0..RECORDS {
        engine
            .wal_store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: format!("replayed{index}"),
                    value: b"v".to_vec(),
                },
                true,
            )
            .expect("the log should accept a record");
    }
    let readable = |engine: &TemporalEngine| {
        (0..RECORDS)
            .filter(|index| {
                matches!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet {
                                key: format!("replayed{index}"),
                            },
                        })
                        .response,
                    CommandResponse::Bytes { value: Some(_) }
                )
            })
            .count()
    };
    // The DENOMINATOR: the shard holds none of these yet, so anything readable after the replay
    // came from the replay.
    assert_eq!(
        readable(&engine),
        0,
        "the records were appended to the log only; the shard should hold none of them yet"
    );

    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            maxmemory_bytes: Some(1),
            ..Config::default()
        },
    });
    // ...and the ceiling really is over.
    assert_eq!(
        write(&engine, "client").code,
        "storage_quota_exceeded",
        "the ceiling did not refuse a client write, so this test would prove nothing"
    );

    let replayed = engine.replay_wal_into_shard(1, 0);
    assert!(
        replayed.is_ok(),
        "replay was refused: {:?} -- a shard that cannot replay cannot be loaded, and cannot \
         reach the maintenance that would bring it back under the ceiling",
        replayed.err()
    );
    assert_eq!(
        readable(&engine),
        RECORDS,
        "the replay was reported as successful but did not bring every record back"
    );
}

// ---------------------------------------------------------------------------------------------
// Where the limit has to be charged.
//
// The single-command path charges before anything else, including its own read-only fast path,
// and the comment there gives the rule: a read served without taking the shard lock still costs
// the shard, and a limit the cheapest reads slip past is not a limit. The batch path is not a
// cheap read -- it is the path that carries most of the traffic in this engine -- and it charged
// nothing.

/// A batch of writes is charged against the same limit a sequence of writes is.
#[test]
fn a_batch_is_charged_against_the_shard_write_limit() {
    const COMMANDS: usize = 40;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.set_shard_quota(
        1,
        ShardQuotaConfig {
            write_qps: 10,
            write_burst: 5,
            ..Default::default()
        },
    );

    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: (0..COMMANDS)
            .map(|index| Command::StringSet {
                key: format!("k{index}"),
                value: b"v".to_vec(),
            })
            .collect(),
    });
    // The DENOMINATOR: every command was actually offered to the batch path.
    assert!(batch.status.ok, "{}", batch.status.message);
    assert_eq!(
        batch.responses.len(),
        COMMANDS,
        "the batch did not carry every command, so counting refusals below means nothing"
    );

    let counters = engine
        .shard_quota_counters(1)
        .expect("the shard carries a limit");
    // Both halves, separately. A batch that charged nothing shows zero on BOTH, and a combined
    // total would hide which of the two was wrong.
    assert_eq!(
        counters.write_allowed + counters.write_refused,
        COMMANDS as u64,
        "the limit saw {} of {COMMANDS} batched writes (allowed {}, refused {})",
        counters.write_allowed + counters.write_refused,
        counters.write_allowed,
        counters.write_refused
    );
    assert!(
        counters.write_allowed > 0,
        "the burst should have let some through"
    );
    assert!(
        counters.write_refused > 0,
        "10 per second with a burst of 5 should have refused most of {COMMANDS} at once"
    );

    let refused = batch
        .responses
        .iter()
        .filter(|response| response.status.code == "quota_exhausted")
        .count() as u64;
    assert_eq!(
        refused, counters.write_refused,
        "what the limit counted as refused and what the batch reported as refused disagree"
    );
}

/// A batch of reads is charged too, and against the read limit rather than the write one.
#[test]
fn a_batch_of_reads_is_charged_against_the_read_limit() {
    const COMMANDS: usize = 40;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    assert!(write(&engine, "k").ok);
    engine.set_shard_quota(
        1,
        ShardQuotaConfig {
            read_qps: 10,
            read_burst: 5,
            ..Default::default()
        },
    );

    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: (0..COMMANDS)
            .map(|_| Command::StringGet {
                key: "k".to_string(),
            })
            .collect(),
    });
    assert_eq!(batch.responses.len(), COMMANDS);

    let counters = engine
        .shard_quota_counters(1)
        .expect("the shard carries a limit");
    assert_eq!(
        counters.read_allowed + counters.read_refused,
        COMMANDS as u64,
        "the limit saw {} of {COMMANDS} batched reads",
        counters.read_allowed + counters.read_refused
    );
    assert!(counters.read_refused > 0, "the read limit refused none");
    // The other direction is untouched: a batch of reads must not spend write credit.
    assert_eq!(
        counters.write_allowed, 0,
        "reads were charged against the write side"
    );
    assert_eq!(counters.write_refused, 0);
}

/// THE STORAGE CEILING WALKS EVERY SLAB DESCRIPTOR, ON EVERY WRITE COMMAND.
///
/// `execute` gates a write on `block_store.slab_summary().total_known_physical_bytes >= limit`.
/// `slab_summary` takes the block-store mutex and summarises the whole descriptor map to produce
/// twenty fields, of which the gate reads one. So the comparison of a single total against a
/// single limit is charged the length of the manifest, under the lock, per command -- and the
/// manifest is the thing that grows for the life of the shard.
///
/// COUNTED, NOT ARGUED, AND WITH A CONTROL. Reading the call site says the walk is there; it does
/// not say the gate is reached, that the summary is not memoised somewhere between, or that the
/// count scales with the slab count rather than with something else. The counter sits on the walk
/// itself, so it reports what was actually walked whoever asked for it.
///
/// THE CONTROL IS THE SAME WRITES WITH `maxmemory_bytes` UNSET. `Option::map` does not call its
/// closure on `None`, so an unconfigured shard must walk NOTHING -- and if the control also walked
/// the descriptors, the walk would be coming from somewhere else in the write path and the fix
/// would be aimed at the wrong function.
#[test]
fn the_storage_ceiling_walks_every_slab_descriptor_on_every_write() {
    const WRITES: usize = 10;
    const SLABS: usize = 40;

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    assert!(write(&engine, "seed").ok);

    // Give the store a manifest worth walking. Rolling seals the active slab and mints a fresh
    // one each time, which is the state a long-running shard reaches by accumulating -- without
    // writing the bytes it would take to get there.  is the wrong
    // lever for this: it no-ops while the active slab is under target, and a slab that was just
    // rolled to is empty, so it rolls exactly once however many times it is called.
    let block_store = engine.block_store();
    let mut rolled = 0;
    while rolled < SLABS {
        block_store.roll_slab().unwrap();
        rolled += 1;
    }

    // HOW MANY DESCRIPTORS ONE SUMMARY WALKS, measured rather than assumed: the counter's own
    // delta across a single `slab_summary` call IS the width of the walk.
    let before = crate::block_store::slab_descriptors_summarised();
    let summary = block_store.slab_summary();
    let descriptors = crate::block_store::slab_descriptors_summarised() - before;

    // DENOMINATOR: the store really did accumulate a manifest, so a per-write walk of it is a
    // cost worth naming. Against a one-slab store every count below is 1 and the test is vacuous.
    assert!(
        descriptors >= SLABS as u64,
        "the store must hold at least {SLABS} descriptors before the walk is charged for them, \
         not {descriptors}"
    );
    assert!(
        summary.active_slabs + summary.sealed_slabs >= SLABS as u64,
        "and they must be live slabs, not purged ones"
    );

    // CONTROL FIRST, so a mutant that kills the treatment assertion cannot stop it running:
    // `maxmemory_bytes` unset, the same commands, on the same store.
    let before = crate::block_store::slab_descriptors_summarised();
    for index in 0..WRITES {
        assert!(
            write(&engine, &format!("unconfigured{index}")).ok,
            "the control writes must succeed, or they are not the same path"
        );
    }
    let control_walked = crate::block_store::slab_descriptors_summarised() - before;

    // TREATMENT: a ceiling far above anything this store holds, so the writes still SUCCEED and
    // the only difference between the halves is that the gate's closure runs.
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            maxmemory_bytes: Some(u64::MAX),
            ..Config::default()
        },
    });
    let before = crate::block_store::slab_descriptors_summarised();
    for index in 0..WRITES {
        assert!(
            write(&engine, &format!("configured{index}")).ok,
            "a ceiling of u64::MAX must not refuse a write, or the halves differ by more than \
             the walk"
        );
    }
    let treatment_walked = crate::block_store::slab_descriptors_summarised() - before;

    println!(
        "  storage ceiling: {descriptors} descriptors, {WRITES} writes -- unset walked \
         {control_walked}, set walked {treatment_walked}"
    );

    assert_eq!(
        control_walked, 0,
        "an unconfigured shard must not summarise the slab manifest on the write path at all; \
         {control_walked} descriptors walked over {WRITES} writes means the walk is not the \
          ceiling's"
    );
    assert!(
        treatment_walked >= WRITES as u64 * descriptors,
        "a configured shard walks the whole manifest per write: expected at least {WRITES} x \
         {descriptors}, got {treatment_walked}"
    );
}
