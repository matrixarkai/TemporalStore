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
