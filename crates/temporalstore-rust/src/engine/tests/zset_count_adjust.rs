// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::engine::TemporalEngine;
use crate::types::{Command, CommandResponse, ExecuteRequest};

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "command failed: {:?}", response.status);
    response.response
}

fn adjust(engine: &TemporalEngine, increment: i64) -> i64 {
    match run(
        engine,
        Command::ZSetCountAdjust {
            key: "identities".to_string(),
            member: b"user-a".to_vec(),
            increment,
        },
    ) {
        CommandResponse::Integer { value } => value,
        other => panic!("expected integer, got {other:?}"),
    }
}

#[test]
fn zset_count_adjust_removes_zero_and_keeps_absent_decrements_at_zero() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    assert_eq!(adjust(&engine, -1), 0);
    assert_eq!(adjust(&engine, 1), 1);
    assert_eq!(adjust(&engine, 1), 2);
    assert_eq!(adjust(&engine, -1), 1);
    assert_eq!(adjust(&engine, -1), 0);
    assert_eq!(adjust(&engine, -1), 0);

    assert_eq!(
        run(
            &engine,
            Command::ZSetCard {
                key: "identities".to_string(),
            },
        ),
        CommandResponse::Integer { value: 0 }
    );
}

#[test]
fn zset_scan_pages_in_member_order_without_sorting_by_count() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (member, increment) in [(b"user-b".to_vec(), 9), (b"user-a".to_vec(), 2)] {
        run(
            &engine,
            Command::ZSetCountAdjust {
                key: "identities".to_string(),
                member,
                increment,
            },
        );
    }
    let first = run(
        &engine,
        Command::ZSetScan {
            key: "identities".to_string(),
            after: Vec::new(),
            limit: 1,
        },
    );
    assert_eq!(
        first,
        CommandResponse::Members {
            members: vec![b"user-a".to_vec(), b"2".to_vec()]
        }
    );
    let second = run(
        &engine,
        Command::ZSetScan {
            key: "identities".to_string(),
            after: b"user-a".to_vec(),
            limit: 1,
        },
    );
    assert_eq!(
        second,
        CommandResponse::Members {
            members: vec![b"user-b".to_vec(), b"9".to_vec()]
        }
    );
}

#[test]
fn zset_scan_rejects_unbounded_pages() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for limit in [0, 10_001] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetScan {
                key: "identities".to_string(),
                after: Vec::new(),
                limit,
            },
        });
        assert!(!response.status.ok);
        assert_eq!(response.status.code, "invalid_argument");
    }
}
