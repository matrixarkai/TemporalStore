// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::engine::TemporalEngine;
use crate::types::{Command, CommandResponse, ExecuteRequest};

fn run(engine: &TemporalEngine, command: Command) -> crate::types::ExecuteResponse {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    })
}

fn members(response: crate::types::ExecuteResponse) -> Vec<Vec<u8>> {
    assert!(
        response.status.ok,
        "set command failed: {:?}",
        response.status
    );
    match response.response {
        CommandResponse::Members { members } => members,
        other => panic!("expected members, got {other:?}"),
    }
}

#[test]
fn set_scan_pages_in_member_order_without_repeating_the_cursor() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for member in [
        b"delta".as_slice(),
        b"alpha".as_slice(),
        b"charlie".as_slice(),
        b"bravo".as_slice(),
    ] {
        let response = run(
            &engine,
            Command::SetAdd {
                key: "catalog".to_string(),
                member: member.to_vec(),
            },
        );
        assert!(response.status.ok);
    }

    let first = members(run(
        &engine,
        Command::SetScan {
            key: "catalog".to_string(),
            after: Vec::new(),
            limit: 2,
        },
    ));
    assert_eq!(first, [b"alpha".to_vec(), b"bravo".to_vec()]);

    let second = members(run(
        &engine,
        Command::SetScan {
            key: "catalog".to_string(),
            after: first[1].clone(),
            limit: 2,
        },
    ));
    assert_eq!(second, [b"charlie".to_vec(), b"delta".to_vec()]);

    assert!(members(run(
        &engine,
        Command::SetScan {
            key: "catalog".to_string(),
            after: second[1].clone(),
            limit: 2,
        },
    ))
    .is_empty());
}

#[test]
fn set_scan_rejects_unbounded_pages() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for limit in [0, 10_001] {
        let response = run(
            &engine,
            Command::SetScan {
                key: "catalog".to_string(),
                after: Vec::new(),
                limit,
            },
        );
        assert!(!response.status.ok);
        assert_eq!(response.status.code, "invalid_argument");
    }
}
