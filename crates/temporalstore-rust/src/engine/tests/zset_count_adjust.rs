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

#[test]
fn zset_count_adjust_mixed_batch_logs_only_changed_blocks() {
    use crate::types::BatchExecuteRequest;

    for existing in [32, 2_048] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        run(
            &engine,
            Command::HashMultiSet {
                key: "catalog".into(),
                entries: (0..existing)
                    .map(|i| (format!("old-{i}"), vec![7; 64]))
                    .collect(),
            },
        );
        let mut commands = vec![Command::HashMultiSet {
            key: "catalog".into(),
            entries: (0..8).map(|i| (format!("new-{i}"), vec![8; 64])).collect(),
        }];
        commands.extend((0..8).map(|i| Command::SetAdd {
            key: "order".into(),
            member: format!("new-{i}").into_bytes(),
        }));
        commands.push(Command::ZSetCountAdjust {
            key: "identities".into(),
            member: b"user-a".to_vec(),
            increment: 8,
        });
        let before = engine.index_log_store.stats_without_rescan(1).bytes_written;
        let response = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "{:?}", response.status);
        assert!(response.responses.iter().all(|row| row.status.ok));
        let records = engine.index_log_store.read_delta_records(1, 0).unwrap();
        let last = records
            .last()
            .expect("mixed write must emit a durable delta");
        let bytes = engine.index_log_store.stats_without_rescan(1).bytes_written - before;
        println!(
            "existing={existing} delta_items={} delta_bytes={bytes} upsert={}",
            last.items.len(),
            last.upsert
        );
        assert!(
            last.upsert,
            "count adjustment downgraded the entire mixed batch"
        );
        assert_eq!(
            last.items.len(),
            17,
            "8 hash fields, 8 set members and one count"
        );
        assert!(
            last.key_states.iter().any(|state| {
                state["key"] == "identities"
                    && state["zset_elements"]
                        .as_array()
                        .is_some_and(|rows| rows.len() == 1)
            }),
            "count score and address must be carried for delta recovery"
        );
        assert!(
            bytes < 16_384,
            "mixed delta restated existing catalog rows: {bytes} bytes"
        );
    }
}

#[test]
fn zset_count_adjust_recovers_mixed_updates_and_zero_removals_without_unload() {
    use crate::types::BatchExecuteRequest;

    for batch in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let open = || {
            TemporalEngine::with_local_dirs(
                16 * 1024 * 1024,
                dir.path().join("cache"),
                dir.path().join("blocks"),
                dir.path().join("indexes"),
            )
        };
        {
            let engine = open();
            engine.load_shard(1);
            let adjust_member = |member: &[u8], increment| {
                let command = Command::ZSetCountAdjust {
                    key: "identities".into(),
                    member: member.to_vec(),
                    increment,
                };
                if batch {
                    let out = engine.batch_execute(BatchExecuteRequest {
                        shard_id: 1,
                        commands: vec![
                            Command::HashSet {
                                key: "catalog".into(),
                                field: "updated".into(),
                                value: vec![9],
                            },
                            command,
                        ],
                    });
                    assert!(out.status.ok);
                    assert!(out.responses.iter().all(|row| row.status.ok));
                } else {
                    run(&engine, command);
                }
            };
            adjust_member(b"unrelated", 11);
            adjust_member(b"user-a", 8);
            adjust_member(b"user-a", 2);
            adjust_member(b"user-b", 4);
            adjust_member(b"user-b", -4);
            let records = engine.index_log_store.read_delta_records(1, 0).unwrap();
            let removal = records.last().expect("zero removal must emit a delta");
            assert!(
                !removal.upsert,
                "zero removal must retain snapshot semantics"
            );
            assert!(
                removal
                    .items
                    .iter()
                    .any(|item| item.object_key.as_ref() == "identities"),
                "removal snapshot must cover the identity object"
            );
            adjust_member(b"user-a", -3);
            adjust_member(b"absent", -1);
        }
        // Do not unload: recovery must fold the durable deltas, not a final base snapshot.
        let recovered = open();
        recovered.load_shard(1);
        assert_eq!(
            run(
                &recovered,
                Command::ZSetScan {
                    key: "identities".into(),
                    after: Vec::new(),
                    limit: 10,
                }
            ),
            CommandResponse::Members {
                members: vec![
                    b"unrelated".to_vec(),
                    b"11".to_vec(),
                    b"user-a".to_vec(),
                    b"7".to_vec()
                ],
            }
        );
        if batch {
            assert_eq!(
                run(
                    &recovered,
                    Command::HashGet {
                        key: "catalog".into(),
                        field: "updated".into(),
                    }
                ),
                CommandResponse::Bytes {
                    value: Some(vec![9])
                }
            );
        }
    }
}
