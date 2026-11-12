// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::path::Path;
use std::process::Command as Process;

const CHILD: &str =
    "engine::stream_batch_methods::batch_wal_barrier_tests::batch_wal_barrier_child";
const ROUNDS: usize = 16;

fn invoke(root: &Path, phase: &str, anchor: bool, legacy: bool) -> std::process::Output {
    Process::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD, "--nocapture", "--test-threads=1"])
        .env("TS_TEST_BATCH_BARRIER_PHASE", phase)
        .env("TS_TEST_BATCH_BARRIER_ROOT", root)
        .env(
            "TS_TEST_BATCH_BARRIER_ANCHOR",
            if anchor { "1" } else { "0" },
        )
        .env("TS_WAL_LEGACY_RECOVERY", if legacy { "1" } else { "0" })
        .output()
        .unwrap()
}

fn score(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn check_recovery(root: &Path, anchor: bool, legacy: bool) {
    let output = invoke(root, "write", anchor, legacy);
    assert_eq!(
        output.status.code(),
        Some(86),
        "writer failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !legacy {
        let indexlogs = root.join("indexes/indexlogs");
        assert!(
            indexlogs.exists(),
            "the test must discard real index-log state"
        );
        std::fs::remove_dir_all(indexlogs).unwrap();
        if anchor {
            let lengths: Vec<(String, u64)> =
                serde_json::from_slice(&std::fs::read(root.join("durable-lengths.json")).unwrap())
                    .unwrap();
            assert!(!lengths.is_empty());
            let mut truncated = 0;
            for entry in std::fs::read_dir(root.join("blocks")).unwrap() {
                let entry = entry.unwrap();
                if entry.path().extension().is_some_and(|value| value == "seg") {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    let keep = lengths
                        .iter()
                        .find(|row| row.0 == name)
                        .map_or(0, |row| row.1);
                    if entry.metadata().unwrap().len() > keep {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(entry.path())
                            .unwrap()
                            .set_len(keep)
                            .unwrap();
                        truncated += 1;
                    }
                }
            }
            assert!(
                truncated > 0,
                "the test must discard a real post-anchor block tail"
            );
        } else {
            assert!(root.join("blocks").exists());
            std::fs::remove_dir_all(root.join("blocks")).unwrap();
            for entry in std::fs::read_dir(root.join("indexes")).unwrap() {
                let entry = entry.unwrap();
                if entry.file_name().to_string_lossy().ends_with(".index.json") {
                    std::fs::remove_file(entry.path()).unwrap();
                }
            }
        }
        assert!(root.join("indexes/wals").exists());
    }
    if root.join("cache").exists() {
        std::fs::remove_dir_all(root.join("cache")).unwrap();
    }
    let output = invoke(root, "read", anchor, legacy);
    assert!(
        output.status.success(),
        "recovery failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("BATCH_RECOVERY_OK"));
}

#[test]
fn batch_wal_barrier_recovers_without_any_non_wal_state() {
    let root = tempfile::tempdir().unwrap();
    check_recovery(root.path(), false, false);
}

#[test]
fn batch_wal_barrier_recovers_lost_index_and_post_anchor_blocks() {
    let root = tempfile::tempdir().unwrap();
    check_recovery(root.path(), true, false);
}

#[test]
fn batch_wal_barrier_preserves_legacy_durability() {
    let root = tempfile::tempdir().unwrap();
    check_recovery(root.path(), false, true);
}

#[test]
fn batch_wal_barrier_child() {
    let Ok(phase) = std::env::var("TS_TEST_BATCH_BARRIER_PHASE") else {
        return;
    };
    let root = std::path::PathBuf::from(std::env::var_os("TS_TEST_BATCH_BARRIER_ROOT").unwrap());
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    if phase == "write" {
        for round in 0..ROUNDS {
            let mut commands = vec![
                Command::HashSet {
                    key: "catalog".into(),
                    field: round.to_string(),
                    value: vec![round as u8; 64],
                },
                Command::HashIncrBy {
                    key: "counter".into(),
                    field: "total".into(),
                    increment: 1,
                },
                Command::SetAdd {
                    key: "order".into(),
                    member: round.to_string().into_bytes(),
                },
            ];
            if round > 0 {
                commands.extend([
                    Command::HashDelete {
                        key: "catalog".into(),
                        field: (round - 1).to_string(),
                    },
                    Command::SetRemove {
                        key: "order".into(),
                        member: (round - 1).to_string().into_bytes(),
                    },
                ]);
            }
            let before = crate::durability_metrics::snapshot();
            for commands in [
                commands,
                vec![Command::HashMultiSet {
                    key: "packed".into(),
                    entries: (0..16)
                        .map(|field| (format!("{round}-{field}"), vec![round as u8, field as u8]))
                        .collect(),
                }],
            ] {
                let count = commands.len();
                let result = engine.batch_execute(BatchExecuteRequest {
                    shard_id: 1,
                    commands,
                });
                assert!(result.status.ok, "{:?}", result.status);
                assert_eq!(result.responses.len(), count);
                assert!(result.responses.iter().all(|row| row.status.ok));
            }
            let after = crate::durability_metrics::snapshot();
            let index_barriers = after.get("engine_index_log_append").copied().unwrap_or(0)
                - before.get("engine_index_log_append").copied().unwrap_or(0);
            if super::super::wal_legacy_recovery() {
                assert!(index_barriers >= 2);
            } else {
                assert_eq!(
                    index_barriers, 0,
                    "default batches must use the WAL barrier"
                );
            }
            let wal_barriers = |counts: &std::collections::BTreeMap<&'static str, u64>| {
                [
                    "engine_wal_append",
                    "engine_wal_group_commit",
                    "engine_wal_flush",
                ]
                .iter()
                .map(|site| counts.get(site).copied().unwrap_or(0))
                .sum::<u64>()
            };
            assert!(
                wal_barriers(&after) > wal_barriers(&before),
                "WAL durability must remain synchronous"
            );
            if round == 7 && std::env::var("TS_TEST_BATCH_BARRIER_ANCHOR").unwrap() == "1" {
                engine.materialize_synthetic_blocks(1);
                engine.flush_shard_index(1);
                let lengths: Vec<_> = std::fs::read_dir(root.join("blocks"))
                    .unwrap()
                    .map(Result::unwrap)
                    .filter(|entry| entry.path().extension().is_some_and(|value| value == "seg"))
                    .map(|entry| {
                        (
                            entry.file_name().to_string_lossy().into_owned(),
                            entry.metadata().unwrap().len(),
                        )
                    })
                    .collect();
                std::fs::write(
                    root.join("durable-lengths.json"),
                    serde_json::to_vec(&lengths).unwrap(),
                )
                .unwrap();
            }
        }
        engine.materialize_synthetic_blocks(1);
        // No Rust destructors or final engine flush before the parent discards deferred state.
        std::process::exit(86);
    }
    assert_eq!(phase, "read");
    if !super::super::wal_legacy_recovery() {
        assert!(engine.storage_recovery_report(1).wal_records > 0);
    }
    for round in 0..ROUNDS {
        assert_eq!(
            score(
                &engine,
                Command::HashGet {
                    key: "catalog".into(),
                    field: round.to_string()
                }
            ),
            CommandResponse::Bytes {
                value: (round == ROUNDS - 1).then(|| vec![round as u8; 64])
            }
        );
        for field in 0..16 {
            assert_eq!(
                score(
                    &engine,
                    Command::HashGet {
                        key: "packed".into(),
                        field: format!("{round}-{field}")
                    }
                ),
                CommandResponse::Bytes {
                    value: Some(vec![round as u8, field as u8])
                }
            );
        }
    }
    assert_eq!(
        score(
            &engine,
            Command::HashGet {
                key: "counter".into(),
                field: "total".into()
            }
        ),
        CommandResponse::Bytes {
            value: Some(ROUNDS.to_string().into_bytes())
        }
    );
    assert_eq!(
        score(
            &engine,
            Command::SetMembers {
                key: "order".into()
            }
        ),
        CommandResponse::Members {
            members: vec![(ROUNDS - 1).to_string().into_bytes()]
        }
    );
    println!("BATCH_RECOVERY_OK");
}
