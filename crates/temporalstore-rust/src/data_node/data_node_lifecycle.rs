// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs;
use std::path::PathBuf;

use super::{
    is_write_command, now_ms, Command, DataNodeLifecyclePersistenceReport,
    DataNodeLifecycleSnapshot, DataNodeRuntimeInner, DataNodeShardLifecycleState, ShardId, Status,
};

pub(super) fn lifecycle_snapshot_path_from_env() -> Option<PathBuf> {
    std::env::var_os("TS_DATA_NODE_LIFECYCLE_SNAPSHOT").map(PathBuf::from)
}

pub(super) fn lifecycle_persistence_report_for_path(
    path: Option<&PathBuf>,
) -> DataNodeLifecyclePersistenceReport {
    DataNodeLifecyclePersistenceReport {
        enabled: path.is_some(),
        path: path.map(|path| path.display().to_string()),
        ..DataNodeLifecyclePersistenceReport::default()
    }
}

/// First byte of a binary lifecycle snapshot. JSON starts with `{` or whitespace, and 0x01 is neither
/// and not valid JSON in any position, so the first byte settles the shape with no chance of reading
/// either as the other. Decided per file and never from a setting.
pub(crate) const LIFECYCLE_SNAPSHOT_CONTAINER_PROTOBUF: u8 = 0x01;

/// A lifecycle snapshot file in EITHER shape. THE ONLY READER of one.
///
/// The JSON arm is PERMANENT: every snapshot a data node has written so far is JSON, and a node
/// that came back unable to read its own snapshot would restart with no record of which shard
/// operations were in flight and no scheduler tokens to fence them. The hold is on what is on disk,
/// not on which writer runs today.
///
/// Errors are strings because both callers turn them straight into a `Status` message; a binary
/// snapshot whose body does not decode, or that carries no recognisable list shape, is refused by
/// name and never defaulted to an empty snapshot (which would restore as "nothing in flight").
pub fn decode_lifecycle_snapshot_file(bytes: &[u8]) -> Result<DataNodeLifecycleSnapshot, String> {
    use prost::Message as _;
    let Some((&LIFECYCLE_SNAPSHOT_CONTAINER_PROTOBUF, body)) = bytes.split_first() else {
        return serde_json::from_slice::<DataNodeLifecycleSnapshot>(bytes)
            .map_err(|err| err.to_string());
    };
    let proto = crate::sdk::v1::DurableDataNodeLifecycleSnapshot::decode(body)
        .map_err(|err| format!("binary lifecycle snapshot did not decode: {err}"))?;
    Ok(DataNodeLifecycleSnapshot {
        format_version: proto.format_version,
        transitions: proto
            .transitions
            .into_iter()
            .map(|state| DataNodeShardLifecycleState {
                shard_id: state.shard_id,
                state: state.state,
                operation: state.operation,
                load_version: state.load_version,
                updated_at_ms: state.updated_at_ms,
                last_status: state.last_status.map(|status| Status {
                    ok: status.ok,
                    code: status.code,
                    message: status.message,
                }),
                scheduler_task_id: state.scheduler_task_id,
                scheduler_generation: state.scheduler_generation,
            })
            .collect(),
        tokens: proto
            .tokens
            .into_iter()
            .map(|token| crate::rebalance::SchedulerLifecycleToken {
                task_id: token.task_id,
                shard_id: token.shard_id,
                operation: token.operation,
                load_version: token.load_version,
                generation: token.generation,
            })
            .collect(),
    })
}

/// A lifecycle snapshot as the binary file. THE ONLY ENCODER: the runtime's persist and the data
/// server's save-to-path both call it, and nothing writes the JSON snapshot any more. An encoder
/// nobody calls is how a retired format comes back; `decode_lifecycle_snapshot_file` keeps its JSON
/// arm because the snapshots already on disk are JSON. Opposite reasons, so one is deleted and the
/// other held.
pub fn encode_lifecycle_snapshot_file(snapshot: &DataNodeLifecycleSnapshot) -> Vec<u8> {
    use prost::Message as _;
    let proto = crate::sdk::v1::DurableDataNodeLifecycleSnapshot {
        format_version: snapshot.format_version,
        transitions: snapshot
            .transitions
            .iter()
            .map(|state| crate::sdk::v1::DurableShardLifecycleState {
                shard_id: state.shard_id,
                state: state.state.clone(),
                operation: state.operation.clone(),
                load_version: state.load_version,
                updated_at_ms: state.updated_at_ms,
                last_status: state.last_status.as_ref().map(|status| {
                    crate::sdk::v1::DurableLifecycleStatus {
                        ok: status.ok,
                        code: status.code.clone(),
                        message: status.message.clone(),
                    }
                }),
                scheduler_task_id: state.scheduler_task_id,
                scheduler_generation: state.scheduler_generation,
            })
            .collect(),
        tokens: snapshot
            .tokens
            .iter()
            .map(|token| crate::sdk::v1::DurableSchedulerLifecycleToken {
                task_id: token.task_id,
                shard_id: token.shard_id,
                operation: token.operation.clone(),
                load_version: token.load_version,
                generation: token.generation,
            })
            .collect(),
    };
    let mut out = vec![LIFECYCLE_SNAPSHOT_CONTAINER_PROTOBUF];
    out.extend(proto.encode_to_vec());
    out
}

pub(super) fn lifecycle_snapshot_inner(inner: &DataNodeRuntimeInner) -> DataNodeLifecycleSnapshot {
    let mut transitions = inner
        .lifecycle
        .lock()
        .expect("runtime lifecycle lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    transitions.sort_by_key(|state| (state.shard_id, state.operation.clone()));
    let mut tokens = inner
        .lifecycle_tokens
        .lock()
        .expect("runtime lifecycle token lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    tokens.sort_by_key(|token| (token.shard_id, token.operation.clone()));
    DataNodeLifecycleSnapshot {
        format_version: 1,
        transitions,
        tokens,
    }
}

pub(super) fn restore_lifecycle_snapshot_inner(
    inner: &DataNodeRuntimeInner,
    snapshot: DataNodeLifecycleSnapshot,
) -> Status {
    if snapshot.format_version != 1 {
        return Status::error(
            "bad_lifecycle_snapshot",
            "unsupported data node lifecycle snapshot version",
        );
    }
    {
        let mut lifecycle = inner
            .lifecycle
            .lock()
            .expect("runtime lifecycle lock poisoned");
        lifecycle.clear();
        for state in snapshot.transitions {
            lifecycle.insert(state.shard_id, state);
        }
    }
    {
        let mut tokens = inner
            .lifecycle_tokens
            .lock()
            .expect("runtime lifecycle token lock poisoned");
        tokens.clear();
        for token in snapshot.tokens {
            tokens.insert((token.shard_id, token.operation.clone()), token);
        }
    }
    Status::ok()
}

pub(super) fn restore_lifecycle_snapshot_from_path_inner(inner: &DataNodeRuntimeInner) {
    let Some(path) = &inner.lifecycle_snapshot_path else {
        return;
    };
    let status = match fs::read(path) {
        Ok(bytes) => match decode_lifecycle_snapshot_file(&bytes) {
            Ok(snapshot) => restore_lifecycle_snapshot_inner(inner, snapshot),
            Err(err) => Status::error("bad_lifecycle_snapshot", err),
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Status::ok(),
        Err(err) => Status::error("lifecycle_snapshot_io", err.to_string()),
    };
    let mut report = inner
        .lifecycle_persistence
        .lock()
        .expect("runtime lifecycle persistence lock poisoned");
    report.last_restore_at_ms = now_ms();
    report.last_restore_status = Some(status.clone());
    if status.ok {
        report.restore_success_total += 1;
    } else {
        report.restore_failure_total += 1;
    }
}

pub(super) fn persist_lifecycle_snapshot_inner(inner: &DataNodeRuntimeInner) {
    let Some(path) = &inner.lifecycle_snapshot_path else {
        return;
    };
    let result: Result<(), Status> = if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| Status::error("lifecycle_snapshot_io", err.to_string()))
        } else {
            Ok(())
        }
    } else {
        Ok(())
    }
    // BINARY: the container byte and a protobuf snapshot. There is no JSON encode path left;
    // `decode_lifecycle_snapshot_file` keeps its JSON arm for the snapshots already on disk.
    .map(|_| encode_lifecycle_snapshot_file(&lifecycle_snapshot_inner(inner)))
    .and_then(|bytes| {
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, bytes)
            .and_then(|_| fs::rename(&tmp_path, path))
            .map_err(|err| Status::error("lifecycle_snapshot_io", err.to_string()))
    });
    let status = match result {
        Ok(()) => Status::ok(),
        Err(status) => status,
    };
    let mut report = inner
        .lifecycle_persistence
        .lock()
        .expect("runtime lifecycle persistence lock poisoned");
    report.last_persist_at_ms = now_ms();
    report.last_persist_status = Some(status.clone());
    if status.ok {
        report.persist_success_total += 1;
    } else {
        report.persist_failure_total += 1;
    }
}

pub(super) fn shard_has_queued_or_running_work(
    inner: &DataNodeRuntimeInner,
    shard_id: ShardId,
) -> bool {
    let queue = inner.queue.lock().expect("runtime queue lock poisoned");
    queue.running_shards.contains(&shard_id)
        || queue
            .by_shard
            .get(&shard_id)
            .map(|queued| !queued.is_empty())
            .unwrap_or(false)
}

pub(super) fn record_lifecycle_state_inner(
    inner: &DataNodeRuntimeInner,
    shard_id: ShardId,
    state: &str,
    operation: &str,
    load_version: u64,
    last_status: Option<Status>,
) {
    let token = inner
        .lifecycle_tokens
        .lock()
        .expect("runtime lifecycle token lock poisoned")
        .get(&(shard_id, operation.to_string()))
        .cloned();
    inner
        .lifecycle
        .lock()
        .expect("runtime lifecycle lock poisoned")
        .insert(
            shard_id,
            DataNodeShardLifecycleState {
                shard_id,
                state: state.to_string(),
                operation: operation.to_string(),
                load_version,
                updated_at_ms: now_ms(),
                last_status,
                scheduler_task_id: token.as_ref().map(|token| token.task_id),
                scheduler_generation: token.as_ref().map(|token| token.generation),
            },
        );
    persist_lifecycle_snapshot_inner(inner);
}

pub(super) fn validate_lifecycle_token_inner(
    inner: &DataNodeRuntimeInner,
    shard_id: ShardId,
    operation: &str,
    load_version: u64,
) -> Result<(), Status> {
    let token = inner
        .lifecycle_tokens
        .lock()
        .expect("runtime lifecycle token lock poisoned")
        .get(&(shard_id, operation.to_string()))
        .cloned();
    let Some(token) = token else {
        return Ok(());
    };
    if token.operation != operation {
        return Err(Status::error(
            "lifecycle_token_mismatch",
            format!(
                "expected lifecycle operation {}, got {operation}",
                token.operation
            ),
        ));
    }
    if token.load_version != 0 && token.load_version != load_version {
        return Err(Status::error(
            "lifecycle_token_mismatch",
            format!(
                "expected lifecycle load_version {}, got {load_version}",
                token.load_version
            ),
        ));
    }
    Ok(())
}

pub(super) fn validate_foreground_write_allowed_inner(
    inner: &DataNodeRuntimeInner,
    shard_id: ShardId,
    commands: &[Command],
) -> Result<(), Status> {
    if !commands.iter().any(is_write_command) {
        return Ok(());
    }
    let lifecycle = inner
        .lifecycle
        .lock()
        .expect("runtime lifecycle lock poisoned")
        .get(&shard_id)
        .cloned();
    let Some(lifecycle) = lifecycle else {
        return Ok(());
    };
    if matches!(
        lifecycle.state.as_str(),
        "loading" | "reloading" | "unloading"
    ) {
        return Err(Status::error(
            "lifecycle_write_blocked",
            format!(
                "foreground write rejected while shard {} is {} for {}",
                shard_id, lifecycle.state, lifecycle.operation
            ),
        ));
    }
    Ok(())
}

pub(super) fn final_loaded_lifecycle_state(readonly: bool, status: &Status) -> &'static str {
    if !status.ok {
        "failed"
    } else if readonly {
        "readonly"
    } else {
        "serving"
    }
}

#[cfg(test)]
mod tests {
    //! THE LIFECYCLE SNAPSHOT IS READ IN EITHER SHAPE.
    //!
    //! Reader half: the writers (the runtime's persist and the data server's save-to-path) still
    //! write JSON, so the binary files below are built by the test-only encoder and PLANTED where
    //! the real reader looks.
    use super::*;
    use crate::rebalance::SchedulerLifecycleToken;
    use crate::engine::TemporalEngine;

    fn a_snapshot_with_every_field(base: ShardId) -> DataNodeLifecycleSnapshot {
        DataNodeLifecycleSnapshot {
            format_version: 1,
            transitions: vec![
                DataNodeShardLifecycleState {
                    shard_id: base,
                    state: "loaded".to_string(),
                    operation: "load".to_string(),
                    load_version: 7,
                    updated_at_ms: 1_700_000_000_123,
                    last_status: Some(Status {
                        ok: false,
                        code: "stale".to_string(),
                        message: "an older load_version".to_string(),
                    }),
                    // Present AND ZERO: task 0 is a task, and absent is a different statement.
                    scheduler_task_id: Some(0),
                    scheduler_generation: Some(9),
                },
                DataNodeShardLifecycleState {
                    shard_id: base + 1,
                    state: "unloading".to_string(),
                    operation: "unload".to_string(),
                    load_version: 0,
                    updated_at_ms: 5,
                    last_status: None,
                    scheduler_task_id: None,
                    scheduler_generation: None,
                },
            ],
            tokens: vec![
                SchedulerLifecycleToken {
                    task_id: 31,
                    shard_id: base,
                    operation: "load".to_string(),
                    load_version: 7,
                    generation: 2,
                },
                SchedulerLifecycleToken {
                    task_id: 0,
                    shard_id: base + 1,
                    operation: "unload".to_string(),
                    load_version: 0,
                    generation: 0,
                },
            ],
        }
    }

    #[test]
    fn a_lifecycle_snapshot_codec_reads_both_shapes_with_every_field() {
        let original = a_snapshot_with_every_field(40);
        let json = serde_json::to_vec_pretty(&original).unwrap();
        assert_eq!(json[0], b'{');
        assert_eq!(
            decode_lifecycle_snapshot_file(&json).unwrap(),
            original,
            "the JSON arm changed the snapshot"
        );
        let binary = encode_lifecycle_snapshot_file(&original);
        assert_eq!(binary[0], LIFECYCLE_SNAPSHOT_CONTAINER_PROTOBUF);
        assert!(serde_json::from_slice::<serde_json::Value>(&binary).is_err());
        assert_eq!(
            decode_lifecycle_snapshot_file(&binary).unwrap(),
            original,
            "the binary arm dropped or changed a field"
        );
        // A body that does not decode is an error, never an empty snapshot ("nothing in flight").
        let mut broken = vec![LIFECYCLE_SNAPSHOT_CONTAINER_PROTOBUF];
        broken.extend_from_slice(&[0x0A, 0xFF, 0x01]);
        assert!(decode_lifecycle_snapshot_file(&broken).is_err());
        assert!(decode_lifecycle_snapshot_file(b"not a snapshot").is_err());
    }

    fn a_runtime_on(
        dir: &std::path::Path,
        snapshot_path: &std::path::Path,
    ) -> crate::data_node::DataNodeRuntime {
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.join("cache"),
            dir.join("blocks"),
            dir.join("indexes"),
        );
        crate::data_node::DataNodeRuntime::new_with_lifecycle_snapshot_path(
            engine,
            crate::data_node::DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
            snapshot_path,
        )
    }

    /// THE RUNTIME RESTORES A SNAPSHOT IN EITHER SHAPE, AND A BAD ONE IS A COUNTED FAILURE.
    ///
    /// Driven through the real constructor, which restores from the path. The two plants share no
    /// shard id, so a restored value can only have come from its own arm; the refusals have the
    /// restored cases as their control.
    #[test]
    fn a_runtime_restores_a_lifecycle_snapshot_in_either_shape_and_refuses_a_bad_one() {
        let dir = tempfile::tempdir().unwrap();

        // ---- the installed base: a JSON file, planted.
        let json_snapshot = a_snapshot_with_every_field(100);
        let json_path = dir.path().join("lifecycle-json.snapshot");
        std::fs::write(&json_path, serde_json::to_vec_pretty(&json_snapshot).unwrap()).unwrap();
        assert_eq!(std::fs::read(&json_path).unwrap()[0], b'{');
        let runtime = a_runtime_on(&dir.path().join("a"), &json_path);
        assert_eq!(runtime.lifecycle_snapshot(), json_snapshot, "the JSON file was not restored");
        let report = runtime.lifecycle_persistence_report();
        assert_eq!((report.restore_success_total, report.restore_failure_total), (1, 0));

        // ---- a binary file, planted.
        let binary_snapshot = a_snapshot_with_every_field(200);
        let binary_path = dir.path().join("lifecycle-binary.snapshot");
        std::fs::write(&binary_path, encode_lifecycle_snapshot_file(&binary_snapshot)).unwrap();
        let runtime = a_runtime_on(&dir.path().join("b"), &binary_path);
        assert_eq!(
            runtime.lifecycle_snapshot(),
            binary_snapshot,
            "the binary file was not restored"
        );
        let report = runtime.lifecycle_persistence_report();
        assert_eq!((report.restore_success_total, report.restore_failure_total), (1, 0));

        // ---- a binary file at a format version this build does not restore: refused by the
        // ---- restore, not by the decoder, and counted.
        let mut wrong_version = a_snapshot_with_every_field(300);
        wrong_version.format_version = 2;
        let wrong_path = dir.path().join("lifecycle-wrong-version.snapshot");
        std::fs::write(&wrong_path, encode_lifecycle_snapshot_file(&wrong_version)).unwrap();
        let runtime = a_runtime_on(&dir.path().join("c"), &wrong_path);
        let report = runtime.lifecycle_persistence_report();
        assert_eq!((report.restore_success_total, report.restore_failure_total), (0, 1));
        assert!(runtime.lifecycle_snapshot().transitions.is_empty());

        // ---- a binary file that does not decode: a counted failure, and NOT an empty restore that
        // ---- reads as "nothing in flight".
        let broken_path = dir.path().join("lifecycle-broken.snapshot");
        std::fs::write(
            &broken_path,
            [LIFECYCLE_SNAPSHOT_CONTAINER_PROTOBUF, 0x0A, 0xFF, 0x01],
        )
        .unwrap();
        let runtime = a_runtime_on(&dir.path().join("d"), &broken_path);
        let report = runtime.lifecycle_persistence_report();
        assert_eq!((report.restore_success_total, report.restore_failure_total), (0, 1));
        assert!(runtime.lifecycle_snapshot().transitions.is_empty());
    }

    /// THE RUNTIME PERSISTS A BINARY SNAPSHOT, AND A RESTART READS IT.
    ///
    /// Through the real paths: `require_lifecycle_token` persists, and a second runtime built on the
    /// same path restores. The JSON arm is not exercised here -- no writer produces it any more --
    /// and is driven by the planted file in the reader test above.
    #[test]
    fn the_runtime_persists_a_binary_lifecycle_snapshot_and_a_restart_reads_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persisted.snapshot");
        let runtime = a_runtime_on(&dir.path().join("first"), &path);
        runtime.require_lifecycle_token(SchedulerLifecycleToken {
            task_id: 0,
            shard_id: 61,
            operation: "load".to_string(),
            load_version: 5,
            generation: 6,
        });
        let written = std::fs::read(&path).unwrap();
        assert_eq!(
            written[0], LIFECYCLE_SNAPSHOT_CONTAINER_PROTOBUF,
            "the runtime did not persist the binary shape"
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&written).is_err(),
            "the persisted snapshot parses as JSON, so the container byte proves nothing"
        );

        let restarted = a_runtime_on(&dir.path().join("second"), &path);
        assert_eq!(restarted.lifecycle_snapshot(), runtime.lifecycle_snapshot());
        assert_eq!(restarted.lifecycle_tokens().len(), 1);
        assert_eq!(restarted.lifecycle_tokens()[0].task_id, 0);
        let report = restarted.lifecycle_persistence_report();
        assert_eq!((report.restore_success_total, report.restore_failure_total), (1, 0));
    }

    /// NO JSON ENCODE PATH IS LEFT FOR A LIFECYCLE SNAPSHOT.
    ///
    /// The test above reads the first byte of what the runtime wrote; this sees an encoder nobody
    /// calls YET, and covers the data server's save-to-path route, which has no test of its own.
    /// It reads each writer with comment lines removed and asserts none names `serde_json`. The
    /// POSITIVE CONTROL is the decoder, which still calls `serde_json::from_slice` (the permanent
    /// JSON arm): a scan that could not see that call would pass over any writer.
    #[test]
    fn no_json_encode_path_is_left_for_a_lifecycle_snapshot() {
        fn body(source: &str, from: &str, to: &str) -> String {
            let start = source
                .find(from)
                .unwrap_or_else(|| panic!("no declaration {from}"));
            let rest = &source[start..];
            let end = rest[from.len()..]
                .find(to)
                .unwrap_or_else(|| panic!("no declaration {to} after {from}"))
                + from.len();
            rest[..end]
                .lines()
                .filter(|line| !line.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n")
        }
        let lifecycle = include_str!("data_node_lifecycle.rs");
        let server = include_str!("../bin/temporalstore_data_server.rs");

        let persist = body(
            lifecycle,
            "pub(super) fn persist_lifecycle_snapshot_inner(",
            "pub(super) fn shard_has_queued_or_running_work(",
        );
        let save = body(
            server,
            "fn save_lifecycle_snapshot_file(",
            "fn load_lifecycle_snapshot_file(",
        );
        for (name, code) in [("persist_lifecycle_snapshot_inner", &persist), ("save_lifecycle_snapshot_file", &save)] {
            assert!(code.len() > 200, "{name}: the scan found almost nothing");
            assert!(
                code.contains("encode_lifecycle_snapshot_file("),
                "{name} does not call the binary encoder"
            );
            assert!(
                !code.contains("serde_json"),
                "{name} still reaches for serde_json: a JSON encode path survives. Delete it; \
                 the decoder keeps its JSON arm for the snapshots already on disk."
            );
        }
        let decoder = body(
            lifecycle,
            "pub fn decode_lifecycle_snapshot_file(",
            "pub fn encode_lifecycle_snapshot_file(",
        );
        assert!(
            decoder.contains("serde_json::from_slice"),
            "the control no longer sees the permanent JSON arm, so this scan could pass over a writer it cannot see"
        );
    }
}
