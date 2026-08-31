// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Worker loop + task dispatch execution, extracted from data_node.rs.

use super::*;
use std::sync::Arc;

pub(super) fn worker_loop(inner: Arc<DataNodeRuntimeInner>) {
    loop {
        let task = {
            let mut queue = inner.queue.lock().expect("runtime queue lock poisoned");
            loop {
                if let Some(task) = queue.pop_ready() {
                    break task;
                }
                queue = inner
                    .queue_signal
                    .wait(queue)
                    .expect("runtime queue lock poisoned");
            }
        };
        let shard_id = task.request.shard_id();
        let output = if Instant::now() > task.deadline {
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .timed_out_total += 1;
            task_timeout_output(task.kind)
        } else if take_canceled(&inner, task.job_id) {
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .canceled_total += 1;
            task_canceled_output(&task, "data node task canceled before execution")
        } else {
            let output = execute_task(&inner, &task);
            if task_output_status(&output).code == "job_canceled"
                && take_canceled(&inner, task.job_id)
            {
                inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned")
                    .canceled_total += 1;
            } else {
                let _ = take_canceled(&inner, task.job_id);
            }
            output
        };
        {
            let mut queue = inner.queue.lock().expect("runtime queue lock poisoned");
            queue.finish_shard(shard_id);
            inner.queue_signal.notify_all();
        }
        let finished = DataNodeTaskStatus {
            job_id: task.job_id,
            kind: task.kind,
            status: task_output_status(&output),
            output: Some(output),
            submitted_at_ms: task.submitted_at_ms,
            finished_at_ms: Some(now_ms()),
        };
        {
            let mut jobs = inner
                .jobs
                .lock()
                .expect("data node jobs lock poisoned");
            jobs.insert(task.job_id, finished);
            // A finished status carries its whole output, and a dump output carries the
            // serialized index: 2.4 MB for 20k records, 7.3 MB for 60k, growing with the
            // store. Nothing removed these, so every dump a node ever ran stayed resident.
            // Unfinished jobs are always kept - the queue holds far more than the retention
            // bound, and a queued job must stay observable until it reports.
            let keep = max_retained_finished_jobs();
            let finished_count = jobs.values().filter(|job| job.finished_at_ms.is_some()).count();
            if finished_count > keep {
                let mut finished_ids: Vec<u64> = jobs
                    .values()
                    .filter(|job| job.finished_at_ms.is_some())
                    .map(|job| job.job_id)
                    .collect();
                finished_ids.sort_unstable();
                for job_id in finished_ids.into_iter().take(finished_count - keep) {
                    jobs.remove(&job_id);
                }
            }
        }
        inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .completed_total += 1;
    }
}

/// TS_MAX_RETAINED_FINISHED_JOBS: how many completed job statuses stay queryable.
///
/// Each carries its full output, and a dump output carries the serialized index, so an
/// unbounded map meant a node retained one index copy per dump for its whole life.
fn max_retained_finished_jobs() -> usize {
    std::env::var("TS_MAX_RETAINED_FINISHED_JOBS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64)
}

pub(super) fn execute_task(inner: &DataNodeRuntimeInner, task: &QueuedTask) -> DataNodeTaskOutput {
    let cancellation = TaskCancellation {
        inner,
        job_id: task.job_id,
    };
    match &task.request {
        TaskRequest::Load(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node load canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Load(load_shard_with_inner(inner, request.clone()))
        }
        TaskRequest::Reload(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node reload canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Reload(reload_shard_with_inner(inner, request.clone()))
        }
        TaskRequest::Unload(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node unload canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Unload(unload_shard_with_inner(inner, request.clone(), false))
        }
        TaskRequest::Execute(request) => {
            if let Err(status) = validate_foreground_write_allowed_inner(
                inner,
                request.shard_id,
                std::slice::from_ref(&request.command),
            ) {
                return DataNodeTaskOutput::Execute(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
            }
            let response = inner.engine.execute(request.clone());
            if response.status.ok && is_write_command(&request.command) {
                mark_dirty(
                    &inner.dirty,
                    request.shard_id,
                    command_key(&request.command).as_deref(),
                );
            }
            DataNodeTaskOutput::Execute(response)
        }
        TaskRequest::CheckedExecute(request) => {
            if let Err(status) = validate_foreground_write_allowed_inner(
                inner,
                request.shard_id,
                std::slice::from_ref(&request.command),
            ) {
                return DataNodeTaskOutput::CheckedExecute(CheckedExecuteResponse {
                    status: status.clone(),
                    response: ExecuteResponse {
                        status,
                        response: CommandResponse::Empty,
                    },
                });
            }
            let response = inner.engine.execute_checked(request.clone());
            if response.status.ok && is_write_command(&request.command) {
                mark_dirty(
                    &inner.dirty,
                    request.shard_id,
                    command_key(&request.command).as_deref(),
                );
            }
            DataNodeTaskOutput::CheckedExecute(response)
        }
        TaskRequest::Dump(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node dump canceled before index export");
            }
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node dump canceled before dirty flush");
            }
            let selected_buckets = if request.selected_routing_buckets.is_empty() {
                inner
                    .engine
                    .storage_lifecycle_plan(StorageLifecycleRequest {
                        shard_id: request.shard_id,
                        selected_dump_buckets: Vec::new(),
                        max_dump_buckets_per_round: 0,
                        min_undumped_wal_records: 0,
                        purge_delayed_destroy: false,
                        prune_bucket_dump_manifests: false,
                        roll_forward_bucket_dump_installs: false,
                        follower_replay_cursors: Vec::new(),
                        page_gc_shared_store_cursors: Vec::new(),
                        page_gc_raft_snapshot_refs: Vec::new(),
                        page_gc_checkpoint_floor_slab_id: None,
                        page_gc_raft_install_floor_slab_id: None,
                        page_gc_delayed_destroy_grace_ms: 0,
                        invalidate_cache: false,
                        warm_cache: false,
                    })
                    .selected_dump_buckets
            } else {
                request.selected_routing_buckets.clone()
            };
            let bucket_dump_manifest = inner
                .engine
                .create_bucket_dump_manifest(request.shard_id, selected_buckets.clone())
                .ok();
            // Only clear the worker's scheduling dirty set when the dump actually succeeded.
            // A failed dump (durability barrier / capture error) returns None and leaves the
            // engine dirty + the reclaim frontier unadvanced (apply_storage_lifecycle skips its
            // dirty-clear on None); clearing the worker set here would stop re-scheduling these
            // still-undumped buckets on a persistently failing disk.
            // The manifest already carries the serialized index, and the response only wants
            // its length. Exporting the index a second time just to measure it serialized the
            // whole thing twice per dump - 403 KB per 4k records here, and it grows with the
            // store. Fall back to the export only when there is no manifest to measure.
            let index_bytes = match bucket_dump_manifest.as_ref() {
                Some(manifest) => manifest.index_bytes.len(),
                None => inner
                    .engine
                    .export_index_bytes(request.shard_id)
                    .map(|bytes| bytes.len())
                    .unwrap_or_default(),
            };
            let dirty_objects_flushed = if bucket_dump_manifest.is_some() {
                clear_dirty_shard_buckets(
                    &inner.dirty,
                    &inner.engine,
                    request.shard_id,
                    &selected_buckets,
                )
            } else {
                0
            };
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .dump_runs += 1;
            DataNodeTaskOutput::Dump(DumpShardResponse {
                status: Status::ok(),
                shard_id: request.shard_id,
                index_bytes,
                dirty_objects_flushed,
                bucket_dump_manifest,
            })
        }
        TaskRequest::Compact(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node compaction canceled before scan");
            }
            DataNodeTaskOutput::Compact(run_compaction_inner(inner, request.clone()))
        }
        TaskRequest::Gc(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node gc canceled before dirty cleanup");
            }
            DataNodeTaskOutput::Gc(run_gc_inner(inner, request.clone()))
        }
        TaskRequest::StorageManager(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node storage manager canceled before prepare",
                );
            }
            let report = inner.engine.run_storage_manager_cycle(request.clone());
            let status = if report.completed && report.errors.is_empty() {
                Status::ok()
            } else {
                Status::error("storage_manager_failed", report.errors.join(";"))
            };
            *inner
                .last_storage_manager_cycle
                .lock()
                .expect("last storage manager cycle lock poisoned") = Some(report.clone());
            let mut stats = inner.stats.lock().expect("runtime stats lock poisoned");
            stats.storage_manager_runs += 1;
            stats.storage_manager_loops += 1;
            DataNodeTaskOutput::StorageManager(StorageManagerResponse { status, report })
        }
    }
}


