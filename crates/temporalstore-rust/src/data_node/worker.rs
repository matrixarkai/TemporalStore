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
        inner
            .jobs
            .lock()
            .expect("data node jobs lock poisoned")
            .insert(task.job_id, finished);
        inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .completed_total += 1;
    }
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
                    command_key(&request.command),
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
                    command_key(&request.command),
                );
            }
            DataNodeTaskOutput::CheckedExecute(response)
        }
        TaskRequest::Dump(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node dump canceled before index export");
            }
            let index_bytes = inner
                .engine
                .export_index_bytes(request.shard_id)
                .map(|bytes| bytes.len())
                .unwrap_or_default();
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node dump canceled before dirty flush");
            }
            let selected_slots = if request.selected_routing_slots.is_empty() {
                inner
                    .engine
                    .storage_lifecycle_plan(StorageLifecycleRequest {
                        shard_id: request.shard_id,
                        selected_dump_slots: Vec::new(),
                        max_dump_slots_per_round: 0,
                        min_undumped_oplog_records: 0,
                        purge_delayed_destroy: false,
                        prune_slot_dump_manifests: false,
                        roll_forward_slot_dump_installs: false,
                        follower_replay_cursors: Vec::new(),
                        page_gc_shared_store_cursors: Vec::new(),
                        page_gc_raft_snapshot_refs: Vec::new(),
                        page_gc_checkpoint_floor_segment_id: None,
                        page_gc_raft_install_floor_segment_id: None,
                        page_gc_delayed_destroy_grace_ms: 0,
                        invalidate_cache: false,
                        warm_cache: false,
                    })
                    .selected_dump_slots
            } else {
                request.selected_routing_slots.clone()
            };
            let slot_dump_manifest = inner
                .engine
                .create_slot_dump_manifest(request.shard_id, selected_slots.clone())
                .ok();
            let dirty_objects_flushed = clear_dirty_shard_slots(
                &inner.dirty,
                &inner.engine,
                request.shard_id,
                &selected_slots,
            );
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
                slot_dump_manifest,
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


