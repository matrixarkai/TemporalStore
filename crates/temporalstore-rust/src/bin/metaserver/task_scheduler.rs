// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// impl MetaTaskScheduler, split from metaserver.rs. Textually include!d, so it
// shares the parent bin body flat scope + use-imports; no mod wrapper.

impl MetaTaskScheduler {
    fn from_env() -> io::Result<Self> {
        let scheduler = std::env::var("TS_META_SCHEDULER_SNAPSHOT")
            .ok()
            .map(|path| Self::with_snapshot_path(PathBuf::from(path)))
            .transpose()
            .map(|scheduler| scheduler.unwrap_or_default())?;
        // Applied here as well as in `with_snapshot_path`: without a snapshot
        // path that branch is never taken, and the pacing would silently only
        // work for deployments that happen to persist their queue.
        Ok(Self {
            default_options: Self::options_from_env(),
            ..scheduler
        })
    }

    /// Pacing from the environment, falling back to the compiled defaults so an
    /// unconfigured metaserver behaves exactly as it did.
    fn options_from_env() -> TaskSchedulerOptions {
        let defaults = TaskSchedulerOptions::default();
        TaskSchedulerOptions {
            base_postpone_ms: env_u64(
                "TS_META_TASK_SCHEDULER_BASE_POSTPONE_MS",
                defaults.base_postpone_ms,
            ),
            max_postpone_ms: env_u64(
                "TS_META_TASK_SCHEDULER_MAX_POSTPONE_MS",
                defaults.max_postpone_ms,
            ),
            max_retry_times: env_u64(
                "TS_META_TASK_SCHEDULER_MAX_RETRY_TIMES",
                defaults.max_retry_times,
            ),
            // How many tasks the metaserver will have in flight at once. The
            // compiled default is 1, which drives one shard move or load at a
            // time; a large fleet will want more, and now can have it without
            // a rebuild.
            max_inflight: env_u64(
                "TS_META_TASK_SCHEDULER_MAX_INFLIGHT",
                defaults.max_inflight as u64,
            ) as usize,
        }
    }

    fn with_snapshot_path(path: PathBuf) -> io::Result<Self> {
        let (scheduler, executions) = if path.exists() {
            let bytes = fs::read(&path)?;
            decode_meta_scheduler_file(&bytes)?
        } else {
            (DeterministicTaskScheduler::default(), Vec::new())
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(scheduler)),
            executions: Arc::new(Mutex::new(executions)),
            snapshot_path: Some(path),
            default_options: Self::options_from_env(),
        })
    }

    fn snapshot(&self) -> MetaSchedulerSnapshotResponse {
        let scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
        MetaSchedulerSnapshotResponse {
            status: Status::ok(),
            snapshot: Some(scheduler.export_snapshot()),
            queue_len: scheduler.queue_len(),
        }
    }

    fn submit(&self, request: MetaSchedulerSubmitRequest) -> MetaSchedulerTaskResponse {
        let (task, queue_len, persist_status) = {
            let mut scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
            let task = scheduler.submit(request.priority, request.now_ms, request.kind);
            let queue_len = scheduler.queue_len();
            let persist_status = self.persist_locked(&scheduler);
            (task, queue_len, persist_status)
        };
        MetaSchedulerTaskResponse {
            status: persist_status,
            lifecycle_token: task.lifecycle_token(),
            task: Some(task),
            queue_len,
        }
    }

    fn run_next(&self, request: MetaSchedulerRunRequest) -> MetaSchedulerRunResponse {
        let mut scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
        match scheduler.run_next(
            request.now_ms,
            request.result,
            request.options.unwrap_or(self.default_options),
        ) {
            Ok(report) => MetaSchedulerRunResponse {
                status: if report.is_some() {
                    self.persist_locked(&scheduler)
                } else {
                    Status::ok()
                },
                report,
                queue_len: scheduler.queue_len(),
            },
            Err(err) => MetaSchedulerRunResponse {
                status: Status::error("scheduler_error", err.to_string()),
                report: None,
                queue_len: scheduler.queue_len(),
            },
        }
    }

    fn restore(&self, request: MetaSchedulerRestoreRequest) -> MetaSchedulerSnapshotResponse {
        match DeterministicTaskScheduler::restore_snapshot(request.snapshot) {
            Ok(restored) => {
                let mut scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
                *scheduler = restored;
                let status = self.persist_locked(&scheduler);
                MetaSchedulerSnapshotResponse {
                    status,
                    snapshot: Some(scheduler.export_snapshot()),
                    queue_len: scheduler.queue_len(),
                }
            }
            Err(err) => MetaSchedulerSnapshotResponse {
                status: Status::error("scheduler_snapshot_error", err.to_string()),
                snapshot: None,
                queue_len: self
                    .inner
                    .lock()
                    .expect("meta scheduler lock poisoned")
                    .queue_len(),
            },
        }
    }

    fn execute_next(&self, request: MetaSchedulerExecuteRequest) -> MetaSchedulerExecuteResponse {
        let Some(task) = self.peek_next(request.now_ms) else {
            return MetaSchedulerExecuteResponse {
                status: Status::error("scheduler_empty", "no runnable scheduler task"),
                task: None,
                lifecycle_token: None,
                node_addr: request.node_addr,
                dry_run: request.dry_run,
                calls: Vec::new(),
                scheduler_report: None,
                node_lifecycle: None,
                lifecycle_state: None,
                raft_membership_report: None,
                queue_len: self.queue_len(),
            };
        };

        let execution = execute_scheduler_task_on_node(&task, &request);
        if request.dry_run {
            return MetaSchedulerExecuteResponse {
                status: execution.status,
                task: Some(task),
                lifecycle_token: execution.lifecycle_token,
                node_addr: request.node_addr,
                dry_run: true,
                calls: execution.calls,
                scheduler_report: None,
                node_lifecycle: None,
                lifecycle_state: None,
                raft_membership_report: execution.raft_membership_report,
                queue_len: self.queue_len(),
            };
        }

        let result = classify_scheduler_execution_result(&execution.status);
        let mut calls = execution.calls;
        let (node_lifecycle, lifecycle_state) = if execution.lifecycle_token.is_some() {
            fetch_node_lifecycle(
                &request.node_addr,
                request.http.into(),
                execution.lifecycle_token.as_ref(),
                &mut calls,
            )
        } else {
            (None, None)
        };
        let run = self.run_next(MetaSchedulerRunRequest {
            now_ms: request.now_ms,
            result,
            options: request.options,
        });
        let status = if execution.status.ok {
            run.status.clone()
        } else {
            execution.status.clone()
        };
        let response = MetaSchedulerExecuteResponse {
            status,
            task: Some(task.clone()),
            lifecycle_token: execution.lifecycle_token.clone(),
            node_addr: request.node_addr,
            dry_run: false,
            calls: calls.clone(),
            scheduler_report: run.report.clone(),
            node_lifecycle,
            lifecycle_state: lifecycle_state.clone(),
            raft_membership_report: execution.raft_membership_report.clone(),
            queue_len: run.queue_len,
        };
        let mut response = response;
        let persisted = self.record_execution(MetaSchedulerExecutionRecord {
            task_id: task.id,
            node_addr: response.node_addr.clone(),
            status: response.status.clone(),
            scheduler_result: result,
            retry_times: run
                .report
                .as_ref()
                .map(|report| report.retry_times)
                .unwrap_or(0),
            next_run_time_ms: run
                .report
                .as_ref()
                .and_then(|report| report.next_run_time_ms),
            calls,
            lifecycle_token: response.lifecycle_token.clone(),
            lifecycle_state,
            raft_membership_report: response.raft_membership_report.clone(),
            queue_len: response.queue_len,
        });
        // Only when the execution itself succeeded. An execution that already
        // failed has the more useful error, and the three sibling paths have no
        // separate outcome to lose so they simply return theirs.
        if response.status.ok && !persisted.ok {
            response.status = persisted;
        }
        response
    }

    fn peek_next(&self, now_ms: u64) -> Option<SchedulerTask> {
        self.inner
            .lock()
            .expect("meta scheduler lock poisoned")
            .snapshot()
            .into_iter()
            .find(|task| task.next_run_time_ms <= now_ms)
    }

    fn queue_len(&self) -> usize {
        self.inner
            .lock()
            .expect("meta scheduler lock poisoned")
            .queue_len()
    }

    fn executions(&self) -> MetaSchedulerExecutionsResponse {
        MetaSchedulerExecutionsResponse {
            status: Status::ok(),
            executions: self
                .executions
                .lock()
                .expect("meta scheduler executions lock poisoned")
                .clone(),
        }
    }

    fn validate_finish_load(&self, request: &LoadFinishRequest) -> Result<(), Status> {
        match (request.scheduler_task_id, request.scheduler_generation) {
            (None, None) => return Ok(()),
            (Some(_), Some(_)) => {}
            _ => {
                return Err(Status::error(
                    "invalid_scheduler_finish_load",
                    "finish_load must include both scheduler_task_id and scheduler_generation",
                ));
            }
        }
        let task_id = request.scheduler_task_id.unwrap();
        let generation = request.scheduler_generation.unwrap();
        let executions = self
            .executions
            .lock()
            .expect("meta scheduler executions lock poisoned");
        let Some(record) = executions.iter().rev().find(|record| {
            record.task_id == task_id
                && record.lifecycle_token.as_ref().is_some_and(|token| {
                    token.task_id == task_id
                        && token.generation == generation
                        && token.shard_id == request.shard_id
                        && token.operation == "load"
                })
        }) else {
            return Err(Status::error(
                "scheduler_finish_load_not_found",
                "no matching scheduler load execution found for finish_load",
            ));
        };
        if record.node_addr != request.server_addr {
            return Err(Status::error(
                "scheduler_finish_load_node_mismatch",
                "finish_load server does not match scheduler execution node",
            ));
        }
        if !record.status.ok {
            return Err(Status::error(
                "scheduler_finish_load_not_ready",
                "scheduler execution did not complete successfully",
            ));
        }
        let Some(token) = &record.lifecycle_token else {
            return Err(Status::error(
                "scheduler_finish_load_not_found",
                "scheduler execution has no lifecycle token",
            ));
        };
        if token.load_version != request.load_version {
            return Err(Status::error(
                "scheduler_finish_load_version_mismatch",
                "finish_load load_version does not match scheduler token",
            ));
        }
        if let Some(state) = &record.lifecycle_state {
            if state.load_version != request.load_version || state.state == "failed" {
                return Err(Status::error(
                    "scheduler_finish_load_state_mismatch",
                    "nodeserver lifecycle state does not confirm the requested load",
                ));
            }
        }
        Ok(())
    }

    /// Record one execution and return what persisting it did.
    ///
    /// The status used to be dropped here while `submit`, `run_next` and
    /// `restore` all fold theirs into the response they return. `persist_current`
    /// writes the execution history *and* the scheduler snapshot, so a failed
    /// write left the post-execution state non-durable while the caller was told
    /// the execution succeeded -- and a restart could hand out a task that had
    /// already run.
    fn record_execution(&self, record: MetaSchedulerExecutionRecord) -> Status {
        {
            let mut executions = self
                .executions
                .lock()
                .expect("meta scheduler executions lock poisoned");
            executions.push(record);
            const MAX_EXECUTION_RECORDS: usize = 128;
            if executions.len() > MAX_EXECUTION_RECORDS {
                let overflow = executions.len() - MAX_EXECUTION_RECORDS;
                executions.drain(0..overflow);
            }
        }
        self.persist_current()
    }

    fn persist_current(&self) -> Status {
        let Some(path) = &self.snapshot_path else {
            return Status::ok();
        };
        let scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
        let executions = self
            .executions
            .lock()
            .expect("meta scheduler executions lock poisoned");
        match save_scheduler_snapshot(path, &scheduler.export_snapshot(), &executions) {
            Ok(()) => Status::ok(),
            Err(err) => Status::error("scheduler_persist_failed", err.to_string()),
        }
    }

    fn persist_locked(&self, scheduler: &DeterministicTaskScheduler) -> Status {
        match &self.snapshot_path {
            Some(path) => {
                let executions = self
                    .executions
                    .lock()
                    .expect("meta scheduler executions lock poisoned");
                match save_scheduler_snapshot(path, &scheduler.export_snapshot(), &executions) {
                    Ok(()) => Status::ok(),
                    Err(err) => Status::error("scheduler_persist_failed", err.to_string()),
                }
            }
            None => Status::ok(),
        }
    }
}
