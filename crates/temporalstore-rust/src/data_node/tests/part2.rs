// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 2, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn distributed_admission_policy_aggregates_peer_counters() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    let peers = vec![
        DistributedAdmissionPeerSnapshot {
            node_id: "node-a".to_string(),
            shard_id: 7,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 5,
            write_count: 3,
        },
        DistributedAdmissionPeerSnapshot {
            node_id: "node-b".to_string(),
            shard_id: 7,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 4,
            write_count: 6,
        },
        DistributedAdmissionPeerSnapshot {
            node_id: "node-c".to_string(),
            shard_id: 8,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 100,
            write_count: 100,
        },
    ];

    let allowed = runtime.distributed_admission_decision(7, &peers, 10, 10, 12);
    assert!(allowed.status.ok, "{allowed:?}");
    assert_eq!(allowed.participating_nodes, 2);
    assert_eq!(allowed.aggregate_read_count, 9);
    assert_eq!(allowed.aggregate_write_count, 9);
    assert!(allowed.read_allowed);
    assert!(allowed.write_allowed);

    let rejected = runtime.distributed_admission_decision(7, &peers, 9, 9, 12);
    assert_eq!(rejected.status.code, "distributed_admission_rejected");
    assert!(!rejected.read_allowed);
    assert!(!rejected.write_allowed);
    assert!(rejected
        .reasons
        .contains(&"distributed_read_budget_exceeded".to_string()));
    assert!(rejected
        .reasons
        .contains(&"distributed_write_budget_exceeded".to_string()));

    let stale = runtime.distributed_admission_decision(7, &peers, 10, 10, 13);
    assert!(stale
        .reasons
        .contains(&"stale_distributed_admission_topology".to_string()));
}

#[test]
fn multi_process_lifecycle_validation_requires_load_reload_unload_and_restart() {
    let reports = vec![
        DataNodeLifecycleReport {
            loaded_shard_count: 1,
            serving_count: 1,
            readonly_count: 0,
            queued_count: 0,
            running_count: 0,
            unloading_count: 0,
            failed_count: 0,
            max_load_version: 42,
            shards: Vec::new(),
            transitions: vec![
                DataNodeShardLifecycleState {
                    shard_id: 7,
                    state: "serving".to_string(),
                    operation: "load".to_string(),
                    load_version: 42,
                    updated_at_ms: 1,
                    last_status: Some(Status::ok()),
                    scheduler_task_id: Some(1),
                    scheduler_generation: Some(10),
                },
                DataNodeShardLifecycleState {
                    shard_id: 7,
                    state: "readonly".to_string(),
                    operation: "reload".to_string(),
                    load_version: 43,
                    updated_at_ms: 2,
                    last_status: Some(Status::ok()),
                    scheduler_task_id: Some(2),
                    scheduler_generation: Some(11),
                },
            ],
            ..DataNodeLifecycleReport::default()
        },
        DataNodeLifecycleReport {
            loaded_shard_count: 0,
            serving_count: 0,
            readonly_count: 0,
            queued_count: 0,
            running_count: 0,
            unloading_count: 0,
            failed_count: 0,
            max_load_version: 43,
            shards: Vec::new(),
            transitions: vec![DataNodeShardLifecycleState {
                shard_id: 7,
                state: "unloaded".to_string(),
                operation: "unload".to_string(),
                load_version: 43,
                updated_at_ms: 3,
                last_status: Some(Status::ok()),
                scheduler_task_id: Some(3),
                scheduler_generation: Some(12),
            }],
            ..DataNodeLifecycleReport::default()
        },
    ];
    let persistence = vec![
        DataNodeLifecyclePersistenceReport {
            enabled: true,
            last_restore_status: Some(Status::ok()),
            restore_success_total: 1,
            ..DataNodeLifecyclePersistenceReport::default()
        },
        DataNodeLifecyclePersistenceReport {
            enabled: true,
            last_restore_status: Some(Status::ok()),
            restore_success_total: 1,
            ..DataNodeLifecyclePersistenceReport::default()
        },
    ];

    let validated = validate_multi_process_lifecycle_reports(&reports, &persistence);
    assert!(validated.passed, "{validated:?}");
    assert_eq!(validated.node_count, 2);
    assert!(validated.load_validated);
    assert!(validated.reload_validated);
    assert!(validated.unload_validated);
    assert!(validated.restart_restore_validated);
    assert!(validated.all_nodes_have_persistence);

    let missing_restart = validate_multi_process_lifecycle_reports(&reports, &[]);
    assert!(!missing_restart.passed);
    assert!(missing_restart
        .blockers
        .contains(&"restart_restore_not_validated".to_string()));
}

#[test]
fn runtime_direct_unload_rejects_busy_shard_without_unloading() {
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 8,
            max_background_queue_depth: 4,
        },
    );

    let queued = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 7,
            selected_routing_buckets: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(queued.status.ok, "{queued:?}");

    let unload = runtime.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 7 });
    assert_eq!(unload.status.code, "shard_busy");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 1);
    assert_eq!(lifecycle.failed_count, 1);
    assert_eq!(lifecycle.transitions[0].state, "failed");
    assert_eq!(lifecycle.transitions[0].operation, "unload");
    assert_eq!(
        lifecycle.transitions[0].last_status.as_ref().unwrap().code,
        "shard_busy"
    );
}

#[test]
fn runtime_queued_unload_waits_for_prior_shard_work() {
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

    let write = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "before-unload".to_string(),
                value: b"value".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let unload = runtime.submit_unload(
        crate::control::UnloadShardRequest { shard_id: 7 },
        RequestController { timeout_ms: 1000 },
    );
    assert!(write.status.ok, "{write:?}");
    assert!(unload.status.ok, "{unload:?}");

    let first = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("first shard task should be ready");
    assert_eq!(first.job_id, write.job_id);
    let output = execute_task(&runtime.inner, &first);
    let DataNodeTaskOutput::Execute(response) = output else {
        panic!("expected execute output");
    };
    assert!(response.status.ok, "{response:?}");
    runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .finish_shard(7);

    let second = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("queued unload should be ready after prior work");
    assert_eq!(second.job_id, unload.job_id);
    let output = execute_task(&runtime.inner, &second);
    let DataNodeTaskOutput::Unload(response) = output else {
        panic!("expected unload output");
    };
    assert!(response.status.ok, "{response:?}");
    assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
}


/// THE RUNTIME'S MUTEXES HAVE ONE GLOBAL ORDER, AND HERE IS THE NUMBER.
///
/// `DataNodeRuntimeInner` declares thirteen mutexes. Nothing stops two of them being held at
/// once, and several paths do hold two or three: `cancel_job` holds `jobs` then `queue` and
/// charges `stats` inside both, `submit` holds `queue` across the two rejection paths that
/// charge `stats`, and `stats()` holds three at once so its report is one cut rather than three
/// readings taken at three instants.
///
/// Two of those paths taking the same pair in OPPOSITE orders is a deadlock with no symptom
/// until it happens and no recovery when it does -- and it is invisible to every test that
/// exercises one path at a time, which is every test there was. `stats()` took
/// stats -> dirty -> queue while `submit` took queue -> stats, so a reporting thread holding
/// `stats` waited for `queue` while a submitting thread holding `queue` waited for `stats`.
/// Both threads exist in a running data node and both calls sit on request paths.
///
/// So this walks the source and asserts a PROPERTY rather than an example: the directed graph of
/// "lock A was held when lock B was taken" edges has no cycle. A cycle of length two is the
/// deadlock above; a longer cycle is the same deadlock with more threads in it. Acyclic means
/// some global order exists that every site respects, which is the only thing that makes holding
/// two of these at once safe.
///
/// NO RANKING IS HAND-ASSIGNED HERE, deliberately. A test that declared "queue outranks stats"
/// would assert the fix rather than the property, and would need editing every time a lock is
/// added -- at which point it asserts whatever the editor believed that day. Cycle-freedom needs
/// no list to maintain and fails for a lock that does not exist yet.
///
/// WHAT THIS CANNOT SEE, stated so a pass is not read as more than it is: it reads the three
/// files where `self.inner.<field>.lock()` / `runtime.inner.<field>.lock()` appears, so a path
/// reaching one of these mutexes through a helper that takes `&DataNodeRuntimeInner` is not
/// followed, and neither is a lock held across a channel hand-off. The denominators below say
/// how many acquisitions and edges were actually seen, so a scan that silently stopped matching
/// fails here instead of reading exactly like a clean run.
#[test]
fn the_data_node_runtime_locks_have_one_global_order() {
    const SOURCES: [(&str, &str); 3] = [
        ("data_node.rs", include_str!("../../data_node.rs")),
        (
            "data_node/runtime_reports.rs",
            include_str!("../runtime_reports.rs"),
        ),
        (
            "data_node/runtime_storage.rs",
            include_str!("../runtime_storage.rs"),
        ),
    ];

    // One held guard. `depth` is the brace depth it was taken at, so leaving that block drops it;
    // `name` is what it is bound to, so an explicit `drop(name)` ends it early.
    struct Held {
        field: String,
        line: usize,
        depth: i64,
        name: String,
    }

    // (outer field, inner field, file, line the outer was taken, line the inner was taken)
    let mut edges: Vec<(String, String, &str, usize, usize)> = Vec::new();
    let mut acquisitions = 0usize;

    for (file, source) in SOURCES {
        // Collapse a chain written one method per line onto the line it starts on, so
        // `self\n    .inner\n    .queue\n    .lock()` matches as one acquisition. Without this
        // step the scan matches almost nothing in this crate -- and a scan that matched nothing
        // reads exactly like a clean one, which is what the denominator assertions below exist
        // to catch.
        let raw: Vec<&str> = source.lines().collect();
        let mut joined: Vec<String> = raw.iter().map(|line| (*line).to_string()).collect();
        let mut owner: Vec<usize> = (0..raw.len()).collect();
        for index in 1..raw.len() {
            if raw[index].trim_start().starts_with('.') {
                let target = owner[index - 1];
                let continuation = raw[index].trim_start().to_string();
                joined[target].push_str(&continuation);
                joined[index].clear();
                owner[index] = target;
            }
        }

        let mut held: Vec<Held> = Vec::new();
        let mut depth: i64 = 0;
        for (index, line) in joined.iter().enumerate() {
            let trimmed = line.trim_start();

            // A new function body resets the stack. Brace depth alone does not: every method of
            // an `impl` block sits at the same depth as its siblings.
            if trimmed.starts_with("fn ")
                || trimmed.starts_with("pub fn ")
                || trimmed.starts_with("pub(crate) fn ")
                || trimmed.starts_with("pub(super) fn ")
                || trimmed.starts_with("async fn ")
            {
                held.clear();
            }

            // An explicit `drop(name)` ends that guard's region before its block does.
            held.retain(|guard| !line.contains(&format!("drop({})", guard.name)));

            if let Some(field) = runtime_lock_field(line) {
                acquisitions += 1;
                for outer in &held {
                    edges.push((outer.field.clone(), field.clone(), file, outer.line, index + 1));
                }
                if let Some(name) = bound_guard_name(trimmed) {
                    held.push(Held {
                        field,
                        line: index + 1,
                        depth,
                        name,
                    });
                }
            }

            depth += line.matches('{').count() as i64;
            depth -= line.matches('}').count() as i64;
            // Leaving the block a guard was taken in drops it.
            held.retain(|guard| guard.depth <= depth);
        }
    }

    // ---- DENOMINATORS, AND THE POSITIVE CONTROL -------------------------------------------
    //
    // "No cycle" is satisfied just as well by a scan that matched nothing, so the counts are
    // asserted before the property is. The thresholds sit far below what these files hold today:
    // they are here to catch a scan that STOPPED matching, not to pin a count a refactor may
    // legitimately move.
    eprintln!(
        "[runtime lock order] {acquisitions} acquisition(s), {} nesting edge(s), {} file(s)",
        edges.len(),
        SOURCES.len(),
    );
    assert!(
        acquisitions >= 30,
        "the scan found only {acquisitions} `inner.<field>.lock()` acquisitions across the three \
runtime files. It matched almost nothing, so the cycle check below proves nothing -- fix the \
scan (the chain-collapsing step is the usual reason) before reading its result",
    );
    assert!(
        edges.len() >= 5,
        "the scan found {} nesting edge(s). These paths DO hold two of the runtime's mutexes at \
once -- `cancel_job` holds `jobs` then `queue`, `stats()` holds three -- so finding next to no \
nesting means the guard-lifetime tracking stopped working and an inversion would go unseen",
        edges.len(),
    );
    // One named edge the scan must see, taken from code this change does NOT touch: `submit` and
    // `cancel_job` both hold `queue` while charging `stats`. Naming an edge produced by the
    // reordered `stats()` instead was tried and rejected -- under mutation the control fired
    // first and the cycle assertion below never ran, so the mutation proved the control worked
    // and said nothing about the thing being guarded. A control has to be anchored in code the
    // change cannot move.
    assert!(
        edges
            .iter()
            .any(|(from, to, ..)| from == "queue" && to == "stats"),
        "the scan did not see `queue` held while `stats` was taken, which `submit` and \
`cancel_job` both do. It is not reading these files the way it thinks it is",
    );
    // And the detector must not be pinned to one outer lock: a bug that only ever recorded edges
    // out of `queue` would make every graph acyclic and pass for ever.
    let mut outer_fields: Vec<&str> = Vec::new();
    for (from, ..) in &edges {
        if !outer_fields.contains(&from.as_str()) {
            outer_fields.push(from.as_str());
        }
    }
    assert!(
        outer_fields.len() >= 3,
        "every nesting edge the scan found comes out of one of {} lock(s) ({:?}). More than that \
many are held across another acquisition in these files, so the scan is recording edges out of \
one guard only -- and an inversion involving any other guard would be invisible",
        outer_fields.len(),
        outer_fields,
    );

    // ---- THE PROPERTY ---------------------------------------------------------------------
    let mut nodes: Vec<&str> = Vec::new();
    for (from, to, ..) in &edges {
        for node in [from.as_str(), to.as_str()] {
            if !nodes.contains(&node) {
                nodes.push(node);
            }
        }
    }
    let mut cycles: Vec<String> = Vec::new();
    for (from, to, ..) in &edges {
        if from == to {
            continue;
        }
        if !edges
            .iter()
            .any(|(other_from, other_to, ..)| other_from == to && other_to == from)
        {
            continue;
        }
        let mut description = format!("`{from}` taken while `{to}` held, AND the reverse:");
        for (edge_from, edge_to, file, outer, inner) in &edges {
            let forward = edge_from == from && edge_to == to;
            let backward = edge_from == to && edge_to == from;
            if forward || backward {
                description.push_str(&format!(
                    "\n      {file}:{outer} holds `{edge_from}` -> :{inner} takes `{edge_to}`"
                ));
            }
        }
        if !cycles.iter().any(|seen| seen.contains(&format!("`{to}`")) && seen.contains(&format!("`{from}`")))
        {
            cycles.push(description);
        }
    }

    assert!(
        cycles.is_empty(),
        "{} of the {} nesting edges over {} runtime mutexes form a cycle, so no global lock \
order exists and two threads on these paths deadlock outright -- each holding what the other \
waits for, with no timeout and no recovery:\n  {}\n\nFix it by taking them in the order the REST \
of the runtime already uses. Do NOT fix it by dropping a guard and re-taking it: releasing one \
mid-report tears the single cut that holding them together exists to produce",
        cycles.len(),
        edges.len(),
        nodes.len(),
        cycles.join("\n  "),
    );
}

/// The field name in `<anything>.inner.<field>.lock()`, once the chain is on one line.
fn runtime_lock_field(line: &str) -> Option<String> {
    let after = line.split(".inner.").nth(1)?;
    let field: String = after
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if field.is_empty() {
        return None;
    }
    if !after[field.len()..].starts_with(".lock()") {
        return None;
    }
    Some(field)
}

/// The name a guard is bound to, for an acquisition whose STATEMENT ENDS at the acquisition.
///
/// `let queue = runtime.inner.queue.lock().expect("...");` holds the guard until its block ends.
/// `let depth = runtime.inner.queue.lock().expect("...").queued_total;` does not -- that guard is
/// a temporary dropped at the semicolon. Treating the second shape as held would invent nestings
/// that do not exist and fail this test against code that is correct, so the tail after the
/// `.expect(...)` has to be exactly the semicolon.
fn bound_guard_name(trimmed: &str) -> Option<String> {
    let rest = trimmed.strip_prefix("let ")?;
    let rest = rest.strip_prefix("mut ").unwrap_or(rest);
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() || name == "_" {
        return None;
    }
    let (_, tail) = trimmed.split_once(".lock().expect(")?;
    // `.expect("...")` and nothing after it but the semicolon.
    let message_end = tail.strip_prefix('"')?.find('"')? + 2;
    if tail[message_end..].trim() != ");" {
        return None;
    }
    Some(name)
}
