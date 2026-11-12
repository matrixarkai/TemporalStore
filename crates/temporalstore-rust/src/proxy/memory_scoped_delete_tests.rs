// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, parse_json, serve_listener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    InsertAfterCounts,
    CountChild,
    CountMissing,
    CountShape,
    CountNumber(&'static [u8]),
    Scan,
    Posting,
    Catalog,
    Tombstone,
}

#[derive(Default)]
struct Seen {
    injected: AtomicUsize,
    writes: AtomicUsize,
    scope_clears: AtomicUsize,
    max_scan_limit: AtomicUsize,
    max_hydration_width: AtomicUsize,
    hydration_batches: AtomicUsize,
}

struct Fixture {
    _dir: tempfile::TempDir,
    proxy: ProxyService,
    engine: Arc<crate::engine::TemporalEngine>,
    seen: Arc<Seen>,
    tenant: u64,
    shard: u64,
    alice: MemoryRecord,
    bob: MemoryRecord,
}

fn record_commands(tenant: u64, record: &MemoryRecord) -> Vec<Command> {
    let mut commands = vec![
        memory_catalog_write(tenant, std::iter::once(record)),
        memory_node_lookup_write(tenant, std::iter::once(record)),
        Command::SetAdd {
            key: memory_order_key(tenant),
            member: memory_order_member(1, &record.id),
        },
        Command::HashSet {
            key: memory_identity_postings_key(tenant),
            field: record.id.clone(),
            value: memory_identity_posting(std::slice::from_ref(record)),
        },
        Command::ZSetCountAdjust {
            key: memory_identity_key(tenant),
            member: MEMORY_TOTAL_MEMBER.to_vec(),
            increment: 1,
        },
    ];
    for member in memory_identity_members(record) {
        commands.push(Command::ZSetCountAdjust {
            key: memory_identity_key(tenant),
            member: member.clone(),
            increment: 1,
        });
        commands.push(Command::SetAdd {
            key: memory_identity_records_key(tenant, &member),
            member: record.id.as_bytes().to_vec(),
        });
    }
    commands
}

fn fixture(fault: Fault, initial: usize) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(crate::engine::TemporalEngine::with_local_dirs(
        16 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    let proxy = ProxyService::new(ProxyOptions {
        context_shard_count: 1,
        route_cache_ttl_ms: 600_000,
        // A deadlock ceiling for debug-engine page writes, not a serving latency target.
        io_timeout_ms: 30_000,
        topology_check_interval_ms: u64::MAX,
        ..Default::default()
    });
    let scope = MemoryScope {
        account_id: "test".into(),
        tenant_id: "scoped-delete".into(),
        ..Default::default()
    };
    let tenant = context::context_tenant_hash(&scope.context_scope(&MemoryIdentity::default()));
    let alice = MemoryRecord {
        id: format!("{tenant:016x}-0000000000000001"),
        context_ref: String::new(),
        memory: "Alice memory".into(),
        metadata: Value::Null,
        tenant_id: scope.tenant_id.clone(),
        account_id: scope.account_id.clone(),
        user_id: "alice".into(),
        agent_id: String::new(),
        run_id: String::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    let bob = MemoryRecord {
        id: format!("{tenant:016x}-0000000000000002"),
        user_id: "bob".into(),
        memory: "Bob memory".into(),
        ..alice.clone()
    };
    let shard = proxy.context_shard_id(tenant);
    engine.load_shard(shard);
    let mut commands = vec![
        Command::StringSet {
            key: memory_order_ready_key(tenant),
            value: b"1".to_vec(),
        },
        Command::StringSet {
            key: memory_identity_ready_key(tenant),
            value: MEMORY_IDENTITY_INDEX_VERSION.to_vec(),
        },
    ];
    for record in [&alice, &bob].into_iter().take(initial) {
        commands.extend(record_commands(tenant, record));
    }
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: shard,
        commands,
    });
    assert!(response.status.ok && response.responses.iter().all(|row| row.status.ok));
    proxy.memory_remember_indexes_ready(tenant);
    let seen = Arc::new(Seen::default());
    let observed = Arc::clone(&seen);
    let serving = Arc::clone(&engine);
    let inserted = bob.clone();
    let alice_member = memory_identity_member(b'u', "alice").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        serve_listener(listener, move |request| match request.path.as_str() {
            "/batch_execute" => {
                let batch: BatchExecuteRequest = parse_json(&request.body).unwrap();
                let counts = batch.commands.iter().all(|command| matches!(command, Command::ZSetScore { .. }));
                let hydration = batch.commands.iter().any(|command| matches!(command,
                    Command::HashMultiGet { key, .. } if key == &memory_catalog_key(tenant)));
                if hydration {
                    observed.hydration_batches.fetch_add(1, Ordering::Relaxed);
                    for command in &batch.commands {
                        if let Command::HashMultiGet { key, fields } = command {
                            if key == &memory_catalog_key(tenant) {
                                observed.max_hydration_width.fetch_max(fields.len(), Ordering::Relaxed);
                            }
                        }
                    }
                }
                let read = batch.commands.iter().all(|command| matches!(command,
                    Command::ZSetScore { .. } | Command::HashMultiGet { .. } | Command::HashGetAll { .. }));
                if !read { observed.writes.fetch_add(1, Ordering::Relaxed); }
                if batch.commands.iter().any(|command| matches!(command,
                    Command::StringDelete { key } if key == &memory_catalog_key(tenant))) {
                    observed.scope_clears.fetch_add(1, Ordering::Relaxed);
                }
                let mut result = serving.batch_execute(batch);
                if counts {
                    if matches!(fault, Fault::InsertAfterCounts)
                        && observed.injected.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                        // The response retains the old counts; another user commits before deletion.
                        let response = serving.batch_execute(BatchExecuteRequest {
                            shard_id: shard, commands: record_commands(tenant, &inserted),
                        });
                        assert!(response.status.ok && response.responses.iter().all(|row| row.status.ok));
                    }
                    match fault {
                        Fault::CountChild => { result.responses[0].status = Status::error("forced_read_error", "counter read failed"); },
                        Fault::CountMissing => { result.responses.pop(); },
                        Fault::CountShape => { result.responses[0].response = CommandResponse::Empty; },
                        Fault::CountNumber(value) => { result.responses[0].response = CommandResponse::Bytes { value: Some(value.to_vec()) }; },
                        _ => return json_response(200, &result),
                    }
                    observed.injected.fetch_add(1, Ordering::Relaxed);
                } else if hydration && matches!(fault, Fault::Catalog | Fault::Tombstone) {
                    let index = if matches!(fault, Fault::Catalog) { 0 } else { 1 };
                    result.responses[index].status = Status::error("forced_read_error", "record read failed");
                    result.responses[index].response = CommandResponse::Empty;
                    observed.injected.fetch_add(1, Ordering::Relaxed);
                }
                json_response(200, &result)
            }
            "/execute" => {
                let body: ExecuteRequest = parse_json(&request.body).unwrap();
                if let Command::SetScan { limit, .. } = &body.command {
                    observed.max_scan_limit.fetch_max(*limit, Ordering::Relaxed);
                }
                let fail = matches!((&fault, &body.command),
                    (Fault::Scan, Command::SetScan { key, .. }) if key == &memory_identity_records_key(tenant, &alice_member))
                    || matches!((&fault, &body.command),
                    (Fault::Posting, Command::HashMultiGet { key, .. }) if key == &memory_identity_postings_key(tenant));
                if fail {
                    observed.injected.fetch_add(1, Ordering::Relaxed);
                    return json_response(200, &ExecuteResponse {
                        status: Status::error("forced_read_error", "page read failed"), response: CommandResponse::Empty,
                    });
                }
                if !matches!(&body.command, Command::SetScan { .. } | Command::HashMultiGet { .. } | Command::StringGet { .. }) {
                    observed.writes.fetch_add(1, Ordering::Relaxed);
                }
                json_response(200, &serving.execute(body))
            }
            _ => panic!("unexpected route {}", request.path),
        }).unwrap();
    });
    proxy.client().insert_cached_route_for_test(shard, addr);
    Fixture {
        _dir: dir,
        proxy,
        engine,
        seen,
        tenant,
        shard,
        alice,
        bob,
    }
}

impl Fixture {
    fn delete(&self, user: Option<&str>, reset: bool) -> (u16, Value) {
        let mut body = json!({"scope": {"tenant_id": "scoped-delete", "account_id": "test"}});
        if let Some(user) = user {
            body["user_id"] = json!(user);
        }
        let (status, bytes) = self
            .proxy
            .handle_memory_route(&HttpRequest {
                method: "POST".into(),
                path: if reset {
                    "/memories/reset"
                } else {
                    "/memories/delete_all"
                }
                .into(),
                body: serde_json::to_vec(&body).unwrap(),
            })
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    fn visible(&self, record: &MemoryRecord) -> bool {
        self.proxy
            .memory_read_one_checked(self.tenant, &record.id)
            .unwrap()
            .is_some()
    }
}

#[test]
fn scoped_delete_keeps_a_user_inserted_after_the_count_read() {
    let f = fixture(Fault::InsertAfterCounts, 1);
    let (status, body) = f.delete(Some("alice"), false);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["deleted"], 1);
    assert_eq!(f.seen.injected.load(Ordering::Relaxed), 1);
    assert!(
        f.visible(&f.bob),
        "a filtered deletion cleared the concurrent user's memory"
    );
    assert!(!f.visible(&f.alice));
    assert_eq!(f.seen.scope_clears.load(Ordering::Relaxed), 0);
}

#[test]
fn scoped_delete_rejects_failed_or_malformed_counter_reads_before_mutation() {
    for fault in [
        Fault::CountChild,
        Fault::CountMissing,
        Fault::CountShape,
        Fault::CountNumber(b"NaN"),
        Fault::CountNumber(b"-1"),
        Fault::CountNumber(b"0.5"),
    ] {
        let f = fixture(fault, 2);
        let (status, body) = f.delete(Some("alice"), false);
        assert_eq!(status, 502, "{fault:?}: {body}");
        assert!(f.seen.injected.load(Ordering::Relaxed) > 0);
        assert_eq!(f.seen.writes.load(Ordering::Relaxed), 0, "{fault:?}");
        assert!(f.visible(&f.alice) && f.visible(&f.bob));
    }
}

#[test]
fn scoped_delete_propagates_page_and_hydration_read_errors() {
    for fault in [
        Fault::Scan,
        Fault::Posting,
        Fault::Catalog,
        Fault::Tombstone,
    ] {
        let f = fixture(fault, 2);
        let (status, body) = f.delete(Some("alice"), false);
        assert_eq!(status, 502, "{fault:?}: {body}");
        assert_eq!(body["code"], "forced_read_error", "{fault:?}");
        assert!(f.seen.injected.load(Ordering::Relaxed) > 0);
        assert_eq!(f.seen.writes.load(Ordering::Relaxed), 0, "{fault:?}");
    }
}

#[test]
fn scoped_delete_preserves_clean_filter_and_reset_behaviour() {
    let f = fixture(Fault::None, 2);
    assert_eq!(
        f.delete(Some("nobody"), false),
        (200, json!({"deleted": 0}))
    );
    assert!(f.visible(&f.alice) && f.visible(&f.bob));
    assert_eq!(f.delete(Some("alice"), false), (200, json!({"deleted": 1})));
    assert!(!f.visible(&f.alice) && f.visible(&f.bob));
    let empty = fixture(Fault::None, 0);
    assert_eq!(empty.delete(None, true), (200, json!({"deleted": 0})));
    let all = fixture(Fault::None, 2);
    assert_eq!(all.delete(None, true), (200, json!({"deleted": 2})));
    assert!(!all.visible(&all.alice) && !all.visible(&all.bob));
}

#[test]
fn scoped_delete_rebuilds_unready_counts_before_reset_acknowledgment() {
    let f = fixture(Fault::None, 2);
    for key in [
        memory_order_ready_key(f.tenant),
        memory_identity_ready_key(f.tenant),
        memory_identity_key(f.tenant),
    ] {
        assert!(
            f.engine
                .execute(ExecuteRequest {
                    shard_id: f.shard,
                    command: Command::StringDelete { key }
                })
                .status
                .ok
        );
    }
    f.proxy.memory_forget_indexes_ready(f.tenant);
    assert_eq!(f.delete(None, true), (200, json!({"deleted": 2})));
}

#[test]
fn scoped_delete_hydrates_grouped_pages_without_per_record_reads() {
    let f = fixture(Fault::None, 2);
    let records = (0..MEMORY_CATALOG_PAGE_SIZE * 2 + 1)
        .map(|index| MemoryRecord {
            id: format!("{:016x}-{:016x}", f.tenant, index + 10),
            ..f.alice.clone()
        })
        .collect::<Vec<_>>();
    for page in records.chunks(MEMORY_CATALOG_PAGE_SIZE) {
        let mut commands = vec![
            memory_catalog_write(f.tenant, page.iter()),
            memory_node_lookup_write(f.tenant, page.iter()),
            Command::HashSet {
                key: memory_identity_postings_key(f.tenant),
                field: page[0].id.clone(),
                value: memory_identity_posting(page),
            },
            Command::ZSetCountAdjust {
                key: memory_identity_key(f.tenant),
                member: MEMORY_TOTAL_MEMBER.to_vec(),
                increment: page.len() as i64,
            },
        ];
        commands.extend(page.iter().map(|record| Command::SetAdd {
            key: memory_order_key(f.tenant),
            member: memory_order_member(1, &record.id),
        }));
        for member in memory_identity_members(&page[0]) {
            commands.push(Command::SetAdd {
                key: memory_identity_records_key(f.tenant, &member),
                member: page[0].id.as_bytes().to_vec(),
            });
            commands.push(Command::ZSetCountAdjust {
                key: memory_identity_key(f.tenant),
                member,
                increment: page.len() as i64,
            });
        }
        let response = f.engine.batch_execute(BatchExecuteRequest {
            shard_id: f.shard,
            commands,
        });
        assert!(response.status.ok && response.responses.iter().all(|row| row.status.ok));
    }
    assert_eq!(
        f.delete(Some("alice"), false),
        (200, json!({"deleted": records.len() + 1}))
    );
    assert_eq!(
        f.seen.max_scan_limit.load(Ordering::Relaxed),
        MEMORY_IDENTITY_POSTINGS_PER_PAGE
    );
    assert_eq!(
        f.seen.max_hydration_width.load(Ordering::Relaxed),
        MEMORY_CATALOG_PAGE_SIZE
    );
    assert_eq!(f.seen.hydration_batches.load(Ordering::Relaxed), 4);
    assert_eq!(f.seen.scope_clears.load(Ordering::Relaxed), 0);
    assert!(f.visible(&f.bob));
    assert!(!f.visible(&f.alice));
}
