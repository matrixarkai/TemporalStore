// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, parse_json, serve_listener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum Index {
    Normal,
    Unknown,
    Anonymous,
    Lookup,
    BadLookup,
    StaleLookup,
    RetryLookup,
    Oversized,
    Missing,
    Malformed,
    Duplicate,
    Stalled,
    Tombstone,
    DeadPrefix,
}

#[derive(Default)]
struct Reads {
    catalog: AtomicUsize,
    bytes: AtomicUsize,
    max_page: AtomicUsize,
    counts: AtomicUsize,
    postings: AtomicUsize,
    scoped_searches: AtomicUsize,
    tenant_searches: AtomicUsize,
    lookup_fields: AtomicUsize,
    lookup_writes: AtomicUsize,
    max_lookup_write: AtomicUsize,
    lookup_write_calls: AtomicUsize,
    retry_payload: std::sync::Mutex<Option<Vec<(String, Vec<u8>)>>>,
}

fn scope() -> MemoryScope {
    MemoryScope {
        tenant_id: "tenant".into(),
        account_id: "account".into(),
        ..MemoryScope::default()
    }
}

fn record(tenant: u64, ordinal: usize) -> MemoryRecord {
    let id = format!("{tenant:016x}-{ordinal:016x}");
    MemoryRecord {
        id: id.clone(),
        context_ref: id,
        memory: "m".repeat(512),
        metadata: Value::Null,
        tenant_id: "tenant".into(),
        account_id: "account".into(),
        user_id: if ordinal < 8 { "alice" } else { "bob" }.into(),
        agent_id: "agent".into(),
        run_id: if ordinal < 8 { "run-a" } else { "run-b" }.into(),
        created_at_ms: 1,
        updated_at_ms: (ordinal / 2 + 1) as u64,
    }
}

fn fixture(total: usize, mode: Index) -> (ProxyService, Arc<Reads>) {
    let tenant = context::context_tenant_hash(&scope().context_scope(&MemoryIdentity::default()));
    let mut order = (0..total)
        .map(|ordinal| {
            let row = record(tenant, ordinal);
            memory_order_member(row.updated_at_ms, &row.id)
        })
        .collect::<Vec<_>>();
    order.sort();
    let node_ids = (8..12)
        .map(|ordinal| {
            let row = record(tenant, ordinal);
            (
                format!("{:016x}", memory_record_node_hash(tenant, &row)),
                memory_identity_record_member(&row.id),
            )
        })
        .collect::<HashMap<_, _>>();
    let reads = Arc::new(Reads::default());
    let seen = Arc::clone(&reads);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            let answer = |command: Command| {
                let mut status = Status::ok();
                let response = match command {
                    Command::StringGet { key } if key == memory_order_ready_key(tenant) => {
                        CommandResponse::Bytes {
                            value: Some(b"1".to_vec()),
                        }
                    }
                    Command::HashMultiSet { key, entries }
                        if key == memory_node_lookup_key(tenant) =>
                    {
                        assert!(entries
                            .iter()
                            .all(|(field, value)| field.len() == 16 && value.len() == 8));
                        let call = seen.lookup_write_calls.fetch_add(1, Ordering::Relaxed);
                        if matches!(mode, Index::RetryLookup) {
                            if call == 0 {
                                *seen.retry_payload.lock().unwrap() = Some(entries.clone());
                                status =
                                    Status::error("write_outcome_unknown", "injected after apply");
                            } else if call == 1 {
                                assert_eq!(
                                    seen.retry_payload.lock().unwrap().as_ref(),
                                    Some(&entries)
                                );
                            }
                        }
                        seen.lookup_writes
                            .fetch_add(entries.len(), Ordering::Relaxed);
                        seen.max_lookup_write
                            .fetch_max(entries.len(), Ordering::Relaxed);
                        CommandResponse::Integer {
                            value: entries.len() as i64,
                        }
                    }
                    Command::HashMultiGet { key, fields }
                        if key == memory_node_lookup_key(tenant) =>
                    {
                        seen.lookup_fields
                            .fetch_add(fields.len(), Ordering::Relaxed);
                        CommandResponse::Values {
                            values: fields
                                .iter()
                                .map(|field| {
                                    if matches!(mode, Index::BadLookup) {
                                        Some(vec![1])
                                    } else if matches!(mode, Index::Lookup | Index::StaleLookup) {
                                        node_ids.get(field).cloned()
                                    } else {
                                        None
                                    }
                                })
                                .collect(),
                        }
                    }
                    Command::ZSetScore { member, .. } => {
                        seen.counts.fetch_add(1, Ordering::Relaxed);
                        let count = match member.as_slice() {
                            MEMORY_TOTAL_MEMBER if matches!(mode, Index::DeadPrefix) => 8,
                            MEMORY_TOTAL_MEMBER => {
                                total - usize::from(matches!(mode, Index::Tombstone))
                            }
                            b"ualice" | b"rrun-a" => {
                                8 - usize::from(matches!(mode, Index::Tombstone))
                            }
                            b"ubob" | b"rrun-b" if matches!(mode, Index::DeadPrefix) => 0,
                            b"ubob" | b"rrun-b" => total - 8,
                            _ => total,
                        };
                        CommandResponse::Bytes {
                            value: (!matches!(mode, Index::Unknown | Index::Anonymous))
                                .then(|| count.to_string().into_bytes()),
                        }
                    }
                    Command::SetScan { key, after, limit } => {
                        let members = if key == memory_order_key(tenant) {
                            order
                                .iter()
                                .filter(|member| member.as_slice() > after.as_slice())
                                .take(limit)
                                .cloned()
                                .collect()
                        } else {
                            seen.postings.fetch_add(1, Ordering::Relaxed);
                            assert!(key.starts_with("context:memory-by-identity:"));
                            let posts = if matches!(mode, Index::Duplicate) {
                                vec![b"p0".to_vec(), b"p1".to_vec()]
                            } else {
                                vec![b"p0".to_vec()]
                            };
                            posts
                                .into_iter()
                                .filter(|member| matches!(mode, Index::Stalled) || member > &after)
                                .take(limit)
                                .collect()
                        };
                        CommandResponse::Members { members }
                    }
                    Command::HashMultiGet { key, fields }
                        if key == memory_identity_postings_key(tenant) =>
                    {
                        let count = match mode {
                            Index::Oversized => total,
                            Index::Missing => 4,
                            _ => 8,
                        };
                        let bytes = if matches!(mode, Index::Malformed) {
                            vec![7]
                        } else {
                            (0..count)
                                .flat_map(|ordinal| (ordinal as u64).to_be_bytes())
                                .collect::<Vec<_>>()
                        };
                        CommandResponse::Values {
                            values: fields.iter().map(|_| Some(bytes.clone())).collect(),
                        }
                    }
                    Command::HashMultiGet { key, fields } => {
                        let catalog = key == memory_catalog_key(tenant);
                        assert!(catalog || key == memory_tombstone_key(tenant));
                        let values = fields
                            .iter()
                            .map(|id| {
                                let ordinal =
                                    usize::from_str_radix(id.split_once('-').unwrap().1, 16)
                                        .unwrap();
                                if catalog {
                                    let mut row = record(tenant, ordinal);
                                    if ordinal == 8 && matches!(mode, Index::StaleLookup) {
                                        row.context_ref = memory_context_ref(&row.id, 10000);
                                    }
                                    Some(row.encode())
                                } else {
                                    ((ordinal == 0 && matches!(mode, Index::Tombstone))
                                        || (ordinal >= 8 && matches!(mode, Index::DeadPrefix)))
                                    .then(|| vec![1])
                                }
                            })
                            .collect::<Vec<_>>();
                        if catalog {
                            seen.catalog.fetch_add(fields.len(), Ordering::Relaxed);
                            seen.max_page.fetch_max(fields.len(), Ordering::Relaxed);
                            seen.bytes.fetch_add(
                                values.iter().flatten().map(Vec::len).sum(),
                                Ordering::Relaxed,
                            );
                        }
                        CommandResponse::Values { values }
                    }
                    command => panic!("unexpected command: {command:?}"),
                };
                ExecuteResponse { status, response }
            };
            match request.path.as_str() {
                "/context/retrieve/nodes" | "/context/retrieve" => {
                    use crate::context_workflow::*;
                    let body: ContextRetrieveRequest = parse_json(&request.body).unwrap();
                    let scoped = request.path.ends_with("/nodes");
                    let ordinals = if scoped {
                        seen.scoped_searches.fetch_add(1, Ordering::Relaxed);
                        let expected = (0..8)
                            .map(|ordinal| {
                                context_source_node_hash(
                                    tenant,
                                    ContextSourceKind::Chat,
                                    &record(tenant, ordinal).context_ref,
                                )
                            })
                            .collect::<HashSet<_>>();
                        assert!(body.node_hashes.iter().all(|node| expected.contains(node)));
                        if body.node_hashes.is_empty() {
                            Vec::new()
                        } else {
                            assert_eq!(body.node_hashes.len(), 8);
                            (0..8).rev().collect::<Vec<_>>()
                        }
                    } else {
                        seen.tenant_searches.fetch_add(1, Ordering::Relaxed);
                        assert!(body.node_hashes.is_empty());
                        (8..12).collect::<Vec<_>>()
                    };
                    let blocks = ordinals
                        .into_iter()
                        .map(|ordinal| {
                            let row = record(tenant, ordinal);
                            ContextBlock {
                                record_type: "context_event".into(),
                                uri: "memory".into(),
                                tier: ContextTier::L2,
                                node_hash: context_source_node_hash(
                                    tenant,
                                    ContextSourceKind::Chat,
                                    &row.context_ref,
                                ),
                                event_time_ms: row.updated_at_ms,
                                text: row.memory,
                                estimated_tokens: 128,
                                source_ref: if scoped
                                    || matches!(
                                        mode,
                                        Index::Anonymous
                                            | Index::Lookup
                                            | Index::BadLookup
                                            | Index::StaleLookup
                                    ) {
                                    String::new()
                                } else {
                                    row.context_ref
                                },
                            }
                        })
                        .collect::<Vec<_>>();
                    json_response(
                        200,
                        &ContextRetrieveReport {
                            status: Status::ok(),
                            node_count: blocks.len(),
                            event_count: blocks.len(),
                            blocks,
                            query_understanding_debug: ContextQueryUnderstandingDebug::default(),
                            fanout_plan: ContextFanoutPlanReport::default(),
                            parity: context_pipeline_parity_evidence(),
                        },
                    )
                }
                "/execute" => {
                    let body: ExecuteRequest = parse_json(&request.body).unwrap();
                    json_response(200, &answer(body.command))
                }
                "/batch_execute" => {
                    let body: BatchExecuteRequest = parse_json(&request.body).unwrap();
                    json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::ok(),
                            responses: body.commands.into_iter().map(answer).collect(),
                        },
                    )
                }
                _ => panic!("unexpected path: {}", request.path),
            }
        })
        .unwrap();
    });
    let proxy = ProxyService::new(ProxyOptions {
        context_shard_count: 1,
        route_cache_ttl_ms: 600_000,
        io_timeout_ms: 30_000,
        topology_check_interval_ms: u64::MAX,
        ..ProxyOptions::default()
    });
    proxy
        .client()
        .insert_cached_route_for_test(proxy.context_shard_id(tenant), addr);
    proxy.memory_remember_indexes_ready(tenant);
    (proxy, reads)
}

fn list(proxy: &ProxyService, user: &str, run: &str) -> Value {
    let (code, bytes) = proxy.memory_list(MemoryQueryRequest {
        scope: scope(),
        identity: MemoryIdentity {
            user_id: user.into(),
            run_id: run.into(),
            ..MemoryIdentity::default()
        },
        limit: Some(4),
        ..MemoryQueryRequest::default()
    });
    assert_eq!(code, 200);
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn sparse_memory_lists_hydrate_bounded_candidates_at_two_corpus_sizes() {
    for total in [4_096, 16_384] {
        let (ordered, old) = fixture(total, Index::Unknown);
        let expected = list(&ordered, "alice", "");
        let (indexed, new) = fixture(total, Index::Normal);
        assert_eq!(list(&indexed, "alice", ""), expected);
        assert_eq!(expected["results"].as_array().unwrap().len(), 4);
        assert_eq!(old.catalog.load(Ordering::Relaxed), total);
        assert_eq!(
            new.catalog.load(Ordering::Relaxed),
            MEMORY_CATALOG_PAGE_SIZE + 8
        );
        println!("corpus={total} ordered_catalog_bytes={} indexed_catalog_bytes={} indexed_candidates={}", old.bytes.load(Ordering::Relaxed), new.bytes.load(Ordering::Relaxed), new.catalog.load(Ordering::Relaxed));
    }
}

#[test]
fn dense_and_small_memory_lists_keep_the_first_page_fast_path() {
    for (total, user) in [(4_096, "bob"), (64, "alice")] {
        let (proxy, reads) = fixture(total, Index::Normal);
        assert_eq!(list(&proxy, user, "")["memories"], 4);
        assert_eq!(
            reads.catalog.load(Ordering::Relaxed),
            total.min(MEMORY_CATALOG_PAGE_SIZE)
        );
        assert_eq!(reads.counts.load(Ordering::Relaxed), 0);
        assert_eq!(reads.postings.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn small_identity_listing_skips_a_large_deleted_order_prefix() {
    let (proxy, reads) = fixture(4_096, Index::DeadPrefix);
    assert_eq!(list(&proxy, "alice", "")["memories"], 4);
    assert_eq!(
        reads.catalog.load(Ordering::Relaxed),
        MEMORY_CATALOG_PAGE_SIZE + 8
    );
}

#[test]
fn identity_listing_falls_back_for_incomplete_or_oversized_postings() {
    let (oracle, _) = fixture(4_096, Index::Unknown);
    let expected = list(&oracle, "alice", "");
    for mode in [
        Index::Oversized,
        Index::Missing,
        Index::Malformed,
        Index::Stalled,
    ] {
        let (proxy, reads) = fixture(4_096, mode);
        assert_eq!(list(&proxy, "alice", ""), expected);
        assert!(reads.catalog.load(Ordering::Relaxed) <= 4_096 + MEMORY_CATALOG_PAGE_SIZE);
        assert!(reads.max_page.load(Ordering::Relaxed) <= MEMORY_CATALOG_PAGE_SIZE);
    }
}

#[test]
fn identity_listing_preserves_intersections_tombstones_and_unique_order() {
    for mode in [Index::Duplicate, Index::Tombstone] {
        let (proxy, reads) = fixture(4_096, mode);
        let result = list(&proxy, "alice", "");
        let (oracle, _) = fixture(4_096, Index::Unknown);
        assert_eq!(result, list(&oracle, "alice", ""));
        let rows = result["results"].as_array().unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row["id"].as_str().unwrap())
                .collect::<HashSet<_>>()
                .len(),
            4
        );
        assert!(reads.catalog.load(Ordering::Relaxed) < 600);
    }
    let (proxy, reads) = fixture(4_096, Index::Normal);
    assert_eq!(list(&proxy, "alice", "run-b")["memories"], 0);
    assert_eq!(
        reads.catalog.load(Ordering::Relaxed),
        MEMORY_CATALOG_PAGE_SIZE + 8
    );
}

#[test]
fn list_ranking_retains_only_the_limit_and_prefers_newer_versions() {
    let mut ranked = BTreeMap::new();
    let mut retained = HashMap::new();
    for ordinal in 0..1_000 {
        memory_list_keep_best(&mut ranked, &mut retained, record(42, ordinal), 4);
        assert_eq!(ranked.len(), retained.len());
        assert!(ranked.len() <= 4);
    }
    let mut replacement = record(42, 999);
    replacement.updated_at_ms = 10_000;
    memory_list_keep_best(&mut ranked, &mut retained, replacement, 4);
    assert_eq!(ranked.len(), 4);
    assert_eq!(retained.len(), 4);
    assert_eq!(
        ranked
            .values()
            .map(|record| &record.id)
            .collect::<HashSet<_>>()
            .len(),
        4
    );
    assert_eq!(ranked.first_key_value().unwrap().1.updated_at_ms, 10_000);
}

#[test]
fn selective_search_uses_identity_nodes_before_retrieval_and_reuses_catalog_rows() {
    for total in [4096, 16384] {
        let (proxy, reads) = fixture(total, Index::Normal);
        proxy.inner.memory_indexes_ready.write().unwrap().clear();
        let (status, body) = proxy.memory_search(MemoryQueryRequest {
            scope: scope(),
            identity: MemoryIdentity {
                user_id: "alice".into(),
                ..MemoryIdentity::default()
            },
            query: "preference".into(),
            limit: Some(4),
            ..MemoryQueryRequest::default()
        });
        assert_eq!(status, 200);
        let result: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["results"].as_array().unwrap().len(), 4);
        assert_eq!(reads.scoped_searches.load(Ordering::Relaxed), 1);
        assert_eq!(reads.tenant_searches.load(Ordering::Relaxed), 0);
        assert_eq!(reads.catalog.load(Ordering::Relaxed), 8);
        assert!(result["results"].as_array().unwrap().iter().all(|row| {
            usize::from_str_radix(row["id"].as_str().unwrap().split_once('-').unwrap().1, 16)
                .unwrap()
                < 8
        }));
    }
}

#[test]
fn empty_identity_intersection_stays_empty_and_missing_counts_keep_fallback() {
    for (mode, run, expected_scoped, expected_fields) in [
        (Index::Normal, "run-b", 1, 8),
        (Index::Unknown, "", 0, 4),
        (Index::Anonymous, "", 0, 0),
    ] {
        let (proxy, reads) = fixture(4096, mode);
        let (status, body) = proxy.memory_search(MemoryQueryRequest {
            scope: scope(),
            identity: MemoryIdentity {
                user_id: "alice".into(),
                run_id: run.into(),
                ..MemoryIdentity::default()
            },
            query: "preference".into(),
            limit: Some(4),
            ..MemoryQueryRequest::default()
        });
        assert_eq!(status, 200);
        let result: Value = serde_json::from_slice(&body).unwrap();
        assert!(result["results"].as_array().unwrap().is_empty());
        assert_eq!(
            reads.scoped_searches.load(Ordering::Relaxed),
            expected_scoped
        );
        assert_eq!(reads.catalog.load(Ordering::Relaxed), expected_fields);
    }
}

#[test]
fn large_identity_and_unscoped_searches_hydrate_only_the_selected_catalog_rows() {
    for total in [8192, 32768] {
        for user in ["", "bob"] {
            let (proxy, reads) = fixture(total, Index::Lookup);
            let (status, body) = proxy.memory_search(MemoryQueryRequest {
                scope: scope(),
                identity: MemoryIdentity {
                    user_id: user.into(),
                    ..MemoryIdentity::default()
                },
                query: "topic".into(),
                limit: Some(4),
                ..MemoryQueryRequest::default()
            });
            assert_eq!(status, 200);
            let result: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(result["results"].as_array().unwrap().len(), 4);
            assert!(result["results"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| !row["id"].as_str().unwrap().is_empty()));
            assert_eq!(reads.lookup_fields.load(Ordering::Relaxed), 4);
            assert_eq!(reads.catalog.load(Ordering::Relaxed), 4);
            assert_eq!(reads.scoped_searches.load(Ordering::Relaxed), 0);
        }
    }
}

#[test]
fn corrupt_lookup_values_fail_closed_and_old_revision_nodes_do_not_return_new_memory() {
    for (mode, status, count) in [(Index::BadLookup, 502, 0), (Index::StaleLookup, 200, 3)] {
        let (proxy, _) = fixture(8192, mode);
        let (code, body) = proxy.memory_search(MemoryQueryRequest {
            scope: scope(),
            identity: MemoryIdentity {
                user_id: "bob".into(),
                ..MemoryIdentity::default()
            },
            query: "topic".into(),
            limit: Some(4),
            ..MemoryQueryRequest::default()
        });
        assert_eq!(code, status);
        let response: Value = serde_json::from_slice(&body).unwrap();
        if code == 200 {
            assert_eq!(response["results"].as_array().unwrap().len(), count);
        } else {
            assert_eq!(response["code"], "invalid_memory_node_lookup");
        }
    }
}

#[test]
fn node_lookup_backfill_is_bounded_and_cursor_retries_are_idempotent() {
    for total in [8192, 32768] {
        let (proxy, reads) = fixture(total, Index::Normal);
        let request = |after: &str| {
            serde_json::from_value(json!({
                "scope":{"tenant_id":"tenant","account_id":"account"},"after":after,
            }))
            .unwrap()
        };
        let (status, body) = proxy.memory_rebuild_node_lookup(request(""));
        assert_eq!(status, 200);
        let first: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(first["scanned"], MEMORY_CATALOG_PAGE_SIZE);
        assert_eq!(first["indexed"], MEMORY_CATALOG_PAGE_SIZE);
        assert_eq!(first["complete"], false);
        let cursor = first["next"].as_str().unwrap();
        assert!(!cursor.is_empty());
        let second = proxy.memory_rebuild_node_lookup(request(cursor));
        let retry = proxy.memory_rebuild_node_lookup(request(cursor));
        assert_eq!(second, retry);
        assert_eq!(
            reads.lookup_writes.load(Ordering::Relaxed),
            MEMORY_CATALOG_PAGE_SIZE * 3
        );
        assert_eq!(
            reads.max_lookup_write.load(Ordering::Relaxed),
            MEMORY_UPDATE_PAGE_SIZE
        );
        assert_eq!(
            reads.max_page.load(Ordering::Relaxed),
            MEMORY_CATALOG_PAGE_SIZE
        );
    }
}

#[test]
fn node_lookup_payload_is_compact_and_tracks_current_revisions() {
    let mut row = record(42, 1);
    let first_node = memory_record_node_hash(42, &row);
    let Command::HashMultiSet { entries, .. } = memory_node_lookup_write(42, std::iter::once(&row))
    else {
        panic!("expected packed lookup");
    };
    assert_eq!(entries[0].0.len() + entries[0].1.len(), 24);
    assert_eq!(
        memory_id_from_identity_record_member(42, &entries[0].1),
        Some(row.id.clone())
    );
    row.context_ref = memory_context_ref(&row.id, 10000);
    assert_ne!(memory_record_node_hash(42, &row), first_node);
    assert!(matches!(
        memory_node_lookup_remove(42, first_node),
        Command::HashDelete { .. }
    ));
}

#[test]
fn node_lookup_backfill_retries_only_the_identical_idempotent_write() {
    let (proxy, reads) = fixture(8192, Index::RetryLookup);
    let request = serde_json::from_value(json!({
        "scope":{"tenant_id":"tenant","account_id":"account"},
    }))
    .unwrap();
    let (status, body) = proxy.memory_rebuild_node_lookup(request);
    assert_eq!(status, 200);
    let report: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(report["indexed"], MEMORY_CATALOG_PAGE_SIZE);
    assert_eq!(report["write_retries"], 1);
    assert_eq!(reads.lookup_write_calls.load(Ordering::Relaxed), 5);
    assert_eq!(
        reads.lookup_writes.load(Ordering::Relaxed),
        MEMORY_CATALOG_PAGE_SIZE + MEMORY_UPDATE_PAGE_SIZE
    );
}

#[test]
fn memory_delete_accepts_engine_empty_success_and_removes_node_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(crate::engine::TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    let proxy = ProxyService::new(ProxyOptions {
        context_shard_count: 1,
        route_cache_ttl_ms: 600_000,
        topology_check_interval_ms: u64::MAX,
        ..ProxyOptions::default()
    });
    let tenant = 42;
    let shard_id = proxy.context_shard_id(tenant);
    engine.load_shard(shard_id);
    let row = record(tenant, 1);
    for command in [
        memory_catalog_write(tenant, std::iter::once(&row)),
        memory_node_lookup_write(tenant, std::iter::once(&row)),
    ] {
        assert!(
            engine
                .execute(ExecuteRequest { shard_id, command })
                .status
                .ok
        );
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serving_engine = Arc::clone(&engine);
    std::thread::spawn(move || {
        let _dir = dir;
        serve_listener(listener, move |request| match request.path.as_str() {
            "/execute" => json_response(
                200,
                &serving_engine.execute(parse_json(&request.body).unwrap()),
            ),
            "/batch_execute" => json_response(
                200,
                &serving_engine.batch_execute(parse_json(&request.body).unwrap()),
            ),
            _ => panic!("unexpected path: {}", request.path),
        })
        .unwrap();
    });
    proxy.client().insert_cached_route_for_test(shard_id, addr);
    proxy.memory_remember_indexes_ready(tenant);
    let (status, body) = proxy.memory_delete(&row.id);
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let response: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["deleted"], true);
    assert!(proxy.memory_read_one(tenant, &row.id).is_none());
    let lookup = engine.execute(ExecuteRequest {
        shard_id,
        command: Command::HashGet {
            key: memory_node_lookup_key(tenant),
            field: format!("{:016x}", memory_record_node_hash(tenant, &row)),
        },
    });
    assert!(lookup.status.ok);
    assert!(matches!(
        lookup.response,
        CommandResponse::Bytes { value: None }
    ));
    let (status, _) = proxy.memory_delete(&row.id);
    assert_eq!(status, 404);
}
