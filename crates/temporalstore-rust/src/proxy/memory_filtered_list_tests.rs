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
    let reads = Arc::new(Reads::default());
    let seen = Arc::clone(&reads);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            let answer = |command: Command| {
                let response = match command {
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
                            value: (!matches!(mode, Index::Unknown))
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
                                    Some(record(tenant, ordinal).encode())
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
                ExecuteResponse {
                    status: Status::ok(),
                    response,
                }
            };
            match request.path.as_str() {
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
