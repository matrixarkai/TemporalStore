// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::http::{json_response, parse_json, serve_listener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const RECORDS_PER_POSTING: usize = 64;
const TENANT: u64 = 42;

#[test]
fn identity_deletion_packs_same_key_tombstones_into_one_command() {
    let records = (0..4_096).map(record).collect::<Vec<_>>();
    let Command::HashMultiSet { key, entries } = memory_tombstone_write(TENANT, records.iter())
    else {
        panic!("tombstones must use one multi-field write");
    };
    assert_eq!(key, memory_tombstone_key(TENANT));
    assert_eq!(entries.len(), records.len());
    for ((id, value), record) in entries.iter().zip(&records) {
        assert_eq!(id, &record.id);
        assert_eq!(value, b"1");
    }
}

fn record(ordinal: usize) -> MemoryRecord {
    let id = format!("{TENANT:016x}-{ordinal:016x}");
    MemoryRecord {
        context_ref: id.clone(),
        id,
        memory: "x".repeat(512),
        metadata: Value::Null,
        tenant_id: "tenant".into(),
        account_id: "account".into(),
        user_id: "alice".into(),
        agent_id: "agent".into(),
        run_id: "session".into(),
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

fn backend(posting_count: usize) -> (ProxyService, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let max_fields = Arc::new(AtomicUsize::new(0));
    let max_bytes = Arc::new(AtomicUsize::new(0));
    let fields_seen = Arc::clone(&max_fields);
    let bytes_seen = Arc::clone(&max_bytes);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        serve_listener(listener, move |request| {
            let answer = |command: Command| match command {
                Command::SetScan { after, limit, .. } => CommandResponse::Members {
                    members: (0..posting_count)
                        .map(|posting| format!("p{posting:08}").into_bytes())
                        .filter(|posting| posting.as_slice() > after.as_slice())
                        .take(limit)
                        .collect(),
                },
                Command::HashMultiGet { key, fields }
                    if key == memory_identity_postings_key(TENANT) =>
                {
                    CommandResponse::Values {
                        values: fields
                            .into_iter()
                            .map(|field| {
                                let posting = field[1..].parse::<usize>().unwrap();
                                let records = (posting * RECORDS_PER_POSTING
                                    ..(posting + 1) * RECORDS_PER_POSTING)
                                    .map(record)
                                    .collect::<Vec<_>>();
                                Some(memory_identity_posting(&records))
                            })
                            .collect(),
                    }
                }
                Command::HashMultiGet { key, fields } => {
                    let catalog = key == memory_catalog_key(TENANT);
                    assert!(catalog || key == memory_tombstone_key(TENANT));
                    let values = fields
                        .into_iter()
                        .map(|id| {
                            let ordinal =
                                usize::from_str_radix(id.split_once('-').unwrap().1, 16).unwrap();
                            if catalog {
                                Some(record(ordinal).encode())
                            } else {
                                (ordinal % 17 == 0).then(|| vec![1])
                            }
                        })
                        .collect::<Vec<_>>();
                    if catalog {
                        fields_seen.fetch_max(values.len(), Ordering::Relaxed);
                        bytes_seen.fetch_max(
                            values.iter().flatten().map(Vec::len).sum(),
                            Ordering::Relaxed,
                        );
                    }
                    CommandResponse::Values { values }
                }
                command => panic!("unexpected command: {command:?}"),
            };
            match request.path.as_str() {
                "/execute" => {
                    let request: ExecuteRequest = parse_json(&request.body).unwrap();
                    json_response(
                        200,
                        &ExecuteResponse {
                            status: Status::ok(),
                            response: answer(request.command),
                        },
                    )
                }
                "/batch_execute" => {
                    let request: BatchExecuteRequest = parse_json(&request.body).unwrap();
                    json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::ok(),
                            responses: request
                                .commands
                                .into_iter()
                                .map(|command| ExecuteResponse {
                                    status: Status::ok(),
                                    response: answer(command),
                                })
                                .collect(),
                        },
                    )
                }
                _ => panic!("unexpected path: {}", request.path),
            }
        })
        .unwrap();
    });
    let proxy = ProxyService::new(ProxyOptions {
        io_timeout_ms: 30_000,
        ..ProxyOptions::default()
    });
    proxy
        .client()
        .insert_cached_route_for_test(proxy.context_shard_id(TENANT), addr);
    (proxy, max_fields, max_bytes)
}

#[test]
fn identity_page_hydration_is_bounded_as_the_corpus_grows() {
    for posting_count in [16, 64] {
        let (proxy, max_fields, max_bytes) = backend(posting_count);
        let mut after = Vec::new();
        let mut seen = HashSet::new();
        loop {
            let (next, rows) =
                proxy.memory_identity_page(TENANT, b"ualice", &after, MEMORY_CATALOG_PAGE_SIZE);
            if next.is_empty() {
                break;
            }
            assert!(next > after);
            after = next;
            assert!(rows.len() <= MEMORY_IDENTITY_POSTINGS_PER_PAGE * RECORDS_PER_POSTING);
            for (_, row) in rows {
                assert!(seen.insert(row.id));
            }
        }
        let expected = (0..posting_count * RECORDS_PER_POSTING)
            .filter(|ordinal| ordinal % 17 != 0)
            .map(|ordinal| record(ordinal).id)
            .collect::<HashSet<_>>();
        assert_eq!(seen, expected);
        assert_eq!(
            max_fields.load(Ordering::Relaxed),
            MEMORY_IDENTITY_POSTINGS_PER_PAGE * RECORDS_PER_POSTING
        );
        let bounded_bytes = max_bytes.load(Ordering::Relaxed);
        println!(
            "postings={posting_count} bounded_fields={} bounded_catalog_bytes={bounded_bytes}",
            max_fields.load(Ordering::Relaxed)
        );

        max_fields.store(0, Ordering::Relaxed);
        max_bytes.store(0, Ordering::Relaxed);
        let (_, rows) =
            proxy.memory_identity_postings_page(TENANT, b"ualice", b"", MEMORY_CATALOG_PAGE_SIZE);
        assert_eq!(
            rows.into_iter()
                .map(|(_, row)| row.id)
                .collect::<HashSet<_>>(),
            expected
        );
        assert_eq!(
            max_fields.load(Ordering::Relaxed),
            posting_count * RECORDS_PER_POSTING
        );
        assert_eq!(
            max_bytes.load(Ordering::Relaxed),
            bounded_bytes * (posting_count / MEMORY_IDENTITY_POSTINGS_PER_PAGE)
        );
        println!(
            "postings={posting_count} unbounded_fields={} unbounded_catalog_bytes={}",
            max_fields.load(Ordering::Relaxed),
            max_bytes.load(Ordering::Relaxed)
        );
    }
}
