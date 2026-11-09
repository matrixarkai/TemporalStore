// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 1, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;
use crate::{Config, SetConfigRequest};
use crate::types::{
    BatchHashMultiSetRow, BatchHashMultiSetValue, BatchHashTimestampSetRequest,
    BatchHashTimestampSetRow,
};

#[derive(Clone, PartialEq, prost::Message)]
struct TestTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

#[test]
fn compact_batch_write_accepts_writes_and_rejects_reads_before_execution() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);

    let written = runtime.batch_write(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"one".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"two".to_vec(),
            },
        ],
    });
    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 2);
    assert!(written.errors.is_empty());
    assert_eq!(runtime.dirty_shards(), vec![1]);

    let rejected = runtime.batch_write(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![Command::StringGet {
            key: "a".to_string(),
        }],
    });
    assert!(!rejected.status.ok);
    assert_eq!(rejected.status.code, "read_in_write_batch");
    assert_eq!(rejected.applied, 0);
    assert!(rejected.errors.is_empty());

    let stored = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "a".to_string(),
        },
    });
    assert_eq!(
        stored.response,
        CommandResponse::Bytes {
            value: Some(b"one".to_vec())
        }
    );
}

#[test]
fn compact_batch_write_keeps_shared_wal_mirroring() {
    #[derive(Debug, Default)]
    struct RecordingSink {
        commands: Mutex<Vec<Command>>,
    }

    impl SharedWalSink for RecordingSink {
        fn record_write(&self, _shard_id: ShardId, command: &Command) {
            self.commands
                .lock()
                .expect("recording sink lock poisoned")
                .push(command.clone());
        }
    }

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let sink = Arc::new(RecordingSink::default());
    runtime.set_shared_wal_sink(sink.clone());

    let written = runtime.batch_write(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"one".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"two".to_vec(),
            },
        ],
    });

    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 2);
    assert_eq!(
        sink.commands
            .lock()
            .expect("recording sink lock poisoned")
            .len(),
        2
    );
}

#[test]
fn shared_field_hash_batch_keeps_requested_order_duplicates_and_missing_values() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["a".to_string(), "b".to_string()],
        rows: vec![BatchHashMultiSetRow(
            "present".to_string(),
            vec![
                BatchHashMultiSetValue(b"one".to_vec()),
                BatchHashMultiSetValue(b"two".to_vec()),
            ],
        )],
    });
    assert!(written.status.ok, "{written:?}");

    let read = runtime.batch_hash_multi_get(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["present".to_string(), "missing".to_string()],
        fields: vec![
            "b".to_string(),
            "absent".to_string(),
            "a".to_string(),
            "b".to_string(),
        ],
    });

    assert!(read.status.ok, "{read:?}");
    assert_eq!(read.responses.len(), 2);
    assert_eq!(
        read.responses[0].response,
        CommandResponse::Values {
            values: vec![
                Some(b"two".to_vec()),
                None,
                Some(b"one".to_vec()),
                Some(b"two".to_vec()),
            ]
        }
    );
    assert_eq!(
        read.responses[1].response,
        CommandResponse::Values {
            values: vec![None, None, None, None]
        }
    );

    let compact = runtime.batch_hash_multi_get_compact(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["present".to_string(), "missing".to_string()],
        fields: vec![
            "b".to_string(),
            "absent".to_string(),
            "a".to_string(),
            "b".to_string(),
        ],
    });
    assert!(compact.status.ok, "{compact:?}");
    assert!(compact.errors.is_empty());
    assert_eq!(compact.rows.len(), 2);
    assert_eq!(
        compact.rows[0].0,
        vec![
            Some(b"two".to_vec()),
            None,
            Some(b"one".to_vec()),
            Some(b"two".to_vec()),
        ]
    );
    assert_eq!(compact.rows[1].0, vec![None, None, None, None]);

    let direct_request = BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["present".to_string(), "missing".to_string()],
        fields: vec![
            "b".to_string(),
            "absent".to_string(),
            "a".to_string(),
            "b".to_string(),
        ],
    };
    let _ = runtime.batch_hash_multi_get_compact_msgpack(direct_request.clone());
    crate::engine::reset_hash_field_binary_search_count_for_test();
    let _ = runtime.batch_hash_multi_get_compact_msgpack(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: direct_request.keys.clone(),
        fields: Vec::new(),
    });
    let fixed_searches = crate::engine::hash_field_binary_search_count_for_test();
    crate::engine::reset_hash_field_binary_search_count_for_test();
    crate::engine::reset_routing_bucket_derivations();
    crate::engine::reset_index_presence_reset_count_for_test();
    let direct = runtime.batch_hash_multi_get_compact_msgpack(direct_request);
    let decoded = rmp_serde::from_slice::<BatchHashMultiGetResponse>(&direct)
        .expect("direct compact response must retain the established wire shape");
    assert_eq!(decoded, compact);
    assert_eq!(
        crate::engine::hash_field_binary_search_count_for_test(),
        fixed_searches,
        "requested packed fields must not add field-map binary searches"
    );
    assert_eq!(
        crate::engine::routing_bucket_derivations(),
        (2, ("present".len() + "missing".len()) as u64),
        "each entity key must be routed once per compact batch"
    );
    assert_eq!(
        crate::engine::index_presence_reset_count_for_test(),
        1,
        "an absent entity must not clear field-projection scratch"
    );
}

#[test]
fn shared_field_hash_write_uses_regular_write_semantics_and_rejects_bad_widths() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    crate::engine::reset_routing_bucket_derivations();

    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["a".to_string(), "b".to_string()],
        rows: vec![
            BatchHashMultiSetRow(
                "first".to_string(),
                vec![
                    BatchHashMultiSetValue(b"one".to_vec()),
                    BatchHashMultiSetValue(b"two".to_vec()),
                ],
            ),
            BatchHashMultiSetRow(
                "second".to_string(),
                vec![
                    BatchHashMultiSetValue(b"three".to_vec()),
                    BatchHashMultiSetValue(b"four".to_vec()),
                ],
            ),
        ],
    });
    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 2);
    assert_eq!(
        crate::engine::routing_bucket_derivations(),
        (2, ("first".len() + "second".len()) as u64),
        "each shared row must derive its routing bucket once"
    );
    assert_eq!(runtime.dirty_shards(), vec![1]);

    let read = runtime.batch_hash_multi_get(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["first".to_string(), "second".to_string()],
        fields: vec!["b".to_string(), "a".to_string()],
    });
    assert_eq!(
        read.responses[0].response,
        CommandResponse::Values {
            values: vec![Some(b"two".to_vec()), Some(b"one".to_vec())]
        }
    );
    assert_eq!(
        read.responses[1].response,
        CommandResponse::Values {
            values: vec![Some(b"four".to_vec()), Some(b"three".to_vec())]
        }
    );

    let rejected = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["a".to_string(), "b".to_string()],
        rows: vec![BatchHashMultiSetRow(
            "bad".to_string(),
            vec![BatchHashMultiSetValue(b"only-one".to_vec())],
        )],
    });
    assert_eq!(rejected.status.code, "field_value_count_mismatch");
    assert_eq!(rejected.applied, 0);
}

#[test]
fn wide_shared_field_hash_write_routes_each_row_once() {
    const FIELDS: usize = 110;
    const ROWS: usize = 3;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let fields = (0..FIELDS)
        .map(|index| format!("feature-{index:03}"))
        .collect::<Vec<_>>();
    let rows = (0..ROWS)
        .map(|row| {
            BatchHashMultiSetRow(
                format!("entity-{row}"),
                (0..FIELDS)
                    .map(|field| BatchHashMultiSetValue(vec![row as u8, field as u8]))
                    .collect(),
            )
        })
        .collect();

    crate::engine::reset_routing_bucket_derivations();
    crate::engine::hashing::reset_object_id_derivations();
    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields,
        rows,
    });

    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, ROWS);
    assert_eq!(
        crate::engine::routing_bucket_derivations(),
        (ROWS as u64, (ROWS * "entity-0".len()) as u64),
        "routing work must scale with rows rather than shared fields"
    );
    assert_eq!(
        crate::engine::hashing::object_id_derivations(),
        (
            (ROWS * 2) as u64,
            (ROWS * 2 * ("hash".len() + "entity-0".len())) as u64
        ),
        "storage and wire identity work must scale with rows rather than shared fields"
    );
}

#[test]
fn shared_field_hash_write_keeps_large_and_small_fields_readable() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let embedding = vec![7; 8 * 1024];

    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["embedding".to_string(), "score".to_string()],
        rows: vec![BatchHashMultiSetRow(
            "entity".to_string(),
            vec![
                BatchHashMultiSetValue(embedding.clone()),
                BatchHashMultiSetValue(b"0.75".to_vec()),
            ],
        )],
    });
    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 1);

    let read = runtime.batch_hash_multi_get(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["entity".to_string()],
        fields: vec!["score".to_string(), "embedding".to_string()],
    });
    assert_eq!(
        read.responses[0].response,
        CommandResponse::Values {
            values: vec![Some(b"0.75".to_vec()), Some(embedding)]
        }
    );
}

#[test]
#[cfg(feature = "alloc-probe")]
fn warm_packed_projection_does_not_copy_the_frame_from_cache() {
    const READS: usize = 32;
    const EMBEDDING_BYTES: usize = 8 * 1024;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let embedding = vec![7; EMBEDDING_BYTES];
    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["embedding".to_string(), "score".to_string()],
        rows: vec![BatchHashMultiSetRow(
            "entity".to_string(),
            vec![
                BatchHashMultiSetValue(embedding),
                BatchHashMultiSetValue(b"0.75".to_vec()),
            ],
        )],
    });
    assert!(written.status.ok, "{written:?}");

    let request = BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["entity".to_string()],
        fields: vec!["score".to_string(), "embedding".to_string()],
    };
    let warm = runtime.batch_hash_multi_get_compact(request.clone());
    assert!(warm.status.ok, "{warm:?}");

    let span = crate::alloc_probe::ClassSpan::open();
    for _ in 0..READS {
        std::hint::black_box(runtime.batch_hash_multi_get_compact(request.clone()));
    }
    let counted = span.close().expect("the counting allocator must be installed");
    let cache_read = counted
        .classes
        .row(crate::alloc_probe::AllocClass::CacheRead);

    assert!(
        cache_read.alloc_bytes < (READS * 2 * 1024) as u64,
        "warm packed projections allocated {} cache-read bytes across {READS} reads; \
         copying each {EMBEDDING_BYTES}-byte frame would allocate at least {} bytes",
        cache_read.alloc_bytes,
        READS * EMBEDDING_BYTES,
    );
}

#[test]
#[cfg(feature = "alloc-probe")]
fn direct_compact_encoding_does_not_copy_each_feature_value_first() {
    const ROWS: usize = 32;
    const VALUE_BYTES: usize = 8 * 1024;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let value = vec![b'x'; VALUE_BYTES];
    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec!["embedding".to_string()],
        rows: (0..ROWS)
            .map(|index| {
                BatchHashMultiSetRow(
                    format!("entity-{index:02}"),
                    vec![BatchHashMultiSetValue(value.clone())],
                )
            })
            .collect(),
    });
    assert!(written.status.ok, "{written:?}");

    let request = BatchHashMultiGetRequest {
        shard_id: 1,
        keys: (0..ROWS).map(|index| format!("entity-{index:02}")).collect(),
        fields: vec!["embedding".to_string()],
    };
    let warm = runtime.batch_hash_multi_get_compact_msgpack(request.clone());
    assert_eq!(
        rmp_serde::from_slice::<BatchHashMultiGetResponse>(&warm)
            .expect("warm direct response")
            .rows
            .len(),
        ROWS
    );

    let control_probe = crate::alloc_probe::Probe::start();
    let control_response = runtime.batch_hash_multi_get_compact(request.clone());
    let control_body = rmp_serde::to_vec_named(&control_response).expect("encode owned response");
    let control = control_probe.stop();

    let direct_probe = crate::alloc_probe::Probe::start();
    let direct_body = runtime.batch_hash_multi_get_compact_msgpack(request);
    let direct = direct_probe.stop();

    assert_eq!(direct_body, control_body);
    assert!(
        direct.allocs + ((ROWS / 2) as u64) < control.allocs,
        "direct={direct:?} control={control:?}"
    );
    assert!(
        direct.alloc_bytes + ((ROWS * VALUE_BYTES / 2) as u64) < control.alloc_bytes,
        "direct={direct:?} control={control:?}"
    );
    std::hint::black_box((direct_body, control_body));
}

#[test]
#[cfg(feature = "alloc-probe")]
fn absent_compact_rows_do_not_allocate_one_null_vector_per_entity() {
    const ROWS: usize = 512;
    const FIELDS: usize = 32;
    const SAMPLES: usize = 3;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let request = || BatchHashMultiGetRequest {
        shard_id: 1,
        keys: (0..ROWS).map(|index| format!("missing-{index}")).collect(),
        fields: (0..FIELDS).map(|index| format!("feature-{index}")).collect(),
    };

    let mut owned_best = u64::MAX;
    let mut direct_best = u64::MAX;
    let mut owned_body = Vec::new();
    let mut direct_body = Vec::new();
    for _ in 0..SAMPLES {
        let owned_request = request();
        let owned_probe = crate::alloc_probe::Probe::start();
        let owned_response = runtime.batch_hash_multi_get_compact(owned_request);
        let candidate_owned_body =
            rmp_serde::to_vec_named(&owned_response).expect("encode owned absent response");
        let owned_cost = owned_probe.stop();
        if owned_cost.allocs < owned_best {
            owned_best = owned_cost.allocs;
            owned_body = candidate_owned_body;
        }

        let direct_request = request();
        let direct_probe = crate::alloc_probe::Probe::start();
        let candidate_direct_body = runtime.batch_hash_multi_get_compact_msgpack(direct_request);
        let direct_cost = direct_probe.stop();
        if direct_cost.allocs < direct_best {
            direct_best = direct_cost.allocs;
            direct_body = candidate_direct_body;
        }
    }

    assert_eq!(direct_body, owned_body);
    let decoded = rmp_serde::from_slice::<BatchHashMultiGetResponse>(&direct_body)
        .expect("decode absent compact response");
    assert!(decoded.status.ok, "{decoded:?}");
    assert_eq!(decoded.rows.len(), ROWS);
    assert!(decoded
        .rows
        .iter()
        .all(|row| row.0.iter().all(Option::is_none)));
    assert!(
        direct_best + ((ROWS / 2) as u64) < owned_best,
        "direct={direct_best} owned={owned_best}"
    );
    std::hint::black_box((direct_body, owned_body));
}

#[test]
#[cfg(feature = "alloc-probe")]
fn conditional_hash_stale_packed_writes_do_not_copy_the_frame() {
    const WRITES: usize = 32;
    const EMBEDDING_BYTES: usize = 8 * 1024;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let encode_timestamp = |seconds| {
        <TestTimestamp as prost::Message>::encode_to_vec(&TestTimestamp {
            seconds,
            nanos: 0,
        })
    };
    let fields = vec![
        "timestamp".to_string(),
        "embedding".to_string(),
        "score".to_string(),
    ];
    let initial = runtime.batch_hash_timestamp_set(BatchHashTimestampSetRequest {
        shard_id: 1,
        fields: fields.clone(),
        timestamp_field: "timestamp".to_string(),
        ttl_ms: None,
        rows: vec![BatchHashTimestampSetRow(
            "entity".to_string(),
            20,
            0,
            vec![
                BatchHashMultiSetValue(encode_timestamp(20)),
                BatchHashMultiSetValue(vec![7; EMBEDDING_BYTES]),
                BatchHashMultiSetValue(b"0.75".to_vec()),
            ],
        )],
    });
    assert!(initial.status.ok, "{initial:?}");
    assert_eq!(initial.applied, 1);

    let stale = BatchHashTimestampSetRequest {
        shard_id: 1,
        fields,
        timestamp_field: "timestamp".to_string(),
        ttl_ms: None,
        rows: vec![BatchHashTimestampSetRow(
            "entity".to_string(),
            10,
            0,
            vec![
                BatchHashMultiSetValue(encode_timestamp(10)),
                BatchHashMultiSetValue(vec![9; EMBEDDING_BYTES]),
                BatchHashMultiSetValue(b"0.25".to_vec()),
            ],
        )],
    };

    let warm = runtime.batch_hash_timestamp_set(stale.clone());
    assert!(warm.status.ok, "{warm:?}");
    assert_eq!(warm.applied, 0);
    assert_eq!(warm.skipped, 1);

    let span = crate::alloc_probe::ClassSpan::open();
    for _ in 0..WRITES {
        let response = runtime.batch_hash_timestamp_set(stale.clone());
        assert!(response.status.ok, "{response:?}");
        assert_eq!(response.applied, 0);
        assert_eq!(response.skipped, 1);
    }
    let counted = span.close().expect("the counting allocator must be installed");
    let cache_read = counted
        .classes
        .row(crate::alloc_probe::AllocClass::CacheRead);

    assert!(
        cache_read.alloc_bytes < (WRITES * 2 * 1024) as u64,
        "stale packed writes allocated {} cache-read bytes across {WRITES} timestamp checks; \
         copying each {EMBEDDING_BYTES}-byte frame would allocate at least {} bytes",
        cache_read.alloc_bytes,
        WRITES * EMBEDDING_BYTES,
    );
}

#[test]
fn conditional_hash_ttl_is_applied_only_with_a_fresh_row() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let encode_timestamp = |seconds| {
        <TestTimestamp as prost::Message>::encode_to_vec(&TestTimestamp {
            seconds,
            nanos: 0,
        })
    };
    let request = |seconds, ttl_ms| BatchHashTimestampSetRequest {
        shard_id: 1,
        fields: vec!["timestamp".to_string(), "score".to_string()],
        timestamp_field: "timestamp".to_string(),
        ttl_ms: Some(ttl_ms),
        rows: vec![BatchHashTimestampSetRow(
            "entity".to_string(),
            seconds,
            0,
            vec![
                BatchHashMultiSetValue(encode_timestamp(seconds)),
                BatchHashMultiSetValue(seconds.to_string().into_bytes()),
            ],
        )],
    };

    let fresh = runtime.batch_hash_timestamp_set(request(20, 60_000));
    assert!(fresh.status.ok, "{fresh:?}");
    assert_eq!((fresh.applied, fresh.skipped), (1, 0));
    let first_ttl = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonTtl {
            key: "entity".to_string(),
        },
    });
    let CommandResponse::Integer { value: first_ttl } = first_ttl.response else {
        panic!("conditional write did not arm an expiry");
    };

    let stale = runtime.batch_hash_timestamp_set(request(10, 120_000));
    assert!(stale.status.ok, "{stale:?}");
    assert_eq!((stale.applied, stale.skipped), (0, 1));
    let second_ttl = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonTtl {
            key: "entity".to_string(),
        },
    });
    let CommandResponse::Integer { value: second_ttl } = second_ttl.response else {
        panic!("conditional write lost its expiry");
    };
    assert!(first_ttl > 0);
    assert!(second_ttl <= first_ttl);
}

#[test]
fn async_shared_field_hash_write_packs_a_small_projection() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(engine
        .set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        })
        .ok);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);

    let written = runtime.batch_hash_multi_set(BatchHashMultiSetRequest {
        shard_id: 1,
        fields: vec![
            "timestamp".to_string(),
            "f0".to_string(),
            "f1".to_string(),
            "f2".to_string(),
        ],
        rows: vec![BatchHashMultiSetRow(
            "entity".to_string(),
            vec![
                BatchHashMultiSetValue(b"stamp".to_vec()),
                BatchHashMultiSetValue(b"one".to_vec()),
                BatchHashMultiSetValue(b"two".to_vec()),
                BatchHashMultiSetValue(b"three".to_vec()),
            ],
        )],
    });
    assert!(written.status.ok, "{written:?}");
    assert_eq!(written.applied, 1);
    let carried_blocks = runtime
        .engine()
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .into_iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(&line).ok())
        .map(|record| record.staged_blocks.len())
        .sum::<usize>();
    assert_eq!(
        carried_blocks, 1,
        "one shared projection must carry one packed WAL block"
    );
    assert_eq!(
        runtime.engine().block_store().stats().writes,
        0,
        "async storage must defer the packed block rather than append it synchronously"
    );

    let read = runtime.batch_hash_multi_get(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["entity".to_string()],
        fields: vec!["f2".to_string(), "f0".to_string(), "f1".to_string()],
    });
    assert_eq!(
        read.responses[0].response,
        CommandResponse::Values {
            values: vec![
                Some(b"three".to_vec()),
                Some(b"one".to_vec()),
                Some(b"two".to_vec()),
            ]
        }
    );
}

#[test]
fn conditional_hash_batch_keeps_only_newer_rows() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 4);
    let encode_timestamp = |seconds| {
        <TestTimestamp as prost::Message>::encode_to_vec(&TestTimestamp {
            seconds,
            nanos: 0,
        })
    };

    let response = runtime.batch_hash_timestamp_set(BatchHashTimestampSetRequest {
        shard_id: 1,
        fields: vec![
            "timestamp".to_string(),
            "f0".to_string(),
            "f1".to_string(),
            "f2".to_string(),
            "f3".to_string(),
        ],
        timestamp_field: "timestamp".to_string(),
        ttl_ms: None,
        rows: vec![
            BatchHashTimestampSetRow(
                "entity".to_string(),
                20,
                0,
                vec![
                    BatchHashMultiSetValue(encode_timestamp(20)),
                    BatchHashMultiSetValue(b"new-0".to_vec()),
                    BatchHashMultiSetValue(b"new-1".to_vec()),
                    BatchHashMultiSetValue(b"new-2".to_vec()),
                    BatchHashMultiSetValue(b"new-3".to_vec()),
                ],
            ),
            BatchHashTimestampSetRow(
                "entity".to_string(),
                10,
                0,
                vec![
                    BatchHashMultiSetValue(encode_timestamp(10)),
                    BatchHashMultiSetValue(b"old-0".to_vec()),
                    BatchHashMultiSetValue(b"old-1".to_vec()),
                    BatchHashMultiSetValue(b"old-2".to_vec()),
                    BatchHashMultiSetValue(b"old-3".to_vec()),
                ],
            ),
        ],
    });
    assert!(response.status.ok, "{response:?}");
    assert_eq!(response.applied, 1);
    assert_eq!(response.skipped, 1);
    assert!(response.errors.is_empty());
    assert_eq!(
        runtime.engine().block_store().stats().writes,
        1,
        "five accepted fields must append one packed block; the stale row appends nothing"
    );

    runtime.engine().cache().invalidate_shard(1).unwrap();
    let reads_before = runtime.engine().block_store().stats().reads;
    let read = runtime.batch_hash_multi_get(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["entity".to_string()],
        fields: vec![
            "timestamp".to_string(),
            "f0".to_string(),
            "f1".to_string(),
            "f2".to_string(),
            "f3".to_string(),
        ],
    });
    assert_eq!(
        runtime.engine().block_store().stats().reads - reads_before,
        1,
        "five projected fields sharing a packed address must fetch that block once"
    );
    assert_eq!(
        read.responses[0].response,
        CommandResponse::Values {
            values: vec![
                Some(encode_timestamp(20)),
                Some(b"new-0".to_vec()),
                Some(b"new-1".to_vec()),
                Some(b"new-2".to_vec()),
                Some(b"new-3".to_vec()),
            ]
        }
    );
}

#[test]
fn runtime_reports_style_shard_worker_ownership() {
    let runtime = DataNodeRuntime::new(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 4,
            max_queue_depth: 16,
            max_background_queue_depth: 8,
        },
    );

    let stats = runtime.stats();
    assert_eq!(stats.worker_threads, 4);
    assert_eq!(stats.max_queue_depth, 16);
    assert_eq!(stats.max_background_queue_depth, 8);
    assert_eq!(
        runtime.shard_worker_info(7),
        ShardWorkerInfo {
            shard_id: 7,
            worker_index: 3,
            worker_threads: 4,
        }
    );
}

#[test]
fn runtime_lifecycle_report_counts_loaded_readonly_and_preflight() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 19,
                readonly: true,
                load_version: 42,
                local_node_id: Some(3),
            })
            .status
            .ok
    );
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 1);
    assert_eq!(lifecycle.serving_count, 0);
    assert_eq!(lifecycle.readonly_count, 1);
    assert_eq!(lifecycle.queued_count, 0);
    assert_eq!(lifecycle.running_count, 0);
    assert_eq!(lifecycle.unloading_count, 0);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.max_load_version, 42);
    assert_eq!(lifecycle.shards.len(), 1);
    assert_eq!(lifecycle.shards[0].shard_id, 7);
    assert_eq!(lifecycle.shards[0].serving_state, "readonly");
    assert!(lifecycle.shards[0].readonly);
    assert_eq!(lifecycle.shards[0].table_name, "tbl");
    assert_eq!(lifecycle.transitions.len(), 0);

    let preflight = runtime.preflight_report();
    assert_eq!(preflight.lifecycle, lifecycle);
}

#[test]
fn runtime_load_reload_unload_records_lifecycle_transitions() {
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.serving_count, 1);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.transitions.len(), 1);
    assert_eq!(lifecycle.transitions[0].state, "serving");
    assert_eq!(lifecycle.transitions[0].operation, "load");
    assert_eq!(lifecycle.transitions[0].load_version, 42);

    let stale = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "stale".to_string(),
        shard_uri: "local://tbl/stale".to_string(),
        start_routing_bucket: 20,
        end_routing_bucket: 29,
        readonly: true,
        load_version: 41,
        local_node_id: Some(3),
    });
    assert_eq!(stale.status.code, "stale_load_version");
    let failed = runtime.lifecycle_report();
    assert_eq!(failed.failed_count, 1);
    assert_eq!(failed.shards[0].serving_state, "failed");
    assert_eq!(failed.transitions[0].state, "failed");
    assert_eq!(failed.transitions[0].operation, "reload");
    assert_eq!(
        failed.transitions[0].last_status.as_ref().unwrap().code,
        "stale_load_version"
    );

    let reload = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl-new".to_string(),
        shard_uri: "local://tbl/7-new".to_string(),
        start_routing_bucket: 20,
        end_routing_bucket: 29,
        readonly: true,
        load_version: 43,
        local_node_id: Some(4),
    });
    assert!(reload.status.ok, "{reload:?}");
    let readonly = runtime.lifecycle_report();
    assert_eq!(readonly.readonly_count, 1);
    assert_eq!(readonly.failed_count, 0);
    assert_eq!(readonly.transitions[0].state, "readonly");
    assert_eq!(readonly.transitions[0].operation, "reload");
    assert_eq!(readonly.transitions[0].load_version, 43);

    let unload = runtime.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 7 });
    assert!(unload.status.ok, "{unload:?}");
    let unloaded = runtime.lifecycle_report();
    assert_eq!(unloaded.loaded_shard_count, 0);
    assert_eq!(unloaded.transitions[0].state, "unloaded");
    assert_eq!(unloaded.transitions[0].operation, "unload");
}

#[test]
fn data_node_grpc_streaming_contract_exposes_callbacks_and_job_status() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    let contract = runtime.grpc_streaming_contract();

    assert_eq!(contract.service_name, "temporalstore.v1.DataNodeService");
    assert_eq!(contract.execute_stream_method, "ExecuteStream");
    assert_eq!(
        contract.lifecycle_callback_stream_method,
        "LifecycleCallbacks"
    );
    assert_eq!(contract.job_status_stream_method, "WatchJobStatus");
    assert!(contract.bidirectional_streaming);
    assert!(contract.callback_ack_required);
    assert!(contract.tonic_surface_ready);
}

