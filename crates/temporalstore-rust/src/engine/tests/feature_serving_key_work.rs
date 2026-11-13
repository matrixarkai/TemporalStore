// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn reset_key_work() {
    crate::engine::hashing::reset_object_id_derivations();
    crate::engine::hashing::reset_routing_bucket_derivations();
}

fn assert_key_work(key: &str, identities: u64, routes: u64) {
    assert_eq!(
        crate::engine::hashing::object_id_derivations(),
        (
            identities,
            identities * ("feature".len() + key.len()) as u64,
        ),
    );
    assert_eq!(
        crate::engine::hashing::routing_bucket_derivations(),
        (routes, routes * key.len() as u64),
    );
}

fn assert_bounded_read_key_work(key: &str) {
    assert_eq!(
        crate::engine::hashing::object_id_derivations(),
        (1, ("feature".len() + key.len()) as u64),
    );
    let (routes, route_bytes) = crate::engine::hashing::routing_bucket_derivations();
    assert!((1..=3).contains(&routes), "unexpected route derivations: {routes}");
    assert_eq!(route_bytes, routes * key.len() as u64);
}

fn feature_engine() -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    (dir, engine)
}

#[test]
fn feature_batch_write_derives_key_metadata_a_constant_number_of_times() {
    let (_dir, engine) = feature_engine();
    let key = "wide-feature-write";
    let points = (1..=128)
        .map(|timestamp_ms| FeaturePoint {
            timestamp_ms,
            value: timestamp_ms.to_le_bytes().to_vec(),
        })
        .collect();

    reset_key_work();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: key.to_string(),
            points,
        },
    });

    assert_key_work(key, 5, 7);
}

#[test]
fn feature_serving_reads_derive_key_metadata_once() {
    let (_dir, engine) = feature_engine();
    let numeric_key = "numeric-feature-row";
    let numeric_points = (1..=128)
        .map(|timestamp_ms| FeaturePoint {
            timestamp_ms,
            value: b"1".to_vec(),
        })
        .collect();
    engine.execute_durable(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: numeric_key.to_string(),
            points: numeric_points,
        },
    });

    reset_key_work();
    let query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: numeric_key.to_string(),
            start_ms: 1,
            end_ms: 128,
            count: Some(128),
        },
    });
    assert!(matches!(
        query.response,
        CommandResponse::FeaturePoints { ref points } if points.len() == 128
    ));
    assert_bounded_read_key_work(numeric_key);

    reset_key_work();
    let aggregate = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: numeric_key.to_string(),
            start_ms: 1,
            end_ms: 128,
            aggregator: "sum".to_string(),
            count: Some(128),
        },
    });
    assert_eq!(
        aggregate.response,
        CommandResponse::Aggregate { value: 128 },
    );
    assert_bounded_read_key_work(numeric_key);

    let sequence_key = "sequence-feature-row";
    let sequence_points = (1..=128)
        .map(|timestamp_ms| {
            let row = SequenceFeatureRow {
                timestamp_ms,
                gid: timestamp_ms,
                action_type: 7,
                duration: 12,
                author_id: 9,
            };
            FeaturePoint {
                timestamp_ms,
                value: row.encode_feature_proto_value(),
            }
        })
        .collect();
    engine.execute_durable(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: sequence_key.to_string(),
            points: sequence_points,
        },
    });

    reset_key_work();
    let filtered = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: sequence_key.to_string(),
            start_ms: 1,
            end_ms: 128,
            count: Some(128),
            filters: vec![FeatureFilter {
                field: "action_type".to_string(),
                op: FeatureFilterOp::Equal,
                value: 7,
            }],
        },
    });
    assert!(matches!(
        filtered.response,
        CommandResponse::FeaturePoints { ref points } if points.len() == 128
    ));
    assert_bounded_read_key_work(sequence_key);

    reset_key_work();
    let sequence = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceQuery {
            key: sequence_key.to_string(),
            start_ms: 1,
            end_ms: 128,
            count: 128,
            filters: Vec::new(),
        },
    });
    assert!(matches!(
        sequence.response,
        CommandResponse::SequenceRows { ref rows } if rows.len() == 128
    ));
    assert_bounded_read_key_work(sequence_key);
}
