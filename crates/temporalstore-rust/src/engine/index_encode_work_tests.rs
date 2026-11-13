// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(dir: &std::path::Path, async_storage: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn seed(engine: &TemporalEngine, count: usize) {
    let commands = (0..count)
        .flat_map(|i| {
            [
                Command::StringSet {
                    key: format!("key-{i}"),
                    value: vec![i as u8; 96],
                },
                Command::HashSet {
                    key: "catalog".into(),
                    field: format!("id-{i}"),
                    value: vec![i as u8; 64],
                },
            ]
        })
        .collect();
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(response.status.ok, "{:?}", response.status);
    assert!(response.responses.iter().all(|row| row.status.ok));
}

fn uncompressed(shard: &ShardState) -> Vec<u8> {
    let mut raw = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut raw).with_struct_map();
    serde::Serialize::serialize(shard, &mut serializer).unwrap();
    raw
}

fn materialized(shard: &ShardState, level: i32) -> Vec<u8> {
    let raw = uncompressed(shard);
    let payload = zstd::stream::encode_all(raw.as_slice(), level).unwrap();
    let mut out = Vec::with_capacity(payload.len() + 11);
    out.extend_from_slice(INDEX_CONTAINER_MAGIC);
    out.push(INDEX_CODEC_ZSTD_MSGPACK);
    out.extend_from_slice(&SHARD_INDEX_FORMAT_VERSION.to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

#[test]
fn buffered_index_keeps_all_decoded_fields_and_reads_both_compression_levels() {
    for async_storage in [false, true] {
        for count in [0, 1, 257] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), async_storage);
            if count > 0 {
                seed(&engine, count);
            }
            let shards = engine.shards.read().unwrap();
            let shard = shards.get(&1).unwrap();
            let raw = uncompressed(shard);
            if count == 257 {
                assert!(raw.len() > 64 * 1024);
            }
            let buffered = encode_index_bytes(shard);
            let old_frame = materialized(shard, 3);
            assert_eq!(&buffered[..11], &old_frame[..11]);
            let current = decode_index_bytes(&buffered).unwrap();
            let previous = decode_index_bytes(&old_frame).unwrap();
            // HashFieldMap's existing wire adapter creates an unordered map on each encode.
            // Compare every decoded field, not the incidental ordering of map entries.
            assert!(
                serde_json::to_value(current).unwrap() == serde_json::to_value(previous).unwrap(),
                "buffered and materialized frames must preserve the same complete state"
            );
        }
    }
}

fn check_values(engine: &TemporalEngine, count: usize) {
    for i in 0..count {
        for (command, expected) in [
            (
                Command::StringGet {
                    key: format!("key-{i}"),
                },
                vec![i as u8; 96],
            ),
            (
                Command::HashGet {
                    key: "catalog".into(),
                    field: format!("id-{i}"),
                },
                vec![i as u8; 64],
            ),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "{:?}", response.status);
            assert!(
                matches!(response.response, CommandResponse::Bytes { value: Some(value) } if value == expected)
            );
        }
    }
}

#[test]
fn existing_level_three_checkpoint_reopens_and_new_compaction_stays_durable() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), async_storage);
        seed(&engine, 32);
        let report = engine.compact_shard_blocks(1).unwrap();
        assert!(report.rewritten_block_refs > 0);
        let path = engine.index_path(1);
        let current = std::fs::read(&path).unwrap();
        let raw = zstd::stream::decode_all(&current[11..]).unwrap();
        let mut old = current[..11].to_vec();
        old.extend(zstd::stream::encode_all(raw.as_slice(), 3).unwrap());
        engine.persist_index_bytes_durable(1, &old).unwrap();
        assert!(
            engine.load_index_base_only(1, false).is_some(),
            "old frame must be readable as a base, not repaired by WAL replay"
        );
        drop(engine);
        let reopened = open(dir.path(), async_storage);
        check_values(&reopened, 32);
        assert!(
            reopened
                .compact_shard_blocks(1)
                .unwrap()
                .rewritten_block_refs
                > 0
        );
        drop(reopened);
        check_values(&open(dir.path(), async_storage), 32);
    }
}

#[cfg(feature = "alloc-probe")]
#[test]
fn buffered_index_avoids_the_full_raw_buffer_and_payload_copy_allocations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    seed(&engine, 8192);
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let raw_bytes = uncompressed(shard).len();
    assert!(raw_bytes > 1024 * 1024);
    drop(materialized(shard, INDEX_ZSTD_LEVEL));
    drop(encode_index_bytes(shard));
    let probe = crate::alloc_probe::Probe::start();
    let old = materialized(shard, INDEX_ZSTD_LEVEL);
    let old_counts = probe.stop();
    let probe = crate::alloc_probe::Probe::start();
    let current = encode_index_bytes(shard);
    let new_counts = probe.stop();
    println!("raw_bytes={raw_bytes} materialized_rust_alloc_bytes={} buffered_rust_alloc_bytes={} materialized_calls={} buffered_calls={}",
        old_counts.alloc_bytes, new_counts.alloc_bytes, old_counts.allocs, new_counts.allocs);
    assert!(
        serde_json::to_value(decode_index_bytes(&old).unwrap()).unwrap()
            == serde_json::to_value(decode_index_bytes(&current).unwrap()).unwrap(),
        "both allocation probes must encode the same complete state"
    );
    // Existing serializer adapters still allocate; isolate the raw buffer removed here.
    assert!(
        old_counts.alloc_bytes >= new_counts.alloc_bytes + raw_bytes as u64,
        "encoding should avoid at least one full raw index worth of Rust allocation requests"
    );
}
