// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Encoding diagnostic only; storage paths do not use this layout. Root sizes are
//! estimates, not filesystem allocation or measured I/O. Preparation includes
//! decompression, chunking, compression, hashing, and byte-round-trip verification,
//! but excludes snapshot capture and filesystem I/O.

use super::*;
use sha2::{Digest, Sha256};

fn compressed_pages(image: &[u8], average: usize) -> (Vec<u8>, Vec<([u8; 32], Vec<u8>)>) {
    let prefix_len = INDEX_CONTAINER_MAGIC.len() + 1 + INDEX_BINARY_VERSION_BYTES;
    assert!(image.starts_with(INDEX_CONTAINER_MAGIC));
    assert_eq!(image[INDEX_CONTAINER_MAGIC.len()], INDEX_CODEC_ZSTD_MSGPACK);
    let raw = zstd::stream::decode_all(&image[prefix_len..]).unwrap();
    let pages = fastcdc::v2020::FastCDC::new(&raw, average / 4, average, average * 4)
        .map(|chunk| {
            let bytes = zstd::stream::encode_all(
                &raw[chunk.offset..chunk.offset + chunk.length],
                INDEX_ZSTD_LEVEL,
            )
            .unwrap();
            (Sha256::digest(&bytes).into(), bytes)
        })
        .collect::<Vec<_>>();
    let mut framed = image[..prefix_len].to_vec();
    for (_, bytes) in &pages {
        framed.extend_from_slice(bytes);
    }
    assert_eq!(
        zstd::stream::decode_all(&framed[prefix_len..]).unwrap(),
        raw
    );
    (framed, pages)
}

#[test]
fn checkpoint_pages_round_trip_and_realign_after_an_insert() {
    let mut seed = 7u64;
    let mut payload = Vec::with_capacity(256 << 10);
    for _ in 0..(256 << 10) / 8 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        payload.extend_from_slice(&seed.to_le_bytes());
    }
    let prefix_len = INDEX_CONTAINER_MAGIC.len() + 1 + INDEX_BINARY_VERSION_BYTES;
    let mut image = encode_index_bytes(&ShardState::default());
    image.truncate(prefix_len);
    image.extend(zstd::stream::encode_all(payload.as_slice(), INDEX_ZSTD_LEVEL).unwrap());
    let (_, before) = compressed_pages(&image, 4096);
    assert!(before.len() > 8, "the fixture must contain multiple frames");
    let known = before
        .iter()
        .map(|(digest, _)| *digest)
        .collect::<BTreeSet<_>>();
    payload.splice(65_536..65_536, [0x7f; 64]);
    image.truncate(prefix_len);
    image.extend(zstd::stream::encode_all(payload.as_slice(), INDEX_ZSTD_LEVEL).unwrap());
    let (_, after) = compressed_pages(&image, 4096);
    let reused = after
        .iter()
        .filter(|(digest, _)| known.contains(digest))
        .count();
    assert!(
        reused * 4 >= before.len() * 3,
        "only {reused} of {} pages were reused",
        before.len()
    );
}

#[test]
#[ignore = "large checkpoint encoding measurement"]
fn checkpoint_pages_measure_round_and_update_cost() {
    let records = std::env::var("CPC_RECORDS")
        .ok()
        .map(|text| text.parse::<usize>().unwrap())
        .unwrap_or(16_000);
    assert!((1_024..=200_000).contains(&records));
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        64 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                load_version: 0,
                local_node_id: None,
                shard_uri: String::new(),
                start_routing_bucket: 0,
                end_routing_bucket: 65_535,
                readonly: false,
                table_name: String::new(),
            })
            .status
            .ok
    );
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok
    );
    let mut seed = 0x9e3779b97f4a7c15u64;
    let mut value = Vec::new();
    for _ in 0..8 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        value.extend_from_slice(&seed.to_le_bytes());
    }
    let key = |index| format!("page-{index:011}");
    for index in 0..records {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key(index),
                        value: value.clone()
                    },
                })
                .status
                .ok
        );
    }
    let mut manifests = Vec::new();
    for round in 0..3 {
        if round == 2 {
            value[0] ^= 0xff;
            for index in 0..64 {
                assert!(
                    engine
                        .execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringSet {
                                key: key(index),
                                value: value.clone()
                            },
                        })
                        .status
                        .ok
                );
            }
        }
        let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            max_dump_buckets_per_round: 64,
            ..Default::default()
        });
        let manifest = report.dump_manifest.expect("a bounded dump must run");
        assert_eq!(manifest.bucket_ids.len(), 64);
        manifests.push(manifest);
    }
    for average in [4096, 8192, 16384, 32768] {
        let mut durable = BTreeSet::new();
        for (round, manifest) in manifests.iter().enumerate() {
            let started = std::time::Instant::now();
            let (image, pages) = compressed_pages(&manifest.index_bytes, average);
            let elapsed_us = started.elapsed().as_micros();
            let state = decode_index_bytes(&image).unwrap();
            assert_eq!(state.strings().len(), records);
            assert_eq!(state.applied_wal_sequence, Some(manifest.wal_sequence));
            let metadata = encode_bucket_dump_manifest(manifest).len() - manifest.index_bytes.len();
            // Size model: 24 header bytes, then a 32-byte digest and 4-byte length per page.
            let root_bytes = metadata + 24 + pages.len() * 36;
            let mut new_bytes = 0;
            let mut new_pages = 0;
            for (digest, bytes) in &pages {
                if durable.insert(*digest) {
                    new_bytes += bytes.len();
                    new_pages += 1;
                }
            }
            eprintln!(
                "CHECKPOINT_PAGE_COST records={records} average={average} round={round} \
                 inline_bytes={} framed_bytes={} pages={} new_pages={new_pages} \
                 estimated_root_bytes={root_bytes} new_payload_bytes={new_bytes} \
                 estimated_total_bytes={} prepare_elapsed_us={elapsed_us}",
                manifest.index_bytes.len() + metadata,
                image.len(),
                pages.len(),
                root_bytes + new_bytes,
            );
        }
    }
}
