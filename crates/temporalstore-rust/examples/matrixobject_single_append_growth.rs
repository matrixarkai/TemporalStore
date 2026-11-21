// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Single, unbatched durable publication of engine-produced binary outcomes.
//! Connects to an existing MatrixObject metadata group. Use a disposable cluster.
//! TARGET names its comma-separated endpoints; COMPACT and RESERVE accept booleans.
//! MILESTONES defaults to 256,1024,8192 writes, VALUE to 4096 bytes, SAMPLES to 128.
//! Samples use the same positions near each segment's end; rotations are separate.
//! This measures one unfenced writer publishing WAL plus its offset index.
//! Engine execution and ownership lease checks are outside this measurement.
//! A quiet host and repeated runs are required before interpreting latency ratios.

use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;
use temporalstore_rust::matrixobject_direct::{DirectAppendOptions, DirectConfig};
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{Command, ExecuteRequest, TemporalEngine};

fn setting(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}

fn phase_command(expected: &str) {
    if std::env::var("PHASE_HANDSHAKE").ok().as_deref() == Some("1") {
        io::stdout().flush().expect("phase marker");
        let mut line = String::new();
        io::stdin().read_line(&mut line).expect("phase command");
        assert_eq!(line.trim(), expected);
    }
}

fn percentile(samples: &mut [u128], percent: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() * percent).div_ceil(100).saturating_sub(1)]
}

fn outcome_template(value_bytes: usize) -> SharedStoreWalEntry {
    let directory = tempfile::tempdir().expect("engine fixture");
    let engine = TemporalEngine::with_local_dirs(
        1024,
        directory.path().join("cache"),
        directory.path().join("blocks"),
        directory.path().join("index"),
    );
    engine.load_shard(1);
    let mut seed = 0x1234_5678u32;
    let value = (0..value_bytes)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "single-append".into(),
            value,
        },
    });
    assert!(response.status.ok);
    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("engine WAL");
    let mut outcomes = records
        .iter()
        .map(|(_, bytes)| temporalstore_rust::wal::decode_wal_line(bytes).expect("binary outcome"))
        .filter(|record| !record.outcomes.is_empty());
    let record = outcomes.next().expect("engine outcome");
    assert!(outcomes.next().is_none() && record.command.is_none());
    let mut blocks = record.staged_blocks;
    if blocks.is_empty() {
        for item in &record.outcomes {
            if let Some(address) = item.resolved_address() {
                blocks.push(temporalstore_rust::wal::StagedBlock {
                    object_id: item.object_id,
                    element_name: item.element_name.as_deref().map(Arc::from),
                    bytes: engine.block_store().read(&address).expect("outcome block"),
                });
            }
        }
    }
    SharedStoreWalEntry {
        shard_id: 1,
        wal_index: 1,
        command: None,
        staged_blocks: blocks,
        outcomes: record.outcomes,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let target = std::env::var("TARGET").expect("TARGET: metadata group endpoints");
    let options = DirectAppendOptions::from_settings(
        Some(&setting("COMPACT", "false")),
        std::env::var("RESERVE").ok().as_deref(),
    )
    .expect("append settings");
    let samples: usize = setting("SAMPLES", "128").parse().expect("SAMPLES");
    let value_bytes: usize = setting("VALUE", "4096").parse().expect("VALUE");
    let segment: usize = setting("SEGMENT_APPENDS", "256")
        .parse()
        .expect("SEGMENT_APPENDS");
    let milestones: Vec<u64> = setting("MILESTONES", "256,1024,8192")
        .split(',')
        .map(|v| v.trim().parse().expect("MILESTONES"))
        .collect();
    assert!((2..=2048).contains(&segment) && samples > 0 && samples < segment);
    assert!((32..=1048576).contains(&value_bytes) && !milestones.is_empty());
    assert!(milestones.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(milestones
        .iter()
        .all(|n| *n > samples as u64 && *n % segment as u64 == 0));
    assert!(
        *milestones.last().unwrap() <= 65536
            && *milestones.last().unwrap() * value_bytes as u64 <= 512 * 1024 * 1024
    );
    let cluster =
        std::env::var("RUN_ID").unwrap_or_else(|_| format!("single-append-{}", std::process::id()));
    assert!(!cluster.is_empty());
    let store = Arc::new(
        DirectConfig::from_env(&target)
            .expect("direct config")
            .connect_with_append_options(options)
            .expect("metadata group"),
    );
    let writer = SharedStoreReplicator::new(&cluster, Arc::clone(&store))
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob)
        .with_wal_segment_appends(segment);
    let template = outcome_template(value_bytes);
    let mut next = 1u64;
    let mut first_p50 = None;
    for milestone in milestones {
        let mut steady = Vec::with_capacity(samples);
        let mut rotations = Vec::new();
        let mut steady_requests = 0;
        let mut wal_frame_bytes = 0;
        while next <= milestone {
            if next == milestone - samples as u64 + 1 {
                println!(
                    "{}",
                    serde_json::json!({"event":"single_append_window_ready","records":milestone,"samples":samples})
                );
                phase_command("GO");
            }
            let mut entry = template.clone();
            entry.wal_index = next;
            let before = store.requests_issued();
            let began = Instant::now();
            let receipt = writer
                .publish_wal_entry(entry)
                .await
                .expect("single durable publication")
                .expect("append receipt");
            let elapsed = began.elapsed().as_micros();
            let requests = store.requests_issued() - before;
            wal_frame_bytes += receipt.bytes_written;
            if (next - 1) % segment as u64 == 0 {
                rotations.push(elapsed);
            }
            if next > milestone - samples as u64 {
                steady.push(elapsed);
                steady_requests += requests;
            }
            next += 1;
        }
        assert_eq!(steady.len(), samples);
        assert_eq!(
            steady_requests,
            samples as u64 * 2,
            "each sampled record needs one WAL and one index append"
        );
        println!(
            "{}",
            serde_json::json!({"event":"single_append_window_done","records":milestone,"samples":samples})
        );
        phase_command("VERIFY");
        let read_store = Arc::new(
            DirectConfig::from_env(&target)
                .expect("fresh direct config")
                .connect_with_append_options(options)
                .expect("fresh direct client"),
        );
        let reader = SharedStoreReplicator::new(&cluster, read_store)
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        let indices = [1, milestone - samples as u64 + 1, milestone];
        for index in indices {
            let actual = reader
                .read_wal_entry_by_offset_metadata(1, index)
                .await
                .expect("fresh reader lookup")
                .expect("persisted outcome");
            let mut expected = template.clone();
            expected.wal_index = index;
            assert_eq!(actual.entry, expected, "stored outcome at {index}");
        }
        let p50 = percentile(&mut steady, 50);
        let p99 = percentile(&mut steady, 99);
        let baseline = *first_p50.get_or_insert(p50.max(1));
        let rotation_p99 = if rotations.is_empty() {
            0
        } else {
            percentile(&mut rotations, 99)
        };
        println!(
            "{}",
            serde_json::json!({"event":"single_append_growth", "compact_append":options.compact_append,"reserve_next":options.reserve_next,"records":milestone,"value_bytes":value_bytes,"samples":samples,"segment_appends":segment,"steady_p50_us":p50,"steady_p99_us":p99,"steady_max_us":steady.last(),"p50_growth_ratio":p50 as f64 / baseline as f64,"rotation_count":rotations.len(),"rotation_p99_us":rotation_p99,"sampled_object_store_requests":steady_requests,"wal_frame_bytes_this_phase":wal_frame_bytes,"fresh_reader_verified_indices":indices,"engine_execution_included":false,"ownership_fence_included":false,"batching":false,"latency_flatness_asserted":false})
        );
    }
}
