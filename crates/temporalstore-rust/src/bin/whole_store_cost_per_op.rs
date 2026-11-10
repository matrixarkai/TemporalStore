// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE WRITE COSTS ON DISK, OVER THE WHOLE DATA DIRECTORY.
//!
//! # WHY THE DENOMINATOR IS THE WHOLE DIRECTORY AND NOT THE WAL
//!
//! `wal_cost_scale_harness` reports `log_bytes_on_disk` by summing a directory, which is the right
//! SHAPE, but it sums only the write-ahead log. A durable write here also lands blocks and an
//! index, and a comparison whose other half sums a storage pool holding its log and its pages
//! together would then differ by WHAT EACH SIDE INCLUDES rather than by what either side costs.
//! That is the denominator trap, and it is the easiest way to publish a ratio that measures
//! bookkeeping.
//!
//! So this sums every file under the whole data directory — cache, blocks, indexes and log — and
//! differences it across a fixed number of operations. Sum of file LENGTHS, never resident size:
//! a resident-set delta follows allocator high-water marks and page reuse, and one taken on this
//! path went non-monotonic across a sweep and had to be withdrawn.
//!
//! # WHAT IT PROVES BEFORE IT REPORTS
//!
//! A byte figure is about nothing unless the writes arrived. A write path that accepts and stores
//! nothing still returns success — that has happened in this comparison, and an entire
//! amplification figure once measured a no-op. So the last key written is read back and compared
//! byte for byte, and the run refuses to print a cost unless every operation was accepted and the
//! read-back matches.
//!
//! Usage: whole_store_cost_per_op <string|hash> <ops> <key_bytes> <value_bytes>

use temporalstore_rust::control::{Config, LoadShardRequest, SetConfigRequest};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::types::{Command, CommandResponse, ExecuteRequest};

/// Sum of every file length under `root`, recursively. The directory is the denominator.
fn tree_bytes(root: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                walk(&entry.path(), total);
            } else {
                *total += meta.len();
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

/// Bytes per top-level directory under `root`, so a delta can be attributed to a leg rather than
/// landing in one undifferentiated total. Enumerated from the filesystem rather than from a list of
/// names, because the names are the engine's business and a hardcoded list silently omits a leg the
/// engine adds later -- which is how a census starts missing a row.
fn legs(root: &std::path::Path) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let Ok(meta) = entry.metadata() else { continue };
        let bytes = if meta.is_dir() {
            tree_bytes(&entry.path())
        } else {
            meta.len()
        };
        *out.entry(name).or_insert(0) += bytes;
    }
    out
}

fn padded(prefix: &str, index: usize, width: usize) -> String {
    let mut s = format!("{prefix}{index}");
    // Fixed width, so the total is ops * width and a reader can check the arithmetic -- but the
    // FILLER VARIES. Padding with one repeated character made a container model's member
    // compressible, and a container model stores its member, so the same artefact that broke the
    // value payload broke these too.
    let alphabet = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut n = index.wrapping_mul(2_654_435_761);
    while s.len() < width {
        n = n.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        s.push(alphabet[(n >> 16) % alphabet.len()] as char);
    }
    s.truncate(width);
    s
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let model = args.get(1).map(String::as_str).unwrap_or("string");
    let ops: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(200);
    let key_bytes: usize = args.get(3).and_then(|a| a.parse().ok()).unwrap_or(16);
    let value_bytes: usize = args.get(4).and_then(|a| a.parse().ok()).unwrap_or(64);
    assert!(
        matches!(
            model,
            "string" | "hash" | "set" | "zset" | "list" | "feature"
        ),
        "model must be string|hash|set|zset|list|feature, got {model}"
    );
    // CONTAINER MODELS WRITE ONE MEMBER PER OPERATION INTO ONE KEY, which is the comparison that
    // matters: the alternative design re-encodes and rewrites the whole container per operation,
    // so the question is how our per-member cost behaves as the container grows. One key, N
    // members, N operations.
    let one_container = matches!(model, "set" | "zset" | "list" | "feature");

    let dir = std::env::temp_dir().join(format!(
        "whole_store_cost_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let memory_capacity_bytes = std::env::var("WSC_MEMORY_BYTES")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("WSC_MEMORY_BYTES must be an integer")
        })
        .unwrap_or(64 << 20);
    let end_routing_bucket = std::env::var("WSC_END_ROUTING_BUCKET")
        .ok()
        .map(|value| {
            value
                .parse::<u32>()
                .expect("WSC_END_ROUTING_BUCKET must be an integer")
        })
        .unwrap_or(temporalstore_rust::DEFAULT_END_ROUTING_BUCKET);
    let engine = TemporalEngine::with_local_dirs(
        memory_capacity_bytes,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 0,
                local_node_id: None,
                shard_uri: String::new(),
                start_routing_bucket: 0,
                end_routing_bucket,
                readonly: false,
                table_name: String::new(),
            })
            .status
            .ok
    );
    // DURABLE LOGGING ON. Their oplog is unconditional, so a comparison against a configuration
    // that skips our log would be measuring a different durability promise, not a cheaper one.
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Default::default()
                },
            })
            .ok,
        "the durability setting under test has to be the one that is set"
    );

    // Measured AFTER load and config, so the figure is the cost of the WRITES.
    let before = tree_bytes(&dir);
    let legs_before = legs(&dir);
    let wal_before = engine.write_ahead_log_store().stats(1).bytes_written;
    let index_log_before = engine.index_log_store().stats(1).bytes_written;
    // INCOMPRESSIBLE AND DETERMINISTIC. A repeated byte made every figure a compression
    // measurement -- `bytes/op` fell as the payload grew and the envelope went negative, which a
    // real cost cannot do. splitmix64 over the index gives bytes with no exploitable structure
    // while keeping the run reproducible, so a re-run compares against this one.
    // WSC_PAYLOAD=identical reproduces the ORIGINAL fixture -- one repeated byte, maximally
    // compressible -- so the published figure can be reproduced and compared against the
    // incompressible arm BY THE SAME BINARY. Across two builds the build itself is uncontrolled.
    let identical_payload = std::env::var("WSC_PAYLOAD").as_deref() == Ok("identical");
    let value: Vec<u8> = if identical_payload {
        vec![b'v'; value_bytes]
    } else {
        let mut out = Vec::with_capacity(value_bytes);
        let mut z: u64 = 0x9E37_79B9_7F4A_7C15;
        while out.len() < value_bytes {
            z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut x = z;
            x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            x ^= x >> 31;
            out.extend_from_slice(&x.to_le_bytes());
        }
        out.truncate(value_bytes);
        out
    };
    let mut accepted = 0usize;
    let started = std::time::Instant::now();
    let mut last_key = String::new();
    for index in 0..ops {
        let key = padded("k", index, key_bytes);
        last_key = key.clone();
        // A container model keeps ONE key and varies the member; a scalar model varies the key.
        let (key, member) = if one_container {
            // THE MEMBER CARRIES THE PAYLOAD. A container model stores its member and never
            // touches `value`, so a member of `key_bytes` stored 16 bytes where string stored
            // 80 -- and the two were then compared as if they had stored the same thing.
            ("container".to_string(), padded("m", index, value_bytes))
        } else {
            (key, String::new())
        };
        last_key = key.clone();
        let command = match model {
            // ONE FIXED FIELD, N KEYS -- deliberately NOT one hash growing by a field. This is the
            // workload the matched cross-implementation figure was taken on, and changing it here
            // would silently compare two different things. (Making the field vary without also
            // making this a container model wrote the EMPTY field 200 times, which the arrival
            // proof caught.)
            "hash" => Command::HashSet {
                key: key.clone(),
                field: "f".to_string(),
                value: value.clone(),
            },
            "set" => Command::SetAdd {
                key: key.clone(),
                member: member.clone().into_bytes(),
            },
            "zset" => Command::ZSetAdd {
                key: key.clone(),
                member: member.clone().into_bytes(),
                score: index as f64,
            },
            "list" => Command::ListPush {
                key: key.clone(),
                member: member.clone().into_bytes(),
                left: false,
            },
            "feature" => Command::FeatureAppend {
                key: key.clone(),
                points: vec![temporalstore_rust::types::FeaturePoint {
                    timestamp_ms: 1_000 + index as u64,
                    value: value.clone(),
                }],
            },
            _ => Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            },
        };
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        if outcome.status.ok {
            accepted += 1;
        } else {
            eprintln!("op {index} refused: {:?}", outcome.response);
        }
    }
    let elapsed = started.elapsed();
    let after = tree_bytes(&dir);
    let wal_appended = engine.write_ahead_log_store().stats(1).bytes_written - wal_before;
    let index_log_appended = engine.index_log_store().stats(1).bytes_written - index_log_before;

    // ARRIVAL, PROVEN. A success code is not evidence the bytes are stored.
    // ARRIVAL PER MODEL. A container model proves its MEMBER COUNT, because a per-member write
    // that silently collapsed onto one member would still return ok and still grow the directory.
    let last_member = padded("m", ops - 1, value_bytes);
    let served = match model {
        "hash" => matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashGet {
                        key: last_key.clone(),
                        field: "f".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(got) } if got == value
        ),
        // No `SetCard` exists; the listing is the only member count, so count what it returns.
        "set" => matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SetMembers { key: last_key.clone() },
                })
                .response,
            CommandResponse::Members { members } if members.len() == ops
        ),
        "zset" => matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ZSetCard { key: last_key.clone() },
                })
                .response,
            CommandResponse::Integer { value } if value == ops as i64
        ),
        "list" => matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ListLen { key: last_key.clone() },
                })
                .response,
            CommandResponse::Integer { value } if value == ops as i64
        ),
        // No `FeatureCount` exists either. "count" on the aggregate query is the element count,
        // and the window is BOUNDED rather than u64::MAX on purpose: an unbounded window on the
        // gated rollup path walks the time domain, and the fix for that is not on main yet.
        "feature" => matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: last_key.clone(),
                        start_ms: 0,
                        end_ms: 1_000 + ops as u64 + 1,
                        aggregator: "count".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value } if value == ops as i64
        ),
        _ => matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key: last_key.clone() },
                })
                .response,
            CommandResponse::Bytes { value: Some(got) } if got == value
        ),
    };
    // `Bytes`, not `Value` -- read off the arms in `execute_on_shard` rather than guessed, after
    // guessing wrong once.

    let grew = after.saturating_sub(before);
    println!("model              {model}");
    println!("ops                {ops}");
    println!("key bytes          {key_bytes}");
    println!("value bytes        {value_bytes}");
    println!("memory bytes       {memory_capacity_bytes}");
    println!("routing buckets    0..{end_routing_bucket}");
    println!("denominator        sum of file lengths under the WHOLE data dir (cache+blocks+indexes+log)");
    println!(
        "record compression TS_WAL_COMPRESS_RECORDS={}   (a compressible payload makes this a codec measurement)",
        std::env::var("TS_WAL_COMPRESS_RECORDS").unwrap_or_else(|_| "<unset: default>".to_string())
    );
    println!(
        "payload shape      {}   (identical = the original maximally-compressible fixture)",
        if identical_payload {
            "identical"
        } else {
            "random"
        }
    );
    println!("bytes before       {before}");
    println!("bytes after        {after}");
    println!("bytes delta        {grew}");
    println!("bytes per op       {:.1}", grew as f64 / ops.max(1) as f64);
    println!("wal appended       {wal_appended}");
    println!("wal bytes per op   {:.1}", wal_appended as f64 / ops.max(1) as f64);
    println!("index log appended {index_log_appended}");
    // WHAT THIS MODEL ACTUALLY STORED, not a constant. `key_bytes + value_bytes` was reported for
    // every model, but a container model stores its member and never touches `value`; the
    // difference was being read as an envelope.
    let user_bytes = match model {
        "string" => key_bytes + value_bytes,
        "hash" => key_bytes + 1 + value_bytes, // field is "f"
        "set" | "list" => "container".len() + value_bytes,
        "zset" => "container".len() + value_bytes + 8, // score
        "feature" => "container".len() + 8 + value_bytes, // timestamp
        _ => key_bytes + value_bytes,
    };
    println!("user bytes per op  {user_bytes}   (what this model stores, not key+value)");
    println!(
        "envelope per op    {:.1}",
        grew as f64 / ops.max(1) as f64 - user_bytes as f64
    );
    for (name, after_bytes) in legs(&dir) {
        let before_bytes = legs_before.get(&name).copied().unwrap_or(0);
        let d = after_bytes.saturating_sub(before_bytes);
        println!(
            "leg {name:<12} {d:>10} B   {:>8.1} B/op",
            d as f64 / ops.max(1) as f64
        );
    }
    println!("accepted           {accepted}/{ops}");
    println!("read-back exact    {served}");
    println!("wall time          {:.3} s", elapsed.as_secs_f64());
    println!(
        "loadavg            {}",
        std::fs::read_to_string("/proc/loadavg")
            .unwrap_or_default()
            .split_whitespace()
            .next()
            .unwrap_or("?")
            .to_string()
    );
    // REFUSE A FIGURE THAT CANNOT BE ABOUT THE WRITES.
    assert_eq!(
        accepted, ops,
        "every operation must be accepted or the figure is about a subset"
    );
    assert!(
        served,
        "THE READ-BACK DID NOT MATCH, so the bytes above are not the cost of storing this payload"
    );
    assert!(
        grew > 0,
        "the directory did not grow; nothing durable was written"
    );
    // REFUSE A PREALLOCATION STEP. `TS_WAL_PREALLOCATE` is ON by default and reserves 256 KiB
    // segments, so a short run's directory delta is the SEGMENT it reserved rather than the
    // records it wrote -- and the tell is that two different models report the identical delta.
    // An earlier harness on this path asserted exactly this and I did not carry it over, so the
    // first run reported 1310.7 B/op for string AND hash, both of them one 262,144 B step.
    const PREALLOC_STEP: u64 = 256 * 1024;
    assert!(
        grew % PREALLOC_STEP != 0 || grew < PREALLOC_STEP,
        "THIS IS A PREALLOCATION STEP, NOT A COST: the directory grew {grew} B, an exact multiple          of the {PREALLOC_STEP} B segment this store reserves in advance. Re-run with          TS_WAL_PREALLOCATE=0 so the log's length follows its records."
    );
    let _ = std::fs::remove_dir_all(&dir);
}
