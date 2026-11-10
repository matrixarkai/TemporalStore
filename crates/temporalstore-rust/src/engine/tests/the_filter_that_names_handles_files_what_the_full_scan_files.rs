// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE FILTER THAT NAMES HANDLES FILES WHAT THE FULL SCAN FILES.
//!
//! Filing a block for a container member decides which of the object's existing blocks it
//! supersedes. That decision used to be a scan of every ref the object holds and now looks at the
//! refs that carry the handle of the block written or the block left. A wrong answer is not a slow
//! add, it is a block kept that should have gone (a member served twice, a rescore leaving two live
//! entries) or one removed that should have stayed (a member lost), so this runs the same seeded
//! history under both filters and requires the same index and the same contents, against an oracle.
//!
//! THE HISTORY is every container model, strings and a trimmed sequence, with writes, overwrites,
//! removals, key deletes, a dump of the whole dirty set and of one bucket, a compaction round, and a
//! release and reload of buckets, from a seeded generator.
//!
//! THE COMPARISON, per seed: after every step the picture of every block's flags under the new
//! filter and under the old one (the switch asks a block about every ref) must be identical; the
//! contents must equal the oracle live, after a graceful reload, and after dropping the engine with
//! no unload (recovery from the dumps and the log alone).
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{CommandResponse, SequenceFeatureRow};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const BOUND: usize = 24;
const KEYS_PER_MODEL: usize = 3;
const STEPS: usize = 420;

struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % bound
    }
}

#[derive(Default, Clone, PartialEq, Debug)]
struct Oracle {
    sets: BTreeMap<String, BTreeSet<Vec<u8>>>,
    zsets: BTreeMap<String, BTreeMap<Vec<u8>, i64>>,
    lists: BTreeMap<String, VecDeque<Vec<u8>>>,
    hashes: BTreeMap<String, BTreeMap<String, Vec<u8>>>,
    strings: BTreeMap<String, Vec<u8>>,
    sequences: BTreeMap<String, BTreeSet<u64>>,
}

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    feature_max_size: BOUND,
                    ..Default::default()
                },
            })
            .ok,
        "the sequence bound has to be the one under test"
    );
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn key(model: &str, index: usize) -> String {
    format!("{model}-{index}")
}

fn member(rng: &mut Rng) -> Vec<u8> {
    format!("m{:02}", rng.next(24)).into_bytes()
}

/// Every block's flags, in a form two runs can be compared by.
fn flag_picture(engine: &TemporalEngine, keys: &[String]) -> String {
    let shards = engine.shards.read().unwrap();
    let Some(shard) = shards.get(&1) else {
        return String::new();
    };
    let mut rows = crate::engine::storage_bucket_internals::collect_live_block_entries(shard)
        .iter()
        .filter(|entry| keys.iter().any(|name| name.as_str() == entry.object_key.as_ref()))
        .map(|entry| {
            format!(
                "{} {} dirty={} deleted={}",
                entry.object_key, entry.kind, entry.dirty, entry.deleted
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    let dirty = keys.iter().filter(|name| shard.dirty_objects.contains(name)).count();
    format!("dirty objects among keys: {dirty}\n{}", rows.join("\n"))
}

fn read_all(engine: &TemporalEngine, keys: &BTreeMap<&str, Vec<String>>) -> Oracle {
    let mut seen = Oracle::default();
    for name in &keys["set"] {
        if let CommandResponse::Members { members } = run(engine, Command::SetMembers { key: name.clone() }) {
            if !members.is_empty() {
                seen.sets.insert(name.clone(), members.into_iter().collect());
            }
        }
    }
    for name in &keys["zset"] {
        if let CommandResponse::Members { members } = run(
            engine,
            Command::ZSetRange { key: name.clone(), start: 0, stop: -1, rev: false },
        ) {
            let map = members
                .chunks(2)
                .map(|pair| {
                    (
                        pair[0].clone(),
                        String::from_utf8_lossy(&pair[1]).parse::<f64>().unwrap() as i64,
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if !map.is_empty() {
                seen.zsets.insert(name.clone(), map);
            }
        }
    }
    for name in &keys["list"] {
        if let CommandResponse::Members { members } = run(
            engine,
            Command::ListRange { key: name.clone(), start: 0, stop: -1 },
        ) {
            if !members.is_empty() {
                seen.lists.insert(name.clone(), members.into_iter().collect());
            }
        }
    }
    for name in &keys["hash"] {
        if let CommandResponse::HashEntries { entries } = run(engine, Command::HashGetAll { key: name.clone() }) {
            if !entries.is_empty() {
                seen.hashes.insert(name.clone(), entries.into_iter().collect());
            }
        }
    }
    for name in &keys["string"] {
        if let CommandResponse::Bytes { value: Some(value) } = run(engine, Command::StringGet { key: name.clone() }) {
            seen.strings.insert(name.clone(), value);
        }
    }
    for name in &keys["sequence"] {
        if let CommandResponse::SequenceRows { rows } = run(
            engine,
            Command::SequenceQuery {
                key: name.clone(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: 1000,
                filters: Vec::new(),
            },
        ) {
            let at = rows.iter().map(|row| row.timestamp_ms).collect::<BTreeSet<_>>();
            if !at.is_empty() {
                seen.sequences.insert(name.clone(), at);
            }
        }
    }
    seen
}

/// One history. `check` is called after every step with the step number and the engine.
fn history(
    engine: &TemporalEngine,
    seed: u64,
    mut check: impl FnMut(usize, &TemporalEngine, &[String]),
) -> (Oracle, BTreeMap<&'static str, Vec<String>>) {
    let mut rng = Rng(seed);
    let mut oracle = Oracle::default();
    let models = ["set", "zset", "list", "hash", "string", "sequence"];
    let keys = models
        .iter()
        .map(|&model| (model, (0..KEYS_PER_MODEL).map(|i| key(model, i)).collect::<Vec<_>>()))
        .collect::<BTreeMap<&'static str, Vec<String>>>();
    let mut stamp = 1_000u64;
    let mut dumped_partial = 0u32;
    let mut ops: Vec<String> = Vec::new();
    for step in 0..STEPS {
        let model = models[rng.next(models.len())];
        let name = keys[model][rng.next(KEYS_PER_MODEL)].clone();
        let class = rng.next(14);
        ops.push(format!("step {step}: class {class} on {name}"));
        match class {
            // Writes weigh most, so containers grow past what a removal empties.
            0..=5 => match model {
                "set" => {
                    let m = member(&mut rng);
                    run(engine, Command::SetAdd { key: name.clone(), member: m.clone() });
                    oracle.sets.entry(name).or_default().insert(m);
                }
                "zset" => {
                    let m = member(&mut rng);
                    let score = rng.next(50) as i64;
                    run(engine, Command::ZSetAdd { key: name.clone(), member: m.clone(), score: score as f64 });
                    oracle.zsets.entry(name).or_default().insert(m, score);
                }
                "list" => {
                    let m = member(&mut rng);
                    run(engine, Command::ListPush { key: name.clone(), member: m.clone(), left: false });
                    oracle.lists.entry(name).or_default().push_back(m);
                }
                "hash" => {
                    let field = format!("f{:02}", rng.next(24));
                    let value = vec![b'a' + rng.next(26) as u8; 1 + rng.next(30)];
                    run(engine, Command::HashSet { key: name.clone(), field: field.clone(), value: value.clone() });
                    oracle.hashes.entry(name).or_default().insert(field, value);
                }
                "string" => {
                    let value = vec![b'a' + rng.next(26) as u8; 1 + rng.next(40)];
                    run(engine, Command::StringSet { key: name.clone(), value: value.clone() });
                    oracle.strings.insert(name, value);
                }
                _ => {
                    // Timestamps repeat now and then, so a row is REPLACED as well as added.
                    stamp = if rng.next(5) == 0 { stamp } else { stamp + 1 + rng.next(3) as u64 };
                    run(
                        engine,
                        Command::SequenceAdd {
                            key: name.clone(),
                            rows: vec![SequenceFeatureRow {
                                timestamp_ms: stamp,
                                gid: stamp,
                                action_type: 1,
                                duration: 2,
                                author_id: 3,
                            }],
                        },
                    );
                    let held = oracle.sequences.entry(name).or_default();
                    held.insert(stamp);
                    while held.len() > BOUND {
                        let oldest = *held.iter().next().unwrap();
                        held.remove(&oldest);
                    }
                }
            },
            6..=8 => match model {
                "set" => {
                    let m = member(&mut rng);
                    run(engine, Command::SetRemove { key: name.clone(), member: m.clone() });
                    if let Some(held) = oracle.sets.get_mut(&name) {
                        held.remove(&m);
                        if held.is_empty() {
                            oracle.sets.remove(&name);
                        }
                    }
                }
                "zset" => {
                    let m = member(&mut rng);
                    run(engine, Command::ZSetRemove { key: name.clone(), member: m.clone() });
                    if let Some(held) = oracle.zsets.get_mut(&name) {
                        held.remove(&m);
                        if held.is_empty() {
                            oracle.zsets.remove(&name);
                        }
                    }
                }
                "list" => {
                    run(engine, Command::ListPop { key: name.clone(), left: rng.next(2) == 0 });
                    // The side is not tracked by the oracle: re-read it below, once, from the engine.
                    oracle.lists.remove(&name);
                    if let CommandResponse::Members { members } =
                        run(engine, Command::ListRange { key: name.clone(), start: 0, stop: -1 })
                    {
                        if !members.is_empty() {
                            oracle.lists.insert(name, members.into_iter().collect());
                        }
                    }
                }
                "hash" => {
                    let field = format!("f{:02}", rng.next(24));
                    run(engine, Command::HashDelete { key: name.clone(), field: field.clone() });
                    if let Some(held) = oracle.hashes.get_mut(&name) {
                        held.remove(&field);
                        if held.is_empty() {
                            oracle.hashes.remove(&name);
                        }
                    }
                }
                "string" => {
                    run(engine, Command::StringDelete { key: name.clone() });
                    oracle.strings.remove(&name);
                }
                _ => {}
            },
            9 => {
                engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
                    shard_id: 1,
                    min_undumped_wal_records: 0,
                    min_undumped_wal_bytes: 0,
                    ..crate::engine::reports::StorageLifecycleRequest::default()
                });
            }
            10 => {
                // ONE bucket: the dump that leaves the others dirty.
                let target = {
                    let shards = engine.shards.read().unwrap();
                    shards.get(&1).and_then(|shard| {
                        let (start, end) = shard.routing_range();
                        let names = keys.values().flatten().filter(|n| shard.dirty_objects.contains(n)).collect::<Vec<_>>();
                        names
                            .get(dumped_partial as usize % names.len().max(1))
                            .map(|n| crate::engine::block_routing_bucket(n, start, end))
                    })
                };
                dumped_partial += 1;
                if let Some(bucket) = target {
                    engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
                        shard_id: 1,
                        selected_dump_buckets: vec![bucket],
                        min_undumped_wal_records: 0,
                        min_undumped_wal_bytes: 0,
                        ..crate::engine::reports::StorageLifecycleRequest::default()
                    });
                }
            }
            11 => {
                let _ = engine.compact_shard_blocks(1);
            }
            12 => {
                engine.release_all_releasable_bucket_index_blocks(1);
            }
            _ => {
                for bucket in engine.released_bucket_index_buckets(1) {
                    engine.reload_released_bucket_index_blocks(1, bucket);
                }
            }
        }
        check(step, engine, &ops);
    }
    for bucket in engine.released_bucket_index_buckets(1) {
        engine.reload_released_bucket_index_blocks(1, bucket);
    }
    (oracle, keys)
}

fn all_keys(keys: &BTreeMap<&'static str, Vec<String>>) -> Vec<String> {
    keys.values().flatten().cloned().collect()
}


use crate::engine::storage_bucket_internals::{refs_naming_handles, set_supersede_by_every_ref};
use crate::engine::state::BlockLookupRef;

fn names() -> Vec<String> {
    (0..KEYS_PER_MODEL)
        .flat_map(|i| ["set", "zset", "list", "hash", "string", "sequence"].map(|m| key(m, i)))
        .collect()
}

/// One arm of one seed: the picture after every step, the contents live, after a graceful reload
/// and after a drop with no unload.
fn arm(seed: u64, every_ref: bool) -> (Vec<String>, Oracle, Oracle, Oracle, Oracle) {
    set_supersede_by_every_ref(every_ref);
    let names = names();
    let graceful_dir = tempfile::tempdir().unwrap();
    let crash_dir = tempfile::tempdir().unwrap();
    let mut pictures = Vec::new();
    let (oracle, keys, live) = {
        let engine = open(crash_dir.path());
        let (oracle, keys) = history(&engine, seed, |_, engine, _| pictures.push(flag_picture(engine, &names)));
        let live = read_all(&engine, &keys);
        (oracle, keys, live)
        // NO UNLOAD: dropping the engine leaves recovery to the dumps and the log.
    };
    let recovered = read_all(&open(crash_dir.path()), &keys);
    let reloaded = {
        let engine = open(graceful_dir.path());
        let _ = history(&engine, seed, |_, _, _| {});
        engine.unload_shard(1);
        drop(engine);
        read_all(&open(graceful_dir.path()), &keys)
    };
    set_supersede_by_every_ref(false);
    (pictures, oracle, live, recovered, reloaded)
}

#[test]
fn the_filter_that_names_handles_files_what_the_full_scan_files() {
    for seed in [7u64, 4242, 31337] {
        let (new_pictures, new_oracle, new_live, new_recovered, new_reloaded) = arm(seed, false);
        let (old_pictures, old_oracle, old_live, old_recovered, old_reloaded) = arm(seed, true);
        let held = |o: &Oracle| o.sets.len() + o.zsets.len() + o.lists.len() + o.hashes.len() + o.strings.len() + o.sequences.len();
        assert!(held(&new_oracle) >= 8, "seed {seed}: the history left almost nothing to compare");
        assert!(
            new_pictures.iter().any(|p| p.contains("dirty=true")) && new_pictures.iter().any(|p| p.contains("dirty=false")),
            "seed {seed}: the history never showed both a dirty and a clean block"
        );
        assert_eq!(new_oracle, old_oracle, "seed {seed}: the two arms drove different histories");
        assert_eq!(new_pictures.len(), old_pictures.len());
        for (step, (left, right)) in new_pictures.iter().zip(&old_pictures).enumerate() {
            assert_eq!(left, right, "seed {seed}, step {step}: the filters filed different blocks");
        }
        for (label, new, old) in [
            ("live", &new_live, &old_live),
            ("recovered with no unload", &new_recovered, &old_recovered),
            ("reloaded", &new_reloaded, &old_reloaded),
        ] {
            assert_eq!(new, &new_oracle, "seed {seed}: the new filter's shard, {label}, disagrees with the oracle");
            assert_eq!(old, &old_oracle, "seed {seed}: the full scan's shard, {label}, disagrees with the oracle");
        }
    }
}

/// A handle is a `u64` hash, so two refs can share one. The narrowing keeps both and lets the test on
/// the block each names decide, so a collision never removes the wrong block.
#[test]
fn a_handle_shared_by_two_refs_is_decided_by_the_block_not_the_handle() {
    let a = BlockLookupRef { routing_bucket: 1, block_ref_key: 5 };
    let b = BlockLookupRef { routing_bucket: 2, block_ref_key: 5 };
    let c = BlockLookupRef { routing_bucket: 1, block_ref_key: 9 };
    let d = BlockLookupRef { routing_bucket: 3, block_ref_key: 11 };
    let refs = vec![a.clone(), b.clone(), c.clone(), d.clone()];
    // Two refs carry the handle; only the one in bucket 2 names the block.
    let found = refs_naming_handles(&refs, 5, None, |r| r.routing_bucket == 2);
    assert_eq!(found, vec![b.clone()], "a shared handle was taken as the answer");
    // Neither names it: nothing is removed, though both carry the handle.
    assert!(refs_naming_handles(&refs, 5, None, |_| false).is_empty());
    // The block left, under another handle, is found beside the block written; a ref that carries
    // neither is never asked.
    let mut asked = Vec::new();
    let found = refs_naming_handles(&refs, 5, Some(11), |r| {
        asked.push(r.clone());
        true
    });
    assert_eq!(found, vec![a, b, d], "the written and the vacated block were not both named");
    assert!(!asked.contains(&c), "a ref carrying neither handle was asked about a block");
    // One handle for both: no ref is named twice.
    let once = refs_naming_handles(&refs, 5, Some(5), |_| true);
    assert_eq!(once.len(), 2);
}
