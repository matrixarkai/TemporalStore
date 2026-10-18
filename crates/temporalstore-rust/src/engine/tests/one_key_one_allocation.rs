// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE KEY, ONE ALLOCATION -- the one-copy invariant, in a test a DEFAULT run executes.
//!
//! # WHY THIS FILE EXISTS BESIDE THE PROBES THAT ALREADY CHECK THIS
//!
//! `index_bytes_per_key` carries three guards on this invariant and a default `cargo test` runs
//! NONE of them: each is `#[cfg(feature = "alloc-probe")]` AND `#[ignore]`, so it needs a feature
//! flag and `--ignored` to execute. Of that file's 21 tests, 14 are ignored. An assertion has three
//! states -- true, false, and never evaluated -- and those were in the third.
//!
//! That matters more here than usual because of HOW the invariant breaks. A map that could share a
//! key allocation but is missing from `adopt_object_key_allocations` keeps its own copy and
//! **nothing fails**: the data is correct, every read answers, and a clone-based byte probe cannot
//! see the duplicate, because cloning an `Arc` is a refcount bump. Twice in one session a map was
//! found holding a second copy only because a probe was widened by hand -- first
//! `control_state_selection`, then `seen` and `buckets`. A guard that only runs when someone
//! remembers to pass `--ignored` would not have caught either.
//!
//! So this is the cheap version: eight keys, no counting allocator, no corpus, nothing ignored. It
//! measures by POINTER, which is the only way to tell one shared allocation from two equal copies.
//! The expensive probes keep their breakdowns and their byte figures; this one keeps the invariant
//! reachable.
//!
//! rust-internal: measures this crate's own resident structures, no product behaviour

use crate::types::{Command, ExecuteRequest};
use crate::TemporalEngine;
use std::sync::Arc;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

#[test]
fn a_key_held_by_several_maps_is_one_allocation() {
    const KEYS: usize = 8;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // The same raw key through every family that keys a per-key map by it. Each command is here
    // because it puts the key in a DIFFERENT map; drop one and this stops covering that map.
    for index in 0..KEYS {
        let raw = format!("one-copy-{index}");
        for command in [
            Command::StringSet {
                key: raw.clone(),
                value: vec![b'v'; 16],
            },
            Command::FeatureAppend {
                key: raw.clone(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index as u64,
                    value: vec![b'f'; 8],
                }],
            },
            Command::ControlStateSelectionSet {
                key: raw.clone(),
                value: vec![b's'; 8],
                occur_time_ms: 1_787_270_070_000 + index as u64,
                ttl_ms: 0,
                selection_type: crate::types::ControlStateSelectionType::Last,
            },
            Command::SeenCheck {
                key: raw.clone(),
                member: vec![b'm'; 8],
                window_ms: 60_000,
            },
            Command::BucketTake {
                key: raw.clone(),
                tokens: 1.0,
                capacity: 10.0,
                refill_per_sec: 1.0,
            },
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // (text, address of its characters) from every map keyed by the raw key. A `String` key and an
    // `Arc<str>` key both expose `as_ptr()`, so this needs no list of which map is keyed by what --
    // and an allowlist of map NAMES is what went stale the first time this guard was written.
    let mut held: std::collections::BTreeMap<&str, Vec<(&str, usize)>> =
        std::collections::BTreeMap::new();
    for (name, keys) in [
        (
            "strings",
            shard
                .strings
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect::<Vec<_>>(),
        ),
        (
            "features",
            shard
                .features
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "control_state_selection",
            shard
                .control_state_selection
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "seen",
            shard
                .seen
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "buckets",
            shard
                .buckets
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
    ] {
        for (text, ptr) in keys {
            held.entry(text).or_default().push((name, ptr));
        }
    }

    // --- DENOMINATORS, because every claim below divides by them. ---
    let multi: Vec<_> = held.iter().filter(|(_, h)| h.len() > 1).collect();
    assert_eq!(
        KEYS,
        multi.len(),
        "the fixture wrote {KEYS} keys through five families but only {} ended up in more than one \
         map, so this run could not have detected a duplicate at all. Held: {:?}",
        multi.len(),
        held
    );
    assert!(
        multi.iter().all(|(_, h)| h.len() == 5),
        "a key reached fewer than five maps, so one of the five commands stopped writing its map \
         and this guard silently stopped covering it: {:?}",
        multi
    );

    // --- THE INVARIANT. ---
    let duplicated: Vec<_> = held
        .iter()
        .filter(|(_, h)| {
            h.iter()
                .map(|(_, p)| *p)
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 1
        })
        .collect();
    assert!(
        duplicated.is_empty(),
        "{} key texts are stored more than once -- their holders report different addresses for \
         the same characters: {:?}. A map holding a key another map already holds must take a \
         `ModelKey` and join `ShardState::adopt_object_key_allocations`; nothing else fails when \
         it does not, because cloning an `Arc` is a refcount bump.",
        duplicated.len(),
        &duplicated[..duplicated.len().min(3)]
    );
}
