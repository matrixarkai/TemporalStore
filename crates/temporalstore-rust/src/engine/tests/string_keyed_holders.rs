// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH `String`-KEYED HOLDERS STORE A KEY SOME OTHER MAP ALREADY HOLDS.
//!
//! `one_key_one_allocation` proves the invariant for the five maps its fixture reaches. It cannot
//! say anything about the seven maps in `ShardState` still keyed by `String`, because none of its
//! five commands writes them -- and a map absent from the fixture reports zero keys, which reads
//! exactly like a map with nothing to share. That mistake has already been made once in this
//! campaign, so this file exists to answer the question with writes rather than with a grep.
//!
//! The seven, as of this measurement:
//!
//!   control_state          control_state_uuid     control_state_rollups   feature_values
//!   feature_rollups        context_events_compat  context_dirty_index
//!
//! All of them are resolved now -- `control_state` converted, the other six measured and
//! refused for a stated reason -- and so are the four `RecordedMap` kinds this file later caught
//! minting their own copy: `hashes`, `sets`, `zsets` and `lists` share the handle too.
//!
//! What matters is not that they are `String`-keyed. It is whether the TEXT they hold is text some
//! `ModelKey` map holds too: a key with one holder has nothing to share with and converting it is
//! pure churn. So this drives every family that writes any of the seven, through the SAME raw key,
//! and prints holder-by-holder addresses. Two holders of one text at two addresses is a duplicated
//! allocation; the same address twice is a shared one.
//!
//! rust-internal: measures this crate's own resident structures, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib which_string_keyed_holders -- --nocapture

use crate::types::{Command, ControlStateFamily, ExecuteRequest};
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
fn which_string_keyed_holders_duplicate_a_key_another_map_holds() {
    const KEYS: usize = 8;
    const T0: u64 = 1_787_270_070_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // One raw key per index, written through every family that populates one of the seven. Each
    // command is here because it writes a DIFFERENT map; dropping one stops covering that map.
    for index in 0..KEYS {
        let raw = format!("holder-{index}");
        let at = T0 + index as u64;
        let commands = vec![
            // the already-shared ModelKey holders, so there is something to share WITH
            Command::StringSet { key: raw.clone(), value: vec![b'v'; 16] },
            Command::FeatureAppend {
                key: raw.clone(),
                points: vec![crate::types::FeaturePoint { timestamp_ms: at, value: vec![b'f'; 8] }],
            },
            Command::ControlStateSelectionSet {
                key: raw.clone(),
                value: vec![b's'; 8],
                occur_time_ms: at,
                ttl_ms: 0,
                selection_type: crate::types::ControlStateSelectionType::Last,
            },
            // control_state, and the rollup cache beside it
            Command::ControlStateIncrement { key: raw.clone(), timestamp_ms: at, amount: 1 },
            Command::ControlStateSetAndGet {
                family: ControlStateFamily::Counter,
                key: raw.clone(),
                timestamp_ms: at,
                amount: 1,
                start_ms: T0,
                end_ms: T0 + 86_400_000,
                aggregator: "sum".to_string(),
            },
            // The four RecordedMap kinds. Their map is a private `HashMap<String, _>` inside
            // `recorded_map`, so nothing outside that module can convert the key -- which is
            // exactly why they were never looked at.
            Command::HashSet {
                key: raw.clone(),
                field: "f".to_string(),
                value: vec![b'h'; 8],
            },
            Command::SetAdd { key: raw.clone(), member: vec![b'm'; 8] },
            Command::ZSetAdd { key: raw.clone(), member: vec![b'z'; 8], score: 1.0 },
            Command::ListPush { key: raw.clone(), member: vec![b'l'; 8], left: true },
            // feature_values and feature_rollups: populated by the AGGREGATE QUERY, not the append
            Command::FeatureAggQuery {
                key: raw.clone(),
                start_ms: T0,
                end_ms: T0 + 86_400_000,
                aggregator: "sum".to_string(),
                count: None,
            },
        ];
        for command in commands {
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // (holder name, text, address of the characters). A `String` key and an `Arc<str>` key both
    // expose `as_ptr()`, so this needs no list of which map is keyed by what -- and an allowlist of
    // names is exactly what went stale the first time a guard like this was written.
    let mut held: std::collections::BTreeMap<String, Vec<(&str, usize)>> =
        std::collections::BTreeMap::new();
    let mut record = |name: &'static str, keys: Vec<(&str, usize)>| {
        for (text, ptr) in keys {
            held.entry(text.to_string()).or_default().push((name, ptr));
        }
    };
    record(
        "strings",
        shard.strings.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record("features", shard.features.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    record(
        "control_state_selection",
        shard.control_state_selection.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record(
        "control_state",
        shard.control_state.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record(
        "control_state_uuid",
        shard.control_state_uuid.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record(
        "control_state_rollups",
        shard.control_state_rollups.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record(
        "feature_values",
        shard.feature_values.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record(
        "feature_rollups",
        shard.feature_rollups.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    // `RecordedMap` keeps its map private and exposes only `keys()`, so this is the one way to
    // ask the question from outside that module.
    record("hashes", shard.hashes.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    record("sets", shard.sets.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    record("zsets", shard.zsets.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    record("lists", shard.lists.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());

    println!();
    println!("  {KEYS} raw keys, written through six families");
    println!();
    let mut duplicated_texts = 0usize;
    let mut duplicate_holders: std::collections::BTreeMap<&str, usize> =
        std::collections::BTreeMap::new();
    for (text, holders) in &held {
        let addresses: std::collections::BTreeSet<usize> =
            holders.iter().map(|(_, p)| *p).collect();
        let shared = addresses.len() == 1;
        if !shared {
            duplicated_texts += 1;
        }
        println!(
            "  {text:<12} {} holder(s), {} allocation(s){}",
            holders.len(),
            addresses.len(),
            if shared { "" } else { "   <-- DUPLICATED" }
        );
        // the minority address is the one that minted: whichever holders do not agree with the
        // most common address are the ones to convert.
        let mut tally: std::collections::BTreeMap<usize, usize> = std::collections::BTreeMap::new();
        for (_, p) in holders {
            *tally.entry(*p).or_default() += 1;
        }
        let canonical = tally.iter().max_by_key(|(_, n)| **n).map(|(p, _)| *p).unwrap_or(0);
        for (name, ptr) in holders {
            let mark = if *ptr == canonical { "shared" } else { "MINTS " };
            println!("      {mark}  {name:<24} {ptr:#x}");
            if *ptr != canonical {
                *duplicate_holders.entry(name).or_default() += 1;
            }
        }
    }

    println!();
    println!("  texts duplicated: {duplicated_texts} of {}", held.len());
    println!("  holders that mint their own copy:");
    if duplicate_holders.is_empty() {
        println!("      (none)");
    }
    for (name, n) in &duplicate_holders {
        println!("      {name:<24} {n} of {KEYS}");
    }

    // ---- DENOMINATORS, because every count above divides by them. -------------------
    //
    // `control_state` holds TWO kinds of key and only one of them can ever share. The counter
    // commands file the RAW key; the family operators file a DECORATED one
    // (`control_state:<family>:<key>`), which no other map holds -- so a saving quoted per
    // `control_state` ENTRY would be quoting the wrong denominator. The split is asserted so a
    // later change cannot quietly move entries from one side to the other.
    let raw: Vec<&String> = held.keys().filter(|text| !text.contains(':')).collect();
    let decorated: Vec<&String> = held.keys().filter(|text| text.contains(':')).collect();
    assert_eq!(
        KEYS,
        raw.len(),
        "the fixture wrote {KEYS} raw keys but {} undecorated texts are resident, so the families          are not keying by the text this measures",
        raw.len()
    );
    assert_eq!(
        KEYS,
        decorated.len(),
        "expected one decorated control-state key per raw key; found {}. If that moved, a saving          quoted per entry and one quoted per raw key are no longer the same ratio",
        decorated.len()
    );
    assert!(
        decorated.iter().all(|text| held[*text].len() == 1),
        "a decorated control-state key gained a second holder, so it CAN now share and this          measurement's conclusion that only the raw half matters is stale"
    );
    // EXACTLY FOUR OF THE EIGHT HOLDERS ARE REACHABLE ON THE SHIPPED DEFAULT, and that is the
    // finding rather than a shortcoming of the fixture. This assert pins the four by name,
    // because the first version of it guessed six and had to be corrected by this run.
    //
    // The four that stay empty, and why -- none of them is a map with nothing to share, which is
    // the conclusion a zero would otherwise invite:
    //
    //   control_state_rollups  |  gated by Config::control_rollup_enabled, which is OFF by
    //   feature_values         |  default. Their own field comments say "empty and inert when
    //   feature_rollups        |  the gate is off", and this run confirms it by writing the
    //                          |  commands that would fill them and finding nothing.
    //   control_state_uuid     |  written only by the WITH-OPTIONS counter operator, and keyed
    //                          |  by a dedup token rather than the object key -- so even when
    //                          |  populated it holds text no other map holds.
    //
    // If this set changes, something material changed: a flag default flipped, or a command
    // stopped writing its map. Either way the conclusions above need re-measuring, not a wider
    // threshold.
    let reached: std::collections::BTreeSet<&str> =
        held.values().flatten().map(|(name, _)| *name).collect();
    let expected: std::collections::BTreeSet<&str> = [
        "control_state",
        "control_state_selection",
        "features",
        "hashes",
        "lists",
        "sets",
        "strings",
        "zsets",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        expected, reached,
        "the set of holders this fixture reaches changed. Gained: {:?}. Lost: {:?}. A gain means \
         a gate opened and that map now has keys to share; a loss means a command stopped \
         writing its map and this measurement stopped covering it.",
        reached.difference(&expected).collect::<Vec<_>>(),
        expected.difference(&reached).collect::<Vec<_>>()
    );

    // And the point of the whole file: with `control_state` adopting the page index handle, no
    // text is stored twice. Before the conversion this was 8 of 16, every raw key.
    let duplicated: Vec<_> = held
        .iter()
        .filter(|(_, holders)| {
            holders.iter().map(|(_, p)| *p).collect::<std::collections::HashSet<_>>().len() > 1
        })
        .map(|(text, _)| text)
        .collect();
    println!();
    println!("  duplicated texts: {} of {}", duplicated.len(), held.len());

    // ---- NO HOLDER MINTS ITS OWN COPY ANY MORE -------------------------------------------
    //
    // This block used to pin four known minters -- `hashes`, `sets`, `zsets` and `lists` -- each
    // owning a second copy of a key text the other four holders had already agreed on. Eight
    // holders of one text reported FIVE allocations. They are converted now, so the pinning is
    // retired and the plain invariant takes over: one text, one allocation, whoever holds it.
    //
    // The pinning was deliberately two-directional, and this is the step it existed to force. A
    // baseline that only forbids NEW minters would have let the conversion land with this test
    // still green and the list of four quietly stale, which is how a guard stops describing the
    // code without anybody noticing.
    assert!(
        duplicate_holders.is_empty(),
        "{} holder(s) mint their own copy of a key text another holder already has: {:?}. A map \
         that CAN share and does not keeps an equal copy and nothing else fails, because cloning \
         an `Arc` is a refcount bump. A `String`-keyed holder needs a `ModelKey` and a place in \
         `ShardState::adopt_object_key_allocations`; a `RecordedMap` kind needs its `adopt_key` \
         called there, because its map is private and nothing outside that module can re-key it.",
        duplicate_holders.len(),
        duplicate_holders
    );
    assert!(
        duplicated.is_empty(),
        "{} key texts are stored more than once: {:?}",
        duplicated.len(),
        &duplicated[..duplicated.len().min(4)]
    );

    // The raw keys must be ONE allocation across all eight holders. Asserted as a count rather
    // than as "no duplicates" so the figure in the doc comment above cannot drift from the code:
    // eight holders, one allocation.
    for (text, holders) in held.iter().filter(|(text, _)| !text.contains(':')) {
        let addresses: std::collections::BTreeSet<usize> =
            holders.iter().map(|(_, p)| *p).collect();
        assert_eq!(
            1,
            addresses.len(),
            "`{text}` is held by {} holders at {} addresses: {:?}",
            holders.len(),
            addresses.len(),
            holders
        );
        assert_eq!(
            8,
            holders.len(),
            "`{text}` reached {} holders, not the eight this fixture writes -- a command stopped \
             writing its map and this measurement stopped covering it: {:?}",
            holders.len(),
            holders
        );
    }
}
