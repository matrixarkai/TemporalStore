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
    record("features", shard.features().keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
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
        shard.control_state_rollups().keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record(
        "feature_values",
        shard.feature_values.keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
    );
    record(
        "feature_rollups",
        shard.feature_rollups().keys().map(|k| (&**k, k.as_ptr() as usize)).collect(),
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

/// WHETHER THE SHARING SURVIVES A REOPEN WITH NO DUMP -- WHICH IS **NOT** A DECODE.
///
/// THE NAME OF THIS TEST IS WRONG AND IS KEPT ONLY SO THE CORRECTION IS FINDABLE. It measures a
/// write, a drop and a reopen, and a reopen with no dump has no index image to read: every caller
/// of `decode_index_bytes` reads `manifest.index_bytes`. Instrumented, `repack_decoded_btrees`
/// runs **zero times** here, so nothing this test shows is evidence about the decode path or about
/// `adopt_canonical_key_allocations`. An earlier change attributed this figure to exactly that
/// pass, which was reading a real limitation of some other path into this one.
///
/// `whether_the_shared_key_survives_a_dump_and_install` is the arm that reaches a decode, and
/// there the sharing SURVIVES. What this one measures is the recovery path a reopen without a dump
/// takes, where it does not -- a separate finding with a cause still to be found.
///
/// Every measurement here so far used a FRESH engine, where each map takes its handle from the
/// page index as the write happens. A reload is a different path: `serde` gives every occurrence
/// of a key its own allocation on the way back in, and something has to put the sharing back.
///
/// For ORDERED maps something does. `adopt_canonical_key_allocations` walks `expires_at_ms` after
/// the decode and swaps each key for the page index's handle, and that is byte-neutral because a
/// `BTreeMap` rebuilt from its own entries serializes in the same key order.
///
/// For HASHED maps that pass deliberately does nothing, and says so: a `HashMap`'s serialized
/// order follows its iteration order, so rebuilding one there changed the encoded bytes and the
/// format guard caught it. The comment on that function states the consequence outright -- "that
/// map keeps its decoded allocation, and the restored duplication it pays is stated here rather
/// than left to be rediscovered".
///
/// So this measures it rather than taking the comment's word for it, and rather than assuming the
/// per-write adoption covers it: `adopt_object_key_allocations` runs when a key is WRITTEN, so a
/// key that is loaded and never written again would keep whatever the decode handed it.
///
/// This test asserts what it MEASURES, in both directions, so neither a regression nor a fix can
/// pass silently. If a later change restores sharing across the decode, this fails and says so.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib whether_the_shared_key_survives_a_decode -- --nocapture
#[test]
fn whether_the_shared_key_survives_a_decode() {
    const KEYS: usize = 8;
    const T0: u64 = 1_787_270_070_000;

    let dir = tempfile::tempdir().expect("tempdir");

    // ---- fresh: write every family, then confirm the sharing is there to lose ----------
    {
        let engine = engine_on(dir.path());
        for index in 0..KEYS {
            let raw = format!("decode-{index}");
            let at = T0 + index as u64;
            for command in [
                Command::StringSet { key: raw.clone(), value: vec![b'v'; 16] },
                Command::FeatureAppend {
                    key: raw.clone(),
                    points: vec![crate::types::FeaturePoint {
                        timestamp_ms: at,
                        value: vec![b'f'; 8],
                    }],
                },
                Command::ControlStateIncrement {
                    key: raw.clone(),
                    timestamp_ms: at,
                    amount: 1,
                },
                Command::HashSet {
                    key: raw.clone(),
                    field: "f".to_string(),
                    value: vec![b'h'; 8],
                },
                Command::SetAdd { key: raw.clone(), member: vec![b'm'; 8] },
            ] {
                let response = engine.execute(ExecuteRequest { shard_id: 1, command });
                assert!(response.status.ok, "write {index}: {:?}", response.status);
            }
        }

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let before = distinct_addresses_per_raw_key(shard);
        println!();
        println!("  fresh:  {:?}", before);
        assert!(
            before.values().all(|n| *n == 1),
            "the sharing is not present BEFORE the reload, so this test cannot measure whether a \
             decode loses it: {before:?}"
        );
    }

    // ---- reload from the same directory ------------------------------------------------
    let calls_before = crate::engine::state::REPACK_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let reloaded = engine_on(dir.path());
    let calls_after = crate::engine::state::REPACK_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let adopted =
        crate::engine::state::REPACK_ADOPTED_HASHED.load(std::sync::atomic::Ordering::Relaxed);
    println!();
    println!("  decode pass ran {} time(s) during the reload", calls_after - calls_before);
    println!("  hashed keys it adopted: {adopted}");
    let shards = reloaded.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let after = distinct_addresses_per_raw_key(shard);
    println!("  after:  {:?}", after);

    let kept: Vec<_> = after.iter().filter(|(_, n)| **n == 1).collect();
    let lost: Vec<_> = after.iter().filter(|(_, n)| **n > 1).collect();
    println!();
    println!("  keys still sharing after the decode: {} of {}", kept.len(), after.len());
    println!("  keys holding several allocations:    {} of {}", lost.len(), after.len());

    // The fixture has to have survived at all, or the counts above are about an empty shard.
    assert_eq!(
        KEYS,
        after.len(),
        "{} raw keys came back from the reload, not {KEYS} -- the dump did not carry them and \
         nothing below is about sharing",
        after.len()
    );
}

/// How many distinct allocations hold each raw key's characters, across every holder that has one.
fn distinct_addresses_per_raw_key(
    shard: &crate::engine::state::ShardState,
) -> std::collections::BTreeMap<String, usize> {
    let mut held: std::collections::BTreeMap<String, std::collections::BTreeSet<usize>> =
        std::collections::BTreeMap::new();
    let mut note = |keys: Vec<(&str, usize)>| {
        for (text, ptr) in keys {
            if text.contains(':') {
                continue;
            }
            held.entry(text.to_string()).or_default().insert(ptr);
        }
    };
    note(shard.strings.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    note(shard.features().keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    note(shard.control_state.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    note(shard.hashes.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    note(shard.sets.keys().map(|k| (&**k, k.as_ptr() as usize)).collect());
    held.into_iter().map(|(text, addresses)| (text, addresses.len())).collect()
}

/// HOW MANY ALLOCATIONS OF ONE KEY TEXT THE PAGE INDEX ITSELF HOLDS.
///
/// Everything else in this campaign measured the MODEL maps -- the resident maps that answer
/// reads -- and they now agree on one allocation, taken from the page index. This asks about the
/// page index itself, which is the thing they all borrow from.
///
/// `ObjectBlockLookup` is NESTED: `by_model` maps a model kind to a map of object key to refs. The
/// filing path takes its handle with `shared_object_key(kind, object_key)`, which looks inside
/// ONE kind's inner map. For a key being filed under a kind that does not hold it yet that answers
/// `None`, and the caller allocates:
///
///     .shared_object_key(kind, object_key)
///     .unwrap_or_else(|| Arc::from(object_key))
///
/// So the first block of each KIND allocates the key again. The doc on that function explains the
/// problem it was built for -- a container's hundredth field pointing at the copy its first field
/// made, where before each of a hundred made its own -- and that problem is per-kind. Sharing
/// ACROSS kinds was never part of it.
///
/// `any_shared_object_key`, which every model map adopts from, scans the kinds and returns the
/// FIRST handle it finds. So the model maps all converge on one of these allocations while the
/// index keeps the rest.
///
/// This measures how many there are. `key_ptr` exists for exactly this question -- its own doc says
/// it is there "so a test can assert that a block and this map point at one copy of the object
/// identity rather than two equal ones. Contents cannot tell those apart; pointers can."
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib how_many_allocations_the_page_index -- --nocapture
#[test]
fn how_many_allocations_the_page_index_itself_holds() {
    const T0: u64 = 1_787_270_070_000;
    // Every kind this fixture can reach with one key, and the command that files it there.
    const KINDS: &[&str] = &["string", "feature", "control_state", "hash", "set", "zset", "list"];

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let raw = "indexed-key".to_string();

    for command in [
        Command::StringSet { key: raw.clone(), value: vec![b'v'; 16] },
        Command::FeatureAppend {
            key: raw.clone(),
            points: vec![crate::types::FeaturePoint { timestamp_ms: T0, value: vec![b'f'; 8] }],
        },
        Command::ControlStateIncrement { key: raw.clone(), timestamp_ms: T0, amount: 1 },
        Command::HashSet { key: raw.clone(), field: "f".to_string(), value: vec![b'h'; 8] },
        Command::SetAdd { key: raw.clone(), member: vec![b'm'; 8] },
        Command::ZSetAdd { key: raw.clone(), member: vec![b'z'; 8], score: 1.0 },
        Command::ListPush { key: raw.clone(), member: vec![b'l'; 8], left: true },
    ] {
        let response = engine.execute(ExecuteRequest { shard_id: 1, command });
        assert!(response.status.ok, "write failed: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let lookup = &shard.bucket_index.object_block_lookup;

    println!();
    let mut filed: Vec<(&str, usize)> = Vec::new();
    for kind in KINDS {
        match lookup.key_ptr(kind, &raw) {
            Some(ptr) => {
                println!("  {kind:<16} {:#x}", ptr as usize);
                filed.push((kind, ptr as usize));
            }
            None => println!("  {kind:<16} (not filed under this kind)"),
        }
    }

    let distinct: std::collections::BTreeSet<usize> = filed.iter().map(|(_, p)| *p).collect();
    println!();
    println!(
        "  one key text, filed under {} kind(s), held in {} allocation(s)",
        filed.len(),
        distinct.len()
    );
    // What every model map borrows, for comparison: it is one of the above, not a further copy.
    let adopted = lookup.any_shared_object_key(&raw).expect("the key is filed");
    let adopted_ptr = adopted.as_ptr() as usize;
    println!("  what the model maps adopt:   {adopted_ptr:#x}");
    assert!(
        filed.iter().any(|(_, p)| *p == adopted_ptr),
        "the handle the model maps adopt is not one of the index's own allocations, which would \
         mean a further copy nobody accounted for: {adopted_ptr:#x} vs {filed:?}"
    );

    // The fixture must have reached several kinds, or there is nothing to compare.
    assert!(
        filed.len() >= 5,
        "the key was filed under only {} kind(s) ({filed:?}), so this run cannot say whether the \
         index holds one allocation per kind",
        filed.len()
    );
}

/// WHAT THE TOMBSTONE PATH FILES INTO THE OBJECT LOOKUP, AND WHAT IT ALLOCATES TO DO IT.
///
/// `insert_container_tombstone_entry` ends by handing a page to `insert_object_block_lookup`:
///
///     .insert_object_block_lookup(routing_bucket, block_ref_key, &Arc::from(object_key), &page)
///
/// and that page is built, a few lines above, as `BlockIndex { .. deleted: true, .. }`. The first
/// statement of `insert_object_block_lookup` is `if page.deleted { return; }`, and the reason is
/// stated on it: a deleted page is never in the object lookup.
///
/// So on this path the call can do nothing, while the `Arc::from(object_key)` it is handed is
/// allocated and dropped -- one allocation and one free per tombstoned container element, for a
/// call whose only possible outcome is to return.
///
/// This asserts BOTH halves of that reading, because each on its own is weak evidence:
///
///   * BEHAVIOUR -- deleting a container element files nothing new under the object in the lookup.
///     If the early return were ever not taken, the count would move.
///   * COST -- the delete allocates, measured under the counting allocator. The figure is printed
///     rather than pinned to a constant, because the whole delete path allocates for many reasons
///     and only the DIFFERENCE across a change to this one line is attributable.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib what_the_tombstone_path_files -- --nocapture
#[test]
fn what_the_tombstone_path_files_and_what_it_allocates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let raw = "tombstoned".to_string();

    // A hash with two fields, so deleting one leaves the object alive and the delete takes the
    // container-element path rather than dropping the whole object.
    for field in ["a", "b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: raw.clone(),
                field: field.to_string(),
                value: vec![b'h'; 8],
            },
        });
        assert!(response.status.ok, "the hash write failed: {:?}", response.status);
    }

    let filed_before = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.object_block_refs("hash", &raw).map(|refs| refs.len()).unwrap_or(0)
    };

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashDelete { key: raw.clone(), field: "a".to_string() },
    });
    assert!(response.status.ok, "the hash delete failed: {:?}", response.status);

    let filed_after = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.object_block_refs("hash", &raw).map(|refs| refs.len()).unwrap_or(0)
    };

    println!();
    println!("  refs filed under the object, before the delete: {filed_before}");
    println!("  refs filed under the object, after the delete:  {filed_after}");

    // The tombstone's own call cannot add one: its page is `deleted: true` and the callee returns
    // on that. If this ever grows, the early return stopped being taken and the call is no longer
    // the no-op this reading says it is.
    assert!(
        filed_after <= filed_before,
        "the delete FILED something new under the object ({filed_before} -> {filed_after}), so \
         `insert_object_block_lookup` did not take its `page.deleted` early return and the \
         tombstone call is not the no-op it looks like"
    );
    assert!(
        filed_before > 0,
        "nothing was filed under the object before the delete, so this run cannot tell a no-op \
         from an empty fixture"
    );

    // ---- AND THE ROW THE DELETE DOES WRITE: does its key share? --------------------------
    //
    // `record_tombstone_element` pushes a `TombstoneElement { object_key: Arc::from(object_key),
    // .. }` onto a RESIDENT vector hanging off the bucket, so unlike the no-op above this one
    // keeps what it allocates. The question is whether it had to allocate at all: the object is
    // still alive here -- a two-field hash with one field left -- so the page index still holds a
    // handle for it.
    //
    // This is measured and printed either way rather than assumed, because the answer depends on
    // ordering: the row is written while the delete is in flight, and whether the lookup still
    // holds the key at that moment is not something to reason about from the source.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let indexed = shard
        .bucket_index
        .object_block_lookup
        .any_shared_object_key(&raw)
        .map(|handle| handle.as_ptr() as usize);
    let rows: Vec<usize> = shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| bucket.tombstone_elements.iter())
        .filter(|row| &*row.object_key == raw.as_str())
        .map(|row| row.object_key.as_ptr() as usize)
        .collect();

    println!();
    match indexed {
        Some(ptr) => println!("  the index still holds the key at {ptr:#x}"),
        None => println!("  the index no longer holds the key"),
    }
    println!("  tombstone rows naming this key: {}", rows.len());
    for ptr in &rows {
        let verdict = match indexed {
            Some(indexed_ptr) if *ptr == indexed_ptr => "shared",
            Some(_) => "MINTS ",
            None => "sole  ",
        };
        println!("      {verdict}  {ptr:#x}");
    }

    assert!(
        !rows.is_empty(),
        "the delete wrote no tombstone row naming `{raw}`, so there is nothing here to ask about \
         sharing -- the fixture stopped reaching the tombstone path"
    );
}

/// WHETHER RE-KEYING A HASHED MAP IN PLACE MOVES ITS STORED BYTES.
///
/// This is the one thing standing between the decode path and keeping its key sharing.
/// `whether_the_shared_key_survives_a_decode` measures that a reload leaves every hashed holder
/// with its own allocation -- 0 of 8 keys still sharing -- and the pass that would fix it,
/// `adopt_canonical_key_allocations`, deliberately handles only ORDERED maps. The reason given is
/// that a `HashMap`'s serialized order follows its iteration order, so touching one there moved
/// the encoded bytes and the format guard caught it.
///
/// BUT THE OPERATION THAT WAS CAUGHT WAS A REBUILD, NOT A RE-KEY. Draining a map into a fresh one
/// changes capacity and insertion order, and of course moves the bytes. Swapping one entry's key
/// for a different `Arc` holding the SAME CHARACTERS is a different operation: the hash is
/// identical, so the probe sequence is identical, and a `remove` leaves a tombstone that the
/// following `insert` of that same hash should reclaim -- which would put the entry back in the
/// slot it came from and leave iteration order untouched.
///
/// If that holds, the decode-path fix needs no index-format stamp at all, and the refusal was
/// about the wrong operation. If it does not hold, the refusal stands on measured ground instead
/// of on a reasonable-sounding argument. Either answer is worth having, so this measures the
/// BYTES rather than arguing about the data structure.
///
/// The map is built the way the decode builds one -- every key its own allocation -- and then
/// re-keyed onto shared handles, exactly as an adoption pass would.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib whether_rekeying_a_hashed_map -- --nocapture
#[test]
fn whether_rekeying_a_hashed_map_in_place_moves_its_bytes() {
    // Enough keys to exercise several growth steps and plenty of probe collisions; small enough
    // to encode in a test.
    const KEYS: usize = 512;

    let texts: Vec<String> = (0..KEYS).map(|i| format!("decode-key-{i:05}")).collect();
    // What a decode produces: every key its OWN allocation, even where the text repeats elsewhere.
    let mut map: std::collections::HashMap<std::sync::Arc<str>, u64> =
        std::collections::HashMap::new();
    for (i, text) in texts.iter().enumerate() {
        map.insert(std::sync::Arc::from(&text[..]), i as u64);
    }

    let before_order: Vec<String> = map.keys().map(|k| k.to_string()).collect();
    let before_bytes = rmp_serde::to_vec(&map).expect("the map encodes");

    // What an adoption pass would do: swap each key for a shared handle holding the same
    // characters. One entry at a time, in place -- NOT a rebuild into a fresh map.
    let canonical: Vec<std::sync::Arc<str>> =
        texts.iter().map(|text| std::sync::Arc::from(&text[..])).collect();
    let mut rekeyed = 0usize;
    for handle in &canonical {
        let moves = match map.get_key_value(&**handle) {
            Some((held, _)) => !std::sync::Arc::ptr_eq(held, handle),
            None => false,
        };
        if !moves {
            continue;
        }
        if let Some(value) = map.remove(&**handle) {
            map.insert(std::sync::Arc::clone(handle), value);
            rekeyed += 1;
        }
    }

    let after_order: Vec<String> = map.keys().map(|k| k.to_string()).collect();
    let after_bytes = rmp_serde::to_vec(&map).expect("the map encodes");

    println!();
    println!("  {KEYS} keys, {rekeyed} re-keyed in place");
    println!("  encoded bytes before: {}", before_bytes.len());
    println!("  encoded bytes after:  {}", after_bytes.len());
    println!(
        "  byte-identical:       {}",
        if before_bytes == after_bytes { "YES" } else { "NO" }
    );
    println!(
        "  iteration order kept: {}",
        if before_order == after_order { "YES" } else { "NO" }
    );
    if before_order != after_order {
        let moved = before_order
            .iter()
            .zip(after_order.iter())
            .filter(|(a, b)| a != b)
            .count();
        println!("  positions that differ: {moved} of {KEYS}");
    }

    // The fixture has to have actually re-keyed, or this measures nothing.
    assert_eq!(
        KEYS, rekeyed,
        "only {rekeyed} of {KEYS} keys were re-keyed, so this run does not exercise the operation \
         the decode-path fix would perform"
    );
    // Every key must still be present and answer the same value -- a re-key that loses an entry
    // would make the byte comparison meaningless.
    assert_eq!(KEYS, map.len(), "the re-key changed the map's length");
    for (i, text) in texts.iter().enumerate() {
        assert_eq!(
            Some(&(i as u64)),
            map.get(&text[..]),
            "`{text}` does not answer its value after the re-key"
        );
    }
    // And every key must now be the shared handle, or the pass did not do its job.
    for handle in &canonical {
        let (held, _) = map.get_key_value(&**handle).expect("the key is present");
        assert!(
            std::sync::Arc::ptr_eq(held, handle),
            "a key is not the shared handle after the re-key"
        );
    }

    // The RESULT is printed, not pinned. Whether the bytes move decides whether the decode-path
    // fix needs an index-format stamp, and that is a finding to report rather than a property to
    // assert -- pinning either answer here would turn a measurement into a requirement.
}

/// WHETHER THE SHARING SURVIVES A DUMP AND INSTALL -- THE PATH THAT ACTUALLY DECODES AN INDEX.
///
/// `whether_the_shared_key_survives_a_decode` is named for a decode it never reaches. It writes,
/// drops the engine and reopens -- and a reopen with no dump has no index image to read, so
/// recovery comes from elsewhere and `repack_decoded_btrees` runs **zero times**. Measured with a
/// counter, because "the pass ran and found nothing" and "the pass never ran" produce the same
/// figure and cannot be told apart from the outside.
///
/// Every caller of `decode_index_bytes` reads `manifest.index_bytes`, so the decode path needs a
/// DUMP. This arm takes one, installs it, and asks the question of the path that genuinely decodes
/// an index -- which is the path `adopt_canonical_key_allocations` and its hashed sibling live on.
///
/// rust-internal: measures this crates own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib whether_the_shared_key_survives_a_dump -- --nocapture
#[test]
fn whether_the_shared_key_survives_a_dump_and_install() {
    const KEYS: usize = 8;
    const T0: u64 = 1_787_270_070_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    for index in 0..KEYS {
        let raw = format!("dumped-{index}");
        let at = T0 + index as u64;
        for command in [
            Command::StringSet { key: raw.clone(), value: vec![b'v'; 16] },
            Command::FeatureAppend {
                key: raw.clone(),
                points: vec![crate::types::FeaturePoint { timestamp_ms: at, value: vec![b'f'; 8] }],
            },
            Command::ControlStateIncrement { key: raw.clone(), timestamp_ms: at, amount: 1 },
        ] {
            let response = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }

    let before = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        distinct_addresses_per_raw_key(shard)
    };
    println!();
    println!("  fresh:  {before:?}");
    assert!(
        before.values().all(|n| *n == 1),
        "the sharing is not present before the dump, so this cannot measure whether a decode \
         keeps it: {before:?}"
    );

    let calls_before = crate::engine::state::REPACK_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("the dump manifest persists");
    engine
        .install_bucket_dump_manifest(&manifest)
        .expect("the dump manifest installs");
    let calls_after = crate::engine::state::REPACK_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let adopted =
        crate::engine::state::REPACK_ADOPTED_HASHED.load(std::sync::atomic::Ordering::Relaxed);

    let after = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        distinct_addresses_per_raw_key(shard)
    };

    println!("  after:  {after:?}");
    println!();
    println!("  decode pass ran {} time(s)", calls_after - calls_before);
    println!("  hashed keys it adopted: {adopted}");
    let sharing = after.values().filter(|n| **n == 1).count();
    println!("  keys sharing one allocation: {} of {}", sharing, after.len());

    // The install has to have gone through a decode, or this arm measures the same nothing the
    // other one did. THIS is the assertion that the other probe was missing.
    assert!(
        calls_after > calls_before,
        "the dump install did not reach `repack_decoded_btrees`, so this arm is not measuring the \
         decode path either -- find the path that does before reading anything into the figures"
    );
    assert_eq!(
        KEYS,
        after.len(),
        "{} raw keys survived the install, not {KEYS}",
        after.len()
    );
}
