// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A decode's repack pass must not bring a stored table into being.
//!
//! WHY THIS EXISTS, as a measurement rather than a worry. `repack_decoded_btrees` reached four
//! tables through accessors that CREATE on first use -- `u64s_in`, `counters_in`, and
//! `objects.entry(kind).or_default()` twice. An absent domain or kind is a table that is NOT in the
//! stored map; an empty one is a table that IS. So on a shard missing any of the four, a pass whose
//! whole job is to move entries between B-tree nodes invented a table and the stored bytes moved
//! with no contents moving:
//!
//!     side_tables at the encode BEFORE the repack:  4 entries, no `expiry`
//!     side_tables at the encode AFTER  the repack:  5 entries, an empty `expiry`
//!     decoded payload: 109,291 -> 109,306 bytes
//!
//! `a_decode_puts_back_exactly_the_series_that_were_encoded` caught the symptom -- it compares the
//! bytes before and after on one state -- and reported it as the stored form having moved, which
//! sent three hypotheses the wrong way before the cause came out. This test states the RULE: the
//! set of tables the pass hands back is the set it was given.
//!
//! TWO PARTS, BECAUSE A SEED CANNOT REACH THREE OF THE FOUR. The bug is only visible when a table
//! is ABSENT, and measured over three seeds -- lists only, strings only, features only -- the only
//! one ever absent after a decode is `side_tables/expiry`. The string kind, the feature kind and
//! the control-state counters are materialised by every decode, so asserting coverage of their
//! reaches from seeded shards would be a claim this test cannot support. Part 2 therefore removes
//! each table by hand and asks the same question, which is the rule at the level the rule lives:
//! the pass must not create a table that is not there, whoever took it out.
//!
//! WHY THE KEY SETS AND NOT ONLY THE BYTES. The byte equality is the property the stored form owes;
//! the key sets say WHICH table moved when it breaks, so the next person does not have to
//! decompress a zstd frame and read msgpack map headers to find out. Both are asserted.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 1;

/// The four tables the pass used to reach through a creating accessor.
const TARGETS: [&str; 4] = [
    "side_tables/expiry",
    "side_tables/control_state_counters",
    "objects/string",
    "objects/feature",
];

/// A shard on the shipped routing range, loaded empty.
fn engine_on_shard(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(SHARD);
    engine
}

/// Which of the four tables are ABSENT in this state.
///
/// Named, not counted: when an assertion below fails, the name is the whole diagnosis.
fn absent_tables(state: &crate::engine::state::ShardState) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    if crate::engine::state::u64s_ref(&state.side_tables, crate::engine::state::SideTable::Expiry)
        .is_none()
    {
        out.insert("side_tables/expiry".to_string());
    }
    if crate::engine::state::counters_ref(
        &state.side_tables,
        crate::engine::state::SideTable::ControlStateCounters,
    )
    .is_none()
    {
        out.insert("side_tables/control_state_counters".to_string());
    }
    if !state
        .objects
        .contains_key(&crate::engine::storage_bucket_internals::StoredModelKind::String)
    {
        out.insert("objects/string".to_string());
    }
    if !state
        .objects
        .contains_key(&crate::engine::storage_bucket_internals::StoredModelKind::Feature)
    {
        out.insert("objects/feature".to_string());
    }
    out
}

/// Every stored table of a state, by name, so a failure says which one appeared.
fn table_names(state: &crate::engine::state::ShardState) -> Vec<String> {
    let mut names: Vec<String> = state
        .side_tables
        .keys()
        .map(|domain| format!("side_tables/{domain:?}"))
        .chain(state.objects.keys().map(|kind| format!("objects/{kind:?}")))
        .collect();
    names.sort();
    names
}

/// Take one of the four tables out of a decoded state.
fn remove_table(state: &mut crate::engine::state::ShardState, target: &str) {
    match target {
        "side_tables/expiry" => {
            state
                .side_tables
                .remove(&crate::engine::state::SideTable::Expiry);
        }
        "side_tables/control_state_counters" => {
            state
                .side_tables
                .remove(&crate::engine::state::SideTable::ControlStateCounters);
        }
        "objects/string" => {
            state
                .objects
                .remove(&crate::engine::storage_bucket_internals::StoredModelKind::String);
        }
        "objects/feature" => {
            state
                .objects
                .remove(&crate::engine::storage_bucket_internals::StoredModelKind::Feature);
        }
        other => panic!("unknown target {other}"),
    }
}

/// Three seeds, so the states under test differ in contents as well as in shape.
fn seed(engine: &TemporalEngine, kind: &str) {
    match kind {
        "lists only" => {
            for i in 0..64u64 {
                let response = engine.execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::ListPush {
                        key: "l0".to_string(),
                        member: vec![b'l'; 16],
                        left: false,
                    },
                });
                assert!(response.status.ok, "list seed must ack at {i}");
            }
        }
        "strings only" => {
            for i in 0..64usize {
                let response = engine.execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringSet {
                        key: format!("s{i}"),
                        value: vec![b'v'; 32],
                    },
                });
                assert!(response.status.ok, "string seed must ack at {i}");
            }
        }
        "features only" => {
            for k in 0..3usize {
                let points = (0..64u64)
                    .map(|t| crate::types::FeaturePoint {
                        timestamp_ms: 1_700_000_000_000 + t,
                        value: vec![b'f'; 32],
                    })
                    .collect::<Vec<_>>();
                let response = engine.execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::FeatureAppend {
                        key: format!("f{k}"),
                        points,
                    },
                });
                assert!(response.status.ok, "feature seed must ack at {k}");
            }
        }
        other => panic!("unknown seed {other}"),
    }
}

/// The encoded index of a freshly seeded shard.
fn seeded_index_bytes(arm: &str) -> Vec<u8> {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on_shard(dir.path());
    seed(&engine, arm);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    crate::engine::encode_index_bytes(shard)
}

/// The shared question: repack this state and say nothing appeared and no byte moved.
///
/// `must_stay_absent` is checked one table at a time rather than as a set, so restoring any single
/// creating accessor fails with that table's name instead of four failing as one byte comparison.
fn the_repack_changes_no_table(
    label: &str,
    mut subject: crate::engine::state::ShardState,
    must_stay_absent: &std::collections::BTreeSet<String>,
) {
    assert!(
        !must_stay_absent.is_empty(),
        "{label}: nothing is absent, so this case cannot exercise a creating reach -- the case, \
         not the rule, needs changing"
    );
    let names_before = table_names(&subject);
    let before = crate::engine::encode_index_bytes(&subject);

    crate::engine::state::repack_decoded_btrees(&mut subject);

    for table in must_stay_absent {
        assert!(
            absent_tables(&subject).contains(table),
            "{label}: the repack brought {table} into being -- an absent table is not in the \
             stored map and an empty one is, so this moves the stored bytes"
        );
    }
    assert_eq!(
        names_before,
        table_names(&subject),
        "{label}: the repack changed which tables the state holds"
    );

    // AND THE BYTES, which is the property the stored form actually owes. zstd is deterministic
    // over the same input, so equal containers mean equal payloads.
    let after = crate::engine::encode_index_bytes(&subject);
    assert_eq!(
        before.len(),
        after.len(),
        "{label}: the encoded length moved across the repack"
    );
    assert_eq!(
        before, after,
        "{label}: repacking changed the bytes the state encodes to"
    );

    // AND IDEMPOTENT, since the pass is reached down more than one path.
    crate::engine::state::repack_decoded_btrees(&mut subject);
    assert_eq!(
        before,
        crate::engine::encode_index_bytes(&subject),
        "{label}: repacking twice is not the same as repacking once"
    );
}

#[test]
fn the_repack_brings_no_table_into_being() {
    // ---- PART 1: the states a decode actually produces. ----
    let mut naturally_absent: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();

    for arm in ["lists only", "strings only", "features only"] {
        let bytes = seeded_index_bytes(arm);
        let subject = crate::engine::decode_index_bytes_inner(&bytes)
            .unwrap_or_else(|error| panic!("{arm}: the index must decode: {error}"));
        let absent = absent_tables(&subject);
        println!("{arm}: absent after a decode: {absent:?}");
        naturally_absent.extend(absent.iter().cloned());
        the_repack_changes_no_table(arm, subject, &absent);
    }

    // THE REAL CASE IS REACHABLE, asserted rather than assumed: this is the one a seeded shard
    // produces on its own, and it is the one that was failing.
    assert!(
        naturally_absent.contains("side_tables/expiry"),
        "no seed left the expiry domain absent, so part 1 no longer covers the case that was \
         broken: it was reachable from an ordinary shard, which is why it mattered"
    );

    // ---- PART 2: the three a decode always materialises, removed by hand. ----
    //
    // Measured: across all three seeds above, `objects/string`, `objects/feature` and
    // `side_tables/control_state_counters` are present after every decode. No seed reaches them, so
    // part 1 cannot speak for their reaches and does not pretend to. Removing the table asks the
    // same question at the level the rule lives.
    for target in TARGETS {
        let bytes = seeded_index_bytes("strings only");
        let mut subject = crate::engine::decode_index_bytes_inner(&bytes)
            .unwrap_or_else(|error| panic!("{target}: the index must decode: {error}"));
        remove_table(&mut subject, target);
        let absent = absent_tables(&subject);
        assert!(
            absent.contains(target),
            "{target}: removing it did not make it absent, so this case tests nothing"
        );
        let mut one: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        one.insert(target.to_string());
        the_repack_changes_no_table(&format!("removed {target}"), subject, &one);
    }
}
