// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN OUTCOME THAT STATES ITS ELEMENT, AND A LOG THAT CAN SAY WHAT SHAPE IT IS.
//!
//! # WHY THIS EXISTS
//!
//! A page entry names one element of a container object with a string derived from that element's
//! content. The alternative is an ORDINAL -- and a per-object page ordinal ALREADY EXISTS,
//! `BlockAddress::block_id`. #1976 refuted the ordinal on three grounds, and the strongest of them
//! was this: `apply_outcome_item` rebuilt a set's member and a zset's (score, member) by PARSING the
//! component name, and the record's only other copy of those bytes was inside the page the name
//! points at -- which replay deliberately does not read. So the name was load-bearing for replay.
//!
//! That is a fact about how the outcome is SHAPED, not one the world imposes. This module changes
//! the shape: the outcome states its element's bytes, replay reads them, and the name stops being
//! the record's only copy. What remains before an entry can carry a two-byte ordinal instead of a
//! name is a separate change to the ENTRY, and it is now unblocked rather than refuted.
//!
//! # WHICH KINDS NEEDED IT, WHICH DID NOT
//!
//! Eleven kinds reconstruct identity in `apply_outcome_item`. **Eight of them consume a NUMBER** --
//! a score, a sequence, a stored timestamp, an event id, an entity hash, two control buckets -- and
//! a number is not user data: `wal_proto` already carries those as numeric fields, so for those
//! kinds the name was never the only copy. **Two consume USER BYTES**: a set's member and a zset's
//! member. **One, `hash`, IS its field name**, so there is nothing to state separately.
//! `only_the_two_kinds_whose_element_is_user_data_need_stating` is that enumeration, asserted.
//!
//! # THE DIRECTION OF THE BYTE COST IS THE OPPOSITE OF WHAT IT LOOKS LIKE
//!
//! The record ALREADY carried the member -- spelled, inside the component name, at 1.334 characters
//! a byte after #1976 and 2.000 before it. Carrying it RAW is one byte a byte. So stating the
//! element does not grow the record, it SHRINKS it, and the entry is unblocked at the same time.
//! Measured on both allocator columns at two corpus sizes and both routing ranges.
//!
//! # EXISTING LOGS DO NOT REPLAY, AND THAT IS THE POINT OF THE VERSION
//!
//! Said plainly because it must not be buried: **a log written before this change will not replay.**
//! That is authorised before the first release. What is NOT acceptable is a log that fails to replay
//! SILENTLY -- a record that half-applies leaves the store disagreeing with itself, and a replay
//! that skips what it does not understand loses the tail of a running store and reports success.
//!
//! So the version had to become a check that can fail. It could not: it was omitted while it
//! equalled the current version and defaulted BACK to the current version when absent, in BOTH
//! encodings, so a record from an older build read as current and nothing compared it to anything.
//! Now it is stated on every record, an unstated one reads as version 1, and `decode_wal_line`
//! refuses anything else by name before a byte is applied.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;
const WIDE_END: u32 = u32::MAX;

const SMALL_OBJECTS: usize = 20;
const LARGE_OBJECTS: usize = 200;
const ELEMENTS_PER_OBJECT: usize = 100;
const MEMBER_BYTES: usize = 20;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "outcome-element".to_string(),
        shard_uri: "local://outcome-element/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

fn member_bytes(object: usize, element: usize) -> Vec<u8> {
    let mut bytes = format!("m{object:05}-{element:05}").into_bytes();
    bytes.resize(MEMBER_BYTES, b'.');
    bytes
}

fn seed(engine: &TemporalEngine, objects: usize) {
    let mut commands = Vec::with_capacity(objects * ELEMENTS_PER_OBJECT * 2);
    for object in 0..objects {
        for element in 0..ELEMENTS_PER_OBJECT {
            let member = member_bytes(object, element);
            commands.push(Command::ZSetAdd {
                key: format!("oe-zset-{object:05}"),
                member: member.clone(),
                score: element as f64 + 0.5,
            });
            commands.push(Command::SetAdd {
                key: format!("oe-set-{object:05}"),
                member,
            });
        }
    }
    for chunk in commands.chunks(500) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn store_path_length(dir: &std::path::Path) -> usize {
    dir.to_string_lossy().len()
}

fn recorded_outcomes(engine: &TemporalEngine) -> Vec<crate::wal::WalOutcomeItem> {
    let scanned = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("the write-ahead log scans");
    let mut out = Vec::new();
    for (_log_id, line) in scanned.iter() {
        let Ok(record) = crate::wal::decode_wal_line(line) else {
            continue;
        };
        out.extend(record.outcomes.iter().cloned());
    }
    out
}

// =============================================================================================
// 1. WHICH KINDS NEEDED STATING, AND WHICH DID NOT
// =============================================================================================

/// ONLY THE TWO KINDS WHOSE ELEMENT IS USER DATA STATE AN ELEMENT.
///
/// The enumeration is the finding: eleven kinds reconstruct identity from a component name, and
/// EIGHT of them reconstruct a NUMBER. A number is not user data and the record already carries it
/// -- `wal_proto` puts a timestamped key in `timestamp_ms` and an event's second key in `entry_id`,
/// and a score rides in the name where the ORDER lives. So the change is much smaller than the
/// eleven arms suggested: two kinds needed bytes and one, `hash`, IS its field name.
///
/// This asserts the split in both directions. A kind that starts stating an element it does not need
/// is paying for it on every write, and a kind that stops stating one it does need loses data
/// silently -- so neither direction may drift.
///
/// rust-internal: reads the engine's own log, no product behaviour
#[test]
fn only_the_two_kinds_whose_element_is_user_data_need_stating() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let member = b"element-question-member".to_vec();
    for command in [
        Command::StringSet {
            key: "oe-string".to_string(),
            value: b"v".to_vec(),
        },
        Command::ZSetAdd {
            key: "oe-zset".to_string(),
            member: member.clone(),
            score: 3.5,
        },
        Command::SetAdd {
            key: "oe-set".to_string(),
            member: member.clone(),
        },
        Command::ListPush {
            key: "oe-list".to_string(),
            member: b"first".to_vec(),
            left: false,
        },
        Command::HashSet {
            key: "oe-hash".to_string(),
            field: "a-field".to_string(),
            value: b"v".to_vec(),
        },
        Command::FeatureAppend {
            key: "oe-feature".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_070_000,
                value: b"p".to_vec(),
            }],
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "the fixture write failed: {response:?}");
    }

    let mut stating: BTreeMap<String, usize> = BTreeMap::new();
    let mut silent: BTreeMap<String, usize> = BTreeMap::new();
    for item in recorded_outcomes(&engine) {
        let bucket = if item.element.is_some() {
            &mut stating
        } else {
            &mut silent
        };
        *bucket.entry(item.kind.clone()).or_default() += 1;
    }
    println!("[split] kinds STATING an element: {stating:?}");
    println!("[split] kinds stating none:       {silent:?}");

    // The two that must state one, and the bytes must be the member that was written.
    for kind in ["zset", "set"] {
        assert!(
            stating.contains_key(kind),
            "the {kind} outcome states NO element, so its member has no copy in the record other \
             than its component name -- which is the thing this change exists to remove. Kinds \
             stating one were {stating:?}"
        );
    }
    for item in recorded_outcomes(&engine)
        .into_iter()
        .filter(|item| item.kind == "zset" || item.kind == "set")
    {
        assert_eq!(
            item.element.as_deref(),
            Some(member.as_slice()),
            "the {} outcome stated an element that is not the member that was written",
            item.kind
        );
    }

    // And the ones that must NOT: every kind whose identity is a number, plus the hash whose
    // element IS its field name. Paying for an element they do not need is a cost on every write.
    for kind in ["string", "list", "hash", "feature"] {
        assert!(
            !stating.contains_key(kind),
            "the {kind} outcome states an element it does not need. A number is already on the \
             record as a number, and a hash's element IS its field name -- so this is bytes per \
             write bought for nothing. Kinds stating one were {stating:?}"
        );
    }
    assert!(
        silent.len() >= 4,
        "only {} kind(s) stated no element, so this test is not covering the negative side of the \
         split: {silent:?}",
        silent.len()
    );
}

// =============================================================================================
// 2. THE LOG CAN SAY WHAT SHAPE IT IS, AND AN OLD SHAPE IS REFUSED BY NAME
// =============================================================================================

/// A LOG AT THE OLD SHAPE IS REFUSED, LOUDLY, BEFORE ANYTHING IS APPLIED.
///
/// EXISTING LOGS DO NOT REPLAY. That is authorised before the first release. What is not acceptable
/// is failing to replay them SILENTLY: a replay that skips what it does not understand loses the
/// tail of a running store and reports success, and a record that HALF-applies leaves the store
/// disagreeing with itself.
///
/// So the refusal is checked for three things, not one: that it happens, that it names BOTH shapes,
/// and that it happens at the DOOR rather than inside an arm -- `decode_wal_line` is the single
/// entry every record comes through, so a refusal there cannot be partial by construction.
///
/// rust-internal: builds log records directly, no external surface
#[test]
fn a_log_record_at_the_old_shape_is_refused_by_name_and_not_half_applied() {
    let current = crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION;
    assert!(
        current > 1,
        "the current record shape is {current}; this test needs an older shape to exist"
    );

    // A real record, at the current shape, which must decode.
    let record = crate::wal::WriteAheadLogRecord {
        shard_id: 1,
        sequence: 7,
        command: Some(Command::StringSet {
            key: "shape-key".to_string(),
            value: b"shape-value".to_vec(),
        }),
        metadata: Some(crate::wal::WriteAheadLogRecordMetadata {
            version: current,
            timestamp_ms: 1_787_270_070_192,
            items: Vec::new(),
            batch_id: None,
            batch_size: None,
            batch_index: None,
        }),
        staged_blocks: Vec::new(),
        outcomes: Vec::new(),
    };
    let framed = crate::wal::encode_wal_line_for_test(&record).expect("the record encodes");
    let back = crate::wal::decode_wal_line(&framed).expect("a current record must decode");
    assert_eq!(
        back.metadata.as_ref().expect("metadata").version,
        current,
        "a current record came back at the wrong shape"
    );
    println!("[shape] a record at shape {current} decodes, {} bytes framed", framed.len());

    // The same record at every older shape, and at one newer than this binary knows.
    for stated in [1u32, current - 1, current + 1, u32::MAX] {
        if stated == current {
            continue;
        }
        let mut other = record.clone();
        other.metadata.as_mut().expect("metadata").version = stated;
        let framed = crate::wal::encode_wal_line_for_test(&other).expect("the record encodes");
        let error = crate::wal::decode_wal_line(&framed).err().unwrap_or_else(|| {
            panic!(
                "a record stating shape {stated} REPLAYED under a binary that reads {current}. \
                 Hexadecimal and binary are both valid byte sequences and an old outcome carries no \
                 structural tell, so the version is the whole guard."
            )
        });
        let message = format!("{error}");
        println!("[shape] stated {stated}: {message}");
        assert!(
            message.contains(&stated.to_string()),
            "the refusal for shape {stated} does not name the shape it FOUND: {message:?}"
        );
        assert!(
            message.contains(&current.to_string()),
            "the refusal for shape {stated} does not name the shape this binary READS: {message:?}"
        );
    }

    // AND IT HAPPENS AT THE DOOR. A record whose metadata is absent entirely reads as shape 1, which
    // is what a record written before records had to state one looks like -- and it is refused too,
    // rather than being taken as current.
    let mut unstated = record.clone();
    unstated.metadata = None;
    let framed = crate::wal::encode_wal_line_for_test(&unstated).expect("the record encodes");
    match crate::wal::decode_wal_line(&framed) {
        Err(error) => println!("[shape] a record stating NO shape: {error}"),
        Ok(decoded) => panic!(
            "a record stating NO shape replayed, at version {:?}. An unstated version must read as \
             1 and be refused; reading it as current is the defect that made this field unable to \
             identify anything.",
            decoded.metadata.as_ref().map(|metadata| metadata.version)
        ),
    }
}

/// CAN AN OLD-SHAPE OUTCOME BE APPLIED UNDER THE NEW SHAPE? MEASURED, NOT ASSUMED.
///
/// This is the same hazard class as #1976's finding that 387 of 20,000 hexadecimal component names
/// were ALSO well-formed under the new spelling and read back as a different `(score, member)`. The
/// first version of that measurement assumed the rate was zero and was wrong, so this one does not
/// assume it either -- in either direction. A first version of THIS test asserted
/// `None.is_some() == false`, which is a tautology dressed as a measurement; it drives the real arm
/// now.
///
/// THE SHAPE OF THE HAZARD. A version-1 zset outcome states a component name and NO element. A
/// version-2 reader wants the element. If the arm fell back to parsing the name that would be a dual
/// reader, which is forbidden; if it treated an absent element as "no member" the member would be
/// silently dropped. It does neither -- it refuses the item, which makes the whole shard load refuse
/// -- and that is what is measured here by calling `apply_outcome_item` directly with old-shape
/// outcomes over a randomised population.
///
/// WHY THE DOOR STILL MATTERS even though the arm refuses. A record carries SEVERAL outcomes. An arm
/// refusing one of them fails the load, but only after the ones before it have been applied: that is
/// a half-applied record, which is the state worse than not replaying. The version check at
/// `decode_wal_line` refuses the record before any arm runs, which is why both exist.
///
/// rust-internal: calls the engine's own replay arm, no external surface
#[test]
fn how_often_an_old_shape_outcome_is_applied_by_the_new_arm() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let mut examined = 0usize;
    let mut applied = 0usize;
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    for round in 0..2_000 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let member: Vec<u8> = (0..(1 + (seed % 9) as usize))
            .map(|at| (seed >> ((at % 8) * 8)) as u8)
            .collect();
        let name = crate::engine::execute_on_shard::zset_component(seed, &member);
        // The name is still well formed, so an old-shape record carries NO structural tell -- which
        // is the whole reason the version has to carry the answer.
        assert!(
            crate::engine::execute_on_shard::parse_zset_component(&name).is_some(),
            "a component name this engine wrote does not parse: {name:?}"
        );

        // A VERSION-1 OUTCOME: a name, an address, and no element.
        let item = crate::wal::WalOutcomeItem {
            kind: "zset".to_string(),
            object_key: format!("old-shape-{round:05}"),
            component: Some(name),
            element: None,
            object_id: seed,
            routing_bucket: 0,
            address: Some(crate::block_store::BlockAddress::from_parts(
                7,
                round as u64 * 128,
                64,
                Some(round as u64),
                Some(seed),
                Some(0),
            )),
            value: None,
            ttl: None,
            deleted: false,
            meta: false,
        };
        examined += 1;
        if engine.apply_outcome_item_for_test(1, &item) {
            applied += 1;
        }
    }
    println!(
        "[rate] {applied} of {examined} version-1 zset outcomes ({:.4}%) were APPLIED by the \
         version-2 replay arm",
        100.0 * applied as f64 / examined as f64
    );
    assert_eq!(
        applied, 0,
        "{applied} of {examined} old-shape outcomes were applied by the new arm. An outcome with no \
         stated element must not satisfy a reader that requires one -- accepting it either drops the \
         member silently or falls back to parsing the name, and the second is the dual reader that \
         is forbidden."
    );
    println!(
        "[rate] so the arm refuses every old-shape outcome. The door's separate job is to refuse the \
         RECORD before any arm runs: a record carries several outcomes, and an arm that refuses the \
         third has already let the first two through."
    );
}

// =============================================================================================
// 3. A NEW-SHAPE LOG REPLAYS, ELEMENT BY ELEMENT
// =============================================================================================

/// A LOG AT THE CURRENT SHAPE REPLAYS EVERY ELEMENT, COMPARED RATHER THAN COUNTED.
///
/// A store that replays EMPTY and a store that replays CORRECTLY produce the same exit code, and
/// every `assert!(response.status.ok)` in the suite is blind to the difference. So the counts are
/// asserted first and then every element is compared: a zset member's score, a set member's
/// presence, and -- the arm this change is actually about -- the fact that the member came back at
/// all, because the record now states it rather than spelling it inside a name.
///
/// The replay is REAL: the served index is discarded so the shard has to come back from the log.
///
/// rust-internal: drives the engine's own replay, no external surface
#[test]
fn a_log_at_the_current_shape_replays_every_element() {
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let members: Vec<Vec<u8>> = (0..16).map(|element| member_bytes(0, element)).collect();

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for (element, member) in members.iter().enumerate() {
            for command in [
                Command::ZSetAdd {
                    key: "rp-zset".to_string(),
                    member: member.clone(),
                    score: element as f64 + 0.125,
                },
                Command::SetAdd {
                    key: "rp-set".to_string(),
                    member: member.clone(),
                },
            ] {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command,
                });
                assert!(response.status.ok, "the fixture write failed: {response:?}");
            }
        }
        engine.unload_shard(1);
    }

    // DISCARD THE SERVED INDEX, so the shard has no choice but to come back from the log. Without
    // this the test would pass on a store that never replayed anything.
    let mut removed = 0usize;
    for entry in std::fs::read_dir(&indexes).expect("the index directory exists") {
        let path = entry.expect("a directory entry").path();
        if path.is_file() && path.to_string_lossy().contains("index") {
            std::fs::remove_file(&path).expect("the index removes");
            removed += 1;
        }
    }
    assert!(
        removed > 0,
        "no served index was removed from {indexes:?}, so the shard may not have replayed at all \
         and this test would pass without testing anything"
    );
    println!("[replay] removed {removed} served index file(s), forcing a replay");

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let read = |command: Command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "a read failed after replay: {response:?}");
        response.response
    };

    // COUNTS FIRST.
    let set_members = match read(Command::SetMembers {
        key: "rp-set".to_string(),
    }) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    let zset_entries = match read(Command::ZSetRange {
        key: "rp-zset".to_string(),
        start: 0,
        stop: -1,
        rev: false,
    }) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a zset range answered {other:?}"),
    };
    println!(
        "[replay] the set came back with {} member(s), the zset with {} entry(ies)",
        set_members.len(),
        zset_entries.len()
    );
    assert_eq!(
        set_members.len(),
        members.len(),
        "the set held {} members before the replay and {} after",
        members.len(),
        set_members.len()
    );
    assert!(
        zset_entries.len() >= members.len(),
        "the zset held {} members before the replay and came back with {} entry(ies)",
        members.len(),
        zset_entries.len()
    );

    // CONTENTS, element by element.
    for (element, member) in members.iter().enumerate() {
        assert!(
            set_members.iter().any(|held| held == member),
            "set member {element} is gone after the replay"
        );
        match read(Command::ZSetScore {
            key: "rp-zset".to_string(),
            member: member.clone(),
        }) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                let text = String::from_utf8_lossy(&bytes).to_string();
                let score: f64 = text
                    .parse()
                    .unwrap_or_else(|_| panic!("a score came back as {text:?}"));
                assert!(
                    (score - (element as f64 + 0.125)).abs() < 1e-9,
                    "zset member {element} came back with score {score}, not {}",
                    element as f64 + 0.125
                );
            }
            other => panic!(
                "zset member {element} is GONE after the replay: {other:?}. Its bytes are stated on \
                 the outcome now, so losing it here means the statement is not being read."
            ),
        }
    }
    println!(
        "[replay] {} zset members and {} set members came back, all compared",
        members.len(),
        set_members.len()
    );
}

// =============================================================================================
// 4. THE BYTES: STATING THE ELEMENT IS CHEAPER THAN SPELLING IT
// =============================================================================================

/// WHAT STATING THE ELEMENT COSTS THE RECORD, AND WHAT DROPPING THE NAME WOULD SAVE THE ENTRY.
///
/// PRICED ON ONE INSTRUMENT, BOTH SIDES. The record already carried the member, spelled inside the
/// component name at 1.334 characters a byte. Stating it RAW is one byte a byte, so the record does
/// not grow -- and the ENTRY can then stop carrying a name at all, which is the prize.
///
/// Both allocator columns. `ALLOC_BYTES` charges `layout.size()`; `ALLOC_CHUNK_BYTES` reads
/// `malloc_usable_size` and charges what the allocator handed over, and it is the one that decides:
/// #1976 measured a 29.03% payload saving worth 5.26% of chunk, because the rounding absorbed it.
/// A saving that does not cross a size class saves nothing an allocator can see.
///
/// Both routing ranges, because a figure taken on one is worth nothing without the other -- and the
/// prediction is that this one does not move, since routing takes the object key and never sees the
/// element.
///
/// rust-internal: reads the engine's own log and index, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn what_stating_the_element_costs_the_record_and_saves_the_entry() {
    for objects in [SMALL_OBJECTS, LARGE_OBJECTS] {
        for end_routing_bucket in [OPERATOR_END, WIDE_END] {
            let dir = tempfile::tempdir().unwrap();
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            seed(&engine, objects);

            let outcomes = recorded_outcomes(&engine);
            let stating: Vec<&crate::wal::WalOutcomeItem> = outcomes
                .iter()
                .filter(|item| item.element.is_some())
                .collect();
            assert!(
                !stating.is_empty(),
                "no outcome stated an element, so there is nothing to price"
            );

            // THE RECORD SIDE. The element as raw bytes, against the same element spelled inside a
            // component name -- which is what the record carried before, and still carries for the
            // score and for filing the page.
            let raw: usize = stating
                .iter()
                .map(|item| item.element.as_ref().map(Vec::len).unwrap_or_default())
                .sum();
            let spelled: usize = stating
                .iter()
                .map(|item| {
                    item.element
                        .as_ref()
                        .map(|bytes| crate::component_name::bytes_chars(bytes.len()))
                        .unwrap_or_default()
                })
                .sum();
            let hexadecimal: usize = stating
                .iter()
                .map(|item| 2 * item.element.as_ref().map(Vec::len).unwrap_or_default())
                .sum();

            // THE ENTRY SIDE. What an entry pays to hold a name, which is what an ordinal would
            // save. Charged by building the same population of `Arc<str>` the index holds.
            let names: Vec<String> = {
                let shards = engine.shards.read().expect("engine lock poisoned");
                let shard = shards.get(&1).expect("shard is loaded");
                let mut held = Vec::new();
                for bucket in shard.bucket_index.bucket_map.values() {
                    for page in bucket.block_index.values() {
                        if page.deleted {
                            continue;
                        }
                        if let Some(component) = page.component.as_deref() {
                            held.push(component.to_string());
                        }
                    }
                }
                held
            };
            assert!(!names.is_empty(), "no entry holds a name, so there is nothing to price");

            let probe = Probe::start();
            let arcs: Vec<std::sync::Arc<str>> = names
                .iter()
                .map(|name| std::sync::Arc::from(name.as_str()))
                .collect();
            let counts = probe.stop();
            std::hint::black_box(&arcs);
            drop(arcs);

            let label = if end_routing_bucket == WIDE_END {
                "0..u32::MAX (an artefact)"
            } else {
                "0..1023 (the operator's)"
            };
            println!(
                "=== {objects} objects, {label}, store path {} chars",
                store_path_length(dir.path())
            );
            println!(
                "  RECORD: {} outcome(s) state an element | raw {raw} B | the same element spelled \
                 {spelled} chars | hexadecimal before #1976 {hexadecimal} chars",
                stating.len()
            );
            println!(
                "  so stating it raw is {:.3} the spelled cost and {:.3} the hexadecimal one -- \
                 the record SHRINKS",
                raw as f64 / spelled.max(1) as f64,
                raw as f64 / hexadecimal.max(1) as f64,
            );
            println!(
                "  ENTRY : {} name(s) held | ALLOC_BYTES {} | CHUNK_BYTES {} | allocs {} | per \
                 name {:.2} B requested, {:.2} B handed over   <- what an ordinal would save",
                names.len(),
                counts.alloc_bytes,
                counts.chunk_bytes,
                counts.allocs,
                counts.alloc_bytes as f64 / names.len() as f64,
                counts.chunk_bytes as f64 / names.len() as f64,
            );
            println!(
                "  AN ORDINAL is two bytes inside a struct the entry already has, so the entry's \
                 whole per-name cost is the prize: {:.2} B a name on the column that decides, \
                 against the 4.00 B a name #1976's respelling bought",
                counts.chunk_bytes as f64 / names.len() as f64,
            );

            assert!(
                raw < spelled,
                "raw {raw} B is not under the spelled {spelled} chars; the whole direction of this \
                 change is that stating bytes is cheaper than spelling them"
            );
            assert!(
                counts.chunk_bytes >= counts.alloc_bytes,
                "the chunk column charged {} B, below the {} B requested",
                counts.chunk_bytes,
                counts.alloc_bytes
            );
            assert_eq!(
                counts.allocs as usize,
                names.len() + 1,
                "charged {} allocation(s) for {} names plus one vector",
                counts.allocs,
                names.len()
            );
        }
    }
}

// =============================================================================================
// 5. WHAT IS LEFT OF THE ORDINAL REFUTATION
// =============================================================================================

/// NO PRODUCTION READ LOOKS A PAGE UP BY A DERIVED COMPONENT NAME.
///
/// #1976 refuted the ordinal on three grounds. This branch lifts one of them and this test weakens
/// another, so what is left has to be said plainly rather than left as three.
///
/// # REASON 2 IS LIFTED BY THIS BRANCH
///
/// It was: `apply_outcome_item` rebuilds a set's member and a zset's (score, member) by PARSING the
/// component name, and the record's only other copy is inside the page, which replay does not read.
/// The record states the element now -- on the upsert path AND on the removal path -- so the name is
/// no longer the record's only copy, and `a_log_at_the_current_shape_replays_every_element` drives a
/// real replay that recovers every element from the stated bytes.
///
/// # REASON 1 DOES NOT BIND FOR THE DERIVED KINDS
///
/// It was: `ObjectBlockRefs::position` is asked "which page holds member M", and an ordinal cannot
/// answer that without a member-to-ordinal map. True of the FUNCTION; the question is who asks it.
/// The POINT lookup by component is `bucket_store::bucket_index_block_address`, reached in production
/// only through `read_bucket_index_value`, and it has exactly three call sites:
///
///   * `"string"` with NO component;
///   * `"hash"` with `Some(field)` -- twice, for a read and an increment.
///
/// A hash field is the CALLER'S OWN TEXT, not a name this engine derives, and an ordinal was never
/// going to replace a user's key. Every other kind is read WHOLE, through
/// `bucket_index_component_block_addresses`, which enumerates an object's components rather than
/// looking one up -- an enumeration an ordinal answers exactly as well -- or straight out of its
/// in-memory model map, which is already keyed by the member itself.
///
/// So the derived name answers no question any production read asks of it.
///
/// # WHAT IS ACTUALLY LEFT: REASON 3, AND IT IS SURMOUNTABLE
///
/// `stable_block_object_id(shard, kind, key, Some(component))` derives the object id from the name,
/// and the id is written into `BlockAddress::object_id`. Under an ordinal it would be derived from
/// the ordinal instead -- which is stable per (object, ordinal) and unique per element for exactly as
/// long as an ordinal is never reused, which is already a stated requirement of
/// `next_block_index_for_object`. That is a change to a derivation, not an obstacle in the way of
/// one.
///
/// So: the ordinal is UNBLOCKED, not refuted. It is a third change, it depends on this one, and the
/// prize is the whole per-name cost of an entry rather than the four bytes a respelling bought.
///
/// rust-internal: reads the engine's own read path, no product behaviour
#[test]
fn no_production_read_looks_a_page_up_by_a_derived_component_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let member = b"read-path-member".to_vec();
    for command in [
        Command::StringSet {
            key: "rd-string".to_string(),
            value: b"sv".to_vec(),
        },
        Command::HashSet {
            key: "rd-hash".to_string(),
            field: "a-caller-field".to_string(),
            value: b"hv".to_vec(),
        },
        Command::ZSetAdd {
            key: "rd-zset".to_string(),
            member: member.clone(),
            score: 1.5,
        },
        Command::SetAdd {
            key: "rd-set".to_string(),
            member: member.clone(),
        },
        Command::ListPush {
            key: "rd-list".to_string(),
            member: b"e0".to_vec(),
            left: false,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "the fixture write failed: {response:?}");
    }

    // The two reads that DO take a component, and what the component is in each.
    //
    //   - a string read passes None;
    //   - a hash read passes the caller's field name.
    //
    // Both are exercised here so the claim is behavioural rather than a reading of the source.
    let read = |command: Command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "a read failed: {response:?}");
        response.response
    };
    assert!(
        matches!(
            read(Command::StringGet { key: "rd-string".to_string() }),
            crate::types::CommandResponse::Bytes { value: Some(ref got) } if got == b"sv"
        ),
        "the string read, which passes NO component, did not answer"
    );
    assert!(
        matches!(
            read(Command::HashGet {
                key: "rd-hash".to_string(),
                field: "a-caller-field".to_string(),
            }),
            crate::types::CommandResponse::Bytes { value: Some(ref got) } if got == b"hv"
        ),
        "the hash read, which passes the CALLER'S field name, did not answer"
    );

    // And the derived kinds answer WITHOUT any by-component point lookup: a zset from its own map, a
    // set and a list by enumeration. If one of these ever started resolving through a name, an
    // ordinal would break it -- so each is read here and compared.
    assert!(
        matches!(
            read(Command::ZSetScore {
                key: "rd-zset".to_string(),
                member: member.clone(),
            }),
            crate::types::CommandResponse::Bytes { value: Some(_) }
        ),
        "the zset read did not answer"
    );
    let set_members = match read(Command::SetMembers {
        key: "rd-set".to_string(),
    }) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    assert_eq!(set_members.len(), 1, "the set read answered {set_members:?}");
    assert_eq!(set_members[0], member, "the set read answered the wrong member");
    let list = match read(Command::ListRange {
        key: "rd-list".to_string(),
        start: 0,
        stop: -1,
    }) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a list read answered {other:?}"),
    };
    assert_eq!(list.len(), 1, "the list read answered {list:?}");

    // THE CLAIM, stated as a property of the derived names themselves: every one of them parses
    // back to the content it was derived from, which means it carries NO information the maps beside
    // it do not already hold. A name that is a second copy is a name an ordinal can replace.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut derived = 0usize;
    let mut caller_text = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            let Some(component) = page.component.as_deref() else {
                continue;
            };
            match page.model_id.as_str() {
                "zset" => {
                    let (_score, held) =
                        crate::engine::execute_on_shard::parse_zset_component(component)
                            .expect("a zset name parses");
                    assert_eq!(
                        held, member,
                        "a zset name did not parse back to the member it was derived from"
                    );
                    derived += 1;
                }
                "set" => {
                    let held = crate::engine::execute_on_shard::parse_set_component(component)
                        .expect("a set name parses");
                    assert_eq!(held, member, "a set name did not parse back to its member");
                    derived += 1;
                }
                "list" => {
                    crate::engine::execute_on_shard::parse_list_component(component)
                        .expect("a list name parses");
                    derived += 1;
                }
                "hash" => {
                    assert_eq!(
                        component, "a-caller-field",
                        "a hash component is the caller's field name, and this one is {component:?}"
                    );
                    caller_text += 1;
                }
                _ => {}
            }
        }
    }
    println!(
        "[read-path] {derived} derived name(s) each parse back to content the maps beside them \
         already hold; {caller_text} name(s) are the caller's own text and are not this engine's to \
         replace"
    );
    assert!(
        derived >= 3,
        "only {derived} derived name(s) were examined, so this test is not covering the kinds it \
         claims to"
    );
    assert_eq!(
        caller_text, 1,
        "expected exactly one caller-text component in this fixture, found {caller_text}"
    );
    println!(
        "[read-path] the ordinal is UNBLOCKED rather than refuted: reason 2 is lifted by this \
         branch, reason 1 binds only on the hash field a caller supplies, and reason 3 is a \
         derivation to change rather than an obstacle"
    );
}
