// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WRITE INTO A SERIES THAT ALREADY EXISTS BORROWS ITS KEY INSTEAD OF MINTING ANOTHER.
//!
//! `entry(Arc::from(key))` builds the key's `Arc` BEFORE it looks the key up: `Arc::from(&str)`
//! always mallocs and copies, and `entry()` takes its key by value. Every counter increment, seen
//! check, token-bucket take, selection set, distinct add and sequence append into an object that
//! was already there allocated the key's characters, copied them in, and dropped them again. The
//! arms now go through `slot_for_write` / `store_for_write`, which find an existing entry through a
//! borrow and mint only for a key the map does not hold.
//!
//! WHAT MOVES: allocation count and bytes of those writes, by exactly the one key `Arc` each. WHAT
//! DOES NOT: the answers, the staged outcomes and the durable bytes -- the key text is the same text
//! in the same maps, only its allocation moved.
//!
//! THREE GUARDS, BECAUSE THEY FAIL ON DISJOINT MUTATIONS.
//!   * `a_slot_keeps_the_key_it_has_and_mints_the_key_it_lacks` pins the helpers: a hit hands back the
//!     SAME `Arc` the map already held (pointer equality, not text equality), a miss inserts one.
//!   * `a_history_through_every_borrowing_arm_answers_what_an_oracle_answers` drives each arm
//!     through the command surface, interleaving a first write (a miss) with later ones (hits), and
//!     compares every answer with a model written here, before and after a reload.
//!   * `no_arm_mints_a_key_before_it_looks_the_key_up` is a source check, comments excluded: the one block
//!     a write saves needs no calibrated ceiling, only that the old spelling is gone from the arms.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::product_model::{slot_for_write, store_for_write};
use crate::engine::state::ModelKey;
use crate::types::{CommandResponse, ControlStateSelectionType, SequenceFeatureRow};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

#[test]
fn a_slot_keeps_the_key_it_has_and_mints_the_key_it_lacks() {
    let mut map: HashMap<ModelKey, Vec<u32>> = HashMap::new();

    // A miss mints: the map now holds the key and a default value.
    slot_for_write(&mut map, "series").push(1);
    assert_eq!(map.len(), 1);
    let (held, _) = map.get_key_value("series").unwrap();
    let held = Arc::clone(held);
    // `held` plus the map's own handle.
    assert_eq!(Arc::strong_count(&held), 2);

    // A hit reaches the SAME allocation: no new key was built and swapped in. Pointer equality,
    // because text equality cannot tell a kept key from a rebuilt copy of it.
    slot_for_write(&mut map, "series").push(2);
    let (after, values) = map.get_key_value("series").unwrap();
    assert!(Arc::ptr_eq(&held, after), "a write at an existing key replaced its key allocation");
    assert_eq!(values, &vec![1, 2]);
    assert_eq!(Arc::strong_count(&held), 2);

    // The overwrite twin: a hit replaces the value under the same key, a miss mints.
    let mut buckets: HashMap<ModelKey, (f64, u64)> = HashMap::new();
    store_for_write(&mut buckets, "b", (1.0, 10));
    let (first, _) = buckets.get_key_value("b").unwrap();
    let first = Arc::clone(first);
    store_for_write(&mut buckets, "b", (2.0, 20));
    let (second, value) = buckets.get_key_value("b").unwrap();
    assert!(Arc::ptr_eq(&first, second), "an overwrite replaced its key allocation");
    assert_eq!(*value, (2.0, 20));
    assert_eq!(buckets.len(), 1);
}

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
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

fn integer(response: CommandResponse) -> i64 {
    match response {
        CommandResponse::Integer { value } => value,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn counter_series(engine: &TemporalEngine, key: &str) -> BTreeMap<u64, i64> {
    match run(
        engine,
        Command::ControlStateDetail {
            key: key.to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: None,
        },
    ) {
        CommandResponse::FeaturePoints { points } => points
            .into_iter()
            .map(|point| {
                (
                    point.timestamp_ms,
                    String::from_utf8(point.value).unwrap().parse::<i64>().unwrap(),
                )
            })
            .collect(),
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }
}

fn row(timestamp_ms: u64, gid: u64) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms,
        gid,
        action_type: 1,
        duration: 5,
        author_id: 9,
    }
}

/// Everything the arms can be asked afterwards, in one comparable value.
#[derive(Debug, PartialEq)]
struct Answers {
    counters: BTreeMap<String, BTreeMap<u64, i64>>,
    windows: Vec<i64>,
    seen_cards: Vec<i64>,
    selections: Vec<Option<Vec<u8>>>,
    distinct: Vec<i64>,
    sequences: Vec<Vec<(u64, u64)>>,
}

const KEYS: [&str; 3] = ["alpha", "beta-with-a-longer-key-name", "g"];

fn ask(engine: &TemporalEngine) -> Answers {
    let mut counters = BTreeMap::new();
    for key in KEYS {
        counters.insert(key.to_string(), counter_series(engine, key));
    }
    let mut windows = Vec::new();
    let mut seen_cards = Vec::new();
    let mut selections = Vec::new();
    let mut distinct = Vec::new();
    let mut sequences = Vec::new();
    for key in KEYS {
        for aggregator in ["sum", "min", "max", "first", "last", "events", "len", "count", "other"] {
            windows.push(integer(run(
                engine,
                Command::ControlStateQuery {
                    key: key.to_string(),
                    start_ms: 1_050,
                    end_ms: 1_400,
                    aggregator: aggregator.to_string(),
                },
            )));
        }
        seen_cards.push(integer(run(engine, Command::SeenCard { key: key.to_string() })));
        selections.push(match run(
            engine,
            Command::ControlStateSelectionQuery { key: key.to_string() },
        ) {
            CommandResponse::Bytes { value } => value,
            other => panic!("expected bytes, got {other:?}"),
        });
        for (start, end) in [(0, u64::MAX), (1_100, 1_250), (1_300, 1_300), (5_000, 6_000)] {
            distinct.push(integer(run(
                engine,
                Command::ControlStateQuery {
                    key: key.to_string(),
                    start_ms: start,
                    end_ms: end,
                    aggregator: "change".to_string(),
                },
            )));
        }
        sequences.push(match run(
            engine,
            Command::SequenceQuery {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: 1_000,
                filters: Vec::new(),
            },
        ) {
            CommandResponse::SequenceRows { rows } => {
                rows.into_iter().map(|row| (row.timestamp_ms, row.gid)).collect()
            }
            other => panic!("expected rows, got {other:?}"),
        });
    }
    Answers {
        counters,
        windows,
        seen_cards,
        selections,
        distinct,
        sequences,
    }
}

/// The same history, applied to plain maps and sets.
#[derive(Default)]
struct Model {
    counters: BTreeMap<String, BTreeMap<u64, i64>>,
    seen: BTreeMap<String, BTreeSet<Vec<u8>>>,
    selection: BTreeMap<String, (u64, Vec<u8>)>,
    changes: BTreeMap<String, BTreeMap<u64, BTreeSet<Vec<u8>>>>,
    sequences: BTreeMap<String, BTreeMap<u64, u64>>,
}

fn model_window(series: &BTreeMap<u64, i64>, aggregator: &str) -> i64 {
    let values: Vec<i64> = series.range(1_050..=1_400).map(|(_, value)| *value).collect();
    match aggregator {
        "min" => values.iter().copied().min().unwrap_or(0),
        "max" => values.iter().copied().max().unwrap_or(0),
        "first" => values.first().copied().unwrap_or(0),
        "last" => values.last().copied().unwrap_or(0),
        "events" | "len" => values.len() as i64,
        _ => values.iter().sum(),
    }
}

#[test]
fn a_history_through_every_borrowing_arm_answers_what_an_oracle_answers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut model = Model::default();
    let mut rng = Rng(0x5eed);
    let mut writes = 0usize;

    for step in 0..600u64 {
        let key = KEYS[rng.next(KEYS.len() as u64) as usize];
        let at = 1_000 + rng.next(60) * 10;
        match rng.next(7) {
            0 => {
                let amount = rng.next(9) as i64 + 1;
                run(
                    &engine,
                    Command::ControlStateIncrement {
                        key: key.to_string(),
                        timestamp_ms: at,
                        amount,
                    },
                );
                *model
                    .counters
                    .entry(key.to_string())
                    .or_default()
                    .entry(at)
                    .or_default() += amount;
            }
            1 => {
                // A precision folds the timestamp into a coarser bucket: the bucket is what is stored.
                let amount = rng.next(5) as i64 + 1;
                run(
                    &engine,
                    Command::ControlStateIncrementWithOptions {
                        key: key.to_string(),
                        timestamp_ms: at,
                        amount,
                        precision_ms: Some(50),
                        ttl_ms: None,
                    },
                );
                *model
                    .counters
                    .entry(key.to_string())
                    .or_default()
                    .entry(at - at % 50)
                    .or_default() += amount;
            }
            2 => {
                let member = format!("m{}", rng.next(12)).into_bytes();
                let duplicate = !model
                    .seen
                    .entry(key.to_string())
                    .or_default()
                    .insert(member.clone());
                let answered = integer(run(
                    &engine,
                    Command::SeenCheck {
                        key: key.to_string(),
                        member,
                        window_ms: 3_600_000,
                    },
                ));
                assert_eq!(answered, i64::from(duplicate), "step {step}: a seen check answered wrongly");
            }
            3 => {
                let value = format!("v{step}").into_bytes();
                let first = rng.next(2) == 0;
                run(
                    &engine,
                    Command::ControlStateSelectionSet {
                        key: key.to_string(),
                        value: value.clone(),
                        occur_time_ms: at,
                        ttl_ms: 0,
                        selection_type: if first {
                            ControlStateSelectionType::First
                        } else {
                            ControlStateSelectionType::Last
                        },
                    },
                );
                let wins = model.selection.get(key).map_or(true, |(held, _)| {
                    if first {
                        at < *held
                    } else {
                        at > *held
                    }
                });
                if wins {
                    model.selection.insert(key.to_string(), (at, value));
                }
            }
            4 => {
                let value = format!("d{}", rng.next(20)).into_bytes();
                run(
                    &engine,
                    Command::ControlStateChangeAdd {
                        key: key.to_string(),
                        timestamp_ms: at,
                        value: value.clone(),
                        precision_ms: None,
                        ttl_ms: None,
                    },
                );
                model
                    .changes
                    .entry(key.to_string())
                    .or_default()
                    .entry(at)
                    .or_default()
                    .insert(value);
            }
            5 => {
                run(
                    &engine,
                    Command::SequenceAdd {
                        key: key.to_string(),
                        rows: vec![row(at + 1, step)],
                    },
                );
                model
                    .sequences
                    .entry(key.to_string())
                    .or_default()
                    .insert(at + 1, step);
            }
            _ => {
                // A token bucket with no refill is a countdown whatever the clock says.
                let tokens = 1.0;
                let taken = match run(
                    &engine,
                    Command::BucketTake {
                        key: format!("bucket-{key}"),
                        tokens,
                        capacity: 5.0,
                        refill_per_sec: 0.0,
                    },
                ) {
                    CommandResponse::Members { members } => members,
                    other => panic!("expected members, got {other:?}"),
                };
                let used = model.counters.entry(format!("bucket-{key}")).or_default();
                let before = used.len() as i64;
                let allowed = before < 5;
                if allowed {
                    used.insert(before as u64, 1);
                }
                assert_eq!(taken[0], if allowed { b"1".to_vec() } else { b"0".to_vec() });
                let left = 5 - used.len() as i64;
                assert_eq!(taken[1], format!("{left}.000").into_bytes());
            }
        }
        writes += 1;
    }
    assert_eq!(writes, 600);

    let expected = |model: &Model| -> Answers {
        let mut counters = BTreeMap::new();
        for key in KEYS {
            counters.insert(key.to_string(), model.counters.get(key).cloned().unwrap_or_default());
        }
        let mut windows = Vec::new();
        let mut seen_cards = Vec::new();
        let mut selections = Vec::new();
        let mut distinct = Vec::new();
        let mut sequences = Vec::new();
        for key in KEYS {
            let series = model.counters.get(key).cloned().unwrap_or_default();
            for aggregator in ["sum", "min", "max", "first", "last", "events", "len", "count", "other"] {
                windows.push(model_window(&series, aggregator));
            }
            seen_cards.push(model.seen.get(key).map_or(0, |members| members.len() as i64));
            selections.push(model.selection.get(key).map(|(_, value)| value.clone()));
            for (start, end) in [(0u64, u64::MAX), (1_100, 1_250), (1_300, 1_300), (5_000, 6_000)] {
                let mut union: BTreeSet<&Vec<u8>> = BTreeSet::new();
                if let Some(buckets) = model.changes.get(key) {
                    for (_, members) in buckets.range(start..=end) {
                        union.extend(members.iter());
                    }
                }
                distinct.push(union.len() as i64);
            }
            sequences.push(
                model
                    .sequences
                    .get(key)
                    .map(|rows| rows.iter().map(|(at, gid)| (*at, *gid)).collect())
                    .unwrap_or_default(),
            );
        }
        Answers {
            counters,
            windows,
            seen_cards,
            selections,
            distinct,
            sequences,
        }
    };

    let live = ask(&engine);
    assert_eq!(live, expected(&model), "the engine and the model disagree before a reload");
    // The control: the history reached every arm, so equal answers are not equal emptiness.
    assert!(live.counters.values().all(|series| !series.is_empty()));
    assert!(live.seen_cards.iter().all(|cards| *cards > 0));
    assert!(live.selections.iter().all(Option::is_some));
    assert!(live.distinct.iter().any(|count| *count > 0));
    assert!(live.sequences.iter().all(|rows| !rows.is_empty()));

    // And the same after the shard is unloaded and loaded again.
    engine.unload_shard(1);
    engine.load_shard(1);
    assert_eq!(ask(&engine), live, "a reload changed what the arms answer");
}

/// No arm that writes into a live series builds a key for `entry()` to drop.
///
/// A source check, because the allocation it removes is one block of a few dozen and a counted
/// ceiling for it would have to be calibrated against a build. COMMENTS ARE EXCLUDED: this file's
/// own explanation and the arms' notes quote the old spelling.
#[test]
fn no_arm_mints_a_key_before_it_looks_the_key_up() {
    fn code(source: &str) -> String {
        source
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    let arms = code(include_str!("../execute_on_shard.rs"));
    let minting = arms.matches(".entry(std::sync::Arc::from(&key[..]))").count();
    // The two feature append arms are another model's and still spell it.
    assert!(minting <= 2, "{minting} arms still mint a key for `entry()`: use `slot_for_write`");
    assert!(
        arms.matches("slot_for_write(").count() >= 7,
        "the counter, seen and sequence arms should reach their series through `slot_for_write`"
    );
    assert!(
        arms.matches("store_for_write(").count() >= 2,
        "the token bucket and the selection arms should overwrite through `store_for_write`"
    );
    let distinct = code(include_str!("../engine_hll.rs"));
    assert!(
        !distinct.contains(".entry(super::state::ModelKey::from(key))"),
        "a distinct add mints the key before it looks it up"
    );
}
