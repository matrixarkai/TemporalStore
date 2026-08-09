//! Property/invariant conformance harness.
//!
//! Instead of sampling behavior with hand-written cases, this drives seeded random command
//! sequences through a real engine and asserts two invariants that catch the bug classes the
//! manual audits kept finding one at a time:
//!   1. NO-PANIC: no generated command (including adversarial reversed range bounds, huge counts,
//!      empty values, duplicate timestamps, cross-model deletes) may panic -- a panic under the
//!      shard write lock poisons it and takes the whole shard down (the reversed-bounds DoS class).
//!   2. RELOAD FIDELITY: the observable state after unload+reload from disk must be byte-identical
//!      to the state before unload -- no lost value, no resurrected delete, no reordering
//!      (the watermark / CP4 / delete_drop / membership-reconcile data-loss classes).
//!
//! Every failure reproduces from its integer seed, so a discovered bug is a deterministic repro.

use super::*;

/// Deterministic xorshift64* PRNG -- no external rand dependency, fully reproducible per seed.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15 | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    /// Model-namespaced key: each model owns a disjoint key space so a key is only ever ONE model
    /// (as real clients use them). This isolates single-model reload fidelity from the separate
    /// cross-model-collision question. `prefix` is the model tag; CommonDelete/Expire pass a union.
    fn model_key(&mut self, prefix: char) -> String {
        format!("{prefix}{}", self.below(4))
    }
    /// A key from ANY model's space, for the cross-model CommonDelete/CommonExpire commands.
    fn any_key(&mut self) -> String {
        let prefix = ['s', 'h', 'e', 'f', 'c', 'i'][self.below(6) as usize];
        self.model_key(prefix)
    }
    fn ts(&mut self) -> u64 {
        self.below(64)
    }
    fn small_bytes(&mut self) -> Vec<u8> {
        let len = self.below(5) as usize;
        (0..len).map(|_| (self.next() & 0xff) as u8).collect()
    }
    fn field(&mut self) -> String {
        format!("f{}", self.below(4))
    }
}

/// A possibly-reversed, possibly-degenerate time range to stress the no-panic invariant.
fn adversarial_bounds(rng: &mut Rng) -> (u64, u64) {
    let a = rng.ts();
    let b = rng.ts();
    match rng.below(4) {
        0 => (b, a),           // possibly reversed (start > end)
        1 => (0, u64::MAX),    // full
        2 => (a, a),           // single point
        _ => (a, b),           // arbitrary
    }
}

fn gen_command(rng: &mut Rng) -> Command {
    match rng.below(20) {
        0 => Command::StringSet {
            key: rng.model_key('s'),
            value: rng.small_bytes(),
        },
        1 => Command::StringDelete {
            key: rng.model_key('s'),
        },
        2 => Command::StringGet {
            key: rng.model_key('s'),
        },
        3 => Command::HashSet {
            key: rng.model_key('h'),
            field: rng.field(),
            value: rng.small_bytes(),
        },
        4 => Command::HashDelete {
            key: rng.model_key('h'),
            field: rng.field(),
        },
        5 => Command::HashIncrBy {
            key: rng.model_key('h'),
            field: rng.field(),
            increment: (rng.below(2001) as i64) - 1000,
        },
        6 => Command::SetAdd {
            key: rng.model_key('e'),
            member: rng.small_bytes(),
        },
        7 => Command::SetRemove {
            key: rng.model_key('e'),
            member: rng.small_bytes(),
        },
        8 => Command::FeatureAppend {
            key: rng.model_key('f'),
            points: (0..1 + rng.below(3))
                .map(|_| FeaturePoint {
                    timestamp_ms: rng.ts(),
                    value: rng.small_bytes(),
                })
                .collect(),
        },
        9 => {
            let (start_ms, end_ms) = adversarial_bounds(rng);
            Command::FeatureQuery {
                key: rng.model_key('f'),
                start_ms,
                end_ms,
                count: if rng.below(3) == 0 {
                    None
                } else {
                    Some(rng.below(10) as usize)
                },
            }
        }
        10 => Command::ControlStateIncrement {
            key: rng.model_key('c'),
            timestamp_ms: rng.ts(),
            amount: (rng.below(2001) as i64) - 1000,
        },
        11 => {
            let (start_ms, end_ms) = adversarial_bounds(rng);
            Command::ControlStateQuery {
                key: rng.model_key('c'),
                start_ms,
                end_ms,
                aggregator: ["sum", "count", "min", "max"][rng.below(4) as usize].to_string(),
            }
        }
        12 => Command::IpsAdd {
            key: rng.model_key('i'),
            timestamp_ms: rng.ts(),
            instance: rng.small_bytes(),
        },
        13 => {
            let (start_ms, end_ms) = adversarial_bounds(rng);
            Command::IpsQueryRange {
                key: rng.model_key('i'),
                start_ms,
                end_ms,
                count: if rng.below(3) == 0 {
                    None
                } else {
                    Some(rng.below(10) as usize)
                },
            }
        }
        14 => Command::CommonDelete { key: rng.any_key() },
        15 => Command::SequenceAdd {
            key: rng.model_key('q'),
            rows: (0..1 + rng.below(3))
                .map(|_| SequenceFeatureRow {
                    timestamp_ms: rng.ts(),
                    gid: rng.below(1000),
                    action_type: rng.below(8) as u32,
                    duration: rng.below(100) as u32,
                    author_id: rng.below(1000),
                })
                .collect(),
        },
        16 => {
            let (start_ms, end_ms) = adversarial_bounds(rng);
            Command::FeatureReplace {
                key: rng.model_key('f'),
                start_ms,
                end_ms,
                points: (0..1 + rng.below(3))
                    .map(|_| FeaturePoint {
                        timestamp_ms: rng.ts(),
                        value: rng.small_bytes(),
                    })
                    .collect(),
            }
        }
        17 => Command::FeatureDelete {
            key: rng.model_key('f'),
        },
        18 => Command::ControlStateChangeAdd {
            key: rng.model_key('g'),
            timestamp_ms: rng.ts(),
            value: rng.small_bytes(),
            precision_ms: None,
            ttl_ms: None,
        },
        // Large TTL only: sets the expiry path without firing during the test, so the reload
        // fidelity check stays time-independent (active expiry is covered by its own tests).
        _ => Command::CommonExpire {
            key: rng.any_key(),
            ttl_ms: 1 << 40,
        },
    }
}

/// A deterministic, full-range read of every model for every key -- the observable state used to
/// compare pre-unload vs post-reload. Stringified so any response shape is comparable.
fn read_snapshot(engine: &TemporalEngine) -> Vec<String> {
    let mut snapshot = Vec::new();
    let mut probe = |label: &str, command: Command| {
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        snapshot.push(format!("{label}={:?}", resp.response));
    };
    for n in 0..4 {
        probe(&format!("s{n}"), Command::StringGet { key: format!("s{n}") });
        probe(&format!("h{n}"), Command::HashGetAll { key: format!("h{n}") });
        probe(&format!("e{n}"), Command::SetMembers { key: format!("e{n}") });
        probe(
            &format!("f{n}"),
            Command::FeatureQuery {
                key: format!("f{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        );
        probe(
            &format!("c{n}"),
            Command::ControlStateQuery {
                key: format!("c{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                aggregator: "sum".to_string(),
            },
        );
        probe(
            &format!("i{n}"),
            Command::IpsQueryRange {
                key: format!("i{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        );
        probe(
            &format!("q{n}"),
            Command::SequenceQuery {
                key: format!("q{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                count: 1_000_000,
                filters: Vec::new(),
            },
        );
        probe(
            &format!("g{n}"),
            Command::ControlStateQuery {
                key: format!("g{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                aggregator: "change".to_string(),
            },
        );
    }
    drop(probe);
    snapshot
}

fn run_one_sequence(seed: u64, maintenance: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &pages, &indexes);
    engine.load_shard(1);

    let mut rng = Rng::new(seed);
    let op_count = 20 + rng.below(50);
    for _ in 0..op_count {
        // Maintenance ops (real dump/compaction work) are expensive, so they are OFF by default
        // (fast suite run) and enabled for deep hunts via CONFORMANCE_MAINTENANCE=1.
        match if maintenance { rng.below(12) } else { 2 + rng.below(10) } {
            // Interleave storage-lifecycle maintenance so the fuzzer also exercises the dump /
            // clear-dirty / reclaim / compaction paths against reload fidelity (the CP4 / watermark
            // / dump-scheduling bug class) -- not only command execution.
            0 => {
                let _ = engine.apply_storage_lifecycle(StorageLifecycleRequest {
                    shard_id: 1,
                    purge_delayed_destroy: true,
                    ..Default::default()
                });
            }
            1 => {
                let _ = engine.compact_shard_pages(1);
            }
            _ => {
                let command = gen_command(&mut rng);
                // A returned error status is fine (e.g. not-an-integer HINCRBY); a PANIC is not --
                // caught by catch_unwind in the caller as a no-panic-invariant violation.
                let _ = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command,
                });
            }
        }
    }

    let before = read_snapshot(&engine);
    engine.unload_shard(1);

    // Two reload cycles: reload-on-load runs reconcile, which may itself persist a rebuilt index.
    // A single reload can hide a bug where reconstruction PERSISTS a subtly-wrong index that only a
    // SECOND reload then reads back wrong (accumulation / non-idempotent reconcile). Every reload's
    // observable state must equal the pre-unload state.
    let mut prior = before.clone();
    for (cycle, cache) in ["cache-b", "cache-c"].into_iter().enumerate() {
        let reloaded =
            TemporalEngine::with_local_dirs(1 << 20, dir.path().join(cache), &pages, &indexes);
        reloaded.load_shard(1);
        let after = read_snapshot(&reloaded);
        if prior != after {
            let diff = prior
                .iter()
                .zip(after.iter())
                .filter(|(b, a)| b != a)
                .map(|(b, a)| format!("\n  before: {b}\n  after:  {a}"))
                .collect::<String>();
            return Err(format!("reload fidelity mismatch (cycle {cycle}):{diff}"));
        }
        reloaded.unload_shard(1);
        prior = after;
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Reference-model oracle: a naive, obviously-correct in-Rust model of the engine's INTENDED
// semantics. Diffing the engine against it after a random sequence catches WRONG-RESULT bugs
// (a query returning the wrong value), which the reload/no-panic invariants cannot see. Scoped to
// the commands with unambiguous semantics (string/hash/set/feature/ips point storage +
// cross-model CommonDelete); the model mirrors the engine's CURRENT documented behavior (e.g.
// HINCRBY parses stored ints strtoll-style skipping leading whitespace, and errors -> no change).
#[derive(Default)]
struct RefModel {
    strings: HashMap<String, Vec<u8>>,
    hashes: HashMap<String, std::collections::BTreeMap<String, Vec<u8>>>,
    sets: HashMap<String, std::collections::BTreeSet<Vec<u8>>>,
    features: HashMap<String, std::collections::BTreeMap<u64, Vec<u8>>>,
    ips: HashMap<String, std::collections::BTreeMap<u64, Vec<u8>>>,
}

impl RefModel {
    fn apply(&mut self, command: &Command) {
        match command {
            Command::StringSet { key, value } => {
                self.strings.insert(key.clone(), value.clone());
            }
            Command::StringDelete { key } => {
                self.strings.remove(key);
            }
            Command::HashSet { key, field, value } => {
                self.hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), value.clone());
            }
            Command::HashDelete { key, field } => {
                if let Some(fields) = self.hashes.get_mut(key) {
                    fields.remove(field);
                    if fields.is_empty() {
                        self.hashes.remove(key); // emptying a hash removes the key (Redis + engine)
                    }
                }
            }
            Command::HashIncrBy {
                key,
                field,
                increment,
            } => {
                let fields = self.hashes.entry(key.clone()).or_default();
                match fields.get(field) {
                    // Absent field increments from 0; only i64 overflow errors.
                    None => {
                        if let Some(next) = 0i64.checked_add(*increment) {
                            fields.insert(field.clone(), next.to_string().into_bytes());
                        }
                    }
                    // Present field: parse like the engine (strtoll -> skip leading ASCII
                    // whitespace). Non-integer or overflow => the engine errors and leaves the
                    // value unchanged.
                    Some(bytes) => {
                        let parsed = std::str::from_utf8(bytes)
                            .ok()
                            .map(|text| text.trim_start_matches(|c: char| c.is_ascii_whitespace()))
                            .and_then(|text| text.parse::<i64>().ok());
                        if let Some(current) = parsed {
                            if let Some(next) = current.checked_add(*increment) {
                                fields.insert(field.clone(), next.to_string().into_bytes());
                            }
                        }
                    }
                }
            }
            Command::SetAdd { key, member } => {
                self.sets.entry(key.clone()).or_default().insert(member.clone());
            }
            Command::SetRemove { key, member } => {
                if let Some(members) = self.sets.get_mut(key) {
                    members.remove(member);
                }
            }
            Command::FeatureAppend { key, points } => {
                let series = self.features.entry(key.clone()).or_default();
                // Last-write-wins per timestamp (matches sorted_feature_points + BTreeMap upsert).
                for point in points {
                    series.insert(point.timestamp_ms, point.value.clone());
                }
            }
            Command::IpsAdd {
                key,
                timestamp_ms,
                instance,
            } => {
                self.ips
                    .entry(key.clone())
                    .or_default()
                    .insert(*timestamp_ms, instance.clone());
            }
            Command::CommonDelete { key } => {
                self.strings.remove(key);
                self.hashes.remove(key);
                self.sets.remove(key);
                self.features.remove(key);
                self.ips.remove(key);
            }
            // CommonExpire uses a non-firing TTL in the oracle generator -> no observable effect.
            _ => {}
        }
    }
}

fn gen_oracle_command(rng: &mut Rng) -> Command {
    match rng.below(13) {
        0 => Command::StringSet {
            key: rng.model_key('s'),
            value: rng.small_bytes(),
        },
        1 => Command::StringDelete {
            key: rng.model_key('s'),
        },
        2 => Command::HashSet {
            key: rng.model_key('h'),
            field: rng.field(),
            value: rng.small_bytes(),
        },
        3 => Command::HashDelete {
            key: rng.model_key('h'),
            field: rng.field(),
        },
        4 => Command::HashIncrBy {
            key: rng.model_key('h'),
            field: rng.field(),
            increment: (rng.below(2001) as i64) - 1000,
        },
        5 => Command::SetAdd {
            key: rng.model_key('e'),
            member: rng.small_bytes(),
        },
        6 => Command::SetRemove {
            key: rng.model_key('e'),
            member: rng.small_bytes(),
        },
        7 | 8 => Command::FeatureAppend {
            key: rng.model_key('f'),
            points: (0..1 + rng.below(3))
                .map(|_| FeaturePoint {
                    timestamp_ms: rng.ts(),
                    value: rng.small_bytes(),
                })
                .collect(),
        },
        9 | 10 => Command::IpsAdd {
            key: rng.model_key('i'),
            timestamp_ms: rng.ts(),
            instance: rng.small_bytes(),
        },
        11 => Command::CommonDelete { key: rng.any_key() },
        _ => Command::CommonExpire {
            key: rng.any_key(),
            ttl_ms: 1 << 40,
        },
    }
}

fn engine_read(engine: &TemporalEngine, command: Command) -> CommandResponse {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command,
        })
        .response
}

fn points_to_pairs(response: CommandResponse) -> Result<Vec<(u64, Vec<u8>)>, String> {
    match response {
        CommandResponse::FeaturePoints { points } => Ok(points
            .into_iter()
            .map(|point| (point.timestamp_ms, point.value))
            .collect()),
        other => Err(format!("expected FeaturePoints, got {other:?}")),
    }
}

fn oracle_mismatch(engine: &TemporalEngine, model: &RefModel) -> Option<String> {
    for n in 0..4 {
        // String: exact value.
        let key = format!("s{n}");
        match engine_read(engine, Command::StringGet { key: key.clone() }) {
            CommandResponse::Bytes { value } => {
                if value != model.strings.get(&key).cloned() {
                    return Some(format!(
                        "string {key}: engine={value:?} model={:?}",
                        model.strings.get(&key)
                    ));
                }
            }
            other => return Some(format!("string {key}: unexpected {other:?}")),
        }

        // Hash: same field->value set (compare sorted to ignore return order).
        let key = format!("h{n}");
        match engine_read(engine, Command::HashGetAll { key: key.clone() }) {
            CommandResponse::HashEntries { mut entries } => {
                entries.sort();
                let expected: Vec<(String, Vec<u8>)> = model
                    .hashes
                    .get(&key)
                    .map(|fields| fields.iter().map(|(f, v)| (f.clone(), v.clone())).collect())
                    .unwrap_or_default();
                if entries != expected {
                    return Some(format!("hash {key}: engine={entries:?} model={expected:?}"));
                }
            }
            other => return Some(format!("hash {key}: unexpected {other:?}")),
        }

        // Set: same member set (compare sorted).
        let key = format!("e{n}");
        match engine_read(engine, Command::SetMembers { key: key.clone() }) {
            CommandResponse::Members { mut members } => {
                members.sort();
                let expected: Vec<Vec<u8>> = model
                    .sets
                    .get(&key)
                    .map(|members| members.iter().cloned().collect())
                    .unwrap_or_default();
                if members != expected {
                    return Some(format!("set {key}: engine={members:?} model={expected:?}"));
                }
            }
            other => return Some(format!("set {key}: unexpected {other:?}")),
        }

        // Feature: all points, timestamp-ascending, exact values.
        let key = format!("f{n}");
        let engine_points = match points_to_pairs(engine_read(
            engine,
            Command::FeatureQuery {
                key: key.clone(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        )) {
            Ok(pairs) => pairs,
            Err(err) => return Some(format!("feature {key}: {err}")),
        };
        let expected: Vec<(u64, Vec<u8>)> = model
            .features
            .get(&key)
            .map(|series| series.iter().map(|(t, v)| (*t, v.clone())).collect())
            .unwrap_or_default();
        if engine_points != expected {
            return Some(format!(
                "feature {key}: engine={engine_points:?} model={expected:?}"
            ));
        }

        // Ips: all instances, timestamp-ascending (current engine order), exact values.
        let key = format!("i{n}");
        let engine_ips = match points_to_pairs(engine_read(
            engine,
            Command::IpsQueryRange {
                key: key.clone(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        )) {
            Ok(pairs) => pairs,
            Err(err) => return Some(format!("ips {key}: {err}")),
        };
        let expected: Vec<(u64, Vec<u8>)> = model
            .ips
            .get(&key)
            .map(|series| series.iter().map(|(t, v)| (*t, v.clone())).collect())
            .unwrap_or_default();
        if engine_ips != expected {
            return Some(format!("ips {key}: engine={engine_ips:?} model={expected:?}"));
        }
    }
    None
}

fn run_one_oracle_sequence(seed: u64) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let mut model = RefModel::default();
    let mut rng = Rng::new(seed);
    let op_count = 20 + rng.below(60);
    for _ in 0..op_count {
        let command = gen_oracle_command(&mut rng);
        model.apply(&command);
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
    }
    oracle_mismatch(&engine, &model).map_or(Ok(()), Err)
}

#[test]
fn conformance_oracle_matches_reference_model() {
    let count = std::env::var("CONFORMANCE_SEEDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(64);
    let start = std::env::var("CONFORMANCE_START")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let outcome = (start..start.saturating_add(count))
        .map(|seed| match run_one_oracle_sequence(seed) {
            Ok(()) => Ok(()),
            Err(msg) => Err(format!("seed {seed}: {msg}")),
        })
        .find(Result::is_err);
    if let Some(Err(failure)) = outcome {
        panic!("oracle found an engine-vs-reference-model mismatch: {failure}");
    }
}

#[test]
#[ignore]
fn debug_minimize_seed() {
    // Diagnostic tool: replays a seed command-by-command, reporting the first command after which a
    // reload loses state (delta-minimizes a harness failure to its trigger). Set `seed` to a
    // failing seed and run: cargo test --lib debug_minimize_seed -- --ignored --nocapture
    let seed = 0u64;
    let mut rng = Rng::new(seed);
    let op_count = 20 + rng.below(50);
    let mut commands = Vec::new();
    for _ in 0..op_count {
        // Mirror run_one_sequence's per-op branch selector (no-maintenance mode) so the rng stream
        // -- and thus the replayed command sequence -- matches the harness exactly.
        let _selector = 2 + rng.below(10);
        commands.push(gen_command(&mut rng));
    }
    for prefix_len in 1..=commands.len() {
        let dir = tempfile::tempdir().unwrap();
        let pages = dir.path().join("pages");
        let indexes = dir.path().join("indexes");
        let engine =
            TemporalEngine::with_local_dirs(1 << 20, dir.path().join("a"), &pages, &indexes);
        engine.load_shard(1);
        for command in &commands[..prefix_len] {
            let _ = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: command.clone(),
            });
        }
        let before = read_snapshot(&engine);
        engine.unload_shard(1);
        let reloaded =
            TemporalEngine::with_local_dirs(1 << 20, dir.path().join("b"), &pages, &indexes);
        reloaded.load_shard(1);
        let after = read_snapshot(&reloaded);
        if before != after {
            println!("FIRST BREAK at prefix_len={prefix_len}, command = {:?}", commands[prefix_len - 1]);
            println!("full prefix:");
            for (i, c) in commands[..prefix_len].iter().enumerate() {
                println!("  [{i}] {c:?}");
            }
            for (b, a) in before.iter().zip(after.iter()).filter(|(b, a)| b != a) {
                println!("  MISMATCH before={b}  after={a}");
            }
            return;
        }
    }
    println!("no break found across {} prefixes", commands.len());
}

#[test]
fn conformance_random_sequences_never_panic_and_survive_reload() {
    // Silence the default panic hook for the duration so a deliberately-provoked panic (if any bug
    // exists) does not spam stderr; catch_unwind still reports it with the reproducing seed.
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    // Default 128 seeds in the suite; a deep hunt can widen without recompiling via
    // CONFORMANCE_SEEDS / CONFORMANCE_START (e.g. CONFORMANCE_SEEDS=4000 cargo test ...).
    let count = std::env::var("CONFORMANCE_SEEDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(64);
    let start = std::env::var("CONFORMANCE_START")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let maintenance = std::env::var("CONFORMANCE_MAINTENANCE").is_ok();
    let outcome = (start..start.saturating_add(count))
        .map(|seed| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_one_sequence(seed, maintenance)
            }));
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(msg)) => Err(format!("seed {seed}: {msg}")),
                Err(_) => Err(format!("seed {seed}: PANICKED (no-panic invariant violated)")),
            }
        })
        .find(Result::is_err);
    std::panic::set_hook(previous_hook);
    if let Some(Err(failure)) = outcome {
        panic!("conformance harness found a repro: {failure}");
    }
}
