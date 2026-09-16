// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What `page_reads`/`block_reads` and `page_writes`/`block_writes` actually count.
//!
//! A committed fixture (`compat/storage_unified_case_report_pair.json`) recorded
//! `page_reads: 4` next to `block_reads: 2` -- a clean factor of two, in six places, in both
//! halves of the pair. That reads as two different measurements, and a rename treating one as
//! a stale spelling of the other would then halve a real number.
//!
//! It is not two measurements. `engine/persistence.rs` assigns all four fields from the SAME
//! `BlockStore` counters, four lines apart:
//!
//! ```text
//! page_reads:   block_store.reads,
//! page_writes:  block_store.writes,
//! block_reads:  block_store.reads,
//! block_writes: block_store.writes,
//! ```
//!
//! `BlockStore` carries exactly one read counter (`stats.reads`, incremented at three call
//! sites in `block_store/read.rs`) and one write counter (`stats.writes`, two sites in
//! `block_store/append.rs`). There is no second measurement for the second name to carry, so
//! the ratio is one -- the fixture's two was authored, never observed.
//!
//! These tests drive the engine over nine shaped workloads and assert the RATIO rather than
//! "a count was recorded", so an edit making one of the pair count something else fails here
//! instead of being rediscovered in a fixture.
#![allow(clippy::all)]
use super::*;

#[derive(Debug, Clone, Copy)]
struct CounterSample {
    name: &'static str,
    page_reads: u64,
    block_reads: u64,
    page_writes: u64,
    block_writes: u64,
}

fn counter_ratio(numerator: u64, denominator: u64) -> String {
    if denominator == 0 {
        format!("undefined (denominator {denominator})")
    } else {
        format!("{:.3}", numerator as f64 / denominator as f64)
    }
}

fn counter_sample(name: &'static str, engine: &TemporalEngine) -> CounterSample {
    let stats = engine.loaded_shard_stats();
    assert_eq!(
        stats.len(),
        1,
        "{name}: expected exactly one loaded shard, got {}",
        stats.len()
    );
    let storage = &stats[0].storage;
    let observed = CounterSample {
        name,
        page_reads: storage.page_reads,
        block_reads: storage.block_reads,
        page_writes: storage.page_writes,
        block_writes: storage.block_writes,
    };
    println!(
        "workload {name}: page_reads={} block_reads={} read_ratio={} \
         page_writes={} block_writes={} write_ratio={}",
        observed.page_reads,
        observed.block_reads,
        counter_ratio(observed.page_reads, observed.block_reads),
        observed.page_writes,
        observed.block_writes,
        counter_ratio(observed.page_writes, observed.block_writes),
    );
    observed
}

fn counter_engine(dir: &std::path::Path, cache_name: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join(cache_name),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn counter_set(engine: &TemporalEngine, key: &str, value: Vec<u8>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value,
        },
    });
    assert!(
        response.status.ok,
        "write of {key} failed: {:?}",
        response.status
    );
}

fn counter_get(engine: &TemporalEngine, key: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    assert!(
        response.status.ok,
        "read of {key} failed: {:?}",
        response.status
    );
}

#[test]
fn page_and_block_counters_are_one_measurement_under_two_names() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let mut samples: Vec<CounterSample> = Vec::new();

    // 1. One write, then one read from a restart with a COLD cache directory, so the read
    //    cannot be served from memory or from the disk block cache.
    {
        let engine = counter_engine(&root, "cache-a");
        counter_set(&engine, "single", vec![b'a'; 512]);
        samples.push(counter_sample("write-one", &engine));
    }
    {
        let engine = counter_engine(&root, "cache-b");
        counter_get(&engine, "single");
        samples.push(counter_sample("read-one-cold", &engine));
    }

    // 2. Ten distinct keys, each read once from a cold cache: ten store reads.
    {
        let engine = counter_engine(&root, "cache-c");
        counter_set(&engine, "k0", vec![b'0'; 512]);
        counter_set(&engine, "k1", vec![b'1'; 512]);
        counter_set(&engine, "k2", vec![b'2'; 512]);
        counter_set(&engine, "k3", vec![b'3'; 512]);
        counter_set(&engine, "k4", vec![b'4'; 512]);
        counter_set(&engine, "k5", vec![b'5'; 512]);
        counter_set(&engine, "k6", vec![b'6'; 512]);
        counter_set(&engine, "k7", vec![b'7'; 512]);
        counter_set(&engine, "k8", vec![b'8'; 512]);
        counter_set(&engine, "k9", vec![b'9'; 512]);
        samples.push(counter_sample("write-ten", &engine));
    }
    {
        let engine = counter_engine(&root, "cache-d");
        counter_get(&engine, "k0");
        counter_get(&engine, "k1");
        counter_get(&engine, "k2");
        counter_get(&engine, "k3");
        counter_get(&engine, "k4");
        counter_get(&engine, "k5");
        counter_get(&engine, "k6");
        counter_get(&engine, "k7");
        counter_get(&engine, "k8");
        counter_get(&engine, "k9");
        samples.push(counter_sample("read-ten-cold", &engine));
    }

    // 3. A large object -- four megabytes, far past any single page target -- read cold. One
    //    store read, not one per page: the counter counts CALLS, not bytes and not extents.
    {
        let engine = counter_engine(&root, "cache-e");
        counter_set(&engine, "large", vec![b'L'; 4 * 1024 * 1024]);
        samples.push(counter_sample("write-large-object", &engine));
    }
    {
        let engine = counter_engine(&root, "cache-f");
        counter_get(&engine, "large");
        samples.push(counter_sample("read-large-object-cold", &engine));
    }

    // 4. A cache hit: the same key twice in one process, the second read served from memory.
    {
        let engine = counter_engine(&root, "cache-g");
        counter_get(&engine, "single");
        let before = counter_sample("read-cache-warm-first", &engine);
        samples.push(before);
        counter_get(&engine, "single");
        let after = counter_sample("read-cache-hit", &engine);
        assert_eq!(
            after.block_reads, before.block_reads,
            "a cache hit must not reach the block store"
        );
        assert_eq!(
            after.page_reads, before.page_reads,
            "a cache hit must not reach the block store"
        );
        samples.push(after);
    }

    // 5. A miss: a key that was never written.
    {
        let engine = counter_engine(&root, "cache-h");
        counter_get(&engine, "never-written");
        samples.push(counter_sample("read-miss", &engine));
    }

    // The denominator. A workload set that silently shrank would otherwise pass vacuously.
    assert_eq!(
        samples.len(),
        9,
        "expected nine recorded samples, recorded {}",
        samples.len()
    );
    // Non-vacuity: workloads must actually have reached the block store, or every ratio below
    // is 0/0 and the equalities mean nothing.
    let with_reads = samples
        .iter()
        .filter(|sample| sample.block_reads > 0)
        .count();
    assert!(
        with_reads >= 4,
        "expected at least four workloads to reach the block store, {with_reads} of {} did",
        samples.len()
    );
    let with_writes = samples
        .iter()
        .filter(|sample| sample.block_writes > 0)
        .count();
    assert!(
        with_writes >= 3,
        "expected at least three workloads to have written, {with_writes} of {} did",
        samples.len()
    );

    // The finding, as a NUMBER: the ratio is exactly one everywhere, never two.
    for observed in &samples {
        assert_eq!(
            observed.page_reads, observed.block_reads,
            "{}: page_reads {} and block_reads {} are published from one BlockStore counter \
             and must stay equal (ratio {})",
            observed.name,
            observed.page_reads,
            observed.block_reads,
            counter_ratio(observed.page_reads, observed.block_reads),
        );
        assert_eq!(
            observed.page_writes, observed.block_writes,
            "{}: page_writes {} and block_writes {} are published from one BlockStore counter \
             and must stay equal (ratio {})",
            observed.name,
            observed.page_writes,
            observed.block_writes,
            counter_ratio(observed.page_writes, observed.block_writes),
        );
    }
}

/// The committed fixture must record a shape the engine can actually produce.
///
/// This is the guard that would have stopped the trap being laid. The fixture's own gate,
/// `tools/validate_storage_unified_case_report_pair.py`, cannot check it: that gate exits
/// early today because the corpus row it reads carries no case names, and it then compiles a
/// runner that is absent from this repository. So the check lives here, where it runs.
#[test]
fn the_committed_case_fixture_records_the_ratio_the_engine_produces() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../compat/storage_unified_case_report_pair.json");
    let text = std::fs::read_to_string(&fixture)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", fixture.display()));
    let parsed: serde_json::Value = serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("cannot parse {}: {error}", fixture.display()));

    let mut steps_seen = 0usize;
    let mut read_pairs = 0usize;
    let mut write_pairs = 0usize;
    for half in ["rust_report", "native_report"] {
        let cases = parsed[half]["cases"]
            .as_array()
            .unwrap_or_else(|| panic!("{half}: cases must be an array"));
        for case in cases {
            for step in case["steps"].as_array().into_iter().flatten() {
                steps_seen += 1;
                let output = &step["output"];
                let page_reads = output.get("page_reads").and_then(|value| value.as_u64());
                let block_reads = output.get("block_reads").and_then(|value| value.as_u64());
                if let (Some(page), Some(block)) = (page_reads, block_reads) {
                    read_pairs += 1;
                    assert_eq!(
                        page, block,
                        "{half}/{}: page_reads {page} beside block_reads {block}; they are \
                         one BlockStore counter and cannot differ (ratio {})",
                        case["name"], counter_ratio(page, block),
                    );
                }
                let page_writes = output.get("page_writes").and_then(|value| value.as_u64());
                let block_writes = output.get("block_writes").and_then(|value| value.as_u64());
                if let (Some(page), Some(block)) = (page_writes, block_writes) {
                    write_pairs += 1;
                    assert_eq!(
                        page, block,
                        "{half}/{}: page_writes {page} beside block_writes {block}; they are \
                         one BlockStore counter and cannot differ (ratio {})",
                        case["name"], counter_ratio(page, block),
                    );
                }
            }
        }
    }

    // Denominators, printed. Without them a fixture that stopped carrying these fields would
    // pass this test by having nothing left to compare.
    println!(
        "fixture: steps={steps_seen} read_pairs={read_pairs} write_pairs={write_pairs}"
    );
    assert!(
        steps_seen >= 100,
        "expected the fixture to carry at least a hundred steps, read {steps_seen}"
    );
    assert_eq!(
        read_pairs, 6,
        "expected six steps publishing both read spellings, found {read_pairs} of {steps_seen}"
    );
    assert_eq!(
        write_pairs, 6,
        "expected six steps publishing both write spellings, found {write_pairs} of \
         {steps_seen}"
    );
}
