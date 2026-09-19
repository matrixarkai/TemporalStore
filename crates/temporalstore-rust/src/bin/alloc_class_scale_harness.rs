// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a stored record's memory is spent on, at a corpus larger than any test builds.
//!
//! The in-suite decomposition (`engine::tests::alloc_class_scale`) runs at 2,000 and 20,000
//! records, which is where every scale figure in this crate has been taken. This is the same
//! measurement with the corpus on the command line, so the per-record figures can be read at a
//! size a test cannot hold and the flat-per-record claim can be checked where it would break.
//!
//! WITHOUT `alloc-probe` THIS BINARY REFUSES TO RUN. The counting allocator is installed only
//! under that feature, and without it every class reads zero -- a table of zeros that looks
//! exactly like a store allocating nothing. It exits non-zero and says so rather than printing it.
//!
//!     cargo run --release --features alloc-probe --bin alloc_class_scale_harness -- \
//!         --records 400000 --value-bytes 1024
//!
//! THE CORPUS IS BUILT BETWEEN SPANS, NEVER INSIDE ONE. Each batch's commands are assembled, then
//! a span is opened around `batch_execute` alone and closed, and the spans are added up. A probe
//! that builds its fixture inside the measured window charges the store for it, which is how this
//! crate's write-ahead budget came to read three allocations a write too high for four rounds.

use std::path::PathBuf;

use temporalstore_rust::alloc_probe::{AllocClass, ClassSpan, ClassifiedCounts};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::types::{BatchExecuteRequest, Command};

// The lib installs the counting allocator only in its own test builds. A binary is a separate
// crate root, so it installs its own -- under the same feature, for the same reason.
#[cfg(feature = "alloc-probe")]
#[global_allocator]
static COUNTING_ALLOCATOR: temporalstore_rust::alloc_probe::CountingAllocator =
    temporalstore_rust::alloc_probe::CountingAllocator;

struct Options {
    records: usize,
    value_bytes: usize,
    batch: usize,
    dir: Option<PathBuf>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            records: 20_000,
            value_bytes: 1_024,
            batch: 100,
            dir: None,
        }
    }
}

fn usage_and_exit() -> ! {
    eprintln!("usage: alloc_class_scale_harness [options]");
    eprintln!("  --records <n>       corpus size, default 20000");
    eprintln!("  --value-bytes <n>   bytes per stored value, default 1024");
    eprintln!("  --batch <n>         commands per batch_execute, default 100");
    eprintln!("  --dir <path>        store directory, default a fresh temporary one");
    std::process::exit(2);
}

fn parse_options() -> Options {
    let mut options = Options::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0usize;
    while index < args.len() {
        let key = &args[index];
        if key == "--help" || key == "-h" {
            usage_and_exit();
        }
        let Some(value) = args.get(index + 1) else {
            usage_and_exit();
        };
        match key.as_str() {
            "--records" => options.records = parse(value, key),
            "--value-bytes" => options.value_bytes = parse(value, key),
            "--batch" => options.batch = parse(value, key),
            "--dir" => options.dir = Some(PathBuf::from(value)),
            other => {
                eprintln!("unknown option: {other}");
                usage_and_exit();
            }
        }
        index += 2;
    }
    if options.records == 0 || options.batch == 0 || options.value_bytes == 0 {
        eprintln!("--records, --batch and --value-bytes must all be positive");
        std::process::exit(2);
    }
    options
}

fn parse<T>(value: &str, key: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse().unwrap_or_else(|err| {
        eprintln!("invalid {key} value {value:?}: {err}");
        std::process::exit(2);
    })
}

/// xorshift64*, seeded by index, so no two records share a payload. A repeated byte compresses,
/// and the biggest class by bytes is a compressing encode.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

/// Resident size in kilobytes, or `None` where `/proc` does not answer.
///
/// Reported beside the allocation figures and never in place of them: resident size conflates
/// live data with allocator retention, which is the reason the class ledger exists.
fn resident_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            return value.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

fn main() {
    let options = parse_options();

    // The floor. Without the counting allocator every number below is a zero that reads like a
    // measurement, so there is nothing worth printing.
    if temporalstore_rust::alloc_probe::classified_now().is_none() {
        eprintln!(
            "the counting allocator is not installed: rebuild with --features alloc-probe. \
             Every class would read zero, which is indistinguishable from a store that allocates \
             nothing."
        );
        std::process::exit(3);
    }

    // `tempfile` is a dev-dependency, so this mints its own path rather than pulling a test-only
    // crate into a shipped binary. The name carries the process id, which is what lets a leftover
    // directory be told from one a running harness still owns.
    let root = options.dir.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("ts-alloc-class-{}", std::process::id()))
    });
    if let Err(err) = std::fs::create_dir_all(&root) {
        eprintln!("could not make the store directory {}: {err}", root.display());
        std::process::exit(1);
    }
    let owned = options.dir.is_none();

    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(temporalstore_rust::types::ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "warm".to_string(),
            value: incompressible(options.value_bytes, u64::MAX),
        },
    });

    let resident_before = resident_kb();
    let started = std::time::Instant::now();
    let mut total: Option<ClassifiedCounts> = None;
    let mut written = 0usize;
    while written < options.records {
        let end = (written + options.batch).min(options.records);
        let mut commands = Vec::with_capacity(end - written);
        let mut cursor = written;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: incompressible(options.value_bytes, cursor as u64),
            });
            cursor += 1;
        }
        // The span covers the write and nothing else; the batch above was built outside it.
        let span = ClassSpan::open();
        let response = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        let counts = span
            .close()
            .expect("the floor above proved the counting allocator is installed");
        if !response.status.ok {
            eprintln!("write failed at record {written}: {:?}", response.status);
            std::process::exit(1);
        }
        total = Some(match total {
            Some(previous) => previous.plus(&counts),
            None => counts,
        });
        written = end;
    }
    let elapsed = started.elapsed();
    let resident_after = resident_kb();
    let counts = total.expect("--records is positive, so at least one batch ran");

    let records = options.records as f64;
    println!(
        "corpus {} records x {} bytes, {} per batch, {:.1}s",
        options.records,
        options.value_bytes,
        options.batch,
        elapsed.as_secs_f64()
    );
    println!(
        "profile {}",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
    );
    if let (Some(before), Some(after)) = (resident_before, resident_after) {
        println!(
            "resident {} -> {} kB ({:.2} kB per record; conflates live data with allocator \
             retention, so it is context and not the measurement)",
            before,
            after,
            (after.saturating_sub(before)) as f64 / records
        );
    }
    println!(
        "  {:<20} {:>14} {:>9} {:>16} {:>11} {:>13}",
        "class", "allocs", "per rec", "alloc bytes", "B per rec", "outstanding"
    );
    for class in AllocClass::ALL.iter() {
        let row = counts.classes.row(*class);
        println!(
            "  {:<20} {:>14} {:>9.3} {:>16} {:>11.1} {:>13}",
            class.label(),
            row.allocs,
            row.allocs as f64 / records,
            row.alloc_bytes,
            row.alloc_bytes as f64 / records,
            row.outstanding()
        );
    }
    let summed = counts.classes.summed();
    println!(
        "  {:<20} {:>14} {:>9.3} {:>16} {:>11.1} {:>13}",
        "classes summed",
        summed.allocs,
        summed.allocs as f64 / records,
        summed.alloc_bytes,
        summed.alloc_bytes as f64 / records,
        summed.outstanding()
    );
    println!(
        "  {:<20} {:>14} {:>9.3} {:>16} {:>11.1} {:>13}   <- independent counter",
        "SPAN TOTAL",
        counts.total.allocs,
        counts.total.allocs as f64 / records,
        counts.total.alloc_bytes,
        counts.total.alloc_bytes as f64 / records,
        counts.total.outstanding()
    );
    println!(
        "  {:<20} {:>14} {:>9.3} {:>16} {:>11.1}                 ({:.1}% of bytes, {:.1}% of \
         calls classified)",
        "residual",
        counts.residual_allocs(),
        counts.residual_allocs() as f64 / records,
        counts.residual_bytes(),
        counts.residual_bytes() as f64 / records,
        counts.classified_byte_share() * 100.0,
        counts.classified_call_share() * 100.0
    );

    if counts.residual_allocs() < 0 {
        eprintln!(
            "the classes claim more allocations than the span made, which is double counting: {}",
            counts.classes.report_line()
        );
        std::process::exit(1);
    }

    // The engine goes first: dropping it after the tree is gone would have it writing into a
    // directory that no longer exists.
    drop(engine);
    if owned {
        let _ = std::fs::remove_dir_all(&root);
    } else {
        println!("store left at {}", root.display());
    }
}
