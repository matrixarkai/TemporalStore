// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A RECORD CARRIES, PER WRITE SHAPE, AT THE SHIPPED DEFAULTS.
//!
//! An acknowledged write must survive the loss of this node. After a crash, replay can put an
//! acked value back from exactly three places in the record that acked it:
//!
//!   1. an OPERATION to re-run,
//!   2. a PAGE carried inside the record,
//!   3. a RESULT that states itself -- the bytes, a removal, or a new deadline -- and so needs no
//!      page at all.
//!
//! A result naming only a block-store ADDRESS is none of those three, because the data-page
//! fdatasync is deferred past the ack: the address can name bytes a power cut left unwritten. So
//! "what does an ack promise" reduces to a count -- per write shape, how many acked results have
//! none of the three. This file takes that count in all eight cells and prints it.
//!
//! # The three choices, and why the eighth cell is not reachable by a page
//!
//!   * SEPARATE or BATCHED. These are different appends. A separate write reaches
//!     `append_for_group_commit`, whose rule is `!staged_blocks.is_empty()` over the single record
//!     it is writing; a batch asks `wal::every_result_can_be_found_again`, the same question per
//!     RESULT. The batch rule used to be `sync || !staged_blocks.is_empty()`, which was wrong
//!     twice over: it asked a per-result question at BATCH granularity, where one carried page
//!     answers for every result in the batch, and `sync` is not a durability fact about a page at
//!     all -- it is a fact about the LOG's barrier, which the page fsync is deferred past.
//!   * SYNCHRONOUS or ASYNCHRONOUS -- `Config::async_storage`, the per-shard durability choice.
//!   * A PAGE CARRIED IN THE RECORD, or NO PAGE AT ALL. There is no third option in production:
//!     every `append_value` call site passes `Some(object_id)`, and `append_value_inner` stages
//!     unconditionally for those, so a page that reaches a slab is always also in its record. The
//!     cell "page in the block store but not in the record" does not exist, and the no-page cell
//!     is a REMOVAL.
//!
//! # Why the count is per RESULT and not per record
//!
//! A batch is ONE record carrying many results. A per-record count calls that record covered as
//! soon as any one of its results is -- which is also the number a batch that lost half its keys
//! would produce. So each address-only result is matched against the pages its own record carries,
//! on (object id, element). That match UNDER-matches by design: a page's object id is derived
//! without the element and a timestamped result's with it, so an uncovered count can only ever be
//! too HIGH. A zero from it is therefore trustworthy, which is the direction a durability claim
//! needs.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use temporalstore_rust::{
    BatchExecuteRequest, Command, Config, ExecuteRequest, FeaturePoint, SetConfigRequest,
    TemporalEngine,
};

/// What the records of one cell are carrying.
///
/// `blocks_in_the_record` is the column the durability note on `TS_WAL_DATA_ONLY` did not have. It
/// counted operations and result values, found zero of each, and concluded there was nothing to
/// rebuild from -- while the pages were travelling inside those same records the whole time.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
struct Carried {
    records: usize,
    records_with_an_operation: usize,
    results: usize,
    results_carrying_bytes: usize,
    results_stating_a_removal: usize,
    results_stating_a_deadline: usize,
    results_naming_only_an_address: usize,
    blocks_in_the_record: usize,
    results_with_no_way_back: usize,
}

impl Carried {
    /// Results that state their own outcome, so no page has to survive for them.
    fn results_stating_themselves(&self) -> usize {
        self.results_carrying_bytes
            + self.results_stating_a_removal
            + self.results_stating_a_deadline
    }

    /// Every record kept the operation that produced it.
    fn every_record_kept_its_operation(&self) -> bool {
        self.records_with_an_operation == self.records
    }
}

#[derive(Debug, Clone, Copy)]
struct Shape {
    label: &'static str,
    synchronous: bool,
    batched: bool,
    /// An upsert produces a page, so its record carries one. A removal has no page to carry.
    upsert: bool,
}

const SHAPES: &[Shape] = &[
    Shape { label: "sync  / separate / upsert ", synchronous: true,  batched: false, upsert: true  },
    Shape { label: "sync  / batched  / upsert ", synchronous: true,  batched: true,  upsert: true  },
    Shape { label: "async / separate / upsert ", synchronous: false, batched: false, upsert: true  },
    Shape { label: "async / batched  / upsert ", synchronous: false, batched: true,  upsert: true  },
    Shape { label: "sync  / separate / removal", synchronous: true,  batched: false, upsert: false },
    Shape { label: "sync  / batched  / removal", synchronous: true,  batched: true,  upsert: false },
    Shape { label: "async / separate / removal", synchronous: false, batched: false, upsert: false },
    Shape { label: "async / batched  / removal", synchronous: false, batched: true,  upsert: false },
];

fn key_of(i: u64) -> String {
    format!("ack-{i:06}")
}

fn value_of(i: u64) -> Vec<u8> {
    format!("ackv-{i:06}-{}", "y".repeat(40)).into_bytes()
}

/// The workload each upsert cell runs.
///
/// Mixed on purpose. One command kind makes a whole cell look like whichever path that kind takes,
/// and these are five different page shapes through the same append: a whole-object value, a hash
/// element, a set member, a scored member, and a packed timestamped series.
fn upsert_workload(base: u64) -> Vec<Command> {
    let mut commands = Vec::new();
    for i in base..base + 4 {
        commands.push(Command::StringSet {
            key: key_of(i),
            value: value_of(i),
        });
        commands.push(Command::HashSet {
            key: format!("h-{i:06}"),
            field: "f".to_string(),
            value: value_of(i),
        });
        commands.push(Command::SetAdd {
            key: format!("s-{i:06}"),
            member: value_of(i),
        });
        commands.push(Command::ZSetAdd {
            key: format!("z-{i:06}"),
            member: value_of(i),
            score: i as f64,
        });
        commands.push(Command::FeatureAppend {
            key: format!("f-{i:06}"),
            points: vec![FeaturePoint {
                timestamp_ms: (i + 1) * 1_000,
                value: value_of(i),
            }],
        });
    }
    commands
}

/// Removals of keys the setup window already wrote, so each one really removes something. A
/// removal of an absent key is not a write and appends no record, which would leave the cell
/// measuring an empty log and agreeing with everything.
fn removal_workload(base: u64) -> Vec<Command> {
    let mut commands = Vec::new();
    for i in base..base + 4 {
        commands.push(Command::StringDelete { key: key_of(i) });
        commands.push(Command::CommonDelete {
            key: format!("s-{i:06}"),
        });
        commands.push(Command::CommonExpire {
            key: format!("h-{i:06}"),
            ttl_ms: 600_000,
        });
    }
    commands
}

fn open_engine(root: &Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        256,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    )
}

fn log_store(root: &Path) -> temporalstore_rust::LocalWriteAheadLogStore {
    let wals = root.join("indexes").join("wals");
    temporalstore_rust::LocalWriteAheadLogStore::new(&wals)
}

/// Every log id in the log right now, so the measured window is "the records that are not among
/// these".
///
/// A SET of ids rather than a count to subtract. A count difference also moves when the setup
/// window appends nothing and the measured one appends twice, and a number two different stories
/// produce is not evidence.
fn log_ids(root: &Path) -> HashSet<u64> {
    log_store(root)
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("scan the log")
        .into_iter()
        .map(|(log_id, _)| log_id)
        .collect()
}

/// What the records appended since `before` carry.
fn carried_since(root: &Path, before: &HashSet<u64>) -> Carried {
    let records = log_store(root)
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("scan the log");
    let mut counts = Carried::default();
    for (log_id, bytes) in &records {
        if before.contains(log_id) {
            continue;
        }
        let record = temporalstore_rust::wal::decode_wal_line(bytes)
            .expect("every record this store wrote must decode");
        counts.records += 1;
        // An operation covers every result in its record: re-running it reproduces all of them.
        let has_operation = record.command.is_some();
        if has_operation {
            counts.records_with_an_operation += 1;
        }
        counts.blocks_in_the_record += record.staged_blocks.len();
        // Which pages this record carries, keyed as the page itself spells its identity.
        let mut pages: HashMap<(u64, Option<String>), usize> = HashMap::new();
        for page in &record.staged_blocks {
            *pages
                .entry((
                    page.object_id,
                    page.component.as_ref().map(|c| c.to_string()),
                ))
                .or_default() += 1;
        }
        for item in &record.outcomes {
            counts.results += 1;
            let carries_bytes = item.value.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
            if carries_bytes {
                counts.results_carrying_bytes += 1;
            }
            if item.deleted {
                counts.results_stating_a_removal += 1;
            }
            if item.ttl.is_some() {
                counts.results_stating_a_deadline += 1;
            }
            let states_itself = carries_bytes || item.deleted || item.ttl.is_some();
            if !states_itself && item.address.is_some() {
                counts.results_naming_only_an_address += 1;
            }
            if states_itself || has_operation {
                continue;
            }
            // Nothing in the result says what the value is and there is no operation to re-run,
            // so the only way back is a page for THIS result inside THIS record.
            let carried = pages.contains_key(&(item.object_id, item.component.clone()))
                || pages.contains_key(&(item.object_id, None));
            if !carried {
                counts.results_with_no_way_back += 1;
            }
        }
    }
    counts
}

/// Drive one cell and report what its records carry.
///
/// The setup window is always the DEFAULT shape -- synchronous, separate, upserting -- so the keys
/// a removal cell removes exist, and the config change an asynchronous cell needs lands before the
/// measured window opens. Nothing the setup appended is counted.
fn measure(shape: Shape) -> Carried {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let engine = open_engine(root);
    engine.load_shard(1);

    for command in upsert_workload(0) {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(
            response.status.ok,
            "setup write failed: {}",
            response.status.message
        );
    }
    if !shape.synchronous {
        let applied = engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        });
        assert!(applied.ok, "set_config(async_storage) failed: {applied:?}");
    }

    let before = log_ids(root);

    let commands = if shape.upsert {
        // A key range the setup did not touch, so these are fresh writes rather than overwrites.
        upsert_workload(100)
    } else {
        removal_workload(0)
    };
    if shape.batched {
        let response = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(
            response.status.ok,
            "batch write failed: {}",
            response.status.message
        );
    } else {
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(
                response.status.ok,
                "measured write failed: {}",
                response.status.message
            );
        }
    }
    carried_since(root, &before)
}

/// Walk every cell and PRINT the table.
///
/// Printed as well as asserted. A guard that only says "still zero" cannot be read by the next
/// person deciding what an ack promises, and that is who this file is for.
fn walk() -> Vec<(Shape, Carried)> {
    let measured: Vec<(Shape, Carried)> = SHAPES
        .iter()
        .map(|shape| (*shape, measure(*shape)))
        .collect();
    println!();
    println!("WHAT A RECORD CARRIES, PER WRITE SHAPE, AT THE SHIPPED DEFAULTS");
    println!(
        "{:<27} {:>7} {:>9} {:>7} {:>6} {:>7} {:>8} {:>9} {:>5} {:>11}",
        "shape",
        "records",
        "operation",
        "results",
        "bytes",
        "removal",
        "deadline",
        "addr-only",
        "pages",
        "no-way-back"
    );
    for (shape, counts) in &measured {
        println!(
            "{:<27} {:>7} {:>9} {:>7} {:>6} {:>7} {:>8} {:>9} {:>5} {:>11}",
            shape.label,
            counts.records,
            counts.records_with_an_operation,
            counts.results,
            counts.results_carrying_bytes,
            counts.results_stating_a_removal,
            counts.results_stating_a_deadline,
            counts.results_naming_only_an_address,
            counts.blocks_in_the_record,
            counts.results_with_no_way_back
        );
    }
    println!();
    measured
}

#[test]
fn the_walk_reaches_every_cell_and_every_cell_appends_records() {
    let measured = walk();
    // FIXTURE REGIME FOR THE WHOLE FILE, AND IT GOES FIRST. Every claim below is about eight
    // cells, and a walk that quietly measured seven of them would agree with all of them.
    assert_eq!(
        measured.len(),
        8,
        "the walk must reach all eight cells, reached {}",
        measured.len()
    );
    for (shape, counts) in &measured {
        assert!(
            counts.records > 0,
            "cell {} appended NO records, so nothing measured about it means anything",
            shape.label.trim()
        );
    }
}

#[test]
fn no_cell_leaves_an_acked_result_with_no_way_back() {
    let measured = walk();
    // Regime: there are results to be about. A walk whose cells recorded nothing would pass the
    // conclusion below by having nothing to fail on. A SUM rather than a count per cell: while the
    // batch rule asked whether the batch had staged any page, the asynchronous batched removal cell
    // recorded no results at all -- it took `append_batch_atomic` and wrote the operations
    // instead -- so a per-cell clause here would have had to carve that cell out. It now records
    // twelve, like its synchronous twin, but the sum stays because a cell legitimately CAN record
    // only operations and `an_operation_or_a_statement_covers_every_record` is what covers that.
    let results: usize = measured.iter().map(|(_, c)| c.results).sum();
    assert!(
        results > 0,
        "the walk recorded no results at all, so a zero below says nothing"
    );
    // THE CONCLUSION, and the one number the durability decision turns on. A result with no
    // operation to re-run, no page in its record, and nothing stated in itself, names a
    // block-store address and nothing else -- and under the single-barrier default that address
    // can be bytes the ack never made durable.
    for (shape, counts) in &measured {
        assert_eq!(
            counts.results_with_no_way_back,
            0,
            "cell {} left {} of {} acked results with NO way back: no operation, no page in the \
             record, and nothing stated in the result itself",
            shape.label.trim(),
            counts.results_with_no_way_back,
            counts.results
        );
    }
}

#[test]
fn an_upsert_carries_its_block_inside_the_record() {
    let measured = walk();
    // Regime: four upsert cells.
    let upserts: Vec<&(Shape, Carried)> =
        measured.iter().filter(|(shape, _)| shape.upsert).collect();
    assert_eq!(
        upserts.len(),
        4,
        "there are four upsert cells, found {}",
        upserts.len()
    );
    // THE CONCLUSION: the bytes travel in the record, and there are at least as many pages as
    // there are results that state nothing but an address. This is the column the durability note
    // omitted, and it is why the recovery suite rebuilds every acked key from the log alone.
    //
    // The regime clause that would belong first here -- "each of these cells recorded an
    // address-only result at all" -- is NOT placed above it, because the same change that empties
    // the page column empties that one too, and a clause behind an overlapping one can never be
    // reached by its own plant. It is asserted on its own in
    // `an_upsert_records_results_that_name_only_an_address`.
    for (shape, counts) in &upserts {
        assert!(
            counts.blocks_in_the_record >= counts.results_naming_only_an_address,
            "upsert cell {} carries {} pages for {} results that name only an address, so some \
             acked value can only be rebuilt from an address the ack did not make durable",
            shape.label.trim(),
            counts.blocks_in_the_record,
            counts.results_naming_only_an_address
        );
    }
}

#[test]
fn an_upsert_records_results_that_name_only_an_address() {
    let measured = walk();
    // The cell count is asserted by `an_upsert_carries_its_block_inside_the_record` and is not
    // repeated here. Two clauses asserting one fact cannot be planted apart, and the plant that
    // moves the upsert count moves the removal count with it -- the two are complementary over
    // one partition, so neither can fail alone.
    let upserts: Vec<&(Shape, Carried)> =
        measured.iter().filter(|(shape, _)| shape.upsert).collect();
    // What makes the carried page LOAD-BEARING rather than spare. A build whose results all
    // carried their own bytes would satisfy `an_upsert_carries_its_block_inside_the_record` without
    // any page mattering, so that test would stop being about anything.
    for (shape, counts) in &upserts {
        assert!(
            counts.results_naming_only_an_address > 0,
            "upsert cell {} recorded {} results and none of them names only an address, so a \
             carried page is no longer what makes this cell recoverable",
            shape.label.trim(),
            counts.results
        );
    }
}

#[test]
fn a_removal_carries_no_block() {
    let measured = walk();
    // Regime: four removal cells.
    let removals: Vec<&(Shape, Carried)> =
        measured.iter().filter(|(shape, _)| !shape.upsert).collect();
    assert_eq!(
        removals.len(),
        4,
        "there are four removal cells, found {}",
        removals.len()
    );
    // These are the cells the limiting rule is about, and this is what makes them so. A removal
    // cell that carried a page would be an upsert wearing the wrong label, and would satisfy
    // `an_operation_or_a_statement_covers_every_record` for the wrong reason.
    for (shape, counts) in &removals {
        assert_eq!(
            counts.blocks_in_the_record,
            0,
            "removal cell {} carried {} pages, so it is not the no-page cell it is labelled",
            shape.label.trim(),
            counts.blocks_in_the_record
        );
    }
}

#[test]
fn an_operation_or_a_statement_covers_every_record() {
    let measured = walk();
    // The cell count is asserted by `a_removal_carries_no_block` and is not repeated here, for the
    // reason given there.
    let removals: Vec<&(Shape, Carried)> =
        measured.iter().filter(|(shape, _)| !shape.upsert).collect();
    // AN ACCOUNTING IDENTITY, not a threshold. With no page to carry, a removal cell is
    // recoverable one of exactly two ways: EVERY result states itself, or EVERY record kept its
    // operation. "At least one result states itself" would be a maximum over one member, and a
    // cell that lost eleven of twelve removals would satisfy it.
    for (shape, counts) in &removals {
        let stated = counts.results_stating_themselves();
        assert!(
            stated == counts.results || counts.every_record_kept_its_operation(),
            "removal cell {}: {} of {} results state themselves and {} of {} records kept an \
             operation, so some acked removal is neither stated nor re-runnable",
            shape.label.trim(),
            stated,
            counts.results,
            counts.records_with_an_operation,
            counts.records
        );
    }
}
