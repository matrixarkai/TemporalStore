// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SEQUENCE READ SHOULD NOT TAKE THE EXCLUSIVE SHARD LOCK.
//!
//! `SequenceQuery` read each row where it sits in a feature block, uncached, under the shard table's
//! exclusive lock, so it waited for every writer and every other reader. It now takes the same plan as
//! the feature range reads: the window's addresses under the shared guard, the rows decoded after it
//! drops, with the WAL-resident blocks pinned exactly as for `FeatureQuery` (the plan is one variant).
//!
//! TWO ARMS.
//!   * `a_sequence_read_answers_while_the_shared_lock_is_held` -- the test holds the shared lock and
//!     the read must answer from another thread.
//!   * `a_sequence_read_answers_what_was_added` -- the rows against a model, over a window, a count
//!     cap, filters (a cap that cuts through refused rows answers fewer rows than the cap), a
//!     replaced instant, an absent key and a lapsed key.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{FeatureFilter, FeatureFilterOp, SequenceFeatureRow};
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const BASE: u64 = 1_787_270_070_000;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn row(index: u64, gid: u64) -> SequenceFeatureRow {
    SequenceFeatureRow {
        timestamp_ms: BASE + index * 1_000,
        gid,
        action_type: (gid % 5) as u32 + 1,
        duration: 3 + (gid % 11) as u32,
        author_id: 9_000 + gid,
    }
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn rows_of(response: CommandResponse) -> Vec<SequenceFeatureRow> {
    match response {
        CommandResponse::SequenceRows { rows } => rows,
        other => panic!("not rows: {other:?}"),
    }
}

fn add(engine: &TemporalEngine, rows: Vec<SequenceFeatureRow>) {
    run(engine, Command::SequenceAdd { key: "q".to_string(), rows });
}

fn query(
    start_ms: u64,
    end_ms: u64,
    count: usize,
    filters: Vec<FeatureFilter>,
) -> Command {
    Command::SequenceQuery { key: "q".to_string(), start_ms, end_ms, count, filters }
}

#[test]
fn a_sequence_read_answers_while_the_shared_lock_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    add(&engine, (0..40).map(|i| row(i, i)).collect());
    for (label, command) in [
        ("SequenceQuery", query(0, u64::MAX, 100, Vec::new())),
        (
            "SequenceQuery filtered",
            query(
                0,
                u64::MAX,
                100,
                vec![FeatureFilter { field: "action_type".into(), op: FeatureFilterOp::Equal, value: 1 }],
            ),
        ),
        (
            "SequenceQuery of an absent key",
            Command::SequenceQuery {
                key: "nope".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: 10,
                filters: Vec::new(),
            },
        ),
    ] {
        let (got, worker) = engine.shards.with_state(1, |owner| {
            assert!(owner.is_some());
            let (sender, receiver) = mpsc::channel();
            let worker = {
                let engine = Arc::clone(&engine);
                std::thread::spawn(move || {
                    let _ = sender.send(run(&engine, command));
                })
            };
            let got = receiver.recv_timeout(Duration::from_secs(5));
            (got, worker)
        });
        worker.join().expect("the read thread");
        assert!(got.is_ok(), "{label}: no answer while the shared lock was held");
    }
}

#[test]
fn a_sequence_read_answers_what_was_added() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let mut model: BTreeMap<u64, SequenceFeatureRow> = BTreeMap::new();
    let mut put = |rows: Vec<SequenceFeatureRow>, model: &mut BTreeMap<u64, SequenceFeatureRow>| {
        for r in &rows {
            model.insert(r.timestamp_ms, r.clone());
        }
        add(&engine, rows);
    };
    put((0..60).map(|i| row(i, i)).collect(), &mut model);
    for i in 60..70 {
        put(vec![row(i, i)], &mut model);
    }
    put(vec![row(10, 1_010), row(65, 1_065)], &mut model);
    let at = |index: u64| BASE + index * 1_000;
    let action_one = vec![FeatureFilter { field: "action_type".into(), op: FeatureFilterOp::Equal, value: 1 }];
    let mut compared = 0usize;
    let mut nonempty = 0usize;
    for (start, end, count, filters) in [
        (0u64, u64::MAX, 1_000usize, Vec::new()),
        (at(20), at(40), 1_000, Vec::new()),
        (at(60), at(70), 4, Vec::new()),
        (0, u64::MAX, 1_000, action_one.clone()),
        (0, u64::MAX, 7, action_one.clone()),
        (at(10), at(10), 1_000, Vec::new()),
        (at(900), u64::MAX, 1_000, Vec::new()),
        (0, u64::MAX, 0, Vec::new()),
    ] {
        let expected: Vec<SequenceFeatureRow> = if start > end {
            Vec::new()
        } else {
            model
                .range(start..=end)
                .take(count)
                .map(|(_, r)| r.clone())
                .filter(|r| filters.is_empty() || r.action_type == 1)
                .collect()
        };
        let got = rows_of(run(&engine, query(start, end, count, filters)));
        assert_eq!(expected, got, "[{start}, {end}] count {count}");
        compared += 1;
        nonempty += !got.is_empty() as usize;
    }
    assert_eq!(8, compared);
    assert!(nonempty >= 5, "only {nonempty} answers held a row");
    // A LAPSED KEY answers empty, through the ordinary path that drops it.
    run(&engine, Command::CommonExpire { key: "q".into(), ttl_ms: 1 });
    std::thread::sleep(Duration::from_millis(30));
    assert!(rows_of(run(&engine, query(0, u64::MAX, 1_000, Vec::new()))).is_empty());
}
