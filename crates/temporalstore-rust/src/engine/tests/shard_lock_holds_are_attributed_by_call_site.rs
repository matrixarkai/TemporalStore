// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! EVERY OWNER HOLD IS ATTRIBUTED TO THE LINE THAT TOOK IT.
//!
//! The probe is off by default and, when on, records each guard's hold under the `file:line` of the
//! owner read/write call (`#[track_caller]`). This pins that it does so, that a poisoned lock still
//! records the hold, and that the entries a `with_*` closure reports land on its own line.
//! No time bound is asserted: a hold-time bound is a timing test.
//!
//! rust-internal: measures this crate's own lock holds, no product behaviour

#![allow(clippy::all)]

use super::*;

use crate::engine::shard_guard;
use std::sync::Arc;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine.load_shard(2);
    engine
}

const THIS_FILE: &str = "shard_lock_holds_are_attributed_by_call_site.rs";

// One test: the probe and its table are process-wide, so two tests would see each other's holds.
#[test]
fn a_hold_is_recorded_under_the_line_that_took_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    shard_guard::set_hold_probe(false);
    shard_guard::reset_holds();
    engine.shards.with_state(1, |state| assert!(state.is_some()));
    assert!(shard_guard::hold_report().0.is_empty(), "the probe is off: nothing may be recorded");

    shard_guard::set_hold_probe(true);
    let write_line = line!() + 1;
    engine.shards.with_state_mut(1, |state| assert!(state.is_some()));
    let read_line = line!() + 1;
    engine.shards.with_state(1, |state| assert!(state.is_some()));
    let with_line = line!() + 1;
    engine.shards.with_shard(1, |state, visited| {
        assert!(state.is_some());
        visited.add(7);
    });
    shard_guard::set_hold_probe(false);

    let (report, dropped) = shard_guard::hold_report();
    assert_eq!(0, dropped);
    let find = |line: u32, write: bool| {
        report
            .iter()
            .find(|site| site.line == line && site.write == write && site.file.ends_with(THIS_FILE))
            .unwrap_or_else(|| panic!("no hold recorded at line {line}: {report:?}"))
    };
    assert_eq!(1, find(write_line, true).count);
    assert_eq!(1, find(read_line, false).count);
    let with = find(with_line, false);
    assert_eq!((1, 7, 7), (with.count, with.entries_total, with.entries_max));
    assert_eq!(7, shard_guard::last_hold_entries());

    // The unwinding owner hold is reported. Poison stays on that owner,
    // while the independently loaded peer remains readable.
    shard_guard::reset_holds();
    shard_guard::set_hold_probe(true);
    let mut poisoned_line = 0;
    let fault = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        poisoned_line = line!() + 1;
        engine.shards.with_state_mut(1, |state| {
            assert!(state.is_some());
            panic!("poison owner one");
        });
    }));
    assert!(fault.is_err());
    assert!(!crate::engine::shard_write_guard::any_shard_guard_held());
    engine.shards.with_state(2, |state| assert!(state.is_some()));
    let mut served = false;
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine.shards.with_state(1, |_| served = true);
    }));
    assert!(refused.is_err() && !served);
    shard_guard::set_hold_probe(false);
    let (report, _) = shard_guard::hold_report();
    assert!(
        report.iter().any(|site| site.file.ends_with(THIS_FILE)
            && site.line == poisoned_line && site.write && site.count == 1),
        "the unwinding owner write was not recorded: {report:?}"
    );
}
