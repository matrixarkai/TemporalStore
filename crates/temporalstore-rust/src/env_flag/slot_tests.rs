// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The three hot flags are read from the environment once per operation.
//!
//! The slots must change nothing an operator can see between operations, and nothing about how a flag
//! is parsed. The first is pinned by flipping a variable around a guard; the second by keeping the
//! three expressions the slots replaced here, unchanged, as oracles for the pure functions that took
//! over their parsing.

use super::*;

/// A name no other test touches, so these can run beside the rest of the suite.
fn unique(label: &str) -> String {
    format!("TS_TEST_ENV_SLOT_{label}_{}", std::process::id())
}

fn read_on(name: &str) -> impl Fn() -> bool + '_ {
    move || is_on_word(std::env::var(name).ok().as_deref())
}

#[test]
fn a_flag_asked_twice_in_an_operation_is_read_from_the_environment_once() {
    let name = unique("ONCE");
    std::env::set_var(&name, "1");
    let operation = OperationEnv::begin();
    assert!(cached_flag(Slot::BulkIngest, read_on(&name)));
    std::env::set_var(&name, "0");
    assert!(
        cached_flag(Slot::BulkIngest, read_on(&name)),
        "a change made during the operation was seen"
    );
    drop(operation);
    assert!(
        !cached_flag(Slot::BulkIngest, read_on(&name)),
        "after the operation the live value has to be read again"
    );
    std::env::remove_var(&name);
}

#[test]
fn outside_an_operation_every_read_is_live() {
    let name = unique("LIVE");
    std::env::set_var(&name, "1");
    assert!(cached_flag(Slot::OutcomeItems, read_on(&name)));
    std::env::set_var(&name, "0");
    assert!(!cached_flag(Slot::OutcomeItems, read_on(&name)));
    std::env::remove_var(&name);
}

#[test]
fn a_flag_that_was_off_when_first_read_stays_off_for_the_operation() {
    let name = unique("OFF");
    std::env::remove_var(&name);
    let operation = OperationEnv::begin();
    assert!(!cached_flag(Slot::BinaryFrame, read_on(&name)));
    std::env::set_var(&name, "1");
    assert!(
        !cached_flag(Slot::BinaryFrame, read_on(&name)),
        "the slot has to remember an off value as well as an on one"
    );
    drop(operation);
    assert!(cached_flag(Slot::BinaryFrame, read_on(&name)));
    std::env::remove_var(&name);
}

#[test]
fn each_slot_holds_its_own_flag() {
    let (a, b, c) = (unique("A"), unique("B"), unique("C"));
    std::env::set_var(&a, "1");
    std::env::set_var(&b, "0");
    std::env::set_var(&c, "1");
    let _operation = OperationEnv::begin();
    assert!(cached_flag(Slot::BulkIngest, read_on(&a)));
    assert!(!cached_flag(Slot::OutcomeItems, read_on(&b)));
    assert!(cached_flag(Slot::BinaryFrame, read_on(&c)));
    for name in [&a, &b, &c] {
        std::env::set_var(name, "x");
    }
    assert!(cached_flag(Slot::BulkIngest, read_on(&a)));
    assert!(!cached_flag(Slot::OutcomeItems, read_on(&b)));
    assert!(cached_flag(Slot::BinaryFrame, read_on(&c)));
    for name in [&a, &b, &c] {
        std::env::remove_var(name);
    }
}

#[test]
fn a_guard_begun_inside_another_leaves_the_outer_operation_alone() {
    let name = unique("NEST");
    std::env::set_var(&name, "1");
    let outer = OperationEnv::begin();
    assert!(cached_flag(Slot::BulkIngest, read_on(&name)));
    {
        let _inner = OperationEnv::begin();
        std::env::set_var(&name, "0");
        assert!(cached_flag(Slot::BulkIngest, read_on(&name)));
    }
    assert!(
        cached_flag(Slot::BulkIngest, read_on(&name)),
        "the inner guard ended the outer operation"
    );
    drop(outer);
    assert!(!cached_flag(Slot::BulkIngest, read_on(&name)));
    std::env::remove_var(&name);
}

#[test]
fn an_operation_belongs_to_the_thread_that_began_it() {
    let name = unique("THREAD");
    std::env::set_var(&name, "1");
    let _operation = OperationEnv::begin();
    assert!(cached_flag(Slot::BulkIngest, read_on(&name)));
    std::env::set_var(&name, "0");
    let other = {
        let name = name.clone();
        std::thread::spawn(move || cached_flag(Slot::BulkIngest, read_on(&name)))
    };
    assert!(
        !other.join().unwrap(),
        "another thread read the first thread's slot, not the environment"
    );
    std::env::remove_var(&name);
}

/// What `bulk_ingest_mode` evaluated before it read through a slot.
fn the_bulk_ingest_expression_it_replaced(raw: Option<&str>) -> bool {
    matches!(
        raw.map(str::to_owned)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// What `wal_outcome_items_enabled` and `binary_frame_enabled` evaluated.
fn the_opt_out_expression_it_replaced(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(str::to_owned)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

const WORDS: &[Option<&str>] = &[
    None,
    Some(""),
    Some(" "),
    Some("1"),
    Some(" 1 "),
    Some("0"),
    Some(" 0\n"),
    Some("true"),
    Some("TRUE"),
    Some("True"),
    Some("yes"),
    Some("YES"),
    Some("on"),
    Some("On"),
    Some("ON"),
    Some("false"),
    Some("False"),
    Some("no"),
    Some("NO"),
    Some("off"),
    Some("OFF"),
    Some("wat"),
    Some("2"),
    Some("onn"),
    Some("t"),
    Some("\u{a0}1"),
    Some("\u{e9}"),
    Some("\u{49}"),
];

#[test]
fn the_on_words_are_the_words_the_bulk_ingest_gate_always_accepted() {
    for raw in WORDS {
        assert_eq!(
            is_on_word(*raw),
            the_bulk_ingest_expression_it_replaced(*raw),
            "{raw:?}"
        );
    }
}

#[test]
fn the_off_words_are_the_words_the_opt_out_flags_always_refused() {
    for raw in WORDS {
        assert_eq!(
            !is_off_word(*raw),
            the_opt_out_expression_it_replaced(*raw),
            "{raw:?}"
        );
    }
}

/// The body of `fn name` in `source`, with the comment lines dropped.
fn body_of(source: &str, name: &str) -> String {
    let start = source
        .find(&format!("fn {name}("))
        .unwrap_or_else(|| panic!("fn {name} must exist"));
    let rest = &source[start..];
    let end = rest.find("\n}\n").expect("the function must end");
    rest[..end]
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn the_hot_flag_readers_go_through_their_slot() {
    // A behavioural test cannot see a reader that reads the environment directly and gets the right
    // answer, so the source is read: these three are called on every write, and one that stops asking
    // its slot quietly puts the scan back.
    for (source, name, slot) in [
        (
            include_str!("../engine.rs"),
            "bulk_ingest_mode",
            "Slot::BulkIngest",
        ),
        (
            include_str!("../wal.rs"),
            "wal_outcome_items_enabled",
            "Slot::OutcomeItems",
        ),
        (
            include_str!("../log_framing.rs"),
            "binary_frame_enabled",
            "Slot::BinaryFrame",
        ),
    ] {
        let body = body_of(source, name);
        assert!(
            body.contains("cached_flag("),
            "{name} does not read through a slot"
        );
        assert!(body.contains(slot), "{name} reads the wrong slot");
    }
}

#[test]
fn an_operation_is_opened_where_a_write_enters_the_engine() {
    let source = include_str!("../engine.rs");
    let body = body_of(source, "execute_with_storage_override");
    assert!(
        body.contains("OperationEnv::begin()"),
        "no operation is opened, so the slots would never be used"
    );
}
