// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE GATE FOR FILING ONE INDEX ENTRY PER PAGE: IT SHIPS OFF, AND IT ANSWERS BOTH WAYS.
//!
//! # WHY THERE IS A GATE AT ALL
//!
//! A container's index holds one entry per ELEMENT, and the page those entries name already
//! carries each element's key in its payload. The index is also a DERIVED PROJECTION of the model
//! maps -- `visit_model_live_blocks` emits one entry per member, and `rebuild_bucket_first_index`
//! re-derives the whole projection twice inside a single compaction sweep. So the filing and the
//! derivation cannot be changed in separate commits: whichever lands first is undone by the other
//! before the sweep returns.
//!
//! A projection can be computed two ways behind one switch. That is what this gate is for, and it
//! is the only reason the collapse can land as a series of reviewable steps rather than as one
//! change touching the derivation, the rebuild, the authority check, the ordinal, the removal
//! representation, the replay arm, the index-log emitter, the reconcile and the listing at once.
//!
//! # WHAT THIS TEST PROVES, AND WHAT IT DOES NOT
//!
//! Stated plainly because a gate test is easy to over-read. At this step **nothing reads the
//! gate**: there is one path through the engine, not two. So this cannot assert that two code paths
//! agree, or that either is correct -- there is only one.
//!
//! What it does pin is the two things that can be true now and must stay true:
//!
//!   * the DEFAULT IS OFF, so no deployment changes behaviour by taking this commit; and
//!   * the SWITCH ANSWERS IN BOTH DIRECTIONS, so when step two gives it a reader, the reader can
//!     actually be reached from a deployment and from a test.
//!
//! The second half is not idle. A gate whose reader can never return `true` is the shape that
//! strands a feature silently, and this repository is already carrying a lever blocked that way --
//! a gate that shipped off and was never flipped. The series this begins ends by flipping the
//! default and then DELETING the gate and the per-element path, so the two projections do not
//! become a permanent fork.
//!
//! # THE VARIABLE'S NAME IS READ, NOT RETYPED
//!
//! The name is a constant beside the gate, and this test reads that constant rather than spelling
//! the string again. A retyped name is a gate an operator cannot turn on and a test that passes
//! anyway, which is the same class as a hand-written subject list going stale.
//!
//! No other test in this crate touches this variable, which is what makes setting it here safe:
//! the value is process-global, so the discipline is that exactly one test owns each name. It is
//! removed again at the end so a later test sees the shipped default.

#![allow(clippy::all)]
use crate::engine::{container_index_files_one_entry_a_page, TS_CONTAINER_ONE_ENTRY_A_PAGE};

/// THE SHIPPED DEFAULT IS OFF.
///
/// Read with the variable removed, which is the state a deployment that has not heard of this
/// gate is in.
#[test]
fn the_one_entry_a_page_gate_ships_off() {
    std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);
    assert!(
        !container_index_files_one_entry_a_page(),
        "the gate is ON by default. Taking this commit would change how a container's index is \
         filed for every deployment that has not asked for it, which is the one thing a staged \
         series must not do at its first step"
    );
}

/// THE SWITCH ANSWERS IN BOTH DIRECTIONS, IN THE VOCABULARY AN OPERATOR WRITES.
///
/// Every word the shared flag reader understands, and the surrounding whitespace a unit file, a
/// heredoc and a shell export all leave behind -- because a gate that silently reads `" 1"` as off
/// is a gate that cannot be turned on, and that exact defect has recurred in this tree.
#[test]
fn the_one_entry_a_page_gate_can_be_reached_in_both_directions() {
    for written in ["1", "true", "yes", "on", "On", "TRUE", " 1", "\tyes\n", "  on  "] {
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, written);
        assert!(
            container_index_files_one_entry_a_page(),
            "{written:?} did not turn the gate on. Step two gives this gate a reader, so a value \
             nobody can turn on is a path nobody can reach"
        );
    }
    for written in ["0", "false", "no", "off", "Off", " 0 ", "\tNO\n"] {
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, written);
        assert!(
            !container_index_files_one_entry_a_page(),
            "{written:?} did not turn the gate off"
        );
    }
    // A VALUE NOBODY CAN READ FALLS BACK TO THE DEFAULT, which for this gate is off. It is
    // asserted rather than assumed because the opposite -- an unreadable value reading as the
    // non-default -- is what the shared flag vocabulary exists to prevent.
    for written in ["", "wat", "2", "enabled", "  "] {
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, written);
        assert!(
            !container_index_files_one_entry_a_page(),
            "{written:?} was not read as the default. An unreadable value is not a request to \
             turn this on"
        );
    }
    // Left as the deployment would have it, so a later test reads the shipped default.
    std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);
    assert!(
        !container_index_files_one_entry_a_page(),
        "the variable was removed and the gate is still on"
    );
}
