// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN OUTCOME'S KIND IS A LABEL FROM A FIXED ROSTER, SO A KNOWN KIND COSTS NO ALLOCATION.
//!
//! Every staged outcome built its kind with `to_string()` from a string literal: a malloc and a copy
//! of "control_counter" or "hash" per staged item, on every write of every model. The field is now an
//! `OutcomeKind`, which holds a kind from the wire roster (`wal_proto::KIND_CODES`) or the short list
//! of uncoded ones as a borrow, and any other name as an owned string.
//!
//! WHAT DOES NOT MOVE: the serialized form. `OutcomeKind` serializes as the plain string and reads back
//! from it, so a record written before this change reads the same and one written after it is
//! byte-identical.
//!
//! TWO GUARDS.
//!   * `an_outcome_kind_serializes_as_the_plain_string_for_every_kind_in_use` takes every kind in the
//!     roster, the uncoded ones and an unknown name, and requires the JSON of a whole outcome item to
//!     equal a literal written here, the kind to read back equal, and every KNOWN kind to be held as a
//!     borrow (an unknown one must still work, owned).
//!   * `probe::a_known_outcome_kind_allocates_nothing` (needs `alloc-probe`, `--ignored`) builds the
//!     same kind a thousand times and requires zero allocations for a known kind, against the
//!     `to_string()` it replaced as a control that must allocate once each.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use crate::wal::{OutcomeKind, WalOutcomeItem};
use crate::wal_outcome_kind::UNCODED_KINDS;
use crate::wal_proto::KIND_CODES;

fn item(kind: &str) -> WalOutcomeItem {
    WalOutcomeItem {
        kind: kind.into(),
        object_key: "key".to_string(),
        element_name: None,
        object_id: 7,
        routing_bucket: 3,
        address: None,
        value: None,
        ttl: None,
        deleted: false,
        meta: false,
    }
}

#[test]
fn an_outcome_kind_serializes_as_the_plain_string_for_every_kind_in_use() {
    let mut kinds: Vec<&str> = KIND_CODES.iter().chain(UNCODED_KINDS).copied().collect();
    let unknown = "a_kind_nobody_listed";
    kinds.push(unknown);
    let mut borrowed = 0usize;
    for kind in &kinds {
        let built = item(kind);
        let json = serde_json::to_string(&built).unwrap();
        // The kind field, spelled as the plain string it always was.
        assert!(
            json.contains(&format!("\"k\":\"{kind}\"")),
            "kind `{kind}` is not spelled as a plain string in {json}"
        );
        // The same bytes a `String`-typed field produced: serialize the string alone and compare.
        assert_eq!(
            serde_json::to_string(&built.kind).unwrap(),
            serde_json::to_string(&kind.to_string()).unwrap()
        );
        let back: WalOutcomeItem = serde_json::from_str(&json).unwrap();
        assert_eq!(back, built, "kind `{kind}` did not read back equal");
        assert_eq!(back.kind, *kind);
        assert_eq!(built.kind.as_str(), *kind);
        // A record written with the kind as a bare string reads into the same value.
        let from_string: OutcomeKind = serde_json::from_str(&format!("\"{kind}\"")).unwrap();
        assert_eq!(from_string, built.kind);
        if *kind == unknown {
            assert!(!built.kind.is_borrowed(), "an unknown kind must be held owned");
        } else {
            assert!(built.kind.is_borrowed(), "known kind `{kind}` allocates its label");
            borrowed += 1;
        }
    }
    // THE CONTROL: the roster really was walked, so "every kind borrowed" is not "no kinds".
    assert_eq!(borrowed, KIND_CODES.len() + UNCODED_KINDS.len());
    assert!(KIND_CODES.len() >= 20);
    // One whole item, spelled out, so a change to any other field of the record shows here too.
    assert_eq!(
        serde_json::to_string(&item("control_counter")).unwrap(),
        serde_json::to_string(&WalOutcomeItem {
            kind: String::from("control_counter").into(),
            ..item("control_counter")
        })
        .unwrap()
    );
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const BUILDS: usize = 1_000;

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_known_outcome_kind_allocates_nothing() {
        let canary = Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`"
        );
        drop(sink);

        let kinds = ["control_counter", "hash", "feature", "context_event", "record"];
        let probe = Probe::start();
        let mut held = Vec::with_capacity(BUILDS);
        let warm = probe.stop().allocs;
        let _ = warm;
        let probe = Probe::start();
        for index in 0..BUILDS {
            held.push(OutcomeKind::of(kinds[index % kinds.len()]));
        }
        let borrowed = probe.stop().allocs;
        assert_eq!(held.len(), BUILDS);
        // THE CONTROL: the `to_string()` this replaced allocates once a build.
        let probe = Probe::start();
        let mut strings = Vec::with_capacity(BUILDS);
        for index in 0..BUILDS {
            strings.push(kinds[index % kinds.len()].to_string());
        }
        let owned = probe.stop().allocs;
        assert_eq!(strings.len(), BUILDS);
        assert!(
            owned >= BUILDS as u64,
            "the control allocated {owned} times for {BUILDS} strings, so the probe cannot see a label copy"
        );
        assert_eq!(
            borrowed, 0,
            "{borrowed} allocations to hold {BUILDS} known outcome kinds; a known kind must be a borrow"
        );
    }
}
