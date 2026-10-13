// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Does the command surface let ONE key hold TWO kinds at once?
//!
//! # WHY THIS IS A DRIVEN TEST AND NOT A READ OF THE MAPS
//!
//! The resident maps are the layer that DESCRIBES what a shard holds. The command handler is the
//! layer that DECIDES what a shard is allowed to hold. `shard.strings` and `shard.hashes` being
//! separate maps says nothing about whether a client can populate both under one key -- a handler
//! that refused the second write would leave exactly the same two maps in the tree. Reading the
//! first layer and concluding about the second is how this question gets answered wrongly, so the
//! answer here comes from `execute`/`ExecuteRequest`, the surface a client actually calls.
//!
//! # THE THREE OUTCOMES, AND WHY ALL THREE ARE DISCRIMINATED
//!
//! A test that only asserts the one outcome it expects passes under two of the three, because
//! "the second write answered ok" is true for COEXIST and for SILENTLY REPLACED alike, and a
//! read-back of only the second kind cannot tell them apart. So both kinds are read back after
//! both writes and the outcome is CLASSIFIED before it is asserted:
//!
//!   * REFUSED -- the second write answers not-ok. A key addresses exactly one object, and
//!     something enforces it.
//!   * COEXIST -- both kinds read back. One key can hold two kinds.
//!   * SILENTLY REPLACED -- the second write answers ok and displaces the first. They cannot
//!     coexist, and nothing says so.
//!
//! A fourth shape -- ok, and the SECOND kind is the one that vanished -- is not one of the three
//! and panics rather than being folded into the nearest neighbour.

use super::*;

/// Which of the three the surface does. Named, so the assertion below reads as the finding.
#[derive(Debug, PartialEq, Eq)]
enum TwoKindOutcome {
    Refused,
    Coexist,
    SilentlyReplaced,
}

fn string_set(engine: &TemporalEngine, key: &str, value: &[u8]) -> Status {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: value.to_vec(),
            },
        })
        .status
}

fn hash_set(engine: &TemporalEngine, key: &str, field: &str, value: &[u8]) -> Status {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: key.to_string(),
                field: field.to_string(),
                value: value.to_vec(),
            },
        })
        .status
}

/// `GET key` through the command surface. `None` for "the surface does not answer with a value",
/// whether that is a nil or a not-ok status -- both mean the string kind is not readable.
fn string_get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    if !answer.status.ok {
        return None;
    }
    match answer.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("StringGet {key} answered {other:?}"),
    }
}

/// `HGET key field` through the command surface, with the same convention as [`string_get`].
fn hash_get(engine: &TemporalEngine, key: &str, field: &str) -> Option<Vec<u8>> {
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: key.to_string(),
            field: field.to_string(),
        },
    });
    if !answer.status.ok {
        return None;
    }
    match answer.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("HashGet {key}/{field} answered {other:?}"),
    }
}

/// Classify from the second write status and BOTH read-backs taken after it.
fn classify(
    second_write: &Status,
    first_kind_after: bool,
    second_kind_after: bool,
) -> TwoKindOutcome {
    if !second_write.ok {
        return TwoKindOutcome::Refused;
    }
    match (first_kind_after, second_kind_after) {
        (true, true) => TwoKindOutcome::Coexist,
        (false, true) => TwoKindOutcome::SilentlyReplaced,
        (first, second) => panic!(
            "the second write answered ok and the outcome is none of the three: the first kind \
             reads back {first} and the second {second}"
        ),
    }
}

/// A string write then a HASH write at the same key.
#[test]
fn a_hash_write_lands_on_a_key_that_already_holds_a_string() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let first = string_set(&engine, "foo", b"string-value");
    assert!(first.ok, "the string write failed: {}", first.message);
    assert_eq!(
        string_get(&engine, "foo").as_deref(),
        Some(&b"string-value"[..]),
        "the string has to be readable BEFORE the hash write, or the outcome below is about an \
         empty key rather than about two kinds"
    );

    let second = hash_set(&engine, "foo", "field-one", b"hash-value");

    let string_after = string_get(&engine, "foo");
    let hash_after = hash_get(&engine, "foo", "field-one");
    let outcome = classify(&second, string_after.is_some(), hash_after.is_some());

    assert_eq!(
        outcome,
        TwoKindOutcome::Coexist,
        "string reads back {string_after:?}, hash reads back {hash_after:?}, second write status \
         ok={} message={:?}",
        second.ok,
        second.message
    );
    // The values, not just presence: a COEXIST in which one kind answers the OTHER kind bytes
    // would satisfy the classification above and would not be two objects.
    assert_eq!(string_after.as_deref(), Some(&b"string-value"[..]));
    assert_eq!(hash_after.as_deref(), Some(&b"hash-value"[..]));

    // Corroboration only. The command surface above is the authority; this says the two kinds
    // are in fact filed in two separate resident maps under the one key, so the coexistence is
    // two objects and not one map answering twice.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    assert!(shard.strings.contains_key("foo"));
    assert!(shard.hashes.contains_key("foo"));
}

/// The same question in the other order, because a one-directional check cannot tell an
/// ENFORCED invariant from a handler that happens to consult only the map it writes.
#[test]
fn a_string_write_lands_on_a_key_that_already_holds_a_hash() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let first = hash_set(&engine, "bar", "field-one", b"hash-value");
    assert!(first.ok, "the hash write failed: {}", first.message);
    assert_eq!(
        hash_get(&engine, "bar", "field-one").as_deref(),
        Some(&b"hash-value"[..])
    );

    let second = string_set(&engine, "bar", b"string-value");

    let hash_after = hash_get(&engine, "bar", "field-one");
    let string_after = string_get(&engine, "bar");
    let outcome = classify(&second, hash_after.is_some(), string_after.is_some());

    assert_eq!(
        outcome,
        TwoKindOutcome::Coexist,
        "hash reads back {hash_after:?}, string reads back {string_after:?}, second write status \
         ok={} message={:?}",
        second.ok,
        second.message
    );
    assert_eq!(hash_after.as_deref(), Some(&b"hash-value"[..]));
    assert_eq!(string_after.as_deref(), Some(&b"string-value"[..]));
}

/// WHAT `key_membership_size` IS AND IS NOT, as a guard rather than as a sentence.
///
/// The twelve-map sum looks like a multi-kind presence count, and it is not one: `strings` and
/// `hashes` are not among the twelve, so a key holding BOTH of the kinds the tests above put
/// there contributes NOTHING to it. It counts per-key auxiliary state -- postings, a deadline,
/// control state, the context maps -- and both of its callers feed `delta_key_state_change`,
/// which reads the number only through `<` against the same key earlier size.
///
/// This matters to the consolidation question because "twelve maps are summed for one key" would
/// otherwise read as evidence that a caller depends on multi-kind presence. None does, and this
/// fails if a kind map is ever added to the sum, which is what would change that.
#[test]
fn the_membership_sum_does_not_count_either_kind() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    assert!(string_set(&engine, "baz", b"string-value").ok);
    assert!(hash_set(&engine, "baz", "field-one", b"hash-value").ok);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    assert!(shard.strings.contains_key("baz"));
    assert!(shard.hashes.contains_key("baz"));
    assert_eq!(
        super::super::key_membership_size(shard, "baz"),
        0,
        "a key holding two kinds is in NONE of the twelve maps the membership sum adds up, so \
         the sum is not a kind-presence count and no caller of it can be reading one"
    );
}
