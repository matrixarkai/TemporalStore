// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the user-facing command surface answers, and what it advertises.
//!
//! Two obligations live here, and neither is visible from inside a single command arm.
//!
//! ONE: a write that REPLACES a key's whole value discards the deadline it overwrote, and a
//! write that AMENDS an existing value keeps it. The engine spells those as two different
//! commands -- `StringSetConditional` with no `ttl_ms` clears the deadline, `StringSet` leaves
//! it in place -- so which one a RESP verb reaches for IS the semantic, and picking the wrong
//! one is spelled as an ordinary-looking line that no type checks.
//!
//! TWO: the `COMMAND` descriptor table is a hand-maintained list beside the dispatcher's
//! `match`, not derived from it, so the two can drift and only the table is wrong.

#![allow(clippy::all)]
use super::*;

use crate::redis::{execute_redis_command, RespValue};

/// Drive one RESP command against a live engine on shard 1.
fn resp(engine: &TemporalEngine, args: &[&str]) -> RespValue {
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.as_bytes().to_vec()).collect();
    execute_redis_command(argv, 1, |command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message.clone())
        }
    })
}

/// `PTTL` in milliseconds: positive when a deadline is armed, -1 when the key exists with no
/// deadline, -2 when there is no key.
fn pttl(engine: &TemporalEngine, key: &str) -> i64 {
    match resp(engine, &["PTTL", key]) {
        RespValue::Integer(value) => value,
        other => panic!("PTTL {key} answered {other:?}"),
    }
}

fn get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match resp(engine, &["GET", key]) {
        RespValue::Bulk(value) => value,
        other => panic!("GET {key} answered {other:?}"),
    }
}

/// A long deadline, so nothing below can pass by the key having actually expired.
const HOUR_MS: &str = "3600000";

/// A write that REPLACES the value discards the deadline; a write that AMENDS it keeps one.
///
/// WHAT WENT WRONG. `SET` reaches for `StringSetConditional` and so clears the deadline, which
/// is right. `GETSET` and `MSET` reached for `StringSet`, which does not touch the expiry
/// index at all -- it calls `remove_if_expired` and then writes the page. So a caller who
/// armed a one-minute deadline on a key and then replaced its contents with `GETSET` or `MSET`
/// got a key that still vanished a minute later, carrying a deadline it never asked to keep
/// and that nothing in either answer mentions. `GETSET` returned the old value and `MSET`
/// returned `OK`; the key was gone by the next minute.
///
/// WHY THOSE TWO AND NOT THE OTHERS. The distinction is not "does it write a string" -- every
/// verb below writes a string. It is whether the caller supplied the WHOLE new value. `GETSET`
/// and `MSET` do, exactly as `SET` does, and carry the same "this key is now this, and nothing
/// else is implied" meaning. `APPEND`, `SETRANGE`, `INCR`, `INCRBY`, `DECR` and `INCRBYFLOAT`
/// derive the new value FROM the old one; they amend a key that is already there and on
/// purpose leave its lifetime alone. Those are asserted below as controls, because the wrong
/// fix here -- teaching `StringSet` itself to clear -- would satisfy every claim about `GETSET`
/// and `MSET` while silently making `INCR` reset a countdown.
///
/// HALVES ASSERTED SEPARATELY. `GETSET` and `MSET` are two arms and two claims. They were both
/// wrong in the same way, which is exactly the case where one combined "no deadline survives"
/// count reads full from a fixed arm and hides the other.
///
/// `MSETNX` IS DELIBERATELY NOT ASSERTED. It shares the corrected write, but it only writes
/// when NONE of its keys exist, and a key that does not exist has no deadline to discard --
/// `CommonExists` calls `remove_if_expired` first. There is no state in which its behaviour
/// differs, so a claim about it would pass no matter which write it used. Asserting it would
/// be a vacuous row, so it is named here instead of counted there.
#[test]
fn a_value_replacing_write_discards_the_deadline_it_overwrote() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- DENOMINATOR: arming a deadline through this path really arms one ---------------
    // Without this every -1 below would also be produced by `SET ... PX` silently doing
    // nothing, and the test would pass while proving the opposite of what it claims.
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "replaced:getset", "v1", "PX", HOUR_MS]),
    );
    let armed = pttl(&engine, "replaced:getset");
    assert!(
        armed > 0,
        "DENOMINATOR: `SET k v PX {HOUR_MS}` must arm a deadline, and PTTL reported {armed}. \
         Nothing below means anything if a deadline is never armed in the first place.",
    );

    // ---- HALF ONE: GETSET -------------------------------------------------------------
    assert_eq!(
        RespValue::Bulk(Some(b"v1".to_vec())),
        resp(&engine, &["GETSET", "replaced:getset", "v2"]),
        "GETSET answers the value it replaced",
    );
    assert_eq!(
        Some(b"v2".to_vec()),
        get(&engine, "replaced:getset"),
        "GETSET really stored the new value",
    );
    assert_eq!(
        -1,
        pttl(&engine, "replaced:getset"),
        "GETSET replaced the whole value, so the deadline that was on the old one is gone. \
         A positive number here is the key still counting down toward a disappearance the \
         caller cancelled when they overwrote it.",
    );

    // ---- HALF TWO: MSET ---------------------------------------------------------------
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "replaced:mset", "v1", "PX", HOUR_MS]),
    );
    let armed = pttl(&engine, "replaced:mset");
    assert!(
        armed > 0,
        "DENOMINATOR: the MSET half needs its own armed deadline; PTTL reported {armed}",
    );
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["MSET", "replaced:mset", "v2"]),
    );
    assert_eq!(
        Some(b"v2".to_vec()),
        get(&engine, "replaced:mset"),
        "MSET really stored the new value",
    );
    assert_eq!(
        -1,
        pttl(&engine, "replaced:mset"),
        "MSET replaced the whole value, so the deadline is gone -- same rule as SET, which is \
         what MSET is a batch of.",
    );

    // ---- CONTROLS, THE OTHER DIRECTION: an amending write KEEPS the deadline ------------
    // These are what stops the fix from being "make every string write clear the deadline".
    for (verb, args) in [
        ("APPEND", vec!["APPEND", "kept:append", "-tail"]),
        ("SETRANGE", vec!["SETRANGE", "kept:setrange", "0", "X"]),
        ("INCR", vec!["INCR", "kept:incr"]),
        ("INCRBY", vec!["INCRBY", "kept:incrby", "5"]),
        ("DECR", vec!["DECR", "kept:decr"]),
        ("INCRBYFLOAT", vec!["INCRBYFLOAT", "kept:incrbyfloat", "1.5"]),
    ] {
        let key = args[1];
        // Every one of these is seeded with "1" so the numeric verbs have something to add to
        // and the byte verbs have something to amend.
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(&engine, &["SET", key, "1", "PX", HOUR_MS]),
        );
        let armed = pttl(&engine, key);
        assert!(
            armed > 0,
            "DENOMINATOR for the {verb} control: PTTL reported {armed} before {verb} ran",
        );
        let answer = resp(&engine, &args);
        assert!(
            !matches!(answer, RespValue::Error(_)),
            "the {verb} control has to actually run: it answered {answer:?}",
        );
        let after = pttl(&engine, key);
        assert!(
            after > 0,
            "{verb} amends a value that is already there and must leave its lifetime alone; \
             PTTL reported {after}. A -1 here means the deadline-clearing write leaked into \
             the amending verbs, which resets a countdown every time a counter moves.",
        );
    }
}

/// Every command the dispatcher accepts is either advertised by `COMMAND`, or named here as
/// deliberately unadvertised -- and everything `COMMAND` advertises really is accepted.
///
/// WHY A GUARD AND NOT JUST THE FOUR MISSING ROWS. `redis_supported_commands()` is a
/// hand-written array in `command_table.rs`; the dispatcher is a `match` in `dispatch.rs`.
/// Nothing connects them. Adding a command arm is one edit in one file, and the table is a
/// second file the author has no reason to open, so the table drifts in exactly one direction:
/// it under-reports, quietly. That is how `INCR`, `DECR`, `INCRBY` and `DECRBY` came to be
/// implemented and unadvertised while `INCRBYFLOAT`, `HINCRBY` and `HINCRBYFLOAT` -- the rest
/// of the same family -- were listed. The omission is not a policy; it is four forgotten rows.
///
/// WHAT THE MISREPORT COSTS. `COMMAND INFO INCR` answers a null, which is the wire spelling of
/// "this server does not have that command", while `INCR k` works. `COMMAND COUNT` undercounts
/// by the same four. A client that introspects before it dispatches -- which is how cluster-
/// aware clients learn where the key sits in each command's argument list -- is being told the
/// wrong thing by the server itself, and the answer it gets is confidently wrong rather than
/// an error it could retry.
///
/// THE OTHER DIRECTION MATTERS TOO, AND IS CURRENTLY CLEAN. A table row with no arm behind it
/// advertises a command the dispatcher will reject. There are none today; this asserts it
/// stays that way, because removing an arm is exactly as one-sided an edit as adding one.
///
/// THE EXEMPTIONS ARE HAND-WRITTEN AND EXACT, NOT PREFIXES. A prefix rule would read
/// "everything starting with F is ours" and would silently swallow a future standard verb that
/// happens to start with F. Spelling each name means a new arm is unlisted AND unexempt, so it
/// fails here until someone decides which it is.
///
/// THIS SCAN'S OWN CONTROLS. The first version of it matched any capitalised string literal at
/// any indentation and reported `WITHSCORES`, `REV`, `FIRST` and `LAST` as commands -- they are
/// option words inside the sorted-set arms, not arms. The negative controls below are those
/// words, and they are what caught it.
#[test]
fn every_dispatched_command_is_advertised_or_named_as_unadvertised() {
    const DISPATCH: &str = include_str!("../../redis/dispatch.rs");
    const TABLE: &str = include_str!("../../redis/command_table.rs");

    /// The control-state families. Three storage shapes (counter, distinct-count, selection)
    /// reachable under both their historical verbs and their descriptive ones; they are a
    /// TemporalStore surface with no Redis counterpart, so `COMMAND` -- whose whole audience
    /// is clients asking what Redis commands this server speaks -- does not claim them.
    const CONTROL_STATE_VERBS: [&str; 33] = [
        "CONTROLSTATECHANGE", "CONTROLSTATECOUNT", "CONTROLSTATEDEBUG", "CONTROLSTATEDETAIL",
        "CONTROLSTATEHSET", "CONTROLSTATEINCR", "CONTROLSTATEINCROPT", "CONTROLSTATEMANAGER",
        "CONTROLSTATEQUERY",
        "COUNTERQUERY", "COUNTERSET", "COUNTERSETANDGET", "COUNTERSETANDGETOPT",
        "CPCQUERY", "CPCSET", "CPCSETANDGET", "CPCSETANDGETOPT",
        "DISTINCTQUERY", "DISTINCTSET", "DISTINCTSETANDGET", "DISTINCTSETANDGETOPT",
        "FOLQUERY", "FOLSET", "FOLSETANDGET", "FOLSETANDGETOPT",
        "HCHANGE", "HQUERY", "HSETANDGET", "HSETANDGETOPT",
        "SELECTIONQUERY", "SELECTIONSET", "SELECTIONSETANDGET", "SELECTIONSETANDGETOPT",
    ];

    /// The feature/sequence verbs: timestamped point series with filtering and aggregation.
    /// Same reason -- no Redis command means any of this, so advertising them would not help
    /// the clients `COMMAND` exists for.
    const FEATURE_VERBS: [&str; 8] = [
        "FAGG", "FAPPEND", "FAPPENDPOLICY", "FDEL",
        "FQUERY", "FQUERYFILTER", "FQUERYFILTERSTR", "FREPLACE",
    ];

    /// Routing introspection: which bucket a key lands in, and its stable hash. Diagnostics
    /// for operators reading a cluster's placement, not part of the data surface.
    const ROUTING_VERBS: [&str; 3] = ["PCLUSTERHASH", "PCLUSTERKEYSLOT", "PSLOTHASHKEY"];

    let exempt: Vec<&str> = CONTROL_STATE_VERBS
        .iter()
        .chain(FEATURE_VERBS.iter())
        .chain(ROUTING_VERBS.iter())
        .copied()
        .collect();

    // ---- the arms, and their DENOMINATOR ----------------------------------------------
    // A top-level arm of the dispatcher's `match command.as_str()` sits at exactly eight
    // spaces and opens with a string literal. Anything deeper is inside some arm's own
    // `match` over its option words -- see the negative controls.
    fn names_on(line: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut rest = line;
        while let Some(open) = rest.find('"') {
            let after = &rest[open + 1..];
            let Some(close) = after.find('"') else { break };
            let token = &after[..close];
            if !token.is_empty()
                && token.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
                && token.starts_with(|c: char| c.is_ascii_uppercase())
            {
                names.push(token.to_string());
            }
            rest = &after[close + 1..];
        }
        names
    }

    let mut arms: Vec<String> = Vec::new();
    for line in DISPATCH.lines() {
        let Some(rest) = line.strip_prefix("        \"") else {
            continue;
        };
        // Re-prepend the quote the prefix test consumed so `names_on` sees a balanced line.
        for name in names_on(&format!("\"{rest}")) {
            if !arms.contains(&name) {
                arms.push(name);
            }
        }
    }

    let mut advertised: Vec<String> = Vec::new();
    for line in TABLE.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("name: \"") else {
            continue;
        };
        let Some(close) = rest.find('"') else { continue };
        advertised.push(rest[..close].to_string());
    }

    assert!(
        arms.len() > 80,
        "VACUITY: the arm scan found only {} dispatch arms. Either the file moved or the arm \
         shape changed, and this guard is reading nothing.",
        arms.len(),
    );
    assert!(
        advertised.len() > 80,
        "VACUITY: the table scan found only {} descriptors. The `name: \"...\"` shape must have \
         changed, and this guard is reading nothing.",
        advertised.len(),
    );

    // ---- POSITIVE CONTROL: both scans really find a command everyone agrees exists -------
    assert!(
        arms.iter().any(|name| name == "GET"),
        "POSITIVE CONTROL: the arm scan cannot even find GET, so it is not reading arms",
    );
    assert!(
        advertised.iter().any(|name| name == "GET"),
        "POSITIVE CONTROL: the table scan cannot even find GET, so it is not reading the table",
    );

    // ---- NEGATIVE CONTROLS: option words are not commands -------------------------------
    // These four live inside the sorted-set arms as `match upper(..)` cases. A scan that
    // counted them would report a pile of phantom "unadvertised commands" and the real four
    // would be lost in the noise -- which is what the first version of this scan did.
    for option_word in ["WITHSCORES", "REV", "FIRST", "LAST"] {
        assert!(
            !arms.iter().any(|name| name == option_word),
            "NEGATIVE CONTROL: the arm scan counted the option word {option_word} as a command, \
             so it is matching literals nested inside arms rather than the arms themselves",
        );
    }

    // ---- CLAIM ONE: nothing is dispatched without being advertised or named --------------
    let unadvertised: Vec<&String> = arms
        .iter()
        .filter(|name| !advertised.contains(name) && !exempt.contains(&name.as_str()))
        .collect();
    assert!(
        unadvertised.is_empty(),
        "{} of {} dispatched command(s) are neither in the COMMAND table nor named above as \
         deliberately unadvertised: {:?}. Add a descriptor to `command_table.rs` if it is a \
         Redis command clients should be able to discover, or add it to one of the exemption \
         lists in this test with the reason it is not.",
        unadvertised.len(),
        arms.len(),
        unadvertised,
    );

    // ---- CLAIM TWO: nothing is advertised that the dispatcher would reject ---------------
    let phantom: Vec<&String> = advertised
        .iter()
        .filter(|name| !arms.contains(name))
        .collect();
    assert!(
        phantom.is_empty(),
        "{} of {} advertised command(s) have no dispatch arm: {:?}. COMMAND is telling clients \
         this server speaks something it answers with an error.",
        phantom.len(),
        advertised.len(),
        phantom,
    );

    // ---- The exemption list must not rot ------------------------------------------------
    // An exemption for a command that no longer exists is a name nobody will ever remove, and
    // it silently widens what the next author can forget to advertise.
    let stale: Vec<&&str> = exempt
        .iter()
        .filter(|name| !arms.contains(&name.to_string()))
        .collect();
    assert!(
        stale.is_empty(),
        "{} exemption(s) name a command the dispatcher no longer has: {:?}. Remove them.",
        stale.len(),
        stale,
    );
}
