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

use crate::redis::{execute_redis_command, unix_time_ms, RespValue};

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

/// What the SHARD itself holds for a key: (the record is still there, a deadline is still
/// recorded against it). Read directly, not through a command.
///
/// WHY THIS EXISTS AND `GET` IS NOT ENOUGH. `GET` cannot tell "the key was deleted" apart from
/// "the key was given a deadline one millisecond out, and that millisecond has since passed":
/// lazy expiry removes it either way before the next command runs, so the assertion is
/// satisfied by the CLOCK rather than by the behaviour under test. That is not hypothetical --
/// both mutations restoring the old `.max(1)` clamp left every `GET` answering nil and this
/// whole test PASSING, which is how they were found. The shard has no such ambiguity: a
/// clamped deadline is an entry in `expires_at_ms` with the record still sitting in `strings`,
/// and a deletion is neither of those, whatever the clock has done in between.
fn shard_state(engine: &TemporalEngine, key: &str) -> (bool, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    (
        shard.strings.contains_key(key),
        shard.expires_at_ms.contains_key(key),
    )
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

    // ---- THE OTHER DIRECTION IS DELIBERATELY NOT CLAIMED HERE ---------------------------
    // "nothing is advertised that the dispatcher would reject" is already asserted, and
    // asserted more strongly, by `advertised_redis_commands_have_dispatch_paths` in
    // `redis.rs`: it RUNS a sample invocation of every descriptor against a live engine and
    // fails if the answer is a syntax error. A source scan for the same thing would be a
    // second, weaker reader of one rule -- two places to update, and the weaker one able to
    // pass while the real behaviour is broken. Adding a descriptor here without a sample
    // there fails that test, which is how the four rows in this change were caught.
    //
    // Only the direction it does NOT cover is claimed above: it iterates the TABLE, so a
    // command the dispatcher has and the table does not is invisible to it.

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

/// A deadline that has already passed removes the key NOW, on all six spellings.
///
/// SIX SPELLINGS, ONE MEANING. `EXPIRE` / `PEXPIRE` take a relative time, so "already past" is
/// spelled as a negative (or zero) number. `EXPIREAT` / `PEXPIREAT` and `GETEX EXAT` / `PXAT`
/// take an absolute moment, so it is spelled as a timestamp behind the clock. There is no such
/// thing as a key that expires in the past: every one of them means "this key is finished",
/// and the integer reply is about whether there WAS a key, the same as when the deadline is
/// ahead.
///
/// WHAT THE TWO KINDS ANSWERED BEFORE, AND THE TWO DIFFERENT WAYS THEY WERE WRONG.
///
///   * The ABSOLUTE four clamped the computed remaining time with `.max(1)` and armed a
///     deadline one millisecond out. That is not a rounding difference, it is a live key: the
///     caller's next command could arrive inside that millisecond and be handed the value they
///     had just asked to be rid of. Whether it did depended on scheduling, which is the worst
///     property a correctness answer can have -- it passes in a test and fails under load.
///   * The RELATIVE two never got that far. A negative number failed `parse_u64`, so
///     `EXPIRE key -1` came back as a syntax error. The caller was told their command was
///     malformed, and the key stayed exactly where it was.
///
/// HALVES ASSERTED SEPARATELY, AND HERE THAT IS SIX. Two different root causes across three
/// helper functions -- a parse that refused the input, and a clamp that rewrote it -- so a
/// single "no key survives a past deadline" count would read full from the four that share the
/// clamp while both of the others stayed broken. Each spelling gets its own claim, behind its
/// own denominator that the key really was there first.
///
/// CONTROLS IN THE OTHER DIRECTION. A deadline in the FUTURE must still arm rather than delete,
/// and a past deadline on a key that is not there must answer 0 rather than erroring. Without
/// those, "delete on anything that is not clearly in the future" would satisfy all six claims
/// above while turning every ordinary `EXPIRE key 60` into a deletion.
#[test]
fn a_deadline_already_in_the_past_removes_the_key_now() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    fn seed(engine: &TemporalEngine, key: &str) {
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(engine, &["SET", key, "v"]),
            "DENOMINATOR: the key has to be there before a past deadline can remove it",
        );
        assert_eq!(
            Some(b"v".to_vec()),
            get(engine, key),
            "DENOMINATOR: and it has to read back",
        );
    }

    // Timestamps comfortably in the past, in the two units. 1_000_000_000 seconds is 2001.
    const PAST_SECONDS: &str = "1000000000";
    const PAST_MILLIS: &str = "1000000000000";

    // ---- THE SIX HALVES ---------------------------------------------------------------
    // Each: seed, apply a past deadline, and the key must be gone in the very next command --
    // no sleep anywhere, because "gone in a millisecond" is exactly the answer being rejected.
    for (label, key, command) in [
        ("EXPIRE with a negative time", "past:expire",
         vec!["EXPIRE", "past:expire", "-1"]),
        ("PEXPIRE with a negative time", "past:pexpire",
         vec!["PEXPIRE", "past:pexpire", "-1"]),
        ("EXPIREAT with a past timestamp", "past:expireat",
         vec!["EXPIREAT", "past:expireat", PAST_SECONDS]),
        ("PEXPIREAT with a past timestamp", "past:pexpireat",
         vec!["PEXPIREAT", "past:pexpireat", PAST_MILLIS]),
    ] {
        seed(&engine, key);
        assert_eq!(
            RespValue::Integer(1),
            resp(&engine, &command),
            "{label}: the key was there, so the reply is 1. An error here means the command was \
             refused outright; a 0 means it was not seen.",
        );
        // THE SHARD IS ASKED FIRST, AND THE ORDER IS LOAD-BEARING. `get` and `pttl` both call
        // `remove_if_expired`, so either of them would DO the deletion this is trying to
        // observe -- the clamped record would be gone by the time the shard was asked, and
        // the claim would hold for the wrong reason. That is not hypothetical either: with
        // the shard check written after the reads, both clamp mutations still passed.
        let (record, deadline) = shard_state(&engine, key);
        assert!(
            !record,
            "{label}: the RECORD must be gone from the shard, not merely unreadable. A record \
             still sitting there under a deadline a millisecond out answers nil to the next \
             GET too, so GET alone cannot tell the two apart -- this asks the shard, and asks \
             it before anything has had a chance to expire the key lazily.",
        );
        assert!(
            !deadline,
            "{label}: and no deadline may be left recorded against it",
        );
        assert_eq!(
            None,
            get(&engine, key),
            "{label}: the key must be gone in the NEXT command, not a millisecond from now. A \
             value here is the key the caller just discarded being handed back to them.",
        );
        assert_eq!(
            -2,
            pttl(&engine, key),
            "{label}: PTTL on a key that is gone is -2. A positive number is a key still \
             counting down; a -1 is a key that will now never expire at all.",
        );
    }

    // GETEX carries the value back as well, so its two spellings are asserted on both halves
    // of their answer: the value the caller reads, and the key that must not survive it.
    for (label, key, command) in [
        ("GETEX EXAT with a past timestamp", "past:getexat",
         vec!["GETEX", "past:getexat", "EXAT", PAST_SECONDS]),
        ("GETEX PXAT with a past timestamp", "past:getpxat",
         vec!["GETEX", "past:getpxat", "PXAT", PAST_MILLIS]),
    ] {
        seed(&engine, key);
        assert_eq!(
            RespValue::Bulk(Some(b"v".to_vec())),
            resp(&engine, &command),
            "{label}: GETEX still answers the value it read",
        );
        // Same ordering rule as above: the shard, then the reads.
        let (record, deadline) = shard_state(&engine, key);
        assert!(
            !record,
            "{label}: the record is gone from the shard, not armed a millisecond out",
        );
        assert!(!deadline, "{label}: and no deadline is left recorded");
        assert_eq!(
            None,
            get(&engine, key),
            "{label}: and the key is gone immediately afterwards",
        );
    }

    // ---- CONTROL: a deadline in the FUTURE arms, and does not delete --------------------
    let future_seconds = (unix_time_ms() / 1000 + 3_600).to_string();
    let future_millis = (unix_time_ms() + 3_600_000).to_string();
    for (label, key, command) in [
        ("EXPIRE", "future:expire", vec!["EXPIRE", "future:expire", "3600"]),
        ("PEXPIRE", "future:pexpire", vec!["PEXPIRE", "future:pexpire", "3600000"]),
        ("EXPIREAT", "future:expireat",
         vec!["EXPIREAT", "future:expireat", future_seconds.as_str()]),
        ("PEXPIREAT", "future:pexpireat",
         vec!["PEXPIREAT", "future:pexpireat", future_millis.as_str()]),
        ("GETEX EXAT", "future:getexat",
         vec!["GETEX", "future:getexat", "EXAT", future_seconds.as_str()]),
    ] {
        seed(&engine, key);
        let answer = resp(&engine, &command);
        assert!(
            !matches!(answer, RespValue::Error(_)),
            "CONTROL {label}: a future deadline must be accepted, and it answered {answer:?}",
        );
        assert_eq!(
            Some(b"v".to_vec()),
            get(&engine, key),
            "CONTROL {label}: a deadline an hour out must NOT delete the key. This is what \
             stops the fix from being 'delete unless the time is obviously ahead'.",
        );
        let remaining = pttl(&engine, key);
        assert!(
            remaining > 0,
            "CONTROL {label}: the deadline must really be armed; PTTL reported {remaining}",
        );
        let (record, deadline) = shard_state(&engine, key);
        assert!(
            record,
            "CONTROL {label}: the record must still be in the shard",
        );
        assert!(
            deadline,
            "CONTROL {label}: and a deadline must really be RECORDED in `expires_at_ms`. This \
             is the denominator for every `!deadline` claim above: without it they would all \
             also hold if the index never recorded anything at all.",
        );
    }

    // ---- CONTROL: a past deadline on a key that is not there answers 0, not an error -----
    // Asking to discard something already gone is a no-op that succeeded, which is what every
    // other deletion on this surface answers.
    assert_eq!(
        RespValue::Integer(0),
        resp(&engine, &["EXPIRE", "past:absent", "-1"]),
        "CONTROL: a past deadline on a missing key is 0",
    );
    assert_eq!(
        RespValue::Integer(0),
        resp(&engine, &["EXPIREAT", "past:absent", PAST_SECONDS]),
        "CONTROL: same for the absolute spelling",
    );

    // ---- CONTROL: a genuinely malformed time is still an error --------------------------
    // Accepting negatives must not turn into accepting anything at all.
    assert!(
        matches!(resp(&engine, &["EXPIRE", "past:absent", "soon"]), RespValue::Error(_)),
        "CONTROL: a non-numeric expiry is still refused",
    );
}

/// Read the deadline the SHARD actually stored for a key, in absolute milliseconds.
///
/// Every command-level way of asking this question adds a clock read of its own --
/// `PTTL` subtracts the shard's clock, `PEXPIRETIME` then adds the RESP layer's back -- so
/// none of them can be used to measure how faithfully a deadline was stored. This reads the
/// map.
fn stored_deadline_ms(engine: &TemporalEngine, key: &str) -> Option<u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard.expires_at_ms.get(key).copied()
}

/// `SET key value KEEPTTL` replaces the value and leaves the deadline running.
///
/// WHAT WAS MISSING. A value-replacing write can do three things to the deadline it
/// overwrites -- arm a new one, discard the old one, or leave it alone -- and this surface
/// could only spell the first two. `KEEPTTL` did not exist at all (zero occurrences in the
/// tree), so `SET k v KEEPTTL` was refused as a syntax error and a caller who wanted the
/// value changed and the countdown kept had to read the remaining time and write it back --
/// which races the countdown it is trying to preserve and silently loses however long the
/// round trip took.
///
/// HALVES ASSERTED SEPARATELY, AND THE SECOND HALF IS THE ONE THAT CATCHES THE LAZY FIX.
/// "KEEPTTL keeps the deadline" and "SET without KEEPTTL still clears it" are two claims.
/// The wrong fix -- making the no-TTL branch stop clearing -- satisfies the first completely
/// while turning plain `SET` into a deadline-preserving write, which is the exact bug #1713
/// closed for `GETSET` and `MSET`. So the clearing half is asserted on its own key, after
/// the keeping half, and neither is folded into a combined count.
///
/// THE DEADLINE IS COMPARED BY ITS ABSOLUTE VALUE, NOT BY "IS IT STILL POSITIVE".
/// A `PTTL > 0` after `KEEPTTL` would also be produced by the write ARMING a fresh deadline
/// of its own from some default, which is a different behaviour that happens to look alive.
/// The shard's stored millisecond is read before and after and must be the SAME number.
#[test]
fn keepttl_replaces_the_value_and_leaves_the_deadline_running() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- DENOMINATOR: the option is accepted at all -------------------------------------
    // Before this change every assertion below would also be satisfied by `SET ... KEEPTTL`
    // failing as a syntax error and leaving the key untouched -- the value would be the old
    // one and the deadline would indeed be unchanged, for entirely the wrong reason. So the
    // reply is pinned first, and the new VALUE is pinned separately from the deadline.
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "keep:ttl", "v1", "PX", HOUR_MS]),
    );
    let armed = stored_deadline_ms(&engine, "keep:ttl");
    assert!(
        armed.is_some(),
        "DENOMINATOR: `SET k v PX {HOUR_MS}` must arm a deadline; the shard stored {armed:?}",
    );

    // ---- HALF ONE: KEEPTTL keeps the EXACT deadline, and really writes the value ---------
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "keep:ttl", "v2", "KEEPTTL"]),
        "`SET k v KEEPTTL` is accepted. A syntax error here is the state before this change, \
         and it would leave every other assertion in this half trivially satisfied.",
    );
    assert_eq!(
        Some(b"v2".to_vec()),
        get(&engine, "keep:ttl"),
        "KEEPTTL still REPLACES the value -- it is an option on SET, not a way to skip it",
    );
    assert_eq!(
        armed,
        stored_deadline_ms(&engine, "keep:ttl"),
        "KEEPTTL must leave the deadline exactly where it was. A different number here is a \
         fresh deadline armed by the write, which reads as alive but is not the countdown \
         the caller asked to keep; None is the clearing branch still running.",
    );

    // ---- HALF TWO, THE CONTROL: SET without KEEPTTL still clears -------------------------
    // This is what stops the fix from being "stop clearing", which would satisfy half one
    // and silently make every plain SET preserve a deadline it was asked to discard.
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "cleared:plain", "v1", "PX", HOUR_MS]),
    );
    assert!(
        stored_deadline_ms(&engine, "cleared:plain").is_some(),
        "DENOMINATOR for the clearing control: a deadline has to be there to be cleared",
    );
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "cleared:plain", "v2"]),
    );
    assert_eq!(
        None,
        stored_deadline_ms(&engine, "cleared:plain"),
        "SET WITHOUT KEEPTTL STILL DISCARDS THE DEADLINE. If this ever reads Some, the \
         KEEPTTL branch has leaked into the default one and every plain SET now preserves a \
         countdown the caller replaced away -- the bug #1713 closed for GETSET and MSET.",
    );
    assert_eq!(
        -1,
        pttl(&engine, "cleared:plain"),
        "and the same answer through the command surface, not just the map",
    );

    // ---- HALF THREE: KEEPTTL on a key with NO deadline leaves it with none ---------------
    // "Keep" has to mean keep, including keeping the absence. Arming anything here would be
    // inventing a deadline the caller never named.
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "keep:none", "v1"]),
    );
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "keep:none", "v2", "KEEPTTL"]),
    );
    assert_eq!(
        None,
        stored_deadline_ms(&engine, "keep:none"),
        "KEEPTTL on a key that had no deadline must not invent one",
    );
    assert_eq!(
        Some(b"v2".to_vec()),
        get(&engine, "keep:none"),
        "and the value is still replaced",
    );

    // ---- HALF FOUR: KEEPTTL composes with NX / XX / GET ----------------------------------
    // KEEPTTL is a deadline option, so it must not disturb the condition or the old-value
    // return. `XX` also proves the option is parsed in either order.
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "keep:xx", "v1", "PX", HOUR_MS]),
    );
    let armed_xx = stored_deadline_ms(&engine, "keep:xx");
    assert!(armed_xx.is_some(), "DENOMINATOR for the XX half");
    assert_eq!(
        RespValue::Bulk(Some(b"v1".to_vec())),
        resp(&engine, &["SET", "keep:xx", "v2", "KEEPTTL", "XX", "GET"]),
        "KEEPTTL beside XX and GET still answers the old value",
    );
    assert_eq!(
        armed_xx,
        stored_deadline_ms(&engine, "keep:xx"),
        "and still keeps the deadline when combined with XX and GET",
    );

    // ---- CONTROLS: the contradictions are refused, and only those ------------------------
    // KEEPTTL and an arming TTL ask for opposite things. Both orders are refused, so the
    // rejection is a rule rather than an artifact of which word the parser met first.
    for args in [
        vec!["SET", "bad:1", "v", "KEEPTTL", "EX", "10"],
        vec!["SET", "bad:2", "v", "EX", "10", "KEEPTTL"],
        vec!["SET", "bad:3", "v", "KEEPTTL", "PX", "10000"],
        vec!["SET", "bad:4", "v", "PX", "10000", "KEEPTTL"],
        vec!["SET", "bad:5", "v", "KEEPTTL", "KEEPTTL"],
    ] {
        let spelling = args.join(" ");
        assert!(
            matches!(resp(&engine, &args), RespValue::Error(_)),
            "CONTROL: `{spelling}` asks for two contradictory things about one deadline and \
             must be refused, not silently resolved by a precedence rule nobody wrote down",
        );
        assert_eq!(
            None,
            get(&engine, args[1]),
            "CONTROL: a refused `{spelling}` must not have written the key either",
        );
    }
    // ...and the option word is not simply being swallowed: an unknown one still fails.
    assert!(
        matches!(
            resp(&engine, &["SET", "bad:6", "v", "KEEPTTLX"]),
            RespValue::Error(_)
        ),
        "CONTROL: the parser matches KEEPTTL exactly, not as a prefix",
    );
}

/// `EXPIREAT` does not store the deadline it was given, and the direction is never early.
///
/// THE MECHANISM. An absolute deadline is converted to a RELATIVE one at the RESP layer
/// (`deadline - unix_time_ms()`) and then converted back to absolute at the shard
/// (`resolve_now_ms() + ttl`). Two readings of the clock, taken at different moments, so what
/// is stored is `named + (t_shard - t_resp)` -- the caller's deadline plus however long the
/// command took to travel. `PEXPIRETIME` then adds a THIRD reading on the way back out, so
/// even reading the value back cannot see the stored number.
///
/// WHAT IS PINNED HERE, AND WHAT IS NOT. The drift's SIZE is a timing measurement and would
/// be a flaky assertion -- on an idle box both clock reads land in the same millisecond and
/// it is zero, under load it is not. The drift's DIRECTION is not a timing measurement: the
/// shard's clock is read strictly after the RESP layer's, so the stored deadline is never
/// BEFORE the one the caller named. That is the safety-relevant half -- this lossiness can
/// only ever let a key live slightly too long, never kill it early -- and it is what is
/// asserted. The magnitude is reported by the ignored measurement test below it.
///
/// Closing the lossiness itself means an absolute-deadline path that does not round-trip
/// through relative, which is a new engine command and a wire change, not a RESP patch.
#[test]
fn an_absolute_deadline_is_stored_no_earlier_than_it_was_named() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // A deadline far enough out that nothing here can pass by the key expiring.
    let named_ms = unix_time_ms() + 3_600_000;

    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "drift:key", "v"]),
    );
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["PEXPIREAT", "drift:key", &named_ms.to_string()]),
        "DENOMINATOR: PEXPIREAT has to report that it armed something",
    );

    let stored = stored_deadline_ms(&engine, "drift:key")
        .expect("DENOMINATOR: PEXPIREAT must leave a deadline in the shard");

    assert!(
        stored >= named_ms,
        "the round trip through a relative TTL reads the RESP clock first and the shard clock \
         second, so the stored deadline can only be at or after the named one. A stored \
         deadline BEFORE the named one ({stored} < {named_ms}) would mean a key dying earlier \
         than the caller asked, which is the direction that loses data.",
    );

    // The drift is real but bounded by how long one command takes; a whole second would mean
    // something other than the two clock reads is moving the deadline.
    let drift = stored - named_ms;
    assert!(
        drift < 1_000,
        "stored deadline drifted {drift} ms past the named one. Two clock reads around one \
         command cannot account for a whole second -- that is a different bug.",
    );
}

/// The measured size of the `EXPIREAT` drift. Reported, not gated.
///
/// Ignored on purpose: the number is a property of how loaded the box is, so asserting a
/// threshold would be asserting the machine. Run it to get the figure:
/// `cargo test -p temporalstore-rust --lib -- --ignored expireat_drift --nocapture`.
#[test]
#[ignore]
fn expireat_drift_measured() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    resp(&engine, &["SET", "drift:measure", "v"]);

    const ROUNDS: usize = 500;
    let mut drifts: Vec<u64> = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let named_ms = unix_time_ms() + 3_600_000;
        resp(
            &engine,
            &["PEXPIREAT", "drift:measure", &named_ms.to_string()],
        );
        let stored = stored_deadline_ms(&engine, "drift:measure").expect("a deadline is armed");
        drifts.push(stored.saturating_sub(named_ms));
    }
    drifts.sort_unstable();
    let nonzero = drifts.iter().filter(|drift| **drift > 0).count();
    let total: u64 = drifts.iter().sum();
    println!(
        "EXPIREAT drift over {ROUNDS} rounds: min {} ms, median {} ms, p99 {} ms, max {} ms, \
         mean {:.3} ms; {nonzero}/{ROUNDS} rounds stored a deadline LATER than the one named",
        drifts[0],
        drifts[ROUNDS / 2],
        drifts[(ROUNDS * 99) / 100],
        drifts[ROUNDS - 1],
        total as f64 / ROUNDS as f64,
    );
}

/// A deadline EQUAL to the instant has already passed.
///
/// WHY THIS IS ITS OWN TEST. Every arm of `execute_on_shard` reaches lazy expiry through
/// `remove_if_expired`, and the whole question it answers is one comparison:
/// `*expires_at <= now`. The boundary is the millisecond where `<=` and `<` disagree, and it
/// is the only millisecond where they disagree at all. A test that arms a deadline and then
/// sleeps past it is satisfied by either spelling, so the entire suite ran green with `<`:
/// changing that one character failed nothing in 201 selected tests, including every test
/// named for expiry.
///
/// WHY THE CLOCK IS FROZEN. The instant cannot be hit by timing. `resolve_now_ms` answers the
/// replay clock when one is installed, so `ReplayClockGuard` pins it and the comparison is
/// then exact rather than a race. That is also the honest shape: the boundary is a property of
/// the comparison, not of how fast the test runs.
///
/// THE THREE CASES ARE ASSERTED SEPARATELY. One combined "expired" count reads full from the
/// past case alone and says nothing about the other two, which is how the boundary survived.
#[test]
fn a_deadline_equal_to_the_instant_has_already_passed() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- DENOMINATOR: the keys are really there before anything can collect them --------
    for key in ["boundary:past", "boundary:equal", "boundary:future"] {
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(&engine, &["SET", key, "v"]),
            "DENOMINATOR: {key} must exist before a deadline can be tested against it",
        );
    }

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");

    // One instant, used for the deadlines AND for the collector, so the comparison is exact.
    let instant = crate::engine::resolve_now_ms();
    crate::engine::set_expiry(shard, "boundary:past".to_string(), instant - 1);
    crate::engine::set_expiry(shard, "boundary:equal".to_string(), instant);
    crate::engine::set_expiry(shard, "boundary:future".to_string(), instant + 1);

    // Freeze the clock the collector reads. Held across all three calls below.
    let _clock = crate::engine::ReplayClockGuard::enter(Some(instant));

    // ---- CONTROL: a deadline one millisecond BEHIND the instant is collected -----------
    // Without this, a collector that had stopped collecting anything at all would satisfy the
    // "future" claim below and look like a result.
    assert!(
        crate::engine::remove_if_expired(shard, "boundary:past"),
        "CONTROL: a deadline one millisecond before the instant must be collected. It was not, \
         so lazy expiry is not running here and nothing else below means anything.",
    );

    // ---- THE CLAIM: the instant itself counts as passed -------------------------------
    assert!(
        crate::engine::remove_if_expired(shard, "boundary:equal"),
        "a deadline EQUAL to the instant has passed and the key must be collected. It was not, \
         so the comparison is `<` where it has to be `<=`, and a key whose deadline is exactly \
         now stays readable for the millisecond it was supposed to stop being readable.",
    );

    // ---- CONTROL: a deadline one millisecond AHEAD is NOT collected -------------------
    // Without this, a collector that removed unconditionally would satisfy both claims above.
    assert!(
        !crate::engine::remove_if_expired(shard, "boundary:future"),
        "CONTROL: a deadline one millisecond after the instant has NOT passed and the key must \
         stay. It did not, so the comparison collects keys that are still live.",
    );

    // ---- and the shard agrees with the answers -----------------------------------------
    assert!(
        !shard.strings.contains_key("boundary:past"),
        "the collected key must leave `strings`, not just answer that it did",
    );
    assert!(
        !shard.strings.contains_key("boundary:equal"),
        "the key whose deadline equals the instant must leave `strings` too",
    );
    assert!(
        shard.strings.contains_key("boundary:future"),
        "the key that has not reached its deadline must still be in `strings`",
    );
}

/// The read-only fast path hides a key whose deadline has passed.
///
/// WHERE THIS PATH IS. `execute` does not take it: `execute_read_only_fast_path` runs only when
/// `execute_with_storage_override` was given a storage override, which is what
/// `execute_durable`, `execute_replicated` and the raft-apply routes do -- and
/// `RecordStore::Local` serves reads through `execute_durable`. So `GET` and `HGETALL` as a
/// deployment answers them go through a deadline test that no test through `engine.execute`
/// can reach, and every command test in this file uses `engine.execute`.
///
/// WHAT THAT COST. The fast path has its own expiry test, separate from the one in
/// `execute_on_shard`, for each of its two commands. Inverting either of them -- so that a
/// LIVE key takes the slow path and an EXPIRED key is served from the fast one -- failed
/// nothing in 201 selected tests. The fast path would hand back the value of a key whose
/// deadline had passed, which is the one thing lazy expiry exists to prevent.
///
/// HALVES ASSERTED SEPARATELY. `StringGet` and `HashGetAll` are two tests in the same shape
/// and were both unguarded; one combined claim reads full from whichever is fixed first.
#[test]
fn the_read_only_fast_path_hides_a_key_whose_deadline_has_passed() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    /// Arm a deadline well in the past WITHOUT letting any command collect the key first --
    /// going through `EXPIRE` would run the slow path and answer the question there instead.
    fn backdate(engine: &TemporalEngine, key: &str) {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, key.to_string(), past);
    }

    fn durable(engine: &TemporalEngine, command: Command) -> CommandResponse {
        let response = engine.execute_durable(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(
            response.status.ok,
            "the durable route must answer, and it failed: {}",
            response.status.message,
        );
        response.response
    }

    // ---- DENOMINATOR: the fast path answers a LIVE key with its value ------------------
    // Without this, every "absent" assertion below would also be produced by the route being
    // broken, or by the key never having been written.
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "fast:string", "v"]),
    );
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HSET", "fast:hash", "f", "v"]));
    assert_eq!(
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        },
        durable(
            &engine,
            Command::StringGet {
                key: "fast:string".to_string()
            }
        ),
        "DENOMINATOR: the durable read route must serve a live string",
    );
    assert!(
        matches!(
            durable(
                &engine,
                Command::HashGetAll {
                    key: "fast:hash".to_string()
                }
            ),
            CommandResponse::HashEntries { ref entries } if !entries.is_empty()
        ),
        "DENOMINATOR: the durable read route must serve a live hash",
    );

    // ---- HALF ONE: StringGet ----------------------------------------------------------
    backdate(&engine, "fast:string");
    assert_eq!(
        CommandResponse::Bytes { value: None },
        durable(
            &engine,
            Command::StringGet {
                key: "fast:string".to_string()
            }
        ),
        "the durable read route must not serve a string whose deadline has passed. It did, so \
         the fast path's own deadline test is not the one it needs to be.",
    );

    // ---- HALF TWO: HashGetAll ---------------------------------------------------------
    backdate(&engine, "fast:hash");
    assert!(
        matches!(
            durable(
                &engine,
                Command::HashGetAll {
                    key: "fast:hash".to_string()
                }
            ),
            CommandResponse::HashEntries { ref entries } if entries.is_empty()
        ),
        "the durable read route must not serve a hash whose deadline has passed",
    );
}

/// The hash-increment validator steps aside for a key whose deadline has passed.
///
/// WHAT IT IS FOR. `validate_command` pre-checks `HINCRBY`: it reads the stored field and
/// refuses a value that is not an integer, or an increment that would overflow, BEFORE the
/// write runs. A key whose deadline has passed is about to be collected, so the stored bytes
/// are not the caller's to be refused over -- the validator returns early instead, and the
/// increment starts from nothing.
///
/// WHY IT NEEDED A TEST. Every existing test of this validator writes a hash with NO deadline,
/// so `expires_at_ms.get(key)` is `None` and the comparison inside the `map` never runs at all.
/// Inverting it -- `>` for `<=` -- failed nothing in 201 selected tests, including the two
/// tests named for this validator: with no deadline armed, the comparison is indistinguishable
/// from a literal `false`, and any predicate would do. Arming one is the whole difference.
///
/// HALVES ASSERTED SEPARATELY. Live-and-refused and lapsed-and-allowed are the two sides of one
/// comparison, and a single claim about either passes while the other is inverted.
#[test]
fn the_hash_increment_validator_steps_aside_for_a_lapsed_deadline() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- HALF ONE / DENOMINATOR: a LIVE non-integer field is refused ------------------
    // This is also the denominator for half two: without it, an error-free answer below could
    // just as well mean the validator never refuses anything.
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["HSET", "incr:lapsed", "f", "notanumber"]),
    );
    assert_eq!(
        RespValue::Error("ERR hash value is not an integer".to_string()),
        resp(&engine, &["HINCRBY", "incr:lapsed", "f", "1"]),
        "DENOMINATOR: a live field holding something that is not an integer must be refused",
    );

    // ---- HALF TWO: the same field, once its deadline has passed ----------------------
    // Armed directly so that nothing collects the key on the way in -- the point is to reach
    // the validator with the stale bytes still in `hashes` and the deadline already behind.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, "incr:lapsed".to_string(), past);
    }
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["HINCRBY", "incr:lapsed", "f", "1"]),
        "a key whose deadline has passed is already gone as far as every read is concerned, so \
         the increment must start from nothing and answer 1 -- not be refused over bytes the \
         caller can no longer see.",
    );
}

/// What a command of one type does to a key of another, TODAY.
///
/// THIS RECORDS A BEHAVIOUR; IT DOES NOT ENDORSE ONE. There is no type check on this surface --
/// the word `WRONGTYPE` appears nowhere in the tree -- and whether to add one is a decision
/// that has not been made. What was missing was any statement of what happens without it, so a
/// change to it happened silently. Deleting this test is the right move the day a type check
/// lands; until then it is the only place that says what a caller gets.
///
/// THREE THINGS, ASSERTED SEPARATELY:
///
///   ONE. A command of the wrong type answers this type's EMPTY value -- nil, an empty array, a
///   zero -- and never an error. It reads exactly like a key that is not there.
///
///   TWO. The types COEXIST. A string, a hash, a list, a set and a sorted set can all live
///   under one key at the same time, each readable through its own commands, because each is a
///   separate map on the shard keyed by the same string.
///
///   THREE. `TYPE` names only the FIRST of them it finds, in its probe order, so it reports
///   `string` for a key holding five things. `DEL` and the deadline, by contrast, cover all of
///   them at once -- `delete_record_exact` clears every map.
#[test]
fn a_command_of_one_type_answers_empty_for_a_key_of_another() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- ONE: the wrong type reads as absent, not as an error -------------------------
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "mixed", "sv"]),
    );
    assert_eq!(
        RespValue::SimpleString("string".to_string()),
        resp(&engine, &["TYPE", "mixed"]),
        "DENOMINATOR: the key is a string and nothing else yet",
    );
    assert_eq!(RespValue::Array(Vec::new()), resp(&engine, &["HGETALL", "mixed"]));
    assert_eq!(RespValue::Bulk(None), resp(&engine, &["HGET", "mixed", "f"]));
    assert_eq!(RespValue::Array(Vec::new()), resp(&engine, &["LRANGE", "mixed", "0", "-1"]));
    assert_eq!(RespValue::Array(Vec::new()), resp(&engine, &["SMEMBERS", "mixed"]));
    assert_eq!(RespValue::Bulk(None), resp(&engine, &["ZSCORE", "mixed", "m"]));
    assert_eq!(RespValue::Integer(0), resp(&engine, &["LLEN", "mixed"]));

    // ---- TWO: five types under one key, all readable ---------------------------------
    assert_eq!(RespValue::Integer(1), resp(&engine, &["HSET", "mixed", "f", "hv"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["LPUSH", "mixed", "lv"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["SADD", "mixed", "m"]));
    assert_eq!(RespValue::Integer(1), resp(&engine, &["ZADD", "mixed", "1", "z"]));
    assert_eq!(
        Some(b"sv".to_vec()),
        get(&engine, "mixed"),
        "the string is still there after four writes of other types over the same key",
    );
    assert_eq!(
        RespValue::Array(vec![
            RespValue::Bulk(Some(b"f".to_vec())),
            RespValue::Bulk(Some(b"hv".to_vec())),
        ]),
        resp(&engine, &["HGETALL", "mixed"]),
        "and so is the hash",
    );
    assert_eq!(
        RespValue::Array(vec![RespValue::Bulk(Some(b"lv".to_vec()))]),
        resp(&engine, &["LRANGE", "mixed", "0", "-1"]),
        "and the list",
    );
    assert_eq!(
        RespValue::Array(vec![RespValue::Bulk(Some(b"m".to_vec()))]),
        resp(&engine, &["SMEMBERS", "mixed"]),
        "and the set",
    );

    // ---- THREE: TYPE names one of the five; DEL covers all of them -------------------
    assert_eq!(
        RespValue::SimpleString("string".to_string()),
        resp(&engine, &["TYPE", "mixed"]),
        "`TYPE` probes string first and returns on the first hit, so it names the string and \
         says nothing about the four other collections under the same key",
    );
    assert_eq!(RespValue::Integer(1), resp(&engine, &["DEL", "mixed"]));
    assert_eq!(None, get(&engine, "mixed"), "DEL cleared the string");
    assert_eq!(RespValue::Array(Vec::new()), resp(&engine, &["HGETALL", "mixed"]), "and the hash");
    assert_eq!(
        RespValue::Array(Vec::new()),
        resp(&engine, &["LRANGE", "mixed", "0", "-1"]),
        "and the list",
    );
    assert_eq!(RespValue::Array(Vec::new()), resp(&engine, &["SMEMBERS", "mixed"]), "and the set");
    assert_eq!(
        RespValue::SimpleString("none".to_string()),
        resp(&engine, &["TYPE", "mixed"]),
        "and `TYPE` has nothing left to name",
    );
}
