// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Four expiry decisions on the command layer that nothing was watching.
//!
//! A mutation audit of this layer scored 7 killed out of 18 attempted -- the lowest of the nine
//! components it covered. Four of the survivors are guarded here. They are four separate
//! claims, in four tests, because a single test that fires for any of them says nothing about
//! which one it was watching.
//!
//! WHAT THE FOUR HAVE IN COMMON. Every one of them is invisible to the command's own ANSWER.
//! `TTL` replies -2 whether or not the collection was recorded; `PERSIST` replies 0 down both
//! branches; `EXPIRE key 0` replies 1 whether the key was discarded or merely given a deadline
//! of now; and a key whose deadline is exactly the current instant reads as absent either way
//! a millisecond later. So each test below asks the SHARD, not the reply -- and asks it before
//! anything else has had a chance to expire the key lazily.
//!
//! WHICH ENTRY POINT, AND WHY. All four go through `engine.execute`, not `execute_durable`.
//! That is a deliberate choice and not the default one: production reads reach
//! `execute_read_only_fast_path`, which only `execute_durable` can enter, and a test through
//! `execute` cannot see it. None of the four sites is on that fast path. Two of them
//! (`CommonTtl`, `CommonPersist`) are arms of `execute_on_shard`, which the fast path never
//! reaches because it answers before dispatch; one is in the RESP layer above the engine
//! entirely; and one is in `validate_command_preconditions`, which runs on the shared path
//! ahead of the fast-path check and is reached identically by both routes. `execute` is
//! therefore the narrower route that still contains all four, and going through
//! `execute_durable` would add a storage override that none of these claims is about.

#![allow(clippy::all)]
use super::*;

use crate::redis::{execute_redis_command, RespValue};

/// Drive one RESP command against a live engine on shard 1, through `engine.execute`.
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

/// Arm a deadline in the past WITHOUT letting any command collect the key on the way in.
///
/// Going through `EXPIRE` would run the very arms under test and answer the question there
/// instead; sleeping past a short deadline would make the test a race. This writes the
/// deadline straight into the shard, which is what the clock would have produced anyway.
fn backdate(engine: &TemporalEngine, key: &str) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
    crate::engine::set_expiry(shard, key.to_string(), past);
}

/// How many live blocks the shard's index accounts for, and whether a given key's routing
/// bucket is still one of them.
fn index_state(engine: &TemporalEngine, key: &str) -> (usize, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let bucket = crate::engine::block_routing_bucket(key, 0, u32::MAX);
    (
        shard.bucket_index.object_block_lookup.len(),
        shard.bucket_index.bucket_map.contains_key(&bucket),
    )
}

/// What the SHARD holds for a key: (the record is still there, a deadline is still recorded).
fn shard_state(engine: &TemporalEngine, key: &str) -> (bool, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    (
        shard.strings.contains_key(key),
        shard.expires_at_ms.contains_key(key),
    )
}

/// Forget which objects are waiting to be written out, so the next command's marking is the
/// only thing the count below can be measuring. A dump does exactly this when it completes.
fn forget_pending_writes(engine: &TemporalEngine) -> usize {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    shard.dirty_objects.clear();
    shard.dirty_objects.len()
}

/// How many objects are marked as needing a write, and whether this key is one of them.
fn pending_writes(engine: &TemporalEngine, key: &str) -> (usize, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    (
        shard.dirty_objects.len(),
        shard.dirty_objects.contains(key),
    )
}

// =====================================================================================
// ONE: a `TTL` that collects an expired key has to REPORT that it collected one.
// =====================================================================================

/// `TTL` on a key whose deadline has passed removes the record, and must say that it did.
///
/// THE LINE. `execute_on_shard`'s `CommonTtl` arm reads
/// `mutated |= drop_if_expired(cache, shard_id, shard, &key)`. Dropping the accumulation --
/// `let _ = drop_if_expired(..)` -- still collects the record and still invalidates its cached
/// copy, so every answer the command gives is unchanged. It survived 201 selected tests and
/// then 566 durability-selected ones.
///
/// WHY THE FLAG IS NOT BOOKKEEPING. `outcome.mutated` is the condition on a block in
/// `execute` that runs from line 915 to line 1344, and everything that keeps the shard's block
/// index honest is inside it. `CommonTtl` is NOT a write command and carries no object keys,
/// so for THIS arm the block reduces to one thing: `rebuild_bucket_block_ownership`, which
/// re-derives the index from the live model maps. Skip it after a record has left `strings`
/// and the index still accounts for a block belonging to a record that is gone -- which is a
/// block that compaction and reclaim will keep treating as live.
///
/// SO THE CLAIM IS COUNTED, NOT ASSERTED AS A FLAG. The test never reads `mutated`. It reads
/// how many blocks the index accounts for, before and after, and that number has to fall.
///
/// THE CONTROL IS A SECOND KEY THAT IS STILL LIVE. Without it, an index that had been emptied
/// wholesale -- or a rebuild that dropped everything it touched -- would satisfy the claim and
/// look like a result.
#[test]
fn a_ttl_that_collects_an_expired_key_maintains_the_block_index() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- DENOMINATOR: two records, two blocks, both accounted for -----------------------
    for key in ["ttl:collected", "ttl:live"] {
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(&engine, &["SET", key, "v"]),
            "DENOMINATOR: {key} has to be written before its block can be accounted for",
        );
    }
    let (blocks_before, collected_bucket_before) = index_state(&engine, "ttl:collected");
    let (_, live_bucket_before) = index_state(&engine, "ttl:live");
    assert_eq!(
        2, blocks_before,
        "DENOMINATOR: the index must account for the two blocks just written, and it \
         accounted for {blocks_before}. Every count below is a change from this one, so if \
         writes are not registering here there is nothing for the rest of the test to measure.",
    );
    assert!(
        collected_bucket_before && live_bucket_before,
        "DENOMINATOR: both keys' routing buckets must be in the bucket map before either can \
         be observed leaving it",
    );

    // Only one of them expires, and it expires without any command seeing it.
    backdate(&engine, "ttl:collected");

    // ---- THE CLAIM: the collection is reported, so the index is maintained --------------
    assert_eq!(
        RespValue::Integer(-2),
        resp(&engine, &["TTL", "ttl:collected"]),
        "TTL on a key whose deadline has passed answers -2. This is the same answer the \
         unreported collection gives, which is exactly why the rest of this test does not \
         look at the answer.",
    );
    let (record, deadline) = shard_state(&engine, "ttl:collected");
    assert!(
        !record && !deadline,
        "the record must have left the shard -- it does so whether or not the collection is \
         reported, and if it has not then the arm did not collect at all and the count below \
         would be measuring nothing",
    );

    let (blocks_after, collected_bucket_after) = index_state(&engine, "ttl:collected");
    assert_eq!(
        1, blocks_after,
        "THE CLAIM: the index accounted for {blocks_before} blocks and must now account for 1, \
         and it accounts for {blocks_after}. A count that has not moved is the collection going \
         unreported: `drop_if_expired` removed the record, `mutated` stayed false, and the \
         whole index-maintenance block was skipped -- so the index still accounts for a block \
         belonging to a record that is no longer there, and reclaim will go on treating it as \
         live.",
    );
    assert!(
        !collected_bucket_after,
        "and the collected key's routing bucket must no longer be in the bucket map",
    );

    // ---- CONTROL: the live key's block is untouched -------------------------------------
    // Without this, an index that had simply been emptied would satisfy the claim above.
    let (_, live_bucket_after) = index_state(&engine, "ttl:live");
    assert!(
        live_bucket_after,
        "CONTROL: the key that has NOT expired must keep its routing bucket. It lost it, so \
         the maintenance is dropping live blocks rather than the collected one.",
    );
    assert_eq!(
        RespValue::Integer(-1),
        resp(&engine, &["TTL", "ttl:live"]),
        "CONTROL: a key with no deadline answers -1, not -2. A -2 here means the control key \
         was collected too and the claim above held for the wrong reason.",
    );
    let (blocks_control, _) = index_state(&engine, "ttl:live");
    assert_eq!(
        1, blocks_control,
        "CONTROL: a TTL that collects NOTHING must not change the count. It moved to \
         {blocks_control}, so the count falls on any TTL at all and says nothing about \
         collection.",
    );
}

// =====================================================================================
// TWO: a `PERSIST` that collects an expired key has to REPORT that it collected one.
// =====================================================================================

/// `PERSIST` on a key whose deadline has passed removes the record, and must say that it did.
///
/// THE LINE. `execute_on_shard`'s `CommonPersist` arm opens with
/// `if remove_if_expired(shard, &key)`. Weakening it to `if remove_if_expired(..) && false`
/// still runs the call -- the left side is evaluated before the right -- so the record still
/// leaves the shard. Execution then falls into the branch meant for a key that is still LIVE,
/// which finds nothing to clear, so `removed` stays false and the reply is
/// `Integer { value: 0 }`: the very same reply the taken branch gives. Nothing about the
/// answer moves. What moves is that the removal is no longer reported.
///
/// WHY THAT MATTERS HERE AND DIFFERENTLY FROM `TTL`. `CommonPersist` IS a write command and
/// DOES carry object keys, so an unreported removal skips the WAL append for the command and
/// leaves the object unmarked for the next write-out. The count below is the marking.
///
/// WHY NOTHING COULD HAVE GUARDED THIS BEFORE. This branch reported a mutation and staged no
/// record of what it had done, so it tripped the standing "changed the shard and recorded
/// nothing" check in every debug build: `SET k v PX 5`, wait, `PERSIST k` panicked on an
/// unmodified tree. A test could not stand here until that was fixed -- and the mutation
/// SUPPRESSED the panic, because a branch that reports nothing never reaches the check. That
/// is the whole reason this one survived: the honest tree crashed where the mutant ran clean.
///
/// THE CONTROL IS THE OTHER BRANCH. `PERSIST` on a key with a live deadline reports a mutation
/// too, by a route the mutation does not touch -- so it still passes under the mutant, and it
/// is what proves the count can see a marking at all.
#[test]
fn persist_on_a_key_whose_deadline_has_passed_reports_what_it_removed() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for key in ["persist:collected", "persist:live"] {
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(&engine, &["SET", key, "v"]),
            "DENOMINATOR: {key} has to exist before PERSIST can do anything to it",
        );
    }
    backdate(&engine, "persist:collected");

    // ---- DENOMINATOR: the count starts at zero, so what it reaches is this command's ----
    let cleared = forget_pending_writes(&engine);
    assert_eq!(
        0, cleared,
        "DENOMINATOR: the pending-write set must start empty, and it held {cleared}. Without \
         this the seeding SET's own marking would satisfy the claim below.",
    );

    // ---- THE CLAIM -----------------------------------------------------------------------
    assert_eq!(
        RespValue::Integer(0),
        resp(&engine, &["PERSIST", "persist:collected"]),
        "PERSIST on a key whose deadline has passed answers 0: there was no live deadline to \
         remove. Both branches answer 0, which is why the count below is the test and this is \
         not.",
    );
    let (record, deadline) = shard_state(&engine, "persist:collected");
    assert!(
        !record && !deadline,
        "the record must have left the shard. It does so either way -- `remove_if_expired` \
         runs before the weakened condition is applied -- so this is a precondition for the \
         count, not the claim.",
    );

    let (pending, marked) = pending_writes(&engine, "persist:collected");
    assert!(
        marked,
        "THE CLAIM: the key the command removed must be marked for write-out, and it was not. \
         The removal went unreported, so the command's whole durability block was skipped: no \
         WAL entry was appended for it and nothing marks the object as changed. The record is \
         gone from memory and nothing durable says so.",
    );
    assert!(
        pending > 0,
        "and the pending-write count must have risen from 0; it reads {pending}",
    );

    // ---- CONTROL: the live-deadline branch still reports, and is not what was weakened ---
    let cleared = forget_pending_writes(&engine);
    assert_eq!(0, cleared, "CONTROL DENOMINATOR: the count is zeroed again");
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["EXPIRE", "persist:live", "3600"]),
        "CONTROL: arm a real deadline an hour out",
    );
    let cleared = forget_pending_writes(&engine);
    assert_eq!(0, cleared, "CONTROL DENOMINATOR: and zeroed once more");
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["PERSIST", "persist:live"]),
        "CONTROL: PERSIST on a key that really has a deadline answers 1",
    );
    let (pending_control, marked_control) = pending_writes(&engine, "persist:live");
    assert!(
        marked_control && pending_control > 0,
        "CONTROL: the live-deadline branch must mark its object too ({pending_control} \
         pending). This branch is untouched by the weakening above, so it is what shows the \
         count is capable of reporting a marking -- if it fails, the claim above proved \
         nothing.",
    );
    let (record_control, deadline_control) = shard_state(&engine, "persist:live");
    assert!(
        record_control && !deadline_control,
        "CONTROL: and the live key keeps its record while losing its deadline, which is what \
         PERSIST is for",
    );
}

// =====================================================================================
// THREE: `EXPIRE key 0` discards the key.
// =====================================================================================

/// A relative expiry of ZERO discards the key, exactly as a negative one does.
///
/// THE LINE. `expire_response` in the RESP layer opens with `if ttl <= 0 { discard_key_now(..) }`.
/// Narrowing it to `if ttl < 0` sends a zero down the ordinary path instead, as
/// `CommonExpire { ttl_ms: 0 }` -- a deadline of "now plus nothing".
///
/// WHY NO ANSWER CHANGES. Both routes reply 1. The discard replies 1 because the key was
/// there; the deadline-of-now replies 1 because the command was accepted. And both leave the
/// key unreadable, because a deadline equal to the instant has already passed, so the very
/// next `GET` collects it lazily and answers nil. A test written on the reply, or on a
/// following read, is satisfied by either.
///
/// WHAT ACTUALLY DIFFERS is the shard. A discard removes the record now. A deadline of now
/// leaves the record sitting in `strings` with an entry in `expires_at_ms`, waiting for
/// something to come and collect it -- so the key the caller asked to discard is still
/// occupying the store, and is still there to be resurrected by anything that clears its
/// deadline before a read arrives.
///
/// WHY ZERO AND NOT NEGATIVE. The existing guard on this surface covers `EXPIRE k -1` and the
/// absolute spellings. Zero is the only value the two comparisons disagree on, and no test
/// used it. The negative case is asserted below as the control that makes the claim mean
/// something.
///
/// THE TWO HALVES ARE COUNTED SEPARATELY. `EXPIRE` and `PEXPIRE` are two verbs through the one
/// function, and one combined "nothing survived" count reads full from whichever is fixed
/// first.
#[test]
fn a_relative_expiry_of_zero_discards_the_key() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    fn seed(engine: &TemporalEngine, key: &str) {
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(engine, &["SET", key, "v"]),
            "DENOMINATOR: {key} has to be there before a zero expiry can discard it",
        );
        let (record, _) = shard_state(engine, key);
        assert!(record, "DENOMINATOR: and the shard has to be holding it");
    }

    // ---- THE TWO HALVES, EACH COUNTED --------------------------------------------------
    let mut attempted = 0usize;
    let mut survivors = 0usize;
    let mut survivor_names: Vec<&str> = Vec::new();

    for (label, key, verb) in [
        ("EXPIRE with a relative time of zero", "zero:expire", "EXPIRE"),
        ("PEXPIRE with a relative time of zero", "zero:pexpire", "PEXPIRE"),
    ] {
        seed(&engine, key);
        attempted += 1;
        assert_eq!(
            RespValue::Integer(1),
            resp(&engine, &[verb, key, "0"]),
            "{label}: the key was there, so the reply is 1",
        );
        // THE SHARD IS ASKED FIRST AND THE ORDER IS LOAD-BEARING. `GET` and `PTTL` both run
        // lazy expiry, so either of them would collect the record this is trying to observe
        // and the claim would hold for the wrong reason.
        let (record, deadline) = shard_state(&engine, key);
        if record || deadline {
            survivors += 1;
            survivor_names.push(label);
        }
    }

    assert_eq!(
        2, attempted,
        "VACUITY FLOOR: both spellings must have been exercised, and only {attempted} were",
    );
    assert_eq!(
        0, survivors,
        "{survivors} of {attempted} zero-length expiries left the key in the shard: \
         {survivor_names:?}. A zero relative time is a request to discard the key now, and a \
         record still sitting in `strings` under a deadline of `now` is not a discarded key -- \
         it is a key waiting to be collected, which still occupies the store and is still \
         there for anything that clears its deadline to bring back. Every reply was 1 and \
         every following GET answers nil either way, so the shard is the only place this is \
         visible.",
    );

    // ---- CONTROL: a NEGATIVE time discards too, which is the already-guarded half --------
    // Without this, a discard that had stopped working entirely would be indistinguishable
    // from the two claims above passing.
    seed(&engine, "zero:negative");
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["EXPIRE", "zero:negative", "-1"]),
    );
    let (record, deadline) = shard_state(&engine, "zero:negative");
    assert!(
        !record && !deadline,
        "CONTROL: a negative relative time still discards. If this fails the discard is broken \
         outright and the claims above were not about zero at all.",
    );

    // ---- CONTROL: a POSITIVE time arms a deadline and KEEPS the record -------------------
    // This is what stops the fix from being "discard whatever you are given".
    seed(&engine, "zero:positive");
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["EXPIRE", "zero:positive", "3600"]),
    );
    let (record, deadline) = shard_state(&engine, "zero:positive");
    assert!(
        record && deadline,
        "CONTROL: an hour out must leave the record in place WITH a deadline recorded. This is \
         the denominator for both `!deadline` claims above: without it they would also hold if \
         the expiry index never recorded anything at all.",
    );

    // ---- CONTROL: zero on a key that is not there answers 0, not an error ----------------
    assert_eq!(
        RespValue::Integer(0),
        resp(&engine, &["EXPIRE", "zero:absent", "0"]),
        "CONTROL: discarding something already gone is a no-op that succeeded",
    );
}

// =====================================================================================
// FOUR: validation treats a deadline EQUAL to the instant as passed.
// =====================================================================================

/// A key whose stored deadline is exactly the current instant is already gone, and
/// `EXPIRE` on it must be refused rather than resurrect it.
///
/// THE LINE. `validate_command_preconditions` pre-checks `Command::CommonExpire` with
/// `*expires_at <= resolve_now_ms()`. Narrowing it to `<` changes the answer in exactly one
/// millisecond -- the one where the stored deadline equals the clock -- and in no other.
///
/// WHY IT IS NOT ALREADY COVERED. Inverting this comparison is killed by three tests, and the
/// boundary by none. There is a guard on the equal-to-the-instant boundary elsewhere on this
/// surface, but it is on the EXECUTOR's comparison in `remove_if_expired`. This is a second,
/// separate comparison, in a different function, reached earlier, and it decides something
/// else: not "collect this key" but "refuse this command". A guard on one says nothing about
/// the other, which is how a boundary can be guarded and still survive.
///
/// WHAT THE NARROWED COMPARISON DOES. Validation lets the command through, execution collects
/// the expired record on the way past, finds no record left to arm, and reports success -- so
/// the caller is told 1, "the deadline is set", about a key that no longer exists. The honest
/// answer is 0.
///
/// WHY THE CLOCK IS FROZEN. The instant cannot be hit by timing. `resolve_now_ms` answers the
/// replay clock when one is installed, so `ReplayClockGuard` pins it and the comparison is
/// exact rather than a race.
///
/// THE THREE CASES ARE COUNTED SEPARATELY. One combined "refused" count reads full from the
/// past case alone -- which the narrowing does not change -- and says nothing about the
/// boundary, which is the whole subject.
#[test]
fn expire_on_a_key_whose_deadline_is_exactly_now_is_refused() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for key in ["validate:past", "validate:equal", "validate:future"] {
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(&engine, &["SET", key, "v"]),
            "DENOMINATOR: {key} must exist, or every refusal below is just 'no such key'",
        );
    }

    // One instant, used for the deadlines AND for the comparison, so it is exact.
    let instant = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let instant = crate::engine::resolve_now_ms();
        crate::engine::set_expiry(shard, "validate:past".to_string(), instant - 1);
        crate::engine::set_expiry(shard, "validate:equal".to_string(), instant);
        crate::engine::set_expiry(shard, "validate:future".to_string(), instant + 60_000);
        instant
    };

    // Frozen across all three commands below.
    let _clock = crate::engine::ReplayClockGuard::enter(Some(instant));

    // ---- CONTROL: one millisecond BEHIND the instant is refused -------------------------
    // The narrowing does not change this case, so it is what shows validation is refusing
    // anything at all. Without it a validator that had stopped checking would satisfy the
    // "future" control below and the claim would be the only thing failing, with no way to
    // tell a broken validator from a narrowed one.
    assert_eq!(
        RespValue::Integer(0),
        resp(&engine, &["EXPIRE", "validate:past", "3600"]),
        "CONTROL: a key whose deadline passed a millisecond ago is gone, and arming a new \
         deadline on it must be refused. It was not, so validation is not testing deadlines \
         here and nothing else in this test means anything.",
    );

    // ---- THE CLAIM: the instant ITSELF counts as passed ---------------------------------
    assert_eq!(
        RespValue::Integer(0),
        resp(&engine, &["EXPIRE", "validate:equal", "3600"]),
        "THE CLAIM: a deadline EQUAL to the instant has passed, so the key is gone and EXPIRE \
         on it answers 0. A 1 here is the comparison reading `<` where it has to read `<=`: \
         validation let the command through, execution collected the expired record on the way \
         past and had nothing left to arm, and the caller was told the deadline was set on a \
         key that does not exist.",
    );

    // ---- CONTROL: a deadline still AHEAD is accepted ------------------------------------
    // Without this, a validator that refused everything would satisfy both claims above.
    assert_eq!(
        RespValue::Integer(1),
        resp(&engine, &["EXPIRE", "validate:future", "3600"]),
        "CONTROL: a key whose deadline is still ahead is live, and arming a new deadline on it \
         must be accepted. It was refused, so validation is rejecting live keys.",
    );

    // ---- and the shard agrees with the three answers ------------------------------------
    let (record, _) = shard_state(&engine, "validate:future");
    assert!(
        record,
        "CONTROL: the live key must still be in the shard after its deadline was re-armed",
    );
}

// =====================================================================================
// FIVE: and the same discarded-answer shape, everywhere it could occur.
// =====================================================================================

/// Every `drop_if_expired` call in `execute_on_shard` accumulates into `mutated`.
///
/// WHY THIS SITS BESIDE A BEHAVIOURAL TEST RATHER THAN INSTEAD OF ONE. The test at the top of
/// this file kills the discarded answer in ONE arm, `CommonTtl`, by counting what the report
/// maintains. There are 36 call sites. Standing up a behavioural case for each would need a
/// constructed `Command` per arm, and the arms that could not be constructed generically would
/// have to be listed and excused -- which is an exemption list, and an exemption list is a
/// hiding place. This asks something uniform instead, which no call site has grounds to be
/// excused from, so it needs no list at all.
///
/// WHY THE PROPERTY IS UNIFORM. `drop_if_expired` exists to REPORT. It wraps
/// `remove_if_expired` and adds the cache invalidation, and it returns whether it collected
/// anything for exactly one purpose: so the caller can fold it into `mutated`. A call that
/// throws the answer away has kept the collection and dropped the record of it, which is the
/// one thing the wrapper is for. That is why there is nothing to exempt here -- unlike bare
/// `remove_if_expired`, which is called in two honest shapes (tested and discarded) and so
/// could not carry a claim like this one without a list of excuses.
///
/// WHAT IT CANNOT SEE. This reads source text, so a rewrite that changed how the
/// accumulation is spelled would blind it. The denominator below is the answer to that: a
/// changed spelling collapses the call-site count and fails the vacuity floor loudly rather
/// than reading clean.
#[test]
fn every_drop_if_expired_call_accumulates_into_the_mutation_report() {
    const SOURCE: &str = include_str!("../execute_on_shard.rs");
    const CALL: &str = "drop_if_expired(";
    const DEFINITION: &str = "fn drop_if_expired(";
    const ACCUMULATE: &str = "mutated |= drop_if_expired(";

    /// (call sites seen, sites that discard the answer with their line numbers)
    fn scan(source: &str) -> (usize, Vec<(usize, String)>) {
        let mut sites = 0usize;
        let mut discarding = Vec::new();
        for (index, line) in source.lines().enumerate() {
            if !line.contains(CALL) || line.contains(DEFINITION) {
                continue;
            }
            sites += 1;
            if !line.contains(ACCUMULATE) {
                discarding.push((index + 1, line.trim().to_string()));
            }
        }
        (sites, discarding)
    }

    // ---- DENOMINATOR ------------------------------------------------------------------
    let definitions = SOURCE.lines().filter(|l| l.contains(DEFINITION)).count();
    assert_eq!(
        1, definitions,
        "VACUITY: `{DEFINITION}` was found {definitions} times, not once. This guard is not \
         reading the file it thinks it is.",
    );

    let (sites, discarding) = scan(SOURCE);
    assert!(
        sites > 20,
        "VACUITY: the scan found only {sites} calls to `drop_if_expired`. Either the helper was \
         renamed or the arms stopped using it, and this guard is watching nothing. The count is \
         the point: it must fail here rather than read clean on an empty scan.",
    );

    // ---- POSITIVE CONTROL: the scan really can see a discarded answer -----------------
    // Planted into a COPY of the source, in the spelling the real weakening used. Without
    // this, a scan that matched nothing would report zero discarding sites and be mistaken
    // for a result.
    let planted = SOURCE.replacen(ACCUMULATE, "let _ = drop_if_expired(", 1);
    assert_ne!(
        SOURCE, planted,
        "POSITIVE CONTROL: nothing was planted, so the control proves nothing",
    );
    let (planted_sites, planted_discarding) = scan(&planted);
    assert_eq!(
        sites, planted_sites,
        "POSITIVE CONTROL: planting must not change how many call sites are seen \
         ({sites} before, {planted_sites} after)",
    );
    assert_eq!(
        1,
        planted_discarding.len(),
        "POSITIVE CONTROL: exactly one planted `let _ = drop_if_expired(..)` must be caught, \
         and {} were. The detector cannot see the shape it exists to catch.",
        planted_discarding.len(),
    );

    // ---- THE CLAIM --------------------------------------------------------------------
    assert!(
        discarding.is_empty(),
        "{} of {sites} `drop_if_expired` calls throw away what they collected:\n{}\n\n\
         `drop_if_expired` returns whether it removed an expired record so the caller can fold \
         it into `mutated`, and `mutated` is the condition on the block that keeps the shard's \
         block index and its durable record in step with what just left memory. A call whose \
         answer is discarded still collects the record and still drops its cached copy, so no \
         command's reply changes and nothing else in the suite notices -- the record simply \
         leaves memory with nothing recording that it did.",
        discarding.len(),
        discarding
            .iter()
            .map(|(line, text)| format!("  execute_on_shard.rs:{line}: {text}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}
