// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Expiry at scale: a round costs the DUE set, not the keyspace.
//!
//! WHAT IS BEING DEFENDED. `expiry_by_deadline` is a `BTreeMap<(deadline_ms, key), ()>` kept
//! beside the key-ordered `expires_at_ms`. Because it is ordered by deadline, the keys that are
//! due are a PREFIX of it, so the sweep reads `due_window` -- which stops at the first deadline in
//! the future -- instead of walking the keyspace and testing each deadline as it goes.
//!
//! Before that, time-to-expire was a function of the KEYSPACE rather than of how many keys were
//! due: the sweep walked `expires_at_ms` in KEY order from a cursor, so a key that was not due was
//! still walked and still charged against the round's scan budget. Measured cost was
//! `keyspace / scan_budget` rounds -- ten expired keys sitting behind 10,000 live ones survived
//! more than sixty rounds, and behind 40,000 they survived proportionally longer. After the
//! ordered index: one round, at every size measured.
//!
//! WHY THIS NEEDS A GUARD THAT ARGUES, NOT JUST ONE THAT PASSES. A ROUND-ROBIN CURSOR is a
//! reasonable-looking design for the same job, and it is the idiom a comparable system uses for
//! every periodic stage it runs: give the round `Scan(N)` slots, walk N of them, remember where
//! you stopped, and a key expires when its slot comes round again. It is uniform, it is trivially
//! bounded, and it makes every periodic stage look alike. Someone comparing the two systems could
//! very reasonably propose adopting it here.
//!
//! It should NOT be adopted, and the reason is not a preference:
//!
//!   * A round-robin cursor BOUNDS the scan. An ordered index REMOVES it. Bounding a walk caps
//!     the cost of a round while leaving the LATENCY proportional to the keyspace -- the key is
//!     found when its slot comes up, which is `keyspace / N` rounds away in the worst case, and
//!     the worst case is the ordinary case for any shard with far more live keys than due ones.
//!     Reading a prefix makes the latency proportional to the DUE set instead, which is the
//!     quantity the operator actually cares about.
//!   * The cursor's bound is not free of the keyspace either: a cursor that has to keep moving to
//!     make progress must eventually traverse everything, so the sweep pays the whole keyspace per
//!     full cycle whether or not anything was due. The prefix read pays nothing when nothing is
//!     due -- the first deadline it looks at is in the future and it stops.
//!   * The scan budget is still here (`expiry_scan_budget`), and still doing the job a cursor
//!     would do, for the case a cursor is actually right for: a long run of DUE keys that `keep`
//!     rejects because they belong to the other residency class. That is a bound on work the round
//!     cannot use, not a substitute for knowing where the work is.
//!
//! So the ordered index is strictly better than the bounded cursor here, and
//! `an_expired_key_is_removed_in_one_round_behind_a_hundred_thousand_live_keys` is what makes that
//! claim falsifiable: swap the sweep back to a key-ordered scan and it dies with a number.
//!
//! THE INVARIANT THAT PAYS FOR IT. Two indexes hold the same facts twice, so they can disagree.
//! `expires_at_ms` MUST only be mutated through `set_expiry` / `clear_expiry`; a site that writes
//! it directly leaves keys that silently never expire. `the_two_expiry_indexes_agree` (part1)
//! asserts that after a workload; the tests here re-assert it after every path that REBUILDS shard
//! state, because `expiry_by_deadline` is `#[serde(skip)]` and therefore arrives EMPTY from any
//! path that deserializes a shard.
#![allow(clippy::all)]
use super::*;

/// The default round limits the storage manager runs with, which is what the scale claim is about.
const HOT_LIMIT: usize = crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND;
const COLD_LIMIT: usize = crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND;

fn sweep_once(engine: &TemporalEngine, shard_id: ShardId) -> ShardExpirySweepReport {
    engine
        .sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id,
            load_cold_buckets: true,
            max_hot_buckets_per_round: HOT_LIMIT,
            max_cold_buckets_per_round: COLD_LIMIT,
            ..ShardExpirySweepRequest::default()
        })
        .expect("shard 1 is loaded")
}

/// (deadlines held, of those how many are already due) -- the denominator for every claim below.
fn deadline_census(engine: &TemporalEngine, shard_id: ShardId) -> (usize, usize) {
    let now = now_ms();
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&shard_id).expect("shard is loaded");
    let held = shard.expires_at_ms.len();
    let due = shard
        .expires_at_ms
        .values()
        .filter(|expires_at| **expires_at <= now)
        .count();
    (held, due)
}

fn disagreements(engine: &TemporalEngine, shard_id: ShardId) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&shard_id).expect("shard is loaded");
    crate::engine::expiry_index_disagreements(shard)
}

/// Is the deadline-ordered view empty right now? True immediately after any deserializing load,
/// because the field is `#[serde(skip)]`. That emptiness is the thing `ensure_expiry_order` has
/// to repair, so the tests below assert it BEFORE claiming the repair happened.
fn deadline_index_len(engine: &TemporalEngine, shard_id: ShardId) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&shard_id).expect("shard is loaded");
    shard.expiry_by_deadline.len()
}

fn write_keys(engine: &TemporalEngine, shard_id: ShardId, keys: Vec<(String, u64)>) {
    for chunk in keys.chunks(1_000) {
        let commands = chunk
            .iter()
            .map(|(key, ttl_ms)| Command::StringSetEx {
                key: key.clone(),
                value: vec![b'v'; 16],
                ttl_ms: *ttl_ms,
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
}

/// A large keyspace with a small due set expires in ONE round.
///
/// 100,000 live keys, 10 due, and the due keys sort AFTER every live one -- the arrangement a
/// key-ordered scan is worst at. The round limits are the storage manager's defaults, so
/// `expiry_scan_budget` is 128*8 = 1,024: a key-ordered scan reaches the due keys only after
/// 100,000/1,024 ~= 98 rounds, and at the storage manager's 30s cadence that is about 49 minutes
/// of a key that the caller was told had a one-millisecond TTL.
///
/// MEASURED, debug build, this box: one round, 10 of 10 removed, `scanned_records` = 10.
///
/// VERIFIED BY MUTATION. Swapping the two `due_window` calls in `sweep_expired_records_with_request`
/// back to the key-ordered `expiry_window` (which is still in the tree, still tested directly by
/// `paging_the_expiry_window_reaches_every_deadline_once`) makes this test report
/// `removed 0 (scanned 128, skipped 128)` -- the round walks 128 live keys, finds none of them
/// due, and stops. It fails on the number, not on a timeout. The same mutation also kills
/// `a_round_looks_at_the_due_set_not_the_keyspace` and the LOAD phase of
/// `the_deadline_index_is_rebuilt_on_load_manifest_install_and_wal_replay`; it does NOT kill
/// `a_large_due_batch_does_not_stall_a_round`, which is correct -- there every key is due, so a
/// key-ordered scan finds work immediately and that test is measuring cost, not latency.
///
/// What this does NOT claim: that the round is cheap. It is not -- the round below takes seconds
/// of wall clock at this size, because a round that expires anything re-serializes and persists
/// the whole shard index once. That residual is measured and explained in
/// `a_round_looks_at_the_due_set_not_the_keyspace`. The claim here is about the number of records
/// a round has to LOOK AT to find what is due, which is what the ordered index changed.
#[test]
fn an_expired_key_is_removed_in_one_round_behind_a_hundred_thousand_live_keys() {
    const LIVE_KEYS: usize = 100_000;
    const DUE_KEYS: usize = 10;
    // What a key-ordered scan would need at this size, for the failure message to quote.
    const ROUNDS_A_KEY_ORDERED_SCAN_WOULD_NEED: usize = LIVE_KEYS / (HOT_LIMIT * 8);

    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let mut seed = Vec::with_capacity(LIVE_KEYS + DUE_KEYS);
    for index in 0..LIVE_KEYS {
        seed.push((format!("live-{index:08}"), 3_600_000u64));
    }
    // "zzz" so every due key sorts after every live one: a key-ordered cursor has to traverse the
    // entire live set before it can reach one of these.
    for index in 0..DUE_KEYS {
        seed.push((format!("zzz-due-{index:04}"), 1u64));
    }
    write_keys(&engine, 1, seed);
    // Deterministically past the 1ms deadlines rather than relying on the seed write being slow.
    std::thread::sleep(std::time::Duration::from_millis(20));

    // THE DENOMINATOR. Every deadline is held, and exactly DUE_KEYS of them are actually due. A
    // fixture where nothing was due would make "one round cleared them" true and empty.
    let (held_before, due_before) = deadline_census(&engine, 1);
    assert_eq!(
        held_before,
        LIVE_KEYS + DUE_KEYS,
        "the fixture did not land: {held_before} deadlines held, expected {}",
        LIVE_KEYS + DUE_KEYS
    );
    assert_eq!(
        due_before, DUE_KEYS,
        "the fixture must have exactly {DUE_KEYS} due keys hidden behind {LIVE_KEYS} live ones, \
         not {due_before} -- with nothing due this test measures nothing, and with the live keys \
         due as well it stops being a needle in a keyspace"
    );

    let started = std::time::Instant::now();
    let report = sweep_once(&engine, 1);
    let round_us = started.elapsed().as_micros();
    println!(
        "  ONE ROUND over {LIVE_KEYS} live + {DUE_KEYS} due: removed {} \
         (scanned {}, skipped {}) in {round_us} us",
        report.expired_records_removed, report.scanned_records, report.skipped_records
    );

    assert_eq!(
        report.expired_records_removed, DUE_KEYS,
        "one round removed {} of {DUE_KEYS} due keys hidden behind {LIVE_KEYS} live ones. Expiry \
         latency has gone back to being a function of the KEYSPACE instead of the due set: a \
         key-ordered scan needs about {ROUNDS_A_KEY_ORDERED_SCAN_WOULD_NEED} rounds at this size \
         (~{} minutes at the storage manager's 30s cadence). Check that \
         sweep_expired_records_with_request still reads due_window (deadline-ordered, due keys are \
         a PREFIX) and not expiry_window (key-ordered, walks the keyspace).",
        report.expired_records_removed,
        ROUNDS_A_KEY_ORDERED_SCAN_WOULD_NEED / 2
    );
    // The round read the DUE set, not the keyspace: what it looked at is the due set plus at most
    // the cold probe, nowhere near the 1,024 the budget would have allowed a keyspace walk.
    assert!(
        report.scanned_records <= DUE_KEYS * 2,
        "the round looked at {} records to find {DUE_KEYS} due ones -- it is walking rather than \
         reading a prefix",
        report.scanned_records
    );

    // And it took the due ones, not an arbitrary window: every live key is still here, and no
    // deadline is left due.
    let (held_after, due_after) = deadline_census(&engine, 1);
    assert_eq!(
        held_after, LIVE_KEYS,
        "the sweep should have removed exactly the {DUE_KEYS} due deadlines and left the \
         {LIVE_KEYS} live ones, but {held_after} remain"
    );
    assert_eq!(due_after, 0, "{due_after} deadlines are still due after the round");
    assert_eq!(
        disagreements(&engine, 1),
        0,
        "the two expiry indexes disagree after the sweep"
    );
}

/// The deadline index survives every path that rebuilds shard state.
///
/// `expiry_by_deadline` carries `#[serde(skip)]`, so it is NOT in the persisted format -- that is
/// what let the ordered index ship without a migration, and it is also what makes this test
/// necessary. Every path that re-creates a `ShardState` produces one whose deadline-ordered view is
/// EMPTY while `expires_at_ms` is full. `ensure_expiry_order` repairs that on first use, and the
/// three production paths are:
///
///   1. LOAD          -- `load_shard` -> `load_index_checked` -> `decode_index_bytes`
///   2. MANIFEST      -- `install_bucket_dump_manifest` -> `decode_index_bytes`, whole-struct swap
///   3. WAL REPLAY    -- `replay_wal_into_shard`, re-applying commands through `set_expiry`
///
/// Each phase asserts the emptiness FIRST (so "the rebuild happened" is not vacuously true of a
/// state that never needed one), then asserts the invariant and, behaviourally, that a deadline
/// restored by that path still expires in one round. A rebuild that silently did not happen shows
/// up as a key that never expires, which is invisible from outside -- hence both checks.
#[test]
fn the_deadline_index_is_rebuilt_on_load_manifest_install_and_wal_replay() {
    const LIVE_KEYS: usize = 2_000;
    const DUE_KEYS: usize = 5;
    // Long enough to survive the persist/reload, short enough that the test does not crawl.
    const DUE_TTL_MS: u64 = 700;

    let dir = tempfile::tempdir().unwrap();
    let index_dir = dir.path().join("indexes");
    let make_engine = || {
        TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            index_dir.clone(),
        )
    };

    let seed = || {
        let mut seed = Vec::with_capacity(LIVE_KEYS + DUE_KEYS);
        for index in 0..LIVE_KEYS {
            seed.push((format!("live-{index:08}"), 3_600_000u64));
        }
        for index in 0..DUE_KEYS {
            seed.push((format!("zzz-due-{index:04}"), DUE_TTL_MS));
        }
        seed
    };

    // Asserts one rebuild path. `held_floor` is the denominator: the deadlines really came back.
    fn assert_rebuilt(engine: &TemporalEngine, path: &str, held_floor: usize, due_keys: usize) {
        let (held, _) = deadline_census(engine, 1);
        assert!(
            held >= held_floor,
            "{path}: only {held} deadlines came back, expected at least {held_floor} -- with no \
             deadlines restored there is nothing for the rebuild to get wrong"
        );
        // Wait out the short TTLs so the due keys are genuinely due on the other side.
        loop {
            let (_, due) = deadline_census(engine, 1);
            if due >= due_keys {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let (_, due_before) = deadline_census(engine, 1);
        assert_eq!(
            due_before, due_keys,
            "{path}: {due_before} deadlines are due, expected {due_keys}"
        );

        let report = sweep_once(engine, 1);
        println!(
            "  {path}: {held} deadlines restored, {due_before} due, one round removed {}",
            report.expired_records_removed
        );
        assert_eq!(
            report.expired_records_removed, due_keys,
            "{path}: one round removed {} of {due_keys} due keys. A deadline restored by this \
             path is not reachable through expiry_by_deadline, so ensure_expiry_order did not run \
             on this path -- the key would silently never expire.",
            report.expired_records_removed
        );
        assert_eq!(
            disagreements(engine, 1),
            0,
            "{path}: the key-ordered and deadline-ordered expiry indexes disagree after the \
             rebuild -- a write path is bypassing set_expiry/clear_expiry"
        );
    }

    // ---- 1. LOAD from a persisted index -------------------------------------------------
    {
        let engine = make_engine();
        engine.load_shard(1);
        write_keys(&engine, 1, seed());
        let (held, _) = deadline_census(&engine, 1);
        assert_eq!(held, LIVE_KEYS + DUE_KEYS, "the fixture did not land");
        engine.unload_shard(1);

        let engine = make_engine();
        engine.load_shard(1);
        // The emptiness this whole test exists for: the deadline-ordered view is not in the
        // persisted format, so a freshly loaded shard has none of it.
        let ordered = deadline_index_len(&engine, 1);
        let (held, _) = deadline_census(&engine, 1);
        assert!(
            ordered == 0 && held > 0,
            "LOAD was expected to produce {held} deadlines and an EMPTY ordered view \
             (#[serde(skip)]), but the ordered view already holds {ordered}. If the field became \
             serialized, this test no longer covers the rebuild it claims to."
        );
        assert_rebuilt(&engine, "LOAD", LIVE_KEYS + DUE_KEYS, DUE_KEYS);
    }

    // ---- 2. MANIFEST INSTALL ------------------------------------------------------------
    {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        write_keys(&engine, 1, seed());
        let manifest = engine
            .create_bucket_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .install_bucket_dump_manifest(&manifest)
            .expect("manifest should install");
        // The install replaces the whole ShardState with one decoded from the manifest's index
        // bytes, so the ordered view is empty again -- even though this shard never left memory.
        let ordered = deadline_index_len(&engine, 1);
        assert_eq!(
            ordered, 0,
            "MANIFEST INSTALL should have swapped in a state decoded from the manifest, whose \
             ordered view is empty; it holds {ordered}"
        );
        assert_rebuilt(&engine, "MANIFEST INSTALL", LIVE_KEYS + DUE_KEYS, DUE_KEYS);
    }

    // ---- 3. RECOVERY WAL REPLAY ---------------------------------------------------------
    {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            index_dir.clone(),
        );
        engine.load_shard(1);
        write_keys(&engine, 1, seed());
        engine.unload_shard(1);
        // Remove the persisted base index so the load has no checkpoint to start from and has to
        // rebuild the shard by replaying the WAL from sequence zero. Without this the load is the
        // LOAD path again and would prove nothing new.
        let removed = std::fs::remove_file(index_dir.join("shard-1.index.json")).is_ok();
        assert!(removed, "the base index should exist to be removed");

        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            index_dir.clone(),
        );
        engine.load_shard(1);
        // Replay rebuilds state by re-executing commands, so here the deadlines arrive THROUGH
        // `set_expiry` -- which calls `ensure_expiry_order` as its first statement -- rather than
        // through a deserialize. That is a different shape of correctness from the two above, not
        // a rebuild but the path that must not need one.
        //
        // The DENOMINATOR for "replay actually ran": the base index was deleted above, so these
        // deadlines can only have come from the log.
        let (held, _) = deadline_census(&engine, 1);
        assert!(
            held >= LIVE_KEYS + DUE_KEYS,
            "WAL REPLAY restored only {held} deadlines from the log with no base index to load \
             from, expected at least {}",
            LIVE_KEYS + DUE_KEYS
        );
        let ordered = deadline_index_len(&engine, 1);
        // Either replay populated it as it went (every deadline-bearing record goes through
        // set_expiry) or it is still empty and the first sweep will rebuild it. A PARTIAL view is
        // the one state neither mechanism produces and the one `ensure_expiry_order` would never
        // repair -- its guard only fires on an entirely empty map.
        assert!(
            ordered == held || ordered == 0,
            "WAL REPLAY left the ordered view holding {ordered} entries for {held} deadlines. A \
             partially populated view is never repaired: ensure_expiry_order only rebuilds when \
             the map is entirely empty, so those deadlines would silently never expire."
        );
        assert_rebuilt(&engine, "WAL REPLAY", LIVE_KEYS + DUE_KEYS, DUE_KEYS);
    }
}

/// The delta fold cannot leave a stale deadline mirror behind.
///
/// `apply_key_states` is the ONE site that writes `expires_at_ms` without going through
/// `set_expiry` / `clear_expiry`: it restores a whole captured map per key, so it can both set and
/// remove a deadline, and it does so on a state that is mid-reconstruction. Today it only ever
/// runs on a freshly decoded state whose mirror is empty, which makes the hazard invisible -- so
/// this test calls it directly on a shard whose mirror IS populated, which is the state the first
/// caller that folds a delta onto a live shard would produce.
///
/// Why it matters more than an ordinary desync: `ensure_expiry_order` repairs only an ENTIRELY
/// empty mirror. A mirror left populated and wrong is never repaired by anything, and the keys it
/// is wrong about silently never expire.
#[test]
fn folding_a_delta_cannot_leave_a_stale_deadline_mirror() {
    let mut shard = ShardState::default();
    crate::engine::set_expiry(&mut shard, "kept".to_string(), 1_000);
    crate::engine::set_expiry(&mut shard, "moved".to_string(), 2_000);
    crate::engine::set_expiry(&mut shard, "dropped".to_string(), 3_000);
    // THE DENOMINATOR. The mirror is populated before the fold, so "they agree afterwards" is not
    // the trivial agreement of two empty maps -- which is exactly the state this path runs in
    // today, and exactly why the hazard does not show up on its own.
    assert_eq!(
        shard.expiry_by_deadline.len(),
        3,
        "the mirror must be populated before the fold or this test proves nothing"
    );

    // One blob moves a deadline; one omits the field entirely, which the fold reads as "this key
    // has no deadline" and turns into a removal. Both directions, one call.
    let key_states = vec![
        serde_json::json!({"key": "moved", "expires_at_ms": 9_000u64}),
        serde_json::json!({"key": "dropped"}),
    ];
    super::apply_key_states(&mut shard, &key_states);

    assert_eq!(
        shard.expires_at_ms.get("moved").copied(),
        Some(9_000),
        "the fold should have moved this deadline"
    );
    assert!(
        !shard.expires_at_ms.contains_key("dropped"),
        "the fold should have removed this deadline"
    );

    // The production repair, exactly as the sweep calls it.
    crate::engine::ensure_expiry_order(&mut shard);
    assert_eq!(
        crate::engine::expiry_index_disagreements(&shard),
        0,
        "after a delta fold the two expiry indexes disagree: {} deadlines vs {} ordered entries. \
         apply_key_states writes expires_at_ms directly, so it must invalidate expiry_by_deadline \
         -- ensure_expiry_order will not repair a mirror that is merely wrong, only one that is \
         empty.",
        shard.expires_at_ms.len(),
        shard.expiry_by_deadline.len(),
    );
    // And the moved deadline is reachable at its NEW position, not only absent from its old one.
    assert!(
        shard.expiry_by_deadline.contains_key(&(9_000, "moved".to_string())),
        "the moved deadline is not in the ordered view at 9000, so the sweep would never see it"
    );
}

/// A large due batch does not stall a round.
///
/// The sweep is not free once it FINDS the keys: every expired key gets a WAL tombstone
/// (`Command::CommonDelete`, appended buffered and unfsynced), a cache invalidation, and then the
/// round anchors `applied_wal_sequence` and persists the index once. So the round's cost has a
/// per-key term the ordered index does not remove -- it only stopped the round paying for keys it
/// was never going to expire.
///
/// This measures that per-key term at a batch far larger than the storage manager's default round
/// (128), and asserts it stays LINEAR: a round clearing 8,000 due keys must not cost more than
/// about twice the per-key cost of one clearing 1,000. A superlinear term here -- a per-key index
/// serialization, a per-key whole-shard walk -- would turn a burst of expiries into a stalled
/// round, which is the failure the ordered index was supposed to have ruled out.
///
/// Wall-clock on a shared box is noisy, so the assertion is a generous RATIO of per-key costs and
/// the absolute numbers are printed rather than asserted.
#[test]
fn a_large_due_batch_does_not_stall_a_round() {
    // Per-key cost of one round that expires `due_keys` of `due_keys` due, in microseconds.
    fn per_key_us(due_keys: usize) -> f64 {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let seed = (0..due_keys)
            .map(|index| (format!("due-{index:08}"), 1u64))
            .collect::<Vec<_>>();
        write_keys(&engine, 1, seed);
        std::thread::sleep(std::time::Duration::from_millis(20));

        // Denominator: they are all really due before the round starts.
        let (held, due) = deadline_census(&engine, 1);
        assert_eq!(held, due_keys, "{held} deadlines held, expected {due_keys}");
        assert_eq!(due, due_keys, "only {due} of {due_keys} keys are due");

        // One round, sized to take the whole batch.
        let started = std::time::Instant::now();
        let report = engine
            .sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: 1,
                load_cold_buckets: true,
                max_hot_buckets_per_round: due_keys,
                max_cold_buckets_per_round: COLD_LIMIT,
                ..ShardExpirySweepRequest::default()
            })
            .expect("shard 1 is loaded");
        let elapsed_us = started.elapsed().as_micros() as f64;

        // The WORK-DONE column: a round that expired nothing is fast for an uninteresting reason.
        assert_eq!(
            report.expired_records_removed, due_keys,
            "the round removed {} of {due_keys} due keys, so the time below is not the cost of \
             clearing them",
            report.expired_records_removed
        );
        let (held_after, _) = deadline_census(&engine, 1);
        assert_eq!(held_after, 0, "{held_after} deadlines survived a round sized to take them all");

        println!(
            "  due_keys={due_keys:>6}  round {:>8.1} ms  {:>7.1} us/key  (removed {})",
            elapsed_us / 1_000.0,
            elapsed_us / due_keys as f64,
            report.expired_records_removed
        );
        elapsed_us / due_keys as f64
    }

    let small = per_key_us(1_000);
    let large = per_key_us(8_000);
    let growth = large / small;
    println!("  per-key: {small:.1} us at 1k due -> {large:.1} us at 8k due  ({growth:.2}x)");
    assert!(
        growth < 3.0,
        "the per-key cost of a sweep round grew {growth:.2}x between a 1,000-key and an \
         8,000-key due batch ({small:.1} -> {large:.1} us/key). The round is meant to be LINEAR \
         in the due set: one WAL tombstone per key, one anchor and one index persist for the \
         round. Something in the per-key path is scaling with the batch."
    );
}

/// What the ordered index removed from a round, and what it did NOT.
///
/// The claim this file defends is about the SCAN: a round no longer looks at keys it was never
/// going to expire. That claim is exact and this test pins it -- `scanned_records` is the due set
/// at every keyspace, not a fraction of the keyspace.
///
/// It was for a long time not a claim that a round is free of the keyspace. A round that expired
/// ANYTHING ended by serializing the whole shard index and persisting it once, which is
/// whole-shard work whether ten keys expired or ten thousand: measured here, debug build, a round
/// expiring 10 keys cost about 0.11 s at a 2,000-key shard and 0.92 s at 20,000 -- 8.5x for the
/// same ten records looked at.
///
/// FIXED. The checkpoint is now an index-log DELTA naming the keys the round removed, so what a
/// round writes is proportional to the round. `an_expiry_round_persists_what_changed` measures
/// that in BYTES against an arm that still writes the whole index, and the timings printed below
/// are the loose confirmation rather than the claim.
///
/// The region change came first and was a different thing: the flush no longer runs under the
/// shard write guard (`the_expiry_sweep_flush_waits_for_the_write_guard_to_drop`, part1). That
/// moved WHO WAITS. This moved how much work a round does.
///
/// The distinction matters for the round-robin-cursor proposal too: a bounded cursor would not
/// have touched either term. It bounds the walk; it does not make the round's fixed cost smaller,
/// and it would have kept the scan cost the index removed.
#[test]
fn a_round_looks_at_the_due_set_not_the_keyspace() {
    const DUE_KEYS: usize = 10;

    // (records the round looked at, wall-clock microseconds) for `live_keys` live + DUE_KEYS due.
    fn round_at(live_keys: usize) -> (usize, u128) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut seed = Vec::with_capacity(live_keys + DUE_KEYS);
        for index in 0..live_keys {
            seed.push((format!("live-{index:08}"), 3_600_000u64));
        }
        for index in 0..DUE_KEYS {
            seed.push((format!("zzz-due-{index:04}"), 1u64));
        }
        write_keys(&engine, 1, seed);
        std::thread::sleep(std::time::Duration::from_millis(20));

        // Denominator, both halves: the keyspace is really there and exactly DUE_KEYS are due.
        let (held, due) = deadline_census(&engine, 1);
        assert_eq!(held, live_keys + DUE_KEYS, "the fixture did not land: {held} deadlines");
        assert_eq!(due, DUE_KEYS, "{due} keys are due, expected {DUE_KEYS}");

        let started = std::time::Instant::now();
        let report = sweep_once(&engine, 1);
        let elapsed_us = started.elapsed().as_micros();
        assert_eq!(
            report.expired_records_removed, DUE_KEYS,
            "the round removed {} of {DUE_KEYS}, so neither number below is the cost of clearing \
             them",
            report.expired_records_removed
        );
        println!(
            "  live_keys={live_keys:>6}  scanned {:>4}  round {:>8.1} ms",
            report.scanned_records,
            elapsed_us as f64 / 1_000.0
        );
        (report.scanned_records, elapsed_us)
    }

    let (scanned_small, round_small_us) = round_at(2_000);
    let (scanned_large, round_large_us) = round_at(20_000);

    // THE WIN, asserted: what the round LOOKS AT is the due set, identically, at a keyspace ten
    // times larger. A key-ordered scan would have looked at the scan budget's worth -- 1,024 --
    // at both sizes and still not reached the due keys at the larger one.
    assert_eq!(
        scanned_small, DUE_KEYS,
        "a round over a 2,000-key shard looked at {scanned_small} records to find {DUE_KEYS} due"
    );
    assert_eq!(
        scanned_large, DUE_KEYS,
        "a round over a 20,000-key shard looked at {scanned_large} records to find {DUE_KEYS} \
         due. The scan is back: what a round examines is scaling with the keyspace instead of \
         with the due set."
    );

    // The round's wall clock, printed not asserted: this box is shared and a timing on it is
    // not evidence. The deterministic form of this claim is
    // `an_expiry_round_persists_what_changed`, which counts BYTES.
    println!(
        "  round cost with {DUE_KEYS} due: {:.1} ms at 2k live -> {:.1} ms at 20k live \
         ({:.2}x) -- scan removed, checkpoint is now a delta of what the round removed",
        round_small_us as f64 / 1_000.0,
        round_large_us as f64 / 1_000.0,
        round_large_us as f64 / round_small_us.max(1) as f64
    );
}

/// THE GUARD: what an expiry round PERSISTS tracks what it changed, not what the shard holds.
///
/// `a_round_looks_at_the_due_set_not_the_keyspace` above fixed the SCAN -- a round looks at ten
/// records whether the shard holds two thousand live keys or a hundred thousand. A whole-shard
/// term outlived it: a round that expired anything re-encoded and rewrote the ENTIRE served index
/// once, so the round's cost still tracked LIVE KEYS. Measured before this changed, debug build,
/// ten due keys: 109 ms at 2,000 live, 922 ms at 20,000 -- 8.5x for the same ten records.
///
/// The round now appends an index-log DELTA instead: one record carrying a tombstone blob per key
/// the round removed, which `fold_index_log_deltas` folds onto the base on load. That is the same
/// record shape the ordinary delete path has always written, and expiry IS a logged delete.
///
/// COUNTED IN BYTES, NOT TIMED. Timings on this box are void above about 24 load, and a ratio
/// between two arms is exactly the thing a noisy box corrupts. Checkpoint bytes are deterministic:
/// the served-index encode bytes (`index_encode_counts`, zero for a delta round) plus what the
/// round appended to the index log.
///
/// THE POSITIVE CONTROL IS AN ARM, NOT A COMMENT. `flush_whole_expiry_index_for_test` keeps the
/// whole-index checkpoint reachable, so both numbers come from the same process on the same
/// fixture. Without it, "the bytes are flat" would be satisfied just as well by a round that
/// stopped writing anything -- and the whole-index arm's ratio is asserted to GROW, which is what
/// proves the measurement can see the shard at all.
#[test]
fn an_expiry_round_persists_what_changed() {
    const DUE_KEYS: usize = 10;
    const SMALL: usize = 2_000;
    const LARGE: usize = 20_000;

    /// Bytes one round wrote for its served-index checkpoint, and the records it removed.
    fn checkpoint_bytes(live_keys: usize, whole_index: bool) -> (u64, usize) {
        let engine = TemporalEngine::default();
        if whole_index {
            engine.flush_whole_expiry_index_for_test();
        }
        engine.load_shard(1);
        let mut seed = Vec::with_capacity(live_keys + DUE_KEYS);
        for index in 0..live_keys {
            seed.push((format!("live-{index:08}"), 3_600_000u64));
        }
        for index in 0..DUE_KEYS {
            seed.push((format!("zzz-due-{index:04}"), 1u64));
        }
        write_keys(&engine, 1, seed);
        std::thread::sleep(std::time::Duration::from_millis(20));

        // Denominator, both halves: the keyspace is really there and exactly DUE_KEYS are due.
        let (held, due) = deadline_census(&engine, 1);
        assert_eq!(held, live_keys + DUE_KEYS, "the fixture did not land: {held} deadlines");
        assert_eq!(due, DUE_KEYS, "{due} keys are due, expected {DUE_KEYS}");

        crate::engine::reset_index_encode_counts();
        let log_before = engine.index_log_store.stats(1).bytes_written;
        let report = sweep_once(&engine, 1);
        let encoded = crate::engine::index_encode_counts().encode_bytes_total;
        let logged = engine
            .index_log_store
            .stats(1)
            .bytes_written
            .saturating_sub(log_before);
        let written = encoded.saturating_add(logged);

        // THE WORK-DONE COLUMN. A round that expired nothing writes nothing, and would sail
        // through every flatness assertion below.
        assert_eq!(
            report.expired_records_removed, DUE_KEYS,
            "the round removed {} of {DUE_KEYS} due keys, so the bytes below are not the cost of \
             checkpointing them",
            report.expired_records_removed
        );
        let (held_after, due_after) = deadline_census(&engine, 1);
        assert_eq!(due_after, 0, "{due_after} due deadlines survived the round");
        assert_eq!(
            held_after, live_keys,
            "the round left {held_after} deadlines, expected exactly the {live_keys} live ones"
        );

        println!(
            "  {} checkpoint, live_keys={live_keys:>6}: {written:>9} bytes written ({encoded} \
encoded + {logged} logged), {} removed",
            if whole_index { "WHOLE" } else { "DELTA" },
            report.expired_records_removed,
        );
        (written, report.expired_records_removed)
    }

    let (whole_small, _) = checkpoint_bytes(SMALL, true);
    let (whole_large, _) = checkpoint_bytes(LARGE, true);
    let (delta_small, _) = checkpoint_bytes(SMALL, false);
    let (delta_large, _) = checkpoint_bytes(LARGE, false);

    let whole_growth = whole_large as f64 / whole_small.max(1) as f64;
    let delta_growth = delta_large as f64 / delta_small.max(1) as f64;
    println!(
        "  {DUE_KEYS} due keys, checkpoint bytes 2k live -> 20k live: WHOLE {whole_small} -> \
{whole_large} ({whole_growth:.2}x), DELTA {delta_small} -> {delta_large} ({delta_growth:.2}x)"
    );

    // POSITIVE CONTROL, asserted: the measurement can see the shard. Ten times the live keys,
    // and the whole-index checkpoint grows with them. If this stops firing, the flatness below
    // is not evidence of anything.
    assert!(
        whole_growth > 4.0,
        "the WHOLE-index checkpoint grew only {whole_growth:.2}x ({whole_small} -> {whole_large} \
         bytes) for a ten-fold larger keyspace. That arm exists to be the thing the delta is \
         measured against; if it no longer tracks the shard, this measurement proves nothing and \
         the flatness assertion below is vacuous"
    );
    assert!(
        delta_small > 0 && delta_large > 0,
        "the delta checkpoint wrote nothing at all ({delta_small} / {delta_large} bytes). A round \
         that removed {DUE_KEYS} keys has to write the record that describes the removal, or the \
         removal survives only in the WAL and the served index is reconstructed without it"
    );

    // THE ASSERTION. Same ten records removed at both sizes, so the checkpoint should cost the
    // same at both sizes.
    assert!(
        delta_growth < 1.5,
        "the expiry checkpoint grew {delta_growth:.2}x ({delta_small} -> {delta_large} bytes) \
         between a 2,000-key and a 20,000-key shard while removing the same {DUE_KEYS} records \
         both times, against {whole_growth:.2}x for the whole-index arm. The round is persisting \
         something that scales with the SHARD again -- check that the checkpoint is still \
         `ExpiryIndexCheckpoint::Delta` and that nothing in building it walks more than the keys \
         the round removed"
    );
}

/// An expired key stays expired across every path that rebuilds a shard.
///
/// This is the correctness half of `an_expiry_round_persists_what_changed`. The round no longer
/// rewrites the served index, so the base index on disk still NAMES the keys the round removed --
/// what carries the removal forward is the index-log delta plus the WAL tombstones the round
/// appended before it. Both have to hold, on every path that reconstructs a shard.
///
/// WHY BOTH SOURCES ARE ALLOWED TO AGREE. The safety argument is directional: the durable index
/// anchor must never move AHEAD of the tombstones. The delta carries the anchor and the deletions
/// in ONE record, so a reader that folds the anchor has folded the deletions, and a delta that
/// never lands leaves the anchor behind the tombstones and WAL replay re-derives them. A test that
/// forbade the WAL from also being right would be testing something the design does not claim.
///
/// COUNTED. The assertion is the deadline census -- `held` and `due` -- not a `StringGet`. Lazy
/// expiry removes an expired key on read, so a `StringGet` returning None would pass even if the
/// sweep's removal had been lost entirely; a deadline that came BACK is visible only in the census.
#[test]
fn an_expired_key_stays_expired_across_load_manifest_install_and_wal_replay() {
    const LIVE_KEYS: usize = 1_000;
    const DUE_KEYS: usize = 5;
    /// Long enough that the due keys survive a persist/reload with their deadlines intact, short
    /// enough that the test does not crawl. Same bargain as
    /// `the_deadline_index_is_rebuilt_on_load_manifest_install_and_wal_replay`.
    const DUE_TTL_MS: u64 = 700;

    fn seed() -> Vec<(String, u64)> {
        let mut seed = Vec::with_capacity(LIVE_KEYS + DUE_KEYS);
        for index in 0..LIVE_KEYS {
            seed.push((format!("live-{index:08}"), 3_600_000u64));
        }
        for index in 0..DUE_KEYS {
            seed.push((format!("zzz-due-{index:04}"), DUE_TTL_MS));
        }
        seed
    }

    /// Wait out the short TTLs, sweep once, and assert the round ran and wrote no whole index.
    fn sweep_after(engine: &TemporalEngine) {
        loop {
            let (_, due) = deadline_census(engine, 1);
            if due >= DUE_KEYS {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let (held, due) = deadline_census(engine, 1);
        assert_eq!(held, LIVE_KEYS + DUE_KEYS, "the fixture did not land: {held} deadlines");
        assert_eq!(due, DUE_KEYS, "{due} keys are due, expected {DUE_KEYS}");

        crate::engine::reset_index_encode_counts();
        let report = sweep_once(engine, 1);
        assert_eq!(
            report.expired_records_removed, DUE_KEYS,
            "the round removed {} of {DUE_KEYS}, so nothing below is recovering from a removal",
            report.expired_records_removed
        );
        // The denominator that makes every arm below a recovery rather than a re-read: the round
        // wrote no whole served index at all.
        assert_eq!(
            crate::engine::index_encode_counts().encodes_total,
            0,
            "the round re-encoded the whole served index, so the recovery below could be reading \
             the removal straight out of a fresh snapshot"
        );
    }

    /// Load, seed and sweep in one engine, for the arms that do not need the base to be stale.
    fn seed_and_sweep(engine: &TemporalEngine) {
        engine.load_shard(1);
        write_keys(engine, 1, seed());
        sweep_after(engine);
    }

    /// Assert a rebuilt shard: the due keys stayed gone, the live keys came back, the two expiry
    /// indexes agree.
    fn assert_recovered(engine: &TemporalEngine, path: &str) {
        let (held, due) = deadline_census(engine, 1);
        assert_eq!(
            due, 0,
            "{path}: {due} deadlines came back DUE after a round that removed them. The removal \
             reached neither the index-log delta nor WAL replay, so the keys were resurrected"
        );
        assert_eq!(
            held, LIVE_KEYS,
            "{path}: {held} deadlines came back, expected exactly the {LIVE_KEYS} live ones. \
             Either the removal was lost (too many) or the rebuild dropped live keys (too few)"
        );
        // Rebuild the deadline mirror the way production does: the sweep's first act is
        // `ensure_expiry_order`. A freshly loaded shard has an EMPTY mirror (`#[serde(skip)]`),
        // so reading agreement before this would be measuring the load rather than the rebuild.
        //
        // And this round is an assertion of its own, the sharpest one here: it must remove
        // NOTHING. A removal the recovery lost comes back as a live key with a deadline in the
        // past, which is exactly what a round finds.
        let after = sweep_once(engine, 1);
        assert_eq!(
            after.expired_records_removed, 0,
            "{path}: a round run after the rebuild removed {} more keys. Those are keys the \
             PREVIOUS round already expired, resurrected by the recovery with their past \
             deadlines intact",
            after.expired_records_removed
        );
        assert_eq!(
            disagreements(engine, 1),
            0,
            "{path}: the key-ordered and deadline-ordered expiry indexes disagree after the \
             rebuild -- a key it is wrong about would silently never expire"
        );
        // And a live key is actually readable, not merely a deadline with nothing behind it.
        let get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "live-00000000".to_string(),
            },
        });
        assert!(
            matches!(get.response, CommandResponse::Bytes { value: Some(_) }),
            "{path}: a live key did not survive the rebuild: {:?}",
            get.response
        );
    }

    // ---- 1. LOAD from a persisted index, and the index is STALE -------------------------
    //
    // The sharpest arrangement this change has to survive, and the one an unload-after-the-round
    // would hide: the base index is materialized BEFORE the round, so it still names every key
    // the round then removes, and nothing rewrites it afterwards. What the reload has to work
    // from is that stale base plus the WAL tail the round appended -- which is precisely the
    // state the removed whole-index write used to prevent.
    {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("indexes");
        let make_engine = || {
            TemporalEngine::with_local_dirs(
                1024,
                dir.path().join("cache"),
                dir.path().join("pages"),
                index_dir.clone(),
            )
        };
        let engine = make_engine();
        engine.load_shard(1);
        write_keys(&engine, 1, seed());
        // Materialize the base WITH the due keys in it.
        engine.unload_shard(1);
        let base_before = std::fs::metadata(index_dir.join("shard-1.index.json"))
            .expect("the base index should exist before the round")
            .len();

        let engine = make_engine();
        engine.load_shard(1);
        sweep_after(&engine);
        // THE DENOMINATOR for "the base is stale": the round left the file exactly as it found
        // it. If this ever changes, the reload below is reading the removal out of a fresh
        // snapshot and proves nothing about surviving on the WAL tail.
        let base_after = std::fs::metadata(index_dir.join("shard-1.index.json"))
            .expect("the base index should still exist after the round")
            .len();
        assert_eq!(
            base_before, base_after,
            "the expiry round rewrote the base index ({base_before} -> {base_after} bytes), so \
             this arm is no longer recovering through a stale base"
        );

        // No unload: a crash-shaped reload onto the stale base.
        let engine = make_engine();
        engine.load_shard(1);
        assert_recovered(&engine, "LOAD (stale base + WAL tail)");
    }

    // ---- 2. MANIFEST INSTALL ------------------------------------------------------------
    {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        seed_and_sweep(&engine);
        let manifest = engine
            .create_bucket_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .install_bucket_dump_manifest(&manifest)
            .expect("manifest should install");
        assert_recovered(&engine, "MANIFEST INSTALL");
    }

    // ---- 3. RECOVERY WAL REPLAY ---------------------------------------------------------
    {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("indexes");
        let make_engine = || {
            TemporalEngine::with_local_dirs(
                1024,
                dir.path().join("cache"),
                dir.path().join("pages"),
                index_dir.clone(),
            )
        };
        let engine = make_engine();
        seed_and_sweep(&engine);
        engine.unload_shard(1);
        // No base index to start from, so the load rebuilds by replaying the WAL from zero --
        // including the CommonDelete tombstones the sweep appended for the expired keys.
        let removed = std::fs::remove_file(index_dir.join("shard-1.index.json")).is_ok();
        assert!(removed, "the base index should exist to be removed");

        let engine = make_engine();
        engine.load_shard(1);
        assert_recovered(&engine, "WAL REPLAY");
    }
}

/// THE GUARD ON THE RECORD ITSELF: the expiry delta names the keys the round removed, says
/// nothing about any other key, and anchors no further than the round's own tombstones.
///
/// `an_expiry_round_persists_what_changed` counts the delta's BYTES; it would be satisfied by a
/// record of the right size carrying the wrong thing. This reads the record back out of the index
/// log and checks what is in it.
///
/// THE THREE PROPERTIES, and each is a way the change could be wrong while still being cheap:
///
///   1. EVERY REMOVED KEY IS NAMED. `delta_record_covered_keys` reads the covered set from the
///      key-state blobs, and a removed key contributes no page items -- the deletes retained its
///      pages out of the bucket before this record was built. So a key with no blob is a key the
///      fold never wipes: it keeps its pages and its deadline, and it comes back alive.
///   2. EACH BLOB IS BARE. A blob carrying only `key` means "this key is in none of the thirteen
///      per-key maps", which is what `apply_key_state_field` turns into a removal from each of
///      them -- `expires_at_ms` included. A blob captured BEFORE the delete would carry the
///      deadline as a live value and the fold would restore it.
///   3. THE ANCHOR IS NOT AHEAD OF THE TOMBSTONES. This is the correctness bar the whole change
///      rests on, inherited from #1620: an anchor past records that are not durably described
///      suppresses their replay. Here the anchor is exactly the WAL sequence after the round's
///      own `CommonDelete`s -- at them, never past them -- so the record that moves the anchor is
///      the same record that describes the deletions.
#[test]
fn the_expiry_delta_names_what_the_round_removed_and_anchors_no_further() {
    const LIVE_KEYS: usize = 200;
    const DUE_KEYS: usize = 5;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut seed = Vec::with_capacity(LIVE_KEYS + DUE_KEYS);
    for index in 0..LIVE_KEYS {
        seed.push((format!("live-{index:08}"), 3_600_000u64));
    }
    for index in 0..DUE_KEYS {
        seed.push((format!("zzz-due-{index:04}"), 1u64));
    }
    write_keys(&engine, 1, seed);
    std::thread::sleep(std::time::Duration::from_millis(20));

    // Denominator: exactly DUE_KEYS are due behind LIVE_KEYS that are not.
    let (held, due) = deadline_census(&engine, 1);
    assert_eq!(held, LIVE_KEYS + DUE_KEYS, "the fixture did not land: {held} deadlines");
    assert_eq!(due, DUE_KEYS, "{due} keys are due, expected {DUE_KEYS}");

    let records_before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the index log should read back")
        .len();
    let wal_before = engine.wal_store.stats(1).last_sequence;

    let report = sweep_once(&engine, 1);
    assert_eq!(
        report.expired_records_removed, DUE_KEYS,
        "the round removed {} of {DUE_KEYS}, so the record below is not the record of a removal",
        report.expired_records_removed
    );

    // The tombstones are real: one WAL record per expired key, appended before the checkpoint.
    let wal_after = engine.wal_store.stats(1).last_sequence;
    assert_eq!(
        wal_after.saturating_sub(wal_before),
        DUE_KEYS as u64,
        "the round appended {} WAL records for {DUE_KEYS} expired keys, expected one \
         CommonDelete each -- without them the anchor assertion below is comparing against \
         nothing",
        wal_after.saturating_sub(wal_before)
    );

    let records = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the index log should read back");
    assert_eq!(
        records.len(),
        records_before + 1,
        "the round appended {} delta records, expected exactly one for the whole round",
        records.len().saturating_sub(records_before)
    );
    let record = records.last().expect("the round appended a record");

    // The record describes the removal with blobs alone. Every page the round's keys had was
    // retained out of its bucket by the delete, so there is nothing left to name -- and the
    // builder relies on that rather than walking the buckets to rediscover it, which is what
    // made a large due batch superlinear. If a delete ever starts leaving a page behind, this
    // fires here instead of the fold silently wiping that page on the next recovery.
    assert!(
        record.items.is_empty(),
        "the expiry delta carries {} page items. The round's deletes are supposed to leave no \
         page for a covered key, which is why the record is built without collecting any -- a \
         surviving page is now described by nothing and gets wiped on the fold",
        record.items.len()
    );

    let named: std::collections::BTreeSet<String> = record
        .key_states
        .iter()
        .filter_map(|blob| blob.get("key").and_then(|value| value.as_str()))
        .map(str::to_string)
        .collect();
    assert!(
        !named.is_empty(),
        "the expiry delta named no keys at all. `delta_record_covered_keys` reads the covered set \
         from these blobs and a removed key contributes no page items, so a record with no blobs \
         wipes nothing: every key this round removed comes back on a delta-fold recovery"
    );

    // 1. Every removed key is named.
    for index in 0..DUE_KEYS {
        let key = format!("zzz-due-{index:04}");
        assert!(
            named.contains(&key),
            "the expiry delta does not name {key}, which the round removed. The fold covers only \
             the keys these blobs name, so this key keeps its pages and its past deadline and \
             comes back alive. Named: {named:?}"
        );
    }
    // ...and nothing else. A delta that named live keys would wipe their pages on the fold.
    let live_named = named.iter().filter(|key| key.contains("live-")).count();
    assert_eq!(
        live_named, 0,
        "the expiry delta names {live_named} keys the round did not remove. The fold WIPES every \
         page of every covered key and restores only the items the record carries, so naming a \
         live key deletes it on recovery. Named: {named:?}"
    );

    // 2. Each blob is bare -- a tombstone in every per-key map rather than a captured value.
    for blob in &record.key_states {
        let fields = blob
            .as_object()
            .map(|object| object.len())
            .expect("each key state is a JSON object");
        assert_eq!(
            fields, 1,
            "a key-state blob in the expiry delta carries {} fields beside its key: {blob}. A \
             blob is captured AFTER the delete precisely so it carries none -- an absent field is \
             what the fold reads as a removal, and a present `expires_at_ms` would RESTORE the \
             deadline of a key this round expired",
            fields.saturating_sub(1)
        );
    }

    // 3. The anchor is at the tombstones, never past them.
    let anchor = record.applied_wal_sequence.unwrap_or(0);
    assert!(
        anchor <= wal_after,
        "the expiry delta anchors at WAL sequence {anchor} while the log ends at {wal_after}. An \
         anchor AHEAD of the records it describes suppresses their replay, which is the one \
         direction this change is not allowed to move in"
    );
    assert_eq!(
        anchor, wal_after,
        "the expiry delta anchors at WAL sequence {anchor}, not the {wal_after} the round's own \
         tombstones reached. The record that moves the anchor has to be the record that describes \
         the deletions, or a fold applies one without the other"
    );
}

/// THE FOLD ITSELF, not the record it reads: a delta-fold recovery applies the expiry
/// tombstones, and it is the only thing in that load which can.
///
/// `the_expiry_delta_names_what_the_round_removed_and_anchors_no_further` above reads the record
/// back out of the index log and checks what is IN it. Nothing then folded it. That hole was not
/// visible from `tests/delta_index_log_gc.rs` either, whose name says otherwise: mutate
/// `fold_index_log_deltas` to apply NOTHING and both of its tests stay green -- and replacing the
/// body with `panic!` leaves them green too, which is the stronger statement. The fold is not
/// merely redundant on that path, it is never CALLED there: under the single-barrier DEFAULT
/// `load_shard_with` takes `load_index_base_only`, which passes `fold_deltas = false`, and
/// reconstructs from the durable base plus a WAL replay instead. The fold is reached only from
/// `load_index_checked`, i.e. only under the `TS_WAL_LEGACY_RECOVERY` escape hatch.
///
/// So this drives `load_index_checked` -- the exact entry point that path uses -- and arranges a
/// state where the delta is the ONLY source of the answer:
///
///   * the durable base is materialized BEFORE the round and asserted BYTE-IDENTICAL after it,
///     so the checkpoint on disk still names every key the round removed;
///   * `load_index_checked` replays no WAL at all, so the tombstones cannot arrive from there;
///   * the CONTROL is the same load with the fold switched off -- `load_index_base_only` over the
///     same two files, in the same process -- and it is asserted to come back with those keys
///     STILL ALIVE, deadline and pages. That is the denominator. A fixture that stopped putting
///     the keys in the base would make the treatment below vacuous, and this fails there instead.
///
/// THE SHAPE COVERED IS #1633's, which is the shape every expiry round now writes: one record,
/// NO page items, one bare key-state blob per removed key. The removal is carried entirely by
/// "this key is in none of the thirteen per-key maps" plus the covered-key page wipe that an
/// EMPTY item list against a covered key spells. A fold that applied only page items, or only key
/// states, would leave half of it behind -- so both halves are asserted, on the same keys.
#[test]
fn a_delta_fold_recovery_applies_the_tombstones_a_stale_base_still_denies() {
    const LIVE_KEYS: usize = 64;
    const DUE_KEYS: usize = 5;

    /// Pages the index holds for one object key, across every bucket. The page half of the
    /// removal, which the key-state blobs say nothing about.
    fn blocks_for(state: &ShardState, key: &str) -> usize {
        state
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| bucket.block_index.values())
            .filter(|page| page.object_key.as_ref() == key)
            .count()
    }

    let due_key = |index: usize| format!("zzz-due-{index:04}");

    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache"), &pages, &indexes);
    engine.load_shard(1);

    let mut seed = Vec::with_capacity(LIVE_KEYS + DUE_KEYS);
    for index in 0..LIVE_KEYS {
        seed.push((format!("live-{index:08}"), 3_600_000u64));
    }
    for index in 0..DUE_KEYS {
        seed.push((due_key(index), 1u64));
    }
    write_keys(&engine, 1, seed);

    // THE STALE BASE. Materialized BEFORE the round, so the durable checkpoint names the due keys
    // as live. Everything below rests on this file not moving again.
    engine.flush_shard_index(1);
    let base_path = engine.index_path(1);
    let base_before = std::fs::read(&base_path).expect("the base index should have been written");
    let base_state = decode_index_bytes(&base_before).expect("the base index should decode");
    let base_anchor = base_state.applied_wal_sequence.unwrap_or(0);
    assert!(
        base_anchor > 0,
        "the base index carries no WAL anchor. The fold SKIPS every delta at or below the base \
         anchor only when that anchor is non-zero; at zero it folds the whole log and the \
         'suffix beyond the base' this test is about is not what is being exercised"
    );
    for index in 0..DUE_KEYS {
        let key = due_key(index);
        assert!(
            base_state.expires_at_ms.contains_key(&key),
            "the base index taken before the round does not name {key}. The base is supposed to \
             be the STALE source that still believes this key is alive -- with it already absent, \
             a fold that applies nothing would look exactly like one that works"
        );
    }

    std::thread::sleep(std::time::Duration::from_millis(20));
    let (held, due) = deadline_census(&engine, 1);
    assert_eq!(held, LIVE_KEYS + DUE_KEYS, "the fixture did not land: {held} deadlines");
    assert_eq!(due, DUE_KEYS, "{due} keys are due, expected {DUE_KEYS}");

    let records_before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the index log should read back")
        .len();
    let report = sweep_once(&engine, 1);
    assert_eq!(
        report.expired_records_removed, DUE_KEYS,
        "the round removed {} of {DUE_KEYS} due keys, so there is no removal for the fold below \
         to apply",
        report.expired_records_removed
    );

    // THE BASE DID NOT MOVE. A round that rewrote the whole index would leave the tombstones in
    // the base itself, and the fold would have nothing left to contribute.
    let base_after = std::fs::read(&base_path).expect("the base index should still exist");
    assert_eq!(
        base_before.len(),
        base_after.len(),
        "the round rewrote the base index ({} -> {} bytes). The delta is no longer the only \
         source of the removal and this test can no longer tell a working fold from a no-op one",
        base_before.len(),
        base_after.len()
    );
    assert!(
        base_before == base_after,
        "the base index changed under the round without changing size. Same conclusion: the \
         durable checkpoint is no longer the stale one this test needs"
    );

    // THE DELTA EXISTS AND CARRIES THE #1633 SHAPE. Without this the assertions after it would be
    // satisfied by an empty log just as well as by a correct one.
    let records = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the index log should read back");
    assert_eq!(
        records.len(),
        records_before + 1,
        "the round appended {} delta records, expected exactly one",
        records.len().saturating_sub(records_before)
    );
    let record = records.last().expect("the round appended a record");
    let record_anchor = record.applied_wal_sequence.unwrap_or(0);
    assert!(
        record_anchor > base_anchor,
        "the expiry delta anchors at WAL sequence {record_anchor}, at or below the base's \
         {base_anchor}. The fold skips every record at or below the base anchor, so this record \
         would never be applied and the load below would be measuring the base alone"
    );
    assert!(
        record.items.is_empty(),
        "the expiry delta carries {} page items. #1633's record describes the removal with an \
         EMPTY item list against covered keys; a record carrying items is a different shape and \
         this test no longer covers the one the expiry round writes",
        record.items.len()
    );
    let named: std::collections::BTreeSet<String> = record
        .key_states
        .iter()
        .filter_map(|blob| blob.get("key").and_then(|value| value.as_str()))
        .map(str::to_string)
        .collect();
    for index in 0..DUE_KEYS {
        let key = due_key(index);
        assert!(
            named.contains(&key),
            "the expiry delta does not name {key}. With no page items in the record, the blobs \
             are the ONLY thing that makes a key covered, so an unnamed key is one the fold \
             cannot touch however correct the fold is. Named: {named:?}"
        );
    }

    // A second engine over the SAME pages and indexes, so both arms below read one set of files.
    let reader = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-control"),
        &pages,
        &indexes,
    );

    // CONTROL: the same load with the fold switched off. No WAL replay on this path either, so
    // this is the base and nothing else -- and the base still believes the due keys are alive.
    let control = reader
        .load_index_base_only(1, false)
        .expect("the base index should load");
    let mut control_blocks = 0usize;
    for index in 0..DUE_KEYS {
        let key = due_key(index);
        assert!(
            control.expires_at_ms.contains_key(&key),
            "CONTROL: a load that does not fold the delta came back WITHOUT {key}'s deadline. \
             Something other than the fold is already removing it, so the treatment below proves \
             nothing -- it would pass with the fold deleted"
        );
        control_blocks += blocks_for(&control, &key);
    }
    assert!(
        control_blocks > 0,
        "CONTROL: the unfolded base holds no pages at all for the {DUE_KEYS} removed keys, so the \
         page half of the fold has nothing to wipe and asserting it wiped them is vacuous"
    );
    assert_eq!(
        control.expires_at_ms.len(),
        LIVE_KEYS + DUE_KEYS,
        "CONTROL: the unfolded base holds {} deadlines, expected all {} the fixture wrote",
        control.expires_at_ms.len(),
        LIVE_KEYS + DUE_KEYS
    );
    assert_eq!(
        control.applied_wal_sequence,
        Some(base_anchor),
        "CONTROL: the unfolded base anchors at {:?}, not the {base_anchor} it was written with",
        control.applied_wal_sequence
    );

    // TREATMENT: the same two files, folded. Everything that differs from the control is the
    // fold's work and nothing else's.
    let folded = reader
        .load_index_checked(1, false)
        .expect("the delta log is intact, so the checked load must not refuse it")
        .expect("the base index should load");

    println!(
        "  stale base at anchor {base_anchor} ({} bytes, unchanged by the round), one delta at \
         anchor {record_anchor} with {} items and {} key blobs: unfolded holds {} deadlines and \
         {control_blocks} pages for the removed keys, folded holds {}",
        base_before.len(),
        record.items.len(),
        record.key_states.len(),
        control.expires_at_ms.len(),
        folded.expires_at_ms.len(),
    );

    for index in 0..DUE_KEYS {
        let key = due_key(index);
        assert!(
            !folded.expires_at_ms.contains_key(&key),
            "the fold left {key} holding the deadline the round cleared. The delta's blob for it \
             is bare -- no `expires_at_ms` field -- which `apply_key_state_field` is supposed to \
             read as a removal, and the stale base is the only other source in this load. An \
             expired key comes back with a deadline in the past, which is a key a later round \
             finds due all over again"
        );
        assert_eq!(
            blocks_for(&folded, &key),
            0,
            "the fold left {} page(s) of {key} attached. The record carries NO items, and an \
             empty item list against a covered key is how the removal is spelled: every page of \
             every covered key is wiped and only the carried items are restored. Pages left \
             behind are pages the deletes already retained -- dangling entries pointing into \
             reclaimable slabs",
            blocks_for(&folded, &key)
        );
    }
    assert_eq!(
        folded.expires_at_ms.len(),
        LIVE_KEYS,
        "the folded index holds {} deadlines, expected exactly the {LIVE_KEYS} live ones. The \
         unfolded control holds {} -- a folded count equal to the control's is a fold that \
         applied nothing",
        folded.expires_at_ms.len(),
        control.expires_at_ms.len()
    );
    for index in [0usize, LIVE_KEYS / 2, LIVE_KEYS - 1] {
        let key = format!("live-{index:08}");
        assert!(
            folded.expires_at_ms.contains_key(&key),
            "the fold removed {key}, which the round did not touch. The covered-key wipe reaches \
             every key the blobs name, so a delta naming more than it removed deletes live data \
             on recovery"
        );
        assert!(
            blocks_for(&folded, &key) > 0,
            "the fold left {key} with no pages while keeping its deadline -- a key that reads as \
             present and answers nothing"
        );
    }
    assert_eq!(
        folded.applied_wal_sequence,
        Some(record_anchor),
        "the fold reconstructed anchor {:?}, not the delta's {record_anchor}. The anchor advances \
         only when a record was actually applied, so this is the same failure counted a second \
         way -- and an anchor left at the base's would make a legacy-recovery load replay the \
         round's own tombstones again",
        folded.applied_wal_sequence
    );
}

/// THE INVARIANT: WAL reclaim never frees a record the DEFAULT load path still has to replay.
///
/// WHY THIS NEEDED ASKING. Three merged changes put a shape on disk that looks unrecoverable:
///
///   * #1644 established that the default load path folds NO index-log deltas. Under the
///     single-barrier default `load_shard_with` takes `load_index_base_only`, so recovery is a
///     durable checkpoint plus a WAL replay of everything past that checkpoint's anchor.
///   * #1633 made the expiry round stop writing a whole index. It appends a DELTA carrying
///     `applied_wal_sequence` and the removed keys' tombstones, and leaves the base file alone.
///   * #1622 made WAL reclaim actually drop whole segment FILES.
///
/// Put together: the delta advances the served anchor to N while the base index file on disk
/// still carries M, M < N. If the reclaim floor came from N, the records in (M, N] could be
/// dropped, and a load that starts at M could not reconstruct them.
///
/// MEASURED, and the two anchors DO diverge -- this is not a hypothetical state. Driving the
/// production cycle on this fixture: immediately after the expiry cycle the base index file still
/// read `applied_wal_sequence = 1` while the highest delta record read 9, the reclaim frontier
/// was 9, and the log had been cut to a single record at sequence 9. Everything in (1, 9] -- the
/// expiry tombstones included -- was gone from the log with the base file still anchored at 1.
///
/// IT IS STILL NOT A LOSS, AND THE REASON IS THE THING THIS TEST PINS. The base index file is not
/// the only durable checkpoint the load path reads. `load_shard_with` raises its replay point to
/// the latest bucket dump manifest's `wal_sequence` when that manifest is newer than the base,
/// and reclaim's ceiling -- `durable_bucket_generation_frontier_wal_sequence` -- is a MINIMUM over
/// those same bucket dump manifests. A minimum over a set cannot exceed a member of it, so the
/// floor cannot climb above the point the load starts replaying from. In the measured state above
/// both sides were 9: the dump that minted the frontier is the same dump the load recovers from.
///
/// That is a load-bearing agreement between two independently-maintained expressions, and nothing
/// asserted it. Either side can move on its own:
///
///   * the plan has a branch (`durable_wal_frontier == u64::MAX` -> `current_wal_sequence`) that
///     mints the frontier from the CURRENT log position rather than from any manifest. It exists
///     so an all-clean shard is not read as "retain everything", and it is the one place the
///     frontier comes from something no load path consults. It is safe today only because a cycle
///     DUMPS (stage `prepare`) before it RECLAIMS (stage `reclaim_wal`), so a manifest at the
///     current position already exists by the time that branch is reached;
///   * the load path could stop consulting manifests, or consult a different one.
///
/// So this asserts the RELATION, at every state a production cycle passes through, rather than
/// either number.
///
/// AND THE SECOND OF THOSE WAS REAL. The load path used to read
/// `latest_bucket_dump_manifest_at`, which orders by `index_log_sequence` -- a MEMBER of the set
/// the floor minimises over, but not an UPPER BOUND on it. The two sequences are minted from two
/// different places and nothing couples them: `index_log_sequence` is the live index-log tail,
/// while `wal_sequence` is the anchor inside the index bytes the dump embeds, which under
/// `MATRIXARK_BULK_INGEST` comes from the FROZEN BASE FILE. One env flag on one shard in one
/// process inverts them, and the last section below builds exactly that state: a manifest that is
/// newest in index-log order carrying the LOWEST WAL anchor on disk. Before
/// `durable_recovery_bucket_dump_manifest_at` (a MAXIMUM over `wal_sequence`, which bounds every
/// subset by construction) that state measured
///
/// ```text
///     reclaim floor 9 (retain_from 10), a real load off the same files replaying from 1
/// ```
///
/// -- sequences (1, 9], the expiry tombstones among them, both reclaimable and required, and the
/// probe load came back with NO SHARD AT ALL. That is the live unrecoverable-loss path this now
/// holds shut, and it is asserted here rather than in a test of its own so the divergent state
/// runs against the same relation, the same real-load probe and the same denominators as every
/// other state.
///
/// IT READS THE REPLAY POINT OFF A REAL LOAD, NOT OFF A COPY OF THE RULE. The first version of
/// this test recomputed `max(base anchor, latest manifest wal_sequence)` itself. That version
/// PASSED with `load_shard_with`'s manifest arm disabled -- the mutation moved the subject and
/// the test went on reading its own reimplementation of it, which is the whole failure mode the
/// relation is here to catch. Every observation below instead copies the durable files, loads
/// them with a fresh engine, and reads `LAST_REPLAY_WATERMARK`: the number `load_shard_with`
/// actually chose, on the path it actually took.
///
/// THE DENOMINATOR. An invariant `a <= b` is worth nothing if `a` and `b` were never allowed to
/// differ in the fixture. This counts the states in which the base index file's anchor sits
/// strictly BELOW the reclaim frontier -- the two-anchor condition the whole worry rests on --
/// and fails if that count is zero, because then the base alone would have covered every reclaim
/// and the manifest that actually carries it would never have been exercised.
///
/// VERIFIED BY MUTATION, twice. Making `load_shard_with`'s single-barrier arm ignore the dump
/// manifest (`Some(manifest) if false && ...`) fails this at the `expired` state with
/// `reclaim floor 10 is above what the default load path replays from (1)`. Putting that arm back
/// on `latest_bucket_dump_manifest_at` -- the index-log order it used before -- fails it at the
/// `manifest orderings diverged` state with `reclaim floor 10 is above ... (1)`.
#[test]
fn wal_reclaim_never_frees_what_the_default_load_path_replays() {
    const PRE_KEYS: usize = 8;
    const DUE_KEYS: usize = 4;
    /// Poison value for `LAST_REPLAY_WATERMARK`. A load that recorded nothing must not be read as
    /// a load that chose zero -- zero is "replay the whole retained log", the most permissive
    /// answer there is, and reading a stale or absent value as that would make every assertion
    /// below pass for the wrong reason.
    const NO_LOAD_RECORDED: u64 = u64::MAX;

    let pre_key = |index: usize| format!("pre-{index:04}");
    let due_key = |index: usize| format!("zzz-due-{index:04}");

    // THE DEFAULT RECOVERY PATH IS THE ONE THIS TEST IS ABOUT (#1644's open point). Everything
    // here reasons about a durable checkpoint plus WAL replay; under `TS_WAL_LEGACY_RECOVERY`
    // recovery folds the deltas instead, and the anchors do not diverge in the same way. No env is
    // set here -- the point is what an unconfigured process does -- so a change that flips the
    // default trips this rather than silently re-pointing recovery at the fold path, whose only
    // coverage is one lib test.
    assert!(
        crate::engine::wal_single_barrier(),
        "the default recovery path is no longer single-barrier base-only. This test's claim -- \
         that reclaim cannot outrun a replay that folds no deltas -- is about that path"
    );

    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        if !from.exists() {
            return;
        }
        std::fs::create_dir_all(to).expect("create the copy target");
        for entry in std::fs::read_dir(from).expect("read the durable tree") {
            let entry = entry.expect("read a durable entry");
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).expect("copy a durable file");
            }
        }
    }

    /// What `load_shard_with` ACTUALLY replays from, given the durable files as they stand.
    ///
    /// A copy, so the probe cannot disturb the shard the cycle is still driving, and a fresh
    /// engine, so this is a cold load and not a cache read.
    fn default_load_replay_point(
        pages: &std::path::Path,
        indexes: &std::path::Path,
        scratch: &std::path::Path,
        label: &str,
    ) -> u64 {
        let root = scratch.join(label.replace(' ', "-"));
        copy_tree(pages, &root.join("pages"));
        copy_tree(indexes, &root.join("indexes"));
        crate::engine::lifecycle::LAST_REPLAY_WATERMARK
            .store(NO_LOAD_RECORDED, std::sync::atomic::Ordering::SeqCst);
        let reader = TemporalEngine::with_local_dirs(
            1 << 20,
            root.join("cache"),
            root.join("pages"),
            root.join("indexes"),
        );
        reader.load_shard(1);
        let watermark = crate::engine::lifecycle::LAST_REPLAY_WATERMARK
            .load(std::sync::atomic::Ordering::SeqCst);
        assert_ne!(
            watermark, NO_LOAD_RECORDED,
            "[{label}] the probe load recorded no replay watermark, so the number this test \
             compares the reclaim floor against would be whatever a previous load left behind"
        );
        watermark
    }

    fn base_index_anchor(engine: &TemporalEngine) -> u64 {
        std::fs::read(engine.index_path(1))
            .ok()
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| decode_index_bytes(&bytes).ok())
            .and_then(|state| state.applied_wal_sequence)
            .unwrap_or(0)
    }

    fn cycle(engine: &TemporalEngine) -> StorageManagerCycleReport {
        engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            load_cold_buckets_for_expire: true,
            ..StorageManagerCycleRequest::default()
        })
    }

    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let probes = dir.path().join("probes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache"), &pages, &indexes);
    engine.load_shard(1);

    let mut seed = Vec::new();
    for index in 0..PRE_KEYS {
        seed.push((pre_key(index), 3_600_000u64));
    }
    for index in 0..DUE_KEYS {
        seed.push((due_key(index), 3_600_000u64));
    }
    write_keys(&engine, 1, seed);
    engine.flush_shard_index(1);

    /// (the WAL anchor of the manifest that is newest in INDEX-LOG order, the highest WAL anchor
    /// over every manifest on disk). Equal on a shard whose two manifest orderings agree; the
    /// first below the second is the inversion the last section builds.
    fn manifest_orderings(indexes: &std::path::Path) -> (u64, u64) {
        let manifests = crate::engine::bucket_dump_io::list_bucket_dump_manifests_at(indexes, 1)
            .expect("the manifest listing reads back");
        (
            manifests
                .last()
                .map(|manifest| manifest.wal_sequence)
                .unwrap_or(0),
            manifests
                .iter()
                .map(|manifest| manifest.wal_sequence)
                .max()
                .unwrap_or(0),
        )
    }

    let mut observations = 0usize;
    let mut safe_states = 0usize;
    let mut diverged_states = 0usize;
    let mut inverted_ordering_states = 0usize;

    let mut observe = |engine: &TemporalEngine, label: &str| {
        observations += 1;
        let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        let base_anchor = base_index_anchor(engine);
        let (latest_by_index_log, highest_anchor) = manifest_orderings(&indexes);
        inverted_ordering_states += usize::from(latest_by_index_log < highest_anchor);
        let load_from = default_load_replay_point(&pages, &indexes, &probes, label);
        if !plan.safe_to_reclaim {
            // A plan that refuses reclaims nothing and so cannot outrun anything. Printed anyway,
            // so a fixture that stopped reaching a reclaiming plan shows up as such below instead
            // of passing as a run in which the floor never misbehaved.
            println!(
                "  [{label}] plan declines ({:?}); base index anchor {base_anchor}, a real load \
                 replays from {load_from}",
                plan.blocker_reasons
            );
            return;
        }
        safe_states += 1;
        let frontier = plan.durable_bucket_generation_frontier_wal_sequence;
        diverged_states += usize::from(base_anchor < frontier);
        println!(
            "  [{label}] base index anchor {base_anchor}, a real load replays from {load_from}, \
             reclaim frontier {frontier} (retain_from {}); manifests: newest in index-log order \
             anchors at {latest_by_index_log}, highest anchor on disk {highest_anchor}",
            plan.retain_from_wal_sequence
        );
        assert!(
            frontier <= load_from,
            "[{label}] reclaim floor {} is above what the default load path replays from \
             ({load_from}). Records at sequences ({load_from}, {frontier}] are both reclaimable \
             and required: reclaim may drop them, and a base-only load has to replay them to \
             rebuild the state they carry. The base index file anchors at {base_anchor}; the \
             floor has to stay at or below the replay point, because a durable checkpoint the \
             load does not read cannot authorise dropping the log that stands in for it. The \
             manifest that is newest in INDEX-LOG order anchors at {latest_by_index_log} and the \
             highest anchor on disk is {highest_anchor}: if those two differ, the load recovered \
             from a manifest that does not bound the minimum the floor is taken over.",
            plan.retain_from_wal_sequence,
        );
    };

    observe(&engine, "seeded");
    // Dump rounds. Nothing is due yet, so `expire` is inert and these only produce manifests.
    cycle(&engine);
    observe(&engine, "dumped");
    cycle(&engine);
    observe(&engine, "dumped again");

    // Bring the deadlines forward, then let ONE cycle expire them. `reclaim_wal` runs BEFORE
    // `expire` within a cycle, so this cycle writes the tombstones and the NEXT one is the first
    // that may reclaim them -- which is the window the whole worry is about.
    for index in 0..DUE_KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: due_key(index),
                ttl_ms: 1,
            },
        });
        assert!(response.status.ok, "could not bring {} forward", due_key(index));
    }
    std::thread::sleep(std::time::Duration::from_millis(30));

    let wal_before_expiry = engine.wal_store().stats(1).last_sequence;
    let expire_cycle = cycle(&engine);
    let expired = expire_cycle
        .stages
        .iter()
        .map(|stage| stage.expired_records_removed)
        .sum::<usize>();
    let wal_after_expiry = engine.wal_store().stats(1).last_sequence;
    let delta_anchor = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the index log reads back")
        .iter()
        .filter_map(|record| record.applied_wal_sequence)
        .max()
        .unwrap_or(0);
    let base_after_expiry = base_index_anchor(&engine);

    // DENOMINATOR for everything below: the expiry round has to have happened, and it has to have
    // put the two anchors apart. Without both, the rest of this test asserts nothing.
    assert_eq!(
        expired, DUE_KEYS,
        "the expiry cycle removed {expired} of {DUE_KEYS} keys, so there are no tombstones in \
         the log for a reclaim to be able to drop"
    );
    assert!(
        wal_after_expiry > wal_before_expiry,
        "the expiry cycle appended no WAL record (log still at {wal_before_expiry}). The \
         tombstones are what a reclaim would be dropping, and with none written the reclaim \
         below has nothing to get wrong"
    );
    assert!(
        delta_anchor > base_after_expiry,
        "the delta anchor {delta_anchor} is not ahead of the base index file's \
         {base_after_expiry}. The two-anchor state this test exists for did not arise, so a \
         reclaim floor taken from the delta would be indistinguishable from one taken from the \
         base"
    );
    println!(
        "  expiry cycle: {expired} keys removed, WAL {wal_before_expiry} -> {wal_after_expiry}, \
         delta anchor {delta_anchor} against base index file anchor {base_after_expiry}"
    );

    observe(&engine, "expired");

    // THE TWO MANIFEST ORDERINGS, PULLED APART. Here, and not after the cycles below, because the
    // state it needs is the one the expiry round just produced: the base index FILE anchored at
    // {base_after_expiry} while the served index is at {delta_anchor}. The next cycle's reclaim
    // materialises the base and closes that distance -- measured, the base reads 9 from
    // `reclaimed after expiry` onward -- and a dump minted then reads the same anchor either way.
    //
    // Everything above rests on the load path recovering from a manifest that BOUNDS the minimum
    // the reclaim floor is taken over. Ordering by `index_log_sequence` gives a member of that
    // set, not a bound on it, and the two orderings are not coupled: `index_log_sequence` is the
    // live index-log tail, while `wal_sequence` is the anchor inside the index bytes the dump
    // embeds -- and `load_served_index_bytes` reads the FROZEN BASE FILE under bulk ingest rather
    // than the live index. After the expiry round above the base file sits far behind the served
    // anchor (asserted as the `delta_anchor > base_after_expiry` denominator), so ONE dump minted
    // with the flag set lands newest in index-log order carrying the lowest anchor on disk.
    //
    // This is a production mint -- `create_bucket_dump_manifest`, the only place a manifest's two
    // sequences are ever assigned -- under a flag the engine reads live on every call. Nothing is
    // fabricated and no file is edited: the state below is one a running node reaches by being
    // restarted with `MATRIXARK_BULK_INGEST` set, which is what that flag is for.
    let (_, anchor_before_bulk_dump) = manifest_orderings(&indexes);
    std::env::set_var("MATRIXARK_BULK_INGEST", "1");
    let bulk_manifest = engine.create_bucket_dump_manifest(1, Vec::new());
    std::env::remove_var("MATRIXARK_BULK_INGEST");
    let bulk_manifest = bulk_manifest.expect("the bulk-ingest dump persists");
    let (latest_by_index_log, highest_anchor) = manifest_orderings(&indexes);

    // DENOMINATOR FOR THE SECTION, before it asserts anything. Two separate things have to be
    // true, and each on its own would let the observation below run against an ordinary state:
    // the bulk dump has to have become the NEWEST manifest in index-log order, and it has to
    // carry an anchor STRICTLY BELOW one already on disk. A future `create_bucket_dump_manifest`
    // that couples the two sequences, or a fixture whose base file stopped lagging the served
    // index, breaks one of them and says so here rather than passing in silence.
    assert_eq!(
        latest_by_index_log, bulk_manifest.wal_sequence,
        "the bulk-ingest dump (index_log_sequence {}, anchor {}) is not the newest manifest in \
         index-log order -- the newest one anchors at {latest_by_index_log} -- so the load path \
         would not be reading it and this section tests nothing",
        bulk_manifest.index_log_sequence, bulk_manifest.wal_sequence,
    );
    assert!(
        bulk_manifest.wal_sequence < anchor_before_bulk_dump
            && highest_anchor == anchor_before_bulk_dump,
        "the bulk-ingest dump anchors at {} against a highest anchor on disk of \
         {anchor_before_bulk_dump} before it and {highest_anchor} after. The two orderings did \
         not come apart, so recovering from the newest manifest and recovering from the \
         highest-anchored one are the same thing here and the relation below cannot tell them \
         apart",
        bulk_manifest.wal_sequence,
    );
    println!(
        "  bulk-ingest dump: index_log_sequence {} (newest on disk) carrying WAL anchor {} \
         against a highest anchor of {highest_anchor}",
        bulk_manifest.index_log_sequence, bulk_manifest.wal_sequence
    );

    // The same relation, the same real-load probe, against the inverted state. Ordering by
    // `index_log_sequence` here measured floor 9 (retain_from 10) against a load replaying from 1.
    observe(&engine, "manifest orderings diverged");

    // ...and then the rest of the cycle the expiry round was in the middle of, so the states the
    // original run covered are still covered, now with the divergent manifest on disk.
    cycle(&engine);
    observe(&engine, "reclaimed after expiry");
    cycle(&engine);
    observe(&engine, "settled");

    // VACUITY GUARD, on the scan rather than on any one state. If the base index file's anchor
    // had tracked the frontier the whole way, the base alone would have covered every reclaim and
    // the manifest that actually carries it -- the mechanism this test is named for -- would never
    // have been the thing keeping the relation true.
    assert!(
        diverged_states > 0,
        "in {observations} observed states ({safe_states} of them with a plan that would \
         reclaim), the base index file's anchor was NEVER below the reclaim frontier. The \
         durable base covered every reclaim on its own, so this run never exercised the state \
         the test exists for and could not have failed"
    );
    assert!(
        safe_states > 0,
        "in {observations} observed states the plan never once reached `safe_to_reclaim`, so no \
         floor was ever applied to anything and the relation above never ran against a live \
         reclaim"
    );
    // THE SECOND VACUITY GUARD, for the second mechanism. The relation can be kept by a load that
    // reads the newest manifest OR by one that reads the highest-anchored manifest, and the two
    // are only told apart in a state where those are different manifests. Zero such states means
    // the run never distinguished them.
    assert!(
        inverted_ordering_states > 0,
        "in {observations} observed states the manifest newest in INDEX-LOG order was never the \
         one carrying the lowest WAL anchor, so every state could have been kept safe by reading \
         either ordering and this run says nothing about which one the load path has to use"
    );
    println!(
        "  {observations} states observed, {safe_states} with a reclaiming plan, \
         {diverged_states} with the base index file anchored BELOW the reclaim frontier, \
         {inverted_ordering_states} with the two manifest orderings inverted"
    );

    // AND THE RECORDS SURVIVED IT. The relation holding is the mechanism; this is the outcome,
    // read off a fresh process over the same durable files by the default load path.
    //
    // Counted SEPARATELY. The two halves fail for different reasons -- a live key missing is a
    // reclaim that outran the replay point, an expired key present is a tombstone reclaimed
    // before any durable checkpoint the load reads described the deletion -- and a combined
    // total would let one hide the other.
    crate::engine::lifecycle::LAST_REPLAY_WATERMARK
        .store(NO_LOAD_RECORDED, std::sync::atomic::Ordering::SeqCst);
    let reader = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-reader"),
        &pages,
        &indexes,
    );
    reader.load_shard(1);
    let replayed_from = crate::engine::lifecycle::LAST_REPLAY_WATERMARK
        .load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(
        replayed_from, NO_LOAD_RECORDED,
        "the final load recorded no replay watermark"
    );
    let shards = reader.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("the shard loaded");
    let live_back = (0..PRE_KEYS)
        .filter(|index| shard.strings.contains_key(&pre_key(*index)))
        .count();
    let expired_back = (0..DUE_KEYS)
        .filter(|index| shard.strings.contains_key(&due_key(*index)))
        .count();
    println!("  fresh default load replayed from {replayed_from}");
    assert_eq!(
        live_back, PRE_KEYS,
        "a fresh default load came back with {live_back} of {PRE_KEYS} never-expired keys. It \
         replayed from {replayed_from}; the missing ones are in neither the durable checkpoint it \
         started from nor the retained log -- a reclaim freed what this load needed"
    );
    assert_eq!(
        expired_back, 0,
        "a fresh default load came back with {expired_back} of {DUE_KEYS} EXPIRED keys alive. It \
         replayed from {replayed_from}, and the tombstones that removed them sat below that \
         point: reclaim dropped them while the durable checkpoint the load starts from did not \
         yet describe the deletion, so the keys resurrect with a deadline already in the past"
    );
}

/// A deadline set through the two `WithOptions` control-state writes must be VISIBLE TO THE SWEEP.
///
/// WHAT WENT WRONG. `expires_at_ms` is the key-ordered map of deadlines; `expiry_by_deadline` is
/// the deadline-ordered mirror the sweep reads, and `due_window` reads ONLY the mirror. The two
/// are kept in step by `set_expiry` / `clear_expiry`. Two arms --
/// `ControlStateIncrementWithOptions` and `ControlStateSetAndGetWithOptions` -- wrote
/// `shard.expires_at_ms.insert(...)` directly instead. Their own siblings a few lines away
/// (`ControlStateChangeAdd`, `ControlStateSet`) call `set_expiry`, so the two spellings of the
/// same request disagreed about whether the key would ever be collected.
///
/// WHY THE REPAIR DOES NOT COVER IT. `ensure_expiry_order` rebuilds the mirror, but only when the
/// mirror is ENTIRELY EMPTY -- that is its contract, because a partially-populated mirror is
/// indistinguishable from a correct one. So the moment any OTHER key on the shard holds a
/// deadline, the mirror is non-empty, the repair is a no-op, and the bypassed key is invisible to
/// every sweep for the life of the shard. It expires only if a command happens to touch it and
/// trip lazy expiry. A caller who asked for a one-second TTL got a key retained forever.
///
/// THE DENOMINATOR THIS TEST ASSERTS FIRST, because without it the test is vacuous: the mirror
/// must be NON-EMPTY before the bypassed write happens. On an empty mirror `ensure_expiry_order`
/// repairs the damage and the defect cannot be reproduced at all.
///
/// HALVES ARE ASSERTED SEPARATELY. The two arms are checked one at a time, each with its own
/// "the deadline was recorded at all" assertion before its "and the sweep can see it" assertion.
/// A combined count would read full from one arm while the other read zero.
#[test]
fn a_deadline_set_with_options_is_visible_to_the_sweep() {
    fn deadline_is_recorded(engine: &TemporalEngine, shard_id: ShardId, key: &str) -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id).expect("shard is loaded");
        shard.expires_at_ms.contains_key(key)
    }
    fn deadline_is_in_the_mirror(engine: &TemporalEngine, shard_id: ShardId, key: &str) -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id).expect("shard is loaded");
        shard
            .expiry_by_deadline
            .keys()
            .any(|(_, mirrored)| mirrored == key)
    }
    fn record_is_gone(engine: &TemporalEngine, shard_id: ShardId, key: &str) -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id).expect("shard is loaded");
        !shard.control_state.contains_key(key) && !shard.expires_at_ms.contains_key(key)
    }

    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // DENOMINATOR. Other keys hold deadlines, so the mirror is NON-EMPTY and
    // `ensure_expiry_order` will not silently repair what the arms below do. Long deadlines, so
    // these keys are never themselves due and cannot be mistaken for the removal being claimed.
    write_keys(
        &engine,
        1,
        (0..64)
            .map(|index| (format!("bystander:{index:04}"), 3_600_000))
            .collect(),
    );
    let (bystanders_held, bystanders_due) = deadline_census(&engine, 1);
    assert_eq!(
        bystanders_held, 64,
        "the bystander deadlines must actually exist, else the mirror is empty and the repair \
         hides the defect this test is about",
    );
    assert_eq!(bystanders_due, 0, "no bystander may be due");
    assert_eq!(
        deadline_index_len(&engine, 1),
        64,
        "the deadline-ordered mirror must be NON-EMPTY before the bypassed writes happen",
    );
    assert_eq!(disagreements(&engine, 1), 0, "the two indexes start in step");

    // ARM 1: ControlStateIncrementWithOptions. Its own half, asserted alone.
    let increment = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateIncrementWithOptions {
            key: "with_options:increment".to_string(),
            timestamp_ms: 1_000,
            amount: 1,
            precision_ms: None,
            ttl_ms: Some(1),
        },
    });
    assert!(increment.status.ok, "seed write failed: {:?}", increment.status);
    assert!(
        deadline_is_recorded(&engine, 1, "with_options:increment"),
        "DENOMINATOR: the TTL must have been recorded at all before asking whether the sweep \
         can see it",
    );
    assert!(
        deadline_is_in_the_mirror(&engine, 1, "with_options:increment"),
        "ControlStateIncrementWithOptions recorded a deadline the sweep's deadline-ordered view \
         never learned about, so the key can never be collected",
    );

    // ARM 2: ControlStateSetAndGetWithOptions. Its own half, asserted alone. The key it writes
    // is family-prefixed, so the deadline lands on `control_state:h:<key>`.
    let set_and_get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateSetAndGetWithOptions {
            family: ControlStateFamily::Counter,
            key: "with_options:setandget".to_string(),
            timestamp_ms: 1_000,
            amount: 1,
            start_ms: 0,
            end_ms: 2_000,
            aggregator: "sum".to_string(),
            precision_ms: None,
            ttl_ms: Some(1),
            uuid: None,
        },
    });
    assert!(
        set_and_get.status.ok,
        "seed write failed: {:?}",
        set_and_get.status
    );
    let family_key = "control_state:h:with_options:setandget";
    assert!(
        deadline_is_recorded(&engine, 1, family_key),
        "DENOMINATOR: the TTL must have been recorded at all before asking whether the sweep \
         can see it",
    );
    assert!(
        deadline_is_in_the_mirror(&engine, 1, family_key),
        "ControlStateSetAndGetWithOptions recorded a deadline the sweep's deadline-ordered view \
         never learned about, so the key can never be collected",
    );

    // The detector that exists for exactly this class of mistake must read zero.
    assert_eq!(
        disagreements(&engine, 1),
        0,
        "the two expiry indexes must agree after both WithOptions writes",
    );

    // AND THE SWEEP MUST ACTUALLY COLLECT THEM. A 1 ms TTL is long past by now; the bystanders
    // are an hour out, so anything the round removes is one of the two keys under test.
    std::thread::sleep(std::time::Duration::from_millis(5));
    let (held_before, due_before) = deadline_census(&engine, 1);
    assert_eq!(
        held_before, 66,
        "64 bystanders plus the two keys under test must all hold deadlines",
    );
    assert_eq!(
        due_before, 2,
        "exactly the two keys under test are due -- this is the round's denominator",
    );
    let report = sweep_once(&engine, 1);
    assert_eq!(
        report.expired_records_removed, 2,
        "one round must collect BOTH due keys behind 64 live ones \
         (scanned {}, skipped {})",
        report.scanned_records, report.skipped_records,
    );
    // Each removal asserted separately, so one arm reading full cannot hide the other at zero.
    assert!(
        record_is_gone(&engine, 1, "with_options:increment"),
        "the ControlStateIncrementWithOptions key survived its own deadline",
    );
    assert!(
        record_is_gone(&engine, 1, family_key),
        "the ControlStateSetAndGetWithOptions key survived its own deadline",
    );
    let (held_after, due_after) = deadline_census(&engine, 1);
    assert_eq!(held_after, 64, "only the bystanders remain");
    assert_eq!(due_after, 0, "nothing is left due");
    assert_eq!(disagreements(&engine, 1), 0, "the indexes are still in step");
}

/// The same two writes, at the sizes #1624 measured, so a semantics change cannot quietly make a
/// round cost the keyspace again.
///
/// The claim is about COUNTS, not wall clock: at 2,000 / 20,000 / 100,000 live keys holding
/// deadlines, a round that collects the two due keys must LOOK AT a number of records that does
/// not grow with the keyspace. The due keys sort AFTER every bystander, which is the arrangement
/// a key-ordered scan is worst at.
#[test]
fn a_with_options_deadline_costs_the_due_set_not_the_keyspace() {
    fn round_at(live_keys: usize) -> (usize, usize, usize) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        write_keys(
            &engine,
            1,
            (0..live_keys)
                .map(|index| (format!("aaa:live:{index:08}"), 3_600_000))
                .collect(),
        );
        assert_eq!(
            deadline_census(&engine, 1).0,
            live_keys,
            "DENOMINATOR: every live key must hold a deadline",
        );
        // Sorts after every `aaa:` bystander, so a key-ordered scan reaches it last.
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateIncrementWithOptions {
                key: "zzz:with_options:due".to_string(),
                timestamp_ms: 1_000,
                amount: 1,
                precision_ms: None,
                ttl_ms: Some(1),
            },
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let (held, due) = deadline_census(&engine, 1);
        assert_eq!(held, live_keys + 1, "denominator: deadlines held");
        assert_eq!(due, 1, "denominator: exactly one key is due");
        let report = sweep_once(&engine, 1);
        (
            report.expired_records_removed,
            report.scanned_records,
            report.skipped_records,
        )
    }

    let (removed_small, scanned_small, skipped_small) = round_at(2_000);
    let (removed_mid, scanned_mid, skipped_mid) = round_at(20_000);
    let (removed_large, scanned_large, skipped_large) = round_at(100_000);
    println!(
        "  live keys 2,000: removed {removed_small}, looked at {scanned_small}, skipped {skipped_small}"
    );
    println!(
        "  live keys 20,000: removed {removed_mid}, looked at {scanned_mid}, skipped {skipped_mid}"
    );
    println!(
        "  live keys 100,000: removed {removed_large}, looked at {scanned_large}, skipped {skipped_large}"
    );

    // Removal asserted separately at each size: a combined count would let one size read full
    // while another read zero.
    assert_eq!(removed_small, 1, "2,000 live keys: the due key must be collected in one round");
    assert_eq!(removed_mid, 1, "20,000 live keys: the due key must be collected in one round");
    assert_eq!(removed_large, 1, "100,000 live keys: the due key must be collected in one round");

    // And the COST of finding it does not grow with the keyspace: a 50x keyspace must not make
    // the round look at more records.
    assert!(
        scanned_large <= scanned_small.max(8),
        "a round at 100,000 live keys looked at {scanned_large} records versus {scanned_small} \
         at 2,000 -- the round is paying for the keyspace again \
         (skipped {skipped_small}/{skipped_mid}/{skipped_large})",
    );
    assert!(
        scanned_mid <= scanned_small.max(8),
        "a round at 20,000 live keys looked at {scanned_mid} records versus {scanned_small} at \
         2,000",
    );
}

/// THE OTHER HALF: a deadline set through the two `WithOptions` control-state writes must also
/// SURVIVE RECOVERY. Asserted in its own test, separately, because the in-memory half and this
/// one fail independently and a single combined check would read full from one and zero from the
/// other.
///
/// WHY IT IS A SEPARATE FAILURE. A WAL record that carries OUTCOMES is INSTALLED, not re-executed
/// -- replay applies the recorded outcomes and `continue`s past the command. Both arms write a
/// control-state page, and the page write stages an outcome, so their records are never empty and
/// their commands are never re-run on recovery. Neither arm staged a meta outcome carrying its
/// deadline. So the page came back and the deadline did not: after any recovery the key was
/// restored with NO deadline at all, permanently, for a caller who asked for one.
///
/// DENOMINATORS, asserted before the claim: the deadline existed before the unload, the base
/// index was really removed so the load can only be a log replay, and the control-state VALUE
/// really came back -- which proves replay ran and installed something, so a missing deadline
/// cannot be explained by "nothing was replayed".
#[test]
fn a_with_options_deadline_survives_recovery() {
    fn deadline_for(engine: &TemporalEngine, shard_id: ShardId, key: &str) -> Option<u64> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id).expect("shard is loaded");
        shard.expires_at_ms.get(key).copied()
    }
    /// Did replay INSTALL this record's outcome? That is the premise of the whole test -- a
    /// record with outcomes is installed rather than re-executed -- so the page the write
    /// produced is the honest denominator. Deliberately NOT `shard.control_state`: that map is
    /// rebuilt from the bucket index by a mechanism with its own separate, pre-existing defect,
    /// and reading it here would make this test fail for a reason that has nothing to do with
    /// deadlines.
    fn outcome_was_installed(engine: &TemporalEngine, shard_id: ShardId, key: &str) -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id).expect("shard is loaded");
        shard.control_state_blocks.contains_key(key)
    }

    // Long enough that nothing under test expires during the round trip.
    const TTL_MS: u64 = 3_600_000;
    const INCREMENT_KEY: &str = "with_options:increment";
    const FAMILY_KEY: &str = "control_state:h:with_options:setandget";

    let dir = tempfile::tempdir().unwrap();
    let index_dir = dir.path().join("indexes");
    let make_engine = || {
        TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            index_dir.clone(),
        )
    };

    let before = {
        let engine = make_engine();
        engine.load_shard(1);
        let increment = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateIncrementWithOptions {
                key: INCREMENT_KEY.to_string(),
                timestamp_ms: 1_000,
                amount: 1,
                precision_ms: None,
                ttl_ms: Some(TTL_MS),
            },
        });
        assert!(increment.status.ok, "seed write failed: {:?}", increment.status);
        let set_and_get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateSetAndGetWithOptions {
                family: ControlStateFamily::Counter,
                key: "with_options:setandget".to_string(),
                timestamp_ms: 1_000,
                amount: 1,
                start_ms: 0,
                end_ms: 2_000,
                aggregator: "sum".to_string(),
                precision_ms: None,
                ttl_ms: Some(TTL_MS),
                uuid: None,
            },
        });
        assert!(
            set_and_get.status.ok,
            "seed write failed: {:?}",
            set_and_get.status
        );

        // DENOMINATOR: both deadlines exist before the round trip. Separately.
        let increment_deadline = deadline_for(&engine, 1, INCREMENT_KEY)
            .expect("ControlStateIncrementWithOptions must record a deadline before recovery");
        let family_deadline = deadline_for(&engine, 1, FAMILY_KEY)
            .expect("ControlStateSetAndGetWithOptions must record a deadline before recovery");
        engine.unload_shard(1);
        (increment_deadline, family_deadline)
    };

    // DENOMINATOR: no base index, so the reload below can only be a WAL replay.
    let removed = std::fs::remove_file(index_dir.join("shard-1.index.json")).is_ok();
    assert!(removed, "the base index should exist to be removed");

    let engine = make_engine();
    engine.load_shard(1);

    // DENOMINATOR: replay really ran and really installed these two records. Without this, a
    // missing deadline could be explained by "the shard came back empty".
    assert!(
        outcome_was_installed(&engine, 1, INCREMENT_KEY),
        "replay installed no outcome for the ControlStateIncrementWithOptions record, so this \
         test cannot say anything about what the record carried",
    );
    assert!(
        outcome_was_installed(&engine, 1, FAMILY_KEY),
        "replay installed no outcome for the ControlStateSetAndGetWithOptions record, so this \
         test cannot say anything about what the record carried",
    );

    // THE CLAIM, one arm at a time.
    assert_eq!(
        deadline_for(&engine, 1, INCREMENT_KEY),
        Some(before.0),
        "the ControlStateIncrementWithOptions deadline did not survive recovery: the record's \
         page outcome was installed and its command never re-run, so the deadline had to be in \
         the record and was not",
    );
    assert_eq!(
        deadline_for(&engine, 1, FAMILY_KEY),
        Some(before.1),
        "the ControlStateSetAndGetWithOptions deadline did not survive recovery",
    );
    assert_eq!(
        disagreements(&engine, 1),
        0,
        "the two expiry indexes disagree after recovery",
    );
}

/// A key whose deadline has passed must read as GONE from every kind, including the two whose
/// state no page backs: the seen-set and the token bucket.
///
/// WHAT IS BEING DEFENDED. A deadline is honoured in two places and both have to agree: the
/// background sweep, which collects the key eventually, and LAZY EXPIRY on the command path
/// (`remove_if_expired`), which makes the key read as gone the instant its deadline passes.
/// Without the second, a key stays visible -- and answers with real data -- for the whole
/// interval between its deadline and whichever sweep round happens to collect it. Collection
/// being late is a cost question; serving a value whose deadline has passed is a correctness one.
///
/// Every other kind's read arms already call `remove_if_expired`. Four did not: `SeenCheck`,
/// `SeenCard`, `BucketTake` and `BucketPeek`. Both kinds are reachable by `EXPIRE` -- they are in
/// `record_exists_exact`, so the deadline is accepted -- and both are removed by `delete_record`,
/// so the sweep does collect them. Only their own reads disagreed.
///
/// WHY THESE TWO IN PARTICULAR. They are the deduplication and rate-limiting primitives. A
/// seen-set past its deadline that still answers "duplicate" suppresses work that should have
/// run; a token bucket past its deadline that still answers "denied" keeps rejecting a caller
/// whose limit was meant to have been discarded. Both fail CLOSED, and silently.
///
/// THE SWEEP IS DELIBERATELY NEVER RUN HERE. Everything below is about lazy expiry on the
/// command path alone, which is the half that was missing.
///
/// HALVES ASSERTED SEPARATELY. Four arms, four claims, each behind its own denominator -- the
/// state really existed, and the deadline was really set and really lapsed -- before the claim
/// that the read reports it gone. A combined count would read full from one arm and zero from
/// another.
#[test]
fn a_lapsed_deadline_hides_a_seen_set_and_a_token_bucket_from_their_own_reads() {
    fn integer(engine: &TemporalEngine, command: Command) -> i64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "command failed: {:?}", response.status);
        match response.response {
            CommandResponse::Integer { value } => value,
            other => panic!("expected an integer, got {other:?}"),
        }
    }
    /// The bucket answers three strings; the first is "1" allowed / "0" denied.
    fn bucket_allowed(engine: &TemporalEngine, command: Command) -> String {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "command failed: {:?}", response.status);
        match response.response {
            CommandResponse::Members { members } => {
                String::from_utf8_lossy(members.first().expect("three strings")).into_owned()
            }
            other => panic!("expected members, got {other:?}"),
        }
    }
    fn seen_check(key: &str) -> Command {
        Command::SeenCheck {
            key: key.to_string(),
            member: b"m".to_vec(),
            window_ms: 600_000,
        }
    }
    fn seen_card(key: &str) -> Command {
        Command::SeenCard {
            key: key.to_string(),
        }
    }
    // Refill zero and capacity two, so the bucket is exhausted after exactly two takes and
    // stays exhausted -- no clock enters the answer.
    fn bucket_take(key: &str) -> Command {
        Command::BucketTake {
            key: key.to_string(),
            tokens: 1.0,
            capacity: 2.0,
            refill_per_sec: 0.0,
        }
    }
    fn bucket_peek(key: &str) -> Command {
        Command::BucketPeek {
            key: key.to_string(),
            tokens: 1.0,
            capacity: 2.0,
            refill_per_sec: 0.0,
        }
    }
    fn arm(engine: &TemporalEngine, key: &str) {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: key.to_string(),
                ttl_ms: 1,
            },
        });
        assert!(
            response.status.ok,
            "EXPIRE on {key} was refused ({:?}) -- if this kind cannot carry a deadline at all, \
             everything below is vacuous",
            response.status
        );
    }
    fn deadline_has_lapsed(engine: &TemporalEngine, key: &str) -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard
            .expires_at_ms
            .get(key)
            .is_some_and(|expires_at| *expires_at <= now_ms())
    }

    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- DENOMINATORS: both kinds really hold state, and it really answers -------------
    assert_eq!(
        integer(&engine, seen_check("expiring:seen")),
        0,
        "the first check of a fresh member is not a duplicate",
    );
    assert_eq!(
        integer(&engine, seen_check("expiring:seen")),
        1,
        "the set really remembers the member, so a later 0 means the set was discarded and not \
         that it never worked",
    );
    assert_eq!(
        integer(&engine, seen_card("expiring:seen")),
        1,
        "the set really holds one member",
    );

    assert_eq!(bucket_allowed(&engine, bucket_take("expiring:bucket")), "1");
    assert_eq!(bucket_allowed(&engine, bucket_take("expiring:bucket")), "1");
    assert_eq!(
        bucket_allowed(&engine, bucket_take("expiring:bucket")),
        "0",
        "the bucket really is exhausted, so a later 1 means it was discarded and not that the \
         limit never applied",
    );
    assert_eq!(
        bucket_allowed(&engine, bucket_peek("expiring:bucket")),
        "0",
        "a peek agrees the bucket is exhausted",
    );

    arm(&engine, "expiring:seen");
    arm(&engine, "expiring:bucket");
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(
        deadline_has_lapsed(&engine, "expiring:seen"),
        "the seen-set's deadline must have lapsed before anything is claimed about it",
    );
    assert!(
        deadline_has_lapsed(&engine, "expiring:bucket"),
        "the token bucket's deadline must have lapsed before anything is claimed about it",
    );

    // ---- THE FOUR CLAIMS, one arm at a time --------------------------------------------
    // Ordered so no earlier claim revives the key a later one asks about: the two pure reads
    // go first, then the two read-modify-writes.
    assert_eq!(
        integer(&engine, seen_card("expiring:seen")),
        0,
        "SeenCard still counts members of a set whose deadline has passed",
    );
    assert_eq!(
        bucket_allowed(&engine, bucket_peek("expiring:bucket")),
        "1",
        "BucketPeek still reports a token bucket whose deadline has passed as exhausted, so a \
         caller stays rate-limited by a limit that should have been discarded",
    );
    assert_eq!(
        integer(&engine, seen_check("expiring:seen")),
        0,
        "SeenCheck still reports a duplicate from a set whose deadline has passed, suppressing \
         work that should run",
    );

    // BucketTake starts the bucket over: a full capacity of two, not the exhausted state.
    assert_eq!(
        bucket_allowed(&engine, bucket_take("expiring:bucket")),
        "1",
        "BucketTake carried the exhausted state across the deadline",
    );
    assert_eq!(bucket_allowed(&engine, bucket_take("expiring:bucket")), "1");
    assert_eq!(
        bucket_allowed(&engine, bucket_take("expiring:bucket")),
        "0",
        "and the restarted bucket must hold exactly its capacity of two, not more",
    );

    // CONTROL: a key that never carried a deadline is untouched by any of this. Without it,
    // every assertion above would also pass if the arms had simply stopped reading state.
    assert_eq!(integer(&engine, seen_check("plain:seen")), 0);
    assert_eq!(
        integer(&engine, seen_check("plain:seen")),
        1,
        "a seen-set with no deadline must still remember its member",
    );
    assert_eq!(bucket_allowed(&engine, bucket_take("plain:bucket")), "1");
    assert_eq!(bucket_allowed(&engine, bucket_take("plain:bucket")), "1");
    assert_eq!(
        bucket_allowed(&engine, bucket_take("plain:bucket")),
        "0",
        "a bucket with no deadline must still exhaust",
    );
}

/// A context node whose deadline has passed must read as GONE from every `Context*` arm that
/// reads it.
///
/// WHAT IS BEING DEFENDED. The same rule the seen-set and token-bucket test above defends, on
/// the arms that hold the largest share of unguarded reads. A deadline is honoured in two
/// places: the background sweep, and LAZY EXPIRY on the command path (`remove_if_expired`).
/// The second is a PER-ARM obligation -- each arm has to call it -- and the `Context*` read
/// arms never did. A context node past its deadline kept answering with its full record, and
/// its embedding vector, for as long as it took a sweep round to collect it.
///
/// FAILURE DIRECTION. These fail OPEN, which is the more severe of the two directions. The
/// four arms closed by the earlier work failed CLOSED -- a lapsed seen-set answered
/// "duplicate" and suppressed work, a lapsed bucket answered "denied". These serve the
/// CONTENT of a record whose deadline has passed: the node body, the canonical name, and the
/// L0 embedding vector that a retrieval scores against. A tenant who set a TTL to bound how
/// long a record may be read has that bound quietly not applied.
///
/// THE SWEEP IS DELIBERATELY NEVER RUN. Everything below is lazy expiry on the command path.
///
/// ONE CLAIM PER ARM, EACH BEHIND ITS OWN DENOMINATOR. Three arms read `ctx:node:` -- a
/// single read, a batch read, and the embedding read -- and a combined count would report
/// full from one and empty from another. Each is asserted to answer first, so a later empty
/// means the record was discarded and not that the arm never worked.
#[test]
fn a_lapsed_deadline_hides_a_context_node_from_every_context_read() {
    const TENANT: u64 = 7;
    const NODE: u64 = 4242;
    const PLAIN_NODE: u64 = 4243;

    fn node_key(tenant_hash: u64, node_hash: u64) -> String {
        format!("ctx:node:{tenant_hash}:{node_hash}")
    }
    fn upsert(engine: &TemporalEngine, node_hash: u64) {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: "n".to_string(),
                    status: 1,
                    last_event_time_ms: 0,
                    raw_metadata_ref: String::new(),
                    l0: "l0 text".to_string(),
                    l1_ref: String::new(),
                    vector: vec![1.0, 0.0, 0.0],
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        });
        assert!(response.status.ok, "upsert failed: {:?}", response.status);
    }
    fn get_node(engine: &TemporalEngine, node_hash: u64) -> Option<ContextNode> {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNode {
                tenant_hash: TENANT,
                node_hash,
            },
        });
        assert!(response.status.ok, "get failed: {:?}", response.status);
        match response.response {
            CommandResponse::ContextNode { node, .. } => node,
            other => panic!("expected a context node, got {other:?}"),
        }
    }
    fn get_nodes(engine: &TemporalEngine, node_hash: u64) -> usize {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNodes {
                tenant_hash: TENANT,
                node_hashes: vec![node_hash],
            },
        });
        assert!(response.status.ok, "batch get failed: {:?}", response.status);
        match response.response {
            CommandResponse::ContextNodes { nodes } => nodes.len(),
            other => panic!("expected context nodes, got {other:?}"),
        }
    }
    fn embeddings(engine: &TemporalEngine, node_hash: u64) -> usize {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryNodeEmbeddings {
                tenant_hash: TENANT,
                node_hashes: vec![node_hash],
            },
        });
        assert!(response.status.ok, "embedding read failed: {:?}", response.status);
        match response.response {
            CommandResponse::ContextNodeEmbeddings { embeddings } => embeddings.len(),
            other => panic!("expected context node embeddings, got {other:?}"),
        }
    }
    fn arm(engine: &TemporalEngine, key: &str) {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: key.to_string(),
                ttl_ms: 1,
            },
        });
        assert!(
            response.status.ok,
            "EXPIRE on {key} was refused ({:?}) -- if a context node cannot carry a deadline at \
             all, everything below is vacuous",
            response.status,
        );
    }
    fn deadline_has_lapsed(engine: &TemporalEngine, key: &str) -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard
            .expires_at_ms
            .get(key)
            .is_some_and(|expires_at| *expires_at <= now_ms())
    }

    let engine = TemporalEngine::default();
    engine.load_shard(1);

    upsert(&engine, NODE);
    upsert(&engine, PLAIN_NODE);

    // ---- DENOMINATORS: every arm below really answers for this node --------------------
    assert!(
        get_node(&engine, NODE).is_some(),
        "the node must read back before its deadline, or a later None means it was never stored",
    );
    assert_eq!(get_nodes(&engine, NODE), 1, "the batch read must see the node first");
    assert_eq!(
        embeddings(&engine, NODE),
        1,
        "the node must carry a readable embedding first",
    );

    // ---- the deadline is set, and it really lapses -------------------------------------
    arm(&engine, &node_key(TENANT, NODE));
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(
        deadline_has_lapsed(&engine, &node_key(TENANT, NODE)),
        "the node's deadline must have lapsed before anything is claimed about it",
    );

    // ---- THE THREE CLAIMS, one arm at a time -------------------------------------------
    assert_eq!(
        get_node(&engine, NODE),
        None,
        "ContextGetNode still serves the body of a node whose deadline has passed",
    );
    assert_eq!(
        get_nodes(&engine, NODE),
        0,
        "ContextGetNodes still serves a node whose deadline has passed",
    );
    assert_eq!(
        embeddings(&engine, NODE),
        0,
        "ContextQueryNodeEmbeddings still serves the embedding vector of a node whose deadline \
         has passed, so a retrieval keeps scoring against it",
    );

    // ---- CONTROL: a node that never carried a deadline is untouched --------------------
    // Without this, every claim above would also pass if the arms had simply stopped reading.
    assert!(
        get_node(&engine, PLAIN_NODE).is_some(),
        "a node with no deadline must still read back",
    );
    assert_eq!(
        get_nodes(&engine, PLAIN_NODE),
        1,
        "a node with no deadline must still appear in a batch read",
    );
    assert_eq!(
        embeddings(&engine, PLAIN_NODE),
        1,
        "a node with no deadline must still surface its embedding",
    );
}

/// Every command arm that can read a key carrying a deadline must consult that deadline, and
/// an arm that does not must say why.
///
/// WHY A GUARD AND NOT JUST THE FIXES. The read-time half of expiry is a PER-CALL-SITE
/// obligation. `execute_on_shard` is one `match` with one arm per command, and each arm has to
/// remember to call `remove_if_expired` (or `drop_if_expired`, which wraps it). Ninety-four
/// call sites of a rule is ninety-four chances to forget it, and they have been forgotten
/// twice now: four arms were found by one audit, and thirty-two more by the next. Nothing in
/// the type system or the compiler notices, because forgetting is spelled as the absence of a
/// line. So the absence is what this reads.
///
/// A CHOKE POINT WOULD BE BETTER, AND IS NOT AVAILABLE HERE. The obvious shape is one
/// object-read function taking a check-expiry flag that every arm goes through. Ours has no
/// such funnel: the `Context*` arms index `shard.context_*` directly and then read a page by
/// ADDRESS, and an address does not know its key, so the page reader cannot consult a
/// deadline. Hoisting the check above the `match` was considered and rejected for two
/// reasons, both recorded here so it is not re-proposed:
///
///   * It would invert the `if remove_if_expired(...) { ... return }` branch in the arms that
///     already guard. Those early returns also invalidate the cache entry for the key; a
///     pre-pass that consumed the removal would send them down the fall-through path instead,
///     where `cached_response` can answer from the cached copy of the record just removed.
///   * It would need a key derivation per command in one place, away from the arm that reads
///     the key -- and a derivation that named the wrong key would leave the arm unguarded
///     while making THIS guard pass, because every arm would be "covered" by the pre-pass.
///     The fix would have removed the site the scan watches.
///
/// So the call stays in the arm, beside the key it is about, and this counts the arms.
///
/// THE EXEMPTIONS ARE HAND-WRITTEN, AND THAT IS THE POINT. They are not derived from the code
/// being checked -- a guard that built its own exemption list from the arms it found would
/// pass no matter what the arms did. A new command arm is unguarded and unlisted, so it fails
/// here until someone decides which it is.
#[test]
fn execute_on_shard_guards_every_arm_that_can_hold_a_deadline() {
    const SOURCE: &str = include_str!("../execute_on_shard.rs");

    // The two spellings that discharge the obligation. `drop_if_expired` wraps
    // `remove_if_expired` and also drops the cache entry.
    const GUARDS: [&str; 2] = ["remove_if_expired", "drop_if_expired"];

    // Arms that read no key able to carry a deadline, each with the reason. `EXPIRE` only
    // records a deadline for a key `record_exists_exact` can see, and only
    // `delete_record_exact` collects one, so "cannot hold a deadline" means: not in those.
    const EXEMPT: [(&str, &str); 8] = [
        ("LeaderEstablish", "touches no object at all; `command_object_keys` gives it none"),
        (
            "ContextResourceBlobBegin",
            "heads the six blob variants, which are dispatched before the shard lock -- blobs \
             live beside the engine, not in shard record state, and carry no deadline",
        ),
        (
            "CommonDelete",
            "deletes the record unconditionally and answers Empty either way, so the expired \
             and the live case are already the same command with the same answer",
        ),
        ("CommonTtl", "guards through `ttl_ms`, which calls `remove_if_expired` first"),
        (
            "ContextMarkSummaryDirty",
            "reads only `context_dirty_index`, an ephemeral in-memory map that \
             `record_exists_exact` cannot see and `delete_record_exact` does not touch",
        ),
        ("ContextQuerySummaryDirty", "reads only `context_dirty_index`; see above"),
        (
            "ContextMarkEmbeddingDirty",
            "reads only `context_embedding_dirty_index`, ephemeral in the same way",
        ),
        ("ContextQueryEmbeddingDirty", "reads only `context_embedding_dirty_index`; see above"),
    ];

    // ---- the arms, and their DENOMINATOR ----------------------------------------------
    let mut arms: Vec<(String, usize)> = Vec::new();
    for (index, line) in SOURCE.lines().enumerate() {
        let Some(rest) = line.strip_prefix("        Command::") else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            arms.push((name, index + 1));
        }
    }
    assert!(
        arms.len() > 80,
        "VACUITY: the arm scan found only {} arms in execute_on_shard.rs. Either the file moved \
         or the `        Command::` shape changed, and this guard is reading nothing.",
        arms.len(),
    );

    // ---- split each arm's body at the next arm ----------------------------------------
    let lines: Vec<&str> = SOURCE.lines().collect();
    let starts: Vec<usize> = arms.iter().map(|(_, line)| line - 1).collect();
    let mut unguarded: Vec<(String, usize)> = Vec::new();
    let mut guarded = 0usize;
    for (index, (name, line)) in arms.iter().enumerate() {
        let end = starts.get(index + 1).copied().unwrap_or(lines.len());
        let body = lines[starts[index]..end].join("\n");
        if GUARDS.iter().any(|guard| body.contains(guard)) {
            guarded += 1;
        } else {
            unguarded.push((name.clone(), *line));
        }
    }
    assert!(
        guarded > 60,
        "VACUITY: only {guarded} of {} arms were read as guarded. A rewrite that changed how the \
         check is spelled would blind this guard exactly this way.",
        arms.len(),
    );

    // ---- POSITIVE CONTROL: the scan can tell the two apart ----------------------------
    // Without this, a scan that matched nothing would report every arm unguarded, and a scan
    // that matched everything would report none -- and either could be mistaken for a result.
    let exempt_names: Vec<&str> = EXEMPT.iter().map(|(name, _)| *name).collect();
    for name in &exempt_names {
        assert!(
            arms.iter().any(|(arm, _)| arm == name),
            "the exemption for `{name}` names an arm that no longer exists. A stale exemption \
             silently excuses nothing and hides the arm that replaced it -- remove it, or point \
             it at the arm that took its place.",
        );
    }
    assert!(
        unguarded.iter().any(|(name, _)| name == "LeaderEstablish"),
        "CONTROL: `LeaderEstablish` contains no expiry check and must be read as unguarded. It \
         is not, so this scan is matching something other than what it claims.",
    );
    assert!(
        !unguarded.iter().any(|(name, _)| name == "StringGet"),
        "CONTROL: `StringGet` calls `remove_if_expired` on its first line and must be read as \
         guarded. It is not, so this scan is missing real checks.",
    );

    // ---- THE CLAIM --------------------------------------------------------------------
    let surprises: Vec<String> = unguarded
        .iter()
        .filter(|(name, _)| !exempt_names.contains(&name.as_str()))
        .map(|(name, line)| format!("  Command::{name} at execute_on_shard.rs:{line}"))
        .collect();
    assert!(
        surprises.is_empty(),
        "{} of {} command arms neither consult a deadline nor are listed as unable to hold \
         one:\n{}\n\nA key whose deadline has passed must not be visible to a read. Either call \
         `drop_if_expired(cache, shard_id, shard, &key)` with the key the arm reads, or add the \
         arm to EXEMPT above WITH THE REASON it cannot hold a deadline -- which means it is in \
         neither `record_exists_exact` (so EXPIRE cannot record one) nor `delete_record_exact` \
         (so no sweep collects one).",
        surprises.len(),
        arms.len(),
        surprises.join("\n"),
    );

    // ---- and the exemptions stay a short, argued list ---------------------------------
    assert_eq!(
        unguarded.len(),
        EXEMPT.len(),
        "every unguarded arm is accounted for, but the counts disagree: {} unguarded against {} \
         exemptions. An exemption that excuses nothing should go.",
        unguarded.len(),
        EXEMPT.len(),
    );
}
