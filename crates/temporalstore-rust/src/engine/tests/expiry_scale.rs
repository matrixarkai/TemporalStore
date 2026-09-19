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
    const EXEMPT: [(&str, &str); 7] = [
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


/// A delta record carries a deadline a write ARMED, and one it MOVED.
///
/// WHAT WAS LOST. The key-state capture that rides on a delta record is gated on
/// `membership_shrank` -- did any per-key collection get SMALLER. `key_membership_size` DOES
/// count the deadline, but only as `expires_at_ms.contains_key(key)`, one unit of presence.
/// That sees a deadline being REMOVED, because 1 -> 0 is a shrink. It is blind to the other
/// two directions:
///
///   * ARMING a deadline where there was none is 0 -> 1, a GROWTH, and the gate fires only on
///     a shrink;
///   * MOVING a deadline to a different millisecond leaves `contains_key` true on both sides,
///     so nothing the gate measures changes at all.
///
/// WHY AN EMPTY CAPTURE IS A LOSS AND NOT JUST A SLOW PATH. The record still carries an
/// ANCHOR, and `fold_index_log_deltas` advances `applied_wal_sequence` to it. On the
/// legacy-recovery load path the WAL is then replayed only BEYOND that anchor, so the
/// `CommonExpire` that armed the deadline sits at or below it and is replayed by nobody. The
/// deadline is recovered from neither the base, nor the record, nor the log. That anchor
/// advance is asserted below rather than assumed, because without it there would be no bug.
///
/// LATENT, NOT LIVE, AND ASSERTED RATHER THAN ASSUMED. #1644 established that the fold is
/// never called on the default load path: `load_shard_with` takes `load_index_base_only` under
/// the single-barrier default, which passes `fold_deltas = false`. The fold is reached only
/// through `load_index_checked`, i.e. only under the `TS_WAL_LEGACY_RECOVERY` escape hatch --
/// which is exactly the entry point this test drives, and the reason it drives that one rather
/// than a plain reload.
///
/// THE TWO HALVES ARE ARM AND MOVE, AND THEY ARE SEPARATE CLAIMS. A fix that captured only
/// when a deadline APPEARED would satisfy the first and leave the second losing every re-arm.
///
/// CLEARING IS A CONTROL HERE, NOT A THIRD CLAIM, AND THE DISTINCTION IS LOAD-BEARING. A
/// removal already shrinks the membership, so it was captured before this change and is
/// captured after it; asserting it proves the change did not BREAK the direction that already
/// worked, and proves nothing about the change itself. It is written down as a control because
/// a reader who mistook it for a claim would conclude this guard covers more than it does --
/// and because a mutation that reverts only the arm/move handling leaves it passing, which is
/// exactly what a vacuous row looks like from the outside.
#[test]
fn a_delta_record_carries_a_deadline_a_write_armed_and_one_it_moved() {
    const LIVE_KEYS: usize = 16;
    let armed_key = "arm-target";
    let moved_key = "move-target";
    let persist_key = "persist-target";

    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache"), &pages, &indexes);
    engine.load_shard(1);

    // Background keys, so the base index is a real one rather than a three-row curiosity.
    let mut seed = Vec::with_capacity(LIVE_KEYS);
    for index in 0..LIVE_KEYS {
        seed.push((format!("live-{index:08}"), 3_600_000u64));
    }
    write_keys(&engine, 1, seed);

    // THE THREE SUBJECTS, IN THE STATE THE BASE MUST CAPTURE THEM IN.
    //  * `armed_key` starts with NO deadline -- only the delta can supply one (0 -> 1, growth);
    //  * `moved_key` starts WITH one, which the write below moves (1 -> 1, no size change);
    //  * `persist_key` starts WITH one, which the write below removes (1 -> 0, a shrink) --
    //    the control.
    let set = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: armed_key.to_string(),
            value: vec![b'v'; 16],
        },
    });
    assert!(set.status.ok, "seeding {armed_key} failed: {:?}", set.status);
    write_keys(
        &engine,
        1,
        vec![
            (moved_key.to_string(), 3_600_000u64),
            (persist_key.to_string(), 3_600_000u64),
        ],
    );

    // THE STALE BASE. Materialized BEFORE any of the deadline writes below.
    engine.flush_shard_index(1);
    let base_path = engine.index_path(1);
    let base_before = std::fs::read(&base_path).expect("the base index should have been written");
    let base_state = decode_index_bytes(&base_before).expect("the base index should decode");
    let base_anchor = base_state.applied_wal_sequence.unwrap_or(0);
    assert!(
        base_anchor > 0,
        "the base index carries no WAL anchor. At zero the fold folds the WHOLE log instead of \
         the suffix beyond the base, which is a different path from the one under test"
    );
    assert!(
        !base_state.expires_at_ms.contains_key(armed_key),
        "DENOMINATOR: the base already names a deadline for {armed_key}, before anything armed \
         one. The base is the stale source that has never heard of it -- with it already \
         present, a fold that applies NOTHING would look exactly like one that works"
    );
    let base_moved = base_state.expires_at_ms.get(moved_key).copied();
    assert!(
        base_moved.is_some(),
        "DENOMINATOR: the base does not name a deadline for {moved_key}, so the move half has \
         no stale value to be corrected away from"
    );
    assert!(
        base_state.expires_at_ms.contains_key(persist_key),
        "DENOMINATOR for the clearing CONTROL: the base does not name {persist_key}"
    );

    let records_before = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the index log should read back")
        .len();

    // THE THREE WRITES. The first GROWS the deadline membership, the second leaves it flat,
    // the third SHRINKS it -- and only the third was visible to the gate before this change.
    let armed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: armed_key.to_string(),
            ttl_ms: 3_600_000,
        },
    });
    assert!(armed.status.ok, "arming failed: {:?}", armed.status);
    // A different duration, so the moved deadline cannot coincide with the one it replaced --
    // an equal re-arm is deliberately NOT a change, and would make this half vacuous.
    let moved = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: moved_key.to_string(),
            ttl_ms: 7_200_000,
        },
    });
    assert!(moved.status.ok, "re-arming failed: {:?}", moved.status);
    let persisted = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonPersist {
            key: persist_key.to_string(),
        },
    });
    assert!(persisted.status.ok, "persist failed: {:?}", persisted.status);

    // What the LIVE shard now holds. If it does not match what was asked for, the recovery
    // assertions below are about the wrong thing entirely.
    let (live_armed, live_moved, live_wal_sequence) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        assert!(
            shard.expires_at_ms.contains_key(armed_key),
            "DENOMINATOR: the LIVE shard has no deadline for {armed_key} after CommonExpire, so \
             there is no deadline for recovery to lose"
        );
        assert!(
            !shard.expires_at_ms.contains_key(persist_key),
            "DENOMINATOR for the control: the LIVE shard still holds a deadline for \
             {persist_key} after CommonPersist"
        );
        (
            shard.expires_at_ms.get(armed_key).copied(),
            shard.expires_at_ms.get(moved_key).copied(),
            shard.applied_wal_sequence.unwrap_or(0),
        )
    };
    assert_ne!(
        live_moved, base_moved,
        "DENOMINATOR: the re-arm left {moved_key} on the SAME millisecond the base already \
         holds ({base_moved:?}). Then the base and the correct answer agree, and the move half \
         would pass without the delta carrying anything"
    );

    // THE RECORDS EXIST AND ANCHOR BEYOND THE BASE. A record at or below the base anchor is
    // SKIPPED by the fold, which would make every claim below vacuous.
    let records = engine
        .index_log_store
        .read_delta_records(1, 0)
        .expect("the index log should read back");
    let appended = records.len().saturating_sub(records_before);
    assert!(
        appended > 0,
        "the three deadline writes appended no delta records at all"
    );
    let max_anchor = records
        .iter()
        .filter_map(|record| record.applied_wal_sequence)
        .max()
        .unwrap_or(0);
    assert!(
        max_anchor > base_anchor,
        "the deadline writes left the delta log anchored at {max_anchor}, at or below the \
         base's {base_anchor}. The fold skips those records, so the load below measures the \
         base alone"
    );

    // THE ANCHOR ADVANCE IS THE MECHANISM, SO IT IS ASSERTED, NOT ASSUMED.
    assert!(
        max_anchor >= live_wal_sequence,
        "the delta anchor {max_anchor} is BEHIND the WAL sequence {live_wal_sequence} the \
         deadline writes reached. That would make the WAL tail replay them and there would be \
         no loss to fix -- the premise of this test would be gone"
    );

    // A second engine over the SAME files, so both arms read one set.
    let reader = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-control"),
        &pages,
        &indexes,
    );

    // CONTROL ARM: the same load with the fold switched OFF -- the stale base and nothing else.
    // It must disagree with the live shard on all three keys, or the treatment is not what is
    // producing the agreement.
    let control = reader
        .load_index_base_only(1, false)
        .expect("the base index should load");
    assert!(
        !control.expires_at_ms.contains_key(armed_key),
        "CONTROL: the unfolded base already holds a deadline for {armed_key}. Something other \
         than the fold supplies it, so the arm half would pass with the capture deleted"
    );
    assert_eq!(
        control.expires_at_ms.get(moved_key).copied(),
        base_moved,
        "CONTROL: the unfolded base does not hold {moved_key}'s ORIGINAL deadline, so the move \
         half cannot tell a corrected value from the stale one"
    );
    assert!(
        control.expires_at_ms.contains_key(persist_key),
        "CONTROL: the unfolded base has already lost {persist_key}'s deadline"
    );

    // TREATMENT: the same two files, folded, through the entry point the legacy-recovery path
    // uses. Everything that differs from the control is the fold's work and nothing else's.
    let folded = reader
        .load_index_checked(1, false)
        .expect("the delta log is intact, so the checked load must not refuse it")
        .expect("the base index should load");

    println!(
        "  base at anchor {base_anchor}: denies {armed_key}, holds {moved_key} at {base_moved:?}, \
         holds {persist_key}; {appended} delta record(s) to anchor {max_anchor} (live WAL \
         {live_wal_sequence}). Unfolded: {} deadlines, {armed_key}=None, {moved_key}={:?}. \
         Folded: {} deadlines, {armed_key}={:?}, {moved_key}={:?}",
        control.expires_at_ms.len(),
        control.expires_at_ms.get(moved_key).copied(),
        folded.expires_at_ms.len(),
        folded.expires_at_ms.get(armed_key).copied(),
        folded.expires_at_ms.get(moved_key).copied(),
    );

    // ---- HALF ONE: the ARMED deadline survives (0 -> 1, a growth the gate could not see) ----
    assert_eq!(
        folded.expires_at_ms.get(armed_key).copied(),
        live_armed,
        "the folded load did not recover the deadline {armed_key} was given. The base never had \
         one and the delta record is the only other source in this load, so a missing or \
         different value here means the record did not carry it -- and because the record's \
         anchor ({max_anchor}) already covers the WAL entry that armed it ({live_wal_sequence}), \
         replay will not supply it either. The key comes back immortal"
    );

    // ---- HALF TWO: the MOVED deadline survives (1 -> 1, no size change at all) --------------
    // Asserted separately and on its own key: a fix that captured only when a deadline
    // APPEARED would satisfy half one completely and leave this one holding the stale value.
    assert_eq!(
        folded.expires_at_ms.get(moved_key).copied(),
        live_moved,
        "the folded load came back with the WRONG deadline for {moved_key}. The stale base \
         holds {base_moved:?} and the live shard holds {live_moved:?}; recovering the base's \
         value means the record did not carry the move, and the key expires at a moment the \
         caller replaced"
    );

    // ---- CONTROL, NOT A CLAIM: a CLEARED deadline stays cleared -----------------------------
    // A removal shrinks the membership, so this direction was captured before this change too.
    // It is here to show the change did not break what already worked -- it is NOT evidence
    // that the change does anything, and a mutation reverting the arm/move handling leaves it
    // passing.
    assert!(
        !folded.expires_at_ms.contains_key(persist_key),
        "CONTROL: the folded load brought back the deadline PERSIST removed from {persist_key}. \
         This direction was already covered by the membership shrink, so a failure here is this \
         change having broken it rather than having missed it"
    );

    // ---- CONTROL, THE OTHER DIRECTION: the untouched keys are untouched ---------------------
    // The capture wipes and restores whole per-key maps, so a blob naming more keys than the
    // write touched deletes live state on recovery.
    assert_eq!(
        folded.expires_at_ms.len(),
        LIVE_KEYS + 2,
        "the folded index holds {} deadlines, expected the {LIVE_KEYS} background keys plus \
         {armed_key} and {moved_key}, and not {persist_key}. The unfolded control holds {}",
        folded.expires_at_ms.len(),
        control.expires_at_ms.len()
    );
    for index in [0usize, LIVE_KEYS / 2, LIVE_KEYS - 1] {
        let key = format!("live-{index:08}");
        assert!(
            folded.expires_at_ms.contains_key(&key),
            "the fold dropped {key}, which none of the three writes touched"
        );
    }
}


/// Every clock read in `execute_on_shard` asks the REPLAY-AWARE clock.
///
/// `resolve_now_ms()` returns the per-record leader timestamp while a record is being replayed
/// and the live clock otherwise. `remove_if_expired` records at engine.rs:3948 why the restart
/// clock is the wrong question during replay -- a key that was live at leader-time would read as
/// expired on recovery, dropping a durably committed write -- and `command_validation.rs:493`
/// states the same obligation for the validator, in as many words: the expiry checks there "use
/// the SAME replay-aware clock as the executor".
///
/// That obligation was stated in two comments and enforced nowhere. One arm disagreed:
/// `Command::CommonTtl` tested its deadline against the bare `now_ms()` while the collection it
/// performed one line later, through `ttl_ms` -> `remove_if_expired`, used the replay-aware one.
/// Ten of the eleven clock reads in the file were already right; that was the eleventh.
///
/// LATENT, AND SAID SO PLAINLY. `CommonTtl` is classified as a read
/// (`is_raft_read_command`, `is_write_command`), so it is never appended to the WAL and therefore
/// never re-executed with a replay clock installed. The split could not be reached from any
/// command path, and four separate attempts to make it produce an observable difference -- a
/// missing WAL tombstone, a stale cached read after TTL, a divergent replayed deadline, and a
/// stale block-ownership index -- all came back negative. This guard is here because the cost of
/// the hazard is not paid until a classification changes, and at that point nothing would have
/// said so. It fails on a NUMBER, not on a shape: the count of non-replay-aware clock reads.
#[test]
fn every_clock_read_in_execute_on_shard_is_replay_aware() {
    const SOURCE: &str = include_str!("../execute_on_shard.rs");

    // The classifier is `classify_clock_reads`, shared with the serving-read-path guard below.
    // Used on the file AND on the controls, so a control can never be checked by different
    // code than the subject, and so the two guards cannot drift apart.
    let classify = classify_clock_reads;

    // ---- POSITIVE CONTROL: the classifier can tell the two spellings apart ---------------
    // Without this, a scan that matched nothing would report every file clean, and that would
    // look exactly like the result this test exists to produce.
    let (control_aware, control_live) = classify("let a = resolve_now_ms();\nlet b = now_ms();");
    assert_eq!(
        (control_aware, control_live.len()),
        (1, 1),
        "CONTROL: the classifier must read one replay-aware and one live clock read out of a \
         line of each; it read {control_aware} and {}.",
        control_live.len(),
    );
    // And it must not be fooled by a comment, which is how the doc above is written.
    let (_, commented) = classify("// this mentions now_ms() only in prose");
    assert!(
        commented.is_empty(),
        "CONTROL: a mention inside a comment must not count as a clock read.",
    );

    // ---- DENOMINATOR --------------------------------------------------------------------
    let (replay_aware, live) = classify(SOURCE);
    assert!(
        replay_aware > 8,
        "VACUITY: only {replay_aware} replay-aware clock reads were found in \
         execute_on_shard.rs. Either the file moved or the spelling changed, and this guard is \
         reading nothing.",
        replay_aware = replay_aware,
    );

    // ---- THE CLAIM ----------------------------------------------------------------------
    let rendered: Vec<String> = live
        .iter()
        .map(|(line, text)| format!("  execute_on_shard.rs:{line}: {text}"))
        .collect();
    assert_eq!(
        live.len(),
        0,
        "{} of {} clock reads in execute_on_shard.rs bypass the replay-aware clock:\n{}\n\nA \
         deadline compared against the restart clock during replay reads a key that was live at \
         leader-time as expired. Use `resolve_now_ms()`, or reach the deadline through \
         `drop_if_expired` / `remove_if_expired`, which already do.",
        live.len(),
        replay_aware + live.len(),
        rendered.join("\n"),
    );
}


/// The classifier both replay-aware clock guards use.
///
/// Returns the count of `resolve_now_ms()` reads and every bare `now_ms()` read as
/// (line-within-the-scanned-text, trimmed source). Lines that START a comment are skipped, so a
/// doc comment naming a spelling is not counted as a reading of it.
///
/// Shared rather than copied so a control can never be checked by different code than the
/// subject, and so a fix to one guard's classifier cannot leave the other's behind.
fn classify_clock_reads(text: &str) -> (usize, Vec<(usize, String)>) {
    let mut replay_aware = 0usize;
    let mut live: Vec<(usize, String)> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        let mut offset = 0usize;
        while let Some(at) = line[offset..].find("now_ms()") {
            let absolute = offset + at;
            if line[..absolute].ends_with("resolve_") {
                replay_aware += 1;
            } else {
                live.push((index + 1, line.trim().to_string()));
            }
            offset = absolute + "now_ms()".len();
        }
    }
    (replay_aware, live)
}

/// Lift one function body out of a source file: from `signature` to the first later line that
/// is EXACTLY `closing`.
///
/// The signature must occur exactly once. A second definition of the same name would otherwise
/// let this read one body while the crate compiles the other, and the guard would be watching a
/// function nothing calls.
fn function_body(source: &str, signature: &str, closing: &str) -> String {
    assert_eq!(
        source.matches(signature).count(),
        1,
        "VACUITY: `{signature}` must appear exactly once in the scanned source; it appeared {}. \
         This guard cannot know which definition it is reading.",
        source.matches(signature).count(),
    );
    let start = source.find(signature).expect("checked above");
    let mut body = String::new();
    for (index, line) in source[start..].lines().enumerate() {
        body.push_str(line);
        body.push('\n');
        if index > 0 && line == closing {
            break;
        }
    }
    body
}

fn render_live_reads(live: &[(usize, String)], scope: &str) -> String {
    live.iter()
        .map(|(line, text)| format!("  {scope} (+{line} lines): {text}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every clock read on the SERVING READ path in `engine.rs` asks the REPLAY-AWARE clock.
///
/// WHY A SECOND GUARD, AND WHAT THE FIRST ONE CANNOT SEE.
/// `every_clock_read_in_execute_on_shard_is_replay_aware` states this rule for exactly one
/// file, and that file is the SLOW path. The route a deployment answers `GET` and `HGETALL`
/// from is `execute_read_only_fast_path`, which lives in `engine.rs` and is outside everything
/// that guard can read: `RecordStore::Local` reads through `execute_durable`, `execute_durable`
/// passes a storage override, and the override is the whole entry condition for the fast path --
/// it is tried BEFORE `execute_on_shard` is ever reached. Three deadline reads there asked the
/// bare `now_ms()`.
///
/// NOT LATENT -- THIS ONE IS REACHED, AND IT WAS MEASURED. Counting every entry into the fast
/// path across this suite gave 7 963 entries, of which 228 ran with a leader clock already
/// installed: all of them `StringGet`, all with `raft_applying` true, every one arriving through
/// `execute_raft_apply_batch_at` -> `execute_with_storage_override` -> the fast path, driven by
/// `raft::apply_committed_recording` under the follower pipeline. So a committed READ entry does
/// reach this code with the leader's stamp in force. `is_raft_read_command`, which keeps reads
/// out of the log, lives in the SERVER BINARY's routing layer -- not in the engine and not in
/// the raft library -- so it cannot be what makes this safe here.
///
/// WHAT THE BARE CLOCK COSTS. Each replica applies the same committed entry at its own moment
/// and against its own wall clock, so one log entry can be answered differently on each replica:
/// the one whose clock has passed the deadline declines the fast path and the slow path hides
/// the key, while the one whose clock has not serves the value straight out of the fast path.
/// That is the divergence `execute_raft_apply_at` already exists to prevent for writes -- its own
/// doc says one committed entry must not produce as many answers as there are replicas -- and the
/// read path was not holding to it.
///
/// SCOPED BY FUNCTION, NOT BY FILE, AND WITH NO EXEMPTION LIST. `engine.rs` reads the live clock
/// for things that are not deadlines -- LRU bucket recency, the temporal-compression window --
/// and those are right to. Scanning the whole file would need a list of blessed lines, and a
/// list is where a real one hides. So this takes the two functions that decide whether a
/// DEADLINE has passed and holds every clock read inside them to the rule.
///
/// It fails on a NUMBER, not on a shape: the count of non-replay-aware clock reads.
#[test]
fn every_clock_read_on_the_serving_read_path_is_replay_aware() {
    const SOURCE: &str = include_str!("../../engine.rs");

    // ---- POSITIVE CONTROL: the classifier can tell the two spellings apart ---------------
    // Without this, a scan that matched nothing would report the path clean, and that looks
    // exactly like the result this guard exists to produce.
    let (control_aware, control_live) =
        classify_clock_reads("let a = resolve_now_ms();\nlet b = now_ms();");
    assert_eq!(
        (control_aware, control_live.len()),
        (1, 1),
        "CONTROL: the classifier must read one replay-aware and one live clock read out of a \
         line of each; it read {control_aware} and {}.",
        control_live.len(),
    );
    let (_, commented) = classify_clock_reads("// this mentions now_ms() only in prose");
    assert!(
        commented.is_empty(),
        "CONTROL: a mention inside a comment must not count as a clock read.",
    );

    // ---- THE SUBJECTS -------------------------------------------------------------------
    let fast_path = function_body(SOURCE, "    fn execute_read_only_fast_path(", "    }");
    let ttl = function_body(SOURCE, "fn ttl_ms(", "}");

    // ---- VACUITY, ON THE SCAN RATHER THAN ON THE OUTCOME --------------------------------
    // A landmark from INSIDE each body, so a function that was renamed, moved or truncated
    // fails here loudly instead of reading as clean. A truncated scan reads exactly like a
    // clean one, which is the failure this pair of assertions exists to prevent.
    assert!(
        fast_path.contains("FastPathRead::String"),
        "VACUITY: the extracted `execute_read_only_fast_path` body ({} lines) does not contain \
         its own `FastPathRead::String` arm, so the extraction is reading the wrong span.",
        fast_path.lines().count(),
    );
    assert!(
        ttl.contains("remove_if_expired"),
        "VACUITY: the extracted `ttl_ms` body ({} lines) does not contain its \
         `remove_if_expired` call, so the extraction is reading the wrong span.",
        ttl.lines().count(),
    );

    let (fast_aware, fast_live) = classify_clock_reads(&fast_path);
    let (ttl_aware, ttl_live) = classify_clock_reads(&ttl);
    let scanned = fast_aware + fast_live.len() + ttl_aware + ttl_live.len();
    assert!(
        scanned >= 3,
        "VACUITY: only {scanned} clock reads were found across the serving read path \
         (`execute_read_only_fast_path` + `ttl_ms`), and there are at least 3 deadline reads \
         there. A body moved or a spelling changed, and this guard is reading nothing.",
    );

    // ---- THE CLAIM, HALVES ASSERTED SEPARATELY ------------------------------------------
    // One combined count reads full from whichever half is fixed first.
    assert_eq!(
        fast_live.len(),
        0,
        "HALF ONE: {} of {} clock reads in `execute_read_only_fast_path` bypass the \
         replay-aware clock:\n{}\n\nThis is the route `RecordStore::Local` answers GET and \
         HGETALL from, and the one a committed read entry reaches under raft apply with the \
         leader's stamp installed. Use `resolve_now_ms()`.",
        fast_live.len(),
        fast_aware + fast_live.len(),
        render_live_reads(&fast_live, "execute_read_only_fast_path"),
    );
    assert_eq!(
        ttl_live.len(),
        0,
        "HALF TWO: {} of {} clock reads in `ttl_ms` bypass the replay-aware clock:\n{}\n\n\
         `ttl_ms` already COLLECTS through `remove_if_expired`, which resolves against the \
         replay clock; the remaining-time arithmetic beside it must ask the same clock, or the \
         two disagree about the same deadline in the same call.",
        ttl_live.len(),
        ttl_aware + ttl_live.len(),
        render_live_reads(&ttl_live, "ttl_ms"),
    );
}

/// A committed READ entry resolves its deadline against the LEADER's clock, not this node's.
///
/// WHY A SOURCE SCAN IS NOT ENOUGH. The guard above counts spellings. This one pins the
/// BEHAVIOUR those spellings buy, through the same public entry point production uses, so a
/// rewrite that keeps the name and loses the meaning still fails.
///
/// WHY THE DEADLINE SITS BETWEEN THE TWO CLOCKS. The fast path's expiry test only DECLINES --
/// `return None` hands the command to the slow path, which re-decides through `remove_if_expired`
/// on the replay-aware clock. So when the leader's stamp is BEHIND this node's clock a bare
/// `now_ms()` merely over-declines and the slow path still answers correctly: nothing to observe.
/// The observable direction is the other one -- a leader stamp AHEAD of this node's clock, which
/// is ordinary inter-node skew on an apply path. There the fast path is the last word: it decides
/// the key has not expired and serves the value, never reaching the slow path that would have
/// hidden it. So the deadline here is placed strictly between the two: not yet passed by this
/// node's clock, already passed at leader time.
///
/// HALVES ASSERTED SEPARATELY. `StringGet`, `HashGetAll` and the remaining-time arithmetic in
/// `ttl_ms` are three sites in the same shape and all three were bare; one combined claim reads
/// full from whichever is fixed first.
#[test]
fn a_committed_read_resolves_its_deadline_against_the_leader_clock() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for command in [
        Command::StringSet {
            key: "skew:string".to_string(),
            value: b"v".to_vec(),
        },
        Command::StringSet {
            key: "ttl:string".to_string(),
            value: b"v".to_vec(),
        },
        Command::HashSet {
            key: "skew:hash".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: command.clone(),
        });
        assert!(
            response.status.ok,
            "the write must land before the deadline is armed: {command:?} -> {}",
            response.status.message,
        );
    }

    // No replay clock is installed here, so this is this node's own clock.
    let local_now = crate::engine::resolve_now_ms();
    let deadline = local_now.saturating_add(60_000);
    let leader_past_deadline = local_now.saturating_add(120_000);

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        crate::engine::set_expiry(shard, "skew:string".to_string(), deadline);
        crate::engine::set_expiry(shard, "skew:hash".to_string(), deadline);
        crate::engine::set_expiry(shard, "ttl:string".to_string(), deadline);
    }

    // The entry point production uses: `execute_raft_apply_batch_at` installs exactly this
    // clock per entry, and a committed `StringGet` was measured arriving here 228 times.
    fn applied_at(engine: &TemporalEngine, command: Command, leader_ms: u64) -> CommandResponse {
        let response = engine.execute_raft_apply_at(
            ExecuteRequest {
                shard_id: 1,
                command,
            },
            Some(leader_ms),
        );
        assert!(
            response.status.ok,
            "the raft-apply read route must answer, and it failed: {}",
            response.status.message,
        );
        response.response
    }

    // ---- DENOMINATOR: at a leader stamp BEFORE the deadline, both are served -------------
    // Without this, the two "hidden" assertions below would also be produced by the route
    // being broken, the keys never having been written, or the deadline being wrong.
    assert_eq!(
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        },
        applied_at(
            &engine,
            Command::StringGet {
                key: "skew:string".to_string()
            },
            local_now,
        ),
        "DENOMINATOR: at a leader stamp 60s before the deadline the string must be served. It \
         was not, so the rest of this test proves nothing.",
    );
    assert!(
        matches!(
            applied_at(
                &engine,
                Command::HashGetAll {
                    key: "skew:hash".to_string()
                },
                local_now,
            ),
            CommandResponse::HashEntries { ref entries } if !entries.is_empty()
        ),
        "DENOMINATOR: at a leader stamp 60s before the deadline the hash must be served.",
    );

    // ---- HALF ONE: StringGet ------------------------------------------------------------
    assert_eq!(
        CommandResponse::Bytes { value: None },
        applied_at(
            &engine,
            Command::StringGet {
                key: "skew:string".to_string()
            },
            leader_past_deadline,
        ),
        "HALF ONE: the fast path served a string whose deadline had already passed at leader \
         time ({deadline} <= {leader_past_deadline}). It compared the deadline against this \
         node's clock instead of the leader's stamp, decided the key was live, and answered \
         from the fast path -- so the slow path that would have hidden it never ran, and a \
         replica whose clock had passed {deadline} answered the same entry differently.",
    );

    // ---- HALF TWO: HashGetAll -----------------------------------------------------------
    let hash_at_leader_time = applied_at(
        &engine,
        Command::HashGetAll {
            key: "skew:hash".to_string(),
        },
        leader_past_deadline,
    );
    assert!(
        matches!(
            hash_at_leader_time,
            CommandResponse::HashEntries { ref entries } if entries.is_empty()
        ),
        "HALF TWO: the fast path served a hash whose deadline had already passed at leader time \
         ({deadline} <= {leader_past_deadline}); it asked this node's clock instead of the \
         leader's stamp. Got {hash_at_leader_time:?}",
    );

    // ---- HALF THREE: CommonTtl, the remaining-time arithmetic in `ttl_ms` ----------------
    // Not a fast-path read -- `CommonTtl` runs in `execute_on_shard`, whose arm already COLLECTS
    // on the replay-aware clock through `drop_if_expired`. The remaining time it reports beside
    // that collection is the third deadline read in the pair of functions this change touches,
    // and it was asking a different clock than the collection one line above it. A leader stamp
    // 30s after this node's clock, against a deadline 60s after it, makes the right answer
    // exactly 30_000 and the wrong one about 60_000 -- a difference no scheduling jitter can
    // close, and the assertion is exact rather than a range because the leader stamp is fixed.
    let leader_before_deadline = local_now.saturating_add(30_000);
    assert_eq!(
        CommandResponse::Integer { value: 30_000 },
        applied_at(
            &engine,
            Command::CommonTtl {
                key: "ttl:string".to_string()
            },
            leader_before_deadline,
        ),
        "HALF THREE: TTL must be the distance from the LEADER's stamp \
         ({leader_before_deadline}) to the deadline ({deadline}), which is exactly 30000ms. \
         Reading this node's clock instead reports about 60000ms, so one committed entry tells \
         each replica a different remaining time.",
    );
}

/// THE COUNTER AT THE ENTRY POINT: an expiry round under WAL replay really does write a delta
/// that carries no anchor and carries the round's tombstones.
///
/// Every claim about what the index log's reclaim does to such a record rests on this record
/// existing, and the code reads both ways: `if !replaying_wal()` wraps the WAL tombstones AND the
/// line that anchors `shard.applied_wal_sequence`, while the checkpoint that carries the round's
/// key-states is built OUTSIDE it. That is a reading, and a reading of this shape has been valid,
/// clean and wrong here before. So this counts the record instead of arguing about it.
///
/// The shard has seen no write outside the guard, so nothing has ever anchored it: this is a
/// store recovering from its log, which is when the round below runs in production.
///
/// TWO COUNTERS, asserted separately, because each one alone is satisfied by a record that proves
/// nothing. A record with no anchor and no content is the legacy whole-index shape, which the
/// sweep is SUPPOSED to remove. A record with content and an anchor is the ordinary write path.
/// Only both together describe the record the sweep must not drop.
#[test]
fn an_expiry_round_under_replay_writes_a_delta_with_no_anchor() {
    const DUE_KEYS: usize = 8;

    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let records_before;
    let report;
    {
        let _replaying = crate::engine::WalReplayGuard::enter();
        // ONE COMMAND AT A TIME, not `write_keys`. The BATCH path anchors
        // `applied_wal_sequence` under `if !config.async_storage && !bulk_ingest_mode()` alone --
        // it does not ask `replaying_wal()` the way the single-command path does -- so seeding
        // through it leaves the shard anchored and this fixture would be measuring the wrong
        // writer. Measured, before this line was what it is: the round's delta then carries
        // Some(1) rather than no anchor at all.
        for index in 0..DUE_KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: format!("due-{index:04}"),
                    value: vec![b'v'; 16],
                    ttl_ms: 1,
                },
            });
            assert!(response.status.ok, "seed write failed: {:?}", response.status);
        }
        std::thread::sleep(std::time::Duration::from_millis(20));

        // DENOMINATOR, with a floor: a round that finds nothing due writes no record at all, and
        // every assertion below would then be about the absence of a record rather than about
        // its contents.
        let (held, due) = deadline_census(&engine, 1);
        assert_eq!(held, DUE_KEYS, "the fixture did not land: {held} deadlines");
        assert_eq!(due, DUE_KEYS, "{due} of {DUE_KEYS} keys are due");

        records_before = engine
            .index_log_store
            .read_delta_records(1, 0)
            .expect("the index log should read back")
            .len();
        report = sweep_once(&engine, 1);
    }

    assert_eq!(
        report.expired_records_removed, DUE_KEYS,
        "the round removed {} of {DUE_KEYS}, so the record below is not the record of a removal",
        report.expired_records_removed
    );

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

    // COUNTER ONE: no anchor. The `unwrap_or(0)` this is about reads exactly this `None` as
    // anchor 0 -- at or below every anchor -- and calls the record reflected.
    assert!(
        record.applied_wal_sequence.is_none(),
        "the round under replay anchored its delta at {:?}, so the anchor-less record the \
         reclaim paths are guarded against is not produced here after all",
        record.applied_wal_sequence
    );
    // COUNTER TWO, separately: it carries content the fold applies. Without this the record
    // would be the legacy whole-index shape, which the sweep removes on purpose.
    assert!(
        !record.key_states.is_empty(),
        "the anchor-less delta carries no key-states, so it describes none of the {DUE_KEYS} \
         deletions this round made and losing it would lose nothing"
    );
    assert!(
        record.items.is_empty(),
        "fixture: the expiry delta describes its removals with key-states alone, and this one \
         carries {} page items",
        record.items.len()
    );
}

// ===========================================================================================
// THE EXPIRY ROUND'S RECORD-CACHE PASS
//
// PR #1911 rewrote the `delete_drop` round's per-key cache sweep into one batched pass and named
// this sweep as the next one -- "the same per-key shape under the same guard", NAMED, not
// measured. Everything below is measured HERE, on this path, on its own fixtures, at two corpus
// sizes. The two paths turn out to agree closely, which is a result and not an assumption: the
// per-key figures below were produced by running these tests against the unmodified tree first.
// ===========================================================================================

/// The corpus for the cache-pass fixtures. Zero-padded so key order and index order agree.
fn cache_pass_key(index: usize) -> String {
    format!("expiry-cache-key-{index:06}")
}

/// Keys the fixture gives SEVERAL cache entries in every namespace the invalidation covers.
///
/// Two of them are inside the due set and one is OUTSIDE it, so the over-invalidation direction --
/// the one a batched predicate can fail and a per-key sweep cannot -- has a subject.
fn cache_pass_marked(objects: usize) -> Vec<String> {
    vec![
        cache_pass_key(3),
        cache_pass_key(17),
        cache_pass_key(objects - 1),
    ]
}

/// The keys the round will find due: the first half of the corpus.
fn cache_pass_due(objects: usize) -> Vec<String> {
    (0..objects / 2).map(cache_pass_key).collect()
}

/// A corpus whose keys all carry a deadline, READ BACK, with several cache entries per marked key
/// in every namespace the invalidation covers.
///
/// WHY THE DEADLINES ARE BACK-DATED RATHER THAN SHORT, and this is the single most expensive
/// mistake available here. A record cache is populated by READS -- `the_cache_namespaces_a_record_
/// can_actually_use` (engine/tests/part4.rs) says so in as many words -- and a read of a key whose
/// deadline has already passed caches nothing. Writing with a one-millisecond TTL and then warming
/// would therefore produce an EMPTY cache, the sweep would walk zero entries, and the cost would
/// measure at its floor, which reads exactly like a cheap operation. So every key is written with
/// an hour-long deadline, the cache is warmed against live keys through the ordinary command path,
/// and only then are the due keys' deadlines moved into the past with `set_expiry`, which touches
/// both expiry indexes and nothing else -- no cache, no shard records.
///
/// The cold arm omits the warming loop and nothing else, so it is the same corpus with an empty
/// record cache. Its zero is reported rather than hidden: it is what an unwarmed fixture measures
/// this at.
fn cache_pass_fixture(
    objects: usize,
    warm: bool,
    due: &[String],
) -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    write_keys(
        &engine,
        1,
        (0..objects)
            .map(|index| (cache_pass_key(index), 3_600_000u64))
            .collect(),
    );
    if warm {
        for index in 0..objects {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: cache_pass_key(index),
                },
            });
            assert!(out.status.ok, "a warming read failed: {:?}", out.status);
        }
        // Three hash fields and three feature windows per marked key, because the namespaces that
        // get SWEPT are exactly the ones that hold more than one entry per key: a hash caches one
        // entry per field and a feature one per query window. A fixture with one entry each would
        // make "drop the entry" and "drop every entry" the same assertion. The read follows the
        // write in every case, because the write is what invalidates.
        for key in cache_pass_marked(objects) {
            let mut run = |command: Command| {
                let out = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command,
                });
                assert!(out.status.ok, "fixture command failed: {:?}", out.status);
            };
            for field in ["alpha", "beta", "gamma"] {
                run(Command::HashSet {
                    key: key.clone(),
                    field: field.to_string(),
                    value: b"hash-value".to_vec(),
                });
                run(Command::HashGet {
                    key: key.clone(),
                    field: field.to_string(),
                });
            }
            run(Command::SetAdd {
                key: key.clone(),
                member: b"sweep-member".to_vec(),
            });
            run(Command::SetMembers { key: key.clone() });
            run(Command::FeatureAppend {
                key: key.clone(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_000,
                    value: b"feature-value".to_vec(),
                }],
            });
            for (start_ms, end_ms) in [(0u64, 10_000u64), (0, 20_000), (500, 9_000)] {
                run(Command::FeatureQuery {
                    key: key.clone(),
                    start_ms,
                    end_ms,
                    count: None,
                });
            }
            run(Command::StringGet { key: key.clone() });
        }
    }
    let now = now_ms();
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        for key in due {
            crate::engine::set_expiry(shard, key.clone(), now.saturating_sub(1_000));
        }
    }
    (dir, engine)
}

/// The shard's whole record cache, `namespace/record_key/selector`, in listing order.
/// `entries_for_shard` already sorts by those three fields, so two of these compare directly.
fn cache_pass_listing(engine: &TemporalEngine) -> Vec<String> {
    engine
        .cache
        .entries_for_shard(1)
        .into_iter()
        .map(|entry| format!("{}/{}/{}", entry.namespace, entry.record_key, entry.selector))
        .collect()
}

/// An order-sensitive fingerprint of a whole listing. FNV-1a over the joined entries, so two
/// listings of the same length holding different entries do not collide the way a count does.
fn cache_pass_fingerprint(listing: &[String]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for entry in listing {
        for byte in entry.as_bytes().iter().chain(std::iter::once(&b'\n')) {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
    }
    hash
}

fn cache_pass_occupied(listing: &[String], namespace: &str, key: &str) -> usize {
    let prefix = format!("{namespace}/{key}/");
    listing
        .iter()
        .filter(|entry| entry.starts_with(&prefix))
        .count()
}

/// THE NAMESPACES BOTH ARMS COVER, DERIVED FROM THE PRODUCTION AUTHORITY.
///
/// `SWEPT_RECORD_NAMESPACES` and `named_record_keys` are read by the per-key primitive and by the
/// batched pass alike, so this is one list read twice rather than two lists that happen to agree
/// today. It is derived AND floored by its caller -- see the floor at the top of the equivalence
/// test, and the mutant that exists to prove the floor is not decoration.
fn cache_pass_namespaces() -> Vec<String> {
    crate::engine::SWEPT_RECORD_NAMESPACES
        .iter()
        .map(|namespace| namespace.to_string())
        .chain(
            crate::engine::named_record_keys(1, "probe")
                .iter()
                .map(|key| key.namespace.to_string()),
        )
        .collect()
}

/// One expiry round, measured for what its cache pass walks and for where its hold went.
#[derive(Debug)]
struct ExpiryRound {
    objects: usize,
    warm: bool,
    /// What `sweep_expired_records_with_request` RETURNS to its caller. Independent of every
    /// counter below, all of which are read from inside the cache primitives.
    expired: usize,
    scanned: usize,
    /// Counted inside `invalidate_record_all` -- the PER-KEY primitive. Zero here is what says the
    /// round stopped calling it; the primitive itself is still live at six other call sites.
    calls: u64,
    sweeps: u64,
    entries_walked: u64,
    named: u64,
    /// Counted inside `invalidate_records_all_batched` -- the ONE pass the round now makes.
    batched_calls: u64,
    keys_batched: u64,
    listings: u64,
    entries_listed: u64,
    entries_returned: u64,
    /// Read OUTSIDE, off the cache itself, either side of the whole call.
    cache_entries_before: usize,
    cache_entries_after: usize,
    /// A fingerprint of the WHOLE shard listing the round left behind, in
    /// `namespace/record_key/selector` form. This is what makes the ROUND's end state comparable
    /// across two BUILDS -- the A/B/B/A slots below swap the production shape and rebuild between
    /// them, so no single process can hold both arms. A count alone would not distinguish two
    /// caches of the same size holding different entries.
    cache_after_fingerprint: u64,
    hold_ns: u64,
    select_ns: u64,
    delete_ns: u64,
    invalidate_ns: u64,
    wal_ns: u64,
    checkpoint_ns: u64,
    unattributed_ns: u64,
}

impl ExpiryRound {
    fn walked_per_key(&self) -> f64 {
        if self.expired == 0 {
            0.0
        } else {
            self.entries_walked as f64 / self.expired as f64
        }
    }

    /// Cache entries the round's ONE pass stepped over, per key it expired. The quantity that was
    /// 2,818 and 22,067 under the per-key shape -- twice the cache, growing with the store.
    fn listed_per_key(&self) -> f64 {
        if self.expired == 0 {
            0.0
        } else {
            self.entries_listed as f64 / self.expired as f64
        }
    }

    /// Keys the round's cache pass was handed that its own expired count does not explain.
    ///
    /// INDEPENDENT, and that is the whole point of it: `keys_batched` is counted inside
    /// `invalidate_records_all_batched`, the primitive that does the invalidating, and `expired`
    /// is the number `sweep_expired_records_with_request` RETURNS to its caller. Neither is
    /// derived from the other, so this is not an identity -- a second invalidating path inside the
    /// round, or a key invalidated that the round did not report expiring, would show here. It is
    /// asserted ACROSS the two corpus sizes rather than against a constant.
    fn pass_residual(&self) -> u64 {
        self.calls
            .saturating_add(self.keys_batched)
            .saturating_sub(self.expired as u64)
    }

    fn pct(&self, part: u64) -> f64 {
        if self.hold_ns == 0 {
            0.0
        } else {
            part as f64 * 100.0 / self.hold_ns as f64
        }
    }
}

/// `armed` turns on the per-walk tier-length read that produces `entries_walked` /
/// `entries_listed`. Every arm that reports a TIME is run disarmed, so the apparatus that explains
/// the hold is never inside the hold it explains.
fn expiry_round_limited(
    objects: usize,
    warm: bool,
    armed: bool,
    hot_limit: usize,
    cold_limit: usize,
) -> ExpiryRound {
    let sweep = &crate::engine::CACHE_SWEEP_COUNTS;
    let nanos = &crate::engine::recovery_sweep_compact::EXPIRY_GUARD_NANOS;
    let (_dir, engine) = cache_pass_fixture(objects, warm, &cache_pass_due(objects));
    let cache_entries_before = engine.cache.entries_for_shard(1).len();
    sweep.reset();
    sweep.set_armed(armed);
    nanos.reset();
    let report = engine
        .sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id: 1,
            load_cold_buckets: true,
            max_hot_buckets_per_round: hot_limit,
            max_cold_buckets_per_round: cold_limit,
            ..ShardExpirySweepRequest::default()
        })
        .expect("shard 1 is loaded");
    sweep.set_armed(false);
    let after_listing = cache_pass_listing(&engine);
    let cache_entries_after = after_listing.len();
    let cache_after_fingerprint = cache_pass_fingerprint(&after_listing);
    let (calls, sweeps, entries_walked, named) = sweep.read();
    let (batched_calls, keys_batched, listings, entries_listed, entries_returned) =
        sweep.read_batched();
    let (hold_ns, select_ns, delete_ns, invalidate_ns, wal_ns, checkpoint_ns) = nanos.read();
    ExpiryRound {
        objects,
        warm,
        expired: report.expired_records_removed,
        scanned: report.scanned_records,
        calls,
        sweeps,
        entries_walked,
        named,
        batched_calls,
        keys_batched,
        listings,
        entries_listed,
        entries_returned,
        cache_entries_before,
        cache_entries_after,
        cache_after_fingerprint,
        hold_ns,
        select_ns,
        delete_ns,
        invalidate_ns,
        wal_ns,
        checkpoint_ns,
        unattributed_ns: nanos.unattributed(),
    }
}

/// The unbounded round: 0 means no limit, which is what `sweep_expired_records` -- and therefore
/// `sweep_all_expired_records` -- passes in production.
fn expiry_round(objects: usize, warm: bool, armed: bool) -> ExpiryRound {
    expiry_round_limited(objects, warm, armed, 0, 0)
}

/// WHAT AN EXPIRY ROUND'S CACHE PASS WALKS, COLD AND WARM, AT TWO CORPUS SIZES.
///
/// WHAT THIS MEASURED BEFORE THE CHANGE, on the unmodified tree, same fixtures, same arms:
///
/// ```text
///                                   cold 500     WARM 500    cold 4000    WARM 4000
/// cache entries BEFORE (outer)             0        1,036            0        8,036
/// keys the round expired                 250          250        2,000        2,000
/// PER-KEY invalidate_record_all          250          250        2,000        2,000
/// walks (invalidate_record)              500          500        4,000        4,000
/// CACHE ENTRIES WALKED                     0      704,548            0   44,134,298
///  .. walked per expired key             0.0      2,818.2          0.0     22,067.1
/// cache entries AFTER  (outer)             0          772            0        6,022
/// ```
///
/// TWO SIZES AND THE RATIO, which is the whole shape claim. The per-key cost goes 2,818.2 to
/// 22,067.1 over an eightfold corpus -- 7.83x -- because each expired key walks two whole cache
/// lengths. What one key costs to expire was a property of the STORE, not of the round. One pass
/// spreads a single cache length over every key the round expires instead, so the same column
/// reads 6.2 and 6.0: flat, and asserted flat below in both directions.
///
/// WHY THIS IS A COUNT AND NOT A TIME. `MultiLayerCache::invalidate_record` chains the key sets of
/// all three cache tiers and filters, so the work is ENTRIES WALKED and it is exactly the sum of
/// the three tier lengths. `note_sweep` reads those three lengths immediately before each walk and
/// `note_listing` reads the SAME three before each listing, so the two arms are measured in one
/// unit and the ratio between them is a ratio. A time on this box is a fact about the box: the
/// nanosecond rows are printed, with the load, and nothing is asserted about them.
///
/// THE COLD ARM LISTS NONE, and that is a finding rather than a nuisance. A fixture that writes a
/// corpus and never reads it back has an EMPTY record cache, so it measures this sweep at zero --
/// which reads exactly like a cheap operation. Every arm that makes a claim here is warm, the
/// cache size is read from OUTSIDE the round either side of it, and the warm arm is asserted to
/// hold more entries than the cold one before anything else is asserted.
// rust-internal: cache-entry visit counts inside the Rust MultiLayerCache primitives
#[test]
fn what_an_expiry_rounds_cache_sweep_walks_cold_and_warm() {
    const SMALL: usize = 500;
    const LARGE: usize = 4000;

    // COUNTS from the armed runs; TIMINGS from the disarmed ones. Same fixture, same round.
    let cold_small = expiry_round(SMALL, false, true);
    let warm_small = expiry_round(SMALL, true, true);
    let cold_large = expiry_round(LARGE, false, true);
    let warm_large = expiry_round(LARGE, true, true);

    println!(
        "\n  THE EXPIRY ROUND'S CACHE PASS, cold cache vs warm cache\n\
         \n                                  {:>12} {:>12} {:>12} {:>12}\n\
           corpus objects              {:>12} {:>12} {:>12} {:>12}\n\
           cache entries BEFORE (outer){:>12} {:>12} {:>12} {:>12}\n\
           keys the round expired      {:>12} {:>12} {:>12} {:>12}\n\
           records scanned             {:>12} {:>12} {:>12} {:>12}\n\
           PER-KEY invalidate_record_all{:>11} {:>12} {:>12} {:>12}\n\
           PER-KEY walks               {:>12} {:>12} {:>12} {:>12}\n\
           CACHE ENTRIES WALKED        {:>12} {:>12} {:>12} {:>12}\n\
           .. walked per expired key   {:>12.1} {:>12.1} {:>12.1} {:>12.1}\n\
           named-key invalidations     {:>12} {:>12} {:>12} {:>12}\n\
           BATCHED passes              {:>12} {:>12} {:>12} {:>12}\n\
           keys handed to the pass     {:>12} {:>12} {:>12} {:>12}\n\
           .. residual, keys - expired {:>12} {:>12} {:>12} {:>12}\n\
           listings those passes made  {:>12} {:>12} {:>12} {:>12}\n\
           CACHE ENTRIES LISTED        {:>12} {:>12} {:>12} {:>12}\n\
           .. listed per expired key   {:>12.1} {:>12.1} {:>12.1} {:>12.1}\n\
           entries the listing RETURNED{:>12} {:>12} {:>12} {:>12}\n\
           cache entries AFTER  (outer){:>12} {:>12} {:>12} {:>12}\n",
        "cold 500", "WARM 500", "cold 4000", "WARM 4000",
        cold_small.objects, warm_small.objects, cold_large.objects, warm_large.objects,
        cold_small.cache_entries_before, warm_small.cache_entries_before,
        cold_large.cache_entries_before, warm_large.cache_entries_before,
        cold_small.expired, warm_small.expired, cold_large.expired, warm_large.expired,
        cold_small.scanned, warm_small.scanned, cold_large.scanned, warm_large.scanned,
        cold_small.calls, warm_small.calls, cold_large.calls, warm_large.calls,
        cold_small.sweeps, warm_small.sweeps, cold_large.sweeps, warm_large.sweeps,
        cold_small.entries_walked, warm_small.entries_walked,
        cold_large.entries_walked, warm_large.entries_walked,
        cold_small.walked_per_key(), warm_small.walked_per_key(),
        cold_large.walked_per_key(), warm_large.walked_per_key(),
        cold_small.named, warm_small.named, cold_large.named, warm_large.named,
        cold_small.batched_calls, warm_small.batched_calls,
        cold_large.batched_calls, warm_large.batched_calls,
        cold_small.keys_batched, warm_small.keys_batched,
        cold_large.keys_batched, warm_large.keys_batched,
        cold_small.pass_residual(), warm_small.pass_residual(),
        cold_large.pass_residual(), warm_large.pass_residual(),
        cold_small.listings, warm_small.listings, cold_large.listings, warm_large.listings,
        cold_small.entries_listed, warm_small.entries_listed,
        cold_large.entries_listed, warm_large.entries_listed,
        cold_small.listed_per_key(), warm_small.listed_per_key(),
        cold_large.listed_per_key(), warm_large.listed_per_key(),
        cold_small.entries_returned, warm_small.entries_returned,
        cold_large.entries_returned, warm_large.entries_returned,
        cold_small.cache_entries_after, warm_small.cache_entries_after,
        cold_large.cache_entries_after, warm_large.cache_entries_after,
    );

    // THE FIXTURE HAS A POPULATED CACHE, asserted BEFORE anything else, because a pass over an
    // empty cache measures at its floor and that reads exactly like a cheap operation.
    assert!(
        warm_small.cache_entries_before > cold_small.cache_entries_before,
        "the warm fixture must hold more cache entries than the cold one before the round, or \
         nothing below is measuring a cache pass at all: {} warm vs {} cold",
        warm_small.cache_entries_before, cold_small.cache_entries_before,
    );
    assert!(
        warm_large.cache_entries_before > cold_large.cache_entries_before,
        "the warm fixture must hold more cache entries than the cold one before the round: \
         {} warm vs {} cold",
        warm_large.cache_entries_before, cold_large.cache_entries_before,
    );
    assert!(
        warm_large.cache_entries_before > warm_small.cache_entries_before,
        "the warm cache must grow with the corpus, or a per-key cost could not have been a \
         property of the store: {} at {SMALL} objects vs {} at {LARGE}",
        warm_small.cache_entries_before, warm_large.cache_entries_before,
    );
    for round in [&cold_small, &warm_small, &cold_large, &warm_large] {
        assert!(
            round.expired > 0,
            "the round expired nothing, so nothing was measured: {round:?}",
        );
        // ONE PASS, whatever the round expired, and NO per-key sweeps left in this path.
        assert_eq!(
            round.batched_calls, 1,
            "a round that expired {} keys must make exactly ONE batched cache pass, not {}",
            round.expired, round.batched_calls,
        );
        assert_eq!(
            round.listings, 1,
            "a round that expired {} keys must list the cache exactly ONCE, not {} times",
            round.expired, round.listings,
        );
        assert_eq!(
            round.calls, 0,
            "the expiry round must no longer call the PER-KEY invalidate_record_all; it made {} \
             calls, so the per-key loop is back",
            round.calls,
        );
        assert_eq!(
            round.sweeps, 0,
            "the expiry round must make no per-key cache walks at all; it made {}",
            round.sweeps,
        );
    }

    // THE PER-KEY QUANTITY NO LONGER MOVES WITH THE CORPUS, in both directions. Under the per-key
    // shape this column was 2,818.2 and 22,067.1; a regression to anything that grows with the
    // store fails here on the number.
    assert!(
        warm_large.listed_per_key() <= warm_small.listed_per_key(),
        "cache entries listed PER EXPIRED KEY must not grow with the corpus: {:.1} at {SMALL} \
         objects and {:.1} at {LARGE}. Under the per-key shape this was 2,818.2 and 22,067.1.",
        warm_small.listed_per_key(), warm_large.listed_per_key(),
    );
    assert!(
        warm_small.listed_per_key() < 100.0 && warm_large.listed_per_key() < 100.0,
        "cache entries listed PER EXPIRED KEY must be a small constant, not a cache length: \
         {:.1} and {:.1}",
        warm_small.listed_per_key(), warm_large.listed_per_key(),
    );

    // THE RESIDUAL, INDEPENDENT AND ACROSS THE SIZES. `keys handed to the pass` is counted inside
    // the cache primitive; `keys the round expired` is what the sweep returns to its caller.
    // Asserted EQUAL BETWEEN THE TWO SIZES, not against a constant, so a second invalidating path
    // in no row of the table above would show as a residual that scaled.
    assert_eq!(
        warm_small.pass_residual(), warm_large.pass_residual(),
        "the residual between what the cache pass was handed and what the round reports expiring \
         moved with the corpus: {} at {SMALL} objects and {} at {LARGE}",
        warm_small.pass_residual(), warm_large.pass_residual(),
    );
    assert_eq!(
        warm_large.pass_residual(), 0,
        "the cache pass was handed {} keys the round does not report expiring",
        warm_large.pass_residual(),
    );
    // The named-key invalidations are two per expired key in both shapes -- they are O(1) and were
    // never the cost. A pass that stopped naming them would fail here rather than silently leave
    // `string` and `set` entries behind.
    assert_eq!(
        warm_large.named as usize, warm_large.expired * 2,
        "every expired key must still name its two O(1) cache entries: {} named for {} keys",
        warm_large.named, warm_large.expired,
    );
}

/// THE ROUND THE STORAGE MANAGER ACTUALLY RUNS, at the default bounds, also makes ONE pass.
///
/// `sweep_expired_records` is unbounded and that is a real production path, but the periodic
/// stage in `data_node.rs` passes `DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND` / `..COLD..` -- 128
/// and 8. A bounded round expires fewer keys, so it had FEWER per-key walks to remove; what does
/// not change is that each of those walks was a whole cache length. At the bound, on the warm
/// 4,000-object fixture, the per-key shape cost 128 keys x 2 walks x ~8,000 entries. The batched
/// shape costs one listing whatever the bound is, and this is what says so.
// rust-internal: Rust storage-manager round bounds against the Rust cache listing
#[test]
fn a_bounded_expiry_round_makes_one_cache_pass_too() {
    const OBJECTS: usize = 4000;
    let round = expiry_round_limited(OBJECTS, true, true, HOT_LIMIT, COLD_LIMIT);
    // AND THE SMALLEST ROUND THERE IS, because that is where the trade runs closest. One listing
    // costs the same whatever the round expires, so a round of ONE key pays a whole listing plus
    // its `statx` to remove what two per-key walks would have removed. The entry-visit column
    // still favours the pass -- one cache length against two -- and the syscalls are what is
    // bought with it. Printed, not asserted: it is the price of the choice, not a claim.
    let single = expiry_round_limited(OBJECTS, true, true, 1, 0);
    println!(
        "  BOUNDED ROUND (hot {HOT_LIMIT}, cold {COLD_LIMIT}) on a warm {OBJECTS}-object store: \
         expired {} scanned {} cache_before {} batched_passes {} listings {} entries_listed {} \
         per_key {:.1} per_key_sweeps {} entries_walked {}",
        round.expired, round.scanned, round.cache_entries_before, round.batched_calls,
        round.listings, round.entries_listed, round.listed_per_key(), round.sweeps,
        round.entries_walked,
    );
    println!(
        "  ONE-KEY ROUND on the same warm {OBJECTS}-object store: expired {} cache_before {} \
         batched_passes {} listings {} entries_listed {} entries_returned {} \
         (the per-key shape would have stepped {} and made no syscall)",
        single.expired, single.cache_entries_before, single.batched_calls, single.listings,
        single.entries_listed, single.entries_returned,
        single.cache_entries_before.saturating_mul(2),
    );
    assert_eq!(
        single.batched_calls, 1,
        "even a one-key round makes exactly one pass, not {}",
        single.batched_calls,
    );
    assert!(
        round.cache_entries_before > 0,
        "the bounded round's fixture must have a populated cache, or it measures nothing",
    );
    assert_eq!(
        round.expired, HOT_LIMIT,
        "a bounded round must expire exactly its hot bound from a corpus with more due than that; \
         it expired {}",
        round.expired,
    );
    assert_eq!(
        round.batched_calls, 1,
        "a bounded round must make exactly ONE cache pass, not {}",
        round.batched_calls,
    );
    assert_eq!(
        round.sweeps, 0,
        "a bounded round must make no per-key cache walks; it made {}",
        round.sweeps,
    );
}

/// WHERE AN EXPIRY ROUND'S SHARD-TABLE WRITE HOLD GOES.
///
/// The round reports what it REMOVED and never what it HELD, and the `shards` write guard is the
/// one lock that excludes every reader and every writer on the shard -- so "how long" is the
/// question a serving operator actually has. `ExpiryGuardNanos` is the seam; this prints it.
///
/// ASSERTS ALMOST NOTHING ON PURPOSE. A time on this box is a fact about the box: the same arm was
/// seen to vary by more than 2x within one session at different loads, so this test exists to be
/// READ, and to be runnable unchanged against a mutant that restores the per-key shape so the two
/// can be compared A/B/B/A with a rebuild between slots. What it does assert is that the apparatus
/// produced numbers at all -- a hold of zero, or a round that expired nothing, would make every
/// printed share meaningless.
///
/// The load is printed with the numbers, because a hold measured above load ~24 on this box is not
/// a measurement of anything.
// rust-internal: phase timings of the Rust shard-table write guard
#[test]
fn where_an_expiry_rounds_write_hold_goes() {
    const SMALL: usize = 500;
    const LARGE: usize = 4000;

    // DISARMED: the tier-length reads that size the pass are apparatus, and apparatus does not
    // belong inside the hold it explains.
    let small = expiry_round(SMALL, true, false);
    let large = expiry_round(LARGE, true, false);

    println!(
        "\n  WHERE AN EXPIRY ROUND'S WRITE HOLD WENT (load {})\n\
         \n                            {:>14} {:>14}\n\
           corpus objects        {:>14} {:>14}\n\
           cache entries before  {:>14} {:>14}\n\
           keys expired          {:>14} {:>14}\n\
           GUARD HELD, total us  {:>14} {:>14}\n\
           .. select (due_window){:>14} {:>14}   {:>5.1}% {:>5.1}%\n\
           .. delete_record      {:>14} {:>14}   {:>5.1}% {:>5.1}%\n\
           .. CACHE PASS         {:>14} {:>14}   {:>5.1}% {:>5.1}%\n\
           .. wal tombstones     {:>14} {:>14}   {:>5.1}% {:>5.1}%\n\
           .. checkpoint build   {:>14} {:>14}   {:>5.1}% {:>5.1}%\n\
           .. UNATTRIBUTED       {:>14} {:>14}   {:>5.1}% {:>5.1}%\n",
        std::fs::read_to_string("/proc/loadavg")
            .unwrap_or_default()
            .split_whitespace()
            .next()
            .unwrap_or("?")
            .to_string(),
        "WARM 500", "WARM 4000",
        small.objects, large.objects,
        small.cache_entries_before, large.cache_entries_before,
        small.expired, large.expired,
        small.hold_ns / 1_000, large.hold_ns / 1_000,
        small.select_ns / 1_000, large.select_ns / 1_000,
        small.pct(small.select_ns), large.pct(large.select_ns),
        small.delete_ns / 1_000, large.delete_ns / 1_000,
        small.pct(small.delete_ns), large.pct(large.delete_ns),
        small.invalidate_ns / 1_000, large.invalidate_ns / 1_000,
        small.pct(small.invalidate_ns), large.pct(large.invalidate_ns),
        small.wal_ns / 1_000, large.wal_ns / 1_000,
        small.pct(small.wal_ns), large.pct(large.wal_ns),
        small.checkpoint_ns / 1_000, large.checkpoint_ns / 1_000,
        small.pct(small.checkpoint_ns), large.pct(large.checkpoint_ns),
        small.unattributed_ns / 1_000, large.unattributed_ns / 1_000,
        small.pct(small.unattributed_ns), large.pct(large.unattributed_ns),
    );
    // THE ROUND'S END STATE, AS A FINGERPRINT OF THE WHOLE LISTING. The A/B/B/A slots swap the
    // production shape and REBUILD between them, so no single process can hold both arms and no
    // assertion inside this process can compare them. Printing the fingerprint makes the ROUND's
    // end state -- not just its entry count, and not just the primitive's -- comparable across the
    // four slots: the per-key arm and the batched arm must leave caches that are equal entry for
    // entry, and two different caches of the same size would collide on a count and not on this.
    println!(
        "  PROBE-EXPIRY-HOLD small_hold_us={} small_pass_us={} large_hold_us={} \
         large_pass_us={} small_expired={} large_expired={} \
         small_after={} small_fingerprint={:016x} large_after={} large_fingerprint={:016x} \
         load={}",
        small.hold_ns / 1_000, small.invalidate_ns / 1_000,
        large.hold_ns / 1_000, large.invalidate_ns / 1_000,
        small.expired, large.expired,
        small.cache_entries_after, small.cache_after_fingerprint,
        large.cache_entries_after, large.cache_after_fingerprint,
        std::fs::read_to_string("/proc/loadavg")
            .unwrap_or_default()
            .split_whitespace()
            .next()
            .unwrap_or("?")
            .to_string(),
    );

    for round in [&small, &large] {
        assert!(
            round.expired > 0,
            "the round expired nothing, so the hold below describes an empty round: {round:?}",
        );
        assert!(
            round.hold_ns > 0,
            "the guard-hold clock produced zero, so every share printed above is meaningless",
        );
        assert!(
            round.cache_entries_before > 0,
            "the fixture's cache is empty, so the cache pass's share is its floor and not its \
             value",
        );
        // THE POSITIVE CONTROL FOR THE SEAM ITSELF. A phase timer that never fires reads as a
        // phase that costs nothing, which is indistinguishable from a phase that got cheaper --
        // and it would make every share above wrong in the flattering direction.
        assert!(
            round.invalidate_ns > 0,
            "the cache pass's phase timer produced zero for a round that expired {} keys against \
             {} cache entries; the seam is not wired, so the table above is measuring nothing",
            round.expired, round.cache_entries_before,
        );
        assert!(
            round.delete_ns > 0 && round.select_ns > 0,
            "the delete and select phase timers must also produce numbers, or the cache pass's \
             share is being compared against absent rows: {round:?}",
        );
        // The residual is what a phase table cannot fake: `total` is timed by its own clock, not
        // summed from the rows, so work belonging to no phase lands here and is visible.
        assert!(
            round.unattributed_ns < round.hold_ns,
            "the unattributed remainder cannot exceed the hold it is a remainder of",
        );
    }
}

/// What one arm of the comparison did, and what it left in the cache.
#[derive(Debug)]
struct ExpiryCachePassArm {
    arm: &'static str,
    objects: usize,
    expired: usize,
    /// Cache entries the arm stepped over. SAME DEFINITION in both arms -- the sum of the three
    /// tier lengths, read inside the primitive immediately before each walk, by `note_sweep` in
    /// one arm and `note_listing` in the other.
    stepped: u64,
    /// `invalidate_record` calls in the per-key arm, `entries_for_shard` listings in the batched
    /// one.
    walks: u64,
    /// Entries the batched arm's listing RETURNED; zero in the per-key arm, which lists nothing.
    /// This is the upper bound on the arm's filesystem `metadata()` calls.
    returned: u64,
    before: Vec<String>,
    after: Vec<String>,
}

impl ExpiryCachePassArm {
    fn stepped_per_key(&self) -> f64 {
        if self.expired == 0 {
            0.0
        } else {
            self.stepped as f64 / self.expired as f64
        }
    }
}

/// The two arms, on two fixtures built by the same function from the same corpus size, handed the
/// same set of expired keys.
fn expiry_cache_pass_arms(objects: usize) -> (ExpiryCachePassArm, ExpiryCachePassArm) {
    let sweep = &crate::engine::CACHE_SWEEP_COUNTS;
    let expired_keys = cache_pass_due(objects);

    let per_key = {
        let (_dir, engine) = cache_pass_fixture(objects, true, &expired_keys);
        let before = cache_pass_listing(&engine);
        sweep.reset();
        sweep.set_armed(true);
        for key in &expired_keys {
            crate::engine::invalidate_record_all(&engine.cache, 1, key, sweep);
        }
        sweep.set_armed(false);
        let (_calls, walks, stepped, _named) = sweep.read();
        ExpiryCachePassArm {
            arm: "N per-key sweeps",
            objects,
            expired: expired_keys.len(),
            stepped,
            walks,
            returned: 0,
            before,
            after: cache_pass_listing(&engine),
        }
    };

    let batched = {
        let (_dir, engine) = cache_pass_fixture(objects, true, &expired_keys);
        let before = cache_pass_listing(&engine);
        sweep.reset();
        sweep.set_armed(true);
        crate::engine::invalidate_records_all_batched(&engine.cache, 1, &expired_keys, sweep);
        sweep.set_armed(false);
        let (_batched_calls, _keys_batched, walks, stepped, returned) = sweep.read_batched();
        ExpiryCachePassArm {
            arm: "ONE batched pass",
            objects,
            expired: expired_keys.len(),
            stepped,
            walks,
            returned,
            before,
            after: cache_pass_listing(&engine),
        }
    };

    (per_key, batched)
}

/// ONE PASS OVER THE CACHE, PRICED AGAINST THE N IT REPLACES, AND PROVED TO REMOVE THE SAME SET.
///
/// THE TWO ARMS, ON MATCHED FIXTURES. Both are built by `cache_pass_fixture` from the same corpus
/// size with the same warming and handed the same expired-key set, and both are measured in the
/// SAME unit -- cache entries stepped over, read from the three tier lengths inside the primitive
/// immediately before each walk -- so the ratio between the arms is a ratio and not a comparison
/// of two different quantities.
///
/// EXACTLY THE SAME SET, and this is the correctness core rather than a sanity check. What is
/// compared is not "did the expired keys go" -- an arm that emptied the whole cache would satisfy
/// that -- but the WHOLE shard listing afterwards, entry for entry, in
/// `namespace/record_key/selector` form. Both arms match exactly four things:
///
///   - `CacheKey::string(shard, key)`, selector "value"        -- NAMED, O(1), per expired key
///   - `CacheKey::set_members(shard, key)`, selector "members" -- NAMED, O(1), per expired key
///   - namespace `hash`, ANY selector, expired record key      -- SWEPT
///   - namespace `feature`, ANY selector, expired record key   -- SWEPT
///
/// and nothing else. Both take the named pair from `named_record_keys` and the swept list from
/// `SWEPT_RECORD_NAMESPACES`, so the enumeration is one list read twice rather than two lists that
/// happen to agree today.
///
/// KEYS THE ROUND DID NOT EXPIRE KEEP THEIR ENTRIES. `expiry-cache-key-{objects - 1}` is marked,
/// so it is cached in every namespace, and its deadline is NOT back-dated. Both arms must leave
/// every one of its entries alone, and the count is asserted EQUAL to what it was before the arm
/// ran -- not merely non-zero, because an arm that removed two of three hash fields would pass a
/// non-zero check. This is the OVER-invalidation direction: the one a batched predicate can fail
/// and a per-key sweep cannot.
///
/// THE POSITIVE CONTROL RUNS FIRST, per namespace, per arm, per size. Everything asserted after
/// the arms is an emptiness or an equality claim, and two empty listings are equal.
// rust-internal: set equality between two Rust cache-invalidation primitives
#[test]
fn one_expiry_pass_walks_the_cache_once_instead_of_twice_per_expired_key() {
    const SMALL: usize = 500;
    const LARGE: usize = 4000;

    let namespaces = cache_pass_namespaces();
    // THE FLOOR ON THE DERIVED LIST, and it is not decoration. Deriving the subject list from the
    // production authority is what stops it going stale -- but deriving cuts both ways: a change
    // that REMOVES a namespace from the authority removes it from this test at the same moment,
    // and the test then passes by checking one thing fewer. So the list is derived AND floored.
    // Adding a namespace is free; losing one fails here. The mutant that removes `feature` from
    // `SWEPT_RECORD_NAMESPACES` exists to prove this floor bites.
    for required in ["hash", "feature", "string", "set"] {
        assert!(
            namespaces.iter().any(|namespace| namespace == required),
            "`{required}` is no longer in the namespace list this test derives from \
             SWEPT_RECORD_NAMESPACES and named_record_keys, so nothing here checks it any more; \
             the list is {namespaces:?}",
        );
    }
    assert!(
        namespaces.len() >= 4,
        "the derived namespace list must cover at least the four this sweep has always covered; \
         it is {namespaces:?}",
    );

    let (small_per_key, small_batched) = expiry_cache_pass_arms(SMALL);
    let (large_per_key, large_batched) = expiry_cache_pass_arms(LARGE);

    let ratio = |per_key: &ExpiryCachePassArm, batched: &ExpiryCachePassArm| {
        if batched.stepped == 0 {
            0.0
        } else {
            per_key.stepped as f64 / batched.stepped as f64
        }
    };
    println!(
        "\n  ONE PASS OVER THE CACHE vs N PER-KEY SWEEPS, same fixture, same expired keys\n\
         \n                              {:>13} {:>13} {:>13} {:>13}\n\
           corpus objects          {:>13} {:>13} {:>13} {:>13}\n\
           cache entries BEFORE    {:>13} {:>13} {:>13} {:>13}\n\
           keys expired            {:>13} {:>13} {:>13} {:>13}\n\
           walks over the cache    {:>13} {:>13} {:>13} {:>13}\n\
           ENTRIES STEPPED OVER    {:>13} {:>13} {:>13} {:>13}\n\
           .. per expired key      {:>13.1} {:>13.1} {:>13.1} {:>13.1}\n\
           listing RETURNED        {:>13} {:>13} {:>13} {:>13}\n\
           cache entries AFTER     {:>13} {:>13} {:>13} {:>13}\n\
         \n           ENTRY VISITS REMOVED{:>26.1}x{:>27.1}x\n",
        "per-key 500", "BATCHED 500", "per-key 4000", "BATCHED 4000",
        small_per_key.objects, small_batched.objects, large_per_key.objects, large_batched.objects,
        small_per_key.before.len(), small_batched.before.len(),
        large_per_key.before.len(), large_batched.before.len(),
        small_per_key.expired, small_batched.expired, large_per_key.expired, large_batched.expired,
        small_per_key.walks, small_batched.walks, large_per_key.walks, large_batched.walks,
        small_per_key.stepped, small_batched.stepped, large_per_key.stepped, large_batched.stepped,
        small_per_key.stepped_per_key(), small_batched.stepped_per_key(),
        large_per_key.stepped_per_key(), large_batched.stepped_per_key(),
        small_per_key.returned, small_batched.returned,
        large_per_key.returned, large_batched.returned,
        small_per_key.after.len(), small_batched.after.len(),
        large_per_key.after.len(), large_batched.after.len(),
        ratio(&small_per_key, &small_batched), ratio(&large_per_key, &large_batched),
    );

    for (per_key, batched, objects) in [
        (&small_per_key, &small_batched, SMALL),
        (&large_per_key, &large_batched, LARGE),
    ] {
        let kept = cache_pass_key(objects - 1);
        let expired_marked = [cache_pass_key(3), cache_pass_key(17)];

        // THE POSITIVE CONTROL, FIRST. Per namespace, per arm.
        for arm in [per_key, batched] {
            assert!(
                !arm.before.is_empty(),
                "{} at {objects} objects started with an EMPTY cache, so every emptiness claim \
                 below would pass against anything",
                arm.arm,
            );
            for namespace in &namespaces {
                for key in expired_marked.iter().chain(std::iter::once(&kept)) {
                    assert!(
                        cache_pass_occupied(&arm.before, namespace, key) > 0,
                        "{} at {objects} objects: `{namespace}` holds no entry for `{key}` before \
                         the arm ran, so nothing here tests that namespace",
                        arm.arm,
                    );
                }
            }
        }
        // Both arms start from the same cache, or nothing they end with can be compared.
        assert_eq!(
            per_key.before, batched.before,
            "the two fixtures at {objects} objects did not start identical: {} vs {} entries",
            per_key.before.len(), batched.before.len(),
        );

        // EXACTLY THE SAME SET: the WHOLE listing afterwards, entry for entry.
        assert_eq!(
            per_key.after, batched.after,
            "at {objects} objects the two arms left DIFFERENT caches behind: {} entries after the \
             per-key sweeps and {} after the batched pass. Only in per-key: {:?}. Only in \
             batched: {:?}",
            per_key.after.len(),
            batched.after.len(),
            per_key.after.iter().filter(|e| !batched.after.contains(e)).collect::<Vec<_>>(),
            batched.after.iter().filter(|e| !per_key.after.contains(e)).collect::<Vec<_>>(),
        );

        // The expired keys are GONE from every namespace, in both arms.
        for arm in [per_key, batched] {
            for namespace in &namespaces {
                for key in &expired_marked {
                    assert_eq!(
                        cache_pass_occupied(&arm.after, namespace, key), 0,
                        "{} at {objects} objects left `{namespace}` entries cached for the \
                         expired key `{key}`",
                        arm.arm,
                    );
                }
            }
        }

        // AND THE KEY THE ROUND DID NOT EXPIRE KEEPS EXACTLY WHAT IT HAD -- equal, not non-zero.
        for arm in [per_key, batched] {
            for namespace in &namespaces {
                let before = cache_pass_occupied(&arm.before, namespace, &kept);
                let after = cache_pass_occupied(&arm.after, namespace, &kept);
                assert_eq!(
                    before, after,
                    "{} at {objects} objects OVER-INVALIDATED: `{kept}` was never expired, but \
                     its `{namespace}` entry count went {before} -> {after}",
                    arm.arm,
                );
            }
        }

        // ONE WALK, NOT 2N.
        assert_eq!(
            batched.walks, 1,
            "the batched arm at {objects} objects made {} walks over the cache, not one",
            batched.walks,
        );
        assert_eq!(
            per_key.walks as usize, per_key.expired * 2,
            "the per-key arm at {objects} objects must make two walks per expired key; it made {}",
            per_key.walks,
        );
        assert!(
            batched.stepped < per_key.stepped,
            "the batched arm stepped over {} cache entries and the per-key arm {} at {objects} \
             objects -- the pass is not cheaper",
            batched.stepped, per_key.stepped,
        );
    }

    // THE SHAPE, ACROSS THE SIZES. The per-key arm's cost PER EXPIRED KEY grows with the corpus;
    // the batched arm's does not. That, not the multiple, is the finding.
    assert!(
        large_per_key.stepped_per_key() > small_per_key.stepped_per_key() * 4.0,
        "the per-key arm's cost per expired key must grow with the corpus -- that is what makes it \
         a property of the store: {:.1} at {SMALL} objects and {:.1} at {LARGE}",
        small_per_key.stepped_per_key(), large_per_key.stepped_per_key(),
    );
    assert!(
        large_batched.stepped_per_key() <= small_batched.stepped_per_key(),
        "the batched arm's cost per expired key must NOT grow with the corpus: {:.1} at {SMALL} \
         objects and {:.1} at {LARGE}",
        small_batched.stepped_per_key(), large_batched.stepped_per_key(),
    );
}

/// THE READ-VISIBILITY ARGUMENT FOR THIS PATH, as a test rather than as prose.
///
/// The obvious way to get the cache pass out of the `shards` write guard is to hoist it past
/// `drop(shards)`. It compiles unchanged -- the pass is handed `&MultiLayerCache` and a key slice
/// and borrows nothing from `shard` -- and it takes the whole pass out of the hold.
///
/// It is still wrong, for the reason #1907 established on the sibling path, and this is the test
/// that makes it wrong HERE rather than by inheritance. A `StringGet` is served by
/// `cached_response`, which is CACHE-FIRST and consults the shard only on a miss, and
/// `CacheKey::string(shard_id, key)` carries no generation, sequence or version stamp -- so
/// nothing in the key or the read path can notice that the shard has moved on. With the pass
/// deferred, `delete_record` removes the key under the guard, the tombstone is appended and
/// `applied_wal_sequence` anchored past it, the guard drops -- and until the deferred pass reaches
/// that key, a reader asking for it is answered out of the cache with the value the shard no
/// longer has.
///
/// IN WHICH DIRECTION IT FAILS. Deferring an invalidation can only ever leave the cache MORE
/// populated than the shard, never less. So the single observable failure is STALE-ALIVE, and a
/// check that only asked "is the cache eventually empty of this key" would pass under the unsafe
/// mutant, because that is a question about the END STATE and the defect is entirely in the
/// middle. This attacks the observable direction: it reads keys the shard has ALREADY removed,
/// while the round is still running.
///
/// WHY IT NEEDS A SECOND THREAD. The window opens and closes inside one call. The reader takes the
/// `shards` lock to decide a key is gone, so under the shipped code it cannot observe that until
/// the guarded section that both deletes and sweeps has completed; under the mutant it observes it
/// the moment the guard drops.
///
/// WARM, because a cold cache holds nothing to serve stale -- on the cold fixture this test would
/// be looking for a stale answer that could not exist, and would pass against any mutant at all.
/// The cache entry count is asserted non-zero before the threads start.
// rust-internal: Rust cache-first read visibility under the Rust shard write guard
#[test]
fn a_key_the_expiry_round_has_removed_is_never_still_answered_out_of_the_cache() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    const OBJECTS: usize = 800;

    // EVERY key due, so the round has the longest hold this fixture can give it.
    let all: Vec<String> = (0..OBJECTS).map(cache_pass_key).collect();
    let (_dir, engine) = cache_pass_fixture(OBJECTS, true, &all);
    let cached_before = engine.cache.entries_for_shard(1).len();
    assert!(
        cached_before > 0,
        "the fixture's cache is empty, so no stale answer could exist and this test would pass \
         against any mutant at all",
    );

    let stop = Arc::new(AtomicBool::new(false));
    let stale_answers = Arc::new(AtomicU64::new(0));
    let removed_keys_read = Arc::new(AtomicU64::new(0));

    let reader = {
        let engine = engine.clone();
        let stop = Arc::clone(&stop);
        let stale_answers = Arc::clone(&stale_answers);
        let removed_keys_read = Arc::clone(&removed_keys_read);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // Which keys the SHARD has already let go of, read under the shard lock.
                let gone = {
                    let shards = engine.shards.read().expect("engine lock poisoned");
                    match shards.get(&1) {
                        Some(shard) => (0..OBJECTS)
                            .map(cache_pass_key)
                            .filter(|key| !shard.strings.contains_key(key.as_str()))
                            .collect::<Vec<_>>(),
                        None => break,
                    }
                };
                for key in gone {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    // The shard has removed this key. Ask for it anyway.
                    removed_keys_read.fetch_add(1, Ordering::Relaxed);
                    let out = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet { key },
                    });
                    if let CommandResponse::Bytes { value: Some(_) } = out.response {
                        stale_answers.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        })
    };

    let report = engine
        .sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id: 1,
            load_cold_buckets: true,
            max_hot_buckets_per_round: 0,
            max_cold_buckets_per_round: 0,
            ..ShardExpirySweepRequest::default()
        })
        .expect("shard 1 is loaded");
    // Let the reader keep going for a moment after the round, so the window a deferred pass opens
    // is sampled rather than raced past.
    std::thread::sleep(std::time::Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    reader.join().expect("the reader thread must not panic");

    let stale = stale_answers.load(Ordering::Relaxed);
    let read = removed_keys_read.load(Ordering::Relaxed);
    println!(
        "  CONCURRENT READER over an expiry round: {} reads of keys the shard had already \
         removed, {stale} of them answered out of the cache with a value; the round expired {} \
         keys and the cache held {cached_before} entries before it",
        read, report.expired_records_removed,
    );

    assert!(
        report.expired_records_removed > 0,
        "the round expired nothing, so the reader had no removed key to ask about",
    );
    // THE DENOMINATOR. Without reads of already-removed keys there is no opportunity for a stale
    // answer, and zero stale answers would mean nothing.
    assert!(
        read > 0,
        "the reader never saw a key the shard had removed, so it never had the chance to be \
         answered stale; the round expired {}",
        report.expired_records_removed,
    );
    assert_eq!(
        stale, 0,
        "{stale} of {read} reads of keys the shard had ALREADY removed were answered out of the \
         cache with a value. The record-cache pass must stay inside the `shards` write guard: a \
         record CacheKey carries no generation, sequence or version stamp, so a cache-first read \
         in that window cannot tell that the shard has moved on.",
    );
}

/// WHAT ONE LISTING OF AN EXPIRY FIXTURE'S CACHE COSTS IN SYSCALLS.
///
/// The batched pass trades N cache walks for one cache walk plus C syscalls, and C is a property
/// of the fixture rather than a constant: `entries_for_shard` falls through to a filesystem
/// `metadata()` for every entry it returns that the disk index does not hold, and for no others.
///
/// THIS PATH'S C IS NOT THE SIBLING PATH'S C, and that is the reason for measuring it again. A
/// `delete_drop` round calls `invalidate_slot` before its cache pass, so by the time it lists, the
/// `page` half of the shard's cache is already gone and the listing returns about half the warm
/// cache. THIS round calls no such thing -- `delete_record` touches shard state only -- so its one
/// listing returns the WHOLE warm cache.
///
/// HOW. The count is a DIFFERENCE taken from OUTSIDE the process: the same test binary is run
/// under `strace -f -c` twice, once with `TS_EXPIRY_PROBE_LIST_REPEATS=0` and once with a positive
/// repeat count, and one listing's cost is the difference over the repeats. Everything else the
/// process does -- the corpus, the warming reads, the probe's own first listing -- is identical in
/// both arms and cancels. Nothing in this process counts its own syscalls.
///
/// THE CACHE MUST BE POPULATED, or the listing costs nothing and this reports a free operation.
/// The entry count is asserted non-zero and printed with its namespace census.
// rust-internal: syscall cost of the Rust MultiLayerCache shard listing
#[test]
fn what_one_listing_of_an_expiry_fixtures_cache_costs_in_syscalls() {
    let objects: usize = std::env::var("TS_EXPIRY_PROBE_OBJECTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(500);
    let repeats: usize = std::env::var("TS_EXPIRY_PROBE_LIST_REPEATS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);

    let (_dir, engine) = cache_pass_fixture(objects, true, &cache_pass_due(objects));
    let listing = engine.cache.entries_for_shard(1);
    let entries = listing.len();
    assert!(
        entries > 0,
        "the fixture's cache is empty, so a listing of it would cost nothing and this probe would \
         report a free operation; objects {objects}",
    );
    let mut by_namespace = std::collections::BTreeMap::<String, usize>::new();
    for entry in &listing {
        *by_namespace.entry(entry.namespace.clone()).or_default() += 1;
    }
    println!(
        "PROBE-EXPIRY-LISTING objects={objects} entries={entries} repeats={repeats} \
         namespaces={by_namespace:?} load={}",
        std::fs::read_to_string("/proc/loadavg")
            .unwrap_or_default()
            .split_whitespace()
            .next()
            .unwrap_or("?")
            .to_string(),
    );
    let started = std::time::Instant::now();
    let mut listed = 0usize;
    for _ in 0..repeats {
        listed = listed.saturating_add(engine.cache.entries_for_shard(1).len());
    }
    let elapsed = started.elapsed().as_micros();
    // The listing must be deterministic, or the syscall difference this probe is driven for is a
    // difference between two different listings.
    assert_eq!(
        engine.cache.entries_for_shard(1).len(),
        entries,
        "two listings of an untouched cache returned different lengths",
    );
    println!(
        "PROBE-EXPIRY-LISTING listed_total={listed} elapsed_us={elapsed} per_listing_us={:.1}",
        if repeats == 0 {
            0.0
        } else {
            elapsed as f64 / repeats as f64
        },
    );
}
