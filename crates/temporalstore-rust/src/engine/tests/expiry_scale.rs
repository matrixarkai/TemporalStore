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
/// It is not a claim that a round is free of the keyspace. A round that expires ANYTHING ends by
/// serializing the whole shard index and persisting it once (`serialize_index_stamped` ->
/// `persist_index_bytes` -> `append_index_bytes` in `sweep_expired_records_with_request`), and
/// that is whole-shard work whether ten keys expired or ten thousand. Measured here, debug build:
/// a round expiring 10 keys costs about 0.11 s at a 2,000-key shard, 0.92 s at 20,000 and several
/// seconds at 100,000, while looking at exactly 10 records at every one of those sizes.
///
/// FOUND, NOT FIXED. That residual term is a real cost -- at the storage manager's cadence a large
/// shard with a trickle of expiries re-serializes its entire index every round that catches one --
/// and it is a DIFFERENT defect from the one the ordered index fixed, with a different fix (batch
/// or defer the persist, or make it a delta). It is recorded here rather than asserted, because an
/// assertion on it would fail today and this test's job is to hold the line that was won.
///
/// The sharpest edge is already off it: the flush no longer runs under the shard write guard, so
/// the round does not queue every other reader and writer behind itself
/// (`the_expiry_sweep_flush_waits_for_the_write_guard_to_drop`, part1). That moved WHO WAITS, not
/// how much work a round does, which is why the ratio below is still what it is.
///
/// The distinction matters for the round-robin-cursor proposal too: a bounded cursor would not
/// have touched this term either. It bounds the walk; it does not make the round's fixed cost
/// smaller, and it would have kept the scan cost the index removed.
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

    // THE RESIDUAL, printed: the once-per-round whole-shard index persist. Not asserted -- see
    // the note above.
    println!(
        "  round cost with {DUE_KEYS} due: {:.1} ms at 2k live -> {:.1} ms at 20k live \
         ({:.2}x) -- scan removed, whole-shard index persist per round remains",
        round_small_us as f64 / 1_000.0,
        round_large_us as f64 / 1_000.0,
        round_large_us as f64 / round_small_us.max(1) as f64
    );
}
