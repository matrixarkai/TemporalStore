// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE TWO DISTINCT-COUNT MAPS ARE ONE MAP, AND THE STAMP THAT PAYS FOR IT.
//!
//! `control_state_change_sketch` has left the stored image and `control_state_changes`' bucket
//! value has become [`DistinctBucket`], so `SHARD_INDEX_FORMAT_VERSION` moved 14 -> 15. This file
//! holds the four things that bump owes.
//!
//! # WHAT THE STAMP IS FOR, AND WHY A TEST HAS TO PROVE THE REFUSAL
//!
//! `decode_index_bytes` checks the version BEFORE decoding, deliberately: the payload is
//! addressed by field order, and decoding it against a different struct shape "does not error, it
//! produces a plausible and wrong `ShardState`". So the refusal is the guard, and a bump whose
//! refusal is untested is a bump that might not refuse.
//!
//! [`a_pre_bump_index_is_refused_rather_than_read`] drives a stale-stamped image through the real
//! load path and asserts `RefusedStaleStamp`.
//! [`an_index_at_the_new_stamp_is_accepted`] is the positive control -- the same fixture at 15 is
//! accepted and its data comes back -- without which the refusal arm passes just as well when the
//! fixture never reached the load path.
//!
//! # AND A SECOND, INDEPENDENT LAYER THAT THIS PARTICULAR CHANGE HAPPENS TO HAVE
//!
//! [`the_pre_bump_bucket_shape_cannot_be_read_as_the_new_one`] asks what would happen if the stamp
//! were NOT there: it takes a current index, rewrites one bucket back into the pre-bump shape --
//! a bare member array where an enum now lives -- and asserts the decode FAILS.
//!
//! That is worth measuring rather than assuming, and it cuts both ways. It means the stamp is
//! belt-and-braces for THIS change rather than the only thing standing between an old image and a
//! plausible-and-wrong state, which is a weaker claim than the general argument for the stamp and
//! an honest one. It does not make the stamp optional: the sketch map leaving is a field no reader
//! claims, `ShardState` sets no `deny_unknown_fields`, and that half WOULD be read silently as
//! "there were never any sketched buckets".
//!
//! # THE REPLAY CONSEQUENCE
//!
//! Every index written before 15 is refused on load, counted as `RefusedStaleStamp`, and the shard
//! rebuilds from its WAL. Slower and correct.
//!
//! # WHAT THIS CHANGE IS NOT
//!
//! It is not a bug fix. [`DistinctBucket`]'s own doc records why, and
//! [`the_old_overlap_state_would_have_counted_correctly`] drives it: a bucket present in both of
//! the old maps would still have produced the right distinct count, because an `Hll` and a
//! `BTreeSet` both absorb a duplicate. The merge deletes a map and makes an invariant
//! unbreakable; it repairs nothing.

#![allow(clippy::all)]
use super::*;

use crate::engine::hll::{count_changes, record_change, DistinctBucket, DISTINCT_SKETCH_THRESHOLD};
use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};

const SHARD: ShardId = 4093;
const KEY: &str = "ctl:change:tenant-9";
const BUCKET_MS: u64 = 1_000;

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn base_index_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("indexes")
        .join(format!("shard-{SHARD}.index.json"))
}

/// A store holding one exact change bucket with three members, flushed.
fn build_fixture(dir: &std::path::Path) {
    let engine = engine_at(dir);
    engine.load_shard(SHARD);
    for value in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
        let answer = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::ControlStateChangeAdd {
                key: KEY.to_string(),
                timestamp_ms: BUCKET_MS,
                value,
                precision_ms: None,
                ttl_ms: None,
            },
        });
        assert!(answer.status.ok, "a change write failed: {}", answer.status.message);
    }
    engine.flush_shard_index(SHARD);
    drop(engine);
}

/// How many distinct members the shard holds for `KEY`, and in which representation.
fn held(engine: &TemporalEngine) -> Option<(u64, bool)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD)?;
    let bucket = shard.control_state_changes.get(KEY)?.get(&BUCKET_MS)?;
    Some((
        bucket.distinct_hint(),
        matches!(bucket, DistinctBucket::Sketch(_)),
    ))
}

/// A PRE-BUMP INDEX IS REFUSED BY THE STAMP, NOT READ.
#[test]
fn a_pre_bump_index_is_refused_rather_than_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path());

    let path = base_index_path(dir.path());
    let bytes = std::fs::read(&path).expect("a flushed base index exists");
    let mut restored = crate::engine::decode_index_bytes(&bytes).expect("the base index decodes");
    assert_eq!(
        restored.index_format_version,
        crate::engine::SHARD_INDEX_FORMAT_VERSION,
        "denominator: the index just written must carry the CURRENT stamp, or staling it below \
         changes nothing"
    );
    restored.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION - 1;
    std::fs::write(&path, crate::engine::encode_index_bytes(&restored)).expect("write");

    reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(SHARD);
    let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
    println!(
        "  stamp {} against {} -> stale {stale_n}, decode {decode_n}, absent {absent_n}, accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION - 1,
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    assert!(
        stale_n > 0,
        "an index stamped below {} must be counted as a STALE-STAMP refusal; got stale {stale_n} \
         decode {decode_n} absent {absent_n} accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    // Refused and REBUILT, not refused and lost: replay re-derives the three members.
    let after = held(&reloaded);
    println!("  after the refusal, replay rebuilt: {after:?}");
    assert_eq!(
        Some((3, false)),
        after,
        "the refusal must fall back to WAL replay and re-derive the bucket exactly -- three \
         distinct members, still EXACT. A refusal that loses the data is not the trade this bump \
         was approved on."
    );
}

/// THE POSITIVE CONTROL: the same fixture at the new stamp is accepted and its data comes back.
#[test]
fn an_index_at_the_new_stamp_is_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path());

    reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(SHARD);
    let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
    println!(
        "  stamp {} -> stale {stale_n}, decode {decode_n}, absent {absent_n}, accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    assert!(
        accepted_n > 0,
        "an index at the current stamp must be ACCEPTED, or the refusal arm is measuring a \
         fixture that never reached the load path; got stale {stale_n} decode {decode_n} absent \
         {absent_n} accepted {accepted_n}"
    );
    assert_eq!(
        Some((3, false)),
        held(&reloaded),
        "the accepted index must deliver the bucket it carries"
    );
}

/// WHAT THE PRE-BUMP BUCKET SHAPE DOES AGAINST THE NEW ONE, WITH THE STAMP TAKEN OUT OF THE WAY.
#[test]
fn the_pre_bump_bucket_shape_cannot_be_read_as_the_new_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path());
    let bytes = std::fs::read(base_index_path(dir.path())).expect("a flushed base index exists");
    let restored = crate::engine::decode_index_bytes(&bytes).expect("the base index decodes");

    // Through the JSON codec so exactly ONE thing differs: the bucket's value shape. Building a
    // minimal hand-written image instead would also be missing every field that has no
    // `#[serde(default)]`, and the decode would fail for that reason rather than this one.
    let json = crate::engine::encode_index_bytes_as_plain_json(&restored);
    let mut value: serde_json::Value =
        serde_json::from_slice(&json).expect("a plain-JSON index parses as JSON");
    let bucket = value
        .get_mut("control_state_changes")
        .and_then(|m| m.get_mut(KEY))
        .and_then(|series| series.get_mut(BUCKET_MS.to_string()))
        .expect("the fixture's bucket is in the JSON");
    println!("  the new bucket shape is {bucket}");
    assert!(
        bucket.get("Exact").is_some(),
        "denominator: the current shape must be the externally-tagged enum, or the rewrite below \
         is not putting back a PRE-BUMP shape"
    );
    // The pre-bump shape: the member set itself, with no variant tag around it.
    *bucket = serde_json::json!([[97], [98], [99]]);
    let pre_bump = serde_json::to_vec(&value).expect("the rewritten image serializes");

    let outcome = crate::engine::decode_index_bytes(&pre_bump);
    println!("  decoding a pre-bump bucket against the new shape -> {outcome:?}");
    assert!(
        outcome.is_err(),
        "a pre-bump bucket value DECODED into the new shape. That is the \"plausible and wrong \
         `ShardState`\" case `decode_index_bytes` warns about, and it would mean the stamp is the \
         only thing standing between an old image and silently wrong distinct counts. The stamp \
         does refuse it -- see the arm above -- but this layer is then load-bearing rather than \
         belt-and-braces, and the bump's body should say so."
    );
}

/// THE CONVERSION IS A VARIANT SWAP, AND THERE IS NO SECOND MAP TO DISAGREE WITH.
#[test]
fn the_conversion_swaps_the_variant_in_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    engine.load_shard(SHARD);

    let mut config = Config {
        version: 2,
        ..Config::default()
    };
    config
        .extend_config
        .insert("control_distinct_sketch".to_string(), "on".to_string());
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: SHARD,
                config
            })
            .ok,
        "setting the distinct-sketch gate failed"
    );
    // One execute, so the per-execute assignment puts the gate on the shard.
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::ControlStateChangeAdd {
                    key: KEY.to_string(),
                    timestamp_ms: BUCKET_MS,
                    value: b"seed".to_vec(),
                    precision_ms: None,
                    ttl_ms: None,
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
        assert!(
            shard.control_distinct_sketch,
            "denominator: the gate must be on the shard, or no conversion will happen"
        );
        for index in 0..=DISTINCT_SKETCH_THRESHOLD {
            record_change(shard, KEY, BUCKET_MS, format!("member-{index}").into_bytes());
        }
    }

    let (hint, sketched) = held(&engine).expect("the bucket exists");
    println!("  after {} writes -> sketched {sketched}, distinct hint {hint}", DISTINCT_SKETCH_THRESHOLD + 2);
    assert!(
        sketched,
        "a bucket past the threshold with the gate on must have been converted to a sketch"
    );
    // ONE bucket, in ONE map. The old shape could hold the same (key, bucket) in two maps at
    // once; this one cannot represent that, which is the whole of what the merge buys.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("the shard is loaded");
    assert_eq!(
        1,
        shard
            .control_state_changes
            .get(KEY)
            .map_or(0, |series| series.len()),
        "the key must hold exactly the one bucket that was written to"
    );
    let counted = count_changes(shard, KEY, 0, BUCKET_MS + 1);
    println!("  count_changes over the sketched window -> {counted}");
    assert!(
        counted > 3_500 && counted < 4_700,
        "the estimate over a converted bucket of {} distinct members came back {counted}, which \
         is outside what a P=12 sketch should produce",
        DISTINCT_SKETCH_THRESHOLD + 2
    );
}

/// THE EXACT PATH IS UNCHANGED WHEN NOTHING IN THE WINDOW IS SKETCHED.
#[test]
fn an_unsketched_window_still_counts_exactly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    engine.load_shard(SHARD);
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
        assert!(
            !shard.control_distinct_sketch,
            "denominator: the gate is off by default, which is the arm this measures"
        );
        for index in 0..10 {
            record_change(shard, KEY, BUCKET_MS, format!("member-{index}").into_bytes());
        }
        // A duplicate, to show the count is DISTINCT and not a total.
        record_change(shard, KEY, BUCKET_MS, b"member-0".to_vec());
        assert_eq!(
            10,
            count_changes(shard, KEY, 0, BUCKET_MS + 1),
            "an unsketched window must count exactly, duplicates absorbed"
        );
    }
}

/// THE OLD OVERLAP STATE WOULD HAVE COUNTED CORRECTLY, so this change is not a bug fix.
///
/// The two old maps were kept mutually exclusive by a remove/insert pair, and a bucket in both was
/// representable. It would NOT have produced a wrong answer: the read path folded the exact values
/// into an HLL and merged the sketches into the same HLL, and an `Hll` absorbs a duplicate exactly
/// as a `BTreeSet` does.
///
/// Driven on the primitive rather than argued, because the overlap state is no longer
/// representable and this is the only way left to show what it would have done: the same members
/// added to one sketch twice, and added to a second sketch that is then merged in, give the same
/// estimate as adding them once.
#[test]
fn the_old_overlap_state_would_have_counted_correctly() {
    use crate::engine::hll::Hll;

    let members: Vec<Vec<u8>> = (0..500).map(|i| format!("m-{i}").into_bytes()).collect();

    let mut once = Hll::new();
    for m in &members {
        once.add(m);
    }

    // The overlap: the same members counted through BOTH representations.
    let mut twice = Hll::new();
    for m in &members {
        twice.add(m);
    }
    let mut other = Hll::new();
    for m in &members {
        other.add(m);
    }
    twice.merge(&other);

    println!(
        "  once {} vs counted through both representations {}",
        once.estimate(),
        twice.estimate()
    );
    assert_eq!(
        once.estimate(),
        twice.estimate(),
        "an HLL did NOT absorb the duplicate, which would mean the old two-map overlap could have \
         produced a wrong distinct count -- and that this merge IS a bug fix after all. Say so \
         loudly if this ever fires: the whole framing of the change depends on it."
    );
}

/// A CONSEQUENCE I DID NOT SET OUT TO CHANGE, PINNED SO IT IS REVIEWABLE RATHER THAN SILENT.
///
/// `key_membership_size` counts `control_state_changes.get(key).len()` -- the number of BUCKETS,
/// not what they hold -- and `delta_key_state_change` fires a key-state capture when that sum
/// SHRINKS.
///
/// Under the two maps, a conversion REMOVED the bucket from `control_state_changes` and inserted
/// it into `control_state_change_sketch`, so the sum went 1 -> 0 and the conversion was captured.
/// Under one map the bucket stays put and the sum is unchanged, so the conversion is NO LONGER
/// CAPTURED. The value changed and a length-based detector cannot see a value change.
///
/// WHAT THAT COSTS, STATED RATHER THAN WAVED AT. On the single-barrier default the index-log fold
/// is never consulted -- `load_shard` reaches `load_index_base_only` with `fold_deltas: false`
/// (#2152) -- so nothing reads those captures and this is unobservable. On the
/// `TS_WAL_LEGACY_RECOVERY` arm, a fold could restore the bucket in its PRE-conversion form: an
/// exact set of every member rather than a sketch. That is MORE memory and an EXACT count where
/// an estimate was expected, which is the safe direction of the two -- it cannot produce a wrong
/// answer, only a bigger bucket -- and WAL replay re-derives the conversion deterministically
/// whenever it runs.
///
/// This is pinned rather than fixed because fixing it means making the capture gate sensitive to a
/// VALUE change rather than a length change, which is a different and larger design than the merge
/// that was approved. If the reviewer wants the capture preserved, this is the test that says what
/// to aim at.
#[test]
fn the_conversion_no_longer_shrinks_the_membership_sum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    engine.load_shard(SHARD);

    let mut config = Config {
        version: 2,
        ..Config::default()
    };
    config
        .extend_config
        .insert("control_distinct_sketch".to_string(), "on".to_string());
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: SHARD,
                config
            })
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::ControlStateChangeAdd {
                    key: KEY.to_string(),
                    timestamp_ms: BUCKET_MS,
                    value: b"seed".to_vec(),
                    precision_ms: None,
                    ttl_ms: None,
                },
            })
            .status
            .ok
    );

    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
    assert!(shard.control_distinct_sketch, "denominator: the gate must be on");

    // Fill to exactly the threshold -- still exact -- and read the sum. ONE SHORT OF THE
    // THRESHOLD, because the seed write above already put a member in: the denominator below
    // caught this as an already-converted bucket on the first run, which is what it is for.
    for index in 0..DISTINCT_SKETCH_THRESHOLD - 1 {
        record_change(shard, KEY, BUCKET_MS, format!("member-{index}").into_bytes());
    }
    let before = super::super::key_membership_size(shard, KEY);
    let exact_before = matches!(
        shard.control_state_changes.get(KEY).and_then(|s| s.get(&BUCKET_MS)),
        Some(DistinctBucket::Exact(_))
    );
    assert!(exact_before, "denominator: the bucket must still be exact before the conversion");

    // One more value tips it over, converting the bucket in place.
    record_change(shard, KEY, BUCKET_MS, b"the-one-that-converts".to_vec());
    let after = super::super::key_membership_size(shard, KEY);
    let sketched_after = matches!(
        shard.control_state_changes.get(KEY).and_then(|s| s.get(&BUCKET_MS)),
        Some(DistinctBucket::Sketch(_))
    );
    println!("  membership sum {before} -> {after}, converted {sketched_after}");
    assert!(
        sketched_after,
        "denominator: the conversion must have happened, or there is nothing to say about the sum"
    );
    assert_eq!(
        before, after,
        "the conversion changed the membership sum. If it SHRANK, the capture fires again and the          consequence this test documents has gone away -- which would be fine, but the doc above          is then wrong and should be deleted rather than left to mislead."
    );
}

/// THE COMPATIBILITY KEY IS GONE FROM THE STORED IMAGE, AND THE ROWS ARE IN THE CONSOLIDATED MAP.
///
/// # WHY THIS REMOVAL BELONGS TO THIS CHANGE AND NOT A LATER ONE
///
/// #2155 wrote the control-state blocks to BOTH the consolidated map and a private copy of the old
/// top-level key, because every image at the then-current stamp carried that key, none of them was
/// stale, `objects` defaults to empty, and `ShardState` sets no `deny_unknown_fields` -- so simply
/// deleting the field would have read every one of those images as "there were no control-state
/// blocks" with no error anywhere and every door shut.
///
/// Its stated expiry was a change that moves `SHARD_INDEX_FORMAT_VERSION`, because that is what
/// makes those images refuse as stale and fall back to replay. This is that change. The shim, its
/// decode fold, its agreement predicate, its disagreement counter, its test-only injector and the
/// tripwire that held it to an expiry condition are all removed here rather than left for later.
///
/// # WHAT THIS ASSERTS, AND THE DENOMINATOR IT NEEDS
///
/// That the flushed image does NOT spell the top-level key, and that the rows are nonetheless
/// readable through the consolidated map afterwards. The second half is the denominator: an image
/// with no control-state blocks at all would satisfy the first half trivially, so the fixture
/// writes one and the assertion requires it to be there.
#[test]
fn the_stored_image_no_longer_carries_the_control_state_blocks_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    engine.load_shard(SHARD);
    let wrote = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::ControlStateIncrement {
            key: "cap:tenant-9".to_string(),
            timestamp_ms: BUCKET_MS,
            amount: 3,
        },
    });
    assert!(wrote.status.ok, "the control-state write failed: {}", wrote.status.message);
    engine.flush_shard_index(SHARD);

    // DENOMINATOR: the fixture really produced a control-state block.
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("the shard is loaded");
        assert!(
            !shard.control_state_blocks().is_empty(),
            "denominator: the fixture wrote no control-state block, so the absence of the stored              key below would be satisfied by an empty store and would mean nothing"
        );
    }

    let bytes = std::fs::read(base_index_path(dir.path())).expect("a flushed base index exists");
    let restored = crate::engine::decode_index_bytes(&bytes).expect("the base index decodes");
    let json = crate::engine::encode_index_bytes_as_plain_json(&restored);
    let value: serde_json::Value =
        serde_json::from_slice(&json).expect("a plain-JSON index parses as JSON");
    let top_level: Vec<&String> = value
        .as_object()
        .expect("the index is a JSON object")
        .keys()
        .collect();
    println!("  top-level keys carrying `control`: {:?}",
        top_level.iter().filter(|k| k.contains("control")).collect::<Vec<_>>());
    assert!(
        value.get("control_state_blocks").is_none(),
        "the stored image still spells the top-level `control_state_blocks` key. That is the          compatibility shim, and the stamp bump in this change is what paid for taking it out;          leaving it means the stamp was spent and the cost it was spent on is still being paid."
    );
    assert!(
        value.get("objects").is_some(),
        "denominator: the consolidated map is absent from the stored image, so the rows have no          home at all rather than having moved"
    );

    // AND THE ROWS COME BACK. Reload and read them through the consolidated map.
    drop(engine);
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(SHARD);
    let shards = reloaded.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("the shard is loaded");
    println!("  control-state blocks after reload: {}", shard.control_state_blocks().len());
    assert!(
        !shard.control_state_blocks().is_empty(),
        "the control-state blocks did not survive a reload through the consolidated map alone.          That is the loss the compatibility shim existed to prevent, arriving because the shim was          removed without the consolidated map carrying the rows."
    );
}
