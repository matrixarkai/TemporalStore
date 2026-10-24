// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SKIPPED SERVED-INDEX CHECKPOINT MUST NOT LOSE A PUBLISHED VALUE.
//!
//! WHAT THIS IS FOR. `publish_shard_index_snapshot_for_keys` republished the pages of the keys it
//! was NAMED and then re-encoded and rewrote the whole served index, at a cost set by the size of
//! the STORE rather than the size of the publish --
//! `what_one_visibility_publish_writes_grows_with_the_store_not_the_publish` prices that at 376,012
//! encoded bytes to publish ONE key on an 8,000-key store, against a figure that does not move when
//! the named set widens 256-fold on a fixed store. The publish now spends the keys it published
//! against `ShardState::publish_index_flush_countdown` and only checkpoints when they run it out,
//! so most publishes write no index at all.
//!
//! THE WHOLE SAFETY CLAIM IS THAT REPLAY RE-DERIVES WHAT THE CHECKPOINT DEFERRED, and it is an
//! argument until something drives it. A publish relocates log-resident blocks into the block store
//! and repoints the index rows at them. It does NOT retire those blocks' registrations and it does
//! NOT clear any bucket's `first_dirty_wal_sequence` -- which is written to zero at exactly one
//! site, and only by a durable dump -- so both WAL retention floors still sit below the records it
//! rewrote: `min_registered_sequence` and the per-bucket first-dirty sequence the reclaim plan
//! reads. A reload therefore finds a served index with an older anchor and the records beyond it
//! still in the log, which is exactly the state replay exists for. This test puts a store into that
//! state on purpose -- publishes whose checkpoint was skipped, COUNTED, not assumed -- drops the
//! engine so only the disk is left, reopens over the same directories, and reads every value back.
//!
//! THE CONTESTED CASE IS ASSERTED, NOT HOPED FOR. If every publish in the arm happened to be due
//! then nothing was deferred and a clean reload proves nothing at all, so the arm counts the
//! publishes that published keys and wrote no index and refuses to conclude without at least one.
//! It also refuses to conclude without at least one key actually published: a publish that found
//! nothing log-resident reloads identically whatever the checkpoint did.
//!
//! AND IT ASSERTS THE OTHER DIRECTION IN THE SAME BREATH. "Every published value came back" is also
//! what a reload would report if it had quietly served the pre-publish copy of everything, so the
//! arm compares the FULL key set and every value BYTE FOR BYTE in both directions -- nothing
//! missing and nothing extra -- rather than checking that the published keys are present.
//!
//! THE SEED GOES THROUGH THE ASYNC-STORAGE ARM because that is the only write path that mints a
//! log-resident block address, and therefore the only one that leaves a publish anything to do. It
//! is also the regime the production caller runs in: the proxy's `matrixark_publish_visibility`
//! exists for stores opened with `MATRIXARK_RUST_PROXY_ASYNC_STORAGE`.
//!
//! THE COUNTERS ARE THREAD-LOCAL. `index_encode_counts` tallies per thread and the encode happens
//! on the calling thread, so a single-threaded arm sees all of it and only it.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 31;
/// Resident keys. Well under the 4,096 default resident-block bound, so every seeded key is still
/// log-resident when the publishes start and none of them was swept out behind the test's back.
const KEYS: usize = 600;
const PUBLISHES: usize = 20;
/// Keys per publish. 20 x 4 = 80 published keys against the shipped interval of 64, so the arm
/// crosses it once and the rest of the publishes are deferred.
const PER_PUBLISH: usize = 4;

fn key_at(item: usize) -> String {
    format!("published-{item:06}")
}

/// An engine over `dir`, with the shard loaded. Reopening over the same `dir` is the reload.
fn engine_over(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

/// Seed string records through the async-storage arm, so their blocks are log-resident.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let response = engine.execute_replicated(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: key_at(item),
                    value: value_for(item),
                },
            }
            .with_async_storage(),
        );
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
    }
}

/// A value whose bytes name the key they belong to, so a reload that resolves the WRONG block is a
/// mismatch here rather than a silent pass.
fn value_for(item: usize) -> Vec<u8> {
    format!("value-for-{item:06}-{}", "p".repeat(100)).into_bytes()
}

/// Every seeded key's value as the engine serves it, read through the public command surface so a
/// reload is exercised the way a reader exercises it.
fn served_values(engine: &TemporalEngine) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut served = std::collections::BTreeMap::new();
    for item in 0..KEYS {
        let key = key_at(item);
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: key.clone() },
        });
        if !response.status.ok {
            continue;
        }
        if let CommandResponse::Bytes { value: Some(bytes) } = response.response {
            served.insert(key, bytes);
        }
    }
    served
}

/// One arm's reading.
struct PublishShape {
    /// Publishes that actually published something. A publish that published nothing is not a
    /// publish.
    publishes_with_keys: usize,
    /// Publishes that encoded the whole base index.
    materializations: usize,
    /// Publishes that published keys and encoded NOTHING. The contested case: if this is zero the
    /// arm never entered the regime it is arguing about and proves nothing.
    skipped: usize,
    /// Bytes encoded across the whole arm.
    encode_bytes: u64,
    /// The keys the arm published, in order.
    published: Vec<String>,
    /// How many keys EACH publish published, in order.
    ///
    /// The identity below replays the countdown over this rather than assuming every publish
    /// published the same number: a publish whose concurrent-write check skips a key moves where
    /// the next checkpoint falls, and a closed-form "about N/K" would read that as a defect.
    published_per_publish: Vec<u64>,
}

/// How many checkpoints a countdown starting at 0 (DUE, the post-reload value) owes over
/// `published_per_publish` at `interval`.
///
/// Written as the arithmetic the implementation does rather than as a ratio, which is what makes
/// it fail when the countdown is reloaded with the wrong number, is not spent at all, or is reset
/// on a publish that was not due.
fn materializations_due(published_per_publish: &[u64], interval: u64) -> usize {
    let mut countdown: u64 = 0;
    let mut due = 0usize;
    for published in published_per_publish {
        countdown = countdown.saturating_sub(*published);
        if countdown == 0 {
            countdown = interval;
            due += 1;
        }
    }
    due
}

/// Run `PUBLISHES` narrow publishes against `engine`, reading what each one encoded.
fn drive_publishes(engine: &TemporalEngine) -> PublishShape {
    let mut shape = PublishShape {
        publishes_with_keys: 0,
        materializations: 0,
        skipped: 0,
        encode_bytes: 0,
        published: Vec::new(),
        published_per_publish: Vec::new(),
    };
    for round in 0..PUBLISHES {
        // DISTINCT KEYS PER PUBLISH. A published block is no longer log-resident, so naming the
        // same key twice gives a publish with nothing to do -- which would report a skip that is
        // really an empty publish and make the count below mean nothing.
        let selected: Vec<String> = (0..PER_PUBLISH)
            .map(|offset| key_at(round * PER_PUBLISH + offset))
            .collect();
        crate::engine::shard_write_guard::reset_index_encode_counts();
        let reported = engine
            .publish_shard_index_snapshot_for_keys(SHARD, selected.clone())
            .expect("publish must succeed");
        let counts = crate::engine::shard_write_guard::index_encode_counts();
        shape.encode_bytes += counts.encode_bytes_total;
        shape.publishes_with_keys += 1;
        shape.published_per_publish.push(selected.len() as u64);
        shape.published.extend(selected);
        if counts.encodes_total > 0 {
            shape.materializations += 1;
            assert!(
                reported > 0,
                "round {round} encoded {} bytes and reported {reported}, so the reported figure \
                 is no longer the bytes the publish wrote",
                counts.encode_bytes_total
            );
        } else {
            shape.skipped += 1;
            assert_eq!(
                0, reported,
                "round {round} encoded nothing and reported {reported} bytes, so the report is \
                 describing an index this publish did not write"
            );
        }
    }
    shape
}

/// A DEFERRED CHECKPOINT STILL RELOADS WITH EVERY PUBLISHED VALUE.
#[test]
fn a_deferred_publish_checkpoint_still_reloads_with_the_published_values() {
    let dir = tempfile::tempdir().expect("tempdir");

    let (before_reload, published, shape) = {
        let engine = engine_over(dir.path());
        seed(&engine, KEYS);

        let seeded = engine
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&SHARD)
            .expect("shard is loaded")
            .strings()
            .len();
        assert_eq!(
            KEYS, seeded,
            "the seed must have landed, or nothing below means anything"
        );

        // THE INTERVAL, READ BACK OUT. A knob the publish consults under a different name than
        // this arm sets leaves the arm silently not running, and it would then agree with whatever
        // it was asked. This is the shipped default, deliberately: the deferral this test is about
        // is the one a deployment gets.
        let interval = engine.visibility_publish_index_flush_interval();
        assert!(
            interval > 1,
            "the engine's interval is {interval}, so every publish is due and nothing is \
             deferred -- this test would pass while measuring the behaviour it was written to \
             replace"
        );

        let shape = drive_publishes(&engine);
        let before_reload = served_values(&engine);
        println!();
        println!(
            "  interval={interval} publishes={PUBLISHES} per_publish={PER_PUBLISH} \
             checkpoints_taken={} checkpoints_skipped={} encode_bytes={}",
            shape.materializations, shape.skipped, shape.encode_bytes
        );
        println!(
            "  seeded={seeded} published_keys={} served_before_reload={}",
            shape.published.len(),
            before_reload.len()
        );
        (before_reload, shape.published.clone(), shape)
        // The engine drops here, so only what reached the disk is left and the reload below has
        // to rebuild the shard from it.
    };

    // --- THE REGIME. All of these have to hold before the reload says anything. ---
    assert!(
        shape.publishes_with_keys > 1,
        "only {} publish(es) ran, so there was never more than one checkpoint's worth of keys to \
         defer",
        shape.publishes_with_keys
    );
    assert!(
        shape.skipped > 0,
        "every one of the {} publish(es) also took its checkpoint, so nothing was deferred and a \
         clean reload below would prove nothing. ({} checkpoint(s) taken.)",
        shape.publishes_with_keys,
        shape.materializations
    );
    assert!(
        !published.is_empty(),
        "no key was published across {PUBLISHES} publishes, so the reload has nothing to \
         re-derive"
    );
    assert_eq!(
        KEYS,
        before_reload.len(),
        "the engine did not serve every seeded key BEFORE the reload, so the comparison below \
         would be against a store that was already short"
    );

    // --- THE PROPERTY. ---
    let reopened = engine_over(dir.path());
    let after_reload = served_values(&reopened);
    println!("  served_after_reload={}", after_reload.len());
    println!();

    // THE PUBLISHED KEYS FIRST, because they are the contested subset: these are the rows whose
    // relocation the skipped checkpoints never wrote down.
    let lost_published: Vec<&String> = published
        .iter()
        .filter(|key| after_reload.get(*key).map(|bytes| &bytes[..]) != {
            before_reload.get(*key).map(|bytes| &bytes[..])
        })
        .collect();
    assert!(
        lost_published.is_empty(),
        "{} of {} key(s) published by a call whose served-index checkpoint was deferred did not \
         read back the same bytes after a reload, so the deferral is NOT re-derived by replay and \
         the interval is unsafe. First few: {:?}",
        lost_published.len(),
        published.len(),
        lost_published.iter().take(8).collect::<Vec<_>>()
    );

    // AND THE WHOLE STORE, IN BOTH DIRECTIONS, because "every published value came back" is also
    // what a reload that served the pre-publish copy of everything would report, and "nothing is
    // missing" is what a reload that invented rows would report too.
    let missing: Vec<&String> = before_reload
        .keys()
        .filter(|key| !after_reload.contains_key(*key))
        .collect();
    assert!(
        missing.is_empty(),
        "{} key(s) the engine served before the reload are gone after it, so the clean result \
         above is a store that lost rows rather than a deferral that replayed. First few: {:?}",
        missing.len(),
        missing.iter().take(8).collect::<Vec<_>>()
    );
    let extra: Vec<&String> = after_reload
        .keys()
        .filter(|key| !before_reload.contains_key(*key))
        .collect();
    assert!(
        extra.is_empty(),
        "{} key(s) exist after the reload that the engine did not serve before it: {:?}",
        extra.len(),
        extra.iter().take(8).collect::<Vec<_>>()
    );
    assert_eq!(
        before_reload, after_reload,
        "the values the reopened engine serves are not the ones the publishes left behind"
    );
}

/// THE SAVING, AGAINST AN ENGINE IN THE SAME PROCESS THAT STILL PAYS PER PUBLISH.
///
/// An interval arm's low checkpoint count says nothing on its own -- a publish that found nothing
/// to do also encodes nothing. The control is an engine whose interval is 1, which is exactly the
/// per-publish encode this change replaced, driven over the same fixture in the same process.
#[test]
fn the_publish_interval_turns_an_encode_per_publish_into_an_encode_per_interval() {
    let control = {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_over(dir.path());
        seed(&engine, KEYS);
        engine.set_visibility_publish_index_flush_interval_for_test(1);
        assert_eq!(
            1,
            engine.visibility_publish_index_flush_interval(),
            "the control arm did not take the interval it set, so it is not a control"
        );
        drive_publishes(&engine)
    };
    let shipped = {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_over(dir.path());
        seed(&engine, KEYS);
        let interval = engine.visibility_publish_index_flush_interval();
        assert!(
            interval > 1,
            "the shipped interval is {interval}, so this arm is the control"
        );
        drive_publishes(&engine)
    };

    println!();
    println!(
        "  control  interval=1  publishes={} checkpoints={} skipped={} encode_bytes={}",
        control.publishes_with_keys,
        control.materializations,
        control.skipped,
        control.encode_bytes
    );
    println!(
        "  shipped  default    publishes={} checkpoints={} skipped={} encode_bytes={}",
        shipped.publishes_with_keys,
        shipped.materializations,
        shipped.skipped,
        shipped.encode_bytes
    );
    println!();

    assert_eq!(
        control.publishes_with_keys, shipped.publishes_with_keys,
        "the two arms did not run the same number of publishes, so their totals are not \
         comparable"
    );
    assert_eq!(
        control.materializations, control.publishes_with_keys,
        "the control arm skipped {} of its {} publishes at interval 1, so it is not reproducing \
         the per-publish encode it exists to reproduce",
        control.skipped, control.publishes_with_keys
    );
    assert_eq!(
        0, control.skipped,
        "the control arm skipped {} checkpoint(s) at interval 1",
        control.skipped
    );
    assert!(
        shipped.materializations < control.materializations,
        "the shipped arm materialized the base index {} time(s) against the control's {}, so \
         nothing was saved",
        shipped.materializations,
        control.materializations
    );
    assert!(
        shipped.encode_bytes < control.encode_bytes,
        "the shipped arm encoded {} bytes against the control's {}",
        shipped.encode_bytes,
        control.encode_bytes
    );
    // AND THE EXACT NUMBER, so "fewer than the control" cannot be satisfied by a countdown that
    // is simply never spent. Spelling it out as the arithmetic the implementation does -- rather
    // than as "about N/K" -- is what makes it fail when the countdown is reloaded with the wrong
    // number, is not decremented at all, or is reset on a publish that was not due.
    let interval = crate::engine::VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT;
    let expected = materializations_due(&shipped.published_per_publish, interval);
    assert_eq!(
        expected, shipped.materializations,
        "at interval {interval} the recorded per-publish counts {:?} make {expected} publish(es) \
         due and the engine materialized the base index {} time(s)",
        shipped.published_per_publish, shipped.materializations
    );
    // AND THE SAME NUMBER FROM THE OUTSIDE, so the identity above cannot be satisfied by a replay
    // that is wrong in the same way the implementation is. `published / interval` rounded down,
    // plus the always-due first publish, is the ceiling however the publishes fall.
    let published_total: u64 = shipped.published_per_publish.iter().sum();
    let ceiling = 1 + published_total / interval;
    assert!(
        shipped.materializations as u64 <= ceiling,
        "{published_total} published keys at interval {interval} cannot owe more than {ceiling} \
         checkpoints, and {} were taken",
        shipped.materializations
    );
}

/// THE INTERVAL IS A REAL SETTING AND ITS DEFAULT IS NOT 1.
///
/// An interval of 1 reproduces the per-publish encode exactly, so a default of 1 would land this
/// change inert -- a feature nothing runs and nothing checks. This asserts the default
/// BEHAVIOURALLY as well as numerically: an engine built with no interval set at all must skip a
/// second consecutive publish's checkpoint, which is false for any default of 1 however the
/// constant reads.
#[test]
fn the_visibility_publish_flush_interval_is_a_real_setting_whose_default_is_not_one() {
    assert!(
        crate::engine::VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT > 1,
        "the default interval is {}; at 1 every publish is due and this change is inert",
        crate::engine::VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_over(dir.path());
    seed(&engine, 200);

    // The engine reads the setting, not a literal: this is the same reader the publish calls, and
    // it is the value an operator's `TS_VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL` lands in.
    let configured = engine.visibility_publish_index_flush_interval();
    assert_eq!(
        crate::engine::visibility_publish_index_flush_interval_from_env(),
        configured,
        "a fresh engine does not carry the configured interval"
    );
    assert!(
        configured > 1,
        "the interval this engine will use is {configured}, so every publish is due and nothing \
         is deferred"
    );

    // BEHAVIOURAL, ON THE DEFAULT, WITH NOTHING SET. The first publish is due because the
    // countdown is 0 on a fresh load; the second must not be.
    crate::engine::shard_write_guard::reset_index_encode_counts();
    let first = engine
        .publish_shard_index_snapshot_for_keys(SHARD, vec![key_at(0)])
        .expect("first publish");
    let after_first = crate::engine::shard_write_guard::index_encode_counts();
    assert!(
        first > 0 && after_first.encodes_total > 0,
        "the first publish after a load must checkpoint -- the countdown is serde-skipped and \
         therefore DUE on a fresh load, which is the safe direction. It reported {first} bytes \
         and encoded {} index(es)",
        after_first.encodes_total
    );

    crate::engine::shard_write_guard::reset_index_encode_counts();
    let second = engine
        .publish_shard_index_snapshot_for_keys(SHARD, vec![key_at(1)])
        .expect("second publish");
    let after_second = crate::engine::shard_write_guard::index_encode_counts();
    assert_eq!(
        0, after_second.encodes_total,
        "the second consecutive publish materialized the base index on the shipped default, so \
         the default is behaving as 1 whatever the constant says"
    );
    assert_eq!(
        0, second,
        "the second publish encoded no index and reported {second} bytes"
    );
}
