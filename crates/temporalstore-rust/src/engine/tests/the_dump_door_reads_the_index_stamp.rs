// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! EVERY DOOR THAT TRUSTS A STORED INDEX NOW READS ITS STAMP, AND THESE ARMS DRIVE EACH ONE.
//!
//! # THE DEFECT THIS CLOSES
//!
//! `SHARD_INDEX_FORMAT_VERSION` exists because a stale index decodes CLEANLY -- the types are
//! unchanged -- while a field's MEANING has moved under it. The real instance is in the field's
//! own doc comment: `context_events` was rekeyed from `timeline_key` to `event_id_hash`, so a v1
//! index decodes with timeline keys sitting in the event-id slot and every time-windowed read
//! silently returns nothing.
//!
//! That constant is a claim about every door that turns stored bytes into a `ShardState` somebody
//! acts on. It was enforced at exactly ONE of them. `decode_index_bytes` has six production
//! callers and only `load_index_inner` compared the stamp; the version check inside
//! `decode_index_bytes` is on the msgpack-container arm and a plain-JSON index never meets it.
//!
//! Measured before this change: a stamp-13 image installed through
//! `install_bucket_dump_manifest` reached a live shard with all four `index_load_path_counts`
//! counters at ZERO. It did not skip the refusal -- it never entered the code the refusal lived
//! in. And the worst door was not an endpoint: the ordinary recovery load reads a durable dump
//! manifest's embedded index as its recovery base, so an old store with an old manifest met no
//! gate on a plain restart.
//!
//! # HOW A STALE IMAGE IS SPELLED NOW
//!
//! The plain-JSON reader is retired, so the stamp has one home: the container header, which
//! `decode_index_bytes` compares with `SHARD_INDEX_FORMAT_VERSION` before it reads a byte of the
//! payload. A stale image is this build's payload behind a header that names another version
//! (`encode_index_bytes_at_version`); it never decodes, and every door refuses it with the decode's
//! own `Err`. The arms below assert that refusal by code and by message, with
//! `index_load_path_counts` as corroboration. THE HEADER CHECK IS WHAT EACH OF THEM DEPENDS ON:
//! remove it and the stale image decodes, carries its stamp, and no door compares it, so every
//! refusal arm goes red.
//!
//! # WHAT THIS MODULE IS FOR
//!
//! Eight arms, because a gate needs both halves driven and a behaviour change needs its
//! compatibility proved:
//!
//!   1, 2  the base-index door, refusing and accepting -- the comparator the rest are read
//!         against, and the door that already worked;
//!   3, 4  the dump-manifest install REFUSES, and refuses BEFORE it can write anything;
//!   5     the same manifest at the current stamp installs and round-trips its payload, so arm 3
//!         is the stamp and not a broken fixture;
//!   6     the ORDINARY recovery load refuses and falls back rather than failing;
//!   7     a manifest this binary actually writes still installs -- the arm that would catch this
//!         change refusing legitimate traffic;
//!   8     the preflight report names the blocker, so an operator sees it before trying.
//!
//! # THE PAYLOAD IS A `features` SERIES ON PURPOSE
//!
//! The measured data loss was a pre-fold `sequences` map, but that map is being removed in a
//! separate change on top of this one. A payload carried in `features` asks the identical
//! question -- does an object-keyed timestamped series survive this door -- and keeps these arms
//! meaningful after that removal lands.
//!
//! # WHY THE RETURN VALUES ARE THE EVIDENCE
//!
//! `index_load_path_counts` is three process-global atomics, so a parallel test's load moves them.
//! Every arm below asserts on something scoped to its own call -- a `Result`, a served response,
//! the presence of a file -- and treats the counters as corroboration, printed and asserted only
//! in the direction that can misreport as a pass rather than as a failure.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

use crate::block_store::ElementEntry;

const SHARD: ShardId = 11;
const SERIES_KEY: &str = "series/written-before-the-stamp-moved";
const ROW_TIMESTAMP_MS: u64 = 1_700_000_000_123;

/// A source shard with real blocks and a real dump manifest over it.
struct DumpFixture {
    dir: tempfile::TempDir,
    manifest: crate::engine::reports::BucketDumpManifest,
}

/// Seed a shard, give it a `features` series, and take a real manifest over the result.
fn a_real_dump_manifest() -> DumpFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    for i in 0..6 {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::StringSet {
                key: format!("seed-{i}"),
                value: format!("seed-value-{i}").into_bytes(),
            },
        });
        assert!(response.status.ok, "seeding write {i}: {response:?}");
    }
    // The payload every arm tracks, written into the live shard so the manifest carries it the
    // way a real dump would rather than being edited in afterwards.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
        let mut series = BTreeMap::new();
        series.insert(
            ROW_TIMESTAMP_MS,
            ElementEntry::from_parts(1, 4_096, 128, Some(3), Some(7)),
        );
        shard.features_mut().insert(SERIES_KEY.into(), series.into());
    }
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("a dump manifest over a seeded shard");
    // DENOMINATORS, both of them. An empty image or a manifest that never carried the payload
    // would make every arm below pass over nothing.
    assert!(
        !manifest.index_bytes.is_empty(),
        "the manifest carries no index image, so there is nothing for a door to trust"
    );
    assert!(
        crate::engine::decode_index_bytes(&manifest.index_bytes)
            .expect("a manifest this binary wrote must decode")
            .features()
            .contains_key(SERIES_KEY),
        "the manifest's image does not carry the series, so the payload arms are about nothing"
    );
    DumpFixture { dir, manifest }
}

/// Re-stamp a real manifest's embedded index, leaving everything else about it real.
///
/// Its blocks, slab ids, bucket summaries and log sequences are untouched, so validation and
/// preflight pass or fail for the same reasons they do in production and the only variable is the
/// stamp. The stamp is the container HEADER's -- the one place the decode compares a struct
/// version -- so the image is this build's payload behind a header that says `stamp`.
fn re_stamp(manifest: &mut crate::engine::reports::BucketDumpManifest, stamp: u32) {
    let restored = crate::engine::decode_index_bytes(&manifest.index_bytes)
        .expect("a manifest this binary wrote must decode");
    manifest.index_bytes = crate::engine::encode_index_bytes_at_version(&restored, stamp);
    manifest.index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
    manifest.dump_generation_id = bucket_dump_generation_id(manifest);
    manifest.checksum =
        bucket_dump_manifest_checksum(manifest).expect("the manifest checksum recomputes");
    // THE FIXTURE CHECKS ITSELF, in the direction each arm needs: a current-stamped image decodes
    // at its stamp, and a stale one is refused on its header's struct version. `serialize_index`
    // writes the current header whatever the shard's own field says, which is exactly how a
    // down-stamped image silently becomes a current one and an arm that means to test a refusal
    // tests an acceptance.
    let read_back = crate::engine::decode_index_bytes(&manifest.index_bytes);
    if stamp == SHARD_INDEX_FORMAT_VERSION {
        assert_eq!(
            stamp,
            read_back
                .expect("the re-stamped current image decodes")
                .index_format_version,
            "the re-stamped current image reads back at another stamp"
        );
    } else {
        let refusal = read_back
            .err()
            .expect("a re-stamped stale image must be refused by the decode");
        assert!(
            refusal.contains("struct version") && refusal.contains(&stamp.to_string()),
            "the re-stamped image was refused, but not on its header's struct version: {refusal}"
        );
    }
}

/// A restore target over the SAME blocks, with its own index dir and its shard not loaded.
///
/// Not loaded on purpose: that is the install-then-load order the install path's own comment
/// calls "the production order, reached over `POST /server/storage/dumps/install`".
fn a_restore_target(dir: &std::path::Path) -> (TemporalEngine, std::path::PathBuf) {
    let index_dir = dir.join("restore-indexes");
    std::fs::create_dir_all(&index_dir).expect("mkdir restore-indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("restore-cache"),
        dir.join("blocks"),
        &index_dir,
    );
    (engine, index_dir)
}

/// Does a persisted base index exist for this shard at all?
fn persisted_index_exists(index_dir: &std::path::Path, shard: ShardId) -> bool {
    index_dir.join(format!("shard-{shard}.index.json")).exists()
}

/// Does the persisted base index carry the payload series?
///
/// WHY THIS DECODES AND DOES NOT SEARCH THE FILE, AND IT IS A RECORDED CORRECTION -- THE SAME
/// MISTAKE CAUGHT TWICE.
///
/// A first version of this probe asked whether the index FILE spelled the key, and read `false`
/// for data that had demonstrably survived. The index is not stored as text: `serialize_index`
/// wraps it in a `TSIDX` container with the compressed MessagePack codec (2). It used to depend on
/// the shard: a stale-stamped one took a compressed JSON codec (1) and a text search answered
/// "absent" for a present key there, and it cannot be run at all under codec 2.
///
/// The first version of that mistake was caught by a positive control reading `false` for data
/// that had survived. The second was caught by the probe's own codec assertion firing instead of
/// quietly answering the wrong thing -- which is why that assertion was there rather than a
/// lossy fallback read.
///
/// `decode_index_bytes` answers it under any codec. The one question it CANNOT answer is whether
/// a field the struct no longer has is still in the bytes -- serde drops that at the decode --
/// and no arm here asks it: the payload lives in `features`, which this binary has.
fn persisted_index_has_series(index_dir: &std::path::Path, shard: ShardId) -> bool {
    let path = index_dir.join(format!("shard-{shard}.index.json"));
    let bytes = std::fs::read(path).expect("the install persisted a base index");
    crate::engine::decode_index_bytes(&bytes)
        .expect("the persisted base index decodes")
        .features()
        .contains_key(SERIES_KEY)
}

/// Whether the shard still serves one of the seeded strings.
fn serves_a_seeded_string(engine: &TemporalEngine) -> bool {
    matches!(
        engine
            .execute(crate::types::ExecuteRequest {
                shard_id: SHARD,
                command: crate::types::Command::StringGet {
                    key: "seed-0".to_string(),
                },
            })
            .response,
        crate::types::CommandResponse::Bytes { value: Some(_) }
    )
}

// ---------------------------------------------------------------------------------------------
// ARMS 1 AND 2: the base-index door. The comparator, and the door that already worked.
// ---------------------------------------------------------------------------------------------

/// Plant a whole index image at `stamp` where the loader looks.
fn engine_over_a_planted_base_index(stamp: u32) -> (tempfile::TempDir, TemporalEngine) {
    let mut state = ShardState::default();
    let mut series = BTreeMap::new();
    series.insert(
        ROW_TIMESTAMP_MS,
        ElementEntry::from_parts(1, 4_096, 128, Some(3), Some(7)),
    );
    state.features_mut().insert(SERIES_KEY.into(), series.into());
    // The stamp is the header's: this payload behind a header that says `stamp`.
    let image = crate::engine::encode_index_bytes_at_version(&state, stamp);

    let dir = tempfile::tempdir().expect("tempdir");
    let indexes = dir.path().join("indexes");
    std::fs::create_dir_all(&indexes).expect("mkdir indexes");
    std::fs::write(indexes.join(format!("shard-{SHARD}.index.json")), &image)
        .expect("plant the index image");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        indexes,
    );
    (dir, engine)
}

/// The door that always read the stamp still reads it, through the shared predicate.
#[test]
fn the_base_index_door_refuses_a_stale_stamp() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let (_dir, engine) = engine_over_a_planted_base_index(stale);
    crate::engine::persistence::reset_index_load_path_counts();
    let loaded = engine
        .load_index_checked(SHARD, false)
        .expect("a refusal is Ok(None), not Err");
    let (accepted, absent, undecodable) = crate::engine::persistence::index_load_path_counts();
    println!(
        "\n=== base door, stamp {stale} against {SHARD_INDEX_FORMAT_VERSION} ===\n  \
         accepted {accepted}, absent {absent}, undecodable {undecodable}"
    );
    assert!(
        loaded.is_none(),
        "the base-index door accepted a stale-stamped image; the header's struct-version check \
         that this whole module rests on is gone or inverted"
    );
    assert!(undecodable > 0, "the refusal was not counted");
}

/// And accepts the same image at the current stamp, so arm 1 is not a door that refuses
/// everything.
#[test]
fn the_base_index_door_accepts_the_same_image_at_the_current_stamp() {
    let (_dir, engine) = engine_over_a_planted_base_index(SHARD_INDEX_FORMAT_VERSION);
    let loaded = engine
        .load_index_checked(SHARD, false)
        .expect("the load must not error");
    let state = loaded.expect(
        "the same image at the CURRENT stamp was refused, so arm 1 proves nothing about the stamp",
    );
    assert!(
        state.features().contains_key(SERIES_KEY),
        "the accepted image did not carry the series, so this control is about nothing"
    );
}

// ---------------------------------------------------------------------------------------------
// ARMS 3 AND 4: the dump-manifest install. THE NEW BEHAVIOUR.
// ---------------------------------------------------------------------------------------------

/// THE INSTALL DOOR NOW REFUSES, WHERE IT USED TO INSTALL SILENTLY.
#[test]
fn the_dump_manifest_install_refuses_a_stale_stamped_image() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let mut fixture = a_real_dump_manifest();
    re_stamp(&mut fixture.manifest, stale);

    let (target, _index_dir) = a_restore_target(fixture.dir.path());
    let installed = target.install_bucket_dump_manifest(&fixture.manifest);
    println!(
        "\n=== install of a stamp-{stale} manifest ===\n  install -> {installed:?}"
    );
    let status = installed.expect_err(
        "install_bucket_dump_manifest ACCEPTED a stale-stamped image. That is the defect this \
         change exists to close: the image reaches a live shard and a persisted index without \
         ever meeting the stamp",
    );
    assert_eq!(
        "slot_dump_invalid_index", status.code,
        "the install refused with {:?} rather than in the decode, so this arm is measuring some \
         other guard -- a fixture that fails validation for an unrelated reason refuses correctly \
         and proves nothing",
        status.code
    );
    assert!(
        status.message.contains("struct version")
            && status.message.contains(&stale.to_string())
            && status.message.contains(&SHARD_INDEX_FORMAT_VERSION.to_string()),
        "the install refused in the decode, but not on the header's struct version, so the \
         refusal is not attributable to the stamp: {status:?}"
    );
}

/// AND IT REFUSES BEFORE IT CAN DESTROY ANYTHING.
///
/// The old behaviour was not merely "installs an image it should not": the install re-serialized
/// the decoded state into the durable base index, so whatever the decode dropped was written away.
/// Refusing inside `validate_bucket_dump_manifest` -- which every install calls first -- is what
/// puts the refusal BEFORE the first write rather than after it.
#[test]
fn the_refused_install_writes_no_index_and_installs_no_shard() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let mut fixture = a_real_dump_manifest();
    re_stamp(&mut fixture.manifest, stale);

    let (target, index_dir) = a_restore_target(fixture.dir.path());
    target
        .install_bucket_dump_manifest(&fixture.manifest)
        .expect_err("arm 3 is what establishes that this refuses");

    let wrote_an_index = persisted_index_exists(&index_dir, SHARD);
    println!(
        "\n=== after the refused install ===\n  a base index was written: {}\n  the shard is \
         loaded: {}",
        wrote_an_index,
        target
            .shards
            .read()
            .expect("engine lock poisoned")
            .contains_key(&SHARD)
    );
    assert!(
        !wrote_an_index,
        "the refused install still wrote a base index. The refusal has to come before the first \
         write, or a store is left describing an image this binary has just said it cannot read"
    );
    assert!(
        !target
            .shards
            .read()
            .expect("engine lock poisoned")
            .contains_key(&SHARD),
        "the refused install still installed a shard state"
    );
}

// ---------------------------------------------------------------------------------------------
// ARM 5: the attribution control. The gate is the stamp, not the fixture.
// ---------------------------------------------------------------------------------------------

/// THE SAME MANIFEST AT THE CURRENT STAMP INSTALLS, AND ITS SERIES SURVIVES END TO END.
///
/// Without this, arms 3 and 4 pass for a fixture that could never have installed at all -- a
/// manifest broken in some unrelated way refuses correctly and says nothing about the stamp.
#[test]
fn the_same_manifest_at_the_current_stamp_installs_and_round_trips_its_series() {
    let mut fixture = a_real_dump_manifest();
    // Through the same re-stamping helper as the refused arms, so the ONLY difference between
    // this arm and arm 3 is the number.
    re_stamp(&mut fixture.manifest, SHARD_INDEX_FORMAT_VERSION);

    let (target, index_dir) = a_restore_target(fixture.dir.path());
    target
        .install_bucket_dump_manifest(&fixture.manifest)
        .expect("a current-stamped manifest must still install");

    let on_disk = persisted_index_has_series(&index_dir, SHARD);
    target.load_shard(SHARD);
    let loaded = {
        let shards = target.shards.read().expect("engine lock poisoned");
        shards
            .get(&SHARD)
            .expect("the shard is loaded")
            .features()
            .contains_key(SERIES_KEY)
    };
    println!(
        "\n=== the same manifest at stamp {SHARD_INDEX_FORMAT_VERSION} ===\n  \
         series in the persisted index: {on_disk}\n  series in `features` after the load: {loaded}"
    );
    assert!(
        on_disk && loaded,
        "the current-stamped manifest installed but its series did not survive (on disk \
         {on_disk}, loaded {loaded}). Then the refusals above are not attributable to the stamp: \
         this door loses an object-keyed series map whatever its stamp says, which is a separate \
         and larger finding"
    );
}

// ---------------------------------------------------------------------------------------------
// ARM 6: the door nobody has to call. An ordinary restart.
// ---------------------------------------------------------------------------------------------

/// THE ORDINARY RECOVERY LOAD REFUSES A STALE MANIFEST AND FALLS BACK RATHER THAN FAILING.
///
/// This is the door that made the defect serious: `load_shard_with`'s single-barrier arm -- the
/// DEFAULT, since `wal_single_barrier()` is `!wal_legacy_recovery()` -- reads a durable dump
/// manifest's embedded index as its recovery base, decoding it directly. No endpoint is involved;
/// a store with an old manifest on disk met this on a plain restart.
///
/// The refusal here is a FALLBACK, not a failure: the arm answers with the base file and the log
/// instead, which is the same trade `load_index_inner` makes. So the load must still succeed.
#[test]
fn the_ordinary_recovery_load_refuses_a_stale_dump_manifest_and_falls_back() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let mut fixture = a_real_dump_manifest();
    re_stamp(&mut fixture.manifest, stale);
    assert!(
        fixture.manifest.wal_sequence > 0,
        "DENOMINATOR: the manifest anchors at 0, so it is not newer than an absent base index and \
         the recovery arm this drives is never entered"
    );

    let (target, index_dir) = a_restore_target(fixture.dir.path());
    // A real pre-change store has both of these beside each other, and the routing-range stamp is
    // what keeps `decide_routing_range` from refusing before the index is read at all -- a
    // refusal for THAT reason leaves every index-load counter at zero and would read exactly like
    // the one under test.
    target
        .persist_bucket_dump_manifest(&fixture.manifest)
        .expect("plant the durable manifest the load will find");
    crate::engine::routing_range_stamp::write_routing_range_stamp(
        &index_dir,
        SHARD,
        crate::engine::routing_range_stamp::RoutingRangeStamp {
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
        },
    )
    .expect("plant the routing-range stamp");

    crate::engine::persistence::reset_index_load_path_counts();
    // `load_shard_with`, not `load_shard`: the plain one discards the status, and whether this
    // load SUCCEEDS is half of what this arm asserts.
    let response = target.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    let (accepted, absent, undecodable) = crate::engine::persistence::index_load_path_counts();
    let served = serves_a_seeded_string(&target);
    println!(
        "\n=== ordinary load over a stamp-{stale} durable manifest ===\n  load status ok: {}\n  \
         accepted {accepted}, absent {absent}, undecodable {undecodable}\n  serves a seeded \
         string from the manifest: {served}",
        response.status.ok
    );
    assert!(
        response.status.ok,
        "the load FAILED rather than falling back: {:?}. A stale manifest should take the same \
         route an undecodable one already took -- the base file and the log -- because refusing \
         the whole load makes a recoverable store unopenable",
        response.status
    );
    assert!(
        undecodable > 0,
        "nothing was counted as a refused index image, so this load did not reach the manifest \
         arm at all and the arm proves nothing: accepted {accepted}, absent {absent}, \
         undecodable {undecodable}"
    );
    assert!(
        !served,
        "the shard served a seeded string, so the refused manifest's image was used as the \
         recovery base after all -- there is no write-ahead log in this target, so anything \
         served here came out of the image the load was supposed to refuse"
    );
}

// ---------------------------------------------------------------------------------------------
// ARM 7: the compatibility arm. This change refuses restores; it must not refuse THESE.
// ---------------------------------------------------------------------------------------------

/// A MANIFEST THIS BINARY WRITES IS STAMPED CURRENT AND STILL INSTALLS, WITH NO EDITING AT ALL.
///
/// The arm that would catch this change breaking every live restore. `create_bucket_dump_manifest`
/// takes its image from `export_index_bytes` -> `load_served_index_bytes` -> `serialize_index`,
/// and `serialize_index` re-stamps a stale shard on the way out -- so the image is stamped
/// current however the in-memory shard was stamped. That is read off the code; this drives it.
#[test]
fn a_manifest_this_binary_writes_is_stamped_current_and_still_installs() {
    let fixture = a_real_dump_manifest();
    let stamp = crate::engine::decode_index_bytes(&fixture.manifest.index_bytes)
        .expect("a manifest this binary wrote must decode")
        .index_format_version;
    println!(
        "\n=== a manifest this binary wrote ===\n  embedded index stamp: {stamp} (current is \
         {SHARD_INDEX_FORMAT_VERSION})"
    );
    assert_eq!(
        SHARD_INDEX_FORMAT_VERSION, stamp,
        "a manifest this binary just wrote carries stamp {stamp}, so the gate added by this \
         change refuses the store's OWN dumps and every restore is broken"
    );

    let (target, index_dir) = a_restore_target(fixture.dir.path());
    target
        .install_bucket_dump_manifest(&fixture.manifest)
        .expect("an unedited manifest must install");
    assert!(
        persisted_index_exists(&index_dir, SHARD),
        "the install reported success and wrote no base index"
    );
    target.load_shard(SHARD);
    assert!(
        serves_a_seeded_string(&target),
        "the restore target does not serve what the dump held, so this compatibility arm passed \
         over an install that did nothing"
    );
}

// ---------------------------------------------------------------------------------------------
// ARM 8: and an operator can see it coming.
// ---------------------------------------------------------------------------------------------

/// THE PREFLIGHT REPORT BLOCKS A STALE-STAMPED IMAGE, SO `install_safe` AGREES WITH THE INSTALL.
///
/// A report that calls a manifest installable and an install that then refuses it is a preflight
/// nobody can act on. The report decodes the image the way the install does, so an image whose
/// header names another struct version is the blocker `invalid_manifest_index` -- and the control
/// is the same manifest at the CURRENT stamp, which carries no such blocker, so the blocker is the
/// stamp's and not the fixture's.
#[test]
fn the_preflight_report_names_the_stale_stamp_as_a_blocker() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let mut fixture = a_real_dump_manifest();
    re_stamp(&mut fixture.manifest, stale);

    let (target, _index_dir) = a_restore_target(fixture.dir.path());
    let report = target.bucket_dump_install_preflight_report(&fixture.manifest);
    println!(
        "\n=== preflight over a stamp-{stale} manifest ===\n  install_safe: {}\n  blockers: {:?}",
        report.install_safe, report.blockers
    );
    assert!(
        report
            .blockers
            .iter()
            .any(|blocker| blocker == "invalid_manifest_index"),
        "the preflight did not block the stale image: blockers {:?}. An operator would be told \
         this manifest is installable and the install would then refuse it",
        report.blockers
    );
    assert!(
        !report.install_safe,
        "the preflight named the blocker and still reported install_safe"
    );

    // THE CONTROL: the same manifest re-stamped at the CURRENT stamp is not blocked on its image.
    let mut current = a_real_dump_manifest();
    re_stamp(&mut current.manifest, SHARD_INDEX_FORMAT_VERSION);
    let (control_target, _control_dir) = a_restore_target(current.dir.path());
    let control = control_target.bucket_dump_install_preflight_report(&current.manifest);
    assert!(
        !control
            .blockers
            .iter()
            .any(|blocker| blocker == "invalid_manifest_index"),
        "the current-stamped control was blocked on its image too: {:?}, so the blocker above is \
         not attributable to the stamp",
        control.blockers
    );
}
