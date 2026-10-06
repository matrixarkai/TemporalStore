// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH RELOAD PATH RAN, AND THE MEMBERSHIP SURVIVING BOTH.
//!
//! # THE PROBLEM THIS MODULE EXISTS FOR
//!
//! A reload that silently rebuilds from the WAL and one that read the persisted index END IN THE SAME
//! STATE. Both leave `index_format_version` at the current constant, both serve the right answers, and
//! nothing afterwards distinguishes them. So a test that reloads and checks its members proves the
//! store is correct and says NOTHING about which code produced it -- and this change moves a stored
//! format, so the path is exactly what needs proving.
//!
//! `persistence::index_load_path_counts` is the discriminator, added by this change for the reason
//! below rather than as scaffolding for these tests.
//!
//! # WHY THE STAMP BUMP MAKES THIS THE LARGEST OPERATIONAL CONSEQUENCE
//!
//! `SHARD_INDEX_FORMAT_VERSION` goes 3 -> 5. `load_index_inner` refuses any index whose stamp is
//! BELOW the constant and returns `Ok(None)`, which the caller cannot tell from an absent index, and
//! then replays the log. So every existing store pays a full replay on its first load under this
//! binary. That is the stamp working as designed; what was missing is any way to SEE it happen.
//!
//! # THE THREE THINGS ASSERTED
//!
//!   1. a reload of a store this binary wrote ACCEPTS the index -- so the tombstone entries and the
//!      pages that record removals survive a round trip through the persisted index itself, not just
//!      through a replay;
//!   2. a reload of a store stamped with the PREVIOUS version REFUSES it and replays;
//!   3. the membership is identical either way, and is the membership after the removal rather than
//!      before it. A counter saying "replayed" beside a resurrected member would be worse than no
//!      counter at all.
//!
//! Each arm asserts the counter tuple rather than a total: a sum cannot tell the two paths apart,
//! which is the whole distinction.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

const MEMBERS: usize = 6;
const KEY: &str = "trp-set";

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "tombstone-reload-path".to_string(),
        shard_uri: "local://tombstone-reload-path/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn members_of(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    assert!(response.status.ok, "SMEMBERS failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("SetMembers answered {other:?}"),
    }
}

/// (live, tombstoned) entries for the set under test.
fn entry_counts(engine: &TemporalEngine) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != KEY {
                continue;
            }
            if page.deleted {
                tombstoned += 1;
            } else {
                live += 1;
            }
        }
    }
    (live, tombstoned)
}

/// Seed the store, remove one member, and unload so the index is materialized.
fn seed_and_unload(dir: &std::path::Path) -> (BTreeSet<Vec<u8>>, Vec<u8>) {
    let engine = engine_on(dir);
    load_on(&engine);
    let members: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|m| format!("member-{m:03}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: KEY.to_string(),
                member: member.clone(),
            },
        );
    }
    // A FOLD FIRST, so the survivors share a page and the removed member's page is one a fold had
    // already rewritten -- the state #2028's refutation was about.
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let victim = members[2].clone();
    write(
        &engine,
        Command::SetRemove {
            key: KEY.to_string(),
            member: victim.clone(),
        },
    );
    let expected: BTreeSet<Vec<u8>> = members
        .iter()
        .filter(|m| **m != victim)
        .cloned()
        .collect();
    assert_eq!(
        expected,
        members_of(&engine),
        "the fixture does not hold what it expects BEFORE the unload"
    );
    let (live, tombstoned) = entry_counts(&engine);
    // THE DENOMINATOR, STATED FOR WHICHEVER PROJECTION IS FILING, and as the invariant rather than
    // as a number. The fixture FOLDS before removing, so the survivors share one page:
    //
    //   * per-element, one entry names each surviving member -- MEMBERS - 1 of them;
    //   * one entry a page, ONE entry names the folded page they all share.
    //
    // Either way exactly one tombstone names the member removed, and the membership assertion
    // directly above this one is what says the survivors are all there -- so this is a count of
    // how they are FILED and not of how many there are. Both of this module's tests failed here,
    // two assertions before the accept-or-replay decision they exist to drive, which is why this
    // is the fixture's statement to correct and not theirs.
    //
    // AND THESE TWO ARE THE TRIPWIRE FOR A STEP THAT HAS NOT HAPPENED YET. `SHARD_INDEX_FORMAT_VERSION`
    // does not move for the collapse, because no stored field does: the entry struct is unchanged
    // and the tombstone's element name was always a field the per-element path filled. What IS new
    // is the combination a gated index holds -- live entries carrying no element name beside
    // tombstones that carry one -- and today nothing in the load path looks at that. If a later
    // step makes the reader's acceptance check consider entry SHAPE rather than only the stamp
    // value, these two tests are where it would surface, because they are the only ones that drive
    // the accept-or-replay decision over a store this binary wrote itself. That is the moment the
    // stamp question reopens.
    let expected_live = if crate::engine::container_index_files_one_entry_a_page() {
        1
    } else {
        MEMBERS - 1
    };
    assert_eq!(
        (expected_live, 1),
        (live, tombstoned),
        "before the unload there are {live} live and {tombstoned} tombstone entries, where this \
         projection files {expected_live} live and 1"
    );
    // Unload materializes the base index, which is what the reload below has to read.
    engine.unload_shard(1);
    (expected, victim)
}

/// rust-internal: drives a real unload/reload and reads the load-path counters
#[test]
fn a_reload_of_a_store_this_binary_wrote_reads_the_index_rather_than_replaying() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (expected, victim) = seed_and_unload(dir.path());

    crate::engine::persistence::reset_index_load_path_counts();
    let engine = engine_on(dir.path());
    load_on(&engine);
    let (accepted, refused, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();

    println!("=== reload of a store written by THIS binary ===");
    println!("  load paths: accepted={accepted} refused_stale={refused} absent={absent} undecodable={undecodable}");
    let (live, tombstoned) = entry_counts(&engine);
    let seen = members_of(&engine);
    println!("  entries after the reload: {live} live, {tombstoned} tombstone");
    println!("  SMEMBERS returns {} member(s)", seen.len());

    // DENOMINATOR: exactly one load must have been classified, or the tuple below says nothing.
    assert_eq!(
        1,
        accepted + refused + absent + undecodable,
        "the reload classified {} index loads, not one -- so the arm below is not about a single \
         load and the counters cannot be attributed",
        accepted + refused + absent + undecodable
    );
    assert_eq!(
        1, accepted,
        "THE RELOAD DID NOT READ THE INDEX. accepted={accepted} refused_stale={refused} \
         absent={absent} undecodable={undecodable}. A store this binary just wrote must load through \
         its own index: if it replayed instead, the membership below would be right for the WRONG \
         REASON and the persisted index's handling of tombstone entries would be untested."
    );

    assert_eq!(
        expected, seen,
        "the reload does not serve the membership the removal left"
    );
    assert!(
        !seen.contains(&victim),
        "THE REMOVED MEMBER IS BACK AFTER A RELOAD THAT READ THE INDEX"
    );
    // THE SAME DENOMINATOR AS THE FIXTURE'S, ON THE OTHER SIDE OF THE ROUND TRIP. What this
    // assertion is FOR is the tombstone surviving -- one of them, either way -- and that half does
    // not move. The live half does: per-element one entry names each survivor, one entry a page
    // names the folded page they share.
    //
    // And the assertions above this one are what make that a count rather than a defect. The
    // accept-or-replay check passes, so the binary reads the index it wrote rather than replaying;
    // the membership is what the removal left; and the removed member is still absent. A short live
    // count here with those three holding is a statement about FILING.
    let expected_live = if crate::engine::container_index_files_one_entry_a_page() {
        1
    } else {
        MEMBERS - 1
    };
    assert_eq!(
        (expected_live, 1),
        (live, tombstoned),
        "the reload restored {live} live and {tombstoned} tombstone entries, where this projection \
         files {expected_live} live and 1. The tombstone entry must SURVIVE the round trip through \
         the persisted index -- if it does not, the pages stop recording the removal the moment a \
         store is reloaded, and a later derivation resurrects it."
    );
}

/// rust-internal: rewrites the stored stamp, then drives a real reload
#[test]
fn a_reload_after_the_stamp_bump_replays_rather_than_reading_the_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (expected, victim) = seed_and_unload(dir.path());

    // STAMP THE STORED INDEX WITH THE PREVIOUS VERSION, which is what every existing store looks
    // like to this binary.
    //
    // THERE ARE TWO STAMPS AND THIS IS THE INNER ONE. The base index is written as a BINARY container
    // whose first bytes carry an outer version, and changing that one makes `decode_index_bytes`
    // REFUSE -- a corrupt-index path, not the stale-stamp path. What `load_index_inner` refuses on is
    // the `index_format_version` FIELD inside the decoded state, which means the bytes must decode
    // cleanly and then be judged. So the index is decoded, its field lowered, and re-serialized: and
    // it is re-serialized as JSON deliberately, because `serialize_index_stamped` writes the binary
    // container only when the field EQUALS the current constant and JSON otherwise, so a
    // down-stamped state cannot be written in the binary form at all.
    let index_path = dir.path().join("indexes").join("shard-1.index.json");
    let raw = std::fs::read(&index_path).expect("the unload wrote a base index");
    let mut stored =
        crate::engine::decode_index_bytes(&raw).expect("the stored base index decodes");
    let current = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    assert_eq!(
        current, stored.index_format_version,
        "the stored index is stamped {} and this binary writes {current}; this fixture lowers a stamp \
         it must first be able to read",
        stored.index_format_version
    );
    stored.index_format_version = current - 1;
    std::fs::write(
        &index_path,
        serde_json::to_vec(&stored).expect("re-serialize the down-stamped state as JSON"),
    )
    .expect("write the down-stamped index");
    // AND THE DOWN-STAMPED FILE STILL DECODES, which is the precondition for the arm below testing
    // the stale-stamp path rather than the undecodable one.
    let reread = std::fs::read(&index_path).expect("re-read");
    let decoded =
        crate::engine::decode_index_bytes(&reread).expect("the down-stamped index still decodes");
    assert_eq!(
        current - 1,
        decoded.index_format_version,
        "the down-stamp did not survive the round trip, so the arm below is not testing a stale stamp"
    );

    crate::engine::persistence::reset_index_load_path_counts();
    let engine = engine_on(dir.path());
    load_on(&engine);
    let (accepted, refused, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();

    println!("=== reload of a store stamped {} (this binary writes {current}) ===", current - 1);
    println!("  load paths: accepted={accepted} refused_stale={refused} absent={absent} undecodable={undecodable}");
    let seen = members_of(&engine);
    let (live, tombstoned) = entry_counts(&engine);
    println!("  entries after the replay: {live} live, {tombstoned} tombstone");
    println!("  SMEMBERS returns {} member(s)", seen.len());

    assert_eq!(
        1,
        accepted + refused + absent + undecodable,
        "the reload classified {} index loads, not one",
        accepted + refused + absent + undecodable
    );
    assert_eq!(
        1, refused,
        "THE STALE STAMP WAS NOT REFUSED. accepted={accepted} refused_stale={refused} \
         absent={absent} undecodable={undecodable}. If the index was ACCEPTED then the bump from 3 \
         to 5 does not gate anything and an index written before the page format changed would be \
         served as if it had not."
    );
    assert_eq!(
        0, accepted,
        "the index was both refused and accepted, which cannot be one load"
    );

    // AND THE REPLAY REBUILDS THE MEMBERSHIP THE REMOVAL LEFT.
    assert_eq!(
        expected, seen,
        "the replay does not rebuild the membership the removal left: it serves {} member(s) where \
         {} were expected",
        seen.len(),
        expected.len()
    );
    assert!(
        !seen.contains(&victim),
        "THE REMOVED MEMBER IS BACK AFTER A REPLAY. The removal's outcome carries the tombstone \
         page's address so the replay can re-file the entry that keeps it reachable; if that address \
         is not carried, replay re-drops the entry and the page is named by nothing."
    );
    assert_eq!(
        MEMBERS - 1,
        live,
        "the replay rebuilt {live} live entries for {} live members",
        MEMBERS - 1
    );

    // AND THE REPLAY RE-FILES THE TOMBSTONE ENTRY, FROM THE OUTCOME'S ADDRESS.
    //
    // THIS WAS A REFUTATION BEFORE IT WAS A FIX, and the distinction it turns on is why the outcome
    // carries an address at all. The membership a replay serves was always correct: the WAL outcome
    // states the removal and the replay applies it to the resident map and the index. What did NOT
    // survive was the ENTRY that keeps the tombstone PAGE reachable -- a replay runs precisely when the
    // index was not usable, so there is no index to preserve it from, and the model maps cannot
    // re-derive it because a removed element is not in them.
    //
    // So the tombstone page's address travels in the removal's outcome and the replay's install hands
    // it back. Drop it and the replay re-drops the entry, the page is named by nothing, and a
    // derivation after the recovery puts the element back: the original defect, on the one path where
    // it is hardest to see.
    assert_eq!(
        1, tombstoned,
        "THE REPLAY REBUILT {tombstoned} TOMBSTONE ENTRIES, NOT ONE. Zero means the removal is no \
         longer recorded in the pages after a recovery, so a membership derived from the pages would \
         resurrect the member. The outcome carries the tombstone page's address for exactly this; if \
         the install stops passing it, or the outcome stops carrying it, this is what fails."
    );
    println!(
        "  the replay refused the stale index, rebuilt the membership from the log, and re-filed the \
         tombstone entry from the outcome's address"
    );
}
