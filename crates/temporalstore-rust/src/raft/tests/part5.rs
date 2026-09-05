// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 5: incremental raft WAL records, and the networked-election primitives
//! the production timer loop drives.
#![allow(clippy::all)]
use super::helpers::*;
use super::*;

fn wal_dir_bytes(root: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                walk(&entry.path(), total);
            } else {
                *total += meta.len();
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

fn propose_range(cluster: &RaftCluster, from: usize, to: usize) {
    for i in from..to {
        cluster
            .propose(Command::StringSet {
                key: format!("wal-cost-{i}"),
                value: vec![7u8; 32],
            })
            .unwrap();
    }
}

/// The WAL record Raft hands to `persist_configured_wal` carries the node's whole log.
/// Writing that verbatim on every append makes each append cost O(log length), which is
/// what made write latency climb with the log. Appends must instead cost about the same
/// no matter how long the log already is.
#[test]
fn wal_append_cost_stays_flat_as_the_log_grows() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 91, [1], RaftConfig::default()).unwrap();

    propose_range(&cluster, 0, 50);
    let before_early = wal_dir_bytes(dir.path());
    propose_range(&cluster, 50, 100);
    let early_block = wal_dir_bytes(dir.path()) - before_early;

    propose_range(&cluster, 100, 450);
    let before_late = wal_dir_bytes(dir.path());
    propose_range(&cluster, 450, 500);
    let late_block = wal_dir_bytes(dir.path()) - before_late;

    // With a full-log payload the later block rewrites a log ~6x longer on every append,
    // so it costs ~6x the earlier block. Incremental records keep the two comparable.
    assert!(
        late_block < early_block * 2,
        "50 appends at log length ~450 cost {late_block} bytes vs {early_block} at length ~50; \
         the payload is still growing with the log"
    );
}

/// Folding incremental records back together has to reproduce the log exactly, including
/// across the segment rotations that write a fresh base record.
#[test]
fn incremental_wal_records_recover_the_whole_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = RaftConfig::default();
    // Small segments so the run crosses several rotations.
    config.max_segment_bytes = 4096;
    config.min_keep_segment_num = 64;
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 92, [1], config.clone()).unwrap();
    for i in 0..200 {
        cluster
            .propose(Command::StringSet {
                key: format!("recover-{i}"),
                value: format!("value-{i}").into_bytes(),
            })
            .unwrap();
    }
    let expected = cluster.status().commit_index;
    assert_eq!(expected, 200);

    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 92, [1], config).unwrap();
    assert_eq!(restored.status().commit_index, expected);
    let response = restored
        .read_local(
            1,
            Command::StringGet {
                key: "recover-199".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        response,
        CommandResponse::Bytes {
            value: Some(b"value-199".to_vec())
        }
    );
    let wal = LocalRaftWal::new(dir.path());
    let record = wal.load_node(92, 1).unwrap().unwrap();
    assert_eq!(record.entries.len(), 200);
    assert_eq!(record.entries.last().unwrap().index, 200);
}

/// A raft leader change can truncate a divergent suffix. The entry the WAL last wrote is
/// then gone, so an incremental record would fold onto a log that no longer exists --
/// the writer has to notice and re-base instead.
#[test]
fn wal_rebases_when_the_log_suffix_is_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 93, [1], RaftConfig::default()).unwrap();
    for i in 0..20 {
        cluster
            .propose(Command::StringSet {
                key: format!("conflict-{i}"),
                value: vec![1u8; 8],
            })
            .unwrap();
    }
    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(93, 1).unwrap().unwrap();
    assert_eq!(record.entries.len(), 20);

    // Simulate a new leader overwriting indexes 11..=15 at a higher term.
    record.entries.truncate(10);
    for index in 11..=15u64 {
        record.entries.push(RaftLogEntry {
            term: 9,
            index,
            shard_id: 93,
            command: Command::StringSet {
                key: format!("overwritten-{index}"),
                value: vec![2u8; 8],
            },
        });
    }
    record.hard_state.commit_index = 15;
    wal.persist_node_segmented(93, 1, &record, 1024 * 1024, 8)
        .unwrap();

    let recovered = wal.recover_node_segmented(93, 1).unwrap().record.unwrap();
    assert_eq!(recovered.entries.len(), 15);
    assert_eq!(recovered.entries.last().unwrap().index, 15);
    assert_eq!(recovered.entries.last().unwrap().term, 9);
    assert!(recovered
        .entries
        .iter()
        .all(|entry| entry.index <= 10 || entry.term == 9));
}

/// A snapshot compaction drops a prefix of the log. Recovery must drop it too rather
/// than resurrect entries from the base record it folded onto.
#[test]
fn wal_recovery_drops_entries_compacted_away() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 94, [1], RaftConfig::default()).unwrap();
    for i in 0..20 {
        cluster
            .propose(Command::StringSet {
                key: format!("compact-{i}"),
                value: vec![3u8; 8],
            })
            .unwrap();
    }
    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(94, 1).unwrap().unwrap();
    // Keep only the tail, as a snapshot install would.
    record.entries.retain(|entry| entry.index > 12);
    wal.persist_node_segmented(94, 1, &record, 1024 * 1024, 8)
        .unwrap();

    let recovered = wal.recover_node_segmented(94, 1).unwrap().record.unwrap();
    assert_eq!(recovered.entries.first().unwrap().index, 13);
    assert_eq!(recovered.entries.len(), 8);
}

/// `prepare_campaign` must make the term bump and self-vote durable before any vote is
/// requested, and `conclude_campaign` must refuse to install a minority leader.
#[test]
fn networked_campaign_promotes_only_with_a_majority() {
    let cluster = RaftCluster::new_single_shard(95, [1, 2, 3]);
    assert_eq!(cluster.leader_id(), 1);
    let before_term = cluster.status().current_term;

    let template = cluster.prepare_campaign(2).unwrap();
    assert_eq!(template.candidate_id, 2);
    assert_eq!(template.shard_id, 95);
    assert!(template.term > before_term);

    // One grant (its own) out of three voters is a minority.
    assert!(!cluster.conclude_campaign(2, template.term, 1).unwrap());
    assert_eq!(
        cluster.leader_id(),
        1,
        "a minority campaign must not install a leader"
    );

    // Two of three is a majority.
    assert!(cluster.conclude_campaign(2, template.term, 2).unwrap());
    assert_eq!(cluster.leader_id(), 2);
    assert_eq!(cluster.status().current_term, template.term);
}

/// A peer answering with a newer term means someone else has already moved on; the
/// candidate has to step down instead of installing itself at a stale term.
#[test]
fn campaign_stands_down_on_a_newer_peer_term() {
    let cluster = RaftCluster::new_single_shard(96, [1, 2, 3]);
    let template = cluster.prepare_campaign(3).unwrap();
    let candidate_term = |cluster: &RaftCluster| {
        cluster
            .status()
            .nodes
            .into_iter()
            .find(|node| node.node_id == 3)
            .map(|node| node.current_term)
            .unwrap()
    };
    assert_eq!(candidate_term(&cluster), template.term);

    cluster.observe_higher_term(3, template.term + 5).unwrap();
    assert_eq!(candidate_term(&cluster), template.term + 5);

    // Votes arrive over the network, so they can land after the candidate has already
    // stepped down. A unanimous tally for the abandoned term must not install it.
    assert!(
        !cluster.conclude_campaign(3, template.term, 3).unwrap(),
        "grants for an abandoned term must not promote"
    );
    assert_eq!(cluster.leader_id(), 1);
}

/// The follower side of failure detection: a leader's AppendEntries is the only signal
/// that it is still there, so accepting one has to be observable.
#[test]
fn leader_contact_epoch_advances_on_append_entries() {
    let cluster = RaftCluster::new_single_shard(97, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "contact".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let before = cluster.leader_contact_epoch();
    let request = cluster.build_append_entries_request(2).unwrap();
    cluster.receive_append_entries(request).unwrap();
    assert!(
        cluster.leader_contact_epoch() > before,
        "accepting AppendEntries must record leader contact"
    );
}

/// With shadow election off, `tick_election` may age the election clock but must never
/// promote: in a real deployment this process only sees its own copy of peer state, and
/// promoting a remote node here would just make it disagree with the rest of the group.
#[test]
fn shadow_election_off_never_promotes_locally() {
    let guarded = RaftCluster::new_single_shard(98, [1, 2, 3]);
    guarded.set_local_shadow_election(false);
    guarded.set_alive(1, false).unwrap();
    for _ in 0..32 {
        let _ = guarded.tick_election();
    }
    assert_eq!(
        guarded.leader_id(),
        1,
        "a production runtime must decide leadership over the wire, not locally"
    );

    // The in-process model keeps its old behavior.
    let shadow = RaftCluster::new_single_shard(99, [1, 2, 3]);
    shadow.set_alive(1, false).unwrap();
    for _ in 0..32 {
        let _ = shadow.tick_election();
    }
    assert_ne!(shadow.leader_id(), 1);
}

/// A leader promoted by a networked election has never replicated to its peers, so this
/// process knows nothing about how far they got. It must still be able to send the probe
/// that discovers it. When the pipeline instead assumed the whole log was in flight, that
/// probe was rejected with backpressure, the new leader could never commit in its own
/// term, and the surviving replicas duelled for leadership indefinitely.
#[test]
fn promoted_leader_can_reach_peers_it_knows_nothing_about() {
    let cluster = RaftCluster::new_single_shard(100, [1, 2, 3]);
    for i in 0..2000 {
        cluster
            .propose(Command::StringSet {
                key: format!("backpressure-{i}"),
                value: vec![9u8; 16],
            })
            .unwrap();
    }
    // A process that was never leader has no confirmed progress for its peers: it only
    // ever learns that from AppendEntries responses it sent itself.
    {
        let mut inner = cluster.inner.write().unwrap();
        for node in inner.nodes.values_mut() {
            if node.id != 2 {
                node.commit_index = 0;
                node.applied_index = 0;
                node.pipeline_state.match_index = 0;
            }
        }
    }

    let template = cluster.prepare_campaign(2).unwrap();
    assert!(cluster.conclude_campaign(2, template.term, 2).unwrap());
    assert_eq!(cluster.leader_id(), 2);

    // Anything that refreshes the pipeline (an incoming AppendEntries, a propose) runs
    // before the new leader gets its first response back, and must not conclude that the
    // whole log is in flight merely because the peer's progress is unknown.
    {
        let mut inner = cluster.inner.write().unwrap();
        let config = inner.config.clone();
        refresh_all_pipeline_states(&mut inner.nodes, 2, None, &config);
    }

    for target in [1u64, 3u64] {
        let request = cluster
            .build_append_entries_request(target)
            .unwrap_or_else(|err| panic!("new leader cannot even probe node {target}: {err}"));
        assert_eq!(request.leader_id, 2);
        assert_eq!(request.target_id, target);
    }
}


/// Concurrent appenders to one node log must SHARE durability barriers, not each take their own.
///
/// An fsync makes every byte already written to the file durable, so a barrier taken while other
/// writers are queued behind it covers them too. This was measured taking one barrier per write
/// no matter how many writers there were -- the fsync was held inside the cursor lock, so writers
/// could not even reach it at the same time, and sixteen writers paid sixteen barriers where one
/// would have done.
#[test]
fn concurrent_appends_to_one_node_log_share_barriers() {
    let dir = tempfile::tempdir().unwrap();
    // Any well-formed record will do -- this test is about how many barriers the appends cost,
    // not what they contain.
    let source = RaftCluster::new_single_shard_with_wal(
        dir.path().join("source"),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    source
        .propose(Command::StringSet { key: "seed".into(), value: b"seed".to_vec() })
        .unwrap();
    let record = source
        .inner
        .read()
        .unwrap()
        .wal_record_for(1)
        .expect("record for node 1")
        .1;

    let wal = std::sync::Arc::new(LocalRaftWal::new(dir.path().join("target")));
    let writers = 16usize;
    let each = 8usize;
    // Release every thread at the same instant, so they genuinely overlap rather than trickling
    // through one at a time and each finding no barrier to ride.
    let start = std::sync::Arc::new(std::sync::Barrier::new(writers));

    crate::durability_metrics::reset();
    let handles: Vec<_> = (0..writers)
        .map(|_| {
            let wal = std::sync::Arc::clone(&wal);
            let record = record.clone();
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..each {
                    wal.persist_node_segmented(1, 1, &record, 1 << 20, 4).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let appends = (writers * each) as u64;
    let barriers = crate::durability_metrics::snapshot()
        .get("raft_wal_append")
        .copied()
        .unwrap_or(0);
    println!("{appends} concurrent appends cost {barriers} barriers");
    assert!(barriers >= 1, "some barrier must actually be taken");
    assert!(
        barriers < appends,
        "{barriers} barriers for {appends} concurrent appends -- nothing coalesced"
    );

    // Sharing a barrier must not cost anyone durability: every append is still on disk, and the
    // log still reads back cleanly.
    let recovered = wal.recover_node(1, 1).unwrap();
    assert!(
        recovered.valid_records > 0,
        "the node log must read back after concurrent appends"
    );
}


/// A propose that returned `Ok` must be on disk, however many proposers were running at once.
///
/// Making barriers coalesce moved the flush out from under the cluster locks, which put the
/// deferral bookkeeping -- shared owner/dirty flags -- in reach of two proposers at once. When
/// that happened, one proposer's "I owe a barrier" flag could be cleared by another opening its
/// own deferral, so the first staged nothing and returned success for a write that was never
/// written at all. Nothing about a single-threaded run shows that up.
#[test]
fn every_concurrent_propose_that_returned_ok_survives_a_restore() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = std::sync::Arc::new(
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap(),
    );
    cluster.set_local_node_id(1);

    let writers = 8usize;
    let each = 12usize;
    let start = std::sync::Arc::new(std::sync::Barrier::new(writers));
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let cluster = std::sync::Arc::clone(&cluster);
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let mut accepted = Vec::new();
                for index in 0..each {
                    let key = format!("durable-{writer:02}-{index:03}");
                    // Only keys whose propose actually returned Ok are claimed below. A refusal
                    // promises nothing and must not be held against the log.
                    if cluster
                        .propose(Command::StringSet {
                            key: key.clone(),
                            value: key.clone().into_bytes(),
                        })
                        .is_ok()
                    {
                        accepted.push(key);
                    }
                }
                accepted
            })
        })
        .collect();
    let accepted: Vec<String> = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(accepted.len(), writers * each, "every propose should have been accepted");

    // Rebuild from nothing but the WAL. Anything acknowledged but never written is missing here.
    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    let logged: std::collections::BTreeSet<String> = restored
        .inner
        .read()
        .unwrap()
        .nodes
        .get(&1)
        .expect("node 1 present after restore")
        .log
        .clone()
        .into_iter()
        .filter_map(|entry| match entry.command {
            Command::StringSet { key, .. } => Some(key),
            _ => None,
        })
        .collect();
    let missing: Vec<&String> = accepted.iter().filter(|key| !logged.contains(*key)).collect();
    assert!(
        missing.is_empty(),
        "{} acknowledged writes are absent from the restored log, first: {:?}",
        missing.len(),
        missing.first()
    );
}


/// Replaying an append-only node log must not rescan everything accumulated so far.
///
/// Folding one incremental record used to scan the WHOLE running log twice -- once for entries a
/// conflict superseded, once for entries compaction removed. Each record carries about one new
/// entry, so the log grows by one per record and those scans cost 1 + 2 + 3 + ... over the
/// replay: quadratic in record count, meaning a long-lived node pays for its entire history every
/// time it starts. Nothing about a short log shows that up, and a stopwatch on a shared machine
/// measures the other tenants, so this asserts the scan is not entered at all.
#[test]
fn replaying_an_append_only_log_never_rescans_it() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(1);
    let records = 400u64;
    for index in 0..records {
        cluster
            .propose(Command::StringSet {
                key: format!("append-{index:05}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }

    let wal = LocalRaftWal::new(dir.path());
    crate::durability_metrics::reset();
    let recovered = wal.recover_node(1, 1).unwrap();
    let counts = crate::durability_metrics::snapshot();

    // Every record here is a plain append: it supersedes nothing and compacts nothing, so the
    // trims have no work to do and must not walk the accumulated log to discover that. Asserting
    // on entries EXAMINED rather than on a branch counter is what makes this catch a regression:
    // an implementation that goes back to scanning unconditionally reports its scan here.
    let scanned = counts.get("replay_entries_scanned").copied().unwrap_or(0);
    println!("replay of {records} append-only records examined {scanned} accumulated entries");
    assert_eq!(
        scanned, 0,
        "replaying {records} plain appends examined {scanned} accumulated entries;          scanning the running log per record makes replay quadratic in record count"
    );

    // ...and the replay must still be right, not merely cheap.
    let record = recovered.record.expect("a record is recovered");
    assert_eq!(
        record.entries.len() as u64,
        records,
        "every proposed entry should survive replay"
    );
    for (position, entry) in record.entries.iter().enumerate() {
        match &entry.command {
            Command::StringSet { key, .. } => {
                assert_eq!(key, &format!("append-{position:05}"), "entries stay in log order")
            }
            other => panic!("unexpected command in the replayed log: {other:?}"),
        }
    }
}


/// Building an AppendEntries must cost what it SENDS, not what the log already holds.
///
/// The entries a peer still needs sit above its `prev_log_index`, and a raft log is in ascending
/// index order, so they are a suffix. Finding that suffix by iterating from index 0 and filtering
/// costs the whole log on every AppendEntries to every peer -- so appending one entry got steadily
/// more expensive as history accumulated, which is one of the ways write latency ends up growing
/// with log length rather than staying flat.
#[test]
fn building_an_append_entries_does_not_walk_the_whole_log() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(1);
    let entries = 500u64;
    for index in 0..entries {
        cluster
            .propose(Command::StringSet {
                key: format!("hist-{index:05}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }

    // Put the peer fully caught up, so the next request carries nothing at all: the ideal case,
    // and the one that used to cost a full scan of every entry ever appended.
    {
        let mut inner = cluster.inner.write().unwrap();
        let last = inner
            .nodes
            .get(&1)
            .map(|node| node.log.last().map(|entry| entry.index).unwrap_or(0))
            .unwrap_or(0);
        if let Some(peer) = inner.nodes.get_mut(&2) {
            peer.pipeline_state.next_index = last + 1;
            peer.pipeline_state.inflight_entries = 0;
            peer.pipeline_state.inflight_bytes = 0;
        }
    }

    crate::durability_metrics::reset();
    let request = cluster.build_append_entries_request(2).unwrap();
    let examined = crate::durability_metrics::snapshot()
        .get("replication_entries_examined")
        .copied()
        .unwrap_or(0);
    println!(
        "with {entries} entries in the log, a caught-up peer's request examined {examined} and carried {}",
        request.entries.len()
    );
    assert!(
        request.entries.is_empty(),
        "a caught-up peer should be sent nothing"
    );
    assert!(
        examined < entries,
        "building a request for a caught-up peer examined {examined} of {entries} entries; \
         the cost of a request must not grow with the history behind it"
    );
}


/// A follower that is behind and not converging must not report itself as caught up.
///
/// Staleness used to be derivable only by comparing log indices against this process's own shadow
/// of the peer, and that shadow does not move when the peer rejects -- so a follower that had
/// stopped converging reported a lag of ZERO while falling further behind. The failure mode
/// reported itself as health, which is why it went unnoticed. Time cannot be faked the same way:
/// the comparison is against the commit index the LEADER sent, and the clock advances whether or
/// not appends are landing.
#[test]
fn a_follower_that_stops_converging_cannot_report_zero_lag() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(2);

    let accepted = |index: u64| AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 2,
        prev_log_index: index - 1,
        prev_log_term: if index == 1 { 0 } else { 1 },
        entries: vec![RaftLogEntry {
            term: 1,
            index,
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k-{index}"),
                value: b"v".to_vec(),
            },
        }],
        leader_commit: index,
    };

    // Land two entries, so the follower is genuinely caught up with what the leader has committed.
    cluster.receive_append_entries(accepted(1)).unwrap();
    cluster.receive_append_entries(accepted(2)).unwrap();
    cluster.advance_time_ms(30_000);
    assert_eq!(
        cluster.replication_stall_ms(2),
        0,
        "a caught-up follower must not look stalled just because time passed"
    );

    // Now the leader moves on, but every append this follower is offered refers to a log position
    // it does not have, so it rejects them all -- while still hearing from the leader. This is
    // exactly the shape that used to report lag 0.
    let mismatched = |index: u64| AppendEntriesRequest {
        prev_log_index: index + 500,
        prev_log_term: 1,
        ..accepted(index)
    };
    for index in 3..8 {
        let response = cluster.receive_append_entries(mismatched(index)).unwrap();
        assert!(!response.success, "the mismatched append should be rejected");
        cluster.advance_time_ms(10_000);
    }

    let stalled = cluster.replication_stall_ms(2);
    assert!(
        stalled >= 50_000,
        "a follower behind the leader and accepting nothing reported a stall of {stalled} ms; \
         it has not made progress for the whole run"
    );

    // Once it accepts again, the stall clears -- it is a progress signal, not a one-way latch.
    cluster.receive_append_entries(accepted(3)).unwrap();
    assert_eq!(
        cluster.replication_stall_ms(2),
        0,
        "a follower that has caught up must report no stall"
    );
}


/// The AUTHENTICATED http entry point must accept a binary append body.
///
/// It extracts the rpc auth metadata before dispatching, and it did that by parsing the body as
/// JSON -- so with binary replication on, every append got a 403 from the auth wrapper and the
/// cluster could not replicate at all. The unauthenticated handler (which the other tests use)
/// was fine, which is exactly why this test targets the authenticated one.
#[test]
fn authenticated_route_accepts_a_binary_append() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    cluster.set_local_node_id(2);
    let request = AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![RaftLogEntry {
            term: 1,
            index: 1,
            shard_id: 1,
            command: Command::StringSet {
                key: "auth".into(),
                value: b"binary".to_vec(),
            },
        }],
        leader_commit: 0,
    };
    let body = wal_proto::encode_append_entries(&request).unwrap();
    let (code, response) = handle_authenticated_raft_http(
        &cluster,
        HttpRequest {
            method: "POST".to_string(),
            path: "/raft/append_entries".to_string(),
            body,
        },
        "",
    );
    assert_eq!(
        code, 200,
        "the authenticated route rejected a binary append: {}",
        String::from_utf8_lossy(&response)
    );
    let parsed: AppendEntriesResponse = serde_json::from_slice(&response).unwrap();
    assert!(parsed.success, "the append must actually be accepted");
}


/// Counts how many appends are in flight per peer at once, failing the moment two overlap.
///
/// Two appends in flight to one follower is the disease the per-follower senders exist to cure:
/// under real network latency the overlapping sends arrive out of order, the follower rejects,
/// and the retries snowball into election churn. Loopback never shows it -- so this transport
/// makes the overlap itself the assertion, latency or no latency.
#[derive(Clone)]
struct OneInFlightTransport {
    cluster: RaftCluster,
    in_flight: std::sync::Arc<
        std::sync::Mutex<std::collections::BTreeMap<RaftNodeId, u32>>,
    >,
    max_seen: std::sync::Arc<std::sync::atomic::AtomicU32>,
}

impl OneInFlightTransport {
    fn enter(&self, peer: RaftNodeId) {
        let mut map = self.in_flight.lock().unwrap();
        let slot = map.entry(peer).or_insert(0);
        *slot += 1;
        self.max_seen
            .fetch_max(*slot, std::sync::atomic::Ordering::SeqCst);
    }
    fn leave(&self, peer: RaftNodeId) {
        let mut map = self.in_flight.lock().unwrap();
        *map.entry(peer).or_insert(1) -= 1;
    }
}

impl RaftTransport for OneInFlightTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let peer = request.target_id;
        self.enter(peer);
        // Hold the request open a moment so a second concurrent sender WOULD overlap here.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let out = self.cluster.receive_append_entries(request);
        self.leave(peer);
        out
    }
    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.cluster.receive_vote_request(request)
    }
    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.cluster.receive_install_snapshot(request)
    }
    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.cluster.receive_install_snapshot_chunk(request)
    }
}

/// Eight concurrent proposers, and never two appends in flight to one follower.
#[test]
fn at_most_one_append_in_flight_per_follower() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    // This test IS the pipeline's invariant; it must not silently test the default.
    cluster.use_follower_pipeline_for_test();
    let transport = OneInFlightTransport {
        cluster: cluster.clone(),
        in_flight: Default::default(),
        max_seen: Default::default(),
    };
    let writers = 8usize;
    let each = 6usize;
    let start = std::sync::Arc::new(std::sync::Barrier::new(writers));
    let cluster = std::sync::Arc::new(cluster);
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let cluster = std::sync::Arc::clone(&cluster);
            let transport = transport.clone();
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let mut accepted = 0usize;
                for index in 0..each {
                    if cluster
                        .propose_distributed(
                            Command::StringSet {
                                key: format!("pipe-{writer:02}-{index:02}"),
                                value: format!("v{writer}-{index}").into_bytes(),
                            },
                            &transport,
                        )
                        .is_ok()
                    {
                        accepted += 1;
                    }
                }
                accepted
            })
        })
        .collect();
    let accepted: usize = handles.into_iter().map(|handle| handle.join().unwrap()).sum();

    let max_in_flight = transport
        .max_seen
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        max_in_flight <= 1,
        "{max_in_flight} appends were in flight to one follower at once"
    );
    assert!(
        max_in_flight == 1,
        "no append ever reached a follower; nothing was replicated"
    );
    // Both follower senders must have answered, or the invariant above held vacuously with the
    // fan-out doing the sending.
    assert!(
        cluster.pipeline_reached_within(60_000) >= 2,
        "the per-follower senders never carried an append"
    );
    assert_eq!(
        accepted,
        writers * each,
        "every propose should commit through the pipeline"
    );
}

/// Each of many concurrent proposers gets ITS OWN command's response back, not whichever
/// happened to apply last in the same commit batch.
#[test]
fn concurrent_proposers_get_their_own_responses() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    // This test IS the pipeline's invariant; it must not silently test the default.
    cluster.use_follower_pipeline_for_test();
    let transport = cluster.clone();
    // Seed distinct values, then read them back through propose_distributed concurrently: a
    // string_get's response carries the value, so a swapped response is immediately visible.
    for key in 0..6 {
        cluster
            .propose_distributed(
                Command::StringSet {
                    key: format!("own-{key}"),
                    value: format!("value-{key}").into_bytes(),
                },
                &transport,
            )
            .unwrap();
    }
    let cluster = std::sync::Arc::new(cluster);
    let start = std::sync::Arc::new(std::sync::Barrier::new(6));
    let handles: Vec<_> = (0..6)
        .map(|key| {
            let cluster = std::sync::Arc::clone(&cluster);
            let transport = (*cluster).clone();
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let response = cluster
                    .propose_distributed(
                        Command::StringGet {
                            key: format!("own-{key}"),
                        },
                        &transport,
                    )
                    .unwrap();
                (key, response)
            })
        })
        .collect();
    for handle in handles {
        let (key, response) = handle.join().unwrap();
        match response {
            CommandResponse::Bytes { value: Some(value) } => assert_eq!(
                value,
                format!("value-{key}").into_bytes(),
                "proposer {key} got another command's response"
            ),
            other => panic!("proposer {key} got {other:?}"),
        }
    }
}


/// After compacting into a state-image snapshot, a restart must serve every value.
///
/// The image path installed correctly on the live install route, but the RESTORE path used an
/// installer that only knew entry-carrying snapshots -- an image snapshot restored there
/// replayed nothing and produced an empty engine. That hole is why the image stayed dark; this
/// is the test that proves it closed, through the binary record encoding and the on-disk WAL.
#[test]
fn restart_after_state_image_compaction_serves_every_value() {
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig {
        // Compact as soon as anything is applied, so the test exercises the image path.
        max_applied_log_bytes: 1,
        ..RaftConfig::default()
    };
    let written: Vec<(String, Vec<u8>)> = (0..40)
        .map(|index| (format!("img-{index:03}"), format!("value-{index:03}").into_bytes()))
        .collect();
    {
        let cluster =
            RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], config.clone())
                .unwrap();
        for (key, value) in &written {
            cluster
                .propose(Command::StringSet {
                    key: key.clone(),
                    value: value.clone(),
                })
                .unwrap();
        }
        let report = cluster.maybe_trigger_snapshot().unwrap();
        assert!(report.triggered, "compaction must fire: {}", report.reason);
        // The log behind the snapshot is gone; the marker carries the image, not the history.
        let status = cluster.status();
        assert!(status.commit_index >= 40);
        // A few more writes AFTER compaction land in the retained tail.
        for index in 40..44 {
            cluster
                .propose(Command::StringSet {
                    key: format!("img-{index:03}"),
                    value: format!("value-{index:03}").into_bytes(),
                })
                .unwrap();
        }
    }
    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 1, [1, 2, 3], config).unwrap();
    for index in 0..44 {
        let key = format!("img-{index:03}");
        let response = restored
            .read_local(1, Command::StringGet { key: key.clone() })
            .unwrap();
        match response {
            CommandResponse::Bytes { value: Some(value) } => assert_eq!(
                value,
                format!("value-{index:03}").into_bytes(),
                "{key} came back wrong after an image restart"
            ),
            other => panic!("{key} unreadable after an image restart: {other:?}"),
        }
    }
}

/// Compaction must actually bound the bytes on disk: with it firing, the log directory stays a
/// fraction of what the uncompacted history costs -- the difference between a record that
/// carries state and one that re-encodes history.
#[test]
fn state_image_compaction_bounds_wal_bytes() {
    fn wal_bytes(root: &std::path::Path) -> u64 {
        fn walk(path: &std::path::Path, total: &mut u64) {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    let Ok(meta) = entry.metadata() else { continue };
                    if meta.is_dir() {
                        walk(&entry.path(), total);
                    } else {
                        *total += meta.len();
                    }
                }
            }
        }
        let mut total = 0;
        walk(root, &mut total);
        total
    }
    let run = |compact: bool| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard_with_wal(
            dir.path(),
            1,
            [1, 2, 3],
            RaftConfig {
                max_applied_log_bytes: 64 * 1024,
                ..RaftConfig::default()
            },
        )
        .unwrap();
        cluster.set_local_node_id(1);
        for index in 0..600 {
            cluster
                .propose(Command::StringSet {
                    // Overwrite a small key set: state stays bounded while history grows, which
                    // is the workload compaction exists for. With unique keys, state equals
                    // history and there is nothing for a snapshot to save.
                    key: format!("b-{:02}", index % 20),
                    value: vec![0x41u8; 512],
                })
                .unwrap();
            if compact && index % 100 == 99 {
                let _ = cluster.maybe_trigger_snapshot();
            }
        }
        wal_bytes(dir.path())
    };
    let unbounded = run(false);
    let bounded = run(true);
    println!("wal bytes: unbounded={unbounded} bounded={bounded}");
    assert!(
        bounded * 2 < unbounded,
        "compaction should bound the log: {bounded} vs {unbounded}"
    );
}


/// Compacting the log must not record progress a peer never made.
///
/// A deployed process owns one node but keeps a view of the whole cluster, so most entries in
/// `nodes` are SHADOWS of peers rather than nodes this process runs. Compaction walked every
/// live node and installed the new snapshot into it, which advances that node's commit and
/// applied indices and truncates its log -- so a follower that had received nothing was recorded
/// as holding a snapshot it was never sent. That is the same shape as a stranded follower
/// reporting no lag: the leader believes its own shadow instead of what the peer actually
/// acknowledged, and anything downstream that asks "how far behind is this peer" gets a
/// fabricated answer.
#[test]
fn compaction_does_not_credit_a_peer_with_a_snapshot_it_never_received() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            can_trigger_snapshot: true,
            // Low enough that a handful of writes crosses it.
            max_applied_log_bytes: 1,
            // This test is about what compaction records for a peer, not about when it runs, so
            // switch off the hold that would otherwise wait for these peers to catch up.
            max_retained_log_bytes: 0,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    // This process runs node 1 only; 2 and 3 are shadows of peers it has not heard from.
    cluster.set_local_node_id(1);

    for index in 0..8u64 {
        cluster
            .propose(Command::StringSet {
                key: format!("compact-{index:03}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }

    // Where the peers actually are, before compaction runs.
    let before: Vec<(u64, u64)> = {
        let inner = cluster.inner.read().unwrap();
        [2u64, 3]
            .iter()
            .map(|id| {
                let node = inner.nodes.get(id).expect("peer shadow");
                (node.commit_index, node.applied_index)
            })
            .collect()
    };

    let report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(report.triggered, "the byte threshold should have triggered a compaction");

    let after: Vec<(u64, u64)> = {
        let inner = cluster.inner.read().unwrap();
        [2u64, 3]
            .iter()
            .map(|id| {
                let node = inner.nodes.get(id).expect("peer shadow");
                (node.commit_index, node.applied_index)
            })
            .collect()
    };

    assert_eq!(
        before, after,
        "compaction moved a peer's recorded progress without the peer acknowledging anything: \
         before={before:?} after={after:?}. A peer learns of a snapshot by being sent one and \
         answering; until then its recorded position must not move."
    );
}


/// Compaction must wait for a follower that still needs the entries -- but not forever.
///
/// Discarding entries a peer has not got yet turns catching it up from "send it the entries" into
/// "install a snapshot", which is the expensive path and the one most likely to go wrong on a
/// node that is already behind. Waiting cannot be unconditional either: a peer that never comes
/// back would pin the log open indefinitely, so past a ceiling the log is compacted regardless.
#[test]
fn compaction_waits_for_a_follower_that_is_behind_but_not_past_the_ceiling() {
    let build = |ceiling: u64| {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard_with_wal(
            dir.path(),
            1,
            [1, 2, 3],
            RaftConfig {
                can_trigger_snapshot: true,
                max_applied_log_bytes: 1,
                max_retained_log_bytes: ceiling,
                ..RaftConfig::default()
            },
        )
        .unwrap();
        cluster.set_local_node_id(1);
        for index in 0..8u64 {
            cluster
                .propose(Command::StringSet {
                    key: format!("hold-{index:03}"),
                    value: b"v".to_vec(),
                })
                .unwrap();
        }
        (dir, cluster)
    };

    // A live follower that has acknowledged nothing: the entries it still needs must survive.
    let (_dir, cluster) = build(1 << 30);
    {
        let mut inner = cluster.inner.write().unwrap();
        for id in [2u64, 3] {
            if let Some(node) = inner.nodes.get_mut(&id) {
                node.alive = true;
                node.pipeline_state.match_index = 0;
            }
        }
    }
    let held = cluster.maybe_trigger_snapshot().unwrap();
    assert!(
        !held.triggered,
        "compaction discarded entries a live follower had not acknowledged (reason: {})",
        held.reason
    );
    assert_eq!(held.reason, "held_for_a_follower_still_catching_up");

    // Once the followers have it, there is nothing left to wait for.
    {
        let mut inner = cluster.inner.write().unwrap();
        let applied = inner.nodes.get(&1).map(|node| node.applied_index).unwrap_or(0);
        for id in [2u64, 3] {
            if let Some(node) = inner.nodes.get_mut(&id) {
                node.pipeline_state.match_index = applied;
            }
        }
    }
    assert!(
        cluster.maybe_trigger_snapshot().unwrap().triggered,
        "with every follower caught up there is nothing to wait for"
    );

    // A follower that stays behind must not hold the log open past the ceiling.
    let (_dir2, tiny) = build(1);
    {
        let mut inner = tiny.inner.write().unwrap();
        for id in [2u64, 3] {
            if let Some(node) = inner.nodes.get_mut(&id) {
                node.alive = true;
                node.pipeline_state.match_index = 0;
            }
        }
    }
    assert!(
        tiny.maybe_trigger_snapshot().unwrap().triggered,
        "past the ceiling a snapshot is the cheaper way to catch that peer up, and an absent \
         peer must not pin the log open"
    );
}


/// A crash mid-write leaves a partial record, and recovery must drop exactly that and keep the
/// rest -- for the encoding actually in use.
///
/// Records are written as a magic byte, a length, and a payload, so a crash can tear one in three
/// distinct places: inside the length itself, inside the payload the length promised, or after a
/// complete-looking frame whose bytes are damaged. The existing crash test appends a fragment of
/// the older text encoding, which exercises none of them. Each case below must truncate the torn
/// tail, leave the records before it intact, and recover the last good one.
#[test]
fn a_torn_binary_record_is_truncated_and_the_records_before_it_survive() {
    for (case, tail) in [
        // Torn inside the length: fewer bytes than the length field needs.
        ("short length", vec![0xA7u8, 0x01, 0x02]),
        // A length that promises far more payload than was written.
        ("length past the end", vec![0xA7u8, 0xFF, 0xFF, 0x00, 0x00, 0x01, 0x02, 0x03]),
        // A complete-looking frame whose payload is not a record.
        (
            "damaged payload",
            {
                let mut bytes = vec![0xA7u8];
                bytes.extend_from_slice(&8u32.to_le_bytes());
                bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF]);
                bytes
            },
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard_with_wal(
            dir.path(),
            7,
            [1, 2, 3],
            RaftConfig::default(),
        )
        .unwrap();
        cluster.set_local_node_id(1);
        for index in 0..3u64 {
            cluster
                .propose(Command::StringSet {
                    key: format!("torn-{index}"),
                    value: format!("v{index}").into_bytes(),
                })
                .unwrap();
        }
        let good = cluster
            .inner
            .read()
            .unwrap()
            .nodes
            .get(&1)
            .map(|node| node.commit_index)
            .unwrap();

        let wal = LocalRaftWal::new(dir.path());
        let report = wal.segment_report(7, 1).unwrap();
        let active = report.segments.last().expect("an active segment").clone();
        let intact_len = std::fs::metadata(&active.path).unwrap().len();
        {
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&active.path)
                .unwrap();
            file.write_all(&tail).unwrap();
            file.sync_data().unwrap();
        }

        let recovery = wal.recover_node(7, 1).unwrap();
        assert!(
            recovery.corrupt_tail,
            "{case}: a torn record must be recognised as a corrupt tail"
        );
        assert_eq!(
            std::fs::metadata(&active.path).unwrap().len(),
            intact_len,
            "{case}: recovery must truncate exactly the torn tail, no more and no less"
        );
        let record = recovery.record.expect("{case}: the records before the tear must survive");
        assert_eq!(
            record.hard_state.commit_index, good,
            "{case}: the last good record must come back unchanged"
        );
    }
}

/// The image build no longer holds the cluster lock while it reads the engine, so it races
/// applies; consistency comes from the applied-index re-check. Hammer overwrites while
/// snapshotting and prove every image is one clean cut: keys are written in a fixed order,
/// generation by generation, so a consistent image read back in that order can only step down
/// by at most one generation across the keys and never rise.
#[test]
fn off_lock_image_build_yields_consistent_images_under_writes() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = std::sync::Arc::new(
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap(),
    );
    let keys = 6u64;
    // Seed a full first generation before anything races: an image snapshot only exists once
    // something has applied, so the first create_snapshot must not beat the first write.
    for key in 0..keys {
        cluster
            .propose(Command::StringSet {
                key: format!("gen-{key}"),
                value: 1u64.to_string().into_bytes(),
            })
            .unwrap();
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let cluster = std::sync::Arc::clone(&cluster);
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut generation = 2u64;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                for key in 0..keys {
                    cluster
                        .propose(Command::StringSet {
                            key: format!("gen-{key}"),
                            value: generation.to_string().into_bytes(),
                        })
                        .unwrap();
                }
                generation += 1;
            }
        })
    };
    for _ in 0..5 {
        let snapshot = cluster.create_snapshot().unwrap();
        assert!(
            snapshot.state_image.is_some(),
            "the image path should be taken while the gate is on"
        );
        let watermark = snapshot.last_included_index;
        let restore_dir = tempfile::tempdir().unwrap();
        let restored = RaftCluster::new_single_shard_with_wal(
            restore_dir.path(),
            1,
            [7],
            RaftConfig::default(),
        )
        .unwrap();
        restored.install_snapshot(7, snapshot).unwrap();
        let mut generations = Vec::new();
        for key in 0..keys {
            let value = match restored
                .propose(Command::StringGet {
                    key: format!("gen-{key}"),
                })
                .unwrap()
            {
                CommandResponse::Bytes { value } => value,
                other => panic!("unexpected response {other:?}"),
            };
            generations.push(
                value
                    .map(|bytes| String::from_utf8(bytes).unwrap().parse::<u64>().unwrap())
                    .unwrap_or(0),
            );
        }
        for pair in generations.windows(2) {
            assert!(
                pair[0] >= pair[1],
                "image at index {watermark} is torn: generations ran {generations:?}"
            );
        }
        assert!(
            generations[0] - generations[keys as usize - 1] <= 1,
            "image at index {watermark} mixes distant generations: {generations:?}"
        );
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    writer.join().unwrap();
}

/// The =0 fallback -- one propose at a time through the same senders -- must stay correct too.
#[test]
fn serialized_pipeline_fallback_holds_the_invariant() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    cluster.use_follower_pipeline_for_test();
    cluster.propose_one_at_a_time_for_test();
    let transport = OneInFlightTransport {
        cluster: cluster.clone(),
        in_flight: Default::default(),
        max_seen: Default::default(),
    };
    let cluster = std::sync::Arc::new(cluster);
    let handles: Vec<_> = (0..4)
        .map(|writer| {
            let cluster = std::sync::Arc::clone(&cluster);
            let transport = transport.clone();
            std::thread::spawn(move || {
                let mut accepted = 0usize;
                for index in 0..4 {
                    if cluster
                        .propose_distributed(
                            Command::StringSet {
                                key: format!("serial-{writer:02}-{index:02}"),
                                value: format!("v{writer}-{index}").into_bytes(),
                            },
                            &transport,
                        )
                        .is_ok()
                    {
                        accepted += 1;
                    }
                }
                accepted
            })
        })
        .collect();
    let accepted: usize = handles.into_iter().map(|handle| handle.join().unwrap()).sum();
    let max_in_flight = transport
        .max_seen
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        max_in_flight <= 1,
        "{max_in_flight} appends were in flight to one follower at once"
    );
    assert_eq!(accepted, 16, "every propose should commit through the serialized fallback");
}

/// An idle leader's lease decays between proposes; the timer's quorum-contact renewal is
/// what keeps the NEXT propose from bouncing off `leader_lease_valid`. Without the renewal
/// this test fails with NoMajority/LeaderUnavailable on the post-idle propose.
#[test]
fn idle_leader_lease_renews_on_quorum_contact() {
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig {
        lease_duration_ms: 50,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], config).unwrap();
    cluster.use_follower_pipeline_for_test();
    let transport = cluster.clone();
    cluster
        .propose_distributed(
            Command::StringSet {
                key: "warm".to_string(),
                value: b"v".to_vec(),
            },
            &transport,
        )
        .unwrap();
    // Idle past the lease: the propose gate would now reject on leader_lease_valid.
    cluster.advance_time_ms(200);
    // The timer loop's quorum-contact renewal (senders answered within the window).
    cluster.renew_leader_lease_after_quorum_contact();
    cluster
        .propose_distributed(
            Command::StringSet {
                key: "after-idle".to_string(),
                value: b"v2".to_vec(),
            },
            &transport,
        )
        .expect("a renewed lease must admit the post-idle propose");
}

/// A follower whose lag exceeds the in-flight window must still be probed forward. The old
/// refusal cut it off for good: no append, no acknowledgement, no drain -- and compaction then
/// held the log for a follower that could never catch up, while writes froze once a second
/// follower reached the same state.
#[test]
fn lagging_follower_beyond_the_window_still_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig {
        max_inflights_replicate: 2,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], config).unwrap();
    let transport = cluster.clone();
    cluster.set_alive(3, false).unwrap();
    for i in 0..10u8 {
        cluster
            .propose_distributed(
                Command::StringSet {
                    key: format!("lag-{i}"),
                    value: vec![i; 64],
                },
                &transport,
            )
            .unwrap();
    }
    let leader_commit = cluster.commit_index(1).unwrap();
    cluster.set_alive(3, true).unwrap();
    // Drive catch-up rounds the way the timer does. Every round must yield a request --
    // a refusal here is the deadlock this test exists to prevent.
    let mut caught_up = false;
    for _ in 0..64 {
        let request = cluster
            .build_append_entries_request(3)
            .expect("catch-up must never be refused, however charged the window reads");
        let response = cluster.receive_append_entries(request).unwrap();
        let _ = cluster.record_append_entries_response(3, &response);
        if cluster.commit_index(3).unwrap() >= leader_commit {
            caught_up = true;
            break;
        }
    }
    assert!(
        caught_up,
        "the probed follower must converge to the leader commit ({leader_commit})"
    );
}

/// The compaction threshold bounds the log ON DISK, so it must be judged by the disk footprint.
///
/// Judging it by the logical size of the commands understates the footprint by the whole
/// encoding overhead: on a 30,000-write corpus the commands were 5 MB while the segments held
/// 36 MB, so an 8 MB bound never fired and the log grew without limit. This test writes a
/// corpus whose ON-DISK size passes a threshold its COMMAND bytes do not, and requires the
/// trigger to fire.
#[test]
fn compaction_threshold_is_judged_on_the_logs_disk_footprint() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            // Never hold compaction for a lagging peer in this test: it is the byte accounting
            // under test, not the retention policy.
            max_retained_log_bytes: 0,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_local_node_id(1);
    let transport = cluster.clone();
    for i in 0..120u32 {
        cluster
            .propose_distributed(
                Command::StringSet {
                    key: format!("disk-{i:04}"),
                    value: vec![(i % 251) as u8; 96],
                },
                &transport,
            )
            .unwrap();
    }

    let logical: u64 = 120 * 96;
    let report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(
        report.applied_log_bytes > logical,
        "the trigger must judge the on-disk footprint ({}), not just the command bytes ({logical})",
        report.applied_log_bytes
    );

    // A threshold ABOVE the command bytes but BELOW the disk footprint must fire: that band is
    // exactly where an unbounded log used to live.
    let between = (logical + report.applied_log_bytes) / 2;
    cluster.set_max_applied_log_bytes_for_test(between);
    let fired = cluster.maybe_trigger_snapshot().unwrap();
    assert!(
        fired.triggered,
        "a threshold inside the encoding overhead must compact (reason: {}, bytes: {}, limit: {between})",
        fired.reason, fired.applied_log_bytes
    );
}

/// The state image is dominated by the served index, which is JSON -- on a scaled corpus 37%
/// of the image's bytes were digits, commas and structural characters, and it opened with
/// dozens of empty index families. The image FILE is therefore compressed. This pins both
/// halves: the file must be materially smaller than the state it carries, and a restore from
/// it must still serve every value.
#[test]
fn the_state_image_file_is_compressed_and_still_restores() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            max_applied_log_bytes: 1,
            max_retained_log_bytes: 0,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_local_node_id(1);
    let transport = cluster.clone();
    for i in 0..60u32 {
        cluster
            .propose_distributed(
                Command::StringSet {
                    key: format!("zstd-{i:04}"),
                    value: vec![(i % 7) as u8; 256],
                },
                &transport,
            )
            .unwrap();
    }
    let report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(report.triggered, "compaction should fire: {}", report.reason);

    // Find the image the compaction externalized.
    let mut image_path = None;
    for entry in walkdir_images(dir.path()) {
        image_path = Some(entry);
    }
    let image_path = image_path.expect("compaction must externalize an image file");
    let image_bytes = std::fs::metadata(&image_path).unwrap().len();
    let raw = std::fs::read(&image_path).unwrap();
    assert!(
        raw.windows(4).any(|window| window == b"TSZ1"),
        "the image file must carry the compression marker"
    );
    // The served index alone repeats every key name and family label; a compressed image is
    // far below the state it describes.
    assert!(
        image_bytes < 60 * 256,
        "a compressed image ({image_bytes} bytes) should be well under the raw value bytes"
    );

    // And it still restores: reattach through recovery and read every value back.
    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    for i in 0..60u32 {
        let response = restored
            .propose(Command::StringGet {
                key: format!("zstd-{i:04}"),
            })
            .unwrap();
        assert_eq!(
            response,
            CommandResponse::Bytes {
                value: Some(vec![(i % 7) as u8; 256])
            },
            "value {i} must survive a restore from the compressed image"
        );
    }
}

fn walkdir_images(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("image-") && name.ends_with(".bin"))
                .unwrap_or(false)
            {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// A DEPLOYED follower's in-memory log must be bounded as the corpus grows.
///
/// The log is trimmed only when a snapshot is installed, and the compaction check refuses to
/// run on anything but the leader. In-process that is harmless -- one process owns every node,
/// so the leader's compaction trims the peers' state too. A deployed follower is a separate
/// process that learns only by RPC, and a follower that stays CAUGHT UP never falls behind the
/// leader's retained range, so it is never sent a snapshot and nothing ever trims it. Its
/// residency then grows with the corpus instead of with the state.
#[test]
fn a_deployed_followers_in_memory_log_is_bounded_as_the_corpus_grows() {
    let dir = tempfile::tempdir().unwrap();
    let follower = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            max_applied_log_bytes: 4096,
            max_retained_log_bytes: 0,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    // This process owns node 2, and node 2 is a follower: the deployed shape.
    follower.set_local_node_id(2);

    let total = 400u64;
    for index in 1..=total {
        let entry = RaftLogEntry {
            term: 1,
            index,
            shard_id: 1,
            command: Command::StringSet {
                key: format!("mem-{index:04}"),
                value: vec![(index % 251) as u8; 128],
            },
        };
        let response = follower
            .receive_append_entries(AppendEntriesRequest {
                rpc: None,
                shard_id: 1,
                term: 1,
                leader_id: 1,
                target_id: 2,
                prev_log_index: index - 1,
                prev_log_term: if index == 1 { 0 } else { 1 },
                entries: vec![entry],
                // The leader commits as it goes, so everything here is applied.
                leader_commit: index,
            })
            .unwrap();
        assert!(response.success, "append {index} should be accepted");
        // The periodic check the timer loop runs on every node, follower included.
        if index % 50 == 0 {
            let _ = follower.maybe_trigger_snapshot();
        }
    }

    let held = follower.in_memory_log_entries(2);
    assert!(
        held < (total / 2) as usize,
        "a caught-up deployed follower held {held} of {total} entries in memory -- its log is \
         growing with the corpus, not with the state"
    );
}

/// The WAL status report must be identical whether it is served from the live cursor or
/// rebuilt from disk.
///
/// Rebuilding it reads and parses EVERY segment end to end, and it is reachable over a plain
/// HTTP GET (`/raft/control/matrixraft_runtime_admin`), so anything polling that endpoint made
/// the node re-read its whole log each time. The cursor already carries the same per-segment
/// info, so the live path serves it -- and this test fails if the two ever disagree.
#[test]
fn the_segment_report_matches_whether_it_is_cached_or_scanned() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(1);
    let transport = cluster.clone();
    for i in 0..30u32 {
        cluster
            .propose_distributed(
                Command::StringSet {
                    key: format!("seg-{i:03}"),
                    value: vec![(i % 251) as u8; 128],
                },
                &transport,
            )
            .unwrap();
    }

    let wal = LocalRaftWal::new(dir.path());
    // A freshly built handle has no cursor, so this is the disk-scanning path.
    let scanned = wal.segment_report(1, 1).unwrap();
    // Seeding a cursor makes the next call take the cached path.
    let _ = wal.recover_node_segmented(1, 1);
    let cached = wal.segment_report(1, 1).unwrap();

    assert_eq!(
        cached.segments, scanned.segments,
        "the cached report must describe exactly the segments the scan finds"
    );
    assert_eq!(cached.active_segment_id, scanned.active_segment_id);
    assert_eq!(cached.first_retained_log_index, scanned.first_retained_log_index);
    assert_eq!(cached.last_retained_log_index, scanned.last_retained_log_index);
    assert!(!cached.segments.is_empty(), "the node should have segments");
}

/// Compaction must be driven by how much LOG there is, not by how big the state is.
///
/// The threshold measures what the log occupies on disk so the bound means something. But the
/// externalized state image sits in that same directory, and it is not log -- compaction cannot
/// reclaim it, it REPLACES it. Counting it means that once a shard's state grows past the
/// threshold, the trigger is permanently satisfied: every check with any new entry rebuilds the
/// whole image to reclaim a few kilobytes of log. On a large shard that is a full state rebuild
/// every check, forever.
#[test]
fn compaction_does_not_rebuild_the_image_for_a_log_that_is_already_small() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            // Chosen so the STATE image exceeds it while the post-compaction log does not:
            // that is the exact band where counting the image made the trigger fire forever.
            // (Below one base record the threshold is degenerate -- a single base always
            // exceeds it -- so this is deliberately a realistic size, not the smallest one.)
            max_applied_log_bytes: 64 * 1024,
            max_retained_log_bytes: 0,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_local_node_id(1);
    let transport = cluster.clone();
    for i in 0..150u32 {
        cluster
            .propose_distributed(
                Command::StringSet {
                    key: format!("thrash-{i:04}"),
                    value: vec![(i % 251) as u8; 256],
                },
                &transport,
            )
            .unwrap();
    }

    // First compaction: legitimate, the log really has outgrown the bound.
    let first = cluster.maybe_trigger_snapshot().unwrap();
    assert!(first.triggered, "the log should compact once: {}", first.reason);

    // The image now exceeds the threshold on its own. One more small write must NOT be enough
    // to justify rebuilding the entire state again.
    cluster
        .propose_distributed(
            Command::StringSet {
                key: "one-more".to_string(),
                value: b"small".to_vec(),
            },
            &transport,
        )
        .unwrap();
    let second = cluster.maybe_trigger_snapshot().unwrap();
    assert!(
        !second.triggered,
        "a single small write must not rebuild the whole state image \
         (reason: {}, measured {} bytes against a {} byte bound)",
        second.reason, second.applied_log_bytes, second.max_applied_log_bytes
    );
}
