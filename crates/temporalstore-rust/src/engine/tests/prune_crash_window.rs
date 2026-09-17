// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a crash in the middle of a bucket-dump manifest prune leaves on disk.
//!
//! The prune does two things: it detaches the survivors whose parent it is about to remove, and
//! it unlinks the pruned manifests. Both orders finish in the same place, so the order looks
//! free. It is not -- it decides what a crash BETWEEN them leaves behind.
//!
//! Unlink first, and the window contains manifests that are gone with survivors still pointing
//! at them. `bucket_dump_manifest_chain_issues` reads that as `missing_parent_manifest` and
//! `storage_production_readiness_report` raises a `broken_slot_dump_manifest_chain` blocker --
//! the state the comment in `apply_bucket_dump_manifest_prune_with_retention_refs` itself calls
//! indistinguishable from real corruption. No data is lost either way (each manifest embeds a
//! complete index, and installing one never walks the chain), so the whole cost is a shard
//! reported as corrupt for a reason that never happened.
//!
//! Detach first and the window contains survivors with no parent link while their parent file is
//! still on disk -- which is exactly what a prune that ran to completion looks like, and
//! consistent either way.
//!
//! The two halves are asserted separately and against the same fixture, because they are two
//! different claims: that the old order's window really is the corrupt-looking state (it is not
//! a hypothetical), and that the new order's window is clean. Neither half is evidence for the
//! other. The crash point itself is not reachable from production code on demand, so the second
//! half drives `stop_bucket_dump_prune_after_first_phase_for_test`, the `cfg(test)` seam that
//! stops the prune between the phases.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;

/// Three manifests, each parented on the last: `first <- second <- third`.
///
/// The retention rule keeps the NEWEST manifest and nothing else, so this prunes two and leaves
/// one survivor whose parent is among the pruned. Three rather than two so that the pruned count
/// is not 1, which would let an off-by-one in either direction read as correct.
fn engine_with_three_chained_manifests(dir: &std::path::Path) -> (TemporalEngine, Vec<String>) {
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    let mut ids = Vec::new();
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "window".to_string(),
            value: b"v1".to_vec(),
        },
    });
    ids.push(
        engine
            .create_bucket_dump_manifest(SHARD, Vec::new())
            .expect("first manifest should persist")
            .manifest_id,
    );
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "window".to_string(),
            value: b"v2".to_vec(),
        },
    });
    ids.push(
        engine
            .create_bucket_dump_manifest(SHARD, Vec::new())
            .expect("second manifest should persist")
            .manifest_id,
    );
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "window".to_string(),
            value: b"v3".to_vec(),
        },
    });
    ids.push(
        engine
            .create_bucket_dump_manifest(SHARD, Vec::new())
            .expect("third manifest should persist")
            .manifest_id,
    );
    (engine, ids)
}

fn manifests_on_disk(engine: &TemporalEngine) -> Vec<BucketDumpManifest> {
    list_bucket_dump_manifests_at(&engine.index_dir, SHARD).expect("manifest dir should list")
}

/// Manifests still on disk that carry a parent link, and how many of those links point at a
/// manifest that is NOT on disk. The second number is the defect: it is what
/// `bucket_dump_manifest_chain_issues` turns into `missing_parent_manifest`.
fn parent_link_counts(engine: &TemporalEngine) -> (usize, usize) {
    let present = manifests_on_disk(engine);
    let ids = present
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let with_link = present
        .iter()
        .filter(|manifest| manifest.parent_manifest_id.is_some())
        .count();
    let dangling = present
        .iter()
        .filter(|manifest| {
            manifest
                .parent_manifest_id
                .as_ref()
                .is_some_and(|parent| !ids.contains(parent))
        })
        .count();
    (with_link, dangling)
}

/// HALF ONE: the order the prune used to run in, stopped where a crash would stop it.
///
/// Reproduces the old first phase directly -- unlink every manifest the plan names, detach
/// nothing -- because that state is the claim under test and building it by hand is the only way
/// to assert it once the production order no longer produces it. What a load sees is then
/// asserted twice over: the chain-issue reason, and the readiness blocker that reason raises.
#[test]
fn the_old_prune_order_leaves_invented_corruption_in_its_crash_window() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, created) = engine_with_three_chained_manifests(dir.path());
    assert_eq!(created.len(), 3, "fixture should create three manifests");

    let plan = engine.bucket_dump_manifest_prune_plan(SHARD);
    let pruned = plan.prunable_manifest_ids.len();
    let (with_link_before, dangling_before) = parent_link_counts(&engine);
    let on_disk_before = manifests_on_disk(&engine).len();
    println!(
        "OLD ORDER  manifests_on_disk={on_disk_before} pruned={pruned} \
         parent_links_before={with_link_before} dangling_before={dangling_before}"
    );
    // Vacuity floor. A prune that names nothing, or a fixture whose manifests carry no parent
    // links, would sail through every assertion below while testing nothing at all.
    assert!(
        pruned >= 2,
        "vacuous: the plan must name at least two manifests to prune, named {pruned}"
    );
    assert!(
        with_link_before >= 1,
        "vacuous: at least one manifest must carry a parent link, {with_link_before} do"
    );
    assert_eq!(
        dangling_before, 0,
        "fixture must start with a whole chain, {dangling_before} links already dangle"
    );

    // The old first phase, and only it: unlink, detach nothing. This is the process dying in the
    // window between the two loops.
    let mut unlinked = 0usize;
    for manifest_id in &plan.prunable_manifest_ids {
        if fs::remove_file(bucket_dump_manifest_path(&engine.index_dir, SHARD, manifest_id)).is_ok()
        {
            unlinked += 1;
        }
    }
    assert_eq!(
        unlinked, pruned,
        "the simulated first phase must unlink every manifest the plan named"
    );

    let (with_link_after, dangling_after) = parent_link_counts(&engine);
    println!(
        "OLD ORDER  after_crash: manifests_on_disk={} parent_links_after={with_link_after} \
         dangling_after={dangling_after}",
        manifests_on_disk(&engine).len()
    );
    assert_eq!(
        dangling_after, 1,
        "the survivor's parent link must now dangle -- that is the window"
    );

    let boundary = engine.storage_recovery_boundary_report(SHARD);
    assert_eq!(
        boundary.manifest_chain_issues.len(),
        1,
        "the dangling link must be reported, out of {} manifests still on disk",
        manifests_on_disk(&engine).len()
    );
    assert_eq!(
        boundary.manifest_chain_issues[0].reason, "missing_parent_manifest",
        "and reported as the reason that is indistinguishable from real corruption"
    );
    let readiness = engine.storage_production_readiness_report(SHARD);
    assert!(
        readiness
            .blockers
            .iter()
            .any(|blocker| blocker == "broken_slot_dump_manifest_chain"),
        "a crash in the old window must raise the recovery blocker; blockers were {:?}",
        readiness.blockers
    );
}

/// HALF TWO: the order the prune runs in now, stopped at the same crash point by the seam.
///
/// Asserts the seam actually stopped the prune (a seam that silently did nothing would let every
/// assertion below pass for the wrong reason), then that the window is clean by all three of the
/// measures half one caught it with, then that a fresh engine loads the shard and serves the
/// value -- which is the whole point of the ordering.
#[test]
fn the_new_prune_order_has_a_clean_crash_window_and_the_shard_still_loads() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, created) = engine_with_three_chained_manifests(dir.path());
    assert_eq!(created.len(), 3, "fixture should create three manifests");

    let plan = engine.bucket_dump_manifest_prune_plan(SHARD);
    let pruned = plan.prunable_manifest_ids.len();
    let (with_link_before, dangling_before) = parent_link_counts(&engine);
    let on_disk_before = manifests_on_disk(&engine).len();
    println!(
        "NEW ORDER  manifests_on_disk={on_disk_before} pruned={pruned} \
         parent_links_before={with_link_before} dangling_before={dangling_before}"
    );
    assert!(
        pruned >= 2,
        "vacuous: the plan must name at least two manifests to prune, named {pruned}"
    );
    assert!(
        with_link_before >= 1,
        "vacuous: at least one manifest must carry a parent link, {with_link_before} do"
    );
    assert_eq!(
        dangling_before, 0,
        "fixture must start with a whole chain, {dangling_before} links already dangle"
    );

    crate::engine::bucket_dump_manifest_methods::stop_bucket_dump_prune_after_first_phase_for_test(
        true,
    );
    let report = engine.apply_bucket_dump_manifest_prune(SHARD);
    crate::engine::bucket_dump_manifest_methods::stop_bucket_dump_prune_after_first_phase_for_test(
        false,
    );

    // The seam fired. Without this the test would also pass against a build where the seam was a
    // no-op and the prune simply ran to completion -- a different, and untested, situation.
    assert!(
        report.removed_manifest_ids.is_empty(),
        "the seam must stop the prune BEFORE it unlinks anything, but it removed {:?}",
        report.removed_manifest_ids
    );
    let on_disk_after = manifests_on_disk(&engine).len();
    assert_eq!(
        on_disk_after, on_disk_before,
        "and every manifest must therefore still be on disk"
    );

    let (with_link_after, dangling_after) = parent_link_counts(&engine);
    println!(
        "NEW ORDER  after_crash: manifests_on_disk={on_disk_after} \
         parent_links_after={with_link_after} dangling_after={dangling_after}"
    );
    assert_eq!(
        dangling_after, 0,
        "no parent link may dangle in the new window, {dangling_after} do out of \
         {with_link_before} links present before the prune"
    );
    assert_eq!(
        with_link_after,
        with_link_before - pruned.min(with_link_before),
        "every link into the pruned set must be detached: {with_link_before} links before, \
         {pruned} manifests bound for unlink, {with_link_after} links after"
    );

    let boundary = engine.storage_recovery_boundary_report(SHARD);
    assert!(
        boundary.manifest_chain_issues.is_empty(),
        "the new window must report no chain issue across {on_disk_after} manifests, got {:?}",
        boundary.manifest_chain_issues
    );
    let readiness = engine.storage_production_readiness_report(SHARD);
    assert!(
        !readiness
            .blockers
            .iter()
            .any(|blocker| blocker == "broken_slot_dump_manifest_chain"),
        "and must raise no chain blocker; blockers were {:?}",
        readiness.blockers
    );

    // The consequence the ordering exists for: a shard that comes up.
    drop(engine);
    let reopened = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    reopened.load_shard(SHARD);
    assert_eq!(
        reopened
            .storage_recovery_boundary_report(SHARD)
            .manifest_chain_issues
            .len(),
        0,
        "a reopened shard must see a whole chain too"
    );
    assert_eq!(
        reopened
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringGet {
                    key: "window".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v3".to_vec()),
        },
        "and must serve the value written before the prune"
    );
}
