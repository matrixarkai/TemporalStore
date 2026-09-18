// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Pre-vote must be on wherever a raft runtime takes `RaftConfig::default()`.
//!
//! Every env-driven entry point in this crate builds its raft config from
//! `RaftConfig::default()` and never names `enable_pre_vote`:
//!
//! * `bin/metaserver.rs` `runtime_options_from_env` -- `config: RaftConfig::default()`
//! * `bin/raft_node.rs` `runtime_options_from_env` -- `config: RaftConfig::default()`
//! * `bin/server.rs` `raft_config_from_env` -- overlays three knobs over `..defaults`
//!
//! so the default is what all three run, and the two public constructors
//! (`RaftCluster::new_single_shard`, `MetaRaftCluster::new`) hand the same default to
//! library consumers. Without pre-vote a node that was partitioned and rejoins bumps its
//! term and forces an election, unseating a leader that was serving fine.
//!
//! The halves are asserted separately: the default itself, and what a runtime built the
//! way production builds it actually ends up with.

use super::*;

/// What `enable_pre_vote` a meta raft runtime ends up with when it is built exactly the
/// way `bin/metaserver.rs` `runtime_options_from_env` builds it.
fn meta_runtime_pre_vote() -> bool {
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        snapshot_check_interval_ms: 30_000,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 1,
        nodes: vec![ProductionRaftNode {
            node_id: 1,
            addr: "127.0.0.1:17101".to_string(),
        }],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 10_000,
        stale_server_after_ms: 30_000,
        forbid_self_clearing_conviction: false,
    })
    .expect("meta raft runtime built from the default config should start");
    let cluster = runtime.cluster();
    let enforced = cluster
        .inner
        .read()
        .expect("meta raft cluster lock poisoned")
        .config
        .enable_pre_vote;
    enforced
}

/// What the public single-shard constructor leaves the cluster enforcing, read back out
/// of the cluster's own admin report rather than off the config that went in.
fn single_shard_pre_vote() -> bool {
    RaftCluster::new_single_shard(1, [1, 2, 3])
        .matrixraft_runtime_admin_report()
        .pre_vote_enforced
}

/// What the public metaserver-cluster constructor leaves the cluster enforcing.
fn meta_cluster_pre_vote() -> bool {
    MetaRaftCluster::new([1, 2, 3])
        .inner
        .read()
        .expect("meta raft cluster lock poisoned")
        .config
        .enable_pre_vote
}

/// Half one: the default itself.
#[test]
fn the_raft_config_default_enables_pre_vote() {
    assert!(
        RaftConfig::default().enable_pre_vote,
        "RaftConfig::default() must enable pre-vote: every env-driven production entry \
         point in this crate takes this default verbatim, and it is also what the public \
         constructors hand to library consumers"
    );
}

/// Half two: what a metaserver raft runtime actually ends up with, built the way
/// `runtime_options_from_env` builds it. Separate from half one on purpose -- the default
/// could be right while the runtime still dropped it on the floor.
#[test]
fn a_meta_raft_runtime_built_the_production_way_enforces_pre_vote() {
    assert!(
        meta_runtime_pre_vote(),
        "a meta raft runtime built with config: RaftConfig::default(), exactly as \
         bin/metaserver.rs runtime_options_from_env builds it, must end up enforcing \
         pre-vote"
    );
}

/// Half two, data side: the public single-shard constructor, read back through the
/// cluster's own admin report.
#[test]
fn a_single_shard_cluster_built_from_the_default_enforces_pre_vote() {
    assert!(
        single_shard_pre_vote(),
        "RaftCluster::new_single_shard takes RaftConfig::default(); the cluster it \
         returns must report pre-vote enforced"
    );
}

/// The number: how many production-shaped raft construction sites end up with pre-vote
/// disabled. Must be zero, out of a printed denominator.
#[test]
fn no_production_shaped_raft_construction_site_disables_pre_vote() {
    // Each entry mirrors, shape for shape, a construction site compiled into a binary or
    // is handed to a library consumer. The bool is what that shape's config ends up with.
    let defaults = RaftConfig::default();
    let examined: Vec<(&str, bool)> = vec![
        // bin/metaserver.rs runtime_options_from_env -- the env-driven meta raft.
        ("bin/metaserver.rs runtime_options_from_env", meta_runtime_pre_vote()),
        // bin/raft_node.rs runtime_options_from_env -- the standalone data raft node.
        ("bin/raft_node.rs runtime_options_from_env", RaftConfig::default().enable_pre_vote),
        // bin/server.rs raft_config_from_env -- three knobs overlaid on ..defaults, and
        // enable_pre_vote is not one of them, so it rides the default through.
        (
            "bin/server.rs raft_config_from_env",
            RaftConfig {
                max_applied_log_bytes: 8 * 1024 * 1024,
                replication_deadline_ms: defaults.replication_deadline_ms,
                max_inflights_replicate: defaults.max_inflights_replicate,
                ..defaults
            }
            .enable_pre_vote,
        ),
        // bin/metaserver_raft_harness.rs -- config: RaftConfig::default().
        ("bin/metaserver_raft_harness.rs", RaftConfig::default().enable_pre_vote),
        // Public library constructor, raft.rs RaftCluster::new_single_shard.
        ("RaftCluster::new_single_shard", single_shard_pre_vote()),
        // Public library constructor, raft/cluster_meta.rs MetaRaftCluster::new.
        ("MetaRaftCluster::new", meta_cluster_pre_vote()),
    ];

    let denominator = examined.len();
    let disabled: Vec<&str> = examined
        .iter()
        .filter(|(_, enabled)| !enabled)
        .map(|(name, _)| *name)
        .collect();

    println!(
        "pre-vote: examined {denominator} production-shaped raft construction sites; \
         {} of them build a runtime with pre-vote disabled",
        disabled.len()
    );

    assert!(
        denominator >= 6,
        "vacuity floor: this must examine at least 6 construction shapes, examined \
         {denominator}"
    );
    assert_eq!(
        disabled.len(),
        0,
        "{} of {denominator} production-shaped raft construction sites build a runtime \
         with pre-vote disabled: {disabled:?}",
        disabled.len()
    );
}
