// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::control::{Config, SetConfigRequest};
use crate::http::{json_response, parse_json, post_json_with_options, serve, HttpRequestOptions};
use crate::meta::{ServerMetaInfo, TableMetaInfo, TableShard};
use crate::rebalance::{
    PartitionSetTopology, DeterministicTaskScheduler, NetworkSchedulerTaskExecution,
    RebalanceStep, SchedulerTaskKind, SchedulerTaskResult, ShardReplica, ShardReplicaState,
    ShardRole, TaskSchedulerOptions,
};
use crate::types::{Command, FeatureFilter, FeatureFilterOp, FeaturePoint, SequenceFeatureRow};
use std::time::{Duration, Instant};

#[path = "tests/raft_test_support.rs"]
mod helpers;
#[allow(unused_imports)]
use helpers::*;
#[path = "tests/replication_codecs_and_snapshot_bootstrap.rs"]
mod part1;
#[path = "tests/network_transport_and_election_safety.rs"]
mod part2;
#[path = "tests/membership_and_replica_catchup.rs"]
mod part3;
#[path = "tests/metadata_consensus_and_durable_apply.rs"]
mod part4;
#[path = "tests/wal_compaction_and_restart.rs"]
mod part5;
#[path = "tests/durable_apply.rs"]
mod durable_apply;
#[path = "tests/pre_vote_default.rs"]
mod pre_vote_default;
#[path = "tests/snapshot_cadence.rs"]
mod snapshot_cadence;
#[path = "tests/snapshot_cost.rs"]
mod snapshot_cost;
#[path = "tests/snapshot_large_store.rs"]
mod snapshot_large_store;
#[path = "tests/snapshot_scale.rs"]
mod snapshot_scale;
#[path = "tests/snapshot_scope.rs"]
mod snapshot_scope;
