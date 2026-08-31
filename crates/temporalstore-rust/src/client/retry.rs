// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::thread;
use std::time::Duration;

use crate::types::Status;

use super::{ClientRetryDecision, ReplicaReadPolicy, TableOptions};

pub(super) fn retry_attempts_for(options: &TableOptions, write: bool) -> usize {
    let retries = if write {
        options.max_write_retries
    } else {
        options.max_read_retries
    };
    retries.saturating_add(1)
}

pub(super) fn replica_read_policy_from_meta(policy: &str) -> ReplicaReadPolicy {
    match policy {
        "first_replica" => ReplicaReadPolicy::FirstReplica,
        "round_robin_replica" => ReplicaReadPolicy::RoundRobinReplica,
        _ => ReplicaReadPolicy::PinPrimary,
    }
}

pub(super) fn sleep_before_retry(options: &TableOptions, attempt: usize) {
    if options.retry_backoff_ms == 0 {
        return;
    }
    let multiplier = u64::try_from(attempt.saturating_add(1)).unwrap_or(u64::MAX);
    let sleep_ms = options.retry_backoff_ms.saturating_mul(multiplier);
    thread::sleep(Duration::from_millis(sleep_ms));
}

pub(super) fn classify_retry_decision(
    status: &Status,
    write: bool,
    attempt: usize,
    retry_budget_attempts: usize,
    topology_refresh_used: bool,
) -> ClientRetryDecision {
    let retryable = status_is_retryable(status);
    let topology_retry = status_is_topology_retryable(status);
    let has_budget = attempt + 1 < retry_budget_attempts;
    let safe_budget_free_write_retry = write && topology_retry && !topology_refresh_used;
    let would_retry = retryable
        && (has_budget
            || (!write && topology_retry && !topology_refresh_used)
            || safe_budget_free_write_retry);
    ClientRetryDecision {
        retryable,
        topology_retry,
        safe_budget_free_write_retry,
        would_retry,
    }
}

pub(super) fn status_is_retryable(status: &Status) -> bool {
    if status.ok {
        return false;
    }
    let code = normalize_status_code(&status.code);
    matches!(
        code.as_str(),
        "deadlineexceeded"
            | "deadline_exceeded"
            | "unavailable"
            | "internal"
            | "retrylater"
            | "retry_later"
            | "partitionloading"
            | "partition_loading"
            | "metachanged"
            | "meta_changed"
            | "topomerror"
            | "topom_error"
            | "notserving"
            | "not_serving"
            | "staleloadversion"
            | "stale_load_version"
            // A request that lands on a node that no longer owns the shard (post-
            // migration) must re-resolve the route -- the not-owner topology error
            // forces a topology re-sync.
            | "shardnotloaded"
            | "shard_not_loaded"
            | "shardnotfound"
            | "shard_not_found"
            | "loadversionmismatch"
            | "load_version_mismatch"
            // A foreground write refused while its shard is loading, reloading or unloading.
            // Every one of those states ends on its own, and the data node refuses the write
            // BEFORE executing it, so asking again cannot duplicate anything. Not topology
            // retryable: the shard has not moved, so re-resolving the route names the same
            // node. Without this a rebalance, a restart or a dump/load cycle reached the
            // caller as failed writes, while the two conditions directly above -- which say
            // the same thing about the same shard -- were retried.
            | "lifecyclewriteblocked"
            | "lifecycle_write_blocked"
    )
}

pub(super) fn status_is_topology_retryable(status: &Status) -> bool {
    if status.ok {
        return false;
    }
    let code = normalize_status_code(&status.code);
    matches!(
        code.as_str(),
        "partitionloading"
            | "partition_loading"
            | "metachanged"
            | "meta_changed"
            | "topomerror"
            | "topom_error"
            | "notserving"
            | "not_serving"
            | "staleloadversion"
            | "stale_load_version"
            // Not-owner after a shard move -> re-resolve route (topology re-sync).
            | "shardnotloaded"
            | "shard_not_loaded"
            | "shardnotfound"
            | "shard_not_found"
            | "loadversionmismatch"
            | "load_version_mismatch"
    )
}

pub(super) fn normalize_status_code(code: &str) -> String {
    code.chars()
        .filter(|ch| *ch != '-' && *ch != ' ')
        .flat_map(char::to_lowercase)
        .collect()
}
