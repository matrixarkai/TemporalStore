// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use temporalstore_rust::http::{
    get_json_with_options, json_response, parse_json, post_json_with_options, serve,
    serve_with_stream_handler, HttpRequest, HttpRequestOptions, RequestHead, StreamAction,
    StreamTransfer,
};
use temporalstore_rust::meta::{
    AckResponse, AddNamespaceRequest, AddTableRequest, AutoRebalanceOptions, ConvictionPolicy,
    DeleteTableRequest, FailureDetectorOptions, FreezeReason, FreezeStaleServersRequest,
    GetShardResponse,
    FreezeAgingOptions, MetaRetentionOptions,
    GetTableTopologyRequest, LoadFinishRequest,
    MetaSnapshot, MetaSnapshotFileRequest, MetaSnapshotFileResponse, MetaSnapshotResponse,
    ProxyHeartbeatRequest, PublishShardSnapshotRequest, RegisterProxyRequest,
    RegisterServerRequest, RegisterShardRequest, SafeModePolicy, ServerHeartbeatRequest,
    ListShardsRequest, ReservedNames, ShardCheckOptions, ShardChecker, ShardReassignment, ShardReassignmentReason, ShardStateRequest, TopologyEventsRequest,
    DropProxyGroupRequest, NotifyStopRequest, ProxyCalibrationOptions, PutProxyGroupRequest, SingleNodeMeta, StateChangeRequest, TopologyVersionRequest, UpdateServerRequest,
    UpdateTableRequest,
};
use temporalstore_rust::raft::{
    ProductionMetaRaftRuntime, ProductionMetaRaftRuntimeOptions, ProductionRaftEngineKind,
    ProductionRaftNode, RaftClusterStatus, RaftConfig, RaftFailoverReport,
    RaftMembershipChangeReport, RaftNodeId,
};
use temporalstore_rust::rebalance::{
    DeterministicTaskScheduler, MembershipUpdateTaskPlan, RebalanceStep, SchedulerRunReport,
    SchedulerTask, SchedulerTaskKind, SchedulerTaskResult, TaskSchedulerOptions,
    TaskSchedulerSnapshot,
};
use temporalstore_rust::{
    types::Status, DataNodeLifecycleReport,
    DataNodeShardLifecycleState, LoadShardRequest, LoadShardResponse, SchedulerLifecycleToken,
    UnloadShardRequest, UnloadShardResponse,
};
use tracing::{debug, error, info, warn};

fn main() {
    temporalstore_rust::telemetry::init();
    let addr = std::env::var("TS_META_BIND_ADDR")
        .or_else(|_| std::env::var("TS_META_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let backend = MetaBackend::from_env().expect("failed to initialize metaserver backend");
    let scheduler =
        MetaTaskScheduler::from_env().expect("failed to initialize metaserver scheduler");
    let stale_after_ms = env_u64("TS_META_STALE_AFTER_MS", 30_000);
    let detector_interval_ms = env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000);
    // Adaptive failure detection. The default detector freezes any datanode whose
    // heartbeat is older than one fixed `stale_after_ms`, which misjudges nodes
    // whose heartbeat cadence differs from the tuning, convicts the entire fleet
    // if the metaserver itself stalls (every node crosses the threshold at once),
    // and freezes a whole rack when a rack-wide fault makes all of its nodes look
    // stale together -- amplifying a partial outage into a total one, because the
    // freeze is what auto-rebalance and raft failover then act on.
    //
    // With TS_META_ADAPTIVE_FAILURE_DETECTOR each datanode is judged against its
    // own learned heartbeat distribution (phi accrual), detection is suppressed
    // for a grace window after a detector stall, and a location losing too many
    // servers at once enters safe mode instead of being frozen wholesale. OFF by
    // default: which servers get frozen is behavior-visible, so the fixed
    // threshold stays the default until a deployment opts in. Both tiers are
    // covered: a correlated proxy failure takes out the routing path entirely,
    // so the proxies need the guard at least as much as the datanodes do.
    let adaptive_detector = env_bool("TS_META_ADAPTIVE_FAILURE_DETECTOR", false);
    let _failure_detector = match &backend {
        MetaBackend::Single(meta) if adaptive_detector => {
            let options = failure_detector_options_from_env();
            let policy = conviction_policy_from_env();
            info!(
                phi_failure_threshold = options.phi_failure_threshold,
                max_round_pause_ms = options.max_round_pause_ms,
                warning_ratio_percent = policy.warning_ratio_percent,
                critical_ratio_percent = policy.critical_ratio_percent,
                safe_mode_enabled = policy.safe_mode_enabled,
                convict_proxies = policy.convict_proxies,
                forbid_orphaning_shards = policy.forbid_orphaning_shards,
                "adaptive failure detection enabled"
            );
            Some(MetaBackground::Single(
                meta.start_adaptive_failure_detector_loop(
                    options,
                    policy,
                    safe_mode_policy_from_env(),
                    detector_interval_ms,
                ),
            ))
        }
        MetaBackend::Single(meta) => Some(MetaBackground::Single(
            meta.start_failure_detector_loop(stale_after_ms, detector_interval_ms),
        )),
        MetaBackend::Raft(runtime) => {
            if adaptive_detector {
                warn!(
                    "TS_META_ADAPTIVE_FAILURE_DETECTOR ignored: raft backend runs its own timer loop"
                );
            }
            Some(MetaBackground::Raft(runtime.start_timer_loop()))
        }
    };
    // Automatic shard rebalancing: on a membership change (a node freezes/leaves,
    // or a fresh node joins) recompute placement and drive the target nodes to
    // load their shards, then rewrite the owner map so clients stop resolving to a
    // dead node. OFF by default (TS_META_AUTO_REBALANCE) so single-node/standalone
    // behavior is unchanged. Only the single-node backend is auto-driven here; the
    // raft backend rebalances through its own membership machinery.
    let _auto_rebalancer = if env_bool("TS_META_AUTO_REBALANCE", false) {
        match &backend {
            MetaBackend::Single(meta) => {
                let interval_ms = env_u64("TS_META_AUTO_REBALANCE_INTERVAL_MS", detector_interval_ms);
                let balance_load = env_bool("TS_META_AUTO_REBALANCE_BALANCE", true);
                // Placement-aware planning honours each table's preferred
                // location and balances per table, so a table cannot end up
                // single-homed while total shard counts look even. OFF by
                // default: it changes which server a shard lands on, which is
                // behavior-visible.
                let placement_aware = env_bool("TS_META_PLACEMENT_AWARE_REBALANCE", false);
                info!(
                    interval_ms,
                    balance_load, placement_aware, "auto-rebalance enabled"
                );
                Some(start_auto_rebalance_loop(
                    meta.clone(),
                    interval_ms,
                    balance_load,
                    placement_aware,
                ))
            }
            MetaBackend::Raft(_) => {
                warn!(
                    "TS_META_AUTO_REBALANCE ignored: shard rebalancing is not available on the \
                     raft backend, and nothing else performs it there"
                );
                None
            }
        }
    } else {
        None
    };
    // Shard-divergence reconciliation: compare the owner map against what each
    // datanode reports serving, and re-place the shards its recorded owner is
    // not actually serving. OFF by default (TS_META_SHARD_DIVERGENCE_CHECK)
    // because it moves data. Only the single-node backend is auto-driven here.
    let _shard_divergence = if env_bool("TS_META_SHARD_DIVERGENCE_CHECK", false) {
        match &backend {
            MetaBackend::Single(meta) => {
                let interval_ms =
                    env_u64("TS_META_SHARD_DIVERGENCE_INTERVAL_MS", detector_interval_ms);
                let defaults = ShardCheckOptions::default();
                let options = ShardCheckOptions {
                    reboot_grace_ms: env_u64(
                        "TS_META_SHARD_DIVERGENCE_REBOOT_GRACE_MS",
                        defaults.reboot_grace_ms,
                    ),
                    settle_grace_ms: env_u64(
                        "TS_META_SHARD_DIVERGENCE_SETTLE_GRACE_MS",
                        defaults.settle_grace_ms,
                    ),
                    max_moves_per_window: env_u64(
                        "TS_META_SHARD_DIVERGENCE_MAX_MOVES",
                        defaults.max_moves_per_window as u64,
                    ) as usize,
                    window_ms: env_u64(
                        "TS_META_SHARD_DIVERGENCE_WINDOW_MS",
                        defaults.window_ms,
                    ),
                };
                info!(
                    interval_ms,
                    reboot_grace_ms = options.reboot_grace_ms,
                    settle_grace_ms = options.settle_grace_ms,
                    max_moves_per_window = options.max_moves_per_window,
                    "shard-divergence reconciliation enabled"
                );
                Some(start_shard_divergence_loop(meta.clone(), options, interval_ms))
            }
            MetaBackend::Raft(_) => {
                warn!(
                    "TS_META_SHARD_DIVERGENCE_CHECK ignored: divergence checking is not available \
                     on the raft backend, and nothing else performs it there"
                );
                None
            }
        }
    } else {
        None
    };
    // Freeze aging. A frozen resource stays frozen forever, so the retention GC
    // below - which only collects *dropped* resources - can never reach a node
    // the failure detector froze. This is the stage between them: a resource
    // that has been frozen longer than its cooldown is dropped, and retention
    // then forgets it. OFF by default (TS_META_FREEZE_AGING); tables are not
    // aged even then unless TS_META_TABLE_FREEZE_MS is set, because freezing a
    // table is an operator action they may still intend to undo.
    let _freeze_aging = if env_bool("TS_META_FREEZE_AGING", false) {
        match &backend {
            MetaBackend::Single(meta) => {
                let defaults = FreezeAgingOptions::default();
                let options = FreezeAgingOptions {
                    server_freeze_ms: env_u64(
                        "TS_META_SERVER_FREEZE_MS",
                        defaults.server_freeze_ms,
                    ),
                    proxy_freeze_ms: env_u64("TS_META_PROXY_FREEZE_MS", defaults.proxy_freeze_ms),
                    table_freeze_ms: env_u64("TS_META_TABLE_FREEZE_MS", defaults.table_freeze_ms),
                    max_drops_per_round: env_u64(
                        "TS_META_FREEZE_AGING_MAX_DROPS",
                        defaults.max_drops_per_round as u64,
                    ) as usize,
                };
                let interval_ms = env_u64("TS_META_FREEZE_AGING_INTERVAL_MS", 60_000);
                info!(
                    interval_ms,
                    server_freeze_ms = options.server_freeze_ms,
                    table_freeze_ms = options.table_freeze_ms,
                    "meta freeze aging enabled"
                );
                Some(meta.start_freeze_aging_loop(options, interval_ms))
            }
            MetaBackend::Raft(_) => {
                warn!(
                    "TS_META_FREEZE_AGING ignored: the raft backend owns its own meta state and \
                     does not age frozen resources, so they stay frozen until an operator acts"
                );
                None
            }
        }
    } else {
        None
    };
    // Retention GC. Dropping a server, proxy or table leaves its entry in the
    // meta state forever, so state, /servers, /tables and every exported
    // snapshot grow for the lifetime of the cluster. This ages the tombstones
    // out. OFF by default (TS_META_RETENTION_GC) because forgetting a resource
    // is not reversible.
    let _retention_gc = if env_bool("TS_META_RETENTION_GC", false) {
        match &backend {
            MetaBackend::Single(meta) => {
                let defaults = MetaRetentionOptions::default();
                let retention_ms = env_u64("TS_META_RETENTION_MS", defaults.server_retention_ms);
                let options = MetaRetentionOptions {
                    server_retention_ms: env_u64("TS_META_SERVER_RETENTION_MS", retention_ms),
                    proxy_retention_ms: env_u64("TS_META_PROXY_RETENTION_MS", retention_ms),
                    table_retention_ms: env_u64("TS_META_TABLE_RETENTION_MS", retention_ms),
                    max_purges_per_round: env_u64(
                        "TS_META_RETENTION_MAX_PURGES",
                        defaults.max_purges_per_round as u64,
                    ) as usize,
                };
                let interval_ms = env_u64("TS_META_RETENTION_INTERVAL_MS", 60_000);
                info!(
                    interval_ms,
                    server_retention_ms = options.server_retention_ms,
                    max_purges_per_round = options.max_purges_per_round,
                    "meta retention GC enabled"
                );
                Some(meta.start_meta_retention_loop(options, interval_ms))
            }
            MetaBackend::Raft(_) => {
                warn!(
                    "TS_META_RETENTION_GC ignored: the raft backend owns its own meta state and \
                     does not collect tombstones, so dropped resources accumulate"
                );
                None
            }
        }
    } else {
        None
    };
    // Proxy capacity calibration: keep every declared proxy group at its target
    // by attaching idle proxies and releasing surplus. OFF by default
    // (TS_META_PROXY_CALIBRATION) because it reassigns which namespace a proxy
    // serves. Only the single-node backend is auto-driven here.
    let _proxy_calibration = if env_bool("TS_META_PROXY_CALIBRATION", false) {
        match &backend {
            MetaBackend::Single(meta) => {
                let interval_ms =
                    env_u64("TS_META_PROXY_CALIBRATION_INTERVAL_MS", detector_interval_ms);
                let defaults = ProxyCalibrationOptions::default();
                let options = ProxyCalibrationOptions {
                    max_changes_per_round: env_u64(
                        "TS_META_PROXY_CALIBRATION_MAX_CHANGES",
                        defaults.max_changes_per_round as u64,
                    ) as usize,
                };
                info!(
                    interval_ms,
                    max_changes_per_round = options.max_changes_per_round,
                    "proxy calibration enabled"
                );
                Some(meta.start_proxy_calibration_loop(options, interval_ms))
            }
            MetaBackend::Raft(_) => {
                warn!(
                    "TS_META_PROXY_CALIBRATION ignored: the raft backend owns its own meta state \
                     and does not calibrate proxy groups, so they keep whatever size they have"
                );
                None
            }
        }
    } else {
        None
    };
    // Raft leader auto-failover: when the failure detector freezes a datanode, the
    // surviving replicas of any raft group it led are never told their leader is
    // gone, so raft's own `tick_election` never observes a dead leader and writes
    // to that group stall. This loop bridges detection->trigger: it POSTs the dead
    // node's liveness + a native failover request to each surviving replica, which
    // re-elects through raft's own majority + log-completeness guards (no
    // split-brain, no committed-write loss — the metaserver never picks a leader).
    // OFF by default (TS_RAFT_AUTO_FAILOVER) so standalone/single-node behavior is
    // byte-for-byte unchanged. Only the single-node backend is auto-driven here;
    // the raft backend fails over through its own timer loop.
    let _raft_failover = if env_bool("TS_RAFT_AUTO_FAILOVER", false) {
        match &backend {
            MetaBackend::Single(meta) => {
                let interval_ms =
                    env_u64("TS_RAFT_AUTO_FAILOVER_INTERVAL_MS", detector_interval_ms);
                info!(interval_ms, "raft auto-failover enabled");
                Some(start_raft_failover_loop(meta.clone(), interval_ms))
            }
            MetaBackend::Raft(_) => {
                warn!(
                    "TS_RAFT_AUTO_FAILOVER ignored: this setting drives failover for datanode                      raft shard groups, which the raft backend does not do -- a frozen datanode                      leaves its groups without a trigger and writes to them stall"
                );
                None
            }
        }
    } else {
        None
    };
    // Admin-surface authentication: read once at startup, like every other
    // security-relevant setting. Changing the variable on a running process
    // deliberately does nothing.
    let admin_token = temporalstore_rust::meta::admin_auth_token();
    if admin_token.is_some() {
        info!("metaserver admin token required (TS_META_ADMIN_TOKEN is set)");
    }
    info!(%addr, "temporalstore metaserver listening");
    if let Err(err) = serve_with_stream_handler(
        &addr,
        move |head: &RequestHead, transfer: &mut StreamTransfer| {
            admin_auth_gate(admin_token.as_deref(), head, transfer)
        },
        move |request| handle(&backend, &scheduler, request),
    ) {
        error!(%err, "metaserver serve loop exited");
        std::process::exit(1);
    }
}

/// Routes served without a credential even when an admin token is required:
/// the liveness/readiness probes and the metrics scrape. Load balancers and
/// Prometheus cannot reasonably attach a bearer token, and none of these
/// mutate or reveal tenant data.
fn admin_auth_exempt(path: &str) -> bool {
    matches!(
        path,
        "/health" | "/readiness" | "/metrics" | "/MasterService/Metrics"
    )
}

/// Whether a request may proceed, given the required admin token (None = the
/// surface is open, the previous behavior).
fn admin_request_allowed(required: Option<&str>, head: &RequestHead) -> bool {
    let Some(required) = required else { return true };
    if admin_auth_exempt(&head.path) {
        return true;
    }
    head.bearer_token.as_deref() == Some(required)
}

/// The head-stage gate in front of every route: a request that fails the token
/// check is answered 401 before its body is read into memory, and the
/// connection stays framed for keep-alive. Allowed requests fall through to
/// the buffered handler untouched.
fn admin_auth_gate(
    required: Option<&str>,
    head: &RequestHead,
    transfer: &mut StreamTransfer,
) -> StreamAction {
    if admin_request_allowed(required, head) {
        return StreamAction::Declined;
    }
    let body = serde_json::to_vec(&Status::error(
        "unauthorized",
        "missing or invalid admin token (TS_META_ADMIN_TOKEN)",
    ))
    .unwrap_or_default();
    let _ = transfer.drain_body();
    let _ = transfer.send_head(401, "application/json", body.len());
    let _ = transfer.write_chunk(&body);
    let _ = transfer.flush();
    StreamAction::Handled
}

/// Which node an operation is about.
#[derive(Debug, Clone, serde::Deserialize)]
struct MetaRaftNodeRequest {
    node_id: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MetaRaftMembershipResponse {
    status: Status,
    leader_id: u64,
    members: Vec<u64>,
    term: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MetaRaftScaleResponse {
    status: Status,
    /// Who the voters are once the change settled, and whether a majority of
    /// them is live. Absent when the change was refused.
    report: Option<temporalstore_rust::raft::RaftScaleChangeReport>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MetaRaftSnapshotTriggerResponse {
    status: Status,
    report: Option<temporalstore_rust::raft::RaftSnapshotTriggerReport>,
}

#[derive(Clone)]
enum MetaBackend {
    Single(SingleNodeMeta),
    Raft(ProductionMetaRaftRuntime),
}

enum MetaBackground {
    Single(std::thread::JoinHandle<()>),
    Raft(temporalstore_rust::raft::ProductionRaftTimerHandle),
}

#[derive(Clone, Default)]
struct MetaTaskScheduler {
    inner: Arc<Mutex<DeterministicTaskScheduler>>,
    executions: Arc<Mutex<Vec<MetaSchedulerExecutionRecord>>>,
    snapshot_path: Option<PathBuf>,
    /// How the scheduler paces itself when a caller does not say.
    ///
    /// Every other background subsystem here takes its pacing from the
    /// environment; this one alone had its retry backoff and its concurrency
    /// compiled in, so changing how hard the metaserver drives tasks meant
    /// changing the code.
    default_options: TaskSchedulerOptions,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerSubmitRequest {
    priority: i32,
    now_ms: u64,
    kind: SchedulerTaskKind,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerRunRequest {
    now_ms: u64,
    result: SchedulerTaskResult,
    #[serde(default)]
    options: Option<TaskSchedulerOptions>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerRestoreRequest {
    snapshot: TaskSchedulerSnapshot,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerDurableSnapshot {
    scheduler: TaskSchedulerSnapshot,
    #[serde(default)]
    executions: Vec<MetaSchedulerExecutionRecord>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerExecuteRequest {
    now_ms: u64,
    node_addr: String,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    load_request: Option<LoadShardRequest>,
    #[serde(default)]
    options: Option<TaskSchedulerOptions>,
    #[serde(default)]
    http: HttpRequestOptionsView,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerTaskResponse {
    status: Status,
    task: Option<SchedulerTask>,
    #[serde(default)]
    lifecycle_token: Option<temporalstore_rust::SchedulerLifecycleToken>,
    queue_len: usize,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerRunResponse {
    status: Status,
    report: Option<SchedulerRunReport>,
    queue_len: usize,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerSnapshotResponse {
    status: Status,
    snapshot: Option<TaskSchedulerSnapshot>,
    queue_len: usize,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerExecuteResponse {
    status: Status,
    task: Option<SchedulerTask>,
    #[serde(default)]
    lifecycle_token: Option<SchedulerLifecycleToken>,
    node_addr: String,
    dry_run: bool,
    calls: Vec<MetaSchedulerNodeCall>,
    scheduler_report: Option<SchedulerRunReport>,
    #[serde(default)]
    node_lifecycle: Option<DataNodeLifecycleReport>,
    #[serde(default)]
    lifecycle_state: Option<DataNodeShardLifecycleState>,
    #[serde(default)]
    raft_membership_report: Option<RaftMembershipChangeReport>,
    queue_len: usize,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerExecutionsResponse {
    status: Status,
    executions: Vec<MetaSchedulerExecutionRecord>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerExecutionRecord {
    task_id: u64,
    node_addr: String,
    status: Status,
    scheduler_result: SchedulerTaskResult,
    retry_times: u64,
    #[serde(default)]
    next_run_time_ms: Option<u64>,
    calls: Vec<MetaSchedulerNodeCall>,
    #[serde(default)]
    lifecycle_token: Option<SchedulerLifecycleToken>,
    #[serde(default)]
    lifecycle_state: Option<DataNodeShardLifecycleState>,
    #[serde(default)]
    raft_membership_report: Option<RaftMembershipChangeReport>,
    queue_len: usize,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerNodeCall {
    path: String,
    skipped: bool,
    status: Status,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct RaftMembershipApplyRequest {
    voters: Vec<RaftNodeId>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct RaftMembershipApplyResponse {
    status: Status,
    report: Option<RaftMembershipChangeReport>,
}

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct HttpRequestOptionsView {
    #[serde(default = "default_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_io_timeout_ms")]
    io_timeout_ms: u64,
    #[serde(default)]
    max_retries: usize,
}

impl Default for HttpRequestOptionsView {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_connect_timeout_ms(),
            io_timeout_ms: default_io_timeout_ms(),
            max_retries: 1,
        }
    }
}

impl From<HttpRequestOptionsView> for HttpRequestOptions {
    fn from(value: HttpRequestOptionsView) -> Self {
        Self {
            connect_timeout_ms: value.connect_timeout_ms,
            io_timeout_ms: value.io_timeout_ms,
            max_retries: value.max_retries,
        }
    }
}

include!("metaserver/task_scheduler.rs");

#[derive(Debug)]
struct SchedulerNodeExecution {
    status: Status,
    lifecycle_token: Option<SchedulerLifecycleToken>,
    calls: Vec<MetaSchedulerNodeCall>,
    raft_membership_report: Option<RaftMembershipChangeReport>,
}

fn execute_scheduler_task_on_node(
    task: &SchedulerTask,
    request: &MetaSchedulerExecuteRequest,
) -> SchedulerNodeExecution {
    let SchedulerTaskKind::RebalanceStep(step) = &task.kind else {
        if let SchedulerTaskKind::UpdateMembership(plan) = &task.kind {
            let voters = membership_voters_from_plan(plan);
            let (status, report) = post_raft_membership_or_error(
                &request.node_addr,
                voters,
                request.http.into(),
                request.dry_run,
            );
            return SchedulerNodeExecution {
                status: status.clone(),
                lifecycle_token: None,
                calls: vec![MetaSchedulerNodeCall {
                    path: "/raft/membership/apply".to_string(),
                    skipped: request.dry_run,
                    status,
                }],
                raft_membership_report: report,
            };
        }
        return SchedulerNodeExecution {
            status: Status::error(
                "unsupported_scheduler_task",
                "remote execution only supports rebalance and membership tasks",
            ),
            lifecycle_token: None,
            calls: Vec::new(),
            raft_membership_report: None,
        };
    };

    let token = task.lifecycle_token();
    let mut calls = Vec::new();
    if let Some(token) = &token {
        let status = post_status_or_error(
            &request.node_addr,
            "/ServerService/RequireLifecycleToken",
            token,
            request.http.into(),
            request.dry_run,
        );
        let ok = status.ok;
        calls.push(MetaSchedulerNodeCall {
            path: "/ServerService/RequireLifecycleToken".to_string(),
            skipped: request.dry_run,
            status,
        });
        if !ok {
            return SchedulerNodeExecution {
                status: calls.last().unwrap().status.clone(),
                lifecycle_token: token.clone().into(),
                calls,
                raft_membership_report: None,
            };
        }
    }

    let status = match step {
        RebalanceStep::LoadTarget {
            shard_id,
            node_id,
            load_version,
            ..
        } => {
            let Some(mut load) = request.load_request.clone() else {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "missing_load_request",
                        "LoadTarget execution requires load_request",
                    ),
                    lifecycle_token: token,
                    calls,
                    raft_membership_report: None,
                };
            };
            if load.shard_id != *shard_id || load.load_version != *load_version {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "load_request_mismatch",
                        "load_request shard_id/load_version does not match scheduler task",
                    ),
                    lifecycle_token: token,
                    calls,
                    raft_membership_report: None,
                };
            }
            load.local_node_id.get_or_insert(*node_id);
            let status = post_load_or_error(
                &request.node_addr,
                "/ServerService/Load",
                &load,
                request.http.into(),
                request.dry_run,
            );
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/Load".to_string(),
                skipped: request.dry_run,
                status: status.clone(),
            });
            status
        }
        RebalanceStep::ReloadTarget {
            shard_id,
            node_id,
            load_version,
            ..
        } => {
            let Some(mut load) = request.load_request.clone() else {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "missing_load_request",
                        "ReloadTarget execution requires load_request",
                    ),
                    lifecycle_token: token,
                    calls,
                    raft_membership_report: None,
                };
            };
            if load.shard_id != *shard_id || load.load_version != *load_version {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "load_request_mismatch",
                        "load_request shard_id/load_version does not match scheduler task",
                    ),
                    lifecycle_token: token,
                    calls,
                    raft_membership_report: None,
                };
            }
            load.local_node_id.get_or_insert(*node_id);
            let status = post_load_or_error(
                &request.node_addr,
                "/ServerService/Reload",
                &load,
                request.http.into(),
                request.dry_run,
            );
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/Reload".to_string(),
                skipped: request.dry_run,
                status: status.clone(),
            });
            status
        }
        RebalanceStep::UnloadSource { shard_id, .. } => {
            let unload = UnloadShardRequest {
                shard_id: *shard_id,
            };
            let status = post_unload_or_error(
                &request.node_addr,
                "/ServerService/Unload",
                &unload,
                request.http.into(),
                request.dry_run,
            );
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/Unload".to_string(),
                skipped: request.dry_run,
                status: status.clone(),
            });
            status
        }
        RebalanceStep::FreezeSource { .. } => Status::ok(),
        RebalanceStep::UpdateMembership { .. } => Status::error(
            "unsupported_rebalance_step",
            "membership rebalance steps use the membership executor path",
        ),
    };

    SchedulerNodeExecution {
        status,
        lifecycle_token: token,
        calls,
        raft_membership_report: None,
    }
}

fn membership_voters_from_plan(plan: &MembershipUpdateTaskPlan) -> Vec<RaftNodeId> {
    let mut voters: Vec<RaftNodeId> = plan
        .requests
        .first()
        .map(|peer| peer.request.replica_node_ids.clone())
        .unwrap_or_else(|| plan.active_replica_ids.clone())
        .into_iter()
        .collect();
    voters.sort_unstable();
    voters.dedup();
    voters
}

fn classify_scheduler_execution_result(status: &Status) -> SchedulerTaskResult {
    if status.ok {
        return SchedulerTaskResult::Ok;
    }
    if is_permanent_scheduler_execution_status(status.code.as_str()) {
        SchedulerTaskResult::Aborted
    } else {
        SchedulerTaskResult::RetryLater
    }
}

fn is_permanent_scheduler_execution_status(code: &str) -> bool {
    matches!(
        code,
        "unsupported_scheduler_task"
            | "unsupported_rebalance_step"
            | "missing_load_request"
            | "load_request_mismatch"
    )
}

fn post_status_or_error<T: serde::Serialize>(
    addr: &str,
    path: &str,
    request: &T,
    options: HttpRequestOptions,
    dry_run: bool,
) -> Status {
    if dry_run {
        return Status::ok();
    }
    post_json_with_options::<_, Status>(addr, path, request, options)
        .unwrap_or_else(|err| Status::error("node_request_failed", err.to_string()))
}

fn post_load_or_error(
    addr: &str,
    path: &str,
    request: &LoadShardRequest,
    options: HttpRequestOptions,
    dry_run: bool,
) -> Status {
    if dry_run {
        return Status::ok();
    }
    post_json_with_options::<_, LoadShardResponse>(addr, path, request, options)
        .map(|response| response.status)
        .unwrap_or_else(|err| Status::error("node_request_failed", err.to_string()))
}

fn post_unload_or_error(
    addr: &str,
    path: &str,
    request: &UnloadShardRequest,
    options: HttpRequestOptions,
    dry_run: bool,
) -> Status {
    if dry_run {
        return Status::ok();
    }
    post_json_with_options::<_, UnloadShardResponse>(addr, path, request, options)
        .map(|response| response.status)
        .unwrap_or_else(|err| Status::error("node_request_failed", err.to_string()))
}

fn post_raft_membership_or_error(
    addr: &str,
    voters: Vec<RaftNodeId>,
    options: HttpRequestOptions,
    dry_run: bool,
) -> (Status, Option<RaftMembershipChangeReport>) {
    if dry_run {
        return (Status::ok(), None);
    }
    post_json_with_options::<_, RaftMembershipApplyResponse>(
        addr,
        "/raft/membership/apply",
        &RaftMembershipApplyRequest { voters },
        options,
    )
    .map(|response| (response.status, response.report))
    .unwrap_or_else(|err| (Status::error("node_request_failed", err.to_string()), None))
}

fn fetch_node_lifecycle(
    addr: &str,
    options: HttpRequestOptions,
    token: Option<&SchedulerLifecycleToken>,
    calls: &mut Vec<MetaSchedulerNodeCall>,
) -> (
    Option<DataNodeLifecycleReport>,
    Option<DataNodeShardLifecycleState>,
) {
    match get_json_with_options::<DataNodeLifecycleReport>(
        addr,
        "/ServerService/GetLifecycle",
        options,
    ) {
        Ok(report) => {
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/GetLifecycle".to_string(),
                skipped: false,
                status: Status::ok(),
            });
            let state = token.and_then(|token| {
                report
                    .transitions
                    .iter()
                    .find(|state| {
                        state.shard_id == token.shard_id
                            && state.operation == token.operation
                            && state.scheduler_task_id == Some(token.task_id)
                            && state.scheduler_generation == Some(token.generation)
                    })
                    .cloned()
            });
            (Some(report), state)
        }
        Err(err) => {
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/GetLifecycle".to_string(),
                skipped: false,
                status: Status::error("node_lifecycle_fetch_failed", err.to_string()),
            });
            (None, None)
        }
    }
}

/// Background loop that drives automatic shard rebalancing for the single-node
/// backend. Each tick it recomputes placement ([`SingleNodeMeta::plan_auto_rebalance`]),
/// tells each target datanode to load its newly-assigned shard (the datanode
/// restores the shard's data from shared storage on load when that backend is
/// configured), unloads shards leaving a still-live source, and rewrites the
/// owner map so `/shards/<id>` no longer resolves to a departed node.
fn start_auto_rebalance_loop(
    meta: SingleNodeMeta,
    interval_ms: u64,
    balance_load: bool,
    placement_aware: bool,
) -> std::thread::JoinHandle<()> {
    let interval = std::time::Duration::from_millis(interval_ms.max(1));
    let http_options = HttpRequestOptions {
        connect_timeout_ms: env_u64("TS_META_AUTO_REBALANCE_CONNECT_TIMEOUT_MS", 500),
        io_timeout_ms: env_u64("TS_META_AUTO_REBALANCE_IO_TIMEOUT_MS", 2_000),
        max_retries: 1,
    };
    std::thread::spawn(move || loop {
        if meta.is_meta_change_muted() {
            std::thread::sleep(interval);
            continue;
        }
        run_auto_rebalance_round(&meta, balance_load, placement_aware, http_options);
        std::thread::sleep(interval);
    })
}

fn run_auto_rebalance_round(
    meta: &SingleNodeMeta,
    balance_load: bool,
    placement_aware: bool,
    http_options: HttpRequestOptions,
) {
    let options = AutoRebalanceOptions {
        balance_load,
        location_scoped: env_bool("TS_META_REBALANCE_LOCATION_SCOPED", true),
        per_table_balance: env_bool("TS_META_REBALANCE_PER_TABLE", true),
        balance_safe_gap: env_u64("TS_META_REBALANCE_SAFE_GAP", 0) as usize,
        ..AutoRebalanceOptions::default()
    };
    let plans = if placement_aware {
        meta.plan_placement_aware_rebalance(options)
    } else {
        meta.plan_auto_rebalance_with_options(options)
    };
    drive_reassignments(meta, plans, http_options);
}

/// Drive a set of reassignments: ask the target to load the shard, unload the
/// source when it still holds it, then rewrite the owner map.
fn drive_reassignments(
    meta: &SingleNodeMeta,
    plans: Vec<ShardReassignment>,
    http_options: HttpRequestOptions,
) {
    for plan in plans {
        let load_version = now_epoch_ms();
        let load_request = LoadShardRequest {
            shard_id: plan.shard_id,
            load_version,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
            readonly: false,
            table_name: String::new(),
        };
        // Ask the target to load (and, on a shared-storage node, restore) the
        // shard. `already_exists` means the target already serves it — also a
        // success for placement purposes.
        let load_status = post_load_or_error(
            &plan.to_server,
            "/load",
            &load_request,
            http_options,
            false,
        );
        if !load_status.ok && load_status.code != "already_exists" {
            error!(
                target = %plan.to_server,
                shard_id = plan.shard_id,
                message = %load_status.message,
                "auto-rebalance: target failed to load shard — retrying next round"
            );
            continue;
        }
        // A balance move or a location pull-back vacates a still-live source:
        // unload there (best-effort). An evacuation has no live source, and a
        // divergence's source is precisely the node that no longer holds the
        // shard, so neither has anything to unload.
        if matches!(
            plan.reason,
            ShardReassignmentReason::Rebalance | ShardReassignmentReason::LocationViolation
        ) {
            if let Some(from_server) = &plan.from_server {
                let unload_status = post_unload_or_error(
                    from_server,
                    "/unload",
                    &UnloadShardRequest {
                        shard_id: plan.shard_id,
                    },
                    http_options,
                    false,
                );
                if !unload_status.ok {
                    warn!(
                        source = %from_server,
                        shard_id = plan.shard_id,
                        message = %unload_status.message,
                        "auto-rebalance: source failed to unload shard"
                    );
                }
            }
        }
        let ack =
            meta.reassign_shard_with_reason(plan.shard_id, &plan.to_server, plan.reason.as_str());
        if ack.status.ok {
            info!(
                shard_id = plan.shard_id,
                to = %plan.to_server,
                reason = ?plan.reason,
                "auto-rebalance: shard reassigned"
            );
        }
    }
}

/// Background loop that reconciles the metaserver's shard->owner map against
/// what datanodes report serving.
///
/// A shard can vanish from a healthy node -- unloaded by hand, failed to reload
/// after a restart, a rolled-back load -- and nothing else notices: the node
/// keeps heartbeating, so neither the stale-heartbeat detector nor reboot
/// detection has anything to say, and auto-rebalance only evacuates shards whose
/// *owner* is unavailable. This owner is available; it is the shard that is
/// gone. Until something compares the two views, every read for that shard is
/// routed to a server that will miss on all of them.
fn start_shard_divergence_loop(
    meta: SingleNodeMeta,
    options: ShardCheckOptions,
    interval_ms: u64,
) -> std::thread::JoinHandle<()> {
    let interval = std::time::Duration::from_millis(interval_ms.max(1));
    let http_options = HttpRequestOptions {
        connect_timeout_ms: env_u64("TS_META_SHARD_DIVERGENCE_CONNECT_TIMEOUT_MS", 500),
        io_timeout_ms: env_u64("TS_META_SHARD_DIVERGENCE_IO_TIMEOUT_MS", 2_000),
        ..HttpRequestOptions::default()
    };
    let mut checker = ShardChecker::new(options);
    std::thread::spawn(move || loop {
        if meta.is_meta_change_muted() {
            std::thread::sleep(interval);
            continue;
        }
        let (report, moves) = meta.check_shard_divergence(&mut checker);
        if !report.diverged.is_empty() {
            warn!(
                diverged = report.diverged.len(),
                planned = moves.len(),
                rate_limited = report.rate_limited,
                settling = report.settling.len(),
                skipped_booting = report.skipped_in_reboot_grace.len(),
                "shard-divergence: owner map disagrees with what datanodes serve"
            );
        }
        drive_reassignments(&meta, moves, http_options);
        std::thread::sleep(interval);
    })
}

/// Wire body for `POST /raft/admin/liveness` on a datanode (mirrors the private
/// request struct in `bin/server.rs`; the response carries only a status).
#[derive(Debug, serde::Serialize)]
struct RaftAdminLivenessRequest {
    node_id: RaftNodeId,
    alive: bool,
}

#[derive(Debug, serde::Deserialize)]
struct RaftAdminLivenessResponse {
    status: Status,
}

/// Read [`FailureDetectorOptions`] from the environment, falling back to the
/// defaults documented on each field.
fn failure_detector_options_from_env() -> FailureDetectorOptions {
    let defaults = FailureDetectorOptions::default();
    FailureDetectorOptions {
        sample_capacity: env_u64(
            "TS_META_FD_SAMPLE_CAPACITY",
            defaults.sample_capacity as u64,
        )
        .max(1) as usize,
        initial_interval_ms: env_u64("TS_META_FD_INITIAL_INTERVAL_MS", defaults.initial_interval_ms),
        max_interval_ms: env_u64("TS_META_FD_MAX_INTERVAL_MS", defaults.max_interval_ms),
        phi_failure_threshold: std::env::var("TS_META_FD_PHI_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(defaults.phi_failure_threshold),
        max_round_pause_ms: env_u64("TS_META_FD_MAX_ROUND_PAUSE_MS", defaults.max_round_pause_ms),
    }
}

/// Read the correlated-failure gate policy from the environment.
fn conviction_policy_from_env() -> ConvictionPolicy {
    let defaults = ConvictionPolicy::default();
    ConvictionPolicy {
        warning_ratio_percent: env_u64(
            "TS_META_CONVICT_WARNING_RATIO_PERCENT",
            defaults.warning_ratio_percent,
        ),
        critical_ratio_percent: env_u64(
            "TS_META_CONVICT_CRITICAL_RATIO_PERCENT",
            defaults.critical_ratio_percent,
        ),
        min_abnormal_for_safe_mode: env_u64(
            "TS_META_CONVICT_MIN_ABNORMAL",
            defaults.min_abnormal_for_safe_mode as u64,
        ) as usize,
        safe_mode_enabled: env_bool("TS_META_CONVICT_SAFE_MODE", defaults.safe_mode_enabled),
        convict_enabled: env_bool("TS_META_CONVICT_ENABLED", defaults.convict_enabled),
        convict_on_reboot: env_bool("TS_META_CONVICT_ON_REBOOT", defaults.convict_on_reboot),
        convict_proxies: env_bool("TS_META_CONVICT_PROXIES", defaults.convict_proxies),
        forbid_orphaning_shards: env_bool(
            "TS_META_FORBID_ORPHANING_SHARDS",
            defaults.forbid_orphaning_shards,
        ),
    }
}

/// Freeze cooldowns applied to servers and proxies frozen by the detector.
fn safe_mode_policy_from_env() -> SafeModePolicy {
    SafeModePolicy {
        server_freeze_cooldown_ms: env_u64("TS_META_SERVER_FREEZE_COOLDOWN_MS", 0),
        proxy_freeze_cooldown_ms: env_u64("TS_META_PROXY_FREEZE_COOLDOWN_MS", 0),
    }
}

/// Wire body for `POST /raft/admin/failover` (the datanode handler ignores the
/// body); the response echoes the native failover report when one is produced.
#[derive(Debug, serde::Serialize)]
struct RaftAdminFailoverRequest {}

#[derive(Debug, serde::Deserialize)]
struct RaftAdminFailoverResponse {
    status: Status,
    #[serde(default)]
    report: Option<RaftFailoverReport>,
}

/// Tell a surviving replica that `node_id` is (not) alive so raft's own election
/// path stops counting it toward quorum. Marking a stale node not-alive is
/// monotonic and safe — the metaserver only does so after its heartbeat detector
/// declared the node stale.
fn post_raft_liveness_or_error(
    addr: &str,
    node_id: RaftNodeId,
    alive: bool,
    options: HttpRequestOptions,
) -> Status {
    post_json_with_options::<_, RaftAdminLivenessResponse>(
        addr,
        "/raft/admin/liveness",
        &RaftAdminLivenessRequest { node_id, alive },
        options,
    )
    .map(|response| response.status)
    .unwrap_or_else(|err| Status::error("node_request_failed", err.to_string()))
}

/// Ask a surviving replica to run its native `failover_primary`. Election safety
/// (live-majority requirement, candidate log-completeness) is enforced by the
/// datanode's raft `elect_leader`, not here. Returns the reported new leader id
/// (0 when the request failed or no report was produced).
fn post_raft_failover_or_error(
    addr: &str,
    options: HttpRequestOptions,
) -> (Status, RaftNodeId) {
    match post_json_with_options::<_, RaftAdminFailoverResponse>(
        addr,
        "/raft/admin/failover",
        &RaftAdminFailoverRequest {},
        options,
    ) {
        Ok(response) => {
            let new_leader = response
                .report
                .map(|report| report.new_leader_id)
                .unwrap_or_default();
            (response.status, new_leader)
        }
        Err(err) => (Status::error("node_request_failed", err.to_string()), 0),
    }
}

/// Background loop that drives raft leader failover for the single-node backend.
/// Each tick it reads membership ([`SingleNodeMeta::plan_raft_failover`]) and, for
/// every frozen datanode, notifies each surviving replica that the node is gone
/// and asks it to re-elect through raft's own safety-guarded path. Idempotent:
/// once a healthy leader exists the datanode's `failover_primary` is a no-op, so
/// each freeze episode is driven until an election is observed and then left
/// alone (tracked in `driven`).
fn start_raft_failover_loop(
    meta: SingleNodeMeta,
    interval_ms: u64,
) -> std::thread::JoinHandle<()> {
    let interval = std::time::Duration::from_millis(interval_ms.max(1));
    let http_options = HttpRequestOptions {
        connect_timeout_ms: env_u64("TS_RAFT_AUTO_FAILOVER_CONNECT_TIMEOUT_MS", 500),
        io_timeout_ms: env_u64("TS_RAFT_AUTO_FAILOVER_IO_TIMEOUT_MS", 2_000),
        max_retries: 1,
    };
    std::thread::spawn(move || {
        let mut driven: std::collections::HashSet<u64> = std::collections::HashSet::new();
        loop {
            run_raft_failover_round(&meta, &mut driven, http_options);
            std::thread::sleep(interval);
        }
    })
}

fn run_raft_failover_round(
    meta: &SingleNodeMeta,
    driven: &mut std::collections::HashSet<u64>,
    http_options: HttpRequestOptions,
) {
    let triggers = meta.plan_raft_failover();
    // Forget nodes that are no longer frozen so a rejoin-then-refreeze re-drives.
    let still_frozen: std::collections::HashSet<u64> =
        triggers.iter().map(|trigger| trigger.dead_node_id).collect();
    driven.retain(|node_id| still_frozen.contains(node_id));

    for trigger in triggers {
        if driven.contains(&trigger.dead_node_id) {
            continue;
        }
        let mut elected = false;
        for target in &trigger.live_targets {
            // 1. Inform the surviving replica the old leader is gone. This feeds
            //    raft's own pre-vote-guarded `tick_election` and excludes the dead
            //    node from the quorum count.
            let liveness_status = post_raft_liveness_or_error(
                target,
                trigger.dead_node_id,
                false,
                http_options,
            );
            if !liveness_status.ok {
                // The target isn't a peer of the dead node (NodeNotFound) or local
                // admin is disabled — nothing to do on this replica.
                debug!(
                    target = %target,
                    dead_node_id = trigger.dead_node_id,
                    message = %liveness_status.message,
                    "raft-failover: liveness update skipped"
                );
                continue;
            }
            // 2. Trigger the native failover. `elect_leader` refuses without a live
            //    majority (no split-brain) and rejects a lagging candidate (no
            //    committed-write loss). A new/current leader id that is neither 0
            //    nor the dead node means leadership is healthy on this group.
            let (failover_status, new_leader) = post_raft_failover_or_error(target, http_options);
            if failover_status.ok && new_leader != 0 && new_leader != trigger.dead_node_id {
                elected = true;
                info!(
                    target = %target,
                    dead_node_id = trigger.dead_node_id,
                    new_leader_id = new_leader,
                    "raft-failover: surviving replica re-elected leader"
                );
            } else if !failover_status.ok {
                warn!(
                    target = %target,
                    dead_node_id = trigger.dead_node_id,
                    message = %failover_status.message,
                    "raft-failover: failover trigger rejected — retrying next round"
                );
            }
        }
        if elected {
            // Stop re-driving this freeze episode; further rounds would be no-ops.
            driven.insert(trigger.dead_node_id);
        }
    }
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn default_connect_timeout_ms() -> u64 {
    200
}

fn default_io_timeout_ms() -> u64 {
    500
}

fn decode_meta_scheduler_file(
    bytes: &[u8],
) -> io::Result<(
    DeterministicTaskScheduler,
    Vec<MetaSchedulerExecutionRecord>,
)> {
    if let Ok(snapshot) = serde_json::from_slice::<MetaSchedulerDurableSnapshot>(bytes) {
        return Ok((
            DeterministicTaskScheduler::restore_snapshot(snapshot.scheduler)
                .map_err(io::Error::other)?,
            snapshot.executions,
        ));
    }
    Ok((
        DeterministicTaskScheduler::decode_snapshot(bytes).map_err(io::Error::other)?,
        Vec::new(),
    ))
}

fn save_scheduler_snapshot(
    path: &PathBuf,
    snapshot: &TaskSchedulerSnapshot,
    executions: &[MetaSchedulerExecutionRecord],
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        serde_json::to_writer_pretty(
            &mut file,
            &MetaSchedulerDurableSnapshot {
                scheduler: snapshot.clone(),
                executions: executions.to_vec(),
            },
        )
        .map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
}

include!("metaserver/backend.rs");

fn save_meta_snapshot_file(path: &str, snapshot: &MetaSnapshot) -> Result<(), String> {
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let tmp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|err| err.to_string())?;
        serde_json::to_writer_pretty(&mut file, snapshot).map_err(|err| err.to_string())?;
        file.write_all(b"\n").map_err(|err| err.to_string())?;
        file.sync_data().map_err(|err| err.to_string())?;
    }
    fs::rename(tmp_path, path).map_err(|err| err.to_string())
}

macro_rules! backend_call {
    ($backend:expr, $method:ident $(, $arg:expr)*) => {
        match $backend {
            MetaBackend::Single(meta) => meta.$method($($arg),*),
            MetaBackend::Raft(runtime) => runtime.cluster().$method($($arg),*),
        }
    };
}

fn handle(
    meta: &MetaBackend,
    scheduler: &MetaTaskScheduler,
    request: HttpRequest,
) -> (u16, Vec<u8>) {
    debug!(method = %request.method, path = %request.path, "serving request");
    if let Some(response) = handle_master_service_route(meta, &request) {
        return response;
    }
    if let Some(response) = handle_manage_service_route(meta, &request) {
        return response;
    }
    if let Some(response) = handle_query_service_route(meta, &request) {
        return response;
    }
    if let Some(response) = handle_heartbeat_service_route(meta, &request) {
        return response;
    }
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json_response(200, &Status::ok()),
        ("GET", "/metrics") | ("GET", "/MasterService/Metrics") => (
            200,
            metaserver_prometheus_metrics(meta, scheduler).into_bytes(),
        ),
        ("GET", "/readiness") => {
            let (code, body) = meta.readiness();
            json_response(code, &body)
        }
        ("GET", "/meta/info") => json_response(200, &backend_call!(meta, info)),
        ("GET", "/meta/stats") => json_response(200, &backend_call!(meta, stats)),
        ("GET", "/meta/preflight") => json_response(200, &backend_call!(meta, preflight_report)),
        ("GET", "/meta/topology_version") | ("GET", "/meta/topology") => json_response(
            200,
            &backend_call!(
                meta,
                topology_version_report,
                TopologyVersionRequest::default()
            ),
        ),
        ("POST", "/meta/topology_version") | ("POST", "/meta/topology") => {
            parse_or(&request.body, |req: TopologyVersionRequest| {
                backend_call!(meta, topology_version_report, req)
            })
        }
        ("GET", "/meta/raft/status") => match meta.raft_status() {
            Some(status) => json_response(200, &status),
            None => json_response(
                200,
                &Status::error("raft_disabled", "meta raft is disabled"),
            ),
        },
        ("GET", "/meta/raft/ready") => json_response(200, &meta.raft_ready()),
        ("GET", "/meta/raft/membership") => json_response(200, &meta.raft_membership()),
        ("POST", "/meta/raft/add_node") => parse_or(&request.body, |req: MetaRaftNodeRequest| {
            meta.raft_add_node(req.node_id)
        }),
        ("POST", "/meta/raft/remove_node") => {
            parse_or(&request.body, |req: MetaRaftNodeRequest| {
                meta.raft_remove_node(req.node_id)
            })
        }
        ("POST", "/meta/raft/transfer_leader") => {
            parse_or(&request.body, |req: MetaRaftNodeRequest| {
                meta.raft_transfer_leader(req.node_id)
            })
        }
        ("POST", "/meta/raft/snapshot") => json_response(200, &meta.raft_trigger_snapshot()),
        ("GET", "/meta/snapshot") => json_response(200, &meta.export_snapshot()),
        ("POST", "/meta/snapshot") | ("POST", "/meta/snapshot/restore") => {
            parse_or(&request.body, |snapshot: MetaSnapshot| {
                meta.install_snapshot(snapshot)
            })
        }
        ("POST", "/meta/snapshot/save") => {
            parse_or(&request.body, |req: MetaSnapshotFileRequest| {
                meta.save_snapshot(req)
            })
        }
        ("POST", "/meta/snapshot/load") => {
            parse_or(&request.body, |req: MetaSnapshotFileRequest| {
                meta.load_snapshot(req)
            })
        }
        ("GET", "/meta/scheduler") | ("GET", "/meta/scheduler/snapshot") => {
            json_response(200, &scheduler.snapshot())
        }
        ("POST", "/meta/topology_events") => {
            parse_or(&request.body, |req: TopologyEventsRequest| {
                backend_call!(meta, topology_events, req)
            })
        }
        ("GET", "/meta/scheduler/executions") => json_response(200, &scheduler.executions()),
        ("POST", "/meta/scheduler/submit") => {
            parse_or(&request.body, |req: MetaSchedulerSubmitRequest| {
                scheduler.submit(req)
            })
        }
        ("POST", "/meta/scheduler/run_next") => {
            parse_or(&request.body, |req: MetaSchedulerRunRequest| {
                scheduler.run_next(req)
            })
        }
        ("POST", "/meta/scheduler/execute_next") => {
            parse_or(&request.body, |req: MetaSchedulerExecuteRequest| {
                scheduler.execute_next(req)
            })
        }
        ("POST", "/meta/scheduler/restore") => {
            parse_or(&request.body, |req: MetaSchedulerRestoreRequest| {
                scheduler.restore(req)
            })
        }
        ("POST", "/register_shard") => parse_or(&request.body, |req: RegisterShardRequest| {
            backend_call!(meta, register, req)
        }),
        ("GET", path) if path.starts_with("/shards/") => {
            let shard_id = path
                .trim_start_matches("/shards/")
                .parse()
                .unwrap_or_default();
            json_response(200, &backend_call!(meta, get, shard_id))
        }
        ("POST", "/shards/freeze") => parse_or(&request.body, |req: ShardStateRequest| {
            json_response(200, &backend_call!(meta, freeze_shard, req))
        }),
        ("POST", "/shards/unfreeze") => parse_or(&request.body, |req: ShardStateRequest| {
            json_response(200, &backend_call!(meta, unfreeze_shard, req))
        }),
        ("POST", "/shards/drop") => parse_or(&request.body, |req: ShardStateRequest| {
            json_response(200, &backend_call!(meta, drop_shard, req))
        }),
        ("POST", "/shards/snapshot") | ("POST", "/publish_shard_snapshot") => {
            parse_or(&request.body, |req: PublishShardSnapshotRequest| {
                backend_call!(meta, publish_shard_snapshot, req)
            })
        }
        ("POST", "/servers/register") => parse_or(&request.body, |req: RegisterServerRequest| {
            backend_call!(meta, register_server, req)
        }),
        ("POST", "/servers/heartbeat") | ("POST", "/server_heartbeat") => {
            parse_or(&request.body, |req: ServerHeartbeatRequest| {
                backend_call!(meta, server_heartbeat, req)
            })
        }
        ("GET", "/servers") => json_response(200, &backend_call!(meta, list_servers)),
        ("POST", "/servers/freeze_stale") => {
            parse_or(&request.body, |req: FreezeStaleServersRequest| {
                backend_call!(
                    meta,
                    freeze_stale_resources_with_policy,
                    req.stale_after_ms,
                    SafeModePolicy {
                        server_freeze_cooldown_ms: req.server_freeze_cooldown_ms,
                        proxy_freeze_cooldown_ms: req.proxy_freeze_cooldown_ms,
                    }
                )
            })
        }
        ("GET", "/meta/safe_mode") | ("GET", "/safe_mode") => {
            json_response(200, &backend_call!(meta, safe_mode_report))
        }
        ("POST", "/shards/finish_load") | ("POST", "/finish_load") => {
            parse_or(&request.body, |req: LoadFinishRequest| {
                match scheduler.validate_finish_load(&req) {
                    Ok(()) => backend_call!(meta, finish_load, req),
                    Err(status) => AckResponse { status },
                }
            })
        }
        ("POST", "/servers/update") => parse_or(&request.body, |req: UpdateServerRequest| {
            json_response(200, &backend_call!(meta, update_server, req))
        }),
        ("POST", "/servers/notify_stop") => {
            parse_or(&request.body, |req: NotifyStopRequest| {
                json_response(200, &backend_call!(meta, notify_server_stop, req))
            })
        }
        ("POST", "/proxies/notify_stop") => {
            parse_or(&request.body, |req: NotifyStopRequest| {
                json_response(200, &backend_call!(meta, notify_proxy_stop, req))
            })
        }
        ("POST", "/meta/mute") => {
            json_response(200, &backend_call!(meta, set_meta_change_muted, true))
        }
        ("POST", "/meta/resume") => {
            json_response(200, &backend_call!(meta, set_meta_change_muted, false))
        }
        ("POST", "/servers/freeze") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, freeze_server, req)
        }),
        ("POST", "/servers/unfreeze") => parse_or(&request.body, |req: StateChangeRequest| {
            json_response(200, &backend_call!(meta, unfreeze_server, req))
        }),
        ("POST", "/servers/drop") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, drop_server, req)
        }),
        ("POST", "/proxies/register") => parse_or(&request.body, |req: RegisterProxyRequest| {
            backend_call!(meta, register_proxy, req)
        }),
        ("POST", "/proxies/heartbeat") | ("POST", "/proxy_heartbeat") => {
            parse_or(&request.body, |req: ProxyHeartbeatRequest| {
                backend_call!(meta, proxy_heartbeat, req)
            })
        }
        ("POST", "/proxy_groups") => parse_or(&request.body, |req: PutProxyGroupRequest| {
            json_response(200, &backend_call!(meta, put_proxy_group, req))
        }),
        ("POST", "/proxy_groups/delete") | ("DELETE", "/proxy_groups") => {
            parse_or(&request.body, |req: DropProxyGroupRequest| {
                json_response(200, &backend_call!(meta, drop_proxy_group, req))
            })
        }
        ("GET", "/proxy_groups") => json_response(200, &backend_call!(meta, list_proxy_groups)),
        ("GET", "/proxies") => json_response(200, &backend_call!(meta, list_proxies)),
        ("POST", "/proxies/freeze") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, freeze_proxy, req)
        }),
        ("POST", "/proxies/unfreeze") => parse_or(&request.body, |req: StateChangeRequest| {
            json_response(200, &backend_call!(meta, unfreeze_proxy, req))
        }),
        ("POST", "/proxies/drop") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, drop_proxy, req)
        }),
        ("POST", "/namespaces") => parse_or(&request.body, |req: AddNamespaceRequest| {
            backend_call!(meta, add_namespace, req)
        }),
        ("POST", "/namespaces/freeze") => parse_or(&request.body, |req: AddNamespaceRequest| {
            backend_call!(meta, freeze_namespace, req)
        }),
        ("POST", "/namespaces/unfreeze") => parse_or(&request.body, |req: AddNamespaceRequest| {
            backend_call!(meta, unfreeze_namespace, req)
        }),
        ("POST", "/namespaces/delete") | ("DELETE", "/namespaces") => {
            parse_or(&request.body, |req: AddNamespaceRequest| {
                backend_call!(meta, drop_namespace, req)
            })
        }
        ("GET", "/meta/reserved_names") => {
            json_response(200, &backend_call!(meta, reserved_names))
        }
        ("POST", "/meta/reserved_names") => parse_or(&request.body, |req: ReservedNames| {
            backend_call!(meta, set_reserved_names, req)
        }),
        ("GET", "/namespaces") => json_response(200, &backend_call!(meta, list_namespaces)),
        ("POST", "/tables") => parse_or(&request.body, |req: AddTableRequest| {
            backend_call!(meta, add_table, req)
        }),
        ("POST", "/tables/delete") | ("DELETE", "/tables") => {
            parse_or(&request.body, |req: DeleteTableRequest| {
                backend_call!(meta, delete_table, req)
            })
        }
        ("POST", "/tables/update") | ("PATCH", "/tables") => {
            parse_or(&request.body, |req: UpdateTableRequest| {
                backend_call!(meta, update_table, req)
            })
        }
        ("POST", "/tables/freeze") => parse_or(&request.body, |req: DeleteTableRequest| {
            backend_call!(meta, freeze_table, req)
        }),
        ("POST", "/tables/unfreeze") => parse_or(&request.body, |req: DeleteTableRequest| {
            backend_call!(meta, unfreeze_table, req)
        }),
        ("GET", "/shards") => json_response(
            200,
            &backend_call!(meta, list_shards, ListShardsRequest::default()),
        ),
        ("POST", "/shards/list") => parse_or(&request.body, |req: ListShardsRequest| {
            json_response(200, &backend_call!(meta, list_shards, req))
        }),
        ("GET", "/tables") => json_response(200, &backend_call!(meta, list_tables)),
        ("POST", "/tables/topology") | ("POST", "/table_topology") => {
            parse_or(&request.body, |req: GetTableTopologyRequest| {
                backend_call!(meta, get_table_topology, req)
            })
        }
        _ => json_response(
            404,
            &GetShardResponse {
                status: Status::error("not_found", "unknown metaserver route"),
                location: None,
            },
        ),
    }
}

fn metaserver_prometheus_metrics(meta: &MetaBackend, scheduler: &MetaTaskScheduler) -> String {
    let stats = backend_call!(meta, stats);
    let servers = backend_call!(meta, list_servers).servers;
    let proxies = backend_call!(meta, list_proxies).proxies;
    let namespaces = backend_call!(meta, list_namespaces).namespaces;
    let tables = backend_call!(meta, list_tables).tables;
    let proxy_groups = backend_call!(meta, list_proxy_groups).groups;
    let reserved = backend_call!(meta, reserved_names).reserved;
    let scheduler_snapshot = scheduler.snapshot();
    let scheduler_executions = scheduler.executions();
    let mut out = String::new();
    // What the background subsystems did, so conviction, divergence, retention
    // and freeze aging are observable without scraping logs.
    out.push_str(&meta.subsystem_prometheus());

    out.push_str("# HELP temporalstore_meta_requests_total Metaserver request counters by kind.\n");
    out.push_str("# TYPE temporalstore_meta_requests_total counter\n");
    for (kind, value) in [
        ("register_shard", stats.register_shard_total),
        ("get_shard", stats.get_shard_total),
        ("server_register", stats.server_register_total),
        ("server_heartbeat", stats.server_heartbeat_total),
        ("proxy_register", stats.proxy_register_total),
        ("proxy_heartbeat", stats.proxy_heartbeat_total),
        ("namespace_create", stats.namespace_create_total),
        ("table_create", stats.table_create_total),
        ("topology_query", stats.topology_query_total),
        ("load_finish", stats.load_finish_total),
    ] {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_requests_total",
            &[("kind", kind)],
            value,
        );
    }

    out.push_str("# HELP temporalstore_meta_inventory Current metaserver inventory counts.\n");
    out.push_str("# TYPE temporalstore_meta_inventory gauge\n");
    for (kind, value) in [
        ("namespace", namespaces.len() as u64),
        ("table", tables.len() as u64),
        ("server", servers.len() as u64),
        ("proxy", proxies.len() as u64),
        ("shard", stats.shard_count as u64),
        ("proxy_group", proxy_groups.len() as u64),
        ("reserved_namespace", reserved.namespaces.len() as u64),
        ("reserved_table", reserved.tables.len() as u64),
    ] {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_inventory",
            &[("kind", kind)],
            value,
        );
    }

    out.push_str(
        "# HELP temporalstore_meta_resource_state Current metaserver resource counts by state.\n",
    );
    out.push_str("# TYPE temporalstore_meta_resource_state gauge\n");
    for state in ["normal", "frozen", "dropped"] {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "server"), ("state", state)],
            servers
                .iter()
                .filter(|server| server.state.as_str() == state)
                .count() as u64,
        );
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "proxy"), ("state", state)],
            proxies
                .iter()
                .filter(|proxy| proxy.state.as_str() == state)
                .count() as u64,
        );
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "table"), ("state", state)],
            tables
                .iter()
                .filter(|table| table.state.as_str() == state)
                .count() as u64,
        );
        // A namespace has had a state since it became something an operator can
        // freeze and drop; nothing reported it.
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "namespace"), ("state", state)],
            namespaces
                .iter()
                .filter(|namespace| namespace.state.as_str() == state)
                .count() as u64,
        );
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "proxy_group"), ("state", state)],
            proxy_groups
                .iter()
                .filter(|group| group.state.as_str() == state)
                .count() as u64,
        );
    }
    // The size of what each node is holding, which every heartbeat has carried
    // and nothing reported. Per server rather than per shard: there can be
    // hundreds of thousands of shards, and a series each would be unusable.
    out.push_str(
        "# HELP temporalstore_meta_server_records Records held per server, as the server reports them.
",
    );
    out.push_str("# TYPE temporalstore_meta_server_records gauge
");
    for server in &servers {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_server_records",
            &[("server", server.server_addr.as_str())],
            server.reported_record_count,
        );
    }
    out.push_str(
        "# HELP temporalstore_meta_server_storage_bytes Stored bytes per server, as the server reports them.
",
    );
    out.push_str("# TYPE temporalstore_meta_server_storage_bytes gauge
");
    for server in &servers {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_server_storage_bytes",
            &[("server", server.server_addr.as_str())],
            server.reported_storage_bytes,
        );
    }

    // Which topology each node last applied. The metaserver already exports
    // its own version, so the distance between the two is how far behind a node
    // is on routing changes -- a shard frozen or moved is not in effect on a
    // node that has not caught up yet, and nothing surfaced that.
    //
    // Reported raw rather than as a computed lag: a node that has never told us
    // a version reports zero, and subtracting that from the current version
    // would invent an enormous lag for a node that is merely new.
    out.push_str(
        "# HELP temporalstore_meta_server_applied_topology Topology version each server last applied; compare against temporalstore_meta_topology_version.
",
    );
    out.push_str("# TYPE temporalstore_meta_server_applied_topology gauge
");
    for server in &servers {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_server_applied_topology",
            &[("server", server.server_addr.as_str())],
            server.runtime_load.last_meta_topology_version,
        );
    }

    // What each node is turning away. Reported on every heartbeat and read by
    // nothing, so a node shedding or timing out work was invisible here.
    for (name, help) in [
        ("rejected", "Requests a server rejected."),
        ("timed_out", "Requests a server timed out."),
        ("canceled", "Requests a server canceled."),
    ] {
        out.push_str(&format!(
            "# HELP temporalstore_meta_server_{name}_total {help}
# TYPE temporalstore_meta_server_{name}_total counter
"
        ));
        for server in &servers {
            let value = match name {
                "rejected" => server.runtime_load.rejected_total,
                "timed_out" => server.runtime_load.timed_out_total,
                _ => server.runtime_load.canceled_total,
            };
            push_meta_metric(
                &mut out,
                &format!("temporalstore_meta_server_{name}_total"),
                &[("server", server.server_addr.as_str())],
                value,
            );
        }
    }

    // A datanode that restarts in place is detected and reported; a proxy
    // that does the same has been counted on its record since restart counting
    // existed, and nothing read the number. A proxy cycling repeatedly is worth
    // seeing for the same reason a datanode is.
    out.push_str(
        "# HELP temporalstore_meta_proxy_restarts Proxy restarts observed in place, by proxy.
",
    );
    out.push_str("# TYPE temporalstore_meta_proxy_restarts gauge
");
    for proxy in &proxies {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_proxy_restarts",
            &[("proxy", proxy.proxy_addr.as_str())],
            proxy.restart_count,
        );
    }

    // Shards are counted from the stats rather than listed: there can be
    // hundreds of thousands of them, and a scrape should not page through the
    // whole placement table to count two numbers.
    push_meta_metric(
        &mut out,
        "temporalstore_meta_resource_state",
        &[("resource", "shard"), ("state", "normal")],
        stats.shard_count.saturating_sub(stats.frozen_shard_count) as u64,
    );
    push_meta_metric(
        &mut out,
        "temporalstore_meta_resource_state",
        &[("resource", "shard"), ("state", "frozen")],
        stats.frozen_shard_count as u64,
    );

    out.push_str(
        "# HELP temporalstore_meta_topology_version Current metaserver topology version.\n",
    );
    out.push_str("# TYPE temporalstore_meta_topology_version gauge\n");
    push_meta_metric(
        &mut out,
        "temporalstore_meta_topology_version",
        &[],
        stats.topology_version,
    );

    out.push_str("# HELP temporalstore_meta_scheduler_queue_depth Current metaserver scheduler queue depth.\n");
    out.push_str("# TYPE temporalstore_meta_scheduler_queue_depth gauge\n");
    push_meta_metric(
        &mut out,
        "temporalstore_meta_scheduler_queue_depth",
        &[],
        scheduler_snapshot.queue_len as u64,
    );

    out.push_str("# HELP temporalstore_meta_scheduler_executions_total Metaserver scheduler execution counters by result.\n");
    out.push_str("# TYPE temporalstore_meta_scheduler_executions_total counter\n");
    for result in ["ok", "retry_later", "aborted", "failed"] {
        let count = scheduler_executions
            .executions
            .iter()
            .filter(|record| scheduler_execution_result_label(record) == result)
            .count() as u64;
        push_meta_metric(
            &mut out,
            "temporalstore_meta_scheduler_executions_total",
            &[("result", result)],
            count,
        );
    }

    if let MetaBackend::Raft(runtime) = meta {
        out.push_str(&runtime.cluster().prometheus_metrics());
    }
    out
}

fn scheduler_execution_result_label(record: &MetaSchedulerExecutionRecord) -> &'static str {
    if !record.status.ok {
        return "failed";
    }
    match record.scheduler_result {
        SchedulerTaskResult::Ok => "ok",
        SchedulerTaskResult::RetryLater => "retry_later",
        SchedulerTaskResult::Aborted => "aborted",
    }
}

fn push_meta_metric(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&escape_meta_metric_label(value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_meta_metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[derive(Debug, serde::Deserialize)]
struct MasterTableOptionsRequest {
    #[serde(default)]
    partition_num: u64,
    #[serde(default)]
    pin_primary: Option<bool>,
    #[serde(default)]
    replica_read_policy: Option<String>,
    #[serde(default)]
    preferred_location: Option<String>,
    #[serde(default)]
    drop_percent: Option<u8>,
    #[serde(default)]
    max_read_retries: Option<u32>,
    #[serde(default)]
    max_write_retries: Option<u32>,
    #[serde(default)]
    retry_backoff_ms: Option<u64>,
    #[serde(default)]
    continuous_failed_time_ms: Option<u64>,
    #[serde(default)]
    io_timeout_ms: Option<u64>,
    #[serde(default)]
    connect_timeout_ms: Option<u64>,
}

impl MasterTableOptionsRequest {
    fn serving_options(&self) -> temporalstore_rust::meta::TableServingOptions {
        // Built through the patch rather than field by field, so a table created with
        // an explicit setting is recorded as having set it. Rebuilding the merge here
        // by hand meant the create path silently dropped that, and a table created
        // asking for a default value -- `drop_percent: 0`, "never shed this table" --
        // could not be told apart from one that had asked for nothing.
        self.serving_options_patch()
            .onto(temporalstore_rust::meta::TableServingOptions::default())
    }

    fn serving_options_patch(&self) -> temporalstore_rust::meta::TableServingOptionsPatch {
        temporalstore_rust::meta::TableServingOptionsPatch {
            pin_primary: self.pin_primary,
            replica_read_policy: self.replica_read_policy.clone(),
            preferred_location: self.preferred_location.clone(),
            drop_percent: self.drop_percent,
            max_read_retries: self.max_read_retries,
            max_write_retries: self.max_write_retries,
            retry_backoff_ms: self.retry_backoff_ms,
            continuous_failed_time_ms: self.continuous_failed_time_ms,
            io_timeout_ms: self.io_timeout_ms,
            connect_timeout_ms: self.connect_timeout_ms,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct MasterCreateTableRequest {
    #[serde(alias = "namespace_name")]
    namespace: String,
    #[serde(alias = "name")]
    table_name: String,
    #[serde(default)]
    first_shard_id: u64,
    #[serde(default)]
    shard_count: u64,
    #[serde(default)]
    partition_set_num: u64,
    #[serde(default = "default_master_replica_count")]
    replica_count: u64,
    #[serde(default)]
    table_options: Option<MasterTableOptionsRequest>,
    #[serde(default)]
    partition_version: u32,
}

#[derive(Debug, serde::Deserialize)]
struct MasterUpdateTableRequest {
    #[serde(alias = "namespace_name")]
    namespace: String,
    #[serde(alias = "name")]
    table_name: String,
    #[serde(default)]
    shard_count: Option<u64>,
    #[serde(default)]
    replica_count: Option<u64>,
    #[serde(default)]
    first_shard_id: Option<u64>,
    #[serde(default)]
    table_options: Option<MasterTableOptionsRequest>,
    #[serde(default)]
    partition_version: Option<u32>,
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct MasterTableRequest {
    #[serde(alias = "namespace_name")]
    namespace: String,
    #[serde(alias = "name")]
    table_name: String,
    #[serde(default)]
    open_version: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MasterOpenTableResponse {
    status: Status,
    open_version: u64,
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct MasterGetTableTopoRequest {
    namespace: String,
    table_name: String,
    #[serde(default)]
    old_topo_version: u64,
    #[serde(default)]
    old_topology_version: u64,
    #[serde(default)]
    idc: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    compress: bool,
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct MasterRegisterServerRequest {
    #[serde(default)]
    server_addr: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u32,
    #[serde(default)]
    table_name: String,
    #[serde(default)]
    node_id: u64,
    #[serde(default)]
    location: String,
    #[serde(default)]
    binary_version: String,
}

#[derive(Debug, serde::Deserialize)]
struct HeartbeatEndpoint {
    #[serde(default)]
    ip4: String,
    #[serde(default)]
    ip6: String,
    #[serde(default)]
    port: u32,
}

#[derive(Debug, serde::Deserialize)]
struct HeartbeatServerRequest {
    #[serde(default)]
    server_addr: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u32,
    #[serde(default)]
    location: String,
    #[serde(default)]
    endpoint: Option<HeartbeatEndpoint>,
    #[serde(default)]
    boot_time_ms: u64,
    #[serde(default)]
    boot_time_us: u64,
    #[serde(default)]
    binary_version: String,
}

#[derive(Debug, serde::Deserialize)]
struct HeartbeatProxyRequest {
    #[serde(default)]
    proxy_addr: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u32,
    #[serde(default)]
    location: String,
    #[serde(default)]
    endpoint: Option<HeartbeatEndpoint>,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    namespace_name: String,
    #[serde(default)]
    config_version: u64,
    #[serde(default)]
    binary_version: String,
    /// Boot time in MICROseconds, as the legacy wire shape reports it. Normalised to
    /// milliseconds before it reaches the meta layer.
    #[serde(default)]
    boot_time_us: u64,
}

#[derive(Debug, serde::Deserialize)]
struct ManageNamespaceRequest {
    #[serde(default, alias = "name")]
    namespace: String,
}

include!("metaserver/service_routes.rs");

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn runtime_options_from_env() -> ProductionMetaRaftRuntimeOptions {
    ProductionMetaRaftRuntimeOptions {
        // Checking is cheap; the byte threshold in RaftConfig decides whether a
        // snapshot is actually taken. Set to 0 to stop checking altogether.
        snapshot_check_interval_ms: env_u64("TS_META_RAFT_SNAPSHOT_CHECK_INTERVAL_MS", 30_000),
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: env_u64("TS_META_RAFT_NODE_ID", 1),
        nodes: parse_meta_raft_nodes(),
        config: RaftConfig::default(),
        heartbeat_interval_ms: env_u64("TS_META_RAFT_HEARTBEAT_INTERVAL_MS", 100),
        election_tick_ms: env_u64("TS_META_RAFT_ELECTION_TICK_MS", 50),
        failure_detector_interval_ms: env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000),
        stale_server_after_ms: env_u64("TS_META_STALE_AFTER_MS", 30_000),
        // Read here as well as on the single-node path: `from_env` returns the
        // raft backend before it reaches that one, so this setting used to do
        // nothing at all on a raft-backed metaserver.
        forbid_self_clearing_conviction: env_bool(
            "TS_META_FORBID_SELF_CLEARING_CONVICTION",
            false,
        ),
    }
}

fn parse_meta_raft_nodes() -> Vec<ProductionRaftNode> {
    std::env::var("TS_META_RAFT_NODES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .enumerate()
                .filter_map(|(index, part)| parse_meta_raft_node(index, part.trim()))
                .collect::<Vec<_>>()
        })
        .filter(|nodes| !nodes.is_empty())
        .unwrap_or_else(|| {
            vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:17101".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:17102".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:17103".to_string(),
                },
            ]
        })
}

/// What to say about a metaserver raft group's node list, if anything.
///
/// A metaserver raft group is built entirely inside the process that starts it,
/// and no meta raft entry is ever sent between processes. With one node that is
/// simply the truth: the group is this process. With more, the list describes
/// peers that will never be dialled -- and if a second metaserver is started
/// from the same list it keeps its own metadata, makes the same node leader,
/// and answers ok to writes the first never sees.
///
/// Returned rather than logged so it can be tested; the caller logs it.
fn unreplicated_meta_raft_warning(nodes: &[ProductionRaftNode]) -> Option<String> {
    if nodes.len() <= 1 {
        return None;
    }
    Some(format!(
        "metaserver raft is configured with {} nodes, but meta raft entries are never sent \
         between processes: this group is built inside this process alone. One metaserver \
         started from this list is consistent; a second one started from it keeps its own \
         metadata and the two diverge silently.",
        nodes.len()
    ))
}

fn parse_meta_raft_node(index: usize, value: &str) -> Option<ProductionRaftNode> {
    if let Some((id, addr)) = value.split_once('=') {
        return Some(ProductionRaftNode {
            node_id: id.trim().parse().ok()?,
            addr: addr.trim().to_string(),
        });
    }
    let node_id = value.parse::<RaftNodeId>().ok()?;
    Some(ProductionRaftNode {
        node_id,
        addr: format!("127.0.0.1:{}", 17101 + index),
    })
}

fn parse_or<T, R>(body: &[u8], f: impl FnOnce(T) -> R) -> (u16, Vec<u8>)
where
    T: serde::de::DeserializeOwned,
    R: serde::Serialize,
{
    match parse_json::<T>(body) {
        Ok(req) => json_response(200, &f(req)),
        Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_metaserver_that_cannot_serve_fails_its_readiness_probe() {
        // The probe answered 200 unconditionally, because it returned
        // production_readiness_report() -- a static description of what the codebase
        // supports, which takes no arguments and reads no live state. A metaserver that
        // had lost quorum, or had only just started, said "ready" exactly as loudly as
        // one serving normally, so a load balancer kept sending it traffic it could not
        // answer. The verdict it needed already existed in validate_ready; nothing was
        // asking for it.
        let (code, body) = meta_readiness_response("raft", Err("no majority: 1/2".to_string()));
        assert_eq!(
            code, 503,
            "a metaserver that cannot serve must fail the probe, not report 200"
        );
        assert!(!body.ready);
        assert_eq!(body.backend, "raft");
        assert!(
            body.reason.contains("no majority"),
            "the reason must be reported rather than left to be inferred from a bare 503: {:?}",
            body.reason
        );
        assert!(!body.status.ok);
        assert_eq!(body.status.code, "meta_not_ready");
    }

    #[test]
    fn a_serving_metaserver_passes_its_readiness_probe() {
        // The other half: this must not become a probe that always fails.
        let (code, body) = meta_readiness_response("single", Ok(()));
        assert_eq!(code, 200);
        assert!(body.ready);
        assert_eq!(body.backend, "single");
        assert!(body.reason.is_empty());
        assert!(body.status.ok);
    }

    #[test]
    fn a_single_node_metaserver_is_ready_and_a_raft_one_answers_for_itself() {
        // Wiring, not just the mapping: a single-node backend has no quorum to lose, so
        // it is ready once it is up.
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let (code, body) = backend.readiness();
        assert_eq!(code, 200);
        assert!(body.ready);
        assert_eq!(body.backend, "single");
    }
    use tempfile::tempdir;
    use temporalstore_rust::data_node::DataNodeLifecycleSnapshot;
    use temporalstore_rust::http::HttpRequest;
    use temporalstore_rust::meta::{MetaEntityState, ShardSnapshotRef, TableTopologyResponse};
    use temporalstore_rust::rebalance::RebalanceStep;
    use temporalstore_rust::ProductionReadinessReport;

    /// Drive one route against a single-node backend.
    fn call(method: &str, path: &str, body: Vec<u8>) -> (u16, Vec<u8>) {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: method.to_string(),
                path: path.to_string(),
                body,
            },
        )
    }

    #[test]
    fn opening_and_closing_a_table_report_its_version_and_state() {
        use temporalstore_rust::meta::{
            AddTableRequest, DeleteTableRequest, GetTableTopologyRequest, RegisterServerRequest,
            RegisterShardRequest,
        };

        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "t".to_string(),
            first_shard_id: 1,
            shard_count: 4,
            replica_count: 1,
            partition_version: 1,
            serving_options: Default::default(),
        });
        for shard in 1..=4u64 {
            meta.register(RegisterShardRequest {
                shard_id: shard,
                server_addr: "node-a".to_string(),
            });
        }
        let expected_version = meta
            .get_table_topology(GetTableTopologyRequest {
                namespace: "ns".to_string(),
                table_name: "t".to_string(),
                old_topology_version: 0,
                client_location: String::new(),
            })
            .table
            .expect("the table is there")
            .topology_version;

        let backend = MetaBackend::Single(meta);
        let scheduler = MetaTaskScheduler::default();
        let post = |path: &str, namespace: &str, table_name: &str| {
            handle(
                &backend,
                &scheduler,
                HttpRequest {
                    method: "POST".to_string(),
                    path: path.to_string(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "namespace": namespace,
                        "table_name": table_name,
                    }))
                    .unwrap(),
                },
            )
        };

        // Opening reports the version the caller must quote to get a topology.
        let (code, body) = post("/MasterService/OpenTable", "ns", "t");
        assert_eq!(code, 200);
        let opened: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            opened["open_version"].as_u64(),
            Some(expected_version),
            "opening a table reported the wrong version: {opened}"
        );
        assert_eq!(opened["status"]["ok"].as_bool(), Some(true));

        // Closing reports whether it could be served.
        let (code, body) = post("/MasterService/CloseTable", "ns", "t");
        assert_eq!(code, 200);
        let closed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(closed["status"]["ok"].as_bool(), Some(true));

        // A table that was never created is refused by both, rather than
        // answered with a zero version.
        let (_, body) = post("/MasterService/OpenTable", "ns", "absent");
        let missing: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(missing["status"]["ok"].as_bool(), Some(false));
        assert_eq!(missing["status"]["code"].as_str(), Some("table_not_found"));

        let (_, body) = post("/MasterService/CloseTable", "ns", "absent");
        let missing: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(missing["status"]["ok"].as_bool(), Some(false));

        // And a dropped table is refused too, not reported as openable.
        let dropped = match &backend {
            MetaBackend::Single(meta) => meta.delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "t".to_string(),
            }),
            MetaBackend::Raft(_) => unreachable!("single-node backend"),
        };
        assert!(dropped.status.ok);
        let (_, body) = post("/MasterService/OpenTable", "ns", "t");
        let gone: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(gone["status"]["ok"].as_bool(), Some(false));
    }

    fn node_body(node_id: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "node_id": node_id })).unwrap()
    }

    #[test]
    fn the_membership_routes_exist() {
        // They did not: changing the shape of the meta raft cluster meant
        // restarting it, because the only raft routes were read-only. A 404
        // here means the operation is still unreachable.
        for (method, path, body) in [
            ("GET", "/meta/raft/membership", Vec::new()),
            ("POST", "/meta/raft/add_node", node_body(7)),
            ("POST", "/meta/raft/remove_node", node_body(7)),
            ("POST", "/meta/raft/transfer_leader", node_body(7)),
            ("POST", "/meta/raft/snapshot", Vec::new()),
        ] {
            let (code, _) = call(method, path, body);
            assert_eq!(code, 200, "{method} {path} is not routed");
        }
    }

    #[test]
    fn a_single_node_metaserver_says_so_rather_than_pretending() {
        // Every one of these is meaningless without a cluster. Refusing
        // clearly beats a route that appears to have done something.
        for (method, path, body) in [
            ("POST", "/meta/raft/add_node", node_body(7)),
            ("POST", "/meta/raft/remove_node", node_body(7)),
            ("POST", "/meta/raft/transfer_leader", node_body(7)),
            ("POST", "/meta/raft/snapshot", Vec::new()),
        ] {
            let (_, body) = call(method, path, body);
            let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                parsed["status"]["code"], "raft_disabled",
                "{method} {path} did not refuse"
            );
        }
    }

    #[test]
    fn listing_membership_without_a_cluster_is_empty_and_says_why() {
        let (code, body) = call("GET", "/meta/raft/membership", Vec::new());
        assert_eq!(code, 200);
        let parsed: MetaRaftMembershipResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.status.code, "raft_disabled");
        assert!(parsed.members.is_empty());
    }

    #[test]
    fn a_scale_change_that_was_refused_carries_no_report() {
        // The report describes the membership a change settled into. Returning
        // one for a change that did not happen would be a lie an operator
        // could act on.
        let (_, body) = call("POST", "/meta/raft/add_node", node_body(7));
        let parsed: MetaRaftScaleResponse = serde_json::from_slice(&body).unwrap();
        assert!(!parsed.status.ok);
        assert!(parsed.report.is_none());
    }

    #[test]
    fn a_membership_request_without_a_node_is_rejected_not_guessed() {
        let (code, body) = call("POST", "/meta/raft/add_node", b"{}".to_vec());
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            code >= 400 || parsed["status"]["ok"] == serde_json::Value::Bool(false),
            "an empty body was accepted: {code} {parsed}"
        );
    }

    /// The scheduler knobs, set for the duration of one test.
    ///
    /// Serialised because the environment is process-wide, and two of these
    /// running at once would read each other's values.
    fn with_scheduler_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|e| e.into_inner());
        for (key, value) in pairs {
            std::env::set_var(key, value);
        }
        let out = body();
        for (key, _) in pairs {
            std::env::remove_var(key);
        }
        out
    }

    #[test]
    fn a_meta_raft_group_says_it_does_not_span_processes() {
        // The group is built inside the process that starts it and no meta raft
        // entry is ever sent between processes, so a list of peers describes
        // addresses that will never be dialled. One node is that truth stated
        // plainly and needs nothing said; more than one is worth saying, because
        // a second metaserver started from the same list diverges in silence.
        let node = |node_id| ProductionRaftNode {
            node_id,
            addr: format!("10.0.0.{node_id}:17001"),
        };
        assert_eq!(unreplicated_meta_raft_warning(&[]), None);
        assert_eq!(unreplicated_meta_raft_warning(&[node(1)]), None);

        let warning = unreplicated_meta_raft_warning(&[node(1), node(2), node(3)])
            .expect("three nodes is worth saying something about");
        assert!(warning.contains("3"), "it should say how many: {warning}");
        assert!(
            warning.contains("diverge"),
            "it should say what goes wrong: {warning}"
        );
    }

    #[test]
    fn the_scheduler_takes_its_pacing_from_the_environment() {
        // Every other background subsystem here is configurable; this one had
        // its backoff and its concurrency compiled in.
        let scheduler = with_scheduler_env(
            &[
                ("TS_META_TASK_SCHEDULER_BASE_POSTPONE_MS", "250"),
                ("TS_META_TASK_SCHEDULER_MAX_POSTPONE_MS", "300000"),
                ("TS_META_TASK_SCHEDULER_MAX_RETRY_TIMES", "3"),
                ("TS_META_TASK_SCHEDULER_MAX_INFLIGHT", "10"),
            ],
            || MetaTaskScheduler::from_env().expect("scheduler"),
        );
        assert_eq!(scheduler.default_options.base_postpone_ms, 250);
        assert_eq!(scheduler.default_options.max_postpone_ms, 300_000);
        assert_eq!(scheduler.default_options.max_retry_times, 3);
        assert_eq!(scheduler.default_options.max_inflight, 10);
    }

    #[test]
    fn an_unconfigured_scheduler_paces_exactly_as_before() {
        // The knobs must change nothing until someone sets them.
        let scheduler = with_scheduler_env(&[], || {
            MetaTaskScheduler::from_env().expect("scheduler")
        });
        assert_eq!(scheduler.default_options, TaskSchedulerOptions::default());
    }

    #[test]
    fn a_request_that_names_its_own_pacing_still_wins() {
        // The per-request options were the only way to set these, and callers
        // that use them must keep overriding the configured default.
        let scheduler = with_scheduler_env(
            &[("TS_META_TASK_SCHEDULER_BASE_POSTPONE_MS", "9999")],
            || MetaTaskScheduler::from_env().expect("scheduler"),
        );
        assert_eq!(scheduler.default_options.base_postpone_ms, 9999);

        let (code, body) = handle(
            &MetaBackend::Single(SingleNodeMeta::default()),
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/run_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 100,
                    "result": "RetryLater",
                    "options": {
                        "base_postpone_ms": 5,
                        "max_postpone_ms": 50,
                        "max_retry_times": 2,
                        "max_inflight": 1
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        // An empty queue answers with no report either way; what matters is
        // that supplying options is still accepted rather than rejected.
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(parsed.get("status").is_some());
    }

    /// Scrape `/metrics` from a backend the caller has set up.
    fn scrape(backend: &MetaBackend) -> String {
        let scheduler = MetaTaskScheduler::default();
        let (code, body) = handle(
            backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/metrics".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        String::from_utf8(body).expect("utf8")
    }

    /// A heartbeat reporting shards of the given sizes.
    fn report_sizes(meta: &SingleNodeMeta, addr: &str, sizes: &[(usize, u64)]) {
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: addr.to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: temporalstore_rust::meta::ServerRuntimeLoad::default(),
            shard_states: sizes
                .iter()
                .enumerate()
                .map(|(index, (records, bytes))| temporalstore_rust::meta::ServerShardServingState {
                    shard_id: index as temporalstore_rust::types::ShardId + 1,
                    serving_state: "serving".to_string(),
                    worker_index: 0,
                    worker_threads: 1,
                    loaded: true,
                    readonly: false,
                    load_version: 1,
                    table_name: "ns.orders".to_string(),
                    shard_uri: String::new(),
                    start_routing_bucket: 0,
                    end_routing_bucket: u32::MAX,
                    total_records: *records,
                    storage_bytes: *bytes,
                    cache_memory_bytes: 0,
                    storage: Default::default(),
                    block_store_bytes_written: 0,
                    wal_sequence: 0,
                    dirty_object_count: 0,
                    dirty_bucket_count: 0,
                })
                .collect(),
        });
    }

    /// A heartbeat carrying a runtime-load report.
    fn report_runtime(
        meta: &SingleNodeMeta,
        addr: &str,
        load: temporalstore_rust::meta::ServerRuntimeLoad,
    ) {
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: addr.to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: load,
            shard_states: Vec::new(),
        });
    }

    fn joined(meta: &SingleNodeMeta, addr: &str) {
        meta.register_server(RegisterServerRequest {
            server_addr: addr.to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
    }

    #[test]
    fn how_far_behind_a_node_is_on_topology_is_visible() {
        // The metaserver already exports its own topology version; each node
        // reports the last one it applied, and nothing compared them. A shard
        // frozen or moved is not in effect on a node that has not caught up.
        let meta = SingleNodeMeta::default();
        joined(&meta, "node-a");
        report_runtime(
            &meta,
            "node-a",
            temporalstore_rust::meta::ServerRuntimeLoad {
                last_meta_topology_version: 7,
                ..Default::default()
            },
        );
        let out = scrape(&MetaBackend::Single(meta));
        assert!(
            out.contains("temporalstore_meta_server_applied_topology{server=\"node-a\"} 7")
        );
        // The metaserver's own version is exported too, so the two can be
        // compared without the metaserver inventing a lag figure.
        assert!(out.contains("temporalstore_meta_topology_version"));
    }

    #[test]
    fn a_node_that_has_never_said_reports_zero_not_a_huge_lag() {
        // Reported raw on purpose: subtracting an unreported zero from the
        // current version would invent an enormous lag for a node that is
        // merely new.
        let meta = SingleNodeMeta::default();
        joined(&meta, "node-a");
        let out = scrape(&MetaBackend::Single(meta));
        assert!(
            out.contains("temporalstore_meta_server_applied_topology{server=\"node-a\"} 0")
        );
    }

    #[test]
    fn what_a_node_turns_away_is_reported() {
        let meta = SingleNodeMeta::default();
        joined(&meta, "node-a");
        report_runtime(
            &meta,
            "node-a",
            temporalstore_rust::meta::ServerRuntimeLoad {
                rejected_total: 12,
                timed_out_total: 3,
                canceled_total: 5,
                ..Default::default()
            },
        );
        let out = scrape(&MetaBackend::Single(meta));
        assert!(out.contains("temporalstore_meta_server_rejected_total{server=\"node-a\"} 12"));
        assert!(out.contains("temporalstore_meta_server_timed_out_total{server=\"node-a\"} 3"));
        assert!(out.contains("temporalstore_meta_server_canceled_total{server=\"node-a\"} 5"));
    }

    #[test]
    fn the_size_a_node_holds_is_reported() {
        // Every heartbeat has carried these figures per shard and nothing read
        // either, so the metaserver knew how much data sat on every node and
        // had no way to say so.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        report_sizes(&meta, "node-a", &[(10, 1_000), (20, 2_000), (30, 3_000)]);

        let out = scrape(&MetaBackend::Single(meta));
        assert!(out.contains("temporalstore_meta_server_records{server=\"node-a\"} 60"));
        assert!(
            out.contains("temporalstore_meta_server_storage_bytes{server=\"node-a\"} 6000")
        );
    }

    #[test]
    fn a_node_that_sheds_shards_reports_less() {
        // The summary describes the latest report. Accumulating would make a
        // node look permanently full once it had ever been busy -- the same
        // trap as the placement figures beside it.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        report_sizes(&meta, "node-a", &[(10, 1_000), (20, 2_000)]);
        let backend = MetaBackend::Single(meta.clone());
        assert!(scrape(&backend).contains("temporalstore_meta_server_records{server=\"node-a\"} 30"));

        report_sizes(&meta, "node-a", &[(5, 500)]);
        let after = scrape(&backend);
        assert!(after.contains("temporalstore_meta_server_records{server=\"node-a\"} 5"));
        assert!(after.contains("temporalstore_meta_server_storage_bytes{server=\"node-a\"} 500"));
    }

    #[test]
    fn a_node_that_has_said_nothing_reports_nothing() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        let out = scrape(&MetaBackend::Single(meta));
        assert!(out.contains("temporalstore_meta_server_records{server=\"node-a\"} 0"));
    }

    #[test]
    fn a_proxy_cycling_in_place_is_visible() {
        // The count has been kept on the proxy's record since restart counting
        // existed and nothing read it. A datanode that restarts in place is
        // detected and reported; a proxy doing the same was silent.
        let meta = SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 0,
            binary_version: "v1".to_string(),
        });
        let beat = |boot_time_ms: u64| {
            meta.proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                config_version: 0,
                boot_time_ms,
                binary_version: "v1".to_string(),
            });
        };
        beat(100);
        let backend = MetaBackend::Single(meta.clone());
        assert!(scrape(&backend)
            .contains("temporalstore_meta_proxy_restarts{proxy=\"proxy-a\"} 0"));

        // Same address, different boot time: it came back as a new process.
        beat(200);
        assert!(scrape(&backend)
            .contains("temporalstore_meta_proxy_restarts{proxy=\"proxy-a\"} 1"));

        // A heartbeat from the same process must not count again.
        beat(200);
        assert!(scrape(&backend)
            .contains("temporalstore_meta_proxy_restarts{proxy=\"proxy-a\"} 1"));
    }

    #[test]
    fn a_frozen_namespace_shows_up_on_the_dashboard() {
        // A namespace has had a state since it became something an operator can
        // freeze and drop, and nothing reported it: you could take a tenant out
        // of service and see no change on any dashboard.
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string(),
        });
        let backend = MetaBackend::Single(meta.clone());
        assert!(scrape(&backend).contains(
            "temporalstore_meta_resource_state{resource=\"namespace\",state=\"normal\"} 1"
        ));

        assert!(
            meta.freeze_namespace(AddNamespaceRequest {
                namespace: "tenant".to_string(),
            })
            .status
            .ok
        );
        let after = scrape(&backend);
        assert!(after.contains(
            "temporalstore_meta_resource_state{resource=\"namespace\",state=\"frozen\"} 1"
        ));
        assert!(after.contains(
            "temporalstore_meta_resource_state{resource=\"namespace\",state=\"normal\"} 0"
        ));
    }

    #[test]
    fn a_frozen_shard_shows_up_on_the_dashboard() {
        let meta = SingleNodeMeta::default();
        for shard_id in [1, 2] {
            meta.register(RegisterShardRequest {
                shard_id,
                server_addr: "node-a".to_string(),
            });
        }
        let backend = MetaBackend::Single(meta.clone());
        assert!(scrape(&backend).contains(
            "temporalstore_meta_resource_state{resource=\"shard\",state=\"normal\"} 2"
        ));

        assert!(meta.freeze_shard(ShardStateRequest { shard_id: 1 }).status.ok);
        let after = scrape(&backend);
        assert!(after.contains(
            "temporalstore_meta_resource_state{resource=\"shard\",state=\"frozen\"} 1"
        ));
        assert!(after.contains(
            "temporalstore_meta_resource_state{resource=\"shard\",state=\"normal\"} 1"
        ));
    }

    #[test]
    fn the_inventory_counts_groups_and_reserved_names() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.put_proxy_group(PutProxyGroupRequest {
                drop_percent: 0,
                group: "front".to_string(),
                namespace: "tenant".to_string(),
                location: String::new(),
                instance_num: 2,
            })
            .status
            .ok
        );
        assert!(
            meta.set_reserved_names(ReservedNames {
                namespaces: ["system".to_string()].into_iter().collect(),
                tables: ["audit".to_string(), "wal".to_string()].into_iter().collect(),
            })
            .status
            .ok
        );
        let out = scrape(&MetaBackend::Single(meta));
        assert!(out.contains("temporalstore_meta_inventory{kind=\"proxy_group\"} 1"));
        assert!(out.contains("temporalstore_meta_inventory{kind=\"reserved_namespace\"} 1"));
        assert!(out.contains("temporalstore_meta_inventory{kind=\"reserved_table\"} 2"));
    }

    #[test]
    fn the_shard_states_always_add_up_to_the_total() {
        // The two rows are derived from one another, so a mistake in either
        // shows as a total that does not match the inventory.
        let meta = SingleNodeMeta::default();
        for shard_id in 1..=5 {
            meta.register(RegisterShardRequest {
                shard_id,
                server_addr: "node-a".to_string(),
            });
        }
        assert!(meta.freeze_shard(ShardStateRequest { shard_id: 3 }).status.ok);
        let out = scrape(&MetaBackend::Single(meta));
        assert!(out.contains("temporalstore_meta_inventory{kind=\"shard\"} 5"));
        assert!(out.contains(
            "temporalstore_meta_resource_state{resource=\"shard\",state=\"normal\"} 4"
        ));
        assert!(out.contains(
            "temporalstore_meta_resource_state{resource=\"shard\",state=\"frozen\"} 1"
        ));
    }

    #[test]
    fn metaserver_metrics_expose_inventory_state_and_scheduler() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: "metrics-server-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "metrics-proxy-a".to_string(),
            namespace: "metrics-ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 7,
            binary_version: "v1".to_string(),
        });
        meta.freeze_proxy(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "metrics-proxy-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "metrics-ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "metrics-ns".to_string(),
            table_name: "metrics-table".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: temporalstore_rust::meta::TableServingOptions::default(),
        });
        let backend = MetaBackend::Single(meta);
        let scheduler = MetaTaskScheduler::default();
        let submitted = scheduler.submit(MetaSchedulerSubmitRequest {
            priority: 0,
            now_ms: 1,
            kind: SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
                shard_id: 1,
                replica_id: 1,
                node_id: 1,
            }),
        });
        assert!(submitted.status.ok, "{submitted:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/metrics".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let metrics = String::from_utf8(body).unwrap();
        assert!(metrics.contains("# TYPE temporalstore_meta_requests_total counter"));
        // The background subsystems report onto the same surface, so an
        // operator sees conviction, divergence and retention without logs.
        assert!(metrics.contains("# TYPE temporalstore_meta_convicted_total counter"));
        assert!(metrics.contains("# TYPE temporalstore_meta_damage_severity gauge"));
        assert!(metrics.contains("temporalstore_meta_shard_divergence_total 0"));
        assert!(metrics.contains("temporalstore_meta_retention_blocked 0"));
        assert!(metrics.contains("temporalstore_meta_inventory{kind=\"namespace\"} 1"));
        assert!(metrics.contains("temporalstore_meta_inventory{kind=\"table\"} 1"));
        assert!(metrics
            .contains("temporalstore_meta_resource_state{resource=\"server\",state=\"normal\"} 1"));
        assert!(metrics
            .contains("temporalstore_meta_resource_state{resource=\"proxy\",state=\"frozen\"} 1"));
        assert!(metrics.contains("temporalstore_meta_scheduler_queue_depth 1"));
        assert!(metrics.contains("temporalstore_meta_topology_version"));
    }

    #[test]
    fn the_raft_backend_reports_no_background_subsystem_activity() {
        // The startup warnings tell an operator that rebalancing, divergence
        // checking, freeze aging, retention and proxy calibration do not happen
        // on this backend. Two of them used to claim raft "manages placement
        // itself"; nothing in the raft module plans a rebalance or checks
        // divergence, and both loops are started only from Single arms.
        //
        // This pins the fact those messages now assert, so a future change that
        // gives raft one of these capabilities has to update the text as well.
        let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
            forbid_self_clearing_conviction: false,
            snapshot_check_interval_ms: 0,
            engine: ProductionRaftEngineKind::TemporalRaft,
            local_node_id: 1,
            nodes: vec![ProductionRaftNode {
                node_id: 1,
                addr: "127.0.0.1:18211".to_string(),
            }],
            config: RaftConfig::default(),
            heartbeat_interval_ms: 100,
            election_tick_ms: 50,
            failure_detector_interval_ms: 1_000,
            stale_server_after_ms: 30_000,
        })
        .unwrap();
        let backend = MetaBackend::Raft(runtime);
        assert!(
            backend.subsystem_prometheus().is_empty(),
            "the raft backend reported subsystem activity it does not perform"
        );

        // The single-node backend does drive them, so an empty report there
        // would mean the check above proves nothing.
        let single = MetaBackend::Single(SingleNodeMeta::default());
        let _ = single.subsystem_prometheus();
    }

    #[test]
    fn metaserver_snapshot_routes_export_save_load_and_restore_state() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("meta-route-snapshot.json");
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: "server-route-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 91,
            server_addr: "server-route-a".to_string(),
        });
        meta.publish_shard_snapshot(PublishShardSnapshotRequest {
            shard_id: 91,
            snapshot: ShardSnapshotRef {
                uri: "s3://cluster/shards/91/snapshots/1-7/manifest.json".to_string(),
                checksum: "sha256:route".to_string(),
                byte_size: 91,
                last_log_index: 7,
                created_at_ms: 10,
            },
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-route-a".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 3,
            binary_version: "v1".to_string(),
        });
        meta.freeze_proxy(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "proxy-route-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 91,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: temporalstore_rust::meta::TableServingOptions::default(),
        });
        let backend = MetaBackend::Single(meta.clone());
        let scheduler = MetaTaskScheduler::default();

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/snapshot".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let exported: MetaSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(exported.status.ok);
        assert_eq!(exported.snapshot.as_ref().unwrap().stats.shard_count, 1);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/save".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let saved: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(saved.status.ok);
        assert!(snapshot_path.exists());

        meta.drop_proxy(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "proxy-route-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        assert_eq!(
            meta.list_proxies().proxies[0].state,
            MetaEntityState::Dropped
        );

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/load".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let loaded: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(loaded.status.ok);
        assert_eq!(
            meta.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );

        let restored = SingleNodeMeta::default();
        let restore_backend = MetaBackend::Single(restored.clone());
        let restore_scheduler = MetaTaskScheduler::default();
        let (code, body) = handle(
            &restore_backend,
            &restore_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/restore".to_string(),
                body: serde_json::to_vec(&exported.snapshot.unwrap()).unwrap(),
            },
        );
        assert_eq!(code, 200);
        let ack: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(ack.status.ok);
        assert_eq!(
            restored.get(91).location.unwrap().server_addr,
            "server-route-a"
        );
        assert_eq!(
            restored.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );
    }

    #[test]
    fn metaserver_safe_mode_route_reports_frozen_cooldown_resources() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            numa_nodes: Vec::new(),
            server_addr: "safe-server".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "safe-proxy".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        });
        let backend = MetaBackend::Single(meta);
        let scheduler = MetaTaskScheduler::default();
        std::thread::sleep(std::time::Duration::from_millis(2));

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/freeze_stale".to_string(),
                body: serde_json::to_vec(&FreezeStaleServersRequest {
                    stale_after_ms: 0,
                    server_freeze_cooldown_ms: 60_000,
                    proxy_freeze_cooldown_ms: 60_000,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let stale: temporalstore_rust::meta::StaleResourceReport =
            serde_json::from_slice(&body).unwrap();
        assert!(stale.status.ok, "{stale:?}");
        assert_eq!(stale.frozen_servers, vec!["safe-server".to_string()]);
        assert_eq!(stale.frozen_proxies, vec!["safe-proxy".to_string()]);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/safe_mode".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let safe_mode: temporalstore_rust::meta::SafeModeReport =
            serde_json::from_slice(&body).unwrap();
        assert!(safe_mode.status.ok, "{safe_mode:?}");
        assert_eq!(safe_mode.blocked_servers, vec!["safe-server".to_string()]);
        assert_eq!(safe_mode.blocked_proxies, vec!["safe-proxy".to_string()]);
    }

    #[test]
    fn metaserver_raft_snapshot_routes_export_save_load_and_restore_state() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("meta-raft-route-snapshot.json");
        let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
            forbid_self_clearing_conviction: false,
            snapshot_check_interval_ms: 0,
            engine: ProductionRaftEngineKind::TemporalRaft,
            local_node_id: 1,
            nodes: vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:18101".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:18102".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:18103".to_string(),
                },
            ],
            config: RaftConfig::default(),
            heartbeat_interval_ms: 100,
            election_tick_ms: 50,
            failure_detector_interval_ms: 1_000,
            stale_server_after_ms: 30_000,
        })
        .unwrap();
        let backend = MetaBackend::Raft(runtime);
        let scheduler = MetaTaskScheduler::default();

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/register".to_string(),
                body: serde_json::to_vec(&RegisterServerRequest {
                    numa_nodes: Vec::new(),
                    server_addr: "raft-server-a".to_string(),
                    node_id: 11,
                    location: "zone-a".to_string(),
                    binary_version: "v1".to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let ack: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(ack.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/snapshot".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let exported: MetaSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(exported.status.ok);
        assert_eq!(exported.snapshot.as_ref().unwrap().stats.server_count, 1);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/save".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let saved: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(saved.status.ok);
        assert!(snapshot_path.exists());

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/drop".to_string(),
                body: serde_json::to_vec(&StateChangeRequest {
                    reason: FreezeReason::Unspecified,
                    endpoint: "raft-server-a".to_string(),
                    freeze_cooldown_ms: 0,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let dropped: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(dropped.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/load".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let loaded: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(loaded.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/servers".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let servers: temporalstore_rust::meta::ListServersResponse =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(servers.servers[0].state, MetaEntityState::Normal);

        let restored_runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
            forbid_self_clearing_conviction: false,
            snapshot_check_interval_ms: 0,
            engine: ProductionRaftEngineKind::TemporalRaft,
            local_node_id: 1,
            nodes: vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:18201".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:18202".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:18203".to_string(),
                },
            ],
            config: RaftConfig::default(),
            heartbeat_interval_ms: 100,
            election_tick_ms: 50,
            failure_detector_interval_ms: 1_000,
            stale_server_after_ms: 30_000,
        })
        .unwrap();
        let restore_backend = MetaBackend::Raft(restored_runtime);
        let (code, body) = handle(
            &restore_backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/restore".to_string(),
                body: serde_json::to_vec(&exported.snapshot.unwrap()).unwrap(),
            },
        );
        assert_eq!(code, 200);
        let restored: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(restored.status.ok);
    }

    #[test]
    fn metaserver_scheduler_routes_submit_run_snapshot_and_restore_tasks() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let low_priority = SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
            shard_id: 7,
            replica_id: 11,
            node_id: 2,
        });
        let high_priority = SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
            shard_id: 7,
            replica_id: 12,
            node_id: 3,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 10,
                    "now_ms": 100,
                    "kind": low_priority,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let low: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(low.status.ok);
        assert_eq!(low.queue_len, 1);
        let low_token = low.lifecycle_token.unwrap();
        assert_eq!(low_token.shard_id, 7);
        assert_eq!(low_token.operation, "freeze");
        assert_eq!(low_token.generation, 100);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 100,
                    "kind": high_priority,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let high: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(high.status.ok);
        assert_eq!(high.queue_len, 2);
        let high_token = high.lifecycle_token.unwrap();
        assert_eq!(high_token.shard_id, 7);
        assert_eq!(high_token.operation, "unload");
        assert_eq!(high_token.generation, 100);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/run_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 100,
                    "result": "RetryLater",
                    "options": {
                        "base_postpone_ms": 5,
                        "max_postpone_ms": 50,
                        "max_retry_times": 3,
                        "max_inflight": 1
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let retry: MetaSchedulerRunResponse = serde_json::from_slice(&body).unwrap();
        assert!(retry.status.ok);
        let retry_report = retry.report.unwrap();
        assert_eq!(retry_report.task_id, high.task.unwrap().id);
        assert_eq!(retry_report.next_run_time_ms, Some(105));
        assert_eq!(retry.queue_len, 2);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let snapshot: MetaSchedulerSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(snapshot.status.ok);
        assert_eq!(snapshot.queue_len, 2);

        let restored_scheduler = MetaTaskScheduler::default();
        let (code, body) = handle(
            &backend,
            &restored_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/restore".to_string(),
                body: serde_json::to_vec(&MetaSchedulerRestoreRequest {
                    snapshot: snapshot.snapshot.unwrap(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let restored: MetaSchedulerSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(restored.status.ok);
        assert_eq!(restored.queue_len, 2);

        let (code, body) = handle(
            &backend,
            &restored_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/run_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 104,
                    "result": "Ok"
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let run: MetaSchedulerRunResponse = serde_json::from_slice(&body).unwrap();
        assert!(run.status.ok);
        assert_eq!(run.report.unwrap().task_id, low.task.unwrap().id);
        assert_eq!(run.queue_len, 1);
    }

    fn one_execution_record() -> MetaSchedulerExecutionRecord {
        MetaSchedulerExecutionRecord {
            task_id: 1,
            node_addr: "127.0.0.1:1".to_string(),
            status: Status::ok(),
            scheduler_result: SchedulerTaskResult::Ok,
            retry_times: 0,
            next_run_time_ms: None,
            calls: Vec::new(),
            lifecycle_token: None,
            lifecycle_state: None,
            raft_membership_report: None,
            queue_len: 0,
        }
    }

    #[test]
    fn an_execution_that_cannot_be_persisted_says_so() {
        // `persist_current` writes the execution history and the scheduler
        // snapshot together. Its result used to be dropped here while `submit`,
        // `run_next` and `restore` all return theirs, so a failed write left the
        // post-execution state non-durable with nobody told -- and a restart
        // could hand out a task that had already run.
        let dir = tempfile::tempdir().unwrap();
        // A regular file where a directory would have to be: `create_dir_all`
        // cannot make a directory under it, so persisting fails for a reason
        // that has nothing to do with the execution itself.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let scheduler =
            MetaTaskScheduler::with_snapshot_path(blocker.join("sub").join("scheduler.json"))
                .unwrap();

        let persisted = scheduler.record_execution(one_execution_record());
        assert!(
            !persisted.ok,
            "persisting was impossible and it reported success"
        );
        assert_eq!(persisted.code, "scheduler_persist_failed");
    }

    #[test]
    fn an_execution_that_persists_reports_ok() {
        // The other half: the guard must not turn a healthy round into an error.
        let dir = tempfile::tempdir().unwrap();
        let scheduler =
            MetaTaskScheduler::with_snapshot_path(dir.path().join("scheduler.json")).unwrap();
        let persisted = scheduler.record_execution(one_execution_record());
        assert!(persisted.ok, "{persisted:?}");
        assert_eq!(scheduler.executions().executions.len(), 1);
    }

    #[test]
    fn metaserver_scheduler_execute_next_dry_run_preserves_queue() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
            shard_id: 42,
            replica_id: 9,
            node_id: 5,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 700,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 700,
                    "node_addr": "127.0.0.1:1",
                    "dry_run": true
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(executed.status.ok);
        assert!(executed.dry_run);
        assert!(executed.scheduler_report.is_none());
        assert_eq!(executed.queue_len, 1);
        assert_eq!(executed.calls.len(), 2);
        assert!(executed.calls.iter().all(|call| call.skipped));
        assert_eq!(
            executed.calls[0].path,
            "/ServerService/RequireLifecycleToken"
        );
        assert_eq!(executed.calls[1].path, "/ServerService/Unload");
        assert_eq!(scheduler.snapshot().queue_len, 1);
        assert_eq!(submitted.queue_len, 1);
    }

    // shared-corpus: control_metaserver_scheduler_lifecycle_workflow
    #[test]
    fn metaserver_scheduler_execute_next_installs_token_then_loads_node() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_recording_nodeserver();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::LoadTarget {
            shard_id: 44,
            replica_id: 8,
            node_id: 9,
            load_version: 5,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 900,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        let submitted_task = submitted.task.unwrap();

        let load_request = LoadShardRequest {
            shard_id: 44,
            load_version: 5,
            local_node_id: None,
            shard_uri: "memory://metaserver-executor".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket: 1023,
            readonly: false,
            table_name: "executor_table".to_string(),
        };
        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 900,
                    "node_addr": node_addr,
                    "load_request": load_request,
                    "http": {
                        "connect_timeout_ms": 1000,
                        "io_timeout_ms": 1000,
                        "max_retries": 10
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(executed.status.ok, "{executed:?}");
        assert_eq!(executed.queue_len, 0);
        assert_eq!(
            executed.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(executed.calls.len(), 3);
        assert_eq!(
            executed.calls[0].path,
            "/ServerService/RequireLifecycleToken"
        );
        assert_eq!(executed.calls[1].path, "/ServerService/Load");
        assert_eq!(executed.calls[2].path, "/ServerService/GetLifecycle");
        let lifecycle_state = executed.lifecycle_state.as_ref().unwrap();
        assert_eq!(lifecycle_state.shard_id, 44);
        assert_eq!(lifecycle_state.operation, "load");
        assert_eq!(lifecycle_state.scheduler_task_id, Some(submitted_task.id));
        assert_eq!(lifecycle_state.scheduler_generation, Some(900));

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler/executions".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let executions: MetaSchedulerExecutionsResponse = serde_json::from_slice(&body).unwrap();
        assert!(executions.status.ok);
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(executions.executions[0].task_id, submitted_task.id);
        assert_eq!(
            executions.executions[0].scheduler_result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(
            executions.executions[0]
                .lifecycle_state
                .as_ref()
                .unwrap()
                .operation,
            "load"
        );

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/register".to_string(),
                body: serde_json::to_vec(&RegisterServerRequest {
                    numa_nodes: Vec::new(),
                    server_addr: node_addr.clone(),
                    node_id: 9,
                    location: "zone-a".to_string(),
                    binary_version: "v1".to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let registered: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(registered.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/shards/finish_load".to_string(),
                body: serde_json::to_vec(&LoadFinishRequest {
                    server_addr: node_addr.clone(),
                    shard_id: 44,
                    load_version: 5,
                    status: Status::ok(),
                    scheduler_task_id: Some(submitted_task.id),
                    scheduler_generation: Some(900),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let finish: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(finish.status.ok, "{finish:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/shards/finish_load".to_string(),
                body: serde_json::to_vec(&LoadFinishRequest {
                    server_addr: node_addr.clone(),
                    shard_id: 44,
                    load_version: 5,
                    status: Status::ok(),
                    scheduler_task_id: Some(submitted_task.id),
                    scheduler_generation: Some(899),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let stale_finish: AckResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(stale_finish.status.code, "scheduler_finish_load_not_found");

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, "/ServerService/RequireLifecycleToken");
        assert_eq!(records[0].1["operation"], "load");
        assert_eq!(records[0].1["task_id"], submitted_task.id);
        assert_eq!(records[1].0, "/ServerService/Load");
        assert_eq!(records[1].1["shard_id"], 44);
        assert_eq!(records[1].1["load_version"], 5);
        assert_eq!(records[1].1["local_node_id"], 9);
        assert_eq!(records[2].0, "/ServerService/GetLifecycle");
    }

    #[test]
    fn metaserver_scheduler_execute_next_installs_token_then_reloads_node() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_recording_reload_nodeserver();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::ReloadTarget {
            shard_id: 44,
            replica_id: 8,
            node_id: 9,
            load_version: 6,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 901,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        let submitted_task = submitted.task.unwrap();
        assert_eq!(
            submitted.lifecycle_token.as_ref().unwrap().operation,
            "reload"
        );

        let load_request = LoadShardRequest {
            shard_id: 44,
            load_version: 6,
            local_node_id: None,
            shard_uri: "memory://metaserver-reload-executor".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket: 1023,
            readonly: true,
            table_name: "executor_table".to_string(),
        };
        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 901,
                    "node_addr": node_addr,
                    "load_request": load_request,
                    "http": {
                        "connect_timeout_ms": 1000,
                        "io_timeout_ms": 1000,
                        "max_retries": 10
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(executed.status.ok, "{executed:?}");
        assert_eq!(executed.queue_len, 0);
        assert_eq!(
            executed.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(executed.calls.len(), 3);
        assert_eq!(
            executed.calls[0].path,
            "/ServerService/RequireLifecycleToken"
        );
        assert_eq!(executed.calls[1].path, "/ServerService/Reload");
        assert_eq!(executed.calls[2].path, "/ServerService/GetLifecycle");
        let lifecycle_state = executed.lifecycle_state.as_ref().unwrap();
        assert_eq!(lifecycle_state.shard_id, 44);
        assert_eq!(lifecycle_state.operation, "reload");
        assert_eq!(lifecycle_state.state, "readonly");
        assert_eq!(lifecycle_state.load_version, 6);
        assert_eq!(lifecycle_state.scheduler_task_id, Some(submitted_task.id));
        assert_eq!(lifecycle_state.scheduler_generation, Some(901));

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler/executions".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let executions: MetaSchedulerExecutionsResponse = serde_json::from_slice(&body).unwrap();
        assert!(executions.status.ok);
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(
            executions.executions[0].scheduler_result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(
            executions.executions[0]
                .lifecycle_state
                .as_ref()
                .unwrap()
                .operation,
            "reload"
        );

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, "/ServerService/RequireLifecycleToken");
        assert_eq!(records[0].1["operation"], "reload");
        assert_eq!(records[0].1["task_id"], submitted_task.id);
        assert_eq!(records[1].0, "/ServerService/Reload");
        assert_eq!(records[1].1["shard_id"], 44);
        assert_eq!(records[1].1["load_version"], 6);
        assert_eq!(records[1].1["local_node_id"], 9);
        assert_eq!(records[2].0, "/ServerService/GetLifecycle");
    }

    #[test]
    fn metaserver_scheduler_retries_busy_unload_without_dropping_task() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_busy_unload_nodeserver();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
            shard_id: 44,
            replica_id: 8,
            node_id: 9,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 900,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        let submitted_task = submitted.task.unwrap();

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 900,
                    "node_addr": node_addr,
                    "options": {
                        "base_postpone_ms": 50,
                        "max_postpone_ms": 50,
                        "max_retry_times": 3,
                        "max_inflight": 1
                    },
                    "http": {
                        "connect_timeout_ms": 1000,
                        "io_timeout_ms": 1000,
                        "max_retries": 10
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(executed.status.code, "shard_busy");
        assert_eq!(executed.queue_len, 1);
        let report = executed.scheduler_report.unwrap();
        assert_eq!(report.task_id, submitted_task.id);
        assert_eq!(report.result, SchedulerTaskResult::RetryLater);
        assert_eq!(report.retry_times, 1);
        assert_eq!(report.next_run_time_ms, Some(950));
        assert!(!report.aborted);
        assert_eq!(scheduler.snapshot().queue_len, 1);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler/executions".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let executions: MetaSchedulerExecutionsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(
            executions.executions[0].scheduler_result,
            SchedulerTaskResult::RetryLater
        );
        assert_eq!(executed.calls.len(), 3);
        assert_eq!(executed.calls[1].path, "/ServerService/Unload");
        assert_eq!(executed.calls[1].status.code, "shard_busy");
        assert_eq!(
            executed
                .lifecycle_state
                .as_ref()
                .unwrap()
                .last_status
                .as_ref()
                .unwrap()
                .code,
            "shard_busy"
        );

        let records = records.lock().unwrap();
        assert_eq!(records[0].0, "/ServerService/RequireLifecycleToken");
        assert_eq!(records[1].0, "/ServerService/Unload");
        assert_eq!(records[2].0, "/ServerService/GetLifecycle");
    }

    #[test]
    fn metaserver_scheduler_drives_raft_membership_apply() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let node_addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let server_records = Arc::clone(&records);
        let server_addr = node_addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/raft/membership/apply" => {
                        let apply: RaftMembershipApplyRequest =
                            serde_json::from_slice(&request.body).unwrap();
                        json_response(
                            200,
                            &RaftMembershipApplyResponse {
                                status: Status::ok(),
                                report: Some(RaftMembershipChangeReport {
                                    plan: temporalstore_rust::raft::RaftMembershipChangePlan {
                                        shard_id: 44,
                                        kind: temporalstore_rust::raft::RaftMembershipChangeKind::AddVoter,
                                        old_voters: vec![1, 2],
                                        new_voters: apply.voters.clone(),
                                        add_voters: vec![4],
                                        remove_voters: Vec::new(),
                                    },
                                    joint_membership:
                                        temporalstore_rust::raft::JointConsensusMembership {
                                            old_voters: vec![1, 2],
                                            new_voters: apply.voters.clone(),
                                        },
                                    committed_membership: temporalstore_rust::raft::RaftMembership {
                                        shard_id: 44,
                                        voters: apply.voters,
                                        leader_id: 1,
                                    },
                                    caught_up_voters: vec![1, 2, 4],
                                    leader_id: 1,
                                    commit_index: 12,
                                }),
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 900,
                    "kind": SchedulerTaskKind::UpdateMembership(MembershipUpdateTaskPlan {
                        shard_id: 44,
                        self_replica_id: 1,
                        active_replica_ids: vec![1, 2, 4],
                        primary_replica_id: 1,
                        membership_version: 3,
                        requests: Vec::new(),
                    }),
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);

        let executed = execute_scheduler_task_with_options(
            &backend,
            &scheduler,
            900,
            &node_addr,
            None,
            Some(TaskSchedulerOptions {
                base_postpone_ms: 50,
                max_postpone_ms: 50,
                max_retry_times: 3,
                max_inflight: 1,
            }),
            HttpRequestOptionsView {
                connect_timeout_ms: 1000,
                io_timeout_ms: 1000,
                max_retries: 10,
            },
        );
        assert!(executed.status.ok, "{executed:?}");
        assert_eq!(executed.queue_len, 0);
        assert_eq!(executed.calls.len(), 1);
        assert_eq!(executed.calls[0].path, "/raft/membership/apply");
        let report = executed.raft_membership_report.as_ref().unwrap();
        assert_eq!(report.committed_membership.voters, vec![1, 2, 4]);
        assert_eq!(report.caught_up_voters, vec![1, 2, 4]);
        assert_eq!(report.commit_index, 12);

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "/raft/membership/apply");
        assert_eq!(records[0].1["voters"], serde_json::json!([1, 2, 4]));
        let executions = scheduler.executions();
        assert_eq!(executions.executions.len(), 1);
        assert!(executions.executions[0].raft_membership_report.is_some());
        assert_eq!(
            executions.executions[0]
                .raft_membership_report
                .as_ref()
                .unwrap()
                .plan
                .new_voters,
            vec![1, 2, 4]
        );
    }

    #[test]
    fn metaserver_scheduler_drives_load_reload_unload_lifecycle_workflow() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_stateful_lifecycle_nodeserver();

        let load_task = submit_scheduler_task(
            &backend,
            &scheduler,
            100,
            RebalanceStep::LoadTarget {
                shard_id: 44,
                replica_id: 8,
                node_id: 9,
                load_version: 5,
            },
        );
        let load = execute_scheduler_task(
            &backend,
            &scheduler,
            100,
            &node_addr,
            Some(LoadShardRequest {
                shard_id: 44,
                load_version: 5,
                local_node_id: None,
                shard_uri: "memory://workflow/load".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: false,
                table_name: "workflow_table".to_string(),
            }),
        );
        assert!(load.status.ok, "{load:?}");
        assert_eq!(
            load.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        let load_state = load.lifecycle_state.as_ref().unwrap();
        assert_eq!(load_state.operation, "load");
        assert_eq!(load_state.state, "serving");
        assert_eq!(load_state.load_version, 5);
        assert_eq!(load_state.scheduler_task_id, Some(load_task.id));
        assert_eq!(load.queue_len, 0);

        let reload_task = submit_scheduler_task(
            &backend,
            &scheduler,
            200,
            RebalanceStep::ReloadTarget {
                shard_id: 44,
                replica_id: 8,
                node_id: 9,
                load_version: 6,
            },
        );
        let reload = execute_scheduler_task(
            &backend,
            &scheduler,
            200,
            &node_addr,
            Some(LoadShardRequest {
                shard_id: 44,
                load_version: 6,
                local_node_id: None,
                shard_uri: "memory://workflow/reload".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: true,
                table_name: "workflow_table".to_string(),
            }),
        );
        assert!(reload.status.ok, "{reload:?}");
        assert_eq!(
            reload.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        let reload_state = reload.lifecycle_state.as_ref().unwrap();
        assert_eq!(reload_state.operation, "reload");
        assert_eq!(reload_state.state, "readonly");
        assert_eq!(reload_state.load_version, 6);
        assert_eq!(reload_state.scheduler_task_id, Some(reload_task.id));
        assert_eq!(reload.queue_len, 0);

        let unload_task = submit_scheduler_task(
            &backend,
            &scheduler,
            300,
            RebalanceStep::UnloadSource {
                shard_id: 44,
                replica_id: 8,
                node_id: 9,
            },
        );
        let unload = execute_scheduler_task(&backend, &scheduler, 300, &node_addr, None);
        assert!(unload.status.ok, "{unload:?}");
        assert_eq!(
            unload.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        let unload_state = unload.lifecycle_state.as_ref().unwrap();
        assert_eq!(unload_state.operation, "unload");
        assert_eq!(unload_state.state, "unloaded");
        assert_eq!(unload_state.scheduler_task_id, Some(unload_task.id));
        assert_eq!(unload.queue_len, 0);

        let executions = scheduler.executions();
        assert_eq!(executions.executions.len(), 3);
        let operations = executions
            .executions
            .iter()
            .map(|record| record.lifecycle_state.as_ref().unwrap().operation.as_str())
            .collect::<Vec<_>>();
        assert_eq!(operations, vec!["load", "reload", "unload"]);

        let records = records.lock().unwrap();
        let paths = records
            .iter()
            .map(|record| record.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Load",
                "/ServerService/GetLifecycle",
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Reload",
                "/ServerService/GetLifecycle",
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Unload",
                "/ServerService/GetLifecycle",
            ]
        );
    }

    #[test]
    fn metaserver_scheduler_reload_survives_nodeserver_lifecycle_snapshot_restart() {
        let dir = tempdir().unwrap();
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (source_addr, source_records) = spawn_stateful_lifecycle_nodeserver();

        let load_task = submit_scheduler_task(
            &backend,
            &scheduler,
            100,
            RebalanceStep::LoadTarget {
                shard_id: 54,
                replica_id: 8,
                node_id: 9,
                load_version: 10,
            },
        );
        let load = execute_scheduler_task(
            &backend,
            &scheduler,
            100,
            &source_addr,
            Some(LoadShardRequest {
                shard_id: 54,
                load_version: 10,
                local_node_id: None,
                shard_uri: "memory://restart-harness/load".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: false,
                table_name: "restart_harness".to_string(),
            }),
        );
        assert!(load.status.ok, "{load:?}");
        assert_eq!(load.lifecycle_state.as_ref().unwrap().operation, "load");
        assert_eq!(
            load.lifecycle_state.as_ref().unwrap().scheduler_task_id,
            Some(load_task.id)
        );

        let snapshot_path = dir.path().join("node-lifecycle-snapshot.json");
        let save = post_json_with_options::<_, StatefulLifecycleSnapshotFileResponse>(
            &source_addr,
            "/ServerService/SaveLifecycleSnapshot",
            &StatefulLifecycleSnapshotFileRequest {
                path: snapshot_path.clone(),
            },
            HttpRequestOptions {
                connect_timeout_ms: 1000,
                io_timeout_ms: 1000,
                max_retries: 10,
            },
        )
        .unwrap();
        assert!(save.status.ok, "{save:?}");
        assert!(snapshot_path.exists());
        assert_eq!(save.snapshot.as_ref().unwrap().tokens.len(), 1);
        assert_eq!(
            save.snapshot.as_ref().unwrap().transitions[0].scheduler_task_id,
            Some(load_task.id)
        );

        let (restarted_addr, restarted_records) = spawn_stateful_lifecycle_nodeserver();
        let restore = post_json_with_options::<_, StatefulLifecycleSnapshotFileResponse>(
            &restarted_addr,
            "/ServerService/LoadLifecycleSnapshot",
            &StatefulLifecycleSnapshotFileRequest {
                path: snapshot_path.clone(),
            },
            HttpRequestOptions {
                connect_timeout_ms: 1000,
                io_timeout_ms: 1000,
                max_retries: 10,
            },
        )
        .unwrap();
        assert!(restore.status.ok, "{restore:?}");
        assert_eq!(
            restore.snapshot.as_ref().unwrap().transitions[0].shard_id,
            54
        );

        let reload_task = submit_scheduler_task(
            &backend,
            &scheduler,
            200,
            RebalanceStep::ReloadTarget {
                shard_id: 54,
                replica_id: 8,
                node_id: 9,
                load_version: 11,
            },
        );
        let reload = execute_scheduler_task(
            &backend,
            &scheduler,
            200,
            &restarted_addr,
            Some(LoadShardRequest {
                shard_id: 54,
                load_version: 11,
                local_node_id: None,
                shard_uri: "memory://restart-harness/reload".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: true,
                table_name: "restart_harness".to_string(),
            }),
        );
        assert!(reload.status.ok, "{reload:?}");
        assert_eq!(
            reload.scheduler_report.as_ref().unwrap().result,
            SchedulerTaskResult::Ok
        );
        let reload_state = reload.lifecycle_state.as_ref().unwrap();
        assert_eq!(reload_state.operation, "reload");
        assert_eq!(reload_state.state, "readonly");
        assert_eq!(reload_state.load_version, 11);
        assert_eq!(reload_state.scheduler_task_id, Some(reload_task.id));
        assert_eq!(
            reload_state.scheduler_generation,
            Some(reload.lifecycle_token.as_ref().unwrap().generation)
        );
        assert_eq!(reload.node_lifecycle.as_ref().unwrap().readonly_count, 1);

        let source_paths = source_records
            .lock()
            .unwrap()
            .iter()
            .map(|record| record.0.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            source_paths,
            vec![
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Load",
                "/ServerService/GetLifecycle",
                "/ServerService/SaveLifecycleSnapshot",
            ]
        );
        let restarted_paths = restarted_records
            .lock()
            .unwrap()
            .iter()
            .map(|record| record.0.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            restarted_paths,
            vec![
                "/ServerService/LoadLifecycleSnapshot",
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Reload",
                "/ServerService/GetLifecycle",
            ]
        );
    }

    #[test]
    fn raft_backed_metaserver_scheduler_drives_lifecycle_workflow() {
        let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
            forbid_self_clearing_conviction: false,
            snapshot_check_interval_ms: 0,
            engine: ProductionRaftEngineKind::TemporalRaft,
            local_node_id: 1,
            nodes: vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:18301".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:18302".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:18303".to_string(),
                },
            ],
            config: RaftConfig::default(),
            heartbeat_interval_ms: 100,
            election_tick_ms: 50,
            failure_detector_interval_ms: 1_000,
            stale_server_after_ms: 30_000,
        })
        .unwrap();
        let backend = MetaBackend::Raft(runtime);
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_stateful_lifecycle_nodeserver();

        let load_task = submit_scheduler_task(
            &backend,
            &scheduler,
            400,
            RebalanceStep::LoadTarget {
                shard_id: 45,
                replica_id: 8,
                node_id: 9,
                load_version: 7,
            },
        );
        let load = execute_scheduler_task(
            &backend,
            &scheduler,
            400,
            &node_addr,
            Some(LoadShardRequest {
                shard_id: 45,
                load_version: 7,
                local_node_id: None,
                shard_uri: "memory://raft-workflow/load".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: false,
                table_name: "raft_workflow_table".to_string(),
            }),
        );
        assert!(load.status.ok, "{load:?}");
        assert_eq!(
            load.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(load.lifecycle_state.as_ref().unwrap().operation, "load");
        assert_eq!(load.lifecycle_state.as_ref().unwrap().state, "serving");
        assert_eq!(
            load.lifecycle_state.as_ref().unwrap().scheduler_task_id,
            Some(load_task.id)
        );

        let reload_task = submit_scheduler_task(
            &backend,
            &scheduler,
            500,
            RebalanceStep::ReloadTarget {
                shard_id: 45,
                replica_id: 8,
                node_id: 9,
                load_version: 8,
            },
        );
        let reload = execute_scheduler_task(
            &backend,
            &scheduler,
            500,
            &node_addr,
            Some(LoadShardRequest {
                shard_id: 45,
                load_version: 8,
                local_node_id: None,
                shard_uri: "memory://raft-workflow/reload".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: true,
                table_name: "raft_workflow_table".to_string(),
            }),
        );
        assert!(reload.status.ok, "{reload:?}");
        assert_eq!(
            reload.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(reload.lifecycle_state.as_ref().unwrap().operation, "reload");
        assert_eq!(reload.lifecycle_state.as_ref().unwrap().state, "readonly");
        assert_eq!(
            reload.lifecycle_state.as_ref().unwrap().scheduler_task_id,
            Some(reload_task.id)
        );

        let unload_task = submit_scheduler_task(
            &backend,
            &scheduler,
            600,
            RebalanceStep::UnloadSource {
                shard_id: 45,
                replica_id: 8,
                node_id: 9,
            },
        );
        let unload = execute_scheduler_task(&backend, &scheduler, 600, &node_addr, None);
        assert!(unload.status.ok, "{unload:?}");
        assert_eq!(
            unload.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(unload.lifecycle_state.as_ref().unwrap().operation, "unload");
        assert_eq!(unload.lifecycle_state.as_ref().unwrap().state, "unloaded");
        assert_eq!(
            unload.lifecycle_state.as_ref().unwrap().scheduler_task_id,
            Some(unload_task.id)
        );

        let executions = scheduler.executions();
        assert_eq!(executions.executions.len(), 3);
        let operations = executions
            .executions
            .iter()
            .map(|record| record.lifecycle_state.as_ref().unwrap().operation.as_str())
            .collect::<Vec<_>>();
        assert_eq!(operations, vec!["load", "reload", "unload"]);
        assert_eq!(scheduler.snapshot().queue_len, 0);

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 9);
        assert_eq!(records[0].0, "/ServerService/RequireLifecycleToken");
        assert_eq!(records[1].0, "/ServerService/Load");
        assert_eq!(records[4].0, "/ServerService/Reload");
        assert_eq!(records[7].0, "/ServerService/Unload");
    }

    #[test]
    fn metaserver_scheduler_retries_when_nodeserver_disappears_during_lifecycle() {
        for case in disappeared_node_lifecycle_cases() {
            let backend = MetaBackend::Single(SingleNodeMeta::default());
            let scheduler = MetaTaskScheduler::default();
            let (task, executed) = execute_missing_node_lifecycle_case(
                &backend,
                &scheduler,
                700,
                case.step,
                case.load_request,
            );
            assert_missing_node_retry(&scheduler, &task, &executed, case.operation, 775);
        }
    }

    #[test]
    fn metaserver_scheduler_persists_disappeared_node_retry_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing-node-retry-scheduler.json");
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::with_snapshot_path(path.clone()).unwrap();
        let case = disappeared_node_lifecycle_cases()
            .into_iter()
            .find(|case| case.operation == "reload")
            .unwrap();
        let (task, executed) = execute_missing_node_lifecycle_case(
            &backend,
            &scheduler,
            800,
            case.step,
            case.load_request,
        );
        assert_missing_node_retry(&scheduler, &task, &executed, "reload", 875);
        assert!(path.exists());

        let restored = MetaTaskScheduler::with_snapshot_path(path).unwrap();
        let snapshot = restored.snapshot();
        assert_eq!(snapshot.queue_len, 1);
        let restored_task = snapshot.snapshot.unwrap().tasks[0].clone();
        assert_eq!(restored_task.id, task.id);
        assert_eq!(restored_task.retry_times, 1);
        assert_eq!(restored_task.next_run_time_ms, 875);
        assert_eq!(restored.executions().executions.len(), 1);
        let record = &restored.executions().executions[0];
        assert_eq!(record.task_id, task.id);
        assert_eq!(record.scheduler_result, SchedulerTaskResult::RetryLater);
        assert_eq!(record.next_run_time_ms, Some(875));
        assert_eq!(record.status.code, "node_request_failed");
        assert!(record.lifecycle_state.is_none());
    }

    #[test]
    fn metaserver_scheduler_persists_snapshot_file_after_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler.json");
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::with_snapshot_path(path.clone()).unwrap();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
            shard_id: 12,
            replica_id: 21,
            node_id: 5,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&MetaSchedulerSubmitRequest {
                    priority: 4,
                    now_ms: 200,
                    kind: task_kind,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        assert!(path.exists());

        let restored_scheduler = MetaTaskScheduler::with_snapshot_path(path).unwrap();
        let (code, body) = handle(
            &backend,
            &restored_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/run_next".to_string(),
                body: serde_json::to_vec(&MetaSchedulerRunRequest {
                    now_ms: 200,
                    result: SchedulerTaskResult::Ok,
                    options: None,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let run: MetaSchedulerRunResponse = serde_json::from_slice(&body).unwrap();
        assert!(run.status.ok);
        assert_eq!(run.report.unwrap().task_id, submitted.task.unwrap().id);
        assert_eq!(run.queue_len, 0);
    }

    #[test]
    fn metaserver_scheduler_restores_execution_tokens_from_snapshot_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler-executions.json");
        let scheduler = MetaTaskScheduler::with_snapshot_path(path.clone()).unwrap();
        scheduler.record_execution(MetaSchedulerExecutionRecord {
            task_id: 12,
            node_addr: "node-a".to_string(),
            status: Status::ok(),
            scheduler_result: SchedulerTaskResult::Ok,
            retry_times: 0,
            next_run_time_ms: None,
            calls: Vec::new(),
            lifecycle_token: Some(SchedulerLifecycleToken {
                task_id: 12,
                shard_id: 44,
                operation: "load".to_string(),
                load_version: 5,
                generation: 900,
            }),
            lifecycle_state: Some(DataNodeShardLifecycleState {
                shard_id: 44,
                state: "serving".to_string(),
                operation: "load".to_string(),
                load_version: 5,
                updated_at_ms: 901,
                last_status: Some(Status::ok()),
                scheduler_task_id: Some(12),
                scheduler_generation: Some(900),
            }),
            raft_membership_report: None,
            queue_len: 0,
        });
        assert!(path.exists());

        let restored = MetaTaskScheduler::with_snapshot_path(path).unwrap();
        let executions = restored.executions();
        assert!(executions.status.ok);
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(executions.executions[0].task_id, 12);
        assert!(restored
            .validate_finish_load(&LoadFinishRequest {
                server_addr: "node-a".to_string(),
                shard_id: 44,
                load_version: 5,
                status: Status::ok(),
                scheduler_task_id: Some(12),
                scheduler_generation: Some(900),
            })
            .is_ok());

        let stale = restored
            .validate_finish_load(&LoadFinishRequest {
                server_addr: "node-a".to_string(),
                shard_id: 44,
                load_version: 5,
                status: Status::ok(),
                scheduler_task_id: Some(12),
                scheduler_generation: Some(899),
            })
            .unwrap_err();
        assert_eq!(stale.code, "scheduler_finish_load_not_found");
    }

    #[test]
    fn metaserver_scheduler_loads_legacy_task_only_snapshot_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-scheduler.json");
        let mut scheduler = DeterministicTaskScheduler::default();
        let task = scheduler.submit(
            4,
            200,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
                shard_id: 12,
                replica_id: 21,
                node_id: 5,
            }),
        );
        fs::write(
            &path,
            serde_json::to_vec_pretty(&scheduler.export_snapshot()).unwrap(),
        )
        .unwrap();

        let restored = MetaTaskScheduler::with_snapshot_path(path).unwrap();
        assert_eq!(restored.executions().executions.len(), 0);
        assert_eq!(restored.queue_len(), 1);
        assert_eq!(restored.peek_next(200).unwrap().id, task.id);
    }

    fn spawn_recording_nodeserver() -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let server_records = Arc::clone(&records);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => json_response(200, &Status::ok()),
                    "/ServerService/Load" => json_response(
                        200,
                        &LoadShardResponse {
                            status: Status::ok(),
                        },
                    ),
                    "/ServerService/Unload" => json_response(
                        200,
                        &UnloadShardResponse {
                            status: Status::ok(),
                        },
                    ),
                    "/ServerService/GetLifecycle" => json_response(
                        200,
                        &DataNodeLifecycleReport {
                            loaded_shard_count: 1,
                            serving_count: 1,
                            readonly_count: 0,
                            queued_count: 0,
                            running_count: 0,
                            unloading_count: 0,
                            failed_count: 0,
                            max_load_version: 5,
                            shards: Vec::new(),
                            transitions: vec![DataNodeShardLifecycleState {
                                shard_id: 44,
                                state: "serving".to_string(),
                                operation: "load".to_string(),
                                load_version: 5,
                                updated_at_ms: 901,
                                last_status: Some(Status::ok()),
                                scheduler_task_id: Some(0),
                                scheduler_generation: Some(900),
                            }],
                            ..DataNodeLifecycleReport::default()
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    fn spawn_recording_reload_nodeserver() -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>)
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let server_records = Arc::clone(&records);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => json_response(200, &Status::ok()),
                    "/ServerService/Reload" => json_response(
                        200,
                        &LoadShardResponse {
                            status: Status::ok(),
                        },
                    ),
                    "/ServerService/GetLifecycle" => json_response(
                        200,
                        &DataNodeLifecycleReport {
                            loaded_shard_count: 1,
                            serving_count: 0,
                            readonly_count: 1,
                            queued_count: 0,
                            running_count: 0,
                            unloading_count: 0,
                            failed_count: 0,
                            max_load_version: 6,
                            shards: Vec::new(),
                            transitions: vec![DataNodeShardLifecycleState {
                                shard_id: 44,
                                state: "readonly".to_string(),
                                operation: "reload".to_string(),
                                load_version: 6,
                                updated_at_ms: 902,
                                last_status: Some(Status::ok()),
                                scheduler_task_id: Some(0),
                                scheduler_generation: Some(901),
                            }],
                            ..DataNodeLifecycleReport::default()
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    fn spawn_busy_unload_nodeserver() -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let server_records = Arc::clone(&records);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => json_response(200, &Status::ok()),
                    "/ServerService/Unload" => json_response(
                        200,
                        &UnloadShardResponse {
                            status: Status::error(
                                "shard_busy",
                                "cannot unload while shard work is queued or running",
                            ),
                        },
                    ),
                    "/ServerService/GetLifecycle" => json_response(
                        200,
                        &DataNodeLifecycleReport {
                            loaded_shard_count: 1,
                            serving_count: 0,
                            readonly_count: 0,
                            queued_count: 1,
                            running_count: 0,
                            unloading_count: 0,
                            failed_count: 1,
                            max_load_version: 0,
                            shards: Vec::new(),
                            transitions: vec![DataNodeShardLifecycleState {
                                shard_id: 44,
                                state: "failed".to_string(),
                                operation: "unload".to_string(),
                                load_version: 0,
                                updated_at_ms: 901,
                                last_status: Some(Status::error(
                                    "shard_busy",
                                    "cannot unload while shard work is queued or running",
                                )),
                                scheduler_task_id: Some(0),
                                scheduler_generation: Some(900),
                            }],
                            ..DataNodeLifecycleReport::default()
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    fn spawn_stateful_lifecycle_nodeserver(
    ) -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let lifecycle = Arc::new(Mutex::new(StatefulLifecycleNode::default()));
        let server_records = Arc::clone(&records);
        let server_lifecycle = Arc::clone(&lifecycle);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => {
                        let token: SchedulerLifecycleToken =
                            serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle.lock().unwrap().token = Some(token);
                        json_response(200, &Status::ok())
                    }
                    "/ServerService/Load" => {
                        let load: LoadShardRequest = serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle
                            .lock()
                            .unwrap()
                            .transition_from_load("load", "serving", &load);
                        json_response(
                            200,
                            &LoadShardResponse {
                                status: Status::ok(),
                            },
                        )
                    }
                    "/ServerService/Reload" => {
                        let load: LoadShardRequest = serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle
                            .lock()
                            .unwrap()
                            .transition_from_load("reload", "readonly", &load);
                        json_response(
                            200,
                            &LoadShardResponse {
                                status: Status::ok(),
                            },
                        )
                    }
                    "/ServerService/Unload" => {
                        let unload: UnloadShardRequest =
                            serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle
                            .lock()
                            .unwrap()
                            .transition_from_unload(&unload);
                        json_response(
                            200,
                            &UnloadShardResponse {
                                status: Status::ok(),
                            },
                        )
                    }
                    "/ServerService/GetLifecycle" => {
                        let report = server_lifecycle.lock().unwrap().report();
                        json_response(200, &report)
                    }
                    "/ServerService/SaveLifecycleSnapshot" => {
                        let request: StatefulLifecycleSnapshotFileRequest =
                            serde_json::from_slice(&request.body).unwrap();
                        let response = server_lifecycle
                            .lock()
                            .unwrap()
                            .save_snapshot_file(request.path);
                        json_response(200, &response)
                    }
                    "/ServerService/LoadLifecycleSnapshot" => {
                        let request: StatefulLifecycleSnapshotFileRequest =
                            serde_json::from_slice(&request.body).unwrap();
                        let response = server_lifecycle
                            .lock()
                            .unwrap()
                            .load_snapshot_file(request.path);
                        json_response(200, &response)
                    }
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct StatefulLifecycleSnapshotFileRequest {
        path: PathBuf,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct StatefulLifecycleSnapshotFileResponse {
        status: Status,
        #[serde(default)]
        snapshot: Option<DataNodeLifecycleSnapshot>,
    }

    #[derive(Debug, Default)]
    struct StatefulLifecycleNode {
        token: Option<SchedulerLifecycleToken>,
        transition: Option<DataNodeShardLifecycleState>,
        loaded: bool,
        readonly: bool,
        load_version: u64,
    }

    impl StatefulLifecycleNode {
        fn transition_from_load(&mut self, operation: &str, state: &str, load: &LoadShardRequest) {
            self.loaded = true;
            self.readonly = load.readonly;
            self.load_version = load.load_version;
            self.transition =
                Some(self.transition_state(load.shard_id, state, operation, load.load_version));
        }

        fn transition_from_unload(&mut self, unload: &UnloadShardRequest) {
            self.loaded = false;
            self.readonly = false;
            self.transition = Some(self.transition_state(
                unload.shard_id,
                "unloaded",
                "unload",
                self.load_version,
            ));
        }

        fn transition_state(
            &self,
            shard_id: u64,
            state: &str,
            operation: &str,
            load_version: u64,
        ) -> DataNodeShardLifecycleState {
            DataNodeShardLifecycleState {
                shard_id,
                state: state.to_string(),
                operation: operation.to_string(),
                load_version,
                updated_at_ms: 1_000 + load_version,
                last_status: Some(Status::ok()),
                scheduler_task_id: self.token.as_ref().map(|token| token.task_id),
                scheduler_generation: self.token.as_ref().map(|token| token.generation),
            }
        }

        fn report(&self) -> DataNodeLifecycleReport {
            DataNodeLifecycleReport {
                loaded_shard_count: usize::from(self.loaded),
                serving_count: usize::from(self.loaded && !self.readonly),
                readonly_count: usize::from(self.loaded && self.readonly),
                queued_count: 0,
                running_count: 0,
                unloading_count: 0,
                failed_count: 0,
                max_load_version: self.load_version,
                shards: Vec::new(),
                transitions: self.transition.clone().into_iter().collect(),
                ..DataNodeLifecycleReport::default()
            }
        }

        fn snapshot(&self) -> DataNodeLifecycleSnapshot {
            DataNodeLifecycleSnapshot {
                format_version: 1,
                transitions: self.transition.clone().into_iter().collect(),
                tokens: self.token.clone().into_iter().collect(),
            }
        }

        fn restore_snapshot(&mut self, snapshot: DataNodeLifecycleSnapshot) -> Status {
            if snapshot.format_version != 1 {
                return Status::error(
                    "bad_lifecycle_snapshot",
                    "unsupported data node lifecycle snapshot version",
                );
            }
            self.token = snapshot.tokens.into_iter().next();
            self.transition = snapshot.transitions.into_iter().next();
            self.loaded = self
                .transition
                .as_ref()
                .map(|state| state.state != "unloaded")
                .unwrap_or(false);
            self.readonly = self
                .transition
                .as_ref()
                .map(|state| state.state == "readonly")
                .unwrap_or(false);
            self.load_version = self
                .transition
                .as_ref()
                .map(|state| state.load_version)
                .unwrap_or_default();
            Status::ok()
        }

        fn save_snapshot_file(&self, path: PathBuf) -> StatefulLifecycleSnapshotFileResponse {
            let snapshot = self.snapshot();
            if let Some(parent) = path.parent() {
                if let Err(err) = fs::create_dir_all(parent) {
                    return StatefulLifecycleSnapshotFileResponse {
                        status: Status::error("lifecycle_snapshot_io", err.to_string()),
                        snapshot: None,
                    };
                }
            }
            match serde_json::to_vec_pretty(&snapshot)
                .map_err(|err| err.to_string())
                .and_then(|bytes| fs::write(&path, bytes).map_err(|err| err.to_string()))
            {
                Ok(()) => StatefulLifecycleSnapshotFileResponse {
                    status: Status::ok(),
                    snapshot: Some(snapshot),
                },
                Err(err) => StatefulLifecycleSnapshotFileResponse {
                    status: Status::error("lifecycle_snapshot_io", err),
                    snapshot: None,
                },
            }
        }

        fn load_snapshot_file(&mut self, path: PathBuf) -> StatefulLifecycleSnapshotFileResponse {
            let snapshot = match fs::read(&path)
                .map_err(|err| err.to_string())
                .and_then(|bytes| {
                    serde_json::from_slice::<DataNodeLifecycleSnapshot>(&bytes)
                        .map_err(|err| err.to_string())
                }) {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    return StatefulLifecycleSnapshotFileResponse {
                        status: Status::error("lifecycle_snapshot_io", err),
                        snapshot: None,
                    };
                }
            };
            let status = self.restore_snapshot(snapshot.clone());
            StatefulLifecycleSnapshotFileResponse {
                snapshot: status.ok.then_some(snapshot),
                status,
            }
        }
    }

    fn submit_scheduler_task(
        backend: &MetaBackend,
        scheduler: &MetaTaskScheduler,
        now_ms: u64,
        step: RebalanceStep,
    ) -> SchedulerTask {
        let (code, body) = handle(
            backend,
            scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": now_ms,
                    "kind": SchedulerTaskKind::RebalanceStep(step),
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok, "{submitted:?}");
        submitted.task.unwrap()
    }

    fn execute_scheduler_task(
        backend: &MetaBackend,
        scheduler: &MetaTaskScheduler,
        now_ms: u64,
        node_addr: &str,
        load_request: Option<LoadShardRequest>,
    ) -> MetaSchedulerExecuteResponse {
        execute_scheduler_task_with_options(
            backend,
            scheduler,
            now_ms,
            node_addr,
            load_request,
            None,
            HttpRequestOptionsView {
                connect_timeout_ms: 1000,
                io_timeout_ms: 1000,
                max_retries: 10,
            },
        )
    }

    fn execute_scheduler_task_with_options(
        backend: &MetaBackend,
        scheduler: &MetaTaskScheduler,
        now_ms: u64,
        node_addr: &str,
        load_request: Option<LoadShardRequest>,
        options: Option<TaskSchedulerOptions>,
        http: HttpRequestOptionsView,
    ) -> MetaSchedulerExecuteResponse {
        let (code, body) = handle(
            backend,
            scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": now_ms,
                    "node_addr": node_addr,
                    "load_request": load_request,
                    "options": options,
                    "http": http
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        serde_json::from_slice(&body).unwrap()
    }

    #[derive(Debug)]
    struct MissingNodeLifecycleCase {
        operation: &'static str,
        step: RebalanceStep,
        load_request: Option<LoadShardRequest>,
    }

    fn disappeared_node_lifecycle_cases() -> Vec<MissingNodeLifecycleCase> {
        vec![
            MissingNodeLifecycleCase {
                operation: "load",
                step: RebalanceStep::LoadTarget {
                    shard_id: 46,
                    replica_id: 8,
                    node_id: 9,
                    load_version: 10,
                },
                load_request: Some(LoadShardRequest {
                    shard_id: 46,
                    load_version: 10,
                    local_node_id: None,
                    shard_uri: "memory://missing-node/load".to_string(),
                    start_routing_bucket: 0,
                    end_routing_bucket: 1023,
                    readonly: false,
                    table_name: "missing_node_table".to_string(),
                }),
            },
            MissingNodeLifecycleCase {
                operation: "reload",
                step: RebalanceStep::ReloadTarget {
                    shard_id: 47,
                    replica_id: 8,
                    node_id: 9,
                    load_version: 11,
                },
                load_request: Some(LoadShardRequest {
                    shard_id: 47,
                    load_version: 11,
                    local_node_id: None,
                    shard_uri: "memory://missing-node/reload".to_string(),
                    start_routing_bucket: 0,
                    end_routing_bucket: 1023,
                    readonly: true,
                    table_name: "missing_node_table".to_string(),
                }),
            },
            MissingNodeLifecycleCase {
                operation: "unload",
                step: RebalanceStep::UnloadSource {
                    shard_id: 48,
                    replica_id: 8,
                    node_id: 9,
                },
                load_request: None,
            },
        ]
    }

    fn execute_missing_node_lifecycle_case(
        backend: &MetaBackend,
        scheduler: &MetaTaskScheduler,
        now_ms: u64,
        step: RebalanceStep,
        load_request: Option<LoadShardRequest>,
    ) -> (SchedulerTask, MetaSchedulerExecuteResponse) {
        let task = submit_scheduler_task(backend, scheduler, now_ms, step);
        let missing_node_addr = reserve_unused_loopback_addr();
        let executed = execute_scheduler_task_with_options(
            backend,
            scheduler,
            now_ms,
            &missing_node_addr,
            load_request,
            Some(TaskSchedulerOptions {
                base_postpone_ms: 75,
                max_postpone_ms: 75,
                max_retry_times: 3,
                max_inflight: 1,
            }),
            HttpRequestOptionsView {
                connect_timeout_ms: 10,
                io_timeout_ms: 10,
                max_retries: 0,
            },
        );
        (task, executed)
    }

    fn assert_missing_node_retry(
        scheduler: &MetaTaskScheduler,
        task: &SchedulerTask,
        executed: &MetaSchedulerExecuteResponse,
        operation: &str,
        next_run_time_ms: u64,
    ) {
        assert_eq!(executed.status.code, "node_request_failed");
        assert_eq!(executed.queue_len, 1);
        assert_eq!(
            executed.lifecycle_token.as_ref().unwrap().operation,
            operation
        );
        assert_eq!(executed.calls.len(), 2);
        assert_eq!(
            executed.calls[0].path,
            "/ServerService/RequireLifecycleToken"
        );
        assert_eq!(executed.calls[0].status.code, "node_request_failed");
        assert_eq!(executed.calls[1].path, "/ServerService/GetLifecycle");
        assert_eq!(executed.calls[1].status.code, "node_lifecycle_fetch_failed");
        assert!(executed.node_lifecycle.is_none());
        assert!(executed.lifecycle_state.is_none());
        let report = executed.scheduler_report.as_ref().unwrap();
        assert_eq!(report.task_id, task.id);
        assert_eq!(report.result, SchedulerTaskResult::RetryLater);
        assert_eq!(report.retry_times, 1);
        assert_eq!(report.next_run_time_ms, Some(next_run_time_ms));
        assert!(!report.aborted);
        assert_eq!(scheduler.snapshot().queue_len, 1);

        let executions = scheduler.executions();
        assert_eq!(executions.executions.len(), 1);
        let record = &executions.executions[0];
        assert_eq!(record.task_id, task.id);
        assert_eq!(record.scheduler_result, SchedulerTaskResult::RetryLater);
        assert_eq!(record.retry_times, 1);
        assert_eq!(record.next_run_time_ms, Some(next_run_time_ms));
        assert_eq!(record.status.code, "node_request_failed");
        assert!(record.lifecycle_state.is_none());
    }

    fn reserve_unused_loopback_addr() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        addr
    }

    fn head_with(path: &str, bearer_token: Option<&str>) -> RequestHead {
        RequestHead {
            method: "POST".to_string(),
            path: path.to_string(),
            content_length: 0,
            keep_alive: false,
            blob_peer_fetch_loop_guard: false,
            bearer_token: bearer_token.map(str::to_string),
        }
    }

    #[test]
    fn without_a_configured_token_every_route_stays_open() {
        assert!(admin_request_allowed(None, &head_with("/meta/mute", None)));
        assert!(admin_request_allowed(None, &head_with("/servers/register", None)));
        assert!(admin_request_allowed(None, &head_with("/health", None)));
    }

    #[test]
    fn a_configured_token_gates_every_route_except_the_probes() {
        let required = Some("s3cret");
        // Probes stay open: a load balancer cannot attach a bearer token.
        for probe in ["/health", "/readiness", "/metrics", "/MasterService/Metrics"] {
            assert!(
                admin_request_allowed(required, &head_with(probe, None)),
                "{probe} must stay open"
            );
        }
        // Everything else needs the exact token.
        for route in ["/meta/mute", "/meta/raft/remove_node", "/meta/snapshot/restore", "/shards"] {
            assert!(
                !admin_request_allowed(required, &head_with(route, None)),
                "{route} must be denied without a token"
            );
            assert!(
                !admin_request_allowed(required, &head_with(route, Some("wrong"))),
                "{route} must be denied with the wrong token"
            );
            assert!(
                admin_request_allowed(required, &head_with(route, Some("s3cret"))),
                "{route} must be allowed with the right token"
            );
        }
    }

    #[test]
    fn the_serve_gate_answers_401_before_the_handler_and_still_serves_health() {
        let addr = reserve_unused_loopback_addr();
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            let _ = serve_with_stream_handler(
                &server_addr,
                |head: &RequestHead, transfer: &mut StreamTransfer| {
                    admin_auth_gate(Some("s3cret"), head, transfer)
                },
                |_request| json_response(200, &Status::ok()),
            );
        });
        let options = HttpRequestOptions {
            connect_timeout_ms: 1000,
            io_timeout_ms: 1000,
            max_retries: 10,
        };
        // The probe is served without a credential.
        let health: Status =
            get_json_with_options(&addr, "/health", options.clone()).expect("health while gated");
        assert!(health.ok);
        // A gated route without the token is refused at the head stage.
        let denied = get_json_with_options::<Status>(&addr, "/meta/info", options.clone());
        match denied {
            Ok(status) => assert!(!status.ok, "an unauthenticated request must not reach the handler"),
            Err(_) => {} // non-200 surfaces as an HttpError; either shape is a refusal
        }
        // The same route with the token reaches the handler.
        let allowed: Status = temporalstore_rust::http::get_json_with_options_and_headers(
            &addr,
            "/meta/info",
            "Authorization: Bearer s3cret\r\n",
            options,
        )
        .expect("authorized request should reach the handler");
        assert!(allowed.ok);
    }
}
