// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::control::{ShardStatInfo, ShardCanonicalStorageStats};
use crate::types::{ShardId, Status};

/// TS_META_ADMIN_TOKEN: shared secret for the metaserver's HTTP surface.
///
/// When the metaserver starts with this set, every route except the liveness
/// probes (`/health`, `/readiness`, `/metrics` and its alias) requires
/// `Authorization: Bearer <token>`; when unset, the surface stays open, which
/// is the previous behavior. The same variable is what the metaserver's
/// clients (proxy, datanode, SDK topology sync) read to attach the credential,
/// so one value configures a whole deployment.
pub fn admin_auth_token() -> Option<String> {
    std::env::var("TS_META_ADMIN_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
}

/// The ready-to-append header line for metaserver-bound requests, from
/// TS_META_ADMIN_TOKEN. Empty when unset, so callers can attach it
/// unconditionally.
pub fn admin_auth_header() -> String {
    admin_auth_token()
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default()
}
mod partitioning;
mod lifecycle;
mod snapshot_ops;
mod node_reports;
mod state_setters;
mod registration;
mod table_ops;
mod topology_helpers;
mod auto_rebalance;
mod failure_detector;
mod location;
mod placement_rebalance;
mod raft_failover;
mod shard_check;
mod retention;
mod proxy_groups;
mod subsystem_metrics;
mod event_bus;
use self::partitioning::*;
use self::topology_helpers::*;
pub use self::auto_rebalance::{
    compute_auto_rebalance, AutoRebalanceOptions, ShardReassignment, ShardReassignmentReason,
};
pub use self::failure_detector::{
    plan_conviction, AdaptiveConvictionReport, ConvictionCandidate, ConvictionPlan,
    ConvictionPolicy, DamageSeverity, Diagnosis, FailureDetectorOptions, LocationDamage,
    MetaFailureDetector,
};
pub use self::placement_rebalance::{
    compute_placement_aware_rebalance, PlacementTarget, ShardPlacement,
};
pub use self::location::{separated_from, separation_ladder, Location};
pub use self::raft_failover::{compute_raft_failover_triggers, RaftFailoverTrigger};
pub use self::shard_check::{
    ShardCheckOptions, ShardCheckReport, ShardChecker, ShardDivergence,
};
pub use self::proxy_groups::{
    plan_proxy_calibration, DropProxyGroupRequest, ListProxyGroupsResponse, ProxyAttachment,
    ProxyCalibrationOptions, ProxyCalibrationPlan, ProxyCalibrationReport, ProxyGroupInfo,
    ProxyGroupShortfall, PutProxyGroupRequest,
};
pub use self::subsystem_metrics::{SubsystemMetrics, TIER_PROXY, TIER_SERVER};
pub use self::event_bus::{TopologyNotice, TopologySubscription, SUBSCRIBER_QUEUE_DEPTH};
pub use self::retention::{
    plan_freeze_aging, plan_meta_retention, FreezeAgingOptions, FreezeAgingPlan,
    FreezeAgingReport, MetaRetentionOptions, MetaRetentionPlan, MetaRetentionReport,
    RetentionCandidate,
};

/// Why a resource was frozen.
///
/// Freezing recorded no reason at all, so the metaserver could not tell an
/// operator taking a node out for maintenance from the failure detector
/// convicting one that stopped answering — even though the two want opposite
/// recovery behaviour. A maintenance freeze ends when the operator says so; a
/// conviction should not end merely because the convicted node asked.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FreezeReason {
    /// No reason was recorded. Either the resource is not frozen, or it was
    /// frozen before reasons existed.
    #[default]
    Unspecified,
    /// An operator froze it explicitly, typically for maintenance.
    Operator,
    /// The failure detector convicted it for going silent.
    Unresponsive,
    /// The metaserver observed it restart in place, so it no longer holds what
    /// the metaserver believes it holds.
    Restarted,
    /// It announced its own shutdown, so it was taken out of service before the
    /// failure detector could notice the silence.
    Stopping,
}

impl FreezeReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Operator => "operator",
            Self::Unresponsive => "unresponsive",
            Self::Restarted => "restarted",
            Self::Stopping => "stopping",
        }
    }

    /// True when the metaserver, not an operator, decided this resource was
    /// unhealthy. These are the freezes a resource must not clear for itself.
    pub fn is_conviction(self) -> bool {
        // `Stopping` is deliberately absent. A node that announced its own
        // shutdown is expected back, and locking it out of re-registration
        // would turn every clean restart into an operator ticket.
        matches!(self, Self::Unresponsive | Self::Restarted)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetaEntityState {
    Normal,
    Frozen,
    Dropped,
}

impl Default for MetaEntityState {
    fn default() -> Self {
        Self::Normal
    }
}

impl MetaEntityState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Frozen => "frozen",
            Self::Dropped => "dropped",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLocation {
    pub shard_id: ShardId,
    pub server_addr: String,
    /// Whether this shard is being served.
    ///
    /// There was previously no way to take one shard out of service: the only
    /// lever was freezing its whole table, which stops every other shard in it
    /// too. A `Frozen` shard keeps its owner entry so it can be returned to
    /// service, but stops being routed to, and the planners leave it alone --
    /// an operator who froze a shard does not want rebalancing to move it or
    /// the divergence check to "repair" it.
    ///
    /// Defaults to `Normal`, so shard entries written before this existed load
    /// as serving.
    #[serde(default)]
    pub state: MetaEntityState,
    #[serde(default)]
    pub latest_snapshot: Option<ShardSnapshotRef>,
<<<<<<< HEAD
    /// When this shard was first registered.
    ///
    /// `last_heartbeat_ms` is reset every time it registers again, and
    /// `boot_time_ms` is when its process started -- neither answers how long
    /// it has been part of the cluster. Registering again keeps the original:
    /// a node that restarted has not newly joined.
    #[serde(default)]
    pub registered_at_ms: u64,
||||||| a7277311
=======
    /// Where this shard in particular should live.
    ///
    /// A table can express a preferred location, and every shard in it inherits
    /// that. There was no way to say anything about one shard: a single hot
    /// shard that wants its own hardware, or one shard whose data has to stay
    /// somewhere its siblings need not, had to be expressed by pinning the
    /// whole table or not at all.
    ///
    /// Empty means "whatever the table says", which is what every existing
    /// shard means.
    #[serde(default)]
    pub preferred_location: String,
>>>>>>> matrixark/main
}

/// Take one shard out of service, or return it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStateRequest {
    pub shard_id: ShardId,
}

/// Where a single shard should live. An empty location releases the pin and
/// returns the shard to whatever its table prefers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardPinRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardRequest {
    pub shard_id: ShardId,
    pub server_addr: String,
    /// When the metaserver first saw this, stamped by the metaserver on the way
    /// in and carried here so a replayed registration keeps the original time
    /// rather than restamping itself to the moment of the replay. Anything a
    /// caller puts here is overwritten.
    #[serde(default)]
    pub registered_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardSnapshotRef {
    pub uri: String,
    pub checksum: String,
    pub byte_size: u64,
    pub last_log_index: u64,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishShardSnapshotRequest {
    pub shard_id: ShardId,
    pub snapshot: ShardSnapshotRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetShardResponse {
    pub status: Status,
    pub location: Option<ShardLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckResponse {
    pub status: Status,
}

/// One memory/CPU domain of a datanode's hardware.
///
/// A server is not uniform: it is some number of domains, each with its own
/// cores and its own memory. The metaserver had no way to know that, so it
/// could not tell a machine that is one large domain from a machine that is
/// four smaller ones -- which is the difference between one big placement
/// target and four that fail independently for memory pressure.
///
/// Recorded here, reported by `/servers`, and carried in snapshots. Placement
/// still treats a server as one target; this is what a placement that wanted to
/// be finer-grained would have to read.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NumaNode {
    /// Index within the reporting server, as the server numbers them.
    pub id: u64,
    /// The cores in this domain, in the form the server reports them
    /// (comma or space separated).
    #[serde(default)]
    pub cpu_list: String,
    #[serde(default)]
    pub memory_size_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterServerRequest {
    pub server_addr: String,
    #[serde(default)]
    pub node_id: u64,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub binary_version: String,
    /// The memory/CPU domains this server is made of. Empty from a server that
    /// does not report them, which is every server built before this existed.
    #[serde(default)]
    pub numa_nodes: Vec<NumaNode>,
    /// When the metaserver first saw this, stamped by the metaserver on the way
    /// in and carried here so a replayed registration keeps the original time
    /// rather than restamping itself to the moment of the replay. Anything a
    /// caller puts here is overwritten.
    #[serde(default)]
    pub registered_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerHeartbeatRequest {
    pub server_addr: String,
    #[serde(default)]
    pub boot_time_ms: u64,
    #[serde(default)]
    pub binary_version: String,
    #[serde(default)]
    pub shard_loads: Vec<ShardLoad>,
    #[serde(default)]
    #[serde(alias = "partition_loads")]
    pub shard_stat_loads: Vec<ShardStatLoad>,
    #[serde(default)]
    pub runtime_load: ServerRuntimeLoad,
    #[serde(default)]
    pub shard_states: Vec<ServerShardServingState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerHeartbeatResponse {
    pub status: Status,
    pub forbid_auto_register: bool,
    #[serde(default)]
    pub topology_version: u64,
    #[serde(default)]
    pub server_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLoad {
    pub shard_id: ShardId,
    pub key_count: u64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStatLoad {
    pub shard_id: ShardId,
    #[serde(alias = "partition_info")]
    pub shard_stat_info: ShardStatInfo,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerRuntimeLoad {
    pub queue_depth: usize,
    pub background_queue_depth: usize,
    pub queued_shard_count: usize,
    pub running_shard_count: usize,
    pub dirty_object_count: usize,
    pub dirty_shard_count: usize,
    pub rejected_total: u64,
    pub rejected_background_total: u64,
    pub timed_out_total: u64,
    pub canceled_total: u64,
    pub dump_runs: u64,
    pub compaction_runs: u64,
    pub gc_runs: u64,
    pub storage_lifecycle_runs: u64,
    #[serde(default)]
    pub last_meta_topology_version: u64,
    #[serde(default)]
    pub meta_heartbeat_consecutive_failures: u64,
    #[serde(default)]
    pub meta_forbid_auto_register: bool,
    #[serde(default)]
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerShardServingState {
    pub shard_id: ShardId,
    pub serving_state: String,
    pub worker_index: usize,
    pub worker_threads: usize,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub table_name: String,
    pub shard_uri: String,
    #[serde(rename = "start_routing_slot")]
    pub start_routing_bucket: u32,
    #[serde(rename = "end_routing_slot")]
    pub end_routing_bucket: u32,
    pub total_records: usize,
    pub storage_bytes: u64,
    pub cache_memory_bytes: u64,
    #[serde(default)]
    pub storage: ShardCanonicalStorageStats,
    #[serde(alias = "page_store_bytes_written")]
    pub block_store_bytes_written: u64,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub dirty_object_count: u64,
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerMetaInfo {
    pub server_addr: String,
    pub node_id: u64,
    pub location: String,
    /// The hardware shape this server declared when it registered.
    #[serde(default)]
    pub numa_nodes: Vec<NumaNode>,
    pub state: MetaEntityState,
    pub last_heartbeat_ms: u64,
    #[serde(default)]
    pub frozen_since_ms: u64,
    #[serde(default)]
    pub freeze_cooldown_until_ms: u64,
    /// Why this server was frozen, or `Unspecified` when it is not.
    #[serde(default)]
    pub freeze_reason: FreezeReason,
    pub boot_time_ms: u64,
    /// The boot time the metaserver anchored on for this server: the first
    /// non-zero boot time it heartbeated after registering. `boot_time_ms`
    /// tracks whatever the latest heartbeat claimed; this one does not move, so
    /// the two disagreeing is the signal that the process restarted.
    #[serde(default)]
    pub reported_boot_time_ms: u64,
    /// Set once the server heartbeats a boot time different from the anchored
    /// one, meaning the process restarted without re-registering. Sticky until
    /// the server registers again, because a restarted datanode has lost the
    /// shards the metaserver still believes it is serving.
    #[serde(default)]
    pub reboot_detected: bool,
    /// Set once this server has been seen reporting `shard_states` at all.
    /// Until then an empty report is indistinguishable from an old build that
    /// does not send them, so the shard-divergence check declines to judge it.
    #[serde(default)]
    pub reports_shard_states: bool,
    /// When this server was first registered.
    ///
    /// `last_heartbeat_ms` is reset every time it registers again, and
    /// `boot_time_ms` is when its process started -- neither answers how long
    /// it has been part of the cluster. Registering again keeps the original:
    /// a node that restarted has not newly joined.
    #[serde(default)]
    pub registered_at_ms: u64,
    /// What `shard_loads` and `shard_states` add up to, summarised when the
    /// server reports them.
    ///
    /// Placement needs three numbers out of those two lists: the total keys,
    /// the total memory, and the worst serving state. It used to add them up
    /// from scratch inside every topology request -- walking every shard of
    /// every live server, twice for the loads and once for the states, to
    /// re-derive figures that only change when a heartbeat arrives. A hundred
    /// servers of fifty shards is fifteen thousand iterations per request, for
    /// an answer identical to the last request's.
    #[serde(default)]
    pub load_key_count: u64,
    #[serde(default)]
    pub load_memory_bytes: u64,
    #[serde(default)]
    pub worst_shard_state_penalty: u8,
    /// What this server's shards add up to, from the states it reports.
    ///
    /// Every heartbeat carries `total_records` and `storage_bytes` for each
    /// shard the server holds, and nothing read either. The metaserver knew how
    /// much data sat on every node and had no way to say so -- `/metrics`
    /// counted shards and never their size.
    ///
    /// Summed here rather than at scrape time, like the placement figures
    /// beside them: a scrape should not walk every shard of every server.
    #[serde(default)]
    pub reported_record_count: u64,
    #[serde(default)]
    pub reported_storage_bytes: u64,
    pub binary_version: String,
    pub shard_loads: Vec<ShardLoad>,
    #[serde(default)]
    #[serde(alias = "partition_loads")]
    pub shard_stat_loads: Vec<ShardStatLoad>,
    #[serde(default)]
    pub runtime_load: ServerRuntimeLoad,
    #[serde(default)]
    pub shard_states: Vec<ServerShardServingState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaleServerReport {
    pub status: Status,
    pub frozen_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaleResourceReport {
    pub status: Status,
    pub frozen_servers: Vec<String>,
    pub frozen_proxies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FreezeStaleServersRequest {
    pub stale_after_ms: u64,
    #[serde(default)]
    pub server_freeze_cooldown_ms: u64,
    #[serde(default)]
    pub proxy_freeze_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafeModePolicy {
    #[serde(default)]
    pub server_freeze_cooldown_ms: u64,
    #[serde(default)]
    pub proxy_freeze_cooldown_ms: u64,
}

/// Names held back from creation.
///
/// A name that is reserved cannot be used to create a namespace or a table.
/// Existing ones are left alone: reserving a name is a statement about what may
/// be created from now on, not a way to delete something already serving.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReservedNames {
    /// Namespaces nobody may create.
    #[serde(default)]
    pub namespaces: BTreeSet<String>,
    /// Table names nobody may create, in any namespace.
    #[serde(default)]
    pub tables: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReservedNamesResponse {
    pub status: Status,
    pub reserved: ReservedNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafeModeReport {
    pub status: Status,
    pub blocked_servers: Vec<String>,
    pub blocked_proxies: Vec<String>,
    pub server_count: usize,
    pub proxy_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterProxyRequest {
    pub proxy_addr: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub config_version: u64,
    #[serde(default)]
    pub binary_version: String,
    /// When the metaserver first saw this, stamped by the metaserver on the way
    /// in and carried here so a replayed registration keeps the original time
    /// rather than restamping itself to the moment of the replay. Anything a
    /// caller puts here is overwritten.
    #[serde(default)]
    pub registered_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHeartbeatRequest {
    pub proxy_addr: String,
    /// Milliseconds-since-epoch when this proxy process started. Lets the
    /// metaserver see a RESTART: the address is unchanged and the heartbeats
    /// never stop, so without this an in-place reboot is invisible and a proxy
    /// that came back with an empty route cache and a reset config looks
    /// identical to one that has been up for days. Datanodes already report it
    /// (`ServerHeartbeatRequest::boot_time_ms`); proxies were the gap.
    #[serde(default)]
    pub boot_time_ms: u64,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub config_version: u64,
    #[serde(default)]
    pub binary_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHeartbeatResponse {
    pub status: Status,
    pub config_changed: bool,
    pub namespace: String,
    pub config_version: u64,
    #[serde(default)]
    pub serving_mode: String,
    /// None means the metaserver has no opinion, which today is always: there is no
    /// per-proxy drop_percent in the meta model at all -- the one that exists is a TABLE
    /// serving option. As a bare `u8` this field could not say that, so it said 0, and the
    /// proxy applied it and lifted whatever drain an operator had put in force.
    #[serde(default)]
    pub drop_percent: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetaInfo {
    pub proxy_addr: String,
    pub namespace: String,
    /// The proxy group this proxy is attached to, or empty when it is idle.
    ///
    /// Assigned by the metaserver rather than declared by the proxy, so the
    /// cluster owns the assignment and can repair it. `namespace` above follows
    /// from the group while attached.
    #[serde(default)]
    pub group: String,
    pub location: String,
    pub state: MetaEntityState,
    pub config_version: u64,
    pub last_heartbeat_ms: u64,
    /// How many heartbeats this proxy has sent since it registered.
    ///
    /// Separate from `last_heartbeat_ms` because registration stamps that clock, so it cannot
    /// answer "has this proxy ever reported in" -- every registered proxy reads non-zero from
    /// the moment it appears. The staleness checks want the timestamp; the question of whether
    /// a proxy is capacity or merely a registration wants this.
    #[serde(default)]
    pub heartbeats_total: u64,
    #[serde(default)]
    pub frozen_since_ms: u64,
    #[serde(default)]
    pub freeze_cooldown_until_ms: u64,
    /// Why this proxy was frozen, or `Unspecified` when it is not.
    #[serde(default)]
    pub freeze_reason: FreezeReason,
    pub binary_version: String,
    /// Boot time last reported by this proxy; `0` when it has never reported one.
    /// When this proxy was first registered.
    ///
    /// `last_heartbeat_ms` is reset every time it registers again, and
    /// `boot_time_ms` is when its process started -- neither answers how long
    /// it has been part of the cluster. Registering again keeps the original:
    /// a node that restarted has not newly joined.
    #[serde(default)]
    pub registered_at_ms: u64,
    /// Mirrors `ServerMetaInfo::boot_time_ms`.
    #[serde(default)]
    pub boot_time_ms: u64,
    /// How many times this proxy has come back with a different boot time while
    /// still registered -- i.e. observed in-place restarts.
    #[serde(default)]
    pub restart_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddNamespaceRequest {
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceMetaInfo {
    pub namespace: String,
    pub table_count: usize,
    pub state: MetaEntityState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddTableRequest {
    pub namespace: String,
    pub table_name: String,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    #[serde(default = "default_replica_count")]
    pub replica_count: u64,
    #[serde(default)]
    pub partition_version: u32,
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteTableRequest {
    pub namespace: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTableRequest {
    pub namespace: String,
    pub table_name: String,
    #[serde(default)]
    pub shard_count: Option<u64>,
    #[serde(default)]
    pub replica_count: Option<u64>,
    #[serde(default)]
    pub first_shard_id: Option<ShardId>,
    #[serde(default)]
    pub partition_version: Option<u32>,
    #[serde(default)]
    pub serving_options: Option<TableServingOptionsPatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableServingOptions {
    #[serde(default = "default_pin_primary")]
    pub pin_primary: bool,
    #[serde(default = "default_replica_read_policy")]
    pub replica_read_policy: String,
    #[serde(default)]
    pub preferred_location: String,
    #[serde(default)]
    pub drop_percent: u8,
    #[serde(default = "default_max_read_retries")]
    pub max_read_retries: u32,
    #[serde(default)]
    pub max_write_retries: u32,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_continuous_failed_time_ms")]
    pub continuous_failed_time_ms: u64,
    #[serde(default = "default_table_io_timeout_ms")]
    pub io_timeout_ms: u64,
    #[serde(default = "default_table_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// The fields this table set for itself, by name.
    ///
    /// Which fields a table has spoken for is not recoverable from the values: the
    /// patch that carries them knows, and flattening it into this struct used to
    /// throw that away. Records written before this field carry none, and are read
    /// exactly as they were before -- see `table_decides`.
    #[serde(default)]
    pub set_fields: BTreeSet<String>,
}

/// A field of `TableServingOptions`, for naming one without spelling it.
///
/// The names go on the wire as plain strings, so a reader that meets a field it does
/// not know simply carries it; but every name a caller can ask for comes from here,
/// so a misspelling is a compile error rather than a silent "the table did not set
/// this".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableServingField {
    PinPrimary,
    ReplicaReadPolicy,
    PreferredLocation,
    DropPercent,
    MaxReadRetries,
    MaxWriteRetries,
    RetryBackoffMs,
    ContinuousFailedTimeMs,
    IoTimeoutMs,
    ConnectTimeoutMs,
}

impl TableServingField {
    pub fn name(self) -> &'static str {
        match self {
            Self::PinPrimary => "pin_primary",
            Self::ReplicaReadPolicy => "replica_read_policy",
            Self::PreferredLocation => "preferred_location",
            Self::DropPercent => "drop_percent",
            Self::MaxReadRetries => "max_read_retries",
            Self::MaxWriteRetries => "max_write_retries",
            Self::RetryBackoffMs => "retry_backoff_ms",
            Self::ContinuousFailedTimeMs => "continuous_failed_time_ms",
            Self::IoTimeoutMs => "io_timeout_ms",
            Self::ConnectTimeoutMs => "connect_timeout_ms",
        }
    }
}

impl TableServingOptions {
    /// Whether this table means to decide `field` itself, rather than leaving it to
    /// whatever the calling client was configured with.
    ///
    /// A table that set the field decides it. Records written before `set_fields`
    /// existed say nothing about what was set, so for those the only signal left is
    /// that the value differs from the default -- which is what every caller used to
    /// rely on, and which cannot express a table deliberately choosing a default
    /// value. `drop_percent: 0` means "never shed this table" and
    /// `max_write_retries: 0` means "never retry a write here"; both equal the
    /// default, so both were quietly replaced by the client's own setting. The two
    /// options whose whole purpose is to hold something back were the two that could
    /// not be said.
    pub fn table_decides(&self, field: TableServingField) -> bool {
        if self.set_fields.contains(field.name()) {
            return true;
        }
        let defaults = Self::default();
        match field {
            TableServingField::PinPrimary => self.pin_primary != defaults.pin_primary,
            TableServingField::ReplicaReadPolicy => {
                self.replica_read_policy != defaults.replica_read_policy
            }
            TableServingField::PreferredLocation => {
                self.preferred_location != defaults.preferred_location
            }
            TableServingField::DropPercent => self.drop_percent != defaults.drop_percent,
            TableServingField::MaxReadRetries => self.max_read_retries != defaults.max_read_retries,
            TableServingField::MaxWriteRetries => {
                self.max_write_retries != defaults.max_write_retries
            }
            TableServingField::RetryBackoffMs => self.retry_backoff_ms != defaults.retry_backoff_ms,
            TableServingField::ContinuousFailedTimeMs => {
                self.continuous_failed_time_ms != defaults.continuous_failed_time_ms
            }
            TableServingField::IoTimeoutMs => self.io_timeout_ms != defaults.io_timeout_ms,
            TableServingField::ConnectTimeoutMs => {
                self.connect_timeout_ms != defaults.connect_timeout_ms
            }
        }
    }
}

impl Default for TableServingOptions {
    fn default() -> Self {
        Self {
            pin_primary: default_pin_primary(),
            replica_read_policy: default_replica_read_policy(),
            preferred_location: String::new(),
            drop_percent: 0,
            max_read_retries: default_max_read_retries(),
            max_write_retries: 0,
            retry_backoff_ms: default_retry_backoff_ms(),
            continuous_failed_time_ms: default_continuous_failed_time_ms(),
            io_timeout_ms: default_table_io_timeout_ms(),
            connect_timeout_ms: default_table_connect_timeout_ms(),
            set_fields: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableServingOptionsPatch {
    #[serde(default)]
    pub pin_primary: Option<bool>,
    #[serde(default)]
    pub replica_read_policy: Option<String>,
    #[serde(default)]
    pub preferred_location: Option<String>,
    #[serde(default)]
    pub drop_percent: Option<u8>,
    #[serde(default)]
    pub max_read_retries: Option<u32>,
    #[serde(default)]
    pub max_write_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff_ms: Option<u64>,
    #[serde(default)]
    pub continuous_failed_time_ms: Option<u64>,
    #[serde(default)]
    pub io_timeout_ms: Option<u64>,
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
}

/// How many resources there are, and how many are in each state.
///
/// A scrape reports exactly this about tables, namespaces and proxy groups and
/// nothing else, but it was cloning every one of them -- names and serving
/// options included -- to call `.len()` and filter on the state field.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateTally {
    pub normal: u64,
    pub frozen: u64,
    pub dropped: u64,
}

impl StateTally {
    fn record(&mut self, state: MetaEntityState) {
        match state {
            MetaEntityState::Normal => self.normal += 1,
            MetaEntityState::Frozen => self.frozen += 1,
            MetaEntityState::Dropped => self.dropped += 1,
        }
    }

    /// Every resource counted, whatever state it is in.
    ///
    /// The three states are the whole of `MetaEntityState`, so this is the
    /// count the listing's `.len()` used to give.
    pub fn total(&self) -> u64 {
        self.normal + self.frozen + self.dropped
    }

    /// How many are in the state of this name, named as
    /// [`MetaEntityState::as_str`] names it.
    pub fn in_state(&self, state: &str) -> u64 {
        match state {
            "normal" => self.normal,
            "frozen" => self.frozen,
            "dropped" => self.dropped,
            _ => 0,
        }
    }
}

/// One server, reduced to what a scrape reports about it.
///
/// A server record carries its shard loads, its stat loads and its shard
/// serving states -- one entry per shard it holds, and the serving states carry
/// strings. A scrape reads none of them, so none of them are here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerScrapeRow {
    pub server_addr: String,
    pub state: MetaEntityState,
    pub reported_record_count: u64,
    pub reported_storage_bytes: u64,
    pub last_meta_topology_version: u64,
    pub rejected_total: u64,
    pub timed_out_total: u64,
    pub canceled_total: u64,
}

/// One proxy, reduced the same way.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyScrapeRow {
    pub proxy_addr: String,
    pub state: MetaEntityState,
    pub restart_count: u64,
}

/// Everything a scrape needs, taken in one pass under one read lock.
///
/// The scrape used to make five separate listing calls, each taking the lock
/// again, so its numbers described five consecutive moments rather than one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaMetricsReport {
    pub status: Status,
    pub tables: StateTally,
    pub namespaces: StateTally,
    pub proxy_groups: StateTally,
    pub servers: Vec<ServerScrapeRow>,
    pub proxies: Vec<ProxyScrapeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetTableTopologyRequest {
    pub namespace: String,
    pub table_name: String,
    #[serde(default)]
    pub old_topology_version: u64,
    /// Where the caller is, in the same hierarchical form as a server's
    /// location. Used only to order each shard's replicas nearest-first; an
    /// empty value leaves the order exactly as it was.
    #[serde(default)]
    pub client_location: String,
}

impl GetTableTopologyRequest {
    /// Ask only whether a table can be served, and at what version.
    ///
    /// A caller that wants the version or just the status does not need the
    /// shard list, and building one is the whole cost of the answer. Measured
    /// through the open-table route, the cost was the size of the table --
    /// 28.9us at 50 shards, 114.6us at 200, 434.9us at 800 -- and asking this
    /// way is 0.5us at every one of them.
    ///
    /// This is the ordinary request with the version set past anything a table
    /// can hold, which is the existing answer for a caller that is already
    /// current: every missing, dropped and frozen check still runs and still
    /// returns exactly what it returned before, and the answer stops before the
    /// shard list. Written this way so the status cannot drift from the status
    /// the full answer gives -- it is the same code producing it.
    pub fn status_only(namespace: String, table_name: String) -> Self {
        Self {
            namespace,
            table_name,
            old_topology_version: u64::MAX,
            client_location: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableMetaInfo {
    pub table_id: u64,
    pub namespace: String,
    pub table_name: String,
    pub state: MetaEntityState,
    pub topology_version: u64,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub replica_count: u64,
    #[serde(default)]
    pub partition_version: u32,
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableShard {
    pub shard_id: ShardId,
    #[serde(rename = "start_slot")]
    pub start_bucket: u64,
    #[serde(rename = "end_slot")]
    pub end_bucket: u64,
    pub primary: Option<String>,
    pub replicas: Vec<String>,
    #[serde(default)]
    pub primary_endpoint: Option<ServerEndpoint>,
    #[serde(default)]
    pub replica_endpoints: Vec<ServerEndpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerEndpoint {
    pub server_addr: String,
    #[serde(default)]
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableTopologyResponse {
    pub status: Status,
    pub table: Option<TableMetaInfo>,
    #[serde(alias = "partitions")]
    pub shards: Vec<TableShard>,
    pub unchanged: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyVersionRequest {
    #[serde(default)]
    pub old_topology_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyVersionReport {
    pub status: Status,
    pub current_topology_version: u64,
    pub old_topology_version: u64,
    pub unchanged: bool,
    pub server_count: usize,
    pub proxy_count: usize,
    pub table_count: usize,
    pub shard_route_count: usize,
    pub normal_servers: usize,
    pub frozen_servers: usize,
    pub dropped_servers: usize,
    pub normal_proxies: usize,
    pub frozen_proxies: usize,
    pub dropped_proxies: usize,
    pub normal_tables: usize,
    pub frozen_tables: usize,
    pub dropped_tables: usize,
    pub changed_tables: Vec<TableMetaInfo>,
    #[serde(default)]
    pub events: Vec<TopologyChangeEvent>,
    #[serde(default)]
    pub event_history_truncated: bool,
}

/// Which slice of the change history to return.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyEventsRequest {
    /// Return changes newer than this topology version. Zero returns whatever
    /// the ring still holds.
    #[serde(default)]
    pub after_version: u64,
    /// Most changes to return. Zero means the ring's own limit.
    #[serde(default)]
    pub limit: usize,
}

/// What the metaserver recorded, and whether the caller missed anything.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyEventsResponse {
    pub status: Status,
    pub events: Vec<TopologyChangeEvent>,
    /// The oldest change still held. The history is a bounded ring, so anything
    /// older than this has been overwritten and cannot be asked for.
    pub oldest_retained_version: u64,
    /// Set when the caller asked to resume from a point the ring no longer
    /// holds, so a gap in what they receive is not mistaken for quiet.
    pub missed_events: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyChangeEvent {
    pub topology_version: u64,
    pub timestamp_ms: u64,
    pub kind: String,
    pub resource: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListNamespacesResponse {
    pub status: Status,
    pub namespaces: Vec<NamespaceMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListTablesResponse {
    pub status: Status,
    pub tables: Vec<TableMetaInfo>,
}

/// Which shards to list, and how many.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListShardsRequest {
    /// Only shards owned by this server. Empty lists every server's.
    #[serde(default)]
    pub server_addr: String,
    /// Resume after this shard id, exclusive. Zero starts from the beginning.
    #[serde(default)]
    pub after_shard_id: ShardId,
    /// Most shards to return. Zero means the default cap.
    #[serde(default)]
    pub limit: usize,
}

/// One shard's placement, as the metaserver has it recorded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardListEntry {
    pub shard_id: ShardId,
    pub server_addr: String,
    /// The table this shard's id falls inside, when one claims it. A registered
    /// shard whose table was dropped, or that was registered before its table
    /// existed, belongs to nothing and reports empty here -- which is worth
    /// seeing rather than hiding.
    pub namespace: String,
    pub table_name: String,
    pub latest_snapshot: Option<ShardSnapshotRef>,
    /// Whether the metaserver considers this shard served.
    ///
    /// A shard can be taken out of service on its own, and the listing had no
    /// way to show it: you could freeze a shard and then not see, here, that it
    /// was frozen.
    #[serde(default)]
    pub state: MetaEntityState,
    /// Whether the owning server still reports this shard as loaded.
    ///
    /// Freezing a shard is a decision the metaserver records immediately, but
    /// the datanode holding it has work to do before it has really let go. Until
    /// then the shard is frozen in the metadata and still resident on the node,
    /// and nothing said which. For a shard that is still serving this reads the
    /// other way: an owner that is not holding it is a divergence.
    ///
    /// `None` means the owner does not report shard states at all, which is not
    /// the same as reporting that it holds nothing -- the same distinction the
    /// divergence check makes before it is willing to judge a server.
    #[serde(default)]
    pub owner_reports_loaded: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListShardsResponse {
    pub status: Status,
    pub shards: Vec<ShardListEntry>,
    /// Set only when the cap cut the page short. Pass it back as
    /// `after_shard_id` to continue; absent means this was the last page.
    pub next_after_shard_id: Option<ShardId>,
}

/// Default and hard cap on one page of shards.
///
/// Every other list endpoint returns everything, which is fine for servers,
/// proxies and tables -- there are tens of those. Shards are the one entity
/// there can be hundreds of thousands of, so this one paginates rather than
/// handing back the whole placement table in a single response.
pub const LIST_SHARDS_DEFAULT_LIMIT: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListServersResponse {
    pub status: Status,
    pub servers: Vec<ServerMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListProxiesResponse {
    pub status: Status,
    pub proxies: Vec<ProxyMetaInfo>,
}

/// Relabel a registered server's location without disturbing anything else
/// about it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateServerRequest {
    pub server_addr: String,
    /// The new location. May be empty, which means "unlabelled, place anywhere".
    #[serde(default)]
    pub location: String,
}

/// A resource announcing that it is shutting down.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NotifyStopRequest {
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateChangeRequest {
    /// Why the resource is being frozen. Absent from the wire it reads as
    /// `Unspecified`; the admin routes set `Operator` explicitly, since a
    /// request arriving there is operator-driven by definition.
    #[serde(default)]
    pub reason: FreezeReason,
    pub endpoint: String,
    #[serde(default)]
    pub freeze_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadFinishRequest {
    pub server_addr: String,
    pub shard_id: ShardId,
    pub load_version: u64,
    pub status: Status,
    #[serde(default)]
    pub scheduler_task_id: Option<u64>,
    #[serde(default)]
    pub scheduler_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaControlPlaneParityReport {
    pub status: Status,
    pub table_topology_ready: bool,
    pub transitional_state_model_ready: bool,
    pub topology_history_ready: bool,
    pub scheduler_owned_finish_load_ready: bool,
    pub scheduler_generation_check_ready: bool,
    pub durable_replay_ready: bool,
    pub real_data_node_coordination_ready: bool,
    pub scheduler_finish_generation_count: usize,
    pub topology_event_count: usize,
    pub topology_version: u64,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaStats {
    pub register_shard_total: u64,
    pub get_shard_total: u64,
    pub server_register_total: u64,
    pub server_heartbeat_total: u64,
    pub proxy_register_total: u64,
    pub proxy_heartbeat_total: u64,
    pub namespace_create_total: u64,
    pub table_create_total: u64,
    pub topology_query_total: u64,
    pub load_finish_total: u64,
    pub topology_version: u64,
    pub server_count: usize,
    pub proxy_count: usize,
    pub namespace_count: usize,
    pub table_count: usize,
    pub shard_count: usize,
    /// How many of those shards are not serving.
    ///
    /// A shard can be taken out of service on its own, and nothing counted it:
    /// an operator could freeze one and see no change on any dashboard. Counted
    /// here rather than kept as a running total, because a tally maintained
    /// beside the shards is a second thing to keep in step with them.
    #[serde(default)]
    pub frozen_shard_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaInfo {
    pub status: Status,
    pub stats: MetaStats,
    pub boot_time_ms: u64,
    pub durable_mutation_log: bool,
    /// Whether recorded metadata mutations are currently refused.
    #[serde(default)]
    pub meta_change_muted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaPreflightReport {
    pub status: Status,
    pub stats: MetaStats,
    pub normal_servers: usize,
    pub frozen_servers: usize,
    pub normal_proxies: usize,
    pub frozen_proxies: usize,
    pub dropped_tables: usize,
    pub shard_routes: usize,
    pub degraded_reasons: Vec<String>,
}

impl MetaMutation {
    /// Whether this change may still be made while metadata change is muted.
    ///
    /// Only the lever itself and the retention purge. The lever, because muting
    /// must not be a one-way door -- refusing it would leave no way back. The
    /// purge, because the single-node path has always allowed it: its one
    /// caller is the retention loop, which the mute stops separately, and the
    /// two backends have to agree on what the mute means rather than each
    /// inventing an answer.
    pub(crate) fn allowed_while_muted(&self) -> bool {
        matches!(self, Self::SetMetaChangeMuted(_) | Self::PurgeMeta(_))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum MetaMutation {
    RegisterShard(RegisterShardRequest),
    PublishShardSnapshot(PublishShardSnapshotRequest),
    RegisterServer(RegisterServerRequest),
    RegisterProxy(RegisterProxyRequest),
    AddNamespace(AddNamespaceRequest),
    AddTable(AddTableRequest),
    DeleteTable(DeleteTableRequest),
    UpdateTable(UpdateTableRequest),
    FreezeTable(DeleteTableRequest),
    UnfreezeTable(DeleteTableRequest),
    FinishLoad(LoadFinishRequest),
    UpdateServer(UpdateServerRequest),
    FreezeServer(StateChangeRequest),
    UnfreezeServer(StateChangeRequest),
    DropServer(StateChangeRequest),
    FreezeProxy(StateChangeRequest),
    UnfreezeProxy(StateChangeRequest),
    DropProxy(StateChangeRequest),
    SetShardState(ShardStateRequest, MetaEntityState),
    DropShard(ShardStateRequest),
    PutProxyGroup(PutProxyGroupRequest),
    DropProxyGroup(DropProxyGroupRequest),
    SetProxyGroup(ProxyAttachment),
    /// One applied retention round. The *outcome* is recorded rather than the
    /// intent to run a round, because retention is computed from the wall clock
    /// and re-planning it during replay would purge a different set. Replaying
    /// the concrete list forgets exactly what the live round forgot.
    PurgeMeta(MetaRetentionPlan),
    /// Freeze, unfreeze or drop a whole namespace.
    SetNamespaceState(AddNamespaceRequest, MetaEntityState),
    /// Replace the set of names held back from creation.
    SetReservedNames(ReservedNames),
    /// Pin one shard to a location, or release it with an empty one.
    SetShardPreferredLocation(ShardPinRequest),
    /// Mute or resume metadata change. Recorded like any other mutation so it
    /// replays in order and reaches raft peers; the guard is deliberately not
    /// applied during replay, because the log only ever contains mutations that
    /// were accepted live, and a mute entry flips the flag at exactly the point
    /// the original sequence did.
    SetMetaChangeMuted(bool),
    /// A snapshot installed over the whole state.
    ///
    /// Carries the snapshot because replay has to reach the same state the
    /// install did. Every record before it is superseded by it, which is what
    /// makes the restore hold across a restart.
    InstallSnapshot(Box<MetaSnapshot>),
}

/// One line of the mutation log: a change, and when the metaserver accepted it.
///
/// The time has to travel with the change. Everything that ages -- retention,
/// freeze aging -- was stamped with `now_ms()` inside the apply path, which
/// replay also runs, so every clock restarted whenever the metaserver did and
/// nothing was ever old enough to act on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaMutationRecord {
    /// Zero on a line written before this field existed. Replay falls back to
    /// the current clock for those, which is what they have always been given.
    #[serde(default)]
    pub at_ms: u64,
    #[serde(flatten)]
    pub mutation: MetaMutation,
}

#[derive(Debug, Clone)]
pub struct LocalMetaMutationLog {
    path: PathBuf,
    /// The handle records are appended through, opened once.
    ///
    /// Opening the file per record cost more than it looked like: with the
    /// barrier split out so writers can share one, a record was opening it
    /// twice.
    write_lock: Arc<Mutex<Option<File>>>,
    /// A second handle for the barrier, so it can run without the write lock
    /// and let the next writer get its bytes down meanwhile. Syncing is per
    /// file, not per handle, so this covers what the other one wrote.
    sync_file: Arc<Mutex<Option<File>>>,
    /// How many records have been written to the file.
    ///
    /// Written under `write_lock`, so it counts the records whose bytes have
    /// reached the file in the order the lock granted.
    written: Arc<AtomicU64>,
    /// How many records a completed sync has covered.
    ///
    /// Guarded by its own lock rather than `write_lock`, so a writer waiting for
    /// durability is not holding up the writer behind it.
    synced: Arc<Mutex<u64>>,
}

impl LocalMetaMutationLog {
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            path,
            write_lock: Arc::default(),
            sync_file: Arc::default(),
            written: Arc::default(),
            synced: Arc::default(),
        })
    }

    pub fn append(&self, mutation: &MetaMutation, at_ms: u64) -> io::Result<()> {
        // The bytes reach the file in the order the write lock grants, and the
        // record is numbered by that order.
        let mine = {
            let mut handle = self
                .write_lock
                .lock()
                .expect("meta mutation log lock poisoned");
            let file = match handle.as_mut() {
                Some(file) => file,
                None => handle.insert(
                    OpenOptions::new().create(true).append(true).open(&self.path)?,
                ),
            };
            let record = MetaMutationRecord {
                at_ms,
                mutation: mutation.clone(),
            };
            serde_json::to_writer(&mut *file, &record).map_err(io::Error::other)?;
            file.write_all(b"\n")?;
            self.written.fetch_add(1, Ordering::SeqCst) + 1
        };
        self.sync_through(mine)
    }

    /// Return once a sync that began after record `mine` was written has
    /// completed.
    ///
    /// The barrier a writer needs is not its own: any sync beginning after its
    /// bytes reached the file covers them, because the file is append-only and
    /// the writes are ordered. So a writer that finds its record already covered
    /// has nothing to do, and one that does not takes the barrier for everything
    /// written so far -- including the writers still queued behind it.
    fn sync_through(&self, mine: u64) -> io::Result<()> {
        let mut synced = self.synced.lock().expect("meta mutation log sync poisoned");
        if *synced >= mine {
            // Somebody else's barrier already covered this record.
            return Ok(());
        }
        // Read before the barrier, published after it. The other order would
        // tell a writer its bytes were durable while the sync was still running.
        let covered = self.written.load(Ordering::SeqCst);
        let mut handle = self
            .sync_file
            .lock()
            .expect("meta mutation log sync handle poisoned");
        let file = match handle.as_mut() {
            Some(file) => file,
            None => handle.insert(
                OpenOptions::new().create(true).append(true).open(&self.path)?,
            ),
        };
        crate::durability_metrics::record_barrier("meta_log_append");
        file.sync_data()?;
        *synced = (*synced).max(covered);
        Ok(())
    }

    /// Read the log back.
    ///
    /// A partial record at the END is what a crash partway through an append
    /// leaves behind, and it is dropped rather than refused. That record was
    /// never acknowledged: a writer only returns once a sync covering its bytes
    /// has completed, so nothing was promised to anybody about it. Refusing the
    /// whole file for it meant a metaserver whose metadata was entirely durable
    /// except for a fraction of one record would not start.
    ///
    /// A line that does not parse with records AFTER it is a different thing.
    /// Those later records WERE acknowledged, and stopping there would discard
    /// them silently, so that is still an error. The distinction is the whole
    /// point: the tail is expected, the middle is not.
    /// Read the log back, repairing a torn tail if there is one.
    ///
    /// A partial record at the END is what a crash partway through an append
    /// leaves behind. It is dropped rather than refused: that record was never
    /// acknowledged, because a writer only returns once a sync covering its
    /// bytes has completed, so nothing was promised to anybody about it.
    /// Refusing the whole file for it meant a metaserver whose metadata was
    /// entirely durable except for a fraction of one record would not start.
    ///
    /// The fragment is also REMOVED, not merely skipped. Appends open the file
    /// for append and write at the end; left in place, the fragment would be
    /// spliced onto the front of the next record and stop that line parsing.
    /// That line would then be the last one, so the following restart would
    /// drop it as a torn tail -- silently losing a write that WAS acknowledged.
    /// Truncating to the end of the last record that parsed means the next
    /// append starts on a record boundary.
    ///
    /// A line that does not parse with records AFTER it is a different thing.
    /// Those later records were acknowledged, and stopping there would discard
    /// them silently, so that is still an error. The tail is expected; the
    /// middle is not.
    pub fn load(&self) -> io::Result<Vec<MetaMutationRecord>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let text = fs::read_to_string(&self.path)?;
        let mut mutations = Vec::new();
        // Where the last record that parsed ends, including its newline. This is
        // the only point the file can be safely cut back to.
        let mut good_end = 0usize;
        let mut offset = 0usize;
        // Held rather than returned: a line that does not parse is only a torn
        // tail if nothing follows it. Anything following turns it into an error.
        let mut unparsed: Option<(usize, String)> = None;
        for (number, line) in text.split('\n').enumerate() {
            let line_end = (offset + line.len() + 1).min(text.len());
            offset += line.len() + 1;
            if line.trim().is_empty() {
                continue;
            }
            if let Some((bad_number, message)) = unparsed.take() {
                return Err(io::Error::other(format!(
                    "meta mutation log {}: line {} does not parse and more lines follow it, \
                     so it is not a torn tail: {}",
                    self.path.display(),
                    bad_number + 1,
                    message
                )));
            }
            match serde_json::from_str::<MetaMutationRecord>(line) {
                Ok(record) => {
                    mutations.push(record);
                    good_end = line_end;
                }
                Err(err) => unparsed = Some((number, err.to_string())),
            }
        }
        if let Some((number, message)) = unparsed {
            tracing::warn!(
                path = %self.path.display(),
                line = number + 1,
                recovered = mutations.len(),
                truncated_to = good_end,
                error = %message,
                "metadata log ends in a partial record; it was never acknowledged, so it is \
                 dropped and the file is cut back to the last whole record"
            );
            let file = OpenOptions::new().write(true).open(&self.path)?;
            file.set_len(good_end as u64)?;
            // The cut has to survive the crash that follows it, or the fragment
            // comes back and the next append splices onto it again.
            crate::durability_metrics::record_barrier("meta_log_truncate");
            file.sync_all()?;
        }
        Ok(mutations)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Service counters, held outside the metadata lock.
///
/// These used to live inside `MetaState`, which meant the two hottest *read*
/// paths -- resolving a table's topology and reading one shard's location --
/// had to take the **exclusive** lock, purely to add one to a number. Every
/// topology lookup therefore serialised against every other lookup and against
/// every metadata write, on a path that every client and every proxy calls.
///
/// Counting is the one thing here that does not need to agree with anything
/// else, so `Relaxed` is the right ordering: the total is a monotonic tally for
/// `/metrics`, never a decision input.
#[derive(Debug, Default)]
struct MetaCounters {
    register_shard_total: AtomicU64,
    get_shard_total: AtomicU64,
    server_register_total: AtomicU64,
    server_heartbeat_total: AtomicU64,
    proxy_register_total: AtomicU64,
    proxy_heartbeat_total: AtomicU64,
    namespace_create_total: AtomicU64,
    table_create_total: AtomicU64,
    topology_query_total: AtomicU64,
    load_finish_total: AtomicU64,
}

impl MetaCounters {
    /// Restore the tallies carried by an installed snapshot, so a peer that
    /// installs one does not report its counters starting from zero.
    fn install_from(&self, stats: &MetaStats) {
        for (slot, value) in [
            (&self.register_shard_total, stats.register_shard_total),
            (&self.get_shard_total, stats.get_shard_total),
            (&self.server_register_total, stats.server_register_total),
            (&self.server_heartbeat_total, stats.server_heartbeat_total),
            (&self.proxy_register_total, stats.proxy_register_total),
            (&self.proxy_heartbeat_total, stats.proxy_heartbeat_total),
            (&self.namespace_create_total, stats.namespace_create_total),
            (&self.table_create_total, stats.table_create_total),
            (&self.topology_query_total, stats.topology_query_total),
            (&self.load_finish_total, stats.load_finish_total),
        ] {
            slot.store(value, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableRecord {
    info: TableMetaInfo,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MetaState {
    shards: HashMap<ShardId, ShardLocation>,
    servers: BTreeMap<String, ServerMetaInfo>,
    proxies: BTreeMap<String, ProxyMetaInfo>,
    proxy_groups: BTreeMap<String, ProxyGroupInfo>,
    namespaces: BTreeMap<String, MetaEntityState>,
    tables: BTreeMap<String, TableRecord>,
    next_table_id: u64,
    topology_version: u64,
    topology_events: VecDeque<TopologyChangeEvent>,
    scheduler_finish_generations: BTreeMap<String, u64>,
    /// When each dropped resource was dropped, keyed `server:<addr>`,
    /// `proxy:<addr>` or `table:<namespace.table>`. Dropping previously recorded
    /// no timestamp at all, so "dropped long enough ago to forget" was not
    /// expressible; retention ages against this. Kept beside the resources
    /// rather than inside them so the wire types are unchanged.
    dropped_since_ms: BTreeMap<String, u64>,
    /// While set, the metaserver refuses every recorded metadata mutation.
    ///
    /// This is an incident lever: every automatic subsystem is otherwise gated
    /// by an environment variable, so the only way to stop the metaserver making
    /// changes was to restart it with different configuration -- during exactly
    /// the incident where a restart is least welcome.
    meta_change_muted: bool,
    /// Names held back from creation. Durable, like every other decision an
    /// operator makes here.
    reserved_names: ReservedNames,
    /// When each frozen table was frozen, keyed `table:<namespace.table>`.
    /// Servers and proxies carry `frozen_since_ms` on the resource itself;
    /// tables do not, and adding a field there would touch every
    /// `TableMetaInfo` literal in the tree, so the metaserver keeps it beside
    /// them the same way it keeps drop times.
    frozen_since_ms: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshot {
    pub format_version: u32,
    pub created_at_ms: u64,
    pub shards: HashMap<ShardId, ShardLocation>,
    pub servers: BTreeMap<String, ServerMetaInfo>,
    pub proxies: BTreeMap<String, ProxyMetaInfo>,
    /// Declared proxy capacity. Carried so a peer installing this snapshot
    /// knows what the routing tier is supposed to look like, not just what it
    /// currently is.
    #[serde(default)]
    pub proxy_groups: BTreeMap<String, ProxyGroupInfo>,
    pub namespaces: BTreeMap<String, MetaEntityState>,
    pub tables: Vec<TableMetaInfo>,
    pub stats: MetaStats,
    pub next_table_id: u64,
    pub topology_version: u64,
    #[serde(default)]
    pub scheduler_finish_generations: BTreeMap<String, u64>,
    /// Drop timestamps for the tombstones in this snapshot. Carried so a peer
    /// that installs the snapshot can keep ageing them instead of restarting
    /// every tombstone's clock.
    #[serde(default)]
    pub dropped_since_ms: BTreeMap<String, u64>,
    /// Whether metadata change was muted when this snapshot was taken. Carried
    /// so a peer installing it does not silently resume a mute an operator put
    /// in place.
    #[serde(default)]
    pub meta_change_muted: bool,
    /// Freeze timestamps for the frozen tables in this snapshot, so a peer that
    /// installs it keeps ageing them instead of restarting their clocks.
    #[serde(default)]
    pub frozen_since_ms: BTreeMap<String, u64>,
    /// The recorded change history, oldest first.
    ///
    /// The topology version travelled while the history it belongs to did not,
    /// so a peer installing a snapshot inherited a version with nothing behind
    /// it -- and then reported its own control plane as blocked on evidence it
    /// had a moment earlier. Bounded to the same ring the metaserver keeps, so
    /// this adds a fixed, small amount to a snapshot rather than growing with
    /// the cluster.
    #[serde(default)]
    pub topology_events: VecDeque<TopologyChangeEvent>,
    /// Names held back from creation. Carried so a peer that installs this
    /// snapshot keeps refusing them rather than quietly allowing what the
    /// operator reserved.
    #[serde(default)]
    pub reserved_names: ReservedNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotFileRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotResponse {
    pub status: Status,
    pub snapshot: Option<MetaSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotFileResponse {
    pub status: Status,
    pub path: String,
    pub snapshot: Option<MetaSnapshot>,
}

#[derive(Debug, Clone)]
pub struct SingleNodeMeta {
    inner: Arc<RwLock<MetaState>>,
    boot_time_ms: u64,
    mutation_log: Option<LocalMetaMutationLog>,
    /// When set, a resource the metaserver convicted cannot register its way
    /// back into service; only an explicit unfreeze returns it. Off by default
    /// because today's automatic recovery is load-bearing for deployments
    /// running with a zero freeze cooldown.
    forbid_self_clearing_conviction: bool,
    /// Where the background subsystems record what each round did, so `/metrics`
    /// can report it. Shared by clone, so every handle to this meta writes to
    /// and reads from the same recorder.
    metrics: SubsystemMetrics,
    /// Fan-out for metadata change, deliberately outside `inner` so a
    /// subscriber's cost never lands inside the metadata lock. Shared by clone,
    /// like the metadata itself.
    events: Arc<event_bus::MetaEventBus>,
    /// Service counters, deliberately outside `inner` -- see [`MetaCounters`].
    /// Shared by clone, like the metadata itself, so every handle counts into
    /// the same tallies.
    counters: Arc<MetaCounters>,
}

impl Default for SingleNodeMeta {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MetaState {
                next_table_id: 1,
                ..MetaState::default()
            })),
            boot_time_ms: now_ms(),
            mutation_log: None,
            forbid_self_clearing_conviction: false,
            metrics: SubsystemMetrics::new(),
            events: Arc::new(event_bus::MetaEventBus::default()),
            counters: Arc::new(MetaCounters::default()),
        }
    }
}

impl SingleNodeMeta {
    /// Set the conviction lock on a meta that already exists.
    ///
    /// The builder cannot reach a raft node's meta: those are constructed inside
    /// the cluster, long after the process read its configuration.
    pub fn set_conviction_lock(&mut self, forbid: bool) {
        self.forbid_self_clearing_conviction = forbid;
    }

    /// Refuse to let a resource the metaserver convicted register its way back
    /// into service; only an explicit unfreeze returns it. Off by default.
    pub fn with_conviction_lock(mut self, forbid: bool) -> Self {
        self.forbid_self_clearing_conviction = forbid;
        self
    }

    /// True when a convicted resource cannot clear its own freeze.
    pub fn conviction_lock_enabled(&self) -> bool {
        self.forbid_self_clearing_conviction
    }

    /// The recorder the background subsystems write their round outcomes into.
    pub fn subsystem_metrics(&self) -> &SubsystemMetrics {
        &self.metrics
    }

    pub fn register(&self, mut request: RegisterShardRequest) -> RegisterShardResponse {
        if let Some(status) = self.meta_change_refusal() {
            return RegisterShardResponse { status };
        }
        // Stamped here, so the recorded mutation carries the metaserver's clock
        // and a replay keeps the original rather than restamping itself.
        request.registered_at_ms = now_ms();
        self.record_mutation(MetaMutation::RegisterShard(request.clone()));
        self.apply_register(request)
    }

    fn apply_register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        self.counters.register_shard_total.fetch_add(1, Ordering::Relaxed);
        let latest_snapshot = state
            .shards
            .get(&request.shard_id)
            .and_then(|location| location.latest_snapshot.clone());
        // Registering a shard again -- to move it, or after a restart -- does
        // not make it a new shard.
        let registered_at_ms = state
            .shards
            .get(&request.shard_id)
            .map(|location| location.registered_at_ms)
            .filter(|first| *first != 0)
            .unwrap_or(request.registered_at_ms);
        state.shards.insert(
            request.shard_id,
            ShardLocation {
<<<<<<< HEAD
                registered_at_ms,
||||||| a7277311
=======
                preferred_location: String::new(),
>>>>>>> matrixark/main
                state: MetaEntityState::Normal,
                shard_id: request.shard_id,
                server_addr: request.server_addr.clone(),
                latest_snapshot,
            },
        );
        ensure_server(&mut state, &request.server_addr);
        record_topology_event(
            &mut state,
            "register_shard",
            format!("shard:{}", request.shard_id),
            format!("server_addr={}", request.server_addr),
        );
        RegisterShardResponse {
            status: Status::ok(),
        }
    }

    /// Stop serving one shard, keeping its owner entry so it can come back.
    pub fn freeze_shard(&self, request: ShardStateRequest) -> AckResponse {
        self.set_shard_state(request, MetaEntityState::Frozen)
    }

    /// Return a frozen shard to service.
    pub fn unfreeze_shard(&self, request: ShardStateRequest) -> AckResponse {
        self.set_shard_state(request, MetaEntityState::Normal)
    }

    fn set_shard_state(&self, request: ShardStateRequest, next: MetaEntityState) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::SetShardState(request.clone(), next));
        self.apply_set_shard_state(request, next)
    }

    pub(crate) fn apply_set_shard_state(
        &self,
        request: ShardStateRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(shard) = state.shards.get_mut(&request.shard_id) else {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        };
        if shard.state == next {
            return AckResponse {
                status: Status::error("not_modified", "shard state is unchanged"),
            };
        }
        shard.state = next;
        // Topology is derived on read, so the version bump is what makes clients
        // stop (or resume) resolving to this shard.
        record_topology_event(
            &mut state,
            "shard_state",
            format!("shard:{}", request.shard_id),
            format!("state={}", next.as_str()),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Forget a shard entirely. The route is removed rather than tombstoned:
    /// a shard has no identity beyond where it is served from.
    pub fn drop_shard(&self, request: ShardStateRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::DropShard(request.clone()));
        self.apply_drop_shard(request)
    }

    pub(crate) fn apply_drop_shard(&self, request: ShardStateRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        if state.shards.remove(&request.shard_id).is_none() {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        }
        record_topology_event(
            &mut state,
            "drop_shard",
            format!("shard:{}", request.shard_id),
            "state=dropped",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn get(&self, shard_id: ShardId) -> GetShardResponse {
        self.counters.get_shard_total.fetch_add(1, Ordering::Relaxed);
        // Reading one shard's location only reads, for the same reason.
        let state = self.inner.read().expect("meta lock poisoned");
        let location = state.shards.get(&shard_id).cloned();
        GetShardResponse {
            status: if location.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not registered")
            },
            location,
        }
    }

    pub fn publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::PublishShardSnapshot(request.clone()));
        self.apply_publish_shard_snapshot(request)
    }

    fn apply_publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(location) = state.shards.get_mut(&request.shard_id) else {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        };
        if location.latest_snapshot.as_ref().map_or(false, |existing| {
            existing.last_log_index > request.snapshot.last_log_index
        }) {
            return AckResponse {
                status: Status::error(
                    "stale_snapshot",
                    "snapshot is older than the latest metaserver record",
                ),
            };
        }
        location.latest_snapshot = Some(request.snapshot);
        record_topology_event(
            &mut state,
            "publish_shard_snapshot",
            format!("shard:{}", request.shard_id),
            "latest_snapshot_updated",
        );
        AckResponse {
            status: Status::ok(),
        }
    }



    /// True while recorded metadata mutations are being refused.
    pub fn is_meta_change_muted(&self) -> bool {
        self.inner
            .read()
            .expect("meta lock poisoned")
            .meta_change_muted
    }

    /// The refusal every guarded entry point returns while muted, or `None` when
    /// metadata change is flowing normally.
    /// The refusal a muted metaserver answers with, so both backends phrase it
    /// identically.
    pub(crate) fn muted_status() -> Status {
        Status::error(
            "meta_change_muted",
            "metadata change is muted; resume it before making changes",
        )
    }

    fn meta_change_refusal(&self) -> Option<Status> {
        self.is_meta_change_muted().then(Self::muted_status)
    }

    /// The names currently held back from creation.
    pub fn reserved_names(&self) -> ReservedNamesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ReservedNamesResponse {
            status: Status::ok(),
            reserved: state.reserved_names.clone(),
        }
    }

    /// Replace the reserved set. Guarded by the mute like any other change, and
    /// recorded so it survives restart and reaches raft peers.
    pub fn set_reserved_names(&self, reserved: ReservedNames) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::SetReservedNames(reserved.clone()));
        self.apply_set_reserved_names(reserved)
    }

    pub(crate) fn apply_set_reserved_names(&self, reserved: ReservedNames) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.reserved_names = reserved;
        let detail = format!(
            "namespaces={},tables={}",
            state.reserved_names.namespaces.len(),
            state.reserved_names.tables.len()
        );
        record_topology_event(&mut state, "reserved_names", "cluster".to_string(), detail);
        AckResponse {
            status: Status::ok(),
        }
    }

    /// The refusal a guarded creation gives, or `None` when the name is free.
    ///
    /// Checked on the way in rather than inside `apply_*`, exactly like the
    /// mute: the mutation log then holds only creations that were accepted, so
    /// replay does not have to re-decide against a reserved set that has since
    /// changed.
    /// Why this change must not be admitted, judged against current state.
    ///
    /// These guards lived only in the public methods. `apply_mutation` dispatches
    /// straight to the `apply_*` functions, so the raft propose path went around
    /// every one of them: a reserved namespace could be created, and a namespace
    /// could be dropped out from under a table that was still live, stranding it.
    ///
    /// Judged before proposing and never while applying. Replay has to reapply
    /// what was already accepted -- a name reserved today must not invalidate a
    /// namespace legitimately created before it.
    pub(crate) fn admission_refusal(&self, mutation: &MetaMutation) -> Option<Status> {
        match mutation {
            MetaMutation::AddNamespace(request) => {
                self.reserved_name_refusal(&request.namespace, None)
            }
            MetaMutation::AddTable(request) => {
                self.reserved_name_refusal(&request.namespace, Some(&request.table_name))
            }
            MetaMutation::SetNamespaceState(request, MetaEntityState::Dropped) => {
                self.namespace_not_empty_refusal(&request.namespace)
            }
            MetaMutation::PutProxyGroup(request) => Self::proxy_group_name_refusal(request),
            _ => None,
        }
    }

    /// Refuses a proxy group that names neither itself nor a namespace.
    ///
    /// The group name is the key it is stored under, and an empty one is also
    /// the value an unattached proxy carries -- so a nameless group is indexed
    /// by "no group at all" and reads as though every idle proxy belongs to it.
    /// The public method has always refused this; the propose path dispatched
    /// straight to `apply_put_proxy_group`, which does not check, and committed
    /// it into replicated metadata.
    ///
    /// Takes no lock: it judges the request alone, not the state around it.
    fn proxy_group_name_refusal(request: &PutProxyGroupRequest) -> Option<Status> {
        (request.group.is_empty() || request.namespace.is_empty())
            .then(|| Status::error("bad_request", "group and namespace are required"))
    }

    /// Refuses dropping a namespace that still holds a table which is not itself
    /// dropped, so dropping a namespace cannot strand one.
    fn namespace_not_empty_refusal(&self, namespace: &str) -> Option<Status> {
        let state = self.inner.read().expect("meta lock poisoned");
        state
            .tables
            .values()
            .any(|table| {
                table.info.namespace == namespace
                    && table.info.state != MetaEntityState::Dropped
            })
            .then(|| {
                Status::error(
                    "namespace_not_empty",
                    "namespace still holds a table that is not dropped",
                )
            })
    }

    fn reserved_name_refusal(&self, namespace: &str, table: Option<&str>) -> Option<Status> {
        let state = self.inner.read().expect("meta lock poisoned");
        if state.reserved_names.namespaces.contains(namespace) {
            return Some(Status::error(
                "name_reserved",
                format!("namespace {namespace} is reserved"),
            ));
        }
        let table = table?;
        state
            .reserved_names
            .tables
            .contains(table)
            .then(|| Status::error("name_reserved", format!("table name {table} is reserved")))
    }

    /// Pin one shard to a location, overriding what its table prefers. An
    /// empty location releases the pin.
    pub fn pin_shard(&self, request: ShardPinRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::SetShardPreferredLocation(request.clone()));
        self.apply_pin_shard(request)
    }

    pub(crate) fn apply_pin_shard(&self, request: ShardPinRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(shard) = state.shards.get_mut(&request.shard_id) else {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        };
        if shard.preferred_location == request.location {
            return AckResponse {
                status: Status::error("not_modified", "shard location preference is unchanged"),
            };
        }
        shard.preferred_location = request.location.clone();
        // Placement is derived on read, so the version bump is what makes the
        // planners reconsider where this shard belongs.
        record_topology_event(
            &mut state,
            "shard_preferred_location",
            format!("shard:{}", request.shard_id),
            if request.location.is_empty() {
                "released".to_string()
            } else {
                format!("location={}", request.location)
            },
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// The recorded change history, oldest first.
    ///
    /// Every metadata change records one of these, and the only way to see them
    /// was to subscribe and wait: an operator looking at an incident that had
    /// already happened could not ask what changed.
    pub fn topology_events(&self, request: TopologyEventsRequest) -> TopologyEventsResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let oldest_retained_version = state
            .topology_events
            .front()
            .map(|event| event.topology_version)
            .unwrap_or_default();
        // Resuming from a version the ring has already overwritten means there
        // are changes the caller will never see. Saying so is the difference
        // between a gap and a quiet period.
        let missed_events = request.after_version > 0
            && oldest_retained_version > request.after_version.saturating_add(1);
        let limit = if request.limit == 0 {
            TOPOLOGY_EVENT_HISTORY_LIMIT
        } else {
            request.limit.min(TOPOLOGY_EVENT_HISTORY_LIMIT)
        };
        let events = state
            .topology_events
            .iter()
            .filter(|event| event.topology_version > request.after_version)
            .take(limit)
            .cloned()
            .collect();
        TopologyEventsResponse {
            status: Status::ok(),
            events,
            oldest_retained_version,
            missed_events,
        }
    }

    /// Mute or resume metadata change.
    ///
    /// Never guarded by the mute itself -- an operator must always be able to
    /// resume -- and recorded so it survives restart and reaches raft peers.
    pub fn set_meta_change_muted(&self, muted: bool) -> AckResponse {
        self.record_mutation(MetaMutation::SetMetaChangeMuted(muted));
        self.apply_set_meta_change_muted(muted)
    }

    pub(crate) fn apply_set_meta_change_muted(&self, muted: bool) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.meta_change_muted = muted;
        record_topology_event(
            &mut state,
            "meta_change_mute",
            "cluster".to_string(),
            format!("muted={muted}"),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Record a change and return the time it was accepted, which the apply
            /// path must stamp with so replay reproduces it rather than the
            /// clock of whenever the metaserver was last restarted.
    fn record_mutation(&self, mutation: MetaMutation) -> u64 {
        let at_ms = now_ms();
        if let Some(log) = &self.mutation_log {
            log.append(&mutation, at_ms)
                .expect("failed to append metaserver mutation log");
        }
        at_ms
    }

    pub(crate) fn apply_mutation(&self, mutation: MetaMutation) -> Status {
        self.apply_mutation_at(mutation, now_ms())
    }

    pub(crate) fn apply_mutation_at(&self, mutation: MetaMutation, at_ms: u64) -> Status {
        match mutation {
            MetaMutation::RegisterShard(request) => self.apply_register(request).status,
            MetaMutation::PublishShardSnapshot(request) => {
                self.apply_publish_shard_snapshot(request).status
            }
            MetaMutation::RegisterServer(request) => self.apply_register_server(request).status,
            MetaMutation::RegisterProxy(request) => self.apply_register_proxy(request).status,
            MetaMutation::AddNamespace(request) => self.apply_add_namespace(request).status,
            MetaMutation::AddTable(request) => self.apply_add_table(request).status,
            MetaMutation::DeleteTable(request) => self.apply_delete_table(request, at_ms).status,
            MetaMutation::UpdateTable(request) => self.apply_update_table(request).status,
            MetaMutation::FreezeTable(request) => {
                self.apply_set_table_state(request, MetaEntityState::Frozen, at_ms)
                    .status
            }
            MetaMutation::UnfreezeTable(request) => {
                self.apply_set_table_state(request, MetaEntityState::Normal, at_ms)
                    .status
            }
            MetaMutation::FinishLoad(request) => self.apply_finish_load(request).status,
            MetaMutation::UnfreezeServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Normal, at_ms)
                    .status
            }
            MetaMutation::UnfreezeProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Normal, at_ms)
                    .status
            }
            MetaMutation::UpdateServer(request) => self.apply_update_server(request).status,
            MetaMutation::SetNamespaceState(request, next) => {
                self.apply_set_namespace_state(request, next, at_ms).status
            }
            MetaMutation::SetReservedNames(reserved) => {
                self.apply_set_reserved_names(reserved).status
            }
            MetaMutation::SetShardPreferredLocation(request) => {
                self.apply_pin_shard(request).status
            }
            MetaMutation::SetMetaChangeMuted(muted) => {
                self.apply_set_meta_change_muted(muted).status
            }
            MetaMutation::PutProxyGroup(request) => {
                self.apply_put_proxy_group(request).status
            }
            MetaMutation::DropProxyGroup(request) => {
                self.apply_drop_proxy_group(request).status
            }
            MetaMutation::SetProxyGroup(request) => {
                self.apply_set_proxy_group(request).status
            }
            MetaMutation::SetShardState(request, state) => {
                self.apply_set_shard_state(request, state).status
            }
            MetaMutation::DropShard(request) => self.apply_drop_shard(request).status,
            MetaMutation::FreezeServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Frozen, at_ms)
                    .status
            }
            MetaMutation::DropServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Dropped, at_ms)
                    .status
            }
            MetaMutation::FreezeProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Frozen, at_ms)
                    .status
            }
            MetaMutation::DropProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Dropped, at_ms)
                    .status
            }
            MetaMutation::PurgeMeta(plan) => {
                self.apply_meta_purge(&plan);
                Status::ok()
            }
            MetaMutation::InstallSnapshot(snapshot) => {
                self.apply_install_snapshot(*snapshot).status
            }
        }
    }
}

fn scheduler_finish_generation_key(shard_id: ShardId, server_addr: &str) -> String {
    format!("{shard_id}@{server_addr}")
}

fn table_key(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

fn default_replica_count() -> u64 {
    1
}

fn default_pin_primary() -> bool {
    true
}

fn default_replica_read_policy() -> String {
    "pin_primary".to_string()
}

fn default_max_read_retries() -> u32 {
    1
}

fn default_retry_backoff_ms() -> u64 {
    2
}

fn default_continuous_failed_time_ms() -> u64 {
    10_000
}

fn default_table_io_timeout_ms() -> u64 {
    200
}

fn default_table_connect_timeout_ms() -> u64 {
    200
}

fn validate_serving_options(options: &TableServingOptions) -> Result<(), String> {
    match options.replica_read_policy.as_str() {
        "pin_primary" | "first_replica" | "round_robin_replica" => {}
        _ => {
            return Err(format!(
                "unsupported replica_read_policy {:?}",
                options.replica_read_policy
            ));
        }
    }
    if options.drop_percent > 100 {
        return Err("drop_percent must be <= 100".to_string());
    }
    if options.io_timeout_ms == 0 || options.connect_timeout_ms == 0 {
        return Err("io/connect timeout must be > 0".to_string());
    }
    Ok(())
}

fn proxy_serving_mode_for_state(state: MetaEntityState) -> &'static str {
    match state {
        MetaEntityState::Normal => "serving",
        MetaEntityState::Frozen | MetaEntityState::Dropped => "not_serving",
    }
}

impl TableServingOptionsPatch {
    /// Apply this patch onto `base`, recording which fields it set.
    ///
    /// This is the only place the merge is written. It used to exist twice -- once
    /// here and once in the metaserver's create handler, which rebuilt the same
    /// field-by-field merge by hand and, having no reason to, did not carry over
    /// which fields the caller had actually sent. That is how a table created with
    /// an explicit setting arrived carrying no record of it.
    pub fn onto(&self, base: TableServingOptions) -> TableServingOptions {
        let patch = self;
        let mut options = base;
    let set = |field: TableServingField, options: &mut TableServingOptions| {
        options.set_fields.insert(field.name().to_string());
    };
    if let Some(pin_primary) = patch.pin_primary {
        options.pin_primary = pin_primary;
        set(TableServingField::PinPrimary, &mut options);
    }
    if let Some(replica_read_policy) = &patch.replica_read_policy {
        options.replica_read_policy = replica_read_policy.clone();
        set(TableServingField::ReplicaReadPolicy, &mut options);
    }
    if let Some(preferred_location) = &patch.preferred_location {
        options.preferred_location = preferred_location.clone();
        set(TableServingField::PreferredLocation, &mut options);
    }
    if let Some(drop_percent) = patch.drop_percent {
        options.drop_percent = drop_percent;
        set(TableServingField::DropPercent, &mut options);
    }
    if let Some(max_read_retries) = patch.max_read_retries {
        options.max_read_retries = max_read_retries;
        set(TableServingField::MaxReadRetries, &mut options);
    }
    if let Some(max_write_retries) = patch.max_write_retries {
        options.max_write_retries = max_write_retries;
        set(TableServingField::MaxWriteRetries, &mut options);
    }
    if let Some(retry_backoff_ms) = patch.retry_backoff_ms {
        options.retry_backoff_ms = retry_backoff_ms;
        set(TableServingField::RetryBackoffMs, &mut options);
    }
    if let Some(continuous_failed_time_ms) = patch.continuous_failed_time_ms {
        options.continuous_failed_time_ms = continuous_failed_time_ms;
        set(TableServingField::ContinuousFailedTimeMs, &mut options);
    }
    if let Some(io_timeout_ms) = patch.io_timeout_ms {
        options.io_timeout_ms = io_timeout_ms;
        set(TableServingField::IoTimeoutMs, &mut options);
    }
    if let Some(connect_timeout_ms) = patch.connect_timeout_ms {
        options.connect_timeout_ms = connect_timeout_ms;
        set(TableServingField::ConnectTimeoutMs, &mut options);
    }
    options
    }
}

fn apply_serving_options_patch(
    options: TableServingOptions,
    patch: &TableServingOptionsPatch,
) -> TableServingOptions {
    patch.onto(options)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_heartbeat_summarises_every_list_it_carries() {
        // These five are folded before the metaserver's write lock is taken, so
        // the reader waiting on that lock is not also waiting on a walk of one
        // datanode's whole shard list. Only two of them had any coverage, and
        // the fold is only safe if all five still say what the lists say.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);

        let state_for = |shard_id: u64, serving_state: &str, records: u64, bytes: u64| {
            ServerShardServingState {
                shard_id,
                serving_state: serving_state.to_string(),
                total_records: records as usize,
                storage_bytes: bytes,
                ..ServerShardServingState::default()
            }
        };

        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: vec![
                    ShardLoad {
                        shard_id: 1,
                        key_count: 10,
                        memory_bytes: 100,
                    },
                    ShardLoad {
                        shard_id: 2,
                        key_count: 7,
                        memory_bytes: 250,
                    },
                ],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                // "serving" scores 0 and "failed" scores 3, so the worst is 3 --
                // a max, not a sum, which is the one of the five that would not
                // survive being folded the wrong way.
                shard_states: vec![
                    state_for(1, "serving", 40, 4_000),
                    state_for(2, "failed", 2, 500),
                ],
            })
            .status
            .ok);

        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert_eq!(server.load_key_count, 17);
        assert_eq!(server.load_memory_bytes, 350);
        assert_eq!(server.reported_record_count, 42);
        assert_eq!(server.reported_storage_bytes, 4_500);
        assert_eq!(server.worst_shard_state_penalty, 3);

        // And a later heartbeat replaces them rather than accumulating.
        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count: 1,
                    memory_bytes: 2,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: vec![state_for(1, "serving", 3, 4)],
            })
            .status
            .ok);
        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert_eq!(server.load_key_count, 1);
        assert_eq!(server.load_memory_bytes, 2);
        assert_eq!(server.reported_record_count, 3);
        assert_eq!(server.reported_storage_bytes, 4);
        assert_eq!(server.worst_shard_state_penalty, 0);
    }

    #[test]
    fn metaserver_tracks_servers_heartbeats_and_shard_routes() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "127.0.0.1:18001".to_string(),
                node_id: 1,
                location: "zone-a".to_string(),
                binary_version: "test".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: 7,
                server_addr: "127.0.0.1:18001".to_string(),
            })
            .status
            .ok
        );
        assert_eq!(meta.get(7).location.unwrap().server_addr, "127.0.0.1:18001");
        let heartbeat = meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "127.0.0.1:18001".to_string(),
            boot_time_ms: 11,
            binary_version: "v1".to_string(),
            shard_loads: vec![ShardLoad {
                shard_id: 7,
                key_count: 10,
                memory_bytes: 100,
            }],
            shard_stat_loads: vec![ShardStatLoad {
                shard_id: 7,
                shard_stat_info: crate::control::ShardStatInfo {
                    shard_id: 7,
                    loaded: true,
                    readonly: false,
                    load_version: 11,
                    table_name: "tbl".to_string(),
                    shard_uri: "local://tbl/7".to_string(),
                    start_routing_bucket: 1,
                    end_routing_bucket: 2,
                    total_records: 10,
                    storage_bytes: 100,
                    object_manager: crate::control::ObjectManagerStats {
                        object_count: 10,
                        page_ref_count: 10,
                        dirty_object_count: 1,
                        dirty_bucket_count: 1,
                        routing_bucket_count: 2,
                    },
                },
            }],
            runtime_load: ServerRuntimeLoad {
                queue_depth: 2,
                background_queue_depth: 1,
                queued_shard_count: 1,
                running_shard_count: 1,
                dirty_object_count: 3,
                dirty_shard_count: 1,
                rejected_total: 4,
                rejected_background_total: 1,
                dump_runs: 5,
                compaction_runs: 6,
                gc_runs: 7,
                storage_lifecycle_runs: 8,
                last_meta_topology_version: 12,
                meta_heartbeat_consecutive_failures: 2,
                degraded_reasons: vec!["background_queue_full".to_string()],
                ..ServerRuntimeLoad::default()
            },
            shard_states: vec![ServerShardServingState {
                shard_id: 7,
                serving_state: "serving".to_string(),
                worker_index: 3,
                worker_threads: 4,
                loaded: true,
                readonly: false,
                load_version: 11,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_bucket: 1,
                end_routing_bucket: 2,
                total_records: 10,
                storage_bytes: 100,
                cache_memory_bytes: 64,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 100,
                wal_sequence: 9,
                dirty_object_count: 1,
                dirty_bucket_count: 1,
            }],
        });
        assert!(heartbeat.status.ok);
        assert_eq!(heartbeat.topology_version, meta.stats().topology_version);
        assert_eq!(heartbeat.server_state, "normal");
        let server = meta.list_servers().servers.remove(0);
        assert_eq!(server.shard_loads[0].key_count, 10);
        assert_eq!(server.shard_stat_loads[0].shard_stat_info.table_name, "tbl");
        assert_eq!(server.shard_stat_loads[0].shard_stat_info.storage_bytes, 100);
        assert_eq!(server.runtime_load.queue_depth, 2);
        assert_eq!(server.runtime_load.storage_lifecycle_runs, 8);
        assert_eq!(server.runtime_load.last_meta_topology_version, 12);
        assert_eq!(server.runtime_load.meta_heartbeat_consecutive_failures, 2);
        assert_eq!(
            server.runtime_load.degraded_reasons,
            vec!["background_queue_full"]
        );
        assert_eq!(server.shard_states[0].serving_state, "serving");
        assert_eq!(server.shard_states[0].wal_sequence, 9);
        assert_eq!(meta.stats().server_heartbeat_total, 1);
    }

    #[test]
    fn a_proxy_that_only_registered_is_not_treated_as_capacity() {
        // `is_idle_candidate` is written to keep a proxy that has never reported in out of a
        // group -- "a registration, not capacity". It tested `last_heartbeat_ms != 0`, and
        // registration stamps that clock, so every registered proxy passed and the rule was
        // never in force. A proxy that registered and died was attached to a group and counted
        // as serving capacity until the failure detector caught up.
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "silent".to_string(),
                namespace: "ns".to_string(),
                location: "zone-a".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.put_proxy_group(PutProxyGroupRequest {
                drop_percent: 0,
                group: "front".to_string(),
                namespace: "ns".to_string(),
                location: String::new(),
                instance_num: 1,
            })
            .status
            .ok
        );

        // It has registered and nothing more, so it is not capacity yet.
        let plan = meta.plan_proxy_calibration_now(ProxyCalibrationOptions::default());
        assert!(
            !plan.attach.iter().any(|item| item.proxy_addr == "silent"),
            "a proxy that has only registered must not be attached, got {:?}",
            plan.attach
        );

        // One heartbeat is the difference between a registration and capacity.
        assert!(
            meta.proxy_heartbeat(ProxyHeartbeatRequest {
                boot_time_ms: 1,
                proxy_addr: "silent".to_string(),
                namespace: "ns".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        let plan = meta.plan_proxy_calibration_now(ProxyCalibrationOptions::default());
        assert!(
            plan.attach.iter().any(|item| item.proxy_addr == "silent"),
            "once it has reported in it is capacity and the group should claim it, got {:?}",
            plan.attach
        );
    }

    #[test]
    fn metaserver_preflight_reports_inventory_and_frozen_resources() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "server-a".to_string(),
                node_id: 7,
                location: "zone-a".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "zone-a".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: 1,
                server_addr: "server-a".to_string(),
            })
            .status
            .ok
        );

        let healthy = meta.preflight_report();
        assert!(healthy.status.ok);
        assert_eq!(healthy.normal_servers, 1);
        assert_eq!(healthy.normal_proxies, 1);
        assert_eq!(healthy.shard_routes, 1);
        assert!(healthy.degraded_reasons.is_empty());

        assert!(
            meta.freeze_server(StateChangeRequest {
                reason: FreezeReason::Unspecified,
                endpoint: "server-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );
        assert!(
            meta.freeze_proxy(StateChangeRequest {
                reason: FreezeReason::Unspecified,
                endpoint: "proxy-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );
        let degraded = meta.preflight_report();
        assert!(!degraded.status.ok);
        assert_eq!(degraded.frozen_servers, 1);
        assert_eq!(degraded.frozen_proxies, 1);
        assert!(degraded
            .degraded_reasons
            .contains(&"frozen_servers".to_string()));
        assert!(degraded
            .degraded_reasons
            .contains(&"frozen_proxies".to_string()));
    }

    #[test]
    fn metaserver_freezes_stale_servers_from_heartbeat_age() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "stale".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
        let report = meta.freeze_stale_servers(0);
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers, vec!["stale".to_string()]);
        assert_eq!(
            meta.list_servers().servers[0].state,
            MetaEntityState::Frozen
        );
    }

    #[test]
    fn metaserver_failure_detector_loop_freezes_stale_servers_and_proxies() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "stale-server".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "stale-proxy".to_string(),
            namespace: "ns".to_string(),
            location: "z".to_string(),
            config_version: 1,
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
        let _detector = meta.start_failure_detector_loop(0, 10);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if meta.list_servers().servers[0].state == MetaEntityState::Frozen
                && meta.list_proxies().proxies[0].state == MetaEntityState::Frozen
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("failure detector loop did not freeze stale resources");
    }

    #[test]
    fn metaserver_detects_a_datanode_that_restarted_in_place() {
        // The heartbeats never stop, so nothing in the stale-timeout path notices
        // this. But the restarted node has dropped every shard the metaserver
        // still believes it serves, and reads routed there will all miss.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });

        let heartbeat = |boot_time_ms: u64| ServerHeartbeatRequest {
            server_addr: "node-a".to_string(),
            boot_time_ms,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        };
        let server_state = || {
            meta.list_servers()
                .servers
                .into_iter()
                .find(|server| server.server_addr == "node-a")
                .expect("registered")
        };

        // The first heartbeat anchors the boot time; repeats of it are normal.
        assert!(meta.server_heartbeat(heartbeat(1_000)).status.ok);
        assert_eq!(server_state().reported_boot_time_ms, 1_000);
        assert!(!server_state().reboot_detected);
        assert!(meta.server_heartbeat(heartbeat(1_000)).status.ok);
        assert!(!server_state().reboot_detected);

        // A different boot time means the process restarted underneath us.
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert!(server_state().reboot_detected);
        // The anchor does not follow the new value, so the verdict is sticky
        // rather than resetting itself on the next beat.
        assert_eq!(server_state().reported_boot_time_ms, 1_000);
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert!(server_state().reboot_detected);

        // Re-registering is how a datanode says it is ready to be trusted again.
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(!server_state().reboot_detected);
        assert_eq!(server_state().reported_boot_time_ms, 0);
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert_eq!(server_state().reported_boot_time_ms, 2_000);
        assert!(!server_state().reboot_detected);
    }

    #[test]
    fn a_datanode_that_never_reports_a_boot_time_is_not_flagged() {
        // Older datanodes send 0. Treating that as a changed boot time would
        // convict the entire fleet on upgrade.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        for boot_time_ms in [0, 0, 0] {
            assert!(meta
                .server_heartbeat(ServerHeartbeatRequest {
                    server_addr: "node-a".to_string(),
                    boot_time_ms,
                    binary_version: "v1".to_string(),
                    shard_loads: Vec::new(),
                    shard_stat_loads: Vec::new(),
                    runtime_load: ServerRuntimeLoad::default(),
                    shard_states: Vec::new(),
                })
                .status
                .ok);
        }
        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert!(!server.reboot_detected);
    }

    fn register(meta: &SingleNodeMeta, addr: &str) -> AckResponse {
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: addr.to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        })
    }

    fn server_state(meta: &SingleNodeMeta, addr: &str) -> ServerMetaInfo {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == addr)
            .expect("registered")
    }

    fn joined(meta: &SingleNodeMeta) -> Vec<String> {
        let state = meta.inner.read().expect("meta lock poisoned");
        state
            .topology_events
            .iter()
            .map(|event| format!("{}:{}", event.kind, event.resource))
            .collect()
    }

    #[test]
    fn a_proxy_joining_is_recorded() {
        // A server registering has always been recorded; a proxy registering
        // was not, so a proxy could enter the cluster and leave no trace in the
        // history at all.
        let meta = SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-1:9000".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        });
        assert!(
            joined(&meta).contains(&"register_proxy:proxy:proxy-1:9000".to_string()),
            "a proxy joined and the history did not notice: {:?}",
            joined(&meta)
        );
    }

    #[test]
    fn a_namespace_being_created_is_recorded() {
        // Freezing and dropping a namespace were recorded, but creating one was
        // not -- so the history could show a namespace being frozen with no
        // record of it ever having existed.
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "tenant-a".to_string(),
        });
        assert!(
            joined(&meta).contains(&"add_namespace:namespace:tenant-a".to_string()),
            "a namespace was created and the history did not notice: {:?}",
            joined(&meta)
        );
    }

    #[test]
    fn creating_the_same_namespace_twice_records_once() {
        // The call is idempotent. An event per repeat would advance the
        // topology version for a change that did not happen, and a caller
        // polling the history would see movement where there was none.
        let meta = SingleNodeMeta::default();
        for _ in 0..3 {
            meta.add_namespace(AddNamespaceRequest {
                namespace: "tenant-a".to_string(),
            });
        }
        let recorded = joined(&meta)
            .into_iter()
            .filter(|entry| entry == "add_namespace:namespace:tenant-a")
            .count();
        assert_eq!(recorded, 1, "an idempotent call recorded more than once");
    }

    #[test]
    fn a_proxy_turned_away_is_not_recorded_as_having_joined() {
        // The refusal paths return before the recording. A proxy under
        // conviction that is refused must not appear in the history as having
        // joined, or the history contradicts the state.
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        let register_proxy = || {
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
        };
        let joins = || {
            joined(&meta)
                .into_iter()
                .filter(|entry| entry.starts_with("register_proxy:"))
                .count()
        };
        assert!(register_proxy().status.ok);
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);

        let before = joins();
        assert_eq!(register_proxy().status.code, "conviction_requires_unfreeze");
        assert_eq!(joins(), before, "a refused registration was recorded anyway");
    }

    #[test]
    fn a_freeze_records_why_it_happened() {
        let meta = SingleNodeMeta::default();
        register(&meta, "node-a");
        register(&meta, "node-b");
        std::thread::sleep(std::time::Duration::from_millis(2));

        // The detector's own freeze is a conviction.
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert_eq!(
            server_state(&meta, "node-a").freeze_reason,
            FreezeReason::Unresponsive
        );
        assert!(server_state(&meta, "node-a").freeze_reason.is_conviction());

        // An operator freeze is not.
        assert!(meta.unfreeze_server(unfreeze_request("node-b")).status.ok);
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-b".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Operator,
            })
            .status
            .ok);
        assert_eq!(
            server_state(&meta, "node-b").freeze_reason,
            FreezeReason::Operator
        );
        assert!(!server_state(&meta, "node-b").freeze_reason.is_conviction());
    }

    fn unfreeze_request(addr: &str) -> StateChangeRequest {
        StateChangeRequest {
            endpoint: addr.to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        }
    }

    #[test]
    fn the_default_detector_reports_what_it_freezes() {
        // temporalstore_meta_convicted_total is exported unconditionally, and
        // only the adaptive detector was recording into it. The adaptive one is
        // off unless asked for, so on a default metaserver the counter sat at
        // zero while this detector froze servers and proxies -- a confident
        // wrong number, which reads worse than an absent series.
        let meta = SingleNodeMeta::default();
        register(&meta, "server-a");
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        std::thread::sleep(std::time::Duration::from_millis(2));

        let report = meta.freeze_stale_resources(0);
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers.len(), 1, "{report:?}");
        assert_eq!(report.frozen_proxies.len(), 1, "{report:?}");

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"server\"} 1"),
            "the detector froze a server and the counter did not move:\n{exported}"
        );
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"proxy\"} 1"),
            "the detector froze a proxy and the counter did not move:\n{exported}"
        );
    }

    #[test]
    fn each_tier_counts_only_its_own_freezes() {
        // `record_conviction` sums both lists, so one call carrying servers and
        // proxies together would count every freeze under both tiers. The
        // counts are deliberately asymmetric -- two servers, one proxy -- so
        // any cross-contamination shows up as a wrong number rather than a
        // coincidentally equal one.
        let meta = SingleNodeMeta::default();
        register(&meta, "server-a");
        register(&meta, "server-b");
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"server\"} 2"),
            "two servers frozen must count two under server:
{exported}"
        );
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"proxy\"} 1"),
            "one proxy frozen must count one under proxy:
{exported}"
        );
    }

    #[test]
    fn a_convicted_server_cannot_register_its_way_back_into_service() {
        // Without this the freeze cooldown is the only guard, and it defaults to
        // zero -- so the convicted node clears the metaserver's decision simply
        // by asking, and conviction is advisory.
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        register(&meta, "node-a");
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);

        let rejected = register(&meta, "node-a");
        assert!(!rejected.status.ok);
        assert_eq!(rejected.status.code, "conviction_requires_unfreeze");
        assert_eq!(
            server_state(&meta, "node-a").state,
            MetaEntityState::Frozen
        );

        // An operator unfreeze is the way back, and afterwards the node
        // registers normally again.
        assert!(meta.unfreeze_server(unfreeze_request("node-a")).status.ok);
        assert_eq!(server_state(&meta, "node-a").state, MetaEntityState::Normal);
        assert_eq!(
            server_state(&meta, "node-a").freeze_reason,
            FreezeReason::Unspecified
        );
        assert!(register(&meta, "node-a").status.ok);
    }

    #[test]
    fn without_the_lock_a_convicted_server_still_recovers_by_registering() {
        // The default path is unchanged: this is the automatic recovery that
        // deployments running a zero freeze cooldown depend on.
        let meta = SingleNodeMeta::default();
        register(&meta, "node-a");
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert!(register(&meta, "node-a").status.ok);
        assert_eq!(server_state(&meta, "node-a").state, MetaEntityState::Normal);
    }

    #[test]
    fn an_operator_freeze_is_not_locked_against_re_registration() {
        // The lock is about the metaserver's own verdicts. A maintenance freeze
        // already has an operator in the loop, and the freeze cooldown is the
        // knob for holding a node out.
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        register(&meta, "node-a");
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-a".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Operator,
            })
            .status
            .ok);
        assert!(register(&meta, "node-a").status.ok);
    }

    #[test]
    fn a_restarted_server_is_locked_out_under_its_own_reason() {
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        register(&meta, "node-a");
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-a".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Restarted,
            })
            .status
            .ok);
        let rejected = register(&meta, "node-a");
        assert_eq!(rejected.status.code, "conviction_requires_unfreeze");
        assert!(rejected.status.message.contains("restarted"));
    }

    #[test]
    fn a_convicted_proxy_is_locked_out_too() {
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        let register_proxy = || {
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
        };
        register_proxy();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert_eq!(
            register_proxy().status.code,
            "conviction_requires_unfreeze"
        );
        assert!(meta.unfreeze_proxy(unfreeze_request("proxy-a")).status.ok);
        assert!(register_proxy().status.ok);
    }

    #[test]
    fn an_unfreeze_survives_mutation_log_replay() {
        // Unfreeze was previously the one state change that recorded no
        // mutation, so recovery would have silently re-frozen the resource.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("unfreeze-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            register(&meta, "node-a");
            std::thread::sleep(std::time::Duration::from_millis(2));
            assert!(meta.freeze_stale_resources(0).status.ok);
            assert!(meta.unfreeze_server(unfreeze_request("node-a")).status.ok);
            assert_eq!(server_state(&meta, "node-a").state, MetaEntityState::Normal);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            server_state(&recovered, "node-a").state,
            MetaEntityState::Normal
        );
        assert_eq!(
            server_state(&recovered, "node-a").freeze_reason,
            FreezeReason::Unspecified
        );
    }

    fn one_rack_cluster(meta: &SingleNodeMeta, servers: &[&str], replica_count: u64) -> Vec<String> {
        for (index, addr) in servers.iter().enumerate() {
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: addr.to_string(),
                    node_id: index as u64 + 1,
                    // One rack: the ladder can separate nothing, so every
                    // replica after the first comes from the fallback fill.
                    location: "us-east/dc1/az1/rack1".to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
        }
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "t".to_string(),
                first_shard_id: 700,
                shard_count: 1,
                replica_count,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);
        meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "t".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        })
        .shards
        .into_iter()
        .next()
        .expect("one shard")
        .replicas
    }

    #[test]
    fn a_shard_does_not_stack_its_replicas_on_one_host() {
        // Four datanodes, two per physical host, all in one rack -- an ordinary
        // small deployment. Both replicas landed on host-a while host-b sat
        // idle: the shard reported two replicas and lost both to one host.
        let meta = SingleNodeMeta::default();
        let replicas = one_rack_cluster(
            &meta,
            &["host-a:1001", "host-a:1002", "host-b:1001", "host-b:1002"],
            2,
        );
        let hosts: BTreeSet<String> = replicas
            .iter()
            .map(|addr| super::topology_helpers::server_host(addr))
            .collect();
        assert_eq!(replicas.len(), 2, "{replicas:?}");
        assert_eq!(
            hosts.len(),
            2,
            "both replicas share a host while another was free: {replicas:?}"
        );
    }

    #[test]
    fn a_host_is_reused_only_when_there_is_no_other() {
        // The point is to spread, not to refuse. With one host there is nothing
        // to spread across, and the shard must still reach its replica count
        // rather than come back short.
        let meta = SingleNodeMeta::default();
        let replicas = one_rack_cluster(&meta, &["host-a:1001", "host-a:1002"], 2);
        assert_eq!(
            replicas.len(),
            2,
            "spreading turned a fill into a shortfall: {replicas:?}"
        );
    }

    #[test]
    fn replicas_spread_across_availability_units_not_just_racks() {
        // Four servers, two availability units, two racks each. Comparing whole
        // location strings makes rack1 and rack2 of az1 look like "different
        // locations", so both replicas of a shard land inside az1 and losing
        // that unit loses both. Placement must reach for az2 instead.
        let meta = SingleNodeMeta::default();
        for (addr, location) in [
            ("a1", "us-east/dc1/az1/rack1"),
            ("a2", "us-east/dc1/az1/rack2"),
            ("b1", "us-east/dc1/az2/rack1"),
            ("b2", "us-east/dc1/az2/rack2"),
        ] {
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: addr.to_string(),
                    node_id: 1,
                    location: location.to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 2,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
        });
        assert!(topology.status.ok);
        assert_eq!(topology.shards.len(), 1);
        let replicas = &topology.shards[0].replicas;
        assert_eq!(replicas.len(), 2, "expected two replicas, got {replicas:?}");

        let units = replicas
            .iter()
            .map(|addr| {
                let server = meta
                    .list_servers()
                    .servers
                    .into_iter()
                    .find(|server| &server.server_addr == addr)
                    .expect("replica is a registered server");
                Location::parse(&server.location).ancestor(3).to_path()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            units.len(),
            2,
            "both replicas landed in the same availability unit: {units:?}"
        );
    }

    fn relabel_meta(servers: &[(&str, &str)]) -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (addr, location) in servers {
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: addr.to_string(),
                    node_id: 1,
                    location: location.to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
        }
        meta
    }

    fn located(meta: &SingleNodeMeta, addr: &str) -> ServerMetaInfo {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == addr)
            .expect("registered")
    }

    #[test]
    fn relabelling_changes_the_location_and_nothing_else() {
        // The whole reason this exists rather than re-registering: a
        // re-registration resets heartbeat state, reported shards and runtime
        // load, which is a disruptive way to fix a label.
        let meta = relabel_meta(&[("node-a", "us-east/dc1/az1/rack1")]);
        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 4_242,
                binary_version: "v9".to_string(),
                shard_loads: vec![ShardLoad {
                    shard_id: 7,
                    key_count: 11,
                    memory_bytes: 22,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad {
                    queue_depth: 3,
                    ..ServerRuntimeLoad::default()
                },
                shard_states: Vec::new(),
            })
            .status
            .ok);
        let before = located(&meta, "node-a");

        assert!(meta
            .update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "us-east/dc1/az2/rack9".to_string(),
            })
            .status
            .ok);

        let after = located(&meta, "node-a");
        assert_eq!(after.location, "us-east/dc1/az2/rack9");
        assert_eq!(after.last_heartbeat_ms, before.last_heartbeat_ms);
        assert_eq!(after.boot_time_ms, 4_242);
        assert_eq!(after.binary_version, "v9");
        assert_eq!(after.shard_loads, before.shard_loads);
        assert_eq!(after.runtime_load.queue_depth, 3);
        assert_eq!(after.state, MetaEntityState::Normal);
    }

    #[test]
    fn relabelling_bumps_the_topology_version() {
        // Placement is derived from location on every topology read, so without
        // this bump clients keep serving from the placement the old label implied.
        let meta = relabel_meta(&[("node-a", "rack-1")]);
        let before = meta.stats().topology_version;
        assert!(meta
            .update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "rack-2".to_string(),
            })
            .status
            .ok);
        assert!(meta.stats().topology_version > before);
    }

    #[test]
    fn relabelling_moves_where_replicas_land() {
        // The payoff. Four servers crammed into one availability unit can only
        // spread replicas across racks; relabelling one into a second unit lets
        // placement reach for the wider split.
        let meta = relabel_meta(&[
            ("a1", "us-east/dc1/az1/rack1"),
            ("a2", "us-east/dc1/az1/rack2"),
            ("a3", "us-east/dc1/az1/rack3"),
            ("a4", "us-east/dc1/az1/rack4"),
        ]);
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 2,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);

        let units = |meta: &SingleNodeMeta| {
            let topology = meta.get_table_topology(GetTableTopologyRequest {
                client_location: String::new(),
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                old_topology_version: 0,
            });
            topology.shards[0]
                .replicas
                .iter()
                .map(|addr| Location::parse(&located(meta, addr).location).ancestor(3).to_path())
                .collect::<std::collections::BTreeSet<_>>()
        };

        // Everything is in az1, so both replicas must share it.
        assert_eq!(units(&meta).len(), 1);

        assert!(meta
            .update_server(UpdateServerRequest {
                server_addr: "a2".to_string(),
                location: "us-east/dc1/az2/rack1".to_string(),
            })
            .status
            .ok);

        // Now the shard can span two units, and does.
        assert_eq!(units(&meta).len(), 2);
    }

    #[test]
    fn relabelling_rejects_an_unknown_or_frozen_server() {
        let meta = relabel_meta(&[("node-a", "rack-1")]);
        assert_eq!(
            meta.update_server(UpdateServerRequest {
                server_addr: "ghost".to_string(),
                location: "rack-2".to_string(),
            })
            .status
            .code,
            "server_not_found"
        );

        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert_eq!(
            meta.update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "rack-2".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );
    }

    #[test]
    fn relabelling_to_the_same_location_is_not_modified() {
        // So a config-reconciler running this on a loop does not bump the
        // topology version on every pass and invalidate every client's cache.
        let meta = relabel_meta(&[("node-a", "rack-1")]);
        let before = meta.stats().topology_version;
        assert_eq!(
            meta.update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "rack-1".to_string(),
            })
            .status
            .code,
            "not_modified"
        );
        assert_eq!(meta.stats().topology_version, before);
    }

    #[test]
    fn a_relabel_survives_mutation_log_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("relabel-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            });
            assert!(meta
                .update_server(UpdateServerRequest {
                    server_addr: "node-a".to_string(),
                    location: "rack-9".to_string(),
                })
                .status
                .ok);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(located(&recovered, "node-a").location, "rack-9");
    }

    fn stop_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        });
        meta
    }

    fn stop(endpoint: &str) -> NotifyStopRequest {
        NotifyStopRequest {
            endpoint: endpoint.to_string(),
        }
    }

    fn stopped_server(meta: &SingleNodeMeta) -> ServerMetaInfo {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered")
    }

    #[test]
    fn announcing_a_stop_beats_the_failure_detector() {
        // The point of the whole thing. A clean shutdown is otherwise
        // indistinguishable from a crash: the node stops heartbeating and the
        // metaserver waits out the detection window, routing reads to a process
        // that has already exited.
        let meta = stop_meta();

        // The detector is not going to help: the heartbeat is fresh, so a sweep
        // with a realistic window leaves the node serving.
        assert!(meta.freeze_stale_resources(30_000).status.ok);
        assert_eq!(stopped_server(&meta).state, MetaEntityState::Normal);

        // Announcing the stop takes it out immediately.
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        let server = stopped_server(&meta);
        assert_eq!(server.state, MetaEntityState::Frozen);
        assert_eq!(server.freeze_reason, FreezeReason::Stopping);
    }

    #[test]
    fn a_stopping_server_is_not_a_conviction_and_can_come_back() {
        // A node that announced its own shutdown is expected back. Treating the
        // freeze as a conviction would turn every clean restart into an operator
        // ticket under the self-clearing lock.
        assert!(!FreezeReason::Stopping.is_conviction());

        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);

        // It restarts and registers again; the lock does not stand in its way.
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v2".to_string(),
            })
            .status
            .ok);
        assert_eq!(stopped_server(&meta).state, MetaEntityState::Normal);
    }

    #[test]
    fn announcing_a_stop_drops_a_proxy_rather_than_freezing_it() {
        // A proxy holds no data, so there is nothing to preserve by keeping a
        // frozen tombstone -- and a frozen proxy stays in the damage accounting
        // the conviction gate reads.
        let meta = stop_meta();
        assert!(meta.notify_proxy_stop(stop("proxy-a")).status.ok);
        let proxy = meta
            .list_proxies()
            .proxies
            .into_iter()
            .find(|proxy| proxy.proxy_addr == "proxy-a")
            .expect("registered");
        assert_eq!(proxy.state, MetaEntityState::Dropped);
    }

    #[test]
    fn announcing_a_stop_twice_is_not_an_error() {
        // A shutdown hook may retry, or race the failure detector. Neither
        // should produce a spurious failure in a node's last log line.
        let meta = stop_meta();
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        assert_eq!(
            meta.notify_server_stop(stop("node-a")).status.code,
            "not_modified"
        );
        assert!(meta.notify_proxy_stop(stop("proxy-a")).status.ok);
        assert_eq!(
            meta.notify_proxy_stop(stop("proxy-a")).status.code,
            "not_modified"
        );
    }

    #[test]
    fn announcing_a_stop_for_an_unknown_endpoint_is_rejected() {
        let meta = stop_meta();
        assert_eq!(
            meta.notify_server_stop(stop("ghost")).status.code,
            "server_not_found"
        );
        assert_eq!(
            meta.notify_proxy_stop(stop("ghost")).status.code,
            "proxy_not_found"
        );
    }

    #[test]
    fn a_stop_leaves_no_freeze_cooldown_behind() {
        // The node is coming back; a cooldown would delay its return for no
        // reason, since nothing about it was judged unhealthy.
        let meta = stop_meta();
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        // A zero cooldown is stored as "expires now" rather than as zero, so
        // assert the behaviour that matters: the window is not in the future,
        // and the node can register again the moment it comes back.
        assert!(stopped_server(&meta).freeze_cooldown_until_ms <= now_ms());
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v2".to_string(),
            })
            .status
            .ok);
    }

    #[test]
    fn a_stop_survives_mutation_log_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("stop-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            });
            assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        let server = recovered
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert_eq!(server.state, MetaEntityState::Frozen);
        assert_eq!(server.freeze_reason, FreezeReason::Stopping);
    }

    fn muted_meta_with_a_server() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(meta.set_meta_change_muted(true).status.ok);
        meta
    }

    #[test]
    fn muting_refuses_every_recorded_mutation() {
        let meta = muted_meta_with_a_server();
        assert!(meta.is_meta_change_muted());

        for (what, status) in [
            (
                "freeze_server",
                meta.freeze_server(StateChangeRequest {
                    endpoint: "node-a".to_string(),
                    freeze_cooldown_ms: 0,
                    reason: FreezeReason::Operator,
                })
                .status,
            ),
            (
                "add_table",
                meta.add_table(AddTableRequest {
                    namespace: "ns".to_string(),
                    table_name: "orders".to_string(),
                    first_shard_id: 1,
                    shard_count: 1,
                    replica_count: 1,
                    partition_version: 0,
                    serving_options: TableServingOptions::default(),
                })
                .status,
            ),
            (
                "register_server",
                meta.register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: "node-b".to_string(),
                    node_id: 2,
                    location: "rack-1".to_string(),
                    binary_version: "v1".to_string(),
                })
                .status,
            ),
            (
                "reassign_shard",
                meta.reassign_shard(1, "node-a").status,
            ),
        ] {
            assert!(!status.ok, "{what} should have been refused");
            assert_eq!(status.code, "meta_change_muted", "{what}");
        }

        // Nothing landed: the fleet is exactly as it was.
        assert_eq!(meta.list_servers().servers.len(), 1);
        assert_eq!(
            meta.list_servers().servers[0].state,
            MetaEntityState::Normal
        );
        assert!(meta.list_tables().tables.is_empty());
    }

    #[test]
    fn muting_does_not_touch_reads_or_heartbeats() {
        // The mute gates recorded mutations. A heartbeat records none -- it is
        // liveness, not a metadata change -- and muting it would blind the
        // failure detector to the very fleet the operator is inspecting.
        let meta = muted_meta_with_a_server();
        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: Vec::new(),
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            })
            .status
            .ok);
        assert!(meta.list_servers().status.ok);
        assert!(meta.list_namespaces().status.ok);
        assert!(meta.stats().shard_count == 0);
    }

    #[test]
    fn a_muted_metaserver_will_not_admit_a_restarting_datanode() {
        // The sharp edge, asserted so it is a decision rather than a surprise:
        // registration is a recorded mutation, so while muted a node that
        // restarts cannot rejoin until an operator resumes.
        let meta = muted_meta_with_a_server();
        let rejoin = meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v2".to_string(),
        });
        assert_eq!(rejoin.status.code, "meta_change_muted");
    }

    #[test]
    fn resume_is_never_itself_muted() {
        // An operator must always be able to undo this, or the lever is a trap.
        let meta = muted_meta_with_a_server();
        assert!(meta.set_meta_change_muted(false).status.ok);
        assert!(!meta.is_meta_change_muted());
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-a".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Operator,
            })
            .status
            .ok);
    }

    #[test]
    fn meta_info_reports_whether_change_is_muted() {
        let meta = SingleNodeMeta::default();
        assert!(!meta.info().meta_change_muted);
        meta.set_meta_change_muted(true);
        assert!(meta.info().meta_change_muted);
    }

    #[test]
    fn a_mute_survives_mutation_log_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("mute-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            });
            assert!(meta.set_meta_change_muted(true).status.ok);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert!(recovered.is_meta_change_muted());
        // The registration recorded before the mute still replayed: the guard
        // applies to live calls, not to replaying a log of accepted changes.
        assert_eq!(recovered.list_servers().servers.len(), 1);
    }

    fn meta_with_history() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        register(&meta, "server-a");
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        assert!(
            !meta
                .inner
                .read()
                .expect("meta lock poisoned")
                .topology_events
                .is_empty(),
            "the fixture recorded no history to lose"
        );
        meta
    }

    #[test]
    fn the_change_history_survives_a_snapshot_install() {
        // The topology version travelled and the history it belongs to did not,
        // so a peer inherited a version with nothing behind it.
        let meta = meta_with_history();
        let expected = meta
            .inner
            .read()
            .expect("meta lock poisoned")
            .topology_events
            .clone();

        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);

        let carried = peer
            .inner
            .read()
            .expect("meta lock poisoned")
            .topology_events
            .clone();
        assert_eq!(carried, expected, "the change history was dropped on install");
    }

    #[test]
    fn a_peer_that_installs_a_snapshot_is_not_reported_as_missing_its_history() {
        // The readiness report treats the history as evidence, so dropping it on
        // install made a healthy peer report its own control plane as blocked on
        // evidence it had a moment earlier.
        let meta = meta_with_history();
        assert!(meta.control_plane_parity_report().topology_history_ready);

        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);

        let report = peer.control_plane_parity_report();
        assert!(
            report.topology_history_ready,
            "a peer reported its history missing right after inheriting it"
        );
        assert!(
            !report
                .blockers
                .iter()
                .any(|blocker| blocker.contains("topology history")),
            "{:?}",
            report.blockers
        );
    }

    #[test]
    fn a_snapshot_written_before_this_field_still_installs() {
        // The field is defaulted, so an older snapshot on disk -- or from a peer
        // that has not been upgraded -- must still load rather than be rejected
        // as malformed.
        let meta = meta_with_history();
        let mut encoded: serde_json::Value =
            serde_json::to_value(meta.export_snapshot()).expect("snapshot encodes");
        encoded
            .as_object_mut()
            .expect("snapshot is an object")
            .remove("topology_events")
            .expect("the field was there to remove");

        let older: MetaSnapshot = serde_json::from_value(encoded).expect("an older snapshot loads");
        assert!(older.topology_events.is_empty());
        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(older).status.ok);
    }

    #[test]
    fn a_mute_survives_a_snapshot_round_trip() {
        // Otherwise a peer installing the snapshot quietly resumes the change an
        // operator stopped.
        let meta = SingleNodeMeta::default();
        meta.set_meta_change_muted(true);
        let snapshot = meta.export_snapshot();
        assert!(snapshot.meta_change_muted);

        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(snapshot).status.ok);
        assert!(restored.is_meta_change_muted());
    }

    /// One table, one shard, registered to node-a, with node-b also live.
    fn owned_shard_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (addr, node_id, location) in [("node-a", 1, "rack-1"), ("node-b", 2, "rack-2")] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: addr.to_string(),
                node_id,
                location: location.to_string(),
                binary_version: "v1".to_string(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-a".to_string(),
        });
        meta
    }

    fn routed_shard(meta: &SingleNodeMeta) -> TableShard {
        meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
        })
        .shards
        .into_iter()
        .next()
        .expect("one shard")
    }

    fn state_change(endpoint: &str) -> StateChangeRequest {
        StateChangeRequest {
            endpoint: endpoint.to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        }
    }

    #[test]
    fn a_shard_is_not_routed_to_a_frozen_owner() {
        // Placement already refuses to pick a server that is not Normal. The
        // recorded owner is the one entry that reaches the topology without
        // passing that filter, so freezing a server left every shard it owns
        // pointing straight at it.
        let meta = owned_shard_meta();
        assert_eq!(routed_shard(&meta).primary, Some("node-a".to_string()));

        assert!(meta.freeze_server(state_change("node-a")).status.ok);
        let shard = routed_shard(&meta);
        assert_eq!(shard.primary, None, "a frozen server is still being routed to");
        assert!(
            !shard.replicas.contains(&"node-a".to_string()),
            "a frozen server is still listed as a replica"
        );
    }

    #[test]
    fn a_shard_is_not_routed_to_a_dropped_owner() {
        // A dropped server is not coming back, so naming it is simply false.
        let meta = owned_shard_meta();
        assert!(meta.drop_server(state_change("node-a")).status.ok);
        assert_eq!(routed_shard(&meta).primary, None);
    }

    #[test]
    fn unfreezing_the_owner_puts_the_shard_back() {
        let meta = owned_shard_meta();
        assert!(meta.freeze_server(state_change("node-a")).status.ok);
        assert_eq!(routed_shard(&meta).primary, None);
        assert!(meta.unfreeze_server(state_change("node-a")).status.ok);
        assert_eq!(routed_shard(&meta).primary, Some("node-a".to_string()));
    }

    #[test]
    fn a_stale_route_does_not_get_a_stand_in_primary() {
        // The candidate scan would happily nominate node-b, which has never
        // loaded this shard: a client that followed the nomination would read
        // an empty shard and believe it.
        let meta = owned_shard_meta();
        assert!(meta.freeze_server(state_change("node-a")).status.ok);
        let shard = routed_shard(&meta);
        assert_ne!(shard.primary, Some("node-b".to_string()));
        assert_eq!(shard.primary, None);
    }

    #[test]
    fn a_shard_with_no_owner_yet_still_gets_a_proposed_placement() {
        // Guards the other direction: a table that nothing has registered
        // against must still be told where its shards should go.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        assert_eq!(routed_shard(&meta).primary, Some("node-a".to_string()));
    }

    #[test]
    fn a_route_naming_a_server_the_metaserver_has_never_heard_of_is_kept() {
        // A route can outlive the server record it names. Treating an unknown
        // address as out of service would unroute shards the metaserver has
        // simply not been told about yet.
        let meta = owned_shard_meta();
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-ghost".to_string(),
        });
        assert_eq!(routed_shard(&meta).primary, Some("node-ghost".to_string()));
    }

    /// One shard replicated across three zones, owned in zone-a. The servers are
    /// registered a-b-c so the load-ordered scan produces that order, which is
    /// what a caller sees today no matter where it is.
    fn three_zone_shard() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (index, (addr, zone)) in [
            ("node-a", "east/zone-a"),
            ("node-b", "east/zone-b"),
            ("node-c", "west/zone-c"),
        ]
        .into_iter()
        .enumerate()
        {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: addr.to_string(),
                node_id: index as u64 + 1,
                location: zone.to_string(),
                binary_version: "v1".to_string(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 3,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-a".to_string(),
        });
        meta
    }

    fn topology_for(meta: &SingleNodeMeta, client_location: &str) -> TableShard {
        meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
            client_location: client_location.to_string(),
        })
        .shards
        .into_iter()
        .next()
        .expect("one shard")
    }

    #[test]
    fn a_caller_is_offered_the_replica_in_its_own_zone_first() {
        // Replicas are deliberately spread as far apart as the topology allows,
        // so most of a shard's replicas are far from any given caller by
        // construction. Without this the caller reads from whichever server
        // sorted first on load, which is a coin flip against crossing zones.
        let meta = three_zone_shard();
        // The order without a caller location is not accidental: the placement
        // scan spreads replicas as widely as the topology allows, so the *far*
        // replica is offered second. That is exactly the behaviour that makes a
        // location-blind read expensive.
        assert_eq!(
            topology_for(&meta, "").replicas,
            vec![
                "node-a".to_string(),
                "node-c".to_string(),
                "node-b".to_string()
            ]
        );
        assert_eq!(
            topology_for(&meta, "east/zone-b").replicas.first(),
            Some(&"node-b".to_string())
        );
        assert_eq!(
            topology_for(&meta, "west/zone-c").replicas.first(),
            Some(&"node-c".to_string())
        );
    }

    #[test]
    fn a_caller_with_no_replica_in_its_zone_still_prefers_the_nearer_half() {
        // Locations are hierarchical, so "no replica here" is not the end of the
        // question: a caller in east/zone-d shares `east` with two of the three.
        let meta = three_zone_shard();
        let replicas = topology_for(&meta, "east/zone-d").replicas;
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas[2], "node-c", "the far replica should sort last");
    }

    #[test]
    fn ordering_replicas_never_moves_the_primary() {
        // The primary is where the shard is actually owned. Reordering is about
        // which copy to read, and must not change who owns it.
        let meta = three_zone_shard();
        for caller in ["", "east/zone-b", "west/zone-c"] {
            assert_eq!(
                topology_for(&meta, caller).primary,
                Some("node-a".to_string()),
                "caller {caller:?} was given a different primary"
            );
        }
    }

    #[test]
    fn the_endpoints_stay_lined_up_with_the_replicas() {
        // The two lists are positional. Reordering one without the other would
        // hand every caller the wrong address for every replica.
        let meta = three_zone_shard();
        let shard = topology_for(&meta, "west/zone-c");
        let addrs = shard
            .replica_endpoints
            .iter()
            .map(|endpoint| endpoint.server_addr.clone())
            .collect::<Vec<_>>();
        assert_eq!(shard.replicas, addrs);
    }

    #[test]
    fn a_caller_that_says_nothing_sees_exactly_what_it_saw_before() {
        // The whole change is opt-in on a field the caller may not send.
        let meta = three_zone_shard();
        let quiet = topology_for(&meta, "");
        let unknown = topology_for(&meta, "somewhere/else");
        assert_eq!(quiet.replicas, unknown.replicas);
        assert_eq!(quiet.primary, unknown.primary);
    }

    #[test]
    fn callers_equally_close_keep_the_order_the_load_scan_chose() {
        // The sort is stable on purpose: among servers the caller cannot tell
        // apart, the placement scan's load ordering is still the better answer.
        let meta = three_zone_shard();
        let replicas = topology_for(&meta, "east/zone-d").replicas;
        assert_eq!(&replicas[..2], &["node-a".to_string(), "node-b".to_string()]);
    }

    /// One table of four shards spread over two servers, plus one shard that no
    /// table claims.
    fn placed_shards() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (index, addr) in ["node-a", "node-b"].into_iter().enumerate() {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                server_addr: addr.to_string(),
                node_id: index as u64 + 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 4,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        for shard_id in 1..=4 {
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id,
                server_addr: if shard_id % 2 == 1 { "node-a" } else { "node-b" }.to_string(),
            });
        }
        // Registered, but inside no table's id range.
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 99,
            server_addr: "node-a".to_string(),
        });
        meta
    }

    fn listed(meta: &SingleNodeMeta, request: ListShardsRequest) -> ListShardsResponse {
        let response = meta.list_shards(request);
        assert!(response.status.ok, "{response:?}");
        response
    }

    #[test]
    fn shard_placement_can_be_listed_at_all() {
        // Servers, proxies, proxy groups, namespaces and tables were all
        // listable. Shards -- the thing the metaserver exists to place -- could
        // only be fetched one id at a time.
        let meta = placed_shards();
        let response = listed(&meta, ListShardsRequest::default());
        assert_eq!(
            response
                .shards
                .iter()
                .map(|entry| entry.shard_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 99],
            "shards should come back in id order"
        );
        assert_eq!(response.next_after_shard_id, None);
    }

    #[test]
    fn a_listed_shard_names_the_table_that_claims_it() {
        let meta = placed_shards();
        let response = listed(&meta, ListShardsRequest::default());
        let first = &response.shards[0];
        assert_eq!(first.namespace, "ns");
        assert_eq!(first.table_name, "orders");
        assert_eq!(first.server_addr, "node-a");
    }

    #[test]
    fn a_shard_no_table_claims_is_listed_rather_than_hidden() {
        // Answering "which shards is nothing serving?" is most of the reason to
        // want this listing, so an unclaimed shard must appear, with empty table
        // fields rather than being dropped from the page.
        let meta = placed_shards();
        let response = listed(&meta, ListShardsRequest::default());
        let orphan = response
            .shards
            .iter()
            .find(|entry| entry.shard_id == 99)
            .expect("the unclaimed shard should be listed");
        assert_eq!(orphan.namespace, "");
        assert_eq!(orphan.table_name, "");
        assert_eq!(orphan.server_addr, "node-a");
    }

    #[test]
    fn shards_can_be_listed_for_one_server() {
        // The operational question is usually "what is on this node?".
        let meta = placed_shards();
        let response = listed(
            &meta,
            ListShardsRequest {
                server_addr: "node-b".to_string(),
                ..ListShardsRequest::default()
            },
        );
        assert_eq!(
            response
                .shards
                .iter()
                .map(|entry| entry.shard_id)
                .collect::<Vec<_>>(),
            vec![2, 4]
        );
        assert!(response
            .shards
            .iter()
            .all(|entry| entry.server_addr == "node-b"));
    }

    #[test]
    fn a_full_page_says_where_to_resume() {
        // A cap that truncates silently reads as "that is all of them", which is
        // the wrong answer to give an operator counting shards.
        let meta = placed_shards();
        let first = listed(
            &meta,
            ListShardsRequest {
                limit: 2,
                ..ListShardsRequest::default()
            },
        );
        assert_eq!(
            first
                .shards
                .iter()
                .map(|entry| entry.shard_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(first.next_after_shard_id, Some(2));

        let second = listed(
            &meta,
            ListShardsRequest {
                limit: 2,
                after_shard_id: first.next_after_shard_id.unwrap(),
                ..ListShardsRequest::default()
            },
        );
        assert_eq!(
            second
                .shards
                .iter()
                .map(|entry| entry.shard_id)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn the_last_page_does_not_offer_a_cursor() {
        // Otherwise a caller loops forever on an empty page.
        let meta = placed_shards();
        let page = listed(
            &meta,
            ListShardsRequest {
                limit: 2,
                after_shard_id: 4,
                ..ListShardsRequest::default()
            },
        );
        assert_eq!(
            page.shards
                .iter()
                .map(|entry| entry.shard_id)
                .collect::<Vec<_>>(),
            vec![99]
        );
        assert_eq!(page.next_after_shard_id, None);
    }

    #[test]
    fn a_listing_cannot_be_asked_for_more_than_the_cap() {
        // The cap is what stops one request returning the whole placement table.
        let meta = placed_shards();
        let response = listed(
            &meta,
            ListShardsRequest {
                limit: usize::MAX,
                ..ListShardsRequest::default()
            },
        );
        assert!(response.shards.len() <= LIST_SHARDS_DEFAULT_LIMIT);
    }

    #[test]
    fn a_listed_shard_carries_its_latest_snapshot() {
        let meta = placed_shards();
        let snapshot = ShardSnapshotRef {
            uri: "s3://cluster/shards/1/manifest.json".to_string(),
            checksum: "sha256:abc".to_string(),
            byte_size: 2048,
            last_log_index: 77,
            created_at_ms: 1_000,
        };
        assert!(
            meta.publish_shard_snapshot(PublishShardSnapshotRequest {
                shard_id: 1,
                snapshot: snapshot.clone(),
            })
            .status
            .ok
        );
        let response = listed(&meta, ListShardsRequest::default());
        assert_eq!(response.shards[0].latest_snapshot, Some(snapshot));
    }

    #[test]
    fn spread_still_holds_across_a_fleet_big_enough_to_misalign() {
        // The scan now indexes into the locations parsed once up front instead
        // of re-parsing each candidate. That is only correct while the two
        // vectors stay in step, so this uses a fleet large enough that an
        // off-by-one would hand a shard two replicas in the same domain.
        let meta = SingleNodeMeta::default();
        for index in 0..12u64 {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                server_addr: format!("node-{index:02}"),
                node_id: index + 1,
                location: format!("region-{}/zone-{}", index % 3, index),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 8,
            replica_count: 3,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        });
        assert_eq!(topology.shards.len(), 8);
        for shard in &topology.shards {
            assert_eq!(shard.replicas.len(), 3, "shard {} short", shard.shard_id);
            let regions = shard
                .replicas
                .iter()
                .map(|addr| {
                    let index: u64 = addr.trim_start_matches("node-").parse().unwrap();
                    index % 3
                })
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                regions.len(),
                3,
                "shard {} put two replicas in one region: {:?}",
                shard.shard_id,
                shard.replicas
            );
        }
    }

    #[test]
    fn the_caller_ordering_is_unchanged_by_computing_distance_once() {
        // The nearest-first sort now works the distance out per replica instead
        // of inside the comparator. Same order, including the stable tie-break.
        let meta = SingleNodeMeta::default();
        for (index, location) in ["east/a", "east/b", "west/c", "east/d"].into_iter().enumerate() {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                server_addr: format!("node-{index}"),
                node_id: index as u64 + 1,
                location: location.to_string(),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 4,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        let ask = |client_location: &str| {
            meta.get_table_topology(GetTableTopologyRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                old_topology_version: 0,
                client_location: client_location.to_string(),
            })
            .shards[0]
                .replicas
                .clone()
        };

        let far = ask("west/c");
        assert_eq!(far.first().map(String::as_str), Some("node-2"));

        // Three servers share `east`; among them the order the scan produced
        // must survive the sort rather than being reshuffled.
        let near = ask("east/z");
        let eastern = near
            .iter()
            .filter(|addr| addr.as_str() != "node-2")
            .cloned()
            .collect::<Vec<_>>();
        let baseline = ask("")
            .into_iter()
            .filter(|addr| addr != "node-2")
            .collect::<Vec<_>>();
        assert_eq!(eastern, baseline, "the tie-break stopped being stable");
    }

    fn bus_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        // Setting the fixture up is itself a metadata change. Pump it through
        // before anyone subscribes, so these tests observe what they cause
        // rather than the registration that built the fixture.
        meta.pump_topology_events();
        meta
    }

    fn freeze(meta: &SingleNodeMeta) {
        meta.freeze_server(StateChangeRequest {
            endpoint: "node-a".to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        });
    }

    fn unfreeze(meta: &SingleNodeMeta) {
        meta.unfreeze_server(StateChangeRequest {
            endpoint: "node-a".to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        });
    }

    fn changed(meta: &SingleNodeMeta, name: &str) {
        register(meta, name);
    }

    fn history(meta: &SingleNodeMeta, after_version: u64) -> TopologyEventsResponse {
        meta.topology_events(TopologyEventsRequest {
            after_version,
            limit: 0,
        })
    }

    #[test]
    fn what_changed_can_be_asked_for_after_the_fact() {
        // Every metadata change records one of these, and the only way to see
        // them was to subscribe and wait -- so an operator looking at an
        // incident that had already happened could not ask what changed.
        let meta = SingleNodeMeta::default();
        changed(&meta, "one");
        changed(&meta, "two");

        let seen = history(&meta, 0);
        assert!(seen.status.ok);
        assert!(seen.events.len() >= 2);
        assert!(seen.events.iter().any(|event| event.resource.contains("one")));
        assert!(seen.events.iter().any(|event| event.resource.contains("two")));
        assert!(!seen.missed_events);
    }

    #[test]
    fn a_caller_can_resume_from_where_it_stopped() {
        let meta = SingleNodeMeta::default();
        changed(&meta, "one");
        let first = history(&meta, 0);
        let resume_from = first.events.last().expect("an event").topology_version;

        changed(&meta, "two");
        let next = history(&meta, resume_from);
        assert!(next.events.iter().all(|event| event.topology_version > resume_from));
        assert!(next.events.iter().any(|event| event.resource.contains("two")));
        assert!(!next.missed_events);
    }

    #[test]
    fn a_caller_that_fell_behind_the_ring_is_told_so() {
        // The history is bounded. A caller resuming from a point that has been
        // overwritten must not read the gap as a quiet period -- that is the
        // difference between "nothing happened" and "you missed it".
        let meta = SingleNodeMeta::default();
        for index in 0..(TOPOLOGY_EVENT_HISTORY_LIMIT + 20) {
            changed(&meta, &format!("ns-{index}"));
        }
        let stale = history(&meta, 1);
        assert!(
            stale.missed_events,
            "a caller resuming from an evicted point was not warned"
        );
        assert!(stale.oldest_retained_version > 1);
    }

    #[test]
    fn asking_from_the_current_version_is_quiet_not_a_gap() {
        // The inverse: caught up is not the same as fallen behind, and must not
        // be reported as missing anything.
        let meta = SingleNodeMeta::default();
        changed(&meta, "one");
        let latest = history(&meta, 0)
            .events
            .last()
            .expect("an event")
            .topology_version;
        let caught_up = history(&meta, latest);
        assert!(caught_up.events.is_empty());
        assert!(!caught_up.missed_events);
    }

    #[test]
    fn the_history_never_returns_more_than_asked_for() {
        let meta = SingleNodeMeta::default();
        for index in 0..10 {
            changed(&meta, &format!("ns-{index}"));
        }
        let capped = meta.topology_events(TopologyEventsRequest {
            after_version: 0,
            limit: 3,
        });
        assert_eq!(capped.events.len(), 3);
    }

    #[test]
    fn a_subscriber_is_told_about_a_change_instead_of_polling_for_it() {
        let meta = bus_meta();
        let subscription = meta.subscribe_topology();
        meta.pump_topology_events();

        freeze(&meta);
        assert_eq!(meta.pump_topology_events(), 1);

        let notice = subscription.try_next().expect("a notice");
        assert_eq!(notice.event.kind, "server_state");
        assert_eq!(notice.event.resource, "server:node-a");
        assert_eq!(notice.missed, 0);
    }

    #[test]
    fn a_subscriber_can_ask_for_only_the_kinds_it_cares_about() {
        let meta = bus_meta();
        let subscription = meta.subscribe_topology_kinds(["add_table"]);
        meta.pump_topology_events();

        freeze(&meta);
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        meta.pump_topology_events();

        let kinds = subscription
            .drain()
            .into_iter()
            .map(|notice| notice.event.kind)
            .collect::<Vec<_>>();
        assert_eq!(kinds, vec!["add_table".to_string()]);
    }

    #[test]
    fn every_subscriber_sees_the_same_change() {
        let meta = bus_meta();
        let first = meta.subscribe_topology();
        let second = meta.subscribe_topology();
        meta.pump_topology_events();

        freeze(&meta);
        meta.pump_topology_events();

        assert_eq!(first.drain().len(), 1);
        assert_eq!(second.drain().len(), 1);
    }

    #[test]
    fn a_subscriber_that_stops_reading_cannot_hold_up_the_others() {
        // The failure this design exists to survive. A subscriber that stops
        // draining must not grow the metaserver's memory, block the pump, or
        // cost anyone else a single event.
        let meta = bus_meta();
        let asleep = meta.subscribe_topology();
        let awake = meta.subscribe_topology();
        meta.pump_topology_events();

        let rounds = SUBSCRIBER_QUEUE_DEPTH + 40;
        for round in 0..rounds {
            if round % 2 == 0 {
                freeze(&meta);
            } else {
                unfreeze(&meta);
            }
            meta.pump_topology_events();
            // The attentive one keeps up; the sleeping one never reads.
            assert_eq!(awake.drain().len(), 1, "attentive subscriber missed round {round}");
        }

        // The sleeper banked at most its queue depth, not one per round.
        let banked = asleep.drain();
        assert!(
            banked.len() <= SUBSCRIBER_QUEUE_DEPTH,
            "queue grew past its bound: {}",
            banked.len()
        );
        assert!(rounds > SUBSCRIBER_QUEUE_DEPTH);
    }

    #[test]
    fn falling_behind_is_reported_rather_than_hidden() {
        // Having missed a freeze is survivable; not knowing you missed it is
        // not, so the count rides along on the next notice that gets through.
        let meta = bus_meta();
        let subscription = meta.subscribe_topology();
        meta.pump_topology_events();

        for round in 0..(SUBSCRIBER_QUEUE_DEPTH + 10) {
            if round % 2 == 0 {
                freeze(&meta);
            } else {
                unfreeze(&meta);
            }
            meta.pump_topology_events();
        }
        // Empty the backlog, then take one more notice: it must carry the count.
        let banked = subscription.drain();
        assert!(!banked.is_empty());
        freeze(&meta);
        unfreeze(&meta);
        meta.pump_topology_events();
        let after = subscription.try_next().expect("a notice after the backlog");
        assert!(
            after.missed > 0,
            "the subscriber was never told it had fallen behind"
        );
    }

    #[test]
    fn events_the_ring_recycled_are_counted_as_missed() {
        // The ring holds a bounded history. If more changes land between pumps
        // than it retains, the pump never sees them -- and versions increase by
        // exactly one per event, so exactly how many is knowable.
        let meta = bus_meta();
        let subscription = meta.subscribe_topology();
        meta.pump_topology_events();

        for round in 0..(TOPOLOGY_EVENT_HISTORY_LIMIT + 50) {
            if round % 2 == 0 {
                freeze(&meta);
            } else {
                unfreeze(&meta);
            }
        }
        meta.pump_topology_events();

        let notices = subscription.drain();
        assert!(!notices.is_empty());
        assert!(
            notices.iter().any(|notice| notice.missed > 0),
            "the pump did not notice the ring had recycled events"
        );
    }

    #[test]
    fn unsubscribing_stops_delivery() {
        let meta = bus_meta();
        let subscription = meta.subscribe_topology();
        meta.pump_topology_events();
        assert_eq!(meta.topology_subscriber_count(), 1);

        meta.unsubscribe_topology(subscription.id);
        assert_eq!(meta.topology_subscriber_count(), 0);

        freeze(&meta);
        meta.pump_topology_events();
        assert!(subscription.try_next().is_none());
    }

    #[test]
    fn a_subscriber_arriving_later_starts_from_now() {
        // The cursor advances even with nobody listening, so subscribing does
        // not replay the whole retained history at you.
        let meta = bus_meta();
        freeze(&meta);
        unfreeze(&meta);
        meta.pump_topology_events();

        let subscription = meta.subscribe_topology();
        meta.pump_topology_events();
        assert!(subscription.try_next().is_none(), "history was replayed");

        freeze(&meta);
        meta.pump_topology_events();
        assert!(subscription.try_next().is_some());
    }

    #[test]
    fn a_dropped_subscription_is_forgotten() {
        let meta = bus_meta();
        {
            let _subscription = meta.subscribe_topology();
            assert_eq!(meta.topology_subscriber_count(), 1);
        }
        // The receiver is gone; the next fan-out reaps it.
        freeze(&meta);
        meta.pump_topology_events();
        assert_eq!(meta.topology_subscriber_count(), 0);
    }

    /// A table occupying shard ids [first, first + count).
    fn table_spanning(meta: &SingleNodeMeta, name: &str, first: ShardId, count: u64) {
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: name.to_string(),
            first_shard_id: first,
            shard_count: count,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
    }

    /// Which table a shard resolves to, observed through a caller that uses the
    /// lookup: retention only purges a shard when it can name the table.
    fn resolves_to(meta: &SingleNodeMeta, shard_id: ShardId) -> Option<String> {
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id,
            server_addr: "node-a".to_string(),
        });
        let plan = meta.plan_meta_retention_now(MetaRetentionOptions::default());
        let _ = plan;
        meta.list_tables()
            .tables
            .into_iter()
            .find(|table| {
                shard_id >= table.first_shard_id
                    && shard_id < table.first_shard_id + table.shard_count
            })
            .map(|table| table.table_name)
    }

    #[test]
    fn a_shard_resolves_to_the_table_whose_range_covers_it() {
        let meta = SingleNodeMeta::default();
        table_spanning(&meta, "orders", 100, 4);
        assert_eq!(resolves_to(&meta, 100).as_deref(), Some("orders"));
        assert_eq!(resolves_to(&meta, 103).as_deref(), Some("orders"));
    }

    #[test]
    fn the_shard_one_past_the_end_belongs_to_nobody() {
        // The boundary the old search got right by construction and a bounds
        // check can get wrong by one.
        let meta = SingleNodeMeta::default();
        table_spanning(&meta, "orders", 100, 4);
        assert_eq!(resolves_to(&meta, 104), None);
        assert_eq!(resolves_to(&meta, 99), None);
    }

    #[test]
    fn a_table_with_no_shards_owns_nothing() {
        let meta = SingleNodeMeta::default();
        // shard_count 0 is refused at creation, so the closest reachable case
        // is a table whose range simply does not cover the shard asked about.
        table_spanning(&meta, "orders", 100, 1);
        assert_eq!(resolves_to(&meta, 200), None);
    }

    #[test]
    fn two_tables_side_by_side_do_not_bleed_into_each_other() {
        let meta = SingleNodeMeta::default();
        table_spanning(&meta, "orders", 100, 4);
        table_spanning(&meta, "events", 104, 4);
        assert_eq!(resolves_to(&meta, 103).as_deref(), Some("orders"));
        assert_eq!(resolves_to(&meta, 104).as_deref(), Some("events"));
        assert_eq!(resolves_to(&meta, 107).as_deref(), Some("events"));
        assert_eq!(resolves_to(&meta, 108), None);
    }

    #[test]
    fn a_table_at_the_top_of_the_id_range_does_not_overflow() {
        // first_shard_id comes from whoever created the table, and computing
        // the end of the range from a value near the maximum would wrap.
        let meta = SingleNodeMeta::default();
        table_spanning(&meta, "orders", u64::MAX - 1, 4);
        // The point is that asking does not panic; the answer is that the
        // shard below the range is not in it.
        assert_eq!(resolves_to(&meta, 10), None);
    }

    fn counted_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-a".to_string(),
        });
        meta
    }

    fn topology_request() -> GetTableTopologyRequest {
        GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        }
    }

    #[test]
    fn resolving_a_topology_does_not_need_the_exclusive_lock() {
        // This is the whole point. Every client and every proxy resolves
        // topology, and it used to take the write lock purely to add one to a
        // counter -- so each lookup serialised against every other lookup and
        // against all metadata change. Holding a *read* guard here and
        // resolving from another thread would have blocked forever before.
        let meta = counted_meta();
        let held = meta.inner.read().expect("meta lock poisoned");

        let (sender, receiver) = std::sync::mpsc::channel();
        let reader = meta.clone();
        let worker = std::thread::spawn(move || {
            let response = reader.get_table_topology(topology_request());
            let _ = sender.send(response.status.ok);
        });

        let finished = receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("a topology read must not wait on a concurrent reader");
        assert!(finished);
        drop(held);
        worker.join().expect("reader thread");
    }

    #[test]
    fn reading_one_shard_does_not_need_the_exclusive_lock_either() {
        let meta = counted_meta();
        let held = meta.inner.read().expect("meta lock poisoned");

        let (sender, receiver) = std::sync::mpsc::channel();
        let reader = meta.clone();
        let worker = std::thread::spawn(move || {
            let _ = sender.send(reader.get(1).status.ok);
        });

        assert!(receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("a shard read must not wait on a concurrent reader"));
        drop(held);
        worker.join().expect("reader thread");
    }

    #[test]
    fn counting_still_counts() {
        // Moving the tallies out of the lock must not lose them.
        let meta = counted_meta();
        let before = meta.stats();
        for _ in 0..3 {
            assert!(meta.get_table_topology(topology_request()).status.ok);
        }
        meta.get(1);
        let after = meta.stats();
        assert_eq!(after.topology_query_total, before.topology_query_total + 3);
        assert_eq!(after.get_shard_total, before.get_shard_total + 1);
    }

    #[test]
    fn every_handle_counts_into_the_same_tallies() {
        // The counters are shared by clone, exactly as they were when they
        // lived inside the shared state.
        let meta = counted_meta();
        let clone = meta.clone();
        clone.get_table_topology(topology_request());
        assert_eq!(meta.stats().topology_query_total, 1);
    }

    #[test]
    fn concurrent_readers_lose_no_counts() {
        // Relaxed ordering is fine for a tally, but it still has to be atomic:
        // a plain += from eight threads would drop increments.
        let meta = counted_meta();
        let mut workers = Vec::new();
        for _ in 0..8 {
            let reader = meta.clone();
            workers.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    reader.get_table_topology(topology_request());
                }
            }));
        }
        for worker in workers {
            worker.join().expect("reader thread");
        }
        assert_eq!(meta.stats().topology_query_total, 400);
    }

    #[test]
    fn installing_a_snapshot_restores_the_tallies() {
        // The counters no longer travel inside MetaState, so the install has to
        // carry them across deliberately or a peer taking over reports every
        // total starting again from zero.
        let meta = counted_meta();
        for _ in 0..5 {
            meta.get_table_topology(topology_request());
        }
        let snapshot = meta.export_snapshot();
        assert_eq!(snapshot.stats.topology_query_total, 5);

        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(snapshot).status.ok);
        assert_eq!(peer.stats().topology_query_total, 5);
    }

    fn domains(count: u64) -> Vec<NumaNode> {
        (0..count)
            .map(|id| NumaNode {
                id,
                cpu_list: format!("{}-{}", id * 16, id * 16 + 15),
                memory_size_mb: 65_536,
            })
            .collect()
    }

    fn register_with(meta: &SingleNodeMeta, addr: &str, numa_nodes: Vec<NumaNode>) {
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: addr.to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes,
        });
    }

    fn listed_server(meta: &SingleNodeMeta, addr: &str) -> ServerMetaInfo {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == addr)
            .expect("registered")
    }

    #[test]
    fn a_server_can_say_what_it_is_made_of() {
        // The metaserver could not tell one large memory domain from four
        // smaller ones, because there was nowhere to say.
        let meta = SingleNodeMeta::default();
        register_with(&meta, "node-a", domains(4));
        let server = listed_server(&meta, "node-a");
        assert_eq!(server.numa_nodes.len(), 4);
        assert_eq!(server.numa_nodes[0].cpu_list, "0-15");
        assert_eq!(server.numa_nodes[3].memory_size_mb, 65_536);
    }

    #[test]
    fn a_server_that_says_nothing_is_recorded_as_saying_nothing() {
        // Every server built before this reports no domains, and that has to be
        // an empty list rather than a guess.
        let meta = SingleNodeMeta::default();
        register_with(&meta, "node-a", Vec::new());
        assert!(listed_server(&meta, "node-a").numa_nodes.is_empty());
    }

    #[test]
    fn re_registering_replaces_the_shape_rather_than_adding_to_it() {
        // A machine that came back with different hardware is describing itself
        // now, not amending what it said before.
        let meta = SingleNodeMeta::default();
        register_with(&meta, "node-a", domains(4));
        register_with(&meta, "node-a", domains(2));
        assert_eq!(listed_server(&meta, "node-a").numa_nodes.len(), 2);
    }

    /// A metaserver with a durable log holding one frozen table, one dropped
    /// table and one dropped server. Returns the log path and the clocks as they
    /// stood before the restart.
    fn clocks_before_restart(
        log_path: &std::path::Path,
    ) -> (BTreeMap<String, u64>, BTreeMap<String, u64>) {
        let meta = SingleNodeMeta::with_mutation_log(log_path).unwrap();
        register(&meta, "server-a");
        register(&meta, "server-b");
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        for (name, first_shard_id) in [("frozen", 900), ("dropped", 950)] {
            assert!(meta
                .add_table(AddTableRequest {
                    namespace: "ns".to_string(),
                    table_name: name.to_string(),
                    first_shard_id,
                    shard_count: 1,
                    replica_count: 1,
                    partition_version: 0,
                    serving_options: TableServingOptions::default(),
                })
                .status
                .ok);
        }
        assert!(meta
            .freeze_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "frozen".to_string()
            })
            .status
            .ok);
        assert!(meta
            .delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "dropped".to_string()
            })
            .status
            .ok);
        assert!(meta
            .drop_server(StateChangeRequest {
                endpoint: "server-b".to_string(),
                reason: FreezeReason::Operator,
                freeze_cooldown_ms: 0,
            })
            .status
            .ok);
        let snapshot = meta.export_snapshot();
        assert!(!snapshot.frozen_since_ms.is_empty() && !snapshot.dropped_since_ms.is_empty());
        (snapshot.frozen_since_ms, snapshot.dropped_since_ms)
    }

    #[test]
    fn the_retention_clocks_survive_a_metaserver_restart() {
        // Everything that ages was stamped with the wall clock inside the apply
        // path, which replay also runs, so every clock restarted whenever the
        // metaserver did. Retention purge and freeze aging could never fire on a
        // cluster that restarts more often than their windows.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta.log");
        let (frozen_before, dropped_before) = clocks_before_restart(&log_path);

        std::thread::sleep(std::time::Duration::from_millis(30));
        let restarted = SingleNodeMeta::with_mutation_log(&log_path)
            .unwrap()
            .export_snapshot();

        assert_eq!(
            restarted.frozen_since_ms, frozen_before,
            "a freeze clock restarted with the metaserver"
        );
        assert_eq!(
            restarted.dropped_since_ms, dropped_before,
            "a retention clock restarted with the metaserver"
        );
    }

    #[test]
    fn a_log_written_before_the_time_was_recorded_still_replays() {
        // Lines already on disk have no time in them. They must still load, and
        // fall back to the current clock, which is what they have always been
        // given -- not be rejected as malformed.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta.log");
        clocks_before_restart(&log_path);

        let older: String = std::fs::read_to_string(&log_path)
            .unwrap()
            .lines()
            .map(|line| {
                let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
                value
                    .as_object_mut()
                    .unwrap()
                    .remove("at_ms")
                    .expect("the time was written");
                format!("{}\n", serde_json::to_string(&value).unwrap())
            })
            .collect();
        std::fs::write(&log_path, older).unwrap();

        let restarted = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        let snapshot = restarted.export_snapshot();
        assert!(
            !snapshot.frozen_since_ms.is_empty(),
            "an older log did not replay"
        );
        assert_eq!(snapshot.tables.len(), 2);
    }

    #[test]
    fn a_line_carrying_the_time_is_still_readable_without_it() {
        // The time is an added field on the log line, so a build that predates
        // it must still be able to read what a newer one wrote.
        let mutation = MetaMutation::AddNamespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        let line = serde_json::to_string(&MetaMutationRecord {
            at_ms: 1234,
            mutation: mutation.clone(),
        })
        .unwrap();
        let without_the_field: MetaMutation = serde_json::from_str(&line)
            .expect("a line carrying the time must still read as a bare change");
        assert_eq!(without_the_field, mutation);
    }

    #[test]
    fn the_hardware_shape_survives_snapshot_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("numa-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            register_with(&meta, "node-a", domains(3));

            let snapshot = meta.export_snapshot();
            let peer = SingleNodeMeta::default();
            assert!(peer.install_snapshot(snapshot).status.ok);
            assert_eq!(listed_server(&peer, "node-a").numa_nodes.len(), 3);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(listed_server(&recovered, "node-a").numa_nodes.len(), 3);
    }

    #[test]
    fn knowing_the_shape_does_not_change_where_a_shard_goes() {
        // Placement still treats a server as one target. Recording the shape is
        // what a finer-grained placement would read; it is not that placement,
        // and this pins that it did not quietly become it.
        let meta = SingleNodeMeta::default();
        register_with(&meta, "node-a", domains(8));
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-b".to_string(),
            node_id: 2,
            location: "rack-2".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 2,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        });
        // Two servers, two replicas: the eight domains on one of them do not
        // turn it into eight candidates.
        assert_eq!(topology.shards[0].replicas.len(), 2);
    }

    fn drop_of(endpoint: &str) -> StateChangeRequest {
        StateChangeRequest {
            endpoint: endpoint.to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        }
    }

    fn drop_clock(meta: &SingleNodeMeta, key: &str) -> Option<u64> {
        meta.export_snapshot().dropped_since_ms.get(key).copied()
    }

    #[test]
    fn a_server_that_comes_back_starts_a_fresh_drop_clock() {
        // stamp_dropped_since keeps the first time it is given, deliberately, so
        // that re-dropping an already dropped resource cannot restart the clock.
        // That is right for drop-then-drop and wrong across a revival: a server
        // dropped, brought back, and dropped again much later would inherit the
        // original time and be collected on the next round with no grace at all.
        let meta = SingleNodeMeta::default();
        let register = || {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            })
        };
        register();
        assert!(meta.drop_server(drop_of("node-a")).status.ok);
        assert!(drop_clock(&meta, "server:node-a").is_some());

        register();
        assert_eq!(
            drop_clock(&meta, "server:node-a"),
            None,
            "the drop clock outlived the drop"
        );
    }

    #[test]
    fn a_proxy_that_comes_back_starts_a_fresh_drop_clock() {
        let meta = SingleNodeMeta::default();
        let register = || {
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: String::new(),
                location: "rack-1".to_string(),
                config_version: 0,
                binary_version: "v1".to_string(),
            })
        };
        register();
        assert!(meta.drop_proxy(drop_of("proxy-a")).status.ok);
        assert!(drop_clock(&meta, "proxy:proxy-a").is_some());

        register();
        assert_eq!(drop_clock(&meta, "proxy:proxy-a"), None);
    }

    #[test]
    fn a_revived_server_dropped_again_is_not_collected_immediately() {
        // The whole point of the clock: a resource dropped a moment ago is
        // inside its retention window and must be kept.
        let meta = SingleNodeMeta::default();
        let register = || {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            })
        };
        register();
        assert!(meta.drop_server(drop_of("node-a")).status.ok);
        register();
        assert!(meta.drop_server(drop_of("node-a")).status.ok);

        let plan = meta.plan_meta_retention_now(MetaRetentionOptions {
            // An hour of grace. The second drop just happened, so nothing is
            // eligible -- unless it is still being aged from the first one.
            server_retention_ms: 60 * 60 * 1_000,
            proxy_retention_ms: 60 * 60 * 1_000,
            table_retention_ms: 60 * 60 * 1_000,
            ..MetaRetentionOptions::default()
        });
        assert!(
            plan.servers.is_empty(),
            "a server dropped a moment ago was already eligible: {plan:?}"
        );
    }

    #[test]
    fn re_dropping_without_a_revival_still_keeps_the_first_clock() {
        // The other direction, which the change must not break: repeatedly
        // dropping an already dropped server cannot hold off collection.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        assert!(meta.drop_server(drop_of("node-a")).status.ok);
        let first = drop_clock(&meta, "server:node-a").expect("stamped");
        // The second drop is refused outright, and must leave the clock alone.
        meta.drop_server(drop_of("node-a"));
        assert_eq!(drop_clock(&meta, "server:node-a"), Some(first));
    }

    /// A namespace with two serving tables on one node.
    fn namespaced_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string(),
        });
        for (index, table_name) in ["orders", "events"].into_iter().enumerate() {
            meta.add_table(AddTableRequest {
                namespace: "tenant".to_string(),
                table_name: table_name.to_string(),
                first_shard_id: index as ShardId + 1,
                shard_count: 1,
                replica_count: 1,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            });
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: index as ShardId + 1,
                server_addr: "node-a".to_string(),
            });
        }
        meta
    }

    fn namespace(name: &str) -> AddNamespaceRequest {
        AddNamespaceRequest {
            namespace: name.to_string(),
        }
    }

    fn topology_status(meta: &SingleNodeMeta, table_name: &str) -> Status {
        meta.get_table_topology(GetTableTopologyRequest {
            namespace: "tenant".to_string(),
            table_name: table_name.to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        })
        .status
    }

    #[test]
    fn freezing_a_namespace_stops_every_table_in_it() {
        // The lever an operator reaches for is the tenant, not the table.
        let meta = namespaced_meta();
        assert!(topology_status(&meta, "orders").ok);
        assert!(topology_status(&meta, "events").ok);

        assert!(meta.freeze_namespace(namespace("tenant")).status.ok);
        assert_eq!(topology_status(&meta, "orders").code, "resource_frozen");
        assert_eq!(topology_status(&meta, "events").code, "resource_frozen");

        assert!(meta.unfreeze_namespace(namespace("tenant")).status.ok);
        assert!(topology_status(&meta, "orders").ok);
        assert!(topology_status(&meta, "events").ok);
    }

    #[test]
    fn a_table_cannot_be_created_into_a_frozen_namespace() {
        // Otherwise a table created after the freeze serves straight through it,
        // and the freeze does not mean what it says.
        let meta = namespaced_meta();
        assert!(meta.freeze_namespace(namespace("tenant")).status.ok);
        let response = meta.add_table(AddTableRequest {
            namespace: "tenant".to_string(),
            table_name: "late".to_string(),
            first_shard_id: 9,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        assert_eq!(response.status.code, "resource_frozen");
    }

    #[test]
fn counting_a_namespace_tables_agrees_with_counting_them_one_by_one() {
        let meta = SingleNodeMeta::default();
        // Three namespaces holding different numbers of tables, one holding
        // none at all, and a dropped table that must not be counted.
        for (namespace, table_name) in [
            ("alpha", "a1"),
            ("alpha", "a2"),
            ("alpha", "a3"),
            ("beta", "b1"),
            ("gamma", "g1"),
            ("gamma", "dropped"),
        ] {
            assert!(meta
                .add_table(AddTableRequest {
                    namespace: namespace.to_string(),
                    table_name: table_name.to_string(),
                    first_shard_id: 1,
                    shard_count: 1,
                    replica_count: 1,
                    partition_version: 1,
                    serving_options: Default::default(),
                })
                .status
                .ok);
        }
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "empty".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .delete_table(DeleteTableRequest {
                namespace: "gamma".to_string(),
                table_name: "dropped".to_string(),
            })
            .status
            .ok);

        let listed = meta.list_namespaces();
        assert!(listed.status.ok);

        // Recomputed the long way: for each namespace, walk every table. This
        // is what the listing used to do, and the tally has to match it.
        let tables = meta.list_tables().tables;
        for namespace in &listed.namespaces {
            let counted_one_by_one = tables
                .iter()
                .filter(|table| {
                    table.namespace == namespace.namespace
                        && table.state != MetaEntityState::Dropped
                })
                .count();
            assert_eq!(
                namespace.table_count, counted_one_by_one,
                "namespace {} was tallied as {} but holds {}",
                namespace.namespace, namespace.table_count, counted_one_by_one
            );
        }

        // And the counts are what they should be, so this is not agreeing on
        // zero everywhere.
        let count_of = |wanted: &str| {
            listed
                .namespaces
                .iter()
                .find(|namespace| namespace.namespace == wanted)
                .unwrap_or_else(|| panic!("{wanted} is missing from the listing"))
                .table_count
        };
        assert_eq!(count_of("alpha"), 3);
        assert_eq!(count_of("beta"), 1);
        assert_eq!(count_of("gamma"), 1, "the dropped table was counted");
        assert_eq!(count_of("empty"), 0);
    }

    #[test]
        fn a_namespace_that_still_holds_a_table_is_not_dropped() {
        // Dropping the namespace out from under a live table would leave the
        // table addressable by name but unreachable through its namespace.
        let meta = namespaced_meta();
        assert_eq!(
            meta.drop_namespace(namespace("tenant")).status.code,
            "namespace_not_empty"
        );

        for table_name in ["orders", "events"] {
            assert!(
                meta.delete_table(DeleteTableRequest {
                    namespace: "tenant".to_string(),
                    table_name: table_name.to_string(),
                })
                .status
                .ok
            );
        }
        assert!(meta.drop_namespace(namespace("tenant")).status.ok);
    }

    #[test]
    fn a_dropped_namespace_can_be_brought_back() {
        // Dropping is a tombstone, not an erasure, so it stays recoverable up
        // until retention forgets it.
        let meta = SingleNodeMeta::default();
        meta.add_namespace(namespace("tenant"));
        assert!(meta.drop_namespace(namespace("tenant")).status.ok);
        assert_eq!(
            meta.list_namespaces().namespaces[0].state,
            MetaEntityState::Dropped
        );
        assert!(meta.unfreeze_namespace(namespace("tenant")).status.ok);
        assert_eq!(
            meta.list_namespaces().namespaces[0].state,
            MetaEntityState::Normal
        );
    }

    #[test]
    fn namespace_state_changes_are_rejected_when_unknown_or_unchanged() {
        let meta = namespaced_meta();
        assert_eq!(
            meta.freeze_namespace(namespace("nope")).status.code,
            "namespace_not_found"
        );
        assert!(meta.freeze_namespace(namespace("tenant")).status.ok);
        assert_eq!(
            meta.freeze_namespace(namespace("tenant")).status.code,
            "not_modified"
        );
    }

    #[test]
    fn a_muted_metaserver_will_not_change_a_namespace() {
        let meta = namespaced_meta();
        assert!(meta.set_meta_change_muted(true).status.ok);
        assert_eq!(
            meta.freeze_namespace(namespace("tenant")).status.code,
            "meta_change_muted"
        );
    }

    #[test]
fn counting_resources_agrees_with_listing_them_and_counting_those() {
        let meta = SingleNodeMeta::default();

        // A server that has heartbeated, so its row has something in it, and a
        // frozen one so the states are spread.
        for node in 0..2u64 {
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: format!("node-{node}"),
                    node_id: node + 1,
                    location: "rack-1".to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: format!("node-{node}"),
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: vec![ShardLoad {
                    shard_id: node + 1,
                    key_count: 7 + node,
                    memory_bytes: 4096,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad {
                    rejected_total: 3 + node,
                    timed_out_total: 1,
                    canceled_total: 2,
                    last_meta_topology_version: 9,
                    ..Default::default()
                },
                // The record and storage counters a scrape reports are summed
                // from these, not from the loads above.
                shard_states: vec![ServerShardServingState {
                    shard_id: node + 1,
                    total_records: 11 + node as usize,
                    storage_bytes: 2048,
                    ..Default::default()
                }],
            });
        }
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-1".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Unresponsive,
            })
            .status
            .ok);

        // Tables in all three states.
        for table_name in ["kept", "frozen", "dropped"] {
            assert!(meta
                .add_table(AddTableRequest {
                    namespace: "ns".to_string(),
                    table_name: table_name.to_string(),
                    first_shard_id: 1,
                    shard_count: 1,
                    replica_count: 1,
                    partition_version: 1,
                    serving_options: Default::default(),
                })
                .status
                .ok);
        }
        assert!(meta
            .freeze_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "frozen".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "dropped".to_string(),
            })
            .status
            .ok);

        // Namespaces in all three states.
        for namespace in ["kept-ns", "frozen-ns", "dropped-ns"] {
            assert!(meta
                .add_namespace(AddNamespaceRequest {
                    namespace: namespace.to_string(),
                })
                .status
                .ok);
        }
        assert!(meta
            .freeze_namespace(AddNamespaceRequest {
                namespace: "frozen-ns".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .drop_namespace(AddNamespaceRequest {
                namespace: "dropped-ns".to_string(),
            })
            .status
            .ok);

        // Proxy groups, one kept and one dropped.
        for group in ["kept-group", "dropped-group"] {
            assert!(meta
                .put_proxy_group(PutProxyGroupRequest {
                    group: group.to_string(),
                    namespace: "ns".to_string(),
                    location: String::new(),
                    instance_num: 1,
                    drop_percent: 0,
                })
                .status
                .ok);
        }
        assert!(meta
            .drop_proxy_group(DropProxyGroupRequest {
                group: "dropped-group".to_string(),
            })
            .status
            .ok);

        let tallies = meta.metrics_report();
        assert!(tallies.status.ok);

        // Counted against listing them and counting those, which is what the
        // scrape did before -- for every state, and for the total.
        let tables = meta.list_tables().tables;
        let namespaces = meta.list_namespaces().namespaces;
        let proxy_groups = meta.list_proxy_groups().groups;
        assert_eq!(tallies.tables.total(), tables.len() as u64, "table total");
        assert_eq!(
            tallies.namespaces.total(),
            namespaces.len() as u64,
            "namespace total"
        );
        assert_eq!(
            tallies.proxy_groups.total(),
            proxy_groups.len() as u64,
            "proxy group total"
        );
        for state in ["normal", "frozen", "dropped"] {
            assert_eq!(
                tallies.tables.in_state(state),
                tables
                    .iter()
                    .filter(|table| table.state.as_str() == state)
                    .count() as u64,
                "tables in state {state}"
            );
            assert_eq!(
                tallies.namespaces.in_state(state),
                namespaces
                    .iter()
                    .filter(|namespace| namespace.state.as_str() == state)
                    .count() as u64,
                "namespaces in state {state}"
            );
            assert_eq!(
                tallies.proxy_groups.in_state(state),
                proxy_groups
                    .iter()
                    .filter(|group| group.state.as_str() == state)
                    .count() as u64,
                "proxy groups in state {state}"
            );
        }

        // And the states are actually spread, so the agreement above is not
        // every count being equal to zero.
        assert_eq!(tallies.tables.normal, 1);
        assert_eq!(tallies.tables.frozen, 1);
        assert_eq!(tallies.tables.dropped, 1);
        assert_eq!(tallies.namespaces.frozen, 1);
        assert_eq!(tallies.namespaces.dropped, 1);

        // A name nothing uses counts as nothing, rather than falling into one
        // of the three.
        assert_eq!(tallies.tables.in_state("loading"), 0);

        // The server and proxy rows carry what the scrape reports, for every
        // server and proxy there is, and they agree with the full records.
        let full_servers = meta.list_servers().servers;
        let full_proxies = meta.list_proxies().proxies;
        assert_eq!(tallies.servers.len(), full_servers.len(), "a server is missing");
        assert_eq!(tallies.proxies.len(), full_proxies.len(), "a proxy is missing");
        for full in &full_servers {
            let row = tallies
                .servers
                .iter()
                .find(|row| row.server_addr == full.server_addr)
                .unwrap_or_else(|| panic!("{} has no row", full.server_addr));
            assert_eq!(row.state, full.state, "{} state", full.server_addr);
            assert_eq!(
                row.reported_record_count, full.reported_record_count,
                "{} record count",
                full.server_addr
            );
            assert_eq!(
                row.reported_storage_bytes, full.reported_storage_bytes,
                "{} storage bytes",
                full.server_addr
            );
            assert_eq!(
                row.last_meta_topology_version, full.runtime_load.last_meta_topology_version,
                "{} topology version",
                full.server_addr
            );
            assert_eq!(
                row.rejected_total, full.runtime_load.rejected_total,
                "{} rejected",
                full.server_addr
            );
            assert_eq!(
                row.timed_out_total, full.runtime_load.timed_out_total,
                "{} timed out",
                full.server_addr
            );
            assert_eq!(
                row.canceled_total, full.runtime_load.canceled_total,
                "{} canceled",
                full.server_addr
            );
        }
        for full in &full_proxies {
            let row = tallies
                .proxies
                .iter()
                .find(|row| row.proxy_addr == full.proxy_addr)
                .unwrap_or_else(|| panic!("{} has no row", full.proxy_addr));
            assert_eq!(row.state, full.state, "{} state", full.proxy_addr);
            assert_eq!(
                row.restart_count, full.restart_count,
                "{} restart count",
                full.proxy_addr
            );
        }
        // The counters are not all zero, so the agreement above means
        // something.
        assert!(
            tallies
                .servers
                .iter()
                .any(|row| row.reported_record_count > 0),
            "no server reported a record count, so the row check proves nothing"
        );
    }

    #[test]
        fn namespace_state_survives_snapshot_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("namespace-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.add_namespace(namespace("tenant"));
            assert!(meta.freeze_namespace(namespace("tenant")).status.ok);
            let snapshot = meta.export_snapshot();
            let restored = SingleNodeMeta::default();
            assert!(restored.install_snapshot(snapshot).status.ok);
            assert_eq!(
                restored.list_namespaces().namespaces[0].state,
                MetaEntityState::Frozen
            );
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.list_namespaces().namespaces[0].state,
            MetaEntityState::Frozen
        );
    }

    /// A two-shard table whose shards are both registered to a live server --
    /// that is, a table that is holding data.
    fn live_two_shard_table() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        for shard_id in [100, 101] {
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id,
                server_addr: "node-a".to_string(),
            });
        }
        meta
    }

    fn bucket_ranges(meta: &SingleNodeMeta) -> Vec<(ShardId, u64, u64)> {
        meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        })
        .shards
        .into_iter()
        .map(|shard| (shard.shard_id, shard.start_bucket, shard.end_bucket))
        .collect()
    }

    fn grow_to(meta: &SingleNodeMeta, shard_count: u64) -> Status {
        meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            shard_count: Some(shard_count),
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: None,
        })
        .status
    }

    #[test]
    fn a_registered_shards_key_range_never_moves() {
        // Bucket ranges are derived from shard_count on every read, so raising
        // it renumbers the whole key space. Nothing rehashes: the data for the
        // buckets that moved is still sitting on the old shard, while the
        // routing table now sends those keys to a shard that has never seen
        // them, and the reads come back as misses rather than errors.
        let meta = live_two_shard_table();
        let before = bucket_ranges(&meta);
        assert_eq!(before.len(), 2);

        grow_to(&meta, 4);

        let after = bucket_ranges(&meta);
        let (_, start, end) = before[0];
        let moved = after
            .iter()
            .find(|(shard_id, _, _)| *shard_id == before[0].0)
            .map(|(_, new_start, new_end)| (*new_start, *new_end));
        assert_eq!(
            moved,
            Some((start, end)),
            "shard {} owned buckets {start}..={end} and now owns {moved:?}",
            before[0].0
        );
    }

    #[test]
    fn a_key_does_not_change_shard_underneath_the_data() {
        // The same defect stated as the client sees it: pick a bucket the first
        // shard owns, and check it still resolves to that shard afterwards.
        let meta = live_two_shard_table();
        let before = bucket_ranges(&meta);
        let probe = before[0].2; // the last bucket of the first shard
        let owner_of = |ranges: &[(ShardId, u64, u64)], bucket: u64| {
            ranges
                .iter()
                .find(|(_, start, end)| bucket >= *start && bucket <= *end)
                .map(|(shard_id, _, _)| *shard_id)
        };
        assert_eq!(owner_of(&before, probe), Some(before[0].0));

        grow_to(&meta, 4);

        assert_eq!(
            owner_of(&bucket_ranges(&meta), probe),
            Some(before[0].0),
            "bucket {probe} was rehomed to a shard that has never held it"
        );
    }

    #[test]
    fn growing_a_table_that_already_owns_shards_is_refused() {
        let meta = live_two_shard_table();
        let refused = grow_to(&meta, 4);
        assert_eq!(refused.code, "shards_registered");
        assert_eq!(meta.list_tables().tables[0].shard_count, 2);
        // An operator reads this one. A literal wrapped across source lines
        // keeps the indentation of the continuation unless it is escaped, and
        // this refusal was arriving with a run of spaces in the middle of it.
        assert!(
            !refused.message.contains("  "),
            "the refusal reads with a run of spaces in it: {:?}",
            refused.message
        );
    }

    #[test]
    fn growing_a_table_before_anything_registers_is_still_allowed() {
        // The legitimate case, and the one the existing suite covers: correcting
        // the shard count of a table that is not yet holding anything.
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        assert!(grow_to(&meta, 4).ok);
        assert_eq!(meta.list_tables().tables[0].shard_count, 4);
    }

    #[test]
    fn a_registered_table_can_still_change_its_other_options() {
        // The refusal is about shard_count alone: replica count and serving
        // options do not move any key.
        let meta = live_two_shard_table();
        let response = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            shard_count: None,
            replica_count: Some(2),
            first_shard_id: None,
            partition_version: None,
            serving_options: None,
        });
        assert!(response.status.ok, "{response:?}");
        assert_eq!(meta.list_tables().tables[0].replica_count, 2);
    }

    fn shard_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        for shard_id in [1, 2] {
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id,
                server_addr: "node-a".to_string(),
            });
        }
        meta
    }

    fn shard(id: ShardId) -> ShardStateRequest {
        ShardStateRequest { shard_id: id }
    }

    fn serving_primaries(meta: &SingleNodeMeta) -> Vec<Option<String>> {
        meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        })
        .shards
        .into_iter()
        .map(|s| s.primary)
        .collect()
    }

    #[test]
    fn freezing_one_shard_leaves_the_rest_of_the_table_serving() {
        // The whole point: the only lever before this was freezing the table,
        // which takes every other shard in it out too.
        let meta = shard_meta();
        assert_eq!(
            serving_primaries(&meta),
            vec![Some("node-a".to_string()), Some("node-a".to_string())]
        );

        assert!(meta.freeze_shard(shard(1)).status.ok);
        let primaries = serving_primaries(&meta);
        assert_eq!(primaries[0], None, "the frozen shard must not be routed to");
        assert_eq!(primaries[1], Some("node-a".to_string()));

        // And the table itself is untouched.
        assert_eq!(meta.list_tables().tables[0].state, MetaEntityState::Normal);
    }

    #[test]
    fn a_frozen_shard_comes_back() {
        let meta = shard_meta();
        assert!(meta.freeze_shard(shard(1)).status.ok);
        assert_eq!(serving_primaries(&meta)[0], None);
        assert!(meta.unfreeze_shard(shard(1)).status.ok);
        assert_eq!(serving_primaries(&meta)[0], Some("node-a".to_string()));
    }

    #[test]
    fn freezing_a_shard_keeps_its_owner_so_it_can_return() {
        // A frozen shard is out of service, not forgotten; the owner entry is
        // what makes unfreezing put it back where it was.
        let meta = shard_meta();
        meta.freeze_shard(shard(1));
        let located = meta.get(1);
        assert!(located.status.ok);
        let location = located.location.expect("still registered");
        assert_eq!(location.server_addr, "node-a");
        assert_eq!(location.state, MetaEntityState::Frozen);
    }

    #[test]
    fn rebalancing_leaves_a_frozen_shard_alone() {
        // An operator froze it on purpose. Moving it would be the planner
        // undoing a deliberate decision.
        let meta = shard_meta();
        meta.freeze_shard(shard(1));
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-b".to_string(),
            node_id: 2,
            location: "rack-2".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        let plans = meta.plan_auto_rebalance();
        assert!(
            plans.iter().all(|plan| plan.shard_id != 1),
            "frozen shard was moved: {plans:?}"
        );
    }

    #[test]
    fn the_divergence_check_ignores_a_frozen_shard() {
        // Its owner not serving it is exactly what freezing means, so flagging
        // it would have the checker "repair" the operator's decision.
        let meta = shard_meta();
        meta.freeze_shard(shard(1));
        let mut checker = ShardChecker::default();
        let (report, moves) = meta.check_shard_divergence(&mut checker);
        assert!(report.diverged.iter().all(|d| d.shard_id != 1));
        assert!(moves.iter().all(|m| m.shard_id != 1));
    }

    #[test]
    fn dropping_a_shard_removes_the_route() {
        let meta = shard_meta();
        assert!(meta.drop_shard(shard(1)).status.ok);
        assert!(!meta.get(1).status.ok);
        assert_eq!(
            meta.drop_shard(shard(1)).status.code,
            "shard_not_found",
            "dropping twice should report the shard is gone"
        );
    }

    #[test]
    fn shard_state_changes_are_rejected_when_unknown_or_unchanged() {
        let meta = shard_meta();
        assert_eq!(meta.freeze_shard(shard(99)).status.code, "shard_not_found");
        assert!(meta.freeze_shard(shard(1)).status.ok);
        assert_eq!(meta.freeze_shard(shard(1)).status.code, "not_modified");
    }

    #[test]
    fn freezing_a_shard_bumps_the_topology_version() {
        let meta = shard_meta();
        let before = meta.stats().topology_version;
        assert!(meta.freeze_shard(shard(1)).status.ok);
        assert!(meta.stats().topology_version > before);
    }

    #[test]
    fn shard_state_survives_snapshot_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("shard-state-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: 5,
                server_addr: "node-a".to_string(),
            });
            assert!(meta.freeze_shard(shard(5)).status.ok);
            let snapshot = meta.export_snapshot();
            let restored = SingleNodeMeta::default();
            assert!(restored.install_snapshot(snapshot).status.ok);
            assert_eq!(
                restored.get(5).location.expect("registered").state,
                MetaEntityState::Frozen
            );
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.get(5).location.expect("registered").state,
            MetaEntityState::Frozen
        );
    }

    fn reserving(namespaces: &[&str], tables: &[&str]) -> ReservedNames {
        ReservedNames {
            namespaces: namespaces.iter().map(|name| name.to_string()).collect(),
            tables: tables.iter().map(|name| name.to_string()).collect(),
        }
    }

    fn make_table(meta: &SingleNodeMeta, namespace: &str, table_name: &str) -> Status {
        meta.add_table(AddTableRequest {
            namespace: namespace.to_string(),
            table_name: table_name.to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        })
        .status
    }

    #[test]
    fn a_reserved_namespace_cannot_be_created() {
        let meta = SingleNodeMeta::default();
        assert!(meta.set_reserved_names(reserving(&["system"], &[])).status.ok);
        assert_eq!(
            meta.add_namespace(AddNamespaceRequest {
                namespace: "system".to_string(),
            })
            .status
            .code,
            "name_reserved"
        );
        // Everything else is still free.
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "tenant".to_string(),
            })
            .status
            .ok);
    }

    #[test]
    fn a_reserved_table_name_cannot_be_created_in_any_namespace() {
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "one".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "two".to_string(),
        });
        assert!(meta.set_reserved_names(reserving(&[], &["audit"])).status.ok);
        assert_eq!(make_table(&meta, "one", "audit").code, "name_reserved");
        assert_eq!(make_table(&meta, "two", "audit").code, "name_reserved");
        assert!(make_table(&meta, "one", "orders").ok);
    }

    #[test]
    fn a_table_cannot_be_created_inside_a_reserved_namespace() {
        // A table lands inside a namespace, so reserving the namespace has to
        // hold back the tables that would be created in it.
        let meta = SingleNodeMeta::default();
        assert!(meta.set_reserved_names(reserving(&["system"], &[])).status.ok);
        assert_eq!(make_table(&meta, "system", "orders").code, "name_reserved");
    }

    #[test]
    fn reserving_a_name_does_not_disturb_what_already_exists() {
        // Reserving is a statement about what may be created from now on. Using
        // it to delete something already serving would be a very surprising way
        // to take a table down.
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string(),
        });
        assert!(make_table(&meta, "tenant", "orders").ok);

        assert!(meta
            .set_reserved_names(reserving(&["tenant"], &["orders"]))
            .status
            .ok);

        assert_eq!(meta.list_tables().tables.len(), 1);
        assert!(meta
            .get_table_topology(GetTableTopologyRequest {
                namespace: "tenant".to_string(),
                table_name: "orders".to_string(),
                old_topology_version: 0,
                client_location: String::new(),
            })
            .status
            .ok);
    }

    #[test]
    fn releasing_a_name_makes_it_creatable_again() {
        let meta = SingleNodeMeta::default();
        assert!(meta.set_reserved_names(reserving(&["system"], &[])).status.ok);
        assert!(!meta
            .add_namespace(AddNamespaceRequest {
                namespace: "system".to_string(),
            })
            .status
            .ok);

        assert!(meta.set_reserved_names(ReservedNames::default()).status.ok);
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "system".to_string(),
            })
            .status
            .ok);
    }

    #[test]
    fn a_muted_metaserver_will_not_change_the_reserved_set() {
        let meta = SingleNodeMeta::default();
        assert!(meta.set_meta_change_muted(true).status.ok);
        assert_eq!(
            meta.set_reserved_names(reserving(&["system"], &[])).status.code,
            "meta_change_muted"
        );
    }

    #[test]
    fn the_reserved_set_survives_snapshot_and_replay() {
        // A peer that installs a snapshot must keep refusing what the operator
        // reserved, rather than quietly allowing it.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("reserved-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            assert!(meta
                .set_reserved_names(reserving(&["system"], &["audit"]))
                .status
                .ok);

            let snapshot = meta.export_snapshot();
            let peer = SingleNodeMeta::default();
            assert!(peer.install_snapshot(snapshot).status.ok);
            assert_eq!(
                peer.add_namespace(AddNamespaceRequest {
                    namespace: "system".to_string(),
                })
                .status
                .code,
                "name_reserved"
            );
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.reserved_names().reserved,
            reserving(&["system"], &["audit"])
        );
    }

    fn beat(meta: &SingleNodeMeta, addr: &str, loads: &[(u64, u64)]) {
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: addr.to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: loads
                .iter()
                .enumerate()
                .map(|(index, (keys, bytes))| ShardLoad {
                    shard_id: index as ShardId + 1,
                    key_count: *keys,
                    memory_bytes: *bytes,
                })
                .collect(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        });
    }

    fn server_summary(meta: &SingleNodeMeta, addr: &str) -> (u64, u64) {
        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == addr)
            .expect("registered");
        (server.load_key_count, server.load_memory_bytes)
    }

    fn loaded_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (addr, location) in [("node-a", "rack-1"), ("node-b", "rack-2")] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                server_addr: addr.to_string(),
                node_id: 1,
                location: location.to_string(),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            });
        }
        meta
    }

    #[test]
    fn a_heartbeat_summarises_the_load_it_reports() {
        let meta = loaded_meta();
        beat(&meta, "node-a", &[(10, 100), (20, 200), (30, 300)]);
        assert_eq!(server_summary(&meta, "node-a"), (60, 600));
    }

    #[test]
    fn a_lighter_heartbeat_lowers_the_summary_rather_than_adding_to_it() {
        // The summary describes the latest report, not the history of them.
        // Accumulating would make a server look permanently loaded once it had
        // ever been busy, and placement would stop sending it anything.
        let meta = loaded_meta();
        beat(&meta, "node-a", &[(10, 100), (20, 200)]);
        assert_eq!(server_summary(&meta, "node-a"), (30, 300));
        beat(&meta, "node-a", &[(5, 50)]);
        assert_eq!(server_summary(&meta, "node-a"), (5, 50));
    }

    #[test]
    fn a_server_that_reports_nothing_summarises_to_nothing() {
        let meta = loaded_meta();
        beat(&meta, "node-a", &[]);
        assert_eq!(server_summary(&meta, "node-a"), (0, 0));
    }

    #[test]
    fn placement_still_prefers_the_lighter_server() {
        // The behaviour the summary exists to serve, unchanged. This is the
        // regression guard: if the summary ever stops tracking the report,
        // placement silently starts choosing on stale numbers and only a test
        // like this notices.
        let meta = loaded_meta();
        beat(&meta, "node-a", &[(1_000, 10_000)]);
        beat(&meta, "node-b", &[(1, 10)]);
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        let shard = meta
            .get_table_topology(GetTableTopologyRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                old_topology_version: 0,
                client_location: String::new(),
            })
            .shards
            .into_iter()
            .next()
            .expect("one shard");
        assert_eq!(
            shard.primary,
            Some("node-b".to_string()),
            "the heavier server was proposed"
        );
    }

    #[test]
    fn the_summary_survives_a_snapshot_round_trip() {
        let meta = loaded_meta();
        beat(&meta, "node-a", &[(10, 100), (20, 200)]);
        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);
        assert_eq!(server_summary(&peer, "node-a"), (30, 300));
    }

    #[test]
    fn every_shard_of_a_table_is_spread_across_domains() {
        // The parsed locations are now indexed rather than re-derived inside
        // the placement loop, so they have to stay lined up with the candidate
        // list they were built from. If an index slipped, the separation check
        // would be reading some other server's location and replicas would
        // start landing in the same domain -- which is precisely what the
        // ladder exists to prevent, and it would fail silently.
        let meta = SingleNodeMeta::default();
        for (index, (addr, location)) in [
            ("node-a", "east/zone-a"),
            ("node-b", "east/zone-b"),
            ("node-c", "west/zone-c"),
            ("node-d", "west/zone-d"),
        ]
        .into_iter()
        .enumerate()
        {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                server_addr: addr.to_string(),
                node_id: index as u64 + 1,
                location: location.to_string(),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 4,
            replica_count: 2,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        });
        assert_eq!(topology.shards.len(), 4);
        for shard in topology.shards {
            assert_eq!(shard.replicas.len(), 2, "shard {} under-replicated", shard.shard_id);
            let domains = shard
                .replica_endpoints
                .iter()
                .map(|endpoint| {
                    endpoint
                        .location
                        .split('/')
                        .next()
                        .unwrap_or_default()
                        .to_string()
                })
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                domains.len(),
                2,
                "shard {} put both replicas in one domain: {:?}",
                shard.shard_id,
                shard.replicas
            );
        }
    }

<<<<<<< HEAD
    fn join_server(meta: &SingleNodeMeta, addr: &str) -> Status {
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: addr.to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        })
        .status
    }

    fn server_joined_at(meta: &SingleNodeMeta, addr: &str) -> u64 {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == addr)
            .expect("registered")
            .registered_at_ms
    }

    #[test]
    fn a_server_records_when_it_first_joined() {
        // `last_heartbeat_ms` is reset on every registration and `boot_time_ms`
        // is when the process started; neither answers how long this node has
        // been part of the cluster.
        let meta = SingleNodeMeta::default();
        assert!(join_server(&meta, "node-a").ok);
        assert!(server_joined_at(&meta, "node-a") > 0);
    }

    #[test]
    fn a_server_that_restarts_has_not_newly_joined() {
        // The whole value of the field: registering again must not reset it, or
        // it degrades into "when did this process last start", which is what
        // boot_time_ms already says.
        let meta = SingleNodeMeta::default();
        assert!(join_server(&meta, "node-a").ok);
        let first = server_joined_at(&meta, "node-a");

        assert!(join_server(&meta, "node-a").ok);
        assert_eq!(server_joined_at(&meta, "node-a"), first);
    }

    #[test]
    fn a_proxy_that_comes_back_keeps_its_original_join() {
        let meta = SingleNodeMeta::default();
        let join = || {
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 0,
                binary_version: "v1".to_string(),
            })
        };
        assert!(join().status.ok);
        let first = meta.list_proxies().proxies[0].registered_at_ms;
        assert!(first > 0);
        assert!(join().status.ok);
        assert_eq!(meta.list_proxies().proxies[0].registered_at_ms, first);
    }

    #[test]
    fn moving_a_shard_does_not_make_it_a_new_shard() {
        // A shard is registered again to move it between servers. That is the
        // same shard, and its age should say so.
        let meta = SingleNodeMeta::default();
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-a".to_string(),
        });
        let first = meta.get(1).location.expect("registered").registered_at_ms;
        assert!(first > 0);

        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-b".to_string(),
        });
        let moved = meta.get(1).location.expect("registered");
        assert_eq!(moved.server_addr, "node-b");
        assert_eq!(moved.registered_at_ms, first, "the move reset the age");
    }

    #[test]
    fn a_join_time_survives_snapshot_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("joined-mutations.jsonl");
        let first;
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            assert!(join_server(&meta, "node-a").ok);
            first = server_joined_at(&meta, "node-a");

            let peer = SingleNodeMeta::default();
            assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);
            assert_eq!(server_joined_at(&peer, "node-a"), first);
        }
        // Replay re-runs the registration, so this pins that a replayed join
        // does not silently restamp itself to the time of the replay.
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(server_joined_at(&recovered, "node-a"), first);
    }

||||||| a7277311
=======
    /// A two-shard table, its shards registered, with servers in two zones.
    fn pinnable(table_preference: &str) -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (index, (addr, location)) in [("node-a", "east/zone-a"), ("node-b", "west/zone-b")]
            .into_iter()
            .enumerate()
        {
            meta.register_server(RegisterServerRequest {
                server_addr: addr.to_string(),
                node_id: index as u64 + 1,
                location: location.to_string(),
                binary_version: "v1".to_string(),
                numa_nodes: Vec::new(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions {
                preferred_location: table_preference.to_string(),
                ..TableServingOptions::default()
            },
        });
        for shard_id in [1, 2] {
            meta.register(RegisterShardRequest {
                shard_id,
                server_addr: "node-a".to_string(),
            });
        }
        meta
    }

    fn pin(meta: &SingleNodeMeta, shard_id: ShardId, location: &str) -> Status {
        meta.pin_shard(ShardPinRequest {
            shard_id,
            location: location.to_string(),
        })
        .status
    }

    fn wanted(meta: &SingleNodeMeta, shard_id: ShardId) -> String {
        meta.shard_placements()
            .get(&shard_id)
            .map(|placement| placement.preferred_location.clone())
            .unwrap_or_default()
    }

    #[test]
    fn one_shard_can_be_pinned_without_moving_its_siblings() {
        // The whole point: a table could be pinned, a shard could not, so a
        // single hot shard wanting its own hardware meant pinning everything.
        let meta = pinnable("");
        assert!(pin(&meta, 1, "west/zone-b").ok);
        assert_eq!(wanted(&meta, 1), "west/zone-b");
        assert_eq!(wanted(&meta, 2), "", "the sibling was pinned too");
    }

    #[test]
    fn a_shards_own_pin_overrides_what_its_table_prefers() {
        let meta = pinnable("east/zone-a");
        assert_eq!(wanted(&meta, 1), "east/zone-a");
        assert!(pin(&meta, 1, "west/zone-b").ok);
        assert_eq!(wanted(&meta, 1), "west/zone-b");
        assert_eq!(wanted(&meta, 2), "east/zone-a", "the sibling stopped following the table");
    }

    #[test]
    fn releasing_a_pin_returns_the_shard_to_its_table() {
        // An empty location is the release, not a pin to nowhere.
        let meta = pinnable("east/zone-a");
        assert!(pin(&meta, 1, "west/zone-b").ok);
        assert!(pin(&meta, 1, "").ok);
        assert_eq!(wanted(&meta, 1), "east/zone-a");
    }

    #[test]
    fn pinning_is_rejected_when_unknown_or_unchanged() {
        let meta = pinnable("");
        assert_eq!(pin(&meta, 99, "east/zone-a").code, "shard_not_found");
        assert!(pin(&meta, 1, "east/zone-a").ok);
        assert_eq!(pin(&meta, 1, "east/zone-a").code, "not_modified");
    }

    #[test]
    fn a_muted_metaserver_will_not_pin_a_shard() {
        let meta = pinnable("");
        assert!(meta.set_meta_change_muted(true).status.ok);
        assert_eq!(pin(&meta, 1, "east/zone-a").code, "meta_change_muted");
    }

    #[test]
    fn a_pin_survives_snapshot_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("pin-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register(RegisterShardRequest {
                shard_id: 7,
                server_addr: "node-a".to_string(),
            });
            assert!(pin(&meta, 7, "east/zone-a").ok);

            let peer = SingleNodeMeta::default();
            assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);
            assert_eq!(
                peer.get(7).location.expect("registered").preferred_location,
                "east/zone-a"
            );
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.get(7).location.expect("registered").preferred_location,
            "east/zone-a"
        );
    }

    #[test]
    fn a_pin_to_a_location_with_no_live_server_does_not_strand_the_shard() {
        // The existing rule for table preferences, which a per-shard pin has to
        // inherit: a preference must not be able to take a shard out of service.
        let meta = pinnable("");
        assert!(pin(&meta, 1, "nowhere/at-all").ok);
        let placements = meta.shard_placements();
        assert!(placements.contains_key(&1), "the shard fell out of placement");
        let plans = meta.plan_placement_aware_rebalance(AutoRebalanceOptions::default());
        // Every planned owner is a live server, so a pin naming a place with
        // none cannot pull the shard anywhere.
        let live = meta
            .list_servers()
            .servers
            .into_iter()
            .map(|server| server.server_addr)
            .collect::<std::collections::BTreeSet<_>>();
        for plan in &plans {
            assert!(
                live.contains(&plan.to_server),
                "planned a move to a server that is not live: {plan:?}"
            );
        }
    }

>>>>>>> matrixark/main
    /// One shard registered to a server that has not said anything yet.
    fn drainable() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
            numa_nodes: Vec::new(),
        });
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-a".to_string(),
        });
        meta
    }

    /// A heartbeat reporting exactly the shards named as loaded.
    fn report_loaded(meta: &SingleNodeMeta, loaded: &[ShardId]) {
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "node-a".to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: loaded
                .iter()
                .map(|shard_id| ServerShardServingState {
                    shard_id: *shard_id,
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
                    total_records: 0,
                    storage_bytes: 0,
                    cache_memory_bytes: 0,
                    storage: ShardCanonicalStorageStats::default(),
                    block_store_bytes_written: 0,
                    wal_sequence: 0,
                    dirty_object_count: 0,
                    dirty_bucket_count: 0,
                })
                .collect(),
        });
    }

    fn only_shard(meta: &SingleNodeMeta) -> ShardListEntry {
        meta.list_shards(ListShardsRequest::default())
            .shards
            .into_iter()
            .next()
            .expect("one shard")
    }

    #[test]
    fn the_listing_says_whether_a_shard_is_serving() {
        // A shard can be taken out of service on its own, and the listing had
        // no way to show it.
        let meta = drainable();
        assert_eq!(only_shard(&meta).state, MetaEntityState::Normal);
        assert!(meta.freeze_shard(ShardStateRequest { shard_id: 1 }).status.ok);
        assert_eq!(only_shard(&meta).state, MetaEntityState::Frozen);
    }

    #[test]
    fn a_frozen_shard_still_held_by_its_owner_is_not_drained_yet() {
        // Freezing is recorded the moment it is asked for, but the datanode
        // holding the shard has work to do before it has really let go. Until
        // now nothing distinguished "frozen and gone" from "frozen and still
        // resident".
        let meta = drainable();
        report_loaded(&meta, &[1]);
        assert!(meta.freeze_shard(ShardStateRequest { shard_id: 1 }).status.ok);

        let entry = only_shard(&meta);
        assert_eq!(entry.state, MetaEntityState::Frozen);
        assert_eq!(
            entry.owner_reports_loaded,
            Some(true),
            "the owner still holds it, and the listing should say so"
        );
    }

    #[test]
    fn once_the_owner_lets_go_the_shard_reads_as_drained() {
        let meta = drainable();
        report_loaded(&meta, &[1]);
        meta.freeze_shard(ShardStateRequest { shard_id: 1 });
        assert_eq!(only_shard(&meta).owner_reports_loaded, Some(true));

        // The next heartbeat no longer names it.
        report_loaded(&meta, &[]);
        assert_eq!(only_shard(&meta).owner_reports_loaded, Some(false));
    }

    #[test]
    fn a_server_that_never_reports_is_not_read_as_reporting_nothing() {
        // The distinction the divergence check already makes before it is
        // willing to judge a server: silence is not a claim.
        let meta = drainable();
        assert_eq!(
            only_shard(&meta).owner_reports_loaded,
            None,
            "silence was read as an empty report"
        );
    }

    #[test]
    fn a_serving_shard_its_owner_does_not_hold_is_visible_too() {
        // The same field read the other way round: this is a divergence, and
        // it shows up in the listing rather than only in a checker's report.
        let meta = drainable();
        report_loaded(&meta, &[2]);
        let entry = only_shard(&meta);
        assert_eq!(entry.state, MetaEntityState::Normal);
        assert_eq!(entry.owner_reports_loaded, Some(false));
    }

    /// A proxy attached to a group serving one namespace.
    fn shedding_meta(drop_percent: u8) -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: "tenant".to_string(),
            location: "rack-1".to_string(),
            config_version: 0,
            binary_version: "v1".to_string(),
        });
        assert!(
            meta.put_proxy_group(PutProxyGroupRequest {
                group: "front".to_string(),
                namespace: "tenant".to_string(),
                location: String::new(),
                instance_num: 1,
                drop_percent,
            })
            .status
            .ok
        );
        assert!(
            meta.set_proxy_group(ProxyAttachment {
                proxy_addr: "proxy-a".to_string(),
                group: "front".to_string(),
            })
            .status
            .ok
        );
        meta
    }

    fn beat_proxy(meta: &SingleNodeMeta, config_version: u64) -> ProxyHeartbeatResponse {
        meta.proxy_heartbeat(ProxyHeartbeatRequest {
            proxy_addr: "proxy-a".to_string(),
            namespace: "tenant".to_string(),
            config_version,
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
        })
    }

    #[test]
    fn the_metaserver_can_tell_a_proxy_to_shed_load() {
        // The proxy has always implemented this -- it reads the figure from its
        // heartbeat, applies it, and exports it as a metric -- but every
        // response carried a hard-coded zero, so the only way to pull the lever
        // was to restart each proxy with different configuration.
        let meta = shedding_meta(25);
        assert_eq!(beat_proxy(&meta, 0).drop_percent, Some(25));
    }

    #[test]
    fn a_group_that_asks_for_nothing_sheds_nothing() {
        let meta = shedding_meta(0);
        assert_eq!(
            beat_proxy(&meta, 0).drop_percent,
            Some(0),
            "a group that asks for zero has still spoken, which is not the same as silence"
        );
    }

    #[test]
    fn changing_the_share_tells_the_proxy_to_re_read() {
        // The proxy only re-reads when the config version moves, so a change
        // nobody is told about is a change that does not happen.
        let meta = shedding_meta(0);
        let settled = beat_proxy(&meta, 0).config_version;

        assert!(
            meta.put_proxy_group(PutProxyGroupRequest {
                group: "front".to_string(),
                namespace: "tenant".to_string(),
                location: String::new(),
                instance_num: 1,
                drop_percent: 40,
            })
            .status
            .ok
        );
        let after = beat_proxy(&meta, settled);
        assert!(
            after.config_version > settled,
            "the version did not move, so an attached proxy would never re-read"
        );
        assert!(after.config_changed);
        assert_eq!(after.drop_percent, Some(40));
    }

    #[test]
    fn an_unattached_proxy_is_not_told_to_shed_anything() {
        // It is not serving a namespace, so a share of its traffic is a share
        // of nothing.
        let meta = shedding_meta(60);
        assert!(
            meta.set_proxy_group(ProxyAttachment {
                proxy_addr: "proxy-a".to_string(),
                group: String::new(),
            })
            .status
            .ok
        );
        assert_eq!(
            beat_proxy(&meta, 0).drop_percent,
            None,
            "no group is holding an opinion about this proxy, so the heartbeat must not \n             carry one -- it used to carry 0, which erased whatever the proxy was \n             configured with"
        );
    }

    #[test]
    fn the_share_survives_snapshot_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("shed-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            assert!(
                meta.put_proxy_group(PutProxyGroupRequest {
                    group: "front".to_string(),
                    namespace: "tenant".to_string(),
                    location: String::new(),
                    instance_num: 1,
                    drop_percent: 35,
                })
                .status
                .ok
            );
            let peer = SingleNodeMeta::default();
            assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);
            assert_eq!(peer.list_proxy_groups().groups[0].drop_percent, 35);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(recovered.list_proxy_groups().groups[0].drop_percent, 35);
    }

    #[test]
    fn a_restored_snapshot_is_still_there_after_a_restart() {
        // Restoring a snapshot answered ok and rolled the state back, and the
        // next start put everything back. Replay reapplies every mutation in
        // the log, and the install left no record in it -- so the changes the
        // operator rolled back were all still in there. The rollback looked
        // like it took, right up until the process restarted.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta.log");

        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "keep".to_string()
            })
            .status
            .ok);
        let snapshot = meta.export_snapshot();
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "unwanted".to_string()
            })
            .status
            .ok);
        assert!(meta.install_snapshot(snapshot).status.ok);

        let names = |meta: &SingleNodeMeta| -> Vec<String> {
            let mut out: Vec<String> = meta
                .list_namespaces()
                .namespaces
                .into_iter()
                .map(|entry| entry.namespace)
                .collect();
            out.sort();
            out
        };
        assert_eq!(names(&meta), vec!["keep".to_string()]);

        drop(meta);
        let reopened = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            names(&reopened),
            vec!["keep".to_string()],
            "the restart brought back what the restore rolled away"
        );

        // And the restored state is still a working metaserver, not a frozen
        // copy: a change after the restore survives its own restart too.
        assert!(reopened
            .add_namespace(AddNamespaceRequest {
                namespace: "after".to_string()
            })
            .status
            .ok);
        drop(reopened);
        let again = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            names(&again),
            vec!["after".to_string(), "keep".to_string()],
            "a change made after the restore did not survive"
        );
    }

    #[test]
    fn a_snapshot_the_log_could_not_replay_is_refused_before_it_is_recorded() {
        // The install is recorded before it is applied, so a snapshot that
        // cannot become state must be refused before it reaches the log --
        // otherwise replay would hit the same failure on every start.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta.log");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "keep".to_string()
            })
            .status
            .ok);

        let mut bad = meta.export_snapshot();
        bad.format_version = 99;
        let refused = meta.install_snapshot(bad);
        assert_eq!(refused.status.code, "bad_snapshot");

        // The log still replays, and still says what it said before.
        drop(meta);
        let reopened = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(reopened.list_namespaces().namespaces.len(), 1);
    }

    #[test]
    fn the_learned_shard_index_answers_what_the_scan_answered() {
        // The index exists because asking every table which shard it owns, once
        // per shard, cost shards times tables on a background round. It is only
        // worth having if it says the same thing -- including for two tables
        // whose ranges overlap, which the scan resolved by table map order.
        use crate::meta::topology_helpers::{shard_owning_tables, table_for_shard};

        for overlapping in [false, true] {
            let meta = SingleNodeMeta::default();
            assert!(meta
                .add_namespace(AddNamespaceRequest {
                    namespace: "ns".to_string()
                })
                .status
                .ok);
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: "node-a".to_string(),
                    node_id: 1,
                    location: "rack-1".to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);

            // "a" owns 100..108. "b" owns 108..116, or 104..112 when the ranges
            // are made to overlap.
            let second_first = if overlapping { 104 } else { 108 };
            for (name, first) in [("a", 100u64), ("b", second_first)] {
                assert!(meta
                    .add_table(AddTableRequest {
                        namespace: "ns".to_string(),
                        table_name: name.to_string(),
                        first_shard_id: first,
                        shard_count: 8,
                        replica_count: 1,
                        partition_version: 0,
                        serving_options: TableServingOptions::default(),
                    })
                    .status
                    .ok);
            }
            // Registered shards across both ranges, and two that no table owns.
            for shard_id in [100u64, 103, 104, 107, 108, 111, 115, 200, 99] {
                assert!(meta
                    .register(RegisterShardRequest {
                        registered_at_ms: 0,
                        shard_id,
                        server_addr: "node-a".to_string(),
                    })
                    .status
                    .ok);
            }

            let state = meta.inner.read().expect("meta lock poisoned");
            let learned = shard_owning_tables(&state);
            for shard_id in state.shards.keys() {
                let scanned = table_for_shard(&state, *shard_id)
                    .map(|table| table_key(&table.info.namespace, &table.info.table_name));
                let indexed = learned
                    .get(shard_id)
                    .map(|table| table_key(&table.info.namespace, &table.info.table_name));
                assert_eq!(
                    indexed, scanned,
                    "shard {shard_id} resolved differently (overlapping={overlapping})"
                );
            }
        }
    }

    #[test]
    fn a_tombstone_with_no_stamp_is_still_left_alone() {
        // Retention starts from the drop stamps now instead of walking every
        // resource. That is the same set only because a tombstone with no stamp
        // is never collected -- it predates the stamps, and treating it as
        // infinitely old would forget the whole history on the first round after
        // an upgrade. The scan used to produce it as a candidate for the planner
        // to reject; starting from the stamps it is simply not a candidate, and
        // the outcome has to stay identical.
        let dir = tempfile::tempdir().unwrap();
        let meta = SingleNodeMeta::with_mutation_log(dir.path().join("meta.log")).unwrap();
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        for name in ["stamped", "unstamped"] {
            assert!(meta
                .add_table(AddTableRequest {
                    namespace: "ns".to_string(),
                    table_name: name.to_string(),
                    first_shard_id: if name == "stamped" { 10 } else { 20 },
                    shard_count: 1,
                    replica_count: 1,
                    partition_version: 0,
                    serving_options: TableServingOptions::default(),
                })
                .status
                .ok);
            assert!(meta
                .delete_table(DeleteTableRequest {
                    namespace: "ns".to_string(),
                    table_name: name.to_string(),
                })
                .status
                .ok);
        }

        // Take the stamp away from one of them, which is what an upgrade from
        // before the stamps existed leaves behind.
        {
            let mut state = meta.inner.write().expect("meta lock poisoned");
            let key = dropped_key("table", &table_key("ns", "unstamped"));
            assert!(state.dropped_since_ms.remove(&key).is_some());
        }

        let report = meta.purge_expired_meta(MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            max_purges_per_round: 20,
        });
        assert!(report.status.ok);
        assert_eq!(
            report.plan.tables,
            vec![table_key("ns", "stamped")],
            "the stamped tombstone is collected and the unstamped one is not"
        );

        let remaining = meta
            .list_tables()
            .tables
            .into_iter()
            .map(|table| table.table_name)
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec!["unstamped".to_string()]);
    }

    #[test]
    fn a_stamp_for_a_resource_that_is_gone_is_not_reported_as_a_purge() {
        // The round starts from the drop stamps, so a stamp left behind for a
        // resource that is no longer in the state would become a candidate, and
        // the round would report forgetting something that was already gone --
        // the plan is what the metrics and the operator read.
        let dir = tempfile::tempdir().unwrap();
        let meta = SingleNodeMeta::with_mutation_log(dir.path().join("meta.log")).unwrap();
        {
            let mut state = meta.inner.write().expect("meta lock poisoned");
            state
                .dropped_since_ms
                .insert(dropped_key("table", &table_key("ns", "vanished")), 1);
            state
                .dropped_since_ms
                .insert(dropped_key("server", "node-gone"), 1);
        }

        let report = meta.purge_expired_meta(MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            max_purges_per_round: 20,
        });
        assert!(report.status.ok);
        assert!(
            report.plan.is_empty(),
            "reported purging resources that were not there: {:?}",
            report.plan
        );
    }

    #[test]
    fn a_purged_table_still_takes_its_shard_routes_with_it() {
        // A round with nothing to collect no longer derives who owns each shard
        // or which table owns each shard -- neither can change what an empty
        // round returns, and deriving them walked every registered shard. The
        // risk in that is a round which does have something to collect quietly
        // losing the shard routes, and the planner's own test supplies those
        // maps ready-made, so it would not notice.
        let dir = tempfile::tempdir().unwrap();
        let meta = SingleNodeMeta::with_mutation_log(dir.path().join("meta.log")).unwrap();
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "gone".to_string(),
                first_shard_id: 400,
                shard_count: 3,
                replica_count: 1,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);
        for shard_id in [400u64, 401, 402] {
            assert!(meta
                .register(RegisterShardRequest {
                    registered_at_ms: 0,
                    shard_id,
                    server_addr: "node-a".to_string(),
                })
                .status
                .ok);
        }

        // Nothing is dropped yet, so the round has nothing to say about shards.
        let quiet = meta.plan_meta_retention_now(MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            max_purges_per_round: 20,
        });
        assert!(quiet.is_empty(), "{quiet:?}");

        assert!(meta
            .delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "gone".to_string(),
            })
            .status
            .ok);

        let report = meta.purge_expired_meta(MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            max_purges_per_round: 20,
        });
        assert!(report.status.ok);
        assert_eq!(report.plan.tables, vec![table_key("ns", "gone")]);
        assert_eq!(
            report.plan.shards,
            vec![400, 401, 402],
            "the purged table's shard routes were left behind"
        );
        assert!(
            meta.list_shards(ListShardsRequest {
                server_addr: String::new(),
                after_shard_id: 0,
                limit: 0,
            })
            .shards
            .is_empty(),
            "the routes are still in the state"
        );
    }

    #[test]
    fn writers_share_a_barrier_without_losing_a_record() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(LocalMetaMutationLog::new(dir.path().join("meta.log")).unwrap());

        // One writer on its own is covered by its own barrier, and its record is
        // readable the moment append returns.
        log.append(
            &MetaMutation::RegisterShard(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: 1,
                server_addr: "solo".to_string(),
            }),
            10,
        )
        .unwrap();
        assert_eq!(log.load().unwrap().len(), 1, "the first record is not there");

        let writers = 8usize;
        let each = 12usize;
        let mut handles = Vec::new();
        for w in 0..writers {
            let log = Arc::clone(&log);
            handles.push(std::thread::spawn(move || {
                for i in 0..each {
                    log.append(
                        &MetaMutation::RegisterShard(RegisterShardRequest {
                            registered_at_ms: 0,
                            shard_id: (w * each + i) as u64 + 100,
                            server_addr: format!("node-{w}"),
                        }),
                        i as u64,
                    )
                    .unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // Every record every writer was told was durable is in the log, once.
        let records = log.load().unwrap();
        assert_eq!(
            records.len(),
            writers * each + 1,
            "a record a writer was told had landed is missing"
        );
        let mut shard_ids = records
            .iter()
            .filter_map(|record| match &record.mutation {
                MetaMutation::RegisterShard(request) => Some(request.shard_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids.dedup();
        assert_eq!(
            shard_ids.len(),
            writers * each + 1,
            "a record was written twice or read back wrong"
        );

        // How many barriers this took is deliberately not asserted. The
        // barrier counter is process-wide and every other test in the binary
        // adds to it, so a count read here says nothing about this log -- and a
        // test that passes alone and fails in the suite is worse than no test.
        // What matters is above: every record a writer was told had landed is
        // in the log, exactly once.
    }

    #[test]
    fn metaserver_safe_mode_cooldown_blocks_rejoin_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("safe-mode-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "cooldown-server".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "cooldown-proxy".to_string(),
            namespace: "ns".to_string(),
            location: "z".to_string(),
            config_version: 1,
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));

        let report = meta.freeze_stale_resources_with_policy(
            0,
            SafeModePolicy {
                server_freeze_cooldown_ms: 60_000,
                proxy_freeze_cooldown_ms: 60_000,
            },
        );
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers, vec!["cooldown-server".to_string()]);
        assert_eq!(report.frozen_proxies, vec!["cooldown-proxy".to_string()]);

        let safe_mode = meta.safe_mode_report();
        assert!(safe_mode.status.ok);
        assert_eq!(
            safe_mode.blocked_servers,
            vec!["cooldown-server".to_string()]
        );
        assert_eq!(
            safe_mode.blocked_proxies,
            vec!["cooldown-proxy".to_string()]
        );
        assert_eq!(
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "cooldown-server".to_string(),
                node_id: 1,
                location: "z".to_string(),
                binary_version: "v".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );
        assert_eq!(
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "cooldown-proxy".to_string(),
                namespace: "ns".to_string(),
                location: "z".to_string(),
                config_version: 1,
                binary_version: "v".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );

        let server_heartbeat = meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "cooldown-server".to_string(),
            boot_time_ms: 1,
            binary_version: "v".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        });
        assert_eq!(server_heartbeat.status.code, "resource_frozen");
        assert!(server_heartbeat.forbid_auto_register);
        assert_eq!(
            meta.proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "cooldown-proxy".to_string(),
                boot_time_ms: 0,
                namespace: "ns".to_string(),
                config_version: 1,
                binary_version: "v".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );

        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.safe_mode_report().blocked_servers,
            vec!["cooldown-server".to_string()]
        );
        assert_eq!(
            recovered.safe_mode_report().blocked_proxies,
            vec!["cooldown-proxy".to_string()]
        );

        let snapshot = meta.export_snapshot();
        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(snapshot).status.ok);
        assert_eq!(
            restored.safe_mode_report().blocked_servers,
            vec!["cooldown-server".to_string()]
        );
        assert_eq!(
            restored.safe_mode_report().blocked_proxies,
            vec!["cooldown-proxy".to_string()]
        );
    }

    #[test]
    fn metaserver_serves_table_topology_and_version_not_modified() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z1".to_string(),
            binary_version: String::new(),
        });
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "s2".to_string(),
            node_id: 2,
            location: "z2".to_string(),
            binary_version: String::new(),
        });
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 10,
            server_addr: "s1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 10,
                shard_count: 2,
                replica_count: 2,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert!(topo.status.ok);
        assert_eq!(topo.shards.len(), 2);
        assert_eq!(topo.shards[0].primary.as_deref(), Some("s1"));
        assert_eq!(topo.shards[0].replicas.len(), 2);
        let topology_version = topo.table.as_ref().unwrap().topology_version;
        let report = meta.topology_version_report(TopologyVersionRequest {
            old_topology_version: 0,
        });
        assert!(report.status.ok);
        assert_eq!(
            report.current_topology_version,
            meta.stats().topology_version
        );
        assert_eq!(report.server_count, 2);
        assert_eq!(report.table_count, 1);
        assert_eq!(report.shard_route_count, 1);
        assert_eq!(report.changed_tables.len(), 1);
        assert_eq!(report.changed_tables[0].topology_version, topology_version);
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == "register_server" && event.resource == "server:s1"));
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == "register_shard" && event.resource == "shard:10"));
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == "add_table" && event.resource == "table:ns/tbl"));

        let unchanged_report = meta.topology_version_report(TopologyVersionRequest {
            old_topology_version: report.current_topology_version,
        });
        assert!(unchanged_report.unchanged);
        assert!(unchanged_report.changed_tables.is_empty());
        assert!(unchanged_report.events.is_empty());

        let unchanged = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: topology_version,
        });
        assert!(unchanged.status.ok);
        assert!(unchanged.unchanged);
        assert!(unchanged.shards.is_empty());
    }

    #[test]
    fn metaserver_delete_table_marks_dropped_and_rejects_topology() {
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 10,
                shard_count: 2,
                replica_count: 1,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        let created_version = meta.list_tables().tables[0].topology_version;

        let deleted = meta.delete_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert!(deleted.status.ok, "{deleted:?}");
        let table = meta.list_tables().tables[0].clone();
        assert_eq!(table.state, MetaEntityState::Dropped);
        assert!(table.topology_version > created_version);
        assert_eq!(meta.list_namespaces().namespaces[0].table_count, 0);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(topology.status.code, "table_not_found");
        assert_eq!(topology.table.unwrap().state, MetaEntityState::Dropped);
        assert!(topology.shards.is_empty());

        let duplicate = meta.delete_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert_eq!(duplicate.status.code, "table_not_found");
    }

    #[test]
    fn metaserver_freeze_table_blocks_topology_update_and_finish_load_until_unfrozen() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 42,
                shard_count: 1,
                replica_count: 1,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );

        let frozen = meta.freeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert!(frozen.status.ok);
        assert_eq!(meta.list_tables().tables[0].state, MetaEntityState::Frozen);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(topology.status.code, "resource_frozen");
        assert!(topology.shards.is_empty());

        let update = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(2),
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(update.status.code, "resource_frozen");

        let finish = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 1,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(finish.status.code, "resource_frozen");

        let unfrozen = meta.unfreeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert!(unfrozen.status.ok);
        assert_eq!(meta.list_tables().tables[0].state, MetaEntityState::Normal);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert!(topology.status.ok);
        assert_eq!(topology.shards.len(), 1);
        let finish = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 1,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert!(finish.status.ok);
    }

    #[test]
    fn setting_a_serving_option_to_its_default_value_is_a_real_change() {
        // "Never shed this table" is spelled drop_percent: 0, and 0 is also the
        // default. An update carrying it used to compare equal to what the table
        // already had, so the metaserver answered not_modified and did nothing --
        // the operator was told plainly that nothing had changed, while the table
        // went on inheriting whatever shedding its clients were configured with.
        //
        // The table now records that it set the field, so this is a change: it says
        // something the table was not saying before.
        let meta = SingleNodeMeta::default();
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });
        let created = meta.list_tables().tables[0].clone();
        assert_eq!(created.serving_options.drop_percent, 0);
        assert!(
            !created
                .serving_options
                .table_decides(TableServingField::DropPercent),
            "a table that never spoke for drop_percent must leave it to the client"
        );

        let updated = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: Some(TableServingOptionsPatch {
                drop_percent: Some(0),
                ..TableServingOptionsPatch::default()
            }),
        });
        assert!(
            updated.status.ok,
            "asking to shed nothing must be accepted, not answered not_modified: {updated:?}"
        );

        let table = meta.list_tables().tables[0].clone();
        assert_eq!(table.serving_options.drop_percent, 0);
        assert!(
            table
                .serving_options
                .table_decides(TableServingField::DropPercent),
            "the table has now spoken for drop_percent and must decide it"
        );
        assert!(
            table.topology_version > created.topology_version,
            "clients only pick this up if the topology version moves"
        );

        // Saying the same thing twice really is unchanged.
        let again = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: Some(TableServingOptionsPatch {
                drop_percent: Some(0),
                ..TableServingOptionsPatch::default()
            }),
        });
        assert_eq!(again.status.code, "not_modified");
    }

    #[test]
    fn metaserver_update_table_expands_topology_and_guards_unsafe_changes() {
        let meta = SingleNodeMeta::default();
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });
        let created = meta.list_tables().tables[0].clone();

        let updated = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(4),
            replica_count: Some(2),
            first_shard_id: None,
            partition_version: None,
            serving_options: None,
        });
        assert!(updated.status.ok, "{updated:?}");
        let table = meta.list_tables().tables[0].clone();
        assert_eq!(table.shard_count, 4);
        assert_eq!(table.replica_count, 2);
        assert!(table.topology_version > created.topology_version);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: created.topology_version,
        });
        assert!(topology.status.ok, "{topology:?}");
        assert_eq!(topology.shards.len(), 4);
        assert_eq!(topology.shards[3].shard_id, 103);

        let unchanged = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(4),
            replica_count: Some(2),
            first_shard_id: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(unchanged.status.code, "not_modified");

        let shrink = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(1),
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(shrink.status.code, "bad_request");

        let retarget = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: Some(200),
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(retarget.status.code, "bad_request");

        assert!(
            meta.delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
            })
            .status
            .ok
        );
        let dropped_update = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(5),
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(dropped_update.status.code, "table_not_found");
    }

    #[test]
    fn metaserver_table_serving_options_update_and_snapshot_round_trip() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "opts".to_string(),
                first_shard_id: 10,
                shard_count: 1,
                replica_count: 1,
                partition_version: 0,
                serving_options: TableServingOptions {
                    pin_primary: false,
                    replica_read_policy: "first_replica".to_string(),
                    preferred_location: "zone-a".to_string(),
                    drop_percent: 7,
                    max_read_retries: 3,
                    max_write_retries: 2,
                    retry_backoff_ms: 11,
                    continuous_failed_time_ms: 22,
                    io_timeout_ms: 333,
                    connect_timeout_ms: 444,
                    set_fields: Default::default(),
                },
            })
            .status
            .ok
        );
        assert_eq!(
            meta.get_table_topology(GetTableTopologyRequest {
                client_location: String::new(),
                namespace: "ns".to_string(),
                table_name: "opts".to_string(),
                old_topology_version: 0,
            })
            .table
            .unwrap()
            .serving_options
            .drop_percent,
            7
        );

        let updated = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "opts".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: Some(TableServingOptionsPatch {
                replica_read_policy: Some("round_robin_replica".to_string()),
                drop_percent: Some(19),
                max_read_retries: Some(4),
                ..TableServingOptionsPatch::default()
            }),
        });
        assert!(updated.status.ok, "{updated:?}");
        let table = meta.list_tables().tables[0].clone();
        assert_eq!(
            table.serving_options.replica_read_policy,
            "round_robin_replica"
        );
        assert_eq!(table.serving_options.drop_percent, 19);
        assert_eq!(table.serving_options.max_read_retries, 4);
        assert_eq!(table.serving_options.max_write_retries, 2);

        let invalid = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "opts".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: None,
            partition_version: None,
            serving_options: Some(TableServingOptionsPatch {
                replica_read_policy: Some("unknown_policy".to_string()),
                ..TableServingOptionsPatch::default()
            }),
        });
        assert_eq!(invalid.status.code, "bad_request");

        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(meta.export_snapshot()).status.ok);
        assert_eq!(
            restored.list_tables().tables[0]
                .serving_options
                .drop_percent,
            19
        );
    }

    #[test]
    fn metaserver_topology_prefers_lower_load_replicas() {
        let meta = SingleNodeMeta::default();
        for (server_addr, key_count, memory_bytes) in [
            ("hot", 10_000, 10_000),
            ("cool", 10, 10),
            ("warm", 100, 100),
        ] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: "z".to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count,
                    memory_bytes,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 100,
            shard_count: 1,
            replica_count: 2,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
            vec!["cool".to_string(), "warm".to_string()]
        );
    }

    #[test]
    fn metaserver_topology_uses_runtime_load_when_record_load_ties() {
        let meta = SingleNodeMeta::default();
        for (server_addr, queue_depth, dirty_object_count, degraded) in [
            ("busy", 50, 100, false),
            ("cool", 0, 0, false),
            ("degraded", 0, 0, true),
        ] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: "z".to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count: 10,
                    memory_bytes: 10,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad {
                    queue_depth,
                    dirty_object_count,
                    degraded_reasons: degraded
                        .then(|| "background_queue_full".to_string())
                        .into_iter()
                        .collect(),
                    ..ServerRuntimeLoad::default()
                },
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "runtime_load".to_string(),
            first_shard_id: 400,
            shard_count: 1,
            replica_count: 2,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "runtime_load".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
            vec!["cool".to_string(), "busy".to_string()]
        );
    }

    #[test]
    fn asking_only_for_status_gives_the_same_answer_without_the_shards() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        for (namespace, table_name) in [
            ("ns", "normal"),
            ("ns", "frozen"),
            ("ns", "dropped"),
            ("frozen-ns", "in-frozen-ns"),
        ] {
            meta.add_table(AddTableRequest {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                first_shard_id: 1,
                shard_count: 4,
                replica_count: 1,
                partition_version: 1,
                serving_options: Default::default(),
            });
        }
        for shard in 1..=4u64 {
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: shard,
                server_addr: "node-a".to_string(),
            });
        }
        meta.freeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "frozen".to_string(),
        });
        meta.delete_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "dropped".to_string(),
        });
        meta.freeze_namespace(AddNamespaceRequest {
            namespace: "frozen-ns".to_string(),
        });

        for (namespace, table_name) in [
            ("ns", "normal"),
            ("ns", "frozen"),
            ("ns", "dropped"),
            ("frozen-ns", "in-frozen-ns"),
            ("ns", "never-created"),
            ("no-such-ns", "never-created"),
        ] {
            let full = meta.get_table_topology(GetTableTopologyRequest {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                old_topology_version: 0,
                client_location: String::new(),
            });
            let cheap = meta.get_table_topology(GetTableTopologyRequest::status_only(
                namespace.to_string(),
                table_name.to_string(),
            ));

            // Whatever the full answer says about whether this table can be
            // served, the cheap one says the same. Opening and closing a table
            // report this status straight back to the caller.
            assert_eq!(
                cheap.status.ok, full.status.ok,
                "{namespace}.{table_name}: the two answers disagree on ok"
            );
            assert_eq!(
                cheap.status.code, full.status.code,
                "{namespace}.{table_name}: the two answers disagree on the code"
            );
            // And the version, which is what opening a table reads.
            assert_eq!(
                cheap.table.as_ref().map(|table| table.topology_version),
                full.table.as_ref().map(|table| table.topology_version),
                "{namespace}.{table_name}: the two answers disagree on the version"
            );
            // The point of asking this way: no shard list is built. If this
            // ever carries shards again the saving is gone, and that is not
            // something a timing assertion could tell you reliably.
            assert!(
                cheap.shards.is_empty(),
                "{namespace}.{table_name}: the cheap answer built a shard list"
            );
        }

        // The full answer really does carry shards for a servable table, so the
        // check above is not passing because there was nothing to build.
        let full = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "normal".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        });
        assert_eq!(
            full.shards.len(),
            4,
            "the full answer stopped carrying shards, so this test proves nothing"
        );
    }

    #[test]
    fn metaserver_topology_avoids_unhealthy_shard_serving_states() {
        let meta = SingleNodeMeta::default();
        for (server_addr, serving_state) in [
            ("freezing", "freezing"),
            ("failed", "failed"),
            ("serving", "serving"),
        ] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: "z".to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count: 10,
                    memory_bytes: 10,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: vec![ServerShardServingState {
                    shard_id: 1,
                    serving_state: serving_state.to_string(),
                    loaded: serving_state != "failed",
                    ..ServerShardServingState::default()
                }],
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "serving_state".to_string(),
            first_shard_id: 500,
            shard_count: 1,
            replica_count: 2,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "serving_state".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
            vec!["serving".to_string(), "freezing".to_string()]
        );
    }

    #[test]
    fn metaserver_topology_prefers_location_diversity_before_same_zone_load() {
        let meta = SingleNodeMeta::default();
        for (server_addr, location, key_count, memory_bytes) in [
            ("zone-a-cool", "zone-a", 10, 10),
            ("zone-a-warm", "zone-a", 20, 20),
            ("zone-b-hot", "zone-b", 10_000, 10_000),
        ] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: location.to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count,
                    memory_bytes,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 200,
            shard_count: 1,
            replica_count: 2,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
            vec!["zone-a-cool".to_string(), "zone-b-hot".to_string()]
        );
        assert_eq!(topo.shards[0].primary.as_deref(), Some("zone-a-cool"));
    }

    #[test]
    fn metaserver_topology_prefers_host_diversity_before_same_host_load() {
        let meta = SingleNodeMeta::default();
        for (server_addr, location, key_count, memory_bytes) in [
            ("10.0.0.1:18001", "zone-a", 10, 10),
            ("10.0.0.1:18002", "zone-b", 20, 20),
            ("10.0.0.2:18001", "zone-c", 10_000, 10_000),
        ] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: location.to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count,
                    memory_bytes,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 300,
            shard_count: 1,
            replica_count: 2,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
            vec!["10.0.0.1:18001".to_string(), "10.0.0.2:18001".to_string()]
        );
    }

    #[test]
    fn metaserver_topology_fills_same_host_when_distinct_hosts_are_insufficient() {
        let meta = SingleNodeMeta::default();
        for (server_addr, location, key_count, memory_bytes) in [
            ("10.0.0.1:18001", "zone-a", 10, 10),
            ("10.0.0.1:18002", "zone-b", 20, 20),
        ] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: location.to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count,
                    memory_bytes,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 301,
            shard_count: 1,
            replica_count: 2,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
            vec!["10.0.0.1:18001".to_string(), "10.0.0.1:18002".to_string()]
        );
    }

    #[test]
    fn metaserver_counts_proxy_restarts_from_boot_time() {
        let meta = SingleNodeMeta::default();
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "127.0.0.1:17000".to_string(),
                namespace: "ns".to_string(),
                location: String::new(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);

        let beat = |boot_time_ms: u64| {
            meta.proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "127.0.0.1:17000".to_string(),
                boot_time_ms,
                namespace: "ns".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
        };
        let recorded = || {
            meta.list_proxies()
                .proxies
                .into_iter()
                .find(|proxy| proxy.proxy_addr == "127.0.0.1:17000")
                .expect("proxy is registered")
        };

        assert!(beat(1_000).status.ok);
        assert_eq!(recorded().boot_time_ms, 1_000);
        assert_eq!(recorded().restart_count, 0);

        assert!(beat(1_000).status.ok);
        assert_eq!(recorded().restart_count, 0);

        assert!(beat(2_000).status.ok);
        assert_eq!(recorded().boot_time_ms, 2_000);
        assert_eq!(recorded().restart_count, 1);

        assert!(beat(0).status.ok);
        assert_eq!(recorded().boot_time_ms, 2_000);
        assert_eq!(recorded().restart_count, 1);
    }
    #[test]
    fn metaserver_tracks_proxy_heartbeat_config_changes() {
        let meta = SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "p1".to_string(),
            namespace: "ns".to_string(),
            location: "z1".to_string(),
            config_version: 3,
            binary_version: "v".to_string(),
        });
        let response = meta.proxy_heartbeat(ProxyHeartbeatRequest {
            boot_time_ms: 0,
            proxy_addr: "p1".to_string(),
            namespace: "ns".to_string(),
            config_version: 2,
            binary_version: "v2".to_string(),
        });
        assert!(response.status.ok);
        assert!(response.config_changed);
        assert_eq!(response.config_version, 3);
        assert_eq!(response.serving_mode, "serving");
        assert_eq!(
            response.drop_percent, None,
            "the metaserver holds no per-proxy drop_percent, so a heartbeat must not appear              to set one -- it used to answer 0, which the proxy applied over an operator's drain"
        );
        assert_eq!(meta.list_proxies().proxies[0].binary_version, "v2");

        let frozen = meta.freeze_proxy(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "p1".to_string(),
            freeze_cooldown_ms: 0,
        });
        assert!(frozen.status.ok, "{frozen:?}");
        let response = meta.proxy_heartbeat(ProxyHeartbeatRequest {
            boot_time_ms: 0,
            proxy_addr: "p1".to_string(),
            namespace: "ns".to_string(),
            config_version: 3,
            binary_version: "v3".to_string(),
        });
        assert_eq!(response.status.code, "resource_frozen");
        assert_eq!(response.serving_mode, "not_serving");
        assert!(response.config_changed);
    }

    #[test]
    fn metaserver_finish_load_updates_shard_route() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        let ack = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert!(ack.status.ok);
        assert_eq!(meta.get(42).location.unwrap().server_addr, "s1");
        assert_eq!(meta.stats().load_finish_total, 1);
    }

    // shared-corpus: control_scheduler_token_stale_rejection;
    #[test]
    fn metaserver_finish_load_enforces_scheduler_generation_and_replays_it() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("scheduler-finish-load.jsonl");
        let snapshot_path = dir.path().join("scheduler-finish-load-snapshot.json");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "s1".to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: vec![ServerShardServingState {
                shard_id: 42,
                serving_state: "serving".to_string(),
                worker_index: 0,
                worker_threads: 1,
                loaded: true,
                readonly: false,
                load_version: 9,
                table_name: "tbl".to_string(),
                shard_uri: "file:///tmp/shard-42".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1024,
                total_records: 1,
                storage_bytes: 10,
                cache_memory_bytes: 1,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 10,
                wal_sequence: 1,
                dirty_object_count: 0,
                dirty_bucket_count: 0,
            }],
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 42,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });
        meta.freeze_proxy(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "proxy-does-not-exist".to_string(),
            freeze_cooldown_ms: 0,
        });

        let missing_generation = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
            scheduler_task_id: Some(100),
            scheduler_generation: None,
        });
        assert_eq!(
            missing_generation.status.code,
            "scheduler_generation_required"
        );

        let accepted = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
            scheduler_task_id: Some(100),
            scheduler_generation: Some(10),
        });
        assert!(accepted.status.ok, "{accepted:?}");

        let stale = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
            scheduler_task_id: Some(101),
            scheduler_generation: Some(9),
        });
        assert_eq!(stale.status.code, "stale_scheduler_generation");

        let snapshot = meta.save_snapshot(&snapshot_path).unwrap();
        assert_eq!(
            snapshot.scheduler_finish_generations.get("42@s1").copied(),
            Some(10)
        );

        let recovered_from_log = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered_from_log
                .export_snapshot()
                .scheduler_finish_generations
                .get("42@s1")
                .copied(),
            Some(10)
        );
        assert_eq!(
            recovered_from_log
                .finish_load(LoadFinishRequest {
                    server_addr: "s1".to_string(),
                    shard_id: 42,
                    load_version: 9,
                    status: Status::ok(),
                    scheduler_task_id: Some(102),
                    scheduler_generation: Some(8),
                })
                .status
                .code,
            "stale_scheduler_generation"
        );

        let recovered_from_snapshot = SingleNodeMeta::default();
        assert!(
            recovered_from_snapshot
                .install_snapshot_from_file(&snapshot_path)
                .unwrap()
                .status
                .ok
        );
        assert_eq!(
            recovered_from_snapshot
                .export_snapshot()
                .scheduler_finish_generations
                .get("42@s1")
                .copied(),
            Some(10)
        );
    }

    // shared-corpus: control_metaserver_scheduler_lifecycle_workflow;
    #[test]
    fn metaserver_control_plane_parity_report_covers_scheduler_topology_and_node_coordination() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("control-plane-parity.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "s1".to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: vec![ServerShardServingState {
                shard_id: 77,
                serving_state: "serving".to_string(),
                worker_index: 0,
                worker_threads: 1,
                loaded: true,
                readonly: false,
                load_version: 5,
                table_name: "tbl".to_string(),
                shard_uri: "file:///tmp/shard-77".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 2048,
                total_records: 2,
                storage_bytes: 20,
                cache_memory_bytes: 2,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 20,
                wal_sequence: 2,
                dirty_object_count: 0,
                dirty_bucket_count: 0,
            }],
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 77,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });
        meta.freeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        meta.unfreeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        let finish = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 77,
            load_version: 5,
            status: Status::ok(),
            scheduler_task_id: Some(200),
            scheduler_generation: Some(20),
        });
        assert!(finish.status.ok, "{finish:?}");

        let report = meta.control_plane_parity_report();
        assert!(report.status.ok, "{report:?}");
        assert!(report.table_topology_ready);
        assert!(report.transitional_state_model_ready);
        assert!(report.topology_history_ready);
        assert!(report.scheduler_owned_finish_load_ready);
        assert!(report.scheduler_generation_check_ready);
        assert!(report.durable_replay_ready);
        assert!(report.real_data_node_coordination_ready);
        assert_eq!(report.scheduler_finish_generation_count, 1);
        assert!(report.topology_event_count >= 4);
        assert!(report.blockers.is_empty());
    }

    #[test]
    fn metaserver_finish_load_rejects_unknown_frozen_and_stale_servers() {
        let meta = SingleNodeMeta::default();
        let missing = meta.finish_load(LoadFinishRequest {
            server_addr: "missing".to_string(),
            shard_id: 7,
            load_version: 1,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(missing.status.code, "server_not_found");

        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "s1".to_string(),
            boot_time_ms: 1,
            binary_version: "v".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: vec![ServerShardServingState {
                shard_id: 7,
                load_version: 9,
                serving_state: "serving".to_string(),
                ..ServerShardServingState::default()
            }],
        });
        let stale = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 7,
            load_version: 8,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(stale.status.code, "stale_load_version");

        meta.freeze_server(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "s1".to_string(),
            freeze_cooldown_ms: 0,
        });
        let frozen = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 7,
            load_version: 10,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(frozen.status.code, "resource_frozen");
    }

    #[test]
    fn a_log_cut_off_mid_record_still_brings_the_metaserver_back() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta.log");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: "node-a".to_string(),
                    node_id: 1,
                    location: "rack-1".to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
            for shard in 1..=3u64 {
                assert!(meta
                    .register(RegisterShardRequest {
                        registered_at_ms: 0,
                        shard_id: shard,
                        server_addr: "node-a".to_string(),
                    })
                    .status
                    .ok);
            }
        }

        // What a process dying partway through an append leaves: a record that
        // starts and stops, with no newline after it.
        let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
        file.write_all(b"{\"at_ms\":123,\"mutation\":{\"RegisterSha").unwrap();
        drop(file);

        // It comes back, with everything that was acknowledged.
        let recovered = SingleNodeMeta::with_mutation_log(&log_path)
            .expect("a torn last record must not stop the metaserver starting");
        let listed = recovered.list_shards(ListShardsRequest {
            server_addr: String::new(),
            after_shard_id: 0,
            limit: 0,
        });
        assert!(listed.status.ok);
        let ids = listed
            .shards
            .iter()
            .map(|entry| entry.shard_id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "every shard acknowledged before the crash must come back"
        );
        assert_eq!(
            recovered.list_servers().servers.len(),
            1,
            "the server registration was acknowledged before the crash"
        );
    }

    #[test]
    fn a_write_after_recovering_from_a_crash_survives_the_next_restart() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta.log");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            for shard in 1..=2u64 {
                assert!(meta
                    .register(RegisterShardRequest {
                        registered_at_ms: 0,
                        shard_id: shard,
                        server_addr: "node-a".to_string(),
                    })
                    .status
                    .ok);
            }
        }
        // A crash partway through an append.
        let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
        file.write_all(b"{\"at_ms\":9,\"mutation\":{\"RegisterSha").unwrap();
        drop(file);

        // Come back and keep serving, which is the whole point of recovering.
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path)
                .expect("a torn last record must not stop the metaserver starting");
            assert!(
                meta.register(RegisterShardRequest {
                    registered_at_ms: 0,
                    shard_id: 3,
                    server_addr: "node-a".to_string(),
                })
                .status
                .ok,
                "the registration after recovery was acknowledged"
            );
        }

        // Restart again. Shard 3 was acknowledged AFTER the crash, so losing it
        // here would be losing a write that was promised -- and losing it
        // quietly, because the damaged line would be last and read as a torn
        // tail. Leaving the fragment in the file is what caused that: the next
        // append was spliced onto the end of it.
        let again = SingleNodeMeta::with_mutation_log(&log_path)
            .expect("the log must still be readable after recovering and writing");
        let listed = again.list_shards(ListShardsRequest {
            server_addr: String::new(),
            after_shard_id: 0,
            limit: 0,
        });
        assert!(listed.status.ok);
        let ids = listed
            .shards
            .iter()
            .map(|entry| entry.shard_id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "a write acknowledged after recovery was lost on the next restart"
        );

        // And the file itself is clean: every line parses, so no fragment is
        // waiting to swallow the next record.
        for (number, line) in fs::read_to_string(&log_path).unwrap().lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            assert!(
                serde_json::from_str::<MetaMutationRecord>(line).is_ok(),
                "line {} of the recovered log does not parse: {line}",
                number + 1
            );
        }
    }

    #[test]
    fn a_log_damaged_in_the_middle_is_refused_rather_than_half_read() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta.log");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            for shard in 1..=3u64 {
                assert!(meta
                    .register(RegisterShardRequest {
                        registered_at_ms: 0,
                        shard_id: shard,
                        server_addr: "node-a".to_string(),
                    })
                    .status
                    .ok);
            }
        }

        // Damage a line that has records after it. Those later records were
        // acknowledged, so stopping at the damage would lose them silently --
        // which is the thing that must not happen quietly.
        let text = fs::read_to_string(&log_path).unwrap();
        let mut lines = text.lines().collect::<Vec<_>>();
        assert!(lines.len() >= 3, "need a line with records after it");
        lines[1] = "{ this is not a record";
        fs::write(&log_path, lines.join("\n") + "\n").unwrap();

        let refused = SingleNodeMeta::with_mutation_log(&log_path);
        assert!(
            refused.is_err(),
            "damage with acknowledged records after it must be refused, not half-read"
        );
        let message = refused.err().unwrap().to_string();
        assert!(
            message.contains("not a torn tail"),
            "the error should say why it is not simply a crash: {message}"
        );
    }

    #[test]
    fn metaserver_mutation_log_recovers_routes_tables_and_state_changes() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert!(meta.info().durable_mutation_log);
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "server-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 7,
            binary_version: "v1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 10,
                shard_count: 1,
                replica_count: 1,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        assert!(
            meta.update_table(UpdateTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                shard_count: Some(2),
                replica_count: Some(2),
                first_shard_id: None,
                partition_version: None,
                serving_options: None,
            })
            .status
            .ok
        );
        // Registered after the table is sized, because shard_count is pinned
        // once a shard is registered against it. Where this sits is incidental
        // to what this test checks: the mutation count and the recovered state
        // are the same either way.
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 10,
            server_addr: "server-a".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "dropped_tbl".to_string(),
                first_shard_id: 11,
                shard_count: 1,
                replica_count: 1,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        assert!(
            meta.delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "dropped_tbl".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.freeze_proxy(StateChangeRequest {
                reason: FreezeReason::Unspecified,
                endpoint: "proxy-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );

        let mutations = LocalMetaMutationLog::new(&log_path)
            .unwrap()
            .load()
            .unwrap();
        assert_eq!(mutations.len(), 9);

        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
<<<<<<< HEAD
            recovered.get(10).location.as_ref().map(|location| {
                (
                    location.shard_id,
                    location.server_addr.clone(),
                    location.state,
                    location.latest_snapshot.clone(),
                )
            }),
            Some((10, "server-a".to_string(), MetaEntityState::Normal, None))
        );
        // The join time is carried through replay rather than restamped, so it
        // cannot be written into the expected value above -- only checked for
        // having survived.
        assert!(
            recovered.get(10).location.expect("registered").registered_at_ms > 0,
            "the shard's join time was lost in replay"
||||||| a7277311
            recovered.get(10).location.unwrap(),
            ShardLocation {
                state: MetaEntityState::Normal,
                shard_id: 10,
                server_addr: "server-a".to_string(),
                latest_snapshot: None,
            }
=======
            recovered.get(10).location.unwrap(),
            ShardLocation {
                preferred_location: String::new(),
                state: MetaEntityState::Normal,
                shard_id: 10,
                server_addr: "server-a".to_string(),
                latest_snapshot: None,
            }
>>>>>>> matrixark/main
        );
        let recovered_tables = recovered.list_tables().tables;
        assert_eq!(recovered_tables.len(), 2);
        assert_eq!(
            recovered_tables
                .iter()
                .find(|table| table.table_name == "dropped_tbl")
                .unwrap()
                .state,
            MetaEntityState::Dropped
        );
        assert_eq!(recovered.list_namespaces().namespaces[0].table_count, 1);
        let recovered_topology = recovered.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(recovered_topology.table.unwrap().replica_count, 2);
        assert_eq!(recovered_topology.shards.len(), 2);
        assert_eq!(
            recovered_topology.shards[0].primary.as_deref(),
            Some("server-a")
        );
        assert_eq!(
            recovered.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );
    }

    #[test]
    fn metaserver_records_latest_shard_snapshot_and_rejects_stale_ref() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("snapshot-meta.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 44,
            server_addr: "server-a".to_string(),
        });
        let first = ShardSnapshotRef {
            uri: "s3://cluster-a/shards/44/snapshots/1-10-a/manifest.json".to_string(),
            checksum: "sha256:first".to_string(),
            byte_size: 1024,
            last_log_index: 10,
            created_at_ms: 100,
        };
        assert!(
            meta.publish_shard_snapshot(PublishShardSnapshotRequest {
                shard_id: 44,
                snapshot: first.clone(),
            })
            .status
            .ok
        );
        let stale = ShardSnapshotRef {
            uri: "s3://cluster-a/shards/44/snapshots/1-9-stale/manifest.json".to_string(),
            checksum: "sha256:stale".to_string(),
            byte_size: 512,
            last_log_index: 9,
            created_at_ms: 90,
        };
        assert_eq!(
            meta.publish_shard_snapshot(PublishShardSnapshotRequest {
                shard_id: 44,
                snapshot: stale,
            })
            .status
            .code,
            "stale_snapshot"
        );

        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.get(44).location.unwrap().latest_snapshot,
            Some(first)
        );
    }

    #[test]
    fn metaserver_snapshot_round_trips_full_metabase_state() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("meta-snapshot.json");
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "server-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "server-b".to_string(),
            node_id: 2,
            location: "zone-b".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 77,
            server_addr: "server-a".to_string(),
        });
        let latest_snapshot = ShardSnapshotRef {
            uri: "s3://cluster/shards/77/snapshots/2-19/manifest.json".to_string(),
            checksum: "sha256:meta".to_string(),
            byte_size: 4096,
            last_log_index: 19,
            created_at_ms: 123,
        };
        meta.publish_shard_snapshot(PublishShardSnapshotRequest {
            shard_id: 77,
            snapshot: latest_snapshot.clone(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 9,
            binary_version: "v1".to_string(),
        });
        meta.freeze_proxy(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "proxy-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 77,
                shard_count: 2,
                replica_count: 2,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        let snapshot = meta.save_snapshot(&snapshot_path).unwrap();
        assert_eq!(snapshot.format_version, 1);
        assert_eq!(snapshot.stats.server_count, 2);
        assert_eq!(snapshot.stats.proxy_count, 1);
        assert_eq!(snapshot.stats.table_count, 1);

        let recovered = SingleNodeMeta::default();
        assert!(
            recovered
                .install_snapshot_from_file(&snapshot_path)
                .unwrap()
                .status
                .ok
        );
        assert_eq!(
            recovered.get(77).location.unwrap().latest_snapshot,
            Some(latest_snapshot)
        );
        assert_eq!(
            recovered.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );
        let topology = recovered.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert!(topology.status.ok);
        assert_eq!(topology.shards.len(), 2);
        assert_eq!(topology.shards[0].primary.as_deref(), Some("server-a"));
        assert_eq!(
            topology.shards[0].replicas,
            vec!["server-a".to_string(), "server-b".to_string()]
        );
        assert_eq!(
            recovered.stats().topology_version,
            snapshot.topology_version
        );
    }

    #[test]
    fn metaserver_mutation_log_ignores_failed_state_changes() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();

        let missing_server = meta.freeze_server(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "missing-server".to_string(),
            freeze_cooldown_ms: 0,
        });
        let missing_proxy = meta.drop_proxy(StateChangeRequest {
            reason: FreezeReason::Unspecified,
            endpoint: "missing-proxy".to_string(),
            freeze_cooldown_ms: 0,
        });

        assert!(!missing_server.status.ok);
        assert!(!missing_proxy.status.ok);
        assert!(LocalMetaMutationLog::new(&log_path)
            .unwrap()
            .load()
            .unwrap()
            .is_empty());
    }
}
