// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Distributed-mode storage-backend selection.
//!
//! A distributed TemporalStore node persists shard data either through **shared
//! storage** (every node reads/writes one authoritative object store, so no
//! log replication is needed) or through **raft replication** (each node keeps
//! a local copy kept in sync by the raft log). Which one a node uses is
//! resolved here, from configuration plus compile-time capability detection.
//!
//! ## `auto` selection (the default)
//!
//! With `TS_STORAGE_BACKEND` unset or `auto`, selection is **intelligent and
//! self-healing**. The goal is to prefer the *networked* shared MatrixObject
//! store whenever it is actually reachable — so shard data follows shards on
//! rebalance and survives datanode loss — but to degrade gracefully rather than
//! wedge a node when it is not:
//!
//! 1. **matrixobject (shared/networked)** — when the `matrixobject` feature is
//!    compiled AND a MatrixObject endpoint is configured (`TS_MATRIXOBJECT_ENDPOINT`)
//!    AND a cheap startup **reachability probe** to that endpoint succeeds.
//! 2. **matrixobject (node-local)** — when the feature is compiled but no
//!    endpoint is configured (the historical node-local behavior, unchanged).
//! 3. **shared path** — otherwise, any configured shared filesystem/object-store
//!    root (`TS_SHARED_STORE_DIR`). This is the graceful downgrade target when a
//!    configured MatrixObject endpoint is *unreachable*.
//! 4. **raft replication** — otherwise. With no raft peers this is effectively a
//!    single local node, matching the historical default
//!    ([`ReplicationMode::default`] is `Raft`).
//!
//! An explicit override (`TS_STORAGE_BACKEND=matrixobject|shared|raft`) forces a
//! tier deterministically and **skips the probe** — the operator has asked for a
//! specific backend. Every resolution yields a [`BackendDecision`] carrying a
//! human-readable `reason` so the caller can emit one clear log line stating
//! which backend was chosen and *why* (including any downgrade).
//!
//! With nothing configured the resolver returns [`StorageBackend::RaftReplication`],
//! so enabling any shared path is opt-in and backward compatible.

use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore, ObjectStoreError};

use crate::e2e::ReplicationMode;

/// Env var: explicit backend override (`auto` | `matrixobject` | `shared` | `raft`).
pub const TS_STORAGE_BACKEND: &str = "TS_STORAGE_BACKEND";
/// Env var: matrixobject bucket name used when the matrixobject backend is chosen.
pub const TS_MATRIXOBJECT_BUCKET: &str = "TS_MATRIXOBJECT_BUCKET";
/// Env var: endpoint (`host:port` or `http://host:port`) of the *networked*
/// MatrixObject object-store service. When set (and the feature is compiled) the
/// `auto` resolver probes it and, only if reachable, selects the shared
/// MatrixObject backend so shard data follows shards across nodes.
pub const TS_MATRIXOBJECT_ENDPOINT: &str = "TS_MATRIXOBJECT_ENDPOINT";
/// Env var: reachability-probe timeout in milliseconds for the MatrixObject
/// endpoint. Kept short so an unreachable store degrades quickly at startup.
pub const TS_MATRIXOBJECT_PROBE_TIMEOUT_MS: &str = "TS_MATRIXOBJECT_PROBE_TIMEOUT_MS";
/// Env var: root directory/URI of a configured shared object store (non-matrixobject).
pub const TS_SHARED_STORE_DIR: &str = "TS_SHARED_STORE_DIR";
/// Env var: cluster id namespacing shared-store keys (all nodes must agree).
pub const TS_SHARED_STORE_CLUSTER_ID: &str = "TS_SHARED_STORE_CLUSTER_ID";

/// Default bucket used for the matrixobject backend when unset.
pub const DEFAULT_MATRIXOBJECT_BUCKET: &str = "temporalstore";
/// Default shared-store cluster id when unset.
pub const DEFAULT_SHARED_STORE_CLUSTER_ID: &str = "temporalstore";
/// Default port assumed for a MatrixObject endpoint given without one (matches
/// the object-store service default, `tools/matrixark_object_store_server.py`).
pub const DEFAULT_OBJECT_STORE_PORT: u16 = 17200;
/// Default reachability-probe timeout (ms) when `TS_MATRIXOBJECT_PROBE_TIMEOUT_MS`
/// is unset or unparseable.
pub const DEFAULT_PROBE_TIMEOUT_MS: u64 = 750;

/// `true` when the crate was compiled with the `matrixobject` feature, i.e. the
/// matrixobject shared-storage backend is available to select at runtime.
pub const fn matrixobject_feature_compiled() -> bool {
    cfg!(feature = "matrixobject")
}

/// The storage/replication strategy a distributed node should use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    /// Shared object storage via matrixobject (the preferred shared backend).
    MatrixObject {
        /// Bucket that holds this cluster's shard objects.
        bucket: String,
        /// Cluster id namespacing shared-store keys.
        cluster_id: String,
    },
    /// Shared object storage rooted at a configured filesystem/mounted path
    /// (a `FileObjectStore`-compatible root shared by every node).
    SharedPath {
        /// Shared object-store root directory (or URI-like root).
        root: PathBuf,
        /// Cluster id namespacing shared-store keys.
        cluster_id: String,
    },
    /// No shared storage available: keep a local copy per node, replicated by raft.
    RaftReplication,
}

impl StorageBackend {
    /// Whether the cache should keep a DISK tier beneath memory.
    ///
    /// A cache tier earns its cost from the latency it spans. With shared storage the durable
    /// copy is on another machine (or a shared mount), and a local disk tier closes a real
    /// millisecond-scale distance. With raft the durable copy is this node's own disk, so the
    /// tier is a third copy of bytes the OS page cache already serves from the same device --
    /// measured at 106 MB of cache against 21.6 MB of pages, 70% of everything on disk, to
    /// shorten a distance that is already zero.
    ///
    /// So: shared storage caches to disk; one-box and raft run memory + their own disk.
    /// `TS_CACHE_DISK_TIER` overrides either way for an operator who knows better.
    pub fn wants_disk_cache_tier(&self) -> bool {
        if let Ok(value) = std::env::var("TS_CACHE_DISK_TIER") {
            let value = value.trim().to_ascii_lowercase();
            if !value.is_empty() {
                return !matches!(value.as_str(), "0" | "false" | "no" | "off");
            }
        }
        match self {
            StorageBackend::MatrixObject { .. } | StorageBackend::SharedPath { .. } => true,
            StorageBackend::RaftReplication => false,
        }
    }

    /// The cluster-level replication mode implied by this backend.
    pub fn replication_mode(&self) -> ReplicationMode {
        match self {
            StorageBackend::MatrixObject { .. } | StorageBackend::SharedPath { .. } => {
                ReplicationMode::SharedStore
            }
            StorageBackend::RaftReplication => ReplicationMode::Raft,
        }
    }

    /// `true` when this backend is one of the shared-storage variants.
    pub fn is_shared_storage(&self) -> bool {
        matches!(
            self,
            StorageBackend::MatrixObject { .. } | StorageBackend::SharedPath { .. }
        )
    }

    /// Short, log-friendly description of the resolved backend.
    pub fn describe(&self) -> String {
        match self {
            StorageBackend::MatrixObject { bucket, cluster_id } => {
                format!("shared-storage:matrixobject(bucket={bucket}, cluster={cluster_id})")
            }
            StorageBackend::SharedPath { root, cluster_id } => {
                format!(
                    "shared-storage:path(root={}, cluster={cluster_id})",
                    root.display()
                )
            }
            StorageBackend::RaftReplication => "raft-replication".to_string(),
        }
    }

    /// Construct the shared object store this backend refers to.
    ///
    /// Returns `Ok(None)` for [`StorageBackend::RaftReplication`] — there is no
    /// shared store; the node replicates via raft. The matrixobject arm is only
    /// reachable when the `matrixobject` feature is compiled (resolution keys on
    /// [`matrixobject_feature_compiled`]); the uncompiled arm is a defensive error.
    pub fn build_shared_object_store(
        &self,
    ) -> Result<Option<Arc<dyn ObjectStore>>, ObjectStoreError> {
        match self {
            StorageBackend::SharedPath { root, .. } => {
                Ok(Some(Arc::new(FileObjectStore::new(root.clone()))))
            }
            #[cfg(feature = "matrixobject")]
            StorageBackend::MatrixObject { bucket, .. } => {
                // TODO(networked-store): when `TS_MATRIXOBJECT_ENDPOINT` is set
                // (see `StorageBackendConfig::matrixobject_endpoint` and the
                // `auto` probe in `resolve_auto_decision`), this must construct a
                // *networked* MatrixObject object store — an HTTP client to the
                // standalone object-store service (`tools/matrixark_object_store_server.py`
                // on :17200, `matrixobject://` scheme; the 208-dedup / X-Object-Meta
                // contract) — so a shard moved during rebalance reads its WAL/data
                // from the shared store on the *new* node. Until that networked
                // `ObjectStore` impl exists, this returns the in-process store,
                // which is node-local and does NOT provide data-follow across
                // nodes. The durability path in `wire_matrixobject_durability`
                // (src/bin/server.rs) must be pointed at the same endpoint.
                let store =
                    crate::matrixobject_store::MatrixObjectObjectStore::with_default_options(
                        bucket.clone(),
                    )?;
                Ok(Some(Arc::new(store)))
            }
            #[cfg(not(feature = "matrixobject"))]
            StorageBackend::MatrixObject { .. } => Err(ObjectStoreError::InvalidKey(
                "matrixobject backend selected but the `matrixobject` feature is not compiled"
                    .to_string(),
            )),
            StorageBackend::RaftReplication => Ok(None),
        }
    }
}

/// A resolved backend together with a human-readable reason for the choice.
///
/// The `reason` is intended to be logged verbatim by the caller as the single
/// "resolved storage backend" line, so it states *why* — e.g. a probe success,
/// a graceful downgrade after an unreachable endpoint, or a forced override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendDecision {
    /// The concrete backend a node should use.
    pub backend: StorageBackend,
    /// Human-readable explanation of why this backend was selected.
    pub reason: String,
}

impl BackendDecision {
    fn new(backend: StorageBackend, reason: impl Into<String>) -> Self {
        Self {
            backend,
            reason: reason.into(),
        }
    }

    /// The cluster-level replication mode implied by the chosen backend.
    pub fn replication_mode(&self) -> ReplicationMode {
        self.backend.replication_mode()
    }
}

/// Signature of a reachability probe: given an endpoint string, return `true`
/// when the networked MatrixObject store appears reachable. Injectable so tests
/// exercise the decision table without a live service.
pub type ReachabilityProbe<'a> = dyn Fn(&str) -> bool + 'a;

/// Parse a `host[:port]` / `scheme://host[:port][/path]` endpoint into the
/// `host:port` pair to probe, defaulting the port to [`DEFAULT_OBJECT_STORE_PORT`].
///
/// Returns `None` for an endpoint with no usable host.
fn parse_endpoint_host_port(endpoint: &str) -> Option<(String, u16)> {
    let mut rest = endpoint.trim();
    if rest.is_empty() {
        return None;
    }
    // Strip an optional scheme (http://, https://, matrixobject://, ...).
    if let Some(idx) = rest.find("://") {
        rest = &rest[idx + 3..];
    }
    // Drop any path/query/fragment and userinfo.
    rest = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    if let Some((_, host)) = rest.rsplit_once('@') {
        rest = host;
    }
    let rest = rest.trim();
    if rest.is_empty() {
        return None;
    }
    // host:port (ignore bracketed IPv6 subtleties — the common case is host:port).
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && !port.is_empty() => {
            let port = port.parse::<u16>().ok()?;
            Some((host.to_string(), port))
        }
        _ => Some((rest.to_string(), DEFAULT_OBJECT_STORE_PORT)),
    }
}

/// Default reachability probe: a short-timeout TCP connect to the endpoint's
/// `host:port` (the same cheap liveness check `proxy.rs`/`raft_node.rs` use).
///
/// A successful TCP connect means the object-store service is listening; the
/// service additionally exposes `GET /v1/healthz` for a semantic check, left as
/// a future strengthening of this probe (no HTTP client is linked here).
pub fn default_endpoint_probe(endpoint: &str) -> bool {
    let Some((host, port)) = parse_endpoint_host_port(endpoint) else {
        return false;
    };
    let timeout = Duration::from_millis(probe_timeout_ms());
    let Ok(addrs) = (host.as_str(), port).to_socket_addrs() else {
        return false;
    };
    for addr in addrs {
        if TcpStream::connect_timeout(&addr, timeout).is_ok() {
            return true;
        }
    }
    false
}

fn probe_timeout_ms() -> u64 {
    std::env::var(TS_MATRIXOBJECT_PROBE_TIMEOUT_MS)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_PROBE_TIMEOUT_MS)
}

/// An explicit `TS_STORAGE_BACKEND` selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum BackendOverride {
    /// Resolve by detection + configuration precedence.
    #[default]
    Auto,
    /// Force the matrixobject shared backend (errors out via fallback if uncompiled).
    MatrixObject,
    /// Force a configured shared-path backend.
    SharedPath,
    /// Force raft replication regardless of shared-storage config.
    Raft,
}

impl BackendOverride {
    fn parse(raw: Option<&str>) -> Self {
        match raw.map(|value| value.trim().to_ascii_lowercase()).as_deref() {
            Some("matrixobject") | Some("matrix_object") | Some("object") => Self::MatrixObject,
            Some("shared") | Some("shared_path") | Some("shared_store") | Some("path") => {
                Self::SharedPath
            }
            Some("raft") | Some("raft_replication") | Some("replication") => Self::Raft,
            // "auto", "", and unknown values all fall back to auto-detection.
            _ => Self::Auto,
        }
    }
}

/// Inputs to storage-backend selection, gathered from the environment (or an
/// injected getter, for tests) plus compile-time capability detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageBackendConfig {
    override_mode: BackendOverride,
    /// Whether the matrixobject backend is available (feature compiled + enabled).
    matrixobject_available: bool,
    matrixobject_bucket: String,
    /// Configured endpoint of the *networked* MatrixObject store, if any. When
    /// present the `auto` resolver probes it before selecting matrixobject.
    matrixobject_endpoint: Option<String>,
    shared_store_dir: Option<PathBuf>,
    cluster_id: String,
}

impl StorageBackendConfig {
    /// Resolve config from process environment variables.
    pub fn from_env() -> Self {
        Self::from_getter(|name| std::env::var(name).ok())
    }

    /// Resolve config from an arbitrary getter (used in tests to avoid touching
    /// real process env). matrixobject availability is taken from the compiled
    /// feature; a value of `TS_STORAGE_BACKEND=raft` still forces raft.
    pub fn from_getter(get: impl Fn(&str) -> Option<String>) -> Self {
        Self::from_parts(matrixobject_feature_compiled(), &get)
    }

    /// Core builder with matrixobject availability injected — the single place
    /// tests can exercise both the "feature present" and "feature absent" paths.
    pub fn from_parts(matrixobject_available: bool, get: &impl Fn(&str) -> Option<String>) -> Self {
        let bucket = non_empty(get(TS_MATRIXOBJECT_BUCKET))
            .unwrap_or_else(|| DEFAULT_MATRIXOBJECT_BUCKET.to_string());
        let cluster_id = non_empty(get(TS_SHARED_STORE_CLUSTER_ID))
            .unwrap_or_else(|| DEFAULT_SHARED_STORE_CLUSTER_ID.to_string());
        let shared_store_dir = non_empty(get(TS_SHARED_STORE_DIR)).map(PathBuf::from);
        let matrixobject_endpoint = non_empty(get(TS_MATRIXOBJECT_ENDPOINT));
        Self {
            override_mode: BackendOverride::parse(get(TS_STORAGE_BACKEND).as_deref()),
            matrixobject_available,
            matrixobject_bucket: bucket,
            matrixobject_endpoint,
            shared_store_dir,
            cluster_id,
        }
    }

    /// Resolve the concrete backend a node should use, applying the precedence
    /// documented on this module, using the default TCP reachability probe for
    /// any configured MatrixObject endpoint.
    pub fn resolve(&self) -> StorageBackend {
        self.resolve_decision().backend
    }

    /// Resolve to a [`BackendDecision`] (backend + reason) using the default
    /// reachability probe. This is what production callers use; the reason is
    /// intended to be logged as the single "resolved storage backend" line.
    pub fn resolve_decision(&self) -> BackendDecision {
        self.resolve_decision_with_probe(&default_endpoint_probe)
    }

    /// Resolve to a [`BackendDecision`] using an injected reachability probe.
    /// Tests drive the full decision table through here with a stub probe.
    pub fn resolve_decision_with_probe(&self, probe: &ReachabilityProbe<'_>) -> BackendDecision {
        match self.override_mode {
            BackendOverride::Raft => BackendDecision::new(
                StorageBackend::RaftReplication,
                "TS_STORAGE_BACKEND=raft: forced raft replication",
            ),
            // Explicit matrixobject override forces the backend without probing —
            // the operator asked for it deterministically.
            BackendOverride::MatrixObject if self.matrixobject_available => BackendDecision::new(
                self.matrixobject(),
                self.forced_matrixobject_reason(),
            ),
            BackendOverride::SharedPath => match &self.shared_store_dir {
                Some(root) => BackendDecision::new(
                    self.shared_path(root),
                    format!(
                        "TS_STORAGE_BACKEND=shared: forced shared-path at {}",
                        root.display()
                    ),
                ),
                // Explicitly asked for shared-path but none configured: fall
                // through to auto so we still pick the best available backend
                // rather than silently mis-configuring.
                None => self.resolve_auto_decision(probe),
            },
            // MatrixObject override without the feature compiled, or Auto.
            BackendOverride::MatrixObject | BackendOverride::Auto => {
                self.resolve_auto_decision(probe)
            }
        }
    }

    fn matrixobject(&self) -> StorageBackend {
        StorageBackend::MatrixObject {
            bucket: self.matrixobject_bucket.clone(),
            cluster_id: self.cluster_id.clone(),
        }
    }

    fn shared_path(&self, root: &PathBuf) -> StorageBackend {
        StorageBackend::SharedPath {
            root: root.clone(),
            cluster_id: self.cluster_id.clone(),
        }
    }

    fn forced_matrixobject_reason(&self) -> String {
        match &self.matrixobject_endpoint {
            Some(endpoint) => format!(
                "TS_STORAGE_BACKEND=matrixobject: forced (endpoint {endpoint}, probe skipped)"
            ),
            None => "TS_STORAGE_BACKEND=matrixobject: forced (node-local, no endpoint)".to_string(),
        }
    }

    /// The `auto` decision: prefer a reachable networked matrixobject store,
    /// then node-local matrixobject, then a configured shared path, then raft.
    fn resolve_auto_decision(&self, probe: &ReachabilityProbe<'_>) -> BackendDecision {
        if self.matrixobject_available {
            match &self.matrixobject_endpoint {
                Some(endpoint) => {
                    if probe(endpoint) {
                        return BackendDecision::new(
                            self.matrixobject(),
                            format!(
                                "auto: matrixobject endpoint {endpoint} reachable — \
                                 shared object storage selected"
                            ),
                        );
                    }
                    // Configured but unreachable: degrade gracefully rather than
                    // wedge the node on a store it cannot reach.
                    let mut fallback = self.fallback_below_matrixobject();
                    fallback.reason = format!(
                        "auto: matrixobject endpoint {endpoint} UNREACHABLE — downgraded to {}",
                        fallback.backend.describe()
                    );
                    return fallback;
                }
                None => {
                    return BackendDecision::new(
                        self.matrixobject(),
                        "auto: matrixobject feature available, node-local \
                         (no TS_MATRIXOBJECT_ENDPOINT configured)",
                    );
                }
            }
        }
        let mut fallback = self.fallback_below_matrixobject();
        fallback.reason = format!(
            "auto: matrixobject unavailable — selected {}",
            fallback.backend.describe()
        );
        fallback
    }

    /// The tier below matrixobject: a configured shared path, else raft.
    fn fallback_below_matrixobject(&self) -> BackendDecision {
        match &self.shared_store_dir {
            Some(root) => BackendDecision::new(
                self.shared_path(root),
                format!("shared-path at {}", root.display()),
            ),
            None => BackendDecision::new(
                StorageBackend::RaftReplication,
                "raft replication (no shared storage configured)",
            ),
        }
    }

    /// Convenience: the cluster-level [`ReplicationMode`] implied by the
    /// resolved backend.
    pub fn replication_mode(&self) -> ReplicationMode {
        self.resolve().replication_mode()
    }
}

impl Default for StorageBackendConfig {
    fn default() -> Self {
        Self::from_getter(|_| None)
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn getter(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn resolve(available: bool, pairs: &[(&str, &str)]) -> StorageBackend {
        StorageBackendConfig::from_parts(available, &getter(pairs)).resolve()
    }

    #[test]
    fn no_config_falls_back_to_raft() {
        // Matches the historical default and keeps the path opt-in.
        assert_eq!(resolve(false, &[]), StorageBackend::RaftReplication);
        assert_eq!(
            StorageBackendConfig::from_parts(false, &getter(&[])).replication_mode(),
            ReplicationMode::Raft
        );
    }

    #[test]
    fn matrixobject_wins_when_available_even_with_shared_dir() {
        let backend = resolve(
            true,
            &[
                (TS_SHARED_STORE_DIR, "/mnt/shared"),
                (TS_MATRIXOBJECT_BUCKET, "prod-bucket"),
                (TS_SHARED_STORE_CLUSTER_ID, "cluster-7"),
            ],
        );
        assert_eq!(
            backend,
            StorageBackend::MatrixObject {
                bucket: "prod-bucket".to_string(),
                cluster_id: "cluster-7".to_string(),
            }
        );
        assert_eq!(backend.replication_mode(), ReplicationMode::SharedStore);
        assert!(backend.is_shared_storage());
    }

    #[test]
    fn shared_path_used_when_matrixobject_absent_but_dir_configured() {
        let backend = resolve(false, &[(TS_SHARED_STORE_DIR, "/mnt/shared")]);
        assert_eq!(
            backend,
            StorageBackend::SharedPath {
                root: PathBuf::from("/mnt/shared"),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
        assert_eq!(backend.replication_mode(), ReplicationMode::SharedStore);
    }

    #[test]
    fn default_matrixobject_bucket_when_unset() {
        let backend = resolve(true, &[]);
        assert_eq!(
            backend,
            StorageBackend::MatrixObject {
                bucket: DEFAULT_MATRIXOBJECT_BUCKET.to_string(),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
    }

    #[test]
    fn explicit_raft_override_beats_all_shared_config() {
        let backend = resolve(
            true,
            &[
                (TS_STORAGE_BACKEND, "raft"),
                (TS_SHARED_STORE_DIR, "/mnt/shared"),
            ],
        );
        assert_eq!(backend, StorageBackend::RaftReplication);
    }

    #[test]
    fn explicit_shared_override_prefers_path_over_matrixobject() {
        let backend = resolve(
            true,
            &[
                (TS_STORAGE_BACKEND, "shared"),
                (TS_SHARED_STORE_DIR, "/mnt/shared"),
            ],
        );
        assert_eq!(
            backend,
            StorageBackend::SharedPath {
                root: PathBuf::from("/mnt/shared"),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
    }

    #[test]
    fn explicit_shared_override_without_dir_falls_back_to_auto() {
        // Asked for shared but nothing configured: still pick best available
        // (matrixobject here) rather than a broken shared-path.
        assert_eq!(
            resolve(true, &[(TS_STORAGE_BACKEND, "shared")]),
            StorageBackend::MatrixObject {
                bucket: DEFAULT_MATRIXOBJECT_BUCKET.to_string(),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
        // ...and raft when nothing at all is available.
        assert_eq!(
            resolve(false, &[(TS_STORAGE_BACKEND, "shared")]),
            StorageBackend::RaftReplication
        );
    }

    #[test]
    fn matrixobject_override_without_feature_falls_back() {
        // Override asks for matrixobject but the feature isn't compiled: fall
        // through to the next best (configured shared path, else raft).
        assert_eq!(
            resolve(
                false,
                &[
                    (TS_STORAGE_BACKEND, "matrixobject"),
                    (TS_SHARED_STORE_DIR, "/mnt/shared"),
                ],
            ),
            StorageBackend::SharedPath {
                root: PathBuf::from("/mnt/shared"),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
        assert_eq!(
            resolve(false, &[(TS_STORAGE_BACKEND, "matrixobject")]),
            StorageBackend::RaftReplication
        );
    }

    #[test]
    fn blank_and_unknown_values_are_ignored() {
        // Blank bucket/dir/override behave as unset.
        let backend = resolve(
            true,
            &[
                (TS_STORAGE_BACKEND, "   "),
                (TS_MATRIXOBJECT_BUCKET, "  "),
                (TS_SHARED_STORE_DIR, ""),
            ],
        );
        assert_eq!(
            backend,
            StorageBackend::MatrixObject {
                bucket: DEFAULT_MATRIXOBJECT_BUCKET.to_string(),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
    }

    #[tokio::test]
    async fn build_shared_object_store_maps_backends() {
        // Raft => no shared store.
        assert!(StorageBackend::RaftReplication
            .build_shared_object_store()
            .unwrap()
            .is_none());
        // SharedPath => a usable FileObjectStore rooted at the configured dir.
        let dir = tempfile::tempdir().unwrap();
        let store = StorageBackend::SharedPath {
            root: dir.path().to_path_buf(),
            cluster_id: "c".to_string(),
        }
        .build_shared_object_store()
        .unwrap()
        .expect("shared path yields an object store");
        store
            .put("probe", bytes::Bytes::from_static(b"ok"))
            .await
            .unwrap();
        assert_eq!(&store.get("probe").await.unwrap()[..], b"ok");
    }

    #[test]
    fn describe_is_stable_and_readable() {
        assert_eq!(
            StorageBackend::RaftReplication.describe(),
            "raft-replication"
        );
        assert!(resolve(true, &[])
            .describe()
            .starts_with("shared-storage:matrixobject"));
    }

    // ---- auto / reachability-probe decision table --------------------------

    use std::cell::Cell;

    fn decide(
        available: bool,
        pairs: &[(&str, &str)],
        probe: &ReachabilityProbe<'_>,
    ) -> BackendDecision {
        StorageBackendConfig::from_parts(available, &getter(pairs)).resolve_decision_with_probe(probe)
    }

    #[test]
    fn auto_endpoint_reachable_selects_matrixobject() {
        let d = decide(
            true,
            &[(TS_MATRIXOBJECT_ENDPOINT, "http://obj:17200")],
            &|_: &str| true,
        );
        assert_eq!(
            d.backend,
            StorageBackend::MatrixObject {
                bucket: DEFAULT_MATRIXOBJECT_BUCKET.to_string(),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
        assert!(d.reason.contains("reachable"), "reason: {}", d.reason);
        assert_eq!(d.replication_mode(), ReplicationMode::SharedStore);
    }

    #[test]
    fn auto_endpoint_unreachable_downgrades_to_shared_dir() {
        let d = decide(
            true,
            &[
                (TS_MATRIXOBJECT_ENDPOINT, "obj:17200"),
                (TS_SHARED_STORE_DIR, "/mnt/shared"),
            ],
            &|_: &str| false,
        );
        assert_eq!(
            d.backend,
            StorageBackend::SharedPath {
                root: PathBuf::from("/mnt/shared"),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
        assert!(d.reason.contains("UNREACHABLE"), "reason: {}", d.reason);
        assert!(d.reason.contains("downgraded"), "reason: {}", d.reason);
    }

    #[test]
    fn auto_endpoint_unreachable_no_shared_dir_downgrades_to_raft() {
        let d = decide(
            true,
            &[(TS_MATRIXOBJECT_ENDPOINT, "obj:17200")],
            &|_: &str| false,
        );
        assert_eq!(d.backend, StorageBackend::RaftReplication);
        assert!(d.reason.contains("UNREACHABLE"), "reason: {}", d.reason);
    }

    #[test]
    fn auto_no_endpoint_selects_node_local_matrixobject() {
        // Feature available but no endpoint configured: historical node-local
        // behavior, and the probe must never be consulted.
        let d = decide(true, &[], &|_: &str| panic!("probe must not run without an endpoint"));
        assert!(matches!(d.backend, StorageBackend::MatrixObject { .. }));
        assert!(d.reason.contains("node-local"), "reason: {}", d.reason);
    }

    #[test]
    fn auto_endpoint_ignored_when_feature_absent() {
        // No matrixobject feature: endpoint is irrelevant, pick shared-dir, and
        // never probe (nothing to probe).
        let d = decide(
            false,
            &[
                (TS_MATRIXOBJECT_ENDPOINT, "obj:17200"),
                (TS_SHARED_STORE_DIR, "/mnt/shared"),
            ],
            &|_: &str| panic!("probe must not run when matrixobject is unavailable"),
        );
        assert_eq!(
            d.backend,
            StorageBackend::SharedPath {
                root: PathBuf::from("/mnt/shared"),
                cluster_id: DEFAULT_SHARED_STORE_CLUSTER_ID.to_string(),
            }
        );
    }

    #[test]
    fn forced_matrixobject_override_skips_probe() {
        // Explicit override forces matrixobject even with an endpoint that would
        // fail a probe — the probe must not be called.
        let d = decide(
            true,
            &[
                (TS_STORAGE_BACKEND, "matrixobject"),
                (TS_MATRIXOBJECT_ENDPOINT, "obj:17200"),
            ],
            &|_: &str| panic!("forced override must skip the probe"),
        );
        assert!(matches!(d.backend, StorageBackend::MatrixObject { .. }));
        assert!(d.reason.contains("forced"), "reason: {}", d.reason);
    }

    #[test]
    fn auto_probe_called_exactly_once_with_endpoint() {
        let calls = Cell::new(0u32);
        let seen = Cell::new(String::new());
        let probe = |endpoint: &str| {
            calls.set(calls.get() + 1);
            seen.set(endpoint.to_string());
            true
        };
        let d = decide(true, &[(TS_MATRIXOBJECT_ENDPOINT, "obj:9999")], &probe);
        assert!(matches!(d.backend, StorageBackend::MatrixObject { .. }));
        assert_eq!(calls.get(), 1);
        assert_eq!(seen.take(), "obj:9999");
    }

    #[test]
    fn parse_endpoint_host_port_forms() {
        assert_eq!(
            parse_endpoint_host_port("http://host:17200"),
            Some(("host".to_string(), 17200))
        );
        assert_eq!(
            parse_endpoint_host_port("host:1234/some/path?q=1"),
            Some(("host".to_string(), 1234))
        );
        assert_eq!(
            parse_endpoint_host_port("bare-host"),
            Some(("bare-host".to_string(), DEFAULT_OBJECT_STORE_PORT))
        );
        assert_eq!(
            parse_endpoint_host_port("matrixobject://obj.internal"),
            Some(("obj.internal".to_string(), DEFAULT_OBJECT_STORE_PORT))
        );
        assert_eq!(parse_endpoint_host_port("   "), None);
        assert_eq!(parse_endpoint_host_port("host:notaport"), None);
    }

    #[test]
    fn default_endpoint_probe_detects_open_and_closed_ports() {
        use std::net::TcpListener;
        // Bound, listening port -> reachable.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("127.0.0.1:{}", addr.port());
        assert!(default_endpoint_probe(&endpoint));
        // Closed port (drop the listener first) -> unreachable.
        let closed = {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            p
        };
        assert!(!default_endpoint_probe(&format!("127.0.0.1:{closed}")));
        // Unparseable endpoint -> unreachable.
        assert!(!default_endpoint_probe("   "));
    }
}

#[cfg(test)]
mod disk_cache_tier_tests {
    use super::*;
    use std::path::PathBuf;

    fn shared_path() -> StorageBackend {
        StorageBackend::SharedPath {
            root: PathBuf::from("/mnt/shared"),
            cluster_id: "c".to_string(),
        }
    }

    fn matrixobject() -> StorageBackend {
        StorageBackend::MatrixObject {
            bucket: "b".to_string(),
            cluster_id: "c".to_string(),
        }
    }

    #[test]
    fn shared_storage_keeps_the_disk_tier_and_local_storage_does_not() {
        std::env::remove_var("TS_CACHE_DISK_TIER");
        // Both halves asserted: a rule that only ever answered "false" would still satisfy the
        // half this change is motivated by.
        assert!(matrixobject().wants_disk_cache_tier());
        assert!(shared_path().wants_disk_cache_tier());
        assert!(!StorageBackend::RaftReplication.wants_disk_cache_tier());
    }

    #[test]
    fn one_box_with_nothing_configured_gets_no_disk_tier() {
        // With nothing configured the resolver returns RaftReplication -- the backend a single
        // box and a raft node both land on -- so both run memory + their own disk.
        std::env::remove_var("TS_CACHE_DISK_TIER");
        let resolved = StorageBackendConfig::default().resolve();
        assert_eq!(resolved, StorageBackend::RaftReplication);
        assert!(!resolved.wants_disk_cache_tier());
    }

    #[test]
    fn the_operator_override_wins_in_both_directions() {
        std::env::set_var("TS_CACHE_DISK_TIER", "0");
        assert!(!matrixobject().wants_disk_cache_tier());
        std::env::set_var("TS_CACHE_DISK_TIER", "1");
        assert!(StorageBackend::RaftReplication.wants_disk_cache_tier());
        // An empty value must not be read as "off" -- it means "unset".
        std::env::set_var("TS_CACHE_DISK_TIER", "");
        assert!(!StorageBackend::RaftReplication.wants_disk_cache_tier());
        assert!(matrixobject().wants_disk_cache_tier());
        std::env::remove_var("TS_CACHE_DISK_TIER");
    }
}
