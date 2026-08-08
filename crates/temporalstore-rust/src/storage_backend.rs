//! Distributed-mode storage-backend selection.
//!
//! A distributed TemporalStore node persists shard data either through **shared
//! storage** (every node reads/writes one authoritative object store, so no
//! log replication is needed) or through **raft replication** (each node keeps
//! a local copy kept in sync by the raft log). Which one a node uses is
//! resolved here, from configuration plus compile-time capability detection,
//! with this precedence:
//!
//! 1. An explicit override (`TS_STORAGE_BACKEND=matrixobject|shared|raft`).
//! 2. **matrixobject**, when the `matrixobject` feature is compiled in and not
//!    disabled — the preferred shared backend.
//! 3. Otherwise, any **other configured shared storage** (`TS_SHARED_STORE_DIR`,
//!    a shared/mounted object-store root).
//! 4. Otherwise (no shared storage available) → **raft replication**.
//!
//! With nothing configured the resolver returns [`StorageBackend::RaftReplication`],
//! matching the historical default ([`ReplicationMode::default`] is `Raft`), so
//! enabling this path is opt-in and backward compatible.

use std::path::PathBuf;
use std::sync::Arc;

use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore, ObjectStoreError};

use crate::e2e::ReplicationMode;

/// Env var: explicit backend override (`auto` | `matrixobject` | `shared` | `raft`).
pub const TS_STORAGE_BACKEND: &str = "TS_STORAGE_BACKEND";
/// Env var: matrixobject bucket name used when the matrixobject backend is chosen.
pub const TS_MATRIXOBJECT_BUCKET: &str = "TS_MATRIXOBJECT_BUCKET";
/// Env var: root directory/URI of a configured shared object store (non-matrixobject).
pub const TS_SHARED_STORE_DIR: &str = "TS_SHARED_STORE_DIR";
/// Env var: cluster id namespacing shared-store keys (all nodes must agree).
pub const TS_SHARED_STORE_CLUSTER_ID: &str = "TS_SHARED_STORE_CLUSTER_ID";

/// Default bucket used for the matrixobject backend when unset.
pub const DEFAULT_MATRIXOBJECT_BUCKET: &str = "temporalstore";
/// Default shared-store cluster id when unset.
pub const DEFAULT_SHARED_STORE_CLUSTER_ID: &str = "temporalstore";

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
        Self {
            override_mode: BackendOverride::parse(get(TS_STORAGE_BACKEND).as_deref()),
            matrixobject_available,
            matrixobject_bucket: bucket,
            shared_store_dir,
            cluster_id,
        }
    }

    /// Resolve the concrete backend a node should use, applying the precedence
    /// documented on this module.
    pub fn resolve(&self) -> StorageBackend {
        let matrixobject = || StorageBackend::MatrixObject {
            bucket: self.matrixobject_bucket.clone(),
            cluster_id: self.cluster_id.clone(),
        };
        let shared_path = |root: &PathBuf| StorageBackend::SharedPath {
            root: root.clone(),
            cluster_id: self.cluster_id.clone(),
        };

        match self.override_mode {
            BackendOverride::Raft => StorageBackend::RaftReplication,
            BackendOverride::MatrixObject if self.matrixobject_available => matrixobject(),
            BackendOverride::SharedPath => match &self.shared_store_dir {
                Some(root) => shared_path(root),
                // Explicitly asked for shared-path but none configured: fall
                // through to auto so we still pick the best available backend
                // rather than silently mis-configuring.
                None => self.resolve_auto(&matrixobject, &shared_path),
            },
            // MatrixObject override without the feature compiled, or Auto.
            BackendOverride::MatrixObject | BackendOverride::Auto => {
                self.resolve_auto(&matrixobject, &shared_path)
            }
        }
    }

    fn resolve_auto(
        &self,
        matrixobject: &impl Fn() -> StorageBackend,
        shared_path: &impl Fn(&PathBuf) -> StorageBackend,
    ) -> StorageBackend {
        // 1. matrixobject when available (detected), 2. other configured shared
        // storage, 3. raft replication.
        if self.matrixobject_available {
            matrixobject()
        } else if let Some(root) = &self.shared_store_dir {
            shared_path(root)
        } else {
            StorageBackend::RaftReplication
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
}
