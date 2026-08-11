// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// impl MetaBackend, split from metaserver.rs (textual include!, shared flat scope).

impl MetaBackend {
    fn from_env() -> std::io::Result<Self> {
        if env_bool("TS_META_RAFT", false) || std::env::var("TS_META_RAFT_NODES").is_ok() {
            return Ok(Self::Raft(
                ProductionMetaRaftRuntime::start(runtime_options_from_env())
                    .expect("failed to initialize metaserver raft runtime"),
            ));
        }
        let meta = std::env::var("TS_META_MUTATION_LOG")
            .ok()
            .map(SingleNodeMeta::with_mutation_log)
            .transpose()?
            .unwrap_or_default();
        Ok(Self::Single(meta))
    }

    fn raft_status(&self) -> Option<RaftClusterStatus> {
        match self {
            Self::Single(_) => None,
            Self::Raft(runtime) => Some(runtime.status()),
        }
    }

    fn raft_ready(&self) -> Status {
        match self {
            Self::Single(_) => Status::error("raft_disabled", "meta raft is disabled"),
            Self::Raft(runtime) => runtime
                .validate_ready()
                .map(|_| Status::ok())
                .unwrap_or_else(|err| Status::error("raft_not_ready", err.to_string())),
        }
    }

    fn export_snapshot(&self) -> MetaSnapshotResponse {
        match self {
            Self::Single(meta) => MetaSnapshotResponse {
                status: Status::ok(),
                snapshot: Some(meta.export_snapshot()),
            },
            Self::Raft(runtime) => match runtime.cluster().export_meta_snapshot() {
                Ok(snapshot) => MetaSnapshotResponse {
                    status: Status::ok(),
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotResponse {
                    status: Status::error("raft_snapshot_export_failed", err.to_string()),
                    snapshot: None,
                },
            },
        }
    }

    fn install_snapshot(&self, snapshot: MetaSnapshot) -> AckResponse {
        match self {
            Self::Single(meta) => meta.install_snapshot(snapshot),
            Self::Raft(runtime) => AckResponse {
                status: runtime
                    .cluster()
                    .install_meta_snapshot_on_live_nodes(snapshot)
                    .map(|_| Status::ok())
                    .unwrap_or_else(|err| {
                        Status::error("raft_snapshot_install_failed", err.to_string())
                    }),
            },
        }
    }

    fn save_snapshot(&self, request: MetaSnapshotFileRequest) -> MetaSnapshotFileResponse {
        match self {
            Self::Single(meta) => match meta.save_snapshot(&request.path) {
                Ok(snapshot) => MetaSnapshotFileResponse {
                    status: Status::ok(),
                    path: request.path,
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_save_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
            Self::Raft(runtime) => match runtime.cluster().export_meta_snapshot() {
                Ok(snapshot) => MetaSnapshotFileResponse {
                    status: save_meta_snapshot_file(&request.path, &snapshot)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("snapshot_save_failed", err)),
                    path: request.path,
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_save_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
        }
    }

    fn load_snapshot(&self, request: MetaSnapshotFileRequest) -> MetaSnapshotFileResponse {
        match self {
            Self::Single(meta) => match SingleNodeMeta::load_snapshot_from_file(&request.path) {
                Ok(snapshot) => {
                    let status = meta.install_snapshot(snapshot.clone()).status;
                    MetaSnapshotFileResponse {
                        status,
                        path: request.path,
                        snapshot: Some(snapshot),
                    }
                }
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_load_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
            Self::Raft(runtime) => match SingleNodeMeta::load_snapshot_from_file(&request.path) {
                Ok(snapshot) => {
                    let status = runtime
                        .cluster()
                        .install_meta_snapshot_on_live_nodes(snapshot.clone())
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| {
                            Status::error("raft_snapshot_install_failed", err.to_string())
                        });
                    MetaSnapshotFileResponse {
                        status,
                        path: request.path,
                        snapshot: Some(snapshot),
                    }
                }
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_load_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
        }
    }
}
