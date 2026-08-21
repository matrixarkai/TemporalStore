// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta snapshot export/install/save/load, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn with_mutation_log(path: impl Into<PathBuf>) -> io::Result<Self> {
        let log = LocalMetaMutationLog::new(path)?;
        let meta = Self {
            mutation_log: Some(log.clone()),
            ..Self::default()
        };
        for mutation in log.load()? {
            meta.apply_mutation(mutation);
        }
        Ok(meta)
    }

    pub fn export_snapshot(&self) -> MetaSnapshot {
        let state = self.inner.read().expect("meta lock poisoned");
        MetaSnapshot::from_state(&state)
    }

    pub(crate) fn state_from_snapshot(snapshot: MetaSnapshot) -> Result<MetaState, Status> {
        if snapshot.format_version != 1 {
            return Err(Status::error(
                "bad_snapshot",
                "unsupported metaserver snapshot version",
            ));
        }
        let mut tables = BTreeMap::new();
        let mut next_table_id = snapshot.next_table_id.max(1);
        for table in snapshot.tables {
            if table.namespace.is_empty() || table.table_name.is_empty() {
                return Err(Status::error(
                    "bad_snapshot",
                    "snapshot contains invalid table name",
                ));
            }
            next_table_id = next_table_id.max(table.table_id.saturating_add(1));
            let key = table_key(&table.namespace, &table.table_name);
            if tables.insert(key, TableRecord { info: table }).is_some() {
                return Err(Status::error(
                    "bad_snapshot",
                    "snapshot contains duplicate table",
                ));
            }
        }
        Ok(MetaState {
            shards: snapshot.shards,
            servers: snapshot.servers,
            proxies: snapshot.proxies,
            proxy_groups: snapshot.proxy_groups,
            namespaces: snapshot.namespaces,
            tables,
            counters: counters_from_stats(&snapshot.stats),
            next_table_id,
            topology_version: snapshot.topology_version,
            topology_events: VecDeque::new(),
            scheduler_finish_generations: snapshot.scheduler_finish_generations,
            // Carried across the install so a peer keeps ageing the tombstones
            // it inherits instead of restarting every one of their clocks.
            dropped_since_ms: snapshot.dropped_since_ms,
            // An operator's mute must survive a snapshot install, or a peer
            // taking over quietly resumes the change they stopped.
            meta_change_muted: snapshot.meta_change_muted,
            frozen_since_ms: snapshot.frozen_since_ms,
        })
    }

    pub fn install_snapshot(&self, snapshot: MetaSnapshot) -> AckResponse {
        let state = match Self::state_from_snapshot(snapshot) {
            Ok(state) => state,
            Err(status) => return AckResponse { status },
        };
        *self.inner.write().expect("meta lock poisoned") = state;
        AckResponse {
            status: Status::ok(),
        }
    }
}

impl MetaSnapshot {
    pub(crate) fn from_state(state: &MetaState) -> Self {
        MetaSnapshot {
            format_version: 1,
            created_at_ms: now_ms(),
            shards: state.shards.clone(),
            servers: state.servers.clone(),
            proxies: state.proxies.clone(),
            proxy_groups: state.proxy_groups.clone(),
            namespaces: state.namespaces.clone(),
            tables: state
                .tables
                .values()
                .map(|table| table.info.clone())
                .collect(),
            stats: stats_from_state(&state),
            next_table_id: state.next_table_id,
            topology_version: state.topology_version,
            scheduler_finish_generations: state.scheduler_finish_generations.clone(),
            dropped_since_ms: state.dropped_since_ms.clone(),
            meta_change_muted: state.meta_change_muted,
            frozen_since_ms: state.frozen_since_ms.clone(),
        }
    }
}

impl SingleNodeMeta {
    pub fn save_snapshot(&self, path: impl AsRef<Path>) -> io::Result<MetaSnapshot> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let snapshot = self.export_snapshot();
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
            serde_json::to_writer_pretty(&mut file, &snapshot).map_err(io::Error::other)?;
            file.write_all(b"\n")?;
            file.sync_data()?;
        }
        fs::rename(&tmp_path, path)?;
        Ok(snapshot)
    }

    pub fn load_snapshot_from_file(path: impl AsRef<Path>) -> io::Result<MetaSnapshot> {
        let file = OpenOptions::new().read(true).open(path)?;
        serde_json::from_reader(file).map_err(io::Error::other)
    }

    pub fn install_snapshot_from_file(&self, path: impl AsRef<Path>) -> io::Result<AckResponse> {
        let snapshot = Self::load_snapshot_from_file(path)?;
        Ok(self.install_snapshot(snapshot))
    }
}
