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
        for record in log.load()? {
            // A line written before the time was recorded gets the current
            // clock, which is exactly what it has always been given.
            let at_ms = if record.at_ms == 0 {
                now_ms()
            } else {
                record.at_ms
            };
            meta.apply_mutation_at(record.mutation, at_ms);
        }
        Ok(meta)
    }

    pub fn export_snapshot(&self) -> MetaSnapshot {
        let state = self.inner.read().expect("meta lock poisoned");
        MetaSnapshot::from_state(&state, &self.counters)
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
        let mut restored = MetaState {
            shards: snapshot.shards.into_iter().collect(),
            servers: snapshot.servers,
            proxies: snapshot.proxies,
            proxy_groups: snapshot.proxy_groups,
            namespaces: snapshot.namespaces,
            tables,
            next_table_id,
            topology_version: snapshot.topology_version,
            topology_events: snapshot.topology_events,
            scheduler_finish_generations: snapshot.scheduler_finish_generations,
            // Carried across the install so a peer keeps ageing the tombstones
            // it inherits instead of restarting every one of their clocks.
            dropped_since_ms: snapshot.dropped_since_ms,
            // Whether the snapshot was taken while muted. The live mute is
            // folded in by the caller, which can see both; a mute must survive
            // an install either way, or a peer taking over -- or an operator
            // restoring data mid-incident -- quietly resumes the change they
            // stopped.
            meta_change_muted: snapshot.meta_change_muted,
            frozen_since_ms: snapshot.frozen_since_ms,
            reserved_names: snapshot.reserved_names,
            // Derived from the tables above rather than carried in the
            // snapshot, so a snapshot written before this existed still
            // restores with an index rather than an empty one.
            table_starts: BTreeMap::new(),
            table_ranges_overlap: false,
        };
        restored.reindex_table_ranges();
        Ok(restored)
    }

    pub fn install_snapshot(&self, snapshot: MetaSnapshot) -> AckResponse {
        // Rejected before it is recorded, so a snapshot the log could not
        // replay never reaches the log.
        if let Err(status) = Self::state_from_snapshot(snapshot.clone()) {
            return AckResponse { status };
        }
        // Recorded before it is applied, so a restart does not undo it. Replay
        // reapplies every mutation in the log; without a record of the install,
        // everything the restore rolled back is still in there and comes
        // straight back -- the operator sees the rollback take, and the next
        // start silently reverses it. Retention records its purges for exactly
        // this reason.
        self.record_mutation(MetaMutation::InstallSnapshot(Box::new(snapshot.clone())));
        self.apply_install_snapshot(snapshot)
    }

    /// Install without recording, for replay -- which must reapply what was
    /// already accepted rather than record it a second time.
    pub(crate) fn apply_install_snapshot(&self, snapshot: MetaSnapshot) -> AckResponse {
        // Taken before the state is consumed: the counters no longer travel
        // inside MetaState, so without this a peer that installs a snapshot
        // reports every total starting again from zero.
        let stats = snapshot.stats.clone();
        let mut state = match Self::state_from_snapshot(snapshot) {
            Ok(state) => state,
            Err(status) => return AckResponse { status },
        };
        let mut live = self.inner.write().expect("meta lock poisoned");
        // A mute is a live control on THIS metaserver, not part of the data
        // being restored, so restoring data does not lift it. Read under the
        // same lock the install takes, so a mute arriving alongside cannot be
        // missed. Either side carrying it is enough: the snapshot's flag keeps
        // a peer that inherits a muted leader muted, and this keeps an
        // operator's mute from being cleared by a snapshot taken before it.
        state.meta_change_muted |= live.meta_change_muted;
        *live = state;
        drop(live);
        self.counters.install_from(&stats);
        AckResponse {
            status: Status::ok(),
        }
    }
}

impl MetaSnapshot {
    pub(crate) fn from_state(state: &MetaState, counters: &MetaCounters) -> Self {
        MetaSnapshot {
            format_version: 1,
            created_at_ms: now_ms(),
            shards: state
                .shards
                .iter()
                .map(|(shard_id, location)| (*shard_id, location.clone()))
                .collect(),
            servers: state.servers.clone(),
            proxies: state.proxies.clone(),
            proxy_groups: state.proxy_groups.clone(),
            namespaces: state.namespaces.clone(),
            tables: state
                .tables
                .values()
                .map(|table| table.info.clone())
                .collect(),
            stats: stats_from_state(state, counters),
            next_table_id: state.next_table_id,
            topology_version: state.topology_version,
            topology_events: state.topology_events.clone(),
            scheduler_finish_generations: state.scheduler_finish_generations.clone(),
            dropped_since_ms: state.dropped_since_ms.clone(),
            meta_change_muted: state.meta_change_muted,
            frozen_since_ms: state.frozen_since_ms.clone(),
            reserved_names: state.reserved_names.clone(),
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
