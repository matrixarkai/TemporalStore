//! SingleNodeMeta table CRUD operations (add/delete/freeze/update_table), extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn add_table(&self, request: AddTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::AddTable(request.clone()));
        self.apply_add_table(request)
    }

    pub(super) fn apply_add_table(&self, request: AddTableRequest) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        if request.shard_count == 0 {
            return AckResponse {
                status: Status::error("bad_request", "shard_count must be > 0"),
            };
        }
        if request.use_cpp_partition_ids {
            if request.shard_count > u32::MAX as u64 {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "shard_count exceeds C++ partition-set range",
                    ),
                };
            }
            if let Err(err) = validate_partition_set_count(request.shard_count as u32) {
                return AckResponse {
                    status: Status::error("bad_request", err.to_string()),
                };
            }
        }
        if let Err(err) = validate_serving_options(&request.serving_options) {
            return AckResponse {
                status: Status::error("bad_request", err),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.table_create_total += 1;
        state
            .namespaces
            .entry(request.namespace.clone())
            .or_insert(MetaEntityState::Normal);
        let key = table_key(&request.namespace, &request.table_name);
        if state.tables.contains_key(&key) {
            return AckResponse {
                status: Status::error("already_exists", "table already exists"),
            };
        }
        let table_id = state.next_table_id;
        if request.use_cpp_partition_ids && table_id > MAX_TABLE_ID as u64 {
            return AckResponse {
                status: Status::error(
                    "bad_request",
                    "table_id exceeds C++ partition id table range",
                ),
            };
        }
        state.next_table_id += 1;
        let namespace = request.namespace.clone();
        let table_name = request.table_name.clone();
        let topology_version = record_topology_event(
            &mut state,
            "add_table",
            format!("table:{namespace}/{table_name}"),
            format!(
                "shards={},replicas={}",
                request.shard_count,
                request.replica_count.max(1)
            ),
        );
        let first_shard_id = if request.use_cpp_partition_ids {
            match PartitionId::new(table_id, 0, 0, request.partition_version as u64) {
                Ok(partition_id) => partition_id.id(),
                Err(err) => {
                    return AckResponse {
                        status: Status::error("bad_request", err.to_string()),
                    };
                }
            }
        } else {
            request.first_shard_id
        };
        let info = TableMetaInfo {
            table_id,
            namespace: request.namespace,
            table_name: request.table_name,
            state: MetaEntityState::Normal,
            topology_version,
            first_shard_id,
            shard_count: request.shard_count,
            replica_count: request.replica_count.max(1),
            use_cpp_partition_ids: request.use_cpp_partition_ids,
            partition_version: request.partition_version,
            serving_options: request.serving_options,
        };
        state.tables.insert(key, TableRecord { info });
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::DeleteTable(request.clone()));
        self.apply_delete_table(request)
    }

    pub(super) fn apply_delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        let key = table_key(&request.namespace, &request.table_name);
        let Some(current_state) = state.tables.get(&key).map(|table| table.info.state) else {
            return AckResponse {
                status: Status::error("table_not_found", "table not found"),
            };
        };
        if current_state == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("table_not_found", "table already dropped"),
            };
        }
        let topology_version = record_topology_event(
            &mut state,
            "delete_table",
            format!("table:{}/{}", request.namespace, request.table_name),
            "state=dropped",
        );
        let table = state
            .tables
            .get_mut(&key)
            .expect("table exists after state check");
        table.info.state = MetaEntityState::Dropped;
        table.info.topology_version = topology_version;
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn freeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::FreezeTable(request.clone()));
        self.apply_set_table_state(request, MetaEntityState::Frozen)
    }

    pub fn unfreeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::UnfreezeTable(request.clone()));
        self.apply_set_table_state(request, MetaEntityState::Normal)
    }

    pub fn update_table(&self, request: UpdateTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::UpdateTable(request.clone()));
        self.apply_update_table(request)
    }

    pub(super) fn apply_update_table(&self, request: UpdateTableRequest) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        if request.shard_count.is_none()
            && request.replica_count.is_none()
            && request.first_shard_id.is_none()
            && request.use_cpp_partition_ids.is_none()
            && request.partition_version.is_none()
            && request.serving_options.is_none()
        {
            return AckResponse {
                status: Status::error("bad_request", "at least one table option is required"),
            };
        }

        let mut state = self.inner.write().expect("meta lock poisoned");
        let key = table_key(&request.namespace, &request.table_name);
        let Some(existing) = state.tables.get(&key).map(|table| table.info.clone()) else {
            return AckResponse {
                status: Status::error("table_not_found", "table not found"),
            };
        };
        if existing.state == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("table_not_found", "table is dropped"),
            };
        }
        if existing.state == MetaEntityState::Frozen {
            return AckResponse {
                status: Status::error("resource_frozen", "table is frozen"),
            };
        }
        if matches!(request.shard_count, Some(0)) {
            return AckResponse {
                status: Status::error("bad_request", "shard_count must be > 0"),
            };
        }
        if let Some(shard_count) = request.shard_count {
            if shard_count < existing.shard_count {
                return AckResponse {
                    status: Status::error("bad_request", "shard_count cannot shrink"),
                };
            }
            if existing.use_cpp_partition_ids {
                if shard_count > u32::MAX as u64 {
                    return AckResponse {
                        status: Status::error(
                            "bad_request",
                            "shard_count exceeds C++ partition-set range",
                        ),
                    };
                }
                if let Err(err) = validate_partition_set_count(shard_count as u32) {
                    return AckResponse {
                        status: Status::error("bad_request", err.to_string()),
                    };
                }
            }
        }
        if let Some(use_cpp_partition_ids) = request.use_cpp_partition_ids {
            if use_cpp_partition_ids != existing.use_cpp_partition_ids {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "use_cpp_partition_ids cannot change after table creation",
                    ),
                };
            }
        }
        if let Some(partition_version) = request.partition_version {
            if partition_version != existing.partition_version {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "partition_version cannot change after table creation",
                    ),
                };
            }
        }
        if let Some(first_shard_id) = request.first_shard_id {
            if existing.use_cpp_partition_ids {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "first_shard_id is derived for C++ partition-id tables",
                    ),
                };
            }
            if first_shard_id != existing.first_shard_id {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "first_shard_id cannot change after table creation",
                    ),
                };
            }
        }

        let new_shard_count = request.shard_count.unwrap_or(existing.shard_count);
        let new_replica_count = request
            .replica_count
            .map(|replica_count| replica_count.max(1))
            .unwrap_or(existing.replica_count);
        let new_serving_options = request
            .serving_options
            .as_ref()
            .map(|patch| apply_serving_options_patch(existing.serving_options.clone(), patch))
            .unwrap_or_else(|| existing.serving_options.clone());
        if let Err(err) = validate_serving_options(&new_serving_options) {
            return AckResponse {
                status: Status::error("bad_request", err),
            };
        }
        let changed = new_shard_count != existing.shard_count
            || new_replica_count != existing.replica_count
            || new_serving_options != existing.serving_options;
        if !changed {
            return AckResponse {
                status: Status::error("not_modified", "table options are unchanged"),
            };
        }
        let topology_version = record_topology_event(
            &mut state,
            "update_table",
            format!("table:{}/{}", request.namespace, request.table_name),
            format!("shards={new_shard_count},replicas={new_replica_count}"),
        );
        let table = state
            .tables
            .get_mut(&key)
            .expect("table exists after update validation");
        table.info.shard_count = new_shard_count;
        table.info.replica_count = new_replica_count;
        table.info.serving_options = new_serving_options;
        table.info.topology_version = topology_version;
        AckResponse {
            status: Status::ok(),
        }
    }
}
