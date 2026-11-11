// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[derive(Default, Deserialize)]
pub(super) struct MemoryNodeLookupRebuildRequest {
    #[serde(default)]
    scope: MemoryScope,
    #[serde(default)]
    after: String,
    #[serde(default)]
    max_pages: Option<usize>,
}

pub(super) fn memory_node_lookup_key(tenant: u64) -> String {
    format!("context:memory-node-lookup:{tenant:016x}")
}

pub(super) fn memory_record_node_hash(tenant: u64, record: &MemoryRecord) -> u64 {
    crate::context_workflow::context_source_node_hash(
        tenant,
        crate::context_workflow::ContextSourceKind::Chat,
        if record.context_ref.is_empty() {
            &record.id
        } else {
            &record.context_ref
        },
    )
}

pub(super) fn memory_node_lookup_write<'a>(
    tenant: u64,
    records: impl Iterator<Item = &'a MemoryRecord>,
) -> Command {
    Command::HashMultiSet {
        key: memory_node_lookup_key(tenant),
        entries: records
            .map(|record| {
                (
                    format!("{:016x}", memory_record_node_hash(tenant, record)),
                    memory_identity_record_member(&record.id),
                )
            })
            .collect(),
    }
}

pub(super) fn memory_node_lookup_remove(tenant: u64, node: u64) -> Command {
    Command::HashDelete {
        key: memory_node_lookup_key(tenant),
        field: format!("{node:016x}"),
    }
}

fn invalid_lookup(message: &str) -> Status {
    Status::error("invalid_memory_node_lookup", message)
}

impl ProxyService {
    fn memory_store_lookup_page<'a>(
        &self,
        tenant: u64,
        records: impl Iterator<Item = &'a MemoryRecord>,
    ) -> Result<usize, Status> {
        let command = memory_node_lookup_write(tenant, records);
        let mut retries = 0;
        loop {
            let response = self.batch_execute(BatchExecuteRequest {
                shard_id: self.context_shard_id(tenant),
                commands: vec![command.clone()],
            });
            let status = if !response.status.ok {
                response.status
            } else if let [response] = response.responses.as_slice() {
                response.status.clone()
            } else {
                invalid_lookup("incomplete lookup write response")
            };
            if status.ok {
                return Ok(retries);
            }
            // Replaying this immutable map overwrite cannot double-apply a counter or append.
            if status.code != "write_outcome_unknown" || retries == 2 {
                return Err(status);
            }
            std::thread::sleep(std::time::Duration::from_millis(10 << retries));
            retries += 1;
        }
    }

    pub(super) fn memory_lookup_node_ids(
        &self,
        tenant: u64,
        blocks: &[crate::context_workflow::ContextBlock],
    ) -> Result<HashMap<u64, String>, Status> {
        let mut seen = HashSet::with_capacity(blocks.len());
        let nodes = blocks
            .iter()
            .map(|block| block.node_hash)
            .filter(|node| seen.insert(*node))
            .collect::<Vec<_>>();
        let mut ids = HashMap::with_capacity(nodes.len());
        for page in nodes.chunks(MEMORY_CATALOG_PAGE_SIZE) {
            let response = self.execute(ExecuteRequest {
                shard_id: self.context_shard_id(tenant),
                command: Command::HashMultiGet {
                    key: memory_node_lookup_key(tenant),
                    fields: page.iter().map(|node| format!("{node:016x}")).collect(),
                },
            });
            if !response.status.ok {
                return Err(response.status);
            }
            let CommandResponse::Values { values } = response.response else {
                return Err(invalid_lookup("expected node lookup values"));
            };
            if values.len() != page.len() {
                return Err(invalid_lookup("incomplete node lookup response"));
            }
            for (node, value) in page.iter().zip(values) {
                if let Some(value) = value {
                    if value.len() != 8 {
                        return Err(invalid_lookup("invalid memory id suffix"));
                    }
                    let id = memory_id_from_identity_record_member(tenant, &value)
                        .ok_or_else(|| invalid_lookup("invalid memory id suffix"))?;
                    ids.insert(*node, id);
                }
            }
        }
        Ok(ids)
    }

    pub(super) fn memory_read_many_checked(
        &self,
        tenant: u64,
        ids: &[String],
    ) -> Result<Vec<Option<MemoryRecord>>, Status> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id: self.context_shard_id(tenant),
            commands: vec![
                Command::HashMultiGet {
                    key: memory_catalog_key(tenant),
                    fields: ids.to_vec(),
                },
                Command::HashMultiGet {
                    key: memory_tombstone_key(tenant),
                    fields: ids.to_vec(),
                },
            ],
        });
        if !response.status.ok {
            return Err(response.status);
        }
        if response.responses.len() != 2 {
            return Err(invalid_lookup("incomplete catalog response"));
        }
        let mut responses = response.responses.into_iter();
        let mut values = Vec::with_capacity(2);
        for response in &mut responses {
            if !response.status.ok {
                return Err(response.status);
            }
            let CommandResponse::Values { values: rows } = response.response else {
                return Err(invalid_lookup("expected catalog values"));
            };
            if rows.len() != ids.len() {
                return Err(invalid_lookup("incomplete catalog values"));
            }
            values.push(rows);
        }
        let tombstones = values.pop().expect("two catalog responses");
        let catalog = values.pop().expect("two catalog responses");
        ids.iter()
            .zip(catalog)
            .zip(tombstones)
            .map(|((id, bytes), tombstone)| {
                if tombstone.is_some() {
                    return Ok(None);
                }
                let Some(bytes) = bytes else {
                    return Ok(None);
                };
                let record = MemoryRecord::decode(&bytes)
                    .ok_or_else(|| invalid_lookup("invalid catalog record"))?;
                if &record.id != id || tenant_hash_from_memory_id(id) != Some(tenant) {
                    return Err(invalid_lookup("catalog identity mismatch"));
                }
                Ok(Some(record))
            })
            .collect()
    }

    pub(super) fn memory_read_one_checked(
        &self,
        tenant: u64,
        id: &str,
    ) -> Result<Option<MemoryRecord>, Status> {
        self.memory_read_many_checked(tenant, &[id.to_owned()])
            .map(|records| records.into_iter().next().flatten())
    }

    pub(super) fn memory_rebuild_node_lookup(
        &self,
        request: MemoryNodeLookupRebuildRequest,
    ) -> (u16, Vec<u8>) {
        let tenant =
            context::context_tenant_hash(&request.scope.context_scope(&MemoryIdentity::default()));
        let marker = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant),
            command: Command::StringGet {
                key: memory_order_ready_key(tenant),
            },
        });
        if !marker.status.ok {
            return crate::http::json_response(502, &marker.status);
        }
        if !matches!(marker.response, CommandResponse::Bytes { value: Some(ref value) } if value == b"1")
        {
            return crate::http::json_response(
                409,
                &Status::error(
                    "memory_order_index_not_ready",
                    "prepare the ordered memory index before backfill",
                ),
            );
        }
        if request.after.len() > 128 {
            return self.memory_bad_request("invalid lookup cursor".into());
        }
        let mut after = match hex::decode(&request.after) {
            Ok(after) => after,
            Err(_) => return self.memory_bad_request("invalid lookup cursor".into()),
        };
        let mut indexed = 0;
        let mut scanned = 0;
        let mut complete = false;
        let mut write_retries = 0;
        for _ in 0..request.max_pages.unwrap_or(1).clamp(1, 64) {
            let response = self.execute(ExecuteRequest {
                shard_id: self.context_shard_id(tenant),
                command: Command::SetScan {
                    key: memory_order_key(tenant),
                    after: after.clone(),
                    limit: MEMORY_CATALOG_PAGE_SIZE,
                },
            });
            if !response.status.ok {
                return crate::http::json_response(502, &response.status);
            }
            let CommandResponse::Members { members } = response.response else {
                return crate::http::json_response(
                    502,
                    &invalid_lookup("expected ordered members"),
                );
            };
            if members.len() > MEMORY_CATALOG_PAGE_SIZE {
                return crate::http::json_response(502, &invalid_lookup("oversized ordered page"));
            }
            let mut ids = Vec::with_capacity(members.len());
            for member in &members {
                if member <= &after {
                    return crate::http::json_response(
                        502,
                        &invalid_lookup("ordered cursor did not advance"),
                    );
                }
                let Some(id) = memory_id_from_order_member(member) else {
                    return crate::http::json_response(
                        502,
                        &invalid_lookup("invalid ordered member"),
                    );
                };
                ids.push(id);
                after.clone_from(member);
            }
            let records = match self.memory_read_many_checked(tenant, &ids) {
                Ok(records) => records,
                Err(status) => return crate::http::json_response(502, &status),
            };
            let live = records.iter().flatten().count();
            for chunk in records.chunks(MEMORY_UPDATE_PAGE_SIZE) {
                if chunk.iter().any(Option::is_some) {
                    match self.memory_store_lookup_page(tenant, chunk.iter().flatten()) {
                        Ok(retries) => write_retries += retries,
                        Err(status) => return crate::http::json_response(502, &status),
                    }
                }
            }
            indexed += live;
            scanned += members.len();
            if members.len() < MEMORY_CATALOG_PAGE_SIZE {
                complete = true;
                break;
            }
        }
        crate::http::json_response(
            200,
            &json!({
                "indexed": indexed, "scanned": scanned, "complete": complete,
                "write_retries": write_retries,
                "next": if complete { String::new() } else { hex::encode(after) },
            }),
        )
    }
}
