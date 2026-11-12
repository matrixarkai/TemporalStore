// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn checked_batch(response: &BatchExecuteResponse, expected: usize) -> Result<(), Status> {
    if !response.status.ok {
        return Err(response.status.clone());
    }
    if response.responses.len() != expected {
        return Err(Status::error(
            "memory_read_incomplete",
            "memory read returned an incomplete batch",
        ));
    }
    for row in &response.responses {
        if !row.status.ok {
            return Err(row.status.clone());
        }
    }
    Ok(())
}

pub(super) fn checked_counts(
    response: &BatchExecuteResponse,
    expected: usize,
) -> Result<Vec<usize>, Status> {
    checked_batch(response, expected)?;
    response
        .responses
        .iter()
        .map(|row| match &row.response {
            CommandResponse::Bytes { value: None } => Ok(0),
            CommandResponse::Bytes { value: Some(bytes) } => std::str::from_utf8(bytes)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| {
                    value.is_finite()
                        && *value >= 0.0
                        && value.fract() == 0.0
                        && *value < usize::MAX as f64
                })
                .map(|value| value as usize)
                .ok_or_else(|| {
                    Status::error(
                        "invalid_memory_count_response",
                        "memory count must be a nonnegative integer",
                    )
                }),
            _ => Err(Status::error(
                "invalid_memory_count_response",
                "expected a memory count score",
            )),
        })
        .collect()
}

impl ProxyService {
    // Mutation callers must distinguish an exhausted page from a failed read. Keep the existing
    // one-posting page bound and hydrate catalog/tombstones together, not one RPC per record.
    pub(super) fn memory_scoped_delete_page(
        &self,
        tenant: u64,
        member: &[u8],
        after: &[u8],
    ) -> Result<MemoryRecordPage, Status> {
        let shard_id = self.context_shard_id(tenant);
        let response = self.execute(ExecuteRequest {
            shard_id,
            command: Command::SetScan {
                key: memory_identity_records_key(tenant, member),
                after: after.to_vec(),
                limit: MEMORY_IDENTITY_POSTINGS_PER_PAGE,
            },
        });
        if !response.status.ok {
            return Err(response.status);
        }
        let postings = match response.response {
            CommandResponse::Members { members } => members,
            _ => {
                return Err(Status::error(
                    "invalid_memory_identity_response",
                    "expected identity postings",
                ))
            }
        };
        if postings.is_empty() {
            return Ok(MemoryRecordPage::default());
        }
        if postings.len() > MEMORY_IDENTITY_POSTINGS_PER_PAGE
            || postings.iter().any(|posting| posting.as_slice() <= after)
            || postings.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Status::error(
                "invalid_memory_identity_response",
                "identity page must advance within its limit",
            ));
        }
        let next = postings.last().cloned().unwrap_or_default();
        let fields = postings
            .iter()
            .map(|posting| {
                std::str::from_utf8(posting)
                    .map(str::to_owned)
                    .map_err(|_| {
                        Status::error(
                            "invalid_memory_identity_posting",
                            "posting key is not UTF-8",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let response = self.execute(ExecuteRequest {
            shard_id,
            command: Command::HashMultiGet {
                key: memory_identity_postings_key(tenant),
                fields,
            },
        });
        if !response.status.ok {
            return Err(response.status);
        }
        let values = match response.response {
            CommandResponse::Values { values } if values.len() == postings.len() => values,
            _ => {
                return Err(Status::error(
                    "invalid_memory_identity_response",
                    "identity posting values are incomplete",
                ))
            }
        };
        let mut keyed_ids = Vec::new();
        for (posting, value) in postings.into_iter().zip(values) {
            let value = value.ok_or_else(|| {
                Status::error(
                    "memory_identity_posting_missing",
                    "identity posting disappeared during the read",
                )
            })?;
            if value.len() % 8 != 0 {
                return Err(Status::error(
                    "invalid_memory_identity_posting",
                    "posting has an incomplete memory id",
                ));
            }
            keyed_ids.extend(
                memory_ids_from_identity_posting(tenant, &value)
                    .into_iter()
                    .map(|id| (posting.clone(), id)),
            );
        }
        let ids = keyed_ids
            .iter()
            .map(|(_, id)| id.clone())
            .collect::<Vec<_>>();
        let candidate_count = ids.len();
        let records = self.memory_scoped_delete_records(tenant, &ids)?;
        let rows = keyed_ids
            .into_iter()
            .zip(records)
            .filter_map(|((posting, _), record)| record.map(|record| (posting, record)))
            .collect();
        Ok(MemoryRecordPage {
            after: next,
            rows,
            candidate_count,
        })
    }

    fn memory_scoped_delete_records(
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
        checked_batch(&response, 2)?;
        let mut rows = response.responses.into_iter();
        let values = |row: Option<ExecuteResponse>| match row.map(|row| row.response) {
            Some(CommandResponse::Values { values }) if values.len() == ids.len() => Ok(values),
            _ => Err(Status::error(
                "invalid_memory_catalog_response",
                "memory record values are incomplete",
            )),
        };
        let catalog = values(rows.next())?;
        let tombstones = values(rows.next())?;
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
                let record = MemoryRecord::decode(&bytes).ok_or_else(|| {
                    Status::error(
                        "invalid_memory_catalog_record",
                        "memory record cannot be decoded",
                    )
                })?;
                if &record.id != id || tenant_hash_from_memory_id(&record.id) != Some(tenant) {
                    return Err(Status::error(
                        "invalid_memory_catalog_identity",
                        "memory record identity mismatch",
                    ));
                }
                Ok(Some(record))
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "memory_scoped_delete_tests.rs"]
mod tests;
