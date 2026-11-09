// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;

use thiserror::Error;

use crate::types::{Command, CommandResponse, ExecuteRequest, ShardId};
use crate::TemporalEngine;

use super::{
    CompactEmbeddingBatch, EmbeddingIndex, EmbeddingIndexError, EmbeddingKey, EmbeddingWriteReport,
};

const COMPACT_ROW_VERSION: u8 = 1;
const COMPACT_ROW_HEADER_BYTES: usize = 1 + 2 + 8 + 4;
const MAX_ROWS_PER_WRITE: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingRecoveryReport {
    pub tenant_id: u64,
    pub recovered_rows: usize,
    pub total_entries: usize,
    pub quantized_vector_bytes: usize,
}

#[derive(Debug, Error)]
pub enum TemporalEmbeddingStoreError {
    #[error("embedding namespace must contain 1 to 64 ASCII letters, digits, '-' or '_'")]
    InvalidNamespace,
    #[error("durable embedding storage requires a nonzero model hash")]
    MissingModelHash,
    #[error("TemporalStore command failed with {code}: {message}")]
    StoreStatus { code: String, message: String },
    #[error("TemporalStore returned an unexpected response for embedding storage")]
    UnexpectedResponse,
    #[error("embedding field '{field}' is not a numeric item id")]
    InvalidItemId { field: String },
    #[error("embedding row for item {item_id} is malformed")]
    MalformedRow { item_id: u64 },
    #[error("embedding row for item {item_id} uses format version {version}")]
    UnsupportedRowVersion { item_id: u64, version: u8 },
    #[error("embedding row for item {item_id} has {actual} dimensions; expected {expected}")]
    DimensionMismatch {
        item_id: u64,
        expected: usize,
        actual: usize,
    },
    #[error("embedding row for item {item_id} uses model {actual}; expected {expected}")]
    ModelMismatch {
        item_id: u64,
        expected: u64,
        actual: u64,
    },
    #[error(transparent)]
    Index(#[from] EmbeddingIndexError),
}

/// Durable compact-vector storage backed by TemporalStore's WAL-protected hash model.
///
/// One hash belongs to one tenant, model and vector width. Writes are capped at a bounded
/// number of rows per command, while recovery reads all compact rows for a tenant once.
pub struct TemporalEmbeddingStore<'a> {
    engine: &'a TemporalEngine,
    shard_id: ShardId,
    namespace: String,
}

impl<'a> TemporalEmbeddingStore<'a> {
    pub fn new(
        engine: &'a TemporalEngine,
        shard_id: ShardId,
        namespace: impl Into<String>,
    ) -> Result<Self, TemporalEmbeddingStoreError> {
        let namespace = namespace.into();
        if namespace.is_empty()
            || namespace.len() > 64
            || !namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(TemporalEmbeddingStoreError::InvalidNamespace);
        }
        Ok(Self {
            engine,
            shard_id,
            namespace,
        })
    }

    pub fn persist_batch(
        &self,
        batch: &CompactEmbeddingBatch,
    ) -> Result<(), TemporalEmbeddingStoreError> {
        if batch.model_hash() == 0 {
            return Err(TemporalEmbeddingStoreError::MissingModelHash);
        }
        if batch.is_empty() {
            return Ok(());
        }

        let first_tenant = batch.key(0).tenant_id;
        if (1..batch.len()).all(|row| batch.key(row).tenant_id == first_tenant) {
            for start in (0..batch.len()).step_by(MAX_ROWS_PER_WRITE) {
                let end = start.saturating_add(MAX_ROWS_PER_WRITE).min(batch.len());
                self.persist_tenant_rows(
                    first_tenant,
                    batch.model_hash(),
                    batch.dimensions(),
                    encode_compact_rows(batch, start..end),
                )?;
            }
            return Ok(());
        }

        let mut tenants = BTreeMap::<u64, Vec<usize>>::new();
        for row in 0..batch.len() {
            tenants
                .entry(batch.key(row).tenant_id)
                .or_default()
                .push(row);
        }
        for (tenant_id, rows) in tenants {
            for chunk in rows.chunks(MAX_ROWS_PER_WRITE) {
                self.persist_tenant_rows(
                    tenant_id,
                    batch.model_hash(),
                    batch.dimensions(),
                    encode_compact_rows(batch, chunk.iter().copied()),
                )?;
            }
        }
        Ok(())
    }

    pub fn recover_tenant(
        &self,
        tenant_id: u64,
        index: &mut EmbeddingIndex,
    ) -> Result<EmbeddingRecoveryReport, TemporalEmbeddingStoreError> {
        if index.model_hash() == 0 {
            return Err(TemporalEmbeddingStoreError::MissingModelHash);
        }
        let response = self.engine.execute(ExecuteRequest {
            shard_id: self.shard_id,
            command: Command::HashGetAll {
                key: self.store_key(tenant_id, index.model_hash(), index.dimensions()),
            },
        });
        ensure_ok(
            response.status.ok,
            response.status.code,
            response.status.message,
        )?;
        let CommandResponse::HashEntries { entries } = response.response else {
            return Err(TemporalEmbeddingStoreError::UnexpectedResponse);
        };

        let mut keys = Vec::with_capacity(entries.len());
        let mut scales = Vec::with_capacity(entries.len());
        let mut vectors = Vec::with_capacity(entries.len().saturating_mul(index.dimensions()));
        for (field, value) in entries {
            let item_id = field
                .parse::<u64>()
                .map_err(|_| TemporalEmbeddingStoreError::InvalidItemId { field })?;
            let (scale, row) =
                decode_compact_row(item_id, &value, index.dimensions(), index.model_hash())?;
            keys.push(EmbeddingKey { tenant_id, item_id });
            scales.push(scale);
            vectors.extend_from_slice(&row);
        }
        let recovered_rows = keys.len();
        let batch = CompactEmbeddingBatch::from_compact_rows(
            index.dimensions(),
            index.model_hash(),
            keys,
            scales,
            vectors,
        )?;
        let write = index.upsert_compact_batch(&batch)?;
        Ok(recovery_report(tenant_id, recovered_rows, write))
    }

    pub fn delete(
        &self,
        key: EmbeddingKey,
        model_hash: u64,
        dimensions: usize,
    ) -> Result<bool, TemporalEmbeddingStoreError> {
        let response = self.engine.execute(ExecuteRequest {
            shard_id: self.shard_id,
            command: Command::HashDelete {
                key: self.store_key(key.tenant_id, model_hash, dimensions),
                field: key.item_id.to_string(),
            },
        });
        ensure_ok(
            response.status.ok,
            response.status.code,
            response.status.message,
        )?;
        match response.response {
            CommandResponse::Integer { value } => Ok(value != 0),
            _ => Err(TemporalEmbeddingStoreError::UnexpectedResponse),
        }
    }

    fn store_key(&self, tenant_id: u64, model_hash: u64, dimensions: usize) -> String {
        format!(
            "llm:emb:{}:{tenant_id}:{model_hash}:{dimensions}",
            self.namespace
        )
    }

    fn persist_tenant_rows(
        &self,
        tenant_id: u64,
        model_hash: u64,
        dimensions: usize,
        entries: Vec<(String, Vec<u8>)>,
    ) -> Result<(), TemporalEmbeddingStoreError> {
        let response = self.engine.execute(ExecuteRequest {
            shard_id: self.shard_id,
            command: Command::HashMultiSet {
                key: self.store_key(tenant_id, model_hash, dimensions),
                entries,
            },
        });
        ensure_ok(
            response.status.ok,
            response.status.code,
            response.status.message,
        )
    }
}

fn recovery_report(
    tenant_id: u64,
    recovered_rows: usize,
    write: EmbeddingWriteReport,
) -> EmbeddingRecoveryReport {
    EmbeddingRecoveryReport {
        tenant_id,
        recovered_rows,
        total_entries: write.total_entries,
        quantized_vector_bytes: write.quantized_vector_bytes,
    }
}

fn ensure_ok(ok: bool, code: String, message: String) -> Result<(), TemporalEmbeddingStoreError> {
    if ok {
        Ok(())
    } else {
        Err(TemporalEmbeddingStoreError::StoreStatus { code, message })
    }
}

fn encode_compact_row(batch: &CompactEmbeddingBatch, row: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(COMPACT_ROW_HEADER_BYTES + batch.dimensions());
    output.push(COMPACT_ROW_VERSION);
    output.extend_from_slice(&(batch.dimensions() as u16).to_le_bytes());
    output.extend_from_slice(&batch.model_hash().to_le_bytes());
    output.extend_from_slice(&batch.scale(row).to_le_bytes());
    output.extend(batch.vector(row).iter().map(|value| *value as u8));
    output
}

fn encode_compact_rows(
    batch: &CompactEmbeddingBatch,
    rows: impl ExactSizeIterator<Item = usize>,
) -> Vec<(String, Vec<u8>)> {
    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        entries.push((
            batch.key(row).item_id.to_string(),
            encode_compact_row(batch, row),
        ));
    }
    entries
}

fn decode_compact_row(
    item_id: u64,
    value: &[u8],
    expected_dimensions: usize,
    expected_model_hash: u64,
) -> Result<(f32, Vec<i8>), TemporalEmbeddingStoreError> {
    if value.len() < COMPACT_ROW_HEADER_BYTES {
        return Err(TemporalEmbeddingStoreError::MalformedRow { item_id });
    }
    if value[0] != COMPACT_ROW_VERSION {
        return Err(TemporalEmbeddingStoreError::UnsupportedRowVersion {
            item_id,
            version: value[0],
        });
    }
    let dimensions = u16::from_le_bytes([value[1], value[2]]) as usize;
    if dimensions != expected_dimensions {
        return Err(TemporalEmbeddingStoreError::DimensionMismatch {
            item_id,
            expected: expected_dimensions,
            actual: dimensions,
        });
    }
    if value.len() != COMPACT_ROW_HEADER_BYTES + dimensions {
        return Err(TemporalEmbeddingStoreError::MalformedRow { item_id });
    }
    let model_hash = u64::from_le_bytes(value[3..11].try_into().expect("fixed model header"));
    if model_hash != expected_model_hash {
        return Err(TemporalEmbeddingStoreError::ModelMismatch {
            item_id,
            expected: expected_model_hash,
            actual: model_hash,
        });
    }
    let scale = f32::from_le_bytes(value[11..15].try_into().expect("fixed scale header"));
    if !scale.is_finite() || scale <= 0.0 {
        return Err(TemporalEmbeddingStoreError::MalformedRow { item_id });
    }
    let row = value[COMPACT_ROW_HEADER_BYTES..]
        .iter()
        .map(|byte| *byte as i8)
        .collect();
    Ok((scale, row))
}

#[cfg(test)]
mod tests {
    use matrixcache::MultiLayerCache;

    use super::*;
    use crate::llm_context::{EmbeddingIndexConfig, EmbeddingVector};

    #[test]
    fn compact_rows_round_trip_through_temporalstore() {
        let engine = TemporalEngine::new(MultiLayerCache::default());
        engine.load_shard(1);
        let store = TemporalEmbeddingStore::new(&engine, 1, "context").unwrap();
        let config = EmbeddingIndexConfig::new(3, 16).with_model_hash(91);
        let source = [1.0, 0.5, 0.0];
        let mut writer = EmbeddingIndex::new(config).unwrap();
        let compact = writer
            .prepare_batch(&[EmbeddingVector {
                key: EmbeddingKey {
                    tenant_id: 7,
                    item_id: 42,
                },
                values: &source,
            }])
            .unwrap();
        store.persist_batch(&compact).unwrap();
        writer.upsert_compact_batch(&compact).unwrap();

        let mut recovered = EmbeddingIndex::new(config).unwrap();
        let report = store.recover_tenant(7, &mut recovered).unwrap();
        assert_eq!(report.recovered_rows, 1);
        assert_eq!(recovered.search(7, &source, 1).unwrap()[0].key.item_id, 42);
    }

    #[test]
    fn single_tenant_batches_persist_in_bounded_chunks() {
        const ROWS: usize = MAX_ROWS_PER_WRITE + 1;

        let engine = TemporalEngine::new(MultiLayerCache::default());
        engine.load_shard(1);
        let store = TemporalEmbeddingStore::new(&engine, 1, "context").unwrap();
        let config = EmbeddingIndexConfig::new(3, ROWS).with_model_hash(91);
        let values = (0..ROWS)
            .map(|row| [1.0, row as f32 + 1.0, 0.5])
            .collect::<Vec<_>>();
        let inputs = values
            .iter()
            .enumerate()
            .map(|(row, values)| EmbeddingVector {
                key: EmbeddingKey {
                    tenant_id: 7,
                    item_id: row as u64,
                },
                values,
            })
            .collect::<Vec<_>>();
        let writer = EmbeddingIndex::new(config).unwrap();
        let compact = writer.prepare_batch(&inputs).unwrap();

        store.persist_batch(&compact).unwrap();

        let mut recovered = EmbeddingIndex::new(config).unwrap();
        assert_eq!(
            store
                .recover_tenant(7, &mut recovered)
                .unwrap()
                .recovered_rows,
            ROWS
        );
    }

    #[test]
    fn mixed_tenant_batches_persist_in_bounded_chunks() {
        const ROWS_PER_TENANT: usize = MAX_ROWS_PER_WRITE + 1;

        let engine = TemporalEngine::new(MultiLayerCache::default());
        engine.load_shard(1);
        let store = TemporalEmbeddingStore::new(&engine, 1, "context").unwrap();
        let config = EmbeddingIndexConfig::new(3, ROWS_PER_TENANT * 2).with_model_hash(91);
        let values = (0..ROWS_PER_TENANT * 2)
            .map(|row| [1.0, row as f32 + 1.0, 0.5])
            .collect::<Vec<_>>();
        let inputs = values
            .iter()
            .enumerate()
            .map(|(row, values)| EmbeddingVector {
                key: EmbeddingKey {
                    tenant_id: 7 + (row % 2) as u64,
                    item_id: (row / 2) as u64,
                },
                values,
            })
            .collect::<Vec<_>>();
        let writer = EmbeddingIndex::new(config).unwrap();
        let compact = writer.prepare_batch(&inputs).unwrap();

        store.persist_batch(&compact).unwrap();

        let mut recovered = EmbeddingIndex::new(config).unwrap();
        assert_eq!(
            store
                .recover_tenant(7, &mut recovered)
                .unwrap()
                .recovered_rows,
            ROWS_PER_TENANT
        );
        assert_eq!(
            store
                .recover_tenant(8, &mut recovered)
                .unwrap()
                .recovered_rows,
            ROWS_PER_TENANT
        );
        assert_eq!(recovered.len(), ROWS_PER_TENANT * 2);
    }
}
