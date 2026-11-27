// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

enum Presence {
    All,
    Inline(u64),
    Wide(Vec<bool>),
}

impl Presence {
    fn capture(scratch: &container_blocks::Utf8ProjectionScratch, count: usize) -> Self {
        if (0..count).all(|index| scratch.is_present_in_index(index)) {
            Self::All
        } else if count <= 64 {
            Self::Inline((0..count).fold(0, |bits, index| {
                bits | (u64::from(scratch.is_present_in_index(index)) << index)
            }))
        } else {
            Self::Wide(
                (0..count)
                    .map(|index| scratch.is_present_in_index(index))
                    .collect(),
            )
        }
    }

    fn contains(&self, index: usize) -> bool {
        match self {
            Self::All => true,
            Self::Inline(bits) => index < 64 && bits & (1 << index) != 0,
            Self::Wide(bits) => bits.get(index).copied().unwrap_or(false),
        }
    }
}

enum ReadSource {
    Ready(ProjectedHashValues),
    Packed {
        address: ElementEntry,
        first_field: usize,
        present: Presence,
    },
    Separate(Vec<Option<ElementEntry>>),
}

pub(in crate::engine) struct PreparedHashRead(ReadSource);

impl PreparedHashRead {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::engine) fn prepare(
        cache: &MultiLayerCache,
        shard_id: ShardId,
        shard: &ShardState,
        key: &str,
        fields: &[String],
        projection: &container_blocks::Utf8ProjectionPlan<'_>,
        scratch: &mut container_blocks::Utf8ProjectionScratch,
        memory_only: bool,
        cache_budget: usize,
    ) -> Self {
        if let [field] = fields {
            if let Some(bytes) = shard.bucket_index.hashes.cached_timestamp_bytes(key, field) {
                if bytes
                    .len()
                    .saturating_add(std::mem::size_of::<Option<Vec<u8>>>())
                    <= cache_budget
                {
                    #[cfg(test)]
                    PROJECTED_FIELD_CACHE_HITS
                        .with(|count| count.set(count.get().saturating_add(1)));
                    return Self(ReadSource::Ready(ProjectedHashValues::Owned(vec![Some(
                        bytes.to_vec(),
                    )])));
                }
            }
        }
        let Some(stored_fields) = shard.bucket_index.hashes.get(key) else {
            return Self(ReadSource::Ready(ProjectedHashValues::Missing(
                fields.len(),
            )));
        };
        if let [field] = fields {
            if stored_fields.get(field).is_some() {
                // Only memory may be consulted while preparing an unlocked batch snapshot.
                let frame =
                    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::CacheRead, || {
                        let cache_key = CacheKey::hash(shard_id, key, field);
                        if memory_only {
                            cache.get_memory_shared(&cache_key)
                        } else {
                            cache.get_shared(&cache_key).ok().flatten()
                        }
                    });
                if let Some(frame) = frame.filter(|frame| {
                    frame
                        .len()
                        .saturating_add(std::mem::size_of::<Option<std::ops::Range<usize>>>())
                        <= cache_budget
                }) {
                    if let Some(Some(value)) =
                        super::super::command_validation::decode_borrowed_cached_bytes(&frame)
                    {
                        let range = if value.is_empty() {
                            Some(0..0)
                        } else {
                            (value.as_ptr() as usize)
                                .checked_sub(frame.as_ptr() as usize)
                                .and_then(|start| {
                                    start
                                        .checked_add(value.len())
                                        .filter(|end| *end <= frame.len())
                                        .map(|end| start..end)
                                })
                        };
                        if let Some(range) = range {
                            #[cfg(test)]
                            PROJECTED_FIELD_CACHE_HITS
                                .with(|count| count.set(count.get().saturating_add(1)));
                            return Self(ReadSource::Ready(ProjectedHashValues::Shared {
                                frame,
                                ranges: vec![Some(range)],
                            }));
                        }
                    }
                }
            }
        }
        let mut shared: Option<(&ElementEntry, usize)> = None;
        let mut present_count = 0;
        scratch.reset_index_presence(fields.len());
        let mut stored = stored_fields.entries().peekable();
        for &index in projection.positions_by_name() {
            let field = fields[index].as_str();
            while stored.peek().is_some_and(|(name, _)| &name[..] < field) {
                stored.next();
            }
            let Some(&(name, address)) = stored.peek() else {
                break;
            };
            if &name[..] != field {
                continue;
            }
            scratch.mark_present_in_index(index);
            present_count += 1;
            match shared {
                None => shared = Some((address, index)),
                Some((held, _)) if held == address => (),
                Some(_) => {
                    return Self(ReadSource::Separate(
                        fields
                            .iter()
                            .map(|field| stored_fields.get(field).cloned())
                            .collect(),
                    ))
                }
            }
        }
        match shared {
            Some((address, first_field)) => Self(ReadSource::Packed {
                address: address.clone(),
                first_field,
                present: if present_count == fields.len() {
                    Presence::All
                } else {
                    Presence::capture(scratch, fields.len())
                },
            }),
            None => Self(ReadSource::Ready(ProjectedHashValues::Missing(
                fields.len(),
            ))),
        }
    }

    pub(in crate::engine) fn retained_bytes(&self) -> usize {
        match &self.0 {
            ReadSource::Ready(value) => value.retained_bytes(),
            _ => 0,
        }
    }

    pub(in crate::engine) fn pin_records(
        &self,
        snapshot: &mut Option<block_in_wal::ReadSnapshotGuard>,
        slabs: &mut Option<crate::block_store::BlockStoreReadSnapshot>,
        blocks: &BlockStore,
        shard: ShardId,
        key: &str,
        fields: &[String],
        rows: usize,
    ) {
        let mut pin = |index: usize, address: &ElementEntry| {
            if crate::wal_record::is_wal_resident(address.block_slab_id()) {
                snapshot
                    .get_or_insert_with(|| {
                        block_in_wal::ReadSnapshotGuard::retained_batch(blocks, shard, rows)
                    })
                    .pin(
                        blocks,
                        stable_block_object_id(shard, "hash", key),
                        Some(&fields[index]),
                        address,
                    );
            } else {
                slabs
                    .get_or_insert_with(|| crate::block_store::BlockStoreReadSnapshot::new(blocks))
                    .pin(address.block_slab_id());
            }
        };
        match &self.0 {
            ReadSource::Packed {
                address,
                first_field,
                ..
            } => pin(*first_field, address),
            ReadSource::Separate(addresses) => {
                for (index, address) in addresses.iter().enumerate() {
                    if let Some(address) = address {
                        pin(index, address);
                    }
                }
            }
            ReadSource::Ready(_) => (),
        }
    }

    pub(in crate::engine) fn load(
        self,
        cache: &MultiLayerCache,
        blocks: &BlockStore,
        shard: ShardId,
        bucket: u32,
        key: &str,
        fields: &[String],
        projection: &container_blocks::Utf8ProjectionPlan<'_>,
        scratch: &mut container_blocks::Utf8ProjectionScratch,
    ) -> ProjectedHashValues {
        match self.0 {
            ReadSource::Ready(value) => value,
            ReadSource::Packed {
                address,
                first_field,
                present,
            } => {
                #[cfg(test)]
                PROJECTED_FRAME_FETCHES.with(|count| count.set(count.get().saturating_add(1)));
                let Some(frame) = read_block_frame_shared(
                    cache,
                    blocks,
                    shard,
                    &address,
                    BlockIdentity::of(shard, "hash", key, Some(&fields[first_field])),
                    Some(bucket),
                ) else {
                    return ProjectedHashValues::Missing(fields.len());
                };
                match container_blocks::project_utf8_container_element_ranges_with_plan(
                    &frame, projection, scratch,
                ) {
                    container_blocks::Utf8ContainerRangeProjection::Values(mut ranges) => {
                        if !matches!(&present, Presence::All) {
                            for (index, range) in ranges.iter_mut().enumerate() {
                                if !present.contains(index) {
                                    *range = None;
                                }
                            }
                        }
                        ProjectedHashValues::Shared { frame, ranges }
                    }
                    container_blocks::Utf8ContainerRangeProjection::Corrupt(_) => {
                        note_corrupt_container_block();
                        ProjectedHashValues::Missing(fields.len())
                    }
                    container_blocks::Utf8ContainerRangeProjection::NotFramed
                    | container_blocks::Utf8ContainerRangeProjection::OtherSpelling => {
                        ProjectedHashValues::Owned(
                            fields
                                .iter()
                                .enumerate()
                                .map(|(index, field)| {
                                    present
                                        .contains(index)
                                        .then(|| select_container_value(&frame, field))
                                        .flatten()
                                })
                                .collect(),
                        )
                    }
                }
            }
            ReadSource::Separate(addresses) => ProjectedHashValues::Owned(
                addresses
                    .iter()
                    .enumerate()
                    .map(|(index, address)| {
                        address.as_ref().and_then(|address| {
                            #[cfg(test)]
                            PROJECTED_FRAME_FETCHES
                                .with(|count| count.set(count.get().saturating_add(1)));
                            read_block_bytes(
                                cache,
                                blocks,
                                shard,
                                address,
                                BlockIdentity::of(shard, "hash", key, Some(&fields[index])),
                                Some(bucket),
                            )
                        })
                    })
                    .collect(),
            ),
        }
    }
}

type Row = (usize, u32, Result<PreparedHashRead, Status>);

pub(in crate::engine) struct PreparedHashBatch {
    first: Option<Row>,
    remaining: Vec<Row>,
    cached_bytes: usize,
}

impl PreparedHashBatch {
    pub(in crate::engine) fn new(rows: usize) -> Self {
        Self {
            first: None,
            remaining: Vec::with_capacity(rows.saturating_sub(1)),
            cached_bytes: 0,
        }
    }

    pub(in crate::engine) fn cache_budget(&self) -> usize {
        (8 * 1024 * 1024usize).saturating_sub(self.cached_bytes)
    }

    pub(in crate::engine) fn push(
        &mut self,
        index: usize,
        bucket: u32,
        result: Result<PreparedHashRead, Status>,
    ) {
        self.cached_bytes = self.cached_bytes.saturating_add(match &result {
            Ok(plan) => plan.retained_bytes(),
            Err(status) => status
                .code
                .capacity()
                .saturating_add(status.message.capacity()),
        });
        if self.first.is_none() {
            self.first = Some((index, bucket, result));
        } else {
            self.remaining.push((index, bucket, result));
        }
    }

    pub(in crate::engine) fn into_rows(self) -> impl Iterator<Item = Row> {
        self.first.into_iter().chain(self.remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_plan_has_no_row_vector_and_preserves_error_order() {
        let point = PreparedHashBatch::new(1);
        assert_eq!(point.remaining.capacity(), 0);
        let mut rows = PreparedHashBatch::new(3);
        rows.push(
            0,
            2,
            Ok(PreparedHashRead(ReadSource::Ready(
                ProjectedHashValues::Missing(2),
            ))),
        );
        rows.push(1, 0, Err(Status::error("limited", "read rejected")));
        rows.push(
            2,
            3,
            Ok(PreparedHashRead(ReadSource::Ready(
                ProjectedHashValues::Missing(0),
            ))),
        );
        assert_eq!(
            rows.into_rows()
                .map(|(index, bucket, value)| (index, bucket, value.is_ok()))
                .collect::<Vec<_>>(),
            [(0, 2, true), (1, 0, false), (2, 3, true)]
        );
    }

    #[test]
    fn cached_payload_budget_counts_complete_frames() {
        let mut rows = PreparedHashBatch::new(2);
        let frame: std::sync::Arc<[u8]> = vec![7; 8192].into();
        rows.push(
            0,
            0,
            Ok(PreparedHashRead(ReadSource::Ready(
                ProjectedHashValues::Shared {
                    frame,
                    ranges: vec![Some(0..1)],
                },
            ))),
        );
        assert!(rows.cache_budget() < 8 * 1024 * 1024 - 8192);
    }

    #[test]
    fn membership_masks_preserve_missing_slots_beyond_one_word() {
        for count in [0, 1, 64, 65, 100] {
            let mut scratch = container_blocks::Utf8ProjectionScratch::new();
            scratch.reset_index_presence(count);
            for index in (0..count).filter(|index| index % 3 != 0) {
                scratch.mark_present_in_index(index);
            }
            let captured = Presence::capture(&scratch, count);
            for index in 0..count {
                assert_eq!(captured.contains(index), index % 3 != 0);
            }
        }
    }
}
