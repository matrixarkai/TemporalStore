// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::timestamped_series::TimestampedSeries;
use super::*;

const KINDS: [&str; 6] = [
    "context_event",
    "context_index",
    "context_audit",
    "context_child",
    "context_summary",
    "context_compression",
];
type Counts = [usize; 6];

#[cfg(test)]
thread_local! {
    static WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(test)]
pub(super) fn take_work() -> (usize, usize) {
    WORK.with(|work| work.replace((0, 0)))
}

fn lengths(shard: &ShardState, key: &str) -> Counts {
    if !key.starts_with("ctx:") && !key.starts_with("ctxidx:") {
        return [0; 6];
    }
    [
        shard.context_events().get(key).map_or(0, |s| s.len()),
        shard.context_indexes().get(key).map_or(0, |s| s.len()),
        shard.context_audits().get(key).map_or(0, |s| s.len()),
        shard.context_children().get(key).map_or(0, |s| s.len()),
        shard.context_summaries().get(key).map_or(0, |s| s.len()),
        shard.context_compressions().get(key).map_or(0, |s| s.len()),
    ]
}

fn addresses(shard: &ShardState, kind: &str, key: &str) -> Vec<ElementEntry> {
    fn collect<S: TimestampedSeries>(series: Option<&S>) -> Vec<ElementEntry> {
        series
            .into_iter()
            .flat_map(|s| s.addresses())
            .map(|address| {
                #[cfg(test)]
                WORK.with(|work| {
                    let (published, visited) = work.get();
                    work.set((published, visited + 1));
                });
                address.clone()
            })
            .collect()
    }
    match kind {
        "context_event" => collect(shard.context_events().get(key)),
        "context_index" => collect(shard.context_indexes().get(key)),
        "context_audit" => collect(shard.context_audits().get(key)),
        "context_child" => collect(shard.context_children().get(key)),
        "context_summary" => collect(shard.context_summaries().get(key)),
        "context_compression" => collect(shard.context_compressions().get(key)),
        _ => Vec::new(),
    }
}

pub(super) enum ContextSeriesBefore {
    None,
    One(Counts),
    Many(Vec<Counts>),
}

impl ContextSeriesBefore {
    pub(super) fn capture(shard: &ShardState, keys: &[String]) -> Self {
        if !keys
            .iter()
            .any(|key| key.starts_with("ctx:") || key.starts_with("ctxidx:"))
        {
            return Self::None;
        }
        match keys {
            [key] => Self::One(lengths(shard, key)),
            keys => Self::Many(keys.iter().map(|key| lengths(shard, key)).collect()),
        }
    }

    pub(super) fn sync(
        &self,
        shard: &mut ShardState,
        shard_id: ShardId,
        key: &str,
        index: usize,
        outcome_start: usize,
    ) -> bool {
        let before = match self {
            Self::None => {
                return storage_bucket_internals::sync_context_blocks_for_object(
                    shard, shard_id, key,
                )
            }
            Self::One(counts) => counts,
            Self::Many(counts) => &counts[index],
        };
        let after = lengths(shard, key);
        let outcomes = block_in_wal::outcomes_for_key_since(outcome_start, key);
        let mut handled = false;
        for (slot, kind) in KINDS.iter().enumerate() {
            if before[slot] == 0 && after[slot] == 0 {
                continue;
            }
            handled = true;
            let items: Vec<_> = outcomes
                .iter()
                .filter(|item| item.kind == *kind && !item.meta)
                .collect();
            let mut point_keys = Vec::with_capacity(items.len());
            let mut additive = !items.is_empty();
            for item in &items {
                if item.deleted || item.address.is_none() {
                    additive = false;
                    continue;
                }
                let name = item.element_name.as_deref();
                let point = if *kind == "context_event" {
                    name.filter(|name| name.len() == 32)
                        .and_then(|name| name.get(16..))
                } else {
                    name
                };
                match point {
                    Some(point) => point_keys.push(point),
                    None => additive = false,
                }
            }
            point_keys.sort_unstable();
            point_keys.dedup();
            // Equal growth proves every distinct outcome added a point: replacements or
            // removals cannot take this path. Packed frames may serve several new points.
            additive &= before[slot].checked_add(point_keys.len()) == Some(after[slot]);
            let live = if additive {
                items
                    .into_iter()
                    .filter_map(|item| item.resolved_address())
                    .collect()
            } else {
                addresses(shard, kind, key)
            };
            #[cfg(test)]
            WORK.with(|work| {
                let (published, visited) = work.get();
                work.set((published + live.len(), visited));
            });
            storage_bucket_internals::sync_bucket_index_object_blocks_with_mode(
                shard, shard_id, kind, key, live, true, !additive,
            );
        }
        if handled {
            true
        } else {
            storage_bucket_internals::sync_context_blocks_for_object(shard, shard_id, key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timestamped_series::TimestampedSeriesWrite;

    fn outcome(key: &str, id: u64, address: Option<ElementEntry>) {
        block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
            kind: "context_event".into(),
            object_key: key.into(),
            element_name: Some(format!("{:016x}{id:016x}", id * 1024)),
            object_id: stable_block_object_id(1, "context_event", key),
            routing_bucket: 0,
            deleted: address.is_none(),
            address,
            value: None,
            ttl: None,
            meta: false,
        });
    }

    fn held(shard: &ShardState) -> std::collections::HashSet<LiveBlockKey> {
        storage_bucket_internals::collect_bucket_index_live_block_entries(shard)
            .iter()
            .filter(|entry| !entry.deleted)
            .map(|entry| live_block_key(&entry.address))
            .collect()
    }

    #[test]
    fn context_series_index_keeps_packed_aliases_until_their_last_point_leaves() {
        let key = "ctx:event:7:42";
        let keys = vec![key.to_string()];
        let mut shard = ShardState::default();
        shard.set_routing_range(0, 1023);
        shard.set_shard_id(1);
        let old = ElementEntry::from_parts(1, 0, 10, Some(0), Some(0));
        let appended = ElementEntry::from_parts(1, 10, 10, Some(1), Some(1));
        let replacement = ElementEntry::from_parts(2, 0, 10, Some(2), Some(2));
        let mut series = crate::engine::object_record::ObjectSlot::default();
        series.set(1, old.clone());
        series.set(2, old.clone());
        shard.context_events_mut().insert(key.into(), series);
        storage_bucket_internals::sync_bucket_index_object_blocks(
            &mut shard,
            1,
            "context_event",
            key,
            vec![old.clone()],
            true,
        );
        let before = ContextSeriesBefore::capture(&shard, &keys);
        block_in_wal::begin_write();
        shard
            .context_events_mut()
            .get_mut(key)
            .unwrap()
            .set(3, appended.clone());
        outcome(key, 3, Some(appended.clone()));
        take_work();
        assert!(before.sync(&mut shard, 1, key, 0, 0));
        assert_eq!(take_work(), (1, 0));
        assert_eq!(held(&shard).len(), 2);

        let before = ContextSeriesBefore::capture(&shard, &keys);
        block_in_wal::begin_write();
        shard
            .context_events_mut()
            .get_mut(key)
            .unwrap()
            .set(1, replacement.clone());
        outcome(key, 1, Some(replacement.clone()));
        assert!(before.sync(&mut shard, 1, key, 0, 0));
        assert_eq!(
            held(&shard),
            [
                live_block_key(&old),
                live_block_key(&appended),
                live_block_key(&replacement)
            ]
            .into()
        );

        let before = ContextSeriesBefore::capture(&shard, &keys);
        block_in_wal::begin_write();
        shard.context_events_mut().get_mut(key).unwrap().clear(2);
        outcome(key, 2, None);
        assert!(before.sync(&mut shard, 1, key, 0, 0));
        assert_eq!(
            held(&shard),
            [live_block_key(&appended), live_block_key(&replacement)].into()
        );

        let before = ContextSeriesBefore::capture(&shard, &keys);
        block_in_wal::begin_write();
        shard.context_events_mut().get_mut(key).unwrap().clear(1);
        shard.context_events_mut().get_mut(key).unwrap().clear(3);
        outcome(key, 1, None);
        outcome(key, 3, None);
        assert!(before.sync(&mut shard, 1, key, 0, 0));
        assert!(held(&shard).is_empty());
        block_in_wal::begin_write();
    }
}
