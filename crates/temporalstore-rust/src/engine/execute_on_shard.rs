//! Standalone per-shard command execution extracted from engine.rs.
use super::*;

pub(crate) fn execute_on_shard(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    feature_max_size: usize,
    async_storage: bool,
    shard_id: ShardId,
    start_routing_slot: u32,
    end_routing_slot: u32,
    shard: &mut ShardState,
    command: Command,
) -> ExecuteOutcome {
    let mut mutated = false;
    let response = match command {
        Command::CommonDelete { key } => {
            mutated = delete_record(shard, &key);
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonExpire { key, ttl_ms } => {
            let expires_at = now_ms().saturating_add(ttl_ms);
            for record_key in associated_record_keys(&key) {
                if record_exists_exact(shard, &record_key) {
                    shard.expires_at_ms.insert(record_key, expires_at);
                }
            }
            mutated = true;
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonTtl { key } => {
            let expired = shard
                .expires_at_ms
                .get(&key)
                .map(|expires_at| *expires_at <= now_ms())
                .unwrap_or(false);
            let value = ttl_ms(shard, &key);
            mutated = expired;
            CommandResponse::Integer { value }
        }
        Command::CommonExists { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                invalidate_record_all(cache, shard_id, &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: if record_exists(shard, &key) { 1 } else { 0 },
            }
        }
        Command::StringSet { key, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "string", &key, None);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                shard.strings.insert(key.clone(), address);
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetEx { key, value, ttl_ms } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "string", &key, None);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                shard.strings.insert(key.clone(), address);
                shard
                    .expires_at_ms
                    .insert(key.clone(), now_ms().saturating_add(ttl_ms));
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetConditional {
            key,
            value,
            ttl_ms,
            condition,
            return_old,
        } => {
            remove_if_expired(shard, &key);
            let old_value = shard
                .strings
                .get(&key)
                .and_then(|address| read_page_bytes(cache, page_store, shard_id, address));
            let exists = old_value.is_some();
            let should_set = match condition {
                StringSetCondition::Always => true,
                StringSetCondition::IfExists => exists,
                StringSetCondition::IfNotExists => !exists,
            };
            if should_set {
                let object_id = stable_page_object_id(shard_id, "string", &key, None);
                let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    upsert_slot_index_page(
                        shard,
                        shard_id,
                        "string",
                        &key,
                        None,
                        address.clone(),
                        true,
                    );
                    shard.strings.insert(key.clone(), address);
                    if let Some(ttl_ms) = ttl_ms {
                        shard
                            .expires_at_ms
                            .insert(key.clone(), now_ms().saturating_add(ttl_ms));
                    } else {
                        shard.expires_at_ms.remove(&key);
                    }
                    mutated = true;
                }
                invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            }
            if return_old {
                CommandResponse::Bytes { value: old_value }
            } else {
                CommandResponse::Integer {
                    value: if mutated { 1 } else { 0 },
                }
            }
        }
        Command::StringGet { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::string(shard_id, &key), || {
                CommandResponse::Bytes {
                    value: read_slot_index_value(
                        cache, page_store, shard_id, shard, "string", &key, None,
                    ),
                }
            })
        }
        Command::StringDelete { key } => {
            mutated |= mark_slot_index_object_deleted(shard, &key);
            mutated |= shard.strings.remove(&key).is_some();
            let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
            CommandResponse::Empty
        }
        Command::HashSet { key, field, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "hash",
                    &key,
                    Some(field.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), address);
                mutated = true;
            }
            invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::HashGet { key, field } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::hash(shard_id, &key, &field), || {
                CommandResponse::Bytes {
                    value: read_slot_index_value(
                        cache,
                        page_store,
                        shard_id,
                        shard,
                        "hash",
                        &key,
                        Some(field.as_str()),
                    ),
                }
            })
        }
        Command::HashMultiGet { key, fields } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Values {
                        values: vec![None; fields.len()],
                    },
                    mutated,
                };
            }
            let hash_fields = shard.hashes.get(&key);
            let values = fields
                .iter()
                .map(|field| {
                    hash_fields
                        .and_then(|entries| entries.get(field))
                        .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
                })
                .collect();
            CommandResponse::Values { values }
        }
        Command::HashMultiSet { key, entries } => {
            remove_if_expired(shard, &key);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let mut applied = Vec::with_capacity(entries.len());
            for (field, value) in entries {
                let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    upsert_slot_index_page(
                        shard,
                        shard_id,
                        "hash",
                        &key,
                        Some(field.clone()),
                        address.clone(),
                        true,
                    );
                    invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                    applied.push((field, address));
                }
            }
            if !applied.is_empty() {
                let fields = shard.hashes.entry(key).or_default();
                for (field, address) in applied {
                    fields.insert(field, address);
                }
                mutated = true;
            }
            CommandResponse::Empty
        }
        Command::HashIncrBy {
            key,
            field,
            increment,
        } => {
            remove_if_expired(shard, &key);
            let current = read_slot_index_value(
                cache,
                page_store,
                shard_id,
                shard,
                "hash",
                &key,
                Some(field.as_str()),
            )
            .and_then(|bytes| parse_i64(&bytes))
            .unwrap_or_default();
            let value = current.saturating_add(increment);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                value.to_string().as_bytes(),
                Some(stable_page_object_id(shard_id, "hash", &key, Some(&field))),
                Some(page_routing_slot(
                    &key,
                    start_routing_slot,
                    end_routing_slot,
                )),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "hash",
                    &key,
                    Some(field.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), address);
                invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                mutated = true;
            }
            CommandResponse::Integer { value }
        }
        Command::HashGetAll { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let entries = slot_index_component_page_addresses(shard, "hash", &key)
                .into_iter()
                .filter_map(|(field, address)| {
                    read_page_bytes(cache, page_store, shard_id, &address)
                        .map(|value| (field.unwrap_or_default(), value))
                })
                .collect();
            CommandResponse::HashEntries { entries }
        }
        Command::HashLen { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: slot_index_component_page_addresses(shard, "hash", &key).len() as i64,
            }
        }
        Command::HashDelete { key, field } => {
            mutated |= mark_slot_index_page_deleted(shard, "hash", &key, Some(field.as_str()));
            if let Some(fields) = shard.hashes.get_mut(&key) {
                mutated |= fields.remove(&field).is_some();
            }
            invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::SetAdd { key, member } => {
            remove_if_expired(shard, &key);
            let member_component = hex::encode(&member);
            let object_id = stable_page_object_id(shard_id, "set", &key, Some(&member_component));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &member,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "set",
                    &key,
                    Some(member_component.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .sets
                    .entry(key.clone())
                    .or_default()
                    .insert(member.clone(), address);
                mutated = true;
            }
            let _ = cache.invalidate_record(shard_id, "set", &key);
            CommandResponse::Empty
        }
        Command::SetMembers { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "set", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::set_members(shard_id, &key), || {
                let members = slot_index_component_page_addresses(shard, "set", &key)
                    .into_iter()
                    .filter_map(|(_, address)| {
                        read_page_bytes(cache, page_store, shard_id, &address)
                    })
                    .collect();
                CommandResponse::Members { members }
            })
        }
        Command::SetRemove { key, member } => {
            let member_component = hex::encode(&member);
            mutated |= mark_slot_index_page_deleted(shard, "set", &key, Some(&member_component));
            if let Some(set) = shard.sets.get_mut(&key) {
                mutated |= set.remove(&member).is_some();
            }
            let _ = cache.invalidate_record(shard_id, "set", &key);
            CommandResponse::Empty
        }
        Command::FeatureAppend { key, points } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = sorted_feature_points(points);
            // feature_append_chunks_and_persists_timestamped_kv_pages: append each
            // timestamped feature point through the page-backed KV layout, then
            // publish the resulting page addresses into the slot index below.
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                } else {
                    break;
                }
            }
            let live_addresses = series.values().cloned().collect::<Vec<_>>();
            sync_slot_index_object_pages(shard, shard_id, "feature", &key, live_addresses, mutated);
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureAppendWithPolicy {
            key,
            points,
            policy,
        } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let mut accepted_points = Vec::new();
            let mut accepted_timestamps = BTreeSet::new();
            for point in sorted_feature_points(points) {
                let exists = series.contains_key(&point.timestamp_ms)
                    || accepted_timestamps.contains(&point.timestamp_ms);
                let should_write = match policy {
                    FeatureWritePolicy::Upsert => true,
                    FeatureWritePolicy::InsertIfAbsent => !exists,
                    FeatureWritePolicy::ReplaceExisting => exists,
                    FeatureWritePolicy::Block => false,
                };
                if should_write {
                    accepted_timestamps.insert(point.timestamp_ms);
                    accepted_points.push(point);
                }
            }
            if !accepted_points.is_empty() {
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "feature",
                    &key,
                    accepted_points,
                    routing_slot,
                    async_storage,
                ) {
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                    mutated = true;
                } else {
                    break;
                }
            }
            let live_addresses = series.values().cloned().collect::<Vec<_>>();
            sync_slot_index_object_pages(shard, shard_id, "feature", &key, live_addresses, mutated);
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::FeatureQuery {
            key,
            start_ms,
            end_ms,
            count,
        } => cached_response(
            cache,
            CacheKey::feature_query(shard_id, &key, start_ms, end_ms, count),
            || {
                let points = shard
                    .features
                    .get(&key)
                    .map(|series| {
                        let mut page_cache = HashMap::new();
                        // feature_append_keeps_oversized_single_timestamped_value_readable:
                        // range queries rehydrate each timestamp through the packed
                        // page reader, so a large single timestamped value remains
                        // readable when it occupies its own page.
                        series
                            .range(start_ms..=end_ms)
                            .take(count.unwrap_or(5000))
                            .filter_map(|(timestamp_ms, address)| {
                                read_feature_point_cached(
                                    cache,
                                    page_store,
                                    shard_id,
                                    *timestamp_ms,
                                    address,
                                    &mut page_cache,
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                CommandResponse::FeaturePoints { points }
            },
        ),
        Command::FeatureQueryFiltered {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            let limit = count.unwrap_or(feature_max_size).min(feature_max_size);
            let points = shard
                .features
                .get(&key)
                .map(|series| {
                    let mut page_cache = HashMap::new();
                    series
                        .range(start_ms..=end_ms)
                        .take(limit)
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point_cached(
                                cache,
                                page_store,
                                shard_id,
                                *timestamp_ms,
                                address,
                                &mut page_cache,
                            )
                            .and_then(|point| {
                                let row = SequenceFeatureRow::decode_cpp_feature_value(
                                    point.timestamp_ms,
                                    &point.value,
                                )?;
                                filters
                                    .iter()
                                    .all(|filter| sequence_filter_matches(&row, filter))
                                    .then_some(point)
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::FeatureReplace {
            key,
            start_ms,
            end_ms,
            points,
        } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let replaced = series
                .range(start_ms..=end_ms)
                .map(|(timestamp_ms, _)| *timestamp_ms)
                .collect::<Vec<_>>();
            for timestamp_ms in replaced {
                series.remove(&timestamp_ms);
                mutated = true;
            }
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                    mutated = true;
                } else {
                    break;
                }
            }
            let live_addresses = series.values().cloned().collect::<Vec<_>>();
            sync_slot_index_object_pages(shard, shard_id, "feature", &key, live_addresses, mutated);
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureDelete { key } => {
            mutated = shard.features.remove(&key).is_some();
            mutated |= mark_slot_index_object_deleted(shard, &key);
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureAggQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Aggregate { value: 0 },
                    mutated,
                };
            }
            let values = shard
                .features
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count.unwrap_or(5000))
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                                .map(|point| point.value)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            CommandResponse::Aggregate {
                value: aggregate_feature_values(&values, &aggregator),
            }
        }
        Command::SequenceAdd { key, rows } => {
            remove_if_expired(shard, &key);
            let series = shard.sequences.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = rows
                .into_iter()
                .filter_map(|row| {
                    serde_json::to_vec(&row).ok().map(|value| FeaturePoint {
                        timestamp_ms: row.timestamp_ms,
                        value,
                    })
                })
                .collect::<Vec<_>>();
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "sequence",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                } else {
                    break;
                }
            }
            let live_addresses = series.values().cloned().collect::<Vec<_>>();
            sync_slot_index_object_pages(
                shard,
                shard_id,
                "sequence",
                &key,
                live_addresses,
                mutated,
            );
            CommandResponse::Empty
        }
        Command::SequenceQuery {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::SequenceRows { rows: Vec::new() },
                    mutated,
                };
            }
            let rows = shard
                .sequences
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count)
                        .filter_map(|(timestamp_ms, address)| {
                            read_sequence_row(cache, page_store, shard_id, *timestamp_ms, address)
                        })
                        .filter(|row| {
                            filters
                                .iter()
                                .all(|filter| sequence_filter_matches(row, filter))
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::SequenceRows { rows }
        }
        Command::SequenceBatchQuery { queries } => {
            let groups = queries
                .into_iter()
                .map(
                    |SequenceQuerySpec {
                         key,
                         start_ms,
                         end_ms,
                         count,
                         filters,
                     }| {
                        if remove_if_expired(shard, &key) {
                            mutated = true;
                            return (key, Vec::new());
                        }
                        let rows = sequence_rows_in_range(
                            cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                            &filters,
                        );
                        (key, rows)
                    },
                )
                .collect();
            CommandResponse::SequenceRowGroups { groups }
        }
        Command::IpsAdd {
            key,
            timestamp_ms,
            instance,
        } => {
            remove_if_expired(shard, &key);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "ips",
                &key,
                vec![FeaturePoint {
                    timestamp_ms,
                    value: instance,
                }],
                routing_slot,
                async_storage,
            ) {
                let address = addresses
                    .into_iter()
                    .find_map(|(timestamp, address)| (timestamp == timestamp_ms).then_some(address))
                    .expect("single IPS timestamped page ref should exist");
                shard
                    .ips
                    .entry(key.clone())
                    .or_default()
                    .insert(timestamp_ms, address.clone());
                shard.ips_meta.entry(key.clone()).or_default().insert(
                    timestamp_ms,
                    IpsPointMeta {
                        address,
                        action_type: None,
                        table_id: None,
                        request_id: None,
                    },
                );
                mutated = true;
            }
            let live_addresses = shard
                .ips
                .get(&key)
                .map(|series| series.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            sync_slot_index_object_pages(shard, shard_id, "ips", &key, live_addresses, mutated);
            CommandResponse::Empty
        }
        Command::IpsAddWithOptions {
            key,
            timestamp_ms,
            instance,
            action_type,
            table_id,
            request_id,
        } => {
            remove_if_expired(shard, &key);
            if let Some(request_id) = &request_id {
                if shard
                    .ips_request_ids
                    .get(&key)
                    .is_some_and(|ids| ids.contains(request_id))
                {
                    return ExecuteOutcome {
                        response: CommandResponse::Integer { value: 0 },
                        mutated: false,
                    };
                }
            }
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "ips",
                &key,
                vec![FeaturePoint {
                    timestamp_ms,
                    value: instance,
                }],
                routing_slot,
                async_storage,
            ) {
                let address = addresses
                    .into_iter()
                    .find_map(|(timestamp, address)| (timestamp == timestamp_ms).then_some(address))
                    .expect("single IPS timestamped page ref should exist");
                shard
                    .ips
                    .entry(key.clone())
                    .or_default()
                    .insert(timestamp_ms, address.clone());
                shard.ips_meta.entry(key.clone()).or_default().insert(
                    timestamp_ms,
                    IpsPointMeta {
                        address,
                        action_type,
                        table_id,
                        request_id: request_id.clone(),
                    },
                );
                if let Some(request_id) = request_id {
                    shard
                        .ips_request_ids
                        .entry(key.clone())
                        .or_default()
                        .insert(request_id);
                }
                mutated = true;
            }
            let live_addresses = shard
                .ips
                .get(&key)
                .map(|series| series.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            sync_slot_index_object_pages(shard, shard_id, "ips", &key, live_addresses, mutated);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsLoad { key, points } => {
            remove_if_expired(shard, &key);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = sorted_feature_points(points);
            let mut loaded = 0i64;
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "ips",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    shard
                        .ips
                        .entry(key.clone())
                        .or_default()
                        .insert(timestamp_ms, address.clone());
                    shard.ips_meta.entry(key.clone()).or_default().insert(
                        timestamp_ms,
                        IpsPointMeta {
                            address,
                            action_type: None,
                            table_id: None,
                            request_id: None,
                        },
                    );
                    mutated = true;
                    loaded += 1;
                }
            }
            let live_addresses = shard
                .ips
                .get(&key)
                .map(|series| series.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            sync_slot_index_object_pages(shard, shard_id, "ips", &key, live_addresses, mutated);
            CommandResponse::Integer { value: loaded }
        }
        Command::IpsQueryLast { key, count } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let points = shard
                .ips
                .get(&key)
                .map(|series| {
                    series
                        .iter()
                        .rev()
                        .take(count)
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::IpsQueryRange {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range(
                    cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsBatchQueryLast { keys, count } => {
            let groups = keys
                .into_iter()
                .map(|key| {
                    if remove_if_expired(shard, &key) {
                        mutated = true;
                        return (key, Vec::new());
                    }
                    let points = shard
                        .ips
                        .get(&key)
                        .map(|series| {
                            series
                                .iter()
                                .rev()
                                .take(count)
                                .filter_map(|(timestamp_ms, address)| {
                                    read_feature_point(
                                        cache,
                                        page_store,
                                        shard_id,
                                        *timestamp_ms,
                                        address,
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    (key, points)
                })
                .collect();
            CommandResponse::FeaturePointGroups { groups }
        }
        Command::IpsRemove { key, timestamp_ms } => {
            if let Some(series) = shard.ips.get_mut(&key) {
                mutated = series.remove(&timestamp_ms).is_some();
                if series.is_empty() {
                    shard.ips.remove(&key);
                }
            }
            if let Some(series) = shard.ips_meta.get_mut(&key) {
                if let Some(meta) = series.remove(&timestamp_ms) {
                    if let Some(request_id) = meta.request_id {
                        if let Some(ids) = shard.ips_request_ids.get_mut(&key) {
                            ids.remove(&request_id);
                        }
                    }
                }
                if series.is_empty() {
                    shard.ips_meta.remove(&key);
                }
            }
            let live_addresses = shard
                .ips
                .get(&key)
                .map(|series| series.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            sync_slot_index_object_pages(shard, shard_id, "ips", &key, live_addresses, mutated);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsDelete { key } => {
            mutated = shard.ips.remove(&key).is_some();
            mutated |= shard.ips_meta.remove(&key).is_some();
            shard.ips_request_ids.remove(&key);
            mutated |= mark_slot_index_object_deleted(shard, &key);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsCount {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let value = shard
                .ips
                .get(&key)
                .map(|series| series.range(start_ms..=end_ms).count() as i64)
                .unwrap_or_default();
            CommandResponse::Integer { value }
        }
        Command::IpsQueryRangeWithOptions {
            key,
            start_ms,
            end_ms,
            count,
            action_type,
            table_id,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range_with_options(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    &key,
                    start_ms,
                    end_ms,
                    count,
                    action_type,
                    table_id,
                ),
            }
        }
        Command::IpsSnapshot {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range(
                    cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsSnapshotReport {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::IpsSnapshotReport {
                        report: empty_ips_snapshot_report(key, start_ms, end_ms, count),
                    },
                    mutated,
                };
            }
            CommandResponse::IpsSnapshotReport {
                report: ips_snapshot_report_in_range(
                    cache, page_store, shard_id, shard, key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsStat {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::IpsStats {
                        stats: IpsStats {
                            total: 0,
                            first_timestamp_ms: None,
                            last_timestamp_ms: None,
                            action_type_counts: Vec::new(),
                            table_id_counts: Vec::new(),
                        },
                    },
                    mutated,
                };
            }
            CommandResponse::IpsStats {
                stats: ips_stats_in_range(shard, &key, start_ms, end_ms),
            }
        }
        Command::IpsFilter {
            key,
            start_ms,
            end_ms,
            count,
            action_type,
            table_id,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range_with_options(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    &key,
                    start_ms,
                    end_ms,
                    count,
                    action_type,
                    table_id,
                ),
            }
        }
        Command::RiskIncrement {
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            *shard
                .risk
                .entry(key.clone())
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskIncrementWithOptions {
            key,
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        } => {
            remove_if_expired(shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            *shard
                .risk
                .entry(key.clone())
                .or_default()
                .entry(bucket_ms)
                .or_default() += amount;
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key.clone(), now_ms().saturating_add(ttl_ms));
            }
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskChangeAdd {
            key,
            timestamp_ms,
            value,
            precision_ms,
            ttl_ms,
        } => {
            remove_if_expired(shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            shard
                .risk_changes
                .entry(key.clone())
                .or_default()
                .entry(bucket_ms)
                .or_default()
                .insert(value);
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskCount {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let value = shard
                .risk
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .map(|(_, value)| *value)
                        .sum()
                })
                .unwrap_or_default();
            CommandResponse::Integer { value }
        }
        Command::RiskQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            if is_risk_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_risk_changes(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .risk
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_risk_values(&values, &aggregator),
                }
            }
        }
        Command::RiskDetail {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let points = shard
                .risk
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count.unwrap_or(usize::MAX))
                        .map(|(timestamp_ms, amount)| FeaturePoint {
                            timestamp_ms: *timestamp_ms,
                            value: amount.to_string().into_bytes(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::RiskSet {
            family,
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            let key = risk_family_key(family, &key);
            *shard
                .risk
                .entry(key.clone())
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskSetAndGet {
            family,
            key,
            timestamp_ms,
            amount,
            start_ms,
            end_ms,
            aggregator,
        } => {
            remove_if_expired(shard, &key);
            let key = risk_family_key(family, &key);
            let series = shard.risk.entry(key.clone()).or_default();
            *series.entry(timestamp_ms).or_default() += amount;
            let values = series
                .range(start_ms..=end_ms)
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Integer {
                value: aggregate_risk_values(&values, &aggregator),
            }
        }
        Command::RiskFamilyQuery {
            family,
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let key = risk_family_key(family, &key);
            if is_risk_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_risk_changes(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .risk
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_risk_values(&values, &aggregator),
                }
            }
        }
        Command::RiskFolSet {
            key,
            value,
            occur_time_ms,
            ttl_ms,
            fol_type,
        } => {
            remove_if_expired(shard, &key);
            let should_store = shard
                .risk_fol
                .get(&key)
                .map(|existing| match fol_type {
                    RiskFolType::First => occur_time_ms < existing.occur_time_ms,
                    RiskFolType::Last => occur_time_ms > existing.occur_time_ms,
                })
                .unwrap_or(true);
            if should_store {
                shard.risk_fol.insert(
                    key.clone(),
                    RiskFolValue {
                        occur_time_ms,
                        value,
                        fol_type,
                    },
                );
            }
            if ttl_ms > 0 {
                shard
                    .expires_at_ms
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskFolQuery { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard.risk_fol.get(&key).map(|stored| stored.value.clone()),
            }
        }
        Command::RiskManager { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut entries = Vec::new();
            for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
                let family_key = risk_family_key(family, &key);
                let values = shard
                    .risk
                    .get(&family_key)
                    .map(|series| series.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                entries.push((
                    format!("{}_events", risk_family_name(family)),
                    values.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{}_sum", risk_family_name(family)),
                    values.iter().sum::<i64>().to_string().into_bytes(),
                ));
            }
            if let Some(fol) = shard.risk_fol.get(&key) {
                entries.push(("fol_value".to_string(), fol.value.clone()));
                entries.push((
                    "fol_occur_time_ms".to_string(),
                    fol.occur_time_ms.to_string().into_bytes(),
                ));
                entries.push((
                    "fol_type".to_string(),
                    match fol.fol_type {
                        RiskFolType::First => b"first".to_vec(),
                        RiskFolType::Last => b"last".to_vec(),
                    },
                ));
            }
            CommandResponse::HashEntries { entries }
        }
        Command::RiskDebug {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut entries = Vec::new();
            entries.push(("key".to_string(), key.as_bytes().to_vec()));
            entries.push(("start_ms".to_string(), start_ms.to_string().into_bytes()));
            entries.push(("end_ms".to_string(), end_ms.to_string().into_bytes()));
            for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
                let family_key = risk_family_key(family, &key);
                let name = risk_family_name(family);
                let series = shard.risk.get(&family_key);
                let all_values = series
                    .map(|series| series.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                let window = series
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(timestamp_ms, value)| (*timestamp_ms, *value))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                entries.push((
                    format!("{name}_events"),
                    all_values.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_sum"),
                    all_values.iter().sum::<i64>().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_events"),
                    window.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_sum"),
                    window
                        .iter()
                        .map(|(_, value)| *value)
                        .sum::<i64>()
                        .to_string()
                        .into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_first_timestamp_ms"),
                    window
                        .first()
                        .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_last_timestamp_ms"),
                    window
                        .last()
                        .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                ));
            }
            if let Some(fol) = shard.risk_fol.get(&key) {
                entries.push(("fol_value".to_string(), fol.value.clone()));
                entries.push((
                    "fol_occur_time_ms".to_string(),
                    fol.occur_time_ms.to_string().into_bytes(),
                ));
                entries.push((
                    "fol_type".to_string(),
                    match fol.fol_type {
                        RiskFolType::First => b"first".to_vec(),
                        RiskFolType::Last => b"last".to_vec(),
                    },
                ));
            }
            CommandResponse::HashEntries { entries }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            let object_key = context_node_key(tenant_hash, node.node_hash);
            let object_id =
                stable_page_object_id(shard_id, "hash", &object_key, Some(CONTEXT_NODE_FIELD));
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            let bytes = context_bytes(&node);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &bytes,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard
                    .hashes
                    .entry(object_key.clone())
                    .or_default()
                    .insert(CONTEXT_NODE_FIELD.to_string(), address);
                mutated = true;
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => {
            let object_key = context_node_key(tenant_hash, node_hash);
            let node = shard
                .hashes
                .get(&object_key)
                .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                .or_else(|| shard.context_nodes.get(&object_key))
                .and_then(|address| {
                    read_page_bytes(cache, page_store, shard_id, address)
                        .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                });
            CommandResponse::ContextNode { object_key, node }
        }
        Command::ContextGetNodes {
            tenant_hash,
            node_hashes,
        } => {
            let nodes = dedupe_nonzero_u64_preserve_order(node_hashes)
                .into_iter()
                .filter_map(|node_hash| {
                    let object_key = context_node_key(tenant_hash, node_hash);
                    shard
                        .hashes
                        .get(&object_key)
                        .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                        .or_else(|| shard.context_nodes.get(&object_key))
                        .and_then(|address| {
                            read_page_bytes(cache, page_store, shard_id, address)
                                .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                        })
                })
                .collect();
            CommandResponse::ContextNodes { nodes }
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            mut event,
            first_write_only,
            cold_storage,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            normalize_context_event_storage_keys(node_hash, &mut event);
            // CONTEXT_TIMELINE_FANOUT is applied inside context_timeline_key so
            // multiple ContextEvent writes at the same millisecond map to stable,
            // timestamp-keyed pages instead of overwriting one another.
            // context_models_match_cpp_keys_timeline_pages_and_filters keeps this
            // key shape aligned with the C++ context event timeline contract.
            let timeline_key = context_timeline_key(event.primary_time_ms(), event.event_id_hash);
            let series = shard.context_events.entry(object_key.clone()).or_default();
            if !(first_write_only && series.contains_key(&timeline_key)) {
                let value = context_bytes(&event);
                let routing_slot =
                    page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_event",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value,
                    }],
                    routing_slot,
                    async_storage && !cold_storage,
                ) {
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            if maybe_auto_compress_context_node(
                cache,
                page_store,
                shard_id,
                shard,
                tenant_hash,
                node_hash,
                &object_key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            ) {
                mutated = true;
            }
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            mut event,
            indexes,
            first_write_only,
            cold_storage,
        } => {
            let event_object_key = context_event_key(tenant_hash, node_hash);
            normalize_context_event_storage_keys(node_hash, &mut event);
            let primary_time_ms = event.primary_time_ms();
            // Extracted events use the same CONTEXT_TIMELINE_FANOUT timeline as
            // raw context events so index refs, filters, and event pages share the
            // C++ wire-compatible timestamp key discipline.
            let event_timeline_key = context_timeline_key(primary_time_ms, event.event_id_hash);
            let event_series = shard
                .context_events
                .entry(event_object_key.clone())
                .or_default();
            if !(first_write_only && event_series.contains_key(&event_timeline_key)) {
                let value = context_bytes(&event);
                let routing_slot =
                    page_routing_slot(&event_object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_event",
                    &event_object_key,
                    vec![FeaturePoint {
                        timestamp_ms: event_timeline_key,
                        value,
                    }],
                    routing_slot,
                    async_storage && !cold_storage,
                ) {
                    for (timestamp_ms, address) in addresses {
                        event_series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            invalidate_record_all(cache, shard_id, &event_object_key);

            let index_ref = ContextIndexRef {
                primary_node_hash: node_hash,
                primary_event_time_ms: primary_time_ms,
                event_id_hash: event.event_id_hash,
            };
            let mut index_object_keys = Vec::new();
            let mut write_default_index =
                |index_name: &str, value_hash: u64, index_time_ms: u64| {
                    if value_hash == 0 || index_time_ms == 0 {
                        return;
                    }
                    let object_key =
                        context_index_key(tenant_hash, index_name, value_hash, indexes.scope_hash);
                    let timeline_key = context_timeline_key(index_time_ms, index_ref.event_id_hash);
                    let value = context_bytes(&index_ref);
                    let routing_slot =
                        page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                    if let Ok(addresses) = append_timestamped_kv_pages(
                        cache,
                        page_store,
                        shard_id,
                        "context_index",
                        &object_key,
                        vec![FeaturePoint {
                            timestamp_ms: timeline_key,
                            value,
                        }],
                        routing_slot,
                        async_storage,
                    ) {
                        let series = shard.context_indexes.entry(object_key.clone()).or_default();
                        for (timestamp_ms, address) in addresses {
                            series.insert(timestamp_ms, address);
                            mutated = true;
                        }
                        invalidate_record_all(cache, shard_id, &object_key);
                        index_object_keys.push(object_key);
                    }
                };

            if !context_index_disabled(&indexes, InternalContextIndex::EventKind) {
                write_default_index(
                    "event_kind",
                    context_event_kind_hash(&event),
                    primary_time_ms,
                );
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Status) {
                write_default_index("status", indexes.status_hash, primary_time_ms);
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Source) {
                write_default_index("source", indexes.source_hash, primary_time_ms);
            }
            if !context_index_disabled(&indexes, InternalContextIndex::EventTimeBucket) {
                write_default_index(
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.event_time_bucket_ms,
                );
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Entity) {
                for entity_hash in &indexes.entity_hashes {
                    write_default_index("entity", *entity_hash, primary_time_ms);
                }
            }
            CommandResponse::ContextExtractedEventWrite {
                event_object_key,
                written_index_count: index_object_keys.len(),
                index_object_keys,
            }
        }
        Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            current_valid_only,
            as_of_ms,
            kinds,
            statuses,
            min_confidence,
            min_importance,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            let events = shard
                .context_events
                .get(&object_key)
                .map(|series| {
                    let mut page_cache = HashMap::new();
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value_cached::<ContextEvent>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                                &mut page_cache,
                            )
                        })
                        .filter(|event| {
                            context_event_matches_filter(
                                event,
                                current_valid_only,
                                as_of_ms,
                                end_time_ms,
                                &kinds,
                                &statuses,
                                min_confidence,
                                min_importance,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextEvents { object_key, events }
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            event_time_ms,
            index_ref,
        } => {
            let object_key =
                context_index_key(tenant_hash, &index_name, index_value_hash, scope_hash);
            let timeline_key = context_timeline_key(event_time_ms, index_ref.event_id_hash);
            let value = context_bytes(&index_ref);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_index",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value,
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard.context_indexes.entry(object_key.clone()).or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let object_key =
                context_index_key(tenant_hash, &index_name, index_value_hash, scope_hash);
            let refs = shard
                .context_indexes
                .get(&object_key)
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextIndexRef>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextIndexRefs { object_key, refs }
        }
        Command::ContextQueryIndexIntersection {
            tenant_hash,
            predicates,
            limit,
        } => {
            let mut scanned_ref_count = 0usize;
            let mut deduped_ref_count = 0usize;
            let mut candidate_refs: Option<HashMap<(u64, u64, u64), ContextIndexRef>> = None;

            for predicate in &predicates {
                let object_key = context_index_key(
                    tenant_hash,
                    &predicate.index_name,
                    predicate.index_value_hash,
                    predicate.scope_hash,
                );
                let mut seen_for_predicate = HashMap::new();
                if let Some(series) = shard.context_indexes.get(&object_key) {
                    let mut page_cache = HashMap::new();
                    for (timeline_key, address) in series.range(
                        context_timeline_start(predicate.start_time_ms)
                            ..context_timeline_end(predicate.end_time_ms),
                    ) {
                        if let Some(index_ref) = read_context_value_cached::<ContextIndexRef>(
                            cache,
                            page_store,
                            shard_id,
                            *timeline_key,
                            address,
                            &mut page_cache,
                        ) {
                            scanned_ref_count += 1;
                            let key = context_index_ref_identity(&index_ref);
                            if seen_for_predicate.insert(key, index_ref).is_some() {
                                deduped_ref_count += 1;
                            }
                        }
                    }
                }

                candidate_refs = match candidate_refs {
                    None => Some(seen_for_predicate),
                    Some(mut existing) => {
                        existing.retain(|key, _| seen_for_predicate.contains_key(key));
                        Some(existing)
                    }
                };
                if candidate_refs.as_ref().is_some_and(HashMap::is_empty) {
                    break;
                }
            }

            let mut refs: Vec<_> = candidate_refs.unwrap_or_default().into_values().collect();
            refs.sort_by_key(|index_ref| {
                (
                    index_ref.primary_event_time_ms,
                    index_ref.event_id_hash,
                    index_ref.primary_node_hash,
                )
            });
            refs.truncate(context_limit(limit));
            CommandResponse::ContextIndexIntersection {
                refs,
                matched_index_count: predicates.len(),
                scanned_ref_count,
                deduped_ref_count,
            }
        }
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            let object_key = context_audit_key(tenant_hash, audit.session_hash);
            let timeline_key =
                context_timeline_key(audit.request_time_ms, stable_object_hash(&audit.query_id));
            let value = context_bytes(&audit);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_audit",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value,
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard.context_audits.entry(object_key.clone()).or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let object_key = context_audit_key(tenant_hash, session_hash);
            let audits = shard
                .context_audits
                .get(&object_key)
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextPackAudit>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextPackAudits { object_key, audits }
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => {
            // In-memory, coalesced summary-dirty tracking. Repeated marks for the same
            // node update a single hashmap entry instead of appending a new persisted
            // page, so dirty records are bounded by distinct dirty nodes, not by events.
            // The map is ephemeral (never written to the page store) and may be lost on
            // restart; the async summary worker re-marks on the next event.
            let object_key = context_dirty_key(tenant_hash, marker.node_hash);
            let entry = shard.context_dirty_index.entry(object_key.clone()).or_default();
            if entry.mark_count == 0 {
                entry.node_hash = marker.node_hash;
                entry.first_event_time_ms = marker.event_time_ms;
                entry.last_event_time_ms = marker.event_time_ms;
            } else {
                entry.first_event_time_ms = entry.first_event_time_ms.min(marker.event_time_ms);
                entry.last_event_time_ms = entry.last_event_time_ms.max(marker.event_time_ms);
            }
            entry.reason = marker.reason;
            entry.propagate_depth = entry.propagate_depth.max(marker.propagate_depth);
            entry.mark_count = entry.mark_count.saturating_add(1);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            // Read the single coalesced in-memory dirty entry for this node and surface it
            // as one marker when it overlaps the requested time window. `limit` is retained
            // for API compatibility but there is at most one coalesced marker per node.
            let _ = limit;
            let object_key = context_dirty_key(tenant_hash, node_hash);
            let markers = shard
                .context_dirty_index
                .get(&object_key)
                .filter(|entry| {
                    entry.mark_count > 0
                        && entry.last_event_time_ms >= start_time_ms
                        && entry.first_event_time_ms <= end_time_ms
                })
                .map(|entry| {
                    vec![ContextSummaryDirtyMarker {
                        node_hash: entry.node_hash,
                        event_time_ms: entry.last_event_time_ms,
                        reason: entry.reason,
                        propagate_depth: entry.propagate_depth,
                    }]
                })
                .unwrap_or_default();
            CommandResponse::ContextSummaryDirtyMarkers {
                object_key,
                markers,
            }
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            let object_key = context_entity_key(tenant_hash, entity.node_hash, entity.entity_hash);
            let object_id = stable_page_object_id(shard_id, "context_entity", &object_key, None);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            let bytes = context_bytes(&entity);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &bytes,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard.context_entities.insert(object_key.clone(), address);
                mutated = true;
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            let object_key = context_entity_key(tenant_hash, node_hash, entity_hash);
            let entity = shard.context_entities.get(&object_key).and_then(|address| {
                read_page_bytes(cache, page_store, shard_id, address)
                    .and_then(|bytes| context_from_bytes::<ContextEntity>(&bytes))
            });
            CommandResponse::ContextEntity { object_key, entity }
        }
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            entity_hashes,
            limit,
        } => {
            let object_key = context_entity_collection_key(tenant_hash, node_hash);
            let entities = dedupe_nonzero_u64_preserve_order(entity_hashes)
                .into_iter()
                .take(context_limit(limit))
                .filter_map(|entity_hash| {
                    let entity_key = context_entity_key(tenant_hash, node_hash, entity_hash);
                    shard.context_entities.get(&entity_key).and_then(|address| {
                        read_page_bytes(cache, page_store, shard_id, address)
                            .and_then(|bytes| context_from_bytes::<ContextEntity>(&bytes))
                    })
                })
                .collect();
            CommandResponse::ContextEntities {
                object_key,
                entities,
            }
        }
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => {
            let object_key = context_child_key(tenant_hash, child_ref.parent_hash);
            let existing = load_context_children(cache, page_store, shard_id, shard, &object_key);
            let created = existing
                .iter()
                .all(|stored| stored.child_hash != child_ref.child_hash);
            if created {
                let timeline_key =
                    context_timeline_key(child_ref.updated_at_ms, child_ref.child_hash);
                let routing_slot =
                    page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_child",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value: context_bytes(&child_ref),
                    }],
                    routing_slot,
                    async_storage,
                ) {
                    let series = shard
                        .context_children
                        .entry(object_key.clone())
                        .or_default();
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextChildRefs {
                object_key,
                refs: Vec::new(),
                created: Some(created),
            }
        }
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            limit,
        } => {
            let object_key = context_child_key(tenant_hash, parent_hash);
            let mut refs = load_context_children(cache, page_store, shard_id, shard, &object_key);
            refs.sort_by_key(|child_ref| (child_ref.updated_at_ms, child_ref.child_hash));
            refs.truncate(context_limit(limit));
            CommandResponse::ContextChildRefs {
                object_key,
                refs,
                created: None,
            }
        }
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => {
            let object_key = context_embedding_key(tenant_hash, embedding.ref_hash);
            let object_id = stable_page_object_id(shard_id, "context_embedding", &object_key, None);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &context_bytes(&embedding),
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard.context_embeddings.insert(object_key.clone(), address);
                mutated = true;
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryEmbeddings {
            tenant_hash,
            ref_hashes,
            limit,
        } => {
            let embeddings = dedupe_nonzero_u64_preserve_order(ref_hashes)
                .into_iter()
                .take(context_limit(limit))
                .filter_map(|ref_hash| {
                    let object_key = context_embedding_key(tenant_hash, ref_hash);
                    shard
                        .context_embeddings
                        .get(&object_key)
                        .and_then(|address| {
                            read_page_bytes(cache, page_store, shard_id, address)
                                .and_then(|bytes| context_from_bytes::<ContextEmbedding>(&bytes))
                        })
                })
                .collect();
            CommandResponse::ContextEmbeddings { embeddings }
        }
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            query_vector,
            max_depth,
            top_k_per_depth,
            max_children_scored_per_parent,
            max_candidate_nodes,
            leaf_only,
        } => {
            let nodes = traverse_context_tree(
                cache,
                page_store,
                shard_id,
                shard,
                tenant_hash,
                start_node_hash,
                &query_vector,
                max_depth,
                top_k_per_depth,
                max_children_scored_per_parent,
                max_candidate_nodes,
                leaf_only,
            );
            CommandResponse::ContextTraversedNodes { nodes }
        }
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => {
            let object_key = context_summary_key(tenant_hash, summary.node_hash, summary.level);
            let timeline_key =
                context_timeline_key(summary.valid_from_ms, u64::from(summary.level));
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_summary",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value: context_bytes(&summary),
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard
                    .context_summaries
                    .entry(object_key.clone())
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            as_of_ms,
            limit,
        } => {
            let object_key = context_summary_key(tenant_hash, node_hash, level);
            let mut summaries = load_context_summaries(
                cache,
                page_store,
                shard_id,
                shard,
                &object_key,
                as_of_ms,
                limit,
            );
            summaries.sort_by_key(|summary| summary.valid_from_ms);
            CommandResponse::ContextSummaries {
                object_key,
                summaries,
            }
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            let object_key = context_compression_key(tenant_hash, event.node_hash);
            let timeline_key =
                context_timeline_key(event.compressed_time_ms, event.compression_id_hash);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_compression",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value: context_bytes(&event),
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard
                    .context_compressions
                    .entry(object_key.clone())
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let mut events = load_context_compression_events(
                cache,
                page_store,
                shard_id,
                shard,
                tenant_hash,
                &node_hashes,
                start_time_ms,
                end_time_ms,
                limit,
            );
            let object_key = node_hashes
                .iter()
                .find(|node_hash| **node_hash != 0)
                .map(|node_hash| context_compression_key(tenant_hash, *node_hash))
                .unwrap_or_else(|| context_compression_key(tenant_hash, 0));
            CommandResponse::ContextCompressionEvents {
                object_key,
                events: {
                    events.truncate(context_limit(limit));
                    events
                },
                source_event_count: None,
                truncated_source_events: None,
            }
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            source_start_ms,
            source_end_ms,
            compressed_time_ms,
            max_source_events,
            min_confidence,
            min_importance,
        } => {
            let object_key = context_compression_key(tenant_hash, node_hash);
            let source_limit = context_limit(max_source_events);
            let mut selected = shard
                .context_events
                .get(&context_event_key(tenant_hash, node_hash))
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(source_start_ms)
                                ..context_timeline_end(source_end_ms),
                        )
                        .filter_map(|(timeline_key, address)| {
                            read_context_value_cold::<ContextEvent>(
                                page_store,
                                *timeline_key,
                                address,
                            )
                        })
                        .filter(|event| {
                            event.confidence >= min_confidence && event.importance >= min_importance
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            selected.sort_by_key(|event| (event.event_time_ms, event.event_id_hash));
            let truncated = selected.len() > source_limit;
            selected.truncate(source_limit);
            if selected.is_empty() {
                CommandResponse::ContextCompressionEvents {
                    object_key,
                    events: Vec::new(),
                    source_event_count: Some(0),
                    truncated_source_events: Some(false),
                }
            } else {
                let event = build_context_compression_event(
                    tenant_hash,
                    node_hash,
                    source_start_ms,
                    source_end_ms,
                    compressed_time_ms,
                    &selected,
                    truncated,
                );
                let timeline_key =
                    context_timeline_key(event.compressed_time_ms, event.compression_id_hash);
                let routing_slot =
                    page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_compression",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value: context_bytes(&event),
                    }],
                    routing_slot,
                    async_storage,
                ) {
                    let series = shard
                        .context_compressions
                        .entry(object_key.clone())
                        .or_default();
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
                invalidate_record_all(cache, shard_id, &object_key);
                CommandResponse::ContextCompressionEvents {
                    object_key,
                    events: vec![event],
                    source_event_count: Some(selected.len() as u32),
                    truncated_source_events: Some(truncated),
                }
            }
        }
        Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            summary_level,
            as_of_ms,
            cold_start_time_ms,
            cold_end_time_ms,
            compression_limit,
        } => {
            let node_key = context_node_key(tenant_hash, node_hash);
            let node = shard
                .hashes
                .get(&node_key)
                .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                .or_else(|| shard.context_nodes.get(&node_key))
                .and_then(|address| {
                    read_page_bytes(cache, page_store, shard_id, address)
                        .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                });
            let level = summary_level.unwrap_or(1).max(1);
            let summary_key = context_summary_key(tenant_hash, node_hash, level);
            let latest_summary = load_latest_context_summary(
                cache,
                page_store,
                shard_id,
                shard,
                &summary_key,
                as_of_ms,
            );
            let cold_window_summaries = if cold_start_time_ms == 0 && cold_end_time_ms == 0 {
                Vec::new()
            } else {
                load_context_compression_events(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    &[node_hash],
                    cold_start_time_ms,
                    cold_end_time_ms,
                    compression_limit,
                )
            };
            CommandResponse::ContextNodeContext {
                node_exists: node.is_some(),
                node,
                overall_summary_exists: latest_summary.is_some(),
                overall_summary: latest_summary,
                cold_window_summaries,
            }
        }
    };
    ExecuteOutcome { response, mutated }
}
