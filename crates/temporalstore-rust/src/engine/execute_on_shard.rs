// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Standalone per-shard command execution extracted from engine.rs.
use super::*;

/// Put aside an outcome that no page backs: a deletion, a deadline, or state that lives only
/// in the index snapshot.
///
/// The page path stages its outcome inside `upsert_bucket_index_page`, because that is where a
/// page outcome is produced. These have no such moment, so they say it here.
#[allow(clippy::too_many_arguments)]
/// Record a page or a value that belongs to one COMPONENT of an object.
///
/// `stage_meta_outcome` states something about the object as a whole -- it is gone, its deadline
/// is this. This states what one part of it became: which page backs it, or the bytes for state no
/// page backs at all.
#[allow(clippy::too_many_arguments)]
fn stage_component_outcome(
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    routing_bucket: u32,
    address: Option<crate::block_store::BlockAddress>,
    value: Option<Vec<u8>>,
) {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: object_key.to_string(),
        component: component.clone(),
        object_id: stable_page_object_id(shard_id, kind, object_key, component.as_deref()),
        routing_bucket,
        address,
        value,
        ttl: None,
        deleted: false,
        meta: false,
    });
}

fn stage_meta_outcome(
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    value: Option<Vec<u8>>,
    ttl: Option<u64>,
    deleted: bool,
) {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: object_key.to_string(),
        component: None,
        object_id: stable_page_object_id(shard_id, kind, object_key, None),
        routing_bucket: page_routing_bucket(object_key, start_routing_bucket, end_routing_bucket),
        address: None,
        value,
        ttl,
        deleted,
        meta: true,
    });
}

/// Read a node record back, by the same two lookups every node reader here uses.
fn load_context_node(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
) -> Option<ContextNode> {
    shard
        .hashes
        .get(object_key)
        .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
        .or_else(|| shard.context_nodes.get(object_key))
        .and_then(|address| {
            // Owning, not shared: the summary upsert calls this while writing, where the page
            // is as likely to be a miss as a hit -- and on a miss the shared read wraps an owned
            // buffer in a fresh Arc, which copies a second time. The query commands, which are
            // hit-heavy, do share.
            read_page_bytes(cache, page_store, shard_id, address)
                .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
        })
}

/// Persist a node record: the one producer of a node page.
///
/// Three commands now write a node -- the upsert, the embedding attach, and the summary upsert
/// keeping the node's copy of its summary vector current -- and each needs the page appended,
/// the outcome staged, the hash field pointed at the new address and the record invalidated, in
/// that order. Held apart, the third copy is where one of those four steps goes missing.
///
/// Returns whether a page was written, which is what the caller records as `mutated`.
#[allow(clippy::too_many_arguments)]
fn write_context_node(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    object_key: &str,
    node: &ContextNode,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let object_id = stable_page_object_id(shard_id, "hash", object_key, Some(CONTEXT_NODE_FIELD));
    let routing_bucket = page_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    let mut wrote = false;
    if let Ok(address) = append_value(
        cache,
        page_store,
        shard_id,
        &context_bytes(node),
        Some(object_id),
        Some(routing_bucket),
        async_storage,
    ) {
        // Its own kind, deliberately: this writes a hash page and -- unlike HashSet --
        // never registers it in the bucket index, so recording it as a "hash" would have
        // a rebuild add an entry the write never made.
        stage_component_outcome(
            shard_id,
            "context_node",
            object_key,
            Some(CONTEXT_NODE_FIELD.to_string()),
            routing_bucket,
            Some(address.clone()),
            None,
        );
        shard
            .hashes
            .entry(object_key.to_string())
            .or_default()
            .insert(CONTEXT_NODE_FIELD.to_string(), address);
        wrote = true;
    }
    invalidate_context_record(cache, shard_id, object_key);
    wrote
}

pub(crate) fn execute_on_shard(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    feature_max_size: usize,
    async_storage: bool,
    control_rollup_enabled: bool,
    control_coalesce_persist: bool,
    control_distinct_sketch: bool,
    shard_id: ShardId,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    shard: &mut ShardState,
    command: Command,
) -> ExecuteOutcome {
    // Publish per-execute hints so the write helpers don't need the flags threaded through
    // every control-state write arm.
    shard.control_coalesce_persist = control_coalesce_persist;
    shard.control_distinct_sketch = control_distinct_sketch;
    let mut mutated = false;
    let response = match command {
        // Applied as nothing: `mutated` stays false, so it neither dirties a shard nor
        // invalidates a cache entry.
        Command::LeaderEstablish => CommandResponse::Empty,
        // Blob commands are dispatched before the shard lock (they live beside the engine,
        // not in shard record state); reaching this arm means the early dispatch was skipped.
        Command::ContextResourceBlobBegin { .. }
        | Command::ContextResourceBlobAppend { .. }
        | Command::ContextResourceBlobCommit { .. }
        | Command::ContextResourceBlobPut { .. }
        | Command::ContextResourceBlobFetch { .. }
        | Command::ContextResourceBlobSweep { .. } => CommandResponse::Empty,
        Command::CommonDelete { key } => {
            // An outcome with no page: the object is gone. Their log item states the same thing
            // with `object_deleted`, and replay applies it without re-running a delete.
            stage_meta_outcome(
                shard_id,
                "object",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            mutated = delete_record(shard, &key);
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonExpire { key, ttl_ms } => {
            let expires_at = resolve_now_ms().saturating_add(ttl_ms);
            for record_key in associated_record_keys(&key) {
                if record_exists_exact(shard, &record_key) {
                    shard.expires_at_ms.insert(record_key, expires_at);
                }
            }
            mutated = true;
            // The deadline is recorded ALREADY RESOLVED. That is the point: a replay applying
            // this outcome needs no clock of its own, where re-running the command would resolve
            // the TTL against the restart clock and extend every recently-expiring key.
            stage_meta_outcome(
                shard_id,
                "object",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                Some(expires_at),
                false,
            );
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonPersist { key } => {
            // Expired-but-unswept is "missing" here, exactly as reads treat it: removing the
            // sweep's pending work must not resurrect a value whose deadline already passed.
            if remove_if_expired(shard, &key) {
                mutated = true;
                invalidate_record_all(cache, shard_id, &key);
                CommandResponse::Integer { value: 0 }
            } else {
                let mut removed = false;
                for record_key in associated_record_keys(&key) {
                    if shard.expires_at_ms.remove(&record_key).is_some()
                        && record_exists_exact(shard, &record_key)
                    {
                        removed = true;
                    }
                }
                if removed {
                    mutated = true;
                    stage_meta_outcome(
                        shard_id,
                        "object",
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                        None,
                        None,
                        false,
                    );
                    invalidate_record_all(cache, shard_id, &key);
                }
                CommandResponse::Integer {
                    value: i64::from(removed),
                }
            }
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
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_page(
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
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                shard.strings.insert(key.clone(), address);
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                shard.expires_at_ms.insert(key.clone(), expires_at);
                // This write sets a value AND a deadline. Recording only the page passes a probe
                // that asks whether the record said anything, and produces a recovered key that
                // never expires -- so the deadline is recorded too, already resolved, exactly as
                // CommonExpire records its own.
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
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
                let routing_bucket =
                    page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_bucket),
                    async_storage,
                ) {
                    upsert_bucket_index_page(
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
                        let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                        shard.expires_at_ms.insert(key.clone(), expires_at);
                        // A conditional write that refreshes a deadline records the refreshed
                        // one. Recording only the page leaves a replay installing the value over
                        // a LAPSED deadline from an earlier record, and the key reads as expired
                        // even though the leader kept it alive.
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            Some(expires_at),
                            false,
                        );
                    } else {
                        shard.expires_at_ms.remove(&key);
                        // No deadline is equally a result. An object outcome carrying neither a
                        // deadline nor a deletion says exactly that.
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            None,
                            false,
                        );
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
                    value: read_bucket_index_value(
                        cache, page_store, shard_id, shard, "string", &key, None,
                    ),
                }
            })
        }
        Command::StringDelete { key } => {
            stage_meta_outcome(
                shard_id,
                "string",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            mutated |= mark_bucket_index_object_deleted(shard, &key);
            mutated |= shard.strings.remove(&key).is_some();
            let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
            CommandResponse::Empty
        }
        Command::HashSet { key, field, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_page(
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
                    value: read_bucket_index_value(
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
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            let mut applied = Vec::with_capacity(entries.len());
            for (field, value) in entries {
                let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_bucket),
                    async_storage,
                ) {
                    upsert_bucket_index_page(
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
            let current = read_bucket_index_value(
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
                Some(page_routing_bucket(
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                )),
                async_storage,
            ) {
                upsert_bucket_index_page(
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
            let entries = bucket_index_component_page_addresses(shard, "hash", &key)
                .into_iter()
                .filter_map(|(field, address)| {
                    read_page_bytes(cache, page_store, shard_id, &address).map(|value| {
                        (
                            field.map(|name| name.to_string()).unwrap_or_default(),
                            value,
                        )
                    })
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
                value: bucket_index_component_page_addresses(shard, "hash", &key).len() as i64,
            }
        }
        Command::HashDelete { key, field } => {
            mutated |=
                mark_bucket_index_page_deleted(shard, shard_id, "hash", &key, Some(field.as_str()));
            if let Some(fields) = shard.hashes.get_mut(&key) {
                mutated |= fields.remove(&field).is_some();
                // Mirror hash2::Del: deleting the last field removes the whole key
                // (DeleteObject on empty). Leaving an empty field map behind makes the key
                // still report as existing (EXISTS=1, TYPE=hash) -- a phantom hash. (Sets do
                // NOT do this on either side, so only Hash needs the cleanup.)
                if fields.is_empty() {
                    shard.hashes.remove(&key);
                }
            }
            invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::SetAdd { key, member } => {
            remove_if_expired(shard, &key);
            let member_component = hex::encode(&member);
            let object_id = stable_page_object_id(shard_id, "set", &key, Some(&member_component));
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &member,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_page(
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
            let _ = cache.invalidate(&CacheKey::set_members(shard_id, &key));
            CommandResponse::Empty
        }
        Command::ZSetAdd { key, member, score } => {
            remove_if_expired(shard, &key);
            let biased = zset_score_bits(score);
            let existed = shard
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(old_biased, _)| *old_biased);
            if existed == Some(biased) {
                // Same score: nothing to rewrite, and the answer is still "not new".
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            if let Some(old_biased) = existed {
                let old_component = zset_component(old_biased, &member);
                mark_bucket_index_page_deleted(shard, shard_id, "zset", &key, Some(&old_component));
            }
            let component = zset_component(biased, &member);
            let object_id = stable_page_object_id(shard_id, "zset", &key, Some(&component));
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &member,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "zset",
                    &key,
                    Some(component.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .zsets
                    .entry(key.clone())
                    .or_default()
                    .insert(member.clone(), (biased, address));
                mutated = true;
            }
            CommandResponse::Integer {
                value: i64::from(existed.is_none()),
            }
        }
        Command::ZSetScore { key, member } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard
                    .zsets
                    .get(&key)
                    .and_then(|members| members.get(&member))
                    .map(|(biased, _)| zset_score_string(*biased).into_bytes()),
            }
        }
        Command::ZSetRemove { key, member } => {
            let removed = shard
                .zsets
                .get_mut(&key)
                .and_then(|members| members.remove(&member));
            match removed {
                None => CommandResponse::Integer { value: 0 },
                Some((biased, _)) => {
                    mutated = true;
                    let component = zset_component(biased, &member);
                    mark_bucket_index_page_deleted(shard, shard_id, "zset", &key, Some(&component));
                    if shard.zsets.get(&key).is_some_and(BTreeMap::is_empty) {
                        shard.zsets.remove(&key);
                    }
                    CommandResponse::Integer { value: 1 }
                }
            }
        }
        Command::ZSetCard { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard.zsets.get(&key).map_or(0, BTreeMap::len) as i64,
            }
        }
        Command::ZSetRange {
            key,
            start,
            stop,
            rev,
        } => {
            remove_if_expired(shard, &key);
            let mut ordered = zset_ordered_members(shard, &key);
            if rev {
                ordered.reverse();
            }
            let length = ordered.len() as i64;
            let resolve = |index: i64| if index < 0 { length + index } else { index };
            let from = resolve(start).max(0);
            let to = resolve(stop).min(length - 1);
            let members = if from > to || length == 0 {
                Vec::new()
            } else {
                ordered[from as usize..=to as usize]
                    .iter()
                    .flat_map(|(member, biased)| {
                        [member.clone(), zset_score_string(*biased).into_bytes()]
                    })
                    .collect()
            };
            CommandResponse::Members { members }
        }
        Command::ZSetRangeByScore {
            key,
            min,
            max,
            min_exclusive,
            max_exclusive,
            rev,
        } => {
            remove_if_expired(shard, &key);
            let min_bits = zset_score_bits(min);
            let max_bits = zset_score_bits(max);
            // The score test runs before the member bytes are copied, so a narrow window copies
            // a narrow window rather than the whole set.
            let mut ordered: Vec<(Vec<u8>, u64)> = zset_members_in_score_range(
                shard,
                &key,
                min_bits,
                max_bits,
                min_exclusive,
                max_exclusive,
            );
            if rev {
                ordered.reverse();
            }
            let members = ordered
                .into_iter()
                .flat_map(|(member, biased)| [member, zset_score_string(biased).into_bytes()])
                .collect();
            CommandResponse::Members { members }
        }
        Command::SeenCheck {
            key,
            member,
            window_ms,
        } => {
            let now = resolve_now_ms();
            let floor = now.saturating_sub(window_ms);
            // No page backs a seen-set; it lives in the index snapshot. So the outcome carries
            // the member and the moment, which is everything an apply needs.
            stage_meta_outcome(
                shard_id,
                "seen",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(member.clone()),
                Some(now),
                false,
            );
            let seen = shard.seen.entry(key).or_default();
            // Bounded sweep from the time-ordered front: enough to keep pace with any
            // sustained rate, never enough to stall a hot call on a huge backlog.
            for _ in 0..128 {
                match seen.by_time.first_key_value() {
                    Some(((seen_at, _), ())) if *seen_at < floor => {
                        let ((seen_at, expired), ()) =
                            seen.by_time.pop_first().expect("front exists");
                        if seen.by_member.get(&expired) == Some(&seen_at) {
                            seen.by_member.remove(&expired);
                        }
                    }
                    _ => break,
                }
            }
            let duplicate = seen
                .by_member
                .get(&member)
                .is_some_and(|seen_at| *seen_at >= floor);
            if !duplicate {
                if let Some(previous) = seen.by_member.insert(member.clone(), now) {
                    seen.by_time.remove(&(previous, member.clone()));
                }
                seen.by_time.insert((now, member), ());
            }
            mutated = true;
            CommandResponse::Integer {
                value: i64::from(duplicate),
            }
        }
        Command::SeenCard { key } => CommandResponse::Integer {
            value: shard.seen.get(&key).map_or(0, |seen| seen.by_member.len()) as i64,
        },
        Command::BucketTake {
            key,
            tokens,
            capacity,
            refill_per_sec,
        } => {
            let now = resolve_now_ms();
            let current = shard.buckets.get(&key).copied();
            let (allowed, remaining, retry_after_ms, next) =
                bucket_take(current, now, tokens, capacity, refill_per_sec);
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
            bucket_state.extend_from_slice(&next.1.to_le_bytes());
            stage_meta_outcome(
                shard_id,
                "bucket",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(bucket_state),
                None,
                false,
            );
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
            bucket_state.extend_from_slice(&next.1.to_le_bytes());
            stage_meta_outcome(
                shard_id,
                "bucket",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(bucket_state),
                None,
                false,
            );
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
            bucket_state.extend_from_slice(&next.1.to_le_bytes());
            stage_meta_outcome(
                shard_id,
                "bucket",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(bucket_state),
                None,
                false,
            );
            shard.buckets.insert(key, next);
            mutated = true;
            CommandResponse::Members {
                members: bucket_answer(allowed, remaining, retry_after_ms),
            }
        }
        Command::BucketPeek {
            key,
            tokens,
            capacity,
            refill_per_sec,
        } => {
            let now = resolve_now_ms();
            let current = shard.buckets.get(&key).copied();
            let (allowed, remaining, retry_after_ms, _) =
                bucket_take(current, now, tokens, capacity, refill_per_sec);
            CommandResponse::Members {
                members: bucket_answer(allowed, remaining, retry_after_ms),
            }
        }
        Command::ZSetIncrBy {
            key,
            member,
            increment,
        } => {
            remove_if_expired(shard, &key);
            let old = shard
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(biased, _)| *biased);
            let score = old.map_or(0.0, zset_score_from_bits) + increment;
            let biased = zset_score_bits(score);
            if let Some(old_biased) = old {
                let old_component = zset_component(old_biased, &member);
                mark_bucket_index_page_deleted(shard, shard_id, "zset", &key, Some(&old_component));
            }
            let component = zset_component(biased, &member);
            let object_id = stable_page_object_id(shard_id, "zset", &key, Some(&component));
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &member,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                shard
                    .zsets
                    .entry(key.clone())
                    .or_default()
                    .insert(member.clone(), (biased, address.clone()));
                upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "zset",
                    &key,
                    Some(component),
                    address,
                    true,
                );
                mutated = true;
            }
            CommandResponse::Bytes {
                value: Some(zset_score_string(biased).into_bytes()),
            }
        }
        Command::ZSetPop { key, min, count } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut ordered = zset_ordered_members(shard, &key);
            if !min {
                ordered.reverse();
            }
            ordered.truncate(count as usize);
            let mut members = Vec::new();
            for (member, biased) in ordered {
                let component = zset_component(biased, &member);
                mark_bucket_index_page_deleted(shard, shard_id, "zset", &key, Some(&component));
                if let Some(entries) = shard.zsets.get_mut(&key) {
                    entries.remove(&member);
                }
                mutated = true;
                members.push(member);
                members.push(zset_score_string(biased).into_bytes());
            }
            if shard.zsets.get(&key).is_some_and(BTreeMap::is_empty) {
                shard.zsets.remove(&key);
            }
            if mutated {
            }
            CommandResponse::Members { members }
        }
        Command::ZSetRank { key, member, rev } => {
            remove_if_expired(shard, &key);
            let target = shard
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(biased, _)| (*biased, member.clone()));
            CommandResponse::Bytes {
                value: target.map(|(biased, member)| {
                    let before = shard
                        .zsets
                        .get(&key)
                        .map(|members| {
                            members
                                .iter()
                                .filter(|(other, (other_biased, _))| {
                                    // Compare the parts directly. Building two owned tuples here
                                    // copied both member names on every member examined -- exactly
                                    // two allocations per member, for a comparison that needs none.
                                    let ordering = other_biased
                                        .cmp(&biased)
                                        .then_with(|| (*other).as_slice().cmp(member.as_slice()));
                                    if rev {
                                        ordering == std::cmp::Ordering::Greater
                                    } else {
                                        ordering == std::cmp::Ordering::Less
                                    }
                                })
                                .count()
                        })
                        .unwrap_or(0);
                    before.to_string().into_bytes()
                }),
            }
        }
        Command::ListPush { key, member, left } => {
            remove_if_expired(shard, &key);
            let seq = {
                let list = shard.lists.entry(key.clone()).or_default();
                if left {
                    list.keys().next().copied().map_or(0, |first| first - 1)
                } else {
                    list.keys().next_back().copied().map_or(0, |last| last + 1)
                }
            };
            // Two's-complement bias makes the hex component sort lexically in list order,
            // which is what lets recovery and range reads walk the bucket index directly.
            let component = format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64));
            let object_id = stable_page_object_id(shard_id, "list", &key, Some(&component));
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &member,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_page(
                    shard,
                    shard_id,
                    "list",
                    &key,
                    Some(component.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .lists
                    .entry(key.clone())
                    .or_default()
                    .insert(seq, address);
                mutated = true;
            }
            let length = shard.lists.get(&key).map_or(0, BTreeMap::len) as i64;
            CommandResponse::Integer { value: length }
        }
        Command::ListPop { key, left } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            let popped = shard.lists.get_mut(&key).and_then(|list| {
                let seq = if left {
                    list.keys().next().copied()
                } else {
                    list.keys().next_back().copied()
                }?;
                list.remove(&seq).map(|address| (seq, address))
            });
            match popped {
                None => CommandResponse::Bytes { value: None },
                Some((seq, address)) => {
                    let component = format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64));
                    mutated = true;
                    mark_bucket_index_page_deleted(shard, shard_id, "list", &key, Some(&component));
                    if shard.lists.get(&key).is_some_and(BTreeMap::is_empty) {
                        shard.lists.remove(&key);
                    }
                    CommandResponse::Bytes {
                        value: read_page_bytes(cache, page_store, shard_id, &address),
                    }
                }
            }
        }
        Command::ListRange { key, start, stop } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            // The length comes from the map itself -- `ListLen` below takes the same number the
            // same way. Copying every address to obtain it, and to index a slice of it, meant a
            // ten-entry page of a four-thousand-entry list copied four thousand addresses.
            let list = shard.lists.get(&key);
            let length = list.map_or(0, |list| list.len()) as i64;
            let resolve = |index: i64| -> i64 {
                if index < 0 {
                    length + index
                } else {
                    index
                }
            };
            let from = resolve(start).max(0);
            let to = resolve(stop).min(length - 1);
            let members = if from > to || length == 0 {
                Vec::new()
            } else {
                // A BTreeMap iterates in key order, which is the list's order -- the same order the
                // materialised Vec had. Only the requested span is READ: `read_page_bytes` runs
                // `wanted` times, not `length` times, which is what stopped a ten-entry page of a
                // four-thousand-entry list from touching four thousand pages.
                //
                // `skip(from)` still ADVANCES the iterator `from` times, so reaching a late offset
                // walks the nodes before it -- cheap per step and allocating nothing, but not
                // free. A read near the head is O(wanted); one near the tail is O(from + wanted).
                // An allocation probe cannot see that difference, so it is stated here rather
                // than left for a flat byte measurement to imply otherwise.
                let wanted = (to - from + 1) as usize;
                list.map(|list| {
                    list.values()
                        .skip(from as usize)
                        .take(wanted)
                        .filter_map(|address| {
                            read_page_bytes(cache, page_store, shard_id, address)
                        })
                        .collect()
                })
                .unwrap_or_default()
            };
            CommandResponse::Members { members }
        }
        Command::ListLen { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard.lists.get(&key).map_or(0, BTreeMap::len) as i64,
            }
        }
        Command::SetMembers { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::set_members(shard_id, &key));
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::set_members(shard_id, &key), || {
                let members = bucket_index_component_page_addresses(shard, "set", &key)
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
            mutated |= mark_bucket_index_page_deleted(
                shard,
                shard_id,
                "set",
                &key,
                Some(&member_component),
            );
            if let Some(set) = shard.sets.get_mut(&key) {
                mutated |= set.remove(&member).is_some();
            }
            let _ = cache.invalidate(&CacheKey::set_members(shard_id, &key));
            CommandResponse::Empty
        }
        Command::FeatureAppend { key, points } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            let points = sorted_feature_points(points);
            let mut published: Vec<BlockAddress> = Vec::new();
            let mut replaced_any = false;
            // feature_append_chunks_and_persists_timestamped_kv_pages: append each
            // timestamped feature point through the page-backed KV layout, then
            // publish the resulting page addresses into the bucket index below.
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_bucket,
                async_storage,
                true,
            ) {
                for (timestamp_ms, address) in addresses {
                    // A replaced timestamp supersedes a page, and a superseded page must be
                    // dropped rather than joined -- so record it and take the restating path.
                    if series.insert(timestamp_ms, address.clone()).is_some() {
                        replaced_any = true;
                    }
                    published.push(address);
                    mutated = true;
                }
            }
            let trimmed = trim_timestamped_series(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                feature_max_size,
            );
            mutated |= trimmed;
            if replaced_any || trimmed {
                // Something was superseded or evicted: the live set has to be restated in full.
                let live_addresses = series.values().cloned().collect::<Vec<_>>();
                sync_bucket_index_object_pages(
                    shard,
                    shard_id,
                    "feature",
                    &key,
                    live_addresses,
                    mutated,
                );
            } else {
                // A pure append. Publishing only the new pages leaves the index in the same
                // state, and stops an append costing the length of the series it joins:
                // restating every address measured 4.07 MB for one point on a 3,200-point series.
                sync_bucket_index_object_pages_with_mode(
                    shard,
                    shard_id,
                    "feature",
                    &key,
                    published,
                    mutated,
                    false,
                );
            }
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
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            let mut accepted_points = Vec::new();
            let mut accepted_timestamps = BTreeSet::new();
            // Process points in REQUEST order, NOT pre-collapsed by timestamp. feature ADD
            // FIRST policy walks the point list in order and
            // skips any ts already present, so for an in-batch duplicate timestamp the FIRST
            // value wins. Pre-collapsing here via sorted_feature_points (last-wins) would silently
            // keep the LAST duplicate under InsertIfAbsent. accepted_points is sorted+collapsed
            // just before the append below (UPSERT/ReplaceExisting keep last-wins, matching).
            for point in points {
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
                    sorted_feature_points(accepted_points),
                    routing_bucket,
                    async_storage,
                    true,
                ) {
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            mutated |= trim_timestamped_series(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                feature_max_size,
            );
            let live_addresses = series.values().cloned().collect::<Vec<_>>();
            sync_bucket_index_object_pages(
                shard,
                shard_id,
                "feature",
                &key,
                live_addresses,
                mutated,
            );
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
        } => {
            // Apply lazy expiry before serving, matching FeatureAggQuery/SequenceQuery and
            // every sibling read: a key past its deadline but not yet swept must read empty,
            // otherwise FeatureQuery and FeatureAggQuery disagree on the same expired key.
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            cached_response(
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
                                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                                // Default read bound follows feature_max_size so raising the
                                // retention cap (long-sequence use) also lifts the read limit.
                                .take(count.unwrap_or(feature_max_size))
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
            )
        }
        Command::FeatureQueryFiltered {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            // Apply lazy expiry before serving, matching FeatureAggQuery/SequenceQuery and
            // every sibling read: an expired-but-unswept key must read empty so filtered and
            // aggregate reads stay consistent with each other on the same key.
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let limit = count.unwrap_or(feature_max_size).min(feature_max_size);
            let points = shard
                .features
                .get(&key)
                .map(|series| {
                    let mut page_cache = HashMap::new();
                    series
                        .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
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
                                let row = SequenceFeatureRow::decode_feature_proto_value(
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
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            let replaced = series
                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                .map(|(timestamp_ms, _)| *timestamp_ms)
                .collect::<Vec<_>>();
            mutated |= drop_timestamped_points(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                &replaced,
            );
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_bucket,
                async_storage,
                true,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            mutated |= trim_timestamped_series(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                feature_max_size,
            );
            let live_addresses = series.values().cloned().collect::<Vec<_>>();
            sync_bucket_index_object_pages(
                shard,
                shard_id,
                "feature",
                &key,
                live_addresses,
                mutated,
            );
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureDelete { key } => {
            // A removal with no component: the whole series went, not one point.
            stage_meta_outcome(
                shard_id,
                "feature",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            mutated = shard.features.remove(&key).is_some();
            mutated |= mark_bucket_index_object_deleted(shard, &key);
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
            // The additive "sum" aggregate over the full window can be served in O(levels) via
            // the shared rollup ladder. Only "sum" is summable here: for features "count"/"" mean
            // element count (aggregate_feature_values), and min/max/first/last are non-additive, so
            // those stay on the raw path. Gated to no explicit `count` cap so it covers the whole
            // window, matching the raw scan on the retained series.
            let use_rollup = control_rollup_enabled
                && aggregator.trim().eq_ignore_ascii_case("sum")
                && count.is_none();
            if use_rollup {
                // Lazily materialize the numeric view of the feature series. Each point's value
                // is parsed through the SAME aggregate_feature_values path, so the folded values
                // are bit-identical to the raw scan; rebuild when the series length changed.
                let feature_len = shard
                    .features
                    .get(&key)
                    .map(|series| series.len())
                    .unwrap_or(0);
                let stale = shard
                    .feature_values
                    .get(&key)
                    .map(|values| values.len() != feature_len)
                    .unwrap_or(feature_len > 0);
                if stale {
                    let decoded: BTreeMap<u64, i64> = shard
                        .features
                        .get(&key)
                        .map(|series| {
                            series
                                .iter()
                                .filter_map(|(timestamp_ms, address)| {
                                    read_feature_point(
                                        cache,
                                        page_store,
                                        shard_id,
                                        *timestamp_ms,
                                        address,
                                    )
                                    .map(|point| {
                                        (
                                            *timestamp_ms,
                                            aggregate_feature_values(&[point.value], "sum"),
                                        )
                                    })
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    shard.feature_values.insert(key.clone(), decoded);
                    shard.feature_rollups.remove(&key);
                }
                CommandResponse::Aggregate {
                    value: control_rollup::feature_windowed_sum(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .features
                    .get(&key)
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .take(count.unwrap_or(feature_max_size))
                            .filter_map(|(timestamp_ms, address)| {
                                read_feature_point(
                                    cache,
                                    page_store,
                                    shard_id,
                                    *timestamp_ms,
                                    address,
                                )
                                .map(|point| point.value)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Aggregate {
                    value: aggregate_feature_values(&values, &aggregator),
                }
            }
        }
        Command::SequenceAdd { key, rows } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_bucket =
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
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
                "feature",
                &key,
                points,
                routing_bucket,
                async_storage,
                true,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            mutated |= trim_timestamped_series(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                feature_max_size,
            );
            let live_addresses = series.values().cloned().collect::<Vec<_>>();
            sync_bucket_index_object_pages(
                shard,
                shard_id,
                "feature",
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
                .features
                .get(&key)
                .map(|series| {
                    series
                        .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
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
        Command::ControlStateIncrement {
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            let resulting = {
                let counter = shard
                    .control_state
                    .entry(key.clone())
                    .or_default()
                    .entry(timestamp_ms)
                    .or_default();
                *counter += amount;
                *counter
            };
            // The RESULT, not the increment. An increment replayed twice counts twice; a count
            // installed twice is the same count, which is what makes a record idempotent.
            stage_component_outcome(
                shard_id,
                "control_counter",
                &key,
                Some(timestamp_ms.to_string()),
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket),
                None,
                Some(resulting.to_le_bytes().to_vec()),
            );
            persist_control_state_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, timestamp_ms, amount);
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateIncrementWithOptions {
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
                .control_state
                .entry(key.clone())
                .or_default()
                .entry(bucket_ms)
                .or_default() += amount;
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key.clone(), resolve_now_ms().saturating_add(ttl_ms));
            }
            persist_control_state_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, bucket_ms, amount);
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateChangeAdd {
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
            stage_component_outcome(
                shard_id,
                "control_change",
                &key,
                Some(bucket_ms.to_string()),
                page_routing_bucket(&key, start_routing_bucket, end_routing_bucket),
                None,
                Some(value.clone()),
            );
            hll::record_change(shard, &key, bucket_ms, value);
            if let Some(ttl_ms) = ttl_ms {
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                shard.expires_at_ms.insert(key.clone(), expires_at);
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateCount {
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
            let value = if control_rollup_enabled {
                control_rollup::windowed_sum(shard, &key, start_ms, end_ms)
            } else {
                shard
                    .control_state
                    .get(&key)
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(_, value)| *value)
                            .sum()
                    })
                    .unwrap_or_default()
            };
            CommandResponse::Integer { value }
        }
        Command::ControlStateQuery {
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
            if is_control_state_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_control_state_changes(shard, &key, start_ms, end_ms),
                }
            } else if control_rollup_enabled && control_rollup::is_sum_family(&aggregator) {
                CommandResponse::Integer {
                    value: control_rollup::windowed_sum(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .control_state
                    .get(&key)
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_control_state_values(&values, &aggregator),
                }
            }
        }
        Command::ControlStateDetail {
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
                .control_state
                .get(&key)
                .map(|series| {
                    series
                        .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
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
        Command::ControlStateSet {
            family,
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            let key = control_state_family_key(family, &key);
            *shard
                .control_state
                .entry(key.clone())
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
            persist_control_state_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, timestamp_ms, amount);
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateSetAndGet {
            family,
            key,
            timestamp_ms,
            amount,
            start_ms,
            end_ms,
            aggregator,
        } => {
            remove_if_expired(shard, &key);
            let key = control_state_family_key(family, &key);
            let series = shard.control_state.entry(key.clone()).or_default();
            *series.entry(timestamp_ms).or_default() += amount;
            let values = series
                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            persist_control_state_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, timestamp_ms, amount);
            }
            mutated = true;
            CommandResponse::Integer {
                value: aggregate_control_state_values(&values, &aggregator),
            }
        }
        Command::ControlStateSetAndGetWithOptions {
            family,
            key,
            timestamp_ms,
            amount,
            start_ms,
            end_ms,
            aggregator,
            precision_ms,
            ttl_ms,
            uuid,
        } => {
            remove_if_expired(shard, &key);
            let key = control_state_family_key(family, &key);
            // UUID idempotency: dedup at-least-once replays within a bounded window,
            // mirroring the control_state dedup ledger. A duplicate is a no-op
            // write that still returns the current windowed aggregate (idempotent).
            let now = resolve_now_ms();
            let is_duplicate = if let Some(uuid) = uuid.as_ref().filter(|u| !u.is_empty()) {
                let dedup_key = format!("{key}\u{1}{uuid}");
                gc_control_state_uuid(shard, now);
                let dup = matches!(shard.control_state_uuid.get(&dedup_key), Some(expiry) if *expiry > now);
                if !dup {
                    shard
                        .control_state_uuid
                        .insert(dedup_key, now.saturating_add(CONTROL_STATE_UUID_DEDUP_MS));
                }
                dup
            } else {
                false
            };
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            let series = shard.control_state.entry(key.clone()).or_default();
            if !is_duplicate {
                *series.entry(bucket_ms).or_default() += amount;
            }
            let value = if is_control_state_change_aggregator(&aggregator) {
                count_control_state_changes(shard, &key, start_ms, end_ms)
            } else {
                let values = shard
                    .control_state
                    .get(&key)
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                aggregate_control_state_values(&values, &aggregator)
            };
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key.clone(), now.saturating_add(ttl_ms));
            }
            persist_control_state_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            if control_rollup_enabled && !is_duplicate {
                control_rollup::record_increment(shard, &key, bucket_ms, amount);
            }
            mutated = true;
            CommandResponse::Integer { value }
        }
        Command::ControlStateFamilyQuery {
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
            let key = control_state_family_key(family, &key);
            if is_control_state_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_control_state_changes(shard, &key, start_ms, end_ms),
                }
            } else if control_rollup_enabled && control_rollup::is_sum_family(&aggregator) {
                CommandResponse::Integer {
                    value: control_rollup::windowed_sum(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .control_state
                    .get(&key)
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_control_state_values(&values, &aggregator),
                }
            }
        }
        Command::ControlStateSelectionSet {
            key,
            value,
            occur_time_ms,
            ttl_ms,
            selection_type,
        } => {
            remove_if_expired(shard, &key);
            // FirstOrLastSet substitutes occur_time==0 with the current time BEFORE the
            // FIRST/LAST comparison (an occur_time of 0 is replaced with the current time).
            // occur_time defaults to 0 on the proto, so a caller that omits it must compare as
            // "now" -- taking 0 literally made an omitted-time FIRST set always win (0 < any)
            // and an omitted-time LAST set always lose.
            let occur_time_ms = if occur_time_ms == 0 {
                resolve_now_ms()
            } else {
                occur_time_ms
            };
            let should_store = shard
                .control_state_selection
                .get(&key)
                .map(|existing| match selection_type {
                    ControlStateSelectionType::First => occur_time_ms < existing.occur_time_ms,
                    ControlStateSelectionType::Last => occur_time_ms > existing.occur_time_ms,
                })
                .unwrap_or(true);
            if should_store {
                // The value that WON the comparison, so a replay installs the winner instead of
                // re-running a comparison against whatever it happens to hold.
                stage_component_outcome(
                    shard_id,
                    "control_selection",
                    &key,
                    Some(match selection_type {
                        ControlStateSelectionType::First => "first".to_string(),
                        ControlStateSelectionType::Last => "last".to_string(),
                    }),
                    page_routing_bucket(&key, start_routing_bucket, end_routing_bucket),
                    None,
                    Some(value.clone()),
                );
                shard.control_state_selection.insert(
                    key.clone(),
                    ControlStateSelectionValue {
                        occur_time_ms,
                        value,
                        selection_type,
                    },
                );
            }
            if ttl_ms > 0 {
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                shard.expires_at_ms.insert(key.clone(), expires_at);
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
            }
            // A comparison this value LOST stored nothing and set no deadline, so it changed
            // nothing. Reporting it as a mutation dirtied the shard and appended a record for a
            // write that did not happen.
            mutated = should_store || ttl_ms > 0;
            CommandResponse::Empty
        }
        Command::ControlStateSelectionQuery { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard
                    .control_state_selection
                    .get(&key)
                    .map(|stored| stored.value.clone()),
            }
        }
        Command::ControlStateManager {
            key,
            op_type,
            field_list,
            start_offset,
            end_offset,
            is_distinct,
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
            let entries = control_state_manager_entries(
                shard,
                &key,
                op_type.as_deref(),
                &field_list,
                &start_offset,
                &end_offset,
                is_distinct,
            );
            CommandResponse::HashEntries { entries }
        }
        Command::ControlStateDebug {
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
            for family in [
                ControlStateFamily::Counter,
                ControlStateFamily::Distinct,
                ControlStateFamily::Selection,
            ] {
                let family_key = control_state_family_key(family, &key);
                let name = control_state_family_name(family);
                let series = shard.control_state.get(&family_key);
                let all_values = series
                    .map(|series| series.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                let window = series
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
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
            if let Some(fol) = shard.control_state_selection.get(&key) {
                entries.push(("fol_value".to_string(), fol.value.clone()));
                entries.push((
                    "fol_occur_time_ms".to_string(),
                    fol.occur_time_ms.to_string().into_bytes(),
                ));
                entries.push((
                    "fol_type".to_string(),
                    match fol.selection_type {
                        ControlStateSelectionType::First => b"first".to_vec(),
                        ControlStateSelectionType::Last => b"last".to_vec(),
                    },
                ));
            }
            CommandResponse::HashEntries { entries }
        }
        Command::ContextQueryNodeEmbeddings {
            tenant_hash,
            node_hashes,
        } => {
            // One node read per hash, and the vector comes back with it -- where the separate
            // record meant a second lookup per node on top of the node read retrieval does
            // anyway.
            // Every id asked about is answered for. A limit belongs on a query that searches,
            // where the caller cannot know how many results exist; this batch is a list of ids the
            // caller already holds, so truncating it drops nodes the caller named, with nothing in
            // the response saying so. The sibling `ContextGetNodes` reads its whole batch, and
            // `command_validation` already declares a key for every hash in this one.
            let embeddings = dedupe_nonzero_u64_preserve_order(node_hashes)
                .into_iter()
                .filter_map(|node_hash| {
                    let object_key = context_node_key(tenant_hash, node_hash);
                    let node = shard
                        .hashes
                        .get(&object_key)
                        .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                        .or_else(|| shard.context_nodes.get(&object_key))
                        .and_then(|address| {
                            read_page_shared(cache, page_store, shard_id, address)
                                .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                        })?;
                    if node.vector.is_empty() {
                        return None;
                    }
                    Some((node_hash, node.vector))
                })
                .collect();
            CommandResponse::ContextNodeEmbeddings { embeddings }
        }
        Command::ContextSetNodeEmbedding {
            tenant_hash,
            node_hash,
            model_hash,
            vector,
            updated_at_ms,
        } => {
            // Read-modify-write of the node record. The vector rides along with the node from
            // here on, so a reader that has fetched the node has already paid for the vector --
            // no second key, no second block, and no hash to invert.
            let object_key = context_node_key(tenant_hash, node_hash);
            let existing = load_context_node(cache, page_store, shard_id, shard, &object_key);
            match existing {
                // No node to attach to. Writing a placeholder here would invent a node that
                // ingest never created, so report it rather than fabricate one.
                None => CommandResponse::ContextObjectKey {
                    object_key: String::new(),
                },
                Some(mut node) => {
                    node.vector = vector;
                    node.embedding_model_hash = model_hash;
                    node.embedding_updated_at_ms = updated_at_ms;
                    // `summary_vector` is left exactly as it was read. It is a copy of the L1
                    // SUMMARY's vector, and re-embedding the node does not re-embed the summary,
                    // so the copy is still the summary's current vector. Clearing it here would
                    // throw away a valid copy; overwriting it with the new node vector would
                    // claim the summary says something it does not.
                    mutated |= write_context_node(
                        cache,
                        page_store,
                        shard_id,
                        shard,
                        &object_key,
                        &node,
                        start_routing_bucket,
                        end_routing_bucket,
                        async_storage,
                    );
                    CommandResponse::ContextObjectKey { object_key }
                }
            }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            let object_key = context_node_key(tenant_hash, node.node_hash);
            mutated |= write_context_node(
                cache,
                page_store,
                shard_id,
                shard,
                &object_key,
                &node,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
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
                    read_page_shared(cache, page_store, shard_id, address)
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
                            read_page_shared(cache, page_store, shard_id, address)
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
            // context_models_match_keys_timeline_pages_and_filters keeps this
            // key shape aligned with the context event timeline contract.
            let timeline_key = context_timeline_key(event.primary_time_ms(), event.event_id_hash);
            let event_id_hash = event.event_id_hash;
            let series = shard.context_events.entry(object_key.clone()).or_default();
            // Idempotence now tests the EVENT ID, which is what first_write_only actually means:
            // the same event written twice. Testing the timeline key made two different events
            // that collided in the low FANOUT bits of one millisecond look like a rewrite.
            if !(first_write_only && series.contains_key(&event_id_hash)) {
                let value = context_bytes(&*event);
                let routing_bucket =
                    page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
                // The PAGE stays timestamp-keyed: pages pack by time, and the load path recovers
                // the timeline key from the packed point. Only the index key changes.
                if let Ok(addresses) = append_timestamped_kv_pages_keyed(
                    cache,
                    page_store,
                    shard_id,
                    "context_event",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value,
                    }],
                    routing_bucket,
                    async_storage && !cold_storage,
                    !cold_storage,
                    event_id_hash,
                ) {
                    for (stored_timeline_key, address) in addresses {
                        series.insert(event_id_hash, address);
                        shard
                            .context_event_timeline
                            .entry(object_key.clone())
                            .or_default()
                            .insert(stored_timeline_key, event_id_hash);
                        mutated = true;
                    }
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
            if !bulk_ingest_mode()
                && maybe_auto_compress_context_node(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    node_hash,
                    &object_key,
                    start_routing_bucket,
                    end_routing_bucket,
                    async_storage,
                )
            {
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
            // wire-compatible timestamp key discipline.
            let event_timeline_key = context_timeline_key(primary_time_ms, event.event_id_hash);
            let event_id_hash = event.event_id_hash;
            let event_series = shard
                .context_events
                .entry(event_object_key.clone())
                .or_default();
            // Same key discipline as ContextWriteEvent since the event rekey: the primary map
            // is keyed by EVENT ID (idempotence tests the id), the page stays timestamp-keyed,
            // and the time index maps the stored timeline key back to the id. This arm was
            // missed by the rekey -- it kept inserting timeline keys into the id-keyed map and
            // never fed the time index, so every extracted event was invisible to time-ranged
            // queries while its write reported success.
            if !(first_write_only && event_series.contains_key(&event_id_hash)) {
                // `event` is boxed on this variant; the wire encoder takes the value.
                let event_value = &*event;
                let value = context_bytes(event_value);
                let routing_bucket = page_routing_bucket(
                    &event_object_key,
                    start_routing_bucket,
                    end_routing_bucket,
                );
                if let Ok(addresses) = append_timestamped_kv_pages_keyed(
                    cache,
                    page_store,
                    shard_id,
                    "context_event",
                    &event_object_key,
                    vec![FeaturePoint {
                        timestamp_ms: event_timeline_key,
                        value,
                    }],
                    routing_bucket,
                    async_storage && !cold_storage,
                    !cold_storage,
                    event_id_hash,
                ) {
                    for (stored_timeline_key, address) in addresses {
                        event_series.insert(event_id_hash, address);
                        shard
                            .context_event_timeline
                            .entry(event_object_key.clone())
                            .or_default()
                            .insert(stored_timeline_key, event_id_hash);
                        mutated = true;
                    }
                }
            }
            invalidate_context_record(cache, shard_id, &event_object_key);

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
                    let routing_bucket =
                        page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
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
                        routing_bucket,
                        async_storage && !cold_storage,
                        !cold_storage,
                    ) {
                        let series = shard.context_indexes.entry(object_key.clone()).or_default();
                        for (timestamp_ms, address) in addresses {
                            series.insert(timestamp_ms, address);
                            mutated = true;
                        }
                        invalidate_context_record(cache, shard_id, &object_key);
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
            max_scan,
            current_valid_only,
            as_of_ms,
            kinds,
            statuses,
            min_confidence,
            min_importance,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            let scan_limit = context_limit(max_scan);
            let events = shard
                .context_events
                .get(&object_key)
                .map(|_series| {
                    let mut page_cache = HashMap::new();
                    // Range the TIME INDEX, not the primary map: the primary is keyed by event
                    // id hash now, so a time window is not contiguous in it.
                    context_event_time_range(shard, &object_key, start_time_ms, end_time_ms)
                        .rev()
                        // Bound the SCAN (kMaxLimit), NOT the result: the caller's
                        // `limit` must be applied AFTER filtering (LimitOrDefault runs
                        // post-filter). Scan newest-first so retrieval can find recent,
                        // serving-relevant context without walking cold history first.
                        .take(scan_limit)
                        .filter_map(|(timeline_key, address)| {
                            read_context_value_cached::<ContextEvent>(
                                cache,
                                page_store,
                                shard_id,
                                timeline_key,
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
                        .take(context_limit(limit))
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
            let routing_bucket =
                page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
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
                routing_bucket,
                async_storage,
                true,
            ) {
                let series = shard.context_indexes.entry(object_key.clone()).or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
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
            let routing_bucket =
                page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
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
                routing_bucket,
                async_storage,
                true,
            ) {
                let series = shard.context_audits.entry(object_key.clone()).or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
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
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
        } => {
            // In-memory, coalesced summary-dirty tracking. Repeated marks for the same
            // node update a single hashmap entry instead of appending a new persisted
            // page, so dirty records are bounded by distinct dirty nodes, not by events.
            // The map is ephemeral (never written to the page store) and may be lost on
            // restart; the async summary worker re-marks on the next event.
            let object_key = context_dirty_key(tenant_hash, node_hash);
            let entry = shard
                .context_dirty_index
                .entry(object_key.clone())
                .or_default();
            if entry.mark_count == 0 {
                entry.node_hash = node_hash;
                entry.first_event_time_ms = event_time_ms;
                entry.last_event_time_ms = event_time_ms;
            } else {
                entry.first_event_time_ms = entry.first_event_time_ms.min(event_time_ms);
                entry.last_event_time_ms = entry.last_event_time_ms.max(event_time_ms);
            }
            entry.reason = reason;
            entry.propagate_depth = entry.propagate_depth.max(propagate_depth);
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
            let nodes = shard
                .context_dirty_index
                .get(&object_key)
                .filter(|entry| {
                    entry.mark_count > 0
                        && entry.last_event_time_ms >= start_time_ms
                        && entry.first_event_time_ms <= end_time_ms
                })
                .map(|entry| {
                    vec![ContextDirtyNode {
                        node_hash: entry.node_hash,
                        first_event_time_ms: entry.first_event_time_ms,
                        last_event_time_ms: entry.last_event_time_ms,
                        reason: entry.reason,
                        propagate_depth: entry.propagate_depth,
                        mark_count: entry.mark_count,
                    }]
                })
                .unwrap_or_default();
            CommandResponse::ContextSummaryDirtyNodes { object_key, nodes }
        }
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
            clear,
        } => {
            // In-memory, coalesced embedding-dirty tracking, independent of the
            // summary-dirty index. `clear` removes the entry (the drainer clears a
            // node once it has been successfully embedded); otherwise repeated marks
            // for the same node coalesce into a single entry so dirty records stay
            // bounded by distinct dirty nodes, not by events. The map is ephemeral
            // (never written to the page store) and re-derived after restart.
            let object_key = context_embedding_dirty_key(tenant_hash, node_hash);
            if clear {
                shard.context_embedding_dirty_index.remove(&object_key);
            } else {
                let entry = shard
                    .context_embedding_dirty_index
                    .entry(object_key.clone())
                    .or_default();
                if entry.mark_count == 0 {
                    entry.tenant_hash = tenant_hash;
                    entry.node_hash = node_hash;
                    entry.first_event_time_ms = event_time_ms;
                    entry.last_event_time_ms = event_time_ms;
                } else {
                    entry.first_event_time_ms = entry.first_event_time_ms.min(event_time_ms);
                    entry.last_event_time_ms = entry.last_event_time_ms.max(event_time_ms);
                }
                entry.reason = reason;
                entry.propagate_depth = entry.propagate_depth.max(propagate_depth);
                entry.mark_count = entry.mark_count.saturating_add(1);
            }
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryEmbeddingDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            // node_hash == 0 -> the drainer's O(pending) scan over the whole
            // embedding-dirty index (all tenants on the shard). node_hash != 0 ->
            // the single coalesced entry for that node. Either way this touches only
            // the pending set, never the corpus.
            let cap = context_limit(limit);
            let mut nodes = Vec::new();
            let mut tenant_hashes = Vec::new();
            let object_key = if node_hash == 0 {
                // Deterministic order (by object key) so a bounded drain pass is
                // stable across calls.
                let mut entries: Vec<_> = shard
                    .context_embedding_dirty_index
                    .iter()
                    .filter(|(_, entry)| {
                        entry.mark_count > 0
                            && entry.last_event_time_ms >= start_time_ms
                            && entry.first_event_time_ms <= end_time_ms
                            && (tenant_hash == 0 || entry.tenant_hash == tenant_hash)
                    })
                    .collect();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                for (_, entry) in entries.into_iter().take(cap) {
                    nodes.push(ContextDirtyNode {
                        node_hash: entry.node_hash,
                        first_event_time_ms: entry.first_event_time_ms,
                        last_event_time_ms: entry.last_event_time_ms,
                        reason: entry.reason,
                        propagate_depth: entry.propagate_depth,
                        mark_count: entry.mark_count,
                    });
                    tenant_hashes.push(entry.tenant_hash);
                }
                context_embedding_dirty_key(tenant_hash, 0)
            } else {
                let object_key = context_embedding_dirty_key(tenant_hash, node_hash);
                if let Some(entry) =
                    shard
                        .context_embedding_dirty_index
                        .get(&object_key)
                        .filter(|entry| {
                            entry.mark_count > 0
                                && entry.last_event_time_ms >= start_time_ms
                                && entry.first_event_time_ms <= end_time_ms
                        })
                {
                    nodes.push(ContextDirtyNode {
                        node_hash: entry.node_hash,
                        first_event_time_ms: entry.first_event_time_ms,
                        last_event_time_ms: entry.last_event_time_ms,
                        reason: entry.reason,
                        propagate_depth: entry.propagate_depth,
                        mark_count: entry.mark_count,
                    });
                    tenant_hashes.push(entry.tenant_hash);
                }
                object_key
            };
            CommandResponse::ContextEmbeddingDirtyNodes {
                object_key,
                nodes,
                tenant_hashes,
            }
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            // The PAGE is still addressed per entity -- the object id must stay unique per
            // entity or two entities of one node would overwrite each other's bytes. Only the
            // INDEX changes shape: the node's collection key owns a BTreeMap keyed by entity
            // hash, so the entity's own key no longer occupies a map slot of its own.
            let object_key = context_entity_key(tenant_hash, entity.node_hash, entity.entity_hash);
            let collection_key = context_entity_collection_key(tenant_hash, entity.node_hash);
            let collection_key_for_response = collection_key.clone();
            let object_id = stable_page_object_id(shard_id, "context_entity", &object_key, None);
            let routing_bucket =
                page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
            let bytes = context_bytes(&entity);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &bytes,
                Some(object_id),
                Some(routing_bucket),
                async_storage,
            ) {
                // insert() on the entity hash OVERWRITES: an entity has one current value, and
                // its history is the separate context_entity_update_audit series. Keying this
                // map by time instead would silently turn every upsert into an append.
                stage_component_outcome(
                    shard_id,
                    "context_entity",
                    &collection_key,
                    Some(entity.entity_hash.to_string()),
                    routing_bucket,
                    Some(address.clone()),
                    None,
                );
                shard
                    .context_entities
                    .entry(collection_key)
                    .or_default()
                    .insert(entity.entity_hash, address);
                mutated = true;
            }
            invalidate_context_record(cache, shard_id, &object_key);
            invalidate_context_record(cache, shard_id, &collection_key_for_response);
            CommandResponse::ContextObjectKey {
                object_key: collection_key_for_response,
            }
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            let collection_key = context_entity_collection_key(tenant_hash, node_hash);
            let object_key = collection_key.clone();
            let entity = shard
                .context_entities
                .get(&collection_key)
                .and_then(|series| series.get(&entity_hash))
                .and_then(|address| {
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
            let series = shard.context_entities.get(&object_key);
            let read_entity = |address: &BlockAddress| {
                read_page_bytes(cache, page_store, shard_id, address)
                    .and_then(|bytes| context_from_bytes::<ContextEntity>(&bytes))
            };
            // An empty entity_hashes now means "every entity of this node" instead of "nothing".
            // Before the fold the caller had to already know each hash, because the entities of
            // one node were separate HashMap keys and a HashMap cannot be prefix-scanned -- so a
            // node's entity set could not be discovered from the engine at all. Passing explicit
            // hashes still selects exactly those, so existing callers are unaffected.
            let entities = match (series, entity_hashes.is_empty()) {
                (None, _) => Vec::new(),
                (Some(series), true) => series
                    .values()
                    .take(context_limit(limit))
                    .filter_map(read_entity)
                    .collect(),
                (Some(series), false) => dedupe_nonzero_u64_preserve_order(entity_hashes)
                    .into_iter()
                    .take(context_limit(limit))
                    .filter_map(|entity_hash| series.get(&entity_hash).and_then(read_entity))
                    .collect(),
            };
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
                let routing_bucket =
                    page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
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
                    routing_bucket,
                    async_storage,
                    true,
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
            invalidate_context_record(cache, shard_id, &object_key);
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
            // Keep the NEWEST `limit`, not the oldest. Sorting ascending and truncating handed back
            // a parent's first children and hid everything recently added -- five children with
            // limit 2 answered with the two oldest, and the newest was unreachable by any query.
            //
            // The same cut, in the same direction, is what traversal was fixed for: the most
            // recently ingested were the first to become invisible, which is the opposite of what
            // a store keyed on time should do. The listing stays in chronological order, which is
            // what an unlimited query already returned.
            let keep = context_limit(limit);
            if refs.len() > keep {
                refs.drain(..refs.len() - keep);
            }
            CommandResponse::ContextChildRefs {
                object_key,
                refs,
                created: None,
            }
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
            let routing_bucket =
                page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
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
                routing_bucket,
                async_storage,
                true,
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
            invalidate_context_record(cache, shard_id, &object_key);
            // Keep the node's copy of this vector current. The summary record just written
            // remains the owner; the copy exists so the scoring pass, which fetches the node
            // anyway, does not have to come back here for one field.
            //
            // Only the L1 level: that is the only level scoring reads a vector from, and the
            // node has one slot, so copying L0 here would overwrite the answer with a summary
            // nothing asks for.
            if context_node_summary_vector_enabled()
                && summary.level == CONTEXT_SUMMARY_LEVEL_L1
                && !summary.vector.is_empty()
            {
                let node_key = context_node_key(tenant_hash, summary.node_hash);
                if let Some(node) = load_context_node(cache, page_store, shard_id, shard, &node_key)
                {
                    // Never move the copy BACKWARDS. A summary can be written with an older
                    // `valid_from_ms` than one already stored -- a backfill, a replay, a
                    // correction -- and taking it would leave the node claiming a superseded
                    // vector is the newest, which is precisely the read the copy answers.
                    if node.summary_vector_valid_from_ms <= summary.valid_from_ms {
                        let mut updated = node.clone();
                        // Stored form, not the value as computed. The node being compared
                        // against came off a page, and both stored vector forms round, so a raw
                        // vector differs from its own round trip -- which would call every
                        // ingest a change and append a second node page for each one.
                        updated.summary_vector = context_vector_as_stored(&summary.vector);
                        updated.summary_vector_valid_from_ms = summary.valid_from_ms;
                        // The encoder stamp travels with the vector, or the copy becomes the one
                        // scored vector whose encoder nothing checks.
                        updated.summary_vector_model_hash = summary.embedding_model_hash;
                        // Write only when the record would actually change. The ingest already
                        // wrote the node carrying this copy, moments before the summary write
                        // that lands here.
                        if context_bytes(&updated) != context_bytes(&node) {
                            mutated |= write_context_node(
                                cache,
                                page_store,
                                shard_id,
                                shard,
                                &node_key,
                                &updated,
                                start_routing_bucket,
                                end_routing_bucket,
                                async_storage,
                            );
                        }
                    }
                }
            }
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
        Command::ContextQuerySummaryVectors {
            tenant_hash,
            node_hashes,
            level,
            as_of_ms,
        } => {
            // One summary read per node -- the same per-node cost the separate embedding rows
            // had, minus the second keyspace. Only the newest summary at or before `as_of_ms`
            // is consulted; a summary carrying no vector contributes nothing, so the caller can
            // tell "not embedded" apart from "not summarized" by the node's absence here.
            let vectors = dedupe_nonzero_u64_preserve_order(node_hashes)
                .into_iter()
                .filter_map(|node_hash| {
                    let object_key = context_summary_key(tenant_hash, node_hash, level);
                    // Newest, not oldest. The series ascends with time, so the previous
                    // `take(1).next()` returned the node's FIRST summary: once a node had been
                    // re-summarised, scoring used the superseded embedding, and a stale vector of
                    // the right width still scores a plausible cosine, so nothing surfaced it.
                    load_newest_context_summary(
                        cache,
                        page_store,
                        shard_id,
                        shard,
                        &object_key,
                        as_of_ms,
                    )
                    .filter(|summary| !summary.vector.is_empty())
                    .map(|summary| ContextSummaryVector {
                        node_hash,
                        embedding_model_hash: summary.embedding_model_hash,
                        vector: summary.vector,
                    })
                })
                .collect();
            CommandResponse::ContextSummaryVectors { vectors }
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            let object_key = context_compression_key(tenant_hash, event.node_hash);
            let timeline_key =
                context_timeline_key(event.compressed_time_ms, event.compression_id_hash);
            let routing_bucket =
                page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
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
                routing_bucket,
                async_storage,
                true,
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
            invalidate_context_record(cache, shard_id, &object_key);
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
                .map(|_series| {
                    context_event_time_range(
                        shard,
                        &context_event_key(tenant_hash, node_hash),
                        source_start_ms,
                        source_end_ms,
                    )
                    .filter_map(|(timeline_key, address)| {
                        read_context_value_cold::<ContextEvent>(page_store, timeline_key, address)
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
                let routing_bucket =
                    page_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
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
                    routing_bucket,
                    async_storage,
                    false,
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
                invalidate_context_record(cache, shard_id, &object_key);
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
                    read_page_shared(cache, page_store, shard_id, address)
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

/// IEEE-754 total-order bias: flip everything for negatives, set the sign for positives, so
/// unsigned comparison of the bits is numeric comparison of the floats.
pub(super) fn zset_score_bits(score: f64) -> u64 {
    let bits = score.to_bits();
    if bits & (1 << 63) != 0 {
        !bits
    } else {
        bits | (1 << 63)
    }
}

pub(super) fn zset_score_from_bits(biased: u64) -> f64 {
    if biased & (1 << 63) != 0 {
        f64::from_bits(biased & !(1 << 63))
    } else {
        f64::from_bits(!biased)
    }
}

pub(super) fn zset_score_string(biased: u64) -> String {
    let score = zset_score_from_bits(biased);
    if score == score.trunc() && score.abs() < 1e17 {
        format!("{}", score as i64)
    } else {
        format!("{score}")
    }
}

/// The persisted component: score bits then member, so lexical order is (score, member) order.
pub(super) fn zset_component(biased: u64, member: &[u8]) -> String {
    format!("{biased:016x}{}", hex::encode(member))
}

/// The members whose score falls in `[min_bits, max_bits]`, in score order.
///
/// Same scan as `zset_ordered_members` -- the map is keyed by member, so every member has to be
/// looked at either way -- but the score test happens BEFORE the member bytes are copied, so a
/// narrow window copies a narrow window. The full-order helper below clones all n up front, which
/// cost 4,115 allocations and 324 KB to return eight members from a set of 4,096.
fn zset_members_in_score_range(
    shard: &ShardState,
    key: &str,
    min_bits: u64,
    max_bits: u64,
    min_exclusive: bool,
    max_exclusive: bool,
) -> Vec<(Vec<u8>, u64)> {
    let mut ordered: Vec<(Vec<u8>, u64)> = shard
        .zsets
        .get(key)
        .map(|members| {
            members
                .iter()
                .filter(|(_, (biased, _))| {
                    let above = if min_exclusive {
                        *biased > min_bits
                    } else {
                        *biased >= min_bits
                    };
                    let below = if max_exclusive {
                        *biased < max_bits
                    } else {
                        *biased <= max_bits
                    };
                    above && below
                })
                .map(|(member, (biased, _))| (member.clone(), *biased))
                .collect()
        })
        .unwrap_or_default();
    ordered.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    ordered
}

fn zset_ordered_members(shard: &ShardState, key: &str) -> Vec<(Vec<u8>, u64)> {
    let mut ordered: Vec<(Vec<u8>, u64)> = shard
        .zsets
        .get(key)
        .map(|members| {
            members
                .iter()
                .map(|(member, (biased, _))| (member.clone(), *biased))
                .collect()
        })
        .unwrap_or_default();
    ordered.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    ordered
}

/// The whole token-bucket model in one pure function, so its arithmetic is testable with
/// explicit clocks: refill by elapsed time at `refill_per_sec` (capped at capacity, and a
/// clock that moved backwards refills nothing), then take if it fits. Answers
/// (allowed, remaining AFTER the outcome, retry-after ms, state to store). An absent bucket
/// starts full.
pub(super) fn bucket_take(
    current: Option<(f64, u64)>,
    now_ms: u64,
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
) -> (bool, f64, u64, (f64, u64)) {
    let tokens = tokens.max(0.0);
    let capacity = capacity.max(0.0);
    let refill_per_sec = refill_per_sec.max(0.0);
    let filled = match current {
        None => capacity,
        Some((had, last_ms)) => {
            let elapsed_ms = now_ms.saturating_sub(last_ms);
            (had + refill_per_sec * (elapsed_ms as f64 / 1000.0)).min(capacity)
        }
    };
    if tokens <= filled {
        let remaining = filled - tokens;
        (true, remaining, 0, (remaining, now_ms))
    } else {
        let shortfall = tokens - filled;
        let retry_after_ms = if refill_per_sec > 0.0 && tokens <= capacity {
            (shortfall / refill_per_sec * 1000.0).ceil() as u64
        } else {
            // A take larger than capacity can never succeed; answer the sentinel.
            u64::MAX
        };
        (false, filled, retry_after_ms, (filled, now_ms))
    }
}

fn bucket_answer(allowed: bool, remaining: f64, retry_after_ms: u64) -> Vec<Vec<u8>> {
    vec![
        if allowed {
            b"1".to_vec()
        } else {
            b"0".to_vec()
        },
        format!("{remaining:.3}").into_bytes(),
        retry_after_ms.to_string().into_bytes(),
    ]
}
