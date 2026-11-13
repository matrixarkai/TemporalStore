// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn engine_at(root: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        4 * 1024 * 1024,
        root.join(cache),
        root.join("blocks"),
        root.join("indexes"),
        BlockStoreOptions::default(),
        false,
    )
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let reply = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(reply.status.ok, "{:?}", reply.status);
    reply.response
}

fn assert_lazy_expiry_clears_siblings(trigger: Command, writes_new_field: bool) {
    assert_lazy_expiry_with_residents(trigger, writes_new_field, 0);
}

fn assert_lazy_expiry_with_residents(trigger: Command, writes_new_field: bool, pages: usize) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache");
    engine.load_shard(1);
    for key in ["due", "live"] {
        for field in ["a", "b"] {
            run(
                &engine,
                Command::HashSet {
                    key: key.into(),
                    field: field.into(),
                    value: vec![7],
                },
            );
            assert_eq!(
                run(
                    &engine,
                    Command::HashGet {
                        key: key.into(),
                        field: field.into()
                    }
                ),
                CommandResponse::Bytes {
                    value: Some(vec![7])
                }
            );
            assert!(engine.cache.peek(&CacheKey::hash(1, key, field)));
        }
    }
    for index in 0..pages {
        engine
            .cache
            .put(
                CacheKey {
                    shard_id: 1,
                    namespace: "page".into(),
                    record_key: format!("resident-{index}").into(),
                    selector: "data".into(),
                },
                vec![3; 16],
            )
            .unwrap();
    }
    assert!(
        engine
            .cache
            .item_count_for_tier(matrixcache::CacheTier::Memory)
            >= pages
    );
    run(
        &engine,
        Command::CommonExpire {
            key: "due".into(),
            ttl_ms: 250,
        },
    );
    let deadline = {
        let shards = engine.shards.read().unwrap();
        *shards[&1]
            .expires_at_ms()
            .get("due")
            .expect("deadline was recorded")
    };
    // EXPIRE invalidates query entries; warm them again while the deadline is live.
    for field in ["a", "b"] {
        assert_eq!(
            run(
                &engine,
                Command::HashGet {
                    key: "due".into(),
                    field: field.into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![7])
            }
        );
        assert!(engine.cache.peek(&CacheKey::hash(1, "due", field)));
    }
    assert!(now_ms() < deadline, "the cache was not warm before expiry");
    let limit = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while now_ms() <= deadline {
        assert!(std::time::Instant::now() < limit, "deadline did not lapse");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    struct StopProbe;
    impl Drop for StopProbe {
        fn drop(&mut self) {
            CACHE_SWEEP_COUNTS.set_armed(false);
        }
    }
    let _probe = StopProbe;
    if pages > 0 {
        CACHE_SWEEP_COUNTS.reset();
        CACHE_SWEEP_COUNTS.set_armed(true);
    }
    run(&engine, trigger);
    if pages > 0 {
        let counts = CACHE_SWEEP_COUNTS.read_batched();
        CACHE_SWEEP_COUNTS.set_armed(false);
        assert_eq!(
            counts,
            (1, 1, 1, 2, 2),
            "expiry must visit only the two cached query fields"
        );
        for index in [0, pages / 2, pages - 1] {
            assert_eq!(
                engine
                    .cache
                    .get(&CacheKey {
                        shard_id: 1,
                        namespace: "page".into(),
                        record_key: format!("resident-{index}").into(),
                        selector: "data".into(),
                    })
                    .unwrap(),
                Some(vec![3; 16])
            );
        }
    }
    assert_eq!(
        run(
            &engine,
            Command::HashGet {
                key: "due".into(),
                field: "b".into()
            }
        ),
        CommandResponse::Bytes { value: None },
        "a later field read served an expired cached value"
    );
    for field in ["a", "b"] {
        assert_eq!(
            run(
                &engine,
                Command::HashGet {
                    key: "due".into(),
                    field: field.into()
                }
            ),
            CommandResponse::Bytes { value: None }
        );
        assert_eq!(
            run(
                &engine,
                Command::HashGet {
                    key: "live".into(),
                    field: field.into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![7])
            }
        );
    }
    if writes_new_field {
        assert_eq!(
            run(
                &engine,
                Command::HashGet {
                    key: "due".into(),
                    field: "new".into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![9])
            }
        );
    }
    engine.wal_store.flush(1).unwrap();
    drop(engine);
    let restored = engine_at(dir.path(), "cache-reopen");
    restored.load_shard(1);
    // Reads do not append a new WAL command. Recovery may retain a lapsed physical row,
    // but neither that row nor a rewritten live row may serve an expired field.
    for field in ["a", "b"] {
        assert_eq!(
            run(
                &restored,
                Command::HashGet {
                    key: "due".into(),
                    field: field.into()
                }
            ),
            CommandResponse::Bytes { value: None },
            "expired field was visible after restart: {field}"
        );
    }
    {
        let shards = restored.shards.read().unwrap();
        let row = shards[&1].bucket_index.hashes.get("due");
        for field in ["a", "b"] {
            assert!(
                row.and_then(|row| row.get(field)).is_none(),
                "expired field returned after restart: {field}"
            );
        }
    }
    assert_eq!(
        run(
            &restored,
            Command::HashGet {
                key: "live".into(),
                field: "b".into()
            }
        ),
        CommandResponse::Bytes {
            value: Some(vec![7])
        }
    );
    if writes_new_field {
        assert_eq!(
            run(
                &restored,
                Command::HashGet {
                    key: "due".into(),
                    field: "new".into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![9])
            }
        );
    }
}

#[test]
fn lazy_expiry_point_read_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(
        Command::HashGet {
            key: "due".into(),
            field: "a".into(),
        },
        false,
    );
}

#[test]
fn lazy_expiry_projected_read_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(
        Command::HashMultiGet {
            key: "due".into(),
            fields: vec!["a".into()],
        },
        false,
    );
}

#[test]
fn lazy_expiry_scalar_write_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(
        Command::HashSet {
            key: "due".into(),
            field: "new".into(),
            value: vec![9],
        },
        true,
    );
}

#[test]
fn lazy_expiry_multi_write_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(
        Command::HashMultiSet {
            key: "due".into(),
            entries: vec![("new".into(), vec![9])],
        },
        true,
    );
}

#[test]
fn lazy_expiry_ttl_write_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(
        Command::HashMultiSetWithTtl {
            key: "due".into(),
            entries: vec![("new".into(), vec![9])],
            ttl_ms: 60_000,
        },
        true,
    );
}

#[test]
fn lazy_expiry_conditional_write_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(
        Command::HashMultiSetIfTimestampOlder {
            key: "due".into(),
            timestamp_field: "timestamp".into(),
            timestamp_seconds: 1,
            timestamp_nanos: 0,
            ttl_ms: None,
            entries: vec![("new".into(), vec![9])],
        },
        true,
    );
}

#[test]
fn lazy_expiry_exists_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(Command::CommonExists { key: "due".into() }, false);
}

#[test]
fn lazy_expiry_ttl_read_retires_cached_siblings() {
    assert_lazy_expiry_clears_siblings(Command::CommonTtl { key: "due".into() }, false);
}

#[test]
fn lazy_expiry_cross_model_write_retires_cached_hash_fields() {
    assert_lazy_expiry_clears_siblings(
        Command::StringSet {
            key: "due".into(),
            value: vec![9],
        },
        false,
    );
}

#[test]
fn lazy_expiry_point_read_visits_only_its_record_with_a_large_resident_cache() {
    assert_lazy_expiry_with_residents(
        Command::HashGet {
            key: "due".into(),
            field: "a".into(),
        },
        false,
        100_000,
    );
}

fn assert_independent_family_expiry(expire_base: bool) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache");
    engine.load_shard(1);
    let family_key = control_state_family_key(ControlStateFamily::Counter, "due");
    let entries = vec![("a".into(), vec![7]), ("b".into(), vec![7])];
    run(
        &engine,
        if expire_base {
            Command::HashMultiSetWithTtl {
                key: "due".into(),
                entries,
                ttl_ms: 250,
            }
        } else {
            Command::HashMultiSet {
                key: "due".into(),
                entries,
            }
        },
    );
    // Create the family after the base deadline was set, so its lifetime is independent.
    run(
        &engine,
        if expire_base {
            Command::StringSet {
                key: family_key.clone(),
                value: vec![5],
            }
        } else {
            Command::StringSetEx {
                key: family_key.clone(),
                value: vec![5],
                ttl_ms: 250,
            }
        },
    );
    let expired_key = if expire_base { "due" } else { &family_key };
    let deadline = *engine.shards.read().unwrap()[&1]
        .expires_at_ms()
        .get(expired_key)
        .unwrap();
    for field in ["a", "b"] {
        assert_eq!(
            run(
                &engine,
                Command::HashGet {
                    key: "due".into(),
                    field: field.into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![7])
            }
        );
    }
    assert!(
        now_ms() < deadline,
        "fixture did not warm before the deadline"
    );
    while now_ms() <= deadline {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    run(
        &engine,
        Command::HashMultiSet {
            key: "due".into(),
            entries: vec![("new".into(), vec![9])],
        },
    );
    engine.wal_store.flush(1).unwrap();
    let (records, truncated) = engine
        .wal_store
        .scan_decoded(1, 0, u64::MAX, 1024 * 1024)
        .unwrap();
    assert!(!truncated);
    let outcomes = &records.last().unwrap().1.outcomes;
    assert_eq!(
        outcomes.iter().filter(|item| item.kind == "record").count(),
        1
    );
    let deletion = &outcomes[0];
    assert_eq!(
        deletion.kind, "record",
        "deletion must precede replacement blocks"
    );
    assert_eq!(deletion.object_key, expired_key);
    assert!(deletion.deleted && deletion.meta);
    for bad_field in 0..4 {
        let mut malformed = deletion.clone();
        match bad_field {
            0 => malformed.meta = false,
            1 => malformed.value = Some(vec![1]),
            2 => malformed.ttl = Some(1),
            _ => malformed.element_name = Some("field".into()),
        }
        assert!(!engine.apply_outcome_item(1, &malformed, &[]));
    }
    let check = |engine: &TemporalEngine| {
        assert_eq!(
            run(
                engine,
                Command::StringGet {
                    key: family_key.clone()
                }
            ),
            CommandResponse::Bytes {
                value: expire_base.then(|| vec![5])
            }
        );
        for field in ["a", "b"] {
            assert_eq!(
                run(
                    engine,
                    Command::HashGet {
                        key: "due".into(),
                        field: field.into()
                    }
                ),
                CommandResponse::Bytes {
                    value: (!expire_base).then(|| vec![7])
                }
            );
        }
        assert_eq!(
            run(
                engine,
                Command::HashGet {
                    key: "due".into(),
                    field: "new".into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![9])
            }
        );
    };
    check(&engine);
    drop(engine);
    let restored = engine_at(dir.path(), "cache-reopen");
    restored.load_shard(1);
    check(&restored);
}

#[test]
fn lazy_expiry_base_rewrite_preserves_an_independently_live_family() {
    assert_independent_family_expiry(true);
}

#[test]
fn lazy_expiry_family_removal_preserves_live_base_fields() {
    assert_independent_family_expiry(false);
}

#[test]
fn lazy_expiry_is_recorded_even_when_a_selection_write_loses() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache");
    engine.load_shard(1);
    let selection = |occur_time_ms| Command::ControlStateSelectionSet {
        key: "due".into(),
        value: vec![1],
        occur_time_ms,
        ttl_ms: 0,
        selection_type: ControlStateSelectionType::First,
    };
    run(&engine, selection(1));
    let family_key = control_state_family_key(ControlStateFamily::Counter, "due");
    run(
        &engine,
        Command::StringSetEx {
            key: family_key.clone(),
            value: vec![5],
            ttl_ms: 250,
        },
    );
    let before = engine.wal_store.cached_last_sequence(1);
    run(&engine, selection(2));
    assert_eq!(
        engine.wal_store.cached_last_sequence(1),
        before,
        "a live losing write must remain a no-op"
    );
    let deadline = *engine.shards.read().unwrap()[&1]
        .expires_at_ms()
        .get(family_key.as_str())
        .unwrap();
    while now_ms() <= deadline {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    run(&engine, selection(2));
    assert_eq!(
        engine.wal_store.cached_last_sequence(1),
        before + 1,
        "the expired family removal was not recorded"
    );
    engine.wal_store.flush(1).unwrap();
    let (records, truncated) = engine
        .wal_store
        .scan_decoded(1, 0, u64::MAX, 1024 * 1024)
        .unwrap();
    assert!(!truncated);
    let outcomes = &records.last().unwrap().1.outcomes;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].kind, "record");
    assert_eq!(outcomes[0].object_key, family_key);
    drop(engine);
    let restored = engine_at(dir.path(), "cache-reopen");
    restored.load_shard(1);
    assert!(!restored.shards.read().unwrap()[&1]
        .strings()
        .contains_key(family_key.as_str()));
    assert_eq!(
        run(
            &restored,
            Command::ControlStateSelectionQuery { key: "due".into() }
        ),
        CommandResponse::Bytes {
            value: Some(vec![1])
        }
    );
}
