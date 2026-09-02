// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 3 of engine tests, split from engine/tests.rs.
#![allow(clippy::all)]
use super::*;

#[test]
fn control_api_reads_page_and_index_streams() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"stream-value".to_vec(),
        },
    });

    let page = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        page_slab_id: 0,
        offset: 0,
        size: 12,
    });
    assert_eq!(page.data, b"stream-value".to_vec());

    let index = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Index,
        page_slab_id: 0,
        offset: 0,
        size: 32,
    });
    assert!(index.status.ok);
    assert!(!index.data.is_empty());

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        page_slab_id: 0,
        start_offset: 0,
        end_offset: 12,
        max_bytes: 12,
    });
    assert_eq!(scan.records.len(), 1);
    assert_eq!(scan.records[0].data, b"stream-value".to_vec());

    let invalid = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        page_slab_id: 0,
        start_offset: 12,
        end_offset: 1,
        max_bytes: 12,
    });
    assert_eq!(invalid.status.code, "invalid_stream_range");
    assert!(invalid.records.is_empty());
}

#[test]
fn a_scan_cut_short_by_its_budget_does_not_claim_the_stream_ended() {
    // scan_stream stops walking for two unrelated reasons -- the window ended, or max_bytes ran
    // out -- and it answered end_of_stream: true for both. So a caller reading a range larger
    // than its budget was handed a prefix and told it had the whole thing, with nothing in the
    // response to suggest otherwise.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..8 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let all = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        page_slab_id: 0,
        start_offset: 0,
        end_offset: u64::MAX,
        max_bytes: u64::MAX,
    });
    assert!(all.status.ok);
    assert!(
        all.records.len() >= 4,
        "premise: there are several records to walk, got {}",
        all.records.len()
    );
    assert!(
        all.end_of_stream,
        "a scan with no budget to run out of did reach the end of the window"
    );

    // A budget that fits the first record and not the second.
    let budget = all.records[0].data.len() as u64 + 1;
    let cut = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        page_slab_id: 0,
        start_offset: 0,
        end_offset: u64::MAX,
        max_bytes: budget,
    });
    assert!(cut.status.ok);
    assert!(
        cut.records.len() < all.records.len(),
        "premise: the budget cut this scan short, got {} of {}",
        cut.records.len(),
        all.records.len()
    );
    assert!(
        !cut.end_of_stream,
        "the budget stopped this scan with records still in the window, so it must not report          the stream as ended"
    );
}

#[test]
fn control_api_reads_and_scans_wal_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k2".to_string(),
            value: b"v2".to_vec(),
        },
    });

    let stream = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        page_slab_id: 0,
        offset: 0,
        size: 4096,
    });
    assert!(stream.status.ok);
    // Decode the records rather than matching on how a field is spelled: the endpoint returns
    // the log's records, and that is what should be asserted.
    // Walk the frames the stream carries rather than splitting it on newlines: a record ends
    // where its frame says it does, and a length-framed payload contains 0x0A of its own.
    let mut sequences: Vec<u64> = Vec::new();
    let mut at = 0usize;
    while at < stream.data.len() {
        if stream.data[at] == 0 {
            break;
        }
        match crate::log_framing::next_frame(&stream.data[at..]) {
            Ok(Some((consumed, _))) if consumed > 0 => {
                sequences.push(
                    crate::wal::decode_wal_line(&stream.data[at..at + consumed])
                        .expect("the stream should carry decodable records")
                        .sequence,
                );
                at += consumed;
            }
            _ => break,
        }
    }
    assert_eq!(
        sequences,
        vec![1, 2],
        "the stream should carry both records, in order"
    );

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        page_slab_id: 0,
        start_offset: 0,
        end_offset: 4096,
        max_bytes: 4096,
    });
    assert_eq!(scan.records.len(), 2);
    assert_eq!(
        engine
            .get_stats(1)
            .stats
            .unwrap()
            .write_ahead_log
            .last_sequence,
        2
    );
}

#[test]
fn control_api_reads_and_scans_index_log_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"hv".to_vec(),
        },
    });

    let stream = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::IndexLog,
        page_slab_id: 0,
        offset: 0,
        size: 8192,
    });
    assert!(stream.status.ok);
    // The index-log advances one record per write. Reading the stream as TEXT and matching a
    // field's spelling was an assumption about the encoding, not about the advance this test
    // is for -- and a record's payload need not be text at all. Walk it the way a reader does
    // and assert over the sequences themselves.
    let mut sequences = Vec::new();
    let mut at = 0usize;
    while at < stream.data.len() {
        let Some((consumed, payload)) = crate::log_framing::next_frame(&stream.data[at..]).unwrap()
        else {
            break;
        };
        let record: crate::index_log::IndexDeltaRecord =
            crate::index_log::decode_index_payload(payload).unwrap();
        sequences.push(record.sequence);
        at += consumed;
    }
    assert_eq!(sequences, vec![1, 2], "one record per write, in order");

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::IndexLog,
        page_slab_id: 0,
        start_offset: 0,
        end_offset: 8192,
        max_bytes: 8192,
    });
    assert_eq!(scan.records.len(), 2);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);

    // Content is asserted over the folded served-index reconstruction (export_index_bytes),
    // which is the complete, current ShardState in both the whole-index (base-file) and the
    // delta (live-in-memory) paths -- the index-log itself is a per-write log, not the
    // authoritative complete image.
    // The served index is written in whichever format the container gate selects, so decode it
    // through the funnel a reader uses and assert over the STATE. Reading the bytes as JSON was an
    // assumption about the encoding, not about the thing being tested.
    let served: serde_json::Value = serde_json::to_value(
        crate::engine::decode_index_bytes(&engine.export_index_bytes(1).unwrap())
            .expect("served index decodes"),
    )
    .expect("shard state re-serializes for assertion");
    // `hashes` is deliberately NOT serialized (`skip_serializing`): it is rebuildable from
    // the durable bucket index on load, and duplicating those page references in every
    // checkpoint is what made large context backfills tens of MB heavier. Assert the hash
    // write through that authority -- the bucket index -- rather than through a map the
    // served index no longer carries.
    // The bucket-index page entries serialize under abbreviated field names on some builds
    // and full names on others, so accept either: what is being asserted is that the hash
    // write is recorded with the right page address, not how the fields are spelled.
    let hash_page = served["slot_index"]["slot_map"]
        .as_object()
        .expect("served index carries the bucket map")
        .values()
        .filter_map(|bucket| {
            bucket
                .get("page_index")
                .or_else(|| bucket.get("p"))
                .and_then(|pages| pages.as_object())
        })
        .flat_map(|pages| pages.values())
        .find(|page| {
            let key = page.get("object_key").or_else(|| page.get("k"));
            let component = page.get("component").or_else(|| page.get("c"));
            key == Some(&serde_json::json!("h")) && component == Some(&serde_json::json!("f"))
        })
        .expect("the hash h/f write is recorded in the bucket index");
    let address = hash_page
        .get("address")
        .or_else(|| hash_page.get("a"))
        .expect("the recorded page carries its address");
    // Either spelling: the field is written short now and the long name is kept as a read alias,
    // which is exactly how this test already reads `object_key` and `component` above.
    let page_slab = address
        .get("ps")
        .or_else(|| address.get("page_segment_id"))
        .expect("the address carries its page slab id");
    assert_eq!(page_slab, &serde_json::json!(0));
    assert!(served["strings"].get("k1").is_some());
}

#[test]
fn readonly_shard_rejects_writes_but_allows_reads() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 1,
                local_node_id: None,
                shard_uri: "file:///tmp/readonly".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 99,
                readonly: true,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(!write.status.ok);
    assert_eq!(write.status.code, "readonly_shard");

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(read.status.ok);
    assert_eq!(read.response, CommandResponse::Bytes { value: None });
}

#[test]
fn checked_execute_rejects_stale_load_version() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 7,
                local_node_id: None,
                shard_uri: "file:///tmp/versioned".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 99,
                readonly: false,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );

    let stale = engine.execute_checked(CheckedExecuteRequest {
        shard_id: 1,
        load_version: 6,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(stale.status.code, "load_version_mismatch");

    let current = engine.execute_checked(CheckedExecuteRequest {
        shard_id: 1,
        load_version: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(current.status.ok);
}

#[test]
fn loaded_shard_stats_reports_per_shard_load() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.load_shard(2);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "a".to_string(),
            value: b"1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"2".to_vec(),
        },
    });

    let stats = engine.loaded_shard_stats();
    assert_eq!(stats.len(), 2);
    assert!(stats
        .iter()
        .any(|stat| stat.shard_id == 1 && stat.string_records == 1));
    assert!(stats
        .iter()
        .any(|stat| stat.shard_id == 2 && stat.hash_records == 1));
}

#[test]
fn string_set_conditional_supports_nx_xx_and_get() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v1".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
        },
    });
    assert_eq!(first.response, CommandResponse::Integer { value: 1 });

    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v2".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
        },
    });
    assert_eq!(rejected.response, CommandResponse::Integer { value: 0 });

    let old = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v3".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfExists,
            return_old: true,
        },
    });
    assert_eq!(
        old.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v3".to_vec())
        }
    );
}

#[test]
fn recovery_validates_all_timestamped_kv_page_families() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let feature_points = (0..8)
        .map(|idx| FeaturePoint {
            timestamp_ms: 1_000 + idx,
            value: vec![b'f'; 10 * 1024],
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: "all-family-feature".to_string(),
                    points: feature_points.clone(),
                },
            })
            .status
            .ok
    );

    let sequence_rows = (0..8)
        .map(|idx| SequenceFeatureRow {
            timestamp_ms: 2_000 + idx,
            gid: idx,
            action_type: 7,
            duration: 11,
            author_id: 13,
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceAdd {
                    key: "all-family-sequence".to_string(),
                    rows: sequence_rows.clone(),
                },
            })
            .status
            .ok
    );

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteEvent {
                    tenant_hash: 44,
                    node_hash: 55,
                    event: ContextEvent {
                        event_id_hash: 66,
                        event_time_ms: 4_000,
                        ingestion_time_ms: 4_000,
                        kind: 1,
                        event_type: 2,
                        actor_hash: 77,
                        status: 1,
                        valid_until_ms: 0,
                        confidence: 0.99,
                        importance: 0.75,
                        text: "context event".to_string(),
                        source_ref: "local://test".to_string(),
                        related_node_hashes: vec![55],
                        compact_attrs: vec![1, 2, 3],
                        vector: Vec::new(),
                    },
                    first_write_only: false,
                    cold_storage: false,
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteIndexRef {
                    tenant_hash: 44,
                    index_name: "actor".to_string(),
                    index_value_hash: 77,
                    scope_hash: 1,
                    event_time_ms: 4_000,
                    index_ref: ContextIndexRef {
                        primary_node_hash: 55,
                        primary_event_time_ms: 4_000,
                        event_id_hash: 66,
                    },
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWritePackAudit {
                    tenant_hash: 44,
                    audit: ContextPackAudit {
                        query_id: "q-all-family".to_string(),
                        session_hash: 88,
                        request_time_ms: 4_100,
                        query_hash: 99,
                        max_prompt_tokens: 128,
                        selected_tokens: 32,
                        selected_refs: vec![ContextAuditRef {
                            node_hash: 55,
                            event_time_ms: 4_000,
                            reason: "selected".to_string(),
                        }],
                        blocked_refs: Vec::new(),
                    },
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextMarkSummaryDirty {
                    tenant_hash: 44,
                    node_hash: 55,
                event_time_ms: 4_200,
                reason: 9,
                propagate_depth: 2,
                },
            })
            .status
            .ok
    );

    let report = engine.storage_recovery_report(1);
    // context_dirty tracking was intentionally moved to an ephemeral in-memory
    // coalesced index (commit 9390d110), so it no longer contributes a persisted
    // timestamped page: feature 16 (8 feature + 8 sequence, now folded into the
    // feature family) + context_event/index/audit 1 each = 19.
    assert_eq!(report.feature_page_layout.indexed_timestamped_points, 19);
    assert!(report.feature_page_layout.packed_timestamped_pages >= 10);
    assert!(
        report
            .feature_page_layout
            .unique_timestamped_page_refs
            .saturating_sub(report.feature_page_layout.packed_timestamped_pages)
            <= report.feature_page_layout.legacy_timestamped_value_pages
    );
    assert!(report
        .feature_page_layout
        .corrupt_packed_feature_pages
        .is_empty());
    assert!(report
        .feature_page_layout
        .missing_indexed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .orphan_packed_timestamps
        .is_empty());
    assert!(report
        .feature_page_layout
        .duplicate_packed_timestamps
        .is_empty());

    let families = report
        .feature_page_layout
        .families
        .iter()
        .map(|family| (family.kind.as_str(), family))
        .collect::<BTreeMap<_, _>>();
    // Sequence folds into the feature family (same timestamped-KV storage, typed row
    // codec at the API layer), so there is no distinct "sequence" recovery family.
    for kind in [
        "feature",
        "context_event",
        "context_index",
        "context_audit",
    ] {
        let family = families.get(kind).expect("timestamped family report");
        assert!(family.indexed_points > 0, "{kind}");
        assert!(family.packed_pages > 0, "{kind}");
        assert_eq!(family.corrupt_pages, 0, "{kind}");
        assert_eq!(family.mismatch_count, 0, "{kind}");
    }
    assert!(
        families
            .get("feature")
            .expect("feature family")
            .unique_page_refs
            > 1
    );

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "all-family-feature".to_string(),
                    start_ms: 1_000,
                    end_ms: 1_010,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: feature_points
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: "all-family-sequence".to_string(),
                    start_ms: 2_000,
                    end_ms: 2_010,
                    count: 16,
                    filters: Vec::new(),
                },
            })
            .response,
        CommandResponse::SequenceRows {
            rows: sequence_rows
        }
    );
}

#[test]
fn control_state_change_matches_distinct_field_semantics() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, value) in [(10, "device-a"), (20, "device-a"), (30, "device-b")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
                key: "control_state-change".to_string(),
                timestamp_ms,
                value: value.as_bytes().to_vec(),
                precision_ms: Some(10),
                ttl_ms: None,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateQuery {
                    key: "control_state-change".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "change".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 2 }
    );

    for (timestamp_ms, value) in [(10, "buyer-1"), (20, "buyer-1"), (30, "buyer-2")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
                key: control_state_family_key(ControlStateFamily::Counter, "control_state-change"),
                timestamp_ms,
                value: value.as_bytes().to_vec(),
                precision_ms: None,
                ttl_ms: None,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateFamilyQuery {
                    family: ControlStateFamily::Counter,
                    key: "control_state-change".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "change".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 2 }
    );
}

#[test]
fn control_state_query_supports_first_last_and_detail_list() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, amount) in [(10, 5), (20, -2), (30, 7)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateIncrement {
                key: "control_state".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    for (aggregator, expected) in [("first", 5), ("last", 7)] {
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ControlStateQuery {
                        key: "control_state".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: aggregator.to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: expected }
        );
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateDetail {
                    key: "control_state".to_string(),
                    start_ms: 15,
                    end_ms: 40,
                    count: Some(2),
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"-2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 30,
                    value: b"7".to_vec(),
                },
            ]
        }
    );
}

#[test]
fn control_state_selection_matches_first_last_string_semantics() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for (occur_time_ms, value) in [(20, "middle"), (10, "first"), (30, "last")] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ControlStateSelectionSet {
                        key: "control_state-fol-first".to_string(),
                        value: value.as_bytes().to_vec(),
                        occur_time_ms,
                        ttl_ms: 60_000,
                        selection_type: ControlStateSelectionType::First,
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ControlStateSelectionSet {
                        key: "control_state-fol-last".to_string(),
                        value: value.as_bytes().to_vec(),
                        occur_time_ms,
                        ttl_ms: 60_000,
                        selection_type: ControlStateSelectionType::Last,
                    },
                })
                .status
                .ok
        );
    }

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateSelectionQuery {
                    key: "control_state-fol-first".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"first".to_vec()),
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateSelectionQuery {
                    key: "control_state-fol-last".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"last".to_vec()),
        }
    );
}

#[test]
fn control_state_selection_omitted_occur_time_resolves_to_now_like_native() {
    // FirstOrLastSet substitutes occur_time==0 with the current time before the FIRST/LAST
    // comparison; an omitted-occur-time FIRST set must NOT beat an earlier explicit record.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateSelectionSet {
                key: "k".to_string(),
                value: b"early".to_vec(),
                occur_time_ms: 5000,
                ttl_ms: 0,
                selection_type: ControlStateSelectionType::First,
            },
        })
        .status
        .ok);
    // occur_time 0 resolves to now (>> 5000), so this FIRST set must not overwrite.
    assert!(engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateSelectionSet {
                key: "k".to_string(),
                value: b"late".to_vec(),
                occur_time_ms: 0,
                ttl_ms: 0,
                selection_type: ControlStateSelectionType::First,
            },
        })
        .status
        .ok);
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateSelectionQuery {
                    key: "k".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"early".to_vec()),
        },
        "an omitted occur_time (0 -> now) FIRST set must not beat an earlier explicit record"
    );
}

#[test]
fn live_page_slab_ids_includes_control_state_pages() {
    // control_state_pages is page-backed; omitting it from the GC live set let a slab holding
    // only a control-state page be reclaimed while still index-referenced -> DataLoss on read.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let resp = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateSet {
            family: ControlStateFamily::Counter,
            key: "cs".to_string(),
            timestamp_ms: 1000,
            amount: 1,
        },
    });
    assert!(resp.status.ok, "{:?}", resp.status);
    assert!(
        !engine.live_page_slab_ids(1).is_empty(),
        "the control-state page's slab must be in the GC live set, else GC can reclaim a slab \
         still referenced by control_state_pages"
    );
}

#[test]
fn control_state_values_survive_unload_reload() {
    // Locks in the control_state reconcile merge (persisted-authoritative) together with the
    // control_state_pages GC live-set fix: control-state values must round-trip through a
    // reload unchanged.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for (timestamp_ms, amount) in [(10, 5), (20, -2), (30, 7)] {
        assert!(engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateIncrement {
                    key: "cs".to_string(),
                    timestamp_ms,
                    amount,
                },
            })
            .status
            .ok);
    }
    let query = |engine: &TemporalEngine, aggregator: &str| -> i64 {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateQuery {
                    key: "cs".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: aggregator.to_string(),
                },
            })
            .response
        {
            CommandResponse::Integer { value } => value,
            other => panic!("expected Integer, got {other:?}"),
        }
    };
    let before = ["sum", "first", "last", "count"].map(|agg| query(&engine, agg));
    assert_eq!(query(&engine, "sum"), 10, "5 - 2 + 7 = 10");

    engine.unload_shard(1);
    engine.load_shard(1);

    let after = ["sum", "first", "last", "count"].map(|agg| query(&engine, agg));
    assert_eq!(
        after, before,
        "control-state aggregates must survive reload unchanged"
    );
}

#[test]
fn sequence_rows_fold_into_shared_feature_storage_and_survive_reload() {
    // Thin-layer Sequence fold: Sequence is backed by the Feature timestamped-KV storage
    // (a single `features` map, no separate `sequences` map). Rows written via SequenceAdd
    // must be (a) readable through the typed SequenceQuery API, (b) physically present in
    // the shared feature storage (a Feature read over the same key sees the same
    // timestamps), and (c) preserved across an unload/reload cycle.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let rows = vec![
        SequenceFeatureRow {
            timestamp_ms: 100,
            gid: 7,
            action_type: 1,
            duration: 10,
            author_id: 900,
        },
        SequenceFeatureRow {
            timestamp_ms: 200,
            gid: 8,
            action_type: 2,
            duration: 20,
            author_id: 901,
        },
    ];
    assert!(engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: "seq-fold".to_string(),
                rows: rows.clone(),
            },
        })
        .status
        .ok);

    let query_rows = |engine: &TemporalEngine| -> Vec<SequenceFeatureRow> {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: "seq-fold".to_string(),
                    start_ms: 0,
                    end_ms: 1_000,
                    count: 10,
                    filters: Vec::new(),
                },
            })
            .response
        {
            CommandResponse::SequenceRows { rows } => rows,
            other => panic!("expected SequenceRows, got {other:?}"),
        }
    };
    assert_eq!(query_rows(&engine), rows, "SequenceQuery returns the rows");

    // Shared storage: the same key is visible through the Feature read path at the same
    // timestamps, proving the rows live in the `features` map rather than a separate
    // sequences map.
    let feature_ts: Vec<u64> = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "seq-fold".to_string(),
                start_ms: 0,
                end_ms: 1_000,
                count: None,
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => {
            points.iter().map(|point| point.timestamp_ms).collect()
        }
        other => panic!("expected FeaturePoints, got {other:?}"),
    };
    assert_eq!(
        feature_ts,
        vec![100, 200],
        "sequence rows are stored in the shared feature storage"
    );

    // Reload fidelity through the merged storage.
    engine.unload_shard(1);
    engine.load_shard(1);
    assert_eq!(
        query_rows(&engine),
        rows,
        "sequence rows survive an unload/reload cycle via feature storage"
    );
}

#[test]
fn insert_if_absent_keeps_the_first_in_batch_duplicate_timestamp() {
    // feature ADD FIRST policy walks the point list in
    // request order and skips a timestamp already present, so for an in-batch duplicate the FIRST
    // value wins. Rust previously pre-collapsed the batch by timestamp (last-wins) before the
    // policy loop, silently keeping the LAST duplicate. Same batch, same timestamp, InsertIfAbsent
    // on a not-yet-present key must land the first value ("A"), not the last ("B").
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: "first-wins".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 5,
                    value: b"A".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 5,
                    value: b"B".to_vec(),
                },
            ],
            policy: FeatureWritePolicy::InsertIfAbsent,
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "first-wins".to_string(),
                    start_ms: 0,
                    end_ms: 10,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 5,
                value: b"A".to_vec(),
            }]
        },
        "InsertIfAbsent must keep the FIRST in-batch duplicate value (FIRST policy), not the last"
    );
}

#[test]
fn feature_write_policy_sequence_batch_dimensions_and_control_state_precision_work() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "feature-policy".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"old".to_vec(),
            }],
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppendWithPolicy {
                    key: "feature-policy".to_string(),
                    points: vec![FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ignored".to_vec(),
                    }],
                    policy: FeatureWritePolicy::InsertIfAbsent,
                },
            })
            .response,
        CommandResponse::Integer { value: 0 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppendWithPolicy {
                    key: "feature-policy".to_string(),
                    points: vec![FeaturePoint {
                        timestamp_ms: 10,
                        value: b"new".to_vec(),
                    }],
                    policy: FeatureWritePolicy::ReplaceExisting,
                },
            })
            .response,
        CommandResponse::Integer { value: 1 }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "feature-policy".to_string(),
                    start_ms: 0,
                    end_ms: 20,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"new".to_vec(),
            }]
        }
    );

    for (key, gid, action_type) in [("seq-a", 1, 7), ("seq-b", 2, 8)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: key.to_string(),
                rows: vec![SequenceFeatureRow {
                    timestamp_ms: 100,
                    gid,
                    action_type,
                    duration: 5,
                    author_id: 9,
                }],
            },
        });
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceBatchQuery {
                    queries: vec![
                        SequenceQuerySpec {
                            key: "seq-a".to_string(),
                            start_ms: 0,
                            end_ms: 200,
                            count: 10,
                            filters: vec![FeatureFilter {
                                field: "action_type".to_string(),
                                op: FeatureFilterOp::Equal,
                                value: 7,
                            }],
                        },
                        SequenceQuerySpec {
                            key: "seq-b".to_string(),
                            start_ms: 0,
                            end_ms: 200,
                            count: 10,
                            filters: Vec::new(),
                        },
                    ],
                },
            })
            .response,
        CommandResponse::SequenceRowGroups {
            groups: vec![
                (
                    "seq-a".to_string(),
                    vec![SequenceFeatureRow {
                        timestamp_ms: 100,
                        gid: 1,
                        action_type: 7,
                        duration: 5,
                        author_id: 9,
                    }],
                ),
                (
                    "seq-b".to_string(),
                    vec![SequenceFeatureRow {
                        timestamp_ms: 100,
                        gid: 2,
                        action_type: 8,
                        duration: 5,
                        author_id: 9,
                    }],
                ),
            ],
        }
    );

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateIncrementWithOptions {
            key: "control_state-bucket".to_string(),
            timestamp_ms: 1_234,
            amount: 3,
            precision_ms: Some(1_000),
            ttl_ms: Some(60_000),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateIncrementWithOptions {
            key: "control_state-bucket".to_string(),
            timestamp_ms: 1_999,
            amount: 4,
            precision_ms: Some(1_000),
            ttl_ms: None,
        },
    });
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateDetail {
                    key: "control_state-bucket".to_string(),
                    start_ms: 0,
                    end_ms: 2_000,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 1_000,
                value: b"7".to_vec(),
            }]
        }
    );
    assert!(matches!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: "control_state-bucket".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value } if value > 0
    ));
}

#[test]
fn maxmemory_config_rejects_writes_when_storage_budget_is_exhausted() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            maxmemory_bytes: Some(0),
            ..Config::default()
        },
    });

    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "first".to_string(),
            value: b"y".to_vec(),
        },
    });
    assert_eq!(rejected.status.code, "storage_quota_exceeded");
}

#[test]
fn write_qps_config_rejects_writes_after_admission_limit() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            write_qps: Some(1),
            ..Config::default()
        },
    });
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "first".to_string(),
                    value: b"x".to_vec(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "second".to_string(),
            value: b"y".to_vec(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "write_qps limit exceeded");
}

#[test]
fn write_qps_zero_means_unlimited_not_deny_all_like_native() {
    // QuotaManager treats a configured qps of 0 as UNLIMITED (it installs no limiter
    // and ConsumeQuota always succeeds), NOT deny-all. The live admission path must not
    // push a limit==0 (which the downstream gate rejects as "is zero"). Regression for the
    // case where the >0 filter existed only in an orphaned, never-compiled module.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            write_qps: Some(0),
            read_qps: Some(0),
            ..Config::default()
        },
    });
    wait_for_fresh_admission_second();
    for i in 0..5 {
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: b"v".to_vec(),
            },
        });
        assert!(
            resp.status.ok,
            "write_qps=0 must be unlimited, not deny-all (got {:?})",
            resp.status
        );
    }
    // read_qps=0 likewise unlimited.
    for i in 0..5 {
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("k{i}"),
            },
        });
        assert!(resp.status.ok, "read_qps=0 must be unlimited (got {:?})", resp.status);
    }
}

#[test]
fn read_qps_config_rejects_reads_after_admission_limit() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "first".to_string(),
                    value: b"x".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            read_qps: Some(1),
            ..Config::default()
        },
    });
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "first".to_string(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "first".to_string(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "read_qps limit exceeded");
}

#[test]
fn table_write_qps_config_is_shared_across_loaded_table_shards() {
    let engine = TemporalEngine::default();
    for shard_id in [1, 2] {
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id,
                    load_version: 1,
                    local_node_id: Some(1),
                    shard_uri: format!("local://feature_table/{shard_id}"),
                    start_routing_bucket: 0,
                    end_routing_bucket: u32::MAX,
                    readonly: false,
                    table_name: "feature_table".to_string(),
                })
                .status
                .ok
        );
        engine.set_config(SetConfigRequest {
            shard_id,
            config: Config {
                version: 2,
                table_write_qps: Some(1),
                ..Config::default()
            },
        });
    }
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "first".to_string(),
                    value: b"x".to_vec(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringSet {
            key: "second".to_string(),
            value: b"y".to_vec(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "table_write_qps limit exceeded");
}

#[test]
fn tenant_read_qps_config_is_shared_across_tables() {
    let engine = TemporalEngine::default();
    for (shard_id, table_name, key) in [(1, "feature_table", "k1"), (2, "control_state_table", "k2")] {
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id,
                    load_version: 1,
                    local_node_id: Some(1),
                    shard_uri: format!("local://{table_name}/{shard_id}"),
                    start_routing_bucket: 0,
                    end_routing_bucket: u32::MAX,
                    readonly: false,
                    table_name: table_name.to_string(),
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"value".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.set_config(SetConfigRequest {
            shard_id,
            config: Config {
                version: 2,
                tenant_name: Some("tenant-a".to_string()),
                tenant_read_qps: Some(1),
                ..Config::default()
            },
        });
    }
    wait_for_fresh_admission_second();

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k1".to_string(),
                },
            })
            .status
            .ok
    );
    let rejected = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringGet {
            key: "k2".to_string(),
        },
    });
    assert_eq!(rejected.status.code, "admission_rejected");
    assert_eq!(rejected.status.message, "tenant_read_qps limit exceeded");
}

#[test]
fn stats_include_style_partition_and_object_manager_accounting() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 9,
                load_version: 77,
                local_node_id: Some(3),
                shard_uri: "local://table/shard-9".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 20,
                readonly: false,
                table_name: "feature_table".to_string(),
            })
            .status
            .ok
    );
    for command in [
        Command::StringSet {
            key: "string-key".to_string(),
            value: b"v".to_vec(),
        },
        Command::HashSet {
            key: "hash-key".to_string(),
            field: "a".to_string(),
            value: b"1".to_vec(),
        },
        Command::HashSet {
            key: "hash-key".to_string(),
            field: "b".to_string(),
            value: b"2".to_vec(),
        },
        Command::SetAdd {
            key: "set-key".to_string(),
            member: b"m1".to_vec(),
        },
        Command::SetAdd {
            key: "set-key".to_string(),
            member: b"m2".to_vec(),
        },
        Command::FeatureAppend {
            key: "feature-key".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 1,
                    value: b"f1".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 2,
                    value: b"f2".to_vec(),
                },
            ],
        },
        Command::SequenceAdd {
            key: "sequence-key".to_string(),
            rows: vec![
                SequenceFeatureRow {
                    timestamp_ms: 10,
                    gid: 1,
                    action_type: 2,
                    duration: 3,
                    author_id: 4,
                },
                SequenceFeatureRow {
                    timestamp_ms: 20,
                    gid: 5,
                    action_type: 6,
                    duration: 7,
                    author_id: 8,
                },
            ],
        },
        Command::ControlStateSet {
            family: ControlStateFamily::Distinct,
            key: "control_state-key".to_string(),
            timestamp_ms: 40,
            amount: 5,
        },
    ] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 9,
                    command,
                })
                .status
                .ok
        );
    }

    let stats = engine.get_stats(9).stats.unwrap();
    assert_eq!(stats.total_records, 6);
    assert_eq!(stats.object_manager.object_count, 6);
    assert_eq!(stats.object_manager.page_ref_count, 9);
    assert_eq!(stats.object_manager.dirty_object_count, 6);
    assert!(stats.object_manager.dirty_bucket_count > 0);
    assert!(stats.object_manager.dirty_bucket_count <= 6);
    assert_eq!(stats.object_manager.routing_bucket_count, 11);
    assert_eq!(stats.shard_stat_info.table_name, "feature_table");
    assert_eq!(stats.shard_stat_info.shard_uri, "local://table/shard-9");
    assert_eq!(stats.shard_stat_info.start_routing_bucket, 10);
    assert_eq!(stats.shard_stat_info.end_routing_bucket, 20);
    assert_eq!(stats.shard_stat_info.object_manager, stats.object_manager);
    assert!(stats.block_store_bands.active_bands >= 1);
    assert!(stats.block_store_bands.active_physical_bytes > 0);
    assert_eq!(
        stats.block_store_bands.live_physical_bytes,
        stats.block_store_bands.active_physical_bytes
            + stats.block_store_bands.sealed_physical_bytes
    );
}

#[test]
fn prometheus_metrics_include_records_cache_page_and_wal() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    engine.block_store().roll_slab().unwrap();

    let metrics = engine.prometheus_metrics();
    assert!(metrics.contains("temporalstore_shard_records{shard_id=\"1\",kind=\"string\"} 1"));
    assert!(metrics.contains("temporalstore_cache_operations_total"));
    assert!(metrics.contains(
        "temporalstore_cache_operations_total{shard_id=\"1\",kind=\"memory_evictions\"}"
    ));
    assert!(metrics.contains("temporalstore_block_store_operations_total"));
    assert!(metrics
        .contains("temporalstore_block_store_band_count{shard_id=\"1\",state=\"sealed\"} 1"));
    assert!(
        metrics.contains("temporalstore_block_store_band_bytes{shard_id=\"1\",kind=\"live\"}")
    );
    assert!(metrics
        .contains("temporalstore_block_store_band_bytes{shard_id=\"1\",kind=\"total_known\"}"));
    assert!(metrics.contains(
        "temporalstore_block_store_band_oldest_unix_ms{shard_id=\"1\",scope=\"known\"}"
    ));
    assert!(metrics.contains(
        "temporalstore_block_store_band_oldest_unix_ms{shard_id=\"1\",scope=\"live\"}"
    ));
    assert!(metrics.contains(
        "temporalstore_block_store_band_oldest_age_ms{shard_id=\"1\",scope=\"known\"}"
    ));
    assert!(metrics
        .contains("temporalstore_block_store_band_oldest_age_ms{shard_id=\"1\",scope=\"live\"}"));
    assert!(metrics.contains("temporalstore_wal_records_total{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_wal_records_total{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_object_manager_objects{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_object_manager_page_refs{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_object_manager_dirty_objects{shard_id=\"1\"} 1"));
    assert!(metrics.contains("temporalstore_storage_slot_page_refs{shard_id=\"1\""));
    assert!(metrics.contains("temporalstore_storage_slot_bytes{shard_id=\"1\""));
    assert!(metrics.contains("temporalstore_storage_slot_dirty_objects{shard_id=\"1\""));
    assert!(metrics.contains("temporalstore_partition_routing_slots{shard_id=\"1\"} 4294967295"));
}

#[test]
fn bucket_storage_summaries_track_live_refs_dirty_buckets_and_manifest_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });
    for key in ["alpha", "beta", "gamma"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    },
                })
                .status
                .ok
        );
    }

    let summaries = engine.bucket_storage_summaries(1);
    assert!(!summaries.is_empty());
    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.page_ref_count)
            .sum::<u64>(),
        3
    );
    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.dirty_object_count)
            .sum::<u64>(),
        3
    );
    let dirty_bucket = summaries
        .iter()
        .find(|summary| summary.dirty_object_count > 0)
        .unwrap()
        .routing_bucket;
    let manifest = engine
        .create_bucket_dump_manifest(1, [dirty_bucket])
        .expect("slot dump manifest should persist");
    engine.validate_bucket_dump_manifest(&manifest).unwrap();
    let summaries = engine.bucket_storage_summaries(1);
    assert!(summaries
        .iter()
        .filter(|summary| summary.routing_bucket == dirty_bucket)
        .all(|summary| summary.last_dump_sequence == manifest.index_log_sequence));
}

#[test]
fn rebuild_bucket_page_ownership_preserves_dirty_watermarks() {
    // rebuild clears + rebuilds bucket_map from the model maps; it must carry over the durable
    // per-bucket dirty_generation / last_dump_sequence. Rebuilding them from BucketNode::default()
    // (as manifest-install / promote do) zeroed the watermarks, making a restored shard mismatch
    // its own dump-manifest generation and forcing unnecessary re-dumps.
    let mut shard = ShardState::default();
    shard.strings.insert(
        "k".to_string(),
        BlockAddress::from_parts(1, 0, 4, Some(1), Some(30), Some(3), Some(1), None),
    );
    shard.bucket_index.bucket_map.insert(
        3,
        BucketNode {
            routing_bucket: 3,
            meta_loaded: true,
            dirty_generation: 7,
            last_dump_sequence: 4,
            ..BucketNode::default()
        },
    );
    rebuild_bucket_page_ownership(1, &mut shard, 0, u32::MAX);
    let bucket = shard
        .bucket_index
        .bucket_map
        .get(&3)
        .expect("bucket 3 should be rebuilt from the string page");
    assert!(!bucket.page_index.is_empty(), "the page should be re-indexed");
    assert_eq!(
        bucket.dirty_generation, 7,
        "dirty_generation must survive the rebuild"
    );
    assert_eq!(
        bucket.last_dump_sequence, 4,
        "last_dump_sequence must survive the rebuild"
    );
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn bucket_page_ownership_is_first_class_and_survives_reload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });
    for field in ["field-a", "field-b"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "hash-key".to_string(),
                        field: field.to_string(),
                        value: field.as_bytes().to_vec(),
                    },
                })
                .status
                .ok
        );
    }

    let physical_before_reload = engine.storage_physical_index_report(1);
    assert!(physical_before_reload.bucket_index_authority);
    assert_eq!(physical_before_reload.page_index_count, 2);
    assert_eq!(physical_before_reload.dirty_bucket_count, 1);
    assert_eq!(physical_before_reload.missing_object_id_count, 0);
    assert_eq!(physical_before_reload.missing_routing_bucket_count, 0);
    assert!(physical_before_reload.bucket_nodes.iter().any(|bucket| {
        bucket.page_ref_count == 2
            && bucket.object_count == 2
            && bucket.dirty_generation >= 2
            && bucket.page_indexes.iter().all(|page| {
                page.model_id == "hash" && page.dirty && !page.deleted && !page.log_backed
            })
    }));
    assert_eq!(
        engine
            .bucket_storage_summaries(1)
            .iter()
            .map(|summary| summary.object_count)
            .sum::<u64>(),
        2
    );
    let ownership = engine.bucket_object_page_ownership_report(1);
    assert!(ownership.first_class_index_present);
    assert!(!ownership.derived_from_model_maps);
    assert_eq!(ownership.page_ref_count, 2);
    assert_eq!(ownership.missing_owner_page_ref_count, 0);
    assert_eq!(ownership.owner_mismatch_page_ref_count, 0);
    let physical = engine.storage_physical_index_report(1);
    assert!(physical.bucket_index_authority);
    assert_eq!(physical.page_index_count, 2);
    assert_eq!(physical.dirty_bucket_count, 1);

    engine.unload_shard(1);
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });
    let physical_after_reload = engine.storage_physical_index_report(1);
    assert!(physical_after_reload.bucket_index_authority);
    assert_eq!(physical_after_reload.page_index_count, 2);
    assert_eq!(physical_after_reload.dirty_bucket_count, 0);
    assert!(physical_after_reload
        .bucket_nodes
        .iter()
        .any(|bucket| bucket.page_ref_count == 2 && bucket.object_count == 2));
    let reloaded_ownership = engine.bucket_object_page_ownership_report(1);
    assert!(reloaded_ownership.first_class_index_present);
    assert!(!reloaded_ownership.derived_from_model_maps);
    assert_eq!(reloaded_ownership.page_ref_count, 2);
    assert_eq!(reloaded_ownership.missing_owner_page_ref_count, 0);
    assert_eq!(reloaded_ownership.owner_mismatch_page_ref_count, 0);
    let reloaded_physical = engine.storage_physical_index_report(1);
    assert!(reloaded_physical.bucket_index_authority);
    assert_eq!(reloaded_physical.page_index_count, 2);
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn bucket_index_is_authoritative_when_secondary_views_are_missing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "slot-authority".to_string(),
                    value: b"slot-value".to_vec(),
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard loaded");
        assert!(!shard.bucket_index.bucket_map.is_empty());
        shard.strings.clear();
        shard.hashes.clear();
        shard.sets.clear();
    }

    let exists = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExists {
            key: "slot-authority".to_string(),
        },
    });
    assert!(exists.status.ok);
    assert_eq!(exists.response, CommandResponse::Integer { value: 1 });

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "slot-authority".to_string(),
        },
    });
    assert!(get.status.ok);
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"slot-value".to_vec())
        }
    );
}

// shared-corpus: storage_recovery_reconciles_bucket_index_to_model_views
#[test]
fn storage_recovery_uses_bucket_index_not_stale_secondary_model_maps() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "slot-authority".to_string(),
                    value: b"authoritative".to_vec(),
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard loaded");
        assert!(!shard.bucket_index.bucket_map.is_empty());
        let stale = shard
            .strings
            .get_mut("slot-authority")
            .expect("secondary string view");
        stale.set_object_id(Some(stale.object_id().unwrap_or_default().wrapping_add(99)));
        stale.set_routing_bucket(Some(stale.routing_bucket().unwrap_or_default().wrapping_add(99)));
        stale.page_slab_id = stale.page_slab_id.wrapping_add(999);
    }

    let recovery = engine.storage_recovery_report(1);
    assert_eq!(recovery.total_page_refs, 1);
    assert_eq!(recovery.readable_page_refs, 1);
    assert!(recovery.all_live_pages_readable);
    assert!(recovery.owner_mismatch_page_refs.is_empty());
    assert_eq!(recovery.missing_owner_page_refs, 0);
    assert_eq!(recovery.object_lifecycle.owner_mismatch_page_refs, 0);
    assert_eq!(recovery.slab_integrity.owner_mismatch_page_ref_count, 0);
    assert!(recovery.slab_integrity.integrity_ok);
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn legacy_model_maps_are_promoted_to_bucket_index_authority() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "legacy-map".to_string(),
                    value: b"promoted".to_vec(),
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard loaded");
        assert!(!shard.strings.is_empty());
        shard.bucket_index.bucket_map.clear();
    }

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "legacy-map".to_string(),
        },
    });
    assert!(get.status.ok);
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"promoted".to_vec())
        }
    );
    let physical = engine.storage_physical_index_report(1);
    assert!(physical.bucket_index_authority);
    assert_eq!(physical.page_index_count, 1);
    assert_eq!(physical.missing_object_id_count, 0);
    assert_eq!(physical.missing_routing_bucket_count, 0);
}

// shared-corpus: storage_cold_read_page_address_fallback
#[test]
fn cold_read_uses_bucket_page_address_after_cache_and_model_maps_are_cleared() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "cold-slot-read".to_string(),
                    value: b"from-disk".to_vec(),
                },
            })
            .status
            .ok
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard loaded");
        assert!(!shard.bucket_index.bucket_map.is_empty());
        shard.strings.clear();
    }
    let _ = engine
        .cache()
        .invalidate(&CacheKey::string(1, "cold-slot-read"));
    engine.cache().clear_memory_for_test();
    let block_reads_before = engine.block_store().stats().reads;

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "cold-slot-read".to_string(),
        },
    });
    assert!(get.status.ok);
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"from-disk".to_vec())
        }
    );
    assert!(engine.block_store().stats().reads > block_reads_before);
}

// shared-corpus: storage_recovery_reconciles_bucket_index_to_model_views
#[test]
fn recovery_reconciles_model_views_from_bucket_index_authority() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1024, &cache_dir, &page_dir, &index_dir);
    engine.load_shard(1);
    assert!(
        engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "recover-slot-view".to_string(),
                    value: b"view".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.unload_shard(1);

    // Empty the string map and put it back through the same encoder that wrote it. Injecting the
    // fault as JSON text quietly required the index to BE JSON, which is not what this test is
    // about -- it is about recovery rebuilding the strings from the bucket index.
    let index_path = index_dir.join("shard-1.index.json");
    let mut damaged =
        crate::engine::decode_index_bytes(&std::fs::read(&index_path).expect("index file"))
            .expect("served index decodes");
    damaged.strings.clear();
    std::fs::write(&index_path, crate::engine::encode_index_bytes(&damaged)).unwrap();

    let recovered = TemporalEngine::with_local_dirs(1024, &cache_dir, &page_dir, &index_dir);
    recovered.load_shard(1);
    {
        let shards = recovered.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("recovered shard");
        assert!(shard.strings.contains_key("recover-slot-view"));
    }
    let get = recovered.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "recover-slot-view".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"view".to_vec())
        }
    );
}

// shared-corpus: storage_dump_load_recovery
#[test]
fn core_index_loads_legacy_bucket_page_field_names() {
    let legacy_json = r#"{
        "slots": {
            "7": {
                "routing_slot": 7,
                "layout": "SinglePageObject",
                "dirty": false,
                "meta_loaded": true,
                "loading": false,
                "in_memory": true,
                "ttl_ms": null,
                "dirty_generation": 3,
                "last_dump_sequence": 11,
                "object_ids": [42],
                "page_refs": {
                    "string:k::1:2": {
                        "object_key": "k",
                        "model_id": "string",
                        "object_id": 42,
                        "address": {
                            "page_segment_id": 1,
                            "offset": 2,
                            "length": 3,
                            "page_id": 4,
                            "object_id": 42,
                            "routing_slot": 7
                        },
                        "dirty": false,
                        "deleted": false,
                        "log_backed": true
                    }
                }
            }
        }
    }"#;

    let index: CoreIndex = serde_json::from_str(legacy_json).unwrap();
    let bucket = index.bucket_map.get(&7).expect("legacy slot should load");
    assert!(bucket.object_index.contains(&42));
    assert_eq!(bucket.page_index.len(), 1);
    assert_eq!(
        bucket.page_index
            .values()
            .next()
            .expect("legacy page index should load")
            .address
            .routing_bucket(),
        Some(7)
    );
}

// shared-corpus: storage_object_page_bucket_parity_surfaces storage_bucket_layout_transitions;
#[test]
fn bucket_store_reports_all_layout_states_and_runtime_flags() {
    let mut shard = ShardState::default();
    shard.bucket_index.bucket_map.insert(
        1,
        BucketNode {
            routing_bucket: 1,
            meta_loaded: true,
            ..BucketNode::default()
        },
    );
    shard.bucket_index.bucket_map.insert(
        2,
        BucketNode {
            routing_bucket: 2,
            layout: BucketLayoutState::SingleObject,
            dirty: true,
            deleted: true,
            meta_loaded: true,
            in_memory: false,
            ttl_ms: Some(5_000),
            dirty_generation: 7,
            object_index: [20].into_iter().collect(),
            ..BucketNode::default()
        },
    );
    shard.bucket_index.bucket_map.insert(
        3,
        BucketNode {
            routing_bucket: 3,
            layout: BucketLayoutState::SinglePageObject,
            meta_loaded: true,
            in_memory: true,
            object_index: [30].into_iter().collect(),
            page_index: [(
                "string:k::1:0".to_string(),
                PageIndex {
                    object_key: Arc::from("k".to_string()),
                    model_id: Arc::from("string".to_string()),
                    component: None,
                    address: BlockAddress::from_parts(1, 0, 4, Some(1), Some(30), Some(3), Some(1), None),
                    dirty: false,
                    deleted: false,
                    log_backed: true,
                },
            )]
            .into_iter()
            .map(|(_key, page): (String, PageIndex)| page)
            .collect(),
            ..BucketNode::default()
        },
    );
    shard.bucket_index.bucket_map.insert(
        4,
        BucketNode {
            routing_bucket: 4,
            layout: BucketLayoutState::MultiPageObject,
            meta_loaded: true,
            loading: true,
            in_memory: true,
            object_index: [40].into_iter().collect(),
            page_index: [
                (
                    "feature:k::2:0".to_string(),
                    PageIndex {
                        object_key: Arc::from("feature-key".to_string()),
                        model_id: Arc::from("feature".to_string()),
                        component: None,
                        address: BlockAddress::from_parts(2, 0, 4, Some(2), Some(40), Some(4), Some(2), None),
                        dirty: false,
                        deleted: false,
                        log_backed: true,
                    },
                ),
                (
                    "feature:k::2:4".to_string(),
                    PageIndex {
                        object_key: Arc::from("feature-key".to_string()),
                        model_id: Arc::from("feature".to_string()),
                        component: None,
                        address: BlockAddress::from_parts(2, 4, 4, Some(3), Some(40), Some(4), Some(3), None),
                        dirty: false,
                        deleted: false,
                        log_backed: true,
                    },
                ),
            ]
            .into_iter()
            .map(|(_key, page): (String, PageIndex)| page)
            .collect(),
            ..BucketNode::default()
        },
    );
    shard.bucket_index.bucket_map.insert(
        5,
        BucketNode {
            routing_bucket: 5,
            layout: BucketLayoutState::MultiObject,
            meta_loaded: true,
            in_memory: true,
            object_index: [50, 51].into_iter().collect(),
            page_index: [
                (
                    "hash:k:a:3:0".to_string(),
                    PageIndex {
                        object_key: Arc::from("hash-key".to_string()),
                        model_id: Arc::from("hash".to_string()),
                        component: Some(Arc::from("a".to_string())),
                        address: BlockAddress::from_parts(3, 0, 1, Some(4), Some(50), Some(5), Some(4), None),
                        dirty: false,
                        deleted: false,
                        log_backed: true,
                    },
                ),
                (
                    "hash:k:b:3:1".to_string(),
                    PageIndex {
                        object_key: Arc::from("hash-key".to_string()),
                        model_id: Arc::from("hash".to_string()),
                        component: Some(Arc::from("b".to_string())),
                        address: BlockAddress::from_parts(3, 1, 1, Some(5), Some(51), Some(5), Some(5), None),
                        dirty: false,
                        deleted: false,
                        log_backed: true,
                    },
                ),
            ]
            .into_iter()
            .map(|(_key, page): (String, PageIndex)| page)
            .collect(),
            ..BucketNode::default()
        },
    );

    let report = bucket_store::runtime_report(&shard);
    assert_eq!(report.empty_buckets, 1);
    assert_eq!(report.single_object_buckets, 1);
    assert_eq!(report.single_page_object_buckets, 1);
    assert_eq!(report.multi_page_object_buckets, 1);
    assert_eq!(report.multi_object_buckets, 1);
    assert_eq!(report.deleted_bucket_count, 1);
    assert_eq!(report.loading_bucket_count, 1);
    assert_eq!(report.ttl_bucket_count, 1);
    assert_eq!(report.in_memory_bucket_count, 3);
    assert_eq!(report.max_dirty_generation, 7);
}

#[test]
fn control_state_set_and_get_with_options_buckets_by_precision() {
    // HSETANDGET conformance: precision floors the write into a single bucket, and
    // the atomic increment-then-read returns the post-increment windowed aggregate.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let day = 1_784_851_200_000u64;
    let precision = 86_400_000u64; // one-day buckets
    let mut last = 0;
    for ts in [day + 10, day + 5_000, day + 86_000_000] {
        last = match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateSetAndGetWithOptions {
                    family: ControlStateFamily::Counter,
                    key: "cap:u1:c1".to_string(),
                    timestamp_ms: ts,
                    amount: 1,
                    start_ms: day,
                    end_ms: day + precision - 1,
                    aggregator: "sum".to_string(),
                    precision_ms: Some(precision),
                    ttl_ms: Some(precision),
                    uuid: None,
                },
            })
            .response
        {
            CommandResponse::Integer { value } => value,
            other => panic!("expected Integer, got {other:?}"),
        };
    }
    // Three increments in the same day window -> count 3, all folded into one bucket.
    assert_eq!(last, 3);
    let series_buckets = engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateDetail {
                key: "control_state:h:cap:u1:c1".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        })
        .response;
    match series_buckets {
        CommandResponse::FeaturePoints { points } => assert_eq!(points.len(), 1),
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

#[test]
fn control_state_set_and_get_with_options_is_idempotent_on_uuid_replay() {
    // At-least-once queue replay: the same uuid within the dedup window must not
    // double-count, and must return the current windowed aggregate unchanged.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let day = 1_784_851_200_000u64;
    let call = |uuid: &str| {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateSetAndGetWithOptions {
                    family: ControlStateFamily::Counter,
                    key: "quota:t1".to_string(),
                    timestamp_ms: day + 1,
                    amount: 1,
                    start_ms: day,
                    end_ms: day + 86_400_000,
                    aggregator: "sum".to_string(),
                    precision_ms: Some(86_400_000),
                    ttl_ms: None,
                    uuid: Some(uuid.to_string()),
                },
            })
            .response
        {
            CommandResponse::Integer { value } => value,
            other => panic!("expected Integer, got {other:?}"),
        }
    };
    assert_eq!(call("evt-1"), 1); // first delivery
    assert_eq!(call("evt-1"), 1); // duplicate replay -> no double count
    assert_eq!(call("evt-1"), 1); // still deduped
    assert_eq!(call("evt-2"), 2); // distinct event increments
    assert_eq!(call("evt-2"), 2); // its replay deduped
}

// shared-corpus: storage_object_page_bucket_parity_surfaces storage_object_hot_cold_reload;
