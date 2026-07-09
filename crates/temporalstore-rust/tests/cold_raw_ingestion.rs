use temporalstore_rust::{
    Command, CommandResponse, ContextEvent, ContextExtractedEventIndexes, ExecuteRequest,
    ScanStreamRequest, StreamKind, TemporalEngine, WriteAheadLogRecord,
};

fn cold_event(event_id_hash: u64, event_time_ms: u64, text: &str) -> ContextEvent {
    ContextEvent {
        event_id_hash,
        event_time_ms,
        ingestion_time_ms: event_time_ms,
        kind: 7,
        event_type: 7,
        actor_hash: 0,
        status: 0,
        valid_until_ms: 0,
        confidence: 0.95,
        importance: 0.85,
        text: text.to_string(),
        source_ref: "backfill://raw".to_string(),
        related_node_hashes: Vec::new(),
        compact_attrs: Vec::new(),
    }
}

#[test]
fn cold_raw_ingestion_writes_wal_and_avoids_cache_promotion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    const TENANT: u64 = 3010;
    const NODE: u64 = 9200;
    const START: u64 = 1_782_400_000_000;

    let cache_puts_before = engine.cache().stats().puts;
    let block_writes_before = engine.block_store().stats().writes;
    for idx in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: cold_event(8000 + idx, START + idx * 10, &format!("cold raw {idx}")),
                first_write_only: false,
                cold_storage: true,
            },
        });
        assert!(response.status.ok);
    }

    assert!(engine.block_store().stats().writes > block_writes_before);
    assert_eq!(engine.cache().stats().puts, cache_puts_before);

    let extracted = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteExtractedEvent {
            tenant_hash: TENANT,
            node_hash: NODE,
            event: cold_event(9001, START + 30, "cold extracted"),
            indexes: ContextExtractedEventIndexes {
                scope_hash: 77,
                entity_hashes: vec![901, 902],
                status_hash: 55,
                source_hash: 66,
                event_time_bucket_ms: START,
                disabled_indexes: Vec::new(),
            },
            first_write_only: false,
            cold_storage: true,
        },
    });
    assert!(extracted.status.ok);
    assert!(matches!(
        extracted.response,
        CommandResponse::ContextExtractedEventWrite {
            written_index_count,
            ..
        } if written_index_count >= 4
    ));
    assert_eq!(engine.cache().stats().puts, cache_puts_before);

    let wal_scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        page_segment_id: 0,
        start_offset: 0,
        end_offset: 64 * 1024,
        max_bytes: 64 * 1024,
    });
    assert!(wal_scan.status.ok);
    let wal_records = wal_scan
        .records
        .iter()
        .map(|record| serde_json::from_slice::<WriteAheadLogRecord>(&record.data).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(wal_records.len(), 4);
    assert!(wal_records.iter().take(3).all(|record| matches!(
        record.command,
        Command::ContextWriteEvent {
            cold_storage: true,
            ..
        }
    )));
    assert!(matches!(
        wal_records.last().unwrap().command,
        Command::ContextWriteExtractedEvent {
            cold_storage: true,
            ..
        }
    ));
    assert_eq!(engine.cache().stats().puts, cache_puts_before);

    let block_reads_before = engine.block_store().stats().reads;
    let compressed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextCompressEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            source_start_ms: START,
            source_end_ms: START + 30,
            compressed_time_ms: START + 1_000,
            max_source_events: Some(4),
            min_confidence: 0.9,
            min_importance: 0.8,
        },
    });
    assert!(compressed.status.ok);
    assert!(engine.block_store().stats().reads > block_reads_before);
    assert_eq!(engine.cache().stats().puts, cache_puts_before);
}
