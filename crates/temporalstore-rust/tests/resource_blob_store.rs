// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Integration coverage for the engine-owned attachment blob store.
//!
//! One TemporalStore holds everything: chunks stay searchable in records, and the FULL
//! original attachment is stored beside the engine and fetchable again by its
//! `temporalstore://resources/{tenant}/{content-hash}` URI. These tests drive the public
//! command surface to prove:
//!   * a multi-part upload commits to a content-addressed URI and fetches back byte-identical,
//!   * the single-shot put equals begin+append+commit and re-putting identical content
//!     lands on the same URI (dedup by content),
//!   * range fetches honor offset/length and report eof exactly at the end,
//!   * the sweep deletes only unreferenced blobs, never referenced ones, and a fresh blob
//!     survives even when unreferenced (the manifest may not have landed yet),
//!   * URI parsing is strict, so a fetch cannot smuggle a path,
//!   * an oversized resource ingested through the context workflow gets a REAL external
//!     URI whose bytes fetch back as the original payload.

use std::path::PathBuf;

use temporalstore_rust::{
    ingest_resource_skill_context, Command, CommandResponse, ContextResourceSkillIngestRequest,
    ExecuteRequest, TemporalEngine,
};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

const SHARD_ID: u64 = 1;
const TENANT: u64 = 77;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-resource-blobs-{name}-{pid}"));
    root
}

fn new_engine(name: &str) -> TemporalEngine {
    let root = unique_root(name);
    let _ = std::fs::remove_dir_all(&root);
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD_ID);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command,
    });
    assert!(
        response.status.ok,
        "command failed: {:?}",
        response.status
    );
    response.response
}

fn put(engine: &TemporalEngine, payload: &[u8]) -> (String, u64, u64) {
    match run(
        engine,
        Command::ContextResourceBlobPut {
            tenant_hash: TENANT,
            payload_base64: BASE64.encode(payload),
        },
    ) {
        CommandResponse::ContextResourceBlobCommitted {
            uri,
            size_bytes,
            content_hash,
        } => (uri, size_bytes, content_hash),
        other => panic!("unexpected response: {other:?}"),
    }
}

fn fetch(engine: &TemporalEngine, uri: &str, offset: u64, length: u64) -> (Vec<u8>, u64, bool) {
    match run(
        engine,
        Command::ContextResourceBlobFetch {
            uri: uri.to_string(),
            offset,
            length,
        },
    ) {
        CommandResponse::ContextResourceBlobChunk {
            payload_base64,
            total_size,
            eof,
        } => (
            BASE64.decode(payload_base64).expect("valid base64"),
            total_size,
            eof,
        ),
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
fn multipart_upload_commits_and_fetches_back_byte_identical() {
    let engine = new_engine("multipart");
    let part_a = vec![7u8; 300_000];
    let part_b = vec![9u8; 200_000];

    let token = match run(
        &engine,
        Command::ContextResourceBlobBegin {
            tenant_hash: TENANT,
        },
    ) {
        CommandResponse::ContextResourceBlobUpload { upload_token, .. } => upload_token,
        other => panic!("unexpected response: {other:?}"),
    };
    for part in [&part_a, &part_b] {
        match run(
            &engine,
            Command::ContextResourceBlobAppend {
                tenant_hash: TENANT,
                upload_token: token.clone(),
                payload_base64: BASE64.encode(part),
            },
        ) {
            CommandResponse::ContextResourceBlobUpload { .. } => {}
            other => panic!("unexpected response: {other:?}"),
        }
    }
    let (uri, size, _hash) = match run(
        &engine,
        Command::ContextResourceBlobCommit {
            tenant_hash: TENANT,
            upload_token: token,
        },
    ) {
        CommandResponse::ContextResourceBlobCommitted {
            uri,
            size_bytes,
            content_hash,
        } => (uri, size_bytes, content_hash),
        other => panic!("unexpected response: {other:?}"),
    };
    assert_eq!(size, (part_a.len() + part_b.len()) as u64);

    let (bytes, total, eof) = fetch(&engine, &uri, 0, 0);
    assert_eq!(total, size);
    assert!(eof);
    let mut expected = part_a.clone();
    expected.extend_from_slice(&part_b);
    assert_eq!(bytes, expected);
}

#[test]
fn identical_content_lands_on_the_same_uri() {
    let engine = new_engine("dedup");
    let payload = b"the same attachment bytes".repeat(1000);
    let (first_uri, first_size, _) = put(&engine, &payload);
    let (second_uri, second_size, _) = put(&engine, &payload);
    assert_eq!(first_uri, second_uri);
    assert_eq!(first_size, second_size);
    let (different_uri, _, _) = put(&engine, b"different bytes");
    assert_ne!(first_uri, different_uri);
}

#[test]
fn range_fetches_honor_offset_length_and_eof() {
    let engine = new_engine("ranges");
    let payload: Vec<u8> = (0..=255u8).cycle().take(10_000).collect();
    let (uri, _, _) = put(&engine, &payload);

    let (bytes, total, eof) = fetch(&engine, &uri, 100, 50);
    assert_eq!(total, 10_000);
    assert_eq!(bytes, &payload[100..150]);
    assert!(!eof);

    let (bytes, _, eof) = fetch(&engine, &uri, 9_990, 100);
    assert_eq!(bytes, &payload[9_990..]);
    assert!(eof, "a read reaching the last byte reports eof");

    let (bytes, total, eof) = fetch(&engine, &uri, 20_000, 10);
    assert!(bytes.is_empty());
    assert_eq!(total, 10_000);
    assert!(eof, "a read past the end is empty and eof");
}

#[test]
fn sweep_deletes_only_old_unreferenced_blobs() {
    let engine = new_engine("sweep");
    let (kept_uri, _, kept_hash) = put(&engine, b"referenced attachment");
    let (dropped_uri, _, _dropped_hash) = put(&engine, b"orphaned attachment");

    // With min_age_ms high, NOTHING is old enough -- both fresh blobs survive even though one
    // is unreferenced: its manifest may simply not have landed yet.
    match run(
        &engine,
        Command::ContextResourceBlobSweep {
            tenant_hash: TENANT,
            referenced_content_hashes: vec![kept_hash],
            min_age_ms: 3_600_000,
        },
    ) {
        CommandResponse::ContextResourceBlobSwept { deleted, .. } => assert_eq!(deleted, 0),
        other => panic!("unexpected response: {other:?}"),
    }
    fetch(&engine, &dropped_uri, 0, 0);

    // With min_age_ms zero, only the unreferenced blob goes.
    match run(
        &engine,
        Command::ContextResourceBlobSweep {
            tenant_hash: TENANT,
            referenced_content_hashes: vec![kept_hash],
            min_age_ms: 0,
        },
    ) {
        CommandResponse::ContextResourceBlobSwept { scanned, deleted } => {
            assert_eq!(scanned, 2);
            assert_eq!(deleted, 1);
        }
        other => panic!("unexpected response: {other:?}"),
    }
    let (bytes, _, _) = fetch(&engine, &kept_uri, 0, 0);
    assert_eq!(bytes, b"referenced attachment");
    let dropped = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextResourceBlobFetch {
            uri: dropped_uri,
            offset: 0,
            length: 0,
        },
    });
    assert!(!dropped.status.ok, "the swept blob must be gone");
}

#[test]
fn uri_parsing_is_strict() {
    let engine = new_engine("uris");
    for bad in [
        "temporalstore://resources/../../etc/passwd",
        "temporalstore://resources/0000000000000047/short",
        "temporalstore://resources/0000000000000047/00000000DEADBEEF", // uppercase
        "objectstore://matrixark/resources/0000000000000000.bin",
        "temporalstore://resources/0000000000000047",
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextResourceBlobFetch {
                uri: bad.to_string(),
                offset: 0,
                length: 0,
            },
        });
        assert!(!response.status.ok, "accepted a malformed uri: {bad}");
    }
}

#[test]
fn oversized_resource_ingest_stores_a_real_fetchable_attachment() {
    let engine = new_engine("ingest");
    // Comfortably past the 1 MiB inline cap.
    let payload = "attachment line: the full original must be fetchable again.\n".repeat(40_000);
    assert!(payload.len() > 1024 * 1024);

    let request_json = serde_json::json!({
        "shard_id": SHARD_ID,
        "tenant_hash": TENANT,
        "resources": [{
            "raw_uri": "file:///tmp/big-attachment.txt",
            "text": payload,
            // Big chunks keep the extract fan-out small -- this test is about the BLOB, and the
            // default 1400-char chunking would push a 2.4MB payload through ~1700 extractions.
            "max_chunk_chars": 200_000,
        }],
        "start_time_ms": 1,
        "end_time_ms": 2,
    });
    let request: ContextResourceSkillIngestRequest =
        serde_json::from_value(request_json).expect("valid ingest request");
    let report = ingest_resource_skill_context(&engine, request);
    let resource = report
        .resources
        .first()
        .expect("one resource in the report");
    assert!(!resource.inline_payload, "an oversized payload must not inline");
    assert!(
        resource
            .external_object_uri
            .starts_with("temporalstore://resources/"),
        "expected a real engine blob uri, got {}",
        resource.external_object_uri
    );

    let (bytes, total, eof) = fetch(&engine, &resource.external_object_uri, 0, 0);
    assert!(eof);
    assert_eq!(total as usize, payload.len());
    assert_eq!(bytes, payload.as_bytes(), "fetched attachment differs from the original");
}
