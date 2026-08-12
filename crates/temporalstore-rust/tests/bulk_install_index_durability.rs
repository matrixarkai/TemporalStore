// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Regression: manifest install-on-load must durably materialize the restored index even
//! under bulk-ingest mode.
//!
//! `persist_index_bytes` is deliberately a no-op under `MATRIXARK_BULK_INGEST` to turn the
//! per-record served-index rewrite from O(n^2) into one write per batch. But manifest
//! *install* is a one-shot recovery step, not the hot path: if it skips the durable index
//! write, `load_index()` keeps reading the stale pre-manifest index while the advanced
//! replay watermark (the manifest's wal_sequence) suppresses replay of the very records
//! the manifest embeds -- records that were already WAL-GC'd and now live ONLY in the
//! manifest. The net effect is silent data loss on a bulk-mode recovering load. Install must
//! therefore bypass the bulk gate (`persist_index_bytes_durable`).
//!
//! This test runs in its own integration-test process, so toggling the process-wide
//! `MATRIXARK_BULK_INGEST` env var cannot race the (single-process) library unit tests.

use std::path::PathBuf;

use temporalstore_rust::{Command, ExecuteRequest, TemporalEngine};

const SHARD_ID: u64 = 1;

fn root() -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-bulk-install-durability-{}", std::process::id()));
    root
}

#[test]
fn manifest_install_materializes_index_under_bulk_mode() {
    let root = root();
    let _ = std::fs::remove_dir_all(&root);
    let index_dir = root.join("indexes");
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        4096,
        root.join("cache"),
        root.join("pages"),
        &index_dir,
    );
    engine.load_shard(SHARD_ID);
    for key in ["alpha", "bravo", "charlie"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok, "{response:?}");
    }

    let manifest = engine
        .create_bucket_dump_manifest(SHARD_ID, Vec::new())
        .expect("dump manifest should persist");

    // Remove the on-disk served index so ONLY the install can re-materialize it. This
    // isolates the durable write from the per-record writes that ran while bulk mode was
    // off, making the assertion below fail iff install honored the (buggy) bulk gate.
    let index_path = index_dir.join(format!("shard-{SHARD_ID}.index.json"));
    let _ = std::fs::remove_file(&index_path);
    assert!(
        !index_path.exists(),
        "index file should be absent right before the bulk-mode install"
    );

    // Enter bulk-ingest mode for the install, then restore immediately.
    std::env::set_var("MATRIXARK_BULK_INGEST", "1");
    let install = engine.install_bucket_dump_manifest(&manifest);
    std::env::remove_var("MATRIXARK_BULK_INGEST");
    install.expect("install should succeed under bulk mode");

    assert!(
        index_path.exists(),
        "manifest install must durably write the restored index even under bulk mode; \
         otherwise the stale index + advanced replay watermark silently lose the manifest's \
         records on a recovering load"
    );
    let bytes = std::fs::read(&index_path).expect("read installed index");
    assert!(
        !bytes.is_empty(),
        "installed index must be non-empty (it carries the restored shard state)"
    );

    let _ = std::fs::remove_dir_all(&root);
}
