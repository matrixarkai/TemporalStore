// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use temporalstore::control::LoadShardRequest;
use temporalstore::engine::TemporalEngine;
use temporalstore::types::{Command, CommandResponse, ExecuteRequest};
use temporalstore::wal::WriteAheadLogRecord;
use temporalstore::BlockStoreOptions;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Ledger = BTreeMap<String, Option<Vec<u8>>>;
const SHARD: u64 = 1;
const MAX_BYTES: u64 = 8 * 1024 * 1024;
const FIXTURE_SHA: &str = env!("UPGRADE_FIXTURE_SOURCE_SHA256");
const OLD: &str = "d771121eeebfc833873dc37a54bfbdcd268c660a";
const NEW: &str = env!("UPGRADE_CONSUMER_COMMIT");

fn target_format() -> u32 {
    env!("UPGRADE_TARGET_FORMAT").parse().expect("bound consumer format")
}

fn check(value: bool, message: impl Into<String>) -> Result<()> {
    if !value { return Err(message.into().into()); }
    Ok(())
}

fn digest(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }

fn header(bytes: &[u8], version: u32) -> Result<()> {
    check(bytes.len() > 11, "truncated checkpoint")?;
    check(&bytes[..6] == b"TSIDX\x01" && bytes[6] == 2, "wrong checkpoint container/codec")?;
    check(u32::from_be_bytes(bytes[7..11].try_into()?) == version, "wrong actual checkpoint version")
}

fn identity() -> Value {
    json!({"schema":1,"role":ROLE,"lib_commit":LIB_COMMIT,
        "fixture_sha256":FIXTURE_SHA,"pid":std::process::id(),
        "expected_format":if ROLE == "producer" {32} else {target_format()}})
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let file = fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    use std::io::Write;
    let mut file = file;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn open(root: &Path) -> Result<TemporalEngine> {
    let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        2 * 1024 * 1024, root.join("cache"), root.join("blocks"), root.join("index"),
        BlockStoreOptions::default(), false,
    );
    let loaded = engine.load_shard_with(LoadShardRequest {
        shard_id: SHARD, table_name: "upgrade3505".into(),
        shard_uri: "local://upgrade3505/1".into(), start_routing_bucket: 0,
        end_routing_bucket: temporalstore::DEFAULT_END_ROUTING_BUCKET,
        readonly: false, load_version: 1, local_node_id: Some(1),
    });
    check(loaded.status.ok, format!("load failed: {:?}", loaded.status))?;
    Ok(engine)
}

fn execute(engine: &TemporalEngine, command: Command) -> Result<CommandResponse> {
    let result = engine.execute(ExecuteRequest { shard_id: SHARD, command });
    check(result.status.ok, format!("execute failed: {:?}", result.status))?;
    Ok(result.response)
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) -> Result<()> {
    execute(engine, Command::StringSet { key: key.into(), value: value.to_vec() })?;
    Ok(())
}

fn ledger(case: &str, upgraded: bool) -> Ledger {
    let mut rows = BTreeMap::from([
        ("stable".into(), Some(vec![0, 1, 255, 128, 0, 7])),
        ("empty-value".into(), Some(Vec::new())),
        ("key-\u{4e2d}".into(), Some(b"unchanged checkpoint-only payload".to_vec())),
        ("overwritten".into(), Some(b"prefix-final".to_vec())),
        ("deleted-prefix".into(), None),
        ("deleted-tail".into(), Some(b"present-at-checkpoint".to_vec())),
        ("new-tail".into(), None),
    ]);
    if case == "retained" {
        rows.insert("overwritten".into(), Some(vec![17, 0, 42, 255]));
        rows.insert("deleted-tail".into(), None);
        rows.insert("new-tail".into(), Some(b"exists only after the cut".to_vec()));
    }
    if upgraded { rows.insert("upgrade-marker".into(), Some(b"upgraded-durable-not-replayed".to_vec())); }
    rows
}

fn verify(engine: &TemporalEngine, expected: &Ledger, require_materialized: bool) -> Result<()> {
    for (key, value) in expected {
        check(execute(engine, Command::StringGet { key: key.clone() })?
            == CommandResponse::Bytes { value: value.clone() }, format!("payload/absence mismatch: {key}"))?;
    }
    let report = engine.storage_physical_index_report(SHARD);
    let mut counts = BTreeMap::<String, usize>::new();
    for bucket in &report.bucket_nodes {
        for block in &bucket.block_indexes {
            if block.model_id == "string" && !block.deleted {
                *counts.entry(block.object_key.clone()).or_default() += 1;
                if require_materialized { check(!block.log_backed, "checkpoint still names WAL-resident String")?; }
            }
        }
    }
    let wanted = expected.iter().filter_map(|(key, value)| value.as_ref().map(|_| key.clone()))
        .collect::<BTreeSet<_>>();
    check(counts.keys().cloned().collect::<BTreeSet<_>>() == wanted, "exact live key set differs")?;
    check(counts.values().all(|count| *count == 1), "duplicate live String placement")
}

fn scan(engine: &TemporalEngine) -> Result<Vec<WriteAheadLogRecord>> {
    let (records, truncated) = engine.write_ahead_log_store().scan_decoded(SHARD, 0, u64::MAX, MAX_BYTES)?;
    check(!truncated, "bounded WAL scan truncated")?;
    Ok(records.into_iter().map(|(_, record)| record).collect())
}

fn walk(root: &Path) -> Result<BTreeMap<String, (u64, String)>> {
    fn visit(root: &Path, at: &Path, rows: &mut BTreeMap<String, (u64, String)>, total: &mut u64) -> Result<()> {
        if !at.exists() { return Ok(()); }
        for entry in fs::read_dir(at)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            check(!metadata.file_type().is_symlink(), "symlink in owned corpus")?;
            if metadata.is_dir() { visit(root, &entry.path(), rows, total)?; }
            else {
                *total = total.checked_add(metadata.len()).ok_or("corpus size overflow")?;
                check(*total <= MAX_BYTES && rows.len() < 256, "corpus exceeded source bound")?;
                let name = entry.path().strip_prefix(root)?.to_str().ok_or("non-UTF8 fixture path")?.replace('\\', "/");
                rows.insert(name, (metadata.len(), digest(&fs::read(entry.path())?)));
            }
        }
        Ok(())
    }
    let mut rows = BTreeMap::new();
    visit(root, root, &mut rows, &mut 0)?;
    Ok(rows)
}

fn checkpoint_files(root: &Path) -> Result<BTreeMap<String, (u64, String)>> {
    Ok(walk(&root.join("index"))?.into_iter().filter(|(name, _)|
        !name.starts_with("wals/") && !name.starts_with("indexlogs/")
    ).collect())
}

fn checkpoint(engine: &TemporalEngine, root: &Path, version: u32, expected: &Ledger) -> Result<(u64, Value)> {
    let moved = engine.materialize_synthetic_blocks(SHARD);
    check(moved > 0, "fixture did not exercise production String materialization")?;
    verify(engine, expected, true)?;
    engine.flush_shard_index(SHARD);
    let bytes = fs::read(root.join("index/shard-1.index.json"))?;
    header(&bytes, version)?;
    let manifest = engine.create_bucket_dump_manifest(SHARD, Vec::<u32>::new())
        .map_err(|status| format!("dump failed: {status:?}"))?;
    engine.validate_bucket_dump_manifest(&manifest).map_err(|status| format!("manifest invalid: {status:?}"))?;
    header(&manifest.index_bytes, version)?;
    check(manifest.wal_sequence > 0, "checkpoint has no positive cut")?;
    check(digest(&manifest.index_bytes) == manifest.index_sha256, "manifest image digest differs")?;
    Ok((manifest.wal_sequence, json!({"anchor":manifest.wal_sequence,"format":version,
        "base_sha256":digest(&bytes),"manifest_id":manifest.manifest_id,
        "manifest_checksum":manifest.checksum,"manifest_image_sha256":manifest.index_sha256,
        "materialized":moved})))
}

fn reclaim(engine: &TemporalEngine, root: &Path, cut: u64) -> Result<Value> {
    engine.write_ahead_log_store().flush(SHARD)?;
    let before = scan(engine)?;
    check(!before.is_empty() && before.iter().any(|record| record.sequence <= cut), "no actual covered prefix exists")?;
    let physical_before = walk(&root.join("index/wals"))?.values().map(|row| row.0).sum::<u64>();
    let plan = engine.storage_wal_reclaim_plan(SHARD, Vec::new(), Vec::new());
    check(plan.safe_to_reclaim && plan.retain_from_wal_sequence > cut,
        format!("reclaim blocked: {:?}", plan.blocker_reasons))?;
    let report = engine.apply_storage_wal_reclaim(plan);
    let after = scan(engine)?;
    let physical_after = walk(&root.join("index/wals"))?.values().map(|row| row.0).sum::<u64>();
    check(report.applied && report.wal_records_removed > 0, "production reclaim did not remove records")?;
    check(report.wal_bytes_before > report.wal_bytes_after || report.wal_segment_bytes_dropped > 0,
        "reclaim report shows no physical removal")?;
    check(physical_before > physical_after, "actual WAL files did not shrink")?;
    check(after.is_empty(), "covered WAL records remain after reclamation")?;
    Ok(json!({"cut":cut,"records_before":before.len(),"records_after":after.len(),
        "physical_bytes_before":physical_before,"physical_bytes_after":physical_after,"report":report}))
}

fn produce(root: &Path, case: &str) -> Result<Value> {
    check(!root.exists(), "producer requires a fresh owned case directory")?;
    fs::create_dir(root)?;
    fs::write(root.join("fixture-owner"), FIXTURE_SHA)?;
    let engine = open(root)?;
    for (key, value) in ledger("empty", false) {
        if let Some(value) = value { set(&engine, &key, &value)?; }
    }
    set(&engine, "overwritten", b"superseded-prefix")?;
    set(&engine, "overwritten", b"prefix-final")?;
    set(&engine, "deleted-prefix", b"must-not-resurrect")?;
    execute(&engine, Command::CommonDelete { key: "deleted-prefix".into() })?;
    let (cut, checkpoint) = checkpoint(&engine, root, 32, &ledger("empty", false))?;
    let reclaim = reclaim(&engine, root, cut)?;
    let fixed_files = checkpoint_files(root)?;
    if case == "retained" {
        set(&engine, "overwritten", &[17, 0, 42, 255])?;
        execute(&engine, Command::CommonDelete { key: "deleted-tail".into() })?;
        set(&engine, "new-tail", b"exists only after the cut")?;
    }
    engine.write_ahead_log_store().flush(SHARD)?;
    let tail = scan(&engine)?;
    let count = if case == "retained" { 3 } else { 0 };
    check(tail.len() == count, "tail record count differs")?;
    for (index, record) in tail.iter().enumerate() {
        check(record.shard_id == SHARD && record.sequence == cut + index as u64 + 1, "tail is not the exact post-cut sequence")?;
        check(record.command.is_some() || !record.outcomes.is_empty(), "tail record describes no mutation")?;
    }
    verify(&engine, &ledger(case, false), false)?;
    check(checkpoint_files(root)? == fixed_files, "tail was checkpointed unexpectedly")?;
    drop(engine);
    check(checkpoint_files(root)? == fixed_files, "drop/unload rewrote the producer checkpoint")?;
    let result = json!({"identity":identity(),"phase":"produce","case":case,"cut":cut,
        "expected":ledger(case,false),"checkpoint":checkpoint,"checkpoint_files":fixed_files,
        "reclaim":reclaim,"tail":tail,"corpus_inventory":walk(root)?});
    write_json(&root.join("producer.json"), &result)?;
    Ok(result)
}

fn producer_receipt(root: &Path, case: &str) -> Result<Value> {
    check(fs::read_to_string(root.join("fixture-owner"))? == FIXTURE_SHA, "wrong corpus owner")?;
    let receipt: Value = serde_json::from_slice(&fs::read(root.join("producer.json"))?)?;
    check(receipt["identity"]["lib_commit"] == OLD && receipt["identity"]["fixture_sha256"] == FIXTURE_SHA,
        "unbound old producer")?;
    check(receipt["case"] == case && receipt["expected"] == serde_json::to_value(ledger(case,false))?, "producer ledger mismatch")?;
    Ok(receipt)
}

fn negative(root: &Path, expected: &Ledger) -> Result<Value> {
    let copy = root.with_file_name(format!("{}-checkpoint-less", root.file_name().ok_or("missing case name")?.to_str().ok_or("case name")?));
    check(!copy.exists(), "negative control destination already exists")?;
    fs::create_dir(&copy)?;
    for folder in ["blocks", "index/wals"] {
        let from = root.join(folder);
        for (name, _) in walk(&from)? {
            let target = copy.join(folder).join(&name);
            fs::create_dir_all(target.parent().ok_or("negative parent")?)?;
            fs::copy(from.join(name), target)?;
        }
    }
    let rejected = match open(&copy) {
        Err(error) => format!("load refused without images: {error}"),
        Ok(engine) => match verify(&engine, expected, false) {
            Err(error) => format!("exact corpus refused without images: {error}"),
            Ok(()) => return Err("checkpoint-less control reconstructed all expected data".into()),
        },
    };
    Ok(json!({"omitted":"all index checkpoints, dump manifests and Index log; blocks and retained WAL copied only",
        "rejected":true,"reason":rejected,"inventory":walk(&copy)?}))
}

fn upgrade(root: &Path, case: &str) -> Result<Value> {
    let old = producer_receipt(root, case)?;
    check(old["checkpoint_files"] == serde_json::to_value(checkpoint_files(root)?)?, "old durable images changed before upgrade")?;
    let negative = negative(root, &ledger(case,false))?;
    let engine = open(root)?;
    check(serde_json::to_value(scan(&engine)?)? == old["tail"], "load altered retained WAL mutation records")?;
    verify(&engine, &ledger(case,false), false)?;
    set(&engine, "upgrade-marker", b"upgraded-durable-not-replayed")?;
    let (cut, checkpoint) = checkpoint(&engine, root, target_format(), &ledger(case,true))?;
    check(cut > old["cut"].as_u64().ok_or("producer cut missing")?, "new cut did not advance beyond v32")?;
    let reclaim = reclaim(&engine, root, cut)?;
    let fixed_files = checkpoint_files(root)?;
    drop(engine);
    check(checkpoint_files(root)? == fixed_files, "consumer drop rewrote checkpoint")?;
    let result = json!({"identity":identity(),"phase":"upgrade","case":case,"expected":ledger(case,true),
        "checkpoint":checkpoint,"checkpoint_files":fixed_files,"reclaim":reclaim,"negative":negative});
    write_json(&root.join("consumer.json"), &result)?;
    Ok(result)
}

fn reopen(root: &Path, case: &str) -> Result<Value> {
    producer_receipt(root, case)?;
    let previous: Value = serde_json::from_slice(&fs::read(root.join("consumer.json"))?)?;
    check(previous["identity"]["lib_commit"] == NEW && previous["identity"]["fixture_sha256"] == FIXTURE_SHA,
        "unbound upgraded consumer")?;
    check(previous["expected"] == serde_json::to_value(ledger(case,true))?, "consumer ledger mismatch")?;
    check(previous["checkpoint_files"] == serde_json::to_value(checkpoint_files(root)?)?, "upgraded durable images changed")?;
    let engine = open(root)?;
    check(scan(&engine)?.is_empty(), "upgraded reopen was rescued by retained WAL")?;
    verify(&engine, &ledger(case,true), true)?;
    header(&fs::read(root.join("index/shard-1.index.json"))?, target_format())?;
    let result = json!({"identity":identity(),"phase":"reopen","case":case,"passed":true,
        "expected":ledger(case,true),"retained_records":0});
    drop(engine);
    write_json(&root.join("reopen.json"), &result)?;
    Ok(result)
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args == ["--identity"] { println!("{}", identity()); return Ok(()); }
    check(args.len() == 3, "usage: BINARY produce|upgrade|reopen ROOT empty|retained")?;
    let root = PathBuf::from(&args[1]);
    let case = &args[2];
    check(root.is_absolute() && matches!(case.as_str(), "empty" | "retained"), "absolute root and exact case required")?;
    let result = match (ROLE, args[0].as_str()) {
        ("producer", "produce") => produce(&root, case)?,
        ("consumer", "upgrade") => upgrade(&root, case)?,
        ("consumer", "reopen") => reopen(&root, case)?,
        _ => return Err("wrong binary identity for requested phase".into()),
    };
    println!("{}", json!({"identity":result["identity"],"phase":result["phase"],"case":case,"passed":true}));
    Ok(())
}
