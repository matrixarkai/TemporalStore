// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};
use temporalstore_rust::http::{get_json_with_options, post_json_with_options, HttpRequestOptions};

#[derive(Debug)]
struct Options {
    infer: bool,
    addr: String,
    records: usize,
    batch_size: usize,
    tenant_shards: usize,
    users: usize,
    searches: usize,
    metadata_updates: usize,
    payload_bytes: usize,
    out: PathBuf,
    data_root: Option<PathBuf>,
    service_pids: Vec<u32>,
    provider: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ApiMetric {
    requests: usize,
    errors: usize,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
    requests_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct ProcessMetric {
    pid: u32,
    start_rss_bytes: u64,
    end_rss_bytes: u64,
    peak_rss_bytes: u64,
    cpu_ticks: u64,
    clock_ticks_per_second: u64,
}

#[derive(Debug, Serialize)]
struct Report {
    infer: bool,
    tenant_id: String,
    requested_records: usize,
    ingested_records: usize,
    metadata_update_rounds: usize,
    tenant_shards: usize,
    payload_bytes: u64,
    elapsed_ms: u128,
    ingest_records_per_sec: f64,
    stored_bytes: Option<u64>,
    storage_amplification: Option<f64>,
    api: BTreeMap<String, ApiMetric>,
    harness_process: ProcessMetric,
    service_processes: Vec<ProcessMetric>,
    provider: String,
    failures: Vec<String>,
}

#[derive(Default)]
struct Timings {
    values: BTreeMap<String, Vec<u128>>,
    errors: BTreeMap<String, usize>,
}

impl Timings {
    fn observe<T>(
        &mut self,
        name: &str,
        started: Instant,
        result: &Result<T, impl std::fmt::Display>,
    ) {
        self.values
            .entry(name.to_string())
            .or_default()
            .push(started.elapsed().as_micros());
        if result.is_err() {
            *self.errors.entry(name.to_string()).or_default() += 1;
        }
    }

    fn finish(self) -> BTreeMap<String, ApiMetric> {
        self.values
            .into_iter()
            .map(|(name, mut values)| {
                values.sort_unstable();
                let elapsed_us = values.iter().sum::<u128>().max(1);
                let metric = ApiMetric {
                    requests: values.len(),
                    errors: self.errors.get(&name).copied().unwrap_or_default(),
                    p50_us: percentile(&values, 50),
                    p95_us: percentile(&values, 95),
                    p99_us: percentile(&values, 99),
                    max_us: values.last().copied().unwrap_or_default(),
                    requests_per_sec: values.len() as f64 * 1_000_000.0 / elapsed_us as f64,
                };
                (name, metric)
            })
            .collect()
    }
}

fn main() {
    let options = parse_options();
    let started = Instant::now();
    let tenant_id = format!(
        "scale-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let harness_sampler = ProcessSampler::start(std::process::id());
    let service_samplers = options
        .service_pids
        .iter()
        .copied()
        .map(ProcessSampler::start)
        .collect::<Vec<_>>();
    let mut timings = Timings::default();
    let mut failures = Vec::new();
    let mut sampled_ids = Vec::new();
    let mut ingested_records = 0usize;
    let mut payload_bytes = 0u64;

    let reset = post(&options.addr, "/memories/reset", &scope_body(&tenant_id));
    if let Err(error) = reset {
        failures.push(format!("initial reset: {error}"));
    }

    let ingest_started = Instant::now();
    let mut progress_at = ingest_started;
    for batch_start in (0..options.records).step_by(options.batch_size) {
        let batch_end = (batch_start + options.batch_size).min(options.records);
        let batch_number = batch_start / options.batch_size;
        let batch_tenant = corpus_tenant(&tenant_id, batch_number % options.tenant_shards);
        let user = format!("user-{:06}", batch_number % options.users);
        let run = format!("run-{batch_number:08}");
        let messages = (batch_start..batch_end)
            .map(|index| {
                let text = corpus_text(index, options.payload_bytes);
                payload_bytes += text.len() as u64;
                json!({"role": "user", "content": text})
            })
            .collect::<Vec<_>>();
        let mut body = json!({
            "infer": options.infer,
            "messages": messages,
            "scope": {"tenant_id": batch_tenant, "account_id": "scale-account"},
            "user_id": user,
            "agent_id": format!("agent-{}", batch_start % 8),
            "run_id": run,
            "metadata": {"corpus": "scale", "batch": batch_number}
        });
        if let Some(provider) = &options.provider {
            body["provider"] = provider.clone();
        }
        let call_started = Instant::now();
        let result = post(&options.addr, "/memories", &body);
        timings.observe("add", call_started, &result);
        match result {
            Ok(value) => {
                let ids = value["results"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|row| row["id"].as_str().map(str::to_owned))
                    .collect::<Vec<_>>();
                ingested_records += ids.len();
                let remaining = 256usize.saturating_sub(sampled_ids.len());
                sampled_ids.extend(ids.into_iter().take(remaining));
            }
            Err(error) => failures.push(format!("add batch {batch_start}: {error}")),
        }
        if progress_at.elapsed() >= Duration::from_secs(30) || batch_end == options.records {
            eprintln!("mem0-scale phase=ingest infer={} acknowledged={} requested={} failed_batches={}",
                options.infer, ingested_records, options.records, failures.len());
            progress_at = Instant::now();
        }
    }
    let ingest_elapsed = ingest_started.elapsed();

    for query_index in 0..options.searches {
        let record_index = query_record_index(options.records, options.batch_size, query_index);
        let query_tenant = corpus_tenant(
            &tenant_id,
            (record_index / options.batch_size) % options.tenant_shards,
        );
        let mut body = scope_body(&query_tenant);
        body["query"] = json!(format!("topic {:08} retrieval marker", record_index));
        body["limit"] = json!(10);
        if let Some(provider) = &options.provider {
            body["provider"] = provider.clone();
        }
        measure_post_checked(
            &mut timings,
            &mut failures,
            "search",
            &options.addr,
            "/memories/search",
            &body,
            |response| require_search_candidates(response, 10),
        );
    }

    let mut list = scope_body(&tenant_id);
    list["limit"] = json!(100);
    measure_post(
        &mut timings,
        &mut failures,
        "list",
        &options.addr,
        "/memories/list",
        &list,
    );
    measure_post(
        &mut timings,
        &mut failures,
        "users",
        &options.addr,
        "/memories/users",
        &list,
    );

    if options.metadata_updates > 0 {
        if let Some(id) = sampled_ids.first() {
            let path = format!("/memories/{id}");
            let baseline = get_json_with_options::<Value>(&options.addr, &path, request_options())
                .map_err(|error| error.to_string())
                .and_then(validate_response);
            match baseline {
                Ok(before) if before["id"] == *id => {
                    let ids = sampled_ids.iter().take(16).collect::<Vec<_>>();
                    for round in 0..options.metadata_updates {
                        measure_post_checked(
                            &mut timings, &mut failures, "metadata_update", &options.addr,
                            &format!("/memories/{id}/update"),
                            &json!({"metadata":{"workload":"metadata-single","round":round}}),
                            |value| if value["updated"] == true { Ok(()) } else { Err("metadata update acknowledgment missing".into()) },
                        );
                        measure_post_checked(
                            &mut timings, &mut failures, "metadata_batch_update", &options.addr,
                            "/memories/batch/update",
                            &json!({"memories":ids.iter().map(|id| json!({
                                "memory_id":id,"metadata":{"workload":"metadata-batch","round":round}
                            })).collect::<Vec<_>>()}),
                            |value| if value["updated"].as_u64() == Some(ids.len() as u64) {
                                Ok(())
                            } else { Err("metadata batch acknowledgment incomplete".into()) },
                        );
                    }
                    let verified = get_json_with_options::<Value>(&options.addr, &path, request_options())
                        .map_err(|error| error.to_string())
                        .and_then(|after| validate_metadata_revision(&before, &after, options.metadata_updates - 1));
                    if let Err(error) = verified {
                        failures.push(format!("metadata verification: {error}"));
                    }
                }
                Ok(_) => failures.push("metadata baseline returned an unexpected memory".into()),
                Err(error) => failures.push(format!("metadata baseline: {error}")),
            }
        } else {
            failures.push("metadata workload requires an acknowledged memory".into());
        }
    }

    if let Some(id) = sampled_ids.first() {
        measure_get(
            &mut timings,
            &mut failures,
            "get",
            &options.addr,
            &format!("/memories/{id}"),
        );
        let mut update = json!({"data": "updated scale memory with retrieval marker"});
        if let Some(provider) = &options.provider {
            update["provider"] = provider.clone();
        }
        measure_post(
            &mut timings,
            &mut failures,
            "update",
            &options.addr,
            &format!("/memories/{id}/update"),
            &update,
        );
        measure_get(
            &mut timings,
            &mut failures,
            "history",
            &options.addr,
            &format!("/memories/{id}/history"),
        );
        measure_post(
            &mut timings,
            &mut failures,
            "feedback",
            &options.addr,
            &format!("/memories/{id}/feedback"),
            &json!({"feedback": "POSITIVE", "feedback_reason": "scale validation"}),
        );
    }

    let batch_ids = sampled_ids
        .iter()
        .skip(1)
        .take(16)
        .cloned()
        .collect::<Vec<_>>();
    let updates = batch_ids
        .iter()
        .map(|id| json!({"memory_id": id, "text": "batch updated scale memory"}))
        .collect::<Vec<_>>();
    let mut batch_update = json!({"memories": updates});
    if let Some(provider) = &options.provider {
        batch_update["provider"] = provider.clone();
    }
    measure_post(
        &mut timings,
        &mut failures,
        "batch_update",
        &options.addr,
        "/memories/batch/update",
        &batch_update,
    );
    measure_post(
        &mut timings,
        &mut failures,
        "batch_delete",
        &options.addr,
        "/memories/batch/delete",
        &json!({"memories": batch_ids}),
    );
    if let Some(id) = sampled_ids.get(24) {
        measure_post(
            &mut timings,
            &mut failures,
            "delete",
            &options.addr,
            &format!("/memories/{id}/delete"),
            &json!({}),
        );
    }

    let mut delete_one = scope_body(&tenant_id);
    delete_one["user_id"] = json!("user-000000");
    measure_post(
        &mut timings,
        &mut failures,
        "delete_all",
        &options.addr,
        "/memories/delete_all",
        &delete_one,
    );
    measure_post(
        &mut timings,
        &mut failures,
        "reset",
        &options.addr,
        "/memories/reset",
        &scope_body(&tenant_id),
    );

    let stored_bytes = options.data_root.as_deref().map(directory_bytes);
    let report = Report {
        infer: options.infer,
        tenant_id,
        requested_records: options.records,
        ingested_records,
        metadata_update_rounds: options.metadata_updates,
        tenant_shards: options.tenant_shards,
        payload_bytes,
        elapsed_ms: started.elapsed().as_millis(),
        ingest_records_per_sec: ingested_records as f64
            / ingest_elapsed.as_secs_f64().max(0.000_001),
        stored_bytes,
        storage_amplification: stored_bytes.map(|bytes| bytes as f64 / payload_bytes.max(1) as f64),
        api: timings.finish(),
        harness_process: harness_sampler.finish(),
        service_processes: service_samplers
            .into_iter()
            .map(ProcessSampler::finish)
            .collect(),
        provider: options
            .provider
            .as_ref()
            .and_then(|value| {
                value["provider_kind"]
                    .as_str()
                    .or_else(|| value["kind"].as_str())
            })
            .map(str::to_owned)
            .or_else(|| std::env::var("TEMPORALSTORE_CONTEXT_PROVIDER").ok())
            .unwrap_or_else(|| "deterministic".to_string()),
        failures,
    };
    let encoded = serde_json::to_vec_pretty(&report).expect("serialize report");
    if let Some(parent) = options.out.parent() {
        fs::create_dir_all(parent).expect("create report directory");
    }
    fs::write(&options.out, &encoded).expect("write report");
    println!("{}", String::from_utf8_lossy(&encoded));
    if !report.failures.is_empty() || report.ingested_records != report.requested_records {
        std::process::exit(1);
    }
}

fn post(addr: &str, path: &str, body: &Value) -> Result<Value, String> {
    let response = post_json_with_options(addr, path, body, request_options())
        .map_err(|error| error.to_string())?;
    validate_response(response)
}

fn measure_post(
    timings: &mut Timings,
    failures: &mut Vec<String>,
    name: &str,
    addr: &str,
    path: &str,
    body: &Value,
) {
    measure_post_checked(timings, failures, name, addr, path, body, |_| Ok(()));
}

fn measure_post_checked(
    timings: &mut Timings,
    failures: &mut Vec<String>,
    name: &str,
    addr: &str,
    path: &str,
    body: &Value,
    check: impl FnOnce(&Value) -> Result<(), String>,
) {
    let started = Instant::now();
    let result = post(addr, path, body).and_then(|response| {
        check(&response)?;
        Ok(response)
    });
    timings.observe(name, started, &result);
    if let Err(error) = result {
        failures.push(format!("{name}: {error}"));
    }
}

fn measure_get(
    timings: &mut Timings,
    failures: &mut Vec<String>,
    name: &str,
    addr: &str,
    path: &str,
) {
    let started = Instant::now();
    let result = get_json_with_options::<Value>(addr, path, request_options())
        .map_err(|error| error.to_string())
        .and_then(validate_response);
    timings.observe(name, started, &result);
    if let Err(error) = result {
        failures.push(format!("{name}: {error}"));
    }
}

fn request_options() -> HttpRequestOptions {
    HttpRequestOptions {
        connect_timeout_ms: 2_000,
        io_timeout_ms: 30_000,
        max_retries: 0,
    }
}

fn validate_response(response: Value) -> Result<Value, String> {
    if response
        .get("failed")
        .and_then(Value::as_array)
        .is_some_and(|failed| !failed.is_empty())
    {
        return Err("batch response contains failed operations".into());
    }
    if ["updated", "deleted"]
        .iter()
        .any(|field| response.get(*field).and_then(Value::as_bool) == Some(false))
    {
        return Err("mutation was not applied".into());
    }
    let top_level_error = response.get("ok").and_then(Value::as_bool) == Some(false);
    let status_error = response
        .get("status")
        .and_then(|status| status.get("ok"))
        .and_then(Value::as_bool)
        == Some(false);
    if top_level_error || status_error {
        let message = response
            .get("error")
            .and_then(Value::as_str)
            .or_else(|| response.pointer("/status/message").and_then(Value::as_str))
            .unwrap_or("request failed");
        return Err(message.to_string());
    }
    Ok(response)
}

fn validate_metadata_revision(before: &Value, after: &Value, round: usize) -> Result<(), String> {
    for field in ["id", "memory", "context_ref"] {
        if before.get(field).and_then(Value::as_str).is_none()
            || before.get(field) != after.get(field) {
            return Err(format!("metadata update changed or omitted {field}"));
        }
    }
    if after["metadata"]["workload"] != "metadata-batch"
        || after["metadata"]["round"].as_u64() != Some(round as u64)
    {
        return Err("latest metadata is not readable".into());
    }
    Ok(())
}

fn scope_body(tenant_id: &str) -> Value {
    json!({"scope": {"tenant_id": tenant_id, "account_id": "scale-account"}})
}

fn corpus_tenant(base: &str, shard: usize) -> String {
    if shard == 0 {
        base.to_string()
    } else {
        format!("{base}-{shard:04}")
    }
}

fn query_record_index(records: usize, batch_size: usize, query_index: usize) -> usize {
    let batch_count = records.div_ceil(batch_size);
    let batch_start = (query_index % batch_count) * batch_size;
    let batch_len = (records - batch_start).min(batch_size);
    batch_start + (query_index / batch_count) % batch_len
}

fn require_search_candidates(response: &Value, limit: usize) -> Result<(), String> {
    let rows = response["results"]
        .as_array()
        .ok_or_else(|| "search response has no results array".to_string())?;
    if rows.is_empty() {
        return Err("search returned no candidates for an ingested corpus query".to_string());
    }
    if rows.len() > limit {
        return Err(format!(
            "search returned {} candidates, limit is {limit}",
            rows.len()
        ));
    }
    let mut ids = HashSet::with_capacity(rows.len());
    for row in rows {
        let id = row["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| "search returned a memory without an id".to_string())?;
        if !ids.insert(id) {
            return Err("search returned duplicate memory ids".to_string());
        }
    }
    Ok(())
}

fn corpus_text(index: usize, target_bytes: usize) -> String {
    let prefix = format!(
        "topic {index:08} retrieval marker; user preference and durable decision {index}; "
    );
    if prefix.len() >= target_bytes {
        return prefix;
    }
    let mut text = String::with_capacity(target_bytes);
    text.push_str(&prefix);
    while text.len() < target_bytes {
        text.push_str("bounded context evidence; ");
    }
    text.truncate(target_bytes);
    text
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    values[(values.len() - 1) * percentile / 100]
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| directory_bytes(&entry.path()))
        .sum()
}

struct ProcessSampler {
    pid: u32,
    start_rss_bytes: u64,
    start_cpu_ticks: u64,
    peak_rss_bytes: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

impl ProcessSampler {
    fn start(pid: u32) -> Self {
        let start_rss_bytes = process_rss_bytes(pid);
        let start_cpu_ticks = process_cpu_ticks(pid);
        let peak_rss_bytes = Arc::new(AtomicU64::new(start_rss_bytes));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_peak = Arc::clone(&peak_rss_bytes);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                thread_peak.fetch_max(process_rss_bytes(pid), Ordering::Relaxed);
                thread::sleep(Duration::from_millis(50));
            }
        });
        Self {
            pid,
            start_rss_bytes,
            start_cpu_ticks,
            peak_rss_bytes,
            stop,
            thread,
        }
    }

    fn finish(self) -> ProcessMetric {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.thread.join();
        let end_rss_bytes = process_rss_bytes(self.pid);
        ProcessMetric {
            pid: self.pid,
            start_rss_bytes: self.start_rss_bytes,
            end_rss_bytes,
            peak_rss_bytes: self
                .peak_rss_bytes
                .load(Ordering::Relaxed)
                .max(end_rss_bytes),
            cpu_ticks: process_cpu_ticks(self.pid).saturating_sub(self.start_cpu_ticks),
            clock_ticks_per_second: clock_ticks_per_second(),
        }
    }
}

fn process_rss_bytes(pid: u32) -> u64 {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or_default()
        .saturating_mul(1024)
}

fn process_cpu_ticks(pid: u32) -> u64 {
    fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            let tail = stat.rsplit_once(") ")?.1;
            let fields = tail.split_whitespace().collect::<Vec<_>>();
            Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
        })
        .unwrap_or_default()
}

fn clock_ticks_per_second() -> u64 {
    std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(100)
}

fn parse_options() -> Options {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let value = |name: &str| {
        args.iter()
            .position(|arg| arg == name)
            .and_then(|index| args.get(index + 1))
            .cloned()
    };
    let number = |name: &str, default: usize| {
        value(name)
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(default)
    };
    let provider = value("--provider-json")
        .or_else(|| std::env::var("TS_MEM0_PROVIDER_JSON").ok())
        .map(|raw| serde_json::from_str(&raw).expect("--provider-json must be valid JSON"));
    Options {
        infer: value("--infer").map(|raw| raw.parse::<bool>().expect("--infer must be true or false")).unwrap_or(true),
        addr: value("--addr").unwrap_or_else(|| "127.0.0.1:17000".to_string()),
        records: number("--records", 100_000).max(1),
        batch_size: number("--batch-size", 64).clamp(1, 1_000),
        tenant_shards: number("--tenant-shards", 1).max(1),
        users: number("--users", 1_000).max(1),
        searches: number("--searches", 100).max(1),
        metadata_updates: number("--metadata-updates", 0).min(100_000),
        payload_bytes: number("--payload-bytes", 512).max(64),
        out: value("--out")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/mem0-scale/report.json")),
        data_root: value("--data-root").map(PathBuf::from),
        service_pids: value("--service-pids")
            .or_else(|| value("--service-pid"))
            .into_iter()
            .flat_map(|raw| {
                raw.split(',')
                    .filter_map(|value| value.trim().parse().ok())
                    .collect::<Vec<_>>()
            })
            .collect(),
        provider,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_batches_and_unapplied_mutations_do_not_pass_the_workload() {
        for response in [
            json!({"updated":1,"failed":[{"memory_id":"m2","status":502}]}),
            json!({"updated":false}),
            json!({"deleted":false}),
        ] {
            assert!(validate_response(response).is_err());
        }
        assert!(validate_response(json!({"updated":true})).is_ok());
        assert!(validate_response(json!({"updated":2,"failed":[]})).is_ok());
        assert!(validate_response(json!({"deleted":2,"failed":[]})).is_ok());
    }

    #[test]
    fn metadata_checks_reject_revision_changes_and_stale_reads() {
        let before = json!({"id":"m1","memory":"unchanged","context_ref":"revision-1"});
        let mut after = before.clone();
        after["metadata"] = json!({"workload":"metadata-batch","round":199});
        assert!(validate_metadata_revision(&before, &after, 199).is_ok());
        for field in ["id", "memory", "context_ref"] {
            let mut changed = after.clone();
            changed[field] = json!("changed");
            assert!(validate_metadata_revision(&before, &changed, 199).is_err());
            assert!(validate_metadata_revision(&json!({}), &after, 199).is_err());
        }
        assert!(validate_metadata_revision(&before, &after, 200).is_err());
    }

    #[test]
    fn query_targets_follow_ingest_batches_including_the_short_tail() {
        assert_eq!(query_record_index(1_025, 512, 0), 0);
        assert_eq!(query_record_index(1_025, 512, 1), 512);
        assert_eq!(query_record_index(1_025, 512, 2), 1_024);
        assert_eq!(query_record_index(1_025, 512, 3), 1);
        assert_eq!(query_record_index(1_025, 512, 5), 1_024);
        for query in 0..10_000 {
            let record = query_record_index(1_025, 512, query);
            assert!(record < 1_025);
            assert_eq!(record / 512, query % 3);
        }
        assert_eq!(query_record_index(1, 512, 100), 0);
    }

    #[test]
    fn empty_or_unbounded_searches_do_not_pass_the_workload() {
        assert!(require_search_candidates(&json!({}), 10).is_err());
        assert!(require_search_candidates(&json!({"results": []}), 10).is_err());
        assert!(require_search_candidates(&json!({"results": [{}]}), 10).is_err());
        assert!(require_search_candidates(&json!({"results": [{"id":""}]}), 10).is_err());
        assert!(
            require_search_candidates(&json!({"results": [{"id":"a"}, {"id":"a"}]}), 10).is_err()
        );
        assert!(require_search_candidates(&json!({"results": [{"id":"a"}]}), 10).is_ok());
        assert!(require_search_candidates(&json!({"results": vec![json!({}); 11]}), 10).is_err());
    }
}
