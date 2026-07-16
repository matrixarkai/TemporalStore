use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

#[derive(Clone, Debug)]
pub(crate) struct ScanRecordCacheEntry {
    pub records: Arc<Vec<Value>>,
    pub scanned_records: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct FilteredScanCacheEntry {
    pub records: Vec<Value>,
    pub scanned_records: u64,
    pub dropped_by_type: u64,
    pub dropped_by_scope: u64,
    pub selected_node_dropped: u64,
    pub secondary_dropped: u64,
    pub secondary_matched: u64,
    pub node_path_filter_count: usize,
}

static SCAN_RECORD_CACHE: OnceLock<Mutex<HashMap<String, ScanRecordCacheEntry>>> = OnceLock::new();
static FILTERED_SCAN_CACHE: OnceLock<Mutex<HashMap<String, FilteredScanCacheEntry>>> =
    OnceLock::new();

fn scan_record_cache() -> &'static Mutex<HashMap<String, ScanRecordCacheEntry>> {
    SCAN_RECORD_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn filtered_scan_cache() -> &'static Mutex<HashMap<String, FilteredScanCacheEntry>> {
    FILTERED_SCAN_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn max_scan_record_cache_entries() -> usize {
    std::env::var("MATRIXARK_RUST_SCAN_RECORD_CACHE_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8)
}

fn max_filtered_scan_cache_entries() -> usize {
    std::env::var("MATRIXARK_RUST_FILTERED_SCAN_CACHE_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(32)
}

pub(crate) fn scan_record_cache_key(record_hash_key: &str, shard_size: u64, count: u64) -> String {
    format!("{record_hash_key}\u{1f}{shard_size}\u{1f}{count}")
}

pub(crate) fn filtered_scan_cache_key(
    raw_cache_key: &str,
    allowed_types: &HashSet<String>,
    selected_nodes: &HashSet<u64>,
    secondary_groups: &[Vec<String>],
    scope: Option<&Value>,
) -> String {
    let mut types: Vec<&str> = allowed_types.iter().map(String::as_str).collect();
    types.sort_unstable();
    let mut nodes: Vec<u64> = selected_nodes.iter().copied().collect();
    nodes.sort_unstable();
    let scope_text = scope
        .and_then(|value| serde_json::to_string(value).ok())
        .unwrap_or_default();
    let secondary_text = serde_json::to_string(secondary_groups).unwrap_or_default();
    format!(
        "{raw_cache_key}\u{1e}types={}\u{1e}nodes={:?}\u{1e}scope={scope_text}\u{1e}secondary={secondary_text}",
        types.join(","),
        nodes
    )
}

pub(crate) fn get_scan_record_cache(key: &str) -> Option<ScanRecordCacheEntry> {
    scan_record_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key).cloned())
}

pub(crate) fn put_scan_record_cache(key: String, entry: ScanRecordCacheEntry) {
    let Ok(mut cache) = scan_record_cache().lock() else {
        return;
    };
    let max_entries = max_scan_record_cache_entries();
    if cache.len() >= max_entries && !cache.contains_key(&key) {
        if let Some(first_key) = cache.keys().next().cloned() {
            cache.remove(&first_key);
        }
    }
    cache.insert(key, entry);
}

pub(crate) fn get_filtered_scan_cache(key: &str) -> Option<FilteredScanCacheEntry> {
    filtered_scan_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key).cloned())
}

pub(crate) fn put_filtered_scan_cache(key: String, entry: FilteredScanCacheEntry) {
    let Ok(mut cache) = filtered_scan_cache().lock() else {
        return;
    };
    let max_entries = max_filtered_scan_cache_entries();
    if cache.len() >= max_entries && !cache.contains_key(&key) {
        if let Some(first_key) = cache.keys().next().cloned() {
            cache.remove(&first_key);
        }
    }
    cache.insert(key, entry);
}
