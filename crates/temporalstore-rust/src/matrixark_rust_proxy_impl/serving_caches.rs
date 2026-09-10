// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// The proxy's serving caches, split from matrixark_rust_proxy_impl.rs (textually include!d;
// shares parent use-imports + flat scope; no use-statements or mod wrapper).
//
// One shard snapshot cache holding payloads, their parses and their prepared candidates; the
// record-count cache; the scan cache; and the candidate-snapshot cache. They are together because
// they are invalidated together: the write path patches or drops a shard's entry, and everything
// derived from it goes with it, which is what keeps a derivative from outliving what it describes.

/// Shard and index snapshots, shared rather than copied.
///
/// The map used to hold the `BTreeMap` itself, which meant a deep copy of the whole snapshot
/// twice: once to store it and once on every hit. For a record shard the values ARE the payloads,
/// so a retrieve touching three shards copied three shards' worth of records to read the handful
/// of fields its index named -- paid in CPU, in allocator traffic, and in a transient doubling of
/// the proxy's resident set at exactly its high-water mark. The copy also grows with the store,
/// which is the shape the soak showed: latency climbing steadily from the first sample on a
/// FRESH store, with no failures and no memory pressure to blame it on.
///
/// `Arc` makes a hit a refcount bump. The write side still patches snapshots in place through
/// `Arc::make_mut`, which copies only while a reader is actually holding one; readers hold theirs
/// for the length of a single call, so in practice it does not copy at all.
/// One cached hash, with what it costs and when it was last wanted.
struct SnapshotEntry {
    map: Arc<BTreeMap<String, String>>,
    /// The shard's records, already parsed, or `None` if nothing has asked for them yet.
    ///
    /// Kept HERE rather than in a cache of its own so that it is dropped by the same call that
    /// makes it stale: the write path patches and removes these entries, and a patched payload
    /// map with a stale decode beside it is the one shape that must not exist.
    decoded: Option<Arc<Vec<Value>>>,
    /// The shard's candidates, per scope signature, or empty until something asks.
    ///
    /// Beside the payloads for the same reason `decoded` is: the write path patches and removes
    /// these entries, so a stale candidate list is dropped by the call that made it stale.
    ///
    /// Keyed by scope because the filter is: one caller means one key, and a second scope pays its
    /// own build once per shard per write rather than on every retrieve.
    prepared: BTreeMap<String, Arc<Vec<CachedRetrieveCandidate>>>,
    /// The shard's inventory counts, or `None` until something asks.
    ///
    /// No scope key, unlike `prepared`: the counters are scope-free, and the scope only enters
    /// when counts are finished into an inventory, once per rebuild rather than once per shard.
    counts: Option<Value>,
    /// The payload bytes. `decoded` is charged separately, so this stays comparable to `weigh`.
    bytes: usize,
    /// What the decoded records and the prepared candidates are charged at, or 0 for none.
    ///
    /// Both are derived from the payloads and both are rebuilt by re-reading them, so they share
    /// one budget: what matters is how much derived data the process holds, not which kind.
    decoded_bytes: usize,
    used: u64,
}

/// Cached shard and index snapshots under a byte budget.
///
/// This cache had NO bound of any kind: it kept a full in-memory copy of every record shard it
/// ever read, and for a record shard the values are the payloads themselves. That is why the
/// proxy's resident set tracked the corpus at roughly ten times durable and ended in an OOM kill
/// at 4.5-5.2 GB rather than settling anywhere.
///
/// The budget is in BYTES, deliberately. A cap on the NUMBER of entries says nothing about memory
/// when the entries are whole shards of variable-size records -- an entry-count cap tried here
/// before changed the resident set by nothing at all, because the count was never what was large.
///
/// Eviction is always safe: this is a read-through cache and a miss re-reads from the engine, the
/// same path a key that was never cached takes. Least-recently-used, found by scanning, because
/// an eviction frees a whole shard and so happens far too rarely to be worth an index.
/// What a shard's parsed records are charged at, as a multiple of the payload bytes they came
/// from. A `serde_json::Value` tree is several times the text it was parsed from -- every map is a
/// `BTreeMap` of `String` keys and every number is a `Value` -- and the exact factor varies by
/// record shape, so this is a deliberate over-estimate: the cost of guessing low is an unbudgeted
/// cache, which is what this type exists to prevent.
const DECODED_BYTES_PER_PAYLOAD_BYTE: usize = 4;

struct SnapshotCache {
    entries: BTreeMap<String, SnapshotEntry>,
    bytes: usize,
    /// Charged parsed-record bytes, budgeted SEPARATELY from the payloads.
    ///
    /// Sharing one budget would cut the number of shards the payload cache can hold to about a
    /// seventh, and every shard that stopped fitting would be re-READ as well as re-parsed --
    /// a change meant to remove parsing causing more work than it saves, on exactly the large
    /// stores that need it most. Evicting a decode costs a parse; evicting a payload costs a
    /// read too, so they are worth keeping on different terms.
    decoded_bytes: usize,
    clock: u64,
    budget: usize,
    decoded_budget: usize,
}

impl SnapshotCache {
    fn new() -> Self {
        let budget = default_snapshot_cache_bytes();
        Self {
            entries: BTreeMap::new(),
            bytes: 0,
            decoded_bytes: 0,
            clock: 0,
            budget,
            // The SAME budget as the payloads, not a fraction of it.
            //
            // This was half, while each parse was charged at six times its payload bytes -- so the
            // parse cache could hold about a twelfth of the payload volume and evicted most of
            // what it was given. Measured on a 27,186-record store, that left the rebuild's read
            // stage at 367-540 ms; with this budget it is 0.1 ms, and the rebuild after a write
            // went 662-720 ms to 153 ms, for 6% more resident memory.
            //
            // Evicting a parse still costs less than evicting a payload -- one re-parse against a
            // re-read AND a re-parse -- which is why they keep separate budgets and separate LRUs
            // rather than one shared pool.
            decoded_budget: budget,
        }
    }

    fn weigh(map: &BTreeMap<String, String>) -> usize {
        // The strings dominate; per-node overhead is a rounding error beside a payload and would
        // only make the budget pessimistic in a way that varies by allocator.
        map.iter()
            .map(|(field, value)| field.len() + value.len())
            .sum()
    }

    fn get(&mut self, key: &str) -> Option<Arc<BTreeMap<String, String>>> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(key)?;
        entry.used = clock;
        Some(Arc::clone(&entry.map))
    }

    fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes = self.bytes.saturating_sub(entry.bytes);
            self.decoded_bytes = self.decoded_bytes.saturating_sub(entry.decoded_bytes);
        }
    }

    /// The shard's parsed records, if this entry still has them.
    fn decoded(&mut self, key: &str) -> Option<Arc<Vec<Value>>> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(key)?;
        entry.used = clock;
        entry.decoded.clone()
    }

    /// The shard's candidates for a scope, if this entry still has them.
    fn prepared_for_scope(&mut self, key: &str, signature: &str) -> Option<Arc<Vec<CachedRetrieveCandidate>>> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(key)?;
        entry.used = clock;
        entry.prepared.get(signature).cloned()
    }

    /// The shard's inventory counts, if this entry still has them.
    fn counts_for(&mut self, key: &str) -> Option<Value> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(key)?;
        entry.used = clock;
        entry.counts.clone()
    }

    /// Attach a shard's inventory counts. Not charged: this is a fixed handful of integers per
    /// shard, and charging it would cost more in bookkeeping than it accounts for.
    fn set_counts(&mut self, key: &str, counts: Value) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.counts = Some(counts);
        }
    }

    /// Attach a shard's candidates for a scope, and charge them.
    ///
    /// Charged at the payload bytes rather than a multiple: the candidates are a filtered subset
    /// of the records, holding a ref of roughly a record's size for the ones that survive. Like
    /// `set_decoded`, this does nothing when the key is gone -- the entry was patched or evicted
    /// while the build ran, so these candidates describe a shard the cache no longer holds.
    fn set_prepared(
        &mut self,
        key: &str,
        signature: &str,
        candidates: Arc<Vec<CachedRetrieveCandidate>>,
    ) {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        let charged = entry.bytes;
        if entry.prepared.insert(signature.to_string(), candidates).is_none() {
            self.decoded_bytes = self.decoded_bytes.saturating_add(charged);
            entry.decoded_bytes = entry.decoded_bytes.saturating_add(charged);
        }
        self.evict_decoded_to_budget();
    }

    /// Attach parsed records to an entry that is still present, and charge them.
    ///
    /// Does nothing when the key is gone: the entry was patched or evicted while the parse was
    /// running, so these records describe a payload map the cache no longer holds.
    fn set_decoded(&mut self, key: &str, records: Arc<Vec<Value>>) {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        let charged = entry.bytes.saturating_mul(DECODED_BYTES_PER_PAYLOAD_BYTE);
        self.decoded_bytes = self.decoded_bytes.saturating_sub(entry.decoded_bytes) + charged;
        entry.decoded = Some(records);
        entry.decoded_bytes = charged;
        self.evict_decoded_to_budget();
    }

    fn insert(&mut self, key: String, map: Arc<BTreeMap<String, String>>) {
        self.remove(&key);
        let bytes = Self::weigh(&map);
        // A single snapshot larger than the whole budget is not cached rather than being cached
        // and immediately evicting everything else to make room for itself.
        if bytes > self.budget {
            return;
        }
        self.clock += 1;
        self.entries.insert(
            key,
            SnapshotEntry {
                map,
                decoded: None,
                prepared: BTreeMap::new(),
                counts: None,
                bytes,
                decoded_bytes: 0,
                used: self.clock,
            },
        );
        self.bytes += bytes;
        self.evict_to_budget();
    }

    /// Apply `patch` to a cached snapshot in place, re-weighing it afterwards.
    ///
    /// The write side keeps snapshots current rather than dropping them, so this is how a
    /// patched snapshot stays accounted for; a patch that grew a shard and did not re-weigh it
    /// would let the budget drift upward silently, which is the bug this whole type exists to
    /// prevent. `patch` returns false to say the snapshot should be dropped instead.
    fn patch<F>(&mut self, key: &str, patch: F)
    where
        F: FnOnce(&mut BTreeMap<String, String>) -> bool,
    {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        let keep = patch(Arc::make_mut(&mut entry.map));
        if !keep || entry.map.is_empty() {
            self.remove(key);
            return;
        }
        // The payloads just changed, so anything parsed from them describes the shard as it WAS.
        // Dropped here, in the same call that changed them, rather than invalidated from outside.
        // The payloads just changed, so the candidates built from them describe the shard as it
        // WAS, exactly as the parse does.
        let dropped = entry.decoded_bytes;
        entry.decoded = None;
        entry.prepared.clear();
        entry.counts = None;
        entry.decoded_bytes = 0;
        let was = entry.bytes;
        let now = Self::weigh(&entry.map);
        entry.bytes = now;
        self.decoded_bytes = self.decoded_bytes.saturating_sub(dropped);
        self.bytes = self.bytes.saturating_sub(was) + now;
        self.evict_to_budget();
    }

    /// Drop the least recently used DECODES until they fit their budget, leaving the payloads.
    fn evict_decoded_to_budget(&mut self) {
        while self.decoded_bytes > self.decoded_budget {
            let Some(victim) = self
                .entries
                .iter()
                .filter(|(_, entry)| entry.decoded.is_some() || !entry.prepared.is_empty())
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            let Some(entry) = self.entries.get_mut(&victim) else {
                return;
            };
            self.decoded_bytes = self.decoded_bytes.saturating_sub(entry.decoded_bytes);
            entry.decoded = None;
            entry.prepared.clear();
            // The counts are a handful of integers; they are dropped with the rest so an evicted
            // shard has nothing derived left behind, not because they are large.
            entry.counts = None;
            entry.decoded_bytes = 0;
        }
    }

    fn evict_to_budget(&mut self) {
        while self.bytes > self.budget {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            self.remove(&victim);
        }
    }
}

/// The snapshot budget: a sixteenth of RAM, held between 64 MiB and 512 MiB.
///
/// Same shape as the engine's own cache sizing, so a small box does not hand this cache a budget
/// its RAM cannot back. `MATRIXARK_PROXY_SNAPSHOT_CACHE_BYTES` overrides it; a zero or unparsable
/// value falls back to the derived default rather than disabling the cache, because a cache of
/// size zero turns every sweep back into per-field reads.
fn default_snapshot_cache_bytes() -> usize {
    const FLOOR: usize = 64 * 1024 * 1024;
    const CEILING: usize = 512 * 1024 * 1024;
    if let Some(raw) = std::env::var("MATRIXARK_PROXY_SNAPSHOT_CACHE_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        if raw > 0 {
            return raw;
        }
    }
    let total = total_memory_bytes();
    if total == 0 {
        return FLOOR;
    }
    (total / 16).clamp(FLOOR, CEILING)
}

/// Total RAM in bytes, or 0 when it cannot be read.
fn total_memory_bytes() -> usize {
    let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            if let Some(kb) = rest.split_whitespace().next() {
                if let Ok(kb) = kb.parse::<usize>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
}


/// What the serving caches are holding, for the process to report about itself.
///
/// The proxy's resident set has been optimised twice this week against readings that could not say
/// which part of it was which: the payload cache, the derived cache, the candidate snapshots and
/// the engine this process holds open are all in the same RSS, and only the last of them reported
/// anything. Returned as a tuple rather than a struct because it has exactly one caller, the
/// metrics render, and a struct here would be a type nothing else names.
///
/// `(entries, payload bytes, derived bytes, payload budget, derived budget)`.
fn serving_cache_gauges() -> (usize, usize, usize, usize, usize) {
    match hgetall_snapshot_cache().lock() {
        Ok(cache) => (
            cache.entries.len(),
            cache.bytes,
            cache.decoded_bytes,
            cache.budget,
            cache.decoded_budget,
        ),
        // A poisoned lock must not cost the rest of the response.
        Err(_) => (0, 0, 0, 0, 0),
    }
}

/// How many candidate snapshots are held, and how many scan results.
///
/// Both are cleared per store when it is written, so a number that keeps climbing here means
/// writes are not reaching the invalidation, not that the cache is unbounded.
fn serving_derived_cache_entries() -> (usize, usize) {
    let candidates = retrieve_candidate_cache()
        .lock()
        .map(|cache| cache.len())
        .unwrap_or(0);
    let scans = matrixark_scan_cache()
        .lock()
        .map(|cache| cache.len())
        .unwrap_or(0);
    (candidates, scans)
}

fn hgetall_snapshot_cache() -> &'static Mutex<SnapshotCache> {
    static HGETALL_SNAPSHOT_CACHE: OnceLock<Mutex<SnapshotCache>> = OnceLock::new();
    HGETALL_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(SnapshotCache::new()))
}

fn hgetall_snapshot_cache_has_entries() -> bool {
    hgetall_snapshot_cache()
        .lock()
        .map(|cache| !cache.is_empty())
        .unwrap_or(false)
}

fn record_count_cache() -> &'static Mutex<BTreeMap<String, String>> {
    static RECORD_COUNT_CACHE: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
    RECORD_COUNT_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn retrieve_candidate_cache() -> &'static Mutex<BTreeMap<String, Arc<RetrieveCandidateSnapshot>>> {
    static RETRIEVE_CANDIDATE_CACHE: OnceLock<
        Mutex<BTreeMap<String, Arc<RetrieveCandidateSnapshot>>>,
    > = OnceLock::new();
    RETRIEVE_CANDIDATE_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn matrixark_scan_cache() -> &'static Mutex<BTreeMap<String, Value>> {
    static MATRIXARK_SCAN_CACHE: OnceLock<Mutex<BTreeMap<String, Value>>> = OnceLock::new();
    MATRIXARK_SCAN_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn clear_matrixark_scan_cache() {
    if let Ok(mut cache) = matrixark_scan_cache().lock() {
        cache.clear();
    }
}

fn is_record_count_key(key: &str) -> bool {
    key.ends_with(":record_count")
}

fn update_record_count_cache(key: &str, value: &[u8]) {
    if !is_record_count_key(key) {
        return;
    }
    if let Ok(text) = std::str::from_utf8(value) {
        if let Ok(mut cache) = record_count_cache().lock() {
            cache.insert(key.to_string(), text.to_string());
        }
    }
}

fn invalidate_record_count_cache(key: &str) {
    if !is_record_count_key(key) {
        return;
    }
    if let Ok(mut cache) = record_count_cache().lock() {
        cache.remove(key);
    }
}

// TemporalStore conformance with the native storage engine: the storage engine's
// record/serving SEQUENCE is an engine-owned MONOTONIC log id, taken from the append
// log's own iterator id and exposed read-only. It is advanced only by the append log /
// commit and is never a client
// read-modify-write of a stored count, so a stale read can never make it regress.
//
// The MatrixArk serving record-log counter (`{prefix}:record_count`) is instead computed
// client-side (Python `_get_count()` + `_record_location(sequence)`), a read-modify-write.
// Under SYNCHRONOUS commit a stale/low counter read makes a subsequent write REGRESS the
// stored counter; that cascades (later turns read low, replay low sequences and OVERWRITE
// earlier serving records -> fact records clobbered -> sync retrieval collapses to 0/14,
// while async stays 14/14). Mirror the contract at the engine boundary: a record_count
// write can only ADVANCE the stored counter, never lower it -> the client always reads a
// correct high sequence -> placement never regresses. Gated (default on;
// MATRIXARK_MONOTONIC_RECORD_COUNT=0 restores prior behavior). Inert for async, whose
// counter already advances monotonically (the clamp only ever raises a low write).
fn clamp_record_count_value(engine: &RecordStore, key: &str, value: Vec<u8>) -> Vec<u8> {
    if !is_record_count_key(key) {
        return value;
    }
    let Some(new_count) = std::str::from_utf8(&value)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
    else {
        return value;
    };
    let existing = read_record_count(engine, key)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if existing > new_count {
        return existing.to_string().into_bytes();
    }
    value
}

fn clamp_record_count_command(engine: &RecordStore, command: Command) -> Command {
    match command {
        Command::StringSet { key, value } if is_record_count_key(&key) => {
            let value = clamp_record_count_value(engine, &key, value);
            Command::StringSet { key, value }
        }
        other => other,
    }
}
