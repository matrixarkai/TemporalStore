// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What it costs to answer "this key does not exist".
//!
//! WHY THIS EXISTS. Right after a cold start or a failover many readers retry at once for keys that
//! are not there. If each of those answers costs physical work, a burst of requests whose every
//! answer is "no" turns into a burst of storage work. So the question is not whether the store
//! answers correctly -- it does -- but what the answer COSTS, and whether it costs more when the
//! key's bucket is not resident.
//!
//! THE HALF THAT WAS ALREADY CHEAP, AND IT IS THE HALF WORTH SAYING FIRST. An absent key never
//! touches the block store. Not when its bucket is resident, not when its bucket has been released,
//! and not when its bucket has never existed: 0 block-store reads per lookup in all three arms, at
//! 500, 2,000 and 8,000 keys. There is no bucket reload, no page read and no manifest consultation
//! behind a "no". That is a clear negative and no plant can falsify it, which is said plainly at
//! the arm that holds it rather than left to look like a proven claim.
//!
//! AND THE READ PATH CAN TELL "ABSENT" FROM "NOT RESIDENT", WHICH IS WHY. A release drops a
//! bucket's `block_index` and its object-lookup entries and leaves the model maps alone -- the model
//! maps are what `reload_released_bucket` rebuilds FROM. So `shard.string_address(key)` is one map
//! get that answers a released bucket's keys exactly as it answers a resident one's, and the read
//! path never has to ask storage about residency in order to answer about existence.
//!
//! THE HALF THAT WAS NOT CHEAP, AND WHAT IT WAS. The two read routes were not the same price:
//!
//!   * `engine.execute` resolves through `read_bucket_index_value`, which asks
//!     `bucket_index_block_address` for an address and returns `None` without calling
//!     `read_block_bytes` when there is none. Nothing below the resident maps was touched: 0
//!     descents, 0 cache admissions.
//!
//!   * `engine.execute_durable` -- the route a deployment serves `GET` through, and the only one
//!     that enters `execute_read_only_fast_path` -- held `address: shard.string_address(key)` and
//!     then wrapped the read in `cached_response`, which is CACHE-FIRST. So an absent key descended
//!     every tier before anything looked at the address it already had: the memory tier, the
//!     persistent-memory tier, the SSD tier's store, and then `fs::read` on the entry's own disk
//!     path -- a failing `openat` for a key that was never written. Then it admitted the negative
//!     answer to the memory tier, so a burst of distinct absent keys was cache pressure too.
//!
//! MEASURED, BEFORE: 1.000 tier descents and 1.000 cache admissions per absent lookup on the
//! deployed route, flat across 500 / 2,000 / 8,000-key stores, identical in all three bucket
//! residencies, against 0.000 and 0.000 on the direct route for the same keys in the same process.
//!
//! THE REMEDY IS NOT A NEW STRUCTURE, AND THAT IS THE RESULT. The thing that can say "definitely
//! absent" without a read already exists, is already read two statements earlier in the same
//! function, and is EXACT: `shard.string_address(key)`. It costs no memory, has no false-positive
//! rate, and needs no invalidation story, because it is not a summary of the model map -- it IS the
//! model map. A sketch or filter sized for this would have been a second, approximate copy of an
//! answer already in hand. The change is that the fast path stops throwing that answer away, and
//! the sibling `FastPathRead::Hash` arm ten lines below it already worked this way.
//!
//! WHAT THIS ASSERTS NOW. Zero descents and zero cache admissions for an absent key, on both
//! routes, in all three residencies. And a PRESENT key, on the same fixture in the same process,
//! still paying a descent on its first read and still being served from memory on its second --
//! because a claim that a cost is gone is only falsifiable against a path that still pays it, and
//! because a change that had removed the cache from this route altogether would otherwise read as a
//! success in every arm here.
//!
//! ONE CONCLUSION PER TEST, DELIBERATELY. A plant stops at the first failing assertion, so a second
//! conclusion sitting behind an overlapping first one can never be reached by its own plant. The
//! descent clause and the admission clause are therefore separate tests per residency, each with
//! its own fixture and its own regime assertions ahead of it.
//!
//! THE REGIME IS ASSERTED, NOT HOPED FOR. A probe whose buckets are all resident never reaches the
//! contested case and would agree with whatever it was asked. Each arm asserts the residency of the
//! bucket its absent keys ROUTE TO -- present and in memory, present and released, or absent from
//! the map entirely -- and fails when the fixture stops reaching that state. Each arm also asserts
//! that its keys really are absent and that its present keys really answer the bytes they were
//! written with, because a probe that accidentally reads a live key is measuring a hit.
//!
//! THE DENOMINATOR IS NAMED. `CacheStats::misses` counts exactly the reads that fell through every
//! tier, which is to say the reads that reached `read_ssd_block` and found nothing -- one SSD-store
//! `get` plus one failing `fs::read` each. `expired_reads` is the only other contributor to that
//! counter, and every span asserts its delta is zero, so a miss here is a tier descent and nothing
//! else. The figures are reported PER LOOKUP over the lookups driven.
//!
//! WHAT IS DELIBERATELY NOT CLAIMED. This never said the descent was slow in wall time; a
//! bloom-filtered store `get` and a failing `openat` are both cheap individually. It said the
//! descent HAPPENED, once per distinct absent key, unconditionally, and that it admitted a cache
//! entry. Those are counts, and counts are what a retry burst multiplies.

#![allow(clippy::all)]
#![allow(dead_code)]
use super::*;

const SHARD: u64 = 1;
/// Probe lookups per span. Every per-lookup figure below is over this.
const LOOKUPS: usize = 200;
/// The store size the single-size arms use. The sweep arm is what varies it.
const PROBE_KEYS: usize = 2_000;

/// The narrow range the existing-bucket arms use: 32 buckets, so every absent key routes into one
/// of them and the seed fills all of them.
const NARROW_START: u32 = 0;
const NARROW_END: u32 = 31;

/// A key that was never written, numbered so two spans cannot share one.
fn absent_key(arm: &str, index: usize) -> String {
    format!("absent-{arm}-{index:06}")
}

/// A key the fixture seeded.
fn present_key(index: usize) -> String {
    format!("seeded-{index:06}")
}

/// The value a seeded key holds, derived from the key so a read of the WRONG key fails loudly.
fn seeded_value(index: usize) -> Vec<u8> {
    let mut value = format!("{index:06}:").into_bytes();
    value.resize(96, b'v');
    value[95] = (index % 251) as u8;
    value
}

/// `keys` string records over a chosen routing range, dumped so their buckets are releasable.
///
/// THE RANGE IS A PARAMETER BECAUSE IT DECIDES THE SHAPE. At the default `0..u32::MAX` every key
/// gets a bucket of its own, so an absent key's bucket is almost certainly not in the map at all --
/// which is one of the three arms and is useless for the other two. A narrow range puts many keys
/// in each bucket, so every absent key routes into a bucket that EXISTS and whose residency the arm
/// can then set.
fn fixture(
    keys: usize,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: present_key(index),
                value: seeded_value(index),
            },
        });
        assert!(response.status.ok, "seed {index}: {:?}", response.status);
    }
    // A dirty bucket refuses release, so the dump is what makes the released arm reachable at all.
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("a loaded shard can be dumped");
    engine.clear_dumped_bucket_dirty_state(SHARD, &manifest);
    // REGIME: the seed landed. An empty store has no buckets at all, and every arm below would
    // collapse into the no-bucket one without saying so.
    let seeded = engine
        .shards
        .read()
        .expect("shards lock poisoned")
        .get(&SHARD)
        .expect("shard is loaded")
        .strings()
        .len();
    assert_eq!(
        keys, seeded,
        "the seed did not land, so nothing measured on this fixture means anything"
    );
    (dir, engine)
}

/// Which route a lookup takes. Both answer `GET`; only one of them is the route a deployment uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// `engine.execute` -- no storage override, so `execute_read_only_fast_path` is never entered.
    Direct,
    /// `engine.execute_durable` -- what `RecordStore::Local` serves reads through, and the only
    /// route into the fast path.
    Durable,
}

fn read_through(engine: &TemporalEngine, route: Route, key: &str) -> Option<Vec<u8>> {
    let request = ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: key.to_string(),
        },
    };
    let response = match route {
        Route::Direct => engine.execute(request),
        Route::Durable => engine.execute_durable(request),
    };
    assert!(
        response.status.ok,
        "{route:?} read of {key}: {:?}",
        response.status
    );
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("{route:?} read of {key} answered {other:?}"),
    }
}

/// The physical work one span of lookups did, taken as a delta.
///
/// `block_store_file_read_counts` is PROCESS-WIDE and shared by every store in the process, so it
/// is only meaningful as a delta across a span -- which is what this is, and why these arms run on
/// one thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Work {
    /// Cache reads that fell through every tier: one SSD-store `get` plus one failing `fs::read`.
    cache_misses: u64,
    /// Cache entries admitted. On an absent key this was the negative answer being cached.
    cache_puts: u64,
    /// Reads served from the memory tier.
    cache_memory_hits: u64,
    /// Entries the memory tier evicted to make room.
    cache_memory_evictions: u64,
    /// Reads that found an entry past its time to live. The only other contributor to
    /// `cache_misses`, asserted zero so a miss is a tier descent and nothing else.
    cache_expired_reads: u64,
    /// Read calls against the block store's own files.
    block_store_reads: u64,
    /// Bytes read off the block store's own files.
    block_store_bytes: u64,
}

impl Work {
    fn take(engine: &TemporalEngine) -> Self {
        let stats = engine.cache().stats();
        let (bytes, reads) = crate::block_store::block_store_file_read_counts();
        Self {
            cache_misses: stats.misses,
            cache_puts: stats.puts,
            cache_memory_hits: stats.memory_hits,
            cache_memory_evictions: stats.memory_evictions,
            cache_expired_reads: stats.expired_reads,
            block_store_reads: reads,
            block_store_bytes: bytes,
        }
    }

    fn since(self, before: Work) -> Self {
        Self {
            cache_misses: self.cache_misses.saturating_sub(before.cache_misses),
            cache_puts: self.cache_puts.saturating_sub(before.cache_puts),
            cache_memory_hits: self
                .cache_memory_hits
                .saturating_sub(before.cache_memory_hits),
            cache_memory_evictions: self
                .cache_memory_evictions
                .saturating_sub(before.cache_memory_evictions),
            cache_expired_reads: self
                .cache_expired_reads
                .saturating_sub(before.cache_expired_reads),
            block_store_reads: self
                .block_store_reads
                .saturating_sub(before.block_store_reads),
            block_store_bytes: self
                .block_store_bytes
                .saturating_sub(before.block_store_bytes),
        }
    }

    fn print(&self, label: &str, route: Route, keys: usize, lookups: usize) {
        println!(
            "{label:<20} route={:<8} keys={keys:<6} misses={:<5} puts={:<5} mem_hits={:<5} \
             mem_evict={:<5} bs_reads={:<5} bs_bytes={:<9}  misses/lookup={:.3} \
             puts/lookup={:.3} bs_reads/lookup={:.3}",
            format!("{route:?}"),
            self.cache_misses,
            self.cache_puts,
            self.cache_memory_hits,
            self.cache_memory_evictions,
            self.block_store_reads,
            self.block_store_bytes,
            self.cache_misses as f64 / lookups as f64,
            self.cache_puts as f64 / lookups as f64,
            self.block_store_reads as f64 / lookups as f64,
        );
    }
}

/// How the bucket an absent key routes to is expected to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Residency {
    /// In the bucket map and holding its pages.
    ResidentBucket,
    /// In the bucket map, pages released, reloadable.
    ReleasedBucket,
    /// Not in the bucket map at all.
    NoBucketAtAll,
}

/// Assert that every probe key in this span routes to a bucket in the expected state.
///
/// THIS IS THE REGIME CLAUSE AND IT COMES FIRST. Without it a span named "released" measures a
/// resident bucket the moment the fixture's range or seed changes, and agrees with whatever it is
/// asked.
fn assert_routing_regime(
    engine: &TemporalEngine,
    arm: &str,
    residency: Residency,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let mut checked = 0usize;
    for index in 0..LOOKUPS {
        let key = absent_key(arm, index);
        let routing_bucket = crate::engine::hashing::block_routing_bucket(
            &key,
            start_routing_bucket,
            end_routing_bucket,
        );
        let bucket = shard.bucket_index.bucket_map.get(&routing_bucket);
        match residency {
            Residency::ResidentBucket => {
                let bucket = bucket.unwrap_or_else(|| {
                    panic!(
                        "{arm}: key {key} routes to bucket {routing_bucket}, which is NOT in the \
                         map -- this span is not measuring a resident bucket"
                    )
                });
                assert!(
                    bucket.in_memory(),
                    "{arm}: key {key} routes to bucket {routing_bucket}, which is in the map but \
                     NOT in memory -- this span is not measuring a resident bucket"
                );
                assert!(
                    !shard.bucket_index.released_buckets.contains(&routing_bucket),
                    "{arm}: bucket {routing_bucket} is in the released registry"
                );
            }
            Residency::ReleasedBucket => {
                let bucket = bucket.unwrap_or_else(|| {
                    panic!(
                        "{arm}: key {key} routes to bucket {routing_bucket}, which is NOT in the \
                         map -- a released bucket must stay routable, so this span is measuring \
                         the wrong thing"
                    )
                });
                assert!(
                    !bucket.in_memory() && bucket.meta_loaded() && !bucket.loading(),
                    "{arm}: key {key} routes to bucket {routing_bucket}, whose residency flags do \
                     not say released -- this span is not measuring a non-resident bucket"
                );
                assert!(
                    shard.bucket_index.released_buckets.contains(&routing_bucket),
                    "{arm}: bucket {routing_bucket} is not in the released registry"
                );
            }
            Residency::NoBucketAtAll => {
                assert!(
                    bucket.is_none(),
                    "{arm}: key {key} routes to bucket {routing_bucket}, which IS in the map -- \
                     this span is not measuring a bucket that does not exist"
                );
            }
        }
        checked += 1;
    }
    assert_eq!(
        LOOKUPS, checked,
        "{arm}: the regime was checked for {checked} of {LOOKUPS} keys"
    );
}

/// Release every bucket on the shard, asserting that it actually happened.
///
/// THE RELEASE IS A REGIME CLAUSE, NOT A STEP. A release that reports a number and drops nothing
/// leaves the arm after it measuring a resident bucket under a released arm's name.
fn release_everything(engine: &TemporalEngine) {
    let (released_buckets, released_blocks, refused) =
        engine.release_all_releasable_bucket_index_blocks(SHARD);
    assert!(
        released_buckets > 0 && released_blocks > 0,
        "nothing was released ({released_buckets} buckets / {released_blocks} pages, {refused} \
         refused), so what follows is not about a released bucket"
    );
    let resident_blocks: usize = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        shards
            .get(&SHARD)
            .expect("shard is loaded")
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.block_index.len())
            .sum()
    };
    assert_eq!(
        0, resident_blocks,
        "{released_blocks} pages were reported released but {resident_blocks} are still filed, so \
         the release reported a number and dropped nothing"
    );
}

/// Drive `LOOKUPS` ABSENT-key lookups on one route and count what they cost.
fn measure_absent(engine: &TemporalEngine, arm: &str, route: Route, keys: usize) -> Work {
    let before = Work::take(engine);
    let mut answered_absent = 0usize;
    for index in 0..LOOKUPS {
        let key = absent_key(arm, index);
        // A key that answers bytes is not an absent key, and everything after this would be the
        // cost of a HIT reported as the cost of a miss.
        assert_eq!(
            None,
            read_through(engine, route, &key),
            "{arm}/{route:?}: {key} was supposed to be absent"
        );
        answered_absent += 1;
    }
    assert_eq!(
        LOOKUPS, answered_absent,
        "{arm}/{route:?}: {answered_absent} of {LOOKUPS} lookups ran"
    );
    let work = Work::take(engine).since(before);
    assert_eq!(
        0, work.cache_expired_reads,
        "{arm}/{route:?}: {} expired reads landed in this span, so a cache miss here is not purely \
         a tier descent and the denominator is not what it says it is",
        work.cache_expired_reads
    );
    work.print(arm, route, keys, LOOKUPS);
    work
}

/// Drive `LOOKUPS` PRESENT-key lookups on one route and count what they cost.
///
/// THIS IS THE FALSIFIER, NOT A SECOND FINDING. The claim everywhere else is that one specific cost
/// is gone for ABSENT keys. A change that had removed the cache from this route altogether would
/// make every absent arm read zero and would be indistinguishable from the intended one -- so an
/// arm that STILL PAYS has to stay reachable, in the same process, on the same fixture.
fn measure_present(engine: &TemporalEngine, arm: &str, route: Route, keys: usize) -> Work {
    assert!(
        LOOKUPS <= keys,
        "{arm}: the fixture seeded {keys} keys and this span wants {LOOKUPS}"
    );
    let before = Work::take(engine);
    let mut answered_present = 0usize;
    for index in 0..LOOKUPS {
        let key = present_key(index);
        // The value is a function of the key, so a read resolving through the WRONG block fails
        // here rather than passing on a lucky length.
        assert_eq!(
            Some(seeded_value(index)),
            read_through(engine, route, &key),
            "{arm}/{route:?}: {key} was supposed to be present"
        );
        answered_present += 1;
    }
    assert_eq!(
        LOOKUPS, answered_present,
        "{arm}/{route:?}: {answered_present} of {LOOKUPS} lookups ran"
    );
    let work = Work::take(engine).since(before);
    assert_eq!(
        0, work.cache_expired_reads,
        "{arm}/{route:?}: {} expired reads landed in this span",
        work.cache_expired_reads
    );
    work.print(arm, route, keys, LOOKUPS);
    work
}

/// ARM 1's reading: absent keys whose bucket exists and is RESIDENT, on the deployed route.
fn resident_bucket_arm(keys: usize) -> Work {
    let (_dir, engine) = fixture(keys, NARROW_START, NARROW_END);
    assert_routing_regime(
        &engine,
        "resident-bucket",
        Residency::ResidentBucket,
        NARROW_START,
        NARROW_END,
    );
    measure_absent(&engine, "resident-bucket", Route::Durable, keys)
}

/// ARM 2's reading: absent keys whose bucket exists and has been RELEASED, on the deployed route.
fn released_bucket_arm(keys: usize) -> Work {
    let (_dir, engine) = fixture(keys, NARROW_START, NARROW_END);
    release_everything(&engine);
    assert_routing_regime(
        &engine,
        "released-bucket",
        Residency::ReleasedBucket,
        NARROW_START,
        NARROW_END,
    );
    measure_absent(&engine, "released-bucket", Route::Durable, keys)
}

/// ARM 3's reading: absent keys whose bucket is not in the map at all, on the deployed route.
fn no_bucket_arm(keys: usize) -> Work {
    // THE DEFAULT RANGE, where every key gets a bucket of its own -- so an absent key's bucket is
    // one nothing has ever filed anything in.
    let (_dir, engine) = fixture(keys, 0, u32::MAX);
    assert_routing_regime(&engine, "no-bucket", Residency::NoBucketAtAll, 0, u32::MAX);
    measure_absent(&engine, "no-bucket", Route::Durable, keys)
}

// ---------------------------------------------------------------------------------------------
// ARM 1: the bucket exists and is resident.
// ---------------------------------------------------------------------------------------------

#[test]
fn an_absent_key_in_a_resident_bucket_descends_no_cache_tier() {
    let work = resident_bucket_arm(PROBE_KEYS);
    assert_eq!(
        0, work.cache_misses,
        "{LOOKUPS} absent lookups into resident buckets made {} tier descents. Each descent is one \
         SSD-store get plus one failing fs::read, for a key the shard's own model map had already \
         said was absent",
        work.cache_misses
    );
}

#[test]
fn an_absent_key_in_a_resident_bucket_is_admitted_to_no_cache_tier() {
    let work = resident_bucket_arm(PROBE_KEYS);
    assert_eq!(
        0, work.cache_puts,
        "{LOOKUPS} absent lookups into resident buckets admitted {} cache entries. A burst of \
         distinct absent keys that admits one entry each is cache pressure that evicts live \
         entries to hold answers that are all \"no\"",
        work.cache_puts
    );
}

// ---------------------------------------------------------------------------------------------
// ARM 2: the bucket exists and has been released. THE CONTESTED CASE.
// ---------------------------------------------------------------------------------------------

#[test]
fn an_absent_key_in_a_released_bucket_descends_no_cache_tier() {
    let work = released_bucket_arm(PROBE_KEYS);
    assert_eq!(
        0, work.cache_misses,
        "{LOOKUPS} absent lookups into RELEASED buckets made {} tier descents, so releasing a \
         bucket has started to cost absent reads",
        work.cache_misses
    );
}

#[test]
fn an_absent_key_in_a_released_bucket_is_admitted_to_no_cache_tier() {
    let work = released_bucket_arm(PROBE_KEYS);
    assert_eq!(
        0, work.cache_puts,
        "{LOOKUPS} absent lookups into RELEASED buckets admitted {} cache entries",
        work.cache_puts
    );
}

// ---------------------------------------------------------------------------------------------
// ARM 3: the bucket does not exist at all.
// ---------------------------------------------------------------------------------------------

#[test]
fn an_absent_key_whose_bucket_does_not_exist_descends_no_cache_tier() {
    let work = no_bucket_arm(PROBE_KEYS);
    assert_eq!(
        0, work.cache_misses,
        "{LOOKUPS} absent lookups whose bucket does not exist made {} tier descents",
        work.cache_misses
    );
}

#[test]
fn an_absent_key_whose_bucket_does_not_exist_is_admitted_to_no_cache_tier() {
    let work = no_bucket_arm(PROBE_KEYS);
    assert_eq!(
        0, work.cache_puts,
        "{LOOKUPS} absent lookups whose bucket does not exist admitted {} cache entries",
        work.cache_puts
    );
}

// ---------------------------------------------------------------------------------------------
// THE CLEAR NEGATIVE, AND IT IS LABELLED AS ONE.
// ---------------------------------------------------------------------------------------------

/// An absent key reads no block-store file, in any of the three residencies.
///
/// WHY THIS IS NOT A PROVEN CLAIM IN THE SAME SENSE AS THE OTHERS. No plant falsifies it: the one
/// change that makes an absent key pay again -- restoring the cache-first order in
/// `execute_read_only_fast_path` -- moves the cache counters and leaves this one at zero, because
/// `read_block_bytes` is reached only through an address and an absent key has none. So this arm is
/// a standing negative: it is the assertion that would fail if an absent answer ever started
/// needing a page, a bucket reload or a dump manifest, and it has nothing to show for being green
/// today beyond that it is.
///
/// THE THREE ARMS ARE IN ONE TEST FOR THAT REASON. Splitting an unplantable negative three ways
/// buys nothing a plant could use.
#[test]
fn an_absent_key_reads_no_block_store_file_in_any_residency() {
    let resident = resident_bucket_arm(PROBE_KEYS);
    let released = released_bucket_arm(PROBE_KEYS);
    let no_bucket = no_bucket_arm(PROBE_KEYS);
    for (name, work) in [
        ("resident bucket", resident),
        ("released bucket", released),
        ("no bucket at all", no_bucket),
    ] {
        assert_eq!(
            0, work.block_store_reads,
            "{LOOKUPS} absent lookups with the key's bucket {name} read the block store {} times \
             ({} bytes), so an absent answer has started to need a page or a bucket reload and the \
             remedy is a different one from the one this module records",
            work.block_store_reads, work.block_store_bytes
        );
    }
}

// ---------------------------------------------------------------------------------------------
// THE OTHER ROUTE, which never paid and is here so the comparison has two sides.
// ---------------------------------------------------------------------------------------------

/// `engine.execute` pays nothing for an absent key, and always did.
///
/// It resolves through `read_bucket_index_value`, which returns `None` without calling
/// `read_block_bytes` when `bucket_index_block_address` has no address -- and never consults the
/// cache at all. This is the control that made the finding attributable to the ENTRY POINT rather
/// than to absent keys in general: same fixture, same keys, same index state, different route.
#[test]
fn the_direct_read_route_pays_nothing_for_an_absent_key() {
    let (_dir, engine) = fixture(PROBE_KEYS, NARROW_START, NARROW_END);
    assert_routing_regime(
        &engine,
        "direct-route",
        Residency::ResidentBucket,
        NARROW_START,
        NARROW_END,
    );
    let work = measure_absent(&engine, "direct-route", Route::Direct, PROBE_KEYS);
    assert_eq!(
        Work {
            cache_misses: 0,
            cache_puts: 0,
            cache_memory_hits: 0,
            cache_memory_evictions: 0,
            cache_expired_reads: 0,
            block_store_reads: 0,
            block_store_bytes: 0,
        },
        work,
        "`engine.execute` did physical work for an absent key, so the control arm is no longer a \
         control and the attribution to the entry point needs revisiting"
    );
}

// ---------------------------------------------------------------------------------------------
// THE FALSIFIER. Everything above is a zero; this is what makes those zeros mean something.
// ---------------------------------------------------------------------------------------------

/// A PRESENT key on the deployed route still descends a tier and still fills the cache.
///
/// WHY IT IS A SEPARATE TEST AND NOT A CLAUSE IN THE ARMS ABOVE. The arms above assert zeros. A
/// change that removed the cache from this route entirely would satisfy every one of them, and the
/// only thing that tells that apart from the intended change is an arm that STILL PAYS. Put after
/// a zero it would never be reached by its own plant, so it is here on its own.
///
/// WHY THE FIGURE IS 2 PER LOOKUP AND NOT 1. A present read consults the cache twice under two
/// different keys: the response under `CacheKey::string`, and the block frame under
/// `CacheKey::page_with_slot` inside `read_block_bytes`. Both miss on a first read of a cold key.
/// That is why this asserts "at least one per lookup" rather than an exact count -- the claim is
/// that the path still descends, not how many times.
///
/// WHAT A PLANT CAN AND CANNOT REACH HERE, SAID PLAINLY. The over-wide version of the change --
/// taking the early return for EVERY key rather than only an absent one -- is what this arm is
/// aimed at, and it is caught by the CORRECTNESS assertion inside `measure_present`, which sits
/// ahead of the figure below and therefore fires first. So a plant proves this arm is not vacuous;
/// it does not prove the `>= LOOKUPS` bound itself, because the only change that would take the
/// descent away while still serving the right bytes is one inside `read_block_bytes`, which is a
/// different function from the one this module is about. The bound is kept as the weaker of the
/// two claims rather than dropped, because it is the one that fails if the cache leaves this route.
#[test]
fn a_present_key_still_descends_a_tier_and_still_fills_the_cache() {
    let (_dir, engine) = fixture(PROBE_KEYS, NARROW_START, NARROW_END);
    let work = measure_present(&engine, "present/control", Route::Durable, PROBE_KEYS);
    assert!(
        work.cache_misses >= LOOKUPS as u64,
        "{LOOKUPS} present lookups on the deployed route made only {} tier descents. The cache is \
         supposed to still be consulted for a key the shard HOLDS, so the zeros the absent arms \
         report may be a path with no cache in it rather than an absent key that stopped paying \
         for one",
        work.cache_misses
    );
}

#[test]
fn a_present_key_still_reads_its_block_off_the_block_store() {
    let (_dir, engine) = fixture(PROBE_KEYS, NARROW_START, NARROW_END);
    let work = measure_present(&engine, "present/control", Route::Durable, PROBE_KEYS);
    assert!(
        work.block_store_reads > 0,
        "{LOOKUPS} present lookups read the block store {} times. The block-store counter has to be \
         shown live in this process, or the zero the absent arms report could be a counter that \
         never moves",
        work.block_store_reads
    );
}

// ---------------------------------------------------------------------------------------------
// THE REPEAT, where the two kinds of key now differ in kind rather than in degree.
// ---------------------------------------------------------------------------------------------

/// An absent key is not admitted to the cache at all, so asking twice costs twice nothing.
///
/// BEFORE, the first ask paid a descent and cached the negative answer, so the SECOND ask was a
/// memory hit -- which looked like a mitigation and was not one, because the entry is per key and a
/// burst is across many distinct keys. Now there is no entry to find and nothing for a later delete
/// to have to invalidate.
#[test]
fn a_repeated_absent_key_is_still_not_cached_on_the_second_ask() {
    let (_dir, engine) = fixture(500, NARROW_START, NARROW_END);
    let missing = absent_key("repeat", 0);

    // REGIME: the key is absent and routes to a bucket that exists.
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&SHARD).expect("shard is loaded");
        assert!(
            !shard.strings().contains_key(missing.as_str()),
            "the absent probe key is in the resident map, so it is not absent"
        );
        let routing_bucket =
            crate::engine::hashing::block_routing_bucket(&missing, NARROW_START, NARROW_END);
        assert!(
            shard.bucket_index.bucket_map.contains_key(&routing_bucket),
            "{missing} routes to bucket {routing_bucket}, which is not in the map"
        );
    }

    let before = Work::take(&engine);
    assert_eq!(None, read_through(&engine, Route::Durable, &missing));
    let first = Work::take(&engine).since(before);

    let between = Work::take(&engine);
    assert_eq!(None, read_through(&engine, Route::Durable, &missing));
    let second = Work::take(&engine).since(between);

    println!(
        "absent  first misses={} puts={} mem_hits={} | second misses={} puts={} mem_hits={}",
        first.cache_misses,
        first.cache_puts,
        first.cache_memory_hits,
        second.cache_misses,
        second.cache_puts,
        second.cache_memory_hits,
    );

    assert_eq!(
        0, second.cache_memory_hits,
        "the second ask for the same absent key took {} memory hits, so the first one cached \
         something after all",
        second.cache_memory_hits
    );
}

/// And the counterpart, so "nothing was cached" is read as a fact about absent keys rather than
/// about this engine: a PRESENT key is cached on its first read and served from memory on its
/// second.
#[test]
fn a_repeated_present_key_is_cached_on_the_first_ask_and_served_from_memory_on_the_second() {
    let (_dir, engine) = fixture(500, NARROW_START, NARROW_END);
    let held = present_key(0);

    // REGIME: the key is present.
    assert!(
        engine
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&SHARD)
            .expect("shard is loaded")
            .strings()
            .contains_key(held.as_str()),
        "the present probe key is not in the resident map, so it is not present"
    );

    let before = Work::take(&engine);
    assert_eq!(
        Some(seeded_value(0)),
        read_through(&engine, Route::Durable, &held),
        "{held} did not answer the bytes it was written with on the first ask"
    );
    let first = Work::take(&engine).since(before);

    let between = Work::take(&engine);
    assert_eq!(
        Some(seeded_value(0)),
        read_through(&engine, Route::Durable, &held),
        "{held} did not answer the bytes it was written with on the second ask"
    );
    let second = Work::take(&engine).since(between);

    println!(
        "present first misses={} puts={} mem_hits={} | second misses={} puts={} mem_hits={}",
        first.cache_misses,
        first.cache_puts,
        first.cache_memory_hits,
        second.cache_misses,
        second.cache_puts,
        second.cache_memory_hits,
    );

    assert!(
        first.cache_puts > 0,
        "the first read of a present key admitted no cache entry, so this engine is not caching \
         reads at all and the absent arms' zeros say nothing"
    );
    assert!(
        second.cache_memory_hits > 0,
        "the second read of a present key took no memory hit, so the entry the first one admitted \
         is not being read back"
    );
}

// ---------------------------------------------------------------------------------------------
// THE SWEEP. The cost is per LOOKUP, which is what makes a retry burst the thing that multiplies
// it -- so it must not move with the size of the store.
// ---------------------------------------------------------------------------------------------

#[test]
fn the_absent_key_cost_does_not_move_with_the_store_size() {
    let sizes = [500usize, 2_000, 8_000];
    let mut rows: Vec<(usize, Work, Work, Work)> = Vec::new();
    for keys in sizes {
        rows.push((
            keys,
            resident_bucket_arm(keys),
            released_bucket_arm(keys),
            no_bucket_arm(keys),
        ));
    }

    // REGIME: the sweep ran at every size. A loop over an empty or short list below would agree
    // with anything.
    assert_eq!(
        sizes.len(),
        rows.len(),
        "the sweep produced {} readings for {} sizes",
        rows.len(),
        sizes.len()
    );
    assert!(
        rows[rows.len() - 1].0 > rows[0].0,
        "the sweep must actually widen: {} -> {}",
        rows[0].0,
        rows[rows.len() - 1].0
    );

    let (first_keys, first_resident, first_released, first_no_bucket) = rows[0];
    for (keys, resident, released, no_bucket) in rows.iter().copied().skip(1) {
        for (name, baseline, measured) in [
            ("resident bucket", first_resident, resident),
            ("released bucket", first_released, released),
            ("no bucket at all", first_no_bucket, no_bucket),
        ] {
            assert_eq!(
                (
                    baseline.cache_misses,
                    baseline.cache_puts,
                    baseline.block_store_reads
                ),
                (
                    measured.cache_misses,
                    measured.cache_puts,
                    measured.block_store_reads
                ),
                "with the key's bucket {name}, {LOOKUPS} absent lookups cost \
                 (descents, admissions, block-store reads) = ({}, {}, {}) in a {first_keys}-key \
                 store and ({}, {}, {}) in a {keys}-key store. An absent answer's cost is supposed \
                 to be set by the lookup and not by the corpus, so if this moves the reading needs \
                 revisiting rather than reporting",
                baseline.cache_misses,
                baseline.cache_puts,
                baseline.block_store_reads,
                measured.cache_misses,
                measured.cache_puts,
                measured.block_store_reads,
            );
        }
    }
}
