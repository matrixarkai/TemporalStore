// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a two-level resident map costs against one container per key, priced before anything is
//! built on it.
//!
//! THE SHAPE BEING PRICED. Today a series model map is `HashMap<key, BTreeMap<at, address>>`: one
//! whole B-tree per object key, however few points that key holds. The alternative keys the resident
//! map by ROUTING BUCKET instead of by object key -- one entry per bucket, holding every object that
//! routes there -- and puts the identity in the entry's payload rather than in a node of its own. The
//! payload is ONE allocation per bucket, so the container cost is paid once per bucket and amortised
//! across its members instead of once per key.
//!
//! WHAT IS DELIBERATELY NOT COPIED, because it is already refuted here: the payload is NOT a
//! variable-length packed buffer searched by linear scan. Variable-length packing is both what makes
//! such a buffer compact and what stops it being binary-searchable, and a linear-cost read path was
//! measured at 3.0-17.8x on misses. These rows are FIXED WIDTH and kept sorted, so a lookup inside a
//! bucket stays O(log n). The two-level shape is the part worth taking; the scan is not.
//!
//! WHY THE ARMS ARE COMPARABLE. Both arms are built from ONE generated row set and are checksummed
//! against each other before either is measured: same row count, same keys, same timestamps, same
//! addresses. An arm whose fixture quietly collapsed -- all keys equal, say -- would read as a
//! brilliant arm, and that is the failure this checksum exists to catch.
//!
//! WHY THE KEY TEXT IS AN `Arc<str>` IN THE SECOND ARM. A row per point carrying an owned key would
//! charge the key text once per POINT, which would price a shape nobody would build. Sharing it is
//! the point: the clone probe charges a shared string nothing, and `arc_str_bytes` adds each distinct
//! allocation back exactly once, which is what the sibling bucket-index walk already does.
//!
//! REGIMES, NOT A NUMBER. The whole question is how many points sit under one key, because that is
//! what the per-container cost is divided by. Measured at three occupancies with the key count moved
//! the other way, so the row total is held near constant and the only thing varying is the shape of
//! the population.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::block_store::BlockAddress;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The shipped routing range's width, so a bucket count here is the one an operator runs.
const SHIPPED_BUCKETS: u32 = 1_024;

/// Bytes the heap gives up to hold a deep copy of `value`, and how many allocations it took.
///
/// The allocation COUNT is reported beside the bytes because the claim being tested is about
/// collapsing allocations, not only about collapsing bytes, and the two can move in opposite
/// directions.
#[cfg(feature = "alloc-probe")]
fn deep_heap(value: &impl Clone) -> (u64, u64) {
    let probe = Probe::start();
    let copy = value.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    (counts.alloc_bytes, counts.allocs)
}

/// An `Arc<str>`'s allocation, counted once per distinct pointer: a clone bumps a refcount and
/// allocates nothing, so a shared string is invisible to the clone probe and has to be added back.
#[cfg(feature = "alloc-probe")]
fn shared_text_bytes(seen: &mut HashSet<usize>, value: &str) -> u64 {
    if !seen.insert(value.as_ptr() as usize) {
        return 0;
    }
    (((16 + value.len()) + 7) / 8 * 8) as u64
}

/// One point of one object: what both arms have to hold.
#[derive(Clone)]
struct Row {
    key: Arc<str>,
    at: u64,
    address: BlockAddress,
}

/// `keys` objects carrying `points_per_key` points each, on the shipped routing range.
fn rows(keys: usize, points_per_key: usize) -> Vec<Row> {
    let mut out = Vec::with_capacity(keys * points_per_key);
    for k in 0..keys {
        let key: Arc<str> = Arc::from(format!("f{k}"));
        for p in 0..points_per_key {
            out.push(Row {
                key: Arc::clone(&key),
                at: 1_700_000_000_000 + p as u64,
                address: BlockAddress::from_parts(
                    (k * points_per_key + p) as u64,
                    0,
                    64,
                    Some(1),
                    Some((k * points_per_key + p) as u64),
                ),
            });
        }
    }
    out
}

/// Which bucket a key routes to on the shipped range. The real placement modulus, so the occupancy
/// below is the one a store has rather than one this fixture chose.
fn bucket_of(key: &str) -> u32 {
    crate::engine::hashing::block_routing_bucket(key, 0, SHIPPED_BUCKETS - 1)
}

/// ARM A -- what ships: one B-tree per object key, PACKED.
///
/// Packed rather than ascending-inserted, because that is what a loaded index now holds: the decode
/// repacks every series as it reads it. Measuring against the ascending-insert shape would credit
/// the two-level arm with a saving that already landed, and the first version of this fixture did
/// exactly that -- it read 0.815x at a thousand points a key purely because its arm A was the old
/// shape.
fn arm_a(rows: &[Row]) -> HashMap<String, BTreeMap<u64, BlockAddress>> {
    let mut staged: HashMap<String, Vec<(u64, BlockAddress)>> = HashMap::new();
    for row in rows {
        staged
            .entry(row.key.to_string())
            .or_default()
            .push((row.at, row.address.clone()));
    }
    staged
        .into_iter()
        .map(|(key, points)| (key, points.into_iter().collect::<BTreeMap<_, _>>()))
        .collect()
}

/// ARM A as it was BEFORE the decode repack, kept as a third reading so the two changes are not
/// confused with each other.
fn arm_a_ascending(rows: &[Row]) -> HashMap<String, BTreeMap<u64, BlockAddress>> {
    let mut map: HashMap<String, BTreeMap<u64, BlockAddress>> = HashMap::new();
    for row in rows {
        map.entry(row.key.to_string())
            .or_default()
            .insert(row.at, row.address.clone());
    }
    map
}

/// ARM B -- two levels: keyed by routing bucket, one fixed-width sorted row run per bucket.
///
/// The rows carry their own identity, so the entry itself carries none -- that is the part of the
/// shape being taken. They are SORTED by (key, timestamp), so a lookup inside a bucket is a binary
/// search and not a walk.
fn arm_b(rows: &[Row]) -> HashMap<u32, Vec<(Arc<str>, u64, BlockAddress)>> {
    let mut map: HashMap<u32, Vec<(Arc<str>, u64, BlockAddress)>> = HashMap::new();
    for row in rows {
        map.entry(bucket_of(&row.key)).or_default().push((
            Arc::clone(&row.key),
            row.at,
            row.address.clone(),
        ));
    }
    for run in map.values_mut() {
        run.sort_by(|a, b| (a.0.as_ref(), a.1).cmp(&(b.0.as_ref(), b.1)));
        // One allocation per bucket, sized to what it holds: the slack a doubling `Vec` would carry
        // is the thing this shape is supposed to remove, so it must not be left in.
        run.shrink_to_fit();
    }
    map
}

/// ARM C -- one level, but a sorted VECTOR per key instead of a B-tree per key.
///
/// Not a new idea: it is the shape `HashFieldMap` already uses for the hash field maps in this tree,
/// where it took 75.82% off that map's container column. It is here because it is the cheap half of
/// what the two-level arm does -- it removes the per-key B-TREE, which is where the control's 17.5x
/// lives, without moving identity out of the key position. If this arm matches arm B where arm B
/// wins, the second level is buying nothing the first level has not already bought, and the much
/// smaller change is the right one.
fn arm_c(rows: &[Row]) -> HashMap<String, Vec<(u64, BlockAddress)>> {
    let mut map: HashMap<String, Vec<(u64, BlockAddress)>> = HashMap::new();
    for row in rows {
        map.entry(row.key.to_string())
            .or_default()
            .push((row.at, row.address.clone()));
    }
    for points in map.values_mut() {
        points.sort_by_key(|(at, _)| *at);
        points.shrink_to_fit();
    }
    map
}

/// One bucket in ARM D: its objects, and one contiguous fixed-width row run for all of them.
///
/// `objects` is sorted by key and names where each object's rows start and how many there are.
/// `rows` is grouped by object in that same order and sorted by inner key inside each group.
#[derive(Clone)]
struct GroupedBucket {
    objects: Vec<(Arc<str>, u32, u32)>,
    rows: Vec<(u64, BlockAddress)>,
}

/// ARM D -- two levels, with identity stored ONCE PER OBJECT instead of once per row.
///
/// THIS IS THE ANSWER TO WHY ARM B LOSES AS A KEY FILLS UP. Arm B pays a 16-byte shared-text pointer
/// in every row; a container a key pays it once. Arm D pays it once per object as well, and reaches
/// an object's rows through a start and a length -- eight bytes per OBJECT.
///
/// AND THAT IS WHY IT DOES NOT HAND THE BYTES BACK. The earlier result that an offset table gives up
/// the saving was about a VARIABLE-LENGTH packed buffer, where an offset is needed for every entry
/// because entries are not the same width. These rows are FIXED WIDTH, so finding a row inside an
/// object's run is arithmetic, and the only thing that needs naming is where each OBJECT's run
/// begins. Per-row offsets would indeed give it back; per-object ones cost eight bytes against a
/// whole run.
///
/// Every lookup stays logarithmic: binary search `objects` for the key, then binary search the
/// object's own contiguous slice of `rows` for the inner key. Nothing is walked.
fn arm_d(rows_in: &[Row]) -> HashMap<u32, GroupedBucket> {
    // Group by bucket, then by key inside the bucket.
    let mut staged: HashMap<u32, HashMap<Arc<str>, Vec<(u64, BlockAddress)>>> = HashMap::new();
    for row in rows_in {
        staged
            .entry(bucket_of(&row.key))
            .or_default()
            .entry(Arc::clone(&row.key))
            .or_default()
            .push((row.at, row.address.clone()));
    }
    let mut out: HashMap<u32, GroupedBucket> = HashMap::new();
    for (bucket, by_key) in staged {
        let mut keys: Vec<(Arc<str>, Vec<(u64, BlockAddress)>)> = by_key.into_iter().collect();
        keys.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
        let total: usize = keys.iter().map(|(_, points)| points.len()).sum();
        let mut objects: Vec<(Arc<str>, u32, u32)> = Vec::with_capacity(keys.len());
        let mut flat: Vec<(u64, BlockAddress)> = Vec::with_capacity(total);
        for (key, mut points) in keys {
            points.sort_by_key(|(at, _)| *at);
            let start = flat.len() as u32;
            let len = points.len() as u32;
            flat.extend(points);
            objects.push((key, start, len));
        }
        objects.shrink_to_fit();
        flat.shrink_to_fit();
        out.insert(bucket, GroupedBucket { objects, rows: flat });
    }
    out
}

/// The lookup arm D would serve, written out so the shape is not merely asserted to be searchable.
///
/// A fixture that measured bytes without ever reading one back would be pricing a structure nobody
/// could use. Two binary searches, no walk.
fn arm_d_lookup(map: &HashMap<u32, GroupedBucket>, key: &str, at: u64) -> Option<BlockAddress> {
    let bucket = map.get(&bucket_of(key))?;
    let which = bucket
        .objects
        .binary_search_by(|(name, _, _)| name.as_ref().cmp(key))
        .ok()?;
    let (_, start, len) = bucket.objects[which];
    let run = &bucket.rows[start as usize..(start + len) as usize];
    let at_index = run.binary_search_by(|(row_at, _)| row_at.cmp(&at)).ok()?;
    Some(run[at_index].1.clone())
}

// =================================================================================================
// THE INSTRUMENT, PROVEN BEFORE THE SUBJECT
// =================================================================================================

/// Two readings whose answers are known, on the same probe every figure below uses.
///
/// A probe that reports near zero for a megabyte makes every structure look free, and a probe that
/// has stopped seeing B-tree nodes makes the shape question disappear. The second reading is the one
/// that matters here: a one-entry B-tree charging many times its value is the whole reason a
/// two-level shape could win, so if that stops being true the subject has not moved -- the
/// instrument has.
#[test]
#[cfg(feature = "alloc-probe")]
fn the_instrument_reads_a_known_container_and_a_known_node() {
    const BYTES: usize = 1 << 20;
    let known: Vec<u8> = vec![7u8; BYTES];
    let (measured, allocs) = deep_heap(&known);
    println!(
        "control: a {BYTES}-byte Vec<u8> charged {measured} B in {allocs} allocations ({:.3}x)",
        measured as f64 / BYTES as f64
    );
    assert!(
        measured >= BYTES as u64 && measured < 2 * BYTES as u64,
        "the probe charged {measured} B for a {BYTES}-byte Vec -- it is not measuring the clone, \
         and every figure below is noise"
    );

    let mut one: BTreeMap<u64, BlockAddress> = BTreeMap::new();
    one.insert(1, BlockAddress::from_parts(1, 0, 64, Some(1), Some(1)));
    let (node_bytes, node_allocs) = deep_heap(&one);
    let value_width = std::mem::size_of::<BlockAddress>() as u64;
    println!(
        "control: a one-entry BTreeMap charged {node_bytes} B in {node_allocs} allocations to carry \
         {value_width} B of value ({:.1}x)",
        node_bytes as f64 / value_width as f64
    );
    // The RATIO here moves when the value width moves, and the value width HAS moved: a
    // `BlockAddress` is 16 bytes now, where an earlier reading of this control had 24. What is being
    // asserted is therefore the structural fact -- a one-entry B-tree costs several times what it
    // carries -- and not a remembered number, because a remembered number would have failed for the
    // address narrowing rather than for anything about trees.
    assert!(
        node_bytes > 4 * value_width,
        "a one-entry B-tree charged {node_bytes} B for {value_width} B of value -- the per-container \
         overhead a two-level shape exists to remove is no longer visible to this probe, so the \
         comparison below cannot mean what it says"
    );
}

// =================================================================================================
// THE COMPARISON
// =================================================================================================

/// What the two shapes cost over the same rows, at three occupancies.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds both shapes over three populations under the counting allocator; run by name"]
fn what_a_two_level_resident_map_costs_against_one_container_per_key() {
    println!(
        "BlockAddress is {} B; a two-level row is {} B",
        std::mem::size_of::<BlockAddress>(),
        std::mem::size_of::<(Arc<str>, u64, BlockAddress)>()
    );
    let mut summary: Vec<(&'static str, f64, f64, f64)> = Vec::new();
    let mut vector_arm: Vec<f64> = Vec::new();
    let mut grouped_arm: Vec<f64> = Vec::new();

    for (label, keys, points_per_key) in [
        ("1 point a key", 40_000usize, 1usize),
        ("2 points a key", 20_000, 2),
        ("4 points a key", 10_000, 4),
        ("10 points a key", 4_000, 10),
        ("100 points a key", 400, 100),
        ("1000 points a key", 40, 1_000),
    ] {
        let data = rows(keys, points_per_key);
        // DENOMINATORS, before anything is divided by them.
        assert_eq!(
            keys * points_per_key,
            data.len(),
            "denominator: every row must be generated"
        );
        let a = arm_a(&data);
        let b = arm_b(&data);
        assert_eq!(keys, a.len(), "denominator: arm A must hold every key");
        assert!(!b.is_empty(), "denominator: arm B must hold some bucket");

        // THE ARMS ARE CHECKSUMMED AGAINST EACH OTHER. A fixture that collapsed one arm's
        // population reads exactly like a brilliant arm.
        let a_rows: usize = a.values().map(|series| series.len()).sum();
        let b_rows: usize = b.values().map(|run| run.len()).sum();
        assert_eq!(
            data.len(), a_rows,
            "arm A lost rows: {} generated, {a_rows} held",
            data.len()
        );
        assert_eq!(
            data.len(), b_rows,
            "arm B lost rows: {} generated, {b_rows} held",
            data.len()
        );
        let a_sum: u64 = a
            .values()
            .flat_map(|series| series.iter())
            .map(|(at, address)| at ^ address.length())
            .fold(0u64, |acc, v| acc.wrapping_add(v));
        let b_sum: u64 = b
            .values()
            .flat_map(|run| run.iter())
            .map(|(_key, at, address)| at ^ address.length())
            .fold(0u64, |acc, v| acc.wrapping_add(v));
        assert_eq!(
            a_sum, b_sum,
            "the two arms do not hold the same rows, so nothing below compares two shapes"
        );
        let a_keys: HashSet<&str> = a.keys().map(|k| k.as_str()).collect();
        let b_keys: HashSet<&str> = b
            .values()
            .flat_map(|run| run.iter())
            .map(|(key, _, _)| key.as_ref())
            .collect();
        assert_eq!(
            a_keys, b_keys,
            "the two arms do not hold the same keys"
        );
        assert_eq!(
            keys,
            a_keys.len(),
            "denominator: the key set must be the one that was generated, not a collapsed one"
        );

        let ascending = arm_a_ascending(&data);
        let (ascending_bytes, _) = deep_heap(&ascending);
        drop(ascending);
        let (a_bytes, a_allocs) = deep_heap(&a);
        let c = arm_c(&data);
        assert_eq!(keys, c.len(), "denominator: arm C must hold every key");
        let c_rows: usize = c.values().map(|points| points.len()).sum();
        assert_eq!(data.len(), c_rows, "arm C lost rows");
        let c_sum: u64 = c
            .values()
            .flat_map(|points| points.iter())
            .map(|(at, address)| at ^ address.length())
            .fold(0u64, |acc, v| acc.wrapping_add(v));
        assert_eq!(a_sum, c_sum, "arm C does not hold the same rows as arm A");
        let (c_bytes, c_allocs) = deep_heap(&c);
        drop(c);
        let d = arm_d(&data);
        let d_rows: usize = d.values().map(|bucket| bucket.rows.len()).sum();
        let d_objects: usize = d.values().map(|bucket| bucket.objects.len()).sum();
        assert_eq!(data.len(), d_rows, "arm D lost rows");
        assert_eq!(keys, d_objects, "arm D must name every object exactly once");
        let d_sum: u64 = d
            .values()
            .flat_map(|bucket| bucket.rows.iter())
            .map(|(at, address)| at ^ address.length())
            .fold(0u64, |acc, v| acc.wrapping_add(v));
        assert_eq!(a_sum, d_sum, "arm D does not hold the same rows as arm A");
        // AND IT MUST ANSWER A READ, or the bytes below price something unusable. Checked on the
        // first and last generated row, and on a key that is absent.
        let first = &data[0];
        let last = &data[data.len() - 1];
        assert_eq!(
            Some(first.address.clone()),
            arm_d_lookup(&d, &first.key, first.at),
            "arm D could not read back the first row"
        );
        assert_eq!(
            Some(last.address.clone()),
            arm_d_lookup(&d, &last.key, last.at),
            "arm D could not read back the last row"
        );
        assert_eq!(
            None,
            arm_d_lookup(&d, "a key that was never written", first.at),
            "arm D answered for a key it does not hold"
        );
        let (d_table_bytes, d_allocs) = deep_heap(&d);
        let mut d_seen: HashSet<usize> = HashSet::new();
        let d_text: u64 = d
            .values()
            .flat_map(|bucket| bucket.objects.iter())
            .map(|(key, _, _)| shared_text_bytes(&mut d_seen, key))
            .sum();
        assert_eq!(keys, d_seen.len(), "arm D must hold one text allocation per key");
        let d_bytes = d_table_bytes + d_text;
        drop(d);
        let (b_table_bytes, b_allocs) = deep_heap(&b);
        // Arm B's shared key text is invisible to a clone, so add each distinct allocation back once.
        let mut seen: HashSet<usize> = HashSet::new();
        let b_text: u64 = b
            .values()
            .flat_map(|run| run.iter())
            .map(|(key, _, _)| shared_text_bytes(&mut seen, key))
            .sum();
        assert_eq!(
            keys,
            seen.len(),
            "arm B must hold exactly one text allocation per key, not one per row"
        );
        let b_bytes = b_table_bytes + b_text;

        let per_row_a = a_bytes as f64 / data.len() as f64;
        let per_row_b = b_bytes as f64 / data.len() as f64;
        let per_key_a = a_bytes as f64 / keys as f64;
        let per_key_b = b_bytes as f64 / keys as f64;
        println!(
            "  {label:<24} {keys:>6} keys x {points_per_key:>5} = {:>7} rows in {:>5} buckets\n\
             {:<26} one container a key: {a_bytes:>10} B  {per_row_a:>7.1} B/row  {per_key_a:>8.1} \
             B/key  {a_allocs:>7} allocations\n\
             {:<26} two levels:          {b_bytes:>10} B  {per_row_b:>7.1} B/row  {per_key_b:>8.1} \
             B/key  {b_allocs:>7} allocations  (of which {b_text} B is shared key text)\n\
             {:<26} two levels / one container a key: {:.3}x bytes, {:.3}x allocations",
            data.len(),
            b.len(),
            "",
            "",
            "",
            b_bytes as f64 / a_bytes.max(1) as f64,
            b_allocs as f64 / a_allocs.max(1) as f64,
        );
        println!(
            "                           a sorted vector a key:  {c_bytes:>10} B  {:>7.1} B/row  {:>8.1} B/key  {c_allocs:>7} allocations  ({:.3}x the B-tree a key)",
            c_bytes as f64 / data.len() as f64,
            c_bytes as f64 / keys as f64,
            c_bytes as f64 / a_bytes.max(1) as f64
        );
        println!(
            "                           identity once an object: {d_bytes:>10} B  {:>7.1} B/row  {:>8.1} B/key  {d_allocs:>7} allocations  ({:.3}x the B-tree a key, {:.3}x flat two levels)",
            d_bytes as f64 / data.len() as f64,
            d_bytes as f64 / keys as f64,
            d_bytes as f64 / a_bytes.max(1) as f64,
            d_bytes as f64 / b_bytes.max(1) as f64
        );
        println!(
            "                           for comparison, one container a key BEFORE the decode              repack: {ascending_bytes:>10} B  {:>7.1} B/row  ({:.3}x the packed arm)",
            ascending_bytes as f64 / data.len() as f64,
            ascending_bytes as f64 / a_bytes.max(1) as f64
        );
        vector_arm.push(c_bytes as f64 / data.len() as f64);
        grouped_arm.push(d_bytes as f64 / data.len() as f64);
        summary.push((
            label,
            per_row_a,
            per_row_b,
            b_bytes as f64 / a_bytes.max(1) as f64,
        ));
    }

    assert_eq!(6, summary.len(), "six occupancies must be measured");
    assert_eq!(
        summary.len(),
        vector_arm.len(),
        "every occupancy must carry all three arms"
    );
    assert_eq!(summary.len(), grouped_arm.len(), "every occupancy must carry arm D too");
    println!("\nsummary, bytes per row -- and WHICH SHAPE WINS at each occupancy:");
    for ((label, a, b, _ratio), c) in summary.iter().zip(vector_arm.iter()) {
        let best = if c <= b && c <= a {
            "a sorted vector a key"
        } else if b < c && b <= a {
            "two levels"
        } else {
            "a B-tree a key (what ships)"
        };
        println!(
            "  {label:<18} B-tree a key {a:>7.1}   vector a key {c:>7.1}   two levels {b:>7.1}   \
             -> {best}"
        );
    }
    // THE SHAPE OF THE ANSWER, asserted so it cannot quietly stop being true. The two-level arm is
    // supposed to WIN at the sparsest occupancy and LOSE at the densest; if both ends went the same
    // way there would be no crossover to locate and this fixture would be measuring something else.
    let (_, _, sparse_two_level, sparse_ratio) = summary[0];
    let (_, dense_btree, dense_two_level, _) = summary[summary.len() - 1];
    assert!(
        sparse_ratio < 1.0,
        "two levels did not win at the sparsest occupancy ({sparse_ratio:.3}x), so the lever this \
         fixture exists to price is not there"
    );
    assert!(
        dense_two_level > dense_btree,
        "two levels did not lose at the densest occupancy ({dense_two_level:.1} against \
         {dense_btree:.1} B/row) -- there is then no crossover, and the recommendation this \
         fixture carries is wrong"
    );
    assert!(
        sparse_two_level < vector_arm[0],
        "at one point a key the two-level arm must beat a vector a key, or the SECOND level is \
         buying nothing anywhere and only the first is worth taking"
    );
    // ARM D'S CLAIM, which is the one that matters: storing identity once per OBJECT rather than
    // once per row must beat what ships at EVERY occupancy -- including the densest, where flat two
    // levels loses. If that holds, the per-object start-and-length is not an offset table that hands
    // the bytes back; it is eight bytes against a whole run.
    println!("\narm D -- identity once an object -- bytes per row, against what ships:");
    for ((label, a, _b, _r), d) in summary.iter().zip(grouped_arm.iter()) {
        println!(
            "  {label:<18} B-tree a key {a:>7.1}   identity once an object {d:>7.1}   {:.3}x",
            d / a
        );
        assert!(
            d < a,
            "at {label} storing identity once an object cost {d:.1} B/row against {a:.1} for what \
             ships -- the per-object start-and-length HAS handed the bytes back, and that is the \
             result rather than the shape"
        );
    }
    assert!(
        *vector_arm.last().expect("six rows") < dense_btree,
        "a sorted vector a key must beat a B-tree a key at the densest occupancy too, or the \
         unconditional recommendation in this fixture's docs does not hold"
    );
}


// =================================================================================================
// WHAT THE SHAPE COSTS TO WRITE, which the footprint arms above do not price
// =================================================================================================

/// How many rows a single insert has to move, as exact arithmetic rather than as a timing.
///
/// WHY THIS EXISTS. The footprint arms above price what each shape COSTS TO HOLD. They say nothing
/// about what it costs to change, and the two-level arms change that cost fundamentally: a key's rows
/// live inside a run shared with every other object in the bucket, so inserting one row shifts every
/// row that sits after it. A B-tree a key shifts within one node and splits at most a path to the
/// root.
///
/// This matters because it scales with the SAME occupancy as the footprint win, in the OPPOSITE
/// direction. The win comes from many objects sharing one allocation; the write cost comes from many
/// rows sharing one allocation. A fixture that measured only the first would recommend a shape whose
/// write path it had never looked at.
///
/// Counted as MOVES rather than timed, because a count is exact, is the same in debug and release,
/// and is not a reading off a shared box.
#[cfg(feature = "alloc-probe")]
fn moves_to_build_grouped(rows_in: &[Row]) -> u64 {
    // One bucket's state: objects sorted by key, and a contiguous run of rows grouped in that order.
    struct Bucket {
        objects: Vec<(Arc<str>, u32, u32)>,
        rows: Vec<(u64, BlockAddress)>,
    }
    let mut buckets: HashMap<u32, Bucket> = HashMap::new();
    let mut moves = 0u64;
    for row in rows_in {
        let bucket = buckets.entry(bucket_of(&row.key)).or_insert_with(|| Bucket {
            objects: Vec::new(),
            rows: Vec::new(),
        });
        match bucket
            .objects
            .binary_search_by(|(name, _, _)| name.as_ref().cmp(row.key.as_ref()))
        {
            Ok(at) => {
                // The object is already here. Its run grows by one, so every row after its run
                // moves, and every later object's start moves.
                let (_, start, len) = bucket.objects[at];
                let run = &bucket.rows[start as usize..(start + len) as usize];
                let inner = run
                    .binary_search_by(|(existing, _)| existing.cmp(&row.at))
                    .unwrap_or_else(|insert_at| insert_at);
                let index = start as usize + inner;
                moves += (bucket.rows.len() - index) as u64;
                bucket.rows.insert(index, (row.at, row.address.clone()));
                bucket.objects[at].2 += 1;
                for later in bucket.objects[at + 1..].iter_mut() {
                    later.1 += 1;
                }
            }
            Err(at) => {
                // A new object. Its run starts where the next object's run used to.
                let index = if at < bucket.objects.len() {
                    bucket.objects[at].1 as usize
                } else {
                    bucket.rows.len()
                };
                moves += (bucket.rows.len() - index) as u64;
                bucket.rows.insert(index, (row.at, row.address.clone()));
                // The objects array shifts too, and that is a move as much as a row is.
                moves += (bucket.objects.len() - at) as u64;
                bucket.objects.insert(at, (Arc::clone(&row.key), index as u32, 1));
                for later in bucket.objects[at + 1..].iter_mut() {
                    later.1 += 1;
                }
            }
        }
    }
    moves
}

/// What a B-tree a key moves for the same inserts, as the same kind of count.
///
/// A `BTreeMap` insert shifts entries inside ONE leaf, and splits at most the path to the root. The
/// leaf holds eleven, so the shift is bounded by eleven however large the map is -- which is the
/// property the grouped run gives up. Counted the same way so the two numbers are comparable: the
/// entries moved inside the node the insert lands in.
#[cfg(feature = "alloc-probe")]
fn moves_to_build_btree_a_key(rows_in: &[Row]) -> u64 {
    // The B-tree's own node shifts are not observable from outside, so this counts the BOUND rather
    // than the actual: at most one leaf's worth per insert. That is an over-estimate for the B-tree
    // and so is the conservative direction for the comparison being made.
    (rows_in.len() as u64) * (ENTRIES_IN_ONE_LEAF_FOR_THE_COUNT as u64)
}

/// `std`'s B-tree leaf capacity, named here only to bound the count above.
#[cfg(feature = "alloc-probe")]
const ENTRIES_IN_ONE_LEAF_FOR_THE_COUNT: usize = 11;

/// What it costs to WRITE each shape, at the same six occupancies the footprint used.
///
/// The ascending-timestamp order is the one the product issues: a counter series and a feature series
/// both append at the end of their own key's run. That is the BEST case for the grouped shape, and it
/// is still the whole tail of the bucket that moves.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds six populations twice, counting row moves; run by name"]
fn what_an_insert_costs_in_each_shape_at_six_occupancies() {
    let mut rows_out: Vec<(&'static str, usize, u64, u64, f64)> = Vec::new();
    for (label, keys, points_per_key) in [
        ("1 point a key", 40_000usize, 1usize),
        ("2 points a key", 20_000, 2),
        ("4 points a key", 10_000, 4),
        ("10 points a key", 4_000, 10),
        ("100 points a key", 400, 100),
        ("1000 points a key", 40, 1_000),
    ] {
        // INTERLEAVED, not key-at-a-time: a store receives one write per key per tick, which is what
        // makes a key's run grow while other objects already sit after it. Building key-at-a-time
        // would append at the very end every time and price the easy case.
        let base = rows(keys, points_per_key);
        let mut interleaved: Vec<Row> = Vec::with_capacity(base.len());
        for point in 0..points_per_key {
            for key_index in 0..keys {
                interleaved.push(base[key_index * points_per_key + point].clone());
            }
        }
        assert_eq!(base.len(), interleaved.len(), "denominator: no row lost in the interleave");

        let grouped = moves_to_build_grouped(&interleaved);
        let btree = moves_to_build_btree_a_key(&interleaved);
        let per_insert = grouped as f64 / interleaved.len() as f64;
        println!(
            "  {label:<18} {:>7} inserts: grouped run moved {grouped:>12} rows ({per_insert:>9.1} \
             an insert); a B-tree a key moves at most {btree:>10} ({:>5.1} an insert)",
            interleaved.len(),
            btree as f64 / interleaved.len() as f64
        );
        // NOT asserted non-zero here. Zero is the TRUE answer at the densest occupancy -- forty keys
        // over forty buckets, every ascending append landing at the very end of its bucket -- and a
        // per-occupancy non-vacuity check forbade the real result. The counter is proved to work
        // below instead, on the occupancies where it must be non-zero.
        rows_out.push((label, interleaved.len(), grouped, btree, per_insert));
    }
    assert_eq!(6, rows_out.len(), "six occupancies must be measured");
    println!("\nmoves an insert -- the write cost the footprint arms do not price:");
    for (label, _n, _g, _b, per_insert) in &rows_out {
        println!("  {label:<18} {per_insert:>10.1} rows moved an insert");
    }
    // WHAT THIS FIXTURE WAS WRITTEN TO FEAR, AND WHAT IT FOUND INSTEAD. It was written expecting the
    // move count to RISE with occupancy -- a key's rows sharing a run with every other object in the
    // bucket, so an insert shifts the whole tail. That assertion was written first and it FAILED,
    // which is the only reason this comment is accurate.
    //
    // It falls instead, and the reason is worth more than the number. Timestamps within one key
    // arrive ASCENDING, so a row lands at the end of ITS OWN object's run; the only rows that move
    // are those belonging to objects that sort AFTER it in the same bucket. So the cost is set by
    // OBJECTS PER BUCKET, not by rows per bucket -- and objects per bucket is what FALLS as each key
    // gets denser, because the corpus is held constant. At a thousand points a key there are forty
    // keys over forty buckets, every append is at the very end of its bucket, and the count is zero.
    //
    // THE PROPERTY WORTH GUARDING is therefore the one that was actually established: the per-insert
    // move count is bounded by a small constant and does NOT grow with the corpus. Forty thousand
    // rows per occupancy and never more than ~13 moves an insert is what makes the shape writable at
    // all; a count that scaled with the bucket's rows would have made it unusable whatever it saved.
    // NON-VACUITY, where it belongs: the counter must have counted SOMETHING, or every zero below is
    // a broken instrument rather than a result. The sparsest occupancy has 40,000 keys over 1,024
    // buckets, so objects certainly share buckets there and rows certainly move.
    let total: u64 = rows_out.iter().map(|(_, _, grouped, _, _)| *grouped).sum();
    assert!(
        total > 0,
        "no occupancy moved a single row, so the move counter is not counting and every figure here \
         is a zero that means nothing"
    );
    assert!(
        rows_out[0].2 > 0,
        "the sparsest occupancy moved no rows, and with 40,000 keys over 1,024 buckets it must -- \
         the counter is blind"
    );
    let sparse = rows_out[0].4;
    let dense = rows_out[rows_out.len() - 1].4;
    println!(
        "  the per-insert count is {sparse:.1} at the sparsest occupancy and {dense:.1} at the \
         densest, over {} rows either way",
        rows_out[0].1
    );
    for (label, inserts, _grouped, _btree, per_insert) in &rows_out {
        assert!(
            *per_insert < 64.0,
            "at {label} the grouped run moved {per_insert:.1} rows an insert over {inserts} \
             inserts -- that is no longer a small constant, so the write cost has started to scale \
             with the bucket and the shape is not writable"
        );
    }
    assert!(
        dense <= sparse,
        "the per-insert count rose from {sparse:.1} at the sparsest occupancy to {dense:.1} at the \
         densest -- the recorded reason for the fall (ascending timestamps land at the end of their \
         own object's run, so only later OBJECTS move) has stopped holding"
    );
}


// =================================================================================================
// THE WRITE PATTERN #2080 SCOPED ITSELF OUT OF: inner keys that do NOT arrive ascending
// =================================================================================================

/// Which end of its own run a write lands on.
#[cfg(feature = "alloc-probe")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Landing {
    /// Each new inner key is ABOVE every key that object already holds, so the row lands at the end
    /// of its own run. A timestamped series and a right push both do this.
    AtTheEnd,
    /// Each new inner key is BELOW every key that object already holds, so the row lands at the start
    /// of its own run. This is a left push, and it is the case #2080 did not measure.
    AtTheStart,
    /// Alternating, because a real list does both.
    AtBothEnds,
}

#[cfg(feature = "alloc-probe")]
impl Landing {
    fn label(self) -> &'static str {
        match self {
            Landing::AtTheEnd => "at the end (a timestamp, a right push)",
            Landing::AtTheStart => "at the start (a left push)",
            Landing::AtBothEnds => "at both ends (a real list)",
        }
    }
}

/// `keys` objects receiving `points_per_key` writes each, with the inner key chosen so every write
/// lands where `landing` says.
///
/// Interleaved one write per key per tick, for the reason #2080 states: building key-at-a-time would
/// append past the end of the bucket every time and price the easy case.
#[cfg(feature = "alloc-probe")]
fn push_workload(keys: usize, points_per_key: usize, landing: Landing) -> Vec<Row> {
    // Mid-range base so a descending run never underflows and an ascending one never collides.
    const BASE: u64 = 1 << 40;
    let mut out = Vec::with_capacity(keys * points_per_key);
    let built: Vec<Arc<str>> = (0..keys).map(|k| Arc::from(format!("l{k}"))).collect();
    for point in 0..points_per_key {
        for (index, key) in built.iter().enumerate() {
            let at = match landing {
                Landing::AtTheEnd => BASE + point as u64,
                Landing::AtTheStart => BASE - point as u64,
                Landing::AtBothEnds => {
                    if point % 2 == 0 {
                        BASE + (point as u64) / 2
                    } else {
                        BASE - (point as u64 + 1) / 2
                    }
                }
            };
            out.push(Row {
                key: Arc::clone(key),
                at,
                address: BlockAddress::from_parts(
                    (index * points_per_key + point) as u64,
                    0,
                    64,
                    Some(1),
                    Some((index * points_per_key + point) as u64),
                ),
            });
        }
    }
    out
}

/// What each landing costs the grouped run, at the same six occupancies.
///
/// WHY THIS DECIDES WHETHER `lists` CAN TAKE THE SHAPE. A list's own declaration says left pushes walk
/// the low end down and right pushes walk the high end up, so both ends are O(log n) in a B-tree and
/// the tree's order IS the list's order. In a grouped run a left push lands at the START of its
/// object's run, so it moves every row that object owns and every row of every object after it -- and
/// unlike the ascending case that gets WORSE as the key gets longer. #2080 measured only the ascending
/// landing and said so; this is the measurement it deferred.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "builds three workloads at six occupancies, counting row moves; run by name"]
fn what_a_push_workload_costs_in_a_grouped_run_at_six_occupancies() {
    let mut worst_at_the_start = 0.0f64;
    let mut rows_at_the_start: Vec<(usize, f64)> = Vec::new();
    for (label, keys, points_per_key) in [
        ("1 point a key", 40_000usize, 1usize),
        ("2 points a key", 20_000, 2),
        ("4 points a key", 10_000, 4),
        ("10 points a key", 4_000, 10),
        ("100 points a key", 400, 100),
        ("1000 points a key", 40, 1_000),
    ] {
        println!("  {label}:");
        for landing in [Landing::AtTheEnd, Landing::AtTheStart, Landing::AtBothEnds] {
            let data = push_workload(keys, points_per_key, landing);
            assert_eq!(
                keys * points_per_key,
                data.len(),
                "denominator: every write must be generated"
            );
            let moves = moves_to_build_grouped(&data);
            let per_insert = moves as f64 / data.len() as f64;
            println!(
                "      {:<40} {:>12} moves over {:>6} writes = {:>9.1} an insert",
                landing.label(),
                moves,
                data.len(),
                per_insert
            );
            if landing == Landing::AtTheStart {
                worst_at_the_start = worst_at_the_start.max(per_insert);
                rows_at_the_start.push((points_per_key, per_insert));
            }
        }
    }

    // NON-VACUITY on the scan, not per row: at least one reading must have moved something, or the
    // counter is blind and every figure above is a zero that means nothing.
    assert_eq!(6, rows_at_the_start.len(), "six occupancies must be measured");
    assert!(
        worst_at_the_start > 0.0,
        "no left-push reading moved a single row, so the counter is not counting"
    );

    println!("\n  the left-push landing, against how long the key is:");
    for (points, per_insert) in &rows_at_the_start {
        println!(
            "      {points:>5} points a key: {per_insert:>9.1} moves an insert ({:.1}x a B-tree's \
             eleven-entry bound)",
            per_insert / 11.0
        );
    }

    // THE QUESTION THIS FIXTURE EXISTS TO ANSWER, as an assertion so the answer cannot drift
    // unnoticed. A left push lands at the start of its object's run, so the cost should GROW with how
    // many rows that object already holds -- which is the opposite of the ascending landing, where it
    // falls. If this ever stops holding, the recorded reason for keeping `lists` on its own shape has
    // stopped holding too.
    let sparse = rows_at_the_start[0].1;
    let dense = rows_at_the_start[rows_at_the_start.len() - 1].1;
    assert!(
        dense > sparse * 4.0,
        "a left push moved {dense:.1} rows an insert at a thousand points a key against \
         {sparse:.1} at one -- the cost is supposed to grow with the key's own length, and if it no \
         longer does then a grouped run can serve a list after all and that is the finding"
    );
}
