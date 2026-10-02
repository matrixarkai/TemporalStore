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
