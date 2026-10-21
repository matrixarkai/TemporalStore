// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE ONE-RECORD-PER-OBJECT MACHINERY IS BUILT AND NOT WIRED, and this says so in a form that
//! cannot go stale.
//!
//! # THE QUESTION THIS ANSWERS
//!
//! `ObjectRecord` exists to collapse the sibling rows an object is named by -- its own doc says so:
//! the model map, the page lookup, the dirty index and the page index. It carries a `deadline` word
//! with two tag bits, a `DIRTY` flag, and a page-layout handle for an address that does not fit
//! inline. So the obvious question is why `expires_at_ms` and `dirty_objects` are still separate
//! structures.
//!
//! The answer is not a design disagreement. **Nothing in production ever fills any of it.** The two
//! constructors that set a deadline or a dirty bit have no production caller; the page-layout side
//! table is touched only from `#[cfg(test)]` code; and the one production path that builds a slot
//! uses `ObjectSlot::default()`, which has no deadline and no dirty bit. The representation changed
//! and **the leg that fills it never moved**.
//!
//! So the duplication the record was built to remove is still being paid, and the record is
//! carrying eight bytes of deadline plus two tag bits that are always zero.
//!
//! # WHY THIS IS A TEST AND NOT A COMMENT
//!
//! A comment saying "this is not wired yet" rots silently in both directions: someone wires it and
//! the comment still says otherwise, or someone deletes the machinery as dead and the comment was
//! the only record of why it existed. This arm fails EITHER WAY -- it names the sites it expects to
//! find and the ones it expects not to -- so the next person is told which world they are in.
//!
//! # WHAT IS NOT A DUPLICATE, STATED SO IT IS NOT "FIXED" BY MISTAKE
//!
//! `dirty_objects` is **not** redundant and must not be folded into the record's bit.
//! `object_record.rs`'s own doc gives the reason: it is grouped BY BUCKET, and answers "which
//! objects in this bucket are dirty", which a per-object bit cannot answer without visiting every
//! object. It is also `#[serde(skip)]`, so it costs no stored bytes at all. The redundant half of
//! that pair is the record's `DIRTY` flag, and since nothing sets it there is no live duplication
//! today -- only an unused bit.
//!
//! `expiry_by_deadline` is **not** redundant either, for the same shape of reason: it is ordered BY
//! DEADLINE and answers "which object expires next". What the record would absorb is
//! `expires_at_ms`, the per-key lookup -- "when does THIS key expire" -- and only that.

/// Production source: every `.rs` under the crate's `src/`, excluding any path with a `tests`
/// component. A `#[cfg(test)]` region inside a production FILE is NOT excluded, which matters here:
/// the only call that fills a record's deadline lives in one, and counting it as production would
/// have reversed this module's entire conclusion.
fn production_sources() -> Vec<(String, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "tests") {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    out.push((path.display().to_string(), text));
                }
            }
        }
    }
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut out = Vec::new();
    walk(&src, &mut out);
    assert!(
        out.len() > 50,
        "the production walk found only {} files, so the scope is broken rather than the claim",
        out.len()
    );
    out
}

/// Lines mentioning `needle`, outside any `#[cfg(test)]` region, as `path:line`.
///
/// REGIONS ARE TRACKED BY BRACE DEPTH from the attribute, which is the fragile part and is why the
/// arm below carries a positive control: a site this is KNOWN to find must still be found.
fn production_mentions(needle: &str) -> Vec<String> {
    let mut hits = Vec::new();
    for (path, text) in production_sources() {
        let mut depth: i32 = 0;
        let mut in_test = false;
        let mut test_depth: i32 = 0;
        for (number, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if !in_test && trimmed.starts_with("#[cfg(test)]") {
                in_test = true;
                test_depth = depth;
                continue;
            }
            let opens = line.matches('{').count() as i32;
            let closes = line.matches('}').count() as i32;
            if !in_test && line.contains(needle) {
                hits.push(format!("{path}:{}", number + 1));
            }
            depth += opens - closes;
            if in_test && depth <= test_depth && closes > 0 {
                in_test = false;
            }
        }
    }
    hits
}

#[test]
fn the_records_deadline_and_dirty_bit_have_no_production_writer() {
    // POSITIVE CONTROL FIRST. If the scanner cannot find something that is definitely there, a
    // zero below means nothing. `ObjectSlot::default()` is the production slot construction, and
    // it is the very call that leaves the deadline unset.
    let control = production_mentions("ObjectSlot::default()");
    assert!(
        !control.is_empty(),
        "the scanner found no `ObjectSlot::default()` in production, so it is not reading \
         production source and every zero it reports is meaningless"
    );

    // The two constructors that fill a deadline and a dirty bit.
    for constructor in ["from_object_rows(", "from_object_rows_with_layout("] {
        let callers: Vec<String> = production_mentions(constructor)
            .into_iter()
            // the declaration and the doc links inside the module itself are not callers
            .filter(|site| !site.contains("object_record.rs"))
            .collect();
        assert!(
            callers.is_empty(),
            "`{constructor}` now HAS production callers: {callers:?}. That is the leg this module \
             says never moved -- if it has moved, the record is carrying real deadlines now and \
             `expires_at_ms` is a live duplicate that should be deleted rather than merely noted. \
             Update this module to describe the new world."
        );
    }

    // The page-layout side table, which the `with_layout` arm needs.
    let layout_sites: Vec<String> = production_mentions("object_page_layouts")
        .into_iter()
        .filter(|site| !site.contains("state.rs"))
        .collect();
    assert!(
        layout_sites.is_empty(),
        "`object_page_layouts` is now reached from production outside its own declaration: \
         {layout_sites:?}. Same conclusion as above -- the machinery is being wired, so this \
         module's claim is out of date."
    );
}

#[test]
fn the_expiry_duplicate_is_still_being_paid() {
    // Both halves still exist as separate structures, which is the cost this module is about. The
    // per-key half is what a wired record would absorb; the ordered half survives by design.
    let per_key = production_mentions("expires_at_ms_mut()");
    assert!(
        per_key.len() >= 2,
        "the per-key expiry table has fewer than two production write sites ({per_key:?}). If it \
         has been absorbed into the record, delete this module; if it has merely moved, follow it."
    );

    let ordered = production_mentions("expiry_by_deadline");
    assert!(
        !ordered.is_empty(),
        "the deadline-ORDERED mirror has no production site left. It is NOT the duplicate -- it \
         answers \"which object expires next\", which a per-object deadline cannot -- so losing \
         it is a correctness regression wearing a saving's clothes."
    );
}

#[test]
fn the_bucket_grouped_dirty_index_is_not_the_duplicate() {
    // Stated as a test so that a later sweep for "redundant" structures cannot quietly remove it.
    // It is grouped BY BUCKET and `serde(skip)`: a per-object bit cannot answer its question, and
    // it costs no stored bytes.
    let grouped = production_mentions("dirty_objects");
    assert!(
        grouped.len() >= 4,
        "the bucket-grouped dirty index has nearly disappeared from production ({} sites). A \
         per-object DIRTY bit does not replace it: the bit answers \"is THIS object dirty\" and \
         this index answers \"which objects in this bucket are dirty\", which is what the flush \
         path asks. Folding the second into the first turns a bucket walk into a shard scan.",
        grouped.len()
    );
}
