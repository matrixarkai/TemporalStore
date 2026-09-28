//! NO FOREIGN TYPE NAMES IN RUST SOURCE.
//!
//! The vocabulary scrub every change here runs covers vendor IDENTITY -- product and company
//! names. It cannot catch what this module catches, and the two failures are different in kind:
//! naming a product merely points at something outside this repository, while a leaked
//! identifier is evidence of it, carried in the source of an Apache-2.0 project that anyone may
//! fork. This crate shipped `"HashOrSet<std::string,std::string>"` as a descriptor value for two
//! context models, and nothing failed, because no gate was looking for a type name.
//!
//! WHAT THIS DOES NOT COVER, deliberately: a DURABLE name. Every `#[serde(alias = ...)]` and
//! `#[serde(rename = ...)]` in this crate is a spelling already written into stores in the
//! world -- `page_in_log`, `page_size`, `routing_slot` -- and a name that entered this codebase
//! from anywhere is OURS once a store carries it, because dropping it is a store that stops
//! loading. The scan below is a scan for DESCRIPTIVE text: a string, a comment or an identifier
//! that names someone else's type where it could have named ours. It is not a renaming campaign
//! over the wire format, and it must never become one.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Namespace-qualified type spellings from another language's standard library. These are
/// unambiguous: a `.rs` file has no legitimate use for any of them, so no judgement call is
/// involved in a hit.
///
/// SCOPED, and this is the whole difficulty of the check. A bare `std::` matcher is useless --
/// `use std::collections::BTreeMap` is on hundreds of lines in this crate -- so each needle names
/// one specific type whose spelling Rust does not share, and the right-boundary rule below
/// rejects the Rust path that merely starts the same way (`std::string::String`,
/// `std::stringify!`). `std::vec`, `std::slice` and `std::array` are ABSENT from this list on
/// purpose: all three are real Rust paths used here, and it is `vector` rather than `vec` that
/// belongs to the other spelling.
const FOREIGN_NAMESPACE_TYPE_NEEDLES: &[&str] = &[
    "std::string",
    "std::vector",
    "std::pair",
    "std::map",
    "std::set",
    "std::list",
    "std::deque",
    "std::unique_ptr",
    "std::shared_ptr",
];

/// A namespace prefix with no Rust meaning at all, so it needs no right boundary -- whatever
/// follows it, the prefix itself does not belong in a `.rs` file.
const FOREIGN_NAMESPACE_PREFIXES: &[&str] = &["absl::"];

/// Fixed-width integer spellings Rust does not use. Rust spells these `u8`/`u16`/`u32`/`u64`/
/// `i64`, so the `_t` form only ever arrives by being copied in. Boundary-checked at both ends,
/// which is also what keeps `uint8_t` from being reported twice -- once as itself, once as the
/// `int8_t` sitting inside it.
const FIXED_WIDTH_INT_NEEDLES: &[&str] =
    &["uint8_t", "uint16_t", "uint32_t", "uint64_t", "int64_t"];

/// DISTINCTIVE TYPE AND METHOD NAMES FROM THE IMPLEMENTATION THIS ENGINE'S DESIGN WORK STUDIED.
///
/// Each row carries the reason it is here, because a list nobody justified becomes furniture that
/// outlives the reason for it. The test on this list asserts every row has a justification of
/// real length, so a future addition cannot be silent.
///
/// The common argument, which does most of the work: this crate's methods are `snake_case`, so a
/// `CamelCase` method name is already not Rust and its presence can only be a copy. The type
/// names each get a specific argument about the word OUR vocabulary uses instead.
const FOREIGN_NAME_NEEDLES: &[(&str, &str)] = &[
    (
        "HashOrSet",
        "Found in this crate and removed by the change that added this guard: it was the \
         `block_primitive` of two context models. Ours are the `model_kind_registry!` spellings \
         -- `hash`, `set`, `string` -- never a template composing two of them.",
    ),
    (
        "FeatureOrSet",
        "Found in this crate and removed by the same change, on six descriptors. `feature` alone \
         IS our spelling and is what those six now carry; the `OrSet` suffix is the borrowed half.",
    ),
    (
        "OrSetModel",
        "The third member of the same `OrSet` family. We say `ModelKind`, and a model's kind is \
         one of seventeen declared names, so there is no shape here for an `OrSet` to describe.",
    ),
    (
        "PersistentMap",
        "A named map template from that implementation. Ours is a `BTreeMap` in `ShardState` made \
         durable by the block index -- we never fold persistence into a type's name. Still \
         present in one `docs/` file, which this Rust-only scan does not reach.",
    ),
    (
        "MultiPageObject",
        "Our word for a stored unit is BLOCK, not page: `BlockAddress`, `block_size`, \
         `block_in_log`, and `page_segment_id` was renamed to `block_slab_id`. A new CamelCase \
         `*Page*` type name is therefore a copy rather than a coinage. Our own layout policies \
         spell this concept `single_page_object` and `component_page_object`, in snake_case, and \
         those are stored strings this scan leaves alone.",
    ),
    (
        "ObjectWithId",
        "We carry identity as a FIELD -- `object_id` on `BlockAddress` -- and name types for what \
         they are (`BlockAddress`, `ContextNodeModel`), never for which fields they happen to \
         hold.",
    ),
    (
        "SlotContextManager",
        "`slot` here is that implementation's word. Ours survives only as the retired wire alias \
         `routing_slot`, whose live spelling is `routing_bucket`; and `context` in this crate \
         names the context MODELS, which have no manager type.",
    ),
    (
        "GatherDirtySlot",
        "CamelCase method, so not Rust. Ours is `context_dirty_index` plus the `dirty` bit that \
         every block address carries.",
    ),
    (
        "MarkSlotDataDirty",
        "CamelCase method, so not Rust, and built on the `slot` vocabulary described above.",
    ),
    (
        "PushDirtySlot",
        "CamelCase method, so not Rust, and built on the `slot` vocabulary described above.",
    ),
    (
        "BlindDumpNewPages",
        "CamelCase method, so not Rust, and built on the `page`-for-`block` vocabulary above.",
    ),
    (
        "WriteKvLog",
        "CamelCase method, so not Rust. Our log-writing vocabulary is `wal`, `index_log` and \
         `roll_wal_segment_if_due`.",
    ),
    (
        "BYTE_ASSERT",
        "That implementation's assertion macro. Ours are `assert!` and `debug_assert!`. This row \
         is genuinely additive to the vendor scrub, which lists company and product names and \
         would not fire on this token at all.",
    ),
    (
        "bcache2",
        "A specific named cache component over there. This crate's caches are named for what \
         they hold, and not one of them is numbered.",
    ),
    (
        "StlWrapper",
        "A wrapper over the other language's template library. Nothing in a Rust crate has such \
         a thing to wrap, so the name cannot arrive here by being needed.",
    ),
    (
        "GetMinTtl",
        "CamelCase method, so not Rust. Our expiry vocabulary is `expiry_by_deadline` and \
         `ensure_expiry_order`.",
    ),
    (
        "ComputeRawObjectSize",
        "CamelCase method, so not Rust. Our sizes are the `stored_size` and `block_size` fields.",
    ),
];

/// THE ONLY EXEMPT PATH, and it has to exist: this file lists every needle as source text, so a
/// scan that read it would report itself. Asserted to be exactly one entry, because the way a
/// check like this dies is by growing one exemption per inconvenient file until it scans nothing.
const EXEMPT_RELATIVE_PATHS: &[&str] = &["foreign_type_name_guard.rs"];

/// Measured on the tree that introduced this guard: 321 files and 17_181_182 bytes under
/// `crates/temporalstore-rust/src`. The floors sit below those with room for ordinary churn --
/// they are here to catch a walk that found nothing or almost nothing, which is the failure a
/// clean report cannot otherwise be told apart from.
const MIN_FILES_SCANNED: usize = 280;
const MIN_BYTES_SCANNED: usize = 12_000_000;

#[derive(Debug)]
struct Finding {
    path: String,
    line: usize,
    needle: String,
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// True when the byte before `start` cannot be part of an identifier, so the needle is not the
/// tail of some longer word.
fn left_boundary_ok(haystack: &[u8], start: usize) -> bool {
    start == 0 || !is_ident_byte(haystack[start - 1])
}

/// True when the byte after the needle cannot continue an identifier AND is not a second `:`.
///
/// THE SECOND HALF IS THE WHOLE POINT for the `std::` needles: `std::string::String` and
/// `std::stringify!` are ordinary Rust, and only this rule tells them apart from a bare
/// `std::string` used as a type. Rejecting on `:` keeps the Rust path silent; rejecting on an
/// identifier byte keeps `stringify` silent.
fn right_boundary_ok(haystack: &[u8], end: usize) -> bool {
    match haystack.get(end) {
        None => true,
        Some(&b) => !is_ident_byte(b) && b != b':',
    }
}

enum Boundary {
    /// Both ends must be clear, and a trailing `:` disqualifies.
    BothEnds,
    /// Only the left end is checked; the needle is a prefix expected to be followed by a name.
    LeftOnly,
}

fn find_needle(text: &str, needle: &str, boundary: &Boundary) -> Vec<usize> {
    let haystack = text.as_bytes();
    let mut hits = Vec::new();
    let mut from = 0usize;
    while let Some(offset) = text[from..].find(needle) {
        let start = from + offset;
        let end = start + needle.len();
        let ok = match boundary {
            Boundary::BothEnds => {
                left_boundary_ok(haystack, start) && right_boundary_ok(haystack, end)
            }
            Boundary::LeftOnly => left_boundary_ok(haystack, start),
        };
        if ok {
            hits.push(start);
        }
        // Every needle starts with an ASCII byte, so `start + 1` is always a char boundary.
        from = start + 1;
    }
    hits
}

fn line_of(text: &str, byte_offset: usize) -> usize {
    text[..byte_offset].bytes().filter(|b| *b == b'\n').count() + 1
}

/// Scan one file's text. Pure, so the controls below can drive it without touching the disk.
fn scan_text(path: &str, text: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut push = |needle: &str, boundary: Boundary| {
        for offset in find_needle(text, needle, &boundary) {
            findings.push(Finding {
                path: path.to_string(),
                line: line_of(text, offset),
                needle: needle.to_string(),
            });
        }
    };
    for needle in FOREIGN_NAMESPACE_TYPE_NEEDLES {
        push(needle, Boundary::BothEnds);
    }
    for needle in FIXED_WIDTH_INT_NEEDLES {
        push(needle, Boundary::BothEnds);
    }
    for (needle, _reason) in FOREIGN_NAME_NEEDLES {
        push(needle, Boundary::BothEnds);
    }
    for needle in FOREIGN_NAMESPACE_PREFIXES {
        push(needle, Boundary::LeftOnly);
    }
    findings.sort_by(|a, b| (a.line, &a.needle).cmp(&(b.line, &b.needle)));
    findings
}

/// Every `.rs` file under the crate's `src`, derived by walking the tree. NOT a list anyone
/// maintains: a hand-written subject list goes stale and nothing fails.
fn walk_crate_rust_files() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("read_dir {} failed: {err}", dir.display()));
        for entry in entries {
            let entry = entry.expect("dir entry readable");
            let path = entry.path();
            let kind = entry.file_type().expect("file type readable");
            if kind.is_dir() {
                stack.push(path);
            } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

fn is_exempt(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    EXEMPT_RELATIVE_PATHS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// POSITIVE CONTROL. A scan that matched nothing and a scan that found nothing produce the
    /// same output -- zero findings -- so every needle is planted and asserted to be recovered
    /// by name. Without this the guard could be silently inert, which is how a scrub in this
    /// repository once read clean three times over a file it had never opened.
    #[test]
    fn foreign_name_scan_finds_every_planted_needle() {
        let mut planted: Vec<String> = Vec::new();
        for needle in FOREIGN_NAMESPACE_TYPE_NEEDLES {
            planted.push((*needle).to_string());
        }
        for needle in FIXED_WIDTH_INT_NEEDLES {
            planted.push((*needle).to_string());
        }
        for (needle, _reason) in FOREIGN_NAME_NEEDLES {
            planted.push((*needle).to_string());
        }
        for needle in FOREIGN_NAMESPACE_PREFIXES {
            planted.push((*needle).to_string());
        }
        assert!(
            planted.len() >= 32,
            "needle set collapsed to {} entries",
            planted.len()
        );

        for needle in &planted {
            // Planted in the two shapes a leak actually takes: a descriptor string literal and a
            // prose comment. Deliberately NOT spelled as a bare token, so a matcher that only
            // worked on whitespace-delimited words would fail here.
            let fixture = format!(
                "// a comment mentioning {needle} in passing\nconst D: &str = \"{needle}<a,b>\";\n"
            );
            let findings = scan_text("fixture.rs", &fixture);
            assert!(
                findings.iter().any(|f| f.needle == *needle),
                "planted {needle:?} was NOT recovered; findings={findings:?}"
            );
        }

        // And the exact defect this change removed, byte for byte as it stood in types.rs.
        let regression = "            \"HashOrSet<std::string,std::string>\",\n";
        let findings = scan_text("types.rs", regression);
        let names: BTreeSet<&str> = findings.iter().map(|f| f.needle.as_str()).collect();
        assert!(
            names.contains("HashOrSet") && names.contains("std::string"),
            "the original defect line must trip both needles; got {names:?}"
        );
    }

    /// NEGATIVE CONTROL. Every line here is legitimate Rust of the kind this crate is full of,
    /// and a matcher that fires on any of them cannot pass -- `std::` is ubiquitous in Rust `use`
    /// statements, and the wire aliases are names we are required to keep.
    #[test]
    fn foreign_name_scan_is_silent_on_legitimate_rust() {
        let legitimate = [
            "use std::collections::BTreeMap;",
            "use std::collections::{BTreeSet, HashMap};",
            "use std::string::String;",
            "use std::vec::Vec;",
            "use std::slice::Iter;",
            "use std::path::{Path, PathBuf};",
            "use std::sync::atomic::{AtomicU64, Ordering};",
            "let converted = value.to_string();",
            "let text = stringify!(ident);",
            "let n: u64 = 0; let m: u8 = 1; let k: i64 = -1; let w: u32 = 2;",
            "/// Everything the harness prints goes to stdout, not the log.",
            "/// The set of members, and the map from key to address.",
            "#[serde(alias = \"page_in_log\")]",
            "#[serde(alias = \"page_size\")]",
            "#[serde(rename = \"routing_slot\")]",
            "pub block_in_log: bool,",
            "    \"single_page_object\"",
            "        \"hash\" | \"set\" => \"component_page_object\",",
            "let mut round_trips = 0; // skips and flips are fine too",
            "Sequence = \"sequence\" @ 5,",
            // What the repaired descriptor table now carries.
            "            \"string\",",
            "            \"feature\",",
        ];
        for line in legitimate {
            let findings = scan_text("legit.rs", line);
            assert!(
                findings.is_empty(),
                "matcher fired on legitimate Rust {line:?}: {findings:?}"
            );
        }
    }

    /// Each needle carries a stated reason, so nobody can add a row without saying why.
    #[test]
    fn every_foreign_name_needle_carries_a_justification() {
        for (needle, reason) in FOREIGN_NAME_NEEDLES {
            assert!(
                reason.len() >= 60,
                "needle {needle:?} has no real justification: {reason:?}"
            );
        }
        let distinct: BTreeSet<&str> = FOREIGN_NAME_NEEDLES.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            distinct.len(),
            FOREIGN_NAME_NEEDLES.len(),
            "duplicate rows in the needle list"
        );
    }

    /// THE EXEMPTION IS EXACTLY ONE PATH. It cannot quietly grow into a place to hide a hit.
    #[test]
    fn the_scan_exemption_is_exactly_one_path() {
        assert_eq!(
            EXEMPT_RELATIVE_PATHS,
            &["foreign_type_name_guard.rs"],
            "the only exempt file is this guard's own source, which must list every needle"
        );
    }

    /// THE SCAN ITSELF, over every `.rs` file in the crate, with the floors that make a clean
    /// result mean something.
    #[test]
    fn no_rust_source_carries_a_foreign_type_name() {
        let files = walk_crate_rust_files();
        assert!(
            files.len() >= MIN_FILES_SCANNED,
            "walk found only {} files, floor is {MIN_FILES_SCANNED} -- that is a broken walk, \
             not a smaller tree",
            files.len()
        );

        // The walk must have recursed: this crate keeps most of its source in subdirectories, so
        // a single-level walk has to be caught directly rather than by the floor above.
        let src_depth = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .components()
            .count();
        let nested = files
            .iter()
            .filter(|path| path.components().count() > src_depth + 1)
            .count();
        assert!(
            nested >= 50,
            "walk did not recurse; only {nested} nested files"
        );

        let mut scanned: BTreeSet<PathBuf> = BTreeSet::new();
        let mut exempted: BTreeSet<PathBuf> = BTreeSet::new();
        let mut bytes = 0usize;
        let mut findings: Vec<Finding> = Vec::new();

        for path in &files {
            if is_exempt(path) {
                exempted.insert(path.clone());
                continue;
            }
            let text = std::fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));
            bytes += text.len();
            scanned.insert(path.clone());
            findings.extend(scan_text(&path.display().to_string(), &text));
        }

        // SCANNED == THE FILE SET THE TREE GAVE US, file for file. Not a count check: the sets
        // are compared, so a file the loop skipped cannot be hidden by another one arriving.
        let walked: BTreeSet<PathBuf> = files.iter().cloned().collect();
        let accounted: BTreeSet<PathBuf> = scanned.union(&exempted).cloned().collect();
        assert_eq!(
            accounted, walked,
            "every walked file must be either scanned or exempt"
        );
        assert_eq!(
            exempted.len(),
            1,
            "expected exactly one exempt file, got {exempted:?}"
        );
        assert!(
            bytes >= MIN_BYTES_SCANNED,
            "scanned only {bytes} bytes, floor is {MIN_BYTES_SCANNED}"
        );

        assert!(
            findings.is_empty(),
            "Rust source carries type names from outside this repository. A descriptive string \
             or comment naming another implementation's type should name OURS instead -- see \
             `model_kind_registry!` for the seventeen kind spellings this engine uses. Do NOT \
             resolve this by touching a `#[serde(alias = ...)]` or `#[serde(rename = ...)]`: \
             those are stored-format names and they stay.\n{findings:#?}"
        );
    }
}
