// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SET OF LOADERS THAT CAN REACH THE INDEX-LOG FOLD, AND THE GATE EVERY PRODUCTION CALLER OF
//! ONE SITS BEHIND.
//!
//! # WHY THIS SET IS LOAD-BEARING
//!
//! `apply_key_state_field(&mut shard.context_nodes, ...)` runs inside the index-log fold, and the
//! fold DOES deliver a `context_nodes` entry -- `the_third_door_stays_shut` drives
//! `load_index_checked` and measures it arriving. What keeps that from being a live route is that
//! no shipped configuration reaches the fold unless an operator turns on
//! `TS_WAL_LEGACY_RECOVERY`: `load_shard_with` selects `load_index_base_only`
//! (`fold_deltas: false`) under `if wal_single_barrier()`, and only its `else` reaches a
//! fold-aware loader.
//!
//! So door 3's closure rests on a CALL-SITE SET staying closed, and nothing failed when it
//! changed. This is that guard.
//!
//! # ENUMERATED AT THE LAYER THAT TAKES THE PARAMETER, NOT AT THE NAME SOMEONE ASKED ABOUT
//!
//! The question was first put as "who calls `load_index_checked`", and answering exactly that
//! would have produced an incomplete set with no way to notice. `fold_deltas` is a parameter of
//! `load_index_inner`, and THREE wrappers pass it:
//!
//! | wrapper | `fold_deltas` |
//! |---|---|
//! | `load_index` | **true** |
//! | `load_index_checked` | **true** |
//! | `load_index_base_only` | false |
//!
//! `load_index_inner` has no other caller, so the set of fold-reaching entry points is CLOSED at
//! two rather than merely long. This file checks the callers of both, and
//! [`the_only_fold_reaching_loaders_are_the_two_this_census_knows_about`] fails if a third
//! wrapper starts passing `true`.
//!
//! # WHAT IS ASSERTED, AND WHICH HALF IS LOAD-BEARING
//!
//! THE GATING IS LOAD-BEARING. THE COUNT IS A CROSS-CHECK. A census that counts call sites passes
//! when someone swaps a gated caller for an un-gated one at the same count, which is the mutation
//! this guard most needs to catch. So every production call site is required to sit inside the
//! `wal_single_barrier()` else-branch -- found by matching braces from the `if`, not by a line
//! number -- and the count is reported beside it rather than relied upon.
//!
//! TEST CALLERS ARE EXCLUDED BY THEIR `#[cfg(test)]`, NOT BY THEIR PATH. `load_index`'s only
//! caller is `test_publish_recovering_shard`, which lives in `lifecycle.rs` -- a production file
//! -- behind a `#[cfg(test)]` on the function. A path rule would have called it production; a
//! path rule would equally have let a new test-looking file hide a real caller. For a call site in
//! a file reached only through a `#[cfg(test)] mod` declaration, the census VERIFIES that
//! declaration in the declaring file instead of trusting the directory name.
//!
//! AN UNCLASSIFIABLE CALL SITE FAILS. It is not skipped. A census whose unknown bucket is a pass
//! is the shape that produced the error this guard exists to prevent.

#![allow(clippy::all)]
use std::path::{Path, PathBuf};

/// The two loaders that pass `fold_deltas: true`, as they appear at a call site.
const FOLD_REACHING_CALLS: [&str; 2] = [".load_index(", ".load_index_checked("];

/// The production functions allowed to hold a fold-reaching call, each with why it is safe.
///
/// `load_shard_with` holds one directly, inside the `wal_single_barrier()` else-branch.
/// `install_latest_manifest_if_newer_on_load` holds one too and is NOT itself inside that
/// branch -- its gating comes from its callers, every production one of which is inside it, which
/// is checked separately below. A new entry here is a new argument someone has to write down.
const GATED_PRODUCTION_FUNCTIONS: [&str; 2] =
    ["load_shard_with", "install_latest_manifest_if_newer_on_load"];

/// Does `line` contain `needle` as CODE rather than inside a string literal?
///
/// # THIS FILE MATCHED ITSELF, AND THAT IS THE WHOLE REASON THIS EXISTS
///
/// The matcher constant is `[".load_index(", ".load_index_checked("]`, which contains both
/// needles -- so the census found a call site at its own declaration, with no enclosing function,
/// and refused to classify it. Every assertion message below naming a loader is the same hazard:
/// a worked example in a test is not inert.
///
/// A quote counter rather than a full lexer, deliberately. The question is only "is this
/// occurrence inside a string on this line", the file holds no raw strings or multi-line string
/// literals spanning a call site, and the escape case (`\"`) is handled. A character inside a
/// `'c'` literal cannot spell either needle.
fn contains_outside_string(line: &str, needle: &str) -> bool {
    let bytes = line.as_bytes();
    let mut from = 0usize;
    while let Some(found) = line[from..].find(needle) {
        let at = from + found;
        let mut quotes = 0usize;
        let mut index = 0usize;
        while index < at {
            if bytes[index] == b'\\' {
                index += 2;
                continue;
            }
            if bytes[index] == b'"' {
                quotes += 1;
            }
            index += 1;
        }
        if quotes % 2 == 0 {
            return true;
        }
        from = at + needle.len();
    }
    false
}

/// One call site of a fold-reaching loader.
#[derive(Debug)]
struct CallSite {
    file: PathBuf,
    line: usize,
    function: String,
    test_only: Option<bool>,
}

fn src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
    out.sort();
}

fn is_fn_decl(trimmed: &str) -> bool {
    for prefix in [
        "fn ",
        "pub fn ",
        "async fn ",
        "pub async fn ",
        "pub(crate) fn ",
        "pub(super) fn ",
        "pub(crate) async fn ",
        "pub(super) async fn ",
    ] {
        if trimmed.starts_with(prefix) {
            return true;
        }
    }
    false
}

fn fn_name(trimmed: &str) -> String {
    let after = trimmed.split("fn ").nth(1).unwrap_or("");
    after
        .split(|c: char| c == '(' || c == '<' || c == ' ')
        .next()
        .unwrap_or("")
        .to_string()
}

/// Is this file reached only through a `#[cfg(test)]` module declaration, anywhere up its chain?
///
/// # AN UPWARD WALK, BECAUSE THE GATE IS NOT NECESSARILY ON THE FILE'S OWN DECLARATION
///
/// `engine/tests/foo.rs` is declared by `engine/tests.rs` with a plain `mod foo;` -- no gate
/// there at all. The `#[cfg(test)]` is one level further up, on `engine.rs`'s `mod tests;`. So a
/// check that only looks at the file's own declaration answers "not test" for every one of the
/// ~130 test modules in this tree, and the first version of this did exactly that and returned
/// `None` for the production files too because it looked for each module's declaration in the
/// wrong file.
///
/// This walks from the file up to `lib.rs`, resolving each module's declaring file as the
/// directory's sibling `.rs` (or its `mod.rs`), and answers `true` at the first `#[cfg(test)]` it
/// finds above the matching `mod` line.
///
/// `None` when a step's declaration cannot be found. The caller treats that as a FAILURE rather
/// than as "probably production", because an unreadable module chain is exactly the state in
/// which a real caller could hide.
fn module_chain_is_test_gated(file: &Path) -> Option<bool> {
    let root = src_root();
    let mut current = file.to_path_buf();
    for _ in 0..12 {
        let name = current.file_stem()?.to_str()?.to_string();
        let dir = current.parent()?.to_path_buf();
        if dir == root && (name == "lib" || name == "main") {
            return Some(false);
        }
        let declaring = if dir == root {
            root.join("lib.rs")
        } else {
            let dir_name = dir.file_name()?.to_str()?.to_string();
            let sibling = dir.parent()?.join(format!("{dir_name}.rs"));
            if sibling.exists() {
                sibling
            } else {
                dir.join("mod.rs")
            }
        };
        match declaration_is_cfg_test(&declaring, &name)? {
            true => return Some(true),
            false => current = declaring,
        }
    }
    None
}

/// Does `declaring` hold `mod <name>;`, and is it preceded by `#[cfg(test)]`?
///
/// `None` when the declaration is not in that file at all -- which is a fact worth surfacing
/// rather than defaulting, since it means the module chain is not what this census assumes.
fn declaration_is_cfg_test(declaring: &Path, name: &str) -> Option<bool> {
    let text = std::fs::read_to_string(declaring).ok()?;
    let lines: Vec<&str> = text.lines().collect();
    let needle = format!("mod {name};");
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        // `mod x;`, `pub mod x;`, `pub(crate) mod x;` -- the visibility is not the question.
        if !trimmed.ends_with(&needle) {
            continue;
        }
        let mut back = index;
        while back > 0 {
            back -= 1;
            let above = lines[back].trim();
            if above == "#[cfg(test)]" {
                return Some(true);
            }
            if !above.starts_with("#[") && !above.starts_with("//") && !above.is_empty() {
                break;
            }
        }
        return Some(false);
    }
    None
}

/// Every call site of a fold-reaching loader under `src/`, classified.
fn fold_reaching_call_sites() -> Vec<CallSite> {
    let root = src_root();
    let mut files = Vec::new();
    collect_rs(&root, &mut files);
    let mut sites = Vec::new();

    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            // Comments and doc comments name these loaders constantly; they call nothing.
            if trimmed.starts_with("//") {
                continue;
            }
            if !FOLD_REACHING_CALLS
                .iter()
                .any(|call| contains_outside_string(line, call))
            {
                continue;
            }
            // The enclosing function, by walking back to the nearest declaration.
            let mut function = String::from("<none>");
            let mut decl_index = None;
            for back in (0..index).rev() {
                let candidate = lines[back].trim_start();
                if is_fn_decl(candidate) {
                    function = fn_name(candidate);
                    decl_index = Some(back);
                    break;
                }
            }
            // `#[cfg(test)]` on that function, over contiguous attributes and doc lines.
            let mut test_only = None;
            if let Some(decl) = decl_index {
                let mut local_gate = false;
                let mut back = decl;
                while back > 0 {
                    back -= 1;
                    let above = lines[back].trim();
                    if above == "#[cfg(test)]" {
                        local_gate = true;
                        break;
                    }
                    if !above.starts_with("#[") && !above.starts_with("//") && !above.is_empty() {
                        break;
                    }
                }
                test_only = if local_gate {
                    Some(true)
                } else {
                    module_chain_is_test_gated(&file)
                };
            }
            sites.push(CallSite {
                file: file.clone(),
                line: index + 1,
                function,
                test_only,
            });
        }
    }
    sites
}

/// The line range of the `else` block belonging to `if wal_single_barrier() {` in
/// `lifecycle.rs`, as (first line after `} else {`, line of the closing brace).
///
/// # FOUND BY INDENTATION, NOT BY COUNTING BRACES, AND THE FIRST VERSION COUNTED BRACES
///
/// A brace counter walking from the `if` never sees depth return to zero at the `} else {`,
/// because that line closes one block and opens another in the same breath -- net zero, so the
/// counter sails past it and reports the end of the ELSE block as an `if` that had no `else`.
/// That is exactly what it did, and it failed loudly rather than silently, which is the only
/// reason this note is accurate rather than speculative.
///
/// Counting braces is also wrong for a second reason that would have bitten later: the range
/// holds `format!` strings with `{placeholder}` braces and comment prose containing braces, so a
/// character counter needs a string-and-comment scanner to be correct at all.
///
/// The structure is matched instead. `rustfmt` puts an `if`/`else` chain's `} else {` and its
/// closing brace at the SAME indentation as the line the `if` opens on; everything inside is
/// deeper. So the three landmarks are unambiguous, and each one is checked for the text it is
/// supposed to be rather than assumed from its position.
fn wal_single_barrier_else_lines() -> (usize, usize) {
    let path = src_root().join("engine").join("lifecycle.rs");
    let text = std::fs::read_to_string(&path).expect("lifecycle.rs is readable");
    let lines: Vec<&str> = text.lines().collect();

    let open = lines
        .iter()
        .position(|line| line.contains("if wal_single_barrier() {"))
        .expect(
            "the `if wal_single_barrier()` gate is gone from lifecycle.rs. That gate is the whole \
             reason the index-log fold is unreachable in a shipped configuration; if the recovery \
             arm has been restructured, re-derive which branch reaches a fold-aware loader before \
             changing this test. See `the_third_door_stays_shut` and #2152.",
        );
    let indent = lines[open].len() - lines[open].trim_start().len();
    let at_same_indent = |line: &str| {
        !line.trim().is_empty() && line.len() - line.trim_start().len() == indent
    };

    let mut else_line = None;
    for (index, line) in lines.iter().enumerate().skip(open + 1) {
        if !at_same_indent(line) {
            continue;
        }
        if line.trim() == "} else {" {
            else_line = Some(index);
            break;
        }
        panic!(
            "the first line at the `if wal_single_barrier()` indentation after lifecycle.rs:{} is \
             {:?} at line {}, not `}} else {{`. The recovery arm has been restructured, so this \
             census can no longer tell which branch reaches a fold-aware loader. Re-derive the \
             gating before changing this test.",
            open + 1,
            line.trim(),
            index + 1
        );
    }
    let else_line = else_line.unwrap_or_else(|| {
        panic!(
            "the `if wal_single_barrier()` at lifecycle.rs:{} has no `}} else {{` at its own \
             indentation, so there is no else-branch for the fold-aware loader to sit in",
            open + 1
        )
    });

    for (index, line) in lines.iter().enumerate().skip(else_line + 1) {
        if !at_same_indent(line) {
            continue;
        }
        let trimmed = line.trim();
        assert!(
            trimmed == "};" || trimmed == "}",
            "the `wal_single_barrier()` else-branch does not close with a brace at its own \
             indentation; line {} is {trimmed:?}. Re-derive the gating before changing this test.",
            index + 1
        );
        return (else_line + 1, index + 1);
    }
    panic!(
        "the `wal_single_barrier()` else-branch opened at lifecycle.rs:{} and never closed at its \
         own indentation",
        else_line + 1
    );
}

/// EVERY PRODUCTION CALLER OF A FOLD-REACHING LOADER SITS BEHIND THE FLAG GATE.
#[test]
fn every_production_caller_of_a_fold_reaching_loader_is_flag_gated() {
    let sites = fold_reaching_call_sites();
    assert!(
        !sites.is_empty(),
        "the census found NO call site of either fold-reaching loader. That is not a clean bill \
         of health -- the matcher has stopped matching. The loaders are {FOLD_REACHING_CALLS:?} \
         and they are called from `engine/lifecycle.rs` at least twice."
    );

    let (else_start, else_end) = wal_single_barrier_else_lines();
    let lifecycle = src_root().join("engine").join("lifecycle.rs");

    let mut production = Vec::new();
    let mut undetermined = Vec::new();
    let mut test_sites = 0usize;
    for site in &sites {
        match site.test_only {
            Some(true) => test_sites += 1,
            Some(false) => production.push(site),
            None => undetermined.push(site),
        }
    }

    assert!(
        undetermined.is_empty(),
        "a fold-reaching call site could not be classified as production or test, and an \
         unclassified site FAILS rather than passing: {undetermined:#?}. Classify it by its \
         `#[cfg(test)]` -- on the function, or on the `mod` declaration that reaches its file -- \
         and then decide whether it belongs in GATED_PRODUCTION_FUNCTIONS."
    );

    for site in &production {
        assert!(
            GATED_PRODUCTION_FUNCTIONS.contains(&site.function.as_str()),
            "A NEW PRODUCTION CALLER OF A FOLD-REACHING LOADER: `{}` at {}:{}.\n\n\
             WHAT THIS MEANS. The index-log fold runs \
             `apply_key_state_field(&mut shard.context_nodes, ...)`, and the fold DOES deliver a \
             `context_nodes` entry -- `the_third_door_stays_shut` measures it. The only reason \
             that is not a live route is that no shipped configuration reaches a fold-aware \
             loader unless `TS_WAL_LEGACY_RECOVERY` is set. A caller outside that gate RE-OPENS \
             door 3.\n\n\
             WHAT DEPENDS ON IT. The `context_nodes` verdict of NOT REMOVABLE (#2148, #2151) \
             rests on this, and so does any further object-keyed map removal that is justified \
             by the same closure. #2152 established the two halves -- the fold delivers, the \
             shipped arm never asks.\n\n\
             WHAT TO DO. If the new caller is behind the flag gate, add its function to \
             GATED_PRODUCTION_FUNCTIONS with the argument for why. If it is NOT, door 3 is open \
             and the maps that depend on its closure must be re-examined before this test is \
             changed.",
            site.function,
            site.file.display(),
            site.line
        );

        // THE GATING, WHICH IS THE LOAD-BEARING HALF.
        if site.function == "load_shard_with" {
            assert!(
                site.file == lifecycle,
                "`load_shard_with` has moved out of lifecycle.rs ({}), so the brace-matched gate \
                 range no longer covers its call site",
                site.file.display()
            );
            assert!(
                site.line > else_start && site.line < else_end,
                "the fold-aware load in `load_shard_with` is at lifecycle.rs:{} which is OUTSIDE \
                 the `wal_single_barrier()` else-branch (lines {}..{}). On the single-barrier \
                 default that call now runs, which means the index-log fold runs, which means \
                 `context_nodes` key-states are applied on a shipped recovery path. Door 3 is \
                 open.",
                site.line,
                else_start,
                else_end
            );
        }
    }

    // `install_latest_manifest_if_newer_on_load` is gated by its CALLERS, not by its own body.
    let indirect_callers = production_callers_of("install_latest_manifest_if_newer_on_load");
    assert!(
        !indirect_callers.is_empty(),
        "no production caller of `install_latest_manifest_if_newer_on_load` was found, so the \
         matcher has stopped matching rather than the function having become unreachable"
    );
    for (file, line, function) in &indirect_callers {
        assert!(
            *file == lifecycle && *line > else_start && *line < else_end,
            "`install_latest_manifest_if_newer_on_load` is called from production at {}:{} (in \
             `{function}`), OUTSIDE the `wal_single_barrier()` else-branch (lines {else_start}..\
             {else_end}). That function holds a fold-aware load, so this caller reaches the \
             index-log fold on a path a deployment takes, and door 3 is open.",
            file.display(),
            line
        );
    }

    println!(
        "  fold-reaching call sites: {} total, {} test-gated, {} production; gate else-branch \
         lines {}..{}",
        sites.len(),
        test_sites,
        production.len(),
        else_start,
        else_end
    );
    // The count, reported and cross-checked but NOT the load-bearing claim: a swap of a gated
    // caller for an un-gated one leaves it unchanged, which is why the gating is asserted above.
    assert_eq!(
        2,
        production.len(),
        "the number of production callers of a fold-reaching loader moved. This is the weaker \
         half of this test -- the gating above is what matters -- but a change here means the \
         enumeration in #2152's follow-up is stale and the body of whatever removal depends on it \
         is quoting a number that is no longer true."
    );
}

/// Production call sites of `name`, as (file, line, enclosing function).
fn production_callers_of(name: &str) -> Vec<(PathBuf, usize, String)> {
    let root = src_root();
    let mut files = Vec::new();
    collect_rs(&root, &mut files);
    let needle = format!(".{name}(");
    let mut out = Vec::new();
    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.trim_start().starts_with("//") || !contains_outside_string(line, &needle) {
                continue;
            }
            let mut function = String::from("<none>");
            let mut decl_index = None;
            for back in (0..index).rev() {
                let candidate = lines[back].trim_start();
                if is_fn_decl(candidate) {
                    function = fn_name(candidate);
                    decl_index = Some(back);
                    break;
                }
            }
            let mut local_gate = false;
            if let Some(decl) = decl_index {
                let mut back = decl;
                while back > 0 {
                    back -= 1;
                    let above = lines[back].trim();
                    if above == "#[cfg(test)]" {
                        local_gate = true;
                        break;
                    }
                    if !above.starts_with("#[") && !above.starts_with("//") && !above.is_empty() {
                        break;
                    }
                }
            }
            if local_gate || module_chain_is_test_gated(&file) == Some(true) {
                continue;
            }
            out.push((file.clone(), index + 1, function));
        }
    }
    out
}

/// THE SET OF FOLD-REACHING WRAPPERS IS CLOSED AT TWO.
///
/// `load_index_inner` takes `fold_deltas`, and this fails if a third wrapper starts passing
/// `true` -- which would be a fold-reaching entry point the census above does not look at, and
/// therefore a hole in exactly the shape that made the first version of this enumeration
/// incomplete.
#[test]
fn the_only_fold_reaching_loaders_are_the_two_this_census_knows_about() {
    let path = src_root().join("engine").join("persistence.rs");
    let text = std::fs::read_to_string(&path).expect("persistence.rs is readable");
    let lines: Vec<&str> = text.lines().collect();

    let mut passing_true = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || !contains_outside_string(line, "load_index_inner(") {
            continue;
        }
        if !line.contains("true") {
            continue;
        }
        // The wrapper this call sits in.
        let mut function = String::from("<none>");
        for back in (0..index).rev() {
            let candidate = lines[back].trim_start();
            if is_fn_decl(candidate) {
                function = fn_name(candidate);
                break;
            }
        }
        passing_true.push(function);
    }
    passing_true.sort();
    passing_true.dedup();

    assert_eq!(
        vec!["load_index", "load_index_checked"],
        passing_true,
        "the set of wrappers passing `fold_deltas: true` to `load_index_inner` has changed. The \
         census in this file only looks at `load_index` and `load_index_checked`, so a third \
         fold-reaching wrapper is a caller set nobody is checking -- which is precisely the hole \
         that made the first version of this enumeration incomplete (it asked about \
         `load_index_checked` alone and missed `load_index`). Add the new wrapper to \
         FOLD_REACHING_CALLS and re-derive its callers."
    );
}

/// WHAT THE FLAG'S READER DOES WITH WHAT AN OPERATOR MIGHT ACTUALLY WRITE.
///
/// Door 3's closure holds while `TS_WAL_LEGACY_RECOVERY` is off, so "off" has to mean off for
/// every spelling of it. The dangerous direction for a DEFAULT-ON flag is a value meant to keep it
/// on that reads as off; for this default-OFF flag it is the reverse -- a value meant to turn it
/// off, or a typo, that reads as ON and silently puts the fold back on the recovery path.
///
/// This drives `parse_bool`, which is pure, and `env_bool` through a variable name that appears
/// NOWHERE ELSE in the crate. The real flag name is deliberately not touched: `env_bool` reads the
/// process environment, which is shared, and setting `TS_WAL_LEGACY_RECOVERY` here would change
/// the recovery arm of every test running beside this one.
#[test]
fn no_spelling_of_off_turns_the_legacy_recovery_flag_on() {
    use crate::env_flag::{env_bool, parse_bool};

    for off in [
        "0", " 0", "0 ", "\t0\n", "false", "FALSE", "False", "off", "OFF", "Off", "no", "NO",
    ] {
        assert_eq!(
            Some(false),
            parse_bool(off),
            "`{off:?}` is a spelling of OFF and must parse as false"
        );
    }
    for on in ["1", " 1", "true", "TRUE", "yes", "on", "ON", "On"] {
        assert_eq!(
            Some(true),
            parse_bool(on),
            "`{on:?}` is a spelling of ON and must parse as true"
        );
    }
    // Outside the vocabulary: `None`, so the caller's own default decides. For this flag the
    // default is `false`, so every unrecognised value leaves the fold unreachable -- the safe
    // direction, and the one that matters here.
    for unknown in ["", " ", "2", "-1", "wat", "ofc", "truthy", "0.0"] {
        assert_eq!(
            None,
            parse_bool(unknown),
            "`{unknown:?}` is outside the boolean vocabulary and must parse as None, so the \
             caller's default decides rather than a guess"
        );
    }

    // And the same through `env_bool`, on a name of this test's own so nothing else is disturbed.
    const PROBE: &str = "TS_FOLD_CENSUS_PROBE_4071";
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            std::env::remove_var(PROBE);
        }
    }
    let _restore = Restore;

    std::env::remove_var(PROBE);
    assert!(
        !env_bool(PROBE, false),
        "unset must fall back to the default, which for the legacy-recovery flag is off"
    );
    for (value, expected) in [
        ("0", false),
        (" 0", false),
        ("off", false),
        ("FALSE", false),
        ("", false),
        ("2", false),
        ("wat", false),
        ("1", true),
        (" 1", true),
        ("on", true),
        ("TRUE", true),
    ] {
        std::env::set_var(PROBE, value);
        assert_eq!(
            expected,
            env_bool(PROBE, false),
            "`{value:?}` read against a default of false must be {expected}"
        );
    }
}

/// THE MATCHER IS SELF-TESTED, because a census whose matcher has stopped matching reports a
/// clean tree.
#[test]
fn the_call_site_matcher_sees_code_and_not_strings() {
    assert!(
        contains_outside_string("        let loaded = self.load_index(shard_id, warm);", ".load_index("),
        "the matcher no longer sees a real call, so the census above would report no call sites \
         at all and its emptiness check is the only thing standing between that and a pass"
    );
    assert!(
        !contains_outside_string(
            r#"const FOLD: [&str; 1] = [".load_index("];"#,
            ".load_index("
        ),
        "the matcher counts its own declaration as a call site, which is what made the first run \
         of this census refuse to classify line 59 of this very file"
    );
    assert!(
        !contains_outside_string(
            r#"        panic!("a caller of .load_index_checked( appeared");"#,
            ".load_index_checked("
        ),
        "the matcher counts a loader named inside an assertion message as a call site"
    );
    assert!(
        contains_outside_string(
            r#"        foo("a note about .load_index("); self.load_index_checked(s, false);"#,
            ".load_index_checked("
        ),
        "a real call later on a line that also mentions a loader in a string must still be seen"
    );
}
