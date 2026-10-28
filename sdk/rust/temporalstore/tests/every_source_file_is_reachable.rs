// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Every `.rs` file under `src/` must be reachable from a crate target.
//!
//! This crate shipped 88 files the compiler never saw. `src/lib.rs` had been reduced to a module
//! root in July 2026 and was then overwritten by a bulk commit that restored the pre-split
//! monolith and dropped every `mod` declaration with it; `src/bin/matrixark_rust_proxy_impl.rs`
//! lost its own 47 declarations the same way a week earlier. The files stayed in the tree, stayed
//! in the published tarball, and went on being hand-edited by later commits -- but nothing
//! compiled them, so none of those edits could fail, and two of them silently reverted fixes that
//! had already landed in the copy that did compile.
//!
//! A comment cannot catch that recurring. `cargo build` cannot either: a file nothing names is
//! exactly as green as a correct one. So the rule is asserted here, against the file system.

use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Target roots, taken from the manifest's own `path = "..."` declarations rather than from a
/// list kept here. A new `[[bin]]` is then covered the moment it is declared, and a root this
/// test does not know about cannot make the sweep quietly pass.
fn declared_roots(manifest: &str) -> BTreeSet<PathBuf> {
    let mut roots = BTreeSet::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some(rest) = line.strip_prefix("path") else {
            continue;
        };
        let Some(rest) = rest.trim_start().strip_prefix('=') else {
            continue;
        };
        if let Some(value) = quoted(rest) {
            if value.ends_with(".rs") && value.starts_with("src/") {
                roots.insert(PathBuf::from(value));
            }
        }
    }
    roots
}

/// The first double-quoted run in `text`.
fn quoted(text: &str) -> Option<String> {
    let start = text.find('"')? + 1;
    let end = text[start..].find('"')? + start;
    Some(text[start..end].to_string())
}

/// `mod name;` declarations and `include!("...")` invocations in one file, as (explicit path,
/// module name) pairs. An explicit `#[path = "..."]` wins over name-based resolution.
fn declarations(source: &str) -> Vec<(Option<String>, Option<String>)> {
    let mut out = Vec::new();
    let mut pending_path: Option<String> = None;
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") {
            continue;
        }
        if trimmed.starts_with("#[path") {
            pending_path = quoted(trimmed);
            continue;
        }
        if let Some(rest) = trimmed.find("include!(").map(|i| &trimmed[i + "include!(".len()..]) {
            if let Some(path) = quoted(rest) {
                out.push((Some(path), None));
            }
            continue;
        }
        let without_vis = trimmed
            .strip_prefix("pub(crate) ")
            .or_else(|| trimmed.strip_prefix("pub "))
            .unwrap_or(trimmed);
        if let Some(rest) = without_vis.strip_prefix("mod ") {
            if let Some(name) = rest.strip_suffix(';') {
                let name = name.trim();
                if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    out.push((pending_path.take(), Some(name.to_string())));
                    continue;
                }
            }
        }
        // An attribute that turned out not to belong to a `mod` does not carry over.
        if !trimmed.is_empty() && !trimmed.starts_with("#[") {
            pending_path = None;
        }
    }
    out
}

/// Where `owner` (relative to the crate root) places a declared child.
fn candidates(owner: &Path, explicit: Option<&str>, name: Option<&str>) -> Vec<PathBuf> {
    let dir = owner.parent().unwrap_or(Path::new("")).to_path_buf();
    if let Some(path) = explicit {
        // `#[path]` and `include!` are both relative to the declaring file's directory.
        return vec![dir.join(path)];
    }
    let Some(name) = name else {
        return Vec::new();
    };
    let mut out = vec![dir.join(format!("{name}.rs")), dir.join(name).join("mod.rs")];
    // A non-root module owns a directory named after itself.
    if let Some(stem) = owner.file_stem().and_then(|s| s.to_str()) {
        if stem != "lib" && stem != "main" && stem != "mod" {
            out.push(dir.join(stem).join(format!("{name}.rs")));
            out.push(dir.join(stem).join(name).join("mod.rs"));
        }
    }
    out
}

#[test]
fn every_source_file_is_reachable_from_a_target() {
    let root = manifest_dir();
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");
    let roots = declared_roots(&manifest);

    // Premise: if the manifest stopped declaring target paths, this sweep would have nothing to
    // walk from and would pass by having no work to do. Pin the roots it actually found.
    assert!(
        roots.contains(Path::new("src/lib.rs")),
        "premise: the manifest must declare the library root, found {roots:?}"
    );
    assert!(
        roots.len() >= 2,
        "premise: the manifest declares a library and at least one binary, found {roots:?}"
    );

    let mut reachable: BTreeSet<PathBuf> = BTreeSet::new();
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    for target in &roots {
        queue.push_back(target.clone());
    }

    while let Some(rel) = queue.pop_front() {
        if !reachable.insert(rel.clone()) {
            continue;
        }
        let source = match fs::read_to_string(root.join(&rel)) {
            Ok(text) => text,
            Err(err) => panic!("a declared target or module is missing: {rel:?}: {err}"),
        };
        for (explicit, name) in declarations(&source) {
            let options = candidates(&rel, explicit.as_deref(), name.as_deref());
            let resolved = options.iter().find(|p| root.join(p).is_file());
            match resolved {
                Some(path) => queue.push_back(path.clone()),
                None => panic!(
                    "{rel:?} declares {:?} but none of {options:?} exist",
                    explicit.or(name).unwrap_or_default()
                ),
            }
        }
    }

    let mut present: BTreeSet<PathBuf> = BTreeSet::new();
    let mut dirs = vec![root.join("src")];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).expect("read src/") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                present.insert(
                    path.strip_prefix(&root)
                        .expect("under the crate root")
                        .to_path_buf(),
                );
            }
        }
    }

    // Premise: the sweep found the tree. A `src/` that read as empty would otherwise make the
    // assertion below trivially true.
    assert!(
        present.len() > 20,
        "premise: src/ holds the crate's sources, found {} file(s)",
        present.len()
    );

    let orphans: Vec<&PathBuf> = present.difference(&reachable).collect();
    assert!(
        orphans.is_empty(),
        "{} file(s) under src/ are not reachable from any target, so nothing compiles them:\n{}",
        orphans.len(),
        orphans
            .iter()
            .map(|p| format!("  {}", p.display()))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
