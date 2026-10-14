// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! No production signature names the series representation, except where naming the STORED or
//! REPORTED form is the point.
//!
//! # WHY THIS GUARD EXISTS, AND WHAT IT REPLACES
//!
//! Eight resident maps hold `object key -> (timestamp -> page address)`. The seam that let them be
//! narrowed one at a time required their helpers to take the OPERATIONS rather than the container.
//! That seam was built by following one function's neighbourhood and covered **four** signatures.
//! There were **nine**. The fifth was found when a migration failed to compile against it, and the
//! remaining four after that only because the type was finally used as the enumeration.
//!
//! So the claim "the seam is complete" was an assurance, twice, and wrong once. This makes it a
//! checked property.
//!
//! # THE OPERATIONAL LESSON, WHICH IS WORTH MORE THAN THE SEAM
//!
//! A seam is only as general as the dimension it varied -- and that includes WHICH CALLERS WERE
//! SAMPLED, not only which type parameter was abstracted. Proximity finds the first few and is
//! never a population.
//!
//! **When the population is characterised by a type, the type is the enumeration.** A one-line
//! query on the type was available the whole time and was worth more than the neighbourhood walk.
//! Any claim of the form "all the places that X" should name the mechanical query that produced
//! it, so a reader can re-run it rather than trust it. This test IS that query.
//!
//! # WHAT COUNTS AS PRODUCTION, STATED RATHER THAN ASSUMED
//!
//! Every `.rs` file under the crate's `src/`, excluding any whose path has a `tests` directory
//! component. The scope is derived from the crate root and walked recursively -- NOT from a list
//! of directories, because a hand-listed scope is the same sampling error inside the detector. A
//! signature in `storage_reporting.rs` or `packed_pages.rs` is exactly what the original walk
//! missed.
//!
//! A `#[cfg(test)]` region inside a production FILE is not excluded. That is deliberate and it is
//! the cheaper rule: tracking attribute regions by brace counting is fragile, and the alternative
//! -- an allowlist entry with a reason -- is both checkable and self-documenting. One such entry
//! exists and says so.
//!
//! rust-internal: reads the crate's own source, no product behaviour

/// The representation the seam exists to stop naming.
const CONCRETE_SERIES: &str = "BTreeMap<u64, ElementEntry>";

/// Where naming it is CORRECT, each with the reason, keyed by file and enclosing function.
///
/// The distinction every entry shares: these name the STORED or REPORTED form rather than
/// operating on a live series. A stored shape genuinely is a plain map, and a report that projects
/// every kind into one uniform form is doing conversion, not leaking a representation.
const ALLOWED: &[(&str, &str, &str)] = &[
    (
        "storage_reporting.rs",
        "timestamped_kv_series",
        "A REPORTING PROJECTION. It collects every kind into one uniform shape for a report, and \
         returns `Cow`, so a kind on a different representation converts into it. Converting into \
         a reporting form is the point of the function; it does not operate on a live series.",
    ),
    (
        "grouped_series_index.rs",
        "from_plain",
        "A DESERIALIZER, naming the STORED form. The stored shape of this index genuinely is a \
         plain map; this is the conversion from it into the internal representation. Naming the \
         wire form is not leaking the in-memory one.",
    ),
    (
        "grouped_series_index.rs",
        "plain",
        "A TEST FIXTURE inside a production file. It builds the stored form to feed the \
         deserializer above. Allowlisted rather than excluded because this guard's definition of \
         production is a path rule, which is checkable, where excluding `#[cfg(test)]` regions \
         would need brace tracking, which is not.",
    ),
];

/// One place the concrete representation is named in a signature.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Naming {
    file: String,
    function: String,
    line: usize,
}

/// Whether a line names the representation in a SIGNATURE position rather than in prose.
///
/// Matched as a construct and not as text: the word appears in comments explaining the seam, and a
/// text match counts those. A signature position is a `fn` line, a parameter line, or a return.
fn names_the_series_in_a_signature(line: &str) -> bool {
    let trimmed = line.trim();
    if !trimmed.contains(CONCRETE_SERIES) {
        return false;
    }
    if trimmed.starts_with("//") || trimmed.starts_with("///") || trimmed.starts_with("*") {
        return false;
    }
    trimmed.starts_with("fn ")
        || trimmed.contains(" fn ")
        || trimmed.starts_with("->")
        || trimmed.contains(") -> ")
        || trimmed
            .split_once(':')
            .is_some_and(|(name, _)| {
                !name.is_empty()
                    && name
                        .trim_start_matches("mut ")
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c == '_')
            })
}

/// Every `.rs` file under a directory, recursively.
fn rust_files(root: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// The enclosing function of a line, found by walking UP to the nearest `fn` declaration.
///
/// By enclosing function and not by a unique string, because a string moves and a plant located by
/// one can match a byte-identical tail somewhere else -- which has happened in this tree.
fn enclosing_function(lines: &[&str], index: usize) -> String {
    for above in (0..=index).rev() {
        let trimmed = lines[above].trim_start();
        if let Some(rest) = trimmed.strip_prefix("fn ").or_else(|| {
            trimmed
                .split_once(" fn ")
                .map(|(_, rest)| rest)
                .filter(|_| trimmed.starts_with("pub"))
        }) {
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                return name;
            }
        }
    }
    "<no enclosing fn>".to_string()
}

/// NO PRODUCTION SIGNATURE NAMES THE SERIES REPRESENTATION.
#[test]
fn no_production_signature_names_the_series_representation() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);

    // --- THE DETECTOR MUST SEE THE POPULATION IT CLAIMS. A scope that silently covered three
    // --- files would report a clean seam, which is this guard reproducing the error it exists
    // --- for.
    assert!(
        files.len() >= 40,
        "only {} .rs file(s) were found under {}, and this crate has far more -- the walk is not \
         reaching the population this guard claims to cover",
        files.len(),
        src.display()
    );

    let mut found: Vec<Naming> = Vec::new();
    let mut production_files = 0usize;
    for path in &files {
        if path
            .components()
            .any(|component| component.as_os_str() == "tests")
        {
            continue;
        }
        production_files += 1;
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if names_the_series_in_a_signature(line) {
                found.push(Naming {
                    file: path
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                        .unwrap_or_default(),
                    function: enclosing_function(&lines, index),
                    line: index + 1,
                });
            }
        }
    }
    assert!(
        production_files >= 30,
        "only {production_files} production file(s) after excluding test directories, which is \
         fewer than this crate has"
    );

    // --- THE SET DIFFERENCE, BOTH WAYS, NAMING THE ELEMENTS. Two counts would not tell a reader
    // --- which signature to go and look at, and that is the part they cannot reconstruct by
    // --- reasoning.
    let unexpected: Vec<&Naming> = found
        .iter()
        .filter(|naming| {
            !ALLOWED
                .iter()
                .any(|(file, function, _)| *file == naming.file && *function == naming.function)
        })
        .collect();
    let stale: Vec<&(&str, &str, &str)> = ALLOWED
        .iter()
        .filter(|(file, function, _)| {
            !found
                .iter()
                .any(|naming| naming.file == *file && naming.function == *function)
        })
        .collect();

    assert!(
        unexpected.is_empty(),
        "these production signatures name the series representation and are not allowlisted:\n{}\n\
         \n\
         The seam exists so a series kind can change representation one at a time. A signature \
         naming `{CONCRETE_SERIES}` pins every caller to that one container, which is what made \
         the eight maps all-or-none before.\n\
         \n\
         Either widen it -- take `impl TimestampedSeries` or a type parameter -- or, if naming the \
         STORED or REPORTED form is the point, add it to ALLOWED with the reason. Every existing \
         entry names a stored or reported form rather than operating on a live series.",
        unexpected
            .iter()
            .map(|naming| format!("  {}:{} in `{}`", naming.file, naming.line, naming.function))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        stale.is_empty(),
        "these allowlist entries no longer match anything, so the list is describing a tree that \
         has moved on:\n{}\n\
         \n\
         An allowlist nobody prunes becomes a list of things that may name the representation, \
         which is the opposite of what it is for. Remove the entry, or find where the signature \
         went.",
        stale
            .iter()
            .map(|(file, function, _)| format!("  {file} in `{function}`"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    println!(
        "  {} production file(s) scanned, {} signature(s) naming the representation, all {} \
         allowlisted with a reason",
        production_files,
        found.len(),
        ALLOWED.len()
    );

    // --- CONTROL ONE: NON-VACUOUS. If the matcher finds nothing at all, every assertion above
    // --- is satisfied by a detector that cannot see.
    assert!(
        !found.is_empty(),
        "the matcher found NO signature naming the representation anywhere, including the \
         allowlisted ones that are known to exist. It is not matching"
    );

    // --- CONTROL TWO: A PLANTED SIGNATURE FIRES, located by its enclosing function.
    let planted = [
        "fn an_unmigrated_helper(",
        "    series: &mut BTreeMap<u64, ElementEntry>,",
        ") {",
    ];
    let planted_lines: Vec<&str> = planted.to_vec();
    assert!(
        names_the_series_in_a_signature(planted_lines[1]),
        "a planted parameter naming the representation was not detected, so a real one would not \
         be either"
    );
    assert_eq!(
        enclosing_function(&planted_lines, 1),
        "an_unmigrated_helper",
        "the plant was not located by its enclosing function, so a failure message would not say \
         where to look"
    );

    // --- CONTROL THREE: PROSE IS NOT A SIGNATURE. The phrase appears in comments explaining this
    // --- very seam, and a text match counts those -- which is how a by-hand count of a sibling
    // --- file reported nine violations where there were none.
    assert!(
        !names_the_series_in_a_signature(
            "    /// Was `BTreeMap<u64, ElementEntry>` before the seam."
        ),
        "a prose mention was counted as a signature"
    );
    assert!(
        !names_the_series_in_a_signature(
            "    // target: &mut HashMap<String, BTreeMap<u64, ElementEntry>>,"
        ),
        "a commented-out signature was counted as a live one"
    );
}
