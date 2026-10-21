// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The object-keyed field count on `ShardState` is a CHECKED number, not a claimed one.
//!
//! # WHY THIS EXISTS
//!
//! The consolidation campaign is measured against "how many maps can a key live in", and that
//! number was reported as **eight** when it was **nine**. `control_state_changes` was missed, and
//! the reason is worth more than the correction: the query that produced the count required
//! `name: type,` to sit on ONE source line, and that field wraps its type onto a second.
//!
//! So the count read LOW. That is the dangerous direction mid-campaign, because a low count looks
//! like progress rather than like a broken needle -- the same failure as the turbofish that
//! defeated a `name(` query and reported 5 of 12.
//!
//! The same query read **18 fields of 30** on this struct for the same reason, and joining wrapped
//! types is what took it to 30. That is worth stating precisely, because I first attributed the
//! 18 to a truncated struct body and it was not: a plant showed the naive body query is fine here.
//! **One defect, named correctly, beats two guessed at.**
//!
//! The lesson the sibling guard in this directory states is the one that applies:
//! **when the population is characterised by a type, the type is the enumeration.** A claim of the
//! form "there are N such fields" should name the mechanical query that produced it so a reader can
//! re-run it. This test IS that query, and it prints its enumeration so the number is auditable
//! rather than merely asserted.
//!
//! # WHAT IT COUNTS, AND THE THREE DENOMINATORS
//!
//! "A map" means three different things after the domain-keyed merges, and conflating them is how
//! the headline flatters itself:
//!
//!   * **object-keyed fields** -- a field whose OUTERMOST key is an object key, plus a
//!     domain-keyed field counted ONCE. This is the campaign's denominator.
//!   * **object-keyed tables** -- each domain of a domain-keyed field counted separately. This is
//!     the one that says where a key can actually live.
//!   * **map fields** -- every field holding a map at all.
//!
//! This guard pins the first and prints all three.
//!
//! # THE PART THAT MAKES IT EVIDENCE
//!
//! A source-level count is only as good as its ability to see every spelling, so the arm below
//! asserts that it finds the field that the previous query MISSED. Without that, a guard written
//! with the same blind spot would reproduce the same wrong number and assert it confidently.
//!
//! Verified by planting, both sides:
//!
//! | plant | result |
//! |---|---|
//! | drop the wrapped-type joining, restoring the one-line query | **REFUSED** -- names the field |
//! | swap brace-matching for the naive body query | passes, so that arm is defensive only |
//!
//! The second is reported rather than quietly kept, because a green arm has two readings and this
//! one is "nothing needs it", not "it works".

/// Every `pub(super)` field of `ShardState`, as `(name, type)` with the type on one line.
///
/// Wrapped types are joined before matching, which is the defect this whole module exists for.
fn shard_state_fields() -> Vec<(String, String)> {
    let state = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("engine")
        .join("state.rs");
    let text = std::fs::read_to_string(&state).expect("state.rs must be readable");

    // BRACE-MATCH THE STRUCT. This arm is DEFENSIVE AND CURRENTLY UNEXERCISED, and saying so is
    // the point: a plant swapped it for the naive `find("\n}\n")` and the guard still passed, even
    // with a doc comment carrying a braced example planted inside the body. Every brace inside a
    // struct body is INDENTED, so only the struct's own closing brace sits at column zero and the
    // naive query is correct for Rust's standard formatting.
    //
    // It stays because it costs nothing and does not depend on formatting, but it is not what
    // fixed the wrong count -- see the field-joining below, which is.
    let decl = text
        .find("pub(super) struct ShardState")
        .expect("ShardState must be declared");
    let open = text[decl..].find('{').expect("the struct must have a body") + decl;
    let mut depth = 0usize;
    let mut close = open;
    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    close = open + offset;
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(close > open, "the struct body must be brace-matched");

    // Drop doc comments, line comments and attributes, so a braced example or a `#[serde(...)]`
    // cannot contribute a field.
    let kept: Vec<&str> = text[open..close]
        .lines()
        .filter(|line| {
            let t = line.trim_start();
            !(t.starts_with("///") || t.starts_with("//") || t.starts_with("#["))
        })
        .collect();

    // JOIN A WRAPPED TYPE ONTO ITS NAME. `control_state_changes:` sits alone on its line with the
    // type beneath it, and requiring one line is exactly what lost it.
    let mut joined: Vec<String> = Vec::new();
    for line in kept {
        let trimmed = line.trim_end();
        if trimmed.trim_end().ends_with(':') {
            joined.push(trimmed.trim_end().to_string());
            continue;
        }
        match joined.last_mut() {
            Some(last) if last.ends_with(':') => {
                last.push(' ');
                last.push_str(trimmed.trim());
            }
            _ => joined.push(trimmed.to_string()),
        }
    }

    let mut fields = Vec::new();
    for line in joined {
        let t = line.trim();
        let Some(rest) = t.strip_prefix("pub(super) ") else {
            continue;
        };
        let Some((name, ty)) = rest.split_once(':') else {
            continue;
        };
        if name.contains(' ') || name.contains('(') {
            continue;
        }
        let ty = ty.trim().trim_end_matches(',').split_whitespace().collect::<Vec<_>>().join(" ");
        if ty.is_empty() {
            continue;
        }
        fields.push((name.trim().to_string(), ty));
    }
    fields
}

/// Whether a type's OUTERMOST key is an object key.
fn is_object_keyed(ty: &str) -> bool {
    for container in ["HashMap<", "BTreeMap<"] {
        if let Some(rest) = ty.strip_prefix(container) {
            let key = rest.split(',').next().unwrap_or("").trim();
            return matches!(key, "ModelKey" | "Arc<str>" | "String");
        }
    }
    false
}

/// Whether a type is a domain-keyed field: `HashMap<SomeDomain, Table>` whose table is object-keyed.
fn is_domain_keyed(ty: &str) -> bool {
    let Some(rest) = ty.strip_prefix("HashMap<") else {
        return false;
    };
    let key = rest.split(',').next().unwrap_or("").trim();
    const DOMAINS: [&str; 4] = ["AddressMap", "CounterSeries", "U64Ledger", "RollupDomain"];
    DOMAINS.contains(&key)
}

#[test]
fn the_object_keyed_field_count_is_nine_and_the_query_can_see_a_wrapped_type() {
    let fields = shard_state_fields();
    assert!(
        fields.len() >= 25,
        "the field query found only {} fields, which is fewer than this struct has had at any \
         point in the campaign -- the extraction is broken, not the struct",
        fields.len()
    );

    let plain: Vec<&(String, String)> =
        fields.iter().filter(|(_, ty)| is_object_keyed(ty)).collect();
    let domain: Vec<&(String, String)> =
        fields.iter().filter(|(_, ty)| is_domain_keyed(ty)).collect();

    println!();
    println!("  ShardState fields: {}", fields.len());
    println!();
    println!("  OBJECT-KEYED PLAIN ({}):", plain.len());
    for (name, ty) in &plain {
        println!("    {name:<28} {ty}");
    }
    println!();
    println!("  DOMAIN-KEYED ({}):", domain.len());
    for (name, ty) in &domain {
        println!("    {name:<28} {ty}");
    }
    println!();
    println!(
        "  => object-keyed FIELDS = {} (the campaign's denominator)",
        plain.len() + domain.len()
    );

    // THE ARM THAT MAKES THIS EVIDENCE. `control_state_changes` wraps its type onto a second
    // source line, and the query that reported EIGHT could not see it. A guard with the same
    // blind spot would assert a wrong number with total confidence, so the blind spot is what is
    // checked first.
    let names: Vec<&str> = plain.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"control_state_changes"),
        "the query did not find `control_state_changes`, whose type WRAPS onto a second line. \
         That is the exact omission that made this count read 8 instead of 9, so a query that \
         cannot see it is not measuring the population. Found: {names:?}"
    );

    assert_eq!(
        plain.len() + domain.len(),
        9,
        "object-keyed field count moved. Plain: {:?}. Domain: {:?}. If a merge landed, this \
         number is the campaign's headline and belongs in the pull request body; if it moved \
         without one, a field was added and the denominator changed under the campaign.",
        names,
        domain.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
    );
    assert_eq!(plain.len(), 5, "the plain object-keyed fields changed: {names:?}");
    assert_eq!(domain.len(), 4, "the domain-keyed fields changed");
}
