// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};
use syn::visit::{self, Visit};
use syn::{Block, Expr, ExprCall, Item, ItemFn, Pat, Stmt};

fn call_name(call: &ExprCall) -> Option<String> {
    match call.func.as_ref() {
        Expr::Path(path) => path.path.segments.last().map(|part| part.ident.to_string()),
        _ => None,
    }
}

fn named(expr: &Expr, name: &str) -> bool {
    matches!(expr, Expr::Path(path) if path.path.is_ident(name))
}

fn is_true(expr: &Expr) -> bool {
    matches!(expr, Expr::Lit(value) if matches!(&value.lit, syn::Lit::Bool(value) if value.value))
}

fn reports_removal(block: &Block) -> bool {
    block.stmts.iter().any(|statement| match statement {
        Stmt::Expr(Expr::Assign(assign), _) => named(&assign.left, "mutated") && is_true(&assign.right),
        Stmt::Expr(Expr::Return(ret), _) => ret.expr.as_deref().is_some_and(|value| {
            matches!(value, Expr::Struct(value) if value.fields.iter().any(|field| {
                matches!(&field.member, syn::Member::Named(name) if name == "mutated") && is_true(&field.expr)
            }))
        }),
        _ => false,
    })
}

#[derive(Default)]
struct MutationUses {
    function: String,
    accounted: bool,
    sites: usize,
    discarded: Vec<String>,
    raw_checks: Vec<String>,
}

impl<'ast> Visit<'ast> for MutationUses {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        let previous = std::mem::replace(&mut self.function, function.sig.ident.to_string());
        visit::visit_item_fn(self, function);
        self.function = previous;
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        if matches!(&local.pat, Pat::Ident(binding) if binding.ident == "mutated") {
            if let Some(init) = &local.init {
                let previous = std::mem::replace(&mut self.accounted, true);
                self.visit_expr(&init.expr);
                self.accounted = previous;
                if let Some((_, diverge)) = &init.diverge {
                    self.visit_expr(diverge);
                }
                return;
            }
        }
        visit::visit_local(self, local);
    }

    fn visit_expr(&mut self, expression: &'ast Expr) {
        match expression {
            Expr::Binary(binary)
                if matches!(binary.op, syn::BinOp::BitOrAssign(_))
                    && named(&binary.left, "mutated") =>
            {
                let previous = std::mem::replace(&mut self.accounted, true);
                self.visit_expr(&binary.right);
                self.accounted = previous;
            }
            Expr::If(branch) if reports_removal(&branch.then_branch) => {
                let previous = std::mem::replace(&mut self.accounted, true);
                self.visit_expr(&branch.cond);
                self.accounted = previous;
                self.visit_block(&branch.then_branch);
                if let Some((_, otherwise)) = &branch.else_branch {
                    self.visit_expr(otherwise);
                }
            }
            _ => visit::visit_expr(self, expression),
        }
    }

    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        match call_name(call).as_deref() {
            Some("drop_if_expired") => {
                self.sites += 1;
                if !self.accounted {
                    self.discarded.push(self.function.clone());
                }
            }
            Some("remove_if_expired" | "remove_if_expired_with")
                if self.function != "drop_if_expired" =>
            {
                self.raw_checks.push(self.function.clone());
            }
            _ => {}
        }
        visit::visit_expr_call(self, call);
    }
}

fn mutation_uses(source: &str) -> MutationUses {
    let file = syn::parse_file(source).expect("valid executor source");
    let mut uses = MutationUses::default();
    uses.visit_file(&file);
    uses
}

pub(super) fn assert_mutation_reports(source: &str) {
    let uses = mutation_uses(source);
    assert!(
        uses.sites > 20,
        "no meaningful command expiry coverage: {}",
        uses.sites
    );
    assert!(
        uses.discarded.is_empty(),
        "expiry removal is not reported in: {:?}",
        uses.discarded
    );
    assert!(
        uses.raw_checks.is_empty(),
        "command expiry bypasses cache retirement in: {:?}",
        uses.raw_checks
    );
    let planted = source.replacen("mutated |= drop_if_expired(", "let _ = drop_if_expired(", 1);
    assert_ne!(source, planted, "mutation control did not change source");
    let planted = mutation_uses(&planted);
    assert_eq!(uses.sites, planted.sites);
    assert_eq!(
        planted.discarded.len(),
        1,
        "discarded-result control must be detected"
    );
}

fn pattern_commands(pattern: &Pat) -> Vec<String> {
    let path = match pattern {
        Pat::Struct(value) => Some(&value.path),
        Pat::TupleStruct(value) => Some(&value.path),
        Pat::Path(value) => Some(&value.path),
        Pat::Or(value) => return value.cases.iter().flat_map(pattern_commands).collect(),
        Pat::Paren(value) => return pattern_commands(&value.pat),
        Pat::Ident(value) => {
            return value
                .subpat
                .as_ref()
                .map(|(_, pattern)| pattern_commands(pattern))
                .unwrap_or_default()
        }
        _ => None,
    };
    path.and_then(|path| {
        let mut parts = path.segments.iter().rev();
        let variant = parts.next()?;
        (parts.next()?.ident == "Command").then(|| vec![variant.ident.to_string()])
    })
    .unwrap_or_default()
}

#[derive(Default)]
struct CommandArms<'ast> {
    arms: Vec<(Vec<String>, &'ast Expr)>,
}

impl<'ast> Visit<'ast> for CommandArms<'ast> {
    fn visit_arm(&mut self, arm: &'ast syn::Arm) {
        let names = pattern_commands(&arm.pat);
        if !names.is_empty() {
            self.arms.push((names, &arm.body));
        } else {
            visit::visit_arm(self, arm);
        }
    }
}

#[derive(Default)]
struct Calls {
    names: Vec<String>,
}

impl<'ast> Visit<'ast> for Calls {
    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        if let Some(name) = call_name(call) {
            self.names.push(name);
        }
        visit::visit_expr_call(self, call);
    }
}

fn guarded(
    expression: &Expr,
    functions: &HashMap<String, &ItemFn>,
    visiting: &mut HashSet<String>,
) -> bool {
    let mut calls = Calls::default();
    calls.visit_expr(expression);
    calls.names.iter().any(|name| {
        if name == "drop_if_expired" {
            return true;
        }
        if name == "execute_on_shard" || !visiting.insert(name.clone()) {
            return false;
        }
        let found = functions.get(name).is_some_and(|function| {
            let body = Expr::Block(syn::ExprBlock {
                attrs: vec![],
                label: None,
                block: (*function.block).clone(),
            });
            guarded(&body, functions, visiting)
        });
        visiting.remove(name);
        found
    })
}

pub(super) fn assert_command_deadlines(source: &str) {
    const EXEMPT: [(&str, &str); 7] = [
        ("LeaderEstablish", "touches no object"),
        (
            "ContextResourceBlobBegin",
            "blob dispatch is outside shard record state",
        ),
        ("CommonDelete", "unconditionally removes the record"),
        (
            "ContextMarkSummaryDirty",
            "only updates ephemeral dirty-index state",
        ),
        (
            "ContextQuerySummaryDirty",
            "only reads ephemeral dirty-index state",
        ),
        (
            "ContextMarkEmbeddingDirty",
            "only updates ephemeral dirty-index state",
        ),
        (
            "ContextQueryEmbeddingDirty",
            "only reads ephemeral dirty-index state",
        ),
    ];
    let file = syn::parse_file(source).expect("valid executor source");
    let functions: HashMap<_, _> = file
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Fn(function) => Some((function.sig.ident.to_string(), function)),
            _ => None,
        })
        .collect();
    let executor = functions
        .get("execute_on_shard")
        .expect("executor function");
    let mut found = CommandArms::default();
    found.visit_block(&executor.block);
    assert!(found.arms.len() > 80, "command arm scan lost its subject");
    let guarded: Vec<_> = found
        .arms
        .iter()
        .map(|(_, body)| guarded(body, &functions, &mut HashSet::new()))
        .collect();
    assert!(
        guarded.iter().filter(|value| **value).count() > 60,
        "expiry guard scan lost its subject"
    );
    let covered: HashSet<_> = found
        .arms
        .iter()
        .zip(&guarded)
        .filter(|(_, covered)| **covered)
        .flat_map(|((names, _), _)| names.iter().cloned())
        .collect();
    assert!(covered.contains("StringGet"));
    assert!(!covered.contains("LeaderEstablish"));
    for (name, _) in EXEMPT {
        assert!(
            found
                .arms
                .iter()
                .any(|(names, _)| names.iter().any(|held| held == name)),
            "stale exemption: {name}"
        );
    }
    let mut unguarded = 0;
    let mut surprises = Vec::new();
    for ((names, body), guarded) in found.arms.iter().zip(guarded) {
        if guarded {
            continue;
        }
        // An unreachable fallback is covered only if another real arm dispatches the same
        // variant through a checked helper. Removing that early dispatch must fail this test.
        if matches!(body, Expr::Macro(value) if value.mac.path.is_ident("unreachable"))
            && names.iter().all(|name| covered.contains(name))
        {
            continue;
        }
        unguarded += 1;
        if !EXEMPT
            .iter()
            .any(|(name, _)| names.iter().any(|held| held == name))
        {
            surprises.push(names.join(" | "));
        }
    }
    assert!(
        surprises.is_empty(),
        "command arms have no expiry check: {surprises:?}"
    );
    assert_eq!(
        unguarded,
        EXEMPT.len(),
        "every unguarded arm needs one reason"
    );
}

#[test]
fn syntax_scan_ignores_comment_and_constructor_decoys() {
    let source = r#"
        fn example() {
            // mutated |= drop_if_expired(cache, shard_id, shard, key);
            let text = "drop_if_expired(cache, shard_id, shard, key)";
            let command = Command::HashGet { key, field };
        }
    "#;
    let uses = mutation_uses(source);
    assert_eq!(uses.sites, 0);
    let file = syn::parse_file(source).unwrap();
    let mut arms = CommandArms::default();
    arms.visit_file(&file);
    assert!(arms.arms.is_empty());
}

#[test]
fn syntax_scan_distinguishes_mutation_reports_and_discarded_checks() {
    let source = r#"
        fn example() {
            let mut mutated = drop_if_expired(cache, shard_id, shard, key);
            mutated |= drop_if_expired(cache, shard_id, shard, key);
            if drop_if_expired(cache, shard_id, shard, key) { mutated = true; }
            if drop_if_expired(cache, shard_id, shard, key) {
                return ExecuteOutcome { mutated: true, response };
            }
            let _ = drop_if_expired(cache, shard_id, shard, key);
            if drop_if_expired(cache, shard_id, shard, key) { return; }
            remove_if_expired(shard, key);
        }
    "#;
    let uses = mutation_uses(source);
    assert_eq!(uses.sites, 6);
    assert_eq!(uses.discarded.len(), 2);
    assert_eq!(uses.raw_checks, vec!["example"]);
}

#[test]
fn delegation_scan_requires_a_real_cache_aware_check() {
    let source = r#"
        fn first() { second(); }
        fn second() { drop_if_expired(cache, shard_id, shard, key); }
        fn cycle() { cycle(); }
        fn raw() { remove_if_expired(shard, key); }
    "#;
    let file = syn::parse_file(source).unwrap();
    let functions = file
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Fn(function) => Some((function.sig.ident.to_string(), function)),
            _ => None,
        })
        .collect();
    for (call, expected) in [
        ("first()", true),
        ("missing()", false),
        ("cycle()", false),
        ("raw()", false),
    ] {
        let expression: Expr = syn::parse_str(call).unwrap();
        assert_eq!(
            guarded(&expression, &functions, &mut HashSet::new()),
            expected,
            "{call}"
        );
    }
}

#[test]
fn syntax_scan_keeps_bound_command_patterns_as_one_dispatch_arm() {
    let source = r#"
        fn example(command: Command) {
            match command {
                held @ (Command::FeatureAppend { .. } | Command::FeatureAppendWithTtl { .. }) => {
                    let key = match held {
                        Command::FeatureAppend { key, .. } => key,
                        Command::FeatureAppendWithTtl { key, .. } => key,
                        _ => unreachable!(),
                    };
                    drop_if_expired(cache, shard_id, shard, key);
                }
            }
        }
    "#;
    let file = syn::parse_file(source).unwrap();
    let mut found = CommandArms::default();
    found.visit_file(&file);
    assert_eq!(found.arms.len(), 1);
    assert_eq!(
        found.arms[0].0,
        vec!["FeatureAppend", "FeatureAppendWithTtl"]
    );
}
