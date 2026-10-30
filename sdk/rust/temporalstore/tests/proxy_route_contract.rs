// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Every route this SDK posts to must be one the proxy answers.
//!
//! #1040 found eleven routes here that the dispatcher replies 404 to, and fixed them. Three of
//! those -- ControlStateCPCSet, ControlStateCPCQuery and ControlStateHquery -- had no served
//! equivalent, so they were left off this SDK rather than redirected; whether to add the routes
//! to the proxy or leave the methods off is a product decision that is still open.
//!
//! Methods calling all three were sitting in `src/proxy_control_state_cpc.rs` and
//! `src/proxy_control_state_hash.rs` the whole time. They were invisible because nothing compiled
//! those files, and restoring the module split would have shipped them. This test is what makes
//! the decision durable: the route list below is a claim that each entry is served, and anything
//! added to the SDK fails here until someone states which it is.
//!
//! The dispatcher's own arms are in `crates/temporalstore-rust/src/proxy/proxy_handle.rs`. This crate is
//! published standalone and cannot depend on it, so the list is checked by hand against those arms
//! and pinned here; it is not derived at run time.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

/// Routes the proxy serves and this SDK is allowed to call.
///
/// Adding a line here asserts that `crates/temporalstore-rust/src/proxy/proxy_handle.rs` matches it for
/// POST. If it does not, the call can only ever fail at run time.
const SERVED_ROUTES: &[&str] = &[
    "/ProxyService/BatchExecuteCmd",
    "/ProxyService/ControlStateCount",
    "/ProxyService/ControlStateFolQuery",
    "/ProxyService/ControlStateFolSet",
    "/ProxyService/ControlStateHset",
    "/ProxyService/ControlStateIncrement",
    "/ProxyService/ControlStateManager",
    "/ProxyService/Delete",
    "/ProxyService/ExecuteCmd",
    "/ProxyService/Exists",
    "/ProxyService/Expire",
    "/ProxyService/FeatureAdd",
    "/ProxyService/FeatureAggQuery",
    "/ProxyService/FeatureDelete",
    "/ProxyService/FeatureQuery",
    "/ProxyService/FeatureReplace",
    "/ProxyService/Get",
    "/ProxyService/GetConfig",
    "/ProxyService/HDel",
    "/ProxyService/HGet",
    "/ProxyService/HGetAll",
    "/ProxyService/HLen",
    "/ProxyService/HMGet",
    "/ProxyService/HMSet",
    "/ProxyService/HSet",
    "/ProxyService/OpenTable",
    "/ProxyService/RefreshTopology",
    "/ProxyService/SAdd",
    "/ProxyService/SMembers",
    "/ProxyService/SRem",
    "/ProxyService/SequenceAdd",
    "/ProxyService/SequenceQuery",
    "/ProxyService/Set",
    "/ProxyService/SetEx",
    "/ProxyService/TableBatchExecuteCmd",
    "/ProxyService/TableExecuteCmd",
    "/ProxyService/Ttl",
    "/ProxyService/UpdateConfig",
];

/// Routes the dispatcher has no arm for. Named rather than merely absent, so that re-adding a
/// method for one of them fails with the reason instead of with a diff.
const UNSERVED_ROUTES: &[&str] = &[
    "/ProxyService/ControlStateCPCQuery",
    "/ProxyService/ControlStateCPCSet",
    "/ProxyService/ControlStateHquery",
];

/// Route literals the SDK's own sources contain.
fn routes_called() -> BTreeSet<String> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = BTreeSet::new();
    let mut dirs = vec![src];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).expect("read src/") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                dirs.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let text = fs::read_to_string(&path).expect("read source");
            for line in text.lines() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                let mut rest = line;
                while let Some(open) = rest.find("\"/") {
                    rest = &rest[open + 1..];
                    let Some(close) = rest.find('"') else { break };
                    let literal = &rest[..close];
                    rest = &rest[close + 1..];
                    if literal.starts_with("/ProxyService/") || literal.starts_with("/v1/") {
                        found.insert(literal.to_string());
                    }
                }
            }
        }
    }
    found
}

#[test]
fn proxy_route_contract() {
    let called = routes_called();

    // Premise: the scan reads the sources. An empty result would otherwise satisfy every
    // assertion below while checking nothing -- which is exactly how 88 files went unnoticed.
    assert!(
        called.len() > 30,
        "premise: the SDK calls the proxy; the scan found {} route(s)",
        called.len()
    );

    let served: BTreeSet<String> = SERVED_ROUTES.iter().map(|r| r.to_string()).collect();
    assert_eq!(
        served.len(),
        SERVED_ROUTES.len(),
        "the served list has a duplicate"
    );

    // Each unserved route, by name, so the failure says why.
    for route in UNSERVED_ROUTES {
        assert!(
            !called.contains(*route),
            "the SDK calls {route}, which the dispatcher answers 404 for. Either add the arm in \
             crates/temporalstore-rust/src/proxy/proxy_handle.rs and move the route into SERVED_ROUTES, \
             or leave the method off as #1040 did."
        );
    }

    // #1040's other half: the `/v1/*` shapes the proxy never served.
    let v1: Vec<&String> = called.iter().filter(|r| r.starts_with("/v1/")).collect();
    assert!(
        v1.is_empty(),
        "the SDK posts to {v1:?}; the proxy serves /ProxyService/* only"
    );

    let unexpected: Vec<&String> = called.difference(&served).collect();
    assert!(
        unexpected.is_empty(),
        "the SDK calls {} route(s) that are not declared served:\n{}\nAdd each to SERVED_ROUTES \
         only after checking the dispatcher has a POST arm for it.",
        unexpected.len(),
        unexpected
            .iter()
            .map(|r| format!("  {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // A route that leaves the SDK should leave the list with it, so the list cannot drift into a
    // record of routes nobody calls.
    let stale: Vec<&String> = served.difference(&called).collect();
    assert!(
        stale.is_empty(),
        "SERVED_ROUTES names {} route(s) the SDK no longer calls:\n{}\nRemove them, so this list \
         stays a statement about this SDK.",
        stale.len(),
        stale
            .iter()
            .map(|r| format!("  {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
