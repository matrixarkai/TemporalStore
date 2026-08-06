# Python Hook / MCP Test Suite — Known Failures & Root-Cause Triage

Last updated: 2026-08-05

Scope: the Python `unittest` suites under `tools/test_matrixark_*` that exercise the
agent hooks and the MCP local pipeline (ingest → extract → retrieve → commit) against
the Rust proxy (`temporalstore-rust`, `no-metaserver` local mode). This is a **separate
suite** from the Rust `cargo test` one — see
[`rust_test_suite_known_failures.md`](rust_test_suite_known_failures.md).

Reproduce (from repo root), pointing at a built proxy:

```bash
cargo build -p temporalstore-rust --bin matrixark_rust_proxy
cd tools
MATRIXARK_TEST_RUST_PROXY=$PWD/../target/debug/matrixark_rust_proxy \
  python3 -m unittest test_matrixark_popular_agent_hooks
```

## Fixed (restored the shared ingest path)

These were real regressions from the in-flight "split helpers out of core" refactor and
are now on `main`:

- **`matrixark_mcp_core_compact`: missing `non_default_classification` import**
  (`35029984`). The compact/context-record split moved `materialize_serving_records` but
  not `non_default_classification`, so **every** `matrixark_ingest` raised `NameError`,
  surfaced as a generic "internal MatrixArk MCP server error". This alone broke all
  subprocess hook tests.
- **`matrixark_mcp_local_adapter`: missing `_mcp_debug_log` import** (`d6a7b5fd`). The
  adapter uses `from matrixark_mcp_core import *`, which skips underscore-prefixed names,
  so the background resource-import failure handler crashed the worker thread with
  `NameError` while trying to report a failure.
- **Two stale hook-test expectations** (`35029984`): `compacts_assistant_response_memory`
  (feature-focused assistant memory now prefers the profile policy and budgets assistant
  profile facts separately) and `post_tool_use_extracts_selected_tool_evidence` (the
  retrieved record is the promoted `tool_evidence` *entity*, budgeted by
  entity_type/memory_scope/session_continuity; the raw event `source_role`/`hook_type`
  are asserted at extraction, not on the entity).

With these, `test_matrixark_popular_agent_hooks` is **23/23 in a clean, isolated run**.

## Measurement caveats (read before trusting counts)

Combined multi-file runs on the dev distro are **not a reliable baseline**:

1. **Rapid upstream churn.** `main` is mid-campaign — nearly every commit is a
   `refactor: split X into mixins/helpers`. A green state does not survive: this suite
   was 23/23 at `35029984` and 1F+3E at `d6a7b5fd` (6 commits later) with no relevant
   code change. Pin a single commit before measuring.
2. **Cross-test contamination.** Running many files together shares `/tmp` stores, leaves
   orphaned `matrixark_rust_proxy` subprocesses, and keeps summary-refresher background
   threads alive, so the *same commit* yields different results across runs. Run one file
   at a time; kill stray `matrixark_rust_proxy`/`matrixark_agent_hook` processes and clear
   `/tmp/temporalstore*` between runs.
3. **Shell-var quirk.** The dev WSL login shell blanks `$$`/loop variables in
   `bash -lc`, corrupting scripted per-file measurement. Use literal paths.

So the ~140/529 "combined-run" failure count is inflated by (1)–(2). The clusters below
are categorized by **root cause**, which is stable, rather than by exact count.

## Remaining failures by root-cause cluster

### 1. Serving-embedding lineage is now debug-only (largest cluster; INTENDED)

Files: `test_matrixark_codex_hook_pipeline` (majority), some `test_matrixark_codex_hook_output`.

Symptom: `KeyError` on `source_memory_scopes`, `source_session_continuities`,
`source_extraction_phases`, `source_role_counts`, `source_session_ids`, `source_roles`
when asserting on **serving/hot embedding** records.

Root cause: a deliberate series of commits moved embedding lineage out of the compact
serving rows — `Keep embedding lineage debug-only`, `Keep embedding lineage out of
serving records`, `Compact context embedding serving rows`, `Compact embedding lineage in
memory storage`, `Strip dirty hash from serving embeddings`. Hot serving embeddings are
now sparse; lineage is retained only when context-debug records are enabled.

Owner decision (do **not** blind-edit to green): either these tests should assert the new
compact shape (lineage absent on serving rows), or they should enable context-debug
records and assert lineage on the debug variant. Editing them to accept whatever comes
out risks masking an unintended lineage drop.

### 2. `mcp_backend_policy` mixin-split churn (in flight)

File: `test_matrixark_mcp_backend_policy` (large count). Being actively refactored upstream
(`refactor(tests): split MatrixArkMcpBackendPolicyTest into 4 mixins`).

Symptoms: `--rust-direct-lib` not found in a generated server script (flag/entrypoint
moved by the MCP entrypoint split); `KeyError` on renamed keys (`profile_entity`,
`selected_refs`, `recovery_status`); assorted `assertIn`/count assertions on
server-script and backend-policy shapes that the split changed.

These should be reconciled by the refactor that is splitting the file — not patched
piecemeal against a moving target.

### 3. `codex_hook_output` / `plugin_session_resolver` churn

Files: `test_matrixark_codex_hook_output`, `test_matrixark_codex_plugin_session_resolver`.
Same class as (2): output-shape and session-resolution assertions that the ongoing
"split into mixins" reorganization has shifted. Reconcile with the owning refactor.

## Recommendation

The core ingest/extract/retrieve path is healthy (cluster of NameError regressions fixed;
`popular_agent_hooks` green in isolation). The remaining red is dominated by (a) an
**intended** serving-embedding compaction that left lineage-expecting tests stale, and (b)
an **in-flight** mixin-split reorganization of the very test files. Both are owner/refactor
work, not bug fixes. Do not edit these test expectations to force green until the split
campaign settles, because that would mask the intended-vs-regression boundary on the
lineage change. Re-measure per-file, at a pinned commit, in a clean process/`/tmp` state.
