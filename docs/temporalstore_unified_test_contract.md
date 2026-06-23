# TemporalStore Unified Test Contract

## Goal

All C++, Rust, and MatrixArk context parity tests should use one input contract,
one command API, and one output report format.

The shared input is:

```bash
sdk/unified/temporalstore_unified_corpus.json
```

The unified runner is:

```bash
python3 tools/run_unified_parity_tests.py \
  --corpus sdk/unified/temporalstore_unified_corpus.json \
  --result-dir /tmp/temporalstore-unified-parity
```

The compatibility wrapper remains:

```bash
bash tools/run_rust_unified_tests.sh
```

## Input API

The corpus is a JSON object:

```json
{
  "name": "temporalstore-cpp-rust-context-parity",
  "schema_version": 1,
  "coverage": {
    "required_case_names": [],
    "required_command_kinds": [],
    "required_raft_case_names": [],
    "required_response_kinds": []
  },
  "cases": [
    {
      "name": "context_tree_event_pack_replay",
      "shard_id": 1,
      "steps": [
        {
          "name": "write_event",
          "command": {
            "kind": "context_write_event",
            "record": {}
          }
        }
      ]
    }
  ]
}
```

Rules:

- `command.kind` is the API surface under test.
- Every new C++ context command must be represented by at least one corpus step.
- Every product-level flow must be represented by a named case.
- `coverage.required_command_kinds` and `coverage.required_case_names` are gates, not comments.

## Output API

The unified runner always writes:

```bash
/result-dir/unified_parity_report.json
/result-dir/unified_parity_report.md
```

The JSON report shape is:

```json
{
  "status": "passed",
  "input": {
    "corpus": "...",
    "schema_version": 1,
    "case_count": 11,
    "command_kind_count": 33,
    "context_step_count": 43,
    "command_kinds": []
  },
  "stages": [
    {
      "name": "python_schema",
      "status": "passed",
      "command": [],
      "cwd": "...",
      "duration_s": 0.1,
      "returncode": 0,
      "stdout_tail": "...",
      "stderr_tail": "..."
    }
  ],
  "artifacts": {
    "json": ".../unified_parity_report.json",
    "markdown": ".../unified_parity_report.md"
  }
}
```

## Stage API

The unified runner executes the same corpus through three stages:

1. `python_schema`
   - validates corpus schema and command-specific input/output fields;
   - command: `tools/run_temporalstore_unified_tests.py --validate-only`.

2. `cpp_context_contract`
   - validates C++ context behavior against the corpus;
   - command: `tools/run_cpp_unified_context_contract.sh`.

3. `rust_unified_corpus`
   - validates Rust SDK parity awareness for the same cases and command kinds;
   - command: `cargo test --no-default-features --features proxy --test unified_corpus`.

## Development Workflow

When adding a context feature:

1. Add the C++ implementation or contract behavior.
2. Add a corpus step under `sdk/unified/temporalstore_unified_corpus.json`.
3. Add the command kind to `coverage.required_command_kinds`.
4. Add or update a named case if the feature is a flow, not just a field.
5. Run:

```bash
bash tools/run_rust_unified_tests.sh
```

6. Inspect `/tmp/temporalstore-unified-parity/unified_parity_report.md`.

Do not claim C++/Rust parity unless the unified runner reports `passed`.
