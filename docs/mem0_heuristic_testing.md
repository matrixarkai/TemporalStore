# Model-Free Mem0 Tests

Use the TemporalStore launcher for local storage and API testing without calling Ollama or another model endpoint:

```bash
bash scripts/mem0_scale_heuristic.sh \
  --skip-build --bin-dir "$BIN_DIR" \
  --work-dir target/mem0-heuristic \
  --records 8192 --batch-size 64 --users 100
```

`BIN_DIR` must contain validated TemporalStore `server`, `proxy`, `metaserver`, and `mem0_scale_harness` binaries. The launcher uses only TemporalStore code. It forwards the normal runner's data-directory, port, process cleanup, and reporting rules. Existing data directories are refused; use a clean isolated work directory.

The child processes receive an explicit mock provider, deterministic test embeddings, heuristic summaries, empty embedding endpoint aliases, and no live-provider fallback. The parent environment, existing services, and production provider defaults are unchanged. Provider overrides are refused; use `scripts/mem0_scale_local.sh` directly for an intentional model run.

The default is `--infer false`, which exercises explicit-memory ingestion and serving. Explicit `--infer true` can exercise the mock extraction pipeline, but it does not establish model fact-extraction quality, entity coverage, or model latency. Do not combine model-free and real-model measurements into a single performance claim. Mock embeddings use their own model identity, not a production vector space.

Run the launcher contract without starting services, compiling the workspace, or calling a model:

```bash
bash scripts/tests/mem0_scale_heuristic_test.sh
```
