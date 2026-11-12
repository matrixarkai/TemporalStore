# Exact-Revision Mem0 Runtime

The `Mem0 runtime bundle` workflow builds `server`, `proxy`, `metaserver` and
`mem0_scale_harness` from a clean public revision on Ubuntu 22.04. Run the
resulting binaries locally; this workflow does not deploy services or run a
corpus on an external host.

```bash
gh workflow run mem0-runtime-build.yml --repo matrixarkai/TemporalStore \
  --ref main -f revision=PUBLIC_COMMIT_SHA
gh run list --repo matrixarkai/TemporalStore --workflow mem0-runtime-build.yml
runtime_root="${HOME}/src/github-services/build/mem0-runtime"
mkdir -p "$runtime_root/download" "$runtime_root/runtime"
test -z "$(ls -A "$runtime_root/runtime")"
gh run download RUN_ID --repo matrixarkai/TemporalStore \
  --name mem0-runtime-PUBLIC_COMMIT_SHA --dir "$runtime_root/download"
tar -xzf "$runtime_root/download/mem0-runtime-PUBLIC_COMMIT_SHA.tar.gz" -C "$runtime_root/runtime"
(cd "$runtime_root/runtime" && sha256sum --check SHA256SUMS)
python3 - "$runtime_root/runtime/build.json" scripts/mem0_scale_local.sh <<'PY'
import hashlib, json, pathlib, sys
manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert hashlib.sha256(pathlib.Path(sys.argv[2]).read_bytes()).hexdigest() == manifest["runner_script_sha256"]
PY
bash scripts/mem0_scale_local.sh --skip-build --bin-dir "$runtime_root/runtime/bin" \
  --work-dir "$runtime_root/run" --port-base 19600 \
  --records 16384 --batch-size 128 --users 1 --tenant-shards 1 \
  --searches 256 --payload-bytes 512 --infer false --metadata-updates 1600
```

Use an exact commit SHA, not a moving branch name, for a reproducible run. Check
`build.json` before running: it records the commit, source tree, compiler,
dependency resolution, lockfile hash, runner hash and binary hashes without
checkout paths.
The bundle also carries the lockfile, license and notices. Build caches are
separate from other Linux images so native libraries are not mixed across
different system-library versions.

The local runner still comes from the checkout where the command is executed;
the hash check above must pass. The runner's `source.json` describes that local
checkout, not the bundle's source revision. Use `build.json` and its binary
hashes for bundle identity. The runner's data directory must not already exist
and its ports must be unused. Data is removed unless retention was explicitly
requested. Retain only the active bundle and one previous baseline; never remove
a bundle while its processes are running. Avoid duplicate checkouts.

The example disables extraction and uses the local deployment's provider
configuration. Supply the existing model-backed provider configuration and
enable extraction separately for an Ollama experiment. A built bundle proves
neither model quality nor API performance. Read the completed report's failure
list, request counts, latency distribution, CPU and sampled RSS. Failed requests
are not successful requests that merely exceeded a timing threshold.
