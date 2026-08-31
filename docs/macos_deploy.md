# macOS Build And Deploy Manual

This guide builds and deploys Rust TemporalStore on macOS. It mirrors the Linux
native deployment and is written for a fresh user starting from a clean Mac.

If you are not sure which platform guide to use, start with
[TemporalStore Install Guide](INSTALL.md).

## What This Installs

The installer builds or installs four Rust binaries, starts local metaserver and
datanode services, creates persistent storage directories, and runs a basic
write/read smoke test. It does not require Docker, MatrixObject, S3, a Raft
cluster, or OSS model downloads for the first local install.

The recommended local macOS deployment runs:

```text
matrixark_rust_metaserver
matrixark_rust_datanode
matrixark_rust_proxy
matrixark_rust_direct_sdk
```

The metaserver and datanode are long-lived processes. Hooks, clients, and
benchmarks call the proxy or direct SDK; the Python hook should not embed
storage.

## Recommended Build OS

Build on recent macOS with Apple Silicon or Intel x86_64. The first open-source
path should use native Rust builds on macOS rather than WSL. Docker is optional
for model gateways or benchmark isolation, but not required for the native
TemporalStore service.

## Dependencies

Required:

```text
macOS 13 or newer recommended
Xcode Command Line Tools   (provides clang/libclang for the RocksDB build)
Homebrew
python3
git
rustup/rustc/cargo
cmake                      (to compile RocksDB via MatrixCache)
network access to github.com  (to fetch the MatrixCache/MatrixRaft crates)
```

Install the common prerequisites:

```bash
xcode-select --install
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install git python rustup-init cmake
rustup-init -y
source "$HOME/.cargo/env"
```

`cmake` and the Xcode Command Line Tools are required because the storage engine
depends on the `matrixcache` crate with the `rocksdb-ssd` feature, which builds
RocksDB from source (`librocksdb-sys` uses `bindgen`/libclang).

Then clone this repository and run commands from the repo root.

If you already have Rust through Homebrew or rustup, keep the existing toolchain.

## Rust Crate Dependencies (MatrixCache / MatrixRaft)

The Rust engine pulls its storage and consensus layers as pinned Git
dependencies from public GitHub (see `crates/temporalstore-rust/Cargo.toml`), so
`cargo build` fetches them automatically on the first build:

```text
matrixraft            https://github.com/matrixarkai/MatrixRaft.git         (Raft consensus)
matrixcache           https://github.com/matrixarkai/MatrixCache.git        (tiered cache + RocksDB SSD store)
```

An optional object-store backend sits behind `--features matrixobject`. It is not part of this build: the feature ships as an empty stub, so nothing is fetched for it and nothing needs to be.

Each is pinned to an exact revision, so you never clone them by hand — you only
need `git` and network access to `github.com`. For offline builds, run
`cargo vendor` on a connected host, or add `[patch]` path overrides pointing at
local MatrixCache/MatrixRaft clones.

## One-Command Build And Deploy

From the repo:

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
./tools/install_macos_temporalstore.sh --check-prereqs
```

Then build and deploy:

```bash
./tools/install_macos_temporalstore.sh --build
```

For baseline-style OSS model support:

```bash
./tools/install_context_oss_models.sh
source .local/context-oss-models/context_oss_models.env
```

Default paths:

```text
repo:        repository containing this script
install dir: ~/.local/temporalstore
data dir:    ~/Library/Application Support/TemporalStore
metaserver:  127.0.0.1:17101
datanode:    127.0.0.1:17102
```

If release binaries are already built:

```bash
./tools/install_macos_temporalstore.sh --skip-build
```

The installer:

- optionally builds release binaries with Cargo;
- installs the four Rust binaries into the install prefix;
- creates persistent cache/page/index/cursor/log directories;
- starts metaserver and datanode;
- validates health and smoke write/read;
- optionally writes Codex hook wrappers;
- optionally writes user launchd plist files.

## Useful Options

```text
--repo PATH                 Source repo. Default: repo containing this script
--install-dir PATH          Install prefix. Default: ~/.local/temporalstore
--data-dir PATH             Persistent data root. Default: ~/Library/Application Support/TemporalStore
--meta-port PORT            Metaserver port. Default: 17101
--data-port PORT            Datanode port. Default: 17102
--build                     Build release binaries before install
--skip-build                Do not build; require release binaries to exist
--check-prereqs             Check dependencies and paths, then exit
--skip-run                  Install only; do not start services
--skip-smoke                Skip health and write/read validation
--install-codex-hook        Generate macOS Codex hook wrappers
--hook-dir PATH             Hook wrapper dir. Default: ~/.matrixark/hooks
--hook-prefix PREFIX        Hook prefix. Default: matrixark:codex-hook:rust
--write-launchd             Write launchd user plist files
```

Environment overrides:

```text
TEMPORALSTORE_REPO
TEMPORALSTORE_INSTALL_DIR
TEMPORALSTORE_DATA_DIR
TEMPORALSTORE_META_PORT
TEMPORALSTORE_DATA_PORT
TEMPORALSTORE_CACHE_MEMORY_BYTES
MATRIXARK_TEMPORALSTORE_PREFIX
MATRIXARK_HOOK_DIR
```

## Runtime Layout

Installed binaries:

```text
~/.local/temporalstore/bin/matrixark_rust_metaserver
~/.local/temporalstore/bin/matrixark_rust_datanode
~/.local/temporalstore/bin/matrixark_rust_proxy
~/.local/temporalstore/bin/matrixark_rust_direct_sdk
```

Persistent storage:

```text
~/Library/Application Support/TemporalStore/cache
~/Library/Application Support/TemporalStore/pages
~/Library/Application Support/TemporalStore/indexes
~/Library/Application Support/TemporalStore/replica-replay-cursors
~/Library/Application Support/TemporalStore/logs
~/Library/Application Support/TemporalStore/run
```

The installer writes:

```text
~/.local/temporalstore/temporalstore.env
```

## Start, Stop, And Inspect

Run or rerun the installer to restart local services:

```bash
./tools/install_macos_temporalstore.sh --skip-build
```

Inspect pids and logs:

```bash
cat "$HOME/Library/Application Support/TemporalStore/run/metaserver.pid"
cat "$HOME/Library/Application Support/TemporalStore/run/datanode.pid"
tail -f "$HOME/Library/Application Support/TemporalStore/logs/metaserver.log"
tail -f "$HOME/Library/Application Support/TemporalStore/logs/datanode.log"
```

Stop services:

```bash
kill "$(cat "$HOME/Library/Application Support/TemporalStore/run/datanode.pid")"
kill "$(cat "$HOME/Library/Application Support/TemporalStore/run/metaserver.pid")"
```

## Health And Smoke Test

```bash
python3 - <<'PY'
import urllib.request
for port in (17101, 17102):
    print(urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=10).read().decode())
PY
```

Write/read smoke:

```bash
python3 - <<'PY'
import json
import urllib.request

url = "http://127.0.0.1:17102/execute"
value = list(b"temporalstore-rust-macos-ok")
write = {"shard_id": 1, "command": {"kind": "string_set", "key": "macos-native-smoke", "value": value}}
read = {"shard_id": 1, "command": {"kind": "string_get", "key": "macos-native-smoke"}}
for body in (write, read):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    print(urllib.request.urlopen(req, timeout=10).read().decode())
PY
```

## Codex Hook Wrapper

Install macOS hook wrappers:

```bash
./tools/install_macos_temporalstore.sh --skip-build --install-codex-hook
```

Generated files:

```text
~/.matrixark/hooks/matrixark-rust-proxy-macos.sh
~/.matrixark/hooks/matrixark-codex-hook-rust-macos.sh
```

Register this command for Codex `UserPromptSubmit`:

```text
~/.matrixark/hooks/matrixark-codex-hook-rust-macos.sh
```

The hook wrapper sets:

```text
MATRIXARK_MCP_BACKEND=temporalstore-rust
MATRIXARK_TEMPORALSTORE_RUST_PROXY=~/.matrixark/hooks/matrixark-rust-proxy-macos.sh
MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:codex-hook:rust
MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:17101
```

Manual hook smoke:

```bash
printf '%s\n' '{"hook_event_name":"UserPromptSubmit","session_id":"manual-macos-hook-smoke","prompt":"manual macOS hook smoke"}' \
  | ~/.matrixark/hooks/matrixark-codex-hook-rust-macos.sh
```

If this succeeds but natural prompts do not appear, TemporalStore is healthy and
the remaining issue is Codex hook registration or reload.

## First Failure Checklist

If installation fails, check in this order:

```bash
python3 --version
git --version
rustc --version
cargo --version
```

If ports are already in use:

```bash
lsof -nP -iTCP:17101 -sTCP:LISTEN || true
lsof -nP -iTCP:17102 -sTCP:LISTEN || true
```

If services start but health fails:

```bash
tail -n 120 "$HOME/Library/Application Support/TemporalStore/logs/metaserver.log"
tail -n 120 "$HOME/Library/Application Support/TemporalStore/logs/datanode.log"
```

The local smoke test only uses string write/read APIs. Context management,
Codex hook ingestion, and OSS model extraction are later layers.

## OSS Model Setup

The baseline memory system-style path uses OpenAI-compatible local readers and local
embedding models. Use the shared model installer:

```bash
./tools/install_context_oss_models.sh
source .local/context-oss-models/context_oss_models.env
```

Ollama/Qwen setup:

```bash
./tools/install_context_oss_models.sh \
  --install-ollama \
  --pull-ollama \
  --ollama-models "qwen2.5:0.5b qwen2.5:1.5b nomic-embed-text"
```

vLLM setup:

```bash
./tools/install_context_oss_models.sh --install-vllm
```

Common baseline-style profiles:

| Profile | Reader/VLM | Embedding |
| --- | --- | --- |
| `matrixark-native-oss-context` | `google/flan-t5-small` | `sentence-transformers/all-MiniLM-L6-v2` |
| `baseline-qwen2_5_vl-local` | `qwen2.5vl:7b` | `nomic-embed-text` |
| `baseline-llava-local` | `llava:7b` | `nomic-embed-text` |
| `baseline-internvl-vllm` | `OpenGVLab/InternVL2_5-8B` | `BAAI/bge-m3` |
| `baseline-minigpt4-gpt-style-vlm` | `Vision-CAIR/MiniGPT-4` | `BAAI/bge-m3` |

For paper-comparable the baseline memory system benchmark claims, use a live
OpenAI-compatible reader endpoint and disable deterministic fallback.

## Optional launchd

Write launchd user plists:

```bash
./tools/install_macos_temporalstore.sh --skip-build --write-launchd --skip-run
```

Load them:

```bash
launchctl load "$HOME/Library/LaunchAgents/com.matrixark.temporalstore.metaserver.plist"
launchctl load "$HOME/Library/LaunchAgents/com.matrixark.temporalstore.datanode.plist"
```

Unload them:

```bash
launchctl unload "$HOME/Library/LaunchAgents/com.matrixark.temporalstore.datanode.plist"
launchctl unload "$HOME/Library/LaunchAgents/com.matrixark.temporalstore.metaserver.plist"
```

## Clean Local State

Only do this when you intentionally want a fresh local TemporalStore:

```bash
rm -rf "$HOME/Library/Application Support/TemporalStore"
```

The install prefix can be removed separately:

```bash
rm -rf ~/.local/temporalstore
```
