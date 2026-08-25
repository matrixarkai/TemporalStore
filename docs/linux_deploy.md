# Linux Build And Deploy Manual

This guide builds and deploys Rust TemporalStore on Linux without Docker. It is
written for a fresh user starting from a clean Ubuntu machine.

If you are not sure which platform guide to use, start with
[TemporalStore Install Guide](INSTALL.md).

## What This Installs

The installer builds or installs four Rust binaries, starts local metaserver and
datanode services, creates persistent storage directories, and runs a basic
write/read smoke test. It does not require MatrixObject, S3, Raft clusters, or
OSS model downloads for the first local install.

The recommended local Linux deployment runs:

```text
matrixark_rust_metaserver
matrixark_rust_datanode
matrixark_rust_proxy
matrixark_rust_direct_sdk
```

The metaserver and datanode are long-lived processes. Hooks, clients, and
benchmarks call the proxy or direct SDK instead of embedding storage in Python.

## Recommended Build OS

Build TemporalStore on Ubuntu for the first Linux release path. We have used
and validated the Linux build/deploy flow on:

```text
Ubuntu 22.04 LTS
Ubuntu 26.04
```

Other modern Linux distributions may work, but Ubuntu is the recommended and
documented target because it keeps Rust, glibc, OpenSSL, systemd, and Docker
behavior predictable across local development and production-style testing.

## Dependencies

Required packages and tools:

```text
Linux x86_64
bash
python3
git
rustup/rustc/cargo
C/toolchain + clang/libclang + cmake   (to compile RocksDB via MatrixCache)
network access to github.com               (to fetch the MatrixCache/MatrixRaft crates)
```

For service management, user-mode `systemd` is optional. By default, the
installer runs services directly with pid files and logs.

On a fresh Ubuntu host:

```bash
sudo apt-get update
sudo apt-get install -y bash git curl ca-certificates python3 \
  build-essential pkg-config libssl-dev clang libclang-dev cmake
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

`clang`, `libclang-dev`, and `cmake` are required because the storage engine
depends on the `matrixcache` crate with the `rocksdb-ssd` feature, which builds
RocksDB from source (via `librocksdb-sys`, which uses `bindgen`/libclang).

Then clone and enter the repo:

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
```

Run the prerequisite check:

```bash
./tools/install_linux_temporalstore.sh --check-prereqs
```

The script defaults to the repo that contains the script. Maintainers can still
override it with `--repo /opt/github-services/TemporalStore`.

## Rust Crate Dependencies (MatrixCache / MatrixRaft)

The Rust TemporalStore engine does **not** vendor its storage and consensus
layers. `crates/temporalstore-rust/Cargo.toml` pulls them as pinned Git
dependencies from public GitHub, so `cargo build` fetches them automatically on
the first build:

```text
matrixraft            https://github.com/matrixarkai/MatrixRaft.git         (Raft consensus)
matrixcache           https://github.com/matrixarkai/MatrixCache.git        (tiered cache + RocksDB SSD store)
```

An optional object-store backend sits behind `--features matrixobject`. It is not part of this build: the feature ships as an empty stub, so nothing is fetched for it and nothing needs to be.

Each dependency is pinned to an exact revision, so builds are reproducible. You
do not clone these repos by hand — you only need `git` and network access to
`github.com` on the build host. The legacy implementation now lives in a
separate repository; this repository is Rust + Python only and needs no
build.

Offline or air-gapped builds: run `cargo vendor` on a connected host and commit
the generated `.cargo/config.toml`, or clone MatrixCache/MatrixRaft locally and
add `[patch."https://github.com/matrixarkai/MatrixCache.git"]` / MatrixRaft path
overrides to a workspace `Cargo.toml`.

## One-Command Build And Deploy

From the repo:

```bash
./tools/install_linux_temporalstore.sh --build
```

For baseline-style OSS model support, install the model runtime
before running extraction, summarization, or benchmark jobs:

```bash
./tools/install_context_oss_models.sh
source .local/context-oss-models/context_oss_models.env
```

That default installs Python model packages, downloads
`sentence-transformers/all-MiniLM-L6-v2`, and writes an env file used by
MatrixArk/TemporalStore hooks and benchmark runners.

Default paths:

```text
repo:        repository containing this script
install dir: ~/.local/temporalstore
data dir:    ~/.local/share/temporalstore
metaserver:  127.0.0.1:17101
datanode:    127.0.0.1:17102
```

If release binaries are already built:

```bash
./tools/install_linux_temporalstore.sh --skip-build
```

The installer:

- optionally builds release binaries with Cargo;
- installs the four Rust binaries into the install prefix;
- creates persistent cache/page/index/cursor/log directories;
- starts metaserver and datanode;
- validates health and smoke write/read;
- optionally writes Codex hook wrappers.

## Useful Options

```text
--repo PATH                 Source repo. Default: repo containing this script
--install-dir PATH          Install prefix. Default: ~/.local/temporalstore
--data-dir PATH             Persistent data root. Default: ~/.local/share/temporalstore
--meta-port PORT            Metaserver port. Default: 17101
--data-port PORT            Datanode port. Default: 17102
--build                     Build release binaries before install
--skip-build                Do not build; require release binaries to exist
--check-prereqs             Check dependencies and paths, then exit
--skip-run                  Install only; do not start services
--skip-smoke                Skip health and write/read validation
--install-codex-hook        Generate Linux Codex hook wrappers
--hook-dir PATH             Hook wrapper dir. Default: ~/.matrixark/hooks
--hook-prefix PREFIX        Hook prefix. Default: matrixark:codex-hook:rust
--write-systemd-user        Write user-mode systemd unit files
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
~/.local/share/temporalstore/cache
~/.local/share/temporalstore/pages
~/.local/share/temporalstore/indexes
~/.local/share/temporalstore/replica-replay-cursors
~/.local/share/temporalstore/logs
~/.local/share/temporalstore/run
```

The installer writes:

```text
~/.local/temporalstore/temporalstore.env
```

That file contains the effective runtime addresses and storage paths used by
both services.

## Start, Stop, And Inspect

Run or rerun the installer to restart the local services:

```bash
./tools/install_linux_temporalstore.sh --skip-build
```

Inspect pids and logs:

```bash
cat ~/.local/share/temporalstore/run/metaserver.pid
cat ~/.local/share/temporalstore/run/datanode.pid
tail -f ~/.local/share/temporalstore/logs/metaserver.log
tail -f ~/.local/share/temporalstore/logs/datanode.log
```

Stop services:

```bash
kill "$(cat ~/.local/share/temporalstore/run/datanode.pid)"
kill "$(cat ~/.local/share/temporalstore/run/metaserver.pid)"
```

## Health And Smoke Test

Health:

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
value = list(b"temporalstore-rust-linux-ok")
write = {"shard_id": 1, "command": {"kind": "string_set", "key": "linux-native-smoke", "value": value}}
read = {"shard_id": 1, "command": {"kind": "string_get", "key": "linux-native-smoke"}}
for body in (write, read):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    print(urllib.request.urlopen(req, timeout=10).read().decode())
PY
```

## Codex Hook Wrapper

Install Linux hook wrappers:

```bash
./tools/install_linux_temporalstore.sh --skip-build --install-codex-hook
```

Generated files:

```text
~/.matrixark/hooks/matrixark-rust-proxy-linux.sh
~/.matrixark/hooks/matrixark-codex-hook-rust-linux.sh
```

Register this command for Codex `UserPromptSubmit`:

```text
~/.matrixark/hooks/matrixark-codex-hook-rust-linux.sh
```

The hook wrapper sets:

```text
MATRIXARK_MCP_BACKEND=temporalstore-rust
MATRIXARK_TEMPORALSTORE_RUST_PROXY=~/.matrixark/hooks/matrixark-rust-proxy-linux.sh
MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:codex-hook:rust
MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:17101
```

Manual hook smoke:

```bash
printf '%s\n' '{"hook_event_name":"UserPromptSubmit","session_id":"manual-linux-hook-smoke","prompt":"manual Linux hook smoke"}' \
  | ~/.matrixark/hooks/matrixark-codex-hook-rust-linux.sh
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
ss -ltnp | grep -E ':17101|:17102' || true
```

If services start but health fails:

```bash
tail -n 120 ~/.local/share/temporalstore/logs/metaserver.log
tail -n 120 ~/.local/share/temporalstore/logs/datanode.log
```

The local smoke test only uses string write/read APIs. Context management,
Codex hook ingestion, and OSS model extraction are later layers.

## OSS Model Setup

The baseline memory system-style path uses OpenAI-compatible local readers and local
embedding models. The repo provides one installer entrypoint:

```bash
./tools/install_context_oss_models.sh --help
```

Recommended minimal setup:

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

vLLM setup, for stronger local OpenAI-compatible readers:

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

The env file sets:

```text
MATRIXARK_EMBEDDING_PROVIDER=oss
MATRIXARK_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
MATRIXARK_EMBEDDING_MODEL_PATH=<local downloaded model path>
MATRIXARK_EXTRACTION_MODEL=qwen2.5:0.5b
MATRIXARK_SUMMARY_MODEL=qwen2.5:0.5b
TEMPORALSTORE_READER_BASE_URL=http://127.0.0.1:11434/v1
```

For paper-comparable the baseline memory system benchmark claims, use a live
OpenAI-compatible reader endpoint and disable deterministic fallback in the
benchmark runner. The deterministic/hash fallback path is only for local
pipeline validation.

## Optional User-Mode Systemd

Write user-mode unit files:

```bash
./tools/install_linux_temporalstore.sh --skip-build --write-systemd-user --skip-run
```

Then start them:

```bash
systemctl --user enable --now temporalstore-rust-metaserver.service
systemctl --user enable --now temporalstore-rust-datanode.service
```

Check status:

```bash
systemctl --user status temporalstore-rust-metaserver.service
systemctl --user status temporalstore-rust-datanode.service
```

## Clean Local State

Only do this when you intentionally want a fresh local TemporalStore:

```bash
rm -rf ~/.local/share/temporalstore
```

The install prefix can be removed separately:

```bash
rm -rf ~/.local/temporalstore
```
