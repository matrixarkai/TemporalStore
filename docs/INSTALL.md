# TemporalStore Install Guide

This guide is for a first-time user who has a fresh machine and no MatrixArk or
TemporalStore background. It explains what to install, which path to choose, how
to verify the service, and where to look when something fails.

> **In a hurry?** With Docker installed, the three commands under
> [Simplest Path: Single Node In Docker](#simplest-path-single-node-in-docker)
> give you a durable, always-on node — no language toolchain required. Then add
> agent memory with one command (see the hook step in your platform's quick start).

## What You Are Installing

The default open-source install runs the Rust TemporalStore local service:

```text
matrixark_rust_metaserver   cluster metadata and readiness
matrixark_rust_datanode     storage and query serving
matrixark_rust_proxy        client/proxy process used by hooks and tools
matrixark_rust_direct_sdk   direct SDK binary for native clients
```

For a local laptop install, the metaserver and datanode run on localhost and
store data under a persistent local directory. Agent hook support — for both
**Codex** and **Claude Code** — is optional and can be enabled after the storage
service passes a smoke test. Both hooks run the same context engine (ingestion,
extraction, retrieval); they differ only in agent identity.

## Simplest Path: Single Node In Docker

The Docker path is the **recommended way to start** for most fresh users: the
host needs **only Docker** — no Rust toolchain, no `clang`/`cmake`, and no RocksDB
build, because the whole toolchain lives inside the image's build stage. (Don't
have Docker yet? See [Step 0](#step-0-install-docker-if-you-dont-have-it).)

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
docker compose -f docker-compose.single-node.yml up --build -d   # -d runs it in the background
```

The first run compiles the two service binaries inside the image (a few minutes,
once); later starts are instant. One health-checked container comes up and
**restarts automatically if it exits**:

```text
http://127.0.0.1:17101   metaserver (metadata, health)
http://127.0.0.1:17102   datanode   (health + writes/reads via POST /execute)
```

Verify it from any terminal on the host:

```bash
curl http://127.0.0.1:17102/health
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_set","key":"hello","value":[119,111,114,108,100]}}'
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_get","key":"hello"}}'
```

Everyday operations:

```bash
docker compose -f docker-compose.single-node.yml logs -f    # follow logs
docker compose -f docker-compose.single-node.yml ps         # status + health
docker compose -f docker-compose.single-node.yml stop       # stop, keep data
docker compose -f docker-compose.single-node.yml up -d      # start again
git pull && docker compose -f docker-compose.single-node.yml up --build -d   # upgrade
```

Data persists across restarts in the named volume `temporalstore-data` (mounted
at `/var/lib/temporalstore`); it grows as agent memory is ingested. Reset the
node and wipe **all** stored memory with:

```bash
docker compose -f docker-compose.single-node.yml down -v
```

### Point an agent hook at the Dockerized store (optional)

The container exposes the **same ports as the native service**, so the hooks
connect to it by pointing their metaserver at the container. One command installs
and registers both agents' hooks against it:

```bash
MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:17101 \
  bash integrations/agent-hooks/install/install.sh --agent both --mode native --repo "$(pwd)"
```

Every agent pointed at the same container shares one always-on store. See
[Claude Code hook integration](matrixark_claude_hook_integration.md) and the
[Codex hook manual](matrixark_codex_mcp_hook_installation_manual.md) for details.

Want a native install (bundled hook wrappers, local store) instead? Pick a path
below.

## Step 0: Install Docker (If You Don't Have It)

Skip this if `docker run --rm hello-world` already works.

**Ubuntu / Debian Linux** — official convenience script, then let your user run
Docker without `sudo`:

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"   # log out/in (or run: newgrp docker)
docker run --rm hello-world
```

**macOS** — Docker Desktop (GUI) or Colima (CLI-only), via Homebrew:

```bash
brew install --cask docker      # then launch Docker.app once
# or, headless:  brew install colima docker && colima start
docker run --rm hello-world
```

**Windows** — install [Docker Desktop](https://www.docker.com/products/docker-desktop/),
enable the WSL 2 backend (run `wsl --install` once from an elevated PowerShell if
needed), start Docker Desktop, then in PowerShell:

```powershell
docker run --rm hello-world
```

## Choose One Install Path

| Platform | Recommended path | Start here |
| --- | --- | --- |
| Any (simplest) | Single-node Docker image | [Single Node In Docker](#simplest-path-single-node-in-docker) |
| Windows | Docker Desktop with Linux container | [Windows Docker](windows_docker_install.md) |
| Ubuntu Linux | Native Rust service | [Linux](linux_deploy.md) |
| macOS | Native Rust service | [macOS](macos_deploy.md) |

New to TemporalStore? The **single-node Docker image is the least-setup option** —
it needs no language toolchain on the host and runs the same engine. Choose a
native path when you want the bundled agent-hook wrappers or prefer not to run
Docker.

Windows users do not need WSL for the normal runtime path. WSL is only useful
for maintainers who rebuild Linux binaries or Docker images from source.

## Before You Start

You need these basics:

```text
git
python3
Rust toolchain for native Linux/macOS builds
C/toolchain + clang/libclang + cmake   for native builds (RocksDB via MatrixCache)
network access to github.com               to fetch the MatrixCache/MatrixRaft crates
Docker Desktop for Windows Docker installs
```

The native (non-Docker) build compiles the Rust engine, which depends on the
`matrixcache` and `matrixraft` crates. Cargo fetches these automatically from
public GitHub — see [Build dependencies](#build-dependencies-matrixcache--matrixraft)
below. The Docker path needs none of the build tools; the toolchain lives inside
the image build stage.

You also need a local clone of this repository:

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
```

If you already have a clone, update it first:

```bash
git pull --ff-only
```

## Build Dependencies (MatrixCache / MatrixRaft)

The Rust TemporalStore engine does not vendor its storage and consensus layers.
`crates/temporalstore-rust/Cargo.toml` pulls them as **pinned Git dependencies**
from public GitHub, so `cargo build` fetches them automatically on the first
build — you never clone them by hand:

```text
matrixraft            https://github.com/matrixarkai/MatrixRaft.git         Raft consensus
matrixcache           https://github.com/matrixarkai/MatrixCache.git        tiered cache + RocksDB SSD store
```

An optional object-store backend sits behind `--features matrixobject`. It is not part of this build: the feature ships as an empty stub, so nothing is fetched for it and nothing needs to be.

Each is pinned to an exact revision, so builds are reproducible. Because
`matrixcache` enables the `rocksdb-ssd` feature, the first build compiles RocksDB
from source (`librocksdb-sys`), which needs a **C/toolchain, `clang`/
`libclang`, and `cmake`** in addition to Rust and `git`:

- Linux (Ubuntu): `sudo apt-get install -y build-essential pkg-config libssl-dev clang libclang-dev cmake`
- macOS: `xcode-select --install` (clang/libclang) and `brew install cmake`

The platform guides ([Linux](linux_deploy.md), [macOS](macos_deploy.md)) list the
full package sets. The legacy implementation now lives in a separate
repository, so this repository is Rust + Python only and needs no build. For
offline/air-gapped builds, run `cargo vendor` on a connected host, or add
`[patch]` path overrides pointing at local MatrixCache/MatrixRaft clones.

## Linux Quick Start

Use Ubuntu 22.04 LTS or Ubuntu 26.04 when possible.

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
./tools/install_linux_temporalstore.sh --check-prereqs
./tools/install_linux_temporalstore.sh --build
```

`--check-prereqs` prints a checklist and the exact command to install anything
that's `[MISSING]` — run it first. Then `--build` builds and starts local services
and runs a write/read smoke test; a successful run prints health responses for
ports `17101` and `17102`.

### Enable agent hooks — Codex and Claude Code (optional)

Once the service works, **one command installs and registers the hooks for both
agents**. It writes the Codex plugin and `~/.claude/settings.json` for you — no
manual config editing:

```bash
bash integrations/agent-hooks/install/install.sh --agent both --mode native --repo "$(pwd)"
```

Use `--agent codex` or `--agent claude` for just one. This registers the full
lifecycle so context is ingested and retrieved automatically on every turn. Then
restart Codex / Claude Code. See
[Claude Code hook integration](matrixark_claude_hook_integration.md) and the
[Codex hook manual](matrixark_codex_mcp_hook_installation_manual.md) for backends,
warm-up, and a quick check.

<details>
<summary>Advanced: generate standalone wrappers and register them yourself</summary>

```bash
./tools/install_linux_temporalstore.sh --skip-build --install-codex-hook --install-claude-hook
```

This writes `~/.matrixark/hooks/matrixark-codex-hook-rust-linux.sh` and
`matrixark-claude-hook-rust-linux.sh`. Point your Codex `UserPromptSubmit` hook
and `~/.claude/settings.json` at them.
</details>

### Warm the store with your existing local context (optional)

So retrieval is not cold on your first turn, backfill TemporalStore from the
memory your agents already have on disk (Claude transcripts, Codex sessions,
`CLAUDE.md`/`AGENTS.md`/`MEMORY.md`, and other local agent memory):

```bash
python3 tools/matrixark_local_backfill_ingester.py --agents claude,codex --dry-run   # preview, no writes
python3 tools/matrixark_local_backfill_ingester.py --agents claude,codex             # ingest
```

See [Memory Model And Local Context Backfill](#memory-model-and-local-context-backfill).

## macOS Quick Start

Install prerequisites first:

```bash
xcode-select --install
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install git python rustup-init
rustup-init -y
source "$HOME/.cargo/env"
```

Then build and run:

```bash
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
./tools/install_macos_temporalstore.sh --check-prereqs
./tools/install_macos_temporalstore.sh --build
```

`--check-prereqs` prints a checklist and the exact command to install anything
that's `[MISSING]` — run it first. `--build` then builds, starts local services,
and runs a write/read smoke test.

### Enable agent hooks — Codex and Claude Code (optional)

Once the smoke test passes, **one command installs and registers the hooks for
both agents** (writes the Codex plugin and `~/.claude/settings.json` for you):

```bash
bash integrations/agent-hooks/install/install.sh --agent both --mode native --repo "$(pwd)"
```

Use `--agent codex` or `--agent claude` for just one, then restart the agent. See
[Claude Code hook integration](matrixark_claude_hook_integration.md) and the
[Codex hook manual](matrixark_codex_mcp_hook_installation_manual.md) for details.

<details>
<summary>Advanced: generate standalone wrappers and register them yourself</summary>

```bash
./tools/install_macos_temporalstore.sh --skip-build --install-codex-hook --install-claude-hook
```

This writes `~/.matrixark/hooks/matrixark-codex-hook-rust-macos.sh` and
`matrixark-claude-hook-rust-macos.sh`. Point your Codex `UserPromptSubmit` hook
and `~/.claude/settings.json` at them.
</details>

### Warm the store with your existing local context (optional)

So retrieval is not cold on your first turn, backfill TemporalStore from the
memory your agents already have on disk (Claude transcripts, Codex sessions,
`CLAUDE.md`/`AGENTS.md`/`MEMORY.md`, and other local agent memory):

```bash
python3 tools/matrixark_local_backfill_ingester.py --agents claude,codex --dry-run   # preview, no writes
python3 tools/matrixark_local_backfill_ingester.py --agents claude,codex             # ingest
```

See [Memory Model And Local Context Backfill](#memory-model-and-local-context-backfill).

## Windows Docker Quick Start

Install Docker Desktop and use Linux containers. Then open PowerShell in the
repository root:

```powershell
git clone https://github.com/matrixarkai/TemporalStore.git
cd TemporalStore
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -CheckPrereqs
```

If you already have a prebuilt image:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -ImageName matrixark-temporalstore-rust:win-local `
  -SkipImagePull
```

If the image is in a registry:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -ImageName <registry>/<image>:<tag> `
  -PullImage
```

### Enable agent hooks — Codex and Claude Code (optional)

On Windows the hooks run through WSL and connect to the Dockerized store. One
command installs and registers **both** agents (writes the Codex plugin and
`%USERPROFILE%\.claude\settings.json`); point `-WslRepo` at your clone inside the
WSL distro:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\integrations\agent-hooks\install\install.ps1 `
  -Agent both -Mode wsl `
  -WslRepo /opt/github-services/TemporalStore
```

Use `-Agent codex` or `-Agent claude` for just one, then restart the agent. See
[Claude Code hook integration](matrixark_claude_hook_integration.md) and the
[Codex hook manual](matrixark_codex_mcp_hook_installation_manual.md) for details.

## Prebuilt Images (Optional)

By default every Docker path here **builds the image locally** from source — the
`docker compose ... up --build` and `Dockerfile.single-node` steps above need no
registry access and no prebuilt artifact.

There is not yet a public TemporalStore image on a shared registry, so "pull an
image" only applies if your team publishes one internally. When you have such an
image, skip the local build and point the installer at it:

```powershell
# Windows: use an image already loaded locally
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -ImageName <registry>/<image>:<tag> -PullImage
```

```bash
# Linux/macOS: pull, then run it as a single node
docker pull <registry>/<image>:<tag>
docker run --rm -p 17101:17101 -p 17102:17102 <registry>/<image>:<tag>
```

## Verify The Service

Health endpoints:

```bash
curl http://127.0.0.1:17101/health
curl http://127.0.0.1:17102/health
```

If `curl` is not installed, use the smoke command from the platform-specific
manual. The installer already runs the same check unless `--skip-smoke` or
`-SkipSmoke` is used.

## Where Data Goes

| Platform | Default data directory |
| --- | --- |
| Linux | `~/.local/share/temporalstore` |
| macOS | `~/Library/Application Support/TemporalStore` |
| Windows Docker | Docker volume `temporalstore-rust-win-data` mounted at `/var/lib/temporalstore` |

Deleting these paths removes local TemporalStore data.

## Memory Model And Local Context Backfill

By default, the Codex and Claude Code hooks treat **TemporalStore as the single
system of record for all agent memory**. Every turn flows through the same
pipeline — ingestion of prompts and tool events, extraction of segments,
entities, summaries, and embeddings, and retrieval of relevant context before the
next LLM call — and all of it is persisted in TemporalStore under an agent-scoped
prefix (`matrixark:codex-hook:rust`, `matrixark:claude-hook:rust`).

**On restart, memory is recovered from TemporalStore's own persistence — not from
logs.** The engine loads its persisted records from the on-disk data dir on start,
so restarting the service (or re-running a hook) keeps all prior memory without
re-reading any transcript. External local context is (re-)ingested **only on a
first start or when the on-disk store is empty** (a fresh or wiped data dir): the
`MATRIXARK_BACKFILL_ON_START` daemon checks each agent's store and skips any that
already holds records, so restarts never re-ingest from external logs.

To **force** a re-ingest from the agents' own logs (Claude/Codex transcripts,
rollouts, resources) even when the store is already populated — e.g. to re-import
history on demand — set `MATRIXARK_BACKFILL_FORCE=1`. It overrides the guard and
is dedup-safe (records are keyed by content hash):

```bash
MATRIXARK_BACKFILL_FORCE=1 bash tools/matrixark_backfill_daemon.sh
```

**Local memory is backfilled into TemporalStore** rather than kept as a separate
store:

- When the local proxy/service is not yet running, the offline Rust backend keeps
  session memory in local on-disk directories and fails open, so a turn is never
  blocked. Once TemporalStore is healthy, that local memory is backfilled in and
  the store stays authoritative.
- Existing local agent context that predates the hook is imported into
  TemporalStore through the **same engine** as live ingestion, so retrieval is
  warm on the next turn. `tools/matrixark_local_backfill_ingester.py` reads the
  real on-disk surfaces both agents leave behind — Claude Code transcripts
  (`~/.claude/projects/...`), Codex session rollouts (`~/.codex/sessions/...`),
  Codex dual-hook captures, local Markdown/resource files (`CLAUDE.md`,
  `AGENTS.md`, `MEMORY.md`, `docs/*.md`), and other local agent memory
  state — normalizes each into a hook payload, and ingests it idempotently
  (nodes are keyed by content hash, so re-runs converge).

Preview first, then ingest (run after the storage service is healthy — see
[Verify The Service](#verify-the-service)):

```bash
# Preview what would be ingested — enumerate and normalize only, no writes:
python3 tools/matrixark_local_backfill_ingester.py --agents claude,codex --dry-run --report /tmp/backfill.json

# Backfill local Claude + Codex context into TemporalStore:
python3 tools/matrixark_local_backfill_ingester.py --agents claude,codex
```

**Zero-effort auto-warm (recommended for fresh users):** instead of running the
command by hand, opt in and let the Claude Code hook do it for you. Set the
environment variable before starting Claude Code:

```bash
export MATRIXARK_BACKFILL_ON_START=1
```

On the next `SessionStart` the hook launches `tools/matrixark_backfill_daemon.sh`
detached, so it **never blocks a turn**. The daemon is resumable (a lockfile and
per-agent offset markers let it pick up after a restart) and dedup-safe (records
are keyed by content hash), and durable memory (`CLAUDE.md`/`AGENTS.md`/`MEMORY.md`,
resources) lands in the cross-session `_global` scope first. Progress
is logged to `/tmp/matrixark-backfill/daemon.log`.

Related tools for other backfill needs:

```text
tools/matrixark_codex_session_bridge.py   stream live Codex desktop/CLI sessions into the hook (-> TemporalStore)
tools/matrixark_context_backfill.py       replay/repair the raw ingestion log into context-serving prefixes
```

## Troubleshooting

| Symptom | First check |
| --- | --- |
| `missing required command: cargo` | Install Rust with `rustup` and reopen the terminal. |
| Health endpoint does not respond | Check metaserver/datanode logs under the platform data directory. |
| Port already in use | Re-run with `--meta-port` / `--data-port`, or stop the old service. |
| Codex manual hook smoke works, but real prompts do not appear | Reload/restart Codex and verify the global `UserPromptSubmit` hook registration. |
| Claude Code hook smoke works, but real prompts do not appear | Restart Claude Code and confirm `~/.claude/settings.json` registers `tools/matrixark_claude_hook.sh` for the lifecycle events. |
| Windows says Docker is unavailable | Start Docker Desktop and wait until `docker info` succeeds. |
| Docker image not found | Use `-PullImage`, provide a registry image name, or build the image as a maintainer. |

## Optional OSS Models

Context extraction and benchmark jobs can use local OSS models. Install them
after TemporalStore itself is healthy:

```bash
./tools/install_context_oss_models.sh
source .local/context-oss-models/context_oss_models.env
```

The storage service does not require OSS models for basic write/read operation.
