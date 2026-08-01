# TemporalStore Install Guide

This guide is for a first-time user who has a fresh machine and no MatrixArk or
TemporalStore background. It explains what to install, which path to choose, how
to verify the service, and where to look when something fails.

## What You Are Installing

The default open-source install runs the Rust TemporalStore local service:

```text
matrixark_rust_metaserver   cluster metadata and readiness
matrixark_rust_datanode     storage and query serving
matrixark_rust_proxy        client/proxy process used by hooks and tools
matrixark_rust_direct_sdk   direct SDK binary for native clients
```

For a local laptop install, the metaserver and datanode run on localhost and
store data under a persistent local directory. Codex hook support is optional
and can be enabled after the storage service passes a smoke test.

## Simplest Path: Single Node In Docker

If you just want a working TemporalStore on any OS and do not need a native
build, use the single-node Docker image. You need only Docker (see
[Step 0](#step-0-install-docker-if-you-dont-have-it)) and a clone of this repo:

```bash
git clone https://github.com/bjmeetsfo/TemporalStore.git
cd TemporalStore
docker compose -f docker-compose.single-node.yml up --build
```

This builds a lean image (the Rust toolchain lives inside the build stage, so
nothing but Docker is installed on the host) and starts one node:

```text
http://127.0.0.1:17101   metaserver (metadata, health)
http://127.0.0.1:17102   datanode   (health + writes/reads via POST /execute)
```

Verify it from another terminal:

```bash
curl http://127.0.0.1:17102/health
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_set","key":"hello","value":[119,111,114,108,100]}}'
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_get","key":"hello"}}'
```

Data persists in the `temporalstore-data` volume. Remove the node and its data
with `docker compose -f docker-compose.single-node.yml down -v`.

Want a native install, or Codex hook integration? Pick a path below.

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

Windows users do not need WSL for the normal runtime path. WSL is only useful
for maintainers who rebuild Linux binaries or Docker images from source.

## Before You Start

You need these basics:

```text
git
python3
Rust toolchain for native Linux/macOS builds
Docker Desktop for Windows Docker installs
```

You also need a local clone of this repository:

```bash
git clone https://github.com/bjmeetsfo/TemporalStore.git
cd TemporalStore
```

If you already have a clone, update it first:

```bash
git pull --ff-only
```

## Linux Quick Start

Use Ubuntu 22.04 LTS or Ubuntu 26.04 when possible.

```bash
git clone https://github.com/bjmeetsfo/TemporalStore.git
cd TemporalStore
./tools/install_linux_temporalstore.sh --check-prereqs
./tools/install_linux_temporalstore.sh --build
```

The script starts local services and runs a write/read smoke test. A successful
run prints health responses for ports `17101` and `17102`.

Install Codex hook wrappers after the service works:

```bash
./tools/install_linux_temporalstore.sh --skip-build --install-codex-hook
```

Register this command as the Codex `UserPromptSubmit` hook:

```text
~/.matrixark/hooks/matrixark-codex-hook-rust-linux.sh
```

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
git clone https://github.com/bjmeetsfo/TemporalStore.git
cd TemporalStore
./tools/install_macos_temporalstore.sh --check-prereqs
./tools/install_macos_temporalstore.sh --build
```

Install Codex hook wrappers after the smoke test passes:

```bash
./tools/install_macos_temporalstore.sh --skip-build --install-codex-hook
```

Register this command as the Codex `UserPromptSubmit` hook:

```text
~/.matrixark/hooks/matrixark-codex-hook-rust-macos.sh
```

## Windows Docker Quick Start

Install Docker Desktop and use Linux containers. Then open PowerShell in the
repository root:

```powershell
git clone https://github.com/bjmeetsfo/TemporalStore.git
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

Install Codex hook wrappers after the container smoke test passes:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -SkipImagePull `
  -InstallCodexHookWrapper
```

Register this command as the Codex `UserPromptSubmit` hook:

```text
%USERPROFILE%\.matrixark\hooks\matrixark-codex-hook-rust-docker.cmd
```

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

## Troubleshooting

| Symptom | First check |
| --- | --- |
| `missing required command: cargo` | Install Rust with `rustup` and reopen the terminal. |
| Health endpoint does not respond | Check metaserver/datanode logs under the platform data directory. |
| Port already in use | Re-run with `--meta-port` / `--data-port`, or stop the old service. |
| Codex manual hook smoke works, but real prompts do not appear | Reload/restart Codex and verify the global `UserPromptSubmit` hook registration. |
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
