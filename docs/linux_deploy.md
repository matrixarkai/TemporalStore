# Linux Build And Deploy Manual

This guide builds and deploys Rust TemporalStore on Linux without Docker. It is
the native Linux counterpart to the Windows Docker installer.

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

Required:

```text
Linux x86_64
bash
python3
git
rustup/rustc/cargo
```

For service management, user-mode `systemd` is optional. The installer can run
the services directly with pid files and logs.

## One-Command Build And Deploy

From the repo:

```bash
./tools/install_linux_temporalstore.sh --build
```

Default paths:

```text
repo:        /root/src/github-services/TemporalStore
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
--repo PATH                 Source repo. Default: /root/src/github-services/TemporalStore
--install-dir PATH          Install prefix. Default: ~/.local/temporalstore
--data-dir PATH             Persistent data root. Default: ~/.local/share/temporalstore
--meta-port PORT            Metaserver port. Default: 17101
--data-port PORT            Datanode port. Default: 17102
--build                     Build release binaries before install
--skip-build                Do not build; require release binaries to exist
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
