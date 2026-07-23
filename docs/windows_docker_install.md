# Windows Docker Installation Manual

This guide installs TemporalStore into Windows Docker Desktop for local
production-style testing and Codex hook ingestion with Rust TemporalStore. By
default, the installer expects the source and build artifacts in:

```text
/root/src/github-services/TemporalStore
```

The recommended first Windows Docker target is the Rust TemporalStore service
split:

- `matrixark_rust_metaserver`
- `matrixark_rust_datanode`
- `matrixark_rust_proxy` as a client/proxy binary
- `matrixark_rust_direct_sdk` as a direct-SDK binary

The container runs the metaserver and datanode as long-lived processes. Client
and benchmark code should call the proxy or direct SDK, not embed storage in the
Python hook path.

## Dependency Summary

Required for the Rust Docker runtime:

```text
Windows 10/11 or Windows Server with WSL2 support
PowerShell 5.1 or newer
Docker Desktop with Linux containers
WSL2 Linux distro, such as Ubuntu 22.04
git inside WSL
Rust toolchain inside WSL: rustup, rustc, cargo
Python 3 inside WSL, for the Codex hook adapter
```

Required for Codex hook integration:

```text
Docker Desktop running
TemporalStore Rust container running
Codex configured to execute the generated hook wrapper
```

Optional:

```text
winget, only when using -InstallDockerDesktop
HTTP/HTTPS proxy, only when your network requires it
```

The installer does not require private paths, credentials, generated caches, or
WSL bind mounts into Docker. It copies only release binaries into a temporary
Windows Docker build context.

## One-Command Installer

The preferred path is the PowerShell installer:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -InstallDockerDesktop `
  -BuildReleaseBinaries `
  -InstallCodexHookWrapper
```

If Docker Desktop and release binaries are already present:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1
```

Useful options:

```text
-WslDistro <name>                 Default: auto-detect first WSL distro
-RepoPath <path>                  Default: /root/src/github-services/TemporalStore
-ImageName <name:tag>             Default: matrixark-temporalstore-rust:win-local
-ContainerName <name>             Default: temporalstore-rust-win
-VolumeName <name>                Default: temporalstore-rust-win-data
-MetaPort <port>                  Default: 17101
-DataPort <port>                  Default: 17102
-InstallDockerDesktop             Install Docker Desktop through winget
-BuildReleaseBinaries             Build Rust release binaries before packaging
-SkipImageBuild                   Reuse an existing Docker image
-SkipRun                          Build only, do not start the container
-SkipSmoke                        Skip health and write/read validation
-NoRestartPersistenceCheck        Skip restart persistence validation
-InstallCodexHookWrapper          Generate Windows .cmd wrappers for Codex hook use
-HookInstallDir <path>            Default: %USERPROFILE%\.matrixark\hooks
-HookPrefix <prefix>              Default: matrixark:codex-hook:rust
-HttpProxy <url>                  Optional proxy for winget/Docker install
-HttpsProxy <url>                 Optional proxy for winget/Docker install
```

The script performs the same steps documented below:

- starts Docker Desktop and waits for the engine;
- validates the four Rust release binaries;
- builds a small Windows Docker image from those binaries;
- starts metaserver and datanode with a persistent Docker volume;
- validates health, write/read, and restart persistence;
- optionally writes Codex hook wrappers.

## Codex Hook Wrapper

Install Rust TemporalStore plus hook wrappers:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -BuildReleaseBinaries `
  -InstallCodexHookWrapper
```

The wrapper files are written to:

```text
%USERPROFILE%\.matrixark\hooks\matrixark-rust-proxy-docker.cmd
%USERPROFILE%\.matrixark\hooks\matrixark-codex-hook-rust-docker.cmd
%USERPROFILE%\.matrixark\hooks\matrixark-rust-proxy-docker.ps1
%USERPROFILE%\.matrixark\hooks\matrixark-codex-hook-rust-docker.ps1
%USERPROFILE%\.matrixark\hooks\matrixark-rust-proxy-docker.sh
%USERPROFILE%\.matrixark\hooks\matrixark-codex-hook-rust-docker.sh
```

Use this command as the Codex `UserPromptSubmit` hook command:

```text
%USERPROFILE%\.matrixark\hooks\matrixark-codex-hook-rust-docker.cmd
```

The Codex hook wrapper sets:

```text
MATRIXARK_MCP_BACKEND=temporalstore-rust
MATRIXARK_TEMPORALSTORE_RUST_PROXY=<generated docker-exec proxy wrapper>
MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:codex-hook:rust
MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:17101
```

The proxy wrapper runs:

```text
Codex -> .cmd wrapper -> PowerShell wrapper -> WSL python3 hook adapter
      -> WSL proxy wrapper -> Windows Docker Desktop
      -> docker exec -i temporalstore-rust-win matrixark_rust_proxy --serve
```

That gives Codex a stable Windows command while the actual Rust TemporalStore
runtime and persistent storage live inside Docker.

For agents or Codex installations that support multiple lifecycle hooks, use
the same wrapper pattern for `Stop`, `PostToolUse`, and session boundary hooks.
For the first open-source path, `UserPromptSubmit` is the minimal real-time
prompt ingestion hook.

## Prerequisites

Install Docker Desktop on Windows:

```powershell
powershell -ExecutionPolicy Bypass `
  -File .\tools\install_windows_docker_temporalstore.ps1 `
  -InstallDockerDesktop `
  -SkipImageBuild `
  -SkipRun `
  -SkipSmoke
```

If your network requires a proxy, pass `-HttpProxy` and `-HttpsProxy` explicitly.

Start Docker Desktop:

```powershell
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe" -WindowStyle Hidden
```

Verify the Windows Docker client is using Docker Desktop:

```powershell
$dockerBin='C:\Program Files\Docker\Docker\resources\bin'
$docker=Join-Path $dockerBin 'docker.exe'
$env:PATH="$dockerBin;$env:PATH"
& $docker version
& $docker info
```

Expected context:

```text
desktop-linux
```

## Build Or Refresh Release Binaries

Use the WSL repo and build release binaries before packaging them:

```powershell
wsl -- bash -lc `
  "cd /root/src/github-services/TemporalStore && \
   git fetch origin main && \
   cargo build --release -p temporalstore-rust --bins"
```

Required release binaries:

```text
/root/src/github-services/TemporalStore/target/release/matrixark_rust_metaserver
/root/src/github-services/TemporalStore/target/release/matrixark_rust_datanode
/root/src/github-services/TemporalStore/target/release/matrixark_rust_proxy
/root/src/github-services/TemporalStore/target/release/matrixark_rust_direct_sdk
```

## Build The Windows Docker Image

Create a small Windows build context that contains only the runtime binaries.
Replace `$distro` if your WSL distro has a different name:

```powershell
$dockerBin='C:\Program Files\Docker\Docker\resources\bin'
$docker=Join-Path $dockerBin 'docker.exe'
$env:PATH="$dockerBin;$env:PATH"

$ctx=Join-Path $env:TEMP ('temporalstore-rust-win-docker-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force $ctx | Out-Null

$distro=((wsl -l -q | Select-Object -First 1) -replace "`0", "").Trim()
$src="\\wsl.localhost\$distro\root\src\github-services\TemporalStore\target\release"
foreach ($name in @(
  'matrixark_rust_metaserver',
  'matrixark_rust_datanode',
  'matrixark_rust_proxy',
  'matrixark_rust_direct_sdk'
)) {
  Copy-Item -LiteralPath (Join-Path $src $name) -Destination (Join-Path $ctx $name) -Force
}
```

Write the runtime Dockerfile:

```powershell
$dockerfile = @'
FROM ubuntu:22.04
COPY matrixark_rust_metaserver /usr/local/bin/matrixark_rust_metaserver
COPY matrixark_rust_datanode /usr/local/bin/matrixark_rust_datanode
COPY matrixark_rust_proxy /usr/local/bin/matrixark_rust_proxy
COPY matrixark_rust_direct_sdk /usr/local/bin/matrixark_rust_direct_sdk
RUN chmod +x /usr/local/bin/matrixark_rust_*
VOLUME ["/var/lib/temporalstore"]
EXPOSE 17101 17102
CMD ["bash", "-lc", "mkdir -p /var/lib/temporalstore/cache /var/lib/temporalstore/pages /var/lib/temporalstore/indexes /var/lib/temporalstore/replica-replay-cursors /var/lib/temporalstore/logs; TS_META_BIND_ADDR=0.0.0.0:17101 TS_META_ADDR=127.0.0.1:17101 matrixark_rust_metaserver > /var/lib/temporalstore/logs/metaserver.log 2>&1 & sleep 1; TS_META_ADDR=127.0.0.1:17101 TS_SERVER_BIND_ADDR=0.0.0.0:17102 TS_SERVER_ADDR=127.0.0.1:17102 TS_SERVER_ADVERTISE_ADDR=127.0.0.1:17102 TS_CACHE_DIR=/var/lib/temporalstore/cache TS_PAGE_STORE_DIR=/var/lib/temporalstore/pages TS_INDEX_DIR=/var/lib/temporalstore/indexes TS_REPLICA_REPLAY_CURSOR_DIR=/var/lib/temporalstore/replica-replay-cursors TS_CACHE_MEMORY_BYTES=67108864 matrixark_rust_datanode > /var/lib/temporalstore/logs/datanode.log 2>&1 & tail -f /var/lib/temporalstore/logs/metaserver.log /var/lib/temporalstore/logs/datanode.log"]
'@
Set-Content -LiteralPath (Join-Path $ctx 'Dockerfile') -Value $dockerfile -Encoding ascii
```

Build the image:

```powershell
& $docker build -t matrixark-temporalstore-rust:win-local $ctx
```

## Run TemporalStore

Run the container with a persistent Docker volume:

```powershell
$dockerBin='C:\Program Files\Docker\Docker\resources\bin'
$docker=Join-Path $dockerBin 'docker.exe'
$env:PATH="$dockerBin;$env:PATH"

$existing = & $docker ps -a --filter name=temporalstore-rust-win --format '{{.Names}}'
if ($existing -contains 'temporalstore-rust-win') {
  & $docker stop temporalstore-rust-win | Out-Null
  & $docker rm temporalstore-rust-win | Out-Null
}

& $docker volume create temporalstore-rust-win-data | Out-Null
& $docker run -d `
  --name temporalstore-rust-win `
  -p 17101:17101 `
  -p 17102:17102 `
  -v temporalstore-rust-win-data:/var/lib/temporalstore `
  matrixark-temporalstore-rust:win-local
```

Service ports:

```text
Rust metaserver: http://127.0.0.1:17101
Rust datanode:   http://127.0.0.1:17102
```

## Validate Health

```powershell
(Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:17101/health' -TimeoutSec 10).Content
(Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:17102/health' -TimeoutSec 10).Content
(Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:17102/server/info' -TimeoutSec 10).Content
```

Expected health response:

```json
{"ok":true,"code":"ok","message":""}
```

Expected logs:

```powershell
& $docker logs --tail 120 temporalstore-rust-win
```

The datanode should report that it registered with the metaserver:

```text
registered server 127.0.0.1:17102 with metaserver 127.0.0.1:17101
registered shard 1 with metaserver 127.0.0.1:17101
temporalstore server listening on 0.0.0.0:17102
```

## Smoke Write And Read

Write a string record:

```powershell
$bytes = [byte[]][char[]]'temporalstore-rust-win-ok'
$write = @{
  shard_id = 1
  command = @{
    kind = 'string_set'
    key = 'windows-docker-smoke'
    value = $bytes
  }
} | ConvertTo-Json -Depth 6 -Compress

(Invoke-WebRequest `
  -UseBasicParsing `
  -Uri 'http://127.0.0.1:17102/execute' `
  -Method POST `
  -ContentType 'application/json' `
  -Body $write `
  -TimeoutSec 10).Content
```

Read it back:

```powershell
$read = @{
  shard_id = 1
  command = @{
    kind = 'string_get'
    key = 'windows-docker-smoke'
  }
} | ConvertTo-Json -Depth 6 -Compress

(Invoke-WebRequest `
  -UseBasicParsing `
  -Uri 'http://127.0.0.1:17102/execute' `
  -Method POST `
  -ContentType 'application/json' `
  -Body $read `
  -TimeoutSec 10).Content
```

Expected read response:

```json
{
  "status": {"ok": true, "code": "ok", "message": ""},
  "response": {
    "kind": "bytes",
    "value": [116,101,109,112,111,114,97,108,115,116,111,114,101,45,114,117,115,116,45,119,105,110,45,111,107]
  }
}
```

## Validate Persistence Across Restart

```powershell
& $docker restart temporalstore-rust-win
Start-Sleep -Seconds 6

(Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:17101/health' -TimeoutSec 10).Content
(Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:17102/health' -TimeoutSec 10).Content
(Invoke-WebRequest `
  -UseBasicParsing `
  -Uri 'http://127.0.0.1:17102/execute' `
  -Method POST `
  -ContentType 'application/json' `
  -Body $read `
  -TimeoutSec 10).Content
```

The `windows-docker-smoke` value should still be present after restart because
the container uses the `temporalstore-rust-win-data` Docker volume.

## Operations

```powershell
& $docker ps --filter name=temporalstore-rust-win
& $docker logs -f temporalstore-rust-win
& $docker exec temporalstore-rust-win bash -lc 'pgrep -af matrixark_rust'
& $docker exec temporalstore-rust-win bash -lc 'ls -lh /var/lib/temporalstore'
& $docker image ls matrixark-temporalstore-rust
```

Stop the service:

```powershell
& $docker stop temporalstore-rust-win
```

Remove the container but keep data:

```powershell
& $docker rm temporalstore-rust-win
```

Remove the persistent data volume:

```powershell
& $docker volume rm temporalstore-rust-win-data
```

## C++ TemporalStore Notes

The same Windows Docker pattern can package C++ release artifacts, but the C++
runtime has additional shared library and dependency wiring requirements. Use a
matched release bundle from the same source tree to avoid `.so` or ABI drift.

Required C++ artifacts generally include:

```text
bcache2-metaserver
bcache2-server
libbcache2.so
the matching dependency shared libraries
```

For C++ parity testing, build and package from:

```text
/root/src/github-services/TemporalStore
```

Do not mix a Python runner from one tree with C++ `.so` files from another tree.
That mismatch has caused previous Windows/WSL comparison crashes.

## Troubleshooting

If `docker` is not found, prepend Docker Desktop's binary path:

```powershell
$dockerBin='C:\Program Files\Docker\Docker\resources\bin'
$env:PATH="$dockerBin;$env:PATH"
```

If Docker reports credential helper errors, use the full Docker Desktop binary
path and make sure it is in `PATH`:

```powershell
& 'C:\Program Files\Docker\Docker\resources\bin\docker.exe' info
```

If a WSL repo bind mount fails from Windows Docker, copy only release binaries
into a Windows temp build context as shown above. That avoids WSL distro mount
service failures and keeps the Docker runtime self-contained.

If WSL reports localhost proxy warnings, it does not block this Windows Docker
runtime. Docker Desktop itself should report its proxy settings through:

```powershell
& $docker info
```
