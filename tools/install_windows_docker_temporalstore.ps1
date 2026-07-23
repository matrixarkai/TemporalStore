param(
    [string]$WslDistro = "Ubuntu2204Deeproute",
    [string]$RepoPath = "/root/src/github-services/TemporalStore",
    [string]$ImageName = "matrixark-temporalstore-rust:win-local",
    [string]$ContainerName = "temporalstore-rust-win",
    [string]$VolumeName = "temporalstore-rust-win-data",
    [int]$MetaPort = 17101,
    [int]$DataPort = 17102,
    [int]$CacheMemoryBytes = 67108864,
    [switch]$InstallDockerDesktop,
    [switch]$BuildReleaseBinaries,
    [switch]$SkipImageBuild,
    [switch]$SkipRun,
    [switch]$SkipSmoke,
    [switch]$NoRestartPersistenceCheck
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "== $Message ==" -ForegroundColor Cyan
}

function Invoke-Checked {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$Label
    )
    Write-Host "+ $FilePath $($Arguments -join ' ')"
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

function Resolve-Docker {
    $docker = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($docker) {
        return $docker.Source
    }
    $dockerDesktopBin = "C:\Program Files\Docker\Docker\resources\bin"
    $dockerDesktopExe = Join-Path $dockerDesktopBin "docker.exe"
    if (Test-Path $dockerDesktopExe) {
        $env:PATH = "$dockerDesktopBin;$env:PATH"
        return $dockerDesktopExe
    }
    throw "docker.exe was not found. Re-run with -InstallDockerDesktop, then start Docker Desktop."
}

function Install-DockerDesktopIfRequested {
    if (-not $InstallDockerDesktop) {
        return
    }
    Write-Step "Install Docker Desktop"
    $winget = Get-Command winget.exe -ErrorAction SilentlyContinue
    if (-not $winget) {
        throw "winget.exe was not found; install Docker Desktop manually from Docker, then re-run this script."
    }
    if (-not $env:HTTP_PROXY) {
        $env:HTTP_PROXY = "http://127.0.0.1:7892"
    }
    if (-not $env:HTTPS_PROXY) {
        $env:HTTPS_PROXY = "http://127.0.0.1:7892"
    }
    Invoke-Checked $winget.Source @(
        "install",
        "--id",
        "Docker.DockerDesktop",
        "--accept-package-agreements",
        "--accept-source-agreements",
        "--silent",
        "--disable-interactivity"
    ) "Docker Desktop install"
}

function Start-DockerDesktop {
    $desktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (Test-Path $desktop) {
        $docker = Get-Command docker.exe -ErrorAction SilentlyContinue
        if (-not $docker) {
            $env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:PATH"
        }
        Start-Process $desktop -WindowStyle Hidden
    }
}

function Wait-DockerReady {
    param([string]$Docker)
    Write-Step "Wait for Docker Desktop engine"
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        try {
            & $Docker info *> $null
            if ($LASTEXITCODE -eq 0) {
                & $Docker version
                return
            }
        } catch {
        }
        Start-Sleep -Seconds 2
    }
    throw "Docker Desktop engine did not become ready within 120 seconds."
}

function Invoke-Wsl {
    param([string]$Command)
    Invoke-Checked "wsl.exe" @("-d", $WslDistro, "--", "bash", "-lc", $Command) "WSL command"
}

function Convert-WslPathToUnc {
    param([string]$Path)
    $trimmed = $Path.TrimStart("/")
    $windowsPath = $trimmed -replace "/", "\"
    return "\\wsl.localhost\$WslDistro\$windowsPath"
}

function Build-ReleaseBinariesIfRequested {
    if (-not $BuildReleaseBinaries) {
        return
    }
    Write-Step "Build Rust TemporalStore release binaries"
    Invoke-Wsl "cd '$RepoPath' && git fetch origin main && cargo build --release -p temporalstore-rust --bins"
}

function Assert-ReleaseBinaries {
    Write-Step "Validate release binaries"
    $releaseDir = Convert-WslPathToUnc "$RepoPath/target/release"
    $required = @(
        "matrixark_rust_metaserver",
        "matrixark_rust_datanode",
        "matrixark_rust_proxy",
        "matrixark_rust_direct_sdk"
    )
    foreach ($name in $required) {
        $path = Join-Path $releaseDir $name
        if (-not (Test-Path $path)) {
            throw "Missing $path. Re-run with -BuildReleaseBinaries."
        }
        $item = Get-Item -LiteralPath $path
        Write-Host "$name $([math]::Round($item.Length / 1MB, 2)) MiB"
    }
    return $releaseDir
}

function New-DockerContext {
    param([string]$ReleaseDir)
    Write-Step "Create Windows Docker build context"
    $context = Join-Path $env:TEMP ("temporalstore-rust-win-docker-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force $context | Out-Null
    foreach ($name in @(
        "matrixark_rust_metaserver",
        "matrixark_rust_datanode",
        "matrixark_rust_proxy",
        "matrixark_rust_direct_sdk"
    )) {
        Copy-Item -LiteralPath (Join-Path $ReleaseDir $name) -Destination (Join-Path $context $name) -Force
    }
    $dockerfile = @"
FROM ubuntu:22.04
COPY matrixark_rust_metaserver /usr/local/bin/matrixark_rust_metaserver
COPY matrixark_rust_datanode /usr/local/bin/matrixark_rust_datanode
COPY matrixark_rust_proxy /usr/local/bin/matrixark_rust_proxy
COPY matrixark_rust_direct_sdk /usr/local/bin/matrixark_rust_direct_sdk
RUN chmod +x /usr/local/bin/matrixark_rust_*
VOLUME ["/var/lib/temporalstore"]
EXPOSE $MetaPort $DataPort
CMD ["bash", "-lc", "mkdir -p /var/lib/temporalstore/cache /var/lib/temporalstore/pages /var/lib/temporalstore/indexes /var/lib/temporalstore/replica-replay-cursors /var/lib/temporalstore/logs; TS_META_BIND_ADDR=0.0.0.0:$MetaPort TS_META_ADDR=127.0.0.1:$MetaPort matrixark_rust_metaserver > /var/lib/temporalstore/logs/metaserver.log 2>&1 & sleep 1; TS_META_ADDR=127.0.0.1:$MetaPort TS_SERVER_BIND_ADDR=0.0.0.0:$DataPort TS_SERVER_ADDR=127.0.0.1:$DataPort TS_SERVER_ADVERTISE_ADDR=127.0.0.1:$DataPort TS_CACHE_DIR=/var/lib/temporalstore/cache TS_PAGE_STORE_DIR=/var/lib/temporalstore/pages TS_INDEX_DIR=/var/lib/temporalstore/indexes TS_REPLICA_REPLAY_CURSOR_DIR=/var/lib/temporalstore/replica-replay-cursors TS_CACHE_MEMORY_BYTES=$CacheMemoryBytes matrixark_rust_datanode > /var/lib/temporalstore/logs/datanode.log 2>&1 & tail -f /var/lib/temporalstore/logs/metaserver.log /var/lib/temporalstore/logs/datanode.log"]
"@
    Set-Content -LiteralPath (Join-Path $context "Dockerfile") -Value $dockerfile -Encoding ascii
    Write-Host "Docker context: $context"
    return $context
}

function Build-DockerImage {
    param(
        [string]$Docker,
        [string]$Context
    )
    if ($SkipImageBuild) {
        Write-Step "Skip Docker image build"
        return
    }
    Write-Step "Build Docker image $ImageName"
    Invoke-Checked $Docker @("build", "-t", $ImageName, $Context) "Docker image build"
}

function Run-TemporalStoreContainer {
    param([string]$Docker)
    if ($SkipRun) {
        Write-Step "Skip container run"
        return
    }
    Write-Step "Run TemporalStore container"
    $existing = & $Docker ps -a --filter "name=$ContainerName" --format "{{.Names}}"
    if ($existing -contains $ContainerName) {
        Invoke-Checked $Docker @("stop", $ContainerName) "Docker stop"
        Invoke-Checked $Docker @("rm", $ContainerName) "Docker rm"
    }
    Invoke-Checked $Docker @("volume", "create", $VolumeName) "Docker volume create"
    Invoke-Checked $Docker @(
        "run",
        "-d",
        "--name",
        $ContainerName,
        "-p",
        "$MetaPort`:$MetaPort",
        "-p",
        "$DataPort`:$DataPort",
        "-v",
        "$VolumeName`:/var/lib/temporalstore",
        $ImageName
    ) "Docker run"
    Start-Sleep -Seconds 5
    & $Docker ps --filter "name=$ContainerName" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    & $Docker logs --tail 80 $ContainerName
}

function Invoke-JsonPost {
    param(
        [string]$Uri,
        [object]$Body
    )
    $json = $Body | ConvertTo-Json -Depth 12 -Compress
    return (Invoke-WebRequest -UseBasicParsing -Uri $Uri -Method POST -ContentType "application/json" -Body $json -TimeoutSec 10).Content
}

function Invoke-SmokeValidation {
    param([string]$Docker)
    if ($SkipSmoke) {
        Write-Step "Skip smoke validation"
        return
    }
    Write-Step "Validate health and smoke write/read"
    $metaHealth = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$MetaPort/health" -TimeoutSec 10).Content
    $dataHealth = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$DataPort/health" -TimeoutSec 10).Content
    Write-Host "metaserver health: $metaHealth"
    Write-Host "datanode health: $dataHealth"

    $value = "temporalstore-rust-win-ok"
    $bytes = [byte[]][char[]]$value
    $write = @{
        shard_id = 1
        command = @{
            kind = "string_set"
            key = "windows-docker-smoke"
            value = $bytes
        }
    }
    $read = @{
        shard_id = 1
        command = @{
            kind = "string_get"
            key = "windows-docker-smoke"
        }
    }
    $writeResponse = Invoke-JsonPost "http://127.0.0.1:$DataPort/execute" $write
    $readResponse = Invoke-JsonPost "http://127.0.0.1:$DataPort/execute" $read
    Write-Host "write response: $writeResponse"
    Write-Host "read response:  $readResponse"
    if ($readResponse -notmatch "116,101,109,112,111,114,97,108,115,116,111,114,101") {
        throw "Smoke read did not return the expected TemporalStore value."
    }

    if (-not $NoRestartPersistenceCheck) {
        Write-Step "Validate persistence after restart"
        Invoke-Checked $Docker @("restart", $ContainerName) "Docker restart"
        Start-Sleep -Seconds 6
        $afterRestart = Invoke-JsonPost "http://127.0.0.1:$DataPort/execute" $read
        Write-Host "post-restart read response: $afterRestart"
        if ($afterRestart -notmatch "116,101,109,112,111,114,97,108,115,116,111,114,101") {
            throw "Post-restart read did not return the expected TemporalStore value."
        }
    }
}

Install-DockerDesktopIfRequested
Start-DockerDesktop
$docker = Resolve-Docker
Wait-DockerReady $docker
Build-ReleaseBinariesIfRequested
$releaseDir = Assert-ReleaseBinaries
$context = New-DockerContext $releaseDir
Build-DockerImage $docker $context
Run-TemporalStoreContainer $docker
Invoke-SmokeValidation $docker

Write-Step "Windows Docker TemporalStore install complete"
& $docker image ls $ImageName
& $docker ps --filter "name=$ContainerName" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
