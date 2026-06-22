param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$thirdparty = Join-Path $repo "thirdparty"

function FirstExistingPath([string[]]$Candidates) {
    foreach ($candidate in $Candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    return $null
}

function LinkDependency([string]$Name, [string[]]$Candidates) {
    $target = FirstExistingPath $Candidates
    if (-not $target) {
        Write-Host "MISSING $Name"
        foreach ($candidate in $Candidates) {
            if ($candidate) {
                Write-Host "  tried: $candidate"
            }
        }
        return $false
    }

    $link = Join-Path $thirdparty $Name
    if (Test-Path -LiteralPath $link) {
        if (-not $Force) {
            Write-Host "EXISTS  $Name -> $link"
            return $true
        }
        Remove-Item -LiteralPath $link -Force -Recurse
    }

    try {
        New-Item -ItemType SymbolicLink -Path $link -Target $target | Out-Null
    } catch [System.UnauthorizedAccessException] {
        New-Item -ItemType Junction -Path $link -Target $target | Out-Null
    }
    Write-Host "LINKED  $Name -> $target"
    return $true
}

New-Item -ItemType Directory -Path $thirdparty -Force | Out-Null

$downloads = Join-Path $env:USERPROFILE "Downloads"
$documents = [Environment]::GetFolderPath("MyDocuments")
$workspace = Split-Path -Parent $repo

$deps = @(
    @{ Name = "byte"; Candidates = @(
        $env:TS_BYTE_SRC,
        (Join-Path $downloads "bytekv-master\bytekv-master\third\byte")
    ) },
    @{ Name = "byteraft"; Candidates = @(
        $env:TS_BYTERAFT_SRC,
        (Join-Path $downloads "bytekv-master\bytekv-master\third\byteraft")
    ) },
    @{ Name = "boost"; Candidates = @(
        $env:TS_BOOST_SRC,
        (Join-Path $downloads "ByteHTAP-bytehtap_2.0_beta_release\ByteHTAP-bytehtap_2.0_beta_release\src\thirdparty\boost")
    ) },
    @{ Name = "rapidjson"; Candidates = @(
        $env:TS_RAPIDJSON_SRC,
        (Join-Path $downloads "ByteHTAP-bytehtap_2.0_beta_release\ByteHTAP-bytehtap_2.0_beta_release\src\thirdparty\rapidjson")
    ) },
    @{ Name = "mtcache"; Candidates = @(
        $env:TS_MTCACHE_SRC,
        (Join-Path $documents "Codex\temporalstore-small\dependencies\mtcache"),
        (Join-Path $workspace "..\..\temporalstore-small\dependencies\mtcache")
    ) }
)

$ok = $true
foreach ($dep in $deps) {
    if (-not (LinkDependency $dep.Name $dep.Candidates)) {
        $ok = $false
    }
}

if (-not $ok) {
    throw "Some local dependencies are missing. Set TS_*_SRC variables or place the dependency beside the known local downloads. Dependency payloads are ignored by Git and must not be committed."
}

Write-Host "Local thirdparty bootstrap complete. Do not git add dependency payloads."
