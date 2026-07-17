param(
  [ValidateSet("codex", "claude", "generic")]
  [string]$Agent = "codex",
  [string]$Mode = "dry-run",
  [string]$Dest = "$env:USERPROFILE\plugins\temporalstore-agent-hooks",
  [string]$Node = $env:CODEX_NODE_PATH
)

if (-not $Node) {
  $Node = "node"
}

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$PluginSource = Join-Path $Root "codex\plugin"
New-Item -ItemType Directory -Force $Dest | Out-Null
Copy-Item -Path (Join-Path $PluginSource "*") -Destination $Dest -Recurse -Force

$Template = Join-Path $Dest "hooks\hooks.template.json"
$Hooks = Join-Path $Dest "hooks\hooks.json"
$PluginRoot = $Dest.Replace("\", "/")
$NodeForJson = $Node.Replace("\", "/")
(Get-Content -Raw $Template).
  Replace("__NODE__", $NodeForJson).
  Replace("__PLUGIN_ROOT__", $PluginRoot) |
  Set-Content -Path $Hooks -Encoding ASCII

@"
TEMPORALSTORE_AGENT_MODE=$Mode
TEMPORALSTORE_REPO=/root/src/github-services/TemporalStore
TEMPORALSTORE_AGENT_PROJECT=TemporalStore
"@ | Set-Content -Path (Join-Path $Dest ".env.example") -Encoding ASCII

if ($Agent -eq "codex") {
  Write-Host "Codex plugin copied to: $Dest"
  Write-Host "Add this plugin to a Codex marketplace or install it with Codex plugin tooling."
} elseif ($Agent -eq "claude") {
  Write-Host "Claude settings template: $(Join-Path $Root 'claude\settings.example.json')"
} else {
  Write-Host "Generic hook launcher copied to: $Dest"
}

Write-Host "Run smoke test:"
Write-Host "  .\integrations\agent-hooks\install\smoke_test.ps1 -Mode $Mode"
