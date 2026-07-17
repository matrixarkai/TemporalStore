param(
  [ValidateSet("codex", "claude", "generic")]
  [string]$Agent = "codex",
  [ValidateSet("dry-run", "wsl", "native", "remote", "docker")]
  [string]$Mode = "dry-run",
  [string]$Dest = "$env:USERPROFILE\plugins\temporalstore-agent-hooks",
  [string]$Node = $env:CODEX_NODE_PATH,
  [string]$CodexBin = "codex",
  [string]$ClaudeSettings = "$env:USERPROFILE\.claude\settings.json",
  [string]$Endpoint = "http://127.0.0.1:18080",
  [string]$Repo = "/root/src/github-services/TemporalStore",
  [string]$WslRepo = "/root/src/github-services/TemporalStore",
  [switch]$SkipCodexAdd
)

if (-not $Node) { $Node = "node" }

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$PluginSource = Join-Path $Root "codex\plugin"
New-Item -ItemType Directory -Force $Dest | Out-Null
Copy-Item -Path (Join-Path $PluginSource "*") -Destination $Dest -Recurse -Force

$Template = Join-Path $Dest "hooks\hooks.template.json"
$Hooks = Join-Path $Dest "hooks\hooks.json"
$PluginRoot = $Dest.Replace("\", "/")
$NodeForJson = $Node.Replace("\", "/")
(Get-Content -Raw $Template).Replace("__NODE__", $NodeForJson).Replace("__PLUGIN_ROOT__", $PluginRoot) | Set-Content -Path $Hooks -Encoding ASCII

@"
TEMPORALSTORE_AGENT_MODE=$Mode
TEMPORALSTORE_AGENT_ENDPOINT=$Endpoint
TEMPORALSTORE_REPO=$Repo
TEMPORALSTORE_WSL_REPO=$WslRepo
TEMPORALSTORE_AGENT_PROJECT=TemporalStore
TEMPORALSTORE_AGENT_NAME=$Agent
TEMPORALSTORE_METASERVER=127.0.0.1:18000
TEMPORALSTORE_NAMESPACE=deploy_ns
TEMPORALSTORE_TABLE=deploy_table
TEMPORALSTORE_LIBRARY=output-ubuntu22/release/sdk/lib/libbcache2.so
MATRIXARK_STORAGE_PREFIX=matrixark:agent-hook
MATRIXARK_ACCOUNT_ID=acct_local
MATRIXARK_TENANT_ID=tenant_codex
MATRIXARK_USER_ID=$env:USERNAME
MATRIXARK_TEAM=agent
MATRIXARK_MAX_CONTEXT_TOKENS=2400
"@ | Set-Content -Path (Join-Path $Dest ".env") -Encoding ASCII
Copy-Item (Join-Path $Dest ".env") (Join-Path $Dest ".env.example") -Force

function Install-CodexPlugin {
  $MarketplaceRoot = $env:USERPROFILE
  $MarketplaceFile = Join-Path $MarketplaceRoot ".agents\plugins\marketplace.json"
  New-Item -ItemType Directory -Force (Split-Path $MarketplaceFile) | Out-Null
  New-Item -ItemType Directory -Force (Join-Path $MarketplaceRoot "plugins") | Out-Null
  $FinalDest = Join-Path $MarketplaceRoot "plugins\temporalstore-agent-hooks"
  if ($Dest -ne $FinalDest) {
    if (Test-Path $FinalDest) { Remove-Item -Recurse -Force $FinalDest }
    Copy-Item -Recurse -Force $Dest $FinalDest
  }
  $json = if (Test-Path $MarketplaceFile) { Get-Content -Raw $MarketplaceFile | ConvertFrom-Json } else { [pscustomobject]@{ name="personal"; interface=[pscustomobject]@{displayName="Personal"}; plugins=@() } }
  if (-not $json.plugins) { $json | Add-Member -NotePropertyName plugins -NotePropertyValue @() }
  $entry = [pscustomobject]@{ name="temporalstore-agent-hooks"; source=[pscustomobject]@{source="local"; path="./plugins/temporalstore-agent-hooks"}; policy=[pscustomobject]@{installation="AVAILABLE"; authentication="ON_INSTALL"}; category="Productivity" }
  $plugins = @($json.plugins | Where-Object { $_.name -ne "temporalstore-agent-hooks" }) + $entry
  $json.plugins = $plugins
  $json | ConvertTo-Json -Depth 10 | Set-Content -Path $MarketplaceFile -Encoding ASCII
  Write-Host "Codex marketplace updated: $MarketplaceFile"
  if (-not $SkipCodexAdd) {
    & $CodexBin plugin add temporalstore-agent-hooks@personal
  } else {
    Write-Host "Codex plugin add skipped. Run: $CodexBin plugin add temporalstore-agent-hooks@personal"
  }
}

function Install-ClaudeHook {
  New-Item -ItemType Directory -Force (Split-Path $ClaudeSettings) | Out-Null
  $launcher = (Join-Path $Dest "scripts\temporalstore_hook_launcher.mjs").Replace("\", "/")
  $nodePath = $Node.Replace("\", "/")
  $settings = if (Test-Path $ClaudeSettings) { Get-Content -Raw $ClaudeSettings | ConvertFrom-Json } else { [pscustomobject]@{} }
  $hooks = [pscustomobject]@{}
  $hooks | Add-Member -NotePropertyName UserPromptSubmit -NotePropertyValue @([pscustomobject]@{ hooks=@([pscustomobject]@{ type="command"; command="`"$nodePath`" `"$launcher`" UserPromptSubmit" }) })
  $hooks | Add-Member -NotePropertyName Stop -NotePropertyValue @([pscustomobject]@{ hooks=@([pscustomobject]@{ type="command"; command="`"$nodePath`" `"$launcher`" Stop" }) })
  if ($settings.PSObject.Properties.Name -contains "hooks") { $settings.hooks = $hooks } else { $settings | Add-Member -NotePropertyName hooks -NotePropertyValue $hooks }
  $settings | ConvertTo-Json -Depth 10 | Set-Content -Path $ClaudeSettings -Encoding ASCII
  Write-Host "Claude settings updated: $ClaudeSettings"
}

if ($Agent -eq "codex") { Install-CodexPlugin }
elseif ($Agent -eq "claude") { Install-ClaudeHook }
else { Write-Host "Generic hook launcher copied to: $Dest" }

Write-Host "Installed TemporalStore agent hooks to: $Dest"
Write-Host "Smoke test: .\integrations\agent-hooks\install\smoke_test.ps1 -Mode $Mode"
