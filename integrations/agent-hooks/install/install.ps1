param(
  [ValidateSet("codex", "claude", "both", "generic")]
  [string]$Agent = "codex",
  [ValidateSet("dry-run", "wsl", "native", "remote", "docker")]
  [string]$Mode = "dry-run",
  [string]$Dest = "$env:USERPROFILE\plugins\temporalstore-agent-hooks",
  [string]$Node = $env:CODEX_NODE_PATH,
  [string]$CodexBin = "codex",
  [string]$ClaudeSettings = "$env:USERPROFILE\.claude\settings.json",
  [string]$Endpoint = "http://127.0.0.1:18080",
  [string]$Repo = "/opt/github-services/TemporalStore",
  [string]$WslRepo = "/opt/github-services/TemporalStore",
  [string]$WslDistro = $env:TEMPORALSTORE_WSL_DISTRO,
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
MATRIXARK_MAX_CONTEXT_TOKENS=10000
MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT=40000
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
  # Native-Windows Claude Code runs the hook command through the OS shell, but the
  # hook engine lives in WSL, so each event invokes the WSL wrapper via wsl.exe.
  # The hook JSON on stdin is inherited by wsl.exe -> bash. Full lifecycle, agent=claude,
  # persisting to the rust TemporalStore.
  New-Item -ItemType Directory -Force (Split-Path $ClaudeSettings) | Out-Null
  $wrap = "$WslRepo/tools/matrixark_claude_hook.sh"
  $distroArg = if ($WslDistro) { "-d $WslDistro " } else { "" }
  function New-ClaudeEntry([string]$event, [int]$timeout, [string]$matcher) {
    $cmd = "wsl.exe ${distroArg}-e bash -lc `"bash $wrap --event $event`""
    $hook = [pscustomobject]@{ type = "command"; command = $cmd; timeout = $timeout }
    $entry = [pscustomobject]@{ hooks = @($hook) }
    if ($matcher) { $entry | Add-Member -NotePropertyName matcher -NotePropertyValue $matcher }
    return ,@($entry)
  }
  $settings = if (Test-Path $ClaudeSettings) { Get-Content -Raw $ClaudeSettings | ConvertFrom-Json } else { [pscustomobject]@{} }
  $hooks = [pscustomobject]@{}
  $hooks | Add-Member -NotePropertyName SessionStart     -NotePropertyValue (New-ClaudeEntry "SessionStart" 600 $null)
  $hooks | Add-Member -NotePropertyName UserPromptSubmit -NotePropertyValue (New-ClaudeEntry "UserPromptSubmit" 30 $null)
  $hooks | Add-Member -NotePropertyName PostToolUse      -NotePropertyValue (New-ClaudeEntry "PostToolUse" 120 "*")
  $hooks | Add-Member -NotePropertyName Stop             -NotePropertyValue (New-ClaudeEntry "Stop" 120 $null)
  $hooks | Add-Member -NotePropertyName SubagentStop     -NotePropertyValue (New-ClaudeEntry "SubagentStop" 120 $null)
  $hooks | Add-Member -NotePropertyName PreCompact       -NotePropertyValue (New-ClaudeEntry "PreCompact" 120 $null)
  $hooks | Add-Member -NotePropertyName SessionEnd       -NotePropertyValue (New-ClaudeEntry "SessionEnd" 120 $null)
  if ($settings.PSObject.Properties.Name -contains "hooks") { $settings.hooks = $hooks } else { $settings | Add-Member -NotePropertyName hooks -NotePropertyValue $hooks }
  $settings | ConvertTo-Json -Depth 10 | Set-Content -Path $ClaudeSettings -Encoding ASCII
  Write-Host "Claude settings updated (full lifecycle -> $wrap): $ClaudeSettings"
}

if ($Agent -eq "codex") { Install-CodexPlugin }
elseif ($Agent -eq "claude") { Install-ClaudeHook }
elseif ($Agent -eq "both") { Install-CodexPlugin; Install-ClaudeHook }
else { Write-Host "Generic hook launcher copied to: $Dest" }

Write-Host "Installed TemporalStore agent hooks to: $Dest"
Write-Host "Smoke test: .\integrations\agent-hooks\install\smoke_test.ps1 -Mode $Mode"
