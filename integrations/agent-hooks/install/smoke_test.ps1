param(
  [string]$Mode = $env:TEMPORALSTORE_AGENT_MODE
)

if (-not $Mode) {
  $Mode = "dry-run"
}

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Node = $env:CODEX_NODE_PATH
if (-not $Node) {
  $Node = "node"
}

$Marker = "TEMPORALSTORE_AGENT_HOOK_SMOKE_$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())"
$Payload = @{
  prompt = $Marker
  session_id = "smoke-session-$Marker"
  cwd = $Root.Path
} | ConvertTo-Json -Compress

$env:TEMPORALSTORE_AGENT_MODE = $Mode
$Payload | & $Node (Join-Path $Root "codex/plugin/scripts/temporalstore_hook_launcher.mjs") UserPromptSubmit
