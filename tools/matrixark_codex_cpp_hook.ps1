param(
  [Parameter(ValueFromRemainingArguments=$true)]
  [string[]]$HookArgs
)

$Distro = if ($env:MATRIXARK_WSL_DISTRO) { $env:MATRIXARK_WSL_DISTRO } else { "Ubuntu2204Deeproute" }
$Repo = if ($env:MATRIXARK_REPO_ROOT) { $env:MATRIXARK_REPO_ROOT } else { "/root/src/github-services/TemporalStore" }

$stdin = [Console]::In.ReadToEnd()
$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stdin))
$quotedArgs = ($HookArgs | ForEach-Object { "'" + ($_ -replace "'", "'\''") + "'" }) -join " "

$script = @"
set -euo pipefail
export MATRIXARK_REPO_ROOT='$Repo'
printf '%s' '$encoded' | base64 -d | '$Repo/tools/matrixark_codex_cpp_hook.sh' $quotedArgs
"@

wsl -d $Distro -- bash -lc $script
exit $LASTEXITCODE
