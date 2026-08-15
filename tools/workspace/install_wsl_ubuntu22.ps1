$ErrorActionPreference = "Continue"

$LogPath = Join-Path $PSScriptRoot "wsl_install_ubuntu22.log"
Start-Transcript -Path $LogPath -Append

Write-Host "=== TemporalStore WSL2 Ubuntu 22.04 setup ==="
Write-Host "Log: $LogPath"
Write-Host "Started: $(Get-Date -Format o)"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
Write-Host "Administrator: $isAdmin"

Write-Host ""
Write-Host "Step 1: Enable WSL optional component"
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
Write-Host "WSL feature exit code: $LASTEXITCODE"

Write-Host ""
Write-Host "Step 2: Enable Virtual Machine Platform optional component"
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
Write-Host "VirtualMachinePlatform feature exit code: $LASTEXITCODE"

Write-Host ""
Write-Host "Step 3: Show WSL status"
wsl.exe --status
Write-Host "wsl --status exit code: $LASTEXITCODE"

Write-Host ""
Write-Host "Step 4: Update WSL kernel/package"
wsl.exe --update
Write-Host "wsl --update exit code: $LASTEXITCODE"

Write-Host ""
Write-Host "Step 5: Set default WSL version to 2"
wsl.exe --set-default-version 2
Write-Host "wsl --set-default-version exit code: $LASTEXITCODE"

Write-Host ""
Write-Host "Step 6: Install Ubuntu 22.04"
wsl.exe --install -d Ubuntu-22.04 --no-launch
Write-Host "wsl install Ubuntu-22.04 exit code: $LASTEXITCODE"

Write-Host ""
Write-Host "Step 7: List distros"
wsl.exe -l -v
Write-Host "wsl -l -v exit code: $LASTEXITCODE"

Write-Host ""
Write-Host "Finished: $(Get-Date -Format o)"
Write-Host "If the output says restart required, restart Windows, reopen Codex, and I will continue the build."

Stop-Transcript
Read-Host "Press Enter to close"
