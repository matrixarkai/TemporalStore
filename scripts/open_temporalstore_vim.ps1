$Root = "/root/src/github-services/TemporalStore"
$target = if ($args.Count -gt 0) { $args[0] } else { "cpp" }
$map = @{
  "cpp" = "cpp"
  "c++" = "cpp"
  "cppsrc" = "cpp"
  "rust" = "rust"
  "rs" = "rust"
  "rustsrc" = "rust"
}
if (-not $map.ContainsKey($target.ToLower())) {
  Write-Error "Usage: .\open_temporalstore_vim.ps1 [cpp|rust]"
  exit 1
}
$kind = $map[$target.ToLower()]
wsl.exe -d Ubuntu2204Deeproute -- bash -lc "cd $Root && ./scripts/open_temporalstore_vim.sh $kind"