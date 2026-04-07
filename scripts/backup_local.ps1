Param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupRoot = Join-Path $repoRoot "backups"
$target = Join-Path $backupRoot $timestamp

New-Item -ItemType Directory -Path $target -Force | Out-Null

$paths = @(
    "data/processed",
    "data/lake"
)

foreach ($p in $paths) {
    $src = Join-Path $repoRoot $p
    if (Test-Path $src) {
        $dst = Join-Path $target $p
        New-Item -ItemType Directory -Path (Split-Path -Parent $dst) -Force | Out-Null
        Copy-Item -Path $src -Destination $dst -Recurse -Force
    }
}

Write-Output "Backup created at: $target"
