Param(
    [Parameter(Mandatory = $true)]
    [string]$BackupDir
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot

if (-not (Test-Path $BackupDir)) {
    throw "Backup directory not found: $BackupDir"
}

$restoreTargets = @(
    "data/processed",
    "data/lake"
)

foreach ($p in $restoreTargets) {
    $src = Join-Path $BackupDir $p
    $dst = Join-Path $repoRoot $p
    if (Test-Path $src) {
        if (Test-Path $dst) {
            Remove-Item -Path $dst -Recurse -Force
        }
        New-Item -ItemType Directory -Path (Split-Path -Parent $dst) -Force | Out-Null
        Copy-Item -Path $src -Destination $dst -Recurse -Force
    }
}

Write-Output "Restore completed from: $BackupDir"
