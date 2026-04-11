param(
    [int]$IngestionIntervalMinutes = 60,
    [int]$FeedsIntervalMinutes = 30,
    [switch]$Unregister
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
$taskName = "PranagA-Team1-Scheduler"

if ($Unregister) {
    schtasks /Delete /TN $taskName /F | Out-Null
    Write-Host "Removed scheduled task: $taskName"
    exit 0
}

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found at $pythonExe"
}

$taskCommand = '"{0}" -m universal_index.scheduler --interval-minutes {1} --feeds-interval-minutes {2}' -f $pythonExe, $IngestionIntervalMinutes, $FeedsIntervalMinutes

schtasks /Create /TN $taskName /SC MINUTE /MO 1 /TR $taskCommand /F | Out-Null
Write-Host "Registered scheduled task: $taskName"
Write-Host "Command: $taskCommand"
