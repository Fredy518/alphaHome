<#
.SYNOPSIS
One-click import Hikyuu 5min HDF5 -> DolphinDB, with optional DFS backup.

.DESCRIPTION
- Generates/updates `scripts/tickers/*.txt` (by market and first digit)
- Initializes `dfs://kline_5min` partitioned table (optional)
- Imports tickers list files sequentially
- Optionally backs up local DolphinDB DFS directory for `dfs://kline_5min`

Typical usage (PowerShell):
  ./scripts/import_all_hikyuu_to_ddb.ps1 -InitTable
  ./scripts/import_all_hikyuu_to_ddb.ps1 -Group sh -Group sz -InitTable
  ./scripts/import_all_hikyuu_to_ddb.ps1 -Backup -DfsRoot "D:/dolphindb/server/data/dfs"

Scheduling:
  Use Windows Task Scheduler to run this script weekly for backups, and daily for incremental imports.
#>

[CmdletBinding()]
param(
  [string]$HikyuuDataDir = $env:HIKYUU_DATA_DIR,
  [ValidateSet("sh","sz","bj","all")]
  [string[]]$Group = @("all"),
  [switch]$InitTable,
  [switch]$DryRun,
  [switch]$Incremental,
  [switch]$ResetDb,
  [string]$CodesFile = "",
  [string]$DbPath = "dfs://kline_5min",
  [string]$Table = "kline_5min",
  [int]$ChunkRows = 200000,
  [string]$Start = "",
  [string]$End = "",

  [switch]$Backup,
  [string]$DfsRoot = $env:DOLPHINDB_DFS_ROOT,
  [string]$DfsDbName = "kline_5min",
  [string]$BackupBaseDir = ""
)

$ErrorActionPreference = "Stop"

function Resolve-RepoRoot {
  $here = Split-Path -Parent $PSCommandPath
  return (Resolve-Path (Join-Path $here "..")).Path
}

function Get-Python {
  $py = Get-Command python -ErrorAction SilentlyContinue
  if ($null -ne $py) { return $py.Source }
  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($null -ne $py) { return $py.Source }
  throw "Python not found. Install Python and ensure it's on PATH."
}

function Invoke-AlphahomeDdb {
  param([Parameter(Mandatory=$true)][string[]]$Args)
  if ($DryRun) {
    Write-Host ("[dry-run] alphahome-ddb " + ($Args -join " "))
    return
  }
  $cmd = Get-Command alphahome-ddb -ErrorAction SilentlyContinue
  if ($null -ne $cmd) {
    & $cmd.Source @Args
    return
  }
  $python = Get-Python
  & $python -m alphahome.integrations.dolphindb.cli @Args
}

function Ensure-Tickers {
  param(
    [Parameter(Mandatory=$true)][string]$RepoRoot,
    [Parameter(Mandatory=$true)][string]$HikyuuDir
  )
  if ($DryRun) {
    Write-Host "[dry-run] generate tickers lists"
    return
  }
  $python = Get-Python
  $gen = Join-Path $RepoRoot "scripts/generate_hikyuu_5min_tickers.py"
  & $python $gen --hikyuu-dir $HikyuuDir --output-dir (Join-Path $RepoRoot "scripts/tickers")
}

function Resolve-BackupBaseDir {
  param([string]$RepoRoot, [string]$HikyuuDir, [string]$Explicit)
  if ($Explicit -and $Explicit.Trim()) { return $Explicit }
  if ($HikyuuDir -and $HikyuuDir.Trim()) {
    return (Join-Path $HikyuuDir "backup/dolphindb")
  }
  return (Join-Path $RepoRoot "backup/dolphindb")
}

function Backup-DfsDb {
  param(
    [Parameter(Mandatory=$true)][string]$DfsRootDir,
    [Parameter(Mandatory=$true)][string]$DfsDb,
    [Parameter(Mandatory=$true)][string]$BackupRoot
  )
  if (-not (Test-Path $DfsRootDir)) {
    throw "DfsRoot not found: $DfsRootDir. Set -DfsRoot or env DOLPHINDB_DFS_ROOT (e.g. D:/dolphindb/server/data/dfs)."
  }

  $src = Join-Path $DfsRootDir $DfsDb
  if (-not (Test-Path $src)) {
    throw "DFS db directory not found: $src"
  }

  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $dst = Join-Path $BackupRoot ("dfs_" + $DfsDb + "_" + $stamp)
  New-Item -ItemType Directory -Force -Path $dst | Out-Null

  Write-Host "[backup] $src -> $dst"
  if ($DryRun) { return }
  # Robust copy (keeps timestamps, retries on transient locks)
  robocopy $src $dst /E /R:3 /W:2 /NFL /NDL /NP | Out-Null
}

$repoRoot = Resolve-RepoRoot
Set-Location $repoRoot

if (-not $HikyuuDataDir -or -not $HikyuuDataDir.Trim()) {
  $HikyuuDataDir = "E:/stock"
}

Ensure-Tickers -RepoRoot $repoRoot -HikyuuDir $HikyuuDataDir

if ($ResetDb) {
  Invoke-AlphahomeDdb @(
    "drop-db",
    "--db-path", $DbPath,
    "--yes"
  )
  $InitTable = $true
}

if ($InitTable) {
  Invoke-AlphahomeDdb @(
    "init-kline5m",
    "--db-path", $DbPath,
    "--table", $Table
  )
}

if ($CodesFile -and $CodesFile.Trim()) {
  $resolved = Resolve-Path $CodesFile -ErrorAction Stop
  Write-Host "[import] codes-file=$resolved"
  $args = @(
    "import-hikyuu-5min",
    "--codes-file", $resolved.Path,
    "--chunk-rows", $ChunkRows.ToString()
  )
  if ($Start -and $Start.Trim()) { $args += @("--start", $Start) }
  if ($End -and $End.Trim()) { $args += @("--end", $End) }
  if ($Incremental) { $args += @("--incremental") }
  Invoke-AlphahomeDdb $args
} else {
  $groups = $Group | Select-Object -Unique
  if ($groups -contains "all") {
    # Full market should run once.
    $groups = @("all")
  }

  foreach ($g in $groups) {
    if ($g -eq "all") {
      $path = Join-Path $repoRoot "scripts/tickers/all.txt"
    } elseif ($g -in @("sh","sz","bj")) {
      $path = Join-Path $repoRoot ("scripts/tickers/" + $g + "_all.txt")
    } else {
      throw "Unsupported group: $g"
    }

    if (-not (Test-Path $path)) {
      throw "Tickers file not found: $path (run scripts/generate_hikyuu_5min_tickers.py first)"
    }

    Write-Host "[import] group=$g file=$(Split-Path -Leaf $path)"
    $args = @(
      "import-hikyuu-5min",
      "--codes-file", $path,
      "--chunk-rows", $ChunkRows.ToString()
    )
    if ($Start -and $Start.Trim()) { $args += @("--start", $Start) }
    if ($End -and $End.Trim()) { $args += @("--end", $End) }
    if ($Incremental) { $args += @("--incremental") }
    Invoke-AlphahomeDdb $args
  }
}

if ($Backup) {
  $backupRoot = Resolve-BackupBaseDir -RepoRoot $repoRoot -HikyuuDir $HikyuuDataDir -Explicit $BackupBaseDir
  New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null
  Backup-DfsDb -DfsRootDir $DfsRoot -DfsDb $DfsDbName -BackupRoot $backupRoot
}

Write-Host "Done."
