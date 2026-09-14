param(
    [string]$Date = 'latest',
    [string]$Cutoff = '',
    [switch]$Observe
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectRoot
try {
    $env:PYTHONUTF8 = '1'
    $logDirectory = [System.IO.Path]::GetFullPath(
        (Join-Path $projectRoot '..\..\logs\fundpos-engine\engine-logs')
    )
    New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
    $logPath = Join-Path $logDirectory ('daily_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '.log')
    $runArguments = @('run', '--frozen', 'fundpos', 'estimate', '--date', $Date)
    if ($Cutoff) { $runArguments += @('--cutoff', $Cutoff) }
    if ($Observe) { $runArguments += '--observe' }
    & uv @runArguments 2>&1 | Tee-Object -FilePath $logPath
    $runExit = $LASTEXITCODE
    exit $runExit
}
finally { Pop-Location }
