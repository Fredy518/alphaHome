param(
    [string]$Config = "$HOME\.alphahome\fundpos_production.json",
    [ValidateSet('check', 'shadow', 'publish')]
    [string]$Mode = 'shadow',
    [string]$TaskName = 'AlphaHome-Fundpos-Shadow',
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
$runner = Join-Path $projectRoot 'scripts\production\fundpos\run_fundpos_daily.py'
$configPath = (Resolve-Path -LiteralPath $Config).Path

if (-not (Test-Path -LiteralPath $python)) { throw "AlphaHome Python not found: $python" }
if (-not (Test-Path -LiteralPath $runner)) { throw "fundpos runner not found: $runner" }

$arguments = '"' + $runner + '" --config "' + $configPath + '" --mode ' + $Mode
$action = New-ScheduledTaskAction -Execute $python -Argument $arguments -WorkingDirectory $projectRoot
$weekdays = @('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
$triggers = @(
    New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $weekdays -At '09:00'
    New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $weekdays -At '12:00'
    New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $weekdays -At '18:00'
)
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive -RunLevel Limited
$definition = New-ScheduledTask -Action $action -Trigger $triggers -Settings $settings `
    -Principal $principal -Description 'AlphaHome fundpos fail-closed shadow production pipeline'

if (-not $Apply) {
    [pscustomobject]@{
        TaskName = $TaskName
        Mode = $Mode
        Schedule = 'Weekdays 09:00, 12:00, 18:00 Asia/Shanghai host time'
        Program = $python
        Arguments = $arguments
        Apply = $false
    } | ConvertTo-Json
    exit 0
}

Register-ScheduledTask -TaskName $TaskName -InputObject $definition -Force | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State, TaskPath | ConvertTo-Json
