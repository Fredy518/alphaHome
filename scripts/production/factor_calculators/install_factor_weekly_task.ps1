param(
    [string]$TaskName = 'AlphaHome-Factor-Weekly',
    [string]$At = '08:00',
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
$runner = Join-Path $projectRoot 'scripts\production\factor_calculators\run_factor_weekly.py'

if (-not (Test-Path -LiteralPath $python)) { throw "AlphaHome Python not found: $python" }
if (-not (Test-Path -LiteralPath $runner)) { throw "factor runner not found: $runner" }

$arguments = '"' + $runner + '"'
$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument $arguments `
    -WorkingDirectory $projectRoot
$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -WeeksInterval 1 `
    -DaysOfWeek Saturday `
    -At $At
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 12)
$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Limited
$definition = New-ScheduledTask `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description 'AlphaHome governed weekly P/G factor run and audit; never runs PIT tasks'

if (-not $Apply) {
    [pscustomobject]@{
        TaskName = $TaskName
        Schedule = "Saturday $At (host local time)"
        Program = $python
        Arguments = $arguments
        Workflow = 'factor_p + factor_g + audit; no PIT execution'
        Apply = $false
    } | ConvertTo-Json
    exit 0
}

Register-ScheduledTask -TaskName $TaskName -InputObject $definition -Force | Out-Null
Get-ScheduledTask -TaskName $TaskName |
    Select-Object TaskName, State, TaskPath |
    ConvertTo-Json
