@echo off
REM PIT数据更新批处理启动器
REM 使用方法：
REM start_pit_data_update.bat [target] [mode] [parallel]
REM
REM 参数说明：
REM target: 要更新的数据类型 (balance/income/financial_indicators/industry_classification/all)
REM mode: 更新模式 (incremental/full)
REM parallel: 是否并行执行 (true/false)

setlocal enabledelayedexpansion

REM 设置默认参数
if "%1"=="" (
    set TARGET=all
) else (
    set TARGET=%1
)

if "%2"=="" (
    set MODE=incremental
) else (
    set MODE=%2
)

if "%3"=="" (
    set PARALLEL=false
) else (
    set PARALLEL=%3
)

REM 设置Python路径
set PYTHONPATH=%~dp0..\..\..\..;%PYTHONPATH%

echo ========================================
echo PIT数据更新启动器
echo ========================================
echo 目标数据类型: %TARGET%
echo 更新模式: %MODE%
echo 并行执行: %PARALLEL%
echo ========================================

REM 构建Python命令
set PYTHON_CMD=python scripts\production\data_updaters\pit\pit_data_update_production.py --target %TARGET% --mode %MODE%

REM 如果需要并行执行，添加--parallel参数
if "%PARALLEL%"=="true" set PYTHON_CMD=%PYTHON_CMD% --parallel

REM 执行Python脚本
%PYTHON_CMD%

if %ERRORLEVEL% EQU 0 (
    echo.
    echo PIT数据更新执行成功！
) else (
    echo.
    echo PIT数据更新执行失败！
    pause
)

echo.
pause
