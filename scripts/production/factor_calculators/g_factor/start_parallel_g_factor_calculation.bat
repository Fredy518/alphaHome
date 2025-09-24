@echo off
REM G因子年度并行计算启动脚本 - 简化版
REM 避免中文字符和复杂路径问题

echo Starting G-Factor Parallel Calculation
echo ======================================

REM 设置计算参数
set START_YEAR=2020
set END_YEAR=2024
set TOTAL_WORKERS=5

echo Year Range: %START_YEAR%-%END_YEAR%
echo Workers: %TOTAL_WORKERS%
echo.

REM 启动5个终端窗口
for /L %%i in (0,1,4) do (
    echo Starting worker process %%i...
    start "Worker-%%i" cmd /k "cd /d E:\CodePrograms\alphaHome && python scripts/production/g_factor_parallel_by_year.py --start_year %START_YEAR% --end_year %END_YEAR% --worker_id %%i --total_workers %TOTAL_WORKERS%"
    timeout /t 2 /nobreak >nul
)

echo.
echo All worker processes started!
echo Check each terminal window for progress
echo.
pause
