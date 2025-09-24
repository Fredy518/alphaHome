@echo off
setlocal ENABLEDELAYEDEXPANSION
REM Tushare 数据源智能增量更新批处理启动器（Windows）

REM 统一使用 UTF-8 编码，避免中文乱码
chcp 65001 >nul

echo Tushare 数据源智能增量更新批处理启动器
echo ====================================================

REM 检查 Python 环境
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ 未找到 Python 环境，请确保已安装 Python 并添加到 PATH
    pause
    exit /b 1
)

REM 设置默认参数
set WORKERS=3
set RETRIES=3
set DELAY=5
set LOG_LEVEL=INFO

REM 解析命令行参数
if "%~1"=="" goto :usage
if "%~1"=="--help" goto :usage_quick
if "%~1"=="help" goto :usage_quick

REM 简单参数解析（支持 workers, retries, delay, log_level）
if not "%~1"=="" set WORKERS=%~1
if not "%~2"=="" set RETRIES=%~2
if not "%~3"=="" set DELAY=%~3
if not "%~4"=="" set LOG_LEVEL=%~4

echo 配置参数:
echo   并发进程数: %WORKERS%
echo   最大重试次数: %RETRIES%
echo   重试间隔: %DELAY%秒
echo   日志级别: %LOG_LEVEL%
echo.

REM 切换到项目根目录（从 scripts\production\data_updaters\tushare 上溯4级）
cd /d %~dp0\..\..\..\..

echo 当前工作目录: %cd%
echo.

REM 检查脚本文件是否存在
if not exist "scripts\production\data_updaters\tushare\tushare_smart_update_production.py" (
    echo 找不到主脚本文件: scripts\production\data_updaters\tushare\tushare_smart_update_production.py
    pause
    exit /b 1
)

echo 启动 Tushare 智能增量更新...
echo.

REM 执行主脚本
python scripts\production\data_updaters\tushare\tushare_smart_update_production.py --workers %WORKERS% --max_retries %RETRIES% --retry_delay %DELAY% --log_level %LOG_LEVEL%

REM 获取执行结果
set EXIT_CODE=%errorlevel%

echo.
if %EXIT_CODE% equ 0 (
    echo Tushare 数据更新执行成功！
) else (
    echo Tushare 数据更新执行失败！
)

echo.
echo 按任意键退出...
pause >nul

exit /b %EXIT_CODE%

:usage
echo 使用方法:
echo   %0 [workers] [retries] [delay] [log_level]
echo.
echo 参数说明:
echo   workers    并发进程数 (默认: 3)
echo   retries    最大重试次数 (默认: 3)
echo   delay      重试间隔秒数 (默认: 5)
echo   log_level  日志级别: DEBUG, INFO, WARNING, ERROR (默认: INFO)
echo.
echo 示例:
echo   "%~f0"
echo   "%~f0" 5
echo   "%~f0" 5 5 10
echo   "%~f0" 3 3 5 DEBUG
echo   ^(参数含义见上方说明^)
echo.
pause
exit /b 0

:usage_quick
echo 使用方法:
echo   %0 [workers] [retries] [delay] [log_level]
echo.
echo 参数说明:
echo   workers    并发进程数 (默认: 3)
echo   retries    最大重试次数 (默认: 3)
echo   delay      重试间隔秒数 (默认: 5)
echo   log_level  日志级别: DEBUG, INFO, WARNING, ERROR (默认: INFO)
echo.
echo 示例:
echo   "%~f0"
echo   "%~f0" 5
echo   "%~f0" 5 5 10
echo   "%~f0" 3 3 5 DEBUG
echo   ^(参数含义见上方说明^)
echo.
exit /b 0
