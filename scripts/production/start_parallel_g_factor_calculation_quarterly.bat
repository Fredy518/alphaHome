@echo off
REM G因子季度并行计算启动器 - Windows批处理版本
REM 
REM 使用方法：
REM start_parallel_g_factor_calculation_quarterly.bat 2020 2024 16
REM 
REM 参数说明：
REM %1 - 开始年份 (默认: 2020)
REM %2 - 结束年份 (默认: 2024)  
REM %3 - 工作进程数 (默认: 16)

setlocal enabledelayedexpansion

REM 设置默认参数
set START_YEAR=%1
set END_YEAR=%2
set WORKERS=%3

if "%START_YEAR%"=="" set START_YEAR=2020
if "%END_YEAR%"=="" set END_YEAR=2024
if "%WORKERS%"=="" set WORKERS=16

echo 🚀 G因子季度并行计算启动器 (Windows版本)
echo ================================================
echo 📅 开始年份: %START_YEAR%
echo 📅 结束年份: %END_YEAR%
echo 👥 工作进程数: %WORKERS%
echo 🕐 启动时间: %date% %time%
echo.

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误: 未找到Python环境，请确保Python已安装并添加到PATH
    pause
    exit /b 1
)

REM 检查脚本文件是否存在
if not exist "scripts\production\start_parallel_g_factor_calculation_quarterly.py" (
    echo ❌ 错误: 未找到启动器脚本文件
    echo 请确保在项目根目录下运行此批处理文件
    pause
    exit /b 1
)

if not exist "scripts\production\g_factor_parallel_by_quarter.py" (
    echo ❌ 错误: 未找到季度计算脚本文件
    echo 请确保在项目根目录下运行此批处理文件
    pause
    exit /b 1
)

echo ✅ 环境检查通过
echo.

REM 启动季度并行计算
echo 🚀 启动G因子季度并行计算...
python scripts\production\start_parallel_g_factor_calculation_quarterly.py --start_year %START_YEAR% --end_year %END_YEAR% --workers %WORKERS%

if errorlevel 1 (
    echo ❌ 启动失败，请检查错误信息
    pause
    exit /b 1
)

echo.
echo ✅ 所有工作进程已启动!
echo.
echo 📊 监控说明:
echo    - 每个终端窗口显示一个工作进程的进度
echo    - 可以随时关闭单个终端窗口来停止对应进程
echo    - 所有进程完成后，G因子数据将保存到数据库
echo.
echo 💡 性能预期:
echo    - 季度并行比年度并行粒度更细，适合大规模计算
echo    - 建议监控数据库连接数和磁盘空间
echo.
echo 🎯 建议:
echo    - 可以随时调整工作进程数来平衡负载
echo    - 定期检查计算进度和系统资源使用情况
echo.

pause
