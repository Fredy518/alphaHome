@echo off
REM G因子排名和评分修复工具 - Windows批处理版本
REM 
REM 使用方法：
REM fix_g_factor_rankings.bat 2020-01-01 2024-12-31
REM 
REM 参数说明：
REM %1 - 开始日期 (YYYY-MM-DD)
REM %2 - 结束日期 (YYYY-MM-DD)

setlocal enabledelayedexpansion

REM 设置参数
set START_DATE=%1
set END_DATE=%2

if "%START_DATE%"=="" (
    echo ❌ 错误: 请指定开始日期
    echo 使用方法: fix_g_factor_rankings.bat 2020-01-01 2024-12-31
    pause
    exit /b 1
)

if "%END_DATE%"=="" (
    echo ❌ 错误: 请指定结束日期
    echo 使用方法: fix_g_factor_rankings.bat 2020-01-01 2024-12-31
    pause
    exit /b 1
)

echo 🔧 G因子排名和评分修复工具 (Windows版本)
echo ================================================
echo 📅 开始日期: %START_DATE%
echo 📅 结束日期: %END_DATE%
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
if not exist "scripts\maintenance\fix_g_factor_rankings_and_scores.py" (
    echo ❌ 错误: 未找到修复脚本文件
    echo 请确保在项目根目录下运行此批处理文件
    pause
    exit /b 1
)

echo ✅ 环境检查通过
echo.

REM 确认操作
echo ⚠️ 警告: 此操作将更新数据库中的G因子排名和评分数据
echo 请确认您已备份相关数据
echo.
set /p CONFIRM="是否继续? (y/N): "
if /i not "%CONFIRM%"=="y" (
    echo 操作已取消
    pause
    exit /b 0
)

echo.
echo 🚀 开始修复G因子排名和评分...
python scripts\maintenance\fix_g_factor_rankings_and_scores.py --start_date %START_DATE% --end_date %END_DATE%

if errorlevel 1 (
    echo ❌ 修复失败，请检查错误信息
    pause
    exit /b 1
)

echo.
echo ✅ 修复完成!
echo.
echo 📊 修复说明:
echo    - 已根据子因子结果重新计算排名和G评分
echo    - 空值因子权重自动调整为0，其他因子权重按比例重新分配
echo    - 确保计算结果的合理性和准确性
echo.

pause
