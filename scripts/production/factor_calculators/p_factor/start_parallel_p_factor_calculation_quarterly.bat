@echo off
chcp 65001 > nul
echo 🚀 P因子季度并行计算启动器
echo.
echo 默认配置:
echo   - 年份范围: 2020-2024
echo   - 工作进程数: 16
echo.

python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16

pause
