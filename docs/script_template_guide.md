# 数据更新脚本开发指南

## 目录
- [数据更新脚本开发指南](#数据更新脚本开发指南)
  - [目录](#目录)
  - [概述](#概述)
  - [脚本结构](#脚本结构)
  - [命名规范](#命名规范)
  - [开发规范](#开发规范)
  - [示例模板](#示例模板)
  - [最佳实践](#最佳实践)

## 概述

本指南旨在规范化数据更新脚本的开发流程，确保所有脚本具有一致的结构和行为。通过遵循这些规范，我们可以：

1. 提高代码可维护性
2. 统一错误处理方式
3. 标准化日志输出
4. 保持一致的用户界面
5. 简化新脚本的开发流程

## 脚本结构

每个更新脚本都应包含以下基本组件：

1. 文件头部信息
   - Shebang
   - 编码声明
   - 文档字符串
   - 版权信息（可选）

2. 导入部分
   - 标准库导入
   - 第三方库导入
   - 项目内部模块导入

3. 配置部分
   - 项目路径设置
   - 环境变量加载
   - 日志配置
   - 常量定义

4. 更新器类定义
   - 类文档字符串
   - 初始化方法
   - 更新任务方法
   - 结果汇总方法

5. 主函数
   - 参数解析
   - 错误处理
   - 任务执行
   - 结果输出

## 命名规范

1. 文件命名
   - 使用小写字母
   - 单词间用下划线连接
   - 按数据类型分类，例如：
     - `update_daily.py`
     - `update_weekly.py`
     - `update_monthly.py`

2. 类命名
   - 使用 PascalCase
   - 以数据类型为前缀
   - 以 Updater 为后缀
   - 例如：`DailyUpdater`, `WeeklyUpdater`

3. 方法命名
   - 使用小写字母和下划线
   - 动词开头
   - 例如：`update_task`, `summarize_result`

4. 常量命名
   - 全大写字母
   - 单词间用下划线连接
   - 例如：`TARGET_TASK_NAME`

## 开发规范

1. 日志记录
   - 使用类级别的 logger
   - 合理使用日志级别
   - 记录关键操作和错误信息
   - 保持信息清晰简洁

2. 错误处理
   - 使用 try-except 包装关键操作
   - 详细记录异常信息
   - 返回统一的错误格式
   - 适当的错误恢复机制

3. 参数处理
   - 必要参数验证
   - 合理的默认值
   - 清晰的帮助信息
   - 参数互斥关系检查

4. 代码风格
   - 遵循 PEP 8
   - 添加适当的注释
   - 保持函数简洁
   - 避免代码重复

## 示例模板

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
{数据类型}数据更新脚本

使用 TaskUpdaterBase 基类实现的{数据类型}数据更新工具。
支持以下功能：
1. {功能1}
2. {功能2}
3. {功能3}
4. 详细的日志记录和结果汇总
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime, timedelta
import dotenv
from pathlib import Path

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from scripts.base.task_updater_base import TaskUpdaterBase
from data_module.task_factory import TaskFactory

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "{task_name}"

class {ClassName}Updater(TaskUpdaterBase):
    """
    {数据类型}数据更新工具
    
    继承自 TaskUpdaterBase，用于更新{数据类型}数据。
    支持{支持的功能描述}。
    """
    
    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="{任务类型}",
            description="{任务描述}",
            support_report_period={是否支持报告期}
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    async def update_task(self, start_date=None, end_date=None, full_update=False):
        """
        更新{数据类型}数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            full_update: 是否全量更新
            
        Returns:
            tuple: (成功更新数量, 失败数量, 错误信息列表)
        """
        self.logger.info("开始更新{数据类型}数据...")
        
        try:
            task = await TaskFactory.get_task(TARGET_TASK_NAME)
            result = await task.run(
                start_date=start_date,
                end_date=end_date,
                full_update=full_update
            )
            
            if not isinstance(result, dict):
                self.logger.error(f"任务返回结果格式错误: {result}")
                return 0, 1, [f"任务返回结果格式错误: {result}"]
                
            success_count = result.get("success", 0)
            failed_count = result.get("failed", 0)
            error_msgs = result.get("error_msgs", [])
            
            self.logger.info(f"更新完成。成功: {success_count}, 失败: {failed_count}")
            if error_msgs:
                self.logger.warning(f"错误信息: {error_msgs}")
                
            return success_count, failed_count, error_msgs
            
        except Exception as e:
            error_msg = f"更新过程发生错误: {str(e)}"
            self.logger.error(error_msg)
            return 0, 1, [error_msg]

    async def summarize_result(self, success_count, failed_count, error_msgs, 
                             start_date=None, end_date=None, full_update=False):
        """
        汇总更新结果
        
        Args:
            success_count: 成功更新数量
            failed_count: 失败数量
            error_msgs: 错误信息列表
            start_date: 开始日期
            end_date: 结束日期
            full_update: 是否全量更新
        """
        total = success_count + failed_count
        
        if total == 0:
            self.logger.info("本次更新无数据")
            return
            
        success_rate = (success_count / total) * 100 if total > 0 else 0
        
        # 输出更新模式
        if full_update:
            self.logger.info("更新模式: 全量更新")
        elif start_date and end_date:
            self.logger.info(f"更新模式: 指定日期范围更新 ({start_date} 至 {end_date})")
        else:
            self.logger.info("更新模式: 增量更新")
            
        # 输出更新结果统计
        self.logger.info(f"更新结果汇总:")
        self.logger.info(f"总数据量: {total}")
        self.logger.info(f"成功数量: {success_count}")
        self.logger.info(f"失败数量: {failed_count}")
        self.logger.info(f"成功率: {success_rate:.2f}%")
        
        # 如果有错误信息，输出详细错误
        if error_msgs:
            self.logger.warning("错误详情:")
            for msg in error_msgs:
                self.logger.warning(f"- {msg}")

async def main():
    updater = {ClassName}Updater()
    
    parser = argparse.ArgumentParser(description="更新{数据类型}数据")
    parser.add_argument("--start-date", help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end-date", help="结束日期 (YYYYMMDD)")
    parser.add_argument("--full-update", action="store_true", help="全量更新")
    
    args = parser.parse_args()
    
    # 参数检查
    if args.start_date and not args.end_date:
        parser.error("如果指定开始日期，必须同时指定结束日期")
    if args.end_date and not args.start_date:
        parser.error("如果指定结束日期，必须同时指定开始日期")
        
    try:
        # 执行更新
        success_count, failed_count, error_msgs = await updater.update_task(
            start_date=args.start_date,
            end_date=args.end_date,
            full_update=args.full_update
        )
        
        # 汇总结果
        await updater.summarize_result(
            success_count,
            failed_count,
            error_msgs,
            start_date=args.start_date,
            end_date=args.end_date,
            full_update=args.full_update
        )
        
    except Exception as e:
        logging.error(f"更新过程发生错误: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
```

## 最佳实践

1. 代码组织
   - 保持文件结构清晰
   - 相关功能放在一起
   - 避免过长的函数
   - 适当抽取公共代码

2. 错误处理
   - 捕获具体异常
   - 提供有用的错误信息
   - 记录错误上下文
   - 优雅地处理失败

3. 日志记录
   - 使用合适的日志级别
   - 记录关键操作点
   - 包含必要的上下文
   - 避免过多的日志

4. 性能优化
   - 避免重复操作
   - 合理使用异步
   - 批量处理数据
   - 注意内存使用

5. 测试
   - 编写单元测试
   - 测试边界条件
   - 模拟异常情况
   - 验证错误处理

6. 文档
   - 及时更新文档
   - 提供使用示例
   - 说明注意事项
   - 记录修改历史 