import abc
import os
import asyncio
import pandas as pd
from typing import Dict, List, Any, Optional
from ...base_task import Task
from .tushare_api import TushareAPI
from tqdm.asyncio import tqdm
from datetime import datetime, timedelta

class TushareTask(Task):
    """基于 Tushare API 的数据任务基类
    
    核心设计：
    1. 基类只提供最基础的数据获取功能
    2. 具体的参数映射和批处理策略由子类决定
    """
    
    # 默认配置
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 5000  # 默认每页数据量
    default_max_retries = 3     # 默认最大重试次数
    default_retry_delay = 2     # 默认重试延迟（秒）
    
    api_name = None  # Tushare API名称，子类必须定义
    fields = None    # 需要获取的字段列表，子类必须定义
    
    def __init__(self, db_connection, api_token=None, api=None):
        """初始化 TushareTask
        
        Args:
            db_connection: 数据库连接
            api_token (str, optional): Tushare API 令牌
            api (TushareAPI, optional): 已初始化的TushareAPI实例
        """
        super().__init__(db_connection)
        
        if self.api_name is None or self.fields is None:
            raise ValueError("必须定义api_name和fields属性")
            
        # 如果未提供token，则从环境变量获取
        if api_token is None:
            api_token = os.environ.get('TUSHARE_TOKEN')
            if not api_token:
                raise ValueError("未提供Tushare API令牌，请通过参数传入或设置TUSHARE_TOKEN环境变量")
                
        # 创建Tushare API客户端
        self.api = api or TushareAPI(api_token)
        
        # 初始化配置
        self.concurrent_limit = self.default_concurrent_limit
        self.page_size = self.default_page_size
        self.max_retries = self.default_max_retries
        self.retry_delay = self.default_retry_delay
        
    def set_config(self, config):
        """设置任务配置
        
        Args:
            config: 配置字典，包含任务特定的配置项
        """
        if not config:
            return
            
        # 设置并发限制
        if 'concurrent_limit' in config:
            self.concurrent_limit = config['concurrent_limit']
            self.logger.debug(f"设置并发限制: {self.concurrent_limit}")
            
        # 设置每页数据量
        if 'page_size' in config:
            self.page_size = config['page_size']
            self.logger.debug(f"设置每页数据量: {self.page_size}")

        # 设置重试配置
        if 'max_retries' in config:
            self.max_retries = config['max_retries']
            self.logger.debug(f"设置最大重试次数: {self.max_retries}")
        if 'retry_delay' in config:
            self.retry_delay = config['retry_delay']
            self.logger.debug(f"设置重试延迟: {self.retry_delay}s")
    
    async def execute(self, **kwargs):
        """执行任务的完整生命周期
        
        重写Task基类的execute方法，实现分批获取、处理和保存数据的流程。
        这种方式更适合处理大数据集，因为它避免了将所有数据一次性加载到内存中。
        
        Args:
            **kwargs: 额外参数，将传递给get_batch_list和prepare_params方法
            
        Returns:
            Dict: 任务执行结果，包含状态和影响的行数
        """
        self.logger.info(f"开始执行任务: {self.name}")
        
        try:
            await self.pre_execute()
            
            # 依赖任务检查已移除
            # 直接进行数据获取
            
            # 获取批处理参数列表
            self.logger.info(f"获取数据，参数: {kwargs}")
            batch_list = await self.get_batch_list(**kwargs)
            if not batch_list:
                self.logger.info("批处理参数列表为空，无法获取数据")
                return {"status": "no_data", "rows": 0}
                
            self.logger.info(f"生成了 {len(batch_list)} 个批处理任务")
            
            # 处理并保存每个批次的数据
            total_rows = 0
            failed_batches = 0 # 记录失败的批次数
            
            # 获取并发限制参数，优先使用传入的参数，其次使用实例属性
            concurrent_limit = kwargs.get('concurrent_limit', self.concurrent_limit)
            show_progress = kwargs.get('show_progress', False)
            progress_desc = kwargs.get('progress_desc', f"执行任务: {self.name}")
            
            if concurrent_limit > 1:
                # 并发处理批次
                self.logger.info(f"使用并发模式处理批次，并发数: {concurrent_limit}")
                semaphore = asyncio.Semaphore(concurrent_limit)
                
                async def process_batch(i, batch_params):
                    async with semaphore:
                        # _process_single_batch 现在返回行数或在最终失败时返回 0
                        processed_rows = await self._process_single_batch(i, len(batch_list), batch_params)
                        if processed_rows == 0: # 假设返回 0 表示失败
                            return None # 返回 None 表示失败，以便后续计数
                        return processed_rows # 返回成功处理的行数
                
                # 创建所有批次的任务
                tasks = [process_batch(i, batch_params) for i, batch_params in enumerate(batch_list)]
                
                # 等待所有任务完成，显示进度条
                if show_progress:
                    batch_results = await tqdm.gather(*tasks, desc=progress_desc, total=len(batch_list), ascii=True, position=0, leave=True)
                else:
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True) # 保持捕获异常
                
                # 处理结果
                for result in batch_results:
                    if isinstance(result, Exception):
                        # gather 捕获的异常通常表示 process_batch 内部未处理的异常
                        self.logger.error(f"批次处理中发生未捕获异常: {str(result)}")
                        failed_batches += 1
                    elif result is None: # 我们用 None 表示批次处理失败（重试后）
                        failed_batches += 1
                    elif isinstance(result, int): # 成功处理的行数
                        total_rows += result
            
            else:
                # 串行处理批次
                progress_iterator = range(len(batch_list))
                if show_progress:
                    progress_iterator = tqdm(progress_iterator, desc=progress_desc, ascii=True, position=0, leave=True)

                for i in progress_iterator:
                    batch_params = batch_list[i]
                    rows = await self._process_single_batch(i, len(batch_list), batch_params)
                    if rows > 0:
                        total_rows += rows
                    else: # rows == 0 表示失败
                        failed_batches += 1
            
            # 后处理
            if failed_batches > 0:
                status = "partial_success" if total_rows > 0 else "failure"
                self.logger.warning(f"任务执行完成，但有 {failed_batches} 个批次处理失败。")
            else:
                status = "success"

            result = {"status": status, "rows": total_rows, "failed_batches": failed_batches}
            await self.post_execute(result)
            
            self.logger.info(f"任务执行完成: {result}")
            return result
        except Exception as e:
            self.logger.error(f"任务执行失败: {str(e)}", exc_info=True)
            return self.handle_error(e)
            
    async def _process_single_batch(self, batch_index, total_batches, batch_params):
        """处理单个批次的数据（包含重试逻辑）
        
        获取、处理、验证并保存单个批次的数据。
        针对获取和保存步骤实现了重试机制。
        
        Args:
            batch_index: 批次索引
            total_batches: 总批次数
            batch_params: 批次参数
            
        Returns:
            int: 处理的行数，如果最终处理失败则返回 0
        """
        batch_log_prefix = f"批次 {batch_index+1}/{total_batches}"
        batch_data = None
        processed_data = None
        rows = 0

        # 1. 获取数据（带重试）
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0: # 如果是重试
                    self.logger.info(f"{batch_log_prefix}: 重试获取数据 (尝试 {attempt+1}/{self.max_retries+1})")
                else:
                     self.logger.info(f"{batch_log_prefix}: 开始获取数据")

                batch_data = await self.fetch_batch(batch_params)
                # 如果成功获取数据（即使是空的），则跳出重试循环
                self.logger.info(f"{batch_log_prefix}: 获取数据成功")
                break
            except Exception as e:
                self.logger.warning(f"{batch_log_prefix}: 获取数据时发生错误 (尝试 {attempt+1}/{self.max_retries+1}): {str(e)}")
                if attempt >= self.max_retries:
                    self.logger.error(f"{batch_log_prefix}: 获取数据失败，已达最大重试次数。")
                    return 0 # 获取数据最终失败
                # 等待后重试
                await asyncio.sleep(self.retry_delay)
                continue # 继续下一次尝试

        # 如果获取的数据为空，直接返回
        if batch_data is None or batch_data.empty:
            self.logger.info(f"{batch_log_prefix}: 没有获取到数据或获取最终失败")
            return 0

        # 2. 处理数据 (通常不重试逻辑错误)
        try:
            self.logger.info(f"{batch_log_prefix}: 处理 {len(batch_data)} 行数据")
            processed_data = self.process_data(batch_data)
        except Exception as e:
            self.logger.error(f"{batch_log_prefix}: 处理数据时发生错误: {str(e)}")
            return 0 # 处理数据失败

        # 3. 验证数据 (通常不重试逻辑错误)
        try:
            self.logger.info(f"{batch_log_prefix}: 验证数据")
            if not self.validate_data(processed_data):
                 self.logger.error(f"{batch_log_prefix}: 数据验证失败")
                 return 0 # 验证失败
        except Exception as e:
            self.logger.error(f"{batch_log_prefix}: 验证数据时发生错误: {str(e)}")
            return 0 # 验证过程中出错

        # 4. 保存数据（带重试）
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"{batch_log_prefix}: 重试保存数据 (尝试 {attempt+1}/{self.max_retries+1})")
                else:
                    self.logger.info(f"{batch_log_prefix}: 保存数据到表 {self.table_name}")

                result = await self.save_data(processed_data)
                rows = result.get('rows', 0)
                self.logger.info(f"{batch_log_prefix}: 已保存 {rows} 行数据")
                return rows # 保存成功，返回行数
            except Exception as e:
                self.logger.warning(f"{batch_log_prefix}: 保存数据时发生错误 (尝试 {attempt+1}/{self.max_retries+1}): {str(e)}")
                if attempt >= self.max_retries:
                    self.logger.error(f"{batch_log_prefix}: 保存数据失败，已达最大重试次数。")
                    return 0 # 保存数据最终失败
                # 等待后重试
                await asyncio.sleep(self.retry_delay)
                continue # 继续下一次尝试

        return 0 # 如果代码能执行到这里，说明保存逻辑有问题，返回 0
            
    async def fetch_batch(self, batch_params: Dict) -> pd.DataFrame:
        """获取单批次数据
        
        该方法负责获取单个批次的数据，通常通过Tushare API调用实现。
        
        Args:
            batch_params (Dict): 批次参数
            
        Returns:
            pd.DataFrame: 批次数据
        """
        # 准备API调用参数
        api_params = self.prepare_params(batch_params)
        
        try:
            # 调用Tushare API
            df = await self.api.query(
                self.api_name, 
                params=api_params, 
                fields=self.fields,
                page_size=self.page_size # Use configured page size
            )
            return df
        except Exception as e:
            self.logger.error(f"获取批次数据失败: {str(e)}，参数: {api_params}")
            return pd.DataFrame() # 返回空DataFrame
        
    def _apply_column_mapping(self, data):
        """应用列名映射
        
        将原始列名映射为目标列名，只处理数据中存在的列。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 应用列名映射后的数据
        """
        if not hasattr(self, 'column_mapping') or not self.column_mapping:
            return data
            
        # 检查映射前的列是否存在
        missing_original_cols = [orig_col for orig_col in self.column_mapping.keys() 
                                if orig_col not in data.columns]
        if missing_original_cols:
            self.logger.warning(f"列名映射失败：原始数据中缺少以下列: {missing_original_cols}")
        
        # 执行重命名，只重命名数据中存在的列
        rename_map = {k: v for k, v in self.column_mapping.items() if k in data.columns}
        if rename_map:
            data.rename(columns=rename_map, inplace=True)
            self.logger.info(f"已应用列名映射: {rename_map}")
            
        return data

    def _process_date_column(self, data):
        """处理日期列
        
        将日期列转换为标准的日期时间格式，并移除无效日期的行。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 处理日期后的数据，如果日期列不存在则返回原始数据
        """
        if not self.date_column or self.date_column not in data.columns:
            if self.date_column:
                self.logger.warning(f"指定的日期列 '{self.date_column}' 不在数据中，无法进行日期格式转换。")
            return data
            
        try:
            # 如果是字符串格式（如'20210101'），转换为日期对象
            data[self.date_column] = pd.to_datetime(data[self.date_column], format='%Y%m%d', errors='coerce')
            # 删除转换失败的行 (NaT)
            original_count = len(data)
            data.dropna(subset=[self.date_column], inplace=True)
            if len(data) < original_count:
                self.logger.warning(f"移除了 {original_count - len(data)} 行，因为日期列 '{self.date_column}' 格式无效。")
        except Exception as e:
            self.logger.warning(f"日期列 {self.date_column} 格式转换时发生错误: {str(e)}")
            
        return data

    def _sort_data(self, data):
        """对数据进行排序
        
        根据日期列和主键列对数据进行排序。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 排序后的数据
        """
        # 构建排序键列表
        sort_keys = []
        if self.date_column and self.date_column in data.columns:
            sort_keys.append(self.date_column)

        if self.primary_keys:
            other_keys = [pk for pk in self.primary_keys if pk != self.date_column and pk in data.columns]
            sort_keys.extend(other_keys)
            
        # 如果没有有效的排序键，则返回原始数据
        if not sort_keys:
            return data
            
        # 检查所有排序键是否都在数据中
        missing_keys = [key for key in sort_keys if key not in data.columns]
        if missing_keys:
            self.logger.warning(f"排序失败：数据中缺少以下排序键: {missing_keys}")
            # 从排序键列表中移除缺失的键
            sort_keys = [key for key in sort_keys if key not in missing_keys]
            if not sort_keys:
                return data
                
        try:
            # 执行排序
            data = data.sort_values(by=sort_keys)
            self.logger.info(f"数据已按以下键排序: {sort_keys}")
        except Exception as e:
            self.logger.warning(f"排序时发生错误: {str(e)}")
            
        return data

    def _apply_transformations(self, data):
        """应用数据转换
        
        根据转换规则对指定列应用转换函数。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 应用转换后的数据
        """
        if not hasattr(self, 'transformations') or not self.transformations:
            return data
            
        for column, transform_func in self.transformations.items():
            if column in data.columns:
                try:
                    data[column] = data[column].apply(transform_func)
                except Exception as e:
                    self.logger.warning(f"列 '{column}' 应用转换函数时发生错误: {str(e)}")
                    
        return data
        
    def process_data(self, data):
        """处理从Tushare获取的数据
        
        除了基类的数据处理外，还处理Tushare特有的数据格式问题，并进行排序。
        包括列名映射、日期处理、数据排序和数据转换。
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要处理")
            return pd.DataFrame()
            
        # 首先应用列名映射
        data = self._apply_column_mapping(data)
        
        # 处理日期列
        data = self._process_date_column(data)
        
        # 应用数据转换
        data = self._apply_transformations(data)
        
        # 对数据进行排序
        data = self._sort_data(data)
        
        # 调用基类的处理方法
        return super().process_data(data)
        
    def validate_data(self, data):
        """验证从Tushare获取的数据
        
        除了基类的数据验证外，还添加了Tushare特有的数据验证
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要验证")
            return True
            
        # 应用自定义验证规则
        if hasattr(self, 'validations') and self.validations:
            for validation_func in self.validations:
                try:
                    if not validation_func(data):
                        self.logger.warning(f"数据验证失败: {validation_func.__name__ if hasattr(validation_func, '__name__') else '未命名验证'}")
                        return False
                except Exception as e:
                    self.logger.warning(f"执行验证时发生错误: {str(e)}")
                    return False
                    
        # 调用基类的验证方法
        return super().validate_data(data)
        
    @abc.abstractmethod
    def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表
        
        该方法负责将用户的查询参数转换为一系列批处理参数，每个批处理参数将用于一次API调用。
        这个方法的主要目的是将大查询分解为多个小查询，以避免超出API限制或提高并行性。
        
        实现此方法时，应考虑以下几点：
        1. 数据量大小（如日期范围、股票数量等）
        2. API的限制（如单次查询的最大记录数）
        3. 性能考虑（如并行查询的优化）
        
        典型的实现示例：
        - 按日期范围分批：将长时间范围分割为多个短时间段
        - 按代码分批：将多个股票代码分组为多个小批次
        - 混合策略：同时考虑时间和代码的分批策略
        
        Args:
            **kwargs: 用户提供的查询参数，如start_date, end_date, ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表，每个元素是一个参数字典，将用于一次API调用
                例如：[
                    {"start_date": "20220101", "end_date": "20220131", "ts_code": "000001.SZ,000002.SZ"},
                    {"start_date": "20220201", "end_date": "20220228", "ts_code": "000001.SZ,000002.SZ"},
                ]
        """
        pass
    
    @abc.abstractmethod
    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数
        
        该方法负责将批处理参数转换为Tushare API调用所需的确切参数格式。
        这个方法的主要目的是处理参数格式转换、默认值设置、参数验证等工作。
        
        实现此方法时，应考虑以下几点：
        1. Tushare API的具体参数要求（参数名称、格式等）
        2. 必要时进行参数格式转换（如日期格式转换）
        3. 添加API调用所需的任何额外参数
        4. 验证参数的有效性
        
        典型的实现示例：
        - 日期格式转换：将datetime对象转换为'YYYYMMDD'格式字符串
        - 代码格式处理：确保股票代码格式符合API要求
        - 参数重命名：将内部使用的参数名映射到API要求的参数名
        - 添加默认参数：如未指定字段列表时使用默认字段
        
        Args:
            batch_params: 由get_batch_list方法生成的单个批次参数字典
            
        Returns:
            Dict: 准备好的API调用参数字典，将直接传递给TushareAPI.query方法的params参数
                例如：{
                    "ts_code": "000001.SZ,000002.SZ",
                    "start_date": "20220101",
                    "end_date": "20220131",
                    "fields": "trade_date,open,high,low,close,vol"
                }
        """
        pass
        
    async def fetch_data(self, **kwargs):
        """从Tushare获取数据
        
        实现了Task基类的抽象方法，但在TushareTask中，我们更推荐使用execute方法直接执行
        任务的完整生命周期，它会分批获取和处理数据，更适合大数据集。
        
        此方法仅作为兼容基类接口保留，不建议直接调用。
        
        Args:
            **kwargs: 额外参数，将传递给get_batch_list和prepare_params方法
            
        Returns:
            DataFrame: 获取的数据
        """
        self.logger.warning("TushareTask.fetch_data方法不建议直接调用，请使用execute方法代替")
        
        # 获取批处理参数列表
        batch_list = self.get_batch_list(**kwargs)
        if not batch_list:
            self.logger.warning("批处理参数列表为空，无法获取数据")
            return pd.DataFrame()
            
        self.logger.info(f"生成了 {len(batch_list)} 个批处理任务")
        
        # 存储所有批次的数据
        all_data = []
        
        # 处理每个批次
        for i, batch_params in enumerate(batch_list):
            try:
                result = await self.fetch_batch(batch_params)
                if not result.empty:
                    self.logger.info(f"批次 {i+1}/{len(batch_list)}: 获取到 {len(result)} 行数据")
                    all_data.append(result)
                else:
                    self.logger.warning(f"批次 {i+1}/{len(batch_list)}: 未获取到数据")
            except Exception as e:
                self.logger.error(f"批次 {i+1}/{len(batch_list)} 获取数据失败: {str(e)}")
                # 继续处理下一个批次，而不是直接失败
                continue
        
        # 合并所有批次的数据
        if not all_data:
            self.logger.warning("所有批次都未获取到数据")
            return pd.DataFrame()
            
        combined_data = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"成功获取数据，共 {len(combined_data)} 行")
        
        return combined_data

    async def update_by_trade_day(self, trade_days_lookback: Optional[int] = None, end_date: Optional[str] = None, **kwargs):
        """根据交易日智能增量更新

        根据数据库中的最新交易日或指定的交易日回溯天数，
        自动计算需要更新的实际交易日范围，然后执行更新。

        Args:
            trade_days_lookback (int, optional): 回溯的交易日天数。
                                                如果提供，则从 `end_date` 向前回溯 `trade_days_lookback` 个交易日作为起始日期。
                                                如果不提供，则自动从数据库最新日期的下一个交易日开始。
            end_date (str, optional): 更新的结束日期 (YYYYMMDD)，默认为当前日期。
            **kwargs: 传递给 `self.execute` 的其他参数。

        Returns:
            Dict: 任务执行结果。
        """
        # Import moved inside method to break circular dependency
        from ...tools.calendar import get_last_trade_day, get_next_trade_day
        
        self.logger.info(f"开始执行 {self.name} 的基于交易日的增量更新")

        # 确定结束日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        start_date = None

        if trade_days_lookback is not None and trade_days_lookback > 0:
            # 按指定交易日回溯天数计算起始日期
            try:
                start_date = await get_last_trade_day(end_date, n=trade_days_lookback)
                self.logger.info(f"按回溯 {trade_days_lookback} 个交易日计算，起始日期为: {start_date}")
            except Exception as e:
                self.logger.error(f"计算回溯 {trade_days_lookback} 个交易日失败: {e}")
                return self.handle_error(e)
        else:
            # 自动模式：从数据库最新日期的下一个交易日开始
            latest_date = await self._get_latest_date()
            if latest_date:
                try:
                    next_day = await get_next_trade_day(latest_date, n=1)
                    if next_day and next_day <= end_date:
                        start_date = next_day
                        self.logger.info(f"数据库最新日期为 {latest_date}，从下一个交易日 {start_date} 开始更新")
                    elif not next_day:
                        self.logger.info(f"无法确定 {latest_date} 的下一个交易日。数据可能已最新。")
                        return {"status": "up_to_date", "rows": 0}
                    else: # next_day > end_date
                        self.logger.info(f"数据库最新日期 {latest_date} 之后无新交易日需要更新 (下一个交易日 {next_day} > 结束日期 {end_date})")
                        return {"status": "up_to_date", "rows": 0}
                except Exception as e:
                    self.logger.error(f"获取 {latest_date} 的下一个交易日时发生错误: {e}")
                    return self.handle_error(e)
            else:
                # 数据库无记录，获取最近30个交易日
                try:
                    start_date = await get_last_trade_day(end_date, n=30)
                    self.logger.info(f"数据库无记录，默认更新最近30个交易日，起始日期: {start_date}")
                except Exception as e:
                    self.logger.error(f"计算最近30个交易日失败: {e}")
                    return self.handle_error(e)

        # 如果成功确定了起始日期，则执行任务
        if start_date and end_date:
            self.logger.info(f"最终执行更新范围: {start_date} 到 {end_date}")
            # 注意：这里调用的是 self.execute，它内部会处理分批等逻辑
            return await self.execute(start_date=start_date, end_date=end_date, **kwargs)
        else:
            self.logger.warning("未能确定有效的更新日期范围，任务未执行")
            return {"status": "no_range", "rows": 0}
