import abc
import os
import asyncio
import pandas as pd
from typing import Dict, List, Any, Optional
from ...task import Task
from .api import TushareAPI
from tqdm.asyncio import tqdm

class TushareTask(Task):
    """基于 Tushare API 的数据任务基类
    
    核心设计：
    1. 基类只提供最基础的数据获取功能
    2. 具体的参数映射和批处理策略由子类决定
    """
    
    # 默认配置
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 5000  # 默认每页数据量
    
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
            
            # 检查依赖任务
            if self.dependencies:
                await self._check_dependencies()
            
            # 获取批处理参数列表
            self.logger.info(f"获取数据，参数: {kwargs}")
            batch_list = self.get_batch_list(**kwargs)
            if not batch_list:
                self.logger.info("批处理参数列表为空，无法获取数据")
                return {"status": "no_data", "rows": 0}
                
            self.logger.info(f"生成了 {len(batch_list)} 个批处理任务")
            
            # 处理并保存每个批次的数据
            total_rows = 0
            
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
                        return await self._process_single_batch(i, len(batch_list), batch_params)
                
                # 创建所有批次的任务
                tasks = [process_batch(i, batch_params) for i, batch_params in enumerate(batch_list)]
                
                # 等待所有任务完成，显示进度条
                if show_progress:
                    batch_results = await tqdm.gather(*tasks, desc=progress_desc, total=len(batch_list), ascii=True, position=0, leave=True)
                else:
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 处理结果
                for result in batch_results:
                    if isinstance(result, Exception):
                        self.logger.error(f"批次处理失败: {str(result)}")
                    elif result:
                        total_rows += result
            else:
                # 串行处理批次
                if show_progress:
                    # 使用tqdm创建进度条
                    pbar = tqdm(total=len(batch_list), desc=progress_desc, ascii=True, position=0, leave=True)
                    for i, batch_params in enumerate(batch_list):
                        rows = await self._process_single_batch(i, len(batch_list), batch_params)
                        if rows:
                            total_rows += rows
                        pbar.update(1)
                    pbar.close()
                else:
                    # 不显示进度条
                    for i, batch_params in enumerate(batch_list):
                        rows = await self._process_single_batch(i, len(batch_list), batch_params)
                        if rows:
                            total_rows += rows
            
            # 后处理
            result = {"status": "success", "rows": total_rows}
            await self.post_execute(result)
            
            self.logger.info(f"任务执行完成: {result}")
            return result
        except Exception as e:
            self.logger.error(f"任务执行失败: {str(e)}", exc_info=True)
            return self.handle_error(e)
            
    async def _process_single_batch(self, batch_index, total_batches, batch_params):
        """处理单个批次的数据
        
        获取、处理、验证并保存单个批次的数据。
        
        Args:
            batch_index: 批次索引
            total_batches: 总批次数
            batch_params: 批次参数
            
        Returns:
            int: 处理的行数，如果处理失败则返回0
        """
        try:
            # 获取批次数据
            if not hasattr(self, '_progress_shown') or not self._progress_shown:
                self.logger.info(f"批次 {batch_index+1}/{total_batches}: 开始获取数据")
            batch_data = await self.fetch_batch(batch_params)
            
            if batch_data is None or batch_data.empty:
                if not hasattr(self, '_progress_shown') or not self._progress_shown:
                    self.logger.info(f"批次 {batch_index+1}/{total_batches}: 没有获取到数据")
                return 0
                
            # 处理数据
            if not hasattr(self, '_progress_shown') or not self._progress_shown:
                self.logger.info(f"批次 {batch_index+1}/{total_batches}: 处理 {len(batch_data)} 行数据")
            processed_data = self.process_data(batch_data)
            
            # 验证数据
            if not hasattr(self, '_progress_shown') or not self._progress_shown:
                self.logger.info(f"批次 {batch_index+1}/{total_batches}: 验证数据")
            self.validate_data(processed_data)
            
            # 保存数据
            if not hasattr(self, '_progress_shown') or not self._progress_shown:
                self.logger.info(f"批次 {batch_index+1}/{total_batches}: 保存数据到表 {self.table_name}")
            result = await self.save_data(processed_data)
            
            rows = result.get('rows', 0)
            if not hasattr(self, '_progress_shown') or not self._progress_shown:
                self.logger.info(f"批次 {batch_index+1}/{total_batches}: 已保存 {rows} 行数据")
            return rows
        except Exception as e:
            if not hasattr(self, '_progress_shown') or not self._progress_shown:
                self.logger.error(f"批次 {batch_index+1}/{total_batches} 处理失败: {str(e)}")
            return 0
            
    async def fetch_batch(self, batch_params: Dict) -> pd.DataFrame:
        """获取单批次数据
        
        该方法负责获取单个批次的数据，通常通过Tushare API调用实现。
        
        Args:
            batch_params (Dict): 批次参数
            
        Returns:
            pd.DataFrame: 批次数据
        """
        raise NotImplementedError("子类必须实现fetch_batch方法")
        
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
        
    async def fetch_batch(self, batch_params):
        """获取单个批次的数据
        
        从Tushare API获取单个批次的数据。
        
        Args:
            batch_params: 单个批次的参数字典
            
        Returns:
            DataFrame: 获取的单批次数据
        """
        try:
            # 准备API调用参数
            api_params = self.prepare_params(batch_params)
            
            # 调用Tushare API
            self.logger.debug(f"调用API {self.api_name} 参数: {api_params}")
            # 使用实例的page_size属性作为页面大小
            result = await self.api.query(self.api_name, params=api_params, page_size=self.page_size)
            
            if result is not None and not result.empty:
                self.logger.debug(f"获取到 {len(result)} 行数据")
                return result
            else:
                self.logger.warning(f"未获取到数据")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"获取数据失败: {str(e)}")
            raise
    
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
