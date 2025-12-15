"""
物化视图监控器

负责记录和查询物化视图的刷新元数据和数据质量检查结果。
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
import logging

from alphahome.common.db_manager import DBManager


class MaterializedViewMonitor:
    """
    物化视图监控器
    
    职责：
    1. 记录物化视图的刷新元数据
    2. 查询刷新历史
    3. 记录数据质量检查结果
    4. 查询质量检查历史
    
    属性：
    - db_manager: 数据库管理器实例
    - logger: 日志记录器
    """
    
    def __init__(self, db_manager: DBManager):
        """
        初始化物化视图监控器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    async def record_refresh_metadata(
        self,
        view_name: str,
        refresh_result: Dict[str, Any]
    ) -> None:
        """
        记录物化视图的刷新元数据
        
        Args:
            view_name: 物化视图名称
            refresh_result: 刷新结果字典，包含：
                - status: success/failed
                - refresh_time: 刷新时间
                - duration_seconds: 刷新耗时
                - row_count: 刷新后的行数
                - error_message: 错误信息（如果失败）
                - view_schema: 物化视图所在的 schema（可选，默认 materialized_views）
                - source_tables: 数据源表列表（可选）
                - refresh_strategy: 刷新策略（可选）
        
        Returns:
            None
        """
        try:
            # 提取刷新结果中的信息
            status = refresh_result.get('status', 'unknown')
            refresh_time = refresh_result.get('refresh_time', datetime.now())
            duration_seconds = refresh_result.get('duration_seconds', 0)
            row_count = refresh_result.get('row_count', 0)
            error_message = refresh_result.get('error_message', None)
            view_schema = refresh_result.get('view_schema', 'materialized_views')
            source_tables = refresh_result.get('source_tables', [])
            refresh_strategy = refresh_result.get('refresh_strategy', 'full')
            
            # 将 source_tables 列表转换为 JSON 字符串
            import json
            source_tables_json = json.dumps(source_tables) if source_tables else '[]'
            
            # 构建 UPSERT 语句
            upsert_sql = """
            INSERT INTO materialized_views.materialized_views_metadata (
                view_name,
                view_schema,
                source_tables,
                refresh_strategy,
                last_refresh_time,
                refresh_status,
                row_count,
                refresh_duration_seconds,
                error_message,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (view_name) DO UPDATE SET
                view_schema = EXCLUDED.view_schema,
                source_tables = EXCLUDED.source_tables,
                refresh_strategy = EXCLUDED.refresh_strategy,
                last_refresh_time = EXCLUDED.last_refresh_time,
                refresh_status = EXCLUDED.refresh_status,
                row_count = EXCLUDED.row_count,
                refresh_duration_seconds = EXCLUDED.refresh_duration_seconds,
                error_message = EXCLUDED.error_message,
                updated_at = NOW();
            """
            
            # 执行 UPSERT
            await self.db_manager.execute(
                upsert_sql,
                view_name,
                view_schema,
                source_tables_json,
                refresh_strategy,
                refresh_time,
                status,
                row_count,
                duration_seconds,
                error_message
            )
            
            self.logger.info(
                f"已记录物化视图 {view_schema}.{view_name} 的刷新元数据: "
                f"status={status}, row_count={row_count}, duration={duration_seconds}s"
            )
        
        except Exception as e:
            self.logger.error(
                f"记录物化视图 {view_name} 的刷新元数据失败: {e}",
                exc_info=True
            )
            raise
    
    async def get_refresh_history(
        self,
        view_name: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        获取物化视图的刷新历史
        
        Args:
            view_name: 物化视图名称
            limit: 返回的最大记录数（默认 10）
        
        Returns:
            List[Dict[str, Any]]: 刷新历史记录列表，每条记录包含：
                - view_name: 物化视图名称
                - view_schema: 物化视图所在的 schema
                - source_tables: 数据源表列表
                - refresh_strategy: 刷新策略
                - last_refresh_time: 最后刷新时间
                - refresh_status: 刷新状态
                - row_count: 行数
                - refresh_duration_seconds: 刷新耗时
                - error_message: 错误信息
        """
        try:
            # 查询刷新历史
            query = """
            SELECT
                view_name,
                view_schema,
                source_tables,
                refresh_strategy,
                last_refresh_time,
                refresh_status,
                row_count,
                refresh_duration_seconds,
                error_message
            FROM materialized_views.materialized_views_metadata
            WHERE view_name = $1
            ORDER BY last_refresh_time DESC
            LIMIT $2;
            """
            
            # 执行查询
            rows = await self.db_manager.fetch(query, view_name, limit)
            
            # 转换结果
            history = []
            for row in rows:
                # 将 source_tables JSON 字符串转换回列表
                import json
                source_tables = json.loads(row['source_tables']) if row['source_tables'] else []
                
                history.append({
                    'view_name': row['view_name'],
                    'view_schema': row['view_schema'],
                    'source_tables': source_tables,
                    'refresh_strategy': row['refresh_strategy'],
                    'last_refresh_time': row['last_refresh_time'],
                    'refresh_status': row['refresh_status'],
                    'row_count': row['row_count'],
                    'refresh_duration_seconds': row['refresh_duration_seconds'],
                    'error_message': row['error_message']
                })
            
            self.logger.info(
                f"获取物化视图 {view_name} 的刷新历史: {len(history)} 条记录"
            )
            
            return history
        
        except Exception as e:
            self.logger.error(
                f"获取物化视图 {view_name} 的刷新历史失败: {e}",
                exc_info=True
            )
            raise
    
    async def record_quality_check(
        self,
        view_name: str,
        check_name: str,
        check_status: str,
        check_message: str,
        check_details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        记录数据质量检查结果
        
        Args:
            view_name: 物化视图名称
            check_name: 检查名称（例如 null_check, outlier_check）
            check_status: 检查状态（pass/warning/error）
            check_message: 检查消息
            check_details: 检查详情（可选）
        
        Returns:
            None
        """
        try:
            # 将 check_details 转换为 JSON 字符串
            import json
            check_details_json = json.dumps(check_details) if check_details else '{}'
            
            # 构建插入语句
            insert_sql = """
            INSERT INTO materialized_views.materialized_views_quality_checks (
                view_name,
                check_name,
                check_status,
                check_message,
                check_details,
                checked_at
            ) VALUES ($1, $2, $3, $4, $5, NOW());
            """
            
            # 执行插入
            await self.db_manager.execute(
                insert_sql,
                view_name,
                check_name,
                check_status,
                check_message,
                check_details_json
            )
            
            self.logger.info(
                f"已记录物化视图 {view_name} 的质量检查: "
                f"check_name={check_name}, status={check_status}"
            )
        
        except Exception as e:
            self.logger.error(
                f"记录物化视图 {view_name} 的质量检查失败: {e}",
                exc_info=True
            )
            raise
    
    async def get_quality_check_history(
        self,
        view_name: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        获取物化视图的数据质量检查历史
        
        Args:
            view_name: 物化视图名称
            limit: 返回的最大记录数（默认 10）
        
        Returns:
            List[Dict[str, Any]]: 质量检查历史记录列表，每条记录包含：
                - id: 检查记录 ID
                - view_name: 物化视图名称
                - check_name: 检查名称
                - check_status: 检查状态
                - check_message: 检查消息
                - check_details: 检查详情
                - checked_at: 检查时间
        """
        try:
            # 查询质量检查历史
            query = """
            SELECT
                id,
                view_name,
                check_name,
                check_status,
                check_message,
                check_details,
                checked_at
            FROM materialized_views.materialized_views_quality_checks
            WHERE view_name = $1
            ORDER BY checked_at DESC
            LIMIT $2;
            """
            
            # 执行查询
            rows = await self.db_manager.fetch(query, view_name, limit)
            
            # 转换结果
            history = []
            for row in rows:
                # 将 check_details JSON 字符串转换回字典
                import json
                check_details = json.loads(row['check_details']) if row['check_details'] else {}
                
                history.append({
                    'id': row['id'],
                    'view_name': row['view_name'],
                    'check_name': row['check_name'],
                    'check_status': row['check_status'],
                    'check_message': row['check_message'],
                    'check_details': check_details,
                    'checked_at': row['checked_at']
                })
            
            self.logger.info(
                f"获取物化视图 {view_name} 的质量检查历史: {len(history)} 条记录"
            )
            
            return history
        
        except Exception as e:
            self.logger.error(
                f"获取物化视图 {view_name} 的质量检查历史失败: {e}",
                exc_info=True
            )
            raise
    
    async def get_latest_refresh_status(
        self,
        view_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        获取物化视图的最新刷新状态
        
        Args:
            view_name: 物化视图名称
        
        Returns:
            Optional[Dict[str, Any]]: 最新刷新状态，如果没有记录则返回 None
        """
        try:
            # 查询最新刷新状态
            query = """
            SELECT
                view_name,
                view_schema,
                source_tables,
                refresh_strategy,
                last_refresh_time,
                refresh_status,
                row_count,
                refresh_duration_seconds,
                error_message
            FROM materialized_views.materialized_views_metadata
            WHERE view_name = $1
            LIMIT 1;
            """
            
            # 执行查询
            row = await self.db_manager.fetch_one(query, view_name)
            
            if row is None:
                self.logger.warning(
                    f"物化视图 {view_name} 没有刷新记录"
                )
                return None
            
            # 将 source_tables JSON 字符串转换回列表
            import json
            source_tables = json.loads(row['source_tables']) if row['source_tables'] else []
            
            status = {
                'view_name': row['view_name'],
                'view_schema': row['view_schema'],
                'source_tables': source_tables,
                'refresh_strategy': row['refresh_strategy'],
                'last_refresh_time': row['last_refresh_time'],
                'refresh_status': row['refresh_status'],
                'row_count': row['row_count'],
                'refresh_duration_seconds': row['refresh_duration_seconds'],
                'error_message': row['error_message']
            }
            
            self.logger.info(
                f"获取物化视图 {view_name} 的最新刷新状态: {status['refresh_status']}"
            )
            
            return status
        
        except Exception as e:
            self.logger.error(
                f"获取物化视图 {view_name} 的最新刷新状态失败: {e}",
                exc_info=True
            )
            raise
