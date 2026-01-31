"""
特征服务

负责处理所有与特征注册、发现、刷新相关的逻辑，包括：
- 特征配方发现和缓存管理
- 特征元数据提取和格式化
- 特征选择状态管理
- 与数据库交互获取物化视图状态
- 为GUI提供统一的特征信息接口
"""
import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory
from ...features import FeatureRegistry

logger = get_logger(__name__)

# --- 缓存和回调 ---
_feature_cache: List[Dict[str, Any]] = []
_send_response_callback: Optional[Callable] = None


def initialize_feature_service(response_callback: Callable):
    """初始化特征服务，设置回调函数。"""
    global _send_response_callback
    _send_response_callback = response_callback
    logger.info("特征服务已初始化。")


def get_cached_features() -> List[Dict[str, Any]]:
    """获取缓存的特征列表。"""
    return _feature_cache


async def handle_get_features():
    """处理获取特征列表的请求。"""
    global _feature_cache
    success = False
    try:
        # 发现所有特征配方
        logger.info("开始发现特征配方...")
        recipes = FeatureRegistry.discover()
        logger.info(f"发现 {len(recipes)} 个特征配方。")

        # 保持现有的选择状态
        existing_selection = {item["name"]: item["selected"] for item in _feature_cache}

        new_cache = []
        for recipe_cls in recipes:
            try:
                # 从类属性获取信息
                name = getattr(recipe_cls, "name", "")
                description = getattr(recipe_cls, "description", "")
                source_tables = getattr(recipe_cls, "source_tables", [])
                refresh_strategy = getattr(recipe_cls, "refresh_strategy", "full")
                
                # 推断分类（从模块路径或 source_tables）
                category = _infer_category(recipe_cls)
                
                feature_info = {
                    "name": name,
                    "description": description,
                    "category": category,
                    "source_tables": source_tables,
                    "refresh_strategy": refresh_strategy,
                    "status": "未知",  # 稍后更新
                    "row_count": "N/A",
                    "last_refresh": "N/A",
                    "selected": existing_selection.get(name, False),
                    "recipe_class": recipe_cls,  # 保存类引用以便后续操作
                }
                new_cache.append(feature_info)
            except Exception as e:
                logger.error(f"获取特征 '{getattr(recipe_cls, 'name', 'unknown')}' 详情失败: {e}")

        # 按分类和名称排序
        _feature_cache = sorted(new_cache, key=lambda x: (x["category"], x["name"]))

        # 更新物化视图状态
        await _update_features_with_db_status(_feature_cache)

        if _send_response_callback:
            # 发送时移除 recipe_class（不可序列化）
            cache_for_ui = [
                {k: v for k, v in f.items() if k != "recipe_class"}
                for f in _feature_cache
            ]
            _send_response_callback("FEATURE_LIST_UPDATE", cache_for_ui)
            _send_response_callback("STATUS", f"特征列表已刷新 (共 {len(_feature_cache)} 个特征)")
        success = True

    except Exception as e:
        logger.exception("获取特征列表时发生严重错误。")
        if _send_response_callback:
            _send_response_callback("ERROR", f"获取特征列表失败: {e}")
    finally:
        if _send_response_callback:
            _send_response_callback("FEATURE_REFRESH_COMPLETE", {"success": success})


async def handle_refresh_features(feature_names: List[str]):
    """
    处理刷新指定特征视图的请求。
    
    Args:
        feature_names: 要刷新的特征名称列表
    """
    success_count = 0
    fail_count = 0
    
    db_manager = UnifiedTaskFactory.get_db_manager()
    if not db_manager:
        logger.error("数据库管理器未初始化，无法刷新特征视图。")
        if _send_response_callback:
            _send_response_callback("ERROR", "数据库未连接，无法刷新特征视图。")
        return
    
    for name in feature_names:
        try:
            # 从缓存获取 recipe_class
            feature = next((f for f in _feature_cache if f["name"] == name), None)
            if not feature or "recipe_class" not in feature:
                logger.warning(f"未找到特征 '{name}' 的配方类。")
                fail_count += 1
                continue
            
            recipe_cls = feature["recipe_class"]
            
            # 创建实例并刷新
            instance = recipe_cls(db_manager=db_manager)
            logger.info(f"正在刷新物化视图: {instance.full_name}")
            
            result = await instance.refresh()
            
            if result.get("success"):
                success_count += 1
                logger.info(f"物化视图 {name} 刷新成功")
            else:
                fail_count += 1
                logger.error(f"物化视图 {name} 刷新失败: {result.get('error')}")
                
        except Exception as e:
            fail_count += 1
            logger.error(f"刷新特征 '{name}' 时发生错误: {e}")
    
    if _send_response_callback:
        _send_response_callback("FEATURE_OPERATION_COMPLETE", {
            "operation": "刷新",
            "success_count": success_count,
            "fail_count": fail_count
        })


async def handle_create_features(feature_names: List[str]):
    """
    处理创建指定特征视图的请求。
    
    Args:
        feature_names: 要创建的特征名称列表
    """
    success_count = 0
    fail_count = 0
    
    db_manager = UnifiedTaskFactory.get_db_manager()
    if not db_manager:
        logger.error("数据库管理器未初始化，无法创建特征视图。")
        if _send_response_callback:
            _send_response_callback("ERROR", "数据库未连接，无法创建特征视图。")
        return
    
    for name in feature_names:
        try:
            # 从缓存获取 recipe_class
            feature = next((f for f in _feature_cache if f["name"] == name), None)
            if not feature or "recipe_class" not in feature:
                logger.warning(f"未找到特征 '{name}' 的配方类。")
                fail_count += 1
                continue
            
            recipe_cls = feature["recipe_class"]
            
            # 创建实例并创建视图
            instance = recipe_cls(db_manager=db_manager)
            logger.info(f"正在创建物化视图: {instance.full_name}")
            
            result = await instance.create(if_not_exists=True)
            
            if result:
                success_count += 1
                logger.info(f"物化视图 {name} 创建成功")
            else:
                fail_count += 1
                logger.error(f"物化视图 {name} 创建失败")
                
        except Exception as e:
            fail_count += 1
            logger.error(f"创建特征 '{name}' 时发生错误: {e}")
    
    if _send_response_callback:
        _send_response_callback("FEATURE_OPERATION_COMPLETE", {
            "operation": "创建",
            "success_count": success_count,
            "fail_count": fail_count
        })


def _infer_category(recipe_cls) -> str:
    """从配方类推断分类。"""
    module = recipe_cls.__module__
    
    # 从模块路径推断: alphahome.features.recipes.mv.market.xxx -> market
    parts = module.split(".")
    if "recipes" in parts:
        idx = parts.index("recipes")
        if len(parts) > idx + 2:
            # recipes.mv.market -> market
            category = parts[idx + 2]
            return category
    
    return "other"


async def _update_features_with_db_status(feature_cache: List[Dict[str, Any]]):
    """使用数据库状态更新特征缓存。"""
    db_manager = UnifiedTaskFactory.get_db_manager()
    if not db_manager:
        logger.warning("数据库管理器未初始化，跳过状态更新。")
        for feature in feature_cache:
            feature["status"] = "未连接"
        return

    logger.info(f"开始更新 {len(feature_cache)} 个特征的数据库状态...")

    async def update_single_feature(feature: Dict[str, Any]):
        """更新单个特征的状态"""
        try:
            recipe_cls = feature.get("recipe_class")
            if not recipe_cls:
                feature["status"] = "错误"
                return
            
            # 创建临时实例获取视图名
            instance = recipe_cls(db_manager=db_manager)
            view_name = instance.view_name
            schema = instance.schema
            
            # 检查物化视图是否存在
            sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM pg_matviews
                WHERE schemaname = '{schema}'
                  AND matviewname = '{view_name}'
            ) AS exists;
            """
            result = await db_manager.fetch(sql)
            exists = result and result[0]["exists"]
            
            if exists:
                feature["status"] = "已创建"
                
                # 获取行数
                try:
                    count_sql = f"SELECT COUNT(*) AS cnt FROM {schema}.{view_name};"
                    count_result = await db_manager.fetch(count_sql)
                    feature["row_count"] = count_result[0]["cnt"] if count_result else 0
                except Exception:
                    feature["row_count"] = "N/A"
                
                # 尝试从刷新日志获取最后刷新时间
                try:
                    # 使用字符串格式化而不是参数化查询，因为某些 db_manager 实现可能不支持 $1 语法
                    refresh_log_sql = f"""
                    SELECT finished_at FROM features.mv_refresh_log
                    WHERE view_name = '{view_name}' AND success = TRUE
                    ORDER BY finished_at DESC
                    LIMIT 1;
                    """
                    logger.debug(f"查询刷新日志 for {view_name}: {refresh_log_sql}")
                    log_result = await db_manager.fetch(refresh_log_sql)
                    logger.debug(f"查询结果: {log_result}")
                    
                    if log_result and len(log_result) > 0 and log_result[0].get("finished_at"):
                        last_refresh = log_result[0]["finished_at"]
                        if isinstance(last_refresh, datetime):
                            feature["last_refresh"] = last_refresh.strftime("%Y-%m-%d %H:%M")
                        else:
                            feature["last_refresh"] = str(last_refresh)[:16]
                    else:
                        feature["last_refresh"] = "N/A"
                except Exception as e:
                    logger.error(f"查询刷新日志失败 for {view_name}: {e}")
                    feature["last_refresh"] = "N/A"
            else:
                feature["status"] = "未创建"
                feature["row_count"] = 0
                feature["last_refresh"] = "N/A"
                
        except Exception as e:
            logger.error(f"更新特征 '{feature.get('name')}' 状态时发生错误: {e}")
            feature["status"] = "错误"

    # 并发更新所有特征状态
    tasks = [update_single_feature(feature) for feature in feature_cache]
    await asyncio.gather(*tasks)

    logger.info("特征状态更新完成。")
