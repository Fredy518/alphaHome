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
import re
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
                
                # 推断存储类型（物化视图或数据表）
                storage_type = _infer_storage_type(recipe_cls)
                
                feature_info = {
                    "name": name,
                    "description": description,
                    "category": category,
                    "storage_type": storage_type,
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


async def handle_refresh_features(feature_names: List[str], strategy: str = "default"):
    """
    处理刷新指定特征视图的请求。
    
    Args:
        feature_names: 要刷新的特征名称列表
        strategy: 刷新策略 ("default", "full", "incremental")
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
            actual_strategy = (
                getattr(recipe_cls, "refresh_strategy", "full")
                if strategy == "default"
                else strategy
            )
            
            # 创建实例并刷新
            instance = recipe_cls(db_manager=db_manager)
            logger.info(f"正在刷新物化视图: {instance.full_name} (策略: {actual_strategy})")
            
            result = await instance.refresh(strategy=actual_strategy)
            
            if result.get("status") == "success":
                success_count += 1
                logger.info(f"物化视图 {name} 刷新成功")
            else:
                fail_count += 1
                logger.error(f"物化视图 {name} 刷新失败: {result.get('error_message')}")
                
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
    # 优先使用类属性中的 category
    if hasattr(recipe_cls, 'category') and recipe_cls.category:
        return recipe_cls.category
    
    module = recipe_cls.__module__
    
    # 从模块路径推断: alphahome.features.recipes.mv.market.xxx -> market
    parts = module.split(".")
    if "recipes" in parts:
        idx = parts.index("recipes")
        if len(parts) > idx + 2:
            # recipes.mv.market -> market
            category = parts[idx + 2]
            # 对于 Python 特征（recipes.python.xxx），尝试从 name 推断
            if category == "python" and hasattr(recipe_cls, 'name'):
                name = recipe_cls.name
                # 从 name 的前缀推断（如 stock_xxx -> stock）
                if '_' in name:
                    prefix = name.split('_')[0]
                    return prefix
            return category
    
    return "other"


def _infer_storage_type(recipe_cls) -> str:
    """从配方类推断存储类型。"""
    # 优先使用显式声明（更类型安全、可维护）
    explicit = getattr(recipe_cls, "storage_type", None)
    if isinstance(explicit, str) and explicit:
        return explicit

    # 检查是否为 Python 特征（数据表）
    if hasattr(recipe_cls, 'is_python_feature') and recipe_cls.is_python_feature:
        return "数据表"
    
    # 检查基类是否为 IncrementalTableView（数据表）
    from alphahome.features.storage.incremental_view import IncrementalTableView
    if any(base.__name__ == 'IncrementalTableView' for base in recipe_cls.__mro__):
        return "数据表"
    
    # 默认为物化视图
    return "物化视图"


async def _update_features_with_db_status(feature_cache: List[Dict[str, Any]]):
    """使用数据库状态更新特征缓存。"""
    db_manager = UnifiedTaskFactory.get_db_manager()
    if not db_manager:
        logger.warning("数据库管理器未初始化，跳过状态更新。")
        for feature in feature_cache:
            feature["status"] = "未连接"
        return

    logger.info(f"开始更新 {len(feature_cache)} 个特征的数据库状态...")
    # 性能优化：将“逐个特征、多次SQL往返”的模式改为“批量查询”
    # 主要收益：
    # - 避免对每个特征执行 COUNT(*)（大表会非常慢）
    # - 将 37×(exists + count + refresh_log) 次往返压缩为 2 次查询
    try:
        identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

        # 1) 收集 schema / view_name（仍通过实例化保证与命名逻辑一致）
        feature_key_to_item: Dict[str, Dict[str, Any]] = {}
        view_names: List[str] = []
        schema = "features"  # Phase1 强约束：当前系统仅允许 features

        for feature in feature_cache:
            recipe_cls = feature.get("recipe_class")
            if not recipe_cls:
                feature["status"] = "错误"
                feature["row_count"] = "N/A"
                feature["last_refresh"] = "N/A"
                continue

            instance = recipe_cls(db_manager=db_manager)
            view_name = instance.view_name
            schema = instance.schema or schema

            if not identifier_re.fullmatch(schema or ""):
                raise ValueError(f"Invalid schema identifier: {schema!r}")
            if not identifier_re.fullmatch(view_name or ""):
                raise ValueError(f"Invalid view_name identifier: {view_name!r}")

            view_names.append(view_name)
            feature_key_to_item[view_name] = feature

        if not view_names:
            logger.info("特征缓存为空，跳过数据库状态更新。")
            return

        # 2) 批量检查对象是否存在（物化视图 或 普通表）
        # 注意：这里不拼接标识符到 SQL，而是通过 join 系统表来判断是否存在
        exists_sql = """
        WITH names AS (
            SELECT unnest($1::text[]) AS obj_name
        )
        SELECT
            n.obj_name AS view_name,
            (m.matviewname IS NOT NULL OR t.table_name IS NOT NULL) AS exists
        FROM names n
        LEFT JOIN pg_matviews m
            ON m.schemaname = $2 AND m.matviewname = n.obj_name
        LEFT JOIN information_schema.tables t
            ON t.table_schema = $2
           AND t.table_name = n.obj_name
           AND t.table_type = 'BASE TABLE';
        """.strip()
        exists_rows = await db_manager.fetch(exists_sql, view_names, schema, timeout=10)
        exists_map = {r["view_name"]: bool(r["exists"]) for r in (exists_rows or [])}

        # 3) 批量获取最后一次成功刷新记录（包含 row_count）
        refresh_log_sql = """
        SELECT DISTINCT ON (view_name)
            view_name,
            finished_at AT TIME ZONE 'Asia/Shanghai' AS finished_at,
            row_count
        FROM features.mv_refresh_log
        WHERE schema_name = $1
          AND view_name = ANY($2::text[])
          AND success = TRUE
        ORDER BY view_name, finished_at DESC;
        """.strip()
        log_rows = await db_manager.fetch(refresh_log_sql, schema, view_names, timeout=10)
        log_map = {r["view_name"]: r for r in (log_rows or [])}

        # 4) 回填到缓存
        for view_name in view_names:
            feature = feature_key_to_item.get(view_name)
            if not feature:
                continue

            exists = exists_map.get(view_name, False)
            if not exists:
                feature["status"] = "未创建"
                feature["row_count"] = 0
                feature["last_refresh"] = "N/A"
                continue

            feature["status"] = "已创建"

            log_row = log_map.get(view_name)
            if log_row:
                # row_count 来自刷新日志（避免 COUNT(*) 的高开销）
                try:
                    feature["row_count"] = int(log_row.get("row_count") or 0)
                except Exception:
                    feature["row_count"] = "N/A"

                last_refresh = log_row.get("finished_at")
                if isinstance(last_refresh, datetime):
                    feature["last_refresh"] = last_refresh.strftime("%Y-%m-%d %H:%M:%S")
                elif last_refresh:
                    feature["last_refresh"] = str(last_refresh)[:19]
                else:
                    feature["last_refresh"] = "N/A"
            else:
                feature["row_count"] = "N/A"
                feature["last_refresh"] = "N/A"

    except Exception as e:
        logger.error(f"批量更新特征状态失败: {e}", exc_info=True)
        for feature in feature_cache:
            if feature.get("status") in ("未知", None):
                feature["status"] = "错误"
            feature.setdefault("row_count", "N/A")
            feature.setdefault("last_refresh", "N/A")

    logger.info("特征状态更新完成。")
