"""
FeatureRegistry - 特征注册表

统一管理所有已入库特征的发现、刷新、校验。

设计约束（见 docs/architecture/features_module_design.md Section 3.3.2）:
- 注册机制：装饰器 @feature_register() + 导入即注册
- 存储形态：存"类"（Type[BaseFeatureView]），需要时再实例化
- discover 策略：默认采用动态扫描（pkgutil.walk_packages）
- 错误处理：重复 name 直接抛错（Fail Fast）
"""

import importlib
import logging
import pkgutil
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

logger = logging.getLogger(__name__)

# 用于类型标注
T = TypeVar("T")


class DuplicateRecipeError(Exception):
    """重复注册异常"""
    pass


class InvalidRecipeError(Exception):
    """配方元信息校验失败"""
    pass


class FeatureRegistry:
    """
    特征注册表
    
    职责:
    - 存储已注册的 Recipe 类（不是实例）
    - 提供 discover() 动态扫描 recipes 子包
    - 提供 get/list_all 查询接口
    
    边界:
    - 不持有数据库连接
    - 不在 import 阶段触发数据库操作
    """

    _recipes: Dict[str, Type] = {}
    _discovered: bool = False

    # 注册时必须校验的字段
    REQUIRED_FIELDS = ["name", "description", "source_tables"]

    @classmethod
    def register(cls, recipe_cls: Type) -> None:
        """
        注册一个特征配方类。
        
        Args:
            recipe_cls: 配方类（继承自 BaseFeatureView 或类似基类）
        
        Raises:
            DuplicateRecipeError: 如果 name 已存在
            InvalidRecipeError: 如果缺少必填字段
        """
        # 获取 name
        name = getattr(recipe_cls, "name", None)
        if not name:
            raise InvalidRecipeError(
                f"Recipe 类 {recipe_cls.__module__}.{recipe_cls.__name__} 缺少 'name' 属性"
            )
        
        # 校验必填字段
        for field in cls.REQUIRED_FIELDS:
            value = getattr(recipe_cls, field, None)
            if not value:
                raise InvalidRecipeError(
                    f"Recipe '{name}' ({recipe_cls.__module__}.{recipe_cls.__name__}) "
                    f"缺少必填字段 '{field}'"
                )
        
        # 检查重复注册
        if name in cls._recipes:
            existing = cls._recipes[name]
            raise DuplicateRecipeError(
                f"Recipe name '{name}' 重复注册:\n"
                f"  已存在: {existing.__module__}.{existing.__name__}\n"
                f"  新注册: {recipe_cls.__module__}.{recipe_cls.__name__}"
            )
        
        # 注册
        cls._recipes[name] = recipe_cls
        logger.debug(
            f"已注册 Recipe: {name} ({recipe_cls.__module__}.{recipe_cls.__name__})"
        )

    @classmethod
    def get(cls, name: str) -> Optional[Type]:
        """
        按名称获取特征配方类。
        
        Args:
            name: recipe.name
            
        Returns:
            配方类，如果不存在返回 None
        """
        return cls._recipes.get(name)

    @classmethod
    def list_all(cls) -> List[str]:
        """
        列出所有已注册特征名。
        
        Returns:
            已注册的 recipe.name 列表（按字母排序）
        """
        return sorted(cls._recipes.keys())

    @classmethod
    def get_all(cls) -> Dict[str, Type]:
        """
        获取所有已注册的配方类。
        
        Returns:
            {name: recipe_cls} 字典
        """
        return dict(cls._recipes)

    @classmethod
    def discover(cls, force_reload: bool = False) -> List[Type]:
        """
        自动发现 features/recipes/ 下所有 Recipe。
        
        使用 pkgutil.walk_packages 动态扫描，import 触发 @feature_register()。
        
        Args:
            force_reload: 是否强制重新扫描（忽略缓存）
            
        Returns:
            已注册的 Recipe 类列表
            
        Raises:
            ImportError: 如果某个模块导入失败（默认不容错）
        """
        if cls._discovered and not force_reload:
            logger.debug("discover() 使用缓存，跳过扫描")
            return list(cls._recipes.values())
        
        if force_reload:
            # 清空已有注册（仅在强制重载时）
            cls._recipes.clear()
            logger.info("discover(force_reload=True): 清空已有注册")
        
        logger.info("开始扫描 alphahome.features.recipes ...")
        
        # 导入 recipes 包
        try:
            import alphahome.features.recipes as recipes_pkg
        except ImportError as e:
            logger.error(f"无法导入 alphahome.features.recipes: {e}")
            raise
        
        # 递归扫描所有子模块
        scanned_count = 0
        for importer, modname, ispkg in pkgutil.walk_packages(
            recipes_pkg.__path__,
            prefix="alphahome.features.recipes."
        ):
            # 跳过 __pycache__ 等
            if "__pycache__" in modname:
                continue
            
            try:
                importlib.import_module(modname)
                scanned_count += 1
                logger.debug(f"已导入: {modname}")
            except Exception as e:
                # 默认不容错，直接抛出
                logger.error(f"导入模块 {modname} 失败: {e}")
                raise
        
        cls._discovered = True
        logger.info(
            f"discover() 完成: 扫描 {scanned_count} 个模块, "
            f"注册 {len(cls._recipes)} 个 Recipe"
        )
        
        return list(cls._recipes.values())

    @classmethod
    def reset(cls) -> None:
        """
        重置注册表（主要用于测试）。
        """
        cls._recipes.clear()
        cls._discovered = False
        logger.debug("FeatureRegistry 已重置")


def feature_register(cls: Optional[Type[T]] = None) -> Callable[[Type[T]], Type[T]]:
    """
    特征配方注册装饰器。
    
    用法 1: @feature_register
    用法 2: @feature_register()
    
    装饰器在类定义时（import 阶段）自动将类注册到 FeatureRegistry。
    
    Example:
        @feature_register
        class MyFeatureMV(BaseFeatureView):
            name = "my_feature"
            description = "..."
            source_tables = ["rawdata.xxx"]
            ...
    """
    def decorator(recipe_cls: Type[T]) -> Type[T]:
        FeatureRegistry.register(recipe_cls)
        return recipe_cls
    
    # 支持 @feature_register 和 @feature_register() 两种写法
    if cls is not None:
        # 直接作为装饰器使用: @feature_register
        return decorator(cls)
    else:
        # 作为工厂函数使用: @feature_register()
        return decorator
