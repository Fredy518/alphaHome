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
from threading import Lock, RLock, get_ident
from typing import Callable, Dict, List, Optional, Type, TypeVar

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
    _lock = RLock()
    _discovery_lock = Lock()
    _discovery_owner: Optional[int] = None
    _pending: Optional[Dict[str, Type]] = None

    # 注册时必须校验的字段
    REQUIRED_FIELDS = ["name", "description", "source_tables"]

    @classmethod
    def register(cls, recipe_cls: Type) -> None:
        with cls._lock:
            cls._register_locked(recipe_cls)

    @classmethod
    def _register_locked(cls, recipe_cls: Type) -> None:
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
        staging = cls._pending is not None and cls._discovery_owner == get_ident()
        recipes = cls._pending if staging else dict(cls._recipes)
        if name in recipes:
            existing = recipes[name]
            if staging and existing is recipe_cls:
                return
            raise DuplicateRecipeError(
                f"Recipe name '{name}' 重复注册:\n"
                f"  已存在: {existing.__module__}.{existing.__name__}\n"
                f"  新注册: {recipe_cls.__module__}.{recipe_cls.__name__}"
            )
        
        # 注册
        recipes[name] = recipe_cls
        recipe_cls._feature_registered_recipe = True
        if not staging:
            cls._recipes = recipes
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
        
        扫描声明并重建候选注册表，全部成功后发布；缓存模块也会重新提取已注册的类。
        
        Args:
            force_reload: 是否强制重新扫描；不重复执行缓存模块的顶层代码
            
        Returns:
            已注册的 Recipe 类列表
            
        Raises:
            ImportError: 如果某个模块导入失败（默认不容错）
        """
        if cls._discovery_owner == get_ident():
            raise RuntimeError("Recursive feature discovery is not supported")
        with cls._discovery_lock:
            if cls._discovered and not force_reload:
                return list(cls._recipes.values())
            prefix = "alphahome.features.recipes."
            # Keep explicitly registered external extensions. Builtins are
            # reconstructed from module declarations, including cached imports.
            with cls._lock:
                cls._discovery_owner = get_ident()
                cls._pending = {
                    name: recipe for name, recipe in cls._recipes.items()
                    if not recipe.__module__.startswith(prefix)
                }
            try:
                import alphahome.features.recipes as recipes_pkg

                scanned = 0
                for _, modname, _ in pkgutil.walk_packages(recipes_pkg.__path__, prefix=prefix):
                    if "__pycache__" in modname:
                        continue
                    module = importlib.import_module(modname)
                    scanned += 1
                    for recipe in vars(module).values():
                        if (isinstance(recipe, type) and recipe.__module__ == modname
                                and recipe.__dict__.get("_feature_registered_recipe", False)):
                            cls.register(recipe)
                with cls._lock:
                    # An unrelated importer may have registered an extension
                    # while we waited for Python's module import lock.
                    for recipe in cls._recipes.values():
                        if not recipe.__module__.startswith(prefix):
                            cls._register_locked(recipe)
                    rebuilt = dict(cls._pending)
                    cls._recipes = rebuilt
                    cls._discovered = True
            finally:
                with cls._lock:
                    cls._pending = None
                    cls._discovery_owner = None
            logger.info("Feature discovery: modules=%s recipes=%s", scanned, len(rebuilt))
            return list(rebuilt.values())

    @classmethod
    def reset(cls) -> None:
        """
        重置注册表（主要用于测试）。
        """
        if cls._discovery_owner == get_ident():
            raise RuntimeError("Cannot reset during feature discovery")
        with cls._discovery_lock:
            with cls._lock:
                cls._recipes = {}
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
