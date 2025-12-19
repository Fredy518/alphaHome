"""
退出码规范模块

定义统一的退出码常量，确保整个CLI系统遵循一致的退出码契约。
"""

# 成功
SUCCESS = 0

# 业务失败
FAILURE = 1

# 参数错误或前置条件不满足
INVALID_ARGS = 2

# 资源不可用（数据库连接失败、文件缺失等）
UNAVAILABLE = 3

# 内部错误
INTERNAL_ERROR = 4

# 用户中断（Ctrl-C）
INTERRUPTED = 130  # 标准 UNIX 约定
