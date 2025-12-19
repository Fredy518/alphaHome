"""
通用异常和错误处理

定义CLI系统使用的异常类型，统一错误处理流程。
"""

class CLIError(Exception):
    """CLI 基础异常类"""
    exit_code = 1
    
    def __init__(self, message: str, exit_code: int = 1):
        self.message = message
        self.exit_code = exit_code
        super().__init__(message)


class CLIArgumentError(CLIError):
    """参数相关错误"""
    exit_code = 2


class CLIResourceError(CLIError):
    """资源不可用错误（数据库、文件等）"""
    exit_code = 3


class CLIInternalError(CLIError):
    """内部错误"""
    exit_code = 4
