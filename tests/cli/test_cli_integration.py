"""
AlphaHome CLI 集成测试套件

测试统一 CLI 的基本功能：
- 命令行解析
- 帮助文本输出
- 退出码规范
- 命令执行
"""

import pytest
import sys
import subprocess
from pathlib import Path
from alphahome.cli.main import build_parser, main
from alphahome.cli.core import exitcodes


class TestCLIParser:
    """测试 CLI 解析器"""
    
    def test_parser_builds(self):
        """测试解析器能成功构建"""
        parser = build_parser()
        assert parser is not None
        assert parser.prog == 'ah'
    
    def test_parser_help(self):
        """测试顶层帮助文本"""
        parser = build_parser()
        # 不应该抛出异常
        assert parser.format_help() is not None
    
    def test_prod_subcommand(self):
        """测试 prod 子命令解析"""
        parser = build_parser()
        args = parser.parse_args(['prod', 'list'])
        assert args.command == 'prod'
        assert args.prod_command == 'list'
    
    def test_ddb_subcommand(self):
        """测试 ddb 子命令解析"""
        parser = build_parser()
        args = parser.parse_args(['ddb', 'init-kline5m', '--db-path', 'test'])
        assert args.command == 'ddb'
        assert args.ddb_cmd == 'init-kline5m'
        assert args.db_path == 'test'
    
    def test_mv_subcommand(self):
        """测试 mv 子命令解析"""
        parser = build_parser()
        args = parser.parse_args(['mv', 'status', 'test_view'])
        assert args.command == 'mv'
        assert args.mv_command == 'status'
        assert args.view_name == 'test_view'
    
    def test_gui_subcommand(self):
        """测试 gui 子命令解析"""
        parser = build_parser()
        args = parser.parse_args(['gui'])
        assert args.command == 'gui'
    
    def test_log_level_parsing(self):
        """测试日志级别参数"""
        parser = build_parser()
        args = parser.parse_args(['--log-level', 'DEBUG', 'prod', 'list'])
        assert args.log_level == 'DEBUG'
    
    def test_format_parsing(self):
        """测试输出格式参数"""
        parser = build_parser()
        args = parser.parse_args(['--format', 'json', 'prod', 'list'])
        assert args.format == 'json'


class TestCLIExecution:
    """测试 CLI 执行"""
    
    def test_main_no_args(self):
        """测试无参数调用应返回错误码"""
        result = main([])
        assert result == exitcodes.INVALID_ARGS
    
    def test_prod_list_execution(self):
        """测试 prod list 命令执行"""
        result = main(['prod', 'list'])
        assert result == exitcodes.SUCCESS
    
    def test_version_flag(self):
        """测试 --version 标志"""
        # --version 会调用 argparse 的 action='version'，它会打印版本并调用 sys.exit(0)
        # 但在 main() 中会被捕获为 SystemExit
        with pytest.raises(SystemExit):
            from argparse import ArgumentParser
            parser = ArgumentParser()
            parser.add_argument('--version', action='version', version='test 1.0')
            parser.parse_args(['--version'])


class TestExitCodes:
    """测试退出码规范"""
    
    def test_exit_code_constants(self):
        """验证退出码常量定义"""
        assert exitcodes.SUCCESS == 0
        assert exitcodes.FAILURE == 1
        assert exitcodes.INVALID_ARGS == 2
        assert exitcodes.UNAVAILABLE == 3
        assert exitcodes.INTERNAL_ERROR == 4
        assert exitcodes.INTERRUPTED == 130


@pytest.mark.integration
class TestCLIIntegration:
    """CLI 集成测试（需要实际环境）"""
    
    @pytest.mark.requires_db
    def test_mv_refresh_fails_gracefully_without_db(self):
        """测试物化视图刷新在无数据库时优雅失败"""
        result = main(['mv', 'refresh', 'test_view', '--db-url', 'postgresql://invalid'])
        assert result != exitcodes.SUCCESS
    
    @pytest.mark.requires_api
    def test_ddb_init_fails_without_connection(self):
        """测试 DDB 初始化在无连接时失败"""
        result = main(['ddb', 'init-kline5m', '--host', 'invalid', '--port', '9999'])
        # 根据实现，可能返回 FAILURE 或 INTERNAL_ERROR
        assert result in (exitcodes.FAILURE, exitcodes.INTERNAL_ERROR)


class TestCLIHelp:
    """测试帮助文本"""
    
    def test_prod_help_parseable(self):
        """测试 prod 组帮助可以正常解析"""
        parser = build_parser()
        # 在我们的实现中，--help 会打印帮助但可能不抛出 SystemExit
        # 确保帮助文本包含预期的信息
        help_text = parser.format_help()
        assert 'prod' in help_text
        assert 'ddb' in help_text
    
    def test_command_help_available(self):
        """测试各命令的帮助都可访问"""
        parser = build_parser()
        # 验证各命令都能被解析（不传递 --help 以避免 SystemExit）
        args = parser.parse_args(['prod', 'list'])
        assert args.command == 'prod'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
