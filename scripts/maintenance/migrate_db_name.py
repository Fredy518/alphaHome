# -*- coding: utf-8 -*-
"""
数据库重命名脚本

功能:
- 将 PostgreSQL 数据库从 'tusharedb' 重命名为 'alphadb'。
- 安全地提示用户输入数据库管理员凭据。
- 自动终止所有到 'tusharedb' 的活动连接。
- 提供清晰的操作反馈和错误处理。

使用方法:
1. 确保已安装 psycopg2-binary: pip install psycopg2-binary
2. 在项目根目录运行此脚本: python scripts/migrate_db_name.py
3. 按照提示输入 PostgreSQL 的连接信息。
"""

import getpass
import sys

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    print("错误: 未找到 'psycopg2' 库。")
    print("请先通过 pip 安装: pip install psycopg2-binary")
    sys.exit(1)

# --- 配置 ---
OLD_DB_NAME = "tusharedb"
NEW_DB_NAME = "alphadb"


def get_db_credentials():
    """安全地获取用户输入的数据库凭据"""
    print("请输入 PostgreSQL 管理员连接信息 (用于重命名数据库):")
    host = input("主机 (默认: localhost): ") or "localhost"
    port = input("端口 (默认: 5432): ") or "5432"
    user = input(f"用户名 (默认: postgres): ") or "postgres"
    password = getpass.getpass("密码: ")
    return {"host": host, "port": port, "user": user, "password": password}


def rename_database():
    """执行数据库重命名逻辑"""
    creds = get_db_credentials()
    conn = None
    try:
        # 1. 连接到模板数据库 (如 postgres) 以执行管理操作
        print(f"\n正在以用户 '{creds['user']}' 连接到管理数据库 'postgres'...")
        conn = psycopg2.connect(dbname="postgres", **creds) # type: ignore
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        print("连接成功。")

        # 2. 检查新数据库名是否已存在
        print(f"正在检查数据库 '{NEW_DB_NAME}' 是否已存在...")
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [NEW_DB_NAME]
        )
        if cursor.fetchone():
            print(f"错误: 数据库 '{NEW_DB_NAME}' 已存在，无需重命名。脚本将退出。")
            return

        # 3. 检查旧数据库是否存在
        print(f"正在检查源数据库 '{OLD_DB_NAME}' 是否存在...")
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [OLD_DB_NAME]
        )
        if not cursor.fetchone():
            print(f"错误: 源数据库 '{OLD_DB_NAME}' 未找到。请确认数据库名称是否正确。")
            return

        # 4. 终止所有到旧数据库的连接
        print(f"正在终止所有到 '{OLD_DB_NAME}' 的活动连接...")
        terminate_query = sql.SQL(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s AND pid <> pg_backend_pid();
            """
        )
        cursor.execute(terminate_query, [OLD_DB_NAME])
        print("活动连接已终止。")

        # 5. 执行重命名
        print(f"正在将数据库 '{OLD_DB_NAME}' 重命名为 '{NEW_DB_NAME}'...")
        rename_query = sql.SQL("ALTER DATABASE {} RENAME TO {};").format(
            sql.Identifier(OLD_DB_NAME), sql.Identifier(NEW_DB_NAME)
        )
        cursor.execute(rename_query)
        print("数据库重命名成功！")

        # 6. 验证结果
        print("正在验证重命名结果...")
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [NEW_DB_NAME]
        )
        if cursor.fetchone():
            print(f"验证成功: 数据库 '{NEW_DB_NAME}' 已存在。")
            print("\n迁移完成。请记得更新你的 config.json 文件，将数据库名称指向 'alphadb'。")
        else:
            print(f"警告: 验证失败，未能找到新的数据库 '{NEW_DB_NAME}'。")

        cursor.close()

    except psycopg2.OperationalError as e:
        print(f"\n操作错误: 无法连接到 PostgreSQL 数据库。")
        print("请检查以下几点:")
        print("- 数据库服务是否正在运行？")
        print("- 主机、端口、用户名和密码是否正确？")
        print(f"- 详细信息: {e}")
    except psycopg2.Error as e:
        print(f"\n数据库错误: 执行命令时发生错误。")
        print(f"- 错误码: {e.pgcode}")
        print(f"- 详细信息: {e.pgerror}")
        print("请检查用户是否具有足够的权限来执行此操作。")
    finally:
        if conn:
            conn.close()
            print("\n数据库连接已关闭。")


if __name__ == "__main__":
    rename_database() 