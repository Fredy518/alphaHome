#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from psycopg2 import Error

def get_database_connection():
    """建立数据库连接"""
    try:
        # 请在这里直接填写您的数据库连接信息
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="tusharedb",
            user="postgres",  # 替换为您的用户名
            password="wuhao123"  # 替换为您的密码
        )
        return conn
    except Error as e:
        print(f"数据库连接失败: {e}")
        raise

def get_qfq_columns(cursor):
    """获取所有带_qfq的列名"""
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'tushare' 
        AND table_name = 'tushare_stock_factor_pro' 
        AND column_name LIKE '%_qfq%'
    """)
    return [row[0] for row in cursor.fetchall()]

def drop_qfq_columns(conn, cursor, columns):
    """删除指定的列"""
    table_name = "tushare.tushare_stock_factor_pro"
    try:
        for column in columns:
            sql = f'ALTER TABLE {table_name} DROP COLUMN "{column}"'
            print(f"正在执行: {sql}")
            cursor.execute(sql)
        conn.commit()
        print("所有_qfq列已成功删除！")
    except Error as e:
        conn.rollback()
        print(f"删除列时出错: {e}")
        raise

def main():
    """主函数"""
    conn = None
    cursor = None
    try:
        # 连接数据库
        print("正在连接数据库...")
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # 获取所有_qfq列
        print("正在获取_qfq列名...")
        qfq_columns = get_qfq_columns(cursor)
        print(f"找到以下_qfq列：\n{', '.join(qfq_columns)}")
        
        # 确认是否继续
        if input("是否继续删除这些列？(y/n): ").lower() != 'y':
            print("操作已取消")
            return
        
        # 删除列
        drop_qfq_columns(conn, cursor, qfq_columns)
        
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main() 