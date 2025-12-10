-- Clean Layer Schema DDL
-- 创建 clean schema 用于存放已清洗、标准化的数据
-- 
-- 执行环境约束：
-- - 仅在 dev/staging 环境执行
-- - 生产环境需 DBA 审批
--
-- 版本: 1.0
-- 创建日期: 2025-12-10

-- 创建 clean schema（如果不存在）
CREATE SCHEMA IF NOT EXISTS clean;

-- 添加 schema 注释
COMMENT ON SCHEMA clean IS 'Clean Layer: 存放已清洗、标准化的数据，作为研究和特征层的统一输入';
