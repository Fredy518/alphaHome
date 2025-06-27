#!/usr/bin/env python3
"""
文档链接检查脚本

检查项目中所有Markdown文档的内部链接是否有效。
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple, Set


def find_markdown_files(root_dir: str) -> List[Path]:
    """查找所有Markdown文件"""
    root_path = Path(root_dir)
    md_files = []
    
    for file_path in root_path.rglob("*.md"):
        md_files.append(file_path)
    
    return md_files


def extract_links(content: str) -> List[str]:
    """从Markdown内容中提取所有链接"""
    # 匹配 [text](link) 格式的链接
    link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    matches = re.findall(link_pattern, content)
    
    links = []
    for text, link in matches:
        # 只检查相对路径链接，跳过HTTP链接和锚点
        if not link.startswith(('http://', 'https://', 'mailto:', '#')):
            links.append(link)
    
    return links


def check_file_exists(base_path: Path, link: str) -> bool:
    """检查链接指向的文件是否存在"""
    # 处理相对路径
    if link.startswith('./'):
        link = link[2:]
        target_path = base_path.parent / link
    elif link.startswith('../'):
        # 处理上级目录引用
        target_path = base_path.parent / link
    else:
        target_path = base_path.parent / link

    # 规范化路径
    try:
        target_path = target_path.resolve()
        return target_path.exists()
    except (OSError, ValueError):
        return False


def check_document_links(file_path: Path) -> List[Tuple[str, bool]]:
    """检查单个文档的所有链接"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except (UnicodeDecodeError, IOError) as e:
        print(f"警告: 无法读取文件 {file_path}: {e}")
        return []
    
    links = extract_links(content)
    results = []
    
    for link in links:
        exists = check_file_exists(file_path, link)
        results.append((link, exists))
    
    return results


def main():
    """主函数"""
    # 项目根目录
    project_root = Path(__file__).parent.parent
    
    print("🔍 AlphaHome 文档链接检查")
    print("=" * 50)
    
    # 查找所有Markdown文件
    md_files = find_markdown_files(str(project_root))
    print(f"📄 找到 {len(md_files)} 个Markdown文件")
    
    total_links = 0
    broken_links = 0
    all_broken_links = []
    
    # 检查每个文件
    for md_file in md_files:
        relative_path = md_file.relative_to(project_root)
        print(f"\n📝 检查文件: {relative_path}")
        
        link_results = check_document_links(md_file)
        
        if not link_results:
            print("   ✅ 无内部链接")
            continue
        
        file_broken_links = []
        for link, exists in link_results:
            total_links += 1
            if exists:
                print(f"   ✅ {link}")
            else:
                print(f"   ❌ {link}")
                broken_links += 1
                file_broken_links.append(link)
        
        if file_broken_links:
            all_broken_links.append((relative_path, file_broken_links))
    
    # 输出总结
    print("\n" + "=" * 50)
    print("📊 检查结果总结")
    print(f"📄 检查文件数: {len(md_files)}")
    print(f"🔗 总链接数: {total_links}")
    print(f"✅ 有效链接: {total_links - broken_links}")
    print(f"❌ 无效链接: {broken_links}")
    
    if broken_links > 0:
        print(f"\n❌ 发现 {broken_links} 个无效链接:")
        for file_path, links in all_broken_links:
            print(f"\n📄 {file_path}:")
            for link in links:
                print(f"   - {link}")
        
        print("\n💡 建议:")
        print("1. 检查文件路径是否正确")
        print("2. 确认目标文件是否存在")
        print("3. 验证相对路径是否正确")
        
        sys.exit(1)
    else:
        print("\n🎉 所有链接检查通过！")
        sys.exit(0)


if __name__ == "__main__":
    main()
