#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据导出脚本
将电液控数据导出为 JSON 文件，供前端使用
"""

import io
import sys

# 设置 stdout 编码为 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from Src import ExportData

if __name__ == "__main__":
    # 示例1：导出单个数据库文件（使用默认路径）
    # exporter = ExportData()
    # exporter.export_all()

    # 示例2：导出单个数据库文件（指定路径）
    # exporter = ExportData(db_paths="Datas/电液控UDP驱动_20250904_14.db")
    # exporter.export_all()

    # 示例3：导出多个数据库文件（合并导出）
    db_paths = [
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_14.db",
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_16.db",
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_18.db",
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_20.db",
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_22.db",
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250905_00.db",
        # "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_02.db",
        # "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_04.db",
        # "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_06.db",
        # "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_08.db",
    ]

    exporter = ExportData(db_paths=db_paths)
    exporter.export_all()

    # 高级用法：单独调用各个导出方法
    # exporter = ExportData(db_paths="Datas/电液控UDP驱动_20250904_14.db")
    # exporter.load_data()
    # stats = exporter.export_statistics()
    # scatter = exporter.export_scatter_data()
    # features = exporter.export_feature_data()
