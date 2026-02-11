#!/usr/bin/env python3
"""
电液控数据导出程序 (增强版)
使用DataProcessor处理数据，并将解析结果导出和打印到控制台
"""

import json
import os
import sys

from pandas.core.arrays.datetimelike import isin

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from Src import DataProcessor
from util.action_receiver import ActionType
from util.Enums import SensorTypeID
from util.shear_position_receiver import ShearerDir


def format_action_type(action_type):
    """格式化动作类型枚举为字符串"""
    if isinstance(action_type, ActionType):
        return action_type.name
    return str(action_type)


def format_action_codes(action_codes):
    """格式化动作代码列表为字符串"""
    if not action_codes:
        return "无"

    return ", ".join(action_codes)


def format_direction(direction):
    """格式化方向枚举为字符串"""
    if isinstance(direction, ShearerDir):
        if direction == ShearerDir.Up:
            return "上行"
        elif direction == ShearerDir.Down:
            return "下行"
        elif direction == ShearerDir.Stop:
            return "停止"
    return "未知"


def export_parsed_data(db_paths, output_file):
    """
    使用DataProcessor处理数据并导出解析结果

    Args:
        db_paths: 数据库路径（支持单个路径字符串或路径列表）
        output_file: 输出文件路径
    """
    # 统一参数为列表
    if isinstance(db_paths, str):
        db_paths = [db_paths]

    # 验证所有数据库文件
    valid_db_paths = []
    for db_path in db_paths:
        if os.path.exists(db_path):
            valid_db_paths.append(db_path)
        else:
            print(f"⚠️ 数据库文件不存在，跳过: {db_path}")

    if not valid_db_paths:
        print(f"❌ 没有有效的数据库文件")
        return

    print(f"🚀 开始处理电液控数据...")
    print(f"📁 数据库文件数: {len(valid_db_paths)}")
    for i, db_path in enumerate(valid_db_paths, 1):
        print(f"   {i}. {db_path}")
    print(f"📄 输出: {output_file}")
    print("=" * 80)

    # 合并所有数据
    all_filtered_data = []
    total_action_count = 0
    total_position_count = 0

    # 处理每个数据库文件
    for i, db_path in enumerate(valid_db_paths, 1):
        print(f"\n📖 正在处理第 {i}/{len(valid_db_paths)} 个数据库...")
        print(f"   文件: {db_path}")

        # 创建DataProcessor实例
        processor = DataProcessor(db_path)

        # 处理数据
        filtered_data = processor.process_data_in_batches()

        if not filtered_data:
            print(f"   ⚠️ 该数据库未找到符合条件的记录")
            continue

        # 统计当前数据库
        action_count = 0
        position_count = 0
        for dt, src_no, parsed_result in filtered_data:
            frame_type = parsed_result.get("frame_type", "")
            if frame_type == "支架动作":
                action_count += 1
            elif frame_type == "煤机位置":
                position_count += 1

        print(
            f"   ✓ 记录数: {len(filtered_data):,} (支架动作: {action_count:,}, 煤机位置: {position_count:,})"
        )

        # 添加到总数据
        all_filtered_data.extend(filtered_data)
        total_action_count += action_count
        total_position_count += position_count

    if not all_filtered_data:
        print("\n⚠️ 所有数据库均未找到符合条件的记录")
        return

    # 打印统计信息
    print("\n" + "=" * 80)
    print("📊 总体统计信息:")
    print("=" * 80)
    print(f"总记录数: {len(all_filtered_data):,}")
    print(f"  - 支架动作: {total_action_count:,}")
    print(f"  - 煤机位置: {total_position_count:,}")

    # 导出到文件
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        # 写入表头
        f.write("时间,源地址,帧类型,详细数据\n")

        # 写入数据
        for dt, src_no, parsed_result in all_filtered_data:
            frame_type = parsed_result.get("frame_type", "未知")
            data = parsed_result.get("data", {})

            # 格式化时间
            time_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # 将详细数据转换为可序列化的格式（枚举转字符串）
            serializable_data = {}
            for key, value in data.items():
                if isinstance(value, ActionType):
                    serializable_data[key] = value.name
                elif isinstance(value, ShearerDir):
                    serializable_data[key] = format_direction(value)
                elif isinstance(value, SensorTypeID):
                    serializable_data[key] = value.name
                else:
                    serializable_data[key] = value

            data_json = json.dumps(serializable_data, ensure_ascii=False)

            # 写入行
            f.write(f"{time_str},{src_no},{frame_type},{data_json}\n")

    print(f"\n✅ 数据已导出到: {output_file}")

    # 显示文件信息
    file_size = os.path.getsize(output_file)
    if file_size > 1024 * 1024:
        print(f"📦 文件大小: {file_size / (1024 * 1024):.2f} MB")
    else:
        print(f"📦 文件大小: {file_size / 1024:.2f} KB")


if __name__ == "__main__":
    # 示例1：导出单个数据库文件
    # db_path = "Datas/电液控UDP驱动_20250904_14.db"
    # output_file = "outPut/电液控数据解析导出.txt"
    # export_parsed_data(db_path, output_file)

    # 示例2：导出多个数据库文件（合并导出）
    db_paths = [
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_14.db",
        # "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_16.db",  # 添加更多数据库文件
        # "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_18.db",  # 添加更多数据库文件
    ]
    output_file = "outPut/电液控数据解析导出.txt"
    export_parsed_data(db_paths, output_file)
