#!/usr/bin/env python3
"""
电液控数据导出程序 (增强版)
使用DataProcessor处理数据，并将解析结果导出和打印到控制台
"""

import json
import os

from Src import DataProcessor
from util.action_receiver import ActionType
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


def export_parsed_data(db_path, output_file):
    """
    使用DataProcessor处理数据并导出解析结果

    Args:
        db_path: 数据库路径
        output_file: 输出文件路径
    """
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return

    print(f"🚀 开始处理电液控数据...")
    print(f"📁 数据库: {db_path}")
    print(f"📄 输出: {output_file}")
    print("=" * 80)

    # 创建DataProcessor实例
    processor = DataProcessor(db_path)

    # 处理数据
    filtered_data = processor.process_data_in_batches()

    if not filtered_data:
        print("⚠️ 未找到符合条件的记录")
        return

    # 统计信息
    action_count = 0
    position_count = 0

    # 打印统计信息
    print("\n" + "=" * 80)
    print("📊 统计信息:")
    print("=" * 80)
    print(f"总记录数: {len(filtered_data):,}")
    print(f"  - 支架动作: {action_count:,}")
    print(f"  - 煤机位置: {position_count:,}")

    # 导出到文件
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        # 写入表头
        f.write("时间,源地址,帧类型,详细数据\n")

        # 写入数据
        for dt, src_no, parsed_result in filtered_data:
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
    db_path = "Datas/电液控UDP驱动_20250904_14.db"
    output_file = "outPut/电液控数据解析导出.txt"

    export_parsed_data(db_path, output_file)
