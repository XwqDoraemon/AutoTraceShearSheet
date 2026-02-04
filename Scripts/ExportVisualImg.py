#!/usr/bin/env python3
"""
电液控数据处理程序 - 主入口
"""

from Src import DataProcessor, DataVisualizer


def main():
    """主函数"""
    db_path = "Datas/电液控UDP驱动_20250904_14.db"

    print("🚀 开始处理电液控数据...")
    print("=" * 60)

    try:
        # 创建数据处理器
        processor = DataProcessor(db_path)

        # 处理数据
        print("🔍 正在提取符合条件的数据 (b_pri=3, b_cmd=4)...")
        filtered_data = processor.process_data_in_batches()

        if not filtered_data:
            print("❌ 未找到符合条件的数据！")
            return

        # 创建可视化
        print("\n📊 正在创建可视化图表...")
        visualizer = DataVisualizer()
        visualizer.create_visualization(
            filtered_data, output_file="human_operation_visualization.png"
        )

        print("\n🎉 处理完成！")

    except Exception as e:
        print(f"❌ 处理过程中发生错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
