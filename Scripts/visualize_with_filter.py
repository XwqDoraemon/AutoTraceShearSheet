"""
关键点可视化脚本 (带异常点过滤版本)

可视化内容包括：
1. 原始数据和过滤后数据对比
2. 标注异常点
3. 标注关键转折点
4. 区分最小值组和最大值组
"""

import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import matplotlib

matplotlib.use("Agg")

from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Rectangle

from Scripts.anomaly_filter import AnomalyFilter
from Scripts.trajectory_key_points_with_filter import TrajectoryKeyPointsExtractor


class EnhancedVisualizer:
    """增强型可视化器"""

    def __init__(self, extractor: TrajectoryKeyPointsExtractor):
        """
        初始化可视化器

        Args:
            extractor: 关键点提取器实例
        """
        self.extractor = extractor
        self._setup_matplotlib()

    def _setup_matplotlib(self):
        """设置matplotlib中文字体"""
        chinese_fonts = ["SimHei", "Microsoft YaHei", "SimSun", "KaiTi", "FangSong"]

        available_font = None
        for font in chinese_fonts:
            try:
                plt.rcParams["font.sans-serif"] = [font]
                plt.rcParams["axes.unicode_minus"] = False
                available_font = font
                break
            except:
                continue

        if not available_font:
            print("警告: 未找到中文字体，图表中文可能显示为方块")

    def create_comprehensive_visualization(self, output_path: str):
        """
        创建综合可视化图表（包含异常点和关键点）

        Args:
            output_path: 输出图片路径
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 创建大图，包含两个子图
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12))

        # ==================== 子图1：原始数据 + 异常点 ====================
        self._plot_original_data_with_anomalies(ax1)

        # ==================== 子图2：过滤后数据 + 关键点 ====================
        self._plot_filtered_data_with_keypoints(ax2)

        # 调整布局
        plt.tight_layout()

        # 保存图片
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"\n综合可视化图表已保存到: {output_path}")

    def _plot_original_data_with_anomalies(self, ax):
        """绘制原始数据和异常点"""
        # 绘制所有原始位置数据
        if self.extractor.position_data:
            times = [
                datetime.strptime(item["x"], "%Y-%m-%d %H:%M:%S")
                for item in self.extractor.position_data
            ]
            positions = [item["position"] for item in self.extractor.position_data]

            ax.scatter(
                times,
                positions,
                c="lightgray",
                s=15,
                alpha=0.5,
                label="原始位置数据",
                zorder=1,
            )

            # 绘制连接线
            ax.plot(times, positions, c="lightgray", linewidth=0.5, alpha=0.3, zorder=0)

        # 标注异常点
        if self.extractor.anomalies:
            anomaly_times = []
            anomaly_positions = []

            for anomaly in self.extractor.anomalies:
                anomaly_times.append(
                    datetime.strptime(anomaly["data"]["x"], "%Y-%m-%d %H:%M:%S")
                )
                anomaly_positions.append(anomaly["data"]["position"])

            ax.scatter(
                anomaly_times,
                anomaly_positions,
                c="red",
                s=100,
                marker="X",
                alpha=0.8,
                label=f"异常点 ({len(self.extractor.anomalies)}个)",
                zorder=5,
                edgecolors="darkred",
                linewidths=2,
            )

        # 设置子图标题和标签
        ax.set_title("原始数据与异常点", fontsize=13, fontweight="bold", pad=10)
        ax.set_ylabel("位置 (Position)", fontsize=11)
        ax.grid(True, alpha=0.3, linestyle="--")
        ax.legend(loc="best", fontsize=10)

        # 格式化时间轴
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    def _plot_filtered_data_with_keypoints(self, ax):
        """绘制过滤后数据和关键点"""
        # 获取位置范围
        min_pos, max_pos = self.extractor.get_position_range()
        min_range_end = min_pos + 15
        max_range_start = max_pos - 15

        # 绘制过滤后的所有数据
        data_source = (
            self.extractor.filtered_position_data
            if self.extractor.filtered_position_data
            else self.extractor.position_data
        )

        if data_source:
            times = [
                datetime.strptime(item["x"], "%Y-%m-%d %H:%M:%S")
                for item in data_source
            ]
            positions = [item["position"] for item in data_source]

            ax.scatter(
                times,
                positions,
                c="lightblue",
                s=15,
                alpha=0.5,
                label="过滤后数据",
                zorder=1,
            )

            # 绘制连接线
            ax.plot(times, positions, c="lightblue", linewidth=0.5, alpha=0.3, zorder=0)

        # 绘制筛选区域的数据
        filtered_data, _, _ = self.extractor.filter_position_data(
            min_pos, max_pos, range_size=15
        )
        if filtered_data:
            times = [
                datetime.strptime(item["x"], "%Y-%m-%d %H:%M:%S")
                for item in filtered_data
            ]
            positions = [item["position"] for item in filtered_data]

            # 按范围分色
            min_times, min_positions = [], []
            max_times, max_positions = [], []

            for item in filtered_data:
                t = datetime.strptime(item["x"], "%Y-%m-%d %H:%M:%S")
                if item["position"] <= min_range_end:
                    min_times.append(t)
                    min_positions.append(item["position"])
                else:
                    max_times.append(t)
                    max_positions.append(item["position"])

            if min_times:
                ax.scatter(
                    min_times,
                    min_positions,
                    c="orange",
                    s=20,
                    alpha=0.6,
                    label="筛选区域-最小值组",
                    zorder=2,
                )
            if max_times:
                ax.scatter(
                    max_times,
                    max_positions,
                    c="purple",
                    s=20,
                    alpha=0.6,
                    label="筛选区域-最大值组",
                    zorder=2,
                )

        # 绘制关键点
        self._plot_keypoints(ax, min_range_end)

        # 设置子图标题和标签
        ax.set_title("过滤后数据与关键点", fontsize=13, fontweight="bold", pad=10)
        ax.set_xlabel("时间", fontsize=11)
        ax.set_ylabel("位置 (Position)", fontsize=11)
        ax.grid(True, alpha=0.3, linestyle="--")
        ax.legend(loc="best", fontsize=10)

        # 格式化时间轴
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    def _plot_keypoints(self, ax, min_range_end):
        """在图上标注关键点"""
        head_points = []  # 机头扫煤最远点
        tail_points = []  # 机尾扫煤最远点
        min_points = []  # 最小值点
        max_points = []  # 最大值点

        for kp in self.extractor.key_points:
            if kp["group_type"] == "最小值组":
                # 最小值点
                min_point = kp["min_position_point"]
                min_points.append(
                    {
                        "time": datetime.strptime(min_point["x"], "%Y-%m-%d %H:%M:%S"),
                        "position": min_point["position"],
                    }
                )

                # 机头扫煤最远点
                if kp["farthest_sweeping_point"]:
                    farthest = kp["farthest_sweeping_point"]
                    head_points.append(
                        {
                            "time": datetime.strptime(
                                farthest["x"], "%Y-%m-%d %H:%M:%S"
                            ),
                            "position": farthest["position"],
                        }
                    )
            else:
                # 最大值点
                max_point = kp["max_position_point"]
                max_points.append(
                    {
                        "time": datetime.strptime(max_point["x"], "%Y-%m-%d %H:%M:%S"),
                        "position": max_point["position"],
                    }
                )

                # 机尾扫煤最远点
                if kp["farthest_sweeping_point"]:
                    farthest = kp["farthest_sweeping_point"]
                    tail_points.append(
                        {
                            "time": datetime.strptime(
                                farthest["x"], "%Y-%m-%d %H:%M:%S"
                            ),
                            "position": farthest["position"],
                        }
                    )

        # 绘制各类关键点
        if head_points:
            times = [p["time"] for p in head_points]
            positions = [p["position"] for p in head_points]
            ax.scatter(
                times,
                positions,
                c="red",
                s=200,
                marker="^",
                alpha=0.9,
                label=f"机头扫煤最远点 ({len(head_points)}个)",
                zorder=10,
                edgecolors="darkred",
                linewidths=2,
            )

            # 添加文字标注
            for p in head_points:
                ax.annotate(
                    f"{p['position']}",
                    xy=(p["time"], p["position"]),
                    xytext=(5, 5),
                    textcoords="offset points",
                    fontsize=9,
                    fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7),
                )

        if tail_points:
            times = [p["time"] for p in tail_points]
            positions = [p["position"] for p in tail_points]
            ax.scatter(
                times,
                positions,
                c="green",
                s=200,
                marker="v",
                alpha=0.9,
                label=f"机尾扫煤最远点 ({len(tail_points)}个)",
                zorder=10,
                edgecolors="darkgreen",
                linewidths=2,
            )

            # 添加文字标注
            for p in tail_points:
                ax.annotate(
                    f"{p['position']}",
                    xy=(p["time"], p["position"]),
                    xytext=(5, -15),
                    textcoords="offset points",
                    fontsize=9,
                    fontweight="bold",
                    bbox=dict(
                        boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.7
                    ),
                )

        if min_points:
            times = [p["time"] for p in min_points]
            positions = [p["position"] for p in min_points]
            ax.scatter(
                times,
                positions,
                c="orange",
                s=120,
                marker="o",
                alpha=0.7,
                label=f"最小值点 ({len(min_points)}个)",
                zorder=5,
            )

        if max_points:
            times = [p["time"] for p in max_points]
            positions = [p["position"] for p in max_points]
            ax.scatter(
                times,
                positions,
                c="purple",
                s=120,
                marker="s",
                alpha=0.7,
                label=f"最大值点 ({len(max_points)}个)",
                zorder=5,
            )

    def print_keypoints_surrounding_points(self, output_file: str, range_size: int = 2):
        """
        打印每个关键点前后指定范围的数据点

        Args:
            output_file: 输出文件路径
            range_size: 前后范围大小，默认为2
        """
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        data_source = (
            self.extractor.filtered_position_data
            if self.extractor.filtered_position_data
            else self.extractor.position_data
        )

        if not data_source:
            print("警告: 没有数据可用于打印")
            return

        # 按时间排序并创建索引映射
        sorted_data = sorted(data_source, key=lambda x: x["x"])
        time_to_index = {item["x"]: i for i, item in enumerate(sorted_data)}

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("=" * 100 + "\n")
            f.write("关键点周围数据点详情\n")
            f.write("=" * 100 + "\n\n")

            for idx, kp in enumerate(self.extractor.key_points):
                f.write(f"\n{'=' * 100}\n")
                f.write(f"关键点 {idx + 1} [{kp['group_type']}]\n")
                f.write("=" * 100 + "\n\n")

                # 确定关键点的类型和时间
                if kp["group_type"] == "最小值组":
                    main_point = kp["min_position_point"]
                    point_type = "最小值点"
                    point_value = kp["min_position_value"]
                else:
                    main_point = kp["max_position_point"]
                    point_type = "最大值点"
                    point_value = kp["max_position_value"]

                main_time = main_point["x"]
                main_index = time_to_index.get(main_time, -1)

                if main_index == -1:
                    f.write(f"警告: 未找到 {point_type} 在数据中的位置\n")
                    f.write(f"  时间: {main_time}\n")
                    f.write(f"  位置: {point_value}\n\n")
                    continue

                # 打印关键点信息
                f.write(f"主要关键点: {point_type}\n")
                f.write(f"  时间: {main_time}\n")
                f.write(f"  位置: {point_value}\n")
                f.write(f"  方向: {main_point.get('direction', '未知')}\n")
                f.write(f"  在数据中的索引: {main_index}\n\n")

                # 打印扫煤最远点信息（如果有）
                if kp["farthest_sweeping_point"]:
                    farthest = kp["farthest_sweeping_point"]
                    farthest_type = (
                        "机头扫煤最远点"
                        if kp["group_type"] == "最小值组"
                        else "机尾扫煤最远点"
                    )
                    f.write(f"次要关键点: {farthest_type}\n")
                    f.write(f"  时间: {farthest['x']}\n")
                    f.write(f"  位置: {farthest['position']}\n")
                    f.write(f"  方向: {farthest.get('direction', '未知')}\n")
                    farthest_index = time_to_index.get(farthest["x"], -1)
                    f.write(f"  在数据中的索引: {farthest_index}\n\n")

                # 打印主要关键点周围的数据点
                f.write(f"主要关键点前后 {range_size} 个数据点:\n")
                f.write("-" * 100 + "\n")
                f.write(
                    f"{'序号':<8}{'时间':<22}{'位置':<10}{'方向':<10}{'距离关键点':<15}\n"
                )
                f.write("-" * 100 + "\n")

                start_index = max(0, main_index - range_size)
                end_index = min(len(sorted_data), main_index + range_size + 1)

                for i in range(start_index, end_index):
                    item = sorted_data[i]
                    offset = i - main_index
                    offset_str = (
                        f"{'前' if offset < 0 else '后'}{abs(offset)}"
                        if offset != 0
                        else "关键点本身"
                    )

                    f.write(
                        f"{i:<8}{item['x']:<22}{item['position']:<10}"
                        f"{item.get('direction', '未知'):<10}{offset_str:<15}\n"
                    )

                f.write("\n")

                # 如果有扫煤最远点，也打印其周围的数据点
                if kp["farthest_sweeping_point"]:
                    farthest = kp["farthest_sweeping_point"]
                    farthest_index = time_to_index.get(farthest["x"], -1)

                    if farthest_index != -1:
                        farthest_type = (
                            "机头扫煤最远点"
                            if kp["group_type"] == "最小值组"
                            else "机尾扫煤最远点"
                        )
                        f.write(f"{farthest_type}前后 {range_size} 个数据点:\n")
                        f.write("-" * 100 + "\n")
                        f.write(
                            f"{'序号':<8}{'时间':<22}{'位置':<10}{'方向':<10}{'距离关键点':<15}\n"
                        )
                        f.write("-" * 100 + "\n")

                        start_index = max(0, farthest_index - range_size)
                        end_index = min(
                            len(sorted_data), farthest_index + range_size + 1
                        )

                        for i in range(start_index, end_index):
                            item = sorted_data[i]
                            offset = i - farthest_index
                            offset_str = (
                                f"{'前' if offset < 0 else '后'}{abs(offset)}"
                                if offset != 0
                                else "关键点本身"
                            )

                            f.write(
                                f"{i:<8}{item['x']:<22}{item['position']:<10}"
                                f"{item.get('direction', '未知'):<10}{offset_str:<15}\n"
                            )

                        f.write("\n")

            f.write("\n" + "=" * 100 + "\n")
            f.write("数据统计\n")
            f.write("=" * 100 + "\n")
            f.write(f"总数据点数: {len(sorted_data)}\n")
            f.write(f"关键点数量: {len(self.extractor.key_points)}\n")
            f.write(f"时间范围: {sorted_data[0]['x']} 至 {sorted_data[-1]['x']}\n")

        print(f"\n关键点周围数据点已保存到: {output_file}")


def create_single_view_visualization(output_path: str):
    """
    创建单图版可视化（原始数据和关键点在一张图上）

    Args:
        output_path: 输出图片路径
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    scatter_json_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "dashboard",
        "public",
        "data",
        "scatter.json",
    )

    # 创建提取器
    extractor = TrajectoryKeyPointsExtractor(scatter_json_path)
    extractor.extract_key_points(enable_filter=True, max_change_threshold=10)

    # 创建图表
    fig, ax = plt.subplots(figsize=(20, 10))

    # 设置中文字体
    chinese_fonts = ["SimHei", "Microsoft YaHei", "SimSun"]
    for font in chinese_fonts:
        try:
            plt.rcParams["font.sans-serif"] = [font]
            plt.rcParams["axes.unicode_minus"] = False
            break
        except:
            pass

    # 绘制原始数据（灰色背景）
    if extractor.position_data:
        times = [
            datetime.strptime(item["x"], "%Y-%m-%d %H:%M:%S")
            for item in extractor.position_data
        ]
        positions = [item["position"] for item in extractor.position_data]

        ax.scatter(
            times, positions, c="lightgray", s=10, alpha=0.3, label="原始数据", zorder=1
        )

    # 标注异常点
    if extractor.anomalies:
        anomaly_times = [
            datetime.strptime(a["data"]["x"], "%Y-%m-%d %H:%M:%S")
            for a in extractor.anomalies
        ]
        anomaly_positions = [a["data"]["position"] for a in extractor.anomalies]

        ax.scatter(
            anomaly_times,
            anomaly_positions,
            c="red",
            s=80,
            marker="X",
            alpha=0.6,
            label=f"异常点 ({len(extractor.anomalies)}个)",
            zorder=3,
        )

    # 绘制过滤后的数据
    if extractor.filtered_position_data:
        times = [
            datetime.strptime(item["x"], "%Y-%m-%d %H:%M:%S")
            for item in extractor.filtered_position_data
        ]
        positions = [item["position"] for item in extractor.filtered_position_data]

        ax.scatter(
            times, positions, c="blue", s=15, alpha=0.5, label="过滤后数据", zorder=2
        )

    # 绘制关键点
    min_pos, max_pos = extractor.get_position_range()
    min_range_end = min_pos + 15

    head_points = []
    tail_points = []

    for kp in extractor.key_points:
        if kp["group_type"] == "最小值组":
            if kp["farthest_sweeping_point"]:
                farthest = kp["farthest_sweeping_point"]
                head_points.append(
                    {
                        "time": datetime.strptime(farthest["x"], "%Y-%m-%d %H:%M:%S"),
                        "position": farthest["position"],
                    }
                )
        else:
            if kp["farthest_sweeping_point"]:
                farthest = kp["farthest_sweeping_point"]
                tail_points.append(
                    {
                        "time": datetime.strptime(farthest["x"], "%Y-%m-%d %H:%M:%S"),
                        "position": farthest["position"],
                    }
                )

    if head_points:
        times = [p["time"] for p in head_points]
        positions = [p["position"] for p in head_points]
        ax.scatter(
            times,
            positions,
            c="red",
            s=250,
            marker="^",
            alpha=0.9,
            label=f"机头扫煤最远点 ({len(head_points)}个)",
            zorder=5,
            edgecolors="darkred",
            linewidths=2,
        )

        for p in head_points:
            ax.annotate(
                f"机头: {p['position']}",
                xy=(p["time"], p["position"]),
                xytext=(10, 10),
                textcoords="offset points",
                fontsize=10,
                fontweight="bold",
                color="red",
                bbox=dict(boxstyle="round,pad=0.5", facecolor="yellow", alpha=0.8),
            )

    if tail_points:
        times = [p["time"] for p in tail_points]
        positions = [p["position"] for p in tail_points]
        ax.scatter(
            times,
            positions,
            c="green",
            s=250,
            marker="v",
            alpha=0.9,
            label=f"机尾扫煤最远点 ({len(tail_points)}个)",
            zorder=5,
            edgecolors="darkgreen",
            linewidths=2,
        )

        for p in tail_points:
            ax.annotate(
                f"机尾: {p['position']}",
                xy=(p["time"], p["position"]),
                xytext=(10, -20),
                textcoords="offset points",
                fontsize=10,
                fontweight="bold",
                color="green",
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgreen", alpha=0.8),
            )

    # 设置标题和标签
    ax.set_title(
        "采煤机轨迹关键点可视化（带异常点过滤）", fontsize=15, fontweight="bold", pad=15
    )
    ax.set_xlabel("时间", fontsize=12)
    ax.set_ylabel("位置 (Position)", fontsize=12)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.legend(loc="best", fontsize=11)

    # 格式化时间轴
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)

    # 调整布局
    plt.tight_layout()

    # 保存图片
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"\n单图版可视化已保存到: {output_path}")


def main():
    """主函数"""
    # 文件路径
    scatter_json_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "dashboard",
        "public",
        "data",
        "scatter.json",
    )

    key_points_txt_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output",
        "trajectory_key_points_with_filter.txt",
    )

    # 两个可视化输出
    comprehensive_output = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output",
        "trajectory_visualization_comprehensive.png",
    )

    single_output = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output",
        "trajectory_visualization_single.png",
    )

    # 关键点周围数据点输出
    surrounding_points_output = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output",
        "keypoints_surrounding_points.txt",
    )

    # 提取关键点
    print("=" * 60)
    print("步骤1: 提取关键点（带异常点过滤）")
    print("=" * 60)
    extractor = TrajectoryKeyPointsExtractor(scatter_json_path)
    extractor.extract_key_points(enable_filter=True, max_change_threshold=10)
    extractor.save_key_points_to_file(key_points_txt_path)

    # 创建综合可视化（双图）
    print("\n" + "=" * 60)
    print("步骤2: 创建综合可视化（双图版）")
    print("=" * 60)
    visualizer = EnhancedVisualizer(extractor)
    visualizer.create_comprehensive_visualization(comprehensive_output)

    # 创建单图版可视化
    print("\n" + "=" * 60)
    print("步骤3: 创建单图版可视化")
    print("=" * 60)
    create_single_view_visualization(single_output)

    # 打印关键点周围的数据点
    print("\n" + "=" * 60)
    print("步骤4: 打印关键点周围的数据点")
    print("=" * 60)
    visualizer.print_keypoints_surrounding_points(
        surrounding_points_output, range_size=2
    )

    print("\n" + "=" * 60)
    print("处理完成!")
    print("=" * 60)
    print(f"关键点文件: {key_points_txt_path}")
    print(f"综合可视化: {comprehensive_output}")
    print(f"单图可视化: {single_output}")
    print(f"周围数据点: {surrounding_points_output}")


if __name__ == "__main__":
    main()
