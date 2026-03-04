#!/usr/bin/env python3
"""
跟机轨迹分割脚本

功能：
1. 从.txt/.csv或数据库中读取数据
2. 根据煤机位置数据检测周期（两个最小值之间的数据为一个周期）
3. 使用流式处理逐批加载数据，边加载边处理边可视化
4. 为每个周期绘制跟机轨迹图

周期检测逻辑：
- 先处理2000条数据，确定最小值和最大值范围
- 通过传感器数据的src地址动态更新最小值和最大值
- 最小值计算：当数据首次落在（最大值-最小值）*50%+最小值，且整体趋势下降时，开始计算最小值
- 最大值计算：当数据首次落在最大值-（最大值-最小值）*50%时，且整体趋势上升时，开始计算最大值
- 两个最小值之间的数据为一个周期
"""

import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from Src.streaming_data_loader import StreamingDataLoader


@dataclass
class CycleData:
    """周期数据类"""

    start_time: datetime  # 周期开始时间（第一个最小值点）
    end_time: datetime  # 周期结束时间（第二个最小值点）
    start_index: int  # 周期开始索引
    end_index: int  # 周期结束索引
    shearer_positions: List[dict] = field(default_factory=list)  # 煤机位置数据
    sensor_data: Dict[int, List[dict]] = field(
        default_factory=dict
    )  # 传感器数据，按src分组
    min_position: Optional[int] = None  # 最小值
    max_position: Optional[int] = None  # 最大值

    def add_shearer_position(self, timestamp: datetime, position: int, direction: str):
        """添加煤机位置数据"""
        self.shearer_positions.append(
            {
                "timestamp": timestamp,
                "position": position,
                "direction": direction,
            }
        )
        # 更新最小值和最大值
        if self.min_position is None or position < self.min_position:
            self.min_position = position
        if self.max_position is None or position > self.max_position:
            self.max_position = position

    def add_sensor_data(
        self, timestamp: datetime, src: int, sensor_type: int, value: float
    ):
        """添加传感器数据"""
        if src not in self.sensor_data:
            self.sensor_data[src] = []
        self.sensor_data[src].append(
            {
                "timestamp": timestamp,
                "sensor_type": sensor_type,
                "value": value,
            }
        )


@dataclass
class RangeInfo:
    """位置范围信息"""

    min_value: int  # 最小值
    max_value: int  # 最大值
    min_threshold: float  # 最小值阈值 (真正最小值的上限)
    max_threshold: float  # 最大值阈值 (真正最大值的下限)
    mid_threshold: float  # 中间阈值
    total_range: int  # 范围差值

    def update(self, new_min: int, new_max: int):
        """更新范围"""
        if new_min < self.min_value:
            self.min_value = new_min
        if new_max > self.max_value:
            self.max_value = new_max
        self._recalculate_thresholds()

    def _recalculate_thresholds(self):
        """重新计算阈值"""
        self.total_range = self.max_value - self.min_value
        self.min_threshold = (
            self.min_value + self.total_range * 0.1
        )  # 真正最小值上限: min + 10%
        self.max_threshold = (
            self.max_value - self.total_range * 0.1
        )  # 真正最大值下限: max - 10%
        self.mid_threshold = (
            self.min_value + self.total_range * 0.5
        )  # 中间阈值: min + 50%


class TraceSplitter:
    """跟机轨迹分割器"""

    def __init__(
        self,
        data_loader: StreamingDataLoader,
        test_mode: bool = True,
    ):
        """
        初始化分割器

        Args:
            data_loader: StreamingDataLoader实例（已初始化）
            test_mode: 测试模式，只绘制一张图后结束
        """
        self.data_loader = data_loader
        self.input_source = data_loader.source
        self.source_type = data_loader.source_type
        self.batch_size = data_loader.batch_size
        self.test_mode = test_mode

        # 数据存储
        self.all_data: List[
            Tuple[datetime, int, dict]
        ] = []  # 所有数据 (timestamp, src, parsed_data)
        self.shearer_positions: List[
            Tuple[datetime, int, int, str]
        ] = []  # (timestamp, src, position, direction)
        self.last_shearer_position: Optional[int] = (
            None  # 上一次的煤机位置，用于过滤重复值
        )
        self.filtered_duplicate_count = 0  # 过滤掉的重复位置计数

        # 范围信息
        self.range_info: Optional[RangeInfo] = None
        self.initial_batch_processed = False

        # 周期数据
        self.cycles: List[CycleData] = []

        # 设置matplotlib中文字体
        self._setup_matplotlib()

    def _setup_matplotlib(self):
        """设置matplotlib中文字体"""
        chinese_fonts = ["SimHei", "Microsoft YaHei", "SimSun", "KaiTi", "FangSong"]
        for font in chinese_fonts:
            try:
                plt.rcParams["font.sans-serif"] = [font]
                plt.rcParams["axes.unicode_minus"] = False
                break
            except:
                continue

    def load_data(self):
        """加载数据（使用传入的StreamingDataLoader）"""
        print("=" * 80)
        print(f"正在加载数据: {self.input_source}")
        print("=" * 80)

        # 使用传入的StreamingDataLoader加载数据
        total_processed = 0
        batch_count = 0

        for timestamp, src, parsed_data in self.data_loader.load_data():
            batch_count += 1 if batch_count == 0 else batch_count
            self.all_data.append((timestamp, src, parsed_data))

            # 提取煤机位置数据
            if parsed_data.get("frame_type") == "煤机位置":
                data = parsed_data.get("data", {})
                position = data.get("position")
                direction = data.get("dir")  # 使用 "dir" 而不是 "direction"
                if position is not None:
                    # 过滤重复位置值
                    if (
                        self.last_shearer_position is not None
                        and position == self.last_shearer_position
                    ):
                        self.filtered_duplicate_count += 1
                    else:
                        self.shearer_positions.append(
                            (timestamp, src, position, str(direction))
                        )
                        self.last_shearer_position = position

            total_processed = len(self.all_data)

            # 每10000条打印一次进度
            if total_processed % 10000 == 0:
                print(f"   已处理 {total_processed:,} 条数据")

        print(f"[OK] 数据加载完成！")
        print(f"   总数据量: {len(self.all_data):,}")
        print(f"   煤机位置数据: {len(self.shearer_positions):,}")
        if self.filtered_duplicate_count > 0:
            print(f"   过滤重复位置: {self.filtered_duplicate_count:,} 条")

    def _initialize_range(self):
        """初始化位置范围（处理前2000条数据）"""
        print("\n" + "=" * 80)
        print("步骤1: 初始化位置范围（处理前2000条数据）")
        print("=" * 80)

        # 从煤机位置数据中获取最小值和最大值
        if len(self.shearer_positions) < 100:
            raise ValueError("数据量太少，无法确定位置范围")

        # 取前100条或所有数据
        sample_size = min(100, len(self.shearer_positions))
        sample_positions = [
            pos for _, _, pos, _ in self.shearer_positions[:sample_size]
        ]

        min_val = min(sample_positions)
        max_val = max(sample_positions)

        # 同时从传感器数据的src地址中获取范围
        sensor_srcs = set()
        for timestamp, src, parsed_data in self.all_data[:sample_size]:
            if parsed_data.get("frame_type") == "传感器数据":
                sensor_srcs.add(src)

        if sensor_srcs:
            sensor_min = min(sensor_srcs)
            sensor_max = max(sensor_srcs)
            # 扩展范围
            min_val = min(min_val, sensor_min)
            max_val = max(max_val, sensor_max)

        self.range_info = RangeInfo(
            min_value=min_val,
            max_value=max_val,
            min_threshold=0,
            max_threshold=0,
            mid_threshold=0,
            total_range=max_val - min_val,
        )
        self.range_info._recalculate_thresholds()

        self.initial_batch_processed = True

        print(f"[OK] 位置范围初始化完成")
        print(f"   最小值: {self.range_info.min_value}")
        print(f"   最大值: {self.range_info.max_value}")
        print(f"   范围: {self.range_info.total_range}")
        print(f"   真正最小值上限 (10%): {self.range_info.min_threshold:.1f}")
        print(f"   真正最大值下限 (10%): {self.range_info.max_threshold:.1f}")
        print(f"   中间阈值 (50%): {self.range_info.mid_threshold:.1f}")

    def _detect_trend(self, positions: List[int], window_size: int = 5) -> str:
        """
        检测位置趋势

        Args:
            positions: 位置列表
            window_size: 检测窗口大小

        Returns:
            "up"（上升）, "down"（下降）, "stable"（平稳）
        """
        if len(positions) < window_size:
            return "stable"

        # 计算最近window_size个点的趋势
        recent = positions[-window_size:]
        # 使用线性回归判断趋势
        x = np.arange(len(recent))
        y = np.array(recent)

        # 简单线性回归
        if len(x) > 1:
            slope = np.polyfit(x, y, 1)[0]
            if slope > 0.5:
                return "up"
            elif slope < -0.5:
                return "down"

        return "stable"

    def _find_max_after_outlier_removal(
        self, positions: List[int], start_index: int
    ) -> Optional[int]:
        """
        去除离群点后查找最大值的索引

        使用 IQR (四分位距) 方法检测并去除离群点，然后找到最大值

        Args:
            positions: 位置值列表
            start_index: 起始索引（用于返回绝对索引）

        Returns:
            最大值在原列表中的相对索引，如果无法确定则返回None
        """
        if len(positions) < 10:
            # 数据点太少，直接找最大值
            return positions.index(max(positions))

        # 转换为numpy数组
        arr = np.array(positions)

        # 计算四分位数
        q1 = np.percentile(arr, 25)
        q3 = np.percentile(arr, 75)
        iqr = q3 - q1

        # 定义离群点边界（使用1.5倍IQR）
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # 过滤离群点
        filtered_mask = (arr >= lower_bound) & (arr <= upper_bound)
        filtered_positions = arr[filtered_mask]

        if len(filtered_positions) == 0:
            # 所有数据都被视为离群点，返回原始最大值的索引
            return positions.index(max(positions))

        # 在过滤后的数据中找最大值
        max_value = np.max(filtered_positions)

        # 找到第一个最大值的位置（可能有多个相同的最大值）
        for i, pos in enumerate(positions):
            if pos == max_value and filtered_mask[i]:
                return i

        return None

    def _find_cycles(self):
        """检测周期 - 使用中间阈值判断周期结束"""

        print("步骤2: 检测周期 (使用中间阈值判断)")
        print("=" * 80)

        if not self.range_info:
            self._initialize_range()

        positions_only = [pos for _, _, pos, _ in self.shearer_positions]
        all_timestamps = [ts for ts, _, _, _ in self.shearer_positions]

        cycle_start_indices = []  # 周期开始索引列表（最小值点）

        # 状态变量
        current_cycle_start = None  # 当前周期开始的索引
        min_candidate_index = None  # 最小值候选点索引

        total_positions = len(positions_only)
        progress_interval = max(1, total_positions // 20)  # 分20个进度段

        print(f"正在扫描周期... (总共 {total_positions:,} 个位置点)")

        for i, position in enumerate(positions_only):
            # 打印进度
            if i % progress_interval == 0 or i == total_positions - 1:
                progress = (i + 1) / total_positions * 100
                print(
                    f"   进度: {i + 1:,}/{total_positions:,} ({progress:.1f}%) - 已检测到 {len(self.cycles)} 个周期"
                )

            # 动态更新范围
            self.range_info.update(position, position)

            # 追踪最小值候选点
            if (
                min_candidate_index is None
                or position < positions_only[min_candidate_index]
            ):
                min_candidate_index = i

            # 当位置超过中间阈值时，检查最小值是否合格
            if position > self.range_info.mid_threshold:
                if min_candidate_index is not None and current_cycle_start is not None:
                    # 检查最小值是否在真正最小值范围内
                    min_value = positions_only[min_candidate_index]
                    if min_value <= self.range_info.min_threshold:
                        # 这是一个合格的最小值，创建周期
                        if min_candidate_index not in cycle_start_indices:
                            # 创建上一个周期
                            self._create_cycle(
                                cycle_start_indices[-1] if cycle_start_indices else 0,
                                min_candidate_index,
                                all_timestamps,
                                positions_only,
                            )
                            cycle_start_indices.append(min_candidate_index)
                            current_cycle_start = min_candidate_index

                            prev_index = (
                                cycle_start_indices[-2]
                                if len(cycle_start_indices) > 1
                                else 0
                            )
                            cycle_length = min_candidate_index - prev_index
                            print(
                                f"   ✓ 检测到周期 #{len(self.cycles)}: 索引 {prev_index} -> {min_candidate_index} "
                            )
                            print(
                                f"      长度: {cycle_length} 个位置点, 最小值: {min_value} (≤ {self.range_info.min_threshold:.2f})"
                            )

                            # 测试模式：只检测一个周期
                            if self.test_mode and len(self.cycles) >= 1:
                                break
                    else:
                        # 最小值不合格（不在真正最小值范围内），继续等待
                        print(
                            f"   ! 最小值 {min_value} 超出阈值 {self.range_info.min_threshold:.2f}，继续检测..."
                        )

                elif current_cycle_start is None and min_candidate_index is not None:
                    # 第一个周期的开始
                    min_value = positions_only[min_candidate_index]
                    if min_value <= self.range_info.min_threshold:
                        current_cycle_start = min_candidate_index
                        cycle_start_indices.append(min_candidate_index)
                        print(
                            f"   → 检测到周期起始点: 索引 {min_candidate_index}, 位置 {min_value}"
                        )

        print(f"[OK] 周期检测完成！共检测到 {len(self.cycles)} 个周期")

    def _create_cycle(
        self,
        start_idx: int,
        end_idx: int,
        all_timestamps: List[datetime],
        all_positions: List[int],
    ):
        """创建周期数据"""
        cycle = CycleData(
            start_time=all_timestamps[start_idx],
            end_time=all_timestamps[end_idx],
            start_index=start_idx,
            end_index=end_idx,
        )

        # 添加煤机位置数据
        positions_added = 0
        for i in range(start_idx, end_idx + 1):
            timestamp = all_timestamps[i]
            position = all_positions[i]
            # 从原始数据中查找方向信息
            direction = "未知"
            for ts, _, pos, dir in self.shearer_positions:
                if ts == timestamp and pos == position:
                    direction = dir
                    break
            cycle.add_shearer_position(timestamp, position, direction)
            positions_added += 1

        print(f"      添加煤机位置数据: {positions_added} 个点")
        print(
            f"      起点: 索引{start_idx}, 位置{all_positions[start_idx]}, 时间{all_timestamps[start_idx]}"
        )
        print(
            f"      终点: 索引{end_idx}, 位置{all_positions[end_idx]}, 时间{all_timestamps[end_idx]}"
        )
        if positions_added > 0:
            print(
                f"      前3个点: {[(all_positions[start_idx + i], all_timestamps[start_idx + i]) for i in range(min(3, positions_added))]}"
            )
            print(
                f"      后3个点: {[(all_positions[end_idx - 2 + i], all_timestamps[end_idx - 2 + i]) for i in range(min(3, positions_added))]}"
            )

        # 添加传感器数据（在此时间范围内的）
        cycle_start_time = cycle.start_time
        cycle_end_time = cycle.end_time

        sensor_count = 0
        src_count = 0
        processed_srcs = set()

        # 统计传感器类型分布
        sensor_type_stats = {}

        for timestamp, src, parsed_data in self.all_data:
            if cycle_start_time <= timestamp <= cycle_end_time:
                if parsed_data.get("frame_type") == "传感器数据":
                    data = parsed_data.get("data", {})

                    # 处理两种数据格式：
                    # 1. 列表格式: [{"sensor_type": ..., "value": ...}, ...]
                    # 2. 单个对象格式: {"sensor_type": ..., "value": ...}
                    if isinstance(data, list):
                        data_list = data
                    elif isinstance(data, dict):
                        data_list = [data]
                    else:
                        continue

                    for sensor_item in data_list:
                        if not isinstance(sensor_item, dict):
                            continue
                        sensor_type = sensor_item.get("sensor_type")
                        value = sensor_item.get("value")
                        if sensor_type is not None and value is not None:
                            cycle.add_sensor_data(timestamp, src, sensor_type, value)
                            sensor_count += 1

                            # 统计传感器类型
                            if sensor_type not in sensor_type_stats:
                                sensor_type_stats[sensor_type] = 0
                            sensor_type_stats[sensor_type] += 1

                            if src not in processed_srcs:
                                processed_srcs.add(src)
                                src_count += 1

        print(f"      添加传感器数据: {sensor_count} 条, 来自 {src_count} 个支架")

        # 打印传感器类型统计
        if sensor_type_stats:
            print(f"      传感器类型分布:")
            # 按数量排序
            sorted_types = sorted(
                sensor_type_stats.items(), key=lambda x: x[1], reverse=True
            )
            for sensor_type, count in sorted_types[:5]:  # 只显示前5个
                print(f"         类型{sensor_type}: {count} 条")
        else:
            print(f"      [警告] 此周期内没有传感器数据")

        self.cycles.append(cycle)

    def _plot_single_cycle(self, cycle: CycleData, output_path: str):
        """绘制单个周期的轨迹

        为每个支架生成单独的前溜行程曲线图

        Args:
            cycle: 周期数据
            output_path: 煤机位置图的输出路径（用于生成文件夹名称）
        """
        # 创建周期专属文件夹
        output_dir = os.path.dirname(output_path)
        cycle_folder = os.path.splitext(os.path.basename(output_path))[0]
        cycle_output_dir = os.path.join(output_dir, cycle_folder)
        os.makedirs(cycle_output_dir, exist_ok=True)

        # 生成煤机位置轨迹图（放在周期文件夹内）
        shearer_output_path = os.path.join(cycle_output_dir, "shearer_position.png")
        self._plot_shearer_position(cycle, shearer_output_path)

        # 为每个支架生成前溜行程曲线图（放在周期文件夹内）
        # 统计每个支架的数据点数量
        src_data_count = {}
        for src, data_list in cycle.sensor_data.items():
            src_data_count[src] = len(data_list)

        # 按支架编号排序
        sorted_srcs = sorted(src_data_count.keys())

        # 将支架每8个一组绘制
        sensors_per_plot = 8
        total_plots = (len(sorted_srcs) + sensors_per_plot - 1) // sensors_per_plot

        print(
            f"      正在为 {len(sorted_srcs)} 个支架绘制前溜行程曲线（每张图8个支架，共{total_plots}张图）..."
        )

        for plot_idx in range(total_plots):
            start_idx = plot_idx * sensors_per_plot
            end_idx = min(start_idx + sensors_per_plot, len(sorted_srcs))
            src_batch = sorted_srcs[start_idx:end_idx]

            # 文件名格式：sensors_起始架号-结束架号.png
            sensor_output_path = os.path.join(
                cycle_output_dir, f"sensors_{src_batch[0]}-{src_batch[-1]}.png"
            )
            self._plot_multiple_sensors(
                cycle, src_batch, sensor_output_path, plot_idx + 1
            )

        # 生成时空热力图（放在周期文件夹内）
        print("      正在生成时空热力图...")
        heatmap_output_path = os.path.join(
            cycle_output_dir, "spatiotemporal_heatmap.png"
        )
        self._plot_spatiotemporal_heatmap(cycle, heatmap_output_path)

    def _plot_shearer_position(self, cycle: CycleData, output_path: str):
        """绘制煤机位置轨迹"""
        fig, ax = plt.subplots(1, 1, figsize=(16, 6))

        if cycle.shearer_positions:
            times = [p["timestamp"] for p in cycle.shearer_positions]
            positions = [p["position"] for p in cycle.shearer_positions]

            ax.plot(
                times,
                positions,
                "b-",
                linewidth=2,
                label="煤机位置",
                marker="o",
                markersize=4,
            )
            ax.scatter(
                times[0],
                positions[0],
                c="green",
                s=150,
                marker="o",
                label="周期开始",
                zorder=5,
            )
            ax.scatter(
                times[-1],
                positions[-1],
                c="red",
                s=150,
                marker="o",
                label="周期结束",
                zorder=5,
            )

            # 标注最小值和最大值
            if cycle.min_position is not None:
                min_indices = [
                    i for i, p in enumerate(positions) if p == cycle.min_position
                ]
                for idx in min_indices:
                    ax.scatter(
                        times[idx],
                        positions[idx],
                        c="orange",
                        s=120,
                        marker="^",
                        label="最小值",
                        zorder=4,
                    )
            if cycle.max_position is not None:
                max_indices = [
                    i for i, p in enumerate(positions) if p == cycle.max_position
                ]
                for idx in max_indices:
                    ax.scatter(
                        times[idx],
                        positions[idx],
                        c="purple",
                        s=120,
                        marker="v",
                        label="最大值",
                        zorder=4,
                    )

            ax.set_title(
                f"煤机位置轨迹\n周期: {cycle.start_time.strftime('%H:%M:%S')} -> {cycle.end_time.strftime('%H:%M:%S')}",
                fontsize=12,
                fontweight="bold",
            )
            ax.set_xlabel("时间", fontsize=11)
            ax.set_ylabel("位置", fontsize=11)
            ax.grid(True, alpha=0.3, linestyle="--")
            ax.legend(loc="best", fontsize=10)

            # 格式化时间轴
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

    def _plot_multiple_sensors(
        self,
        cycle: CycleData,
        src_list: List[int],
        output_path: str,
        plot_index: int = 1,
    ):
        """为多个支架绘制前溜行程曲线（每张图8个支架）

        Args:
            cycle: 周期数据
            src_list: 支架编号列表
            output_path: 输出文件路径
            plot_index: 图索引（用于标题）
        """
        num_sensors = len(src_list)

        # 固定布局：1列，顶部+煤机位置，下方+8个支架（垂直排布）
        cols = 1
        rows = num_sensors + 1  # +1 为煤机位置图

        # 创建子图网格
        fig, axes = plt.subplots(rows, cols, figsize=(12, 2.5 * rows))

        # 处理axes的形状，使其始终是一维数组
        if rows == 1:
            axes = [axes]

        # 第一个子图：绘制煤机位置
        ax_shearer = axes[0]
        if cycle.shearer_positions:
            times = [p["timestamp"] for p in cycle.shearer_positions]
            positions = [p["position"] for p in cycle.shearer_positions]

            ax_shearer.plot(
                times,
                positions,
                "g-",
                linewidth=2,
                label="煤机位置",
                marker="o",
                markersize=3,
            )
            ax_shearer.scatter(
                [times[0]],
                [positions[0]],
                c="blue",
                s=80,
                marker="o",
                label="周期开始",
                zorder=5,
            )
            ax_shearer.scatter(
                [times[-1]],
                [positions[-1]],
                c="red",
                s=80,
                marker="o",
                label="周期结束",
                zorder=5,
            )

            ax_shearer.set_title(
                "煤机位置轨迹",
                fontsize=11,
                fontweight="bold",
            )
            ax_shearer.set_ylabel("位置", fontsize=9)
            ax_shearer.grid(True, alpha=0.3, linestyle="--")
            ax_shearer.legend(loc="best", fontsize=8)

            # 格式化时间轴
            ax_shearer.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            plt.setp(ax_shearer.xaxis.get_majorticklabels(), rotation=45, fontsize=8)
            plt.setp(ax_shearer.yaxis.get_majorticklabels(), fontsize=8)

        # 只在煤机位置图显示x轴标签（如果只有一个支架）或隐藏
        if num_sensors > 0:
            ax_shearer.set_xlabel("")

        # 为每个支架绘制曲线（从第二个子图开始）
        for idx, src in enumerate(src_list):
            ax = axes[idx + 1]  # +1 跳过煤机位置图

            if src in cycle.sensor_data:
                data_list = cycle.sensor_data[src]
                times = [d["timestamp"] for d in data_list]
                values = [d["value"] for d in data_list]

                if times and values:
                    ax.plot(times, values, "b-", linewidth=1.5, label=f"支架{src}")

                    # 标注最大值和最小值
                    min_val = min(values)
                    max_val = max(values)
                    min_idx = values.index(min_val)
                    max_idx = values.index(max_val)

                    ax.scatter(
                        [times[min_idx]],
                        [min_val],
                        c="red",
                        s=30,
                        marker="v",
                        zorder=5,
                    )
                    ax.scatter(
                        [times[max_idx]],
                        [max_val],
                        c="green",
                        s=30,
                        marker="^",
                        zorder=5,
                    )

                    ax.set_title(
                        f"支架{src} ({len(values)}点)",
                        fontsize=10,
                        fontweight="bold",
                    )
                    ax.grid(True, alpha=0.3, linestyle="--")

                    # 格式化时间轴
                    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=8)
                    plt.setp(ax.yaxis.get_majorticklabels(), fontsize=8)
            else:
                ax.text(
                    0.5,
                    0.5,
                    f"支架{src}\n无数据",
                    ha="center",
                    va="center",
                    fontsize=10,
                    transform=ax.transAxes,
                )

            # 只在最后一个子图显示x轴标签
            if idx < num_sensors - 1:
                ax.set_xlabel("")
            else:
                ax.set_xlabel("时间", fontsize=9)

        # 设置总标题
        fig.suptitle(
            f"支架前溜行程曲线（第{plot_index}组，支架{src_list[0]}-{src_list[-1]}）\n"
            f"周期: {cycle.start_time.strftime('%H:%M:%S')} -> {cycle.end_time.strftime('%H:%M:%S')}",
            fontsize=12,
            fontweight="bold",
        )

        plt.tight_layout(rect=[0, 0, 1, 0.94])  # 为总标题留出空间
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

    def _plot_single_sensor(self, cycle: CycleData, src: int, output_path: str):
        """为单个支架绘制前溜行程曲线"""
        fig, ax = plt.subplots(1, 1, figsize=(16, 6))

        if src in cycle.sensor_data:
            data_list = cycle.sensor_data[src]
            times = [d["timestamp"] for d in data_list]
            values = [d["value"] for d in data_list]

            if times and values:
                ax.plot(times, values, "b-", linewidth=2, label=f"支架{src} 前溜行程")

                # 标注最大值和最小值
                min_val = min(values)
                max_val = max(values)
                min_idx = values.index(min_val)
                max_idx = values.index(max_val)

                ax.scatter(
                    [times[min_idx]],
                    [min_val],
                    c="red",
                    s=100,
                    marker="v",
                    label=f"最小值: {min_val}",
                    zorder=5,
                )
                ax.scatter(
                    [times[max_idx]],
                    [max_val],
                    c="green",
                    s=100,
                    marker="^",
                    label=f"最大值: {max_val}",
                    zorder=5,
                )

                ax.set_title(
                    f"支架 {src} 前溜行程曲线\n"
                    f"周期: {cycle.start_time.strftime('%H:%M:%S')} -> {cycle.end_time.strftime('%H:%M:%S')}",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("时间", fontsize=11)
                ax.set_ylabel("行程值 (mm)", fontsize=11)
                ax.grid(True, alpha=0.3, linestyle="--")
                ax.legend(loc="best", fontsize=10)

                # 格式化时间轴
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        else:
            ax.text(
                0.5,
                0.5,
                f"支架 {src} 无传感器数据",
                ha="center",
                va="center",
                fontsize=14,
                transform=ax.transAxes,
            )

        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

    def _plot_spatiotemporal_heatmap(self, cycle: CycleData, output_path: str):
        """绘制时空热力图（横坐标：时间，纵坐标：支架号，颜色：行程值）

        Args:
            cycle: 周期数据
            output_path: 输出文件路径
        """
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.cm import get_cmap
        from matplotlib.colors import BoundaryNorm

        # 收集所有传感器数据（不限制sensor_type，使用所有可用数据）
        sensor_data_for_heatmap = {}  # {src: [(timestamp, value), ...]}

        # 调试：检查所有传感器类型
        all_sensor_types = set()
        for src, sensor_list in cycle.sensor_data.items():
            for sensor_data in sensor_list:
                st = sensor_data.get("sensor_type")
                if st is not None:
                    all_sensor_types.add(st)

        if all_sensor_types:
            print(f"         [调试] 周期内的传感器类型: {sorted(all_sensor_types)}")

        # 收集所有传感器数据（不限制类型，使用所有可用数据）
        for src, sensor_list in cycle.sensor_data.items():
            for sensor_data in sensor_list:
                value = sensor_data.get("value")
                if value is not None:
                    if src not in sensor_data_for_heatmap:
                        sensor_data_for_heatmap[src] = []
                    sensor_data_for_heatmap[src].append(
                        (sensor_data["timestamp"], value)
                    )

        # 统计数据
        total_points = sum(len(data) for data in sensor_data_for_heatmap.values())
        print(
            f"         传感器数据: {total_points} 条, 来自 {len(sensor_data_for_heatmap)} 个支架"
        )

        if total_points == 0:
            print("         [跳过] 没有可用的传感器数据点，跳过热力图生成")
            return

        # 提取所有行程值用于确定颜色范围
        all_values = []
        for data_list in sensor_data_for_heatmap.values():
            all_values.extend([v for _, v in data_list])

        if not all_values:
            print("         [跳过] 没有有效的行程值，跳过热力图生成")
            return

        min_value = min(all_values)
        max_value = max(all_values)

        # 创建5个档位的边界
        levels = 5
        boundaries = np.linspace(min_value, max_value, levels + 1)
        print(f"         行程范围: {min_value:.1f} - {max_value:.1f} mm")
        print(f"         分级边界: {['%.1f' % b for b in boundaries]}")

        # 创建颜色映射
        cmap = get_cmap("YlOrRd")
        norm = BoundaryNorm(boundaries, ncolors=cmap.N, clip=True)

        # 创建图形
        fig, ax = plt.subplots(figsize=(20, 10))

        # 为每个支架绘制数据点
        sorted_srcs = sorted(sensor_data_for_heatmap.keys())
        for src in sorted_srcs:
            data_list = sensor_data_for_heatmap[src]
            if not data_list:
                continue

            timestamps = [t for t, _ in data_list]
            values = [v for _, v in data_list]

            # 为每个数据点创建颜色数组
            colors = [cmap(norm(v)) for v in values]

            # 绘制散点，每个点用对应的行程值颜色
            ax.scatter(
                timestamps,
                [src] * len(timestamps),
                c=colors,
                s=30,
                alpha=0.7,
                edgecolors="none",
                marker="o",
            )

        # 绘制采煤机轨迹（使用右侧的Y轴）
        if cycle.shearer_positions:
            shearer_times = [p["timestamp"] for p in cycle.shearer_positions]
            shearer_positions = [p["position"] for p in cycle.shearer_positions]

            # 创建第二个Y轴用于采煤机位置
            ax2 = ax.twinx()

            # 绘制采煤机轨迹线
            ax2.plot(
                shearer_times,
                shearer_positions,
                "b-",
                linewidth=2.5,
                alpha=0.8,
                label="采煤机轨迹",
                zorder=10,
            )

            # 标记起点和终点
            ax2.scatter(
                [shearer_times[0]],
                [shearer_positions[0]],
                c="lime",
                s=150,
                marker="o",
                edgecolors="black",
                linewidths=2,
                label="起点",
                zorder=11,
            )
            ax2.scatter(
                [shearer_times[-1]],
                [shearer_positions[-1]],
                c="red",
                s=150,
                marker="o",
                edgecolors="black",
                linewidths=2,
                label="终点",
                zorder=11,
            )

            # 设置右侧Y轴
            ax2.set_ylabel("采煤机位置", fontsize=13, fontweight="bold", color="blue")
            ax2.tick_params(axis="y", labelcolor="blue", labelsize=10)
            ax2.spines["right"].set_edgecolor("blue")
            ax2.spines["right"].set_linewidth(1.5)

            # 添加图例（合并左右轴的图例）
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(
                lines1 + lines2,
                labels1 + labels2,
                loc="upper right",
                fontsize=10,
                framealpha=0.9,
            )

        # 添加颜色条
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, pad=0.02)
        cbar.set_label("传感器值", fontsize=12, fontweight="bold")

        # 设置颜色条刻度为档位标签
        tick_positions = (boundaries[:-1] + boundaries[1:]) / 2
        tick_labels = [
            f"{boundaries[i]:.0f}-{boundaries[i + 1]:.0f}" for i in range(levels)
        ]
        cbar.set_ticks(tick_positions)
        cbar.set_ticklabels(tick_labels)

        # 设置坐标轴
        ax.set_xlabel("时间", fontsize=13, fontweight="bold")
        ax.set_ylabel("支架号", fontsize=13, fontweight="bold")

        # 生成标题
        start_str = cycle.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_str = cycle.end_time.strftime("%H:%M:%S")
        ax.set_title(
            f"时空热力图 - 支架传感器数据 ({start_str} -> {end_str})",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )

        # 格式化X轴时间显示
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

        # 添加网格
        ax.grid(True, alpha=0.3, linestyle="--")

        # 添加统计信息
        stats_text = (
            f"数据点: {total_points:,}\n"
            f"支架数: {len(sensor_data_for_heatmap)}\n"
            f"时间范围: {cycle.start_time.strftime('%H:%M:%S')} - {cycle.end_time.strftime('%H:%M:%S')}\n"
            f"数值范围: {min_value:.1f} - {max_value:.1f}\n"
            f"分档数: {levels}"
        )
        ax.text(
            0.02,
            0.98,
            stats_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8),
        )

        # 调整布局
        plt.tight_layout()

        # 保存图片
        import os

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   [OK] 时空热力图已保存: {output_path}")

    def _find_closest_shearer_position(
        self,
        shearer_position_dict: dict,
        target_time: datetime,
        max_time_diff_seconds: float = 5.0,
    ) -> Optional[int]:
        """
        查找指定时间最接近的采煤机位置

        Args:
            shearer_position_dict: 时间到位置的映射字典
            target_time: 目标时间
            max_time_diff_seconds: 最大时间差（秒）

        Returns:
            采煤机位置，如果找不到则返回None
        """
        if not shearer_position_dict:
            return None

        min_diff = float("inf")
        closest_position = None

        for timestamp, position in shearer_position_dict.items():
            time_diff = abs((timestamp - target_time).total_seconds())
            if time_diff < min_diff:
                min_diff = time_diff
                closest_position = position

        # 如果时间差太大，认为没有对应的位置
        if min_diff > max_time_diff_seconds:
            return None

        return closest_position

    def process_and_visualize(self, output_dir: str):
        """处理数据并可视化（流式处理：加载一批处理一批）

        使用StreamingDataLoader统一处理所有数据源类型

        Yields:
            tuple: (cycle_index, cycle, output_file) 每完成一个周期就yield一次
        """
        print("\n" + "=" * 80)
        print("步骤3: 处理和可视化（流式模式）")
        print("=" * 80)

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        # 使用统一的流式处理方法
        yield from self._process_streaming(output_dir)

        print(f"\n[OK] 处理完成！")
        print(f"   输出目录: {output_dir}")
        print(f"   共绘制 {len(self.cycles)} 张轨迹图")

    def _process_streaming(self, output_dir: str):
        """统一的流式处理方法（使用传入的StreamingDataLoader）

        Args:
            output_dir: 输出目录

        Yields:
            tuple: (cycle_index, cycle, output_file) 每完成一个周期就yield一次
        """
        print(f"正在流式加载数据: {self.input_source}")
        print(f"   源类型: {self.source_type}")

        # 初始化处理状态
        batch_count = 0
        total_processed = 0
        cycle_count = 0

        # 初始化范围信息
        self.range_info = None
        self.initial_batch_processed = False

        # 最小煤机位置数量阈值
        self.min_shearer_positions_threshold = 500

        print(f"   批次大小: {self.batch_size}")
        print(f"   最小煤机位置数量阈值: {self.min_shearer_positions_threshold}")

        # 流式加载数据并处理（使用传入的StreamingDataLoader）
        for timestamp, src, parsed_data in self.data_loader.load_data():
            batch_count += 1 if batch_count == 0 else batch_count
            self.all_data.append((timestamp, src, parsed_data))

            # 提取煤机位置数据
            if parsed_data.get("frame_type") == "煤机位置":
                data = parsed_data.get("data", {})
                position = data.get("position")
                direction = data.get("dir")
                if position is not None:
                    # 过滤重复位置值
                    if (
                        self.last_shearer_position is not None
                        and position == self.last_shearer_position
                    ):
                        self.filtered_duplicate_count += 1
                    else:
                        self.shearer_positions.append(
                            (timestamp, src, position, str(direction))
                        )
                        self.last_shearer_position = position

            total_processed = len(self.all_data)

            # 每10000条打印一次进度
            if total_processed % 10000 == 0:
                print(f"\n批次处理进度:")
                print(f"   累计数据: {total_processed:,} 条")
                print(f"   煤机位置: {len(self.shearer_positions):,} 个")
                if self.filtered_duplicate_count > 0:
                    print(f"   过滤重复: {self.filtered_duplicate_count:,} 条")

            # 初始化范围（当煤机位置数量达到阈值）
            if not self.initial_batch_processed:
                if len(self.shearer_positions) >= self.min_shearer_positions_threshold:
                    print(
                        f"\n   → 煤机位置数据达到 {self.min_shearer_positions_threshold} 个，开始初始化位置范围..."
                    )
                    self._initialize_range()
                    self.initial_batch_processed = True

            # 如果已初始化范围，尝试检测周期
            if self.initial_batch_processed:
                # 检测是否有新的周期完成
                new_cycles = self._detect_new_cycles()
                for cycle in new_cycles:
                    cycle_count += 1
                    output_file = os.path.join(
                        output_dir,
                        f"cycle_{cycle_count:03d}_{cycle.start_time.strftime('%Y%m%d_%H%M%S')}.png",
                    )
                    self._plot_single_cycle(cycle, output_file)
                    yield (cycle_count, cycle, output_file)

                    # 测试模式：只绘制两张图（两个循环）
                    if self.test_mode and cycle_count >= 2:
                        print("\n测试模式：已绘制两张图后结束")
                        return

        print(f"\n   [OK] 所有数据处理完成！共 {total_processed:,} 条数据")
        print(f"   煤机位置: {len(self.shearer_positions):,} 个")
        if self.filtered_duplicate_count > 0:
            print(f"   过滤重复: {self.filtered_duplicate_count:,} 条")

        # 如果所有数据处理完但还没有初始化范围，则尝试初始化
        if not self.initial_batch_processed and len(self.shearer_positions) > 0:
            print(f"\n   → 数据加载完成，煤机位置数量: {len(self.shearer_positions)}")
            if len(self.shearer_positions) >= 10:  # 至少需要10个位置点
                print(f"   → 开始初始化位置范围...")
                self._initialize_range()
                self.initial_batch_processed = True

                # 尝试检测周期
                new_cycles = self._detect_new_cycles()
                for cycle in new_cycles:
                    cycle_count += 1
                    output_file = os.path.join(
                        output_dir,
                        f"cycle_{cycle_count:03d}_{cycle.start_time.strftime('%Y%m%d_%H%M%S')}.png",
                    )
                    self._plot_single_cycle(cycle, output_file)
                    yield (cycle_count, cycle, output_file)

                    # 测试模式：只绘制两张图（两个循环）
                    if self.test_mode and cycle_count >= 2:
                        print("\n测试模式：已绘制两张图后结束")
                        return
            else:
                print(
                    f"   ⚠️ 煤机位置数据太少（{len(self.shearer_positions)} 个），无法分割周期"
                )

    def _detect_new_cycles(self) -> List[CycleData]:
        """检测新周期（增量检测）

        周期定义：最小值点 -> 最大值点 -> 最小值点

        逻辑：
        1. 找到第一个真正的最小值点作为周期起点
        2. 离开最小值区域，上升到最大值
        3. 从最大值下降，回到最小值区域
        4. 在最小值区域中找到真正的最小值点作为周期终点
        5. 该终点同时也是下一个周期的起点

        注意：初始化后不再更新最小值、最大值和阈值

        Returns:
            新检测到的周期列表
        """
        if not self.range_info:
            return []

        positions_only = [pos for _, _, pos, _ in self.shearer_positions]
        all_timestamps = [ts for ts, _, _, _ in self.shearer_positions]

        new_cycles = []

        # 获取上一个周期的结束索引
        last_processed_index = self.cycles[-1].end_index if self.cycles else -1

        # 状态变量（需要持久化）
        if not hasattr(self, "_cycle_state"):
            self._cycle_state = {
                "phase": "seeking_first_min",  # seeking_first_min, seeking_max, seeking_second_min
                "was_in_min_zone": False,  # 是否曾经进入过最小值区域
                "left_min_zone": False,  # 是否已经离开最小值区域
                "first_min_index": None,  # 第一个最小值点的索引
                "max_index": None,  # 最大值点的索引
                "first_min_search_start": None,  # 第一次进入最小值区域的起始位置
            }

        state = self._cycle_state

        # 从上次处理的位置继续扫描
        start_idx = max(0, last_processed_index + 1)

        for i in range(start_idx, len(positions_only)):
            position = positions_only[i]

            # 判断当前位置所在区域
            in_min_zone_now = position <= self.range_info.min_threshold
            in_max_zone_now = position >= self.range_info.max_threshold

            # 状态机：寻找完整的周期（最小值 -> 最大值 -> 最小值）
            if state["phase"] == "seeking_first_min":
                # 阶段1：寻找第一个真正的最小值点

                # 进入最小值区域
                if in_min_zone_now:
                    if not state["was_in_min_zone"]:
                        # 第一次进入最小值区域，记录起始位置
                        if state["first_min_search_start"] is None:
                            state["first_min_search_start"] = i
                    state["was_in_min_zone"] = True

                # 如果曾经进入过最小值区域，现在离开了，说明找到了谷底
                if state["was_in_min_zone"] and not in_min_zone_now:
                    # 确保已经离开最小值区域足够远（至少30个点）
                    if (
                        i
                        > (
                            state["first_min_search_start"]
                            if state["first_min_search_start"]
                            else 0
                        )
                        + 30
                    ):
                        # 在整个最小值区域内找最小值点
                        search_start = (
                            state["first_min_search_start"]
                            if state["first_min_search_start"]
                            else max(0, i - 50)
                        )
                        search_end = i
                        window_positions = positions_only[search_start:search_end]

                        if window_positions:
                            local_min_index = search_start + window_positions.index(
                                min(window_positions)
                            )

                            # 验证找到的最小值确实在最小值区域
                            min_position = positions_only[local_min_index]
                            if min_position <= self.range_info.min_threshold:
                                state["first_min_index"] = local_min_index
                                state["left_min_zone"] = True
                                state["phase"] = "seeking_max"
                                print(
                                    f"   → 检测到周期起点(最小值): 索引 {local_min_index}, 位置 {positions_only[local_min_index]}"
                                )

            elif state["phase"] == "seeking_max":
                # 阶段2：寻找最大值点
                if in_max_zone_now:
                    # 检测趋势，确保是上升趋势
                    trend = self._detect_trend(positions_only[max(0, i - 10) : i + 1])
                    if trend == "up" or trend == "stable":
                        # 在当前窗口找最大值
                        search_start = max(state["first_min_index"] + 5, i - 15)
                        search_end = min(len(positions_only), i + 5)
                        window_positions = positions_only[search_start:search_end]

                        if window_positions:
                            # 去除离群点后查找最大值
                            cleaned_max_index = self._find_max_after_outlier_removal(
                                window_positions, search_start
                            )

                            if cleaned_max_index is not None:
                                local_max_index = cleaned_max_index
                                state["max_index"] = local_max_index
                                state["phase"] = "seeking_second_min"
                                print(
                                    f"   → 检测到周期最大值(去除离群点后): 索引 {local_max_index}, 位置 {positions_only[local_max_index]}"
                                )

            elif state["phase"] == "seeking_second_min":
                # 阶段3：寻找第二个最小值点（周期终点）
                # 先添加状态变量到 _cycle_state
                if "second_was_in_min_zone" not in state:
                    state["second_was_in_min_zone"] = False
                if "second_min_search_start" not in state:
                    state["second_min_search_start"] = None

                # 进入最小值区域
                if in_min_zone_now and i > state["max_index"] + 20:
                    state["second_was_in_min_zone"] = True
                    # 记录开始进入最小值区域的位置
                    if state["second_min_search_start"] is None:
                        state["second_min_search_start"] = i

                # 如果曾经进入过最小值区域，现在离开了，说明找到了第二个谷底
                # 并且确保已经离开最小值区域足够远（至少50个点）
                if state["second_was_in_min_zone"] and not in_min_zone_now:
                    # 确保已经离开最小值区域一段时间
                    if i > state["max_index"] + 50:
                        # 在最小值区域范围内找最小值点
                        search_start = max(
                            state["max_index"] + 10,
                            state["second_min_search_start"]
                            if state["second_min_search_start"]
                            else i - 30,
                        )
                        search_end = i
                        window_positions = positions_only[search_start:search_end]

                        if window_positions:
                            local_min_index = search_start + window_positions.index(
                                min(window_positions)
                            )

                            # 验证找到的最小值确实在最小值区域
                            min_position = positions_only[local_min_index]
                            if min_position <= self.range_info.min_threshold:
                                # 创建周期：最小值 -> 最大值 -> 最小值
                                self._create_cycle(
                                    state["first_min_index"],
                                    local_min_index,
                                    all_timestamps,
                                    positions_only,
                                )
                                new_cycles.append(self.cycles[-1])

                                cycle_length = (
                                    local_min_index - state["first_min_index"]
                                )
                                print(
                                    f"   [*] 检测到新周期 #{len(self.cycles)}: "
                                    f"索引 {state['first_min_index']} -> {state['max_index']} -> {local_min_index} "
                                    f"(长度: {cycle_length} 个位置点)"
                                )

                                # 重置状态，开始寻找下一个周期
                                # 当前周期的终点就是下一个周期的起点
                                state["first_min_index"] = local_min_index
                                state["max_index"] = None
                                state["left_min_zone"] = False
                                state["was_in_min_zone"] = False
                                state["second_was_in_min_zone"] = False
                                state["second_min_search_start"] = None
                                state["phase"] = "seeking_max"

                                # 测试模式：只检测一个周期
                                if self.test_mode:
                                    break

        return new_cycles


def main():
    """主函数"""
    # 配置参数
    # input_source = "Datas/潞宁矿数据"
    out_dir_name = "jiayang_31232"
    source_type = "txt"  # "txt", "csv", "db", "clickhouse"
    batch_size = 2000  # 批处理大小
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output",
        "trace_cycles",
        out_dir_name,
    )
    test_mode = False  # 测试模式，只绘制一张图

    print("=" * 80)
    print("跟机轨迹分割程序")
    print("=" * 80)
    print(f"源类型: {source_type}")
    print(f"输出目录: {output_dir}")
    print(f"测试模式: {'是' if test_mode else '否'}")
    print("=" * 80)

    # 先创建StreamingDataLoader
    print("步骤0: 初始化StreamingDataLoader")
    # TXT文件示例:
    # data_loader = StreamingDataLoader(
    #     source="Datas/3217工作面/电液控.txt",
    #     source_type="txt",
    #     batch_size=batch_size,
    # )
    # ClickHouse示例（使用默认连接配置）:
    data_loader = StreamingDataLoader(
        source="clickhouse",  # 连接标识（任意字符串）
        source_type="clickhouse",
        batch_size=batch_size,
        db_name="jiayang",
        table_name="_31232",
        send_receive_timeout=600,
    )
    print("1、数据加载完成")
    # 再创建分割器（传入已初始化的StreamingDataLoader）
    splitter = TraceSplitter(
        data_loader=data_loader,
        test_mode=test_mode,
    )
    # 处理和可视化（流式处理）
    try:
        for cycle_idx, cycle, output_file in splitter.process_and_visualize(output_dir):
            print(f"Completed cycle {cycle_idx}: {output_file}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
