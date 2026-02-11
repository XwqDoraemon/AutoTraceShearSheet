#!/usr/bin/env python3
"""
时空热力图可视化模块

功能：
1. 将采煤机轨迹和支架行程热力图绘制在一个图上
2. 颜色深浅表示行程值的大小
3. 按比例划分为5个档位
"""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import BoundaryNorm, ListedColormap
from matplotlib.patches import Rectangle
from scipy.interpolate import griddata


@dataclass
class HeatmapDataPoint:
    """热力图数据点"""

    timestamp: datetime
    position: int  # 煤机位置
    src: int  # 支架号
    stroke_value: float  # 行程值


class SpatiotemporalHeatmap:
    """时空热力图类"""

    def __init__(self, figure_size: Tuple[int, int] = (20, 10)):
        """
        初始化时空热力图

        Args:
            figure_size: 图形大小 (width, height)
        """
        self.figure_size = figure_size
        self.data_points: List[HeatmapDataPoint] = []
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

    def add_data_point(
        self, timestamp: datetime, position: int, src: int, stroke_value: float
    ):
        """
        添加数据点

        Args:
            timestamp: 时间戳
            position: 煤机位置
            src: 支架号
            stroke_value: 行程值（单位：mm）
        """
        self.data_points.append(
            HeatmapDataPoint(timestamp, position, src, stroke_value)
        )

    def add_shearer_position(self, timestamp: datetime, position: int, direction: str):
        """
        添加采煤机位置数据（用于绘制轨迹线）

        Args:
            timestamp: 时间戳
            position: 煤机位置
            direction: 方向 ("Up", "Down", "Stop")
        """
        if not hasattr(self, "shearer_positions"):
            self.shearer_positions = []
        self.shearer_positions.append(
            {"timestamp": timestamp, "position": position, "direction": direction}
        )

    def create_heatmap(
        self,
        output_path: str,
        title: str = "时空热力图 - 采煤机轨迹与支架行程",
        dpi: int = 300,
        levels: int = 5,
        colormap: str = "YlOrRd",
        show_colorbar: bool = True,
        grid_resolution: int = 100,
    ):
        """
        创建时空热力图

        Args:
            output_path: 输出文件路径
            title: 图表标题
            dpi: 输出分辨率
            levels: 颜色分级数量（默认5档）
            colormap: 颜色映射名称
            show_colorbar: 是否显示颜色条
            grid_resolution: 网格分辨率
        """
        if not self.data_points:
            # 没有数据点，静默返回
            return

        print(f"正在生成时空热力图...")
        print(f"  数据点数量: {len(self.data_points):,}")

        # 提取数据
        timestamps = [dp.timestamp for dp in self.data_points]
        positions = [dp.position for dp in self.data_points]
        srcs = [dp.src for dp in self.data_points]
        stroke_values = [dp.stroke_value for dp in self.data_points]

        # 计算行程值的分级边界
        min_stroke = min(stroke_values)
        max_stroke = max(stroke_values)

        # 创建5个档位的边界
        boundaries = np.linspace(min_stroke, max_stroke, levels + 1)
        print(f"  行程范围: {min_stroke:.1f} - {max_stroke:.1f} mm")
        print(f"  分级边界: {['%.1f' % b for b in boundaries]}")

        # 创建图形和主轴
        fig, ax = plt.subplots(figsize=self.figure_size)

        # 创建网格进行插值
        # X轴：时间，Y轴：位置
        time_num = mdates.date2num(timestamps)

        # 创建规则网格
        xi = np.linspace(min(time_num), max(time_num), grid_resolution)
        yi = np.linspace(min(positions), max(positions), grid_resolution)
        xi_grid, yi_grid = np.meshgrid(xi, yi)

        # 使用网格数据插值生成热力图
        # 将行程值映射到网格上
        zi = griddata(
            (time_num, positions),
            stroke_values,
            (xi_grid, yi_grid),
            method="cubic",
            fill_value=np.nan,
        )

        # 创建颜色映射和归一化
        cmap = plt.get_cmap(colormap)
        norm = BoundaryNorm(boundaries, ncolors=cmap.N, clip=True)

        # 绘制热力图
        im = ax.pcolormesh(
            xi_grid, yi_grid, zi, shading="gouraud", cmap=cmap, norm=norm, alpha=0.8
        )

        # 绘制采煤机轨迹线（如果有的话）
        if hasattr(self, "shearer_positions") and self.shearer_positions:
            shearer_times = [sp["timestamp"] for sp in self.shearer_positions]
            shearer_positions = [sp["position"] for sp in self.shearer_positions]

            # 绘制轨迹线
            ax.plot(
                shearer_times,
                shearer_positions,
                "b-",
                linewidth=3,
                alpha=0.7,
                label="采煤机轨迹",
            )

            # 标记起点和终点
            ax.scatter(
                [shearer_times[0]],
                [shearer_positions[0]],
                c="green",
                s=200,
                marker="o",
                edgecolors="black",
                linewidths=2,
                label="起点",
                zorder=5,
            )
            ax.scatter(
                [shearer_times[-1]],
                [shearer_positions[-1]],
                c="red",
                s=200,
                marker="o",
                edgecolors="black",
                linewidths=2,
                label="终点",
                zorder=5,
            )

        # 添加颜色条
        if show_colorbar:
            cbar = plt.colorbar(im, ax=ax, pad=0.02)
            cbar.set_label("支架行程 (mm)", fontsize=12, fontweight="bold")

            # 设置颜色条刻度为档位标签
            tick_positions = (boundaries[:-1] + boundaries[1:]) / 2
            tick_labels = [
                f"{boundaries[i]:.0f}-{boundaries[i + 1]:.0f}" for i in range(levels)
            ]
            cbar.set_ticks(tick_positions)
            cbar.set_ticklabels(tick_labels)

        # 设置坐标轴
        ax.set_xlabel("时间", fontsize=13, fontweight="bold")
        ax.set_ylabel("采煤机位置 (支架号)", fontsize=13, fontweight="bold")
        ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # 格式化X轴时间显示
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

        # 添加网格
        ax.grid(True, alpha=0.3, linestyle="--")

        # 添加图例
        ax.legend(loc="upper right", fontsize=11)

        # 添加统计信息
        stats_text = (
            f"数据点: {len(self.data_points):,}\n"
            f"时间范围: {timestamps[0].strftime('%H:%M:%S')} - {timestamps[-1].strftime('%H:%M:%S')}\n"
            f"位置范围: {min(positions)} - {max(positions)}\n"
            f"行程范围: {min_stroke:.1f} - {max_stroke:.1f} mm"
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
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path, dpi=dpi, bbox_inches="tight")
        plt.close()

        print(f"✓ 热力图已保存: {output_path}")

    def create_dual_axis_heatmap(
        self,
        output_path: str,
        title: str = "时空热力图 - 采煤机轨迹与支架行程",
        dpi: int = 300,
        levels: int = 5,
        colormap: str = "YlOrRd",
    ):
        """
        创建双轴时空热力图（X轴：时间，Y轴：位置，颜色：行程值）

        这是一个简化版本，直接绘制散点图，不进行插值

        Args:
            output_path: 输出文件路径
            title: 图表标题
            dpi: 输出分辨率
            levels: 颜色分级数量
            colormap: 颜色映射名称
        """
        if not self.data_points:
            # 没有数据点，静默返回
            return

        print(f"正在生成双轴时空热力图...")
        print(f"  数据点数量: {len(self.data_points):,}")

        # 提取数据
        timestamps = [dp.timestamp for dp in self.data_points]
        positions = [dp.position for dp in self.data_points]
        stroke_values = [dp.stroke_value for dp in self.data_points]

        # 计算行程值的分级边界
        min_stroke = min(stroke_values)
        max_stroke = max(stroke_values)

        # 创建5个档位的边界
        boundaries = np.linspace(min_stroke, max_stroke, levels + 1)

        # 创建颜色映射和归一化
        cmap = plt.get_cmap(colormap)
        norm = BoundaryNorm(boundaries, ncolors=cmap.N, clip=True)

        # 创建图形
        fig, ax = plt.subplots(figsize=self.figure_size)

        # 将每个数据点映射到对应的颜色
        colors = [cmap(norm(value)) for value in stroke_values]

        # 绘制散点图
        scatter = ax.scatter(
            timestamps,
            positions,
            c=stroke_values,
            cmap=cmap,
            norm=norm,
            s=20,
            alpha=0.6,
            edgecolors="none",
        )

        # 绘制采煤机轨迹线
        if hasattr(self, "shearer_positions") and self.shearer_positions:
            shearer_times = [sp["timestamp"] for sp in self.shearer_positions]
            shearer_positions_vals = [sp["position"] for sp in self.shearer_positions]

            # 绘制轨迹线
            ax.plot(
                shearer_times,
                shearer_positions_vals,
                "b-",
                linewidth=3,
                alpha=0.8,
                label="采煤机轨迹",
                zorder=5,
            )

            # 标记起点和终点
            ax.scatter(
                [shearer_times[0]],
                [shearer_positions_vals[0]],
                c="green",
                s=200,
                marker="o",
                edgecolors="black",
                linewidths=2,
                label="起点",
                zorder=6,
            )
            ax.scatter(
                [shearer_times[-1]],
                [shearer_positions_vals[-1]],
                c="red",
                s=200,
                marker="o",
                edgecolors="black",
                linewidths=2,
                label="终点",
                zorder=6,
            )

        # 添加颜色条
        cbar = plt.colorbar(scatter, ax=ax, pad=0.02)
        cbar.set_label("支架行程 (mm)", fontsize=12, fontweight="bold")

        # 设置颜色条刻度为档位标签
        tick_positions = (boundaries[:-1] + boundaries[1:]) / 2
        tick_labels = [
            f"档位{i + 1}\n({boundaries[i]:.0f}-{boundaries[i + 1]:.0f})"
            for i in range(levels)
        ]
        cbar.set_ticks(tick_positions)
        cbar.set_ticklabels(tick_labels)

        # 设置坐标轴
        ax.set_xlabel("时间", fontsize=13, fontweight="bold")
        ax.set_ylabel("采煤机位置 (支架号)", fontsize=13, fontweight="bold")
        ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # 格式化X轴时间显示
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

        # 添加网格
        ax.grid(True, alpha=0.3, linestyle="--")

        # 添加图例
        ax.legend(loc="upper right", fontsize=11)

        # 添加统计信息
        stats_text = (
            f"数据点: {len(self.data_points):,}\n"
            f"时间范围: {timestamps[0].strftime('%H:%M:%S')} - {timestamps[-1].strftime('%H:%M:%S')}\n"
            f"位置范围: {min(positions)} - {max(positions)}\n"
            f"行程范围: {min_stroke:.1f} - {max_stroke:.1f} mm\n"
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
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path, dpi=dpi, bbox_inches="tight")
        plt.close()

        print(f"✓ 双轴热力图已保存: {output_path}")

    @staticmethod
    def create_from_cycle_data(
        cycle_data: "CycleData",
        output_path: str,
        title: Optional[str] = None,
        use_interpolation: bool = False,
        levels: int = 5,
        colormap: str = "YlOrRd",
    ):
        """
        从周期数据创建时空热力图

        Args:
            cycle_data: 周期数据对象
            output_path: 输出文件路径
            title: 图表标题（可选）
            use_interpolation: 是否使用插值生成平滑热力图
            levels: 颜色分级数量
            colormap: 颜色映射名称
        """
        heatmap = SpatiotemporalHeatmap()

        # 添加采煤机位置数据
        if hasattr(cycle_data, "shearer_positions") and cycle_data.shearer_positions:
            for pos_data in cycle_data.shearer_positions:
                heatmap.add_shearer_position(
                    pos_data["timestamp"],
                    pos_data["position"],
                    pos_data.get("direction", "Stop"),
                )

        # 添加传感器数据（行程值）
        if hasattr(cycle_data, "sensor_data"):
            for src, sensor_list in cycle_data.sensor_data.items():
                for sensor_data in sensor_list:
                    # 只添加前溜行程数据
                    if sensor_data.get("sensor_type") == 2:  # SensorTypeID.前溜行程
                        # 需要找到对应时间的采煤机位置
                        shearer_pos = (
                            SpatiotemporalHeatmap._find_shearer_position_at_time(
                                cycle_data.shearer_positions, sensor_data["timestamp"]
                            )
                        )
                        if shearer_pos is not None:
                            heatmap.add_data_point(
                                sensor_data["timestamp"],
                                shearer_pos,
                                src,
                                sensor_data["value"],
                            )

        # 生成标题
        if title is None:
            start_str = cycle_data.start_time.strftime("%Y-%m-%d %H:%M:%S")
            end_str = cycle_data.end_time.strftime("%H:%M:%S")
            title = f"时空热力图 - 周期 ({start_str} -> {end_str})"

        # 创建热力图
        if use_interpolation:
            heatmap.create_heatmap(
                output_path, title=title, levels=levels, colormap=colormap
            )
        else:
            heatmap.create_dual_axis_heatmap(
                output_path, title=title, levels=levels, colormap=colormap
            )

    @staticmethod
    def _find_shearer_position_at_time(
        shearer_positions: List[dict],
        target_time: datetime,
        max_time_diff_seconds: float = 5.0,
    ) -> Optional[int]:
        """
        查找指定时间最接近的采煤机位置

        Args:
            shearer_positions: 采煤机位置列表
            target_time: 目标时间
            max_time_diff_seconds: 最大时间差（秒）

        Returns:
            采煤机位置，如果找不到则返回None
        """
        if not shearer_positions:
            return None

        # 找到时间最接近的位置
        min_diff = float("inf")
        closest_position = None

        for pos_data in shearer_positions:
            time_diff = abs((pos_data["timestamp"] - target_time).total_seconds())
            if time_diff < min_diff:
                min_diff = time_diff
                closest_position = pos_data["position"]

        # 如果时间差太大，认为没有对应的位置
        if min_diff > max_time_diff_seconds:
            return None

        return closest_position

    def clear(self):
        """清空数据"""
        self.data_points = []
        if hasattr(self, "shearer_positions"):
            self.shearer_positions = []
