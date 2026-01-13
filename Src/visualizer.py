#!/usr/bin/env python3
"""
数据可视化模块
负责电液控数据的可视化展示
"""

from datetime import datetime
from typing import List, Tuple

# 必须在导入pyplot之前设置后端
import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class DataVisualizer:
    """数据可视化器"""

    def __init__(self):
        """初始化可视化器"""
        self._setup_matplotlib()

    def _setup_matplotlib(self):
        """设置matplotlib中文支持"""
        try:
            # 尝试不同的中文字体
            chinese_fonts = [
                "SimHei",  # 黑体
                "Microsoft YaHei",  # 微软雅黑
                "SimSun",  # 宋体
                "KaiTi",  # 楷体
                "FangSong",  # 仿宋
                "Arial Unicode MS",  # Arial Unicode MS
            ]

            # 获取系统可用字体
            available_fonts = set(f.name for f in fm.fontManager.ttflist)

            # 找到第一个可用的中文字体
            selected_font = None
            for font in chinese_fonts:
                if font in available_fonts:
                    selected_font = font
                    break

            if selected_font:
                plt.rcParams["font.sans-serif"] = [selected_font]
                plt.rcParams["font.family"] = "sans-serif"
                print(f"✓ 使用中文字体: {selected_font}")
            else:
                print("⚠ 警告: 未找到中文字体，使用默认字体")
                plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]

            # 设置其他matplotlib参数
            plt.rcParams["axes.unicode_minus"] = False  # 正常显示负号
            plt.rcParams["figure.dpi"] = 100
            plt.rcParams["savefig.dpi"] = 300

        except Exception as e:
            print(f"⚠ 字体设置失败: {e}")
            plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
            plt.rcParams["axes.unicode_minus"] = False

    def create_visualization(
        self,
        data: List[Tuple[datetime, int]],
        output_file: str = "human_operation_visualization.png",
    ):
        """
        创建数据可视化

        Args:
            data: 处理后的数据 [(时间, b_Src), ...]
            output_file: 输出文件名
        """
        if not data:
            print("❌ 没有符合条件的数据，无法创建可视化图表")
            return

        print(f"📈 正在创建可视化图表...")

        # 转换为DataFrame便于处理
        df = pd.DataFrame(data, columns=["时间", "b_Src"])

        # 获取所有唯一的b_Src值并排序
        unique_src = sorted(df["b_Src"].unique())
        print(f"📍 发现 {len(unique_src)} 个不同的源地址")

        # 创建图表
        fig, ax = plt.subplots(figsize=(16, 10))

        # 为每个b_Src创建散点图
        if len(unique_src) <= 20:
            # 源地址较少时，使用不同颜色
            colors = plt.cm.tab20(np.linspace(0, 1, len(unique_src)))
            for i, src in enumerate(unique_src):
                src_data = df[df["b_Src"] == src]
                ax.scatter(
                    src_data["b_Src"],
                    src_data["时间"],
                    c=[colors[i]],
                    label=f"源地址 {src}",
                    alpha=0.7,
                    s=30,
                )
        else:
            # 源地址较多时，使用单一颜色
            ax.scatter(df["b_Src"], df["时间"], c="blue", alpha=0.6, s=20)

        # 设置图表属性
        ax.set_xlabel("源地址 (b_Src)", fontsize=14, fontweight="bold")
        ax.set_ylabel("时间", fontsize=14, fontweight="bold")
        ax.set_title(
            "电液控人为操作数据可视化\n(条件: b_pri=3, b_cmd=4)",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )

        # 设置时间轴格式
        ax.yaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.yaxis.set_major_locator(mdates.MinuteLocator(interval=5))

        # 设置x轴
        if len(unique_src) > 30:
            # 如果源地址太多，只显示部分标签
            step = max(1, len(unique_src) // 20)
            ax.set_xticks(unique_src[::step])
        else:
            ax.set_xticks(unique_src)

        # 美化图表
        ax.grid(True, alpha=0.3, linestyle="--")
        ax.set_facecolor("#f8f9fa")

        # 设置图例
        if len(unique_src) <= 15:
            ax.legend(
                bbox_to_anchor=(1.05, 1),
                loc="upper left",
                fontsize=10,
                frameon=True,
                fancybox=True,
                shadow=True,
            )

        # 调整布局
        plt.tight_layout()

        # 保存图表
        plt.savefig(
            output_file,
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )
        print(f"💾 可视化图表已保存为: {output_file}")

        # 显示统计信息
        self._print_statistics(df)

    def _print_statistics(self, df: pd.DataFrame):
        """
        打印统计信息

        Args:
            df: 数据DataFrame
        """
        print("\n" + "=" * 50)
        print("📊 数据统计信息")
        print("=" * 50)
        print(f"📈 总记录数: {len(df):,}")
        print(f"⏰ 时间范围: {df['时间'].min()} 到 {df['时间'].max()}")
        print(f"🎯 源地址范围: {df['b_Src'].min()} 到 {df['b_Src'].max()}")
        print(f"🔢 不同源地址数量: {df['b_Src'].nunique()}")

        # 按源地址统计
        src_counts = df["b_Src"].value_counts().sort_index()
        print(f"\n📋 各源地址操作次数 (前10个):")
        for src, count in src_counts.head(10).items():
            print(f"   源地址 {src:3d}: {count:4d} 次")
        if len(src_counts) > 10:
            print(f"   ... 还有 {len(src_counts) - 10} 个源地址")

        # 时间分布统计
        df["小时"] = df["时间"].dt.hour
        hour_counts = df["小时"].value_counts().sort_index()
        print(f"\n⏱️  按小时分布 (前5个):")
        for hour, count in hour_counts.head(5).items():
            print(f"   {hour:2d}时: {count:4d} 次")
