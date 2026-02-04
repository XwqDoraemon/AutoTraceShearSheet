#!/usr/bin/env python3
"""
数据可视化模块
负责电液控数据的可视化展示
"""

from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# 必须在导入pyplot之前设置后端
import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


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
        data: List[Tuple[datetime, int, dict]],
        output_file: str = "human_operation_visualization.png",
    ):
        """
        创建数据可视化
        - 横坐标：时间
        - 纵坐标：
          - 支架动作：源地址
          - 煤机位置：位置值
        - 两种类型使用不同样式

        Args:
            data: 处理后的数据 [(时间, src_no, 解析结果), ...]
            output_file: 输出文件名
        """
        if not data:
            print("❌ 没有符合条件的数据，无法创建可视化图表")
            return

        print(f"📈 正在创建可视化图表...")

        # 分离两种类型的数据
        action_data = []  # 支架动作数据
        position_data = []  # 煤机位置数据

        for dt, src_no, parsed_result in data:
            frame_type = parsed_result.get("frame_type")
            result_data = parsed_result.get("data", {})

            if frame_type == "支架动作":
                action_data.append(
                    {
                        "时间": dt,
                        "源地址": src_no,
                        "frame_type": "支架动作",
                        "data": result_data,
                    }
                )
            elif frame_type == "煤机位置":
                position_data.append(
                    {
                        "时间": dt,
                        "源地址": src_no,
                        "frame_type": "煤机位置",
                        "位置值": result_data.get("position", 0),
                        "方向": result_data.get("dir"),
                        "data": result_data,
                    }
                )

        print(f"📍 支架动作记录: {len(action_data)} 条")
        print(f"📍 煤机位置记录: {len(position_data)} 条")

        # 创建图表 - 使用子图分别显示两种类型
        if action_data and position_data:
            # 两种数据都存在，创建双子图
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12), sharex=True)
            self._plot_action_data(ax1, action_data)
            self._plot_position_data(ax2, position_data)
            fig.suptitle(
                "电液控数据可视化\n支架动作 & 煤机位置",
                fontsize=16,
                fontweight="bold",
            )
        elif action_data:
            # 只有支架动作
            fig, ax1 = plt.subplots(figsize=(18, 10))
            self._plot_action_data(ax1, action_data)
            ax1.set_title(
                "电液控数据可视化 - 支架动作",
                fontsize=16,
                fontweight="bold",
                pad=20,
            )
        elif position_data:
            # 只有煤机位置
            fig, ax2 = plt.subplots(figsize=(18, 10))
            self._plot_position_data(ax2, position_data)
            ax2.set_title(
                "电液控数据可视化 - 煤机位置",
                fontsize=16,
                fontweight="bold",
                pad=20,
            )
        else:
            print("❌ 没有有效的数据类型")
            return

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
        plt.close()
        print(f"💾 可视化图表已保存为: {output_file}")

        # 显示统计信息
        self._print_detailed_statistics(action_data, position_data)

    def _plot_action_data(self, ax, action_data: List[dict]):
        """
        绘制支架动作数据

        Args:
            ax: matplotlib 轴对象
            action_data: 支架动作数据列表
        """
        if not action_data:
            return

        # 转换为 DataFrame
        df = pd.DataFrame(action_data)

        # 获取唯一源地址
        unique_src = sorted(df["源地址"].unique())

        # 绘制散点图 - 横轴时间，纵轴源地址
        if len(unique_src) <= 20:
            # 源地址较少时，使用不同颜色
            colors = plt.cm.tab20(np.linspace(0, 1, len(unique_src)))
            for i, src in enumerate(unique_src):
                src_data = df[df["源地址"] == src]
                ax.scatter(
                    src_data["时间"],
                    src_data["源地址"],
                    c=[colors[i]],
                    label=f"源地址 {src}",
                    alpha=0.7,
                    s=50,
                    marker="o",  # 圆形标记
                    edgecolors="darkblue",
                    linewidths=0.5,
                )
        else:
            # 源地址较多时，使用单一颜色
            ax.scatter(
                df["时间"],
                df["源地址"],
                c="steelblue",
                alpha=0.6,
                s=40,
                marker="o",
                edgecolors="navy",
                linewidths=0.5,
                label="支架动作",
            )

        # 设置轴标签
        ax.set_ylabel("源地址", fontsize=12, fontweight="bold")
        ax.set_title("支架动作数据", fontsize=14, fontweight="bold")

        # 设置Y轴
        # 设置Y轴刻度更密集，缩小每格间距
        ax.yaxis.set_major_locator(
            plt.MaxNLocator(nbins="auto", integer=True, steps=[1, 2, 5, 10])
        )
        ax.set_yticks(unique_src)
        if len(unique_src) > 30:
            # 源地址太多时，只显示部分标签
            step = max(1, len(unique_src) // 20)
            ax.set_yticks(unique_src[::step])

        # 美化
        ax.grid(True, alpha=0.3, linestyle="--")
        ax.set_facecolor("#f0f8ff")  # 淡蓝色背景

        # 设置图例
        if len(unique_src) <= 15:
            ax.legend(
                bbox_to_anchor=(1.05, 1),
                loc="upper left",
                fontsize=9,
                frameon=True,
            )

        # 设置时间轴格式
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    def _plot_position_data(self, ax, position_data: List[dict]):
        """
        绘制煤机位置数据

        Args:
            ax: matplotlib 轴对象
            position_data: 煤机位置数据列表
        """
        if not position_data:
            return

        # 转换为 DataFrame
        df = pd.DataFrame(position_data)

        # 按方向分组绘制
        directions = df["方向"].unique()

        # 颜色映射
        direction_colors = {
            "ShearerDir.Up": "green",  # 上行 - 绿色
            "ShearerDir.Down": "red",  # 下行 - 红色
            "ShearerDir.Stop": "gray",  # 停止 - 灰色
        }

        direction_markers = {
            "ShearerDir.Up": "^",  # 上行 - 三角形向上
            "ShearerDir.Down": "v",  # 下行 - 三角形向下
            "ShearerDir.Stop": "s",  # 停止 - 方形
        }

        direction_labels = {
            "ShearerDir.Up": "上行",
            "ShearerDir.Down": "下行",
            "ShearerDir.Stop": "停止",
        }

        for direction in directions:
            dir_data = df[df["方向"] == direction]
            color = direction_colors.get(str(direction), "blue")
            marker = direction_markers.get(str(direction), "o")
            label = direction_labels.get(str(direction), str(direction))

            ax.scatter(
                dir_data["时间"],
                dir_data["位置值"],
                c=color,
                marker=marker,
                label=label,
                alpha=0.7,
                s=60,
                edgecolors="black",
                linewidths=0.5,
            )

        # 设置轴标签
        ax.set_xlabel("时间", fontsize=12, fontweight="bold")
        ax.set_ylabel("煤机位置值", fontsize=12, fontweight="bold")
        ax.set_title("煤机位置数据", fontsize=14, fontweight="bold")

        # 设置Y轴刻度更密集，缩小每格间距
        ax.yaxis.set_major_locator(
            plt.MaxNLocator(nbins="auto", integer=True, steps=[1, 2, 5, 10])
        )

        # 美化
        ax.grid(True, alpha=0.3, linestyle="--")
        ax.set_facecolor("#fff8dc")  # 淡黄色背景

        # 设置图例
        ax.legend(
            loc="best",
            fontsize=10,
            frameon=True,
            fancybox=True,
            shadow=True,
        )

        # 设置时间轴格式
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    def _print_detailed_statistics(
        self, action_data: List[dict], position_data: List[dict]
    ):
        """
        打印详细的统计信息

        Args:
            action_data: 支架动作数据
            position_data: 煤机位置数据
        """
        print("\n" + "=" * 60)
        print("📊 数据统计信息")
        print("=" * 60)

        # 支架动作统计
        if action_data:
            df_action = pd.DataFrame(action_data)
            print(f"\n📋 支架动作数据:")
            print(f"   总记录数: {len(action_data):,}")
            print(f"   不同源地址: {df_action['源地址'].nunique()}")

            src_counts = df_action["源地址"].value_counts().sort_index()
            print(f"\n   各源地址操作次数 (前10个):")
            for src, count in src_counts.head(10).items():
                print(f"      源地址 {src:3d}: {count:4d} 次")
            if len(src_counts) > 10:
                print(f"      ... 还有 {len(src_counts) - 10} 个源地址")

        # 煤机位置统计
        if position_data:
            df_position = pd.DataFrame(position_data)
            print(f"\n📋 煤机位置数据:")
            print(f"   总记录数: {len(position_data):,}")
            print(
                f"   位置范围: {df_position['位置值'].min()} - {df_position['位置值'].max()}"
            )
            print(f"   不同源地址: {df_position['源地址'].nunique()}")

            dir_counts = df_position["方向"].value_counts()
            print(f"\n   方向分布:")
            for direction, count in dir_counts.items():
                dir_name = {
                    "ShearerDir.Up": "上行",
                    "ShearerDir.Down": "下行",
                    "ShearerDir.Stop": "停止",
                }.get(str(direction), str(direction))
                print(f"      {dir_name}: {count:4d} 次")

        # 时间范围统计
        all_times = []
        if action_data:
            all_times.extend([d["时间"] for d in action_data])
        if position_data:
            all_times.extend([d["时间"] for d in position_data])

        if all_times:
            print(f"\n⏰ 时间范围:")
            print(f"   {min(all_times)} 到 {max(all_times)}")
            print(
                f"   总计: {(max(all_times) - min(all_times)).total_seconds() / 60:.1f} 分钟"
            )

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

    def visualize_features(
        self,
        features: pd.DataFrame,
        output_dir: str = "outPut/features",
    ):
        """
        创建基于特征的可视化图表

        Args:
            features: tsfresh 提取的特征 DataFrame
            output_dir: 输出目录
        """
        if features.empty:
            print("❌ 没有特征数据，无法创建可视化")
            return

        print(f"\n📊 正在创建特征可视化图表...")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 1. 特征相关性热图
        self._create_feature_correlation_heatmap(features, output_path)

        # 2. 特征分布箱线图
        self._create_feature_distribution_boxplot(features, output_path)

        # 3. PCA 降维可视化
        self._create_pca_visualization(features, output_path)

        # 4. 特征重要性排名 (基于方差)
        self._create_feature_importance_ranking(features, output_path)

        print(f"✅ 特征可视化图表已保存到: {output_path}")

    def _create_feature_correlation_heatmap(
        self, features: pd.DataFrame, output_path: Path
    ):
        """创建特征相关性热图"""
        print("  📈 创建特征相关性热图...")

        # 选择前50个特征 (如果特征太多)
        if features.shape[1] > 50:
            # 按方差排序，选择方差最大的前50个特征
            feature_variances = features.var().sort_values(ascending=False)
            top_features = feature_variances.head(50).index.tolist()
            plot_data = features[top_features]
            title_suffix = "\n(显示方差最大的前50个特征)"
        else:
            plot_data = features
            title_suffix = ""

        # 计算相关性矩阵
        corr_matrix = plot_data.corr()

        # 创建热图
        fig, ax = plt.subplots(figsize=(16, 14))
        sns.heatmap(
            corr_matrix,
            cmap="coolwarm",
            center=0,
            square=True,
            linewidths=0.5,
            cbar_kws={"shrink": 0.8},
            ax=ax,
        )
        ax.set_title(
            f"特征相关性热图{title_suffix}",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )
        plt.tight_layout()
        plt.savefig(
            output_path / "feature_correlation_heatmap.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

    def _create_feature_distribution_boxplot(
        self, features: pd.DataFrame, output_path: Path
    ):
        """创建特征分布箱线图"""
        print("  📊 创建特征分布箱线图...")

        # 选择前20个特征
        if features.shape[1] > 20:
            feature_variances = features.var().sort_values(ascending=False)
            top_features = feature_variances.head(20).index.tolist()
            plot_data = features[top_features]
            title_suffix = "\n(显示方差最大的前20个特征)"
        else:
            plot_data = features
            title_suffix = ""

        # 标准化数据以便比较
        scaler = StandardScaler()
        plot_data_scaled = pd.DataFrame(
            scaler.fit_transform(plot_data),
            columns=plot_data.columns,
            index=plot_data.index,
        )

        # 重塑为长格式
        plot_data_long = plot_data_scaled.melt(var_name="特征", value_name="标准化值")

        # 创建箱线图
        fig, ax = plt.subplots(figsize=(16, 8))
        sns.boxplot(
            data=plot_data_long,
            x="特征",
            y="标准化值",
            palette="Set3",
            ax=ax,
        )
        ax.set_title(
            f"特征分布箱线图{title_suffix}",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )
        ax.set_xlabel("特征名称", fontsize=12, fontweight="bold")
        ax.set_ylabel("标准化值", fontsize=12, fontweight="bold")
        plt.xticks(rotation=90, ha="right")
        plt.tight_layout()
        plt.savefig(
            output_path / "feature_distribution_boxplot.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

    def _create_pca_visualization(self, features: pd.DataFrame, output_path: Path):
        """创建 PCA 降维可视化"""
        print("  🔍 创建 PCA 降维可视化...")

        # 标准化数据
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features.fillna(0))

        # 执行 PCA
        n_components = min(3, features.shape[0], features.shape[1])
        pca = PCA(n_components=n_components)
        features_pca = pca.fit_transform(features_scaled)

        # 创建散点图
        fig, ax = plt.subplots(figsize=(12, 8))

        if n_components >= 2:
            scatter = ax.scatter(
                features_pca[:, 0],
                features_pca[:, 1],
                c=range(len(features)),
                cmap="viridis",
                alpha=0.7,
                s=100,
                edgecolors="black",
                linewidths=0.5,
            )
            ax.set_xlabel(
                f"主成分1 ({pca.explained_variance_ratio_[0] * 100:.1f}%)",
                fontsize=12,
                fontweight="bold",
            )
            ax.set_ylabel(
                f"主成分2 ({pca.explained_variance_ratio_[1] * 100:.1f}%)",
                fontsize=12,
                fontweight="bold",
            )

            # 添加源地址标签
            for i, (idx, _) in enumerate(features.iterrows()):
                ax.annotate(
                    str(idx),
                    (features_pca[i, 0], features_pca[i, 1]),
                    fontsize=8,
                    alpha=0.7,
                )

        ax.set_title(
            f"PCA 降维可视化\n(累积解释方差: {sum(pca.explained_variance_ratio_) * 100:.1f}%)",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )
        ax.grid(True, alpha=0.3, linestyle="--")
        plt.tight_layout()
        plt.savefig(
            output_path / "pca_visualization.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

    def _create_feature_importance_ranking(
        self, features: pd.DataFrame, output_path: Path
    ):
        """创建特征重要性排名图"""
        print("  🏆 创建特征重要性排名图...")

        # 计算每个特征的方差作为重要性指标
        feature_variances = features.var().sort_values(ascending=False)

        # 选择前30个特征
        top_features = feature_variances.head(30)

        # 创建水平条形图
        fig, ax = plt.subplots(figsize=(12, 10))
        y_pos = np.arange(len(top_features))

        ax.barh(
            y_pos,
            top_features.values,
            color="skyblue",
            edgecolor="navy",
            linewidth=0.5,
        )
        ax.set_yticks(y_pos)
        ax.set_yticklabels(top_features.index, fontsize=9)
        ax.invert_yaxis()  # 最重要的在顶部
        ax.set_xlabel(
            "方差 (特征重要性)",
            fontsize=12,
            fontweight="bold",
        )
        ax.set_title(
            "特征重要性排名 (基于方差)\n(前30个特征)",
            fontsize=16,
            fontweight="bold",
            pad=20,
        )
        ax.grid(axis="x", alpha=0.3, linestyle="--")

        plt.tight_layout()
        plt.savefig(
            output_path / "feature_importance_ranking.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()
