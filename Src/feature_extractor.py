#!/usr/bin/env python3
"""
时间序列特征提取模块
使用 tsfresh 从电液控数据中自动提取特征
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tsfresh import extract_features, select_features
from tsfresh.feature_extraction import EfficientFCParameters
from tsfresh.utilities.dataframe_functions import impute


class TsfreshFeatureExtractor:
    """tsfresh 特征提取器 - 从时间序列数据中自动提取特征"""

    def __init__(self, output_dir: str = "outPut/features"):
        """
        初始化特征提取器

        Args:
            output_dir: 特征导出目录
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 使用精简的特征计算配置以提高性能
        self.fc_parameters = EfficientFCParameters()

        print(f"✓ 特征提取器初始化完成 (输出目录: {self.output_dir})")

    def prepare_dataframe(self, data: List[Tuple[datetime, int, dict]]) -> pd.DataFrame:
        """
        将处理后的数据转换为 tsfresh 需要的 DataFrame 格式

        Args:
            data: 处理后的数据 [(时间, src_no, 解析结果), ...]

        Returns:
            符合 tsfresh 格式的 DataFrame，包含列:
            - id: 序列标识 (src_no)
            - time: 时间戳
            - kind: 数据类型 (action_count/position/direction)
            - value: 数值
        """
        print("📊 正在准备时间序列数据...")

        rows = []

        for dt, src_no, parsed_result in data:
            frame_type = parsed_result.get("frame_type")
            result_data = parsed_result.get("data", {})

            if frame_type == "支架动作":
                # 为支架动作创建特征
                action_type = result_data.get("actionType")
                action_codes = result_data.get("actionCodes", [])

                # 特征1: 动作数量
                rows.append(
                    {
                        "id": src_no,
                        "time": dt,
                        "kind": "action_count",
                        "value": len(action_codes),
                    }
                )

                # 特征2: 动作类型编码 (将动作类型转换为数值)
                action_type_map = {
                    "ActionType.无动作": 0,
                    "ActionType.单动动作": 1,
                    "ActionType.成组动作": 2,
                    "ActionType.跟机自动化": 3,
                    "ActionType.点动动作": 4,
                    "ActionType.自动补压": 5,
                    "ActionType.单架自动": 6,
                }
                action_type_value = action_type_map.get(str(action_type), 0)
                rows.append(
                    {
                        "id": src_no,
                        "time": dt,
                        "kind": "action_type",
                        "value": action_type_value,
                    }
                )

            elif frame_type == "煤机位置":
                # 为煤机位置创建特征
                direction = result_data.get("dir")
                position = result_data.get("position", 0)

                # 特征1: 位置值
                rows.append(
                    {
                        "id": src_no,
                        "time": dt,
                        "kind": "position",
                        "value": position,
                    }
                )

                # 特征2: 方向编码
                dir_map = {
                    "ShearerDir.Stop": 0,
                    "ShearerDir.Down": 1,
                    "ShearerDir.Up": 2,
                }
                dir_value = dir_map.get(str(direction), 0)
                rows.append(
                    {
                        "id": src_no,
                        "time": dt,
                        "kind": "direction",
                        "value": dir_value,
                    }
                )

        df = pd.DataFrame(rows)

        if not df.empty:
            print(f"✓ 数据准备完成: {len(df)} 行, {df['id'].nunique()} 个源地址")
            print(f"  - 数据类型 (kind): {df['kind'].unique()}")
        else:
            print("⚠ 警告: 没有可用的数据")

        return df

    def extract_features(
        self,
        df: pd.DataFrame,
        column_id: str = "id",
        column_kind: str = "kind",
        column_value: str = "value",
        column_sort: str = "time",
    ) -> pd.DataFrame:
        """
        使用 tsfresh 提取时间序列特征

        Args:
            df: tsfresh 格式的 DataFrame
            column_id: 序列ID列名
            column_kind: 数据类型列名
            column_value: 数值列名
            column_sort: 时间排序列名

        Returns:
            特征 DataFrame，每行代表一个 id (src_no)，列为提取的特征
        """
        if df.empty:
            print("❌ 数据为空，无法提取特征")
            return pd.DataFrame()

        print("🔬 正在提取时间序列特征...")
        print(f"  - 使用 {len(self.fc_parameters)} 个特征计算器")

        try:
            # 提取特征 - 使用正确的参数名
            extracted_features = extract_features(
                df,
                column_id=column_id,
                column_sort=column_sort,
                column_kind=column_kind,
                column_value=column_value,
                default_fc_parameters=self.fc_parameters,  # 修复: 使用 default_fc_parameters
                impute_function=impute,  # 自动填充缺失值
                n_jobs=0,  # 使用所有可用CPU核心
            )

            print(
                f"✓ 特征提取完成: {extracted_features.shape[1]} 个特征 × {extracted_features.shape[0]} 个源地址"
            )
            print(f"  - 特征数量: {extracted_features.shape[1]}")
            print(f"  - 样本数量: {extracted_features.shape[0]}")

            # 显示特征统计
            self._print_feature_statistics(extracted_features)

            return extracted_features

        except Exception as e:
            print(f"❌ 特征提取失败: {e}")
            import traceback

            traceback.print_exc()
            return pd.DataFrame()

    def select_relevant_features(
        self,
        features: pd.DataFrame,
        target: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        选择相关特征 (可选功能)

        Args:
            features: 提取的特征 DataFrame
            target: 目标变量 (如果有分类/回归任务)

        Returns:
            筛选后的特征 DataFrame
        """
        if features.empty:
            return features

        print("🎯 正在筛选重要特征...")

        # 如果没有提供目标变量，直接返回所有特征
        if target is None:
            print("  - 未提供目标变量，保留所有特征")
            return features

        try:
            # 选择与目标相关的特征
            selected_features = select_features(
                features,
                target,
                n_jobs=0,
            )

            print(
                f"✓ 特征筛选完成: {selected_features.shape[1]} / {features.shape[1]} 个特征"
            )
            print(
                f"  - 移除了 {features.shape[1] - selected_features.shape[1]} 个无关特征"
            )

            return selected_features

        except Exception as e:
            print(f"⚠ 特征筛选失败: {e}")
            return features

    def export_features(
        self,
        features: pd.DataFrame,
        filename: str = "extracted_features.csv",
    ):
        """
        导出特征到 CSV 文件

        Args:
            features: 特征 DataFrame
            filename: 输出文件名
        """
        if features.empty:
            print("❌ 没有特征可导出")
            return

        output_path = self.output_dir / filename

        try:
            features.to_csv(output_path, index=True, encoding="utf-8-sig")
            print(f"💾 特征已导出到: {output_path}")
            print(f"  - 文件大小: {output_path.stat().st_size / 1024:.2f} KB")
        except Exception as e:
            print(f"❌ 导出失败: {e}")

    def _print_feature_statistics(self, features: pd.DataFrame):
        """打印特征统计信息"""
        print("\n" + "=" * 60)
        print("📊 特征统计信息")
        print("=" * 60)

        # 显示前10个特征列名
        print(f"📋 前10个特征列:")
        for i, col in enumerate(features.columns[:10]):
            print(f"   {i + 1}. {col}")

        if len(features.columns) > 10:
            print(f"   ... 还有 {len(features.columns) - 10} 个特征")

        # 显示统计摘要
        print(f"\n📈 特征值统计摘要:")
        print(features.describe())

        # 显示缺失值情况
        missing_count = features.isnull().sum().sum()
        total_values = features.shape[0] * features.shape[1]
        missing_pct = (missing_count / total_values * 100) if total_values > 0 else 0

        print(f"\n🔍 缺失值: {missing_count:,} / {total_values:,} ({missing_pct:.2f}%)")
