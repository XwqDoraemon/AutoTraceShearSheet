#!/usr/bin/env python3
"""
数据导出类
将电液控数据导出为 JSON 文件，供前端使用
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from .dataProcessor import DataProcessor
from .feature_extractor import TsfreshFeatureExtractor
from .trajectory_analyzer import TrajectoryAnalyzer


class ExportData:
    """
    数据导出类
    负责将电液控数据导出为各种格式的 JSON 文件
    """

    def __init__(
        self,
        db_paths: Optional[Union[str, List[str]]] = None,
        output_dir: Optional[str] = None,
    ):
        """
        初始化导出器

        Args:
            db_paths: 数据库路径（支持单个路径字符串或路径列表），默认为 ["Datas/电液控UDP驱动_20250904_14.db"]
            output_dir: 输出目录，默认为 "dashboard/public/data"
        """
        # 设置默认数据库路径
        if db_paths is None:
            db_paths = ["Datas/电液控UDP驱动_20250904_14.db"]

        # 统一参数为列表
        if isinstance(db_paths, str):
            self.db_paths = [db_paths]
        else:
            self.db_paths = db_paths

        # 设置输出目录
        if output_dir is None:
            project_root = Path(__file__).parent.parent
            self.output_dir = project_root / "dashboard" / "public" / "data"
        else:
            self.output_dir = Path(output_dir)

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 存储加载的数据
        self.data = []
        self.statistics = {}

    def load_data(self) -> List[Tuple]:
        """
        加载数据库数据

        Returns:
            加载的数据列表
        """
        print("加载数据...")

        # 验证数据库文件
        valid_db_paths = []
        for db_path in self.db_paths:
            if Path(db_path).exists():
                valid_db_paths.append(db_path)
            else:
                print(f"⚠️ 数据库文件不存在，跳过: {db_path}")

        if not valid_db_paths:
            print("❌ 没有有效的数据库文件")
            return []

        # 合并所有数据库的数据
        all_data = []
        for i, db_path in enumerate(valid_db_paths, 1):
            print(f"📖 正在处理第 {i}/{len(valid_db_paths)} 个数据库: {db_path}")
            processor = DataProcessor(db_path)
            data = processor.process_data_in_batches()
            print(f"   ✓ 加载了 {len(data)} 条记录")
            all_data.extend(data)

        self.data = all_data
        print(f"\n✓ 总共加载了 {len(self.data)} 条记录\n")

        return self.data

    def export_statistics(self) -> Dict:
        """
        导出统计信息到 JSON 文件

        Returns:
            统计信息字典
        """
        print("导出统计信息...")

        action_records = []
        position_records = []
        all_times = []
        unique_sources = set()

        for dt, src_no, parsed_result in self.data:
            frame_type = parsed_result.get("frame_type")
            all_times.append(dt)
            unique_sources.add(src_no)

            if frame_type == "支架动作":
                action_records.append((dt, src_no))
            elif frame_type == "煤机位置":
                position_records.append((dt, src_no))

        self.statistics = {
            "total_records": len(self.data),
            "action_records": len(action_records),
            "position_records": len(position_records),
            "unique_sources": len(unique_sources),
            "time_range_start": min(all_times).isoformat() if all_times else None,
            "time_range_end": max(all_times).isoformat() if all_times else None,
            "duration_minutes": (max(all_times) - min(all_times)).total_seconds() / 60
            if all_times
            else 0,
            "export_time": datetime.now().isoformat(),
        }

        stats_file = self.output_dir / "statistics.json"
        with open(stats_file, "w", encoding="utf-8") as f:
            json.dump(self.statistics, f, ensure_ascii=False, indent=2)

        print(f"✓ 统计信息已导出: statistics.json\n")
        return self.statistics

    def export_scatter_data(self) -> Dict:
        """
        导出散点图数据到 JSON 文件

        Returns:
            散点图数据字典
        """
        print("导出散点图数据...")

        action_data = []
        position_data = []
        preview_data = []

        for dt, src_no, parsed_result in self.data:
            frame_type = parsed_result.get("frame_type")
            result_data = parsed_result.get("data", {})

            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")

            if frame_type == "支架动作":
                action_codes = result_data.get("actionCodes", [])
                action_type = str(result_data.get("actionType", ""))

                # 如果有 action_codes，则拆分成多个对象
                if action_codes:
                    for action_code in action_codes:
                        item = {
                            "x": time_str,
                            "y": src_no,
                            "name": f"源地址{src_no}",
                            "frame_type": "支架动作",
                            "src_no": src_no,
                            "action_code": str(action_code),
                            "action_type": action_type,
                        }
                        action_data.append(item)

                        # 预览数据
                        if len(preview_data) < 100:
                            preview_data.append(
                                {
                                    "time": time_str,
                                    "src_no": src_no,
                                    "frame_type": "支架动作",
                                    "action_type": action_type,
                                    "action_code": str(action_code),
                                }
                            )
                else:
                    # 如果没有 action_codes，保留原对象
                    item = {
                        "x": time_str,
                        "y": src_no,
                        "name": f"源地址{src_no}",
                        "frame_type": "支架动作",
                        "src_no": src_no,
                        "action_code": "",
                        "action_type": action_type,
                    }
                    action_data.append(item)

                    # 预览数据
                    if len(preview_data) < 100:
                        preview_data.append(
                            {
                                "time": time_str,
                                "src_no": src_no,
                                "frame_type": "支架动作",
                                "action_type": action_type,
                                "action_code": "",
                            }
                        )

            elif frame_type == "煤机位置":
                position = result_data.get("position", 0)
                direction = result_data.get("dir", "")

                item = {
                    "x": time_str,
                    "y": position,
                    "name": f"位置{position}",
                    "frame_type": "煤机位置",
                    "position": position,
                    "direction": str(direction),
                }
                position_data.append(item)

                # 预览数据
                if len(preview_data) < 100:
                    preview_data.append(
                        {
                            "time": time_str,
                            "src_no": src_no,
                            "frame_type": "煤机位置",
                            "position": position,
                            "direction": str(direction),
                        }
                    )

        scatter_data = {
            "action_data": action_data,
            "position_data": position_data,
            "export_time": datetime.now().isoformat(),
        }

        scatter_file = self.output_dir / "scatter.json"
        with open(scatter_file, "w", encoding="utf-8") as f:
            json.dump(scatter_data, f, ensure_ascii=False, indent=2)

        print(f"✓ 散点图数据已导出: scatter.json")
        print(f"  - 支架动作: {len(action_data)} 条")
        print(f"  - 煤机位置: {len(position_data)} 条\n")

        # 导出预览数据
        preview_file = self.output_dir / "preview.json"
        with open(preview_file, "w", encoding="utf-8") as f:
            json.dump({"data": preview_data}, f, ensure_ascii=False, indent=2)
        print(f"✓ 预览数据已导出: preview.json ({len(preview_data)} 条)\n")

        return scatter_data

    def export_feature_data(self) -> Dict:
        """
        导出特征数据（PCA 和特征重要性）到 JSON 文件

        Returns:
            特征数据字典
        """
        print("导出特征数据...")
        results = {}

        try:
            extractor = TsfreshFeatureExtractor(output_dir=str(self.output_dir))
            tsfresh_df = extractor.prepare_dataframe(self.data)

            if not tsfresh_df.empty:
                features = extractor.extract_features(tsfresh_df)

                if not features.empty:
                    # PCA 数据
                    from sklearn.decomposition import PCA
                    from sklearn.preprocessing import StandardScaler

                    scaler = StandardScaler()
                    features_scaled = scaler.fit_transform(features.fillna(0))

                    n_components = min(2, features.shape[0], features.shape[1])
                    pca = PCA(n_components=n_components)
                    features_pca = pca.fit_transform(features_scaled)

                    pca_data = []
                    for i, (idx, _) in enumerate(features.iterrows()):
                        pca_data.append(
                            {
                                "component_1": float(features_pca[i, 0])
                                if n_components >= 1
                                else 0,
                                "component_2": float(features_pca[i, 1])
                                if n_components >= 2
                                else 0,
                                "src_no": int(idx),
                                "variance_ratio": float(
                                    sum(pca.explained_variance_ratio_)
                                ),
                            }
                        )

                    pca_result = {
                        "data": pca_data,
                        "variance_ratio": [
                            float(v) for v in pca.explained_variance_ratio_
                        ],
                        "export_time": datetime.now().isoformat(),
                    }

                    pca_file = self.output_dir / "pca.json"
                    with open(pca_file, "w", encoding="utf-8") as f:
                        json.dump(pca_result, f, ensure_ascii=False, indent=2)
                    print(f"✓ PCA 数据已导出: pca.json")

                    results["pca"] = pca_result

                    # 特征重要性
                    variances = features.var().sort_values(ascending=False).head(30)
                    feature_importance = []
                    for feature_name, variance in variances.items():
                        feature_importance.append(
                            {"name": feature_name, "value": float(variance)}
                        )

                    importance_result = {
                        "features": feature_importance,
                        "export_time": datetime.now().isoformat(),
                    }

                    importance_file = self.output_dir / "importance.json"
                    with open(importance_file, "w", encoding="utf-8") as f:
                        json.dump(importance_result, f, ensure_ascii=False, indent=2)
                    print(f"✓ 特征重要性已导出: importance.json\n")

                    results["importance"] = importance_result
                else:
                    print("⚠ 特征提取失败，跳过特征数据导出\n")
            else:
                print("⚠ 数据准备失败，跳过特征数据导出\n")
        except Exception as e:
            print(f"⚠ 特征导出出错: {e}\n")

        return results

    def export_trajectory_segments(self) -> Dict:
        """
        Export trajectory segmentation data to JSON

        Returns:
            Trajectory segments dictionary
        """
        print("导出轨迹分段数据...")

        # Extract position data from loaded data
        position_data = []
        for dt, src_no, parsed_result in self.data:
            if parsed_result.get("frame_type") == "煤机位置":
                result_data = parsed_result.get("data", {})
                position = result_data.get("position", 0)
                direction = str(result_data.get("dir", ""))
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")

                position_data.append(
                    {
                        "x": time_str,
                        "y": position,
                        "position": position,
                        "direction": direction,
                    }
                )

        if not position_data:
            print("⚠️ 没有煤机位置数据，跳过轨迹分段")
            return {}

        # Analyze trajectory
        analyzer = TrajectoryAnalyzer(position_data)
        result = analyzer.analyze()

        # Export to JSON
        segments_file = self.output_dir / "trajectory_segments.json"
        with open(segments_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"✓ 轨迹分段数据已导出: trajectory_segments.json")
        print(
            f"  - 关键位置: 机头{result['key_positions']['min_head']}-{result['key_positions']['max_head_cleaning']}, "
            f"机尾{result['key_positions']['min_tail_cleaning']}-{result['key_positions']['max_tail']}"
        )
        print(f"  - 识别周期数: {result['total_cycles']}")

        if result["key_positions"].get("head_triangular_point"):
            print(
                f"  - 机头三角煤折返点: {result['key_positions']['head_triangular_point']}"
            )
        if result["key_positions"].get("tail_triangular_point"):
            print(
                f"  - 机尾三角煤折返点: {result['key_positions']['tail_triangular_point']}"
            )

        print()

        return result

    def export_all(self):
        """
        导出所有数据
        """
        print("=" * 60)
        print("数据导出脚本 - 导出 JSON 文件")
        print("=" * 60)
        print(f"输出目录: {self.output_dir}")
        print(f"数据库文件数: {len(self.db_paths)}")
        for i, db_path in enumerate(self.db_paths, 1):
            print(f"   {i}. {db_path}")
        print()

        # 1. 加载数据
        print("[1/4] 加载数据...")
        self.load_data()

        if not self.data:
            print("❌ 没有数据可导出")
            return

        # 2. 导出统计信息
        print("[2/5] 导出统计信息...")
        self.export_statistics()

        # 3. 导出散点图数据
        print("[3/5] 导出散点图数据...")
        self.export_scatter_data()

        # 4. 导出轨迹分段数据
        print("[4/5] 导出轨迹分段数据...")
        self.export_trajectory_segments()

        # 5. 导出特征数据（可选）
        print("[5/5] 导出特征数据...")
        self.export_feature_data()

        print()
        print("=" * 60)
        print("✅ 数据导出完成！")
        print("=" * 60)
        print()
        print("导出的文件:")
        print(f"  📄 statistics.json")
        print(f"  📄 scatter.json")
        print(f"  📄 preview.json")
        print(f"  📄 trajectory_segments.json")
        if (self.output_dir / "pca.json").exists():
            print(f"  📄 pca.json")
            print(f"  📄 importance.json")
        print()
        print("💡 提示: 在前端点击'重新加载'按钮来刷新数据")
