#!/usr/bin/env python3
"""
数据导出脚本
将电液控数据导出为 JSON 文件，供前端使用
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from Src.dataProcessor import DataProcessor
from Src.feature_extractor import TsfreshFeatureExtractor


def export_to_json(db_paths=None):
    """
    导出数据到 JSON 文件

    Args:
        db_paths: 数据库路径（支持单个路径字符串或路径列表），默认为 ["Datas/电液控UDP驱动_20250904_14.db"]
    """

    # 输出目录
    output_dir = project_root / "dashboard" / "public" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 设置默认数据库路径
    if db_paths is None:
        db_paths = ["Datas/电液控UDP驱动_20250904_14.db"]

    # 统一参数为列表
    if isinstance(db_paths, str):
        db_paths = [db_paths]

    print("=" * 60)
    print("数据导出脚本 - 导出 JSON 文件")
    print("=" * 60)
    print(f"输出目录: {output_dir}")
    print(f"数据库文件数: {len(db_paths)}")
    for i, db_path in enumerate(db_paths, 1):
        print(f"   {i}. {db_path}")
    print()

    # 1. 加载数据
    print("[1/4] 加载数据...")

    # 验证数据库文件
    valid_db_paths = []
    for db_path in db_paths:
        if Path(db_path).exists():
            valid_db_paths.append(db_path)
        else:
            print(f"⚠️ 数据库文件不存在，跳过: {db_path}")

    if not valid_db_paths:
        print("❌ 没有有效的数据库文件")
        return

    # 合并所有数据库的数据
    all_data = []
    for i, db_path in enumerate(valid_db_paths, 1):
        print(f"📖 正在处理第 {i}/{len(valid_db_paths)} 个数据库: {db_path}")
        processor = DataProcessor(db_path)
        data = processor.process_data_in_batches()
        print(f"   ✓ 加载了 {len(data)} 条记录")
        all_data.extend(data)

    data = all_data
    print(f"\n✓ 总共加载了 {len(data)} 条记录")
    print()

    # 2. 导出统计信息
    print("[2/4] 导出统计信息...")
    action_records = []
    position_records = []
    all_times = []
    unique_sources = set()

    for dt, src_no, parsed_result in data:
        frame_type = parsed_result.get("frame_type")
        all_times.append(dt)
        unique_sources.add(src_no)

        if frame_type == "支架动作":
            action_records.append((dt, src_no))
        elif frame_type == "煤机位置":
            position_records.append((dt, src_no))

    statistics = {
        "total_records": len(data),
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

    stats_file = output_dir / "statistics.json"
    with open(stats_file, "w", encoding="utf-8") as f:
        json.dump(statistics, f, ensure_ascii=False, indent=2)
    print(f"✓ 统计信息已导出: statistics.json")
    print()

    # 3. 导出散点图数据
    print("[3/4] 导出散点图数据...")
    action_data = []
    position_data = []
    preview_data = []

    for dt, src_no, parsed_result in data:
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

    scatter_file = output_dir / "scatter.json"
    with open(scatter_file, "w", encoding="utf-8") as f:
        json.dump(scatter_data, f, ensure_ascii=False, indent=2)
    print(f"✓ 散点图数据已导出: scatter.json")
    print(f"  - 支架动作: {len(action_data)} 条")
    print(f"  - 煤机位置: {len(position_data)} 条")
    print()

    # 导出预览数据
    preview_file = output_dir / "preview.json"
    with open(preview_file, "w", encoding="utf-8") as f:
        json.dump({"data": preview_data}, f, ensure_ascii=False, indent=2)
    print(f"✓ 预览数据已导出: preview.json ({len(preview_data)} 条)")
    print()

    # 4. 导出特征数据（可选）
    print("[4/4] 导出特征数据...")
    try:
        extractor = TsfreshFeatureExtractor(output_dir=str(output_dir))
        tsfresh_df = extractor.prepare_dataframe(data)

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
                            "variance_ratio": float(sum(pca.explained_variance_ratio_)),
                        }
                    )

                pca_result = {
                    "data": pca_data,
                    "variance_ratio": [float(v) for v in pca.explained_variance_ratio_],
                    "export_time": datetime.now().isoformat(),
                }

                pca_file = output_dir / "pca.json"
                with open(pca_file, "w", encoding="utf-8") as f:
                    json.dump(pca_result, f, ensure_ascii=False, indent=2)
                print(f"✓ PCA 数据已导出: pca.json")

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

                importance_file = output_dir / "importance.json"
                with open(importance_file, "w", encoding="utf-8") as f:
                    json.dump(importance_result, f, ensure_ascii=False, indent=2)
                print(f"✓ 特征重要性已导出: importance.json")
            else:
                print("⚠ 特征提取失败，跳过特征数据导出")
        else:
            print("⚠ 数据准备失败，跳过特征数据导出")
    except Exception as e:
        print(f"⚠ 特征导出出错: {e}")

    print()
    print("=" * 60)
    print("✅ 数据导出完成！")
    print("=" * 60)
    print()
    print("导出的文件:")
    print(f"  📄 {stats_file.name}")
    print(f"  📄 {scatter_file.name}")
    print(f"  📄 {preview_file.name}")
    if (output_dir / "pca.json").exists():
        print(f"  📄 pca.json")
        print(f"  📄 importance.json")
    print()
    print("💡 提示: 在前端点击'重新加载'按钮来刷新数据")


if __name__ == "__main__":
    # 示例1：导出单个数据库文件（使用默认路径）
    # export_to_json()

    # 示例2：导出单个数据库文件（指定路径）
    # export_to_json("Datas/电液控UDP驱动_20250904_14.db")

    # 示例3：导出多个数据库文件（合并导出）
    db_paths = [
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_14.db",
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_16.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_18.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_20.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250904_22.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月4日4点班/电液控UDP驱动_20250905_00.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_02.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_04.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_06.db",  # 添加更多数据库文件
        "Datas/乌东数据/9月5日0点班/电液控UDP驱动_20250905_08.db",  # 添加更多数据库文件
    ]
    export_to_json(db_paths)
