"""
采煤机轨迹关键点提取器 (带异常点过滤)

根据position_data中的实际position范围，从数据中提取关键点：
1. 机头扫煤最远点（最小值组中的最大转折点）
2. 机尾扫煤最远点（最大值组中的最小转折点）

改进功能：
- 异常点过滤：去除趋势突变点和不合理数据
- 相间分布：确保最小值组和最大值组按时间顺序相间分布
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from Scripts.anomaly_filter import AnomalyFilter


class TrajectoryKeyPointsExtractor:
    """轨迹关键点提取器（带异常点过滤）"""

    def __init__(self, scatter_json_path: str):
        """
        初始化提取器

        Args:
            scatter_json_path: scatter.json文件路径
        """
        self.scatter_json_path = scatter_json_path
        self.action_data = []
        self.position_data = []
        self.filtered_position_data = []
        self.anomalies = []
        self.key_points = []

    def load_data(self):
        """加载scatter.json数据"""
        print(f"正在加载数据: {self.scatter_json_path}")
        with open(self.scatter_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.action_data = data.get("action_data", [])
        self.position_data = data.get("position_data", [])

        print(
            f"加载完成: action_data={len(self.action_data)}条, position_data={len(self.position_data)}条"
        )

    def filter_anomalies(self, max_change_threshold: int = 10) -> int:
        """
        过滤异常点

        Args:
            max_change_threshold: 最大允许变化幅度

        Returns:
            过滤掉的异常点数量
        """
        print("\n" + "-" * 60)
        print("异常点过滤")
        print("-" * 60)

        filter_obj = AnomalyFilter(self.position_data)
        anomalies = filter_obj.detect_anomalies()

        print(f"检测到 {len(anomalies)} 个异常点")

        if anomalies:
            print("\n异常点列表:")
            for i, anomaly in enumerate(anomalies):
                print(
                    f"  {i + 1}. 时间: {anomaly['data']['x']}, 位置: {anomaly['data']['position']}"
                )
                print(f"     原因: {anomaly['reason']}")

        # 过滤异常点
        self.filtered_position_data = filter_obj.filter_anomalies(keep_anomalies=False)
        self.anomalies = anomalies

        stats = filter_obj.get_statistics()
        print(f"\n过滤统计:")
        print(f"  原始数据: {stats['original_count']}条")
        print(f"  异常点: {stats['anomaly_count']}条")
        print(f"  过滤后: {stats['filtered_count']}条")
        print(f"  过滤率: {stats['filter_rate']:.2f}%")

        return len(anomalies)

    def get_position_range(self) -> Tuple[int, int]:
        """
        从filtered_position_data中获取position的最小值和最大值

        Returns:
            (最小position, 最大position)
        """
        data_source = (
            self.filtered_position_data
            if self.filtered_position_data
            else self.position_data
        )
        positions = [item["position"] for item in data_source]
        min_pos = min(positions)
        max_pos = max(positions)

        print(f"\nposition范围: 最小值={min_pos}, 最大值={max_pos}")
        return min_pos, max_pos

    def filter_position_data(
        self, min_pos: int, max_pos: int, range_size: int = 15
    ) -> List[Dict]:
        """
        筛选position_data中位于[min_pos, min_pos+range_size]和[max_pos-range_size, max_pos]区间的数据

        Args:
            min_pos: 最小position
            max_pos: 最大position
            range_size: 筛选区间大小，默认15

        Returns:
            筛选后的位置数据列表
        """
        data_source = (
            self.filtered_position_data
            if self.filtered_position_data
            else self.position_data
        )

        min_range_end = min_pos + range_size
        max_range_start = max_pos - range_size

        filtered = [
            item
            for item in data_source
            if (min_pos <= item["position"] <= min_range_end)
            or (max_range_start <= item["position"] <= max_pos)
        ]

        print(f"\n筛选位置数据: 原始{len(data_source)}条 -> 筛选后{len(filtered)}条")
        print(f"  筛选范围1 (最小值组): [{min_pos}, {min_range_end}]")
        print(f"  筛选范围2 (最大值组): [{max_range_start}, {max_pos}]")

        return filtered, min_range_end, max_range_start

    def group_by_time(
        self, filtered_data: List[Dict], min_range_end: int, max_range_start: int
    ) -> List[List[Dict]]:
        """
        将数据按时间分组，确保最小值组和最大值组相间分布

        Args:
            filtered_data: 筛选后的位置数据
            min_range_end: 最小值组的范围上限
            max_range_start: 最大值组的范围下限

        Returns:
            分组后的数据列表，每组包含连续的时间相近的数据点，且相邻组类型不同
        """
        if not filtered_data:
            return []

        # 按时间排序
        sorted_data = sorted(filtered_data, key=lambda x: x["x"])

        # 第一步：初步分组（按范围切换或时间间隔）
        preliminary_groups = []
        current_group = [sorted_data[0]]
        current_range = "min" if sorted_data[0]["position"] <= min_range_end else "max"

        for item in sorted_data[1:]:
            item_range = "min" if item["position"] <= min_range_end else "max"

            # 计算时间差
            time_diff = self._get_time_diff(current_group[-1]["x"], item["x"])
            range_changed = item_range != current_range

            # 判断是否需要创建新组
            should_create_new_group = (range_changed and time_diff > 60) or (
                time_diff > 300
            )

            if should_create_new_group:
                preliminary_groups.append(current_group)
                current_group = [item]
                current_range = item_range
            else:
                current_group.append(item)

        if current_group:
            preliminary_groups.append(current_group)

        # 第二步：合并相邻的同类型组
        groups = []
        for group in preliminary_groups:
            group_type = "min" if group[0]["position"] <= min_range_end else "max"

            if not groups:
                groups.append(group)
            else:
                last_group_type = (
                    "min" if groups[-1][0]["position"] <= min_range_end else "max"
                )

                if group_type == last_group_type:
                    # 同类型，合并
                    groups[-1].extend(group)
                else:
                    # 不同类型，添加新组
                    groups.append(group)

        print(f"\n时间分组: 共{len(groups)}组（相间分布）")
        for i, group in enumerate(groups):
            positions = [item["position"] for item in group]
            range_type = (
                "最小值组" if group[0]["position"] <= min_range_end else "最大值组"
            )
            print(
                f"  组{i + 1}: {range_type}, {len(group)}个点, position范围[{min(positions)}, {max(positions)}]"
            )

        # 验证相间分布
        if len(groups) > 1:
            all_alternating = True
            for i in range(len(groups) - 1):
                type1 = "min" if groups[i][0]["position"] <= min_range_end else "max"
                type2 = (
                    "min" if groups[i + 1][0]["position"] <= min_range_end else "max"
                )
                if type1 == type2:
                    print(f"  [警告] 组{i + 1}和组{i + 2}类型相同，不满足相间分布")
                    all_alternating = False

            if all_alternating:
                print(f"  [验证通过] 所有组均满足相间分布")

        return groups

    def _get_time_diff(self, time1: str, time2: str) -> int:
        """计算两个时间字符串的差值（秒）"""
        t1 = datetime.strptime(time1, "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(time2, "%Y-%m-%d %H:%M:%S")
        return abs((t2 - t1).total_seconds())

    def find_min_group_key_points(self, group: List[Dict]) -> Dict:
        """从最小值组中找到关键点"""
        positions = [item["position"] for item in group]
        min_position = min(positions)

        min_point = None
        for item in group:
            if item["position"] == min_position:
                min_point = item
                break

        turning_points = []
        for i in range(1, len(group) - 1):
            current_pos = group[i]["position"]
            left_pos = group[i - 1]["position"]
            right_pos = group[i + 1]["position"]

            if current_pos > left_pos and current_pos > right_pos:
                turning_points.append(group[i])

        farthest_point = None
        if turning_points:
            farthest_point = max(turning_points, key=lambda x: x["position"])

        return {
            "group_type": "最小值组",
            "min_position_point": min_point,
            "min_position_value": min_position,
            "farthest_sweeping_point": farthest_point,
            "turning_points_count": len(turning_points),
            "group_size": len(group),
        }

    def find_max_group_key_points(self, group: List[Dict]) -> Dict:
        """从最大值组中找到关键点"""
        positions = [item["position"] for item in group]
        max_position = max(positions)

        max_point = None
        for item in group:
            if item["position"] == max_position:
                max_point = item
                break

        turning_points = []
        for i in range(1, len(group) - 1):
            current_pos = group[i]["position"]
            left_pos = group[i - 1]["position"]
            right_pos = group[i + 1]["position"]

            if current_pos < left_pos and current_pos < right_pos:
                turning_points.append(group[i])

        farthest_point = None
        if turning_points:
            farthest_point = min(turning_points, key=lambda x: x["position"])

        return {
            "group_type": "最大值组",
            "max_position_point": max_point,
            "max_position_value": max_position,
            "farthest_sweeping_point": farthest_point,
            "turning_points_count": len(turning_points),
            "group_size": len(group),
        }

    def extract_key_points(
        self,
        range_size: int = 15,
        enable_filter: bool = True,
        max_change_threshold: int = 10,
    ):
        """
        提取所有关键点

        Args:
            range_size: 筛选区间大小，默认15
            enable_filter: 是否启用异常点过滤，默认True
            max_change_threshold: 最大允许变化幅度，默认10
        """
        print("\n" + "=" * 60)
        print("开始提取关键点（带异常点过滤版本）")
        print("=" * 60)

        # 1. 加载数据
        self.load_data()

        # 2. 过滤异常点
        if enable_filter:
            self.filter_anomalies(max_change_threshold=max_change_threshold)

        # 3. 获取position范围
        min_pos, max_pos = self.get_position_range()

        # 4. 筛选position_data
        filtered_data, min_range_end, max_range_start = self.filter_position_data(
            min_pos, max_pos, range_size
        )

        if not filtered_data:
            print("警告: 筛选后的数据为空，无法提取关键点")
            return

        # 5. 按时间分组（确保相间分布）
        groups = self.group_by_time(filtered_data, min_range_end, max_range_start)

        if not groups:
            print("警告: 无法分组，无法提取关键点")
            return

        # 6. 提取每组的关键点
        print("\n" + "-" * 60)
        print("关键点详情")
        print("-" * 60)

        for i, group in enumerate(groups):
            first_position = group[0]["position"]

            if first_position <= min_range_end:
                key_point = self.find_min_group_key_points(group)
                self.key_points.append(key_point)

                print(f"\n组{i + 1} [最小值组]:")
                print(
                    f"  煤机位置最小值: {key_point['min_position_value']} (时间: {key_point['min_position_point']['x']})"
                )
                if key_point["farthest_sweeping_point"]:
                    print(
                        f"  机头扫煤最远点: {key_point['farthest_sweeping_point']['position']} (时间: {key_point['farthest_sweeping_point']['x']})"
                    )
                else:
                    print(f"  机头扫煤最远点: 未找到转折点")
                print(f"  转折点数量: {key_point['turning_points_count']}")
            else:
                key_point = self.find_max_group_key_points(group)
                self.key_points.append(key_point)

                print(f"\n组{i + 1} [最大值组]:")
                print(
                    f"  煤机位置最大值: {key_point['max_position_value']} (时间: {key_point['max_position_point']['x']})"
                )
                if key_point["farthest_sweeping_point"]:
                    print(
                        f"  机尾扫煤最远点: {key_point['farthest_sweeping_point']['position']} (时间: {key_point['farthest_sweeping_point']['x']})"
                    )
                else:
                    print(f"  机尾扫煤最远点: 未找到转折点")
                print(f"  转折点数量: {key_point['turning_points_count']}")

    def save_key_points_to_file(self, output_path: str):
        """将关键点保存到文件"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("采煤机轨迹关键点（带异常点过滤版本）\n")
            f.write("=" * 80 + "\n\n")

            # 写入过滤统计
            if self.anomalies:
                f.write(f"数据预处理: 过滤了 {len(self.anomalies)} 个异常点\n")
                f.write("-" * 80 + "\n\n")

            # 写入关键点
            for i, kp in enumerate(self.key_points):
                f.write(f"组{i + 1} [{kp['group_type']}]\n")
                f.write("-" * 80 + "\n")

                if kp["group_type"] == "最小值组":
                    f.write(f"煤机位置最小值: {kp['min_position_value']}\n")
                    f.write(f"  时间: {kp['min_position_point']['x']}\n")
                    f.write(f"  方向: {kp['min_position_point']['direction']}\n")

                    if kp["farthest_sweeping_point"]:
                        f.write(
                            f"\n机头扫煤最远点: {kp['farthest_sweeping_point']['position']}\n"
                        )
                        f.write(f"  时间: {kp['farthest_sweeping_point']['x']}\n")
                        f.write(
                            f"  方向: {kp['farthest_sweeping_point']['direction']}\n"
                        )
                    else:
                        f.write(f"\n机头扫煤最远点: 未找到转折点\n")
                else:
                    f.write(f"煤机位置最大值: {kp['max_position_value']}\n")
                    f.write(f"  时间: {kp['max_position_point']['x']}\n")
                    f.write(f"  方向: {kp['max_position_point']['direction']}\n")

                    if kp["farthest_sweeping_point"]:
                        f.write(
                            f"\n机尾扫煤最远点: {kp['farthest_sweeping_point']['position']}\n"
                        )
                        f.write(f"  时间: {kp['farthest_sweeping_point']['x']}\n")
                        f.write(
                            f"  方向: {kp['farthest_sweeping_point']['direction']}\n"
                        )
                    else:
                        f.write(f"\n机尾扫煤最远点: 未找到转折点\n")

                f.write(f"\n转折点数量: {kp['turning_points_count']}\n")
                f.write(f"组内数据点数量: {kp['group_size']}\n")
                f.write("\n" + "=" * 80 + "\n\n")

        print(f"\n关键点已保存到: {output_path}")


def main():
    """主函数"""
    # 数据文件路径
    scatter_json_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "dashboard",
        "public",
        "data",
        "scatter.json",
    )

    # 输出文件路径
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output",
        "trajectory_key_points_with_filter.txt",
    )

    # 创建提取器并提取关键点
    extractor = TrajectoryKeyPointsExtractor(scatter_json_path)
    extractor.extract_key_points(
        range_size=15,
        enable_filter=True,  # 启用异常点过滤
        max_change_threshold=10,
    )
    extractor.save_key_points_to_file(output_path)

    print("\n" + "=" * 60)
    print("关键点提取完成!")
    print("=" * 60)

    return extractor


if __name__ == "__main__":
    main()
