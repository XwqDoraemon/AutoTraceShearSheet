"""
数据预处理器 - 过滤异常点

识别并过滤掉不合理的突变点：
遇到突变点，当该点前面两点的变化趋势和该点后面两点的变化趋势相同，则该点是异常点，过滤掉
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Tuple


class AnomalyFilter:
    """异常点过滤器"""

    def __init__(self, position_data: List[Dict]):
        """
        初始化过滤器

        Args:
            position_data: 位置数据列表
        """
        self.position_data = self._remove_adjacent_duplicates(position_data)
        self.anomalies = []
        self.filtered_data = []

    def _remove_adjacent_duplicates(self, data: List[Dict]) -> List[Dict]:
        """
        移除相邻的重复点（位置值相同的点）

        Args:
            data: 原始数据列表

        Returns:
            移除重复点后的数据列表
        """
        if not data:
            return []

        # 按时间排序
        sorted_data = sorted(data, key=lambda x: x["x"])

        # 保留第一个点，然后移除后续位置相同的相邻点
        filtered = [sorted_data[0]]
        for i in range(1, len(sorted_data)):
            if sorted_data[i]["position"] != filtered[-1]["position"]:
                filtered.append(sorted_data[i])

        removed_count = len(sorted_data) - len(filtered)
        if removed_count > 0:
            print(f"移除了 {removed_count} 个相邻重复点")

        return filtered

    def detect_anomalies(self) -> List[Dict]:
        """
        检测异常点

        检测规则：
        1. 先检测当前点是否为拐点（前后趋势发生变化）
        2. 如果是拐点，再检查该点前面两点的斜率和后面两点的斜率是否相同
        3. 如果斜率相同，说明该点是异常的突变点

        斜率计算：
        - 前面斜率 = 前一点 - 与前一点不同的前面的点
        - 后面斜率 = 后一点 - 与后一点不同的点后面的点

        Returns:
            异常点列表
        """
        # 按时间排序
        sorted_data = sorted(self.position_data, key=lambda x: x["x"])

        self.anomalies = []

        # 需要至少5个点才能检测：前两点 + 当前点 + 后两点
        for i in range(2, len(sorted_data) - 2):
            pos_curr = sorted_data[i]["position"]
            pos_prev1 = sorted_data[i - 1]["position"]
            pos_next1 = sorted_data[i + 1]["position"]

            # 步骤1: 检测当前点是否为拐点
            # 计算进入趋势（前一点到当前点）
            slope_into = pos_curr - pos_prev1

            # 计算离开趋势（当前点到后一点）
            slope_out = pos_next1 - pos_curr

            # 判断是否为拐点：进入趋势和离开趋势方向相反
            is_inflection_point = (slope_into > 0 and slope_out < 0) or (
                slope_into < 0 and slope_out > 0
            )

            # 如果不是拐点，跳过
            if not is_inflection_point:
                continue

            # 步骤2: 对于拐点，计算前面和后面的斜率
            # 前面斜率 = 前一点 - 与前一点不同的前面的点
            slope_before = None
            for j in range(i - 2, -1, -1):
                if sorted_data[j]["position"] != pos_prev1:
                    slope_before = pos_prev1 - sorted_data[j]["position"]
                    break

            # 后面斜率 = 后一点 - 与后一点不同的点后面的点
            slope_after = None
            for j in range(i + 2, len(sorted_data)):
                if sorted_data[j]["position"] != pos_next1:
                    slope_after = sorted_data[j]["position"] - pos_next1
                    break

            # 如果找不到有效的斜率，跳过
            if slope_before is None or slope_after is None:
                continue

            # 判断前后斜率是否同向（都为正或都为负）
            same_direction = (slope_before > 0 and slope_after > 0) or (
                slope_before < 0 and slope_after < 0
            )

            # 如果前后斜率同向，说明该点是异常的突变点
            if same_direction:
                trend_direction = "上升" if slope_before > 0 else "下降"

                self.anomalies.append(
                    {
                        "index": i,
                        "data": sorted_data[i],
                        "type": "inflection_anomaly",
                        "slope_before": slope_before,
                        "slope_into": slope_into,
                        "slope_out": slope_out,
                        "slope_after": slope_after,
                        "reason": f"拐点异常: 前斜率={slope_before:+d}, 后斜率={slope_after:+d}, 整体趋势={trend_direction}",
                    }
                )

        # 按索引排序
        self.anomalies.sort(key=lambda x: x["index"])

        return self.anomalies

    def filter_anomalies(self, keep_anomalies: bool = False) -> List[Dict]:
        """
        过滤异常点

        Args:
            keep_anomalies: 是否保留异常点（用于调试）

        Returns:
            过滤后的数据
        """
        sorted_data = sorted(self.position_data, key=lambda x: x["x"])

        # 创建异常点索引集合
        anomaly_indices = set(a["index"] for a in self.anomalies)

        # 过滤数据
        if keep_anomalies:
            self.filtered_data = sorted_data  # 保留所有数据
        else:
            self.filtered_data = [
                (i, data)
                for i, data in enumerate(sorted_data)
                if i not in anomaly_indices
            ]
            # 移除索引
            self.filtered_data = [data for _, data in self.filtered_data]

        return self.filtered_data

    def get_statistics(self) -> Dict:
        """
        获取过滤统计信息

        Returns:
            统计信息字典
        """
        return {
            "original_count": len(self.position_data),
            "anomaly_count": len(self.anomalies),
            "filtered_count": len(self.filtered_data),
            "filter_rate": len(self.anomalies) / len(self.position_data) * 100,
        }

    def save_filtered_data(self, output_path: str):
        """
        保存过滤后的数据

        Args:
            output_path: 输出文件路径
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("数据过滤报告\n")
            f.write("=" * 80 + "\n\n")

            stats = self.get_statistics()
            f.write(f"原始数据点数: {stats['original_count']}\n")
            f.write(f"检测到的异常点: {stats['anomaly_count']}\n")
            f.write(f"过滤后数据点数: {stats['filtered_count']}\n")
            f.write(f"过滤率: {stats['filter_rate']:.2f}%\n\n")

            if self.anomalies:
                f.write("异常点详情:\n")
                f.write("-" * 80 + "\n")
                for i, anomaly in enumerate(self.anomalies):
                    f.write(
                        f"\n{i + 1}. [索引{anomaly['index']}] 类型: {anomaly['type']}\n"
                    )
                    f.write(f"   时间: {anomaly['data']['x']}\n")
                    f.write(f"   位置: {anomaly['data']['position']}\n")
                    f.write(f"   原因: {anomaly['reason']}\n")
                    if "change1" in anomaly:
                        f.write(
                            f"   变化: {anomaly['change1']:+d} → {anomaly['change2']:+d}\n"
                        )
                    if "deviation" in anomaly:
                        f.write(
                            f"   偏离: {anomaly['deviation']:.1f} (周围平均: {anomaly['avg_pos']:.1f})\n"
                        )

            f.write("\n" + "=" * 80 + "\n")
            f.write("过滤后的数据:\n")
            f.write("-" * 80 + "\n")
            for item in self.filtered_data:
                f.write(f"{item['x']}, {item['position']}, {item['direction']}\n")

        print(f"\n过滤报告已保存到: {output_path}")


def main():
    """主函数 - 测试异常点过滤"""
    # 数据文件路径
    scatter_json_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "dashboard",
        "public",
        "data",
        "scatter.json",
    )

    # 输出文件路径
    report_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output",
        "anomaly_filter_report.txt",
    )

    # 加载数据
    print("=" * 60)
    print("异常点过滤测试")
    print("=" * 60)
    print(f"\n正在加载数据: {scatter_json_path}")

    with open(scatter_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    position_data = data.get("position_data", [])
    print(f"加载完成: {len(position_data)}条位置数据\n")

    # 创建过滤器
    filter_obj = AnomalyFilter(position_data)

    # 检测异常点
    print("-" * 60)
    print("步骤1: 检测异常点")
    print("-" * 60)
    anomalies = filter_obj.detect_anomalies()

    print(f"检测到 {len(anomalies)} 个异常点\n")

    # 显示前10个异常点
    if anomalies:
        print("异常点详情（前10个）:")
        for i, anomaly in enumerate(anomalies[:10]):
            print(f"\n{i + 1}. 时间: {anomaly['data']['x']}")
            print(f"   位置: {anomaly['data']['position']}")
            print(f"   类型: {anomaly['type']}")
            print(f"   原因: {anomaly['reason']}")

    # 过滤异常点
    print("\n" + "-" * 60)
    print("步骤2: 过滤异常点")
    print("-" * 60)
    filtered_data = filter_obj.filter_anomalies(keep_anomalies=False)

    stats = filter_obj.get_statistics()
    print(f"原始数据: {stats['original_count']}条")
    print(f"异常点: {stats['anomaly_count']}条")
    print(f"过滤后: {stats['filtered_count']}条")
    print(f"过滤率: {stats['filter_rate']:.2f}%")

    # 保存报告
    filter_obj.save_filtered_data(report_path)

    print("\n" + "=" * 60)
    print("过滤完成!")
    print("=" * 60)

    return filter_obj


if __name__ == "__main__":
    main()
