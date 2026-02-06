#!/usr/bin/env python3
"""
煤机轨迹分析器
识别采煤机轨迹中的折返点，分析关键位置，并将轨迹分割成切割周期
"""

from datetime import datetime
from typing import Dict, List, Optional


class TrajectoryAnalyzer:
    """
    轨迹分析器类
    分析采煤机位置数据，识别切割周期和关键位置
    """

    def __init__(self, position_data: List[dict]):
        """
        初始化轨迹分析器

        Args:
            position_data: 位置数据列表，每个元素包含:
                {
                    'x': time_str (e.g., "2025-09-04 14:00:00"),
                    'y': position (int),
                    'position': position (int),
                    'direction': "ShearerDir.Up/Down/Stop"
                }
        """
        self.position_data = position_data
        self.trajectory_segments = []

    def analyze(self) -> dict:
        """
        执行完整的轨迹分析

        Returns:
            {
                'segments': List[segment_dict],  # 切割周期列表
                'key_positions': {...},          # 关键位置信息
                'total_cycles': int,             # 总周期数
                'export_time': ISO timestamp     # 导出时间
            }
        """
        print("开始轨迹分析...")

        # Step 1: 找到所有折返点（方向变化点）
        turning_points = self._find_turning_points()
        print(f"✓ 识别到 {len(turning_points)} 个折返点")

        # Step 2: 计算关键位置
        key_positions = self._calculate_key_positions(turning_points)
        print(
            f"✓ 关键位置: 机头[{key_positions['min_head']}-{key_positions['max_head_cleaning']}], "
            f"机尾[{key_positions['min_tail_cleaning']}-{key_positions['max_tail']}]"
        )

        # Step 3: 识别三角煤点
        triangular_points = self._identify_triangular_coal(key_positions)
        if triangular_points.get("head_triangular_point"):
            print(f"✓ 机头三角煤折返点: {triangular_points['head_triangular_point']}")
        if triangular_points.get("tail_triangular_point"):
            print(f"✓ 机尾三角煤折返点: {triangular_points['tail_triangular_point']}")

        # Step 4: 将轨迹分割成周期
        segments = self._split_into_cycles(
            turning_points, key_positions, triangular_points
        )
        print(f"✓ 识别到 {len(segments)} 个切割周期")

        return {
            "segments": segments,
            "key_positions": {**key_positions, **triangular_points},
            "total_cycles": len(segments),
            "export_time": datetime.now().isoformat(),
        }

    def _find_turning_points(self) -> List[dict]:
        """
        识别方向变化的折返点

        算法:
        1. 按时间遍历位置数据
        2. 跳过 direction="Stop" 的记录
        3. 当方向发生变化时，记录折返点

        Returns:
            折返点列表: [
                {
                    'position': int,
                    'time': str,
                    'direction': str,
                    'index': int
                },
                ...
            ]
        """
        turning_points = []
        prev_direction = None

        for i, point in enumerate(self.position_data):
            curr_direction = point.get("direction", "")

            # 跳过 Stop 记录
            if "Stop" in curr_direction:
                continue

            # 检查方向是否发生变化
            if prev_direction and curr_direction != prev_direction:
                turning_points.append(
                    {
                        "position": point["position"],
                        "time": point["x"],
                        "direction": curr_direction,
                        "index": i,
                    }
                )

            prev_direction = curr_direction

        return turning_points

    def _calculate_key_positions(self, turning_points: List[dict]) -> dict:
        """
        从折返点计算关键位置

        算法:
        1. 如果没有折返点，从所有数据中计算
        2. 找到最小和最大位置
        3. 计算机头位置: position <= min_position + 10
           - min_head = min(head_positions)
           - max_head_cleaning = max(head_positions)
        4. 计算机尾位置: position >= max_position - 10
           - max_tail = max(tail_positions)
           - min_tail_cleaning = min(tail_positions)

        Args:
            turning_points: 折返点列表

        Returns:
            {
                'min_head': int,
                'max_head_cleaning': int,
                'max_tail': int,
                'min_tail_cleaning': int
            }
        """
        if turning_points:
            all_positions = [tp["position"] for tp in turning_points]
        else:
            # 如果没有折返点，从所有位置数据中计算
            all_positions = [p["position"] for p in self.position_data]

        if not all_positions:
            return {
                "min_head": 0,
                "max_head_cleaning": 0,
                "max_tail": 0,
                "min_tail_cleaning": 0,
            }

        min_pos = min(all_positions)
        max_pos = max(all_positions)

        # 找到机头位置（接近最小值的位置）
        head_positions = [p for p in all_positions if p <= min_pos + 10]
        min_head = min(head_positions) if head_positions else min_pos
        max_head_cleaning = max(head_positions) if head_positions else min_pos

        # 找到机尾位置（接近最大值的位置）
        tail_positions = [p for p in all_positions if p >= max_pos - 10]
        max_tail = max(tail_positions) if tail_positions else max_pos
        min_tail_cleaning = min(tail_positions) if tail_positions else max_pos

        return {
            "min_head": min_head,
            "max_head_cleaning": max_head_cleaning,
            "max_tail": max_tail,
            "min_tail_cleaning": min_tail_cleaning,
        }

    def _identify_triangular_coal(self, key_positions: dict) -> dict:
        """
        识别三角煤切割点

        算法:
        1. 机头三角煤: position > min_head + 10
           - 找到离开机头后的第一个出现点
        2. 机尾三角煤: position < max_tail - 10
           - 找到到达机尾前的最后一个出现点

        Args:
            key_positions: 关键位置字典

        Returns:
            {
                'head_triangular_point': int | None,
                'tail_triangular_point': int | None
            }
        """
        min_head = key_positions["min_head"]
        max_tail = key_positions["max_tail"]

        head_triangular_point = None
        tail_triangular_point = None

        # 查找机头三角煤点
        head_threshold = min_head + 10
        for point in self.position_data:
            pos = point["position"]
            if pos > head_threshold:
                head_triangular_point = pos
                break

        # 查找机尾三角煤点（反向查找）
        tail_threshold = max_tail - 10
        for point in reversed(self.position_data):
            pos = point["position"]
            if pos < tail_threshold:
                tail_triangular_point = pos
                break

        return {
            "head_triangular_point": head_triangular_point,
            "tail_triangular_point": tail_triangular_point,
        }

    def _split_into_cycles(
        self, turning_points: List[dict], key_positions: dict, triangular_points: dict
    ) -> List[dict]:
        """
        使用状态机将轨迹分割成切割周期

        状态定义:
        - AT_HEAD: 在机头位置 (position <= max_head_cleaning)
        - MOVING_TO_TAIL: 向机尾移动 (direction=Up, position increasing)
        - AT_TAIL: 在机尾位置 (position >= min_tail_cleaning)
        - MOVING_TO_HEAD: 向机头移动 (direction=Down, position decreasing)
        - CYCLE_COMPLETE: 完成一个完整周期

        状态转换:
        1. AT_HEAD → MOVING_TO_TAIL: position > max_head_cleaning AND direction=Up
        2. MOVING_TO_TAIL → AT_TAIL: position >= min_tail_cleaning
        3. MOVING_TO_TAIL → MOVING_TO_HEAD: direction changes to Down (未到达机尾)
        4. AT_TAIL → MOVING_TO_HEAD: direction=Down
        5. MOVING_TO_HEAD → CYCLE_COMPLETE: position <= max_head_cleaning AND direction=Down
        6. CYCLE_COMPLETE → MOVING_TO_TAIL: position > max_head_cleaning AND direction=Up (下一周期)
        7. CYCLE_COMPLETE → AT_HEAD: position <= max_head_cleaning

        Args:
            turning_points: 折返点列表
            key_positions: 关键位置字典
            triangular_points: 三角煤点字典

        Returns:
            周期列表: [
                {
                    'cycle_id': int,
                    'start': {'position': int, 'time': str, 'direction': str},
                    'end': {'position': int, 'time': str, 'direction': str},
                    'has_head_triangular_coal': bool,
                    'has_tail_triangular_coal': bool
                },
                ...
            ]
        """
        segments = []
        current_cycle = None
        state = "AT_HEAD"

        max_head_cleaning = key_positions["max_head_cleaning"]
        min_tail_cleaning = key_positions["min_tail_cleaning"]

        for i, point in enumerate(self.position_data):
            pos = point["position"]
            direction = point.get("direction", "")
            time_str = point["x"]

            # 跳过 Stop 记录
            if "Stop" in direction:
                continue

            # 状态机转换
            if state == "AT_HEAD":
                # 检查是否开始新周期
                if pos > max_head_cleaning and ("Up" in direction or "up" in direction):
                    current_cycle = {
                        "cycle_id": len(segments) + 1,
                        "start": {
                            "position": pos,
                            "time": time_str,
                            "direction": direction,
                        },
                        "end": None,
                    }
                    state = "MOVING_TO_TAIL"

            elif state == "MOVING_TO_TAIL":
                # 检查是否到达机尾
                if pos >= min_tail_cleaning:
                    state = "AT_TAIL"
                # 检查是否反向（未到达机尾就开始返回）
                elif "Down" in direction or "down" in direction:
                    state = "MOVING_TO_HEAD"

            elif state == "AT_TAIL":
                # 从机尾开始返回
                if "Down" in direction or "down" in direction:
                    state = "MOVING_TO_HEAD"

            elif state == "MOVING_TO_HEAD":
                # 检查是否回到机头
                if pos <= max_head_cleaning:
                    # 周期完成
                    if current_cycle is not None:
                        current_cycle["end"] = {
                            "position": pos,
                            "time": time_str,
                            "direction": direction,
                        }

                        # 检查三角煤
                        current_cycle["has_head_triangular_coal"] = (
                            triangular_points.get("head_triangular_point") is not None
                        )
                        current_cycle["has_tail_triangular_coal"] = (
                            triangular_points.get("tail_triangular_point") is not None
                        )

                        segments.append(current_cycle)
                    current_cycle = None
                    state = "CYCLE_COMPLETE"
                # 检查是否再次反向（未到达机头就又开始向机尾移动）
                elif "Up" in direction or "up" in direction:
                    state = "MOVING_TO_TAIL"

            elif state == "CYCLE_COMPLETE":
                # 检查是否开始新周期
                if pos > max_head_cleaning and ("Up" in direction or "up" in direction):
                    current_cycle = {
                        "cycle_id": len(segments) + 1,
                        "start": {
                            "position": pos,
                            "time": time_str,
                            "direction": direction,
                        },
                        "end": None,
                    }
                    state = "MOVING_TO_TAIL"
                else:
                    state = "AT_HEAD"

        # 处理未完成的周期（数据在周期中间结束）
        if current_cycle is not None and current_cycle["end"] is None:
            # 使用最后一个点作为结束点
            last_point = self.position_data[-1]
            current_cycle["end"] = {
                "position": last_point["position"],
                "time": last_point["x"],
                "direction": last_point.get("direction", ""),
            }
            current_cycle["has_head_triangular_coal"] = (
                triangular_points.get("head_triangular_point") is not None
            )
            current_cycle["has_tail_triangular_coal"] = (
                triangular_points.get("tail_triangular_point") is not None
            )
            segments.append(current_cycle)

        return segments


# 测试代码
if __name__ == "__main__":
    # 使用需求文档中的示例数据测试
    test_data = [
        {
            "x": "2026-02-05 10:02:36",
            "y": 4,
            "position": 4,
            "direction": "ShearerDir.Down",
        },
        {
            "x": "2026-02-05 10:02:37",
            "y": 5,
            "position": 5,
            "direction": "ShearerDir.Up",
        },
        {
            "x": "2026-02-05 10:12:36",
            "y": 4,
            "position": 4,
            "direction": "ShearerDir.Down",
        },
        {
            "x": "2026-02-05 11:02:36",
            "y": 156,
            "position": 156,
            "direction": "ShearerDir.Up",
        },
        {
            "x": "2026-02-05 11:12:36",
            "y": 155,
            "position": 155,
            "direction": "ShearerDir.Down",
        },
        {
            "x": "2026-02-05 11:22:36",
            "y": 156,
            "position": 156,
            "direction": "ShearerDir.Up",
        },
        {
            "x": "2026-02-05 11:50:36",
            "y": 132,
            "position": 132,
            "direction": "ShearerDir.Down",
        },
        {
            "x": "2026-02-05 11:55:36",
            "y": 156,
            "position": 156,
            "direction": "ShearerDir.Up",
        },
        {
            "x": "2026-02-05 12:02:36",
            "y": 4,
            "position": 4,
            "direction": "ShearerDir.Down",
        },
    ]

    print("=" * 60)
    print("轨迹分析器测试")
    print("=" * 60)
    print()

    analyzer = TrajectoryAnalyzer(test_data)
    result = analyzer.analyze()

    print()
    print("=" * 60)
    print("分析结果")
    print("=" * 60)
    print(f"关键位置: {result['key_positions']}")
    print(f"总周期数: {result['total_cycles']}")
    print()
    print("周期详情:")
    for segment in result["segments"]:
        print(f"  周期 {segment['cycle_id']}:")
        print(
            f"    起点: 位置 {segment['start']['position']} @ {segment['start']['time']} ({segment['start']['direction']})"
        )
        print(
            f"    终点: 位置 {segment['end']['position']} @ {segment['end']['time']} ({segment['end']['direction']})"
        )
        print(
            f"    三角煤: 机头={segment['has_head_triangular_coal']}, 机尾={segment['has_tail_triangular_coal']}"
        )
