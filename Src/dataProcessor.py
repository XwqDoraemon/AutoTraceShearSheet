#!/usr/bin/env python3
"""
电液控数据处理程序 (增强版)
根据需求提取和可视化人为操作信息，支持中文显示
"""

import os
import sqlite3
from ast import Continue
from datetime import datetime
from typing import List, Tuple

# from typing_extensions import ParamSpecArgs
from util import ActionType, FramePacket
from util.action_receiver import ActionReceiver
from util.sensor_receiver import SensorReceiver
from util.shear_position_receiver import ShearPositionReceiver


class DataProcessor:
    """数据处理器"""

    def __init__(self, db_path: str):
        """
        初始化数据处理器

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.batch_size = 10000  # 批处理大小，防止内存溢出

        # 初始化接收器
        self.action_receiver = ActionReceiver()
        self.shear_position_receiver = ShearPositionReceiver()
        self.sen_receiver = SensorReceiver()

        # 用于存储上一帧信息（按 src_no 分组）
        # {src_no: (dt, parsed_result)}
        self.last_frames = {}

        # 用于存储 b_cmd==0 帧的上一帧数据（按 src_no 分组）
        # {src_no: last_datas}
        self.last_shear_datas = {}

    def _is_duplicate_frame(
        self, current_dt: datetime, current_src_no: int, current_data: dict
    ) -> bool:
        """
        判断当前帧是否为重复帧

        规则：5秒内同一个 src_no 的相同数据帧只保留第一次出现的

        Args:
            current_dt: 当前帧时间
            current_src_no: 当前帧源地址
            current_data: 当前帧解析数据

        Returns:
            True 如果是重复帧（与同一 src_no 的前一帧相同且时间差<5秒），False 否则
        """
        # 检查该 src_no 是否有历史记录
        if current_src_no not in self.last_frames:
            return False

        last_dt, last_data = self.last_frames[current_src_no]

        # 判断 data 是否相同
        if current_data == last_data:
            # 计算时间差
            time_diff = (current_dt - last_dt).total_seconds()
            if time_diff < 5.0:  # 5秒以内
                return True

        return False

    def _is_duplicate_shear_data(
        self, current_src_no: int, current_datas: bytes
    ) -> bool:
        """
        判断煤机位置帧数据是否与上一帧相同

        规则：不论时间间隔多久，只要 frame.datas 相同就滤除

        Args:
            current_src_no: 当前帧源地址
            current_datas: 当前帧的数据载荷 (frame.datas)

        Returns:
            True 如果与上一帧数据相同，False 否则
        """
        # 检查该 src_no 是否有历史数据
        if current_src_no not in self.last_shear_datas:
            return False

        last_datas = self.last_shear_datas[current_src_no]

        # 判断 datas 是否相同
        if current_datas == last_datas:
            return True

        return False

    def process_data_in_batches(self) -> List[Tuple[datetime, int, dict]]:
        """
        分批处理数据，提取符合条件的记录

        Returns:
            符合条件的数据列表 [(时间, src_no, 解析结果), ...]
        """
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 获取总记录数
            cursor.execute("SELECT COUNT(*) FROM t_sac_frame")
            total_rows = cursor.fetchone()[0]
            print(f"📊 数据库总记录数: {total_rows:,}")

            filtered_data = []
            processed_count = 0

            # 分批处理数据
            for offset in range(0, total_rows, self.batch_size):
                cursor.execute(
                    """
                    SELECT f_date_time, f_buffer
                    FROM t_sac_frame
                    ORDER BY f_id
                    LIMIT ? OFFSET ?
                """,
                    (self.batch_size, offset),
                )

                batch_data = cursor.fetchall()
                batch_filtered = self._process_batch(batch_data)
                filtered_data.extend(batch_filtered)

                processed_count += len(batch_data)
                progress = processed_count / total_rows * 100
                print(
                    f"🔄 已处理: {processed_count:,}/{total_rows:,} ({progress:.1f}%), "
                    f"符合条件: {len(filtered_data):,}"
                )

            print(f"✅ 处理完成！共找到 {len(filtered_data):,} 条符合条件的记录")
            return filtered_data

        finally:
            conn.close()

    def get_data_for_tsfresh(self) -> List[Tuple[datetime, int, dict]]:
        """
        获取用于 tsfresh 特征提取的数据

        这是 process_data_in_batches() 的别名方法，提供更清晰的语义

        Returns:
            符合条件的数据列表 [(时间, src_no, 解析结果), ...]
        """
        return self.process_data_in_batches()

    def _process_batch(
        self, batch_data: List[Tuple]
    ) -> List[Tuple[datetime, int, dict]]:
        """
        处理单个批次的数据
        Args:
            batch_data: 批次数据

        Returns:
            符合条件的数据列表 [(时间, src_no, 解析结果), ...]
        """
        filtered_batch = []
        for date_time_str, buffer_data in batch_data:
            try:
                # 解析时间
                dt = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S.%f")

                # 使用FramePacket解析buffer
                frame = FramePacket(buffer_data)

                # 检查条件：b_pri == 3 且 b_cmd == 4 (支架动作)
                if frame.b_pri == 3 and frame.b_cmd == 4:
                    # 使用ActionReceiver解析
                    parsed_result = self.action_receiver.process_packet(frame)
                    if parsed_result:  # 如果解析成功
                        # print(parsed_result["frame_type"])
                        if parsed_result["data"]["actionType"] != ActionType.无动作:
                            # 检查是否为重复帧
                            if not self._is_duplicate_frame(
                                dt, frame.src_no, parsed_result["data"]
                            ):
                                filtered_batch.append((dt, frame.src_no, parsed_result))
                                # 更新该 src_no 的最新帧信息
                                self.last_frames[frame.src_no] = (
                                    dt,
                                    parsed_result["data"],
                                )

                # 检查条件：b_pri == 3 且 b_cmd == 0 (煤机位置)
                elif frame.b_pri == 3 and frame.b_cmd == 0:
                    # 检查是否与上一帧数据相同
                    if not self._is_duplicate_shear_data(frame.src_no, frame.datas):
                        # 使用ShearPositionReceiver解析
                        parsed_result = self.shear_position_receiver.process_packet(
                            frame
                        )
                        if parsed_result:  # 如果解析成功
                            filtered_batch.append((dt, frame.src_no, parsed_result))
                            # 更新该 src_no 的最新数据
                            self.last_shear_datas[frame.src_no] = frame.datas
                elif frame.b_pri == 3 and frame.b_cmd == 10:
                    # 检查是否与上一帧数据相同
                    parsed_result = self.sen_receiver.process_packet(frame)
                    # 过滤空数据，只保留有传感器数据的记录
                    if (
                        parsed_result
                        and parsed_result.get("data")
                        and len(parsed_result["data"]) > 0
                    ):
                        print(frame.src_no, parsed_result["data"])
                        for result in parsed_result["data"]:
                            out = parsed_result
                            out["data"] = result
                            # print(out)
                            filtered_batch.append((dt, frame.src_no, out))
            except Exception:
                # 跳过解析失败的记录
                continue

        return filtered_batch
