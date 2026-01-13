#!/usr/bin/env python3
"""
电液控数据处理程序 (增强版)
根据需求提取和可视化人为操作信息，支持中文显示
"""

import os
import sqlite3
import sys
from datetime import datetime
from typing import List, Tuple

from util import FramePacket
from util.action_receiver import ActionReceiver
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
                frame = FramePacket(buffer_data, is_nc_mode=True)

                # 检查条件：b_pri == 3 且 b_cmd == 4 (支架动作)
                if frame.b_pri == 3 and frame.b_cmd == 4:
                    # 使用ActionReceiver解析
                    parsed_result = self.action_receiver.process_packet(frame)
                    if parsed_result:  # 如果解析成功
                        filtered_batch.append((dt, frame.src_no, parsed_result))

                # 检查条件：b_pri == 3 且 b_cmd == 0 (煤机位置)
                elif frame.b_pri == 3 and frame.b_cmd == 0:
                    # 使用ShearPositionReceiver解析
                    parsed_result = self.shear_position_receiver.process_packet(frame)
                    if parsed_result:  # 如果解析成功
                        filtered_batch.append((dt, frame.src_no, parsed_result))

            except Exception:
                # 跳过解析失败的记录
                continue

        return filtered_batch
