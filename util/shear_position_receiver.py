"""
煤机位置接收器模块
根据C# ShearPositionReceiver.cs转换的Python版本
处理煤机位置数据包解析和处理
"""

from enum import Enum
from typing import Dict

from .frame_packet import FramePacket


class ShearerDir(Enum):
    """煤机方向枚举"""

    Stop = 0  # 停止
    Down = 1  # 下行
    Up = 2  # 上行


class ShearPositionReceiver:
    """煤机位置接收器类 - 处理煤机位置数据的接收和解析"""

    def __init__(self):
        self.is_use_shearer_pos = True
        self.is_use_hight_bit = False

        # 跟机方向字典
        self.dir_value_pairs: Dict[int, ShearerDir] = {
            0: ShearerDir.Stop,
            1: ShearerDir.Down,
            2: ShearerDir.Up,
        }

    def process_packet(self, packet: FramePacket) -> dict:
        """
        处理煤机位置报文

        Args:
            packet: FramePacket数据包
            support_system: 支架系统对象

        Returns:
            解析结果字典
        """
        # 判断入口条件
        if not (packet.b_cmd == 0 and packet.b_pri == 3):
            return {}

        if self.is_use_shearer_pos:
            # 解析煤机位置
            if self.is_use_hight_bit:
                position = packet.datas[0] | ((packet.datas[3] & 0x01) << 8)
            else:
                position = packet.datas[0]

            # 返回结果
            direction = self._get_direction(packet)

            return {
                "frame_type": "煤机位置",
                "data": {"dir": direction, "position": position},
            }

        return {}

    def _get_direction(self, packet: FramePacket) -> ShearerDir:
        """获取方向枚举"""
        dir_val = packet.datas[1]
        if dir_val in self.dir_value_pairs:
            return self.dir_value_pairs[dir_val]
        return ShearerDir.Stop


# 为了方便使用，提供一个简单的使用示例
if __name__ == "__main__":
    # 示例：创建接收器并处理数据包
    receiver = ShearPositionReceiver()

    # 示例数据包
    # packet = FramePacket(b'\x00\x01\x02\x00\x03...')
    # result = receiver.process_packet(packet)
    # print(result)
    pass
