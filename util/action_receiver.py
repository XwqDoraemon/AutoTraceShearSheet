"""
动作命令解析模块
根据C# ActionReceiver.cs转换的Python版本
处理支架动作数据包解析和处理
"""

import json
from pathlib import Path
from typing import Dict

from .Enums import ActionType
from .frame_packet import FramePacket


class ActionReceiver:
    """动作接收器类 - 处理支架动作数据的接收和解析"""

    def __init__(self):
        self.UNITNO = 508

        # 初始化简单协议字节数组
        self._simple_protocol = bytearray(16)
        self._action_flag = 0

        # 加载枚举映射配置
        self._enum_mapping = self._load_enum_mapping()

    def _load_enum_mapping(self) -> Dict[str, Dict[int, str]]:
        """
        加载并反转枚举定义JSON文件

        Returns:
            包含反转后枚举映射的字典，格式: {"enActionCode": {0: "ACT_NOP", 1: "PROP_UP", ...}, ...}
        """
        json_path = Path(__file__).parent.parent / "Options" / "enum_defs.json"

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                enum_defs = json.load(f)

            # 反转 key-value 映射，从 {name: value} 变为 {value: name}
            enum_mapping = {}
            for enum_name, enum_dict in enum_defs.items():
                enum_mapping[enum_name] = {v: k for k, v in enum_dict.items()}

            return enum_mapping

        except FileNotFoundError:
            print(f"警告: 未找到枚举定义文件 {json_path}")
            return {}
        except json.JSONDecodeError as e:
            print(f"警告: 枚举定义文件格式错误: {e}")
            return {}

    def process_packet(
        self,
        packet: FramePacket,
        tri_self_transfer=None,
    ) -> dict:
        """
        处理数据包

        Args:
            packet: FramePacket数据包

        Returns:
            解析结果字典
        """
        if packet.b_cmd != 4 or packet.b_pri != 3:
            return {}

        self._simple_protocol = bytearray(16)
        return self._process_nc_protocol(packet, tri_self_transfer)

    def _process_nc_protocol(self, packet: FramePacket, tri_self_transfer) -> dict:
        """处理新架构私有协议"""
        if packet.datas is None or len(packet.datas) < 2:
            return {}

        protocol_type = ActionType(packet.datas[0])

        if protocol_type == ActionType.无动作:
            return self._process_nc_protocol0(packet, tri_self_transfer)
        else:
            return self._process_nc_protocol17(packet, tri_self_transfer)
        return {}

    def _process_nc_protocol0(self, packet: FramePacket, tri_self_transfer) -> dict:
        """处理无动作协议"""

        return {
            "frame_type": "支架动作",
            "data": {"actionCodes": [], "actionType": ActionType.无动作},
        }

    def _process_nc_protocol17(self, packet: FramePacket, tri_self_transfer) -> dict:
        """处理类型1-7编码"""
        protocol_type = ActionType(packet.datas[0])

        # 获取动作代码
        codes = []

        auto_action_id = "无"
        # 简单协议处理
        # trigger_no = (packet.datas[6] & 0x01) << 8 | packet.datas[7]
        mapDict = self._enum_mapping.get("enActionCode", {})
        # 根据协议类型选择映射字典
        if (
            protocol_type != ActionType.单动动作
            and protocol_type != ActionType.自动动作执行状态
            and protocol_type != ActionType.自动动作调度信息
        ):
            auto_action_id = self._enum_mapping.get("enAutoActCode", {}).get(
                packet.datas[6]
            )
        elif protocol_type == ActionType.自动动作执行状态:
            auto_action_id = self._enum_mapping.get("enAutoActCode", {}).get(
                packet.datas[2]
            )
        elif protocol_type == ActionType.自动动作调度信息:
            auto_action_id = self._enum_mapping.get("enAutoActCode", {}).get(
                packet.datas[1]
            )
        if (
            protocol_type != ActionType.自动动作调度信息
            and protocol_type != ActionType.自动动作执行状态
        ):
            for i in range(5):
                if packet.datas[i + 1] != 0:
                    codes.append(
                        mapDict.get(
                            packet.datas[i + 1], f"UNKNOWN_{packet.datas[i + 1]}"
                        )
                    )
        return {
            "frame_type": "支架动作",
            "data": {
                "actionType": protocol_type,
                "actionCodes": codes,
                "AutoActId": auto_action_id,
            },
        }
