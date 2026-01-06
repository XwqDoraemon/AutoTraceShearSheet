"""
FramePacket 数据帧包类
根据C# FramePacket实现转换为Python版本
"""

import struct
from typing import List, Optional


class FramePacket:
    """数据帧包"""

    def __init__(self, buffer: Optional[bytes] = None, is_nc_mode: bool = False):
        """
        初始化数据帧包

        Args:
            buffer: 原始字节数据
            is_nc_mode: 是否属于网络型私有协议
        """
        self._is_nc_mode = is_nc_mode

        if buffer is None:
            # 空包初始化
            self._buffer = bytearray(24)  # 固定长度24字节
            self._datas = bytearray(8)
        else:
            # 从buffer复制数据
            self._buffer = bytearray(buffer)
            # 扩展buffer长度到至少24字节
            if len(self._buffer) < 24:
                self._buffer.extend(b"\x00" * (24 - len(self._buffer)))
            self._datas = bytearray(8)

            # 如果是标准帧，转换为扩展帧
            if not self._is_nc_mode and self.b_is_extend == 0:
                uid = self.ul_dword
                uid <<= 18
                self.ul_dword = uid & 0x3FFFFFFFF
                self.b_is_extend = 1

    @property
    def ul_dword(self) -> int:
        """获取/设置 uID (4字节)"""
        return struct.unpack(">I", self._buffer[0:4])[0]

    @ul_dword.setter
    def ul_dword(self, value: int):
        """设置 uID"""
        self._buffer[0:4] = struct.pack(">I", value & 0xFFFFFFFF)

    @property
    def b_src(self) -> int:
        """源地址 (低8位)"""
        return self.ul_dword & 0xFF

    @b_src.setter
    def b_src(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFFFFFF00) | (value & 0xFF)

    @property
    def b_src_hbit(self) -> int:
        """源地址高位"""
        return (self.ul_dword >> 16) & 0x01

    @b_src_hbit.setter
    def b_src_hbit(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFFFEFFFF) | ((value & 0x01) << 16)

    @property
    def src_no(self) -> int:
        """开始架号 (源地址完整值)"""
        return (self.b_src_hbit << 8) | self.b_src

    @src_no.setter
    def src_no(self, value: int):
        self.ul_dword = (
            (self.ul_dword & 0xFFFEFF00) | ((value & 0x100) << 8) | (value & 0xFF)
        )

    @property
    def b_dst(self) -> int:
        """目的地址 (低8位)"""
        return (self.ul_dword >> 8) & 0xFF

    @b_dst.setter
    def b_dst(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFFFF00FF) | ((value & 0xFF) << 8)

    @property
    def b_dst_hbit(self) -> int:
        """目的地址高位"""
        return (self.ul_dword >> 17) & 0x01

    @b_dst_hbit.setter
    def b_dst_hbit(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFFFDFFFF) | ((value & 0x01) << 17)

    @property
    def dst_no(self) -> int:
        """结束架号 (目的地址完整值)"""
        return (self.b_dst_hbit << 8) | self.b_dst

    @dst_no.setter
    def dst_no(self, value: int):
        self.ul_dword = (
            (self.ul_dword & 0xFFFD00FF)
            | ((value & 0x100) << 9)
            | ((value & 0xFF) << 8)
        )

    @property
    def b_pri(self) -> int:
        """优先级位域"""
        return (self.ul_dword >> 26) & 0x07

    @b_pri.setter
    def b_pri(self, value: int):
        self.ul_dword = (self.ul_dword & 0xF3FFFFFF) | ((value & 0x07) << 26)

    @property
    def b_cmd(self) -> int:
        """命令码位域"""
        return (self.ul_dword >> 22) & 0x0F

    @b_cmd.setter
    def b_cmd(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFC3FFFFF) | ((value & 0x0F) << 22)

    @property
    def b_ack(self) -> int:
        """响应码位域"""
        return (self.ul_dword >> 20) & 0x03

    @b_ack.setter
    def b_ack(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFFCFFFFF) | ((value & 0x03) << 20)

    @property
    def b_dir(self) -> int:
        """方向码位域"""
        return (self.ul_dword >> 18) & 0x03

    @b_dir.setter
    def b_dir(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFFF3FFFF) | ((value & 0x03) << 18)

    @property
    def b_rsv(self) -> int:
        """保留位域"""
        return (self.ul_dword >> 16) & 0x03

    @b_rsv.setter
    def b_rsv(self, value: int):
        self.ul_dword = (self.ul_dword & 0xFFFCFFFF) | ((value & 0x03) << 16)

    @property
    def b_is_extend(self) -> int:
        """是否扩展帧"""
        return (self.ul_dword >> 30) & 0x01

    @b_is_extend.setter
    def b_is_extend(self, value: int):
        self.ul_dword = (self.ul_dword & 0xBFFFFFFF) | ((value & 0x01) << 30)

    @property
    def b_is_remote(self) -> int:
        """是否远程帧"""
        return (self.ul_dword >> 29) & 0x01

    @b_is_remote.setter
    def b_is_remote(self, value: int):
        self.ul_dword = (self.ul_dword & 0xDFFFFFFF) | ((value & 0x01) << 29)

    @property
    def uc_data_len(self) -> int:
        """数据长度"""
        if len(self._buffer) > 4:
            return self._buffer[4]
        return 0

    @uc_data_len.setter
    def uc_data_len(self, value: int):
        if len(self._buffer) > 4:
            self._buffer[4] = value & 0xFF

    @property
    def uc_tmcan_type(self) -> int:
        """模拟TMCAN帧类型"""
        if self._is_nc_mode and len(self._buffer) > 5:
            return self._buffer[5]
        return 0

    @uc_tmcan_type.setter
    def uc_tmcan_type(self, value: int):
        if self._is_nc_mode and len(self._buffer) > 5:
            self._buffer[5] = value & 0xFF

    @property
    def datas(self) -> bytes:
        """数据区"""
        offset = 6 if self._is_nc_mode else 5
        data_len = self.uc_data_len
        return bytes(self._buffer[offset : offset + data_len])

    @datas.setter
    def datas(self, value: bytes):
        offset = 6 if self._is_nc_mode else 5
        for i, b in enumerate(value):
            if offset + i < len(self._buffer):
                self._buffer[offset + i] = b
        self.uc_data_len = len(value)

    @property
    def enable_broadcast(self) -> bool:
        """使能广播"""
        return getattr(self, "_enable_broadcast", False)

    @enable_broadcast.setter
    def enable_broadcast(self, value: bool):
        self._enable_broadcast = value

    @property
    def buffer(self) -> bytes:
        """获取完整的buffer"""
        return bytes(self._buffer[: self.uc_data_len + (6 if self._is_nc_mode else 5)])

    @property
    def data_string(self) -> str:
        """数据（十六进制字符串）"""
        return "-".join(f"{b:02X}" for b in self.datas)

    @property
    def buffer_string(self) -> str:
        """报文（十六进制字符串）"""
        return "-".join(f"{b:02X}" for b in self.buffer)

    @classmethod
    def create_network_control_packet(
        cls,
        cmd: int,
        pri: int,
        ack: int,
        src_no: int,
        dst_no: int,
        data: List[int],
        src_ext: int = 0,
        dst_ext: int = 0,
    ) -> "FramePacket":
        """
        创建网络型远程控制数据包

        Args:
            cmd: 命令码
            pri: 优先级
            ack: 应答位
            src_no: 源地址
            dst_no: 目的地址
            data: 数据列表
            src_ext: 源地址扩展（bit 16）
            dst_ext: 目的地址扩展（bit 17）

        Returns:
            FramePacket实例
        """
        packet = cls(is_nc_mode=True)
        packet.b_pri = pri
        packet.src_no = src_no
        packet.b_ack = ack
        packet.dst_no = dst_no
        packet.b_cmd = cmd
        packet.uc_data_len = len(data)
        packet.b_is_extend = 1
        packet.datas = bytes(data)
        # 设置扩展地址：bit 17 = 目的地址扩展, bit 16 = 源地址扩展
        packet.b_rsv = (dst_ext & 0x01) << 1 | (src_ext & 0x01)
        return packet

    def __str__(self) -> str:
        """字符串形式"""
        if self._is_nc_mode:
            return (
                f"源地址={self.src_no:03d}, "
                f"目的地址={self.dst_no:03d}, "
                f"优先级={self.b_pri:01d}, "
                f"命令码={self.b_cmd:02d}, "
                f"应答位={self.b_ack:01d}, "
                f"方向={self.b_dir:01d}, "
                f"长度={self.uc_data_len:01d}, "
                f"数据={self.data_string}"
            )
        else:
            return (
                f"源地址={self.b_src:03d}, "
                f"目的地址={self.b_dst:03d}, "
                f"优先级={self.b_pri:01d}, "
                f"命令码={self.b_cmd:02d}, "
                f"应答位={self.b_ack:01d}, "
                f"方向={self.b_dir:01d}, "
                f"长度={self.uc_data_len:01d}, "
                f"数据={self.data_string}"
            )
