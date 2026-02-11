"""
数据格式转换模块
将txt文本数据转化为结构化的dict对象
支持流式读取，避免内存溢出
"""

from abc import ABC, abstractmethod
from datetime import datetime
from multiprocessing import Value
from pathlib import Path
from socket import SIO_RCVALL
from typing import Dict, Generator, List, Optional, Tuple

from util import SensorTypeID, ShearerDir


class FrameParser(ABC):
    """帧解析器抽象基类，用于扩展不同类型的帧解析"""

    @abstractmethod
    def can_parse(self, frame_type: str, src: int, value: str) -> bool:
        """
        判断是否可以解析该类型的帧

        Args:
            frame_type: 帧类型字符串
            src: 数据源地址

        Returns:
            是否可以解析
        """
        pass

    @abstractmethod
    def parse(
        self, timestamp: datetime, src: int, frame_type: str, value: str, context: Dict
    ) -> Optional[Tuple]:
        """
        解析帧数据

        Args:
            timestamp: 时间戳
            src: 数据源地址
            frame_type: 帧类型字符串
            value: 数据值字符串
            context: 上下文信息（用于保存状态，如上一个煤机位置）

        Returns:
            解析后的元组 (timestamp, src, data_dict) 或 None
        """
        pass


class ShearerPositionParser(FrameParser):
    """煤机位置帧解析器"""

    def can_parse(self, frame_type: str, src: int, value: str) -> bool:
        """判断是否为煤机位置帧"""
        return frame_type == "煤机位置"

    def parse(
        self, timestamp: datetime, src: int, frame_type: str, value: str, context: Dict
    ) -> Optional[Tuple]:
        """
        解析煤机位置数据

        示例: 2024-12-28 18:39:10.902,0,煤机位置,5
        """
        try:
            position = int(value)

            # 获取上一个位置用于判断方向
            last_position = context.get("last_shearer_position")

            # 判断方向
            if last_position is None:
                dir = ShearerDir.Stop
            elif position > last_position:
                dir = ShearerDir.Up
            elif position < last_position:
                dir = ShearerDir.Down
            else:
                dir = ShearerDir.Stop

            # 更新上下文
            context["last_shearer_position"] = position

            data = {
                "frame_type": "煤机位置",
                "data": {"dir": dir, "position": position},
            }

            return (timestamp, src, data)

        except (ValueError, TypeError):
            return None


class SensorDataParser(FrameParser):
    """传感器数据帧解析器"""

    # 传感器名称到枚举的映射
    SENSOR_NAME_MAP = {
        "前柱压力": SensorTypeID.前柱压力,
        "后柱压力": SensorTypeID.后柱压力,
        "前溜行程": SensorTypeID.前溜行程,
        "后溜行程": SensorTypeID.后溜行程,
        "尾梁行程": SensorTypeID.尾梁行程,
        "插板行程": SensorTypeID.插板行程,
        "一级护帮行程": SensorTypeID.一级护帮行程,
        "红外": SensorTypeID.红外,
        "顶梁X": SensorTypeID.顶梁X,
        "顶梁Y": SensorTypeID.顶梁Y,
        "高度": SensorTypeID.高度,
        "倾角2X": SensorTypeID.倾角2X,
        "倾角3X": SensorTypeID.倾角3X,
        "倾角4X": SensorTypeID.倾角4X,
        "高度姿态X": SensorTypeID.高度姿态X,
        "高度姿态Y": SensorTypeID.高度姿态Y,
        "高度姿态Z": SensorTypeID.高度姿态Z,
        "激光测距": SensorTypeID.激光测距,
        "伸缩梁行程": SensorTypeID.伸缩梁行程,
    }

    def can_parse(self, frame_type: str, src: int, value: str) -> bool:
        """判断是否为传感器数据帧"""
        # 检查帧类型是否包含传感器名称
        for sensor_name in self.SENSOR_NAME_MAP.keys():
            if (
                (sensor_name in frame_type)
                and (sensor_name == "前溜行程")
                and (float(value) < 2500)
            ):
                return True
        return False

    def parse(
        self, timestamp: datetime, src: int, frame_type: str, value: str, context: Dict
    ) -> Optional[Tuple]:
        """
        解析传感器数据

        示例: 2024-12-28 18:39:11.054,0,支架集.005.前溜行程,900
        """
        try:
            # 从帧类型中提取传感器名称
            sensor_type = None
            for sensor_name, sensor_enum in self.SENSOR_NAME_MAP.items():
                if sensor_name in frame_type:
                    sensor_type = sensor_enum
                    break

            if sensor_type is None:
                return None

            # 解析数值
            parsed_value = float(value)

            data = {
                "frame_type": "传感器数据",
                "data": {"sensor_type": sensor_type, "value": parsed_value},
                "src_no": src,
            }

            return (timestamp, src, data)

        except (ValueError, TypeError):
            return None


class TxtDataConverter:
    """
    txt数据转换器

    将txt文本数据转化为结构化的dict对象，支持流式读取
    """

    def __init__(self, src_txt: str):
        """
        初始化转换器

        Args:
            src_txt: 源文件路径
        """
        self.src_txt = Path(src_txt)
        if not self.src_txt.exists():
            raise FileNotFoundError(f"源文件不存在: {src_txt}")

        # 注册帧解析器
        self._parsers: List[FrameParser] = [
            ShearerPositionParser(),
            SensorDataParser(),
        ]

        # 上下文信息，用于保存解析状态
        self._context = {}

    def register_parser(self, parser: FrameParser):
        """
        注册自定义帧解析器

        Args:
            parser: 帧解析器实例
        """
        self._parsers.append(parser)

    def parse_line(self, line: str) -> Optional[Tuple]:
        """
        解析单行数据

        数据格式: timestamp,quality,frame_type,value
        示例: 2024-11-27 00:00:30.001,131072,支架集.008.前溜行程,827
               2024-11-27 00:01:00.001,131072,煤机位置,7

        筛选规则:
        - 帧类型为"煤机位置"的数据，交给ShearerPositionParser处理
        - 帧类型以"支架集."开头的数据，从中提取src地址，交给SensorDataParser处理

        Args:
            line: 文本行

        Returns:
            解析后的元组 (timestamp, src, data_dict) 或 None
        """
        line = line.strip()
        if not line:
            return None

        try:
            # 解析CSV格式: timestamp,quality,frame_type,value
            parts = line.split(",")
            if len(parts) < 4:
                return None

            # 解析时间戳
            timestamp_str = parts[0]
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")

            # 解析质量（暂不使用）
            # quality = int(parts[1])

            # 解析帧类型
            frame_type = parts[2]

            # 解析值
            value = parts[3]

            # 筛选数据：只处理"煤机位置"和"支架集."开头的数据
            src = None

            if frame_type == "煤机位置":
                # 煤机位置数据，使用ShearerPositionParser处理
                # 对于煤机位置，src从parts[1]获取
                src = 253  # 煤机位置的默认src，可根据需要调整
                for parser in self._parsers:
                    if isinstance(parser, ShearerPositionParser):
                        result = parser.parse(
                            timestamp, src, frame_type, value, self._context
                        )
                        if result:
                            return result

            elif frame_type.startswith("支架集."):
                # 支架集数据，格式: 支架集.008.前溜行程
                # 提取src地址（第2部分，如"008"）
                frame_parts = frame_type.split(".")
                if len(frame_parts) >= 2:
                    try:
                        src = int(frame_parts[1])
                    except ValueError:
                        return None

                    # 重新构造frame_type，去掉src部分，如"支架集.008.前溜行程" -> "前溜行程"
                    sensor_name = frame_parts[2] if len(frame_parts) > 2 else ""

                    # 使用SensorDataParser处理
                    for parser in self._parsers:
                        if isinstance(parser, SensorDataParser):
                            if parser.can_parse(sensor_name, src, value):
                                result = parser.parse(
                                    timestamp, src, sensor_name, value, self._context
                                )
                                if result:
                                    return result

            # 不符合筛选条件的数据，返回None（过滤掉）
            return None

        except (ValueError, IndexError) as e:
            print(f"解析行失败: {line}, 错误: {e}")
            return None

    def parse_batch(self, batch_size: int = 1000) -> Generator[List[Tuple], None, None]:
        """
        分批解析文件，使用yield生成器返回数据

        Args:
            batch_size: 每批读取的行数

        Yields:
            每批解析结果列表 List[Tuple]，每个元素为 (timestamp, src, data_dict)
        """
        batch = []

        with open(self.src_txt, "r", encoding="utf-8") as f:
            for line in f:
                result = self.parse_line(line)
                if result:
                    batch.append(result)

                    # 达到批次大小后返回
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

            # 返回剩余的数据
            if batch:
                yield batch

    def parse_all(self) -> List[Tuple]:
        """
        解析整个文件（谨慎使用，大文件可能导致内存溢出）

        Returns:
            所有解析结果列表 List[Tuple]
        """
        results = []
        for batch in self.parse_batch(batch_size=10000):
            results.extend(batch)
        return results

    @property
    def context(self) -> Dict:
        """获取上下文信息"""
        return self._context

    def reset_context(self):
        """重置上下文信息"""
        self._context = {}
