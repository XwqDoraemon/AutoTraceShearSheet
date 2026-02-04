from enum import Enum


class ActionType(Enum):
    """动作类型枚举"""

    无动作 = 0
    单动动作 = 1
    单架自动 = 6
    成组动作 = 2
    跟机动作 = 3
    点动动作 = 4
    自动补压 = 5
    其它动作 = 7
    自动动作执行状态 = 64
    自动动作调度信息 = 65


class AutoActionCode(Enum):
    """自动动作编码枚举"""

    无动作 = 0
    MOMVE_PUSH_FAST = 6  # 成组快速ASQ
    FOLLOW_PUSH_FAST = 9  # 跟机快速ASQ
    FOLLOW_PUSH_SEQUENCE = 11  # 跟机顺序ASQ
    FOLLOW_PUSH_SEQ_FAST = 12  # 跟机顺序快速ASQ


class ActionInfoType(Enum):
    """动作信息类型枚举"""

    单动缺省 = 0
    主机远控单动 = 1
    就地单动 = 2
    主机远控成组 = 17
    就地成组 = 18
    主机下发跟机 = 33
    自动跟机 = 34
    成组动作码 = 48
    跟机动作码 = 64
    点动动作码 = 80
    补压动作码 = 81
    单架自动动作码 = 96


class RemoteControlTypeEnum(Enum):
    """远程控制类型枚举"""

    NoneA = 0  # 无动作类别
    Local = 1  # 工人按键盘控制的动作
    Host = 2  # 主机上远控操作动作
    Auto = 3  # 跟机自动化的动作
