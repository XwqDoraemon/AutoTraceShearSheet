<template>
  <div class="scatter-chart">
    <el-card>
      <template #header>
        <div class="header">
          <span>电液控数据散点图分析</span>
          <el-button type="primary" @click="handleRefresh">刷新数据</el-button>
        </div>
      </template>

      <!-- 动作筛选标签 -->
      <el-row :gutter="10" style="margin-bottom: 20px">
        <el-col :span="24">
          <div class="filter-section">
            <span class="filter-label">动作筛选：</span>
            <el-tag
              v-for="tag in actionCodeTags"
              :key="tag.value"
              :type="tag.checked ? 'primary' : 'info'"
              :effect="tag.checked ? 'dark' : 'plain'"
              @click="toggleActionFilter(tag.value)"
              style="margin-right: 8px; margin-bottom: 8px; cursor: pointer"
            >
              {{ tag.label }}
            </el-tag>
            <el-tag v-if="actionCodeTags.length === 0" type="info">
              暂无动作数据
            </el-tag>
          </div>
        </el-col>
      </el-row>

      <el-row :gutter="20">
        <el-col :span="24">
          <div ref="chartRef" style="height: 600px"></div>
        </el-col>
      </el-row>
    </el-card>

    <!-- 煤机位置分析功能 -->
    <el-card style="margin-top: 20px">
      <template #header>
        <div class="header">
          <span>煤机位置分析</span>
          <el-button type="primary" size="small" @click="toggleAnalysisPanel">
            {{ showAnalysisPanel ? "收起" : "展开" }}
          </el-button>
        </div>
      </template>

      <el-collapse-transition>
        <div v-show="showAnalysisPanel">
          <el-alert
            title="分析说明"
            type="info"
            :closable="false"
            style="margin-bottom: 20px"
          >
            <template #default>
              选择起始位置和结束位置及对应的时间点，系统将分析这两个时间点之间的支架动作数据。
              例如：起始位置25 @ 2025-09-04 14:00:55，结束位置23 @ 2025-09-04
              14:25:30，分析 2025-09-04 14:00:55 ~ 2025-09-04 14:25:30
              之间的动作。
            </template>
          </el-alert>

          <!-- 筛选条件区域 -->
          <div class="filter-section">
            <el-row :gutter="20">
              <!-- 煤机位置1 - 起始位置 -->
              <el-col :span="12">
                <el-card shadow="hover" class="filter-card">
                  <template #header>
                    <span class="card-title">📍 起始位置</span>
                  </template>
                  <el-form label-width="80px">
                    <el-form-item label="位置值">
                      <el-select
                        v-model="position1.selectedPosition"
                        placeholder="请选择位置"
                        filterable
                        @change="handlePosition1Change"
                        style="width: 100%"
                      >
                        <el-option
                          v-for="pos in uniquePositions"
                          :key="pos"
                          :label="pos"
                          :value="pos"
                        />
                      </el-select>
                    </el-form-item>
                    <el-form-item label="时间点">
                      <el-select
                        v-model="position1.selectedTime"
                        placeholder="请选择时间点"
                        filterable
                        :disabled="!position1.selectedPosition"
                        @change="handleTime1Change"
                        style="width: 100%"
                      >
                        <el-option
                          v-for="time in position1.availableTimes"
                          :key="time"
                          :label="time"
                          :value="time"
                        />
                      </el-select>
                    </el-form-item>
                    <el-form-item>
                      <el-tag
                        type="success"
                        v-if="
                          position1.selectedPosition !== null &&
                          position1.selectedTime
                        "
                      >
                        位置 {{ position1.selectedPosition }} @
                        {{ position1.selectedTime }}
                      </el-tag>
                      <el-tag type="info" v-else>请选择位置和时间点</el-tag>
                    </el-form-item>
                  </el-form>
                </el-card>
              </el-col>

              <!-- 煤机位置2 - 结束位置 -->
              <el-col :span="12">
                <el-card shadow="hover" class="filter-card">
                  <template #header>
                    <span class="card-title">📍 结束位置</span>
                  </template>
                  <el-form label-width="80px">
                    <el-form-item label="位置值">
                      <el-select
                        v-model="position2.selectedPosition"
                        placeholder="请选择位置"
                        filterable
                        @change="handlePosition2Change"
                        style="width: 100%"
                      >
                        <el-option
                          v-for="pos in uniquePositions"
                          :key="pos"
                          :label="pos"
                          :value="pos"
                        />
                      </el-select>
                    </el-form-item>
                    <el-form-item label="时间点">
                      <el-select
                        v-model="position2.selectedTime"
                        placeholder="请选择时间点"
                        filterable
                        :disabled="!position2.selectedPosition"
                        @change="handleTime2Change"
                        style="width: 100%"
                      >
                        <el-option
                          v-for="time in position2.availableTimes"
                          :key="time"
                          :label="time"
                          :value="time"
                        />
                      </el-select>
                    </el-form-item>
                    <el-form-item>
                      <el-tag
                        type="success"
                        v-if="
                          position2.selectedPosition !== null &&
                          position2.selectedTime
                        "
                      >
                        位置 {{ position2.selectedPosition }} @
                        {{ position2.selectedTime }}
                      </el-tag>
                      <el-tag type="info" v-else>请选择位置和时间点</el-tag>
                    </el-form-item>
                  </el-form>
                </el-card>
              </el-col>
            </el-row>

            <!-- 分析按钮 -->
            <el-row style="margin-top: 20px; text-align: center">
              <el-col :span="24">
                <el-button
                  type="primary"
                  size="large"
                  @click="analyzeData"
                  :disabled="!canAnalyze"
                >
                  开始分析
                </el-button>
              </el-col>
            </el-row>
          </div>

          <!-- 结果展示区域 -->
          <el-divider v-if="analysisResult.data.length > 0" />

          <!-- 统计信息 -->
          <div v-if="showAnalysisResult" class="result-section">
            <el-card shadow="hover" style="margin-bottom: 20px">
              <template #header>
                <span class="card-title">📊 分析统计</span>
              </template>
              <el-descriptions :column="2" border>
                <el-descriptions-item label="起始位置">
                  位置 {{ position1.selectedPosition }} @
                  {{ position1.selectedTime }}
                </el-descriptions-item>
                <el-descriptions-item label="结束位置">
                  位置 {{ position2.selectedPosition }} @
                  {{ position2.selectedTime }}
                </el-descriptions-item>
                <el-descriptions-item label="时间区间">
                  {{ timeRange.start }} ~ {{ timeRange.end }}
                </el-descriptions-item>
                <el-descriptions-item label="支架动作总数">
                  {{ analysisResult.data.length }}
                </el-descriptions-item>
                <el-descriptions-item label="涉及源地址数">
                  {{ getUniqueSources(analysisResult.data) }}
                </el-descriptions-item>
                <el-descriptions-item label="动作类型数">
                  {{ getActionTypes(analysisResult.data).length }}
                </el-descriptions-item>
              </el-descriptions>
            </el-card>

            <!-- 散点图可视化 -->
            <el-card shadow="hover" style="margin-top: 20px">
              <template #header>
                <span class="card-title">📈 动作散点图</span>
              </template>
              <ActionScatterChart
                v-if="analysisResult.data.length > 0"
                :data="analysisResult.data"
                :positionData="getPositionDataInRange()"
                title="支架动作分布"
                height="500px"
              />
              <el-empty v-else description="请先分析数据以显示散点图" />
            </el-card>

            <!-- 详细数据表格 -->
            <el-card shadow="hover" style="margin-top: 20px">
              <template #header>
                <span class="card-title">📋 详细动作数据</span>
              </template>
              <el-table :data="analysisResult.data" max-height="500" stripe>
                <el-table-column prop="x" label="时间" width="180" />
                <el-table-column prop="y" label="源地址" width="100" />
                <el-table-column
                  prop="action_code"
                  label="动作代码"
                  width="150"
                >
                  <template #default="scope">
                    <el-tag size="small" v-if="scope.row.action_code">
                      {{ getActionCodeLabel(scope.row.action_code) }}
                    </el-tag>
                    <span v-else style="color: #ccc">-</span>
                  </template>
                </el-table-column>
                <el-table-column prop="action_type" label="动作类型">
                  <template #default="scope">
                    {{ formatActionType(scope.row.action_type) }}
                  </template>
                </el-table-column>
              </el-table>
            </el-card>
          </div>
        </div>
      </el-collapse-transition>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, computed } from "vue";
import * as echarts from "echarts";
import { loadDataApi } from "@/api/data";
import type { EChartsOption } from "echarts";
import { ElMessage } from "element-plus";
import ActionScatterChart from "@/components/ActionScatterChart.vue";

interface ActionCodeTag {
  value: string;
  label: string;
  checked: boolean;
}

const chartRef = ref<HTMLElement>();
let chartInstance: echarts.ECharts | null = null;

// 原始数据
let rawActionData: any[] = [];
let rawPositionData: any[] = [];

// 动作代码标签
const actionCodeTags = ref<ActionCodeTag[]>([]);

// 煤机位置分析面板显示状态
const showAnalysisPanel = ref(false);

// 煤机位置1的选择状态（起始位置）
const position1 = ref({
  selectedPosition: null as number | null,
  availableTimes: [] as string[],
  selectedTime: "",
});

// 煤机位置2的选择状态（结束位置）
const position2 = ref({
  selectedPosition: null as number | null,
  availableTimes: [] as string[],
  selectedTime: "",
});

// 唯一的位置值列表
const uniquePositions = ref<number[]>([]);

// 分析结果
const analysisResult = ref({
  data: [] as any[],
});

// 时间范围
const timeRange = ref({
  start: "",
  end: "",
});

// 从 sessionStorage 加载筛选状态
const loadFilterState = () => {
  try {
    const saved = sessionStorage.getItem("scatterFilterState");
    if (saved) {
      return JSON.parse(saved);
    }
  } catch (e) {
    console.error("加载筛选状态失败", e);
  }
  return null;
};

// 保存筛选状态到 sessionStorage
const saveFilterState = () => {
  try {
    const state = actionCodeTags.value.map((tag) => ({
      value: tag.value,
      checked: tag.checked,
    }));
    sessionStorage.setItem("scatterFilterState", JSON.stringify(state));
  } catch (e) {
    console.error("保存筛选状态失败", e);
  }
};

// 加载位置选择状态
const loadPositionState = () => {
  try {
    const saved = sessionStorage.getItem("shearerPositionState");
    if (saved) {
      return JSON.parse(saved);
    }
  } catch (e) {
    console.error("加载位置状态失败", e);
  }
  return null;
};

// 保存位置选择状态
const savePositionState = () => {
  try {
    const state = {
      position1: {
        selectedPosition: position1.value.selectedPosition,
        selectedTime: position1.value.selectedTime,
      },
      position2: {
        selectedPosition: position2.value.selectedPosition,
        selectedTime: position2.value.selectedTime,
      },
    };
    sessionStorage.setItem("shearerPositionState", JSON.stringify(state));
  } catch (e) {
    console.error("保存位置状态失败", e);
  }
};

// 动作名称映射
const actionCodeLabels: Record<string, string> = {
  SPRAY: "喷雾",
  BPROP_UP: "升柱",
  BPROP_DOWN: "降柱",
  FPROP_UP: "前升柱",
  FPROP_DOWN: "前降柱",
  PUSH_SLIDE: "推溜",
  PULL_FRAME: "拉架",
  PROTECT_BAR: "护帮",
  EXTEND_BEAM: "伸缩梁",
};

const getActionCodeLabel = (code: string): string => {
  return actionCodeLabels[code] || code;
};

const formatActionType = (type: string): string => {
  if (!type) return "-";
  return type.replace("ActionType.", "");
};

// 是否显示分析结果
const showAnalysisResult = computed(() => {
  return analysisResult.value.data.length > 0;
});

// 是否可以开始分析
const canAnalyze = computed(() => {
  return (
    position1.value.selectedPosition !== null &&
    position1.value.selectedTime &&
    position2.value.selectedPosition !== null &&
    position2.value.selectedTime
  );
});

// 切换分析面板显示
const toggleAnalysisPanel = () => {
  showAnalysisPanel.value = !showAnalysisPanel.value;
};

// 初始化图表
const initChart = () => {
  if (!chartRef.value) return;

  chartInstance = echarts.init(chartRef.value);
  loadChartData();
};

// 切换动作筛选
const toggleActionFilter = (actionCode: string) => {
  const tag = actionCodeTags.value.find((t) => t.value === actionCode);
  if (tag) {
    tag.checked = !tag.checked;
    updateChart();
    // 保存筛选状态
    saveFilterState();
  }
};

// 更新图表
const updateChart = () => {
  if (!chartInstance) return;

  // 获取选中的动作代码
  const selectedActionCodes = actionCodeTags.value
    .filter((t) => t.checked)
    .map((t) => t.value);

  // 过滤动作数据
  const filteredActionData =
    selectedActionCodes.length > 0
      ? rawActionData.filter((d) => selectedActionCodes.includes(d.action_code))
      : rawActionData;

  // 为不同的动作代码分配颜色
  const colorMap: Record<string, string> = {};
  const colors = [
    "#409EFF",
    "#67C23A",
    "#E6A23C",
    "#F56C6C",
    "#909399",
    "#C71585",
    "#00CED1",
    "#FF8C00",
    "#9370DB",
    "#3CB371",
  ];
  actionCodeTags.value.forEach((tag, index) => {
    colorMap[tag.value] = colors[index % colors.length];
  });

  const option: EChartsOption = {
    title: {
      text: "电液控数据散点图",
      left: "center",
    },
    tooltip: {
      trigger: "item",
      formatter: (params: any) => {
        const data = params.data;
        if (data?.frame_type === "支架动作") {
          const actionLabel = getActionCodeLabel(data.action_code);
          return `时间: ${data.name}<br/>源地址: ${data.src_no}<br/>类型: 支架动作<br/>动作: ${actionLabel}`;
        } else if (data?.frame_type === "煤机位置") {
          return `时间: ${data.name}<br/>位置: ${data.position}<br/>类型: 煤机位置<br/>方向: ${data.direction}`;
        }
        return "";
      },
    },
    legend: {
      data: ["支架动作", "煤机位置"],
      top: 30,
    },
    grid: {
      left: "3%",
      right: "4%",
      bottom: "3%",
      containLabel: true,
    },
    xAxis: {
      type: "category",
      name: "时间",
      nameLocation: "middle",
      nameGap: 30,
      axisLabel: {
        rotate: 45,
      },
      data: [
        ...new Set([
          ...rawActionData.map((d) => d.x),
          ...rawPositionData.map((d) => d.x),
        ]),
      ].sort(),
    },
    yAxis: [
      {
        type: "value",
        name: "源地址 / 位置值",
        position: "left",
        minInterval: 1,
        splitNumber: 10,
        axisLabel: {
          interval: 0,
        },
      },
    ],
    series: [
      {
        name: "支架动作",
        type: "scatter",
        yAxisIndex: 0,
        data: filteredActionData.map((d) => ({
          value: [d.x, d.y],
          name: d.x,
          frame_type: d.frame_type,
          src_no: d.src_no,
          action_code: d.action_code,
        })),
        itemStyle: {
          color: (params: any) => {
            const actionCode = params.data.action_code;
            return colorMap[actionCode] || "#409EFF";
          },
        },
        symbolSize: 8,
      },
      {
        name: "煤机位置",
        type: "scatter",
        yAxisIndex: 0,
        data: rawPositionData.map((d) => ({
          value: [d.x, d.y],
          name: d.x,
          frame_type: d.frame_type,
          position: d.y,
          direction: d.direction,
        })),
        itemStyle: {
          color: (params: any) => {
            const dir = params.data?.direction || "";
            if (dir.includes("Up")) return "#67C23A";
            if (dir.includes("Down")) return "#F56C6C";
            return "#909399";
          },
        },
        symbol: "triangle",
        symbolRotate: (params: any) => {
          const dir = params.data?.direction || "";
          if (dir.includes("Up")) return 0; // 向上三角形
          if (dir.includes("Down")) return 180; // 向下三角形（旋转180度）
          return 0; // 停止 - 默认方向
        },
        symbolSize: 12,
      },
    ],
  };

  chartInstance.setOption(option, true);
};

// 加载图表数据
const loadChartData = async () => {
  try {
    const data = await loadDataApi.getScatterData();

    // 保存原始数据
    rawActionData = data.action_data.map((d: any) => ({
      x: d.x,
      y: d.y,
      name: d.name,
      frame_type: d.frame_type,
      src_no: d.src_no,
      action_code: d.action_code || "",
    }));

    rawPositionData = data.position_data.map((d: any) => ({
      x: d.x,
      y: d.y,
      name: d.name,
      frame_type: d.frame_type,
      position: d.position,
      direction: d.direction,
    }));

    // 提取所有唯一的动作代码
    const uniqueActionCodes = [
      ...new Set(rawActionData.map((d) => d.action_code).filter((c) => c)),
    ];

    // 尝试加载保存的筛选状态
    const savedState = loadFilterState();

    // 创建动作代码标签
    actionCodeTags.value = uniqueActionCodes.map((code) => ({
      value: code,
      label: getActionCodeLabel(code),
      checked: savedState
        ? (savedState.find((s: any) => s.value === code)?.checked ?? true)
        : true, // 如果有保存的状态则使用，否则默认选中
    }));

    // 更新唯一位置值列表
    if (rawPositionData && rawPositionData.length > 0) {
      const positions = [...new Set(rawPositionData.map((d) => d.y))];
      const sorted = positions.sort((a, b) => a - b);
      uniquePositions.value = sorted;
      console.log("Unique positions updated:", sorted);

      // 尝试恢复保存的位置选择状态
      const savedPositionState = loadPositionState();
      if (savedPositionState) {
        // 恢复位置1的选择
        if (
          savedPositionState.position1?.selectedPosition &&
          sorted.includes(savedPositionState.position1.selectedPosition)
        ) {
          position1.value.selectedPosition =
            savedPositionState.position1.selectedPosition;
          // 触发位置变化以加载时间选项
          setTimeout(() => {
            handlePosition1Change();
          }, 100);
        }

        // 恢复位置2的选择
        if (
          savedPositionState.position2?.selectedPosition &&
          sorted.includes(savedPositionState.position2.selectedPosition)
        ) {
          position2.value.selectedPosition =
            savedPositionState.position2.selectedPosition;
          // 触发位置变化以加载时间选项
          setTimeout(() => {
            handlePosition2Change();
          }, 100);
        }
      }
    } else {
      uniquePositions.value = [];
      console.log("No position data available");
    }

    // 初次渲染图表
    updateChart();

    // 数据加载后调用 resize 确保图表正确渲染
    setTimeout(() => {
      chartInstance?.resize();
    }, 100);
  } catch (error) {
    console.error("加载图表数据失败", error);
    ElMessage.error("加载数据失败");
  }
};

// 处理位置1变化
const handlePosition1Change = () => {
  if (
    position1.value.selectedPosition === null ||
    rawPositionData.length === 0
  ) {
    position1.value.availableTimes = [];
    position1.value.selectedTime = "";
    savePositionState();
    return;
  }

  // 获取该位置对应的所有时间点
  const times = rawPositionData
    .filter((d) => d.y === position1.value.selectedPosition)
    .map((d) => d.x)
    .sort();

  position1.value.availableTimes = times;

  // 尝试恢复保存的时间选择
  const savedState = loadPositionState();
  if (
    savedState?.position1?.selectedTime &&
    times.includes(savedState.position1.selectedTime)
  ) {
    position1.value.selectedTime = savedState.position1.selectedTime;
  } else {
    position1.value.selectedTime = times[0] || "";
  }

  console.log(`位置 ${position1.value.selectedPosition} 可用时间:`, times);

  // 保存状态
  savePositionState();
};

// 处理时间1选择
const handleTime1Change = () => {
  savePositionState();
};

// 处理位置2变化
const handlePosition2Change = () => {
  if (
    position2.value.selectedPosition === null ||
    rawPositionData.length === 0
  ) {
    position2.value.availableTimes = [];
    position2.value.selectedTime = "";
    savePositionState();
    return;
  }

  // 获取该位置对应的所有时间点
  const times = rawPositionData
    .filter((d) => d.y === position2.value.selectedPosition)
    .map((d) => d.x)
    .sort();

  position2.value.availableTimes = times;

  // 尝试恢复保存的时间选择
  const savedState = loadPositionState();
  if (
    savedState?.position2?.selectedTime &&
    times.includes(savedState.position2.selectedTime)
  ) {
    position2.value.selectedTime = savedState.position2.selectedTime;
  } else {
    position2.value.selectedTime = times[0] || "";
  }

  console.log(`位置 ${position2.value.selectedPosition} 可用时间:`, times);

  // 保存状态
  savePositionState();
};

// 处理时间2选择
const handleTime2Change = () => {
  savePositionState();
};

// 时间字符串转秒数（用于比较）
const timeToSeconds = (timeStr: string): number => {
  // 解析完整的时间字符串 "YYYY-MM-DD HH:MM:SS"
  const date = new Date(timeStr);
  return date.getTime() / 1000;
};

// 分析数据
const analyzeData = () => {
  if (rawActionData.length === 0) {
    ElMessage.warning("数据未加载");
    return;
  }

  const time1 = position1.value.selectedTime;
  const time2 = position2.value.selectedTime;

  // 确定开始和结束时间
  let startTime = time1;
  let endTime = time2;

  if (timeToSeconds(time1) > timeToSeconds(time2)) {
    startTime = time2;
    endTime = time1;
  }

  timeRange.value = {
    start: startTime,
    end: endTime,
  };

  console.log(`分析时间区间: ${startTime} ~ ${endTime}`);

  // 获取选中的动作代码（使用散点图的筛选状态）
  const selectedActionCodes = actionCodeTags.value
    .filter((t) => t.checked)
    .map((t) => t.value);

  // 提取所有时间点（用于确定时间范围）
  const allActionTimes = rawActionData.map((d) => d.x).sort();

  // 找到时间范围内的所有时间点
  const timesInRange = allActionTimes.filter((t) => {
    const seconds = timeToSeconds(t);
    return (
      seconds >= timeToSeconds(startTime) && seconds <= timeToSeconds(endTime)
    );
  });

  console.log(`时间范围内的动作时间点数量: ${timesInRange.length}`);

  // 筛选该时间范围内的所有支架动作
  let filteredActions = rawActionData.filter((d) => timesInRange.includes(d.x));

  console.log(`筛选前的动作数量: ${filteredActions.length}`);

  // 应用动作类型筛选
  if (selectedActionCodes.length > 0) {
    filteredActions = filteredActions.filter((d) =>
      selectedActionCodes.includes(d.action_code || ""),
    );
  }

  console.log(`筛选后的动作数量: ${filteredActions.length}`);

  analysisResult.value.data = filteredActions;

  ElMessage.success(`分析完成，共找到 ${filteredActions.length} 条动作`);
};

// 获取唯一源地址数量
const getUniqueSources = (data: any[]): number => {
  return new Set(data.map((d) => d.y)).size;
};

// 获取动作类型列表
const getActionTypes = (data: any[]): string[] => {
  return [...new Set(data.map((d) => d.action_code).filter(Boolean))];
};

// 获取时间范围内的煤机位置数据
const getPositionDataInRange = () => {
  if (
    rawPositionData.length === 0 ||
    !timeRange.value.start ||
    !timeRange.value.end
  ) {
    return [];
  }

  const startTime = timeRange.value.start;
  const endTime = timeRange.value.end;

  // 获取时间范围内的所有煤机位置数据
  const positionDataInRange = rawPositionData.filter((d) => {
    const seconds = timeToSeconds(d.x);
    return (
      seconds >= timeToSeconds(startTime) && seconds <= timeToSeconds(endTime)
    );
  });

  return positionDataInRange;
};

const handleRefresh = () => {
  ElMessage.info("正在刷新数据...");
  loadChartData();
};

onMounted(() => {
  initChart();

  window.addEventListener("resize", () => {
    chartInstance?.resize();
  });
});

onUnmounted(() => {
  chartInstance?.dispose();
  window.removeEventListener("resize", () => {
    chartInstance?.resize();
  });
});
</script>

<style scoped>
.scatter-chart {
  padding: 20px;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-section {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
}

.filter-label {
  font-weight: bold;
  margin-right: 10px;
  color: #606266;
}

.filter-card {
  height: 100%;
}

.card-title {
  font-weight: bold;
  font-size: 16px;
}

.result-section {
  margin-top: 20px;
}
</style>
