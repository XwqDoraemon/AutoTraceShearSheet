<template>
  <div ref="chartRef" class="action-scatter-chart"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from "vue";
import * as echarts from "echarts";
import type { EChartsOption } from "echarts";

interface ActionDataPoint {
  x: string;
  y: number;
  action_code?: string;
  action_type?: string;
}

interface PositionDataPoint {
  x: string;
  y: number;
  position: number;
  direction: string;
}

interface Props {
  data: ActionDataPoint[];
  positionData?: PositionDataPoint[];
  title?: string;
  height?: string;
}

const props = withDefaults(defineProps<Props>(), {
  positionData: () => [],
  title: "支架动作散点图",
  height: "500px",
});

const chartRef = ref<HTMLElement>();
let chartInstance: echarts.ECharts | null = null;

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

// 初始化图表
const initChart = () => {
  if (!chartRef.value) return;

  chartInstance = echarts.init(chartRef.value);
  updateChart();

  // 监听窗口大小变化
  window.addEventListener("resize", handleResize);
};

// 更新图表
const updateChart = () => {
  if (!chartInstance) return;

  // 按动作代码分组
  const actionGroups: Record<string, any[]> = {};
  props.data.forEach((item) => {
    const code = item.action_code || "无动作代码";
    if (!actionGroups[code]) {
      actionGroups[code] = [];
    }
    actionGroups[code].push(item);
  });

  // 为每个动作类型分配颜色
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
    "#1E90FF",
    "#FF69B4",
  ];

  let colorIndex = 0;
  Object.keys(actionGroups).forEach((code) => {
    colorMap[code] = colors[colorIndex % colors.length];
    colorIndex++;
  });

  // 创建系列数据 - 支架动作
  const series = Object.keys(actionGroups).map((code) => ({
    name: getActionCodeLabel(code),
    type: "scatter",
    data: actionGroups[code].map((item) => ({
      value: [item.x, item.y],
      name: item.x,
      action_code: item.action_code,
      action_type: item.action_type,
      src_no: item.y,
    })),
    itemStyle: {
      color: colorMap[code],
    },
    symbolSize: 10,
    emphasis: {
      itemStyle: {
        borderColor: "#000",
        borderWidth: 2,
        shadowBlur: 10,
        shadowColor: "rgba(0, 0, 0, 0.3)",
      },
    },
  }));

  // 添加煤机位置数据
  if (props.positionData && props.positionData.length > 0) {
    // 按方向分组煤机位置数据
    const positionGroups: Record<string, any[]> = {};
    props.positionData.forEach((item) => {
      const dir = item.direction || "未知";
      if (!positionGroups[dir]) {
        positionGroups[dir] = [];
      }
      positionGroups[dir].push(item);
    });

    // 煤机位置方向颜色和符号映射
    const directionConfig: Record<
      string,
      { color: string; symbol: string; symbolRotate: number; label: string }
    > = {
      "ShearerDir.Up": {
        color: "#67C23A",
        symbol: "triangle",
        symbolRotate: 0,
        label: "上行",
      },
      "ShearerDir.Down": {
        color: "#F56C6C",
        symbol: "triangle",
        symbolRotate: 180,
        label: "下行",
      },
      "ShearerDir.Stop": {
        color: "#909399",
        symbol: "circle",
        symbolRotate: 0,
        label: "停止",
      },
    };

    // 添加煤机位置系列
    Object.keys(positionGroups).forEach((dir) => {
      const config = directionConfig[dir] || {
        color: "#999",
        symbol: "circle",
        symbolRotate: 0,
        label: dir,
      };
      series.push({
        name: `煤机-${config.label}`,
        type: "scatter",
        data: positionGroups[dir].map((item) => ({
          value: [item.x, item.y],
          name: item.x,
          position: item.position,
          direction: item.direction,
          src_no: item.y,
        })),
        itemStyle: {
          color: config.color,
        },
        symbol: config.symbol,
        symbolRotate: config.symbolRotate,
        symbolSize: 16,
        emphasis: {
          itemStyle: {
            borderColor: "#000",
            borderWidth: 2,
            shadowBlur: 10,
            shadowColor: "rgba(0, 0, 0, 0.5)",
          },
          symbolSize: 20,
        },
      });
    });
  }

  // 获取所有唯一的时间点（包括动作和位置数据）
  const allTimes = [
    ...new Set([
      ...props.data.map((d) => d.x),
      ...props.positionData.map((d) => d.x),
    ]),
  ].sort();

  // 构建图例数据
  const legendData = [
    ...Object.keys(actionGroups).map((code) => getActionCodeLabel(code)),
  ];

  if (props.positionData && props.positionData.length > 0) {
    const directions = [...new Set(props.positionData.map((d) => d.direction))];
    const directionLabels: Record<string, string> = {
      "ShearerDir.Up": "煤机-上行",
      "ShearerDir.Down": "煤机-下行",
      "ShearerDir.Stop": "煤机-停止",
    };
    directions.forEach((dir) => {
      legendData.push(directionLabels[dir] || dir);
    });
  }

  const option: EChartsOption = {
    title: {
      text: `${props.title} (动作: ${props.data.length} 条${props.positionData.length ? `, 位置: ${props.positionData.length} 条` : ""})`,
      left: "center",
      textStyle: {
        fontSize: 16,
        fontWeight: "bold",
      },
    },
    tooltip: {
      trigger: "item",
      formatter: (params: any) => {
        const data = params.data;
        if (data.direction) {
          // 煤机位置数据
          const dirLabel: Record<string, string> = {
            "ShearerDir.Up": "上行",
            "ShearerDir.Down": "下行",
            "ShearerDir.Stop": "停止",
          };
          return `
            时间: ${data.name}<br/>
            位置值: ${data.position}<br/>
            方向: ${dirLabel[data.direction] || data.direction}<br/>
            源地址: ${data.src_no}
          `;
        } else {
          // 支架动作数据
          return `
            时间: ${data.name}<br/>
            源地址: ${data.src_no}<br/>
            动作: ${getActionCodeLabel(data.action_code)}<br/>
            类型: ${formatActionType(data.action_type)}
          `;
        }
      },
    },
    legend: {
      data: legendData,
      top: 30,
      type: "scroll",
    },
    grid: {
      left: "5%",
      right: "5%",
      bottom: "15%",
      top: "20%",
      containLabel: true,
    },
    xAxis: {
      type: "category",
      name: "时间",
      nameLocation: "middle",
      nameGap: 30,
      nameTextStyle: {
        fontWeight: "bold",
      },
      data: allTimes,
      axisLabel: {
        rotate: 45,
        interval: "auto",
      },
    },
    yAxis: {
      type: "value",
      name: "源地址 / 位置值",
      nameLocation: "middle",
      nameGap: 50,
      nameTextStyle: {
        fontWeight: "bold",
      },
      minInterval: 1,
      splitNumber: 10,
      axisLabel: {
        interval: 0,
      },
    },
    series: series,
    dataZoom: [
      {
        type: "inside",
        start: 0,
        end: 100,
        xAxisIndex: 0,
      },
      {
        type: "slider",
        start: 0,
        end: 100,
        xAxisIndex: 0,
        height: 20,
        bottom: 10,
      },
    ],
  };

  chartInstance.setOption(option, true);

  // 调整大小
  setTimeout(() => {
    chartInstance?.resize();
  }, 100);
};

// 处理窗口大小变化
const handleResize = () => {
  chartInstance?.resize();
};

// 监听数据变化
watch(
  () => [props.data, props.positionData],
  () => {
    nextTick(() => {
      updateChart();
    });
  },
  { deep: true },
);

onMounted(() => {
  initChart();
});

onUnmounted(() => {
  window.removeEventListener("resize", handleResize);
  chartInstance?.dispose();
});
</script>

<style scoped>
.action-scatter-chart {
  width: 100%;
  height: v-bind("height");
}
</style>
