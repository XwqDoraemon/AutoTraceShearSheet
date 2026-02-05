<template>
  <div class="pca-chart">
    <el-card>
      <template #header>
        <div class="header">
          <span>PCA 降维可视化</span>
          <div>
            <el-button @click="openExportGuide">数据导出指南</el-button>
            <el-button type="primary" @click="handleRefresh">刷新数据</el-button>
          </div>
        </div>
      </template>

      <el-alert
        title="PCA 降维说明"
        type="info"
        :closable="false"
        style="margin-bottom: 20px"
      >
        <template #default>
          PCA (主成分分析) 将高维特征数据降维到 2D 空间，便于可视化分析源地址之间的相似性。
          距离较近的点表示操作模式相似。
          <br><br>
          <strong>数据导出</strong>: 运行 <code>python export_data.py</code> 来生成最新的特征数据。
        </template>
      </el-alert>

      <el-row :gutter="20">
        <el-col :span="24">
          <div ref="chartRef" style="height: 600px"></div>
        </el-col>
      </el-row>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { loadFeatureApi } from '@/api/features'
import type { EChartsOption } from 'echarts'
import { ElMessage } from 'element-plus'

const chartRef = ref<HTMLElement>()
let chartInstance: echarts.ECharts | null = null

const initChart = () => {
  if (!chartRef.value) return

  chartInstance = echarts.init(chartRef.value)
  loadChartData()
}

const loadChartData = async () => {
  try {
    const result = await loadFeatureApi.getPCAData()

    const option: EChartsOption = {
      title: {
        text: 'PCA 降维可视化',
        subtext: `累积解释方差: ${(result.variance_ratio[0] + result.variance_ratio[1]) * 100:.1f}%`,
        left: 'center'
      },
      tooltip: {
        trigger: 'item',
        formatter: (params: any) => {
          return `源地址: ${params.data[2]}<br/>主成分1: ${params.data[0].toFixed(2)}<br/>主成分2: ${params.data[1].toFixed(2)}`
        }
      },
      xAxis: {
        type: 'value',
        name: `主成分1 (${result.variance_ratio[0] * 100:.1f}%)`,
        nameLocation: 'middle',
        nameGap: 30
      },
      yAxis: {
        type: 'value',
        name: `主成分2 (${result.variance_ratio[1] * 100:.1f}%)`,
        nameLocation: 'middle',
        nameGap: 40
      },
      grid: {
        left: '10%',
        right: '10%',
        bottom: '15%'
      },
      series: [{
        type: 'scatter',
        symbolSize: 10,
        data: result.data.map(item => [item.component_1, item.component_2, item.src_no]),
        itemStyle: {
          color: '#409EFF',
          opacity: 0.7
        },
        emphasis: {
          itemStyle: {
            color: '#F56C6C',
            borderColor: '#000',
            borderWidth: 2
          }
        }
      }]
    }

    // 使用 notMerge: true 确保完全替换旧数据，不合并
    chartInstance?.setOption(option, true)

    // 数据加载后调用 resize 确保图表正确渲染
    setTimeout(() => {
      chartInstance?.resize()
    }, 100)
  } catch (error) {
    console.error('加载 PCA 数据失败', error)
    ElMessage.warning('PCA 数据文件不存在，请先运行 python export_data.py')
  }
}

const handleRefresh = () => {
  ElMessage.info('正在刷新数据...')
  loadChartData()
}

const openExportGuide = () => {
  ElMessage({
    message: '在项目根目录运行: python export_data.py',
    duration: 5000,
    type: 'info'
  })
}

onMounted(() => {
  initChart()

  window.addEventListener('resize', () => {
    chartInstance?.resize()
  })
})

onUnmounted(() => {
  chartInstance?.dispose()
  window.removeEventListener('resize', () => {
    chartInstance?.resize()
  })
})
</script>

<style scoped>
.pca-chart {
  padding: 20px;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
