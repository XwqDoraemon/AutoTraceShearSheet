<template>
  <div class="feature-analysis">
    <el-card>
      <template #header>
        <div class="header">
          <span>特征重要性分析</span>
          <el-button type="primary" @click="handleRefresh">刷新数据</el-button>
        </div>
      </template>

      <el-alert
        title="特征重要性说明"
        type="info"
        :closable="false"
        style="margin-bottom: 20px"
      >
        <template #default>
          显示基于方差的前 30 个最重要特征。
          方差越大表示该特征在不同源地址之间的差异越大，区分度越高。
          <br><br>
          <strong>数据导出</strong>: 运行 <code>python export_data.py</code> 来生成最新的特征数据。
        </template>
      </el-alert>

      <el-row :gutter="20">
        <el-col :span="24">
          <div ref="chartRef" style="height: 800px"></div>
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
    const result = await loadFeatureApi.getFeatureImportance()

    const features = result.features.slice(0, 30)
    const featureNames = features.map(f => {
      // 简化特征名称显示
      const name = f.name
      const parts = name.split('__')
      if (parts.length > 1) {
        const featureType = parts[0]
        const calcMethod = parts[1]
        return `${featureType.substring(0, 15)}...\n${calcMethod.substring(0, 20)}...`
      }
      return name
    })

    const option: EChartsOption = {
      title: {
        text: '特征重要性排名 (基于方差)',
        left: 'center'
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'shadow'
        }
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
      },
      xAxis: {
        type: 'value',
        name: '方差',
        nameLocation: 'middle',
        nameGap: 30
      },
      yAxis: {
        type: 'category',
        data: featureNames,
        axisLabel: {
          interval: 0,
          fontSize: 10
        }
      },
      series: [{
        type: 'bar',
        data: features.map(f => f.value),
        itemStyle: {
          color: '#409EFF'
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
    console.error('加载特征重要性失败', error)
    ElMessage.warning('特征数据文件不存在，请先运行 python export_data.py')
  }
}

const handleRefresh = () => {
  ElMessage.info('正在刷新数据...')
  loadChartData()
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
.feature-analysis {
  padding: 20px;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
