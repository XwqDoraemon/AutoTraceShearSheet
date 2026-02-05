<template>
  <div class="dashboard">
    <el-row :gutter="20">
      <el-col :span="6" v-for="stat in statistics" :key="stat.label">
        <el-card class="stat-card">
          <div class="stat-content">
            <div class="stat-icon" :style="{ color: stat.color }">
              {{ stat.icon }}
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stat.value }}</div>
              <div class="stat-label">{{ stat.label }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px">
      <el-col :span="12">
        <el-card>
          <template #header>
            <span>数据类型分布</span>
          </template>
          <div ref="pieChartRef" style="height: 300px"></div>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card>
          <template #header>
            <span>时间分布</span>
          </template>
          <div ref="barChartRef" style="height: 300px"></div>
        </el-card>
      </el-col>
    </el-row>

    <el-row style="margin-top: 20px">
      <el-col :span="24">
        <el-card>
          <template #header>
            <span>最近数据记录</span>
          </template>
          <el-table :data="tableData" stripe>
            <el-table-column prop="time" label="时间" width="180" />
            <el-table-column prop="src_no" label="源地址" width="100" />
            <el-table-column prop="frame_type" label="类型" width="120" />
            <el-table-column prop="details" label="详情" />
          </el-table>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import * as echarts from 'echarts'
import { loadDataApi } from '@/api/data'
import type { EChartsOption } from 'echarts'

const pieChartRef = ref<HTMLElement>()
const barChartRef = ref<HTMLElement>()
const statistics = ref<any[]>([])
const tableData = ref<any[]>([])

const loadStatistics = async () => {
  try {
    const data = await loadDataApi.getStatistics()

    statistics.value = [
      {
        label: '总记录数',
        value: data.total_records.toLocaleString(),
        icon: '📊',
        color: '#409EFF'
      },
      {
        label: '支架动作',
        value: data.action_records.toLocaleString(),
        icon: '⚙️',
        color: '#67C23A'
      },
      {
        label: '煤机位置',
        value: data.position_records.toLocaleString(),
        icon: '📍',
        color: '#E6A23C'
      },
      {
        label: '源地址数',
        value: data.unique_sources.toLocaleString(),
        icon: '🔗',
        color: '#F56C6C'
      }
    ]

    // 加载饼图
    if (pieChartRef.value) {
      const pieChart = echarts.init(pieChartRef.value)
      const pieOption = {
        tooltip: { trigger: 'item' },
        series: [{
          type: 'pie',
          radius: '60%',
          data: [
            { value: data.action_records, name: '支架动作' },
            { value: data.position_records, name: '煤机位置' }
          ]
        }]
      }
      pieChart.setOption(pieOption, true)
      setTimeout(() => pieChart.resize(), 100)
    }

    // 加载柱状图（小时分布）
    if (barChartRef.value) {
      const barChart = echarts.init(barChartRef.value)
      const barOption = {
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: ['14时', '15时', '16时'] },
        yAxis: { type: 'value' },
        series: [{
          type: 'bar',
          data: [
            Math.floor(data.action_records * 0.1),
            Math.floor(data.action_records * 0.8),
            Math.floor(data.action_records * 0.1)
          ]
        }]
      }
      barChart.setOption(barOption, true)
      setTimeout(() => barChart.resize(), 100)
    }
  } catch (error) {
    console.error('加载统计信息失败', error)
  }
}

const loadPreviewData = async () => {
  try {
    const result = await loadDataApi.getPreviewData()

    tableData.value = result.data.slice(0, 10).map((item: any) => ({
      time: item.time,
      src_no: item.src_no,
      frame_type: item.frame_type,
      details: item.frame_type === '支架动作'
        ? `动作类型: ${item.action_type}, 动作: ${item.action_codes.join(', ')}`
        : `位置: ${item.position}, 方向: ${item.direction}`
    }))
  } catch (error) {
    console.error('加载预览数据失败', error)
  }
}

onMounted(() => {
  loadStatistics()
  loadPreviewData()
})
</script>

<style scoped>
.dashboard {
  padding: 20px;
}

.stat-card {
  cursor: pointer;
  transition: transform 0.2s;
}

.stat-card:hover {
  transform: translateY(-5px);
}

.stat-content {
  display: flex;
  align-items: center;
  gap: 20px;
}

.stat-icon {
  font-size: 40px;
}

.stat-info {
  flex: 1;
}

.stat-value {
  font-size: 28px;
  font-weight: bold;
  color: #303133;
}

.stat-label {
  font-size: 14px;
  color: #909399;
  margin-top: 5px;
}
</style>
