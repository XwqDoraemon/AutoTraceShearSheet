<template>
  <el-container class="app-container">
    <el-aside width="200px" class="sidebar">
      <div class="logo">
        <h2>电液控数据可视化</h2>
      </div>
      <el-menu
        :default-active="activeMenu"
        router
        background-color="#545c64"
        text-color="#fff"
        active-text-color="#ffd04b"
      >
        <el-menu-item index="/">
          <template #title>
            <span>数据概览</span>
          </template>
        </el-menu-item>
        <el-menu-item index="/scatter">
          <template #title>
            <span>散点图分析</span>
          </template>
        </el-menu-item>
        <el-menu-item index="/sensor">
          <template #title>
            <span>传感器轨迹</span>
          </template>
        </el-menu-item>
        <el-menu-item index="/features">
          <template #title>
            <span>特征分析</span>
          </template>
        </el-menu-item>
      </el-menu>
    </el-aside>

    <el-container>
      <el-header>
        <div class="header-content">
          <h3>电液控数据处理系统</h3>
          <div class="actions">
            <el-button @click="handleShowGuide"> 📖 使用说明 </el-button>
          </div>
        </div>
      </el-header>

      <el-main>
        <keep-alive>
          <router-view />
        </keep-alive>
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { useRoute } from "vue-router";
import { ElMessage } from "element-plus";

const route = useRoute();

const activeMenu = computed(() => route.path);

const handleShowGuide = () => {
  ElMessage({
    message: "刷新页面或点击各页面的刷新按钮来更新数据",
    duration: 3000,
    type: "info",
  });
};
</script>

<style scoped>
.app-container {
  height: 100vh;
}

.sidebar {
  background-color: #545c64;
}

.logo {
  padding: 20px;
  color: #fff;
  text-align: center;
  border-bottom: 1px solid #434a50;
}

.logo h2 {
  margin: 0;
  font-size: 18px;
}

.el-header {
  background-color: #fff;
  border-bottom: 1px solid #e6e6e6;
  display: flex;
  align-items: center;
}

.header-content {
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header-content h3 {
  margin: 0;
}

.el-main {
  background-color: #f5f7fa;
  padding: 20px;
  overflow-y: auto;
}
</style>
