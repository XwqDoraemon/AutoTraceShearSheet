import { createRouter, createWebHistory, RouteRecordRaw } from "vue-router";

const routes: RouteRecordRaw[] = [
  {
    path: "/",
    name: "Dashboard",
    component: () => import("@/views/Dashboard.vue"),
    meta: { keepAlive: true },
  },
  {
    path: "/scatter",
    name: "Scatter",
    component: () => import("@/views/ScatterChart.vue"),
    meta: { keepAlive: true },
  },
  {
    path: "/sensor",
    name: "Sensor",
    component: () => import("@/views/SensorTrajectory.vue"),
    meta: { keepAlive: true },
  },
  {
    path: "/features",
    name: "Features",
    component: () => import("@/views/FeatureAnalysis.vue"),
    meta: { keepAlive: true },
  },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
  scrollBehavior(to, from, savedPosition) {
    // 保持滚动位置
    if (savedPosition) {
      return savedPosition;
    } else {
      return { top: 0 };
    }
  },
});

export default router;
