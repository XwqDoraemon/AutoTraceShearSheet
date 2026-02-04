/**
 * 数据加载工具
 * 从 public/data 目录加载 JSON 文件
 */

export interface Statistics {
  total_records: number;
  action_records: number;
  position_records: number;
  unique_sources: number;
  time_range_start: string;
  time_range_end: string;
  duration_minutes: number;
  export_time: string;
}

export interface ScatterDataPoint {
  x: string;
  y: number;
  name: string;
  frame_type: string;
  src_no?: number;
  direction?: string;
  position?: number;
  action_code?: string;
  action_type?: string;
}

export interface ScatterData {
  action_data: ScatterDataPoint[];
  position_data: ScatterDataPoint[];
  export_time: string;
}

export interface PreviewData {
  time: string;
  src_no: number;
  frame_type: string;
  action_type?: string;
  action_codes?: string[];
  position?: number;
  direction?: string;
}

/**
 * 加载 JSON 文件
 * 使用随机数缓存破坏确保每次都获取最新数据
 */
async function loadJSON<T>(filename: string): Promise<T> {
  // 使用随机数而非时间戳，确保每次请求都被视为唯一的
  const cacheBuster = Math.random().toString(36).substring(7);
  const response = await fetch(`/data/${filename}?_v=${cacheBuster}`, {
    cache: "no-store", // 禁用浏览器缓存
  });
  if (!response.ok) {
    throw new Error(`加载 ${filename} 失败`);
  }
  return response.json();
}

/**
 * 数据加载 API
 */
export const loadDataApi = {
  /**
   * 获取统计信息
   */
  async getStatistics(): Promise<Statistics> {
    return loadJSON<Statistics>("statistics.json");
  },

  /**
   * 获取散点图数据
   */
  async getScatterData(): Promise<ScatterData> {
    return loadJSON<ScatterData>("scatter.json");
  },

  /**
   * 获取预览数据
   */
  async getPreviewData(): Promise<{ data: PreviewData[] }> {
    return loadJSON<{ data: PreviewData[] }>("preview.json");
  },
};
