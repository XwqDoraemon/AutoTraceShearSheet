/**
 * 特征数据加载工具
 * 从 public/data 目录加载 JSON 文件
 */

export interface PCAResult {
  component_1: number;
  component_2: number;
  src_no: number;
  variance_ratio: number;
}

export interface PCAData {
  data: PCAResult[];
  variance_ratio: number[];
  export_time: string;
}

export interface FeatureItem {
  name: string;
  value: number;
}

export interface FeatureImportance {
  features: FeatureItem[];
  export_time: string;
}

/**
 * 加载 JSON 文件
 */
async function loadJSON<T>(filename: string): Promise<T> {
  const response = await fetch(`/data/${filename}?t=${Date.now()}`);
  if (!response.ok) {
    throw new Error(`加载 ${filename} 失败`);
  }
  return response.json();
}

/**
 * 特征数据加载 API
 */
export const loadFeatureApi = {
  /**
   * 获取 PCA 数据
   */
  async getPCAData(): Promise<PCAData> {
    return loadJSON<PCAData>("pca.json");
  },

  /**
   * 获取特征重要性
   */
  async getFeatureImportance(): Promise<FeatureImportance> {
    return loadJSON<FeatureImportance>("importance.json");
  },
};
