#!/usr/bin/env python3
"""
示例: 如何使用 tsfresh 进行时间序列特征提取
展示常见使用场景和代码示例
"""

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest

from Src import DataProcessor, DataVisualizer, TsfreshFeatureExtractor


# ============================================================================
# 示例 1: 基本特征提取和导出
# ============================================================================
def example_1_basic_extraction():
    """基本特征提取流程"""
    print("=" * 60)
    print("示例 1: 基本特征提取")
    print("=" * 60)

    # 1. 加载数据
    processor = DataProcessor("Datas/电液控UDP驱动_20250904_14.db")
    filtered_data = processor.process_data_in_batches()

    # 2. 提取特征
    extractor = TsfreshFeatureExtractor()
    tsfresh_df = extractor.prepare_dataframe(filtered_data)
    features = extractor.extract_features(tsfresh_df)

    # 3. 导出特征
    extractor.export_features(features, filename="example_1_features.csv")

    print("✅ 示例 1 完成！")
    return features


# ============================================================================
# 示例 2: 操作模式聚类分析
# ============================================================================
def example_2_clustering(features: pd.DataFrame):
    """使用 K-Means 聚类分析操作模式"""
    print("\n" + "=" * 60)
    print("示例 2: 操作模式聚类分析")
    print("=" * 60)

    # 检查特征是否为空
    if features.empty:
        print("⚠ 特征为空，跳过聚类分析")
        return pd.DataFrame()

    # 填充缺失值
    features_filled = features.fillna(0)

    # 标准化
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features_filled)

    # K-Means 聚类
    n_clusters = 3  # 假设分为 3 类操作模式
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = kmeans.fit_predict(features_scaled)

    # 输出聚类结果
    results = pd.DataFrame({"src_no": features.index, "cluster": clusters})

    print("\n📊 聚类结果:")
    for cluster_id in range(n_clusters):
        cluster_members = results[results["cluster"] == cluster_id]["src_no"].tolist()
        print(f"  簇 {cluster_id}: {cluster_members}")

    # 导出聚类结果
    results.to_csv("outPut/features/clustering_results.csv", index=False)
    print("\n✅ 聚类结果已导出到: outPut/features/clustering_results.csv")

    return results


# ============================================================================
# 示例 3: 异常检测
# ============================================================================
def example_3_anomaly_detection(features: pd.DataFrame):
    """使用 Isolation Forest 检测异常操作"""
    print("\n" + "=" * 60)
    print("示例 3: 异常检测")
    print("=" * 60)

    # 检查特征是否为空
    if features.empty:
        print("⚠ 特征为空，跳过异常检测")
        return pd.DataFrame()

    # 填充缺失值
    features_filled = features.fillna(0)

    # 训练异常检测模型
    clf = IsolationForest(contamination=0.1, random_state=42)
    anomalies = clf.fit_predict(features_filled)

    # 识别异常源地址
    anomaly_results = pd.DataFrame(
        {
            "src_no": features.index,
            "is_normal": anomalies == 1,
            "anomaly_score": clf.score_samples(features_filled),
        }
    )

    normal_src = anomaly_results[anomaly_results["is_normal"]]["src_no"].tolist()
    anomaly_src = anomaly_results[~anomaly_results["is_normal"]]["src_no"].tolist()

    print(f"\n📊 异常检测结果:")
    print(f"  正常源地址 ({len(normal_src)} 个): {normal_src}")
    print(f"  异常源地址 ({len(anomaly_src)} 个): {anomaly_src}")

    # 导出异常检测结果
    anomaly_results.to_csv("outPut/features/anomaly_detection_results.csv", index=False)
    print("\n✅ 异常检测结果已导出到: outPut/features/anomaly_detection_results.csv")

    return anomaly_results


# ============================================================================
# 示例 4: 自定义特征集
# ============================================================================
def example_4_custom_features():
    """使用自定义特征集提取"""
    print("\n" + "=" * 60)
    print("示例 4: 自定义特征集提取")
    print("=" * 60)

    # 加载数据
    processor = DataProcessor("Datas/电液控UDP驱动_20250904_14.db")
    filtered_data = processor.process_data_in_batches()

    # 创建特征提取器
    extractor = TsfreshFeatureExtractor()

    # 自定义特征计算器 (只提取统计量)
    custom_fc_parameters = {
        "mean": None,
        "std": None,
        "variance": None,
        "maximum": None,
        "minimum": None,
        "median": None,
        "length": None,
    }

    extractor.fc_parameters = custom_fc_parameters

    # 准备数据并提取特征
    tsfresh_df = extractor.prepare_dataframe(filtered_data)
    features = extractor.extract_features(tsfresh_df)

    print(f"\n📊 自定义特征集提取完成:")
    print(f"  提取的特征数: {features.shape[1]}")
    print(f"  样本数: {features.shape[0]}")

    # 导出特征
    extractor.export_features(features, filename="example_4_custom_features.csv")

    print("✅ 示例 4 完成！")
    return features


# ============================================================================
# 示例 5: 可视化比较
# ============================================================================
def example_5_visualization_comparison(features: pd.DataFrame):
    """比较不同可视化方法"""
    print("\n" + "=" * 60)
    print("示例 5: 创建完整的特征可视化")
    print("=" * 60)

    # 检查特征是否为空
    if features.empty:
        print("⚠ 特征为空，跳过可视化")
        return

    # 创建可视化
    visualizer = DataVisualizer()
    visualizer.visualize_features(features, output_dir="outPut/features/example_5")

    print("✅ 示例 5 完成！生成的图表:")
    print("  - feature_correlation_heatmap.png")
    print("  - feature_distribution_boxplot.png")
    print("  - pca_visualization.png")
    print("  - feature_importance_ranking.png")


# ============================================================================
# 主函数
# ============================================================================
def main():
    """运行所有示例"""
    print("\n" + "=" * 60)
    print("tsfresh 特征提取示例集")
    print("=" * 60)

    try:
        # 示例 1: 基本特征提取
        features = example_1_basic_extraction()

        # 示例 2: 聚类分析
        clustering_results = example_2_clustering(features)

        # 示例 3: 异常检测
        anomaly_results = example_3_anomaly_detection(features)

        # 示例 4: 自定义特征集
        custom_features = example_4_custom_features()

        # 示例 5: 可视化
        example_5_visualization_comparison(features)

        print("\n" + "=" * 60)
        print("🎉 所有示例运行完成！")
        print("=" * 60)
        print("\n📁 生成的文件:")
        print("  - example_1_features.csv")
        print("  - clustering_results.csv")
        print("  - anomaly_detection_results.csv")
        print("  - example_4_custom_features.csv")
        print("  - example_5/ (包含所有可视化图表)")

    except Exception as e:
        print(f"\n❌ 示例运行出错: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
