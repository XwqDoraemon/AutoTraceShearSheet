#!/usr/bin/env python3
"""
电液控数据处理程序
根据需求提取和可视化人为操作信息
"""

import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from datetime import datetime
import pandas as pd
from frame_packet import FramePacket
import numpy as np
from typing import List, Tuple
import sys

# 设置中文字体支持
def setup_chinese_font():
    """设置matplotlib中文字体支持"""
    try:
        # 尝试不同的中文字体
        chinese_fonts = ['SimHei', 'Microsoft YaHei', 'SimSun', 'KaiTi', 'FangSong']
        
        # 获取系统可用字体
        available_fonts = set(f.name for f in fm.fontManager.ttflist)
        
        # 找到第一个可用的中文字体
        selected_font = None
        for font in chinese_fonts:
            if font in available_fonts:
                selected_font = font
                break
        
        if selected_font:
            plt.rcParams['font.sans-serif'] = [selected_font, 'DejaVu Sans']
            print(f"使用中文字体: {selected_font}")
        else:
            print("警告: 未找到中文字体，可能无法正确显示中文")
            plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
            
        plt.rcParams['axes.unicode_minus'] = False  # 正常显示负号
        
    except Exception as e:
        print(f"字体设置失败: {e}")
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False

# 初始化中文字体
setup_chinese_font()

class DataProcessor:
    """数据处理器"""
    
    def __init__(self, db_path: str):
        """
        初始化数据处理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.batch_size = 10000  # 批处理大小，防止内存溢出
        
    def process_data_in_batches(self) -> List[Tuple[datetime, int]]:
        """
        分批处理数据，提取符合条件的记录
        
        Returns:
            符合条件的数据列表 [(时间, b_Src), ...]
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 获取总记录数
        cursor.execute("SELECT COUNT(*) FROM t_sac_frame")
        total_rows = cursor.fetchone()[0]
        print(f"数据库总记录数: {total_rows}")
        
        filtered_data = []
        processed_count = 0
        
        # 分批处理数据
        for offset in range(0, total_rows, self.batch_size):
            cursor.execute("""
                SELECT f_date_time, f_buffer 
                FROM t_sac_frame 
                ORDER BY f_id 
                LIMIT ? OFFSET ?
            """, (self.batch_size, offset))
            
            batch_data = cursor.fetchall()
            batch_filtered = self._process_batch(batch_data)
            filtered_data.extend(batch_filtered)
            
            processed_count += len(batch_data)
            print(f"已处理: {processed_count}/{total_rows} ({processed_count/total_rows*100:.1f}%), "
                  f"符合条件记录: {len(filtered_data)}")
        
        conn.close()
        print(f"处理完成！共找到 {len(filtered_data)} 条符合条件的记录")
        return filtered_data
    
    def _process_batch(self, batch_data: List[Tuple]) -> List[Tuple[datetime, int]]:
        """
        处理单个批次的数据
        
        Args:
            batch_data: 批次数据
            
        Returns:
            符合条件的数据列表
        """
        filtered_batch = []
        
        for date_time_str, buffer_data in batch_data:
            try:
                # 解析时间
                dt = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                
                # 使用FramePacket解析buffer
                frame = FramePacket(buffer_data, is_nc_mode=True)
                
                # 检查条件：b_pri == 3 且 b_cmd == 4
                if frame.b_pri == 3 and frame.b_cmd == 4:
                    filtered_batch.append((dt, frame.src_no))
                    
            except Exception as e:
                # 跳过解析失败的记录
                continue
                
        return filtered_batch
    
    def create_visualization(self, data: List[Tuple[datetime, int]], 
                           output_file: str = "human_operation_visualization.png"):
        """
        创建数据可视化
        
        Args:
            data: 处理后的数据 [(时间, b_Src), ...]
            output_file: 输出文件名
        """
        if not data:
            print("没有符合条件的数据，无法创建可视化图表")
            return
            
        # 转换为DataFrame便于处理
        df = pd.DataFrame(data, columns=['时间', 'b_Src'])
        
        # 获取所有唯一的b_Src值并排序
        unique_src = sorted(df['b_Src'].unique())
        print(f"发现 {len(unique_src)} 个不同的源地址: {unique_src}")
        
        # 创建图表
        plt.figure(figsize=(15, 10))
        
        # 为每个b_Src创建散点图
        colors = plt.cm.tab20(np.linspace(0, 1, len(unique_src)))
        
        for i, src in enumerate(unique_src):
            src_data = df[df['b_Src'] == src]
            plt.scatter(src_data['b_Src'], src_data['时间'], 
                       c=[colors[i]], label=f'源地址 {src}', 
                       alpha=0.7, s=20)
        
        # 设置图表属性
        plt.xlabel('源地址 (b_Src)', fontsize=12)
        plt.ylabel('时间', fontsize=12)
        plt.title('电液控人为操作数据可视化\n(b_pri=3, b_cmd=4)', fontsize=14, fontweight='bold')
        
        # 设置时间轴格式
        plt.gca().yaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.gca().yaxis.set_major_locator(mdates.MinuteLocator(interval=5))
        
        # 设置x轴
        if len(unique_src) > 20:
            # 如果源地址太多，只显示部分标签
            step = max(1, len(unique_src) // 20)
            plt.xticks(unique_src[::step])
        else:
            plt.xticks(unique_src)
        
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # 如果源地址数量不太多，显示图例
        if len(unique_src) <= 10:
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # 保存图表
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"可视化图表已保存为: {output_file}")
        
        # 显示统计信息
        self._print_statistics(df)
        
        plt.show()
    
    def _print_statistics(self, df: pd.DataFrame):
        """
        打印统计信息
        
        Args:
            df: 数据DataFrame
        """
        print("\n=== 数据统计信息 ===")
        print(f"总记录数: {len(df)}")
        print(f"时间范围: {df['时间'].min()} 到 {df['时间'].max()}")
        print(f"源地址范围: {df['b_Src'].min()} 到 {df['b_Src'].max()}")
        print(f"不同源地址数量: {df['b_Src'].nunique()}")
        
        # 按源地址统计
        src_counts = df['b_Src'].value_counts().sort_index()
        print(f"\n各源地址操作次数:")
        for src, count in src_counts.head(10).items():
            print(f"  源地址 {src}: {count} 次")
        if len(src_counts) > 10:
            print(f"  ... 还有 {len(src_counts) - 10} 个源地址")

def main():
    """主函数"""
    db_path = "电液控UDP驱动_20250904_14.db"
    
    print("开始处理电液控数据...")
    print("=" * 50)
    
    # 创建数据处理器
    processor = DataProcessor(db_path)
    
    # 处理数据
    print("正在提取符合条件的数据 (b_pri=3, b_cmd=4)...")
    filtered_data = processor.process_data_in_batches()
    
    if not filtered_data:
        print("未找到符合条件的数据！")
        return
    
    # 创建可视化
    print("\n正在创建可视化图表...")
    processor.create_visualization(filtered_data)
    
    print("\n处理完成！")

if __name__ == "__main__":
    main()