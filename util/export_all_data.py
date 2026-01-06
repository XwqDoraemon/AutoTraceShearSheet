#!/usr/bin/env python3
"""
电液控数据导出程序
将数据库中所有数据解析并导出为txt格式
格式: 时间, Src, Dst, Pri, Cmd, Buffer
"""

import sqlite3
from frame_packet import FramePacket
from datetime import datetime
import os
import sys

class DataExporter:
    """数据导出器"""
    
    def __init__(self, db_path: str, output_file: str = "电液控数据导出.txt"):
        """
        初始化数据导出器
        
        Args:
            db_path: 数据库文件路径
            output_file: 输出文件路径
        """
        self.db_path = db_path
        self.output_file = output_file
        self.batch_size = 5000  # 批处理大小，防止内存溢出
        
    def export_all_data(self):
        """导出所有数据"""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")
            
        print(f"🚀 开始导出数据...")
        print(f"📁 数据库文件: {self.db_path}")
        print(f"📄 输出文件: {self.output_file}")
        print("="*60)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 获取总记录数
            cursor.execute("SELECT COUNT(*) FROM t_sac_frame")
            total_rows = cursor.fetchone()[0]
            print(f"📊 数据库总记录数: {total_rows:,}")
            
            # 创建输出文件并写入表头
            with open(self.output_file, 'w', encoding='utf-8') as f:
                # 写入表头
                header = "时间,Src,Dst,Pri,Cmd,Buffer\n"
                f.write(header)
                
                exported_count = 0
                error_count = 0
                processed_count = 0
                
                # 分批处理数据
                for offset in range(0, total_rows, self.batch_size):
                    cursor.execute("""
                        SELECT f_id, f_date_time, f_buffer 
                        FROM t_sac_frame 
                        ORDER BY f_id 
                        LIMIT ? OFFSET ?
                    """, (self.batch_size, offset))
                    
                    batch_data = cursor.fetchall()
                    batch_results = self._process_batch(batch_data)
                    
                    # 写入批次数据
                    for line in batch_results['lines']:
                        f.write(line)
                    
                    exported_count += batch_results['exported']
                    error_count += batch_results['errors']
                    processed_count += len(batch_data)
                    
                    # 显示进度
                    progress = processed_count / total_rows * 100
                    print(f"🔄 已处理: {processed_count:,}/{total_rows:,} ({progress:.1f}%), "
                          f"成功导出: {exported_count:,}, 错误: {error_count:,}")
            
            print(f"\n✅ 导出完成！")
            print(f"📈 总处理记录: {processed_count:,}")
            print(f"✅ 成功导出: {exported_count:,}")
            print(f"❌ 解析错误: {error_count:,}")
            print(f"📄 输出文件: {self.output_file}")
            
            # 显示文件大小
            file_size = os.path.getsize(self.output_file)
            if file_size > 1024 * 1024:
                print(f"📦 文件大小: {file_size / (1024 * 1024):.2f} MB")
            else:
                print(f"📦 文件大小: {file_size / 1024:.2f} KB")
                
        finally:
            conn.close()
    
    def _process_batch(self, batch_data: list) -> dict:
        """
        处理单个批次的数据
        
        Args:
            batch_data: 批次数据
            
        Returns:
            处理结果字典
        """
        lines = []
        exported_count = 0
        error_count = 0
        
        for record_id, date_time_str, buffer_data in batch_data:
            try:
                # 解析时间
                dt = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                time_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 保留3位毫秒
                
                # 使用FramePacket解析buffer
                frame = FramePacket(buffer_data, is_nc_mode=True)
                
                # 获取解析后的字段
                src = frame.src_no
                dst = frame.dst_no
                pri = frame.b_pri
                cmd = frame.b_cmd
                buffer_hex = buffer_data.hex().upper()
                
                # 格式化输出行
                line = f"{time_str},{src},{dst},{pri},{cmd},{buffer_hex}\n"
                lines.append(line)
                exported_count += 1
                
            except Exception as e:
                # 记录解析失败的情况
                error_count += 1
                # 可选：记录错误信息到日志
                # print(f"解析记录 {record_id} 失败: {e}")
                continue
        
        return {
            'lines': lines,
            'exported': exported_count,
            'errors': error_count
        }
    
    def preview_data(self, limit: int = 10):
        """
        预览导出数据格式
        
        Args:
            limit: 预览记录数
        """
        print(f"📋 预览前 {limit} 条记录的导出格式:")
        print("-" * 80)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT f_id, f_date_time, f_buffer 
                FROM t_sac_frame 
                ORDER BY f_id 
                LIMIT ?
            """, (limit,))
            
            preview_data = cursor.fetchall()
            
            # 打印表头
            print("时间,Src,Dst,Pri,Cmd,Buffer")
            print("-" * 80)
            
            for record_id, date_time_str, buffer_data in preview_data:
                try:
                    # 解析时间
                    dt = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                    time_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    
                    # 使用FramePacket解析buffer
                    frame = FramePacket(buffer_data, is_nc_mode=True)
                    
                    # 获取解析后的字段
                    src = frame.src_no
                    dst = frame.dst_no
                    pri = frame.b_pri
                    cmd = frame.b_cmd
                    buffer_hex = buffer_data.hex().upper()
                    
                    # 截断过长的buffer显示
                    if len(buffer_hex) > 40:
                        buffer_display = buffer_hex[:40] + "..."
                    else:
                        buffer_display = buffer_hex
                    
                    print(f"{time_str},{src},{dst},{pri},{cmd},{buffer_display}")
                    
                except Exception as e:
                    print(f"记录 {record_id} 解析失败: {e}")
                    
        finally:
            conn.close()
        
        print("-" * 80)

def main():
    """主函数"""
    db_path = "电液控UDP驱动_20250904_14.db"
    output_file = "电液控数据导出.txt"
    
    try:
        # 创建数据导出器
        exporter = DataExporter(db_path, output_file)
        
        # 预览数据格式
        print("📋 数据格式预览:")
        exporter.preview_data(5)
        
        print("\n" + "="*60)
        
        # 确认是否继续
        response = input("是否继续导出所有数据？(y/n): ").lower().strip()
        if response not in ['y', 'yes', '是', '']:
            print("❌ 用户取消导出")
            return
        
        # 导出所有数据
        exporter.export_all_data()
        
        print(f"\n🎉 数据导出完成！请查看文件: {output_file}")
        
    except Exception as e:
        print(f"❌ 导出过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()