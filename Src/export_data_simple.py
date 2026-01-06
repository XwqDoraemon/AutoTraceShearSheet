#!/usr/bin/env python3
"""
电液控数据导出程序 (简化版)
直接将数据库中所有数据解析并导出为txt格式
格式: 时间,Src,Dst,Pri,Cmd,Buffer
"""

import sqlite3
from frame_packet import FramePacket
from datetime import datetime
import os

def export_all_data():
    """导出所有数据"""
    db_path = "电液控UDP驱动_20250904_14.db"
    output_file = "电液控数据导出.txt"
    batch_size = 5000
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return
        
    print(f"🚀 开始导出电液控数据...")
    print(f"📁 数据库: {db_path}")
    print(f"📄 输出: {output_file}")
    print("="*50)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 获取总记录数
        cursor.execute("SELECT COUNT(*) FROM t_sac_frame")
        total_rows = cursor.fetchone()[0]
        print(f"📊 总记录数: {total_rows:,}")
        
        # 创建输出文件
        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入表头
            f.write("时间,Src,Dst,Pri,Cmd,Buffer\n")
            
            exported_count = 0
            error_count = 0
            processed_count = 0
            
            # 分批处理
            for offset in range(0, total_rows, batch_size):
                cursor.execute("""
                    SELECT f_date_time, f_buffer 
                    FROM t_sac_frame 
                    ORDER BY f_id 
                    LIMIT ? OFFSET ?
                """, (batch_size, offset))
                
                batch_data = cursor.fetchall()
                
                # 处理批次数据
                for date_time_str, buffer_data in batch_data:
                    try:
                        # 解析时间
                        dt = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                        time_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        
                        # 解析FramePacket
                        frame = FramePacket(buffer_data, is_nc_mode=True)
                        
                        # 获取字段
                        src = frame.src_no
                        dst = frame.dst_no
                        pri = frame.b_pri
                        cmd = frame.b_cmd
                        buffer_hex = buffer_data.hex().upper()
                        
                        # 写入文件
                        f.write(f"{time_str},{src},{dst},{pri},{cmd},{buffer_hex}\n")
                        exported_count += 1
                        
                    except Exception:
                        error_count += 1
                        continue
                
                processed_count += len(batch_data)
                progress = processed_count / total_rows * 100
                print(f"🔄 进度: {processed_count:,}/{total_rows:,} ({progress:.1f}%) "
                      f"- 成功: {exported_count:,}, 错误: {error_count:,}")
        
        print(f"\n✅ 导出完成！")
        print(f"📈 处理记录: {processed_count:,}")
        print(f"✅ 成功导出: {exported_count:,}")
        print(f"❌ 解析错误: {error_count:,}")
        
        # 显示文件信息
        file_size = os.path.getsize(output_file)
        if file_size > 1024 * 1024:
            print(f"📦 文件大小: {file_size / (1024 * 1024):.2f} MB")
        else:
            print(f"📦 文件大小: {file_size / 1024:.2f} KB")
        
        print(f"📄 输出文件: {output_file}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    export_all_data()