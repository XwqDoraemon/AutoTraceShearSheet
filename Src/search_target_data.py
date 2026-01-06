#!/usr/bin/env python3
"""
搜索目标数据程序
快速搜索数据库中是否存在b_pri=3且b_cmd=4的记录
"""

import sqlite3
from frame_packet import FramePacket
from datetime import datetime

def search_target_data():
    """搜索符合条件的数据"""
    db_path = "电液控UDP驱动_20250904_14.db"
    
    print("连接数据库...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取总记录数
    cursor.execute("SELECT COUNT(*) FROM t_sac_frame")
    total_rows = cursor.fetchone()[0]
    print(f"数据库总记录数: {total_rows}")
    
    batch_size = 10000
    found_records = []
    processed_count = 0
    
    print("\n开始搜索符合条件的记录 (b_pri=3, b_cmd=4)...")
    
    # 分批搜索
    for offset in range(0, min(100000, total_rows), batch_size):  # 先搜索前10万条
        cursor.execute("""
            SELECT f_id, f_date_time, f_buffer 
            FROM t_sac_frame 
            ORDER BY f_id 
            LIMIT ? OFFSET ?
        """, (batch_size, offset))
        
        batch_data = cursor.fetchall()
        
        for record_id, date_time_str, buffer_data in batch_data:
            try:
                # 解析时间
                dt = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                
                # 使用FramePacket解析buffer
                frame = FramePacket(buffer_data, is_nc_mode=True)
                
                # 检查条件：b_pri == 3 且 b_cmd == 4
                if frame.b_pri == 3 and frame.b_cmd == 4:
                    found_records.append({
                        'id': record_id,
                        'time': dt,
                        'src_no': frame.src_no,
                        'dst_no': frame.dst_no,
                        'data': frame.data_string,
                        'buffer': buffer_data.hex()
                    })
                    
                    print(f"找到符合条件的记录 #{len(found_records)}:")
                    print(f"  记录ID: {record_id}")
                    print(f"  时间: {dt}")
                    print(f"  源地址: {frame.src_no}")
                    print(f"  目标地址: {frame.dst_no}")
                    print(f"  数据: {frame.data_string}")
                    print()
                    
                    # 找到前10条就停止
                    if len(found_records) >= 10:
                        break
                        
            except Exception as e:
                continue
        
        processed_count += len(batch_data)
        print(f"已搜索: {processed_count}/{min(100000, total_rows)} "
              f"({processed_count/min(100000, total_rows)*100:.1f}%), "
              f"找到: {len(found_records)} 条")
        
        if len(found_records) >= 10:
            break
    
    conn.close()
    
    print(f"\n搜索完成！")
    print(f"在前 {processed_count} 条记录中找到 {len(found_records)} 条符合条件的记录")
    
    if len(found_records) == 0:
        print("\n建议:")
        print("1. 检查筛选条件是否正确 (当前: b_pri=3, b_cmd=4)")
        print("2. 尝试搜索更多数据或调整筛选条件")
        print("3. 查看数据中实际存在的b_pri和b_cmd值")
        
        # 统计实际的b_pri和b_cmd分布
        print("\n正在统计数据中的b_pri和b_cmd分布...")
        analyze_data_distribution()

def analyze_data_distribution():
    """分析数据中b_pri和b_cmd的分布"""
    db_path = "电液控UDP驱动_20250904_14.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 分析前1000条记录的分布
    cursor.execute("SELECT f_buffer FROM t_sac_frame LIMIT 1000")
    sample_data = cursor.fetchall()
    
    pri_counts = {}
    cmd_counts = {}
    
    for (buffer_data,) in sample_data:
        try:
            frame = FramePacket(buffer_data, is_nc_mode=True)
            
            pri = frame.b_pri
            cmd = frame.b_cmd
            
            pri_counts[pri] = pri_counts.get(pri, 0) + 1
            cmd_counts[cmd] = cmd_counts.get(cmd, 0) + 1
            
        except:
            continue
    
    print(f"\nb_pri 分布 (前1000条记录):")
    for pri in sorted(pri_counts.keys()):
        print(f"  b_pri={pri}: {pri_counts[pri]} 次")
    
    print(f"\nb_cmd 分布 (前1000条记录):")
    for cmd in sorted(cmd_counts.keys()):
        print(f"  b_cmd={cmd}: {cmd_counts[cmd]} 次")
    
    conn.close()

if __name__ == "__main__":
    search_target_data()