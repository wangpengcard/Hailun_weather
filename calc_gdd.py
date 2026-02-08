import pandas as pd
from datetime import datetime, timedelta
import os

def run_calculation():
    source_file = '2026.csv'
    target_file = 'GDD_2026.csv'
    
    if not os.path.exists(source_file):
        print(f"找不到源文件: {source_file}")
        return

    # 1. 自动获取北京时间昨天的日期
    # 修改为 .strftime('%Y-%m-%d') 以确保识别 YYYY-MM-DD 格式（如 2026-02-07）
    yesterday = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=1))
    target_date = yesterday.strftime('%Y-%m-%d')
    print(f"目标日期: {target_date}")

    # 2. 读取并筛选
    df = pd.read_csv(source_file)
    
    # 确保 CSV 中的日期列也是字符串匹配
    day_data = df[df['date'] == target_date]
    
    if day_data.empty:
        print(f"未找到 {target_date} 的数据，跳过计算。")
        return

    # 3. 计算结果列表
    results = []
    
    # 直接从当天数据中提取唯一的乡镇
    town_groups = day_data.groupby(['town_name', 'town_id'])

    for (t_name, t_id), group in town_groups:
        temps = group['temp']
        
        # --- 活动积温 act_add ---
        avg_t = temps.mean()
        act_val = round(avg_t, 2) if avg_t >= 10 else 0.0
        
        # --- 有效积温 eff_add ---
        # 上限 30 截断，下限 10 截断
        eff_temps = temps.clip(upper=30) - 10
        eff_temps = eff_temps.clip(lower=0)
        eff_val = round(eff_temps.mean(), 2)
        
        results.append({
            'town_name': t_name,
            'town_id': t_id,
            'date': target_date,
            'act_add': act_val,
            'eff_add': eff_val
        })

    # 4. 写入文件（保持 utf-8-sig 以便 Excel 正确显示中文）
    new_df = pd.DataFrame(results)
    
    if os.path.exists(target_file) and os.path.getsize(target_file) > 0:
        old_df = pd.read_csv(target_file)
        # 排除已存在的同日期数据防止重复追加
        old_df = old_df[old_df['date'] != target_date]
        final_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        final_df = new_df

    final_df.to_csv(target_file, index=False, encoding='utf-8-sig')
    print(f"成功计算并更新 {target_date} 的数据至 {target_file}。")

if __name__ == "__main__":
    run_calculation()
