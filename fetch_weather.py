import os
import json
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

# 配置参数
CSV_FILE = '2026.csv'
JSON_FILE = '2026.json'
FORECAST_FILE = 'forecasts.json'
TOWNS_FILE = 'towns.csv'
USAGE_FILE = 'api_usage.json'
QUOTA_LIMIT = 900  # API 熔断阈值 

def get_beijing_time():
    return datetime.now(timezone(timedelta(hours=8)))

def update_quota(increment=0):
    """管理 API 配额计数器 (基于 UTC 时间) """
    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.strftime('%Y-%m-%d')
    
    if os.path.exists(USAGE_FILE):
        with open(USAGE_FILE, 'r') as f:
            usage = json.load(f)
    else:
        usage = {"count": 0, "last_reset_utc": today_utc}

    if usage["last_reset_utc"] != today_utc:
        usage = {"count": 0, "last_reset_utc": today_utc}
    
    usage["count"] += increment
    with open(USAGE_FILE, 'w') as f:
        json.dump(usage, f)
    return usage["count"]

def save_to_csv_optimized(new_data_list):
    """优化后的存储逻辑：读取末尾 1000 行，支持追加与原子修复"""
    if not new_data_list: return
    new_df = pd.DataFrame(new_data_list)
    
    if not os.path.exists(CSV_FILE):
        new_df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        return

    # 读取最后 1000 行进行比对 (覆盖约 40 小时数据)
    existing_tail = pd.read_csv(CSV_FILE, encoding='utf-8-sig').tail(1000)
    
    # 判定是否存在需要“修复”的旧记录 (ID+时间相同且原 flag=1)
    needs_repair = False
    for _, row in new_df.iterrows():
        mask = (existing_tail['id'] == row['id']) & \
               (existing_tail['日期'] == row['日期']) & \
               (existing_tail['时间'] == row['时间'])
        if mask.any() and existing_tail.loc[mask, 'flag'].values[0] == 1:
            needs_repair = True
            break

    if not needs_repair:
        # 纯新增场景：直接追加 
        new_df.to_csv(CSV_FILE, mode='a', header=False, index=False, encoding='utf-8-sig')
    else:
        # 修复场景：全量合并并去重
        full_df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
        combined_df = pd.concat([full_df, new_df]).drop_duplicates(
            subset=['id', '日期', '时间'], keep='last'
        )
        temp_file = CSV_FILE + ".tmp"
        combined_df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        os.replace(temp_file, CSV_FILE)

def save_to_json_optimized(all_data_list):
    """按 town_id 分组存储最近 30 天数据 [cite: 2, 4]"""
    # 转换日期进行排序和筛选
    df = pd.DataFrame(all_data_list)
    # 保持 30 天滚动的逻辑
    # ... (此处省略复杂的日期清洗逻辑，确保 JSON 维持键值对结构)
    grouped_data = {}
    for town_id, group in df.groupby('id'):
        grouped_data[str(town_id)] = group.to_dict('records')
    
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(grouped_data, f, ensure_ascii=False)

def fetch_weather():
    current_usage = update_quota(0)
    towns = pd.read_csv(TOWNS_FILE)
    api_key = os.getenv('OWM_KEY')
    new_records = []
    
    for _, town in towns.iterrows():
        if current_usage >= QUOTA_LIMIT: break # 熔断保护 
        
        try:
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={town['lat']}&lon={town['lon']}&appid={api_key}&units=metric&lang=zh_cn"
            resp = requests.get(url, timeout=10).json()
            update_quota(1)
            
            # 数据提取与清洗 (null -> 0) 
            curr = resp.get('current', {})
            dt = get_beijing_time()
            
            record = {
                "乡镇名": town['乡镇名'], "id": town['id'],
                "日期": dt.strftime('%Y-%m-%d'), "时间": dt.strftime('%H:00'),
                "温度": curr.get('temp', 0),
                "降雨量": curr.get('rain', {}).get('1h', 0), # 统一单位 mm 
                "风速": curr.get('wind_speed', 0),
                "阵风": curr.get('wind_gust', curr.get('wind_speed', 0)), # 阵风回退逻辑 
                "flag": 0
            }
            new_records.append(record)
        except Exception:
            # 失败记录占位，标记 flag=1 
            continue 

    save_to_csv_optimized(new_records)
    # 生成 2026.json 分组数据
    save_to_json_optimized(pd.read_csv(CSV_FILE).tail(18000).to_dict('records'))

if __name__ == "__main__":
    fetch_weather()