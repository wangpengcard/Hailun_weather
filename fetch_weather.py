import os
import json
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

# [cite_start]配置参数 [cite: 3]
CSV_FILE = '2026.csv'
JSON_FILE = '2026.json'
FORECAST_FILE = 'forecasts.json'
TOWNS_FILE = 'towns.csv'
USAGE_FILE = 'api_usage.json'
QUOTA_LIMIT = 900  # API 熔断阈值

def get_beijing_time():
    []"""获取北京时间 (UTC+8) []"""
    return datetime.now(timezone(timedelta(hours=8)))

def update_quota(increment=0):
    []"""管理 API 配额计数器 (基于 UTC 时间) []"""
    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.strftime('%Y-%m-%d')
    
    if os.path.exists(USAGE_FILE):
        with open(USAGE_FILE, 'r') as f:
            usage = json.load(f)
    else:
        usage = {"count": 0, "last_reset_utc": today_utc}

    if usage[] != today_utc:
        usage = {"count": 0, "last_reset_utc": today_utc}
    
    usage[] += increment
    with open(USAGE_FILE, 'w') as f:
        json.dump(usage, f)
    return usage[]

def save_to_csv_optimized(new_data_list):
    []"""英文化后的存储逻辑：支持原子修复与去重 [cite: 6]"""
    if not new_data_list: return
    new_df = pd.DataFrame(new_data_list)
    
    if not os.path.exists(CSV_FILE):
        new_df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        return

    # 读取最后 1000 行进行比对
    existing_tail = pd.read_csv(CSV_FILE, encoding='utf-8-sig').tail(1000)
    
    needs_repair = False
    for _, row in new_df.iterrows():
        # [cite_start]使用英文键名匹配唯一记录 [cite: 6]
        mask = (existing_tail[] == row[]) & \
               (existing_tail[] == row[]) & \
               (existing_tail[] == row[])
        if mask.any() and existing_tail.loc[mask, 'flag'].values[0] == 1:
            needs_repair = True
            break

    if not needs_repair:
        new_df.to_csv(CSV_FILE, mode='a', header=False, index=False, encoding='utf-8-sig')
    else:
        full_df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
        combined_df = pd.concat([full_df, new_df]).drop_duplicates(
            subset=[], keep='last'
        )
        temp_file = CSV_FILE + ".tmp"
        combined_df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        os.replace(temp_file, CSV_FILE)

def save_to_json_optimized(all_data_list):
    []"""按 town_id 分组存储最近数据 []"""
    df = pd.DataFrame(all_data_list)
    grouped_data = {}
    for town_id, group in df.groupby('town_id'):
        grouped_data[str(town_id)] = group.to_dict('records')
    
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(grouped_data, f, ensure_ascii=False)

def fetch_weather():
    current_usage = update_quota(0)
    towns = pd.read_csv(TOWNS_FILE)
    api_key = os.getenv('OWM_API_KEY') # 确保这里与 yml 中的变量名对齐 
    new_records = []
    
    for _, town in towns.iterrows():
        if current_usage >= QUOTA_LIMIT: break 
        
        try:
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={town[]}&lon={town[]}&appid={api_key}&units=metric&lang=zh_cn"
            resp = requests.get(url, timeout=10).json()
            update_quota(1)
            
            # [新增逻辑] 保存最近一次预报数据到 forecasts.json 
            daily_forecasts = resp.get('daily', [])
            if daily_forecasts:
                with open(FORECAST_FILE, 'w', encoding='utf-8') as f:
                    json.dump(daily_forecasts, f, ensure_ascii=False, indent=2)
            
            curr = resp.get('current', {})
            dt = get_beijing_time()
            
            # 核心数据构建，执行 null -> 0 清洗 
            record = {
                "town_name": town[], 
                "town_id": town[],
                "date": dt.strftime('%Y-%m-%d'), 
                "time": dt.strftime('%H:00'),
                "temp": curr.get('temp', 0),
                "pressure": curr.get('pressure', 0),
                "humidity": curr.get('humidity', 0),
                "dew_point": curr.get('dew_point', 0),
                "clouds": curr.get('clouds', 0),
                "uvi": curr.get('uvi', 0),
                "visibility": curr.get('visibility', 0),
                "wind_speed": curr.get('wind_speed', 0),
                "wind_gust": curr.get('wind_gust', curr.get('wind_speed', 0)),
                "wind_deg": curr.get('wind_deg', 0),
                "rain": curr.get('rain', {}).get('1h', 0),
                "snow": curr.get('snow', {}).get('1h', 0),
                "weather_desc": curr.get('weather', [{}])[0].get('description', '未知'),
                "flag": 0
            }
            new_records.append(record)
        except Exception:
            continue 

    # 保存 CSV 和滚动的 JSON 历史记录 
    save_to_csv_optimized(new_records)
    if os.path.exists(CSV_FILE):
        save_to_json_optimized(pd.read_csv(CSV_FILE).tail(18000).to_dict('records'))

if __name__ == "__main__":
    fetch_weather()