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
    """获取北京时间 (UTC+8)"""
    return datetime.now(timezone(timedelta(hours=8)))

def update_quota(increment=0):
    """管理 API 配额计数器"""
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
    """持久化存储实况数据到 CSV"""
    if not new_data_list: return
    new_df = pd.DataFrame(new_data_list)
    
    if not os.path.exists(CSV_FILE):
        new_df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        return

    existing_tail = pd.read_csv(CSV_FILE, encoding='utf-8-sig').tail(1000)
    needs_repair = False
    for _, row in new_df.iterrows():
        mask = (existing_tail['town_id'] == row['town_id']) & \
               (existing_tail['date'] == row['date']) & \
               (existing_tail['time'] == row['time'])
        if mask.any() and existing_tail.loc[mask, 'flag'].values[0] == 1:
            needs_repair = True
            break

    if not needs_repair:
        new_df.to_csv(CSV_FILE, mode='a', header=False, index=False, encoding='utf-8-sig')
    else:
        full_df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
        combined_df = pd.concat([full_df, new_df]).drop_duplicates(
            subset=['town_id', 'date', 'time'], keep='last'
        )
        temp_file = CSV_FILE + ".tmp"
        combined_df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        os.replace(temp_file, CSV_FILE)

def save_to_json_optimized(new_data_list):
    """主程序简化逻辑：仅负责向 2026.json 实时追加实况点"""
    if not new_data_list: return
    
    # 1. 加载现有 JSON
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            try:
                json_data = json.load(f)
            except:
                json_data = {}
    else:
        json_data = {}

    # 2. 将新记录按 town_id 追加到对应列表中
    for record in new_data_list:
        tid = str(record['town_id'])
        if tid not in json_data:
            json_data[tid] = []
        
        # 避免主程序重复运行导致 JSON 内出现相同时间点的重复数据
        if not any(d['date'] == record['date'] and d['time'] == record['time'] for d in json_data[tid]):
            json_data[tid].append(record)
    
    # 3. 保存回 JSON 文件 (白天不在此处进行 720 小时截断，由凌晨修补程序统一截断)
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False)

def fetch_weather():
    current_usage = update_quota(0)
    towns = pd.read_csv(TOWNS_FILE)
    api_key = os.getenv('OWM_API_KEY')
    new_records = []
    forecast_storage = {} # 用于存储所有城镇的预报数据
    
    for _, town in towns.iterrows():
        if current_usage >= QUOTA_LIMIT: break 
        
        try:
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={town['lat']}&lon={town['lon']}&appid={api_key}&units=metric&lang=zh_cn"
            resp = requests.get(url, timeout=15).json()
            update_quota(1)
            
            # --- 处理 48 小时预报 ---
            hourly_data = resp.get('hourly', [])
            if hourly_data:
                town_forecasts = []
                for h in hourly_data[:48]: # 只取未来 48 小时
                    h_dt = datetime.fromtimestamp(h['dt'], timezone(timedelta(hours=8)))
                    town_forecasts.append({
                        "time": h_dt.strftime('%H:00'),
                        "date": h_dt.strftime('%m-%d'),
                        "temp": h.get('temp', 0),
                        "rain": h.get('rain', {}).get('1h', 0),
                        "snow": h.get('snow', {}).get('1h', 0),
                        "pressure": h.get('pressure', 0),
                        "humidity": h.get('humidity', 0),
                        "dew_point": h.get('dew_point', 0),
                        "uvi": h.get('uvi', 0),
                        "clouds": h.get('clouds', 0),
                        "visibility": h.get('visibility', 0),
                        "wind_speed": h.get('wind_speed', 0),
                        "wind_gust": h.get('wind_gust', h.get('wind_speed', 0)),
                        "wind_deg": h.get('wind_deg', 0),
                        "weather_desc": h.get('weather', [{}])[0].get('description', '未知'),
                        "flag": 1
                    })
                forecast_storage[town['town_id']] = town_forecasts

            # --- 处理当前实况 ---
            curr = resp.get('current', {})
            dt = get_beijing_time()
            record = {
                "town_name": town["town_name"], 
                "town_id": town["town_id"],
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
        except Exception as e:
            print(f"Error fetching {town['town_id']}: {e}")
            continue 

    # 统一保存所有城镇的预报数据到 forecasts.json
    if forecast_storage:
        with open(FORECAST_FILE, 'w', encoding='utf-8') as f:
            json.dump(forecast_storage, f, ensure_ascii=False, indent=2)

    # 保存实况数据到 CSV 和 2026.json
    save_to_csv_optimized(new_records)
    save_to_json_optimized(new_records)

if __name__ == "__main__":
    fetch_weather()