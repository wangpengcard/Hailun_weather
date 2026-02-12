import os
import json
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

# 配置参数
JSON_FILE = '2026.json'
FORECAST_FILE = 'forecasts.json'
TOWNS_FILE = 'towns.csv'
USAGE_FILE = 'api_usage.json'
QUOTA_LIMIT = 900  # API 熔断阈值
ROLLING_HOURS = 720  # 严格保留过去720小时（30天）的数据

def get_beijing_time():
    """获取北京时间 (UTC+8)"""
    return datetime.now(timezone(timedelta(hours=8)))

def update_quota(increment=0):
    """管理 API 配额计数器"""
    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.strftime('%Y-%m-%d')
    
    if os.path.exists(USAGE_FILE):
        with open(USAGE_FILE, 'r') as f:
            try:
                usage = json.load(f)
            except:
                usage = {"count": 0, "last_reset_utc": today_utc}
    else:
        usage = {"count": 0, "last_reset_utc": today_utc}

    if usage.get("last_reset_utc") != today_utc:
        usage = {"count": 0, "last_reset_utc": today_utc}
    
    usage["count"] += increment
    with open(USAGE_FILE, 'w') as f:
        json.dump(usage, f)
    return usage["count"]

def fetch_weather():
    current_usage = update_quota(0)
    if not os.path.exists(TOWNS_FILE):
        print(f"Error: {TOWNS_FILE} missing.")
        return
        
    towns_df = pd.read_csv(TOWNS_FILE)
    api_key = os.getenv('OWM_API_KEY')
    if not api_key:
        print("Error: OWM_API_KEY environment variable missing.")
        return

    # 1. 加载现有实况数据
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            try:
                all_history = json.load(f)
            except:
                all_history = {}
    else:
        all_history = {}

    forecast_storage = {}
    bj_now = get_beijing_time()
    cutoff_time = bj_now - timedelta(hours=ROLLING_HOURS)

    # 2. 遍历乡镇获取数据
    for _, town in towns_df.iterrows():
        if current_usage >= QUOTA_LIMIT: 
            print("Quota limit reached, skipping remaining towns.")
            break 
        
        t_id = str(town['town_id'])
        t_name = town['town_name']
        
        try:
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={town['lat']}&lon={town['lon']}&appid={api_key}&units=metric&lang=zh_cn"
            resp = requests.get(url, timeout=15).json()
            current_usage = update_quota(1)
            
            # --- A. 处理 48 小时预报 (按照要求的爱民乡模板) ---
            hourly_data = resp.get('hourly', [])
            if hourly_data:
                town_forecasts = []
                for h in hourly_data[:48]:
                    h_dt = datetime.fromtimestamp(h['dt'], timezone(timedelta(hours=8)))
                    
                    forecast_item = {
                        "town_name": t_name,
                        "town_id": t_id,
                        "date": h_dt.strftime('%Y-%m-%d'),
                        "time": h_dt.strftime('%H:00'),
                        "temp": h.get('temp', 0),
                        "pressure": h.get('pressure', 0),
                        "humidity": h.get('humidity', 0),
                        "dew_point": h.get('dew_point', 0),
                        "clouds": h.get('clouds', 0),
                        "uvi": h.get('uvi', 0),
                        "visibility": h.get('visibility', 0),
                        "wind_speed": h.get('wind_speed', 0),
                        "wind_gust": h.get('wind_gust', h.get('wind_speed', 0)),
                        "wind_deg": h.get('wind_deg', 0),
                        "rain": h.get('rain', {}).get('1h', 0),
                        "snow": h.get('snow', {}).get('1h', 0),
                        "weather_desc": h.get('weather', [{}])[0].get('description', '未知'),
                        "flag": 1
                    }
                    town_forecasts.append(forecast_item)
                
                forecast_storage[t_id] = town_forecasts

            # --- B. 处理当前实况 ---
            curr = resp.get('current', {})
            new_record = {
                "town_name": t_name, 
                "town_id": t_id,
                "date": bj_now.strftime('%Y-%m-%d'), 
                "time": bj_now.strftime('%H:00'),
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

            if t_id not in all_history:
                all_history[t_id] = []
            
            # 查重：确保不添加重复的小时点
            if not any(d['date'] == new_record['date'] and d['time'] == new_record['time'] for d in all_history[t_id]):
                all_history[t_id].append(new_record)

            # --- C. 720小时滚动清理 ---
            temp_list = []
            for rec in all_history[t_id]:
                try:
                    # 解析实况中的时间，确保格式匹配 %Y-%m-%d %H:%M
                    rec_dt = datetime.strptime(f"{rec['date']} {rec['time']}", '%Y-%m-%d %H:%M').replace(tzinfo=timezone(timedelta(hours=8)))
                    if rec_dt >= cutoff_time:
                        temp_list.append(rec)
                except:
                    continue
            all_history[t_id] = temp_list

            print(f"Updated: {t_name}")

        except Exception as e:
            print(f"Error fetching {t_id}: {e}")
            continue 

    # 3. 统一保存预报 JSON
    if forecast_storage:
        try:
            with open(FORECAST_FILE, 'w', encoding='utf-8') as f:
                json.dump(forecast_storage, f, ensure_ascii=False, indent=2) # 预报通常供预览，保留缩进
        except Exception as e:
            print(f"Error saving forecasts: {e}")

    # 4. 原子化保存历史实况 JSON
    tmp_json = JSON_FILE + ".tmp"
    try:
        with open(tmp_json, 'w', encoding='utf-8') as f:
            # 实况数据池较大，使用紧凑格式节省空间
            json.dump(all_history, f, ensure_ascii=False, separators=(',', ':'))
        
        if os.path.exists(JSON_FILE):
            os.remove(JSON_FILE)
        os.rename(tmp_json, JSON_FILE)
        print("JSON history data updated successfully.")
    except Exception as e:
        print(f"Critical Error saving history JSON: {e}")

if __name__ == "__main__":
    fetch_weather()
