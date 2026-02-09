import os
import json
import pandas as pd
import requests
import time
from datetime import datetime, timezone, timedelta

# 配置参数
CSV_FILE = '2026.csv'
JSON_FILE = '2026.json'
TOWNS_FILE = 'towns.csv'
USAGE_FILE = 'api_usage.json'
QUOTA_LIMIT = 900  # API 熔断阈值

def get_beijing_now():
    """获取当前的北京时间 (UTC+8)"""
    return datetime.now(timezone(timedelta(hours=8)))

def update_quota(increment=0):
    """管理 API 配额计数器 (与 fetch_weather.py 逻辑同步)"""
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

def repair_weather():
    api_key = os.getenv('OWM_API_KEY')
    if not api_key:
        print("Error: OWM_API_KEY not found.")
        return

    # 1. 确定核查日期：北京时间的昨天
    bj_now = get_beijing_now()
    bj_yesterday = bj_now - timedelta(days=1)
    target_date = bj_yesterday.strftime('%Y-%m-%d')
    print(f"Targeting repair for date: {target_date}")

    # 2. 读取基础数据
    if not os.path.exists(CSV_FILE) or not os.path.exists(TOWNS_FILE):
        print("Error: 2026.csv or towns.csv missing.")
        return

    towns_df = pd.read_csv(TOWNS_FILE)
    df_history = pd.read_csv(CSV_FILE, encoding='utf-8-sig')

    # 3. 统计缺失时段
    new_repairs = []
    current_usage = update_quota(0)

    for _, town in towns_df.iterrows():
        # 获取该镇昨天的已有时间点
        town_data = df_history[(df_history['town_id'] == town['town_id']) & (df_history['date'] == target_date)]
        existing_hours = set(town_data['time'].tolist())
        
        # 检查 00:00 到 23:00
        for h in range(24):
            time_str = f"{h:02d}:00"
            if time_str not in existing_hours:
                # 触发熔断检查
                if current_usage >= QUOTA_LIMIT:
                    print("Warning: API quota limit reached. Stopping repair.")
                    break
                
                # 4. 调用 TimeMachine 接口
                dt_obj = datetime.strptime(f"{target_date} {time_str}", '%Y-%m-%d %H:%M')
                dt_obj = dt_obj.replace(tzinfo=timezone(timedelta(hours=8)))
                timestamp = int(dt_obj.timestamp())

                try:
                    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={town['lat']}&lon={town['lon']}&dt={timestamp}&appid={api_key}&units=metric&lang=zh_cn"
                    resp = requests.get(url, timeout=15).json()
                    current_usage = update_quota(1)

                    if 'data' in resp and len(resp['data']) > 0:
                        data = resp['data'][0]
                        record = {
                            "town_name": town["town_name"],
                            "town_id": town["town_id"],
                            "date": target_date,
                            "time": time_str,
                            "temp": data.get('temp', 0),
                            "pressure": data.get('pressure', 0),
                            "humidity": data.get('humidity', 0),
                            "dew_point": data.get('dew_point', 0),
                            "clouds": data.get('clouds', 0),
                            "uvi": data.get('uvi', 0),
                            "visibility": data.get('visibility', 0),
                            "wind_speed": data.get('wind_speed', 0),
                            "wind_gust": data.get('wind_gust', data.get('wind_speed', 0)),
                            "wind_deg": data.get('wind_deg', 0),
                            "rain": data.get('rain', 0) if isinstance(data.get('rain'), (int, float)) else data.get('rain', {}).get('1h', 0),
                            "snow": data.get('snow', 0) if isinstance(data.get('snow'), (int, float)) else data.get('snow', {}).get('1h', 0),
                            "weather_desc": data.get('weather', [{}])[0].get('description', '未知'),
                            "flag": 0
                        }
                        new_repairs.append(record)
                        print(f"Repaired: {town['town_name']} @ {time_str}")
                    
                    time.sleep(0.1)

                except Exception as e:
                    print(f"Error fetching repair data for {town['town_id']} at {time_str}: {e}")
        
        if current_usage >= QUOTA_LIMIT: break

    # 5. 合并、整理并保存 CSV (强制执行去重和排序)
    if new_repairs:
        df_new = pd.DataFrame(new_repairs)
        df_final = pd.concat([df_history, df_new])
        print(f"Successfully repaired {len(new_repairs)} records.")
    else:
        df_final = df_history
        print("No missing records found for yesterday.")

    # 物理清洁：去重（保留最新的修复点）并按时间排序
    df_final = df_final.drop_duplicates(subset=['town_id', 'date', 'time'], keep='last')
    df_final = df_final.sort_values(by=['date', 'time', 'town_id'], ascending=[True, True, True])
    
    # 持久化 CSV
    df_final.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')

    # 6. 重建 JSON 缓存 (最近 720 小时滚动)
    print("Rebuilding 2026.json cache (720h rolling)...")
    
    # 转换为 datetime 以便计算时间窗口
    df_final['dt_temp'] = pd.to_datetime(df_final['date'] + ' ' + df_final['time'])
    
    # 计算北京时间 720 小时前的截止点
    cutoff_time = bj_now.replace(tzinfo=None) - timedelta(hours=720)
    
    # 截取数据并按 town_id 分组
    df_rolling = df_final[df_final['dt_temp'] >= cutoff_time].copy()
    df_rolling = df_rolling.drop(columns=['dt_temp'])
    
    grouped_json = {}
    for town_id, group in df_rolling.groupby('town_id'):
        grouped_json[str(town_id)] = group.to_dict('records')
        
    # 覆盖写入 JSON
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(grouped_json, f, ensure_ascii=False, indent=2)
    
    print(f"Repair process complete. 2026.json updated with records since {cutoff_time.strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    repair_weather()