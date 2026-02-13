import os
import requests
import json
import csv
import time
from datetime import datetime

API_KEY = os.getenv('OWM_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/2.5/onecall"

def fetch_weather_data():
    # --- 改进：先读取现有数据，实现历史累加 ---
    if os.path.exists('2026.json'):
        try:
            with open('2026.json', 'r', encoding='utf-8') as f:
                realtime_map = json.load(f)
        except:
            realtime_map = {}
    else:
        realtime_map = {}

    forecast_map = {}
    
    try:
        with open('towns.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader) 
            towns = list(reader)
    except Exception as e:
        print(f"读取 towns.csv 失败: {e}")
        return

    for row in towns:
        if len(row) < 5: continue
        name, town_id, lon, lat = row[1], row[2], row[3], row[4]
        
        params = {
            "lat": lat, "lon": lon,
            "appid": API_KEY, "units": "metric",
            "exclude": "minutely,alerts", "lang": "zh_cn"
        }
        
        try:
            print(f"正在抓取 {name} 的气象数据...")
            response = requests.get(BASE_URL, params=params)
            data = response.json()
            
            # --- 改进：补全 V4 前端需要的参数 ---
            current = data.get('current', {})
            realtime_entry = {
                "date": datetime.fromtimestamp(current.get('dt')).strftime('%Y-%m-%d'),
                "time": datetime.fromtimestamp(current.get('dt')).strftime('%H:%M'),
                "temp": current.get('temp'),
                "rain": data.get('hourly', [{}])[0].get('rain', {}).get('1h', 0), # 1小时降雨
                "wind_speed": current.get('wind_speed'),
                "wind_gust": current.get('wind_gust'),
                "wind_deg": current.get('wind_deg'),
                "humidity": current.get('humidity'),
                "pressure": current.get('pressure'),
                "dew_point": current.get('dew_point'),
                "uvi": current.get('uvi'),
                "visibility": current.get('visibility'),
                "clouds": current.get('clouds'),
                "weather_desc": current.get('weather', [{}])[0].get('description', ''),
                "flag": 0
            }
            
            if town_id not in realtime_map: realtime_map[town_id] = []
            realtime_map[town_id].append(realtime_entry)
            
            # 限制历史长度：每个乡镇保留最近 7 天（168条）记录，防止文件无限变大
            realtime_map[town_id] = realtime_map[town_id][-168:]
            
            # --- 处理预测数据 ---
            f_list = []
            for hour in data.get('hourly', [])[:48]:
                f_list.append({
                    "date": datetime.fromtimestamp(hour.get('dt')).strftime('%Y-%m-%d'),
                    "time": datetime.fromtimestamp(hour.get('dt')).strftime('%H:%M'),
                    "temp": hour.get('temp'),
                    "rain": hour.get('rain', {}).get('1h', 0),
                    "pop": int(hour.get('pop', 0) * 100),
                    "flag": 1
                })
            forecast_map[town_id] = f_list
            
            time.sleep(0.2)
        except Exception as e:
            print(f"抓取 {name} 失败: {e}")

    # 保存 JSON
    with open('2026.json', 'w', encoding='utf-8') as f:
        json.dump(realtime_map, f, ensure_ascii=False, separators=(',', ':'))
    with open('forecasts.json', 'w', encoding='utf-8') as f:
        json.dump(forecast_map, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__":
    fetch_weather_data()
