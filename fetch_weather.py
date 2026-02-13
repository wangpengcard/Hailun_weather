import os
import requests
import json
import csv
import time
from datetime import datetime

# 配置信息
API_KEY = os.getenv('OWM_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/2.5/onecall"

def fetch_weather_data():
    # 读取旧历史数据
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
            print(f"同步 {name} 数据...")
            response = requests.get(BASE_URL, params=params)
            data = response.json()
            
            # --- 实时数据处理 ---
            curr = data.get('current', {})
            # 定义标准表头字段
            def get_standard_entry(item, is_forecast=False):
                return {
                    "town_name": name,
                    "town_id": town_id,
                    "date": datetime.fromtimestamp(item.get('dt')).strftime('%Y-%m-%d'),
                    "time": datetime.fromtimestamp(item.get('dt')).strftime('%H:%M'),
                    "temp": item.get('temp'),
                    "pressure": item.get('pressure'),
                    "humidity": item.get('humidity'),
                    "dew_point": item.get('dew_point'),
                    "clouds": item.get('clouds'),
                    "uvi": item.get('uvi'),
                    "visibility": item.get('visibility', 10000), # 预测数据若无则默认10000
                    "wind_speed": item.get('wind_speed'),
                    "wind_gust": item.get('wind_gust', item.get('wind_speed')), # 预测若无阵风用风速代替
                    "wind_deg": item.get('wind_deg'),
                    "rain": item.get('rain', {}).get('1h', 0) if isinstance(item.get('rain'), dict) else item.get('rain', 0),
                    "snow": item.get('snow', {}).get('1h', 0) if isinstance(item.get('snow'), dict) else item.get('snow', 0),
                    "weather_desc": item.get('weather', [{}])[0].get('description', ''),
                    "pop": int(item.get('pop', 0) * 100) if is_forecast else 0, # 只有预测有降水概率
                    "flag": 1 if is_forecast else 0
                }

            # 写入历史记录
            if town_id not in realtime_map: realtime_map[town_id] = []
            realtime_map[town_id].append(get_standard_entry(curr, False))
            realtime_map[town_id] = realtime_map[town_id][-720:]
            
            # --- 预测数据处理 ---
            f_list = []
            for hour in data.get('hourly', [])[:48]:
                f_list.append(get_standard_entry(hour, True))
            forecast_map[town_id] = f_list
            
            time.sleep(0.3)
        except Exception as e:
            print(f"{name} 失败: {e}")

    # 保存
    with open('2026.json', 'w', encoding='utf-8') as f:
        json.dump(realtime_map, f, ensure_ascii=False, separators=(',', ':'))
    with open('forecasts.json', 'w', encoding='utf-8') as f:
        json.dump(forecast_map, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__":
    fetch_weather_data()
