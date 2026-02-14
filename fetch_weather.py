import os
import requests
import json
import csv
import time
from datetime import datetime

# 1. 配置信息 (从 GitHub Secrets 读取)
API_KEY = os.getenv('OWM_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/2.5/onecall"

def fetch_weather_data():
    # 读取旧历史数据，实现历史追加
    if os.path.exists('2026.json'):
        try:
            with open('2026.json', 'r', encoding='utf-8') as f:
                realtime_map = json.load(f)
        except:
            realtime_map = {}
    else:
        realtime_map = {}

    forecast_map = {}
    
    # 2. 读取乡镇列表
    try:
        with open('towns.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader) 
            towns_list = list(reader)
    except Exception as e:
        print(f"读取 towns.csv 失败: {e}")
        return

    # 3. 循环请求 API
    for row in towns_list:
        if len(row) < 5: continue
        name, town_id, lon, lat = row[1], row[2], row[3], row[4]
        
        params = {
            "lat": lat, "lon": lon,
            "appid": API_KEY, "units": "metric",
            "exclude": "minutely,alerts", "lang": "zh_cn"
        }
        
        try:
            print(f"正在同步 {name} 的气象数据...")
            response = requests.get(BASE_URL, params=params)
            data = response.json()
            
            # API 状态检查
            if "cod" in data and str(data["cod"]) != "200":
                print(f"{name} API 报错: {data.get('message')}")
                continue

            # --- 严格遵循官方字段提取函数 (已移除 pop) ---
            def get_standard_entry(item, is_forecast=False):
                # 处理 rain (官方格式: {"1h": mm})
                rain_val = 0
                if 'rain' in item:
                    rain_val = item['rain'].get('1h', 0) if isinstance(item['rain'], dict) else item['rain']

                # 处理 snow (官方格式: {"1h": mm})
                snow_val = 0
                if 'snow' in item:
                    snow_val = item['snow'].get('1h', 0) if isinstance(item['snow'], dict) else item['snow']

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
                    "visibility": item.get('visibility', 10000), # 官方上限10km
                    "wind_speed": item.get('wind_speed'),
                    "wind_gust": item.get('wind_gust', item.get('wind_speed')), # 官方标注 where available
                    "wind_deg": item.get('wind_deg'),
                    "rain": rain_val,
                    "snow": snow_val,
                    "weather_desc": item.get('weather', [{}])[0].get('description', ''),
                    "flag": 1 if is_forecast else 0
                }

            # 处理实时记录 (写入 2026.json)
            curr = data.get('current', {})
            if curr:
                if town_id not in realtime_map: realtime_map[town_id] = []
                realtime_map[town_id].append(get_standard_entry(curr, False))
                # 保持 720 条历史记录 (D30算法需要)
                realtime_map[town_id] = realtime_map[town_id][-720:]
            
            # 处理预测数据 (写入 forecasts.json)
            hourly = data.get('hourly', [])
            if hourly:
                f_list = []
                for hour in hourly[:48]:
                    f_list.append(get_standard_entry(hour, True))
                forecast_map[town_id] = f_list
            
            # 延时避开频率限制
            time.sleep(0.3)
            
        except Exception as e:
            print(f"{name} 运行失败: {e}")

    # 4. 写入压缩文件
    with open('2026.json', 'w', encoding='utf-8') as f:
        json.dump(realtime_map, f, ensure_ascii=False, separators=(',', ':'))

    with open('forecasts.json', 'w', encoding='utf-8') as f:
        json.dump(forecast_map, f, ensure_ascii=False, separators=(',', ':'))

    print(f"同步完成。监控点: {len(realtime_map)} | 预测点: {len(forecast_map)}")

if __name__ == "__main__":
    if not API_KEY:
        print("错误：未找到 OWM_API_KEY")
    else:
        fetch_weather_data()
