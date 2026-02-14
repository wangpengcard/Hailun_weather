import os
import requests
import json
import csv
import time
from datetime import datetime

# 1. 配置信息
API_KEY = os.getenv('OWM_API_KEY')
# 必须使用 3.0 接口
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"

def fetch_weather_data():
    # --- A. 数据初始化 ---
    if os.path.exists('2026.json'):
        try:
            with open('2026.json', 'r', encoding='utf-8') as f:
                realtime_map = json.load(f)
        except:
            realtime_map = {}
    else:
        realtime_map = {}

    forecast_map = {}

    # --- B. 读取乡镇列表 ---
    try:
        with open('towns.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader) # 跳过表头
            towns_list = list(reader)
    except Exception as e:
        print(f"读取 towns.csv 失败: {e}")
        return

    # --- C. 核心提取逻辑函数 ---
    def get_standard_entry(item, name, town_id, is_forecast=False):
        """
        严格遵循官方 API 3.0 字段提取规则
        """
        # 1. 处理降水 (rain.1h) 和 降雪 (snow.1h)
        # 规则：存在则取值，不存在则记为 0
        rain_val = 0
        if 'rain' in item:
            rain_val = item['rain'].get('1h', 0) if isinstance(item['rain'], dict) else item['rain']
        
        snow_val = 0
        if 'snow' in item:
            snow_val = item['snow'].get('1h', 0) if isinstance(item['snow'], dict) else item['snow']

        # 2. 处理阵风 (wind_gust)
        # 规则：存在则取值，不存在则记为 0
        wind_gust = item.get('wind_gust', 0)

        # 3. 处理能见度 (visibility)
        # 规则：存在则取值，不存在则记为官方最大值 10000
        visibility = item.get('visibility', 10000)

        # 4. 获取中文描述 (lang=zh_cn)
        weather_list = item.get('weather', [])
        desc = weather_list[0].get('description', '') if weather_list else ''

        # 5. 构建输出字典 (排除 hourly.pop)
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
            "visibility": visibility,
            "wind_speed": item.get('wind_speed'),
            "wind_gust": wind_gust,
            "wind_deg": item.get('wind_deg'),
            "rain": rain_val,
            "snow": snow_val,
            "weather_desc": desc,
            "flag": 1 if is_forecast else 0
        }

    # --- D. 循环请求 API ---
    for row in towns_list:
        if len(row) < 5: continue
        name, town_id, lat, lon = row[1], row[2], row[3], row[4]
        
        params = {
            "lat": lat, "lon": lon,
            "appid": API_KEY, "units": "metric",
            "exclude": "minutely,daily,alerts", "lang": "zh_cn"
        }
        
        try:
            print(f"正在同步 {name} 的气象数据...")
            response = requests.get(BASE_URL, params=params)
            
            # 严格检查状态码
            if response.status_code != 200:
                print(f"!!! {name} 请求失败 (状态码 {response.status_code}): {response.text}")
                continue
                
            data = response.json()

            # 1. 处理实时记录 (追加到 realtime_map)
            curr = data.get('current')
            if curr:
                if town_id not in realtime_map: 
                    realtime_map[town_id] = []
                
                realtime_map[town_id].append(get_standard_entry(curr, name, town_id, False))
                # 保持 720 条历史记录 (D30算法需要)
                realtime_map[town_id] = realtime_map[town_id][-720:]
            
            # 2. 处理逐小时预测数据 (重写 forecast_map)
            hourly = data.get('hourly', [])
            if hourly:
                f_list = []
                for hour in hourly[:48]: # 提取未来48小时
                    f_list.append(get_standard_entry(hour, name, town_id, True))
                forecast_map[town_id] = f_list
            
            # 延时避开频率限制 (One Call 3.0 建议)
            time.sleep(0.2)
            
        except Exception as e:
            print(f"{name} 运行时发生异常: {e}")

    # --- E. 写入文件 ---
    # 写入实时追加数据
    with open('2026.json', 'w', encoding='utf-8') as f:
        json.dump(realtime_map, f, ensure_ascii=False, separators=(',', ':'))

    # 写入预测数据
    with open('forecasts.json', 'w', encoding='utf-8') as f:
        json.dump(forecast_map, f, ensure_ascii=False, separators=(',', ':'))

    print(f"--- 同步任务结束 ---")
    print(f"成功更新实时点: {len(realtime_map)} | 预测点: {len(forecast_map)}")

if __name__ == "__main__":
    if not API_KEY:
        print("错误：未找到 OWM_API_KEY，请检查 GitHub Secrets 配置。")
    else:
        fetch_weather_data()

