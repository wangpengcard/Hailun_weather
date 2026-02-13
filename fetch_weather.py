import os
import requests
import json
import csv
import time
from datetime import datetime

# 1. 配置信息（从 GitHub Secrets 读取）
API_KEY = os.getenv('OWM_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/2.5/onecall"

def fetch_weather_data():
    realtime_map = {}
    forecast_map = {}
    
    # 2. 读取乡镇列表 (towns.csv)
    # 假设格式：序号,乡镇名,ID,经度,纬度
    try:
        with open('towns.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过表头
            towns = list(reader)
    except Exception as e:
        print(f"读取 towns.csv 失败: {e}")
        return

    # 3. 循环请求天气 API
    for row in towns:
        if len(row) < 5: continue
        name, town_id, lon, lat = row[1], row[2], row[3], row[4]
        
        params = {
            "lat": lat,
            "lon": lon,
            "appid": API_KEY,
            "units": "metric",
            "exclude": "minutely,alerts",
            "lang": "zh_cn"
        }
        
        try:
            print(f"正在抓取 {name} 的气象数据...")
            response = requests.get(BASE_URL, params=params)
            data = response.json()
            
            # --- 处理实时数据 ---
            current = data.get('current', {})
            realtime_entry = {
                "time": datetime.fromtimestamp(current.get('dt')).strftime('%H:%M'),
                "temp": current.get('temp'),
                "humidity": current.get('humidity'),
                "pop": int(data.get('hourly', [{}])[0].get('pop', 0) * 100) # 拿第一个小时的降水概率
            }
            
            # 为了看板显示历史，我们需要读取旧数据并追加
            # 简化版：这里演示直接存储，实际运行中 Actions 会合并文件
            if town_id not in realtime_map: realtime_map[town_id] = []
            realtime_map[town_id].append(realtime_entry)
            
            # --- 处理 48 小时预测数据 ---
            forecast_list = []
            for hour in data.get('hourly', [])[:48]:
                forecast_list.append({
                    "time": datetime.fromtimestamp(hour.get('dt')).strftime('%H:%M'),
                    "temp": hour.get('temp'),
                    "pop": int(hour.get('pop', 0) * 100)
                })
            forecast_map[town_id] = forecast_list
            
            # 防止请求过快被封
            time.sleep(0.2)
            
        except Exception as e:
            print(f"抓取 {name} 失败: {e}")

    # 4. 改造点：使用紧凑模式保存 JSON，提升前端在 Pages 上的加载速度
    # separators=(',', ':') 删除了空格和缩进，大幅减小体积
    with open('2026.json', 'w', encoding='utf-8') as f:
        json.dump(realtime_map, f, ensure_ascii=False, separators=(',', ':'))

    with open('forecasts.json', 'w', encoding='utf-8') as f:
        json.dump(forecast_map, f, ensure_ascii=False, separators=(',', ':'))

    print(f"所有数据同步完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    if not API_KEY:
        print("错误：未设置 OWM_API_KEY 环境变量")
    else:
        fetch_weather_data()
