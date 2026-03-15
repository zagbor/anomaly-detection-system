"""
Генератор тестовых MQTT сообщений в формате Sparkplug B
"""

import paho.mqtt.client as mqtt
import time
import random
import json
from datetime import datetime

# Создаём клиент
client = mqtt.Client(client_id="data_generator")

# Подключаемся к локальному брокеру
host = "127.0.0.1"
port = 1883

print(f"Подключение к {host}:{port}...")
client.connect(host, port, 60)
client.loop_start()

# Ждём подключения
time.sleep(1)

# Генерируем сообщения для 5 устройств
devices = ["device_001", "device_002", "device_003", "device_004", "device_005"]
tags = ["temperature", "pressure", "vibration", "flow_rate", "power"]

print("Начинаем генерацию сообщений...")

try:
    for i in range(100):
        device_id = random.choice(devices)
        tag_name = random.choice(tags)
        
        # Генерируем значение с небольшими вариациями
        base_value = {
            "temperature": 25.0,
            "pressure": 100.0,
            "vibration": 0.5,
            "flow_rate": 50.0,
            "power": 1000.0
        }[tag_name]
        
        # Добавляем случайную вариацию
        value = base_value + random.uniform(-5, 5)
        
        # Создаём сообщение в формате Sparkplug B (упрощённый JSON)
        message = {
            "timestamp": datetime.now().isoformat(),
            "metric": [
                {
                    "name": tag_name,
                    "value": value,
                    "timestamp": int(time.time() * 1000)
                }
            ]
        }
        
        # Публикуем сообщение
        topic = f"spBv1.0/Group_A/DDATA/{device_id}"
        client.publish(topic, json.dumps(message))
        
        # Иногда создаём аномалию
        if random.random() < 0.1:  # 10% шанс аномалии
            anomaly_value = base_value + random.choice([-50, 50])
            message["metric"][0]["value"] = anomaly_value
            client.publish(topic, json.dumps(message))
            print(f"Аномалия: {device_id} - {tag_name} = {anomaly_value}")
        
        if i % 10 == 0:
            print(f"Отправлено {i} сообщений...")
        
        time.sleep(0.5)
    
    print("Генерация завершена!")
    
except KeyboardInterrupt:
    print("\nГенерация остановлена")

client.loop_stop()
client.disconnect()
