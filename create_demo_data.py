"""
Скрипт для создания демо-данных для системы обнаружения аномалий
"""

import sys
import os
from datetime import datetime, timedelta
import random

# Добавляем src в путь
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from storage.db_manager import DatabaseManager, AnomalyRecord

def create_demo_data():
    """Создать демо-данные для демонстрации"""
    
    # Инициализация базы данных
    db = DatabaseManager("data/anomalies.db")
    
    # Регистрация устройств
    devices = [
        ("device_001", "edge_node_1", "group_A"),
        ("device_002", "edge_node_1", "group_A"),
        ("device_003", "edge_node_2", "group_B"),
        ("device_004", "edge_node_2", "group_B"),
        ("device_005", "edge_node_3", "group_C"),
    ]
    
    for device_id, node_id, group_id in devices:
        db.register_device(device_id, node_id, group_id)
        print(f"Зарегистрировано устройство: {device_id}")
    
    # Создание аномалий за последние 24 часа
    anomaly_types = ["severe_outlier", "moderate_outlier", "mild_outlier"]
    severities = ["critical", "high", "medium", "low"]
    tag_names = ["temperature", "pressure", "vibration", "flow_rate", "power"]
    
    now = datetime.now()
    
    # Создаём аномалии за последние 24 часа
    for hours_ago in range(24, 0, -1):
        base_time = now - timedelta(hours=hours_ago)
        
        # Количество аномалий в час (случайное от 0 до 5)
        num_anomalies = random.randint(0, 5)
        
        for _ in range(num_anomalies):
            device_id = random.choice([d[0] for d in devices])
            tag_name = random.choice(tag_names)
            anomaly_type = random.choice(anomaly_types)
            severity = random.choice(severities)
            
            # Score зависит от严重程度
            if severity == "critical":
                score = random.uniform(-1.0, -0.5)
            elif severity == "high":
                score = random.uniform(-0.5, -0.2)
            elif severity == "medium":
                score = random.uniform(-0.2, -0.05)
            else:
                score = random.uniform(-0.05, 0.0)
            
            # Случайное время в пределах часа
            anomaly_time = base_time + timedelta(
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            anomaly = AnomalyRecord(
                timestamp=anomaly_time,
                device_id=device_id,
                tag_name=tag_name,
                anomaly_score=score,
                anomaly_type=anomaly_type,
                description=f"Обнаружена аномалия (score={score:.4f})",
                severity=severity
            )
            
            db.save_anomaly(anomaly)
    
    # Создаём несколько недавних аномалий
    for i in range(10):
        device_id = random.choice([d[0] for d in devices])
        tag_name = random.choice(tag_names)
        anomaly_type = random.choice(anomaly_types)
        severity = random.choice(severities)
        
        if severity == "critical":
            score = random.uniform(-1.0, -0.5)
        elif severity == "high":
            score = random.uniform(-0.5, -0.2)
        elif severity == "medium":
            score = random.uniform(-0.2, -0.05)
        else:
            score = random.uniform(-0.05, 0.0)
        
        anomaly_time = now - timedelta(minutes=random.randint(0, 30))
        
        anomaly = AnomalyRecord(
            timestamp=anomaly_time,
            device_id=device_id,
            tag_name=tag_name,
            anomaly_score=score,
            anomaly_type=anomaly_type,
            description=f"Обнаружена аномалия (score={score:.4f})",
            severity=severity
        )
        
        db.save_anomaly(anomaly)
    
    # Вывод статистики
    stats = db.get_anomaly_statistics(hours=24)
    print(f"\n=== Статистика демо-данных ===")
    print(f"Всего аномалий за 24 часа: {stats['total']}")
    print(f"По严重程度:")
    for severity, count in stats['by_severity'].items():
        print(f"  {severity}: {count}")
    print(f"По типу:")
    for anomaly_type, count in stats['by_type'].items():
        print(f"  {anomaly_type}: {count}")
    
    devices_list = db.get_devices()
    print(f"\nЗарегистрировано устройств: {len(devices_list)}")
    
    print("\nДемо-данные успешно созданы!")
    print("Теперь можно запустить веб-интерфейс: cd src/web && python3 app.py")

if __name__ == "__main__":
    create_demo_data()
