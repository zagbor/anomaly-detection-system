"""
Модуль извлечения признаков из MQTT-трафика
Создаёт временные окна и вычисляет статистические признаки
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FeatureExtractor:
    """Извлекает признаки из MQTT-сообщений для обучения ML-модели"""
    
    def __init__(self, window_size_seconds: int = 60):
        """
        Инициализация экстрактора признаков
        
        Args:
            window_size_seconds: Размер временного окна в секундах
        """
        self.window_size = timedelta(seconds=window_size_seconds)
        
        # Буфер сообщений для каждого устройства
        self.message_buffers: Dict[str, List[Dict]] = defaultdict(list)
        
        # Кэш вычисленных признаков
        self.feature_cache: Dict[str, List[Dict]] = defaultdict(list)
    
    def add_message(self, device_id: str, tag_name: str, value: float, 
                   timestamp: datetime, message_type: str = "DDATA"):
        """
        Добавить сообщение в буфер
        
        Args:
            device_id: ID устройства
            tag_name: Имя тега
            value: Значение тега
            timestamp: Время получения сообщения
            message_type: Тип сообщения (DDATA, NBIRTH, и т.д.)
        """
        message = {
            "device_id": device_id,
            "tag_name": tag_name,
            "value": value,
            "timestamp": timestamp,
            "message_type": message_type
        }
        
        self.message_buffers[device_id].append(message)
        
        # Удаляем старые сообщения за пределами окна
        self._cleanup_old_messages(device_id, timestamp)
    
    def _cleanup_old_messages(self, device_id: str, current_time: datetime):
        """
        Удалить сообщения старше временного окна
        
        Args:
            device_id: ID устройства
            current_time: Текущее время
        """
        cutoff_time = current_time - self.window_size
        
        # Фильтруем сообщения
        self.message_buffers[device_id] = [
            msg for msg in self.message_buffers[device_id]
            if msg["timestamp"] > cutoff_time
        ]
    
    def extract_features(self, device_id: str, current_time: Optional[datetime] = None) -> Optional[Dict]:
        """
        Извлечь признаки для устройства за текущее временное окно
        
        Args:
            device_id: ID устройства
            current_time: Текущее время (если None, используется текущее)
            
        Returns:
            Словарь с признаками или None если нет данных
        """
        if current_time is None:
            current_time = datetime.now()
        
        # Очистка старых сообщений
        self._cleanup_old_messages(device_id, current_time)
        
        messages = self.message_buffers[device_id]
        
        if not messages:
            return None
        
        # Группировка сообщений по тегам
        tag_messages: Dict[str, List[Dict]] = defaultdict(list)
        for msg in messages:
            tag_messages[msg["tag_name"]].append(msg)
        
        # Вычисление признаков
        features = {
            "device_id": device_id,
            "window_start": current_time - self.window_size,
            "window_end": current_time,
            "message_count": len(messages),
            "unique_tags": len(tag_messages),
            "tags": {}
        }
        
        # Признаки для каждого тега
        for tag_name, msgs in tag_messages.items():
            values = [msg["value"] for msg in msgs]
            timestamps = [msg["timestamp"] for msg in msgs]
            
            tag_features = {
                "count": len(values),
                "mean": np.mean(values),
                "std": np.std(values),
                "min": np.min(values),
                "max": np.max(values),
                "median": np.median(values),
                "range": np.max(values) - np.min(values)
            }
            
            # Признаки временных интервалов
            if len(timestamps) > 1:
                intervals = [(timestamps[i+1] - timestamps[i]).total_seconds() 
                            for i in range(len(timestamps)-1)]
                tag_features["interval_mean"] = np.mean(intervals)
                tag_features["interval_std"] = np.std(intervals)
                tag_features["interval_min"] = np.min(intervals)
                tag_features["interval_max"] = np.max(intervals)
            else:
                tag_features["interval_mean"] = 0
                tag_features["interval_std"] = 0
                tag_features["interval_min"] = 0
                tag_features["interval_max"] = 0
            
            features["tags"][tag_name] = tag_features
        
        # Глобальные признаки устройства
        all_values = [msg["value"] for msg in messages]
        if all_values:
            features["global_mean"] = np.mean(all_values)
            features["global_std"] = np.std(all_values)
            features["global_min"] = np.min(all_values)
            features["global_max"] = np.max(all_values)
        
        return features
    
    def extract_feature_vector(self, device_id: str, current_time: Optional[datetime] = None) -> Optional[np.ndarray]:
        """
        Извлечь вектор признаков для ML-модели
        
        Args:
            device_id: ID устройства
            current_time: Текущее время
            
        Returns:
            Вектор признаков (numpy array) или None
        """
        features = self.extract_features(device_id, current_time)
        
        if features is None:
            return None
        
        # Формируем вектор из признаков
        vector_parts = [
            features["message_count"],
            features["unique_tags"],
            features.get("global_mean", 0),
            features.get("global_std", 0),
            features.get("global_min", 0),
            features.get("global_max", 0),
        ]
        
        # Добавляем признаки тегов (усреднённые по всем тегам)
        tag_means = []
        tag_stds = []
        tag_counts = []
        
        for tag_name, tag_features in features["tags"].items():
            tag_means.append(tag_features["mean"])
            tag_stds.append(tag_features["std"])
            tag_counts.append(tag_features["count"])
        
        if tag_means:
            vector_parts.extend([
                np.mean(tag_means),
                np.mean(tag_stds),
                np.mean(tag_counts),
                np.max(tag_means),
                np.max(tag_stds),
            ])
        else:
            vector_parts.extend([0, 0, 0, 0, 0])
        
        return np.array(vector_parts)
    
    def get_feature_names(self) -> List[str]:
        """
        Получить имена признаков
        
        Returns:
            Список имён признаков
        """
        return [
            "message_count",
            "unique_tags",
            "global_mean",
            "global_std",
            "global_min",
            "global_max",
            "tag_mean_avg",
            "tag_std_avg",
            "tag_count_avg",
            "tag_mean_max",
            "tag_std_max",
        ]
    
    def clear_device(self, device_id: str):
        """
        Очистить буфер сообщений для устройства
        
        Args:
            device_id: ID устройства
        """
        if device_id in self.message_buffers:
            del self.message_buffers[device_id]
        if device_id in self.feature_cache:
            del self.feature_cache[device_id]
    
    def clear_all(self):
        """Очистить все буферы"""
        self.message_buffers.clear()
        self.feature_cache.clear()
    
    def get_buffer_info(self) -> Dict[str, int]:
        """
        Получить информацию о буферах
        
        Returns:
            Словарь с количеством сообщений по устройствам
        """
        return {device_id: len(messages) 
                for device_id, messages in self.message_buffers.items()}


def main():
    """Тестовая функция для проверки экстрактора признаков"""
    print("=== Тест экстрактора признаков ===")
    
    extractor = FeatureExtractor(window_size_seconds=60)
    
    # Симуляция сообщений
    now = datetime.now()
    
    # Добавляем сообщения для устройства Boiler1
    for i in range(10):
        extractor.add_message(
            device_id="Boiler1",
            tag_name="Temperature",
            value=150 + i * 2 + np.random.randn(),
            timestamp=now - timedelta(seconds=60 - i * 5),
            message_type="DDATA"
        )
    
    # Добавляем сообщения для другого тега
    for i in range(5):
        extractor.add_message(
            device_id="Boiler1",
            tag_name="Pressure",
            value=12 + i * 0.1 + np.random.randn() * 0.5,
            timestamp=now - timedelta(seconds=60 - i * 10),
            message_type="DDATA"
        )
    
    # Извлечение признаков
    features = extractor.extract_features("Boiler1", now)
    
    print(f"\nПризнаки для устройства Boiler1:")
    print(f"  Количество сообщений: {features['message_count']}")
    print(f"  Уникальных тегов: {features['unique_tags']}")
    print(f"  Глобальное среднее: {features.get('global_mean', 0):.2f}")
    print(f"  Глобальное std: {features.get('global_std', 0):.2f}")
    
    print(f"\nПризнаки по тегам:")
    for tag_name, tag_features in features["tags"].items():
        print(f"  {tag_name}:")
        print(f"    Среднее: {tag_features['mean']:.2f}")
        print(f"    Std: {tag_features['std']:.2f}")
        print(f"    Min/Max: {tag_features['min']:.2f}/{tag_features['max']:.2f}")
    
    # Вектор признаков
    vector = extractor.extract_feature_vector("Boiler1", now)
    print(f"\nВектор признаков ({len(vector)} элементов):")
    print(f"  {vector}")
    
    # Имена признаков
    print(f"\nИмена признаков:")
    for i, name in enumerate(extractor.get_feature_names()):
        print(f"  {i}: {name}")
    
    # Информация о буферах
    buffer_info = extractor.get_buffer_info()
    print(f"\nИнформация о буферах:")
    for device_id, count in buffer_info.items():
        print(f"  {device_id}: {count} сообщений")


if __name__ == "__main__":
    main()
