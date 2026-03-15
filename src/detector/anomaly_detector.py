"""
Главный модуль детектора аномалий
Объединяет все компоненты системы
"""

import sys
import os
import logging
from datetime import datetime
from typing import Optional

# Добавляем родительскую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from capture.mqtt_client import TrafficCapture
from features.extractor import FeatureExtractor
from ml.model import AnomalyDetector
from storage.db_manager import DatabaseManager, AnomalyRecord

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AnomalyDetectionSystem:
    """Главная система обнаружения аномалий"""
    
    def __init__(
        self,
        broker_host: str = "broker.hivemq.com",
        broker_port: int = 1883,
        topic_pattern: str = "spBv1.0/#",
        db_path: str = "data/anomalies.db",
        model_path: str = "models/isolation_forest.pkl",
        window_size_seconds: int = 60,
        contamination: float = 0.05
    ):
        """
        Инициализация системы
        
        Args:
            broker_host: Хост MQTT-брокера
            broker_port: Порт MQTT-брокера
            topic_pattern: Паттерн топиков для подписки
            db_path: Путь к базе данных
            model_path: Путь к модели ML
            window_size_seconds: Размер временного окна
            contamination: Ожидаемая доля аномалий
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic_pattern = topic_pattern
        
        # Инициализация компонентов
        self.capture = TrafficCapture(broker_host, broker_port, topic_pattern)
        self.extractor = FeatureExtractor(window_size_seconds)
        self.detector = AnomalyDetector(contamination=contamination, model_path=model_path)
        self.db = DatabaseManager(db_path)
        
        # Статистика
        self.anomalies_detected = 0
        self.messages_processed = 0
        self.start_time = None
        
        logger.info("Система обнаружения аномалий инициализирована")
    
    def _parse_sparkplug_topic(self, topic: str) -> Optional[dict]:
        """
        Парсинг топика Sparkplug B
        
        Формат: spBv1.0/Group_ID/Message_Type/Edge_Node_ID/Device_ID
        
        Args:
            topic: Топик MQTT
            
        Returns:
            Словарь с компонентами топика или None
        """
        parts = topic.split('/')
        
        if len(parts) < 5 or parts[0] != "spBv1.0":
            return None
        
        return {
            "version": parts[0],
            "group_id": parts[1],
            "message_type": parts[2],
            "node_id": parts[3],
            "device_id": parts[4] if len(parts) > 4 else None
        }
    
    def _process_message(self, msg):
        """
        Обработка входящего MQTT-сообщения
        
        Args:
            msg: Объект mqtt.MQTTMessage
        """
        try:
            self.messages_processed += 1
            
            # Парсинг топика
            topic_info = self._parse_sparkplug_topic(msg.topic)
            
            if topic_info is None:
                return
            
            device_id = topic_info["device_id"]
            message_type = topic_info["message_type"]
            
            # Регистрация устройства в БД
            if device_id:
                self.db.register_device(
                    device_id=device_id,
                    node_id=topic_info["node_id"],
                    group_id=topic_info["group_id"]
                )
            
            # Обработка только сообщений с данными (DDATA)
            if message_type != "DDATA":
                return
            
            # TODO: Парсинг payload Sparkplug B (Protobuf)
            # Для простоты сейчас предполагаем, что payload содержит значение
            try:
                value = float(msg.payload.decode('utf-8'))
                tag_name = "unknown"  # TODO: Извлечь из Protobuf
                
                # Добавление сообщения в экстрактор
                self.extractor.add_message(
                    device_id=device_id,
                    tag_name=tag_name,
                    value=value,
                    timestamp=datetime.now(),
                    message_type=message_type
                )
                
                # Проверка на аномалию каждые 10 сообщений
                if self.messages_processed % 10 == 0 and device_id:
                    self._check_for_anomalies(device_id)
                
            except (ValueError, UnicodeDecodeError):
                # Payload не является числом или не декодируется
                pass
            
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
    
    def _check_for_anomalies(self, device_id: str):
        """
        Проверить устройство на наличие аномалий
        
        Args:
            device_id: ID устройства
        """
        try:
            # Извлечение признаков
            features = self.extractor.extract_feature_vector(device_id)
            
            if features is None:
                return
            
            # Предсказание
            label, score = self.detector.predict_single(features)
            
            # Если аномалия
            if label == -1:
                self.anomalies_detected += 1
                
                # Определение типа и严重程度 аномалии
                probability = self.detector.get_anomaly_probability(score)
                
                if probability > 0.8:
                    severity = "critical"
                    anomaly_type = "severe_outlier"
                elif probability > 0.6:
                    severity = "high"
                    anomaly_type = "moderate_outlier"
                else:
                    severity = "medium"
                    anomaly_type = "mild_outlier"
                
                # Создание записи об аномалии
                anomaly_record = AnomalyRecord(
                    timestamp=datetime.now(),
                    device_id=device_id,
                    tag_name="multiple",  # TODO: Определить конкретный тег
                    anomaly_score=score,
                    anomaly_type=anomaly_type,
                    description=f"Обнаружена аномалия (score={score:.4f}, prob={probability:.2%})",
                    severity=severity
                )
                
                # Сохранение в БД
                self.db.save_anomaly(anomaly_record)
                
                logger.warning(
                    f"Аномалия обнаружена: device={device_id}, "
                    f"score={score:.4f}, severity={severity}"
                )
            
        except Exception as e:
            logger.error(f"Ошибка проверки аномалий: {e}")
    
    def start(self, training_samples: int = 1000):
        """
        Запустить систему
        
        Args:
            training_samples: Количество образцов для обучения модели
        """
        logger.info("Запуск системы обнаружения аномалий...")
        self.start_time = datetime.now()
        
        # Установка колбэка для обработки сообщений
        self.capture.set_message_callback(self._process_message)
        
        # Подключение к брокеру
        if not self.capture.connect():
            logger.error("Не удалось подключиться к брокеру")
            return False
        
        logger.info("Система запущена. Ожидание сообщений...")
        logger.info(f"Сбор {training_samples} образцов для обучения модели...")
        
        # Сбор данных для обучения
        import time
        while self.messages_processed < training_samples:
            time.sleep(1)
            
            # Логирование прогресса каждые 100 сообщений
            if self.messages_processed % 100 == 0:
                logger.info(f"Собрано {self.messages_processed}/{training_samples} образцов")
        
        logger.info(f"Собрано {self.messages_processed} образцов")
        
        # Обучение модели
        self._train_model()
        
        logger.info("Система работает в режиме детектирования")
        
        return True
    
    def _train_model(self):
        """Обучить модель на собранных данных"""
        logger.info("Подготовка данных для обучения...")
        
        # Сбор признаков из всех устройств
        all_features = []
        
        for device_id in list(self.extractor.message_buffers.keys()):
            features = self.extractor.extract_feature_vector(device_id)
            if features is not None:
                all_features.append(features)
        
        if not all_features:
            logger.warning("Нет данных для обучения модели")
            return
        
        import numpy as np
        X = np.array(all_features)
        
        logger.info(f"Обучение модели на {X.shape[0]} образцах, {X.shape[1]} признаках...")
        
        # Обучение
        if self.detector.train(X):
            # Сохранение модели
            self.detector.save_model()
            logger.info("Модель обучена и сохранена")
        else:
            logger.error("Не удалось обучить модель")
    
    def stop(self):
        """Остановить систему"""
        logger.info("Остановка системы...")
        
        self.capture.disconnect()
        self.db.close()
        
        duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        logger.info(f"Система остановлена")
        logger.info(f"Статистика:")
        logger.info(f"  Время работы: {duration:.2f} сек")
        logger.info(f"  Сообщений обработано: {self.messages_processed}")
        logger.info(f"  Аномалий обнаружено: {self.anomalies_detected}")
    
    def get_statistics(self) -> dict:
        """
        Получить статистику работы системы
        
        Returns:
            Словарь со статистикой
        """
        duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "start_time": self.start_time,
            "duration_seconds": duration,
            "messages_processed": self.messages_processed,
            "anomalies_detected": self.anomalies_detected,
            "messages_per_second": self.messages_processed / duration if duration > 0 else 0,
            "capture_stats": self.capture.get_statistics(),
            "model_info": self.detector.get_info(),
            "buffer_info": self.extractor.get_buffer_info()
        }


def main():
    """Главная функция для запуска системы"""
    print("=== Система обнаружения аномалий промышленного трафика ===")
    print()
    
    # Создание системы
    system = AnomalyDetectionSystem(
        broker_host="broker.hivemq.com",
        broker_port=1883,
        topic_pattern="spBv1.0/#",
        db_path="data/anomalies.db",
        model_path="models/isolation_forest.pkl",
        window_size_seconds=60,
        contamination=0.05
    )
    
    try:
        # Запуск системы
        if system.start(training_samples=100):
            print("\nСистема работает. Нажмите Ctrl+C для остановки...")
            
            # Бесконечный цикл работы
            import time
            while True:
                time.sleep(10)
                
                # Вывод статистики каждые 30 секунд
                stats = system.get_statistics()
                print(f"\rСообщений: {stats['messages_processed']} | "
                      f"Аномалий: {stats['anomalies_detected']} | "
                      f"Устройств: {len(stats['buffer_info'])}", end="")
    
    except KeyboardInterrupt:
        print("\n\nОстановка по запросу пользователя...")
    
    finally:
        system.stop()


if __name__ == "__main__":
    main()
