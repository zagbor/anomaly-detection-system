"""
Главный модуль детектора аномалий
Объединяет все компоненты системы
"""

import sys
import os
import logging
import time
from datetime import datetime
from typing import Optional, List, Dict
import numpy as np

# Добавляем родительскую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from capture.mqtt_client import TrafficCapture
from features.extractor import FeatureExtractor
from ml.model import AnomalyDetector
from storage.db_manager import DatabaseManager, AnomalyRecord
import os
import sys
import json
import struct
import re

# Настройка логирования
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log_dir = os.path.join(base_dir, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

# Очищаем существующие обработчики
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'system.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Устанавливаем уровень INFO для всех логгеров
logging.getLogger().setLevel(logging.INFO)


class SparkplugManualDecoder:
    """
    Легковесный парсер Sparkplug B для извлечения метрик без зависимости от Protobuf.
    Используется как fallback, если официальный декодер не сработал.
    """
    @staticmethod
    def decode_metrics(payload: bytes) -> List[Dict]:
        metrics = []
        try:
            # Ищем паттерны названий тегов
            tag_regex = r'[a-zA-Z0-9_\-\/]{3,50}'
            
            for match in re.finditer(tag_regex.encode(), payload):
                tag_name = match.group().decode()
                
                # Если это похоже на промышленный тег
                if '/' in tag_name or any(x in tag_name.lower() for x in ['temp', 'press', 'vib', 'flow', 'power', 'hum', 'stat']):
                    start_pos = match.end()
                    # Ищем данные в окне после названия тега
                    search_range = payload[start_pos:start_pos+40]
                    
                    found_val = None
                    
                    # 1. Поиск по маркерам типов Sparkplug B
                    # Datatype обычно идет после timestamp (tag 3, 18 xx...)
                    datatype_match = re.search(b'\x20([\x00-\x15])', search_range)
                    if datatype_match:
                        dtype = datatype_match.group(1)[0]
                        dtype_pos = datatype_match.start()
                        
                        # Таблица типов Sparkplug B: 9=Float, 10=Double, 3=Int32
                        if dtype == 9: # Float
                            # Ищем маркер 0x55 (Tag 10) или 0x65 (Tag 12)
                            for marker in [b'\x65', b'\x55']:
                                val_pos = search_range.find(marker, dtype_pos)
                                if val_pos != -1 and val_pos < len(search_range) - 4:
                                    # Пробуем и Little Endian (стандарт), и Big Endian (встречается в диких условиях)
                                    bytes_val = search_range[val_pos+1:val_pos+5]
                                    v_le = struct.unpack('<f', bytes_val)[0]
                                    v_be = struct.unpack('>f', bytes_val)[0]
                                    
                                    # Выбираем "разумное" значение
                                    if 0.1 < abs(v_be) < 5000: found_val = float(v_be)
                                    elif 0.1 < abs(v_le) < 5000: found_val = float(v_le)
                                    break
                                    
                        elif dtype == 10: # Double
                            val_pos = search_range.find(b'\x59', dtype_pos) # Tag 11
                            if val_pos != -1 and val_pos < len(search_range) - 8:
                                bytes_val = search_range[val_pos+1:val_pos+9]
                                v_le = struct.unpack('<d', bytes_val)[0]
                                v_be = struct.unpack('>d', bytes_val)[0]
                                if 0.1 < abs(v_be) < 5000: found_val = float(v_be)
                                elif 0.1 < abs(v_le) < 5000: found_val = float(v_le)

                    # 2. Если по структуре не вышло, ищем любое float/int в диапазоне
                    if found_val is None:
                        for i in range(len(search_range) - 4):
                            try:
                                v_be = struct.unpack('>f', search_range[i:i+4])[0]
                                if 0.1 < abs(v_be) < 5000:
                                    found_val = float(v_be)
                                    break
                            except: continue

                    if found_val is not None and not np.isinf(found_val) and not np.isnan(found_val):
                        # Избегаем дубликатов тегов в одном сообщении
                        if not any(m['name'] == tag_name for m in metrics):
                            metrics.append({"name": tag_name, "value": found_val})
            
            return metrics
        except Exception as e:
            logger.debug(f"Ошибка ручного декодирования: {e}")
            return []


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
        
        # Настройки системы (инициализация)
        self.system_mode = 'collect'
        self.training_period_minutes = 60
        self.collection_start_time = datetime.now()
        self.last_settings_load = 0
        
        logger.info("Система обнаружения аномалий инициализирована")
    
    def _parse_sparkplug_topic(self, topic: str) -> Optional[dict]:
        """
        Парсинг топика Sparkplug B
        
        Форматы: 
        spBv1.0/Group_ID/Message_Type/Edge_Node_ID/Device_ID
        spBv1.0/Group_ID/Message_Type/Device_ID
        
        Args:
            topic: Топик MQTT
            
        Returns:
            Словарь с компонентами топика или None
        """
        parts = topic.split('/')
        
        if len(parts) < 4 or parts[0] != "spBv1.0":
            return None
        
        # Стандарт: spBv1.0/group_id/message_type/edge_node_id/[device_id]
        # Упрощенно: если 4 части, считаем последнюю за device_id
        if len(parts) == 4:
            return {
                "version": parts[0],
                "group_id": parts[1],
                "message_type": parts[2],
                "node_id": "default_node",
                "device_id": parts[3]
            }
        
        return {
            "version": parts[0],
            "group_id": parts[1],
            "message_type": parts[2],
            "node_id": parts[3],
            "device_id": parts[4] if len(parts) > 4 else None
        }
    
    def _process_message(self, msg):
        """
        Колбэк для обработки входящих MQTT сообщений
        """
        try:
            self.messages_processed += 1
            self._load_settings()
            
            # Разбор топика: spBv1.0/group_id/message_type/node_id/[device_id]
            parts = msg.topic.split('/')
            if len(parts) < 3:
                return

            group_id = parts[1]
            
            # Ищем тип сообщения из стандартных Sparkplug B
            spb_types = ["NBIRTH", "NDEATH", "DBIRTH", "DDEATH", "NDATA", "DDATA", "NCMD", "DCMD"]
            message_type = "UNKNOWN"
            for p in parts:
                if p in spb_types:
                    message_type = p
                    break
            
            if message_type == "UNKNOWN":
                message_type = parts[2] if len(parts) > 2 else "UNKNOWN"

            # Определяем node_id и device_id
            if len(parts) >= 5:
                node_id = parts[3]
                device_id = parts[4]
            elif len(parts) == 4:
                node_id = parts[3]
                device_id = f"{node_id}_node"
            else:
                node_id = "default_node"
                device_id = parts[-1] if len(parts) > 1 else "unknown"

            # Регистрация устройства
            self.db.register_device(device_id, node_id, group_id)

            # Пытаемся декодировать тело сообщения
            metrics_to_process = []
            
            # 1. Сначала пробуем как бинарный Sparkplug B (Protobuf)
            try:
                # Динамически импортируем, чтобы не падать при старте если файл битый
                from .sparkplug_b_pb2 import Payload as SparkplugPayload
                inbound_payload = SparkplugPayload()
                inbound_payload.ParseFromString(msg.payload)
                for metric in inbound_payload.metrics:
                    val = None
                    if metric.HasField('int_value'): val = float(metric.int_value)
                    elif metric.HasField('long_value'): val = float(metric.long_value)
                    elif metric.HasField('float_value'): val = float(metric.float_value)
                    elif metric.HasField('double_value'): val = float(metric.double_value)
                    elif metric.HasField('boolean_value'): val = float(1.0 if metric.boolean_value else 0.0)
                    elif metric.HasField('string_value'):
                        try: val = float(metric.string_value)
                        except: pass
                    
                    if val is not None:
                        metrics_to_process.append({
                            "name": metric.name if metric.name else f"alias_{metric.alias}",
                            "value": val
                        })
            except Exception as e:
                # 2. Если Protobuf не сработал, используем ручной Fallback-декодер
                logger.debug(f"Protobuf decoder failed, using fallback: {e}")
                metrics_to_process = SparkplugManualDecoder.decode_metrics(msg.payload)
                
                # 3. Если и это не сработало, пробуем как JSON
                if not metrics_to_process:
                    try:
                        payload_str = msg.payload.decode('utf-8', errors='ignore')
                        data = json.loads(payload_str)
                        if isinstance(data, dict) and "metric" in data:
                            metric = data["metric"][0]
                            value = float(metric["value"])
                            tag_name = metric["name"]
                            metrics_to_process.append({"name": tag_name, "value": value})
                    except:
                        # Пробуем прямое число
                        try:
                            val = float(msg.payload.decode('utf-8', errors='ignore'))
                            metrics_to_process.append({"name": "unknown", "value": val})
                        except:
                            pass

            # Логируем и обрабатываем все найденные метрики
            for m in metrics_to_process:
                tag_name = m["name"]
                value = m["value"]

                # Логируем трафик
                self.db.log_traffic(
                    topic=msg.topic,
                    device_id=device_id,
                    tag_name=tag_name,
                    value=value,
                    payload=str(msg.payload)[:200]
                )
                
                if self.messages_processed % 10 == 0:
                    logger.info(f"Сохранено в БД: {device_id}/{tag_name}={value}")

                # Регистрация тега (делаем для всех типов сообщений, включая BIRTH)
                self.db.register_tag(device_id, tag_name, value)

                # Обработка в экстракторе и аномалии только для DDATA (данные)
                if message_type != "DDATA":
                    continue

                # Добавление сообщения в экстрактор
                self.extractor.add_message(
                    device_id=device_id,
                    tag_name=tag_name,
                    value=value,
                    timestamp=datetime.now(),
                    message_type=message_type
                )
                
            # Проверка на аномалию каждые 10 сообщений (только для DDATA)
            if self.messages_processed % 10 == 0 and device_id and message_type == "DDATA":
                # 1. Проверяем режим системы
                if self.system_mode == 'collect':
                    return

                # 2. Проверяем достаточно ли данных для этого устройства
                try:
                    cursor = self.db.conn.cursor()
                    cursor.execute('SELECT first_seen FROM devices WHERE device_id = ?', (device_id,))
                    row = cursor.fetchone()
                    if row:
                        first_seen = row[0]
                        if isinstance(first_seen, str):
                            first_seen = datetime.fromisoformat(first_seen)
                        
                        observation_time = (datetime.now() - first_seen).total_seconds() / 60
                        if observation_time < self.training_period_minutes:
                            if self.messages_processed % 100 == 0:
                                logger.info(f"Устройство {device_id} в режиме обучения: {observation_time:.1f}/{self.training_period_minutes} мин")
                            return
                except Exception as e:
                    logger.error(f"Ошибка проверки периода обучения: {e}")
                    return

                # 3. Запускаем обнаружение
                self._check_for_anomalies(device_id)
            
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
        self.last_stats_log = time.time()
        
        # Настройки системы
        self.system_mode = 'collect'
        self.training_period_minutes = 60
        self.collection_start_time = datetime.now()
        self.last_settings_load = 0
        self._load_settings()

        return self.run(training_samples)

    def _load_settings(self):
        """Загрузка настроек из БД"""
        now_ts = time.time()
        if now_ts - self.last_settings_load > 30: # Теперь чаще - раз в 30 секунд
            try:
                self.system_mode = self.db.get_setting('system_mode', 'collect')
                self.training_period_minutes = int(self.db.get_setting('training_period_minutes', '60'))
                
                start_time_str = self.db.get_setting('collection_start_time')
                if start_time_str:
                    try:
                        self.collection_start_time = datetime.fromisoformat(start_time_str)
                    except:
                        self.collection_start_time = datetime.now()
                
                # Проверка автоматического переключения
                if self.system_mode == 'collect':
                    elapsed = (datetime.now() - self.collection_start_time).total_seconds() / 60
                    if elapsed >= self.training_period_minutes:
                        logger.info(f"Период обучения ({self.training_period_minutes}м) завершен. Автоматический переход в MONITORING.")
                        self.system_mode = 'detect'
                        self.db.set_setting('system_mode', 'detect')
                
                self.last_settings_load = now_ts
                logger.info(f"Настройки загружены: mode={self.system_mode}, period={self.training_period_minutes}m")
            except Exception as e:
                logger.error(f"Ошибка загрузки настроек: {e}")

    def run(self, training_samples: int = 1000):
        """
        Запустить систему
        
        Args:
            training_samples: Количество образцов для обучения модели
        """
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
    
    # Определение путей относительно корня проекта
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = os.path.join(project_root, "data", "anomalies.db")
    model_path = os.path.join(project_root, "models", "isolation_forest.pkl")

    # Создание системы
    system = AnomalyDetectionSystem(
        broker_host="broker.hivemq.com",
        broker_port=1883,
        topic_pattern="spBv1.0/#",
        db_path=db_path,
        model_path=model_path,
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
