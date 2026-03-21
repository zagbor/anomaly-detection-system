"""
Модуль захвата MQTT-трафика
Подключается к MQTT-брокеру и подписывается на топики Sparkplug B
"""

import paho.mqtt.client as mqtt
import json
import logging
from datetime import datetime
from typing import Callable, Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrafficCapture:
    """Класс для захвата MQTT-трафика"""
    
    def __init__(
        self,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        topic_pattern: str = "#",
        client_id: Optional[str] = None,
        use_websocket: bool = False
    ):
        """
        Инициализация клиента MQTT
        
        Args:
            broker_host: Хост MQTT-брокера
            broker_port: Порт MQTT-брокера
            topic_pattern: Паттерн топиков для подписки
            client_id: ID клиента (если None, генерируется автоматически)
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic_pattern = topic_pattern
        
        # Создание клиента MQTT
        if client_id is None:
            client_id = f"anomaly_detector_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        self.use_websocket = use_websocket
        
        self.client = mqtt.Client(client_id=client_id)
        
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        # Убираем on_disconnect для упрощения
        
        # Колбэк для обработки сообщений
        self.message_callback: Optional[Callable] = None
        
        # Статистика
        self.messages_received = 0
        self.connected = False
        self.start_time = None
        
    def _on_connect(self, client, userdata, flags, rc):
        """Колбэк при подключении к брокеру"""
        if rc == 0:
            logger.info(f"Подключено к брокеру {self.broker_host}:{self.broker_port}")
            self.connected = True
            self.start_time = datetime.now()
            # Подписка на топики
            client.subscribe(self.topic_pattern)
            logger.info(f"Подписка на топик: {self.topic_pattern}")
        else:
            logger.error(f"Ошибка подключения. Код: {rc}")
            self.connected = False
    
    def _on_message(self, client, userdata, msg):
        """Колбэк при получении сообщения"""
        self.messages_received += 1
        
        # Логирование каждых 100 сообщений
        if self.messages_received % 100 == 0:
            logger.info(f"Получено сообщений: {self.messages_received}")
        
        # Вызов колбэка если установлен
        if self.message_callback:
            try:
                self.message_callback(msg)
            except Exception as e:
                logger.error(f"Ошибка в колбэке: {e}")
    
    def _on_disconnect(self, client, userdata, reason_code, properties, *args):
        """Колбэк при отключении от брокера"""
        self.connected = False
        if reason_code != 0:
            logger.warning(f"Неожиданное отключение. Код: {reason_code}")
        else:
            logger.info("Отключено от брокера")
    
    def set_message_callback(self, callback: Callable):
        """
        Установить колбэк для обработки сообщений
        
        Args:
            callback: Функция, принимающая объект mqtt.MQTTMessage
        """
        self.message_callback = callback
        logger.info("Колбэк для обработки сообщений установлен")
    
    def connect(self) -> bool:
        """
        Подключиться к MQTT-брокеру
        
        Returns:
            True если подключение успешно, иначе False
        """
        try:
            logger.info(f"Подключение к {self.broker_host}:{self.broker_port}...")
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            
            # Ожидание подключения
            import time
            for _ in range(10):  # 5 секунд максимум
                if self.connected:
                    return True
                time.sleep(0.5)
            
            logger.error("Таймаут подключения")
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при подключении: {e}")
            return False
    
    def disconnect(self):
        """Отключиться от брокера"""
        if self.connected:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Клиент остановлен")
    
    def get_statistics(self) -> dict:
        """
        Получить статистику захвата
        
        Returns:
            Словарь со статистикой
        """
        duration = None
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
        
        return {
            "connected": self.connected,
            "messages_received": self.messages_received,
            "duration_seconds": duration,
            "messages_per_second": self.messages_received / duration if duration else 0
        }


def main():
    """Тестовая функция для проверки подключения к MQTT Lab"""
    print("=== Тест подключения к MQTT Lab ===")
    print(f"Брокер: broker.hivemq.com:1883")
    print(f"Топик: spBv1.0/#")
    print()
    
    def message_handler(msg):
        """Обработчик сообщений для теста"""
        topic = msg.topic
        payload = msg.payload.decode('utf-8', errors='ignore')
        print(f"[{topic}] {payload[:100]}...")  # Первые 100 символов
        
        # Остановить после 10 сообщений
        if capture.messages_received >= 10:
            print("\nПолучено 10 сообщений. Остановка...")
            capture.disconnect()
    
    # Создание клиента
    capture = TrafficCapture()
    capture.set_message_callback(message_handler)
    
    # Подключение
    if capture.connect():
        print("Ожидание сообщений...")
        
        # Ожидание сообщений
        import time
        while capture.connected and capture.messages_received < 10:
            time.sleep(0.1)
        
        # Статистика
        stats = capture.get_statistics()
        print(f"\nСтатистика:")
        print(f"  Сообщений получено: {stats['messages_received']}")
        print(f"  Длительность: {stats['duration_seconds']:.2f} сек")
        print(f"  Сообщений/сек: {stats['messages_per_second']:.2f}")
    else:
        print("Не удалось подключиться к брокеру")


if __name__ == "__main__":
    main()
