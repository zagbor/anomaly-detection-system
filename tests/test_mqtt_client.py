"""
Тесты для модуля захвата MQTT-трафика
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from capture.mqtt_client import TrafficCapture
import unittest
from unittest.mock import Mock, patch
from datetime import datetime


class TestTrafficCapture(unittest.TestCase):
    """Тесты класса TrafficCapture"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        self.capture = TrafficCapture(
            broker_host="test.broker.com",
            broker_port=1883,
            topic_pattern="test/#"
        )
    
    def test_initialization(self):
        """Тест инициализации клиента"""
        self.assertEqual(self.capture.broker_host, "test.broker.com")
        self.assertEqual(self.capture.broker_port, 1883)
        self.assertEqual(self.capture.topic_pattern, "test/#")
        self.assertFalse(self.capture.connected)
        self.assertEqual(self.capture.messages_received, 0)
    
    def test_set_message_callback(self):
        """Тест установки колбэка"""
        callback = Mock()
        self.capture.set_message_callback(callback)
        self.assertEqual(self.capture.message_callback, callback)
    
    def test_get_statistics_empty(self):
        """Тест статистики без сообщений"""
        stats = self.capture.get_statistics()
        self.assertEqual(stats['connected'], False)
        self.assertEqual(stats['messages_received'], 0)
        self.assertIsNone(stats['duration_seconds'])
        self.assertEqual(stats['messages_per_second'], 0)
    
    @patch('capture.mqtt_client.mqtt.Client')
    def test_connect_success(self, mock_mqtt_client):
        """Тест успешного подключения"""
        mock_client_instance = Mock()
        mock_mqtt_client.return_value = mock_client_instance
        
        # Мокаем успешное подключение
        mock_client_instance.connect.return_value = 0
        mock_client_instance.loop_start.return_value = None
        
        result = self.capture.connect()
        
        # Проверяем, что connect был вызван с правильными параметрами
        mock_client_instance.connect.assert_called_once_with("test.broker.com", 1883, 60)
        mock_client_instance.loop_start.assert_called_once()
    
    def test_on_connect_success(self):
        """Тест колбэка при успешном подключении"""
        mock_client = Mock()
        mock_userdata = {}
        rc = 0  # Успешное подключение
        
        self.capture._on_connect(mock_client, mock_userdata, rc)
        
        self.assertTrue(self.capture.connected)
        self.assertIsNotNone(self.capture.start_time)
    
    def test_on_connect_failure(self):
        """Тест колбэка при неудачном подключении"""
        mock_client = Mock()
        mock_userdata = {}
        rc = 1  # Ошибка подключения
        
        self.capture._on_connect(mock_client, mock_userdata, rc)
        
        self.assertFalse(self.capture.connected)


if __name__ == '__main__':
    unittest.main()
