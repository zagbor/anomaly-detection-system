import paho.mqtt.client as mqtt
import json
import logging
import time

logger = logging.getLogger(__name__)

class TestMQTTPublisher:
    """Утилита для отправки тестовых сигналов и аномалий"""
    
    def __init__(self, host="127.0.0.1", port=1883):
        self.host = host
        self.port = port
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"GENERATOR: Connected to MQTT broker {self.host}")
        else:
            logger.error(f"GENERATOR: Failed to connect to MQTT broker, rc={rc}")

    def _on_publish(self, client, userdata, mid):
        logger.debug(f"GENERATOR: Message {mid} published")
        
    def connect(self):
        try:
            logger.info(f"GENERATOR: Connecting to MQTT broker {self.host}:{self.port}...")
            self.client.connect(self.host, self.port, 60)
            self.client.loop_start()
            return True
        except Exception as e:
            logger.error(f"GENERATOR: Connection error: {e}")
            return False
            
    def publish_metric(self, group_id, node_id, device_id, tag_name, value):
        """Отправка метрики в формате Sparkplug B (JSON fallback)"""
        topic = f"spBv1.0/{group_id}/DDATA/{node_id}/{device_id}"
        
        # Для совместимости с нашим детектором (он ищет "metric" в JSON)
        payload_compat = {
            "metric": [
                {
                    "name": tag_name,
                    "value": float(value)
                }
            ]
        }
        
        payload_str = json.dumps(payload_compat)
        logger.info(f"GENERATOR: Preparing to publish to {topic} | payload={payload_str}")
        
        # Используем QoS 1 для гарантированной доставки (или ошибки)
        result = self.client.publish(topic, payload_str, qos=1)
        
        # Ждем завершения публикации (на период таймаута)
        try:
            result.wait_for_publish(timeout=2.0)
            if result.is_published():
                logger.info(f"GENERATOR: MQTT Publish CONFIRMED to {topic}")
            else:
                logger.error(f"GENERATOR: MQTT Publish NOT CONFIRMED (rc={result.rc})")
        except Exception as e:
            logger.error(f"GENERATOR: MQTT Publish error while waiting: {e}")
            
        return topic

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
