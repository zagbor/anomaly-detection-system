"""
Синхронный тест подключения к MQTT брокеру
"""

import paho.mqtt.client as mqtt
import time

# Создаём клиент
client = mqtt.Client(client_id="test_sync")

# Используем бесплатный публичный брокер EMQX
host = "broker.emqx.io"
port = 1883

print(f"Подключение к {host}:{port}...")
result = client.connect(host, port, 60)
print(f"Результат connect(): {result}")

# Используем синхронный loop
print("Запуск loop()...")
try:
    for i in range(10):
        client.loop(timeout=1.0)
        print(f"Loop iteration {i+1}/10")
    
    print("✓ Loop работает!")
    
    # Публикуем сообщение
    print("\nПубликация сообщения...")
    info = client.publish("test/topic", "Hello from Python 3.10!")
    print(f"Результат publish(): {info}")
    
    # Ждём отправки
    for i in range(5):
        client.loop(timeout=1.0)
        print(f"Loop iteration {i+1}/5 после publish")
    
    print("✓ Тест завершён")
    
except Exception as e:
    print(f"✗ Ошибка: {e}")
    import traceback
    traceback.print_exc()

client.disconnect()
