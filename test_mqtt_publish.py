"""
Тест публикации сообщения в MQTT с on_connect
"""

import paho.mqtt.client as mqtt
import time

connected = False
published = False

def on_connect(client, userdata, flags, rc):
    global connected
    print(f"✓ on_connect вызван! Код: {rc}, flags: {flags}")
    connected = True

def on_publish(client, userdata, mid):
    global published
    print(f"✓ Сообщение опубликовано! mid: {mid}")
    published = True

# Создаём клиент
client = mqtt.Client(client_id="test_publisher")
client.on_connect = on_connect
client.on_publish = on_publish

# Используем бесплатный публичный брокер EMQX
host = "broker.emqx.io"
port = 1883

print(f"Подключение к {host}:{port}...")
result = client.connect(host, port, 60)
print(f"Результат connect(): {result}")

# Запускаем loop
client.loop_start()

# Ждём подключения
for i in range(10):
    time.sleep(1)
    if connected:
        print("✓ on_connect был вызван!")
        break
    print(f"Ожидание on_connect... {i+1}/10")

# Публикуем сообщение
print("\nПубликация сообщения...")
info = client.publish("test/topic", "Hello from Python!")
print(f"Результат publish(): {info}")

# Ждём публикации
for i in range(10):
    time.sleep(1)
    if published:
        print("✓ Успешная публикация!")
        break
    print(f"Ожидание публикации... {i+1}/10")

client.loop_stop()
client.disconnect()

print(f"\nИтог: connected={connected}, published={published}")
