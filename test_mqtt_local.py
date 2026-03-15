"""
Тест подключения к локальному MQTT брокеру с 127.0.0.1
"""

import paho.mqtt.client as mqtt
import time

connected = False
message_count = 0

def on_connect(client, userdata, flags, rc):
    global connected
    print(f"✓ on_connect вызван! Код: {rc}, flags: {flags}")
    connected = True
    client.subscribe("test/#")

def on_message(client, userdata, msg):
    global message_count
    message_count += 1
    print(f"✓ on_message вызван! #{message_count}: {msg.topic} - {msg.payload.decode()[:50]}")

# Создаём клиент
client = mqtt.Client(client_id="test_local")
client.on_connect = on_connect
client.on_message = on_message

# Подключаемся к локальному брокеру через 127.0.0.1
host = "127.0.0.1"
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
        print("✓ Успешное подключение!")
        # Публикуем тестовое сообщение
        print("Публикация тестового сообщения...")
        client.publish("test/topic", "Hello from local broker!")
        
        # Ждём сообщения
        for j in range(5):
            time.sleep(1)
            if message_count > 0:
                print(f"✓ Получено {message_count} сообщений")
                break
        break
    print(f"Ожидание подключения... {i+1}/10, connected={connected}")

client.loop_stop()
client.disconnect()

if connected:
    print("✓ Тест успешно завершён")
else:
    print("✗ Не удалось подключиться")
