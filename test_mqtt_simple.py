"""
Простой тест подключения к MQTT брокеру с loop_start и длительным ожиданием
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
client = mqtt.Client(client_id="test_client")
client.on_connect = on_connect
client.on_message = on_message

# Используем бесплатный публичный брокер EMQX
host = "broker.emqx.io"
port = 1883

print(f"Подключение к {host}:{port}...")
result = client.connect(host, port, 60)
print(f"Результат connect(): {result}")

# Запускаем loop в отдельном потоке
client.loop_start()

# Ждём подключения с более длительным ожиданием
for i in range(30):
    time.sleep(1)
    if connected:
        print("✓ Успешное подключение!")
        # Ждём сообщения
        for j in range(15):
            time.sleep(1)
            if message_count > 0:
                print(f"✓ Получено {message_count} сообщений")
                break
        break
    if i % 5 == 0:
        print(f"Ожидание подключения... {i+1}/30, connected={connected}")

client.loop_stop()
client.disconnect()

if connected:
    print("✓ Тест успешно завершён")
else:
    print("✗ Не удалось подключиться")
