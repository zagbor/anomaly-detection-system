"""
Ретранслятор MQTT: получает сообщения с внешнего брокера и пересылает в локальный
"""

import paho.mqtt.client as mqtt
import time
import json

# Настройки внешнего брокера
EXTERNAL_HOST = "broker.hivemq.com"
EXTERNAL_PORT = 1883
EXTERNAL_TOPIC = "spBv1.0/#"

# Настройки локального брокера
LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 1883
LOCAL_TOPIC_PREFIX = "spBv1.0/"

# Статистика
messages_received = 0
messages_relayed = 0

def on_connect_external(client, userdata, flags, rc):
    """Колбэк при подключении к внешнему брокеру"""
    if rc == 0:
        print(f"✓ Подключено к внешнему брокеру {EXTERNAL_HOST}:{EXTERNAL_PORT}")
        client.subscribe(EXTERNAL_TOPIC)
        print(f"✓ Подписка на топик: {EXTERNAL_TOPIC}")
    else:
        print(f"✗ Ошибка подключения к внешнему брокеру. Код: {rc}")

def on_message_external(client, userdata, msg):
    """Колбэк при получении сообщения от внешнего брокера"""
    global messages_received, messages_relayed
    
    messages_received += 1
    
    # Пересылаем сообщение в локальный брокер
    try:
        local_client.publish(msg.topic, msg.payload)
        messages_relayed += 1
        
        if messages_received % 10 == 0:
            print(f"Получено: {messages_received}, Переслано: {messages_relayed}")
    except Exception as e:
        print(f"✗ Ошибка пересылки: {e}")

# Создаём клиент для внешнего брокера
external_client = mqtt.Client(client_id="relay_external")
external_client.on_connect = on_connect_external
external_client.on_message = on_message_external

# Создаём клиент для локального брокера
local_client = mqtt.Client(client_id="relay_local")

print(f"Подключение к локальному брокеру {LOCAL_HOST}:{LOCAL_PORT}...")
try:
    local_client.connect(LOCAL_HOST, LOCAL_PORT, 60)
    local_client.loop_start()
    time.sleep(1)
    print("✓ Подключено к локальному брокеру")
except Exception as e:
    print(f"✗ Ошибка подключения к локальному брокеру: {e}")
    exit(1)

print(f"Подключение к внешнему брокеру {EXTERNAL_HOST}:{EXTERNAL_PORT}...")
try:
    external_client.connect(EXTERNAL_HOST, EXTERNAL_PORT, 60)
    external_client.loop_start()
    
    # Ждём подключения
    for i in range(10):
        time.sleep(1)
        if messages_received > 0:
            print("✓ Начинаем приём сообщений...")
            break
        print(f"Ожидание сообщений... {i+1}/10")
    
    # Работаем бесконечно
    print("Ретранслятор работает. Нажмите Ctrl+C для остановки.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nОстановка ретранслятора...")
        
except Exception as e:
    print(f"✗ Ошибка: {e}")
    import traceback
    traceback.print_exc()
finally:
    external_client.loop_stop()
    external_client.disconnect()
    local_client.loop_stop()
    local_client.disconnect()
    
    print(f"\nСтатистика:")
    print(f"  Получено сообщений: {messages_received}")
    print(f"  Переслано сообщений: {messages_relayed}")
