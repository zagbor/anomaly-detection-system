"""
Тест подключения к внешнему MQTT брокеру с помощью gmqtt
"""

import gmqtt
import asyncio

connected = False
message_count = 0

def on_connect(client, flags, rc, properties):
    global connected
    print(f"✓ on_connect вызван! Код: {rc}, flags: {flags}")
    connected = True
    client.subscribe("test/#")

def on_message(client, topic, payload, qos, properties):
    global message_count
    message_count += 1
    print(f"✓ on_message вызван! #{message_count}: {topic} - {payload[:50]}")

def on_disconnect(client, packet, exc=None):
    global connected
    print(f"✓ on_disconnect вызван! packet: {packet}")
    connected = False

async def test_mqtt():
    client = gmqtt.Client("test_gmqtt_external")
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    # Пробуем подключиться к broker.hivemq.com
    host = "broker.hivemq.com"
    port = 1883
    
    print(f"Подключение к {host}:{port}...")
    
    try:
        await client.connect(host, port, version=4)
        
        # Ждём подключения
        for i in range(15):
            await asyncio.sleep(1)
            if connected:
                print("✓ Успешное подключение!")
                # Публикуем тестовое сообщение
                print("Публикация тестового сообщения...")
                client.publish("test/topic", "Hello from gmqtt to external broker!")
                
                # Ждём сообщения
                for j in range(10):
                    await asyncio.sleep(1)
                    if message_count > 0:
                        print(f"✓ Получено {message_count} сообщений")
                        break
                break
            print(f"Ожидание подключения... {i+1}/15, connected={connected}")
        
        await client.disconnect()
        
        if connected:
            print("✓ Тест успешно завершён")
        else:
            print("✗ Не удалось подключиться")
            
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()

# Запускаем асинхронный тест
asyncio.run(test_mqtt())
