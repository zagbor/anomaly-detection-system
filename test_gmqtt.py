"""
Тест подключения к MQTT брокеру с помощью gmqtt
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

async def test_mqtt():
    client = gmqtt.Client("test_gmqtt")
    
    client.on_connect = on_connect
    client.on_message = on_message
    
    host = "127.0.0.1"
    port = 1883
    
    print(f"Подключение к {host}:{port}...")
    
    try:
        await client.connect(host, port)
        
        # Ждём подключения
        for i in range(10):
            await asyncio.sleep(1)
            if connected:
                print("✓ Успешное подключение!")
                # Публикуем тестовое сообщение
                print("Публикация тестового сообщения...")
                client.publish("test/topic", "Hello from gmqtt!")
                
                # Ждём сообщения
                for j in range(5):
                    await asyncio.sleep(1)
                    if message_count > 0:
                        print(f"✓ Получено {message_count} сообщений")
                        break
                break
            print(f"Ожидание подключения... {i+1}/10, connected={connected}")
        
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
