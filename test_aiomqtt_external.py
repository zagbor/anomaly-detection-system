"""
Тест подключения к внешнему MQTT брокеру с помощью aiomqtt
"""

import asyncio
from aiomqtt import Client

async def test_mqtt():
    connected = False
    message_count = 0
    
    # Пробуем подключиться к broker.hivemq.com
    host = "broker.hivemq.com"
    port = 1883
    
    print(f"Подключение к {host}:{port}...")
    
    try:
        async with Client(host, port) as client:
            print("✓ Подключение установлено!")
            connected = True
            
            # Подписываемся на тестовый топик
            print("Подписка на test/#...")
            await client.subscribe("test/#")
            
            # Публикуем тестовое сообщение
            print("Публикация тестового сообщения...")
            await client.publish("test/topic", "Hello from aiomqtt to external broker!")
            
            # Ждём сообщения
            print("Ожидание сообщений...")
            async for message in client.messages:
                message_count += 1
                print(f"✓ Получено сообщение #{message_count}: {message.topic.value} - {message.payload.decode()[:50]}")
                
                if message_count >= 3:
                    print("✓ Получено достаточно сообщений!")
                    break
            
            print("✓ Тест успешно завершён")
            
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()

# Запускаем асинхронный тест
asyncio.run(test_mqtt())
