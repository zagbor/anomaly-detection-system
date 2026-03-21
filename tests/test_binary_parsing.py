
import sys
import os
import binascii

# Добавляем путь к src
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Импортируем только декодер из anomaly_detector, не трогая сломанный pb2
from detector.anomaly_detector import SparkplugManualDecoder

def test_real_data_decoding():
    print("=== Тест ручного декодирования реальных данных IcoMS ===")
    
    # Данные из лога пользователя: spBv1.0/IcoMS/DBIRTH/IcoMS/IcoMS
    hex_payload = "08e49dba3312220a1273656e736f722f74656d706572617475726518e39dba3320096542121f0a0f73656e736f722f68756d696469747918e39dba33200965687341121a0a0d73656e736f722f73746174696318e39dba33200350641801"
    binary_data = binascii.unhexlify(hex_payload)
    
    print(f"Размер бинарных данных: {len(binary_data)} байт")
    
    # Пробуем декодировать
    metrics = SparkplugManualDecoder.decode_metrics(binary_data)
    
    if metrics:
        print(f"✓ Успешно найдено метрик: {len(metrics)}")
        for m in metrics:
            print(f"  - Тег: {m['name']}, Значение: {m['value']}")
    else:
        print("✗ Метрики не найдены. Проверьте логику SparkplugManualDecoder.")

if __name__ == "__main__":
    test_real_data_decoding()
