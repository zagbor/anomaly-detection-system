# Система обнаружения аномалий промышленного сетевого трафика

Встраиваемая система пассивного мониторинга промышленного сетевого трафика на базе протокола MQTT и спецификации Sparkplug B с использованием методов машинного обучения (Isolation Forest).

## Структура проекта

```
anomaly-detection-system/
├── src/
│   ├── capture/          # Модуль захвата MQTT-трафика
│   ├── features/         # Модуль извлечения признаков
│   ├── ml/               # Модуль машинного обучения
│   ├── detector/         # Главный детектор аномалий
│   ├── storage/          # Модуль работы с базой данных
│   └── web/              # Веб-интерфейс
├── data/
│   ├── raw/              # CSV с сырыми данными
│   ├── processed/        # CSV с признаками
│   └── anomalies.db      # SQLite база аномалий
├── models/
│   └── isolation_forest.pkl  # Обученная модель
├── logs/
│   └── system.log        # Логи системы
├── tests/                # Тесты
├── requirements.txt       # Зависимости Python
└── README.md             # Этот файл
```

## Установка

### Требования
- Python 3.10+
- pip

### Шаги установки

1. Клонируйте или скачайте проект

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Создайте необходимые директории:
```bash
mkdir -p data/raw data/processed models logs
```

## Использование

### Тест подключения к MQTT Lab

Проверьте подключение к облачному сервису MQTT Lab:

```bash
cd src/capture
python mqtt_client.py
```

Ожидаемый вывод:
```
=== Тест подключения к MQTT Lab ===
Брокер: broker.hivemq.com:1883
Топик: spBv1.0/#

Подключено к брокеру broker.hivemq.com:1883
Подписка на топик: spBv1.0/#
Ожидание сообщений...
[spBv1.0/...] ...
```

### Запуск основной системы

Запустите систему обнаружения аномалий:

```bash
cd src/detector
python anomaly_detector.py
```

Система выполнит следующие шаги:
1. Подключится к MQTT Lab (broker.hivemq.com:1883)
2. Соберёт 1000 сообщений для обучения модели
3. Обучит модель Isolation Forest
4. Начнёт детектирование аномалий в реальном времени

### Тестирование отдельных модулей

**Тест базы данных:**
```bash
cd src/storage
python db_manager.py
```

**Тест экстрактора признаков:**
```bash
cd src/features
python extractor.py
```

**Тест ML-модели:**
```bash
cd src/ml
python model.py
```

## Конфигурация

Параметры системы можно изменить в файле `src/detector/anomaly_detector.py`:

```python
system = AnomalyDetectionSystem(
    broker_host="broker.hivemq.com",  # Хост MQTT-брокера
    broker_port=1883,                  # Порт
    topic_pattern="spBv1.0/#",        # Паттерн топиков
    db_path="data/anomalies.db",      # Путь к БД
    model_path="models/isolation_forest.pkl",  # Путь к модели
    window_size_seconds=60,            # Размер временного окна
    contamination=0.05                  # Ожидаемая доля аномалий
)
```

## MQTT Lab

Для работы с реальным Sparkplug B трафиком используется облачный сервис:

- **URL**: https://mqttlab.iotsim.io/sparkplug/
- **Брокер**: broker.hivemq.com
- **Порт**: 1883
- **Топик**: spBv1.0/#

Преимущества:
- Не требует установки дополнительного ПО
- 100 виртуальных узлов с реальными данными
- Возможность создавать аномалии через интерфейс

## Архитектура

```
MQTT Lab → TrafficCapture → FeatureExtractor → 
AnomalyDetector → DatabaseManager
```

1. **TrafficCapture**: Подключается к MQTT-брокеру и подписывается на топики
2. **FeatureExtractor**: Создаёт временные окна и вычисляет признаки
3. **AnomalyDetector**: Использует Isolation Forest для детектирования
4. **DatabaseManager**: Сохраняет аномалии в SQLite

## Технологический стек

| Компонент | Технология |
|-----------|-----------|
| Язык | Python 3.10+ |
| MQTT | paho-mqtt |
| ML | scikit-learn (Isolation Forest) |
| База данных | SQLite |
| Данные | Pandas, NumPy |
| Логирование | logging |

## Разработка

### Запуск тестов
```bash
pytest tests/
```

### Форматирование кода
```bash
black src/
```

## Лицензия

Проект создан в рамках ВКР.

## Контакты

Для вопросов и предложений обращайтесь к автору.
