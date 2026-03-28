@echo off
echo Остановка всех Python-процессов...
taskkill /F /IM python.exe > nul 2>&1

echo Запуск ретранслятора внешних данных (mqtt_relay.py)...
start "MQTT Relay" .\.venv\Scripts\python.exe mqtt_relay.py

echo Запуск детектора аномалий...
start "Anomaly Detector" .\.venv\Scripts\python.exe src\detector\anomaly_detector.py

echo Запуск основного веб-дашборда (порт 5000)...
start "Dashboard" .\.venv\Scripts\python.exe src\web\app.py

echo Запуск генератора тестового трафика (порт 5001)...
start "Generator" .\.venv\Scripts\python.exe src\web\generator_app.py

echo Все 4 компонента успешно запущены в отдельных окнах!
