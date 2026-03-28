from flask import Flask, render_template, request, jsonify
import sys
import os
import threading
import time
import random
import logging

# Настройка логирования
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log_dir = os.path.join(base_dir, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'system.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Добавляем родительскую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from capture.test_mqtt_publisher import TestMQTTPublisher
from storage.db_manager import DatabaseManager

app = Flask(__name__)

# Инициализация БД
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
db_path = os.path.join(project_root, "data", "anomalies.db")
db = DatabaseManager(db_path)

publisher = TestMQTTPublisher()
publisher.connect()

# Состояние генератора
state = {
    "active": False,
    "group_id": "zagbor_group",
    "node_id": "zagbor_node",
    "device_id": "test_device",
    "tag_name": "test_signal",
    "base_value": 50.0,
    "current_value": 50.0,
    "drift": 0.0,
    "noise": 0.0,
    "last_sent": None
}

def generator_loop():
    """Фоновый поток для непрерывной генерации сигнала"""
    while True:
        if state["active"]:
            # Вносим шум и дрейф
            state["base_value"] += state["drift"]
            noise_val = random.uniform(-state["noise"], state["noise"])
            state["current_value"] = state["base_value"] + noise_val
            
            # Отправка в MQTT
            publisher.publish_metric(
                state["group_id"], 
                state["node_id"], 
                state["device_id"], 
                state["tag_name"], 
                state["current_value"]
            )
            state["last_sent"] = time.time()
            
        time.sleep(1.0) # Отправляем раз в секунду

# Запуск фонового потока
threading.Thread(target=generator_loop, daemon=True).start()

@app.route('/')
def index():
    return render_template('generator_ui.html', state=state)

@app.route('/api/devices')
def get_devices():
    """Получить список устройств из БД"""
    devices = db.get_devices()
    return jsonify(devices)

@app.route('/api/tags/<device_id>')
def get_tags(device_id):
    """Получить теги для конкретного устройства"""
    tags = db.get_device_tags(device_id)
    return jsonify(tags)

@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings():
    """Управление общими настройками (захват трафика)"""
    if request.method == 'POST':
        data = request.json
        if 'traffic_capture_enabled' in data:
            db.set_setting('traffic_capture_enabled', data['traffic_capture_enabled'])
        return jsonify({"status": "ok"})
    
    # GET: возвращаем текущие настройки
    return jsonify({
        "traffic_capture_enabled": db.get_setting('traffic_capture_enabled', 'true')
    })

@app.route('/api/configure', methods=['POST'])
def configure():
    data = request.json
    global state
    
    # Обновляем состояние
    for key, value in data.items():
        if key in state:
            if key in ['base_value', 'drift', 'noise']:
                try: state[key] = float(value)
                except: pass
            else:
                state[key] = value
                
    # Обработка действий (старт/стоп)
    action = data.get('action')
    if action == 'start' or data.get('active') is True:
        state["active"] = True
        logger.info(f"GENERATOR: Start/Update generation for device={state['device_id']}, tag={state['tag_name']}")
        logger.info(f"  Params: base={state['base_value']}, noise={state['noise']}, drift={state['drift']}")
    elif action == 'stop' or data.get('active') is False:
        state["active"] = False
        logger.info(f"GENERATOR: Stop/Update generation for device={state['device_id']}")
    
    return jsonify({"status": "ok", "active": state["active"]})

@app.route('/api/spike', methods=['POST'])
@app.route('/api/inject_spike', methods=['POST'])
def inject_spike():
    data = request.json
    
    # Поддержка разных форматов данных
    # Напрямую используем введенное значение как абсолютное (а не относительное)
    spike_value = float(data.get('value', data.get('amplitude', 50.0)))
    
    device_id = state['device_id']
    tag_name = state['tag_name']
    group_id = state['group_id']
    node_id = state['node_id']
    
    logger.info(f"GENERATOR: Injecting SPIKE! device={device_id}, tag={tag_name}, value={spike_value}")
    
    # Отправляем один аномальный замер
    publisher.publish_metric(
        group_id=group_id,
        node_id=node_id,
        device_id=device_id,
        tag_name=tag_name,
        value=spike_value
    )
    
    return jsonify({"status": "spike_injected", "value": spike_value})

if __name__ == '__main__':
    # Запуск на порту 5001
    app.run(host='0.0.0.0', port=5001, debug=False)
