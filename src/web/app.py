"""
Веб-интерфейс системы обнаружения аномалий
Flask приложение с дашбордом и API
"""

import sys
import os
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request

# Добавляем родительскую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.db_manager import DatabaseManager

app = Flask(__name__)
app.config['SECRET_KEY'] = 'anomaly-detection-secret-key-2026'

# Инициализация базы данных
# Путь относительно корня проекта (src/web/app.py -> src/web -> src -> root)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
db_path = os.path.join(project_root, "data", "anomalies.db")
db = DatabaseManager(db_path)

def safe_serialize(obj):
    """Безопасная сериализация данных для JSON"""
    if isinstance(obj, list):
        return [safe_serialize(item) for item in obj]
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            if k in ['timestamp', 'created_at', 'first_seen', 'last_seen'] and v:
                if hasattr(v, 'isoformat'):
                    new_dict[k] = v.isoformat()
                else:
                    new_dict[k] = str(v)
            elif k == 'payload' and v:
                # Очистка payload от непечатаемых символов для JSON
                if isinstance(v, bytes):
                    new_dict[k] = v.decode('utf-8', errors='replace')
                else:
                    new_dict[k] = str(v).encode('ascii', 'replace').decode()
            else:
                new_dict[k] = v
        return new_dict
    return obj


@app.route('/')
def dashboard():
    """Главная страница с дашбордом"""
    return render_template('dashboard.html')


@app.route('/traffic')
def traffic_page():
    """Отдельная страница мониторинга трафика (Live) с фильтрами"""
    return render_template('traffic.html')


@app.route('/anomalies')
def anomalies_page():
    """Отдельная страница журнала аномалий с фильтрами"""
    return render_template('anomalies.html')


@app.route('/api/anomalies')
def get_anomalies():
    """API: Получить последние аномалии"""
    limit = request.args.get('limit', 50, type=int)
    anomalies = db.get_recent_anomalies(limit=limit)
    return jsonify(safe_serialize(anomalies))


@app.route('/api/anomalies/<device_id>')
def get_device_anomalies(device_id):
    """API: Получить аномалии для конкретного устройства"""
    limit = request.args.get('limit', 50, type=int)
    anomalies = db.get_anomalies_by_device(device_id, limit)
    return jsonify(safe_serialize(anomalies))


@app.route('/api/statistics')
def get_statistics():
    """API: Получить статистику аномалий"""
    hours = request.args.get('hours', 24, type=int)
    stats = db.get_anomaly_statistics(hours=hours)
    
    return jsonify(stats)


@app.route('/api/devices')
def get_devices():
    """API: Получить список устройств"""
    devices = db.get_devices()
    return jsonify(safe_serialize(devices))


@app.route('/api/traffic/<device_id>')
def get_device_traffic(device_id):
    """API: Получить историю трафика для конкретного устройства"""
    limit = request.args.get('limit', 100, type=int)
    traffic = db.get_device_traffic(device_id, limit=limit)
    return jsonify(safe_serialize(traffic))


@app.route('/api/tags/<device_id>')
def get_device_tags(device_id):
    """API: Получить список тегов для конкретного устройства"""
    tags = db.get_device_tags(device_id)
    return jsonify(safe_serialize(tags))


@app.route('/api/traffic')
def get_traffic():
    """API: Получить последние сообщения трафика"""
    limit = request.args.get('limit', 20, type=int)
    traffic = db.get_recent_traffic(limit=limit)
    return jsonify(safe_serialize(traffic))


@app.route('/api/summary')
def get_summary():
    """API: Получить сводную информацию для дашборда"""
    # Статистика за 24 часа
    stats_24h = db.get_anomaly_statistics(hours=24)
    
    # Статистика за 1 час
    stats_1h = db.get_anomaly_statistics(hours=1)
    
    # Последние аномалии
    recent_anomalies = db.get_recent_anomalies(limit=5)
    
    # Устройства
    devices = db.get_devices()
    
    # Последний трафик
    recent_traffic = db.get_recent_traffic(limit=10)
    
    summary = {
        "anomalies_24h": stats_24h['total'],
        "anomalies_1h": stats_1h['total'],
        "devices_count": len(devices),
        "by_severity_24h": stats_24h['by_severity'],
        "by_type_24h": stats_24h['by_type'],
        "recent_anomalies": safe_serialize(recent_anomalies),
        "recent_traffic": safe_serialize(recent_traffic),
        "top_devices": safe_serialize(sorted(devices, key=lambda x: x['tags_count'], reverse=True)[:5]),
        "settings": {
            "training_period_minutes": db.get_setting('training_period_minutes', '60'),
            "system_mode": db.get_setting('system_mode', 'collect')
        },
        "training_status": {
            "elapsed_minutes": 0,
            "remaining_minutes": 0,
            "progress_percent": 100
        }
    }
    
    # Расчет прогресса обучения
    if summary["settings"]["system_mode"] == 'collect':
        try:
            start_str = db.get_setting('collection_start_time')
            if start_str:
                start_time = datetime.fromisoformat(start_str)
                period = int(summary["settings"]["training_period_minutes"])
                elapsed = (datetime.now() - start_time).total_seconds() / 60
                
                remaining = max(0, period - elapsed)
                progress = min(100, (elapsed / period * 100) if period > 0 else 100)
                
                summary["training_status"] = {
                    "elapsed_minutes": round(elapsed, 1),
                    "remaining_minutes": round(remaining, 1),
                    "progress_percent": round(progress, 1)
                }
        except Exception as e:
            app.logger.error(f"Error calculating progress: {e}")

    return jsonify(summary)


@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings():
    """API: Получить или обновить настройки"""
    if request.method == 'POST':
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        if 'training_period_minutes' in data:
            db.set_setting('training_period_minutes', data['training_period_minutes'])
        if 'system_mode' in data:
            db.set_setting('system_mode', data['system_mode'])
            # Если включаем сбор, сбрасываем время старта
            if data['system_mode'] == 'collect':
                db.set_setting('collection_start_time', datetime.now().isoformat())
            
        return jsonify({"status": "success"})
    
    # GET
    return jsonify({
        "training_period_minutes": db.get_setting('training_period_minutes', '60'),
        "system_mode": db.get_setting('system_mode', 'collect')
    })


@app.errorhandler(404)
def not_found(error):
    """Обработка ошибки 404"""
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    """Обработка ошибки 500"""
    return jsonify({"error": "Internal server error"}), 500


def main():
    """Запуск Flask приложения"""
    print("=== Запуск веб-сервера ===")
    print(f"Используемая база данных: {os.path.abspath(db_path)}")
    print("Откройте в браузере: http://localhost:5000")
    print()
    
    app.run(host='0.0.0.0', port=5000, debug=True)


if __name__ == "__main__":
    main()
