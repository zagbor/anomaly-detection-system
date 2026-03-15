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
db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "anomalies.db")
db = DatabaseManager(db_path)


@app.route('/')
def dashboard():
    """Главная страница с дашбордом"""
    return render_template('dashboard.html')


@app.route('/api/anomalies')
def get_anomalies():
    """API: Получить последние аномалии"""
    limit = request.args.get('limit', 50, type=int)
    anomalies = db.get_recent_anomalies(limit=limit)
    
    # Преобразование datetime в строку для JSON
    for anomaly in anomalies:
        if anomaly['timestamp']:
            anomaly['timestamp'] = anomaly['timestamp'].isoformat()
        if anomaly['created_at']:
            anomaly['created_at'] = anomaly['created_at'].isoformat()
    
    return jsonify(anomalies)


@app.route('/api/anomalies/<device_id>')
def get_device_anomalies(device_id):
    """API: Получить аномалии для конкретного устройства"""
    limit = request.args.get('limit', 50, type=int)
    anomalies = db.get_anomalies_by_device(device_id, limit)
    
    for anomaly in anomalies:
        if anomaly['timestamp']:
            anomaly['timestamp'] = anomaly['timestamp'].isoformat()
        if anomaly['created_at']:
            anomaly['created_at'] = anomaly['created_at'].isoformat()
    
    return jsonify(anomalies)


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
    
    for device in devices:
        if device['first_seen']:
            device['first_seen'] = device['first_seen'].isoformat()
        if device['last_seen']:
            device['last_seen'] = device['last_seen'].isoformat()
    
    return jsonify(devices)


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
    
    summary = {
        "anomalies_24h": stats_24h['total'],
        "anomalies_1h": stats_1h['total'],
        "devices_count": len(devices),
        "by_severity_24h": stats_24h['by_severity'],
        "by_type_24h": stats_24h['by_type'],
        "recent_anomalies": recent_anomalies,
        "top_devices": sorted(devices, key=lambda x: x['tags_count'], reverse=True)[:5]
    }
    
    return jsonify(summary)


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
    print("Откройте в браузере: http://localhost:5000")
    print()
    
    app.run(host='0.0.0.0', port=5000, debug=True)


if __name__ == "__main__":
    main()
