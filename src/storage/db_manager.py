"""
Модуль для работы с SQLite базой данных
Хранит обнаруженные аномалии и логи системы
"""

import sqlite3
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class AnomalyRecord:
    """Запись об аномалии"""
    timestamp: datetime
    device_id: str
    tag_name: str
    anomaly_score: float
    anomaly_type: str
    description: str
    severity: str  # 'low', 'medium', 'high', 'critical'


class DatabaseManager:
    """Менеджер базы данных SQLite"""
    
    def __init__(self, db_path: str = "data/anomalies.db"):
        """
        Инициализация менеджера базы данных
        
        Args:
            db_path: Путь к файлу базы данных
        """
        self.db_path = db_path
        self.conn = None
        self._init_database()
    
    def _init_database(self):
        """Создание таблиц базы данных"""
        try:
            # Создаём директорию если не существует
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            
            cursor = self.conn.cursor()
            
            # Таблица аномалий
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS anomalies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    device_id TEXT NOT NULL,
                    tag_name TEXT,
                    anomaly_score REAL NOT NULL,
                    anomaly_type TEXT NOT NULL,
                    description TEXT,
                    severity TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица устройств
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS devices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT UNIQUE NOT NULL,
                    node_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    first_seen DATETIME NOT NULL,
                    last_seen DATETIME NOT NULL,
                    tags_count INTEGER DEFAULT 0
                )
            ''')
            
            # Таблица тегов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT NOT NULL,
                    tag_name TEXT NOT NULL,
                    tag_type TEXT,
                    first_seen DATETIME NOT NULL,
                    last_seen DATETIME NOT NULL,
                    value_min REAL,
                    value_max REAL,
                    value_avg REAL,
                    FOREIGN KEY (device_id) REFERENCES devices(device_id)
                )
            ''')
            
            # Индексы для ускорения запросов
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_device ON anomalies(device_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity)')
            
            self.conn.commit()
            logger.info(f"База данных инициализирована: {self.db_path}")
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка инициализации базы данных: {e}")
            raise
    
    def save_anomaly(self, anomaly: AnomalyRecord) -> int:
        """
        Сохранить запись об аномалии
        
        Args:
            anomaly: Объект AnomalyRecord
            
        Returns:
            ID вставленной записи
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO anomalies 
                (timestamp, device_id, tag_name, anomaly_score, anomaly_type, description, severity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                anomaly.timestamp,
                anomaly.device_id,
                anomaly.tag_name,
                anomaly.anomaly_score,
                anomaly.anomaly_type,
                anomaly.description,
                anomaly.severity
            ))
            
            self.conn.commit()
            anomaly_id = cursor.lastrowid
            logger.info(f"Аномалия сохранена: ID={anomaly_id}, device={anomaly.device_id}")
            return anomaly_id
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка сохранения аномалии: {e}")
            self.conn.rollback()
            return -1
    
    def get_recent_anomalies(self, limit: int = 100) -> List[Dict]:
        """
        Получить последние аномалии
        
        Args:
            limit: Максимальное количество записей
            
        Returns:
            Список словарей с данными аномалий
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT * FROM anomalies 
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (limit,))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения аномалий: {e}")
            return []
    
    def get_anomalies_by_device(self, device_id: str, limit: int = 50) -> List[Dict]:
        """
        Получить аномалии для конкретного устройства
        
        Args:
            device_id: ID устройства
            limit: Максимальное количество записей
            
        Returns:
            Список словарей с данными аномалий
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT * FROM anomalies 
                WHERE device_id = ?
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (device_id, limit))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения аномалий устройства: {e}")
            return []
    
    def get_anomaly_statistics(self, hours: int = 24) -> Dict:
        """
        Получить статистику аномалий за период
        
        Args:
            hours: Период в часах
            
        Returns:
            Словарь со статистикой
        """
        try:
            cursor = self.conn.cursor()
            
            # Общее количество
            cursor.execute('''
                SELECT COUNT(*) FROM anomalies 
                WHERE timestamp >= datetime('now', '-' || ? || ' hours')
            ''', (hours,))
            total = cursor.fetchone()[0]
            
            # По типам
            cursor.execute('''
                SELECT anomaly_type, COUNT(*) as count
                FROM anomalies 
                WHERE timestamp >= datetime('now', '-' || ? || ' hours')
                GROUP BY anomaly_type
            ''', (hours,))
            by_type = {row[0]: row[1] for row in cursor.fetchall()}
            
            # По严重程度
            cursor.execute('''
                SELECT severity, COUNT(*) as count
                FROM anomalies 
                WHERE timestamp >= datetime('now', '-' || ? || ' hours')
                GROUP BY severity
            ''', (hours,))
            by_severity = {row[0]: row[1] for row in cursor.fetchall()}
            
            return {
                "total": total,
                "by_type": by_type,
                "by_severity": by_severity,
                "period_hours": hours
            }
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {"total": 0, "by_type": {}, "by_severity": {}, "period_hours": hours}
    
    def register_device(self, device_id: str, node_id: str, group_id: str) -> bool:
        """
        Зарегистрировать новое устройство или обновить время последнего seen
        
        Args:
            device_id: ID устройства
            node_id: ID узла
            group_id: ID группы
            
        Returns:
            True если успешно
        """
        try:
            cursor = self.conn.cursor()
            now = datetime.now()
            
            cursor.execute('''
                INSERT OR REPLACE INTO devices 
                (device_id, node_id, group_id, first_seen, last_seen)
                VALUES (?, ?, ?, 
                    COALESCE((SELECT first_seen FROM devices WHERE device_id = ?), ?),
                    ?
                )
            ''', (device_id, node_id, group_id, device_id, now, now))
            
            self.conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка регистрации устройства: {e}")
            return False
    
    def get_devices(self) -> List[Dict]:
        """
        Получить список всех устройств
        
        Returns:
            Список словарей с данными устройств
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM devices ORDER BY last_seen DESC')
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения устройств: {e}")
            return []
    
    def close(self):
        """Закрыть соединение с базой данных"""
        if self.conn:
            self.conn.close()
            logger.info("Соединение с базой данных закрыто")


def main():
    """Тестовая функция для проверки работы базы данных"""
    print("=== Тест базы данных ===")
    
    # Создание менеджера
    db = DatabaseManager("data/test_anomalies.db")
    
    # Тестовая аномалия
    test_anomaly = AnomalyRecord(
        timestamp=datetime.now(),
        device_id="Boiler1",
        tag_name="Temperature",
        anomaly_score=0.95,
        anomaly_type="value_out_of_range",
        description="Температура превышает допустимый предел",
        severity="high"
    )
    
    # Сохранение
    anomaly_id = db.save_anomaly(test_anomaly)
    print(f"Аномалия сохранена с ID: {anomaly_id}")
    
    # Регистрация устройства
    db.register_device("Boiler1", "BoilerRoom", "Factory1")
    print("Устройство зарегистрировано")
    
    # Получение аномалий
    anomalies = db.get_recent_anomalies(limit=5)
    print(f"\nПоследние аномалии ({len(anomalies)}):")
    for anomaly in anomalies:
        print(f"  - {anomaly['device_id']}: {anomaly['description']}")
    
    # Статистика
    stats = db.get_anomaly_statistics(hours=24)
    print(f"\nСтатистика за 24 часа:")
    print(f"  Всего аномалий: {stats['total']}")
    print(f"  По типам: {stats['by_type']}")
    print(f"  По严重程度: {stats['by_severity']}")
    
    # Закрытие
    db.close()
    print("\nТест завершен")


if __name__ == "__main__":
    main()
