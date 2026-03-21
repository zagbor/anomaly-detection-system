import sqlite3
import os

db_path = os.path.join("data", "anomalies.db")

print(f"Checking database at: {os.path.abspath(db_path)}")

if not os.path.exists("data"):
    os.makedirs("data")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Проверка и создание таблицы traffic
cursor.execute('''
    CREATE TABLE IF NOT EXISTS traffic (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        topic TEXT NOT NULL,
        device_id TEXT,
        tag_name TEXT,
        value REAL,
        payload TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# Проверка и создание таблицы tags
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

# Создание индексов
cursor.execute('CREATE INDEX IF NOT EXISTS idx_traffic_timestamp ON traffic(timestamp)')

conn.commit()
conn.close()

print("Database schema fixed successfully!")
