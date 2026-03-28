import sqlite3
import sys

with open('tmp_out.txt', 'w', encoding='utf-8') as f:
    sys.stdout = f

    print("--- RECENT ANOMALIES ---")
    conn = sqlite3.connect('data/anomalies.db')
    c = conn.cursor()
    c.execute("SELECT id, timestamp, device_id, severity, anomaly_score, description FROM anomalies ORDER BY id DESC LIMIT 10")
    for row in c.fetchall():
        print(row)
    conn.close()

    print("\n--- LATEST DETECTOR LOG ---")
    try:
        with open('logs/system.log', 'r', encoding='utf-8', errors='replace') as log_f:
            lines = log_f.readlines()
            for line in lines[-100:]:
                print(line.strip())
    except Exception as e:
        print(e)
