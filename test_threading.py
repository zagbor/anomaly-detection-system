"""
Проверка работы threading
"""

import threading
import time

def worker():
    for i in range(5):
        print(f"Thread working: {i}")
        time.sleep(1)

print("Запуск потока...")
thread = threading.Thread(target=worker)
thread.start()

for i in range(5):
    print(f"Main thread: {i}")
    time.sleep(1)

thread.join()
print("Поток завершён")
