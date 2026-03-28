"""
Модуль машинного обучения
Реализует модель Isolation Forest для обнаружения аномалий
"""

import numpy as np
import pickle
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import Optional, Tuple, List
import logging
from datetime import datetime
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Детектор аномалий на основе Isolation Forest"""
    
    def __init__(
        self,
        contamination: float = 0.05,
        random_state: int = 42,
        model_path: str = "models/isolation_forest.pkl"
    ):
        """
        Инициализация детектора аномалий
        
        Args:
            contamination: Ожидаемая доля аномалий (0.0 - 0.5)
            random_state: Сид для воспроизводимости
            model_path: Путь для сохранения/загрузки модели
        """
        self.contamination = contamination
        self.random_state = random_state
        self.model_path = model_path
        
        # Модель Isolation Forest
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100,
            max_samples='auto',
            n_jobs=-1
        )
        
        # Scaler для нормализации данных
        self.scaler = StandardScaler()
        
        # Флаг обучения
        self.is_trained = False
        
        # Статистика обучения
        self.training_samples = 0
        self.training_time = None
    
    def train(self, X: np.ndarray) -> bool:
        """
        Обучить модель на нормальных данных
        
        Args:
            X: Матрица признаков (n_samples, n_features)
            
        Returns:
            True если обучение успешно
        """
        try:
            logger.info(f"Начало обучения модели...")
            logger.info(f"Размер обучающей выборки: {X.shape}")
            
            start_time = datetime.now()
            
            # Нормализация данных
            X_scaled = self.scaler.fit_transform(X)
            
            # Обучение модели
            self.model.fit(X_scaled)
            
            self.training_time = (datetime.now() - start_time).total_seconds()
            self.training_samples = X.shape[0]
            self.is_trained = True
            
            logger.info(f"Модель обучена за {self.training_time:.2f} сек")
            logger.info(f"Обучено на {self.training_samples} образцах")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обучении модели: {e}")
            return False
    
    def predict(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Предсказать аномалии
        
        Args:
            X: Матрица признаков (n_samples, n_features)
            
        Returns:
            Кортеж (labels, scores):
                - labels: 1 = норма, -1 = аномалия
                - scores: оценка аномальности (чем меньше, тем более аномально)
        """
        if not self.is_trained:
            logger.warning("Модель не обучена. Результаты могут быть некорректными.")
        
        try:
            # Нормализация данных
            X_scaled = self.scaler.transform(X)
            
            # Предсказание
            labels = self.model.predict(X_scaled)
            scores = self.model.decision_function(X_scaled)
            
            return labels, scores
            
        except Exception as e:
            logger.error(f"Ошибка при предсказании: {e}")
            return np.array([]), np.array([])
    
    def predict_single(self, features: np.ndarray) -> Tuple[int, float]:
        """
        Предсказать для одного образца
        
        Args:
            features: Вектор признаков (n_features,)
            
        Returns:
            Кортеж (label, score):
                - label: 1 = норма, -1 = аномалия
                - score: оценка аномальности
        """
        # Преобразуем в 2D массив
        X = features.reshape(1, -1)
        
        labels, scores = self.predict(X)
        
        if len(labels) > 0:
            return labels[0], scores[0]
        else:
            return 1, 0.0
    
    def get_anomaly_probability(self, score: float) -> float:
        """
        Преобразовать score в вероятность аномалии
        
        Args:
            score: Оценка аномальности от модели
            
        Returns:
            Вероятность аномалии (0.0 - 1.0)
        """
        # Isolation Forest в sklearn выдает score от 0 до ~ -0.3 для аномалий, 
        # зависящий от contamination_offset. Для растягивания до [0, 1] 
        # используем более агрессивный множитель.
        if score < 0:
            return min(1.0, abs(score) * 5.0)
        else:
            return max(0.0, 1.0 - score)
    
    def save_model(self, path: Optional[str] = None) -> bool:
        """
        Сохранить модель в файл
        
        Args:
            path: Путь для сохранения (если None, используется model_path из __init__)
            
        Returns:
            True если сохранение успешно
        """
        if path is None:
            path = self.model_path
        
        try:
            # Создаём директорию если не существует
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # Сохраняем модель и scaler
            model_data = {
                'model': self.model,
                'scaler': self.scaler,
                'contamination': self.contamination,
                'random_state': self.random_state,
                'is_trained': self.is_trained,
                'training_samples': self.training_samples,
                'training_time': self.training_time
            }
            
            with open(path, 'wb') as f:
                pickle.dump(model_data, f)
            
            logger.info(f"Модель сохранена: {path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении модели: {e}")
            return False
    
    def load_model(self, path: Optional[str] = None) -> bool:
        """
        Загрузить модель из файла
        
        Args:
            path: Путь к файлу модели (если None, используется model_path из __init__)
            
        Returns:
            True если загрузка успешно
        """
        if path is None:
            path = self.model_path
        
        try:
            with open(path, 'rb') as f:
                model_data = pickle.load(f)
            
            self.model = model_data['model']
            self.scaler = model_data['scaler']
            self.contamination = model_data['contamination']
            self.random_state = model_data['random_state']
            self.is_trained = model_data['is_trained']
            self.training_samples = model_data['training_samples']
            self.training_time = model_data['training_time']
            
            logger.info(f"Модель загружена: {path}")
            logger.info(f"Обучена на {self.training_samples} образцах")
            return True
            
        except FileNotFoundError:
            logger.error(f"Файл модели не найден: {path}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при загрузке модели: {e}")
            return False
    
    def get_info(self) -> dict:
        """
        Получить информацию о модели
        
        Returns:
            Словарь с информацией о модели
        """
        return {
            "is_trained": self.is_trained,
            "contamination": self.contamination,
            "random_state": self.random_state,
            "training_samples": self.training_samples,
            "training_time": self.training_time,
            "model_path": self.model_path
        }


def main():
    """Тестовая функция для проверки модели"""
    print("=== Тест модели Isolation Forest ===")
    
    # Создание детектора
    detector = AnomalyDetector(contamination=0.1)
    
    # Генерация тестовых данных
    np.random.seed(42)
    
    # Нормальные данные (гауссово распределение)
    normal_data = np.random.randn(100, 5)
    
    # Аномальные данные (выбросы)
    anomaly_data = np.random.randn(10, 5) * 5 + 10
    
    # Объединяем данные
    all_data = np.vstack([normal_data, anomaly_data])
    
    # Обучение только на нормальных данных
    print("\nОбучение на нормальных данных...")
    detector.train(normal_data)
    
    # Предсказание
    print("\nПредсказание на всех данных...")
    labels, scores = detector.predict(all_data)
    
    # Статистика
    n_anomalies = np.sum(labels == -1)
    n_normal = np.sum(labels == 1)
    
    print(f"\nРезультаты:")
    print(f"  Всего образцов: {len(all_data)}")
    print(f"  Нормальных: {n_normal}")
    print(f"  Аномалий: {n_anomalies}")
    print(f"  Ожидаемая доля аномалий: {detector.contamination}")
    
    # Пример предсказания для одного образца
    test_sample = np.array([0, 0, 0, 0, 0])
    label, score = detector.predict_single(test_sample)
    probability = detector.get_anomaly_probability(score)
    
    print(f"\nПредсказание для тестового образца {test_sample}:")
    print(f"  Метка: {'аномалия' if label == -1 else 'норма'}")
    print(f"  Score: {score:.4f}")
    print(f"  Вероятность аномалии: {probability:.2%}")
    
    # Сохранение модели
    print("\nСохранение модели...")
    detector.save_model("models/test_model.pkl")
    
    # Информация о модели
    info = detector.get_info()
    print(f"\nИнформация о модели:")
    for key, value in info.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
