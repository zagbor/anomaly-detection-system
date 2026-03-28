import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

X_train = np.array([[60.0, 1.0, 50.0, 0.5, 48.0, 52.0]])
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_train)

clf = IsolationForest(contamination=0.1, random_state=42)
clf.fit(X_scaled)

X_test = np.array([[10.0, 1.0, 100115.0, 0.0, 100115.0, 100115.0]])
X_test_scaled = scaler.transform(X_test)
pred = clf.predict(X_test_scaled)
print(f"Prediction: {pred}")
