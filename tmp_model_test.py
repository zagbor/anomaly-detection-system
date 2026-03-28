import pickle
import numpy as np

# Load the model
with open('data/../models/isolation_forest.pkl', 'rb') as f:
    model_data = pickle.load(f)

model = model_data['model']
scaler = model_data['scaler']

print(f"Model trained on {model_data['training_samples']} samples")
print(f"Contamination: {model_data['contamination']}")

# Let's inspect the scaler mean and scale
print(f"Scaler Mean: {scaler.mean_}")
print(f"Scaler Scale: {scaler.scale_}")

# Let's create a "normal" sample (around the mean)
normal_sample = scaler.mean_.reshape(1, -1)
print(f"\nNormal Sample (mean): {normal_sample[0]}")
normal_scaled = scaler.transform(normal_sample)
print(f"Normal Scaled: {normal_scaled[0]}")
normal_score = model.decision_function(normal_scaled)[0]
print(f"Normal Score: {normal_score:.4f}")

# Let's create an extreme spike in value (Feature 0 is usually value)
# Assuming features: [value, mean, std, rate_of_change]
spike_sample = np.copy(normal_sample)
spike_sample[0, 0] = 10000000000.0
spike_sample[0, 3] = 10000000000.0 / 10.0 # huge rate of change

print(f"\nSpike Sample: {spike_sample[0]}")
spike_scaled = scaler.transform(spike_sample)
print(f"Spike Scaled: {spike_scaled[0]}")
spike_score = model.decision_function(spike_scaled)[0]
print(f"Spike Score: {spike_score:.4f} -> Prob: {abs(spike_score)*2:.2%}")

# Try finding the theoretical minimum score this model can give
test_samples = []
for p in [2, 10, 100, 1000, 10000, 1e6, 1e10, 1e20]:
    s = np.copy(normal_sample)
    s[0] = s[0] * p
    test_samples.append(s[0])

test_scaled = scaler.transform(np.array(test_samples))
scores = model.decision_function(test_scaled)
for p, score in zip([2, 10, 100, 1000, 10000, 1e6, 1e10, 1e20], scores):
    print(f"Multiplier {p}: Score = {score:.4f} -> Prob: {abs(score)*2:.2%}")

