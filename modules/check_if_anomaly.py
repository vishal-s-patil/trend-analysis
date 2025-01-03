from sklearn.ensemble import IsolationForest
import numpy as np

file_name = 'session_train.data'

# Example training data (list of numbers)
with open(file_name, 'r') as file:
    training_data = [float(line.strip()) for line in file if line.strip()]


X_train = np.array(training_data).reshape(-1, 1)

model = IsolationForest(contamination=0.24, random_state=42)  # 10% contamination assumed
model.fit(X_train)


def check_outliers(new_data, model):
    X_new = np.array(new_data).reshape(-1, 1)

    # Predict (-1 indicates outliers, 1 indicates inliers)
    predictions = model.predict(X_new)

    results = [(value, "Outlier" if pred == -1 else "Inlier") for value, pred in zip(new_data, predictions)]
    return results


new_data = [11, 12, 14, 50, 60]
outlier_results = check_outliers(new_data, model)

# Print results
for value, status in outlier_results:
    print(f"Value: {value}, Status: {status}")
