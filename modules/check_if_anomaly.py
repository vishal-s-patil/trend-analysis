from sklearn.ensemble import IsolationForest
import numpy as np

file_name = '../session_train.data'

with open(file_name, 'r') as file:
    training_data = [float(line.strip()) for line in file if line.strip()]

X_train = np.array(training_data).reshape(-1, 1)

model = IsolationForest(contamination=0.50, random_state=42)
model.fit(X_train)


def check_outliers(new_data, model):
    X_new = np.array(new_data).reshape(-1, 1)

    predictions = model.predict(X_new)

    results = [(value, "Outlier" if pred == -1 else "Inlier") for value, pred in zip(new_data, predictions)]
    return results


with open('../session_test.data', 'r') as file:
    new_data = [float(line.strip()) for line in file if line.strip()]
outlier_results = check_outliers(new_data, model)


for value, status in outlier_results:
    with open('output.out', 'a') as file:
        file.write(f"Value: {value}, Status: {status}\n")
