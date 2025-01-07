from sklearn.ensemble import IsolationForest
import numpy as np
from datetime import datetime
import re

# File paths
# train_file_name = '../session_train.data'
train_file_name = 'train_test_2.csv'
test_file_name = 'sep_dec_test_data.csv'
output_file_name = 'output.out'

# Read training data (date, count) pairs
pattern = r'^\s*([\d-]+\s[\d:]+)\s*\|\s*(\d+)\s*$'
training_data = []
test_data = []
with open(train_file_name, 'r') as file:
    for line_no, line in enumerate(file, start=1):
        try:
            line = line.strip()
            match = re.match(pattern, line)
            if match:
                date, count = match.groups()
                # if line_no <= 20000:
                training_data.append((date, float(count)))
                # else:
                # test_data.append((date, float(count)))
            else:
                print(f"Skipping invalid line {line_no}: {line}")
        except Exception as e:
            print(f"Error processing line {line_no}: {line} -> {e}")

with open(test_file_name, 'r') as file:
    for line_no, line in enumerate(file, start=1):
        try:
            line = line.strip()
            match = re.match(pattern, line)
            if match:
                date, count = match.groups()
                # if line_no <= 20000:
                #     training_data.append((date, float(count)))
                # else:
                # training_data.append((date, float(count)))
                test_data.append((date, float(count)))
            else:
                print(f"Skipping invalid line {line_no}: {line}")
        except Exception as e:
            print(f"Error processing line {line_no}: {line} -> {e}")

# Extract counts for training
X_train = np.array([count for _, count in training_data]).reshape(-1, 1)

# Train the Isolation Forest model
model = IsolationForest(contamination=0.15, random_state=42)
model.fit(X_train)


# with open(test_file_name, 'r') as file:
#     for line in file:
#         if line.strip():
#             date, count = line.strip().split(',')
#             test_data.append((date, float(count)))


def check_outliers_with_persistence(new_data, model, persistence_thresh=1440, predictions_file='predictions_file.out',
                                    output_file_name='outlier_summary.out'):
    """
    Detect outliers and group results into date ranges with average count and duration in days.

    Parameters:
    - new_data: List of (date, count) pairs for testing.
    - model: Trained Isolation Forest model.
    - persistence_thresh: Size of the window to determine if majority are outliers.
    - predictions_file: File to save predictions and counts (for both inliers and outliers).
    - output_file_name: File to save summarized outlier information.

    Returns:
    - List of (from_date, to_date, avg_count, days_diff, status) tuples.
    """
    # Extract counts for prediction
    counts = [count for _, count in new_data]
    X_new = np.array(counts).reshape(-1, 1)

    # Predict outliers
    predictions = model.predict(X_new)  # -1 for outliers, 1 for inliers

    results = []

    with open('predictions_file.out', 'w') as pred_file:
        pred_file.write("Date, Count, Prediction\n")
        for (date, count), prediction in zip(new_data, predictions):
            status = "Outlier" if prediction == -1 else "Inlier"
            pred_file.write(f"{date}, {count}, {status}\n")

    # Open the output file for writing results
    with open(output_file_name, 'w') as output_file:
        output_file.write("From Date, To Date, Avg Count, Window Size, Status\n")

        # Sliding window logic
        for i in range(0, len(new_data) - persistence_thresh + 1):
            # Get the window predictions
            window_data = new_data[i:i + persistence_thresh]
            window_predictions = predictions[i:i + persistence_thresh]

            # Calculate the sum of predictions in the window
            sum_predictions = sum(window_predictions)

            # Determine the status (Outlier if sum_predictions >= 0, otherwise Inlier)
            status = "Outlier" if sum_predictions <= 100 else "Inlier"

            # Calculate the average count in the window
            avg_count = np.mean([count for _, count in window_data])

            # Get the from and to dates for the window
            from_date = window_data[0][0]
            to_date = window_data[-1][0]

            # Append results and write to file
            results.append((from_date, to_date, avg_count, persistence_thresh, status))
            output_file.write(f"{from_date}, {to_date}, {avg_count:.2f}, {persistence_thresh}, {status}\n")

    return results

# Detect outliers with persistence logic
outlier_results = check_outliers_with_persistence(test_data, model)

# Write results to output file
with open(output_file_name, 'w') as output_file:
    for from_date, to_date, avg_count, number_of_days, status in outlier_results:
        output_file.write(f"From Date: {from_date}, To Date: {to_date}, Avg Count: {avg_count}, Number of Days: {number_of_days}, Status: {status}\n")