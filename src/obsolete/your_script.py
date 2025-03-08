import numpy as np
import sys
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, train_test_split
from joblib import parallel_backend, PrintTime

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Load dataset
iris = load_iris()
X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target, test_size=0.2, random_state=42)

# Define model and hyperparameter grid
param_grid = {
    "n_estimators": [10, 50, 100],
    "max_depth": [None, 5, 10]
}

model = RandomForestClassifier(random_state=42)

# Use multiprocessing backend but force workers to flush output
with parallel_backend("loky"):
    grid_search = GridSearchCV(model, param_grid, cv=3, verbose=2, n_jobs=-1)
    grid_search.fit(X_train, y_train)

# Ensure final message prints in the correct order
print("GridSearchCV completed successfully.", flush=True)
