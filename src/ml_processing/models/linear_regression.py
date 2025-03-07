import pandas as pd
from sklearn.linear_model import LinearRegression

def train_model(config, params):
    """
    1. Load the train dataset from a parquet file (path from config).
    2. Perform any feature selection specifically for linear regression.
    3. Train a LinearRegression model using `params`.
    4. Return the fitted model.
    """
    # 1) Load the train data from your final folder
    train_data_path = config["paths"]["final_train"]  # e.g. "final/train_data.parquet"
    df_train = pd.read_parquet(train_data_path)

    # 2) Suppose for regression we want these columns:
    # Let's say config["models"]["linear_regression"]["features"] = ["feat1", "feat2", "feat3"]
    feature_cols = config["models"]["linear_regression"].get("features", [])
    target_col = config["models"]["linear_regression"].get("target", "target_reg")

    X_train = df_train[feature_cols].copy()
    y_train = df_train[target_col].copy()

    # 3) Create & fit the scikit-learn model
    model = LinearRegression(**params)
    model.fit(X_train, y_train)

    return model
