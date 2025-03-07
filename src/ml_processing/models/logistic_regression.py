import pandas as pd
from sklearn.linear_model import LogisticRegression

def train_model(config, params):
    """
    1. Load the train dataset from a parquet file (path from config).
    2. Exclude columns listed in 'exclude_features', and also remove the target column from features.
    3. Train a LogisticRegression model and return it.
    """

    # 1) Load train data
    train_data_path = config["paths"]["final_train"]  # e.g. "final/train_data.parquet"
    df_train = pd.read_parquet(train_data_path)

    # 2) Build your feature set by excluding certain columns
    exclude_cols = config["models"]["logistic_regression"].get("exclude_features", [])
    target_col = config["models"]["logistic_regression"].get("target", "target_class")

    # remove them from the dict so they're not passed to LogisticRegression
    valid_model_params = {
        k: v for k, v in params.items()
        if k not in ["exclude_features", "target"]
    }

    # Convert columns to a Python list for easy manipulation
    all_cols = df_train.columns.tolist()

    # Ensure target column is not included in features
    # Combine the user-defined excluded columns + the target
    # so we skip both of them from the feature matrix
    exclude_final = set(exclude_cols + [target_col])

    # The features we keep are "all columns minus excluded"
    feature_cols = [c for c in all_cols if c not in exclude_final]

    # Grab the data subsets
    X_train = df_train[feature_cols].copy()
    y_train = df_train[target_col].copy()

    # 3) Train the model
    model = LogisticRegression(**valid_model_params)
    model.fit(X_train, y_train)

    return model
