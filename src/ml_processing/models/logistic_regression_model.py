# ─── Load Libraries ──────────────────────────────────────────────────────────
from sklearn.linear_model import LogisticRegression

# ─── Train Function ──────────────────────────────────────────────────────────
def train_model(X_train, y_train, params):
    """
    Train a Logistic Regression model with the given hyperparameters.
    
    Args:
        X_train (array-like): Training features.
        y_train (array-like): Training labels.
        params (dict): Model hyperparameters from config (e.g. {"fit_intercept": True}).
    
    Returns:
        model: Trained LinearRegression object.
    """
    # Create and train
    model = LogisticRegression(**params)
    model.fit(X_train, y_train)
    return model
