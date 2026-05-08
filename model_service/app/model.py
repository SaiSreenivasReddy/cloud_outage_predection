import pandas as pd
import numpy as np
import joblib
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer
from sklearn.model_selection import train_test_split

MODEL_PATH = "/data/model.pkl"
MODEL_VERSION = "v1.0"

NUMERIC_FEATURES = ["system_load_before_outage", "number_of_customers_affected", "ticket_count", "backup_system_triggered_enc"]
CATEGORICAL_FEATURES = ["cloud_provider", "service", "severity"]


def encode_backup(X):
    """
    Transforms the 'backup_system_triggered' feature from 'Yes'/'No' 
    strings into binary (1/0) numerical values.
    """
    X = X.copy()
    X["backup_system_triggered_enc"] = X["backup_system_triggered"].map({"Yes": 1, "No": 0}).fillna(0)
    return X


def train_and_save(csv_path: str):
    """
    Reads the provided dataset, trains a RandomForestRegressor model
    to predict cloud outage durations (in hours), and persists the
    pipeline to disk.
    """
    # Load raw dataset and drop rows lacking critical features
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=["duration_minutes", "cloud_provider", "service", "severity",
                            "system_load_before_outage", "number_of_customers_affected",
                            "ticket_count", "backup_system_triggered"])

    # Pre-process backup feature
    df["backup_system_triggered_enc"] = df["backup_system_triggered"].map({"Yes": 1, "No": 0}).fillna(0)

    # Select features and scale target value to hours
    X = df[["cloud_provider", "service", "severity", "system_load_before_outage",
            "number_of_customers_affected", "ticket_count", "backup_system_triggered"]]
    y = df["duration_minutes"] / 60.0  # predict hours

    # Build transformation pipeline for standard scaling and one-hot encoding
    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), ["system_load_before_outage", "number_of_customers_affected", "ticket_count", "backup_system_triggered_enc"]),
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["cloud_provider", "service", "severity"]),
    ])

    encoder = FunctionTransformer(encode_backup, validate=False)

    # Construct the final ML pipeline
    pipeline = Pipeline([
        ("encode_backup", encoder),
        ("preprocessor", preprocessor),
        ("model", RandomForestRegressor(n_estimators=100, random_state=42)),
    ])

    # Split dataset and train the pipeline
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    pipeline.fit(X_train, y_train)

    # Save the serialized model to disk
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    print(f"Model trained and saved to {MODEL_PATH}")
    return pipeline


def load_model():
    """
    Attempts to load a previously trained ML model from the data volume.
    Returns (model, version) or (None, None) if not found.
    """
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH), MODEL_VERSION
    return None, None


def predict(model, features: dict) -> float:
    """
    Transforms dictionary features into a DataFrame and returns the 
    single prediction produced by the trained Random Forest model.
    """
    df = pd.DataFrame([features])
    return float(model.predict(df)[0])
