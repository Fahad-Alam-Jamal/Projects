from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import classification_report, roc_auc_score
from xgboost import XGBClassifier
import pandas as pd
import logging
import joblib
import os

def train_and_validate(path):
    parent_dir = '/home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter'

    previous_model_path = f"{parent_dir}/Model/Final/xgb_fraud_detection_model.pkl"
    previous_encoder_path = f"{parent_dir}/Model/Final/onehot_encoder.pkl"

    temp_model_path = f"{parent_dir}/Model/Temp/xgb_fraud_detection_model.pkl"
    temp_encoder_path = f"{parent_dir}/Model/Temp/onehot_encoder.pkl"


    # Load dataset
    df = pd.read_parquet(path)

    # Drop non-informative ID columns
    df = df.drop(["nameOrig", "nameDest"], axis=1)

    # Split features and target
    X = df.drop("isFraud", axis=1)
    y = df["isFraud"]

    # Identify categorical columns
    categorical_features = ["type"]

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    # Load or create encoder
    if os.path.exists(previous_encoder_path):
        encoder = joblib.load(previous_encoder_path)
        logging.info("Loaded existing OneHotEncoder.")
    else:
        encoder = OneHotEncoder(handle_unknown="ignore")
        encoder.fit(X_train[categorical_features])
        joblib.dump(encoder, temp_encoder_path)
        logging.info("Fitted and saved new OneHotEncoder.")

    # Transform categorical features
    X_train_cat = encoder.transform(X_train[categorical_features]).toarray()
    X_test_cat = encoder.transform(X_test[categorical_features]).toarray()

    # Combine categorical + numeric features
    numeric_cols = [c for c in X_train.columns if c not in categorical_features]
    X_train_final = pd.concat(
        [pd.DataFrame(X_train_cat), X_train[numeric_cols].reset_index(drop=True)], axis=1
    )
    X_test_final = pd.concat(
        [pd.DataFrame(X_test_cat), X_test[numeric_cols].reset_index(drop=True)], axis=1
    )

    # Compute imbalance ratio
    scale_pos_weight = len(y_train[y_train == 0]) / len(y_train[y_train == 1])

    # Define base model
    xgb_model = XGBClassifier(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary:logistic",
        eval_metric="auc",
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        n_jobs=-1
    )

    # Load and incrementally train model if available
    if os.path.exists(previous_model_path):
        logging.info("Loading existing XGBoost model for incremental training...")
        old_model = joblib.load(previous_model_path)
        booster = old_model.get_booster()
        logging.info("Continuing training from previous booster...")
        xgb_model.fit(X_train_final, y_train, xgb_model=booster)
    else:
        logging.info("No existing model found — training new model...")
        xgb_model.fit(X_train_final, y_train)

    logging.info("Model training complete!")

    # Validation
    y_pred = xgb_model.predict(X_test_final)
    y_proba = xgb_model.predict_proba(X_test_final)[:, 1]

    metrics = {
        "classification_report": classification_report(y_test, y_pred, digits=4),
        "auc_score": roc_auc_score(y_test, y_proba)
    }

    report = f"\n📊 Classification Report\n{metrics['classification_report']}\n\nROC-AUC : {metrics['auc_score']}"

    with open(f'{parent_dir}/Model/Temp/Model_Metrics.txt', "w") as f:
        f.write(report)

    # Save updated model temporarily
    joblib.dump(xgb_model, temp_model_path)

    return metrics["auc_score"]
