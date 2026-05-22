import asyncpg
import asyncio
import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib
import os
from src.config import settings

# Absolute path mapped directly to the Docker shared volume mount point
OUTPUT_DIR = "/app/src/ml_service/models"
os.makedirs(OUTPUT_DIR, exist_ok=True)
model_save_path = os.path.join(OUTPUT_DIR, "fraud_model.pkl")


async def fetch_combined_training_data():
    conn = await asyncpg.connect(
        host=getattr(settings, "postgres_host", "timescaledb"),
        port=getattr(settings, "postgres_port", 5432),
        user=getattr(settings, "postgres_user", "postgres"),
        password=getattr(settings, "postgres_password", "password"),
        database=getattr(settings, "postgres_db", "fraud_detection_db"),
    )

    # Select rows, but explicitly LIMIT how much baseline data we consume to prevent total memorization
    query = """
            SELECT amount, location, is_fraud
            FROM transactions
            WHERE current_state NOT IN ('RECEIVED', 'ESCALATED')
            ORDER BY RANDOM() LIMIT 3000;
            """
    try:
        rows = await conn.fetch(query)
    finally:
        await conn.close()
    return rows


def preprocess_and_train(rows):
    if not rows or len(rows) < 100:
        print("Not enough rows to split and train.")
        return

    df = pd.DataFrame([dict(r) for r in rows])
    df['label'] = df['is_fraud'].apply(lambda x: 1 if x is True else 0)

    df['lat'] = df['location'].apply(lambda x: float(str(x).split(',')[0]) if (x and ',' in str(x)) else 0.0)
    df['lon'] = df['location'].apply(lambda x: float(str(x).split(',')[1]) if (x and ',' in str(x)) else 0.0)

    X = df[['amount', 'lat', 'lon']]
    y = df['label']

    # STEP 1: SPLIT INTO TRAIN AND A TEMPORARY HOLDER (30%)
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )

    # STEP 2: SPLIT THE TEMPORARY HOLDER EQUALLY INTO VALIDATION AND TEST (15% / 15%)
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=0.5, random_state=42, stratify=y_temp
    )

    print(f"Data Split: Train={len(X_train)} | Val={len(X_val)} | Unseen Test Set={len(X_test)}")

    # Keep parameters highly constrained to prevent overfitting on simulation data
    model = XGBClassifier(
        n_estimators=100,
        max_depth=3,  # Shallow trees prevent memorization of exact coordinate blocks
        learning_rate=0.05,
        scale_pos_weight=4,
        random_state=42,
        early_stopping_rounds=5  # Stops automatically if val loss stops improving
    )

    # Train using the validation set to monitor overfitting live
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False
    )

    # Evaluate exclusively on the hidden, unseen test set
    preds = model.predict(X_test)
    print("\nPerformance Evaluation on 100% Unseen Test Set:")
    print(classification_report(y_test, preds, target_names=['Safe', 'Fraud']))

    os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
    joblib.dump(model, model_save_path)
    print(f"Generalized model saved to {model_save_path}")


if __name__ == "__main__":
    raw_rows = asyncio.run(fetch_combined_training_data())
    preprocess_and_train(raw_rows)
