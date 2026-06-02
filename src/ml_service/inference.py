import os
import joblib
import numpy as np
from src.config import configure_logging

logger = configure_logging(__name__)

OUTPUT_DIR = "/app/src/ml_service/models"
CHAMPION_PATH = os.path.join(OUTPUT_DIR, "fraud_model.pkl")  # Baseline/Initial train output
CHALLENGER_PATH = os.path.join(OUTPUT_DIR, "challenger_model.pkl")  # Retrain worker 10-batch output


class FraudMLInference:
    def __init__(self, shadow_mode: bool = True):
        self.shadow_mode = shadow_mode
        self.challenger = None
        self.champion = None
        self._load_models_from_disk()

    def _load_models_from_disk(self):
        """Attempts to load model artifacts from the shared storage volume"""
        # 1. Load the stable Champion Control Model
        if os.path.exists(CHAMPION_PATH):
            try:
                self.champion = joblib.load(CHAMPION_PATH)
                logger.info(f"Champion Control Model loaded successfully from: {CHAMPION_PATH}")
            except Exception as e:
                logger.error(f"Failed to load champion model matrix: {e}")

        # 2. Load the hot-swappable Challenger Model
        if os.path.exists(CHALLENGER_PATH):
            try:
                self.challenger = joblib.load(CHALLENGER_PATH)
                logger.info(f"Challenger Model (Active Worker Feedback) loaded successfully from: {CHALLENGER_PATH}")
            except Exception as e:
                logger.error(f"Failed to load challenger model matrix: {e}")

        # 3. Dynamic Fallback Safety
        if self.champion and not self.challenger:
            # If system just booted up and no challenger is trained yet, mirror champion
            self.challenger = self.champion
        elif self.challenger and not self.champion:
            self.champion = self.challenger

    def reload_models(self):
        """
        Public interface called by the anomaly_detection file watcher
        Bypasses guards to explicitly hot-swap memory pointers with fresh disk states
        """
        logger.warning("Executing hot-swap reload of Champion and Challenger model pointers from disk...")
        self._load_models_from_disk()

    def evaluate_transaction_risk(self, amount: float, location_str: str) -> dict:
        """
        Executes parallel predictions (Shadow Inferences)
        Returns both scores so the main processor can selectively route based on safety settings
        """
        # Safety dynamic reload check in case of clean volume wipes at boot
        if not self.challenger or not self.champion:
            self._load_models_from_disk()

        try:
            lat, lon = map(float, location_str.split(','))
        except (ValueError, AttributeError):
            lat, lon = 0.0, 0.0

        features = np.array([[amount, lat, lon]])

        # Calculate scores dynamically using the loaded model matrices
        challenger_score = float(self.challenger.predict_proba(features)[0][1]) if self.challenger else 0.25
        champion_score = float(self.champion.predict_proba(features)[0][1]) if self.champion else 0.25

        # Determine the definitive routing decision score based on our deployment strategy
        routing_score = champion_score if self.shadow_mode else challenger_score

        return {
            "routing_score": routing_score,
            "champion_score": champion_score,
            "challenger_score": challenger_score,
            "shadow_active": self.shadow_mode
        }
