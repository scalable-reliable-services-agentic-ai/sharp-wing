import os
import joblib
import numpy as np
from src.config import configure_logging

logger = configure_logging(__name__)

# Force absolute path targeting the Docker shared volume mount point
OUTPUT_DIR = "/app/src/ml_service/models"
CHALLENGER_PATH = os.path.join(OUTPUT_DIR, "fraud_model.pkl")
CHAMPION_PATH = os.path.join(OUTPUT_DIR, "fraud_model_champion.pkl")


class FraudMLInference:
    def __init__(self, shadow_mode: bool = True):
        self.shadow_mode = shadow_mode
        self.challenger = None
        self.champion = None
        self._load_models_from_disk()

    def _load_models_from_disk(self):
        """Attempts to load model artifacts from the shared storage volume"""
        # Load the hot-swappable Challenger Model
        if not self.challenger and os.path.exists(CHALLENGER_PATH):
            try:
                self.challenger = joblib.load(CHALLENGER_PATH)
                logger.info("Challenger Model (Active Worker Feedback) loaded successfully from shared volume.")
            except Exception as e:
                logger.error(f"Failed to load challenger model matrix: {e}")

        # Load the stable Champion Control Model
        if not self.champion and os.path.exists(CHAMPION_PATH):
            try:
                self.champion = joblib.load(CHAMPION_PATH)
                logger.info("Champion Control Model loaded successfully from shared volume.")
            except Exception as e:
                logger.error(f"Failed to load champion model matrix: {e}")
        else:
            # Fallback if we don't have a baseline champion yet
            self.champion = self.challenger

    def evaluate_transaction_risk(self, amount: float, location_str: str) -> dict:
        """
        Executes parallel predictions (Shadow Inferences)
        Returns both scores so the main processor can selectively route based on safety settings
        """
        # Safety check: If a model wasn't loaded at boot (e.g. during a fresh volume wipe),
        # try reloading it live now that the bootstrap worker has populated the directory.
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
