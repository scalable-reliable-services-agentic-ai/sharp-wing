import os
import joblib
import numpy as np
from src.config import configure_logging

logger = configure_logging(__name__)

BASE_DIR = os.path.dirname(__file__)
CHALLENGER_PATH = os.path.join(BASE_DIR, "models", "fraud_model.pkl")
CHAMPION_PATH = os.path.join(BASE_DIR, "models", "fraud_model_champion.pkl")


class FraudMLInference:
    def __init__(self, shadow_mode: bool = True):
        self.shadow_mode = shadow_mode

        # Load the hot-swappable Challenger Model
        if os.path.exists(CHALLENGER_PATH):
            self.challenger = joblib.load(CHALLENGER_PATH)
            logger.info("Challenger Model (Active Worker Feedback) loaded successfully.")
        else:
            self.challenger = None

        # Load the stable Champion Control Model
        if os.path.exists(CHAMPION_PATH):
            self.champion = joblib.load(CHAMPION_PATH)
            logger.info("Champion Control Model loaded successfully.")
        else:
            # Fallback if we don't have a baseline champion yet
            self.champion = self.challenger
            logger.warning("Champion model artifact not found. Using Challenger as production control.")

    def evaluate_transaction_risk(self, amount: float, location_str: str) -> dict:
        """
        Executes parallel predictions (Shadow Inferences).
        Returns both scores so the main processor can selectively route based on safety settings.
        """
        try:
            lat, lon = map(float, location_str.split(','))
        except (ValueError, AttributeError):
            lat, lon = 0.0, 0.0

        features = np.array([[amount, lat, lon]])

        # Calculate scores
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
