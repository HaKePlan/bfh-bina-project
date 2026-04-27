"""Precipitation conversion, category classification, and delay prediction."""

from pathlib import Path

import joblib
import numpy as np


def convert_hourly_to_10min(mm_per_hour: float) -> float:
    """Convert hourly precipitation (mm/h) to approximate 10-minute equivalent."""
    return mm_per_hour / 6.0


def classify_precip_category(precip_10min: float) -> tuple[str, str]:
    """Classify precipitation into category and alert emoji.

    Thresholds match db/init.sql exactly:
    - dry:      = 0.0 mm
    - light:    > 0.0 and < 0.5 mm
    - moderate: >= 0.5 and < 2.0 mm
    - heavy:    >= 2.0 mm
    """
    if precip_10min == 0.0:
        return "dry", "🟢"
    if precip_10min < 0.5:
        return "light", "🟡"
    if precip_10min < 2.0:
        return "moderate", "🟠"
    return "heavy", "🔴"


def predict_delay(model, precip_10min: float) -> float:
    """Predict arrival delay in minutes for a given 10-min precipitation value."""
    features = np.array([[precip_10min]])
    prediction = model.predict(features)
    return float(prediction[0])


def load_model(model_path: Path):
    """Load a trained scikit-learn model from disk.

    Raises FileNotFoundError if the model file does not exist.
    """
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found at {model_path}")
    return joblib.load(model_path)
