"""Tests for app/prediction.py — precipitation conversion and delay prediction."""

from unittest.mock import MagicMock
from pathlib import Path

import numpy as np
import pytest

from app.prediction import convert_hourly_to_10min, classify_precip_category, predict_delay, load_model


class TestConvertHourlyTo10Min:
    """Verify hourly precipitation is divided by 6 to approximate 10-min equivalent."""

    def test_zero_precipitation(self):
        assert convert_hourly_to_10min(0.0) == 0.0

    def test_known_value(self):
        result = convert_hourly_to_10min(6.0)
        assert result == 1.0

    def test_fractional_value(self):
        result = convert_hourly_to_10min(1.2)
        assert abs(result - 0.2) < 1e-9


class TestClassifyPrecipCategory:
    """Verify precipitation category thresholds match db/init.sql exactly."""

    def test_dry(self):
        category, emoji = classify_precip_category(0.0)
        assert category == "dry"
        assert emoji == "🟢"

    def test_light_just_above_zero(self):
        category, emoji = classify_precip_category(0.001)
        assert category == "light"
        assert emoji == "🟡"

    def test_light_boundary_below_moderate(self):
        category, emoji = classify_precip_category(0.499)
        assert category == "light"
        assert emoji == "🟡"

    def test_moderate_at_boundary(self):
        category, emoji = classify_precip_category(0.5)
        assert category == "moderate"
        assert emoji == "🟠"

    def test_moderate_below_heavy(self):
        category, emoji = classify_precip_category(1.999)
        assert category == "moderate"
        assert emoji == "🟠"

    def test_heavy_at_boundary(self):
        category, emoji = classify_precip_category(2.0)
        assert category == "heavy"
        assert emoji == "🔴"

    def test_heavy_large_value(self):
        category, emoji = classify_precip_category(10.0)
        assert category == "heavy"
        assert emoji == "🔴"


class TestPredictDelay:
    """Verify predict_delay wraps model.predict correctly."""

    def test_returns_float(self):
        model = MagicMock()
        model.predict.return_value = np.array([1.5])
        result = predict_delay(model, 0.2)
        assert isinstance(result, float)
        assert result == 1.5

    def test_passes_correct_shape(self):
        model = MagicMock()
        model.predict.return_value = np.array([0.8])
        predict_delay(model, 0.33)
        args = model.predict.call_args[0][0]
        assert args.shape == (1, 1)
        assert abs(args[0][0] - 0.33) < 1e-9


class TestLoadModel:
    """Verify model loading behavior."""

    def test_raises_when_model_missing(self, tmp_path):
        missing_path = tmp_path / "nonexistent.pkl"
        with pytest.raises(FileNotFoundError):
            load_model(missing_path)

