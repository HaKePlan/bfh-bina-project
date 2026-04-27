"""Tests for app/forecast.py — Open-Meteo API and forecast table building."""

from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd

from app.forecast import fetch_forecast, extract_precip_at_hour, build_forecast_table


class TestFetchForecast:
    """Verify Open-Meteo API response is parsed into a DataFrame."""

    def test_returns_dataframe_with_expected_columns(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hourly": {
                "time": [
                    "2026-04-27T00:00",
                    "2026-04-27T01:00",
                    "2026-04-27T02:00",
                ],
                "precipitation": [0.0, 0.5, 1.2],
            }
        }
        mock_response.raise_for_status = MagicMock()

        with patch("app.forecast.requests.get", return_value=mock_response):
            df = fetch_forecast(47.3779, 8.5403)

        assert isinstance(df, pd.DataFrame)
        assert "time" in df.columns
        assert "precipitation" in df.columns
        assert len(df) == 3


class TestExtractPrecipAtHour:
    """Verify filtering to one row per day at target hour."""

    def _make_7day_df(self):
        """Create synthetic 168-row DataFrame (7 days × 24 hours)."""
        times = pd.date_range("2026-04-27", periods=168, freq="h")
        precip = [float(i % 24) / 10.0 for i in range(168)]
        return pd.DataFrame({"time": times, "precipitation": precip})

    def test_returns_7_rows(self):
        df = self._make_7day_df()
        result = extract_precip_at_hour(df, target_hour=8)
        assert len(result) == 7

    def test_all_rows_match_target_hour(self):
        df = self._make_7day_df()
        result = extract_precip_at_hour(df, target_hour=14)
        assert all(t.hour == 14 for t in result["time"])

    def test_each_day_represented_once(self):
        df = self._make_7day_df()
        result = extract_precip_at_hour(df, target_hour=8)
        dates = result["time"].dt.date.tolist()
        assert len(set(dates)) == 7


class TestBuildForecastTable:
    """Verify build_forecast_table produces correct output structure."""

    def test_output_has_expected_columns(self):
        times = pd.date_range("2026-04-27 08:00", periods=7, freq="D")
        daily_df = pd.DataFrame(
            {
                "time": times,
                "precipitation": [0.0, 1.2, 3.0, 0.6, 0.0, 8.0, 0.1],
            }
        )
        model = MagicMock()
        model.predict.return_value = np.array([1.0])

        result = build_forecast_table(daily_df, model)

        expected_cols = {
            "date",
            "day",
            "precip_mm_h",
            "model_input_mm_10min",
            "alert",
            "predicted_delay",
        }
        assert set(result.columns) == expected_cols
        assert len(result) == 7

    def test_german_day_names(self):
        # Monday 2026-04-27
        times = pd.date_range("2026-04-27 08:00", periods=7, freq="D")
        daily_df = pd.DataFrame(
            {
                "time": times,
                "precipitation": [0.0] * 7,
            }
        )
        model = MagicMock()
        model.predict.return_value = np.array([0.5])

        result = build_forecast_table(daily_df, model)
        assert result["day"].iloc[0] == "Mo"

    def test_delay_format(self):
        times = pd.date_range("2026-04-27 08:00", periods=7, freq="D")
        daily_df = pd.DataFrame(
            {
                "time": times,
                "precipitation": [6.0] * 7,
            }
        )
        model = MagicMock()
        model.predict.return_value = np.array([1.5])

        result = build_forecast_table(daily_df, model)
        assert result["predicted_delay"].iloc[0].startswith("+")

