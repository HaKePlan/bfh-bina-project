"""Open-Meteo API forecast fetching and processing."""

import requests
import pandas as pd
from datetime import datetime, timedelta

try:
    from app.prediction import convert_hourly_to_10min, classify_precip_category, predict_delay
except ImportError:
    from prediction import convert_hourly_to_10min, classify_precip_category, predict_delay

API_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
FORECAST_MODEL = "meteoswiss_icon_ch2"
FORECAST_DAYS = 7

STATIONS = {
    "Zürich HB": {"city": "Zürich", "lat": 47.3779, "lon": 8.5403},
    "Basel SBB": {"city": "Basel", "lat": 47.5476, "lon": 7.5898},
    "Bern": {"city": "Bern", "lat": 46.9481, "lon": 7.4474},
}

GERMAN_DAY_NAMES = {0: "Mo", 1: "Di", 2: "Mi", 3: "Do", 4: "Fr", 5: "Sa", 6: "So"}

# Hourly precipitation patterns per station for demo mode (mm/h, 24 values per day).
# Each station has 7 daily patterns covering all categories:
# dry (0.0), light (0<x<3), moderate (3≤x<12), heavy (≥12).
_SAMPLE_DAILY_PATTERNS = {
    "Zürich HB": [
        [0.0] * 24,                                                          # day 1: dry
        [0.0]*6 + [1.2]*4 + [0.8]*4 + [1.5]*4 + [0.0]*6,                   # day 2: light
        [0.0] * 24,                                                          # day 3: dry
        [0.0]*6 + [5.0]*4 + [7.0]*4 + [4.0]*4 + [0.0]*6,                   # day 4: moderate
        [0.0]*8 + [0.5]*8 + [0.0]*8,                                        # day 5: light
        [0.0]*4 + [3.0]*4 + [15.0]*4 + [18.0]*4 + [12.0]*4 + [0.0]*4,      # day 6: heavy
        [0.0] * 24,                                                          # day 7: dry
    ],
    "Basel SBB": [
        [0.0]*8 + [0.3]*8 + [0.0]*8,                                        # day 1: light
        [0.0] * 24,                                                          # day 2: dry
        [0.0]*4 + [4.0]*6 + [6.0]*6 + [3.5]*4 + [0.0]*4,                   # day 3: moderate
        [0.0] * 24,                                                          # day 4: dry
        [0.0]*4 + [8.0]*4 + [14.0]*4 + [16.0]*4 + [13.0]*4 + [0.0]*4,      # day 5: heavy
        [0.0]*6 + [2.0]*6 + [1.0]*6 + [0.0]*6,                             # day 6: light
        [0.0] * 24,                                                          # day 7: dry
    ],
    "Bern": [
        [0.0] * 24,                                                          # day 1: dry
        [0.0] * 24,                                                          # day 2: dry
        [0.0]*6 + [1.8]*6 + [2.5]*6 + [0.0]*6,                             # day 3: light
        [0.0]*4 + [12.0]*4 + [16.0]*4 + [14.0]*4 + [13.0]*4 + [0.0]*4,     # day 4: heavy
        [0.0]*6 + [4.5]*4 + [6.0]*4 + [5.0]*4 + [0.0]*6,                   # day 5: moderate
        [0.0] * 24,                                                          # day 6: dry
        [0.0]*8 + [0.6]*8 + [0.0]*8,                                        # day 7: light
    ],
}


def fetch_forecast(lat: float, lon: float) -> pd.DataFrame:
    """Fetch 7-day hourly precipitation forecast from Open-Meteo.

    Returns DataFrame with 'time' (datetime) and 'precipitation' (float) columns.
    Raises requests.RequestException on API failure.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation",
        "models": FORECAST_MODEL,
        "forecast_days": FORECAST_DAYS,
        "timezone": "Europe/Zurich",
    }
    response = requests.get(API_ENDPOINT, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    hourly = data["hourly"]
    df = pd.DataFrame({
        "time": pd.to_datetime(hourly["time"]),
        "precipitation": hourly["precipitation"],
    })
    # Fill null precipitation values with 0.0 (forecast gaps)
    df["precipitation"] = df["precipitation"].fillna(0.0)
    return df


def extract_precip_at_hour(forecast_df: pd.DataFrame, target_hour: int) -> pd.DataFrame:
    """Filter forecast DataFrame to one row per day at the target hour."""
    mask = forecast_df["time"].dt.hour == target_hour
    return forecast_df[mask].reset_index(drop=True)


def build_forecast_table(daily_df: pd.DataFrame, model) -> pd.DataFrame:
    """Build a display-ready forecast table from daily precipitation data.

    Args:
        daily_df: DataFrame with 'time' and 'precipitation' columns (one row per day).
        model: Trained scikit-learn model.

    Returns:
        DataFrame with columns: date, day, precip_mm_h, model_input_mm_10min,
        alert, predicted_delay.
    """
    rows = []
    for _, row in daily_df.iterrows():
        dt = row["time"]
        precip_hourly = float(row["precipitation"] or 0.0)
        precip_10min = convert_hourly_to_10min(precip_hourly)
        category, emoji = classify_precip_category(precip_10min)
        delay = predict_delay(model, precip_10min)

        rows.append({
            "date": dt.strftime("%d.%m.%Y"),
            "day": GERMAN_DAY_NAMES[dt.weekday()],
            "precip_mm_h": f"{precip_hourly:.1f} mm/h",
            "model_input_mm_10min": f"{precip_10min:.2f} mm/10min",
            "alert": emoji,
            "predicted_delay": f"+{delay:.1f} min",
        })
    return pd.DataFrame(rows)


def get_sample_forecast(station_name: str) -> pd.DataFrame:
    """Return a synthetic 7-day hourly forecast for demo mode.

    Uses hardcoded precipitation patterns that cover all categories
    (dry, light, moderate, heavy). Dates are relative to today.
    """
    patterns = _SAMPLE_DAILY_PATTERNS.get(
        station_name, _SAMPLE_DAILY_PATTERNS["Zürich HB"]
    )
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    times = [today + timedelta(hours=h) for h in range(168)]
    precip = []
    for day_pattern in patterns:
        precip.extend(day_pattern)
    return pd.DataFrame({"time": pd.to_datetime(times), "precipitation": precip})

