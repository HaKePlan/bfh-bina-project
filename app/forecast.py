"""Open-Meteo API forecast fetching and processing."""

import requests
import pandas as pd

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
