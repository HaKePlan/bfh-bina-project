"""SBB Delay Prediction — Streamlit application entry point."""

import sys
from datetime import time
from pathlib import Path

# Ensure the app/ directory is on the path so sibling modules can be imported
# regardless of how Streamlit is invoked.
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st

try:
    from app.forecast import STATIONS, fetch_forecast, extract_precip_at_hour, build_forecast_table, get_sample_forecast
    from app.prediction import (
        load_model,
        convert_hourly_to_10min,
        classify_precip_category,
        predict_delay,
    )
except ImportError:
    from forecast import STATIONS, fetch_forecast, extract_precip_at_hour, build_forecast_table, get_sample_forecast
    from prediction import (
        load_model,
        convert_hourly_to_10min,
        classify_precip_category,
        predict_delay,
    )

MODEL_PATH = Path(__file__).parent.parent / "models" / "model_all_data.pkl"

DISCLAIMER = """\
⚠️ This prediction is based on a single-predictor model (R² ≈ 0.000).
Precipitation explains less than 0.25% of delay variance.
This tool indicates a general trend only — not a precise per-trip forecast.
Hourly forecast precipitation is divided by 6 as an approximation of the
10-minute input the model expects.
Data source: MeteoSwiss ICON CH2 via Open-Meteo (open-meteo.com).
Model trained on SBB Istdaten 2022–2025.
"""


@st.cache_resource
def _load_model():
    return load_model(MODEL_PATH)


def main():
    st.set_page_config(page_title="SBB Delay Prediction", page_icon="🚂", layout="wide")
    st.title("🚂 SBB Delay Prediction")
    st.caption("Expected arrival delay based on precipitation forecast")

    # --- Model loading ---
    try:
        model = _load_model()
    except FileNotFoundError:
        st.error(
            "Model file not found at models/model_all_data.pkl.\n\n"
            "Run the analysis notebook (Section 4) first to train and save the model."
        )
        return

    # --- Sidebar ---
    st.sidebar.header("Settings")
    station = st.sidebar.selectbox("Station", list(STATIONS.keys()))
    arrival_time = st.sidebar.time_input("Planned arrival time", value=time(8, 0))
    demo_mode = st.sidebar.checkbox("Demo mode", value=False)

    station_info = STATIONS[station]

    # --- Section 1: 7-Day Forecast ---
    st.header(f"📅 7-Day Forecast — {station}")
    if demo_mode:
        st.info("🎭 Demo mode — using sample data with varied weather conditions.")

    try:
        if demo_mode:
            forecast_df = get_sample_forecast(station)
        else:
            forecast_df = _fetch_cached_forecast(station_info["lat"], station_info["lon"])
        daily_df = extract_precip_at_hour(forecast_df, target_hour=arrival_time.hour)

        if daily_df.empty:
            st.warning("No forecast data available for the selected time.")
        else:
            table = build_forecast_table(daily_df, model)
            _display_forecast_table(table)
    except Exception as e:
        st.error(f"Failed to fetch forecast: {e}")

    # --- Section 2: Manual Input ---
    st.header(f"🔬 Manual Precipitation Input — {station}")
    st.caption("0 mm/h = dry  |  < 3 mm/h = light  |  3–12 mm/h = moderate  |  > 12 mm/h = heavy")

    precip_hourly = st.slider(
        "Precipitation intensity (mm/h)",
        min_value=0.0, max_value=20.0, value=0.0, step=0.1,
    )

    precip_10min = convert_hourly_to_10min(precip_hourly)
    category, emoji = classify_precip_category(precip_10min)
    delay = predict_delay(model, precip_10min)

    col1, col2, col3 = st.columns(3)
    col1.metric("Category", f"{emoji} {category}")
    col2.metric("Model input", f"{precip_10min:.2f} mm/10min")
    col3.metric("Predicted delay", f"+{delay:.1f} min")

    # --- Footer ---
    st.divider()
    st.caption(DISCLAIMER)


@st.cache_data(ttl=3600)
def _fetch_cached_forecast(lat: float, lon: float):
    return fetch_forecast(lat, lon)


def _display_forecast_table(table):
    """Render the forecast table."""
    st.dataframe(
        table,
        column_config={
            "alert": st.column_config.TextColumn("Alert", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "day": st.column_config.TextColumn("Day", width="small"),
            "precip_mm_h": st.column_config.TextColumn("Precip (mm/h)"),
            "model_input_mm_10min": st.column_config.TextColumn("Model input (mm/10min)"),
            "predicted_delay": st.column_config.TextColumn("Predicted delay"),
        },
        column_order=["date", "day", "precip_mm_h", "model_input_mm_10min", "alert", "predicted_delay"],
        hide_index=True,
        use_container_width=True,
    )


if __name__ == "__main__":
    main()

