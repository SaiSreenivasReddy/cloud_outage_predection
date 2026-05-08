import streamlit as st
import requests
import pandas as pd
import os
from datetime import date, datetime

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Past Predictions", layout="wide")

col1, col2 = st.columns([0.9, 0.1])
with col1:
    st.title("Past Predictions")
with col2:
    if st.button("Predictions", use_container_width=True):
        st.switch_page("1_Prediction.py")

col1, col2, col3 = st.columns(3)
with col1:
    start_date = st.date_input("Start Date", value=date(2024, 1, 1))
with col2:
    end_date = st.date_input("End Date", value=date.today())
with col3:
    source = st.selectbox("Prediction Source", ["all", "webapp", "scheduled"])

limit = st.slider("Max results", 10, 500, 100)

if st.button("Fetch Predictions"):
    # Construct query parameters based on user selections
    params = {
        "source": source,
        # Convert date objects to ISO 8601 strings for the API
        "start_date": datetime.combine(start_date, datetime.min.time()).isoformat(),
        "end_date": datetime.combine(end_date, datetime.max.time()).isoformat(),
        "limit": limit,
    }
    
    try:
        # Call the GET endpoint to retrieve historical data
        resp = requests.get(f"{API_URL}/past-predictions", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            st.info("No predictions found for the selected filters.")
        else:
            df = pd.DataFrame(data)
            df["is_anomaly"] = df["is_anomaly"].map({True: "Yes", False: "No"})
            df["predicted_hours"] = df["predicted_hours"].round(2)
            st.success(f"Found {len(df)} predictions.")
            st.dataframe(df, use_container_width=True)

            st.subheader("Summary")
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Predictions", len(df))
            m2.metric("Avg Duration (hrs)", round(df["predicted_hours"].mean(), 2))
            m3.metric("Anomalies", df["is_anomaly"].value_counts().get("Yes", 0))

    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to API. Is it running?")
    except Exception as e:
        st.error(f"Error: {e}")
