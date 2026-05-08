import streamlit as st
import os

st.set_page_config(
    page_title="Cloud Outage Prediction System",
    page_icon="cloud",
    layout="wide",
)

st.title("Cloud Outage Prediction System")
st.markdown(
    """
    Welcome to the **Cloud Outage Prediction** platform.

    Use the navigation sidebar to:

    - **Prediction** — Make single or batch predictions on cloud outage duration
    - **Past Predictions** — Browse historical predictions with filters by date and source

    ---

    ### About
    This system predicts the **duration (in hours)** of cloud outages based on incident attributes
    such as cloud provider, severity, service type, and operational metrics.

    > Model: Random Forest Regressor · Anomaly threshold: > 5 hours
    """
)

API_URL = os.getenv("API_URL", "http://localhost:8000")

try:
    import requests
    resp = requests.get(f"{API_URL}/health", timeout=3)
    if resp.status_code == 200 and resp.json().get("model_loaded"):
        st.success("API is online — model loaded and ready.")
    else:
        st.warning("API is reachable but model is not loaded yet.")
except Exception:
    st.error("Cannot reach the API. Make sure the model service is running.")
