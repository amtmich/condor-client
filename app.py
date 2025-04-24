import streamlit as st
import os
from dotenv import load_dotenv
from datetime import datetime, date
import pandas as pd
from flight_search import FlightSearch

# Load environment variables from .env
load_dotenv()

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "condor_data")

# Initialize FlightSearch
flight_search = FlightSearch(
    host=ELASTICSEARCH_HOST,
    user=ELASTICSEARCH_USER,
    password=ELASTICSEARCH_PASSWORD,
    index=ELASTICSEARCH_INDEX
)

def main():
    st.title("Condor Flight Search")

    # --- SIDEBAR INPUTS ---
    # 1. Required: Destination code(s)
    destination = st.sidebar.text_input("Destination(s) (comma-separated, required)")

    # 2. Optional: comma-separated list of origin codes to exclude
    exclude_origins_str = st.sidebar.text_input("Exclude origin codes (comma-separated, optional)", "")
    origin_exclusions = []
    if exclude_origins_str.strip():
        origin_exclusions = [o.strip().upper() for o in exclude_origins_str.split(",") if o.strip()]

    # 3. Optional: comma-separated list of origin codes to include
    include_origins_str = st.sidebar.text_input("Include origin codes (comma-separated, optional)", "")
    origin_codes = []
    if include_origins_str.strip():
        origin_codes = [o.strip().upper() for o in include_origins_str.split(",") if o.strip()]

    # 4. Required: type (roundtrip/oneway), default oneway
    flight_type = st.sidebar.selectbox("Type", ["oneway", "roundtrip"], index=0)

    # 5. Optional: min and max date
    min_date_val = st.sidebar.date_input("Minimum Date (optional)", value=None)
    max_date_val = st.sidebar.date_input("Maximum Date (optional)", value=None)

    # 6. date_retrieved (default today)
    default_date_retrieved = datetime.today().strftime("%Y%m%d")
    date_retrieved_val = st.sidebar.text_input("Date retrieved (default today)", value=default_date_retrieved)

    # 7. Optional: concrete date
    concrete_date_str = st.sidebar.text_input("Concrete date YYYYMMDD (optional)", "")
    concrete_date_val = concrete_date_str.strip() if concrete_date_str.strip() else None

    # Validate required fields
    if not destination.strip():
        st.warning("Please provide a destination code.")
        st.stop()

    # --- BUILD QUERY AND GET RESULTS ---
    base_query = flight_search.build_base_query(
        destination=destination.upper(),
        origin_exclusions=origin_exclusions,
        origin_codes=origin_codes,
        flight_type=flight_type,
        min_date_val=min_date_val if isinstance(min_date_val, date) else None,
        max_date_val=max_date_val if isinstance(max_date_val, date) else None,
        date_retrieved_val=date_retrieved_val,
        concrete_date_val=concrete_date_val
    )

    # --- DISPLAY RESULTS ---
    if flight_type == "oneway":
        st.subheader("Oneway Results (100 cheapest)")
        results = flight_search.get_oneway_results(base_query)
        df = pd.DataFrame(results)
        st.dataframe(df)
    else:
        st.subheader("Roundtrip Results (100 cheapest pairs)")
        results = flight_search.get_roundtrip_results(base_query)
        df = pd.DataFrame(results)
        st.dataframe(df)

if __name__ == "__main__":
    main()
