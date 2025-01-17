import streamlit as st
import os
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
from elasticsearch import Elasticsearch

# Load environment variables from .env file
load_dotenv()

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "condor_data")  # default name if needed

# Create Elasticsearch client
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    # If using HTTPS, you may need additional SSL configuration
    # e.g., verify_certs=False, ssl_show_warn=False
)

###############################################################################
# Streamlit App
###############################################################################
st.title("Flight Search")

# 1) Destination code (required)
destination_code = st.text_input("Destination code (required)", value="", help="Example: PUJ").strip()

# 2) Comma-separated list of origin codes to exclude (optional)
excluded_origins_input = st.text_input("Excluded origin codes (comma separated, optional)", value="").strip()

# 3) Type (roundtrip/oneway) (optional)
flight_type = st.selectbox("Flight type (optional)", ["", "oneway", "roundtrip"])

# 4) Minimum and maximum date (optional)
col1, col2 = st.columns(2)
with col1:
    min_date = st.date_input("Minimum date (optional)", value=None)
with col2:
    max_date = st.date_input("Maximum date (optional)", value=None)

# 5) date_retrieved (default today)
date_retrieved_input = st.date_input("Date retrieved", value=date.today())

# 6) Concrete date (optional)
concrete_date_input = st.date_input("Concrete date (optional)", value=None)

# Button to trigger search
if st.button("Search"):
    if not destination_code:
        st.error("Destination code is required.")
        st.stop()

    ###############################################################################
    # Build Elasticsearch query
    ###############################################################################
    filters = []

    # Must: Destination code
    filters.append({"term": {"destination": destination_code}})

    # Excluded origins (if provided)
    if excluded_origins_input:
        excluded_origins = [o.strip() for o in excluded_origins_input.split(",") if o.strip()]
        if excluded_origins:
            # Use must_not to exclude these origins
            filters.append({
                "bool": {
                    "must_not": [
                        {"terms": {"origin": excluded_origins}}
                    ]
                }
            })

    # Flight type (if provided)
    if flight_type:
        filters.append({"term": {"type": flight_type}})

    # Min date (if provided)
    if min_date:
        filters.append({"range": {"date": {"gte": min_date.strftime("%Y%m%d")}}})

    # Max date (if provided)
    if max_date:
        filters.append({"range": {"date": {"lte": max_date.strftime("%Y%m%d")}}})

    # date_retrieved (default today)
    if date_retrieved_input:
        filters.append({"term": {"date_retrieved": date_retrieved_input.strftime("%Y%m%d")}})

    # Concrete date (if provided)
    if concrete_date_input:
        filters.append({"term": {"date": concrete_date_input.strftime("%Y%m%d")}})

    # Combine filters into a bool query
    query = {
        "bool": {
            "must": filters
        }
    }

    # Search body
    search_body = {
        "size": 100,
        "query": query,
        "sort": [{"price": {"order": "asc"}}]
    }

    ###############################################################################
    # Execute query
    ###############################################################################
    try:
        response = es.search(index=ELASTICSEARCH_INDEX, body=search_body)
    except Exception as e:
        st.error(f"Error while searching Elasticsearch: {str(e)}")
        st.stop()

    if "hits" not in response or "hits" not in response["hits"]:
        st.write("No records found.")
        st.stop()

    hits = response["hits"]["hits"]

    ###############################################################################
    # Process and display results
    # - Convert price to price/100
    # - Compute price difference vs previous day (based on date_retrieved)
    ###############################################################################
    records = []
    for hit in hits:
        doc = hit["_source"]
        record = {
            "origin": doc.get("origin", ""),
            "destination": doc.get("destination", ""),
            "date": doc.get("date", ""),
            "date_retrieved": doc.get("date_retrieved", ""),
            "type": doc.get("type", ""),
            # real price is price/100
            "price": doc.get("price", 0) / 100.0,
            "price_change_vs_previous_day": None
        }

        # Compute price change vs. previous day
        current_day_str = doc.get("date_retrieved", "")
        if current_day_str:
            try:
                current_day_dt = datetime.strptime(current_day_str, "%Y%m%d")
                previous_day_str = (current_day_dt - timedelta(days=1)).strftime("%Y%m%d")

                # Look up the record with the same origin/destination/date/type but date_retrieved = previous_day
                sub_query = {
                    "bool": {
                        "must": [
                            {"term": {"origin": record["origin"]}},
                            {"term": {"destination": record["destination"]}},
                            {"term": {"date": record["date"]}},
                            {"term": {"type": record["type"]}},
                            {"term": {"date_retrieved": previous_day_str}}
                        ]
                    }
                }

                sub_search_body = {
                    "size": 1,
                    "query": sub_query
                }

                try:
                    sub_response = es.search(index=ELASTICSEARCH_INDEX, body=sub_search_body)
                    if sub_response["hits"]["total"]["value"] > 0:
                        prev_price_raw = sub_response["hits"]["hits"][0]["_source"]["price"]
                        prev_price = prev_price_raw / 100.0
                        record["price_change_vs_previous_day"] = record["price"] - prev_price
                except Exception as sub_e:
                    # If there's an error, just ignore the price difference
                    pass

            except ValueError:
                # If parsing current_day_str as a date fails, ignore price difference
                pass

        records.append(record)

    if not records:
        st.write("No records found.")
    else:
        st.write(f"Found {len(records)} records (showing up to 100).")
        # Display as table
        st.table(records)
