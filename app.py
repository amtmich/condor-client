import streamlit as st
import os
from dotenv import load_dotenv
from datetime import datetime, date
from elasticsearch import Elasticsearch
import pandas as pd

# Load environment variables from .env
load_dotenv()

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "condor_data")

# Initialize Elasticsearch client
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    # If you need SSL verification, set verify_certs=True, etc.
    verify_certs=False
)

def build_base_query(
    destination: str,
    origin_exclusions: list,
    origin_codes: list,
    flight_type: str,
    min_date_val,
    max_date_val,
    date_retrieved_val,
    concrete_date_val
):
    """
    Builds the "base" part of the Elasticsearch query (a bool query).
    """
    must_clauses = []
    must_not_clauses = []

    # 1) Required: Destination
    must_clauses.append({"term": {"destination": destination}})

    # 2) Optional: list of origin codes to exclude
    if origin_exclusions:
        must_not_clauses.append({"terms": {"origin": origin_exclusions}})

    # 2.1) Optional: list of origin codes to include
    if origin_codes:
        must_clauses.append({"terms": {"origin": origin_codes}})

    # 3) Required flight type (oneway/roundtrip)
    must_clauses.append({"term": {"type": flight_type}})

    # 4) date_retrieved (default today) - treat as exact match
    #    The user will enter e.g. '20250117' format.
    if date_retrieved_val:
        must_clauses.append({
            "term": {"date_retrieved": date_retrieved_val}
        })

    # 5) If a concrete date is provided, we'll ignore min/max date
    if concrete_date_val:
        must_clauses.append({"term": {"date": concrete_date_val}})
    else:
        # Use a range if min_date_val or max_date_val is provided
        date_range = {}
        if min_date_val:
            date_range["gte"] = min_date_val.strftime("%Y%m%d")
        if max_date_val:
            date_range["lte"] = max_date_val.strftime("%Y%m%d")

        if date_range:  # only add if not empty
            must_clauses.append({"range": {"date": date_range}})

    # Build the final query
    base_query = {
        "bool": {
            "must": must_clauses,
            "must_not": must_not_clauses
        }
    }

    return base_query

def search_elasticsearch(query_body, size=1000):
    """
    Executes a search against Elasticsearch with the provided query body.
    """
    response = es.search(
        index=ELASTICSEARCH_INDEX,
        body={
            "query": query_body,
            # Sort by price ascending for easy retrieval of cheapest
            "sort": [
                {"price": {"order": "asc"}}
            ],
            "size": size
        }
    )

    # Extract hits
    hits = response.get("hits", {}).get("hits", [])
    # Return _source merged with doc id for convenience
    data = [dict(hit["_source"], _id=hit["_id"]) for hit in hits]
    return data

def get_previous_day_price(
    origin: str,
    destination: str,
    flight_date: str,
    flight_type: str,
    prev_date_retrieved: str
):
    """
    Retrieve the price for the same flight (origin, destination, date, type)
    on the previous day 'date_retrieved'. Return None if not found.
    """
    query = {
        "bool": {
            "must": [
                {"term": {"origin": origin}},
                {"term": {"destination": destination}},
                {"term": {"date": flight_date}},
                {"term": {"type": flight_type}},
                {"term": {"date_retrieved": prev_date_retrieved}}
            ]
        }
    }

    resp = es.search(
        index=ELASTICSEARCH_INDEX,
        body={
            "query": query,
            "size": 1
        }
    )
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return None
    # We only asked for 1 doc, so just return the price from the first one
    return hits[0]["_source"]["price"]

def display_oneway_results(base_query):
    """
    Fetch up to 100 cheapest oneway flights and display them.
    Also calculate price change vs. previous day if available.
    """
    results = search_elasticsearch(query_body=base_query, size=100)

    # Prepare data for display
    rows = []
    # We'll assume date_retrieved is 'YYYYMMDD'. 
    # We'll try to find the 'previous day' in the same format.
    # If the user used a single date_retrieved in base_query, we can read it from the query structure,
    # or from the first doc in results. For simplicity, let's read from the doc itself (assuming all have same date_retrieved).
    for doc in results:
        origin = doc.get("origin")
        destination = doc.get("destination")
        date_retrieved = doc.get("date_retrieved")
        flight_date = doc.get("date")
        raw_price = doc.get("price", 0)
        # Convert price to real price
        real_price = round(raw_price / 100, 2)

        # Price difference vs previous day
        try:
            current_dr = datetime.strptime(date_retrieved, "%Y%m%d")
            prev_day = (current_dr).replace(day=current_dr.day - 1)  # naive approach
            prev_date_retrieved_str = prev_day.strftime("%Y%m%d")
        except ValueError:
            # If there's some date parsing error, skip difference
            prev_date_retrieved_str = None

        if prev_date_retrieved_str:
            prev_price = get_previous_day_price(
                origin=origin,
                destination=destination,
                flight_date=flight_date,
                flight_type=doc.get("type"),
                prev_date_retrieved=prev_date_retrieved_str
            )
            if prev_price is not None:
                price_diff = round((raw_price - prev_price) / 100, 2)
            else:
                price_diff = None
        else:
            price_diff = None

        rows.append({
            "origin": origin,
            "destination": destination,
            "date_retrieved": date_retrieved,
            "date": flight_date,
            "price": real_price,
            "type": doc.get("type"),
            "price_change_vs_previous_day": price_diff if price_diff is not None else "N/A"
        })

    df = pd.DataFrame(rows)
    st.dataframe(df)

def display_roundtrip_results(base_query):
    """
    For roundtrip, we fetch two sets:
     - set1 (direction=forth)
     - set2 (direction=back)
    Then, combine them to find the 100 cheapest sums.
    """
    # Make copies of base_query so we can add direction constraints
    import copy
    forth_query = copy.deepcopy(base_query)
    forth_query["bool"]["must"].append({"term": {"direction": "forth"}})

    back_query = copy.deepcopy(base_query)
    back_query["bool"]["must"].append({"term": {"direction": "back"}})

    # Fetch results
    set1 = search_elasticsearch(forth_query, size=1000)  # might fetch more, then filter
    set2 = search_elasticsearch(back_query, size=1000)

    # We want to find valid pairs:
    #  item1.destination = item2.destination
    #  item1.origin = item2.origin
    #  item2.date > item1.date
    # Then sum price (lowest 100).
    # For better efficiency, you could index by (origin, destination) etc. 
    # For demonstration, we'll do a naive nested loop.  

    pairs = []
    for f in set1:
        for b in set2:
            if (
                f.get("origin") == b.get("origin") and
                f.get("destination") == b.get("destination")
            ):
                # Compare date fields as strings "YYYYMMDD" => convert to date
                try:
                    f_date = datetime.strptime(f["date"], "%Y%m%d")
                    b_date = datetime.strptime(b["date"], "%Y%m%d")
                except ValueError:
                    continue

                if b_date > f_date:  # date of item2 is higher than date of item1
                    total_price = f["price"] + b["price"]
                    pairs.append({
                        "origin": f["origin"],
                        "destination": f["destination"],
                        "forth_date": f["date"],
                        "back_date": b["date"],
                        "forth_price": round(f["price"] / 100, 2),
                        "back_price": round(b["price"] / 100, 2),
                        "total_price": round(total_price / 100, 2)
                    })

    # sort by total_price ascending
    pairs.sort(key=lambda x: x["total_price"])
    # take top 100
    cheapest_pairs = pairs[:100]

    df = pd.DataFrame(cheapest_pairs)
    st.dataframe(df)

def main():
    st.title("Condor Flight Search")

    # --- SIDEBAR INPUTS ---
    # 1. Required: Destination code
    destination = st.sidebar.text_input("Destination (required)")

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
    #    Streamlit date_input returns a datetime.date or a list of them. We'll keep them as date objects.
    min_date_val = st.sidebar.date_input("Minimum Date (optional)", value=None)
    max_date_val = st.sidebar.date_input("Maximum Date (optional)", value=None)

    # If user leaves them empty, it might return today's date (Streamlit behavior),
    # so we need to check if they've actually chosen a date or not.
    # For demonstration, we'll just trust that the user picks or not.
    # If you want truly empty by default, you can do something more sophisticated.

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

    # --- BUILD QUERY ---
    base_query = build_base_query(
        destination=destination.upper(),
        origin_exclusions=origin_exclusions,
        origin_codes=origin_codes,
        flight_type=flight_type,
        min_date_val=min_date_val if isinstance(min_date_val, date) else None,
        max_date_val=max_date_val if isinstance(max_date_val, date) else None,
        date_retrieved_val=date_retrieved_val,
        concrete_date_val=concrete_date_val
    )

    # --- EXECUTE & DISPLAY RESULTS ---
    if flight_type == "oneway":
        st.subheader("Oneway Results (100 cheapest)")
        display_oneway_results(base_query)
    else:
        st.subheader("Roundtrip Results (100 cheapest pairs)")
        display_roundtrip_results(base_query)

if __name__ == "__main__":
    main()
