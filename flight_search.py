from elasticsearch import Elasticsearch
from datetime import datetime, date
from typing import List, Dict, Optional, Union
import os

class FlightSearch:
    def __init__(self, host: str, user: str, password: str, index: str = "condor_data"):
        """Initialize the FlightSearch with Elasticsearch connection details."""
        self.es = Elasticsearch(
            [host],
            http_auth=(user, password),
            verify_certs=False
        )
        self.index = index

    def build_base_query(
        self,
        destination: str,
        origin_exclusions: List[str] = None,
        origin_codes: List[str] = None,
        flight_type: str = "oneway",
        min_date_val: Optional[date] = None,
        max_date_val: Optional[date] = None,
        date_retrieved_val: Optional[str] = None,
        concrete_date_val: Optional[str] = None
    ) -> Dict:
        """Build the base Elasticsearch query."""
        must_clauses = []
        must_not_clauses = []

        # Handle destinations
        destinations = [d.strip().upper() for d in destination.split(",") if d.strip()]
        if len(destinations) == 1:
            must_clauses.append({"term": {"destination": destinations[0]}})
        else:
            must_clauses.append({"terms": {"destination": destinations}})

        # Handle origin exclusions
        if origin_exclusions:
            must_not_clauses.append({"terms": {"origin": origin_exclusions}})

        # Handle origin inclusions
        if origin_codes:
            must_clauses.append({"terms": {"origin": origin_codes}})

        # Flight type
        must_clauses.append({"term": {"type": flight_type}})

        # Date retrieved
        if date_retrieved_val:
            must_clauses.append({"term": {"date_retrieved": date_retrieved_val}})

        # Handle dates
        if concrete_date_val:
            must_clauses.append({"term": {"date": concrete_date_val}})
        else:
            date_range = {}
            if min_date_val:
                date_range["gte"] = min_date_val.strftime("%Y%m%d")
            if max_date_val:
                date_range["lte"] = max_date_val.strftime("%Y%m%d")

            if date_range:
                must_clauses.append({"range": {"date": date_range}})

        return {
            "bool": {
                "must": must_clauses,
                "must_not": must_not_clauses
            }
        }

    def search_elasticsearch(self, query_body: Dict, size: int = 1000) -> List[Dict]:
        """Execute a search against Elasticsearch."""
        response = self.es.search(
            index=self.index,
            body={
                "query": query_body,
                "sort": [{"price": {"order": "asc"}}],
                "size": size
            }
        )
        hits = response.get("hits", {}).get("hits", [])
        return [dict(hit["_source"], _id=hit["_id"]) for hit in hits]

    def get_previous_day_price(
        self,
        origin: str,
        destination: str,
        flight_date: str,
        flight_type: str,
        prev_date_retrieved: str
    ) -> Optional[float]:
        """Get the price for the same flight from the previous day."""
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

        resp = self.es.search(
            index=self.index,
            body={"query": query, "size": 1}
        )
        hits = resp.get("hits", {}).get("hits", [])
        return hits[0]["_source"]["price"] if hits else None

    def get_oneway_results(self, base_query: Dict) -> List[Dict]:
        """Get formatted oneway flight results."""
        results = self.search_elasticsearch(query_body=base_query, size=100)
        formatted_results = []

        for doc in results:
            origin = doc.get("origin")
            destination = doc.get("destination")
            date_retrieved = doc.get("date_retrieved")
            flight_date = doc.get("date")
            raw_price = doc.get("price", 0)
            real_price = round(raw_price / 100, 2)

            # Calculate price difference
            try:
                current_dr = datetime.strptime(date_retrieved, "%Y%m%d")
                prev_day = (current_dr).replace(day=current_dr.day - 1)
                prev_date_retrieved_str = prev_day.strftime("%Y%m%d")
                prev_price = self.get_previous_day_price(
                    origin=origin,
                    destination=destination,
                    flight_date=flight_date,
                    flight_type=doc.get("type"),
                    prev_date_retrieved=prev_date_retrieved_str
                )
                price_diff = round((raw_price - prev_price) / 100, 2) if prev_price is not None else None
            except ValueError:
                price_diff = None

            formatted_results.append({
                "origin": origin,
                "destination": destination,
                "date_retrieved": date_retrieved,
                "date": flight_date,
                "price": real_price,
                "type": doc.get("type"),
                "price_change_vs_previous_day": price_diff if price_diff is not None else "N/A"
            })

        return formatted_results

    def get_roundtrip_results(self, base_query: Dict) -> List[Dict]:
        """Get formatted roundtrip flight results."""
        import copy

        # Split query for forth and back flights
        forth_query = copy.deepcopy(base_query)
        forth_query["bool"]["must"].append({"term": {"direction": "forth"}})

        back_query = copy.deepcopy(base_query)
        back_query["bool"]["must"].append({"term": {"direction": "back"}})

        # Get results
        set1 = self.search_elasticsearch(forth_query, size=1000)
        set2 = self.search_elasticsearch(back_query, size=1000)

        pairs = []
        for f in set1:
            for b in set2:
                if (f.get("origin") == b.get("origin") and
                    f.get("destination") == b.get("destination")):
                    try:
                        f_date = datetime.strptime(f["date"], "%Y%m%d")
                        b_date = datetime.strptime(b["date"], "%Y%m%d")
                    except ValueError:
                        continue

                    if b_date > f_date:
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

        pairs.sort(key=lambda x: x["total_price"])
        return pairs[:100] 