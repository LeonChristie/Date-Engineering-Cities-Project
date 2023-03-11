import os
from typing import Any, Dict, List, Optional

import ndjson
import requests
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

# Flights Lab API
API_KEY = os.environ.get("FLIGHTS_API_KEY")
URL = f"https://app.goflightlabs.com/historical/2023-03-07?access\
    _key={API_KEY}&code=LHW&type=arrival&date_to=2023-03-08"


# BigQuery
CREDENTIALS_PATH = "pythonflightsbq.privatekey.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
BQ_CLIENT = bigquery.Client()
TABLE_ID = "flights-project-379412.heathrow.heathrow-flights"


# Data fetching and caching
def data_cacher(use_cache: bool, json_cache: str, URL: str) -> bool:
    if use_cache:
        # Check if cached data exists
        _cache_exists: bool = os.path.exists(json_cache)
        if _cache_exists:
            return True
        else:
            json_data = requests.get(URL).json()
            flight_data: Optional[List[Dict[str, Any]]] = json_data["data"]
            with open(json_cache, "w") as file:
                ndjson.dump(flight_data, file)
            return True
    elif not use_cache:
        json_data = requests.get(URL).json()
        flight_data = json_data["data"]
        with open(json_cache, "w") as file:
            ndjson.dump(flight_data, file)
        print("There was no data in the cache, force fetching data...")
        print("Data cached for optional use later")
        return True
    return False


# Schema
custom_schema = [
    SchemaField("type", "STRING", mode="NULLABLE"),
    SchemaField("status", "STRING", mode="NULLABLE"),
    SchemaField(
        "departure",
        "RECORD",
        fields=[
            SchemaField("iataCode", "STRING", mode="NULLABLE"),
            SchemaField("iacoCode", "STRING", mode="NULLABLE"),
            SchemaField("terminal", "STRING", mode="NULLABLE"),
            SchemaField("gate", "STRING", mode="NULLABLE"),
            SchemaField("delay", "INT64", mode="NULLABLE"),
            SchemaField("scheduledTime", "STRING", mode="NULLABLE"),
            SchemaField("estimatedTime", "STRING", mode="NULLABLE"),
            SchemaField("actualTime", "STRING", mode="NULLABLE"),
            SchemaField("estimatedRunway", "STRING", mode="NULLABLE"),
            SchemaField("actualRunway", "STRING", mode="NULLABLE"),
        ],
    ),
    SchemaField(
        "arrival",
        "RECORD",
        fields=[
            SchemaField("iataCode", "STRING", mode="NULLABLE"),
            SchemaField("iacoCode", "STRING", mode="NULLABLE"),
            SchemaField("terminal", "STRING", mode="NULLABLE"),
            SchemaField("baggage", "STRING", mode="NULLABLE"),
            SchemaField("delay", "INT64", mode="NULLABLE"),
            SchemaField("scheduledTime", "STRING", mode="NULLABLE"),
            SchemaField("estimatedTime", "STRING", mode="NULLABLE"),
        ],
    ),
    SchemaField(
        "airline",
        "RECORD",
        fields=[
            SchemaField("name", "STRING", mode="NULLABLE"),
            SchemaField("iataCode", "STRING", mode="NULLABLE"),
            SchemaField("iacoCode", "STRING", mode="NULLABLE"),
        ],
    ),
    SchemaField(
        "flight",
        "RECORD",
        fields=[
            SchemaField("number", "STRING", mode="NULLABLE"),
            SchemaField("iataNaumber", "STRING", mode="NULLABLE"),
            SchemaField("iacoNumber", "STRING", mode="NULLABLE"),
        ],
    ),
    SchemaField(
        "codeshared",
        "RECORD",
        fields=[
            SchemaField(
                "airline",
                "RECORD",
                fields=[
                    SchemaField("name", "STRING", mode="NULLABLE"),
                    SchemaField("iataCode", "STRING", mode="NULLABLE"),
                    SchemaField("iacoCode", "STRING", mode="NULLABLE"),
                ],
            ),
            SchemaField(
                "flight",
                "RECORD",
                fields=[
                    SchemaField("number", "STRING", mode="NULLABLE"),
                    SchemaField("iataNumber", "STRING", mode="NULLABLE"),
                    SchemaField("iacoNumber", "STRING", mode="NULLABLE"),
                ],
            ),
        ],
    ),
]


def main(URL: str, use_cache: bool, json_cache: str):

    if data_cacher(use_cache, json_cache, URL):
        # # Config
        job_config = bigquery.LoadJobConfig(
            schema=custom_schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            ignore_unknown_values=True,
        )
        # # Send data to BigQeury
        with open("cached_data/json_cache.ndjson", "rb") as source_file:
            job = BQ_CLIENT.load_table_from_file(
                source_file, TABLE_ID, job_config=job_config
            )
            print(job.state)
            job.result()
            print(job.state)


if __name__ == "__main__":
    use_cache = True
    json_cache = "cached_data/json_cache.ndjson"
    URL = URL
    main(URL, use_cache, json_cache)
