import requests
import json
import ndjson
import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from pprint import pprint
from typing import Dict, List, Any, Optional
import time

# Flights Lab API 
API_KEY = os.environ.get("FLIGHTS_API_KEY")
URL = f"https://app.goflightlabs.com/historical/2023-02-01?access_key={API_KEY}&code=LHW&type=arrival&date_to=2023-02-25&codeshared=null"


# BigQuery
CREDENTIALS_PATH = "/Users/leonchristie/Documents/GitHub/Date-Engineering-Cities-Project/pythonflightsbq.privatekey.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
BQ_CLIENT = bigquery.Client()
TABLE_ID = "flights-project-379412.heathrow.heathrow-flights"



# API call and caching
def fetch_data(*, update: bool = False, json_cache: str, URL: str):
    if update:
        flight_data = None
    else:
        try:
            with open(json_cache, 'r') as file:
                flight_data = json.load(file)
                print('Data fetched from local cache.')
        except(FileNotFoundError, json.JSONDecodeError) as e:
                print(f'no local cache found... ({e})')
                flight_data = None

    if not flight_data:
        print('Fetching new json data... (Creating Local Cache)')
        json_data = requests.get(URL).json()
        flight_data: Optional[List[Dict[str, Any]]] = json_data["data"]
        with open (json_cache, 'w') as file:
            ndjson.dump(flight_data, file)

    return flight_data

if __name__ == '__main__':
    json_cache = "Cached Data/json_cache.ndjson"
  
    data = fetch_data(update=False, json_cache=json_cache, URL=URL)




# Schema
custom_schema = [
    bigquery.SchemaField("type", "string", mode="NULLABLE"),
    bigquery.SchemaField("status", "string", mode="NULLABLE"),
    bigquery.SchemaField(
        "departure",
        "string",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("iataCode", "string", mode="NULLABLE"),
            bigquery.SchemaField("iacoCode", "string", mode="NULLABLE"),
            bigquery.SchemaField("terminal", "string", mode="NULLABLE"),
            bigquery.SchemaField("gate", "string", mode="NULLABLE"),
            bigquery.SchemaField("scheduledTime", "string", mode="NULLABLE"),
            bigquery.SchemaField("estimatedTime", "string", mode="NULLABLE"),
            bigquery.SchemaField("actualTime", "string", mode="NULLABLE"),
            bigquery.SchemaField("estimatedRunway", "string", mode="NULLABLE"),
            bigquery.SchemaField("actualRunway", "string", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "arrival",
        "string",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("iataCode", "string", mode="NULLABLE"),
            bigquery.SchemaField("iacoCode", "string", mode="NULLABLE"),
            bigquery.SchemaField("terminal", "string", mode="NULLABLE"),
            bigquery.SchemaField("baggage", "string", mode="NULLABLE"),
            bigquery.SchemaField("scheduledTime", "string", mode="NULLABLE"),
            bigquery.SchemaField("estimatedTime", "string", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "airline",
        "string",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("name", "string", mode="NULLABLE"),
            bigquery.SchemaField("iatoCode", "string", mode="NULLABLE"),
            bigquery.SchemaField("iacoCode", "string", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "flight",
        "string",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("number", "string", mode="NULLABLE"),
            bigquery.SchemaField("iatoNaumber", "string", mode="NULLABLE"),
            bigquery.SchemaField("iacoNumber", "string", mode="NULLABLE"),
        ],
    ),
]

# Config
job_config = bigquery.LoadJobConfig(
    schema=custom_schema,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition="WRITE_APPEND",
)

# Send data to BigQeury
with open("flights_data_nld.json", "rb") as source_file:
    job = BQ_CLIENT.load_table_from_file(source_file, TABLE_ID, job_config=job_config)

response = job.result()


