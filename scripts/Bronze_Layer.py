import requests
import json
from datetime import datetime
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingestion(**context):

    response = requests.get(URL , timeout=30)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")


    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    path.parent.mkdir(parents = True , exist_ok = True)

    with open(path , "w") as f:
        json.dump(data ,f)

    context['ti'].xcom_push(key = 'bronze_file', value = str(path))










# =============================================================================
# DOCUMENTATION
# =============================================================================
#
# Purpose
# -------
# Bronze ingestion step of the flight data pipeline. Pulls live flight
# positions from the OpenSky Network API and saves the raw response as a
# timestamped JSON file. No cleaning happens here; that is the silver step.
#
# Function
# --------
# run_bronze_ingestion(**context)
#   Called by an Airflow PythonOperator. Airflow passes in `context`, which
#   holds run details such as the task instance ("ti").
#
# What it does
# ------------
# 1. Calls the OpenSky API (30 second timeout).
# 2. Fails the task on any error status (429, 500, etc.) so Airflow can retry.
# 3. Builds a unique filename from the current UTC time.
# 4. Creates the bronze folder if it doesn't exist.
# 5. Saves the raw response to disk, unchanged.
# 6. Pushes the file path to XCom for the next task.
#
# Input
# -----
# URL: https://opensky-network.org/api/states/all
#   Returns the current state (position, speed, altitude, etc.) of all
#   tracked flights worldwide. Anonymous access is rate-limited.
#
# Output
# ------
# File:  /opt/airflow/data/bronze/flights_<YYYYMMDDHHMMSS>.json
#        (appears in ./data/bronze on the host via the Docker volume)
# XCom:  key="bronze_file", value=<full file path as a string>
#        Read it in the next task with: ti.xcom_pull(key="bronze_file")
#
# Errors
# ------
# requests.exceptions.HTTPError : API returned an error status
# requests.exceptions.Timeout   : API took longer than 30 seconds
#
# Notes
# -----
# - Bronze layer = raw data exactly as received. Silver = cleaned,
#   gold = ready for reporting.
# - Each run writes a new file and never overwrites an old one.
# - datetime.utcnow() is deprecated in newer Python; the modern version is
#   datetime.now(timezone.utc).