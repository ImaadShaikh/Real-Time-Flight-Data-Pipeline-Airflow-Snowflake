import json
import pandas as pd
from pathlib import Path


def run_silver_transform(**context):
    execution_date = context["ts_nodash"]

    bronze_file = context["ti"].xcom_pull(
        key = "bronze_file",
        task_ids = "bronze_ingest"
    )

    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom")


    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents = True , exist_ok =  True)

    with open(bronze_file) as f:
        raw = json.load(f)

    df_raw = pd.DataFrame(raw["states"])

    df_raw.columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
        "latitude", "baro_altitude", "on_ground",
        "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]

    # When OpenSky took this snapshot (same value for every row in the file)
    df_raw["snapshot_time"] = pd.to_datetime(raw["time"], unit="s")

    # Clean the callsign and pull out the 3-letter airline code (EK, QR, etc. style)
    df_raw["callsign"] = df_raw["callsign"].str.strip()
    df_raw["airline_code"] = df_raw["callsign"].str.extract(r"^([A-Z]{3})\d")[0]

    df = df_raw[
        [
            "snapshot_time",
            "icao24",
            "callsign",
            "airline_code",
            "origin_country",
            "longitude",
            "latitude",
            "baro_altitude",
            "velocity",
            "vertical_rate",
            "on_ground"
        ]
    ]

    output_file = silver_path / f"flights_silver_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    context["ti"].xcom_push(
        key="silver_file",
        value=str(output_file)
    )