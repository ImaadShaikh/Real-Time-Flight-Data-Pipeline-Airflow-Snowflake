import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook


def num(x, cast=float):
    """Turn empty values (NaN) into None so Snowflake stores NULL."""
    return None if pd.isna(x) else cast(x)


def snowflake_load(**context):

    gold_file = context["ti"].xcom_pull(key="gold_file", task_ids="gold_layer")
    silver_file = context["ti"].xcom_pull(key="silver_file", task_ids="silver_transform")

    if not gold_file or not silver_file:
        raise ValueError("Gold or silver file not found in XCom")

    execution_date = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")

    gold = pd.read_csv(gold_file)
    silver = pd.read_csv(silver_file)

    conn = BaseHook.get_connection("flight_snowflake")

    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database="FLIGHTS",
        schema="FLIGHTS_SCHEMA",
        role=conn.extra_dejson.get("role"),
    )

    merge_sql = """
        MERGE INTO FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE tgt
        USING (
            SELECT
                TO_TIMESTAMP(%s) AS WINDOW_START,
                %s AS ORIGIN_COUNTRY,
                %s AS TOTAL_FLIGHTS,
                %s AS AVG_AIRBORNE_KMH,
                %s AS ON_GROUND,
                %s AS PCT_ON_GROUND,
                %s AS SHARE_OF_TOTAL_PCT
        ) src
        ON tgt.WINDOW_START = src.WINDOW_START
           AND tgt.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY

        WHEN MATCHED THEN UPDATE SET
            TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
            AVG_AIRBORNE_KMH = src.AVG_AIRBORNE_KMH,
            ON_GROUND = src.ON_GROUND,
            PCT_ON_GROUND = src.PCT_ON_GROUND,
            SHARE_OF_TOTAL_PCT = src.SHARE_OF_TOTAL_PCT,
            LOAD_TIME = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT
        (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_AIRBORNE_KMH,
         ON_GROUND, PCT_ON_GROUND, SHARE_OF_TOTAL_PCT)
        VALUES
        (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_AIRBORNE_KMH,
         src.ON_GROUND, src.PCT_ON_GROUND, src.SHARE_OF_TOTAL_PCT);
    """

    create_silver_sql = """
        CREATE TABLE IF NOT EXISTS FLIGHTS.FLIGHTS_SCHEMA.FLIGHTS_SILVER (
            SNAPSHOT_TIME TIMESTAMP_NTZ,
            ICAO24 STRING,
            CALLSIGN STRING,
            AIRLINE_CODE STRING,
            ORIGIN_COUNTRY STRING,
            LONGITUDE FLOAT,
            LATITUDE FLOAT,
            BARO_ALTITUDE FLOAT,
            VELOCITY FLOAT,
            VERTICAL_RATE FLOAT,
            ON_GROUND BOOLEAN
        )
    """

    insert_silver_sql = """
        INSERT INTO FLIGHTS.FLIGHTS_SCHEMA.FLIGHTS_SILVER
        (SNAPSHOT_TIME, ICAO24, CALLSIGN, AIRLINE_CODE, ORIGIN_COUNTRY, LONGITUDE,
         LATITUDE, BARO_ALTITUDE, VELOCITY, VERTICAL_RATE, ON_GROUND)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        with sf_conn.cursor() as cursor:

            # ---- Gold: one row per country (MERGE = no duplicates on re-run)
            for _, row in gold.iterrows():
                cursor.execute(
                    merge_sql,
                    (
                        execution_date,
                        row["origin_country"],
                        int(row["total_flights"]),
                        num(row["avg_airborne_kmh"]),
                        int(row["on_ground"]),
                        num(row["pct_on_ground"]),
                        num(row["share_of_total_pct"]),
                    ),
                )

            # ---- Silver: one row per aircraft, appended every snapshot
            cursor.execute(create_silver_sql)

            snapshot_time = str(silver["snapshot_time"].iloc[0])

            # Delete this snapshot first so a re-run can't create duplicates
            cursor.execute(
                "DELETE FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHTS_SILVER WHERE SNAPSHOT_TIME = %s",
                (snapshot_time,),
            )

            cols = ["snapshot_time", "icao24", "callsign", "airline_code", "origin_country",
                    "longitude", "latitude", "baro_altitude", "velocity",
                    "vertical_rate", "on_ground"]
            clean = silver[cols].astype(object).where(silver[cols].notna(), None)
            cursor.executemany(insert_silver_sql, clean.values.tolist())
    finally:
        sf_conn.close()