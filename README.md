# Real-Time-Flight-Data-Pipeline-Airflow-Snowflake

A batch pipeline that pulls live aircraft positions from the [OpenSky Network](https://opensky-network.org/) API every 30 minutes, processes them through a **medallion architecture** (bronze → silver → gold) with Apache Airflow, loads the results into Snowflake, and visualises them in a Snowsight dashboard.

## Architecture

```
OpenSky API
    │
    ▼
Bronze  – raw JSON, exactly as received (one file per run)
    │
    ▼
Silver  – cleaned CSV, one row per aircraft (adds snapshot time and airline code)
    │
    ▼
Gold    – per-country summary CSV (flights, % on ground, airborne speed, share of total)
    │
    ▼
Snowflake – FLIGHTS_SILVER (aircraft) + FLIGHT_TABLE (country summary)
    │
    ▼
Snowsight dashboard
```

**Airflow DAG:** `flights_ops_medallion_pipe`, every 30 minutes (`*/30 * * * *`)
`bronze_ingest → silver_transform → gold_layer → snowflake_load`

| Layer | Script | What it does |
|---|---|---|
| Bronze | `scripts/bronze_layer.py` | Calls the API and saves the raw response to `data/bronze/flights_<timestamp>.json` |
| Silver | `scripts/silver_layer.py` | Names the 17 columns, keeps the useful ones, strips callsigns, extracts the 3-letter airline code, adds the snapshot time |
| Gold | `scripts/gold_layer.py` | Per country: total flights, % on ground, share of total, average speed of **airborne** aircraft only (km/h) |
| Load | `scripts/snowflake_implement.py` | MERGEs gold into `FLIGHT_TABLE`; appends silver to `FLIGHTS_SILVER` (deletes the same snapshot first, so re-runs don't duplicate) |

## Tech stack

Apache Airflow 2.9.3 · Docker Compose · PostgreSQL 15 (Airflow metadata) · Python / pandas · Snowflake · OpenSky REST API

## Project structure

```
.
├── dags/
│   └── flight_pipeline.py        # the Airflow DAG
├── scripts/
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   ├── gold_layer.py
│   └── snowflake_implement.py
├── sql/
│   ├── 01_setup.sql              # Snowflake database, tables, view
│   └── 02_dashboard_queries.sql  # one query per dashboard tile
├── docs/                         # dashboard screenshots
├── docker-compose.yml
├── .env.example
└── README.md
```

## Getting started

1. **Clone the repo** and copy the environment file:
   ```bash
   cp .env.example .env     # then edit .env with your own values
   ```
2. **Start Airflow:**
   ```bash
   docker compose up -d
   ```
   Open `http://localhost:8080` and log in with the admin user from your `.env`.
3. **Set up Snowflake:** run `sql/01_setup.sql` in a Snowsight worksheet (role `ACCOUNTADMIN`).
4. **Create the Airflow connection** (Admin → Connections → `+`):
   - Connection Id: `flight_snowflake`
   - Connection Type: `Snowflake`
   - Login: your Snowflake **login name** (check with `SHOW USERS`; it can be an email address)
   - Password: your Snowflake password
   - Schema: `FLIGHTS_SCHEMA`
   - Extra:
     ```json
     {
       "account": "<account_locator>.<region>.<cloud>",
       "warehouse": "COMPUTE_WH",
       "database": "FLIGHTS",
       "role": "ACCOUNTADMIN"
     }
     ```
5. **Run it:** unpause `flights_ops_medallion_pipe` and trigger a run with the play button.
6. **Build the dashboard:** in Snowsight, create a dashboard and add one tile per query in `sql/02_dashboard_queries.sql`.

## Dashboard

![Dashboard](docs/dashboard.png)

Seven tiles that read as a story: how big the sky is (aircraft tracked, % in the air, data freshness) → who flies it (top airlines, top countries) → what they are doing (flight phase) → how it changes over the day (traffic over time).

## Things to know

- **"Country" means where the aircraft is registered**, not where it is flying.
- **Coverage is uneven.** OpenSky data comes from volunteer receivers, which are dense in Europe and the US and thin over oceans and parts of Asia and Africa.
- **Anonymous API access is rate-limited**, so very frequent runs can fail with HTTP 429.
- **Raw speeds can contain glitches** (values above 1,500 km/h), so speed is only used as an airborne average, not as a "fastest aircraft" ranking.
- All timestamps are **UTC**.
- If you signed up to Snowflake with Google, your user may have no password. Set one with `ALTER USER <name> SET PASSWORD = '...'`, or create a dedicated user for Airflow.

## Credits

Based on [data-guru0/AIRFLOW-SNOWFLAKE-PROJECT](https://github.com/data-guru0/AIRFLOW-SNOWFLAKE-PROJECT). Changes in this version: more silver columns (position, altitude, vertical rate, airline code), timestamped silver/gold files so runs don't overwrite each other, airborne-only speed in gold, silver loaded to Snowflake for aircraft-level analysis, and a Snowsight dashboard.
