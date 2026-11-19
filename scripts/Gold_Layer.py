import pandas as pd
from pathlib import Path

def run_gold_layer(**context):

    silver_file = context["ti"].xcom_pull(
        key="silver_file",
        task_ids="silver_transform"
    )

    df = pd.read_csv(silver_file)

    # Speed should only count planes that are flying, not parked ones
    airborne = df[df["on_ground"] == False]
    avg_speed = (
        airborne.groupby("origin_country")["velocity"].mean().mul(3.6).round(0)
        .rename("avg_airborne_kmh")
    )

    agg = (
        df.groupby("origin_country")
        .agg(
            total_flights=("icao24", "count"),
            on_ground=("on_ground", "sum"),
        )
        .join(avg_speed)
        .reset_index()
    )

    agg["pct_on_ground"] = (100 * agg["on_ground"] / agg["total_flights"]).round(1)
    agg["share_of_total_pct"] = (100 * agg["total_flights"] / agg["total_flights"].sum()).round(2)
    agg["snapshot_time"] = df["snapshot_time"].iloc[0]

    agg = agg.sort_values("total_flights", ascending=False)

    gold_path = Path(silver_file.replace("silver", "gold"))
    gold_path.parent.mkdir(parents=True, exist_ok=True)

    agg.to_csv(gold_path, index=False)

    context["ti"].xcom_push(key="gold_file", value=str(gold_path))