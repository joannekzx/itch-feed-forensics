"""The order-book replay, run per stock on PySpark.

Every ITCH order message carries the stock locate in its header, and a replace keeps the order in
the same stock, so the book of one stock never depends on another. That makes the replay
embarrassingly parallel by locate: group the event table by locate and replay each group with the
same `replay` function used in the single-process version.

Assumption: an order's later messages carry the same locate as its add. The single-process replay
counts violations as `locate_mismatch`; here a violation would instead show up as an unknown
reference inside a group. On the 300 MB sample there are none.
"""
import pandas as pd
from pyspark.sql.types import LongType, StructField, StructType

from .book import ANOMALIES, replay
from .spark import SQL_DIR

SCHEMA = StructType([StructField(n, LongType()) for n in
                     ("locate", "events", "live_orders", "resting_shares_orders", "resting_shares_levels",
                      "executed_shares", "bbo_changes", *ANOMALIES)])


def _replay_one_stock(pdf: pd.DataFrame) -> pd.DataFrame:
    loc = int(pdf["locate"].iloc[0])
    ev = pdf.sort_values("seq", kind="stable").reset_index(drop=True)
    ev["ref"] = ev["ref"].astype("uint64")
    ev["new_ref"] = ev["new_ref"].astype("uint64")
    bad, _, bbo, state = replay(ev, monitored=[loc])
    row = {"locate": loc, "events": len(ev), "live_orders": state["live_orders"],
           "resting_shares_orders": state["resting_shares_orders"],
           "resting_shares_levels": state["resting_shares_levels"],
           "executed_shares": int(state["executed_by_locate"].get(loc, 0)),
           "bbo_changes": len(bbo)}
    row.update({a: int(bad.get(a, 0)) for a in ANOMALIES})
    return pd.DataFrame([row])


def replay_by_stock(spark) -> pd.DataFrame:
    """One row per stock. Requires the tables registered by `itch_forensics.spark.register`."""
    spark.sql((SQL_DIR / "04_order_events.sql").read_text()).createOrReplaceTempView("events")
    out = spark.table("events").groupBy("locate").applyInPandas(_replay_one_stock, schema=SCHEMA)
    return out.toPandas().sort_values("locate").reset_index(drop=True)


if __name__ == "__main__":
    # python -m itch_forensics.scaled --parquet data/parquet --cores 2   (prints one JSON line)
    import argparse
    import json
    import time

    from .spark import get_spark, register
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--cores", default="*")
    a = ap.parse_args()
    sp = get_spark(f"replay-{a.cores}", shuffle_partitions=16, cores=a.cores)
    register(sp, a.parquet)
    t = time.time()
    r = replay_by_stock(sp)
    dt = time.time() - t
    print(json.dumps({"cores": a.cores, "seconds": round(dt, 1), "events": int(r.events.sum()),
                      "events_per_s": round(r.events.sum() / dt)}))
