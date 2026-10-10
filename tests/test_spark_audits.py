"""Spark-side tests on the real one-symbol stream (VIXY, 2019-12-30): scaled replay and the SQL audits."""
import pathlib

import pandas as pd
import pytest

from conftest import LOCATE
from itch_forensics.book import ANOMALIES, build_events, replay
from itch_forensics.build import build
from itch_forensics.decode import decode_all
from itch_forensics.framing import index_messages
from itch_forensics.scaled import replay_by_stock
from itch_forensics.spark import get_spark, register, run_sql


@pytest.fixture(scope="module")
def spark(tmp_path_factory, raw):
    d = tmp_path_factory.mktemp("itch")
    (d / "raw.bin").write_bytes(raw)
    build(d / "raw.bin", d / "parquet")
    s = get_spark("itch-tests", shuffle_partitions=2)
    register(s, d / "parquet")
    s.parquet_dir = d / "parquet"
    yield s
    s.stop()


def test_scaled_replay_equals_single_process_replay(spark, raw):
    idx = index_messages(raw)
    ev = build_events(decode_all(raw, idx))
    bad, _, bbo, state = replay(ev, [LOCATE])
    r = replay_by_stock(spark)
    assert len(r) == 1 and int(r.locate[0]) == LOCATE
    row = r.iloc[0]
    assert (row.events, row.live_orders, row.resting_shares_orders) == (len(ev), state["live_orders"], 148664)
    assert row.resting_shares_orders == row.resting_shares_levels
    assert row.bbo_changes == len(bbo) == 291
    assert row.executed_shares == state["executed_by_locate"][LOCATE]
    assert all(row[a] == 0 for a in ANOMALIES) and bad == {}


def test_lifetime_cohort_accounts_for_every_order(spark):
    """With no follow-up cut every order is in the cohort. Each real delete and replace ends exactly one order."""
    life = run_sql(spark, "03_order_lifetime", cohort_end_ns=1 << 62)
    by = life.groupby("how_it_ended").orders.sum().to_dict()
    assert sum(by.values()) == 1097 + 72 + 153            # A + F + orders created by a replace
    assert by["deleted"] == 871 and by["replaced"] == 153  # D and U messages in the stream
    assert by.get("still resting after follow-up", 0) == 254   # live orders at the end of the replay


def test_lifetime_cohort_excludes_orders_added_too_late(spark):
    everyone = run_sql(spark, "03_order_lifetime", cohort_end_ns=1 << 62).orders.sum()
    early = run_sql(spark, "03_order_lifetime", cohort_end_ns=25_000_000_000_000).orders.sum()   # before ~06:56
    assert 0 < early < everyone


def test_halt_audit_finds_nothing_in_a_trading_stock(spark):
    out = run_sql(spark, "05_halts")
    assert set(out.state) == {"T"} and out.events.sum() > 0


def test_halt_audit_catches_executions_in_a_halted_stock(spark):
    """Negative control: mark the stock halted from the start of the day and the executions must be reported."""
    ta = pd.read_parquet(spark.parquet_dir / "trading_action.parquet")
    ta["trading_state"] = "H"
    ta["ts_ns"] = 0
    spark.createDataFrame(ta).createOrReplaceTempView("trading_action")
    out = run_sql(spark, "05_halts")
    halted = out[out.state == "H"]
    assert halted.events.sum() == 55 + 5                  # 55 executions (E) and 5 non-displayed trades (P)
    register(spark, spark.parquet_dir)                    # restore the real table for any later test
