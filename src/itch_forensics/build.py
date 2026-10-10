"""Decode a daily ITCH file once and write analysis tables as Parquet (for Spark SQL / pandas).

    python -m itch_forensics.build --raw data/itch.bin --out data/parquet
"""
import argparse
import pathlib

import numpy as np
import pandas as pd

from .checks import header_frame
from .decode import decode_all
from .framing import index_messages


def _plain(df: pd.DataFrame) -> pd.DataFrame:
    """Strings instead of categoricals, signed 64-bit ints (Spark has no unsigned types)."""
    out = df.copy()
    for c in out.columns:
        if isinstance(out[c].dtype, pd.CategoricalDtype):
            out[c] = out[c].astype(str)
        elif out[c].dtype == np.uint64:
            out[c] = out[c].astype(np.int64)
    return out


def build(raw: pathlib.Path, out: pathlib.Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    buf = raw.read_bytes()
    idx = index_messages(buf)
    fr = decode_all(buf, idx)
    h = header_frame(buf, idx)
    h["seq"] = np.arange(len(h))
    h["type"] = [chr(t) for t in idx.types]
    tables = {
        "messages": h[["seq", "type", "locate", "ts_ns"]],
        "add_order": pd.concat([fr["A"].assign(mpid=None), fr["F"]], ignore_index=True).sort_values("seq"),
        "execution": pd.concat([fr["E"].assign(printable=None, price=None).assign(type="E"),
                                fr["C"].assign(type="C")], ignore_index=True).sort_values("seq"),
        "cancel": fr["X"], "delete": fr["D"], "replace": fr["U"], "trade": fr["P"],
        "cross": fr["Q"], "noii": fr["I"], "directory": fr["R"],
    }
    counts = {}
    for name, df in tables.items():
        df = _plain(df)
        if name == "execution":
            df["price"] = df["price"].astype("Int64")
        df.to_parquet(out / f"{name}.parquet", index=False)
        counts[name] = len(df)
    return counts


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    a = ap.parse_args()
    for k, v in build(a.raw, a.out).items():
        print(f"{k:<10} {v:>10,}")
