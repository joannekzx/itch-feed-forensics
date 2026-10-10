"""Feed-level audits that do not need the order book."""
import numpy as np
import pandas as pd

from .framing import Index

OPEN_NS = int(9.5 * 3600 * 1e9)       # 09:30:00, regular-hours open


def header_frame(buf, idx: Index) -> pd.DataFrame:
    """seq, type, locate and timestamp for every message (the 11-byte header is common to all)."""
    arr = np.frombuffer(buf, dtype=np.uint8)
    M = arr[idx.offsets[:, None] + np.arange(11)]
    locate = (M[:, 1].astype(np.int64) << 8) | M[:, 2]
    ts = np.zeros(len(M), dtype=np.int64)
    for i in range(5, 11):
        ts = (ts << 8) | M[:, i]
    return pd.DataFrame({"type": idx.types, "locate": locate, "ts_ns": ts})


def timestamp_regressions(h: pd.DataFrame) -> dict:
    d = np.diff(h["ts_ns"].to_numpy())
    back = d < 0
    return {"messages": len(h), "regressions": int(back.sum()), "largest_step_back_ns": int(-d.min()) if back.any() else 0,
            "same_timestamp_pairs": int((d == 0).sum())}


def directory_mismatches(frames: dict) -> pd.DataFrame:
    """Symbols carried in add/trade messages versus the stock directory entry for the same locate."""
    dirmap = frames["R"].set_index("locate")["stock"].astype(str)
    rows = []
    for t in ("A", "F", "P"):
        df = frames.get(t)
        if df is None:
            continue
        expected = df["locate"].map(dirmap)
        bad = df["stock"].astype(str) != expected
        rows.append({"type": t, "messages": len(df), "symbol_mismatch": int(bad.sum()),
                     "locate_not_in_directory": int(expected.isna().sum())})
    return pd.DataFrame(rows)


def match_number_duplicates(frames: dict) -> dict:
    parts = [frames[t][["match"]].assign(type=t) for t in ("E", "C", "P", "Q") if t in frames]
    m = pd.concat(parts, ignore_index=True)
    dup = m["match"].duplicated(keep=False)
    return {"matched_messages": len(m), "distinct_match_numbers": int(m["match"].nunique()),
            "messages_sharing_a_match_number": int(dup.sum()),
            "by_type_of_duplicates": m[dup]["type"].value_counts().to_dict()}


def top_of_book_state(bbo: pd.DataFrame, end_ns: int) -> pd.DataFrame:
    """Time spent in each top-of-book state per monitored symbol, split at the 09:30 open."""
    b = bbo.sort_values(["locate", "seq"]).copy()
    b["until"] = b.groupby("locate")["ts_ns"].shift(-1).fillna(end_ns).astype(np.int64)
    one_sided = b["bid"].isna() | b["ask"].isna()
    state = np.where(one_sided, "one-sided", np.where(b["bid"] > b["ask"], "crossed",
                     np.where(b["bid"] == b["ask"], "locked", "normal")))
    b["state"] = state
    out = []
    for phase, lo, hi in (("pre-market", 0, OPEN_NS), ("regular hours", OPEN_NS, 1 << 62)):
        dur = (np.minimum(b["until"], hi) - np.maximum(b["ts_ns"], lo)).clip(lower=0)
        out.append(b.assign(phase=phase, dur_s=dur / 1e9))
    return pd.concat(out).groupby(["phase", "state"])["dur_s"].sum().unstack(fill_value=0)
