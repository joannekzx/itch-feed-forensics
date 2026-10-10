"""Compare this package's parse and replay with an independent implementation: itch-book (MIT, C++ core).

    pip install -e ".[validate]"
    python -m itch_forensics.crosscheck --raw data/itch.bin --date 2019-12-30

Both read the same bytes. itch-book refuses a file that ends mid-message, so the cut is trimmed to a whole
message first (that trims the same trailing bytes this package reports). The comparison covers every order
message field by field, the remaining shares after every execute / cancel, trades by match number, and the
best bid and offer for the monitored symbols. itch-book numbers messages from 1 and this package from 0.
"""
import argparse
import datetime as dt
import pathlib
import tempfile

import numpy as np
import pandas as pd

from .book import build_events, replay
from .decode import decode_all
from .framing import index_messages


def trimmed_copy(raw: pathlib.Path, out: pathlib.Path) -> int:
    buf = raw.read_bytes()
    idx = index_messages(buf)
    out.write_bytes(buf[:len(buf) - idx.trailing])
    return idx.trailing


def read_itch_book(path, date):
    import itch_book as ib
    M, T, B = [], [], []
    feed = ib.open(str(path), date=date, price_type="fixed")
    for b in feed.batches(tables=("messages", "trades", "bbo"), rows=2_000_000):
        M.append(pd.DataFrame(b.messages)); T.append(pd.DataFrame(b.trades)); B.append(pd.DataFrame(b.bbo))
    stats = dict(feed.stats or {})
    feed.close()
    M, T, B = (pd.concat(x, ignore_index=True) for x in (M, T, B))
    for df, cols in ((M, ("type", "side")), (T, ("kind", "cross_type"))):
        for c in cols:
            df[c] = df[c].str.decode("ascii")
    for df in (M, T, B):
        df["seq0"] = df["seq"].astype(np.int64) - 1
    return M, T, B, stats


def compare(buf: bytes, M, T, B, monitored) -> pd.DataFrame:
    idx = index_messages(buf)
    fr = decode_all(buf, idx)
    ev = build_events(fr)
    _, _, bbo_ours, _ = replay(ev, monitored)
    rows = []

    def check(name, a, b):
        ok = np.asarray(a) == np.asarray(b)
        rows.append((name, int(ok.sum()), len(ok)))

    Mi = M.set_index("seq0")

    def joined(t, ours):
        j = ours.set_index("seq").join(Mi[Mi.type == t], rsuffix="_t", how="outer")
        assert j.locate_t.notna().all() and j.locate.notna().all(), f"{t}: messages present on one side only"
        return j

    for t in "AF":
        j = joined(t, fr[t])
        check(f"{t} add: stock locate", j.locate, j.locate_t)
        check(f"{t} add: order reference", j.ref.astype("uint64"), j.order_id)
        check(f"{t} add: side", j.side.astype(str), j.side_t)
        check(f"{t} add: shares", j.shares, j["size"])
        check(f"{t} add: price", j.price, j.price_t)
    j = joined("E", fr["E"])
    check("E execute: order reference", j.ref.astype("uint64"), j.order_id)
    check("E execute: shares", j.shares, j["size"])
    j = joined("C", fr["C"])
    check("C execute at price: order reference", j.ref.astype("uint64"), j.order_id)
    check("C execute at price: shares", j.shares, j["size"])
    check("C execute at price: price", j.price, j.price_t)
    j = joined("X", fr["X"])
    check("X cancel: order reference", j.ref.astype("uint64"), j.order_id)
    check("X cancel: shares", j.cancelled, j["size"])
    j = joined("D", fr["D"])
    check("D delete: order reference", j.ref.astype("uint64"), j.order_id)
    j = joined("U", fr["U"])
    check("U replace: old reference", j.ref.astype("uint64"), j.old_order_id)
    check("U replace: new reference", j.new_ref.astype("uint64"), j.order_id)
    check("U replace: shares", j.shares, j["size"])
    check("U replace: price", j.price, j.price_t)

    # remaining shares after each execute / cancel: computed here from the messages, compared with itch-book's
    starts = pd.concat([fr["A"][["ref", "shares"]], fr["F"][["ref", "shares"]],
                        fr["U"][["new_ref", "shares"]].rename(columns={"new_ref": "ref"})]).rename(columns={"shares": "start"})
    dec = pd.concat([fr["E"][["seq", "ref", "shares"]], fr["C"][["seq", "ref", "shares"]],
                     fr["X"][["seq", "ref", "cancelled"]].rename(columns={"cancelled": "shares"})]).sort_values("seq")
    dec["cum"] = dec.groupby("ref").shares.cumsum()
    dec = dec.merge(starts.assign(ref=starts.ref.astype("uint64")).astype({"ref": dec.ref.dtype}), on="ref", how="left")
    check("remaining shares after every E / C / X", (dec.start - dec.cum).values, Mi.loc[dec.seq.values, "remaining"].values)

    # trades by match number (itch-book reports a C execution only when it is printable)
    printable = fr["C"][fr["C"].printable.astype(str) == "Y"]
    for kind, ours in (("E", fr["E"]), ("P", fr["P"]), ("Q", fr["Q"]), ("C", printable)):
        t = T[T.kind == kind]
        j = ours.assign(match=ours.match.astype("uint64")).merge(t, left_on="match", right_on="match_number",
                                                                how="outer", indicator=True, suffixes=("", "_t"))
        rows.append((f"{kind} trades: same set of match numbers", int((j._merge == "both").sum()), len(j)))
        j = j[j._merge == "both"]
        check(f"{kind} trades: size", j.shares, j["size"])
        if kind != "E":
            check(f"{kind} trades: price", j.price, j.price_t)

    # best bid and offer, collapsed to price changes (itch-book also emits rows for size-only changes)
    Bm = B[B.locate.isin(monitored)].sort_values(["locate", "seq0"]).copy()
    Bm["bid"], Bm["ask"] = Bm.bid_px.where(Bm.bid_px > 0), Bm.ask_px.where(Bm.ask_px > 0)
    prev = Bm.groupby("locate")[["bid", "ask"]].shift()
    first = ~Bm.duplicated("locate", keep="first")
    differs = prev.bid.fillna(-1).ne(Bm.bid.fillna(-1)) | prev.ask.fillna(-1).ne(Bm.ask.fillna(-1))
    Bc = Bm[first | differs].reset_index(drop=True)
    O = bbo_ours.sort_values(["locate", "seq"]).reset_index(drop=True)
    if len(Bc) == len(O):
        same = (Bc.locate.values == O.locate.values) & (Bc.seq0.values == O.seq.values) \
            & (np.nan_to_num(Bc.bid.values, nan=-1) == np.nan_to_num(O.bid.astype(float).values, nan=-1)) \
            & (np.nan_to_num(Bc.ask.values, nan=-1) == np.nan_to_num(O.ask.astype(float).values, nan=-1))
        rows.append((f"best bid/offer changes for {len(monitored)} symbols: same sequence, bid and ask", int(same.sum()), len(O)))
    else:
        rows.append((f"best bid/offer changes for {len(monitored)} symbols: same number of rows", len(O), len(Bc)))
    out = pd.DataFrame(rows, columns=["check", "agree", "of"])
    out["all agree"] = out.agree == out["of"]
    return out


def run(raw: pathlib.Path, date: dt.date, n_monitored=25) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as d:
        whole = pathlib.Path(d) / "whole_messages.bin"
        trailing = trimmed_copy(raw, whole)
        buf = whole.read_bytes()
        ev = build_events(decode_all(buf, index_messages(buf)))
        monitored = ev.groupby("locate").size().nlargest(n_monitored).index.tolist()
        M, T, B, _ = read_itch_book(whole, date)
    res = compare(buf, M, T, B, monitored)
    res.attrs["trailing_bytes_trimmed"] = trailing
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, type=pathlib.Path)
    ap.add_argument("--date", required=True, type=dt.date.fromisoformat)
    a = ap.parse_args()
    r = run(a.raw, a.date)
    print(r.to_string(index=False))
    raise SystemExit(0 if r["all agree"].all() else 1)
