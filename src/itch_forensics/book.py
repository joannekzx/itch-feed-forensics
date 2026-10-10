"""Rebuild the limit order book from add / execute / cancel / delete / replace messages.

The daily file starts at the beginning of the day, so the book starts empty and every order
that is later executed, cancelled, deleted or replaced must already be live. The replay does
not assume that: each inconsistency is counted (and the first few kept as examples) instead
of being papered over, because those counts are the audit.
"""
import heapq
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

ANOMALIES = ("add_duplicate_ref", "exec_unknown_ref", "cancel_unknown_ref", "delete_unknown_ref",
             "replace_unknown_ref", "replace_new_ref_collision", "exec_over_remaining",
             "cancel_over_remaining", "locate_mismatch", "negative_level")
KIND = {"A": 0, "F": 0, "E": 1, "C": 2, "X": 3, "D": 4, "U": 5}
BID, ASK = 0, 1


def build_events(frames: dict) -> pd.DataFrame:
    """Order-affecting messages from all types, in file order, as one table."""
    parts = []
    for t, kind in KIND.items():
        df = frames.get(t)
        if df is None:
            continue
        n = len(df)
        parts.append(pd.DataFrame({
            "seq": df["seq"].to_numpy(),
            "ts_ns": df["ts_ns"].to_numpy(),
            "kind": np.full(n, kind, dtype=np.int8),
            "type": np.full(n, ord(t), dtype=np.int8),
            "locate": df["locate"].to_numpy(),
            "ref": df["ref"].to_numpy(),
            "new_ref": df["new_ref"].to_numpy() if "new_ref" in df else np.zeros(n, dtype=np.uint64),
            "side": (df["side"].to_numpy().astype(str) == "S").astype(np.int8) if "side" in df else -1,
            "shares": (df["shares"] if "shares" in df else df["cancelled"] if "cancelled" in df
                       else pd.Series(np.zeros(n, dtype=np.int64))).to_numpy().astype(np.int64),
            "price": df["price"].to_numpy().astype(np.int64) if "price" in df else 0,
        }))
    return pd.concat(parts, ignore_index=True).sort_values("seq", kind="stable").reset_index(drop=True)


def replay(events: pd.DataFrame, monitored=(), keep_examples=5):
    """Replay the events. Returns (anomaly counters, examples, BBO-change table, final state)."""
    monitored = set(int(m) for m in monitored)
    orders = {}                       # ref -> [locate, side, price, shares]
    levels = defaultdict(int)         # (locate, side, price) packed -> resting shares
    heaps = {}                        # (locate, side) -> heap of candidate best prices (monitored only)
    last_bbo = {}
    bbo = []                          # (seq, ts_ns, locate, bid, ask) whenever a monitored top of book changes
    bad = Counter()
    examples = defaultdict(list)
    executed = defaultdict(int)       # locate -> shares executed against displayed orders

    def note(name, row):
        bad[name] += 1
        if len(examples[name]) < keep_examples:
            examples[name].append(row)

    def key(loc, side, price):
        return (((loc << 1) | side) << 32) | price

    def add_level(loc, side, price, shares):
        k = key(loc, side, price)
        if levels[k] == 0 and loc in monitored:
            heapq.heappush(heaps.setdefault((loc, side), []), -price if side == BID else price)
        levels[k] += shares

    def sub_level(loc, side, price, shares):
        k = key(loc, side, price)
        left = levels[k] - shares
        if left < 0:
            bad["negative_level"] += 1
            left = 0
        if left == 0:
            del levels[k]
        else:
            levels[k] = left

    def top(loc, side):
        h = heaps.get((loc, side))
        while h:
            p = -h[0] if side == BID else h[0]
            if levels.get(key(loc, side, p), 0) > 0:
                return p
            heapq.heappop(h)
        return None

    cols = [events[c].tolist() for c in ("seq", "ts_ns", "kind", "locate", "ref", "new_ref", "side", "shares", "price")]
    for seq, ts, kind, loc, ref, new_ref, side, shares, price in zip(*cols):
        touched = loc
        if kind == 0:                                           # add (A or F)
            if ref in orders:
                note("add_duplicate_ref", (seq, ref))
                continue
            orders[ref] = [loc, side, price, shares]
            add_level(loc, side, price, shares)
        elif kind in (1, 2, 3):                                 # execute (E, C) / partial cancel (X)
            o = orders.get(ref)
            if o is None:
                note({1: "exec_unknown_ref", 2: "exec_unknown_ref", 3: "cancel_unknown_ref"}[kind], (seq, ref))
                continue
            if o[0] != loc:
                note("locate_mismatch", (seq, ref, o[0], loc))
            if shares > o[3]:
                note("exec_over_remaining" if kind != 3 else "cancel_over_remaining", (seq, ref, shares, o[3]))
                shares = o[3]
            sub_level(o[0], o[1], o[2], shares)
            o[3] -= shares
            if kind != 3:
                executed[o[0]] += shares
            if o[3] == 0:
                del orders[ref]
        elif kind == 4:                                         # delete
            o = orders.pop(ref, None)
            if o is None:
                note("delete_unknown_ref", (seq, ref))
                continue
            if o[0] != loc:
                note("locate_mismatch", (seq, ref, o[0], loc))
            sub_level(o[0], o[1], o[2], o[3])
        else:                                                   # replace: delete the old, add a new order
            o = orders.pop(ref, None)
            if o is None:
                note("replace_unknown_ref", (seq, ref))
                continue
            if o[0] != loc:
                note("locate_mismatch", (seq, ref, o[0], loc))
            sub_level(o[0], o[1], o[2], o[3])
            if new_ref in orders:
                note("replace_new_ref_collision", (seq, new_ref))
                continue
            orders[new_ref] = [o[0], o[1], price, shares]
            add_level(o[0], o[1], price, shares)
        if touched in monitored:
            now = (top(touched, BID), top(touched, ASK))
            if last_bbo.get(touched) != now:
                last_bbo[touched] = now
                bbo.append((seq, ts, touched, now[0], now[1]))

    book_total = sum(levels.values())
    order_total = sum(o[3] for o in orders.values())
    live = defaultdict(lambda: [0, 0])
    for o in orders.values():
        live[o[0]][0] += 1
        live[o[0]][1] += o[3]
    by_locate = pd.DataFrame([(loc, n, s) for loc, (n, s) in live.items()],
                             columns=["locate", "live_orders", "resting_shares"])
    state = {"live_orders": len(orders), "resting_shares_orders": order_total,
             "resting_shares_levels": book_total, "levels": len(levels),
             "executed_by_locate": dict(executed), "by_locate": by_locate}
    bbo_df = pd.DataFrame(bbo, columns=["seq", "ts_ns", "locate", "bid", "ask"])
    return dict(bad), dict(examples), bbo_df, state
