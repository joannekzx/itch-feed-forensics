"""Tests run on real bytes copied from the Nasdaq ITCH 5.0 file of 2019-12-30 (see conftest)."""
import struct
from collections import defaultdict

import pytest

from conftest import LOCATE, join, split
from itch_forensics.book import BID, ASK, build_events, replay
from itch_forensics.checks import header_frame, timestamp_regressions
from itch_forensics.decode import decode, decode_all
from itch_forensics.framing import index_messages, length_mismatches


@pytest.fixture(scope="module")
def parsed(raw):
    idx = index_messages(raw)
    return idx, decode_all(raw, idx)


def run(raw_bytes):
    idx = index_messages(raw_bytes)
    ev = build_events(decode_all(raw_bytes, idx))
    return replay(ev, [LOCATE])


# ---------------------------------------------------------------- framing
def test_framing_covers_every_byte_and_matches_spec(raw, parsed):
    idx, _ = parsed
    assert len(idx) == 2519 and idx.trailing == 0
    assert length_mismatches(idx) == []


def test_truncated_file_reports_trailing_bytes(raw):
    idx = index_messages(raw[:-5])           # cut the last message short
    assert len(idx) == 2518 and idx.trailing > 0


# ---------------------------------------------------------------- decoding
def test_known_real_values(parsed):
    _, fr = parsed
    r = fr["R"].iloc[0]
    assert (r.stock, r.locate, r.round_lot_size, r.etp_flag) == ("VIXY", LOCATE, 100, "Y")
    a = fr["A"].iloc[0]
    assert (a.ref, a.side, a.shares, a.stock, a.price) == (4404, "S", 20000, "VIXY", 125200)   # $12.52
    u = fr["U"].iloc[0]
    assert (u.ref, u.new_ref, u.shares, u.price) == (7445840, 7566796, 100, 125900)
    q = fr["Q"].iloc[0]
    assert (q.cross_type, q.shares, q.price) == ("O", 0, 0)       # no opening cross happened for this symbol


def test_numpy_decoder_matches_an_independent_struct_decoder(raw, parsed):
    """Decode add, execute and replace messages again with struct.unpack and compare every field."""
    _, fr = parsed
    expect = defaultdict(list)
    for m in split(raw):
        t = chr(m[0])
        if t == "A":
            _, loc, trk, ts_hi, ts_lo, ref, side, shares, stock, price = struct.unpack(">cHHHIQcI8sI", m)
            expect["A"].append((loc, (ts_hi << 32) | ts_lo, ref, side.decode(), shares, stock.decode().rstrip(), price))
        elif t == "E":
            _, loc, trk, ts_hi, ts_lo, ref, shares, match = struct.unpack(">cHHHIQIQ", m)
            expect["E"].append((loc, (ts_hi << 32) | ts_lo, ref, shares, match))
        elif t == "U":
            _, loc, trk, ts_hi, ts_lo, ref, new_ref, shares, price = struct.unpack(">cHHHIQQII", m)
            expect["U"].append((loc, (ts_hi << 32) | ts_lo, ref, new_ref, shares, price))
    a = fr["A"]
    assert expect["A"] == list(zip(a.locate, a.ts_ns, a.ref, a.side.astype(str), a.shares, a.stock.astype(str), a.price))
    e = fr["E"]
    assert expect["E"] == list(zip(e.locate, e.ts_ns, e.ref, e.shares, e.match))
    u = fr["U"]
    assert expect["U"] == list(zip(u.locate, u.ts_ns, u.ref, u.new_ref, u.shares, u.price))


def test_timestamps_never_go_backwards(raw, parsed):
    idx, _ = parsed
    assert timestamp_regressions(header_frame(raw, idx))["regressions"] == 0


def test_message_counts_by_type(parsed):
    _, fr = parsed
    assert {k: len(v) for k, v in fr.items()} == {
        "S": 3, "R": 1, "H": 1, "Y": 1, "L": 23, "V": 1, "K": 3, "A": 1097, "F": 72,
        "E": 55, "C": 0, "X": 111, "D": 871, "U": 153, "P": 5, "Q": 1, "I": 121}


# ---------------------------------------------------------------- order book
def test_real_stream_replays_with_no_inconsistency(raw):
    bad, _, bbo, state = run(raw)
    assert bad == {}
    assert state["live_orders"] == 254
    assert state["resting_shares_orders"] == state["resting_shares_levels"] == 148664
    assert len(bbo) == 291


def test_final_best_bid_offer_matches_a_brute_force_scan(raw, parsed):
    """Independent replay with plain dicts and a linear scan for the best prices."""
    live = {}
    for m in split(raw):
        t = chr(m[0])
        if t in "AF":
            _, _, _, _, _, ref, side, shares, _, price = struct.unpack(">cHHHIQcI8sI", m[:36])
            live[ref] = [side, price, shares]
        elif t in "EC":
            ref, shares = struct.unpack(">QI", m[11:23])
            live[ref][2] -= shares
            if live[ref][2] == 0:
                del live[ref]
        elif t == "X":
            ref, shares = struct.unpack(">QI", m[11:23])
            live[ref][2] -= shares
            if live[ref][2] == 0:
                del live[ref]
        elif t == "D":
            del live[struct.unpack(">Q", m[11:19])[0]]
        elif t == "U":
            ref, new_ref, shares, price = struct.unpack(">QQII", m[11:35])
            side = live.pop(ref)[0]
            live[new_ref] = [side, price, shares]
    best_bid = max(p for s, p, _ in live.values() if s == b"B")
    best_ask = min(p for s, p, _ in live.values() if s == b"S")
    _, _, bbo, _ = run(raw)
    last = bbo.iloc[-1]
    assert (last.bid, last.ask) == (best_bid, best_ask)


# ------------------------------------------- the audit must be able to fail
def test_missing_add_is_reported_as_unknown_reference(raw):
    msgs = split(raw)
    first_delete = next(m for m in msgs if chr(m[0]) == "D")
    ref = int.from_bytes(first_delete[11:19], "big")
    kept = [m for m in msgs if not (chr(m[0]) in "AF" and int.from_bytes(m[11:19], "big") == ref)]
    bad, examples, *_ = run(join(kept))
    assert bad.get("delete_unknown_ref", 0) >= 1
    assert any(ex[1] == ref for ex in examples["delete_unknown_ref"])


def test_duplicated_add_is_reported(raw):
    msgs = split(raw)
    i = next(i for i, m in enumerate(msgs) if chr(m[0]) == "A")
    bad, *_ = run(join(msgs[:i + 1] + [msgs[i]] + msgs[i + 1:]))
    assert bad == {"add_duplicate_ref": 1}


def test_cancel_larger_than_remaining_is_reported(raw):
    msgs = split(raw)
    i = next(i for i, m in enumerate(msgs) if chr(m[0]) == "X")
    m = bytearray(msgs[i])
    m[19:23] = (2_000_000_000).to_bytes(4, "big")
    msgs[i] = bytes(m)
    bad, *_ = run(join(msgs))
    assert bad.get("cancel_over_remaining", 0) == 1


def test_wrong_stock_locate_is_reported(raw):
    msgs = split(raw)
    i = next(i for i, m in enumerate(msgs) if chr(m[0]) == "D")
    m = bytearray(msgs[i])
    m[1:3] = (LOCATE + 1).to_bytes(2, "big")
    msgs[i] = bytes(m)
    bad, *_ = run(join(msgs))
    assert bad.get("locate_mismatch", 0) == 1


def test_timestamp_regression_is_detected(raw):
    msgs = split(raw)
    m = bytearray(msgs[500])
    m[5:11] = (1).to_bytes(6, "big")                 # one nanosecond after midnight
    msgs[500] = bytes(m)
    stream = join(msgs)
    assert timestamp_regressions(header_frame(stream, index_messages(stream)))["regressions"] == 1       # one step back; the step after it goes forward again


def test_wrong_message_length_is_flagged(raw):
    msgs = split(raw)
    msgs[600] = msgs[600] + b"\x00"                  # one stray byte on one message
    idx = index_messages(join(msgs))
    assert length_mismatches(idx) == [(chr(msgs[600][0]), len(msgs[600]), 1, f"spec says {len(msgs[600]) - 1}")]
