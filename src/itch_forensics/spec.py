"""ITCH 5.0 message layouts (Nasdaq TotalView-ITCH 5.0 specification).

Offsets are from the first byte of the message body, i.e. the message-type byte, after the
2-byte big-endian length prefix that frames each message in the daily file. Every message
starts with the same 11-byte header: type (1), stock locate (2), tracking number (2) and a
48-bit timestamp in nanoseconds since midnight (6).

Field kinds: u2/u4/u6/u8 = big-endian unsigned integer of that many bytes, c = one ASCII
character, a<N> = N-character space-padded ASCII string.
"""

HEADER = [("locate", "u2", 1), ("tracking", "u2", 3), ("ts_ns", "u6", 5)]

LAYOUTS = {
    "S": (12, "system_event", [("event_code", "c", 11)]),
    "R": (39, "stock_directory", [
        ("stock", "a8", 11), ("market_category", "c", 19), ("financial_status", "c", 20),
        ("round_lot_size", "u4", 21), ("round_lots_only", "c", 25), ("issue_class", "c", 26),
        ("issue_subtype", "a2", 27), ("authenticity", "c", 29), ("short_sale_threshold", "c", 30),
        ("ipo_flag", "c", 31), ("luld_tier", "c", 32), ("etp_flag", "c", 33),
        ("etp_leverage", "u4", 34), ("inverse", "c", 38)]),
    "H": (25, "trading_action", [
        ("stock", "a8", 11), ("trading_state", "c", 19), ("reserved", "c", 20), ("reason", "a4", 21)]),
    "Y": (20, "reg_sho", [("stock", "a8", 11), ("action", "c", 19)]),
    "L": (26, "participant_position", [
        ("mpid", "a4", 11), ("stock", "a8", 15), ("primary", "c", 23), ("mode", "c", 24), ("state", "c", 25)]),
    "V": (35, "mwcb_levels", [("level1", "u8", 11), ("level2", "u8", 19), ("level3", "u8", 27)]),
    "K": (28, "ipo_quoting", [
        ("stock", "a8", 11), ("release_time", "u4", 19), ("qualifier", "c", 23), ("price", "u4", 24)]),
    "A": (36, "add_order", [
        ("ref", "u8", 11), ("side", "c", 19), ("shares", "u4", 20), ("stock", "a8", 24), ("price", "u4", 32)]),
    "F": (40, "add_order_mpid", [
        ("ref", "u8", 11), ("side", "c", 19), ("shares", "u4", 20), ("stock", "a8", 24),
        ("price", "u4", 32), ("mpid", "a4", 36)]),
    "E": (31, "order_executed", [("ref", "u8", 11), ("shares", "u4", 19), ("match", "u8", 23)]),
    "C": (36, "order_executed_price", [
        ("ref", "u8", 11), ("shares", "u4", 19), ("match", "u8", 23), ("printable", "c", 31), ("price", "u4", 32)]),
    "X": (23, "order_cancel", [("ref", "u8", 11), ("cancelled", "u4", 19)]),
    "D": (19, "order_delete", [("ref", "u8", 11)]),
    "U": (35, "order_replace", [
        ("ref", "u8", 11), ("new_ref", "u8", 19), ("shares", "u4", 27), ("price", "u4", 31)]),
    "P": (44, "trade", [
        ("ref", "u8", 11), ("side", "c", 19), ("shares", "u4", 20), ("stock", "a8", 24),
        ("price", "u4", 32), ("match", "u8", 36)]),
    "Q": (40, "cross_trade", [
        ("shares", "u8", 11), ("stock", "a8", 19), ("price", "u4", 27), ("match", "u8", 31), ("cross_type", "c", 39)]),
    "I": (50, "noii", [
        ("paired", "u8", 11), ("imbalance", "u8", 19), ("direction", "c", 27), ("stock", "a8", 28),
        ("far_price", "u4", 36), ("near_price", "u4", 40), ("ref_price", "u4", 44),
        ("cross_type", "c", 48), ("price_variation", "c", 49)]),
}

EXPECTED_LENGTH = {t: spec[0] for t, spec in LAYOUTS.items()}
NAME = {t: spec[1] for t, spec in LAYOUTS.items()}
PRICE_SCALE = 10_000          # prices are integers with 4 implied decimals
