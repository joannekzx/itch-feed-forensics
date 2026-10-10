"""Split the daily file into messages. Each message is a 2-byte big-endian length, then the body."""
from dataclasses import dataclass

import numpy as np

from .spec import EXPECTED_LENGTH


@dataclass
class Index:
    offsets: np.ndarray      # position of each message body (its type byte) in the buffer
    lengths: np.ndarray      # body length, from the length prefix
    types: np.ndarray        # message-type byte as uint8
    trailing: int            # bytes left at the end that do not form a whole message

    def __len__(self):
        return len(self.offsets)


def index_messages(buf) -> Index:
    """One sequential pass: the position of message n+1 depends on the length of message n."""
    n = len(buf)
    offs, lens = [], []
    pos = 0
    while pos + 2 <= n:
        ln = (buf[pos] << 8) | buf[pos + 1]
        if ln == 0 or pos + 2 + ln > n:
            break
        offs.append(pos + 2)
        lens.append(ln)
        pos += 2 + ln
    offsets = np.asarray(offs, dtype=np.int64)
    arr = np.frombuffer(buf, dtype=np.uint8)
    return Index(offsets, np.asarray(lens, dtype=np.int32), arr[offsets], n - pos)


def length_mismatches(idx: Index):
    """(type, observed length, count) for every message whose length differs from the spec."""
    rows = []
    for t in np.unique(idx.types):
        ch = chr(t)
        expected = EXPECTED_LENGTH.get(ch)
        sel = idx.types == t
        if expected is None:
            for ln, c in zip(*np.unique(idx.lengths[sel], return_counts=True)):
                rows.append((ch, int(ln), int(c), "unknown type"))
        else:
            bad = sel & (idx.lengths != expected)
            for ln, c in zip(*np.unique(idx.lengths[bad], return_counts=True)):
                rows.append((ch, int(ln), int(c), f"spec says {expected}"))
    return rows
