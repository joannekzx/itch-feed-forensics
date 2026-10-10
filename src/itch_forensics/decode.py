"""Vectorised decoding: gather all messages of one type into a byte matrix, slice fields as columns."""
import numpy as np
import pandas as pd

from .framing import Index
from .spec import HEADER, LAYOUTS


def _uint(block: np.ndarray, size: int) -> np.ndarray:
    """Big-endian unsigned integer of 1-8 bytes from an (n, size) uint8 matrix."""
    padded = np.zeros((len(block), 8), dtype=np.uint8)
    padded[:, 8 - size:] = block
    return padded.view(">u8").ravel().astype(np.uint64)


def _column(M: np.ndarray, kind: str, off: int):
    if kind[0] == "u":
        size = int(kind[1:])
        v = _uint(M[:, off:off + size], size)
        return v.astype(np.int64) if size < 8 else v
    if kind == "c":
        raw = np.ascontiguousarray(M[:, off:off + 1]).view("S1").ravel()
        uniq, inv = np.unique(raw, return_inverse=True)
        return pd.Categorical.from_codes(inv, categories=[u.decode("ascii") for u in uniq])
    size = int(kind[1:])                                      # a<N>: space-padded string
    raw = np.ascontiguousarray(M[:, off:off + size]).view(f"S{size}").ravel()
    uniq, inv = np.unique(raw, return_inverse=True)
    labels = [u.decode("ascii").rstrip() for u in uniq]
    return pd.Categorical.from_codes(inv, categories=pd.Index(labels).unique()) \
        if len(set(labels)) == len(labels) else pd.Series(labels)[inv].reset_index(drop=True)


def decode(buf, idx: Index, msg_type: str) -> pd.DataFrame:
    """All messages of one type as a DataFrame. `seq` is the message's position in the file."""
    length, _, fields = LAYOUTS[msg_type]
    arr = np.frombuffer(buf, dtype=np.uint8)
    sel = np.flatnonzero((idx.types == ord(msg_type)) & (idx.lengths == length))
    M = arr[idx.offsets[sel][:, None] + np.arange(length)]
    cols = {"seq": sel}
    for name, kind, off in HEADER + fields:
        cols[name] = _column(M, kind, off)
    return pd.DataFrame(cols)


def decode_all(buf, idx: Index, types=None) -> dict:
    # every requested type is returned, as an empty table when the file has none of it
    return {t: decode(buf, idx, t) for t in (types or LAYOUTS)}
