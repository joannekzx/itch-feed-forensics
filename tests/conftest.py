import gzip
import pathlib

import pytest

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "real_one_symbol.itch.gz"
LOCATE = 8382      # VIXY: every real message for this symbol on 2019-12-30, plus the 3 system events


@pytest.fixture(scope="session")
def raw() -> bytes:
    return gzip.decompress(FIXTURE.read_bytes())


def split(buf: bytes):
    """Independent framing, in plain Python, for building mutated streams."""
    out, pos = [], 0
    while pos + 2 <= len(buf):
        ln = int.from_bytes(buf[pos:pos + 2], "big")
        out.append(buf[pos + 2:pos + 2 + ln])
        pos += 2 + ln
    return out


def join(msgs) -> bytes:
    return b"".join(len(m).to_bytes(2, "big") + m for m in msgs)
