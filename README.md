# itch-feed-forensics

Parse a **real Nasdaq TotalView-ITCH 5.0 feed**, rebuild the limit order book from it, and audit the feed: is it internally consistent, where does the volume go, how do orders live and die, and how well does the opening-cross indication predict the opening cross?

Data: Nasdaq's published sample ITCH file for **30 December 2019**, first 300 MB: 8,733,895 messages from the start of the feed through **09:30:36** (pre-market, the opening cross, and 36 seconds of regular trading). It is not a full day, and every number below is about that window.

Stack: Python (numpy, pandas), Spark SQL, Parquet, pytest, Jupyter.

## Read in this order

| Notebook | What it shows |
|---|---|
| [01 · Feed and framing](notebooks/01_feed_and_framing.ipynb) | The binary format, message mix, every length checked against the spec, a field-by-field comparison with an independent `struct.unpack` parse, the session timeline, message volume. |
| [02 · Order book](notebooks/02_order_book.ipynb) | Full replay of 7.4M order events, reconciliation checks, four deliberate corruptions that the audit must catch, top-of-book state, stub quotes. |
| [03 · Findings](notebooks/03_findings.ipynb) | Spark SQL audits: message rate, order-to-trade ratio, order lifetimes; the opening cross versus the indicative price; shared match numbers; reference-data consistency. |

## Findings (from the executed notebooks)

- **The format parses exactly.** All 8.7M messages have the length the ITCH 5.0 spec gives for their type, and timestamps never decrease. A second parse with plain `struct.unpack` of 3.76M add / execute / replace messages agrees with the numpy decoder on every field compared.
- **The order stream is internally consistent.** Replaying 7,397,062 add / execute / cancel / delete / replace events from an empty book found no unknown order references, duplicates, over-sized executions or cancels, or stock-locate mismatches. At the end of the sample 1,538,626 orders are live and resting shares total 879,432,660 whether summed by order or by price level. The check can fail: dropping one add, duplicating one add, oversizing one cancel, or changing one locate each triggers the matching counter (notebook 02 and `tests/`).
- **The open is a burst.** The second starting 09:30:00 carries 527,644 messages, 3,068 times the median pre-market second (172).
- **Most displayed orders do not trade.** Of 3.93M orders added, 57.1% were later deleted, 6.4% replaced, 0.8% executed (and not deleted or replaced), and 35.8% were still resting at the cut. Across the 401 symbols with at least 1,000 adds and one execution, the median is about 200 order-entry messages per execution (pre-market is thin, so this describes this window, not a full day).
- **Orders are short-lived.** 28% of deleted orders were deleted within 100 ms of being added; 9.5% lived over a minute.
- **The book is never locked or crossed** for the 25 busiest symbols, before or after the open; the only abnormal state is one empty side (0.4% of pre-market time).
- **Stub quotes distort the touch.** 808 sell orders at $100,000 or more across 34 symbols (158 at exactly $199,999.98) and 4,082 buy orders at $0.01 or less. Spreads and mid prices need these filtered.
- **The opening cross is not quite what the indication said.** Nasdaq publishes an indicative cross price once a second from 09:28:00. Of the 3,086 symbols that traded in the cross (5,820 of 8,906 had no cross), the realised price equals the last indicative price for 75.5%, and the shares equal the last indicative paired shares for 62.0%. When the price differs it moved by a median 0.08% (max 3.1%). The last indication is up to 1.6 s old, so this is the book moving, not an error.
- **A match number is not a unique key.** Three match numbers appear on two execution messages each: always one `E` and one `C`, same nanosecond, same stock, 100 shares, different order references. That fits a trade between two displayed orders reported once per side; the sample cannot prove it. Use `(match, ref)`.

## Caveats

- 36 seconds of regular trading is not a trading day. Closing-cross, end-of-day volume and cancel behaviour after the open are out of scope.
- One day, one venue. A single venue's book is not the national best bid and offer, so "never crossed" says nothing about locks across venues.
- Order-to-trade ratios are computed on a window dominated by pre-market quoting.
- Interpretations are marked as such; the data shows the pattern, not the intent.
- The order book replay is plain Python (about 300k events/s here). It is written for clarity and auditing, not latency.

## Data

Not included (Nasdaq sample data): `12302019.NASDAQ_ITCH50.gz` from <https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/>. The notebooks use the first 300 MB, decompressed:

```bash
mkdir -p data
gunzip -c 12302019.NASDAQ_ITCH50.gz | head -c 300000000 > data/itch.bin   # the cut lands mid-message; the parser reports the 34 trailing bytes
```

`tests/fixtures/real_one_symbol.itch.gz` is a 25 KB excerpt of the same file (every message for one symbol plus the system events), kept so the tests run without the big file.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[notebooks,test]"          # Spark SQL needs Java and pyspark
python -m itch_forensics.build --raw data/itch.bin --out data/parquet   # ~20 s, 10 Parquet tables
jupyter lab notebooks/
pytest                                       # 14 tests on real bytes
```

## Layout

```
src/itch_forensics/   spec.py (message layouts), framing.py, decode.py, book.py, checks.py, build.py, spark.py
sql/                  01_message_rate, 02_order_to_trade, 03_order_lifetime
notebooks/            01-03, committed with outputs
tests/                14 tests; fixture is real bytes
```

## Design notes

- **Framing is sequential, decoding is vectorised.** Message n+1 starts where message n ends, so one pass reads the length prefixes (2.8 s for 8.7M messages). Decoding then gathers all messages of a type into a byte matrix and slices fields as columns, driven by one layout table in `spec.py`.
- **The replay counts, it does not repair.** Each inconsistency increments a named counter and keeps the first few examples. Message lengths for every type were checked against the spec on real data; the fields of add, execute and replace messages were additionally checked against an independent parse.
- **Top of book uses lazy heaps** for the monitored symbols only; the final best bid/offer is verified against a brute-force scan in the tests.
