# src/bronze/orderbook.py  (DROP-IN)

import asyncio
import logging
import time
from collections import deque
from typing import Deque, Dict, Optional, Tuple, List, Any

import httpx


# ---------- Exceptions ----------

class GapError(Exception):
    """Raised when depth update continuity is violated (U/u/pu checks)."""
    pass


# ---------- Local in-memory orderbook state (raw-only) ----------

class LocalBook:
    """
    Minimal in-memory order book that applies Binance depth deltas and
    exposes a RAW snapshot (no derived features). Keep this hot path tiny.
    """
    def __init__(self):
        # Raw maps as strings exactly like Binance (price_str -> qty_str)
        self.bids: Dict[str, str] = {}
        self.asks: Dict[str, str] = {}
        # Last applied update id (u)
        self.last_u: Optional[int] = None
        # Wall-clock of last applied delta (ns) for age/freshness
        self.last_ts_ns: Optional[int] = None
        # Count continuity issues encountered since init
        self.gap_count: int = 0

    def apply_deltas(self, ev: dict):
        """
        Apply a single depth update event with continuity checks.
        Expected fields:
          U: first update id in event
          u: last update id in event
          pu: optional previous last update id
          b: list[[price_str, qty_str], ...] bid deltas
          a: list[[price_str, qty_str], ...] ask deltas
          ts_recv_ns: optional local receive time (ns) for freshness
        """
        if "U" not in ev or "u" not in ev:
            self.gap_count += 1
            raise GapError("missing U/u in event")
        U = int(ev["U"]); u = int(ev["u"])
        pu = ev.get("pu")

        if self.last_u is None:
            raise RuntimeError("book not initialized with a snapshot")

        if pu is not None:
            if int(pu) != self.last_u:
                self.gap_count += 1
                raise GapError(f"pu {pu} != last_u {self.last_u}")
        else:
            # classic continuity check
            if not (U <= self.last_u + 1 <= u):
                self.gap_count += 1
                raise GapError(f"continuity fail U={U} u={u} last={self.last_u}")

        # Apply bid deltas
        for p, q in ev.get("b", []):
            if q == "0" or q == "0.00000000":
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        # Apply ask deltas
        for p, q in ev.get("a", []):
            if q == "0" or q == "0.00000000":
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self.last_u = u
        self.last_ts_ns = int(ev.get("ts_recv_ns") or time.time_ns())

    def snapshot(self, symbol: str, top_n: Optional[int] = None) -> dict:
        """
        Return a RAW snapshot (no derived fields): {symbol, last_u, bids, asks}.
        - bids/asks: [[price_str, qty_str], ...] possibly trimmed to top_n by price.
        """
        bids = self._top_n_side(self.bids, top_n, reverse=True)
        asks = self._top_n_side(self.asks, top_n, reverse=False)
        return {
            "symbol": symbol,
            "last_u": self.last_u,
            "bids": bids,
            "asks": asks,
        }

    def _top_n_side(self, side: Dict[str, str], n: Optional[int], reverse: bool) -> List[List[str]]:
        if n is None:
            # preserve current dict order (insertion) when no sorting requested
            return [[p, q] for p, q in side.items()]
        # sort numerically by price (keep string values)
        levels = sorted(((float(p), p, q) for p, q in side.items()),
                        key=lambda t: t[0], reverse=reverse)[:n]
        return [[p_str, q] for _, p_str, q in levels]


# ---------- REST snapshot (bootstrap/resync) ----------

async def fetch_snapshot(symbol: str, limit: int = 1000, retries: int = 5) -> dict:
    """
    Fetch a RAW order book snapshot from Binance Futures REST.
    Returns dict with keys: lastUpdateId, bids, asks (all strings).
    """
    url = "https://fapi.binance.com/fapi/v1/depth"
    params = {"symbol": symbol.upper(), "limit": limit}
    backoff = 0.25
    for _ in range(retries):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, params=params)
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    await asyncio.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp.json()
        except (httpx.HTTPError, httpx.ReadTimeout):
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 5.0)
    raise RuntimeError(f"snapshot failed for {symbol}")


# ---------- Init + plumbing (with bronze REST snapshot write) ----------

async def init_book(
    symbol: str,
    speed: str = "100ms",
    periodic_resync_sec: float = 3600.0,
    *,
    external_buf: Optional[Deque[dict]] = None,
    external_event: Optional[asyncio.Event] = None,
    wal_writer=None,  # <-- NEW: optional bronze WAL writer for REST snapshot
):
    """
    Initialize LocalBook from a REST snapshot and prepare buffers/signals.
    Returns: (book, buf, stop, new_event, reserved)
    - If wal_writer is provided, writes a bronze REST snapshot: etype="ob_snapshot_rest".
    """
    stream = f"{symbol.lower()}@depth@{speed}"
    buf = external_buf or deque(maxlen=200_000)
    new_event = external_event or asyncio.Event()
    stop = asyncio.Event()

    # Fetch REST snapshot (bootstrap)
    snap = await fetch_snapshot(symbol, limit=1000)
    last_id = int(snap["lastUpdateId"])

    # Initialize book from REST
    book = LocalBook()
    book.bids = {p: q for p, q in snap["bids"]}
    book.asks = {p: q for p, q in snap["asks"]}
    book.last_u = last_id
    book.last_ts_ns = time.time_ns()

    # Write bronze REST snapshot (raw-only) if WAL writer provided
    if wal_writer is not None:
        try:
            rest_payload = {
                "source": "rest",
                "lastUpdateId": int(snap["lastUpdateId"]),
                "bids": snap["bids"],  # list[list[str, str]]
                "asks": snap["asks"],
            }
            await wal_writer.append("ob_snapshot_rest", symbol, rest_payload, time.time_ns())
        except Exception:
            logging.exception("[init_book:%s] WAL write of REST snapshot failed", symbol)

    # Keep return signature backward-compatible
    return book, buf, stop, new_event, None


# ---------- Periodic local RAW snapshot → bronze WAL ----------

def start_snapshotter(
    book: LocalBook,
    symbol: str,
    wal_writer,
    *,
    interval_sec: float = 1.0,
    top_n: int = 10,
    stop_event: Optional[asyncio.Event] = None,
) -> asyncio.Task:
    """
    Periodically write RAW local order book snapshots to bronze WAL as:
      etype="ob_snapshot_local"
      payload = {
        "source": "local",
        "last_u": <int>,
        "bids": [[price_str, qty_str], ...],  # top_n trimmed
        "asks": [[price_str, qty_str], ...],
      }
    NOTE: No derived features here. Silver layer will compute those.
    """
    async def _run():
        try:
            while True:
                if stop_event and stop_event.is_set():
                    break
                snap = book.snapshot(symbol, top_n=top_n)
                payload = {
                    "source": "local",
                    "last_u": int(snap["last_u"]) if snap.get("last_u") is not None else None,
                    "bids": snap["bids"],
                    "asks": snap["asks"],
                }
                try:
                    await wal_writer.append("ob_snapshot_local", symbol, payload, time.time_ns())
                except Exception:
                    logging.exception("[snapshotter:%s] WAL append failed", symbol)
                await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            # graceful shutdown
            pass

    return asyncio.create_task(_run())
