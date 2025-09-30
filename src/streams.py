# src/bronze/streams.py  (metrics-free drop-in)

import asyncio
import time
import logging
import inspect
import orjson
import websockets  # WebSocket client
from collections import deque
from typing import Deque, Dict, Tuple, Optional


def _symbol_etype_from_stream(stream: str) -> tuple[str, str]:
    """
    Parse combined stream name like:
      "btcusdt@depth@100ms"      -> ("BTCUSDT", "depth")
      "ethusdt@markPrice@1s"     -> ("ETHUSDT", "markPrice")
      "btcusdt@aggTrade"         -> ("BTCUSDT", "aggTrade")
    """
    s = stream.lower()
    parts = s.split("@")
    if len(parts) < 2:
        return stream.upper(), "unknown"
    symbol = parts[0].upper()
    et_raw = parts[1]
    if et_raw == "markprice":
        etype = "markPrice"
    elif et_raw == "aggtrade":
        etype = "aggTrade"
    elif et_raw == "forceorder":
        etype = "forceOrder"
    else:
        etype = et_raw
    return symbol, etype


class StreamRouter:
    """Fan-out router for a combined WebSocket."""

    def __init__(self, maxlen: int = 200_000):
        self._feeds: Dict[Tuple[str, str], Tuple[Deque[dict], asyncio.Event]] = {}
        self._maxlen = maxlen

    def get_feed(self, symbol: str, etype: str) -> Tuple[Deque[dict], asyncio.Event]:
        key = (symbol.upper(), etype)
        if key not in self._feeds:
            self._feeds[key] = (deque(maxlen=self._maxlen), asyncio.Event())
        return self._feeds[key]

    def callback(self, stream: str, data: dict):
        """
        Route a single message to its per-(symbol,etype) buffer.

        EXPECTS: data already enriched upstream with:
          - data["ts_recv_ns"] : int  (local ingest ns)
          - data["_meta"]      : dict (includes "stream", "shard_id", optional "arr_idx")
        """
        symbol, etype = _symbol_etype_from_stream(stream)
        buf, ev = self.get_feed(symbol, etype)
        buf.append(data)
        if len(buf) == buf.maxlen:
            logging.warning(
                "[router] feed (%s, %s) buffer at capacity (%d)", symbol, etype, buf.maxlen
            )
        if not ev.is_set():
            ev.set()


async def ws_consume_combined(
    url: str,
    shard_id: str,
    wal_writer,
    on_msg_cb=None,
    *,
    wal_write_raw: bool = True,
):
    """
    Consume a combined WebSocket forever:
      1) Write each frame to WAL (raw or structured).
      2) Optionally fan-out to in-proc consumers via on_msg_cb (router).

    Patch highlights (metrics removed):
      - Every routed message is enriched with:
          data["ts_recv_ns"] = time.time_ns()          # local ingest clock
          data["_meta"] = {"stream": stream,
                           "shard_id": shard_id,
                           "arr_idx": <monotonic per-conn counter>}
    """
    base_backoff = 0.25
    max_backoff = 30.0

    while True:
        backoff = base_backoff
        arr_idx = 0  # monotonic tie-breaker per connection for ordering
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_queue=None,
                max_size=None,
                compression="deflate",
            ) as ws:
                logging.info("[ws] connected shard=%s url=%s", shard_id, url)
                conn_start = time.monotonic()
                backoff = base_backoff
                arr_idx = 0

                while True:
                    raw = await ws.recv()
                    ts_recv_ns = time.time_ns()

                    raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else raw.encode()

                    # ---- WAL path (source-of-truth persistence) ----
                    try:
                        if wal_write_raw and hasattr(wal_writer, "append_raw_frame"):
                            await wal_writer.append_raw_frame(raw_bytes, ts_recv_ns)
                        else:
                            msg_for_wal = orjson.loads(raw_bytes)
                            stream_for_wal = msg_for_wal.get("stream")
                            data_for_wal = msg_for_wal.get("data")
                            symbol_for_wal, etype_for_wal = _symbol_etype_from_stream(stream_for_wal or "")
                            payload_for_wal = data_for_wal if isinstance(data_for_wal, dict) else {"raw": data_for_wal}
                            payload_for_wal.setdefault("_meta", {})
                            payload_for_wal["_meta"].update({"shard_id": shard_id, "stream": stream_for_wal})
                            await wal_writer.append(
                                etype=etype_for_wal,
                                symbol=symbol_for_wal,
                                payload=payload_for_wal,
                                ts_recv_ns=ts_recv_ns,
                            )
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logging.exception("[ws] WAL append failed: %s", e)
                        continue  # keep socket flowing

                    # ---- Router / in-process callback (fan-out) ----
                    if on_msg_cb:
                        try:
                            msg = orjson.loads(raw_bytes)
                        except orjson.JSONDecodeError:
                            logging.warning("[ws] JSON decode error (ignored for routing)")
                            continue

                        stream = msg.get("stream")
                        data = msg.get("data")
                        if stream is None or data is None:
                            logging.debug("[ws] missing stream/data; keys=%s", list(msg.keys()))
                            continue

                        # Enrich payload with ingest timestamp + provenance
                        try:
                            d = dict(data)  # shallow copy to avoid aliasing
                            meta = dict(d.get("_meta", {}))
                            meta.update({"stream": stream, "shard_id": shard_id, "arr_idx": arr_idx})
                            d["_meta"] = meta
                            d["ts_recv_ns"] = ts_recv_ns
                            arr_idx += 1

                            result = on_msg_cb(stream, d)
                            if inspect.isawaitable(result):
                                await result
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logging.exception("[ws] on_msg_cb error")

        except asyncio.CancelledError:
            logging.info("[ws] cancelled shard=%s", shard_id)
            break

        except websockets.ConnectionClosedError as e:
            logging.warning(
                "[ws] connection closed code=%s reason=%s",
                getattr(e, "code", "?"),
                getattr(e, "reason", "?"),
            )
            try:
                _ = max(0.0, time.monotonic() - conn_start)  # keep duration behavior (no metrics)
            except UnboundLocalError:
                pass

        except Exception as e:
            logging.exception("[ws] error: %s", e)
            try:
                _ = max(0.0, time.monotonic() - conn_start)
            except UnboundLocalError:
                pass

        # Exponential backoff with jitter before reconnect
        jitter = 0.8 + 0.4 * ((time.time_ns() % 1_000_000) / 1_000_000)
        delay = backoff * jitter
        await asyncio.sleep(delay)
        backoff = min(backoff * 2, max_backoff)
