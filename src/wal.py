#!/usr/bin/env python3
# src/wal.py  — rotation-safe WAL writer with idle-time rotation
from __future__ import annotations

import asyncio
import os
import time
import pathlib
import logging
from typing import Optional

import orjson
from concurrent.futures import ThreadPoolExecutor


class WalWriter:
    """
    Append-only WAL writer with batched async writes.

    Fixes vs. prior version:
      - No nested lock during rotation (_rotate_with_fsync -> _open_new handles its own locking).
      - Periodic wake (timeout on q.get) so time-based rotation and fsync can run while idle.
      - Writes are offloaded to a thread pool; fsync also offloaded to avoid blocking the loop.
    """

    def __init__(
        self,
        base_dir: str,
        rotate_bytes: int = 128 * 1024 * 1024,
        fsync_interval: float = 5.0,
        *,
        rotate_sec: Optional[float] = None,
        write_queue_maxsize: int = 200_000,
        batch_max_records: int = 1024,
        batch_max_bytes: int = 1 * 1024 * 1024,
        io_workers: int = 1,
        raw_mode: bool = False,
        idle_tick_sec: float = 1.0,  # how often to wake even if no frames
    ):
        self.q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=write_queue_maxsize)
        self.base = pathlib.Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)

        self.rotate_bytes = int(rotate_bytes)
        self.rotate_sec = None if rotate_sec is None else float(rotate_sec)
        self.fsync_interval = float(fsync_interval)
        self.idle_tick_sec = float(idle_tick_sec)

        # Current open file handle and metadata
        self.cur = None  # type: Optional[object]
        self.nbytes = 0
        self._counter = 0
        self._last_fsync = time.time()
        self._open_path: Optional[str] = None
        self._open_day: Optional[str] = None
        self._open_walltime = time.time()

        # Batching
        self._batch_max_records = int(batch_max_records)
        self._batch_max_bytes = int(batch_max_bytes)

        # I/O threading + locking
        self._io_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=max(1, int(io_workers)), thread_name_prefix="wal-io"
        )
        import threading
        self._io_lock = threading.Lock()  # avoid nested acquire; _open_new owns it

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._raw_mode = bool(raw_mode)

    # -------------------------
    # Append APIs
    # -------------------------

    async def append(self, etype: str, symbol: str, payload: dict, ts_recv_ns: int):
        rec = {
            "ts_recv_ns": ts_recv_ns,
            "etype": etype,
            "symbol": symbol,
            "payload": payload,
            "seq": self._counter,
        }
        self._counter += 1
        line = orjson.dumps(rec)
        await self.q.put(line)

    async def append_raw_frame(self, raw: bytes, ts_recv_ns: int):
        if not isinstance(raw, (bytes, bytearray)):
            raise TypeError("append_raw_frame expects bytes")
        # One line: "<ts_ns> <raw_bytes>"
        line = str(ts_recv_ns).encode("ascii") + b" " + (raw if isinstance(raw, bytes) else bytes(raw))
        await self.q.put(line)

    # -------------------------
    # Main run loop
    # -------------------------

    async def run(self):
        self._loop = asyncio.get_running_loop()
        if self.cur is None:
            self._open_new()

        async def _write_batch_offloaded(buf: bytes):
            if not buf:
                return
            await self._loop.run_in_executor(self._io_pool, self._sync_write, buf)

        try:
            while True:
                # Wake at least every idle_tick_sec to run housekeeping, even if no frames arrive.
                try:
                    first = await asyncio.wait_for(self.q.get(), timeout=self.idle_tick_sec)
                except asyncio.TimeoutError:
                    first = None

                batch_bytes = bytearray()
                size = 0
                records = 0

                if first is not None:
                    batch_bytes += first + b"\n"
                    size += len(first) + 1
                    records = 1

                    # Drain queue up to limits
                    while records < self._batch_max_records and size < self._batch_max_bytes:
                        try:
                            item = self.q.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        batch_bytes += item + b"\n"
                        size += len(item) + 1
                        records += 1

                    # Offload write
                    try:
                        await _write_batch_offloaded(batch_bytes)
                    except Exception:
                        logging.exception("[wal] batched write failed")
                        try:
                            # Reopen and retry write once
                            self._open_new()
                            await _write_batch_offloaded(batch_bytes)
                        except Exception:
                            logging.exception("[wal] retry batched write failed")

                    self.nbytes += size

                # Housekeeping: fsync + rotate checks (runs even if idle)
                now = time.time()

                if (self.nbytes >= self.rotate_bytes) or ((now - self._last_fsync) >= self.fsync_interval):
                    await self._fsync_offloaded()
                    self._last_fsync = now

                if (self.rotate_sec is not None) and ((now - self._open_walltime) >= self.rotate_sec):
                    await self._rotate_with_fsync("time-rotate")

                # Day-boundary rollover
                now_day = time.strftime("%Y-%m-%d", time.gmtime())
                if (self._open_day is not None) and (now_day != self._open_day):
                    await self._rotate_with_fsync("day-rollover")

                if self.nbytes >= self.rotate_bytes:
                    await self._rotate_with_fsync("size-rotate")

        except asyncio.CancelledError:
            logging.info("[wal] cancelled; draining")
            raise

    # -------------------------
    # Low-level I/O helpers
    # -------------------------

    def _open_new(self):
        """Create a new segment and safely close the old one — owns the I/O lock."""
        now_tm = time.gmtime()
        day = time.strftime("%Y-%m-%d", now_tm)
        ts = time.strftime("%Y%m%d-%H%M%S", now_tm)

        day_dir = self.base / day
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / f"wal-{ts}.ndjson"

        with self._io_lock:
            # Close old handle (flush + fsync) if present
            if self.cur:
                try:
                    self.cur.flush()
                    os.fsync(self.cur.fileno())
                except Exception:
                    pass
                try:
                    self.cur.close()
                except Exception:
                    pass

            # Open new handle
            self.cur = open(path, "ab", buffering=1024 * 1024)

        self._open_path = str(path)
        self.nbytes = 0
        self._last_fsync = time.time()
        self._open_day = day
        self._open_walltime = time.time()
        logging.info("[wal] opened new segment %s", path)

    def _sync_write(self, buf: bytes):
        if not buf:
            return
        with self._io_lock:
            if not self.cur:
                raise RuntimeError("WAL file handle is not open")
            self.cur.write(buf)

    async def _fsync_offloaded(self):
        def _do_fsync():
            with self._io_lock:
                if not self.cur:
                    return
                self.cur.flush()
                os.fsync(self.cur.fileno())
        await self._loop.run_in_executor(self._io_pool, _do_fsync)

    async def _rotate_with_fsync(self, label: str):
        # Ensure current data is on disk, then atomically open a new segment.
        await self._fsync_offloaded()
        self._open_new()
        logging.info("[wal] %s -> %s", label, self._open_path)
