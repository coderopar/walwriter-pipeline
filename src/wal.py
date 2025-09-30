# src/bronze/wal.py  (metrics-free drop-in)

import asyncio
import os
import time
import pathlib
import orjson
import logging
from typing import Optional


class WalWriter:
    """Append-only WAL writer (no compression)."""

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
    ):
        self.q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=write_queue_maxsize)
        self.base = pathlib.Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)

        self.rotate_bytes = int(rotate_bytes)
        self.rotate_sec = rotate_sec if rotate_sec is None else float(rotate_sec)
        self.fsync_interval = float(fsync_interval)

        self.cur = None
        self.nbytes = 0
        self._counter = 0
        self._last_fsync = time.time()
        self._open_path: Optional[str] = None
        self._open_day: Optional[str] = None
        self._open_walltime = time.time()

        self._batch_max_records = int(batch_max_records)
        self._batch_max_bytes = int(batch_max_bytes)
        from concurrent.futures import ThreadPoolExecutor as _TPE
        self._io_pool: _TPE = _TPE(max_workers=max(1, int(io_workers)), thread_name_prefix="wal-io")
        import threading
        self._io_lock = threading.Lock()
        self._loop = None

        self._raw_mode = bool(raw_mode)

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
        line = str(ts_recv_ns).encode("ascii") + b" " + (raw if isinstance(raw, bytes) else bytes(raw))
        await self.q.put(line)

    async def run(self):
        self._loop = asyncio.get_running_loop()
        if self.cur is None:
            self._open_new()

        async def _write_batch_offloaded(buf: bytes):
            await self._loop.run_in_executor(self._io_pool, self._sync_write, buf)

        try:
            while True:
                # Dequeue first item (blocking), then drain up to batch limits.
                first = await self.q.get()
                batch_bytes = bytearray()
                batch_bytes += first + b"\n"
                records = 1
                size = len(first) + 1

                while records < self._batch_max_records and size < self._batch_max_bytes:
                    try:
                        item = self.q.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    batch_bytes += item + b"\n"
                    records += 1
                    size += len(item) + 1

                try:
                    await _write_batch_offloaded(batch_bytes)
                except Exception:
                    logging.exception("[wal] batched write failed")
                    try:
                        with self._io_lock:
                            if self.cur:
                                self.cur.close()
                            self._open_new()
                        await _write_batch_offloaded(batch_bytes)
                    except Exception:
                        logging.exception("[wal] retry batched write failed")

                self.nbytes += size

                now = time.time()
                if (self.nbytes >= self.rotate_bytes) or (now - self._last_fsync >= self.fsync_interval):
                    t0 = time.monotonic()
                    await self._fsync_offloaded()
                    # (metrics removed) previously observed fsync duration
                    self._last_fsync = now

                if self.rotate_sec is not None and (now - self._open_walltime) >= self.rotate_sec:
                    await self._rotate_with_fsync("time-rotate")

                now_day = time.strftime("%Y-%m-%d", time.gmtime())
                if self._open_day is not None and now_day != self._open_day:
                    await self._rotate_with_fsync("day-rollover")

                if self.nbytes >= self.rotate_bytes:
                    await self._rotate_with_fsync("size-rotate")
        except asyncio.CancelledError:
            logging.info("[wal] cancelled; draining")
            raise

    def _open_new(self):
        now_tm = time.gmtime()
        day = time.strftime("%Y-%m-%d", now_tm)
        ts = time.strftime("%Y%m%d-%H%M%S", now_tm)
        day_dir = self.base / day
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / f"wal-{ts}.ndjson"
        if self.cur:
            self.cur.flush()
            os.fsync(self.cur.fileno())
            self.cur.close()
        with self._io_lock:
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
        await self._fsync_offloaded()
        with self._io_lock:
            if self.cur:
                self.cur.close()
            self._open_new()
        logging.info("[wal] %s -> %s", label, self._open_path)
