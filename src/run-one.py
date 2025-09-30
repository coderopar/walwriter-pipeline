# src/run_one.py  (metrics-free drop-in)

import argparse
import asyncio
import logging
import signal
from typing import List, Tuple

import yaml

from src.wal import WalWriter
from src.streams import ws_consume_combined, StreamRouter
from src import orderbook


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg


def setup_logging(level_str: str):
    level = getattr(logging, (level_str or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def start_stream_shards(cfg: dict, wal: WalWriter, router: StreamRouter) -> List[asyncio.Task]:
    streams_cfg = cfg.get("streams", {})
    wal_write_raw = bool(streams_cfg.get("wal_write_raw", True))
    shards = streams_cfg.get("shards", [])

    tasks: List[asyncio.Task] = []
    for s in shards:
        sid = s.get("id", "shard-0")
        url = s["url"]
        t = asyncio.create_task(
            ws_consume_combined(
                url=url,
                shard_id=sid,
                wal_writer=wal,
                on_msg_cb=router.callback,
                wal_write_raw=wal_write_raw,
            )
        )
        tasks.append(t)
        logging.info("started stream shard id=%s url=%s", sid, url)
    return tasks


async def start_orderbooks(
    cfg: dict, router: StreamRouter, wal: WalWriter
) -> List[Tuple[asyncio.Event, asyncio.Task | None, asyncio.Task | None]]:
    """
    Start orderbooks that consume from the router feed (no second socket).
    Returns a list of (stop_event, applier_task, snapshotter_task).
    """
    ob_cfgs = cfg.get("orderbooks", [])
    tasks = []
    for ob in ob_cfgs:
        symbol = ob["symbol"].upper()
        speed = ob.get("speed", "100ms")
        periodic_resync_sec = float(ob.get("periodic_resync_sec", 3600.0))

        # Get router feed for depth updates
        depth_buf, depth_event = router.get_feed(symbol, "depth")

        # Initialize the orderbook (snapshot + covering wait handled inside)
        book, _, stop_ev, reader_task, applier_task = await orderbook.init_book(
            symbol=symbol,
            speed=speed,
            periodic_resync_sec=periodic_resync_sec,
            external_buf=depth_buf,
            external_event=depth_event,
        )
        # reader_task is None because we used external feed
        logging.info("orderbook ready symbol=%s last_u=%s", symbol, book.last_u)

        # Optional snapshotter
        snap_cfg = (ob.get("snapshotter") or {})
        if snap_cfg.get("enabled", False):
            interval_sec = float(snap_cfg.get("interval_sec", 1.0))
            top_n = int(snap_cfg.get("top_n", 10))
            snap_task = orderbook.start_snapshotter(
                book, symbol, wal, interval_sec=interval_sec, top_n=top_n
            )
        else:
            snap_task = None

        tasks.append((stop_ev, applier_task, snap_task))
    return tasks


async def main_async(cfg_path: str):
    cfg = load_config(cfg_path)
    setup_logging(cfg.get("log_level", "INFO"))

    # WAL
    wcfg = cfg.get("wal", {})
    wal = WalWriter(
        base_dir=wcfg.get("base_dir", "./data/bronze"),
        rotate_bytes=int(wcfg.get("rotate_bytes", 128 * 1024 * 1024)),
        fsync_interval=float(wcfg.get("fsync_interval", 5.0)),
        rotate_sec=(
            None if wcfg.get("rotate_sec", None) in (None, "null") else float(wcfg["rotate_sec"])
        ),
        write_queue_maxsize=int(wcfg.get("write_queue_maxsize", 200_000)),
        batch_max_records=int(wcfg.get("batch_max_records", 1024)),
        batch_max_bytes=int(wcfg.get("batch_max_bytes", 1 * 1024 * 1024)),
        io_workers=int(wcfg.get("io_workers", 1)),
        raw_mode=bool(wcfg.get("raw_mode", True)),
    )
    wal_task = asyncio.create_task(wal.run())

    # Streams + Router
    router = StreamRouter(maxlen=200_000)
    shard_tasks = await start_stream_shards(cfg, wal, router)

    # Orderbooks
    ob_tasks = await start_orderbooks(cfg, router, wal)

    # Graceful shutdown
    stop_ev = asyncio.Event()

    def _handle_signal(signame):
        logging.info("received %s, starting graceful shutdown", signame)
        stop_ev.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _handle_signal, s.name)
        except NotImplementedError:
            signal.signal(s, lambda *_: _handle_signal(s.name))  # Windows fallback

    await stop_ev.wait()

    # Teardown order: shards -> orderbooks -> wal
    for st in shard_tasks:
        st.cancel()
    for (ob_stop, applier_task, snap_task) in ob_tasks:
        ob_stop.set()
        if applier_task:
            applier_task.cancel()
        if snap_task:
            snap_task.cancel()
    wal_task.cancel()

    # Await cancellations
    await asyncio.gather(*shard_tasks, return_exceptions=True)
    await asyncio.gather(*(t[1] for t in ob_tasks if t[1] is not None), return_exceptions=True)
    await asyncio.gather(*(t[2] for t in ob_tasks if t[2] is not None), return_exceptions=True)
    await asyncio.gather(wal_task, return_exceptions=True)
    logging.info("shutdown complete")


def main():
    parser = argparse.ArgumentParser(description="Trading pipeline bootstrap")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()