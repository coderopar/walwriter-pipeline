# src/compress_closed.py
#!/usr/bin/env python3
"""
Compress CLOSED WAL segments to .zst, without touching the live writer.

Strategy
- Scan bronze_dir for wal-*.ndjson.
- Skip files modified in the last --grace-sec (default 120s) to avoid racing the writer.
- Skip if a newer .zst already exists.
- Compress to .zst.tmp, then atomic rename to .zst, then (optionally) delete the .ndjson.
- Idempotent & safe to rerun.
"""
from __future__ import annotations
import argparse, os, time, pathlib, logging
import zstandard as zstd

def compress_file(src: pathlib.Path, level: int = 10, delete_src: bool = True) -> bool:
    dst = src.with_suffix(src.suffix + ".zst")
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    try:
        # If already compressed and newer, skip
        if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime and dst.stat().st_size > 0:
            if delete_src:
                try: src.unlink()
                except Exception: pass
            return False
        cctx = zstd.ZstdCompressor(level=level)
        with open(src, "rb") as fin, open(tmp, "wb") as fout:
            with cctx.stream_writer(fout) as zw:
                for chunk in iter(lambda: fin.read(1<<20), b""):
                    zw.write(chunk)
        os.replace(tmp, dst)  # atomic
        if delete_src:
            try: src.unlink()
            except Exception: pass
        logging.info("compressed %s -> %s (level=%d)", src, dst, level)
        return True
    except Exception as e:
        try:
            if tmp.exists(): tmp.unlink()
        except Exception:
            pass
        logging.exception("compress failed for %s: %s", src, e)
        return False

def main():
    ap = argparse.ArgumentParser(description="Compress closed WAL ndjson files to zstd")
    ap.add_argument("--bronze-dir", required=True, help="Base bronze directory (e.g. /opt/marketdata/data/bronze)")
    ap.add_argument("--level", type=int, default=10, help="zstd compression level")
    ap.add_argument("--grace-sec", type=int, default=120, help="Skip files modified within the last N seconds")
    ap.add_argument("--keep-ndjson", action="store_true", help="Keep original .ndjson (default deletes after compress)")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(message)s")

    base = pathlib.Path(args.bronze_dir)
    if not base.exists():
        logging.warning("bronze dir not found: %s", base)
        return

    now = time.time()
    # Find all *.ndjson under date subfolders
    ndjsons = sorted(base.glob("*/wal-*.ndjson"))  # one level (YYYY-MM-DD)
    # Heuristic: skip the newest file entirely; it’s likely the open segment.
    if ndjsons:
        newest = ndjsons[-1]
    else:
        newest = None

    count = 0
    for p in ndjsons:
        if p == newest:
            continue
        try:
            mtime = p.stat().st_mtime
        except FileNotFoundError:
            continue
        if (now - mtime) < args.grace_sec:
            continue
        compress_file(p, level=args.level, delete_src=(not args.keep_ndjson))
        count += 1
    logging.info("scan complete; considered=%d", count)

if __name__ == "__main__":
    main()
