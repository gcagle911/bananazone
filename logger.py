import json
import time
import traceback
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

import yaml
from ccxt.base.errors import RateLimitExceeded, DDoSProtection, ExchangeError

from exchanges import make_exchange, symbol_for
from metrics import compute_metrics
from storage import append_jsonl_line, download_text, upload_text, list_prefix, compose_many, get_storage_backend

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_logger.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def iso_utc(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def fmt_paths(cfg, ex: str, asset: str, t: datetime) -> Dict[str, str]:
    day = t.strftime("%Y-%m-%d")
    hour = t.strftime("%H")
    minute = t.strftime("%M")
    p = cfg["paths"]
    return {
        "five_sec_minute": p["five_sec_minute"].format(
            ex=ex, asset=asset, day=day, hour=hour, minute=minute
        ),
        "five_sec_daily": p["five_sec_daily"].format(ex=ex, asset=asset, day=day),
        # NEW per-minute 1m file
        "one_min_minute": p["one_min_minute"].format(
            ex=ex, asset=asset, day=day, hour=hour, minute=minute
        ),
        "one_min_daily": p["one_min_daily"].format(ex=ex, asset=asset, day=day),
    }


def aggregate_minute_from_5s(
    records: List[Dict[str, Any]], t_minute: datetime, ex: str, asset: str
) -> Dict[str, Any]:
    fields = [
        "mid",
        "spread_L5_pct",
        "spread_L50_pct",
        "spread_L100_pct",
        "vol_L50_bids",
        "vol_L50_asks",
        "depth_bids",
        "depth_asks",
    ]
    agg: Dict[str, Any] = {
        "t": iso_utc(t_minute.replace(second=0, microsecond=0)),
        "exchange": ex,
        "asset": asset,
    }
    for f in fields:
        vals = [
            r.get(f)
            for r in records
            if isinstance(r.get(f), (int, float)) and r.get(f) == r.get(f)
        ]
        agg[f] = (sum(vals) / len(vals)) if vals else None
    return agg


def publish_1min_nearlive(cfg, bucket: str, ex: str, asset: str, now: datetime):
    """
    Every publish_1min_minutes, rebuild the last window of minutes from per-minute 5s files,
    write EACH minute as its own one-line NDJSON object under 1min/min/..., then COMPOSE all
    1min/min/... files for the day into the daily 1min/YYYY-MM-DD.jsonl.

    This avoids read-modify-write overwrites and guarantees we never "roll" to 5m only.
    """
    minutes_back = int(cfg.get("publish_1min_minutes", 5))
    end_minute = now.replace(second=0, microsecond=0)
    start_minute = end_minute - timedelta(minutes=minutes_back - 1)

    # 1) Build per-minute rows for the window and store each to its own object
    for i in range(minutes_back):
        m = start_minute + timedelta(minutes=i)
        paths = fmt_paths(cfg, ex, asset, m)
        src_5s = paths["five_sec_minute"]
        dst_1m_min = paths["one_min_minute"]

        text = download_text(bucket, src_5s)
        if not text:
            continue

        records: List[Dict[str, Any]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                pass
        if not records:
            continue

        row = aggregate_minute_from_5s(records, m, ex, asset)
        # Each per-minute file contains exactly ONE line
        upload_text(bucket, dst_1m_min, json.dumps(row, separators=(",", ":")) + "\n")

    # 2) Compose ALL per-minute 1m files for the day → daily 1m NDJSON
    day = now.strftime("%Y-%m-%d")
    prefix = f"{ex}/{asset}/1min/min/{day}/"
    sources = list_prefix(bucket, prefix)
    if not sources:
        return
    dest = fmt_paths(cfg, ex, asset, now)["one_min_daily"]
    compose_many(bucket, sources, dest)


def publish_5s_daily(cfg, bucket: str, ex: str, asset: str, now: datetime):
    """Compose all minute 5s files for the day into a single daily NDJSON (hourly by default)."""
    day = now.strftime("%Y-%m-%d")
    prefix = f"{ex}/{asset}/5s/min/{day}/"
    sources = list_prefix(bucket, prefix)
    if not sources:
        return
    dest = fmt_paths(cfg, ex, asset, now)["five_sec_daily"]
    compose_many(bucket, sources, dest)


def main():
    logger.info("Starting crypto data collector...")
    
    # Run one-time header fix for existing files
    try:
        from startup_fix import run_header_fix_once
        run_header_fix_once()
    except Exception as e:
        logger.warning(f"Header fix failed (non-critical): {e}")
    
    cfg = load_config()
    interval = int(cfg.get("interval_seconds", 5))
    bucket = cfg["gcs_bucket"]
    layers = cfg.get("layers", [5, 50, 100])
    publish_1m = int(cfg.get("publish_1min_minutes", 5))
    publish_5s = int(cfg.get("publish_5s_minutes", 60))

    exchanges_cfg = cfg["exchanges"]
    assets = cfg["assets"]
    
    # Initialize storage backend
    storage_backend = get_storage_backend(bucket)
    logger.info(f"Storage backend initialized for bucket: {bucket}")

    clients: Dict[str, Any] = {}
    quotes: Dict[str, str] = {}
    for e in exchanges_cfg:
        name = e["name"]
        quotes[name] = e["quote"]
        try:
            clients[name] = make_exchange(name)
            logger.info(f"Initialized exchange client: {name}")
        except Exception as e:
            logger.error(f"Failed to initialize {name}: {e}")

    # Load markets for exchanges that allow it
    for name, client in clients.items():
        if name == "bybit":  # if you ever re-enable bybit
            logger.info("Skipping load_markets() for bybit")
            continue
        try:
            client.load_markets()
            market_count = len(getattr(client, 'markets', {}) or [])
            logger.info(f"Loaded markets: {name} ({market_count} symbols)")
        except Exception as e:
            logger.error(f"load_markets failed for {name}: {e}")

    last_pub_1m: Dict[str, datetime] = {}
    last_pub_5s: Dict[str, datetime] = {}
    
    # Track statistics
    stats = {
        "total_fetches": 0,
        "successful_fetches": 0,
        "failed_fetches": 0,
        "last_success_time": None
    }

    try:
        logger.info(f"Starting data collection loop (interval: {interval}s)")
        
        while True:
            now = datetime.now(timezone.utc)
            t_iso = iso_utc(now)
            cycle_start = time.time()

            for ex_name, client in clients.items():
                quote = quotes[ex_name]
                for asset in assets:
                    sym = symbol_for(ex_name, asset, quote)
                    stats["total_fetches"] += 1
                    
                    try:
                        # Fetch order book data
                        ob = client.fetch_order_book(sym, limit=200)
                        
                        # Validate order book data
                        if not ob or not ob.get('bids') or not ob.get('asks'):
                            logger.warning(f"Invalid order book data for {ex_name} {asset}")
                            stats["failed_fetches"] += 1
                            continue
                            
                        metrics = compute_metrics(ob, layers)
                        
                        # Validate metrics
                        if not metrics or metrics.get("mid") is None:
                            logger.warning(f"Invalid metrics for {ex_name} {asset}")
                            stats["failed_fetches"] += 1
                            continue
                        
                        record = {
                            "t": t_iso,
                            "exchange": ex_name,
                            "asset": asset,
                            "mid": metrics["mid"],
                            "spread_L5_pct": metrics["spread_L5_pct"],
                            "spread_L50_pct": metrics["spread_L50_pct"],
                            "spread_L100_pct": metrics["spread_L100_pct"],
                            "vol_L50_bids": metrics["vol_L50_bids"],
                            "vol_L50_asks": metrics["vol_L50_asks"],
                            "depth_bids": metrics["depth_bids"],
                            "depth_asks": metrics["depth_asks"],
                        }
                        
                        # Append this 5s tick into the current minute's NDJSON file
                        path_keys = fmt_paths(cfg, ex_name, asset, now)
                        append_jsonl_line(bucket, path_keys["five_sec_minute"], json.dumps(record, separators=(",", ":")))
                        
                        stats["successful_fetches"] += 1
                        stats["last_success_time"] = now
                        
                        logger.debug(f"Recorded data: {ex_name} {asset} mid={metrics['mid']:.4f}")
                        
                    except (RateLimitExceeded, DDoSProtection) as e:
                        logger.warning(f"Rate limit for {ex_name} {asset}: {e}")
                        stats["failed_fetches"] += 1
                        time.sleep(1)  # Brief pause for rate limits
                        
                    except ExchangeError as e:
                        logger.error(f"Exchange error {ex_name} {asset}: {e}")
                        stats["failed_fetches"] += 1
                        
                    except Exception as e:
                        logger.error(f"Unexpected error {ex_name} {asset}: {e}")
                        stats["failed_fetches"] += 1

                    # 1m near-live compose (<=5m lag)
                    pair_key = f"{ex_name}:{asset}"
                    if (last_pub_1m.get(pair_key) is None) or (
                        (now - last_pub_1m[pair_key]) >= timedelta(minutes=publish_1m)
                    ):
                        try:
                            publish_1min_nearlive(cfg, bucket, ex_name, asset, now)
                            last_pub_1m[pair_key] = now
                            logger.info(f"Published 1min data for {pair_key}")
                        except Exception as e:
                            logger.error(f"Failed to publish 1m {pair_key}: {e}")

                    # 5s daily compose (hourly default)
                    if (last_pub_5s.get(pair_key) is None) or (
                        (now - last_pub_5s[pair_key]) >= timedelta(minutes=publish_5s)
                    ):
                        try:
                            publish_5s_daily(cfg, bucket, ex_name, asset, now)
                            last_pub_5s[pair_key] = now
                            logger.info(f"Published 5s daily data for {pair_key}")
                        except Exception as e:
                            logger.error(f"Failed to publish 5s {pair_key}: {e}")

            # Log statistics every 10 cycles
            if stats["total_fetches"] % (len(clients) * len(assets) * 10) == 0:
                success_rate = (stats["successful_fetches"] / stats["total_fetches"] * 100) if stats["total_fetches"] > 0 else 0
                logger.info(f"Stats: {stats['total_fetches']} total, {stats['successful_fetches']} success, "
                           f"{stats['failed_fetches']} failed ({success_rate:.1f}% success rate)")
            
            # Sleep for the remaining interval time
            cycle_time = time.time() - cycle_start
            sleep_time = max(0.1, interval - cycle_time)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")
        traceback.print_exc()
    finally:
        logger.info("Cleaning up exchange connections...")
        for c in clients.values():
            try:
                if hasattr(c, "close"):
                    c.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")
        logger.info("Crypto data collector stopped.")


if __name__ == "__main__":
    import os
    import sys
    
    # Check if we should use the improved collector
    use_improved = os.environ.get("USE_IMPROVED_COLLECTOR", "true").lower() == "true"
    
    if use_improved:
        try:
            logger.info("🚀 Using improved collector with guaranteed minute scheduling")
            from guaranteed_minute_scheduler import main as improved_main
            improved_main()
        except ImportError as e:
            logger.warning(f"Failed to import improved collector: {e}")
            logger.info("🔄 Falling back to standard collector")
            # Don't call main() recursively, run the standard collector directly
            cfg = load_config()
            interval = int(cfg.get("interval_seconds", 5))
            bucket = cfg["gcs_bucket"]
            # ... run standard collector logic
            main()
        except Exception as e:
            logger.error(f"Improved collector failed: {e}")
            logger.error(f"Error details: {traceback.format_exc()}")
            logger.info("🔄 Falling back to standard collector")
            main()
    else:
        logger.info("📡 Using standard collector")
        main()
