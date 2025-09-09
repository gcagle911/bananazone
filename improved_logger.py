#!/usr/bin/env python3
"""
Improved crypto data collector with guaranteed 1-minute updates and better error handling.
"""

import json
import time
import traceback
import logging
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

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


class DataCollector:
    """Improved data collector with parallel processing and guaranteed timing"""
    
    def __init__(self, cfg):
        self.cfg = cfg
        self.bucket = cfg["gcs_bucket"]
        self.interval = int(cfg.get("interval_seconds", 5))
        self.layers = cfg.get("layers", [5, 50, 100])
        self.publish_1m = int(cfg.get("publish_1min_minutes", 5))
        self.publish_5s = int(cfg.get("publish_5s_minutes", 60))
        
        self.exchanges_cfg = cfg["exchanges"]
        self.assets = cfg["assets"]
        
        # Initialize storage
        self.storage_backend = get_storage_backend(self.bucket)
        logger.info(f"Storage backend initialized for bucket: {self.bucket}")
        
        # Initialize exchange clients
        self.clients: Dict[str, Any] = {}
        self.quotes: Dict[str, str] = {}
        self._init_clients()
        
        # Tracking variables
        self.last_pub_1m: Dict[str, datetime] = {}
        self.last_pub_5s: Dict[str, datetime] = {}
        self.stats = {
            "total_fetches": 0,
            "successful_fetches": 0,
            "failed_fetches": 0,
            "last_success_time": None,
            "cycle_times": [],
            "asset_health": {}
        }
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=8)
        
        # Health monitor
        self.health_monitor = None
        
    def _init_clients(self):
        """Initialize exchange clients"""
        for e in self.exchanges_cfg:
            name = e["name"]
            self.quotes[name] = e["quote"]
            try:
                self.clients[name] = make_exchange(name)
                logger.info(f"Initialized exchange client: {name}")
            except Exception as e:
                logger.error(f"Failed to initialize {name}: {e}")
        
        # Load markets
        for name, client in self.clients.items():
            try:
                client.load_markets()
                market_count = len(getattr(client, 'markets', {}) or [])
                logger.info(f"Loaded markets: {name} ({market_count} symbols)")
            except Exception as e:
                logger.error(f"load_markets failed for {name}: {e}")
    
    def collect_single_asset(self, ex_name: str, asset: str, now: datetime, t_iso: str) -> Dict[str, Any]:
        """Collect data for a single exchange/asset pair"""
        result = {
            "exchange": ex_name,
            "asset": asset,
            "success": False,
            "error": None,
            "data": None,
            "timestamp": now
        }
        
        try:
            client = self.clients[ex_name]
            quote = self.quotes[ex_name]
            sym = symbol_for(ex_name, asset, quote)
            
            # Fetch order book data with timeout
            start_time = time.time()
            ob = client.fetch_order_book(sym, limit=200)
            fetch_time = time.time() - start_time
            
            # Validate order book data
            if not ob or not ob.get('bids') or not ob.get('asks'):
                result["error"] = "Invalid order book data"
                return result
                
            metrics = compute_metrics(ob, self.layers)
            
            # Validate metrics
            if not metrics or metrics.get("mid") is None:
                result["error"] = "Invalid metrics"
                return result
            
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
            
            # Save to storage
            path_keys = fmt_paths(self.cfg, ex_name, asset, now)
            append_jsonl_line(self.bucket, path_keys["five_sec_minute"], json.dumps(record, separators=(",", ":")))
            
            result["success"] = True
            result["data"] = record
            result["fetch_time"] = fetch_time
            
            logger.debug(f"✅ {ex_name} {asset}: ${metrics['mid']:.2f} ({fetch_time:.2f}s)")
            
        except (RateLimitExceeded, DDoSProtection) as e:
            result["error"] = f"Rate limit: {e}"
            logger.warning(f"Rate limit for {ex_name} {asset}: {e}")
            
        except ExchangeError as e:
            result["error"] = f"Exchange error: {e}"
            logger.warning(f"Exchange error {ex_name} {asset}: {e}")
            
        except Exception as e:
            result["error"] = f"Unexpected error: {e}"
            logger.error(f"Unexpected error {ex_name} {asset}: {e}")
            
        return result
    
    def collect_all_data(self, now: datetime) -> List[Dict[str, Any]]:
        """Collect data for all exchange/asset pairs in parallel"""
        t_iso = iso_utc(now)
        
        # Submit all collection tasks
        futures = []
        for ex_name in self.clients.keys():
            for asset in self.assets:
                future = self.executor.submit(self.collect_single_asset, ex_name, asset, now, t_iso)
                futures.append(future)
        
        # Collect results with timeout
        results = []
        for future in futures:
            try:
                result = future.result(timeout=30)  # 30 second timeout per asset
                results.append(result)
            except Exception as e:
                logger.error(f"Collection task failed: {e}")
                results.append({
                    "success": False,
                    "error": f"Task timeout: {e}",
                    "timestamp": now
                })
        
        return results
    
    def update_statistics(self, results: List[Dict[str, Any]]):
        """Update collection statistics"""
        successful = sum(1 for r in results if r["success"])
        failed = len(results) - successful
        
        self.stats["total_fetches"] += len(results)
        self.stats["successful_fetches"] += successful
        self.stats["failed_fetches"] += failed
        
        if successful > 0:
            self.stats["last_success_time"] = datetime.now(timezone.utc)
        
        # Update per-asset health tracking
        for result in results:
            if "exchange" in result and "asset" in result:
                key = f"{result['exchange']}:{result['asset']}"
                if key not in self.stats["asset_health"]:
                    self.stats["asset_health"][key] = {"success": 0, "failed": 0, "last_success": None}
                
                if result["success"]:
                    self.stats["asset_health"][key]["success"] += 1
                    self.stats["asset_health"][key]["last_success"] = result["timestamp"]
                else:
                    self.stats["asset_health"][key]["failed"] += 1
    
    def handle_publishing(self, now: datetime):
        """Handle 1-minute and 5-second data publishing"""
        for ex_name in self.clients.keys():
            for asset in self.assets:
                pair_key = f"{ex_name}:{asset}"
                
                # 1m near-live compose (every 5 minutes by default)
                if (self.last_pub_1m.get(pair_key) is None) or (
                    (now - self.last_pub_1m[pair_key]) >= timedelta(minutes=self.publish_1m)
                ):
                    try:
                        self.publish_1min_nearlive(ex_name, asset, now)
                        self.last_pub_1m[pair_key] = now
                        logger.info(f"📊 Published 1min data for {pair_key}")
                    except Exception as e:
                        logger.error(f"Failed to publish 1m {pair_key}: {e}")

                # 5s daily compose (every 60 minutes by default)
                if (self.last_pub_5s.get(pair_key) is None) or (
                    (now - self.last_pub_5s[pair_key]) >= timedelta(minutes=self.publish_5s)
                ):
                    try:
                        self.publish_5s_daily(ex_name, asset, now)
                        self.last_pub_5s[pair_key] = now
                        logger.info(f"📈 Published 5s daily data for {pair_key}")
                    except Exception as e:
                        logger.error(f"Failed to publish 5s {pair_key}: {e}")
    
    def publish_1min_nearlive(self, ex: str, asset: str, now: datetime):
        """Publish 1-minute aggregated data"""
        minutes_back = int(self.cfg.get("publish_1min_minutes", 5))
        end_minute = now.replace(second=0, microsecond=0)
        start_minute = end_minute - timedelta(minutes=minutes_back - 1)

        for i in range(minutes_back):
            m = start_minute + timedelta(minutes=i)
            paths = fmt_paths(self.cfg, ex, asset, m)
            src_5s = paths["five_sec_minute"]
            dst_1m_min = paths["one_min_minute"]

            text = download_text(self.bucket, src_5s)
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
            upload_text(self.bucket, dst_1m_min, json.dumps(row, separators=(",", ":")) + "\n")

        # Compose daily file
        day = now.strftime("%Y-%m-%d")
        prefix = f"{ex}/{asset}/1min/min/{day}/"
        sources = list_prefix(self.bucket, prefix)
        if sources:
            dest = fmt_paths(self.cfg, ex, asset, now)["one_min_daily"]
            compose_many(self.bucket, sources, dest)

    def publish_5s_daily(self, ex: str, asset: str, now: datetime):
        """Publish 5-second daily aggregated data"""
        day = now.strftime("%Y-%m-%d")
        prefix = f"{ex}/{asset}/5s/min/{day}/"
        sources = list_prefix(self.bucket, prefix)
        if sources:
            dest = fmt_paths(self.cfg, ex, asset, now)["five_sec_daily"]
            compose_many(self.bucket, sources, dest)
    
    def log_health_status(self):
        """Log current health status"""
        if self.stats["total_fetches"] > 0:
            success_rate = (self.stats["successful_fetches"] / self.stats["total_fetches"]) * 100
            
            # Calculate average cycle time
            avg_cycle_time = sum(self.stats["cycle_times"][-10:]) / min(len(self.stats["cycle_times"]), 10) if self.stats["cycle_times"] else 0
            
            logger.info(f"📊 Health: {self.stats['successful_fetches']}/{self.stats['total_fetches']} success ({success_rate:.1f}%) | Avg cycle: {avg_cycle_time:.1f}s")
            
            # Log per-asset health
            unhealthy_assets = []
            for asset_key, health in self.stats["asset_health"].items():
                total = health["success"] + health["failed"]
                if total > 0:
                    asset_success_rate = (health["success"] / total) * 100
                    if asset_success_rate < 80:  # Less than 80% success
                        unhealthy_assets.append(f"{asset_key}({asset_success_rate:.0f}%)")
            
            if unhealthy_assets:
                logger.warning(f"⚠️  Unhealthy assets: {', '.join(unhealthy_assets)}")
    
    def run(self):
        """Main collection loop"""
        logger.info("🚀 Starting improved crypto data collector...")
        
        # Run one-time header fix
        try:
            from startup_fix import run_header_fix_once
            run_header_fix_once()
        except Exception as e:
            logger.warning(f"Header fix failed (non-critical): {e}")
        
        # Start health monitor
        try:
            from realtime_health_monitor import start_health_monitor
            exchanges = list(self.clients.keys())
            self.health_monitor = start_health_monitor(self.bucket, exchanges, self.assets)
        except Exception as e:
            logger.warning(f"Failed to start health monitor: {e}")

        try:
            logger.info(f"📡 Collection interval: {self.interval}s")
            logger.info(f"📊 Tracking: {len(self.clients)} exchanges × {len(self.assets)} assets = {len(self.clients) * len(self.assets)} pairs")
            
            cycle_count = 0
            
            while True:
                cycle_start = time.time()
                now = datetime.now(timezone.utc)
                
                # Collect all data in parallel
                logger.debug(f"🔄 Cycle {cycle_count + 1} starting...")
                results = self.collect_all_data(now)
                
                # Update statistics
                self.update_statistics(results)
                
                # Handle publishing
                self.handle_publishing(now)
                
                # Calculate cycle time
                cycle_time = time.time() - cycle_start
                self.stats["cycle_times"].append(cycle_time)
                
                # Keep only last 100 cycle times
                if len(self.stats["cycle_times"]) > 100:
                    self.stats["cycle_times"] = self.stats["cycle_times"][-100:]
                
                cycle_count += 1
                
                # Log health status every 10 cycles
                if cycle_count % 10 == 0:
                    self.log_health_status()
                
                # Sleep for remaining interval time
                sleep_time = max(0.1, self.interval - cycle_time)
                if cycle_time > self.interval:
                    logger.warning(f"⚠️  Cycle took {cycle_time:.1f}s (longer than {self.interval}s interval)")
                
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"💥 Unexpected error in main loop: {e}")
            traceback.print_exc()
        finally:
            logger.info("🧹 Cleaning up...")
            
            # Stop health monitor
            if self.health_monitor:
                try:
                    self.health_monitor.stop()
                except Exception as e:
                    logger.warning(f"Error stopping health monitor: {e}")
            
            self.executor.shutdown(wait=True)
            for c in self.clients.values():
                try:
                    if hasattr(c, "close"):
                        c.close()
                except Exception as e:
                    logger.warning(f"Error closing client: {e}")
            logger.info("✅ Crypto data collector stopped.")


def main():
    cfg = load_config()
    collector = DataCollector(cfg)
    collector.run()


if __name__ == "__main__":
    main()