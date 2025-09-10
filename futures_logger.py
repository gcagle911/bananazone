#!/usr/bin/env python3
"""
Futures/Leverage data collector for Bybit, Upbit, OKX, and Coinbase futures.
Saves to same GCS bucket but under 'futures/' path structure.
"""

import json
import time
import traceback
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

import yaml
from ccxt.base.errors import RateLimitExceeded, DDoSProtection, ExchangeError

from futures_exchanges import make_futures_exchange, get_futures_symbol, test_exchange_connection, validate_exchange_health
from metrics import compute_metrics
from storage import append_jsonl_line, download_text, upload_text, list_prefix, compose_many, get_storage_backend

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('futures_logger.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_futures_config(path: str = "futures_config.yaml") -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def iso_utc(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def fmt_futures_paths(cfg, ex: str, asset: str, t: datetime) -> Dict[str, str]:
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
        "data_type": "futures",  # Mark as futures data
    }
    for f in fields:
        vals = [
            r.get(f)
            for r in records
            if isinstance(r.get(f), (int, float)) and r.get(f) == r.get(f)
        ]
        agg[f] = (sum(vals) / len(vals)) if vals else None
    return agg


class FuturesDataCollector:
    """Futures data collector with robust error handling"""
    
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
        logger.info(f"Futures storage backend initialized for bucket: {self.bucket}")
        
        # Initialize exchange clients with error handling
        self.clients: Dict[str, Any] = {}
        self.quotes: Dict[str, str] = {}
        self.symbols: Dict[str, Dict[str, str]] = {}  # exchange -> asset -> symbol
        self.failed_exchanges: set = set()
        
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
            "exchange_health": {}
        }
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=8)
        
    def _init_clients(self):
        """Initialize futures exchange clients with robust error handling"""
        
        logger.info("🚀 Initializing Futures Exchange Clients")
        logger.info("=" * 50)
        
        for e in self.exchanges_cfg:
            name = e["name"]
            if not e.get("enabled", True):
                logger.info(f"⏸️  {name}: Disabled in config")
                continue
                
            self.quotes[name] = e["quote"]
            
            try:
                logger.info(f"🔌 Connecting to {name}...")
                client = make_futures_exchange(name)
                
                # Test connection and get available symbols
                available_symbols = test_exchange_connection(
                    name, client, self.assets, self.quotes[name]
                )
                
                if available_symbols:
                    self.clients[name] = client
                    self.symbols[name] = available_symbols
                    logger.info(f"✅ {name}: Ready with {len(available_symbols)} assets")
                else:
                    logger.error(f"❌ {name}: No assets available, skipping")
                    self.failed_exchanges.add(name)
                    
            except Exception as e:
                logger.error(f"❌ {name}: Failed to initialize - {e}")
                self.failed_exchanges.add(name)
        
        active_exchanges = len(self.clients)
        failed_exchanges = len(self.failed_exchanges)
        
        logger.info(f"\n📊 Initialization Summary:")
        logger.info(f"   ✅ Active exchanges: {active_exchanges}")
        logger.info(f"   ❌ Failed exchanges: {failed_exchanges}")
        
        if active_exchanges == 0:
            raise Exception("No exchanges available! Check configuration and network connectivity.")
    
    def collect_single_futures_asset(self, ex_name: str, asset: str, now: datetime, t_iso: str) -> Dict[str, Any]:
        """Collect futures data for a single exchange/asset pair"""
        
        result = {
            "exchange": ex_name,
            "asset": asset,
            "success": False,
            "error": None,
            "data": None,
            "timestamp": now,
            "data_type": "futures"
        }
        
        if ex_name in self.failed_exchanges:
            result["error"] = "Exchange marked as failed"
            return result
        
        try:
            client = self.clients[ex_name]
            symbol = self.symbols[ex_name][asset]
            
            # Fetch order book data with timeout
            start_time = time.time()
            ob = client.fetch_order_book(symbol, limit=200)
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
                "data_type": "futures",
                "symbol": symbol,  # Include the actual symbol used
                "mid": metrics["mid"],
                "spread_L5_pct": metrics["spread_L5_pct"],
                "spread_L50_pct": metrics["spread_L50_pct"],
                "spread_L100_pct": metrics["spread_L100_pct"],
                "vol_L50_bids": metrics["vol_L50_bids"],
                "vol_L50_asks": metrics["vol_L50_asks"],
                "depth_bids": metrics["depth_bids"],
                "depth_asks": metrics["depth_asks"],
            }
            
            # Save to storage (futures path structure)
            path_keys = fmt_futures_paths(self.cfg, ex_name, asset, now)
            append_jsonl_line(self.bucket, path_keys["five_sec_minute"], json.dumps(record, separators=(",", ":")))
            
            result["success"] = True
            result["data"] = record
            result["fetch_time"] = fetch_time
            
            logger.debug(f"✅ {ex_name} {asset} futures: ${metrics['mid']:.2f} ({fetch_time:.2f}s)")
            
        except (RateLimitExceeded, DDoSProtection) as e:
            result["error"] = f"Rate limit: {e}"
            logger.warning(f"🚫 Rate limit for {ex_name} {asset}: {e}")
            
        except ExchangeError as e:
            result["error"] = f"Exchange error: {e}"
            logger.warning(f"💥 Exchange error {ex_name} {asset}: {e}")
            
            # Mark exchange as failed if too many errors
            self._check_exchange_health(ex_name)
            
        except Exception as e:
            result["error"] = f"Unexpected error: {e}"
            logger.error(f"💥 Unexpected error {ex_name} {asset}: {e}")
            
        return result
    
    def _check_exchange_health(self, ex_name: str):
        """Check if exchange should be temporarily disabled due to errors"""
        
        if ex_name not in self.stats["exchange_health"]:
            self.stats["exchange_health"][ex_name] = {"errors": 0, "last_error": None}
        
        self.stats["exchange_health"][ex_name]["errors"] += 1
        self.stats["exchange_health"][ex_name]["last_error"] = datetime.now(timezone.utc)
        
        # Disable exchange if too many consecutive errors
        if self.stats["exchange_health"][ex_name]["errors"] > 10:
            logger.error(f"🚫 Disabling {ex_name} due to too many errors")
            self.failed_exchanges.add(ex_name)
            if ex_name in self.clients:
                del self.clients[ex_name]
    
    def collect_all_futures_data(self, now: datetime) -> List[Dict[str, Any]]:
        """Collect futures data for all exchange/asset pairs in parallel"""
        t_iso = iso_utc(now)
        
        # Submit all collection tasks
        futures = []
        for ex_name, symbols in self.symbols.items():
            if ex_name in self.failed_exchanges:
                continue
                
            for asset in symbols.keys():
                future = self.executor.submit(self.collect_single_futures_asset, ex_name, asset, now, t_iso)
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
    
    def handle_publishing(self, now: datetime):
        """Handle 1-minute and 5-second futures data publishing"""
        for ex_name, symbols in self.symbols.items():
            if ex_name in self.failed_exchanges:
                continue
                
            for asset in symbols.keys():
                pair_key = f"{ex_name}:{asset}:futures"
                
                # 1m near-live compose
                if (self.last_pub_1m.get(pair_key) is None) or (
                    (now - self.last_pub_1m[pair_key]) >= timedelta(minutes=self.publish_1m)
                ):
                    try:
                        self.publish_1min_nearlive(ex_name, asset, now)
                        self.last_pub_1m[pair_key] = now
                        logger.info(f"📊 Published 1min futures data for {pair_key}")
                    except Exception as e:
                        logger.error(f"Failed to publish 1m futures {pair_key}: {e}")

                # 5s daily compose
                if (self.last_pub_5s.get(pair_key) is None) or (
                    (now - self.last_pub_5s[pair_key]) >= timedelta(minutes=self.publish_5s)
                ):
                    try:
                        self.publish_5s_daily(ex_name, asset, now)
                        self.last_pub_5s[pair_key] = now
                        logger.info(f"📈 Published 5s futures daily data for {pair_key}")
                    except Exception as e:
                        logger.error(f"Failed to publish 5s futures {pair_key}: {e}")
    
    def publish_1min_nearlive(self, ex: str, asset: str, now: datetime):
        """Publish 1-minute aggregated futures data"""
        minutes_back = int(self.cfg.get("publish_1min_minutes", 5))
        end_minute = now.replace(second=0, microsecond=0)
        start_minute = end_minute - timedelta(minutes=minutes_back - 1)

        for i in range(minutes_back):
            m = start_minute + timedelta(minutes=i)
            paths = fmt_futures_paths(self.cfg, ex, asset, m)
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
        prefix = f"futures/{ex}/{asset}/1min/min/{day}/"
        sources = list_prefix(self.bucket, prefix)
        if sources:
            dest = fmt_futures_paths(self.cfg, ex, asset, now)["one_min_daily"]
            compose_many(self.bucket, sources, dest)

    def publish_5s_daily(self, ex: str, asset: str, now: datetime):
        """Publish 5-second daily aggregated futures data"""
        day = now.strftime("%Y-%m-%d")
        prefix = f"futures/{ex}/{asset}/5s/min/{day}/"
        sources = list_prefix(self.bucket, prefix)
        if sources:
            dest = fmt_futures_paths(self.cfg, ex, asset, now)["five_sec_daily"]
            compose_many(self.bucket, sources, dest)
    
    def log_health_status(self):
        """Log current health status"""
        if self.stats["total_fetches"] > 0:
            success_rate = (self.stats["successful_fetches"] / self.stats["total_fetches"]) * 100
            
            avg_cycle_time = sum(self.stats["cycle_times"][-10:]) / min(len(self.stats["cycle_times"]), 10) if self.stats["cycle_times"] else 0
            
            active_exchanges = len([ex for ex in self.clients.keys() if ex not in self.failed_exchanges])
            
            logger.info(f"📊 Futures Health: {self.stats['successful_fetches']}/{self.stats['total_fetches']} success ({success_rate:.1f}%) | "
                       f"Active exchanges: {active_exchanges} | Avg cycle: {avg_cycle_time:.1f}s")
            
            if self.failed_exchanges:
                logger.warning(f"⚠️  Failed exchanges: {', '.join(self.failed_exchanges)}")
    
    def run(self):
        """Main futures collection loop"""
        logger.info("🚀 Starting Futures Data Collector...")
        
        try:
            logger.info(f"📡 Collection interval: {self.interval}s")
            logger.info(f"📊 Tracking: {len(self.clients)} exchanges × avg assets = futures pairs")
            
            cycle_count = 0
            
            while True:
                cycle_start = time.time()
                now = datetime.now(timezone.utc)
                
                # Collect all futures data in parallel
                logger.debug(f"🔄 Futures cycle {cycle_count + 1} starting...")
                results = self.collect_all_futures_data(now)
                
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
                    logger.warning(f"⚠️  Futures cycle took {cycle_time:.1f}s (longer than {self.interval}s interval)")
                
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, shutting down futures collector...")
        except Exception as e:
            logger.error(f"💥 Unexpected error in futures main loop: {e}")
            traceback.print_exc()
        finally:
            logger.info("🧹 Cleaning up futures collector...")
            self.executor.shutdown(wait=True)
            for c in self.clients.values():
                try:
                    if hasattr(c, "close"):
                        c.close()
                except Exception as e:
                    logger.warning(f"Error closing futures client: {e}")
            logger.info("✅ Futures data collector stopped.")


def main():
    cfg = load_futures_config()
    collector = FuturesDataCollector(cfg)
    collector.run()


if __name__ == "__main__":
    main()