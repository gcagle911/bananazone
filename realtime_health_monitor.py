#!/usr/bin/env python3
"""
Real-time health monitoring that runs alongside the data collector.
Logs health status and alerts for missing data.
"""

import time
import threading
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List
import json

logger = logging.getLogger(__name__)

class RealtimeHealthMonitor:
    """Monitor data collection health in real-time"""
    
    def __init__(self, bucket_name: str, exchanges: List[str], assets: List[str]):
        self.bucket_name = bucket_name
        self.exchanges = exchanges
        self.assets = assets
        self.running = False
        self.thread = None
        
        # Track health metrics
        self.health_stats = {
            "last_check": None,
            "total_files_checked": 0,
            "healthy_files": 0,
            "stale_files": 0,
            "missing_files": 0,
            "alerts": []
        }
    
    def start(self):
        """Start the health monitor"""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        logger.info("🏥 Started real-time health monitor")
    
    def stop(self):
        """Stop the health monitor"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("🏥 Stopped health monitor")
    
    def _monitor_loop(self):
        """Main monitoring loop - checks health every 2 minutes"""
        while self.running:
            try:
                self._perform_health_check()
                time.sleep(120)  # Check every 2 minutes
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
                time.sleep(60)  # Retry in 1 minute on error
    
    def _perform_health_check(self):
        """Perform a quick health check"""
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        
        self.health_stats["last_check"] = now.isoformat()
        self.health_stats["total_files_checked"] = 0
        self.health_stats["healthy_files"] = 0
        self.health_stats["stale_files"] = 0
        self.health_stats["missing_files"] = 0
        self.health_stats["alerts"] = []
        
        # Check 1-minute files (most important for charts)
        for exchange in self.exchanges:
            for asset in self.assets:
                self._check_asset_health(exchange, asset, today, now)
        
        # Log summary
        total = self.health_stats["total_files_checked"]
        healthy = self.health_stats["healthy_files"]
        stale = self.health_stats["stale_files"]
        missing = self.health_stats["missing_files"]
        
        if total > 0:
            health_percentage = (healthy / total) * 100
            
            if health_percentage >= 90:
                logger.info(f"💚 Health: {healthy}/{total} files healthy ({health_percentage:.0f}%)")
            elif health_percentage >= 70:
                logger.warning(f"💛 Health: {healthy}/{total} files healthy ({health_percentage:.0f}%) - {stale} stale, {missing} missing")
            else:
                logger.error(f"💔 Health: {healthy}/{total} files healthy ({health_percentage:.0f}%) - {stale} stale, {missing} missing")
        
        # Log specific alerts
        for alert in self.health_stats["alerts"][-5:]:  # Show last 5 alerts
            logger.warning(f"🚨 {alert}")
    
    def _check_asset_health(self, exchange: str, asset: str, date: str, now: datetime):
        """Check health of a specific asset"""
        try:
            import requests
            
            # Check 1-minute data file
            url = f"https://storage.googleapis.com/{self.bucket_name}/{exchange}/{asset}/1min/{date}.jsonl"
            
            self.health_stats["total_files_checked"] += 1
            
            try:
                response = requests.get(url, timeout=10)
                
                if response.status_code == 404:
                    self.health_stats["missing_files"] += 1
                    self.health_stats["alerts"].append(f"Missing: {exchange} {asset} 1min data")
                    return
                elif response.status_code != 200:
                    self.health_stats["alerts"].append(f"HTTP {response.status_code}: {exchange} {asset}")
                    return
                
                # Parse and check data freshness
                text = response.text.strip()
                if not text:
                    self.health_stats["missing_files"] += 1
                    self.health_stats["alerts"].append(f"Empty file: {exchange} {asset}")
                    return
                
                lines = [line for line in text.split('\n') if line.strip()]
                if not lines:
                    self.health_stats["missing_files"] += 1
                    self.health_stats["alerts"].append(f"No data: {exchange} {asset}")
                    return
                
                # Check last record timestamp
                try:
                    last_record = json.loads(lines[-1])
                    last_timestamp = datetime.fromisoformat(last_record['t'].replace('Z', '+00:00'))
                    data_age_minutes = (now - last_timestamp).total_seconds() / 60
                    
                    if data_age_minutes <= 5:  # Data is fresh (within 5 minutes)
                        self.health_stats["healthy_files"] += 1
                    elif data_age_minutes <= 15:  # Slightly stale but acceptable
                        self.health_stats["stale_files"] += 1
                        if data_age_minutes > 10:  # Only alert if >10 minutes
                            self.health_stats["alerts"].append(f"Stale data: {exchange} {asset} ({data_age_minutes:.0f}min old)")
                    else:  # Very stale
                        self.health_stats["stale_files"] += 1
                        self.health_stats["alerts"].append(f"Very stale: {exchange} {asset} ({data_age_minutes:.0f}min old)")
                    
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    self.health_stats["alerts"].append(f"Parse error: {exchange} {asset} - {e}")
                    
            except requests.exceptions.RequestException as e:
                self.health_stats["alerts"].append(f"Network error: {exchange} {asset} - {e}")
                
        except Exception as e:
            logger.error(f"Error checking {exchange} {asset}: {e}")
    
    def get_health_summary(self) -> Dict:
        """Get current health summary"""
        return self.health_stats.copy()


def start_health_monitor(bucket_name: str, exchanges: List[str], assets: List[str]) -> RealtimeHealthMonitor:
    """Start and return a health monitor instance"""
    monitor = RealtimeHealthMonitor(bucket_name, exchanges, assets)
    monitor.start()
    return monitor


if __name__ == "__main__":
    # Standalone health monitor
    import yaml
    
    # Load config
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    
    bucket = cfg["gcs_bucket"]
    exchanges = [e["name"] for e in cfg["exchanges"]]
    assets = cfg["assets"]
    
    monitor = start_health_monitor(bucket, exchanges, assets)
    
    try:
        print("🏥 Running standalone health monitor. Press Ctrl+C to stop.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n🛑 Stopping health monitor...")
        monitor.stop()