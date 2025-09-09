#!/usr/bin/env python3
"""
Guaranteed 1-minute data scheduler - ensures data is collected every minute regardless of main loop issues.
"""

import time
import threading
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Set
import json

from improved_logger import DataCollector, load_config

logger = logging.getLogger(__name__)

class GuaranteedMinuteScheduler:
    """Ensures 1-minute data is collected even if main loop has issues"""
    
    def __init__(self, collector: DataCollector):
        self.collector = collector
        self.running = False
        self.thread = None
        self.last_minute_collected: Set[str] = set()  # Track which minutes we've collected
        
    def start(self):
        """Start the guaranteed minute scheduler"""
        if self.running:
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        logger.info("⏰ Started guaranteed minute scheduler")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("⏰ Stopped guaranteed minute scheduler")
    
    def _run_scheduler(self):
        """Main scheduler loop - runs every 30 seconds to check for missed minutes"""
        while self.running:
            try:
                now = datetime.now(timezone.utc)
                current_minute = now.replace(second=0, microsecond=0)
                minute_key = current_minute.strftime("%Y-%m-%d %H:%M")
                
                # Check if we need to collect data for the previous minute
                prev_minute = current_minute - timedelta(minutes=1)
                prev_minute_key = prev_minute.strftime("%Y-%m-%d %H:%M")
                
                # Only collect if we're past the minute boundary and haven't collected yet
                if now.second >= 30 and prev_minute_key not in self.last_minute_collected:
                    logger.info(f"⏰ Ensuring data collection for {prev_minute_key}")
                    self._ensure_minute_data(prev_minute)
                    self.last_minute_collected.add(prev_minute_key)
                    
                    # Clean up old entries (keep last 60 minutes)
                    cutoff_time = current_minute - timedelta(minutes=60)
                    cutoff_key = cutoff_time.strftime("%Y-%m-%d %H:%M")
                    self.last_minute_collected = {
                        k for k in self.last_minute_collected 
                        if k >= cutoff_key
                    }
                
            except Exception as e:
                logger.error(f"Error in minute scheduler: {e}")
            
            time.sleep(30)  # Check every 30 seconds
    
    def _ensure_minute_data(self, target_minute: datetime):
        """Ensure we have data for the target minute"""
        
        # For each exchange/asset pair, check if we have recent data
        for ex_name in self.collector.clients.keys():
            for asset in self.collector.assets:
                try:
                    # Check if we have data in the target minute
                    if not self._has_recent_data(ex_name, asset, target_minute):
                        logger.warning(f"⚠️  Missing data for {ex_name} {asset} at {target_minute.strftime('%H:%M')}")
                        
                        # Try to collect data now
                        self._force_collect_data(ex_name, asset, target_minute)
                        
                except Exception as e:
                    logger.error(f"Error checking data for {ex_name} {asset}: {e}")
    
    def _has_recent_data(self, ex_name: str, asset: str, target_minute: datetime) -> bool:
        """Check if we have data for the target minute"""
        try:
            from storage import download_text
            
            from improved_logger import fmt_paths
            paths = fmt_paths(self.collector.cfg, ex_name, asset, target_minute)
            minute_file = paths["five_sec_minute"]
            
            # Check if the minute file exists and has data
            text = download_text(self.collector.bucket, minute_file)
            if text and text.strip():
                lines = [line for line in text.strip().split('\n') if line.strip()]
                if len(lines) > 0:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking recent data: {e}")
            return False
    
    def _force_collect_data(self, ex_name: str, asset: str, target_minute: datetime):
        """Force collection of data for a specific exchange/asset"""
        try:
            # Use the current time but with target minute timestamp
            now = datetime.now(timezone.utc)
            target_iso = target_minute.replace(second=30).isoformat().replace("+00:00", "Z")
            
            result = self.collector.collect_single_asset(ex_name, asset, target_minute, target_iso)
            
            if result["success"]:
                logger.info(f"✅ Force collected data for {ex_name} {asset}")
            else:
                logger.warning(f"❌ Failed to force collect {ex_name} {asset}: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error force collecting data: {e}")


class ImprovedDataCollectorWithScheduler(DataCollector):
    """Enhanced data collector with guaranteed minute scheduler"""
    
    def __init__(self, cfg):
        super().__init__(cfg)
        self.scheduler = GuaranteedMinuteScheduler(self)
    
    def run(self):
        """Run with guaranteed minute scheduler"""
        try:
            # Start the scheduler
            self.scheduler.start()
            
            # Run the normal collection loop
            super().run()
            
        finally:
            # Stop the scheduler
            self.scheduler.stop()


def main():
    """Main entry point with improved collector"""
    cfg = load_config()
    collector = ImprovedDataCollectorWithScheduler(cfg)
    collector.run()


if __name__ == "__main__":
    main()