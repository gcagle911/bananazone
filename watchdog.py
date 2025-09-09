#!/usr/bin/env python3
"""
Watchdog script to monitor data collection and alert if it stops.
Can be run periodically to check health and send alerts.
"""

import json
import requests
import time
from datetime import datetime, timezone, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_data_freshness(max_age_minutes=6):
    """Check if data is fresh across all assets"""
    
    bucket_name = "bananazone"
    exchanges = ["coinbase", "kraken"]
    assets = ["BTC", "ETH"]  # Check key assets
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    
    stale_count = 0
    total_count = 0
    issues = []
    
    for exchange in exchanges:
        for asset in assets:
            total_count += 1
            url = f"https://storage.googleapis.com/{bucket_name}/{exchange}/{asset}/1min/{date}.jsonl"
            
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    text = response.text.strip()
                    if text:
                        lines = [line for line in text.split('\n') if line.strip()]
                        if lines:
                            try:
                                last_record = json.loads(lines[-1])
                                last_time = datetime.fromisoformat(last_record['t'].replace('Z', '+00:00'))
                                age_minutes = (now - last_time).total_seconds() / 60
                                
                                if age_minutes > max_age_minutes:
                                    stale_count += 1
                                    issues.append(f"{exchange} {asset}: {age_minutes:.1f}min old")
                                    
                            except Exception as e:
                                stale_count += 1
                                issues.append(f"{exchange} {asset}: Parse error")
                        else:
                            stale_count += 1
                            issues.append(f"{exchange} {asset}: Empty file")
                    else:
                        stale_count += 1
                        issues.append(f"{exchange} {asset}: No content")
                else:
                    stale_count += 1
                    issues.append(f"{exchange} {asset}: HTTP {response.status_code}")
                    
            except Exception as e:
                stale_count += 1
                issues.append(f"{exchange} {asset}: Network error")
    
    is_healthy = stale_count == 0
    health_percentage = ((total_count - stale_count) / total_count) * 100 if total_count > 0 else 0
    
    return {
        "healthy": is_healthy,
        "health_percentage": health_percentage,
        "stale_count": stale_count,
        "total_count": total_count,
        "issues": issues,
        "check_time": now.isoformat()
    }

def send_alert(health_status):
    """Send alert about unhealthy data collection"""
    
    logger.error("🚨 DATA COLLECTION ALERT")
    logger.error(f"   Health: {health_status['health_percentage']:.0f}% ({health_status['total_count'] - health_status['stale_count']}/{health_status['total_count']} healthy)")
    logger.error(f"   Issues:")
    
    for issue in health_status['issues'][:5]:  # Show first 5 issues
        logger.error(f"     • {issue}")
    
    if len(health_status['issues']) > 5:
        logger.error(f"     ... and {len(health_status['issues']) - 5} more issues")
    
    logger.error(f"   Checked at: {health_status['check_time']}")
    logger.error(f"   🔧 Recommended actions:")
    logger.error(f"     1. Check Render deployment logs")
    logger.error(f"     2. Restart the service if needed")
    logger.error(f"     3. Monitor for recovery")

def main():
    """Main watchdog check"""
    
    logger.info("🐕 Watchdog: Checking data collection health...")
    
    health_status = check_data_freshness()
    
    if health_status["healthy"]:
        logger.info(f"✅ Data collection is healthy ({health_status['health_percentage']:.0f}%)")
    elif health_status["health_percentage"] >= 50:
        logger.warning(f"⚠️  Data collection degraded ({health_status['health_percentage']:.0f}%)")
        logger.warning(f"   Issues: {', '.join(health_status['issues'][:3])}")
    else:
        send_alert(health_status)
    
    return health_status["healthy"]

if __name__ == "__main__":
    healthy = main()
    exit(0 if healthy else 1)