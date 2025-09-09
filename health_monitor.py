#!/usr/bin/env python3
"""
Comprehensive health monitoring for crypto data collection.
Checks data freshness, gaps, and collection frequency across all assets.
"""

import json
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import time

def analyze_data_health(bucket_name="bananazone", date=None):
    """Analyze the health of data collection across all assets and exchanges"""
    
    if not date:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    print(f"🏥 Health Check for {date}")
    print("=" * 50)
    
    exchanges = ["coinbase", "kraken"]
    assets = ["BTC", "ETH", "ADA", "XRP"]
    timeframes = ["1min", "5s"]
    
    health_report = {
        "overall_status": "healthy",
        "issues": [],
        "stats": {},
        "last_updates": {},
        "data_gaps": {},
        "recommendations": []
    }
    
    for exchange in exchanges:
        for asset in assets:
            for timeframe in timeframes:
                print(f"\n📊 Checking {exchange} {asset} {timeframe}...")
                
                url = f"https://storage.googleapis.com/{bucket_name}/{exchange}/{asset}/{timeframe}/{date}.jsonl"
                
                try:
                    response = requests.get(url, timeout=10)
                    
                    if response.status_code == 404:
                        issue = f"❌ Missing file: {exchange}/{asset}/{timeframe}"
                        print(f"   {issue}")
                        health_report["issues"].append(issue)
                        health_report["overall_status"] = "unhealthy"
                        continue
                    elif response.status_code != 200:
                        issue = f"⚠️  HTTP {response.status_code}: {exchange}/{asset}/{timeframe}"
                        print(f"   {issue}")
                        health_report["issues"].append(issue)
                        continue
                    
                    # Parse data
                    text = response.text.strip()
                    if not text:
                        issue = f"📄 Empty file: {exchange}/{asset}/{timeframe}"
                        print(f"   {issue}")
                        health_report["issues"].append(issue)
                        continue
                    
                    lines = text.split('\n')
                    records = []
                    
                    for line in lines:
                        if line.strip():
                            try:
                                records.append(json.loads(line.strip()))
                            except json.JSONDecodeError as e:
                                print(f"   ⚠️  JSON parse error in line: {e}")
                    
                    if not records:
                        issue = f"📄 No valid records: {exchange}/{asset}/{timeframe}"
                        print(f"   {issue}")
                        health_report["issues"].append(issue)
                        continue
                    
                    # Analyze timestamps
                    timestamps = [datetime.fromisoformat(r['t'].replace('Z', '+00:00')) for r in records]
                    timestamps.sort()
                    
                    # Stats
                    first_time = timestamps[0]
                    last_time = timestamps[-1]
                    total_records = len(records)
                    
                    # Expected interval
                    expected_interval = 60 if timeframe == "1min" else 5  # seconds
                    
                    # Find gaps
                    gaps = []
                    for i in range(1, len(timestamps)):
                        time_diff = (timestamps[i] - timestamps[i-1]).total_seconds()
                        if time_diff > expected_interval * 1.5:  # Allow 50% tolerance
                            gaps.append({
                                'start': timestamps[i-1].isoformat(),
                                'end': timestamps[i].isoformat(),
                                'duration_minutes': time_diff / 60
                            })
                    
                    # Data freshness (how old is the latest data?)
                    now = datetime.now(timezone.utc)
                    data_age_minutes = (now - last_time).total_seconds() / 60
                    
                    # Store results
                    key = f"{exchange}_{asset}_{timeframe}"
                    health_report["stats"][key] = {
                        "total_records": total_records,
                        "first_timestamp": first_time.isoformat(),
                        "last_timestamp": last_time.isoformat(),
                        "data_age_minutes": data_age_minutes,
                        "gaps_count": len(gaps),
                        "total_gap_minutes": sum(g['duration_minutes'] for g in gaps)
                    }
                    
                    health_report["last_updates"][key] = last_time.isoformat()
                    health_report["data_gaps"][key] = gaps
                    
                    # Print summary
                    print(f"   ✅ {total_records} records")
                    print(f"   📅 Range: {first_time.strftime('%H:%M')} → {last_time.strftime('%H:%M')}")
                    print(f"   🕐 Data age: {data_age_minutes:.1f} minutes")
                    
                    if gaps:
                        print(f"   ⚠️  {len(gaps)} gaps found:")
                        for gap in gaps[:3]:  # Show first 3 gaps
                            gap_start = datetime.fromisoformat(gap['start']).strftime('%H:%M')
                            gap_end = datetime.fromisoformat(gap['end']).strftime('%H:%M')
                            print(f"      🕳️  {gap_start} → {gap_end} ({gap['duration_minutes']:.1f}min)")
                        if len(gaps) > 3:
                            print(f"      ... and {len(gaps) - 3} more gaps")
                    
                    # Health checks
                    if data_age_minutes > 10:  # Data older than 10 minutes
                        issue = f"🕐 Stale data: {exchange}/{asset}/{timeframe} ({data_age_minutes:.1f}min old)"
                        health_report["issues"].append(issue)
                        if health_report["overall_status"] == "healthy":
                            health_report["overall_status"] = "degraded"
                    
                    if len(gaps) > 5:  # More than 5 gaps
                        issue = f"🕳️  Too many gaps: {exchange}/{asset}/{timeframe} ({len(gaps)} gaps)"
                        health_report["issues"].append(issue)
                        if health_report["overall_status"] == "healthy":
                            health_report["overall_status"] = "degraded"
                    
                    if total_records < 10:  # Very few records
                        issue = f"📉 Low data volume: {exchange}/{asset}/{timeframe} ({total_records} records)"
                        health_report["issues"].append(issue)
                        if health_report["overall_status"] == "healthy":
                            health_report["overall_status"] = "degraded"
                
                except requests.exceptions.RequestException as e:
                    issue = f"🌐 Network error: {exchange}/{asset}/{timeframe} - {e}"
                    print(f"   {issue}")
                    health_report["issues"].append(issue)
                    health_report["overall_status"] = "unhealthy"
                
                except Exception as e:
                    issue = f"💥 Unexpected error: {exchange}/{asset}/{timeframe} - {e}"
                    print(f"   {issue}")
                    health_report["issues"].append(issue)
                    health_report["overall_status"] = "unhealthy"
    
    # Generate recommendations
    if health_report["overall_status"] != "healthy":
        print(f"\n🚨 Overall Status: {health_report['overall_status'].upper()}")
        print(f"📋 Issues found: {len(health_report['issues'])}")
        
        for issue in health_report["issues"]:
            print(f"   • {issue}")
        
        # Generate specific recommendations
        if any("Missing file" in issue for issue in health_report["issues"]):
            health_report["recommendations"].append("Check if Render deployment is running and GCS credentials are valid")
        
        if any("Stale data" in issue for issue in health_report["issues"]):
            health_report["recommendations"].append("Check if the data collection process is still running")
        
        if any("gaps" in issue for issue in health_report["issues"]):
            health_report["recommendations"].append("Check for rate limiting or network issues with exchanges")
        
        if any("Low data volume" in issue for issue in health_report["issues"]):
            health_report["recommendations"].append("Verify the data collection interval and publishing frequency")
        
        print(f"\n💡 Recommendations:")
        for i, rec in enumerate(health_report["recommendations"], 1):
            print(f"   {i}. {rec}")
    
    else:
        print(f"\n✅ Overall Status: HEALTHY")
        print(f"🎉 All systems operational!")
    
    return health_report

def monitor_live_updates(bucket_name="bananazone", duration_minutes=5):
    """Monitor live updates for a few minutes to check real-time data flow"""
    
    print(f"\n🔄 Live Monitoring for {duration_minutes} minutes...")
    print("=" * 40)
    
    # Track one file for live monitoring
    exchange, asset, timeframe = "coinbase", "BTC", "1min"
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://storage.googleapis.com/{bucket_name}/{exchange}/{asset}/{timeframe}/{date}.jsonl"
    
    last_record_count = 0
    start_time = time.time()
    
    while (time.time() - start_time) < (duration_minutes * 60):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                lines = [line for line in response.text.strip().split('\n') if line.strip()]
                current_count = len(lines)
                
                if current_count > last_record_count:
                    new_records = current_count - last_record_count
                    if lines:
                        try:
                            last_record = json.loads(lines[-1])
                            timestamp = last_record['t']
                            price = last_record['mid']
                            print(f"📈 +{new_records} records | Latest: {timestamp} | BTC: ${price:.2f}")
                        except:
                            print(f"📈 +{new_records} records added")
                    last_record_count = current_count
                else:
                    print(f"⏸️  No new data ({current_count} total records)")
            else:
                print(f"❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"💥 Error: {e}")
        
        time.sleep(60)  # Check every minute
    
    print(f"🏁 Live monitoring complete")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--live":
        monitor_live_updates()
    else:
        health_report = analyze_data_health()
        
        # Save report to file
        with open("health_report.json", "w") as f:
            json.dump(health_report, f, indent=2)
        
        print(f"\n💾 Full report saved to: health_report.json")