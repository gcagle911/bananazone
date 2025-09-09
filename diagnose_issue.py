#!/usr/bin/env python3
"""
Quick diagnostic script to check what's happening with data collection.
"""

import json
import requests
from datetime import datetime, timezone, timedelta

def check_all_assets():
    """Check the latest data for all assets"""
    
    print("🔍 DIAGNOSTIC: Checking latest data across all assets")
    print("=" * 60)
    
    bucket_name = "bananazone"
    exchanges = ["coinbase", "kraken"]
    assets = ["BTC", "ETH", "ADA", "XRP"]
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    
    all_latest_times = []
    
    for exchange in exchanges:
        for asset in assets:
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
                                price = last_record['mid']
                                
                                status = "🟢" if age_minutes <= 2 else "🟡" if age_minutes <= 5 else "🔴"
                                print(f"{status} {exchange:8} {asset:3}: {last_time.strftime('%H:%M:%S')} (${price:8.2f}) - {age_minutes:.1f}min old")
                                
                                all_latest_times.append(last_time)
                                
                            except Exception as e:
                                print(f"❌ {exchange:8} {asset:3}: Parse error - {e}")
                        else:
                            print(f"📄 {exchange:8} {asset:3}: Empty file")
                    else:
                        print(f"📄 {exchange:8} {asset:3}: No content")
                elif response.status_code == 404:
                    print(f"❌ {exchange:8} {asset:3}: File not found")
                else:
                    print(f"❌ {exchange:8} {asset:3}: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"💥 {exchange:8} {asset:3}: Network error - {e}")
    
    print(f"\n📊 Analysis:")
    if all_latest_times:
        latest_time = max(all_latest_times)
        oldest_time = min(all_latest_times)
        
        latest_age = (now - latest_time).total_seconds() / 60
        oldest_age = (now - oldest_time).total_seconds() / 60
        
        print(f"   Most recent data: {latest_time.strftime('%H:%M:%S')} ({latest_age:.1f}min ago)")
        print(f"   Oldest data:      {oldest_time.strftime('%H:%M:%S')} ({oldest_age:.1f}min ago)")
        
        if latest_age > 5:
            print(f"   🚨 ISSUE: All data is stale (>{latest_age:.0f} minutes old)")
            print(f"   💡 Likely cause: Data collector stopped or crashed")
        elif oldest_age - latest_age > 5:
            print(f"   🚨 ISSUE: Inconsistent data ages (spread: {oldest_age - latest_age:.1f} minutes)")
            print(f"   💡 Likely cause: Some exchanges/assets failing")
        else:
            print(f"   ✅ Data collection appears healthy")
    
    print(f"\n🕐 Current time: {now.strftime('%H:%M:%S')} UTC")

def check_5s_data():
    """Check 5-second data to see if raw collection is still working"""
    print(f"\n🔍 Checking 5-second data (raw collection):")
    print("-" * 40)
    
    bucket_name = "bananazone"
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    
    # Check Coinbase BTC 5s data
    url = f"https://storage.googleapis.com/{bucket_name}/coinbase/BTC/5s/{date}.jsonl"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            text = response.text.strip()
            if text:
                lines = [line for line in text.split('\n') if line.strip()]
                if lines:
                    # Check last few records
                    print(f"   📊 Total 5s records: {len(lines)}")
                    
                    for i, line in enumerate(lines[-3:]):
                        try:
                            record = json.loads(line)
                            last_time = datetime.fromisoformat(record['t'].replace('Z', '+00:00'))
                            age_seconds = (now - last_time).total_seconds()
                            print(f"   Record {i-2}: {last_time.strftime('%H:%M:%S')} ({age_seconds:.0f}s ago)")
                        except Exception as e:
                            print(f"   Parse error: {e}")
                else:
                    print(f"   📄 5s file exists but empty")
            else:
                print(f"   📄 5s file has no content")
        else:
            print(f"   ❌ 5s file HTTP {response.status_code}")
            
    except Exception as e:
        print(f"   💥 5s file error: {e}")

def suggest_actions():
    """Suggest next steps based on findings"""
    print(f"\n💡 Suggested Actions:")
    print("-" * 20)
    print(f"1. Check Render deployment logs for errors")
    print(f"2. Look for rate limiting or API errors in logs")
    print(f"3. Verify the improved collector is running")
    print(f"4. Check if Render service is still active")
    print(f"5. Monitor for the next few minutes to see if collection resumes")

if __name__ == "__main__":
    check_all_assets()
    check_5s_data()
    suggest_actions()