#!/usr/bin/env python3
"""
Investigate data switching between old and new versions randomly.
This could be due to caching, race conditions, or multiple file versions.
"""

import json
import requests
import time
from datetime import datetime, timezone
import hashlib

def monitor_url_consistency(url, duration_seconds=300, check_interval=10):
    """Monitor a URL for consistency over time"""
    
    print(f"🔍 Monitoring URL consistency for {duration_seconds}s")
    print(f"URL: {url}")
    print(f"Check interval: {check_interval}s")
    print("=" * 80)
    
    results = []
    start_time = time.time()
    check_count = 0
    
    while (time.time() - start_time) < duration_seconds:
        check_count += 1
        check_time = datetime.now(timezone.utc)
        
        try:
            # Make request with cache-busting
            headers = {
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                text = response.text.strip()
                
                # Calculate content hash
                content_hash = hashlib.md5(text.encode()).hexdigest()[:8]
                
                # Get file info
                lines = [line for line in text.split('\n') if line.strip()]
                total_records = len(lines)
                
                # Get first and last record timestamps
                first_time = None
                last_time = None
                last_price = None
                
                if lines:
                    try:
                        first_record = json.loads(lines[0])
                        last_record = json.loads(lines[-1])
                        
                        first_time = first_record['t']
                        last_time = last_record['t']
                        last_price = last_record['mid']
                    except:
                        pass
                
                # Get HTTP headers
                content_length = response.headers.get('content-length', 'unknown')
                last_modified = response.headers.get('last-modified', 'unknown')
                etag = response.headers.get('etag', 'unknown')
                
                result = {
                    'check': check_count,
                    'time': check_time.strftime('%H:%M:%S'),
                    'status': response.status_code,
                    'content_hash': content_hash,
                    'total_records': total_records,
                    'first_time': first_time,
                    'last_time': last_time,
                    'last_price': last_price,
                    'content_length': content_length,
                    'last_modified': last_modified,
                    'etag': etag
                }
                
                results.append(result)
                
                # Print current status
                age_info = ""
                if last_time:
                    try:
                        last_dt = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                        age_minutes = (check_time - last_dt).total_seconds() / 60
                        age_info = f"({age_minutes:.1f}min old)"
                    except:
                        pass
                
                print(f"Check {check_count:2d}: {check_time.strftime('%H:%M:%S')} | "
                      f"Hash: {content_hash} | Records: {total_records:4d} | "
                      f"Last: {last_time} {age_info}")
                
                # Check for changes from previous
                if len(results) > 1:
                    prev = results[-2]
                    curr = results[-1]
                    
                    if prev['content_hash'] != curr['content_hash']:
                        print(f"  🔄 CONTENT CHANGED! Hash: {prev['content_hash']} → {curr['content_hash']}")
                        
                    if prev['total_records'] != curr['total_records']:
                        print(f"  📊 RECORD COUNT CHANGED! {prev['total_records']} → {curr['total_records']}")
                        
                    if prev['last_time'] != curr['last_time']:
                        print(f"  🕐 TIMESTAMP CHANGED! {prev['last_time']} → {curr['last_time']}")
                        
                    if prev['etag'] != curr['etag']:
                        print(f"  🏷️  ETAG CHANGED! {prev['etag']} → {curr['etag']}")
            else:
                print(f"Check {check_count:2d}: {check_time.strftime('%H:%M:%S')} | HTTP {response.status_code}")
                
        except Exception as e:
            print(f"Check {check_count:2d}: {check_time.strftime('%H:%M:%S')} | ERROR: {e}")
        
        time.sleep(check_interval)
    
    return results

def analyze_results(results):
    """Analyze the monitoring results for patterns"""
    
    print(f"\n📊 Analysis of {len(results)} checks:")
    print("=" * 50)
    
    if not results:
        print("No results to analyze")
        return
    
    # Check for unique hashes
    unique_hashes = set(r['content_hash'] for r in results if r.get('content_hash'))
    print(f"🔍 Unique content hashes found: {len(unique_hashes)}")
    
    if len(unique_hashes) > 1:
        print("🚨 MULTIPLE VERSIONS DETECTED!")
        
        # Group by hash
        hash_groups = {}
        for result in results:
            h = result.get('content_hash')
            if h:
                if h not in hash_groups:
                    hash_groups[h] = []
                hash_groups[h].append(result)
        
        for i, (hash_val, group) in enumerate(hash_groups.items(), 1):
            print(f"\n  Version {i} (hash: {hash_val}):")
            print(f"    Seen {len(group)} times")
            print(f"    Records: {group[0]['total_records']}")
            print(f"    Last timestamp: {group[0]['last_time']}")
            print(f"    Times seen: {[r['time'] for r in group[:5]]}")
            if len(group) > 5:
                print(f"    ... and {len(group) - 5} more times")
    else:
        print("✅ Content was consistent across all checks")
    
    # Check record counts
    record_counts = [r['total_records'] for r in results if r.get('total_records')]
    if record_counts:
        min_records = min(record_counts)
        max_records = max(record_counts)
        
        if min_records != max_records:
            print(f"📊 Record count varied: {min_records} to {max_records}")
        else:
            print(f"📊 Record count consistent: {min_records}")
    
    # Check timestamps
    last_times = [r['last_time'] for r in results if r.get('last_time')]
    unique_last_times = set(last_times)
    
    if len(unique_last_times) > 1:
        print(f"🕐 Multiple last timestamps: {len(unique_last_times)} different")
        for timestamp in sorted(unique_last_times):
            count = last_times.count(timestamp)
            print(f"    {timestamp}: seen {count} times")
    else:
        print(f"🕐 Last timestamp consistent: {list(unique_last_times)[0] if unique_last_times else 'None'}")

def test_multiple_assets():
    """Test multiple assets quickly to see if the issue is widespread"""
    
    print(f"\n🧪 Quick test across multiple assets:")
    print("=" * 40)
    
    bucket_name = "bananazone"
    date = "2025-09-09"
    
    test_urls = [
        f"https://storage.googleapis.com/{bucket_name}/coinbase/BTC/1min/{date}.jsonl",
        f"https://storage.googleapis.com/{bucket_name}/coinbase/ETH/1min/{date}.jsonl",
        f"https://storage.googleapis.com/{bucket_name}/kraken/BTC/1min/{date}.jsonl",
        f"https://storage.googleapis.com/{bucket_name}/kraken/ETH/1min/{date}.jsonl",
    ]
    
    for url in test_urls:
        asset_name = url.split('/')[-3:-1]  # Extract exchange/asset
        print(f"\n{asset_name[0]} {asset_name[1]}:")
        
        # Make 3 quick requests
        hashes = []
        for i in range(3):
            try:
                headers = {'Cache-Control': 'no-cache'}
                response = requests.get(url, headers=headers, timeout=5)
                if response.status_code == 200:
                    content_hash = hashlib.md5(response.text.encode()).hexdigest()[:8]
                    hashes.append(content_hash)
                    print(f"  Request {i+1}: {content_hash}")
                else:
                    print(f"  Request {i+1}: HTTP {response.status_code}")
            except Exception as e:
                print(f"  Request {i+1}: Error - {e}")
            
            time.sleep(1)
        
        # Check consistency
        if len(set(hashes)) == 1:
            print(f"  ✅ Consistent")
        else:
            print(f"  🚨 INCONSISTENT! {len(set(hashes))} different versions")

if __name__ == "__main__":
    # Test multiple assets first
    test_multiple_assets()
    
    # Then do detailed monitoring of Coinbase BTC
    print(f"\n" + "="*80)
    print("🔍 DETAILED MONITORING OF COINBASE BTC")
    print("="*80)
    
    coinbase_btc_url = "https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl"
    
    results = monitor_url_consistency(
        url=coinbase_btc_url,
        duration_seconds=120,  # Monitor for 2 minutes
        check_interval=5       # Check every 5 seconds
    )
    
    analyze_results(results)