#!/usr/bin/env python3
"""
Monitoring script to check the health of the crypto data collection system.
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

def check_system_health():
    """Check the health of the data collection system"""
    
    print("🔍 Crypto Data Collection System Health Check")
    print("=" * 50)
    
    data_dir = Path("data")
    
    if not data_dir.exists():
        print("❌ No data directory found - system may not be running")
        return False
    
    # Check for recent data
    now = datetime.now()
    recent_files = []
    total_files = 0
    total_records = 0
    
    exchanges = set()
    assets = set()
    
    # Find all JSONL files
    for jsonl_file in data_dir.rglob("*.jsonl"):
        total_files += 1
        
        # Check if file was modified recently (within last 10 minutes)
        modified_time = datetime.fromtimestamp(jsonl_file.stat().st_mtime)
        if (now - modified_time) < timedelta(minutes=10):
            recent_files.append(jsonl_file)
        
        # Count records and extract metadata
        try:
            with open(jsonl_file, 'r') as f:
                lines = f.readlines()
                file_records = len(lines)
                total_records += file_records
                
                if lines and file_records > 0:
                    # Parse first record to get exchange/asset info
                    try:
                        record = json.loads(lines[0].strip())
                        exchanges.add(record.get('exchange', 'unknown'))
                        assets.add(record.get('asset', 'unknown'))
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            print(f"⚠️  Error reading {jsonl_file}: {e}")
    
    print(f"📊 System Statistics:")
    print(f"   Total files: {total_files}")
    print(f"   Total records: {total_records:,}")
    print(f"   Recent files (last 10min): {len(recent_files)}")
    print(f"   Exchanges: {', '.join(sorted(exchanges))}")
    print(f"   Assets: {', '.join(sorted(assets))}")
    
    # Check data freshness
    if len(recent_files) > 0:
        print(f"✅ System is actively collecting data")
        
        # Show some recent files
        print(f"\n📁 Recent data files:")
        for recent_file in sorted(recent_files)[-5:]:  # Show last 5
            modified_time = datetime.fromtimestamp(recent_file.stat().st_mtime)
            size = recent_file.stat().st_size
            print(f"   {recent_file} ({size} bytes, {modified_time.strftime('%H:%M:%S')})")
        
        return True
    else:
        print(f"❌ No recent data files found - system may have stopped")
        return False

def show_sample_data():
    """Show sample data from recent files"""
    print(f"\n📈 Sample Data:")
    print("-" * 30)
    
    data_dir = Path("data")
    sample_files = list(data_dir.rglob("*5s/min*/*.jsonl"))[-3:]  # Get last 3 5s files
    
    for sample_file in sample_files:
        try:
            with open(sample_file, 'r') as f:
                lines = f.readlines()
                if lines:
                    record = json.loads(lines[-1].strip())  # Last record
                    print(f"   {record['exchange']} {record['asset']}: "
                          f"${record['mid']:.2f} at {record['t']}")
        except Exception as e:
            print(f"   Error reading {sample_file}: {e}")

def monitor_live():
    """Monitor the system in real-time"""
    print(f"\n🔄 Live Monitoring (press Ctrl+C to stop)")
    print("-" * 40)
    
    last_record_count = 0
    
    try:
        while True:
            data_dir = Path("data")
            current_record_count = 0
            
            # Count all records
            for jsonl_file in data_dir.rglob("*.jsonl"):
                try:
                    with open(jsonl_file, 'r') as f:
                        current_record_count += len(f.readlines())
                except Exception:
                    pass
            
            new_records = current_record_count - last_record_count
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            if new_records > 0:
                print(f"   {timestamp}: +{new_records} records (total: {current_record_count:,})")
            else:
                print(f"   {timestamp}: No new records (total: {current_record_count:,})")
            
            last_record_count = current_record_count
            time.sleep(10)  # Check every 10 seconds
            
    except KeyboardInterrupt:
        print(f"\n   Monitoring stopped.")

if __name__ == "__main__":
    import sys
    
    healthy = check_system_health()
    
    if healthy:
        show_sample_data()
        
        if len(sys.argv) > 1 and sys.argv[1] == "--live":
            monitor_live()
    else:
        print(f"\n💡 To start the system, run: python3 logger.py")
        print(f"💡 To check logs, run: tail -f crypto_logger.log")