#!/usr/bin/env python3
"""
Test script to verify the crypto data collection system is working properly.
"""

import os
import time
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from logger import main as logger_main
from storage import get_storage_backend

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_collection():
    """Test that the data collection system creates JSON files properly"""
    
    logger.info("Starting system test...")
    
    # Create a data directory to check
    data_dir = Path("data")
    if data_dir.exists():
        import shutil
        shutil.rmtree(data_dir)
    
    logger.info("Running data collection for 30 seconds...")
    
    # Start the main process in background (we'll kill it after 30 seconds)
    import subprocess
    import signal
    
    process = subprocess.Popen(
        ["python3", "logger.py"], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Let it run for 30 seconds
    try:
        stdout, stderr = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    
    logger.info("Data collection stopped, checking results...")
    
    # Check if data directory was created
    if not data_dir.exists():
        logger.error("Data directory was not created!")
        return False
    
    # Look for JSON files
    json_files = list(data_dir.rglob("*.jsonl"))
    
    if not json_files:
        logger.error("No JSONL files were created!")
        return False
    
    logger.info(f"Found {len(json_files)} JSONL files:")
    
    total_records = 0
    sample_data = []
    
    for json_file in json_files[:5]:  # Check first 5 files
        logger.info(f"  {json_file}")
        
        try:
            with open(json_file, 'r') as f:
                lines = f.readlines()
                total_records += len(lines)
                
                # Parse a sample record
                if lines:
                    sample_record = json.loads(lines[0].strip())
                    sample_data.append(sample_record)
                    logger.info(f"    Records: {len(lines)}")
                    logger.info(f"    Sample: {sample_record['exchange']} {sample_record['asset']} "
                               f"mid={sample_record['mid']:.4f} at {sample_record['t']}")
        except Exception as e:
            logger.error(f"    Error reading file: {e}")
    
    logger.info(f"Total records found: {total_records}")
    
    if total_records == 0:
        logger.error("No data records were found in files!")
        return False
    
    # Check file structure
    expected_exchanges = ["coinbase", "kraken"]
    expected_assets = ["BTC", "ETH", "ADA", "XRP"]
    
    found_combinations = set()
    for record in sample_data:
        found_combinations.add((record['exchange'], record['asset']))
    
    logger.info(f"Found exchange/asset combinations: {found_combinations}")
    
    if len(found_combinations) > 0:
        logger.info("✅ System test PASSED - Data collection is working!")
        return True
    else:
        logger.error("❌ System test FAILED - No valid data combinations found")
        return False

def show_file_structure():
    """Show the current file structure"""
    data_dir = Path("data")
    if not data_dir.exists():
        logger.info("No data directory found")
        return
    
    logger.info("Current data file structure:")
    for path in sorted(data_dir.rglob("*")):
        if path.is_file():
            size = path.stat().st_size
            logger.info(f"  {path} ({size} bytes)")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--show-files":
        show_file_structure()
    else:
        success = test_data_collection()
        if success:
            show_file_structure()
        sys.exit(0 if success else 1)