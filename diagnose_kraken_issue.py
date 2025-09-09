#!/usr/bin/env python3
"""
Diagnose specific issues with Kraken data collection.
"""

import json
import requests
from datetime import datetime, timezone
import ccxt

def test_kraken_api_directly():
    """Test Kraken API directly to see if there are issues"""
    
    print("🔍 Testing Kraken API directly...")
    print("-" * 40)
    
    try:
        # Create Kraken client
        kraken = ccxt.kraken({
            "enableRateLimit": True,
            "timeout": 25000,
        })
        
        # Test loading markets
        print("📋 Loading Kraken markets...")
        kraken.load_markets()
        print(f"✅ Loaded {len(kraken.markets)} markets")
        
        # Test each asset
        assets = ["BTC", "ETH", "ADA", "XRP"]
        quote = "USD"
        
        for asset in assets:
            symbol = f"{asset}/{quote}"
            
            print(f"\n📊 Testing {symbol}:")
            
            try:
                # Check if symbol exists
                if symbol not in kraken.markets:
                    print(f"❌ Symbol {symbol} not found in markets")
                    # Try alternative symbols
                    alternatives = [key for key in kraken.markets.keys() if asset in key and "USD" in key]
                    if alternatives:
                        print(f"💡 Alternatives found: {alternatives[:3]}")
                        symbol = alternatives[0]
                        print(f"🔄 Trying {symbol} instead...")
                
                # Fetch order book
                start_time = datetime.now()
                ob = kraken.fetch_order_book(symbol, limit=200)
                fetch_time = (datetime.now() - start_time).total_seconds()
                
                # Check data quality
                bids = ob.get('bids', [])
                asks = ob.get('asks', [])
                
                if bids and asks:
                    mid_price = (bids[0][0] + asks[0][0]) / 2
                    print(f"✅ Success: ${mid_price:.2f} ({len(bids)} bids, {len(asks)} asks) - {fetch_time:.1f}s")
                else:
                    print(f"⚠️  Empty order book: {len(bids)} bids, {len(asks)} asks")
                    
            except ccxt.RateLimitExceeded as e:
                print(f"🚫 Rate limited: {e}")
            except ccxt.ExchangeError as e:
                print(f"💥 Exchange error: {e}")
            except Exception as e:
                print(f"❌ Error: {e}")
                
    except Exception as e:
        print(f"💥 Failed to create Kraken client: {e}")

def check_kraken_symbol_mapping():
    """Check what symbols Kraken actually uses"""
    
    print(f"\n🔍 Checking Kraken symbol mapping...")
    print("-" * 40)
    
    try:
        kraken = ccxt.kraken()
        kraken.load_markets()
        
        # Look for BTC, ETH, ADA, XRP symbols
        target_assets = ["BTC", "ETH", "ADA", "XRP"]
        
        for asset in target_assets:
            print(f"\n🔎 Symbols containing '{asset}':")
            matching_symbols = [
                symbol for symbol in kraken.markets.keys() 
                if asset in symbol and "USD" in symbol
            ]
            
            for symbol in matching_symbols[:5]:  # Show first 5
                market = kraken.markets[symbol]
                print(f"  {symbol:15} - {market.get('base', '')}/{market.get('quote', '')}")
                
    except Exception as e:
        print(f"💥 Error checking symbols: {e}")

def diagnose_data_collection_pattern():
    """Look at the pattern of when Kraken data stopped"""
    
    print(f"\n🔍 Analyzing Kraken data collection patterns...")
    print("-" * 50)
    
    bucket_name = "bananazone"
    date = "2025-09-09"
    
    for asset in ["BTC", "ETH", "ADA", "XRP"]:
        url = f"https://storage.googleapis.com/{bucket_name}/kraken/{asset}/1min/{date}.jsonl"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                text = response.text.strip()
                if text:
                    lines = [line for line in text.split('\n') if line.strip()]
                    
                    if len(lines) >= 5:
                        print(f"\n📊 Kraken {asset} - Last 5 records:")
                        
                        for line in lines[-5:]:
                            try:
                                record = json.loads(line)
                                timestamp = record['t']
                                price = record['mid']
                                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                print(f"  {dt.strftime('%H:%M:%S')} - ${price:.4f}")
                            except:
                                print(f"  Parse error")
                    else:
                        print(f"\n📊 Kraken {asset}: Only {len(lines)} records total")
                        
        except Exception as e:
            print(f"💥 Error checking Kraken {asset}: {e}")

if __name__ == "__main__":
    test_kraken_api_directly()
    check_kraken_symbol_mapping()
    diagnose_data_collection_pattern()