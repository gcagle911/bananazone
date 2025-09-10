#!/usr/bin/env python3
"""
Test script to verify futures exchange connections before running the main collector.
"""

import logging
from futures_exchanges import make_futures_exchange, test_exchange_connection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_all_futures_exchanges():
    """Test all futures exchanges to see which ones work"""
    
    logger.info("🧪 Testing Futures Exchange Connections")
    logger.info("=" * 50)
    
    exchanges_config = [
        {"name": "bybit", "quote": "USDT"},
        {"name": "upbit", "quote": "USDT"},
        {"name": "okx", "quote": "USDT"},
        {"name": "coinbase", "quote": "USD"},
    ]
    
    assets = ["BTC", "ETH", "ADA", "XRP"]
    working_exchanges = {}
    
    for config in exchanges_config:
        name = config["name"]
        quote = config["quote"]
        
        logger.info(f"\n🔌 Testing {name.upper()}...")
        logger.info("-" * 30)
        
        try:
            # Create client
            client = make_futures_exchange(name)
            
            # Test connection
            available_symbols = test_exchange_connection(name, client, assets, quote)
            
            if available_symbols:
                working_exchanges[name] = {
                    "client": client,
                    "symbols": available_symbols,
                    "quote": quote
                }
                logger.info(f"✅ {name.upper()}: WORKING with {len(available_symbols)} assets")
            else:
                logger.error(f"❌ {name.upper()}: NO ASSETS AVAILABLE")
                
        except Exception as e:
            logger.error(f"❌ {name.upper()}: CONNECTION FAILED - {e}")
    
    # Summary
    logger.info(f"\n📊 SUMMARY")
    logger.info("=" * 20)
    logger.info(f"✅ Working exchanges: {len(working_exchanges)}")
    logger.info(f"❌ Failed exchanges: {len(exchanges_config) - len(working_exchanges)}")
    
    if working_exchanges:
        logger.info(f"\n🎯 Ready to collect futures data from:")
        for name, info in working_exchanges.items():
            assets_list = ", ".join(info["symbols"].keys())
            logger.info(f"   • {name.upper()}: {assets_list}")
    else:
        logger.error(f"\n🚨 NO WORKING EXCHANGES! Check network and API access.")
    
    return working_exchanges

if __name__ == "__main__":
    test_all_futures_exchanges()