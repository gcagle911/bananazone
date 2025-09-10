import ccxt
import logging
import time

logger = logging.getLogger(__name__)

# Browser-y headers (keeps various CDNs/WAFs happy)
COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

def make_futures_exchange(exchange_name: str):
    """Create futures exchange clients with proper error handling"""
    
    try:
        if exchange_name == "bybit":
            return ccxt.bybit({
                "enableRateLimit": True,
                "timeout": 30000,
                "headers": COMMON_HEADERS,
                "sandbox": False,  # Use production API
                "options": {
                    "defaultType": "swap",  # Use perpetual futures
                }
            })
        elif exchange_name == "upbit":
            return ccxt.upbit({
                "enableRateLimit": True,
                "timeout": 25000,
                "headers": COMMON_HEADERS,
                "options": {
                    "defaultType": "future",
                }
            })
        elif exchange_name == "okx":
            return ccxt.okx({
                "enableRateLimit": True,
                "timeout": 25000,
                "headers": COMMON_HEADERS,
                "sandbox": False,
                "options": {
                    "defaultType": "swap",  # Use perpetual swaps
                }
            })
        elif exchange_name == "coinbase":
            # Coinbase Advanced (has futures)
            return ccxt.coinbase({
                "enableRateLimit": True,
                "timeout": 20000,
                "headers": COMMON_HEADERS,
                "sandbox": False,
                "options": {
                    "defaultType": "future",
                }
            })
        else:
            raise ValueError(f"Unsupported futures exchange: {exchange_name}")
            
    except Exception as e:
        logger.error(f"Failed to create {exchange_name} client: {e}")
        raise

def get_futures_symbol(exchange_name: str, base: str, quote: str) -> str:
    """Get the correct futures symbol format for each exchange"""
    
    # Different exchanges use different symbol formats for futures
    if exchange_name == "bybit":
        # Bybit uses BASE/QUOTE:QUOTE format for perpetuals
        return f"{base}/{quote}:{quote}"
    elif exchange_name == "upbit":
        # Upbit uses BASE-QUOTE format
        return f"{base}-{quote}"
    elif exchange_name == "okx":
        # OKX uses BASE-QUOTE-SWAP format for perpetuals
        return f"{base}-{quote}-SWAP"
    elif exchange_name == "coinbase":
        # Coinbase uses BASE-QUOTE format for futures
        return f"{base}-{quote}"
    else:
        # Default CCXT format
        return f"{base}/{quote}"

def test_exchange_connection(exchange_name: str, client, assets: list, quote: str):
    """Test if exchange connection works and return available symbols"""
    
    logger.info(f"🧪 Testing {exchange_name} connection...")
    
    try:
        # Load markets
        markets = client.load_markets()
        logger.info(f"✅ {exchange_name}: Loaded {len(markets)} markets")
        
        # Test each asset
        available_symbols = {}
        
        for asset in assets:
            symbol = get_futures_symbol(exchange_name, asset, quote)
            
            # Try alternative symbol formats if primary doesn't work
            alternative_symbols = [
                symbol,
                f"{asset}/{quote}",
                f"{asset}{quote}",
                f"{asset}_{quote}",
                f"{asset}-{quote}",
                f"{asset}/{quote}:USDT" if quote == "USDT" else f"{asset}/{quote}:USD"
            ]
            
            found_symbol = None
            for test_symbol in alternative_symbols:
                if test_symbol in markets:
                    found_symbol = test_symbol
                    break
            
            if found_symbol:
                # Test fetching order book
                try:
                    ob = client.fetch_order_book(found_symbol, limit=100)
                    if ob and ob.get('bids') and ob.get('asks'):
                        available_symbols[asset] = found_symbol
                        mid_price = (ob['bids'][0][0] + ob['asks'][0][0]) / 2
                        logger.info(f"  ✅ {asset}: {found_symbol} - ${mid_price:.2f}")
                    else:
                        logger.warning(f"  ⚠️  {asset}: Empty order book for {found_symbol}")
                except Exception as e:
                    logger.warning(f"  ❌ {asset}: Failed to fetch order book - {e}")
            else:
                logger.warning(f"  ❌ {asset}: No valid symbol found (tried {len(alternative_symbols)} formats)")
        
        if available_symbols:
            logger.info(f"✅ {exchange_name}: {len(available_symbols)} assets available")
            return available_symbols
        else:
            logger.error(f"❌ {exchange_name}: No assets available")
            return None
            
    except Exception as e:
        logger.error(f"❌ {exchange_name}: Connection test failed - {e}")
        return None

def validate_exchange_health(exchange_name: str, client, symbols: dict):
    """Check if exchange is healthy and responsive"""
    
    try:
        # Test fetching one symbol
        test_asset = list(symbols.keys())[0]
        test_symbol = symbols[test_asset]
        
        start_time = time.time()
        ob = client.fetch_order_book(test_symbol, limit=50)
        response_time = time.time() - start_time
        
        if ob and ob.get('bids') and ob.get('asks') and response_time < 10:
            return True, f"Healthy (response: {response_time:.1f}s)"
        else:
            return False, f"Slow response ({response_time:.1f}s) or empty data"
            
    except Exception as e:
        return False, f"Health check failed: {e}"