import ccxt

# Some WAFs/CDNs are picky about default Python UAs; use a browser-y header
COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def make_exchange(exchange_name: str):
    if exchange_name == "coinbase":
        # Coinbase spot, rate-limited, longer timeout, friendly headers
        return ccxt.coinbase(
            {
                "enableRateLimit": True,
                "timeout": 20000,
                "headers": COMMON_HEADERS,
            }
        )
    elif exchange_name == "bybit":
        # Bybit spot, rate-limited, longer timeout, friendly headers
        return ccxt.bybit(
            {
                "enableRateLimit": True,
                "timeout": 25000,
                "headers": COMMON_HEADERS,
                "options": {
                    "defaultType": "spot",  # ensure spot endpoints
                },
            }
        )
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")


def symbol_for(_exchange_name: str, base: str, quote: str) -> str:
    # Both exchanges use BASE/QUOTE format in ccxt
    return f"{base}/{quote}"
