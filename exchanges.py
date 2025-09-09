import ccxt

# Browser-y headers (keeps various CDNs/WAFs happy)
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
        # Coinbase spot
        return ccxt.coinbase({
            "enableRateLimit": True,
            "timeout": 20000,
            "headers": COMMON_HEADERS,
        })
    elif exchange_name == "kraken":
        # Kraken spot
        return ccxt.kraken({
            "enableRateLimit": True,
            "timeout": 25000,
            "headers": COMMON_HEADERS,
        })
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

def symbol_for(exchange_name: str, base: str, quote: str) -> str:
    # ccxt uses BASE/QUOTE string for both exchanges
    return f"{base}/{quote}"
