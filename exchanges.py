import ccxt

def make_exchange(exchange_name: str):
    if exchange_name == "coinbase":
        return ccxt.coinbase()
    elif exchange_name == "bybit":
        # Spot markets only
        return ccxt.bybit({'options': {'defaultType': 'spot'}})
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

def symbol_for(_exchange_name: str, base: str, quote: str) -> str:
    # Both use standard BASE/QUOTE with slash in ccxt
    return f"{base}/{quote}"
