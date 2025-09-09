from typing import List, Tuple, Dict, Any
import math

Level = Tuple[float, float]  # (price, size)

def _mean_price(levels: List[Level], n: int) -> float:
    if not levels:
        return math.nan
    take = min(n, len(levels))
    s = 0.0
    for i in range(take):
        s += levels[i][0]
    return s / take

def _sum_volume(levels: List[Level], n: int) -> float:
    if not levels:
        return 0.0
    take = min(n, len(levels))
    s = 0.0
    for i in range(take):
        s += levels[i][1]
    return s

def _mid(bids: List[Level], asks: List[Level]) -> float:
    if not bids or not asks:
        return math.nan
    return (bids[0][0] + asks[0][0]) / 2.0

def _pct(numer: float, denom: float) -> float:
    if denom == 0.0 or math.isnan(denom):
        return math.nan
    return (numer / denom) * 100.0

def compute_metrics(ob: Dict[str, Any], layers: List[int]) -> Dict[str, Any]:
    bids: List[Level] = ob.get("bids", [])
    asks: List[Level] = ob.get("asks", [])
    mid = _mid(bids, asks)

    out: Dict[str, Any] = {
        "mid": mid,
        "depth_bids": len(bids),
        "depth_asks": len(asks),
    }

    for n in layers:
        avg_bid_n = _mean_price(bids, n)
        avg_ask_n = _mean_price(asks, n)
        spread = avg_ask_n - avg_bid_n
        out[f"spread_L{n}_pct"] = _pct(spread, mid)

    # L50 volumes
    out["vol_L50_bids"] = float(_sum_volume(bids, 50))
    out["vol_L50_asks"] = float(_sum_volume(asks, 50))

    return out
