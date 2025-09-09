import json
import time
import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

import yaml
from ccxt.base.errors import RateLimitExceeded, DDoSProtection, ExchangeError

from exchanges import make_exchange, symbol_for
from metrics import compute_metrics
from gcs import append_jsonl_line, download_text, upload_text, list_prefix, compose_many


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def iso_utc(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def fmt_paths(cfg, ex: str, asset: str, t: datetime) -> Dict[str, str]:
    day = t.strftime("%Y-%m-%d")
    hour = t.strftime("%H")
    minute = t.strftime("%M")
    p = cfg["paths"]
    return {
        "five_sec_minute": p["five_sec_minute"].format(
            ex=ex, asset=asset, day=day, hour=hour, minute=minute
        ),
        "five_sec_daily": p["five_sec_daily"].format(ex=ex, asset=asset, day=day),
        # NEW per-minute 1m file
        "one_min_minute": p["one_min_minute"].format(
            ex=ex, asset=asset, day=day, hour=hour, minute=minute
        ),
        "one_min_daily": p["one_min_daily"].format(ex=ex, asset=asset, day=day),
    }


def aggregate_minute_from_5s(
    records: List[Dict[str, Any]], t_minute: datetime, ex: str, asset: str
) -> Dict[str, Any]:
    fields = [
        "mid",
        "spread_L5_pct",
        "spread_L50_pct",
        "spread_L100_pct",
        "vol_L50_bids",
        "vol_L50_asks",
        "depth_bids",
        "depth_asks",
    ]
    agg: Dict[str, Any] = {
        "t": iso_utc(t_minute.replace(second=0, microsecond=0)),
        "exchange": ex,
        "asset": asset,
    }
    for f in fields:
        vals = [
            r.get(f)
            for r in records
            if isinstance(r.get(f), (int, float)) and r.get(f) == r.get(f)
        ]
        agg[f] = (sum(vals) / len(vals)) if vals else None
    return agg


def publish_1min_nearlive(cfg, bucket: str, ex: str, asset: str, now: datetime):
    """
    Every publish_1min_minutes, rebuild the last window of minutes from per-minute 5s files,
    write EACH minute as its own one-line NDJSON object under 1min/min/..., then COMPOSE all
    1min/min/... files for the day into the daily 1min/YYYY-MM-DD.jsonl.

    This avoids read-modify-write overwrites and guarantees we never "roll" to 5m only.
    """
    minutes_back = int(cfg.get("publish_1min_minutes", 5))
    end_minute = now.replace(second=0, microsecond=0)
    start_minute = end_minute - timedelta(minutes=minutes_back - 1)

    # 1) Build per-minute rows for the window and store each to its own object
    for i in range(minutes_back):
        m = start_minute + timedelta(minutes=i)
        paths = fmt_paths(cfg, ex, asset, m)
        src_5s = paths["five_sec_minute"]
        dst_1m_min = paths["one_min_minute"]

        text = download_text(bucket, src_5s)
        if not text:
            continue

        records: List[Dict[str, Any]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                pass
        if not records:
            continue

        row = aggregate_minute_from_5s(records, m, ex, asset)
        # Each per-minute file contains exactly ONE line
        upload_text(bucket, dst_1m_min, json.dumps(row, separators=(",", ":")) + "\n")

    # 2) Compose ALL per-minute 1m files for the day → daily 1m NDJSON
    day = now.strftime("%Y-%m-%d")
    prefix = f"{ex}/{asset}/1min/min/{day}/"
    sources = list_prefix(bucket, prefix)
    if not sources:
        return
    dest = fmt_paths(cfg, ex, asset, now)["one_min_daily"]
    compose_many(bucket, sources, dest)


def publish_5s_daily(cfg, bucket: str, ex: str, asset: str, now: datetime):
    """Compose all minute 5s files for the day into a single daily NDJSON (hourly by default)."""
    day = now.strftime("%Y-%m-%d")
    prefix = f"{ex}/{asset}/5s/min/{day}/"
    sources = list_prefix(bucket, prefix)
    if not sources:
        return
    dest = fmt_paths(cfg, ex, asset, now)["five_sec_daily"]
    compose_many(bucket, sources, dest)


def main():
    cfg = load_config()
    interval = int(cfg.get("interval_seconds", 5))
    bucket = cfg["gcs_bucket"]
    layers = cfg.get("layers", [5, 50, 100])
    publish_1m = int(cfg.get("publish_1min_minutes", 5))
    publish_5s = int(cfg.get("publish_5s_minutes", 60))

    exchanges_cfg = cfg["exchanges"]
    assets = cfg["assets"]

    clients: Dict[str, Any] = {}
    quotes: Dict[str, str] = {}
    for e in exchanges_cfg:
        name = e["name"]
        quotes[name] = e["quote"]
        clients[name] = make_exchange(name)

    # Load markets for exchanges that allow it
    for name, client in clients.items():
        if name == "bybit":  # if you ever re-enable bybit
            print("Skipping load_markets() for bybit")
            continue
        try:
            client.load_markets()
            print(f"loaded markets: {name} ({len(getattr(client, 'markets', {}) or [])} symbols)")
        except Exception as e:
            print(f"load_markets failed for {name}: {e}")

    last_pub_1m: Dict[str, datetime] = {}
    last_pub_5s: Dict[str, datetime] = {}

    try:
        while True:
            now = datetime.now(timezone.utc)
            t_iso = iso_utc(now)

            for ex_name, client in clients.items():
                quote = quotes[ex_name]
                for asset in assets:
                    sym = symbol_for(ex_name, asset, quote)
                    try:
                        ob = client.fetch_order_book(sym, limit=200)
                        metrics = compute_metrics(ob, layers)
                        record = {
                            "t": t_iso,
                            "exchange": ex_name,
                            "asset": asset,
                            "mid": metrics["mid"],
                            "spread_L5_pct": metrics["spread_L5_pct"],
                            "spread_L50_pct": metrics["spread_L50_pct"],
                            "spread_L100_pct": metrics["spread_L100_pct"],
                            "vol_L50_bids": metrics["vol_L50_bids"],
                            "vol_L50_asks": metrics["vol_L50_asks"],
                            "depth_bids": metrics["depth_bids"],
                            "depth_asks": metrics["depth_asks"],
                        }
                        # Append this 5s tick into the current minute's NDJSON file
                        path_keys = fmt_paths(cfg, ex_name, asset, now)
                        append_jsonl_line(bucket, path_keys["five_sec_minute"], json.dumps(record, separators=(",", ":")))
                    except Exception as e:
                        print(f"ERROR fetch/write {ex_name} {asset}: {e}")
                        traceback.print_exc()

                    # 1m near-live compose (<=5m lag)
                    pair_key = f"{ex_name}:{asset}"
                    if (last_pub_1m.get(pair_key) is None) or (
                        (now - last_pub_1m[pair_key]) >= timedelta(minutes=publish_1m)
                    ):
                        try:
                            publish_1min_nearlive(cfg, bucket, ex_name, asset, now)
                            last_pub_1m[pair_key] = now
                        except Exception as e:
                            print(f"ERROR publish 1m {pair_key}: {e}")

                    # 5s daily compose (hourly default)
                    if (last_pub_5s.get(pair_key) is None) or (
                        (now - last_pub_5s[pair_key]) >= timedelta(minutes=publish_5s)
                    ):
                        try:
                            publish_5s_daily(cfg, bucket, ex_name, asset, now)
                            last_pub_5s[pair_key] = now
                        except Exception as e:
                            print(f"ERROR publish 5s {pair_key}: {e}")

            time.sleep(max(1, interval))
    finally:
        for c in clients.values():
            try:
                if hasattr(c, "close"):
                    c.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
