# 🚀 Futures/Leverage Data Collection Branch

This branch collects **futures/leverage data** from multiple exchanges and saves it to the same GCS bucket under a `futures/` path structure.

## 📊 **Data Sources:**

- **Bybit** - Perpetual futures (USDT pairs)
- **Upbit** - Futures markets (USDT pairs)  
- **OKX** - Perpetual swaps (USDT pairs)
- **Coinbase** - Futures markets (USD pairs)

## 💰 **Assets Tracked:**
- BTC, ETH, ADA, XRP (same as spot data)

## 🗂️ **File Structure:**

```
bananazone/
├── futures/
│   ├── bybit/
│   │   ├── BTC/
│   │   │   ├── 5s/
│   │   │   │   ├── 2025-09-09.jsonl          # Daily 5s futures data
│   │   │   │   └── min/2025-09-09/17/
│   │   │   │       └── 2025-09-09T17:44.jsonl # Per-minute 5s data
│   │   │   └── 1min/
│   │   │       ├── 2025-09-09.jsonl          # Daily 1min futures data  
│   │   │       └── min/2025-09-09/17/
│   │   │           └── 2025-09-09T17:44.jsonl # Per-minute 1min data
│   │   ├── ETH/ ...
│   │   └── ADA/, XRP/ ...
│   ├── upbit/ ... (same structure)
│   ├── okx/ ... (same structure)  
│   └── coinbase/ ... (same structure)
└── [existing spot data unchanged]
    ├── coinbase/ ...
    └── kraken/ ...
```

## 📄 **Data Format:**

Each JSON record contains futures-specific data:

```json
{
  "t": "2025-09-09T17:44:37.074071Z",
  "exchange": "bybit",
  "asset": "BTC",
  "data_type": "futures",
  "symbol": "BTC/USDT:USDT",
  "mid": 111088.535,
  "spread_L5_pct": 0.005365,
  "spread_L50_pct": 0.029398,
  "spread_L100_pct": 0.063058,
  "vol_L50_bids": 7.259,
  "vol_L50_asks": 12.559,
  "depth_bids": 200,
  "depth_asks": 200
}
```

## 🔧 **Configuration:**

Edit `futures_config.yaml`:
```yaml
exchanges:
  - name: bybit
    type: futures
    quote: USDT
    enabled: true
  # ... other exchanges
```

## 🚀 **Usage:**

### 1. **Test Exchange Connections:**
```bash
python3 test_futures_exchanges.py
```

### 2. **Run Futures Data Collection:**
```bash
python3 futures_logger.py
```

### 3. **Monitor Health:**
```bash
tail -f futures_logger.log
```

## 🛡️ **Error Handling:**

- **Automatic Exchange Skipping**: Failed exchanges are automatically disabled
- **Symbol Auto-Detection**: Tries multiple symbol formats per exchange
- **Robust Error Recovery**: Continues collecting from working exchanges
- **Health Monitoring**: Tracks exchange performance and availability

## 🔗 **Public URLs:**

Access futures data via:
```
https://storage.googleapis.com/bananazone/futures/bybit/BTC/1min/2025-09-09.jsonl
https://storage.googleapis.com/bananazone/futures/okx/ETH/1min/2025-09-09.jsonl
https://storage.googleapis.com/bananazone/futures/upbit/ADA/5s/2025-09-09.jsonl
```

## 📈 **Features:**

✅ **Same GCS Bucket** - Uses existing storage infrastructure  
✅ **Parallel Collection** - All exchanges collected simultaneously  
✅ **Automatic Failover** - Skips problematic exchanges  
✅ **Symbol Auto-Discovery** - Finds correct futures symbols automatically  
✅ **Consistent Data Format** - Same structure as spot data  
✅ **Real-time Health Monitoring** - Tracks exchange performance  
✅ **Daily File Rotation** - Same rotation as spot data  
✅ **Web-Friendly Headers** - Ready for Vercel integration  

## 🔄 **Deployment:**

This branch can run:
- **Alongside spot data collection** (different processes)
- **On same Render instance** (different config files)
- **Separate Render instance** (recommended for isolation)

## 📊 **Monitoring:**

The system provides detailed logging:
- Exchange connection status
- Symbol availability per exchange
- Collection success rates
- Failed exchange tracking
- Performance metrics

## 🎯 **Benefits:**

- **Leverage data** alongside spot data
- **Multiple exchanges** for redundancy
- **Same infrastructure** as spot collection
- **Automatic error handling** 
- **Ready for charting** in Vercel applications