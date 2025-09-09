# Multi‑Layer Order Book Collector (Coinbase + Kraken) — Near‑Live 1m

**Bucket:** `bananazone`  
**Pairs:** Coinbase/USD + Kraken/USD for BTC, ETH, ADA, XRP  
**Cadence:** every 5s; near‑live 1‑minute daily updated ≤ 5 minutes

## 🚀 Quick Start

1. **Install dependencies:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the collector:**
   ```bash
   python3 logger.py
   ```

The system automatically uses local file storage if Google Cloud Storage credentials are not available.

## 📁 Data Structure

The system creates two types of JSON files with daily rotation:

### 5-Second Data Files
- **Path:** `data/{exchange}/{asset}/5s/min/{day}/{hour}/{day}T{hour}:{minute}.jsonl`
- **Content:** Raw order book metrics collected every 5 seconds
- **Daily Aggregation:** `data/{exchange}/{asset}/5s/{day}.jsonl`

### 1-Minute Data Files  
- **Path:** `data/{exchange}/{asset}/1min/min/{day}/{hour}/{day}T{hour}:{minute}.jsonl`
- **Content:** Averaged metrics from 5-second data
- **Daily Aggregation:** `data/{exchange}/{asset}/1min/{day}.jsonl`

## 📊 Data Format

Each JSON record contains:
```json
{
  "t": "2025-09-09T17:44:37.074071Z",
  "exchange": "coinbase",
  "asset": "BTC", 
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

## ☁️ Google Cloud Storage Setup

For GCS upload functionality:

1. **Create GCS credentials:**
   ```bash
   python3 create_gcs_template.py
   cp gcs-key.json.template gcs-key.json
   # Edit gcs-key.json with your actual GCS service account credentials
   ```

2. **Ensure your service account has Storage Admin permissions on the bucket**

## 🔧 Configuration

Edit `config.yaml` to customize:
- Collection interval (default: 5 seconds)
- GCS bucket name
- Assets to track
- Publishing intervals

## 📈 System Features

✅ **Fixed Issues:**
- Missing GCS credentials now falls back to local storage
- Proper JSON file creation and rotation
- 5-second data collection working
- 1-minute data aggregation working
- Daily file rotation working
- Comprehensive logging and error handling
- Real-time statistics tracking

## 🗂️ File Structure Example

```
data/
├── coinbase/
│   ├── BTC/
│   │   ├── 5s/
│   │   │   ├── 2025-09-09.jsonl          # Daily 5s data
│   │   │   └── min/2025-09-09/17/
│   │   │       ├── 2025-09-09T17:44.jsonl # Per-minute 5s data
│   │   │       └── 2025-09-09T17:45.jsonl
│   │   └── 1min/
│   │       ├── 2025-09-09.jsonl          # Daily 1min data  
│   │       └── min/2025-09-09/17/
│   │           └── 2025-09-09T17:44.jsonl # Per-minute 1min data
│   ├── ETH/ ...
│   ├── ADA/ ...
│   └── XRP/ ...
└── kraken/ ...
```

## 🔍 Monitoring

- **Log file:** `crypto_logger.log`
- **Console output:** Real-time status updates
- **Statistics:** Success/failure rates logged every 10 cycles

## 📋 Outputs (NDJSON)

- **1‑minute daily (near‑live ≤5m):**
