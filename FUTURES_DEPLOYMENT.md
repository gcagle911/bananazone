# 🚀 Futures Data Collection - Deployment Guide

## ✅ **Branch Status: READY FOR DEPLOYMENT**

**Working Exchanges:** 3/4
- ✅ **Upbit** - All 4 assets (BTC, ETH, ADA, XRP)
- ✅ **OKX** - All 4 assets (BTC, ETH, ADA, XRP)  
- ✅ **Coinbase** - All 4 assets (BTC, ETH, ADA, XRP)
- ❌ **Bybit** - Geo-blocked (disabled in config)

## 🔧 **Deployment Options:**

### **Option 1: Same Render Instance (Recommended)**
Run alongside spot data collection:

```bash
# In Render, update start command to:
python3 futures_logger.py & python3 logger.py
```

**Pros:** 
- Uses same GCS credentials
- Same infrastructure
- Cost efficient

**Cons:**
- Shared resources
- If one fails, both affected

### **Option 2: Separate Render Instance**
Deploy as separate service:

1. Create new Render service
2. Point to `futures-leverage-data` branch
3. Use same `gcs-key.json` secret file
4. Start command: `python3 futures_logger.py`

**Pros:**
- Isolated processes
- Independent scaling
- Separate monitoring

**Cons:**
- Additional cost
- Duplicate GCS credentials

### **Option 3: Local Testing**
Test locally first:

```bash
git checkout futures-leverage-data
source venv/bin/activate
python3 test_futures_exchanges.py  # Test connections
python3 futures_logger.py          # Start collection
```

## 📊 **Expected Data Structure:**

After deployment, you'll have:

```
https://storage.googleapis.com/bananazone/
├── futures/
│   ├── upbit/
│   │   ├── BTC/1min/2025-09-09.jsonl
│   │   ├── ETH/1min/2025-09-09.jsonl
│   │   ├── ADA/1min/2025-09-09.jsonl
│   │   └── XRP/1min/2025-09-09.jsonl
│   ├── okx/
│   │   └── [same structure]
│   └── coinbase/
│       └── [same structure]
└── [existing spot data unchanged]
    ├── coinbase/...
    └── kraken/...
```

## 🔗 **Public URLs:**

Access futures data via:
```
https://storage.googleapis.com/bananazone/futures/upbit/BTC/1min/2025-09-09.jsonl
https://storage.googleapis.com/bananazone/futures/okx/ETH/1min/2025-09-09.jsonl
https://storage.googleapis.com/bananazone/futures/coinbase/ADA/5s/2025-09-09.jsonl
```

## 📈 **Vercel Integration:**

Update your Vercel code to include futures data:

```javascript
// Fetch both spot and futures data
const spotData = await fetch('https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl');
const futuresData = await fetch('https://storage.googleapis.com/bananazone/futures/upbit/BTC/1min/2025-09-09.jsonl');

// Parse both datasets
const spotRecords = (await spotData.text()).trim().split('\n').map(line => JSON.parse(line));
const futuresRecords = (await futuresData.text()).trim().split('\n').map(line => JSON.parse(line));

// Combine for comprehensive market view
const marketData = {
  spot: spotRecords,
  futures: futuresRecords
};
```

## 🔍 **Monitoring:**

Check logs for:
```
✅ Success indicators:
- "Futures storage backend initialized"
- "UPBIT: WORKING with 4 assets"
- "Published 1min futures data"

❌ Error indicators:
- "Failed exchanges: bybit" (expected)
- "No assets available"
- "Connection failed"
```

## 🎯 **Benefits of This Setup:**

1. **Comprehensive Data:** Spot + Futures/Leverage
2. **Multiple Sources:** 3 different exchanges for redundancy
3. **Same Infrastructure:** Uses existing GCS bucket and storage system
4. **Automatic Failover:** Skips problematic exchanges
5. **Web-Ready:** Same headers as spot data for Vercel integration
6. **Real-time:** 5-second collection, 1-minute aggregation

## 🚀 **Ready to Deploy!**

The futures branch is tested and ready. Choose your deployment option and start collecting leverage/futures data alongside your existing spot data! 🎉