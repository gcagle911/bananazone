# 🤖 ML Correlation Analysis System

This branch provides **machine learning analysis** of your crypto data to detect correlations between spreads, volume, and price movements across all exchanges.

## 🎯 **What It Does:**

### **📊 Correlation Detection:**
- **Spread Analysis:** How spread changes correlate with price movements
- **Volume Analysis:** How volume patterns predict price direction
- **Cross-Exchange:** Compare patterns across Coinbase, Kraken, Upbit, OKX
- **Spot vs Futures:** Analyze differences between spot and leverage markets

### **🔍 Pattern Recognition:**
- **Spread Compression:** Detect when spreads tighten before price moves
- **Volume Surges:** Identify volume spikes that predict direction
- **Imbalance Shifts:** Track bid/ask volume imbalances
- **Arbitrage Opportunities:** Find price differences across exchanges

### **🤖 Machine Learning:**
- **Predictive Models:** Random Forest models for each exchange/asset
- **Feature Importance:** Identify which spread/volume metrics matter most
- **Trading Signals:** Generate buy/sell signals based on patterns
- **Anomaly Detection:** Flag unusual market conditions

## 🗂️ **Output Structure:**

### **ML Insights APIs:**
```
bananazone/
└── ml_insights/
    ├── latest.json                    # Current insights (for Vercel)
    ├── assets/
    │   ├── btc.json                   # BTC-specific insights
    │   ├── eth.json                   # ETH-specific insights
    │   └── ada.json, xrp.json
    └── historical/
        └── 2025-09-09_14-30.json      # Timestamped analysis
```

### **Public API URLs:**
```
https://storage.googleapis.com/bananazone/ml_insights/latest.json
https://storage.googleapis.com/bananazone/ml_insights/assets/btc.json
https://storage.googleapis.com/bananazone/ml_insights/assets/eth.json
```

## 📊 **Data Analysis:**

### **Input Data Sources:**
- ✅ **Spot Data:** Coinbase + Kraken (existing)
- ✅ **Futures Data:** Upbit + OKX + Coinbase futures (new)
- ✅ **All Assets:** BTC, ETH, ADA, XRP
- ✅ **Real-time:** Analyzes latest 6-24 hours of data

### **Features Analyzed:**
```python
# Spread metrics
- spread_L5_pct, spread_L50_pct, spread_L100_pct
- spread_L5_change, spread_L50_change

# Volume metrics  
- vol_L50_bids, vol_L50_asks, total_volume
- volume_change, volume_return, volume_imbalance

# Price metrics
- price_change, price_return
- price_change_1m, price_change_5m, price_change_15m
```

## 🚀 **Deployment Options:**

### **Option 1: Separate ML Service (Recommended)**
Create new Render Background Worker:
- **Branch:** `ml-correlation-analysis`
- **Start Command:** `python3 ml_scanner.py`
- **Build Command:** `pip install -r requirements.txt && pip install -r ml_requirements.txt`

### **Option 2: Add to Existing Service**
Update existing Render start command:
```bash
python3 ml_scanner.py & python3 logger.py
```

### **Option 3: Scheduled Analysis**
Run analysis every few hours:
```bash
python3 ml_insights_api.py
```

## 📈 **Vercel Integration:**

### **Access ML Insights:**
```javascript
// Get latest ML analysis
const mlData = await fetch('https://storage.googleapis.com/bananazone/ml_insights/latest.json');
const insights = await mlData.json();

// Current trading signals
console.log('Trading signals:', insights.current_signals);

// Top correlations
console.log('Top correlations:', insights.top_correlations);

// Arbitrage opportunities
console.log('Arbitrage:', insights.arbitrage_opportunities);

// Asset-specific insights
const btcInsights = await fetch('https://storage.googleapis.com/bananazone/ml_insights/assets/btc.json');
const btcData = await btcInsights.json();
```

## 🔍 **Analysis Features:**

### **1. Correlation Analysis:**
- Detects relationships between spread changes and price movements
- Identifies volume patterns that predict price direction
- Compares predictive power across exchanges

### **2. Pattern Detection:**
- **Spread Compression:** Spreads tightening before price moves
- **Volume Surges:** Large volume increases and their price impact
- **Cross-layer Divergence:** When different spread layers behave differently
- **Imbalance Shifts:** Changes in bid/ask volume ratios

### **3. Predictive Modeling:**
- Random Forest models for each exchange/asset combination
- Feature importance ranking (which metrics matter most)
- Model performance tracking (R² scores)
- Real-time prediction capability

### **4. Trading Signals:**
- **Bullish:** Spread tightening + volume increase
- **Bearish:** Spread widening + volume decrease
- **Confidence scoring** based on historical accuracy
- **Signal strength** from 0-1 scale

### **5. Arbitrage Detection:**
- Cross-exchange price differences
- Spot vs futures arbitrage opportunities
- Real-time opportunity alerts

## 📊 **Example Insights Output:**

```json
{
  "timestamp": "2025-09-09T21:45:00Z",
  "top_correlations": [
    {
      "source": "spot_coinbase_BTC",
      "feature": "volume_total_volume",
      "price_metric": "price_change_5m",
      "correlation": 0.67,
      "strength": "Strong"
    }
  ],
  "current_signals": [
    {
      "exchange": "coinbase",
      "asset": "BTC", 
      "direction": "bullish",
      "strength": 0.8,
      "confidence": 0.75
    }
  ],
  "arbitrage_opportunities": [
    {
      "asset": "ETH",
      "price_difference_pct": 0.15,
      "high_exchange": "kraken (spot)",
      "low_exchange": "upbit (futures)"
    }
  ]
}
```

## 🎯 **Benefits:**

✅ **Real-time Insights** - Continuous analysis of all your data  
✅ **Cross-Exchange Analysis** - Compare patterns across all sources  
✅ **Spot + Futures** - Comprehensive market view  
✅ **Predictive Models** - ML-based price movement prediction  
✅ **Trading Signals** - Actionable buy/sell recommendations  
✅ **Arbitrage Detection** - Find profitable price differences  
✅ **API Ready** - JSON endpoints for Vercel integration  

## 🔧 **Configuration:**

Edit analysis parameters in the ML scripts:
- **Scan interval:** How often to run analysis (default: 15 minutes)
- **Lookback period:** How much historical data to analyze (default: 6 hours)
- **Significance threshold:** What constitutes a "significant" price movement (default: 0.5%)
- **Correlation threshold:** Minimum correlation to report (default: 0.2)

This ML system will continuously analyze your crypto data and provide actionable insights for trading and market understanding! 🎯