# Troubleshooting GCS Public URLs

## ✅ Current Status: URLs ARE Working!

Your URLs are actually working correctly:
- **Status:** HTTP 200 ✅
- **Data:** Returns valid JSON ✅  
- **Access:** Publicly accessible ✅

Example working URL:
```
https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl
```

## 🔍 Common Issues & Solutions

### 1. **"It's not working" - What to check:**

**A) Are you using the correct URL format?**
```
✅ Correct: https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl
❌ Wrong:   https://bananazone.storage.googleapis.com/coinbase/BTC/1min/2025-09-09.jsonl
```

**B) Is the date correct?**
```javascript
// Use today's date
const today = new Date().toISOString().split('T')[0]; // "2025-09-09"
const url = `https://storage.googleapis.com/bananazone/coinbase/BTC/1min/${today}.jsonl`;
```

**C) Does the file exist for that date?**
- Files are only created when the system is running
- Check if your Render deployment is active
- Data starts accumulating from when the system first runs

### 2. **Browser Shows "Download" Instead of JSON**

**Issue:** Old files have `application/octet-stream` content-type

**Solutions:**
1. **Wait for new files** - New files (after the latest deploy) will have correct headers
2. **Force refresh** - Clear browser cache
3. **Use in code** - Fetch API works regardless of content-type

### 3. **404 Not Found**

**Causes:**
- File doesn't exist for that date
- Render deployment not running
- Wrong URL format

**Check:**
```bash
# Test if bucket is accessible
curl -I https://storage.googleapis.com/bananazone/

# List available files (if bucket is public)
# Go to: https://console.cloud.google.com/storage/browser/bananazone
```

### 4. **403 Forbidden**

**Cause:** Bucket not public

**Fix:**
```bash
# Make bucket public
gsutil iam ch allUsers:objectViewer gs://bananazone
```

### 5. **CORS Issues in Vercel**

If you get CORS errors, the metadata headers should handle it, but you can also:

**Option A:** Use Next.js API route as proxy:
```javascript
// pages/api/crypto/[...params].js
export default async function handler(req, res) {
  const { params } = req.query;
  const [exchange, asset, timeframe, date] = params;
  
  const url = `https://storage.googleapis.com/bananazone/${exchange}/${asset}/${timeframe}/${date}.jsonl`;
  
  try {
    const response = await fetch(url);
    const data = await response.text();
    
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json');
    res.status(200).send(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}
```

**Option B:** Use Vercel's built-in CORS handling

## 🧪 Test Your URLs

### Manual Testing:
```bash
# Test in terminal
curl "https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl" | head -1

# Test in browser
# Open: https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl
```

### JavaScript Testing:
```javascript
async function testUrl() {
  const url = "https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl";
  
  try {
    const response = await fetch(url);
    console.log('Status:', response.status);
    console.log('Content-Type:', response.headers.get('content-type'));
    
    const text = await response.text();
    const lines = text.trim().split('\n');
    const firstRecord = JSON.parse(lines[0]);
    
    console.log('First record:', firstRecord);
    console.log('Total records:', lines.length);
  } catch (error) {
    console.error('Error:', error.message);
  }
}

testUrl();
```

## 📊 Available Data Files

Your system creates these file patterns:

### 1-Minute Data (Best for Charts):
```
https://storage.googleapis.com/bananazone/{exchange}/{asset}/1min/{date}.jsonl

Examples:
- https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl
- https://storage.googleapis.com/bananazone/coinbase/ETH/1min/2025-09-09.jsonl
- https://storage.googleapis.com/bananazone/kraken/BTC/1min/2025-09-09.jsonl
- https://storage.googleapis.com/bananazone/kraken/ETH/1min/2025-09-09.jsonl
```

### 5-Second Data (Real-time):
```
https://storage.googleapis.com/bananazone/{exchange}/{asset}/5s/{date}.jsonl
```

### Variables:
- `{exchange}`: `coinbase` or `kraken`
- `{asset}`: `BTC`, `ETH`, `ADA`, `XRP`  
- `{date}`: `YYYY-MM-DD` format (e.g., `2025-09-09`)

## 🚀 Next Steps

1. **Verify your Render deployment is running**
2. **Check the logs** to ensure data is being uploaded to GCS
3. **Test with today's date** (files only exist for dates when system was running)
4. **Use the Vercel integration example** provided in `vercel_integration_example.js`

## 💡 Pro Tips

- **Cache the data** in Vercel to reduce API calls
- **Use 1-minute data for charts** (cleaner, smaller files)
- **Use 5-second data for real-time updates**
- **Parse NDJSON correctly**: `text.split('\n').map(line => JSON.parse(line))`