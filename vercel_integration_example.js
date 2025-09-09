// vercel_integration_example.js
// Example of how to fetch and use the crypto data in Vercel/Next.js

/**
 * Fetch crypto data from GCS public URLs
 * @param {string} exchange - 'coinbase' or 'kraken'
 * @param {string} asset - 'BTC', 'ETH', 'ADA', or 'XRP'
 * @param {string} timeframe - '5s' or '1min'
 * @param {string} date - Date in 'YYYY-MM-DD' format
 * @returns {Promise<Array>} Array of crypto data records
 */
async function fetchCryptoData(exchange, asset, timeframe, date) {
  const bucketName = 'bananazone';
  const url = `https://storage.googleapis.com/${bucketName}/${exchange}/${asset}/${timeframe}/${date}.jsonl`;
  
  try {
    console.log(`Fetching: ${url}`);
    
    const response = await fetch(url);
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    const text = await response.text();
    
    // Parse NDJSON (each line is a separate JSON object)
    const records = text
      .trim()
      .split('\n')
      .filter(line => line.trim()) // Remove empty lines
      .map(line => JSON.parse(line));
    
    console.log(`Loaded ${records.length} records from ${exchange} ${asset}`);
    return records;
    
  } catch (error) {
    console.error(`Error fetching crypto data: ${error.message}`);
    return [];
  }
}

/**
 * Get the latest crypto prices for all assets
 * @param {string} date - Date in 'YYYY-MM-DD' format (default: today)
 * @returns {Promise<Object>} Latest prices by exchange and asset
 */
async function getLatestPrices(date = null) {
  if (!date) {
    date = new Date().toISOString().split('T')[0]; // Today's date
  }
  
  const exchanges = ['coinbase', 'kraken'];
  const assets = ['BTC', 'ETH', 'ADA', 'XRP'];
  const latestPrices = {};
  
  for (const exchange of exchanges) {
    latestPrices[exchange] = {};
    
    for (const asset of assets) {
      try {
        // Get 1-minute data (better for price display)
        const data = await fetchCryptoData(exchange, asset, '1min', date);
        
        if (data.length > 0) {
          const latestRecord = data[data.length - 1]; // Last record
          latestPrices[exchange][asset] = {
            price: latestRecord.mid,
            timestamp: latestRecord.t,
            spread: latestRecord.spread_L5_pct,
            volume_bids: latestRecord.vol_L50_bids,
            volume_asks: latestRecord.vol_L50_asks
          };
        }
      } catch (error) {
        console.warn(`Failed to get ${exchange} ${asset} data:`, error.message);
        latestPrices[exchange][asset] = null;
      }
    }
  }
  
  return latestPrices;
}

/**
 * Get historical data for charting
 * @param {string} exchange - Exchange name
 * @param {string} asset - Asset symbol
 * @param {Array<string>} dates - Array of dates to fetch
 * @returns {Promise<Array>} Combined historical data
 */
async function getHistoricalData(exchange, asset, dates) {
  const allData = [];
  
  for (const date of dates) {
    const dayData = await fetchCryptoData(exchange, asset, '1min', date);
    allData.push(...dayData);
  }
  
  // Sort by timestamp
  allData.sort((a, b) => new Date(a.t) - new Date(b.t));
  
  return allData;
}

/**
 * Example usage in a Next.js API route or React component
 */
async function exampleUsage() {
  try {
    // Get latest prices
    console.log('=== Latest Prices ===');
    const prices = await getLatestPrices();
    console.log('Coinbase BTC:', prices.coinbase?.BTC?.price);
    console.log('Kraken ETH:', prices.kraken?.ETH?.price);
    
    // Get historical data for charting
    console.log('\n=== Historical Data ===');
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 24*60*60*1000).toISOString().split('T')[0];
    
    const btcHistory = await getHistoricalData('coinbase', 'BTC', [yesterday, today]);
    console.log(`BTC historical data: ${btcHistory.length} records`);
    
    // Example chart data format
    const chartData = btcHistory.map(record => ({
      timestamp: new Date(record.t).getTime(),
      price: record.mid,
      volume: record.vol_L50_bids + record.vol_L50_asks
    }));
    
    console.log('Chart data sample:', chartData.slice(0, 3));
    
  } catch (error) {
    console.error('Example usage failed:', error);
  }
}

// Export for use in Next.js
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    fetchCryptoData,
    getLatestPrices,
    getHistoricalData
  };
}

// Example usage (uncomment to test)
// exampleUsage();