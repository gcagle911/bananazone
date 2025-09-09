// vercel_cache_solution.js
// Solution for GCS caching inconsistency in Vercel applications

/**
 * Fetch crypto data with cache-busting to ensure fresh data
 * This solves the issue where public GCS URLs serve stale cached data
 * while authenticated URLs show current data.
 */

class CryptoDataFetcher {
  constructor(bucketName = 'bananazone') {
    this.bucketName = bucketName;
    this.baseUrl = `https://storage.googleapis.com/${bucketName}`;
    
    // Cache for storing fresh data temporarily
    this.cache = new Map();
    this.cacheTimeout = 30000; // 30 seconds
  }

  /**
   * Generate cache-busting URL
   */
  getCacheBustingUrl(path) {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(7);
    
    // Multiple cache-busting strategies
    return `${this.baseUrl}/${path}?t=${timestamp}&v=${random}&cb=${Date.now()}`;
  }

  /**
   * Fetch with aggressive cache-busting headers
   */
  async fetchWithCacheBusting(url, maxRetries = 3) {
    const headers = {
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0',
      // Add random header to bypass any proxy caching
      'X-Requested-With': `fetch-${Date.now()}-${Math.random()}`
    };

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        console.log(`Attempt ${attempt}: Fetching ${url}`);
        
        const response = await fetch(url, { 
          headers,
          cache: 'no-store'  // Prevent browser caching
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        // Verify we got fresh data by checking headers
        const lastModified = response.headers.get('last-modified');
        const etag = response.headers.get('etag');
        
        console.log(`Response headers - Last-Modified: ${lastModified}, ETag: ${etag}`);

        return response;

      } catch (error) {
        console.warn(`Attempt ${attempt} failed:`, error.message);
        
        if (attempt === maxRetries) {
          throw error;
        }
        
        // Wait before retry with exponential backoff
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  /**
   * Fetch crypto data with consistency checks
   */
  async fetchCryptoData(exchange, asset, timeframe, date, options = {}) {
    const {
      maxRetries = 3,
      consistencyCheck = true,
      useCache = true
    } = options;

    const path = `${exchange}/${asset}/${timeframe}/${date}.jsonl`;
    const cacheKey = `${path}-${Math.floor(Date.now() / this.cacheTimeout)}`;
    
    // Check cache first
    if (useCache && this.cache.has(cacheKey)) {
      console.log('Returning cached data');
      return this.cache.get(cacheKey);
    }

    // Try multiple URLs with different cache-busting strategies
    const urls = [
      this.getCacheBustingUrl(path),
      `${this.baseUrl}/${path}?nocache=${Date.now()}`,
      `${this.baseUrl}/${path}?_=${Math.random()}`
    ];

    let bestResult = null;
    let latestTimestamp = null;

    for (const url of urls) {
      try {
        const response = await this.fetchWithCacheBusting(url, 2);
        const text = await response.text();
        
        if (!text.trim()) {
          console.warn(`Empty response from ${url}`);
          continue;
        }

        const lines = text.trim().split('\n').filter(line => line.trim());
        
        if (lines.length === 0) {
          console.warn(`No data lines in response from ${url}`);
          continue;
        }

        // Parse and validate data
        const records = lines.map(line => {
          try {
            return JSON.parse(line);
          } catch (e) {
            console.warn('Failed to parse line:', line.substring(0, 100));
            return null;
          }
        }).filter(record => record !== null);

        if (records.length === 0) {
          console.warn(`No valid records from ${url}`);
          continue;
        }

        // Check data freshness
        const lastRecord = records[records.length - 1];
        const timestamp = lastRecord.t;
        
        console.log(`URL ${url} returned ${records.length} records, latest: ${timestamp}`);

        // Keep the result with the most recent timestamp
        if (!latestTimestamp || timestamp > latestTimestamp) {
          latestTimestamp = timestamp;
          bestResult = {
            records,
            metadata: {
              url,
              timestamp,
              recordCount: records.length,
              fetchTime: new Date().toISOString()
            }
          };
        }

      } catch (error) {
        console.warn(`Failed to fetch from ${url}:`, error.message);
      }
    }

    if (!bestResult) {
      throw new Error(`Failed to fetch data from all URLs for ${exchange} ${asset}`);
    }

    // Consistency check - verify data makes sense
    if (consistencyCheck) {
      const { records } = bestResult;
      const lastRecord = records[records.length - 1];
      const dataAge = (Date.now() - new Date(lastRecord.t).getTime()) / (1000 * 60);
      
      if (dataAge > 60) { // More than 1 hour old
        console.warn(`Data seems stale: ${dataAge.toFixed(1)} minutes old`);
      }
      
      if (records.length < 10) {
        console.warn(`Suspiciously few records: ${records.length}`);
      }
    }

    // Cache the result
    if (useCache) {
      this.cache.set(cacheKey, bestResult);
      
      // Clean old cache entries
      for (const [key] of this.cache) {
        if (!key.endsWith(`-${Math.floor(Date.now() / this.cacheTimeout)}`)) {
          this.cache.delete(key);
        }
      }
    }

    console.log(`Returning ${bestResult.records.length} records from ${bestResult.metadata.url}`);
    return bestResult;
  }

  /**
   * Get latest prices for all assets with cache-busting
   */
  async getLatestPrices(date = null, options = {}) {
    if (!date) {
      date = new Date().toISOString().split('T')[0];
    }

    const exchanges = ['coinbase', 'kraken'];
    const assets = ['BTC', 'ETH', 'ADA', 'XRP'];
    const results = {};

    for (const exchange of exchanges) {
      results[exchange] = {};
      
      for (const asset of assets) {
        try {
          const data = await this.fetchCryptoData(exchange, asset, '1min', date, options);
          const lastRecord = data.records[data.records.length - 1];
          
          results[exchange][asset] = {
            price: lastRecord.mid,
            timestamp: lastRecord.t,
            spread: lastRecord.spread_L5_pct,
            volume: lastRecord.vol_L50_bids + lastRecord.vol_L50_asks,
            recordCount: data.records.length,
            fetchTime: data.metadata.fetchTime
          };
          
        } catch (error) {
          console.error(`Failed to fetch ${exchange} ${asset}:`, error.message);
          results[exchange][asset] = null;
        }
      }
    }

    return results;
  }
}

// Usage examples
async function exampleUsage() {
  const fetcher = new CryptoDataFetcher();

  try {
    // Get fresh data with cache-busting
    console.log('Fetching latest prices with cache-busting...');
    const prices = await fetcher.getLatestPrices();
    
    console.log('Coinbase BTC:', prices.coinbase?.BTC?.price);
    console.log('Kraken ETH:', prices.kraken?.ETH?.price);

    // Get historical data for a specific asset
    console.log('\nFetching Coinbase BTC historical data...');
    const btcData = await fetcher.fetchCryptoData('coinbase', 'BTC', '1min', '2025-09-09');
    
    console.log(`Got ${btcData.records.length} BTC records`);
    console.log('Latest record:', btcData.records[btcData.records.length - 1]);

  } catch (error) {
    console.error('Example failed:', error);
  }
}

// Export for use in Next.js/Vercel
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { CryptoDataFetcher };
}

// For browser/ES6 modules
if (typeof window !== 'undefined') {
  window.CryptoDataFetcher = CryptoDataFetcher;
}

// Uncomment to test
// exampleUsage();