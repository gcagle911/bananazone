#!/usr/bin/env python3
"""
Data loader for ML analysis - reads all crypto data from GCS bucket.
Handles both spot and futures data across all exchanges and assets.
"""

import json
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logger = logging.getLogger(__name__)

class CryptoDataLoader:
    """Load and preprocess crypto data from GCS for ML analysis"""
    
    def __init__(self, bucket_name: str = "bananazone"):
        self.bucket_name = bucket_name
        self.base_url = f"https://storage.googleapis.com/{bucket_name}"
        
        # Define data sources
        self.spot_exchanges = ["coinbase", "kraken"]
        self.futures_exchanges = ["upbit", "okx", "coinbase"]  # From futures branch
        self.assets = ["BTC", "ETH", "ADA", "XRP"]
        self.timeframes = ["1min", "5s"]
        
        # Cache for loaded data
        self.data_cache = {}
        self.cache_expiry = {}
        self.cache_duration = 300  # 5 minutes
        
    def get_available_dates(self, days_back: int = 7) -> List[str]:
        """Get list of available dates for analysis"""
        dates = []
        today = datetime.now(timezone.utc)
        
        for i in range(days_back):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            dates.append(date)
        
        return dates
    
    def load_single_file(self, exchange: str, asset: str, timeframe: str, date: str, data_type: str = "spot") -> Optional[pd.DataFrame]:
        """Load a single data file and return as DataFrame"""
        
        # Construct URL based on data type
        if data_type == "spot":
            url = f"{self.base_url}/{exchange}/{asset}/{timeframe}/{date}.jsonl"
        else:  # futures
            url = f"{self.base_url}/futures/{exchange}/{asset}/{timeframe}/{date}.jsonl"
        
        cache_key = f"{data_type}_{exchange}_{asset}_{timeframe}_{date}"
        
        # Check cache
        if cache_key in self.data_cache and time.time() < self.cache_expiry.get(cache_key, 0):
            logger.debug(f"Using cached data for {cache_key}")
            return self.data_cache[cache_key]
        
        try:
            # Add cache-busting to ensure fresh data
            cache_bust_url = f"{url}?t={int(time.time())}"
            
            response = requests.get(cache_bust_url, timeout=30, headers={
                'Cache-Control': 'no-cache, no-store, must-revalidate'
            })
            
            if response.status_code == 404:
                logger.debug(f"File not found: {url}")
                return None
            elif response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} for {url}")
                return None
            
            text = response.text.strip()
            if not text:
                logger.debug(f"Empty file: {url}")
                return None
            
            # Parse NDJSON
            records = []
            for line_num, line in enumerate(text.split('\n')):
                if line.strip():
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON parse error in {url} line {line_num}: {e}")
            
            if not records:
                logger.debug(f"No valid records in {url}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['t'])
            df = df.sort_values('timestamp')
            
            # Add metadata columns
            df['data_type'] = data_type
            df['source_exchange'] = exchange
            df['source_asset'] = asset
            df['source_timeframe'] = timeframe
            df['source_date'] = date
            
            # Cache the result
            self.data_cache[cache_key] = df
            self.cache_expiry[cache_key] = time.time() + self.cache_duration
            
            logger.debug(f"Loaded {len(df)} records from {exchange} {asset} {data_type}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading {url}: {e}")
            return None
    
    def load_all_data(self, dates: List[str], timeframe: str = "1min", include_futures: bool = True) -> pd.DataFrame:
        """Load all available data across exchanges, assets, and dates"""
        
        logger.info(f"📊 Loading all {timeframe} data for {len(dates)} dates...")
        
        all_dataframes = []
        tasks = []
        
        # Create tasks for parallel loading
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Spot data
            for exchange in self.spot_exchanges:
                for asset in self.assets:
                    for date in dates:
                        task = executor.submit(
                            self.load_single_file, exchange, asset, timeframe, date, "spot"
                        )
                        tasks.append((task, f"spot_{exchange}_{asset}_{date}"))
            
            # Futures data (if requested)
            if include_futures:
                for exchange in self.futures_exchanges:
                    for asset in self.assets:
                        for date in dates:
                            task = executor.submit(
                                self.load_single_file, exchange, asset, timeframe, date, "futures"
                            )
                            tasks.append((task, f"futures_{exchange}_{asset}_{date}"))
            
            # Collect results
            for task, task_name in tasks:
                try:
                    df = task.result(timeout=60)
                    if df is not None and len(df) > 0:
                        all_dataframes.append(df)
                        logger.debug(f"✅ {task_name}: {len(df)} records")
                    else:
                        logger.debug(f"⚠️  {task_name}: No data")
                except Exception as e:
                    logger.warning(f"❌ {task_name}: {e}")
        
        if not all_dataframes:
            logger.error("No data loaded from any source!")
            return pd.DataFrame()
        
        # Combine all data
        logger.info(f"🔄 Combining {len(all_dataframes)} datasets...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        combined_df = combined_df.sort_values('timestamp')
        
        logger.info(f"✅ Loaded {len(combined_df):,} total records")
        logger.info(f"📅 Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
        
        # Log data summary
        summary = combined_df.groupby(['data_type', 'source_exchange', 'source_asset']).size()
        logger.info(f"📊 Data summary:")
        for (data_type, exchange, asset), count in summary.items():
            logger.info(f"   {data_type:7} {exchange:8} {asset:3}: {count:,} records")
        
        return combined_df
    
    def get_latest_data(self, hours_back: int = 24, timeframe: str = "1min") -> pd.DataFrame:
        """Get the most recent data for analysis"""
        
        # Calculate date range
        now = datetime.now(timezone.utc)
        dates = []
        
        for i in range(3):  # Last 3 days to ensure we get data
            date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            dates.append(date)
        
        # Load all data
        df = self.load_all_data(dates, timeframe=timeframe)
        
        if df.empty:
            return df
        
        # Filter to last N hours
        cutoff_time = now - timedelta(hours=hours_back)
        df_recent = df[df['timestamp'] >= cutoff_time].copy()
        
        logger.info(f"🕐 Filtered to last {hours_back} hours: {len(df_recent):,} records")
        
        return df_recent
    
    def prepare_ml_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for ML analysis"""
        
        if df.empty:
            return df
        
        logger.info("🔧 Engineering features for ML analysis...")
        
        # Sort by timestamp
        df = df.sort_values('timestamp').copy()
        
        # Calculate price changes and returns
        for group_cols in [['source_exchange', 'source_asset', 'data_type']]:
            df['price_change'] = df.groupby(group_cols)['mid'].diff()
            df['price_return'] = df.groupby(group_cols)['mid'].pct_change()
            df['price_change_1m'] = df.groupby(group_cols)['mid'].diff(periods=1)
            df['price_change_5m'] = df.groupby(group_cols)['mid'].diff(periods=5)
            df['price_change_15m'] = df.groupby(group_cols)['mid'].diff(periods=15)
        
        # Calculate moving averages for spreads and volumes
        for col in ['spread_L5_pct', 'spread_L50_pct', 'spread_L100_pct', 'vol_L50_bids', 'vol_L50_asks']:
            df[f'{col}_ma_5'] = df.groupby(['source_exchange', 'source_asset', 'data_type'])[col].rolling(5).mean().reset_index(0, drop=True)
            df[f'{col}_ma_15'] = df.groupby(['source_exchange', 'source_asset', 'data_type'])[col].rolling(15).mean().reset_index(0, drop=True)
        
        # Calculate spread changes
        df['spread_L5_change'] = df.groupby(['source_exchange', 'source_asset', 'data_type'])['spread_L5_pct'].diff()
        df['spread_L50_change'] = df.groupby(['source_exchange', 'source_asset', 'data_type'])['spread_L50_pct'].diff()
        
        # Calculate volume changes
        df['total_volume'] = df['vol_L50_bids'] + df['vol_L50_asks']
        df['volume_change'] = df.groupby(['source_exchange', 'source_asset', 'data_type'])['total_volume'].diff()
        df['volume_return'] = df.groupby(['source_exchange', 'source_asset', 'data_type'])['total_volume'].pct_change()
        
        # Calculate volume imbalance
        df['volume_imbalance'] = (df['vol_L50_bids'] - df['vol_L50_asks']) / (df['vol_L50_bids'] + df['vol_L50_asks'])
        
        # Time-based features
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        # Drop rows with NaN values from calculations
        df = df.dropna()
        
        logger.info(f"✅ Feature engineering complete: {len(df):,} records with {len(df.columns)} features")
        
        return df

def test_data_loading():
    """Test the data loading functionality"""
    
    logger.info("🧪 Testing Data Loading System")
    logger.info("=" * 40)
    
    loader = CryptoDataLoader()
    
    # Test loading recent data
    try:
        df = loader.get_latest_data(hours_back=6, timeframe="1min")
        
        if df.empty:
            logger.error("❌ No data loaded!")
            return False
        
        logger.info(f"✅ Loaded {len(df):,} records")
        logger.info(f"📊 Columns: {list(df.columns)}")
        logger.info(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        # Show data summary
        summary = df.groupby(['data_type', 'source_exchange', 'source_asset']).size()
        logger.info(f"📈 Data sources:")
        for (data_type, exchange, asset), count in summary.head(10).items():
            logger.info(f"   {data_type:7} {exchange:8} {asset:3}: {count:,} records")
        
        # Test feature engineering
        df_features = loader.prepare_ml_features(df)
        logger.info(f"🔧 Features: {len(df_features)} records with {len(df_features.columns)} columns")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Data loading test failed: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_data_loading()