#!/usr/bin/env python3
"""
Simplified ML scanner using only basic libraries to avoid deployment issues.
"""

import json
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import statistics
import math

from storage import upload_text, get_storage_backend

logger = logging.getLogger(__name__)

class SimpleCryptoAnalyzer:
    """Simplified analyzer using only standard libraries"""
    
    def __init__(self, bucket_name: str = "bananazone"):
        self.bucket_name = bucket_name
        self.base_url = f"https://storage.googleapis.com/{bucket_name}"
        
    def load_recent_data(self, hours_back: int = 6) -> Dict[str, List]:
        """Load recent data from all sources"""
        
        logger.info(f"📊 Loading data from last {hours_back} hours...")
        
        # Data sources
        spot_exchanges = ["coinbase", "kraken"]
        futures_exchanges = ["upbit", "okx", "coinbase"]
        assets = ["BTC", "ETH", "ADA", "XRP"]
        
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        all_data = {
            'spot': {},
            'futures': {}
        }
        
        # Load spot data
        for exchange in spot_exchanges:
            for asset in assets:
                url = f"{self.base_url}/{exchange}/{asset}/1min/{date}.jsonl"
                data = self._fetch_and_parse(url)
                if data:
                    all_data['spot'][f"{exchange}_{asset}"] = data
        
        # Load futures data
        for exchange in futures_exchanges:
            for asset in assets:
                url = f"{self.base_url}/futures/{exchange}/{asset}/1min/{date}.jsonl"
                data = self._fetch_and_parse(url)
                if data:
                    all_data['futures'][f"{exchange}_{asset}"] = data
        
        # Filter to recent hours
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        
        for data_type in all_data:
            for key, records in all_data[data_type].items():
                filtered_records = []
                for record in records:
                    try:
                        record_time = datetime.fromisoformat(record['t'].replace('Z', '+00:00'))
                        if record_time >= cutoff_time:
                            filtered_records.append(record)
                    except:
                        continue
                all_data[data_type][key] = filtered_records
        
        total_records = sum(len(records) for data_type in all_data.values() for records in data_type.values())
        logger.info(f"✅ Loaded {total_records:,} records")
        
        return all_data
    
    def _fetch_and_parse(self, url: str) -> Optional[List[Dict]]:
        """Fetch and parse a single JSONL file"""
        
        try:
            # Add cache-busting
            cache_bust_url = f"{url}?t={int(time.time())}"
            
            response = requests.get(cache_bust_url, timeout=30, headers={
                'Cache-Control': 'no-cache'
            })
            
            if response.status_code != 200:
                return None
            
            text = response.text.strip()
            if not text:
                return None
            
            records = []
            for line in text.split('\n'):
                if line.strip():
                    try:
                        records.append(json.loads(line.strip()))
                    except:
                        continue
            
            return records
            
        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None
    
    def analyze_correlations(self, data: Dict) -> Dict:
        """Simple correlation analysis using basic statistics"""
        
        logger.info("🔍 Analyzing correlations...")
        
        correlations = {}
        
        for data_type, datasets in data.items():
            for key, records in datasets.items():
                
                if len(records) < 20:
                    continue
                
                # Extract time series
                prices = [r['mid'] for r in records if 'mid' in r]
                spreads_L5 = [r['spread_L5_pct'] for r in records if 'spread_L5_pct' in r]
                volumes = [r.get('vol_L50_bids', 0) + r.get('vol_L50_asks', 0) for r in records]
                
                if len(prices) < 10 or len(spreads_L5) < 10:
                    continue
                
                # Calculate price changes
                price_changes = []
                for i in range(1, len(prices)):
                    change = ((prices[i] - prices[i-1]) / prices[i-1]) * 100
                    price_changes.append(change)
                
                # Calculate spread changes
                spread_changes = []
                for i in range(1, len(spreads_L5)):
                    change = spreads_L5[i] - spreads_L5[i-1]
                    spread_changes.append(change)
                
                # Simple correlation calculation
                if len(price_changes) >= 10 and len(spread_changes) >= 10:
                    min_len = min(len(price_changes), len(spread_changes))
                    
                    # Pearson correlation
                    corr = self._calculate_correlation(
                        spread_changes[:min_len], 
                        price_changes[:min_len]
                    )
                    
                    if abs(corr) > 0.1:  # Meaningful correlation
                        correlations[f"{data_type}_{key}"] = {
                            'spread_price_correlation': corr,
                            'data_points': min_len,
                            'avg_price_change': statistics.mean([abs(x) for x in price_changes]),
                            'avg_spread': statistics.mean(spreads_L5),
                            'avg_volume': statistics.mean(volumes) if volumes else 0
                        }
        
        logger.info(f"🔍 Found {len(correlations)} meaningful correlations")
        return correlations
    
    def _calculate_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient"""
        
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        try:
            n = len(x)
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(x[i] * y[i] for i in range(n))
            sum_x2 = sum(x[i] ** 2 for i in range(n))
            sum_y2 = sum(y[i] ** 2 for i in range(n))
            
            numerator = n * sum_xy - sum_x * sum_y
            denominator = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))
            
            if denominator == 0:
                return 0.0
            
            return numerator / denominator
            
        except:
            return 0.0
    
    def detect_patterns(self, data: Dict) -> Dict:
        """Detect simple patterns without heavy ML libraries"""
        
        logger.info("🔍 Detecting patterns...")
        
        patterns = {
            'spread_tightening_before_moves': [],
            'volume_surges': [],
            'cross_exchange_divergence': []
        }
        
        for data_type, datasets in data.items():
            for key, records in datasets.items():
                
                if len(records) < 10:
                    continue
                
                # Pattern 1: Spread tightening before price moves
                for i in range(5, len(records) - 5):
                    current = records[i]
                    prev_5 = records[i-5:i]
                    next_5 = records[i:i+5]
                    
                    # Check if spreads were tightening
                    prev_spreads = [r.get('spread_L5_pct', 0) for r in prev_5]
                    if len(prev_spreads) >= 5:
                        spread_trend = (prev_spreads[-1] - prev_spreads[0]) / len(prev_spreads)
                        
                        # Check subsequent price movement
                        if len(next_5) >= 2:
                            price_start = next_5[0]['mid']
                            price_end = next_5[-1]['mid']
                            price_change = ((price_end - price_start) / price_start) * 100
                            
                            if spread_trend < -0.001 and abs(price_change) > 0.2:  # Tightening + significant move
                                patterns['spread_tightening_before_moves'].append({
                                    'timestamp': current['t'],
                                    'source': key,
                                    'data_type': data_type,
                                    'spread_tightening_rate': spread_trend,
                                    'subsequent_price_change': price_change,
                                    'pattern_strength': abs(spread_trend) * abs(price_change)
                                })
                
                # Pattern 2: Volume surges
                volumes = [r.get('vol_L50_bids', 0) + r.get('vol_L50_asks', 0) for r in records]
                if len(volumes) >= 20:
                    avg_volume = statistics.mean(volumes)
                    
                    for i, record in enumerate(records):
                        current_volume = record.get('vol_L50_bids', 0) + record.get('vol_L50_asks', 0)
                        
                        if current_volume > avg_volume * 2:  # 2x average volume
                            patterns['volume_surges'].append({
                                'timestamp': record['t'],
                                'source': key,
                                'data_type': data_type,
                                'volume_multiple': current_volume / avg_volume,
                                'price': record['mid']
                            })
        
        logger.info(f"🔍 Pattern detection complete:")
        for pattern_type, pattern_list in patterns.items():
            logger.info(f"   {pattern_type}: {len(pattern_list)} patterns")
        
        return patterns
    
    def generate_insights(self, correlations: Dict, patterns: Dict, data: Dict) -> Dict:
        """Generate actionable insights"""
        
        # Find strongest correlations
        strong_correlations = [
            (key, corr_data) for key, corr_data in correlations.items()
            if abs(corr_data['spread_price_correlation']) > 0.3
        ]
        
        # Current market state
        latest_prices = {}
        for data_type, datasets in data.items():
            for key, records in datasets.items():
                if records:
                    latest = records[-1]
                    latest_prices[f"{data_type}_{key}"] = {
                        'price': latest['mid'],
                        'spread': latest.get('spread_L5_pct', 0),
                        'timestamp': latest['t']
                    }
        
        insights = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'strong_correlations': strong_correlations,
            'latest_market_state': latest_prices,
            'pattern_summary': {
                'spread_patterns': len(patterns.get('spread_tightening_before_moves', [])),
                'volume_patterns': len(patterns.get('volume_surges', [])),
                'divergence_patterns': len(patterns.get('cross_exchange_divergence', []))
            },
            'top_patterns': {
                'spread_tightening': patterns.get('spread_tightening_before_moves', [])[:5],
                'volume_surges': patterns.get('volume_surges', [])[:5]
            }
        }
        
        return insights
    
    def run_analysis_cycle(self):
        """Run one complete analysis cycle"""
        
        try:
            # Load data
            data = self.load_recent_data(hours_back=6)
            
            # Analyze correlations
            correlations = self.analyze_correlations(data)
            
            # Detect patterns
            patterns = self.detect_patterns(data)
            
            # Generate insights
            insights = self.generate_insights(correlations, patterns, data)
            
            # Save to GCS
            self.save_insights(insights)
            
            logger.info("✅ Analysis cycle complete")
            return insights
            
        except Exception as e:
            logger.error(f"💥 Analysis cycle failed: {e}")
            return None
    
    def save_insights(self, insights: Dict):
        """Save insights to GCS"""
        
        try:
            insights_json = json.dumps(insights, indent=2)
            
            # Save latest insights
            upload_text(self.bucket_name, "ml_insights/latest.json", insights_json)
            
            # Save timestamped version
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
            upload_text(self.bucket_name, f"ml_insights/historical/{timestamp}.json", insights_json)
            
            logger.info("💾 Saved insights to GCS")
            
        except Exception as e:
            logger.error(f"Failed to save insights: {e}")

def main():
    """Main loop for simplified ML scanner"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger.info("🤖 Starting Simplified ML Scanner")
    
    analyzer = SimpleCryptoAnalyzer()
    
    try:
        while True:
            logger.info("🔄 Starting analysis cycle...")
            
            start_time = time.time()
            insights = analyzer.run_analysis_cycle()
            cycle_time = time.time() - start_time
            
            if insights:
                strong_corr_count = len(insights.get('strong_correlations', []))
                pattern_count = sum(insights['pattern_summary'].values())
                
                logger.info(f"📊 Found {strong_corr_count} strong correlations, {pattern_count} patterns")
            
            logger.info(f"⏱️  Cycle completed in {cycle_time:.1f}s")
            logger.info("😴 Sleeping for 15 minutes...")
            
            time.sleep(900)  # 15 minutes
            
    except KeyboardInterrupt:
        logger.info("🛑 Stopping ML scanner...")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")

if __name__ == "__main__":
    main()