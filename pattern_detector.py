#!/usr/bin/env python3
"""
Advanced pattern detection for identifying spread/volume patterns that predict price movements.
"""

import numpy as np
import pandas as pd
from scipy import signal
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class PatternDetector:
    """Detect recurring patterns in spread/volume data that correlate with price movements"""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.pattern_library = {}
        self.signal_patterns = {}
        
    def detect_spread_patterns(self, df: pd.DataFrame) -> Dict[str, List]:
        """Detect patterns in spread behavior"""
        
        logger.info("🔍 Detecting spread patterns...")
        
        patterns = {
            'spread_compression': [],
            'spread_expansion': [],
            'spread_oscillation': [],
            'cross_layer_divergence': []
        }
        
        for (exchange, asset, data_type), group in df.groupby(['source_exchange', 'source_asset', 'data_type']):
            
            if len(group) < 30:
                continue
            
            group = group.sort_values('timestamp').copy()
            
            # Pattern 1: Spread Compression (spreads tightening across all layers)
            spread_cols = ['spread_L5_pct', 'spread_L50_pct', 'spread_L100_pct']
            available_spreads = [col for col in spread_cols if col in group.columns]
            
            if len(available_spreads) >= 2:
                # Calculate rolling correlation between spreads
                spread_data = group[available_spreads].rolling(10).corr().dropna()
                
                # Find periods where all spreads are compressing
                for i in range(5, len(group) - 5):
                    window = group.iloc[i-5:i+5]
                    
                    # Check if spreads are consistently decreasing
                    spread_trends = []
                    for col in available_spreads:
                        if col in window.columns:
                            trend = np.polyfit(range(len(window)), window[col], 1)[0]  # Linear trend
                            spread_trends.append(trend)
                    
                    if len(spread_trends) >= 2 and all(t < 0 for t in spread_trends):  # All decreasing
                        # Check subsequent price movement
                        future_window = group.iloc[i:i+10] if i+10 < len(group) else group.iloc[i:]
                        if len(future_window) > 1:
                            price_change = ((future_window['mid'].iloc[-1] - future_window['mid'].iloc[0]) / future_window['mid'].iloc[0]) * 100
                            
                            patterns['spread_compression'].append({
                                'timestamp': group.iloc[i]['timestamp'].isoformat(),
                                'exchange': exchange,
                                'asset': asset,
                                'data_type': data_type,
                                'spread_compression_rate': np.mean(spread_trends),
                                'subsequent_price_change': price_change,
                                'pattern_strength': abs(np.mean(spread_trends))
                            })
            
            # Pattern 2: Cross-layer spread divergence
            if 'spread_L5_pct' in group.columns and 'spread_L100_pct' in group.columns:
                group['spread_divergence'] = group['spread_L100_pct'] - group['spread_L5_pct']
                
                # Find unusual divergences
                divergence_zscore = (group['spread_divergence'] - group['spread_divergence'].rolling(50).mean()) / group['spread_divergence'].rolling(50).std()
                
                for idx in group[np.abs(divergence_zscore) > 2].index:
                    row = group.loc[idx]
                    
                    # Look at subsequent price movement
                    future_data = group[group['timestamp'] > row['timestamp']].head(10)
                    if len(future_data) > 0:
                        price_change = ((future_data['mid'].iloc[-1] - row['mid']) / row['mid']) * 100
                        
                        patterns['cross_layer_divergence'].append({
                            'timestamp': row['timestamp'].isoformat(),
                            'exchange': exchange,
                            'asset': asset,
                            'data_type': data_type,
                            'divergence_zscore': divergence_zscore.loc[idx],
                            'L5_spread': row['spread_L5_pct'],
                            'L100_spread': row['spread_L100_pct'],
                            'subsequent_price_change': price_change
                        })
        
        # Log pattern counts
        for pattern_type, pattern_list in patterns.items():
            logger.info(f"📊 {pattern_type}: {len(pattern_list)} patterns detected")
        
        return patterns
    
    def detect_volume_patterns(self, df: pd.DataFrame) -> Dict[str, List]:
        """Detect patterns in volume behavior"""
        
        logger.info("📈 Detecting volume patterns...")
        
        patterns = {
            'volume_surge': [],
            'volume_imbalance_shift': [],
            'volume_drying_up': [],
            'bid_ask_volume_divergence': []
        }
        
        for (exchange, asset, data_type), group in df.groupby(['source_exchange', 'source_asset', 'data_type']):
            
            if len(group) < 30:
                continue
            
            group = group.sort_values('timestamp').copy()
            
            # Pattern 1: Volume Surge
            if 'total_volume' in group.columns:
                volume_ma = group['total_volume'].rolling(20).mean()
                volume_surge = group['total_volume'] > (volume_ma * 2)  # 2x average volume
                
                for idx in group[volume_surge].index:
                    row = group.loc[idx]
                    
                    # Check price impact of volume surge
                    future_data = group[group['timestamp'] > row['timestamp']].head(5)
                    if len(future_data) > 0:
                        price_change = ((future_data['mid'].iloc[-1] - row['mid']) / row['mid']) * 100
                        
                        patterns['volume_surge'].append({
                            'timestamp': row['timestamp'].isoformat(),
                            'exchange': exchange,
                            'asset': asset,
                            'data_type': data_type,
                            'volume_multiple': row['total_volume'] / volume_ma.loc[idx],
                            'subsequent_price_change': price_change,
                            'volume_value': row['total_volume']
                        })
            
            # Pattern 2: Volume Imbalance Shifts
            if 'volume_imbalance' in group.columns:
                # Find sudden shifts in volume imbalance
                imbalance_change = group['volume_imbalance'].diff().abs()
                large_shifts = imbalance_change > imbalance_change.quantile(0.95)
                
                for idx in group[large_shifts].index:
                    row = group.loc[idx]
                    
                    future_data = group[group['timestamp'] > row['timestamp']].head(5)
                    if len(future_data) > 0:
                        price_change = ((future_data['mid'].iloc[-1] - row['mid']) / row['mid']) * 100
                        
                        patterns['volume_imbalance_shift'].append({
                            'timestamp': row['timestamp'].isoformat(),
                            'exchange': exchange,
                            'asset': asset,
                            'data_type': data_type,
                            'imbalance_before': group['volume_imbalance'].iloc[group.index.get_loc(idx) - 1] if group.index.get_loc(idx) > 0 else 0,
                            'imbalance_after': row['volume_imbalance'],
                            'imbalance_shift': imbalance_change.loc[idx],
                            'subsequent_price_change': price_change
                        })
        
        # Log pattern counts
        for pattern_type, pattern_list in patterns.items():
            logger.info(f"📈 {pattern_type}: {len(pattern_list)} patterns detected")
        
        return patterns
    
    def identify_predictive_sequences(self, df: pd.DataFrame, lookback_periods: int = 5) -> List[Dict]:
        """Identify sequences of spread/volume changes that predict price movements"""
        
        logger.info(f"🔮 Identifying predictive sequences (lookback: {lookback_periods} periods)...")
        
        predictive_sequences = []
        
        for (exchange, asset, data_type), group in df.groupby(['source_exchange', 'source_asset', 'data_type']):
            
            if len(group) < lookback_periods + 10:
                continue
            
            group = group.sort_values('timestamp').copy()
            
            # Look for sequences that precede significant price movements
            for i in range(lookback_periods, len(group) - 5):
                
                current_row = group.iloc[i]
                if not current_row.get('is_significant', False):
                    continue
                
                # Extract the sequence before the significant movement
                sequence = group.iloc[i-lookback_periods:i]
                
                # Calculate sequence characteristics
                sequence_features = {
                    'spread_trend': self._calculate_trend(sequence, 'spread_L5_pct'),
                    'volume_trend': self._calculate_trend(sequence, 'total_volume'),
                    'imbalance_trend': self._calculate_trend(sequence, 'volume_imbalance'),
                    'spread_volatility': sequence['spread_L5_pct'].std() if 'spread_L5_pct' in sequence.columns else 0,
                    'volume_volatility': sequence['total_volume'].std() if 'total_volume' in sequence.columns else 0
                }
                
                # Price movement that followed
                future_movement = current_row.get('price_change', 0)
                
                predictive_sequences.append({
                    'timestamp': current_row['timestamp'].isoformat(),
                    'exchange': exchange,
                    'asset': asset,
                    'data_type': data_type,
                    'sequence_features': sequence_features,
                    'predicted_movement': future_movement,
                    'movement_magnitude': abs(future_movement),
                    'movement_direction': 'up' if future_movement > 0 else 'down'
                })
        
        # Sort by movement magnitude to find most significant patterns
        predictive_sequences.sort(key=lambda x: x['movement_magnitude'], reverse=True)
        
        logger.info(f"🔮 Found {len(predictive_sequences)} predictive sequences")
        
        return predictive_sequences[:50]  # Return top 50
    
    def _calculate_trend(self, series_data: pd.DataFrame, column: str) -> float:
        """Calculate linear trend in a time series"""
        
        if column not in series_data.columns or len(series_data) < 3:
            return 0.0
        
        y = series_data[column].dropna()
        if len(y) < 3:
            return 0.0
        
        x = np.arange(len(y))
        slope, _, _, _, _ = stats.linregress(x, y)
        
        return slope
    
    def cluster_similar_patterns(self, patterns: List[Dict]) -> Dict[str, List]:
        """Cluster similar patterns to identify recurring themes"""
        
        logger.info("🎯 Clustering similar patterns...")
        
        if len(patterns) < 10:
            return {'clusters': []}
        
        # Extract numerical features for clustering
        feature_matrix = []
        pattern_metadata = []
        
        for pattern in patterns:
            if 'sequence_features' in pattern:
                features = pattern['sequence_features']
                feature_vector = [
                    features.get('spread_trend', 0),
                    features.get('volume_trend', 0),
                    features.get('imbalance_trend', 0),
                    features.get('spread_volatility', 0),
                    features.get('volume_volatility', 0),
                    pattern.get('movement_magnitude', 0)
                ]
                
                if not any(np.isnan(feature_vector)):
                    feature_matrix.append(feature_vector)
                    pattern_metadata.append(pattern)
        
        if len(feature_matrix) < 5:
            return {'clusters': []}
        
        # Standardize features
        feature_matrix = self.scaler.fit_transform(feature_matrix)
        
        # Cluster patterns
        clustering = DBSCAN(eps=0.5, min_samples=3)
        cluster_labels = clustering.fit_predict(feature_matrix)
        
        # Group patterns by cluster
        clusters = {}
        for i, label in enumerate(cluster_labels):
            if label == -1:  # Noise
                continue
            
            if label not in clusters:
                clusters[label] = []
            
            clusters[label].append(pattern_metadata[i])
        
        logger.info(f"🎯 Found {len(clusters)} pattern clusters")
        
        # Analyze each cluster
        cluster_analysis = []
        for cluster_id, cluster_patterns in clusters.items():
            
            # Calculate cluster characteristics
            movements = [p['predicted_movement'] for p in cluster_patterns]
            
            cluster_info = {
                'cluster_id': cluster_id,
                'pattern_count': len(cluster_patterns),
                'avg_movement': np.mean(movements),
                'movement_std': np.std(movements),
                'movement_direction': 'bullish' if np.mean(movements) > 0 else 'bearish',
                'consistency': 1 - (np.std(movements) / (np.mean(np.abs(movements)) + 1e-6)),
                'examples': cluster_patterns[:3]  # Top 3 examples
            }
            
            cluster_analysis.append(cluster_info)
        
        return {'clusters': cluster_analysis}

def run_pattern_analysis(df: pd.DataFrame) -> Dict:
    """Run complete pattern analysis on the data"""
    
    logger.info("🎯 Running Pattern Analysis")
    logger.info("=" * 30)
    
    detector = PatternDetector()
    
    # Detect spread patterns
    spread_patterns = detector.detect_spread_patterns(df)
    
    # Detect volume patterns
    volume_patterns = detector.detect_volume_patterns(df)
    
    # Identify predictive sequences
    predictive_sequences = detector.identify_predictive_sequences(df)
    
    # Cluster similar patterns
    pattern_clusters = detector.cluster_similar_patterns(predictive_sequences)
    
    # Compile results
    analysis_results = {
        'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
        'data_summary': {
            'total_records': len(df),
            'exchanges': df['source_exchange'].unique().tolist(),
            'assets': df['source_asset'].unique().tolist(),
            'data_types': df['data_type'].unique().tolist()
        },
        'spread_patterns': spread_patterns,
        'volume_patterns': volume_patterns,
        'predictive_sequences': predictive_sequences,
        'pattern_clusters': pattern_clusters,
        'summary_stats': {
            'total_patterns': sum(len(v) for v in spread_patterns.values()) + sum(len(v) for v in volume_patterns.values()),
            'predictive_sequences_count': len(predictive_sequences),
            'pattern_clusters_count': len(pattern_clusters.get('clusters', []))
        }
    }
    
    logger.info(f"✅ Pattern analysis complete:")
    logger.info(f"   Total patterns: {analysis_results['summary_stats']['total_patterns']}")
    logger.info(f"   Predictive sequences: {analysis_results['summary_stats']['predictive_sequences_count']}")
    logger.info(f"   Pattern clusters: {analysis_results['summary_stats']['pattern_clusters_count']}")
    
    return analysis_results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample data
    from data_loader import CryptoDataLoader
    
    loader = CryptoDataLoader()
    df = loader.get_latest_data(hours_back=12)
    
    if not df.empty:
        df = loader.prepare_ml_features(df)
        results = run_pattern_analysis(df)
        
        # Save results
        with open('pattern_analysis_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info("💾 Pattern analysis results saved")
    else:
        logger.error("❌ No data available for pattern analysis")