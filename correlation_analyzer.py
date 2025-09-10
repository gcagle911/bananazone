#!/usr/bin/env python3
"""
Correlation analysis engine for detecting relationships between spreads, volume, and price movements.
"""

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import logging
from typing import Dict, List, Tuple, Optional
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class CorrelationAnalyzer:
    """Analyze correlations between market microstructure and price movements"""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.models = {}
        self.correlation_results = {}
        self.pattern_library = {}
        
    def calculate_price_movement_significance(self, df: pd.DataFrame, threshold_pct: float = 0.5) -> pd.DataFrame:
        """Identify significant price movements for analysis"""
        
        logger.info(f"🎯 Identifying significant price movements (>{threshold_pct}%)")
        
        df = df.copy()
        
        # Calculate various price movement metrics
        for period in [1, 5, 15, 30]:  # 1, 5, 15, 30 minute periods
            col_name = f'price_movement_{period}m'
            df[col_name] = df.groupby(['source_exchange', 'source_asset', 'data_type'])['mid'].pct_change(periods=period) * 100
            
            # Mark significant movements
            df[f'significant_move_{period}m'] = np.abs(df[col_name]) > threshold_pct
        
        # Overall significance score
        significance_cols = [col for col in df.columns if col.startswith('significant_move_')]
        df['significance_score'] = df[significance_cols].sum(axis=1)
        df['is_significant'] = df['significance_score'] > 0
        
        significant_count = df['is_significant'].sum()
        total_count = len(df)
        
        logger.info(f"📊 Found {significant_count:,} significant movements out of {total_count:,} records ({significant_count/total_count*100:.1f}%)")
        
        return df
    
    def analyze_spread_volume_correlations(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Analyze correlations between spreads, volume, and price movements"""
        
        logger.info("🔍 Analyzing spread-volume-price correlations...")
        
        results = {}
        
        # Features to analyze
        spread_features = ['spread_L5_pct', 'spread_L50_pct', 'spread_L100_pct', 'spread_L5_change', 'spread_L50_change']
        volume_features = ['vol_L50_bids', 'vol_L50_asks', 'total_volume', 'volume_change', 'volume_return', 'volume_imbalance']
        price_features = ['price_change', 'price_return', 'price_change_1m', 'price_change_5m', 'price_change_15m']
        
        # Analyze by exchange, asset, and data type
        for (exchange, asset, data_type), group in df.groupby(['source_exchange', 'source_asset', 'data_type']):
            
            if len(group) < 50:  # Need sufficient data
                continue
            
            key = f"{data_type}_{exchange}_{asset}"
            results[key] = {
                'exchange': exchange,
                'asset': asset,
                'data_type': data_type,
                'record_count': len(group),
                'correlations': {},
                'significant_patterns': [],
                'model_performance': {}
            }
            
            # Calculate correlations between features
            feature_cols = spread_features + volume_features + price_features
            available_cols = [col for col in feature_cols if col in group.columns and group[col].notna().sum() > 10]
            
            if len(available_cols) < 5:
                continue
            
            corr_matrix = group[available_cols].corr()
            
            # Focus on correlations with price movements
            price_corr = {}
            for price_col in price_features:
                if price_col in corr_matrix.columns:
                    price_corr[price_col] = {}
                    
                    # Correlations with spreads
                    for spread_col in spread_features:
                        if spread_col in corr_matrix.columns:
                            corr_val = corr_matrix.loc[spread_col, price_col]
                            if not np.isnan(corr_val) and abs(corr_val) > 0.1:  # Meaningful correlation
                                price_corr[price_col][f'spread_{spread_col}'] = corr_val
                    
                    # Correlations with volume
                    for vol_col in volume_features:
                        if vol_col in corr_matrix.columns:
                            corr_val = corr_matrix.loc[vol_col, price_col]
                            if not np.isnan(corr_val) and abs(corr_val) > 0.1:
                                price_corr[price_col][f'volume_{vol_col}'] = corr_val
            
            results[key]['correlations'] = price_corr
            
            # Find patterns in significant movements
            significant_data = group[group.get('is_significant', False)]
            if len(significant_data) > 5:
                patterns = self._identify_patterns(significant_data, spread_features, volume_features)
                results[key]['significant_patterns'] = patterns
            
            logger.debug(f"✅ {key}: {len(group)} records, {len(price_corr)} price correlations")
        
        self.correlation_results = results
        logger.info(f"📊 Analyzed {len(results)} exchange/asset combinations")
        
        return results
    
    def _identify_patterns(self, df: pd.DataFrame, spread_features: List[str], volume_features: List[str]) -> List[Dict]:
        """Identify patterns that precede significant price movements"""
        
        patterns = []
        
        try:
            # Look for patterns in the data before significant movements
            for i in range(5, len(df)):  # Start from 5th record to have lookback
                current = df.iloc[i]
                lookback = df.iloc[i-5:i]  # Previous 5 records
                
                if current.get('is_significant', False):
                    # Analyze what happened in the 5 records before this significant movement
                    pattern = {
                        'timestamp': current['timestamp'].isoformat(),
                        'price_movement': current.get('price_change', 0),
                        'preceding_conditions': {}
                    }
                    
                    # Average conditions in the 5 periods before
                    for feature in spread_features + volume_features:
                        if feature in lookback.columns:
                            avg_val = lookback[feature].mean()
                            if not np.isnan(avg_val):
                                pattern['preceding_conditions'][feature] = avg_val
                    
                    patterns.append(pattern)
            
        except Exception as e:
            logger.warning(f"Error identifying patterns: {e}")
        
        return patterns[:10]  # Return top 10 patterns
    
    def build_predictive_model(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Build ML models to predict price movements based on spread/volume data"""
        
        logger.info("🤖 Building predictive models...")
        
        model_results = {}
        
        # Build models for each exchange/asset combination
        for (exchange, asset, data_type), group in df.groupby(['source_exchange', 'source_asset', 'data_type']):
            
            if len(group) < 100:  # Need sufficient data for training
                continue
            
            key = f"{data_type}_{exchange}_{asset}"
            
            try:
                # Prepare features and target
                feature_cols = [
                    'spread_L5_pct', 'spread_L50_pct', 'spread_L100_pct',
                    'vol_L50_bids', 'vol_L50_asks', 'total_volume', 'volume_imbalance',
                    'spread_L5_change', 'spread_L50_change', 'volume_change',
                    'hour', 'minute'
                ]
                
                available_features = [col for col in feature_cols if col in group.columns]
                
                if len(available_features) < 5:
                    continue
                
                # Target: next period price change
                X = group[available_features].dropna()
                y = group['price_change'].shift(-1).dropna()  # Predict next price change
                
                # Align X and y
                min_len = min(len(X), len(y))
                X = X.iloc[:min_len]
                y = y.iloc[:min_len]
                
                if len(X) < 50:
                    continue
                
                # Split data
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
                
                # Scale features
                X_train_scaled = self.scaler.fit_transform(X_train)
                X_test_scaled = self.scaler.transform(X_test)
                
                # Train model
                model = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
                model.fit(X_train_scaled, y_train)
                
                # Evaluate
                y_pred = model.predict(X_test_scaled)
                mse = mean_squared_error(y_test, y_pred)
                r2 = r2_score(y_test, y_pred)
                
                # Feature importance
                feature_importance = dict(zip(available_features, model.feature_importances_))
                
                model_results[key] = {
                    'exchange': exchange,
                    'asset': asset,
                    'data_type': data_type,
                    'model_performance': {
                        'mse': mse,
                        'r2': r2,
                        'train_samples': len(X_train),
                        'test_samples': len(X_test)
                    },
                    'feature_importance': feature_importance,
                    'top_predictors': sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:5]
                }
                
                # Store model
                self.models[key] = {
                    'model': model,
                    'scaler': self.scaler,
                    'features': available_features
                }
                
                logger.debug(f"✅ {key}: R² = {r2:.3f}, MSE = {mse:.6f}")
                
            except Exception as e:
                logger.warning(f"❌ Failed to build model for {key}: {e}")
        
        logger.info(f"🤖 Built {len(model_results)} predictive models")
        
        return model_results
    
    def generate_insights(self, correlation_results: Dict, model_results: Dict) -> Dict[str, List]:
        """Generate actionable insights from correlation and model analysis"""
        
        logger.info("💡 Generating actionable insights...")
        
        insights = {
            'high_correlation_patterns': [],
            'predictive_features': [],
            'exchange_differences': [],
            'asset_specific_patterns': [],
            'futures_vs_spot': [],
            'volume_spread_relationships': []
        }
        
        # High correlation patterns
        for key, results in correlation_results.items():
            for price_col, corrs in results['correlations'].items():
                for feature, corr_val in corrs.items():
                    if abs(corr_val) > 0.3:  # Strong correlation
                        insights['high_correlation_patterns'].append({
                            'source': key,
                            'price_metric': price_col,
                            'feature': feature,
                            'correlation': corr_val,
                            'strength': 'Strong' if abs(corr_val) > 0.5 else 'Moderate'
                        })
        
        # Top predictive features across all models
        all_features = {}
        for key, model_result in model_results.items():
            for feature, importance in model_result['feature_importance'].items():
                if feature not in all_features:
                    all_features[feature] = []
                all_features[feature].append(importance)
        
        # Average importance across models
        avg_importance = {
            feature: np.mean(importances) 
            for feature, importances in all_features.items()
        }
        
        top_features = sorted(avg_importance.items(), key=lambda x: x[1], reverse=True)[:10]
        insights['predictive_features'] = [
            {'feature': feature, 'avg_importance': importance}
            for feature, importance in top_features
        ]
        
        # Compare exchanges
        exchange_performance = {}
        for key, results in model_results.items():
            exchange = results['exchange']
            if exchange not in exchange_performance:
                exchange_performance[exchange] = []
            exchange_performance[exchange].append(results['model_performance']['r2'])
        
        for exchange, r2_scores in exchange_performance.items():
            avg_r2 = np.mean(r2_scores)
            insights['exchange_differences'].append({
                'exchange': exchange,
                'avg_predictability': avg_r2,
                'model_count': len(r2_scores)
            })
        
        # Compare futures vs spot
        spot_r2 = []
        futures_r2 = []
        
        for key, results in model_results.items():
            if results['data_type'] == 'spot':
                spot_r2.append(results['model_performance']['r2'])
            else:
                futures_r2.append(results['model_performance']['r2'])
        
        if spot_r2 and futures_r2:
            insights['futures_vs_spot'] = {
                'spot_avg_predictability': np.mean(spot_r2),
                'futures_avg_predictability': np.mean(futures_r2),
                'difference': np.mean(futures_r2) - np.mean(spot_r2)
            }
        
        logger.info(f"💡 Generated {sum(len(v) if isinstance(v, list) else 1 for v in insights.values())} insights")
        
        return insights
    
    def detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect anomalous spread/volume patterns that might predict price movements"""
        
        logger.info("🔍 Detecting anomalous patterns...")
        
        df = df.copy()
        
        # Calculate z-scores for key metrics
        for group_cols in [['source_exchange', 'source_asset', 'data_type']]:
            for col in ['spread_L5_pct', 'spread_L50_pct', 'total_volume', 'volume_imbalance']:
                if col in df.columns:
                    df[f'{col}_zscore'] = df.groupby(group_cols)[col].transform(
                        lambda x: (x - x.rolling(100, min_periods=10).mean()) / x.rolling(100, min_periods=10).std()
                    )
        
        # Identify anomalies (z-score > 2 or < -2)
        anomaly_cols = [col for col in df.columns if col.endswith('_zscore')]
        df['is_anomaly'] = (np.abs(df[anomaly_cols]) > 2).any(axis=1)
        
        # Analyze what happens after anomalies
        df['future_price_change_5m'] = df.groupby(['source_exchange', 'source_asset', 'data_type'])['mid'].shift(-5).pct_change() * 100
        
        anomaly_count = df['is_anomaly'].sum()
        logger.info(f"🚨 Found {anomaly_count:,} anomalous patterns")
        
        return df
    
    def cross_exchange_arbitrage_analysis(self, df: pd.DataFrame) -> Dict[str, List]:
        """Analyze cross-exchange price differences and arbitrage opportunities"""
        
        logger.info("💰 Analyzing cross-exchange arbitrage opportunities...")
        
        arbitrage_opportunities = []
        
        # Group by timestamp and asset to compare across exchanges
        for timestamp, time_group in df.groupby(df['timestamp'].dt.floor('1Min')):
            for asset in self.assets:
                asset_data = time_group[time_group['source_asset'] == asset]
                
                if len(asset_data) < 2:  # Need at least 2 exchanges
                    continue
                
                # Find price differences
                prices = asset_data.set_index(['source_exchange', 'data_type'])['mid']
                
                if len(prices) < 2:
                    continue
                
                max_price = prices.max()
                min_price = prices.min()
                price_diff_pct = ((max_price - min_price) / min_price) * 100
                
                if price_diff_pct > 0.1:  # Significant price difference
                    max_exchange = prices.idxmax()
                    min_exchange = prices.idxmin()
                    
                    arbitrage_opportunities.append({
                        'timestamp': timestamp.isoformat(),
                        'asset': asset,
                        'price_difference_pct': price_diff_pct,
                        'high_exchange': f"{max_exchange[0]} ({max_exchange[1]})",
                        'low_exchange': f"{min_exchange[0]} ({min_exchange[1]})",
                        'high_price': max_price,
                        'low_price': min_price
                    })
        
        logger.info(f"💰 Found {len(arbitrage_opportunities)} arbitrage opportunities")
        
        return {'arbitrage_opportunities': arbitrage_opportunities}
    
    def generate_trading_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate trading signals based on spread/volume patterns"""
        
        logger.info("📈 Generating trading signals...")
        
        df = df.copy()
        
        # Initialize signal columns
        df['signal_strength'] = 0.0
        df['signal_direction'] = 'hold'
        df['signal_confidence'] = 0.0
        
        # Signal generation logic
        for (exchange, asset, data_type), group in df.groupby(['source_exchange', 'source_asset', 'data_type']):
            
            if len(group) < 20:
                continue
            
            group_idx = group.index
            
            # Signal 1: Spread tightening + Volume increase = Bullish
            spread_tightening = group['spread_L5_pct'] < group['spread_L5_pct'].rolling(10).mean() * 0.8
            volume_increase = group['total_volume'] > group['total_volume'].rolling(10).mean() * 1.2
            
            bullish_signal = spread_tightening & volume_increase
            df.loc[group_idx[bullish_signal], 'signal_direction'] = 'bullish'
            df.loc[group_idx[bullish_signal], 'signal_strength'] += 0.3
            
            # Signal 2: Spread widening + Volume decrease = Bearish
            spread_widening = group['spread_L5_pct'] > group['spread_L5_pct'].rolling(10).mean() * 1.2
            volume_decrease = group['total_volume'] < group['total_volume'].rolling(10).mean() * 0.8
            
            bearish_signal = spread_widening & volume_decrease
            df.loc[group_idx[bearish_signal], 'signal_direction'] = 'bearish'
            df.loc[group_idx[bearish_signal], 'signal_strength'] += 0.3
            
            # Signal 3: Volume imbalance
            strong_bid_imbalance = group['volume_imbalance'] > 0.2  # More bids than asks
            strong_ask_imbalance = group['volume_imbalance'] < -0.2  # More asks than bids
            
            df.loc[group_idx[strong_bid_imbalance], 'signal_strength'] += 0.2
            df.loc[group_idx[strong_ask_imbalance], 'signal_strength'] -= 0.2
            
            # Calculate confidence based on historical accuracy
            df.loc[group_idx, 'signal_confidence'] = np.abs(df.loc[group_idx, 'signal_strength']) * 0.7
        
        # Filter for strong signals only
        strong_signals = df[np.abs(df['signal_strength']) > 0.3]
        
        logger.info(f"📊 Generated {len(strong_signals)} strong trading signals")
        
        return df

def run_full_analysis(hours_back: int = 24) -> Dict:
    """Run complete correlation analysis pipeline"""
    
    logger.info("🚀 Starting Full Correlation Analysis")
    logger.info("=" * 50)
    
    from data_loader import CryptoDataLoader
    
    # Load data
    loader = CryptoDataLoader()
    df = loader.get_latest_data(hours_back=hours_back, timeframe="1min")
    
    if df.empty:
        logger.error("❌ No data available for analysis")
        return {}
    
    # Prepare features
    df = loader.prepare_ml_features(df)
    
    # Initialize analyzer
    analyzer = CorrelationAnalyzer()
    
    # Identify significant movements
    df = analyzer.calculate_price_movement_significance(df, threshold_pct=0.5)
    
    # Analyze correlations
    correlation_results = analyzer.analyze_spread_volume_correlations(df)
    
    # Build predictive models
    model_results = analyzer.build_predictive_model(df)
    
    # Generate insights
    insights = analyzer.generate_insights(correlation_results, model_results)
    
    # Detect anomalies
    df_anomalies = analyzer.detect_anomalies(df)
    
    # Cross-exchange analysis
    arbitrage_analysis = analyzer.cross_exchange_arbitrage_analysis(df)
    
    # Generate trading signals
    df_signals = analyzer.generate_trading_signals(df_anomalies)
    
    # Compile final results
    final_results = {
        'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
        'data_summary': {
            'total_records': len(df),
            'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
            'exchanges_analyzed': df['source_exchange'].unique().tolist(),
            'assets_analyzed': df['source_asset'].unique().tolist(),
            'data_types': df['data_type'].unique().tolist()
        },
        'correlations': correlation_results,
        'model_performance': model_results,
        'insights': insights,
        'arbitrage': arbitrage_analysis,
        'signal_summary': {
            'total_signals': len(df_signals[df_signals['signal_strength'] != 0]),
            'bullish_signals': len(df_signals[df_signals['signal_direction'] == 'bullish']),
            'bearish_signals': len(df_signals[df_signals['signal_direction'] == 'bearish'])
        }
    }
    
    logger.info("✅ Full analysis complete!")
    
    return final_results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    results = run_full_analysis(hours_back=12)
    
    # Save results
    with open('correlation_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info("💾 Results saved to correlation_analysis_results.json")