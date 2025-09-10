#!/usr/bin/env python3
"""
ML Insights API - Provides real-time insights from correlation analysis.
Creates JSON endpoints for accessing ML findings.
"""

import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import logging

from data_loader import CryptoDataLoader
from correlation_analyzer import CorrelationAnalyzer
from pattern_detector import PatternDetector, run_pattern_analysis
from storage import upload_text, get_storage_backend

logger = logging.getLogger(__name__)

class MLInsightsAPI:
    """Generate and serve ML insights as JSON APIs"""
    
    def __init__(self, bucket_name: str = "bananazone"):
        self.bucket_name = bucket_name
        self.data_loader = CryptoDataLoader(bucket_name)
        self.analyzer = CorrelationAnalyzer()
        self.pattern_detector = PatternDetector()
        
    def generate_live_insights(self) -> Dict:
        """Generate current live insights from all data"""
        
        logger.info("🔍 Generating live ML insights...")
        
        # Load recent data
        df = self.data_loader.get_latest_data(hours_back=6, timeframe="1min")
        
        if df.empty:
            return {"error": "No data available", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        # Prepare features
        df = self.data_loader.prepare_ml_features(df)
        
        # Run analysis
        df = self.analyzer.calculate_price_movement_significance(df)
        correlation_results = self.analyzer.analyze_spread_volume_correlations(df)
        model_results = self.analyzer.build_predictive_model(df)
        insights = self.analyzer.generate_insights(correlation_results, model_results)
        
        # Pattern analysis
        pattern_results = run_pattern_analysis(df)
        
        # Cross-exchange arbitrage
        arbitrage_analysis = self.analyzer.cross_exchange_arbitrage_analysis(df)
        
        # Generate trading signals
        df_signals = self.analyzer.generate_trading_signals(df)
        
        # Compile insights
        live_insights = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data_freshness": {
                "latest_data_time": df['timestamp'].max().isoformat(),
                "data_age_minutes": (datetime.now(timezone.utc) - df['timestamp'].max()).total_seconds() / 60,
                "total_records": len(df)
            },
            "market_overview": self._generate_market_overview(df),
            "top_correlations": insights.get('high_correlation_patterns', [])[:10],
            "predictive_features": insights.get('predictive_features', [])[:5],
            "current_signals": self._extract_current_signals(df_signals),
            "arbitrage_opportunities": arbitrage_analysis.get('arbitrage_opportunities', [])[:5],
            "pattern_summary": pattern_results.get('summary_stats', {}),
            "exchange_health": self._assess_exchange_health(df),
            "asset_insights": self._generate_asset_insights(df, correlation_results),
            "futures_vs_spot": insights.get('futures_vs_spot', {})
        }
        
        return live_insights
    
    def _generate_market_overview(self, df: pd.DataFrame) -> Dict:
        """Generate high-level market overview"""
        
        latest_data = df.groupby(['source_exchange', 'source_asset', 'data_type']).last()
        
        overview = {
            "total_assets_tracked": len(df[['source_exchange', 'source_asset', 'data_type']].drop_duplicates()),
            "average_spreads": {
                "L5": latest_data['spread_L5_pct'].mean(),
                "L50": latest_data['spread_L50_pct'].mean(),
                "L100": latest_data['spread_L100_pct'].mean()
            },
            "total_market_volume": latest_data['total_volume'].sum(),
            "market_volatility": df.groupby(['source_asset'])['price_return'].std().mean(),
            "active_exchanges": {
                "spot": df[df['data_type'] == 'spot']['source_exchange'].unique().tolist(),
                "futures": df[df['data_type'] == 'futures']['source_exchange'].unique().tolist()
            }
        }
        
        return overview
    
    def _extract_current_signals(self, df_signals: pd.DataFrame) -> List[Dict]:
        """Extract current active trading signals"""
        
        # Get signals from last hour
        recent_time = datetime.now(timezone.utc) - timedelta(hours=1)
        recent_signals = df_signals[
            (df_signals['timestamp'] >= recent_time) & 
            (np.abs(df_signals['signal_strength']) > 0.3)
        ]
        
        signals = []
        for _, signal in recent_signals.iterrows():
            signals.append({
                "timestamp": signal['timestamp'].isoformat(),
                "exchange": signal['source_exchange'],
                "asset": signal['source_asset'],
                "data_type": signal['data_type'],
                "direction": signal['signal_direction'],
                "strength": signal['signal_strength'],
                "confidence": signal['signal_confidence'],
                "current_price": signal['mid']
            })
        
        return sorted(signals, key=lambda x: abs(x['strength']), reverse=True)[:10]
    
    def _assess_exchange_health(self, df: pd.DataFrame) -> Dict:
        """Assess health and data quality of each exchange"""
        
        health_assessment = {}
        
        for exchange in df['source_exchange'].unique():
            exchange_data = df[df['source_exchange'] == exchange]
            
            # Calculate health metrics
            data_freshness = (datetime.now(timezone.utc) - exchange_data['timestamp'].max()).total_seconds() / 60
            data_completeness = len(exchange_data) / (6 * 4)  # Expected records (6 hours * 4 assets)
            
            # Check for gaps
            time_diffs = exchange_data.groupby('source_asset')['timestamp'].diff().dt.total_seconds() / 60
            avg_gap = time_diffs.mean()
            
            health_assessment[exchange] = {
                "data_freshness_minutes": data_freshness,
                "data_completeness_pct": min(data_completeness * 100, 100),
                "average_gap_minutes": avg_gap,
                "total_records": len(exchange_data),
                "assets_covered": exchange_data['source_asset'].nunique(),
                "health_score": self._calculate_health_score(data_freshness, data_completeness, avg_gap)
            }
        
        return health_assessment
    
    def _calculate_health_score(self, freshness: float, completeness: float, avg_gap: float) -> float:
        """Calculate overall health score (0-100)"""
        
        freshness_score = max(0, 100 - freshness * 2)  # Penalty for stale data
        completeness_score = completeness * 100
        gap_score = max(0, 100 - avg_gap * 2)  # Penalty for large gaps
        
        return (freshness_score + completeness_score + gap_score) / 3
    
    def _generate_asset_insights(self, df: pd.DataFrame, correlation_results: Dict) -> Dict:
        """Generate insights specific to each asset"""
        
        asset_insights = {}
        
        for asset in df['source_asset'].unique():
            asset_data = df[df['source_asset'] == asset]
            
            # Price volatility across exchanges
            price_volatility = asset_data.groupby(['source_exchange', 'data_type'])['price_return'].std()
            
            # Spread analysis
            avg_spreads = asset_data.groupby(['source_exchange', 'data_type'])[
                ['spread_L5_pct', 'spread_L50_pct', 'spread_L100_pct']
            ].mean()
            
            # Volume analysis
            avg_volumes = asset_data.groupby(['source_exchange', 'data_type'])['total_volume'].mean()
            
            # Find best exchange for this asset (lowest spreads + high volume)
            exchange_scores = {}
            for (exchange, data_type), group in asset_data.groupby(['source_exchange', 'data_type']):
                spread_score = 100 / (group['spread_L5_pct'].mean() + 1)  # Lower spreads = higher score
                volume_score = np.log(group['total_volume'].mean() + 1)  # Higher volume = higher score
                
                exchange_scores[f"{exchange}_{data_type}"] = {
                    "spread_score": spread_score,
                    "volume_score": volume_score,
                    "combined_score": spread_score + volume_score
                }
            
            best_exchange = max(exchange_scores.items(), key=lambda x: x[1]['combined_score'])
            
            asset_insights[asset] = {
                "price_volatility": price_volatility.to_dict(),
                "average_spreads": avg_spreads.to_dict(),
                "average_volumes": avg_volumes.to_dict(),
                "best_exchange": {
                    "name": best_exchange[0],
                    "scores": best_exchange[1]
                },
                "correlation_strength": self._get_asset_correlation_strength(asset, correlation_results)
            }
        
        return asset_insights
    
    def _get_asset_correlation_strength(self, asset: str, correlation_results: Dict) -> Dict:
        """Get correlation strength summary for an asset"""
        
        asset_correlations = []
        
        for key, results in correlation_results.items():
            if results['asset'] == asset:
                for price_metric, corrs in results['correlations'].items():
                    for feature, corr_val in corrs.items():
                        asset_correlations.append(abs(corr_val))
        
        if asset_correlations:
            return {
                "avg_correlation_strength": np.mean(asset_correlations),
                "max_correlation_strength": max(asset_correlations),
                "correlation_count": len(asset_correlations)
            }
        else:
            return {"avg_correlation_strength": 0, "max_correlation_strength": 0, "correlation_count": 0}
    
    def save_insights_to_gcs(self, insights: Dict):
        """Save insights as JSON files in GCS for API access"""
        
        try:
            # Save timestamped insights
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
            timestamped_path = f"ml_insights/historical/{timestamp}.json"
            
            # Save latest insights (for API access)
            latest_path = "ml_insights/latest.json"
            
            # Save asset-specific insights
            for asset in insights.get('asset_insights', {}).keys():
                asset_path = f"ml_insights/assets/{asset.lower()}.json"
                asset_data = {
                    "timestamp": insights["timestamp"],
                    "asset": asset,
                    "insights": insights["asset_insights"][asset],
                    "current_signals": [s for s in insights.get('current_signals', []) if s['asset'] == asset]
                }
                upload_text(self.bucket_name, asset_path, json.dumps(asset_data, indent=2, default=str))
            
            # Save main insights
            insights_json = json.dumps(insights, indent=2, default=str)
            upload_text(self.bucket_name, timestamped_path, insights_json)
            upload_text(self.bucket_name, latest_path, insights_json)
            
            logger.info(f"💾 Saved insights to GCS: {latest_path}")
            
        except Exception as e:
            logger.error(f"Failed to save insights to GCS: {e}")

def main():
    """Generate and save current insights"""
    
    logging.basicConfig(level=logging.INFO)
    
    api = MLInsightsAPI()
    insights = api.generate_live_insights()
    
    # Save to GCS
    api.save_insights_to_gcs(insights)
    
    # Save locally
    with open('live_insights.json', 'w') as f:
        json.dump(insights, f, indent=2, default=str)
    
    logger.info("✅ ML insights generated and saved")

if __name__ == "__main__":
    main()