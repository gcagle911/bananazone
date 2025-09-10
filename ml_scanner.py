#!/usr/bin/env python3
"""
Continuous ML scanner that constantly analyzes crypto data for correlations and patterns.
Runs alongside data collection to provide real-time insights.
"""

import time
import json
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List
import pandas as pd

from data_loader import CryptoDataLoader
from correlation_analyzer import CorrelationAnalyzer
from storage import upload_text, get_storage_backend

logger = logging.getLogger(__name__)

class ContinuousMLScanner:
    """Continuously scan crypto data for ML insights and correlations"""
    
    def __init__(self, bucket_name: str = "bananazone", scan_interval_minutes: int = 15):
        self.bucket_name = bucket_name
        self.scan_interval = scan_interval_minutes * 60  # Convert to seconds
        self.running = False
        self.thread = None
        
        # Initialize components
        self.data_loader = CryptoDataLoader(bucket_name)
        self.analyzer = CorrelationAnalyzer()
        self.storage_backend = get_storage_backend(bucket_name)
        
        # Track analysis history
        self.analysis_history = []
        self.insights_log = []
        
        logger.info(f"🤖 ML Scanner initialized (scan every {scan_interval_minutes} minutes)")
    
    def start(self):
        """Start the continuous ML scanning"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._scan_loop, daemon=True)
        self.thread.start()
        logger.info("🚀 Started continuous ML scanning")
    
    def stop(self):
        """Stop the ML scanning"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("🛑 Stopped ML scanning")
    
    def _scan_loop(self):
        """Main scanning loop"""
        
        while self.running:
            try:
                start_time = time.time()
                
                logger.info("🔍 Starting ML analysis scan...")
                self._perform_analysis_cycle()
                
                scan_duration = time.time() - start_time
                logger.info(f"✅ Analysis cycle completed in {scan_duration:.1f}s")
                
                # Sleep until next scan
                sleep_time = max(60, self.scan_interval - scan_duration)
                logger.info(f"😴 Sleeping for {sleep_time/60:.1f} minutes until next scan")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"💥 Error in ML scan loop: {e}")
                time.sleep(300)  # Sleep 5 minutes on error
    
    def _perform_analysis_cycle(self):
        """Perform one complete analysis cycle"""
        
        analysis_timestamp = datetime.now(timezone.utc)
        
        try:
            # Load recent data
            logger.info("📊 Loading recent data...")
            df = self.data_loader.get_latest_data(hours_back=6, timeframe="1min")
            
            if df.empty:
                logger.warning("⚠️  No recent data available for analysis")
                return
            
            # Prepare features
            df = self.data_loader.prepare_ml_features(df)
            
            # Identify significant movements
            df = self.analyzer.calculate_price_movement_significance(df, threshold_pct=0.3)
            
            # Analyze correlations
            correlation_results = self.analyzer.analyze_spread_volume_correlations(df)
            
            # Build/update models
            model_results = self.analyzer.build_predictive_model(df)
            
            # Generate insights
            insights = self.analyzer.generate_insights(correlation_results, model_results)
            
            # Detect anomalies
            df_anomalies = self.analyzer.detect_anomalies(df)
            anomaly_count = df_anomalies['is_anomaly'].sum()
            
            # Generate trading signals
            df_signals = self.analyzer.generate_trading_signals(df_anomalies)
            active_signals = len(df_signals[np.abs(df_signals['signal_strength']) > 0.3])
            
            # Cross-exchange analysis
            arbitrage_analysis = self.analyzer.cross_exchange_arbitrage_analysis(df)
            
            # Compile scan results
            scan_results = {
                'scan_timestamp': analysis_timestamp.isoformat(),
                'data_summary': {
                    'total_records': len(df),
                    'significant_movements': df['is_significant'].sum(),
                    'anomalies_detected': anomaly_count,
                    'active_signals': active_signals,
                    'arbitrage_opportunities': len(arbitrage_analysis.get('arbitrage_opportunities', []))
                },
                'top_correlations': self._get_top_correlations(correlation_results),
                'model_performance': self._get_model_summary(model_results),
                'recent_insights': insights,
                'alerts': self._generate_alerts(df_signals, df_anomalies, arbitrage_analysis)
            }
            
            # Save results
            self._save_scan_results(scan_results)
            
            # Log key findings
            self._log_key_findings(scan_results)
            
            # Store in history
            self.analysis_history.append(scan_results)
            
            # Keep only last 100 analyses
            if len(self.analysis_history) > 100:
                self.analysis_history = self.analysis_history[-100:]
                
        except Exception as e:
            logger.error(f"💥 Error in analysis cycle: {e}")
            import traceback
            traceback.print_exc()
    
    def _get_top_correlations(self, correlation_results: Dict) -> List[Dict]:
        """Extract top correlations across all exchanges/assets"""
        
        top_correlations = []
        
        for key, results in correlation_results.items():
            for price_metric, corrs in results['correlations'].items():
                for feature, corr_val in corrs.items():
                    if abs(corr_val) > 0.2:  # Meaningful correlation
                        top_correlations.append({
                            'source': key,
                            'price_metric': price_metric,
                            'feature': feature,
                            'correlation': corr_val,
                            'abs_correlation': abs(corr_val)
                        })
        
        # Sort by absolute correlation strength
        top_correlations.sort(key=lambda x: x['abs_correlation'], reverse=True)
        
        return top_correlations[:20]  # Top 20
    
    def _get_model_summary(self, model_results: Dict) -> Dict:
        """Summarize model performance across all assets"""
        
        if not model_results:
            return {}
        
        r2_scores = [r['model_performance']['r2'] for r in model_results.values()]
        
        return {
            'models_built': len(model_results),
            'avg_r2_score': np.mean(r2_scores),
            'best_r2_score': max(r2_scores),
            'worst_r2_score': min(r2_scores),
            'models_above_threshold': sum(1 for r2 in r2_scores if r2 > 0.1)
        }
    
    def _generate_alerts(self, df_signals: pd.DataFrame, df_anomalies: pd.DataFrame, arbitrage_analysis: Dict) -> List[Dict]:
        """Generate alerts for significant findings"""
        
        alerts = []
        
        # Strong trading signals
        strong_signals = df_signals[np.abs(df_signals['signal_strength']) > 0.5]
        if len(strong_signals) > 0:
            for _, signal in strong_signals.tail(5).iterrows():  # Last 5 strong signals
                alerts.append({
                    'type': 'trading_signal',
                    'severity': 'high',
                    'message': f"Strong {signal['signal_direction']} signal for {signal['source_exchange']} {signal['source_asset']} ({signal['data_type']})",
                    'timestamp': signal['timestamp'].isoformat(),
                    'strength': signal['signal_strength'],
                    'confidence': signal['signal_confidence']
                })
        
        # Recent anomalies
        recent_anomalies = df_anomalies[df_anomalies['is_anomaly'] & (df_anomalies['timestamp'] > datetime.now(timezone.utc) - timedelta(hours=1))]
        if len(recent_anomalies) > 0:
            alerts.append({
                'type': 'anomaly_detection',
                'severity': 'medium',
                'message': f"Detected {len(recent_anomalies)} anomalies in last hour",
                'count': len(recent_anomalies),
                'assets_affected': recent_anomalies['source_asset'].unique().tolist()
            })
        
        # Large arbitrage opportunities
        large_arbitrage = [opp for opp in arbitrage_analysis.get('arbitrage_opportunities', []) 
                          if opp['price_difference_pct'] > 1.0]
        if large_arbitrage:
            alerts.append({
                'type': 'arbitrage_opportunity',
                'severity': 'high',
                'message': f"Large arbitrage opportunity detected",
                'opportunities': large_arbitrage[:3]  # Top 3
            })
        
        return alerts
    
    def _save_scan_results(self, results: Dict):
        """Save scan results to GCS for historical tracking"""
        
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
            results_path = f"ml_analysis/scans/{timestamp}.json"
            
            results_json = json.dumps(results, indent=2, default=str)
            upload_text(self.bucket_name, results_path, results_json)
            
            # Also save latest results
            latest_path = "ml_analysis/latest_scan.json"
            upload_text(self.bucket_name, latest_path, results_json)
            
            logger.debug(f"💾 Saved scan results to {results_path}")
            
        except Exception as e:
            logger.warning(f"Failed to save scan results: {e}")
    
    def _log_key_findings(self, results: Dict):
        """Log key findings from the analysis"""
        
        summary = results['data_summary']
        logger.info(f"📊 Analysis Summary:")
        logger.info(f"   Records analyzed: {summary['total_records']:,}")
        logger.info(f"   Significant movements: {summary['significant_movements']}")
        logger.info(f"   Anomalies detected: {summary['anomalies_detected']}")
        logger.info(f"   Active signals: {summary['active_signals']}")
        logger.info(f"   Arbitrage opportunities: {summary['arbitrage_opportunities']}")
        
        # Log top correlations
        top_corrs = results.get('top_correlations', [])[:3]
        if top_corrs:
            logger.info(f"🔍 Top Correlations:")
            for corr in top_corrs:
                logger.info(f"   {corr['source']}: {corr['feature']} → {corr['price_metric']} ({corr['correlation']:.3f})")
        
        # Log alerts
        alerts = results.get('alerts', [])
        for alert in alerts[:3]:  # Show top 3 alerts
            severity_emoji = "🚨" if alert['severity'] == 'high' else "⚠️"
            logger.info(f"{severity_emoji} {alert['type']}: {alert['message']}")

def main():
    """Main entry point for continuous ML scanning"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_scanner.log'),
            logging.StreamHandler()
        ]
    )
    
    logger.info("🤖 Starting Continuous ML Scanner for Crypto Correlations")
    
    # Create and start scanner
    scanner = ContinuousMLScanner(scan_interval_minutes=15)
    
    try:
        scanner.start()
        
        # Keep main thread alive
        while True:
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("🛑 Received interrupt signal...")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
    finally:
        scanner.stop()
        logger.info("✅ ML Scanner stopped")

if __name__ == "__main__":
    main()