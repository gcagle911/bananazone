// vercel_trading_bot_example.js
// Example of how to build a trading bot using the ML insights

class CryptoTradingBot {
  constructor() {
    this.insightsUrl = 'https://storage.googleapis.com/bananazone/ml_insights/latest.json';
    this.isRunning = false;
    this.tradingHistory = [];
  }

  async getMLInsights() {
    try {
      const response = await fetch(`${this.insightsUrl}?t=${Date.now()}`, {
        headers: { 'Cache-Control': 'no-cache' }
      });
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error('Failed to get ML insights:', error);
      return null;
    }
  }

  analyzeSignals(insights) {
    const signals = insights.current_signals || [];
    
    // Filter for high-confidence signals
    const strongSignals = signals.filter(signal => 
      signal.strength > 0.7 && 
      signal.confidence > 0.6
    );

    const tradingDecisions = [];

    strongSignals.forEach(signal => {
      const decision = {
        timestamp: new Date().toISOString(),
        exchange: signal.exchange,
        asset: signal.asset,
        action: signal.direction, // 'bullish' or 'bearish'
        strength: signal.strength,
        confidence: signal.confidence,
        reason: `ML signal: ${signal.strength.toFixed(2)} strength, ${signal.confidence.toFixed(2)} confidence`,
        dataType: signal.data_type
      };

      tradingDecisions.push(decision);
      
      console.log(`🎯 TRADING SIGNAL: ${decision.action.toUpperCase()} ${decision.asset} on ${decision.exchange}`);
      console.log(`   Strength: ${(decision.strength * 100).toFixed(0)}% | Confidence: ${(decision.confidence * 100).toFixed(0)}%`);
    });

    return tradingDecisions;
  }

  analyzeArbitrage(insights) {
    const opportunities = insights.arbitrage_opportunities || [];
    
    // Filter for profitable arbitrage (accounting for typical 0.1% trading fees)
    const profitableArbitrage = opportunities.filter(opp => 
      opp.price_difference_pct > 0.25  // Must be > 0.25% to cover fees + profit
    );

    const arbitrageActions = [];

    profitableArbitrage.forEach(opp => {
      const netProfit = opp.price_difference_pct - 0.2; // Subtract estimated fees
      
      const action = {
        timestamp: new Date().toISOString(),
        type: 'arbitrage',
        asset: opp.asset,
        buyExchange: opp.low_exchange,
        sellExchange: opp.high_exchange,
        buyPrice: opp.low_price,
        sellPrice: opp.high_price,
        grossProfitPct: opp.price_difference_pct,
        netProfitPct: netProfit,
        reason: `Arbitrage opportunity: ${netProfit.toFixed(2)}% net profit`
      };

      arbitrageActions.push(action);
      
      console.log(`💰 ARBITRAGE: ${action.asset}`);
      console.log(`   Buy: ${action.buyExchange} at $${action.buyPrice.toFixed(2)}`);
      console.log(`   Sell: ${action.sellExchange} at $${action.sellPrice.toFixed(2)}`);
      console.log(`   Net Profit: ${action.netProfitPct.toFixed(2)}%`);
    });

    return arbitrageActions;
  }

  analyzeMarketConditions(insights) {
    const health = insights.exchange_health || {};
    const overview = insights.market_overview || {};
    
    // Assess overall market health
    const exchangeHealthScores = Object.values(health).map(h => h.health_score || 0);
    const avgHealth = exchangeHealthScores.length > 0 ? 
      exchangeHealthScores.reduce((a, b) => a + b, 0) / exchangeHealthScores.length : 0;

    const marketCondition = {
      timestamp: new Date().toISOString(),
      overallHealth: avgHealth,
      healthStatus: avgHealth > 80 ? 'healthy' : avgHealth > 60 ? 'degraded' : 'poor',
      totalVolume: overview.total_market_volume || 0,
      avgSpread: overview.average_spreads?.L5 || 0,
      volatility: overview.market_volatility || 0,
      recommendation: this.getMarketRecommendation(avgHealth, overview)
    };

    console.log(`🏥 Market Health: ${marketCondition.healthStatus} (${avgHealth.toFixed(0)}%)`);
    console.log(`📊 Recommendation: ${marketCondition.recommendation}`);

    return marketCondition;
  }

  getMarketRecommendation(health, overview) {
    if (health < 60) {
      return 'CAUTION: Poor data quality, avoid trading';
    }
    
    const volatility = overview.market_volatility || 0;
    const avgSpread = overview.average_spreads?.L5 || 0;
    
    if (volatility > 0.05) {
      return 'HIGH_VOLATILITY: Good for scalping, risky for holds';
    } else if (avgSpread < 0.01) {
      return 'TIGHT_SPREADS: Good liquidity, favorable for trading';
    } else if (avgSpread > 0.05) {
      return 'WIDE_SPREADS: Poor liquidity, trade with caution';
    } else {
      return 'NORMAL_CONDITIONS: Standard trading environment';
    }
  }

  async executeAnalysis() {
    console.log('🤖 Running ML Trading Analysis...');
    
    const insights = await this.getMLInsights();
    if (!insights) {
      console.log('❌ No insights available');
      return;
    }

    // Analyze different aspects
    const signals = this.analyzeSignals(insights);
    const arbitrage = this.analyzeArbitrage(insights);
    const market = this.analyzeMarketConditions(insights);

    // Compile trading recommendations
    const recommendations = {
      timestamp: new Date().toISOString(),
      marketCondition: market,
      tradingSignals: signals,
      arbitrageOpportunities: arbitrage,
      actionItems: this.generateActionItems(signals, arbitrage, market)
    };

    // Log summary
    console.log('\n📋 TRADING SUMMARY:');
    console.log(`   Signals: ${signals.length} strong signals`);
    console.log(`   Arbitrage: ${arbitrage.length} opportunities`);
    console.log(`   Market: ${market.healthStatus}`);
    
    recommendations.actionItems.forEach((action, i) => {
      console.log(`   ${i+1}. ${action}`);
    });

    // Store for tracking
    this.tradingHistory.push(recommendations);
    
    return recommendations;
  }

  generateActionItems(signals, arbitrage, market) {
    const actions = [];

    if (market.healthStatus === 'poor') {
      actions.push('⚠️  WAIT - Poor data quality, avoid trading until health improves');
      return actions;
    }

    // Priority 1: High-profit arbitrage
    const highProfitArbitrage = arbitrage.filter(a => a.netProfitPct > 0.5);
    highProfitArbitrage.forEach(arb => {
      actions.push(`💰 ARBITRAGE: ${arb.asset} - ${arb.netProfitPct.toFixed(2)}% profit available`);
    });

    // Priority 2: Strong trading signals
    const veryStrongSignals = signals.filter(s => s.strength > 0.8);
    veryStrongSignals.forEach(signal => {
      actions.push(`🎯 ${signal.action.toUpperCase()}: ${signal.asset} on ${signal.exchange} (${(signal.strength * 100).toFixed(0)}% strength)`);
    });

    // Priority 3: Market condition advice
    actions.push(`📊 Market: ${market.recommendation}`);

    if (actions.length === 1) { // Only market condition
      actions.push('😴 No strong signals or arbitrage - HOLD current positions');
    }

    return actions;
  }

  // Auto-trading simulation (for testing)
  async startAutoAnalysis(intervalMinutes = 15) {
    console.log(`🤖 Starting auto-analysis (every ${intervalMinutes} minutes)`);
    this.isRunning = true;

    while (this.isRunning) {
      try {
        await this.executeAnalysis();
        console.log(`😴 Sleeping for ${intervalMinutes} minutes...\n`);
        await new Promise(resolve => setTimeout(resolve, intervalMinutes * 60 * 1000));
      } catch (error) {
        console.error('Analysis error:', error);
        await new Promise(resolve => setTimeout(resolve, 60000)); // Wait 1 minute on error
      }
    }
  }

  stop() {
    this.isRunning = false;
    console.log('🛑 Stopping auto-analysis');
  }
}

// Usage Examples:

// 1. One-time analysis
async function runSingleAnalysis() {
  const bot = new CryptoTradingBot();
  const recommendations = await bot.executeAnalysis();
  console.log('Analysis complete:', recommendations);
}

// 2. Continuous monitoring  
async function startContinuousMonitoring() {
  const bot = new CryptoTradingBot();
  await bot.startAutoAnalysis(15); // Check every 15 minutes
}

// 3. Quick signal check
async function quickSignalCheck() {
  const bot = new CryptoTradingBot();
  const insights = await bot.getMLInsights();
  
  if (insights?.current_signals) {
    console.log('🎯 Current Signals:');
    insights.current_signals.forEach(signal => {
      if (signal.strength > 0.6) {
        console.log(`   ${signal.direction.toUpperCase()} ${signal.asset} on ${signal.exchange}`);
      }
    });
  }
}

// Export for Node.js/Vercel
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { CryptoTradingBot };
}

// For browser testing
if (typeof window !== 'undefined') {
  window.CryptoTradingBot = CryptoTradingBot;
  
  // Auto-start for demo
  // quickSignalCheck();
}