#!/usr/bin/env python3
"""
Emergency restart script - falls back to standard collector if improved version fails.
"""

import os
import sys
import logging
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_improved_collector():
    """Test if the improved collector can start without errors"""
    try:
        logger.info("🧪 Testing improved collector...")
        
        # Try importing all required modules
        from guaranteed_minute_scheduler import ImprovedDataCollectorWithScheduler
        from improved_logger import DataCollector, load_config
        from realtime_health_monitor import RealtimeHealthMonitor
        
        # Try loading config
        cfg = load_config()
        
        # Try creating collector (but don't run it)
        collector = ImprovedDataCollectorWithScheduler(cfg)
        
        logger.info("✅ Improved collector test passed")
        return True, None
        
    except Exception as e:
        error_msg = f"Improved collector test failed: {e}"
        logger.error(f"❌ {error_msg}")
        traceback.print_exc()
        return False, error_msg

def run_standard_collector():
    """Run the standard collector as fallback"""
    try:
        logger.info("🔄 Starting standard collector as fallback...")
        
        # Force use of standard collector
        os.environ["USE_IMPROVED_COLLECTOR"] = "false"
        
        # Import and run standard collector
        from logger import main as standard_main
        standard_main()
        
    except Exception as e:
        logger.error(f"💥 Standard collector also failed: {e}")
        traceback.print_exc()
        sys.exit(1)

def run_improved_collector():
    """Run the improved collector"""
    try:
        logger.info("🚀 Starting improved collector...")
        
        from guaranteed_minute_scheduler import main as improved_main
        improved_main()
        
    except Exception as e:
        logger.error(f"💥 Improved collector failed: {e}")
        traceback.print_exc()
        
        # Fall back to standard collector
        logger.info("🔄 Falling back to standard collector...")
        run_standard_collector()

def main():
    """Main entry point with smart fallback"""
    
    logger.info("🚨 Emergency restart initiated")
    
    # Check if we should force standard collector
    force_standard = os.environ.get("FORCE_STANDARD_COLLECTOR", "false").lower() == "true"
    
    if force_standard:
        logger.info("🔧 FORCE_STANDARD_COLLECTOR=true, using standard collector")
        run_standard_collector()
        return
    
    # Test improved collector first
    can_use_improved, error = test_improved_collector()
    
    if can_use_improved:
        run_improved_collector()
    else:
        logger.warning(f"⚠️  Cannot use improved collector: {error}")
        run_standard_collector()

if __name__ == "__main__":
    main()