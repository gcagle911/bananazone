#!/usr/bin/env python3
"""
One-time startup script to fix existing file headers.
This will run automatically when the app starts on Render.
"""

import os
import time
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_header_fix_once():
    """Run the header fix only once by using a marker file"""
    
    marker_file = Path("headers_fixed.marker")
    
    if marker_file.exists():
        logger.info("Headers already fixed (marker file exists)")
        return
    
    logger.info("Running one-time header fix for existing GCS files...")
    
    try:
        # Import and run the fix
        from fix_headers_on_render import fix_headers_in_gcs
        
        success = fix_headers_in_gcs()
        
        if success:
            # Create marker file so we don't run this again
            marker_file.touch()
            logger.info("Header fix completed successfully - created marker file")
        else:
            logger.error("Header fix failed - will retry on next startup")
            
    except Exception as e:
        logger.error(f"Error during header fix: {e}")

if __name__ == "__main__":
    run_header_fix_once()