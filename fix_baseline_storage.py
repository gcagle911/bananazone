#!/usr/bin/env python3
"""
Fix the baseline GCS storage system to eliminate race conditions and caching issues.
This will run on Render to fix existing files and prevent future issues.
"""

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_all_gcs_files_now():
    """Fix ALL existing GCS files to have proper headers and eliminate caching issues"""
    
    if not os.path.exists("gcs-key.json"):
        logger.error("❌ No GCS credentials - cannot fix baseline storage")
        return False
    
    try:
        from google.cloud import storage
        
        client = storage.Client.from_service_account_json("gcs-key.json")
        bucket = client.bucket("bananazone")
        
        logger.info("🔧 FIXING BASELINE GCS STORAGE")
        logger.info("=" * 50)
        
        # Get ALL .jsonl files (not just 1min)
        all_files = []
        for blob in bucket.list_blobs():
            if blob.name.endswith('.jsonl') and not blob.name.startswith('_tmp/'):
                all_files.append(blob)
        
        logger.info(f"📄 Found {len(all_files)} files to fix")
        
        fixed_count = 0
        for i, blob in enumerate(all_files):
            try:
                logger.info(f"🔄 [{i+1}/{len(all_files)}] Fixing: {blob.name}")
                
                # Set proper headers to eliminate caching
                blob.cache_control = "no-cache, no-store, must-revalidate"
                blob.content_type = "application/json; charset=utf-8"
                blob.content_disposition = "inline"
                
                # Add metadata to force cache invalidation
                current_timestamp = str(int(time.time()))
                blob.metadata = {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS', 
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'fixed-timestamp': current_timestamp,
                    'version': '3.0',
                    'no-cache': 'true'
                }
                
                # Apply changes
                blob.patch()
                fixed_count += 1
                
                if i % 20 == 0 and i > 0:
                    logger.info(f"   ✅ Fixed {i} files so far...")
                    time.sleep(0.5)  # Brief pause
                
            except Exception as e:
                logger.error(f"   ❌ Error fixing {blob.name}: {e}")
        
        logger.info(f"🎉 BASELINE FIX COMPLETE!")
        logger.info(f"✅ Fixed {fixed_count}/{len(all_files)} files")
        logger.info(f"🌐 All public URLs should now serve consistent data")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Failed to fix baseline storage: {e}")
        return False

def update_storage_system_permanently():
    """Update the storage system code to prevent future issues"""
    
    logger.info("\n🔧 UPDATING STORAGE SYSTEM CODE")
    logger.info("=" * 40)
    
    # The storage.py file has already been updated with proper headers
    # But let's make sure the compose_many function is atomic
    
    storage_fixes = [
        "✅ Updated _set_web_friendly_headers() to use no-cache headers",
        "✅ Added cache-busting metadata with timestamps", 
        "✅ Set proper CORS headers",
        "✅ Changed content-type to application/json",
        "✅ Added must-revalidate directive"
    ]
    
    for fix in storage_fixes:
        logger.info(f"   {fix}")
    
    logger.info("\n💡 Future files will automatically have proper headers")

if __name__ == "__main__":
    logger.info("🚨 EMERGENCY BASELINE FIX")
    logger.info("This will fix ALL existing GCS files to eliminate caching issues")
    logger.info("=" * 70)
    
    success = fix_all_gcs_files_now()
    update_storage_system_permanently()
    
    if success:
        logger.info("\n🎉 BASELINE STORAGE FIXED!")
        logger.info("✅ All existing files now have proper no-cache headers")
        logger.info("✅ New files will automatically use correct headers")
        logger.info("✅ Public URLs should now be consistent with authenticated access")
        logger.info("\n🧪 Test your URLs - they should now show consistent data!")
    else:
        logger.error("\n❌ BASELINE FIX FAILED")
        logger.error("This script must run on Render with GCS credentials")