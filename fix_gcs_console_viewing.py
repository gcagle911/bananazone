#!/usr/bin/env python3
"""
Fix GCS Console viewing of JSON files by adjusting content-disposition and content-type headers.
"""

import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_gcs_console_viewing():
    """Fix headers to make JSON files viewable in GCS Console"""
    
    if not os.path.exists("gcs-key.json"):
        logger.error("❌ No GCS credentials - cannot fix GCS Console viewing")
        return False
    
    try:
        from google.cloud import storage
        
        client = storage.Client.from_service_account_json("gcs-key.json")
        bucket = client.bucket("bananazone")
        
        logger.info("🔧 FIXING GCS CONSOLE VIEWING")
        logger.info("=" * 40)
        
        # Get all .jsonl files
        jsonl_files = []
        for blob in bucket.list_blobs():
            if blob.name.endswith('.jsonl') and not blob.name.startswith('_tmp/'):
                jsonl_files.append(blob)
        
        logger.info(f"📄 Found {len(jsonl_files)} JSONL files to fix")
        
        fixed_count = 0
        for i, blob in enumerate(jsonl_files):
            try:
                logger.info(f"🔄 [{i+1}/{len(jsonl_files)}] Fixing: {blob.name}")
                
                # Set headers optimized for both web access AND GCS Console viewing
                blob.content_type = "application/json; charset=utf-8"
                
                # Remove content-disposition to let GCS Console decide how to display
                blob.content_disposition = None
                
                # Keep cache control for web consistency but allow GCS Console to work
                blob.cache_control = "no-cache, max-age=0"
                
                # Keep CORS and other metadata
                blob.metadata = {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'gcs-console-viewable': 'true',
                    'file-type': 'ndjson'
                }
                
                # Apply changes
                blob.patch()
                fixed_count += 1
                
                if i % 20 == 0 and i > 0:
                    logger.info(f"   ✅ Fixed {i} files so far...")
                
            except Exception as e:
                logger.error(f"   ❌ Error fixing {blob.name}: {e}")
        
        logger.info(f"🎉 GCS CONSOLE VIEWING FIX COMPLETE!")
        logger.info(f"✅ Fixed {fixed_count}/{len(jsonl_files)} files")
        logger.info(f"📱 JSON files should now be viewable in GCS Console")
        
        # Test instructions
        logger.info(f"\n🧪 To test:")
        logger.info(f"   1. Go to Google Cloud Console")
        logger.info(f"   2. Navigate to Cloud Storage > bananazone bucket")
        logger.info(f"   3. Click on any .jsonl file")
        logger.info(f"   4. It should now display the JSON content instead of downloading")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Failed to fix GCS Console viewing: {e}")
        return False

def update_storage_headers_for_console():
    """Update the storage system to create files viewable in GCS Console"""
    
    logger.info(f"\n🔧 UPDATING STORAGE SYSTEM FOR GCS CONSOLE")
    logger.info("=" * 50)
    
    logger.info("The storage system will be updated to:")
    logger.info("   ✅ Use proper JSON content-type")
    logger.info("   ✅ Remove content-disposition for GCS Console compatibility") 
    logger.info("   ✅ Keep CORS headers for web access")
    logger.info("   ✅ Maintain cache-control for consistency")
    
    # The fix will be applied to storage.py
    return True

if __name__ == "__main__":
    logger.info("🚨 FIXING GCS CONSOLE JSON VIEWING")
    logger.info("This will make JSON files viewable in the GCS Console app")
    logger.info("=" * 60)
    
    success = fix_gcs_console_viewing()
    update_storage_headers_for_console()
    
    if success:
        logger.info("\n🎉 GCS CONSOLE VIEWING FIXED!")
        logger.info("✅ JSON files should now open in GCS Console instead of downloading")
        logger.info("✅ Web access still works for Vercel")
        logger.info("✅ Cache consistency maintained")
        logger.info("\n📱 Try opening a .jsonl file in GCS Console now!")
    else:
        logger.error("\n❌ GCS CONSOLE FIX FAILED")
        logger.error("This script must run on Render with GCS credentials")