#!/usr/bin/env python3
"""
Fix GCS caching inconsistency between authenticated and public URLs.
The issue is that GCS public URLs can serve stale cached versions while
authenticated access shows the latest data.
"""

import os
from storage import get_storage_backend
import logging

logger = logging.getLogger(__name__)

def fix_gcs_object_caching(bucket_name="bananazone"):
    """
    Fix GCS object caching issues by updating object metadata to force cache invalidation.
    This ensures public URLs serve the same data as authenticated access.
    """
    
    print("🔧 Fixing GCS Object Caching Issues")
    print("=" * 50)
    
    if not os.path.exists("gcs-key.json"):
        print("❌ No GCS credentials found - cannot fix caching issues")
        print("   This fix requires authenticated GCS access")
        return False
    
    try:
        from google.cloud import storage
        
        client = storage.Client.from_service_account_json("gcs-key.json")
        bucket = client.bucket(bucket_name)
        
        print(f"📁 Connected to bucket: {bucket_name}")
        
        # Find all 1min daily files (most important for Vercel)
        target_pattern = "*/*/1min/*.jsonl"
        
        # List files to fix
        files_to_fix = []
        for blob in bucket.list_blobs():
            if "/1min/" in blob.name and blob.name.endswith('.jsonl') and not blob.name.startswith('_tmp/'):
                files_to_fix.append(blob)
        
        print(f"📄 Found {len(files_to_fix)} files to fix")
        
        if len(files_to_fix) == 0:
            print("✅ No files need fixing")
            return True
        
        # Fix each file
        fixed_count = 0
        for i, blob in enumerate(files_to_fix):
            try:
                print(f"🔄 [{i+1}/{len(files_to_fix)}] Fixing: {blob.name}")
                
                # Method 1: Update cache-control to force immediate expiration
                blob.cache_control = "no-cache, max-age=0"
                blob.patch()
                
                # Method 2: Add a custom metadata to force version change
                import time
                current_time = str(int(time.time()))
                
                if not blob.metadata:
                    blob.metadata = {}
                
                blob.metadata['cache-bust'] = current_time
                blob.metadata['fixed-timestamp'] = current_time
                blob.patch()
                
                print(f"   ✅ Updated cache-control and metadata")
                fixed_count += 1
                
                # Brief pause to avoid rate limits
                if i % 10 == 0 and i > 0:
                    print(f"   💤 Brief pause after {i} files...")
                    time.sleep(1)
                
            except Exception as e:
                print(f"   ❌ Error fixing {blob.name}: {e}")
        
        print(f"\n🎉 Fixed {fixed_count} files!")
        print(f"✅ Public URLs should now serve consistent data")
        
        # Test a sample file
        if files_to_fix:
            sample_file = files_to_fix[0].name
            public_url = f"https://storage.googleapis.com/{bucket_name}/{sample_file}"
            print(f"\n🧪 Test this URL (should show fresh data):")
            print(f"   {public_url}")
        
        return True
        
    except Exception as e:
        print(f"💥 Error fixing GCS caching: {e}")
        return False

def update_storage_headers_for_consistency():
    """
    Update the storage.py to use headers that minimize caching inconsistencies.
    """
    
    print(f"\n🔧 Updating Storage Headers for Better Consistency")
    print("-" * 55)
    
    # The fix is to modify how we set headers when uploading/composing files
    recommendations = [
        "1. Use 'no-cache, max-age=0' instead of 'public, max-age=60'",
        "2. Add 'must-revalidate' directive", 
        "3. Set custom metadata to force cache busting",
        "4. Use generation-based ETags",
        "5. Consider using Cloud CDN with proper cache invalidation"
    ]
    
    for rec in recommendations:
        print(f"   • {rec}")
    
    print(f"\n💡 For immediate fix: Run this script on Render to update existing files")
    print(f"💡 For long-term fix: Update storage.py headers (already improved in latest version)")

def create_cache_busting_urls():
    """
    Show how to create cache-busting URLs for Vercel integration.
    """
    
    print(f"\n🔗 Cache-Busting URL Strategies for Vercel")
    print("-" * 50)
    
    base_url = "https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl"
    
    strategies = [
        ("Timestamp parameter", f"{base_url}?t={{timestamp}}"),
        ("Random parameter", f"{base_url}?v={{random}}"),
        ("Generation parameter", f"{base_url}?generation={{generation}}"),
        ("Cache-Control headers", "Add 'Cache-Control: no-cache' to fetch request"),
    ]
    
    print("Strategies to use in Vercel:")
    for name, example in strategies:
        print(f"   • {name}: {example}")
    
    print(f"\nExample Vercel code:")
    print("""
// In your Vercel function
const timestamp = Date.now();
const url = `https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl?t=${timestamp}`;

const response = await fetch(url, {
  headers: {
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache'
  }
});
""")

if __name__ == "__main__":
    success = fix_gcs_object_caching()
    update_storage_headers_for_consistency()
    create_cache_busting_urls()
    
    if success:
        print(f"\n✅ GCS caching fix completed!")
        print(f"🔄 Public URLs should now be consistent with authenticated data")
    else:
        print(f"\n⚠️  Could not apply GCS fix (requires running on Render with credentials)")
        print(f"💡 Use the Vercel cache-busting strategies instead")