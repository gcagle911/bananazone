#!/usr/bin/env python3
"""
Script to run on Render to fix content-type headers for existing GCS files.
This should be run once after deploying the updated storage code.
"""

import os
import sys
from storage import get_storage_backend

def fix_headers_in_gcs():
    """Fix headers for existing files in GCS - runs on Render with proper credentials"""
    
    print("🔧 Fixing GCS File Headers on Render")
    print("=" * 40)
    
    # Check if we're in the right environment
    if not os.path.exists("gcs-key.json"):
        print("❌ No GCS credentials found")
        print("   This script should run on Render where gcs-key.json exists")
        return False
    
    try:
        from google.cloud import storage
        
        bucket_name = "bananazone"
        client = storage.Client.from_service_account_json("gcs-key.json")
        bucket = client.bucket(bucket_name)
        
        print(f"📁 Connected to bucket: {bucket_name}")
        
        # Find all .jsonl files
        files_to_fix = []
        for blob in bucket.list_blobs():
            if blob.name.endswith('.jsonl') and not blob.name.startswith('_tmp/'):
                # Check if it needs fixing
                if blob.content_type != "application/json; charset=utf-8":
                    files_to_fix.append(blob)
        
        print(f"📄 Found {len(files_to_fix)} files needing header updates")
        
        if len(files_to_fix) == 0:
            print("✅ All files already have correct headers!")
            return True
        
        # Fix headers in batches
        fixed_count = 0
        for i, blob in enumerate(files_to_fix):
            try:
                print(f"🔄 [{i+1}/{len(files_to_fix)}] Fixing: {blob.name}")
                
                # Update to proper JSON headers
                blob.content_type = "application/json; charset=utf-8"
                blob.cache_control = "public, max-age=60"
                blob.content_disposition = "inline"
                
                # Add CORS metadata
                blob.metadata = {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type',
                }
                
                blob.patch()
                fixed_count += 1
                
                # Progress indicator
                if (i + 1) % 10 == 0:
                    print(f"   ✅ Fixed {i + 1} files so far...")
                    
            except Exception as e:
                print(f"   ❌ Error fixing {blob.name}: {e}")
        
        print(f"\n🎉 Successfully fixed {fixed_count} files!")
        
        # Test a sample URL
        if files_to_fix:
            sample_file = files_to_fix[0].name
            sample_url = f"https://storage.googleapis.com/{bucket_name}/{sample_file}"
            print(f"\n🧪 Test this URL now (should show JSON in browser):")
            print(f"   {sample_url}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = fix_headers_in_gcs()
    sys.exit(0 if success else 1)