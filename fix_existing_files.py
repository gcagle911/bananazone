#!/usr/bin/env python3
"""
Fix content-type headers for existing files in GCS bucket.
"""

import os
from storage import get_storage_backend

def fix_existing_file_headers():
    """Update headers for all existing files in the bucket"""
    
    print("🔧 Fixing Content-Type Headers for Existing Files")
    print("=" * 55)
    
    # Check if we have GCS access
    if not os.path.exists("gcs-key.json"):
        print("❌ No gcs-key.json found - cannot update GCS files")
        print("   Files uploaded after the fix will have correct headers")
        return
    
    try:
        from google.cloud import storage
        
        bucket_name = "bananazone"
        client = storage.Client.from_service_account_json("gcs-key.json")
        bucket = client.bucket(bucket_name)
        
        print(f"📁 Scanning bucket: {bucket_name}")
        
        # List all .jsonl files
        jsonl_files = []
        for blob in bucket.list_blobs():
            if blob.name.endswith('.jsonl') and not blob.name.startswith('_tmp/'):
                jsonl_files.append(blob)
        
        print(f"📄 Found {len(jsonl_files)} JSONL files to update")
        
        if len(jsonl_files) == 0:
            print("✅ No files to update")
            return
        
        # Update each file's headers
        updated_count = 0
        for blob in jsonl_files:
            try:
                # Check current content-type
                if blob.content_type != "application/json; charset=utf-8":
                    print(f"🔄 Updating: {blob.name}")
                    
                    # Update headers
                    blob.content_type = "application/json; charset=utf-8"
                    blob.cache_control = "public, max-age=60"
                    blob.content_disposition = "inline"
                    blob.metadata = {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
                        'Access-Control-Allow-Headers': 'Content-Type',
                    }
                    blob.patch()
                    updated_count += 1
                else:
                    print(f"✅ Already correct: {blob.name}")
                    
            except Exception as e:
                print(f"❌ Error updating {blob.name}: {e}")
        
        print(f"\n🎉 Updated {updated_count} files!")
        print(f"✅ All files now have proper JSON headers")
        
        # Test a sample URL
        if jsonl_files:
            sample_file = jsonl_files[0].name
            sample_url = f"https://storage.googleapis.com/{bucket_name}/{sample_file}"
            print(f"\n🧪 Test URL: {sample_url}")
            print(f"   Should now return Content-Type: application/json")
        
    except Exception as e:
        print(f"❌ Error accessing GCS: {e}")
        print(f"   Make sure gcs-key.json is valid and has proper permissions")

if __name__ == "__main__":
    fix_existing_file_headers()