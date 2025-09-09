#!/usr/bin/env python3
"""
Script to help set up public access for GCS bucket and test URL accessibility.
"""

import json
import requests
from storage import get_storage_backend

def test_public_url_access(bucket_name: str, sample_files: list):
    """Test if GCS public URLs return JSON instead of prompting download"""
    
    print("🔍 Testing GCS Public URL Access")
    print("=" * 50)
    
    for file_path in sample_files:
        public_url = f"https://storage.googleapis.com/{bucket_name}/{file_path}"
        
        print(f"\n📄 Testing: {file_path}")
        print(f"🔗 URL: {public_url}")
        
        try:
            response = requests.get(public_url, timeout=10)
            
            print(f"📊 Status: {response.status_code}")
            print(f"📋 Content-Type: {response.headers.get('content-type', 'Not set')}")
            print(f"📋 Content-Disposition: {response.headers.get('content-disposition', 'Not set')}")
            print(f"📋 Cache-Control: {response.headers.get('cache-control', 'Not set')}")
            
            if response.status_code == 200:
                try:
                    # Try to parse as JSON
                    data = response.json() if response.headers.get('content-type', '').startswith('application/json') else None
                    if data:
                        print("✅ SUCCESS: Returns JSON data")
                        print(f"   Sample: {str(data)[:100]}...")
                    else:
                        # Try parsing as NDJSON
                        lines = response.text.strip().split('\n')
                        if lines:
                            first_record = json.loads(lines[0])
                            print("✅ SUCCESS: Returns NDJSON data")
                            print(f"   Sample: {str(first_record)[:100]}...")
                except json.JSONDecodeError:
                    print("❌ ERROR: Response is not valid JSON")
                    print(f"   Content: {response.text[:200]}...")
            elif response.status_code == 403:
                print("❌ ERROR: Access denied - bucket not public or file doesn't exist")
            elif response.status_code == 404:
                print("❌ ERROR: File not found")
            else:
                print(f"❌ ERROR: HTTP {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ ERROR: Request failed - {e}")

def generate_public_urls(bucket_name: str):
    """Generate sample public URLs for your data"""
    
    print(f"\n🔗 Sample Public URLs for Vercel Integration")
    print("=" * 60)
    
    base_url = f"https://storage.googleapis.com/{bucket_name}"
    
    # Sample URL patterns
    sample_urls = [
        # Daily aggregated files (best for charts)
        f"{base_url}/coinbase/BTC/1min/{{date}}.jsonl",
        f"{base_url}/coinbase/ETH/1min/{{date}}.jsonl", 
        f"{base_url}/kraken/BTC/1min/{{date}}.jsonl",
        f"{base_url}/kraken/ETH/1min/{{date}}.jsonl",
        
        # 5-second data (for real-time)
        f"{base_url}/coinbase/BTC/5s/{{date}}.jsonl",
        f"{base_url}/coinbase/ETH/5s/{{date}}.jsonl",
    ]
    
    print("📊 For daily 1-minute aggregated data (recommended for charts):")
    for url in sample_urls[:4]:
        print(f"   {url}")
    
    print(f"\n⚡ For 5-second real-time data:")
    for url in sample_urls[4:]:
        print(f"   {url}")
    
    print(f"\n💡 Usage in Vercel:")
    print(f"   Replace {{date}} with actual date like '2025-09-09'")
    print(f"   Each line in the JSONL file is a separate JSON record")
    print(f"   Parse with: response.text.split('\\n').map(line => JSON.parse(line))")

def show_gcs_bucket_setup_instructions():
    """Show instructions for making GCS bucket public"""
    
    print(f"\n🌐 GCS Bucket Public Access Setup")
    print("=" * 40)
    
    print(f"To make your bucket publicly readable:")
    print(f"")
    print(f"1. **Via Google Cloud Console:**")
    print(f"   • Go to Cloud Storage → Buckets")
    print(f"   • Click on your 'bananazone' bucket")
    print(f"   • Go to 'Permissions' tab")
    print(f"   • Click 'Add Principal'")
    print(f"   • Principal: allUsers")
    print(f"   • Role: Storage Object Viewer")
    print(f"   • Save")
    print(f"")
    print(f"2. **Via gcloud CLI:**")
    print(f"   gsutil iam ch allUsers:objectViewer gs://bananazone")
    print(f"")
    print(f"3. **Via gsutil (alternative):**")
    print(f"   gsutil acl ch -u AllUsers:R gs://bananazone/**")
    print(f"")
    print(f"⚠️  **Security Note:**")
    print(f"   This makes ALL objects in the bucket publicly readable.")
    print(f"   Only do this if you want the data to be public.")

def main():
    print("🚀 GCS Public Access Setup Helper")
    print("=" * 50)
    
    bucket_name = "bananazone"
    
    # Show setup instructions
    show_gcs_bucket_setup_instructions()
    
    # Generate sample URLs
    generate_public_urls(bucket_name)
    
    # Test some URLs if files exist
    import os
    from datetime import datetime
    
    today = datetime.now().strftime("%Y-%m-%d")
    sample_files = [
        f"coinbase/BTC/1min/{today}.jsonl",
        f"coinbase/ETH/5s/{today}.jsonl",
    ]
    
    print(f"\n🧪 Want to test current URLs? Run:")
    print(f"   python3 -c \"")
    print(f"from setup_public_access import test_public_url_access")
    print(f"test_public_url_access('{bucket_name}', {sample_files})")
    print(f"   \"")

if __name__ == "__main__":
    main()