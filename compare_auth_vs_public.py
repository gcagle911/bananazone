#!/usr/bin/env python3
"""
Compare authenticated vs public GCS URLs to identify caching/consistency issues.
"""

import json
import requests
from datetime import datetime, timezone
import hashlib

def compare_auth_vs_public_gcs():
    """Compare the same GCS file via authenticated and public access"""
    
    print("🔍 Comparing Authenticated vs Public GCS Access")
    print("=" * 60)
    
    # Test Coinbase BTC file
    bucket_name = "bananazone"
    file_path = "coinbase/BTC/1min/2025-09-09.jsonl"
    
    # Public URL (what you're using in Vercel)
    public_url = f"https://storage.googleapis.com/{bucket_name}/{file_path}"
    
    # Authenticated URL (what GCS sees internally) 
    auth_url = f"https://storage.cloud.google.com/{bucket_name}/{file_path}"
    
    print(f"📁 File: {file_path}")
    print(f"🌐 Public URL:  {public_url}")
    print(f"🔐 Auth URL:    {auth_url}")
    print()
    
    results = {}
    
    # Test public URL
    print("🌐 Testing Public URL...")
    try:
        headers = {
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache'
        }
        response = requests.get(public_url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            text = response.text.strip()
            lines = [line for line in text.split('\n') if line.strip()]
            
            content_hash = hashlib.md5(text.encode()).hexdigest()[:8]
            
            # Get last record
            last_record = None
            last_timestamp = None
            last_price = None
            
            if lines:
                try:
                    last_record = json.loads(lines[-1])
                    last_timestamp = last_record['t']
                    last_price = last_record['mid']
                except:
                    pass
            
            results['public'] = {
                'status': response.status_code,
                'content_hash': content_hash,
                'total_records': len(lines),
                'last_timestamp': last_timestamp,
                'last_price': last_price,
                'content_length': response.headers.get('content-length'),
                'last_modified': response.headers.get('last-modified'),
                'etag': response.headers.get('etag'),
                'cache_control': response.headers.get('cache-control'),
                'age': response.headers.get('age'),
            }
            
            # Calculate data age
            age_info = ""
            if last_timestamp:
                try:
                    last_dt = datetime.fromisoformat(last_timestamp.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    age_minutes = (now - last_dt).total_seconds() / 60
                    age_info = f"({age_minutes:.1f}min old)"
                except:
                    pass
            
            print(f"  ✅ Status: {response.status_code}")
            print(f"  📊 Records: {len(lines)}")
            print(f"  🔑 Hash: {content_hash}")
            print(f"  🕐 Last: {last_timestamp} {age_info}")
            print(f"  💰 Price: ${last_price:.2f}" if last_price else "")
            print(f"  📏 Size: {response.headers.get('content-length', 'unknown')} bytes")
            print(f"  🏷️  ETag: {response.headers.get('etag', 'none')}")
            print(f"  🕐 Modified: {response.headers.get('last-modified', 'unknown')}")
            print(f"  💾 Cache: {response.headers.get('cache-control', 'none')}")
            print(f"  ⏰ Age: {response.headers.get('age', 'none')} seconds")
            
        else:
            results['public'] = {'status': response.status_code, 'error': 'HTTP error'}
            print(f"  ❌ HTTP {response.status_code}")
            
    except Exception as e:
        results['public'] = {'error': str(e)}
        print(f"  💥 Error: {e}")
    
    print()
    
    # Test with authenticated client (if available)
    print("🔐 Testing with GCS Client (if available)...")
    
    try:
        # Try to use GCS client
        import os
        if os.path.exists("gcs-key.json"):
            from google.cloud import storage
            
            client = storage.Client.from_service_account_json("gcs-key.json")
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            
            if blob.exists():
                # Get blob properties
                blob.reload()  # Refresh metadata
                
                # Download content
                content = blob.download_as_text()
                lines = [line for line in content.strip().split('\n') if line.strip()]
                
                content_hash = hashlib.md5(content.encode()).hexdigest()[:8]
                
                # Get last record
                last_record = None
                last_timestamp = None
                last_price = None
                
                if lines:
                    try:
                        last_record = json.loads(lines[-1])
                        last_timestamp = last_record['t']
                        last_price = last_record['mid']
                    except:
                        pass
                
                results['authenticated'] = {
                    'content_hash': content_hash,
                    'total_records': len(lines),
                    'last_timestamp': last_timestamp,
                    'last_price': last_price,
                    'size': blob.size,
                    'etag': blob.etag,
                    'updated': blob.updated.isoformat() if blob.updated else None,
                    'generation': blob.generation,
                    'metageneration': blob.metageneration,
                    'content_type': blob.content_type,
                    'cache_control': blob.cache_control,
                }
                
                # Calculate data age
                age_info = ""
                if last_timestamp:
                    try:
                        last_dt = datetime.fromisoformat(last_timestamp.replace('Z', '+00:00'))
                        now = datetime.now(timezone.utc)
                        age_minutes = (now - last_dt).total_seconds() / 60
                        age_info = f"({age_minutes:.1f}min old)"
                    except:
                        pass
                
                print(f"  ✅ Blob exists")
                print(f"  📊 Records: {len(lines)}")
                print(f"  🔑 Hash: {content_hash}")
                print(f"  🕐 Last: {last_timestamp} {age_info}")
                print(f"  💰 Price: ${last_price:.2f}" if last_price else "")
                print(f"  📏 Size: {blob.size} bytes")
                print(f"  🏷️  ETag: {blob.etag}")
                print(f"  🕐 Updated: {blob.updated}")
                print(f"  🔢 Generation: {blob.generation}")
                print(f"  📝 Content-Type: {blob.content_type}")
                print(f"  💾 Cache-Control: {blob.cache_control}")
                
            else:
                results['authenticated'] = {'error': 'Blob does not exist'}
                print(f"  ❌ Blob does not exist")
                
        else:
            results['authenticated'] = {'error': 'No GCS credentials'}
            print(f"  ⚠️  No GCS credentials available")
            
    except Exception as e:
        results['authenticated'] = {'error': str(e)}
        print(f"  💥 Error: {e}")
    
    # Compare results
    print(f"\n📊 COMPARISON:")
    print("=" * 30)
    
    if 'public' in results and 'authenticated' in results:
        pub = results['public']
        auth = results['authenticated']
        
        if pub.get('content_hash') == auth.get('content_hash'):
            print("✅ Content hashes MATCH - data is consistent")
        else:
            print("🚨 Content hashes DIFFER - INCONSISTENCY DETECTED!")
            print(f"   Public hash:    {pub.get('content_hash', 'N/A')}")
            print(f"   Auth hash:      {auth.get('content_hash', 'N/A')}")
        
        if pub.get('total_records') == auth.get('total_records'):
            print("✅ Record counts MATCH")
        else:
            print("🚨 Record counts DIFFER!")
            print(f"   Public records: {pub.get('total_records', 'N/A')}")
            print(f"   Auth records:   {auth.get('total_records', 'N/A')}")
        
        if pub.get('last_timestamp') == auth.get('last_timestamp'):
            print("✅ Last timestamps MATCH")
        else:
            print("🚨 Last timestamps DIFFER!")
            print(f"   Public timestamp: {pub.get('last_timestamp', 'N/A')}")
            print(f"   Auth timestamp:   {auth.get('last_timestamp', 'N/A')}")
        
        # Check ETags
        pub_etag = pub.get('etag', '').strip('"')
        auth_etag = auth.get('etag', '').strip('"')
        
        if pub_etag == auth_etag:
            print("✅ ETags MATCH")
        else:
            print("🚨 ETags DIFFER!")
            print(f"   Public ETag:  {pub.get('etag', 'N/A')}")
            print(f"   Auth ETag:    {auth.get('etag', 'N/A')}")
    
    return results

def test_cache_busting_methods():
    """Test different cache-busting methods"""
    
    print(f"\n🧪 Testing Cache-Busting Methods")
    print("=" * 40)
    
    public_url = "https://storage.googleapis.com/bananazone/coinbase/BTC/1min/2025-09-09.jsonl"
    
    methods = [
        ("No headers", {}),
        ("Cache-Control no-cache", {"Cache-Control": "no-cache"}),
        ("Cache-Control no-store", {"Cache-Control": "no-cache, no-store, must-revalidate"}),
        ("Pragma no-cache", {"Pragma": "no-cache"}),
        ("All cache headers", {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }),
    ]
    
    for method_name, headers in methods:
        print(f"\n{method_name}:")
        try:
            response = requests.get(public_url, headers=headers, timeout=10)
            if response.status_code == 200:
                text = response.text.strip()
                lines = [line for line in text.split('\n') if line.strip()]
                content_hash = hashlib.md5(text.encode()).hexdigest()[:8]
                
                if lines:
                    last_record = json.loads(lines[-1])
                    last_timestamp = last_record['t']
                    
                print(f"  Hash: {content_hash} | Records: {len(lines)} | Last: {last_timestamp}")
            else:
                print(f"  HTTP {response.status_code}")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    compare_auth_vs_public_gcs()
    test_cache_busting_methods()