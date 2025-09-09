# storage.py - Unified storage interface with local fallback
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging

# Try to import GCS, but fall back to local storage if not available
try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    storage = None

logger = logging.getLogger(__name__)

class StorageBackend:
    """Abstract storage backend interface"""
    
    def download_text(self, key: str) -> str:
        raise NotImplementedError
    
    def object_exists(self, key: str) -> bool:
        raise NotImplementedError
    
    def upload_text(self, key: str, text: str) -> None:
        raise NotImplementedError
    
    def append_jsonl_line(self, key: str, line: str) -> None:
        raise NotImplementedError
    
    def list_prefix(self, prefix: str) -> List[str]:
        raise NotImplementedError
    
    def compose_many(self, sources: List[str], destination: str) -> None:
        raise NotImplementedError


class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend"""
    
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        logger.info(f"Using local storage at: {self.base_path.absolute()}")
    
    def _get_path(self, key: str) -> Path:
        path = self.base_path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    
    def download_text(self, key: str) -> str:
        path = self._get_path(key)
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding='utf-8')
        except Exception as e:
            logger.error(f"Error reading {path}: {e}")
            return ""
    
    def object_exists(self, key: str) -> bool:
        return self._get_path(key).exists()
    
    def upload_text(self, key: str, text: str) -> None:
        path = self._get_path(key)
        try:
            path.write_text(text, encoding='utf-8')
            logger.debug(f"Wrote {len(text)} chars to {path}")
        except Exception as e:
            logger.error(f"Error writing to {path}: {e}")
    
    def append_jsonl_line(self, key: str, line: str) -> None:
        path = self._get_path(key)
        try:
            with open(path, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
            logger.debug(f"Appended line to {path}")
        except Exception as e:
            logger.error(f"Error appending to {path}: {e}")
    
    def list_prefix(self, prefix: str) -> List[str]:
        prefix_path = self.base_path / prefix
        if not prefix_path.exists():
            return []
        
        files = []
        try:
            for path in prefix_path.rglob('*'):
                if path.is_file():
                    rel_path = path.relative_to(self.base_path)
                    files.append(str(rel_path).replace('\\', '/'))  # Normalize path separators
        except Exception as e:
            logger.error(f"Error listing prefix {prefix}: {e}")
        
        return sorted(files)
    
    def compose_many(self, sources: List[str], destination: str) -> None:
        if not sources:
            return
        
        dest_path = self._get_path(destination)
        
        try:
            with open(dest_path, 'w', encoding='utf-8') as dest_file:
                for source in sorted(sources):
                    source_path = self._get_path(source)
                    if source_path.exists():
                        content = source_path.read_text(encoding='utf-8')
                        dest_file.write(content)
            
            logger.info(f"Composed {len(sources)} files into {dest_path}")
        except Exception as e:
            logger.error(f"Error composing files to {dest_path}: {e}")


class GCSStorageBackend(StorageBackend):
    """Google Cloud Storage backend"""
    
    def __init__(self, bucket_name: str, key_path: str = "gcs-key.json"):
        if not GCS_AVAILABLE:
            raise ImportError("Google Cloud Storage libraries not available")
        
        self.bucket_name = bucket_name
        self._client = storage.Client.from_service_account_json(key_path)
        self._bucket = self._client.bucket(bucket_name)
        logger.info(f"Using GCS bucket: {bucket_name}")
    
    def download_text(self, key: str) -> str:
        blob = self._bucket.blob(key)
        if not blob.exists():
            return ""
        try:
            return blob.download_as_text()
        except Exception as e:
            logger.error(f"Error downloading {key}: {e}")
            return ""
    
    def object_exists(self, key: str) -> bool:
        return self._bucket.blob(key).exists()
    
    def upload_text(self, key: str, text: str) -> None:
        blob = self._bucket.blob(key)
        try:
            blob.upload_from_string(text, content_type="text/plain; charset=utf-8")
            blob.cache_control = "no-cache, max-age=0"
            blob.content_disposition = "inline"
            blob.patch()
            logger.debug(f"Uploaded {len(text)} chars to gs://{self.bucket_name}/{key}")
        except Exception as e:
            logger.error(f"Error uploading {key}: {e}")
    
    def append_jsonl_line(self, key: str, line: str) -> None:
        # Atomic append using server-side compose
        temp_key = f"_tmp/append/{key}.{uuid.uuid4().hex}.jsonl"
        
        # Create temp object
        temp_blob = self._bucket.blob(temp_key)
        try:
            temp_blob.upload_from_string((line + "\n").encode("utf-8"), 
                                       content_type="text/plain; charset=utf-8")
            
            dest_blob = self._bucket.blob(key)
            
            if dest_blob.exists():
                # Compose to append
                dest_blob.compose([dest_blob, temp_blob])
            else:
                # First write: move temp -> dest
                dest_blob.rewrite(temp_blob)
            
            # Set headers
            dest_blob.cache_control = "no-cache, max-age=0"
            dest_blob.content_disposition = "inline"
            dest_blob.patch()
            
            logger.debug(f"Appended line to gs://{self.bucket_name}/{key}")
        except Exception as e:
            logger.error(f"Error appending to {key}: {e}")
        finally:
            # Cleanup temp file
            try:
                temp_blob.delete()
            except Exception:
                pass
    
    def list_prefix(self, prefix: str) -> List[str]:
        try:
            return [blob.name for blob in self._bucket.list_blobs(prefix=prefix)]
        except Exception as e:
            logger.error(f"Error listing prefix {prefix}: {e}")
            return []
    
    def compose_many(self, sources: List[str], destination: str) -> None:
        if not sources:
            return
        
        sources = sorted(sources)
        
        try:
            if len(sources) == 1:
                dest = self._bucket.blob(destination)
                dest.rewrite(self._bucket.blob(sources[0]))
            else:
                # Handle large compose operations (GCS limit is 32 objects per compose)
                dest = self._bucket.blob(destination)
                source_blobs = [self._bucket.blob(name) for name in sources]
                
                # For simplicity, we'll do it in chunks of 30
                chunk_size = 30
                current = None
                temp_prefix = f"_tmp/compose/{destination}"
                
                for i in range(0, len(source_blobs), chunk_size):
                    chunk = source_blobs[i:i + chunk_size]
                    if current is None:
                        dest.compose(chunk)
                        current = dest
                    else:
                        temp_name = f"{temp_prefix}.part{i}"
                        temp_blob = self._bucket.blob(temp_name)
                        temp_blob.compose([current] + chunk)
                        current = temp_blob
                
                if current != dest:
                    dest.rewrite(current)
            
            # Set headers
            dest_blob = self._bucket.blob(destination)
            dest_blob.cache_control = "no-cache, max-age=0"
            dest_blob.content_disposition = "inline"
            dest_blob.patch()
            
            logger.info(f"Composed {len(sources)} files to gs://{self.bucket_name}/{destination}")
        except Exception as e:
            logger.error(f"Error composing files to {destination}: {e}")


# Global storage instance
_storage_backend: Optional[StorageBackend] = None

def get_storage_backend(bucket_name: str = None, force_local: bool = False) -> StorageBackend:
    """Get the appropriate storage backend (GCS or local fallback)"""
    global _storage_backend
    
    if _storage_backend is not None:
        return _storage_backend
    
    if force_local:
        _storage_backend = LocalStorageBackend()
        return _storage_backend
    
    # Try GCS first if available and credentials exist
    if GCS_AVAILABLE and bucket_name and os.path.exists("gcs-key.json"):
        try:
            _storage_backend = GCSStorageBackend(bucket_name)
            return _storage_backend
        except Exception as e:
            logger.warning(f"Failed to initialize GCS backend: {e}")
    
    # Fall back to local storage
    logger.info("Using local storage backend (GCS not available or configured)")
    _storage_backend = LocalStorageBackend()
    return _storage_backend


# Convenience functions for backward compatibility
def download_text(bucket_name: str, key: str) -> str:
    return get_storage_backend(bucket_name).download_text(key)

def object_exists(bucket_name: str, key: str) -> bool:
    return get_storage_backend(bucket_name).object_exists(key)

def upload_text(bucket_name: str, key: str, text: str) -> None:
    get_storage_backend(bucket_name).upload_text(key, text)

def append_jsonl_line(bucket_name: str, key: str, line: str) -> None:
    get_storage_backend(bucket_name).append_jsonl_line(key, line)

def list_prefix(bucket_name: str, prefix: str) -> List[str]:
    return get_storage_backend(bucket_name).list_prefix(prefix)

def compose_many(bucket_name: str, sources: List[str], destination: str) -> None:
    get_storage_backend(bucket_name).compose_many(sources, destination)