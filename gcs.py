# gcs.py — race-safe NDJSON writes with browser-friendly headers
from typing import List, Optional
import uuid
from google.cloud import storage

_KEY_PATH = "gcs-key.json"  # Render Secret File path
_client_singleton: Optional[storage.Client] = None

# Serve inline in browsers (fixes iOS "download?" prompt)
CACHE_CONTROL = "no-cache, max-age=0"
CONTENT_TYPE  = "text/plain; charset=utf-8"   # <- instead of application/json
DISPOSITION   = "inline"


def _client() -> storage.Client:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = storage.Client.from_service_account_json(_KEY_PATH)
    return _client_singleton


def _bucket(name: str) -> storage.Bucket:
    return _client().bucket(name)


def download_text(bucket_name: str, key: str) -> str:
    b = _bucket(bucket_name)
    blob = b.blob(key)
    if not blob.exists():
        return ""
    return blob.download_as_text()


def object_exists(bucket_name: str, key: str) -> bool:
    b = _bucket(bucket_name)
    return b.blob(key).exists()


def get_generation(bucket_name: str, key: str) -> Optional[int]:
    b = _bucket(bucket_name)
    blob = b.get_blob(key)
    return None if blob is None else blob.generation


def _persist_headers(blob: storage.Blob):
    blob.cache_control = CACHE_CONTROL
    blob.content_type = CONTENT_TYPE
    blob.content_disposition = DISPOSITION
    blob.patch()


def upload_text(
    bucket_name: str,
    key: str,
    text: str,
    if_generation_match: Optional[int] = None,
) -> None:
    """
    Upload full text to an object with headers that force inline render in browsers.
    """
    b = _bucket(bucket_name)
    blob = b.blob(key)
    blob.cache_control = CACHE_CONTROL
    blob.content_type = CONTENT_TYPE
    blob.content_disposition = DISPOSITION
    if if_generation_match is None:
        blob.upload_from_string(text, content_type=CONTENT_TYPE)
    else:
        blob.upload_from_string(text, content_type=CONTENT_TYPE, if_generation_match=if_generation_match)
    _persist_headers(blob)


def append_jsonl_line(bucket_name: str, dest_key: str, line: str) -> None:
    """
    Atomic append using server-side compose:
      1) write temp object containing 'line\\n' with correct headers
      2) if dest exists -> compose [dest, temp] -> dest
         else -> rewrite temp -> dest
      3) set headers on dest
    Safe under concurrent writers.
    """
    b = _bucket(bucket_name)
    temp_key = f"_tmp/append/{dest_key}.{uuid.uuid4().hex}.jsonl"

    # 1) temp write
    temp_blob = b.blob(temp_key)
    temp_blob.cache_control = CACHE_CONTROL
    temp_blob.content_type = CONTENT_TYPE
    temp_blob.content_disposition = DISPOSITION
    temp_blob.upload_from_string((line + "\n").encode("utf-8"), content_type=CONTENT_TYPE)
    _persist_headers(temp_blob)

    dest_blob = b.blob(dest_key)

    try:
        if dest_blob.exists():
            # 2a) compose to append
            dest_blob.compose([dest_blob, temp_blob])
            _persist_headers(dest_blob)
        else:
            # 2b) first write: move temp -> dest
            dest_blob.rewrite(temp_blob)
            _persist_headers(dest_blob)
    finally:
        # best-effort cleanup
        try:
            temp_blob.delete()
        except Exception:
            pass


def list_prefix(bucket_name: str, prefix: str) -> List[str]:
    b = _bucket(bucket_name)
    return [blob.name for blob in b.list_blobs(prefix=prefix)]


def compose_many(
    bucket_name: str,
    sources: List[str],
    destination: str,
    temp_prefix: str = "_tmp/compose/",
) -> None:
    """
    Compose many parts into a single destination. After each compose/rename, ensure
    headers are set so browsers render inline.
    """
    if not sources:
        return

    b = _bucket(bucket_name)
    sources = list(sorted(sources))

    if len(sources) == 1:
        dest = b.blob(destination)
        dest.rewrite(b.blob(sources[0]))
        _persist_headers(dest)
        return

    def _compose(to_names: List[str], out_name: str):
        dest = b.blob(out_name)
        dest.compose([b.blob(n) for n in to_names])
        _persist_headers(dest)

    temp_name = f"{temp_prefix}{destination}"
    i = 0
    chunk = 32
    current = None
    while i < len(sources):
        batch = sources[i:i + chunk]
        if current is None:
            _compose(batch, temp_name)
            current = temp_name
        else:
            temp2 = f"{temp_name}.part{i}"
            _compose([current] + batch, temp2)
            current = temp2
        i += chunk

    dest = b.blob(destination)
    dest.rewrite(b.blob(current))
    _persist_headers(dest)
