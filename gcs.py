# gcs.py
from typing import List, Optional
from google.cloud import storage

# === Auth: load key directly from a Render Secret File in the app root ===
# In Render, add a Secret File with path exactly:  gcs-key.json
# Paste your full service account JSON as the contents.
_KEY_PATH = "gcs-key.json"

# Keep a single client for the process
_client_singleton: Optional[storage.Client] = None


def _client() -> storage.Client:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = storage.Client.from_service_account_json(_KEY_PATH)
    return _client_singleton


def _bucket(name: str) -> storage.Bucket:
    return _client().bucket(name)


def download_text(bucket_name: str, key: str) -> str:
    """Return object contents as text, or '' if missing."""
    b = _bucket(bucket_name)
    blob = b.blob(key)
    if not blob.exists():
        return ""
    return blob.download_as_text()


def object_exists(bucket_name: str, key: str) -> bool:
    b = _bucket(bucket_name)
    return b.blob(key).exists()


def get_generation(bucket_name: str, key: str) -> Optional[int]:
    """Return GCS generation number for optimistic concurrency, or None if missing."""
    b = _bucket(bucket_name)
    blob = b.get_blob(key)
    return None if blob is None else blob.generation


def upload_text(
    bucket_name: str,
    key: str,
    text: str,
    if_generation_match: Optional[int] = None,
    content_type: str = "application/json",
) -> None:
    """
    Upload full text to an object. If if_generation_match is provided,
    GCS will only write if the object's generation matches (race-safe).
    Use 0 to require that the object does not yet exist.
    """
    b = _bucket(bucket_name)
    blob = b.blob(key)
    if if_generation_match is None:
        blob.upload_from_string(text, content_type=content_type)
    else:
        blob.upload_from_string(
            text, content_type=content_type, if_generation_match=if_generation_match
        )


def append_jsonl_line(
    bucket_name: str, key: str, line: str, max_retries: int = 2
) -> None:
    """
    Safely append one NDJSON line using generation preconditions.
    Retries if someone else wrote between read and write (e.g., restart overlap).
    """
    for _ in range(max_retries + 1):
        # Fast path: create if missing
        if not object_exists(bucket_name, key):
            try:
                upload_text(bucket_name, key, line + "\n", if_generation_match=0)
                return
            except Exception:
                # Object appeared between check and write; fall through
                pass

        # Read-modify-write with optimistic lock
        gen = get_generation(bucket_name, key)
        current = download_text(bucket_name, key)
        new_text = current + ("" if current.endswith("\n") or current == "" else "\n") + line + "\n"
        try:
            upload_text(bucket_name, key, new_text, if_generation_match=gen)
            return
        except Exception:
            # Generation mismatch; someone else wrote. Retry.
            continue

    # Last attempt (very unlikely to reach)
    gen = get_generation(bucket_name, key)
    current = download_text(bucket_name, key)
    new_text = current + ("" if current.endswith("\n") or current == "" else "\n") + line + "\n"
    upload_text(bucket_name, key, new_text, if_generation_match=gen)


def list_prefix(bucket_name: str, prefix: str) -> List[str]:
    """List object names under a prefix (non-recursive in UI sense; GCS is flat)."""
    b = _bucket(bucket_name)
    return [blob.name for blob in b.list_blobs(prefix=prefix)]


def compose_many(
    bucket_name: str,
    sources: List[str],
    destination: str,
    temp_prefix: str = "_tmp/compose/",
) -> None:
    """
    Compose many small NDJSON parts into one destination object.
    GCS compose supports up to 32 sources per call; we chain in batches.
    The parts are concatenated in sorted order.
    """
    if not sources:
        return

    b = _bucket(bucket_name)
    sources = list(sorted(sources))

    # If only one source, just copy/rewrite it to destination.
    if len(sources) == 1:
        b.blob(destination).rewrite(b.blob(sources[0]))
        return

    def _compose_batch(src_names: List[str], out_name: str) -> None:
        b.blob(out_name).compose([b.blob(n) for n in src_names])

    temp_name = f"{temp_prefix}{destination}"
    i = 0
    chunk = 32
    current: Optional[str] = None

    while i < len(sources):
        batch = sources[i : i + chunk]
        if current is None:
            # First batch → temp
            _compose_batch(batch, temp_name)
            current = temp_name
        else:
            # Compose current + next batch → new temp
            temp2 = f"{temp_name}.part{i}"
            _compose_batch([current] + batch, temp2)
            current = temp2
        i += chunk

    # Final copy to destination
    b.blob(destination).rewrite(b.blob(current))
