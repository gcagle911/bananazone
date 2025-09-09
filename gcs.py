import json
from typing import List, Optional
from google.cloud import storage

def _client():
    return storage.Client()

def _bucket(name: str):
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

def upload_text(bucket_name: str, key: str, text: str, if_generation_match: Optional[int] = None, content_type: str = "application/json"):
    b = _bucket(bucket_name)
    blob = b.blob(key)
    if if_generation_match is None:
        blob.upload_from_string(text, content_type=content_type)
    else:
        blob.upload_from_string(text, content_type=content_type, if_generation_match=if_generation_match)

def append_jsonl_line(bucket_name: str, key: str, line: str, max_retries: int = 2):
    """
    Safely append one NDJSON line using generation preconditions.
    Retries if someone else wrote between read and write (e.g., restart overlap).
    """
    for _ in range(max_retries + 1):
        exists = object_exists(bucket_name, key)
        if not exists:
            try:
                upload_text(bucket_name, key, line + "\n", if_generation_match=0)
                return
            except Exception:
                pass  # lost the race to create; fall through to read-modify-write

        gen = get_generation(bucket_name, key)
        current = download_text(bucket_name, key)
        new_text = current + ("" if current.endswith("\n") or current == "" else "\n") + line + "\n"
        try:
            upload_text(bucket_name, key, new_text, if_generation_match=gen)
            return
        except Exception:
            continue  # generation mismatch, retry

    # last attempt
    gen = get_generation(bucket_name, key)
    current = download_text(bucket_name, key)
    new_text = current + ("" if current.endswith("\n") or current == "" else "\n") + line + "\n"
    upload_text(bucket_name, key, new_text, if_generation_match=gen)

def list_prefix(bucket_name: str, prefix: str) -> List[str]:
    b = _bucket(bucket_name)
    return [blob.name for blob in b.list_blobs(prefix=prefix)]

def compose_many(bucket_name: str, sources: List[str], destination: str, temp_prefix: str = "_tmp/compose/"):
    """
    Compose many small objects into one (chains in batches of 32, GCS limit).
    Sources must be NDJSON pieces to remain valid when concatenated.
    """
    if not sources:
        return
    b = _bucket(bucket_name)
    sources = list(sorted(sources))

    if len(sources) == 1:
        src = b.blob(sources[0])
        dest = b.blob(destination)
        dest.rewrite(src)
        return

    def _compose(to_names: List[str], out_name: str):
        dest = b.blob(out_name)
        dest.compose([b.blob(n) for n in to_names])

    temp_name = f"{temp_prefix}{destination}"

    i = 0
    chunk = 32
    current = None
    while i < len(sources):
        batch = sources[i:i+chunk]
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
