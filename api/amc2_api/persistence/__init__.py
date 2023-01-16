from amc2_api.utils import URL

from .model import Persisted, PersistedStat, ObjectStore, ObjectNotFound, WriteNotSupported
from .file_store import parse_file_url, FileObjectStore, SingleFileObjectStore, NullObjectStore
from .cached_store import CachedObjectStore
from .http_store import HTTPObjectStore


def object_store_factory(base_url: str) -> ObjectStore:
    url = URL(base_url)
    if url.scheme.lower() == 'file':
        return FileObjectStore(base_url)
    elif url.scheme.lower() in ('s3', 's3s'):
        try:
            from .s3_store import S3ObjectStore
        except ImportError as e:
            raise ValueError("Cannot use s3:// url, missing dependency: " + str(e))
        return S3ObjectStore.from_url(url)
    elif url.scheme.lower() == 'null':
        return NullObjectStore()
    raise ValueError(f"Unknown URL scheme '{url.scheme}'")


__all__ = [
    'Persisted',
    'PersistedStat',
    'ObjectStore',
    'ObjectNotFound',
    'WriteNotSupported',

    'parse_file_url',
    'FileObjectStore',
    'SingleFileObjectStore',
    'NullObjectStore',

    'CachedObjectStore',

    'HTTPObjectStore',
]
