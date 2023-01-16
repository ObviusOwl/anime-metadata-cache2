import io
import logging
import pathlib
import time

import minio
from urllib3.response import HTTPResponse

from amc2_api.utils import URL
from .model import Persisted, PersistedStat, ObjectNotFound, parse_mtime, format_mtime

from typing import List, Optional, Dict, Union, Tuple, cast, Any, Dict


_logger = logging.getLogger(__name__)


class S3ObjectStore:
    endpoint: str
    bucket: str
    path: str
    secure: bool
    empty_is_abstent: bool

    @classmethod
    def from_url(cls, url: Union[str, URL]) -> 'S3ObjectStore':
        # TODO: support username/password
        url = URL(url)
        endpoint = url.hostname + (f':{url.port}' if url.port is not None else '')

        # note: parts[0] is '/' parts[1] is the bucket name, the rest is the path
        parts = pathlib.PurePath(url.path).parts
        if len(parts) < 1:
            raise ValueError("Missing bucket name in s3:// URL")
        bucket = parts[1]
        path = '/'.join(parts[2:])
        secure = (url.scheme.lower() == 's3s')

        return cls(endpoint, bucket=bucket, path=path, no_empty=True, secure=secure)

    def __init__(
        self, 
        endpoint: str, 
        bucket: str, 
        path: str = '',
        no_empty: bool = True, 
        secure: bool = False
    ) -> None:
        self.endpoint = endpoint
        self.bucket = bucket
        self.path = path
        self.secure = secure
        self.empty_is_abstent = no_empty
    
    def _make_path(self, name: str) -> str:
        return self.path + '/' + name if self.path else name
    
    def _stat(self, client: minio.Minio, name: str) -> PersistedStat:
        try:
            obj = client.stat_object(self.bucket, self._make_path(name))
            metadata = cast(Dict[str, Any], obj.metadata)

            if self.empty_is_abstent and obj.size == 0:
                raise ObjectNotFound()

            obj_mtime = parse_mtime(obj.last_modified, time.time())
            mtime = parse_mtime(metadata.get('x-amz-meta-last-modified'), obj_mtime)
            ftime = parse_mtime(metadata.get('x-amz-meta-last-fetched'), mtime)

            return PersistedStat(
                content_type=cast(str, obj.content_type),
                last_modified=mtime,
                last_fetched=ftime,
                size=cast(int, obj.size)
            )
        except minio.error.S3Error as e:
            if e.code == 'NoSuchKey':
                raise ObjectNotFound()
            raise e

    def stat(self, name: str) -> PersistedStat:
        client = minio.Minio(self.endpoint, secure=self.secure)
        return self._stat(client, name)

    def get(self, name: str) -> Persisted:
        client = minio.Minio(self.endpoint, secure=self.secure)

        stat = self._stat(client, name)
        response: Optional[HTTPResponse] = None
        try:
            response = client.get_object(self.bucket, self._make_path(name))
            return Persisted.with_data(stat, response.data)
        except minio.error.S3Error as e:
            if e.code == 'NoSuchKey':
                raise ObjectNotFound()
            raise e
        finally:
            if response:
                response.close()
                response.release_conn()

    def put(self, name: str, obj: Persisted) -> None:
        client = minio.Minio(self.endpoint, secure=self.secure)
        metadata = {
            'x-amz-meta-last-fetched': format_mtime(obj.last_fetched),
            'x-amz-meta-last-modified': format_mtime(obj.last_modified)
        }
        client.put_object(
            self.bucket, 
            self._make_path(name), 
            io.BytesIO(obj.data),
            obj.size,
            content_type=obj.content_type,
            metadata=metadata
        )
