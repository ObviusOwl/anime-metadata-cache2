import logging
import pathlib
import threading
import urllib.parse
import os
import xattr
import mimetypes
import time

from .model import Persisted, PersistedStat, ObjectNotFound, parse_mtime, format_mtime

from typing import List, Optional, Dict, Union, Tuple


_logger = logging.getLogger(__name__)


def parse_file_url(url: str) -> pathlib.Path:
    if not url.startswith('file://'):
        raise ValueError(f"Is not a file:// URL '{url}'")

    parts = urllib.parse.urlsplit(url)
    if not parts.path:
        raise ValueError(f"The URL '{url}' must contain a path")
    
    return pathlib.Path(parts.path)


def guess_file_content_type(path: Union[str, pathlib.Path]) -> str:
    """
    Guesses the mime type for the given file by looking at the xattrs or extension.

    See Also:
        - https://www.freedesktop.org/wiki/CommonExtendedAttributes/
        - https://docs.python.org/3/library/mimetypes.html
    """
    mime: Optional[str] = None
    try:
        # if present the xattr shall be leading
        mime = xattr.xattr(path).get('user.mime_type').decode()
    except (OSError, UnicodeError):
        # guess based on the extension
        mime, _ = mimetypes.guess_type(path)
    
    # fallback to octet-stream
    return mime if mime is not None else 'application/octet-stream'


class XAttrFile:
    path: pathlib.Path
    namespace: str

    def __init__(self, path: Union[str, pathlib.Path], ns: str = 'user') -> None:
        self.path = pathlib.Path(path)
        self.namespace = ns
    
    def full_key(self, key: str) -> str:
        return '.'.join([self.namespace, key])

    def get_attr(self, key: str, default: Optional[str] = None) -> Optional[str]:
        try:
            return xattr.xattr(self.path).get(self.full_key(key)).decode()
        except (OSError, UnicodeError):
            return default
    
    def set_attr(self, key: str, value: str) -> None:
        key = self.full_key(key)
        try:
            xattr.xattr(self.path).set(key, value.encode())
        except OSError as e:
            _logger.error(f"Failed to set xattr {key} for {self.path}: {e}")


class FileObjectStore:
    """
    This store can access objects via an absolute filesystem path, a file:// URL
    or a relative path (relative to the base path). 
    
    The base path can be an absolute path or a file:// URL. 
    This can be used in a factory method to transparently create object-store 
    instances based on on the input URL (http:// or file:// etc...)

    The content_type is stored as xattr using the Freedesktop standard (user.mime_type).
    If not present, the mime is guessed using the file extension.
    """

    base_path: pathlib.Path
    _lock: threading.RLock

    def __init__(self, base_path: Union[pathlib.Path, str]) -> None:
        if isinstance(base_path, pathlib.Path):
            self.base_path = base_path
        elif isinstance(base_path, str) and base_path.startswith('file://'):
            self.base_path = parse_file_url(base_path)
        else:
            self.base_path = pathlib.Path(base_path)

        self._lock = threading.RLock()

    def _name2path(self, name: str) -> pathlib.Path:
        if name.startswith('file://'):
            return parse_file_url(name)
        elif name.startswith('/'):
            return pathlib.Path(name)
        else:
            return self.base_path.joinpath(name)

    def _get_metadata(self, path: pathlib.Path) -> Dict[str, str]:
        keys = (
            'mime_type',
            'last_modified',
            'last_fetched',
        )
        metadata: Dict[str, str] = {}
        file = XAttrFile(path, ns='user')

        for key in keys:
            if value := file.get_attr(key) is not None:
                metadata[key] = value
        return metadata
    
    def _set_metadata(self, path: pathlib.Path, metadata: Dict[str, str]) -> None:
        file = XAttrFile(path, ns='user')
        for key, value in metadata.items():
            file.set_attr(key, value)

    def stat(self, name: str) -> PersistedStat:
        path = self._name2path(name)
        try:
            with self._lock:
                stat = path.stat()
                mime = guess_file_content_type(path)
                metadata = self._get_metadata(path)
        except OSError as e:
            _logger.debug(f"File HEAD '{path}' 404, {str(e)}")
            raise ObjectNotFound()

        mtime = parse_mtime(metadata.get('last_modified'), stat.st_mtime)
        ftime = parse_mtime(metadata.get('last_fetched'), mtime)

        _logger.debug(f"File STAT '{path}' 200")
        return PersistedStat(
            content_type=mime,
            last_modified=mtime,
            last_fetched=ftime,
            size=stat.st_size,
        )

    def get(self, name: str) -> Persisted:
        path = self._name2path(name)
        with self._lock:
            stat = self.stat(name)
            try:
                with open(path, 'rb') as fh:
                    data = fh.read()
                    _logger.debug(f"File GET '{path}' 200 {len(data)}")
                    return Persisted.with_data(stat, data)
            except OSError as e:
                _logger.error(f"Failed to read file {path}: {e}")

    def put(self, name: str, obj: Persisted) -> None:
        path = self._name2path(name)
        
        metadata = {
            'mime_type':  obj.content_type,
            'last_modified': format_mtime(obj.last_modified),
            'last_fetched': format_mtime(obj.last_fetched),
        }

        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'wb') as fh:
                fh.write(obj.data)
            
            os.utime(path, times=(time.time(), obj.last_modified))
            self._set_metadata(path, metadata)

            _logger.debug(f"File PUT '{path}' 200 {obj.size}")


class SingleFileObjectStore(FileObjectStore):
    override_name: str

    def __init__(self, file_path: Union[pathlib.Path, str]) -> None:
        if not isinstance(file_path, pathlib.Path):
            if isinstance(file_path, str) and file_path.startswith('file://'):
                file_path = parse_file_url(file_path)
            else:
                file_path = pathlib.Path(file_path)

        self.override_name = file_path.name
        super().__init__(file_path.parent)

    def stat(self, name: str) -> PersistedStat:
        return super().stat(self.override_name)

    def get(self, name: str) -> Persisted:
        return super().get(self.override_name)

    def put(self, name: str, obj: Persisted) -> None:
        super().put(self.override_name, obj)


class NullObjectStore:
    def stat(self, name: str) -> PersistedStat:
        raise ObjectNotFound(name)

    def get(self, name: str) -> Persisted:
        raise ObjectNotFound(name)

    def put(self, name: str, obj: Persisted) -> None:
        pass
