import datetime
import time

from typing import List, Optional, Dict, Union, Tuple, Protocol, Any


class ObjectNotFound(Exception):
    pass


class WriteNotSupported(Exception):
    pass


def parse_mtime(value: Any, default: Optional[float] = None) -> float:
    try:
        if isinstance(value, datetime.datetime):
            return value.timestamp()
        elif isinstance(value, str):
            return datetime.datetime.fromisoformat(value).timestamp()
        elif isinstance(value, float):
            return value
        elif value is None:
            raise ValueError("Time must not be none")
        raise ValueError("Value is not a time spec")
    except ValueError as e:
        if default is not None:
            return default
        raise


def format_mtime(value: float) -> str:
    dt = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
    return dt.isoformat()


class PersistedStat:
    content_type: str
    _last_modified: float
    _last_fetched: float
    _ttl: float
    _size: int

    def __init__(
        self,
        content_type: str = 'application/octet-stream',
        last_modified: Optional[float] = None,
        last_fetched: Optional[float] = None,
        ttl: float = -1,
        size: int = 0,
    ) -> None:
        self.content_type = content_type

        if last_modified is None:
            last_modified = time.time()
        self.last_modified = last_modified

        if last_fetched is None:
            last_fetched = time.time()
        self.last_fetched = last_fetched
    
        self.ttl = ttl
        self.size = size


    @property
    def size(self) -> int:
        return self._size
    
    @size.setter
    def size(self, value: int) -> None:
        if value < 0:
            raise ValueError("Size must be a positive value")
        self._size = value

    @property
    def last_modified(self) -> float:
        return self._last_modified
    
    @last_modified.setter
    def last_modified(self, value: float) -> None:
        if value < 0:
            raise ValueError("last_modified date must be a positive value")
        self._last_modified = value

    @property
    def last_fetched(self) -> float:
        return self._last_fetched

    @last_fetched.setter
    def last_fetched(self, value: float) -> None:
        if value < 0:
            raise ValueError("last_fetched date must be a positive value")
        self._last_fetched = value

    @property
    def ttl(self) -> float:
        return self._ttl
    
    @ttl.setter
    def ttl(self, value: float) -> None:
        self._ttl = value

    def is_expired(
        self, 
        ttl: Union[None, float, int] = None,
        now: Union[None, datetime.datetime, float] = None
    ) -> bool:
        if ttl is None:
            ttl = self.ttl
        if ttl < 0:
            return False
        
        if now is None:
            now = time.time()
        elif isinstance(now, datetime.datetime):
            now = now.timestamp()
        
        return now >= (self.last_fetched + ttl)
    
    def expiry_time(self):
        if self._ttl < 0:
            return float("inf")
        return self._last_fetched + self._ttl


class Persisted(PersistedStat):
    data: bytes

    def __init__(
        self,
        content_type: str = 'application/octet-stream',
        last_modified: Optional[float] = None,
        last_fetched: Optional[float] = None,
        ttl: float = -1,
        data: bytes = b'',
    ) -> None:
        super().__init__(
            content_type=content_type,
            last_modified=last_modified,
            last_fetched=last_fetched,
            ttl=ttl
        )
        self.data = data

    @classmethod
    def with_data(cls, base: PersistedStat, data: bytes) -> 'PersistedStat':
        return cls(
            content_type=base.content_type,
            last_modified=base.last_modified,
            last_fetched=base.last_fetched,
            ttl=base.ttl,
            data=data
        )
    
    @property
    def size(self) -> int:
        return len(self.data)
    
    @size.setter
    def size(self, value: int) -> None:
        pass


class ObjectStore(Protocol):

    def stat(self, name: str) -> PersistedStat:
        raise NotImplementedError()

    def get(self, name: str) -> Persisted:
        raise NotImplementedError()

    def put(self, name: str, obj: Persisted) -> None:
        # read only stores shall raise WriteNotSupported
        raise NotImplementedError()
