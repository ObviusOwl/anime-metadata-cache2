import logging
import threading

from .model import Persisted, PersistedStat, ObjectNotFound, ObjectStore

from typing import List, Optional, Dict, Union, Tuple


_logger = logging.getLogger(__name__)


class CachedObjectStore:
    """
    This cache may return outdated data when the refresh was not successful.
    Also ObjectNotFound erros from the backend are not reflected in the cache.
    This is the implementation of a cache that favors archival over correctness
    (It is better to have data that may once have been correct, than none at all)
    """
    backend: ObjectStore
    cache: ObjectStore

    _ttu: float
    _lock: threading.RLock

    def __init__(
        self, 
        backend: ObjectStore, 
        cache: ObjectStore,
        ttu: int
    ) -> None:
        self.backend = backend
        self.cache = cache
        self._ttu = float(ttu)
        self._lock= threading.RLock()

    def _set_ttl(self, obj: PersistedStat) -> PersistedStat:
        if obj.ttl <= 0:
            obj.ttl = self._ttu
        else:
            obj.ttl = min(obj.ttl, self._ttu)
        return obj

    def stat(self, name: str) -> PersistedStat:
        obj: Optional[PersistedStat] = None
        with self._lock:
            # first try to get a not yet outdated S3 cache entry 
            obj = self._head_cache(name, self._ttu)

            # try to update the cache
            if obj is None:
                obj = self._head_backend(name)
            
            # if still so success, retry the cache accepting outdated data
            if obj is None:
                obj = self._head_cache(name, float('inf'))

            # finally give up
            if obj is None:
                raise ObjectNotFound()
        return obj

    def _head_cache(self, name: str, max_age: float) -> Optional[PersistedStat]:
        try:
            stat = self.cache.stat(name)
            if stat.is_expired(ttl=max_age):
                return None
            return self._set_ttl(stat)
        except ObjectNotFound as e:
            return None

    def _get_cache(self, name: str, max_age: float) -> Optional[Persisted]:
        try:
            stat = self.cache.stat(name)
            if stat.is_expired(ttl=max_age):
                _logger.debug(f"Item {name} found in cache but outdated")
                return None
            return self._set_ttl(self.cache.get(name))
        except ObjectNotFound as e:
            _logger.debug(f"Item {name} not found in cache: " + str(e))
            return None
    
    def _head_backend(self, name) -> Optional[PersistedStat]:
        try:
            return self._set_ttl(self.backend.stat(name))
        except ObjectNotFound as e:
            return None

    def _get_backend(self, name) -> Optional[Persisted]:
        try:
            return self._set_ttl(self.backend.get(name))
        except ObjectNotFound as e:
            _logger.debug(f"Item {name} not found in backend: " + str(e))
            return None

    def get(self, name: str) -> Persisted:
        with self._lock:
            obj: Optional[Persisted] = None

            # first try to get a not yet outdated S3 cache entry 
            obj = self._get_cache(name, self._ttu)

            # try to update the cache
            if obj is None:
                obj = self._get_backend(name)
                if obj is not None:
                    self.cache.put(name, obj)
            
            # if still so success, retry the cache accepting outdated data
            if obj is None:
                obj = self._get_cache(name, float('inf'))

            # finally give up
            if obj is None:
                raise ObjectNotFound()
            return obj

    def put(self, name: str, obj: Persisted) -> None:
        with self._lock:
            # may raise WriteNotSupported to prevent the put request
            self.backend.put(name, obj)
            # if put was successfull, keep the cache coherent
            self.cache.put(name, obj)
