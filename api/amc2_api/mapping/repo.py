import threading
import sqlite3
import logging
import json
import pathlib

from amc2_api.persistence import ObjectStore, ObjectNotFound, Persisted, object_store_factory
from amc2_api.utils import URL

from .model import AnimeMapping, AnimeMappingRepo

from typing import  List, Dict, Union, Optional, Tuple, Any

_logger = logging.getLogger(__name__)


def anime_mapping_repo(url: Union[str, URL]) -> AnimeMappingRepo:
    url = URL(url)
    if url.scheme == 'sqlite':
        return SqliteAnimeMappingRepo(url.path)
    else:
        # s3://endpoint:9000/bucket/path/to/myfile.json
        path = pathlib.PurePath(url.path)
        if path.suffix != '.json':
            path = path.with_suffix('.json')

        url.path = str(path.parent)
        backend = object_store_factory(str(url))
        return JsonAnimeMappingRepo(path.name, backend)


class SqliteAnimeMappingRepo:
    _lock: threading.RLock
    _conn: sqlite3.Connection

    _ddl = '''
        CREATE TABLE IF NOT EXISTS anime_mapping (
            anidb_id TEXT,
            tmdb_id TEXT,
            PRIMARY KEY (anidb_id, tmdb_id) ON CONFLICT REPLACE
        )
    '''

    def __init__(self, dbfile: str = ':memory:') -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(dbfile, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(self._ddl)
        self._conn.commit()

    def _query_id(self, field_name: str, field_value: str) -> List[AnimeMapping]:
        query = f'SELECT anidb_id, tmdb_id FROM anime_mapping'
        data: List[str] = []

        if field_name:
            query += f' WHERE {field_name} = ?'
            data += [field_value]

        result: List[AnimeMapping] = []
        with self._lock:
            for row in self._conn.execute(query, tuple(data)):
                m = AnimeMapping(anidb=str(row['anidb_id']), tmdb=str(row['tmdb_id']))
                result.append(m)
        return result

    def resolve_tmdb(self, query: AnimeMapping) -> List[AnimeMapping]:
        if not query.anidb:
            raise ValueError("Expected anidb id to be set")
        return self._query_id('anidb_id', query.anidb)

    def resolve_anidb(self, query: AnimeMapping) -> List[AnimeMapping]:
        if not query.tmdb:
            raise ValueError("Expected tmdb id to be set")
        return self._query_id('tmdb_id', query.tmdb)

    def load(self, query: AnimeMapping) -> Optional[AnimeMapping]:
        if not query.tmdb or not query.anidb:
            raise ValueError("Expected tmdb and anidb ids to be set")

        sql_query  = f'SELECT anidb_id, tmdb_id FROM anime_mapping'
        sql_query += f' WHERE anidb_id = ? AND tmdb_id = ?'

        with self._lock:
            row = self._conn.execute(sql_query, (query.anidb, query.tmdb)).fetchone()
            if row:
                return AnimeMapping(anidb=str(row['anidb_id']), tmdb=str(row['tmdb_id']))
        return None

    def store(self, values: List[AnimeMapping], replace: bool = True) -> None:
        query_ins = 'INSERT INTO anime_mapping (anidb_id, tmdb_id) VALUES (?, ?)'
        query_del = 'DELETE FROM anime_mapping WHERE anidb_id = ? OR tmdb_id = ?'

        data: List[Tuple[str, str]] = []
        for value in values:
            if not value.tmdb or not value.anidb:
                raise ValueError("Expected tmdb and anidb id to be set")
            data.append((value.anidb, value.tmdb))

        with self._lock:
            try:
                if replace:
                    self._conn.executemany(query_del, data)
                self._conn.executemany(query_ins, data)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def remove(self, value: AnimeMapping) -> None:
        data: Dict[str, str] = {}
        if value.tmdb:
            data['tmdb_id'] = value.tmdb
        if value.anidb:
            data['anidb_id'] = value.anidb

        if not data:
            return []
        
        query = 'DELETE FROM anime_mapping WHERE '
        query += 'AND'.join([f' {k} = ? ' for k in data.keys()])

        with self._lock:
            try:
                self._conn.execute(query, tuple(data.values()))
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def dump(self) -> List[AnimeMapping]:
        return self._query_id('', '')

    def purge(self) -> None:
        with self._lock:
            try:
                self._conn.execute('DELETE FROM anime_mapping')
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise


class JsonAnimeMappingRepo:
    """
    A mappings repo that uses a simple json document stored on objectstore.

    Based on the idea that mappings are not much data that is also seldomly written.
    Useful for example to keep everything on the S3 store to make backups easier.

    The file is loaded/dumped as a whole when first loaded or when updated.
    Normal read access uses a cache that is kept in sync. 
    Modification of the backing file from outside is not supported.
    """

    filename: str
    cache: AnimeMappingRepo
    backend: ObjectStore
    _loaded: bool
    _lock: threading.RLock

    def __init__(
        self, 
        filename: str,
        backend: ObjectStore, 
        cache: Optional[AnimeMappingRepo] = None
    ) -> None:
        self.filename = filename
        self.backend = backend
        self.cache = cache if cache else SqliteAnimeMappingRepo()
        self._loaded = False
        self._lock = threading.RLock()
        
    def _load(self) -> None:
        if self._loaded:
            return

        self.cache.purge()
        try:
            data = self.backend.get(self.filename).data.decode()
            if data:
                items = [self._from_json(x) for x in json.loads(data)]
                self.cache.store(items, replace=False)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
            _logger.error("Failed to decode anime mapping repo json data: " + str(e))
        except ObjectNotFound:
            pass
        self._loaded = True

    def _save(self) -> None:
        json_data = [self._to_json(item) for item in self.cache.dump()]
        json_str = json.dumps(json_data, indent=4)
        obj = Persisted(content_type='text/json', data=json_str.encode())
        self.backend.put(self.filename, obj)

    def _to_json(self, value: AnimeMapping) -> Any:
        return {'anidb': value.anidb, 'tmdb': value.tmdb}

    def _from_json(self, value: Any) -> AnimeMapping:
        return AnimeMapping(anidb=str(value['anidb']), tmdb=str(value['tmdb']))

    def resolve_tmdb(self, query: AnimeMapping) -> List[AnimeMapping]:
        with self._lock:
            self._load()
            return self.cache.resolve_tmdb(query)

    def resolve_anidb(self, query: AnimeMapping) -> List[AnimeMapping]:
        with self._lock:
            self._load()
            return self.cache.resolve_anidb(query)

    def load(self, query: AnimeMapping) -> Optional[AnimeMapping]:
        with self._lock:
            self._load()
            return self.cache.load(query)

    def store(self, values: List[AnimeMapping], replace: bool = True) -> None:
        with self._lock:
            self._load()
            self.cache.store(values, replace=replace)
            self._save()

    def remove(self, value: AnimeMapping) -> None:
        with self._lock:
            self._load()
            self.cache.remove(value)
            self._save()

    def dump(self) -> List[AnimeMapping]:
        with self._lock:
            self._load()
            return self.cache.dump()

    def purge(self) -> None:
        with self._lock:
            self._load()
            self.cache.purge()
            self._save()
