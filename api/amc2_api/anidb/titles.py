import datetime
import threading
import logging
import gzip
import time

import requests
import sqlite3

from amc2_api.model import Title, TitleRepo, TitleEntry
from amc2_api.persistence import Persisted, PersistedStat, ObjectNotFound, ObjectStore, WriteNotSupported, SingleFileObjectStore, HTTPObjectStore

from .parse import parse_titles_xml

from typing import List, Optional, Dict, Union, Tuple

_logger = logging.getLogger(__name__)


class AnidbApiTitleStore(HTTPObjectStore):
    titles_url: str
    user_agent = 'animemetacache'
    client_id = 'animemetacache'
    client_version = '1'

    def __init__(self, titles_url: str) -> None:
        self.titles_url = titles_url
        super().__init__(req_interval=4, err_interval=30*60)

    def _make_url(self, name: str, stat: bool) -> str:
        return self.titles_url

    def stat(self, name: str) -> PersistedStat:
        # Don't actually reach out to the API. By definition always fresh.
        return PersistedStat(content_type='text/xml')

    def _make_content(self, name: str, response: requests.Response) -> bytes:
        # note: requests handles gzip transport encoding, not gzip file download
        return gzip.decompress(response.content)

    def stat(self, name: str) -> PersistedStat:
        # Don't actually reach out to the API. By definition always fresh.
        return PersistedStat(content_type='text/xml')

    def put(self, name: str, obj: Persisted) -> None:
        raise WriteNotSupported("No upload to the Anidb titles XML file")


def anidb_titles_store(titles_url: str) -> ObjectStore:
    if titles_url.startswith('https://') or titles_url.startswith('http://'):
        return AnidbApiTitleStore(titles_url)
    elif titles_url.startswith('file://') or titles_url.startswith('/'):
        return SingleFileObjectStore(titles_url)
    else:
        raise ValueError("Invalid base url, expected http://, file:// or absolute path")


class SqliteTitleRepo(TitleRepo):
    _lock: threading.RLock
    _conn: sqlite3.Connection

    _ddl = '''
        CREATE TABLE IF NOT EXISTS titles (
            aid TEXT,
            type TEXT,
            lang TEXT,
            value TEXT,
            age TEXT,
            PRIMARY KEY (aid, type, lang, value) ON CONFLICT REPLACE
        )
    '''

    def __init__(self, dbfile: str = ':memory:') -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(dbfile, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(self._ddl)

    def find(self, title: Title) -> List[TitleEntry]:
        query = 'SELECT aid, type, lang, value, age FROM titles WHERE '
        data: Dict[str, str] = {}

        if title.value:
            data['value'] = title.value
        if title.lang:
            data['title'] = title.lang
        if title.type:
            data['type'] = title.type
        if title.aid:
            data['aid'] = title.aid
        
        if not data:
            # don't allow listing the full database (not so useful anyway)
            return []
        
        query += 'AND'.join([f' {k} = ? ' for k in data.keys()])

        result = []
        with self._lock:
            for row in self._conn.execute(query, tuple(data.values())):
                ftitle = Title(
                    value=row['value'], 
                    aid=row['aid'],
                    lang=row['lang'], 
                    type=row['type']
                )
                age = datetime.datetime.fromisoformat(row['age'])
                result.append(TitleEntry(title=ftitle, age=age))
        return result
    
    def store(self, title: TitleEntry) -> None:
        query = 'INSERT INTO titles (aid, type, lang, value, age) VALUES (?, ?, ?, ?, ?)'
        data = (
            title.title.aid,
            title.title.type,
            title.title.lang,
            title.title.value,
            title.age.isoformat()
        )
        with self._lock:
            self._conn.execute(query, data)
    
    def purge(self) -> None:
        query = 'DELETE FROM titles'
        with self._lock:
            self._conn.execute(query)

    def remove(self, title: Title) -> None:
        if not title.value:
            raise ValueError("Expected the title to have a value")

        query = 'DELETE FROM titles WHERE value = ?'
        data = [title.value]
        if title.aid:
            query += 'AND aid = ? '
            data.append(title.aid)
        if title.lang:
            query += 'AND title = ? '
            data.append(title.lang)
        if title.type:
            query += 'AND type = ? '
            data.append(title.type)

        with self._lock:
            self._conn.execute(query, data)
    

class XMLTitleRepo(TitleRepo):
    _repo: TitleRepo

    def __init__(self, backend: Optional[TitleRepo] = None) -> None:
        if backend is not None:
            self._repo = backend
        else:
            self._repo = SqliteTitleRepo(':memory:')
    
    def parse_data(self, data: str, age: Optional[datetime.datetime] = None) -> None:
        if not data:
            return
        
        if age is None:
            age = datetime.datetime.now(tz=datetime.timezone.utc)
        
        def handle_title(title: Title) -> None:
            self._repo.store(TitleEntry(title=title, age=age))

        parse_titles_xml(data, handle_title)
        
    def find(self, title: Title) -> List[TitleEntry]:
        return self._repo.find(title)

    def store(self, title: TitleEntry) -> None:
        self._repo.store(title)
    
    def purge(self) -> None:
        self._repo.purge()
    
    def remove(self, title: Title) -> None:
        self._repo.remove(title)


class OverlayTitleRepo(TitleRepo):
    base: TitleRepo
    overlay: TitleRepo

    def __init__(self, lower: TitleRepo, upper: TitleRepo) -> None:
        self.base = lower
        self.overlay = upper

    def find(self, title: Title) -> List[TitleEntry]:
        return self.base.find(title) + self.overlay.find(title)
    
    def store(self, title: TitleEntry) -> None:
        self.overlay.store(title)
    
    def purge(self) -> None:
        self.overlay.purge()
    
    def remove(self, title: Title) -> None:
        self.overlay.remove(title)


class PeristsedTitleRepo:
    _xml_store: ObjectStore
    _xml_repo: XMLTitleRepo
    _repo: TitleRepo

    _lock: threading.RLock
    _valid_until: float

    def __init__(self, xml_store: ObjectStore, extra_titles: TitleRepo) -> None:
        self._xml_store = xml_store
        self._xml_repo = XMLTitleRepo()
        self._repo = OverlayTitleRepo(self._xml_repo, extra_titles)

        self._lock = threading.RLock()
        self._valid_until = float("-inf")

    def _load(self):
        # make sure to reload the titles when the app runs for a long time
        if self._valid_until < time.time():
            obj = self._xml_store.get('anime-titles.xml')
            age = datetime.datetime.fromtimestamp(obj.last_modified)

            self._xml_repo.purge()
            self._xml_repo.parse_data(obj.data.decode(), age=age)
            self._valid_until = obj.expiry_time()
    
    def find(self, title: Title) -> List[TitleEntry]:
        with self._lock:
            self._load()
            return self._repo.find(title)
    
    def store(self, title: TitleEntry) -> None:
        self._repo.store(title)

    def purge(self) -> None:
        pass
    
    def remove(self, title: Title) -> None:
        self._repo.remove(title)

