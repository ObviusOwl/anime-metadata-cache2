import logging

import requests

from amc2_api.model import Title, TitleEntry, TitleRepo
from amc2_api.model import Episode, Anime, AnimeEntry, AnimeRepo
from amc2_api.persistence import Persisted, PersistedStat, ObjectNotFound, ObjectStore, WriteNotSupported, HTTPObjectStore, object_store_factory
from amc2_api.utils import URL, parse_mime

from .parse import parse_api_error, AnimeXMLParser


from typing import List, Optional, Dict, Union, Tuple

_logger = logging.getLogger(__name__)


class AnidbApiAnimeStore(HTTPObjectStore):
    """
    Base URL should be ``http://api.anidb.net:9001/httpapi``or an equivalent.
    Implements rate limiting of one request every two seconds.

    Does not check if the anime exists before making an HTTP call. 
    This should be done using the titles XML in the main API.

    Stores objects using the key "{aid}.xml" where "aid" is the anime ID.
    """

    user_agent = 'animemetacache'
    client_id = 'animemetacache'
    client_version = '1'
    base_url: str

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        super().__init__(req_interval=4, err_interval=30*60)

    def _make_url(self, name: str, stat: bool) -> str:
        if name.endswith('.xml'):
            name = name[:-4]
        if not name.isdigit():
            raise ValueError("Anidb aid value is digits only")
        
        url = URL(self.base_url)
        url.query['request'] = 'anime'
        url.query['client'] = self.client_id
        url.query['clientver'] = self.client_version
        url.query['protover'] = '1'
        url.query['aid'] = name
        return str(url)

    def _handle_api_errors(self, name: str, data: bytes) -> None:
        err = parse_api_error(data)
        if err == 'anime not found':
            _logger.error(f"Anime {name} not found in the Anidb API")
            raise ObjectNotFound("Anime not found")
        elif err == 'banned':
            _logger.error("Anidb client got banned")
            self._err_throttler.mark()
            raise ObjectNotFound("Anidb cient got banned")
        elif err != '':
            _logger.error(f"Unknown Anidb error: '{err}'")
            self._err_throttler.mark()
            raise RuntimeError(f"Unknown Anidb error: '{err}'")

    def stat(self, name: str) -> PersistedStat:
        # Don't actually reach out to the API. By definition always fresh.
        return PersistedStat(content_type='text/xml')

    def _make_persisted(self, name: str, response: requests.Response) -> Persisted:
        self._handle_api_errors(name, response.content)
        return super()._make_persisted(name, response)


def anidb_anime_store(base_url: str) -> ObjectStore:
    if base_url.startswith('https://') or base_url.startswith('http://'):
        return AnidbApiAnimeStore(base_url)
    else:
        return object_store_factory(base_url)


class AnidbApiImageStore(HTTPObjectStore):
    """
    Base URL should be ``https://cdn-eu.anidb.net/images/main``or an equivalent.
    """

    user_agent = 'animemetacache'
    base_url: str

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        super().__init__(req_interval=4, err_interval=30*60)

    def _make_url(self, name: str, stat: bool) -> str:
        return str(URL(self.base_url).joinpath(name))


def anidb_image_store(base_url: str) -> ObjectStore:
    if base_url.startswith('https://') or base_url.startswith('http://'):
        return AnidbApiImageStore(base_url)
    else:
        return object_store_factory(base_url)


class PersistedAnimeRepo:
    _xml_store: ObjectStore
    _title_repo: TitleRepo

    def __init__(self, xml_store: ObjectStore, title_repo: TitleRepo) -> None:
        self._xml_store = xml_store
        self._title_repo = title_repo
    
    def _patch_anime(self, anime: Anime) -> None:
        title_entries = self._title_repo.find(Title(type='extra', aid=anime.id))
        anime.titles += [ent.title for ent in title_entries]
    
    def _check_exists(self, aid: str) -> bool:
        title_entries = self._title_repo.find(Title(aid=aid))
        title_entries = [ent for ent in title_entries if ent.title.type != 'extra']
        return bool(title_entries)

    def get(self, aid: str) -> Optional[AnimeEntry]:
        if not self._check_exists(aid):
            return None
        
        try:
            anime_xml_obj = self._xml_store.get(aid + '.xml')
        except ObjectNotFound:
            return None

        if parse_mime(anime_xml_obj.content_type)[1] != 'xml':
            raise RuntimeError(f"Expected xml mime-type, got {anime_xml_obj.content_type}")
        
        try:
            anime = AnimeXMLParser.parse(anime_xml_obj.data.decode())
        except (UnicodeError, ValueError) as e:
            raise RuntimeError(str(e))
        
        self._patch_anime(anime)
        return AnimeEntry(anime=anime, age=anime_xml_obj.last_modified)

