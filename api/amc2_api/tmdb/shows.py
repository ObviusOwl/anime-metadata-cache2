import logging
import pathlib
import json

import requests

from amc2_api.model import Title, TitleEntry, TitleRepo
from amc2_api.model import Episode, Anime, AnimeEntry, AnimeRepo, TmdbSeasonId
from amc2_api.persistence import Persisted, PersistedStat, ObjectNotFound, ObjectStore, WriteNotSupported, FileObjectStore, HTTPObjectStore, object_store_factory
from amc2_api.utils import URL, Throttler, parse_mime

from .parse import iter_collection, AnimeTmdbJsonParser


from typing import List, Optional, Dict, Union, Tuple, Protocol, Any, Iterator

_logger = logging.getLogger(__name__)


class TmdbApiShowStore(HTTPObjectStore):
    """
    Base URL should be ``https://api.themoviedb.org/3`` or an equivalent.
    Implements rate limiting.
    """

    user_agent = 'animemetacache'
    base_url: str
    api_key: str

    languages: List[str] = ['de', 'en']

    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url
        self.api_key = api_key
        super().__init__(req_interval=0.25, err_interval=15*60)

    def _parse_name(self, name: str) -> URL:
        path = pathlib.PurePath(name)
        lang = path.parts[0]

        if lang not in self.languages:
            raise ObjectNotFound(f"Invalid language '{lang}', expected {self.languages}")

        if path.suffix.lower() != '.json':
            raise ObjectNotFound(f"Not a json file, ends with '{path.suffix}'")
        tid = path.stem

        url = URL(self.base_url).joinpath('tv', tid)

        url.query['api_key'] = self.api_key
        if lang != 'en':
            url.query['language'] = lang
        
        return url
    
    def _api_fetch(self, url: URL, subpath: str) -> requests.Response:
        if subpath:
            url = url.joinpath(subpath)
        else:
            url = url.copy()
        return self._http('GET', url)
    
    def _api_json(self, url: URL, subpath: str) -> Any:
        try:
            return self._api_fetch(url, subpath).json()
        except requests.JSONDecodeError:
            raise ObjectNotFound(f"Error decoding API JSON, URL {url}")
    
    def _api_images(self, url: URL, base: str) -> Any:
        # also query images without language, in english and japanese
        url = url.with_qs(include_image_language='en,null,ja')
        base = base.rstrip('/')
        subpath = base + '/images' if base else 'images'
        return self._api_json(url, subpath)
    
    def stat(self, name: str) -> PersistedStat:
        # Don't actually reach out to the API. By definition always fresh.
        return PersistedStat(content_type='text/json')

    def get(self, name: str) -> Persisted:
        try:
            url = self._parse_name(name)

            main = self._api_json(url, '')
            main['images'] = self._api_images(url, '')
            main['alternative_titles'] = self._api_json(url, 'alternative_titles')

            for season, sid in iter_collection(main, 'seasons', 'season_number'):
                season_base = f'season/{sid}'
                full_season = self._api_json(url, season_base)
                season.clear()
                season.update(full_season)
                season['images'] = self._api_images(url, season_base)
                season['credits'] = self._api_json(url, season_base + '/aggregate_credits')

                for episode, eid in iter_collection(season, 'episodes', 'episode_number'):
                    episode_base = season_base + f'/episode/{eid}'
                    full_episode = self._api_json(url, episode_base)
                    episode.clear()
                    episode.update(full_episode)
                    episode['images'] = self._api_images(url, episode_base)

            data = json.dumps(main).encode()
            return Persisted(content_type='text/json', data=data)
        except ObjectNotFound as e:
            e.object_name = name
            raise e


def tmdb_show_store(base_url: str) -> ObjectStore:
    url = URL(base_url)
    if url.scheme.lower() in ('http', 'https'):
        api_key = ''
        try:
            api_key = url.query['api_key']
        except KeyError:
            pass

        if not api_key:
            raise ValueError("The tvdb base URL must contain the api_key query parameter")

        return TmdbApiShowStore(base_url, api_key)
    else:
        return object_store_factory(base_url)


class TmdbApiImageStore(HTTPObjectStore):
    """
    Base URL should be ``https://api.themoviedb.org/3?api_key=...``or an equivalent.
    The /condiguration endpoint needs api key auth, the image files don't
    """

    user_agent = 'animemetacache'

    _api_url: URL
    _base_url: Optional[URL]
    _config_throttler: Throttler

    def __init__(self, api_url: Union[URL, str]) -> None:
        self._api_url = URL(api_url)
        self._base_url = None
        self._config_throttler = Throttler(60*60*24*2)
        super().__init__(req_interval=4, err_interval=30*60)

    def _fetch_config(self) -> Any:
        resp = self._http('GET', self._api_url.joinpath('configuration'))
        if not resp.ok:
            raise ObjectNotFound("Failed to contact tmdb API /configuration endpoint: " + resp.reason)
        return resp.json()

    def _make_url(self, name: str, stat: bool) -> str:
        if self._base_url is None or self._config_throttler.check():
            self._base_url = URL(self._fetch_config()['images']['secure_base_url'])
            self._config_throttler.mark()
        url = self._base_url.joinpath('original', name.strip('/'))
        return str(url)


def tmdb_image_store(base_url: str) -> ObjectStore:
    if base_url.startswith('https://') or base_url.startswith('http://'):
        return TmdbApiImageStore(base_url)
    else:
        return object_store_factory(base_url)


class PersistedTmdbAnimeRepo:
    _json_store: ObjectStore

    def __init__(self, json_store: ObjectStore) -> None:
        self._json_store = json_store
    
    def get(self, aid: str) -> Optional[AnimeEntry]:
        try:
            json_obj = self._json_store.get(f'en/{aid}.json')
        except ObjectNotFound:
            return None
        
        if parse_mime(json_obj.content_type)[1] != 'json':
            raise RuntimeError(f"Expected json mime-type, got {json_obj.content_type}")
        
        try:
            data = json_obj.data.decode()
            anime = AnimeTmdbJsonParser.parse(
                data, 
                lang='en'
            )
        except (UnicodeError, ValueError) as e:
            raise RuntimeError(str(e))
        
        if anime is not None:
            return AnimeEntry(anime=anime, age=json_obj.last_modified)
        return None
