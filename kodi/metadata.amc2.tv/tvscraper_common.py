# -*- coding: UTF-8 -*-
import xbmcplugin, xbmcgui, xbmc, xbmcaddon # type: ignore
import requests
import functools
import json

from typing import Dict, Optional, Any, cast, List, Tuple, Union

from utils import URL, get_plugin_params, get_plugin_handle


class ConfigError(Exception):
    pass


class Config:
    _addon: Any
    _params: Dict[str, str]
    _instance_settings: Dict[str, str]

    def __init__(self) -> None:
        self._addon = xbmcaddon.Addon()
        self._params = {}
        self._instance_settings = {}

    def _get_main(self, name: str) -> str:
        return str(self._addon.getSetting(name))
    
    def _get_instance(self, name) -> str:
        if not self._instance_settings:
            json_str = self.plugin_params.get('pathSettings', '{}')
            data = {str(k): str(v) for k,v in json.loads(json_str).items()}
            self._instance_settings = data
        return self._instance_settings[name]
    
    @property
    def addon_id(self) -> str:
        return str(self._addon.getAddonInfo('id'))

    @property
    def plugin_params(self) -> Dict[str, str]:
        if not self._params:
            self._params = get_plugin_params()
        return self._params
    
    @property
    def plugin_handle(self) -> int:
        return get_plugin_handle()

    @property
    def api_base_url(self) -> URL:
        url = self._get_instance('api_base_url')
        if not url:
            raise ConfigError("The API base url must be set")
        
        try:
            return URL(url)
        except ValueError:
            raise ConfigError("The API base url is not a valid URL") from None
    
    @property
    def language(self) -> str:
        # TODO
        return 'en'

    @property
    def default_rating_source(self) -> str:
        # TODO
        return 'anidb'
    
    @property
    def cast_picture_is_character(self) -> bool:
        # TODO
        return False


def api_make_request(
    method: str, 
    url: Union[str, URL],
    json: Optional[Dict[Any, Any]] = None
) -> Any:
    headers = {
        'User-Agent': 'Amc2KodiScraper'
    }
    res = requests.request(method, str(url), headers=headers, json=json)
    res.raise_for_status()
    return res.json()


@functools.lru_cache
def api_get_json(url: str) -> Any:
    return api_make_request('GET',url)


def api_get_anime(config: Config, anime_id: str) -> Any:
    anime_url = config.api_base_url.joinpath('anime', anime_id)
    return api_get_json(str(anime_url))


def parse_episode_url(url: Union[str, URL]) -> Tuple[str, int, int]:
    # ('/', 'anime', 'A123T456S7', 'seasons', '1', 'episodes', '5')

    try:
        parts = URL(url).path_parts()
    except ValueError as e:
        raise ValueError("Not an episode url: not an url at all") from None

    if len(parts) < 7:
        raise ValueError("Not an episode url: path is too short")

    if not (parts[1], parts[3], parts[5]) == ('anime', 'seasons', 'episodes'):
        raise ValueError("Not an episode url: missing 'anime', 'seasons', 'episodes' path components")
    
    anime_id = parts[2]
    try:
        s_num = int(parts[4])
        ep_num = int(parts[6])
    except ValueError:
        raise ValueError("Not an episode url: episode/season number is not an integer") from None

    return anime_id, s_num, ep_num


def find_title(titles: Any, lang: str = 'en', use_main: bool = False) -> str:
    main = [t for t in titles if t['type'] == 'main']
    if main and use_main:
        return main[0]['title']

    by_lang = [t for t in titles if t['lang'] == lang]
    official = [t for t in by_lang if t['type'] == 'official']

    if official:
        return official[0]['title']
    elif by_lang:
        return by_lang[0]['title']
    
    # fallbacks
    no_lang = [t for t in titles if t['lang'] == '']
    if main:
        return main[0]['title']
    if no_lang:
        return no_lang[0]['title']
    return titles[0]['title']


def find_by_number(items: Any, number: int) -> Optional[Any]:
    for item in items:
        if item['number'] == number:
            return item
    return None


def filter_castrole(config: Config, cast: Any) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    for role in cast:
        item = {
            'name': role['actor'], 
            'role': role['character']
        }

        actor_img = role.get('actor_image', None)
        char_img = role.get('character_image', None)

        if actor_img is not None and not config.cast_picture_is_character:
            item['thumbnail'] = actor_img['_links']['image']['href']
        elif char_img is not None and config.cast_picture_is_character:
            item['thumbnail'] = char_img['_links']['image']['href']

        result.append(item)
    return result


def filter_writers(config: Config, credits: Any) -> List[str]:
    result: str = []
    for credit in credits:
        if credit['category'].lower() == 'writing':
            result.append(credit['name'])
    return result
