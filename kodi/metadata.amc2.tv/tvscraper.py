# -*- coding: UTF-8 -*-
import xbmcplugin, xbmcgui, xbmc, xbmcaddon # type: ignore
import json
import datetime
import time
import re

from typing import Dict, Any, List, Tuple, Union, Optional

from utils import URL

from tvscraper_common import api_get_anime, api_make_request, parse_episode_url
from tvscraper_common import Config, find_title, filter_castrole, filter_writers, find_by_number


# https://kodi.wiki/view/Python_Problems
# monkeymatch datetime
# TypeError: 'NoneType' object is not callable
class proxydt(datetime.datetime):
    @classmethod
    def strptime(cls, date_string, format):
        return datetime.datetime(*(time.strptime(date_string, format)[:6]))
datetime.datetime = proxydt


def log(msg, level=xbmc.LOGDEBUG):
    xbmc.log(msg=msg, level=level)


def set_listitem_images(liz: Any, images: Any) -> None:
    fanart: List[Dict[str, str]] = []

    for image in images:
        img_url = image['_links']['image']['href']
        img_type = image['type']

        if img_type == 'backdrop':
            fanart.append({'image': img_url})
        elif img_type in ('poster', 'banner', 'thumb'):
            liz.addAvailableArtwork(img_url, img_type)
    
    liz.setAvailableFanart(fanart)


def set_listitem_ratings(liz: Any, ratings: Any, default_source: str = '') -> None:
    for rating in ratings:
        liz.setRating(
            rating['source'], 
            rating['average'], 
            votes = rating['votes'], 
            defaultt = (rating['source'] == default_source)
        )


def plugin_find(config: Config):
    url = config.api_base_url.joinpath('match').with_qs(title=config.plugin_params['title'])
    resp = api_make_request('GET', url)

    for item in resp['items']:
        anime_id = str(item['anime_id'])
        label = item['anidb']['title']['title'] + ' | ' + item['tmdb']['title']['title']
        liz = xbmcgui.ListItem(label, offscreen=True)
        #liz.setProperty('relevance', '0.5')
        liz.setInfo('video', {'title': label})

        xbmcplugin.addDirectoryItem(
            handle=config.plugin_handle, 
            url=anime_id,
            listitem=liz, 
            isFolder=True
        )
    xbmcplugin.endOfDirectory(config.plugin_handle)


def plugin_getdetails(config: Config) -> None:
    anime_id = config.plugin_params['url']

    # make sure the API remembers the user's choice (if this is a mapping ID)
    if 'A' in anime_id and 'T' in anime_id and 'S' in anime_id:
        match_url = config.api_base_url.joinpath('match', anime_id)
        api_make_request('PUT', match_url)

    anime = api_get_anime(config, anime_id)

    uniqueids = {str(k): str(v) for k, v in anime['uniqueids'].items()}
    uniqueids['amc2'] = anime['id']

    title = find_title(anime['titles'], lang=config.language, use_main=False)

    airdate: Optional[datetime.datetime] = None
    if 'airdate' in anime and anime['airdate']:
        airdate = datetime.datetime.strptime(anime['airdate'], '%Y-%m-%d')

    info = {
        'mediatype': 'tvshow',
        'title': title,
        'plot': anime['description'],
        'director': [str(d) for d in anime['directors']],
        'writer': filter_writers(config, anime['credits']),
        'genre': anime['genres'],
        'tag': [str(t) for t in anime['tags']],
        'tvshowtitle': title,
        # note: the wiki suggests that the pisodeguide should be a json-encode dict
        # containing a mapping of unique ids for multiple online databases
        # the string is also saved to the database
        'episodeguide': json.dumps(uniqueids),
    }

    if airdate:
        info['year'] = str(airdate.year)
        info['premiered'] = airdate.date().isoformat()

    liz = xbmcgui.ListItem(title, offscreen=True)
    liz.setInfo('video', info)
    #liz.addSeason(1, title)
    liz.setUniqueIDs(uniqueids, 'amc2')
    liz.setCast(filter_castrole(config, anime['cast']))

    set_listitem_images(liz, anime['images'])
    set_listitem_ratings(liz, anime['ratings'], config.default_rating_source)

    for season in anime['seasons']:
        title = find_title(season['titles'], lang=config.language, use_main=False)
        liz.addSeason(season['number'], title)
    
    xbmcplugin.setResolvedUrl(handle=config.plugin_handle, succeeded=True, listitem=liz)
    xbmcplugin.endOfDirectory(config.plugin_handle)


def plugin_getepisodelist(config: Config) -> None:
    uniqueids = json.loads(config.plugin_params['url'])
    anime_id = uniqueids['amc2']

    anime_url = config.api_base_url.joinpath('anime', anime_id)
    anime = api_get_anime(config, anime_id)

    for season in anime['seasons']:
        s_num = int(season['number'])
        for episode in season['episodes']:
            ep_num = int(episode['number'])

            title = find_title(episode['titles'], lang=config.language)

            info = {
                'title': title,
                'season': s_num,
                'episode': ep_num,
                'aired': episode['airdate'],
            }

            liz=xbmcgui.ListItem(title, offscreen=True)
            liz.setInfo('video', info)

            url = anime_url.joinpath('seasons', str(s_num), 'episodes', str(ep_num))

            xbmcplugin.addDirectoryItem(
                handle=config.plugin_handle, 
                url=str(url), 
                listitem=liz, 
                isFolder=False
            )

    xbmcplugin.endOfDirectory(config.plugin_handle)



def plugin_getepisodedetails(config: Config) -> None:
    anime_id, s_num, ep_num = parse_episode_url(config.plugin_params['url'])

    anime = api_get_anime(config, anime_id)

    season = find_by_number(anime['seasons'], s_num)
    if season is None:
        raise RuntimeError(f"Season S{s_num} not found")

    episode = find_by_number(season['episodes'], ep_num)
    if episode is None:
        raise RuntimeError(f"Episode S{s_num}E{ep_num} not found")

    title = find_title(episode['titles'], lang=config.language)
    info = {
        'title': title,
        'season': s_num,
        'episode': episode['number'],
        'aired': episode['airdate'],
        'plot': episode['description'],
        'duration': episode['length'] * 60,
    }

    liz = xbmcgui.ListItem(title, offscreen=True)
    liz.setInfo('video', info)

    set_listitem_images(liz, episode['images'])
    set_listitem_ratings(liz, episode['ratings'], config.default_rating_source)

    xbmcplugin.setResolvedUrl(
        handle=config.plugin_handle, 
        succeeded=True, 
        listitem=liz
    )

    xbmcplugin.endOfDirectory(config.plugin_handle)


def plugin_getartwork(config: Config) -> None:
    anime_id = config.plugin_params['id']

    anime = api_get_anime(config, anime_id)
    title = find_title(anime['titles'], lang=config.language, use_main=False)

    liz = xbmcgui.ListItem(title, offscreen=True)
    set_listitem_images(liz, anime['images'])
    set_listitem_ratings(liz, anime['ratings'], config.default_rating_source)
    
    xbmcplugin.setResolvedUrl(handle=config.plugin_handle, succeeded=True, listitem=liz)
    xbmcplugin.endOfDirectory(config.plugin_handle)



def parse_nfo(nfo: str) -> str:
    regexes = [
        r'\s*A(?P<anidb>[0-9]+)-T(?P<tmdb>[0-9]+)S(?P<tmdb_s>[0-9]+)\s*',
        r'\s*A(?P<anidb>[0-9]+)\s*',
        r'\s*T(?P<anidb>[0-9]+)\s*',
        r'\s*https?://anidb\.net/\.*aid=(?P<anidb>[0-9]+)\s*',
        r'\s*https?://www\.themoviedb\.org/tv/(?P<tmdb>[0-9]+)(?:[^/]*)/season/(?P<tmdb_s>[0-9]+).*\s*',
        r'\s*https?://www\.themoviedb\.org/tv/(?P<tmdb>[0-9]+)[^0-9]*.*\s*',
    ]

    ids = {}
    for line in nfo.splitlines():
        if line.startswith('#'):
            continue
        for reg in regexes:
            m = re.match(reg, line)
            if m:
                ids.update({k: v for k,v in m.groupdict('').items() if v})

    anidb = ids.get('anidb', '')
    tmdb = ids.get('tmdb', '')
    tmdb_s = ids.get('tmdb_s', '')

    if anidb and tmdb and tmdb_s:
        return f"A{anidb}-T{tmdb}S{tmdb_s}"
    elif anidb:
        return f"A{anidb}"
    elif tmdb:
        return f"T{tmdb}"
    return ''


def plugin_nfourl(config: Config) -> None:
    nfo = config.plugin_params['nfo']
    anime_id = parse_nfo(nfo)

    if anime_id:
        # make sure the API remembers the user's choice
        if 'A' in anime_id and 'T' in anime_id and 'S' in anime_id:
            match_url = config.api_base_url.joinpath('match', anime_id)
            api_make_request('PUT', match_url)

        anime = api_get_anime(config, anime_id)

        title = find_title(anime['titles'], lang=config.language, use_main=False)

        uniqueids = {str(k): str(v) for k, v in anime['uniqueids'].items()}
        uniqueids['amc2'] = anime['id']

        liz = xbmcgui.ListItem(title, offscreen=True)
        liz.setUniqueIDs(uniqueids, 'amc2')
        xbmcplugin.addDirectoryItem(handle=config.plugin_handle, url=anime_id, listitem=liz, isFolder=True)

    xbmcplugin.endOfDirectory(config.plugin_handle)


config = Config()

try:
    action = config.plugin_params['action']
except KeyError:
    action = ''

if action == 'find':
    plugin_find(config)
elif action == 'getdetails':
    plugin_getdetails(config)
elif action == 'getepisodelist':
    plugin_getepisodelist(config)
elif action == 'getepisodedetails':
    plugin_getepisodedetails(config)
elif action == 'getartwork':
    plugin_getartwork(config)
elif action == 'NfoUrl':
    plugin_nfourl(config)

