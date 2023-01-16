import logging
import functools

from amc2_api.model import TitleRepo, AnimeRepo
from amc2_api import anidb
from amc2_api import tmdb
from amc2_api.mapping import anime_mapping_repo as anime_mapping_repo_factory
from amc2_api.mapping import AnimeMappingRepo
from amc2_api.persistence import CachedObjectStore, ObjectStore, object_store_factory
from amc2_api.utils import URL
from .config import settings

from typing import Optional

_logger = logging.getLogger(__name__)


class Databases:
    anidb_titles: TitleRepo
    anidb_animes_raw: ObjectStore
    anidb_animes: AnimeRepo
    anidb_images: ObjectStore

    tmdb_titles: TitleRepo
    tmdb_shows_raw: ObjectStore
    tmdb_animes: AnimeRepo
    tmdb_images: ObjectStore

    anime_mapping: AnimeMappingRepo

    def __init__(self) -> None:
        self.anidb_titles = anidb_title_repo()
        self.anidb_animes_raw = anidb_anime_store()
        self.anidb_animes = anidb_anime_repo()
        self.anidb_images = anidb_image_store()

        self.tmdb_titles = tmdb_title_repo()
        self.tmdb_shows_raw = tmdb_show_store()
        self.tmdb_animes = tmdb_anime_repo()
        self.tmdb_images = tmdb_image_store()

        self.anime_mapping = anime_mapping_repo()


def tmdb_api_url(base: str = 'https://api.themoviedb.org/3') -> URL:
    if not settings.tmdb_api_key:
        raise ValueError("The TMDB_API_KEY variable mus not be empty")
    url = URL(base)
    url.query['api_key'] = settings.tmdb_api_key
    return url


@functools.lru_cache
def anidb_title_repo() -> TitleRepo:
    ttl = settings.anidb_titles_cache_time
    raw_store = anidb.titles.anidb_titles_store(settings.anidb_titles_url)
    cache_store = object_store_factory(settings.anidb_titles_cache_url)
    titles_store = CachedObjectStore(raw_store, cache_store, ttl)
    extra_repo = anidb.titles.SqliteTitleRepo()
    return anidb.titles.PeristsedTitleRepo(titles_store, extra_repo)


@functools.lru_cache
def anidb_anime_store() -> ObjectStore:
    ttl = settings.anidb_api_cache_time
    raw = anidb.anime.anidb_anime_store(settings.anidb_api_url)
    cache = object_store_factory(settings.anidb_api_cache_url)
    return CachedObjectStore(raw, cache, ttl)


@functools.lru_cache
def anidb_image_store() -> ObjectStore:
    ttl = settings.anidb_image_cache_time
    raw = anidb.anime.anidb_image_store(settings.anidb_image_url)
    cache = object_store_factory(settings.anidb_image_cache_url)
    return CachedObjectStore(raw, cache, ttl)


@functools.lru_cache
def anidb_anime_repo() -> AnimeRepo:
    title_repo = anidb_title_repo()
    anime_store = anidb_anime_store()
    return anidb.anime.PersistedAnimeRepo(anime_store, title_repo)


@functools.lru_cache
def tmdb_show_store() -> ObjectStore:
    ttl = settings.tmdb_api_cache_time
    raw = tmdb.shows.tmdb_show_store(str(tmdb_api_url()))
    cache = object_store_factory(settings.tmdb_api_cache_url)
    return CachedObjectStore(raw, cache, ttl)


@functools.lru_cache
def tmdb_image_store() -> ObjectStore:
    ttl = settings.tmdb_image_cache_time
    raw = tmdb.shows.tmdb_image_store(str(tmdb_api_url()))
    cache = object_store_factory(settings.tmdb_image_cache_url)
    return CachedObjectStore(raw, cache, ttl)


@functools.lru_cache
def tmdb_anime_repo() -> AnimeRepo:
    show_store = tmdb_show_store()
    return tmdb.shows.PersistedTmdbAnimeRepo(show_store)


@functools.lru_cache
def tmdb_title_repo() -> TitleRepo:
    return tmdb.titles.TmdbApiTitleRepo(str(tmdb_api_url()))


@functools.lru_cache
def anime_mapping_repo() -> AnimeMappingRepo:
    return anime_mapping_repo_factory(settings.anime_mapping_url)
