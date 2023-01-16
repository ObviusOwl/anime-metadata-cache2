import enum

from pydantic import BaseSettings, validator

from amc2_api.utils import parse_timedelta

from typing import Any


class LogLevel(enum.Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    ERROR = 'ERROR'


class Settings(BaseSettings):
    self_base_url: str

    anidb_titles_url: str = 'http://anidb.net/api/anime-titles.xml.gz'
    anidb_titles_cache_url: str
    anidb_titles_cache_time: int = 2*24*60*60

    anidb_api_url: str = 'http://api.anidb.net:9001/httpapi'
    anidb_api_cache_url: str
    anidb_api_cache_time: int = 2*24*60*60

    anidb_image_url: str = 'https://cdn-eu.anidb.net/images/main'
    anidb_image_cache_url: str
    anidb_image_cache_time: int = 100*24*60*60

    tmdb_api_cache_url: str
    tmdb_api_cache_time: int = 1*24*60*60

    tmdb_image_cache_url: str
    tmdb_image_cache_time: int = 100*24*60*60

    tmdb_api_key: str

    anime_mapping_url: str

    logging_level: LogLevel = LogLevel.INFO


    @validator(
        'anidb_titles_cache_time', 
        'anidb_api_cache_time',
        'anidb_image_cache_time',
        'tmdb_api_cache_time',
        'tmdb_image_cache_time',
        pre=True
    )
    def parse_timedeltas(cls, v: Any) -> int:
        if isinstance(v, (int, float)):
            return int(v)
        elif isinstance(v, str):
            return parse_timedelta(v)
        else:
            raise ValueError("Expected int, float or str")

    class Config:
        env_file = ".env"


settings = Settings()