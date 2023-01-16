import datetime
import enum
import copy
import dataclasses
import re

from pydantic import BaseModel, validator


from typing import List, Optional, Protocol, Union, Optional, Dict, Tuple


@dataclasses.dataclass(frozen=True)
class TmdbSeasonId:
    tvshow: int
    season: int

    def __init__(self, value: Union[Tuple[int, int], str, 'TmdbSeasonId']) -> None:
        if isinstance(value, TmdbSeasonId):
            tvshow = value.tvshow
            season = value.season
        elif isinstance(value, tuple):
            if not (len(value) == 2 and isinstance(value[0], int) and isinstance(value[1], int)):
                raise ValueError(f"Invalid TMDB season id, expected a tuple of 2 ints, got {value}")
            tvshow = value[0]
            season = value[1]
        elif isinstance(value, str):
            if (m := re.match(r'^T([0-9]+)S([0-9]+)$', value)) is not None:
                tvshow = int(m.group(1))
                season = int(m.group(2))
            else:
                raise ValueError(f"Invalid TMDB season id, expected a string in the format of T##S##")
        else:
            raise ValueError(f"Invalid TMDB season id, expected a string, tuple or TmdbSeasonId, got {type(value)}")

        super().__setattr__('tvshow', tvshow)
        super().__setattr__('season', season)
    
    def __str__(self) -> str:
        return f"T{self.tvshow}S{self.season}"


@dataclasses.dataclass(frozen=True)
class AnidbId:
    anime: int

    def __init__(self, value: Union[int, str, 'AnidbId']) -> None:
        anime: int
        if isinstance(value, AnidbId):
            anime = value.anime
        elif isinstance(value, int):
            anime = value
        elif isinstance(value, str):
            if not value:
                raise ValueError("Invalid AnidbId, string must not be empty")
            elif value[0].upper() == 'A' and value[1:].isdecimal():
                anime = int(value[1:])
            elif value.isdecimal():
                anime = int(value)
            else:
                raise ValueError("Invalid AnidbId, string must be decimal")
        else:
            raise ValueError(f"Invalid AnidbId, expected string|int|AnidbId, got {type(value)}")

        super().__setattr__('anime', anime)

    def __str__(self) -> str:
        return f"A{self.anime}"


@dataclasses.dataclass(frozen=True)
class TmdbId:
    show: int

    def __init__(self, value: Union[int, str, 'TmdbId']) -> None:
        show: int
        if isinstance(value, TmdbId):
            show = value.show
        elif isinstance(value, int):
            show = value
        elif isinstance(value, str):
            if not value:
                raise ValueError("Invalid TmdbId, string must not be empty")
            elif value.isdecimal():
                show = int(value)
            elif (m := re.match(r'^T([0-9]+)(?:S[0-9]+)?$', value, re.I)):
                show = int(m.group(1))
            else:
                raise ValueError("Invalid TmdbId, string must be decimal")
        else:
            raise ValueError(f"Invalid AnidbId, expected string|int|AnidbId, got {type(value)}")

        super().__setattr__('show', show)

    def __str__(self) -> str:
        return f"T{self.show}"


@dataclasses.dataclass(frozen=True)
class AnimeMappingId:
    anidb: AnidbId
    tmdb: TmdbSeasonId

    @property
    def anidb_show(self) -> int:
        return self.anidb.anime

    @property
    def tmdb_show(self) -> int:
        return self.tmdb.tvshow

    @property
    def tmdb_season(self) -> int:
        return self.tmdb.season

    @classmethod
    def parse(cls, value: str) -> 'AnimeMappingId':
        parts = value.split('-')
        try:
            return AnimeMappingId(
                anidb=AnidbId(parts[0]),
                tmdb=TmdbSeasonId(parts[1])
            )
        except KeyError:
            raise ValueError("Invalid anime id: expected two parts")


    def __str__(self) -> str:
        return f"{self.anidb}-{self.tmdb}"


def parse_anime_id(value: str) -> Union[AnidbId, TmdbId, TmdbSeasonId, AnimeMappingId]:
    if (m := re.match(r'^A[0-9]+$', value)):
        return AnidbId(value)
    elif (m := re.match(r'^T[0-9]+$', value)):
        return TmdbId(value)
    elif (m := re.match(r'^T[0-9]+S[0-9]+$', value)):
        return TmdbSeasonId(value)
    elif (m := re.match(r'^A([0-9]+)-T([0-9]+)S([0-9]+)$', value)):
        anidb = AnidbId(int(m.group(1)))
        tmdb = TmdbSeasonId((int(m.group(2)), int(m.group(3))))
        return AnimeMappingId(anidb=anidb, tmdb=tmdb)
    else:
        raise ValueError("Invalid anime id")


class ImageType(enum.Enum):
    # the printout in movie theaters or on the DVD box
    poster = 'poster'
    # big background image without text (tmdb: backdrop, kodi: fanart)
    backdrop = 'backdrop'
    # wide an short image featuring the characters (kodi)
    banner = 'banner'
    # thumbnail image for a video (tmdb: still, kodi: thumb)
    thumb = 'thumb'
    # anything else
    unknown: 'unknown'


class Image(BaseModel):
    source: str
    name: str
    type: ImageType
    # TODO: optional width, height, aspect


class Title(BaseModel):
    value: str = ''
    aid: str = ''
    lang: str = ''
    type: str = ''


class TitleEntry(BaseModel):
    title: Title
    age: Optional[datetime.datetime] = None


class TitleRepo(Protocol):

    def find(self, title: Title) -> List[TitleEntry]:
        # note: an emty field in the requested Title means no restriction.
        raise NotImplementedError()
    
    def store(self, title: TitleEntry) -> None:
        raise NotImplementedError()
    
    def purge(self) -> None:
        raise NotImplementedError()
    
    def remove(self, title: Title) -> None:
        raise NotImplementedError()


class CastRole(BaseModel):
    character: str
    actor: str
    character_image: Optional[Image] = None
    actor_image: Optional[Image] = None


class Credit(BaseModel):
    name: str
    job: str
    department: str = ''
    category: str = ''


class Rating(BaseModel):
    source: str
    average: float
    votes: int = 0


class Episode(BaseModel):
    number: int
    length: int
    airdate: datetime.date
    titles: List[Title]
    summary: str
    images: List[Image]
    ratings: List[Rating] = []


class Season(BaseModel):
    id: str
    number: int
    uniqueids: Dict[str, str] = {}
    titles: List[Title] = []

    description: str = ""
    genres: List[str] = []
    tags: List[str] = []
    airdate: Optional[datetime.date] = None
    episodes: List[Episode] = []
    images: List[Image] = []
    ratings: List[Rating] = []

    cast: List[CastRole] = []
    directors: List[str] = []
    credits: List[Credit] = []

    @validator('episodes')
    def validate_sorted_episodes(cls, value: List[Episode]) -> List[Episode]:
        # note: this may change when mapping episode order will be supported
        # the order in the list may not be the same as the order of episode numbers
        return sorted(value, key=lambda e: e.number)

    class Config:
        validate_assignment = True
    
    def find_episode_by_number(self, ep_no: str) -> Optional[Episode]:
        for epispode in self.episodes:
            if epispode.number == ep_no:
                return epispode
        return None


class Anime(BaseModel):
    id: str
    uniqueids: Dict[str, str] = {}
    titles: List[Title] = []

    description: str = ""
    genres: List[str] = []
    tags: List[str] = []
    airdate: Optional[datetime.date] = None
    seasons: List[Season] = []
    images: List[Image] = []
    ratings: List[Rating] = []

    cast: List[CastRole] = []
    directors: List[str] = []
    credits: List[Credit] = []

    @validator('seasons')
    def validate_sorted_seasons(cls, value: List[Season]) -> List[Season]:
        return sorted(value, key=lambda e: e.number)

    class Config:
        validate_assignment = True
    
    def find_season_by_number(self, s_no: int) -> Optional[Season]:
        for season in self.seasons:
            if season.number == s_no:
                return season
        return None


class AnimeEntry(BaseModel):
    anime: Anime
    age: datetime.datetime


class AnimeRepo(Protocol):
    def get(self, aid: str) -> Optional[AnimeEntry]:
        raise NotImplementedError()


def combine_anime(
    anidb_anime: Union[Anime, AnimeEntry],
    tmdb_anime: Union[Anime, AnimeEntry],
    tmdb_season: int,
) -> Anime:
    if isinstance(anidb_anime, AnimeEntry):
        anidb_anime = anidb_anime.anime
    if isinstance(tmdb_anime, AnimeEntry):
        tmdb_anime = tmdb_anime.anime

    # don't match episodes: order does not need to match and also 
    # some anidb animes span over multiple tmdb seasons

    anidb_id = AnidbId(anidb_anime.id)
    tmdb_id = TmdbSeasonId((TmdbId(tmdb_anime.id).show, tmdb_season))

    anime = copy.deepcopy(anidb_anime)
    anime.id = str(AnimeMappingId(anidb=anidb_id, tmdb=tmdb_id))
    anime.uniqueids.update(tmdb_anime.uniqueids)
    anime.images += copy.deepcopy(tmdb_anime.images)
    anime.ratings += copy.deepcopy(tmdb_anime.ratings)

    # anidb does not have genres
    anime.genres = copy.deepcopy(tmdb_anime.genres)

    season_map: List[Tuple[int, int]] = [(0, 0), (1, tmdb_season)]
    new_seasons: List[Season] = []

    for anidb_sid, tmdb_sid in season_map:
        anidb_s = anime.find_season_by_number(anidb_sid)
        tmdb_s = tmdb_anime.find_season_by_number(tmdb_sid)
        if anidb_s is not None and tmdb_s is not None:
            anidb_s.images += copy.deepcopy(tmdb_s.images)
            anidb_s.ratings += copy.deepcopy(tmdb_s.ratings)
            new_seasons.append(anidb_s)
    anime.seasons = new_seasons

    return anime
