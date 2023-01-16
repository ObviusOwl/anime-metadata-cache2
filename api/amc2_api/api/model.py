import datetime

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

from typing import List, Union, Optional, Dict, Tuple, Generic, TypeVar

from amc2_api.model import Anime, Title, Episode, Image, ImageType, CastRole, Rating, Credit, Season
from amc2_api.model import parse_anime_id, AnidbId, TmdbId, TmdbSeasonId, AnimeMappingId
from amc2_api.utils import URL
from amc2_api.mapping import TitleMappingResult, AnimeMapping


T = TypeVar('T')


class ViewBaseModel(BaseModel):
    class Config:
        underscore_attrs_are_private = False


class Link(ViewBaseModel):
    href: str
    method: Optional[str]


LinksCollection = Dict[str, Link]
LinksField = lambda: Field(default={}, alias="_links")


class CollectionView(GenericModel, Generic[T]):
    items: List[T]
    links: LinksCollection = LinksField()

    class Config:
        underscore_attrs_are_private = False


def anime_link(
    selfurl: str, 
    anime_id: Union[str, AnidbId, TmdbId, AnimeMappingId], 
    method: str = 'GET'
) -> Link:
    #aid = AnimeId(anime_id)
    url = URL(selfurl).joinpath('anime', str(anime_id))
    return Link(href=str(url), method=method)


class TitleView(ViewBaseModel):
    title: str
    lang: str
    type: str

    @classmethod
    def from_model(cls, obj: Title) -> 'TitleView':
        return TitleView.construct(title=obj.value, lang=obj.lang, type=obj.type)


class ImageView(ViewBaseModel):
    source: str
    name: str
    type: ImageType
    links: LinksCollection = LinksField()

    @classmethod
    def from_model(cls, obj: Image, selfurl: str) -> 'ImageView':
        url = ''
        if obj.source == 'anidb':
            url = str(URL(selfurl).joinpath('anidb/images', obj.name))
        elif obj.source == 'tmdb':
            url = str(URL(selfurl).joinpath('tmdb/images', obj.name))
        
        img = ImageView.construct(source=obj.source, name=obj.name, type=obj.type)
        if url:
            img.links['image'] = Link(href=url, method='GET')
        return img


class CastRoleView(ViewBaseModel):
    character: str
    actor: str
    actor_image: Optional[ImageView] = None
    character_image: Optional[ImageView] = None

    @classmethod
    def from_model(cls, obj: CastRole, selfurl: str) -> 'CastRoleView':
        view = CastRoleView(
            character=obj.character,
            actor=obj.actor,
        )
        if obj.actor_image is not None:
            view.actor_image = ImageView.from_model(obj.actor_image, selfurl)
        if obj.character_image is not None:
            view.character_image = ImageView.from_model(obj.character_image, selfurl)
        return view


class CreditView(ViewBaseModel):
    name: str
    job: str
    department: str
    category: str = ''

    @classmethod
    def from_model(cls, obj: Credit) -> 'CastRoleView':
        return CreditView(
            name=obj.name,
            job=obj.job,
            department=obj.department,
            category=obj.category,
        )


class RatingView(ViewBaseModel):
    source: str
    average: float
    votes: int = 0

    @classmethod
    def from_model(cls, obj: Rating) -> 'RatingView':
        return RatingView(source=obj.source, average=obj.average, votes=obj.votes)


class EpisodeView(ViewBaseModel):
    number: int
    titles: List[TitleView]
    description: str
    length: int
    airdate: datetime.date
    images: List[ImageView]
    ratings: List[RatingView] = []
    links: LinksCollection = LinksField()

    @classmethod
    def from_model(cls, obj: Episode, selfurl: str) -> 'EpisodeView':
        return EpisodeView.construct(
            number=obj.number,
            titles=[TitleView.from_model(t) for t in obj.titles],
            description=obj.summary,
            length=obj.length,
            airdate=obj.airdate,
            images=[ImageView.from_model(i, selfurl) for i in obj.images],
            ratings=[RatingView.from_model(r) for r in obj.ratings],
        )



class SeasonView(ViewBaseModel):
    id: str
    number: int
    uniqueids: Dict[str, str]
    titles: List[TitleView]

    description: str
    genres: List[str] = []
    tags: List[str] = []
    airdate: Optional[datetime.date] = None
    episodes: List[EpisodeView]
    images: List[ImageView]
    ratings: List[RatingView] = []

    cast: List[CastRoleView]
    directors: List[str] = []
    credits: List[CreditView] = []

    links: LinksCollection = LinksField()

    @classmethod
    def from_model(cls, obj: Season, selfurl: str) -> 'SeasonView':
        return SeasonView.construct(
            id=obj.id,
            number=obj.number,
            uniqueids=obj.uniqueids,
            titles=[TitleView.from_model(t) for t in obj.titles],
            description=obj.description,
            genres=list(obj.genres),
            tags=list(obj.tags),
            episodes=[EpisodeView.from_model(ep, selfurl) for ep in obj.episodes],            
            images=[ImageView.from_model(i, selfurl) for i in obj.images],
            cast=[CastRoleView.from_model(c, selfurl) for c in obj.cast],
            directors=list(obj.directors),
            airdate=obj.airdate,
            ratings=[RatingView.from_model(r) for r in obj.ratings],
            credits=[CreditView.from_model(c) for c in obj.credits],
        )



class AnimeView(ViewBaseModel):
    id: str
    uniqueids: Dict[str, str]
    titles: List[TitleView]

    description: str
    genres: List[str] = []
    tags: List[str] = []
    airdate: Optional[datetime.date] = None
    seasons: List[SeasonView]
    images: List[ImageView]
    ratings: List[RatingView] = []

    cast: List[CastRoleView]
    directors: List[str] = []
    credits: List[CreditView] = []

    links: LinksCollection = LinksField()

    @classmethod
    def from_model(cls, obj: Anime, selfurl: str) -> 'AnimeView':
        return AnimeView.construct(
            id=obj.id,
            uniqueids=obj.uniqueids,
            titles=[TitleView.from_model(t) for t in obj.titles],
            description=obj.description,
            genres=list(obj.genres),
            tags=list(obj.tags),
            seasons=[SeasonView.from_model(s, selfurl) for s in obj.seasons],            
            images=[ImageView.from_model(i, selfurl) for i in obj.images],
            cast=[CastRoleView.from_model(c, selfurl) for c in obj.cast],
            directors=list(obj.directors),
            airdate=obj.airdate,
            ratings=[RatingView.from_model(r) for r in obj.ratings],
            credits=[CreditView.from_model(c) for c in obj.credits],
        )


class TitleMappingView(ViewBaseModel):

    class _Anime(BaseModel):
        title: TitleView
        id: str

    anime_id: str
    anidb: _Anime
    tmdb: _Anime
    links: LinksCollection = LinksField()

    @classmethod
    def from_model(cls, obj: TitleMappingResult, selfurl: str) -> 'TitleMappingView':
        anidb_title = TitleView.from_model(obj.anidb)
        tmdb_title = TitleView.from_model(obj.tmdb)

        anidb_id = AnidbId(obj.anidb.aid)
        tmdb_id = TmdbSeasonId(obj.tmdb.aid)

        view = TitleMappingView.construct(
            anime_id=str(AnimeMappingId(anidb=anidb_id, tmdb=tmdb_id)),
            anidb=TitleMappingView._Anime(title=anidb_title, id=obj.anidb.aid),
            tmdb=TitleMappingView._Anime(title=tmdb_title, id=obj.tmdb.aid),
        )

        view.links['anime'] = anime_link(selfurl, view.anime_id)

        match_url = URL(selfurl).joinpath('match', view.anime_id)
        if not obj.is_from_storage:
            view.links['remember'] = Link(href=str(match_url), method='PUT')
        else:
            view.links['forget'] = Link(href=str(match_url), method='DELETE')

        return view


class AnimeMappingView(ViewBaseModel):
    anime_id: str
    uniqueids: Dict[str, str]    
    links: LinksCollection = LinksField()

    @classmethod
    def from_model(cls, obj: AnimeMapping, selfurl: str) -> 'AnimeMappingView':
        anidb_id = AnidbId(obj.anidb)
        tmdb_id = TmdbSeasonId(obj.tmdb)
        aid = f"{anidb_id}-{tmdb_id}"

        uniqueids = {
            'anidb': aid.anidb, 
            'tmdb': str(aid.tmdb.tvshow),
            'tmdb_season': str(aid.tmdb.season),
        }

        view = AnimeMappingView(
            anime_id=str(aid),
            uniqueids=uniqueids,
        )

        view.links['anime'] = anime_link(selfurl, view.anime_id)

        match_url = URL(selfurl).joinpath('match', view.anime_id)
        view.links['forget'] = Link(href=str(match_url), method='DELETE')

        return view
