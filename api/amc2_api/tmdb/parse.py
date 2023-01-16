import datetime
import logging
import json
import copy

from amc2_api.model import Title, Episode, Anime, Image, ImageType, TmdbSeasonId, CastRole, Rating, Credit, Season

from typing import List, Optional, Callable, Dict, Union, Tuple, Any, Iterator

_logger = logging.getLogger(__name__)


def iter_collection(obj: Any, collection_key: str, item_key: str) -> Iterator[Tuple[Any, str]]:
    for item in obj.get(collection_key, []):
        num = item.get(item_key, None)
        if num is not None:
            yield item, str(num)


class AnimeTmdbJsonParser:
    language: str

    @classmethod
    def parse(
        cls, 
        json_data: str, 
        lang: str = 'en'
    ) -> Optional[Anime]:
        try:
            root = json.loads(json_data)
            return cls(lang=lang).parse_anime(root)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON: " + str(e))
    
    def __init__(self, lang: str = 'en') -> None:
        self.language = lang

    def _parse_str(self, obj: Any, key: str, default: str) -> str:
        if key not in obj:
            return default
        return str(obj[key])

    def _parse_int(self, obj: Any, key: str, default: int) -> int:
        if key not in obj or not obj[key]:
            return default
        return int(obj[key])
    
    def _parse_date(self, obj: Any, key: str, default: str) -> datetime.date:
        if key not in obj or not obj[key]:
            return datetime.date.fromisoformat(default)
        return datetime.date.fromisoformat(str(obj[key]))
    
    def _parse_image(self, obj: Any, img_type: ImageType) -> Image:
        name = str(obj['file_path']).strip('/')
        return Image(type=img_type, name=name, source='tmdb')
    
    def _parse_vote(self, obj: Any) -> Optional[Rating]:
        try:
            return Rating(
                source='tmdb', 
                average=float(obj['vote_average']),
                votes=int(obj['vote_count']),
            )
        except (ValueError, KeyError):
            return None

    def parse_cast(self, list_obj: Any) -> List[CastRole]:
        roles: List[CastRole] = []
        for obj in list_obj:
            iname = cname = aname = ""
            if 'roles' in obj and len(obj['roles']) > 0 and 'character' in obj['roles'][0]:
                cname = obj['roles'][0]['character']
            if 'name' in obj:
                aname = obj['name']
            if 'profile_path' in obj:
                iname = str(obj['profile_path']).strip('/')

            if not cname or not aname:
                continue
            
            role = CastRole(character=cname, actor=aname)
            if iname:
                role.actor_image = Image(type=ImageType.thumb, name=iname, source='tmdb')
            roles.append(role)
        return roles
    
    def parse_credits(self, list_obj: Any) -> List[Credit]:
        credits: List[Credit] = []
        for obj in list_obj:
            name = obj.get('name', '')
            dep = obj.get('department', '')

            cat = obj.get('known_for_department', None)
            if cat is None:
                cat = ''
            cat = cat.lower()

            jobs = [job['job'] for job in obj.get('jobs', []) if job['job']]
            
            if name and dep:
                for job in jobs:
                    credit = Credit(name=name, job=job, department=dep, category=cat)
                    credits.append(credit)

        return credits

    def parse_images(self, images: Any) -> List[Image]:
        imgs: List[Image] = []
        imgs += [self._parse_image(o, ImageType.poster) for o in images.get('posters', [])]
        imgs += [self._parse_image(o, ImageType.backdrop) for o in images.get('backdrops', [])]
        imgs += [self._parse_image(o, ImageType.thumb) for o in images.get('stills', [])]
        return imgs

    def parse_episode(self, episode: Any) -> Episode:
        episode_num = self._parse_int(episode, 'episode_number', 0)
        length = self._parse_int(episode, 'runtime', 0)
        airdate = self._parse_date(episode, 'air_date', '0001-01-01')
        name = self._parse_str(episode, 'name', '')
        summary = self._parse_str(episode, 'overview', '')
        images = self.parse_images(episode.get('images', {}))

        ratings: List[Rating] = []
        if (vote := self._parse_vote(episode)) is not None:
            ratings = [vote]

        return Episode(
            number=episode_num,
            length=length,
            airdate=airdate,
            titles=[Title(lang=self.language, type='main', value=name)],
            summary=summary,
            images=images,
            ratings=ratings,
        )


    def parse_season(self, season: Any, parent_id: str) -> Season:
        season_num = self._parse_int(season, 'season_number', 0)
        season_name = self._parse_str(season, 'name', '')
        anime_id = str(TmdbSeasonId((parent_id, season_num)))

        title = Title(lang=self.language, type='main', value=season_name, aid=anime_id)

        descr = self._parse_str(season, 'overview', '')
        
        episodes = [self.parse_episode(e) for e in season.get('episodes', [])]

        # seasons don't have a backdrop, the main tvshow has
        images = self.parse_images(season.get('images', {}))

        cast: List[CastRole] = []
        if 'credits' in season and 'cast' in season['credits']:
            cast = self.parse_cast(season['credits']['cast'])
        
        credits: List[Credit] = []
        if 'credits' in season and 'crew' in season['credits']:
            credits = self.parse_credits(season['credits']['crew'])

        airdate: Optional[datetime.date] = None
        if 'air_date' in season and season['air_date']:
            airdate = datetime.date.fromisoformat(str(season['air_date']))

        return Season(
            id=anime_id,
            number=season_num, 
            uniqueids={'tmdb': parent_id, 'tmdb_season': str(season_num)},
            titles=[title], 

            description=descr, 
            genres=[],
            tags=[],
            airdate=airdate,
            episodes=episodes,
            images=images,
            ratings=[],

            cast=cast,
            directors=[],
            credits=credits,
        )
    
    def parse_anime(self, root: Any) -> Anime:
        show_id = self._parse_int(root, 'id', 0)
        show_name = self._parse_str(root, 'name', '')

        title = Title(lang=self.language, type='main', value=show_name)

        descr = self._parse_str(root, 'overview', '')

        # seasons don't have a backdrop, the main tvshow has
        images = self.parse_images(root.get('images', {}))

        genres: List[str] = []    
        if 'genres' in root:
            genres = [g.get('name', '') for g in root['genres'] if g.get('name', '')]

        # tmdb keeps those on a per season level
        cast: List[CastRole] = []
        credits: List[Credit] = []
        airdate: Optional[datetime.date] = None

        seasons: List[Season] = []
        for season_obj, sid in iter_collection(root, 'seasons', 'season_number'):
            season = self.parse_season(season_obj, show_id)

            # seasons dont have generes
            season.genres = genres

            # seasons don't have a backdrop, the main tvshow has
            season.images += [copy.deepcopy(img) for img in images if img.type == ImageType.backdrop]

            if sid == '1':
                # heuristic to populate missing tvshow metadata with the data from season 1
                cast = copy.deepcopy(season.cast)
                credits = copy.deepcopy(season.credits)
                airdate = copy.deepcopy(season.airdate)

            seasons.append(season)
        
        return Anime(
            id='T' + str(show_id), 
            uniqueids={'tmdb': str(show_id)},
            titles=[title], 
    
            description=descr, 
            genres=genres,
            airdate=airdate,
            seasons=seasons,
            images=images,

            cast=cast,
            credits=credits,
        )

