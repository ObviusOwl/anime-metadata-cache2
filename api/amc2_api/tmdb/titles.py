import requests
import logging
import re
import datetime

from amc2_api.model import Title, TitleEntry, TmdbSeasonId
from amc2_api.utils import URL, Throttler

from typing import Union, List, Any, Tuple

_logger = logging.getLogger(__name__)


class TmdbApiTitleRepo:
    """
    When searching for a title, all fields except the title value are ignored.

    Uses the TMDB API for searching and extracts the seasons. 
    Does not handle multipage responses (only a few shows should match)

    Ignores specials season if it is named "Specials" and has number 0.
    
    If a season has a generic title like "Season xx" a new title is synthetized 
    using the show's title and the season name, else the season name is used as is.

    If season 1 is named "Season 1" it's name is always replaced with the show name.

    Always returns IDs in the form TxxSyy.

    Note that the TMDB API returns shows instead of seasons/animes. Thus the result
    always contains all seasons of the found shows. Thus if there is a perfect 
    match, the result also contains the other related seasons.

    Note that tme API provides "alternative titles" for the show, however they
    have no season information and thus cannot be used.
    """

    _api_url: URL
    _throttler: Throttler

    def __init__(self, api_url: Union[str, URL]) -> None:
        self._api_url = URL(api_url)
        self._throttler = Throttler(1)

    def _search(self, value: str) -> List[int]:
        self._throttler.wait()

        url = self._api_url.joinpath('search/tv')
        url.query['query'] = value
        resp = requests.get(str(url))
        if not resp.ok:
            return []
        return [r['id'] for r in resp.json()['results']]

    def _get_show(self, tid: int) -> Any:
        self._throttler.wait()

        url = self._api_url.joinpath(f'tv/{tid}')
        resp = requests.get(str(url))
        if not resp.ok:
            return None
        return resp.json()
    
    def _is_generic_name(self, name: str, num: int = -1) -> bool:
        if (m := re.match(r'season\s+([0-9]+)', name.strip(), re.I)) is not None:
            parsed_num = int(m.group(1))
            return True if num < 0 else num == parsed_num
        return False
    
    def _is_specials_name(self, name: str) -> bool:
        return name.strip().lower() == 'specials'

    def _handle_tvshow(self, show: Any) -> List[TitleEntry]:
        show_id = int(show['id'])
        show_name = str(show['name'])

        seasons = [(str(s['name']), int(s['season_number'])) for s in show['seasons']]
        seasons = [s for s in seasons if not self._is_specials_name(s[0])]

        entries: List[TitleEntry] = []
        for s_name, s_num in seasons:
            if self._is_generic_name(s_name, num=1):
                title = show_name
            elif self._is_generic_name(s_name):
                title = show_name + ' ' + s_name
            else:
                title = s_name

            t = Title(value=title, aid=str(TmdbSeasonId((show_id, s_num))))
            age = datetime.datetime.now(tz=datetime.timezone.utc)
            entries.append(TitleEntry(title=t, age=age))
        return entries
        
    def find(self, title: Title) -> List[TitleEntry]:
        if not title.value:
            raise ValueError("Expected a title value")

        # TODO: maybe support title.lang

        entries: List[TitleEntry] = []

        for tid in self._search(title.value):
            show = self._get_show(tid)
            if show is not None:
                entries += self._handle_tvshow(show)
        
        return entries

    def store(self, title: TitleEntry) -> None:
        raise NotImplementedError()
    
    def purge(self) -> None:
        raise NotImplementedError()
    
    def remove(self, title: Title) -> None:
        raise NotImplementedError()
