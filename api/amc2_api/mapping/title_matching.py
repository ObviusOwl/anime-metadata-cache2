import itertools

from amc2_api.model import Title, TitleEntry, TitleRepo

from typing import  List, Dict, Union, Optional

from .model import TitleMappingResult, AnimeMapping, AnimeMappingRepo


class AnidbTitleMatcher:
    """
    Uses anidb as primay source to find anime of the given title string. 
    Each found anime is matched to a tvdb counterpart.

    The title is first looked up on anidb, the result of that is matched with
    tmdb. The reverse direction is yet to be implemented as separate service.
    This service contains logic tied to the title types and languages on anidb.
    
    The title can point to multiple anidb entries. Each anidb entry can also 
    map to multiple tmdb entries (seasons). Thus the result can contain mappings 
    which are not correct. It is up to the user to select one.

    The mappings repo is queried prior to using the tmdb titles repo. If all mappings 
    can be resolved with the mappings repo, the tmdb titles repo is not used.

    The mapping result is tagged with boolean flags. Perfect matches should be 
    persisted so that they don't need to get matched again. The calling service 
    should do this according to the flags set on the results.
    """

    _anidb_repo: TitleRepo
    _tmdb_repo: TitleRepo
    _mapping_repo: AnimeMappingRepo

    def __init__(
        self,
        anidb_title_repo: TitleRepo,
        tmdb_title_repo: TitleRepo,
        mapping_repo: AnimeMappingRepo,
    ) -> None:
        self._anidb_repo = anidb_title_repo
        self._tmdb_repo = tmdb_title_repo
        self._mapping_repo = mapping_repo


    def match_title(self, title: Title) -> List[TitleMappingResult]:
        """
        Expects title.value to be set for the search. 
        The field title.aid, if known, can be set to the anidb id.
        The field title.lang sets the language to query (in both title repos)
        """

        lang = title.lang

        anidb_titles = self._anidb_repo.find(title)
        anidb_titles_idx = self._index_titles(anidb_titles)

        result: List[TitleMappingResult] = []

        # eliminate titles where we have a persisted mapping
        for titles in anidb_titles_idx.values():
            main_title = self._get_main_title(titles)
            result += self._find_stored_match(main_title)

        # dont match stored mappings again
        for item in result:
            anidb_titles_idx.pop(item.anidb.aid, None)

        # avoid hitting the tmdb API if the work is already done
        if not anidb_titles_idx:
            return result

        # try to match one anime after another
        for anidb_id in anidb_titles_idx.keys():
            anidb_titles = self._anidb_repo.find(Title(aid=anidb_id))

            result += self._find_tmdb_match(anidb_titles, lang)
        
        return result
    
    def _result(
        self, 
        pri: Union[TitleEntry, Title], 
        sec: Union[TitleEntry, Title], 
        match: bool = False, 
        load: bool = False
    ) -> TitleMappingResult:
        pri = pri if isinstance(pri, Title) else pri.title
        sec = sec if isinstance(sec, Title) else sec.title
        return TitleMappingResult(anidb=pri, tmdb=sec, is_from_match=match, is_from_storage=load)

    def _find_stored_match(self, entry: TitleEntry) -> List[TitleMappingResult]:
        res: List[TitleMappingResult] = []

        query = AnimeMapping(anidb=entry.title.aid)
        for id_tuple in self._mapping_repo.resolve_tmdb(query):
            result = self._result(entry, Title(aid=id_tuple.tmdb), load=True)
            res.append(result)
        
        return res

    def _find_tmdb_match(
        self,
        anidb_titles: List[TitleEntry], 
        lang: str
    ) -> List[TitleMappingResult]:

        result: List[TitleMappingResult] = []

        # do multiple attempts to match a title agains tmdb
        # each attempt generates a tmdb search request (so keep the list short)
        # also each attempt checks agains all titles we have for that anime
        # anidb/romaji/func_param -> anidb/english/repo_find -> tmdb/english/match
        
        for anidb_title in self._get_mapping_titles(anidb_titles):
            tmdb_query = Title(lang=lang, value=anidb_title.title.value)
            tmdb_titles = self._tmdb_repo.find(tmdb_query)

            perfect_match = self._find_perfect_title_match(anidb_titles, tmdb_titles)
            if perfect_match is not None:
                # return only the perfect match, the others can never be valid
                # return early as the other attempts are now redundant
                return [perfect_match]
            else:
                # with multiple trials to get a perfect-match we end up with redundant 
                # mappings, however they use different languages, which the user can 
                # use to decide on the correct series match
                for tmdb_title in tmdb_titles:
                    result.append(self._result(anidb_title, tmdb_title))

        # the list of tmdb seasons that may correlate with the given anidb anime    
        return result

    def _find_perfect_title_match(
        self,
        anidb_titles: List[TitleEntry], 
        tmdb_titles: List[TitleEntry]
    ) -> Optional[TitleMappingResult]:
        # search in the cartesian product and abort early if found
        # expects the titles to be from the same show (other matches/shows are lost)
        for anidb_title, tmdb_title in itertools.product(anidb_titles, tmdb_titles):
            # note: keep it strict, anime sequels often have a 1-character difference
            t1 = anidb_title.title.value.strip().lower()
            t2 = tmdb_title.title.value.strip().lower()
            if t1 == t2 and t1 != '':
                return self._result(anidb_title, tmdb_title, match=True)
        return None

    def _index_titles(self, titles: List[TitleEntry]) -> Dict[str, List[TitleEntry]]:
        entry_map: Dict[str, List[TitleEntry]] = {}
        for entry in titles:
            if entry.title.aid not in entry_map:
                entry_map[entry.title.aid] = []
            entry_map[entry.title.aid].append(entry)
        return entry_map

    def _get_main_title(self, titles: List[TitleEntry]) -> TitleEntry:
        # prefer the main title
        for entry in titles:
            if entry.title.type == 'main':
                return entry

        # maybe there is an english official title    
        for entry in titles:
            if entry.title.type == 'official' and entry.title.lang == 'en':
                return entry

        # there must be an official japanese title, right?
        for entry in titles:
            if entry.title.type == 'official' and entry.title.lang == 'ja':
                return entry
        
        # return the first title
        if titles:
            return titles[0]
        
        # give up
        raise ValueError("No suitable main title found")


    def _get_mapping_titles(self, titles: List[TitleEntry]) -> List[TitleEntry]:
        # the list of titles to try to map, in order
        r  = [t for t in titles if t.title.type == 'official' and t.title.lang == 'en']
        r += [t for t in titles if t.title.type == 'main']
        r += [t for t in titles if t.title.type == 'official' and t.title.lang == 'ja']
        return r
        
