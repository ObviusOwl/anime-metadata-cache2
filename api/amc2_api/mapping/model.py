import dataclasses

from amc2_api.model import Title, TitleEntry

from typing import  List, Dict, Union, Optional, Protocol


@dataclasses.dataclass(frozen=True)
class TitleMappingResult:
    anidb: Title
    tmdb: Title

    is_from_match: bool = False
    is_from_storage: bool = False


@dataclasses.dataclass(frozen=True)
class AnimeMapping:
    anidb: str = ''
    tmdb: str = ''


class AnimeMappingRepo(Protocol):
    # keep the protocol open for extension (a third metadata provider)

    def resolve_tmdb(self, query: AnimeMapping) -> List[AnimeMapping]:
        """Find the tmdb IDs for any of the given IDs"""
        pass

    def resolve_anidb(self, query: AnimeMapping) -> List[AnimeMapping]:
        """Find the Anidb IDs for any of the given IDs"""
        pass

    def load(self, query: AnimeMapping) -> Optional[AnimeMapping]:
        pass

    def store(self, values: List[AnimeMapping], replace: bool = True) -> None:
        pass

    def remove(self, value: AnimeMapping) -> None:
        pass

    def dump(self) -> List[AnimeMapping]:
        pass

    def purge(self) -> None:
        pass

